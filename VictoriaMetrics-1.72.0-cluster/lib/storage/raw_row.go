package storage

import (
	"sort"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// rawRow reperesents raw timeseries row.
type rawRow struct {  // 插入数据文件的原始数据格式, 一共56字节
	// TSID is time series id.
	TSID TSID  //32字节

	// Timestamp is unix timestamp in milliseconds.
	Timestamp int64

	// Value is time series value for the given timestamp.
	Value float64

	// PrecisionBits is the number of the significant bits in the Value
	// to store. Possible values are [1..64].
	// 1 means max. 50% error, 2 - 25%, 3 - 12.5%, 64 means no error, i.e.
	// Value stored without information loss.
	PrecisionBits uint8
}

type rawRowsMarshaler struct {  // 用于序列化 tsid + timestamp + value的数据
	bsw blockStreamWriter

	auxTimestamps  []int64  // 在连续多条TSID相同的情况下， timestamp的数据直接追加到这里
	auxValues      []int64
	auxFloatValues []float64
}  //存储同一个TSID的数据，临时存放

func (rrm *rawRowsMarshaler) reset() {
	rrm.bsw.reset()

	rrm.auxTimestamps = rrm.auxTimestamps[:0]
	rrm.auxValues = rrm.auxValues[:0]
	rrm.auxFloatValues = rrm.auxFloatValues[:0]
}

// Use sort.Interface instead of sort.Slice in order to optimize rows swap.
type rawRowsSort []rawRow

func (rrs *rawRowsSort) Len() int { return len(*rrs) }
func (rrs *rawRowsSort) Less(i, j int) bool {  // 对 TSID 和 time stamp进行排序
	x := *rrs
	if i < 0 || j < 0 || i >= len(x) || j >= len(x) {
		// This is no-op for compiler, so it doesn't generate panic code
		// for out of range access on x[i], x[j] below
		return false
	}
	a := &x[i]
	b := &x[j]
	ta := &a.TSID
	tb := &b.TSID

	// Manually inline TSID.Less here, since the compiler doesn't inline it yet :(
	if ta.AccountID != tb.AccountID {
		return ta.AccountID < tb.AccountID
	}
	if ta.ProjectID != tb.ProjectID {
		return ta.ProjectID < tb.ProjectID
	}
	if ta.MetricGroupID != tb.MetricGroupID {
		return ta.MetricGroupID < tb.MetricGroupID
	}
	if ta.JobID != tb.JobID {
		return ta.JobID < tb.JobID
	}
	if ta.InstanceID != tb.InstanceID {
		return ta.InstanceID < tb.InstanceID
	}
	if ta.MetricID != tb.MetricID {
		return ta.MetricID < tb.MetricID
	}
	return a.Timestamp < b.Timestamp
}
func (rrs *rawRowsSort) Swap(i, j int) {
	x := *rrs
	x[i], x[j] = x[j], x[i]
}

func (rrm *rawRowsMarshaler) marshalToInmemoryPart(mp *inmemoryPart, rows []rawRow) {  // 把1万个data point放到 inmemoryPart
	if len(rows) == 0 {
		return
	}
	if uint64(len(rows)) >= 1<<32 {
		logger.Panicf("BUG: rows count must be smaller than 2^32; got %d", len(rows))
	}

	rrm.bsw.InitFromInmemoryPart(mp)  // 初始化 blockStreamWriter 对象

	ph := &mp.ph  // part header
	ph.Reset()

	// Sort rows by (TSID, Timestamp) if they aren't sorted yet.
	rrs := rawRowsSort(rows)
	if !sort.IsSorted(&rrs) {
		sort.Sort(&rrs)  // 对 tsid 和 timestamp 排序
	}

	// Group rows into blocks.
	var scale int16
	var rowsMerged uint64
	r := &rows[0]
	tsid := &r.TSID
	precisionBits := r.PrecisionBits
	tmpBlock := getBlock()
	defer putBlock(tmpBlock)
	for i := range rows {  // 逐条处理原始数据
		r = &rows[i]
		if r.TSID.MetricID == tsid.MetricID && len(rrm.auxTimestamps) < maxRowsPerBlock {  // 处理连续多条相同的 TSID 的数据
			rrm.auxTimestamps = append(rrm.auxTimestamps, r.Timestamp)
			rrm.auxFloatValues = append(rrm.auxFloatValues, r.Value)
			continue
		}
		// 不同的TSID是不同的block。 这里有多少个TSID就会写多少个BLOCK
		rrm.auxValues, scale = decimal.AppendFloatToDecimal(rrm.auxValues[:0], rrm.auxFloatValues)
		tmpBlock.Init(tsid, rrm.auxTimestamps, rrm.auxValues, scale, precisionBits)  // 初始化block对象
		rrm.bsw.WriteExternalBlock(tmpBlock, ph, &rowsMerged)
			//切换到存储下一个TSID的数据
		tsid = &r.TSID  // 记录上一次产生的TSID
		precisionBits = r.PrecisionBits
		rrm.auxTimestamps = append(rrm.auxTimestamps[:0], r.Timestamp)
		rrm.auxFloatValues = append(rrm.auxFloatValues[:0], r.Value)
	}
	// 最后一个 tsid 的数据
	rrm.auxValues, scale = decimal.AppendFloatToDecimal(rrm.auxValues[:0], rrm.auxFloatValues)
	tmpBlock.Init(tsid, rrm.auxTimestamps, rrm.auxValues, scale, precisionBits)
	rrm.bsw.WriteExternalBlock(tmpBlock, ph, &rowsMerged)
	if rowsMerged != uint64(len(rows)) {
		logger.Panicf("BUG: unexpected rowsMerged; got %d; want %d", rowsMerged, len(rows))
	}
	rrm.bsw.MustClose()
}

func getRawRowsMarshaler() *rawRowsMarshaler {
	v := rrmPool.Get()
	if v == nil {
		return &rawRowsMarshaler{}
	}
	return v.(*rawRowsMarshaler)
}

func putRawRowsMarshaler(rrm *rawRowsMarshaler) {
	rrm.reset()
	rrmPool.Put(rrm)
}

var rrmPool sync.Pool
