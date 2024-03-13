package storage

import (
	"fmt"
	"io"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storagepacelimiter"
)

// BlockRef references a Block.
//
// BlockRef is valid only until the corresponding Search is valid,
// i.e. it becomes invalid after Search.MustClose is called.
type BlockRef struct {
	p  *part
	bh blockHeader
}

func (br *BlockRef) reset() {
	br.p = nil
	br.bh = blockHeader{}
}

func (br *BlockRef) init(p *part, bh *blockHeader) {
	br.p = p
	br.bh = *bh
}

// MustReadBlock reads block from br to dst.
//
// if fetchData is false, then only block header is read, otherwise all the data is read.
func (br *BlockRef) MustReadBlock(dst *Block, fetchData bool) {
	dst.Reset()
	dst.bh = br.bh
	if !fetchData {
		return
	}

	dst.timestampsData = bytesutil.Resize(dst.timestampsData[:0], int(br.bh.TimestampsBlockSize))
	br.p.timestampsFile.MustReadAt(dst.timestampsData, int64(br.bh.TimestampsBlockOffset))

	dst.valuesData = bytesutil.Resize(dst.valuesData[:0], int(br.bh.ValuesBlockSize))
	br.p.valuesFile.MustReadAt(dst.valuesData, int64(br.bh.ValuesBlockOffset))
}

// MetricBlockRef contains reference to time series block for a single metric.
type MetricBlockRef struct {
	// The metric name
	MetricName []byte

	// The block reference. Call BlockRef.MustReadBlock in order to obtain the block.
	BlockRef *BlockRef
}

// MetricBlock is a time series block for a single metric.
type MetricBlock struct {  //代表单个metric查询到的数据
	// MetricName is metric name for the given Block.
	MetricName []byte  // metric的label name + label value的部分

	// Block is a block for the given MetricName
	Block Block  // time series的数据部分
}

// Marshal marshals MetricBlock to dst
func (mb *MetricBlock) Marshal(dst []byte) []byte {
	dst = encoding.MarshalBytes(dst, mb.MetricName)
	return MarshalBlock(dst, &mb.Block)
}

// MarshalBlock marshals b to dst.
//
// b.MarshalData must be called on b before calling MarshalBlock.
func MarshalBlock(dst []byte, b *Block) []byte {
	dst = b.bh.Marshal(dst)
	dst = encoding.MarshalBytes(dst, b.timestampsData)
	dst = encoding.MarshalBytes(dst, b.valuesData)
	return dst
}

// Unmarshal unmarshals MetricBlock from src
func (mb *MetricBlock) Unmarshal(src []byte) ([]byte, error) {
	mb.Block.Reset()
	tail, mn, err := encoding.UnmarshalBytes(src)
	if err != nil {
		return tail, fmt.Errorf("cannot unmarshal MetricName: %w", err)
	}
	mb.MetricName = append(mb.MetricName[:0], mn...)
	src = tail

	return UnmarshalBlock(&mb.Block, src)
}

// UnmarshalBlock unmarshal Block from src to dst.
//
// dst.UnmarshalData isn't called on the block.
func UnmarshalBlock(dst *Block, src []byte) ([]byte, error) {
	tail, err := dst.bh.Unmarshal(src)
	if err != nil {
		return tail, fmt.Errorf("cannot unmarshal blockHeader: %w", err)
	}
	src = tail

	tail, tds, err := encoding.UnmarshalBytes(src)
	if err != nil {
		return tail, fmt.Errorf("cannot unmarshal timestampsData: %w", err)
	}
	dst.timestampsData = append(dst.timestampsData[:0], tds...)
	src = tail

	tail, vd, err := encoding.UnmarshalBytes(src)
	if err != nil {
		return tail, fmt.Errorf("cannot unmarshal valuesData: %w", err)
	}
	dst.valuesData = append(dst.valuesData[:0], vd...)
	src = tail

	return src, nil
}

// Search is a search for time series.
type Search struct {
	// MetricBlockRef is updated with each Search.NextMetricBlock call.
	MetricBlockRef MetricBlockRef

	idb *indexDB

	ts tableSearch

	// tr contains time range used in the serach.
	tr TimeRange  //查询时间范围

	// tfss contains tag filters used in the search.
	tfss []*TagFilters  //过滤表达式

	// deadline in unix timestamp seconds for the current search.
	deadline uint64  //超时时间

	err error

	needClosing bool

	loops int

	prevMetricID uint64
}

func (s *Search) reset() {
	s.MetricBlockRef.MetricName = s.MetricBlockRef.MetricName[:0]
	s.MetricBlockRef.BlockRef = nil

	s.idb = nil
	s.ts.reset()
	s.tr = TimeRange{}
	s.tfss = nil
	s.deadline = 0
	s.err = nil
	s.needClosing = false
	s.loops = 0
	s.prevMetricID = 0
}

// Init initializes s from the given storage, tfss and tr.
//
// MustClose must be called when the search is done.
//
// Init returns the upper bound on the number of found time series.  // maxMetrics 默认30万
func (s *Search) Init(storage *Storage, tfss []*TagFilters, tr TimeRange, maxMetrics int, deadline uint64) int {
	if s.needClosing {
		logger.Panicf("BUG: missing MustClose call before the next call to Init")
	}

	s.reset()
	s.tr = tr  // TimeRange
	s.tfss = tfss  // TagFilters   通过解析好的metric组，来决定到底搜索哪些数据
	s.deadline = deadline  // 客户端发过来的超时时间
	s.needClosing = true

	tsids, err := storage.searchTSIDs(tfss, tr, maxMetrics, deadline)  // 根据标签，搜索符合的TSID
	if err == nil {
		err = storage.prefetchMetricNames(tsids, deadline)  //猜测是根据tsid找到metric的完整信息
	}
	// It is ok to call Init on error from storage.searchTSIDs.
	// Init must be called before returning because it will fail
	// on Seach.MustClose otherwise.
	s.ts.Init(storage.tb, tsids, tr)

	if err != nil {
		s.err = err
		return 0
	}

	s.idb = storage.idb()  //index db
	return len(tsids)
}

// MustClose closes the Search.
func (s *Search) MustClose() {
	if !s.needClosing {
		logger.Panicf("BUG: missing Init call before MustClose")
	}
	s.ts.MustClose()
	s.reset()
}

// Error returns the last error from s.
func (s *Search) Error() error {
	if s.err == io.EOF || s.err == nil {
		return nil
	}
	return fmt.Errorf("error when searching for tagFilters=%s on the time range %s: %w", s.tfss, s.tr.String(), s.err)
}

// NextMetricBlock proceeds to the next MetricBlockRef.
func (s *Search) NextMetricBlock() bool {
	if s.err != nil {
		return false
	}
	for s.ts.NextBlock() {
		if s.loops&paceLimiterSlowIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(s.deadline); err != nil {
				s.err = err
				return false
			}
		}
		s.loops++
		tsid := &s.ts.BlockRef.bh.TSID
		if tsid.MetricID != s.prevMetricID {
			var err error
			s.MetricBlockRef.MetricName, err = s.idb.searchMetricNameWithCache(s.MetricBlockRef.MetricName[:0], tsid.MetricID, tsid.AccountID, tsid.ProjectID)
			if err != nil {
				if err == io.EOF {
					// Skip missing metricName for tsid.MetricID.
					// It should be automatically fixed. See indexDB.searchMetricNameWithCache for details.
					continue
				}
				s.err = err
				return false
			}
			s.prevMetricID = tsid.MetricID
		}
		s.MetricBlockRef.BlockRef = s.ts.BlockRef
		return true
	}
	if err := s.ts.Error(); err != nil {
		s.err = err
		return false
	}

	s.err = io.EOF
	return false
}

// SearchQuery is used for sending search queries from vmselect to vmstorage.
type SearchQuery struct {  //来自vm-select的query_range请求的格式
	AccountID    uint32
	ProjectID    uint32
	MinTimestamp int64
	MaxTimestamp int64
	TagFilterss  [][]TagFilter  //metric分组，每一组内又是多个label_name = label_value
}

// NewSearchQuery creates new search query for the given args.
func NewSearchQuery(accountID, projectID uint32, start, end int64, tagFilterss [][]TagFilter) *SearchQuery {
	return &SearchQuery{
		AccountID:    accountID,
		ProjectID:    projectID,
		MinTimestamp: start,
		MaxTimestamp: end,
		TagFilterss:  tagFilterss,
	}
}

// TagFilter represents a single tag filter from SearchQuery.
type TagFilter struct {
	Key        []byte
	Value      []byte
	IsNegative bool
	IsRegexp   bool
}

// String returns string representation of tf.
func (tf *TagFilter) String() string {
	var bb bytesutil.ByteBuffer
	fmt.Fprintf(&bb, "{Key=%q, Value=%q, IsNegative: %v, IsRegexp: %v}", tf.Key, tf.Value, tf.IsNegative, tf.IsRegexp)
	return string(bb.B)
}

// Marshal appends marshaled tf to dst and returns the result.
func (tf *TagFilter) Marshal(dst []byte) []byte {
	dst = encoding.MarshalBytes(dst, tf.Key)
	dst = encoding.MarshalBytes(dst, tf.Value)

	x := 0
	if tf.IsNegative {
		x = 2
	}
	if tf.IsRegexp {
		x |= 1
	}
	dst = append(dst, byte(x))

	return dst
}

// Unmarshal unmarshals tf from src and returns the tail.
func (tf *TagFilter) Unmarshal(src []byte) ([]byte, error) {  //解析一个过滤标签
	tail, k, err := encoding.UnmarshalBytes(src)  //  长度 + 内容 这样的格式
	if err != nil {
		return tail, fmt.Errorf("cannot unmarshal Key: %w", err)
	}
	tf.Key = append(tf.Key[:0], k...)
	src = tail

	tail, v, err := encoding.UnmarshalBytes(src)
	if err != nil {
		return tail, fmt.Errorf("cannot unmarshal Value: %w", err)
	}
	tf.Value = append(tf.Value[:0], v...)
	src = tail

	if len(src) < 1 {
		return src, fmt.Errorf("cannot unmarshal IsNegative+IsRegexp from empty src")
	}
	x := src[0]  // bit0-是否是不等于,  bit1-是否是正则表达式
	switch x {
	case 0:
		tf.IsNegative = false
		tf.IsRegexp = false
	case 1:
		tf.IsNegative = false
		tf.IsRegexp = true
	case 2:
		tf.IsNegative = true
		tf.IsRegexp = false
	case 3:
		tf.IsNegative = true
		tf.IsRegexp = true
	default:
		return src, fmt.Errorf("unexpected value for IsNegative+IsRegexp: %d; must be in the range [0..3]", x)
	}
	src = src[1:]

	return src, nil
}

// String returns string representation of the search query.
func (sq *SearchQuery) String() string {
	var bb bytesutil.ByteBuffer
	fmt.Fprintf(&bb, "AccountID=%d, ProjectID=%d, MinTimestamp=%s, MaxTimestamp=%s, TagFilters=[\n",
		sq.AccountID, sq.ProjectID, timestampToTime(sq.MinTimestamp), timestampToTime(sq.MaxTimestamp))
	for _, tagFilters := range sq.TagFilterss {
		for _, tf := range tagFilters {
			fmt.Fprintf(&bb, "%s", tf.String())
		}
		fmt.Fprintf(&bb, "\n")
	}
	fmt.Fprintf(&bb, "]")
	return string(bb.B)
}

// Marshal appends marshaled sq to dst and returns the result.
func (sq *SearchQuery) Marshal(dst []byte) []byte {
	dst = encoding.MarshalUint32(dst, sq.AccountID)
	dst = encoding.MarshalUint32(dst, sq.ProjectID)
	dst = encoding.MarshalVarInt64(dst, sq.MinTimestamp)
	dst = encoding.MarshalVarInt64(dst, sq.MaxTimestamp)
	dst = encoding.MarshalVarUint64(dst, uint64(len(sq.TagFilterss)))
	for _, tagFilters := range sq.TagFilterss {
		dst = encoding.MarshalVarUint64(dst, uint64(len(tagFilters)))
		for i := range tagFilters {
			dst = tagFilters[i].Marshal(dst)
		}
	}
	return dst
}

// Unmarshal unmarshals sq from src and returns the tail.
func (sq *SearchQuery) Unmarshal(src []byte) ([]byte, error) {
	if len(src) < 4 {
		return src, fmt.Errorf("cannot unmarshal AccountID: too short src len: %d; must be at least %d bytes", len(src), 4)
	}
	sq.AccountID = encoding.UnmarshalUint32(src)
	src = src[4:]

	if len(src) < 4 {
		return src, fmt.Errorf("cannot unmarshal ProjectID: too short src len: %d; must be at least %d bytes", len(src), 4)
	}
	sq.ProjectID = encoding.UnmarshalUint32(src)
	src = src[4:]

	tail, minTs, err := encoding.UnmarshalVarInt64(src)
	if err != nil {
		return src, fmt.Errorf("cannot unmarshal MinTimestamp: %w", err)
	}
	sq.MinTimestamp = minTs
	src = tail

	tail, maxTs, err := encoding.UnmarshalVarInt64(src)
	if err != nil {
		return src, fmt.Errorf("cannot unmarshal MaxTimestamp: %w", err)
	}
	sq.MaxTimestamp = maxTs
	src = tail

	tail, tfssCount, err := encoding.UnmarshalVarUint64(src)  //有N组过滤器
	if err != nil {
		return src, fmt.Errorf("cannot unmarshal the count of TagFilterss: %w", err)
	}
	if n := int(tfssCount) - cap(sq.TagFilterss); n > 0 {
		sq.TagFilterss = append(sq.TagFilterss[:cap(sq.TagFilterss)], make([][]TagFilter, n)...)
	}
	sq.TagFilterss = sq.TagFilterss[:tfssCount]
	src = tail

	for i := 0; i < int(tfssCount); i++ {
		tail, tfsCount, err := encoding.UnmarshalVarUint64(src)  //当前这一组的过滤标签数
		if err != nil {
			return src, fmt.Errorf("cannot unmarshal the count of TagFilters: %w", err)
		}
		src = tail

		tagFilters := sq.TagFilterss[i]
		if n := int(tfsCount) - cap(tagFilters); n > 0 {
			tagFilters = append(tagFilters[:cap(tagFilters)], make([]TagFilter, n)...)
		}
		tagFilters = tagFilters[:tfsCount]
		for j := 0; j < int(tfsCount); j++ {  //当前这一组的标签
			tail, err := tagFilters[j].Unmarshal(src)
			if err != nil {
				return tail, fmt.Errorf("cannot unmarshal TagFilter #%d: %w", j, err)
			}
			src = tail
		}
		sq.TagFilterss[i] = tagFilters
	}

	return src, nil
}

func checkSearchDeadlineAndPace(deadline uint64) error {
	if fasttime.UnixTimestamp() > deadline {
		return ErrDeadlineExceeded
	}
	storagepacelimiter.Search.WaitIfNeeded()
	return nil
}

const (
	paceLimiterFastIterationsMask   = 1<<16 - 1
	paceLimiterMediumIterationsMask = 1<<14 - 1
	paceLimiterSlowIterationsMask   = 1<<12 - 1
)
