package storage

import (
	"bytes"
	"container/heap"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/VictoriaMetrics/fastcache"
	xxhash "github.com/cespare/xxhash/v2"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/uint64set"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/workingsetcache"
)

const ( // 非常重要。以下应该是索引的类型。整个mergeset中存放了下列七种索引
	// Prefix for MetricName->TSID entries.
	nsPrefixMetricNameToTSID = 0 // func (db *indexDB) createIndexes 中使用了此类型

	// Prefix for Tag->MetricID entries.
	nsPrefixTagToMetricIDs = 1 // func (db *indexDB) createIndexes 中使用了此类型

	// Prefix for MetricID->TSID entries.
	nsPrefixMetricIDToTSID = 2 // func (db *indexDB) createIndexes 中使用了此类型

	// Prefix for MetricID->MetricName entries.
	nsPrefixMetricIDToMetricName = 3 // func (db *indexDB) createIndexes 中使用了此类型

	// Prefix for deleted MetricID entries.
	nsPrefixDeletedMetricID = 4 // func deleteMetricIDs中使用了此类型

	// Prefix for Date->MetricID entries.
	nsPrefixDateToMetricID = 5 // 插入数据部分后，再创建此索引

	// Prefix for (Date,Tag)->MetricID entries.
	nsPrefixDateTagToMetricIDs = 6 //  插入数据部分后，再创建此索引
)

// indexDB represents an index db.
type indexDB struct {
	// Atomic counters must go at the top of the structure in order to properly align by 8 bytes on 32-bit archs.
	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/212 .

	refCount uint64 // 以引用计数的方式来管理这个对象

	// The counter for newly created time series. It can be used for determining time series churn rate.
	newTimeseriesCreated uint64

	// The number of missing MetricID -> TSID entries.
	// High rate for this value means corrupted indexDB.
	missingTSIDsForMetricID uint64

	// The number of calls for date range searches.
	dateRangeSearchCalls uint64

	// The number of hits for date range searches.
	dateRangeSearchHits uint64

	// The number of calls for global search.
	globalSearchCalls uint64

	// missingMetricNamesForMetricID is a counter of missing MetricID -> MetricName entries.
	// High rate may mean corrupted indexDB due to unclean shutdown.
	// The db must be automatically recovered after that.
	missingMetricNamesForMetricID uint64

	mustDrop uint64 // 当切换indexdb的时候，prev的数据库会被打上这个标记

	name string          // unixnano()原子加后的十六进制值
	tb   *mergeset.Table // table 对象

	extDB     *indexDB // 存储前一个31天的索引
	extDBLock sync.Mutex

	// Cache for fast TagFilters -> TSIDs lookup.
	tagFiltersCache *workingsetcache.Cache // key 是序列化后的搜索表达式，value是tsid数组

	// The parent storage.
	s *Storage

	// Cache for (date, tagFilter) -> loopsCount, which is used for reducing
	// the amount of work when matching a set of filters.
	loopsPerDateTagFilterCache *workingsetcache.Cache

	indexSearchPool sync.Pool
}

// openIndexDB opens index db from the given path with the given caches.
func openIndexDB(path string, s *Storage) (*indexDB, error) { // 选取其中一个时间段的索引目录，然后打开
	if s == nil {
		logger.Panicf("BUG: Storage must be nin-nil")
	}
	// 主要使用 merge tree 这个数据结构，来提供索引结构
	tb, err := mergeset.OpenTable(path, invalidateTagFiltersCache, mergeTagToMetricIDsRows)
	if err != nil {
		return nil, fmt.Errorf("cannot open indexDB %q: %w", path, err)
	}

	name := filepath.Base(path)

	// Do not persist tagFiltersCache in files, since it is very volatile.
	mem := memory.Allowed()

	db := &indexDB{
		refCount: 1,
		tb:       tb,
		name:     name,

		tagFiltersCache:            workingsetcache.New(mem/32, time.Hour), // 使用 fastcache
		s:                          s,
		loopsPerDateTagFilterCache: workingsetcache.New(mem/128, time.Hour), // 使用 fastcache
	}
	return db, nil
}

const noDeadline = 1<<64 - 1

// IndexDBMetrics contains essential metrics for indexDB.
type IndexDBMetrics struct { // 这个结构用来做监控上报的。自监控
	TagFiltersCacheSize         uint64
	TagFiltersCacheSizeBytes    uint64
	TagFiltersCacheSizeMaxBytes uint64
	TagFiltersCacheRequests     uint64
	TagFiltersCacheMisses       uint64

	DeletedMetricsCount uint64

	IndexDBRefCount uint64

	NewTimeseriesCreated    uint64 // 全新的time series有多少？
	MissingTSIDsForMetricID uint64

	RecentHourMetricIDsSearchCalls uint64
	RecentHourMetricIDsSearchHits  uint64

	DateRangeSearchCalls uint64
	DateRangeSearchHits  uint64
	GlobalSearchCalls    uint64

	MissingMetricNamesForMetricID uint64

	IndexBlocksWithMetricIDsProcessed      uint64
	IndexBlocksWithMetricIDsIncorrectOrder uint64

	MinTimestampForCompositeIndex     uint64
	CompositeFilterSuccessConversions uint64
	CompositeFilterMissingConversions uint64

	mergeset.TableMetrics
}

func (db *indexDB) scheduleToDrop() {
	atomic.AddUint64(&db.mustDrop, 1)
}

// UpdateMetrics updates m with metrics from the db.
func (db *indexDB) UpdateMetrics(m *IndexDBMetrics) {
	var cs fastcache.Stats

	cs.Reset()
	db.tagFiltersCache.UpdateStats(&cs)
	m.TagFiltersCacheSize += cs.EntriesCount
	m.TagFiltersCacheSizeBytes += cs.BytesSize
	m.TagFiltersCacheSizeMaxBytes += cs.MaxBytesSize
	m.TagFiltersCacheRequests += cs.GetCalls
	m.TagFiltersCacheMisses += cs.Misses

	m.DeletedMetricsCount += uint64(db.s.getDeletedMetricIDs().Len())

	m.IndexDBRefCount += atomic.LoadUint64(&db.refCount)
	m.NewTimeseriesCreated += atomic.LoadUint64(&db.newTimeseriesCreated)
	m.MissingTSIDsForMetricID += atomic.LoadUint64(&db.missingTSIDsForMetricID)

	m.DateRangeSearchCalls += atomic.LoadUint64(&db.dateRangeSearchCalls)
	m.DateRangeSearchHits += atomic.LoadUint64(&db.dateRangeSearchHits)
	m.GlobalSearchCalls += atomic.LoadUint64(&db.globalSearchCalls)

	m.MissingMetricNamesForMetricID += atomic.LoadUint64(&db.missingMetricNamesForMetricID)

	m.IndexBlocksWithMetricIDsProcessed = atomic.LoadUint64(&indexBlocksWithMetricIDsProcessed)
	m.IndexBlocksWithMetricIDsIncorrectOrder = atomic.LoadUint64(&indexBlocksWithMetricIDsIncorrectOrder)

	m.MinTimestampForCompositeIndex = uint64(db.s.minTimestampForCompositeIndex)
	m.CompositeFilterSuccessConversions = atomic.LoadUint64(&compositeFilterSuccessConversions)
	m.CompositeFilterMissingConversions = atomic.LoadUint64(&compositeFilterMissingConversions)

	db.tb.UpdateMetrics(&m.TableMetrics)
	db.doExtDB(func(extDB *indexDB) {
		extDB.tb.UpdateMetrics(&m.TableMetrics)
		m.IndexDBRefCount += atomic.LoadUint64(&extDB.refCount)
	})
}

func (db *indexDB) doExtDB(f func(extDB *indexDB)) bool { // 传入某个执行函数。确保线程安全的引用对象
	db.extDBLock.Lock()
	extDB := db.extDB
	if extDB != nil {
		extDB.incRef()
	}
	db.extDBLock.Unlock()
	if extDB == nil {
		return false
	}
	f(extDB) // 使用引用计数的方法来获取对象，确保并发期间总是获得有效的对象
	extDB.decRef()
	return true
}

// SetExtDB sets external db to search.
//
// It decrements refCount for the previous extDB.
func (db *indexDB) SetExtDB(extDB *indexDB) {
	db.extDBLock.Lock()
	prevExtDB := db.extDB
	db.extDB = extDB
	db.extDBLock.Unlock()

	if prevExtDB != nil {
		prevExtDB.decRef()
	}
}

// MustClose closes db.
func (db *indexDB) MustClose() {
	db.decRef()
}

func (db *indexDB) incRef() {
	atomic.AddUint64(&db.refCount, 1)
}

func (db *indexDB) decRef() {
	n := atomic.AddUint64(&db.refCount, ^uint64(0))
	if int64(n) < 0 {
		logger.Panicf("BUG: negative refCount: %d", n)
	}
	if n > 0 {
		return
	}

	tbPath := db.tb.Path()
	db.tb.MustClose()
	db.SetExtDB(nil)

	// Free space occupied by caches owned by db.
	db.tagFiltersCache.Stop()
	db.loopsPerDateTagFilterCache.Stop()

	db.tagFiltersCache = nil
	db.s = nil
	db.loopsPerDateTagFilterCache = nil

	if atomic.LoadUint64(&db.mustDrop) == 0 {
		return
	}

	logger.Infof("dropping indexDB %q", tbPath)
	fs.MustRemoveAll(tbPath) // 如果 mustDrop 为 1，删除这个31天为周期的目录
	logger.Infof("indexDB %q has been dropped", tbPath)
}

func (db *indexDB) getFromTagFiltersCache(key []byte) ([]TSID, bool) { // 根据序列化的搜索表达式，在cache中查找TSID
	compressedBuf := tagBufPool.Get()
	defer tagBufPool.Put(compressedBuf)
	compressedBuf.B = db.tagFiltersCache.GetBig(compressedBuf.B[:0], key)
	if len(compressedBuf.B) == 0 {
		return nil, false
	}
	buf := tagBufPool.Get()
	defer tagBufPool.Put(buf)
	var err error
	buf.B, err = encoding.DecompressZSTD(buf.B[:0], compressedBuf.B)
	if err != nil {
		logger.Panicf("FATAL: cannot decompress tsids from tagFiltersCache: %s", err)
	}
	tsids, err := unmarshalTSIDs(nil, buf.B)
	if err != nil {
		logger.Panicf("FATAL: cannot unmarshal tsids from tagFiltersCache: %s", err)
	}
	return tsids, true
}

var tagBufPool bytesutil.ByteBufferPool

func (db *indexDB) putToTagFiltersCache(tsids []TSID, key []byte) { // 写入表达式cache
	buf := tagBufPool.Get()
	buf.B = marshalTSIDs(buf.B[:0], tsids)
	compressedBuf := tagBufPool.Get()
	compressedBuf.B = encoding.CompressZSTDLevel(compressedBuf.B[:0], buf.B, 1)
	tagBufPool.Put(buf)
	db.tagFiltersCache.SetBig(key, compressedBuf.B)
	tagBufPool.Put(compressedBuf)
}

func (db *indexDB) getFromMetricIDCache(dst *TSID, metricID uint64) error {
	// There is no need in prefixing the key with (accountID, projectID),
	// since metricID is globally unique across all (accountID, projectID) values.
	// See getUniqueUint64.

	// There is no need in checking for deleted metricIDs here, since they
	// must be checked by the caller.
	buf := (*[unsafe.Sizeof(*dst)]byte)(unsafe.Pointer(dst))
	key := (*[unsafe.Sizeof(metricID)]byte)(unsafe.Pointer(&metricID))
	tmp := db.s.metricIDCache.Get(buf[:0], key[:])
	if len(tmp) == 0 {
		// The TSID for the given metricID wasn't found in the cache.
		return io.EOF
	}
	if &tmp[0] != &buf[0] || len(tmp) != len(buf) {
		return fmt.Errorf("corrupted MetricID->TSID cache: unexpected size for metricID=%d value; got %d bytes; want %d bytes", metricID, len(tmp), len(buf))
	}
	return nil
}

func (db *indexDB) putToMetricIDCache(metricID uint64, tsid *TSID) {
	buf := (*[unsafe.Sizeof(*tsid)]byte)(unsafe.Pointer(tsid))
	key := (*[unsafe.Sizeof(metricID)]byte)(unsafe.Pointer(&metricID))
	db.s.metricIDCache.Set(key[:], buf[:])
}

func (db *indexDB) getMetricNameFromCache(dst []byte, metricID uint64) []byte {
	// There is no need in prefixing the key with (accountID, projectID),
	// since metricID is globally unique across all (accountID, projectID) values.
	// See getUniqueUint64.

	// There is no need in checking for deleted metricIDs here, since they
	// must be checked by the caller.
	key := (*[unsafe.Sizeof(metricID)]byte)(unsafe.Pointer(&metricID))
	return db.s.metricNameCache.Get(dst, key[:])
}

func (db *indexDB) putMetricNameToCache(metricID uint64, metricName []byte) {
	key := (*[unsafe.Sizeof(metricID)]byte)(unsafe.Pointer(&metricID))
	db.s.metricNameCache.Set(key[:], metricName)
}

func marshalTagFiltersKey(dst []byte, tfss []*TagFilters, tr TimeRange,
	versioned bool) []byte { // 把当次的搜索表达式序列化到一个buffer中
	prefix := ^uint64(0)
	if versioned {
		prefix = atomic.LoadUint64(&tagFiltersKeyGen) // tagFiltersKeyGen每10秒加1，通过这个来使cache过期
	}
	// Round start and end times to per-day granularity according to per-day inverted index.
	startDate := uint64(tr.MinTimestamp) / msecPerDay
	endDate := uint64(tr.MaxTimestamp) / msecPerDay
	dst = encoding.MarshalUint64(dst, prefix) // versioned为true的时候，这里的prefix每10秒变化一次
	dst = encoding.MarshalUint64(dst, startDate)
	dst = encoding.MarshalUint64(dst, endDate)
	if len(tfss) == 0 {
		return dst
	}
	dst = encoding.MarshalUint32(dst, tfss[0].accountID)
	dst = encoding.MarshalUint32(dst, tfss[0].projectID)
	for _, tfs := range tfss {
		dst = append(dst, 0) // separator between tfs groups.
		for i := range tfs.tfs {
			dst = tfs.tfs[i].MarshalNoAccountIDProjectID(dst)
		}
	}
	return dst
}

func invalidateTagFiltersCache() { // table上的回调函数，10秒执行一次。这个用来使表达式cache过期
	// This function must be fast, since it is called each
	// time new timeseries is added.
	atomic.AddUint64(&tagFiltersKeyGen, 1)
}

var tagFiltersKeyGen uint64 // 记录cache是第几代

func marshalTSIDs(dst []byte, tsids []TSID) []byte {
	dst = encoding.MarshalUint64(dst, uint64(len(tsids)))
	for i := range tsids {
		dst = tsids[i].Marshal(dst)
	}
	return dst
}

func unmarshalTSIDs(dst []TSID, src []byte) ([]TSID, error) {
	if len(src) < 8 {
		return dst, fmt.Errorf("cannot unmarshal the number of tsids from %d bytes; require at least %d bytes", len(src), 8)
	}
	n := encoding.UnmarshalUint64(src)
	src = src[8:]
	dstLen := len(dst)
	if nn := dstLen + int(n) - cap(dst); nn > 0 {
		dst = append(dst[:cap(dst)], make([]TSID, nn)...)
	}
	dst = dst[:dstLen+int(n)]
	for i := 0; i < int(n); i++ {
		tail, err := dst[dstLen+i].Unmarshal(src)
		if err != nil {
			return dst, fmt.Errorf("cannot unmarshal tsid #%d out of %d: %w", i, n, err)
		}
		src = tail
	}
	if len(src) > 0 {
		return dst, fmt.Errorf("non-zero tail left after unmarshaling %d tsids; len(tail)=%d", n, len(src))
	}
	return dst, nil
}

// getTSIDByNameNoCreate fills the dst with TSID for the given metricName.
//
// It returns io.EOF if the given mn isn't found locally.
func (db *indexDB) getTSIDByNameNoCreate(dst *TSID, metricName []byte) error {
	is := db.getIndexSearch(0, 0, noDeadline) // 从内存池获取 index search 对象
	err := is.getTSIDByMetricName(dst, metricName)
	db.putIndexSearch(is) // 放回内存池
	if err == nil {
		return nil
	}
	if err != io.EOF {
		return fmt.Errorf("cannot search TSID by MetricName %q: %w", metricName, err)
	}

	// Do not search for the TSID in the external storage,
	// since this function is already called by another indexDB instance.

	// The TSID for the given mn wasn't found.
	return io.EOF
}

type indexSearch struct {
	db *indexDB             // 父对象
	ts mergeset.TableSearch // index search 对象中包含table search对象
	kb bytesutil.ByteBuffer
	mp tagToMetricIDsRowParser

	accountID uint32
	projectID uint32

	// deadline in unix timestamp seconds for the given search.
	deadline uint64 // 超时时间

	// tsidByNameMisses and tsidByNameSkips is used for a performance
	// hack in GetOrCreateTSIDByName. See the comment there.
	tsidByNameMisses int
	tsidByNameSkips  int
}

// GetOrCreateTSIDByName fills the dst with TSID for the given metricName.  // 为新的 time series创建tsid
func (is *indexSearch) GetOrCreateTSIDByName(dst *TSID, metricName []byte) error {
	// A hack: skip searching for the TSID after many serial misses.
	// This should improve insertion performance for big batches
	// of new time series.
	if is.tsidByNameMisses < 100 {
		err := is.getTSIDByMetricName(dst, metricName)
		if err == nil {
			is.tsidByNameMisses = 0
			return nil
		}
		if err != io.EOF {
			return fmt.Errorf("cannot search TSID by MetricName %q: %w", metricName, err)
		}
		is.tsidByNameMisses++
	} else {
		is.tsidByNameSkips++
		if is.tsidByNameSkips > 10000 {
			is.tsidByNameSkips = 0
			is.tsidByNameMisses = 0
		}
	}

	// TSID for the given name wasn't found. Create it.
	// It is OK if duplicate TSID for mn is created by concurrent goroutines.
	// Metric results will be merged by mn after TableSearch.
	if err := is.db.createTSIDByName(dst, metricName); err != nil { // 完全新的time series，要创建，并生成tsid
		return fmt.Errorf("cannot create TSID by MetricName %q: %w", metricName, err)
	}
	return nil
}

func (db *indexDB) getIndexSearch(accountID, projectID uint32, deadline uint64) *indexSearch {
	v := db.indexSearchPool.Get() // 搜索的时候，从对象池获取一个 index search 对象
	if v == nil {
		v = &indexSearch{
			db: db,
		}
	}
	is := v.(*indexSearch)
	is.ts.Init(db.tb) // 对其中的 table search 对象进行初始化
	is.accountID = accountID
	is.projectID = projectID
	is.deadline = deadline
	return is
}

func (db *indexDB) putIndexSearch(is *indexSearch) {
	is.ts.MustClose()
	is.kb.Reset()
	is.mp.Reset()
	is.accountID = 0
	is.projectID = 0
	is.deadline = 0

	// Do not reset tsidByNameMisses and tsidByNameSkips,
	// since they are used in GetOrCreateTSIDByName across call boundaries.

	db.indexSearchPool.Put(is)
}

func (db *indexDB) createTSIDByName(dst *TSID, metricName []byte) error { // 为具体的某个time series创建tsid
	mn := GetMetricName()
	defer PutMetricName(mn)
	if err := mn.Unmarshal(metricName); err != nil { // 反序列化 time series
		return fmt.Errorf("cannot unmarshal metricName %q: %w", metricName, err)
	}

	if err := db.generateTSID(dst, metricName, mn); err != nil {
		return fmt.Errorf("cannot generate TSID: %w", err)
	}
	if err := db.createIndexes(dst, mn); err != nil { // 为新的监控项创建索引
		return fmt.Errorf("cannot create indexes: %w", err)
	}

	// There is no need in invalidating tag cache, since it is invalidated
	// on db.tb flush via invalidateTagFiltersCache flushCallback passed to OpenTable.
	atomic.AddUint64(&db.newTimeseriesCreated, 1)
	if logNewSeries {
		logger.Infof("new series created: %s", mn.String())
	}
	return nil
}

// SetLogNewSeries updates new series logging.
//
// This function must be called before any calling any storage functions.
func SetLogNewSeries(ok bool) {
	logNewSeries = ok
}

var logNewSeries = false

func (db *indexDB) generateTSID(dst *TSID, metricName []byte, mn *MetricName) error { // 为新的 time series创建tsid
	// Search the TSID in the external storage.  // metricName 是未decode之前的完整数据, mn是decode后的数据
	// This is usually the db from the previous period.  // dst 是 out 参数
	var err error
	if db.doExtDB(func(extDB *indexDB) { // 前一个31天的索引数据
		err = extDB.getTSIDByNameNoCreate(dst, metricName)
	}) { // 如果存在 prev db
		if err == nil {
			// The TSID has been found in the external storage.
			return nil
		}
		if err != io.EOF {
			return fmt.Errorf("external search failed: %w", err)
		} //  err==io.EOF， 说明在 prev db没有搜索到相同的 time series
	}

	// The TSID wasn't found in the external storage.
	// Generate it locally.
	dst.AccountID = mn.AccountID
	dst.ProjectID = mn.ProjectID
	dst.MetricGroupID = xxhash.Sum64(mn.MetricGroup) // 以 __name__ 来取hash值
	if len(mn.Tags) > 0 {
		dst.JobID = uint32(xxhash.Sum64(mn.Tags[0].Value)) // todo:这个写法太牵强了。排序后，这个字段不一定是job id
	}
	if len(mn.Tags) > 1 {
		dst.InstanceID = uint32(xxhash.Sum64(mn.Tags[1].Value)) // todo:太不严谨了
	}
	dst.MetricID = generateUniqueMetricID() // 通过原子加来产生唯一的 metricID
	return nil
}

func (db *indexDB) createIndexes(tsid *TSID, mn *MetricName) error { // 计算得到新的TSID后，创建索引
	// The order of index items is important.
	// It guarantees index consistency.  // 它保证了索引的一致性

	ii := getIndexItems() // 从内存池获取 indexItems 对象.
	defer putIndexItems(ii)

	// Create MetricName -> TSID index.
	ii.B = append(ii.B, nsPrefixMetricNameToTSID) // 这个字节表示索引的类型
	ii.B = mn.Marshal(ii.B)                       // 整个metric序列化以后的数据
	ii.B = append(ii.B, kvSeparatorChar)
	ii.B = tsid.Marshal(ii.B) // 上面把数据序列化为存储要求的格式
	ii.Next()                 // 产生一个新的item

	// Create MetricID -> MetricName index.
	ii.B = marshalCommonPrefix(ii.B, nsPrefixMetricIDToMetricName, mn.AccountID, mn.ProjectID)
	ii.B = encoding.MarshalUint64(ii.B, tsid.MetricID)
	ii.B = mn.Marshal(ii.B) // todo:这里的序列化值得优化
	ii.Next()

	// Create MetricID -> TSID index.
	ii.B = marshalCommonPrefix(ii.B, nsPrefixMetricIDToTSID, mn.AccountID, mn.ProjectID)
	ii.B = encoding.MarshalUint64(ii.B, tsid.MetricID)
	ii.B = tsid.Marshal(ii.B)
	ii.Next()

	prefix := kbPool.Get() // ByteBufferPool
	prefix.B = marshalCommonPrefix(prefix.B[:0], nsPrefixTagToMetricIDs, mn.AccountID, mn.ProjectID)
	ii.registerTagIndexes(prefix.B, mn, tsid.MetricID) // 建立了好几种不同类型的索引
	kbPool.Put(prefix)

	return db.tb.AddItems(ii.Items) // 把多个索引放到 indexItem对象中，然后发给table对象
}

type indexItems struct { // 相当于把所有的主键都集中在一起存放. 这个类用于索引的序列化
	B     []byte   // 这是一个大数组，用于顺序的存放多个 time series的数据
	Items [][]byte // 这个结构引用上面的数据

	start int // 记录在 B 中插入的位置
}

func (ii *indexItems) reset() {
	ii.B = ii.B[:0]
	ii.Items = ii.Items[:0]
	ii.start = 0
}

func (ii *indexItems) Next() {
	ii.Items = append(ii.Items, ii.B[ii.start:]) // 把当前游标追加的数据作为一个item，然后游标移动到下一个追加位置
	ii.start = len(ii.B)
}

func getIndexItems() *indexItems {
	v := indexItemsPool.Get()
	if v == nil {
		return &indexItems{}
	}
	return v.(*indexItems)
}

func putIndexItems(ii *indexItems) {
	ii.reset()
	indexItemsPool.Put(ii)
}

var indexItemsPool sync.Pool

// SearchTagKeysOnTimeRange returns all the tag keys on the given tr.
func (db *indexDB) SearchTagKeysOnTimeRange(accountID, projectID uint32, tr TimeRange, maxTagKeys int,
	deadline uint64) ([]string, error) {
	tks := make(map[string]struct{})
	is := db.getIndexSearch(accountID, projectID, deadline)
	err := is.searchTagKeysOnTimeRange(tks, tr, maxTagKeys)
	db.putIndexSearch(is)
	if err != nil {
		return nil, err
	}

	ok := db.doExtDB(func(extDB *indexDB) {
		is := extDB.getIndexSearch(accountID, projectID, deadline)
		err = is.searchTagKeysOnTimeRange(tks, tr, maxTagKeys)
		extDB.putIndexSearch(is)
	})
	if ok && err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(tks))
	for key := range tks {
		// Do not skip empty keys, since they are converted to __name__
		keys = append(keys, key)
	}
	// Do not sort keys, since they must be sorted by vmselect.
	return keys, nil
}

func (is *indexSearch) searchTagKeysOnTimeRange(tks map[string]struct{}, tr TimeRange, maxTagKeys int) error {
	minDate := uint64(tr.MinTimestamp) / msecPerDay
	maxDate := uint64(tr.MaxTimestamp) / msecPerDay
	if minDate > maxDate || maxDate-minDate > maxDaysForPerDaySearch {
		return is.searchTagKeys(tks, maxTagKeys)
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	var errGlobal error
	for date := minDate; date <= maxDate; date++ {
		wg.Add(1)
		go func(date uint64) {
			defer wg.Done()
			tksLocal := make(map[string]struct{})
			isLocal := is.db.getIndexSearch(is.accountID, is.projectID, is.deadline)
			err := isLocal.searchTagKeysOnDate(tksLocal, date, maxTagKeys)
			is.db.putIndexSearch(isLocal)
			mu.Lock()
			defer mu.Unlock()
			if errGlobal != nil {
				return
			}
			if err != nil {
				errGlobal = err
				return
			}
			if len(tks) >= maxTagKeys {
				return
			}
			for k := range tksLocal {
				tks[k] = struct{}{}
			}
		}(date)
	}
	wg.Wait()
	return errGlobal
}

func (is *indexSearch) searchTagKeysOnDate(tks map[string]struct{}, date uint64, maxTagKeys int) error {
	ts := &is.ts
	kb := &is.kb
	mp := &is.mp
	mp.Reset()
	dmis := is.db.s.getDeletedMetricIDs()
	loopsPaceLimiter := 0
	kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixDateTagToMetricIDs)
	kb.B = encoding.MarshalUint64(kb.B, date)
	prefix := kb.B
	ts.Seek(prefix)
	for len(tks) < maxTagKeys && ts.NextItem() {
		if loopsPaceLimiter&paceLimiterFastIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
				return err
			}
		}
		loopsPaceLimiter++
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			break
		}
		if err := mp.Init(item, nsPrefixDateTagToMetricIDs); err != nil {
			return err
		}
		if mp.IsDeletedTag(dmis) {
			continue
		}
		key := mp.Tag.Key
		if isArtificialTagKey(key) {
			// Skip artificially created tag key.
			continue
		}
		// Store tag key.
		tks[string(key)] = struct{}{}

		// Search for the next tag key.
		// The last char in kb.B must be tagSeparatorChar.
		// Just increment it in order to jump to the next tag key.
		kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixDateTagToMetricIDs)
		kb.B = encoding.MarshalUint64(kb.B, date)
		kb.B = marshalTagValue(kb.B, key)
		kb.B[len(kb.B)-1]++
		ts.Seek(kb.B)
	}
	if err := ts.Error(); err != nil {
		return fmt.Errorf("error during search for prefix %q: %w", prefix, err)
	}
	return nil
}

// SearchTagKeys returns all the tag keys for the given accountID, projectID.
func (db *indexDB) SearchTagKeys(accountID, projectID uint32, maxTagKeys int, deadline uint64) ([]string, error) {
	tks := make(map[string]struct{})

	is := db.getIndexSearch(accountID, projectID, deadline)
	err := is.searchTagKeys(tks, maxTagKeys)
	db.putIndexSearch(is)
	if err != nil {
		return nil, err
	}

	ok := db.doExtDB(func(extDB *indexDB) {
		is := extDB.getIndexSearch(accountID, projectID, deadline)
		err = is.searchTagKeys(tks, maxTagKeys)
		extDB.putIndexSearch(is)
	})
	if ok && err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(tks))
	for key := range tks {
		// Do not skip empty keys, since they are converted to __name__
		keys = append(keys, key)
	}
	// Do not sort keys, since they must be sorted by vmselect.
	return keys, nil
}

func (is *indexSearch) searchTagKeys(tks map[string]struct{}, maxTagKeys int) error {
	ts := &is.ts
	kb := &is.kb
	mp := &is.mp
	mp.Reset()
	dmis := is.db.s.getDeletedMetricIDs()
	loopsPaceLimiter := 0
	kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixTagToMetricIDs)
	prefix := kb.B
	ts.Seek(prefix) // 相当于是找到所有这个类型的块，然后在后续的遍历中去匹配
	for len(tks) < maxTagKeys && ts.NextItem() {
		if loopsPaceLimiter&paceLimiterFastIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
				return err
			}
		}
		loopsPaceLimiter++
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			break
		}
		if err := mp.Init(item, nsPrefixTagToMetricIDs); err != nil {
			return err
		}
		if mp.IsDeletedTag(dmis) {
			continue
		}
		key := mp.Tag.Key
		if isArtificialTagKey(key) {
			// Skip artificailly created tag keys.
			continue
		}
		// Store tag key.
		tks[string(key)] = struct{}{}

		// Search for the next tag key.
		// The last char in kb.B must be tagSeparatorChar.
		// Just increment it in order to jump to the next tag key.
		kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixTagToMetricIDs)
		kb.B = marshalTagValue(kb.B, key)
		kb.B[len(kb.B)-1]++
		ts.Seek(kb.B) // 丑陋的代码，为什么这个地方又搞个seek ???
	}
	if err := ts.Error(); err != nil {
		return fmt.Errorf("error during search for prefix %q: %w", prefix, err)
	}
	return nil
}

// SearchTagValuesOnTimeRange returns all the tag values for the given tagKey on tr.
func (db *indexDB) SearchTagValuesOnTimeRange(accountID, projectID uint32, tagKey []byte, tr TimeRange,
	maxTagValues int, deadline uint64) ([]string, error) {
	tvs := make(map[string]struct{})
	is := db.getIndexSearch(accountID, projectID, deadline)
	err := is.searchTagValuesOnTimeRange(tvs, tagKey, tr, maxTagValues)
	db.putIndexSearch(is)
	if err != nil {
		return nil, err
	}
	ok := db.doExtDB(func(extDB *indexDB) {
		is := extDB.getIndexSearch(accountID, projectID, deadline)
		err = is.searchTagValuesOnTimeRange(tvs, tagKey, tr, maxTagValues)
		extDB.putIndexSearch(is)
	})
	if ok && err != nil {
		return nil, err
	}

	tagValues := make([]string, 0, len(tvs))
	for tv := range tvs {
		if len(tv) == 0 {
			// Skip empty values, since they have no any meaning.
			// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/600
			continue
		}
		tagValues = append(tagValues, tv)
	}
	// Do not sort tagValues, since they must be sorted by vmselect.
	return tagValues, nil
}

func (is *indexSearch) searchTagValuesOnTimeRange(tvs map[string]struct{}, tagKey []byte, tr TimeRange,
	maxTagValues int) error {
	minDate := uint64(tr.MinTimestamp) / msecPerDay
	maxDate := uint64(tr.MaxTimestamp) / msecPerDay
	if minDate > maxDate || maxDate-minDate > maxDaysForPerDaySearch {
		return is.searchTagValues(tvs, tagKey, maxTagValues)
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	var errGlobal error
	for date := minDate; date <= maxDate; date++ {
		wg.Add(1)
		go func(date uint64) {
			defer wg.Done()
			tvsLocal := make(map[string]struct{})
			isLocal := is.db.getIndexSearch(is.accountID, is.projectID, is.deadline)
			err := isLocal.searchTagValuesOnDate(tvsLocal, tagKey, date, maxTagValues)
			is.db.putIndexSearch(isLocal)
			mu.Lock()
			defer mu.Unlock()
			if errGlobal != nil {
				return
			}
			if err != nil {
				errGlobal = err
				return
			}
			if len(tvs) >= maxTagValues {
				return
			}
			for v := range tvsLocal {
				tvs[v] = struct{}{}
			}
		}(date)
	}
	wg.Wait()
	return errGlobal
}

func (is *indexSearch) searchTagValuesOnDate(tvs map[string]struct{}, tagKey []byte, date uint64,
	maxTagValues int) error {
	ts := &is.ts
	kb := &is.kb
	mp := &is.mp
	mp.Reset()
	dmis := is.db.s.getDeletedMetricIDs()
	loopsPaceLimiter := 0
	kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixDateTagToMetricIDs)
	kb.B = encoding.MarshalUint64(kb.B, date)
	kb.B = marshalTagValue(kb.B, tagKey)
	prefix := kb.B
	ts.Seek(prefix)
	for len(tvs) < maxTagValues && ts.NextItem() {
		if loopsPaceLimiter&paceLimiterFastIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
				return err
			}
		}
		loopsPaceLimiter++
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			break
		}
		if err := mp.Init(item, nsPrefixDateTagToMetricIDs); err != nil {
			return err
		}
		if mp.IsDeletedTag(dmis) {
			continue
		}

		// Store tag value
		tvs[string(mp.Tag.Value)] = struct{}{}

		if mp.MetricIDsLen() < maxMetricIDsPerRow/2 {
			// There is no need in searching for the next tag value,
			// since it is likely it is located in the next row,
			// because the current row contains incomplete metricIDs set.
			continue
		}
		// Search for the next tag value.
		// The last char in kb.B must be tagSeparatorChar.
		// Just increment it in order to jump to the next tag value.
		kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixDateTagToMetricIDs)
		kb.B = encoding.MarshalUint64(kb.B, date)
		kb.B = marshalTagValue(kb.B, mp.Tag.Key)
		kb.B = marshalTagValue(kb.B, mp.Tag.Value)
		kb.B[len(kb.B)-1]++
		ts.Seek(kb.B)
	}
	if err := ts.Error(); err != nil {
		return fmt.Errorf("error when searching for tag name prefix %q: %w", prefix, err)
	}
	return nil
}

// SearchTagValues returns all the tag values for the given tagKey
func (db *indexDB) SearchTagValues(accountID, projectID uint32, tagKey []byte, maxTagValues int,
	deadline uint64) ([]string, error) {
	tvs := make(map[string]struct{})
	is := db.getIndexSearch(accountID, projectID, deadline)
	err := is.searchTagValues(tvs, tagKey, maxTagValues)
	db.putIndexSearch(is)
	if err != nil {
		return nil, err
	}
	ok := db.doExtDB(func(extDB *indexDB) {
		is := extDB.getIndexSearch(accountID, projectID, deadline)
		err = is.searchTagValues(tvs, tagKey, maxTagValues)
		extDB.putIndexSearch(is)
	})
	if ok && err != nil {
		return nil, err
	}

	tagValues := make([]string, 0, len(tvs))
	for tv := range tvs {
		if len(tv) == 0 {
			// Skip empty values, since they have no any meaning.
			// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/600
			continue
		}
		tagValues = append(tagValues, tv)
	}
	// Do not sort tagValues, since they must be sorted by vmselect.
	return tagValues, nil
}

func (is *indexSearch) searchTagValues(tvs map[string]struct{}, tagKey []byte, maxTagValues int) error {
	ts := &is.ts
	kb := &is.kb
	mp := &is.mp
	mp.Reset()
	dmis := is.db.s.getDeletedMetricIDs()
	loopsPaceLimiter := 0
	kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixTagToMetricIDs)
	kb.B = marshalTagValue(kb.B, tagKey)
	prefix := kb.B
	ts.Seek(prefix)
	for len(tvs) < maxTagValues && ts.NextItem() {
		if loopsPaceLimiter&paceLimiterFastIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
				return err
			}
		}
		loopsPaceLimiter++
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			break
		}
		if err := mp.Init(item, nsPrefixTagToMetricIDs); err != nil {
			return err
		}
		if mp.IsDeletedTag(dmis) {
			continue
		}

		// Store tag value
		tvs[string(mp.Tag.Value)] = struct{}{}

		if mp.MetricIDsLen() < maxMetricIDsPerRow/2 {
			// There is no need in searching for the next tag value,
			// since it is likely it is located in the next row,
			// because the current row contains incomplete metricIDs set.
			continue
		}
		// Search for the next tag value.
		// The last char in kb.B must be tagSeparatorChar.
		// Just increment it in order to jump to the next tag value.
		kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixTagToMetricIDs)
		kb.B = marshalTagValue(kb.B, mp.Tag.Key)
		kb.B = marshalTagValue(kb.B, mp.Tag.Value)
		kb.B[len(kb.B)-1]++
		ts.Seek(kb.B)
	}
	if err := ts.Error(); err != nil {
		return fmt.Errorf("error when searching for tag name prefix %q: %w", prefix, err)
	}
	return nil
}

// SearchTagValueSuffixes returns all the tag value suffixes for the given tagKey and tagValuePrefix on the given tr.
//
// This allows implementing https://graphite-api.readthedocs.io/en/latest/api.html#metrics-find or similar APIs.
//
// If it returns maxTagValueSuffixes suffixes, then it is likely more than maxTagValueSuffixes suffixes is found.
func (db *indexDB) SearchTagValueSuffixes(accountID, projectID uint32, tr TimeRange, tagKey, tagValuePrefix []byte,
	delimiter byte, maxTagValueSuffixes int, deadline uint64) ([]string, error) {
	// TODO: cache results?

	tvss := make(map[string]struct{})
	is := db.getIndexSearch(accountID, projectID, deadline)
	err := is.searchTagValueSuffixesForTimeRange(tvss, tr, tagKey, tagValuePrefix, delimiter, maxTagValueSuffixes)
	db.putIndexSearch(is)
	if err != nil {
		return nil, err
	}
	if len(tvss) < maxTagValueSuffixes {
		ok := db.doExtDB(func(extDB *indexDB) {
			is := extDB.getIndexSearch(accountID, projectID, deadline)
			err = is.searchTagValueSuffixesForTimeRange(tvss, tr, tagKey, tagValuePrefix, delimiter, maxTagValueSuffixes)
			extDB.putIndexSearch(is)
		})
		if ok && err != nil {
			return nil, err
		}
	}

	suffixes := make([]string, 0, len(tvss))
	for suffix := range tvss {
		// Do not skip empty suffixes, since they may represent leaf tag values.
		suffixes = append(suffixes, suffix)
	}
	if len(suffixes) > maxTagValueSuffixes {
		suffixes = suffixes[:maxTagValueSuffixes]
	}
	// Do not sort suffixes, since they must be sorted by vmselect.
	return suffixes, nil
}

func (is *indexSearch) searchTagValueSuffixesForTimeRange(tvss map[string]struct{}, tr TimeRange,
	tagKey, tagValuePrefix []byte, delimiter byte, maxTagValueSuffixes int) error {
	minDate := uint64(tr.MinTimestamp) / msecPerDay
	maxDate := uint64(tr.MaxTimestamp) / msecPerDay
	if minDate > maxDate || maxDate-minDate > maxDaysForPerDaySearch {
		return is.searchTagValueSuffixesAll(tvss, tagKey, tagValuePrefix, delimiter, maxTagValueSuffixes)
	}
	// Query over multiple days in parallel.
	var wg sync.WaitGroup
	var errGlobal error
	var mu sync.Mutex // protects tvss + errGlobal from concurrent access below.
	for minDate <= maxDate {
		wg.Add(1)
		go func(date uint64) {
			defer wg.Done()
			tvssLocal := make(map[string]struct{})
			isLocal := is.db.getIndexSearch(is.accountID, is.projectID, is.deadline)
			err := isLocal.searchTagValueSuffixesForDate(tvssLocal, date, tagKey, tagValuePrefix, delimiter, maxTagValueSuffixes)
			is.db.putIndexSearch(isLocal)
			mu.Lock()
			defer mu.Unlock()
			if errGlobal != nil {
				return
			}
			if err != nil {
				errGlobal = err
				return
			}
			if len(tvss) > maxTagValueSuffixes {
				return
			}
			for k := range tvssLocal {
				tvss[k] = struct{}{}
			}
		}(minDate)
		minDate++
	}
	wg.Wait()
	return errGlobal
}

func (is *indexSearch) searchTagValueSuffixesAll(tvss map[string]struct{}, tagKey, tagValuePrefix []byte,
	delimiter byte, maxTagValueSuffixes int) error {
	kb := &is.kb
	nsPrefix := byte(nsPrefixTagToMetricIDs)
	kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefix)
	kb.B = marshalTagValue(kb.B, tagKey)
	kb.B = marshalTagValue(kb.B, tagValuePrefix)
	kb.B = kb.B[:len(kb.B)-1] // remove tagSeparatorChar from the end of kb.B
	prefix := append([]byte(nil), kb.B...)
	return is.searchTagValueSuffixesForPrefix(tvss, nsPrefix, prefix, len(tagValuePrefix), delimiter, maxTagValueSuffixes)
}

func (is *indexSearch) searchTagValueSuffixesForDate(tvss map[string]struct{}, date uint64,
	tagKey, tagValuePrefix []byte, delimiter byte, maxTagValueSuffixes int) error {
	nsPrefix := byte(nsPrefixDateTagToMetricIDs)
	kb := &is.kb
	kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefix)
	kb.B = encoding.MarshalUint64(kb.B, date)
	kb.B = marshalTagValue(kb.B, tagKey)
	kb.B = marshalTagValue(kb.B, tagValuePrefix)
	kb.B = kb.B[:len(kb.B)-1] // remove tagSeparatorChar from the end of kb.B
	prefix := append([]byte(nil), kb.B...)
	return is.searchTagValueSuffixesForPrefix(tvss, nsPrefix, prefix, len(tagValuePrefix), delimiter, maxTagValueSuffixes)
}

func (is *indexSearch) searchTagValueSuffixesForPrefix(tvss map[string]struct{}, nsPrefix byte, prefix []byte,
	tagValuePrefixLen int, delimiter byte, maxTagValueSuffixes int) error {
	kb := &is.kb
	ts := &is.ts
	mp := &is.mp
	mp.Reset()
	dmis := is.db.s.getDeletedMetricIDs()
	loopsPaceLimiter := 0
	ts.Seek(prefix)
	for len(tvss) < maxTagValueSuffixes && ts.NextItem() {
		if loopsPaceLimiter&paceLimiterFastIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
				return err
			}
		}
		loopsPaceLimiter++
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			break
		}
		if err := mp.Init(item, nsPrefix); err != nil {
			return err
		}
		if mp.IsDeletedTag(dmis) {
			continue
		}
		tagValue := mp.Tag.Value
		suffix := tagValue[tagValuePrefixLen:]
		n := bytes.IndexByte(suffix, delimiter)
		if n < 0 {
			// Found leaf tag value that doesn't have delimiters after the given tagValuePrefix.
			tvss[string(suffix)] = struct{}{}
			continue
		}
		// Found non-leaf tag value. Extract suffix that end with the given delimiter.
		suffix = suffix[:n+1]
		tvss[string(suffix)] = struct{}{}
		if suffix[len(suffix)-1] == 255 {
			continue
		}
		// Search for the next suffix
		suffix[len(suffix)-1]++
		kb.B = append(kb.B[:0], prefix...)
		kb.B = marshalTagValue(kb.B, suffix)
		kb.B = kb.B[:len(kb.B)-1] // remove tagSeparatorChar
		ts.Seek(kb.B)
	}
	if err := ts.Error(); err != nil {
		return fmt.Errorf("error when searching for tag value sufixes for prefix %q: %w", prefix, err)
	}
	return nil
}

// GetSeriesCount returns the approximate number of unique timeseries for the given (accountID, projectID).
//
// It includes the deleted series too and may count the same series
// up to two times - in db and extDB.
func (db *indexDB) GetSeriesCount(accountID, projectID uint32, deadline uint64) (uint64, error) {
	is := db.getIndexSearch(accountID, projectID, deadline)
	n, err := is.getSeriesCount()
	db.putIndexSearch(is)
	if err != nil {
		return 0, err
	}

	var nExt uint64
	ok := db.doExtDB(func(extDB *indexDB) {
		is := extDB.getIndexSearch(accountID, projectID, deadline)
		nExt, err = is.getSeriesCount()
		extDB.putIndexSearch(is)
	})
	if ok && err != nil {
		return 0, fmt.Errorf("error when searching in extDB: %w", err)
	}
	return n + nExt, nil
}

func (is *indexSearch) getSeriesCount() (uint64, error) {
	ts := &is.ts
	kb := &is.kb
	mp := &is.mp
	loopsPaceLimiter := 0
	var metricIDsLen uint64
	// Extract the number of series from ((__name__=value): metricIDs) rows
	kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixTagToMetricIDs)
	kb.B = marshalTagValue(kb.B, nil)
	ts.Seek(kb.B)
	for ts.NextItem() {
		if loopsPaceLimiter&paceLimiterFastIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
				return 0, err
			}
		}
		loopsPaceLimiter++
		item := ts.Item
		if !bytes.HasPrefix(item, kb.B) {
			break
		}
		tail := item[len(kb.B):]
		n := bytes.IndexByte(tail, tagSeparatorChar)
		if n < 0 {
			return 0, fmt.Errorf("invalid tag->metricIDs line %q: cannot find tagSeparatorChar %d", item, tagSeparatorChar)
		}
		tail = tail[n+1:]
		if err := mp.InitOnlyTail(item, tail); err != nil {
			return 0, err
		}
		// Take into account deleted timeseries too.
		// It is OK if series can be counted multiple times in rare cases -
		// the returned number is an estimation.
		metricIDsLen += uint64(mp.MetricIDsLen())
	}
	if err := ts.Error(); err != nil {
		return 0, fmt.Errorf("error when counting unique timeseries: %w", err)
	}
	return metricIDsLen, nil
}

// GetTSDBStatusWithFiltersForDate returns topN entries for tsdb status for the given tfss, date, accountID and projectID.
func (db *indexDB) GetTSDBStatusWithFiltersForDate(accountID, projectID uint32, tfss []*TagFilters, date uint64,
	topN int, deadline uint64) (*TSDBStatus, error) {
	is := db.getIndexSearch(accountID, projectID, deadline)
	status, err := is.getTSDBStatusWithFiltersForDate(tfss, date, topN)
	db.putIndexSearch(is)
	if err != nil {
		return nil, err
	}
	if status.hasEntries() {
		return status, nil
	}
	ok := db.doExtDB(func(extDB *indexDB) {
		is := extDB.getIndexSearch(accountID, projectID, deadline)
		status, err = is.getTSDBStatusWithFiltersForDate(tfss, date, topN)
		extDB.putIndexSearch(is)
	})
	if ok && err != nil {
		return nil, fmt.Errorf("error when obtaining TSDB status from extDB: %w", err)
	}
	return status, nil
}

// getTSDBStatusWithFiltersForDate returns topN entries for tsdb status for the given tfss and the given date.
func (is *indexSearch) getTSDBStatusWithFiltersForDate(tfss []*TagFilters, date uint64, topN int) (*TSDBStatus, error) {
	var filter *uint64set.Set
	if len(tfss) > 0 {
		tr := TimeRange{
			MinTimestamp: int64(date) * msecPerDay,
			MaxTimestamp: int64(date+1) * msecPerDay,
		}
		metricIDs, err := is.searchMetricIDsInternal(tfss, tr, 2e9)
		if err != nil {
			return nil, err
		}
		if metricIDs.Len() == 0 {
			// Nothing found.
			return &TSDBStatus{}, nil
		}
		filter = metricIDs
	}

	ts := &is.ts
	kb := &is.kb
	mp := &is.mp
	thLabelValueCountByLabelName := newTopHeap(topN)
	thSeriesCountByLabelValuePair := newTopHeap(topN)
	thSeriesCountByMetricName := newTopHeap(topN)
	var tmp, labelName, labelNameValue []byte
	var labelValueCountByLabelName, seriesCountByLabelValuePair uint64
	nameEqualBytes := []byte("__name__=")

	loopsPaceLimiter := 0
	kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixDateTagToMetricIDs)
	kb.B = encoding.MarshalUint64(kb.B, date)
	prefix := kb.B
	ts.Seek(prefix)
	for ts.NextItem() {
		if loopsPaceLimiter&paceLimiterFastIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
				return nil, err
			}
		}
		loopsPaceLimiter++
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			break
		}
		matchingSeriesCount := 0
		if filter != nil {
			if err := mp.Init(item, nsPrefixDateTagToMetricIDs); err != nil {
				return nil, err
			}
			mp.ParseMetricIDs()
			for _, metricID := range mp.MetricIDs {
				if filter.Has(metricID) {
					matchingSeriesCount++
				}
			}
			if matchingSeriesCount == 0 {
				// Skip rows without matching metricIDs.
				continue
			}
		}
		tail := item[len(prefix):]
		var err error
		tail, tmp, err = unmarshalTagValue(tmp[:0], tail)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal tag key from line %q: %w", item, err)
		}
		if isArtificialTagKey(tmp) {
			// Skip artificially created tag keys.
			continue
		}
		if len(tmp) == 0 {
			tmp = append(tmp, "__name__"...)
		}
		if !bytes.Equal(tmp, labelName) {
			thLabelValueCountByLabelName.pushIfNonEmpty(labelName, labelValueCountByLabelName)
			labelValueCountByLabelName = 0
			labelName = append(labelName[:0], tmp...)
		}
		tmp = append(tmp, '=')
		tail, tmp, err = unmarshalTagValue(tmp, tail)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal tag value from line %q: %w", item, err)
		}
		if !bytes.Equal(tmp, labelNameValue) {
			thSeriesCountByLabelValuePair.pushIfNonEmpty(labelNameValue, seriesCountByLabelValuePair)
			if bytes.HasPrefix(labelNameValue, nameEqualBytes) {
				thSeriesCountByMetricName.pushIfNonEmpty(labelNameValue[len(nameEqualBytes):], seriesCountByLabelValuePair)
			}
			seriesCountByLabelValuePair = 0
			labelValueCountByLabelName++
			labelNameValue = append(labelNameValue[:0], tmp...)
		}
		if filter == nil {
			if err := mp.InitOnlyTail(item, tail); err != nil {
				return nil, err
			}
			matchingSeriesCount = mp.MetricIDsLen()
		}
		// Take into account deleted timeseries too.
		// It is OK if series can be counted multiple times in rare cases -
		// the returned number is an estimation.
		seriesCountByLabelValuePair += uint64(matchingSeriesCount)
	}
	if err := ts.Error(); err != nil {
		return nil, fmt.Errorf("error when counting time series by metric names: %w", err)
	}
	thLabelValueCountByLabelName.pushIfNonEmpty(labelName, labelValueCountByLabelName)
	thSeriesCountByLabelValuePair.pushIfNonEmpty(labelNameValue, seriesCountByLabelValuePair)
	if bytes.HasPrefix(labelNameValue, nameEqualBytes) {
		thSeriesCountByMetricName.pushIfNonEmpty(labelNameValue[len(nameEqualBytes):], seriesCountByLabelValuePair)
	}
	status := &TSDBStatus{
		SeriesCountByMetricName:     thSeriesCountByMetricName.getSortedResult(),
		LabelValueCountByLabelName:  thLabelValueCountByLabelName.getSortedResult(),
		SeriesCountByLabelValuePair: thSeriesCountByLabelValuePair.getSortedResult(),
	}
	return status, nil
}

// TSDBStatus contains TSDB status data for /api/v1/status/tsdb.
//
// See https://prometheus.io/docs/prometheus/latest/querying/api/#tsdb-stats
type TSDBStatus struct {
	SeriesCountByMetricName     []TopHeapEntry
	LabelValueCountByLabelName  []TopHeapEntry
	SeriesCountByLabelValuePair []TopHeapEntry
}

func (status *TSDBStatus) hasEntries() bool {
	return len(status.SeriesCountByLabelValuePair) > 0
}

// topHeap maintains a heap of topHeapEntries with the maximum TopHeapEntry.n values.
type topHeap struct {
	topN int
	a    []TopHeapEntry
}

// newTopHeap returns topHeap for topN items.
func newTopHeap(topN int) *topHeap {
	return &topHeap{
		topN: topN,
	}
}

// TopHeapEntry represents an entry from `top heap` used in stats.
type TopHeapEntry struct {
	Name  string
	Count uint64
}

func (th *topHeap) pushIfNonEmpty(name []byte, count uint64) {
	if count == 0 {
		return
	}
	if len(th.a) < th.topN {
		th.a = append(th.a, TopHeapEntry{
			Name:  string(name),
			Count: count,
		})
		heap.Fix(th, len(th.a)-1)
		return
	}
	if count <= th.a[0].Count {
		return
	}
	th.a[0] = TopHeapEntry{
		Name:  string(name),
		Count: count,
	}
	heap.Fix(th, 0)
}

func (th *topHeap) getSortedResult() []TopHeapEntry {
	result := append([]TopHeapEntry{}, th.a...)
	sort.Slice(result, func(i, j int) bool {
		a, b := result[i], result[j]
		if a.Count != b.Count {
			return a.Count > b.Count
		}
		return a.Name < b.Name
	})
	return result
}

// heap.Interface implementation for topHeap.

func (th *topHeap) Len() int {
	return len(th.a)
}

func (th *topHeap) Less(i, j int) bool {
	a := th.a
	return a[i].Count < a[j].Count
}

func (th *topHeap) Swap(i, j int) {
	a := th.a
	a[j], a[i] = a[i], a[j]
}

func (th *topHeap) Push(x interface{}) {
	panic(fmt.Errorf("BUG: Push shouldn't be called"))
}

func (th *topHeap) Pop() interface{} {
	panic(fmt.Errorf("BUG: Pop shouldn't be called"))
}

// searchMetricNameWithCache appends metric name for the given metricID to dst
// and returns the result.
func (db *indexDB) searchMetricNameWithCache(dst []byte, metricID uint64, accountID, projectID uint32) ([]byte, error) {
	metricName := db.getMetricNameFromCache(dst, metricID)
	if len(metricName) > len(dst) {
		return metricName, nil
	}

	is := db.getIndexSearch(accountID, projectID, noDeadline)
	var err error
	dst, err = is.searchMetricName(dst, metricID)
	db.putIndexSearch(is)
	if err == nil {
		// There is no need in verifying whether the given metricID is deleted,
		// since the filtering must be performed before calling this func.
		db.putMetricNameToCache(metricID, dst)
		return dst, nil
	}
	if err != io.EOF {
		return dst, err
	}

	// Try searching in the external indexDB.
	if db.doExtDB(func(extDB *indexDB) {
		is := extDB.getIndexSearch(accountID, projectID, noDeadline)
		dst, err = is.searchMetricName(dst, metricID)
		extDB.putIndexSearch(is)
		if err == nil {
			// There is no need in verifying whether the given metricID is deleted,
			// since the filtering must be performed before calling this func.
			extDB.putMetricNameToCache(metricID, dst)
		}
	}) {
		return dst, err
	}

	// Cannot find MetricName for the given metricID. This may be the case
	// when indexDB contains incomplete set of metricID -> metricName entries
	// after a snapshot or due to unflushed entries.
	atomic.AddUint64(&db.missingMetricNamesForMetricID, 1)

	// Mark the metricID as deleted, so it will be created again when new data point
	// for the given time series will arrive.
	if err := db.deleteMetricIDs([]uint64{metricID}); err != nil {
		return dst, fmt.Errorf("cannot delete metricID for missing metricID->metricName entry; metricID=%d; error: %w", metricID, err)
	}
	return dst, io.EOF
}

// DeleteTSIDs marks as deleted all the TSIDs matching the given tfss.
//
// The caller must reset all the caches which may contain the deleted TSIDs.
//
// Returns the number of metrics deleted.
func (db *indexDB) DeleteTSIDs(tfss []*TagFilters) (int, error) {
	if len(tfss) == 0 {
		return 0, nil
	}

	// Obtain metricIDs to delete.
	tr := TimeRange{
		MinTimestamp: 0,
		MaxTimestamp: (1 << 63) - 1,
	}
	is := db.getIndexSearch(tfss[0].accountID, tfss[0].projectID, noDeadline)
	metricIDs, err := is.searchMetricIDs(tfss, tr, 2e9)
	db.putIndexSearch(is)
	if err != nil {
		return 0, err
	}
	if err := db.deleteMetricIDs(metricIDs); err != nil {
		return 0, err
	}

	// Delete TSIDs in the extDB.
	deletedCount := len(metricIDs)
	if db.doExtDB(func(extDB *indexDB) {
		var n int
		n, err = extDB.DeleteTSIDs(tfss)
		deletedCount += n
	}) {
		if err != nil {
			return deletedCount, fmt.Errorf("cannot delete tsids in extDB: %w", err)
		}
	}
	return deletedCount, nil
}

func (db *indexDB) deleteMetricIDs(metricIDs []uint64) error {
	if len(metricIDs) == 0 {
		// Nothing to delete
		return nil
	}

	// atomically add deleted metricIDs to an inmemory map.
	dmis := &uint64set.Set{}
	dmis.AddMulti(metricIDs)
	db.s.updateDeletedMetricIDs(dmis)

	// Reset TagFilters -> TSIDS cache, since it may contain deleted TSIDs.
	invalidateTagFiltersCache()

	// Reset MetricName -> TSID cache, since it may contain deleted TSIDs.
	db.s.resetAndSaveTSIDCache()

	// Store the metricIDs as deleted.
	// Make this after updating the deletedMetricIDs and resetting caches
	// in order to exclude the possibility of the inconsistent state when the deleted metricIDs
	// remain available in the tsidCache after unclean shutdown.
	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1347
	items := getIndexItems()
	for _, metricID := range metricIDs {
		items.B = append(items.B, nsPrefixDeletedMetricID)
		items.B = encoding.MarshalUint64(items.B, metricID)
		items.Next()
	}
	err := db.tb.AddItems(items.Items)
	putIndexItems(items)
	return err
}

func (db *indexDB) loadDeletedMetricIDs() (*uint64set.Set, error) {
	is := db.getIndexSearch(0, 0, noDeadline)
	dmis, err := is.loadDeletedMetricIDs()
	db.putIndexSearch(is)
	if err != nil {
		return nil, err
	}
	return dmis, nil
}

func (is *indexSearch) loadDeletedMetricIDs() (*uint64set.Set, error) {
	dmis := &uint64set.Set{}
	ts := &is.ts
	kb := &is.kb
	kb.B = append(kb.B[:0], nsPrefixDeletedMetricID)
	ts.Seek(kb.B)
	for ts.NextItem() {
		item := ts.Item
		if !bytes.HasPrefix(item, kb.B) {
			break
		}
		item = item[len(kb.B):]
		if len(item) != 8 {
			return nil, fmt.Errorf("unexpected item len; got %d bytes; want %d bytes", len(item), 8)
		}
		metricID := encoding.UnmarshalUint64(item)
		dmis.Add(metricID)
	}
	if err := ts.Error(); err != nil {
		return nil, err
	}
	return dmis, nil
}

// searchTSIDs returns sorted tsids matching the given tfss over the given tr.  // 在indexDB 中搜索
func (db *indexDB) searchTSIDs(tfss []*TagFilters, tr TimeRange, maxMetrics int, deadline uint64) ([]TSID, error) {
	if len(tfss) == 0 {
		return nil, nil
	}
	if tr.MinTimestamp >= db.s.minTimestampForCompositeIndex { // 这个条件证明，所有要搜索的数据，在时间范围上都在当前indexDB上
		tfss = convertToCompositeTagFilterss(tfss) // 转换搜索的标签的格式
	} // ??? 这里为什么没有else的处理

	tfKeyBuf := tagFiltersKeyBufPool.Get()
	defer tagFiltersKeyBufPool.Put(tfKeyBuf)

	tfKeyBuf.B = marshalTagFiltersKey(tfKeyBuf.B[:0], tfss, tr, true) // 把搜索标签序列化, versioned为true，则序列化数据的prefix，每10秒都会不同
	tsids, ok := db.getFromTagFiltersCache(tfKeyBuf.B)                // 在缓存中搜索
	if ok {
		// Fast path - tsids found in the cache.
		return tsids, nil // 找到了就直接返回
	} // ??? 缓存如何解决新的TSID的问题?

	// Slow path - search for tsids in the db and extDB.
	accountID := tfss[0].accountID
	projectID := tfss[0].projectID
	is := db.getIndexSearch(accountID, projectID, deadline) // 使用 index search对象来搜索TSID
	localTSIDs, err := is.searchTSIDs(tfss, tr, maxMetrics)
	db.putIndexSearch(is)
	if err != nil {
		return nil, err
	}

	var extTSIDs []TSID
	if db.doExtDB(func(extDB *indexDB) { // 在前一个31天的indexDB中查询
		tfKeyExtBuf := tagFiltersKeyBufPool.Get()
		defer tagFiltersKeyBufPool.Put(tfKeyExtBuf)

		// Data in extDB cannot be changed, so use unversioned keys for tag cache.
		tfKeyExtBuf.B = marshalTagFiltersKey(tfKeyExtBuf.B[:0], tfss, tr, false) // 过去的分片，是只读的，所以不需要过期
		tsids, ok := extDB.getFromTagFiltersCache(tfKeyExtBuf.B)
		if ok {
			extTSIDs = tsids
			return
		}
		is := extDB.getIndexSearch(accountID, projectID, deadline)
		extTSIDs, err = is.searchTSIDs(tfss, tr, maxMetrics)
		extDB.putIndexSearch(is)

		sort.Slice(extTSIDs, func(i, j int) bool { return extTSIDs[i].Less(&extTSIDs[j]) })
		extDB.putToTagFiltersCache(extTSIDs, tfKeyExtBuf.B)
	}) {
		if err != nil {
			return nil, err
		}
	}

	// Merge localTSIDs with extTSIDs.
	tsids = mergeTSIDs(localTSIDs, extTSIDs)

	// Sort the found tsids, since they must be passed to TSID search
	// in the sorted order.
	sort.Slice(tsids, func(i, j int) bool { return tsids[i].Less(&tsids[j]) })

	// Store TSIDs in the cache.
	db.putToTagFiltersCache(tsids, tfKeyBuf.B)

	return tsids, err
}

var tagFiltersKeyBufPool bytesutil.ByteBufferPool

func (is *indexSearch) getTSIDByMetricName(dst *TSID, metricName []byte) error { // 根据time series的原始数据，查询tsid
	dmis := is.db.s.getDeletedMetricIDs() // metricName 是序列化以后的 time series数据
	ts := &is.ts                          // table search 对象
	kb := &is.kb                          // bytes buffer 对象
	kb.B = append(kb.B[:0], nsPrefixMetricNameToTSID)
	kb.B = append(kb.B, metricName...)
	kb.B = append(kb.B, kvSeparatorChar) // 拼接成内存中的原始数据的格式
	ts.Seek(kb.B)
	for ts.NextItem() {
		if !bytes.HasPrefix(ts.Item, kb.B) {
			// Nothing found.
			return io.EOF
		}
		v := ts.Item[len(kb.B):]
		tail, err := dst.Unmarshal(v)
		if err != nil {
			return fmt.Errorf("cannot unmarshal TSID: %w", err)
		}
		if len(tail) > 0 {
			return fmt.Errorf("unexpected non-empty tail left after unmarshaling TSID: %X", tail)
		}
		if dmis.Len() > 0 {
			// Verify whether the dst is marked as deleted.
			if dmis.Has(dst.MetricID) {
				// The dst is deleted. Continue searching.
				continue
			}
		}
		// Found valid dst.
		return nil
	}
	if err := ts.Error(); err != nil {
		return fmt.Errorf("error when searching TSID by metricName; searchPrefix %q: %w", kb.B, err)
	}
	// Nothing found
	return io.EOF
}

// 根据metricID搜索metricNAME
func (is *indexSearch) searchMetricNameWithCache(dst []byte, metricID uint64) ([]byte, error) {
	metricName := is.db.getMetricNameFromCache(dst, metricID)
	if len(metricName) > len(dst) {
		return metricName, nil
	}
	var err error
	dst, err = is.searchMetricName(dst, metricID)
	if err == nil {
		// There is no need in verifying whether the given metricID is deleted,
		// since the filtering must be performed before calling this func.
		is.db.putMetricNameToCache(metricID, dst)
		return dst, nil
	}
	return dst, err
}

func (is *indexSearch) searchMetricName(dst []byte, metricID uint64) ([]byte, error) {
	ts := &is.ts
	kb := &is.kb
	kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixMetricIDToMetricName)
	kb.B = encoding.MarshalUint64(kb.B, metricID)
	if err := ts.FirstItemWithPrefix(kb.B); err != nil {
		if err == io.EOF {
			return dst, err
		}
		return dst, fmt.Errorf("error when searching metricName by metricID; searchPrefix %q: %w", kb.B, err)
	}
	v := ts.Item[len(kb.B):]
	dst = append(dst, v...)
	return dst, nil
}

func mergeTSIDs(a, b []TSID) []TSID {
	if len(b) > len(a) {
		a, b = b, a
	}
	if len(b) == 0 {
		return a
	}
	m := make(map[uint64]TSID, len(a))
	for i := range a {
		tsid := &a[i]
		m[tsid.MetricID] = *tsid
	}
	for i := range b {
		tsid := &b[i]
		m[tsid.MetricID] = *tsid
	}

	tsids := make([]TSID, 0, len(m))
	for _, tsid := range m {
		tsids = append(tsids, tsid)
	}
	return tsids
}

// 这个函数证明最小时间那一天是否有数据。 ??? 严重的不靠谱，应该选择多天来验证。如果只是某一天不存在，这个结果明显是不严谨的。
func (is *indexSearch) containsTimeRange(tr TimeRange) (bool,
	error) { // 存在date+metricid的索引，以date为前缀进行匹配，匹配到就证明索引中可以支持这个日期的查询
	ts := &is.ts // mergeset.TableSearch
	kb := &is.kb // 目的缓冲区

	// Verify whether the maximum date in `ts` covers tr.MinTimestamp.
	minDate := uint64(tr.MinTimestamp) / msecPerDay
	kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixDateToMetricID) // 索引是个包容的大杂烩，其中一种索引是日期
	prefix := kb.B
	kb.B = encoding.MarshalUint64(kb.B, minDate) // ??? 为什么不是检查一个日期范围呢?
	ts.Seek(kb.B)                                // kb.B 序列化了要搜索的原始KEY。 vm-storage的底层，是不是也可以理解为是一个KV存储？
	if !ts.NextItem() {                          // 确保游标是否可用
		if err := ts.Error(); err != nil {
			return false, fmt.Errorf("error when searching for minDate=%d, prefix %q: %w", minDate, kb.B, err)
		}
		return false, nil
	}
	if !bytes.HasPrefix(ts.Item, prefix) { // 按照日期来做前缀匹配。如果前缀匹配，就证明找到了
		// minDate exceeds max date from ts.
		return false, nil
	}
	return true, nil
}

func (is *indexSearch) searchTSIDs(tfss []*TagFilters, tr TimeRange, maxMetrics int) ([]TSID, error) { // 根据查询表达式，搜索tsid
	ok, err := is.containsTimeRange(tr) // 时间范围是否有效的检查。 ?? 奇怪，这个检查为什么不提前呢
	if err != nil {
		return nil, err
	}
	if !ok {
		// Fast path - the index doesn't contain data for the given tr.
		return nil, nil
	}
	metricIDs, err := is.searchMetricIDs(tfss, tr, maxMetrics) // 这里执行表达式的查询，非常重要！！！
	if err != nil {
		return nil, err
	}
	if len(metricIDs) == 0 {
		// Nothing found.
		return nil, nil
	}

	// Obtain TSID values for the given metricIDs.
	tsids := make([]TSID, len(metricIDs))
	i := 0
	for loopsPaceLimiter, metricID := range metricIDs {
		if loopsPaceLimiter&paceLimiterSlowIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
				return nil, err
			}
		}
		// Try obtaining TSIDs from MetricID->TSID cache. This is much faster
		// than scanning the mergeset if it contains a lot of metricIDs.
		tsid := &tsids[i]
		err := is.db.getFromMetricIDCache(tsid, metricID)
		if err == nil {
			// Fast path - the tsid for metricID is found in cache.
			i++
			continue
		}
		if err != io.EOF {
			return nil, err
		}
		if err := is.getTSIDByMetricID(tsid, metricID); err != nil {
			if err == io.EOF {
				// Cannot find TSID for the given metricID.
				// This may be the case on incomplete indexDB
				// due to snapshot or due to unflushed entries.
				// Just increment errors counter and skip it.
				atomic.AddUint64(&is.db.missingTSIDsForMetricID, 1)
				continue
			}
			return nil, fmt.Errorf("cannot find tsid %d out of %d for metricID %d: %w", i, len(metricIDs), metricID, err)
		}
		is.db.putToMetricIDCache(metricID, tsid)
		i++
	}
	tsids = tsids[:i]

	// Do not sort the found tsids, since they will be sorted later.
	return tsids, nil
}

func (is *indexSearch) getTSIDByMetricID(dst *TSID, metricID uint64) error {
	// There is no need in checking for deleted metricIDs here, since they
	// must be checked by the caller.
	ts := &is.ts
	kb := &is.kb
	kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixMetricIDToTSID)
	kb.B = encoding.MarshalUint64(kb.B, metricID)
	if err := ts.FirstItemWithPrefix(kb.B); err != nil {
		if err == io.EOF {
			return err
		}
		return fmt.Errorf("error when searching TSID by metricID; searchPrefix %q: %w", kb.B, err)
	}
	v := ts.Item[len(kb.B):] // value存储于索引的后半部分
	tail, err := dst.Unmarshal(v)
	if err != nil {
		return fmt.Errorf("cannot unmarshal TSID=%X: %w", v, err)
	}
	if len(tail) > 0 {
		return fmt.Errorf("unexpected non-zero tail left after unmarshaling TSID: %X", tail)
	}
	return nil
}

// updateMetricIDsByMetricNameMatch matches metricName values for the given srcMetricIDs against tfs
// and adds matching metrics to metricIDs.
func (is *indexSearch) updateMetricIDsByMetricNameMatch(metricIDs, srcMetricIDs *uint64set.Set,
	tfs []*tagFilter) error {
	// sort srcMetricIDs in order to speed up Seek below.
	sortedMetricIDs := srcMetricIDs.AppendTo(nil)

	kb := &is.kb
	kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixTagToMetricIDs)
	tfs = removeCompositeTagFilters(tfs, kb.B)

	metricName := kbPool.Get()
	defer kbPool.Put(metricName)
	mn := GetMetricName()
	defer PutMetricName(mn)
	for loopsPaceLimiter, metricID := range sortedMetricIDs {
		if loopsPaceLimiter&paceLimiterSlowIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
				return err
			}
		}
		var err error
		metricName.B, err = is.searchMetricNameWithCache(metricName.B[:0], metricID)
		if err != nil {
			if err == io.EOF {
				// It is likely the metricID->metricName entry didn't propagate to inverted index yet.
				// Skip this metricID for now.
				continue
			}
			return fmt.Errorf("cannot find metricName by metricID %d: %w", metricID, err)
		}
		if err := mn.Unmarshal(metricName.B); err != nil {
			return fmt.Errorf("cannot unmarshal metricName %q: %w", metricName.B, err)
		}

		// Match the mn against tfs.
		ok, err := matchTagFilters(mn, tfs, &is.kb)
		if err != nil {
			return fmt.Errorf("cannot match MetricName %s against tagFilters: %w", mn, err)
		}
		if !ok {
			continue
		}
		metricIDs.Add(metricID)
	}
	return nil
}

func removeCompositeTagFilters(tfs []*tagFilter, prefix []byte) []*tagFilter {
	if !hasCompositeTagFilters(tfs, prefix) {
		return tfs
	}
	var tagKey []byte
	var name []byte
	tfsNew := make([]*tagFilter, 0, len(tfs)+1)
	for _, tf := range tfs {
		if !bytes.HasPrefix(tf.prefix, prefix) {
			tfsNew = append(tfsNew, tf)
			continue
		}
		suffix := tf.prefix[len(prefix):]
		var err error
		_, tagKey, err = unmarshalTagValue(tagKey[:0], suffix)
		if err != nil {
			logger.Panicf("BUG: cannot unmarshal tag key from suffix=%q: %s", suffix, err)
		}
		if len(tagKey) == 0 || tagKey[0] != compositeTagKeyPrefix {
			tfsNew = append(tfsNew, tf)
			continue
		}
		tagKey = tagKey[1:]
		var nameLen uint64
		tagKey, nameLen, err = encoding.UnmarshalVarUint64(tagKey)
		if err != nil {
			logger.Panicf("BUG: cannot unmarshal nameLen from tagKey %q: %s", tagKey, err)
		}
		if nameLen == 0 {
			logger.Panicf("BUG: nameLen must be greater than 0")
		}
		if uint64(len(tagKey)) < nameLen {
			logger.Panicf("BUG: expecting at %d bytes for name in tagKey=%q; got %d bytes", nameLen, tagKey, len(tagKey))
		}
		name = append(name[:0], tagKey[:nameLen]...)
		tagKey = tagKey[nameLen:]
		var tfNew tagFilter
		if err := tfNew.Init(prefix, tagKey, tf.value, tf.isNegative, tf.isRegexp); err != nil {
			logger.Panicf("BUG: cannot initialize {%s=%q} filter: %s", tagKey, tf.value, err)
		}
		tfsNew = append(tfsNew, &tfNew)
	}
	if len(name) > 0 {
		var tfNew tagFilter
		if err := tfNew.Init(prefix, nil, name, false, false); err != nil {
			logger.Panicf("BUG: unexpected error when initializing {__name__=%q} filter: %s", name, err)
		}
		tfsNew = append(tfsNew, &tfNew)
	}
	return tfsNew
}

func hasCompositeTagFilters(tfs []*tagFilter, prefix []byte) bool {
	var tagKey []byte
	for _, tf := range tfs {
		if !bytes.HasPrefix(tf.prefix, prefix) {
			continue
		}
		suffix := tf.prefix[len(prefix):]
		var err error
		_, tagKey, err = unmarshalTagValue(tagKey[:0], suffix)
		if err != nil {
			logger.Panicf("BUG: cannot unmarshal tag key from suffix=%q: %s", suffix, err)
		}
		if len(tagKey) > 0 && tagKey[0] == compositeTagKeyPrefix {
			return true
		}
	}
	return false
}

func matchTagFilters(mn *MetricName, tfs []*tagFilter, kb *bytesutil.ByteBuffer) (bool, error) {
	kb.B = marshalCommonPrefix(kb.B[:0], nsPrefixTagToMetricIDs, mn.AccountID, mn.ProjectID)
	for i, tf := range tfs {
		if bytes.Equal(tf.key, graphiteReverseTagKey) {
			// Skip artificial tag filter for Graphite-like metric names with dots,
			// since mn doesn't contain the corresponding tag.
			continue
		}
		if len(tf.key) == 0 || string(tf.key) == "__graphite__" {
			// Match against mn.MetricGroup.
			b := marshalTagValue(kb.B, nil)
			b = marshalTagValue(b, mn.MetricGroup)
			kb.B = b[:len(kb.B)]
			ok, err := tf.match(b)
			if err != nil {
				return false, fmt.Errorf("cannot match MetricGroup %q with tagFilter %s: %w", mn.MetricGroup, tf, err)
			}
			if !ok {
				// Move failed tf to start.
				// This should reduce the amount of useless work for the next mn.
				if i > 0 {
					tfs[0], tfs[i] = tfs[i], tfs[0]
				}
				return false, nil
			}
			continue
		}
		// Search for matching tag name.
		tagMatched := false
		tagSeen := false
		for _, tag := range mn.Tags {
			if string(tag.Key) != string(tf.key) {
				continue
			}

			// Found the matching tag name. Match the value.
			tagSeen = true
			b := tag.Marshal(kb.B)
			kb.B = b[:len(kb.B)]
			ok, err := tf.match(b)
			if err != nil {
				return false, fmt.Errorf("cannot match tag %q with tagFilter %s: %w", tag, tf, err)
			}
			if !ok {
				// Move failed tf to start.
				// This should reduce the amount of useless work for the next mn.
				if i > 0 {
					tfs[0], tfs[i] = tfs[i], tfs[0]
				}
				return false, nil
			}
			tagMatched = true
			break
		}
		if !tagSeen && tf.isNegative && !tf.isEmptyMatch {
			// tf contains negative filter for non-exsisting tag key
			// and this filter doesn't match empty string, i.e. {non_existing_tag_key!="foobar"}
			// Such filter matches anything.
			//
			// Note that the filter `{non_existing_tag_key!~"|foobar"}` shouldn't match anything,
			// since it is expected that it matches non-empty `non_existing_tag_key`.
			// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/546 for details.
			continue
		}
		if tagMatched {
			// tf matches mn. Go to the next tf.
			continue
		}
		// Matching tag name wasn't found.
		// Move failed tf to start.
		// This should reduce the amount of useless work for the next mn.
		if i > 0 {
			tfs[0], tfs[i] = tfs[i], tfs[0]
		}
		return false, nil
	}
	return true, nil
}

// 根据查询表达式，搜索对应的TSID
func (is *indexSearch) searchMetricIDs(tfss []*TagFilters, tr TimeRange, maxMetrics int) ([]uint64, error) {
	metricIDs, err := is.searchMetricIDsInternal(tfss, tr, maxMetrics)
	if err != nil {
		return nil, err
	}
	if metricIDs.Len() == 0 {
		// Nothing found
		return nil, nil
	}

	sortedMetricIDs := metricIDs.AppendTo(nil)

	// Filter out deleted metricIDs.
	dmis := is.db.s.getDeletedMetricIDs()
	if dmis.Len() > 0 {
		metricIDsFiltered := sortedMetricIDs[:0]
		for _, metricID := range sortedMetricIDs {
			if !dmis.Has(metricID) {
				metricIDsFiltered = append(metricIDsFiltered, metricID)
			}
		}
		sortedMetricIDs = metricIDsFiltered
	}

	return sortedMetricIDs, nil
}

// 非常重要，根据查询表达式，搜索metricid
func (is *indexSearch) searchMetricIDsInternal(tfss []*TagFilters, tr TimeRange, maxMetrics int) (*uint64set.Set,
	error) {
	metricIDs := &uint64set.Set{} // 用于保存结果
	for _, tfs := range tfss {
		if len(tfs.tfs) == 0 {
			// An empty filters must be equivalent to `{__name__!=""}`
			tfs = NewTagFilters(tfs.accountID, tfs.projectID)
			if err := tfs.Add(nil, nil, true, false); err != nil {
				logger.Panicf(`BUG: cannot add {__name__!=""} filter: %s`, err)
			}
		}
		if err := is.updateMetricIDsForTagFilters(metricIDs, tfs, tr, maxMetrics+1); err != nil { // 搜索一个具体的表达式
			return nil, err
		}
		if metricIDs.Len() > maxMetrics {
			return nil, fmt.Errorf("the number of matching unique timeseries exceeds %d; either narrow down the search or increase -search.maxUniqueTimeseries", maxMetrics)
		}
	}
	return metricIDs, nil
}

func (is *indexSearch) updateMetricIDsForTagFilters(metricIDs *uint64set.Set, tfs *TagFilters, tr TimeRange,
	maxMetrics int) error { // 具体某一个tag表达式的搜索过程
	err := is.tryUpdatingMetricIDsForDateRange(metricIDs, tfs, tr, maxMetrics)
	if err == nil {
		// Fast path: found metricIDs by date range.
		return nil
	}
	if !errors.Is(err, errFallbackToGlobalSearch) { // 如果每天的索引里面搜索不到，就去全局索引搜索
		return err
	}

	// Slow path - fall back to search in the global inverted index.
	atomic.AddUint64(&is.db.globalSearchCalls, 1)
	m, err := is.getMetricIDsForDateAndFilters(0, tfs, maxMetrics)
	if err != nil {
		return err
	}
	metricIDs.UnionMayOwn(m)
	return nil
}

func (is *indexSearch) getMetricIDsForTagFilter(tf *tagFilter, maxMetrics int, maxLoopsCount int64) (*uint64set.Set,
	int64, error) { // 查询单个过滤项，一般以 date+metricName 为key进行搜索
	if tf.isNegative {
		logger.Panicf("BUG: isNegative must be false")
	}
	metricIDs := &uint64set.Set{}
	if len(tf.orSuffixes) > 0 {
		// Fast path for orSuffixes - seek for rows for each value from orSuffixes.
		loopsCount, err := is.updateMetricIDsForOrSuffixes(tf, metricIDs, maxMetrics, maxLoopsCount)
		if err != nil {
			return nil, loopsCount, fmt.Errorf("error when searching for metricIDs for tagFilter in fast path: %w; tagFilter=%s", err, tf)
		}
		return metricIDs, loopsCount, nil
	}

	// Slow path - scan for all the rows with the given prefix.
	loopsCount, err := is.getMetricIDsForTagFilterSlow(tf, metricIDs.Add, maxLoopsCount)
	if err != nil {
		return nil, loopsCount, fmt.Errorf("error when searching for metricIDs for tagFilter in slow path: %w; tagFilter=%s", err, tf)
	}
	return metricIDs, loopsCount, nil
}

var errTooManyLoops = fmt.Errorf("too many loops is needed for applying this filter")

func (is *indexSearch) getMetricIDsForTagFilterSlow(tf *tagFilter, f func(metricID uint64), maxLoopsCount int64) (int64,
	error) { // 根据单个过滤表达式，查询metricID
	if len(tf.orSuffixes) > 0 {
		logger.Panicf("BUG: the getMetricIDsForTagFilterSlow must be called only for empty tf.orSuffixes; got %s", tf.orSuffixes)
	}

	// Scan all the rows with tf.prefix and call f on every tf match.
	ts := &is.ts
	kb := &is.kb
	mp := &is.mp
	mp.Reset()
	var prevMatchingSuffix []byte
	var prevMatch bool
	var loopsCount int64
	loopsPaceLimiter := 0
	prefix := tf.prefix
	ts.Seek(prefix)     // 调用 table search的seek，遍历了所有的part
	for ts.NextItem() { // 循环使用游标，从heap中提取数据
		if loopsPaceLimiter&paceLimiterMediumIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
				return loopsCount, err
			}
		}
		loopsPaceLimiter++
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			return loopsCount, nil
		}
		tail := item[len(prefix):]
		n := bytes.IndexByte(tail, tagSeparatorChar)
		if n < 0 {
			return loopsCount, fmt.Errorf("invalid tag->metricIDs line %q: cannot find tagSeparatorChar=%d", item, tagSeparatorChar)
		}
		suffix := tail[:n+1]
		tail = tail[n+1:]
		if err := mp.InitOnlyTail(item, tail); err != nil {
			return loopsCount, err
		}
		mp.ParseMetricIDs()
		loopsCount += int64(mp.MetricIDsLen())
		if loopsCount > maxLoopsCount {
			return loopsCount, errTooManyLoops
		}
		if prevMatch && string(suffix) == string(prevMatchingSuffix) {
			// Fast path: the same tag value found.
			// There is no need in checking it again with potentially
			// slow tf.matchSuffix, which may call regexp.
			for _, metricID := range mp.MetricIDs {
				f(metricID)
			}
			continue
		}
		// Slow path: need tf.matchSuffix call.
		ok, err := tf.matchSuffix(suffix)
		// Assume that tf.matchSuffix call needs 10x more time than a single metric scan iteration.
		loopsCount += 10 * int64(tf.matchCost)
		if err != nil {
			return loopsCount, fmt.Errorf("error when matching %s against suffix %q: %w", tf, suffix, err)
		}
		if !ok {
			prevMatch = false
			if mp.MetricIDsLen() < maxMetricIDsPerRow/2 {
				// If the current row contains non-full metricIDs list,
				// then it is likely the next row contains the next tag value.
				// So skip seeking for the next tag value, since it will be slower than just ts.NextItem call.
				continue
			}
			// Optimization: skip all the metricIDs for the given tag value
			kb.B = append(kb.B[:0], item[:len(item)-len(tail)]...)
			// The last char in kb.B must be tagSeparatorChar. Just increment it
			// in order to jump to the next tag value.
			if len(kb.B) == 0 || kb.B[len(kb.B)-1] != tagSeparatorChar || tagSeparatorChar >= 0xff {
				return loopsCount, fmt.Errorf("data corruption: the last char in k=%X must be %X", kb.B, tagSeparatorChar)
			}
			kb.B[len(kb.B)-1]++
			ts.Seek(kb.B)
			// Assume that a seek cost is equivalent to 1000 ordinary loops.
			loopsCount += 1000
			continue
		}
		prevMatch = true
		prevMatchingSuffix = append(prevMatchingSuffix[:0], suffix...)
		for _, metricID := range mp.MetricIDs {
			f(metricID) // 把数据加入到 uint64set
		}
	}
	if err := ts.Error(); err != nil {
		return loopsCount, fmt.Errorf("error when searching for tag filter prefix %q: %w", prefix, err)
	}
	return loopsCount, nil
}

func (is *indexSearch) updateMetricIDsForOrSuffixes(tf *tagFilter, metricIDs *uint64set.Set, maxMetrics int,
	maxLoopsCount int64) (int64, error) {
	if tf.isNegative {
		logger.Panicf("BUG: isNegative must be false")
	}
	kb := kbPool.Get()
	defer kbPool.Put(kb)
	var loopsCount int64
	for _, orSuffix := range tf.orSuffixes {
		kb.B = append(kb.B[:0], tf.prefix...)
		kb.B = append(kb.B, orSuffix...)
		kb.B = append(kb.B, tagSeparatorChar)
		lc, err := is.updateMetricIDsForOrSuffix(kb.B, metricIDs, maxMetrics, maxLoopsCount-loopsCount)
		loopsCount += lc
		if err != nil {
			return loopsCount, err
		}
		if metricIDs.Len() >= maxMetrics {
			return loopsCount, nil
		}
	}
	return loopsCount, nil
}

func (is *indexSearch) updateMetricIDsForOrSuffix(prefix []byte, metricIDs *uint64set.Set, maxMetrics int,
	maxLoopsCount int64) (int64, error) {
	ts := &is.ts
	mp := &is.mp
	mp.Reset()
	var loopsCount int64
	loopsPaceLimiter := 0
	ts.Seek(prefix)
	for metricIDs.Len() < maxMetrics && ts.NextItem() {
		if loopsPaceLimiter&paceLimiterFastIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
				return loopsCount, err
			}
		}
		loopsPaceLimiter++
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			return loopsCount, nil
		}
		if err := mp.InitOnlyTail(item, item[len(prefix):]); err != nil {
			return loopsCount, err
		}
		loopsCount += int64(mp.MetricIDsLen())
		if loopsCount > maxLoopsCount {
			return loopsCount, errTooManyLoops
		}
		mp.ParseMetricIDs()
		metricIDs.AddMulti(mp.MetricIDs)
	}
	if err := ts.Error(); err != nil {
		return loopsCount, fmt.Errorf("error when searching for tag filter prefix %q: %w", prefix, err)
	}
	return loopsCount, nil
}

var errFallbackToGlobalSearch = errors.New("fall back from per-day index search to global index search")

const maxDaysForPerDaySearch = 40

func (is *indexSearch) tryUpdatingMetricIDsForDateRange(metricIDs *uint64set.Set, tfs *TagFilters, tr TimeRange,
	maxMetrics int) error { // 搜索一个tag的过程
	atomic.AddUint64(&is.db.dateRangeSearchCalls, 1) // 存在一个每天的索引，先在每天的索引里面搜索
	minDate := uint64(tr.MinTimestamp) / msecPerDay
	maxDate := uint64(tr.MaxTimestamp) / msecPerDay
	if minDate > maxDate || maxDate-minDate > maxDaysForPerDaySearch { // todo: 后面要看看，vm-select是不是会把长周期自动拆成多个40天
		// Too much dates must be covered. Give up, since it may be slow.
		return errFallbackToGlobalSearch
	}
	if minDate == maxDate {
		// Fast path - query only a single date.
		m, err := is.getMetricIDsForDateAndFilters(minDate, tfs, maxMetrics)
		if err != nil {
			return err
		}
		metricIDs.UnionMayOwn(m)
		atomic.AddUint64(&is.db.dateRangeSearchHits, 1)
		return nil
	}

	// Slower path - search for metricIDs for each day in parallel.
	var wg sync.WaitGroup
	var errGlobal error
	var mu sync.Mutex // protects metricIDs + errGlobal vars from concurrent access below
	for minDate <= maxDate {
		wg.Add(1)
		go func(date uint64) {
			defer wg.Done()
			isLocal := is.db.getIndexSearch(is.accountID, is.projectID, is.deadline)
			m, err := isLocal.getMetricIDsForDateAndFilters(date, tfs, maxMetrics)
			is.db.putIndexSearch(isLocal)
			mu.Lock()
			defer mu.Unlock()
			if errGlobal != nil {
				return
			}
			if err != nil {
				dateStr := time.Unix(int64(date*24*3600), 0)
				errGlobal = fmt.Errorf("cannot search for metricIDs at %s: %w", dateStr, err)
				return
			}
			if metricIDs.Len() < maxMetrics {
				metricIDs.UnionMayOwn(m)
			}
		}(minDate)
		minDate++
	}
	wg.Wait()
	if errGlobal != nil {
		return errGlobal
	}
	atomic.AddUint64(&is.db.dateRangeSearchHits, 1)
	return nil
}

// 查询只需要一天数据的情况
func (is *indexSearch) getMetricIDsForDateAndFilters(date uint64, tfs *TagFilters, maxMetrics int) (*uint64set.Set,
	error) {
	// Sort tfs by loopsCount needed for performing each filter.
	// This stats is usually collected from the previous queries.
	// This way we limit the amount of work below by applying fast filters at first.
	type tagFilterWithWeight struct {
		tf               *tagFilter
		loopsCount       int64
		filterLoopsCount int64
	}
	tfws := make([]tagFilterWithWeight, len(tfs.tfs))
	currentTime := fasttime.UnixTimestamp()
	for i := range tfs.tfs { // 遍历每个过滤项 tag=value
		tf := &tfs.tfs[i]
		loopsCount, filterLoopsCount, timestamp := is.getLoopsCountAndTimestampForDateFilter(date, tf)
		if currentTime > timestamp+3600 {
			// Update stats once per hour for relatively fast tag filters.
			// There is no need in spending CPU resources on updating stats for heavy tag filters.
			if loopsCount <= 10e6 {
				loopsCount = 0
			}
			if filterLoopsCount <= 10e6 {
				filterLoopsCount = 0
			}
		}
		tfws[i] = tagFilterWithWeight{
			tf:               tf,
			loopsCount:       loopsCount,
			filterLoopsCount: filterLoopsCount,
		}
	}
	sort.Slice(tfws, func(i, j int) bool {
		a, b := &tfws[i], &tfws[j]
		if a.loopsCount != b.loopsCount {
			return a.loopsCount < b.loopsCount
		}
		return a.tf.Less(b.tf)
	})
	getFirstPositiveLoopsCount := func(tfws []tagFilterWithWeight) int64 {
		for i := range tfws {
			if n := tfws[i].loopsCount; n > 0 {
				return n
			}
		}
		return int64Max
	}
	storeLoopsCount := func(tfw *tagFilterWithWeight, loopsCount int64) {
		if loopsCount != tfw.loopsCount {
			tfw.loopsCount = loopsCount
			is.storeLoopsCountForDateFilter(date, tfw.tf, tfw.loopsCount, tfw.filterLoopsCount)
		}
	}

	// Populate metricIDs for the first non-negative filter with the cost smaller than maxLoopsCount.
	var metricIDs *uint64set.Set
	tfwsRemaining := tfws[:0]
	maxDateMetrics := intMax
	if maxMetrics < intMax/50 {
		maxDateMetrics = maxMetrics * 50
	}
	for i, tfw := range tfws {
		tf := tfw.tf
		if tf.isNegative || tf.isEmptyMatch {
			tfwsRemaining = append(tfwsRemaining, tfw)
			continue
		}
		maxLoopsCount := getFirstPositiveLoopsCount(tfws[i+1:])
		m, loopsCount, err := is.getMetricIDsForDateTagFilter(tf, date, tfs.commonPrefix, maxDateMetrics, maxLoopsCount)
		if err != nil {
			if errors.Is(err, errTooManyLoops) {
				// The tf took too many loops compared to the next filter. Postpone applying this filter.
				storeLoopsCount(&tfw, 2*loopsCount)
				tfwsRemaining = append(tfwsRemaining, tfw)
				continue
			}
			// Move failing filter to the end of filter list.
			storeLoopsCount(&tfw, int64Max)
			return nil, err
		}
		if m.Len() >= maxDateMetrics {
			// Too many time series found by a single tag filter. Move the filter to the end of list.
			storeLoopsCount(&tfw, int64Max-1)
			tfwsRemaining = append(tfwsRemaining, tfw)
			continue
		}
		storeLoopsCount(&tfw, loopsCount)
		metricIDs = m // ??? 为什么这里不是取并集呢 ??
		tfwsRemaining = append(tfwsRemaining, tfws[i+1:]...)
		break
	}
	tfws = tfwsRemaining

	if metricIDs == nil {
		// All the filters in tfs are negative or match too many time series.
		// Populate all the metricIDs for the given (date),
		// so later they can be filtered out with negative filters.
		m, err := is.getMetricIDsForDate(date, maxDateMetrics)
		if err != nil {
			return nil, fmt.Errorf("cannot obtain all the metricIDs: %w", err)
		}
		if m.Len() >= maxDateMetrics {
			// Too many time series found for the given (date). Fall back to global search.
			return nil, errFallbackToGlobalSearch
		}
		metricIDs = m
	}

	sort.Slice(tfws, func(i, j int) bool {
		a, b := &tfws[i], &tfws[j]
		if a.filterLoopsCount != b.filterLoopsCount {
			return a.filterLoopsCount < b.filterLoopsCount
		}
		return a.tf.Less(b.tf)
	})
	getFirstPositiveFilterLoopsCount := func(tfws []tagFilterWithWeight) int64 {
		for i := range tfws {
			if n := tfws[i].filterLoopsCount; n > 0 {
				return n
			}
		}
		return int64Max
	}
	storeFilterLoopsCount := func(tfw *tagFilterWithWeight, filterLoopsCount int64) {
		if filterLoopsCount != tfw.filterLoopsCount {
			is.storeLoopsCountForDateFilter(date, tfw.tf, tfw.loopsCount, filterLoopsCount)
		}
	}

	// Intersect metricIDs with the rest of filters.
	//
	// Do not run these tag filters in parallel, since this may result in CPU and RAM waste
	// when the intial tag filters significantly reduce the number of found metricIDs,
	// so the remaining filters could be performed via much faster metricName matching instead
	// of slow selecting of matching metricIDs.
	var tfsPostponed []*tagFilter
	for i, tfw := range tfws {
		tf := tfw.tf
		metricIDsLen := metricIDs.Len()
		if metricIDsLen == 0 {
			// There is no need in applying the remaining filters to an empty set.
			break
		}
		if tfw.filterLoopsCount > int64(metricIDsLen)*loopsCountPerMetricNameMatch {
			// It should be faster performing metricName match on the remaining filters
			// instead of scanning big number of entries in the inverted index for these filters.
			for _, tfw := range tfws[i:] {
				tfsPostponed = append(tfsPostponed, tfw.tf)
			}
			break
		}
		maxLoopsCount := getFirstPositiveFilterLoopsCount(tfws[i+1:])
		if maxLoopsCount == int64Max {
			maxLoopsCount = int64(metricIDsLen) * loopsCountPerMetricNameMatch
		}
		m, filterLoopsCount, err := is.getMetricIDsForDateTagFilter(tf, date, tfs.commonPrefix, intMax, maxLoopsCount)
		if err != nil {
			if errors.Is(err, errTooManyLoops) {
				// Postpone tf, since it took more loops than the next filter may need.
				storeFilterLoopsCount(&tfw, 2*filterLoopsCount)
				tfsPostponed = append(tfsPostponed, tf)
				continue
			}
			// Move failing tf to the end of filter list
			storeFilterLoopsCount(&tfw, int64Max)
			return nil, err
		}
		storeFilterLoopsCount(&tfw, filterLoopsCount)
		if tf.isNegative || tf.isEmptyMatch {
			metricIDs.Subtract(m)
		} else {
			metricIDs.Intersect(m)
		}
	}
	if metricIDs.Len() == 0 {
		// There is no need in applying tfsPostponed, since the result is empty.
		return nil, nil
	}
	if len(tfsPostponed) > 0 {
		// Apply the postponed filters via metricName match.
		var m uint64set.Set
		if err := is.updateMetricIDsByMetricNameMatch(&m, metricIDs, tfsPostponed); err != nil {
			return nil, err
		}
		return &m, nil
	}
	return metricIDs, nil
}

const (
	intMax   = int((^uint(0)) >> 1)
	int64Max = int64((1 << 63) - 1)
)

func (is *indexSearch) storeDateMetricID(date, metricID uint64, mn *MetricName) error { // 在KV中写入数据，说明某天有某个metricID
	ii := getIndexItems()
	defer putIndexItems(ii)

	ii.B = is.marshalCommonPrefix(ii.B, nsPrefixDateToMetricID) // 构造原始key
	ii.B = encoding.MarshalUint64(ii.B, date)
	ii.B = encoding.MarshalUint64(ii.B, metricID)
	ii.Next()

	// Create per-day inverted index entries for metricID.
	kb := kbPool.Get()
	defer kbPool.Put(kb)
	kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixDateTagToMetricIDs) // 构造索引的公共前缀
	kb.B = encoding.MarshalUint64(kb.B, date)
	ii.registerTagIndexes(kb.B, mn, metricID)           // 写入公共前缀
	if err := is.db.tb.AddItems(ii.Items); err != nil { // 写入日期+metricID的索引
		return fmt.Errorf("cannot add per-day entires for metricID %d: %w", metricID, err)
	}
	return nil
}

func (ii *indexItems) registerTagIndexes(prefix []byte, mn *MetricName, metricID uint64) { // 写入公共前缀
	// Add index entry for MetricGroup -> MetricID
	ii.B = append(ii.B, prefix...) // prefix 一般是 mn.AccountID, mn.ProjectID
	ii.B = marshalTagValue(ii.B, nil)
	ii.B = marshalTagValue(ii.B, mn.MetricGroup) // __name__ -> MetricID
	ii.B = encoding.MarshalUint64(ii.B, metricID)
	ii.Next()                                              // 追加为一个 item
	ii.addReverseMetricGroupIfNeeded(prefix, mn, metricID) // graphite 体系的特殊处理

	// Add index entries for tags: tag -> MetricID
	for _, tag := range mn.Tags {
		ii.B = append(ii.B, prefix...)
		ii.B = tag.Marshal(ii.B)
		ii.B = encoding.MarshalUint64(ii.B, metricID)
		ii.Next() // 每个label name + label value形成一个索引 item
	}

	// Add index entries for composite tags: MetricGroup+tag -> MetricID  // MetricGroup就是 __name__
	compositeKey := kbPool.Get()
	for _, tag := range mn.Tags {
		compositeKey.B = marshalCompositeTagKey(compositeKey.B[:0], mn.MetricGroup, tag.Key) // __name__ + label_name
		ii.B = append(ii.B, prefix...)
		ii.B = marshalTagValue(ii.B, compositeKey.B)
		ii.B = marshalTagValue(ii.B, tag.Value)
		ii.B = encoding.MarshalUint64(ii.B, metricID)
		ii.Next() // 看起来是建立了一堆符合索引
	}
	kbPool.Put(compositeKey)
}

func (ii *indexItems) addReverseMetricGroupIfNeeded(prefix []byte, mn *MetricName, metricID uint64) {
	if bytes.IndexByte(mn.MetricGroup, '.') < 0 {
		// The reverse metric group is needed only for Graphite-like metrics with points.
		return
	} // __name__ 中有 . 这个字符的时候，特殊处理
	// This is most likely a Graphite metric like 'foo.bar.baz'.
	// Store reverse metric name 'zab.rab.oof' in order to speed up search for '*.bar.baz'
	// when the Graphite wildcard has a suffix matching small number of time series.
	ii.B = append(ii.B, prefix...)
	ii.B = marshalTagValue(ii.B, graphiteReverseTagKey)
	revBuf := kbPool.Get()
	revBuf.B = reverseBytes(revBuf.B[:0], mn.MetricGroup)
	ii.B = marshalTagValue(ii.B, revBuf.B)
	kbPool.Put(revBuf)
	ii.B = encoding.MarshalUint64(ii.B, metricID)
	ii.Next()
}

func isArtificialTagKey(key []byte) bool {
	if bytes.Equal(key, graphiteReverseTagKey) {
		return true
	}
	if len(key) > 0 && key[0] == compositeTagKeyPrefix {
		return true
	}
	return false
}

// The tag key for reverse metric name used for speeding up searching
// for Graphite wildcards with suffix matching small number of time series,
// i.e. '*.bar.baz'.
//
// It is expected that the given key isn't be used by users.
var graphiteReverseTagKey = []byte("\xff")

// The prefix for composite tag, which is used for speeding up searching
// for composite filters, which contain `{__name__="<metric_name>"}` filter.
//
// It is expected that the given prefix isn't used by users.
const compositeTagKeyPrefix = '\xfe'

func marshalCompositeTagKey(dst, name, key []byte) []byte {
	dst = append(dst, compositeTagKeyPrefix)
	dst = encoding.MarshalVarUint64(dst, uint64(len(name)))
	dst = append(dst, name...)
	dst = append(dst, key...)
	return dst
}

func reverseBytes(dst, src []byte) []byte {
	for i := len(src) - 1; i >= 0; i-- {
		dst = append(dst, src[i])
	}
	return dst
}

func (is *indexSearch) hasDateMetricID(date, metricID uint64) (bool, error) {
	ts := &is.ts
	kb := &is.kb
	kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixDateToMetricID)
	kb.B = encoding.MarshalUint64(kb.B, date)
	kb.B = encoding.MarshalUint64(kb.B, metricID)
	if err := ts.FirstItemWithPrefix(kb.B); err != nil {
		if err == io.EOF {
			return false, nil
		}
		return false, fmt.Errorf("error when searching for (date=%d, metricID=%d) entry: %w", date, metricID, err)
	}
	if string(ts.Item) != string(kb.B) {
		return false, fmt.Errorf("unexpected entry for (date=%d, metricID=%d); got %q; want %q", date, metricID, ts.Item, kb.B)
	}
	return true, nil
}

func (is *indexSearch) getMetricIDsForDateTagFilter(tf *tagFilter, date uint64, commonPrefix []byte, maxMetrics int,
	maxLoopsCount int64) (*uint64set.Set, int64, error) { // 过滤单个metric的表达式
	if !bytes.HasPrefix(tf.prefix, commonPrefix) {
		logger.Panicf("BUG: unexpected tf.prefix %q; must start with commonPrefix %q", tf.prefix, commonPrefix)
	}
	kb := kbPool.Get()
	defer kbPool.Put(kb)
	if date != 0 {
		// Use per-date search.
		kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixDateTagToMetricIDs) // 使用 日期+metricName 这个key
		kb.B = encoding.MarshalUint64(kb.B, date)
	} else {
		// Use global search if date isn't set.
		kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixTagToMetricIDs)
	}
	prefix := kb.B
	kb.B = append(kb.B, tf.prefix[len(commonPrefix):]...)
	tfNew := *tf
	tfNew.isNegative = false // isNegative for the original tf is handled by the caller.
	tfNew.prefix = kb.B
	metricIDs, loopsCount, err := is.getMetricIDsForTagFilter(&tfNew, maxMetrics, maxLoopsCount)
	if err != nil {
		return nil, loopsCount, err
	}
	if tf.isNegative || !tf.isEmptyMatch {
		return metricIDs, loopsCount, nil
	}
	// The tag filter, which matches empty label such as {foo=~"bar|"}
	// Convert it to negative filter, which matches {foo=~".+",foo!~"bar|"}.
	// This fixes https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1601
	// See also https://github.com/VictoriaMetrics/VictoriaMetrics/issues/395
	maxLoopsCount -= loopsCount
	tfNew = tagFilter{}
	if err := tfNew.Init(prefix, tf.key, []byte(".+"), false, true); err != nil {
		logger.Panicf(`BUG: cannot init tag filter: {%q=~".+"}: %s`, tf.key, err)
	}
	m, lc, err := is.getMetricIDsForTagFilter(&tfNew, maxMetrics, maxLoopsCount)
	loopsCount += lc
	if err != nil {
		return nil, loopsCount, err
	}
	m.Subtract(metricIDs)
	return m, loopsCount, nil
}

func (is *indexSearch) getLoopsCountAndTimestampForDateFilter(date uint64, tf *tagFilter) (int64, int64, uint64) {
	is.kb.B = appendDateTagFilterCacheKey(is.kb.B[:0], is.db.name, date, tf, is.accountID, is.projectID)
	kb := kbPool.Get()
	defer kbPool.Put(kb)
	kb.B = is.db.loopsPerDateTagFilterCache.Get(kb.B[:0], is.kb.B)
	if len(kb.B) != 3*8 {
		return 0, 0, 0
	}
	loopsCount := encoding.UnmarshalInt64(kb.B)
	filterLoopsCount := encoding.UnmarshalInt64(kb.B[8:])
	timestamp := encoding.UnmarshalUint64(kb.B[16:])
	return loopsCount, filterLoopsCount, timestamp
}

func (is *indexSearch) storeLoopsCountForDateFilter(date uint64, tf *tagFilter, loopsCount, filterLoopsCount int64) {
	currentTimestamp := fasttime.UnixTimestamp()
	is.kb.B = appendDateTagFilterCacheKey(is.kb.B[:0], is.db.name, date, tf, is.accountID, is.projectID)
	kb := kbPool.Get()
	kb.B = encoding.MarshalInt64(kb.B[:0], loopsCount)
	kb.B = encoding.MarshalInt64(kb.B, filterLoopsCount)
	kb.B = encoding.MarshalUint64(kb.B, currentTimestamp)
	is.db.loopsPerDateTagFilterCache.Set(is.kb.B, kb.B)
	kbPool.Put(kb)
}

func appendDateTagFilterCacheKey(dst []byte, indexDBName string, date uint64, tf *tagFilter,
	accountID, projectID uint32) []byte {
	dst = append(dst, indexDBName...)
	dst = encoding.MarshalUint64(dst, date)
	dst = tf.Marshal(dst, accountID, projectID)
	return dst
}

func (is *indexSearch) getMetricIDsForDate(date uint64, maxMetrics int) (*uint64set.Set, error) {
	// Extract all the metricIDs from (date, __name__=value)->metricIDs entries.
	kb := kbPool.Get()
	defer kbPool.Put(kb)
	if date != 0 {
		// Use per-date search
		kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixDateTagToMetricIDs)
		kb.B = encoding.MarshalUint64(kb.B, date)
	} else {
		// Use global search
		kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixTagToMetricIDs)
	}
	kb.B = marshalTagValue(kb.B, nil)
	var metricIDs uint64set.Set
	if err := is.updateMetricIDsForPrefix(kb.B, &metricIDs, maxMetrics); err != nil {
		return nil, err
	}
	return &metricIDs, nil
}

func (is *indexSearch) updateMetricIDsForPrefix(prefix []byte, metricIDs *uint64set.Set, maxMetrics int) error {
	ts := &is.ts
	mp := &is.mp
	loopsPaceLimiter := 0
	ts.Seek(prefix)
	for ts.NextItem() {
		if loopsPaceLimiter&paceLimiterFastIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
				return err
			}
		}
		loopsPaceLimiter++
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			return nil
		}
		tail := item[len(prefix):]
		n := bytes.IndexByte(tail, tagSeparatorChar)
		if n < 0 {
			return fmt.Errorf("invalid tag->metricIDs line %q: cannot find tagSeparatorChar %d", item, tagSeparatorChar)
		}
		tail = tail[n+1:]
		if err := mp.InitOnlyTail(item, tail); err != nil {
			return err
		}
		mp.ParseMetricIDs()
		metricIDs.AddMulti(mp.MetricIDs)
		if metricIDs.Len() >= maxMetrics {
			return nil
		}
	}
	if err := ts.Error(); err != nil {
		return fmt.Errorf("error when searching for all metricIDs by prefix %q: %w", prefix, err)
	}
	return nil
}

// The estimated number of index scan loops a single loop in updateMetricIDsByMetricNameMatch takes.
const loopsCountPerMetricNameMatch = 150

var kbPool bytesutil.ByteBufferPool

// Returns local unique MetricID.
func generateUniqueMetricID() uint64 {
	// It is expected that metricIDs returned from this function must be dense.
	// If they will be sparse, then this may hurt metric_ids intersection
	// performance with uint64set.Set.
	return atomic.AddUint64(&nextUniqueMetricID, 1)
}

// This number mustn't go backwards on restarts, otherwise metricID
// collisions are possible. So don't change time on the server
// between VictoriaMetrics restarts.
var nextUniqueMetricID = uint64(time.Now().UnixNano())

func marshalCommonPrefix(dst []byte, nsPrefix byte, accountID, projectID uint32) []byte {
	dst = append(dst, nsPrefix)
	dst = encoding.MarshalUint32(dst, accountID)
	dst = encoding.MarshalUint32(dst, projectID)
	return dst
}

func (is *indexSearch) marshalCommonPrefix(dst []byte, nsPrefix byte) []byte {
	return marshalCommonPrefix(dst, nsPrefix, is.accountID, is.projectID)
}

func unmarshalCommonPrefix(src []byte) ([]byte, byte, uint32, uint32, error) {
	if len(src) < commonPrefixLen {
		return nil, 0, 0, 0, fmt.Errorf("cannot unmarshal common prefix from %d bytes; need at least %d bytes; data=%X", len(src), commonPrefixLen, src)
	}
	prefix := src[0]
	accountID := encoding.UnmarshalUint32(src[1:])
	projectID := encoding.UnmarshalUint32(src[5:])
	return src[commonPrefixLen:], prefix, accountID, projectID, nil
}

// 1 byte for prefix, 4 bytes for accountID, 4 bytes for projectID
const commonPrefixLen = 9

type tagToMetricIDsRowParser struct {
	// NSPrefix contains the first byte parsed from the row after Init call.
	// This is either nsPrefixTagToMetricIDs or nsPrefixDateTagToMetricIDs.
	NSPrefix byte

	// AccountID contains parsed value after Init call
	AccountID uint32

	// ProjectID contains parsed value after Init call
	ProjectID uint32

	// Date contains parsed date for nsPrefixDateTagToMetricIDs rows after Init call
	Date uint64

	// MetricIDs contains parsed MetricIDs after ParseMetricIDs call
	MetricIDs []uint64

	// Tag contains parsed tag after Init call
	Tag Tag

	// tail contains the remaining unparsed metricIDs
	tail []byte
}

func (mp *tagToMetricIDsRowParser) Reset() {
	mp.NSPrefix = 0
	mp.AccountID = 0
	mp.ProjectID = 0
	mp.Date = 0
	mp.MetricIDs = mp.MetricIDs[:0]
	mp.Tag.Reset()
	mp.tail = nil
}

// Init initializes mp from b, which should contain encoded tag->metricIDs row.
//
// b cannot be re-used until Reset call.
func (mp *tagToMetricIDsRowParser) Init(b []byte, nsPrefixExpected byte) error {
	tail, nsPrefix, accountID, projectID, err := unmarshalCommonPrefix(b)
	if err != nil {
		return fmt.Errorf("invalid tag->metricIDs row %q: %w", b, err)
	}
	if nsPrefix != nsPrefixExpected {
		return fmt.Errorf("invalid prefix for tag->metricIDs row %q; got %d; want %d", b, nsPrefix, nsPrefixExpected)
	}
	if nsPrefix == nsPrefixDateTagToMetricIDs {
		// unmarshal date.
		if len(tail) < 8 {
			return fmt.Errorf("cannot unmarshal date from (date, tag)->metricIDs row %q from %d bytes; want at least 8 bytes", b, len(tail))
		}
		mp.Date = encoding.UnmarshalUint64(tail)
		tail = tail[8:]
	}
	mp.NSPrefix = nsPrefix
	mp.AccountID = accountID
	mp.ProjectID = projectID
	tail, err = mp.Tag.Unmarshal(tail)
	if err != nil {
		return fmt.Errorf("cannot unmarshal tag from tag->metricIDs row %q: %w", b, err)
	}
	return mp.InitOnlyTail(b, tail)
}

// MarshalPrefix marshals row prefix without tail to dst.
func (mp *tagToMetricIDsRowParser) MarshalPrefix(dst []byte) []byte {
	dst = marshalCommonPrefix(dst, mp.NSPrefix, mp.AccountID, mp.ProjectID)
	if mp.NSPrefix == nsPrefixDateTagToMetricIDs {
		dst = encoding.MarshalUint64(dst, mp.Date)
	}
	dst = mp.Tag.Marshal(dst)
	return dst
}

// InitOnlyTail initializes mp.tail from tail.
//
// b must contain tag->metricIDs row.
// b cannot be re-used until Reset call.
func (mp *tagToMetricIDsRowParser) InitOnlyTail(b, tail []byte) error {
	if len(tail) == 0 {
		return fmt.Errorf("missing metricID in the tag->metricIDs row %q", b)
	}
	if len(tail)%8 != 0 {
		return fmt.Errorf("invalid tail length in the tag->metricIDs row; got %d bytes; must be multiple of 8 bytes", len(tail))
	}
	mp.tail = tail
	return nil
}

// EqualPrefix returns true if prefixes for mp and x are equal.
//
// Prefix contains (tag, accountID, projectID)
func (mp *tagToMetricIDsRowParser) EqualPrefix(x *tagToMetricIDsRowParser) bool {
	if !mp.Tag.Equal(&x.Tag) {
		return false
	}
	return mp.ProjectID == x.ProjectID && mp.AccountID == x.AccountID && mp.Date == x.Date && mp.NSPrefix == x.NSPrefix
}

// MetricIDsLen returns the number of MetricIDs in the mp.tail
func (mp *tagToMetricIDsRowParser) MetricIDsLen() int {
	return len(mp.tail) / 8
}

// ParseMetricIDs parses MetricIDs from mp.tail into mp.MetricIDs.
func (mp *tagToMetricIDsRowParser) ParseMetricIDs() {
	tail := mp.tail
	mp.MetricIDs = mp.MetricIDs[:0]
	n := len(tail) / 8
	if n <= cap(mp.MetricIDs) {
		mp.MetricIDs = mp.MetricIDs[:n]
	} else {
		mp.MetricIDs = append(mp.MetricIDs[:cap(mp.MetricIDs)], make([]uint64, n-cap(mp.MetricIDs))...)
	}
	metricIDs := mp.MetricIDs
	_ = metricIDs[n-1]
	for i := 0; i < n; i++ {
		if len(tail) < 8 {
			logger.Panicf("BUG: tail cannot be smaller than 8 bytes; got %d bytes; tail=%X", len(tail), tail)
			return
		}
		metricID := encoding.UnmarshalUint64(tail)
		metricIDs[i] = metricID
		tail = tail[8:]
	}
}

// IsDeletedTag verifies whether the tag from mp is deleted according to dmis.
//
// dmis must contain deleted MetricIDs.
func (mp *tagToMetricIDsRowParser) IsDeletedTag(dmis *uint64set.Set) bool {
	if dmis.Len() == 0 {
		return false
	}
	mp.ParseMetricIDs()
	for _, metricID := range mp.MetricIDs {
		if !dmis.Has(metricID) {
			return false
		}
	}
	return true
}

// merge的时候会回调这个函数
func mergeTagToMetricIDsRows(data []byte, items []mergeset.Item) ([]byte, []mergeset.Item) {
	data, items = mergeTagToMetricIDsRowsInternal(data, items, nsPrefixTagToMetricIDs)
	data, items = mergeTagToMetricIDsRowsInternal(data, items, nsPrefixDateTagToMetricIDs)
	return data, items
}

// 合并的时候，会回调这个函数
func mergeTagToMetricIDsRowsInternal(data []byte, items []mergeset.Item, nsPrefix byte) ([]byte, []mergeset.Item) {
	// Perform quick checks whether items contain rows starting from nsPrefix
	// based on the fact that items are sorted.
	if len(items) <= 2 {
		// The first and the last row must remain unchanged.
		return data, items
	}
	firstItem := items[0].Bytes(data)
	if len(firstItem) > 0 && firstItem[0] > nsPrefix {
		return data, items
	}
	lastItem := items[len(items)-1].Bytes(data)
	if len(lastItem) > 0 && lastItem[0] < nsPrefix {
		return data, items
	}

	// items contain at least one row starting from nsPrefix. Merge rows with common tag.
	tmm := getTagToMetricIDsRowsMerger()
	tmm.dataCopy = append(tmm.dataCopy[:0], data...)
	tmm.itemsCopy = append(tmm.itemsCopy[:0], items...)
	mp := &tmm.mp
	mpPrev := &tmm.mpPrev
	dstData := data[:0]
	dstItems := items[:0]
	for i, it := range items {
		item := it.Bytes(data)
		if len(item) == 0 || item[0] != nsPrefix || i == 0 || i == len(items)-1 {
			// Write rows not starting with nsPrefix as-is.
			// Additionally write the first and the last row as-is in order to preserve
			// sort order for adjacent blocks.
			dstData, dstItems = tmm.flushPendingMetricIDs(dstData, dstItems, mpPrev)
			dstData = append(dstData, item...)
			dstItems = append(dstItems, mergeset.Item{
				Start: uint32(len(dstData) - len(item)),
				End:   uint32(len(dstData)),
			})
			continue
		}
		if err := mp.Init(item, nsPrefix); err != nil {
			logger.Panicf("FATAL: cannot parse row starting with nsPrefix %d during merge: %s", nsPrefix, err)
		}
		if mp.MetricIDsLen() >= maxMetricIDsPerRow {
			dstData, dstItems = tmm.flushPendingMetricIDs(dstData, dstItems, mpPrev)
			dstData = append(dstData, item...)
			dstItems = append(dstItems, mergeset.Item{
				Start: uint32(len(dstData) - len(item)),
				End:   uint32(len(dstData)),
			})
			continue
		}
		if !mp.EqualPrefix(mpPrev) {
			dstData, dstItems = tmm.flushPendingMetricIDs(dstData, dstItems, mpPrev)
		}
		mp.ParseMetricIDs()
		tmm.pendingMetricIDs = append(tmm.pendingMetricIDs, mp.MetricIDs...)
		mpPrev, mp = mp, mpPrev
		if len(tmm.pendingMetricIDs) >= maxMetricIDsPerRow {
			dstData, dstItems = tmm.flushPendingMetricIDs(dstData, dstItems, mpPrev)
		}
	}
	if len(tmm.pendingMetricIDs) > 0 {
		logger.Panicf("BUG: tmm.pendingMetricIDs must be empty at this point; got %d items: %d", len(tmm.pendingMetricIDs), tmm.pendingMetricIDs)
	}
	if !checkItemsSorted(dstData, dstItems) {
		// Items could become unsorted if initial items contain duplicate metricIDs:
		//
		//   item1: 1, 1, 5
		//   item2: 1, 4
		//
		// Items could become the following after the merge:
		//
		//   item1: 1, 5
		//   item2: 1, 4
		//
		// i.e. item1 > item2
		//
		// Leave the original items unmerged, so they can be merged next time.
		// This case should be quite rare - if multiple data points are simultaneously inserted
		// into the same new time series from multiple concurrent goroutines.
		atomic.AddUint64(&indexBlocksWithMetricIDsIncorrectOrder, 1)
		dstData = append(dstData[:0], tmm.dataCopy...)
		dstItems = append(dstItems[:0], tmm.itemsCopy...)
		if !checkItemsSorted(dstData, dstItems) {
			logger.Panicf("BUG: the original items weren't sorted; items=%q", dstItems)
		}
	}
	putTagToMetricIDsRowsMerger(tmm)
	atomic.AddUint64(&indexBlocksWithMetricIDsProcessed, 1)
	return dstData, dstItems
}

var indexBlocksWithMetricIDsIncorrectOrder uint64
var indexBlocksWithMetricIDsProcessed uint64

func checkItemsSorted(data []byte, items []mergeset.Item) bool {
	if len(items) == 0 {
		return true
	}
	prevItem := items[0].String(data)
	for _, it := range items[1:] {
		currItem := it.String(data)
		if prevItem > currItem {
			return false
		}
		prevItem = currItem
	}
	return true
}

// maxMetricIDsPerRow limits the number of metricIDs in tag->metricIDs row.
//
// This reduces overhead on index and metaindex in lib/mergeset.
const maxMetricIDsPerRow = 64

type uint64Sorter []uint64

func (s uint64Sorter) Len() int { return len(s) }
func (s uint64Sorter) Less(i, j int) bool {
	return s[i] < s[j]
}
func (s uint64Sorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type tagToMetricIDsRowsMerger struct {
	pendingMetricIDs uint64Sorter
	mp               tagToMetricIDsRowParser
	mpPrev           tagToMetricIDsRowParser

	itemsCopy []mergeset.Item
	dataCopy  []byte
}

func (tmm *tagToMetricIDsRowsMerger) Reset() {
	tmm.pendingMetricIDs = tmm.pendingMetricIDs[:0]
	tmm.mp.Reset()
	tmm.mpPrev.Reset()

	tmm.itemsCopy = tmm.itemsCopy[:0]
	tmm.dataCopy = tmm.dataCopy[:0]
}

func (tmm *tagToMetricIDsRowsMerger) flushPendingMetricIDs(dstData []byte, dstItems []mergeset.Item,
	mp *tagToMetricIDsRowParser) ([]byte, []mergeset.Item) {
	if len(tmm.pendingMetricIDs) == 0 {
		// Nothing to flush
		return dstData, dstItems
	}
	// Use sort.Sort instead of sort.Slice in order to reduce memory allocations.
	sort.Sort(&tmm.pendingMetricIDs)
	tmm.pendingMetricIDs = removeDuplicateMetricIDs(tmm.pendingMetricIDs)

	// Marshal pendingMetricIDs
	dstDataLen := len(dstData)
	dstData = mp.MarshalPrefix(dstData)
	for _, metricID := range tmm.pendingMetricIDs {
		dstData = encoding.MarshalUint64(dstData, metricID)
	}
	dstItems = append(dstItems, mergeset.Item{
		Start: uint32(dstDataLen),
		End:   uint32(len(dstData)),
	})
	tmm.pendingMetricIDs = tmm.pendingMetricIDs[:0]
	return dstData, dstItems
}

func removeDuplicateMetricIDs(sortedMetricIDs []uint64) []uint64 {
	if len(sortedMetricIDs) < 2 {
		return sortedMetricIDs
	}
	prevMetricID := sortedMetricIDs[0]
	hasDuplicates := false
	for _, metricID := range sortedMetricIDs[1:] {
		if prevMetricID == metricID {
			hasDuplicates = true
			break
		}
		prevMetricID = metricID
	}
	if !hasDuplicates {
		return sortedMetricIDs
	}
	dstMetricIDs := sortedMetricIDs[:1]
	prevMetricID = sortedMetricIDs[0]
	for _, metricID := range sortedMetricIDs[1:] {
		if prevMetricID == metricID {
			continue
		}
		dstMetricIDs = append(dstMetricIDs, metricID)
		prevMetricID = metricID
	}
	return dstMetricIDs
}

func getTagToMetricIDsRowsMerger() *tagToMetricIDsRowsMerger {
	v := tmmPool.Get()
	if v == nil {
		return &tagToMetricIDsRowsMerger{}
	}
	return v.(*tagToMetricIDsRowsMerger)
}

func putTagToMetricIDsRowsMerger(tmm *tagToMetricIDsRowsMerger) {
	tmm.Reset()
	tmmPool.Put(tmm)
}

var tmmPool sync.Pool
