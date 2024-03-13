package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/uint64set"
)

// table represents a single table with time series data.  // 数据表对象, 存储time series的data部分
type table struct {
	path                string  // 存储路径
	smallPartitionsPath string  // 分为大小分区
	bigPartitionsPath   string

	getDeletedMetricIDs func() *uint64set.Set
	retentionMsecs      int64  //默认31天

	ptws     []*partitionWrapper  // table下面包含多个 partition
	ptwsLock sync.Mutex

	flockF *os.File

	stop chan struct{}

	retentionWatcherWG  sync.WaitGroup
	finalDedupWatcherWG sync.WaitGroup
}

// partitionWrapper provides refcounting mechanism for the partition.
type partitionWrapper struct {
	// Atomic counters must be at the top of struct for proper 8-byte alignment on 32-bit archs.
	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/212

	refCount uint64  // 通过引用计数，解决并发问题

	// The partition must be dropped if mustDrop > 0
	mustDrop uint64

	pt *partition
}

func (ptw *partitionWrapper) incRef() {
	atomic.AddUint64(&ptw.refCount, 1)
}

func (ptw *partitionWrapper) decRef() {
	n := atomic.AddUint64(&ptw.refCount, ^uint64(0))
	if int64(n) < 0 {
		logger.Panicf("BUG: pts.refCount must be positive; got %d", int64(n))
	}
	if n > 0 {
		return
	}
	// 引用计数为0后，丢弃这个partition
	// refCount is zero. Close the partition.
	ptw.pt.MustClose()

	if atomic.LoadUint64(&ptw.mustDrop) == 0 {
		ptw.pt = nil
		return
	}

	// ptw.mustDrop > 0. Drop the partition.
	ptw.pt.Drop()  // 删除目录下的所有文件
	ptw.pt = nil
}

func (ptw *partitionWrapper) scheduleToDrop() {
	atomic.AddUint64(&ptw.mustDrop, 1)
}

// openTable opens a table on the given path with the given retentionMsecs.
//
// The table is created if it doesn't exist.
//
// Data older than the retentionMsecs may be dropped at any time.  //  打开数据文件
func openTable(path string, getDeletedMetricIDs func() *uint64set.Set, retentionMsecs int64) (*table, error) {
	path = filepath.Clean(path)

	// Create a directory for the table if it doesn't exist yet.
	if err := fs.MkdirAllIfNotExist(path); err != nil {
		return nil, fmt.Errorf("cannot create directory for table %q: %w", path, err)
	}

	// Protect from concurrent opens.
	flockF, err := fs.CreateFlockFile(path)
	if err != nil {
		return nil, err
	}

	// Create directories for small and big partitions if they don't exist yet.  //只有一个table, table分为big和small两部分。每个big/small下又有多个partition
	smallPartitionsPath := path + "/small"
	if err := fs.MkdirAllIfNotExist(smallPartitionsPath); err != nil {
		return nil, fmt.Errorf("cannot create directory for small partitions %q: %w", smallPartitionsPath, err)
	}
	smallSnapshotsPath := smallPartitionsPath + "/snapshots"
	if err := fs.MkdirAllIfNotExist(smallSnapshotsPath); err != nil {
		return nil, fmt.Errorf("cannot create %q: %w", smallSnapshotsPath, err)
	}
	bigPartitionsPath := path + "/big"
	if err := fs.MkdirAllIfNotExist(bigPartitionsPath); err != nil {
		return nil, fmt.Errorf("cannot create directory for big partitions %q: %w", bigPartitionsPath, err)
	}
	bigSnapshotsPath := bigPartitionsPath + "/snapshots"
	if err := fs.MkdirAllIfNotExist(bigSnapshotsPath); err != nil {
		return nil, fmt.Errorf("cannot create %q: %w", bigSnapshotsPath, err)
	}

	// Open partitions.
	pts, err := openPartitions(smallPartitionsPath, bigPartitionsPath, getDeletedMetricIDs, retentionMsecs)  //打开partition
	if err != nil {
		return nil, fmt.Errorf("cannot open partitions in the table %q: %w", path, err)
	}

	tb := &table{
		path:                path,
		smallPartitionsPath: smallPartitionsPath,
		bigPartitionsPath:   bigPartitionsPath,
		getDeletedMetricIDs: getDeletedMetricIDs,
		retentionMsecs:      retentionMsecs,

		flockF: flockF,

		stop: make(chan struct{}),
	}
	for _, pt := range pts {
		tb.addPartitionNolock(pt)  // 把 partition 加到 table 对象上面
	}
	tb.startRetentionWatcher()  // 每分钟检查一次，partition是否过期
	tb.startFinalDedupWatcher()  // 每小时一次
	return tb, nil
}

// CreateSnapshot creates tb snapshot and returns paths to small and big parts of it.
func (tb *table) CreateSnapshot(snapshotName string) (string, string, error) {
	logger.Infof("creating table snapshot of %q...", tb.path)
	startTime := time.Now()

	ptws := tb.GetPartitions(nil)  //把partition拷贝出来，默认情况下是1个
	defer tb.PutPartitions(ptws)

	dstSmallDir := fmt.Sprintf("%s/small/snapshots/%s", tb.path, snapshotName)
	if err := fs.MkdirAllFailIfExist(dstSmallDir); err != nil {
		return "", "", fmt.Errorf("cannot create dir %q: %w", dstSmallDir, err)
	}
	dstBigDir := fmt.Sprintf("%s/big/snapshots/%s", tb.path, snapshotName)
	if err := fs.MkdirAllFailIfExist(dstBigDir); err != nil {
		return "", "", fmt.Errorf("cannot create dir %q: %w", dstBigDir, err)
	}

	for _, ptw := range ptws {  //遍历每个partition
		smallPath := dstSmallDir + "/" + ptw.pt.name
		bigPath := dstBigDir + "/" + ptw.pt.name
		if err := ptw.pt.CreateSnapshotAt(smallPath, bigPath); err != nil {
			return "", "", fmt.Errorf("cannot create snapshot for partition %q in %q: %w", ptw.pt.name, tb.path, err)
		}
	}

	fs.MustSyncPath(dstSmallDir)
	fs.MustSyncPath(dstBigDir)
	fs.MustSyncPath(filepath.Dir(dstSmallDir))
	fs.MustSyncPath(filepath.Dir(dstBigDir))

	logger.Infof("created table snapshot for %q at (%q, %q) in %.3f seconds", tb.path, dstSmallDir, dstBigDir, time.Since(startTime).Seconds())
	return dstSmallDir, dstBigDir, nil
}

// MustDeleteSnapshot deletes snapshot with the given snapshotName.
func (tb *table) MustDeleteSnapshot(snapshotName string) {
	smallDir := fmt.Sprintf("%s/small/snapshots/%s", tb.path, snapshotName)
	fs.MustRemoveAll(smallDir)
	bigDir := fmt.Sprintf("%s/big/snapshots/%s", tb.path, snapshotName)
	fs.MustRemoveAll(bigDir)
}

func (tb *table) addPartitionNolock(pt *partition) {
	ptw := &partitionWrapper{
		pt:       pt,
		refCount: 1,
	}
	tb.ptws = append(tb.ptws, ptw)
}

// MustClose closes the table.
// It is expected that all the pending searches on the table are finished before calling MustClose.
func (tb *table) MustClose() {
	close(tb.stop)
	tb.retentionWatcherWG.Wait()
	tb.finalDedupWatcherWG.Wait()

	tb.ptwsLock.Lock()
	ptws := tb.ptws
	tb.ptws = nil
	tb.ptwsLock.Unlock()

	for _, ptw := range ptws {
		if n := atomic.LoadUint64(&ptw.refCount); n != 1 {
			logger.Panicf("BUG: unexpected refCount=%d when closing the partition; probably there are pending searches", n)
		}
		ptw.decRef()
	}

	// Release exclusive lock on the table.
	if err := tb.flockF.Close(); err != nil {
		logger.Panicf("FATAL: cannot release lock on %q: %s", tb.flockF.Name(), err)
	}
}

// flushRawRows flushes all the pending rows, so they become visible to search.
//
// This function is for debug purposes only.
func (tb *table) flushRawRows() {
	ptws := tb.GetPartitions(nil)
	defer tb.PutPartitions(ptws)

	for _, ptw := range ptws {
		ptw.pt.flushRawRows(true)
	}
}

// TableMetrics contains essential metrics for the table.
type TableMetrics struct {
	partitionMetrics

	PartitionsRefCount uint64
}

// UpdateMetrics updates m with metrics from tb.
func (tb *table) UpdateMetrics(m *TableMetrics) {
	tb.ptwsLock.Lock()
	for _, ptw := range tb.ptws {
		ptw.pt.UpdateMetrics(&m.partitionMetrics)
		m.PartitionsRefCount += atomic.LoadUint64(&ptw.refCount)
	}
	tb.ptwsLock.Unlock()
}

// ForceMergePartitions force-merges partitions in tb with names starting from the given partitionNamePrefix.
//
// Partitions are merged sequentially in order to reduce load on the system.
func (tb *table) ForceMergePartitions(partitionNamePrefix string) error {
	ptws := tb.GetPartitions(nil)
	defer tb.PutPartitions(ptws)
	for _, ptw := range ptws {
		if !strings.HasPrefix(ptw.pt.name, partitionNamePrefix) {
			continue
		}
		logger.Infof("starting forced merge for partition %q", ptw.pt.name)
		startTime := time.Now()
		if err := ptw.pt.ForceMergeAllParts(); err != nil {
			return fmt.Errorf("cannot complete forced merge for partition %q: %w", ptw.pt.name, err)
		}
		logger.Infof("forced merge for partition %q has been finished in %.3f seconds", ptw.pt.name, time.Since(startTime).Seconds())
	}
	return nil
}

// AddRows adds the given rows to the table tb.
func (tb *table) AddRows(rows []rawRow) error {  // 插入数据部分
	if len(rows) == 0 {
		return nil
	}

	// Verify whether all the rows may be added to a single partition.
	ptwsX := getPartitionWrappers()  // 从内存池获取对象
	defer putPartitionWrappers(ptwsX)

	ptwsX.a = tb.GetPartitions(ptwsX.a[:0])  // 拷贝多个数据分区对象
	ptws := ptwsX.a
	for i, ptw := range ptws {  //遍历每个 partition
		singlePt := true
		for j := range rows {  //遍历每行
			if !ptw.pt.HasTimestamp(rows[j].Timestamp) {  // 检查当前分区是否在支持的时间范围内
				singlePt = false
				break
			}
		}
		if !singlePt {
			continue  // ？？？ 这里不明白，干嘛不找每个time series各自适合的分区？ 看起来是全部分区都没找到的情况下，在这个循环后的代码中处理的
		}

		if i != 0 {
			// Move the partition with the matching rows to the front of tb.ptws,
			// so it will be detected faster next time.
			tb.ptwsLock.Lock()
			for j := range tb.ptws {
				if ptw == tb.ptws[j] {
					tb.ptws[0], tb.ptws[j] = tb.ptws[j], tb.ptws[0]  // ??? 没看懂
					break
				}
			}
			tb.ptwsLock.Unlock()
		}

		// Fast path - add all the rows into the ptw.
		ptw.pt.AddRows(rows)
		tb.PutPartitions(ptws)  //取消对分区的引用，通过减少引用计数来实现
		return nil
	}

	// Slower path - split rows into per-partition buckets.
	ptBuckets := make(map[*partitionWrapper][]rawRow)
	var missingRows []rawRow
	for i := range rows {
		r := &rows[i]
		ptFound := false
		for _, ptw := range ptws {
			if ptw.pt.HasTimestamp(r.Timestamp) {
				ptBuckets[ptw] = append(ptBuckets[ptw], *r)
				ptFound = true
				break
			}
		}
		if !ptFound {
			missingRows = append(missingRows, *r)
		}
	}

	for ptw, ptRows := range ptBuckets {
		ptw.pt.AddRows(ptRows)
	}
	tb.PutPartitions(ptws)
	if len(missingRows) == 0 {
		return nil
	}

	// The slowest path - there are rows that don't fit any existing partition.
	// Create new partitions for these rows.
	// Do this under tb.ptwsLock.
	minTimestamp, maxTimestamp := tb.getMinMaxTimestamps()
	tb.ptwsLock.Lock()
	var errors []error
	for i := range missingRows {
		r := &missingRows[i]

		if r.Timestamp < minTimestamp || r.Timestamp > maxTimestamp {
			// Silently skip row outside retention, since it should be deleted anyway.
			continue
		}

		// Make sure the partition for the r hasn't been added by another goroutines.
		ptFound := false
		for _, ptw := range tb.ptws {
			if ptw.pt.HasTimestamp(r.Timestamp) {
				ptFound = true
				ptw.pt.AddRows(missingRows[i : i+1])
				break
			}
		}
		if ptFound {
			continue
		}
		// 如果每个月才一个分区，是不是要月份切换的时候，才会触发创建分区 ???
		pt, err := createPartition(r.Timestamp, tb.smallPartitionsPath, tb.bigPartitionsPath, tb.getDeletedMetricIDs, tb.retentionMsecs)
		if err != nil {
			errors = append(errors, err)
			continue
		}
		pt.AddRows(missingRows[i : i+1])
		tb.addPartitionNolock(pt)
	}
	tb.ptwsLock.Unlock()

	if len(errors) > 0 {
		// Return only the first error, since it has no sense in returning all errors.
		return fmt.Errorf("errors while adding rows to table %q: %w", tb.path, errors[0])
	}
	return nil
}

func (tb *table) getMinMaxTimestamps() (int64, int64) {
	now := int64(fasttime.UnixTimestamp() * 1000)
	minTimestamp := now - tb.retentionMsecs
	maxTimestamp := now + 2*24*3600*1000 // allow max +2 days from now due to timezones shit :)
	if minTimestamp < 0 {
		// Negative timestamps aren't supported by the storage.
		minTimestamp = 0
	}
	if maxTimestamp < 0 {
		maxTimestamp = (1 << 63) - 1
	}
	return minTimestamp, maxTimestamp
}

func (tb *table) startRetentionWatcher() {
	tb.retentionWatcherWG.Add(1)
	go func() {
		tb.retentionWatcher()
		tb.retentionWatcherWG.Done()
	}()
}

func (tb *table) retentionWatcher() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-tb.stop:
			return
		case <-ticker.C:  // 阻塞一分钟
		}

		minTimestamp := int64(fasttime.UnixTimestamp()*1000) - tb.retentionMsecs
		var ptwsDrop []*partitionWrapper
		tb.ptwsLock.Lock()
		dst := tb.ptws[:0]
		for _, ptw := range tb.ptws {
			if ptw.pt.tr.MaxTimestamp < minTimestamp {
				ptwsDrop = append(ptwsDrop, ptw)  //选出超过时间范围的分区，丢弃他们
			} else {
				dst = append(dst, ptw)
			}
		}
		tb.ptws = dst
		tb.ptwsLock.Unlock()

		if len(ptwsDrop) == 0 {
			continue
		}

		// There are paritions to drop. Drop them.

		// Remove table references from partitions, so they will be eventually
		// closed and dropped after all the pending searches are done.
		for _, ptw := range ptwsDrop {
			ptw.scheduleToDrop()
			ptw.decRef()  // 超过管理时间范围以外的partition, 通过触发引用计数为0来删除老文件
		}
	}
}

func (tb *table) startFinalDedupWatcher() {
	tb.finalDedupWatcherWG.Add(1)
	go func() {
		tb.finalDedupWatcher()
		tb.finalDedupWatcherWG.Done()
	}()
}

func (tb *table) finalDedupWatcher() {
	if !isDedupEnabled() {
		// Deduplication is disabled.
		return
	}
	f := func() {
		ptws := tb.GetPartitions(nil)
		defer tb.PutPartitions(ptws)
		timestamp := timestampFromTime(time.Now())
		currentPartitionName := timestampToPartitionName(timestamp)
		for _, ptw := range ptws {
			if ptw.pt.name == currentPartitionName {
				// Do not run final dedup for the current month.
				continue
			}
			if err := ptw.pt.runFinalDedup(); err != nil {
				logger.Errorf("cannot run final dedup for partition %s: %s", ptw.pt.name, err)
				continue
			}
		}
	}
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		select {
		case <-tb.stop:
			return
		case <-t.C:  //阻塞一小时
			f()
		}
	}
}

// GetPartitions appends tb's partitions snapshot to dst and returns the result.
//
// The returned partitions must be passed to PutPartitions
// when they no longer needed.
func (tb *table) GetPartitions(dst []*partitionWrapper) []*partitionWrapper {
	tb.ptwsLock.Lock()
	for _, ptw := range tb.ptws {
		ptw.incRef()  // 通过引用计数来复制 数据分区 的对象
		dst = append(dst, ptw)
	}
	tb.ptwsLock.Unlock()

	return dst
}

// PutPartitions deregisters ptws obtained via GetPartitions.
func (tb *table) PutPartitions(ptws []*partitionWrapper) {
	for _, ptw := range ptws {
		ptw.decRef()
	}
}

func openPartitions(smallPartitionsPath, bigPartitionsPath string, getDeletedMetricIDs func() *uint64set.Set, retentionMsecs int64) ([]*partition, error) {
	// Certain partition directories in either `big` or `small` dir may be missing
	// after restoring from backup. So populate partition names from both dirs.  // 打开partition
	ptNames := make(map[string]bool)
	if err := populatePartitionNames(smallPartitionsPath, ptNames); err != nil {  //遍历目录
		return nil, err
	}
	if err := populatePartitionNames(bigPartitionsPath, ptNames); err != nil {
		return nil, err
	}
	var pts []*partition
	for ptName := range ptNames {
		smallPartsPath := smallPartitionsPath + "/" + ptName
		bigPartsPath := bigPartitionsPath + "/" + ptName
		pt, err := openPartition(smallPartsPath, bigPartsPath, getDeletedMetricIDs, retentionMsecs)  // 打开具体的某个分区
		if err != nil {
			mustClosePartitions(pts)
			return nil, fmt.Errorf("cannot open partition %q: %w", ptName, err)
		}
		pts = append(pts, pt)
	}
	return pts, nil
}

func populatePartitionNames(partitionsPath string, ptNames map[string]bool) error {  //遍历目录，返回合法的partition目录名称
	d, err := os.Open(partitionsPath)
	if err != nil {
		return fmt.Errorf("cannot open directory with partitions %q: %w", partitionsPath, err)
	}
	defer fs.MustClose(d)

	fis, err := d.Readdir(-1)
	if err != nil {
		return fmt.Errorf("cannot read directory with partitions %q: %w", partitionsPath, err)
	}
	for _, fi := range fis {
		if !fs.IsDirOrSymlink(fi) {
			// Skip non-directories
			continue
		}
		ptName := fi.Name()
		if ptName == "snapshots" {
			// Skip directory with snapshots
			continue
		}
		ptNames[ptName] = true
	}
	return nil
}

func mustClosePartitions(pts []*partition) {
	for _, pt := range pts {
		pt.MustClose()
	}
}

type partitionWrappers struct {
	a []*partitionWrapper  // 数据分区对象
}

func getPartitionWrappers() *partitionWrappers {
	v := ptwsPool.Get()
	if v == nil {
		return &partitionWrappers{}
	}
	return v.(*partitionWrappers)
}

func putPartitionWrappers(ptwsX *partitionWrappers) {
	ptwsX.a = ptwsX.a[:0]
	ptwsPool.Put(ptwsX)
}

var ptwsPool sync.Pool
