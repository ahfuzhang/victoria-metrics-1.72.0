package storage

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
)

func getMaxCachedIndexBlocksPerPart() int {
	maxCachedIndexBlocksPerPartOnce.Do(func() {
		n := memory.Allowed() / 1024 / 1024 / 8
		if n < 16 {
			n = 16
		}
		maxCachedIndexBlocksPerPart = n
	})
	return maxCachedIndexBlocksPerPart
}

var (
	maxCachedIndexBlocksPerPart     int
	maxCachedIndexBlocksPerPartOnce sync.Once
)

// part represents a searchable part containing time series data.
type part struct {  // 每个 part 管理一部分 tsid
	ph partHeader

	// Filesystem path to the part.
	//
	// Empty for in-memory part.
	path string

	// Total size in bytes of part data.
	size uint64

	timestampsFile fs.MustReadAtCloser   // timestamp.bin
	valuesFile     fs.MustReadAtCloser   // values.bin
	indexFile      fs.MustReadAtCloser   // index.bin

	metaindex []metaindexRow  //  metaindex.bin 解析后的内容， 按照 tsid 排序

	ibCache *indexBlockCache
}

// openFilePart opens file-based part from the given path.
func openFilePart(path string) (*part, error) {  //打开各个bin文件
	path = filepath.Clean(path)

	var ph partHeader
	if err := ph.ParseFromPath(path); err != nil {
		return nil, fmt.Errorf("cannot parse path to part: %w", err)
	}

	timestampsPath := path + "/timestamps.bin"
	timestampsFile := fs.MustOpenReaderAt(timestampsPath)  // 内存映射文件
	timestampsSize := fs.MustFileSize(timestampsPath)

	valuesPath := path + "/values.bin"
	valuesFile := fs.MustOpenReaderAt(valuesPath)  // 内存映射文件
	valuesSize := fs.MustFileSize(valuesPath)

	indexPath := path + "/index.bin"
	indexFile := fs.MustOpenReaderAt(indexPath)  // 内存映射文件
	indexSize := fs.MustFileSize(indexPath)

	metaindexPath := path + "/metaindex.bin"
	metaindexFile, err := filestream.Open(metaindexPath, true)
	if err != nil {
		timestampsFile.MustClose()
		valuesFile.MustClose()
		indexFile.MustClose()
		return nil, fmt.Errorf("cannot open metaindex file: %w", err)
	}
	metaindexSize := fs.MustFileSize(metaindexPath)

	size := timestampsSize + valuesSize + indexSize + metaindexSize
	return newPart(&ph, path, size, metaindexFile, timestampsFile, valuesFile, indexFile)  //构造 part 对象
}

// newPart returns new part initialized with the given arguments.
//
// The returned part calls MustClose on all the files passed to newPart
// when calling part.MustClose.  // 打开数据文件的part
func newPart(ph *partHeader, path string, size uint64, metaindexReader filestream.ReadCloser, timestampsFile, valuesFile, indexFile fs.MustReadAtCloser) (*part, error) {
	var errors []error
	metaindex, err := unmarshalMetaindexRows(nil, metaindexReader)  // 解析 metaindex.bin 文件， metaindex数组按照tsid排序
	if err != nil {
		errors = append(errors, fmt.Errorf("cannot unmarshal metaindex data: %w", err))
	}
	metaindexReader.MustClose()

	var p part
	p.ph = *ph
	p.path = path
	p.size = size
	p.timestampsFile = timestampsFile  // timestamp.bin
	p.valuesFile = valuesFile  // values.bin
	p.indexFile = indexFile  // index.bin

	p.metaindex = metaindex  // 按照 tsid 排序的数组
	p.ibCache = newIndexBlockCache()  // key:偏移量 value: IndexBlock对象

	if len(errors) > 0 {
		// Return only the first error, since it has no sense in returning all errors.
		err = fmt.Errorf("cannot initialize part %q: %w", &p, errors[0])
		p.MustClose()
		return nil, err
	}

	return &p, nil
}

// String returns human-readable representation of p.
func (p *part) String() string {
	if len(p.path) > 0 {
		return p.path
	}
	return p.ph.String()
}

// MustClose closes all the part files.
func (p *part) MustClose() {
	p.timestampsFile.MustClose()
	p.valuesFile.MustClose()
	p.indexFile.MustClose()

	isBig := p.size > maxSmallPartSize()
	p.ibCache.MustClose(isBig)
}

type indexBlock struct {
	bhs []blockHeader
}

func (idxb *indexBlock) SizeBytes() int {
	return cap(idxb.bhs) * int(unsafe.Sizeof(blockHeader{}))
}

type indexBlockCache struct {
	// Put atomic counters to the top of struct in order to align them to 8 bytes on 32-bit architectures.
	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/212
	requests uint64
	misses   uint64

	m  map[uint64]*indexBlockCacheEntry  //以偏移量为key, value为blockHeader数组
	mu sync.RWMutex

	cleanerStopCh chan struct{}
	cleanerWG     sync.WaitGroup
}

type indexBlockCacheEntry struct {
	// Atomically updated counters must go first in the struct, so they are properly
	// aligned to 8 bytes on 32-bit architectures.
	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/212
	lastAccessTime uint64

	ib *indexBlock
}

func newIndexBlockCache() *indexBlockCache {
	var ibc indexBlockCache
	ibc.m = make(map[uint64]*indexBlockCacheEntry)

	ibc.cleanerStopCh = make(chan struct{})
	ibc.cleanerWG.Add(1)
	go func() {
		defer ibc.cleanerWG.Done()
		ibc.cleaner()
	}()
	return &ibc
}

func (ibc *indexBlockCache) MustClose(isBig bool) {
	close(ibc.cleanerStopCh)
	ibc.cleanerWG.Wait()
	ibc.m = nil
}

// cleaner periodically cleans least recently used items.
func (ibc *indexBlockCache) cleaner() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ibc.cleanByTimeout()
		case <-ibc.cleanerStopCh:
			return
		}
	}
}

func (ibc *indexBlockCache) cleanByTimeout() {  // 每30秒执行一次
	currentTime := fasttime.UnixTimestamp()
	ibc.mu.Lock()
	for k, ibe := range ibc.m {
		// Delete items accessed more than two minutes ago.
		// This time should be enough for repeated queries.
		if currentTime-atomic.LoadUint64(&ibe.lastAccessTime) > 2*60 {
			delete(ibc.m, k)  //超过120秒的key会被删除掉
		}
	}
	ibc.mu.Unlock()
}

func (ibc *indexBlockCache) Get(k uint64) *indexBlock {
	atomic.AddUint64(&ibc.requests, 1)

	ibc.mu.RLock()
	ibe := ibc.m[k]
	ibc.mu.RUnlock()

	if ibe != nil {
		currentTime := fasttime.UnixTimestamp()
		if atomic.LoadUint64(&ibe.lastAccessTime) != currentTime {
			atomic.StoreUint64(&ibe.lastAccessTime, currentTime)
		}
		return ibe.ib
	}
	atomic.AddUint64(&ibc.misses, 1)
	return nil
}

func (ibc *indexBlockCache) Put(k uint64, ib *indexBlock) {
	ibc.mu.Lock()

	// Clean superfluous cache entries.
	if overflow := len(ibc.m) - getMaxCachedIndexBlocksPerPart(); overflow > 0 {
		// Remove 10% of items from the cache.
		overflow = int(float64(len(ibc.m)) * 0.1)
		for k := range ibc.m {
			delete(ibc.m, k)
			overflow--
			if overflow == 0 {
				break
			}
		}
	}

	// Store frequently requested ib in the cache.
	ibe := &indexBlockCacheEntry{
		lastAccessTime: fasttime.UnixTimestamp(),
		ib:             ib,
	}
	ibc.m[k] = ibe
	ibc.mu.Unlock()
}

func (ibc *indexBlockCache) Requests() uint64 {
	return atomic.LoadUint64(&ibc.requests)
}

func (ibc *indexBlockCache) Misses() uint64 {
	return atomic.LoadUint64(&ibc.misses)
}

func (ibc *indexBlockCache) Len() uint64 {
	ibc.mu.Lock()
	n := uint64(len(ibc.m))
	ibc.mu.Unlock()
	return n
}

func (ibc *indexBlockCache) SizeBytes() uint64 {
	n := 0
	ibc.mu.Lock()
	for _, e := range ibc.m {
		n += e.ib.SizeBytes()
	}
	ibc.mu.Unlock()
	return uint64(n)
}

func (ibc *indexBlockCache) SizeMaxBytes() uint64 {
	avgBlockSize := float64(64 * 1024)
	blocksCount := ibc.Len()
	if blocksCount > 0 {
		avgBlockSize = float64(ibc.SizeBytes()) / float64(blocksCount)
	}
	return uint64(avgBlockSize * float64(getMaxCachedIndexBlocksPerPart()))
}
