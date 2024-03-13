package mergeset

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
		n := memory.Allowed() / 1024 / 1024 / 4  // 假设最大4GB内存,则 n=256
		if n == 0 {
			n = 10
		}
		maxCachedIndexBlocksPerPart = n
	})
	return maxCachedIndexBlocksPerPart
}

var (
	maxCachedIndexBlocksPerPart     int
	maxCachedIndexBlocksPerPartOnce sync.Once
)

func getMaxCachedInmemoryBlocksPerPart() int {  //每个part允许cache的总量
	maxCachedInmemoryBlocksPerPartOnce.Do(func() {
		n := memory.Allowed() / 1024 / 1024 / 4  //128GB内存，等于32768
		if n == 0 {
			n = 10
		}
		maxCachedInmemoryBlocksPerPart = n
	})
	return maxCachedInmemoryBlocksPerPart
}  // 32768块 * 64KB = 2gb, 每个part最多允许2GB缓存

var (
	maxCachedInmemoryBlocksPerPart     int
	maxCachedInmemoryBlocksPerPartOnce sync.Once
)

type part struct {  // 所有的time series应该是排序后存储的，每个part对象管理一部分time sereis
	ph partHeader  // 头部信息

	path string

	size uint64

	mrs []metaindexRow  //这个数组的内容来自对 metaindex.bin文件的解析
		// 数组按照 firstItem 来排序，便于做二分查找
	indexFile fs.MustReadAtCloser  // 三个内存映射文件  index.bin
	itemsFile fs.MustReadAtCloser  //  items.bin
	lensFile  fs.MustReadAtCloser  // lens.bin

	idxbCache *indexBlockCache  // 从 index.bin中加载的数据，放在cache里面  //两级缓存。这一级缓存indexBlock
	ibCache   *inmemoryBlockCache  // 以偏移量为key  //  这一级缓存 block
}

func openFilePart(path string) (*part, error) {  // 打开具体的一个part
	path = filepath.Clean(path)

	var ph partHeader  //包含文件夹名称中的 items count, blocks count等信息
	if err := ph.ParseFromPath(path); err != nil {
		return nil, fmt.Errorf("cannot parse path to part: %w", err)
	}

	metaindexPath := path + "/metaindex.bin"  // 全部读到内存，ZSTD解压，解析成数组
	metaindexFile, err := filestream.Open(metaindexPath, true)
	if err != nil {
		return nil, fmt.Errorf("cannot open %q: %w", metaindexPath, err)
	}
	metaindexSize := fs.MustFileSize(metaindexPath)

	indexPath := path + "/index.bin"  // 内存映射文件
	indexFile := fs.MustOpenReaderAt(indexPath)
	indexSize := fs.MustFileSize(indexPath)

	itemsPath := path + "/items.bin"  // 内存映射文件
	itemsFile := fs.MustOpenReaderAt(itemsPath)
	itemsSize := fs.MustFileSize(itemsPath)

	lensPath := path + "/lens.bin"  // 内存映射文件
	lensFile := fs.MustOpenReaderAt(lensPath)
	lensSize := fs.MustFileSize(lensPath)

	size := metaindexSize + indexSize + itemsSize + lensSize
	return newPart(&ph, path, size, metaindexFile, indexFile, itemsFile, lensFile)
}

func newPart(ph *partHeader, path string, size uint64, metaindexReader filestream.ReadCloser, indexFile, itemsFile, lensFile fs.MustReadAtCloser) (*part, error) {
	var errors []error  // 根据文件打开part目录下的数据文件
	mrs, err := unmarshalMetaindexRows(nil, metaindexReader)  // 解析 metaindex.bin
	if err != nil {
		errors = append(errors, fmt.Errorf("cannot unmarshal metaindexRows: %w", err))
	}
	metaindexReader.MustClose()

	var p part
	p.path = path  // path为空字符串，说明是一个 inmemory part
	p.size = size
	p.mrs = mrs  // []metaindexRow 数组的内容，数组按照firstItem进行排序

	p.indexFile = indexFile  // 三个内存映射文件
	p.itemsFile = itemsFile
	p.lensFile = lensFile

	p.ph.CopyFrom(ph)  // part head
	p.idxbCache = newIndexBlockCache()  //以偏移量为key的fastcache
	p.ibCache = newInmemoryBlockCache()  // 这个很重要

	if len(errors) > 0 {
		// Return only the first error, since it has no sense in returning all errors.
		err := fmt.Errorf("error opening part %s: %w", p.path, errors[0])
		p.MustClose()
		return nil, err
	}
	return &p, nil
}

func (p *part) MustClose() {
	p.indexFile.MustClose()
	p.itemsFile.MustClose()
	p.lensFile.MustClose()

	p.idxbCache.MustClose()
	p.ibCache.MustClose()
}

type indexBlock struct {  // 内容来自 index.bin 文件
	bhs []blockHeader  // 按照 first item排序的数组，可用于二分查找
}

func (idxb *indexBlock) SizeBytes() int {
	bhs := idxb.bhs[:cap(idxb.bhs)]
	n := int(unsafe.Sizeof(*idxb))
	for i := range bhs {
		n += bhs[i].SizeBytes()
	}
	return n
}

type indexBlockCache struct {  //cache的容量没有限制，整个part的index.bin可能都会被装载
	// Atomically updated counters must go first in the struct, so they are properly
	// aligned to 8 bytes on 32-bit architectures.
	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/212
	requests uint64
	misses   uint64  // 总的缓存没命中的次数

	m  map[uint64]*indexBlockCacheEntry  //以偏移量为key, value为 indexBlock 对象
	mu sync.RWMutex

	perKeyMisses     map[uint64]int  // 猜测是记录没有命中的数据，120秒清理一次
	perKeyMissesLock sync.Mutex

	cleanerStopCh chan struct{}
	cleanerWG     sync.WaitGroup
}

type indexBlockCacheEntry struct {
	// Atomically updated counters must go first in the struct, so they are properly
	// aligned to 8 bytes on 32-bit architectures.
	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/212
	lastAccessTime uint64  //记录最后一次访问时间

	idxb *indexBlock
}

func newIndexBlockCache() *indexBlockCache {
	var idxbc indexBlockCache
	idxbc.m = make(map[uint64]*indexBlockCacheEntry)
	idxbc.perKeyMisses = make(map[uint64]int)
	idxbc.cleanerStopCh = make(chan struct{})
	idxbc.cleanerWG.Add(1)
	go func() {
		defer idxbc.cleanerWG.Done()
		idxbc.cleaner()  //开启一个协程定期清理过期数据
	}()
	return &idxbc
}

func (idxbc *indexBlockCache) MustClose() {
	close(idxbc.cleanerStopCh)
	idxbc.cleanerWG.Wait()
	idxbc.m = nil
	idxbc.perKeyMisses = nil
}

// cleaner periodically cleans least recently used items.
func (idxbc *indexBlockCache) cleaner() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	perKeyMissesTicker := time.NewTicker(2 * time.Minute)
	defer perKeyMissesTicker.Stop()
	for {
		select {
		case <-ticker.C:  // 每30秒触发
			idxbc.cleanByTimeout()
		case <-perKeyMissesTicker.C:  // 每2分钟触发
			idxbc.perKeyMissesLock.Lock()
			idxbc.perKeyMisses = make(map[uint64]int, len(idxbc.perKeyMisses))
			idxbc.perKeyMissesLock.Unlock()
		case <-idxbc.cleanerStopCh:
			return
		}
	}
}

func (idxbc *indexBlockCache) cleanByTimeout() {  //删除超过120秒的item
	currentTime := fasttime.UnixTimestamp()
	idxbc.mu.Lock()
	for k, idxbe := range idxbc.m {
		// Delete items accessed more than two minutes ago.
		// This time should be enough for repeated queries.
		if currentTime-atomic.LoadUint64(&idxbe.lastAccessTime) > 2*60 {
			delete(idxbc.m, k)
		}
	}
	idxbc.mu.Unlock()
}

func (idxbc *indexBlockCache) Get(k uint64) *indexBlock {  //在缓存中查询
	atomic.AddUint64(&idxbc.requests, 1)
	idxbc.mu.RLock()
	idxbe := idxbc.m[k]
	idxbc.mu.RUnlock()

	if idxbe != nil {
		currentTime := fasttime.UnixTimestamp()
		if atomic.LoadUint64(&idxbe.lastAccessTime) != currentTime {
			atomic.StoreUint64(&idxbe.lastAccessTime, currentTime)  //更新最后一次查询时间
		}
		return idxbe.idxb
	}
	idxbc.perKeyMissesLock.Lock()
	idxbc.perKeyMisses[k]++
	idxbc.perKeyMissesLock.Unlock()
	atomic.AddUint64(&idxbc.misses, 1)
	return nil
}

// Put puts idxb under the key k into idxbc.
func (idxbc *indexBlockCache) Put(k uint64, idxb *indexBlock) {  //写入缓存
	idxbc.perKeyMissesLock.Lock()
	doNotCache := idxbc.perKeyMisses[k] == 1  //至少被查过两次，才会缓存
	idxbc.perKeyMissesLock.Unlock()
	if doNotCache {
		// Do not cache ib if it has been requested only once (aka one-time-wonders items).
		// This should reduce memory usage for the ibc cache.
		return
	}

	idxbc.mu.Lock()
	// Remove superfluous entries.  //最多允许缓存N条，超过了就要淘汰
	if overflow := len(idxbc.m) - getMaxCachedIndexBlocksPerPart(); overflow > 0 {
		// Remove 10% of items from the cache.
		overflow = int(float64(len(idxbc.m)) * 0.1)
		for k := range idxbc.m {
			delete(idxbc.m, k)
			overflow--
			if overflow == 0 {
				break
			}
		}
	}

	// Store idxb in the cache.
	idxbe := &indexBlockCacheEntry{
		lastAccessTime: fasttime.UnixTimestamp(),
		idxb:           idxb,
	}
	idxbc.m[k] = idxbe
	idxbc.mu.Unlock()
}

func (idxbc *indexBlockCache) Len() uint64 {
	idxbc.mu.RLock()
	n := len(idxbc.m)
	idxbc.mu.RUnlock()
	return uint64(n)
}

func (idxbc *indexBlockCache) SizeBytes() uint64 {
	n := 0
	idxbc.mu.RLock()
	for _, e := range idxbc.m {
		n += e.idxb.SizeBytes()
	}
	idxbc.mu.RUnlock()
	return uint64(n)
}

func (idxbc *indexBlockCache) SizeMaxBytes() uint64 {
	avgBlockSize := float64(64 * 1024)
	blocksCount := idxbc.Len()
	if blocksCount > 0 {
		avgBlockSize = float64(idxbc.SizeBytes()) / float64(blocksCount)
	}
	return uint64(avgBlockSize * float64(getMaxCachedIndexBlocksPerPart()))
}

func (idxbc *indexBlockCache) Requests() uint64 {  // 统计数据
	return atomic.LoadUint64(&idxbc.requests)
}

func (idxbc *indexBlockCache) Misses() uint64 {
	return atomic.LoadUint64(&idxbc.misses)
}

type inmemoryBlockCache struct {  // key为偏移量， value为 inmemoryBlock对象
	// Atomically updated counters must go first in the struct, so they are properly
	// aligned to 8 bytes on 32-bit architectures.
	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/212
	requests uint64
	misses   uint64

	m  map[inmemoryBlockCacheKey]*inmemoryBlockCacheEntry  //128GB内存下，最多允许32768个KEY
	mu sync.RWMutex

	perKeyMisses     map[inmemoryBlockCacheKey]int
	perKeyMissesLock sync.Mutex

	cleanerStopCh chan struct{}
	cleanerWG     sync.WaitGroup
}

type inmemoryBlockCacheKey struct {
	itemsBlockOffset uint64  // 以偏移量作为cache的key，指向某个block
}

func (ibck *inmemoryBlockCacheKey) Init(bh *blockHeader) {
	ibck.itemsBlockOffset = bh.itemsBlockOffset
}

type inmemoryBlockCacheEntry struct {
	// Atomically updated counters must go first in the struct, so they are properly
	// aligned to 8 bytes on 32-bit architectures.
	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/212
	lastAccessTime uint64

	ib *inmemoryBlock  //已经加载到内存中的block
}

func newInmemoryBlockCache() *inmemoryBlockCache {
	var ibc inmemoryBlockCache
	ibc.m = make(map[inmemoryBlockCacheKey]*inmemoryBlockCacheEntry)
	ibc.perKeyMisses = make(map[inmemoryBlockCacheKey]int)
	ibc.cleanerStopCh = make(chan struct{})
	ibc.cleanerWG.Add(1)
	go func() {
		defer ibc.cleanerWG.Done()
		ibc.cleaner()
	}()
	return &ibc
}

func (ibc *inmemoryBlockCache) MustClose() {
	close(ibc.cleanerStopCh)
	ibc.cleanerWG.Wait()
	ibc.m = nil
	ibc.perKeyMisses = nil
}

// cleaner periodically cleans least recently used items.
func (ibc *inmemoryBlockCache) cleaner() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	perKeyMissesTicker := time.NewTicker(2 * time.Minute)
	defer perKeyMissesTicker.Stop()
	for {
		select {
		case <-ticker.C:
			ibc.cleanByTimeout()
		case <-perKeyMissesTicker.C:
			ibc.perKeyMissesLock.Lock()
			ibc.perKeyMisses = make(map[inmemoryBlockCacheKey]int, len(ibc.perKeyMisses))
			ibc.perKeyMissesLock.Unlock()
		case <-ibc.cleanerStopCh:
			return
		}
	}
}

func (ibc *inmemoryBlockCache) cleanByTimeout() {
	currentTime := fasttime.UnixTimestamp()
	ibc.mu.Lock()
	for k, ibe := range ibc.m {
		// Delete items accessed more than two minutes ago.
		// This time should be enough for repeated queries.
		if currentTime-atomic.LoadUint64(&ibe.lastAccessTime) > 2*60 {
			delete(ibc.m, k)
		}
	}
	ibc.mu.Unlock()
}

func (ibc *inmemoryBlockCache) Get(k inmemoryBlockCacheKey) *inmemoryBlock {
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
	ibc.perKeyMissesLock.Lock()
	ibc.perKeyMisses[k]++
	ibc.perKeyMissesLock.Unlock()
	atomic.AddUint64(&ibc.misses, 1)
	return nil
}

// Put puts ib under key k into ibc.
func (ibc *inmemoryBlockCache) Put(k inmemoryBlockCacheKey, ib *inmemoryBlock) {
	ibc.perKeyMissesLock.Lock()
	doNotCache := ibc.perKeyMisses[k] == 1
	ibc.perKeyMissesLock.Unlock()
	if doNotCache {
		// Do not cache ib if it has been requested only once (aka one-time-wonders items).
		// This should reduce memory usage for the ibc cache.
		return
	}

	ibc.mu.Lock()
	// Clean superfluous entries in cache.
	if overflow := len(ibc.m) - getMaxCachedInmemoryBlocksPerPart(); overflow > 0 {
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

	// Store ib in the cache.
	ibe := &inmemoryBlockCacheEntry{
		lastAccessTime: fasttime.UnixTimestamp(),
		ib:             ib,
	}
	ibc.m[k] = ibe
	ibc.mu.Unlock()
}

func (ibc *inmemoryBlockCache) Len() uint64 {
	ibc.mu.RLock()
	n := len(ibc.m)
	ibc.mu.RUnlock()
	return uint64(n)
}

func (ibc *inmemoryBlockCache) SizeBytes() uint64 {
	n := 0
	ibc.mu.RLock()
	for _, e := range ibc.m {
		n += e.ib.SizeBytes()
	}
	ibc.mu.RUnlock()
	return uint64(n)
}

func (ibc *inmemoryBlockCache) SizeMaxBytes() uint64 {
	avgBlockSize := float64(128 * 1024)
	blocksCount := ibc.Len()
	if blocksCount > 0 {
		avgBlockSize = float64(ibc.SizeBytes()) / float64(blocksCount)
	}
	return uint64(avgBlockSize * float64(getMaxCachedInmemoryBlocksPerPart()))
}

func (ibc *inmemoryBlockCache) Requests() uint64 {
	return atomic.LoadUint64(&ibc.requests)
}

func (ibc *inmemoryBlockCache) Misses() uint64 {
	return atomic.LoadUint64(&ibc.misses)
}
