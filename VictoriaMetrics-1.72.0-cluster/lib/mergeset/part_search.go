package mergeset

import (
	"fmt"
	"io"
	"sort"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

type partSearch struct {
	// Item contains the last item found after the call to NextItem.
	//
	// The Item content is valid until the next call to NextItem.
	Item []byte

	// p is a part to search.
	p *part  // 每个part search指向对应的 part

	// The remaining metaindex rows to scan, obtained from p.mrs.
	mrs []metaindexRow  // 这个数组直接复制 part 对象中的对应数组

	// The remaining block headers to scan in the current metaindexRow.
	bhs []blockHeader  // 当前扫描到的 metaindexRow中的 blockHeader 数组

	idxbCache *indexBlockCache  // indexBlock对象的fastcache, 以 metaindexRow中的偏移量信息为key
	ibCache   *inmemoryBlockCache  // 这里在一个大 []byte 数组里面二分查找。 每个inmemoryBlock对象是64KB

	// err contains the last error.
	err error

	indexBuf           []byte
	compressedIndexBuf []byte  // 这些临时对象其实不用放在这里。但是放在这里的话，能够减少GC

	sb storageBlock  // 缓存从items.bin, lens.bin中读出的数据

	ib        *inmemoryBlock  // 当前搜索到的块里面的多个 time series
	ibItemIdx int
}

func (ps *partSearch) reset() {
	ps.Item = nil
	ps.p = nil
	ps.mrs = nil
	ps.bhs = nil
	ps.idxbCache = nil
	ps.ibCache = nil
	ps.err = nil

	ps.indexBuf = ps.indexBuf[:0]
	ps.compressedIndexBuf = ps.compressedIndexBuf[:0]

	ps.sb.Reset()

	ps.ib = nil
	ps.ibItemIdx = 0
}

// Init initializes ps for search in the p.
//
// Use Seek for search in p.
func (ps *partSearch) Init(p *part) {
	ps.reset()

	ps.p = p
	ps.idxbCache = p.idxbCache
	ps.ibCache = p.ibCache
}

// Seek seeks for the first item greater or equal to k in ps.
func (ps *partSearch) Seek(k []byte) {  // 在 part 中，根据原始的 time series数据进行搜索
	if err := ps.Error(); err != nil {
		// Do nothing on unrecoverable error.
		return
	}
	ps.err = nil

	if string(k) > string(ps.p.ph.lastItem) {  // part 与 part 之间是排序的吗？
		// Not matching items in the part.  // todo: string() 值得优化
		ps.err = io.EOF
		return  // 如果 time sereis比 part 的最后一个 item 还要大，说明数据不在这个part里，返回EOF
	}

	if ps.tryFastSeek(k) {  //  在 in-memory block中搜索
		return
	}

	ps.Item = nil
	ps.mrs = ps.p.mrs  // 复制排序了的 metaindex
	ps.bhs = nil

	ps.indexBuf = ps.indexBuf[:0]
	ps.compressedIndexBuf = ps.compressedIndexBuf[:0]

	ps.sb.Reset()  // sb storageBlock，一个空的容器，用来存放从items.bin, lens.bin中加载的内容

	ps.ib = nil
	ps.ibItemIdx = 0

	if string(k) <= string(ps.p.ph.firstItem) {  // 如果比第一个time sereis还要小
		// The first item in the first block matches.
		ps.err = ps.nextBlock()  // 没看懂，这里为什么是 nextBlock ?
		return
	}

	// Locate the first metaindexRow to scan.
	if len(ps.mrs) == 0 {
		logger.Panicf("BUG: part without metaindex rows passed to partSearch")
	}
	n := sort.Search(len(ps.mrs), func(i int) bool {  // 二分查找
		return string(k) <= string(ps.mrs[i].firstItem)  // todo: 优化string()
	})
	if n > 0 {
		// The given k may be located in the previous metaindexRow, so go to it.
		n--
	}
	ps.mrs = ps.mrs[n:]

	// Read block headers for the found metaindexRow.
	if err := ps.nextBHS(); err != nil {
		ps.err = err
		return
	}

	// Locate the first block to scan.
	n = sort.Search(len(ps.bhs), func(i int) bool {
		return string(k) <= string(ps.bhs[i].firstItem)
	})
	if n > 0 {
		// The given k may be located in the previous block, so go to it.
		n--
	}
	ps.bhs = ps.bhs[n:]

	// Read the block.
	if err := ps.nextBlock(); err != nil {
		ps.err = err
		return
	}

	// Locate the first item to scan in the block.
	items := ps.ib.items
	data := ps.ib.data
	cpLen := commonPrefixLen(ps.ib.commonPrefix, k)
	if cpLen > 0 {
		keySuffix := k[cpLen:]
		ps.ibItemIdx = sort.Search(len(items), func(i int) bool {
			it := items[i]
			it.Start += uint32(cpLen)
			return string(keySuffix) <= it.String(data)
		})
	} else {
		ps.ibItemIdx = binarySearchKey(data, items, k)
	}
	if ps.ibItemIdx < len(items) {
		// The item has been found.
		return
	}

	// Nothing found in the current block. Proceed to the next block.
	// The item to search must be the first in the next block.
	if err := ps.nextBlock(); err != nil {
		ps.err = err
		return
	}
}

func (ps *partSearch) tryFastSeek(k []byte) bool {
	if ps.ib == nil {  //  inmemoryBlock
		return false
	}
	data := ps.ib.data
	items := ps.ib.items
	idx := ps.ibItemIdx
	if idx >= len(items) {
		// The ib is exhausted.  耗尽了
		return false
	}
	if string(k) > items[len(items)-1].String(data) {  //比最后一个time series还大，说明不在这个 inmemoryBlock里面
		// The item is located in next blocks.
		return false
	}

	// The item is located either in the current block or in previous blocks.
	if idx > 0 {
		idx--  // idx是干嘛的？没看懂
	}
	if string(k) < items[idx].String(data) {  // todo: 优化string()
		if string(k) < items[0].String(data) {  // 在上次查询的index的基础上继续查找???
			// The item is located in previous blocks.
			return false
		}
		idx = 0
	}

	// The item is located in the current block
	ps.ibItemIdx = idx + binarySearchKey(data, items[idx:], k)
	return true
}

// NextItem advances to the next Item.
//
// Returns true on success.
func (ps *partSearch) NextItem() bool {
	if ps.err != nil {
		return false
	}

	items := ps.ib.items
	if ps.ibItemIdx < len(items) {
		// Fast path - the current block contains more items.
		// Proceed to the next item.
		ps.Item = items[ps.ibItemIdx].Bytes(ps.ib.data)
		ps.ibItemIdx++
		return true
	}

	// The current block is over. Proceed to the next block.
	if err := ps.nextBlock(); err != nil {
		ps.err = err
		return false
	}

	// Invariant: len(ps.ib.items) > 0 after nextBlock.
	ps.Item = ps.ib.items[0].Bytes(ps.ib.data)
	ps.ibItemIdx++
	return true
}

// Error returns the last error occurred in the ps.
func (ps *partSearch) Error() error {
	if ps.err == io.EOF {
		return nil
	}
	return ps.err
}

func (ps *partSearch) nextBlock() error {
	if len(ps.bhs) == 0 {
		// The current metaindexRow is over. Proceed to the next metaindexRow.
		if err := ps.nextBHS(); err != nil {
			return err
		}
	}
	bh := &ps.bhs[0]
	ps.bhs = ps.bhs[1:]
	ib, err := ps.getInmemoryBlock(bh)
	if err != nil {
		return err
	}
	ps.ib = ib
	ps.ibItemIdx = 0
	return nil
}

func (ps *partSearch) nextBHS() error {
	if len(ps.mrs) == 0 {
		return io.EOF
	}
	mr := &ps.mrs[0]
	ps.mrs = ps.mrs[1:]
	idxbKey := mr.indexBlockOffset
	idxb := ps.idxbCache.Get(idxbKey)
	if idxb == nil {
		var err error
		idxb, err = ps.readIndexBlock(mr)  //读取index.bin文件，返回index block对象
		if err != nil {
			return fmt.Errorf("cannot read index block: %w", err)
		}
		ps.idxbCache.Put(idxbKey, idxb)  // 以偏移量为key，写入indexBlock对象
	}
	ps.bhs = idxb.bhs
	return nil
}

func (ps *partSearch) readIndexBlock(mr *metaindexRow) (*indexBlock, error) {  // 通过 metaindexRow的信息，加载 index.bin 中的信息，返回indexBlock对象
	ps.compressedIndexBuf = bytesutil.Resize(ps.compressedIndexBuf, int(mr.indexBlockSize))
	ps.p.indexFile.MustReadAt(ps.compressedIndexBuf, int64(mr.indexBlockOffset))

	var err error
	ps.indexBuf, err = encoding.DecompressZSTD(ps.indexBuf[:0], ps.compressedIndexBuf)  // 每个 index.bin 中的block同样是ZSTD压缩的
	if err != nil {
		return nil, fmt.Errorf("cannot decompress index block: %w", err)
	}
	idxb := &indexBlock{}
	idxb.bhs, err = unmarshalBlockHeaders(idxb.bhs[:0], ps.indexBuf, int(mr.blockHeadersCount))  // 解析结构，并按照first item排序，便于后续做二分查找
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal block headers from index block (offset=%d, size=%d): %w", mr.indexBlockOffset, mr.indexBlockSize, err)
	}
	return idxb, nil
}

func (ps *partSearch) getInmemoryBlock(bh *blockHeader) (*inmemoryBlock, error) {
	var ibKey inmemoryBlockCacheKey
	ibKey.Init(bh)  // 以偏移量作为cache的key
	ib := ps.ibCache.Get(ibKey)
	if ib != nil {
		return ib, nil
	}
	ib, err := ps.readInmemoryBlock(bh)  // 从文件加载数据到 inmemoryBlock
	if err != nil {
		return nil, err
	}
	ps.ibCache.Put(ibKey, ib)  // 放到 index block cache 中
	return ib, nil
}

func (ps *partSearch) readInmemoryBlock(bh *blockHeader) (*inmemoryBlock, error) {  // 根据blockHeader的信息，加载items.bin和lens.bin文件中的对应信息
	ps.sb.Reset()  // storageBlock

	ps.sb.itemsData = bytesutil.Resize(ps.sb.itemsData, int(bh.itemsBlockSize))
	ps.p.itemsFile.MustReadAt(ps.sb.itemsData, int64(bh.itemsBlockOffset))

	ps.sb.lensData = bytesutil.Resize(ps.sb.lensData, int(bh.lensBlockSize))
	ps.p.lensFile.MustReadAt(ps.sb.lensData, int64(bh.lensBlockOffset))

	ib := getInmemoryBlock()
	if err := ib.UnmarshalData(&ps.sb, bh.firstItem, bh.commonPrefix, bh.itemsCount, bh.marshalType); err != nil {  // 把文件中的内容，加载到对象
		return nil, fmt.Errorf("cannot unmarshal storage block with %d items: %w", bh.itemsCount, err)
	}  // 从items.bin, lens.bin中读出数据，然后填充到 inmemoryBlock对象中

	return ib, nil
}
//  data 把所有time sereis的数据，排序后顺序放一起。 items记录了每个ts的起始位置
func binarySearchKey(data []byte, items []Item, key []byte) int {
	if len(items) == 0 {
		return 0
	}
	if string(key) <= items[0].String(data) {
		// Fast path - the item is the first.
		return 0
	}
	items = items[1:]
	offset := uint(1)

	// This has been copy-pasted from https://golang.org/src/sort/search.go
	n := uint(len(items))
	i, j := uint(0), n
	for i < j {
		h := uint(i+j) >> 1
		if h >= 0 && h < uint(len(items)) && string(key) > items[h].String(data) {
			i = h + 1
		} else {
			j = h
		}
	}
	return int(i + offset)
}
