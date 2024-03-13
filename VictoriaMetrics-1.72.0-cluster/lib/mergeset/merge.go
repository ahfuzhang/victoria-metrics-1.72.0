package mergeset

import (
	"container/heap"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// PrepareBlockCallback can transform the passed items allocated at the given data.
//
// The callback is called during merge before flushing full block of the given items
// to persistent storage.
//
// The callback must return sorted items. The first and the last item must be unchanged.
// The callback can re-use data and items for storing the result.
type PrepareBlockCallback func(data []byte, items []Item) ([]byte, []Item)

// mergeBlockStreams merges bsrs and writes result to bsw.
//
// It also fills ph.
//
// prepareBlock is optional.
//
// The function immediately returns when stopCh is closed.
//
// It also atomically adds the number of items merged to itemsMerged.  // 合并15个 inmemoryPart
func mergeBlockStreams(ph *partHeader, bsw *blockStreamWriter, bsrs []*blockStreamReader, prepareBlock PrepareBlockCallback, stopCh <-chan struct{},
	itemsMerged *uint64) error {
	bsm := bsmPool.Get().(*blockStreamMerger)
	if err := bsm.Init(bsrs, prepareBlock); err != nil {  // 把 压缩好的数据又解压缩了，方便读取。（蛋疼啊！一开始就不要压缩就好了）
		return fmt.Errorf("cannot initialize blockStreamMerger: %w", err)
	}  // 把数据放到了 bsrHeap 字段
	err := bsm.Merge(bsw, ph, stopCh, itemsMerged)  //两两合并
	bsm.reset()
	bsmPool.Put(bsm)
	bsw.MustClose()
	if err == nil {
		return nil
	}
	return fmt.Errorf("cannot merge %d block streams: %s: %w", len(bsrs), bsrs, err)
}

var bsmPool = &sync.Pool{
	New: func() interface{} {
		return &blockStreamMerger{}
	},
}

type blockStreamMerger struct {
	prepareBlock PrepareBlockCallback  // 初始化 storage 对象时候提供的回调函数
        //  lib/storage/index_db.go:3751  mergeTagToMetricIDsRows
	bsrHeap bsrHeap  // 这个是一个堆的数据结构

	// ib is a scratch block with pending items.
	ib inmemoryBlock  //用于合并过程中，把别的 inmemoryBlock 中的 KEY 拷贝过来

	phFirstItemCaught bool

	// This are auxiliary buffers used in flushIB
	// for consistency checks after prepareBlock call.
	firstItem []byte
	lastItem  []byte
}

func (bsm *blockStreamMerger) reset() {
	bsm.prepareBlock = nil

	for i := range bsm.bsrHeap {
		bsm.bsrHeap[i] = nil
	}
	bsm.bsrHeap = bsm.bsrHeap[:0]
	bsm.ib.Reset()

	bsm.phFirstItemCaught = false
}

func (bsm *blockStreamMerger) Init(bsrs []*blockStreamReader, prepareBlock PrepareBlockCallback) error {  // 初始化用于合并的对象
	bsm.reset()   //  bsrs []*blockStreamReader 是数据源，一般是15个元素
	bsm.prepareBlock = prepareBlock
	for _, bsr := range bsrs {  // 遍历每个 blockStreamReader 对象
		if bsr.Next() {  // 第一次调用Next()方法其实是准备好数据，方便像游标一样读取它们
			bsm.bsrHeap = append(bsm.bsrHeap, bsr)  // 加入堆中
		}

		if err := bsr.Error(); err != nil {
			return fmt.Errorf("cannot obtain the next block from blockStreamReader %q: %w", bsr.path, err)
		}
	}
	heap.Init(&bsm.bsrHeap)  // 调整堆  ??? 调整堆和sort()有什么差别？  // 根据firstItem来建立堆

	if len(bsm.bsrHeap) == 0 {
		return fmt.Errorf("bsrHeap cannot be empty")
	}

	return nil
}

var errForciblyStopped = fmt.Errorf("forcibly stopped")
  //两两归并排序的过程
func (bsm *blockStreamMerger) Merge(bsw *blockStreamWriter, ph *partHeader, stopCh <-chan struct{}, itemsMerged *uint64) error {  // 合并15个 inmemoryPart  //两两合并。每合并满64KB，就写入 mpDst 的 ByteBuffer 中
again:  // todo: 丑陋的代码
	if len(bsm.bsrHeap) == 0 {
		// Write the last (maybe incomplete) inmemoryBlock to bsw.
		bsm.flushIB(bsw, ph, itemsMerged)
		return nil
	}

	select {
	case <-stopCh:  // 这里证明了从 nil 的 channel 中出队，不会报错
		return errForciblyStopped
	default:
	}

	bsr := heap.Pop(&bsm.bsrHeap).(*blockStreamReader)  // 从堆中弹出一个元素，其实就是最小的元素

	var nextItem []byte
	hasNextItem := false
	if len(bsm.bsrHeap) > 0 {
		nextItem = bsm.bsrHeap[0].bh.firstItem  //下一个block中的数据
		hasNextItem = true  //将游标指向下一个元素
	}
	items := bsr.Block.items  // inmemoryBlock 中的数据
	data := bsr.Block.data
	for bsr.blockItemIdx < len(bsr.Block.items) {  //遍历每条数据
		item := items[bsr.blockItemIdx].Bytes(data)
		if hasNextItem && string(item) > string(nextItem) {  // 编译器优化string()
			break  //走到这里，说明这个块和下个块的排序存在交叉
		}
		if !bsm.ib.Add(item) {  //拷贝到新的 inmemoryBlock
			// The bsm.ib is full. Flush it to bsw and continue.
			bsm.flushIB(bsw, ph, itemsMerged)  // inmemoryBlock 超过64KB后，走到这里。写入了 ByteBuffer 对象
			continue
		}
		bsr.blockItemIdx++  //移动第一个块的游标，直到和第二个块对齐
	}
	if bsr.blockItemIdx == len(bsr.Block.items) {  //当前块处理完成后，处理下一个块
		// bsr.Block is fully read. Proceed to the next block.
		if bsr.Next() {
			heap.Push(&bsm.bsrHeap, bsr)
			goto again
		}
		if err := bsr.Error(); err != nil {
			return fmt.Errorf("cannot read storageBlock: %w", err)
		}
		goto again
	}

	// The next item in the bsr.Block exceeds nextItem.
	// Adjust bsr.bh.firstItem and return bsr to heap.
	bsr.bh.firstItem = append(bsr.bh.firstItem[:0], bsr.Block.items[bsr.blockItemIdx].String(bsr.Block.data)...)
	heap.Push(&bsm.bsrHeap, bsr)  //当两个block遇到交叉的key，放回堆内重新排序。重新两两合并
	goto again
}
  // inmemoryBlock 超过64KB后，进入这里
func (bsm *blockStreamMerger) flushIB(bsw *blockStreamWriter, ph *partHeader, itemsMerged *uint64) {
	items := bsm.ib.items  // inmemoryBlock 中的数据
	data := bsm.ib.data
	if len(items) == 0 {
		// Nothing to flush.
		return
	}
	atomic.AddUint64(itemsMerged, uint64(len(items)))
	if bsm.prepareBlock != nil {  // 如果指定了回调函数，就执行回调函数。
		bsm.firstItem = append(bsm.firstItem[:0], items[0].String(data)...)
		bsm.lastItem = append(bsm.lastItem[:0], items[len(items)-1].String(data)...)
		data, items = bsm.prepareBlock(data, items)  // 调用回调函数
		bsm.ib.data = data
		bsm.ib.items = items
		if len(items) == 0 {
			// Nothing to flush
			return
		}
		// Consistency checks after prepareBlock call.
		firstItem := items[0].String(data)
		if firstItem != string(bsm.firstItem) {
			logger.Panicf("BUG: prepareBlock must return first item equal to the original first item;\ngot\n%X\nwant\n%X", firstItem, bsm.firstItem)
		}
		lastItem := items[len(items)-1].String(data)
		if lastItem != string(bsm.lastItem) {
			logger.Panicf("BUG: prepareBlock must return last item equal to the original last item;\ngot\n%X\nwant\n%X", lastItem, bsm.lastItem)
		}
		// Verify whether the bsm.ib.items are sorted only in tests, since this
		// can be expensive check in prod for items with long common prefix.
		if isInTest && !bsm.ib.isSorted() {
			logger.Panicf("BUG: prepareBlock must return sorted items;\ngot\n%s", bsm.ib.debugItemsString())
		}
	}
	ph.itemsCount += uint64(len(items))
	if !bsm.phFirstItemCaught {
		ph.firstItem = append(ph.firstItem[:0], items[0].String(data)...)
		bsm.phFirstItemCaught = true
	}
	ph.lastItem = append(ph.lastItem[:0], items[len(items)-1].String(data)...)
	bsw.WriteBlock(&bsm.ib)  // 把当前的 inmemoryBlock 进行写入，其实是拷贝到 ByteBuffer 对象(经过了ZSTD压缩)
	bsm.ib.Reset()  //写入
	ph.blocksCount++
}

type bsrHeap []*blockStreamReader

func (bh *bsrHeap) Len() int {
	return len(*bh)
}

func (bh *bsrHeap) Swap(i, j int) {
	x := *bh
	x[i], x[j] = x[j], x[i]
}

func (bh *bsrHeap) Less(i, j int) bool {
	x := *bh
	return string(x[i].bh.firstItem) < string(x[j].bh.firstItem)  //编译器优化string()
}

func (bh *bsrHeap) Pop() interface{} {  // 去掉最后一个元素
	a := *bh
	v := a[len(a)-1]
	*bh = a[:len(a)-1]
	return v
}

func (bh *bsrHeap) Push(x interface{}) {
	v := x.(*blockStreamReader)
	*bh = append(*bh, v)
}
