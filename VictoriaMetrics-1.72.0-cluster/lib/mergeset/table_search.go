package mergeset

import (
	"bytes"
	"container/heap"
	"fmt"
	"io"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// TableSearch is a reusable cursor used for searching in the Table.
type TableSearch struct {
	// Item contains the next item after successful NextItem
	// or FirstItemWithPrefix call.
	//
	// Item contents breaks after the next call to NextItem.
	Item []byte  //存储当前按照前缀搜索到的第一个索引

	tb *Table

	pws []*partWrapper  // index下面的多个 part

	psPool []partSearch  // 可能是每个part, 对应着一个part search对象
	psHeap partSearchHeap  //猜测是把含有可能的前缀的元素，放到一个堆里面去。 ??? 为什么是堆呢
       // 采用优先队列
	err error

	nextItemNoop bool  //默认false，指示后续是否还有数据
	needClosing  bool  // 必须把part的引用进行归还,  []*partWrapper
}

func (ts *TableSearch) reset() {  // 初始化对象的相关成员
	ts.Item = nil
	ts.tb = nil

	for i := range ts.pws {
		ts.pws[i] = nil
	}
	ts.pws = ts.pws[:0]

	for i := range ts.psPool {
		ts.psPool[i].reset()
	}
	ts.psPool = ts.psPool[:0]

	for i := range ts.psHeap {
		ts.psHeap[i] = nil
	}
	ts.psHeap = ts.psHeap[:0]

	ts.err = nil

	ts.nextItemNoop = false
	ts.needClosing = false
}

// Init initializes ts for searching in the tb.
//
// MustClose must be called when the ts is no longer needed.
func (ts *TableSearch) Init(tb *Table) {  // 初始化 table search对象
	if ts.needClosing {
		logger.Panicf("BUG: missing MustClose call before the next call to Init")
	}

	ts.reset()

	ts.tb = tb  // table 对象
	ts.needClosing = true

	ts.pws = ts.tb.getParts(ts.pws[:0])  //所有的part，复制出来

	// Initialize the psPool.
	if n := len(ts.pws) - cap(ts.psPool); n > 0 {
		ts.psPool = append(ts.psPool[:cap(ts.psPool)], make([]partSearch, n)...)
	}
	ts.psPool = ts.psPool[:len(ts.pws)]
	for i, pw := range ts.pws {  //初始化 partSearch
		ts.psPool[i].Init(pw.p)  // 从 part 对象拷贝过来
	}
}

// Seek seeks for the first item greater or equal to k in the ts.  //??? 为什么搜索的key没保存下来？
func (ts *TableSearch) Seek(k []byte) {  // 传入原始的k格式，进行搜索。原始格式包含了序列化后的time series数据
	if err := ts.Error(); err != nil {
		// Do nothing on unrecoverable error.
		return
	}
	ts.err = nil

	// Initialize the psHeap.
	var errors []error
	ts.psHeap = ts.psHeap[:0]  // 数组清空
	for i := range ts.psPool {  // psPool 是 part search对象的数组. ??? psPool到底是按照什么排序的?
		ps := &ts.psPool[i]   //psPool的排序是parts的排序
		ps.Seek(k)  // 在每个part search 中继续搜索。这一步说明，这个时候，part中所有符合条件的数据都被定为到了
		if !ps.NextItem() {
			if err := ps.Error(); err != nil {
				errors = append(errors, err)
			}
			continue
		}
		ts.psHeap = append(ts.psHeap, ps)  // 搜索到了，加入优先队列
	}
	if len(errors) > 0 {
		// Return only the first error, since it has no sense in returning all errors.
		ts.err = fmt.Errorf("cannot seek %q: %w", k, errors[0])
		return
	}
	if len(ts.psHeap) == 0 {
		ts.err = io.EOF
		return
	}
	heap.Init(&ts.psHeap)   //猜测这里可能只是粗筛，把含有可能的前缀的part都放进去
	ts.Item = ts.psHeap[0].Item  //找到的第一条数据
	ts.nextItemNoop = true
}

// FirstItemWithPrefix seeks for the first item with the given prefix in the ts.
//
// It returns io.EOF if such an item doesn't exist.
func (ts *TableSearch) FirstItemWithPrefix(prefix []byte) error {  //根据前缀来查找KEY
	ts.Seek(prefix)  //这种操作打开游标
	if !ts.NextItem() {
		if err := ts.Error(); err != nil {
			return err
		}
		return io.EOF
	}
	if err := ts.Error(); err != nil {
		return err
	}
	if !bytes.HasPrefix(ts.Item, prefix) {  //搜索，并比较第一条数据
		return io.EOF
	}
	return nil
}

// NextItem advances to the next item.
func (ts *TableSearch) NextItem() bool {  // 猜测：每次调用，返回前缀匹配的第一个KEY
	if ts.err != nil {
		return false
	}
	if ts.nextItemNoop {  //一开始 nextItemNoop 为false
		ts.nextItemNoop = false  //第一条数据的游标已经赋值了，所以这里有这种极其变态的操作！！！
		return true
	}

	ts.err = ts.nextBlock()  //对 Item 字段赋值
	if ts.err != nil {
		if ts.err != io.EOF {
			ts.err = fmt.Errorf("cannot obtain the next block to search in the table: %w", ts.err)
		}
		return false
	}
	return true
}

func (ts *TableSearch) nextBlock() error {  //初筛的数据放到heap，再精筛   //看起来又不像筛选，如果是筛选，应该要传入需要比较的key
	psMin := ts.psHeap[0]  // 获取堆顶的 *partSearch 对象
	if psMin.NextItem() {  //这个是检查游标的吗？
		heap.Fix(&ts.psHeap, 0)  //删除堆顶元素后，调整堆
		ts.Item = ts.psHeap[0].Item
		return nil
	}

	if err := psMin.Error(); err != nil {
		return err
	}

	heap.Pop(&ts.psHeap)  // 某种意义上也是 nextPart

	if len(ts.psHeap) == 0 {
		return io.EOF
	}

	ts.Item = ts.psHeap[0].Item  //找到后，对 item 字段进行赋值。这个字段应该是最终找到的内容
	return nil
}

// Error returns the last error in ts.
func (ts *TableSearch) Error() error {
	if ts.err == io.EOF {
		return nil
	}
	return ts.err
}

// MustClose closes the ts.
func (ts *TableSearch) MustClose() {
	if !ts.needClosing {
		logger.Panicf("BUG: missing Init call before MustClose call")
	}
	ts.tb.putParts(ts.pws)
	ts.reset()
}

type partSearchHeap []*partSearch

func (psh *partSearchHeap) Len() int {
	return len(*psh)
}

func (psh *partSearchHeap) Less(i, j int) bool {
	x := *psh
	return string(x[i].Item) < string(x[j].Item)
}

func (psh *partSearchHeap) Swap(i, j int) {
	x := *psh
	x[i], x[j] = x[j], x[i]
}

func (psh *partSearchHeap) Push(x interface{}) {
	*psh = append(*psh, x.(*partSearch))
}

func (psh *partSearchHeap) Pop() interface{} {  //弹出就是把数组最后一个元素踢掉
	a := *psh
	v := a[len(a)-1]
	*psh = a[:len(a)-1]
	return v
}
