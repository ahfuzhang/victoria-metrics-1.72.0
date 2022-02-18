**<font size=1 color=gray>作者:张富春(ahfuzhang)，转载时请注明作者和引用链接，谢谢！</font>**
* <font size=1 color=gray>[cnblogs博客](https://www.cnblogs.com/ahfuzhang/)</font>
* <font size=1 color=gray>[zhihu](https://www.zhihu.com/people/ahfuzhang/posts)</font>
* <font size=1 color=gray>公众号:一本正经的瞎扯</font>
![](https://img2022.cnblogs.com/blog/1457949/202202/1457949-20220216153819145-1193738712.png)


----

VictoriaMetrics监控组件（以下简称VM）号称比Prometheus快了至少3倍，内存占用比Prometheus小了7倍。

为什么能快这么多呢？下面是阅读vm-storage源码后的心得：



# 1.CPU和并发

## 基于可用的CPU核数来规划并发

see：[victoria-metrics-1.72.0/blob/master/VictoriaMetrics-1.72.0-cluster/lib/cgroup/cpu.go](https://github.com/ahfuzhang/victoria-metrics-1.72.0/blob/master/VictoriaMetrics-1.72.0-cluster/lib/cgroup/cpu.go)

```go
// AvailableCPUs returns the number of available CPU cores for the app.
func AvailableCPUs() int {
	return runtime.GOMAXPROCS(-1)
}
```

首先通过AvailableCPUs()函数来获取容器环境可用的CPU核数，然后在很多场景中都以可用的CPU核数来设计协程的数量。

协程的个数不是越多越好，对于计算密级的协程而言，少于可用的核数则浪费了资源；多余可用的核数，多出核数的协程必然无法同时被调度到，反而会增加了golang调度器的压力。进一步，协程过多会导致大量CPU白白浪费在协程调度上。因此，协程数与可用核数相关是很好的策略。(IO协程又另当别论)

## 区分IO协程和计算协程

see:  [VictoriaMetrics-1.72.0-cluster/app/vmstorage/transport/server.go#L261](https://github.com/ahfuzhang/victoria-metrics-1.72.0/blob/63987bc868a6983bc94192ed24136590016575c8/VictoriaMetrics-1.72.0-cluster/app/vmstorage/transport/server.go#L261)

IO协程执行收包、解包的任务，然后就把请求数据扔到队列，不再做复杂的其他计算。

计算协程从channel中获取任务来执行计算。

## 计算协程数量与CPU核数相当，且提供了协程优先级的处理策略

计算协程分为两种，写入协程和查询协程。写入协程与CPU核数相当，查询协程是CPU核数的两倍。查询协程更多的原因是查询期间可能引发IO，所以需要并发数多于核数。

在vm-storage的场景里，写入的优先级高于查询。

在写入协程中，设置了一个长度为CPU核数的队列(channel)，每次准备写入前先入队，如果队满就等待。这样就严格限制了写入协程的并发。同时，队列如果满，证明高优先级的写入协程没有被调度。

vm-storage提供了机制让查询协程主动让出，在写入队列满的时候通知查询协程，避免查询太多影响写入。

具体的细节请看我的这篇分析文章：《[VictoriaMetrics中协程优先级的处理方式](https://www.cnblogs.com/ahfuzhang/p/15847860.html)》



## 特定的成员有特定的锁

```go
// Table represents mergeset table.  // 索引部分的table对象
type Table struct {

	partsLock sync.Mutex  //专门针对parts数组的锁
	parts     []*partWrapper  // table对象所属的所有 part 的数组。使用引用计数
}
```

例如如上的代码，parts数组可能存在并发的问题，专门对这个成员设置了锁。

这样的话，就不必用一个很大的锁来引发剧烈的竞争。代码中大量此类的优化技巧。



## 数据分桶，减少锁竞争

see: [VictoriaMetrics-1.72.0-cluster/lib/mergeset/table.go#L128](https://github.com/ahfuzhang/victoria-metrics-1.72.0/blob/63987bc868a6983bc94192ed24136590016575c8/VictoriaMetrics-1.72.0-cluster/lib/mergeset/table.go#L128)

```go
type rawItemsShards struct {
	shardIdx uint32  // 通过原子加来确定分桶，保障各个核操作不同的桶，减少竞争

	// shards reduce lock contention when adding rows on multi-CPU systems.
	shards []rawItemsShard  // 数组长度与CPU核数一致，通过分桶来减少桶的竞争
}

func (riss *rawItemsShards) addItems(tb *Table, items [][]byte) error {  // items中包含了多个类型的索引数据
	n := atomic.AddUint32(&riss.shardIdx, 1)  // 通过原子加来分散到不同的对象，避免锁的冲突
	shards := riss.shards
	idx := n % uint32(len(shards))
	shard := &shards[idx]
	return shard.addItems(tb, items)  //确定分桶后，再转到具体的分桶对象上处理
}
```

对于频繁写入的大量同类对象，VM采用了分桶的策略。通过原子加来选定一个不同的分桶，以此来减少竞争。



## 拷贝出来再处理，减少加锁的时间

```go
// getParts appends parts snapshot to dst and returns it.
//
// The appended parts must be released with putParts.
func (tb *Table) getParts(dst []*partWrapper) []*partWrapper {  // 复制table 对象的所有 part
	tb.partsLock.Lock()
	for _, pw := range tb.parts {
		pw.incRef()  // 通过引用计数的方法来复制
	}
	dst = append(dst, tb.parts...)
	tb.partsLock.Unlock()

	return dst
}
```

如果对整个数组全部处理完需要很长的时间，还不如把数据拷贝出来，减少加锁的时间。



## 引用计数机制，解决并发中可能带来的对象新增和删除问题

```go
func (pw *partWrapper) incRef() {
	atomic.AddUint64(&pw.refCount, 1)
}

func (pw *partWrapper) decRef() {
	n := atomic.AddUint64(&pw.refCount, ^uint64(0))
	if int64(n) < 0 {
		logger.Panicf("BUG: pw.refCount must be bigger than 0; got %d", int64(n))
	}
	if n > 0 {
		return
	}

	if pw.mp != nil {
		putInmemoryPart(pw.mp)
		pw.mp = nil
	}
	pw.p.MustClose()
	pw.p = nil
}
```

通过一个整形值来记录对象的引用情况。

某个协程需要这个对象的时候，调用incRef()来增加引用计数；使用完成后，调用decRef()来减少计数。

当记录为0时，把对象放入sync.Pool对象池。



## 使用sync.Value来处理并发期间可能切换的成员

```go
// Storage represents TSDB storage.
type Storage struct {


	idbCurr atomic.Value  // indexdb对象，当前的时间片
}

func (s *Storage) idb() *indexDB {
	return s.idbCurr.Load().(*indexDB)
}
```

存储中的索引对象被引用得非常频繁，且存在索引切换的可能。

在这种场景下，使用 sync.Value 来解决并发环境下的对象引用。



# 2.内存、对象池、栈逃逸、GC

相比C/C++这样的语言，golang在性能方面的最大劣势是GC。因此，尽可能的减少堆上的对象，可以减少GC的压力，并减少GC运行引起的STW的延迟。VM在这个方面的优化真是做得极其深入。



## 基于可用的内存来限定对象的总量

see: [/VictoriaMetrics-1.72.0-cluster/lib/cgroup/mem.go](https://github.com/ahfuzhang/victoria-metrics-1.72.0/blob/master/VictoriaMetrics-1.72.0-cluster/lib/cgroup/mem.go)

通过GetMemoryLimit()函数来获取容器环境的最大可用内存，同时每个VM的组件都可以配置`-memory.allowedBytes`或`-memory.allowedPercent`来限制进程的最大内存。

在内存资源限制的基础上，各种cache和业务处理对象就按照比例进行分配，确保进程绝不会发生OOM而崩溃。

相比之下，prometheus和thanos在请求量大的情况下极易发生OOM崩溃。



## 区分fast path和slow path

猜测VM的团队是反反复复做了很多的profile分析，代码执行路径中最常见的路径，都加上了fast path的注释，其他也标上了slow path。

fast path代表了绝大多数time series的处理路径，对内存的优化主要集中在fast path上。



## fast path上一个栈逃逸都没有

也就是说，fast path上**没有任何一次堆内存分配**！！！

从而绝大多数情况下，不会频繁的新增对象，不会给GC带来大的压力，更没有长时间的STW...

由此可以，团队在很长的时间里，对栈逃逸、内存profile等繁琐无聊的做了无数遍，最终优化到一次堆内存分配都没有的程度！！！

这个手段不值得惊奇，这个效果和这个专业态度实在令人惊奇！

下面这个函数的实现，可见一斑：[VictoriaMetrics-1.72.0-cluster/lib/mergeset/encoding.go#L30](https://github.com/ahfuzhang/victoria-metrics-1.72.0/blob/63987bc868a6983bc94192ed24136590016575c8/VictoriaMetrics-1.72.0-cluster/lib/mergeset/encoding.go#L30)

```go
// Bytes returns bytes representation of it obtained from data.
//
// The returned bytes representation belongs to data.
func (it Item) Bytes(data []byte) []byte {  // 参数 data 其实没有必要。但是如果由外部传入，就不必再分配，对GC的优化达到了极致！牛逼
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	sh.Cap = int(it.End - it.Start)
	sh.Len = int(it.End - it.Start)
	sh.Data += uintptr(it.Start)
	return data
}
```



## mmap + 紧凑分配——fastcache中解决海量小对象的分配

关于fastcache组件，请看我的这篇分析：《[介绍一个golang库：fastcache](https://www.cnblogs.com/ahfuzhang/p/15840313.html)》

1. 使用mmap系统调用来分配内存，这样内存就绕过了GC
2. 自己来记录对象在一个大数组中的起始位置，紧凑的存放。这样就又节约内存又不用考虑大量小对象的产生。



## 类的部分成员变量其实是临时变量——小型对象分配的优化

VM中有大量类似下方的写法：

```go
type inmemoryPart struct {
	ph partHeader
	sb storageBlock
	bh blockHeader
	mr metaindexRow
	//下方四个成员变量其实都是临时变量
	unpackedIndexBlockBuf []byte  // 保存上方的block header序列化后的数据
	packedIndexBlockBuf   []byte  // 把 unpackedIndexBlockBuf 做 ZSTD压缩。

	unpackedMetaindexBuf []byte  // 把metaindexRow序列化后，存在这里
	packedMetaindexBuf   []byte  // 把 unpackedMetaindexBuf 进行ZSTD压缩后，存在这里

	metaindexData bytesutil.ByteBuffer
	indexData     bytesutil.ByteBuffer
	itemsData     bytesutil.ByteBuffer
	lensData      bytesutil.ByteBuffer
}
```

当存在一些需要在函数间传递的临时变量时，VM把这些临时变量作为对象的成员变量来处理。这样的话，在对象的生命期内，临时变量只会被分配一次，从而小对象就不会频繁的分配了。

## sync.Pool的对象池的使用——中型对象分配的优化

中型对象的分配和释放相对不是很频繁，因此使用sync.Pool来作为对象池，就可以重用这些对象。
中型对象提供了reset()方法，把缓冲区的开始位置置0，而不是解除引用。中型对象相关的所有成员都只会分配一次，然后不再释放。



如果担心某些中型对象太耗内存，VM中还使用了channel来保存对象，限制了总的对象个数。这里同样也是大型对象的处理策略。

```go
var mpPool = make(chan *inmemoryPart, cgroup.AvailableCPUs())

func getInmemoryPart() *inmemoryPart {
	select {
	case mp := <-mpPool:
		return mp
	default:
		return &inmemoryPart{}
	}
}

func putInmemoryPart(mp *inmemoryPart) {
	mp.Reset()
	select {
	case mpPool <- mp:
	default:
		// Drop mp in order to reduce memory usage.
	}
}
```



## 数组的重复使用

VM代码中的几乎所有数组都只分配不释放，对象使用完成后放回sync.Pool，以备下次重复使用。

```go
func (o *Obj)foo(){
    o.buf = o.buf[:0]  //把临时数组作为对象的成员。使用前清空
    o.buf = o.bar(o.buf)  //函数调用中，通常把目的数组传入进去
}
```





# 3.通讯协议

## 连gogoproto都没用，自己用TLV的方式来序列化数据

因为数据都是time series的 label name + label value + timestamp + value，因此vm中定义了自己的序列化格式，简单的以TLV的方式来组合。在非常具体的业务场景下，这必然是最快的方法。



## 极其简单的单工的TCP通讯协议

与prometheus不同，vm-insert与vm-storage之间的通讯协议没有选择HTTP协议。

> 常常有很多的专家冒出来说HTTP协议不慢，云时代用HTTP又标准又可读……我不以为然，二进制协议和文本协议的性能差异是巨大的，在海量高性能低延迟的场景，二进制协议是必然的选择。

与thanos体系不同，vm也没有使用RPC协议。把多条time series数据转发到vm-storage存储节点这个场景是极其简单的。简单的场景就用简单的方法来做，非得套个标准和方便扩展的理由太牵强。

因此，VM选择了极简的策略，在通讯中使用了文本信息来完成握手，然后用固定4字节表示长度+BODY的方式来传输。并且，传输协议是一请求一应答的单工协议。所以解析协议的代码非常简单且高效。



## 使用ZSTD压缩算法来压缩传输内容

通常来说，不管是网络IO还是磁盘IO，相比CPU和内存来说都要慢很多。因此，减少IO取得的性能优化提升比优化CPU和内存还要来得更简单。对传输内容压缩肯定是必由之路，且对于time series数据传输这样的场景来说，大多数数据都是文本，获得的压缩比更优。

zstd是facebook开源的一个C语言的压缩库。 从官方提供的压测数据看，它的压缩速度与众所周知的以快著称的snappy的压缩速度几乎持平，但是压缩率上比老牌的gzip还要高。

关于ZSTD的介绍，请看我的这篇帖子：《[介绍一个golang库：zstd](https://www.cnblogs.com/ahfuzhang/p/15842350.html)》



## 合并发送，单一连接

当vm-insert需要向vm-storage发送数据时，先追加到一个buffer中；达到一定时间或者buffer满时，才会触发发送逻辑。首先，对time series数据传输这种场合，秒级延迟就够了，对延迟不敏感；其次，合并了较多的数据后再压缩，减小了网络IO的压力。

与直觉相违背的是，vm-insert与vm-storage之间只有1个连接，没有使用连接池。更多的连接需要更多的IO协程来管理并发，且vm-insert与vm-storage一般都部署在同一IDC，一个连接足够了。非常的克制！



## 缺点

当然也是有缺点的：

* 增加了传输延迟（业务场景允许）
* 数据可靠性降低
    * 如果进程突然崩溃，必然导致数据丢失
    * 少量的丢失time series数据仍然是允许的，相当于采样率降低，对整体的影响不大



# 4.其他

## 几乎没有日志

程序日志应该只有一个用途：记录关键事件。

而由用户请求引起的事件的记录，我认为应该归类到`结构化数据上报`的范畴，通过把数据导入另一套在线OLAP系统来解决。



如果程序日志太多，通常的原因是：

* 开发没有做好严谨的测试，对自己的代码没信心
* 将来为质量原因而引发的灾难做一个兜底策略



VM的代码中几乎没有日志，给我们竖立了一个很好的典范。



## 没有panic和recovery

也意味着，没有什么行为是超出开发者预期的。



## 监控数据上报，没有使用经典的metric上报API

VM自己就是一个监控系统，但是vm-storage内部的监控数据上报并未使用metricAPI.

相反，VM在每个需要监控上报的位置，用uint64类型的成员变量来记录统计值，使用原子加来累加。

最后由额外的上报协程来读取这些变量，进行上报。



see: [VictoriaMetrics-1.72.0-cluster/lib/storage/storage.go](https://github.com/ahfuzhang/victoria-metrics-1.72.0/blob/63987bc868a6983bc94192ed24136590016575c8/VictoriaMetrics-1.72.0-cluster/lib/storage/storage.go#L509)

```go
// UpdateMetrics updates m with metrics from s.
func (s *Storage) UpdateMetrics(m *Metrics) {
	m.RowsAddedTotal = atomic.LoadUint64(&rowsAddedTotal)
	m.DedupsDuringMerge = atomic.LoadUint64(&dedupsDuringMerge)

	m.TooSmallTimestampRows += atomic.LoadUint64(&s.tooSmallTimestampRows)
	m.TooBigTimestampRows += atomic.LoadUint64(&s.tooBigTimestampRows)
}
```



## sync.Once来做低频率对象的延迟初始化

see: VictoriaMetrics-1.72.0-cluster/lib/storage/part.go

```go
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
```



## 模仿roaringBitmap的数据结构

请移步到我的这篇文章：《[vm中仿照RoaringBitmap的实现：uint64set](https://www.cnblogs.com/ahfuzhang/p/15900852.html)》



## 位运算技巧

请移步到我的这篇文章：《[如何计算一个uint64类型的二进制值的尾部有多少个0](https://www.cnblogs.com/ahfuzhang/p/15900551.html)》



## if语句上的string()转换会被编译器优化

请移步到我的这篇文章：《[golang的if比较中的string转换会被编译器优化](https://www.cnblogs.com/ahfuzhang/p/15879423.html)》



## 强制约定了for循环的写法

> range 在迭代过程中返回的是迭代值的拷贝，如果每次迭代的元素的内存占用很低，那么 for 和 range 的性能几乎是一样，例如 []int。但是如果迭代的元素内存占用较高，例如一个包含很多属性的 struct 结构体，那么 for 的性能将显著地高于 range，有时候甚至会有上千倍的性能差异。对于这种场景，建议使用 for，如果使用 range，建议只迭代下标，通过下标访问迭代值，这种使用方式和 for 就没有区别了。如果想使用 range 同时迭代下标和值，则需要将切片/数组的元素改为指针，才能不影响性能。
>
> ——《[Go 语言高性能编程](https://geektutu.com/post/hpg-range.html)》

VM团队应该是在编码规范上强制约定了for循环的写法。代码中几乎所有的数组循环都只有下标部分：

```go
func (riss *rawItemsShards) Len() int {
	n := 0
	for i := range riss.shards {
		n += riss.shards[i].Len()
	}
	return n
}
```

但是，还是应该区分数组是否是结构体数组：

1. 如果数组不是结构体数组，上面的写法没有任何性能优化的收益；
2. 如果数组非常大，反而会拖慢——因为直接访问数组下标，必然产生数据越界检查的两条指令。

```
idx := 2
a := arrary[idx]
其实被编译成了这样的代码：
if idx<0 || idx>=len(array){
    panic("out of range")
} else {
    a = array[idx]
}
```



## 通过fasttime来减少 time.Now().Unix()的调用

```go
func init() {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for tm := range ticker.C {
			t := uint64(tm.Unix())
			atomic.StoreUint64(&currentTimestamp, t)
		}
	}()
}

var currentTimestamp = uint64(time.Now().Unix())

func UnixTimestamp() uint64 {
	return atomic.LoadUint64(&currentTimestamp)
}
```

我测试过benchmark，缓存time.Now().Unix()后，性能提升3倍。

对于大量的需要低精度时间的场合，是个不错的优化。



## 缺点：存储中使用mmap映射文件，可能导致协程调度器阻塞

golang的调度器是很牛的——网络IO和磁盘IO的繁忙都不会阻塞住协程调度器，IO繁忙只会阻塞住具体的发起io系统调用的协程。

但是，当mmap映射一个大文件时，情况就会不一样了：

* mmap映射文件后，得到一片虚地址空间，此时还没有对应的物理内存
* 程序访问这些内存地址后，触发操作系统的缺页中断
* 操作系统挂起进程调度，从磁盘加载数据到page cache；然后把虚地址映射到page cache
* 操作系统重新调度进程

由此，如果缺页中断发生的时候，正好处理IO繁忙，则整个物理线程就会被挂起——从而协程调度器被挂起，处于这个调度器上的所有协程被挂起。所幸，一般还有其他核可用。但这是一个可能的风险点。



# 5.展望

clickhouse的文档里有这样一段话：

>  通常有两种不同的加速查询处理的方法：矢量化查询执行和运行时代码生成。在后者中，动态地为每一类查询生成代码，消除了间接分派和动态分派。这两种方法中，并没有哪一种严格地比另一种好。运行时代码生成可以更好地将多个操作融合在一起，从而充分利用 CPU 执行单元和流水线。矢量化查询执行不是特别实用，因为它涉及必须写到缓存并读回的临时向量。如果 L2 缓存容纳不下临时数据，那么这将成为一个问题。但矢量化查询执行更容易利用 CPU 的 SIMD 功能。朋友写的一篇研究论文表明，将两种方法结合起来是更好的选择。ClickHouse 使用了矢量化查询执行，同时初步提供了有限的运行时动态代码生成。

从实现带来来看，VictoriaMetrics并未采用SIMD和JIT这两项技术。可以期待，未来VM的性能还能更高！



我已将注释版的源码放在了这里：https://github.com/ahfuzhang/victoria-metrics-1.72.0，对存储引擎实现有兴趣的朋友可以来一起注释。

敬请期待我后续对于存储引擎的分析文章。

希望对你有用，have fun :-)

