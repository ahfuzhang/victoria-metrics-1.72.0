这里跟踪索引的merge流程：

| 源码 | 函数 | 调用了 | 被调函数 | 说明 |
| ---- | ---- | ---- | ---- | ---- |
| [lib/mergeset/table.go:864](https://github.com/ahfuzhang/victoria-metrics-1.72.0/blob/master/VictoriaMetrics-1.72.0-cluster/lib/mergeset/table.go#L864) | func (tb *Table) mergeParts |  |  | merge的入口函数<br />上层调用者通过[]*partWrapper参数传入要merge的part对象数组 |
|  |  | :889 | bsrs := make([]*blockStreamReader, 0, len(pws)) | 每个part对应着一个 blockStreamReader 对象 |
|  |  | :895 |  | 遍历part数组，初始化blockStreamReader对象：<br />分为inmemoryPart和filePart两种情况 |
|  |  | :926 | bsw := getBlockStreamWriter() | 初始化BlockStreamWriter对象 |
|  |  | :927 | getCompressLevelForPartItems | 根据数据量来决定压缩率 |
| | | :928 | bsw.InitFromFilePart | 在临时文件夹创建目标part文件 |
| | | :934 | mergeBlockStreams | 执行合并，写入新的part |
| | | :939 | ph.WriteMetadata | 写入partHeader<br />也就是metaindex.bin和metadata.json |
| | | :954 |  | 所有合并前的文件part的路径，写到一个文本文件 |
| | | :965 | runTransaction | 删除合并以前的part文件及其目录 |
| | | :970 | openFilePart | 合并后的新part，打开 |
| | | :990 | removeParts | 数组中删除老part |
| | | :991 | tb.parts = append(tb.parts, newPW) | 新的 part 加进去 |


merge函数跟进：
| 源码 | 函数 | 调用了 | 被调函数 | 说明 |
| ---- | ---- | ---- | ---- | ---- |
| [lib/mergeset/merge.go:30](https://github.com/ahfuzhang/victoria-metrics-1.72.0/blob/master/VictoriaMetrics-1.72.0-cluster/lib/mergeset/merge.go#L30) | mergeBlockStreams |  |  | merge多个part |
|  |  | :32 |  | 构造blockStreamMerger对象 |
|  |  | :33 | bsm.Init | blockStreamMerger对象初始化 |
| | | :36 | bsm.Merge | 两两合并的主要流程 |
| | | :39 | bsw.MustClose() | 新的filePart对象关闭 |
| :80 | func (bsm *blockStreamMerger) Init |  |  | blockStreamMerger对象初始化 |
|  |  | :83 |  | 遍历每个part |
|  |  | :84 | bsr.Next() | 打开每个part的读取游标 |
|  |  | :92 | heap.Init(&bsm.bsrHeap) | 根据part的firstItem字段来调整好堆 |

blockStreamReader准备游标的过程：
| 源码 | 函数 | 调用了 | 被调函数 | 说明 |
| ---- | ---- | ---- | ---- | ---- |
| lib/mergeset/block_stream_reader.go:188 | func (bsr *blockStreamReader) Next() |  |  | 像游标一样逐块读取数据 |

两两合并的核心流程：
| 源码 | 函数 | 调用了 | 被调函数 | 说明 |
| ---- | ---- | ---- | ---- | ---- |
| [lib/mergeset/merge.go:103](https://github.com/ahfuzhang/victoria-metrics-1.72.0/blob/master/VictoriaMetrics-1.72.0-cluster/lib/mergeset/merge.go#L103) | func (bsm *blockStreamMerger) Merge |  |  | 两两合并的主要流程 |
|  |  | :105 |  | 检查堆。堆空了说明已经合并完了 |
|  |  | :117 | heap.Pop(&bsm.bsrHeap) | 从堆中弹出最后一个元素 |



