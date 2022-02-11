* [大铁憨(胡建洪):浅析下开源时序数据库VictoriaMetrics的存储机制](https://zhuanlan.zhihu.com/p/368912946)
* [介绍一个golang库：fastcache](https://mp.weixin.qq.com/s?__biz=MzI0NzM3NDAyNQ==&mid=2247483766&idx=1&sn=5b941a3c2211eff104064d595c04e7df&chksm=e9b048d0dec7c1c687e639928c8ff3e299194e8ff1ed8ad5d0eb2258468eb48ead77e273e0d6&token=1570151211&lang=zh_CN#rd)
* [介绍一个golang库：zstd](https://www.cnblogs.com/ahfuzhang/p/15842350.html)
* [golang源码阅读：VictoriaMetrics中协程优先级的处理方式](https://www.cnblogs.com/ahfuzhang/p/15847860.html)
* [ahfuzhang随笔分类 - VictoriaMetrics](https://www.cnblogs.com/ahfuzhang/category/2076800.html)
* [大铁憨(胡建洪)的知乎专栏](https://www.zhihu.com/people/datiehan/posts)
* [blackbox:VictoriaMetrics阅读笔记](https://zhuanlan.zhihu.com/p/394961301)
* [valyala: How VictoriaMetrics makes instant snapshots for multi-terabyte time series data](https://valyala.medium.com/how-victoriametrics-makes-instant-snapshots-for-multi-terabyte-time-series-data-e1f3fb0e0282)

文章引用：https://valyala.medium.com/how-victoriametrics-makes-instant-snapshots-for-multi-terabyte-time-series-data-e1f3fb0e0282
```
什么是合并树？
MergeTree 是基于类似于Log Structured Merge Tree的数据结构构建的面向列的表引擎。合并树属性：
* 每列的数据单独存储。这减少了列扫描期间的开销，因为不需要花费资源来读取和跳过其他列的数据。这也提高了每列的压缩率，因为各个列通常包含相似的数据。
* 行按“主键”排序，该“主键”可能跨越多个列。主键没有唯一约束——多行可能有相同的主键。这允许通过主键或其前缀进行快速行查找和范围扫描。此外，这提高了压缩率，因为连续排序的行通常包含相似的数据。
* 行被分成中等大小的块。每个块由每列的子块组成。每个块都是独立处理的。这意味着在多 CPU 系统上接近完美的可扩展性——只需为所有可用的 CPU 内核提供独立的块。可以配置块大小，但建议使用大小在 64KB-2MB 范围内的子块，以便它们适合 CPU 缓存。这提高了性能，因为 CPU 缓存访问比 RAM 访问快得多。此外，当必须从具有许多行的块中访问仅几行时，这会减少开销。
* 块被合并成“部分”。这些部分类似于Log Structured Merge (LSM) 树中的SSTables。ClickHouse 在后台将较小的部分合并为较大的部分。与规范的 LSM 不同，MergeTree 对类似大小的部分没有严格的级别。合并过程提高了查询性能，因为每个查询检查的部件数量较少。此外，合并过程减少了数据文件的数量，因为每个部分都包含与列数成比例的固定数量的文件。部分的合并还有另一个好处——更好的压缩率，因为它为已排序的行移动了更近的列数据。
* 部分通过“分区键”分组到分区中。最初 ClickHouse 允许在 Date 列上创建每月分区。现在可以使用任意表达式来构建分区键。分区键的不同值导致单独的分区。这允许快速轻松地对每个分区的数据进行归档/删除。
```
