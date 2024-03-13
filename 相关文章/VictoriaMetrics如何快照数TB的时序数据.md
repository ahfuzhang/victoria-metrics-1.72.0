https://zhuanlan.zhihu.com/p/315583711
iyacontrol

VictoriaMetrics是一个快速的时间序列数据库。它支持Prometheus PromQL查询，其具有以下突出特点：

对高基数数据具有非常高的写入性能。有关详细信息，请参见本文。
对大量数据时具有很高的查询性能。有关详细信息，请参见本文和此电子表格。
对时间序列数据具有高压缩率。
在线即时快照，而不会影响数据库操作。
让我们研究一下VictoriaMetrics支持下的即时快照如何工作。

简单介绍ClickHouse
VictoriaMetrics将数据存储在类似于ClickHouse的 MergeTree表 数据结构中。它是用于分析数据和其他事件流的最快的数据库。在典型的分析查询上，它的性能要比PostgreSQL和MySQL等传统数据库高10到1000倍。这意味着单个ClickHouse服务器可以替代大量的常规数据库。

ClickHouse的速度如此之快，这要归功于其MergeTree表引擎的架构。

什么是 MergeTree?
MergeTree是面向列的表引擎，其建立在类似于Log Structured Merge Tree 的数据结构上。 MergeTree属性：

每列的数据分别存储。由于不需要在读取和跳过其他列的数据上花费资源，因此减少了列扫描期间的开销。由于各个列通常包含相似的数据，因此这也提高了每列的压缩率。
行按"主键"排序，该主键可以包含多列。主键没有唯一的约束--多行可能具有相同的主键。这样可以通过主键或其前缀快速进行行查找和范围扫描。另外，由于连续排序的行通常包含相似的数据，因此可以提高压缩率。
行被分成中等大小的块。每个块由每个列的子块组成。每个块都是独立处理的。这意味着在多CPU系统上具有接近完美的可扩展性--只需为所有可用的CPU内核提供独立的块即可。可以配置块大小，但是建议使用大小在64KB-2MB之间的子块，因此它们适合CPU缓存。由于CPU缓存访问比RAM访问快得多，因此可以提高性能。另外，当必须从具有多行的块中仅访问几行时，这会减少开销。
块合并为"parts"。这些parts类似于LSM树中的SSTables。 ClickHouse在后台将较小的parts合并为较大的parts。与规范的LSM不同，MergeTree没有严格的part大小。合并过程提高了查询性能，因为每次查询都检查了较少的parts。另外，合并过程减少了数据文件的数量，因为每个部分都包含与列数成比例的固定数量的文件。Parts合并还有另一个好处--更好的压缩率，因为它可以移动更接近已排序行的列数据。
Parts通过"分区"分组到分区中。最初，ClickHouse允许在"Date"列上创建每月分区。现在，可以使用任意表达式来构建分区键。分区键的不同值导致不同的分区。这样可以快速方便地按分区进行数据存档/删除。
接下来真正介绍VictoriaMetrics中的即时快照。

VictoriaMetrics中的即时快照
VictoriaMetrics将时间序列数据存储在类似MergeTree的表中，因此它受益于上述功能。此外，它还存储了倒排索引，以便通过给定的时间序列选择器进行快速查找。倒排索引存储在mergeset中--一种数据结构，该数据结构建立在MergeTree思想的基础上，并针对倒排索引查找进行了优化。

MergeTree还有一个上面没有提到的重要属性--原子性。这意味着：

新增加的parts要么出现在MergeTree中，要么无法出现。 MergeTree从不包含部分创建的parts。合并过程也是如此--parts要么完全合并为新part，要么无法合并。 MergeTree中没有部分合并的parts。
MergeTree中的parts内容永不变。Parts是不可变的。合并到更大的Part后，才可以删除它们。
这意味着MergeTree始终处于一致状态。

许多文件系统都提供诸如硬链接之类的"怪兽"。硬链接文件共享原始文件的内容。它与原始文件没有区别。硬链接不会在文件系统上占用额外的空间。删除原始文件后，硬链接文件将成为“原始文件”，因为它成为指向原始文件内容的唯一文件。无论文件大小如何，都会立即创建硬链接。

VictoriaMetrics使用硬链接为时间序列数据和倒排索引创建即时快照，因为它们都存储在类似MergeTree的数据结构中。它只是从原子上将所有parts的所有文件硬链接到快照目录。这是安全的，因为parts永远不会改变。之后，可以使用任何合适的工具（例如，用于云存储的rsync或cp）将快照备份/存档到任何存储（例如，Amazon S3或Google Cloud Storage）。存档前无需进行快照压缩，因为它已包含高度压缩的数据--VictoriaMetrics为时间序列数据提供了最佳的压缩率。

新创建的快照在文件系统上不占用额外的空间，因为其文件指向MergeTree表中的现有文件。即使快照大小超过10 TB也是如此！在原始MergeTree合并/删除从快照链接的部分之后，快照开始占用额外的空间。因此，不要忘记删除旧快照以释放磁盘空间。

结论
VictoriaMetrics基于ClickHouse中MergeTree表引擎的出色构想构建了即时快照。可以随时创建这些快照，而不会在VictoriaMetrics上造成任何停机时间或正常操作的中断。

可以在单docker镜像或单服务器二进制文件上评估快照功能。可通过以下http处理程序使用：

/snapshot/list — 列出所有可用的快照
/snapshot/create — 创建一个新的快照
/snapshot/delete?snapshot=… — 删除指定的快照
创建的快照位于/<-storageDataPath>/snapshots目录下，其中-storageDataPath是命令行标志，其中包含VictoriaMetrics存储数据的文件系统路径。

PS: 本文属于翻译，原文



