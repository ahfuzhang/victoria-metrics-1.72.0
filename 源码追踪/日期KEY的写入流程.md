vm-storage的底层索引是mergetree
mergetree可以看成是一个key-value存储
这个key-value存储中存在以下类型的键值对：
```go
const (
   // Prefix for MetricName->TSID entries.
   nsPrefixMetricNameToTSID = 0

   // Prefix for Tag->MetricID entries.
   nsPrefixTagToMetricIDs = 1

   // Prefix for MetricID->TSID entries.
   nsPrefixMetricIDToTSID = 2

   // Prefix for MetricID->MetricName entries.
   nsPrefixMetricIDToMetricName = 3

   // Prefix for deleted MetricID entries.
   nsPrefixDeletedMetricID = 4

   // Prefix for Date->MetricID entries.
   nsPrefixDateToMetricID = 5

   // Prefix for (Date,Tag)->MetricID entries.
   nsPrefixDateTagToMetricIDs = 6
)
```

这里跟踪 nsPrefixDateToMetricID 类型的存储过程。
使用date+metricID为key，记录某天某个监控项是否存在。
使用date为commonPrefix，这样的话匹配某个日期是否有数据，就匹配date的commonPrefix就行了。

| 源码 | 函数 | 调用了 | 被调函数 | 说明 |
| ---- | ---- | ---- | ---- | ---- |
| app/vmstorage/main.go:53 | func main() | - | - | 入口 |
| - | - | app/vmstorage/main.go:93 | go srv.RunVMInsert() | 启动服务器的协程 |
| app/vmstorage/transport/server.go:85 | func (s *Server) RunVMInsert | - | - | 启动vm-insert对应的服务端 |
| - | - | app/vmstorage/transport/server.go:135 | s.processVMInsertConn(bc) | - |
| app/vmstorage/transport/server.go:261 | func (s *Server) processVMInsertConn | - | - | vm-insert的服务端，用于处理请求 |
| - | - | app/vmstorage/transport/server.go:264 | return s.storage.AddRows(rows, uint8(*precisionBits)) | 写入解析后的metric数据 |
| lib/storage/storage.go:1641 | func (s *Storage) AddRows | - | - | 写入多行数据 |
| - | - | lib/storage/storage.go:1685 | s.add(ic.rrs, ic.tmpMrs, mrsBlock, precisionBits) | 写入数据 |
| lib/storage/storage.go:1800 | func (s *Storage) add | - | - | 来自vm-insert的写入的数据 |
| - | - | lib/storage/storage.go:1943 | s.updatePerDateData(rows, dstMrs) | 调用写入每天的metricID的函数 |
| [lib/storage/storage.go:2048](https://github.com/ahfuzhang/victoria-metrics-1.72.0/blob/8958f6612a36d435723209c74e67749b9aff5477/VictoriaMetrics-1.72.0-cluster/lib/storage/storage.go#L2048) | func (s *Storage) updatePerDateData | - | - | 写入每天的 date + metricID 的KEY到KV存储 |
| - | - | lib/storage/storage.go:2198 | is.storeDateMetricID(date, metricID, mn) | - |
| [lib/storage/index_db.go:2609](https://github.com/ahfuzhang/victoria-metrics-1.72.0/blob/8958f6612a36d435723209c74e67749b9aff5477/VictoriaMetrics-1.72.0-cluster/lib/storage/index_db.go#L2609) | func (is *indexSearch) storeDateMetricID | - | - | - |
| - | - | [lib/storage/index_db.go:2624](https://github.com/ahfuzhang/victoria-metrics-1.72.0/blob/8958f6612a36d435723209c74e67749b9aff5477/VictoriaMetrics-1.72.0-cluster/lib/storage/index_db.go#L2624) | is.db.tb.AddItems(ii.Items) | 把key添加到内存中的 rawItems 对象中，并达到一定数量后进行merge |

