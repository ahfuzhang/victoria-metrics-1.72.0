
| 源码 | 函数 | 调用了 | 被调函数 | 说明 |
| ---- | ---- | ---- | ---- | ---- |
| VictoriaMetrics-1.72.0-cluster/lib/mergeset/block_stream_writer.go:82 | func (bsw *blockStreamWriter) InitFromFilePart |  |  |  |
|  |  | :95 | filestream.Create() | 创建文件对象 |
| lib/filestream/filestream.go:200 | func Create(path string, nocache bool) |  |  |  |
|  |  | :205 | newWriter |  |
| :208 | newWriter |  |  |  |
|  |  | :211 | getBufioWriter(f) |  |
| :300 | getBufioWriter |  |  |  |
|  |  | :301 | statWriter | statWriter对象，直接读写文件 |


reader对象：

| 源码 | 函数 | 调用了 | 被调函数 | 说明 |
| ---- | ---- | ---- | ---- | ---- |
| lib/filestream/filestream.go:60 | func OpenReaderAt |  |  |  |



vm_filestream_buffered_read_bytes_total 包含了 vm_filestream_real_read_bytes_total

