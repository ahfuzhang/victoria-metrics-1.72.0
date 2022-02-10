在victoria-metrics-1.72.0的群集版的基础上增加注释，是为了深入研究存储引擎的实现细节。
注释不改变原有的行号。

# 1.相关名词
* xxhash
* metricRow
* tsid
* 数据和索引
* 索引：indexDB, table, part, block
* 数据: table, big/small, partition, part, block

# 2.需要了解的内容
* 原始的time series数据是如何序列化的
* vm-insert如何转发time series数据到vm-storage
* time series的label name的排序
* 目录结构
* 文件结构
* 对象的层级关系
