| 源码                                         | 函数                | 调用了   | 被调函数 | 说明                                        |
|--------------------------------------------|-------------------|-------| ---- |-------------------------------------------|
| app/vmselect/main.go:63                    | main              | -     | - | -                                         |
| -                                          | -                 | :92   | requestHandler | http回调                                    |
| :132                                       | requestHandler    | -     | - | -                                         |
| -                                          | -                 | :223  | selectHandler | 查询入口                                      |
| :238                                       | selectHandler     | -     | - | -                                         |
| -                                          | -                 | :329  | prometheus.QueryRangeHandler | 执行query-range                             |
| app/vmselect/prometheus/prometheus.go:1197 | QueryRangeHandler | -     | - | -                                         |
| :1227                                      | queryRangeHandler | -     | - | 解析参数后，执行queryRange主逻辑:<br/>解析参数->执行->返回结果 |
| - | - | :1263 |  promql.Exec | 执行查询语句 |
