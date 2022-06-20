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
| - | - | :1263 |  promql.Exec | 执行查询语句<br />执行完成后，对结果没有加工 |
| app/vmselect/promql/exec.go:30 | Exec |  |  |  |
|  |  | :39 | parsePromQLWithCache | 解析语句 |
|  |  | :45 | evalExpr | 执行语句 |
|  |  | :60 | timeseriesToResult | 结果格式转换 |
| app/vmselect/promql/eval.go:204 | |  |  |  |



语句解析：
| 源码   | 函数| 调用了   | 被调函数 | 说明   |
|----|----|-------| ---- |----|
| app/vmselect/promql/exec.go:224 | parsePromQLWithCache |  |  |  |
|  |  | :227 | metricsql.Parse(q) |  |
| github.com/VictoriaMetrics/metricsql/parser.go:15 |  |  |  |  |

聚合函数的执行过程：

* 聚合函数映射表：VictoriaMetrics-1.72.0-cluster/app/vmselect/promql/rollup.go:21  
  * 通过一个map，来映射各个函数对应的处理流程

----


汇聚函数执行过程：
| 源码   | 函数| 调用了   | 被调函数 | 说明   |
|----|----|-------| ---- |----|
| app/vmselect/promql/eval.go:204 | evalExpr |  |  | 执行表达式 |
|  |  | :222 | fe, ok := e.(*metricsql.FuncExpr) | 执行函数的情况 |
|  |  | :223 | getRollupFunc | 通过解析好的表达式，得到回调函数 |
|  |  | :244 | evalRollupFuncArgs | 表达式中找到确定函数的情况 |
| app/vmselect/promql/rollup.go:339 | getRollupFunc |  |  | 通过一个map来查询回调handle<br />increase函数对应的call back为 rollupDelta |



increase()的回调handle:
| 源码   | 函数| 调用了   | 被调函数 | 说明   |
|----|----|-------| ---- |----|
| app/vmselect/promql/rollup.go:1442 | rollupDelta |  |  |  |

