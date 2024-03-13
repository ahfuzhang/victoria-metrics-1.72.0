| 源码 | 函数 | 调用了 | 被调函数 | 说明 |
| ---- | ---- | ---- | ---- | ---- |
| app/vmselect/main.go:63 | func main() |  |  | vm-select入口 |
|  |  | app/vmselect/main.go:92 | httpserver.Serve(*httpListenAddr, requestHandler) | 启动http服务 |
| app/vmselect/main.go:132 | func requestHandler |  |  | 请求回调 |
|  |  | app/vmselect/main.go:222 | selectHandler | 查询的处理 |
| app/vmselect/main.go:236 | func selectHandler |  |  |  |
|  |  | app/vmselect/main.go:326 | prometheus.QueryRangeHandler | query_range的调用 |
| app/vmselect/prometheus/prometheus.go:1197 | func QueryRangeHandler |  |  |  |
|  |  | app/vmselect/prometheus/prometheus.go:1221 | queryRangeHandler |  |
| app/vmselect/prometheus/prometheus.go:1227 | func queryRangeHandler |  |  |  |
|  |  | app/vmselect/prometheus/prometheus.go:1263 | promql.Exec | 执行查询 |
| app/vmselect/promql/exec.go:29 | func Exec |  |  |  |
|  |  | app/vmselect/promql/exec.go:38 | parsePromQLWithCache(q) | 把字符串的查询表达式，解析为结构化的对象 |
|  |  | app/vmselect/promql/exec.go:44 | evalExpr |  |
| app/vmselect/promql/eval.go:203 | func evalExpr |  |  | 区分是哪一种表达式计算 |
|  |  | app/vmselect/promql/eval.go:208 | evalRollupFunc | 普通查询表达式的情况 |
| app/vmselect/promql/eval.go:502 | func evalRollupFunc |  |  |  |
|  |  | app/vmselect/promql/eval.go:504 | evalRollupFuncWithoutAt | 表达式中没有@的情况 |
| app/vmselect/promql/eval.go:535 | func evalRollupFuncWithoutAt |  |  |  |
|  |  | app/vmselect/promql/eval.go:562 | evalRollupFuncWithMetricExpr |  |
| app/vmselect/promql/eval.go:718 | func evalRollupFuncWithMetricExpr |  |  |  |
|  |  | app/vmselect/promql/eval.go:757 | netstorage.ProcessSearchQuery |  |
| app/vmselect/netstorage/netstorage.go:1405 | func ProcessSearchQuery |  |  |  |
|  |  | app/vmselect/netstorage/netstorage.go:1440 | processSearchQuery |  |
| app/vmselect/netstorage/netstorage.go:1472 | func processSearchQuery |  |  |  |
|  |  | app/vmselect/netstorage/netstorage.go:1477 | startStorageNodesRequest | 有多少个storage节点就创建多少个协程 |

