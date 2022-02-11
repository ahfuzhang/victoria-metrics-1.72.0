VictoriaMetrics的存储引擎，其思想来自clickhouse
了解clickhouse能够帮助我们更好的理解VM

* [论文：Vectorization vs. Compilation in Query Execution](https://15721.courses.cs.cmu.edu/spring2016/papers/p5-sompolski.pdf)
* [zhihu数据库论文阅读:Vectorization vs. Compilation in Query Execution](https://zhuanlan.zhihu.com/p/385807716)
* [Overview of ClickHouse Architecture](https://clickhouse.com/docs/en/development/architecture/)
* [ClickHouse 架构概述](https://clickhouse.com/docs/zh/development/architecture/)

一段精彩的话：
```
通常有两种不同的加速查询处理的方法：矢量化查询执行和运行时代码生成。在后者中，动态地为每一类查询生成代码，消除了间接分派和动态分派。这两种方法中，并没有哪一种严格地比另一种好。运行时代码生成可以更好地将多个操作融合在一起，从而充分利用 CPU 执行单元和流水线。矢量化查询执行不是特别实用，因为它涉及必须写到缓存并读回的临时向量。如果 L2 缓存容纳不下临时数据，那么这将成为一个问题。但矢量化查询执行更容易利用 CPU 的 SIMD 功能。朋友写的一篇研究论文表明，将两种方法结合起来是更好的选择。ClickHouse 使用了矢量化查询执行，同时初步提供了有限的运行时动态代码生成。
```
