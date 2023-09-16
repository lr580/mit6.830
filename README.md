项目简介：知名的 mit6.5830(麻省理工大学作业)，内容是手写实现关系型数据库的核心逻辑，使用 Java，务必使用 UTF8 编码(有 `⨝` 等，不然过不了编译)

环境部署：

> source: [mit6.830官方介绍](http://dsg.csail.mit.edu/6.5830/) [初始代码仓库](https://github.com/MIT-DB-Class/simple-db-hw-2022)

将上述初始代码仓库克隆到本地，然后可以使用 eclipse 打开项目即可，注意修改项目版本为 jre11 或其他(参见 `README0.md`(即该项目本来的 readme 文档))。

检验部署成功的测试：运行 `test/simpledb/TupleTest.java` 能过编译(但显然会 raise，如第 40 行 assert fail)。

开发过程详细记录：详见 `开发过程笔记.md`。

花费在项目的总有效用时约为 89.6h

## 项目介绍

基本情况：

MIT 6.830

- 使用 Java 编写的一个关系型数据库。
- 使用分页存取数据，用缓存池维护使用的页面，用 LRU 算法置换页面。
- 实现了事务管理，基于页级锁、两阶段锁、no-steal/force缓存管理、死锁检测。
- 实现了 B+ 树索引、日志回滚恢复、查询优化等其他技术。

简要介绍一些关键技术：

- 严格两阶段锁(Two-Phase Locking)：事务在增长阶段(growing)不断获得锁、不能释放锁；在缩小阶段(shrinking)只能释放锁，严格使得只能一次释放锁。可以使得依赖图无环，严格地保证冲突可串行化。严格能避免级联回滚(cascading aborts)。[参考](https://zhuanlan.zhihu.com/p/480379228)
- no-steal表示未commit的事务不允许将修改写回磁盘，使得abort不需要磁盘回滚。force表示在事务commit必须马上将更新写回磁盘，则即使之后数据库崩溃commit也存在磁盘了。
- 死锁检测使用了有向图拓扑排序判是否成环，以事务为节点，有向边表示等待关系，所有未分配锁的事务都有等待关系。如果加入当前事务产生了死锁，那么就abort掉当前事务。

## 更新日志

- 2023/09/16 5-10h 归纳了项目技术笔记，修复一个直方图 bug
- 2023/07/23 5-10h debug，完成了 `Lab6.Exercise1-2`，完成了全部内容
- 2023/07/21 5-8h debug，修复了一些问题
- 2023/07/19 2-4h debug，修复了一些问题
- 2023/07/19 30min-1h debug，完成了 `Lab5.Exercise4`
- 2023/07/19 2-3h debug，完成 `Lab5.Exercise3`
- 2023/07/18 3-5h debug，完成 `Lab5.Exercise2`
- 2023/07/17 1.5h 完成 `Lab5.Exercise1`
- 2023/07/16 30min 修复高并发安全 bug
- 2023/07/16 2-4h debug，完成了 `Lab4.Exercise5`
- 2023/07/16 1-2h 完成了死锁处理
- 2023/07/16 30min-1h 完成了 `Lab4.Exercise2-4`
- 2023/07/16 1-1.5h 完成了 `Lab4.Exercise1`
- 2023/07/14 3h debug, 完成了 `Lab3.Exercise4`
- 2023/07/14 30min-1h 完成了 `Lab3.Exercise3`
- 2023/07/13 1h-1.5h 完成了 `Lab3.Exercise2`
- 2023/07/13 1h-2h 完成了 `Lab3.Exercise1`
- 2023/07/12 1h 调整编译选项，完成 `Lab2`
- 2023/07/12 10min 完成了 `Lab2.Exercise5`
- 2023/07/12 30min 完成了 `Lab2.Exercise4`
- 2023/07/11 2-2.5h 实现了其余要求类，完成了 `Lab2.Exercise3`
- 2023/07/11 2h 修复一些 bugs，并实现了 `HeapPage` 类
- 2023/07/10 1.5h 实现其余要求类，完成了 `Lab2.Exercise2`
- 2023/07/10 2h 实现了 `IntegerAggregator` 类
- 2023/07/07 1h 实现其余要求类，完成了 `Lab2.Exercise1`
- 2023/07/06 1-2h 实现了 `Predicate` 和 `Filter` 类并通过对应单元测试
- 2023/07/06 30min-1h 完成了 `Lab1.Exercise6`
- 2023/07/05 2-2.5h 完成了 `Lab1.Exercise3` 和 `Lab1.Exercise5`
- 2023/07/04 1.5h 完成了 `Lab1.Exercise4`
- 2023/07/04 1h 实现了 `Catalog` 类，完成 `Lab1.Exercise2`
- 2023/07/03 25min 实现了 `Tuple` 类，完成 `Lab1.Exercise1`(未完成 `RecordId` 部分)
- 2023/07/03 1-2h 阅读 lab1 要求，实现了 `TupleDesc` 类并通过单元测试
- 2023/07/01 1-2h 确定选题，浏览项目，决定做 mit6.5830