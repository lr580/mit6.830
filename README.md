项目简介：知名的 mit6.5830(麻省理工大学作业)，内容是手写实现关系型数据库的核心逻辑，使用 Java。

环境部署：

> source: [mit6.830官方介绍](http://dsg.csail.mit.edu/6.5830/) [初始代码仓库](https://github.com/MIT-DB-Class/simple-db-hw-2022)

将上述初始代码仓库克隆到本地，然后可以使用 eclipse 打开项目即可，注意修改项目版本为 jre11 或其他(参见 `README0.md`(即该项目本来的 readme 文档))。

检验部署成功的测试：运行 `test/simpledb/TupleTest.java` 能过编译(但显然会 raise，如第 40 行 assert fail)。

开发过程详细记录：详见 `开发过程笔记.md`

## 更新日志

- 2023/07/07 1h 实现其余要求类，完成了 `Lab2.Exercise1`
- 2023/07/06 1-2h 实现了 `Predicate` 和 `Filter` 类并通过对应单元测试
- 2023/07/06 30min-1h 完成了 `Lab1.Exercise6`
- 2023/07/05 2-2.5h 完成了 `Lab1.Exercise3` 和 `Lab1.Exercise5`
- 2023/07/04 1.5h 完成了 `Lab1.Exercise4`
- 2023/07/04 1h 实现了 `Catalog` 类，完成 `Lab1.Exercise2`
- 2023/07/03 25min 实现了 `Tuple` 类，完成 `Lab1.Exercise1`(未完成 `RecordId` 部分)
- 2023/07/03 1-2h 阅读 lab1 要求，实现了 `TupleDesc` 类并通过单元测试
- 2023/07/01 1-2h 确定选题，浏览项目，决定做 mit6.5830