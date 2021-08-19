# Pidan

Pidan是一个简易版的批处理计算引擎，主要用于学习、研究和交流。每一次commit都是一次较为完整的功能开发，跟随这些提交记录，可以了解到一个分布式批处理计算引擎从无到有的设计思路。

## 构建

`mvn clean package`

将pidan-dist模块中的pre-commit脚本拷贝到本地项目的.git/hooks路径下，保证每次commit前都要跑所有的单元测试。

## 项目特征

* 有WordCount作为示例
* 支持单机模式（单节点单进程多线程）和伪分布式模式（单节点多进程）运行
* 实现了Map、Filter、FlatMap、Reduce、Join等基本语义
* 有比较清晰的shuffle过程，且实现了普通shuffle和sort-shuffle两种shuffle方式
* 有较为完整的单元测试覆盖各项功能
* 有较好的扩展性

## 未来规划

* 支持更多计算语义
* 实现完全分布式的运行模式
* 扩展流处理模块
* 提供更上层的API，如Table API和SQL



