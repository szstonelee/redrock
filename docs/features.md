[回主目录页](../README.md)

# RedRock特性

## 支持所有Redis特性

RedRock是基于Redis源码修改的，所以它继承了所有的Redis的特性，同时支持Redis所有的命令（除了swapdb）。

这样，你的客户端用用程序不需要做任何修改，加上RedRock服务器程序只是一个可执行文件redrock，所以只要像redis-server一样，进行简单替换即可。

此时，RedRock将自动兼容支持所有的原有的正在运行的系统且不用做任何改变和设置，同时，自动开始利用磁盘进行读写，扩展你的数据集远超内存限制。

你甚至可以将RedRock和Redis混用，比如：在Redis集群系统里，有的用RedRock，有的用Redis，没有任何问题。相信可参考： [集群管理](cluster.md)。

## 自动内存/磁盘冷热转化

RedRock将监视内存的使用，当内存快不够的时候，采用LRU/LFU算法，自动将多出的数据转到磁盘。

这样，内存里永远都是最新和最热的数据。

如果万一客户端访问存在磁盘上的冷数据，RedRock将自动读出磁盘的冷数据放入内存。RedRock服务器程序采用定期的后台处理，自动将一些没有那么热的数据再转储到磁盘上。这样反复运行，保证你的系统总是热数据在内存中，而多出的冷数据自动转存到磁盘上。

## 扩展数据的前提下，不降低性能

我们采用Redis的目的，是为了它的速度。所以，RedRock是充分遵从这一原则，并没有采用计算/存储分类的架构来实现，这样可以大大降低时延Latency，让RedRock和Redis一样快。

由于采用自动冷热转化，所以99%的访问仍发生在内存，这样实际的时延Latency仍很快，在1ms以下。

同时，它的磁盘访问就在本地，避免了通过网络访问带来的时延，相比一些计算/存储分离的类Redsi系统，时延要更好，同时更节省机器和内存。

有两个其他类似产品（都是Tony开发，而且用到一样的技术）的测试验证证明了这个性能：

1. 老程序的性能测试报告

当前系统是在我之前写的老程序基础上，升级而来，从代码的效能看，当前的系统要更高一些，因此，其性能报告将优于老的测试报告。而老的测试报告已经证明，这种处理方式的性能相比Redis不会有太呆的损失。

想参考老的性能测试报告，请点击这里：
* [英文性能报告](https://github.com/szstonelee/redrock_old/blob/master/documents/performance_en.md)
* [中文性能报告](https://github.com/szstonelee/redrock_old/blob/master/documents/performance_cn.md)

2. BunnyRedis的测试报告

BunnyRedis是我写的一个让Redis实现强一致的Redis改进程序，它也用到了磁盘存储。从原理上看，BunnyRedis的性能会低于RedRock（特别是写命令）。

BunnyRedis的测试报告已经证明其和Redis相当，所以RedRock的性能只能是更好。

想参考BunnyRedis的性能测试报告，请点击
* [BunnyRedis单机性能测试](https://github.com/szstonelee/bunnyredis/wiki/One-node-benchmark)
* [BunnyRedis三机性能测试](https://github.com/szstonelee/bunnyredis/wiki/Three-nodes-benchmark)
* [BunnyRedis用Pipeline和Transaction提速](https://github.com/szstonelee/bunnyredis/wiki/Improve-by-pipeline-transaction)

## 支持RDB/AOF存盘以及转移

RedRock仍采用Redis的老式存盘方式，即RDB/AOF。这样，你的数据可以保存磁盘，以防止万一掉电、系统奔溃等错误导致的数据完全丢失。

同时，这些备份文件，完全是Redis格式，因此，它可以被转移到其他机器上，让这些机器上新的RedRock服务器，甚至是Redis服务器启动时自动恢复数据。

## 支持Redis的集群系统，并能混用

支持Redis的集群特性，包括：Master/Slave，Sentinel, Cluster这三种模式。

而且，可以混用，即在现有的集群系统下，有的延续用Redis，有的替换为RedRock，没有任何不妥。

详细请参考：[集群管理](cluster.md)

## 简单替换redis-server

因为RedRock全面兼容Redis所有特性，所以，只要用编译好的redrock执行程序，替换掉redis-server即可。

RedRock可以读取Redis所有的配置参数，不管是用命令行启动，还是服务器配置文件redis.conf，同时可以读取和恢复原来Redis的存盘文件RDB/AOF。

所以，如果你想进一步偷懒，可以将redrock改名为redis-server，并替换系统里的服务器执行文件，原来的配置都不用做任何修改。当然，不推荐这样做，特别是混搭系统（集群系统里既有reddrock，也有redis-server），还是用redrock会更清晰，当然，系统配置也要随之改变，不过工作量并不大。

## 同一硬件记起你上，可以启动多个redrcok

像redis-server一样，redrock可以在同一硬件服务器（或VM）上启动，只要监听的端口不同。对于RocksDB工作目录，redrock自动区分，不会产生冲突。

这样，可以像redis-server一样，充分利用CPU多核的优势。

唯一要注意的是：在这种应用场景下，请配置内存的参数，特别是maxrockmem，保证多个redrock进程在同一物理机器上正确工作，详细可参考[内存磁盘管理](memory.md)。

