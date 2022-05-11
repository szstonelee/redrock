[回RedRock主目录页](../README.md)

# RedRock的特性概括

## 支持所有Redis特性

RedRock是基于Redis源码修改的，所以它继承了所有的Redis的特性（除了eviction，因为不需要），同时支持Redis所有的命令（除了swapdb）。

这样，你的客户端应用程序不需要做任何修改，加上RedRock服务器程序只有一个可执行文件redrock，所以只要像redis-server一样，进行简单替换即可。

此时，RedRock将自动兼容，支持所有的原有的正在运行的功能，且不用做任何大的改变和特别设置，同时，自动开始利用磁盘进行读写，扩展你的数据集远超内存限制。

你甚至可以将RedRock和Redis混用，比如：在Redis集群系统里，有的节点用RedRock，有的节点用Redis，没有任何问题。

## 自动内存/磁盘冷热转化

RedRock将监视内存的使用，当内存快不够的时候，采用LRU/LFU算法，自动将多出的数据转到磁盘。

这样，内存里永远都是最新和最热的数据。这样，就保证了速度。因为RedRock本质上，还是内存数据库。

如果万一客户端访问到在磁盘上的冷数据，RedRock将自动找到并读出磁盘的对应的存储数据，放入内存。RedRock服务器程序采用后台处理，自动将一些没有那么热的数据再转储到磁盘上。这样反复运行和自我调整，保证你的系统总是热数据在内存中，而多出的冷数据自动转存到磁盘上。

## 扩展数据到磁盘，但不降低性能

我们采用Redis的目的，是为了它的速度。所以，RedRock也是充分遵从这一原则，即**速度第一**。

由于采用自动冷热转化，所以大部分的访问仍只发生在内存而无需读盘，这样实际99%的时延Latency不变，仍在1ms以下。同时维持多个客户端的连接，达到性能Throughput到几十或几百K qps。

注意：这并不需要99%的访问都是内存里的热数据，只要90%的访问都是常规数据即可（甚至可以更低，具体多低，需要根据应用的实际情况进行测试）。而90%的访问都是常规数据，这是典型的互联网应用的规律和大部分应用的实际场景。所以，基本上，绝大部分的应用的P99都可以达到1ms以下。这就和Redis的性能几乎一致，也就能让RedRock既扩大了数据到磁盘（因此数据大小可以有千倍的提升），同时也具有Redis的高性能。

同时，它的磁盘访问就在本地，避免了通过网络访问带来的时延，相比一些计算/存储分离的类Redis系统，时延要更好，同时更节省机器和内存。我个人觉得，对于Redis这种类型的应用，用存储和计算相分离的方案，并不好。

有两个其他类似产品（都是Tony开发，而且用到一样的技术）的测试，证明了这个高性能保证：

1. 老RedRock的性能测试报告

当前系统是在我之前写的老程序基础上，升级而来，从代码的效能看，当前的系统要更高一些，因此，其性能将优于老的测试报告。而老的测试报告已经证明，这种处理方式的性能相比Redis不会有太多的损失。

想参考老的性能测试报告，请点击这里：
* [老RedRock的英文性能报告](https://github.com/szstonelee/redrock_old/blob/master/documents/performance_en.md)
* [老RedRock的中文性能报告](https://github.com/szstonelee/redrock_old/blob/master/documents/performance_cn.md)

2. BunnyRedis的性能测试报告

BunnyRedis是我写的一个让Redis实现强一致和Half Master/Master集群的Redis改进程序，它也用到了磁盘存储。从原理上看，BunnyRedis的性能会略低于RedRock（特别是写命令）。

BunnyRedis的测试报告已经证明其和Redis相当，所以RedRock的性能只能是更好，也就更接近Redis的高性能。

想参考BunnyRedis的性能测试报告，请点击
* [BunnyRedis单机性能测试](https://github.com/szstonelee/bunnyredis/wiki/One-node-benchmark)
* [BunnyRedis三机性能测试](https://github.com/szstonelee/bunnyredis/wiki/Three-nodes-benchmark)
* [BunnyRedis用Pipeline和Transaction提速](https://github.com/szstonelee/bunnyredis/wiki/Improve-by-pipeline-transaction)

## 支持RDB/AOF存盘以及数据迁移

RedRock仍采用Redis的老式存盘方式，即RDB/AOF。这样，你的数据可以保存磁盘，以防止万一掉电、系统崩溃等错误导致的数据完全丢失。

同时，这些备份文件，完全是Redis格式，因此，它可以被转移到其他机器上，让这些机器上新的RedRock服务器，甚至是原有的Redis服务器启动时自动恢复数据。

这种兼容模式，保证了RedRock的数据可以自由迁移，混合使用。

注：虽然RedRock是基于Redis6.2.2代码更改，所以其RDB/AOF的文件格式也是和Redis6.2.2一样，但由于Redis自己的RDB/AOF文件本来就有兼容性设计，即高版本服务器产生的备份文件，如果里面不出现老版本不兼容的数据（比如：AOF文件里存有新增的6.2.2 Redis命令），那么这些数据文件仍可以被老的服务器识别并使用。比如，对于测试用例的sample.db，是RedRock产生的（因此就是Redis6.2.2版本的RDB格式），在Redis5.0下测试，仍是正常使用没有任何异常。当然，Redis新的高版本服务器可以自动兼容老版本的数据文件，所以当前基于6.2.2的RedRock可以读取之前Redis老版本的任何备份文件。

## 支持Redis的所有HA模式，并能混用

支持Redis的多机特性，包括：Master/Slave，Sentinel, Cluster这三种模式。

而且，可以混用，即在现有的集群系统下，有的节点node延续用Redis，有的节点node替换为RedRock，没有任何不妥。

一个好处在于：保证系统无故障时间可以提供真正的Redis性能，同时大大降低成本。

比如：对于master/slave，master采用昂贵的大内存服务器并使用Redis（客户配置小磁盘或定量云磁盘），而slave采用低成本的小内存的RedRock（只要配备磁盘），这样，就能保证原有Redis性能的基础上，降低整个系统的成本，因为很多时候，slave的利用率实在是太低了，只有故障时（或者系统升级维护时）才临时顶一下。这时，用低成本的RedRock服务器去救急，就是一个很好的解决方案。

详细请参考：[集群管理](cluster.md)

## 简单替换，Redis系统升级到RedRock足够简洁

因为RedRock全面兼容Redis所有特性，所以，只要用编译好的redrock执行程序，替换掉redis-server即可。

RedRock可以读取Redis所有的配置参数，不管是用命令行启动，还是服务器配置文件redis.conf，同时可以读取和恢复原来Redis的存盘文件RDB/AOF。

所以，如果你想进一步偷懒，可以将redrock直接改名为redis-server，并替换系统里的Redis服务器执行文件，原来的所有的配置都不用做一个字的修改。当然，不推荐这样做，特别是混搭系统，还是用redrock会更清晰。不过，就算用redrock这个名字，系统配置改变的工作量并不大，因为redis.conf、RDB/AOF、集群配置这些都不用改变。

当然，如果你想根据自己的应用和硬件做最优定制，可以使用RedRock新增的命令和配置，可参考：[新增命令\配置参数\取消特性](manual.md)

## 可以针对大Hash进行部分Field存盘

在Redis应用中，我们有时会用到比较大的Hash，比如百万记录的大Hash。

如果我们针对一个key整体来进行存储磁盘，那么这个大Hash的读写盘，将会是非常大的代价。

RedRock可以自动处理这种大的Hash，通过设置参数[hash-max-rock-entries](manual.md#hash-max-rock-entries)，可以让RedRock对于大Hash进行部分存盘，即只针对field进行存盘。

而且，算法仍是LRU/LFU，只是针对的是field的访问统计，而不是key的访问统计。

详细可参考：[内存磁盘管理](memory.md)

## 同一硬件上，支持多个redrcok进程并行

像redis-server一样，redrock可以在同一硬件服务器（或VM）上启动，只要监听的端口不同。对于RocksDB工作目录，redrock自动区分，不会产生冲突。

这样，可以像redis-server一样，充分利用CPU多核的优势。尽管从Redis6.0开始，Redis在通信上已经用到了多线程，而且之前Redis也有很多多线程特性（比如其他线程处理大对象的删除），但Redis主逻辑仍是单线程，也就是Redis更偏重于某个CPU核的使用。而RedRock对于磁盘的读写，采用的是多线程，而且RocksDB库也是希望能有多核CPU支持从而加速其compaction任务，即RedRock对于CPU多核的依赖，会远高于Redis，但由于当前很多服务器的CPU核非常多（比如我们看到过百的CPU核，甚至未来会出现过千的CPU核），所以，在一些特定情况下，有用户还是希望能在一个硬件服务器上使用多服务器进程，从而充分利用CPU多核。因此，RedRock像Redis一样，也是支持多进程运行，从而利用到多核CPU。

不过，大部分应用情况下，RedRock服务器瓶颈和Redis一样，来自网络而不是CPU，因此，挑选合适的内存大小以及合适CPU核数的硬件服务器，运行单进程RedRock，仍是最好和最节省的硬件选择和资源利用。建议你根据自己的应用进行仔细的测试，尽可能选择单进程模式去运行RedRock，就像Redis一样。

唯一要注意的是：在多进程并发这种应用场景下，必须配置内存的参数，特别是maxrockmem这个RedRock参数，保证多个redrock进程在同一物理机器上正确工作，详细可参考[新增命令\配置参数\取消特性](manual.md#maxrockmem)。

## 让Redis性能指标自动显示到监控面板

RedRock可以自动汇报Redis的一些监控参数Metrics到StatsD，这样，就可以通过Grafana这样的工具实现服务器性能的自动监控。

对于运维人员，这将大大简化我们的工作操作和相关运维成本。对于一些故障提前预警，对于整个系统的运行状况有可视化的感知。

详细请参考：[新增命令\配置参数\取消特性](manual.md#statsd)

