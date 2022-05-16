[回RedRock主目录页](../README.md)

# RedRock内存磁盘管理

## RedRock的内存和磁盘机理

RedRock是自动监视内存的使用情况，一旦发现内存超过某个阀值，它将在后台进行内存清理，采用LRU/LFU算法，让冷温数据入磁盘，热数据继续保留内存中。

同时，所有的key一直都在内存里，只有key对应的值value才会转入磁盘。

这个阀值，在RedRock里叫maxrockmem。它有缺省配置，为0，表示：当前操作系统的内存值（确切说，是当前操作系统的free memory），再减去2G内存。

比如：你的硬件配置是16G，那么当RedRock发现当前消耗的内存超过14G时，它将在后台定时将一些数据写入磁盘，从而腾出内存空间。

那么这个消耗的内存是什么？

这个消耗的内存空间，是通过Redis的内存分配库（编译配置中是deps/目录下的Jemalloc库）分配的内存，在没有将value转写到盘开始发生时，你可以理解它就是整个数据集的大小（同时也包括一些客户端TCP连接消耗以及临时的buffer，但一般不是太多，可忽略）。

但有一些内存，是不通过Redis进行分配的，比如：加载的代码。更麻烦的是：RocksDB。

注意：**maxrockmem并不考虑RocksDB所用的内存**。

当RedRock读写磁盘时，它是用RocksDB进行的，而RocksDB读写磁盘所用到的内存消耗，并不好控制，因为涉及它的一个特性，LSM tree compaction。详细可自行了解LSM tree的工作原理。

一般情况下，我们给RocksDB留出1G内存空间，大部分情况下都够用了。但如果发生RocksDB大量的读写和compaction，那么1G内存空间会不够。要多少才够RocksDB使用，对不起，我不知道（可能Facebook也不知道），因为它是极其动态的（而且和你的负载load直接相关）。如果你发现RedRock由于磁盘大量读写导致RocksDB所需内存远超过1G，从而发生操作系统Kill掉RedRock进程，那么，你就需要留出更多的内存给RocksDB，以应付高峰QPS时刻。

同时，我们还需要留出至少1G内存给操作系统工作。如果操作系统内存不足，它的工作也不会正常。这个可以通过观测操作系统的free memory得知，在RedRock里，可以通过ROCKSTAT命令获悉这个指标。

缺省情况下(maxrockmem == 0)，RedRock认为内存阀值是：启动加载后操作系统的空余内存 - 2G。这是一个比较高的阀值，也就是相对风险比较高的阀值，但好处是，系统不忙时，内存被充分留给RedRock的热数据。

注意：如果RedRock系统被长时间运行后，导致有大量的page cache占用内存，从而导致RedRock重新启动空余内存不够，此时，RedRock会拒绝启动。只要根据提示，手工清理一下page cache即可。

你可以更改RedRock这个参数maxrockmem，不用缺省值0，给maxrockmem一个足够安全的低值，留出更多的内存空间给RocksDB和操作系统，从而让RedRock更安全，更不易导致操作系统Kill服务器进程。具体多少，因为涉及每个应用程序实际的工作环境不一样，只有实践中不断调试和观测中获得（请参考本文中下面的“如何监测内存和磁盘情况以及相关应急处理”）。

记住：这是个trade-off。

* 如果maxrockmem高，你就会有更多的内存保留给热数据，从而不浪费内存，但缺点是，如果RedRock的RocksDB很忙，它会耗费大量的内存导致RedRcok进程被杀。

* 如果maxrockmem低，就留出更多的内存给RocksDB和操作系统使用，RedRcok进程就更安全，但坏处是：热数据所占用的内存会变少，即内存利用率不足。这就有可能会损害到整体的Throughput，因为热数据少了，读盘可能会变多。

你可能会问，RedRock不是有自动转储磁盘功能，从而腾出内存空间吗？

是的，RedRock是自动清理内存，让多于内存的value写入磁盘，从而腾出更多的内存空间。

但是，这个转储是一个慢动作，因为涉及磁盘操作，同时我们也不能让主线程花费太多的时间做清理工作（否则客户端的响应将会变慢），所以，如果有大量的新key注入，内存的增长速度会远远快于磁盘的写入速度，最后会导致写磁盘释放内存远远慢于新key的到来，那么最终会让内存耗尽，从而导致服务器进程被操作系统杀死（或者操作系统由于内存缺乏陷入死机状态）。

为了防止这种现象的发生，除了设置maxrockmem外，我们还有几个解救办法：

1. 设置maxrockpsmem

这样，一旦服务器进程内存（注意：不是Redis通过Jemalloc分配的内存，而是整个进程消耗的内存，含RocksDB）超过这个限度，那么RedRock将拒绝新key的进入。（也包括很多Redis的写命令，因为它也可能导致内存增长，比如APPEND命令）。即执行这些命令的客户端都会获得错误反馈，直到内存得到缓解，i.e., 整个RedRock进程的内存使用低于maxrockpsmem这个值。

如果想取消maxrockpsmem特性，设置它为负数即可，缺省情况下，它是-1，即disabled。

2. 提高后台的清理速度

这个可以设置hz这个配置参数，让RedRock更快去处理内存清理。但同样地，这是个trade-off，因为RedRock如果更多去处理内存清理，那么它对Redis客户端的响应时间就会变少。

同时，再快的后台处理速度，也可能会赶不上新来key的大量涌入。

3. 用新增的命令直接主动清理

RedRock新增了ROCKMEM等命令，可以主动清理内存。但这也是个trade-off，因为执行这个命令可能会需要不少时间，这个时间内，RedRock是无法响应其他客户端的其他命令。

所以，你只能用maxrockmem\maxrockpsmem\hz\ROCKMEM这几个工具，共同解决RocsDB和操作系统所需的内存不够，这个大QPS高峰状况下可能发生的危机。没有一个是最好的，都有trade-off，根据你的应用情况，选择关键的参数或命令，进行组合才是最佳的。

预了解maxrockmem\maxrockpsmem\hz\ROCKMEM更多信息，详细可参考：[新增命令\配置参数\取消特性](manual.md)以及下面的[如何监测内存和磁盘情况以及相关应急处理](#如何监测内存和磁盘情况以及相关应急处理)

另外，当RedRock服务器是多个进程同时运行在一台硬件服务器上，或者操作系统里还有其他可能巨大消耗内存的进程（比如Redis的redis-server），那么，你绝对不能使用缺省的maxrockmem，因为这个参数的缺省值（即0）是为本操作系统里只有一个进程RedRock使用大量内存的工作环境准备的。

## Key整体存盘和大Hash（部分Field存盘）

缺省情况下，RedRock只对key进行整体的写盘，即每个key都是全部写盘或读盘，这包括所有的Redis内部的Redis数据结构，比如：string, hash, set, list, zset等。

但存在这样一种应用场景，有些应用，会用到很大的Hash，比如，百万记录级别的Hash，这时，对这个大对象，进行整体的读写盘，会是一个非常大的代价。

所以，RedRock还做了特别的优化，当设置了一个特别参数hash-max-rock-entries后，RedRock将对这些大的Hash进行Field部分存储盘。

比如：设置hash-max-rock-entries为1023，那么当某个Hash的Field数达到或超过1024时，它将自动进行部分field存盘，即整个大Hash的Field会一直保留在内存，而部分fieldf对应的value会存盘，从而腾出内存空间。此时，Hash里的field处理，有点类似key了。

所以，这还是一个trade-off。相比整个Hash key存盘，当设置了hash-max-rock-entries从而启用部分field存盘，整个field仍会占内存空间，但field对应的value却可以存盘并腾出内存。所以，我们应该：

1. hash-max-rock-entrie设置一个比较大的数，从而区分哪些是大Hash，哪些不是大Hash。具体多大合适，请根据应用场景进行调试。一般建议至少过千。

2. 大Hash里的Field尽可能短，类似Redis的key的设计。

而对于set没有这样的设计，因为set没有对应的value值。

list和zset也没有这样的设计，因为1，这两个数据结构，很难短时确定某个部分并分解存盘；2，其对应的values值要么不存在，要么很小。而且list和zset如果使用，一般都是经常访问的key，所以，其不太可能会被存储到磁盘上，可参考下面的LRU/LFU算法说明。

另外，stream是不会被存盘的。道理显而易见。

进一步参考：[新增命令\配置参数\取消特性](manual.md#hash-max-rock-entries)

## LRU和LFU算法

RedRock使用LRU和LFU算法，来区分冷热数据。

到底是用LRU，还是LFU，是通过maxmemory-policy这个系统配置参数，可参考[新增命令\配置参数\取消特性](manual.md#maxmemory-policy)

几个特别的点：

1. RedRock使用的LRU/LFU算法，和Redis一样

Redis采用的LRU/LFU算法，是一种很特别的算法，它的好处是，很节省内存和速度极快，坏处是，并不是严格的LRU/LFU。比如：如果有10个key，其中有一个key是最老的，那么进行淘汰挑选时，并不保证这个最老的key一定被选择，只保证它是最大概率被选中。

[具体可参考Redis作者对于这一算法的描述](http://antirez.com/news/109)

2. RedRock还对hash field进行了LRU/LFU

不像Redis，它的LRU和LFU，只针对key。在RedRock里，如果我们设置了hash-max-rock-entries，RedRock将对大的Hash里的field对应的value进行磁盘转储。

这时，LRU/LFU是针对Hash的field而去的，比如：某个大Hash，名字叫myhash，它有两个field，一个是f1, f2。如果f1最近被访问了，那么f2对应的value可能会先被转储到磁盘(概率比f1要大些)。

那么，RedRock进行key和大Hash转储时，又是如何整体对待的呢？

算法是：如果内存的key多（其对象全部在内存），先对key进行LRU/LFU存储磁盘；如果所有的大Hash的所有的field多（其对应的value都在内存），则对所有的大Hash的field进行LRU/LFU存储磁盘。

因此，可能某个时刻，RedRock在对key进行LRU/LFU写盘，当key降低到一定程度（没有足够的key所对应的对象在内存），它转而去处理大Hash里的各个field。这样来回反复，保证整体上，对于key和大Hash是公平的。

## RDB/AOF的影响

RedRock像Redis一样，也采用RDB/AOF备份数据，但是，它对内存的影响是不同的。

### 先看RDB

对于RDB，RedRock也是像Redis一样，有两种模式，一个是前台命令，即[SAVE](https://redis.io/commands/save/)命令，还有一个是后台模式，要么通过[BGSAVE](https://redis.io/commands/bgsave/)主动进行，要么是自动在后台进行，通过配置参数CONFIG SET SAVE或redis.conf（也包含启动命令参数）来设置。

不管是哪种，RDB会对于整个数据集进行备份，写盘，就是一个snapshot backup。

Redis的后台模式，是利用Linux的COW特性，只生成memeroy page table的一个备份，让Redis进程和RDB后台进程共享一份内存。如果Redis在RDB备份时间，数据集不发生改变，那么内存没有太多的增加（理论上，只是多了一个RDB后台进程、一个memory page table的copy）。即使在备份期间，一些数据集发生了修改，由于Linux COW特性，只有被该修改的内存页（memory modified page）才会产生新增内存，因此总体所增加的内存页并不会太多。

但到了RedRock这里，就有很多顾虑。虽然RedRock像Redis一样，也是新开一个RDB备份进程，采用COW来处理内存的page table，但是，有下面几个因素的影响：

1. RocksDB有大量的读盘。因为很多value其实不在内存里，必须从磁盘读出，所以RocksDB要参与工作，而RocksDB工作很忙时，对于内存的需求会变得很大。

2. RocksDB从磁盘读出的数据必须进入内存，因此，RDB备份进程所看到的数据集，并不是一个静态不变的内存区块，而是需要动态增加的（注意：RedRock还是尽可能动态释放这些新增内存已保证内存够用）。

3. 备份时间长（因为一是数据集远大于内存，二是读盘是个慢速动作），从而导致主进程修改的内存更多。如COW原理一样，备份这段时间，主进程仍继续处理客户端的命令处理，从而需要新的内存页（即COW共享内存页的效能降低）。

上面3点，都导致RDB备份时，RedRock对于内存的需求，会远大于Redis的RDB进程备份所需内存。

这就存在两个个风险：

其一，当用RDB备份模式，RedRock需要的内存可能会非常大，导致操作系统分配不到足够的内存，从而出现要么内存不足有进程被杀（而且用内存最多的RedRock最可能被杀），要么是操作系统内存不足，从而导致常规的调用也会很慢，即操作系统陷入恶化死机状态。

其二，RocksDB的RDB进程肯定会有大量的读盘，从而和主进程进行磁盘的竞争。特别是当前处于高QPS阶段，因为客户端的响应，也有可能需要读写盘，即磁盘可能会成为瓶颈。

所以，如果你对RedRock启用RDB备份功能，必须注意到这个风险，建议解决的方案如下：

* 在系统不忙的时候启动RDB备份。

* 给RocksDB留出足够的内存，这个请设置maxrockmem为一个较低的值。

我的建议，如果可以，最好不使用RDB备份，而使用AOF备份。

关闭RDB备份很简单，可以命令行启动带入如下的参数

```
./redrock --save 0
```
或者，通过redis.conf文件里对应的参数，或者redis-cli在线连接执行CONFIG SET SAVE ""

### RedRock推荐最好只使用AOF备份

而AOF备份和RDB备份不同，它是将客户端的命令原型写入AOF备份文件。

这个动作的内存消耗就很低，因为每来一个写命令（读命令都不需要存盘），才追加写入AOF文件尾部，而且AOF可以设置为不是每次写入都sync磁盘，磁盘效率非常高。

不过AOF相比RDB也有不利的地方，比如：SET key val123被执行1百万次，对于RDB来说，它只存一个数据即可，但对于AOF来，如果没有压缩，将会存百万个记录。

当然，AOF做了优化，会定期执行AOF Rewrite，将一些重复的操作合并，降低AOF磁盘的大小（但不可能做到RDB那么彻底，比如SET key中间有其他操作）。

但需要注意，AOF Rewrite会导致对内存的需求变大，因为需要分析多个写操作，然后做合并压缩。但相比RDB对内存的需求要好很多。

不管如何，AOF的文件一般而言，都大于RDB文件，会导致用AOF文件恢复数据的时间更长。但trade-off是，相比RDB，对于内存的需求会少很多。

所以，RedRock运行时，我推荐只用AOF备份模式，因为内存还是整个系统的最大命门，特别是对于RedRock这种数据大小远远超过内存大小的应用场景而言。可以考虑作为一个补充，在系统不忙时（比如：深夜），由系统管理员做一下人工的RDB备份作为，如果觉得AOF文件太大的话，但前提是上面的RDB各种顾虑要充分考虑到。

一些相关的AOF配置命令如下:

启动（或关闭AOF）
```
./redrock --appendonly yes
```
也可以在redis.conf里配置，或者通过redis-cli在线修改，用CONFIG SET appendonly yes。
如果想关闭AOF：请用no替代yes

如果想关闭AOF Rewrite（不受AOF Rewrie的内存影响，代价是很大的AOF文件）
```
./redrock --auto-aof-rewrite-percentage 0
```
也可以在redis.conf里配置，或者通过redis-cli在线修改（用CONFIG SET auto-aof-rewrite-percentage 0）。

### 不采用RDB/AOF的解决方案

还有一种策略，可以彻底抛弃RDB/AOF。

就是用RedRock的集群方案（也就是Redis的集群方案）来替代RDB/AOF方案，然后集群中的每个RedRock节点node，都被设置为既不采用RDB，也不采用AOF.

集群模式包括master/slave和cluster两种模式。

比如对于master/slave，可以挂接多个slave（而且可以系统运行期间自由增减slave），如果master死了，升级其中一个slave为master即可。只要集群里还有一台机器活着，那么数据就没有丢。

这时，数据不完全丢失的保证，并不是通过RDB/AOF，而是通过集群的有效（或者说，足够安全的node机器数量）来保证。而且，这也节省了磁盘空间，同时也免除了RDB/AOF和RocksDB对于磁盘的竞争。

当然，也可以用集群技术，同时辅助RDB/AOF模式。比如：slave在不忙的时候做定时的RDB备份，但master不开启RDB/AOF。

详细可参考：[集群管理](cluster.md)

## 如何监测内存和磁盘情况以及相关应急处理

RedRock提供了一个重要的命令ROCKSTAT，让你了解当前RedRock内存和磁盘状况。

请先参考[新增命令\配置参数\取消特性](manual.md#rockstat)里面对RORCKSTAT的详细说明。

当我们发现RedRock恶化时，一般是free_hmem这个指标很低。同时，这时对应的读写盘的频率很高，可以观察ROCKSTAT里面的几个指数：

key_percent 和 field_percent，如果这个值过高（多高我不知道，根据自己的应用观测），很可能是磁盘读写过多引发RocksDB太忙。

注意：key_percent和field_percent，是从上次执行CONFIG RESETSTAT开始的所有时间的统计，如果从没有执行过CONFIG RESETSTAT，那么就是开机以来。所以，如果想统计忙时的情况，请在忙时开始时，执行一次CONFIG RESETSTAT，比如：应用的高峰期是晚上8点到11点。那么8点，需要执行这个清零动作。

弥补的方法有以下几个：

1. 设置合理（更低）的maxrockmem

[maxrockmem的帮助请先看这里](manual.md#maxrockmem)

maxrockmem是0或者自己定义的某个上限值，这个值太高了。我们需要降低它，从而给操作系统以及RocksDB留出更多的空间。

一般而言，maxrockmem是最重要的调整参数。它设置好了，一劳永逸。

2. 调整hz

[hz的帮助请先看这里](manual.md#hz)

如果RocksDB并不是很剧烈地产生内存需求波动，但在高峰时，我们会发现RedRock还是可能出现free_hmem有点危险，但相对上面的情况比较平滑没有那么剧烈，我们可以尝试调整hz这个参数，让它适当变大。

这时，RedRock会用更多的后台时间去清理内存，但trade-off是，可能让客户端的相应时间变少（因此Latency可能边长，Throughput可能变低）。

相比rockmem，hz是个比较和缓的解决办法，它可以保留足够多的maxrockmem内存给热数据，同时，让RedRock性能损失不大，但仅针对应用不发生剧烈QPS变动的情况下。

3. 救急的ROCKMEM和maxrockpsmem

[ROCKMEM的帮助在此](manual.md#rockmem)，[maxrockpsmem的帮助在此](manual.md#maxrockpsmem)

不管是rockmem，还是hz，它都是需要RedRock通过后台处理方式，通过一段比较长的时间（可能是小时级）来缓解系统的压力。

如果万一在短时间内有大量的输入（Write）涌入，从而很可能导致RedRock恶化并死机，那么此时，唯一能解救的办法就是：

ROCKMEM命令，立刻清理一批内存，注意，这个命令全部完成的时间可能会相当长（比如：分钟级甚至小时级），虽然你可以通过timeout来限定时间，但限定timemout时间后，可能降低的内存不足够从而无法挽救系统。同时，执行ROCKMEM命令后，这段执行时间，其他客户端的命令都不能得以执行。最后，如果操作系统实在恶化的厉害，ROCKMEM也可能失败（太慢或者RedRock进程还是内存不足被操作系统杀死，因为ROCKMEM还是要用到RocksDB存盘）

或者 

设置maxrockpsmem，拒绝此后所有的写入命令，直到进程内存降低，系统恢复正常。但坏处是，所有客户端的写命令都被挡住了（读命令还可以继续，但如果读涉及大量读盘的话，仍可能导致RocksDB需要大量内存）。

上面的工具没有任何一个是万能的，你只能在实践中不断探索，根据你自己的应用，找到最好的配置参数或者解决方案。


