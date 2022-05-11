[回RedRock主目录页](../README.md)

# RedRock内存磁盘管理

## RedRock的内存和磁盘机理

RedRock是自动监视内存的使用情况，一旦发现内存超过某个阀值，它将在后台进行内存清理，采用LRU/LFU算法，让冷温数据入磁盘，热数据继续保留内存中。

这个阀值，在RedRock里叫maxrockmem。它有缺省配置，即你不配置时，它是当前操作系统的内存值，再减去2G内存。

比如：你的硬件配置是16G，那么当RedRock发现当前消耗的内存超过14G时，它将在后台定时将一些数据写入磁盘，从而腾出内存空间。

那么这个消耗的内存是什么？

这个消耗的内存空间，是通过Redis的内存分配库（编译配置中是deps/目录下的Jemalloc库）分配的内存，在没有写盘发生时，你可以理解它就是整个数据集的大小（同时也包括一些临TCP连接消耗以及临时的buffer，但一般不是太多，可忽略）。

但有一些内存，是不通过Redis进行分配的，比如：加载的代码。更麻烦的是：RocksDB。

当RedRock读写磁盘时，它是用RocksDB进行的，而RocksDB读写磁盘所用到的内存消耗，并不好控制，因为涉及它的一个特性，compaction。

一般情况下，我们给RocksDB留出1G内存空间，大部分情况下都够用了。但如果发生RocksDB大量的读写和compaction，那么1G内存空间会不够。要多少，对不起，我不知道。如果你发现RedRock由于磁盘大量读写导致RocksDB所需内存远超过1G，从而发生操作系统Kill掉RedRock进程，那么，你就需要留出更多的内存给RocksDB，以应付高峰QPS时刻。

同时，我们还需要留出至少1G内存给操作系统工作。

这也是缺省情况下(maxrockmem == 0)，RedRock认为内存阀值是：操作系统内存 - 2G。

你可以更改RedRock这个参数maxrockmem，不用缺省值0，给maxrockmem一个足够安全的低值，留出更多的内存空间给RocksDB和操作系统，从而让RedRock更安全，更不易导致操作系统Kill服务器进程。具体多少，因为涉及每个应用程序实际的工作环境不一样，只有实践中不断调试和观测中获得。

记住：这是个trade-off。

* 如果maxrockmem高，你就会有更多的内存给与热数据，从而不浪费内存，但缺点是，如果RedRock的RocksDB很忙，它会耗费大量的内存导致RedRcok进程被杀。
* 如果maxrockmem低，就留出更多的内存给RocksDB和操作系统使用，RedRcok进程就更安全，但坏处是：热数据所占用的内存会变少，即内存利用率不足。

你可能会问，RedRock不是有自动转储磁盘功能，从而腾出内存空间吗？

是的，RedRock是自动清理内存，让多于内存写入磁盘，从而腾出更多的空间。

但是，这个转储是一个慢动作，因为涉及磁盘操作，同时我们也不能让主线程花费太多的时间处理（否则客户端的响应将会变慢），所以，如果有大量的新key注入，内存的增长速度会远远快于磁盘的写入速度，最后会导致写磁盘释放内存远远慢于新key的到来，那么最终会让内存不够，从而导致服务器进程被操作系统杀死（或者操作系统由于内存缺乏陷入死机状态）。

为了防止这种现象的发生，除了设置maxrockmem外，我们还有几个解救办法：

1. 设置maxrockpsmem

这样，一旦服务器进程内存超过这个限度，那么RedRock将拒绝新key的进入。（也包括很多Redis的写命令，因为它也可能导致内存增长，比如APPEND命令）。即执行这些命令的客户端都会获得错误反馈，知道内存得到缓解，i.e., 整个RedRock进程的内存使用低于maxrockpsmem这个值。

如果想取消maxrockpsmem特性，设置它为负数即可，缺省情况下，它是-1。

2. 提高后台的清理速度

这个可以设置hz这个配置参数，让RedRock更快去处理内存清理。但同样地，这是个trade-off，因为RedRock如果更多去处理内存清理，那么它对Redis客户端的响应时间就会变少，同时，再快的后台处理速度，也赶不上新来key的大量涌入。

3. 用新增的命令直接主动清理

RedRock新增了ROCKMEM等命令，可以主动清理内存。但这也是个trade-off，因为执行这个命令可能会需要不少时间，这个时间内，RedRock是无法响应其他客户端的其他命令。

所以，你只能用maxrockmem\maxrockpsmem\hz\ROCKMEM这几个工具，共同解决RocsDB和操作系统所需的内存不够，这个大QPS高峰状况下可能发生的危机。没有一个是最好的，都有trade-off，根据你的应用情况，选择关键的参数或命令，进行组合才是最佳的。

预了解maxrockmem\maxrockpsmem\hz\ROCKMEM更多信息，详细可参考：[新增命令\配置参数\取消特性](manual.md)。

另外，当RedRock服务器是多个进程同时运行在一台硬件服务器上，或者操作系统里还有其他可能巨大消耗内存的进程（比如Redis的redis-server），那么，你不能使用缺省的maxrockmem，因为这个参数是为单进程RedRock使用的环境准备的。

## Key和大Hash（Field）

缺省情况下，RedRock只对key进行整体的写盘，即每个key都是全部写盘或读盘，这包括所有的Redis内部的Redis数据结构，比如：string, hash, set, list, zset等。

但存在这样一种应用场景，有些应用，会用到很大的Hash，比如，百万记录级别的Hash，这时，对这个大对象，进行整体的读写盘，会是一个非常大的代价。

所以，RedRock还做了特别的优化，当设置了一个特别参数hash-max-rock-entries后，RedRock将对这些大的Hash进行Field部分存储盘。

比如：设置hash-max-rock-entries为1023，那么当某个Hash的Field数达到或超过1024时，它将自动进行部分field存盘，即整个大Hash的Field会一直保留在内存，而部分field对应的value会存盘，从而腾出内存空间。此时，Hash里的field，有点像Redis里的key。

所以，这还是一个trade-off。相比整个Hash key存盘，当设置了hash-max-rock-entries从而启用部分field存盘，整个field仍会占内存空间，但对应的value却可以存盘并腾出内存。所以，我们应该：

1. hash-max-rock-entrie设置一个比价大的数，从而区分哪些是大Hash，哪些不是大Hash。具体多大合适，请根据自己的应用场景进行调试。
2. 大Hash里的Field尽可能短，类似Redis的key的设计

而对于set没有这样的设计，因为set没有对应的value值。

list和zset也没有这样的设计，因为1，很难短时确定某个部分；2，其对应的值要么不存在，要么很小。而且list和zset如果使用，一般都是经常访问的key，所以，其不太可能会被存储到磁盘上，可参考下面的LRU/LFU算法说明。

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

这时，LRU/LFU是针对Hash的field而去的，比如：某个大Hash，名字叫myhash，它有两个field，一个是f1, f2。如果f1最近被访问了，那么f2对应的value可能会先被转储到磁盘。

那么，RedRock进行key和大Hash转储时，又是如何整体对待的呢？

算法是：如果内存的key多（其对象全部在内存），先对key进行LRU/LFU存储磁盘；如果所有的大Hash的所有的field多（其对应的value都在内存），则对所有的大Hash的field进行LRU/LFU存储磁盘。

因此，可能某个时刻，RedRock在对key进行LRU/LFU写盘，当key降低到一定程度（没有足够的key所对应的对象在内存），它转而去处理大Hash里的各个field。这样来回反复，保证整体上，对于key和大Hash是公平的。

## 如何监测内存和磁盘情况以及相关处理应急

RedRock提供了一个重要的命令ROCKSTAT，让你了解当前RedRock内存和磁盘状况，

请先参考[[新增命令\配置参数\取消特性](manual.md#rockstat)里面对RORCKSTAT的详细说明。

当我们发现RedRock恶化时，一般是free_hmem这个指标很低，我们就需要做些操作来弥补。

弥补和检查的方法有以下几个：

1. 设置合理（更低）的rockmem

[rockmem的帮助请先看这里](manual.md#rockmem)

rockmem缺省值是0，表示当前Redis消耗内存（注意：不是RedRock进程内存）为操作系统内存减去2G，这个值太高了。我们需要降低它，从而给操作系统以及RocksDB留出更多的空间。

一般而言，这是读写盘的评率很高，可以观察ROCKSTAT里面的几个指数：

key_percent 和 field_percent，如果这个值过高（多高我不知道，根据自己的应用观测），很可能是磁盘读写过多引发RocksDB太忙。

注意：key_percent和field_percent，是从上次执行CONFIG RESETSTAT开始的所有时间的统计，如果从没有执行过CONFIG RESETSTAT，那么就是开机以来。所以，如果想统计忙时的情况，请在忙时执行一次CONFIG RESETSTAT，比如：应用的高峰期是晚上8点到11点。那么8点，需要执行这个清零动作。

一般而言，rockmem是最重要的调整参数。它设置好了，一劳永逸。

2. 调整hz

[hz的帮助请先看这里](manual.md#hz)

如果RocksDB并不是很剧烈地产生内存需求眼里，但在高峰时，我们会发现RedRock还是可能出现free_hmem有点危险，但相对上面的情况比较平滑没有那么剧烈，我们可以尝试调整hz这个参数，让它适当变大。

这时，RedRock会用更多的后台时间去清理内存，但trade-off是，可能让客户端的相应时间变少（因此Latency可能边长，Throughput可能变低）。

相比rockmem，hz是个比较和缓的解决办法，它可以保留足够多的rockmem内存给热数据，同时，让RedRock性能损失不大，但仅针对应用不发生剧烈QPS变动的情况下。

3. 救急的ROCKMEM和maxrockpsmem

[ROCKMEM的帮助在此](manual.md#rockmem)，[maxrockpsmem的帮助在此](manual.md#maxrockpsmem)

不管是rockmem，还是hz，它都是需要RedRock通过后台处理方式，通过一段比较长的时间（可能是小时级）来缓解系统的压力。

如果万一在短时间内有大量的输入融入，从而很可能导致RedRock恶化并死机，那么此时，唯一能解救的办法就是：

ROCKMEM命令，立刻清理一批内存，注意，这个时间肯能会相当长（比如：分钟级甚至小时级），虽然你可以通过timeout来限定时间，但限定timemout时间后，可能降低的内存不足够从而无法挽救系统。同时，执行ROCKMEM命令后，这段执行时间，其他客户端的命令都不能得以执行。最后，如果操作系统实在恶化的厉害，ROCKMEM也可能失败（太慢或者RedRock进程还是内存不足被操作系统杀死，因为ROCKMEM还是要用到RocksDB存盘）

或者 

设置maxrockpsmem，拒绝此后所有的写入命令，直到观测到内存降低，系统恢复正常。但坏处是，所有客户端的写命令都被挡住了（读命令还可以继续，但如果读涉及大量读盘的话，仍可能导致RocksDB需要大量内存）。

上面的工具没有任何一个是万能的，你只能在实践中不断模式，根据你自己的应用，找到最好的配置参数或者解决方案。


