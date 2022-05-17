[回RedRock主目录页](../README.md)

# RedRock新增命令\配置参数\取消特性

## 一些新增的命令

| 新增命令 | 说明 | 相关链接 |
| -- | -- | :--: |
| rockevict | 将某个（或某些）key存盘 | [内存磁盘管理](memory.md) |
| rockevicthash | 将Hash的某个（或某些）field存盘 | [内存磁盘管理](memory.md) |
| rockstat | 磁盘存盘的相关统计信息 | [内存磁盘管理](memory.md) |
| rockall | 将所有的数据存盘 | [内存磁盘管理](memory.md) |
| rockmem | 按某一内存标准进行存盘从而腾出内存空间 | [内存磁盘管理](memory.md) |

### rockevict

ROCKEVICT key [key ...]

将至少一个key（或多个key）存储到磁盘，如果可以，将提示成功，否则，提示不能转储和原因。

注意：有些key是不能或无需转储到磁盘的，比如：有些Redis中有些key的值是共享的（e.g., set key 1），这时，对于这种值进行存盘，没有意义，因为节省不了磁盘。对于TTL的key，也不转储，因为TTL的key会在不久的将来自动消失，转储磁盘无太大的意义。但rockall和rockmem，以及RedRock自动后台处理，是可以将TTL的key转储到磁盘上的。

### rockevicthash

ROCKEVICTHASH key field [field ...]

对于大Hash的某个field（或多个fields）存储到磁盘。

### rockstat

ROCKSTAT

获得当前RedRock对于磁盘相关的一些统计信息。

这个命令执行很快，不用担心它的耗时，类似Redis的INFO命令。

| 输出字段 | 意义 |
| -- | -- |
| used_human | 就是Redis INFO命令中的memory相关字段，表示当前Redis正在使用的内存（注意：不含RocksDB内存）|
| used_peak_human | 就是Redis INFO命令中的memory相关字段，历史发生的最高Redis正在使用的内存（注意：不含RocksDB内存）|
| sys_human | 当前机器硬件（操作系统）的内存数量 |
| free_hmem | 当前操作系统认为的free memory，注意：操作系统的page cache不在这个统计之类，而且page cache如果较多，可以由操作系统自行判断，在未来转为free memory of OS |
| max_ps_hmem | 参考下面的maxpsmem |
| max_rock_human | 参考下面的maxrockmem |
| rss_hmem | 当前RedRock进程真正所使用的内存，类似ps，top命令的进程内存汇报 |
| rocksdb(and other) | RocksDB（也包括其他Redis不知的内存，如程序代码）所占的内存 |
| no_zero_dbnum | 有几个Redis库有数据，RedRock缺省也是16个库 |
| key_num | 所有key的总数，注意，它并保证等于 evict_key_num + key_in_disk_num，因为有些key是不能转储到磁盘的，比如set k 1，此时k是共享状态 |
| evict_key_num | 在内存中，未来可以被转储的key的数量（不含可以Field转储的Hash）|
| key_in_disk_num | 已经被存储到磁盘上的key的数量（不含可以Field转储的Hash）|
| evict_hash_num | 在内存中，未来可以被转储的Hash的数量 |
| evict_field_num | 在内存中，可以转储Field的Hash中，还有value在内存的Field的总数 |
| field_in_disk_num | 已经被转储到磁盘的Field的数量 |
| hash-max-ziplist-entries | 参考下面的hash-max-rock-entries |
| hash-max-rock-entries | 参考下面的hash-max-rock-entries |
| stat_key_total | 一段时间内，所有的key的访问总数（不含涉及field的统计） |
| stat_key_rock | 一段时间内，这些访问key的数量中，有多少key是位于磁盘 |
| key_percent | stat_key_rock / stat_key_total * 100%，可以得知key miss in memory的百分率 |
| stat_field_total | 一段时间内，所有涉及大Hash的field的访问总数 |
| stat_field_rock | 一段时间内，这些访问field中，有多少field是位于磁盘 |
| field_percent | stat_field_rock / stat_field_total * 100%，可以得知field miss in memory的百分率 |

注意：CONFIG RESETSTAT将重置stat_key_total、stat_key_rock、stat_field_total、stat_field_roc为0。

### rockall

ROCKALL

将所有的key（也包括大Hash的所有field）进行存盘。

注意：如果数据记录集很大，这个将相当耗时（可能是分钟级的），因为需要全部写盘。

### rockmem

ROCKMEM memsize [timeout_seconds]

将内存量近似等于memsize的数据转储到磁盘，从而腾出这么多的内存。

memsize必须是以下的格式：

num**m** or num**M** or num**g** or num**G**

e.g. ```rockmem 77m``` ```rockmem 77M``` ```rockmem 77g``` ```rockmem 77G```

上面的例子分别是将77M字节或77G字节的内存转储到磁盘从而腾出这么多的内存空间（如果有这么多的话，如果不够将全部数据转储）。

如果不带附加参数timeout_seconds，那么RedRock将完成这个操作才能处理其他命令，即服务器会在这段时间block住（类似Redis的SAVE命令）。

如果想在最多一个时间限额完成或者到时即未完成结束（没有完成足够量内存的磁盘转储，但至少发生了一部分转储），那么请带入timeout_seconds这个参数。则这个命令将不会超过这个时间限额，从而让存盘有时间保证，不会耽误整个系统的正常运行（比如：timeout_seconds == 3，这样此次操作，系统不会停止处理Redis命令超过3秒钟）。注意：timeout_seconds是近似秒数，一般大多数情况下，不会有1秒的误差，操作系统如果恶化则不能保证，操作系统恶化一般发生在内存很紧张，从而导致正常的系统调用超时完成。

## 一些新增和修改的配置参数

| 配置参数 | 性质 | 说明 |
| -- | -- | -- |
| maxrockmem | 新增，运行中可动态配置 | 内存在什么情况下，将数据存取磁盘，详细请参考[内存磁盘管理](memory.md) |
| maxpsmem | 新增，运行中可动态配置 | 内存在什么情况下，对于可能产生内存新消耗的Redis命令拒绝执行，详细请参考[内存磁盘管理](memory.md) |
| maxmemory | 改变，不可修改，永远disable | maxpsmem替换了maxmemory，RedRock不支持自动Eviction功能 |
| maxmemory-policy | 改变，运行中可动态配置 | 不再支持Eviction，而用于LRU/LFU算法进行磁盘转储 |
| hash-max-rock-entries | 新增，运行中可动态配置 | hash数据结构在什么情况下，将部分存盘而不是全部存盘，详细请参考[内存磁盘管理](memory.md) |
| hash-max-ziplist-entries | 改变，运行中可动态配置 | 和hash-max-rock-entries有一定的相关性，详细请参考[内存磁盘管理](memory.md) |
| statsd | 新增，运行中可动态配置 | 配置RedRock如何输出metric报告给StatsD服务器 |
| hz | 改变，运行中可动态配置 | 新增服务器定时清理内存到磁盘，详细请参考[内存磁盘管理](memory.md) |
| rocksdb_folder | 新增，运行中不可改变 | RedRock工作时使用的临时目录，RocksDB存盘的父目录 |

注1：和Redis一样，这些参数都可以在命令行启动时加入，或则直接写到redis.conf文件里，例如：

```
sudo ./redrock --rocksdb_folder /opt/temp/myfolder --bind 0.0.0.0
```

注2：可动态配置参数，像Redis一样，请使用CONFIG GET和CONFIG SET命令。

### maxrockmem

设置什么内存条件下，RedRock将开始磁盘转储。

缺省是0，这时将自动定义内存限额是：程序启动时的系统剩余内存 - 2G。这时，RedRock进程认为这个机器上，只有它一个比较消耗内存的服务进程。

对于新启动的操作系统，而且只有一个RedRock服务进程的硬件环境，我们可以认为：

启动加载后操作系统的空余内存 约等于 硬件的内存。

注意1：如果RedRock系统被长时间运行后，导致有大量的page cache占用内存，从而导致重新启动空余内存不够，RedRock会拒绝启动，这时，只要根据提示，手工清理一下page cache即可。方法如下：
```
sync; echo 1 > /proc/sys/vm/drop_caches
```
同时，如果是重启RedRock进程而且maxrockmem是缺省值为0的情况下，建议提前用这个命令清除一下操作系统的page cache。

注意2，RedRock认为的内存使用，是Redis认为的内存消耗，主要是内存数据集，同时也包括TCP连接消耗、临时buffer（比如：Redis AOF Rewrite），但是不包括RocksDB所使用的动态内存（也不包括操作系统加载代码消耗的内存）。

即用Redis INFO命令观测到的内存消耗，而不是诸如Linux ps, top命令观测到的进程内存消耗。

所以，如果保守，应该让maxrockmem低一点。

我的建议：如果你的机器上只用于RedRock，那么留出1G内存给操作系统，1G内存给RocksDB，是最少的。当然，maxrockmem越小越安全，因为RocksDB有可能会超出1G这个数量(RocksDB在大工作量情况下，内存使用可能很大，而且不好控制)。

一个范例：

如果你的机器是16G，那么让maxrockmem设置为14G，是最大限度。如果能更小，比如13G, 12G, 11G，系统会更安全（但内存可能没有全用满）。

另外，如果机器上有其他很消耗内存的进程运行，比如还有其他Redis或RedRock进程，那么千万不要用maxrockmem的缺省数值0，因为这样，两个RedRock进程都认为它可以使用14G内存，从而会肯定导致至少一个进程被操作系统杀死。

你可以定义maxrockmem为一个很大的数，远远超过系统内存，这时候，RedRock将不会发生磁盘转储，也就意味着RedRock会容纳足够多的内存数据，直到被操作系统杀死。

### maxpsmem

这个配置，是控制内存的进程最大容量。ps，是process的缩写。

缺省是负值-1，表示不启用。

当是其他正值时（如果是0，则是系统内存的90%），那么RedRock如果发现自己进程的内存（注意：不是Redis INFO命令汇报的，而是类似Linux ps命令获得的内存消耗）超过这个阀值时，RedRock将不执行一些可能导致内存增加的Redis命令，比如：APPEND、SET等。

客户端你不能执行这些Write命令的提示如下：
```
(error) OOM command not allowed when process memory is more than 'maxpsmem'.
```

这时只读命令仍可以执行，因为只读命令不消耗内存。

即RedRock开始保护整个进程的安全，在这个内存限额下禁止Write类型的命令。

注：当内存消耗超过maxpsmem，你可以继续使用DEL命令，因为这些Write命令是减少内存的。

### maxmemory-policy

如果指定LFU算法时，RedRock将使用LFU算法对于内存里多出的对象进行数据存盘。

其他情况下，都是LRU算法转储磁盘。

注意：这个配置本来Redis是用于eviction功能的。由于Eviction功能在RedRock不再需要，因为磁盘已经扩展了内存，所以，对于eviction policy，除了LFU，其他诸如Random/NoEviction/Volatile等，RedRock都自动采用LRU算法。

即RedRock总是将冷数据存盘，如果不是LFU算法（通过maxmemory-policy设置），那么就一定是LRU算法。

### hash-max-rock-entries

这个是设置大Hash存储field时，至少需要多少field number数。

它必须大于hash-max-ziplist-entries这个参数。

缺省值是0，表示不启用Hash只存储部分field到磁盘这个特性。这时，整个hash都作为一个整体进行存储，即使这个hash有上百万个field。

Redis中，当一个Hash，其feild number超过hash-max-ziplist-entries时，Redis将不采用ZipList编码，而是纯HashMap编码。即hash-max-ziplist-entries是为小的Hash准备的内存压缩编码方式参数。

而hash-max-rock-entries是为了让RedRock决定，什么样的Hash，采用全部入磁盘（全部field value都入盘），还是部分入磁盘（只入部分fiield对应的value值）。

例如：

设置hash-max-ziplist-entries为512，那么当Hash的field数量小于512时，它将用ZipList编码在内存。而超过这个field number，将使用纯HashMap在内存编码。

如果RedRock需要对这个Hash进行部分field存盘，它将参考另外一个参数：hash-max-rock-entries。

比如：我们设置hash-max-rock-entries为1023。那么当这个Hash的field number达到1024或以上的时候，RedRock将只对部分field存盘，而不是整个Hash都存盘。

这对于比较大的Hash非常有帮助。比如：有的Hash有百万个field，如果用整个Hash存盘，那么效率会非常低。这时，采用部分field 存盘会有助于内存/磁盘的真正优化。

详细可以参考：[内存磁盘管理](memory.md)

### statsd

缺省情况下，或者参数为空字符串，不启用metric汇报给StatsD。

如果想用StatsD存储监控指标metric，那么可以设置这个参数。样例如下：

```127.0.0.1:8125```，或者```127.0.0.1:8125:myprefix```

首先是IP地址，然后其后跟着字符:，然后是StatsD的端口。如果希望所汇报的metric有前缀，可以再加上:myprefix，myprefix是自己定义的metric前缀。

注：StatsD服务器可以down掉，对于RedRock不受影响，因为走的是UDP协议。

当设置了statsd参数后，RedRock将每秒向StatsD服务器汇报下面的metric(.前面会是myprefix):

* .FreeMem，当前操作系统free的内存
* .UsedMem，RedRock所获得的Redis使用内存（不含RocksDB内存）
* .Rss，RedRock进程内存
* .TotalKey，所有的key数量
* .TotalEvictKey，所有可以入磁盘的key的数量
* .TotalDiskKey，所有已如磁盘的key的数量
* .TotalHash，所有可以如磁盘的大Hash的数量
* .TotalHashField，所有field的数量
* .TotalDiskField，所有如磁盘的field的数量
* .KeyTotalVisits，一段时间内，所有key的访问数量
* .KeyDiskVisits，一段时间内，这些key访问到磁盘的数量
* .FieldTotalVisits，一段时间内，所有field的访问数量
* .FieldDiskVisits，一段时间内，这些field访问到磁盘的数量
* .KeySpaceHits，参考Redis的统计说明
* .KeySpaceMisses，参考Redis的统计说明
* .TotalReads，参考Redis的统计说明
* .TotalWrites，参考Redis的统计说明
* .ConnectedClients，参考Redis的统计说明
* .MaxClients，参考Redis的统计说明
* .BlockedClients，参考Redis的统计说明
* .RdbSaveSecs，参考Redis的统计说明
* .RdbCowBytes，参考Redis的统计说明
* .AofRewriteSecs，参考Redis的统计说明
* .AofCowBytes，参考Redis的统计说明
* .TotalReceivedConnections，参考Redis的统计说明
* .TotalProcessedCommands，参考Redis的统计说明
* .InstantOpsPerSecond，参考Redis的统计说明
* .LatestForkUsec，参考Redis的统计说明
* .cmds.%s.calls，注：%s是每个用到的命令，比如GET，SET，其他参考Redis的统计说明
* .cmds.%s.usecPerCall，注：%s是每个用到的命令，比如GET，SET，其他参考Redis的统计说明

### hz

RedRock将定期清理内存，这个定期，是由hz这个参数控制。

比如；hz == 10，则1秒钟内，RedRock将定时清理10次内存到磁盘。

注意：每次清理的幅度都很小，一般如果有需要（内存消耗超过maxrockmem），才进行清理，大部分清理的时间不到1ms，同时，最大timeout也被限制为4ms（不过偶尔会超过，因为RocksDB的特性）。所以，不会导致RedRock执行后台内存清理工作，让服务器陷死。

一般情况下，redis.conf里这个值(缺省值为10)，不需要改变。

如果你的应用内存增加的很频繁，导致RedRock经常由于内存不足而被操作系统杀死，可以调高这个值，但太高（比如超过100）不建议，这会导致RedRock花费太多的时间去处理内存清理工作。

另外一个配搭的工具，就是用rockmem命令来主动清理一部分内存，但这会消耗比较大的时间，因为rockmem是命令，会执行完才返回。但rockmem的集中处理的效率，会高于后台清理，而且会保证完成一定量的内存清理。

### rocksdb_folder

这个是RedRock工作时，让RocksDB存盘的父目录。缺省是：/opt/redrock。

注意：RedRock并不是直接在/opt/redrock下存取SST等文件，而是在其下的子目录，子目录名为rocksdb6379，其中后面是RedRock服务器监听端口号。这样，就可以保证多个RedRock进程同时在一台机器上运行，不形成冲突。

## 减少的命令和特性

减少了一个Redis命令S[WAPDB](https://redis.io/commands/swapdb/)。

因为涉及磁盘存取的一致性，所以，这个命令被取消。我认为这个命令被实际用到的概率并不高。

减少了Redis的Eviction特性，因为不需要。

Redis采用Eviction，是因为内存不够，所以要从内存里淘汰出一些Key，空出新的内存，否则Redis可能因为吃尽硬件内存被操作系统Kill掉，或者让系统陷入整体恶化近似死机。

而RedRock采用了磁盘去扩展了内存，所以内存不够的数据全部转存磁盘，并保证不丢失（Redis Eviction则是肯定丢失，而且是随机的或者近似随机的）。

如果你实在手痒想清除一些内存，可以用[DEL](https://redis.io/commands/del/)、[UNLINK](https://redis.io/commands/unlink/)、[HDEL](https://redis.io/commands/hdel/)，这些删除命令来直接消灭内存里的对象，这时，就腾出了空间。

当然，最好用新增的RedRock命令，比如rockevict\rockevicthash\rockall\rockmem，去主动清理内存到磁盘，同时不丢失任何一个key或hash field。

