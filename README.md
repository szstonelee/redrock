# RedRock首页

## 目录

* [RedRock是什么](#redrock是什么)
* [RedRock的安装](#安装redrock)
* [RedRock的特性](docs/features.md)
* [内存磁盘管理](docs/memory.md)
* [集群管理](docs/cluster.md)
* [新增命令\配置参数\取消特性](docs/manual.md)

## RedRock是什么？

**RedRock是一个100%兼容的Redis服务器程序，且支持数据扩大到磁盘**

虽然Redis有RDB/AOF磁盘文件，但对于Redis而言，这只是数据的备份，即Redis的命令不能实时读写RDB/AOF文件里包含的数据，整个Redis数据大小仍受限于服务器硬件内存的限制。顶级服务器内存最高也就到几个TB，一般服务器内存就是GB量级，而内存太贵。

而RedRock是将超出内存的数据自动转为磁盘存储，这样，热数据在内存保证访问速度，冷温数据在磁盘并且支持实时读写，RedRock为此做了自动的冷热转化，从而大大节省硬件成本。因为磁盘的价格相比内存可以几乎忽略，同时磁盘的大小远远超越内存的限制。内存一般都是GB级别，而磁盘可以轻易配置到TB甚至PB级别，在成本差不多的情况下，数据的大小提升最高到千倍以上。

性能不会有太多损失，大部分的访问性能和原来的Redis一样，因为热数据仍在内存中，即99%的时延Latency都在1ms以下。

RedRock是在Redis源码上直接修改的(基于6.2.2版本)，用RocksDB库进行磁盘存储。RedRock支持Redis的所有特性，包括：

* 全数据结构：String，Hash, Set, List, Sorted Set(ZSet)，Bitmap，HyperLogLog，Geo，Stream。
* 所有的特性：Pipeline，Transaction，Script(Lua)，Pub/Sub，Module。
* 所有的管理：Server & Connection & Memory management，ACL，TLS，SlowLog，Config。
* 所有的存储：包括RDB以及AOF，支持同步和异步两种存盘指令。自动存盘和启动时自动恢复备份文件。
* 所有的集群：包括Cluster，Master/Slave，Sentinel。对于原有的Redis集群系统不用做任何修改。
* 所有的命令：你的客户端程序不做任何更改，只要将服务器执行文件（只有一个）替换掉即可。
* 增加的特性：直接对接StatsD并转为Grafana监测性能指标，并增加一些命令和配置管理内存和磁盘。

详细可以参考：[RedRock的特性](docs/features.md)

## 安装RedRock

### 安装方式一：直接下载执行文件redrock

#### Linux（CentOS, Ubuntu, Debian）下载压缩包redrock.tar

可以用curl、wget、浏览器点击链接三种方式之一，直接下载压缩文件redrock.tar(80M)，然后解压为执行文件redrock，在已测试的平台Ubuntu 20，Ubuntu 18，CentOS 8，CentOS 7，Debian 11上像Redis一样直接运行或配置为service。

##### 用curl下载redrock.tar

GitHub站点
```
curl -L https://github.com/szstonelee/redrock/raw/master/dl/redrock.tar -o redrock.tar
```
或者镜像站点（对于中国用户一般更快）
```
curl -L https://hub.fastgit.xyz/szstonelee/redrock/raw/master/dl/redrock.tar -o redrock.tar
```

##### 用wget下载redrock.tar

GitHub站点
```
wget https://github.com/szstonelee/redrock/raw/master/dl/redrock.tar -O redrock.tar
```
或者镜像站点（对于中国用户一般更快）
```
wget https://hub.fastgit.xyz/szstonelee/redrock/raw/master/dl/redrock.tar -O redrock.tar
```

##### 浏览器直接点链接下载（浏览器里点击并存盘即可）

* GitHub: [https://github.com/szstonelee/redrock/raw/master/dl/redrock.tar](https://github.com/szstonelee/redrock/raw/master/dl/redrock.tar)
* 镜像站点：[https://hub.fastgit.xyz/szstonelee/redrock/raw/master/dl/redrock.tar](https://hub.fastgit.xyz/szstonelee/redrock/raw/master/dl/redrock.tar)

##### 成功下载redrock.tar（80M字节左右）后需要解压和执行
```
tar -xzf redrock.tar
```

然后可以看到本目录下有一个执行文件redrock（200多M，包含lz4和RocksDB静态库），执行它和执行redis-server一样，只需要
```
sudo ./redrock
```

如果客户端（如redis-cli）想从其他机器连接服务器，请使用
```
sudo ./redrock --bind 0.0.0.0
```

注意：请用root身份，或者sudo命令执行redrock，因为需要权限读写磁盘（缺省是：/opt/redrock目录）。

其他Linux平台，用户也可以尝试下载和运行，理论上所有的Linux都可以运行。

#### MacOS直接下载执行文件redrock_mac
下面四个方法选择其中一种即可：

```
curl -L https://github.com/szstonelee/redrock/raw/master/dl/redrock_mac -o redrock
chmod +x redrock
```
```
curl -L https://hub.fastgit.xyz/szstonelee/redrock/raw/master/dl/redrock_mac -o redrock
chmod +x redrock
```
```
wget https://github.com/szstonelee/redrock/raw/master/dl/redrock_mac -O redrock
chmod +x redrock
```
```
wget https://hub.fastgit.xyz/szstonelee/raw/master/redrock/dl/redrock_mac -O redrock
chmod +x redrock
```

另外，MacOS需要安装lz4和RocksDB动态链接库才能让这个执行文件有合适的运行环境，方法如下：
```
brew install lz4
brew install rocksdb
brew link --overwrite rocksdb
```

注1：redrock_mac不需要tar解压，因为redrock_mac这个执行文件很小，不到2M，因为MacOS倾向于使用动态链接库，但你需要安装动态库lz4和RocksDB。

注2：同时，需要chmod将文件设置为可执行。

注3：最好check一下，方法，首先find /usr -name librocksdb.dylib，然后在找到的librocksdb.dylib，进行ls -all，看link到的RocksDB版本是否7以上。

### 安装方式二：源码编译

请参考[源码编译](docs/source-build.md)

## 简单验证RedRock的磁盘功效和内存节省

如何证明RedRock有上面的磁盘特性，请用下面的测试用例

### 测试说明

我们将一百万条记录的String的数据启动时加载到RedRock里，然后再用一个全新的命令ROCKALL，将内存的数据全部转到磁盘上，看这前后服务器的内存的变化。

这一百万记录已预先做成一个Redis的备份RDB数据文件可进行下载，在dl/目录下叫sample.rdb。

数据库记录说明：

* 类型: string
* 数量: 一百万
* Key: k1, k2, ..., k1000000
* Val: 最开始是2到2000数字，后面跟着这个数量的字符'v'。比如：2vv, 4vvvv ...

这样大概平均每条记录是1K字节以上，整个数据记录在1G字节这个量级。

### 内存观测工具

**By Redis Info Command**

可以用Redis提供的redis-cli工具，连接redrock服务器，执行INFO命令获取内存统计(读取Memory报告中的used_memory_human)。

如果没有redis-cli，可以用下面的Shell命令直接执行：

```
echo -e "*1\r\n\$4\r\nINFO\r\n" | nc 127.0.0.1 6379 | grep used_memory_human
```

**By Linux ps tool**

```
ps -eo command -eo rss | grep redrock
```

### 如何下载提前准备好的测试Redis数据库数据备份文件(RDB格式)

可以直接从GitHub下载这个文件sample.rdb（32.5M），然后将之改为dump.rdb。这样，redrock启动时会像redis-server一样自动加载这个测试数据库备份。

方法如下
```
curl -L https://github.com/szstonelee/redrock/raw/master/dl/sample.rdb -o dump.rdb
```
注：可以用镜像站点hub.fastgit.xyz替代github.com，也可以用wget得到这个备份文件。

### 如何将测试数据全部存盘

RedRock新加了一个ROCKALL命令（Redis命令不区分大小写），这个命令强制将所有的数据存盘（但内存仍保留所有的key字段）。正常情况下，不需要这样做，因为RedRock是自动存盘，而且只将超出内存的冷数据自动存盘。但这里，我们需要立刻比较被测试的数据进入磁盘的前后内存使用情况对比情况（因为上面的测试数据并不大，正常1个G的内存就可以全部装下），所以需要用到这个RedRock新增的ROCKALL命令。

直接在redis-cli里执行rockall，或者没有redis-cli，用下面的shell命令
```
echo -e "*1\r\n\$7\r\nROCKALL\r\n" | nc 127.0.0.1 6379 
```

注：对于比较多（一般是更多）的的Redis内存记录集或者比较慢的磁盘（CPU的多少也是个影响因素，特别是针对VM，建议多于1个CPU Core），这个执行需要点时间，因为有大量的写盘动作。正常情况下，几秒钟就完成这百万记录的写盘。

### 测试步骤

每步都需要执行上面的INFO命令和ps命令并记录内存结果：

1. 空数据库: redrock执行文件目录下不要有dump.rdb和appendonly.aof文件，然后启动redrock。

2. 加载测试数据：下载测试数数据库数据备份文件并替换为当前执行目录下的dump.rdb，需要重启redrock。

3. 数据全部存盘：当redrock已经在内存里加载了测试数数据，再执行ROCKALL命令。

### 测试结果（表中数据为内存使用量）

#### RedRock

| | 空数据库 | 加载测试数据 | 数据全部存盘 |
| -- | :--: | :--: | :--: |
| By Redis Info Command | 875K | 1.11G | 54.3M |
| By Linux ps tool | 14M | 1.19G | 167.3M |

注1：Linux shell tool汇报的内存比Redis命令INFO要高，是因为Redis不能统计到RocksDB所占的内存（还包括操作系统提前分配的一些内存，比如加载的程序代码）。

注2：不同操作系统下可能内存数字稍有不同，但数量级基本一致。

#### Pure Redis对比参照

我们用一个6.2.2版本的Redis作为对比参照（请自行下载和安装redis，但必须是6.2.2版本的，如果愿意：[可以点这里下载，也可以到dl/目录](https://github.com/szstonelee/redrock/raw/master/dl/redis-server-6.2.2)，当然，Redis不支持ROCKALL命令进行存盘。所以只能做上面测试步骤的1和2。

| | 空数据库 | 加载测试数据 |
| -- | :--: | :--: | 
| By Redis Info Command | 853K | 1.08G |
| By Linux ps tool | 10.2M | 1.16G |

### 测试结论

1. 对于百万String记录，当RedRock存盘后，不管是用Redis Info还是ps看，内存由1G降低到百兆上下，降低了90%以上。

2. Redis Info和ps显示的内存的不同，主要是Info不知道RocksDB所使用的内存量。

3. 如果用RedRock和纯Redis相比，同等状态下，基本上内存差别不大，即RedRock新增的额外内存相比Redis差别不大。

注：你可以做自己的测试，比如，不止一百万记录，可以是一亿条记录，详细参考源码tests/rock/目录下的gen_some_str_keys.py。

### 全部存盘后的根据key读取string value验证

当做完测试步骤3后，你可以用redis-cli连入，然后执行Redis的GET命令，key的名字任意，只要是kxxx，xxx为1-1000000的任意数字，例如：
```
get k123
```

如果没有redis-cli，可以用下面的shell命令代替（不过建议用redis-cli，因为下面的命令，如果随意改变kxxx，需要你熟悉Redis的RESP协议）
```
echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\nk123\r\n" | nc 127.0.0.1 6379
```
注：如果用echo，你可能需要执行两次，但redis-cli不存在这个问题。

这时，你会发现所有存盘的数据（value）仍旧可以读出，对于上面的key123，显示结果如下：

>1243vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv


你甚至可以在步骤2时，就先用GET命令先读出一部分key，然后执行测试步骤3，全部存盘后，再用同样的命令读出，最后进行结果比对。

## The old RedRock project

Link is here: https://github.com/szstonelee/redrock_old

Note: The RedRock project is completely re-written for code. I suggest the new one for RedRock. But the documents of RedRock are now only for Chinese. Please use some translation tool for the documents. I am sorry about that. 




