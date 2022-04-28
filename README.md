## 目录

* [RedRock是什么](#redrock是什么)
* [RedRock的安装](#安装)
* [RedRock的特性](features.md)
* [RedRock的管理](management.md)

## RedRock是什么？

RedRock是一个100%兼容的Redis服务器程序，但支持数据扩大到磁盘。因为内存太贵，我们希望用磁盘来存取大部分冷温数据，而内存只存储热数据，这样可以大大节省硬件成本，同时性能和原来的Redis几乎一样。

虽然Redis有RDB/AOF去写盘，但对于Redis而言，这只是数据的备份，即Redeis的命令不能实时读写磁盘上的RDB/AOF文件里包含的数据，整个服务器程序的数据大小仍受限于服务器硬件内存的限制（一般顶级服务器内存最高也就到几个TB）。而RedRock是将超出内存的数据自动转为磁盘存储，这样，热数据在内存保证访问速度，冷温数据在磁盘并且支持实时读写,RedRock为此做了自动的冷热转化，从而大大节省硬件成本。因为磁盘的价格相比内存可以几乎忽略，同时磁盘的大小远远超越内存的限制。内存一般都是GB级别，而磁盘可以轻易配置到TB甚至PB级别，在成本差不多的情况下，数据的大小提升最高到千倍以上。

整个应用的性能不会有太多损失，比如：大部分的访问性能和原来的Redis一样（因为热数据仍在内存中），即99%的时延Latency都在1ms以下。

RedRock是在Redis源码(当前基于Redis 6.2.2版本)上直接修改的，增加了RocksDB作为磁盘存储库。因此，RedRock支持Redis的所有特性，包括：

1. 全数据结构：String，Hash, Set, List, Sorted Set(ZSet)，Bitmap，HyperLogLog，Geo，Stream
2. 所有的特性：Pipeline，Transaction，Script(Lua)，Pub/Sub，Module
3. 所有的管理：Server & Connection & Memory management，ACL，TLS，SlowLog，Config
4. 所有的存储：包括RDB以及AOF，支持同步和异步两种存盘指令。既可以存盘，也可以启动时自动恢复数据
5. 所有的集群：包括Cluster，Master/Slave，Sentinel。对于原有的Redis集群系统不用做任何修改
6. 所有的命令：你的客户端程序不做任何更改，只要将服务器执行文件（只有一个）替换掉即可
7. 增加的特性：可以直接对接StatsD并转为Grafana监测

详细可以参考：[RedRock的特性](features.md)

## 安装

### 安装方式一：直接下载执行文件

#### Linux

可以用（其中之一）curl、wget、点击下面的https连接。直接下载压缩文件redrock.tar(80M)，然后解压为执行文件redrock，然后在平台Ubuntu 20，Ubuntu 18，CentOS 8，CentOS 7，Debian 11（都经过测试）直接运行，其他Linux平台，用户也可以尝试下载和运行。

##### 用curl下载

```
curl https://github.com/szstonelee/redrock/dl/redrock.tar -o redrock.tar
```
或者镜像站点
```
curl https://hub.fastgit.xyz/szstonelee/redrock/dl/redrock.tar -o redrock.tar
```

##### 用wget下载

```
wget https://github.com/szstonelee/redrock/dl/redrock.tar -o redrock.tar
```
或者镜像站点
```
wget https://hub.fastgit.xyz/szstonelee/redrock/dl/redrock.tar -o redrock.tar
```

##### 直接点链接下载（浏览器里点击并存盘即可）

或者下面的连接：
* github: [https://github.com/szstonelee/redrock/dl/redrock.tar](https://github.com/szstonelee/redrock/dl/redrock.tar)
* 镜像站点：[https://hub.fastgit.xyz/szstonelee/redrock/dl/redrock.tar](https://hub.fastgit.xyz/szstonelee/redrock/dl/redrock.tar)

##### 解压和执行
```
tar -xzf redrock.tar
```

然后可以看到本目录下有一个执行文件redrock，执行它和执行redis-server一样，只需要
```
sudo ./redrock
```

如果想从请其他机器连接服务器，请使用
```
sudo ./redrock --bind 0.0.0.0
```

注意：请用root身份，或者sudo命令执行redrock，因为需要权限读写磁盘（缺省是：/opt/redrock目录）

#### Mac

```
curl https://github.com/szstonelee/redrock/dl/redrock_mac -o redrock
```
```
curl https://hub.fastgit.xyz/szstonelee/redrock/dl/redrock_mac -o redrock
```
```
wget https://github.com/szstonelee/redrock/dl/redrock_mac -o redrcok
```
```
wget https://hub.fastgit.xyz/szstonelee/redrock/dl/redrock_mac -o redrock
```

注：redrock_mac不需要tar解压，因为redrock_mac这个执行文件很小（不到2M，因为MacOS倾向于使用动态链接库），但你需要安装下面的动态库lz4和RocksDB

```
brew install lz4
brew install rocksdb
```

### 安装方式二：源码编译

请参考[源码编译](source-build.md)

## 简单验证RedRock的磁盘功效

如何证明RedRock有上面的磁盘特性，请用下面的测试用例