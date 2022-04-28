## RedRock是什么？

RedRock是一个100%的Redis服务器程序，但支持数据扩大到磁盘。因为内存太贵，我们希望用磁盘来存取大部分冷温数据，而内存只存储热数据。

虽然Redis有RDB/AOF去读写盘，但对于Redis而言，这只是数据的备份。而RedRock是将整个Redis的数据，超出内存的部分转为磁盘存储，这样，但你的应用要存取的数据超过整个内存，又希望整个应用的性能不要太多损失（比如：90%的热数据的访问和原来的Redis一样，时延Latency都在1ms以下），同时整个成本能大大降低（比如：数据大小超过内存的10倍或100倍，这样，整个存储的成本只有原来的1/10甚至1/100）。

RedRock基于Redis源码(当前基于Redis 6.2.2版本)修改，用了RocksDB作为磁盘存储库。因此，RedRock支持Redis的所有特性，包括：

1. 所有的数据结构：String，Hash, Set, List, Sorted Set(ZSet)，Bitmap，HyperLogLog，Geo，Stream
2. 所有的特性：Pipeline，Transaction，Script(Lua)，Pub/Sub，Module
3. 所有的管理：Server Management，Connection Management，ACL，TLS，SlowLog，Memory Management, Config
4. 所有的存储：包括RDB以及AOF(含Rewrite)，同时支持同步和异步两种指令
5. 所有的集群：包括Cluster，Master/Slave，Sentinel
6. 所有的命令：这样你的客户端程序不用做任何更改，只要将服务器执行文件（只有一个）替换掉即可，系统替换很简单

详细可以参考：[RedRock的特性](features.md)

## 安装

### 直接下载执行文件

#### Linux

可以用（其中之一）curl、wget、或者点击下面的https连接直接下载执行文件redrock（），然后在下面的平台Ubuntu 20，Ubuntu 18，CentOS 8，CentOS 7，Debian 11（都经过测试）直接运行，其他Linux平台用户也可以尝试下载

```
curl https://github.com/szstonelee/redrock/dl/redrock -o redrock
或者镜像站点
curl https://hub.fastgit.xyz/szstonelee/redrock/dl/redrock -o redrock
```

```
wget https://github.com/szstonelee/redrock/dl/redrock -o redrock
或者镜像站点
wget https://hub.fastgit.xyz/szstonelee/redrock/dl/redrock -o redrock
```

或者下面的连接：
github: [https://github.com/szstonelee/redrock/dl/redrock](https://github.com/szstonelee/redrock/dl/redrock)
镜像站点：[https://hub.fastgit.xyz/szstonelee/redrock/dl/redrock](https://hub.fastgit.xyz/szstonelee/redrock/dl/redrock)s

#### Mac

```
curl https://github.com/szstonelee/redrock/dl/redrock_mac -o redrock
curl https://hub.fastgit.xyz/szstonelee/redrock/dl/redrock -o redrock
wget https://github.com/szstonelee/redrock/dl/redrock_mac -o redrcok
wget https://hub.fastgit.xyz/szstonelee/redrock/dl/redrock -o redrock
```

### 源码编译

请参考[源码编译](source-build.md)

## 简单验证磁盘效果

如何证明RedRock有上面的磁盘特性，请用下面的测试用例