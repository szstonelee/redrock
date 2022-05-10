[回主目录页](../README.md)

# 正在建设中 。。。

## 一些新增的命令

| 新增命令 | 说明 | 相关链接 |
| -- | -- | :--: |
| rockevict | 将某个key存盘 | [内存磁盘管理](memory.md) |
| rockevicthash | 将Hash的某个field存盘 | [内存磁盘管理](memory.md) |
| rockstat | 磁盘存盘的相关统计信息 | [内存磁盘管理](memory.md) |
| rockall | 将所有的数据存盘 | [内存磁盘管理](memory.md) |
| rockmem | 按某一内存标准进行存盘 | [内存磁盘管理](memory.md) |

注：swapdb命令不再支持

## 一些新增和修改的配置参数

| 配置参数 | 新增 or 改变 | 说明 |
| -- | :--: | -- |
| maxrockmem | 新增 | 内存在什么情况下，将数据存取磁盘，详细请参考[内存磁盘管理](memory.md) |
| maxrockpsmem | 新增 | 内存在什么情况下，对于可能产生内存新消耗的Redis命令拒绝执行，详细请参考[内存磁盘管理](memory.md) |
| maxmemory | 改变 | maxrockpsmem替换了maxmemory，RedRock不支持自动Eviction功能 |
| hash-max-rock-entries | 新增 | hash数据结构在什么情况下，将部分存盘而不是全部存盘，详细请参考[内存磁盘管理](memory.md) |
| hash-max-ziplist-entries | 改变 | 和hash-max-rock-entries有一定的相关性，详细请参考[内存磁盘管理](memory.md) |
| statsd | 新增 | 配置RedRock如何输出metric报告给StatsD服务器 |
| hz | 改变 | 新增服务器定时清理内存到磁盘，详细请参考[内存磁盘管理](memory.md) |
| rocksdb_folder | 新增 | RedRock工作时使用的临时目录，RocksDB存盘的父目录 |

## 减少的命令和特性



