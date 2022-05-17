[回RedRock主目录页](../README.md)

# RedRock的集群管理

RedRock支持Redis的所有集群模式，包括：Master/Slave, Sentinel, Cluster。而且可以混用

## Master/Slave

像Redis的Master/Slave一样，进行配置。可以有下面两种方案

### 全部用RedRock搭建Master/Slave

因为RedRock全面兼容Redis，所以没有什么特别。

请参考Redis的官网，[Redis Replication](https://redis.io/docs/manual/replication/)

其他只要根据RedRock的文档，选择好合适的内存、磁盘以及相关配置参数即可。

### 混搭Redis做成Master/Slave

混搭模式是在Redis Master/Slave方案中，有的节点采用redis-server，有的节点采用redrock。

一般而言，建议Master采用Redis服务器。

因为Redis服务器不具备磁盘功能，所以一定要配置比较大的内存，这样，Redis服务器才可以容纳比较大的数据。

注：虽然RedRock不支持Eviction功能，但在混搭系统里，为了保护Redis服务器，你可以设置开启Redis的Eviction功能从而保护Redis服务器，如果某个key被Redis服务器evicted，那么它也会被RedRock删除，这并不会导致任何不兼容。

范例：

```                             
*****************************                          *****************************            
*       Redis master        *                          *     RedRock slave         *
* 640G内存，(云盘或小HDD磁盘)  *           。。。          *   16G内存，本地SSD磁盘      *
*****************************                          *****************************
```

上图中，master采用Redis系统（redis-server），配备640G的大内存，客户端的读写都发生于此。本地可以无磁盘（比如用云盘）或者很小的HDD磁盘。

slave采用RedRock系统，只需配备16G内存，然后配有本地的高速的SSD磁盘。

这样，即使我们的数据集大小达到了600多G，整个系统仍旧安全工作。

万一master死机，或者需要停机维护，我们可以提升RedRock服务器成为master，这样系统仍然正常工作，同时绝大部分数据不会丢失（注意：Redis的Replication并不保证灾难时，i.e., master突然crash，所有数据一个都不会丢失）。

当Redis机器检修回来（或者新启动另外一个大内存服务器），它可以先做成slave同步RedRock，再重新切换为master。

这个成本相比都采用两个640G的服务器做成Redis的master/slave，要便宜很多，而且一般情况下，slave的负载都很低，也可以不用提供服务（因为slave不可以写，读而言，Redis已经足够快了，一般绰绰有余）。而且，因为master是高级配置的服务器，其响应速度是一流的。

这个方案的一个风险在于：

如果客户端访问的负载QPS很大，而且访问的key是平均散列的， 即平均访问到整个数据集600多G，那么当RedRock顶替为master时，其性能可能堪忧，因为会有大量的磁盘读写，因为RedRock适合的应用场景是必须有热数据（大部分应用都遵从有热数据这个规律，可以根据自己的应用环境，测试判断）。

所以，建议在下面两个限定条件下用这个方案（满足其一即可）

1. 不会出现大的QPS负载时进行master/slav切换，即我们可以在深夜业务不忙时，进行Redis服务器的检修，而不能让灾难发生在业务高峰期（但麻烦的是：灾难很多时候是发生在高峰期）。

2. 数据的访问是热数据比较少。比如：虽然整个数据集有600多G，但90%或80%或70%的业务访问只到16G的热数据（具体比例多少合适需要测试）。

## Sentinel

因为RedRock全面兼容Redis，Sentinel的方案也没有什么不同。

你可以用redis-server（建议用6.2.2版本）作为Sentinel进程，也可以用RedRock作为Sentinel进程。

在Sentinel监视下的应用服务器，可以是Redis服务器，也可以是RedRock服务器，即支持混用。

详细可参考：[Redis Sentinel](https://redis.io/docs/manual/sentinel/)

## Cluster

因为RedRock全面兼容Redis，所以可以直接用RedRock服务器，即使用Redis Clusster，但服务器全部采用RedRock。

同样地，也可以进行混搭，即部分服务器采用Redis，部分服务器采用RedRock。

对应的硬件配置考虑和Master/Slave一样。

如果混搭，可以考虑让Redis服务器上的slot多一些，负载重一些，而RedRock服务器相对轻一些，这样就可以充分利用硬件的成本。

详细可参考：[Redis Cluster](https://redis.io/docs/manual/scaling/)
