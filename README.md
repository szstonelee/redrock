#RedRockHome

## Table of contents

* [What is RedRock] (#redrock is)
* [Installation of RedRock](#install redrock)
* [RedRock features](docs/features.md)
* [Memory Disk Management](docs/memory.md)
* [Cluster Management](docs/cluster.md)
* [Add command\config parameter\cancel feature](docs/manual.md)
* [Simplify Redis system and dynamic extension](docs/replacement.md)
* [Fifty million high access Redis crack](docs/50M_qps_problem.md)

## What is RedRock?

**RedRock is a 100% compatible Redis server program and supports data expansion to disk**

Although Redis has RDB/AOF disk files, for Redis, this is only a data backup, that is, Redis commands cannot read and write data contained in RDB/AOF files in real time, and the entire Redis data size is still limited by the server hardware memory. limit. The memory of the top-level server is only a few TB at most. Generally, the memory of the server is in the order of GB, and the memory is too expensive.

RedRock automatically converts data that exceeds memory to disk storage. In this way, hot data is in memory to ensure access speed, and cold and warm data is in disk and supports real-time reading and writing. RedRock does automatic hot and cold conversion for this purpose, thereby greatly saving hardware costs. . Because the price of disk is almost negligible compared to memory, and the size of disk far exceeds the limit of memory. The memory is generally at the GB level, and the disk can be easily configured to the TB or even PB level. Under the condition of similar cost, the size of the data can be increased by more than a thousand times.

There will not be much loss in performance, and most of the access performance is the same as the original Redis, because the hot data is still in memory, that is, 99% of the latency is less than 1ms.

RedRock is directly modified on the Redis source code (based on version 6.2.2), and uses the RocksDB library for disk storage. RedRock supports all features of Redis, including:

* Full data structure: String, Hash, Set, List, Sorted Set(ZSet), Bitmap, HyperLogLog, Geo, Stream.
* All features: Pipeline, Transaction, Script(Lua), Pub/Sub, Module.
* All management: Server & Connection & Memory management, ACL, TLS, SlowLog, Config.
* All storage: including RDB and AOF, supports both synchronous and asynchronous save commands. Autosave and restore backup files automatically at startup.
* All clusters: including Cluster, Master/Slave, Sentinel. The cluster configuration can remain unchanged and can be mixed.
* All commands: do not make any changes to your client program, just replace the server executable file (only one).
* Added features: directly connect to StatsD and turn to Grafana to monitor performance indicators, and add some commands and configurations to manage memory and disk.

For details, please refer to: [RedRock features](docs/features.md)

## Install RedRock

### Installation method 1: directly download the executable file redrock

#### Linux (CentOS, Ubuntu, Debian) download the compressed package redrock.tar

You can use curl, wget, or browser to click the link in one of the three ways to directly download the compressed file redrock.tar (80M), and then extract it to the executable file redrock, on the tested platforms Ubuntu 20, Ubuntu 18, Ubuntu 16, CentOS 8 , CentOS 7, Debian 11 run directly like Redis or configured as service.

##### Download redrock.tar with curl

GitHub site
````
curl -L https://github.com/szstonelee/redrock/raw/master/dl/redrock.tar -o redrock.tar
````
Or a mirror site (generally faster for Chinese users)
````
curl -L https://hub.fastgit.xyz/szstonelee/redrock/raw/master/dl/redrock.tar -o redrock.tar
````

##### Download redrock.tar with wget

GitHub site
````
wget https://github.com/szstonelee/redrock/raw/master/dl/redrock.tar -O redrock.tar
````
Or a mirror site (generally faster for Chinese users)
````
wget https://hub.fastgit.xyz/szstonelee/redrock/raw/master/dl/redrock.tar -O redrock.tar
````

##### The browser directly clicks the link to download (click in the browser and save it)

* GitHub: [https://github.com/szstonelee/redrock/raw/master/dl/redrock.tar](https://github.com/szstonelee/redrock/raw/master/dl/redrock.tar)
* Mirror site: [https://hub.fastgit.xyz/szstonelee/redrock/raw/master/dl/redrock.tar](https://hub.fastgit.xyz/szstonelee/redrock/raw/master/dl/ redrock.tar)

##### After successfully downloading redrock.tar (about 80M bytes), it needs to be decompressed and executed
````
tar -xzf redrock.tar
````

Then you can see that there is an executable file redrock (more than 200 M, including lz4 and RocksDB static libraries) in this directory. Executing it is the same as executing redis-server, only need
````
sudo ./redrock
````

If a client (like redis-cli) wants to connect to the server from another machine, use
````
sudo ./redrock --bind 0.0.0.0
````

Note: Please execute redrock as root, or sudo command, because you need permission to read and write the disk (default: /opt/redrock directory).

Other Linux platforms, users can also try to download and run, in theory all Linux can run.

#### MacOS directly download the executable file redrock_mac
Choose one of the following four methods:

````
curl -L https://github.com/szstonelee/redrock/raw/master/dl/redrock_mac -o redrock
chmod +x redrock
````
````
curl -L https://hub.fastgit.xyz/szstonelee/redrock/raw/master/dl/redrock_mac -o redrock
chmod +x redrock
````
````
wget https://github.com/szstonelee/redrock/raw/master/dl/redrock_mac -O redrock
chmod +x redrock
````
````
wget https://hub.fastgit.xyz/szstonelee/raw/master/redrock/dl/redrock_mac -O redrock
chmod +x redrock
````

In addition, MacOS needs to install the lz4 and RocksDB dynamic link libraries to make this executable file have a suitable running environment. The methods are as follows:
````
brew install lz4
brew install rocksdb
brew link --overwrite rocksdb
````

Note 1: redrock_mac does not need tar decompression, because the redrock_mac executable file is very small, less than 2M, because MacOS tends to use dynamic link libraries, but you need to install dynamic libraries lz4 and RocksDB.

Note 2: Also, chmod is required to make the file executable.

Note 3: It is best to check, the method, first find /usr -name librocksdb.dylib, and then perform ls -all on the found librocksdb.dylib to see if the RocksDB version linked to is above 7.

### Installation method 2: source code compilation

Please refer to [source build](docs/source-build.md)

## Simple verification of RedRock's disk efficiency and memory savings

How to prove that RedRock has the above disk characteristics, please use the following test case

### Test instruction

We load the String data of one million records into RedRock at startup, and then use a brand-new command ROCKALL to transfer all the data in memory to the disk to see the changes in the server's memory before and after this.

These million records have been pre-made into a Redis backup RDB data file for download, which is called sample.rdb in the dl/ directory.

Database record description:

* type: string
* Quantity: one million
* Key: k1, k2, ..., k1000000
* Val: starts with a number from 2 to 2000, followed by that number of characters 'v'. For example: 2vv, 4vvvv ...

In this way, each record is about 1K bytes on average, and the entire data record is on the order of 1G bytes.

### Memory Watch Tool

**By Redis Info Command**

You can use the redis-cli tool provided by Redis to connect to the redrock server and execute the INFO command to obtain memory statistics (read used_memory_human in the Memory report).

If there is no redis-cli, it can be executed directly with the following shell command:

````
echo -e "*1\r\n\$4\r\nINFO\r\n" | nc 127.0.0.1 6379 | grep used_memory_human
````

**By Linux ps tool**

````
ps -eo command -eo rss | grep redrock
````

### How to download the prepared test Redis database data backup file (RDB format)

You can download this file sample.rdb (32.5M) directly from GitHub and change it to dump.rdb. In this way, redrock will automatically load this test database backup like redis-server when it starts.

Methods as below
````
curl -L https://github.com/szstonelee/redrock/raw/master/dl/sample.rdb -o dump.rdb
````
Note: You can use the mirror site hub.fastgit.xyz instead of github.com, or you can use wget to get this backup file.

### How to save all test data to disk

RedRock has added a new ROCKALL command (Redis commands are case insensitive), which forces all data to be saved to disk (but the memory still retains all key fields). Under normal circumstances, this is not necessary, because RedRock automatically saves and only automatically saves the cold data that exceeds the memory. But here, we need to immediately compare the memory usage before and after the tested data enters the disk (because the above test data is not large, normally 1 G of memory can be installed), so we need to use this RedRock new Added ROCKALL command.

Execute rockall directly in redis-cli, or without redis-cli, use the following shell command
````
echo -e "*1\r\n\$7\r\nROCKALL\r\n" | nc 127.0.0.1 6379
````

Note: For more (usually more) Redis memory record sets or slower disks (the number of CPUs is also an influencing factor, especially for VMs, it is recommended to have more than 1 CPU Core), this execution takes some time , because there is a lot of writing to the disk. Under normal circumstances, the writing of this million records to the disk is completed in a few seconds.

### Test steps

Each step needs to execute the above INFO command and ps command and record the memory result

:

1. Empty database: There should be no dump.rdb and appendonly.aof files in the redrock executable directory, and then start redrock.

2. Load test data: Download the test data database data backup file and replace it with dump.rdb in the current execution directory, you need to restart redrock.

3. Save all data to disk: When redrock has loaded the test data in the memory, execute the ROCKALL command.

### Test results (the data in the table is the memory usage)

#### RedRock

| | Empty database | Load test data | Save all data to disk |
| -- | :--: | :--: | :--: |
| By Redis Info Command | 875K | 1.11G | 54.3M |
| By Linux ps tool | 14M | 1.19G | 167.3M |

Note 1: The memory reported by the Linux shell tool is higher than that of the Redis command INFO, because Redis cannot count the memory occupied by RocksDB (including some memory allocated in advance by the operating system, such as loaded program code).

Note 2: The number of memory may be slightly different under different operating systems, but the order of magnitude is basically the same.

#### Pure Redis comparison reference

We use a 6.2.2 version of Redis as a comparison reference (please download and install redis by yourself, but it must be version 6.2.2, if you want: [You can click here to download, or you can go to the dl/ directory](https:// github.com/szstonelee/redrock/raw/master/dl/redis-server-6.2.2), of course, Redis does not support ROCKALL command for saving. So you can only do 1 and 2 of the above test steps.

| | Empty database | Load test data |
| -- | :--: | :--: |
| By Redis Info Command | 853K | 1.08G |
| By Linux ps tool | 10.2M | 1.16G |

### Test conclusion

1. For millions of String records, when RedRock saves it, whether it is viewed with Redis Info or ps, the memory is reduced from 1G to around 100M, a reduction of more than 90%.

2. The difference between the memory displayed by Redis Info and ps is that Info does not know the amount of memory used by RocksDB.

3. If RedRock is compared with pure Redis, under the same state, there is basically little difference in memory, that is, the additional memory added by RedRock is not much different from that of Redis.

Note: You can do your own tests, for example, more than one million records, it can be 100 million records, please refer to gen_some_str_keys.py in the source code tests/rock/ directory for details.

### After all the files are saved, read the string value according to the key and verify

After completing the test step 3, you can use redis-cli to connect, and then execute the GET command of Redis. The name of the key is arbitrary, as long as it is kxxx, and xxx is any number from 1 to 1000000, for example:
````
get k123
````

If there is no redis-cli, you can use the following shell command instead (but it is recommended to use redis-cli, because the following command, if you change kxxx at will, you need to be familiar with the RESP protocol of Redis)
````
echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\nk123\r\n" | nc 127.0.0.1 6379
````
Note: If you use echo, you may need to execute it twice, but redis-cli does not have this problem.

At this time, you will find that all the saved data (value) can still be read out. For the above key123, the display results are as follows:

>1243vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv


You can even use the GET command to read out a part of the key in step 2, and then execute the test step 3. After all the keys are saved, use the same command to read out, and finally compare the results.

## The old RedRock project

Link is here: https://github.com/szstonelee/redrock_old

Note: The RedRock project is completely re-written for code. I suggest the new one for RedRock. But the documents of RedRock are now only for Chinese. Please use some translation tool for the documents. I am sorry about that.
