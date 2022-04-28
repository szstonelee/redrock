## RedRock源码编译

注：最好以root身份运行下面的命令，否则请加上sudo

### 首先安装C/C++编译环境

RedRock需要最低gcc/g++7编译环境，如果未安装，可以安装最新的gcc/g++

#### CentOS

#### Ubuntu

#### MacOS


### 下载和编译支持库

需要至少两个库的支持，先是lz4，然后是RocksDB（RocksDB需要知道lz4安装成功）

#### lz4

```
git clone https://github.com/lz4/lz4.git
cd lz4
make
make install
```
注：上面的github.com可以替换为hub.fastgit.xyz

#### RocksDB

```
git clone -b v7.2.0 https://github.com/facebook/rocksdb.git
cd rocksdb
make shared_lib
make install
```

### 下载RedRock源代码

```
git clone https://github.com/szstonelee/redrock.git
```
注：上面的github.com可以替换为hub.fastgit.xyz

### 编译RedRock

```
cd redrock
cd src
make server
```

将在src目录下，产生一个执行文件redrock，然后运行它
```
sudo ./redrock
```