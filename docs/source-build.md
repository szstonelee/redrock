[回RedRock主目录页](../README.md)

# RedRock源码编译

## 一、安装编译环境

注：最好以root身份运行下面的命令，否则请加上sudo

需要安装C/C++编译环境，需要gcc/g++ 7.0以上，以及git，make, autoconf这几个工具

### CentOS安装编译环境

```
yum update -y
yum install git -y
yum groupinstall 'Development Tools' -y
```

这时，gcc和g++的version是4.8，可以用gcc -v和g++ -v检查。

我们需要升级到gcc7和g++7，并且使其有效（否则，即使安装了gcc7，缺省仍是4.8）

```
yum install centos-release-scl -y
yum install devtoolset-7-gcc-c++ -y
scl enable devtoolset-7 bash
```

这时，用gcc -v和g++ -v检查，发现版本到7.3

同时，最好让每次登录都能自动切换到gcc7，请修改~/.bash_profile，加入下面
```
scl enable devtoolset-7 bashs
```

### Ubuntu安装编译环境

```
apt update -y
apt install build-essential -y
apt install autoconf -y
```

### MacOS安装编译环境

```
brew update
brew install autoconf
brew install make
brew install gcc@7
```

check: gcc -v 和 g++ -v


## 二、下载和编译支持库lz4和RocksDB

需要至少两个库的支持，先是lz4，然后是RocksDB（RocksDB需要知道lz4安装成功）

### 先编译和安装lz4支持库

```
git clone https://github.com/lz4/lz4.git
cd lz4
make
make install
```
注：上面的github.com可以替换为hub.fastgit.xyz

可以检查lz4是否成功

对于Linux
```
find /usr -name liblz4.so
find /usr -name liblz4.a
```

对于Mac
```
find /usr -name liblz4.dylib
```

### 接着编译和安装RocksDB支持库

```
git clone -b v7.2.0 https://github.com/facebook/rocksdb.git
cd rocksdb
make shared_lib
make install
```
注：编译时间很长，你最好去喝杯咖啡。

可以检查RocksDB是否成功

对于Linux
```
find /usr -name librocksdb.so
```
然后在用ls命令检查这个文件链接是到7.0以上正确的RocksDB库。

注意：如果查不到，请重新执行```make shared_lib```和```make install```一次。

对于Mac
```
find /usr -name librocksdb.dylib
```
然后在用ls命令检查这个文件链接是到7.0以上正确的RocksDB库。

注意：如果查不到，请重新执行```make shared_lib```和```make install```一次。

## 三、下载和编译RedRock源代码

### 下载RedRock源码

```
git clone https://github.com/szstonelee/redrock.git
```
注：上面的github.com可以替换为hub.fastgit.xyz

### 编译RedRock源码

```
cd redrock
cd src
make server
```

将在src目录下，产生一个执行文件redrock，然后运行它
```
sudo ./redrock
```

你可以检查一下当前redrock的动态链接库是否正确，方法如下：
```
ldd redrock
```
注：如果是MacOS，请用```otool -L redrock```

如果所有的动态链接库.so文件都可以找到，一般是没有问题的，否则，请看下面的一些问题的解决来处理。

## 四、一些问题的解决办法

### 运行时找不到动态链接库

首先确认上面的动态链接库已经正确编译和安装

当运行redrock时，出现下面的提示librocksdb.so.7.2
```
error while loading shared libraries: librocksdb.so.7.2
```

可能是你的操作系统的环境中的动态链接库的路径不对，请找到动态库位置librocksdb.so.7.2
```
find /usr -name librocksdb.so.7.2
```

假设输出是：```/usr/local/lib/librocksdb.so```

那么需要加入动态链接库的搜索，如下
```
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
```

### 编译redrock/deps出错，然后之后也总是出错

首先根据出错安装或调整操作系统一些参数。

然后，需要让deps重新编译，deps目录下是Redis依赖的一些自己的库，比如改写的Jemalloc等，方法如下

```
cd redrock
cd src
make distclean
```

这会清理deps/目录下的编译，然后重新执行
```
cd redrock
cd src
make server
```

### 运行redrock时出现这个提示：error while trying to create folder = /opt/redrock

这说明你的redrock对于/opt/redrock没有权限。

解决办法：请用```sudo ./redrock```。或者以root身份执行redrock。

### 运行redrock出现这个提示：remove RocksDB folder failed, folder = /opt/redrock/rocksdb6379/

一般是再次运行redrock时，忘了以sudo或root身份运行。

原因：redrock每次执行时，如果发现/opt/redrock目录存在，都会全部删除，因为每次redrock运行都会重新建立RocksDB库，并不使用以前的。

解决办法：请用```sudo ./redrock```。或者以root身份执行redrock。

