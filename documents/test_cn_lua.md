[回中文总目录](menu_cn.md) 

# [返回测试目录](test_cn.md)

## 测试Script(Lua)

### 所用方法

```
def _warm_up_with_string()
def _check_lua1()
def _check_lua2()
```

### 如何运行

#### Lua用例1

1. 启动RedRock
```
sudo ./redis-server --maxmemory 100m --enable-rocksdb-feature yes --maxmemory-only-for-rocksdb yes --save ""  --bind 0.0.0.0
```
2, Python3下运行
```
_warm_up_with_string()
_check_lua1()
```

#### Lua用例2

1. 启动RedRock
```
sudo ./redis-server --maxmemory 200m --enable-rocksdb-feature yes --maxmemory-only-for-rocksdb yes --save "" --bind 0.0.0.0
```
2, Python3下运行
```
_warm_up_with_string()
_check_lua2()
```

在testredrock目录下查看test_redrock.py了解更多.