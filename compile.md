# 使用CMake进行编译

## 依赖
* C++11，GCC4.8以上
* openssl
* zlib
* boost(thread, filesystem, 1.56以上)
* rapidjson
* apache arrow
* gflags
* glog
* snappy
* google re2
* protobuf
* rocksdb 
* [brpc](https://github.com/apache/incubator-brpc) (开启GLOG, 额外依赖leveldb)
* [braft](https://github.com/brpc/braft) (开启GLOG)
* libmariadb(Baikal-Client需要, 一个高性能异步Mysql连接池)
* tcmalloc, gperf (可选)

上述依赖CMAKE会自动下载编译。如果指定了编译选项`-DWITH_SYSTEM_LIBS=ON`，则会使用系统依赖，需要自行手动安装。  

## 编译

### 选项

| 选项 | 默认值 | 说明 |  
| --- | --- | ------------------ |   
| WITH_BAIKAL_CLIENT | ON | 编译 baikal-client |  
| DEBUG | OFF | 开启DEBUG |  
| WITH_SYSTEM_LIBS | OFF | 查找系统目录的库，而不是CMake自动依赖。 |  
| WITH_DEBUG_SYMBOLS | OFF | 附带debug symbols |  
| WITH_GPERF | ON | 链接tcmalloc and profiler |  

### 安装必备工具

Ubuntu/WSL
```bash
sudo apt-get install cmake flex bison libssl-dev
```

Centos
```bash
yum install cmake flex bison
yum install openssl-devel
```

### 命令
```bash
mkdir buildenv && cd buildenv
cmake .. && make
```
