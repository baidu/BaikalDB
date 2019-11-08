# 异步Mysql连接池

## 编译

## 依赖
* boost 1.56以上
* libmariadb
* gflags
* [bthread](https://github.com/apache/incubator-brpc)(开启GLOG)  

上述依赖使用CMAKE自动下载编译。如果指定了编译选项`-DWITH_SYSTEM_LIBS=ON`，则会使用系统依赖，需要自行手动安装。  

Ubuntu/WSL
```bash
sudo apt-get install cmake libssl-dev
```

Centos
```bash
yum install cmake
yum install openssl-devel
```

## 命令
```bash
mkdir build && cd build
cmake .. && make
```
