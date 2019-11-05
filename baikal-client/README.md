# 异步Mysql连接池

## 编译

## 依赖
* boost 1.56以上
* libmariadb
* gflags
* [bthread](https://github.com/apache/incubator-brpc)(开启GLOG)

## 命令
```bash
mkdir build && cd build
CMAKE_INCLUDE_PATH="/path/to/brpc/include" CMAKE_LIBRARY_PATH="/path/to/brpc/lib" cmake ..
make
```
