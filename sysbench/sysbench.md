## 安装sysbench环境
* 安装sysbench0.5以上版本
* 安装MYSQL
* 机器支持lua
* 环境变量支持
    * export LD_LIBRARY_PATH=mysql安装地址/lib
    * source ~/.bashrc


## sysbench有哪些功能
* sysbench支持以下几种测试模式
    * 支持CPU运算性能测试
	* 支持磁盘IO性能测试
	* 支持调度程序性能测试
	* 支持内存分配及传输速度测试
	* 支持POSIX线程性能测试
	* 支持数据库性能(OLTP基准测试)测试
* 本文主要使用到：支持数据库性能(`OLTP基准测试`)测试



## 执行sysbench测试case
* 将scripts目录防在src目录下与lua同级
* 配置自己的baikaldb连接信息到config.conf
* 到scripts目录下执行sh prepare.sh/cleanup.sh/read-write_noprepare.sh等测试脚本


## 分析sysbench执行结果

```javascript
  SQL statistics:
    queries performed:
        read:                            55374780
        write:                           0
        other:                           0
        total:                           55374780
    transactions:                        55374780 (92287.54 per sec.)
    queries:                             55374780 (92287.54 per sec.)
    ignored errors:                      0      (0.00 per sec.)
    reconnects:                          0      (0.00 per sec.)
 
  Throughput:
    events/s (eps):                      92287.5373
    time elapsed:                        600.0245s
    total number of events:              55374780
 
  Latency (ms):
         min:                                    0.44
         avg:                                    2.77
         max:                                  144.99
         95th percentile:                        6.21
         sum:                            153566955.61
 
  Threads fairness:
    events (avg/stddev):           216307.7344/12789.87
    execution time (avg/stddev):   599.8709/0.01
```
