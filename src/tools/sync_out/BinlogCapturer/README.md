## BinlogCapturer

### 一、概述

BinlogCapturer用于收集BaikalDB的binlog，并按事务提交顺序排序，全局有序的将数据同步给下游，支持多种输出方式：MySQL、Kafka及本地文件。

BaikalDB开启binlog的普通表会LINK至一个binlog元数据表(m:1)，每个binlog元数据表对应一个BinlogCapturer实例(1:1)，每个BinlogCapturer实例对应一个Sender，Sender可以并发的发送至下游(1:n)。

经测试，单实例16线程同步至MySQL QPS 4K+，同步至kafka QPS 2k+。如需增加整体吞吐，只需要增加binlog元数据表及对应的BinlogCapturer实例即可。

若单表产生的binlog数据量过大，需要开启binlog元数据表的分区功能，Capturer模块需要对分区实现Merge Sort逻辑(暂未实现)。

### 二、工作流程

1. Capturer 从各个 store 中获取 binlog，归并后按照顺序解析 binlog并放入queue中；
2. SenderManager 从queue获取binlog，检测冲突并构造SQL放入sqlQueue中；
3. mysqlSender/kafkaSender/localSender从sqlQueue中获取SQL并同步至下游。

### 三、快速启动

#### 1. 安装依赖

```
pip install protobuf
pip install pymysql
pip install kafka-python
```

#### 2. 普通表开启binlog功能

```
$ cat update_binlog.sh
#!/bin/sh
#Created on 2020-6-22
curl -d '{
    "op_type":"OP_LINK_BINLOG",
    "table_info": {
        "table_name": "nation",
        "database": "TestDB",
        "namespace_name": "TEST",
        "link_field": {
            "field_name": "nkey"
        }
    },
    "binlog_info": {
        "table_name": "blog",
        "database": "TestDB",
        "namespace_name": "TEST"
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"
 
$ bash update_binlog.sh 127.0.0.1:8010
{"errcode":"SUCCESS","errmsg":"success","op_type":"OP_LINK_BINLOG"}
```

#### 3. 修改源端配置

```
$ vim conf/config.cfg
[global]
# 配置BaikalDB集群metaServer
metaServer=xx.xx.xx.xx:8010,xx.xx.xx.xx:8010,xx.xx.xx.xx:8010

# 还原点保存路径
binlogCheckPointPath=./conf/binlog_checkpoint.txt
# binlog表TableID
binlogTableID=xx
# binlog表RegionID
binlogRegionID=xx

# 目标端配置文件路径
senderConfig=./conf/senderConfig.cfg
# 过滤规则路径
filterRuleDict=./conf/filter_rule.txt

# HTTP服务端口号
httpPort=8080
```

#### 4. 修改目标端配置

```
$ vim senderConfig.cfg
[global]
# 修改下游目标，可以同时向一到多个目标端同步
type=mysql,kafka,local
[mysql]
# MySQL主机信息等
host=xx.xx.xx.xx
port=3306
user=test
password=test
database=test
# 同步MySQL线程数
threadnum = 16
[kafka]
# kafka信息
servers=xx.xx.xx.xx:xxxx
topic=test

[local]
# 本地binlog保存路径
filepath=./data/test.sql
```

#### 5. 启动

```
sh start.sh

tips: 
每次启动时会从conf/binlog_checkpoint.txt获取还原点，还原点默认为0；
工具运行中每3s更新还原点，记录binlog消费的时间戳
```

#### 6. TODO

目前没有实现分区binlog表的收集时的Merge Sort逻辑。

