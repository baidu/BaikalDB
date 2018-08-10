[English Version](README.md)

# BaikalDB
BaikalDB是一个分布式可扩展的存储系统，支持PB级结构化数据的随机实时读写。
提供MySQL接口，支持常用的SELECT，UPDATE，INSERT，DELETE语法。

提供各种WHERE过滤、GROUP BY聚合，HAVING过滤，ORDER BY排序等功能，用户可以组合实现各种在线OLAP需求，具备秒级别的亿级数据扫描聚合能力。

另外，为了满足各种业务的检索需求，该系统内置全文检索需求，满足大部分快速检索的业务场景。

在虚拟化部署方面，该系统采用share-nothing的架构，可部署在容器中，也实现了多租户隔离，有自定义用户的身份识别和权限访问控制等功能。

BaikalDB 的主要特性如下：

全自主化的容量管理，可以自动扩容和自动数据均衡，支持自动故障迁移，无单点，很容易实现云化，目前运行在Paas虚拟化平台之上。
面向查询优化，支持各种二级索引，包括全文索引，支持常用的 OLAP 需求，支持层级模型。
兼容 mysql 协议，对应用方提供 SQL 界面，支持高性能的Schema 加列。
基于 RocksDB 实现单机存储，基于Multi Raft 协议（我们使用[braft库](https://github.com/brpc/braft)）保障副本数据一致性，基于brpc实现节点通讯交互。
支持多租户，meta 信息共享，数据存储完全隔离。

![BaikalDB架构简图](docs/resources/baikaldb_arch.png)

其中
* BaikalStore 负责数据存储，用 region 组织，三个 Store 的 三个region形成一个 Raft group 实现三副本，多实例部署，Store实例宕机可以自动迁移 Region数据。
* BaikalMeta 负责元信息管理，包括分区，容量，权限，均衡等， Raft 保障的3副本部署，Meta 宕机只影响数据无法扩容迁移，不影响数据读写。
* BaikaDB 负责前端SQL解析，查询计划生成执行，无状态全同构多实例部署，宕机实例数不超过 qps 承载极限即可。

## 编译步骤
外部编译使用[bazel构建工具](https://www.bazel.build)
### Ubuntu 16.04
* [安装bazel](https://docs.bazel.build/versions/master/install-ubuntu.html)。
* 安装flex, bison, openssl库：
  sudo apt-get install flex bison libssl-dev
* 安装g++ (版本不低于4.8.2)：
  sudo apt-get install g++
* bazel build //:all

### CentOS 7
* [安装bazel](https://docs.bazel.build/versions/master/install-redhat.html)。
* sudo yum install flex bison patch openssl-devel
* sudo yum install gcc-c++ (版本不低于4.8.2)
* bazel build //:all

## 部署方式
### 前言
* BaikalDB 有三个模块组成，分别为BaikalMeta, BaikalStore, BaikalDB
* 每个模块的目录结构与通用的C++程序一致：
  * Baikal**/bin  // ./bin目录下存放编译好的bin文件
  * Baikal**/conf // conf目录下存放配置文件:gflags.conf
* 必须先部署BaikalMeta模块，再部署BaikalStore, 最后部署BaikalDB
* 只有部署了BaikalStore之后才能进行建表、灌数据等操作
* 如果先启动三个副本的table，baikalStore模块至少部署三个以上

### 部署BaikalMeta
* 执行编译构建并拉取BaikalMeta可执行文件。
* 添加配置文件（conf/gflags.conf）
  * -meta_replica_number=1 //meta实例（副本）数量，线上推荐3，测试可用1
  * -meta_port=8010
  * -meta_server_bns=xx.xx.xx.xx:8010,xx.xx.xx.xx:8010,xx.xx.xx.xx:8010 //所有实例的IP+端口
  * -meta_raft_peers=xx.xx.xx.xx:8010,xx.xx.xx.xx:8010,xx.xx.xx.xx:8010 //所有实例的IP+端口
* 添加配置文件（conf/comlog.conf), 配置内容参考icode: fcdata-baikaldb/conf/comlog.conf
* 启动实例 nohup bin/baikalMeta &
* 找到leader地址，通过http://xx.xx.xx.xx:8010/raft_stat 查看当前leader是哪个实例；或者是第6步的脚本发送到任何一台机器，若该机器不是leader会返回leader地址
* 执行初始化scripts（scripts目录为src/tools/script)，每个scipt的参数均为leader_ip:port
  * sh init_meta_server.sh leader_ip:port
  * sh create_namespace.sh leader_ip:port //创建namespace
  * sh create_database.sh leader_ip:port //创建database
  * sh create_user.sh leader_ip:port //创建用户

### 部署BaikalStore
* 执行编译构建并拉取BaikalStore可执行文件。
* 添加配置文件(conf/gflags.conf):
  * -db_path=./rocks_db
  * -election_timeout_ms=10000
  * -raft_max_election_delay_ms=5000
  * -raft_election_heartbeat_factor=3
  * -snapshot_uri=local://./raft_data/snapshot
  * -meta_server_bns=xx.xx.xx.xx:8010,xx.xx.xx.xx:8010,xx.xx.xx.xx:8010
  * -store_port=8110
* 启动baikalStore: nohup bin/baikalStore &

### 部署BaikalDB
* 执行编译构建并拉取BaikalDB可执行文件。
* 添加配置文件（conf/gflags.conf）
  * -meta_server_bns=xx.xx.xx.xx:8010,xx.xx.xx.xx:8010,xx.xx.xx.xx:8010
  * -baikal_port=28282
  * -fetch_instance_id=true
* 发送请求给BaikalMeta，创建BaikalDB实例表
  sh create_internal_table.sh meta_leader_ip:port
* 启动BaikalDB: nohup bin/baikaldb &

### Create Table
```
CREATE TABLE `test_table` (
  `userid` int(11) NOT NULL,
  `username` varchar(50) NOT NULL,
  `birthdate` DATE NOT NULL,
  `register_time` DATETIME NOT NULL,
  `user_desc` TEXT NOT NULL,
  PRIMARY KEY (`userid`),
  KEY `name_key` (`username`),
  FULLTEXT KEY `desc_key` (`user_desc`)
) ENGINE=Rocksdb DEFAULT CHARSET=gbk AVG_ROW_LENGTH=100 COMMENT='{"comment":"", "resource_tag":"", "namespace":"TEST_NAMESPACE"}';
```
建表语句中，列定义中暂不支持注释。建表成功后，表信息从BaikalMeta同步到BaikalDB需要10-30s的间隔。同步完成后才能通过show tables和desc table查到表信息。

## 文档
* [用户手册](docs/cn/user_guide.md)

## 如何贡献
提交issue前请确认你的代码符合如下要求：
* 符合C++代码规范和项目代码风格
* 功能合理。
* 有对应的测试代码和步骤。

## 咨询和讨论
* 请邮件联系项目成员或直接在github上发起讨论issue。
