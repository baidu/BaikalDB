# Canal-python Client for BaikalDB
## 一、概述

**Canal**是一款阿里巴巴开源的基于mysql数据库binlog的增量订阅&消费组件。canal 特别设计了 client-server 模式，交互协议使用 protobuf 3.0 , client 端可采用不同语言实现不同的消费逻辑，本项目forks canal官网的python客户端：[Canal-python client](https://github.com/alibaba/canal/wiki) ， 并添加了对 [**BaikalDB**](https://github.com/baidu/BaikalDB) 的支持。

有关**Canal** 和 **Canal-python** 更多的信息请访问：

- **Canal**： https://github.com/alibaba/canal/wiki
- **Canal-python** ：https://github.com/haozi3156666/canal-python



## 二、工作流程

1. Canal连接到mysql数据库，模拟slave
2. Canal-python 与Canal 建立连接
3. 数据库发生变更写入到binlog
4. Canal向数据库发送dump请求，获取binlog并解析
5. Canal-python 向 Canal 请求数据库变更
6. Canal 发送解析后的数据给Canal-python
7. Canal-python收到数据，同步给BaikalDB，发送回执。
8. Canal记录消费位置。



## 三、快速启动

### 安装部署Canal Server

Canal 的安装以及配置使用请查看 https://github.com/alibaba/canal/wiki/QuickStart

### 安装部署Canal client

#### 环境要求

jdk < 9 
python <= 2

#### 下载

```git clone https://github.com/hualiyang/canal-python.git```

#### 修改配置
##### 建立与数据源canal server的连接

```config
cd canal-client/
vim conf/sender.cnf

#配置canal server 端的ip和port
[server_info]
#部署canal server的ip和port
host = xx.XX.XX.XX
port = 11111


#配置同步的任务
[subscribe_instance]
client_id = 1001 #一个instance一个client_id
#instance_destination当前server上部署的instance列表，这里用的是example,要与canal/conf目录下创建的instance配置文件夹一致
instance_destination = example 
#这里同步的是DSTtest库的user表
filter = DSTtest.user #DSTtest库下的所有表DSTtest..*


#配置mysql用于同步的账户、密码
[syn_user_auth_info]
username=test
password=********
```

##### 建立与数据目的地BaikalDB的连接

```
vim conf/sender.cnf

#配置目标库信息
host=xx.xx.xx.xx
user=test
passwd=********
db=DSTtest
port=28282
charset=utf8
```

#### 启动

```shell
python src/sender.py conf/sender.cnf
```

运行 sender.py 的日志存放在 canal-client/log 目录下。

