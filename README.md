[中文版](README_cn.md)

[![Build Status](https://travis-ci.org/baidu/BaikalDB.svg?branch=master)](https://travis-ci.org/baidu/BaikalDB)

# Introduction BaikalDB
BaikalDB is a distributed enhanced structured database system.
It supports sequential and randomised realtime read/write of structural data in petabytes-scale.
BaikalDB is compatible with MySQL protocol and it supports MySQL style SQL dialect, by which users can migrate their data storage from MySQL to BaikalDB seamlessly.

BaikalDB internally provides projections, filter operators (corresponding with SQL WHERE or HAVING clause), aggregation operators (corresponding with GROPY BY clause) and sort operators (corresponding with SQL ORDER BY), with which users can fulfill their complex and time-critical anayltical and transational requirement by writing SQL statements. In a typical scenario, hundreds of millions of rows can be scanned and aggregated in few seconds.
BaikalDB also supports full-text search by building inverted indices after words segmentation. 
Users can harness fuzzy search feature simply by adding a `FULLTEXT KEY` type index when creating tables and then use LIKE clause in their queries.

BaikalDB uses share-nothing architecture. All three components (BaikalDB/BaikalMeta/BaikalStore) can be deployed in containers (like docker or kubernetes) with ease.
BaikalDB features multi-tenant isolation by:
* binding each user with a specific NAMESPACE; user can only access databases and tables within this NAMESPACE.
* associating each table to a resource tag; table regions will be allocated only on baikalStore instances with this resource tag.
* defining user's privileges (read-only or read-write) to access databases and tables.

Below is a summary of BaikalDB key features:
* Multi-Replica High Avalibility. All user data and meta data are stored in three replicas across diffrent machines or diffrent geo-zones. In case of node corruption, data on the corrupted node will be migrated to healthy nodes. 
* Strong consistency. Multi-Raft consensus algorithm (we use the [braft](https://github.com/brpc/braft) implementation) is used to ensure multi-replica data consistency.
* State-Free Horizontal scalability. For more storage space of a BaikalDB cluster, new nodes can be added to the cluster with ease. Load Balancer will then take the new added nodes into consideration and migrate region replicas from busy nodes to idle nodes automatically.
* MySQL Compatibility. BaikalDB supports MySQL client-server protocol and a subset of MySQL dialect of query language, including Select, Select...Join, Insert, Delete, Replace, Update, Create Table, Create Database, etc. It also supports dynamic adding or deleting fields for a table without refreshing the table data.
Users can migrate their data from MySQL to BaikalDB with very low cost, while saving the cost of manually sharding the tables when data grows larger.
* Ease of Deployment. BaikalDB has a list of sophisticated startup options (using gflags), but most of them can keep default for nomal usage. Deploying a BaikalDB cluster contains a few very simple steps. We deploy BaikalDB in hundreds of containers inside Baidu private cloud.
* Multi-Tenant Data Isolation. For typical usage, multiple BaikalStore clusters with different resouce tags can be deployed to share a single BaikalMeta and BaikalDB cluster, and users can only access databases and tables with specific resource tags.

![BaikalDB Architecture](docs/resources/baikaldb_arch.png)

Above is the BaikalDB architecture figure, in which,
* BaikalStore stores the table data. Data within a table may be partitioned into multiple regions, and each region is a Raft group, which has three peers located in different nodes. A peer can be migrated from one nodes to another for better load balance or data recovery after node crash.
* BaikalMeta stores the meta data for baikalStore, including table schema, region info, user info, database info, baikalStore instance status, etc. BaikalMeta itself is a raft group for meta data safety and consistency. Typically three BaikalMeta instances (replicas) is required for online product usage.
* BaikalDB is responsible for query parsing, planning and executing. BaikalDB is stateless and we can deploy as many BaikalDB instance as required by the scale of query workloads. 

## How To Compile
We use [Bazel](https://www.bazel.build) to resolve dependencies and build BaikalDB automatically.
The build has been successful on Ubuntu 16.04 and CentOS 7. More platforms will be supported soon.

### Ubuntu 16.04
* [Install bazel](https://docs.bazel.build/versions/master/install-ubuntu.html).
* Install flex, bison and openssl library: 
  sudo apt-get install flex bison libssl-dev
* Install g++ (v4.8.2 or later): 
  sudo apt-get install g++
* bazel build //:all

### CentOS 7
* [Install bazel](https://docs.bazel.build/versions/master/install-redhat.html).
* sudo yum install flex bison patch openssl-devel
* sudo yum install gcc-c++ (v4.8.2 or later)
* bazel build //:all

You may brew and enjoy a cup of coffee waiting for the tedious building process

## How To Deploy
### Preface
* BaikalDB has three components, namely baikalMeta, baikalStore and baikalDB.
* The directory structure of each component is similar with that of most C++ programs:
  * baikal**/bin  // bin directory is where the binary program resides
  * baikal**/conf // conf directory is where the configuration file gflags.conf resides
* It is compulsory to first deploy baikalMeta, then baikalStore and finally baikalDB
* If multiple replica for table data is required, at least three baikalStore instances should be deployed.

### baikalMeta Deployment
* copy the baikalMeta binary to ./bin 
* add configuration file（conf/gflags.conf), below is a simple configuration:
  * -meta_replica_number=1 // peer(replica) number, 1 is okay for demo, 3 is recommended for product.
  * -meta_port=8010        // instance port
  * -meta_server_bns=xx.xx.xx.xx:8010,xx.xx.xx.xx:8010,xx.xx.xx.xx:8010 // The IP+Port list of all instances
* run baikalMeta instance: nohup bin/baikalMeta &
* check the leader address: open http://xx.xx.xx.xx:8010/raft_stat on a web browser and get the leader address of "meta_raft"
* run initialization scripts（all scripts are in src/tools/script):
  * sh init_meta_server.sh leader_ip:port
  * sh create_namespace.sh leader_ip:port // create namespace
  * sh create_database.sh leader_ip:port  // create database
  * sh create_user.sh leader_ip:port      // create user

### baikalStore Deployment
* copy the baikalStore binary to ./bin 
* add configuration file（conf/gflags.conf), below is a simple configuration:
  * -db_path=./rocks_db
  * -election_timeout_ms=10000
  * -raft_max_election_delay_ms=5000
  * -raft_election_heartbeat_factor=3
  * -snapshot_uri=local://./raft_data/snapshot
  * -meta_server_bns=xx.xx.xx.xx:8010,xx.xx.xx.xx:8010,xx.xx.xx.xx:8010 // list of baikalMeta instance ip+port
  * -store_port=8110
* run baikalStore instance: nohup bin/baikalStore &

### baikaldb Deployment
* copy the baikaldb binary to ./bin 
* add configuration file（conf/gflags.conf), below is a simple configuration:
  * -meta_server_bns=xx.xx.xx.xx:8010,xx.xx.xx.xx:8010,xx.xx.xx.xx:8010 // list of baikalMeta instance ip+port
  * -baikal_port=28282
  * -fetch_instance_id=true
* run script to create baikaldb instance meta-table:
  sh create_internal_table.sh meta_leader_ip:port
* run baikaldb instance: nohup bin/baikaldb &

### Create Table
```
CREATE TABLE `TestDB`.`test_table` (
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

After CREATE TABLE returns successfully, the synchronization of table schema from baikalMeta to baikaldb/baikalStore requires 10-30 seconds, during which SQL command like `show tables` and `desc table` may fail.

### How To Upgrade BaikalStore to the space-efficient-snapshot version
If your cluster is deployed with the binary built from the code submitted before Dec. 25 2018, you are required to follow the below instructions to upgrade the BaikalStore binary to the latest version.
1. Upgrade all BaikalStore instances to the latest no_snapshot_compatible branch. This is an intermediate branch, on which all add_peer operations will fail.
2. Create new snapshot manually for all regions.
  *  How to create snapshot? Execute `sh src/tools/script/store_snapshot_region.sh ip:port` for all BaikalStore instances.
  *  How to verify? Check the snapshot directory of each region. Each directory should only contain the file with _raft_snapshot_meta suffix, and no snap_region_***.extra.json file.
3. Upgrade all BaikalStore instances to the latest master branch.

## Documents
* [User Manual](docs/cn/user_guide.md)

## How To Contributes
You are welcome to constribute new features and bugfix codes. Before code submission please ensure:
* Good coding style
* Reasonable features 
* Docs and test codes are included.

## Q&A and discussion
* You can contact the team members by email or launch an issue for discussion.

## Acknowledgements
* We are especially grateful to the teams of RocksDB, brpc and braft, who built powerful and stable libraries to support important features of BaikalDB.
* We give special thanks to TiDB team and Impala team. We referred their design schemes when designing and developing BaikalDB.
* Last but not least, we give special thanks to the authors of all libraries that BaikalDB depends on, without whom BaikalDB could not have been developed and built so easily.
* Thanks our friend team -- The Baidu TafDB team, who provide the space efficient snapshot scheme based on braft.
