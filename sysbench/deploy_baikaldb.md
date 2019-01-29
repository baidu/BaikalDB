## 安装baikalDB环境
* 编译baikalDB+baikalMeta+baikalStore bin文件
* 创建文件目录
  * bin
  * conf
  * log
* 更新baikalDB+baikalMeta+baikalStore配置
  * baikalMeta conf更新
    * -meta_server_bns=metaIP:metaport(部署机器IP:空闲端口)
    * -db_path=./rocks_db(默认在部署目录同级目录，可根据需要改变目录)
    * -meta_raft_peers=metaIP:metaport(部署机器IP:空闲端口，部署一个实例时与meta_server_bns一致即可)
    * -meta_port=metaport（meta的端口）
    * 其余参数可根据需要更新
  * baikalStore conf更新
    * -db_path=db_path(指定一个db文件存放地址)
    * -stable_uri=local://stable_uri(指定stable_uri地址)
    * -snapshot_uri=local://snapshot_uri(指定snapshot_uri地址)
    * -save_applied_index_path=save_applied_index_path/save_applied_index(指定save_applied_index_path地址)
    * -quit_gracefully_file=quit_gracefully_file/gracefully_quit.txt(指定quit_gracefully_file地址)
    * -meta_server_bns=metaIP:metaport
    * -store_port=storeport
    * 其余参数可根据需要更新
  * baikaldb conf更新
    * -meta_server_bns=metaIP:metaport
    * -baikal_port=baikalport
    * 其余参数可根据需要更新

## 启动baikalDB环境
* 启动baikalMeta
* 启动三个baikalStore
* 创建数据库、表
  * 执行sh init.sh创建database与机房过滤等,$1为metaIP:metaport
  * 执行sh create_internal_table.sh 创建内部表,$1为metaIP:metaport
* 启动baikaldb

## 测试baikalDB环境
* 找到一个安装mysql环境的机器，mysql -hbaikaldbIP -pbaikaldbport -uroot -proot进行测试连接
* baikaldb的使用方式与mysql完全一致

