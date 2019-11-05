// Copyright (c) 2019 Baidu, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
 
/**
 * @file baikal_client_service.h
 * @author liuhuicong(com@baidu.com)
 * @date 2015/11/09 11:27:51
 * @brief 
 *  
 **/

#ifndef  FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_SERVICE_H
#define  FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_SERVICE_H

#include "baikal_client_define.h"
#include <string>
#include <vector>
#include <map>
#ifdef BAIDU_INTERNAL
#include "Configure.h"
#endif

#include "global.h"
#include "baikal_client_logic_db.h"
#include "baikal_client_bns_connection_pool.h"

namespace baikal {
namespace client {
//MysqlService 类，对应每个物理库
class Service {

public:
    // @brief 构造函数
    Service(std::map<std::string, BnsInfo*>& bns_infos, 
            int no_permission_wait_s, bool faulty_exit, bool async);

    // @brief 析构函数
    ~Service();

#ifdef BAIDU_INTERNAL
    // @brief 初始化
    // @conf_unit: service 配置单元
    // @bns_infos: 已经初始化完成的从bns服务同步信息
    int init(comcfg::ConfigUnit& conf_unit);

    //后调用初始化接口，name_service_url可以配置为"bns://<bns-name>"或"list://0$10.0.0.1:3386,10.0.0.2:3386;1$..."
    int init2(const std::string& naming_service_url,
              const std::string& user,
              const std::string& passwd,
              const std::string& charset);

    // 只初始化bns健康检查线程, 参数同init2, 只支持bns://格式, 凤脉capturer专用
    int init_bns_healthcheck(const std::string& naming_service_url,
            const std::string& user,
            const std::string& passwd,
            const std::string& charset);

    int _syn_instance_info_by_bns_name(const std::string& bns_name);
    int _syn_instance_info_from_db_shard_conf(comcfg::ConfigUnit& conf_unit);
#else
    //后调用初始化接口，link_list 形式为："list://0$10.0.0.1:3386,10.0.0.2:3386;1$..."
    int init2(const std::string& link_url,
            const std::string& user, const std::string& passwd, const std::string& charset);
#endif
    struct TableSplitOption {
        std::string name;
        int sub_tables;
        std::string table_split_function;
    };

    struct LogicDbOption {
        std::string name;

        std::vector<TableSplitOption> table_splits;
    };

    struct DbShardOption {
        int id;
        int read_timeout;
        int write_timeout;
        int connect_timeout;
        std::string charset;
        std::string username;
        std::string password;
        std::string ip_list; // form like 127.0.0.1:1111,127.0.0.2:2222
    };

    struct Option {
        std::string service_name;
        int connection_num;
        std::string comment_format;
        bool hang_check = true;
        int max_connection_per_instance = 0;
        int select_algorithm = ROLLING;
        int read_timeout = DEFAULT_READ_TIMEOUT;
        int write_timeout = DEFAULT_WRITE_TIMEOUT;
        int connect_timeout = DEFAULT_CONNECT_TIMEOUT;
        std::string charset = DEFAULT_CHARSET;
        std::string username;
        std::string password;
        bool connect_all = false;
        std::string db_split_function;

        std::vector<DbShardOption> db_shards;

        std::vector<LogicDbOption> logic_dbs;
    };

    int init(const Option& options);

    int _syn_instance_info_from_db_shard_conf(const Option& option);

    // @breif 为用户提供一组简单整合的接口，实现取连接，执行sql语句，关闭连接的功能
    // 该方法与dal 1.0 版的query方法功能相似
    // @note 每次执行该方法会重新选取一个新连接
    // 若需要再同一个连接上执行一个sql 序列，不能使用该方法
    // @inputparm result 若为NULL, 则只执行sql不保存结果
    // @returnVal int 0 表示执行成功
    int query(uint32_t partition_key, const std::string& sql, ResultSet* result);

    //给定分片id，具体实例(ip_port，为空则随机/轮询调度)的接口
    //内部带重试，秒级超时设置（-1不超时）
    //暂时只有凤脉能用到
    int query_timeout(int shard_id, const std::string& ip_port,
            const std::string& sql, MYSQL_RES*& res, int second);

    //给定分片id，具体实例(ip_port，为空则随机/轮询调度)的接口,增加了db实例信息返回
    //内部带重试，秒级超时设置（-1不超时）
    //暂时只有凤脉能用到
    int query_timeout(int shard_id, const std::string& ip_port,
            const std::string& sql, MYSQL_RES*& res, int second, std::string& real_ip_port);

    // @brief 选连接
    // @SmartConnection 返回智能连接
    SmartConnection fetch_connection();

    // @brief 选连接
    // @partition_key: 分库分表id
    // @SmartConnection 返回智能连接
    SmartConnection fetch_connection(uint32_t partition_key);
    
    // @brief 选连接，先select db
    // @logic_db_name: 逻辑库名
    // @SmartConnection 返回智能连接
    SmartConnection fetch_connection(const std::string& logic_db_name);

    // @brief 选连接, 先select_db
    // @partition_key: 分库分表id
    // @SmartConnection 返回智能连接
    SmartConnection fetch_connection(const std::string& logic_db_name,
                                     uint32_t partition_key);
    //fengmai单独的fetch_connection逻辑，给分片号与ip_port，优先找该ip_port下的连接
    //当这个实例故障才选择其他实例的连接
    SmartConnection fetch_connection_by_shard(int shard_id, const std::string& ip_port);

    //凤脉capturer用于检测指定分片的指定ip端口是否有效
    //有效返回0, 否则返回-1
    int check_ip_by_shard(int shard_id, const std::string& ip_port);

    std::map<int, BnsConnectionPool*> get_id_bns_map() const;
    
    std::string get_name() const;
    
    std::string get_comment_format() const;

    bool is_inited() const {
        return _is_inited;
    }
    bool get_hang_check() const {
        return _hang_check;
    }

    // 实例置为故障后再次选连接的次数
    static const int fetch_conn_times = 3;
private:
    
    BnsConnectionPool* _get_pool_no_partition_key();
    BnsConnectionPool* _get_pool_partition_key(uint32_t partition_key);

    // @brief 从连接池中取出连接并执行选择数据库操作
    
    SmartConnection _fetch_and_select_db(BnsConnectionPool* pool,
                std::string logic_db_name,
                bool has_partition_key,
                uint32_t partition_key);
    
    // @brief 计算分库id, 选择对应的bns连接池
    // 调用该方法的前提是数据库进行分库，有分库公式
    int _get_shard_id(uint32_t partition_key, int* shard_id);
    
    // @brief 从配置文件加载bns信息
#ifdef BAIDU_INTERNAL
    int _load_bns_info(comcfg::ConfigUnit& conf_unit);

    // @brief 从配置文件加载逻辑库配置
    int _load_logic_db_info(comcfg::ConfigUnit& conf_unit);

    // @brief 从配置文件加载每个表的分表配置
    int _load_table_split_info(comcfg::ConfigUnit& conf_unit, int i, LogicDB* logic_db);
#else
    int _load_bns_info(const std::vector<DbShardOption>& options);

    int _load_logic_db_info(const std::vector<LogicDbOption>& options);

    int _load_table_split_info(const std::vector<TableSplitOption>& options, LogicDB* logic_db);
#endif
    
    // @brief 释放所占内存资源
    void _clear();
private:
    bool _hang_check;
    std::string _name;
    ConnectionConf _conn_conf;
    int _conn_num_per_bns;
    int _max_conn_per_instance;

    // 注释格式
    std::string _comment_format;
    // 分库公式
    std::vector<std::string> _db_split_function;
    // bns的id和map对应关系
    std::map<int, BnsConnectionPool*> _id_bns_map;
    boost::mutex _pool_map_lock;
    
    // 计算分表id, string为逻辑库名
    std::map<std::string, LogicDB*> _name_logic_db_map; 

    // 选择实例的算法，随机还是轮询 
    SelectAlgo _select_algo;
    //manager里的引用
    std::map<std::string, BnsInfo*>& _bns_infos; 
    
    bool _connect_all;
    bool _is_inited;
    int _no_permission_wait_s;
    bool _faulty_exit;
    bool _async;
    Service(const Service&);
    Service& operator=(const Service&);
};
}
}

#endif  //FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_SERVICE_H

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
