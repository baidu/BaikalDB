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
 * @file baikal_client_connection.h
 * @author liuhuicong(com@baidu.com)
 * @date 2015/11/09 16:44:12
 * @brief 
 *  
 **/

#ifndef  FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_CONNECTION_H
#define  FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_CONNECTION_H

#include "mysql.h"
#include <string>
#include <sstream>
#include <vector>
#include "boost/atomic.hpp"
#include "baikal_client_result_set.h"

namespace baikal {
namespace client {
class Instance;
class BnsConnectionPool;
class ScopeProcWeight {
public:
    ScopeProcWeight(Connection* connection);
    ~ScopeProcWeight();
private:
    Connection* _connection;
};
class Connection {
public:
    friend class ScopeProcWeight;
    // @brief 构造函数
    // instance: 指向连接所属的实例
    // pool: 指向连接所属的连接池
    Connection(Instance* instance, BnsConnectionPool* pool, ConnectionType conn_type);

    virtual ~Connection();
   
    // @brief 连接数据库
    virtual int connect(ConnectionConf& conn_conf, bool change_status) = 0;
    
    // @brief 断开数据库，重连
    virtual int reconnect(ConnectionConf& conn_conf, bool change_status) = 0;

    //
    virtual void kill() {
        _is_hang.store(true);
    } 
    // @brief 在connection上执行sql语句，支持分表
    // vector<string> table_list : sql语句中的表名，由用户提供
    // table_name: 有两种格式
    //             1、db_name.table_name, 程序会分别读出db_name和table_name,
    //             然后根据该db下的分表逻辑去对table_name进行分表改写
    //             2、table_name，程序会认为该table_name所属的db_name是fetch_connection时
    //              传入的table_name
    virtual int execute(const std::string& sql,
                        std::vector<std::string>& table_name_list,
                        ResultSet* result);

    // @brief 在connection上执行sql语句，支持分表
    // vector<string> table_list : sql语句中的表名，由用户提供
    // 是否保存结果由store决定
    virtual int execute(const std::string& sql,
                        std::vector<std::string>& vector_list,
                        bool store,
                        ResultSet* result);

    // @brief 在connection上执行sql语句，保存存储结果
    // 从sql语句中解析表名 
    virtual int execute(const std::string& sql, ResultSet* result);

    // @brief 在connection上执行sql语句
    // 是否保存结果由store决定
    // 从sql语句中解析表名
    virtual int execute(const std::string& sql, bool store, ResultSet* result);

    // @brief 在connection上执行sql语句，直接返回MYSQL_RES,需要用户释放资源
    virtual int execute_raw(const std::string& sql, bool store, MYSQL_RES*& result);
    virtual int execute_raw(const std::string& sql, MYSQL_RES*& result);
    
    // @brief 在connection上执行start transition操作
    virtual int begin_transaction();
    
    // @brief 在connection上执行commit操作
    virtual int commit();

    // @brief 在connection上执行rollback操作
    virtual int rollback();

    // @brief 得到mysql句柄, 仅供mysql类型连接使用
    MYSQL* get_mysql_handle();

    bool get_is_hang() {
        return _is_hang.load();
    }
    ConnectionType get_conn_type() {
        return _conn_type;
    }
    // @brief 供客户端关闭连接使用
    // 不会真正释放连接,只更改连接的状态
    virtual void close();

    // @breif 对连接做ping()操作
    virtual int ping() = 0;

    virtual void set_read_timeout(const int32_t& read_timeout) = 0;

    // @brief 返回_sqlhandle上的错误码
    virtual int get_error_code(int* error_code) = 0;
    
    // @breif 返回_sqlhandle上的错误描述
    virtual std::string get_error_des() = 0;

    virtual int reset() = 0; 
    bool compare_exchange_strong(ConnectionStatus expect, ConnectionStatus desired);

    bool get_has_partition_key() const;

    void set_has_partition_key(bool has_partition_key);

    uint32_t get_partition_key() const;

    void set_partition_key(uint32_t partition_key);

    bool get_has_logic_db() const;
    
    void set_async(bool has_logic_key);

    bool get_async() const;
    
    void set_has_logic_db(bool has_logic_key);
    
    std::string get_logic_db() const;
    
    void set_logic_db(std::string logic_db);

    void set_instance(Instance* instance);
    
    Instance* get_instance() const;

    void set_pool(BnsConnectionPool* pool);
    
    BnsConnectionPool* get_pool() const;

    ConnectionStatus get_status() const;
    std::string get_trace_time_os() const;
    std::string get_instance_info();

    int64_t get_begin_time_us() const {
        return _begin_time_us;
    }
    bool get_begin_execute() const {
        return _begin_execute;
    }
    int get_tmp_port() const {
        return _tmp_port;
    }
    
protected:

    // 在连接的建立查询过程中发生实例故障
    void _instance_to_faulty();

protected:

    //客户端fetch_connection时是否传入partition_key
    bool _has_partition_key;
    uint32_t _partition_key;
    //客户端在fetch_connection时是否传入logic_db
    bool _has_logic_db;
    bool _async;
    int _tmp_port;
    std::string _logic_db;
    std::ostringstream _trace_time_os;

    int64_t _begin_time_us;
    bool _begin_execute;
    
    Instance* _instance;
    BnsConnectionPool* _pool;

    ConnectionType _conn_type;
    MYSQL* _sql_handle;

    boost::atomic<ConnectionStatus> _status;

    boost::atomic<bool> _is_hang;
    Connection(const Connection&);
    Connection& operator=(const Connection&);
};

//封装mysql connection类
typedef boost::shared_ptr<Connection> SmartConnection;
}
}

#endif  //FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_CONNECTION_H

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
