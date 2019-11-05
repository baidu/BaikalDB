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
 * @file baikal_client_connection.cpp
 * @author liuhuicong(com@baidu.com)
 * @date 2015/11/09 17:02:46
 * @brief 
 *  
 **/

#include "baikal_client_connection.h"
#include <string.h>
#include <stdio.h>
#include <algorithm>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#ifdef BAIDU_INTERNAL
#include "com_log.h"
#endif
#include "errmsg.h"
#include "baikal_client_define.h"
#include "baikal_client_util.h"
#include "baikal_client_instance.h"
#include "baikal_client_bns_connection_pool.h"

using std::string;
using std::vector;

namespace baikal {
namespace client {    
ScopeProcWeight::ScopeProcWeight(Connection* connection) {
    _connection = connection;
    _connection->_begin_time_us = butil::gettimeofday_us();
    _connection->get_instance()->add_inflight(connection->_begin_time_us);
    _connection->_begin_execute = true;
}
ScopeProcWeight::~ScopeProcWeight() {
    Instance* instance = _connection->get_instance();
    _connection->_begin_execute = false;
    instance->sub_inflight(_connection->_begin_time_us);
    int64_t diff = instance->update(_connection->_begin_time_us);
    instance->get_bns_pool()->update_parent_weight(diff, instance->get_index());
    instance->get_bns_pool()->total_fetch_add(diff); 
    instance->update_quartile_value(butil::gettimeofday_us() - _connection->_begin_time_us);
    //instance->print_weight();
    //_connection->_pool->print_instance_weight();
}
Connection::Connection(
        Instance* instance,
        BnsConnectionPool* pool, ConnectionType conn_type) :
        _has_partition_key(false),
        _partition_key(0),
        _has_logic_db(false),
        _async(false),
        _begin_time_us(0),
        _begin_execute(false),
        _instance(instance),
        _pool(pool),
        _conn_type(conn_type),
        _sql_handle(NULL),
        _status(IS_NOT_CONNECTED),
        _is_hang(false) {}

Connection::~Connection() {
    if (_sql_handle != NULL) {
        mysql_close(_sql_handle);
    }
    _sql_handle = NULL;
    _instance = NULL;
    _pool = NULL;
}

int Connection::execute(const string& /*sql*/, 
                        vector<string>& /*table_name_list*/,
                        bool /*store*/,
                        ResultSet* /*result*/) {
    CLIENT_FATAL("Execute base connection");
    return EXECUTE_FAIL;
}
 
int Connection::execute(const string& /*sql*/,
                        vector<string>& /*table_name_list*/,
                        ResultSet* /*result*/) {
    CLIENT_FATAL("Execute base connection");
    return EXECUTE_FAIL;
}

int Connection::execute(const string& /*sql*/, bool /*store*/, ResultSet* /*result*/) {
    CLIENT_FATAL("Execute base connection");
    return EXECUTE_FAIL;
}

int Connection::execute(const string& /*sql*/, ResultSet* /*result*/) {
    CLIENT_FATAL("Execute base connection");
    return EXECUTE_FAIL;
}

int Connection::execute_raw(const string& /*sql*/, bool /*store*/, MYSQL_RES*& /*result*/) {
    CLIENT_FATAL("Execute base connection");
    return EXECUTE_FAIL;
}

int Connection::execute_raw(const string& /*sql*/, MYSQL_RES*& /*result*/) {
    CLIENT_FATAL("Execute base connection");
    return EXECUTE_FAIL;
}

int Connection::begin_transaction() {
    CLIENT_FATAL("begin_transaction base connection");
    return EXECUTE_FAIL;
}

int Connection::commit() {
    CLIENT_FATAL("commit base connection");
    return EXECUTE_FAIL;
}

int Connection::rollback() {
    CLIENT_FATAL("rollback base connection");
    return EXECUTE_FAIL;
}
MYSQL* Connection::get_mysql_handle() {
    return _sql_handle;
}

void Connection::close() {
    //状态置为IS_NOT_USED
    if (compare_exchange_strong(IS_USING, IS_NOT_USED)) {  
        _has_partition_key = false;
        _partition_key = 0;
        _has_logic_db = false;
        _logic_db = "";
    } else {
        CLIENT_WARNING("The connection has been closed");
    }
}

bool Connection::compare_exchange_strong(ConnectionStatus expect,
                                                   ConnectionStatus desired) {
    ConnectionStatus& expected = expect;
    return _status.compare_exchange_strong(expected, desired);
}

bool Connection::get_has_partition_key() const {
    return _has_partition_key;
}

void Connection::set_has_partition_key(bool has_partition_key) {
    _has_partition_key = has_partition_key;
}

uint32_t Connection::get_partition_key() const {
    return _partition_key;
}

void Connection::set_partition_key(uint32_t partition_key) {
    _partition_key = partition_key;
}

bool Connection::get_has_logic_db() const {
    return _has_logic_db;
}
    
void Connection::set_has_logic_db(bool has_logic_db) {
    _has_logic_db = has_logic_db;
}

bool Connection::get_async() const {
    return _async;
}
    
void Connection::set_async(bool async) {
    _async = async;
}

string Connection::get_logic_db() const {
    return _logic_db;
}

void Connection::set_logic_db(string logic_db) {
    _logic_db = logic_db;
}

void Connection::set_instance(Instance* instance) {
    _instance = instance;
}

Instance* Connection::get_instance() const {
    return _instance;
}

void Connection::set_pool(BnsConnectionPool* pool) {
    _pool = pool;
}

BnsConnectionPool* Connection::get_pool() const {
    return _pool;
}

ConnectionStatus Connection::get_status() const {
    return _status.load();
}

std::string Connection::get_trace_time_os() const {
    return _trace_time_os.str();
}

std::string Connection::get_instance_info() {
    Instance* instance = _instance;
    if (instance != NULL) {
        char port[10];
        snprintf(port, sizeof(port), "%d", instance->get_port());
        return instance->get_ip() + ":" + port;
    }
//    CWARNING_LOG("connection has been deleted");
    return "";
}
void Connection::_instance_to_faulty() {
    Instance* instance = _instance;
    if (instance != NULL) {
        instance->status_online_to_faulty();
        CLIENT_DEBUG("instance:%s:%d status online to faulty in sql execute process",
                     _instance->get_ip().c_str(), _instance->get_port());
    } else {
        CLIENT_WARNING("connection has been deleted");
    } 
}

} //namespace client
} //namespace baikal

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
