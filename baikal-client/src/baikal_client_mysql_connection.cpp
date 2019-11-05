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
#include <sys/epoll.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <algorithm>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#ifdef BAIDU_INTERNAL
#include <base/time.h>
#include "com_log.h"
#else
#include "butil/time.h"
#endif
#include "errmsg.h"
#include "baikal_client_define.h"
#include "baikal_client_util.h"
#include "baikal_client_instance.h"
#include "baikal_client_bns_connection_pool.h"
#include "baikal_client_mysql_async.h"

using std::string;
using std::vector;

namespace baikal {
namespace client {

MysqlConnection::MysqlConnection(
        Instance* instance,
        BnsConnectionPool* pool) :
        Connection(instance, pool, MYSQL_CONN) {}

int MysqlConnection::connect(ConnectionConf& conn_conf, bool change_status) {
    if (_sql_handle != NULL) {
        CLIENT_WARNING("sql handle is not NULL");
        return CONNECTION_CONNECT_FAIL;
    }
    _async = conn_conf.async;
    int retry = 0;
    MYSQL* mysql = NULL;
    do {
        _sql_handle = mysql_init(NULL);
        if (_sql_handle == NULL) {
            CLIENT_WARNING("mysql init fail");
            return CONNECTION_CONNECT_FAIL;
        }
        mysql_options(_sql_handle, MYSQL_OPT_NONBLOCK, 0);
        mysql_options(_sql_handle, MYSQL_OPT_CONNECT_TIMEOUT, (void *)&(conn_conf.connect_timeout));
        mysql_options(_sql_handle, MYSQL_OPT_READ_TIMEOUT, (void *)&(conn_conf.read_timeout));
        mysql_options(_sql_handle, MYSQL_OPT_WRITE_TIMEOUT, (void *)&(conn_conf.write_timeout));
        mysql = mysql_async_real_connect(
                this,
                _instance->get_ip().c_str(),
                conn_conf.username.c_str(),
                conn_conf.password.c_str(),
                NULL,
                _instance->get_port(),
                NULL,
                0);
        if (mysql == NULL) {
            int error_code = 0;
            get_error_code(&error_code);
            CLIENT_WARNING("connect fail, ip:%s, port:%d, error_code:%d error_des:%s retry:%d",
                    _instance->get_ip().c_str(), _instance->get_port(),
                    error_code, get_error_des().c_str(), retry);
            mysql_close(_sql_handle);
            _sql_handle = NULL;
            if (error_code != 1045) {
                break;
            }
        } else {
            // success
            break;
        }
        sleep(1);
    } while (retry++ < conn_conf.no_permission_wait_s);
    if (mysql == NULL) {
        return INSTANCE_FAULTY_ERROR;
    }

    if (0 != mysql_async_set_character_set(this, conn_conf.charset.c_str())) {
        CLIENT_WARNING("failed set charset to %s, error_no:%d, error:%s, ip:%s, port:%d",
                     conn_conf.charset.c_str(), mysql_errno(_sql_handle),
                     mysql_error(_sql_handle),  _instance->get_ip().c_str(), _instance->get_port());
        mysql_close(_sql_handle);
        _sql_handle = NULL;
        return CONNECTION_CONNECT_FAIL;
    }
    int fd = mysql_get_socket(_sql_handle);
    struct sockaddr_in fd_name;
    uint32_t namelen = sizeof(fd_name);
    getsockname(fd, (sockaddr*)&fd_name, &namelen);
    _tmp_port = ntohs(fd_name.sin_port);
    if (change_status) {
        _status.store(IS_NOT_USED);
    }
    return SUCCESS;
}

int MysqlConnection::reconnect(ConnectionConf& conf, bool change_status) {
    if (_sql_handle != NULL) {
        mysql_close(_sql_handle);
    }
    _sql_handle = NULL;
    return connect(conf, change_status);
}

int MysqlConnection::reset() {
    return reconnect(_instance->get_conn_conf(), false);
}

int MysqlConnection::execute(const string& sql,
                        vector<string>& table_name_list,
                        bool store,
                        ResultSet* result) {
    TimeCost time_cost;
    if (sql.empty()) {
        CLIENT_WARNING("Input sql is empty");
        return INPUTPARAM_ERROR;
    }
    BnsConnectionPool* pool = _pool;
    if (pool == NULL) {
        CLIENT_WARNING("the connection has been deleted, please fetch connection again ");
        return CONNECTION_ALREADY_DELETED;
    }
    string sql_rewrite;
    string comment_format;

    // 增加注释
    int ret = _add_comment(pool, comment_format);
    if (ret < 0) {
        CLIENT_WARNING("add comment fail");
        return ret;
    }
    sql_rewrite = comment_format + " " + sql;
    // 有partition_key则可能进行分表，若没有，直接查询
    if (_has_partition_key && pool->is_split_table()) {
        ret = _split_table(table_name_list, sql_rewrite);
        if (ret < 0) {
            CLIENT_WARNING("split table fail, sql statement:%s", sql_rewrite.c_str());
            return ret;
        }
    }
    int64_t pre_t1 = time_cost.get_time();
    time_cost.reset();
    //分类讨论ret返回值，区分出来哪些是连接错误
    ScopeProcWeight proc_weight(this); 
    int64_t update_weight_t1 = time_cost.get_time();
    time_cost.reset();
    ret = _process_query(sql_rewrite);
    int64_t process_query_t1 = time_cost.get_time();
    time_cost.reset();
    if (ret < 0) {
        CLIENT_WARNING("exectue time, pre_t1:%ld, update_weight_t1:%ld, process_query_t1:%ld",
                    pre_t1, update_weight_t1, process_query_t1);
        return ret;
    }
    // 只执行sql语句，不返回任何结果
    if (result == NULL) {
        return SUCCESS;
    }
    // 包装sql执行结果
    ret = _process_result(store, result);
    int64_t process_result_t1 = time_cost.get_time();
    //CLIENT_WARNING("exectue time, pre_t1:%ld, update_weight_t1:%ld, process_query_t1:%ld, process_result_t1:%ld",
                    //pre_t1, update_weight_t1, process_query_t1, process_result_t1);
    return ret;
}

int MysqlConnection::execute(const string& sql,
                                  vector<string>& table_name_list,
                                  ResultSet* result) {
    int ret = execute(sql, table_name_list, true, result);
    return ret;
}

int MysqlConnection::execute(const string& sql, bool store, ResultSet* result) {
    vector<string> table_name_list;
    BnsConnectionPool* pool = _pool;
    if (pool == NULL) {
        CLIENT_WARNING("the connection has been deleted, please fetch connection again");
        return CONNECTION_ALREADY_DELETED;
    }
    if (_has_partition_key && pool->is_split_table()) {
        _parse_table_name_list(sql, table_name_list);
    }
    return execute(sql, table_name_list, store, result);
}

int MysqlConnection::execute(const string& sql, ResultSet* result) {
    int ret = execute(sql, true, result);
    return ret;
}

int MysqlConnection::execute_raw(const string& sql,
                              bool store,
                              MYSQL_RES*& res) {
    //分类讨论ret返回值，区分出来哪些是连接错误
    ScopeProcWeight proc_weight(this);
    int ret = _process_query(sql);
    if (ret < 0) {
        return ret;
    }
    if (0 == mysql_field_count(_sql_handle)) {
        res = NULL;
        return SUCCESS;
    }
    res = store ? mysql_async_store_result(this) : mysql_use_result(_sql_handle);
    if (res == NULL) {
        CLIENT_WARNING("mysql store or mysql use fail, ip:%s, port:%d",
                _instance->get_ip().c_str(), _instance->get_port());
        return CONNECTION_QUERY_FAIL;
    }
    // 包装sql执行结果
    return ret;
}

int MysqlConnection::execute_raw(const string& sql, MYSQL_RES*& res) {
    int ret = execute_raw(sql, true, res);
    return ret;
}

int MysqlConnection::begin_transaction() {
    string sql = "Start Transaction";
    int ret = execute(sql, NULL);
    if (ret == SUCCESS) {
        _active_txn = true;
    }
    return ret;
}

int MysqlConnection::commit() {
    string sql = "Commit";
    int ret = execute(sql, NULL);
    if (ret == SUCCESS) {
        _active_txn = false;
    }
    return ret;
}

int MysqlConnection::rollback() {
    string sql = "Rollback";
    int ret = execute(sql, NULL);
    if (ret == SUCCESS) {
        _active_txn = false;
    }
    return ret;
}

int MysqlConnection::ping() {
    if (_sql_handle) {
        int ret = mysql_async_ping(this);
        if (ret != 0) {
            CLIENT_WARNING("ping fail, file code:%d, ip:%s, port:%d",
                    ret, _instance->get_ip().c_str(), _instance->get_port());
            return CONNECTION_PING_FAIL;
        }
        return SUCCESS;
    }
    CLIENT_WARNING("sql ping fail, sql handle is null");
    return CONNECTION_HANDLE_NULL;
}

int MysqlConnection::get_error_code(int* error_code) {
    if (_sql_handle) {
        *error_code = mysql_errno(_sql_handle);
        return 0;
    }
    CLIENT_WARNING("Get error code fail, sql handle is null");
    return CONNECTION_HANDLE_NULL;
}

string MysqlConnection::get_error_des() {
    if (_sql_handle) {
        return mysql_error(_sql_handle);
    }
    CLIENT_WARNING("get error des fail, sql handle is null ");
    return string("");
}
void MysqlConnection::set_read_timeout(const int32_t& read_time) {
    if (_sql_handle) {
        mysql_options(_sql_handle, MYSQL_OPT_READ_TIMEOUT, (void *)&(read_time));
    }
}
int MysqlConnection::_split_table(vector<string>& table_name_list, string& sql_rewrite) {
    uint32_t partition_key = _partition_key;
    vector<string>::iterator iter = table_name_list.begin();
    int ret = 0;
    for (; iter != table_name_list.end(); ++iter) {
        //解析db_name and table_name
        string db_name;
        string table_name;
        ret = _get_db_table_name(iter, db_name, table_name);
        if (ret < 0) {
            CLIENT_WARNING("get db and table name fail:%s", (*iter).c_str());
            return ret;
        }
        BnsConnectionPool* pool = _pool;
        if (pool == NULL) {
            CLIENT_WARNING("the connection has been deleted, please fetch connection again");
            return CONNECTION_ALREADY_DELETED;
        }
        bool table_split = false;
        int table_id = -1;
        int ret = pool->get_table_id(db_name,
                                     table_name,
                                     partition_key,
                                     &table_split,
                                     &table_id);
        if (ret < 0) {
            CLIENT_WARNING("get table id fail, table_name:%s", iter->c_str());
            return ret;
        }
        if (table_split) {
            _rewrite_table_name(sql_rewrite,
                                db_name,
                                table_name,
                                boost::lexical_cast<string>(table_id));
        }
    }
    return SUCCESS;
}

void MysqlConnection::_parse_db_name(const string& sql_rewrite,
                                            int pos,
                                            string& sql_db_name) {
    int num = 0;
    int start = pos -2;
    while (start >= 0) {
        if (!_is_valid_char(sql_rewrite[start])){
            break;
        }
        --start;
        ++num;
    }
    sql_db_name = sql_rewrite.substr(++start, num);
}

void MysqlConnection::_parse_table_name_list(const string& sql_rewrite,
                vector<string>& table_name_list) {
    //找到第一个有效的from关键字
    string::size_type from_locate = _find_key_word_after_num(sql_rewrite, "FROM", 0, false);
    // 找到第一个有效的where关键字
    string::size_type where_locate = _find_key_word_after_num(sql_rewrite, "WHERE", 0, false);
    if (where_locate == string::npos) {
        where_locate = sql_rewrite.size();
    }
    if (from_locate != string::npos) {
        string sub = sql_rewrite.substr(from_locate + 4, where_locate - from_locate - 4);
        vector<string> tables;
        boost::split(tables, sub, boost::is_any_of(","));
        for (size_t i = 0; i < tables.size(); ++i) {
            string result;
            _parse_first_string(tables[i], 0, result);
            if (!result.empty()) {
                table_name_list.push_back(result);
            }
        }
    }
    _parse_after_key_word("UPDATE", sql_rewrite, table_name_list);
    _parse_after_key_word("JOIN", sql_rewrite, table_name_list);
    _parse_after_key_word("INTO", sql_rewrite, table_name_list);
}

void MysqlConnection::_parse_after_key_word(const string& key_word,
                     const string& sql_rewrite,
                     vector<string>& table_name_list) {
    string::size_type start = 0;
    while (start < sql_rewrite.size()) {
        start = _find_key_word_after_num(sql_rewrite, key_word, start, false);
        if (start == string::npos) {
            return;
        }
        string result;
        start = _parse_first_string(sql_rewrite, start + key_word.size(), result);
        if (!result.empty()) {
            table_name_list.push_back(result);
        }
    }
}

string::size_type MysqlConnection::_parse_first_string(
        const string& sql_rewrite,
        string::size_type pos,
        string& result) {
    while (pos < sql_rewrite.size()) {
        if (!_is_valid_char(sql_rewrite[pos])) {
            ++pos;
        } else {
            break;
        }
    }
    int num = 0;
    string::size_type start = pos;
    while (pos < sql_rewrite.size()) {
        if (!_is_valid_char(sql_rewrite[pos]) && sql_rewrite[pos] != '.') {
            break;
        }
        ++pos;
        ++num;
    }
    result = sql_rewrite.substr(start, num);
    return pos;
}

void MysqlConnection::_rewrite_table_name(string& sql_rewrite,
                                               const string& db_name,
                                               const string& table_name,
                                               const string table_id) {
    string new_table_name = table_name + table_id;
    string::size_type start_pos = 0;
    while (start_pos < sql_rewrite.size()) {
        start_pos = _find_key_word_after_num(sql_rewrite, table_name, start_pos, true);
        if (start_pos == string::npos) {
            return;
        }
        if (start_pos == 0 || sql_rewrite[start_pos-1] != '.') {
            if (_has_logic_db && _logic_db == db_name) {
                sql_rewrite.replace(start_pos, table_name.size(), new_table_name);
            }
        } else {
            //解析出.之前的数据库名称
            string sql_db_name;
            _parse_db_name(sql_rewrite, start_pos, sql_db_name);
            if (sql_db_name == db_name) {
                sql_rewrite.replace(start_pos, table_name.size(), new_table_name);
            }
        }
        start_pos += new_table_name.size();
    }
}

int MysqlConnection::_get_db_table_name(vector<string>::iterator iter,
                                             string& db_name,
                                             string& table_name) {
    vector<string> db_table_name;
    boost::trim(*iter);
    boost::split(db_table_name, *iter, boost::is_any_of("."));
    if (db_table_name.size() <= 0 || db_table_name.size() > 2) {
        CLIENT_WARNING("input table_name format is wrong, table_name:%s", iter->c_str());
        return INPUTPARAM_ERROR;
    }
    if (db_table_name.size() == 1 && _has_logic_db == false) {
        CLIENT_WARNING("cannot decide which db input table_name belongs to, table_name:%s",
                      iter->c_str());
        return INPUTPARAM_ERROR;
    }
    if (db_table_name.size() == 2) {
        boost::trim(db_table_name[0]);
        db_name = db_table_name[0];
        boost::trim(db_table_name[1]);
        table_name = db_table_name[1];
    } else {
        db_name = _logic_db;
        boost::trim(db_table_name[0]);
        table_name = db_table_name[0];
    }
    return SUCCESS;
}

int MysqlConnection::_process_result(bool store, ResultSet* result) {
    result->set_conn_type(MYSQL_CONN);
    if (0 == mysql_field_count(_sql_handle)) {
        result->_free_result();
        result->_is_select = false;
        result->_affected_rows = mysql_affected_rows(_sql_handle);
        result->_auto_insert_id = mysql_insert_id(_sql_handle);
        return SUCCESS;
    }
    _trace_time_os << " store_in:" << butil::gettimeofday_ms() % 1000000;
    MYSQL_RES* tmp_res =
        store ? mysql_async_store_result(this) : mysql_use_result(_sql_handle);
    _trace_time_os << " store_out:" << butil::gettimeofday_ms() % 1000000;
    if (!tmp_res) {
        CLIENT_WARNING("mysql store or mysql use fail, ip:%s, port:%d",
                _instance->get_ip().c_str(), _instance->get_port());
        return CONNECTION_QUERY_FAIL;
    }
    result->_wrap(_sql_handle, tmp_res, store);
    return SUCCESS;
}

int MysqlConnection::_process_query(const string& sql_rewrite) {
    TimeCost cost;
    _trace_time_os.str("");
    _trace_time_os << " in:" << butil::gettimeofday_ms() % 1000000;
    int ret = mysql_async_real_query(this, sql_rewrite.c_str(), sql_rewrite.size(), &_trace_time_os);
    _trace_time_os << " out:" << butil::gettimeofday_ms() % 1000000;
    if (ret == 0) {
        return SUCCESS;
    }
    int error_code = 0;
    get_error_code(&error_code);
    CLIENT_WARNING("error_code:%d error_des:%s info:%s cost:%ld us",
            error_code, get_error_des().c_str(), get_instance_info().c_str(), cost.get_time());
    if (_is_hang.load()) {
        CLIENT_WARNING("instace is hang, ip:[%s], port:[%d], sql_len:%d, sql is:%s",
                _instance->get_ip().c_str(), _instance->get_port(),
                sql_rewrite.size(), sql_rewrite.c_str());
        _instance_to_faulty();
        return CONNECTION_IS_KILLED;
    }
    if (error_code == CR_SERVER_GONE_ERROR || error_code == CR_SERVER_LOST) {
        _instance_to_faulty();
        CLIENT_WARNING("instace faulty, ip:[%s], port:[%d], sql_len:%d, sql is:%s",
                 _instance->get_ip().c_str(), _instance->get_port(),
                 sql_rewrite.size(), sql_rewrite.c_str());
        return INSTANCE_FAULTY_ERROR;
        //if (reconnect(instance->get_conn_conf(), false) == SUCCESS) {
        //    CLIENT_WARNING("sql execute fail, but reconnect success, sql:%s, ip:%s, port:%d",
        //            sql_rewrite.c_str(), _instance->get_ip().c_str(), _instance->get_port());
        //    return CONNECTION_RECONN_SUCCESS;
        //} else {
        //    CLIENT_WARNING("instace faulty, sql is:%s, ip:[%s], port:[%d]",
        //             sql_rewrite.c_str(), _instance->get_ip().c_str(), _instance->get_port());
        //    return INSTANCE_FAULTY_ERROR;
        //}
    }
    if (error_code == CR_COMMANDS_OUT_OF_SYNC || error_code == CR_UNKNOWN_ERROR) {
        CLIENT_WARNING("query execute fail, ip:[%s], port:[%d], sql_len:%d, sql is:%s",
                     _instance->get_ip().c_str(), _instance->get_port(),
                     sql_rewrite.size(), sql_rewrite.c_str());
        return CONNECTION_QUERY_FAIL;
    }
    CLIENT_WARNING("query execute fail, ip:[%s], port:[%d], sql_len:%d, sql is:%s",
            _instance->get_ip().c_str(), _instance->get_port(),
            sql_rewrite.size(), sql_rewrite.c_str());
    return CONNECTION_QUERY_FAIL;
}

int MysqlConnection::_add_comment(BnsConnectionPool* pool,
                                       string& comment_format) {
    comment_format = pool->get_comment_format();
    if (!comment_format.empty()) {
        string::size_type pos = comment_format.find("$");
        // 若注释为 "$"形式，则将 "$"替换为partition_key
        // 若无上述字符，则comment_format不需要替换
        if (pos != string::npos && _has_partition_key) {
            comment_format.replace(pos, 1, boost::lexical_cast<string>(_partition_key));
        } 
        //else if (pos != string::npos && !_has_partition_key) {
        //    CLIENT_WARNING("comment format need partition key,"
        //                 "but no partition key when fetch_connection");
        //    CLIENT_WARNING("please close connection first,"
        //                 "then fetch connection again with partition key");
        //    return NO_PARTITION_KEY_ERROR;
        //}
    }
    return SUCCESS;
}

string::size_type MysqlConnection::_find_key_word_after_num(
            const string& sql_rewrite,
            const string& key_word,
            string::size_type num,
            bool case_sensitive) {
    // 大小写敏感查找
    if (case_sensitive) {
        return _findn_case_sensitive(sql_rewrite, key_word, num);
    }
    //忽略大小写的查找
    string copy_sql = sql_rewrite;
    transform(sql_rewrite.begin(), sql_rewrite.end(), copy_sql.begin(), ::toupper);
    string copy_key_word = key_word;
    transform(key_word.begin(), key_word.end(), copy_key_word.begin(), ::toupper);
    return _findn_case_sensitive(copy_sql, copy_key_word, num);
}

string::size_type MysqlConnection::_findn_case_sensitive(
            const string& sql_rewrite,
            const string& key_word,
            string::size_type num) {
    string::size_type start_pos = num;
    while (start_pos < sql_rewrite.size()) {
        start_pos = sql_rewrite.find(key_word, start_pos);
        if (start_pos == string::npos) {
            return start_pos;
        }
        string::size_type end_pos = start_pos + key_word.size() - 1;
        if (_is_valid_key_word(start_pos, end_pos, sql_rewrite)) {
            return start_pos;
        }
        start_pos = end_pos + 1;
    }
    return string::npos;
}

bool MysqlConnection::_is_valid_key_word(string::size_type start_pos,
                                              string::size_type end_pos,
                                              const string& sql_rewrite) {
    string::size_type sql_size = sql_rewrite.size();
    if (start_pos == 0 && end_pos == sql_size -1 ||
        start_pos == 0 && !_is_valid_char(sql_rewrite[end_pos + 1]) ||
        end_pos == sql_size-1 && !_is_valid_char(sql_rewrite[start_pos-1]) ||
        !_is_valid_char(sql_rewrite[start_pos - 1])
        && !_is_valid_char(sql_rewrite[end_pos + 1])) {
        return true;
    }
    return false;
}

bool MysqlConnection::_is_valid_char(char c) {
    // 字母、数字、_、$被认为是有效字符，其他均为无效
    if (c == '$' ||
        c == '_' ||
        c >= '0' && c <= '9' ||
        c >= 'a' && c <= 'z' ||
        c >= 'A' && c <= 'Z') {
        return true;
    }
    return false;
}

void MysqlConnection::close() {
    if (_active_txn) {
        int ret = rollback();
        if (ret != SUCCESS) {
            CLIENT_FATAL("rollback transaction failed when close: %d", ret);
            _status.store(IS_BAD);   
            return;
        }        
    }
    Connection::close();
}
}
} //namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
