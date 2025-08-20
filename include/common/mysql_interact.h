// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
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

#pragma once

#include "common.h"
#ifdef BAIDU_INTERNAL
#include "baikal_client.h"
#else
typedef int MYSQL;
namespace baikal {
namespace client {
enum ErrorCode {
    SUCCESS = 0,
    VALUE_IS_NULL = -11
};
class ResultSet {
public:
    uint64_t get_affected_rows() {
        return 0;
    }
    bool next() {
        return false;
    }
    int get_uint64(const std::string& col, uint64_t* val) {
        return 0;
    }
    int get_int64(const std::string& col, int64_t* val) {
        return 0;
    }
    int get_string(const std::string& col, std::string* value) {
        return 0;
    }
    int get_string(uint32_t column_index, std::string* value) {
        return 0;
    }
};
class Service {
public:
    int query(int i, const std::string& sql, ResultSet* res) {
        return -1;
    }
};
class Manager {
public:
    int init(const std::string& path, const std::string& conf) {
        return -1;
    }
    Service* get_service(const std::string& name) {
        return nullptr;
    }
};
class MysqlShortConnection {
public:
    int execute(const std::string& sql, ResultSet* result) {
        return 0;
    }
    int execute(const std::string& sql, bool store, ResultSet* result) {
        return 0;
    }
    MYSQL* get_mysql_handle() {
        return NULL;
    }
    int get_error_code(int* error_code) {
        return 0;
    }
    std::string get_error_des() {
        return 0;
    }
};

}
}
#endif
#include "proto/common.pb.h"

namespace baikaldb {

class MysqlInteract {
public:
    MysqlInteract(const pb::MysqlInfo& mysql_info) : _mysql_info(mysql_info) {}
    virtual ~MysqlInteract() {}

    // @brief 请求mysql获取数据
    // @param sql: sql语句
    // @param result_set: 返回结果集
    // @param store: 是否为流式读，true表示不是流式读
    int query(const std::string& sql, baikal::client::ResultSet* result_set, bool store = true);

    // @brief 获取当前连接错误码
    int get_error_code();

    // @brief 获取当前连接，如果连接不存在，则会新创建连接
    baikal::client::MysqlShortConnection* get_connection();

    // @brief 对字符串进行转义
    static std::string mysql_escape_string(baikal::client::MysqlShortConnection* conn, const std::string& value);
    
private:
    std::unique_ptr<baikal::client::MysqlShortConnection> fetch_connection();

private:
    pb::MysqlInfo _mysql_info;
    std::unique_ptr<baikal::client::MysqlShortConnection> _mysql_conn;
};

} // namespace baikaldb
