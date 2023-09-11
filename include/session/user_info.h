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
#include <stdint.h>
#include <mutex>
#include <set>
#include <unordered_set>
#include <map>
#include <memory>
#include <string>
#include "proto/meta.interface.pb.h"
#include "proto/plan.pb.h"
#include "common.h"

namespace baikaldb {
DECLARE_bool(need_verify_ddl_permission);
DECLARE_bool(use_read_index);
struct UserInfo {
public:
    UserInfo() : query_count(0) {
    }

    ~UserInfo() {}

    bool is_exceed_quota() {
        if (query_cost.get_time() > 1000000) {
            query_cost.reset();
            query_count = 0;
            return false;
        }
        return query_count++ > query_quota;
    }

    bool connection_inc() {
        bool res = false;
        std::lock_guard<std::mutex> guard(conn_mutex);
        if (cur_connection < max_connection) {
            cur_connection++;
            res = true;
        } else {
            res = false;
        }
        return res;
    }

    void connection_dec() {
        std::lock_guard<std::mutex> guard(conn_mutex);
        if (cur_connection > 0) {
            cur_connection--;
        }
    }

    bool allow_write(int64_t db, int64_t tbl, const std::string& tbl_name) {
        if (database.count(db) == 1 && database[db] == pb::WRITE) {
            return true;
        }
        if (table.count(tbl) == 1 && table[tbl] == pb::WRITE) {
            return true;
        }
        if (table_name.count(tbl_name) == 1 && table_name[tbl_name] == pb::WRITE) {
            return true;
        }
        return false;
    }

    bool allow_read(int64_t db, int64_t tbl, const std::string& tbl_name) {
        if (database.count(db) == 1) {
            return true;
        }
        if (table.count(tbl) == 1) {
            return true;
        }
        if (table_name.count(tbl_name) == 1) {
            return true;
        }
        return false;
    }

    bool allow_op(pb::OpType op_type, int64_t db, int64_t tbl, const std::string& table_name) {
        if (op_type == pb::OP_SELECT) {
            return allow_read(db, tbl, table_name);
        } else {
            return allow_write(db, tbl, table_name);
        }
    }

    bool allow_addr(const std::string& ip) {
        if (need_auth_addr) {
            if (auth_ip_set.count(ip)) {
                return true;
            } else {
                return false;
            }
        }
        return true;
    }

    bool allow_ddl() {
        if (!FLAGS_need_verify_ddl_permission) {
            return true;
        }
        return ddl_permission;
    }

    bool need_use_read_index() {
        if (!FLAGS_use_read_index) {
            return false;
        }
        return use_read_index;
    }

public:
    std::string     username;
    std::string     password;
    std::string     namespace_;

    int64_t         namespace_id = 0;
    int64_t         version = 0;
    int64_t         txn_lock_timeout = -1;
    uint8_t         scramble_password[20];

    TimeCost        query_cost;
    std::mutex      conn_mutex;
    uint32_t        max_connection = 0;
    uint32_t        cur_connection = 0;
    uint32_t        query_quota = 0;
    bool            need_auth_addr = false;
    bool            ddl_permission = false;
    bool            use_read_index = true; // 上线先默认true
    bool            enable_plan_cache = false;

    pb::RangePartitionType request_range_partition_type = pb::RPT_DEFAULT;

    std::atomic<uint32_t>    query_count;
    std::map<int64_t, pb::RW> database;
    std::map<int64_t, pb::RW> table;
    std::map<std::string, pb::RW> table_name;
    // show databases使用
    std::set<int64_t> all_database;
    std::unordered_set<std::string> auth_ip_set;
    std::string resource_tag;
};
} // namespace baikaldb
