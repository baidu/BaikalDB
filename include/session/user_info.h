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
#include "parser.h"
#include <optional>
namespace baikaldb {
DECLARE_bool(need_verify_ddl_permission);
DECLARE_bool(use_read_index);
inline uint32_t get_op_require_acl(pb::OpType op_type) {
    switch (op_type) {
        case pb::OP_SELECT:
        case pb::OP_UNION:
            return parser::SELECT_ACL;
        case pb::OP_INSERT:
            return parser::INSERT_ACL;
        case pb::OP_DELETE:
        case pb::OP_TRUNCATE_TABLE:
            return parser::DELETE_ACL;
        case pb::OP_UPDATE:
            return parser::UPDATE_ACL;
        case pb::OP_LOAD:
            return parser::FILE_ACL;
        case pb::OP_CREATE_NAMESPACE:
        case pb::OP_CREATE_DATABASE:
        case pb::OP_CREATE_TABLE:
            return parser::CREATE_ACL;
        case pb::OP_DROP_NAMESPACE:
        case pb::OP_DROP_DATABASE:
        case pb::OP_DROP_TABLE:
            return parser::DROP_ACL;
        case pb::OP_MODIFY_NAMESPACE:
        case pb::OP_MODIFY_DATABASE:
        case pb::OP_RENAME_TABLE:
        case pb::OP_ADD_FIELD:
        case pb::OP_DROP_FIELD:
        case pb::OP_RENAME_FIELD:
        case pb::OP_MODIFY_FIELD:
            return parser::ALTER_ACL;
        case pb::OP_ADD_INDEX:
        case pb::OP_DROP_INDEX:
        case pb::OP_RENAME_INDEX:
            return parser::INDEX_ACL;
        case pb::OP_CREATE_USER:
        case pb::OP_DROP_USER:
        case pb::OP_MODIFY_USER:
            return parser::CREATE_USER_ACL;
        case pb::OP_ADD_PRIVILEGE :
        case pb::OP_DROP_PRIVILEGE:
            return parser::GRANT_ACL;
        default:
            return 0U;
    }
}

struct UserInfo {
public:
    UserInfo() : query_count(0) {
    }

    ~UserInfo() {}

    bool is_exceed_quota();
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

    bool allow_op_v1(pb::OpType op_type, int64_t db, int64_t tbl, const std::string& table_name) {
        if (is_super) {
            return true;
        }
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
        if (is_super) {
            return true;
        }
        return ddl_permission;
    }

    bool need_use_read_index() {
        if (use_read_index_opt.has_value()) {
            return use_read_index_opt.value();
        }
        if (FLAGS_use_read_index) {
            return true;
        }
        return false;
    }

    bool acl_v2_is_valid() {
        return acl_user != 0 || acl_database.size() > 0 || acl_table.size() > 0;
    }

    bool allow_op_v2(pb::OpType op_type, int64_t db, int64_t tbl) {
        uint32_t acl_require = get_op_require_acl(op_type);
        return contain_privs(acl_require, db, tbl);
    }
    bool contain_privs(uint32_t acl_require, int64_t db, int64_t tbl) {
        if (acl_require == (acl_require & acl_user)) {
            return true;
        }
        if ((db != -1) && acl_database.count(db) > 0) {
            if (acl_require == (acl_require & acl_database[db])) {
                return true;
            }
        }
        if ((tbl != -1) && acl_table.count(tbl) > 0) {
            if (acl_require == (acl_require & acl_table[tbl])) {
                return true;
            }
        }
        return false;
    }

    bool allow_op(pb::OpType op_type, int64_t db, int64_t tbl, const std::string& table_name) {
        if (acl_v2_is_valid()) { // 优先用v2版本权限控制
            return allow_op_v2(op_type, db, tbl);
        } else { // 兼容v1版本权限控制
            return allow_op_v1(op_type, db, tbl, table_name);
        }
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
    std::optional<bool> use_read_index_opt;
    bool            enable_plan_cache = false;
    bool            is_super = false;
    bool            is_request_additional = false;


    pb::RangePartitionType request_range_partition_type = pb::RPT_DEFAULT;

    std::atomic<uint32_t>    query_count;
    std::map<int64_t, pb::RW> database;
    std::map<int64_t, pb::RW> table;
    std::map<std::string, pb::RW> table_name;

    // TODO: 'user_name'@'host_name'， 按照IP授权
    uint32_t acl_user = 0;
    std::map<int64_t, uint32_t> acl_database; // <database_id, acl>
    std::map<int64_t, uint32_t> acl_table; // <table_id, acl>

    // show databases使用
    std::set<int64_t> all_database;
    std::unordered_set<std::string> auth_ip_set;
    std::string resource_tag;
    std::unordered_set<int64_t> switch_tables;
};
} // namespace baikaldb
