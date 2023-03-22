// Copyright (c) 2020-present Baidu, Inc. All Rights Reserved.
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
#include "proto/binlog.pb.h"
#include "table_record.h"
#include "expr_value.h"
#include "schema_factory.h"
#include "meta_server_interact.hpp"

#ifdef BAIDU_INTERNAL
#include <base/endpoint.h>
#include <baidu/rpc/channel.h>
#include <baidu/rpc/server.h>
#include <baidu/rpc/controller.h>
#else
#include <butil/endpoint.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include <brpc/controller.h>
#endif

#include <memory>

namespace baikaldb {

DECLARE_string(meta_server_bns);
class TsoFetcher {
public:
    static int64_t get_tso();
};

class BinlogContext {
public:
    BinlogContext() {
        _factory = SchemaFactory::get_instance();
    };
    ~BinlogContext() { };

    int get_binlog_regions(uint64_t log_id);

    pb::PrewriteValue* mutable_binlog_value() {
        return &_binlog_value;
    }
    const pb::PrewriteValue& binlog_value() const {
        return _binlog_value;
    }

    int64_t start_ts() const {
        return _start_ts;
    }

    void set_start_ts(int64_t start_ts) {
        _start_ts = start_ts;
    }

    int64_t commit_ts() const {
        return _commit_ts;
    }

    void set_commit_ts(int64_t commit_ts) {
        _commit_ts = commit_ts;
    }
    
    pb::RegionInfo& binglog_region() {
        return _binlog_region;
    }

    void set_partition_record(SmartRecord partition_record) {
        _partition_record = partition_record;
    }

    bool has_data_changed() {
        return _partition_record != nullptr;
    }

    void set_table_info(SmartTable table_info) {
        _table_info = table_info;
    }

    uint64_t get_partition_key() const {
        return _partition_key;
    }

    void calc_binlog_row_cnt() {
         _binlog_row_cnt = 0;
         for (const auto& mutation : _binlog_value.mutations()) {
             _binlog_row_cnt += mutation.insert_rows_size();
             _binlog_row_cnt += mutation.update_rows_size();
             _binlog_row_cnt += mutation.deleted_rows_size();
         }
    }

    int64_t get_binlog_row_cnt() const { return _binlog_row_cnt; }

    void add_sql_info(const std::string& db, const std::string& table, const uint64_t sign) {
        _db_tables.insert(db + "." + table);
        _signs.insert(sign);
    }

    std::set<std::string>& get_db_tables() {
        return _db_tables;
    } 

    std::set<uint64_t>& get_signs() {  
        return _signs;
    }

private:
    SmartTable            _table_info = nullptr;
    int64_t               _start_ts = -1;
    int64_t               _commit_ts = -1;
    pb::PrewriteValue     _binlog_value;
    SmartRecord           _partition_record;
    pb::RegionInfo        _binlog_region;
    SchemaFactory*        _factory = nullptr;
    uint64_t              _partition_key = 0;
    int64_t               _binlog_row_cnt = 0;
    std::set<std::string> _db_tables;
    std::set<uint64_t>    _signs;
};

} // namespace baikaldb
