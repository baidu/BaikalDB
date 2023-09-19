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
#include <raft/repeated_timer_task.h>
#else
#include <butil/endpoint.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include <brpc/controller.h>
#include <braft/repeated_timer_task.h>
#endif

#include <memory>

namespace baikaldb {

DECLARE_string(meta_server_bns);
class TsoFetcher : public braft::RepeatedTimerTask {
public:
    static TsoFetcher* get_instance() {
        static TsoFetcher ins;
        return &ins;
    }
    ~TsoFetcher() {
        stop();
        destroy();
    }
    int init();
    int64_t get_tso(int64_t count);
    void set_instance_id(uint64_t instance_id) {
        _instance_id = instance_id;
    }
    void run() override;
    void on_destroy() override {}
    void set_degrade() {
        _need_degrade = true;
    }

    bvar::Adder<int64_t> tso_count;
    bvar::Adder<int64_t> tso_error;
private:
    pb::TsoTimestamp _tso_obj;
    bthread::Mutex _tso_mutex;  // 保护_tso_time
    bool _need_degrade = false;
    uint64_t _instance_id = 0;
};

struct PartitionBinlog {
    int64_t           start_ts = -1;
    int64_t           commit_ts = -1;
    pb::RegionInfo        binlog_region;
    pb::PrewriteValue     binlog_value;
    std::set<std::string> db_tables;
    TimeCost              binlog_prewrite_time;
    uint64_t              partition_id = 0;
    int64_t               binlog_row_cnt = 0;
    std::set<uint64_t>    signs;
    void calc_binlog_row_cnt() {
        binlog_row_cnt = 0;
        for (const auto& mutation : binlog_value.mutations()) {
            binlog_row_cnt += mutation.insert_rows_size();
            binlog_row_cnt += mutation.update_rows_size();
            binlog_row_cnt += mutation.deleted_rows_size();
        }
    }
};

typedef std::shared_ptr<PartitionBinlog> SmartPartitionBinlog;

struct BinlogInfo {
    SmartTable       binlog_table_info = nullptr;
    bool             partition_is_same_hint = true;
    std::map<int64_t, SmartPartitionBinlog> binlog_by_partition;
};
struct WriteBinlogParam;
class BinlogContext {
public:
    BinlogContext() {
        _factory = SchemaFactory::get_instance();
    };
    ~BinlogContext() { };

    // 每个partition设置自己的start_ts和commit_ts
    void set_start_ts(int64_t start_ts) {
        _start_ts = start_ts;
        for (auto& pair : _table_binlogs) {
            for (auto& pair2 : pair.second.binlog_by_partition) {
                pair2.second->start_ts = start_ts++;
            }
        }
    }

    void set_commit_ts(int64_t commit_ts) {
        for (auto& pair : _table_binlogs) {
            for (auto& pair2 : pair.second.binlog_by_partition) {
                pair2.second->commit_ts = commit_ts++;
            }
        }
        _commit_ts = --commit_ts;
    }

    int64_t get_first_start_ts() const {
        return _start_ts;
    }
    // binlog反查primary_region时使用
    int64_t get_last_commit_ts() const {
        return _commit_ts;
    }

    bool has_data_changed() {
        return _table_binlogs.size() > 0;
    }

    int tso_count() const {
        return _tso_count;
    }

    void set_repl_mark_info(SmartTable& table_info,
                          const std::string& sql,
                          const uint64_t sign,
                          SmartRecord& record) {
        _repl_mark_table_info = table_info;
        _repl_mark_sql = sql;
        _repl_mark_sign = sign;
        _repl_mark_record = record;
    }

    SmartPartitionBinlog get_partition_binlog_ptr(int64_t binlog_id,
                                                  BinlogInfo& binlog_info,
                                                  int64_t partition_id);
    
    void add_mutation(SmartPartitionBinlog& binlog_ptr,
                    int64_t table_id,
                    const std::string& sql,
                    const uint64_t sign,
                    pb::MutationType type,
                    int64_t partition_id,
                    bool all_in_one,
                    const std::map<int64_t, std::vector<std::string>>& retrun_records,
                    const std::map<int64_t, std::vector<std::string>>& retrun_old_records);

    int64_t get_partition_id(const SmartTable& binlog_table_info,
                                  const SmartRecord& record,
                                  const FieldInfo& link_field) {
        if (binlog_table_info->partition_num == 1) {
            return 0;
        }
        auto field_desc = record->get_field_by_idx(link_field.pb_idx);
        ExprValue value = record->get_value(field_desc);
        int64_t partition_id = 0;
        if (binlog_table_info->partition_ptr != nullptr) {
            std::shared_ptr<UserInfo> user_info = nullptr; // OLAPTODO - binlog不支持主备分区表
            partition_id = binlog_table_info->partition_ptr->calc_partition(user_info, value);
        } else {
            DB_WARNING("partition info error, value:%s", value.get_string().c_str());
            return -1;
        }
        _partition_keys[partition_id] = value.get_numberic<uint64_t>();
        return partition_id;
    }

    int add_binlog_values(SmartTable& table_info,
                          const std::string& sql,
                          const uint64_t sign,
                          pb::MutationType type,
                          const std::map<int64_t, std::vector<std::string>>& retrun_records,
                          const std::map<int64_t, std::vector<std::string>>& retrun_old_records);
    
    int add_binlog_values(SmartTable& table_info,
                          const std::string& sql,
                          const uint64_t sign,
                          pb::MutationType type,
                          const std::vector<SmartRecord>& retrun_records,
                          const std::vector<SmartRecord>& retrun_old_records);
    
    int write_binlog(const WriteBinlogParam* param);
    int send_binlog_data(const WriteBinlogParam* param, const SmartPartitionBinlog& partition_binlog_ptr);

private:
    SchemaFactory*        _factory = nullptr;
    SmartTable            _repl_mark_table_info = nullptr;
    SmartRecord           _repl_mark_record;
    std::string           _repl_mark_sql;
    uint64_t              _repl_mark_sign = 0;
    std::map<int64_t, uint64_t>  _partition_keys;
    int                   _tso_count = 0;
    int64_t               _start_ts = -1;
    int64_t               _commit_ts = -1;
    // binlog_table_id-->Binlog
    std::map<int64_t, BinlogInfo> _table_binlogs;
};

} // namespace baikaldb