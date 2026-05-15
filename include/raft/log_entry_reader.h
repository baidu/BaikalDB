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

#include "rocks_wrapper.h"
#include "store.interface.pb.h"

namespace baikaldb {
typedef std::function<int(const rocksdb::Slice&, const rocksdb::Slice&, bool& need_break)> KV_PROCESS_FUNC;

class LogEntryReader {
public:
    virtual ~LogEntryReader() {}
   
    static LogEntryReader* get_instance() {
        static LogEntryReader _instance;
        return &_instance;
    } 
    void init(RocksWrapper* rocksdb, 
            rocksdb::ColumnFamilyHandle* log_cf,
            rocksdb::ColumnFamilyHandle* new_log_cf) {
        _rocksdb = rocksdb;
        _log_cf = log_cf;
        _new_log_cf = new_log_cf;
    }
    bool is_txn_op_type(const pb::OpType& op_type) {
        if (op_type == pb::OP_INSERT
            || op_type == pb::OP_DELETE
            || op_type == pb::OP_UPDATE
            || op_type == pb::OP_PREPARE
            || op_type == pb::OP_ROLLBACK
            || op_type == pb::OP_COMMIT
            || op_type == pb::OP_SELECT_FOR_UPDATE
            || op_type == pb::OP_KV_BATCH
            || op_type == pb::OP_PARTIAL_ROLLBACK) {
            return true;
        }
        return false;
    }
    int read_log_entry(int64_t region_id, int64_t log_index, std::string& log_entry); 
    int read_log_entry(int64_t region_id, int64_t start_log_index, int64_t end_log_index,
        std::set<uint64_t>& txn_ids, std::map<int64_t, std::string>& log_entrys);
    int read_log_entry(int64_t region_id, int64_t start_log_index, int64_t end_log_index, uint64_t txn_id,
        std::map<int64_t, std::string>& log_entrys) {
        std::set<uint64_t> txn_ids;
        txn_ids.insert(txn_id);
        return read_log_entry(region_id, start_log_index, end_log_index, txn_ids, log_entrys);
    }
    int read_txn_last_log_entry(int64_t region_id, int64_t start_log_index, int64_t end_log_index,
        std::set<uint64_t>& txn_ids, std::map<uint64_t, std::string>& log_entrys);

    /**
     * 迭代器遍历处理 log_index [start_index, end_index]内的raft log
     * 包含end_index，会保证iterator不越界
     * 屏蔽新老raft_log_storage区别，使用时只需要关注处理raft log的逻辑
     * @param region_id region_id
     * @param start_index 起始log_index
     * @param end_index 终止 end_index, 小于0表示无上限，需要在 process_raft_log 内判断何时break
     * @param process_raft_log 处理函数
     * @return status
     */
    int process_logs(int64_t region_id,
                     int64_t start_index,
                     int64_t end_index,
                     KV_PROCESS_FUNC process_raft_log);

    int remove_log_entry(int64_t drop_region_id);

private:
    LogEntryReader() {}

private:
    RocksWrapper*       _rocksdb;
    rocksdb::ColumnFamilyHandle* _log_cf;
    rocksdb::ColumnFamilyHandle* _new_log_cf;
    DISALLOW_COPY_AND_ASSIGN(LogEntryReader);  
};

} // end of namespace
