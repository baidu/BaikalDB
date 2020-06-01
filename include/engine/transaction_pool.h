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
 
#include <unordered_map>
#include "common.h"
#include "transaction.h"

namespace baikaldb {
DECLARE_int32(transaction_clear_delay_ms);
//class Region;
class Region;
class MetaWriter;
// TODO: remove locking for thread-safe codes
class TransactionPool {
public:
    virtual ~TransactionPool() {}

    void close() {
        std::unique_lock<std::mutex> lock(_map_mutex);
        auto iter = _txn_map.begin();
        while (iter != _txn_map.end()) {
            iter->second = nullptr;
            iter = _txn_map.erase(iter);
        }
    }

    TransactionPool() : _num_prepared_txn(0), _txn_count(0) {}

    int init(int64_t region_id, bool use_ttl);

    // -1 means insert error (already exists)
    int begin_txn(uint64_t txn_id, SmartTransaction& txn, int64_t primary_region_id);

    void remove_txn(uint64_t txn_id);

    SmartTransaction get_txn(uint64_t txn_id) {
        std::unique_lock<std::mutex> lock(_map_mutex);
        if (_txn_map.count(txn_id) == 0) {
            return nullptr;
        }
        return _txn_map[txn_id];
    }
    // -1 not found;
    int get_finished_txn_affected_rows(uint64_t txn_id) {

        std::unique_lock<std::mutex> lock(_map_mutex);
        if (_finished_txn_map.read()->count(txn_id) == 1) {
            return _finished_txn_map.read()->at(txn_id);
        }
        if (_finished_txn_map.read_background()->count(txn_id) == 1) {
            return _finished_txn_map.read_background()->at(txn_id);
        }
        return -1;
    }

    void increase_prepared() {
        _num_prepared_txn.increase();
    }

    void decrease_prepared() {
        _num_prepared_txn.decrease_signal();
    }

    int32_t num_prepared() {
        return _num_prepared_txn.count();
    }

    int32_t num_began() {
        return _txn_count.load();
    }

    bool use_ttl() const {
        return _use_ttl;
    }

    void clear_transactions(Region* region);

    void on_leader_stop_rollback();

    void on_leader_stop_rollback(uint64_t txn_id);

    void on_leader_start_recovery(Region* region);

    int on_shutdown_recovery(
            std::vector<rocksdb::Transaction*>& recovered_txns,
            std::unordered_map<uint64_t, pb::TransactionInfo>& prepared_txn);

    // int on_crash_recovery(
    //         std::unordered_map<uint64_t, pb::TransactionInfo>& prepared_txn);

    // get transaction info (num_increase_rows and seq_id) for all prepared txns 
    // on shutdown gracefully. used for txn recovery on graceful shutdown
    //std::unordered_map<uint64_t, PreparedTxnInfo> get_prepared_txn_info();
    void get_prepared_txn_info(
            std::unordered_map<uint64_t, pb::TransactionInfo>& prepared_txn,
            bool graceful_shutdown);

    void update_txn_num_rows_after_split(const pb::TransactionInfo& txn_info);

    void txn_query_primary_region(SmartTransaction txn, Region* region, pb::RegionInfo& region_info);
    void txn_commit_through_raft(SmartTransaction txn, pb::RegionInfo& region_info, pb::OpType op_type);
    void get_txn_state(const pb::StoreReq* request, pb::StoreRes* response);
    void read_only_txn_process(SmartTransaction txn, pb::OpType op_type, bool optimize_1pc);
    //清空所有的状态
    void clear();
private:
    int64_t _region_id = 0;
    bool _use_ttl = false;

    // txn_id => txn handler mapping
    std::unordered_map<uint64_t, SmartTransaction>  _txn_map;
    // txn_id => affected_rows use for idempotent
    DoubleBuffer<std::unordered_map<uint64_t, int>> _finished_txn_map;
    TimeCost _clean_finished_txn_cost;
    std::mutex _map_mutex;

    BthreadCond  _num_prepared_txn;  // total number of prepared transactions
    std::atomic<int32_t> _txn_count;
    MetaWriter*          _meta_writer = nullptr;
};
}
