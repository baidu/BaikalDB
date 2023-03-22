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
//class Region;
class Region;
class MetaWriter;
// TODO: remove locking for thread-safe codes
class TransactionPool {
public:
    virtual ~TransactionPool() {
    }

    void close() {
        _txn_map.clear();
        _txn_count = 0;
    }

    TransactionPool() : _num_prepared_txn(0), _txn_count(0) {}

    int init(int64_t region_id, bool use_ttl, int64_t online_ttl_base_expire_time_us);

    // -1 means insert error (already exists)
    int begin_txn(uint64_t txn_id, SmartTransaction& txn, int64_t primary_region_id,
             int64_t txn_timeout, int64_t txn_lock_timeout);

    void remove_txn(uint64_t txn_id, bool mark_finished);

    SmartTransaction get_txn(uint64_t txn_id) {
        return _txn_map.get(txn_id);
    }
    // -1 not found;
    int get_finished_txn_affected_rows(uint64_t txn_id) {
        if (_finished_txn_map.read()->exist(txn_id)) {
            return _finished_txn_map.read()->get(txn_id);
        }
        if (_finished_txn_map.read_background()->exist(txn_id)) {
            return _finished_txn_map.read_background()->get(txn_id);
        }
        return -1;
    }

    void rollback_mark_finished(uint64_t txn_id) {
        (*_finished_txn_map.read())[txn_id] = -1;
    }

    bool is_mark_finished(uint64_t txn_id) {
        if (_finished_txn_map.read()->exist(txn_id)) {
            return true;
        }
        if (_finished_txn_map.read_background()->exist(txn_id)) {
            return true;
        }
        return false;
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

    int64_t online_ttl_base_expire_time_us() const {
        return _online_ttl_base_expire_time_us;
    }

    void update_ttl_info(bool use_ttl, int64_t online_ttl_base_expire_time_us) {
        _use_ttl = use_ttl;
        _online_ttl_base_expire_time_us = online_ttl_base_expire_time_us;
    }

    bool exec_1pc_out_fsm();

    void clear_transactions(Region* region);

    void on_leader_stop_rollback();

    void clear_orphan_transactions();

    void rollback_txn_before(int64_t txn_timeout);

    void update_primary_timestamp(const pb::TransactionInfo& txn_info);

    void get_prepared_txn_info(std::unordered_map<uint64_t, pb::TransactionInfo>& prepared_txn, bool for_num_rows);

    void update_txn_num_rows_after_split(const std::vector<pb::TransactionInfo>& txn_infos);

    void txn_query_primary_region(uint64_t txn_id, Region* region, pb::RegionInfo& region_info);
    void txn_commit_through_raft(uint64_t txn_id, pb::RegionInfo& region_info, pb::OpType op_type);
    void get_txn_state(const pb::StoreReq* request, pb::StoreRes* response);
    void read_only_txn_process(int64_t region_id, SmartTransaction txn, pb::OpType op_type, bool optimize_1pc);
    int get_region_info_from_meta(int64_t region_id, pb::RegionInfo& region_info);
    //清空所有的状态
    void clear();
private:
    struct TxnParams {
        bool is_primary_region = true;
        bool is_finished = false;
        bool is_prepared = false;
        int seq_id = 0;
        int64_t primary_region_id = -1;
    };
    int64_t _region_id = 0;
    int64_t _latest_active_txn_ts = 0;
    bool _use_ttl = false;
    int64_t _online_ttl_base_expire_time_us = 0;

    // txn_id => txn handler mapping
    ThreadSafeMap<uint64_t, SmartTransaction>  _txn_map;
    // txn_id => affected_rows use for idempotent
    DoubleBuffer<ThreadSafeMap<uint64_t, int>> _finished_txn_map;
    TimeCost _clean_finished_txn_cost;

    BthreadCond  _num_prepared_txn;  // total number of prepared transactions
    std::atomic<int32_t> _txn_count;
    MetaWriter*          _meta_writer = nullptr;
};
}
