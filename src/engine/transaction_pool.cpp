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

#include "transaction_pool.h"
#include <gflags/gflags.h>
#include <boost/algorithm/string.hpp>

namespace baikaldb {
//DECLARE_int32(rocks_transaction_expiration_ms);
DEFINE_int32(transaction_clear_delay_ms, 3600 * 1000, 
        "delay duration to clear prepared and expired transactions");
DEFINE_int64(clean_finished_txn_interval_us, 600 * 1000 * 1000LL, 
        "clean_finished_txn_interval_us");

int TransactionPool::init(int64_t region_id, bool use_ttl) {
    _region_id = region_id;
    _use_ttl = use_ttl;
    return 0;
}

// -1 means insert error (already exists)
int TransactionPool::begin_txn(uint64_t txn_id, SmartTransaction& txn) {
    //int64_t region_id = _region->get_region_id();
    std::string txn_name = std::to_string(_region_id) + "_" + std::to_string(txn_id);
    std::unique_lock<std::mutex> lock(_map_mutex);
    if (_txn_map.count(txn_id) != 0) {
        DB_FATAL("txn already exists, txn_id: %lu", txn_id);
        return -1;
    }
    txn = SmartTransaction(new (std::nothrow)Transaction(txn_id, this, _use_ttl));
    if (txn == nullptr) {
        DB_FATAL("new txn failed, txn_id: %lu", txn_id);
        return -1;
    }
    auto ret = txn->begin();
    if (ret != 0) {
        DB_FATAL("begin txn failed, txn_id: %lu", txn_id);
        txn.reset();
        return -1;
    }
    auto res = txn->get_txn()->SetName(txn_name);
    if (!res.ok()) {
        DB_WARNING("unknown error: %d, %s", res.code(), res.ToString().c_str());
    }
    //DB_WARNING("txn_begin: %p, %s", txn->get_txn(), txn_name.c_str());
    _txn_map.insert(std::make_pair(txn_id, txn));
    _txn_count++;
    return 0;
}

void TransactionPool::remove_txn(uint64_t txn_id) {
    std::unique_lock<std::mutex> lock(_map_mutex);
    if (_txn_map.count(txn_id) == 0) {
        return;
    }
    (*_finished_txn_map.read())[txn_id] = _txn_map[txn_id]->dml_num_affected_rows;
    //DB_WARNING("txn_removed: %p, %lu", txn, txn->GetName().c_str());
    _txn_map.erase(txn_id);
    _txn_count--;
}

// 清理僵尸事务：包括长时间（clear_delay_ms）未更新的事务
void TransactionPool::clear_transactions(int32_t clear_delay_ms) {
    std::unique_lock<std::mutex> lock(_map_mutex);
    // 10分钟清理过期幂等事务id
    if (_clean_finished_txn_cost.get_time() > FLAGS_clean_finished_txn_interval_us) {
        _finished_txn_map.read_background()->clear();
        _finished_txn_map.swap();
        _clean_finished_txn_cost.reset();
    }

    auto iter = _txn_map.begin();
    while (iter != _txn_map.end()) {
        auto txn = iter->second;
        bool clear = false;
        auto cur_time = butil::gettimeofday_us();

        //if (!txn->is_prepared() && cur_time - txn->last_active_time > clear_delay_ms * 1000LL) {
        // 事务太久没提交，就回滚掉
        if (cur_time - txn->last_active_time > clear_delay_ms * 1000LL) {
            DB_FATAL("TransactionFatal: txn %s is idle for %d ms, %ld, %ld, %ld, %ld",
                txn->get_txn()->GetName().c_str(), clear_delay_ms, 
                cur_time, 
                txn->last_active_time,
                cur_time - txn->last_active_time,
                clear_delay_ms);
            txn->rollback();
            iter = _txn_map.erase(iter);
            _txn_count--;
            clear = true;
        }
        if (clear == false) {
            iter++;
        }
    }
    return;
}

// rollback ALL un-prepared transactions when raft on_leader_stop callback is called
void TransactionPool::on_leader_stop_rollback() {
    std::unique_lock<std::mutex> lock(_map_mutex);
    auto iter = _txn_map.begin();
    while (iter != _txn_map.end()) {
        auto& txn = iter->second;
        if (!txn->is_prepared() && !txn->prepare_apply()) {
            DB_WARNING("TransactionNote: txn %s is rollback due to leader stop", 
                txn->get_txn()->GetName().c_str());
            txn->rollback();
            iter = _txn_map.erase(iter);
            _txn_count--;
        } else {
            iter++;
        }
    }
}

// rollback specific transaction when PREPARE apply failed due to leader stop
void TransactionPool::on_leader_stop_rollback(uint64_t txn_id) {
    std::unique_lock<std::mutex> lock(_map_mutex);
    if (_txn_map.count(txn_id) == 0) {
        return;
    }
    if (!_txn_map[txn_id]->is_prepared()) {
        DB_WARNING("TransactionNote: txn %s is rollback due to leader stop", 
            _txn_map[txn_id]->get_txn()->GetName().c_str());
        _txn_map[txn_id]->rollback();
        _txn_map.erase(txn_id);
        _txn_count--;
    }
}

int TransactionPool::on_shutdown_recovery(
        std::vector<rocksdb::Transaction*>& recovered_txns,
        std::unordered_map<uint64_t, pb::TransactionInfo>& prepared_txn) {

    std::string region_prefix = std::to_string(_region_id);
    //std::unique_lock<std::mutex> lock(_map_mutex);
    auto iter = recovered_txns.begin();
    while (iter != recovered_txns.end()) {
        if ((*iter) == nullptr) {
            DB_WARNING("txn is nullptr: %ld", _region_id);
            continue;
        }
        std::string txn_name = (*iter)->GetName();
        if (!boost::algorithm::starts_with(txn_name, region_prefix)) {
            iter++;
            continue;
        }
        uint64_t txn_id = strtoull(txn_name.substr(txn_name.find('_') + 1).c_str(), nullptr, 0);
        SmartTransaction txn(new (std::nothrow)Transaction(txn_id, this, _use_ttl));
        if (txn == nullptr) {
            DB_FATAL("TransactionError: new txn failed, txn_id: %lu", txn_id);
            return -1;
        }
        int ret = txn->begin(*iter);
        if (ret != 0) {
            DB_FATAL("TransactionError: begin txn failed, txn_id: %lu", txn_id);
            return -1;
        }
        auto txn_iter = prepared_txn.find(txn_id);
        if (txn_iter == prepared_txn.end()) {
            DB_FATAL("TransactionError: txn_increase_rows not found, region_id: %ld, txn_id: %lu", 
                _region_id, txn_id);
            return -1;
        }
        auto& txn_info = txn_iter->second;
        txn->num_increase_rows = txn_info.num_rows();
        txn->set_seq_id(txn_info.seq_id());
        for (auto& plan : txn_info.cache_plans()) {
            txn->cache_plan_map().insert({plan.seq_id(), plan});
        }
        _txn_map.insert(std::make_pair(txn_id, txn));
        _txn_count++;
        iter = recovered_txns.erase(iter);
        DB_WARNING("region_id: %ld, txn_id: %lu, txn_name: %s, num_rows: %ld, seq_id: %d, txn recovered", 
            _region_id,
            txn_id,
            txn_name.c_str(), 
            txn->num_increase_rows,
            txn->seq_id());
    }
    return 0;
}

// int TransactionPool::on_crash_recovery(
//         std::unordered_map<uint64_t, pb::TransactionInfo>& prepared_txn) {
//     return _region->replay_txn_for_recovery(prepared_txn);
// }

// 该函数执行时需要保证没有事务的修改操作
void TransactionPool::get_prepared_txn_info(
        std::unordered_map<uint64_t, pb::TransactionInfo>& prepared_txn,
        bool graceful_shutdown) {
    std::unique_lock<std::mutex> lock(_map_mutex);
    for (auto& pair : _txn_map) {
        auto txn = pair.second;
        if (!txn->is_prepared() || !txn->has_write()) {
            continue;
        }
        pb::TransactionInfo txn_info;
        txn_info.set_txn_id(pair.first);
        txn_info.set_seq_id(txn->seq_id());
        txn_info.set_start_seq_id(1);
        txn_info.set_optimize_1pc(false);
        CachePlanMap& cache_plan_map = txn->cache_plan_map();
        for (auto& cache_plan : cache_plan_map) {
            txn_info.add_cache_plans()->CopyFrom(cache_plan.second);
        }
        txn_info.set_num_rows(txn->num_increase_rows);
        DB_WARNING("region_id: %ld, txn_id: %lu, num_rows: %ld", 
            _region_id, pair.first, txn->num_increase_rows);
        prepared_txn.insert({pair.first, txn_info});
    }
    return;
}

void TransactionPool::update_txn_num_rows_after_split(const pb::TransactionInfo& txn_info) {
    uint64_t txn_id = txn_info.txn_id();
    if (_txn_map.count(txn_id) == 0) {
        return;
    }
    DB_WARNING("TransactionNote: region_id: %ld, txn_id: %lu, old_lines: %ld, dec_lines: %ld",
        _region_id, 
        txn_id, 
        _txn_map[txn_id]->num_increase_rows, 
        txn_info.num_rows());
    _txn_map[txn_id]->num_increase_rows -= txn_info.num_rows();
}
void TransactionPool::clear() {
    std::unique_lock<std::mutex> lock(_map_mutex);
    auto iter = _txn_map.begin();
    for (auto& txn : _txn_map) {
        DB_WARNING("TransactionNote: txn %s is rollback due to leader stop", 
            txn.second->get_txn()->GetName().c_str());
        txn.second->rollback();
    }
    _txn_map.clear();
    _txn_count = 0;
}
}
