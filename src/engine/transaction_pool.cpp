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

#include "rpc_sender.h"
#include "meta_server_interact.hpp"
#include "store.h"
#include "meta_writer.h"
#include "region.h"

namespace baikaldb {
//DECLARE_int32(rocks_transaction_expiration_ms);
DECLARE_int32(retry_interval_us);
DEFINE_int32(transaction_clear_delay_ms, 600 * 1000,
        "delay duration to clear prepared and expired transactions");
DEFINE_int32(long_live_txn_interval_ms, 900 * 1000,
        "delay duration to clear prepared and expired transactions");
DEFINE_int64(clean_finished_txn_interval_us, 600 * 1000 * 1000LL,
        "clean_finished_txn_interval_us");
DEFINE_int32(transaction_query_primary_region_interval_ms, 10 * 1000,
        "interval duration send request to primary region");

int TransactionPool::init(int64_t region_id, bool use_ttl) {
    _region_id = region_id;
    _use_ttl = use_ttl;
    _meta_writer = MetaWriter::get_instance();
    return 0;
}

// -1 means insert error (already exists)
int TransactionPool::begin_txn(uint64_t txn_id, SmartTransaction& txn, int64_t primary_region_id) {
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
        DB_WARNING("unknown error: %d, %s txn_id: %lu", res.code(), res.ToString().c_str(), txn_id);
    }
    if (primary_region_id > 0) {
        txn->set_primary_region_id(primary_region_id);
    }
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

void TransactionPool::txn_query_primary_region(SmartTransaction txn, Region* region,
            pb::RegionInfo& region_info) {
    pb::StoreReq request;
    pb::StoreRes response;
    request.set_op_type(pb::OP_TXN_QUERY_PRIMARY_REGION);
    request.set_region_id(region_info.region_id());
    request.set_region_version(region_info.version());
    pb::TransactionInfo* pb_txn = request.add_txn_infos();
    pb_txn->set_txn_id(txn->txn_id());
    pb_txn->set_seq_id(txn->seq_id());
    if (txn->is_finished()) {
        return ;
    } else if (txn->is_prepared()) {
        pb_txn->set_txn_state(pb::TXN_PREPARED);
    } else {
        pb_txn->set_txn_state(pb::TXN_BEGINED);
    }
    int retry_times = 1;
    bool success = false;
    do {
        if (txn->is_finished()) {
            break;
        }
        RpcSender::send_query_method(request, response, region_info.leader(), region_info.region_id());
        switch (response.errcode()) {
            case pb::SUCCESS: {
                auto txn_info = response.txn_infos(0);
                if (txn_info.txn_state() == pb::TXN_ROLLBACKED) {
                    txn_commit_through_raft(txn, region->region_info(), pb::OP_ROLLBACK);
                } else if (txn_info.txn_state() == pb::TXN_COMMITTED) {
                    txn_commit_through_raft(txn, region->region_info(), pb::OP_COMMIT);
                } else {
                    // primary没有查到rollback_tag，认为是commit，但是secondary不是PREPARE状态
                    DB_FATAL("primary committed, secondary need catchup log, region_id:%ld,"
                        "primary_region_id: %ld txn_id: %lu",
                        region->region_info().region_id(), region_info.region_id(), txn->txn_id());
                    success = true;
                    return;
                }
                remove_txn(txn->txn_id());
                success = true;
                DB_WARNING("send txn query success request:%s response: %s",
                    request.ShortDebugString().c_str(),
                    response.ShortDebugString().c_str());
                break;
            }
            case pb::NOT_LEADER: {
                if (response.leader() != "0.0.0.0:0") {
                    region_info.set_leader(response.leader());
                }
                DB_WARNING("send txn query NOT_LEADER , request:%s response: %s",
                    request.ShortDebugString().c_str(),
                    response.ShortDebugString().c_str());
                bthread_usleep(retry_times * FLAGS_retry_interval_us);
            break;
            }
            case pb::VERSION_OLD: {
                for (auto r : response.regions()) {
                    if (r.region_id() == region_info.region_id()) {
                        region_info.CopyFrom(r);
                        request.set_region_version(region_info.version());
                    }
                }
                DB_WARNING("send txn query VERSION_OLD , request:%s response: %s",
                    request.ShortDebugString().c_str(),
                    response.ShortDebugString().c_str());
                bthread_usleep(retry_times * FLAGS_retry_interval_us);
                break;
            }
            case pb::REGION_NOT_EXIST: {
                region_info = region->region_info();
                other_peer_to_leader(region_info);
                break;
            }
            case pb::TXN_IS_EXISTING: {
                DB_WARNING("send txn query TXN_IS_EXISTING , request:%s response: %s",
                    request.ShortDebugString().c_str(),
                    response.ShortDebugString().c_str());
                success = true;
                break;
            }
            default: {
                DB_WARNING("send txn query failed , request:%s response: %s",
                    request.ShortDebugString().c_str(),
                    response.ShortDebugString().c_str());
                bthread_usleep(retry_times * FLAGS_retry_interval_us);
                break;
            }
        }
        retry_times++;
    } while (!success && retry_times <= 5);
}

void TransactionPool::get_txn_state(const pb::StoreReq* request, pb::StoreRes* response) {
    std::unique_lock<std::mutex> lock(_map_mutex);
    for (auto txn_pair : _txn_map) {
        auto pb_txn = response->add_txn_infos();
        auto txn = txn_pair.second;
        pb_txn->set_txn_id(txn_pair.first);
        pb_txn->set_seq_id(txn->seq_id());
        pb_txn->set_primary_region_id(txn->primary_region_id());
        if (txn->is_rolledback()) {
            pb_txn->set_txn_state(pb::TXN_ROLLBACKED);
        } else if (txn->is_finished()) {
            pb_txn->set_txn_state(pb::TXN_COMMITTED);
        } else if (txn->is_prepared()) {
            pb_txn->set_txn_state(pb::TXN_PREPARED);
        } else {
            pb_txn->set_txn_state(pb::TXN_BEGINED);
        }
        auto cur_time = butil::gettimeofday_us();
        // seconds
        pb_txn->set_live_time((cur_time - txn->last_active_time) / 1000000LL);
    }
}

void TransactionPool::read_only_txn_process(SmartTransaction txn, pb::OpType op_type, bool optimize_1pc) {
    uint64_t txn_id = txn->txn_id();
    switch (op_type) {
        case pb::OP_PREPARE:
            if (optimize_1pc) {
                txn->rollback();
                remove_txn(txn_id);
            } else {
                txn->prepare();
            }
            break;
        case pb::OP_ROLLBACK:
            txn->rollback();
            remove_txn(txn_id);
            break;
        case pb::OP_COMMIT:
            txn->commit();
            remove_txn(txn_id);
            break;
        default:
            break;
    }
    DB_NOTICE("dml type: %s region_id: %ld, txn_id: %lu optimize_1pc:%d", pb::OpType_Name(op_type).c_str(),
            _region_id, txn_id, optimize_1pc);
}

void TransactionPool::txn_commit_through_raft(SmartTransaction txn,
            pb::RegionInfo& region_info,
            pb::OpType op_type) {
    // 只读事务       
    if (!txn->has_write()) {
        DB_WARNING("read-only txn rollback directly region_id:%ld txn_id:%ld:%d",
            region_info.region_id(), txn->txn_id(), txn->seq_id());
        txn->rollback();
        return;
    }
    pb::StoreReq request;
    pb::StoreRes response;
    request.set_op_type(op_type);
    request.set_region_id(region_info.region_id());
    request.set_region_version(region_info.version());
    pb::TransactionInfo* pb_txn = request.add_txn_infos();
    pb_txn->set_txn_id(txn->txn_id());
    pb_txn->set_seq_id(txn->seq_id() + 1);
    pb::Plan* plan = request.mutable_plan();
    pb::PlanNode* pb_node = plan->add_nodes();
    pb_node->set_node_type(pb::TRANSACTION_NODE);
    pb_node->set_limit(-1);
    pb_node->set_num_children(0);
    auto txn_node = pb_node->mutable_derive_node()->mutable_transaction_node();
    if (op_type == pb::OP_COMMIT) {
        txn_node->set_txn_cmd(pb::TXN_COMMIT_STORE);
    } else {
        txn_node->set_txn_cmd(pb::TXN_ROLLBACK_STORE);
    }
    int retry_times = 1;
    bool success = false;
    do {
        RpcSender::send_query_method(request, response, region_info.leader(), region_info.region_id());
        switch (response.errcode()) {
        case pb::SUCCESS:
        case pb::TXN_IS_ROLLBACK: {
            DB_WARNING("txn process success , request:%s response: %s",
                request.ShortDebugString().c_str(),
                response.ShortDebugString().c_str());
            success = true;
            break;
        }
        case pb::NOT_LEADER: {
            if (response.leader() != "0.0.0.0:0") {
                region_info.set_leader(response.leader());
            } else {
                other_peer_to_leader(region_info);
            }
            DB_WARNING("send txn commit NOT_LEADER , request:%s response: %s",
                request.ShortDebugString().c_str(),
                response.ShortDebugString().c_str());
            bthread_usleep(retry_times * FLAGS_retry_interval_us);
            break;
        }
        case pb::VERSION_OLD: {
            for (auto r : response.regions()) {
                if (r.region_id() == region_info.region_id()) {
                    region_info.CopyFrom(r);
                    request.set_region_version(region_info.version());
                } else {
                    txn_commit_through_raft(txn, r, op_type);
                }
            }
            DB_WARNING("send txn commit VERSION_OLD , request:%s response: %s",
                request.ShortDebugString().c_str(),
                response.ShortDebugString().c_str());
            bthread_usleep(retry_times * FLAGS_retry_interval_us);
            break;
        }
        case pb::REGION_NOT_EXIST: {
            // todo region_info信息可能需要查meta
            other_peer_to_leader(region_info);
            break;
        }
        default: {
            DB_WARNING("send txn commit failed , request:%s response: %s",
                request.ShortDebugString().c_str(),
                response.ShortDebugString().c_str());
            bthread_usleep(retry_times * FLAGS_retry_interval_us);
            break;
        }
        if (retry_times < 10) {
            retry_times++;
        }
        }
   } while (!success);
}

// 清理僵尸事务：包括长时间（clear_delay_ms）未更新的事务
void TransactionPool::clear_transactions(Region* region) {
    std::vector<SmartTransaction>  txns_need_reverse;
    std::vector<SmartTransaction>  primary_txns_need_clear;
    std::vector<SmartTransaction>  secondary_txns_need_clear;
    {
        std::unique_lock<std::mutex> lock(_map_mutex);
        // 10分钟清理过期幂等事务id
        if (_clean_finished_txn_cost.get_time() > FLAGS_clean_finished_txn_interval_us) {
            _finished_txn_map.read_background()->clear();
            _finished_txn_map.swap();
            _clean_finished_txn_cost.reset();
        }
        for (auto txn_pair :  _txn_map) {
            auto txn = txn_pair.second;
            auto cur_time = butil::gettimeofday_us();
            // 事务存在时间过长报警
            if (cur_time - txn->begin_time > FLAGS_long_live_txn_interval_ms *1000LL) {
                DB_FATAL("TransactionWarning: txn %s is alive for %d ms, %ld, %ld, %ld, %ld",
                     txn->get_txn()->GetName().c_str(), FLAGS_long_live_txn_interval_ms,
                     cur_time,
                     txn->begin_time,
                     cur_time - txn->begin_time,
                     FLAGS_long_live_txn_interval_ms * 1000);
            }
            // 10min未更新的primary region事务直接rollback
            if (txn->is_primary_region() &&
               (cur_time - txn->last_active_time > FLAGS_transaction_clear_delay_ms * 1000LL)) {
                primary_txns_need_clear.push_back(txn);
                DB_FATAL("TransactionFatal: primary txn %s is idle for %d ms, %ld, %ld, %ld, %ld",
                     txn->get_txn()->GetName().c_str(), FLAGS_transaction_clear_delay_ms,
                     cur_time,
                     txn->last_active_time,
                     cur_time - txn->last_active_time,
                     FLAGS_transaction_clear_delay_ms * 1000);
            } else if (cur_time - txn->last_active_time > FLAGS_transaction_clear_delay_ms * 1000LL) {
                secondary_txns_need_clear.push_back(txn);
                DB_FATAL("TransactionFatal: secondary txn %s is idle for %d ms, %ld, %ld, %ld, %ld",
                     txn->get_txn()->GetName().c_str(), FLAGS_transaction_clear_delay_ms,
                     cur_time,
                     txn->last_active_time,
                     cur_time - txn->last_active_time,
                     FLAGS_transaction_clear_delay_ms * 1000);               
            // 10s未更新的事务询问primary region事务状态
            } else if (cur_time - txn->last_active_time > FLAGS_transaction_query_primary_region_interval_ms * 1000LL) {
                if (txn->primary_region_id_seted()) {
                    txns_need_reverse.push_back(txn);
                }
            }
        }
    }

    // region只读事务rollback，否则等leader反查
    for (auto txn : secondary_txns_need_clear) {
        if (!txn->has_write() || txn->seq_id() <= 1) {
            DB_WARNING("read-only txn rollback directly region_id:%ld txn_id:%ld:%d",
                region->get_region_id(), txn->txn_id(), txn->seq_id());
            txn->rollback();
            remove_txn(txn->txn_id());
        }
    }
    auto it = primary_txns_need_clear.begin();
    while (it != primary_txns_need_clear.end()) {
        auto txn = *it;
        if (!txn->has_write() || txn->seq_id() <= 1) {
            DB_WARNING("read-only txn rollback directly region_id:%ld txn_id:%ld:%d",
                region->get_region_id(), txn->txn_id(), txn->seq_id());
            txn->rollback();
            remove_txn(txn->txn_id());
            it = primary_txns_need_clear.erase(it);
        } else {
            ++it;
        }        
    }

    if (!region->is_leader()) {
        return ;
    }
    // 只对primary region进行超时rollback
    for (auto txn : primary_txns_need_clear) {
        txn_commit_through_raft(txn, region->region_info(), pb::OP_ROLLBACK);
        remove_txn(txn->txn_id());
    }
    MetaServerInteract&   meta_server_interact = Store::get_instance()->get_meta_server_interact();
    for (auto txn : txns_need_reverse) {
        if (!txn->is_finished() && !txn->is_primary_region()) {
            pb::QueryRequest query_request;
            pb::QueryResponse query_response;
            query_request.set_op_type(pb::QUERY_REGION);
            query_request.set_region_id(txn->primary_region_id());
            if (meta_server_interact.send_request("query", query_request, query_response) != 0) {
                DB_FATAL("send query request to meta server fail primary_region_id: %ld "
                        "region_id:%ld txn_id: %lu seq_id: %d res: %s",
                    txn->primary_region_id(), region->get_region_id(), txn->txn_id(), txn->seq_id(),
                    query_response.ShortDebugString().c_str());
                if (query_response.errcode() == pb::REGION_NOT_EXIST) {
                    txn->rollback();
                    remove_txn(txn->txn_id());
                }
                continue;
            }
            pb::RegionInfo region_info = query_response.region_infos(0);
            txn_query_primary_region(txn, region, region_info);
        }
    }
    return;
}

void TransactionPool::on_leader_start_recovery(Region* region) {
    std::unique_lock<std::mutex> lock(_map_mutex);
    std::map<uint64_t, SmartTransaction> replay_txns;
    for (auto iter : _txn_map) {
        auto& txn = iter.second;
        if (txn->is_finished()) {
            continue;
        }
        // 兼容
        if (!txn->primary_region_id_seted()) {
            continue;
        }
        DB_WARNING("TransactionNote: txn %s need replay due to leader transfer seq_id:%d",
                txn->get_txn()->GetName().c_str(), txn->seq_id());
        replay_txns[txn->txn_id()] = txn;
    }
    if (replay_txns.size() > 0) {
        // 异步执行,释放raft线程
        auto replay_last_log_fun = [region, replay_txns] {
            region->recovery_when_leader_start(replay_txns);
            region->leader_start();
        };
        Bthread bth;
        bth.run(replay_last_log_fun);
    } else {
        region->leader_start();
    }
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
        // 事务所有指令都发送给新region
        pb::TransactionInfo txn_info;
        int ret = txn->get_cache_plan_infos(txn_info);
        if (ret < 0) {
            continue;
        }
        DB_WARNING("region_id: %ld, txn_id: %lu seq_id: %d num_rows: %ld primary_region_id: %ld",
            _region_id, pair.first, txn->seq_id(), txn->num_increase_rows, txn_info.primary_region_id());
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
    for (auto& txn : _txn_map) {
        DB_WARNING("TransactionNote: txn %s is rollback due to leader stop", 
            txn.second->get_txn()->GetName().c_str());
        txn.second->rollback();
    }
    _txn_map.clear();
    _txn_count = 0;
}
}
