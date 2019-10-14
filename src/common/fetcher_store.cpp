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

#include "fetcher_store.h"
#include <gflags/gflags.h>
#ifdef BAIDU_INTERNAL
#include <baidu/rpc/channel.h>
#else
#include <brpc/channel.h>
#endif
#include "network_socket.h"
#include "dml_node.h"

namespace baikaldb {

DECLARE_int32(retry_interval_us);
DECLARE_int32(single_store_concurrency);
DECLARE_int64(max_select_rows);
DECLARE_int64(print_time_us);
DEFINE_int32(fetcher_request_timeout, 100000,
                    "store as server request timeout, default:10000ms");
DEFINE_int32(fetcher_connect_timeout, 1000,
                    "store as server connect timeout, default:1000ms");
                    
ErrorType FetcherStore::send_request(
        RuntimeState* state,
        ExecNode* store_request,
        pb::RegionInfo& info,
        int64_t old_region_id, 
        int64_t region_id, 
        uint64_t log_id, 
        int retry_times, 
        int start_seq_id,
        int current_seq_id,
        pb::OpType op_type) {
    int64_t entry_ms = butil::gettimeofday_ms() % 1000;
    if (error != E_OK) {
        DB_WARNING("recieve error, need not requeset to region_id: %ld, log_id: %lu", region_id, log_id);
        return E_WARNING;
    }
    if (state->is_cancelled()) {
        DB_FATAL("region_id: %ld is cancelled, log_id: %lu", region_id, log_id);
        return E_OK;
    }
    //DB_WARNING("region_info; txn: %ld, %s, %lu", _txn_id, info.ShortDebugString().c_str(), records.size());
    if (retry_times >= 5) {
        DB_WARNING("region_id: %ld, txn_id: %lu, log_id:%lu rpc error; retry:%d", 
            region_id, state->txn_id, log_id, retry_times);
        return E_FATAL;    
    }
    auto rand_peer_func = [this](pb::RegionInfo& info) -> std::string {
        uint32_t i = butil::fast_rand() % info.peers_size();
        return info.peers(i);
    };
    auto other_peer_to_leader_func = [this, rand_peer_func](pb::RegionInfo& info) {
        DB_WARNING("region_id:%ld choose rand old leader:%s", info.region_id(), info.leader().c_str());
        auto peer = rand_peer_func(info);
        if (peer != info.leader()) {
            info.set_leader(peer);
            return;
        }
        for (auto& peer : info.peers()) {
            if (peer != info.leader()) {
                info.set_leader(peer);
                break;
            }
        }
    };
    // for exec next_statement_after_begin, begin must be added
    if (current_seq_id == 2 && state->single_sql_autocommit() == false) {
        DB_WARNING("start seq id is reset to 1, region_id: %ld", region_id);
        start_seq_id = 1;
    }
    {
        BAIDU_SCOPED_LOCK(state->client_conn()->region_lock);
        if (state->client_conn()->region_infos.count(region_id) == 0) {
            //DB_WARNING("start seq id is reset to 1, region_id: %ld", region_id);
            start_seq_id = 1;    
        }
    }
    TimeCost cost;
    auto client_conn = state->client_conn();
    SchemaFactory* schema_factory = SchemaFactory::get_instance();
    pb::StoreReq req;
    pb::StoreRes res;
    brpc::Controller cntl;
    cntl.set_log_id(log_id);
    if (info.leader() == "0.0.0.0:0" || info.leader() == "") {
        info.set_leader(rand_peer_func(info));
        //other_peer_to_leader_func(info);
    }
    req.set_db_conn_id(client_conn->get_global_conn_id());
    req.set_op_type(op_type);
    req.set_region_id(region_id);
    req.set_region_version(info.version());
    req.set_log_id(log_id);
    for (auto& desc : state->tuple_descs()) {
        req.add_tuples()->CopyFrom(desc);
    }

    pb::TransactionInfo* txn_info = req.add_txn_infos();
    txn_info->set_txn_id(state->txn_id);
    txn_info->set_seq_id(current_seq_id);
    txn_info->set_autocommit(state->single_sql_autocommit());
    for (int id : client_conn->need_rollback_seq) {
        txn_info->add_need_rollback_seq(id);
    }
    txn_info->set_start_seq_id(start_seq_id);
    txn_info->set_optimize_1pc(state->optimize_1pc());
    int64_t entry_ms2 = butil::gettimeofday_ms() % 1000;

    // DB_WARNING("txn_id: %lu, start_seq_id: %d, autocommit:%d", _txn_id, start_seq_id, state->autocommit());
    // 将缓存的plan中seq_id >= start_seq_id的部分追加到request中
    // rollback cmd does not need to send cache
    //DB_WARNING("op_type: %d, start_seq_id:%d, cache_plans_size: %d", 
    //            op_type, start_seq_id, client_conn->cache_plans.size());
    if (start_seq_id >= 0 && op_type != pb::OP_ROLLBACK && op_type != pb::OP_COMMIT) {
        for (auto& pair : client_conn->cache_plans) {
            //DB_WARNING("op_type: %d, pair.first:%d, start_seq_id:%d", op_type, pair.first, start_seq_id);
            auto& plan_item = pair.second;
            if (pair.first < start_seq_id || pair.first >= current_seq_id) {
                continue;
            }
            if (op_type == pb::OP_PREPARE && plan_item.op_type == pb::OP_PREPARE) {
                continue;
            }
            if (plan_item.tuple_descs.size() > 0 && 
                static_cast<DMLNode*>(plan_item.root)->global_index_id() != info.table_id()
                && plan_item.op_type != pb::OP_BEGIN) {
                DB_WARNING("TransactionNote: cache_item table_id mismatch,"
                    " cache global_index_id: %ld, region_info index_id : %ld",
                    static_cast<DMLNode*>(plan_item.root)->global_index_id(), info.table_id());
                continue;
            }
            pb::CachePlan* pb_cache_plan = txn_info->add_cache_plans();
            pb_cache_plan->set_op_type(plan_item.op_type);
            pb_cache_plan->set_seq_id(plan_item.sql_id);
            ExecNode::create_pb_plan(old_region_id, pb_cache_plan->mutable_plan(), plan_item.root);
            for (auto& desc : plan_item.tuple_descs) {
                pb_cache_plan->add_tuples()->CopyFrom(desc);
            }
        }
    }
    // save region id for txn commit/rollback
    int64_t client_lock_tm = 0;
    if (state->txn_id != 0) {
        TimeCost cost;
        BAIDU_SCOPED_LOCK(client_conn->region_lock);
        if (client_conn->region_infos.count(region_id) == 0) {
            client_conn->region_infos.insert(std::make_pair(region_id, info));
        }
        client_lock_tm = cost.get_time();
    }
    int64_t entry_ms3 = butil::gettimeofday_ms() % 1000;
    ExecNode::create_pb_plan(old_region_id, req.mutable_plan(), store_request);
    int64_t entry_ms4 = butil::gettimeofday_ms() % 1000;

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.max_retry = 1;
    option.connect_timeout_ms = FLAGS_fetcher_connect_timeout; 
    option.timeout_ms = FLAGS_fetcher_request_timeout;
    int ret = 0;
    std::string addr = info.leader();
    // 事务读也读leader
    if (op_type == pb::OP_SELECT && state->txn_id == 0) {
        // 多机房优化
        if (retry_times == 0) {
            choose_opt_instance(info, addr);
        }
        req.set_select_without_leader(true);
    }
    ret = channel.Init(addr.c_str(), &option);
    if (ret != 0) {
        DB_WARNING("channel init failed, addr:%s, ret:%d, region_id: %ld, log_id:%lu", 
                addr.c_str(), ret, region_id, log_id);
        return E_FATAL;
    }
    int64_t entry_ms5 = butil::gettimeofday_ms() % 1000;
    TimeCost query_time;
    pb::StoreService_Stub(&channel).query(&cntl, &req, &res, NULL);

    //DB_WARNING("fetch store req: %s", req.DebugString().c_str());
    //DB_WARNING("fetch store res: %s", res.DebugString().c_str());
    if (cost.get_time() > FLAGS_print_time_us || retry_times > 0) {
        DB_WARNING("entry_ms:%d, %d, %d, %d, %d, lock:%ld, wait region_id: %ld version:%ld time:%ld rpc_time: %ld log_id:%lu txn_id: %lu, ip:%s", 
                entry_ms, entry_ms2, entry_ms3, entry_ms4, entry_ms5, client_lock_tm, region_id, 
                info.version(), cost.get_time(), query_time.get_time(), log_id, state->txn_id,
                butil::endpoint2str(cntl.remote_side()).c_str());
    }
    if (cntl.Failed()) {
        DB_WARNING("call failed region_id: %ld, error:%s, log_id:%lu", 
                region_id, cntl.ErrorText().c_str(), log_id);
        other_peer_to_leader_func(info);
        //schema_factory->update_leader(info);
        bthread_usleep(retry_times * FLAGS_retry_interval_us);
        return send_request(state, store_request, info, old_region_id, region_id, log_id,
                  retry_times + 1, start_seq_id, current_seq_id, op_type);
    }
    if (res.errcode() == pb::NOT_LEADER) {
        int last_seq_id = res.has_last_seq_id()? res.last_seq_id() : 0;
        DB_WARNING("NOT_LEADER, region_id: %ld, retry:%d, new_leader:%s, log_id:%lu", 
                region_id, retry_times, res.leader().c_str(), log_id);

        if (res.leader() != "0.0.0.0:0") {
            info.set_leader(res.leader());
            schema_factory->update_leader(info);
            if (state->txn_id != 0 ) {
                BAIDU_SCOPED_LOCK(client_conn->region_lock);
                client_conn->region_infos[region_id].set_leader(res.leader());
            }
        } else {
            other_peer_to_leader_func(info);
        }
        bthread_usleep(retry_times * FLAGS_retry_interval_us);
        return send_request(state, store_request, info, old_region_id, region_id, log_id,
             retry_times + 1, last_seq_id + 1, current_seq_id, op_type);
    }
    if (res.errcode() == pb::TXN_FOLLOW_UP) {
        int last_seq_id = res.has_last_seq_id()? res.last_seq_id() : 0;
        DB_WARNING("TXN_FOLLOW_UP, region_id: %ld, retry:%d, log_id:%lu, op:%d, last_seq_id:%d", 
                region_id, retry_times, log_id, op_type, last_seq_id + 1);
        //对于commit，store返回TXN_FOLLOW_UP不能重发缓存命令，需要手工处理
        //对于rollback, 直接忽略返回成功
        //其他命令需要重发缓存
        if (op_type == pb::OP_COMMIT) {
            DB_FATAL("TransactionError: commit returns TXN_FOLLOW_UP: region_id: %ld, log_id:%lu, txn_id: %lu",
                region_id, log_id, state->txn_id);
            return E_FATAL;
        } else if (op_type == pb::OP_ROLLBACK) {
            return E_OK;
        }
        return send_request(state, store_request, info, old_region_id, region_id, log_id,
                  retry_times + 1, last_seq_id + 1, current_seq_id,  op_type);
    }
    //todo 需要处理分裂情况
    if (res.errcode() == pb::VERSION_OLD) {
        DB_WARNING("VERSION_OLD, region_id: %ld, retry:%d, now:%s, log_id:%lu", 
                region_id, retry_times, info.ShortDebugString().c_str(), log_id);
        if (res.regions_size() >= 2) {
            auto regions = res.regions();
            regions.Clear();
            for (auto r : res.regions()) {
                DB_WARNING("version region:%s, lod_id: %lu", r.ShortDebugString().c_str(), log_id);
                if (end_key_compare(r.end_key(), info.end_key()) > 0) {
                    DB_WARNING("region:%ld r.end_key:%s > info.end_key:%s, log_id: %lu", 
                            r.region_id(),
                            str_to_hex(r.end_key()).c_str(),
                            str_to_hex(info.end_key()).c_str(),
                            log_id);
                    continue;
                }
                *regions.Add() = r;
            }
            schema_factory->update_regions(regions);
            //auto orgin_info = res.regions(0);
            //auto new_info = res.regions(1);
            // 为了方便，串行执行
            // 靠store自己过滤数据
            //bthread_usleep(retry_times * FLAGS_retry_interval_us);
            if (op_type == pb::OP_PREPARE && client_conn->transaction_has_write()) {
                state->set_optimize_1pc(false);
                DB_WARNING("TransactionNote: disable optimize_1pc due to split: txn_id: %lu, seq_id: %d, region_id: %ld", 
                state->txn_id, current_seq_id, region_id);
            }
            for (auto& r : regions) {
                if (r.region_id() != region_id) {
                    BAIDU_SCOPED_LOCK(client_conn->region_lock);
                    client_conn->region_infos[r.region_id()] = r;
                } else {
                    if (res.leader() != "0.0.0.0:0") {
                        DB_WARNING("region_id: %ld set new_leader: %s when old_version", region_id, r.leader().c_str());
                        r.set_leader(res.leader());
                    }
                    BAIDU_SCOPED_LOCK(client_conn->region_lock);
                    client_conn->region_infos[region_id].set_end_key(r.end_key());
                    client_conn->region_infos[region_id].set_version(r.version());
                    if (r.leader() != "0.0.0.0:0") {
                        client_conn->region_infos[region_id].set_leader(r.leader());
                    }
                }
            }
            int last_seq_id = res.has_last_seq_id()? res.last_seq_id() : start_seq_id;
            for (auto& r : regions) {
                ErrorType ret;
                ret = send_request(state, store_request, r, old_region_id, r.region_id(), 
                         log_id, retry_times + 1, last_seq_id, current_seq_id, op_type);
                if (ret != E_OK) {
                    DB_WARNING("retry failed, region_id: %ld, log_id:%lu, txn_id: %lu", 
                            r.region_id(), log_id, state->txn_id);
                    return ret;
                }
            }
            return E_OK;
        }
        return E_FATAL;
    }
    if (res.errcode() == pb::REGION_NOT_EXIST || res.errcode() == pb::INTERNAL_ERROR) {
        DB_WARNING("REGION_NOT_EXIST, region_id:%ld, retry:%d, new_leader:%s, log_id:%lu", 
                region_id, retry_times, res.leader().c_str(), log_id);
        other_peer_to_leader_func(info);
        //bthread_usleep(retry_times * FLAGS_retry_interval_us);
        return send_request(state, store_request, info, old_region_id, region_id, log_id, 
                   retry_times + 1, start_seq_id, current_seq_id, op_type);
    }
    if (res.errcode() != pb::SUCCESS) {
        if (res.has_mysql_errcode()) {
            BAIDU_SCOPED_LOCK(region_lock);
            state->error_code = (MysqlErrCode)res.mysql_errcode();
            state->error_msg.str(res.errmsg());
        }
        DB_WARNING("errcode:%d, mysql_errcode:%d, msg:%s, failed, region_id:%ld, log_id:%lu", 
                res.errcode(), res.mysql_errcode(), res.errmsg().c_str(), region_id, log_id);
        if (state->error_code == ER_DUP_ENTRY) {
            return E_WARNING;
        }
        return E_FATAL;
    }

    if (res.records_size() > 0) {
        int64_t main_table_id = info.main_table_id();
        if (main_table_id <= 0) {
            DB_FATAL("impossible branch region_id:%ld, log_id:%lu", region_id, log_id);
            return E_FATAL;
        }
        std::map<int64_t, std::vector<SmartRecord>> result_records;
        SmartRecord record_template = schema_factory->new_record(main_table_id);
        for (auto& records_pair : res.records()) {
            int64_t index_id = records_pair.index_id();
            for (auto& str_record : records_pair.records()) {
                SmartRecord record = record_template->clone(false);
                auto ret = record->decode(str_record);
                if (ret < 0) {
                    DB_FATAL("decode to record fail, region_id:%ld, log_id:%lu", region_id, log_id);
                    return E_FATAL;
                }
                result_records[index_id].push_back(record);
            }
        }
        {
            BAIDU_SCOPED_LOCK(region_lock);
            for (auto& result_record : result_records) {
                int64_t index_id = result_record.first;
                index_records[index_id].insert(index_records[index_id].end(), result_record.second.begin(), result_record.second.end());
            }
        }
    }
    if (op_type != pb::OP_SELECT) {
        affected_rows += res.affected_rows();
        return E_OK;
    }
    if (res.leader() != "0.0.0.0:0" && res.leader() != "" && res.leader() != info.leader()) {
        info.set_leader(res.leader());
        schema_factory->update_leader(info);
        if (state->txn_id != 0) {
            BAIDU_SCOPED_LOCK(client_conn->region_lock);
            client_conn->region_infos[region_id].set_leader(res.leader());
        }
    }
    cost.reset();
    std::shared_ptr<RowBatch> batch = std::make_shared<RowBatch>();
    for (auto& pb_row : *res.mutable_row_values()) {
        std::unique_ptr<MemRow> row = state->mem_row_desc()->fetch_mem_row();
        for (int i = 0; i < res.tuple_ids_size(); i++) {
            int32_t tuple_id = res.tuple_ids(i);
            row->from_string(tuple_id, pb_row.tuple_values(i));
        }
        batch->move_row(std::move(row));
    }
    int64_t lock_tm = 0;
    {
        TimeCost lock;
        BAIDU_SCOPED_LOCK(region_lock);
        start_key_sort[info.start_key()] = region_id;
        region_batch[region_id] = batch;
        lock_tm= lock.get_time();
        row_cnt += batch->size();
        // TODO reduce mem used by streaming
        if ((!state->is_full_export) && (row_cnt > FLAGS_max_select_rows)) {
            DB_FATAL("_row_cnt:%ld > max_select_rows", row_cnt, FLAGS_max_select_rows);
            return E_BIG_SQL;
        }
    }
    if (cost.get_time() > FLAGS_print_time_us) {
        DB_WARNING("lock_tm:%ld, parse region:%ld time:%ld rows:%u log_id:%lu ", 
                lock_tm, region_id, cost.get_time(), batch->size(), log_id);
    }
    return E_OK;
}

void FetcherStore::choose_opt_instance(pb::RegionInfo& info, std::string& addr) {
    SchemaFactory* schema_factory = SchemaFactory::get_instance();
    std::string baikaldb_logical_room = schema_factory->get_logical_room();
    if (baikaldb_logical_room.empty()) {
        return;
    }
    std::vector<std::string> candicate_peers;
    for (auto& peer: info.peers()) {
        std::string logical_room = schema_factory->logical_room_for_instance(peer);
        if (!logical_room.empty()  && logical_room == baikaldb_logical_room) {
            candicate_peers.push_back(peer);
        }  
    }
    if (std::find(candicate_peers.begin(), candicate_peers.end(), addr) 
            != candicate_peers.end()) {
        return;
    }
    if (candicate_peers.size() > 0) {
        uint32_t i = butil::fast_rand() % candicate_peers.size();
        addr = candicate_peers[i];
    }
}

int FetcherStore::run(RuntimeState* state, 
                    std::map<int64_t, pb::RegionInfo>& region_infos, 
                    ExecNode* store_request, 
                    int start_seq_id,
                    int current_seq_id,
                    pb::OpType op_type) {
    //DB_WARNING("start_seq_id: %d, current_seq_id: %d op_type: %s", start_seq_id,
    //        current_seq_id, pb::OpType_Name(op_type).c_str());
    region_batch.clear();
    index_records.clear();
    start_key_sort.clear();
    error = E_OK;
    affected_rows = 0;
    row_cnt = 0;
    // 构造并发送请求
    std::map<std::string, std::set<int64_t>> send_region_ids_map; // leader ip => region_ids
    for (auto& pair : region_infos) {
        send_region_ids_map[pair.second.leader()].insert(pair.first);
    }
    uint64_t log_id = state->log_id();
    TimeCost cost;
    BthreadCond store_cond; // 不同store发请全并发
    for (auto& pair : send_region_ids_map) {
        store_cond.increase();
        auto store_thread = [this, state, store_request, pair, log_id, start_seq_id, current_seq_id,
                              &region_infos, &store_cond, op_type]() {
            ON_SCOPE_EXIT([&store_cond]{store_cond.decrease_signal();});
            BthreadCond cond(-FLAGS_single_store_concurrency); // 单store内并发数
            for (auto region_id : pair.second) {
                // 这两个资源后续不会分配新的，因此不需要加锁
                pb::RegionInfo* info = nullptr;
                if (region_infos.count(region_id) != 0) {
                    info = &region_infos[region_id];
                } else if (state->txn_id != 0) {
                    BAIDU_SCOPED_LOCK(state->client_conn()->region_lock);
                    info = &(state->client_conn()->region_infos[region_id]);
                }
                cond.increase();
                cond.wait();
                auto req_thread = [this, state, store_request, info, region_id, log_id, current_seq_id,
                          start_seq_id, &cond, op_type]() {
                    ON_SCOPE_EXIT([&cond]{cond.decrease_signal();});
                    auto ret = send_request(state, store_request, *info, region_id, region_id, log_id,
                           0, start_seq_id, current_seq_id, op_type);
                    if (ret != E_OK) {
                        DB_WARNING("rpc error, region_id:%ld, log_id:%lu", region_id, log_id);
                        error = ret;
                    }
                };
                Bthread bth(&BTHREAD_ATTR_SMALL);
                bth.run(req_thread);
            }
            cond.wait(-FLAGS_single_store_concurrency);
        };
        Bthread bth(&BTHREAD_ATTR_SMALL);
        bth.run(store_thread);
    }
    store_cond.wait();
    if (error != E_OK) {
        if (error == E_FATAL
                || error == E_BIG_SQL) {
            DB_FATAL("fetcher node open fail, log_id:%lu, txn_id: %lu, seq_id: %d op_type: %s", 
                    log_id, state->txn_id, current_seq_id, pb::OpType_Name(op_type).c_str());
            if (error == E_BIG_SQL) {
                state->error_code = ER_SQL_TOO_BIG;
                state->error_msg.str("sql too big");
            }
        } else {
            DB_WARNING("fetcher node open fail, log_id:%lu, txn_id: %lu, seq_id: %d op_type: %s", 
                    log_id, state->txn_id, current_seq_id, pb::OpType_Name(op_type).c_str());
        }
        return -1;
    }
    //DB_WARNING("fetcher time:%ld, txn_id: %lu, log_id:%lu, batch_size:%lu", 
    //        cost.get_time(), state->txn_id, log_id, region_batch.size());
    return affected_rows.load();
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
