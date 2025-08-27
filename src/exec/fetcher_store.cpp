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
#ifdef BAIDU_INTERNAL
#include <baidu/rpc/channel.h>
#include <baidu/rpc/selective_channel.h>
#else
#include <brpc/channel.h>
#include <brpc/selective_channel.h>
#endif
#include <gflags/gflags.h>
#include "binlog_context.h"
#include "query_context.h"
#include "dml_node.h"
#include "scan_node.h"
#include "trace_state.h"
#include "rocksdb_scan_node.h"
#include "arrow/util/byte_size.h"
#include "filter_node.h"

namespace baikaldb {

DEFINE_int64(retry_interval_us, 500 * 1000, "retry interval ");
DEFINE_int32(single_store_concurrency, 20, "max request for one store");
DEFINE_int64(max_select_rows, 10000000, "query will be fail when select too much rows");
DEFINE_int64(max_affected_rows, 10000000, "query will be fail when affect too much rows");
DEFINE_int64(print_time_us, 10000, "print log when time_cost > print_time_us(us)");
DEFINE_int64(baikaldb_alive_time_s, 10 * 60, "obervation time length in baikaldb, default:10 min");
BRPC_VALIDATE_GFLAG(print_time_us, brpc::NonNegativeInteger);
DEFINE_int32(fetcher_request_timeout, 100000,
                    "store as server request timeout, default:100000ms");
DEFINE_int32(fetcher_connect_timeout, 1000,
                    "store as server connect timeout, default:1000ms");
DEFINE_bool(fetcher_follower_read, true, "where allow follower read for fether");
DEFINE_bool(fetcher_learner_read, false, "where allow learner read for fether");
DEFINE_string(insulate_fetcher_resource_tag, "", "store read insulate resource_tag");
DEFINE_string(fetcher_resource_tag, "", "store read resource_tag perfered, only first time valid");
DECLARE_int32(transaction_clear_delay_ms);
DEFINE_bool(use_dynamic_timeout, false, "whether use dynamic_timeout");
BRPC_VALIDATE_GFLAG(use_dynamic_timeout, brpc::PassValidate);
DEFINE_bool(use_read_index, false, "whether use follower read");
DEFINE_bool(read_random_select_peer, false, "read random select peers");
DEFINE_int32(sql_exec_timeout, -1, "sql exec timeout. -1 means no limit");
BRPC_VALIDATE_GFLAG(sql_exec_timeout, brpc::PassValidate);
DEFINE_bool(enable_batch_rpc, false, "enable batch rpc");
DEFINE_int32(enable_batch_rpc_region_num, 5, "enable batch rpc region num");
DEFINE_bool(open_prefer_arrow_data, false, "open prefer arrow data in vectore execution");
bvar::Adder<int64_t> OnRPCDone::async_rpc_region_count {"async_rpc_region_count"};
bvar::LatencyRecorder OnRPCDone::total_send_request {"total_send_request"};
bvar::LatencyRecorder OnRPCDone::add_backup_send_request {"add_backup_send_request"};
bvar::LatencyRecorder OnRPCDone::has_backup_send_request {"has_backup_send_request"};
bvar::LatencyRecorder OnBatchRPCDone::batch_rpc_region_count {"batch_rpc_region_count"};

OnRPCDone::OnRPCDone(FetcherStore* fetcher_store, RuntimeState* state, 
                        ExecNode* store_request, int start_seq_id, int current_seq_id, 
                        pb::OpType op_type, bool need_check_memory) : 
    _fetcher_store(fetcher_store), _state(state), _store_request(store_request),
    _start_seq_id(start_seq_id),  _current_seq_id(current_seq_id), _op_type(op_type),
    _need_check_memory(need_check_memory) {
    _client_conn = _state->client_conn();
    if (_store_request->get_trace() != nullptr) {
        _trace_node = std::make_shared<pb::TraceNode>();
    }
    std::vector<ExecNode*> scan_nodes;
    // in主键, 全局索引 会有多个plan
    _store_request->get_node(pb::SCAN_NODE, scan_nodes);
    if (scan_nodes.size() > 0) {
        auto& scan_indexs = static_cast<RocksdbScanNode*>(scan_nodes[0])->scan_indexs();
        for (auto& scan_index_info : scan_indexs) {
            if (scan_index_info.index_id == scan_index_info.router_index_id 
                && scan_index_info.region_primary.size() > 0) {
                _has_multi_plan = true;
                break;
            }
        }
    }
}
OnRPCDone::~OnRPCDone() {
    if (!_has_multi_plan && _op_type == pb::OP_SELECT) {
        // batch rpc开始选地址时，没有AddAllocated
        if (_is_batch && _batch_request.plan_size() > 0) {
            _batch_request.mutable_plan()->ReleaseLast();
        } else if (!_is_batch) {
            _request.release_plan();
        }
    }
}
// 检查状态，判断是否需要继续执行
ErrorType OnRPCDone::check_status() {
    if (_fetcher_store->error != E_OK) {
        DB_DONE(WARNING, "recieve error, other region failed");
        return _fetcher_store->error;
    }

    if (_op_type != pb::OP_ROLLBACK) {
        if (_state->is_cancelled() || _fetcher_store->is_cancelled) {
            DB_DONE(FATAL, "cancelled, state cancel: %d, fetcher_store cancel: %d", 
                    _state->is_cancelled(), _fetcher_store->is_cancelled);
            return E_FATAL;
        }
    }

    if (_retry_times >= 5) {
        DB_DONE(WARNING, "too many retries");
        return E_FATAL;
    }

    return E_OK;
}

ErrorType OnRPCDone::fill_single_request(pb::StoreReq& single_req, pb::RegionInfo& region_info, int64_t region_id, int64_t old_region_id) {
    if (_trace_node != nullptr) {
        single_req.set_is_trace(true);
    }
    if (_state->explain_type == ANALYZE_STATISTICS) {
        pb::AnalyzeInfo* info = single_req.mutable_analyze_info();
        bool need_hist = false;
        if (_state->statistics_types == nullptr || _state->statistics_types->empty()) {
            // 缺失的话和老版本一致
            info->add_statistics_types(pb::StatisticType::ST_CMSKETCH);
            info->add_statistics_types(pb::StatisticType::ST_HISTOGRAM);
            need_hist = true;
        } else {
            for (auto statistic_type : *(_state->statistics_types)) {
                info->add_statistics_types(statistic_type);
                if (statistic_type == pb::StatisticType::ST_HISTOGRAM) {
                    need_hist = true;
                }
            }
        }
        
        if (_state->cmsketch != nullptr) {
            info->set_depth(_state->cmsketch->get_depth());
            info->set_width(_state->cmsketch->get_width());
            info->set_sample_rows(_state->cmsketch->get_sample_rows());
            info->set_table_rows(_state->cmsketch->get_table_rows());
        }

        if (need_hist) {
            info->set_sample_rows(_state->sample_rows);
            info->set_table_rows(_state->table_rows);
        }
    }
    // for exec next_statement_after_begin, begin must be added
    if (_current_seq_id == 2 && !_state->single_sql_autocommit()) {
        _start_seq_id = 1;
    }
    
    bool need_copy_cache_plan = true;
    if (_state->txn_id != 0) {
        BAIDU_SCOPED_LOCK(_client_conn->region_lock);
        if (_client_conn->region_infos.count(region_id) == 0) {
            _start_seq_id = 1;
            _fetcher_store->no_copy_cache_plan_set.emplace(region_id);
        }
        if (_fetcher_store->no_copy_cache_plan_set.count(region_id) != 0) {
            need_copy_cache_plan = false;
        }
    }
    
    if (region_info.leader() == "0.0.0.0:0" || region_info.leader() == "") {
        region_info.set_leader(rand_peer(region_info));
    }
    single_req.set_db_conn_id(_client_conn->get_global_conn_id());
    single_req.set_op_type(_op_type);
    single_req.set_region_id(region_id);
    single_req.set_region_version(region_info.version());
    single_req.set_log_id(_state->log_id());
    single_req.set_sql_sign(_state->sign);
    single_req.mutable_extra_req()->set_sign_latency(_fetcher_store->sign_latency);
    if (FLAGS_open_prefer_arrow_data 
            && _state->execute_type == pb::EXEC_ARROW_ACERO 
            && _fetcher_store->received_arrow_data) {
        // 后续肯定走向量化执行, store这边最好直接返回arrow格式, 避免中间额外序列化反序列化pb
        single_req.set_execute_type(pb::EXEC_ARROW_ACERO_PREFER_RETURN_ARROW_DATA);
    } else {
        single_req.set_execute_type(_state->execute_type);
    }
    for (auto& desc : _state->tuple_descs()) {
        if (desc.has_tuple_id()){
            single_req.add_tuples()->CopyFrom(desc);
        }
    }
    pb::TransactionInfo* txn_info = single_req.add_txn_infos();
    txn_info->set_txn_id(_state->txn_id);
    txn_info->set_seq_id(_current_seq_id);
    txn_info->set_autocommit(_state->single_sql_autocommit());
    for (int id : _client_conn->need_rollback_seq) {
        txn_info->add_need_rollback_seq(id);
    }
    txn_info->set_start_seq_id(_start_seq_id);
    txn_info->set_optimize_1pc(_state->optimize_1pc());
    if (_state->txn_id != 0) {
        txn_info->set_primary_region_id(_client_conn->primary_region_id.load());
        if (_fetcher_store->need_process_binlog(_state, _op_type)) {
            auto binlog_ctx = _client_conn->get_binlog_ctx();
            txn_info->set_commit_ts(binlog_ctx->get_last_commit_ts());
            txn_info->set_open_binlog(true);
        }
        if (_client_conn->primary_region_id != -1
            && _client_conn->primary_region_id != region_id
            && !_fetcher_store->primary_timestamp_updated) {
            if (butil::gettimeofday_us() - _client_conn->txn_pri_region_last_exec_time > (FLAGS_transaction_clear_delay_ms / 2) * 1000LL) {
                _fetcher_store->primary_timestamp_updated = true;
                txn_info->set_need_update_primary_timestamp(true);
                _client_conn->txn_pri_region_last_exec_time = butil::gettimeofday_us();
            }
        } else if (_client_conn->primary_region_id == region_id) {
            _client_conn->txn_pri_region_last_exec_time = butil::gettimeofday_us();
        }
    }

    // 将缓存的plan中seq_id >= start_seq_id的部分追加到request中
    // rollback cmd does not need to send cache
    if (_start_seq_id >= 0 && _op_type != pb::OP_COMMIT) {
        for (auto& pair : _client_conn->cache_plans) {
            //DB_WARNING("op_type: %d, pair.first:%d, start_seq_id:%d", op_type, pair.first, start_seq_id);
            auto& plan_item = pair.second;
            if ((plan_item.op_type != pb::OP_BEGIN)
                && (pair.first < _start_seq_id || pair.first >= _current_seq_id)) {
                continue;
            }
            if (!need_copy_cache_plan && plan_item.op_type != pb::OP_BEGIN
                && !_state->single_txn_cached()) {
                DB_DONE(DEBUG, "not copy cache");
                continue;
            }
            // rollback只带上begin
            if (_op_type == pb::OP_ROLLBACK && plan_item.op_type != pb::OP_BEGIN) {
                continue;
            }
            if (plan_item.tuple_descs.size() > 0 &&
                plan_item.op_type != pb::OP_BEGIN &&
                static_cast<DMLNode*>(plan_item.root)->global_index_id() != region_info.table_id()
                ) {
                continue;
            }
            pb::CachePlan* pb_cache_plan = txn_info->add_cache_plans();
            pb_cache_plan->set_op_type(plan_item.op_type);
            pb_cache_plan->set_seq_id(plan_item.sql_id);
            ExecNode::create_pb_plan(old_region_id, pb_cache_plan->mutable_plan(), plan_item.root);
            if (plan_item.op_type != pb::OP_BEGIN && !_state->single_txn_cached()) {
                DB_DONE(WARNING, "TranstationNote: copy cache, cache_plan:%s", pb_cache_plan->ShortDebugString().c_str());
            }
            for (auto& desc : plan_item.tuple_descs) {
                if (desc.has_tuple_id()){
                    pb_cache_plan->add_tuples()->CopyFrom(desc);
                }
            }
        }
    }
    // save region id for txn commit/rollback
    if (_state->txn_id != 0) {
        BAIDU_SCOPED_LOCK(_client_conn->region_lock);
        if (_client_conn->region_infos.count(region_id) == 0) {
            _client_conn->region_infos.insert(std::make_pair(region_id, region_info));
        }
    }

    if (!_is_batch) {
        std::vector<ExecNode*> scan_nodes;
        ScanNode* scan_node = nullptr;
        _store_request->get_node(pb::SCAN_NODE, scan_nodes);
        if (scan_nodes.size() == 1) {
            scan_node = static_cast<ScanNode*>(scan_nodes[0]);
        }

        // 需要对当前使用的router index id 加锁，可能由于存在全局二级索引backup，store_request在不同索引之间并发使用，需要区分当前处理的req属于哪个router index
        if (scan_node != nullptr) {
            bool use_global_backup = _fetcher_store->global_backup_type == GBT_LEARNER;
            scan_node->set_index_useage_and_lock(use_global_backup);
        }

        if (!_has_multi_plan && _op_type == pb::OP_SELECT) {
            if (_fetcher_store->shared_plan == nullptr) {
                _fetcher_store->shared_plan = std::make_shared<pb::Plan>();
                ExecNode::create_pb_plan(old_region_id, _fetcher_store->shared_plan.get(), _store_request);
                if (_need_check_memory) {
                    // 这里加内存, 最后整个请求析构的时候减掉
                    int64_t shared_request_size = _fetcher_store->shared_plan->ByteSizeLong();
                    if (shared_request_size > 0 
                            && 0 != _state->memory_limit_exceeded(std::numeric_limits<int>::max(), shared_request_size)) {
                        _fetcher_store->error = E_BIG_SQL;
                        DB_WARNING_STATE(_state, "Memory limit exceeded when add shared plan size: %ld.", shared_request_size);
                        return E_BIG_SQL;
                    }
                }
            }
            // 发送给不同的region, 共用同一个plan, 修改arena，需要注意这个函数调用
            single_req.set_allocated_plan(_fetcher_store->shared_plan.get());
        } else {
            ExecNode::create_pb_plan(old_region_id, single_req.mutable_plan(), _store_request);
        }

        if (scan_node != nullptr) {
            scan_node->current_index_unlock();
        }
    }

    if (_meta_id != 0) {
        if (del_meta_info(single_req) != 0) {
            DB_FATAL("Fail to del_meta_info");
            return E_FATAL;
        }
    }
    return E_OK;
}

// 指定访问resource_tag读从
void OnRPCDone::select_resource_insulate_read_addr(pb::RegionInfo& info,
                                                std::string& addr,
                                                const std::string& insulate_resource_tag,
                                                bool& select_without_leader,
                                                bool& resource_insulate_read) {
    // offline强制隔离
    // TODO:全局索引update也会有SELECT，查询不在事务会不会有问题
    _state->txn_id = 0;
    std::vector<std::string> valid_addrs;
    if (info.learners_size() > 0) {
        // 指定访问的resource tag, 可能是learner，可能是follower, 先判断有没有满足条件的learner
        select_valid_peers(info, insulate_resource_tag, info.learners(), valid_addrs);
    }

    // 事务读也读leader
    if (info.learners_size() > 0 &&
            ((FLAGS_fetcher_learner_read && valid_addrs.size() > 0)
            || _state->need_learner_backup()
            || _fetcher_store->global_backup_type == GBT_LEARNER/*全局索引降级，强制访问learner*/)) {
        // 指定了resource tag,没有可选learner, 在强制降级的情况下，忽略指定的resource tag
        if (valid_addrs.empty() 
            && (_state->need_learner_backup() || _fetcher_store->global_backup_type == GBT_LEARNER)) {
            select_valid_peers(info, "", info.learners(), valid_addrs);
        }
        if (valid_addrs.size() > 0) {
            // 有可选learner
            addr = valid_addrs[0];
            pb::Status addr_status = pb::NORMAL;
            FetcherStore::choose_opt_instance(info.region_id(), valid_addrs, "", addr, addr_status, nullptr);
            resource_insulate_read = true;
        } else {
            // 无可选learner
            pb::Status addr_status = pb::NORMAL;
            std::set<std::string> cannot_access_peers;
            _fetcher_store->peer_status.get_cannot_access_peer(info.region_id(), cannot_access_peers);
            FetcherStore::choose_opt_instance(info.region_id(), info.peers(), "", addr, addr_status, nullptr, cannot_access_peers);
        }
        select_without_leader = true;
        resource_insulate_read = true;
    } else if (FLAGS_fetcher_follower_read) {
        select_valid_peers(info, insulate_resource_tag, info.peers(), valid_addrs);
        pb::Status addr_status = pb::NORMAL;
        if (valid_addrs.size() > 0) {
            addr = valid_addrs[0];
            FetcherStore::choose_opt_instance(info.region_id(), valid_addrs, "", addr, addr_status, nullptr);
        } else {
            std::set<std::string> cannot_access_peers;
            _fetcher_store->peer_status.get_cannot_access_peer(info.region_id(), cannot_access_peers);
            FetcherStore::choose_opt_instance(info.region_id(), info.peers(), "", addr, addr_status, nullptr, cannot_access_peers);
        }
        select_without_leader = true;
        resource_insulate_read = true;
    } else if (_retry_times == 0) {
        // 重试前已经选择了normal的实例
        // 或者store返回了正确的leader
        FetcherStore::choose_other_if_dead(info, addr);
    }

    // 存在全局索引降级的情况，强制访问主集群的情况下不要backup
    if (_fetcher_store->global_backup_type == GBT_MAIN) {
        _backup.clear();
    }
}

void OnRPCDone::select_addr(pb::RegionInfo& info, 
                            std::string& addr,
                            bool& resource_insulate_read,
                            bool& select_without_leader) {
    addr = info.leader();
    resource_insulate_read = false; // 是否读learner，或者指定读从集群，进行资源隔离
    if (_state->need_learner_backup() && info.learners_size() == 0) {
        // 没有learner副本时报警
        DB_DONE(DEBUG, "has abnormal learner, learner size: 0");
    }
    // 是否指定访问资源隔离, 如offline
    std::string insulate_resource_tag = FLAGS_insulate_fetcher_resource_tag;
    if (_state->client_conn() != nullptr 
        && _state->client_conn()->user_info != nullptr
        && !_state->client_conn()->user_info->resource_tag.empty()) {
        insulate_resource_tag = _state->client_conn()->user_info->resource_tag;
    }
    if (!insulate_resource_tag.empty() && _op_type == pb::OP_SELECT) {
        return select_resource_insulate_read_addr(info, addr, 
                insulate_resource_tag, select_without_leader, resource_insulate_read);
    }

    if (_op_type == pb::OP_SELECT && _state->txn_id == 0 && info.learners_size() > 0 && 
        (FLAGS_fetcher_learner_read || _state->need_learner_backup()
            || _fetcher_store->global_backup_type == GBT_LEARNER/*全局索引降级，强制访问learner*/)) {
        std::vector<std::string> valid_learners;
        select_valid_peers(info, "", info.learners(), valid_learners);
        if (!valid_learners.empty()) {
            addr = valid_learners[0];
            pb::Status addr_status = pb::NORMAL;
            FetcherStore::choose_opt_instance(info.region_id(), valid_learners, "", addr, addr_status, nullptr);
            resource_insulate_read = true;
        } else {
            pb::Status addr_status = pb::NORMAL;
            FetcherStore::choose_opt_instance(info.region_id(), info.peers(), "", addr, addr_status, nullptr);
        }
        select_without_leader = true;
    } else if (_op_type == pb::OP_SELECT && _state->txn_id == 0 
            && _client_conn != nullptr
            && _client_conn->query_ctx->peer_index != -1) {
        int64_t peer_index = _client_conn->query_ctx->peer_index;
        std::vector<std::string> sorted_peers; // leader first
        sorted_peers.emplace_back(info.leader());
        SchemaFactory* schema_factory = SchemaFactory::get_instance();
        for (auto& peer: info.peers()) {
            if (info.leader() != peer) {
                sorted_peers.emplace_back(peer);
            }
        }
        if (_retry_times == 0) {
            if (peer_index < sorted_peers.size()) {
                addr = sorted_peers[peer_index];
                FetcherStore::choose_other_if_dead(info, addr);
                DB_WARNING("choose peer %s, index: %ld", addr.c_str(), peer_index);
            }
        }
        select_without_leader = true;
    } else if (_op_type == pb::OP_SELECT && _state->txn_id == 0 && FLAGS_fetcher_follower_read) {
        // 多机房优化
        if (info.learners_size() > 0) {
            pb::Status addr_status = pb::NORMAL;
            FetcherStore::choose_opt_instance(info.region_id(), info.peers(), "", addr, addr_status, nullptr);
            pb::Status backup_status = pb::NORMAL;
            FetcherStore::choose_opt_instance(info.region_id(), info.learners(), "", _backup, backup_status, nullptr);
            bool backup_can_access = (!_backup.empty()) && (backup_status == pb::NORMAL) && 
                                        _fetcher_store->peer_status.can_access(info.region_id(), _backup);
            if (addr_status != pb::NORMAL && backup_can_access && 
                    _fetcher_store->global_backup_type != GBT_MAIN/*全局索引降级，强制访问主集群不可以只访问learner*/) {
                addr = _backup;
                _backup.clear();
                _state->need_statistics = false;
                resource_insulate_read = true;
            } else if (!backup_can_access) {
                _backup.clear();
            }
        } else {
            if (_retry_times == 0) {
                pb::Status addr_status = pb::NORMAL;
                bool read_random_select_peer = FLAGS_read_random_select_peer;
                if (_state->need_read_rolling) {
                    read_random_select_peer = true;
                }
                FetcherStore::choose_opt_instance(info.region_id(), info.peers(), FLAGS_fetcher_resource_tag, addr, addr_status, &_backup, {}, read_random_select_peer);
            }
        }
        select_without_leader = true;
    } else if (_retry_times == 0) {
        // 重试前已经选择了normal的实例
        // 或者store返回了正确的leader
        FetcherStore::choose_other_if_dead(info, addr);
    }

    // 存在全局索引降级的情况，强制访问主集群的情况下不要backup
    if (_fetcher_store->global_backup_type == GBT_MAIN) {
        _backup.clear();
    }
}

ErrorType OnRPCDone::send_async() {
    _cntl.Reset();
    _cntl.set_log_id(_state->log_id());
    if (_is_batch) {
        for (auto& region_id : _region_ids) {
            if (region_id == 0) {
                DB_DONE(FATAL, "region_id == 0");
                return E_FATAL;
            }
        }
    } else if (!_is_batch && _region_id == 0) {
        DB_DONE(FATAL, "region_id == 0");
        return E_FATAL;
    }
    brpc::ChannelOptions option;
    option.max_retry = 1;
    option.connect_timeout_ms = FLAGS_fetcher_connect_timeout;
    option.timeout_ms = FLAGS_fetcher_request_timeout;
    if (!_is_batch && _fetcher_store->dynamic_timeout_ms > 0 && !_backup.empty() && _backup != _addr) {
        option.backup_request_ms = _fetcher_store->dynamic_timeout_ms;
    }

    pb::OpType op_type;
    if (_is_batch) {
        op_type = _batch_request.store_req(0).op_type();
    } else {
        op_type = _request.op_type();
    }
    if (!_state->is_ddl_work() && op_type == pb::OP_SELECT && _state->explain_type == EXPLAIN_NULL) {
        int32_t sql_exec_time_left = FLAGS_fetcher_request_timeout;
        if (FLAGS_sql_exec_timeout > 0) {
            int64_t sql_exec_time_left = std::min(FLAGS_sql_exec_timeout - _state->get_cost_time() / 1000,
                                                  FLAGS_fetcher_request_timeout * 1L);
            if (sql_exec_time_left <= 0) {
                DB_WARNING("logid: %lu, sql exec timeout, op_type: %d, total_cost: %ld", _state->log_id(),
                           op_type, _state->get_cost_time());
                return E_FATAL;
            }
        }
        if (_is_batch) {
            for (int i = 0; i < _batch_request.store_req_size(); i++) {
                _batch_request.mutable_store_req(i)->set_sql_exec_timeout(sql_exec_time_left);
            }
        } else {
            _request.set_sql_exec_timeout(sql_exec_time_left);
        }
        option.timeout_ms = sql_exec_time_left;
    }
    if (_is_batch) {
        auto& batch_response = *_batch_response_ptr;
        batch_response.Clear();
        brpc::Channel channel;
        int ret = 0;
        ret = channel.Init(_addr.c_str(), &option);
        if (ret != 0) {
            DB_WARNING("channel init failed, addr:%s, ret:%d, batch region_id: %s, log_id:%lu",
                    _addr.c_str(), ret, _region_ids_str.c_str(), _state->log_id());
            return E_FATAL;
        }
        _fetcher_store->insert_callid(_cntl.call_id());
        _query_time.reset();
        pb::StoreService_Stub(&channel).query_batch(&_cntl, &_batch_request, &batch_response, this);
    } else {
        auto& response = *_response_ptr;
        response.Clear();
        // SelectiveChannel在init时会出core,开源版先注释掉
    #ifdef BAIDU_INTERNAL
        brpc::SelectiveChannel channel;
        int ret = channel.Init("rr", &option);
        if (ret != 0) {
            DB_DONE(WARNING, "SelectiveChannel init failed, ret:%d", ret);
            return E_FATAL;
        }
        // sub_channel do not need backup_request_ms
        option.backup_request_ms = -1;
        option.max_retry = 0;
        brpc::Channel* sub_channel1 = new brpc::Channel;
        ret = sub_channel1->Init(_addr.c_str(), &option);
        if (ret != 0) {
            DB_DONE(WARNING, "channel init failed, ret:%d", ret);
            delete sub_channel1;
            return E_FATAL;
        }
        channel.AddChannel(sub_channel1, NULL);
        if (_fetcher_store->dynamic_timeout_ms > 0 && !_backup.empty() && _backup != _addr) {
            //开源版brpc和内部不大一样
            brpc::SocketId sub_id2;
            brpc::Channel* sub_channel2 = new brpc::Channel;
            ret = sub_channel2->Init(_backup.c_str(), &option);
            if (ret != 0) {
                DB_DONE(WARNING, "sub_channel init failed, ret:%d", ret);
                delete sub_channel2;
                return E_FATAL;
            }
            channel.AddChannel(sub_channel2, &sub_id2);
            // SelectiveChannel返回的sub_id2可以当做ExcludedServers使用
            // 保证第一次请求必定走addr，并且这个ExcludedServers不影响backup_request
            brpc::ExcludedServers* exclude = brpc::ExcludedServers::Create(1);
            exclude->Add(sub_id2);
            _cntl.set_excluded_servers(exclude);
        } else {
            //命中backup可以不cancel
            _client_conn->insert_callid(_addr, _region_id, _cntl.call_id());
        }
    #else
        brpc::Channel channel;
        int ret = 0;
        ret = channel.Init(_addr.c_str(), &option);
        if (ret != 0) {
            DB_WARNING("channel init failed, addr:%s, ret:%d, region_id: %ld, log_id:%lu",
                _addr.c_str(), ret, _region_id, _state->log_id());
            return E_FATAL;
        }
    #endif
        _fetcher_store->insert_callid(_cntl.call_id());
        _query_time.reset();
        pb::StoreService_Stub(&channel).query(&_cntl, &_request, &response, this);
    }
    return E_ASYNC;
}

void OnRPCDone::Run(const std::string& version) {
    std::string remote_side = butil::endpoint2str(_cntl.remote_side()).c_str();
    int64_t query_cost = _query_time.get_time();
    if (_is_batch) {
        DB_DONE(DEBUG, "fetch store batch req: %s", _batch_request.ShortDebugString().c_str());
        DB_DONE(DEBUG, "fetch store batch res: %s", _batch_response_ptr->ShortDebugString().c_str());
        if ((query_cost > FLAGS_print_time_us || _retry_times > 0) && _batch_response_ptr->store_res_size() > 0) {
            DB_DONE(WARNING, "batch version:%s time:%ld rpc_time:%ld ip:%s, dynamic_timeout_ms:%ld vectorize:[%d, %d]",
                version.substr(0, 100).c_str(), _total_cost.get_time(), query_cost, remote_side.c_str(),
                _fetcher_store->dynamic_timeout_ms, _batch_request.store_req(0).execute_type(), _batch_response_ptr->store_res(0).execute_type());
        }
    } else {
        DB_DONE(DEBUG, "fetch store req: %s", _request.ShortDebugString().c_str());
        DB_DONE(DEBUG, "fetch store res: %s", _response_ptr->ShortDebugString().c_str());
        if (query_cost > FLAGS_print_time_us || _retry_times > 0) {
            DB_DONE(WARNING, "version:%s time:%ld rpc_time:%ld ip:%s, dynamic_timeout_ms:%ld vectorize:[%d, %d]",
                    version.c_str(), _total_cost.get_time(), query_cost, remote_side.c_str(), 
                    _fetcher_store->dynamic_timeout_ms, _request.execute_type(), _response_ptr->execute_type());
        }
    }
    total_send_request << query_cost;
    if (!_backup.empty() && _backup != _addr) {
        add_backup_send_request << query_cost;
    }
    SchemaFactory* schema_factory = SchemaFactory::get_instance();
    if (_cntl.Failed()) {
        DB_DONE(WARNING, "call failed, errcode:%d, error:%s", _cntl.ErrorCode(), _cntl.ErrorText().c_str());
        schema_factory->update_instance(remote_side, pb::FAULTY, false, false);
        // 只有网络相关错误码才重试
        if (!FetcherStore::rpc_need_retry(_cntl.ErrorCode())) {
            _fetcher_store->error = E_FATAL;
            _rpc_ctrl->task_finish(this);
            return;
        }
        if (_op_type != pb::OP_SELECT && _cntl.ErrorCode() == ECANCELED) {
            _fetcher_store->error = E_FATAL;
            _rpc_ctrl->task_finish(this);
            return;
        }
        if (_op_type == pb::OP_SELECT && _cntl.ErrorCode() == ECANCELED && _resource_insulate_read) {
            _fetcher_store->error = E_FATAL;
            _rpc_ctrl->task_finish(this);
            return;
        }

        retry_region_task(remote_side);
        return;
    }

    // 如果已经失败或取消则不再处理
    if (_fetcher_store->error != E_OK) {
        _rpc_ctrl->task_finish(this);
        return;
    } 

    if (_state->is_cancelled() || _fetcher_store->is_cancelled) {
        DB_DONE(WARNING, "rpc cancelled, state cancel: %d, fetcher store cancel: %d", 
            _state->is_cancelled(), _fetcher_store->is_cancelled);
        _fetcher_store->error = E_FATAL;
        _rpc_ctrl->task_finish(this);
        return;
    }

    retry_or_finish_task(remote_side);
    return;
}

ErrorType OnRPCDone::handle_version_old(const pb::RegionInfo& info, pb::StoreRes& response) {
    int64_t region_id = info.region_id();
    SchemaFactory* schema_factory = SchemaFactory::get_instance();
    DB_DONE(WARNING, "VERSION_OLD, now:%s", info.ShortDebugString().c_str());
    if (response.regions_size() >= 2) {
        auto regions = response.regions();
        regions.Clear();
        if (!response.is_merge()) {
            for (auto r : response.regions()) {
                DB_WARNING("version region:%s", r.ShortDebugString().c_str());
                if (end_key_compare(r.end_key(), info.end_key()) > 0) {
                    DB_WARNING("region:%ld r.end_key:%s > info.end_key:%s",
                                r.region_id(),
                                str_to_hex(r.end_key()).c_str(),
                                str_to_hex(info.end_key()).c_str());
                    continue;
                }
                *regions.Add() = r;
            }
        } else {
            //merge场景，踢除当前region，继续走下面流程
            for (auto r : response.regions()) {
                if (r.region_id() == region_id) {
                    DB_WARNING("merge can`t add this region:%s",
                                r.ShortDebugString().c_str());
                    continue;
                }
                DB_WARNING("version region:%s", r.ShortDebugString().c_str());
                *regions.Add() = r;
            }
        }
        schema_factory->update_regions(regions);
        if (_op_type == pb::OP_PREPARE && _client_conn->transaction_has_write()) {
            _state->set_optimize_1pc(false);
            DB_DONE(WARNING, "TransactionNote: disable optimize_1pc due to split");
        }
        for (auto& r : regions) {
            if (r.region_id() != region_id) {
                BAIDU_SCOPED_LOCK(_client_conn->region_lock);
                _client_conn->region_infos[r.region_id()] = r;
                _fetcher_store->skip_region_set.insert(r.region_id());
                _fetcher_store->region_count++;
            } else {
                if (response.leader() != "0.0.0.0:0") {
                    DB_WARNING("region_id: %ld set new_leader: %s when old_version", region_id, r.leader().c_str());
                    r.set_leader(response.leader());
                }
                BAIDU_SCOPED_LOCK(_client_conn->region_lock);
                _client_conn->region_infos[region_id] = r;
                if (r.leader() != "0.0.0.0:0") {
                    _client_conn->region_infos[region_id].set_leader(r.leader());
                }
            }
        }
        int last_seq_id = response.has_last_seq_id()? response.last_seq_id() : _start_seq_id;
        for (auto& r : regions) {
            pb::RegionInfo* info_ptr = nullptr;
            {       
                BAIDU_SCOPED_LOCK(_client_conn->region_lock);
                info_ptr = &(_client_conn->region_infos[r.region_id()]);
            }
            auto task = new OnSingleRPCDone(_fetcher_store, _state, _store_request, info_ptr, 
                    _old_region_id, info_ptr->region_id(), last_seq_id, _current_seq_id, 
                    _op_type, _need_check_memory);
            _rpc_ctrl->add_new_task(task);
        }
        return E_OK;
    } else if (response.regions_size() == 1) {
        auto regions = response.regions();
        regions.Clear();
        for (auto r : response.regions()) {
            if (r.region_id() != region_id) {
                DB_WARNING("not the same region:%s",
                            r.ShortDebugString().c_str());
                return E_FATAL;
            }
            if (!(r.start_key() <= info.start_key() &&
                    end_key_compare(r.end_key(), info.end_key()) >= 0)) {
                DB_FATAL("store region not overlap local region, region_id:%ld",
                        region_id);
                return E_FATAL;
            }
            DB_WARNING("version region:%s", r.ShortDebugString().c_str());
            *regions.Add() = r;
        }
        int last_seq_id = response.has_last_seq_id()? response.last_seq_id() : _start_seq_id;
        for (auto& r : regions) {
            pb::RegionInfo* info_ptr = nullptr;
            {
                BAIDU_SCOPED_LOCK(_client_conn->region_lock);
                _client_conn->region_infos[r.region_id()] = r;
                if (r.leader() != "0.0.0.0:0") {
                    _client_conn->region_infos[r.region_id()].set_leader(r.leader());
                }
                info_ptr = &(_client_conn->region_infos[r.region_id()]);
            }
            auto task = new OnSingleRPCDone(_fetcher_store, _state, _store_request, info_ptr, 
                        _old_region_id, info_ptr->region_id(), last_seq_id, _current_seq_id,
                        _op_type, _need_check_memory);
            _rpc_ctrl->add_new_task(task);
        }
        return E_OK;
    }
    return E_FATAL;
}

ErrorType OnRPCDone::handle_single_response(const std::string& remote_side,
                                            int64_t region_id,
                                            const std::string& addr,
                                            pb::RegionInfo& info,
                                            std::shared_ptr<pb::StoreRes> single_response_ptr) {
    pb::StoreRes& single_response = *single_response_ptr;
    _region_id = region_id;
    if (_meta_id != 0) {
        if (add_meta_info(single_response, _meta_id) != 0) {
            DB_FATAL("Fail to del_meta_info");
            return E_FATAL;
        }
    }
    SchemaFactory* schema_factory = SchemaFactory::get_instance();
    if (!_is_batch && _cntl.has_backup_request()) {
        DB_DONE(WARNING, "has_backup_request, rpc_time:%ld ip:%s, dynamic_timeout_ms:%ld", 
                _query_time.get_time(), remote_side.c_str(), _fetcher_store->dynamic_timeout_ms);
        has_backup_send_request << _query_time.get_time();
        // backup先回，整体时延包含dynamic_timeout_ms，不做统计
        // remote_side != addr 说明backup先回
        if (remote_side != addr) {
            _state->need_statistics = false;
            // backup为learner需要设置_resource_insulate_read为true
            if (info.learners_size() > 0) {
                for (auto& peer : info.learners()) {
                    if (peer == remote_side) {
                        _resource_insulate_read = true;
                        break;
                    }
                }
            }
        }
    } else {
        if (single_response.errcode() != pb::SUCCESS) {
            // 失败请求会重试，可能统计的时延不准，不做统计
            _state->need_statistics = false;
        } else {
            // 请求结束再次判断请求的实例状态，非NORMAL则时延不可控，不做统计
            if (_state->need_statistics) {
                auto status = SchemaFactory::get_instance()->get_instance_status(addr);
                if (status.status != pb::NORMAL) {
                    _state->need_statistics = false;
                }
            }
        }
    }
    // 使用read_index、指定访问store集群进行资源隔离、访问learner，读失败，不重试leader
    if (_resource_insulate_read 
            && (single_response.errcode() == pb::REGION_NOT_EXIST 
                || single_response.errcode() == pb::LEARNER_NOT_READY 
                || single_response.errcode() == pb::NOT_LEADER)) {
        DB_DONE(WARNING, "peer/learner not ready, errcode: %s, errmsg: %s", 
                pb::ErrCode_Name(single_response.errcode()).c_str(), single_response.errmsg().c_str());
        _fetcher_store->peer_status.set_cannot_access(info.region_id(), addr);
        bthread_usleep(_retry_times * FLAGS_retry_interval_us);
        return E_RETRY;
    }
    // 要求读主、store version old、store正在shutdown/init，在leader重试
    if (single_response.errcode() == pb::NOT_LEADER) {
        // 兼容not leader报警，匹配规则 NOT_LEADER.*retry:4
        DB_DONE(WARNING, "NOT_LEADER, new_leader:%s, retry:%d, errmsg: %s", 
            single_response.leader().c_str(), _retry_times, single_response.errmsg().c_str());
        // 临时修改，后面改成store_access
        if (_retry_times > 1 && single_response.leader() == "0.0.0.0:0") {
            schema_factory->update_instance(remote_side, pb::FAULTY, false, false);
        }
        if (single_response.leader() != "0.0.0.0:0" && single_response.leader() != "") {
            // store返回了leader，则相信store，不判断normal
            info.set_leader(single_response.leader());
            schema_factory->update_leader(info);
        } else {
            FetcherStore::other_normal_peer_to_leader(info, addr);
           
        }
        if (_state->txn_id != 0 ) {
            BAIDU_SCOPED_LOCK(_client_conn->region_lock);
            _client_conn->region_infos[region_id].set_leader(info.leader());
        }
        // leader切换在秒级
        bthread_usleep(_retry_times * FLAGS_retry_interval_us);
        return E_RETRY;
    }
    if (single_response.errcode() == pb::RETRY_LATER) {
        DB_DONE(WARNING, "request failed, errcode: %s, errmsg: %s", pb::ErrCode_Name(single_response.errcode()).c_str(), single_response.errmsg().c_str());
        if (FLAGS_fetcher_follower_read) {
            // choose another peer to retry
            schema_factory->update_instance(remote_side, pb::BUSY, false, false);
            FetcherStore::other_normal_peer_to_leader(info, addr);
        }
        bthread_usleep(_retry_times * FLAGS_retry_interval_us);
        return E_RETRY;
    }
    if (single_response.errcode() == pb::DISABLE_WRITE_TIMEOUT || single_response.errcode() == pb::IN_PROCESS) {
        DB_DONE(WARNING, "request failed, errcode: %s", pb::ErrCode_Name(single_response.errcode()).c_str());
        bthread_usleep(_retry_times * FLAGS_retry_interval_us);
        return E_RETRY;
    }

    if (single_response.errcode() == pb::VERSION_OLD) {
        return handle_version_old(info, single_response);
    }
    if (single_response.errcode() == pb::TXN_IS_ROLLBACK) {
        DB_DONE(WARNING, "TXN_IS_ROLLBACK, new_leader:%s", single_response.leader().c_str());
        return E_RETURN;
    }
    if (single_response.errcode() == pb::REGION_NOT_EXIST || single_response.errcode() == pb::INTERNAL_ERROR) {
        DB_DONE(WARNING, "new_leader:%s, errcode: %s", single_response.leader().c_str(), pb::ErrCode_Name(single_response.errcode()).c_str());
        if (single_response.errcode() == pb::REGION_NOT_EXIST) {
            pb::RegionInfo tmp_info;
            // 已经被merge了并且store已经删掉了，按正常处理
            int ret = schema_factory->get_region_info(info.table_id(), region_id, tmp_info);
            if (ret != 0) {
                DB_DONE(WARNING, "REGION_NOT_EXIST, region merge, new_leader:%s", single_response.leader().c_str());
                return E_OK;
            }
            // backup先回REGION_NOT_EXIST, 清空_backup 
            if (remote_side == _backup) {
                _backup.clear(); 
            }
        }
        schema_factory->update_instance(remote_side, pb::FAULTY, false, false);
        FetcherStore::other_normal_peer_to_leader(info, addr);
        return E_RETRY;
    }
    if (single_response.errcode() != pb::SUCCESS) {
        if (single_response.has_mysql_errcode()) {
            BAIDU_SCOPED_LOCK(_fetcher_store->region_lock);
            _fetcher_store->error_code = (MysqlErrCode)single_response.mysql_errcode();
            _fetcher_store->error_msg.str(single_response.errmsg());
        }
        DB_DONE(WARNING, "errcode:%s, mysql_errcode:%d, msg:%s, failed",
                pb::ErrCode_Name(single_response.errcode()).c_str(), single_response.mysql_errcode(), single_response.errmsg().c_str());
        if (_fetcher_store->error_code == ER_DUP_ENTRY) {
            return E_WARNING;
        }
        return E_FATAL;
    }

    if (single_response.records_size() > 0) {
        int64_t main_table_id = info.has_main_table_id() ? info.main_table_id() : info.table_id();
        if (main_table_id <= 0) {
            DB_DONE(FATAL, "impossible branch");
            return E_FATAL;
        }
        std::map<int64_t, std::vector<SmartRecord>> result_records;
        std::vector<std::string> return_str_records;
        std::vector<std::string> return_str_old_records;
        SmartRecord record_template = schema_factory->new_record(main_table_id);
        for (auto& records_pair : single_response.records()) {
            int64_t index_id = records_pair.index_id();
            if (records_pair.local_index_binlog()) {
                for (auto& str_record : records_pair.records()) {
                    return_str_records.emplace_back(str_record);
                }
                for (auto& str_record : records_pair.old_records()) {
                    return_str_old_records.emplace_back(str_record);
                }
            } else {
                for (auto& str_record : records_pair.records()) {
                    SmartRecord record = record_template->clone(false);
                    auto ret = record->decode(str_record);
                    if (ret < 0) {
                        DB_DONE(FATAL, "decode to record fail");
                        return E_FATAL;
                    }
                    //DB_WARNING("record: %s", record->debug_string().c_str());
                    result_records[index_id].emplace_back(record);
                }
            }
        }
        {
            BAIDU_SCOPED_LOCK(_fetcher_store->region_lock);
            for (auto& result_record : result_records) {
                int64_t index_id = result_record.first;
                _fetcher_store->index_records[index_id].insert(_fetcher_store->index_records[index_id].end(),
                    result_record.second.begin(), result_record.second.end());
            }
            if (!return_str_records.empty()) {
                std::vector<std::string>&  str_records = _fetcher_store->return_str_records[info.partition_id()];
                str_records.insert(str_records.end(), return_str_records.begin(), return_str_records.end());
            }
            if (!return_str_old_records.empty()) {
                std::vector<std::string>&  str_old_records =  _fetcher_store->return_str_old_records[info.partition_id()];
                str_old_records.insert(str_old_records.end(), return_str_old_records.begin(), return_str_old_records.end());
            }
        }
    }
    if (single_response.has_scan_rows()) {
        _fetcher_store->scan_rows += single_response.scan_rows();
    }
    if (single_response.has_read_disk_size()) {
        _fetcher_store->read_disk_size += single_response.read_disk_size();
    }
    if (single_response.has_filter_rows()) {
        _fetcher_store->filter_rows += single_response.filter_rows();
    }
    if (single_response.has_last_insert_id()) {
        _client_conn->last_insert_id = single_response.last_insert_id();
    }
    if (_op_type != pb::OP_SELECT && _op_type != pb::OP_SELECT_FOR_UPDATE && _op_type != pb::OP_ROLLBACK && _op_type != pb::OP_COMMIT) {
        _fetcher_store->affected_rows += single_response.affected_rows();
        _client_conn->txn_affected_rows += single_response.affected_rows();
        // 事务限制affected_rows，非事务限制会导致部分成功
        if (_client_conn->txn_affected_rows > FLAGS_max_affected_rows && _state->txn_id != 0) {
            DB_DONE(FATAL, "_affected_row:%ld > %ld FLAGS_max_affected_rows", 
                    _client_conn->txn_affected_rows.load(), FLAGS_max_affected_rows);
            return E_BIG_SQL;
        }
        return E_OK;
    }
    if (!single_response.leader().empty() && single_response.leader() != "0.0.0.0:0" && single_response.leader() != info.leader()) {
        info.set_leader(single_response.leader());
        schema_factory->update_leader(info);
        if (_state->txn_id != 0) {
            BAIDU_SCOPED_LOCK(_client_conn->region_lock);
            _client_conn->region_infos[region_id].set_leader(single_response.leader());
        }
    }
    TimeCost cost;
    if (single_response.row_values_size() > 0) {
        _fetcher_store->row_cnt += single_response.row_values_size();
    }
    // TODO reduce mem used by streaming
    if ((!_state->is_full_export) && (_fetcher_store->row_cnt > FLAGS_max_select_rows)) {
        DB_DONE(FATAL, "_row_cnt:%ld > %ld max_select_rows", _fetcher_store->row_cnt.load(), FLAGS_max_select_rows);
        return E_BIG_SQL;
    }
    uint64_t returned_row_size = 0;
    // EXEC_ROW: region只会返回memrow
    // EXEC_ARROW_ACERO: region可能返回memrow, 可能返回arrow
    if (single_response.execute_type() == pb::EXEC_ROW) {
        // 处理行存memrow
        std::shared_ptr<RowBatch> batch = std::make_shared<RowBatch>();
        std::vector<int64_t> ttl_batch;
        ttl_batch.reserve(100);
        bool global_ddl_with_ttl = (single_response.row_values_size() > 0 && single_response.row_values_size() == single_response.ttl_timestamp_size()) ? true : false;
        int ttl_idx = 0;
        int64_t memrow_total_used_size = 0;
        int64_t used_size = 0;
        for (auto& pb_row : single_response.row_values()) {
            if (pb_row.tuple_values_size() != single_response.tuple_ids_size()) {
                // brpc SelectiveChannel+backup_request有bug，pb的repeated字段merge到一起了
                SQL_TRACE("backup_request size diff, tuple_values_size:%d tuple_ids_size:%d rows:%d", 
                        pb_row.tuple_values_size(), single_response.tuple_ids_size(), single_response.row_values_size());
                for (auto id : single_response.tuple_ids()) {
                    SQL_TRACE("tuple_id:%d  ", id);
                }
                return E_RETRY;
            }
            std::unique_ptr<MemRow> row = _state->mem_row_desc()->fetch_mem_row();
            for (int i = 0; i < single_response.tuple_ids_size(); i++) {
                int32_t tuple_id = single_response.tuple_ids(i);
                row->from_string(tuple_id, pb_row.tuple_values(i));
            }
            row->set_partition_id(info.partition_id());
            int64_t row_size = row->used_size();
            used_size += row_size;
            memrow_total_used_size += row_size;
            if (used_size > 1024 * 1024LL) {
                if (0 != _state->memory_limit_exceeded(_fetcher_store->row_cnt, used_size)) {
                    BAIDU_SCOPED_LOCK(_fetcher_store->region_lock);
                    _state->error_code = ER_TOO_BIG_SELECT;
                    _state->error_msg.str("select reach memory limit");
                    return E_FATAL;
                }
                used_size = 0;
            }
            batch->move_row(std::move(row));
            if (global_ddl_with_ttl) {
                int64_t time_us = single_response.ttl_timestamp(ttl_idx++);
                ttl_batch.emplace_back(time_us);
                DB_DEBUG("region_id: %ld, ttl_timestamp: %ld", region_id, time_us);
            }
        }
        returned_row_size = batch->size();
        _fetcher_store->db_handle_bytes += memrow_total_used_size;
        _fetcher_store->db_handle_rows += returned_row_size;
        if (global_ddl_with_ttl) {
            BAIDU_SCOPED_LOCK(_fetcher_store->region_lock);
            _fetcher_store->region_id_ttl_timestamp_batch[region_id] = ttl_batch;
            DB_DEBUG("region_id: %ld, ttl_timestamp_size: %ld", region_id, ttl_batch.size());
        }
        if (single_response.has_cmsketch() && _state->cmsketch != nullptr) {
            _state->cmsketch->add_proto(single_response.cmsketch());
            DB_DONE(WARNING, "cmsketch:%s", single_response.cmsketch().ShortDebugString().c_str());
        }
        if (single_response.has_hll() && _state->hll != nullptr) {
            _state->hll->add_proto(single_response.hll());
            DB_DONE(WARNING, "table hll: %s", single_response.hll().ShortDebugString().c_str());
        }
        std::shared_ptr<arrow::RecordBatch> out = nullptr;
        if (_fetcher_store->received_arrow_data) {
            // 已经收到region返回arrow格式, 其他返回行的数据直接转向量化
            // 提前到这里转. 因为FetcherStoreVectorizedReader是单线程执行
            std::shared_ptr<Chunk> chunk = std::make_shared<Chunk>();
            std::vector<const pb::TupleDescriptor*> tuples;
            tuples.reserve(single_response.tuple_ids_size());
            for (auto tuple_id : single_response.tuple_ids()) {
                tuples.emplace_back(_state->get_tuple_desc(tuple_id));
            }
            if (0 != batch->transfer_rowbatch_to_arrow(tuples, chunk, nullptr, &out)) {
                DB_DONE(WARNING, "transfer_rowbatch_to_arrow error");
            }
        }
        if (out == nullptr) {
            BAIDU_SCOPED_LOCK(_fetcher_store->region_lock);
            // merge可能会重复请求相同的region_id
            if (_fetcher_store->region_batch.count(region_id) == 1) {
                _fetcher_store->region_batch[region_id].set_row_data(batch);
            } else {
                //分裂单独处理start_key_sort
                _fetcher_store->start_key_sort.emplace(info.start_key(), region_id);
                _fetcher_store->region_batch[region_id].set_row_data(batch);
            }
        } else {
            BAIDU_SCOPED_LOCK(_fetcher_store->region_lock);
            // merge可能会重复请求相同的region_id
            if (_fetcher_store->region_batch.count(region_id) == 1) {
                _fetcher_store->region_batch[region_id].set_arrow_data(out);
            } else {
                //分裂单独处理start_key_sort
                _fetcher_store->start_key_sort.emplace(info.start_key(), region_id);
                _fetcher_store->region_batch[region_id].set_arrow_data(out);
            }
        }
    } else {
        // 处理列存arrow recordbatch
        std::shared_ptr<arrow::Schema> schema = nullptr;
        const auto& vector_rows = single_response.mutable_extra_res()->vectorized_rows();
        const auto& vector_schema = single_response.mutable_extra_res()->vectorized_schema();
        std::shared_ptr<arrow::RecordBatch> batch;
        if (vector_rows.size() > 0) {
            // 解析列存格式
            std::shared_ptr<arrow::Buffer> schema_buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(vector_schema.data()),
                static_cast<int64_t>(vector_schema.size()));
            arrow::io::BufferReader schema_reader(schema_buffer);
            auto schema_ret = arrow::ipc::ReadSchema(&schema_reader, nullptr);
            if (schema_ret.ok()) {
                schema = *schema_ret;
            } else {
                DB_WARNING("parser from schema error [%s]. ", schema_ret.status().ToString().c_str());
                _state->error_code = ER_EXEC_PLAN_FAILED;
                _state->error_msg.str("parse arrow schema fail");
                return E_FATAL;
            }
            // 解析列存数据
            std::shared_ptr<arrow::Buffer> buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(vector_rows.data()),
                static_cast<int64_t>(vector_rows.size()));
            arrow::io::BufferReader buf_reader(buffer);
            auto ret = arrow::ipc::ReadRecordBatch(schema, nullptr, arrow::ipc::IpcReadOptions::Defaults(), &buf_reader);
            if (ret.ok()) {
                batch = *ret;
                returned_row_size = batch->num_rows();
                //DB_WARNING("region: %ld return vector row: %ld, batch: %s", region_id, returned_row_size, batch->ToString().c_str());
            } else {
                DB_WARNING("parser from array error [%s]. ", ret.status().ToString().c_str());
                _state->error_code = ER_EXEC_PLAN_FAILED;
                _state->error_msg.str("parse arrow data fail");
                return E_FATAL;
            }
            // [ARROW TODO] 如何限制内存? 
            // 向量化是将store返回的数据都存在内存, 等请求结束这部分内存才释放
            if (vector_rows.size() > 1024 * 1024LL) {
                if (0 != _state->memory_limit_exceeded(returned_row_size, vector_rows.size())) {
                    BAIDU_SCOPED_LOCK(_fetcher_store->region_lock);
                    _state->error_code = ER_TOO_BIG_SELECT;
                    _state->error_msg.str("select reach memory limit");
                    return E_FATAL;
                }
            }
            _fetcher_store->db_handle_rows += returned_row_size;
            _fetcher_store->db_handle_bytes += vector_rows.size();
        } 
        {
            BAIDU_SCOPED_LOCK(_fetcher_store->region_lock);
            if (schema != nullptr && _fetcher_store->arrow_schema == nullptr) {
                _fetcher_store->arrow_schema = schema;
                _fetcher_store->received_arrow_data = true;
            }
            if (_fetcher_store->region_batch.count(region_id) == 1) {
                _fetcher_store->region_batch[region_id].set_arrow_data(batch);
            } else {
                //分裂单独处理start_key_sort
                _fetcher_store->start_key_sort.emplace(info.start_key(), region_id);
                _fetcher_store->region_batch[region_id].set_arrow_data(batch);
            }
            // buffer和解析出来的recordbatch是直接使用pb里的vectorized_rows内存, 是zero copy的
            // 所以列存要求response生命周期比recordBatch生命周期长
            set_region_vectorized_response(region_id, single_response_ptr);
        }
    } 

    if (_trace_node != nullptr) {
        std::string desc = "baikalDB FetcherStore send_request "
                            + pb::ErrCode_Name(single_response.errcode());
        _trace_node->set_description(_trace_node->description() + " " + desc);
        _trace_node->set_total_time(_total_cost.get_time());
        _trace_node->set_affect_rows(single_response.affected_rows());
        pb::TraceNode* local_trace = _trace_node->add_child_nodes();
        if (single_response.has_errmsg() && single_response.errcode() == pb::SUCCESS) {
            pb::TraceNode trace;
            if (!trace.ParseFromString(single_response.errmsg())) {
                DB_FATAL("parse from pb fail");
            } else {
                (*local_trace) = trace;
            }
        }
    }
    if (cost.get_time() > FLAGS_print_time_us) {
        DB_DONE(WARNING, "parse time:%ld rows:%lu", cost.get_time(), returned_row_size);
    }
    return E_OK;
}

void OnRPCDone::send_request() {
    auto err = check_status();
    if (err != E_OK) {
        _fetcher_store->error = err;
        _rpc_ctrl->task_finish(this);
        return;
    }

    // 处理request，重试时不用再填充req
    if (!_has_fill_request) {
        err = fill_request();
        if (err != E_OK) {
            _fetcher_store->error = err;
            _rpc_ctrl->task_finish(this);
            return;
        }
        if (_need_check_memory && _has_multi_plan && _op_type == pb::OP_SELECT) {
            // 多次构造store request不复用, 每个请求在这里加内存, 在task_finish时释放请求并减去请求占用的内存
            if (!_is_batch) {
                _request_size = _request.ByteSizeLong();
            } else {
                _request_size = _batch_request.ByteSizeLong();
            }
            if (_request_size > 0 
                    && 0 != _state->memory_limit_exceeded(std::numeric_limits<int>::max(), _request_size)) {
                _fetcher_store->error = E_BIG_SQL;
                _rpc_ctrl->task_finish(this);
                return;
            }
        }
        _has_fill_request = true;
    }

    // 选择请求的store地址, batch request不用选择
    pre_send_async();

    err = send_async();
    if (err == E_RETRY) {
        _rpc_ctrl->task_retry(this);
    } else if (err != E_ASYNC) {
        if (err != E_OK) {
            _fetcher_store->error = err;
        }
        _rpc_ctrl->task_finish(this);
    }
}

OnSingleRPCDone::OnSingleRPCDone(FetcherStore* fetcher_store, RuntimeState* state, ExecNode* store_request, pb::RegionInfo* info_ptr, 
    int64_t old_region_id, int64_t region_id, int start_seq_id, int current_seq_id, pb::OpType op_type, bool need_check_memory) : 
    OnRPCDone(fetcher_store, state, store_request, start_seq_id, current_seq_id, op_type, need_check_memory),
    _info(*info_ptr) {
    _old_region_id = old_region_id;
    _region_id = region_id;
    std::string resource_tag = FLAGS_insulate_fetcher_resource_tag.empty() ? FLAGS_fetcher_resource_tag : FLAGS_insulate_fetcher_resource_tag;
    if (!resource_tag.empty()) {
        // region分组优化，让每个store均匀请求region
        pick_addr_for_resource_tag(resource_tag, _store_addr);
    } 
    if (_store_addr.empty()) {
        if (_info.leader() == "0.0.0.0:0" || _info.leader() == "") {
            _store_addr = rand_peer(_info);
        } else {
            _store_addr = _info.leader();
        }
    }
    _meta_id = ::baikaldb::get_meta_id(_info.table_id());
    async_rpc_region_count << 1;
    DB_DONE(DEBUG, "OnSingleRPCDone");
}

OnSingleRPCDone::~OnSingleRPCDone() {
    async_rpc_region_count << -1;
}

ErrorType OnSingleRPCDone::fill_request() {
    return OnRPCDone::fill_single_request(_request, _info, _region_id, _old_region_id);
}

void OnSingleRPCDone::select_addr() {
    bool select_without_leader = false;
    OnRPCDone::select_addr(_info, _addr, _resource_insulate_read, select_without_leader);

    if (select_without_leader) {
        _request.set_select_without_leader(true);
    }
    if (_op_type == pb::OP_SELECT && _state->txn_id == 0) {
        // follower read, 走read index
        if (_state->need_use_read_index) {
            _request.mutable_extra_req()->set_use_read_idx(true);
        }
    }
}

void OnSingleRPCDone::Run() {
    OnRPCDone::Run(std::to_string(_info.version()));
}

ErrorType OnSingleRPCDone::handle_response(const std::string& remote_side) {
    return handle_single_response(remote_side, _region_id, _addr, _info, _response_ptr);
}

void OnSingleRPCDone::set_region_vectorized_response(int64_t region_id, std::shared_ptr<pb::StoreRes> single_response_ptr) {
    _fetcher_store->region_vectorized_response[region_id] = single_response_ptr;
}

void OnSingleRPCDone::retry_region_task(const std::string& remote_side) {
   _fetcher_store->peer_status.set_cannot_access(_info.region_id(), remote_side);
    FetcherStore::other_normal_peer_to_leader(_info, _addr);
    bthread_usleep(_retry_times * FLAGS_retry_interval_us);
    _rpc_ctrl->task_retry(this);
}

void OnSingleRPCDone::retry_or_finish_task(const std::string& remote_side) {
    auto err = handle_response(remote_side);
    if (err == E_RETRY) {
        _rpc_ctrl->task_retry(this);
    } else {
        if (err != E_OK) {
            _fetcher_store->error = err;
        }
        _rpc_ctrl->task_finish(this);
    }
}

OnBatchRPCDone::OnBatchRPCDone(FetcherStore* fetcher_store, 
                RuntimeState* state, 
                ExecNode* store_request, 
                std::vector<pb::RegionInfo*> infos,
                int start_seq_id, 
                int current_seq_id, 
                pb::OpType op_type,
                int64_t limit_single_store_concurrency_cnts,
                bool need_check_memory) : 
    OnRPCDone(fetcher_store, state, store_request, start_seq_id, current_seq_id, op_type, need_check_memory),
    _infos(infos), _limit_single_store_concurrency_cnts(limit_single_store_concurrency_cnts) {
    for (auto& info : infos) {
        _region_ids.push_back(info->region_id());
        _versions_str += std::to_string(info->version()) + ",";
        _region_ids_str += std::to_string(info->region_id()) + ",";
        _info_map[info->region_id()] = *info;
    }

    _is_batch = true;
    DB_DONE(DEBUG, "OnBatchRPCDone");
}

OnBatchRPCDone::OnBatchRPCDone(FetcherStore* fetcher_store, 
                RuntimeState* state, 
                ExecNode* store_request, 
                std::vector<RegionInfoData> info_datas,
                const std::string& store_addr,
                int start_seq_id, 
                int current_seq_id, 
                pb::OpType op_type,
                int64_t limit_single_store_concurrency_cnts,
                bool need_check_memory) : 
    OnRPCDone(fetcher_store, state, store_request, start_seq_id, current_seq_id, op_type, need_check_memory),
    _info_datas(info_datas), _limit_single_store_concurrency_cnts(limit_single_store_concurrency_cnts) {
    _store_addr = store_addr;
    _addr = store_addr;
    for (auto& info_data : info_datas) {
        pb::RegionInfo* info = info_data.region_info;
        _infos.push_back(info);
        _region_ids.push_back(info->region_id());
        _versions_str += std::to_string(info->version()) + ",";
        _region_ids_str += std::to_string(info->region_id()) + ",";
        _info_map[info->region_id()] = *info;
        if (_resource_insulate_read == false && info_data.resource_insulate_read) {
            _resource_insulate_read = true;
        }
    }

    if (_infos.size() > 0) {
        _meta_id = ::baikaldb::get_meta_id(_infos[0]->table_id());       
    }
    async_rpc_region_count << _region_ids.size();
    _is_batch = true;
    _is_real_exec = true;
    DB_DONE(DEBUG, "OnBatchRPCDone");
}

OnBatchRPCDone::~OnBatchRPCDone() {
    if (_is_real_exec) {
        async_rpc_region_count << - _region_ids.size();
    }
}

ErrorType OnBatchRPCDone::fill_request() {
    batch_rpc_region_count <<_info_datas.size();
    for (auto& info_data : _info_datas) {
        pb::StoreReq& single_req = *(_batch_request.add_store_req());
        int64_t region_id = info_data.region_info->region_id();
        pb::RegionInfo& info = *(info_data.region_info);
        ErrorType ret = OnRPCDone::fill_single_request(single_req, info, region_id, region_id);
        if (ret != E_OK) {
            return ret;
        }
        single_req.set_select_without_leader(info_data.select_without_leader);
        if (_op_type == pb::OP_SELECT && _state->txn_id == 0) {
            // follower read, 走read index
            if (_state->need_use_read_index) {
                single_req.mutable_extra_req()->set_use_read_idx(true);
            }
        }
    }
    _batch_request.set_limit_single_store_concurrency(_limit_single_store_concurrency_cnts);

    if (_has_multi_plan) {
        for (int i = 0; i < _region_ids.size(); ++i) {
            ExecNode::create_pb_plan(_region_ids[i], _batch_request.add_plan(), _store_request);
            if (_meta_id != 0) {
                if (del_meta_info(*_batch_request.mutable_plan(i)) != 0) {
                    DB_FATAL("Fail to del_meta_info");
                    return E_FATAL;
                }
            }
        }
    } else {
        if (_fetcher_store->shared_plan == nullptr) {
            _fetcher_store->shared_plan = std::make_shared<pb::Plan>();
            ExecNode::create_pb_plan(_region_ids[0], _fetcher_store->shared_plan.get(), _store_request);
            if (_need_check_memory) {
                // 这里加1次内存, 无论多少个region, 最后整个请求析构的时候统一release
                int64_t shared_request_size = _fetcher_store->shared_plan->ByteSizeLong();
                if (shared_request_size > 0 
                        && 0 != _state->memory_limit_exceeded(std::numeric_limits<int>::max(), shared_request_size)) {
                    _fetcher_store->error = E_BIG_SQL;
                    DB_WARNING_STATE(_state, "Memory limit exceeded when add shared batch plan size: %ld.", shared_request_size);
                    return E_BIG_SQL;
                }
            }
        }
        // 发送给不同的store, 共用同一个plan, 修改arena，需要注意这个函数调用
        _batch_request.mutable_plan()->AddAllocated(_fetcher_store->shared_plan.get());
        if (_meta_id != 0) {
            if (del_meta_info(*_batch_request.mutable_plan(0)) != 0) {
                DB_FATAL("Fail to del_meta_info");
                return E_FATAL;
            }
        }
    }

    return E_OK;
}

std::vector<OnBatchRPCDone*> OnBatchRPCDone::select_addr() {
    std::map<std::string, std::vector<RegionInfoData>> addr_batch_info_map;
    for (auto& info_ptr : _infos) {
        pb::RegionInfo& info = *info_ptr;
        std::string addr;
        bool resource_insulate_read = false;
        bool select_without_leader = false;
        OnRPCDone::select_addr(info, addr, resource_insulate_read, select_without_leader);
        RegionInfoData region_info_data;
        region_info_data.region_info = info_ptr;
        region_info_data.select_without_leader = select_without_leader;
        region_info_data.resource_insulate_read = resource_insulate_read;
        addr_batch_info_map[addr].push_back(region_info_data);
    }
    std::vector<OnBatchRPCDone*> batch_rpc_done_list;
    for (auto& iter : addr_batch_info_map) {
        batch_rpc_done_list.emplace_back(
                new OnBatchRPCDone(_fetcher_store, _state, _store_request, iter.second,
                        iter.first, _start_seq_id, _current_seq_id, _op_type, 
                        _limit_single_store_concurrency_cnts, _need_check_memory));
    }
    return batch_rpc_done_list;
}

void OnBatchRPCDone::Run() {
    OnRPCDone::Run(_versions_str);
}

ErrorType OnBatchRPCDone::handle_response(const std::string& remote_side, std::vector<OnSingleRPCDone*>& need_retry_tasks) {
    auto& batch_response = *_batch_response_ptr;
    for (int i = 0; i < batch_response.store_res_size(); ++i) {
        std::shared_ptr<pb::StoreRes> smart_single_response_ptr(batch_response.mutable_store_res(i), [](pb::StoreRes* ptr) {});
        int64_t region_id = smart_single_response_ptr->orig_region_id();
        pb::RegionInfo& info = _info_map[region_id];
        auto ret = handle_single_response(remote_side, region_id, _addr, info, smart_single_response_ptr);
        if (ret == E_RETRY) {
            need_retry_tasks.push_back(new OnSingleRPCDone(_fetcher_store, _state, _store_request, &info, 
                   region_id, info.region_id(), _start_seq_id, _current_seq_id, _op_type, _need_check_memory));
        }
        if (ret != E_OK && ret != E_RETRY) {
            return ret;
        }
    }
    return E_OK;
}

void OnBatchRPCDone::set_region_vectorized_response(int64_t region_id, std::shared_ptr<pb::StoreRes> single_response_ptr) {
    _fetcher_store->region_vectorized_response[region_id] = single_response_ptr;
    _fetcher_store->batch_region_vectorized_response[region_id] = _batch_response_ptr;
}

void OnBatchRPCDone::retry_region_task(const std::string& remote_side) {
    for (auto& info_ptr : _infos) {
        _fetcher_store->peer_status.set_cannot_access(info_ptr->region_id(), remote_side);
        FetcherStore::other_normal_peer_to_leader(*info_ptr, _addr);
    }
    bthread_usleep(_retry_times * FLAGS_retry_interval_us);
    for (auto& info_ptr : _infos) {
        _rpc_ctrl->add_new_task(new OnSingleRPCDone(_fetcher_store, _state, _store_request, info_ptr, 
            info_ptr->region_id(), info_ptr->region_id(), _start_seq_id, _current_seq_id, 
            _op_type, _need_check_memory));
    }
    _rpc_ctrl->task_finish(this);
}

void OnBatchRPCDone::retry_or_finish_task(const std::string& remote_side) {
    std::vector<OnSingleRPCDone*> need_retry_tasks;
    auto err = handle_response(remote_side, need_retry_tasks);
    if (need_retry_tasks.size() > 0) {
        for (auto& task : need_retry_tasks) {
            _rpc_ctrl->add_new_task(task);
        }
    }
    if (err != E_OK && err != E_RETRY) {
        _fetcher_store->error = err;
    }
    _rpc_ctrl->task_finish(this);
}

void FetcherStore::choose_other_if_dead(pb::RegionInfo& info, std::string& addr) {
    SchemaFactory* schema_factory = SchemaFactory::get_instance();
    auto status = schema_factory->get_instance_status(addr);
    if (status.status != pb::DEAD) {
        return;
    }

    std::vector<std::string> normal_peers;
    std::vector<std::string> other_peers;
    normal_peers.reserve(3);
    other_peers.reserve(3);
    for (auto& peer: info.peers()) {
        auto status = schema_factory->get_instance_status(peer);
        if (status.status == pb::NORMAL) {
            normal_peers.emplace_back(peer);
        } else if (status.status != pb::DEAD) {
            other_peers.emplace_back(peer);
        }
    }
    if (normal_peers.size() > 0) {
        uint32_t i = butil::fast_rand() % normal_peers.size();
        addr = normal_peers[i];
    } else if(other_peers.size() > 0) {
        uint32_t i = butil::fast_rand() % other_peers.size();
        addr = other_peers[i];
    } else {
        DB_DEBUG("all peer faulty, %ld", info.region_id());
    }
}

void FetcherStore::other_normal_peer_to_leader(pb::RegionInfo& info, const std::string& addr) {
    SchemaFactory* schema_factory = SchemaFactory::get_instance();

    std::vector<std::string> normal_peers;
    for (auto& peer: info.peers()) {
        auto status = schema_factory->get_instance_status(peer);
        if (status.status == pb::NORMAL && peer != addr) {
            normal_peers.push_back(peer);
        }
    }
    if (normal_peers.size() > 0) {
        uint32_t i = butil::fast_rand() % normal_peers.size();
        info.set_leader(normal_peers[i]);
    } else {
        for (auto& peer : info.peers()) {
            if (peer != addr) {
                info.set_leader(peer);
                break;
            }
        }
        DB_DEBUG("all peer faulty, %ld", info.region_id());
    }
}

int64_t FetcherStore::get_commit_ts() {
    auto binlog_ctx = client_conn->get_binlog_ctx();
    int64_t timestamp = TsoFetcher::get_instance()->get_tso(binlog_ctx->tso_count());
    if (timestamp < 0) {
        return -1;
    }
    return timestamp;
}

ErrorType FetcherStore::process_binlog_start(RuntimeState* state, pb::OpType op_type) {
    if (need_process_binlog(state, op_type)) {
        auto binlog_ctx = client_conn->get_binlog_ctx();
        uint64_t log_id = state->log_id();
        binlog_cond.increase();
        auto write_binlog_func = [this, state, binlog_ctx, op_type, log_id]() {
            ON_SCOPE_EXIT([this]() {
                binlog_cond.decrease_signal();
            });
            if (op_type == pb::OP_PREPARE) {
                int64_t timestamp = TsoFetcher::get_instance()->get_tso(binlog_ctx->tso_count());
                if (timestamp < 0) {
                    DB_WARNING("get tso failed log_id: %lu txn_id:%lu op_type:%s", log_id, state->txn_id,
                        pb::OpType_Name(op_type).c_str());
                    error = E_FATAL;
                    need_send_rollback = false;
                    return;
                }
                binlog_ctx->set_start_ts(timestamp);
            }
            write_binlog_param.txn_id = state->txn_id;
            write_binlog_param.log_id = log_id;
            write_binlog_param.primary_region_id = client_conn->primary_region_id;
            write_binlog_param.global_conn_id = client_conn->get_global_conn_id();
            write_binlog_param.username = client_conn->user_info->username;
            write_binlog_param.ip = client_conn->ip;
            write_binlog_param.client_conn = client_conn;
            write_binlog_param.fetcher_store = this;
            write_binlog_param.op_type = op_type;
            auto ret = binlog_ctx->write_binlog(&write_binlog_param);
            if (ret != E_OK) {
                error = E_FATAL;
            }
        };
        Bthread bth(&BTHREAD_ATTR_SMALL);
        bth.run(write_binlog_func);
        return E_OK;
    }
    return E_OK;
}

int64_t FetcherStore::get_dynamic_timeout_ms(ExecNode* store_request, pb::OpType op_type, uint64_t sign) {
    int64_t dynamic_timeout_ms = -1;

    if (FLAGS_use_dynamic_timeout && op_type == pb::OP_SELECT) {
        SchemaFactory* factory = SchemaFactory::get_instance();
        std::shared_ptr<SqlStatistics> sql_info = factory->get_sql_stat(sign);
        if (sql_info != nullptr) {
            dynamic_timeout_ms = sql_info->dynamic_timeout_ms();
        }

        std::vector<ExecNode*> scan_nodes;
        store_request->get_node(pb::SCAN_NODE, scan_nodes);
        if (sql_info != nullptr && scan_nodes.size() == 1) {
            ScanNode* scan_node = static_cast<ScanNode*>(scan_nodes[0]);
            int64_t heap_top = sql_info->latency_heap_top();
            if (scan_node->learner_use_diff_index() && heap_top > 0) {
                DB_WARNING("dynamic_timeout_ms: %ld, heap_top: %ld", dynamic_timeout_ms, heap_top);
                if (dynamic_timeout_ms <= 0 ) {
                    dynamic_timeout_ms = heap_top;
                } else {
                    dynamic_timeout_ms = std::min(dynamic_timeout_ms, heap_top);
                }
            }
        }
    }

    return dynamic_timeout_ms;
}

int64_t FetcherStore::get_sign_latency(pb::OpType op_type, uint64_t sign) {
    int64_t latency = -1;

    if (FLAGS_use_dynamic_timeout && op_type == pb::OP_SELECT) {
        SchemaFactory* factory = SchemaFactory::get_instance();
        std::shared_ptr<SqlStatistics> sql_info = factory->get_sql_stat(sign);
        if (sql_info != nullptr) {
            latency = sql_info->latency_us_9999;
        }
    }
    return latency;
}

void FetcherStore::send_request(RuntimeState* state,
                        ExecNode* store_request, 
                        std::vector<pb::RegionInfo*> infos, 
                        int start_seq_id,
                        int current_seq_id,
                        pb::OpType op_type) {
    int64_t limit_single_store_concurrency_cnts = 0;
    limit_single_store_concurrency_cnts = state->calc_single_store_concurrency(op_type);
    std::set<std::shared_ptr<pb::TraceNode>> traces;

    RPCCtrl rpc_ctrl(limit_single_store_concurrency_cnts, need_check_memory);
    if (!state->force_single_rpc
            && FLAGS_enable_batch_rpc 
            && !is_full_export
            && op_type == pb::OP_SELECT 
            && state->txn_id == 0
            && infos.size() > FLAGS_enable_batch_rpc_region_num) {
        // 临时用来选addr
        OnBatchRPCDone base_task(this, state, store_request, infos, start_seq_id, current_seq_id, op_type, 
            limit_single_store_concurrency_cnts, need_check_memory); 
        std::vector<OnBatchRPCDone*> batch_rpc_done_list = base_task.select_addr();
        for (auto& task : batch_rpc_done_list) {
            rpc_ctrl.add_new_task(task);
            traces.insert(task->get_trace());
        }
    } else {
        for (auto info : infos) {
            auto task = new OnSingleRPCDone(this, state, store_request, info, 
                    info->region_id(), info->region_id(), start_seq_id, current_seq_id, 
                    op_type, need_check_memory);
            rpc_ctrl.add_new_task(task);
            traces.insert(task->get_trace());
        }
    }

    rpc_ctrl.execute();

    if (store_request->get_trace() != nullptr) {           
        for (auto trace : traces) {
            (*store_request->get_trace()->add_child_nodes()) = *trace;
        }
    }
}

int FetcherStore::run_not_set_state(RuntimeState* state,
                    std::map<int64_t, pb::RegionInfo>& region_infos,
                    ExecNode* store_request,
                    int start_seq_id,
                    int current_seq_id,
                    pb::OpType op_type, 
                    GlobalBackupType backup_type) {
    region_batch.clear();
    index_records.clear();
    start_key_sort.clear();
    no_copy_cache_plan_set.clear();
    error = E_OK;
    skip_region_set.clear();
    callids.clear();
    primary_timestamp_updated = false;
    affected_rows = 0;
    scan_rows = 0;
    filter_rows = 0;
    row_cnt = 0;
    db_handle_rows = 0;
    db_handle_bytes = 0;
    client_conn = state->client_conn();
    region_count += region_infos.size();
    global_backup_type = backup_type;
    arrow_schema.reset();
    region_vectorized_response.clear();
    received_arrow_data = false;
    batch_region_vectorized_response.clear();
    shared_plan.reset();
    if (region_infos.size() == 0) {
        DB_WARNING("region_infos size == 0, op_type:%s", pb::OpType_Name(op_type).c_str());
        return 0;
    }
    auto scan_node = store_request->get_node(pb::SCAN_NODE);
    ExecNode* filter_node = (scan_node == nullptr ? nullptr : scan_node->get_parent());
    if (filter_node != nullptr
            && (filter_node->node_type() == pb::TABLE_FILTER_NODE
                || filter_node->node_type() == pb::WHERE_FILTER_NODE)) {
        need_check_memory = static_cast<FilterNode*>(filter_node)->need_check_memory();
    }
    dynamic_timeout_ms = get_dynamic_timeout_ms(store_request, op_type, state->sign);
    sign_latency = get_sign_latency(op_type, state->sign);
    // 预分配空洞
    for (auto& pair : region_infos) {
        start_key_sort.emplace(pair.second.start_key(), pair.first);
        region_batch[pair.first] = RegionReturnData{nullptr, nullptr};
    }
    uint64_t log_id = state->log_id();
    // 选择primary region同时保证第一次请求primary region成功
    if ((state->txn_id != 0) && (client_conn->primary_region_id == -1) && op_type != pb::OP_SELECT) {
        auto info_iter = region_infos.begin();
        client_conn->primary_region_id = info_iter->first;
        client_conn->txn_pri_region_last_exec_time = butil::gettimeofday_us();
        send_request(state, store_request, &info_iter->second, start_seq_id, current_seq_id, op_type);
        if (error == E_RETURN) {
            DB_WARNING("primary_region_id:%ld rollbacked, log_id:%lu op_type:%s",
                client_conn->primary_region_id.load(), log_id, pb::OpType_Name(op_type).c_str());
            if (op_type == pb::OP_COMMIT || op_type == pb::OP_ROLLBACK) {
                return 0;
            } else {
                client_conn->state = STATE_ERROR;
                return -1;
            }
        }
        if (error != E_OK) {
            if (error == E_FATAL) {
            DB_FATAL("fetcher node open fail, log_id:%lu, txn_id: %lu, seq_id: %d op_type: %s",
                    log_id, state->txn_id, current_seq_id, pb::OpType_Name(op_type).c_str());
            } else {
                DB_WARNING("fetcher node open fail, log_id:%lu, txn_id: %lu, seq_id: %d op_type: %s",
                        log_id, state->txn_id, current_seq_id, pb::OpType_Name(op_type).c_str());
            }
            if (error != E_WARNING) {
                client_conn->state = STATE_ERROR;
            }
            return -1;
        }
        skip_region_set.insert(info_iter->first);
    }

    // 保证primary region执行commit/rollback成功,其他region请求异步执行(死循环FixMe)
    if ((op_type == pb::OP_COMMIT || op_type == pb::OP_ROLLBACK)
        && skip_region_set.count(client_conn->primary_region_id) == 0) {
        int64_t primary_region_id = client_conn->primary_region_id;
        auto iter = client_conn->region_infos.find(primary_region_id);
        if (iter == client_conn->region_infos.end()) {
            DB_FATAL("something wrong primary_region_id: %ld", primary_region_id);
            return 0;
        }
        // commit命令获取commit_ts需要发送给store
        if (op_type == pb::OP_COMMIT && need_process_binlog(state, op_type)) {
            int64_t commit_ts = get_commit_ts();
            if (commit_ts < 0) {
                DB_WARNING("get commit_ts fail");
                return -1;
            }
            auto binlog_ctx = client_conn->get_binlog_ctx();
            binlog_ctx->set_commit_ts(commit_ts);
        }
        int retry_times = 0;
        do {
            error = E_OK; // 每次重试前将error设置为E_OK
            send_request(state, store_request, &iter->second, start_seq_id, current_seq_id, op_type);
            if (error == E_RETURN) {
                DB_WARNING("primary_region_id:%ld rollbacked, log_id:%lu op_type:%s",
                    primary_region_id, log_id, pb::OpType_Name(op_type).c_str());
                if (op_type == pb::OP_COMMIT) {
                    client_conn->state = STATE_ERROR;
                    return -1;
                } else {
                    // 让其他region也能执行rollback
                    error = E_OK;
                    break;
                }
            }
            if (error != E_OK) {
                DB_FATAL("send optype:%s to region_id:%ld txn_id:%lu failed, log_id:%lu ", pb::OpType_Name(op_type).c_str(),
                    primary_region_id, state->txn_id, log_id);
                if (retry_times < 5) {
                    retry_times++;
                }
                // commit rpc请求被cancel不能直接发rollback, 可能请求已经在store执行，需要store返查primary region
                if (state->is_cancelled() || (op_type != pb::OP_COMMIT && is_cancelled)) {
                    return -1;
                }
                // 每次多延迟5s重试，leader切换耗时评估后考虑去掉无限重试
                bthread_usleep(retry_times * FLAGS_retry_interval_us * 10L);
            }
        } while (error != E_OK);
        skip_region_set.insert(primary_region_id);
    }

    auto ret = process_binlog_start(state, op_type);
    if (ret != E_OK) {
        DB_FATAL("process binlog op_type:%s txn_id:%lu failed, log_id:%lu ", pb::OpType_Name(op_type).c_str(),
             state->txn_id, log_id);
        return -1;
    }

    // 构造并发送请求
    std::vector<pb::RegionInfo*> infos;
    infos.reserve(region_infos.size());
    for (auto& pair : region_infos) {
        int64_t region_id = pair.first;
        if (skip_region_set.count(region_id) > 0) {
            continue;
        }

        pb::RegionInfo* info = nullptr;
        if (region_infos.count(region_id) != 0) {
            info = &region_infos[region_id];
        } else if (state->txn_id != 0) {
            BAIDU_SCOPED_LOCK(client_conn->region_lock);
            info = &(client_conn->region_infos[region_id]);
        }
        infos.emplace_back(info);
    }

    send_request(state, store_request, infos, start_seq_id, current_seq_id, op_type);

    process_binlog_done(state, op_type);

    if (op_type == pb::OP_COMMIT || op_type == pb::OP_ROLLBACK) {
        // 清除primary region信息
        client_conn->primary_region_id = -1;
        return 0;
    }

    if (error != E_OK) {
        if (error == E_FATAL
                || error == E_BIG_SQL) {
            DB_FATAL("fetcher node open fail, log_id:%lu, txn_id: %lu, seq_id: %d op_type: %s",
                    log_id, state->txn_id, current_seq_id, pb::OpType_Name(op_type).c_str());
            if (error == E_BIG_SQL) {
                error_code = ER_SQL_TOO_BIG;
                error_msg.str("sql/txn too big");
            }
        } else {
            DB_WARNING("fetcher node open fail, log_id:%lu, txn_id: %lu, seq_id: %d op_type: %s",
                    log_id, state->txn_id, current_seq_id, pb::OpType_Name(op_type).c_str());
        }
        return -1;
    }

    return affected_rows.load();
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
