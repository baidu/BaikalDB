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
namespace baikaldb {

DEFINE_int64(retry_interval_us, 500 * 1000, "retry interval ");
DEFINE_int32(single_store_concurrency, 20, "max request for one store");
DEFINE_int64(max_select_rows, 10000000, "query will be fail when select too much rows");
DEFINE_int64(max_affected_rows, 10000000, "query will be fail when affect too much rows");
DEFINE_int64(print_time_us, 10000, "print log when time_cost > print_time_us(us)");
DEFINE_int64(baikaldb_alive_time_s, 10 * 60, "obervation time length in baikaldb, default:10 min");
BRPC_VALIDATE_GFLAG(print_time_us, brpc::NonNegativeInteger);
DEFINE_int32(fetcher_request_timeout, 100000,
                    "store as server request timeout, default:10000ms");
DEFINE_int32(fetcher_connect_timeout, 1000,
                    "store as server connect timeout, default:1000ms");
DEFINE_bool(fetcher_follower_read, true, "where allow follower read for fether");
DEFINE_bool(fetcher_learner_read, false, "where allow learner read for fether");
DEFINE_string(insulate_fetcher_resource_tag, "", "store read insulate resource_tag");
DEFINE_string(fetcher_resource_tag, "", "store read resource_tag perfered, only first time valid");
DECLARE_int32(transaction_clear_delay_ms);
DEFINE_bool(use_dynamic_timeout, false, "whether use dynamic_timeout");
DEFINE_bool(use_read_index, false, "whether use follower read");
DEFINE_bool(read_random_select_peer, false, "read random select peers");

BRPC_VALIDATE_GFLAG(use_dynamic_timeout, brpc::PassValidate);
bvar::Adder<int64_t> OnRPCDone::async_rpc_region_count {"async_rpc_region_count"};
bvar::LatencyRecorder OnRPCDone::total_send_request {"total_send_request"};
bvar::LatencyRecorder OnRPCDone::add_backup_send_request {"add_backup_send_request"};
bvar::LatencyRecorder OnRPCDone::has_backup_send_request {"has_backup_send_request"};

OnRPCDone::OnRPCDone(FetcherStore* fetcher_store, RuntimeState* state, ExecNode* store_request, pb::RegionInfo* info_ptr, 
    int64_t old_region_id, int64_t region_id, int start_seq_id, int current_seq_id, pb::OpType op_type) : 
    _fetcher_store(fetcher_store), _state(state), _store_request(store_request), _info(*info_ptr),
    _old_region_id(old_region_id), _region_id(region_id), 
    _start_seq_id(start_seq_id),  _current_seq_id(current_seq_id), _op_type(op_type) {
    _client_conn = _state->client_conn();
    if (_store_request->get_trace() != nullptr) {
        _trace_node = std::make_shared<pb::TraceNode>();
    }
    std::string resource_tag = FLAGS_insulate_fetcher_resource_tag.empty() ? FLAGS_fetcher_resource_tag : FLAGS_insulate_fetcher_resource_tag;
    if (!resource_tag.empty()) {
        std::string baikaldb_logical_room = SchemaFactory::get_instance()->get_logical_room();
        for (auto& peer : _info.peers()) {
            auto status = SchemaFactory::get_instance()->get_instance_status(peer);
            if (status.status == pb::NORMAL 
                    && status.resource_tag == resource_tag 
                    && status.logical_room == baikaldb_logical_room) {
                _store_addr = peer;
                break;
            }
        }
    } 
    if (_store_addr.empty()) {
        if (_info.leader() == "0.0.0.0:0" || _info.leader() == "") {
            _store_addr = rand_peer(_info);
        } else {
            _store_addr = _info.leader();
        }
    }
    async_rpc_region_count << 1;
    DB_DONE(DEBUG, "OnRPCDone");
}
OnRPCDone::~OnRPCDone() {
    async_rpc_region_count << -1;
}
// 检查状态，判断是否需要继续执行
ErrorType OnRPCDone::check_status() {
    if (_fetcher_store->error != E_OK) {
        DB_DONE(WARNING, "recieve error, other region failed");
        return _fetcher_store->error;
    }

    if (_state->is_cancelled() || _fetcher_store->is_cancelled) {
        DB_DONE(FATAL, "cancelled, state cancel: %d, fetcher_store cancel: %d", 
                _state->is_cancelled(), _fetcher_store->is_cancelled);
        return E_FATAL;
    }

    if (_retry_times >= 5) {
        DB_DONE(WARNING, "too many retries");
        return E_FATAL;
    }

    return E_OK;
}

ErrorType OnRPCDone::fill_request() {
    if (_trace_node != nullptr) {
        _request.set_is_trace(true);
    }
    if (_state->explain_type == ANALYZE_STATISTICS) {
        if (_state->cmsketch != nullptr) {
            pb::AnalyzeInfo* info = _request.mutable_analyze_info();
            info->set_depth(_state->cmsketch->get_depth());
            info->set_width(_state->cmsketch->get_width());
            info->set_sample_rows(_state->cmsketch->get_sample_rows());
            info->set_table_rows(_state->cmsketch->get_table_rows());
        }
    }
    // for exec next_statement_after_begin, begin must be added
    if (_current_seq_id == 2 && !_state->single_sql_autocommit()) {
        _start_seq_id = 1;
    }
    
    bool need_copy_cache_plan = true;
    if (_state->txn_id != 0) {
        BAIDU_SCOPED_LOCK(_client_conn->region_lock);
        if (_client_conn->region_infos.count(_region_id) == 0) {
            _start_seq_id = 1;
            _fetcher_store->no_copy_cache_plan_set.emplace(_region_id);
        }
        if (_fetcher_store->no_copy_cache_plan_set.count(_region_id) != 0) {
            need_copy_cache_plan = false;
        }
    }
    
    if (_info.leader() == "0.0.0.0:0" || _info.leader() == "") {
        _info.set_leader(rand_peer(_info));
    }
    _request.set_db_conn_id(_client_conn->get_global_conn_id());
    _request.set_op_type(_op_type);
    _request.set_region_id(_region_id);
    _request.set_region_version(_info.version());
    _request.set_log_id(_state->log_id());
    _request.set_sql_sign(_state->sign);
    _request.mutable_extra_req()->set_sign_latency(_fetcher_store->sign_latency);
    for (auto& desc : _state->tuple_descs()) {
        if (desc.has_tuple_id()){
            _request.add_tuples()->CopyFrom(desc);
        }
    }
    pb::TransactionInfo* txn_info = _request.add_txn_infos();
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
            && _client_conn->primary_region_id != _region_id
            && !_fetcher_store->primary_timestamp_updated) {
            if (butil::gettimeofday_us() - _client_conn->txn_pri_region_last_exec_time > (FLAGS_transaction_clear_delay_ms / 2) * 1000LL) {
                _fetcher_store->primary_timestamp_updated = true;
                txn_info->set_need_update_primary_timestamp(true);
                _client_conn->txn_pri_region_last_exec_time = butil::gettimeofday_us();
            }
        } else if (_client_conn->primary_region_id == _region_id) {
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
                static_cast<DMLNode*>(plan_item.root)->global_index_id() != _info.table_id()
                ) {
                continue;
            }
            pb::CachePlan* pb_cache_plan = txn_info->add_cache_plans();
            pb_cache_plan->set_op_type(plan_item.op_type);
            pb_cache_plan->set_seq_id(plan_item.sql_id);
            ExecNode::create_pb_plan(_old_region_id, pb_cache_plan->mutable_plan(), plan_item.root);
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
        if (_client_conn->region_infos.count(_region_id) == 0) {
            _client_conn->region_infos.insert(std::make_pair(_region_id, _info));
        }
    }

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

    ExecNode::create_pb_plan(_old_region_id, _request.mutable_plan(), _store_request);

    if (scan_node != nullptr) {
        scan_node->current_index_unlock();
    }

    return E_OK;
}   

// 指定访问resource_tag读从
void OnRPCDone::select_resource_insulate_read_addr(const std::string& insulate_resource_tag) {
    std::vector<std::string> valid_addrs;
    if (_info.learners_size() > 0) {
        // 指定访问的resource tag, 可能是learner，可能是follower, 先判断有没有满足条件的learner
        select_valid_peers(insulate_resource_tag, _info.learners(), valid_addrs);
    }

    // 事务读也读leader
    if (_info.learners_size() > 0 &&
            ((FLAGS_fetcher_learner_read && valid_addrs.size() > 0)
            || _state->need_learner_backup()
            || _fetcher_store->global_backup_type == GBT_LEARNER/*全局索引降级，强制访问learner*/)) {
        // 指定了resource tag,没有可选learner, 在强制降级的情况下，忽略指定的resource tag
        if (valid_addrs.empty() 
            && (_state->need_learner_backup() || _fetcher_store->global_backup_type == GBT_LEARNER)) {
            select_valid_peers("", _info.learners(), valid_addrs);
        }
        if (valid_addrs.size() > 0) {
            // 有可选learner
            _addr = valid_addrs[0];
            pb::Status addr_status = pb::NORMAL;
            FetcherStore::choose_opt_instance(_info.region_id(), valid_addrs, _addr, addr_status, nullptr);
            _resource_insulate_read = true;
        } else {
            // 无可选learner
            pb::Status addr_status = pb::NORMAL;
            std::set<std::string> cannot_access_peers;
            _fetcher_store->peer_status.get_cannot_access_peer(_info.region_id(), cannot_access_peers);
            FetcherStore::choose_opt_instance(_info.region_id(), _info.peers(), _addr, addr_status, nullptr, cannot_access_peers);
        }
        _request.set_select_without_leader(true);
        _resource_insulate_read = true;
    } else if (FLAGS_fetcher_follower_read) {
        select_valid_peers(insulate_resource_tag, _info.peers(), valid_addrs);
        pb::Status addr_status = pb::NORMAL;
        if (valid_addrs.size() > 0) {
            _addr = valid_addrs[0];
            FetcherStore::choose_opt_instance(_info.region_id(), valid_addrs, _addr, addr_status, nullptr);
        } else {
            std::set<std::string> cannot_access_peers;
            _fetcher_store->peer_status.get_cannot_access_peer(_info.region_id(), cannot_access_peers);
            FetcherStore::choose_opt_instance(_info.region_id(), _info.peers(), _addr, addr_status, nullptr, cannot_access_peers);
        }
        _request.set_select_without_leader(true);
        _resource_insulate_read = true;
    } else if (_retry_times == 0) {
        // 重试前已经选择了normal的实例
        // 或者store返回了正确的leader
        FetcherStore::choose_other_if_dead(_info, _addr);
    }

    // 存在全局索引降级的情况，强制访问主集群的情况下不要backup
    if (_fetcher_store->global_backup_type == GBT_MAIN) {
        _backup.clear();
    }
}

void OnRPCDone::select_addr() {
    _addr = _info.leader();
    _resource_insulate_read = false; // 是否读learner，或者指定读从集群，进行资源隔离
    if (_state->need_learner_backup() && _info.learners_size() == 0) {
        // 没有learner副本时报警
        DB_DONE(DEBUG, "has abnormal learner, learner size: 0");
    }
    if (_op_type == pb::OP_SELECT && _state->txn_id == 0) {
        // follower read, 走read index
        if (_state->need_use_read_index) {
            _request.mutable_extra_req()->set_use_read_idx(true);
        }
        // 读随机访问所有peer
        if (FLAGS_read_random_select_peer && _retry_times == 0) {
            FetcherStore::other_normal_peer_to_leader(_info, "");
            _addr = _info.leader();
        }
        // 倾向访问的store集群，仅第一次有效, 如pap-bj db第一次优先访问pap-bj的store
        if (FLAGS_fetcher_resource_tag != "" && _retry_times == 0) {
            std::string baikaldb_logical_room = SchemaFactory::get_instance()->get_logical_room();
            for (auto& peer : _info.peers()) {
                auto status = SchemaFactory::get_instance()->get_instance_status(peer);
                if (status.status == pb::NORMAL 
                        && status.resource_tag == FLAGS_fetcher_resource_tag
                        && status.logical_room == baikaldb_logical_room) {
                    _addr = peer;
                    break;
                }
            }
        }
    }
    
    // 是否指定访问资源隔离, 如offline
    std::string insulate_resource_tag = FLAGS_insulate_fetcher_resource_tag;
    if (_state->client_conn() != nullptr 
        && _state->client_conn()->user_info != nullptr
        && !_state->client_conn()->user_info->resource_tag.empty()) {
        insulate_resource_tag = _state->client_conn()->user_info->resource_tag;
    }
    if (!insulate_resource_tag.empty() && _op_type == pb::OP_SELECT && _state->txn_id == 0) {
        return select_resource_insulate_read_addr(insulate_resource_tag);
    }

    if (_op_type == pb::OP_SELECT && _state->txn_id == 0 && _info.learners_size() > 0 && 
        (FLAGS_fetcher_learner_read || _state->need_learner_backup()
            || _fetcher_store->global_backup_type == GBT_LEARNER/*全局索引降级，强制访问learner*/)) {
        std::vector<std::string> valid_learners;
        select_valid_peers("", _info.learners(), valid_learners);
        if (!valid_learners.empty()) {
            _addr = valid_learners[0];
            pb::Status addr_status = pb::NORMAL;
            FetcherStore::choose_opt_instance(_info.region_id(), valid_learners, _addr, addr_status, nullptr);
            _resource_insulate_read = true;
        } else {
            pb::Status addr_status = pb::NORMAL;
            FetcherStore::choose_opt_instance(_info.region_id(), _info.peers(), _addr, addr_status, nullptr);
        }
        _request.set_select_without_leader(true);
    } else if (_op_type == pb::OP_SELECT && _state->txn_id == 0 && FLAGS_fetcher_follower_read) {
        // 多机房优化
        if (_info.learners_size() > 0) {
            pb::Status addr_status = pb::NORMAL;
            FetcherStore::choose_opt_instance(_info.region_id(), _info.peers(), _addr, addr_status, nullptr);
            pb::Status backup_status = pb::NORMAL;
            FetcherStore::choose_opt_instance(_info.region_id(), _info.learners(), _backup, backup_status, nullptr);
            bool backup_can_access = (!_backup.empty()) && (backup_status == pb::NORMAL) && 
                                        _fetcher_store->peer_status.can_access(_info.region_id(), _backup);
            if (addr_status != pb::NORMAL && backup_can_access && 
                    _fetcher_store->global_backup_type != GBT_MAIN/*全局索引降级，强制访问主集群不可以只访问learner*/) {
                _addr = _backup;
                _backup.clear();
                _state->need_statistics = false;
                _resource_insulate_read = true;
            } else if (!backup_can_access) {
                _backup.clear();
            }
        } else {
            if (_retry_times == 0) {
                if (_client_conn != nullptr && _client_conn->query_ctx->peer_index != -1) {
                    int64_t peer_index = _client_conn->query_ctx->peer_index;
                    std::vector<std::string> sorted_peers; // leader first
                    sorted_peers.emplace_back(_info.leader());
                    for (auto& peer: _info.peers()) {
                        if (_info.leader() != peer) {
                            sorted_peers.emplace_back(peer);
                        }
                    }
                    if (peer_index < sorted_peers.size()) {
                        _addr = sorted_peers[peer_index];
                        DB_WARNING("choose peer %s, index: %ld", _addr.c_str(), peer_index);
                    }
                } else {
                    pb::Status addr_status = pb::NORMAL;
                    FetcherStore::choose_opt_instance(_info.region_id(), _info.peers(), _addr, addr_status, &_backup);
                }
            }
        }
        _request.set_select_without_leader(true);
    } else if (_retry_times == 0) {
        // 重试前已经选择了normal的实例
        // 或者store返回了正确的leader
        FetcherStore::choose_other_if_dead(_info, _addr);
    }

    // 存在全局索引降级的情况，强制访问主集群的情况下不要backup
    if (_fetcher_store->global_backup_type == GBT_MAIN) {
        _backup.clear();
    }
}

ErrorType OnRPCDone::send_async() {
    _cntl.Reset();
    _cntl.set_log_id(_state->log_id());
    _response.Clear();
    if (_region_id == 0) {
        DB_DONE(FATAL, "region_id == 0");
        return E_FATAL;
    }
    brpc::ChannelOptions option;
    option.max_retry = 1;
    option.connect_timeout_ms = FLAGS_fetcher_connect_timeout;
    option.timeout_ms = FLAGS_fetcher_request_timeout;
    if (_fetcher_store->dynamic_timeout_ms > 0 && !_backup.empty() && _backup != _addr) {
        option.backup_request_ms = _fetcher_store->dynamic_timeout_ms;
    }
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
    pb::StoreService_Stub(&channel).query(&_cntl, &_request, &_response, this);
    return E_ASYNC;
}

void OnRPCDone::Run() {
    DB_DONE(DEBUG, "fetch store req: %s", _request.ShortDebugString().c_str());
    DB_DONE(DEBUG, "fetch store res: %s", _response.ShortDebugString().c_str());
    std::string remote_side = butil::endpoint2str(_cntl.remote_side()).c_str();
    int64_t query_cost = _query_time.get_time();
    if (query_cost > FLAGS_print_time_us || _retry_times > 0) {
        DB_DONE(WARNING, "version:%ld time:%ld rpc_time:%ld ip:%s",
                 _info.version(), _total_cost.get_time(), query_cost, remote_side.c_str());
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

        _fetcher_store->peer_status.set_cannot_access(_info.region_id(), remote_side);
        FetcherStore::other_normal_peer_to_leader(_info, _addr);
        bthread_usleep(_retry_times * FLAGS_retry_interval_us);
        _rpc_ctrl->task_retry(this);
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

    auto err = handle_response(remote_side);
    if (err == E_RETRY) {
        _rpc_ctrl->task_retry(this);
    } else {
        if (err != E_OK) {
            _fetcher_store->error = err;
        }
        _rpc_ctrl->task_finish(this);
    }
    return;
}

ErrorType OnRPCDone::handle_version_old() {
    SchemaFactory* schema_factory = SchemaFactory::get_instance();
    DB_DONE(WARNING, "VERSION_OLD, now:%s", _info.ShortDebugString().c_str());
    if (_response.regions_size() >= 2) {
        auto regions = _response.regions();
        regions.Clear();
        if (!_response.is_merge()) {
            for (auto r : _response.regions()) {
                DB_WARNING("version region:%s", r.ShortDebugString().c_str());
                if (end_key_compare(r.end_key(), _info.end_key()) > 0) {
                    DB_WARNING("region:%ld r.end_key:%s > info.end_key:%s",
                                r.region_id(),
                                str_to_hex(r.end_key()).c_str(),
                                str_to_hex(_info.end_key()).c_str());
                    continue;
                }
                *regions.Add() = r;
            }
        } else {
            //merge场景，踢除当前region，继续走下面流程
            for (auto r : _response.regions()) {
                if (r.region_id() == _region_id) {
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
            if (r.region_id() != _region_id) {
                BAIDU_SCOPED_LOCK(_client_conn->region_lock);
                _client_conn->region_infos[r.region_id()] = r;
                _fetcher_store->skip_region_set.insert(r.region_id());
                _fetcher_store->region_count++;
            } else {
                if (_response.leader() != "0.0.0.0:0") {
                    DB_WARNING("region_id: %ld set new_leader: %s when old_version", _region_id, r.leader().c_str());
                    r.set_leader(_response.leader());
                }
                BAIDU_SCOPED_LOCK(_client_conn->region_lock);
                _client_conn->region_infos[_region_id] = r;
                if (r.leader() != "0.0.0.0:0") {
                    _client_conn->region_infos[_region_id].set_leader(r.leader());
                }
            }
        }
        int last_seq_id = _response.has_last_seq_id()? _response.last_seq_id() : _start_seq_id;
        for (auto& r : regions) {
            pb::RegionInfo* info = nullptr;
            {       
                BAIDU_SCOPED_LOCK(_client_conn->region_lock);
                info = &(_client_conn->region_infos[r.region_id()]);
            }
            auto task = new OnRPCDone(_fetcher_store, _state, _store_request, info, 
                    _old_region_id, info->region_id(), last_seq_id, _current_seq_id, _op_type);
            _rpc_ctrl->add_new_task(task);
        }
        return E_OK;
    } else if (_response.regions_size() == 1) {
        auto regions = _response.regions();
        regions.Clear();
        for (auto r : _response.regions()) {
            if (r.region_id() != _region_id) {
                DB_WARNING("not the same region:%s",
                            r.ShortDebugString().c_str());
                return E_FATAL;
            }
            if (!(r.start_key() <= _info.start_key() &&
                    end_key_compare(r.end_key(), _info.end_key()) >= 0)) {
                DB_FATAL("store region not overlap local region, region_id:%ld",
                        _region_id);
                return E_FATAL;
            }
            DB_WARNING("version region:%s", r.ShortDebugString().c_str());
            *regions.Add() = r;
        }
        int last_seq_id = _response.has_last_seq_id()? _response.last_seq_id() : _start_seq_id;
        for (auto& r : regions) {
            pb::RegionInfo* info = nullptr;
            {
                BAIDU_SCOPED_LOCK(_client_conn->region_lock);
                _client_conn->region_infos[r.region_id()] = r;
                if (r.leader() != "0.0.0.0:0") {
                    _client_conn->region_infos[r.region_id()].set_leader(r.leader());
                }
                info = &(_client_conn->region_infos[r.region_id()]);
            }
            auto task = new OnRPCDone(_fetcher_store, _state, _store_request, info, 
                        _old_region_id, info->region_id(), last_seq_id, _current_seq_id, _op_type);
            _rpc_ctrl->add_new_task(task);
        }
        return E_OK;
    }
    return E_FATAL;
}

ErrorType OnRPCDone::handle_response(const std::string& remote_side) {
    SchemaFactory* schema_factory = SchemaFactory::get_instance();
    if (_cntl.has_backup_request()) {
        DB_DONE(WARNING, "has_backup_request");
        has_backup_send_request << _query_time.get_time();
        // backup先回，整体时延包含dynamic_timeout_ms，不做统计
        // remote_side != _addr 说明backup先回
        if (remote_side != _addr) {
            //业务快速置状态
            schema_factory->update_instance(_addr, pb::BUSY, true, false);
            _state->need_statistics = false;
            // backup为learner需要设置_resource_insulate_read为true
            if (_info.learners_size() > 0) {
                for (auto& peer : _info.learners()) {
                    if (peer == remote_side) {
                        _resource_insulate_read = true;
                        break;
                    }
                }
            }
        }
    } else {
        if (_response.errcode() != pb::SUCCESS) {
            // 失败请求会重试，可能统计的时延不准，不做统计
            _state->need_statistics = false;
        } else {
            // 请求结束再次判断请求的实例状态，非NORMAL则时延不可控，不做统计
            if (_state->need_statistics) {
                auto status = SchemaFactory::get_instance()->get_instance_status(_addr);
                if (status.status != pb::NORMAL) {
                    _state->need_statistics = false;
                }
            }
        }
    }
    // 使用read_index、指定访问store集群进行资源隔离、访问learner，读失败，不重试leader
    if (_resource_insulate_read 
            && (_response.errcode() == pb::REGION_NOT_EXIST 
                || _response.errcode() == pb::LEARNER_NOT_READY 
                || _response.errcode() == pb::NOT_LEADER)) {
        DB_DONE(WARNING, "peer/learner not ready, errcode: %s, errmsg: %s", 
                pb::ErrCode_Name(_response.errcode()).c_str(), _response.errmsg().c_str());
        _fetcher_store->peer_status.set_cannot_access(_info.region_id(), _addr);
        bthread_usleep(_retry_times * FLAGS_retry_interval_us);
        return E_RETRY;
    }
    // 要求读主、store version old、store正在shutdown/init，在leader重试
    if (_response.errcode() == pb::NOT_LEADER) {
        // 兼容not leader报警，匹配规则 NOT_LEADER.*retry:4
        DB_DONE(WARNING, "NOT_LEADER, new_leader:%s, retry:%d", _response.leader().c_str(), _retry_times);
        // 临时修改，后面改成store_access
        if (_retry_times > 1 && _response.leader() == "0.0.0.0:0") {
            schema_factory->update_instance(remote_side, pb::FAULTY, false, false);
        }
        if (_response.leader() != "0.0.0.0:0") {
            // store返回了leader，则相信store，不判断normal
            _info.set_leader(_response.leader());
            schema_factory->update_leader(_info);
        } else {
            FetcherStore::other_normal_peer_to_leader(_info, _addr);
           
        }
        if (_state->txn_id != 0 ) {
            BAIDU_SCOPED_LOCK(_client_conn->region_lock);
            _client_conn->region_infos[_region_id].set_leader(_info.leader());
        }
        // leader切换在秒级
        bthread_usleep(_retry_times * FLAGS_retry_interval_us);
        return E_RETRY;
    }
    if (_response.errcode() == pb::RETRY_LATER) {
        DB_DONE(WARNING, "request failed, errcode: %s, errmsg: %s", pb::ErrCode_Name(_response.errcode()).c_str(), _response.errmsg().c_str());
        if (FLAGS_fetcher_follower_read) {
            // choose another peer to retry
            schema_factory->update_instance(remote_side, pb::BUSY, false, false);
            FetcherStore::other_normal_peer_to_leader(_info, _addr);
        }
        bthread_usleep(_retry_times * FLAGS_retry_interval_us);
        return E_RETRY;
    }
    if (_response.errcode() == pb::DISABLE_WRITE_TIMEOUT || _response.errcode() == pb::IN_PROCESS) {
        DB_DONE(WARNING, "request failed, errcode: %s", pb::ErrCode_Name(_response.errcode()).c_str());
        bthread_usleep(_retry_times * FLAGS_retry_interval_us);
        return E_RETRY;
    }

    if (_response.errcode() == pb::VERSION_OLD) {
        return handle_version_old();
    }
    if (_response.errcode() == pb::TXN_IS_ROLLBACK) {
        DB_DONE(WARNING, "TXN_IS_ROLLBACK, new_leader:%s", _response.leader().c_str());
        return E_RETURN;
    }
    if (_response.errcode() == pb::REGION_NOT_EXIST || _response.errcode() == pb::INTERNAL_ERROR) {
        DB_DONE(WARNING, "new_leader:%s，errcode: %s", _response.leader().c_str(), pb::ErrCode_Name(_response.errcode()).c_str());
        if (_response.errcode() == pb::REGION_NOT_EXIST) {
            pb::RegionInfo tmp_info;
            // 已经被merge了并且store已经删掉了，按正常处理
            int ret = schema_factory->get_region_info(_info.table_id(), _region_id, tmp_info);
            if (ret != 0) {
                DB_DONE(WARNING, "REGION_NOT_EXIST, region merge, new_leader:%s", _response.leader().c_str());
                return E_OK;
            }
        }
        schema_factory->update_instance(remote_side, pb::FAULTY, false, false);
        FetcherStore::other_normal_peer_to_leader(_info, _addr);
        return E_RETRY;
    }
    if (_response.errcode() != pb::SUCCESS) {
        if (_response.has_mysql_errcode()) {
            BAIDU_SCOPED_LOCK(_fetcher_store->region_lock);
            _fetcher_store->error_code = (MysqlErrCode)_response.mysql_errcode();
            _fetcher_store->error_msg.str(_response.errmsg());
        }
        DB_DONE(WARNING, "errcode:%s, mysql_errcode:%d, msg:%s, failed",
                pb::ErrCode_Name(_response.errcode()).c_str(), _response.mysql_errcode(), _response.errmsg().c_str());
        if (_fetcher_store->error_code == ER_DUP_ENTRY) {
            return E_WARNING;
        }
        return E_FATAL;
    }

    if (_response.records_size() > 0) {
        int64_t main_table_id = _info.has_main_table_id() ? _info.main_table_id() : _info.table_id();
        if (main_table_id <= 0) {
            DB_DONE(FATAL, "impossible branch");
            return E_FATAL;
        }
        std::map<int64_t, std::vector<SmartRecord>> result_records;
        std::vector<std::string> return_str_records;
        std::vector<std::string> return_str_old_records;
        SmartRecord record_template = schema_factory->new_record(main_table_id);
        for (auto& records_pair : _response.records()) {
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
                std::vector<std::string>&  str_records = _fetcher_store->return_str_records[_info.partition_id()];
                str_records.insert(str_records.end(), return_str_records.begin(), return_str_records.end());
            }
            if (!return_str_old_records.empty()) {
                std::vector<std::string>&  str_old_records =  _fetcher_store->return_str_old_records[_info.partition_id()];
                str_old_records.insert(str_old_records.end(), return_str_old_records.begin(), return_str_old_records.end());
            }
        }
    }
    if (_response.has_scan_rows()) {
        _fetcher_store->scan_rows += _response.scan_rows();
    }
    if (_response.has_filter_rows()) {
        _fetcher_store->filter_rows += _response.filter_rows();
    }
    if (_response.has_last_insert_id()) {
        _client_conn->last_insert_id = _response.last_insert_id();
    }
    if (_op_type != pb::OP_SELECT && _op_type != pb::OP_SELECT_FOR_UPDATE && _op_type != pb::OP_ROLLBACK && _op_type != pb::OP_COMMIT) {
        _fetcher_store->affected_rows += _response.affected_rows();
        _client_conn->txn_affected_rows += _response.affected_rows();
        // 事务限制affected_rows，非事务限制会导致部分成功
        if (_client_conn->txn_affected_rows > FLAGS_max_affected_rows && _state->txn_id != 0) {
            DB_DONE(FATAL, "_affected_row:%ld > %ld FLAGS_max_affected_rows", 
                    _client_conn->txn_affected_rows.load(), FLAGS_max_affected_rows);
            return E_BIG_SQL;
        }
        return E_OK;
    }
    if (!_response.leader().empty() && _response.leader() != "0.0.0.0:0" && _response.leader() != _info.leader()) {
        _info.set_leader(_response.leader());
        schema_factory->update_leader(_info);
        if (_state->txn_id != 0) {
            BAIDU_SCOPED_LOCK(_client_conn->region_lock);
            _client_conn->region_infos[_region_id].set_leader(_response.leader());
        }
    }
    TimeCost cost;
    if (_response.row_values_size() > 0) {
        _fetcher_store->row_cnt += _response.row_values_size();
    }
    // TODO reduce mem used by streaming
    if ((!_state->is_full_export) && (_fetcher_store->row_cnt > FLAGS_max_select_rows)) {
        DB_DONE(FATAL, "_row_cnt:%ld > %ld max_select_rows", _fetcher_store->row_cnt.load(), FLAGS_max_select_rows);
        return E_BIG_SQL;
    }
    std::shared_ptr<RowBatch> batch = std::make_shared<RowBatch>();
    std::vector<int64_t> ttl_batch;
    ttl_batch.reserve(100);
    bool global_ddl_with_ttl = (_response.row_values_size() > 0 && _response.row_values_size() == _response.ttl_timestamp_size()) ? true : false;
    int ttl_idx = 0;
    int64_t used_size = 0;
    for (auto& pb_row : _response.row_values()) {
        if (pb_row.tuple_values_size() != _response.tuple_ids_size()) {
            // brpc SelectiveChannel+backup_request有bug，pb的repeated字段merge到一起了
            SQL_TRACE("backup_request size diff, tuple_values_size:%d tuple_ids_size:%d rows:%d", 
                    pb_row.tuple_values_size(), _response.tuple_ids_size(), _response.row_values_size());
            for (auto id : _response.tuple_ids()) {
                SQL_TRACE("tuple_id:%d  ", id);
            }
            return E_RETRY;
        }
        std::unique_ptr<MemRow> row = _state->mem_row_desc()->fetch_mem_row();
        for (int i = 0; i < _response.tuple_ids_size(); i++) {
            int32_t tuple_id = _response.tuple_ids(i);
            row->from_string(tuple_id, pb_row.tuple_values(i));
        }
        used_size += row->used_size();
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
            int64_t time_us = _response.ttl_timestamp(ttl_idx++);
            ttl_batch.emplace_back(time_us);
            DB_DEBUG("region_id: %ld, ttl_timestamp: %ld", _region_id, time_us);
        }
    }
    if (global_ddl_with_ttl) {
        BAIDU_SCOPED_LOCK(_fetcher_store->region_lock);
        _fetcher_store->region_id_ttl_timestamp_batch[_region_id] = ttl_batch;
        DB_DEBUG("_region_id: %ld, ttl_timestamp_size: %ld", _region_id, ttl_batch.size());
    }
    if (_response.has_cmsketch() && _state->cmsketch != nullptr) {
        _state->cmsketch->add_proto(_response.cmsketch());
        DB_DONE(WARNING, "cmsketch:%s", _response.cmsketch().ShortDebugString().c_str());
    }
    {
        BAIDU_SCOPED_LOCK(_fetcher_store->region_lock);
        // merge可能会重复请求相同的region_id
        if (_fetcher_store->region_batch.count(_region_id) == 1) {
            _fetcher_store->region_batch[_region_id] = batch;
        } else {
            //分裂单独处理start_key_sort
            _fetcher_store->start_key_sort.emplace(_info.start_key(), _region_id);
            _fetcher_store->region_batch[_region_id] = batch;
        }
    }

    if (_trace_node != nullptr) {
        std::string desc = "baikalDB FetcherStore send_request "
                            + pb::ErrCode_Name(_response.errcode());
        _trace_node->set_description(_trace_node->description() + " " + desc);
        _trace_node->set_total_time(_total_cost.get_time());
        _trace_node->set_affect_rows(_response.affected_rows());
        pb::TraceNode* local_trace = _trace_node->add_child_nodes();
        if (_response.has_errmsg() && _response.errcode() == pb::SUCCESS) {
            pb::TraceNode trace;
            if (!trace.ParseFromString(_response.errmsg())) {
                DB_FATAL("parse from pb fail");
            } else {
                (*local_trace) = trace;
            }
        }
    }
    if (cost.get_time() > FLAGS_print_time_us) {
        DB_DONE(WARNING, "parse time:%ld rows:%lu", cost.get_time(), batch->size());
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
        _has_fill_request = true;
    }

    // 选择请求的store地址
    select_addr();

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

void FetcherStore::choose_other_if_dead(pb::RegionInfo& info, std::string& addr) {
    SchemaFactory* schema_factory = SchemaFactory::get_instance();
    auto status = schema_factory->get_instance_status(addr);
    if (status.status != pb::DEAD) {
        return;
    }

    std::vector<std::string> normal_peers;
    for (auto& peer: info.peers()) {
        auto status = schema_factory->get_instance_status(peer);
        if (status.status == pb::NORMAL) {
            normal_peers.push_back(peer);
        }
    }
    if (normal_peers.size() > 0) {
        uint32_t i = butil::fast_rand() % normal_peers.size();
        addr = normal_peers[i];
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
                write_binlog_param.txn_id = state->txn_id;
                write_binlog_param.log_id = log_id;
                write_binlog_param.primary_region_id = client_conn->primary_region_id;
                write_binlog_param.global_conn_id = client_conn->get_global_conn_id();
                write_binlog_param.username = client_conn->user_info->username;
                write_binlog_param.ip = client_conn->ip;
                write_binlog_param.client_conn = client_conn;
                write_binlog_param.fetcher_store = this;
                binlog_ctx->set_start_ts(timestamp);
            }
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
    client_conn = state->client_conn();
    region_count += region_infos.size();
    global_backup_type = backup_type;
    if (region_infos.size() == 0) {
        DB_WARNING("region_infos size == 0, op_type:%s", pb::OpType_Name(op_type).c_str());
        return E_OK;
    }

    dynamic_timeout_ms = get_dynamic_timeout_ms(store_request, op_type, state->sign);
    sign_latency = get_sign_latency(op_type, state->sign);
    // 预分配空洞
    for (auto& pair : region_infos) {
        start_key_sort.emplace(pair.second.start_key(), pair.first);
        region_batch[pair.first] = nullptr;
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
                return E_OK;
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
            return E_OK;
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
                return E_OK;
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
        return E_OK;
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
