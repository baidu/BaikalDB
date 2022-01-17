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
#include "trace_state.h"
#ifdef BAIDU_INTERNAL
#include "baidu/rpc/reloadable_flags.h"
#else
#include "brpc/reloadable_flags.h"
#endif

namespace baikaldb {

DEFINE_int64(retry_interval_us, 500 * 1000, "retry interval ");
DEFINE_int32(single_store_concurrency, 20, "max request for one store");
DEFINE_int64(max_select_rows, 10000000, "query will be fail when select too much rows");
DEFINE_int64(print_time_us, 10000, "print log when time_cost > print_time_us(us)");
DEFINE_int64(binlog_alarm_time_s, 30, "alarm, > binlog_alarm_time_s from prewrite to commit");
#ifdef BAIDU_INTERNAL
BAIDU_RPC_VALIDATE_GFLAG(print_time_us, brpc::NonNegativeInteger);
#else
BRPC_VALIDATE_GFLAG(print_time_us, brpc::NonNegativeInteger);
#endif
DEFINE_int32(fetcher_request_timeout, 100000,
                    "store as server request timeout, default:10000ms");
DEFINE_int32(fetcher_connect_timeout, 1000,
                    "store as server connect timeout, default:1000ms");
DEFINE_int64(db_row_number_to_check_memory, 10240, "do memory limit when row number more than #, default: 10240");
DEFINE_bool(fetcher_follower_read, true, "where allow follower read for fether");
DEFINE_bool(fetcher_learner_read, false, "where allow learner read for fether");
DECLARE_int32(transaction_clear_delay_ms);
DEFINE_bool(use_dynamic_timeout, false, "whether use dynamic_timeout");
#ifdef BAIDU_INTERNAL
BAIDU_RPC_VALIDATE_GFLAG(use_dynamic_timeout, brpc::PassValidate);
#else
BRPC_VALIDATE_GFLAG(use_dynamic_timeout, brpc::PassValidate);
#endif
                    
ErrorType FetcherStore::send_request(
        RuntimeState* state,
        ExecNode* store_request,
        pb::RegionInfo& info,
        pb::TraceNode* trace_node,
        int64_t old_region_id,
        int64_t region_id,
        uint64_t log_id,
        int retry_times,
        int start_seq_id,
        int current_seq_id,
        pb::OpType op_type) {
    pb::StoreReq req;
    pb::StoreRes res;
    TimeCost total_cost;
    if (trace_node != nullptr) {
        req.set_is_trace(true);
    }

    if (state->explain_type == ANALYZE_STATISTICS) {
        if (state->cmsketch != nullptr) {
            pb::AnalyzeInfo* info = req.mutable_analyze_info();
            info->set_depth(state->cmsketch->get_depth());
            info->set_width(state->cmsketch->get_width());
            info->set_sample_rows(state->cmsketch->get_sample_rows());
            info->set_table_rows(state->cmsketch->get_table_rows());
        }
    }
    ScopeGuard auto_update_trace([&]() {
        if (trace_node != nullptr) {
            std::string desc = "baikalDB FetcherStore send_request "
                               + pb::ErrCode_Name(res.errcode());
            trace_node->set_description(trace_node->description() + " " + desc);
            trace_node->set_total_time(total_cost.get_time());
            trace_node->set_affect_rows(res.affected_rows());
            pb::TraceNode* local_trace = trace_node->add_child_nodes();
            if (res.has_errmsg() && res.errcode() == pb::SUCCESS) {
                pb::TraceNode trace;
                if (!trace.ParseFromString(res.errmsg())) {
                    DB_FATAL("parse from pb fail");
                } else {
                    (*local_trace) = trace;
                }
            }
        }
    });
    if (error != E_OK) {
        DB_WARNING("recieve error, need not requeset to region_id: %ld, log_id: %lu", region_id, log_id);
        return E_WARNING;
    }
    if (state->is_cancelled()) {
        DB_FATAL("region_id: %ld is cancelled, log_id: %lu op_type:%s", 
                region_id, log_id, pb::OpType_Name(op_type).c_str());
        return E_OK;
    }
    //DB_WARNING("region_info; txn: %ld, %s, %lu", _txn_id, info.ShortDebugString().c_str(), records.size());
    if (retry_times >= 5) {
        DB_WARNING("region_id: %ld, txn_id: %lu, log_id:%lu, op_type:%s rpc error; retry:%d",
            region_id, state->txn_id, log_id, pb::OpType_Name(op_type).c_str(), retry_times);
        return E_FATAL;
    }
    // for exec next_statement_after_begin, begin must be added
    if (current_seq_id == 2 && state->single_sql_autocommit() == false) {
        //DB_WARNING("start seq id is reset to 1, region_id: %ld", region_id);
        start_seq_id = 1;
    }
    bool need_copy_cache_plan = true;
    if (state->txn_id != 0) {
        BAIDU_SCOPED_LOCK(state->client_conn()->region_lock);
        if (state->client_conn()->region_infos.count(region_id) == 0) {
            //DB_WARNING("start seq id is reset to 1, region_id: %ld", region_id);
            start_seq_id = 1;
            no_copy_cache_plan_set.emplace(region_id);
        }
        if (no_copy_cache_plan_set.count(region_id) != 0) {
            need_copy_cache_plan = false;
        }
    }
    TimeCost cost;
    auto client_conn = state->client_conn();
    SchemaFactory* schema_factory = SchemaFactory::get_instance();
    brpc::Controller cntl;
    cntl.set_log_id(log_id);
    if (info.leader() == "0.0.0.0:0" || info.leader() == "") {
        info.set_leader(rand_peer(info));
    }
    req.set_db_conn_id(client_conn->get_global_conn_id());
    req.set_op_type(op_type);
    req.set_region_id(region_id);
    req.set_region_version(info.version());
    req.set_log_id(log_id);
    req.set_sql_sign(state->sign);
    for (auto& desc : state->tuple_descs()) {
        if (desc.has_tuple_id()){
            req.add_tuples()->CopyFrom(desc);
        }
    }
    pb::TransactionInfo* txn_info = req.add_txn_infos();
    txn_info->set_txn_id(state->txn_id);
    txn_info->set_seq_id(current_seq_id);
    txn_info->set_autocommit(state->single_sql_autocommit());
    // 全局二级索引online ddl 设置超时时间 40 s
    if (client_conn->txn_timeout > 0) {
        txn_info->set_txn_timeout(client_conn->txn_timeout);
    }
    for (int id : client_conn->need_rollback_seq) {
        txn_info->add_need_rollback_seq(id);
    }
    txn_info->set_start_seq_id(start_seq_id);
    txn_info->set_optimize_1pc(state->optimize_1pc());
    if (state->txn_id != 0) {
        txn_info->set_primary_region_id(client_conn->primary_region_id.load());
        if (need_process_binlog(state, op_type)) {
            auto binlog_ctx = client_conn->get_binlog_ctx();
            txn_info->set_commit_ts(binlog_ctx->commit_ts());
            txn_info->set_open_binlog(true);
        }
        if (client_conn->primary_region_id != -1
            && client_conn->primary_region_id != region_id
            && !primary_timestamp_updated) {
            if (butil::gettimeofday_us() - client_conn->txn_pri_region_last_exec_time > (FLAGS_transaction_clear_delay_ms / 2) * 1000LL) {
                primary_timestamp_updated = true;
                txn_info->set_need_update_primary_timestamp(true);
                client_conn->txn_pri_region_last_exec_time = butil::gettimeofday_us();
            }
        } else if (client_conn->primary_region_id == region_id) {
            client_conn->txn_pri_region_last_exec_time = butil::gettimeofday_us();
        }
    }

    // DB_WARNING("txn_id: %lu, start_seq_id: %d, autocommit:%d", _txn_id, start_seq_id, state->autocommit());
    // 将缓存的plan中seq_id >= start_seq_id的部分追加到request中
    // rollback cmd does not need to send cache
    //DB_WARNING("op_type: %d, start_seq_id:%d, cache_plans_size: %d",
    //            op_type, start_seq_id, client_conn->cache_plans.size());
    if (start_seq_id >= 0 && op_type != pb::OP_COMMIT) {
        for (auto& pair : client_conn->cache_plans) {
            //DB_WARNING("op_type: %d, pair.first:%d, start_seq_id:%d", op_type, pair.first, start_seq_id);
            auto& plan_item = pair.second;
            if ((plan_item.op_type != pb::OP_BEGIN)
                && (pair.first < start_seq_id || pair.first >= current_seq_id)) {
                continue;
            }
            if (op_type == pb::OP_PREPARE && plan_item.op_type == pb::OP_PREPARE) {
                continue;
            }
            if (!need_copy_cache_plan && plan_item.op_type != pb::OP_BEGIN
                && !state->single_txn_cached()) {
                DB_DEBUG("not copy cache region_id: %ld, log_id:%lu txn_id:%lu", region_id, log_id, state->txn_id);
                continue;
            }
            // rollback只带上begin
            if (op_type == pb::OP_ROLLBACK && plan_item.op_type != pb::OP_BEGIN) {
                continue;
            }
            if (plan_item.tuple_descs.size() > 0 &&
                plan_item.op_type != pb::OP_BEGIN &&
                static_cast<DMLNode*>(plan_item.root)->global_index_id() != info.table_id()
                ) {
                /*DB_WARNING("TransactionNote: cache_item table_id mismatch,"
                    " cache global_index_id: %ld, region_info index_id : %ld",
                    static_cast<DMLNode*>(plan_item.root)->global_index_id(), info.table_id());
                */
                continue;
            }
            pb::CachePlan* pb_cache_plan = txn_info->add_cache_plans();
            pb_cache_plan->set_op_type(plan_item.op_type);
            pb_cache_plan->set_seq_id(plan_item.sql_id);
            ExecNode::create_pb_plan(old_region_id, pb_cache_plan->mutable_plan(), plan_item.root);
            if (plan_item.op_type != pb::OP_BEGIN && !state->single_txn_cached()) {
                DB_WARNING("TranstationNote: copy cache region_id: %ld, old_region_id:%ld log_id:%lu txn_id:%lu "
                "start_seq_id:%d current_seq_id:%d cache_plan:%s", 
                    region_id, old_region_id, log_id, state->txn_id, start_seq_id, current_seq_id,
                    pb_cache_plan->ShortDebugString().c_str());
            }
            for (auto& desc : plan_item.tuple_descs) {
                if (desc.has_tuple_id()){
                    pb_cache_plan->add_tuples()->CopyFrom(desc);
                }
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
    ExecNode::create_pb_plan(old_region_id, req.mutable_plan(), store_request);

    std::string addr = info.leader();
    std::string backup;
    // 事务读也读leader
    if (op_type == pb::OP_SELECT && state->txn_id == 0 && 
        info.learners_size() > 0 && (FLAGS_fetcher_learner_read || state->need_learner_backup())) {
        // 多机房优化
        if (retry_times == 0) {
            choose_opt_instance(info.region_id(), info.learners(), addr, nullptr);
        }
        req.set_select_without_leader(true);
    } else if (op_type == pb::OP_SELECT && state->txn_id == 0 && FLAGS_fetcher_follower_read) {
        // 多机房优化
        if (info.learners_size() > 0) {
            choose_opt_instance(info.region_id(), info.peers(), addr, nullptr);
            choose_opt_instance(info.region_id(), info.learners(), backup, nullptr);
        } else {
            if (retry_times == 0) {
                choose_opt_instance(info.region_id(), info.peers(), addr, &backup);
            }
        }
        req.set_select_without_leader(true);
    } else if (retry_times == 0) {
        // 重试前已经选择了normal的实例
        // 或者store返回了正确的leader
        choose_other_if_faulty(info, addr);
    }
    brpc::ChannelOptions option;
    option.max_retry = 1;
    option.connect_timeout_ms = FLAGS_fetcher_connect_timeout;
    option.timeout_ms = FLAGS_fetcher_request_timeout;
    if (dynamic_timeout_ms > 0 && !backup.empty() && backup != addr) {
        option.backup_request_ms = dynamic_timeout_ms;
    }
    brpc::SelectiveChannel channel;
    int ret = 0;
    ret = channel.Init("rr", &option);
    if (ret != 0) {
        DB_WARNING("SelectiveChannel init failed, addr:%s, ret:%d, region_id: %ld, log_id:%lu",
                addr.c_str(), ret, region_id, log_id);
        return E_FATAL;
    }
    // sub_channel do not need backup_request_ms
    option.backup_request_ms = -1;
    option.max_retry = 0;
    brpc::Channel* sub_channel1 = new brpc::Channel;
    ret = sub_channel1->Init(addr.c_str(), &option);
    if (ret != 0) {
        DB_WARNING("channel init failed, addr:%s, ret:%d, region_id: %ld, log_id:%lu",
                addr.c_str(), ret, region_id, log_id);
        delete sub_channel1;
        return E_FATAL;
    }
    channel.AddChannel(sub_channel1, NULL);
    if (dynamic_timeout_ms > 0 && !backup.empty() && backup != addr) {
#ifdef BAIDU_INTERNAL
        //开源版brpc和内部不大一样
        brpc::SocketId sub_id2;
        brpc::Channel* sub_channel2 = new brpc::Channel;
        ret = sub_channel2->Init(backup.c_str(), &option);
        if (ret != 0) {
            DB_WARNING("channel init failed, backup:%s, ret:%d, region_id: %ld, log_id:%lu",
                    backup.c_str(), ret, region_id, log_id);
            delete sub_channel2;
            return E_FATAL;
        }
        channel.AddChannel(sub_channel2, &sub_id2);
        // SelectiveChannel返回的sub_id2可以当做ExcludedServers使用
        // 保证第一次请求必定走addr，并且这个ExcludedServers不影响backup_request
        brpc::ExcludedServers* exclude = brpc::ExcludedServers::Create(1);
        exclude->Add(sub_id2);
        cntl.set_excluded_servers(exclude);
#endif
    } else {
        //命中backup可以不cancel
        client_conn->insert_callid(addr, region_id, cntl.call_id());
    }

    TimeCost query_time;
    pb::StoreService_Stub(&channel).query(&cntl, &req, &res, NULL);

    DB_DEBUG("fetch store req: %s", req.ShortDebugString().c_str());
    DB_DEBUG("fetch store res: %s", res.ShortDebugString().c_str());
    if (cost.get_time() > FLAGS_print_time_us || retry_times > 0) {
        DB_WARNING("op_type:%s, lock:%ld, wait region_id: %ld retry_times:%d "
                "version:%ld time:%ld rpc_time: %ld log_id:%lu txn_id: %lu, ip:%s, addr:%s backup:%s",
                pb::OpType_Name(op_type).c_str(), client_lock_tm, region_id, retry_times,
                info.version(), cost.get_time(), query_time.get_time(), log_id, state->txn_id,
                butil::endpoint2str(cntl.remote_side()).c_str(), addr.c_str(), backup.c_str());
    }
    if (cntl.Failed()) {
        DB_WARNING("call failed region_id: %ld, errcode:%d, error:%s, log_id:%lu, op_type:%s",
                region_id, cntl.ErrorCode(), cntl.ErrorText().c_str(), log_id, pb::OpType_Name(op_type).c_str());
        // 只有网络相关错误码才重试
        if (cntl.ErrorCode() != ETIMEDOUT &&
                cntl.ErrorCode() != ECONNREFUSED &&
                cntl.ErrorCode() != EHOSTDOWN &&
                cntl.ErrorCode() != ECANCELED) {
            return E_FATAL;
        }
        if (op_type != pb::OP_SELECT && cntl.ErrorCode() == ECANCELED) {
            return E_FATAL;
        }
        other_normal_peer_to_leader(info, addr);
        bthread_usleep(retry_times * FLAGS_retry_interval_us);
        return send_request(state, store_request, info, trace_node, old_region_id, region_id, log_id,
                  retry_times + 1, start_seq_id, current_seq_id, op_type);
    }
    if (cntl.has_backup_request()) {
        DB_WARNING("has_backup_request region_id: %ld, addr:%s, backup:%s, log_id:%lu",
                region_id, addr.c_str(), backup.c_str(), log_id);
        SchemaFactory* factory = SchemaFactory::get_instance();
        //业务快速置状态
        factory->update_instance(addr, pb::FAULTY, true);
    }
    if (res.errcode() == pb::NOT_LEADER) {
        DB_WARNING("NOT_LEADER, region_id: %ld, addr:%s retry:%d, new_leader:%s, log_id:%lu",
                region_id, addr.c_str(), retry_times, res.leader().c_str(), log_id);

        if (res.leader() != "0.0.0.0:0") {
            // store返回了leader，则相信store，不判断normal
            info.set_leader(res.leader());
            schema_factory->update_leader(info);
        } else {
            other_normal_peer_to_leader(info, addr);
           
        }
        if (state->txn_id != 0 ) {
            BAIDU_SCOPED_LOCK(client_conn->region_lock);
            client_conn->region_infos[region_id].set_leader(info.leader());
        }
        // leader切换在秒级
        bthread_usleep(retry_times * FLAGS_retry_interval_us);
        return send_request(state, store_request, info, trace_node, old_region_id, region_id, log_id,
             retry_times + 1, start_seq_id, current_seq_id, op_type);
    }
    if (res.errcode() == pb::DISABLE_WRITE_TIMEOUT) {
        // region正在分裂, 处于禁写状态
        DB_WARNING("DISABLE_WRITE_TIMEOUT, region_id: %ld, addr:%s retry:%d, log_id:%lu",
                   region_id, addr.c_str(), retry_times, log_id);
        bthread_usleep(retry_times * FLAGS_retry_interval_us);
        return send_request(state, store_request, info, trace_node, old_region_id, region_id, log_id,
                            retry_times + 1, start_seq_id, current_seq_id, op_type);
    }
    if (res.errcode() == pb::RETRY_LATER) {
        DB_WARNING("RETRY_LATER, region_id: %ld, retry:%d, log_id:%lu, op:%d, start_seq_id:%d current_seq_id:%d",
                region_id, retry_times, log_id, op_type, start_seq_id, current_seq_id);
        bthread_usleep(retry_times * FLAGS_retry_interval_us);
        return send_request(state, store_request, info, trace_node, old_region_id, region_id, log_id,
                  retry_times + 1, start_seq_id, current_seq_id,  op_type);
    }
    if (res.errcode() == pb::IN_PROCESS) {
        DB_WARNING("txn IN_PROCESS, region_id: %ld, retry:%d, log_id:%lu, op:%d, start_seq_id:%d current_seq_id:%d",
                region_id, retry_times, log_id, op_type, start_seq_id, current_seq_id);
        bthread_usleep(retry_times * FLAGS_retry_interval_us);
        return send_request(state, store_request, info, trace_node, old_region_id, region_id, log_id,
                  retry_times + 1, start_seq_id, current_seq_id,  op_type);
    }
    //todo 需要处理分裂情况
    if (res.errcode() == pb::VERSION_OLD) {
        DB_WARNING("VERSION_OLD, region_id: %ld, start_seq_id:%d retry:%d, now:%s, log_id:%lu",
                region_id, start_seq_id, retry_times, info.ShortDebugString().c_str(), log_id);
        if (res.regions_size() >= 2) {
            auto regions = res.regions();
            regions.Clear();
            if (!res.is_merge()) {
                for (auto r : res.regions()) {
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
                for (auto r : res.regions()) {
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
                    skip_region_set.insert(r.region_id());
                    state->region_count++;
                } else {
                    if (res.leader() != "0.0.0.0:0") {
                        DB_WARNING("region_id: %ld set new_leader: %s when old_version", region_id, r.leader().c_str());
                        r.set_leader(res.leader());
                    }
                    BAIDU_SCOPED_LOCK(client_conn->region_lock);
                    client_conn->region_infos[region_id].set_end_key(r.start_key());
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
                ret = send_request(state, store_request, r, trace_node, old_region_id, r.region_id(),
                         log_id, retry_times, last_seq_id, current_seq_id, op_type);
                if (ret != E_OK) {
                    DB_WARNING("retry failed, region_id: %ld, log_id:%lu, txn_id: %lu",
                            r.region_id(), log_id, state->txn_id);
                    return ret;
                }
            }
            return E_OK;
        } else if (res.regions_size() == 1) {
            auto regions = res.regions();
            regions.Clear();
            for (auto r : res.regions()) {
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
            int last_seq_id = res.has_last_seq_id()? res.last_seq_id() : start_seq_id;
            for (auto& r : regions) {
                auto r_copy = r;
                {
                    BAIDU_SCOPED_LOCK(client_conn->region_lock);
                    client_conn->region_infos[region_id].set_start_key(r_copy.start_key());
                    client_conn->region_infos[region_id].set_end_key(r_copy.end_key());
                    client_conn->region_infos[region_id].set_version(r_copy.version());
                    if (r_copy.leader() != "0.0.0.0:0") {
                        client_conn->region_infos[region_id].set_leader(r_copy.leader());
                    }
                }
                ret = send_request(state, store_request, r_copy, trace_node, old_region_id, r_copy.region_id(),
                                   log_id, retry_times + 1, last_seq_id, current_seq_id, op_type);
                if (ret != E_OK) {
                    DB_WARNING("retry failed, region_id: %ld, log_id:%lu, txn_id: %lu",
                               r_copy.region_id(), log_id, state->txn_id);
                    return E_FATAL;
                }
            }
            return E_OK;
        }
        return E_FATAL;
    }
    if (res.errcode() == pb::TXN_IS_ROLLBACK) {
        DB_WARNING("TXN_IS_ROLLBACK, region_id:%ld, txn_id:%lu, new_leader:%s, log_id:%lu op_type:%s",
                region_id, state->txn_id, res.leader().c_str(), log_id, pb::OpType_Name(op_type).c_str());
        return E_RETURN;
    }
    if (res.errcode() == pb::REGION_NOT_EXIST || res.errcode() == pb::INTERNAL_ERROR) {
        DB_WARNING("REGION_NOT_EXIST, region_id:%ld, retry:%d, new_leader:%s, log_id:%lu",
                region_id, retry_times, res.leader().c_str(), log_id);
        other_normal_peer_to_leader(info, addr);
        //bthread_usleep(retry_times * FLAGS_retry_interval_us);
        return send_request(state, store_request, info, trace_node, old_region_id, region_id, log_id,
                   retry_times + 1, start_seq_id, current_seq_id, op_type);
    }
    if (res.errcode() != pb::SUCCESS) {
        if (res.has_mysql_errcode()) {
            BAIDU_SCOPED_LOCK(region_lock);
            state->error_code = (MysqlErrCode)res.mysql_errcode();
            state->error_msg.str(res.errmsg());
        }
        DB_WARNING("errcode:%d, mysql_errcode:%d, msg:%s, failed, instance:%s region_id:%ld, log_id:%lu",
                res.errcode(), res.mysql_errcode(), res.errmsg().c_str(), addr.c_str(), region_id, log_id);
        if (state->error_code == ER_DUP_ENTRY) {
            return E_WARNING;
        }
        return E_FATAL;
    }

    if (res.records_size() > 0) {
        int64_t main_table_id = info.has_main_table_id() ? info.main_table_id() : info.table_id();
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
                //DB_WARNING("record: %s", record->debug_string().c_str());
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
    if (res.has_scan_rows()) {
        scan_rows += res.scan_rows();
    }
    if (res.has_filter_rows()) {
        filter_rows += res.filter_rows();
    }
    if (res.has_last_insert_id()) {
        client_conn->last_insert_id = res.last_insert_id();
    }
    if (op_type != pb::OP_SELECT && op_type != pb::OP_SELECT_FOR_UPDATE) {
        affected_rows += res.affected_rows();
        return E_OK;
    }
    if (!res.leader().empty() && res.leader() != "0.0.0.0:0" && res.leader() != info.leader()) {
        info.set_leader(res.leader());
        schema_factory->update_leader(info);
        if (state->txn_id != 0) {
            BAIDU_SCOPED_LOCK(client_conn->region_lock);
            client_conn->region_infos[region_id].set_leader(res.leader());
        }
    }
    cost.reset();
    if (res.row_values_size() > 0) {
        row_cnt += res.row_values_size();
    }
    // TODO reduce mem used by streaming
    if ((!state->is_full_export) && (row_cnt > FLAGS_max_select_rows)) {
        DB_FATAL("_row_cnt:%ld > %ld max_select_rows", row_cnt.load(), FLAGS_max_select_rows);
        return E_BIG_SQL;
    }
    std::shared_ptr<RowBatch> batch = std::make_shared<RowBatch>();
    std::vector<int64_t> ttl_batch;
    ttl_batch.reserve(100);
    bool global_ddl_with_ttl = (res.row_values_size() > 0 && res.row_values_size() == res.ttl_timestamp_size()) ? true : false;
    int ttl_idx = 0;
    for (auto& pb_row : res.row_values()) {
        if (pb_row.tuple_values_size() != res.tuple_ids_size()) {
            // brpc SelectiveChannel+backup_request有bug，pb的repeated字段merge到一起了
            SQL_TRACE("backup_request size diff, tuple_values_size:%d tuple_ids_size:%d rows:%d", 
                    pb_row.tuple_values_size(), res.tuple_ids_size(), res.row_values_size());
            for (auto id : res.tuple_ids()) {
                SQL_TRACE("tuple_id:%d  ", id);
            }
            return send_request(state, store_request, info, trace_node, old_region_id, region_id, log_id,
                    retry_times + 1, start_seq_id, current_seq_id, op_type);
        }
        std::unique_ptr<MemRow> row = state->mem_row_desc()->fetch_mem_row();
        for (int i = 0; i < res.tuple_ids_size(); i++) {
            int32_t tuple_id = res.tuple_ids(i);
            row->from_string(tuple_id, pb_row.tuple_values(i));
            //DB_WARNING("row:%s", row->debug_string(tuple_id).c_str());
        }
        if (0 != memory_limit_exceeded(state, row.get())) {
            return E_FATAL;
        }
        batch->move_row(std::move(row));
        if (global_ddl_with_ttl) {
            int64_t time_us = res.ttl_timestamp(ttl_idx++);
            ttl_batch.emplace_back(time_us);
            DB_DEBUG("region_id: %ld, ttl_timestamp: %ld", region_id, time_us);
        }
    }
    if (global_ddl_with_ttl) {
        BAIDU_SCOPED_LOCK(ttl_timestamp_mutex);
        region_id_ttl_timestamp_batch[region_id] = ttl_batch;
        DB_DEBUG("region_id: %ld, ttl_timestamp_size: %ld", region_id, ttl_batch.size());
    }
    if (res.has_cmsketch() && state->cmsketch != nullptr) {
        state->cmsketch->add_proto(res.cmsketch());
        DB_WARNING("region_id:%ld, cmsketch:%s", region_id, res.cmsketch().ShortDebugString().c_str());
    }
    // 减少锁冲突
    if (region_batch.count(region_id) == 1) {
        region_batch[region_id] = batch;
    } else {
        //分裂单独处理
        BAIDU_SCOPED_LOCK(region_lock);
        split_start_key_sort.emplace(info.start_key(), region_id);
        split_region_batch[region_id] = batch;
    }
    if (cost.get_time() > FLAGS_print_time_us) {
        DB_WARNING("parse region:%ld time:%ld rows:%lu log_id:%lu ",
                region_id, cost.get_time(), batch->size(), log_id);
    }
    return E_OK;
}

int FetcherStore::memory_limit_exceeded(RuntimeState* state, MemRow* row) {
    if (row_cnt > FLAGS_db_row_number_to_check_memory) {
        if (0 != state->memory_limit_exceeded(row->used_size())) {
            BAIDU_SCOPED_LOCK(region_lock);
            state->error_code = ER_TOO_BIG_SELECT;
            state->error_msg.str("select reach memory limit");
            return -1;
        }
    }
    used_bytes += row->used_size();
    return 0;
}

void FetcherStore::choose_other_if_faulty(pb::RegionInfo& info, std::string& addr) {
    SchemaFactory* schema_factory = SchemaFactory::get_instance();
    auto status = schema_factory->get_instance_status(addr);
    if (status.status == pb::NORMAL) {
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

void FetcherStore::other_normal_peer_to_leader(pb::RegionInfo& info, std::string& addr) {
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
    int64_t timestamp = TsoFetcher::get_tso();
    if (timestamp < 0) {
        return -1;
    }
    return timestamp;
}

ErrorType FetcherStore::process_binlog_start(RuntimeState* state, pb::OpType op_type) {
    if (need_process_binlog(state, op_type)) {
        auto binlog_ctx = client_conn->get_binlog_ctx();
        uint64_t log_id = state->log_id();
        if (need_get_binlog_region) {
            need_get_binlog_region = false;
            int ret = binlog_ctx->get_binlog_regions(log_id);
            if (ret < 0) {
                DB_WARNING("binlog ctx prepare fail log_id:%lu", log_id);
                return E_FATAL;
            }
        }
        if (op_type == pb::OP_PREPARE || binlog_prepare_success) {
            binlog_cond.increase();
            auto write_binlog_func = [this, state, binlog_ctx, op_type, log_id]() {
                ON_SCOPE_EXIT([this]() {
                    binlog_cond.decrease_signal();
                });
                if (op_type == pb::OP_PREPARE) {
                    int64_t timestamp = TsoFetcher::get_tso();
                    if (timestamp < 0) {
                        DB_WARNING("get tso failed log_id: %lu txn_id:%lu op_type:%s", log_id, state->txn_id,
                            pb::OpType_Name(op_type).c_str());
                        error = E_FATAL;
                        return;
                    }
                    binlog_ctx->set_start_ts(timestamp);
                }
                auto ret = write_binlog(state, op_type, log_id);
                if (ret != E_OK) {
                    error = ret;
                }
            };
            Bthread bth(&BTHREAD_ATTR_SMALL);
            bth.run(write_binlog_func);
        }
        return E_OK;
    }
    return E_OK;
}

ErrorType FetcherStore::write_binlog(RuntimeState* state,
                                     const pb::OpType op_type,
                                     const uint64_t log_id) {
    auto binlog_ctx = client_conn->get_binlog_ctx();
    pb::StoreReq req;
    pb::StoreRes res;

    req.set_db_conn_id(client_conn->get_global_conn_id());
    req.set_log_id(log_id);
    auto binlog_desc = req.mutable_binlog_desc();
    binlog_desc->set_txn_id(state->txn_id);
    binlog_desc->set_start_ts(binlog_ctx->start_ts());
    binlog_desc->set_primary_region_id(client_conn->primary_region_id.load());
    auto binlog = req.mutable_binlog();
    binlog->set_start_ts(binlog_ctx->start_ts());
    binlog->set_partition_key(binlog_ctx->get_partition_key());
    if (op_type == pb::OP_PREPARE) {
        binlog->set_type(pb::BinlogType::PREWRITE);
        req.set_op_type(pb::OP_PREWRITE_BINLOG);
        binlog_desc->set_binlog_ts(binlog_ctx->start_ts());
        auto prewrite_value = binlog->mutable_prewrite_value();
        prewrite_value->CopyFrom(binlog_ctx->binlog_value());
    } else if (op_type == pb::OP_COMMIT) {
        binlog->set_type(pb::BinlogType::COMMIT);
        req.set_op_type(pb::OP_COMMIT_BINLOG);
        binlog_desc->set_binlog_ts(binlog_ctx->commit_ts());
        binlog->set_commit_ts(binlog_ctx->commit_ts());
    } else if (op_type == pb::OP_ROLLBACK) {
        binlog->set_type(pb::BinlogType::ROLLBACK);
        req.set_op_type(pb::OP_ROLLBACK_BINLOG);
        binlog_desc->set_binlog_ts(binlog_ctx->start_ts());
    } else {
        // todo DDL
    }
    int ret = 0;
    pb::RegionInfo& info = binlog_ctx->binglog_region();
    int64_t region_id = info.region_id();
    req.set_region_id(region_id);
    req.set_region_version(info.version());
    int retry_times = 0;
    do {
        brpc::Channel channel;
        brpc::Controller cntl;
        cntl.set_log_id(log_id);
        brpc::ChannelOptions option;
        option.max_retry = 1;
        option.connect_timeout_ms = FLAGS_fetcher_connect_timeout;
        option.timeout_ms = FLAGS_fetcher_request_timeout;
        std::string addr = info.leader();
        if (retry_times == 0) {
            // 重试前已经选择了normal的实例
            // 或者store返回了正确的leader
            choose_other_if_faulty(info, addr);
        }
        ret = channel.Init(addr.c_str(), &option);
        if (ret != 0) {
            DB_WARNING("binlog channel init failed, addr:%s, ret:%d, log_id:%lu",
                    addr.c_str(), ret, log_id);
            return E_FATAL;
        }

        client_conn->insert_callid(addr, region_id, cntl.call_id());

        pb::StoreService_Stub(&channel).query_binlog(&cntl, &req, &res, NULL);
        if (cntl.Failed()) {
            DB_WARNING("binlog call failed  errcode:%d, error:%s, region_id:%ld log_id:%lu",
                cntl.ErrorCode(), cntl.ErrorText().c_str(), region_id, log_id);
            // 只有网络相关错误码才重试
            if (cntl.ErrorCode() != ETIMEDOUT &&
                    cntl.ErrorCode() != ECONNREFUSED &&
                    cntl.ErrorCode() != EHOSTDOWN &&
                    cntl.ErrorCode() != ECANCELED) {
                return E_FATAL;
            }
            other_normal_peer_to_leader(info, addr);
            bthread_usleep(FLAGS_retry_interval_us);
            retry_times++;
            continue;
        }
        //DB_WARNING("binlog fetch store req: %s log_id:%lu", req.DebugString().c_str(), log_id);
        //DB_WARNING("binlog fetch store res: %s log_id:%lu", res.DebugString().c_str(), log_id);
        if (res.errcode() == pb::NOT_LEADER) {
            DB_WARNING("binlog NOT_LEADER, addr:%s region_id:%ld retry:%d, new_leader:%s, log_id:%lu", addr.c_str(),
                region_id, retry_times, res.leader().c_str(), log_id);

            if (res.leader() != "0.0.0.0:0") {
                // store返回了leader，则相信store，不判断normal
                info.set_leader(res.leader());
                SchemaFactory::get_instance()->update_leader(info);
            } else {
                other_normal_peer_to_leader(info, addr);
            }
            retry_times++;
            bthread_usleep(retry_times * FLAGS_retry_interval_us);
        } else if (res.errcode() == pb::VERSION_OLD) {
            DB_WARNING("VERSION_OLD, region_id: %ld, retry:%d, now:%s, log_id:%lu",
                    region_id, retry_times, info.ShortDebugString().c_str(), log_id);
            for (auto r : res.regions()) {
                DB_WARNING("new version region:%s", r.ShortDebugString().c_str());
                info.CopyFrom(r);
            }
            req.set_region_id(info.region_id());
            req.set_region_version(info.version());
        } else if (res.errcode() != pb::SUCCESS) {
            DB_WARNING("errcode:%d, failed, instance:%s region_id:%ld retry:%d log_id:%lu",
                    res.errcode(), addr.c_str(), region_id, retry_times, log_id);
            return E_FATAL;
        } else {
            // success
            binlog_prepare_success = true;
            break;
        }
    } while (retry_times < 5);

    if (binlog_prepare_success) {
        if (op_type == pb::OP_PREPARE) {
            binlog_prewrite_time.reset();
        } else if (op_type == pb::OP_COMMIT) {
            if (binlog_prewrite_time.get_time() > FLAGS_binlog_alarm_time_s * 1000 * 1000LL) {
                // 报警日志
                DB_WARNING("binlog takes too long from prewrite to commit, txn_id: %ld, binlog_region_id: %ld, start_ts: %ld, commit_ts: %ld",
                    state->txn_id, region_id, binlog_ctx->start_ts(), binlog_ctx->commit_ts());
            }
        } else {
            // do nothing
        }
        return E_OK;
    } else {
        DB_WARNING("exec failed log_id:%lu", log_id);
        return E_FATAL;
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
    split_region_batch.clear();
    index_records.clear();
    start_key_sort.clear();
    split_start_key_sort.clear();
    no_copy_cache_plan_set.clear();
    error = E_OK;
    skip_region_set.clear();
    primary_timestamp_updated = false;
    affected_rows = 0;
    scan_rows = 0;
    filter_rows = 0;
    row_cnt = 0;
    client_conn = state->client_conn();
    state->region_count += region_infos.size();
    analyze_fail_cnt = 0;
    //TimeCost cost;
    if (region_infos.size() == 0) {
        DB_WARNING("region_infos size == 0, op_type:%s", pb::OpType_Name(op_type).c_str());
        return E_OK;
    }
    if (FLAGS_use_dynamic_timeout && op_type == pb::OP_SELECT) {
        SchemaFactory* factory = SchemaFactory::get_instance();
        std::shared_ptr<SqlStatistics> sql_info = factory->get_sql_stat(state->sign);
        if (sql_info != nullptr) {
            dynamic_timeout_ms = sql_info->dynamic_timeout_ms();
        }
    }
    // 预分配空洞
    for (auto& pair : region_infos) {
        start_key_sort.emplace(pair.second.start_key(), pair.first);
        region_batch[pair.first] = nullptr;
    }
    uint64_t log_id = state->log_id();
    ErrorType ret = E_OK;
    // 选择primary region同时保证第一次请求primary region成功
    if ((state->txn_id != 0) && (client_conn->primary_region_id == -1) && op_type != pb::OP_SELECT) {
        auto info_iter = region_infos.begin();
        client_conn->primary_region_id = info_iter->first;
        client_conn->txn_pri_region_last_exec_time = butil::gettimeofday_us();
        //DB_WARNING("select primary_region_id:%ld txn_id:%lu op_type:%s",
        //       client_conn->primary_region_id.load(), state->txn_id, pb::OpType_Name(op_type).c_str());
        ret = send_request(state, store_request, info_iter->second, info_iter->first, info_iter->first, log_id,
                0, start_seq_id, current_seq_id, op_type);
        if (ret == E_RETURN) {
            DB_WARNING("primary_region_id:%ld rollbacked, log_id:%lu op_type:%s",
                client_conn->primary_region_id.load(), log_id, pb::OpType_Name(op_type).c_str());
            if (op_type == pb::OP_COMMIT || op_type == pb::OP_ROLLBACK) {
                return E_OK;
            } else {
                client_conn->state = STATE_ERROR;
                return -1;
            }
        }
        if (ret != E_OK) {
            DB_WARNING("rpc error, primary_region_id:%ld, log_id:%lu op_type:%s",
                            client_conn->primary_region_id.load(), log_id, pb::OpType_Name(op_type).c_str());
            if (ret != E_WARNING) {
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
        ErrorType ret = E_OK;
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
            ret = send_request(state, store_request, iter->second, primary_region_id, primary_region_id, log_id,
                0, start_seq_id, current_seq_id, op_type);
            if (ret == E_RETURN) {
                DB_WARNING("primary_region_id:%ld rollbacked, log_id:%lu op_type:%s",
                    primary_region_id, log_id, pb::OpType_Name(op_type).c_str());
                return E_OK;
            }
            if (ret != E_OK) {
                DB_FATAL("send optype:%s to region_id:%ld txn_id:%lu failed, log_id:%lu ", pb::OpType_Name(op_type).c_str(),
                    primary_region_id, state->txn_id, log_id);
                if (retry_times < 5) {
                    retry_times++;
                }
                // 每次多延迟5s重试，leader切换耗时评估后考虑去掉无限重试
                bthread_usleep(retry_times * FLAGS_retry_interval_us * 10L);
            }
        } while (ret != E_OK);
        skip_region_set.insert(primary_region_id);
    }

    ret = process_binlog_start(state, op_type);
    if (ret != E_OK) {
        DB_FATAL("process binlog op_type:%s txn_id:%lu failed, log_id:%lu ", pb::OpType_Name(op_type).c_str(),
             state->txn_id, log_id);
        return -1;
    }

    // 构造并发送请求
    std::map<std::string, std::set<std::shared_ptr<TraceDesc>>> send_region_ids_map; // leader ip => region_ids
    int send_region_count = 0;
    for (auto& pair : region_infos) {
        if (skip_region_set.count(pair.first) > 0) {
            continue;
        }
        ++send_region_count;
        std::shared_ptr<TraceDesc> trace = std::make_shared<TraceDesc>();
        trace->region_id = pair.first;
        if (store_request->get_trace() != nullptr) {
            std::shared_ptr<pb::TraceNode> child_trace = std::make_shared<pb::TraceNode>();
            trace->trace_node = child_trace;
        }
        send_region_ids_map[pair.second.leader()].insert(trace);
    }
    if (send_region_count == 1) {
        auto& trace = *send_region_ids_map.begin()->second.begin();
        int64_t region_id = trace->region_id;
        // 这两个资源后续不会分配新的，因此不需要加锁
        pb::RegionInfo* info = nullptr;
        if (region_infos.count(region_id) != 0) {
            info = &region_infos[region_id];
        } else if (state->txn_id != 0) {
            BAIDU_SCOPED_LOCK(state->client_conn()->region_lock);
            info = &(state->client_conn()->region_infos[region_id]);
        }
        auto ret = send_request(state, store_request, *info, trace->trace_node.get(), region_id, region_id, log_id,
                0, start_seq_id, current_seq_id, op_type);
        if (ret != E_OK) {
            DB_WARNING("rpc error, region_id:%ld, log_id:%lu op_type:%s",
                    region_id, log_id, pb::OpType_Name(op_type).c_str());
            error = ret;
        }
    } else if (send_region_count <= FLAGS_single_store_concurrency) {
        ConcurrencyBthread con_bth(send_region_count, &BTHREAD_ATTR_SMALL);
        for (auto& pair : send_region_ids_map) {
            for (auto& trace : pair.second) {
                int64_t region_id = trace->region_id;
                // 这两个资源后续不会分配新的，因此不需要加锁
                pb::RegionInfo* info = nullptr;
                if (region_infos.count(region_id) != 0) {
                    info = &region_infos[region_id];
                } else if (state->txn_id != 0) {
                    BAIDU_SCOPED_LOCK(state->client_conn()->region_lock);
                    info = &(state->client_conn()->region_infos[region_id]);
                }

                auto req_thread = [this, state, store_request, info, trace, region_id, log_id, current_seq_id,
                     start_seq_id, op_type]() {
                         auto ret = send_request(state, store_request, *info, trace->trace_node.get(), region_id, region_id, log_id,
                                 0, start_seq_id, current_seq_id, op_type);
                         if (ret != E_OK) {
                             DB_WARNING("rpc error, region_id:%ld, log_id:%lu op_type:%s",
                                     region_id, log_id, pb::OpType_Name(op_type).c_str());
                             error = ret;
                         }
                     };
                con_bth.run(req_thread);
            }
        }
        con_bth.join();
    } else {
        BthreadCond store_cond; // 不同store并发
        for (auto& pair : send_region_ids_map) {
            store_cond.increase();
            auto store_thread = [this, state, store_request, pair, log_id, start_seq_id, current_seq_id,
                 &region_infos, &store_cond, op_type, send_region_count]() {
                     ON_SCOPE_EXIT([&store_cond]{store_cond.decrease_signal();});
                     BthreadCond cond(-FLAGS_single_store_concurrency); // 单store内并发数
                     for (auto& trace : pair.second) {
                         int64_t region_id = trace->region_id;
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
                         auto req_thread = [this, state, store_request, info, trace, region_id, log_id, current_seq_id,
                              start_seq_id, &cond, op_type, send_region_count]() {
                                  ON_SCOPE_EXIT([&cond]{cond.decrease_signal();});
                                  auto ret = send_request(state, store_request, *info, trace->trace_node.get(), region_id, region_id, log_id,
                                          0, start_seq_id, current_seq_id, op_type);
                                  if (ret != E_OK) {
                                      DB_WARNING("rpc error, region_id:%ld, log_id:%lu op_type:%s",
                                              region_id, log_id, pb::OpType_Name(op_type).c_str());
                                        if (state->explain_type == ANALYZE_STATISTICS) {
                                            // 代价采样时，个别region报错可以继续
                                            analyze_fail_cnt++;
                                            //超过10%报错则退出
                                            if (analyze_fail_cnt.load()*1.0/send_region_count > 0.1) {
                                                error = ret;
                                            }
                                        } else {
                                            error = ret;
                                        }
                                  }
                              };
                         // 怀疑栈溢出
                         Bthread bth(&BTHREAD_ATTR_SMALL);
                         bth.run(req_thread);
                     }
                     cond.wait(-FLAGS_single_store_concurrency);
                 };
            Bthread bth(&BTHREAD_ATTR_SMALL);
            bth.run(store_thread);
        }

        // commit/rollback请求后续可以考虑异步执行，不等待
        store_cond.wait();
    }

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
                state->error_code = ER_SQL_TOO_BIG;
                state->error_msg.str("sql too big");
            }
        } else {
            DB_WARNING("fetcher node open fail, log_id:%lu, txn_id: %lu, seq_id: %d op_type: %s",
                    log_id, state->txn_id, current_seq_id, pb::OpType_Name(op_type).c_str());
        }
        return -1;
    }
    for (auto& pair : split_start_key_sort) {
        start_key_sort.emplace(pair.first, pair.second);
    }
    for (auto& pair : split_region_batch) {
        region_batch.emplace(pair.first, pair.second);
    }
    std::map<uint64_t, std::shared_ptr<pb::TraceNode>> cost_trace_map;
    if (store_request->get_trace() != nullptr) {
        for (auto& pair : send_region_ids_map) {
            for (auto& trace : pair.second) {
                cost_trace_map[trace->trace_node->total_time()] = trace->trace_node;
            }
        }
        int region_cnt = cost_trace_map.size();
        if (region_cnt > 10 && state->explain_type == SHOW_TRACE) {
            for (auto& pair : cost_trace_map) {
                if (region_cnt > 10) {
                    pb::LocalTraceNode* local_node = store_request->get_trace()->mutable_store_agg();
                    int64_t scan_rows = TraceLocalNode::get_scan_rows(pair.second.get());
                    int64_t affect_rows = pair.second->affect_rows();
                    local_node->set_scan_rows(scan_rows + local_node->scan_rows());
                    local_node->set_affect_rows(affect_rows + local_node->affect_rows());
                } else {
                    pb::TraceNode* trace = store_request->get_trace()->add_child_nodes();
                    (*trace) = (*pair.second);
                }
                --region_cnt;
            }
        } else {
            for (auto& pair : send_region_ids_map) {
                for (auto& trace : pair.second) {
                    (*store_request->get_trace()->add_child_nodes()) = *trace->trace_node;
                }
            }
        }
    }
    state->set_num_scan_rows(state->num_scan_rows() + scan_rows.load());
    state->set_num_filter_rows(state->num_filter_rows() + filter_rows.load());
    //DB_WARNING("fetcher time:%ld, txn_id: %lu, log_id:%lu, batch_size:%lu",
    //        cost.get_time(), state->txn_id, log_id, region_batch.size());
    return affected_rows.load();
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
