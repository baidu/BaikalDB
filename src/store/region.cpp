// Copyright (c) 2018 Baidu, Inc. All Rights Reserved.
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

#include "region.h"
#include <algorithm>
#include <boost/filesystem.hpp>
#include "table_key.h"
#include "runtime_state.h"
#include "mem_row_descriptor.h"
#include "exec_node.h"
#include "table_record.h"
#include "my_raft_log_storage.h"
#include "log_entry_reader.h"
#include "raft_log_compaction_filter.h"
#include "split_compaction_filter.h"
#include "rpc_sender.h"
#include "concurrency.h"
#include "store.h"
#include "closure.h"
#include "rapidjson/rapidjson.h"

namespace baikaldb {
DEFINE_int32(election_timeout_ms, 1000, "raft election timeout(ms)");
DEFINE_int32(skew, 5, "split skew, default : 45% - 55%");
DEFINE_int32(reverse_level2_len, 5000, "reverse index level2 length, default : 5000");
DEFINE_string(log_uri, "myraftlog://my_raft_log?id=", "raft log uri");
DEFINE_string(stable_uri, "local://./raft_data/stable", "raft stable path");
DEFINE_string(snapshot_uri, "local://./raft_data/snapshot", "raft snapshot path");
DEFINE_int64(disable_write_wait_timeout_us, 1000 * 1000, 
        "disable write wait timeout(us) default 1s");
DEFINE_int64(real_writing_wait_timeout_us, 1000 * 1000, 
        "real writing wait timeout(us) default 1s");
DEFINE_int32(snapshot_interval_s, 600, "raft snapshot interval(s)");
DEFINE_int32(snapshot_timed_wait, 120 * 1000 * 1000LL, "snapshot timed wait default 120S");
DEFINE_int64(snapshot_diff_lines, 10000, "save_snapshot when num_table_lines diff");
DEFINE_int64(snapshot_diff_logs, 2000, "save_snapshot when log entries diff");
DEFINE_int64(snapshot_log_exec_time_s, 60, "save_snapshot when log entries apply time");
//分裂判断标准，如果3600S没有收到请求，则认为分裂失败
DEFINE_int64(split_duration_us, 3600 * 1000 * 1000LL, "split duration time : 3600s");
DEFINE_int64(compact_delete_lines, 200000, "compact when _num_delete_lines > compact_delete_lines");
DEFINE_int64(time_cost_us, 10000, "print dml log when dml_time_cost > gflags_dml_time_cost(us)");

//const size_t  Region::REGION_MIN_KEY_SIZE = sizeof(int64_t) * 2 + sizeof(uint8_t);
const uint8_t Region::PRIMARY_INDEX_FLAG = 0x01;                                   
const uint8_t Region::SECOND_INDEX_FLAG = 0x02;
const int BATCH_COUNT = 1024;

ScopeProcStatus::~ScopeProcStatus() {
    if (_region != NULL) {
        _region->reset_region_status();
        _region->reset_allow_write(); 
        _region->reset_split_status();
        baikaldb::Store::get_instance()->sub_split_num();
    }
}

int Region::init(bool new_region, int32_t snapshot_times) {
    _data_cf = _rocksdb->get_data_handle();
    _meta_cf = _rocksdb->get_meta_info_handle();
    _writer = MetaWriter::get_instance();
    TimeCost time_cost;
    _resource.reset(new RegionResource);
    //如果是新建region需要
    if (new_region) {
        TimeCost write_db_cost;
        if (_writer->init_meta_info(_region_info) != 0) {
            DB_FATAL("write region to rocksdb fail when init reigon, region_id: %ld", _region_id);
            return -1;
        }
        DB_WARNING("region_id: %ld write init meta info: %ld", _region_id, write_db_cost.get_time());
    } else {
        _report_peer_info = true;
    }
    // 初始化倒排索引
    TableInfo& table_info = _resource->table_info;
    _resource->region_id = _region_id;
    _resource->table_id = _region_info.table_id();
    table_info = _factory->get_table_info(_region_info.table_id());
    if (table_info.id == -1) {
        DB_WARNING("tableinfo get fail, table_id:%ld, region_id: %ld", 
                    _region_info.table_id(), _region_id);
        return -1;
    }
    for (int64_t index_id : table_info.indices) {
        IndexInfo info = _factory->get_index_info(index_id);
        if (info.id == -1) {
            continue;
        }
        if (info.type == pb::I_PRIMARY) {
            _resource->pri_info = info;
        }
        _resource->index_infos[info.id] = info;
        pb::SegmentType segment_type = info.segment_type;
        switch (info.type) {
            case pb::I_FULLTEXT: 
                if (info.fields.size() != 1) {
                    DB_FATAL("I_FULLTEXT field must be 1, table_id:% ld", table_info.id);
                    return -1;
                }
                if (info.fields[0].type != pb::STRING) {
                    segment_type = pb::S_NO_SEGMENT;
                }
                if (segment_type == pb::S_DEFAULT) {
#ifdef BAIDU_INTERNAL
                    segment_type = pb::S_WORDRANK;
#else
                    segment_type = pb::S_SIMPLE;
#endif
                }
                _reverse_index_map[index_id] = new ReverseIndex<CommonSchema>(
                        _region_id, 
                        index_id,
                        FLAGS_reverse_level2_len,
                        _rocksdb,
                        segment_type,
                        false, // common need not cache
                        true);
                break;
            case pb::I_RECOMMEND: {
                _reverse_index_map[index_id] = new ReverseIndex<XbsSchema>(
                        _region_id, 
                        index_id,
                        FLAGS_reverse_level2_len,
                        _rocksdb,
                        segment_type,
                        true,
                        false); // xbs need not cache segment
                int32_t userid_field_id = get_field_id_by_name(table_info.fields, "userid");
                int32_t source_field_id = get_field_id_by_name(table_info.fields, "source");
                _reverse_index_map[index_id]->add_field("userid", userid_field_id);
                _reverse_index_map[index_id]->add_field("source", source_field_id);
                break;
            }
            default:
                break;
        }
    }
    braft::NodeOptions options;
    //construct init peer
    std::vector<braft::PeerId> peers;
    for (int i = 0; i < _region_info.peers_size(); ++i) {
        butil::EndPoint end_point;
        if (butil::str2endpoint(_region_info.peers(i).c_str(), &end_point) != 0) {
            DB_FATAL("str2endpoint fail, peer:%s, region id:%lu", 
                            _region_info.peers(i).c_str(), _region_id);
            return -1;
        }
        peers.push_back(braft::PeerId(end_point));
    }
    options.election_timeout_ms = FLAGS_election_timeout_ms;
    options.fsm = this;
    options.initial_conf = braft::Configuration(peers);
    options.snapshot_interval_s = 0;
    //options.snapshot_interval_s = FLAGS_snapshot_interval_s; // 禁止raft自动触发snapshot
    options.log_uri = FLAGS_log_uri + 
                       boost::lexical_cast<std::string>(_region_id);  
#if BAIDU_INTERNAL
    options.stable_uri = FLAGS_stable_uri + 
                           boost::lexical_cast<std::string>(_region_id);
#else
    options.raft_meta_uri = FLAGS_stable_uri + 
                           boost::lexical_cast<std::string>(_region_id);
#endif
    options.snapshot_uri = FLAGS_snapshot_uri + "/region_" + 
                                boost::lexical_cast<std::string>(_region_id);
    options.snapshot_file_system_adaptor = &_snapshot_adaptor;
    _txn_pool.init(_region_id);
    if (_node.init(options) != 0) {
        DB_FATAL("raft node init fail, region_id: %ld, region_info:%s", 
                 _region_id, pb2json(_region_info).c_str());
        return -1;
    }
    
    if (peers.size() == 1) {
        _node.reset_election_timeout_ms(0); //10ms
        DB_WARNING("region_id: %ld, vote 0", _region_id);
    }
    //bthread_usleep(5000);
    if (peers.size() == 1) { 
        _node.reset_election_timeout_ms(FLAGS_election_timeout_ms);
        DB_WARNING("region_id: %ld reset_election_timeout_ms", _region_id);
    }
    _time_cost.reset();
    while (snapshot_times > 0) {
        _region_control.sync_do_snapshot();
        --snapshot_times;
    }
    copy_region(&_resource->region_info);
    //compaction时候删掉多余的数据
    SplitCompactionFilter::get_instance()->set_range_key(
            _region_id,
            _resource->region_info.start_key(),
            _resource->region_info.end_key());
    _init_success = true;
    DB_WARNING("region_id: %ld init success, region_info:%s, time_cost:%ld", 
                _region_id, _resource->region_info.ShortDebugString().c_str(), 
                time_cost.get_time());
    return 0;
}

void Region::update_average_cost(int64_t request_time_cost) {
    const int64_t end_time_us = butil::gettimeofday_us();
    StatisticsInfo info = {request_time_cost, end_time_us};
    std::unique_lock<std::mutex> lock(_queue_lock);
    if (!_statistics_queue.empty()) {
        info.time_cost_sum += _statistics_queue.bottom()->time_cost_sum;
    }
    _statistics_queue.elim_push(info);
    const int64_t top = _statistics_queue.top()->end_time_us;
    const size_t n = _statistics_queue.size();
    
    // more than one element in the queue
    if (end_time_us > top) {
        _qps = (n - 1) * 1000000L / (end_time_us - top);
        _average_cost = 
            (info.time_cost_sum - _statistics_queue.top()->time_cost_sum) / (n - 1);
    } else {
        _average_cost = request_time_cost;
        _qps = 1;
    }
    //DB_WARNING("req_cost: %ld, avg_cost: %ld", request_time_cost, _average_cost.load());
}

bool Region::check_region_legal_complete() {
    do {
        //bthread_usleep(FLAGS_split_duration_us);
        bthread_usleep(10 * 1000 * 1000);
        //3600S没有收到请求， 并且version 也没有更新的话，分裂失败
        if (_removed) {
            DB_WARNING("region_id: %ld has been removed", _region_id);
            return true;
        }
        if (_time_cost.get_time() > FLAGS_split_duration_us) {
            if (compare_and_set_illegal()) {
                DB_WARNING("split or add_peer fail, set illegal, region_id: %ld",
                           _region_id);
                return false;
            } else {
                DB_WARNING("split or add_peer  success, region_id: %ld", _region_id);
                return true;
            }
        } else if (_region_info.version() > 0) {
            DB_WARNING("split or add_peer success, region_id: %ld", _region_id);
            return true;
        } else {
            DB_WARNING("split or add_peer not complete, need wait, region_id: %ld, cost_time: %ld", 
                _region_id, _time_cost.get_time());
        }
    } while (1);
}

bool Region::validate_version(const pb::StoreReq* request, pb::StoreRes* response) {
    if (request->region_version() < _region_info.version()) {
        response->Clear();
        response->set_errcode(pb::VERSION_OLD);
        response->set_errmsg("region version too old");

        const char* leader_str = butil::endpoint2str(_node.leader_id().addr).c_str();
        response->set_leader(leader_str);
        auto region = response->add_regions();
        copy_region(region);
        region->set_leader(leader_str);

        for (auto& r : _new_region_infos) {
            if (r.region_id() != 0 && r.version() != 0) {
                response->add_regions()->CopyFrom(r);
                DB_WARNING("new region %ld, %ld", _region_info.region_id(), r.region_id());
            } else {
                DB_FATAL("r:%s", pb2json(r).c_str());
            }
        }
        return false;
    }
    return true;
}

int Region::execute_cached_cmd(const pb::StoreReq& request, pb::StoreRes& response, 
        uint64_t txn_id, SmartTransaction& txn, int64_t applied_index, int64_t term, uint64_t log_id) {
    if (request.op_type() == pb::OP_ROLLBACK || request.txn_infos_size() == 0) {
        return 0;
    }
    const pb::TransactionInfo& txn_info = request.txn_infos(0);
    int last_seq = (txn == nullptr)? 0 : txn->seq_id();
    //DB_WARNING("TransactionNote: region_id: %ld, txn_id: %lu, op_type: %d, "
    //        "last_seq: %d, cache_plan_size: %d, log_id: %lu",
    //        _region_id, txn_id, request.op_type(), last_seq, txn_info.cache_plans_size(), log_id);

    // executed the cached cmd from last_seq + 1
    for (auto& cache_item : txn_info.cache_plans()) {
        const pb::OpType op_type = cache_item.op_type();
        const pb::Plan& plan = cache_item.plan();
        const RepeatedPtrField<pb::TupleDescriptor>& tuples = cache_item.tuples();

        if (op_type != pb::OP_BEGIN 
                && op_type != pb::OP_INSERT 
                && op_type != pb::OP_DELETE 
                && op_type != pb::OP_UPDATE) {
                //&& op_type != pb::OP_PREPARE) {
            response.set_errcode(pb::UNSUPPORT_REQ_TYPE);
            response.set_errmsg("unexpected cache plan op_type: " + std::to_string(op_type));
            DB_WARNING("TransactionWarn: unexpected op_type: %d", op_type);
            return -1;
        }
        int seq_id = cache_item.seq_id();
        if (seq_id <= last_seq) {
            //DB_WARNING("TransactionNote: txn %ld_%lu:%d has been executed.", _region_id, txn_id, seq_id);
            continue;
        } else {
            //DB_WARNING("TransactionNote: txn %ld_%lu:%d executed cached. op_type: %d",  
            //    _region_id, txn_id, seq_id, op_type);
        }
        
        // normally, cache plan should be execute successfully, because it has been executed 
        // on other peers, except for single-stmt transactions
        pb::StoreRes res;
        dml_2pc(request, op_type, plan, tuples, res, applied_index, term, seq_id);
        if (res.has_errcode() && res.errcode() != pb::SUCCESS) {
            response.set_errcode(res.errcode());
            response.set_errmsg(res.errmsg());
            if (res.has_mysql_errcode()) {
                response.set_mysql_errcode(res.mysql_errcode());
            }
            if (txn_info.autocommit() == false) {
                DB_FATAL("TransactionError: txn: %ld_%lu:%d executed failed.", _region_id, txn_id, seq_id);
            }
            return -1;
        }
        // if this is the BEGIN cmd, we need to refresh the txn handler
        if (op_type == pb::OP_BEGIN && (nullptr == (txn = _txn_pool.get_txn(txn_id)))) {
            char errmsg[100];
            snprintf(errmsg, sizeof(errmsg), "TransactionError: txn: %ld_%lu:%d last_seq:%d"
                "get txn failed after begin", _region_id, txn_id, seq_id, last_seq);
            DB_FATAL("%s", errmsg);
            response.set_errcode(pb::EXEC_FAIL);
            response.set_errmsg(errmsg);
            return -1;
        }
    }
    //DB_WARNING("region_id: %ld, txn_id: %lu, execute_cached success.", _region_id, txn_id);
    return 0;
}

// execute query within a transaction context
void Region::exec_in_txn_query(google::protobuf::RpcController* controller,
            const pb::StoreReq* request, 
            pb::StoreRes* response, 
            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = (brpc::Controller*)controller;
    uint64_t log_id = 0;
    if (cntl->has_log_id()) { 
        log_id = cntl->log_id();
    }

    const char* remote_side = butil::endpoint2str(cntl->remote_side()).c_str();
    pb::OpType op_type = request->op_type();
    const pb::TransactionInfo& txn_info = request->txn_infos(0);
    uint64_t txn_id = txn_info.txn_id();
    int seq_id = txn_info.seq_id();

    SmartTransaction txn = _txn_pool.get_txn(txn_id);
    // when commit/rollback in 2 phase commit, no need to execute cache plan beforehand.
    // since prepare has been applied into raft,
    if (op_type == pb::OP_ROLLBACK || op_type == pb::OP_COMMIT) {
        if (txn == nullptr) {
            DB_WARNING("TransactionNote: no txn handler when commit/rollback, region_id: %ld, txn_id: %lu", 
                _region_id, txn_id);
            response->set_affected_rows(0);
            response->set_errcode(pb::SUCCESS);
            return;
        }
        int64_t disable_write_wait = get_split_wait_time();
        int ret = _disable_write_cond.timed_wait(disable_write_wait);
        _real_writing_cond.increase();
        ScopeGuard auto_decrease([this]() {
                _real_writing_cond.decrease_signal();
                });
        if (ret != 0) {
            response->set_errcode(pb::DISABLE_WRITE_TIMEOUT);
            response->set_errmsg("_diable_write_cond wait timeout");
            DB_FATAL("_diable_write_cond wait timeout, ret:%d, region_id: %ld", ret, _region_id);
            return;
        }
        // double check，防止写不一致
        if (!_is_leader.load()) {
            response->set_errcode(pb::NOT_LEADER);
            response->set_leader(butil::endpoint2str(_node.leader_id().addr).c_str());
            response->set_errmsg("not leader");
            DB_WARNING("not leader old version, leader:%s, region_id: %ld, log_id:%lu",
                    butil::endpoint2str(_node.leader_id().addr).c_str(), _region_id, log_id);
            return;
        }
        if (validate_version(request, response) == false) {
            DB_WARNING("region version too old, region_id: %ld, log_id:%lu,"
                    " request_version:%ld, region_version:%ld",
                    _region_id, log_id, request->region_version(), _region_info.version());
            return;
        }

        butil::IOBuf data;
        butil::IOBufAsZeroCopyOutputStream wrapper(&data);
        if (!request->SerializeToZeroCopyStream(&wrapper)) {
            cntl->SetFailed(brpc::EREQUEST, "Fail to serialize request");
            return;
        }
        DMLClosure* c = new DMLClosure;
        c->cost.reset();
        c->op_type = request->op_type();
        c->cntl = cntl;
        c->response = response;
        c->done = done_guard.release();
        c->region = this;
        c->remote_side = remote_side;
        braft::Task task;
        task.data = &data;
        task.done = c;
        auto_decrease.release();
        _node.apply(task);
        return;
    }
    // seq_id within a transaction should be continuous regardless of failure or success
    int last_seq = (txn == nullptr)? 0 : txn->seq_id();
    if (txn_info.start_seq_id() > last_seq + 1) {
        char errmsg[100];
        snprintf(errmsg, sizeof(errmsg), "region_id: %ld, txn_id: %lu, txn_last_seq: %d, request_start_seq: %d", 
            _region_id, txn_id, last_seq, txn_info.start_seq_id());
        //DB_WARNING("%s", errmsg);
        response->set_errcode(pb::TXN_FOLLOW_UP);
        response->set_last_seq_id(last_seq);
        response->set_errmsg(errmsg);
        return;
    }
    // for tail splitting new region replay txn
    if (request->has_start_key() && !request->start_key().empty()) {
        pb::RegionInfo region_info_mem;
        copy_region(&region_info_mem);
        region_info_mem.set_start_key(request->start_key());
        set_region_with_update_range(region_info_mem);
    }
    int ret = 0;
    if (/*op_type != pb::OP_PREPARE && */last_seq < seq_id - 1) {
        ret = execute_cached_cmd(*request, *response, txn_id, txn, 0, 0, log_id);
        if (ret != 0) {
            DB_FATAL("execute cached failed, region_id: %ld, txn_id: %lu", _region_id, txn_id);
            return;
        }
    }

    // execute the current cmd
    // OP_BEGIN cmd is always cached
    switch (op_type) {
        case pb::OP_SELECT: {
            TimeCost cost;
            select(*request, *response);
            int64_t select_cost = cost.get_time();
            Store::get_instance()->select_time_cost << select_cost;
            if (select_cost > FLAGS_time_cost_us) {
                DB_NOTICE("select type: %s, region_id: %ld, txn_id: %lu, seq_id: %d, "
                        "time_cost: %ld, log_id: %lu, remote_side: %s", 
                        pb::OpType_Name(request->op_type()).c_str(), _region_id, txn_id, seq_id, 
                        cost.get_time(), log_id, remote_side);
            }
            if (txn != nullptr) {
                txn->set_seq_id(seq_id);
            }
        }
        break;
        case pb::OP_INSERT:
        case pb::OP_DELETE:
        case pb::OP_UPDATE: {
            dml(*request, *response, (int64_t)0, (int64_t)0);
        }
        break;
        case pb::OP_PREPARE_V2: 
        case pb::OP_PREPARE: {
            if (_split_param.split_slow_down) {
                DB_WARNING("region is spliting, slow down time:%ld, "
                            "region_id: %ld, remote_side: %s", 
                            _split_param.split_slow_down_cost, _region_id, remote_side);
                bthread_usleep(_split_param.split_slow_down_cost);
            }

            //TODO
            int64_t disable_write_wait = get_split_wait_time();
            ret = _disable_write_cond.timed_wait(disable_write_wait);
            _real_writing_cond.increase();
            ScopeGuard auto_decrease([this]() {
                _real_writing_cond.decrease_signal();
            });
            if (ret != 0) {
                response->set_errcode(pb::DISABLE_WRITE_TIMEOUT);
                response->set_errmsg("_diable_write_cond wait timeout");
                DB_FATAL("_diable_write_cond wait timeout, ret:%d, region_id: %ld", ret, _region_id);
                return;
            }

            // double check，防止写不一致
            if (!_is_leader.load()) {
                response->set_errcode(pb::NOT_LEADER);
                response->set_leader(butil::endpoint2str(_node.leader_id().addr).c_str());
                response->set_errmsg("not leader");
                DB_WARNING("not leader old version, leader:%s, region_id: %ld, log_id:%lu",
                        butil::endpoint2str(_node.leader_id().addr).c_str(), _region_id, log_id);
                return;
            }
            if (validate_version(request, response) == false) {
                DB_WARNING("region version too old, region_id: %ld, log_id:%lu,"
                           " request_version:%ld, region_version:%ld",
                            _region_id, log_id, request->region_version(), _region_info.version());
                return;
            }
            pb::StoreReq prepare_req;
            prepare_req.CopyFrom(*request);
            pb::TransactionInfo* prepare_txn = prepare_req.mutable_txn_infos(0);
            prepare_txn->clear_cache_plans();
            prepare_txn->set_start_seq_id(1);

            // packet all cmd (starting from BEGIN) of this txn and send to raft log entry
            int cur_seq_id = 0;
            if (txn != nullptr) {
                for (auto& cache_item : txn->cache_plan_map()) {
                    prepare_txn->add_cache_plans()->CopyFrom(cache_item.second);
                    cur_seq_id = cache_item.second.seq_id();
                }
            }
            size_t txn_size = txn_info.cache_plans_size();
            for (size_t idx = 0; idx < txn_size; ++idx) {
                auto& plan = txn_info.cache_plans(idx);
                if (plan.seq_id() <= cur_seq_id) {
                    continue;
                }
                prepare_txn->add_cache_plans()->CopyFrom(plan);
            }

            butil::IOBuf data;
            butil::IOBufAsZeroCopyOutputStream wrapper(&data);
            if (!prepare_req.SerializeToZeroCopyStream(&wrapper)) {
                cntl->SetFailed(brpc::EREQUEST, "Fail to serialize request");
                return;
            }
            DMLClosure* c = new DMLClosure;
            c->cost.reset();
            c->op_type = prepare_req.op_type();
            c->cntl = cntl;
            c->response = response;
            c->done = done_guard.release();
            c->region = this;
            c->transaction = txn;
            c->remote_side = remote_side;
            braft::Task task;
            task.data = &data;
            task.done = c;
            auto_decrease.release();
            if (txn != nullptr) {
                txn->set_prepare_apply();
            }
            _node.apply(task);
        }
        break;
        default: {
            response->set_errcode(pb::UNSUPPORT_REQ_TYPE);
            response->set_errmsg("unsupported in_txn_query type");
            DB_FATAL("unsupported out_txn_query type: %d, region_id: %ld, log_id:%lu, txn_id: %lu", 
                op_type, _region_id, log_id, txn_id);
        }
    }
    return;
}

void Region::exec_out_txn_query(google::protobuf::RpcController* controller,
            const pb::StoreReq* request, 
            pb::StoreRes* response, 
            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = (brpc::Controller*)controller;
    uint64_t log_id = 0;
    if (cntl->has_log_id()) { 
        log_id = cntl->log_id();
    }
    const char* remote_side = butil::endpoint2str(cntl->remote_side()).c_str();
    pb::OpType op_type = request->op_type();
    switch (op_type) {
        case pb::OP_SELECT: {
            TimeCost cost;
            select(*request, *response);
            int64_t select_cost = cost.get_time();
            Store::get_instance()->select_time_cost << select_cost;
            if (select_cost > FLAGS_time_cost_us) {
                DB_NOTICE("select type:%s, seq_id: %d, region_id: %ld, time_cost:%ld, log_id: %lu, remote_side: %s", 
                        pb::OpType_Name(request->op_type()).c_str(), 0, _region_id, cost.get_time(), log_id, remote_side);
            }
            break;
        }
        case pb::OP_INSERT:
        case pb::OP_DELETE:
        case pb::OP_UPDATE:
        case pb::OP_TRUNCATE_TABLE: {
            if (_split_param.split_slow_down) {
                DB_WARNING("region is spliting, slow down time:%ld, region_id: %ld, remote_side: %s",
                            _split_param.split_slow_down_cost, _region_id, remote_side);
                bthread_usleep(_split_param.split_slow_down_cost);
            }
            //TODO
            int64_t disable_write_wait = get_split_wait_time();
            int ret = _disable_write_cond.timed_wait(disable_write_wait);
            _real_writing_cond.increase();
            ScopeGuard auto_decrease([this]() {
                _real_writing_cond.decrease_signal();
            });
            if (ret != 0) {
                response->set_errcode(pb::DISABLE_WRITE_TIMEOUT);
                response->set_errmsg("_diable_write_cond wait timeout");
                DB_FATAL("_diable_write_cond wait timeout, ret:%d, region_id: %ld", ret, _region_id);
                return;
            }

            // double check，防止写不一致
            if (!_is_leader.load()) {
                response->set_errcode(pb::NOT_LEADER);
                response->set_leader(butil::endpoint2str(_node.leader_id().addr).c_str());
                response->set_errmsg("not leader");
                DB_WARNING("not leader old version, leader:%s, region_id: %ld, log_id:%lu",
                        butil::endpoint2str(_node.leader_id().addr).c_str(), _region_id, log_id);
                return;
            }
            if (validate_version(request, response) == false) {
                DB_WARNING("region version too old, region_id: %ld, log_id:%lu, "
                           "request_version:%ld, region_version:%ld",
                            _region_id, log_id, 
                            request->region_version(), _region_info.version());
                return;
            }

            butil::IOBuf data;
            butil::IOBufAsZeroCopyOutputStream wrapper(&data);
            if (!request->SerializeToZeroCopyStream(&wrapper)) {
                cntl->SetFailed(brpc::EREQUEST, "Fail to serialize request");
                return;
            }
            DMLClosure* c = new DMLClosure;
            c->cost.reset();
            c->op_type = op_type;
            c->cntl = cntl;
            c->response = response;
            c->done = done_guard.release();
            c->region = this;
            c->remote_side = remote_side;
            braft::Task task;
            task.data = &data;
            task.done = c;
            auto_decrease.release();
            _node.apply(task);
        } break;
        default: {
            response->set_errcode(pb::UNSUPPORT_REQ_TYPE);
            response->set_errmsg("unsupported out_txn_query type");
            DB_FATAL("unsupported out_txn_query type: %d, region_id: %ld, log_id:%lu", 
                op_type, _region_id, log_id);
        } break;
    }
    return;
}

void Region::query(google::protobuf::RpcController* controller,
                   const pb::StoreReq* request,
                   pb::StoreRes* response,
                   google::protobuf::Closure* done) {
    _time_cost.reset();
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = (brpc::Controller*)controller;
    uint64_t log_id = 0;
    if (cntl->has_log_id()) { 
        log_id = cntl->log_id();
    }
    const char* remote_side = butil::endpoint2str(cntl->remote_side()).c_str();
    if (!_is_leader.load() && 
        //为了性能，支持非一致性读
            (!request->select_without_leader() || _shutdown || !_init_success)) {
        response->set_errcode(pb::NOT_LEADER);
        response->set_leader(butil::endpoint2str(_node.leader_id().addr).c_str());
        response->set_errmsg("not leader");
        DB_WARNING("not leader, leader:%s, region_id: %ld, log_id:%lu, remote_side:%s",
                        butil::endpoint2str(_node.leader_id().addr).c_str(), 
                        _region_id, log_id, remote_side);
        return;
    }
    response->set_leader(butil::endpoint2str(_node.leader_id().addr).c_str()); // 每次都返回leader
    if (validate_version(request, response) == false) {
        //add_version的第二次或者打三次重试，需要把num_table_line返回回去
        if (request->op_type() == pb::OP_ADD_VERSION_FOR_SPLIT_REGION) {
            response->set_affected_rows(_num_table_lines.load());
            response->clear_txn_infos();
            std::unordered_map<uint64_t, pb::TransactionInfo> prepared_txn;
            _txn_pool.get_prepared_txn_info(prepared_txn, true);
            for (auto &pair : prepared_txn) {
                auto txn_info = response->add_txn_infos();
                txn_info->CopyFrom(pair.second);
            }
            DB_FATAL("region_id: %ld, num_table_lines:%ld, OP_ADD_VERSION_FOR_SPLIT_REGION retry", 
                    _region_id, _num_table_lines.load());
        }
        DB_WARNING("region version too old, region_id: %ld, log_id:%lu,"
                   " request_version:%ld, region_version:%ld",
                    _region_id, log_id, 
                    request->region_version(), _region_info.version());
        return;
    }
    // 启动时，或者follow落后太多，需要读leader
    if (request->op_type() == pb::OP_SELECT && request->region_version() > _region_info.version()) {
        response->set_errcode(pb::NOT_LEADER);
        response->set_leader(butil::endpoint2str(_node.leader_id().addr).c_str());
        response->set_errmsg("not leader");
        DB_WARNING("not leader, leader:%s, region_id: %ld, version:%ld, log_id:%lu, remote_side:%s",
                        butil::endpoint2str(_node.leader_id().addr).c_str(), 
                        _region_id, _region_info.version(), log_id, remote_side);
        return;
    }
    // int ret = 0;
    // TimeCost cost;
    switch (request->op_type()) {
        case pb::OP_SELECT:
        case pb::OP_INSERT:
        case pb::OP_DELETE:
        case pb::OP_UPDATE:
        case pb::OP_PREPARE_V2:
        case pb::OP_PREPARE:
        case pb::OP_COMMIT:
        case pb::OP_ROLLBACK:
        case pb::OP_TRUNCATE_TABLE: {
            uint64_t txn_id = 0;
            if (request->txn_infos_size() > 0) {
                txn_id = request->txn_infos(0).txn_id();
            }
            if (txn_id == 0 || request->op_type() == pb::OP_TRUNCATE_TABLE) {
                exec_out_txn_query(controller, request, response, done_guard.release());
            } else {
                exec_in_txn_query(controller, request, response, done_guard.release());
            }
            break;
        }
        case pb::OP_ADD_VERSION_FOR_SPLIT_REGION:
        case pb::OP_NONE: {
            butil::IOBuf data;
            butil::IOBufAsZeroCopyOutputStream wrapper(&data);
            if (!request->SerializeToZeroCopyStream(&wrapper)) {
                cntl->SetFailed(brpc::EREQUEST, "Fail to serialize request");
                return;
            }
            DMLClosure* c = new DMLClosure;
            c->cost.reset();
            c->op_type = request->op_type();
            c->cntl = cntl;
            c->response = response;
            c->done = done_guard.release();
            c->region = this;
            c->remote_side = remote_side;
            braft::Task task;
            task.data = &data;
            task.done = c;
            _real_writing_cond.increase();
            _node.apply(task);
            break;
        }
        default:
            response->set_errcode(pb::UNSUPPORT_REQ_TYPE);
            response->set_errmsg("unsupport request type");
            DB_WARNING("not support op_type when dml request,op_type:%d region_id: %ld, log_id:%lu",
                        request->op_type(), _region_id, log_id);
    }
    return;
}

void Region::dml(const pb::StoreReq& request, pb::StoreRes& response,
                 int64_t applied_index, int64_t term) {
    bool optimize_1pc = false;
    int32_t seq_id = 0;
    if (request.txn_infos_size() > 0) {
        optimize_1pc = request.txn_infos(0).optimize_1pc();
        seq_id = request.txn_infos(0).seq_id();
    }
    if ((request.op_type() == pb::OP_PREPARE || request.op_type() == pb::OP_PREPARE_V2) && optimize_1pc) {
        dml_1pc(request, request.op_type(), request.plan(), request.tuples(), 
            response, applied_index, term);
    } else {
        dml_2pc(request, request.op_type(), request.plan(), request.tuples(), 
            response, applied_index, term, seq_id);
    }
    return;
}

void Region::dml_2pc(const pb::StoreReq& request, 
        pb::OpType op_type,
        const pb::Plan& plan, 
        const RepeatedPtrField<pb::TupleDescriptor>& tuples, 
        pb::StoreRes& response, 
        int64_t applied_index, 
        int64_t term,
        int32_t seq_id) {
    //DB_WARNING("num_prepared:%d region_id: %ld", num_prepared(), _region_id);
    std::set<int> need_rollback_seq;
    if (request.txn_infos_size() == 0) {
        response.set_errcode(pb::EXEC_FAIL);
        response.set_errmsg("request txn_info is empty");
        DB_FATAL("request txn_info is empty: %ld", _region_id);
        return;
    }
    const pb::TransactionInfo& txn_info = request.txn_infos(0);
    for (int rollback_seq : txn_info.need_rollback_seq()) {
        need_rollback_seq.insert(rollback_seq);
    }
    int64_t txn_num_increase_rows = 0;

    uint64_t txn_id = txn_info.txn_id();
    auto txn = _txn_pool.get_txn(txn_id);
    // txn may be rollback by transfer leader thread
    if (op_type != pb::OP_BEGIN && (txn == nullptr || txn->is_rolledback())) {
        response.set_errcode(pb::NOT_LEADER);
        response.set_leader(butil::endpoint2str(_node.leader_id().addr).c_str());
        response.set_errmsg("not leader, maybe transfer leader");
        DB_WARNING("no txn found: region_id: %ld, txn_id: %lu:%d, op_type: %d", _region_id, txn_id, seq_id, op_type);
        return;
    }
    if (op_type != pb::OP_BEGIN && txn != nullptr) {
        // rollback already executed cmds
        for (int seq : need_rollback_seq) {
            if (txn->cache_plan_map().count(seq) == 0) {
                DB_WARNING("cache does not contain seq: %d", seq);
                continue;
            }
            txn->rollback_to_point(seq - 1);
            DB_WARNING("rollback seq_id: %d region_id: %ld, txn_id: %lu, seq_id: %d, req_seq: %d", 
                seq, _region_id, txn_id, txn->seq_id(), seq_id);
        }
        // if current cmd need rollback, simply not execute
        if (need_rollback_seq.count(seq_id) != 0) {
            DB_WARNING("cmd need rollback, not executed and cached. region_id: %ld, txn_id: %lu, seq_id: %d",
                _region_id, txn_id, seq_id);
            txn->set_seq_id(seq_id);
            return;
        }
        // set checkpoint for current DML operator
        if (op_type != pb::OP_PREPARE && op_type != pb::OP_PREPARE_V2 
                && op_type != pb::OP_COMMIT && op_type != pb::OP_ROLLBACK) {
            txn->set_save_point();
        }
        // 提前更新txn的当前seq_id，防止dml执行失败导致seq_id更新失败
        // 而导致当前region为follow_up, 每次都需要从baikaldb拉取cached命令
        txn->set_seq_id(seq_id);
        // 提前保存txn->num_increase_rows，以便事务提交/回滚时更新num_table_lines
        if (op_type == pb::OP_COMMIT) {
            txn_num_increase_rows = txn->num_increase_rows;
        }
    }

    int ret = 0;
    TimeCost cost;
    RuntimeState state;
    {
        BAIDU_SCOPED_LOCK(_ptr_mutex);
        state.set_resource(_resource);
    }
    ret = state.init(request, plan, tuples, &_txn_pool);
    if (ret < 0) {
        response.set_errcode(pb::EXEC_FAIL);
        response.set_errmsg("RuntimeState init fail");
        DB_FATAL("RuntimeState init fail, region_id: %ld, txn_id: %lu", _region_id, txn_id);
        return;
    }
    if (seq_id > 0) {
        // when executing cache query, use the seq_id of corresponding cache query (passed by user)
        state.seq_id = seq_id;
    }
    state.set_reverse_index_map(_reverse_index_map);
    ExecNode* root = nullptr;
    ret = ExecNode::create_tree(plan, &root);
    if (ret < 0) {
        ExecNode::destroy_tree(root);
        response.set_errcode(pb::EXEC_FAIL);
        response.set_errmsg("create plan fail");
        DB_FATAL("create plan fail, region_id: %ld, txn_id: %lu", _region_id, txn_id);
        return;
    }
    ret = root->open(&state);
    if (ret < 0) {
        root->close(&state);
        ExecNode::destroy_tree(root);
        response.set_errcode(pb::EXEC_FAIL);
        if (state.error_code != ER_ERROR_FIRST) {
            response.set_mysql_errcode(state.error_code);
            response.set_errmsg(state.error_msg.str());
        } else {
            response.set_errmsg("plan open failed");
        }
        DB_FATAL("plan open fail, region_id: %ld, txn_id: %lu", _region_id, txn_id);
        return;
    }

    txn = _txn_pool.get_txn(txn_id);
    if (txn != nullptr) {
        txn->set_seq_id(seq_id);
        auto& plan_map = txn->cache_plan_map();
        // DB_WARNING("seq_id: %d, %d, op:%d", seq_id, plan_map.count(seq_id), op_type);
        // commit/rollback命令不加缓存
        if (op_type != pb::OP_COMMIT && op_type != pb::OP_ROLLBACK && plan_map.count(seq_id) == 0) {
            pb::CachePlan plan_item;
            plan_item.set_op_type(op_type);
            plan_item.set_seq_id(seq_id);
            plan_item.mutable_plan()->CopyFrom(plan);
            for (auto& tuple : tuples) {
                plan_item.add_tuples()->CopyFrom(tuple);
            }
            plan_map.insert(std::make_pair(seq_id, plan_item));
            //DB_WARNING("put txn cmd to cache: region_id: %ld, txn_id: %lu:%d", _region_id, txn_id, seq_id);
        }
    } else if (op_type != pb::OP_COMMIT && op_type != pb::OP_ROLLBACK) {
        // after commit or rollback, txn will be deleted
        root->close(&state);
        ExecNode::destroy_tree(root);
        response.set_errcode(pb::NOT_LEADER);
        response.set_leader(butil::endpoint2str(_node.leader_id().addr).c_str());
        response.set_errmsg("not leader, maybe transfer leader");
        DB_WARNING("no txn found: region_id: %ld, txn_id: %lu:%d, op_type: %d", _region_id, txn_id, seq_id, op_type);
        return;
    }
    if (txn_info.autocommit() && (op_type == pb::OP_UPDATE || op_type == pb::OP_INSERT || op_type == pb::OP_DELETE)) {
        txn->dml_num_affected_rows = ret;
    }
    response.set_affected_rows(ret);
    root->close(&state);
    ExecNode::destroy_tree(root);
    response.set_errcode(pb::SUCCESS);

    if (op_type == pb::OP_TRUNCATE_TABLE) {
        _num_table_lines = 0;
        // truncate后主动执行compact
        DB_WARNING("region_id: %ld, truncate do compact in queue", _region_id);
        compact_data_in_queue();
    } else if (op_type != pb::OP_COMMIT && op_type != pb::OP_ROLLBACK) {
        txn->num_increase_rows += state.num_increase_rows();
    } else if (op_type == pb::OP_COMMIT) {
        // 事务提交/回滚时更新num_table_line
        _num_table_lines += txn_num_increase_rows;
        if (txn_num_increase_rows < 0) {
            _num_delete_lines -= txn_num_increase_rows;
        }
    }
    //这一步跟commit指令不原子，如果在这中间core会出错(todo)
    if (op_type == pb::OP_COMMIT || op_type == pb::OP_ROLLBACK) {
        rocksdb::WriteBatch batch;
        batch.Put(_writer->get_handle(), _writer->applied_index_key(_region_id), _writer->encode_applied_index(applied_index));
        batch.Put(_writer->get_handle(), _writer->num_table_lines_key(_region_id), _writer->encode_num_table_lines(_num_table_lines));
        batch.Delete(_writer->get_handle(), _writer->transcation_log_index_key(_region_id, txn_id));
        auto ret = _writer->write_batch(&batch, _region_id);
        //DB_WARNING("write meta info wheen commit or rollback,"
        //            " region_id: %ld, applied_index: %ld, num_table_line: %ld, txn_id: %lu", 
        //            _region_id, applied_index, _num_table_lines.load(), txn_id); 
        if (ret < 0) {
            DB_FATAL("Write Metainfo fail, region_id: %ld, txn_id: %lu, log_index: %ld", 
                        _region_id, txn_id, applied_index);
        }
    }
    if (op_type == pb::OP_INSERT || op_type == pb::OP_DELETE || op_type == pb::OP_UPDATE) {
       update_average_cost(cost.get_time()); 
    }
    int64_t dml_cost = cost.get_time();
    Store::get_instance()->dml_time_cost << dml_cost;
    if (dml_cost > FLAGS_time_cost_us) {
        DB_NOTICE("dml type:%d, time_cost:%ld, region_id: %ld, txn_id: %lu, num_table_lines:%ld, "
                  "affected_rows:%d, applied_index:%ld, term:%d, txn_num_rows:%ld,"
                  " average_cost: %ld, log_id:%lu", 
                op_type, cost.get_time(), _region_id, txn_id, _num_table_lines.load(), ret, 
                applied_index, term, txn_num_increase_rows, _average_cost.load(), state.log_id());
    }
}

void Region::dml_1pc(const pb::StoreReq& request, pb::OpType op_type,
        const pb::Plan& plan, const RepeatedPtrField<pb::TupleDescriptor>& tuples, 
        pb::StoreRes& response, int64_t applied_index, int64_t term) {
    //DB_WARNING("_num_table_lines:%ld region_id: %ld", _num_table_lines.load(), _region_id);
    int ret = 0;
    TimeCost cost;
    RuntimeState state;
    {
        BAIDU_SCOPED_LOCK(_ptr_mutex);
        state.set_resource(_resource);
    }
    ret = state.init(request, plan, tuples, &_txn_pool);
    if (ret < 0) {
        response.set_errcode(pb::EXEC_FAIL);
        response.set_errmsg("RuntimeState init fail");
        DB_FATAL("RuntimeState init fail, region_id: %ld, applied_index: %ld", 
                    _region_id, applied_index);
        return;
    }
    // for out-txn dml query, create new txn.
    // for single-region 2pc query, simply fetch the txn created before.
    bool is_new_txn = !((request.op_type() == pb::OP_PREPARE || request.op_type() == pb::OP_PREPARE_V2) 
            && request.txn_infos(0).optimize_1pc());
    if (is_new_txn) {
        state.create_txn_if_null();
    }
    bool commit_succ = false;
    ScopeGuard auto_rollback([&]() {
        if (!state.txn()) {
            return;
        }
        // rollback if not commit succ
        if (false == commit_succ) {
            state.txn()->rollback();
        }
        // if txn in pool (new_txn == false), remove it from pool
        // else directly delete it
        if (false == is_new_txn) {
            _txn_pool.remove_txn(state.txn_id);
        }
    });
    auto txn = state.txn();
    int64_t tmp_num_table_lines = _num_table_lines;
    if (plan.nodes_size() > 0) {
        // for single-region autocommit and force-1pc cmd, exec the real dml cmd
        state.set_reverse_index_map(_reverse_index_map);
        ExecNode* root = nullptr;
        ret = ExecNode::create_tree(plan, &root);
        if (ret < 0) {
            ExecNode::destroy_tree(root);
            response.set_errcode(pb::EXEC_FAIL);
            response.set_errmsg("create plan fail");
            DB_FATAL("create plan fail, region_id: %ld, txn_id: %lu:%d, applied_index: %ld", 
                _region_id, state.txn_id, state.seq_id, applied_index);
            return;
        }
        ret = root->open(&state);
        if (ret < 0) {
            root->close(&state);
            ExecNode::destroy_tree(root);
            response.set_errcode(pb::EXEC_FAIL);
            if (state.error_code != ER_ERROR_FIRST) {
                response.set_mysql_errcode(state.error_code);
                response.set_errmsg(state.error_msg.str());
            } else {
                response.set_errmsg("plan open fail");
            }
            if (state.error_code == ER_DUP_KEY) {
                DB_WARNING("plan open fail, region_id: %ld, txn_id: %lu:%d, "
                        "applied_index: %ld, error_code: %d, mysql_errcode:%d", 
                        _region_id, state.txn_id, state.seq_id, applied_index, 
                        state.error_code, state.error_code);
            } else {
                DB_FATAL("plan open fail, region_id: %ld, txn_id: %lu:%d, "
                        "applied_index: %ld, error_code: %d, mysql_errcode:%d", 
                        _region_id, state.txn_id, state.seq_id, applied_index, 
                        state.error_code, state.error_code);
            }
            return;
        }
        root->close(&state);
        ExecNode::destroy_tree(root);
    }
    if (op_type != pb::OP_TRUNCATE_TABLE) {
        txn->num_increase_rows += state.num_increase_rows();
    } else {
        tmp_num_table_lines = 0;
        // truncate后主动执行compact
        DB_WARNING("region_id: %ld, truncate do compact in queue", _region_id);
        compact_data_in_queue();
    }
    int64_t txn_num_increase_rows = txn->num_increase_rows;
    tmp_num_table_lines += txn_num_increase_rows;
    //老的insert update delete接口
    if (state.txn_id == 0) {
        txn->put_meta_info(_writer->applied_index_key(_region_id), _writer->encode_applied_index(_applied_index));
        txn->put_meta_info(_writer->num_table_lines_key(_region_id), _writer->encode_num_table_lines(tmp_num_table_lines));
        //DB_WARNING("write meta info when dml_1pc,"
        //            " region_id: %ld, num_table_line: %ld, applied_index: %ld", 
        //            _region_id, tmp_num_table_lines, _applied_index);
    }
    auto res = txn->commit();
    if (res.ok()) {
        commit_succ = true;
    } else if (res.IsExpired()) {
        DB_WARNING("txn expired, region_id: %ld, txn_id: %lu, applied_index: %ld", 
                    _region_id, state.txn_id, applied_index);
        commit_succ = false;
    } else {
        DB_WARNING("unknown error: region_id: %ld, txn_id: %lu, errcode:%d, msg:%s", 
            _region_id, state.txn_id, res.code(), res.ToString().c_str());
        commit_succ = false;
    }
    if (commit_succ) {
        if (txn_num_increase_rows < 0) {
            _num_delete_lines -= txn_num_increase_rows;
        }
        _num_table_lines = tmp_num_table_lines;
        response.set_affected_rows(ret);
        response.set_errcode(pb::SUCCESS);
    } else {
        response.set_errcode(pb::EXEC_FAIL);
        response.set_errmsg("txn commit failed.");
        DB_FATAL("txn commit failed, region_id: %ld, txn_id: %lu, applied_index: %ld", 
                    _region_id, state.txn_id, applied_index);
    }
    //跟commit指令不原子，如果在这中间core会出错(todo)
    if (state.txn_id != 0) {
        rocksdb::WriteBatch batch;
        batch.Put(_writer->get_handle(), _writer->applied_index_key(_region_id), _writer->encode_applied_index(applied_index));
        batch.Put(_writer->get_handle(), _writer->num_table_lines_key(_region_id), _writer->encode_num_table_lines(_num_table_lines));
        batch.Delete(_writer->get_handle(), _writer->transcation_log_index_key(_region_id, state.txn_id));
        auto ret = _writer->write_batch(&batch, _region_id);
        DB_WARNING("write meta info wheen commit or rollback,"
                    " region_id: %ld, applied_index: %ld, num_table_line: %ld, txn_id: %lu", 
                    _region_id, applied_index, _num_table_lines.load(), state.txn_id); 
        if (ret < 0) {
            DB_FATAL("Write Metainfo fail, region_id: %ld, txn_id: %lu, log_index: %ld", 
                        _region_id, state.txn_id, applied_index);
        }
    }
    if (state.txn_id != 0 && 
            (op_type == pb::OP_INSERT || op_type == pb::OP_DELETE || op_type == pb::OP_UPDATE)) {
       update_average_cost(cost.get_time());  
    }
    int64_t dml_cost = cost.get_time();
    Store::get_instance()->dml_time_cost << dml_cost;
    if (dml_cost > FLAGS_time_cost_us) {
        DB_NOTICE("dml type:%d, time_cost:%ld, region_id: %ld, txn_id: %lu, num_table_lines:%ld, "
                  "affected_rows:%d, applied_index:%ld, term:%d, txn_num_rows:%ld,"
                  " average_cost: %ld, log_id:%lu", 
                op_type, cost.get_time(), _region_id, state.txn_id, _num_table_lines.load(), ret, 
                applied_index, term, txn_num_increase_rows, _average_cost.load(), state.log_id());
    }
}

void Region::select(const pb::StoreReq& request, pb::StoreRes& response) {
    select(request, request.plan(), request.tuples(), response);
}

void Region::select(const pb::StoreReq& request, 
        const pb::Plan& plan,
        const RepeatedPtrField<pb::TupleDescriptor>& tuples,
        pb::StoreRes& response) {
    //DB_WARNING("req:%s", request.DebugString().c_str());
    int ret = 0;
    RuntimeState state;
    {
        BAIDU_SCOPED_LOCK(_ptr_mutex);
        state.set_resource(_resource);
    }
    ret = state.init(request, plan, tuples, &_txn_pool);
    if (ret < 0) {
        response.set_errcode(pb::EXEC_FAIL);
        response.set_errmsg("RuntimeState init fail");
        DB_FATAL("RuntimeState init fail, region_id: %ld", _region_id);
        return;
    }
    // double check, ensure resource match the req version
    if (validate_version(&request, &response) == false) {
        DB_WARNING("double check region version too old, region_id: %ld,"
                   " request_version:%ld, region_version:%ld",
                    _region_id, request.region_version(), _region_info.version());
        return;
    }
    const pb::TransactionInfo& txn_info = request.txn_infos(0);
    bool is_new_txn = false;
    auto txn = state.txn();
    if (txn_info.txn_id() != 0 && (txn == nullptr || txn->is_rolledback())) {
        response.set_errcode(pb::NOT_LEADER);
        response.set_leader(butil::endpoint2str(_node.leader_id().addr).c_str());
        response.set_errmsg("not leader, maybe transfer leader");
        DB_WARNING("no txn found: region_id: %ld, txn_id: %lu:%d", _region_id, txn_info.txn_id(), txn_info.seq_id());
        return;
    }
    if (txn != nullptr) {
        std::set<int> need_rollback_seq;
        for (int rollback_seq : txn_info.need_rollback_seq()) {
            need_rollback_seq.insert(rollback_seq);
        }
        // rollback already executed cmds
        for (int seq : need_rollback_seq) {
            if (txn->cache_plan_map().count(seq) == 0) {
                continue;
            }
            txn->rollback_to_point(seq - 1);
            DB_WARNING("rollback seq_id: %d region_id: %ld, txn_id: %lu, seq_id: %d", 
                seq, _region_id, txn->txn_id(), txn->seq_id());
        }
    } else {
        // DB_WARNING("create tmp txn for select cmd: %ld", _region_id)
        is_new_txn = true;
        txn = state.create_txn_if_null();
    }
    ScopeGuard auto_rollback([&]() {
        if (is_new_txn) {
            txn->rollback();
        }
    });

    state.set_reverse_index_map(_reverse_index_map);
    MemRowDescriptor* mem_row_desc = state.mem_row_desc();
    ExecNode* root = nullptr;    
    ret = ExecNode::create_tree(plan, &root);
    if (ret < 0) {
        ExecNode::destroy_tree(root);
        response.set_errcode(pb::EXEC_FAIL);
        response.set_errmsg("create plan fail");
        DB_FATAL("create plan fail, region_id: %ld", _region_id);
        return;
    }
    ret = root->open(&state);
    if (ret < 0) {
        root->close(&state);
        ExecNode::destroy_tree(root);
        response.set_errcode(pb::EXEC_FAIL);
        if (state.error_code != ER_ERROR_FIRST) {
            response.set_mysql_errcode(state.error_code);
            response.set_errmsg(state.error_msg.str());
        } else {       
            response.set_errmsg("plan open fail");
        }
        DB_FATAL("plan open fail, region_id: %ld", _region_id);
        return;
    }
    bool eos = false;
    int count = 0;
    int rows = 0;
    for (auto& tuple : state.tuple_descs()) {
        response.add_tuple_ids(tuple.tuple_id());
    }
    while (!eos) {
        RowBatch batch;
        batch.set_capacity(state.row_batch_capacity());
        ret = root->get_next(&state, &batch, &eos);
        if (ret < 0) {
            root->close(&state);
            ExecNode::destroy_tree(root);
            response.set_errcode(pb::EXEC_FAIL);
            response.set_errmsg("plan get_next fail");
            DB_FATAL("plan get_next fail, region_id: %ld", _region_id);
            return;
        }
        count++;
        for (batch.reset(); !batch.is_traverse_over(); batch.next()) {
            MemRow* row = batch.get_row().get();
            rows++;
            if (row == NULL) {
                DB_FATAL("row is null; region_id: %ld, rows:%d", _region_id, rows);
                continue;
            }
            pb::RowValue* row_value = response.add_row_values();
            for (int i = 0; i < mem_row_desc->tuple_size(); i++) {
                std::string* tuple_value = row_value->add_tuple_values();
                row->to_string(i, tuple_value);
            }
        }
    }
    root->close(&state);
    ExecNode::destroy_tree(root);
    response.set_errcode(pb::SUCCESS);
    if (is_new_txn) {
        txn->commit(); // no write & lock, no failure
        auto_rollback.release();
    }
}

void Region::construct_heart_beat_request(pb::StoreHeartBeatRequest& request, bool need_peer_balance) {
    if (_shutdown) {
        return;
    }
    if (_num_delete_lines > FLAGS_compact_delete_lines) {
        DB_WARNING("region_id: %ld, delete %ld rows, do compact in queue",
                _region_id, _num_delete_lines.load());
        // 删除大量数据后做compact
        compact_data_in_queue();
    }
    if (_region_info.version() == 0) {
        DB_WARNING("region version is 0, region_id: %ld", _region_id);
        return;
    }
    _region_info.set_num_table_lines(_num_table_lines.load());
    //增加peer心跳信息
    if (need_peer_balance && _report_peer_info) {
        pb::PeerHeartBeat* peer_info = request.add_peer_infos();
        peer_info->set_table_id(_region_info.table_id());
        peer_info->set_region_id(_region_id);
        peer_info->set_log_index(_applied_index);
    }
    //添加leader的心跳信息，同时更新状态
    std::vector<braft::PeerId> peers;
    if (is_leader() && _node.list_peers(&peers).ok()) {
        pb::LeaderHeartBeat* leader_heart = request.add_leader_regions();
        leader_heart->set_status(_region_control.get_status());
        pb::RegionInfo* leader_region =  leader_heart->mutable_region();
        copy_region(leader_region);
        leader_region->set_status(_region_control.get_status());
        //在分裂线程里更新used_sized
        leader_region->set_used_size(_region_info.used_size());
        leader_region->set_leader(_address);
        //fix bug 不能直接取reigon_info的log index, 
        //因为如果系统在做过snapshot再重启之后，一直没有数据，
        //region info里的log index是之前持久化在磁盘的log index, 这个log index不准
        leader_region->set_log_index(_applied_index);
        ////填到心跳包中，并且更新本地缓存，只有leader操作
        //_region_info.set_leader(_address);
        //_region_info.clear_peers();
        leader_region->clear_peers();
        for (auto& peer : peers) {
            leader_region->add_peers(butil::endpoint2str(peer.addr).c_str());
            //_region_info.add_peers(butil::endpoint2str(peer.addr).c_str());
        }
    }
}

void Region::set_can_add_peer() {
    if (!_region_info.has_can_add_peer() || !_region_info.can_add_peer()) {
        pb::RegionInfo region_info_mem;
        copy_region(&region_info_mem);
        region_info_mem.set_can_add_peer(true);
        if (_writer->update_region_info(region_info_mem) != 0) {
            DB_FATAL("update can add peer fail, region_id: %ld", _region_id); 
        } else {
            DB_WARNING("update can add peer success, region_id: %ld", _region_id);
        }
        _region_info.set_can_add_peer(true);
    }
}

void Region::on_apply(braft::Iterator& iter) {
    Concurrency::get_instance()->service_write_concurrency.increase_wait();
    ON_SCOPE_EXIT([]() {
        Concurrency::get_instance()->service_write_concurrency.decrease_broadcast();
    });
    for (; iter.valid(); iter.next()) {
        braft::Closure* done = iter.done();
        brpc::ClosureGuard done_guard(done);
        butil::IOBuf data = iter.data();
        butil::IOBufAsZeroCopyInputStream wrapper(data);
        pb::StoreReq request;
        if (!request.ParseFromZeroCopyStream(&wrapper)) {
            DB_FATAL("parse from protobuf fail, region_id: %ld", _region_id);
            if (done) {
                ((DMLClosure*)done)->response->set_errcode(pb::PARSE_FROM_PB_FAIL);
                ((DMLClosure*)done)->response->set_errmsg("parse from protobuf fail");
                braft::run_closure_in_bthread(done_guard.release());
            }
            continue;
        }
        pb::OpType op_type = request.op_type();
        _region_info.set_log_index(iter.index());
        if (iter.index() <= _applied_index) {
            //DB_WARNING("this log entry has been executed, log_index:%ld, applied_index:%ld, region_id: %ld",
            //            iter.index(), _applied_index, _region_id);
            continue;
        }
        _applied_index = iter.index();
        int64_t term = iter.term();

        pb::StoreRes res;
        switch (op_type) {
            case pb::OP_PREPARE_V2:
            case pb::OP_PREPARE:
            case pb::OP_COMMIT:
            case pb::OP_ROLLBACK: {
                apply_txn_request(request, done, _applied_index, term);
                break;
            }
            // 兼容老版本无事务功能时的log entry, 以及强制1PC的DML query(如灌数据时使用)
            case pb::OP_INSERT:
            case pb::OP_DELETE:
            case pb::OP_UPDATE: 
            case pb::OP_TRUNCATE_TABLE: {
                dml_1pc(request, request.op_type(), request.plan(), request.tuples(), 
                    res, iter.index(), iter.term());
                if (done) {
                    ((DMLClosure*)done)->response->set_errcode(res.errcode());
                    if (res.has_errmsg()) {
                        ((DMLClosure*)done)->response->set_errmsg(res.errmsg());
                    }
                      if (res.has_mysql_errcode()) {
                            ((DMLClosure*)done)->response->set_mysql_errcode(res.mysql_errcode());
                        }
                    if (res.has_leader()) {
                        ((DMLClosure*)done)->response->set_leader(res.leader());
                    }
                    if (res.has_affected_rows()) {
                        ((DMLClosure*)done)->response->set_affected_rows(res.affected_rows());
                    }
                }
                break;
            }
            //split的各类请求传进的来的done类型各不相同，不走下边的if(done)逻辑，直接处理完成，然后continue
            case pb::OP_NONE: {
                _writer->update_apply_index(_region_id, _applied_index);
                if (done) {
                    ((DMLClosure*)done)->response->set_errcode(pb::SUCCESS);
                }
                DB_NOTICE("op_type=%s, region_id: %ld, applied_index:%ld, term:%d", 
                    pb::OpType_Name(request.op_type()).c_str(), _region_id, _applied_index, term);
                break;
            }
            case pb::OP_START_SPLIT: {
                start_split(done, _applied_index, term); 
                DB_NOTICE("op_type: %s, region_id: %ld, applied_index:%ld, term:%d", 
                    pb::OpType_Name(request.op_type()).c_str(), _region_id, _applied_index, term);
                break;
            }
            case pb::OP_START_SPLIT_FOR_TAIL: {
                start_split_for_tail(done, _applied_index, term);
                DB_NOTICE("op_type: %s, region_id: %ld, applied_index:%ld, term:%d", 
                    pb::OpType_Name(request.op_type()).c_str(), _region_id, _applied_index, term);
                break;
            }
            case pb::OP_VALIDATE_AND_ADD_VERSION: {
                validate_and_add_version(request, done, _applied_index, term);
                DB_NOTICE("op_type: %s, region_id: %ld, applied_index:%ld, term:%d", 
                    pb::OpType_Name(request.op_type()).c_str(), _region_id, _applied_index, term);
                break;
            }
            case pb::OP_ADD_VERSION_FOR_SPLIT_REGION: {
                add_version_for_split_region(request, done, _applied_index, term); 
                DB_NOTICE("op_type: %s, region_id: %ld, applied_index:%ld, term:%d", 
                    pb::OpType_Name(request.op_type()).c_str(), _region_id, _applied_index, term);
                break;
            }
            default:
                _writer->update_apply_index(_region_id, _applied_index);
                DB_WARNING("unsupport request type, op_type:%d, region_id: %ld", 
                        request.op_type(), _region_id);
                if (done) {
                    ((DMLClosure*)done)->response->set_errcode(pb::UNSUPPORT_REQ_TYPE); 
                    ((DMLClosure*)done)->response->set_errmsg("unsupport request type");
                }
                DB_NOTICE("op_type: %s, region_id: %ld, applied_index:%ld, term:%d", 
                    pb::OpType_Name(request.op_type()).c_str(), _region_id, _applied_index, term);
                break;
        }
        if (done) {
            braft::run_closure_in_bthread(done_guard.release());
        }
    }
}

void Region::apply_txn_request(const pb::StoreReq& request, braft::Closure* done, int64_t index, int64_t term) {
    uint64_t txn_id = request.txn_infos_size() > 0 ? request.txn_infos(0).txn_id():0;
    if (txn_id == 0) {
        if (done) {
            ((DMLClosure*)done)->response->set_errcode(pb::INPUT_PARAM_ERROR);
            ((DMLClosure*)done)->response->set_errmsg("txn control cmd out-of-txn");
        }
        return;
    }
    pb::StoreRes res;
    pb::OpType op_type = request.op_type();
    auto txn = _txn_pool.get_txn(txn_id);
    int ret = 0;
    if (op_type == pb::OP_PREPARE_V2 || op_type == pb::OP_PREPARE) {
        // for tail splitting new region replay txn
        if (request.has_start_key() && !request.start_key().empty()) {
            pb::RegionInfo region_info_mem;
            copy_region(&region_info_mem);
            region_info_mem.set_start_key(request.start_key());
            set_region(region_info_mem);
        }
        ret = execute_cached_cmd(request, res, txn_id, txn, index, term);
    }
    if (ret != 0) {
        DB_FATAL("on_prepare execute cached cmd failed, region:%ld, txn_id:%lu", _region_id, txn_id);
        if (done) {
            ((DMLClosure*)done)->response->set_errcode(res.errcode());
            if (res.has_errmsg()) {
                ((DMLClosure*)done)->response->set_errmsg(res.errmsg());
            }
            if (res.has_mysql_errcode()) {
                ((DMLClosure*)done)->response->set_mysql_errcode(res.mysql_errcode());
            }
            if (res.has_leader()) {
                ((DMLClosure*)done)->response->set_leader(res.leader());
            }
        }
        return;
    }
    // rollback is executed only if txn is not null (since we do not execute
    // cached cmd for rollback, the txn handler may be nullptr)
    if (op_type != pb::OP_ROLLBACK || txn != nullptr) {
        //perared指令并且不能优化为1pc
        if (op_type == pb::OP_PREPARE || op_type == pb::OP_PREPARE_V2) {
            rocksdb::WriteBatch batch;
            batch.Put(_writer->get_handle(), _writer->applied_index_key(_region_id), _writer->encode_applied_index(index));
            batch.Put(_writer->get_handle(), 
                        _writer->transcation_log_index_key(_region_id, txn_id), 
                        _writer->encode_transcation_log_index_value(index));
            auto ret = _writer->write_batch(&batch, _region_id);
            //DB_WARNING("write meta info when prepare, region_id: %ld, applied_index: %ld, txn_id: %ld", 
            //            _region_id, index, txn_id);
            if (ret < 0) {
                res.set_errcode(pb::EXEC_FAIL);
                res.set_errmsg("Write Metainfo fail");
                DB_FATAL("Write Metainfo fail, region_id: %ld, txn_id: %lu, log_index: %ld", 
                            _region_id, txn_id, index);
                return;
            }
        }
        dml(request, res, index, term);
    } else {
        DB_WARNING("rollback a not started txn, region_id: %ld, txn_id: %lu",
            _region_id, txn_id);
    }
    if (done) {
        ((DMLClosure*)done)->response->set_errcode(res.errcode());
        if (res.has_errmsg()) {
            ((DMLClosure*)done)->response->set_errmsg(res.errmsg());
        }
          if (res.has_mysql_errcode()) {
                ((DMLClosure*)done)->response->set_mysql_errcode(res.mysql_errcode());
            }
        if (res.has_leader()) {
            ((DMLClosure*)done)->response->set_leader(res.leader());
        }
        if (res.has_affected_rows()) {
            ((DMLClosure*)done)->response->set_affected_rows(res.affected_rows());
        }
    }
}
void Region::start_split(braft::Closure* done, int64_t applied_index, int64_t term) {
    _writer->update_apply_index(_region_id, applied_index);
    //只有leader需要处理split请求，记录当前的log_index, term和迭代器
    if (done) {
        _split_param.split_start_index = applied_index + 1;
        _split_param.split_term = term;
        _split_param.snapshot = _rocksdb->get_db()->GetSnapshot();
        _txn_pool.get_prepared_txn_info(_split_param.prepared_txn, true);

        ((SplitClosure*)done)->ret = 0;
        if (_split_param.snapshot == nullptr) {
            ((SplitClosure*)done)->ret = -1;
        }
        DB_WARNING("begin start split, region_id: %ld, split_start_index:%ld, term:%ld, num_prepared: %lu",
                    _region_id, applied_index + 1, term, _split_param.prepared_txn.size());
    } else {
        DB_WARNING("only leader process start split request, region_id: %ld", _region_id);
    }
}

void Region::start_split_for_tail(braft::Closure* done, int64_t applied_index, int64_t term) {    
    _writer->update_apply_index(_region_id, applied_index);
    if (done) {
        _split_param.split_end_index = applied_index;
        _split_param.split_term = term;
        int64_t tableid = _region_info.table_id();
        if (tableid < 0) {
            DB_WARNING("invalid tableid: %ld, region_id: %ld", 
                        tableid, _region_id);
            ((SplitClosure*)done)->ret = -1;
            return;
        }
        rocksdb::ReadOptions read_options;
        read_options.total_order_seek = true;
        read_options.prefix_same_as_start = false;
        std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _data_cf));
        _txn_pool.get_prepared_txn_info(_split_param.prepared_txn, true);

        MutTableKey key;
        //不够精确，但暂且可用。不允许主键是FFFFF
        key.append_i64(_region_id).append_i64(tableid).append_u64(0xFFFFFFFFFFFFFFFF);
        iter->SeekForPrev(key.data());
        if (!iter->Valid()) {
            DB_WARNING("get split key for tail split fail, region_id: %ld, tableid:%ld, iter not valid",
                        _region_id, tableid);
            ((SplitClosure*)done)->ret = -1;
            return;
        }
        if (iter->key().size() <= 16 || !iter->key().starts_with(key.data().substr(0, 16))) {
            DB_WARNING("get split key for tail split fail, region_id: %ld, data:%s, key_size:%ld",
                        _region_id, rocksdb::Slice(iter->key().data()).ToString(true).c_str(), 
                        iter->key().size());
            ((SplitClosure*)done)->ret = -1;
            return;
        }
        TableKey table_key(iter->key());
        int64_t _region = table_key.extract_i64(0);
        int64_t _table = table_key.extract_i64(sizeof(int64_t));
        if (tableid != _table || _region_id != _region) {
            DB_WARNING("get split key for tail split fail, region_id: %ld:%ld, tableid:%ld:%ld,"
                    "data:%s", _region_id, _region, tableid, _table, iter->key().data());
            ((SplitClosure*)done)->ret = -1;
            return;
        }
        _split_param.split_key = std::string(iter->key().data() + 16, iter->key().size() - 16) 
                                 + std::string(1, 0xFF);
        DB_WARNING("table_id:%ld, tail split, split_key:%s, region_id: %ld, num_prepared: %lu",
                   tableid, rocksdb::Slice(_split_param.split_key).ToString(true).c_str(), 
                   _region_id, _split_param.prepared_txn.size());
    } else {
        DB_WARNING("only leader process start split for tail, region_id: %ld", _region_id);
    }
}
void Region::validate_and_add_version(const pb::StoreReq& request, braft::Closure* done, int64_t applied_index, int64_t term) {
    rocksdb::WriteBatch batch;
    batch.Put(_writer->get_handle(), _writer->applied_index_key(_region_id), _writer->encode_applied_index(applied_index));
    ON_SCOPE_EXIT(([this, &batch]() {
            _writer->write_batch(&batch, _region_id);
            DB_WARNING("write metainfo when add version, region_id: %ld", _region_id); 
        }));
    if (request.split_term() != term || request.split_end_index() + 1 != applied_index) {
        DB_FATAL("split fail, region_id: %ld, new_region_id: %ld, split_term:%ld, "
                "current_term:%ld, split_end_index:%ld, current_index:%ld, disable_write:%d",
                _region_id, _split_param.new_region_id,
                request.split_term(), term, request.split_end_index(), 
                applied_index, _disable_write_cond.count());
        if (done) {
            start_thread_to_remove_region(_split_param.new_region_id, _split_param.instance);
            ((SplitClosure*)done)->ret = -1;
        }
        return;
    }
    //持久化数据到rocksdb
    pb::RegionInfo region_info_mem;
    copy_region(&region_info_mem);
    region_info_mem.set_version(request.region_version());
    region_info_mem.set_end_key(request.end_key());
    batch.Put(_writer->get_handle(), _writer->region_info_key(_region_id), _writer->encode_region_info(region_info_mem));
    _new_region_infos.push_back(request.new_region_info());
    if (done) {
        ((SplitClosure*)done)->ret = 0;
    }
    DB_WARNING("update region info for all peer,"
                " region_id: %ld, add version %ld=>%ld, number_table_line:%ld, delta_number_table_line:%ld, "
                "applied_index:%ld, term:%ld",
                _region_id, 
                _region_info.version(), request.region_version(),
                _num_table_lines.load(), request.reduce_num_lines(),
                applied_index, term);
    set_region_with_update_range(region_info_mem);
    _num_table_lines -= request.reduce_num_lines();
    batch.Put(_writer->get_handle(), _writer->num_table_lines_key(_region_id), _writer->encode_num_table_lines(_num_table_lines));
    for (auto& txn_info : request.txn_infos()) {
        _txn_pool.update_txn_num_rows_after_split(txn_info);
    }
    // 分裂后主动执行compact
    DB_WARNING("region_id: %ld, new_region_id: %ld, split do compact in queue", 
            _region_id, _split_param.new_region_id);
    compact_data_in_queue();
}

void Region::add_version_for_split_region(const pb::StoreReq& request, braft::Closure* done, int64_t applied_index, int64_t term) {
    rocksdb::WriteBatch batch;
    batch.Put(_writer->get_handle(), _writer->applied_index_key(_region_id), _writer->encode_applied_index(applied_index));
    if (!compare_and_set_legal_for_split()) {
        _writer->write_batch(&batch, _region_id);    
        DB_FATAL("split timeout, region was set split fail, region_id: %ld", _region_id);
        if (done) {
            ((DMLClosure*)done)->response->set_errcode(pb::SPLIT_TIMEOUT);
            ((DMLClosure*)done)->response->set_errmsg("split timeout");
        }
        return;
    }
    pb::RegionInfo region_info_mem;
    copy_region(&region_info_mem);
    region_info_mem.set_version(1);
    region_info_mem.set_status(pb::IDLE);
    region_info_mem.set_start_key(request.start_key());
    batch.Put(_writer->get_handle(), _writer->region_info_key(_region_id), _writer->encode_region_info(region_info_mem));
    int ret = _writer->write_batch(&batch, _region_id);
    //DB_WARNING("write meta info for new split region, region_id: %ld", _region_id);
    if (ret != 0) {
        DB_FATAL("add version for new region when split fail, region_id: %ld", _region_id);
        //回滚一下，上边的compare会把值置为1, 出现这个问题就需要手工删除这个region
        _region_info.set_version(0);
        if (done) {
            ((DMLClosure*)done)->response->set_errcode(pb::INTERNAL_ERROR);
            ((DMLClosure*)done)->response->set_errmsg("write region to rocksdb fail");
        }
    } else {
        DB_WARNING("new region add verison, region status was reset, region_id: %ld, "
                    "applied_index:%ld, term:%ld", 
                    _region_id, _applied_index, term);
        _region_control.reset_region_status();
        set_region_with_update_range(region_info_mem);
        std::unordered_map<uint64_t, pb::TransactionInfo> prepared_txn;
        _txn_pool.get_prepared_txn_info(prepared_txn, true);
        if (done) {
            ((DMLClosure*)done)->response->set_errcode(pb::SUCCESS);
            ((DMLClosure*)done)->response->set_errmsg("success");
            ((DMLClosure*)done)->response->set_affected_rows(_num_table_lines.load());
            ((DMLClosure*)done)->response->clear_txn_infos();
            for (auto &pair : prepared_txn) {
                auto txn_info = ((DMLClosure*)done)->response->add_txn_infos();
                txn_info->CopyFrom(pair.second);
            }
        }
    }
    
} 

void Region::on_shutdown() {
     DB_WARNING("shut down, region_id: %ld", _region_id);
}

void Region::on_leader_start() {
    DB_WARNING("leader start, region_id: %ld", _region_id);
    _is_leader.store(true);
    _region_info.set_leader(butil::endpoint2str(_node.leader_id().addr).c_str());
}

void Region::on_leader_start(int64_t term) {
    DB_WARNING("leader start at term:%ld, region_id: %ld", term, _region_id);
    on_leader_start();
}

void Region::on_leader_stop() {
    DB_WARNING("leader stop at term, region_id: %ld", _region_id);
    _is_leader.store(false);
    _txn_pool.on_leader_stop_rollback();
}

void Region::on_leader_stop(const butil::Status& status) {   
    DB_WARNING("leader stop, region_id: %ld, error_code:%d, error_des:%s",
                _region_id, status.error_code(), status.error_cstr());
    _is_leader.store(false);
    _txn_pool.on_leader_stop_rollback();
}

void Region::on_error(const ::braft::Error& e) {
    DB_FATAL("raft node meet error, region_id: %ld, error_type:%d, error_desc:%s",
                _region_id, e.type(), e.status().error_cstr());
}

void Region::on_configuration_committed(const::braft::Configuration& conf) {
    on_configuration_committed(conf, 0);
}

void Region::on_configuration_committed(const::braft::Configuration& conf, int64_t index) {
    if (_applied_index < index) {
        _applied_index = index;
    }
    std::vector<braft::PeerId> peers;
    conf.list_peers(&peers);
    std::string conf_str;
    pb::RegionInfo tmp_region;
    copy_region(&tmp_region);
    tmp_region.clear_peers();
    for (auto& peer : peers) {
        if (butil::endpoint2str(peer.addr).c_str() == _address)  {
            _report_peer_info = true;
        }
        tmp_region.add_peers(butil::endpoint2str(peer.addr).c_str());
        conf_str += std::string(butil::endpoint2str(peer.addr).c_str()) + ",";
    }
    tmp_region.set_leader(butil::endpoint2str(_node.leader_id().addr).c_str());
    set_region(tmp_region);
    DB_WARNING("region_id: %ld, configurantion:%s leader:%s, log_index: %ld",
                _region_id, conf_str.c_str(),
                butil::endpoint2str(_node.leader_id().addr).c_str(), index); 
}

void Region::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
    TimeCost time_cost;
    brpc::ClosureGuard done_guard(done);
    if (writer->add_file(SNAPSHOT_DATA_FILE) != 0
            || writer->add_file(SNAPSHOT_META_FILE) != 0) {
        done->status().set_error(EINVAL, "Fail to add snapshot");
        DB_WARNING("Error while adding extra_fs to writer, region_id: %ld", _region_id);
        return;
    }
    DB_WARNING("region_id: %ld shnapshot save complete, time_cost: %ld", 
                _region_id, time_cost.get_time());
    _snapshot_num_table_lines = _num_table_lines.load();
    _snapshot_index = _applied_index;
    _snapshot_time_cost.reset();
}
void Region::snapshot(braft::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    bool need_snapshot = false;
    if (_shutdown) {
        return;
    }
    if (_snapshot_time_cost.get_time() < FLAGS_snapshot_interval_s * 1000 * 1000) {
        return;
    }
    if (_applied_index - _snapshot_index > FLAGS_snapshot_diff_logs) {
        need_snapshot = true;
    } else if (abs(_snapshot_num_table_lines - _num_table_lines.load()) > FLAGS_snapshot_diff_lines) {
        need_snapshot = true;
    } else if ((_applied_index - _snapshot_index) * _average_cost.load()
                > FLAGS_snapshot_log_exec_time_s * 1000 * 1000) {
        need_snapshot = true;
    }
    if (!need_snapshot) {
        return;
    }
    DB_WARNING("region_id: %ld do snapshot, snapshot_num_table_lines:%ld, num_table_lines:%ld "
            "snapshot_index:%ld, applied_index:%ld, snapshot_time_cost:%ld",
            _region_id, _snapshot_num_table_lines, _num_table_lines.load(),
            _snapshot_index, _applied_index, _snapshot_time_cost.get_time());
    done_guard.release();
    _node.snapshot(done);
}
void Region::on_snapshot_load_for_restart(braft::SnapshotReader* reader, 
        std::map<int64_t, std::string>& prepared_log_entrys) {
     //不管是哪种启动方式，prepared的但没有commit的日志都通过log_entry恢复, 所以prepared事务要回滚
    TimeCost time_cost;
    _txn_pool.clear();
    std::set<int64_t> prepared_log_indexs;
    if (Store::get_instance()->has_prepared_tran()) {
        //恢复prepared 但没有commited事务的log_index
        _writer->parse_txn_log_indexs(_region_id, prepared_log_indexs);
        for (auto log_index : prepared_log_indexs) {
            std::string log_entry;
            int ret = LogEntryReader::get_instance()->read_log_entry(_region_id, log_index, log_entry);
            if (ret < 0) {
                DB_FATAL("read prepared and not commited log entry fail, _region_id: %ld, log_index: %ld",
                            _region_id, log_index);
                continue;
            }
            prepared_log_entrys[log_index] = log_entry;
        }
    }
    DB_WARNING("success load snapshot, snapshot file not exist, "
                "region_id: %ld, prepared_log_size: %ld,"
                " prepared_log_entrys_size: %ld, time_cost: %ld",
                _region_id, prepared_log_indexs.size(), 
                prepared_log_entrys.size(), time_cost.get_time());
}

int Region::on_snapshot_load(braft::SnapshotReader* reader) {
    _time_cost.reset();
    TimeCost time_cost;
    DB_WARNING("region_id: %ld start to on snapshot load", _region_id);
    std::string data_sst_file = reader->get_path() + SNAPSHOT_DATA_FILE_WITH_SLASH;
    std::string meta_sst_file = reader->get_path() + SNAPSHOT_META_FILE_WITH_SLASH;
    boost::filesystem::path snapshot_meta_file = meta_sst_file;
    std::map<int64_t, std::string> prepared_log_entrys; 
    //本地重启， 不需要加载snasphot
    if (_restart || !boost::filesystem::exists(snapshot_meta_file)) {
        on_snapshot_load_for_restart(reader, prepared_log_entrys);
        _restart = false;            
    } else {
        int ret = Concurrency::get_instance()->snapshot_load_concurrency.increase_wait();
        ON_SCOPE_EXIT([](){
            Concurrency::get_instance()->snapshot_load_concurrency.decrease_broadcast();
        });
        DB_WARNING("snapshot load, region_id: %ld, wait_time:%ld, ret:%d", 
                    _region_id, time_cost.get_time(), ret);
        //不管是哪种启动方式，prepared的但没有commit的日志都通过log_entry恢复, 所以prepared事务要回滚
        _txn_pool.clear();
        //清空数据
        if (_region_info.version() != 0) {
            DB_WARNING("region_id: %ld, clear_data on_snapshot_load", _region_id);
            ret = clear_data();
            if (ret != 0) {
                DB_FATAL("clear data fail when on snapshot load, region_id: %ld", _region_id);
                return -1;
            }
        } else {
            DB_WARNING("region_id: %ld is new, no need clear_data. region_info: %s",
                        _region_id, _region_info.ShortDebugString().c_str());
        }
        // ingest sst
        ret = ingest_sst(data_sst_file, meta_sst_file);
        if (ret != 0) {
            DB_FATAL("ingest sst fail when on snapshot load, region_id: %ld", _region_id);
            return -1;
        }
        _writer->parse_txn_infos(_region_id, prepared_log_entrys);
        ret = _writer->clear_txn_infos(_region_id);
        if (ret != 0) {
            DB_FATAL("clear txn infos from rocksdb fail when on snapshot load, region_id: %ld", _region_id);
            return -1;
        }
        DB_WARNING("success load snapshot, ingest sst file, region_id: %ld", _region_id);
    }
    //回放prepared但没有commit的事务
    for (auto log_entry_pair : prepared_log_entrys) {
        int64_t log_index = log_entry_pair.first;
        pb::StoreReq store_req;
        if (!store_req.ParseFromString(log_entry_pair.second)) {
            DB_FATAL("parse prepared exec plan fail from log entry, region_id: %ld", _region_id);
            return -1; 
        }
        if (store_req.op_type() != pb::OP_PREPARE && store_req.op_type() != pb::OP_PREPARE_V2) {
            DB_FATAL("op_type is not prepared when parse log entry, region_id: %ld, op_type: %s, log_index: %ld", 
                    _region_id, pb::OpType_Name(store_req.op_type()).c_str(), log_index);
            return -1;
        }
        apply_txn_request(store_req, NULL, log_index, 0);
        DB_WARNING("recovered prepared but not committed transaction, region_id: %ld, log_index: %ld", 
                    _region_id, log_index);
    }
    //恢复内存中applied_index 和number_table_line
    _applied_index = _writer->read_applied_index(_region_id);
    _num_table_lines = _writer->read_num_table_lines(_region_id);
    pb::RegionInfo region_info;
    int ret = _writer->read_region_info(_region_id, region_info);
    if (ret < 0) {
        DB_FATAL("read region info fail when on snapshot load, region_id: %ld", _region_id);
        return -1;
    }
    if (_applied_index < 0) {
        DB_FATAL("recovery applied index or num table line fail,"
                    " _region_id: %ld, applied_index: %ld",
                    _region_id, _applied_index);
        return -1;
    }
    if (_num_table_lines < 0) {
        DB_FATAL("num table line fail,"
                    " _region_id: %ld, num_table_line: %ld",
                    _region_id, _num_table_lines.load());
        _writer->update_num_table_lines(_region_id, 0);
        _num_table_lines = 0;
    }
    region_info.set_can_add_peer(true);
    set_region(region_info);
    set_region_with_update_range(region_info);
    if (!compare_and_set_legal()) {
        DB_FATAL("region is not illegal, should be removed, region_id: %ld", _region_id);
        return -1;
    }
    _new_region_infos.clear();
    _snapshot_num_table_lines = _num_table_lines.load();
    _snapshot_index = _applied_index;
    _snapshot_time_cost.reset();
    DB_WARNING("snapshot load success, region_id: %ld, num_table_lines: %ld,"
                " applied_index: %ld, region_info: %s, cost:%ld",
                _region_id, _num_table_lines.load(), _applied_index, 
                region_info.ShortDebugString().c_str(), time_cost.get_time());
    return 0;
}

int Region::ingest_sst(const std::string& data_sst_file, const std::string& meta_sst_file) {
    int ret_data = 0;
    if (boost::filesystem::exists(boost::filesystem::path(data_sst_file))) {
        RegionControl::ingest_data_sst(data_sst_file, _region_id);
    } else {
        DB_WARNING("region_id: %ld is empty when on snapshot load", _region_id);
    }
    int ret_meta = RegionControl::ingest_meta_sst(meta_sst_file, _region_id);
    if (ret_data < 0 || ret_meta < 0) {
        DB_FATAL("ingest sst fail, region_id: %ld", _region_id);
        return -1;
    }
    return 0;
}

int Region::clear_data() {
    //删除preapred 但没有committed的事务
    _txn_pool.clear();
    RegionControl::remove_data(_region_id);
    RegionControl::remove_meta(_region_id);
    // 单线程执行compact
    DB_WARNING("region_id: %ld, clear_data do compact in queue", _region_id);
    compact_data_in_queue();
    return 0;
}

void Region::compact_data_in_queue() {
    _num_delete_lines = 0;
    RegionControl::compact_data_in_queue(_region_id);
}

void Region::reverse_merge() {
    if (_shutdown) {
        return;
    }
    _multi_thread_cond.increase();
    ON_SCOPE_EXIT([this]() {
        _multi_thread_cond.decrease_signal();
    });
    TimeCost cost;
    for (auto& pair : _reverse_index_map) {
        pair.second->reverse_merge_func(_resource->region_info);
    }
    //DB_WARNING("region_id: %ld reverse merge:%lu", _region_id, cost.get_time());
    SELF_TRACE("region_id: %ld reverse merge:%lu", _region_id, cost.get_time());
}

// dump the the tuples in this region in format {{k1:v1},{k2:v2},{k3,v3}...}
// used for debug
std::string Region::dump_hex() {
    auto data_cf = _rocksdb->get_data_handle();
    if (data_cf == nullptr) {
        DB_WARNING("get rocksdb data column family failed, region_id: %ld", _region_id);
        return "{}";
    }

    //encode pk fields
    //TableKey key;
    //key.append_i64(_region_id);
    rocksdb::ReadOptions read_option;
    //read_option.prefix_same_as_start = true;
    std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_option, RocksWrapper::DATA_CF));

    std::string dump_str("{");
    for (iter->SeekToFirst();
            iter->Valid() ; iter->Next()) {
        dump_str.append("\n{");
        dump_str.append(iter->key().ToString(true));
        dump_str.append(":");
        dump_str.append(iter->value().ToString(true));
        dump_str.append("},");
    }
    if (!iter->status().ok()) {
        DB_FATAL("Fail to iterate rocksdb, region_id: %ld", _region_id);
        return "{}";
    }
    if (dump_str[dump_str.size() - 1] == ',') {
        dump_str.pop_back();
    }
    dump_str.append("}");
    return dump_str;
}

//region处理split的入口方法
//该方法构造OP_SPLIT_START请求，收到请求后，记录分裂开始时的index, 迭代器等一系列状态
void Region::start_process_split(const pb::RegionSplitResponse& split_response,
                                 bool tail_split,
                                 const std::string& split_key) {
    if (_shutdown) {
        return;
    }
    _multi_thread_cond.increase();
    ON_SCOPE_EXIT([this]() {
        _multi_thread_cond.decrease_signal();
    });
    pb::RegionStatus expected_status = pb::IDLE; 
    if (!_region_control.compare_exchange_strong(expected_status, pb::DOING)) {
        DB_FATAL("split fail, region status is not idle when start split,"
                 " region_id: %ld, new_region_id: %ld",
                  _region_id, split_response.new_region_id());
        baikaldb::Store::get_instance()->sub_split_num();
        return;
    }
    _split_param.total_cost.reset(); 
    TimeCost new_region_cost;

    reset_split_status(); 
    _split_param.new_region_id = split_response.new_region_id();
    _split_param.instance = split_response.new_instance();
    if (!tail_split) {
        _split_param.split_key = split_key;
    }
    DB_WARNING("start split, region_id: %ld, version:%ld, new_region_id: %ld, "
            "split_key:%s, start_key:%s, end_key:%s, instance:%s",
                _region_id, _region_info.version(),
                _split_param.new_region_id,
                rocksdb::Slice(_split_param.split_key).ToString(true).c_str(),
                str_to_hex(_region_info.start_key()).c_str(), 
                str_to_hex(_region_info.end_key()).c_str(),
                _split_param.instance.c_str());
    
    //分裂的第一步修改为新建region
    ScopeProcStatus split_status(this);
    //构建initit_region请求，创建一个数据为空，peer只有一个，状态为DOING, version为0的空region 
    pb::InitRegion init_region_request;
    pb::RegionInfo* region_info = init_region_request.mutable_region_info();
    copy_region(region_info);
    region_info->set_region_id(_split_param.new_region_id);
    region_info->set_version(0);
    region_info->set_conf_version(1);
    region_info->set_start_key(_split_param.split_key);
    //region_info->set_end_key(_region_info.end_key());
    region_info->clear_peers();
    region_info->add_peers(_split_param.instance);
    region_info->set_leader(_split_param.instance);
    region_info->clear_used_size();
    region_info->set_log_index(0);
    region_info->set_status(pb::DOING);
    region_info->set_parent(_region_id);
    region_info->set_timestamp(time(NULL));
    region_info->set_can_add_peer(false);
    _new_region_info = *region_info; 
    //如果此参数设置为true，则认为此region是分裂出来的region
    //需要判断分裂多久之后有没有成功，没有成功则认为是失败，需要自己删除自己
    init_region_request.set_split_start(true);
    if (tail_split) {
        init_region_request.set_snapshot_times(2);
    } else {
        init_region_request.set_snapshot_times(1);
    }
    if (_region_control.init_region_to_store(_split_param.instance, init_region_request, NULL) != 0) {
        DB_FATAL("create new region fail, split fail, region_id: %ld, new_region_id: %ld, new_instance:%s",
                 _region_id, _split_param.new_region_id, _split_param.instance.c_str());
        return;
    }
    //等待新建的region选主
    //bthread_usleep(10000);
    DB_WARNING("init region success when region split, "
                "region_id: %ld, new_region_id: %ld, instance:%s, time_cost:%ld",
                _region_id, _split_param.new_region_id, 
                _split_param.instance.c_str(), new_region_cost.get_time());
    _split_param.new_region_cost = new_region_cost.get_time(); 
    int64_t average_cost = 50000;
    if (_average_cost.load() != 0) {
        average_cost = _average_cost.load();
    }
    _split_param.split_slow_down_cost = std::min(
            std::max(average_cost, (int64_t)50000), (int64_t)5000000);

    //如果是尾部分裂，不需要进行OP_START_SPLIT步骤
    if (tail_split) {
        split_status.reset(); 
        //split 开始计时
        _split_param.op_start_split_cost = 0;
        _split_param.op_snapshot_cost = 0;
        _split_param.write_sst_cost = 0;
        _split_param.send_first_log_entry_cost = 0;
        _split_param.send_second_log_entry_cost = 0;
        _split_param.tail_split = true;
        get_split_key_for_tail_split();
        return;
    }

    _split_param.tail_split = false;
    _split_param.op_start_split.reset();
    pb::StoreReq split_request;
    //开始分裂, new_iterator, get start index
    split_request.set_op_type(pb::OP_START_SPLIT);
    split_request.set_region_id(_region_id);
    split_request.set_region_version(_region_info.version());
    butil::IOBuf data;
    butil::IOBufAsZeroCopyOutputStream wrapper(&data);
    if (!split_request.SerializeToZeroCopyStream(&wrapper)) {
        //把状态切回来
        DB_FATAL("start split fail, serializeToString fail, region_id: %ld", _region_id);
        return;
    }
    split_status.reset();
    SplitClosure* c = new SplitClosure;
    //NewIteratorClosure* c = new NewIteratorClosure;
    c->next_step = [this]() {write_local_rocksdb_for_split();};
    c->region = this;
    c->new_instance = _split_param.instance;
    c->step_message = "op_start_split";
    c->op_type = pb::OP_START_SPLIT;
    c->split_region_id = _split_param.new_region_id;
    braft::Task task;
    task.data = &data;
    task.done = c;

    _node.apply(task);
    DB_WARNING("start first step for split, new iterator, get start index and term, region_id: %ld",
                _region_id);
}

void Region::get_split_key_for_tail_split() {
    ScopeProcStatus split_status(this);
    TimeCost time_cost;
    if (!is_leader()) {
        DB_FATAL("leader transfer when split, split fail, region_id: %ld", _region_id);
        return;
    }
    _split_param.no_write_time_cost.reset();
    //设置禁写 并且等待正在写入任务提交
    _disable_write_cond.increase();
    int64_t disable_write_wait = get_split_wait_time();
    int ret = _real_writing_cond.timed_wait(disable_write_wait);
    if (ret != 0) {
        DB_FATAL("_real_writing_cond wait timeout, region_id: %ld", _region_id);
        return;
    }
    DB_WARNING("start not allow write, region_id: %ld, time_cost:%ld", 
            _region_id, time_cost.get_time());
    _split_param.write_wait_cost = time_cost.get_time();
    
    _split_param.op_start_split_for_tail.reset();
    pb::StoreReq split_request;
    //尾分裂开始, get end index, get_split_key
    split_request.set_op_type(pb::OP_START_SPLIT_FOR_TAIL);
    split_request.set_region_id(_region_id);
    split_request.set_region_version(_region_info.version());
    butil::IOBuf data;
    butil::IOBufAsZeroCopyOutputStream wrapper(&data);
    if (!split_request.SerializeToZeroCopyStream(&wrapper)) {
        //把状态切回来
        DB_FATAL("start split fail for split, serializeToString fail, region_id: %ld", _region_id);
        return;
    }
    split_status.reset();
    SplitClosure* c = new SplitClosure;
    //NewIteratorClosure* c = new NewIteratorClosure;
    c->next_step = [this]() {send_complete_to_new_region_for_split();};
    c->region = this;
    c->new_instance = _split_param.instance;
    c->step_message = "op_start_split_for_tail";
    c->op_type = pb::OP_START_SPLIT_FOR_TAIL;
    c->split_region_id = _split_param.new_region_id;
    braft::Task task;
    task.data = &data;
    task.done = c;
    _node.apply(task);
    DB_WARNING("start first step for tail split, get split key and term, region_id: %ld, new_region_id: %ld",
                _region_id, _split_param.new_region_id);
}

//开始发送数据
void Region::write_local_rocksdb_for_split() {
    if (_shutdown) {
        return;
    }
    _multi_thread_cond.increase();
    ON_SCOPE_EXIT([this]() {
        _multi_thread_cond.decrease_signal();
    });
    _split_param.op_start_split_cost = _split_param.op_start_split.get_time();
    ScopeProcStatus split_status(this);

    _split_param.split_slow_down = true;
    TimeCost write_sst_time_cost;
    //uint64_t imageid = TableKey(_split_param.split_key).extract_u64(0);

    DB_WARNING("split param, region_id: %ld, term:%ld, split_start_index:%ld, split_end_index:%ld,"
                " new_region_id: %ld, split_key:%s, instance:%s",
                _region_id,
                _split_param.split_term,
                _split_param.split_start_index,
                _split_param.split_end_index,
                _split_param.new_region_id,
                rocksdb::Slice(_split_param.split_key).ToString(true).c_str(),
                //imageid,
                _split_param.instance.c_str());
    if (!is_leader()) {
        DB_FATAL("leader transfer when split, split fail, region_id: %ld", _region_id);
        return;
    }
    //write to new sst
    MutTableKey region_prefix;
    region_prefix.append_i64(_region_id);
    int64_t table_id = get_table_id();
    //MutTableKey table_prefix;
    //table_prefix.append_i64(_region_id).append_i64(table_id);
    std::atomic<int64_t> write_sst_lines(0);
    TableInfo table_info = _factory->get_table_info(table_id);
    _split_param.reduce_num_lines = 0;

    IndexInfo pk_info = _factory->get_index_info(table_id);

    ConcurrencyBthread copy_bth(5, &BTHREAD_ATTR_SMALL);
    for (int64_t index_id : table_info.indices) {
        auto read_and_write = [this, &pk_info, &write_sst_lines, 
                                index_id] () {
            MutTableKey table_prefix;
            table_prefix.append_i64(_region_id).append_i64(index_id);
            rocksdb::WriteOptions write_options;
            TimeCost cost;
            int64_t num_write_lines = 0;
            int64_t skip_write_lines = 0;
            rocksdb::ReadOptions read_options;
            read_options.prefix_same_as_start = true;
            read_options.total_order_seek = false;
            read_options.snapshot = _split_param.snapshot;
           
            IndexInfo index_info = _factory->get_index_info(index_id);
            std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _data_cf));
            if (index_info.type == pb::I_PRIMARY) {
                table_prefix.append_index(_split_param.split_key);
            }
            int64_t count = 0;
            for (iter->Seek(table_prefix.data()); iter->Valid(); iter->Next()) {
                ++count;
                if (count % 100 == 0 && (!is_leader() || _shutdown)) {
                    DB_WARNING("index %ld, old region_id: %ld write to new region_id: %ld failed, not leader",
                                index_id, _region_id, _split_param.new_region_id);
                    _split_param.err_code = -1;
                    return;
                }
                //int ret1 = 0; 
                rocksdb::Slice key_slice(iter->key());
                key_slice.remove_prefix(2 * sizeof(int64_t));
                if (index_info.type == pb::I_PRIMARY) {
                    // check end_key
                    // tail split need not send rocksdb
                    if (key_slice.compare(_region_info.end_key()) >= 0) {
                        break;
                    }
                } else if (index_info.type == pb::I_UNIQ || index_info.type == pb::I_KEY) {
                    if (!Transaction::fits_region_range(key_slice, iter->value(),
                            &_split_param.split_key, &_region_info.end_key(), 
                            pk_info, index_info)) {
                        // DB_WARNING("skip_key: %s, split: %s, end: %s index: %ld region: %ld", 
                        //     key_slice.ToString(true).c_str(), str_to_hex(_split_param.split_key).c_str(), str_to_hex(_region_info.end_key()).c_str(), index_id, _region_id);
                        skip_write_lines++;
                        continue;
                    }
                }
                MutTableKey key(iter->key());
                key.replace_i64(_split_param.new_region_id, 0);
                auto s = _rocksdb->put(write_options, _data_cf, key.data(), iter->value());
                if (!s.ok()) {
                    DB_FATAL("index %ld, old region_id: %ld write to new region_id: %ld failed, status: %s", 
                    index_id, _region_id, _split_param.new_region_id, s.ToString().c_str());
                    _split_param.err_code = -1;
                    return;
                }
                num_write_lines++;
            }
            write_sst_lines += num_write_lines;
            if (index_info.type == pb::I_PRIMARY) {
                _split_param.reduce_num_lines = num_write_lines;
            }
            DB_WARNING("scan index:%ld, cost=%ld, lines=%ld, skip:%ld, region_id: %ld", 
                        index_id, cost.get_time(), num_write_lines, skip_write_lines, _region_id);

        };
        copy_bth.run(read_and_write); 
    }
    // write all non-pk column values to cstore
    std::set<int32_t> pri_field_ids;
    for (auto& field_info : pk_info.fields) {
        pri_field_ids.insert(field_info.id);
    }
    for (auto& field_info : table_info.fields) {
        int32_t field_id = field_info.id;
        // skip pk fields
        if (pri_field_ids.count(field_id) != 0) {
            continue;
        }
        auto read_and_write_column = [this, &pk_info, &write_sst_lines,
                               field_id] () {
            MutTableKey table_prefix;
            table_prefix.append_i64(_region_id);
            table_prefix.append_i32(_region_info.table_id()).append_i32(field_id);
            rocksdb::WriteOptions write_options;
            TimeCost cost;
            int64_t num_write_lines = 0;
            int64_t skip_write_lines = 0;
            rocksdb::ReadOptions read_options;
            read_options.prefix_same_as_start = true;
            read_options.total_order_seek = false;
            read_options.snapshot = _split_param.snapshot;

            std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _data_cf));
            table_prefix.append_index(_split_param.split_key);
            int64_t count = 0;
            for (iter->Seek(table_prefix.data()); iter->Valid(); iter->Next()) {
                ++count;
                if (count % 100 == 0 && (!is_leader() || _shutdown)) {
                    DB_WARNING("field %d, old region_id: %ld write to new region_id: %ld failed, not leader",
                                field_id, _region_id, _split_param.new_region_id);
                    _split_param.err_code = -1;
                    return;
                }
                //int ret1 = 0;
                rocksdb::Slice key_slice(iter->key());
                key_slice.remove_prefix(2 * sizeof(int64_t));
                // check end_key
                // tail split need not send rocksdb
                if (key_slice.compare(_region_info.end_key()) >= 0) {
                    break;
                }
                MutTableKey key(iter->key());
                key.replace_i64(_split_param.new_region_id, 0);
                auto s = _rocksdb->put(write_options, _data_cf, key.data(), iter->value());
                if (!s.ok()) {
                    DB_FATAL("index %ld, old region_id: %ld write to new region_id: %ld failed, status: %s",
                    field_id, _region_id, _split_param.new_region_id, s.ToString().c_str());
                    _split_param.err_code = -1;
                    return;
                }
                num_write_lines++;
            }
            write_sst_lines += num_write_lines;
            DB_WARNING("scan filed:%d, cost=%ld, lines=%ld, skip:%ld, region_id: %ld",
                        field_id, cost.get_time(), num_write_lines, skip_write_lines, _region_id);

        };
        copy_bth.run(read_and_write_column);
    }
    copy_bth.join();
    if (_split_param.err_code != 0) {
        return;
    }
    DB_WARNING("region split success when write sst file to new region,"
              "region_id: %ld, new_region_id: %ld, instance:%s, write_sst_lines:%ld, time_cost:%ld",
              _region_id, 
              _split_param.new_region_id, 
              _split_param.instance.c_str(),
              write_sst_lines.load(),
              write_sst_time_cost.get_time());
    _split_param.write_sst_cost = write_sst_time_cost.get_time();
    SmartRegion new_region = Store::get_instance()->get_region(_split_param.new_region_id);
    if (!new_region) {
        DB_FATAL("new region is null, split fail. region_id: %ld, new_region_id:%ld, instance:%s",
                  _region_id, _split_param.new_region_id, _split_param.instance.c_str());
        return;
    }
    new_region->set_num_table_lines(_split_param.reduce_num_lines);

    // replay txn commands on new region by local write
    if (0 != new_region->replay_txn_for_recovery(_split_param.prepared_txn)) {
        DB_WARNING("replay_txn_for_recovery failed: region_id: %ld, new_region_id: %ld",
            _region_id, _split_param.new_region_id);
        return;
    }
    // replay txn commands on new region by network write
    // if (0 != replay_txn_for_recovery(_split_param.new_region_id, 
    //         _split_param.instance, "",
    //         _split_param.prepared_txn)) {
    //     DB_WARNING("replay_txn_for_recovery failed: region_id: %ld, new_region_id: %ld",
    //         _region_id, _split_param.new_region_id);
    //     return;
    // }

    //snapshot 之前发送5个NO_OP请求
    int ret = RpcSender::send_no_op_request(_split_param.instance, _split_param.new_region_id, 0);
    if (ret < 0) {
        DB_FATAL("new region request fail, send no_op reqeust,"
                 " region_id: %ld, new_reigon_id:%ld, instance:%s",
                _region_id, _split_param.new_region_id, 
                _split_param.instance.c_str());
        return;
    }
    //bthread_usleep(30 * 1000 * 1000);
    _split_param.op_snapshot.reset();
    //增加一步，做snapshot
    split_status.reset();
    SplitClosure* c = new SplitClosure;
    //NewIteratorClosure* c = new NewIteratorClosure;
    c->next_step = [this]() {send_log_entry_to_new_region_for_split();};
    c->region = this;
    c->new_instance = _split_param.instance;
    c->step_message = "snapshot";
    c->split_region_id = _split_param.new_region_id;
    new_region->_node.snapshot(c);
}

// replay txn commands on local peer
int Region::replay_txn_for_recovery(
        const std::unordered_map<uint64_t, pb::TransactionInfo>& prepared_txn) {

    for (auto& pair : prepared_txn) {
        uint64_t txn_id = pair.first;
        const pb::TransactionInfo& txn_info = pair.second;

        auto plan_size = txn_info.cache_plans_size();
        if (plan_size == 0) {
            DB_FATAL("TransactionError: invalid command type, region_id: %ld, txn_id: %lu", _region_id, txn_id);
            return -1;
        }
        for (auto& plan : txn_info.cache_plans()) {
            // construct prepare request to send to new_plan
            pb::StoreReq request;
            pb::StoreRes response;
            request.set_op_type(plan.op_type());
            for (auto& tuple : plan.tuples()) {
                request.add_tuples()->CopyFrom(tuple);
            }
            request.set_region_id(_region_id);
            request.set_region_version(get_version());
            request.mutable_plan()->CopyFrom(plan.plan());

            pb::TransactionInfo* txn = request.add_txn_infos();
            txn->set_txn_id(txn_id);
            txn->set_seq_id(plan.seq_id());

            dml(request, response, 0, 0);
            if (response.errcode() != pb::SUCCESS) {
                DB_FATAL("TransactionError: replay failed region_id: %ld, txn_id: %lu, seq_id: %d", 
                    _region_id, txn_id, plan.seq_id());
                return -1;
            }
        }
        DB_WARNING("replay txn on region success, region_id: %ld, txn_id: %lu", _region_id, txn_id);
    }
    return 0;
}

// replay txn commands on local or remote peer
// start_key is used when sending request to tail splitting new region,
// whose start_key is not set yet.
int Region::replay_txn_for_recovery(
        int64_t region_id,
        const std::string& instance,
        std::string start_key,
        const std::unordered_map<uint64_t, pb::TransactionInfo>& prepared_txn) {

    for (auto& pair : prepared_txn) {
        uint64_t txn_id = pair.first;
        const pb::TransactionInfo& txn_info = pair.second;
        auto plan_size = txn_info.cache_plans_size();
        if (plan_size == 0) {
            DB_FATAL("TransactionError: invalid command type, region_id: %ld, txn_id: %lu", _region_id, txn_id);
            return -1;
        }
        auto& prepare_plan = txn_info.cache_plans(plan_size - 1);
        if (prepare_plan.op_type() != pb::OP_PREPARE && prepare_plan.op_type() != pb::OP_PREPARE_V2) {
            DB_FATAL("TransactionError: invalid command type, region_id: %ld, txn_id: %lu, op_type: %d", 
                _region_id, txn_id, prepare_plan.op_type());
            return -1;
        }

        // construct prepare request to send to new_plan
        pb::StoreReq request;
        pb::StoreReq response;
        request.set_op_type(prepare_plan.op_type());
        for (auto& tuple : prepare_plan.tuples()) {
            request.add_tuples()->CopyFrom(tuple);
        }
        request.set_region_id(region_id);
        request.set_region_version(0);
        request.mutable_plan()->CopyFrom(prepare_plan.plan());
        if (start_key.size() > 0) {
            // send new start_key to new_region, only once
            request.set_start_key(start_key);
            start_key.clear();
        }
        pb::TransactionInfo* txn = request.add_txn_infos();
        txn->CopyFrom(txn_info);
        txn->mutable_cache_plans()->RemoveLast();
        int ret = RpcSender::send_query_method(request, instance, region_id);
        if (ret < 0) {
            DB_FATAL("TransactionError: new region request fail, region_id: %ld, new_region_id:%ld, instance:%s, txn_id: %lu",
                    _region_id, region_id, instance.c_str(), txn_id);
            return -1;
        }
        DB_WARNING("replay txn on region success, region_id: %ld, target_region_id: %ld, txn_id: %lu",
            _region_id, region_id, txn_id);
    }
    return 0;
}

void Region::send_log_entry_to_new_region_for_split() {
    if (_shutdown) {
        return;
    }
    _multi_thread_cond.increase();
    ON_SCOPE_EXIT([this]() {
        _multi_thread_cond.decrease_signal();
    });
    _split_param.op_snapshot_cost = _split_param.op_snapshot.get_time();
    ScopeProcStatus split_status(this);
    if (!is_leader()) {
        DB_FATAL("leader transfer when split, split fail, region_id: %ld, new_region_id: %ld", 
                  _region_id, _split_param.new_region_id);
        return;
    }

    TimeCost send_first_log_entry_time;
    //禁写之前先读取一段log_entry
    int64_t start_index = _split_param.split_start_index;
    std::vector<pb::StoreReq> requests;
    int64_t average_cost = 50000;
    if (_average_cost.load() != 0) {
        average_cost = _average_cost.load();
    }
    int ret = 0;
    int while_count = 0;
    int write_count_max = 1000000 / average_cost / 2;
    if (write_count_max == 0) {
        write_count_max = 1;
    }
    do {
        TimeCost time_cost_one_pass;
        ++while_count;
        int64_t end_index = 0;
        requests.clear();
        ret = get_log_entry_for_split(start_index, 
                                      _split_param.split_term,
                                      requests, 
                                      end_index);
        if (ret < 0) {
            DB_FATAL("get log split fail before not allow when region split, "
                      "region_id: %ld, new_region_id:%ld",
                       _region_id, _split_param.new_region_id);
            return;
        }
        int64_t send_request_count = 0;
        for (auto& request : requests) {
            ++send_request_count;
            if (send_request_count % 10 == 0 && !is_leader()) {
                DB_WARNING("leader stop when send log entry,"
                            " region_id: %ld, new_region_id:%ld, instance:%s",
                            _region_id, _split_param.new_region_id,
                            _split_param.instance.c_str());
                return;
            }
            
            int ret = RpcSender::send_query_method(request,
                                                  _split_param.instance, 
                                                  _split_param.new_region_id);
            if (ret < 0) {
                DB_FATAL("new region request fail, send log entry fail before not allow write,"
                         " region_id: %ld, new_region_id:%ld, instance:%s",
                        _region_id, _split_param.new_region_id, 
                        _split_param.instance.c_str());
                return;
            }
        }
        int64_t qps_send_log_entry = 1000000L * requests.size() / time_cost_one_pass.get_time();
        if (qps_send_log_entry < 2 * _qps.load() && qps_send_log_entry != 0) {
            _split_param.split_slow_down_cost = 
                _split_param.split_slow_down_cost * 2 * _qps.load() / qps_send_log_entry;
        }
        DB_WARNING("qps:%ld for send log entry, qps:%ld for region_id: %ld, split_slow_down:%ld",
                    qps_send_log_entry, _qps.load(), _region_id, _split_param.split_slow_down_cost);
        start_index = end_index + 1;
    } while ((_applied_index - start_index) > write_count_max && while_count < 10);
   
    DB_WARNING("send log entry before not allow success when split, "
                "region_id: %ld, new_region_id:%ld, instance:%s, time_cost:%ld, "
                "start_index:%ld, end_index:%ld, applied_index:%ld, while_count:%d, write_count_max: %d",
                _region_id, _split_param.new_region_id,
                _split_param.instance.c_str(), send_first_log_entry_time.get_time(),
                _split_param.split_start_index, start_index, _applied_index, while_count, write_count_max);

    _split_param.send_first_log_entry_cost = send_first_log_entry_time.get_time();
    
    _split_param.no_write_time_cost.reset();
    //设置禁写 并且等待正在写入任务提交
    TimeCost write_wait_cost;
    _disable_write_cond.increase();
    int64_t disable_write_wait = get_split_wait_time();
    usleep(100);
    ret = _real_writing_cond.timed_wait(disable_write_wait);
    if (ret != 0) {
        DB_FATAL("_real_writing_cond wait timeout, region_id: %ld", _region_id);
        return;
    }
    DB_WARNING("start not allow write, region_id: %ld, new_region_id: %ld, time_cost:%ld", 
                _region_id, _split_param.new_region_id, write_wait_cost.get_time());
    _split_param.write_wait_cost = write_wait_cost.get_time();

    //读取raft_log
    TimeCost send_second_log_entry_cost;
    requests.clear();
    ret = get_log_entry_for_split(start_index, 
                                  _split_param.split_term, 
                                  requests, 
                                  _split_param.split_end_index);
    if (ret < 0) {
        DB_FATAL("get log split fail when region split, region_id: %ld, new_region_id: %ld",
                   _region_id, _split_param.new_region_id);
        return;
    }
    int64_t send_request_count = 0;
    //发送请求到新region
    for (auto& request : requests) {
            ++send_request_count;
            if (send_request_count % 10 == 0 && !is_leader()) {
                DB_WARNING("leader stop when send log entry,"
                            " region_id: %ld, new_region_id:%ld, instance:%s",
                            _region_id, _split_param.new_region_id,
                            _split_param.instance.c_str());
                return;
            }
        int ret = RpcSender::send_query_method(request, 
                                              _split_param.instance,  
                                              _split_param.new_region_id);
        if (ret < 0) {
            DB_FATAL("new region request fail, send log entry fail, region_id: %ld, new_region_id:%ld, instance:%s",
                    _region_id, _split_param.new_region_id, _split_param.instance.c_str());
            return;
        }
    }
    DB_WARNING("region split success when send second log entry to new region,"
              "region_id: %ld, new_region_id:%ld, split_end_index:%ld, instance:%s, time_cost:%ld",
              _region_id, 
              _split_param.new_region_id, 
              _split_param.split_end_index,
              _split_param.instance.c_str(),
              send_second_log_entry_cost.get_time());
    _split_param.send_second_log_entry_cost = send_second_log_entry_cost.get_time();
    //下一步
    split_status.reset();
    _split_param.op_start_split_for_tail.reset();
    send_complete_to_new_region_for_split();
}

void Region::send_complete_to_new_region_for_split() {
    if (_shutdown) {
        return;
    }
    _multi_thread_cond.increase();
    ON_SCOPE_EXIT([this]() {
        _multi_thread_cond.decrease_signal();
    });
    _split_param.op_start_split_for_tail_cost = 
        _split_param.op_start_split_for_tail.get_time();
    ScopeProcStatus split_status(this); 
    if (!is_leader()) {
        DB_FATAL("leader transfer when split, split fail, region_id: %ld", _region_id);
        return;
    }

    if (_split_param.tail_split) {
        // replay txn commands on new region
        if (0 != replay_txn_for_recovery(
                _split_param.new_region_id, 
                _split_param.instance, 
                _split_param.split_key,
                _split_param.prepared_txn)) {
            DB_FATAL("replay_txn_for_recovery failed: region_id: %ld, new_region_id: %ld",
                _region_id, _split_param.new_region_id);
            start_thread_to_remove_region(_split_param.new_region_id, _split_param.instance);
            return;
        }
    }

    int retry_times = 0;
    TimeCost time_cost;
    pb::StoreRes response;
    //给新region发送更新完成请求，verison 0 -> 1, 状态由Spliting->Normal, start->end
    do {
        brpc::Channel channel;
        brpc::ChannelOptions channel_opt;
        channel_opt.timeout_ms = FLAGS_store_request_timeout;
        channel_opt.connect_timeout_ms = FLAGS_store_connect_timeout;
        if (channel.Init(_split_param.instance.c_str(), &channel_opt)) {
            DB_WARNING("send complete signal to new region fail when split,"
                        " region_id: %ld, new_region_id:%ld, instance:%s",
                      _region_id, _split_param.new_region_id, 
                      _split_param.instance.c_str());
            ++retry_times;
            continue;
        }
        brpc::Controller cntl;
        pb::StoreReq request;
        request.set_op_type(pb::OP_ADD_VERSION_FOR_SPLIT_REGION);
        request.set_start_key(_split_param.split_key);
        request.set_region_id(_split_param.new_region_id);
        request.set_region_version(0);
        //request.set_reduce_num_lines(_split_param.reduce_num_lines);
        butil::IOBuf data; 
        butil::IOBufAsZeroCopyOutputStream wrapper(&data);
        if (!request.SerializeToZeroCopyStream(&wrapper)) {
             DB_WARNING("send complete faila when serilize to iobuf for split fail,"
                        " region_id: %ld, request:%s",
                        _region_id, pb2json(request).c_str());
             ++retry_times;
             continue;
        }
        response.Clear();
        pb::StoreService_Stub(&channel).query(&cntl, &request, &response, NULL);
        if (cntl.Failed()) {
            DB_WARNING("region split fail when add version for split, err:%s",  cntl.ErrorText().c_str());
            ++retry_times;
            continue;
        }
        if (response.errcode() != pb::SUCCESS && response.errcode() != pb::VERSION_OLD) {
            DB_WARNING("region split fail when add version for split, "
                        "region_id: %ld, new_region_id:%ld, instance:%s, response:%s, must process!!!!",
                        _region_id, _split_param.new_region_id,
                        _split_param.instance.c_str(), pb2json(response).c_str());
            ++retry_times;
            continue;
        } else {
            break;
        }
    } while (retry_times < 3);
    
    if (retry_times >= 3) {
        //分离失败，回滚version 和 end_key
        DB_WARNING("region split fail when send complete signal to new version for split region,"
                    " region_id: %ld, new_region_id:%ld, instance:%s, need remove new region, time_cost:%ld",
                 _region_id, _split_param.new_region_id, _split_param.instance.c_str(), time_cost.get_time());
        start_thread_to_remove_region(_split_param.new_region_id, _split_param.instance);
        return;
    }

    if (!is_leader()) {
        DB_FATAL("leader transfer when split, split fail, region_id: %ld", _region_id);
        start_thread_to_remove_region(_split_param.new_region_id, _split_param.instance);
        return;
    }

    DB_WARNING("send split complete to new region success, begin add version for self"
                " region_id: %ld, time_cost:%ld", _region_id, time_cost.get_time());
    _split_param.send_complete_to_new_region_cost = time_cost.get_time();
    _split_param.op_add_version.reset();
    
    pb::StoreReq add_version_request;
    add_version_request.set_op_type(pb::OP_VALIDATE_AND_ADD_VERSION);
    add_version_request.set_region_id(_region_id);
    add_version_request.set_end_key(_split_param.split_key);
    add_version_request.set_split_term(_split_param.split_term);
    add_version_request.set_split_end_index(_split_param.split_end_index);
    add_version_request.set_region_version(_region_info.version() + 1);
    //add_version_request.set_reduce_num_lines(_split_param.reduce_num_lines);
    add_version_request.set_reduce_num_lines(response.affected_rows());
    for (auto& txn_info : response.txn_infos()) {
        add_version_request.add_txn_infos()->CopyFrom(txn_info);
    }
    
    _new_region_info.set_version(1);
    _new_region_info.set_start_key(_split_param.split_key);
    *(add_version_request.mutable_new_region_info()) = _new_region_info;
    
    butil::IOBuf data;
    butil::IOBufAsZeroCopyOutputStream wrapper(&data);
    if (!add_version_request.SerializeToZeroCopyStream(&wrapper)) {
        DB_FATAL("forth step for split fail, serializeToString fail, region_id: %ld", _region_id);  
        return;
    }
    split_status.reset();
    SplitClosure* c = new SplitClosure;
    c->region = this;
    c->next_step = [this]() {complete_split();}; 
    c->new_instance = _split_param.instance;
    c->step_message = "op_validate_and_add_version";
    c->op_type = pb::OP_VALIDATE_AND_ADD_VERSION;
    c->split_region_id = _split_param.new_region_id;
    braft::Task task; 
    task.data = &data; 
    task.done = c;
    _node.apply(task);
}

void Region::complete_split() {
    if (_shutdown) {
        return;
    }
    _multi_thread_cond.increase();
    ON_SCOPE_EXIT([this]() {
        _multi_thread_cond.decrease_signal();
    });
    _split_param.op_add_version_cost = _split_param.op_add_version.get_time();
    DB_WARNING("split complete, region_id: %ld new_region_id: %ld, total_cost:%ld, no_write_time_cost:%ld,"
               " new_region_cost:%ld, op_start_split_cost:%ld, op_start_split_for_tail_cost:%d, write_sst_cost:%ld,"
               " send_first_log_entry_cost:%ld, write_wait_cost:%ld, send_second_log_entry_cost:%ld,"
               " send_complete_to_new_region_cost:%ld, op_add_version_cost:%ld",
                _region_id, _split_param.new_region_id,
                _split_param.total_cost.get_time(), 
                _split_param.no_write_time_cost.get_time(),
                _split_param.new_region_cost,
                _split_param.op_start_split_cost,
                _split_param.op_start_split_for_tail_cost,
                _split_param.write_sst_cost,
                _split_param.send_first_log_entry_cost,
                _split_param.write_wait_cost,
                _split_param.send_second_log_entry_cost,
                _split_param.send_complete_to_new_region_cost,
                _split_param.op_add_version_cost);
    {
        ScopeProcStatus split_status(this);
    }
    
    //分离完成后立即发送一次心跳
    baikaldb::Store::get_instance()->send_heart_beat();
    
    //主动transfer_leader
    std::vector<braft::PeerId> peers;
    if (!_node.list_peers(&peers).ok()) {
        DB_FATAL("node list peer fail when add_peer, region_id: %ld", _region_id);
        return;
    }
    std::string new_leader = _address;
    int64_t max_applied_index = 0;
    for (auto& peer : peers) {
        std::string peer_string = butil::endpoint2str(peer.addr).c_str();
        if (peer_string == _address) {
            continue;
        }
        int64_t peer_applied_index = RpcSender::get_peer_applied_index(peer_string, _region_id);
        DB_WARNING("region_id: %ld, peer:%s, applied_index:%ld after split", 
                    _region_id, peer_string.c_str(), peer_applied_index);
        if (peer_applied_index > max_applied_index) {
            new_leader = peer_string;
            max_applied_index = peer_applied_index;
        }
    }
    if (new_leader == _address) {
        DB_WARNING("random new leader is equal with address, region_id: %ld", _region_id);
        return;
    }
    if ((_applied_index - max_applied_index) * _average_cost.load() > FLAGS_election_timeout_ms * 1000LL) {
        DB_WARNING("peer applied index: %ld is less than applied index: %ld, average_cost: %ld",
                    max_applied_index, _applied_index, _average_cost.load());
        return;
    }
    //分裂完成之后主动做一次transfer_leader, 机器随机选一个
    int ret = _node.transfer_leadership_to(new_leader);
    if (ret != 0) {
        DB_WARNING("node:%s %s transfer leader fail"
                    " original_leader_applied_index:%ld, new_leader_applied_index:%ld",
                        _node.node_id().group_id.c_str(),
                        _node.node_id().peer_id.to_string().c_str(),
                        _applied_index,
                        max_applied_index);
    } else {
        DB_WARNING("node:%s %s transfer leader success after split,"
                    " original_leader_applied_index:%ld, new_leader_applied_index:%ld",
                        _node.node_id().group_id.c_str(),
                        _node.node_id().peer_id.to_string().c_str(),
                        _applied_index,
                        max_applied_index); 
    }
}

int Region::get_log_entry_for_split(const int64_t split_start_index, 
                                    const int64_t expected_term,
                                    std::vector<pb::StoreReq>& requests, 
                                    int64_t& split_end_index) {
    TimeCost cost;
    int64_t start_index = split_start_index;
    MutTableKey log_data_key;
    log_data_key.append_i64(_region_id).append_u8(MyRaftLogStorage::LOG_DATA_IDENTIFY).append_i64(split_start_index);
    rocksdb::ReadOptions opt;
    opt.prefix_same_as_start = true;
    opt.total_order_seek = false;
    std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(opt, RocksWrapper::RAFT_LOG_CF));
    iter->Seek(log_data_key.data());
    for (; iter->Valid(); iter->Next()) {
        TableKey key(iter->key());
        int64_t log_index = key.extract_i64(sizeof(int64_t) + 1);
        if (log_index != start_index) {
            DB_FATAL("log index not continueous, start_index:%ld, log_index:%ld, region_id: %ld", 
                    start_index, log_index, _region_id);
            return -1;
        }
        rocksdb::Slice value_slice(iter->value());
        LogHead head(iter->value());
        value_slice.remove_prefix(MyRaftLogStorage::LOG_HEAD_SIZE); 
        if (head.term != expected_term) {
            DB_FATAL("term not equal to expect_term, term:%ld, expect_term:%ld, region_id: %ld", 
                      head.term, expected_term, _region_id);
            return -1;
        }
        if ((braft::EntryType)head.type != braft::ENTRY_TYPE_DATA) {
            DB_FATAL("log entry is not data, log_index:%ld, region_id: %ld", log_index, _region_id);
            continue;
        }
        pb::StoreReq store_req;
        if (!store_req.ParseFromArray(value_slice.data(), value_slice.size())) {
            DB_FATAL("Fail to parse request fail, split fail, region_id: %ld", _region_id);
            return -1;
        }
        // 加指令的时候这边要加上
        if (store_req.op_type() != pb::OP_INSERT
                && store_req.op_type() != pb::OP_DELETE
                && store_req.op_type() != pb::OP_UPDATE
                && store_req.op_type() != pb::OP_PREPARE
                && store_req.op_type() != pb::OP_PREPARE_V2
                && store_req.op_type() != pb::OP_ROLLBACK
                && store_req.op_type() != pb::OP_COMMIT) {
            DB_WARNING("unexpected store_req:%s, region_id: %ld", 
                     pb2json(store_req).c_str(), _region_id);
            return -1;
        }
        store_req.set_region_id(_split_param.new_region_id);
        store_req.set_region_version(0);
        requests.push_back(store_req);
        ++start_index;
    }
    split_end_index = start_index - 1;
    DB_WARNING("get_log_entry_for_split_time:%ld, region_id: %ld, split_end_index:%ld", 
            cost.get_time(), _region_id, split_end_index);
    return 0;
}

int Region::get_split_key(std::string& split_key) {
    int64_t tableid = _region_info.table_id();
    if (tableid < 0) {
        DB_WARNING("invalid tableid: %ld, region_id: %ld", 
                    tableid, _region_id);
        return -1;
    }
    rocksdb::ReadOptions read_options;
    read_options.total_order_seek = false;
    read_options.prefix_same_as_start = true;
    std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _data_cf));
    MutTableKey key;

    // 尾部插入优化, 非尾部插入可能会导致分裂两次
    //if (!_region_info.has_end_key() || _region_info.end_key() == "") {
    //    key.append_i64(_region_id).append_i64(tableid).append_u64(0xFFFFFFFFFFFFFFFF);
    //    iter->SeekForPrev(key.data());
    //    _split_param.split_key = std::string(iter->key().data() + 16, iter->key().size() - 16);
    //    split_key = _split_param.split_key;
    //    DB_WARNING("table_id:%ld, tail split, split_key:%s, region_id: %ld", 
    //        tableid, rocksdb::Slice(split_key).ToString(true).c_str(), _region_id);
    //    return 0;
    //}
    key.append_i64(_region_id).append_i64(tableid);

    int64_t cur_idx = 0;
    int64_t pk_cnt = _num_table_lines.load();
    int64_t random_skew_lines = butil::fast_rand() % (pk_cnt * FLAGS_skew / 100);
    
    int64_t lower_bound = pk_cnt / 2 - random_skew_lines;
    int64_t upper_bound = pk_cnt / 2 + random_skew_lines;

    std::string prev_key;
    std::string min_diff_key;
    uint32_t min_diff = UINT32_MAX;

    for (iter->Seek(key.data()); iter->Valid() 
            && iter->key().starts_with(key.data()); iter->Next()) {
        rocksdb::Slice pk_slice(iter->key());
        pk_slice.remove_prefix(2 * sizeof(int64_t));
        // check end_key
        if (pk_slice.compare(_region_info.end_key()) >= 0) {
            break;
        }

        cur_idx++;
        if (cur_idx < lower_bound) {
            continue;
        }
        if (cur_idx > upper_bound) {
            break;
        }
        if (prev_key.empty()) {
            prev_key = std::string(iter->key().data(), iter->key().size());
            continue;
        }
        uint32_t diff = rocksdb::Slice(prev_key).difference_offset(iter->key());
        if (diff < min_diff) {
            min_diff = diff;
            min_diff_key = iter->key().ToString();
        }
        if (min_diff == 2 * sizeof(int64_t)) {
            break;
        }
        prev_key = std::string(iter->key().data(), iter->key().size());
    }
    if (min_diff_key.size() < 16) {
        DB_WARNING("min_diff_key is: %d, %d, %d, %d, %d, %ld, %s, %s, %s",
             _num_table_lines.load(), iter->Valid(), cur_idx, lower_bound, upper_bound, min_diff_key.size(),
             min_diff_key.c_str(),
             iter->key().ToString(true).c_str(), 
             iter->value().ToString(true).c_str());
        return -1;
    }
    _split_param.split_key = min_diff_key.substr(16);
    split_key = _split_param.split_key;
    DB_WARNING("table_id:%ld, split_pos:%ld, split_key:%s, region_id: %ld", 
        tableid, cur_idx, rocksdb::Slice(split_key).ToString(true).c_str(), _region_id);
    return 0;
}

} // end of namespace
