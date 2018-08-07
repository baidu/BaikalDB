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
#include <sstream>
#include <fstream>
#include <boost/filesystem.hpp>
#include "table_key.h"
#include "raft_control.h"
#include "runtime_state.h"
#include "mem_row_descriptor.h"
#include "exec_node.h"
#include "table_record.h"
#include "my_raft_log_storage.h"
#include "raft_log_compaction_filter.h"
#include "split_compaction_filter.h"
#include "store.h"
#include "store_interact.hpp"

#include "rapidjson/rapidjson.h"
#include "rapidjson/reader.h"
#include "rapidjson/writer.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h" // for stringify JSON

namespace baikaldb {
DEFINE_int32(election_timeout_ms, 1000, "raft election timeout(ms)");
DEFINE_int32(skew, 5, "split skew, default : 45% - 55%");
DEFINE_int32(reverse_level2_len, 5000, "reverse index level2 length, default : 5000");
DEFINE_string(log_uri, "myraftlog://my_raft_log?id=", "raft log uri");
DEFINE_string(stable_uri, "local://./raft_data/stable", "raft stable path");
DEFINE_string(snapshot_uri, "local://./raft_data/snapshot", "raft snapshot path");
DEFINE_string(save_applied_index_path, "./save_applied_index", "applied index path when stop store");
DEFINE_int64(disable_write_wait_timeout_us, 1000 * 1000, 
        "disable write wait timeout(us) default 1s");
DEFINE_int64(real_writing_wait_timeout_us, 1000 * 1000, 
        "real writing wait timeout(us) default 1s");
DEFINE_int32(snapshot_interval_s, 600, "raft snapshot interval(s)");
DEFINE_int32(snapshot_load_num, 8, "snapshot load concurrency, default 8");
DEFINE_int32(snapshot_timed_wait, 120 * 1000 * 1000LL, "snapshot timed wait default 120S");
//DEFINE_int32(snapshot_trigger_lower_time_s, 600, "raft snapshot interval(s)");
//DEFINE_int32(snapshot_trigger_upper_time_s, 6000, "raft snapshot interval(s)");
DEFINE_int64(snapshot_diff_lines, 10000, "save_snapshot when num_table_lines diff");
DEFINE_int64(snapshot_diff_logs, 2000, "save_snapshot when log entries diff");
DEFINE_int64(snapshot_log_exec_time_s, 60, "save_snapshot when log entries apply time");
//分裂判断标准，如果3600S没有收到请求，则认为分裂失败
DEFINE_int64(split_duration_us, 3600 * 1000 * 1000LL, "split duration time : 3600s");
class ScopeProcStatus {
public:
    ScopeProcStatus(Region* region) : _region(region) {}
    ~ScopeProcStatus() {
        if (_region != NULL) {
            _region->reset_region_status();
            _region->reset_allow_write(); 
            _region->reset_split_status();
            baikaldb::Store::get_instance()->sub_split_num();
        }
    }
    void reset() {
        _region = NULL;
    }
private:
    Region* _region;
};

//const size_t  Region::REGION_MIN_KEY_SIZE = sizeof(int64_t) * 2 + sizeof(uint8_t);
const uint8_t Region::PRIMARY_INDEX_FLAG = 0x01;                                   
const uint8_t Region::SECOND_INDEX_FLAG = 0x02;
const int BATCH_COUNT = 1024;

struct DMLClosure : public braft::Closure {
    virtual void Run() {
        int64_t region_id = 0;
        butil::EndPoint leader;
        if (region != nullptr) {
            region->real_writing_decrease();
            region_id = region->get_region_id();
            leader = region->get_leader();
        }
        uint64_t log_id = 0;
        if (cntl->has_log_id()) { 
            log_id = cntl->log_id();
        }
        //const char* remote_side = butil::endpoint2str(cntl->remote_side()).c_str();
        if (!status().ok()) {
            response->set_errcode(pb::NOT_LEADER);
            response->set_leader(butil::endpoint2str(leader).c_str());
            response->set_errmsg("not leader");
            if (op_type == pb::OP_PREPARE && transaction != nullptr) {
                uint64_t txn_id = transaction->txn_id();
                if (region != nullptr) {
                    region->get_txn_pool().on_leader_stop_rollback(txn_id);
                }
            }
            DB_WARNING("region_id: %ld  status:%s ,leader:%s, log_id:%lu,",
                        region_id, 
                        status().error_cstr(),
                        butil::endpoint2str(leader).c_str(), 
                        log_id);
        }
        done->Run();
        if (region != nullptr) {
            region->update_average_cost(cost.get_time());
        }
        DB_NOTICE("dml log_id:%lu, type:%d, raft_total_cost:%ld, region_id: %ld, "
                    "qps:%ld, average_cost:%ld, num_prepared:%d remote_side:%s",
                    log_id, 
                    op_type, 
                    cost.get_time(), 
                    region_id, 
                    (region != nullptr)?region->get_qps():0, 
                    (region != nullptr)?region->get_average_cost():0, 
                    (region != nullptr)?region->num_prepared():0, 
                    remote_side.c_str());
        delete this;
    }

    brpc::Controller* cntl = nullptr;
    int op_type;
    pb::StoreRes* response = nullptr;
    google::protobuf::Closure* done = nullptr;
    Region* region = nullptr;
    Transaction* transaction = nullptr;
    TimeCost cost;
    std::string remote_side;
};

struct AddPeerClosure : public braft::Closure {
    virtual void Run() {  
        if (!status().ok()) {
            DB_WARNING("region add peer fail, new_instance:%s, status:%s, region_id: %ld, cost:%ld", 
                      new_instance.c_str(),
                      status().error_cstr(), 
                      region->get_region_id(),
                      cost.get_time());
            if (response) {
                response->set_errcode(pb::NOT_LEADER);
                response->set_leader(butil::endpoint2str(region->get_leader()).c_str());
                response->set_errmsg("not leader");
            }
            //region->send_remove_region_to_store(region->get_region_id(), new_instance);
        } else {
            DB_WARNING("region add peer success, region_id: %ld, cost:%ld", 
                        region->get_region_id(),
                        cost.get_time());
        }
        DB_WARNING("region status was reset, region_id: %ld", region->get_region_id());
        region->reset_region_status();
        if (done) {
            done->Run();
        }
        delete this;
    }
    Region* region;
    std::string new_instance;
    TimeCost cost;
    google::protobuf::Closure* done = NULL;
    pb::StoreRes* response = NULL;
};

struct SplitClosure : public braft::Closure {
    virtual void Run() {
        bool split_fail = false;
        ScopeProcStatus split_status(region);
        if (!status().ok()) { 
            DB_FATAL("split step(%s) fail, region_id: %ld status:%s, time_cost:%ld", 
                     step_message.c_str(), 
                     region->get_region_id(), 
                     status().error_cstr(), 
                     cost.get_time());
            split_fail = true;
        } /*else if (ret == -2) {
            DB_WARNING("has commit-pending txns: %d, region_id: %ld", 
                region->num_prepared(), region->get_region_id());
            split_fail = true;
            region->reset_prepare_slow_down_for_split();
        } */else if (ret < 0) {
            DB_FATAL("split step(%s) fail, region_id: %ld, cost:%ld", 
                      step_message.c_str(),
                      region->get_region_id(),
                      cost.get_time());
            split_fail = true;
        } else {
            split_status.reset();
            Bthread bth(&BTHREAD_ATTR_SMALL);
            bth.run(next_step);
            DB_WARNING("last step(%s) for split, start to next step, "
                        "region_id: %ld, cost:%ld", 
                        step_message.c_str(),
                        region->get_region_id(),
                        cost.get_time());
        }
        // OP_VALIDATE_AND_ADD_VERSION 这步失败了也不能自动删除new region
        // 防止出现flase negative，raft返回失败，实际成功
        // 如果真实失败，需要手工drop new region
        // todo 增加自动删除步骤，删除与分裂解耦
        if (split_fail && op_type != pb::OP_VALIDATE_AND_ADD_VERSION) {
            DB_WARNING("split fail, start remove region, old_region_id: %ld, split_region_id: %ld, new_instance:%s",
                        region->get_region_id(), split_region_id, new_instance.c_str());
            region->send_remove_region_to_store(split_region_id, new_instance);
        }
        delete this;
    }
    std::function<void()> next_step;
    Region* region;
    std::string new_instance;
    int64_t split_region_id;
    std::string step_message;
    pb::OpType op_type;
    int ret = 0;
    TimeCost cost;
};

struct ConvertToSyncClosure : public braft::Closure {
    ConvertToSyncClosure(BthreadCond& _sync_sign) : sync_sign(_sync_sign) {};
    virtual void Run() {
        if (!status().ok()) { 
            DB_FATAL("asyn step exec fail, status:%s, time_cost:%ld", 
                     status().error_cstr(), 
                     cost.get_time());
        } else {
            DB_WARNING("asyn step exec success, time_cost: %ld", cost.get_time());
        }
        sync_sign.decrease_signal();
        delete this;
    }
    BthreadCond& sync_sign;
    TimeCost cost;
};

int Region::init(bool write_db, int32_t snapshot_times) {
    _data_cf = _rocksdb->get_data_handle();
    _region_cf = _rocksdb->get_meta_info_handle();

    TimeCost time_cost;
    _resource.reset(new RegionResource);
    if (write_db) {
        TimeCost write_db_cost;
        if (_write_region_to_rocksdb(_region_info) != 0) {
            DB_FATAL("write region to rocksdb fail when init reigon, region_id: %ld", _region_id);
            return -1;
        }
        DB_WARNING("region_id: %ld _write_region_to_rocksdb:%ld", _region_id, write_db_cost.get_time());
    } else {
        _report_peer_info = true;
    }
    _resource->region_info = _region_info;
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
    _factory->update_region(_region_info);
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
                    segment_type = pb::SIMPLE;
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
    options.snapshot_interval_s = 0; // 禁止raft自动触发snapshot
    options.log_uri = FLAGS_log_uri + 
                       boost::lexical_cast<std::string>(_region_id);  
#if BAIDU_INTERNAL
    options.stable_uri = FLAGS_stable_uri + "/region_" + 
                           boost::lexical_cast<std::string>(_region_id);
#else
    options.raft_meta_uri = FLAGS_stable_uri + "/region_" + 
                           boost::lexical_cast<std::string>(_region_id);
#endif
    options.snapshot_uri = FLAGS_snapshot_uri + "/region_" + 
                                boost::lexical_cast<std::string>(_region_id);

    _txn_pool.init(_region_id, &_region_info);
    if (_node.init(options) != 0) {
        DB_FATAL("raft node init fail, region_id: %ld, region_info:%s", 
                 _region_id, pb2json(_region_info).c_str());
        _factory->delete_region_without_lock(_region_info); 
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
        sync_do_snapshot();
        --snapshot_times;
    }
    pb::RegionInfo& tmp_region = _resource->region_info;
    copy_region(&tmp_region);
    _init_success = true;
    DB_WARNING("region_id: %ld init success, region_info:%s, time_cost:%ld", 
                _region_id, tmp_region.ShortDebugString().c_str(), 
                time_cost.get_time());
    return 0;
}

void Region::sync_do_snapshot() {
    // send dummy requests (NO_OP type request)
    auto ret = send_no_op_request(Store::get_instance()->address(),
                       _region_id, 
                       _region_info.version());
    if (ret < 0) {
        DB_WARNING("send no op fail, region_id:%ld", _region_id);
    }
    BthreadCond sync_sign;
    sync_sign.increase();
    ConvertToSyncClosure* done = new ConvertToSyncClosure(sync_sign);
    _node.snapshot(done);
    sync_sign.wait();
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

void Region::raft_control(google::protobuf::RpcController* controller,
                          const pb::RaftControlRequest* request,
                          pb::RaftControlResponse* response,
                          google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint64_t log_id = 0;
    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(controller);
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    //每个region同时只能进行一个动作
    pb::RegionStatus expected_status = pb::IDLE;
    if (_status.compare_exchange_strong(expected_status, pb::DOING)) {
        DB_WARNING("region status to doning becase of raft control, region_id: %ld, request:%s",
                    _region_id, pb2json(*request).c_str());
        common_raft_control(controller, request, response, done_guard.release(), &_node);
    } else {
        DB_FATAL("region status is not idle, region_id: %ld, lod_id:%ld", _region_id, log_id);
        response->set_errcode(pb::REGION_ERROR_STATUS);
        response->set_errmsg("status not idle");
    }
}

bool Region::check_split_complete() {
    do {
        //bthread_usleep(FLAGS_split_duration_us);
        bthread_usleep(10 * 1000 * 1000);
        //30S没有收到请求， 并且version 也没有更新的话，分裂失败
        if (_removed) {
            DB_WARNING("region_id: %ld has been removed", _region_id);
            return true;
        }
        if (_time_cost.get_time() > FLAGS_split_duration_us) {
            if (compare_and_set_fail_for_split()) {
                DB_WARNING("split fail, set split success false, region_id: %ld",
                           _region_id);
                return false;
            } else {
                DB_WARNING("split success, region_id: %ld", _region_id);
                return true;
            }
        } else if (_region_info.version() > 0) {
            DB_WARNING("split success, region_id: %ld", _region_id);
            return true;
        } else {
            DB_WARNING("split not complete, need wait, region_id: %ld, cost_time: %ld", 
                _region_id, _time_cost.get_time());
            continue;
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
        uint64_t txn_id, Transaction*& txn, int64_t applied_index, int64_t term, uint64_t log_id) {
    if (request.op_type() == pb::OP_ROLLBACK || request.txn_infos_size() == 0) {
        return 0;
    }
    const pb::TransactionInfo& txn_info = request.txn_infos(0);
    int last_seq = (txn == nullptr)? 0 : txn->seq_id();
    DB_WARNING("TransactionNote: region_id: %ld, txn_id: %lu, op_type: %d, "
            "last_seq: %d, cache_plan_size: %d, log_id: %lu",
            _region_id, txn_id, request.op_type(), last_seq, txn_info.cache_plans_size(), log_id);

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
            DB_WARNING("TransactionNote: txn %ld_%lu:%d has been executed.", _region_id, txn_id, seq_id);
            continue;
        } else {
            DB_WARNING("TransactionNote: txn %ld_%lu:%d executed cached. op_type: %d",  
                _region_id, txn_id, seq_id, op_type);
        }
        
        // normally, cache plan should be execute successfully, because it has been executed 
        // on other peers.
        pb::StoreRes res;
        dml(request, op_type, plan, tuples, res, applied_index, term, seq_id);
        if (res.has_errcode() && res.errcode() != pb::SUCCESS) {
            response.set_errcode(res.errcode());
            response.set_errmsg(res.errmsg());
            DB_FATAL("TransactionError: txn: %ld_%lu:%d executed failed.", _region_id, txn_id, seq_id);
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
    DB_WARNING("region_id: %ld, txn_id: %lu, execute_cached success.", _region_id, txn_id);
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

    Transaction* txn = _txn_pool.get_txn(txn_id);
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
        return;
    }
    // seq_id within a transaction should be continuous regardless of failure or success
    int last_seq = (txn == nullptr)? 0 : txn->seq_id();
    if (txn_info.start_seq_id() > last_seq + 1) {
        char errmsg[100];
        snprintf(errmsg, sizeof(errmsg), "region_id: %ld, txn_id: %lu, txn_last_seq: %d, request_start_seq: %d", 
            _region_id, txn_id, last_seq, txn_info.start_seq_id());
        DB_WARNING("%s", errmsg);
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
        set_region(region_info_mem);
    }
    int ret = 0;
    if (op_type != pb::OP_PREPARE && last_seq < seq_id - 1) {
        //TODO: if last_seq_id = seq_id - 1, skip execute_cached_cmd
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
            DB_NOTICE("select type: %s, region_id: %ld, txn_id: %lu, seq_id: %d, "
                    "time_cost: %ld, log_id: %lu, remote_side: %s", 
                    pb::OpType_Name(request->op_type()).c_str(), _region_id, txn_id, seq_id, 
                    cost.get_time(), log_id, remote_side);
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
            DB_NOTICE("select type:%s, seq_id: %d, region_id: %ld, time_cost:%ld, log_id: %lu, remote_side: %s", 
                    pb::OpType_Name(request->op_type()).c_str(), 0, _region_id, cost.get_time(), log_id, remote_side);
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
    /*
    if (_shutdown) {
        response->set_errcode(pb::INTERNAL_ERROR);
        response->set_errmsg("node has been closed");
        DB_FATAL("region_id: %ld has been closed", _region_id);
        return;
    }*/
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
    if (validate_version(request, response) == false) {
        //add_version的第二次或者打三次重试，需要把num_table_line返回回去
        if (request->op_type() == pb::OP_ADD_VERSION_FOR_SPLIT_REGION) {
            response->set_affected_rows(_num_table_lines.load());
            response->clear_txn_infos();
            for (auto &pair : _prepared_txn_info) {
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
    if (request->op_type() == pb::OP_SELECT && request->region_version() > _region_info.version()) {
        response->set_errcode(pb::NOT_LEADER);
        response->set_leader(butil::endpoint2str(_node.leader_id().addr).c_str());
        response->set_errmsg("not leader");
        DB_WARNING("not leader, leader:%s, region_id: %ld, version:%ld, log_id:%lu, remote_side:%s",
                        butil::endpoint2str(_node.leader_id().addr).c_str(), 
                        _region_id, _region_info.version(), log_id, remote_side);
    }

    // int ret = 0;
    // TimeCost cost;
    switch (request->op_type()) {
        case pb::OP_SELECT:
        case pb::OP_INSERT:
        case pb::OP_DELETE:
        case pb::OP_UPDATE:
        case pb::OP_PREPARE:
        case pb::OP_COMMIT:
        case pb::OP_ROLLBACK:
        case pb::OP_TRUNCATE_TABLE: {
            // DB_WARNING("request: %s", request->DebugString().c_str());
            uint64_t txn_id = 0;
            if (request->txn_infos_size() > 0) {
                txn_id = request->txn_infos(0).txn_id();
            }
            // DB_WARNING("txn_id: %lu", txn_id);
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
    if (request.op_type() == pb::OP_PREPARE && optimize_1pc) {
        dml_1pc(request, request.op_type(), request.plan(), request.tuples(), 
            response, applied_index, term);
    } else {
        dml(request, request.op_type(), request.plan(), request.tuples(), 
            response, applied_index, term, seq_id);
    }
    return;
}

void Region::dml(const pb::StoreReq& request, 
        pb::OpType op_type,
        const pb::Plan& plan, 
        const RepeatedPtrField<pb::TupleDescriptor>& tuples, 
        pb::StoreRes& response, 
        int64_t applied_index, 
        int64_t term,
        int32_t seq_id) {
    //DB_WARNING("num_prepared:%d region_id: %ld", num_prepared(), _region_id);
    std::set<int> need_rollback_seq;
    if (!request.txn_infos_size() > 0) {
        response.set_errcode(pb::EXEC_FAIL);
        response.set_errmsg("request txn_info is empty");
        DB_FATAL("request txn_info is empty: %ld", _region_id);
        return;
    }
    const pb::TransactionInfo& txn_info = request.txn_infos(0);
    for (int rollback_seq : txn_info.need_rollback_seq()) {
        need_rollback_seq.insert(rollback_seq);
    }

    uint64_t txn_id = txn_info.txn_id();
    //int seq_id = txn_info.seq_id();
    auto txn = _txn_pool.get_txn(txn_id);
    int64_t txn_num_increase_rows = 0;

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
        if (op_type != pb::OP_PREPARE && op_type != pb::OP_COMMIT && op_type != pb::OP_ROLLBACK) {
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
    {
        BAIDU_SCOPED_LOCK(_ptr_mutex);
        state.set_resource(_resource);
    }

    state.set_reverse_index_map(_reverse_index_map);
    ExecNode* root = nullptr;
    ret = ExecNode::create_tree(plan, &root);
    if (ret < 0) {
        ExecNode::destory_tree(root);
        response.set_errcode(pb::EXEC_FAIL);
        response.set_errmsg("create plan fail");
        DB_FATAL("create plan fail, region_id: %ld, txn_id: %lu", _region_id, txn_id);
        return;
    }
    ret = root->open(&state);
    if (ret < 0) {
        root->close(&state);
        ExecNode::destory_tree(root);
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
            DB_WARNING("put txn cmd to cache: region_id: %ld, txn_id: %lu:%d", _region_id, txn_id, seq_id);
        }
    } else if (op_type != pb::OP_COMMIT && op_type != pb::OP_ROLLBACK) {
        // after commit or rollback, txn will be deleted
        root->close(&state);
        ExecNode::destory_tree(root);
        response.set_errcode(pb::EXEC_FAIL);
        response.set_errmsg("no txn found");
        DB_FATAL("no txn found: region_id: %ld, txn_id: %lu:%d, op_type: %d", 
            _region_id, txn_id, seq_id, op_type);
        return;
    }
    response.set_affected_rows(ret);
    root->close(&state);
    ExecNode::destory_tree(root);
    response.set_errcode(pb::SUCCESS);

    if (op_type == pb::OP_TRUNCATE_TABLE) {
        _num_table_lines = 0;
    } else if (op_type != pb::OP_COMMIT && op_type != pb::OP_ROLLBACK) {
        txn->num_increase_rows += state.num_increase_rows();
    } else if (op_type == pb::OP_COMMIT) {
        // 事务提交/回滚时更新num_table_line
        _num_table_lines += txn_num_increase_rows;
    }
    DB_NOTICE("dml type:%d, time_cost:%ld, region_id: %ld, txn_id: %lu, num_table_lines:%ld, "
              "affected_rows:%d, applied_index:%ld, term:%d, txn_num_rows:%ld", 
            op_type, cost.get_time(), _region_id, txn_id, _num_table_lines.load(), ret, 
            applied_index, term, txn_num_increase_rows);
}

void Region::dml_1pc(const pb::StoreReq& request, pb::OpType op_type,
        const pb::Plan& plan, const RepeatedPtrField<pb::TupleDescriptor>& tuples, 
        pb::StoreRes& response, int64_t applied_index, int64_t term) {
    //DB_WARNING("_num_table_lines:%ld region_id: %ld", _num_table_lines.load()
    //        , _region_id);
    int ret = 0;
    TimeCost cost;
    RuntimeState state;
    ret = state.init(request, plan, tuples, &_txn_pool);
    if (ret < 0) {
        response.set_errcode(pb::EXEC_FAIL);
        response.set_errmsg("RuntimeState init fail");
        DB_FATAL("RuntimeState init fail, region_id: %ld, applied_index: %ld", 
                    _region_id, applied_index);
        return;
    }
    {
        BAIDU_SCOPED_LOCK(_ptr_mutex);
        state.set_resource(_resource);
    }

    // for single-region autocommit and force-1pc cmd, create new txn.
    // for single-region multi-query, simply fetch the txn created before.
    bool is_new_txn = !(request.op_type() == pb::OP_PREPARE && request.txn_infos(0).optimize_1pc());
    if (is_new_txn) {
        state.create_txn_if_null(&_region_info);
    }
    bool commit_succ = false;
    ScopeGuard auto_rollback([&]() {
        if (!state.txn()) {
            return;
        }
        // rollback if not commit succ
        if (!commit_succ) {
            state.txn()->rollback();
        }
        // if txn in pool (new_txn == false), remove it from pool
        // else directly delete it
        if (!is_new_txn) {
            _txn_pool.remove_txn(state.txn_id);
        } else {
            delete state.txn();
        }
    });

    if (plan.nodes_size() > 0) {
        // for single-region autocommit and force-1pc cmd, exec the real dml cmd
        state.set_reverse_index_map(_reverse_index_map);
        ExecNode* root = nullptr;
        ret = ExecNode::create_tree(plan, &root);
        if (ret < 0) {
            ExecNode::destory_tree(root);
            response.set_errcode(pb::EXEC_FAIL);
            response.set_errmsg("create plan fail");
            DB_FATAL("create plan fail, region_id: %ld, txn_id: %lu:%d, applied_index: %ld", 
                _region_id, state.txn_id, state.seq_id, applied_index);
            return;
        }
        ret = root->open(&state);
        if (ret < 0) {
            root->close(&state);
            ExecNode::destory_tree(root);
            response.set_errcode(pb::EXEC_FAIL);
            if (state.error_code != ER_ERROR_FIRST) {
                response.set_mysql_errcode(state.error_code);
                response.set_errmsg(state.error_msg.str());
            } else {
                response.set_errmsg("plan open fail");
            }
            DB_FATAL("plan open fail, region_id: %ld, txn_id: %lu:%d, applied_index: %ld, error_code: %d", 
                _region_id, state.txn_id, state.seq_id, applied_index, state.error_code);
            return;
        }
        root->close(&state);
        ExecNode::destory_tree(root);
    }
    auto txn = state.txn();
    if (op_type != pb::OP_TRUNCATE_TABLE) {
        txn->num_increase_rows += state.num_increase_rows();
    } else {
        _num_table_lines = 0;
    }
    int64_t txn_num_increase_rows = txn->num_increase_rows;

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
        _num_table_lines += txn_num_increase_rows;
        response.set_affected_rows(ret);
        response.set_errcode(pb::SUCCESS);
        DB_NOTICE("dml type:%d, time_cost:%ld, region_id: %ld, txn_id: %lu, "
                    "num_table_lines:%ld, affected_rows:%d, "
                    "applied_index:%ld, term:%d, txn_num_rows:%ld", 
                    op_type, cost.get_time(), _region_id, state.txn_id, _num_table_lines.load(), 
                    ret, applied_index, term, txn_num_increase_rows);
    } else {
        response.set_errcode(pb::EXEC_FAIL);
        response.set_errmsg("txn commit failed.");
        DB_FATAL("txn commit failed, region_id: %ld, txn_id: %lu, applied_index: %ld", 
                    _region_id, state.txn_id, applied_index);
    }
    return;
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
    ret = state.init(request, plan, tuples, &_txn_pool);
    if (ret < 0) {
        response.set_errcode(pb::EXEC_FAIL);
        response.set_errmsg("RuntimeState init fail");
        DB_FATAL("RuntimeState init fail, region_id: %ld", _region_id);
        return;
    }
    {
        BAIDU_SCOPED_LOCK(_ptr_mutex);
        state.set_resource(_resource);
    }
    // double check, ensure resource match the req version
    if (validate_version(&request, &response) == false) {
        DB_WARNING("double check region version too old, region_id: %ld,"
                   " request_version:%ld, region_version:%ld",
                    _region_id, request.region_version(), _region_info.version());
        return;
    }
    bool is_new_txn = false;
    auto txn = state.txn();
    if (txn != nullptr) {
        std::set<int> need_rollback_seq;

        const pb::TransactionInfo& txn_info = request.txn_infos(0);
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
        txn = state.create_txn_if_null(&_region_info);
    }
    auto rollback_func = [is_new_txn](Transaction* txn) {
        if (is_new_txn) {
            txn->rollback();
            delete txn;
        }
    };
    std::unique_ptr<Transaction, decltype(rollback_func)> auto_rollback(txn, rollback_func);

    state.set_reverse_index_map(_reverse_index_map);
    MemRowDescriptor* mem_row_desc = state.mem_row_desc();
    ExecNode* root = nullptr;    
    ret = ExecNode::create_tree(plan, &root);
    if (ret < 0) {
        ExecNode::destory_tree(root);
        response.set_errcode(pb::EXEC_FAIL);
        response.set_errmsg("create plan fail");
        DB_FATAL("create plan fail, region_id: %ld", _region_id);
        return;
    }
    ret = root->open(&state);
    if (ret < 0) {
        root->close(&state);
        ExecNode::destory_tree(root);
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
            ExecNode::destory_tree(root);
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
    //DB_WARNING("region_id: %ld, rows:%d", _region_id, rows);
    root->close(&state);
    ExecNode::destory_tree(root);
    response.set_errcode(pb::SUCCESS);

    if (is_new_txn) {
        auto_rollback.release();
        txn->commit();
        delete txn;
    }
}

//leader收到metaServer传来的新增一个peer的请求
void Region::add_peer(const pb::AddPeer& add_peer) {
    if (_whether_legal_for_add_peer(add_peer, NULL) != 0) {
        return;
    }
    //这是按照在metaServer里add_peer的构造规则解析出来的newew——instance,
    //是解析new_instance的偷懒版本
    std::string new_instance = add_peer.new_peers(add_peer.new_peers_size() - 1);
    //在leader_send_init_region将状态置为doing, 在add_peer结束时将region状态置回来
    pb::RegionStatus expected_status = pb::IDLE;
    if (!_status.compare_exchange_strong(expected_status, pb::DOING)) {
        DB_WARNING("region status is not idle when add peer, region_id: %ld", _region_id);
        return;
    } else {
        DB_WARNING("region status to doning becase of add peer of heartbeat response, region_id: %ld",
                    _region_id);
    }
    if (_leader_send_init_region(new_instance, NULL) != 0) {
        reset_region_status(); 
        return;
    }
    _leader_add_peer(add_peer, new_instance, NULL, NULL);
}

void Region::add_peer(const pb::AddPeer* request,
                      pb::StoreRes* response,
                      google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (_whether_legal_for_add_peer(*request, response) != 0) {
        return;
    }
    std::set<std::string> old_peers;
    for (auto& old_peer : request->old_peers()) {
        old_peers.insert(old_peer);
    }
    std::string new_instance;
    for (auto& new_peer : request->new_peers()) {
        if (old_peers.find(new_peer) == old_peers.end()) {
            new_instance = new_peer;
            break;
        }
    }
    if (new_instance.empty()) {
        response->set_errcode(pb::INPUT_PARAM_ERROR);
        response->set_errmsg("no new peer");
        return;
    }
    pb::RegionStatus expected_status = pb::IDLE;
    if (!_status.compare_exchange_strong(expected_status, pb::DOING)) {
        DB_FATAL("region status is not idle when add peer, region_id: %ld", _region_id);
        response->set_errcode(pb::INTERNAL_ERROR);
        response->set_errmsg("new region fail");
        return;
    } else {
        DB_WARNING("region status to doning becase of add peer of heartbeat response, region_id: %ld",
                    _region_id);
    }
    if (_leader_send_init_region(new_instance, response) != 0) {
        reset_region_status(); 
        return;
    }
    _leader_add_peer(*request, new_instance, response, done_guard.release());
}
void Region::_leader_add_peer(const pb::AddPeer& add_peer, 
                              const std::string& new_instance,
                              pb::StoreRes* response,
                              google::protobuf::Closure* done) {
    //send add_peer to self
    braft::PeerId peer;
    AddPeerClosure* add_peer_done = new AddPeerClosure;
    add_peer_done->region = this;
    add_peer_done->done = done;
    add_peer_done->response = response;
    add_peer_done->new_instance = new_instance;
    std::vector<braft::PeerId> old_peers_vec;
    //done方法中会把region_status重置为IDLE
    for (auto& old_peer : add_peer.old_peers()) {
        braft::PeerId peer_mem;
        peer_mem.parse(old_peer); 
        old_peers_vec.push_back(peer_mem);
    }
    braft::PeerId add_peer_instance;
    add_peer_instance.parse(new_instance);
#ifdef BAIDU_INTERNAL
    _node.add_peer(old_peers_vec, add_peer_instance, add_peer_done);
#else
    _node.add_peer(add_peer_instance, add_peer_done);
#endif
}

//add_peer第一步
int Region::_leader_send_init_region(const std::string& new_instance, pb::StoreRes* response) {
    //send init_region to  new instance
    pb::InitRegion init_request;
    pb::RegionInfo* region_info = init_request.mutable_region_info();
    copy_region(region_info);
    region_info->set_log_index(0);
    region_info->clear_peers();//初始peer为空
    region_info->set_leader(_address);
    region_info->set_status(pb::IDLE);
    region_info->set_can_add_peer(true);
    init_request.set_snapshot_times(0);

    if (send_init_region_to_store(new_instance, init_request, response) != 0) {
        DB_FATAL("new init region fail when add peer, region_id: %ld, new_instance:%s",
                  _region_id, new_instance.c_str());
        return -1;
    }
    DB_WARNING("send init region to store:%s success, region_id: %ld", 
                new_instance.c_str(), _region_id);
    return 0;
}
int Region::_whether_legal_for_add_peer(const pb::AddPeer& add_peer, pb::StoreRes* response) {
    //判断收到请求的合法性，包括该peer是否是leader，list_peer值跟add_peer的old_peer是否相等，该peer的状态是否是IDEL
    if (!is_leader()) {
        DB_WARNING("node is not leader when add_peer, region_id: %ld", _region_id);
        if (response != NULL) {
            response->set_errcode(pb::NOT_LEADER);
            response->set_errmsg("not leader");
        }
        return -1;
    }
    if ((_region_info.has_can_add_peer() && !_region_info.can_add_peer())
            || _region_info.version() < 1) {
        DB_WARNING("region_id: %ld can not add peer, can_add_peer:%d, region_version:%ld",
                  _region_id, _region_info.can_add_peer(), _region_info.version());
        if (response != NULL) {
            response->set_errcode(pb::CANNOT_ADD_PEER);
            response->set_errmsg("can not add peer");
        }
        return -1;
    }
    std::vector<braft::PeerId> peers;
    std::vector<std::string> str_peers;
    if (!_node.list_peers(&peers).ok()) {
        DB_WARNING("node list peer fail when add_peer, region_id: %ld", _region_id);
        if (response != NULL) {
            response->set_errcode(pb::PEER_NOT_EQUAL);
            response->set_errmsg("list peer fail");
        }
        return -1;
    }
    if ((int)peers.size() != add_peer.old_peers_size()) {
        DB_WARNING("peer size is not equal when add peer, region_id: %ld", _region_id);
        if (response != NULL) {
            response->set_errcode(pb::PEER_NOT_EQUAL);
            response->set_errmsg("peer size not equal");
        }

        return -1;
    }
    for (auto& peer : peers) {
        str_peers.push_back(butil::endpoint2str(peer.addr).c_str());
    }
    for (auto& old_peer : add_peer.old_peers()) {
        auto iter = std::find(str_peers.begin(), str_peers.end(), old_peer);
        if (iter == str_peers.end()) {
            DB_FATAL("old_peer not equal to list peers, region_id: %ld, old_peer:%s",
                     _region_id, pb2json(add_peer).c_str());
            if (response != NULL) {
                response->set_errcode(pb::PEER_NOT_EQUAL);
                response->set_errmsg("peer not equal");
            }
            return -1;
        }
    }
    return 0;
}
int Region::transfer_leader(const pb::TransLeaderRequest& trans_leader_request) {
    std::string new_leader = trans_leader_request.new_leader();
    int64_t peer_applied_index = get_peer_applied_index(new_leader, _region_id);
    if ((_applied_index - peer_applied_index) * _average_cost.load() > FLAGS_election_timeout_ms * 1000LL) {
        DB_WARNING("peer applied index: %ld is less than applied index: %ld, average_cost: %ld",
                    peer_applied_index, _applied_index, _average_cost.load());
        return -1;
    }
    pb::RegionStatus expected_status = pb::IDLE;
    if (!_status.compare_exchange_strong(expected_status, pb::DOING)) {
        DB_FATAL("region status is not idle when transfer leader, region_id: %ld", _region_id);
        return -1;
    } else {
        DB_WARNING("region status to doning becase of transfer leader of heartbeat response,"
                    " region_id: %ld", _region_id);
    }
    int ret = _node.transfer_leadership_to(new_leader);
    reset_region_status(); 
    if (ret != 0) {
        DB_WARNING("node:%s %s transfer leader fail",
                        _node.node_id().group_id.c_str(),
                        _node.node_id().peer_id.to_string().c_str());
        return -1;
    }
    return 0; 
}
void Region::construct_heart_beat_request(pb::StoreHeartBeatRequest& request, bool need_peer_balance) {
    if (_shutdown) {
        return;
    }
    if (_region_info.version() == 0) {
        DB_WARNING("region info is splitting, region_id: %ld", _region_id);
        return;
    }
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
        leader_heart->set_status(_status.load());
        pb::RegionInfo* leader_region =  leader_heart->mutable_region();
        copy_region(leader_region);
        leader_region->set_status(_status.load());
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
        if (_write_region_to_rocksdb(region_info_mem) != 0) {
            DB_FATAL("update can add peer fail, region_id: %ld", _region_id); 
        } else {
            DB_WARNING("update can add peer success, region_id: %ld", _region_id);
        }
        _region_info.set_can_add_peer(true);
    }
}

void Region::on_apply(braft::Iterator& iter) {
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
            DB_WARNING("this log entry has been executed, log_index:%ld, applied_index:%ld, region_id: %ld",
                        iter.index(), _applied_index, _region_id);
            continue;
        }
        _applied_index = iter.index();
        int64_t term = iter.term();

        pb::StoreRes res;
        switch (op_type) {
            case pb::OP_PREPARE:
            case pb::OP_COMMIT:
            case pb::OP_ROLLBACK: {
                uint64_t txn_id = request.txn_infos_size() > 0 ? request.txn_infos(0).txn_id():0;
                if (txn_id == 0) {
                    if (done) {
                        ((DMLClosure*)done)->response->set_errcode(pb::INPUT_PARAM_ERROR);
                        ((DMLClosure*)done)->response->set_errmsg("txn control cmd out-of-txn");
                    }
                    break;
                }
                Transaction* txn = _txn_pool.get_txn(txn_id);
                int ret = 0;
                if (op_type == pb::OP_PREPARE) {
                    // for tail splitting new region replay txn
                    if (request.has_start_key() && !request.start_key().empty()) {
                        pb::RegionInfo region_info_mem;
                        copy_region(&region_info_mem);
                        region_info_mem.set_start_key(request.start_key());
                        set_region(region_info_mem);
                    }
                    ret = execute_cached_cmd(request, res, txn_id, txn, iter.index(), iter.term());
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
                    break;
                }
                // rollback is executed only if txn is not null (since we do not execute
                // cached cmd for rollback, the txn handler may be nullptr)
                if (op_type != pb::OP_ROLLBACK || txn != nullptr) {
                    dml(request, res, iter.index(), iter.term());
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
                break;
            }
            // 兼容老版本无事务功能时的log entry, 以及强制1PC的DML query(如灌数据时使用)
            case pb::OP_INSERT:
            case pb::OP_DELETE:
            case pb::OP_UPDATE: 
            case pb::OP_TRUNCATE_TABLE:
                //dml_compatible(request, res);
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
                //DB_NOTICE("dml, request:%s, response:%s", 
                //            request.ShortDebugString().c_str(), res.ShortDebugString().c_str());
                break;

            //split的各类请求传进的来的done类型各不相同，不走下边的if(done)逻辑，直接处理完成，然后continue
            case pb::OP_NONE: {
                if (done) {
                    ((DMLClosure*)done)->response->set_errcode(pb::SUCCESS);
                }
                DB_NOTICE("op_type=%s, region_id: %ld, applied_index:%ld, term:%d", 
                    pb::OpType_Name(request.op_type()).c_str(), _region_id, _applied_index, term);
                break;
            }
            case pb::OP_START_SPLIT: {
                //只有leader需要处理split请求，记录当前的log_index, term和迭代器
                if (done) {
                    _split_param.split_start_index = iter.index() + 1;
                    _split_param.split_term = iter.term();
                    _split_param.snapshot = _rocksdb->get_db()->GetSnapshot();
                    _txn_pool.get_prepared_txn_info(_split_param.prepared_txn, true);

                    ((SplitClosure*)done)->ret = 0;
                    if (_split_param.snapshot == nullptr) {
                        ((SplitClosure*)done)->ret = -1;
                    }
                    DB_WARNING("begin start split, region_id: %ld, split_start_index:%ld, term:%ld, num_prepared: %lu",
                                _region_id, iter.index() + 1, iter.term(), _split_param.prepared_txn.size());
                } else {
                    DB_WARNING("only leader process start split request, region_id: %ld", _region_id);
                }
                DB_NOTICE("op_type: %s, region_id: %ld, applied_index:%ld, term:%d", 
                    pb::OpType_Name(request.op_type()).c_str(), _region_id, _applied_index, term);
                break;
            }
            case pb::OP_START_SPLIT_FOR_TAIL: {
                if (done) {
                    _split_param.split_end_index = iter.index();
                    _split_param.split_term = iter.term();
                    int64_t tableid = _region_info.table_id();
                    if (tableid < 0) {
                        DB_WARNING("invalid tableid: %ld, region_id: %ld", 
                                    tableid, _region_id);
                        ((SplitClosure*)done)->ret = -1;
                        break;
                    }
                    rocksdb::ReadOptions read_options;
                    //read_options.total_order_seek = false;
                    //read_options.prefix_same_as_start = true;
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
                        break;
                    }
                    if (iter->key().size() <= 16 || !iter->key().starts_with(key.data().substr(0, 16))) {
                        DB_WARNING("get split key for tail split fail, region_id: %ld, data:%s, key_size:%ld",
                                    _region_id, rocksdb::Slice(iter->key().data()).ToString(true).c_str(), 
                                    iter->key().size());
                        ((SplitClosure*)done)->ret = -1;
                        break;
                    }
                    TableKey table_key(iter->key());
                    int64_t _region = table_key.extract_i64(0);
                    int64_t _table = table_key.extract_i64(sizeof(int64_t));
                    if (tableid != _table || _region_id != _region) {
                        DB_WARNING("get split key for tail split fail, region_id: %ld:%ld, tableid:%ld:%ld,"
                                "data:%s", _region_id, _region, tableid, _table, iter->key().data());
                        ((SplitClosure*)done)->ret = -1;
                        break;
                    }
                    _split_param.split_key = std::string(iter->key().data() + 16, iter->key().size() - 16) 
                                             + std::string(1, 0xFF);
                    DB_WARNING("table_id:%ld, tail split, split_key:%s, region_id: %ld, num_prepared: %lu",
                               tableid, rocksdb::Slice(_split_param.split_key).ToString(true).c_str(), 
                               _region_id, _split_param.prepared_txn.size());
                } else {
                    DB_WARNING("only leader process start split for tail, region_id: %ld", _region_id);
                }
                DB_NOTICE("op_type: %s, region_id: %ld, applied_index:%ld, term:%d", 
                    pb::OpType_Name(request.op_type()).c_str(), _region_id, _applied_index, term);
                break;
            }
            case pb::OP_VALIDATE_AND_ADD_VERSION: {
                if (request.split_term() != iter.term() || request.split_end_index() + 1 != iter.index()) {
                    DB_FATAL("split fail, region_id: %ld, new_region_id: %ld, split_term:%ld, "
                            "current_term:%ld, split_end_index:%ld, current_index:%ld, disable_write:%d",
                            _region_id, _split_param.new_region_id,
                            request.split_term(), iter.term(), request.split_end_index(), 
                            iter.index(), _disable_write_cond.count());
                    if (done) {
                        start_thread_to_remove_region(_split_param.new_region_id, _split_param.instance);
                        ((SplitClosure*)done)->ret = -1;
                    }
                    break;
                }
                //持久化数据到rocksdb
                pb::RegionInfo region_info_mem;
                copy_region(&region_info_mem);
                region_info_mem.set_version(request.region_version());
                region_info_mem.set_end_key(request.end_key());
                int ret = _write_region_to_rocksdb(region_info_mem);
                // 可能导致主从不一致
                if (ret != 0) {
                    DB_FATAL("add version when split fail, region_id: %ld", _region_id);
                    if (done) {
                        ((SplitClosure*)done)->ret = -1; 
                    }
                } else {
                    _new_region_infos.push_back(request.new_region_info());
                    if (done) {
                        ((SplitClosure*)done)->ret = 0;
                        DB_WARNING("add version %ld=>%ld for old region when split success, region_id: %ld",
                                    _region_info.version(), request.region_version(), _region_id);
                    }
                    DB_WARNING("update region info for all peer,"
                                " region_id: %ld, number_table_line:%ld, delta_number_table_line:%ld, "
                                "applied_index:%ld, term:%ld",
                                _region_id, _num_table_lines.load(), request.reduce_num_lines(),
                                _applied_index, term);
                    set_region(region_info_mem);
                    // region_info更新range，替换resource
                    // todo:shared_ptr本身不是原子的，需要加锁
                    std::shared_ptr<RegionResource> new_resource(new RegionResource);
                    *new_resource = *_resource;
                    new_resource->region_info = region_info_mem;
                    {
                        BAIDU_SCOPED_LOCK(_ptr_mutex);
                        _resource = new_resource;
                    }
                    _factory->update_region(region_info_mem);
                    _num_table_lines -= request.reduce_num_lines();
                    for (auto& txn_info : request.txn_infos()) {
                        _txn_pool.update_txn_num_rows_after_split(txn_info);
                    }
                    //compaction时候删掉多余的数据
                    SplitCompactionFilter::get_instance()->set_range_key(
                            _region_id,
                            _region_info.start_key(),
                            _region_info.end_key());
                } 
                DB_NOTICE("op_type: %s, region_id: %ld, applied_index:%ld, term:%d", 
                    pb::OpType_Name(request.op_type()).c_str(), _region_id, _applied_index, term);
                break;
            }
            case pb::OP_ADD_VERSION_FOR_SPLIT_REGION: {
                if (!compare_and_set_success_for_split()) {
                    DB_FATAL("split timeout, region was set split fail, region_id: %ld", _region_id);
                    if (done) {
                        ((DMLClosure*)done)->response->set_errcode(pb::SPLIT_TIMEOUT);
                        ((DMLClosure*)done)->response->set_errmsg("split timeout");
                    }
                    break;
                }
                pb::RegionInfo region_info_mem;
                copy_region(&region_info_mem);
                region_info_mem.set_version(1);
                region_info_mem.set_status(pb::IDLE);
                region_info_mem.set_start_key(request.start_key());
                int ret = _write_region_to_rocksdb(region_info_mem);
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
                    reset_region_status();
                    set_region(region_info_mem);
                    // region_info更新range，替换resource
                    std::shared_ptr<RegionResource> new_resource(new RegionResource);
                    *new_resource = *_resource;
                    new_resource->region_info = region_info_mem;
                    {
                        BAIDU_SCOPED_LOCK(_ptr_mutex);
                        _resource = new_resource;
                    }
                    _factory->update_region(region_info_mem);
                    //_num_table_lines += request.reduce_num_lines();
                    //SplitCompactionFilter::get_instance()->set_end_key(
                    //    _region_id, _region_info.start_key(), _region_info.end_key());
                    get_prepared_txn_info();
                    if (done) {
                        ((DMLClosure*)done)->response->set_errcode(pb::SUCCESS);
                        ((DMLClosure*)done)->response->set_errmsg("success");
                        ((DMLClosure*)done)->response->set_affected_rows(_num_table_lines.load());
                        ((DMLClosure*)done)->response->clear_txn_infos();
                        for (auto &pair : _prepared_txn_info) {
                            auto txn_info = ((DMLClosure*)done)->response->add_txn_infos();
                            txn_info->CopyFrom(pair.second);
                        }
                    }
                }
                DB_NOTICE("op_type: %s, region_id: %ld, applied_index:%ld, term:%d", 
                    pb::OpType_Name(request.op_type()).c_str(), _region_id, _applied_index, term);
                break;
            }
            default:
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
    //_status.store(pb::IDLE);
}

void Region::on_leader_stop(const butil::Status& status) {   
    DB_WARNING("leader stop, region_id: %ld, error_code:%d, error_des:%s",
                _region_id, status.error_code(), status.error_cstr());
    _is_leader.store(false);
    _txn_pool.on_leader_stop_rollback();
    //_status.store(pb::IDLE);
}

void Region::on_error(const ::braft::Error& e) {
    DB_FATAL("raft node meet error, region_id: %ld, error_type:%d, error_desc:%s",
                _region_id, e.type(), e.status().error_cstr());
}

void Region::on_configuration_committed(const::braft::Configuration& conf) {
    std::vector<braft::PeerId> peers;
    conf.list_peers(&peers);
    std::string conf_str;
    pb::RegionInfo tmp_region;
    copy_region(&tmp_region);
    tmp_region.clear_peers();
    for (auto& peer : peers) {
        if (butil::endpoint2str(peer.addr).c_str() == _address)  {
            DB_WARNING("region_id: %ld can report peer info", _region_id);
            _report_peer_info = true;
        } 
        tmp_region.add_peers(butil::endpoint2str(peer.addr).c_str());
        conf_str += std::string(butil::endpoint2str(peer.addr).c_str()) + ",";    
    }
    tmp_region.set_leader(butil::endpoint2str(_node.leader_id().addr).c_str());
    set_region(tmp_region);
    DB_WARNING("region_id: %ld, configurantion:%s leader:%s",
                _region_id, conf_str.c_str(), 
                butil::endpoint2str(_node.leader_id().addr).c_str());
}

void Region::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
    DB_WARNING("start on shnapshot save, region_id: %ld", _region_id);
    //创建snapshot
    rocksdb::ReadOptions read_options;
    //read_options.prefix_same_as_start = false;
    read_options.total_order_seek = true;
    auto data_iter = _rocksdb->new_iterator(read_options, _data_cf);
    MutTableKey region_prefix;
    region_prefix.append_i64(_region_id); 
    data_iter->Seek(region_prefix.data());
    // prepare_slow_down is reset after get the rocksdb iterator.
    // reset_prepare_slow_down_for_snapshot();
    // DB_WARNING("reset_prepare_slow_down: %d, region_id: %ld", 
    //     _prepare_slow_down_for_snapshot, _region_id);

    rapidjson::Document root;
    root.SetObject();
    rapidjson::Document::AllocatorType& alloc = root.GetAllocator();
    root.AddMember("num_table_lines", _num_table_lines.load(), alloc);
    root.AddMember("applied_index", _applied_index, alloc);

    std::unordered_map<uint64_t, pb::TransactionInfo> prepared_txn;
    _txn_pool.get_prepared_txn_info(prepared_txn, false);
    save_prepared_txn(prepared_txn, root);

    _snapshot_num_table_lines = _num_table_lines.load();
    _snapshot_index = _applied_index;
    _snapshot_time_cost.reset();
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> json_writer(buffer);
    root.Accept(json_writer);
    std::string extra(buffer.GetString());
   
    //snapshot时保存region_info信息
    pb::RegionInfo snapshot_region_info;
    copy_region(&snapshot_region_info);
    DB_WARNING("num_table_lines:%ld, applied_index:%ld, region_info:%s when save snapshot",
                 _num_table_lines.load(), _applied_index, pb2json(snapshot_region_info).c_str());
    
    Bthread bth(&BTHREAD_ATTR_SMALL);
    std::function<void()> save_snapshot_function = 
        [this, done, data_iter, writer, region_prefix, extra, snapshot_region_info]() {
            save_snapshot(done, data_iter, writer, region_prefix, extra, snapshot_region_info);
        };
    bth.run(save_snapshot_function);
}

int Region::load_applied_index() {
    std::string applied_path = FLAGS_save_applied_index_path + "/region_" + std::to_string(_region_id);
    std::string applied_file = applied_path + "/applied_index.json";
    if (!boost::filesystem::exists(boost::filesystem::path(applied_file))) {
        DB_WARNING("applied file:%s is not exist", applied_file.c_str());
        return -1;
    }
    //读取文件
    std::ifstream applied_fs(applied_file);
    std::string extra((std::istreambuf_iterator<char>(applied_fs)),                        
            std::istreambuf_iterator<char>());
    rapidjson::Document root;                                                            
    try {
        root.Parse<0>(extra.c_str());                                                    
        if (root.HasParseError()) {                                                      
            rapidjson::ParseErrorCode code = root.GetParseError();                       
            DB_WARNING("parse extra file error [code:%d][%s], region_id: %ld",            
                        code, extra.c_str(), _region_id);                                
            boost::filesystem::path remove_path(applied_path);
            boost::filesystem::remove_all(remove_path);
            return -1;
        }                                                                                
    } catch (...) {                
        DB_WARNING("parse extra file erro [%s], region_id: %ld",                         
                    extra.c_str(), _region_id);                                          
        boost::filesystem::path remove_path(applied_path);
        boost::filesystem::remove_all(remove_path);
        return -1;                  
    }
    auto json_iter = root.FindMember("num_table_lines");                                 
    if (json_iter != root.MemberEnd()) {
        _num_table_lines = json_iter->value.GetInt64();
        DB_WARNING("number table lines:%ld when load snapshot, region_id: %ld",           
                    _num_table_lines.load(), _region_id);
    } else {
        DB_WARNING("get num_table_lines fail, region_id: %ld", _region_id);               
        boost::filesystem::path remove_path(applied_path);
        boost::filesystem::remove_all(remove_path);
        return -1;
    }

    json_iter = root.FindMember("init_applied_index"); 
    if (json_iter != root.MemberEnd()) {
        _applied_index = json_iter->value.GetInt64();
        DB_WARNING("_applied_index:%ld when load snapshot, region_id: %ld",           
                    _applied_index, _region_id);
    } else {                                                                             
        DB_WARNING("get init_applied_index fail, region_id: %ld", _region_id);
        boost::filesystem::path remove_path(applied_path);
        boost::filesystem::remove_all(remove_path);
        return -1;
    }
    if (0 != load_prepared_txn(_prepared_txn_info, root)) {
        DB_WARNING("TransactionError: load_prepared_txn failed, region_id: %ld", _region_id);
        return -1;
    }

    //第一次加载完成之后要删除目录
    boost::filesystem::path remove_path(applied_path);
    boost::filesystem::remove_all(remove_path);
    DB_WARNING("_applied_index:%ld, num_table_lines:%ld, region_id: %ld",
                _applied_index, _num_table_lines.load(), _region_id);
    if (_applied_index == 0) {
        DB_WARNING("applied_index:%ld is 0", _applied_index);
        return -1;
    }
    return 0; 
}

int Region::on_snapshot_load(braft::SnapshotReader* reader) {
    //DB_WARNING("start on snapshot load, region_id: %ld", _region_id);
    TimeCost time_cost;
    int ret = Store::get_instance()->snapshot_load_currency.increase_timed_wait(FLAGS_snapshot_timed_wait);
    std::shared_ptr<BthreadCond> auto_decrease(&(Store::get_instance()->snapshot_load_currency), 
                                            [](BthreadCond* cond) { cond->decrease_broadcast();});
    DB_WARNING("snapshot load, region_id: %ld, wait_time:%ld, ret:%d", 
                _region_id, time_cost.get_time(), ret);
    //todo 恢复文件
    std::string data_sst_file = reader->get_path();
    data_sst_file.append("/snap_region_" + std::to_string(get_region_id()));
    std::string extra_file_name = data_sst_file + ".extra.json";
    std::string region_sst_file = data_sst_file + ".region_info.sst";

    std::ifstream extra_fs(extra_file_name);
    std::string extra((std::istreambuf_iterator<char>(extra_fs)),
            std::istreambuf_iterator<char>());
    rapidjson::Document root;
    try {
        root.Parse<0>(extra.c_str());
        if (root.HasParseError()) {
            rapidjson::ParseErrorCode code = root.GetParseError();
            DB_WARNING("parse extra file error [code:%d][%s], region_id: %ld", 
                        code, extra.c_str(), _region_id);
            return -1;
        }
    } catch (...) {
        DB_WARNING("parse extra file error [%s], region_id: %ld",
                    extra.c_str(), _region_id);
        return -1;
    } 
    auto json_iter = root.FindMember("num_table_lines");
    if (json_iter != root.MemberEnd()) {
        _num_table_lines = json_iter->value.GetInt64();
        DB_WARNING("number table lines:%ld when load snapshot, region_id: %ld",
                    _num_table_lines.load(), _region_id);
    } else {
        DB_WARNING("get num_table_lines fail, region_id: %ld", _region_id);
        return -1;
    }
    json_iter = root.FindMember("applied_index");
    if (json_iter != root.MemberEnd()) {
        _applied_index = json_iter->value.GetInt64();
        DB_WARNING("applied_index:%ld when load snapshot, region_id: %ld",
                    _applied_index, _region_id);
    } else {
        DB_WARNING("get applied_index fail, region_id: %ld", _region_id);
    }
    //优先读extra_json，获取snapshot的信息
    //如果启动不走snapshot会覆盖_num_table_lines和_applied_index
    _snapshot_num_table_lines = _num_table_lines.load();
    _snapshot_index = _applied_index;
    _snapshot_time_cost.reset();

    ret = load_applied_index();
    if (_region_info.can_add_peer() == false) {
        _region_info.set_can_add_peer(true);
    }
    if (ret == 0) {
        auto recovered_txns = baikaldb::Store::get_instance()->get_recovered_txns();
        ret = _txn_pool.on_shutdown_recovery(recovered_txns, _prepared_txn_info);
        if (ret == 0) {
            //不需要读取snapshot
            DB_WARNING("recover data through rocksdb, region_id: %ld", _region_id);
            return 0;
        }
    }
    
    if (_need_clear_data) {
        ret = clear_data();
    } else {
        ret = 0;
        _need_clear_data = true;
    }
    if (ret != 0) {
        DB_FATAL("clear data fail when on snapshot load, region_id: %ld", _region_id);
        return -1;
    }

    boost::filesystem::path data_sst_boost_path = data_sst_file;
    if (boost::filesystem::exists(data_sst_boost_path) &&
        boost::filesystem::file_size(data_sst_boost_path) > 0) {
        rocksdb::IngestExternalFileOptions ifo;
        auto res = _rocksdb->ingest_external_file(_data_cf, {data_sst_file}, ifo);
        if (!res.ok()) {
            DB_WARNING("Error while adding file %s, Error %s, region_id: %ld",
                    data_sst_file.c_str(), res.ToString().c_str(), _region_id);
            return -1;
        }
    } else {
        DB_WARNING("snapshot load no entries, num_table_lines:%ld, region_id: %ld", 
            _num_table_lines.load(), _region_id);
    }

    boost::filesystem::path region_sst_boost_path = region_sst_file;
    if (boost::filesystem::exists(region_sst_boost_path)) {
        rocksdb::IngestExternalFileOptions ifo;
        auto res = _rocksdb->ingest_external_file(_region_cf, {region_sst_file}, ifo);
        if (!res.ok()) {
            DB_WARNING("Error while adding file %s, Error %s, region_id: %ld",
                    region_sst_file.c_str(), res.ToString().c_str(), _region_id);
            return -1;
        }
        //把region_info读进来，更新信息
        int64_t region_id = _region_id;
        std::string region_key = Store::SCHEMA_IDENTIFY;
        region_key += Store::REGION_SCHEMA_IDENTIFY;
        region_key.append((char*)&region_id, sizeof(int64_t));
        std::string region_value;
        res = _rocksdb->get(rocksdb::ReadOptions(), _region_cf, region_key, &region_value);
        if (!res.ok()) {
            DB_WARNING("read region info from rocksdb fail, Error %s, region_id: %ld",
                        res.ToString().c_str(), _region_id);
            return -1;
        }
        pb::RegionInfo region_info;
        if (!region_info.ParseFromString(region_value)) {
            DB_FATAL("parse from pb fail when load region snapshot, key:%s",
                        region_value.c_str()); 
            return -1;
        }
        if (region_info.can_add_peer() == false) {
            region_info.set_can_add_peer(true);
            _write_region_to_rocksdb(region_info);
        }
        DB_WARNING("region_info start_key:%s, end_key:%s, region_info:%s when load snapshot", 
                    str_to_hex(region_info.start_key()).c_str(), 
                    str_to_hex(region_info.end_key()).c_str(),
                    pb2json(region_info).c_str());
        set_region(region_info);
        _factory->force_update_region(region_info);
    } else {
        DB_FATAL("snapshot has no data region info sst, region_id: %ld", _region_id);
    }
    
    if (0 != load_prepared_txn(_prepared_txn_info, root)) {
        DB_WARNING("TransactionError: load_prepared_txn failed, region_id: %ld", _region_id);
        return -1;
    }
    if (0 != replay_txn_for_recovery(_prepared_txn_info)) { 
        DB_WARNING("TransactionError: on_crash_recovery failed, region_id: %ld", _region_id);
        return -1;
    }

    _new_region_infos.clear();
    DB_WARNING("snapshot load success, region_id: %ld, num_table_lines:%ld, snapshot_path:%s, cost:%ld",
        _region_id, _num_table_lines.load(), data_sst_file.c_str(), time_cost.get_time());
    return 0;
}

int Region::clear_data() {
    rocksdb::WriteOptions options;
    //options.sync = true;
    //options.disableWAL = true;

    MutTableKey start_key;
    MutTableKey end_key;
    start_key.append_i64(_region_id);

    end_key.append_i64(_region_id);
    end_key.append_u64(0xFFFFFFFFFFFFFFFF);

    auto data_cf = _rocksdb->get_data_handle();
    if (data_cf == nullptr) {
        DB_WARNING("get rocksdb data column family failed, region_id: %ld", _region_id);
        return -1;
    }
    TimeCost cost;
    auto res = _rocksdb->remove_range(options, data_cf, 
            start_key.data(), end_key.data());
    if (!res.ok()) {
        DB_WARNING("remove_range error: code=%d, msg=%s, region_id: %ld", 
            res.code(), res.ToString().c_str(), _region_id);
        return -1;
    }
    DB_WARNING("remove_range cost:%ld, region_id: %ld", cost.get_time(), _region_id);
    // cost.reset();
    // rocksdb::Slice start(start_key.data());
    // rocksdb::Slice end(end_key.data());
    // rocksdb::CompactRangeOptions compact_options;
    // res = _rocksdb->compact_range(compact_options, data_cf, &start, &end);
    // if (!res.ok()) {
    //     DB_WARNING("compact_range error: code=%d, msg=%s", 
    //         res.code(), res.ToString().c_str());
    //     return -1;
    // }
    // DB_WARNING("compact_range cost:%ld, region_id: %ld", cost.get_time(), _region_id);
    return 0;
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
        pair.second->reverse_merge_func();
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
    if (!_status.compare_exchange_strong(expected_status, pb::DOING)) {
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
    if (send_init_region_to_store(_split_param.instance, init_region_request, NULL) != 0) {
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
    _split_param.split_slow_down_cost = std::max(average_cost, (int64_t)50000);

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

    BthreadCond cond;
    BthreadCond concurrency_cond(-5); // -n就是并发跑n个bthread
    for (int64_t index_id : table_info.indices) {
        auto read_and_write = [this, &pk_info, &write_sst_lines, 
                                index_id, &cond, &concurrency_cond] () {
            //int prefix_len = sizeof(int64_t) * 2;
            ON_SCOPE_EXIT([&cond]() {
                cond.decrease_signal();
            });
            ON_SCOPE_EXIT([&concurrency_cond]() {
                concurrency_cond.decrease_signal();
            });
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
        concurrency_cond.increase();
        concurrency_cond.wait();
        cond.increase();
        Bthread bth(&BTHREAD_ATTR_SMALL);
        bth.run(read_and_write);
    }
    cond.wait();
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

    // replay txn commands on new region
    if (0 != new_region->replay_txn_for_recovery(_split_param.prepared_txn)) {
        DB_WARNING("replay_txn_for_recovery failed: region_id: %ld, new_region_id: %ld",
            _region_id, _split_param.new_region_id);
        return;
    }

    //snapshot 之前发送5个NO_OP请求
    int ret = send_no_op_request(_split_param.instance, _split_param.new_region_id, 0);
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
        if (prepare_plan.op_type() != pb::OP_PREPARE) {
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
        int ret = send_request_to_region(request, instance, region_id);
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
    while ((_applied_index - start_index) > write_count_max && while_count < 10) {
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
            int ret = send_request_to_region(request,
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
    }
   
    DB_WARNING("send log entry before not allow success when split, "
                "region_id: %ld, new_region_id:%ld, instance:%s, time_cost:%ld, "
                "start_index:%ld, end_index:%ld, applied_index:%ld, while_count:%d",
                _region_id, _split_param.new_region_id,
                _split_param.instance.c_str(), send_first_log_entry_time.get_time(),
                _split_param.split_start_index, start_index, _applied_index, while_count);

    _split_param.send_first_log_entry_cost = send_first_log_entry_time.get_time();
    
    _split_param.no_write_time_cost.reset();
    //设置禁写 并且等待正在写入任务提交
    TimeCost write_wait_cost;
    _disable_write_cond.increase();
    int64_t disable_write_wait = get_split_wait_time();
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
        int ret = send_request_to_region(request, 
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
        int64_t peer_applied_index = get_peer_applied_index(peer_string, _region_id);
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
int64_t Region::get_peer_applied_index(const std::string& peer, int64_t region_id) {
        brpc::Channel channel;
        brpc::ChannelOptions channel_opt;
        channel_opt.timeout_ms = FLAGS_store_request_timeout;
        channel_opt.connect_timeout_ms = FLAGS_store_connect_timeout;
        if (channel.Init(peer.c_str(), &channel_opt)) {
            DB_FATAL("get peer applied index when connect new instance, region_id: %ld, instance:%s",
                          region_id, peer.c_str());
            return 0; 
        }
        uint64_t log_id = butil::fast_rand();
        brpc::Controller cntl;
        cntl.set_log_id(log_id);
        pb::GetAppliedIndex request;
        request.set_region_id(region_id);
        pb::StoreRes response;
        pb::StoreService_Stub(&channel).get_applied_index(&cntl, &request, &response, NULL);
        if (cntl.Failed()) {
            DB_FATAL("get applied index fail, region_id: %ld, instance%s, err_txt:%s",
                     region_id, peer.c_str(), cntl.ErrorText().c_str());
            return 0;
        } 
        if (response.errcode() != pb::SUCCESS) {
            DB_FATAL("cntl failed , err:%s, response", 
                     cntl.ErrorText().c_str(), pb2json(response).c_str());
            return 0;
        } 
        return response.applied_index();
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

int Region::save_prepared_txn(
        std::unordered_map<uint64_t, pb::TransactionInfo>& prepared_txn_info,
        rapidjson::Document& root) {

    rapidjson::Document::AllocatorType& alloc = root.GetAllocator();
    DB_WARNING("_prepared_txn_info.size(): %lu", prepared_txn_info.size());

    if (prepared_txn_info.size() > 0) {
        rapidjson::Value txn_array(rapidjson::kArrayType);
        for (auto& pair : prepared_txn_info) {
            std::string txn_str = std::to_string(pair.first);
            pb::TransactionInfo& txn_info = pair.second;

            rapidjson::Value txn_item(rapidjson::kObjectType);

            rapidjson::Value txn_key(rapidjson::kStringType);
            txn_key.SetString(txn_str.c_str(), txn_str.size(), alloc);

            rapidjson::Value txn_value(rapidjson::kStringType);
            std::string encoded_txn = pb2json(txn_info);
            if (encoded_txn.empty()) {
                DB_WARNING("TransactionError: encode txn failed, region_id: %ld, txn_id: %s", 
                    _region_id, txn_str.c_str());
                return -1;
            }
            txn_value.SetString(encoded_txn.c_str(), encoded_txn.size(), alloc);

            DB_WARNING("key: %s, value: %s", txn_str.c_str(), encoded_txn.c_str());
            // std::string encoded_plan;
            // item_value.AddMember("num_rows", pair.second.num_rows, alloc);
            // item_value.AddMember("seq_id", pair.second.seq_id, alloc);
            // item_value.AddMember("write_batch", encoded_batch);

            txn_item.AddMember(txn_key, txn_value, alloc);
            txn_array.PushBack(txn_item, alloc);
        }
        root.AddMember("txn_info", txn_array, alloc);
    }
    return 0;
}

int Region::load_prepared_txn(
        std::unordered_map<uint64_t, pb::TransactionInfo>& prepared_txn_info,
        rapidjson::Document& root) {

    auto json_iter = root.FindMember("txn_info");
    if (json_iter != root.MemberEnd()) {
        auto& txn_array = json_iter->value;
        for (auto iter = txn_array.Begin(); iter != txn_array.End(); ++iter){
            std::string txn_str = iter->MemberBegin()->name.GetString();
            std::string txn_value = iter->MemberBegin()->value.GetString();

            pb::TransactionInfo txn;
            std::string err = json2pb(txn_value, &txn);
            if (!err.empty()) {
                DB_WARNING("decode txn failed, region_id: %ld, txn_id: %s, txn_value: %s, err: %s", 
                    _region_id, txn_str.c_str(), txn_value.c_str(), err.c_str());
                return -1;
            }
            prepared_txn_info.insert({strtoull(txn_str.c_str(), nullptr, 0), txn});
        }
    } else {
        DB_WARNING("no prepared txn when shutdown, region_id: %ld", _region_id);
    }
    return 0;
}

// When quit gracefully, save region info for recovery:
// 1) current applied_index of raft state machine
// 2) num_table_lines of the region primary record
// 3) num_increased_rows of each prepared txn
void Region::save_applied_index() {
    rapidjson::Document root;
    root.SetObject();
    rapidjson::Document::AllocatorType& alloc = root.GetAllocator();
    root.AddMember("init_applied_index", _applied_index, alloc);
    root.AddMember("num_table_lines", _num_table_lines.load(), alloc);

    save_prepared_txn(_prepared_txn_info, root);

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> json_writer(buffer);                      
    root.Accept(json_writer);                                                            
    
    std::string applied_index(buffer.GetString());
    std::string applied_path = FLAGS_save_applied_index_path + "/region_" + std::to_string(_region_id);
    std::string applied_file = "/applied_index.json";
    
    boost::filesystem::path remove_path(applied_path);
    boost::filesystem::remove_all(remove_path);  

    //新建目录
    boost::filesystem::create_directories(applied_path);

    std::ofstream applied_fs(applied_path + applied_file, 
            std::ofstream::out | std::ofstream::trunc);
    applied_fs.write(applied_index.data(), applied_index.size());
    applied_fs.close();
}

void Region::save_snapshot(braft::Closure* done, 
                            rocksdb::Iterator* iter, 
                            braft::SnapshotWriter* writer, 
                            MutTableKey region_prefix,
                            std::string extra,
                            pb::RegionInfo snapshot_region_info) {
    TimeCost time_cost;
    brpc::ClosureGuard done_guard(done);
    std::unique_ptr<rocksdb::Iterator> iter_lock(iter);
    
    std::string snapshot_path = writer->get_path();
   
    //snapshot保存三个文件，数据sst, json, region_info的sst文件 
    std::string sst_file_name = "/snap_region_";
    sst_file_name += std::to_string(_region_id);
    
    std::string extra_file_name = sst_file_name  + ".extra.json";
    std::string region_info_sst = sst_file_name + ".region_info.sst";

    std::ofstream extra_fs(snapshot_path + extra_file_name, 
            std::ofstream::out | std::ofstream::trunc);
    extra_fs.write(extra.data(), extra.size());
    extra_fs.close();
    if (writer->add_file(extra_file_name) != 0) {
        done->status().set_error(EINVAL, "Fail to add snapshot");
        DB_WARNING("Error while adding extra_fs to writer, region_id: %ld", _region_id);
        return;
    }

    int64_t row_count = 0;
    auto ret = _write_sst_for_data(done, snapshot_path + sst_file_name, iter, region_prefix, row_count); 
    if (ret != 0) {
        DB_WARNING("Error write sst for data, region_id: %ld", _region_id);
        return;
    }
    if (row_count != 0 && writer->add_file(sst_file_name) != 0) {
        done->status().set_error(EINVAL, "Fail to add snapshot");
        DB_WARNING("Error while adding sst file to writer, region_id: %ld", _region_id);
        return;
    }

    ret  = _write_sst_for_region_info(done, snapshot_path + region_info_sst, snapshot_region_info);
    if (ret != 0) {
        DB_WARNING("Error write sst for region_info,  region_id: %ld", _region_id);
        return;
    }

    if (writer->add_file(region_info_sst) != 0) {
        done->status().set_error(EINVAL, "Fail to add snapshot");
        DB_WARNING("Error while adding sst file to writer, region_id: %ld", _region_id);
        return;
    }
    DB_WARNING("save snapshot success, num_table_lines:%ld, region_id: %ld, time_cost:%ld", 
                _num_table_lines.load(), _region_id, time_cost.get_time());
    return;
}
int Region::_write_sst_for_region_info(braft::Closure* done, 
                               const std::string& sst_file, 
                               pb::RegionInfo& snapshot_region_info) {
    rocksdb::Options option = _rocksdb->get_options(_region_cf);
    rocksdb::SstFileWriter sst_writer(rocksdb::EnvOptions(), option, nullptr, true);
    // Open the file for writing
    auto s = sst_writer.Open(sst_file);
    if (!s.ok()) {
        DB_WARNING("Error while opening file %s, Error: %s, region_id: %ld", 
                    sst_file.c_str(),
                    s.ToString().c_str(),
                    _region_id);
        done->status().set_error(EINVAL, "Fail to create SstFileWriter");
        return -1;
    }
    int64_t region_id = snapshot_region_info.region_id(); 
    std::string region_key = Store::SCHEMA_IDENTIFY;
    region_key += Store::REGION_SCHEMA_IDENTIFY;
    region_key.append((char*)&region_id, sizeof(int64_t));
    std::string region_value;
    if (!snapshot_region_info.SerializeToString(&region_value)) {
        DB_WARNING("request serializeToArray fail, request:%s, region_id: %ld",
                    snapshot_region_info.ShortDebugString().c_str(), _region_id); 
        done->status().set_error(EINVAL, "Fail to serialize to array");
        sst_writer.Finish();
        return -1;
    }
    s = sst_writer.Put(region_key, region_value);
    if (!s.ok()) {
        DB_WARNING("Error while adding region_info sst, Error: %s, region_id: %ld", 
                s.ToString().c_str(),
                _region_id);
        done->status().set_error(EINVAL, "Fail to write SstFileWriter");
        sst_writer.Finish();
        return -1;
    }
    // Close the file
    s = sst_writer.Finish();
    if (!s.ok()) {
        DB_WARNING("Error while finishing file %s, Error: %s, region_id: %ld", 
                    sst_file.c_str(),
                    s.ToString().c_str(),
                    _region_id);
        done->status().set_error(EINVAL, "Fail to finish SstFileWriter");
        return -1;
    }
    return 0;
}
int Region::_write_sst_for_data(braft::Closure* done,
                                const std::string& sst_file, 
                                rocksdb::Iterator* iter, 
                                MutTableKey& region_prefix,
                                int64_t& row_count) {
    rocksdb::Options option = _rocksdb->get_options(_data_cf);
    rocksdb::SstFileWriter sst_writer(rocksdb::EnvOptions(), option, nullptr, true);
    // Open the file for writing
    auto s = sst_writer.Open(sst_file);
    if (!s.ok()) {
        DB_WARNING("Error while opening file %s, Error: %s, region_id: %ld", 
                    sst_file.c_str(),
                    s.ToString().c_str(),
                    _region_id);
        done->status().set_error(EINVAL, "Fail to create SstFileWriter");
        return -1;
    }
    for (; iter->Valid() && iter->key().starts_with(region_prefix.data()); 
            iter->Next()) {
        auto s = sst_writer.Put(iter->key(), iter->value());
        ++row_count;
        if (!s.ok()) {
            DB_WARNING("Error while adding Key: %s, Error: %s, region_id: %ld", 
                    iter->key().data(),
                    s.ToString().c_str(),
                    _region_id);
            done->status().set_error(EINVAL, "Fail to write SstFileWriter");
            sst_writer.Finish();
            return -1;
        }
    }
    // Close the file
    s = sst_writer.Finish();
    if (row_count == 0) {
        DB_WARNING("region_id: %ld has no entries", _region_id);
        return 0;
    }
    if (!s.ok()) {
        DB_WARNING("Error while finishing file %s, Error: %s, region_id: %ld", 
                    sst_file.c_str(),
                    s.ToString().c_str(),
                    _region_id);
        done->status().set_error(EINVAL, "Fail to finish SstFileWriter");
        return -1;
    }
    return 0;
}
int Region::send_no_op_request(const std::string& instance, 
                               int64_t recevie_region_id, 
                               int64_t request_version) {
    int ret = 0;
    for (int i = 0; i < 5; ++i) {
        pb::StoreReq request;
        request.set_op_type(pb::OP_NONE);
        request.set_region_id(recevie_region_id);
        request.set_region_version(request_version);
        ret = send_request_to_region(request, instance, recevie_region_id);
        if (ret < 0) {
            DB_WARNING("send no op fail, region_id: %ld, reqeust: %s", 
                        _region_id, request.ShortDebugString().c_str());
            bthread_usleep(1000 * 1000LL);
        }
    }
    return ret;
}

int Region::send_request_to_region(const pb::StoreReq& request, 
                                        const std::string& instance, 
                                        int64_t receive_region_id) {
    uint64_t log_id = butil::fast_rand();
    int retry_times = 0;
    TimeCost time_cost;
    do {
        DB_WARNING("send_request_to_region: log_id: %lu, retry: %d", log_id, retry_times);
        brpc::Channel channel;
        brpc::ChannelOptions channel_opt;
        channel_opt.timeout_ms = FLAGS_store_request_timeout;
        channel_opt.connect_timeout_ms = FLAGS_store_connect_timeout;
        if (channel.Init(instance.c_str(), &channel_opt)) {
             DB_FATAL("channel init fail when new region,"
                      " region_id: %ld, receive_region_id: %ld, instance:%s",
                      _region_id, receive_region_id, instance.c_str());
             ++retry_times;
             continue;
        }
        brpc::Controller cntl;
        cntl.set_log_id(log_id);
        pb::StoreRes response;
        pb::StoreService_Stub(&channel).query(&cntl, &request, &response, NULL);
        if (cntl.Failed()) {
            DB_FATAL("cntl fail with new reigon fail,"
                     " region_id:%ld, receive_region_id: %ld, instance%s, err_txt:%s",
                     _region_id, receive_region_id,  instance.c_str(),
                     cntl.ErrorText().c_str());
            ++retry_times;
            continue;
        } 
        if (response.errcode() != pb::SUCCESS && response.errcode() != pb::NOT_LEADER) {
            DB_FATAL("send request to new region fail,"
                     "region_id: %ld, receive_region_id: %ld, instance:%s, errcode:%s, response:%s",
                     _region_id, 
                     receive_region_id, 
                     instance.c_str(), 
                     pb::ErrCode_Name(response.errcode()).c_str(),
                     pb2json(response).c_str());
            ++retry_times;
            continue;
        }
        if (response.errcode() == pb::NOT_LEADER) {
            ++retry_times;
            bthread_usleep(10 * 1000); // 10ms
        } else {
            DB_WARNING("send request to new region success,"
                        " response:%s, region_id: %ld, receive_region_id: %ld, time_cost:%ld", 
                        pb2json(response).c_str(),
                        _region_id,
                        receive_region_id,
                        time_cost.get_time());
            return 0;
        } 
    } while (retry_times < 3);
    return -1;
}

int Region::send_init_region_to_store(const std::string instance_address,
                                       const pb::InitRegion& init_region_request,
                                       pb::StoreRes* store_response) {
    brpc::Channel channel;
    brpc::ChannelOptions channel_opt;
    channel_opt.timeout_ms = FLAGS_store_request_timeout;
    channel_opt.connect_timeout_ms = FLAGS_store_connect_timeout;
    if (channel.Init(instance_address.c_str(), &channel_opt)) {
        DB_FATAL("add peer fail when connect new instance, region_id: %ld, instance:%s",
                  _region_id, _split_param.instance.c_str());
        if (store_response != NULL) {
            store_response->set_errcode(pb::CONNECT_FAIL);
            store_response->set_errmsg("connet with " + instance_address + " fail");
        }
        return -1;
    }
    brpc::Controller cntl;
    pb::StoreRes response;
    uint64_t log_id = butil::fast_rand();
    cntl.set_log_id(log_id);
    //发送一个创建region的请求，node的节点为空
    pb::StoreService_Stub(&channel).init_region(&cntl, 
                                                &init_region_request, 
                                                &response, 
                                                NULL);
    int64_t region_id = init_region_request.region_info().region_id(); 
    if (cntl.Failed()) {
        DB_WARNING("add peer fail when init region,"
                   " region_id: %ld, instance:%s, cntl.err_msg:%s",
                   region_id, instance_address.c_str(), cntl.ErrorText().c_str());
        if (store_response != NULL) {
             store_response->set_errcode(pb::EXEC_FAIL);
             store_response->set_errmsg(cntl.ErrorText().c_str());
        }
        return -1;
    }
    if (response.errcode() != pb::SUCCESS) {
        DB_WARNING("add peer fail when init region,"
                   " region_id: %ld, instance:%s, errcode:%s",
                   region_id, instance_address.c_str(), 
                   pb::ErrCode_Name(response.errcode()).c_str());
        if (response.errcode() != pb::REGION_ALREADY_EXIST) {
            start_thread_to_remove_region(region_id, instance_address);
        }
        if (store_response != NULL) {
            store_response->set_errcode(response.errcode());
            store_response->set_errmsg(response.errmsg());
        }
        return -1;
    }
    return 0;
}

void Region::start_thread_to_remove_region(int64_t drop_region_id, std::string instance_address) {
    Bthread bth(&BTHREAD_ATTR_SMALL);
    std::function<void()> remove_region_function = 
        [this, drop_region_id, instance_address]() {
            send_remove_region_to_store(drop_region_id, instance_address);
        };
    bth.run(remove_region_function);
}

void Region::send_remove_region_to_store(int64_t drop_region_id, std::string instance_address) {
    _multi_thread_cond.increase();
    ON_SCOPE_EXIT([this]() {
        _multi_thread_cond.decrease_signal();
    });
    brpc::Channel channel;
    brpc::ChannelOptions channel_opt;
    channel_opt.timeout_ms = FLAGS_store_request_timeout;
    channel_opt.connect_timeout_ms = FLAGS_store_connect_timeout;
    if (channel.Init(instance_address.c_str(), &channel_opt)) {
        DB_FATAL("add peer fail when connect new instance, region_id: %ld, instance:%s",
                  _region_id, _split_param.instance.c_str());
        _multi_thread_cond.decrease_signal();
        return;
    }
    brpc::Controller cntl;
    pb::StoreRes response;
    pb::RemoveRegion remove_region_request;
    remove_region_request.set_region_id(drop_region_id);
    remove_region_request.set_force(true);
    uint64_t log_id = butil::fast_rand();
    cntl.set_log_id(log_id);
    //发送一个创建region的请求，node的节点为空
    pb::StoreService_Stub(&channel).remove_region(&cntl, 
                                                 &remove_region_request,
                                                 &response,
                                                 NULL);
    if (cntl.Failed()) {
        DB_WARNING("remove region fail,"
                    " region_id: %ld, drop_region_id: %ld, instance:%s, cntl:%d",
                     _region_id, drop_region_id, 
                     instance_address.c_str(), cntl.ErrorText().c_str());
    } else if (response.errcode() != pb::SUCCESS) {
        DB_WARNING("remove region fail,"
                    " region_id: %ld, drop_region_id: %ld, instance:%s, errcode:%d",
                     _region_id, drop_region_id, instance_address.c_str(),
                     pb::ErrCode_Name(response.errcode()).c_str());
    } else {
        DB_WARNING("remove region success, region_id: %ld, drop_region_id: %ld, instance:%s", 
                    _region_id, drop_region_id, instance_address.c_str());
    }
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

int Region::_write_region_to_rocksdb(const pb::RegionInfo& region_info) {
    int64_t region_id = region_info.region_id(); 
    rocksdb::WriteBatch batch;
    rocksdb::WriteOptions options;
    //options.sync = true;
    //options.disableWAL = true;
    std::string region_key = Store::SCHEMA_IDENTIFY;
    region_key += Store::REGION_SCHEMA_IDENTIFY;
    region_key.append((char*)&region_id, sizeof(int64_t));
    std::string region_value;
    if (!region_info.SerializeToString(&region_value)) {
        DB_WARNING("request serializeToArray fail, request:%s, region_id: %ld",
                    pb2json(region_info).c_str(), _region_id);                           
        return -1;                                                                        
    }
    batch.Put(_region_cf, region_key, region_value);
    auto status = _rocksdb->write(options, &batch);
    if (!status.ok()) {
        DB_WARNING("write region_id: %ld info to rocksdb fail, err_msg:%s",
                    region_id, status.ToString().c_str());
        return -1;
    }
    //rocksdb::FlushOptions flush_options;
    //status = _rocksdb->flush(flush_options, _region_cf);
    //if (!status.ok()) {
    //    DB_WARNING("write region_id: %ld info to rocksdb fail, err_msg:%s",
    //                region_id, status.ToString().c_str());
    //    return -1;
    //}
    DB_WARNING("write region into to rocksdb, region_info: %s", region_info.ShortDebugString().c_str());
    return 0;  
}
} // end of namespace
