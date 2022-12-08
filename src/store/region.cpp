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

#include "region.h"
#include <algorithm>
#include <fstream>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
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
#include "qos.h"
#ifdef BAIDU_INTERNAL
#include <base/files/file.h>
#else
#include <butil/files/file.h>
#endif
#ifdef BAIDU_INTERNAL
namespace butil = base;
#endif

#ifdef BAIDU_INTERNAL
namespace raft {
#else
namespace braft {
#endif
DECLARE_int32(raft_election_heartbeat_factor);
}

namespace baikaldb {
DEFINE_bool(use_fulltext_wordweight_segment, true, "load wordweight dict");
DEFINE_bool(use_fulltext_wordseg_wordrank_segment, true, "load wordseg wordrank dict");
DEFINE_int32(election_timeout_ms, 1000, "raft election timeout(ms)");
DEFINE_int32(skew, 5, "split skew, default : 45% - 55%");
DEFINE_int32(reverse_level2_len, 5000, "reverse index level2 length, default : 5000");
DEFINE_string(raftlog_uri, "myraftlog://my_raft_log?id=", "raft log uri");
DEFINE_string(binlog_uri, "mybinlog://my_bin_log?id=", "bin log uri");
//不兼容配置，默认用写到rocksdb的信息; raft自带的local://./raft_data/stable/region_
DEFINE_string(stable_uri, "myraftmeta://my_raft_meta?id=", "raft stable path");
DEFINE_string(snapshot_uri, "local://./raft_data/snapshot", "raft snapshot path");
DEFINE_int64(disable_write_wait_timeout_us, 1000 * 1000, 
        "disable write wait timeout(us) default 1s");
DEFINE_int64(real_writing_wait_timeout_us, 1000 * 1000, 
        "real writing wait timeout(us) default 1s");
DEFINE_int32(snapshot_interval_s, 600, "raft snapshot interval(s)");
DEFINE_int32(fetch_log_timeout_s, 60, "raft learner fetch log time out(s)");
DEFINE_int32(fetch_log_interval_ms, 10, "raft learner fetch log interval(ms)");
DEFINE_int32(snapshot_timed_wait, 120 * 1000 * 1000LL, "snapshot timed wait default 120S");
DEFINE_int64(snapshot_diff_lines, 10000, "save_snapshot when num_table_lines diff");
DEFINE_int64(snapshot_diff_logs, 2000, "save_snapshot when log entries diff");
DEFINE_int64(snapshot_log_exec_time_s, 60, "save_snapshot when log entries apply time");
//分裂判断标准，如果3600S没有收到请求，则认为分裂失败
DEFINE_int64(split_duration_us, 3600 * 1000 * 1000LL, "split duration time : 3600s");
DEFINE_int64(compact_delete_lines, 200000, "compact when _num_delete_lines > compact_delete_lines");
DEFINE_int64(throttle_throughput_bytes, 50 * 1024 * 1024LL, "throttle throughput bytes");
DEFINE_int64(tail_split_wait_threshold, 600 * 1000 * 1000LL, "tail split wait threshold(10min)");
DEFINE_int64(split_send_first_log_entry_threshold, 3600 * 1000 * 1000LL, "split send log entry threshold(1h)");
DEFINE_int64(split_send_log_batch_size, 20, "split send log batch size");
DEFINE_int64(no_write_log_entry_threshold, 1000, "max left logEntry to be exec before no write");
DEFINE_int64(split_adjust_slow_down_cost, 40, "split adjust slow down cost");
DECLARE_int64(transfer_leader_catchup_time_threshold);
DEFINE_bool(force_clear_txn_for_fast_recovery, false, "clear all txn info for fast recovery");
DEFINE_bool(split_add_peer_asyc, false, "asyc split add peer");
DECLARE_int64(exec_1pc_out_fsm_timeout_ms);
DECLARE_string(db_path);
DECLARE_int64(print_time_us);
DECLARE_int64(store_heart_beat_interval_us);
DECLARE_int64(min_split_lines);
DECLARE_bool(use_approximate_size);
DECLARE_bool(use_approximate_size_to_split);
DECLARE_bool(open_service_write_concurrency);
DECLARE_bool(open_new_sign_read_concurrency);
DECLARE_bool(stop_ttl_data);
//const size_t  Region::REGION_MIN_KEY_SIZE = sizeof(int64_t) * 2 + sizeof(uint8_t);
const uint8_t Region::PRIMARY_INDEX_FLAG = 0x01;                                   
const uint8_t Region::SECOND_INDEX_FLAG = 0x02;
const int BATCH_COUNT = 1024;

ScopeProcStatus::~ScopeProcStatus() {
    if (_region != NULL) {
        _region->reset_region_status();
        if (_region->is_disable_write()) {
            _region->reset_allow_write();
        }
        _region->reset_split_status();
        baikaldb::Store::get_instance()->sub_split_num();
    }
}

ScopeMergeStatus::~ScopeMergeStatus() {
    if (_region != NULL) {
        _region->reset_region_status();
        _region->reset_allow_write(); 
    }
}

int Region::init(bool new_region, int32_t snapshot_times) {
    _shutdown = false;
    if (_init_success) {
        DB_WARNING("region_id: %ld has inited before", _region_id);
        return 0;
    }
    // 对于没有table info的region init_success一直false，导致心跳不上报，无法gc
    ON_SCOPE_EXIT([this]() {
        _can_heartbeat = true;
    });
    MutTableKey start;
    MutTableKey end;
    start.append_i64(_region_id);
    end.append_i64(_region_id);
    end.append_u64(UINT64_MAX);
    _rocksdb_start = start.data();
    _rocksdb_end = end.data();

    _backup.set_info(get_ptr(), _region_id);
    _data_cf = _rocksdb->get_data_handle();
    _meta_cf = _rocksdb->get_meta_info_handle();
    _meta_writer = MetaWriter::get_instance();
    TimeCost time_cost;
    _resource.reset(new RegionResource);
    //如果是新建region需要
    if (new_region) {
        std::string snapshot_path_str(FLAGS_snapshot_uri, FLAGS_snapshot_uri.find("//") + 2);
        snapshot_path_str += "/region_" + std::to_string(_region_id);
        boost::filesystem::path snapshot_path(snapshot_path_str);
        // 新建region发现有时候snapshot目录没删掉，可能有gc不完整情况
        if (boost::filesystem::exists(snapshot_path)) {
            DB_FATAL("new region_id: %ld exist snapshot path:%s", 
                    _region_id, snapshot_path_str.c_str());
            RegionControl::remove_data(_region_id);
            RegionControl::remove_meta(_region_id);
            RegionControl::remove_log_entry(_region_id);
            RegionControl::remove_snapshot_path(_region_id);
        }
        // 被addpeer的node不需要init meta
        // on_snapshot_load时会ingest meta sst
        if (_region_info.peers_size() > 0) {
            TimeCost write_db_cost;
            if (_meta_writer->init_meta_info(_region_info) != 0) {
                DB_FATAL("write region to rocksdb fail when init reigon, region_id: %ld", _region_id);
                return -1;
            }
            if (_is_learner && _meta_writer->write_learner_key(_region_info.region_id(), _is_learner) != 0) {
                DB_FATAL("write learner to rocksdb fail when init reigon, region_id: %ld", _region_id);
                return -1;
            }
            DB_WARNING("region_id: %ld write init meta info: %ld", _region_id, write_db_cost.get_time());
        }
    } else {
        _report_peer_info = true;
    }
    if (!_is_global_index) {
        auto table_info = _factory->get_table_info(_region_info.table_id());
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
                        segment_type = pb::S_UNIGRAMS;
#endif
                    }
#ifdef BAIDU_INTERNAL
                    if (segment_type == pb::S_WORDRANK || segment_type == pb::S_WORDSEG_BASIC || 
                        segment_type == pb::S_WORDRANK_Q2B_ICASE || segment_type == pb::S_WORDRANK_Q2B_ICASE_UNLIMIT) {
                        if (!FLAGS_use_fulltext_wordseg_wordrank_segment) {
                            DB_FATAL("region %ld store not support word[seg/rank] segment, \
                                open flag use_fulltext_workseg_wordrank_segment", _region_id);
                            return -1;
                        }
                    } else if ((segment_type == pb::S_WORDWEIGHT || segment_type == pb::S_WORDWEIGHT_NO_FILTER) &&
                         !FLAGS_use_fulltext_wordweight_segment) {
                        DB_FATAL("region %ld store not support wordweight segment, \
                            open flag use_fulltext_wordweight_segment", _region_id);
                        return -1;
                    }
#endif

                    if (info.storage_type == pb::ST_PROTOBUF_OR_FORMAT1) {
                        DB_NOTICE("create pb schema.");
                        _reverse_index_map[index_id] = new ReverseIndex<CommonSchema>(
                            _region_id, 
                            index_id,
                            FLAGS_reverse_level2_len,
                            _rocksdb,
                            segment_type,
                            false, // common need not cache
                            true);
                    } else {
                        DB_NOTICE("create arrow schema.");
                        _reverse_index_map[index_id] = new ReverseIndex<ArrowSchema>(
                            _region_id, 
                            index_id,
                            FLAGS_reverse_level2_len,
                            _rocksdb,
                            segment_type,
                            false, // common need not cache
                            true);
                    }
                    break;
                default:
                    break;
            }
        }
    }

    TTLInfo ttl_info = _factory->get_ttl_duration(get_table_id());
    if (ttl_info.ttl_duration_s > 0) {
        _use_ttl = true;
        if (ttl_info.online_ttl_expire_time_us > 0) {
            // online TTL 
            _online_ttl_base_expire_time_us = ttl_info.online_ttl_expire_time_us; 
        } 
    }
    _storage_compute_separate = _factory->get_separate_switch(get_table_id());

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
    if (!_is_binlog_region) {
        options.log_uri = FLAGS_raftlog_uri + 
                        boost::lexical_cast<std::string>(_region_id);  
    } else {
        options.log_uri = FLAGS_binlog_uri + 
                        boost::lexical_cast<std::string>(_region_id);  
    }
#ifdef BAIDU_INTERNAL
    options.stable_uri = FLAGS_stable_uri + 
                           boost::lexical_cast<std::string>(_region_id);
#else
    options.raft_meta_uri = FLAGS_stable_uri + 
                           boost::lexical_cast<std::string>(_region_id);
#endif
    options.snapshot_uri = FLAGS_snapshot_uri + "/region_" + 
                                boost::lexical_cast<std::string>(_region_id);
    options.snapshot_file_system_adaptor = &_snapshot_adaptor;

    _txn_pool.init(_region_id, _use_ttl, _online_ttl_base_expire_time_us);
    bool is_restart = _restart;
    if (_is_learner) {
        DB_DEBUG("init learner.");
        int64_t check_cycle = 10;
        scoped_refptr<braft::SnapshotThrottle> tst(
            new braft::ThroughputSnapshotThrottle(FLAGS_throttle_throughput_bytes, check_cycle));
#ifdef BAIDU_INTERNAL
        options.snapshot_throttle = &tst;
        options.catchup_margin = 2;
        options.fetch_log_timeout_s = FLAGS_fetch_log_timeout_s;
        options.fetch_log_interval_ms = FLAGS_fetch_log_interval_ms;
        options.learner_auto_load_applied_logs = true;
#endif
        int ret = _learner->init(options);
        if (ret != 0) {
            DB_FATAL("init region_%ld fail.", _region_id);
            return -1;
        }
    } else {
        DB_DEBUG("node init.");
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
    }
    if (!is_restart && is_addpeer()) {
        _need_decrease = true;
    }
    reset_timecost();
    while (snapshot_times > 0) {
        // init的region会马上选主，等一会为leader
        bthread_usleep(1 * 1000 * 1000LL);
        int ret = _region_control.sync_do_snapshot();
        if (ret != 0) {
            DB_FATAL("init region_%ld do snapshot fail.", _region_id);
            return -1;
        }
        --snapshot_times;
    }
    copy_region(&_resource->region_info);
    //compaction时候删掉多余的数据
    if (_is_binlog_region) {
        //binlog region把start key和end key设置为空，防止filter把数据删掉
        SplitCompactionFilter::get_instance()->set_filter_region_info(
                _region_id, "", false, 0);
        SplitCompactionFilter::get_instance()->set_binlog_region(_region_id);
    } else {
        SplitCompactionFilter::get_instance()->set_filter_region_info(
                _region_id, _resource->region_info.end_key(), 
                _use_ttl, _online_ttl_base_expire_time_us);
    }
    DB_WARNING("region_id: %ld init success, region_info:%s, time_cost:%ld", 
                _region_id, _resource->region_info.ShortDebugString().c_str(), 
                time_cost.get_time());
    _init_success = true;
    //特殊逻辑，后续下掉
    if (_table_id == 858) {
        _reverse_remove_range = true;
    }
    return 0;
}

bool Region::check_region_legal_complete() {
    do {
        bthread_usleep(10 * 1000 * 1000);
        //3600S没有收到请求， 并且version 也没有更新的话，分裂失败
        if (_removed) {
            DB_WARNING("region_id: %ld has been removed", _region_id);
            return true;
        }
        if (get_timecost() > FLAGS_split_duration_us) {
            if (compare_and_set_illegal()) {
                DB_WARNING("split or add_peer fail, set illegal, region_id: %ld",
                           _region_id);
                return false;
            } else {
                DB_WARNING("split or add_peer  success, region_id: %ld", _region_id);
                return true;
            }
        } else if (get_version() > 0) {
            DB_WARNING("split or add_peer success, region_id: %ld", _region_id);
            return true;
        } else {
            DB_WARNING("split or add_peer not complete, need wait, region_id: %ld, cost_time: %ld", 
                _region_id, get_timecost());
        }
    } while (1);
}

bool Region::validate_version(const pb::StoreReq* request, pb::StoreRes* response) {
    if (request->region_version() < get_version()) {
        response->Clear();
        response->set_errcode(pb::VERSION_OLD);
        response->set_errmsg("region version too old");

        std::string leader_str = butil::endpoint2str(get_leader()).c_str();
        response->set_leader(leader_str);
        auto region = response->add_regions();
        copy_region(region);
        region->set_leader(leader_str);
        if (!region->start_key().empty() 
                && region->start_key() == region->end_key()) {
            //start key == end key region发生merge，已经为空
            response->set_is_merge(true);
            if (_merge_region_info.start_key() != region->start_key()) {
                DB_FATAL("merge region:%ld start key ne regiond:%ld",
                        _merge_region_info.region_id(),
                        _region_id);
            } else {
                response->add_regions()->CopyFrom(_merge_region_info);
                DB_WARNING("region id:%ld, merge region info:%s", 
                           _region_id,
                           pb2json(_merge_region_info).c_str());
            }
        } else {
            response->set_is_merge(false);
            for (auto& r : _new_region_infos) {
                if (r.region_id() != 0 && r.version() != 0) {
                    response->add_regions()->CopyFrom(r);
                    DB_WARNING("new region %ld, %ld", 
                               _region_id, r.region_id());
                } else {
                    DB_FATAL("r:%s", pb2json(r).c_str());
                }
            }
        }
        return false;
    }
    return true;
}

int Region::execute_cached_cmd(const pb::StoreReq& request, pb::StoreRes& response, 
        uint64_t txn_id, SmartTransaction& txn, 
        int64_t applied_index, int64_t term, uint64_t log_id) {
    if (request.txn_infos_size() == 0) {
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
                && op_type != pb::OP_UPDATE
                && op_type != pb::OP_SELECT_FOR_UPDATE) {
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
        if (op_type != pb::OP_SELECT_FOR_UPDATE) {
            dml_2pc(request, op_type, plan, tuples, res, applied_index, term, seq_id, false);
        } else {
            select(request, res);
        }
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
        if (res.has_last_insert_id()) {
            response.set_last_insert_id(res.last_insert_id());
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

void Region::exec_txn_query_state(google::protobuf::RpcController* controller,
            const pb::StoreReq* request,
            pb::StoreRes* response,
            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    // brpc::Controller* cntl = (brpc::Controller*)controller;
    // uint64_t log_id = 0;
    // if (cntl->has_log_id()) {
    //     log_id = cntl->log_id();
    // }
    response->add_regions()->CopyFrom(this->region_info());
    _txn_pool.get_txn_state(request, response);
    response->set_leader(butil::endpoint2str(get_leader()).c_str());
    response->set_errcode(pb::SUCCESS);
}

void Region::exec_txn_complete(google::protobuf::RpcController* controller,
            const pb::StoreReq* request,
            pb::StoreRes* response,
            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    for (auto txn_id : request->rollback_txn_ids()) {
        SmartTransaction txn = _txn_pool.get_txn(txn_id);
        if (txn != nullptr) {
            DB_WARNING("TransactionNote: txn is alive, region_id: %ld, txn_id: %lu, OP_ROLLBACK it",
                    _region_id, txn_id);
            if (!request->force()) {
                _txn_pool.txn_commit_through_raft(txn_id, region_info(), pb::OP_ROLLBACK);
            } else {
                txn->rollback();
            }
            _txn_pool.remove_txn(txn_id, false);
        } else {
            DB_WARNING("TransactionNote: txn not exist region_id: %ld txn_id: %lu",
                    _region_id, txn_id);
        }
    }
    for (auto txn_id : request->commit_txn_ids()) {
        SmartTransaction txn = _txn_pool.get_txn(txn_id);
        if (txn != nullptr) {
            DB_WARNING("TransactionNote: txn is alive, region_id: %ld, txn_id: %lu, OP_COMMIT it",
                    _region_id, txn_id);
            if (!request->force()) {
                _txn_pool.txn_commit_through_raft(txn_id, region_info(), pb::OP_COMMIT);
            } else {
                txn->commit();
            }
            _txn_pool.remove_txn(txn_id, false);
        } else {
            DB_WARNING("TransactionNote: txn not exist region_id: %ld txn_id: %lu",
                    _region_id, txn_id);
        }
    }
    if (request->txn_infos_size() > 0 && request->force()) {
        int64_t txn_timeout = request->txn_infos(0).txn_timeout();
        _txn_pool.rollback_txn_before(txn_timeout);
    }
    response->set_errcode(pb::SUCCESS);
}

void Region::exec_update_primary_timestamp(const pb::StoreReq& request, braft::Closure* done,
        int64_t applied_index, int64_t term) {
    const pb::TransactionInfo& txn_info = request.txn_infos(0);
    uint64_t txn_id = txn_info.txn_id();
    SmartTransaction txn = _txn_pool.get_txn(txn_id);
    if (txn != nullptr) {
        DB_WARNING("TransactionNote: region_id: %ld, txn_id: %lu applied_index:%ld",
            _region_id, txn_id, applied_index);
        if (done != nullptr) {
            ((DMLClosure*)done)->response->set_errcode(pb::SUCCESS);
            ((DMLClosure*)done)->response->set_errmsg("txn timestamp updated");
        }
        txn->reset_active_time();
    } else {
        DB_WARNING("TransactionNote: TXN_IS_ROLLBACK region_id: %ld, txn_id: %lu applied_index:%ld",
            _region_id, txn_id, applied_index);
        if (done != nullptr) {
            ((DMLClosure*)done)->response->set_errcode(pb::TXN_IS_ROLLBACK);
            ((DMLClosure*)done)->response->set_errmsg("txn not found");
        }
        if (get_version() == 0) {
            _async_apply_param.apply_log_failed = true;
        }
    }
}

void Region::exec_txn_query_primary_region(google::protobuf::RpcController* controller,
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
    const pb::TransactionInfo& txn_info = request->txn_infos(0);
    uint64_t txn_id = txn_info.txn_id();
    pb::TxnState txn_state = txn_info.txn_state();
    auto txn_res = response->add_txn_infos();
    txn_res->set_seq_id(txn_info.seq_id());
    txn_res->set_txn_id(txn_id);
    DB_WARNING("TransactionNote: txn has state(%s), region_id: %ld, txn_id: %lu, log_id: %lu, remote_side: %s",
                pb::TxnState_Name(txn_state).c_str(), _region_id, txn_id, log_id, remote_side);
    SmartTransaction txn = _txn_pool.get_txn(txn_id);
    if (txn != nullptr) {
        //txn还在，不做处理，可能primary region正在执行commit
        DB_WARNING("TransactionNote: txn is alive, region_id: %ld, txn_id: %lu, log_id: %lu try later",
                _region_id, txn_id, log_id);
        response->set_errcode(pb::TXN_IS_EXISTING);
        txn_res->set_seq_id(txn->seq_id());
        return;
    } else {
        int ret = _meta_writer->read_transcation_rollbacked_tag(_region_id, txn_id);
        if (ret == 0) {
            //查询meta存在说明是ROLLBACK，自己执行ROLLBACK
            DB_WARNING("TransactionNote: txn is rollback, region_id: %ld, txn_id: %lu, log_id: %lu",
                _region_id, txn_id, log_id);
            response->set_errcode(pb::SUCCESS);
            txn_res->set_txn_state(pb::TXN_ROLLBACKED);
            return;
        } else {
            //查询meta不存在说明是COMMIT
            if (txn_state == pb::TXN_BEGINED) {
                //secondary的事务还未prepare，可能是切主导致raft日志apply慢，让secondary继续追
                response->set_errcode(pb::SUCCESS);
                txn_res->set_txn_state(pb::TXN_BEGINED);
                return;
            }
            if (txn_info.has_open_binlog() && txn_info.open_binlog()) {
                int64_t commit_ts = get_commit_ts(txn_id, txn_info.start_ts());
                if (commit_ts == -1) {
                    commit_ts = Store::get_instance()->get_last_commit_ts();    
                    if (commit_ts < 0) {
                        response->set_errcode(pb::INPUT_PARAM_ERROR);
                        response->set_errmsg("get tso failed");
                        return;
                    }
                    DB_WARNING("txn_id:%lu region_id:%ld not found commit_ts in store, need get tso from meta ts:%ld",
                        txn_id, _region_id, commit_ts);
                }
                txn_res->set_commit_ts(commit_ts);
            }
            // secondary执行COMMIT
            response->set_errcode(pb::SUCCESS);
            txn_res->set_txn_state(pb::TXN_COMMITTED);
            return;
        }
    }
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
    int ret = 0;
    const auto& remote_side_tmp = butil::endpoint2str(cntl->remote_side());
    const char* remote_side = remote_side_tmp.c_str();

    pb::OpType op_type = request->op_type();
    const pb::TransactionInfo& txn_info = request->txn_infos(0);
    uint64_t txn_id = txn_info.txn_id();
    int seq_id = txn_info.seq_id();
    SmartTransaction txn = _txn_pool.get_txn(txn_id);
    // seq_id within a transaction should be continuous regardless of failure or success
    int last_seq = (txn == nullptr)? 0 : txn->seq_id();

    if (txn_info.need_update_primary_timestamp()) {
        _txn_pool.update_primary_timestamp(txn_info);
    }

    if (txn == nullptr) {
        ret = _meta_writer->read_transcation_rollbacked_tag(_region_id, txn_id);
        if (ret == 0) {
            //查询meta存在说明已经ROLLBACK，baikaldb直接返回TXN_IS_ROLLBACK错误码
            DB_FATAL("TransactionError: txn has been rollbacked due to timeout, remote_side:%s, "
                "region_id: %ld, txn_id: %lu, log_id:%lu op_type: %s",
            remote_side, _region_id, txn_id, log_id, pb::OpType_Name(op_type).c_str());
            response->set_errcode(pb::TXN_IS_ROLLBACK);
            response->set_affected_rows(0);
            return;
        }
        // 事务幂等处理
        // 拦截事务结束后由于core，切主，超时等原因导致的事务重发
        int finish_affected_rows = _txn_pool.get_finished_txn_affected_rows(txn_id);
        if (finish_affected_rows != -1) {
            DB_FATAL("TransactionError: txn has exec before, remote_side:%s, "
                    "region_id: %ld, txn_id: %lu, log_id:%lu op_type: %s",
                remote_side, _region_id, txn_id, log_id, pb::OpType_Name(op_type).c_str());
            response->set_affected_rows(finish_affected_rows);
            response->set_errcode(pb::SUCCESS);
            return;
        }
        if (op_type == pb::OP_ROLLBACK) {
            // old leader状态机外执行失败后切主
            DB_WARNING("TransactionWarning: txn not exist, remote_side:%s, "
                    "region_id: %ld, txn_id: %lu, log_id:%lu op_type: %s",
                remote_side, _region_id, txn_id, log_id, pb::OpType_Name(op_type).c_str());
            response->set_affected_rows(0);
            response->set_errcode(pb::SUCCESS);
            return;
        }
    } else if (txn_info.start_seq_id() != 1 && last_seq >= seq_id) {
        // 事务幂等处理，多线程等原因，并不完美
        // 拦截事务过程中由于超时导致的事务重发
        // leader切换会重放最后一条DML
        DB_WARNING("TransactionWarning: txn has exec before, remote_side:%s "
                "region_id: %ld, txn_id: %lu, op_type: %s, last_seq:%d, seq_id:%d log_id:%lu",
            remote_side, _region_id, txn_id, pb::OpType_Name(op_type).c_str(), last_seq, seq_id, log_id);
        txn->swap_last_response(*response);
        response->set_affected_rows(txn->dml_num_affected_rows);
        response->set_errcode(txn->err_code);
        return;
    }
    
    if (txn != nullptr) {
        if (!txn->txn_set_process_cas(false, true) && !txn->is_finished()) {
            DB_WARNING("TransactionNote: txn in process remote_side:%s "
                    "region_id: %ld, txn_id: %lu, op_type: %s log_id:%lu",
                    remote_side, _region_id, txn_id, pb::OpType_Name(op_type).c_str(), log_id);
            response->set_affected_rows(0);
            response->set_errcode(pb::IN_PROCESS);
            return;
        }
        if (txn->is_finished()) {
            DB_WARNING("TransactionWarning: txn is_finished, remote_side:%s "
                    "region_id: %ld, txn_id: %lu, op_type: %s, last_seq:%d, seq_id:%d log_id:%lu",
                    remote_side, _region_id, txn_id, pb::OpType_Name(op_type).c_str(), last_seq, seq_id, log_id);
            response->set_affected_rows(txn->dml_num_affected_rows);
            response->set_errcode(txn->err_code);
            return;
        }
    }
    // read-only事务不提交raft log，直接prepare/commit/rollback
    if (txn_info.start_seq_id() != 1 && !txn_info.has_from_store() &&
        (op_type == pb::OP_ROLLBACK || op_type == pb::OP_COMMIT || op_type == pb::OP_PREPARE)) {
        if (txn != nullptr && !txn->has_dml_executed()) {
            bool optimize_1pc = txn_info.optimize_1pc();
            _txn_pool.read_only_txn_process(_region_id, txn, op_type, optimize_1pc);
            txn->set_in_process(false);
            response->set_affected_rows(0);
            response->set_errcode(pb::SUCCESS);
            // DB_WARNING("TransactionNote: no write DML when commit/rollback, remote_side:%s "
            //         "region_id: %ld, txn_id: %lu, op_type: %s log_id:%lu optimize_1pc:%d",
            //         remote_side, _region_id, txn_id, pb::OpType_Name(op_type).c_str(), log_id, optimize_1pc);
            return;
        }
    }
    // for tail splitting new region replay txn
    if (request->has_start_key() && !request->start_key().empty()) {
        pb::RegionInfo region_info_mem;
        copy_region(&region_info_mem);
        region_info_mem.set_start_key(request->start_key());
        set_region_with_update_range(region_info_mem);
    }
    bool apply_success = true;
    ScopeGuard auto_rollback_current_request([this, &txn, txn_id, &apply_success]() {
        if (txn != nullptr && !apply_success) {
            txn->rollback_current_request();
            //DB_WARNING("region_id:%ld txn_id: %lu need rollback cur seq_id:%d", _region_id, txn->txn_id(), txn->seq_id());
            txn->set_in_process(false);
        }
    });
    int64_t expected_term = _expected_term;
    if (/*op_type != pb::OP_PREPARE && */last_seq < seq_id - 1) {
        ret = execute_cached_cmd(*request, *response, txn_id, txn, 0, 0, log_id);
        if (ret != 0) {
            apply_success = false;
            DB_FATAL("execute cached failed, region_id: %ld, txn_id: %lu log_id: %lu remote_side: %s",
                _region_id, txn_id, log_id, remote_side);
            return;
        }
    }

    // execute the current cmd
    // OP_BEGIN cmd is always cached
    switch (op_type) {
        case pb::OP_SELECT:
        case pb::OP_SELECT_FOR_UPDATE: {
            TimeCost cost;
            ret = select(*request, *response);
            int64_t select_cost = cost.get_time();
            Store::get_instance()->select_time_cost << select_cost;
            if (select_cost > FLAGS_print_time_us) {
                //担心ByteSizeLong对性能有影响，先对耗时长的，返回行多的请求做压缩
                if (response->affected_rows() > 1024) {
                    cntl->set_response_compress_type(brpc::COMPRESS_TYPE_SNAPPY);
                } else if (response->ByteSizeLong() > 1024 * 1024) {
                    cntl->set_response_compress_type(brpc::COMPRESS_TYPE_SNAPPY);
                }
                DB_NOTICE("select type: %s, region_id: %ld, txn_id: %lu, seq_id: %d, "
                        "time_cost: %ld, log_id: %lu, sign: %lu, rows: %ld, scan_rows: %ld, remote_side: %s",
                        pb::OpType_Name(request->op_type()).c_str(), _region_id, txn_id, seq_id, 
                        cost.get_time(), log_id, request->sql_sign(),
                        response->affected_rows(), response->scan_rows(), remote_side);
            }
            if (txn != nullptr) {
                txn->select_update_txn_status(seq_id);
            }
            if (op_type == pb::OP_SELECT_FOR_UPDATE && ret != -1) {
                butil::IOBuf data;
                butil::IOBufAsZeroCopyOutputStream wrapper(&data);
                if (!request->SerializeToZeroCopyStream(&wrapper)) {
                    apply_success = false;
                    cntl->SetFailed(brpc::EREQUEST, "Fail to serialize request");
                    return;
                }
                DMLClosure* c = new DMLClosure;
                c->cost.reset();
                c->op_type = op_type;
                c->log_id = log_id;
                c->response = response;
                c->done = done_guard.release();
                c->region = this;
                c->transaction = txn;
                c->remote_side = remote_side;
                braft::Task task;
                task.data = &data;
                task.done = c;
                task.expected_term = expected_term;
                int64_t disable_write_wait = get_split_wait_time();
                ret = _disable_write_cond.timed_wait(disable_write_wait);
                if (ret != 0) {
                    apply_success = false;
                    response->set_errcode(pb::DISABLE_WRITE_TIMEOUT);
                    response->set_errmsg("_disable_write_cond wait timeout");
                    DB_FATAL("_disable_write_cond wait timeout, ret:%d, region_id: %ld", ret, _region_id);
                    return;
                }
                if (txn != nullptr) {
                    txn->set_applying(true);
                }
                _real_writing_cond.increase();
                _node.apply(task);
            }
        }
        break;
        case pb::OP_INSERT:
        case pb::OP_DELETE:
        case pb::OP_UPDATE:
        case pb::OP_PREPARE:
        case pb::OP_ROLLBACK:
        case pb::OP_COMMIT: {
            if (_split_param.split_slow_down) {
                DB_WARNING("region is spliting, slow down time:%ld, region_id: %ld, txn_id: %lu:%d log_id:%lu remote_side: %s",
                            _split_param.split_slow_down_cost, _region_id, txn_id, seq_id, log_id, remote_side);
                bthread_usleep(_split_param.split_slow_down_cost);
            }
            //TODO
            int64_t disable_write_wait = get_split_wait_time();
            ret = _disable_write_cond.timed_wait(disable_write_wait);
            if (ret != 0) {
                apply_success = false;
                response->set_errcode(pb::DISABLE_WRITE_TIMEOUT);
                response->set_errmsg("_disable_write_cond wait timeout");
                DB_FATAL("_disable_write_cond wait timeout, ret:%d, region_id: %ld txn_id: %lu:%d log_id:%lu remote_side: %s",
                         ret, _region_id, txn_id, seq_id, log_id, remote_side);
                return;
            }
            _real_writing_cond.increase();
            ScopeGuard auto_decrease([this]() {
                _real_writing_cond.decrease_signal();
            });
            
            // double check，防止写不一致
            if (!is_leader()) {
                apply_success = false;
                response->set_errcode(pb::NOT_LEADER);
                response->set_leader(butil::endpoint2str(get_leader()).c_str());
                response->set_errmsg("not leader");
                DB_WARNING("not leader old version, leader:%s, region_id: %ld, txn_id: %lu:%d log_id:%lu remote_side: %s",
                        butil::endpoint2str(get_leader()).c_str(), _region_id, txn_id, seq_id, log_id, remote_side);
                return;
            }
            if (validate_version(request, response) == false) {
                apply_success = false;
                DB_WARNING("region version too old, region_id: %ld, log_id:%lu,"
                           " request_version:%ld, region_version:%ld",
                            _region_id, log_id, request->region_version(), get_version());
                return;
            }
            pb::StoreReq* raft_req = nullptr;
            if (is_dml_op_type(op_type)) {
                dml(*request, *response, (int64_t)0, (int64_t)0, false);
                if (response->errcode() != pb::SUCCESS) {
                    apply_success = false;
                    DB_WARNING("dml exec failed, region_id: %ld txn_id: %lu:%d log_id:%lu remote_side: %s",
                        _region_id, txn_id, seq_id, log_id, remote_side);
                    return;
                }
                if (txn != nullptr && txn->is_separate()) {
                    raft_req = txn->get_raftreq();
                    raft_req->clear_txn_infos();
                    pb::TransactionInfo* kv_txn_info = raft_req->add_txn_infos();
                    kv_txn_info->CopyFrom(txn_info);
                    raft_req->set_op_type(pb::OP_KV_BATCH);
                    raft_req->set_region_id(_region_id);
                    raft_req->set_region_version(_version);
                    raft_req->set_num_increase_rows(txn->num_increase_rows);
                }
            } else if (op_type == pb::OP_PREPARE && txn != nullptr && !txn->has_dml_executed()) {
                bool optimize_1pc = txn_info.optimize_1pc();
                _txn_pool.read_only_txn_process(_region_id, txn, op_type, optimize_1pc);
                txn->set_in_process(false);
                response->set_affected_rows(0);
                response->set_errcode(pb::SUCCESS);
                DB_WARNING("TransactionNote: no write DML when commit/rollback, remote_side:%s "
                        "region_id: %ld, txn_id: %lu, op_type: %s log_id:%lu optimize_1pc:%d",
                        remote_side, _region_id, txn_id, pb::OpType_Name(op_type).c_str(), log_id, optimize_1pc);
                return;
            }
            butil::IOBuf data;
            butil::IOBufAsZeroCopyOutputStream wrapper(&data);
            int ret = 0;
            if (raft_req == nullptr) {
                ret = request->SerializeToZeroCopyStream(&wrapper);
            } else {
                ret = raft_req->SerializeToZeroCopyStream(&wrapper);
            }
            if (ret < 0) {
                apply_success = false;
                cntl->SetFailed(brpc::EREQUEST, "Fail to serialize request");
                return;
            }
            DMLClosure* c = new DMLClosure;
            c->cost.reset();
            c->op_type = op_type;
            c->log_id = log_id;
            c->response = response;
            c->done = done_guard.release();
            c->region = this;
            c->transaction = txn;
            if (txn != nullptr) {
                txn->set_applying(true);
                c->is_separate = txn->is_separate();
            }
            c->remote_side = remote_side;
            braft::Task task;
            task.data = &data;
            task.done = c;
            task.expected_term = expected_term;
            auto_decrease.release();
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
    const auto& remote_side_tmp = butil::endpoint2str(cntl->remote_side());
    const char* remote_side = remote_side_tmp.c_str();
    pb::OpType op_type = request->op_type();
    switch (op_type) {
        // OP_SELECT_FOR_UPDATE 只出现在事务中。
        case pb::OP_SELECT: {
            TimeCost cost;
            select(*request, *response);
            int64_t select_cost = cost.get_time();
            Store::get_instance()->select_time_cost << select_cost;
            if (select_cost > FLAGS_print_time_us) {
                //担心ByteSizeLong对性能有影响，先对耗时长的，返回行多的请求做压缩
                if (response->affected_rows() > 1024) {
                    cntl->set_response_compress_type(brpc::COMPRESS_TYPE_SNAPPY);
                } else if (response->ByteSizeLong() > 1024 * 1024) {
                    cntl->set_response_compress_type(brpc::COMPRESS_TYPE_SNAPPY);
                }
                DB_NOTICE("select type: %s, region_id: %ld, txn_id: %lu, seq_id: %d, "
                        "time_cost: %ld, log_id: %lu, sign: %lu, rows: %ld, scan_rows: %ld, remote_side: %s",
                        pb::OpType_Name(request->op_type()).c_str(), _region_id, (uint64_t)0, 0, 
                        cost.get_time(), log_id, request->sql_sign(),
                        response->affected_rows(), response->scan_rows(), remote_side);
            }
            break;
        }
        case pb::OP_KILL:
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
            if (ret != 0) {
                response->set_errcode(pb::DISABLE_WRITE_TIMEOUT);
                response->set_errmsg("_diable_write_cond wait timeout");
                DB_FATAL("_diable_write_cond wait timeout, ret:%d, region_id: %ld", ret, _region_id);
                return;
            }
            _real_writing_cond.increase();
            ScopeGuard auto_decrease([this]() {
                _real_writing_cond.decrease_signal();
            });

            // double check，防止写不一致
            if (!is_leader()) {
                response->set_errcode(pb::NOT_LEADER);
                response->set_leader(butil::endpoint2str(get_leader()).c_str());
                response->set_errmsg("not leader");
                DB_WARNING("not leader old version, leader:%s, region_id: %ld, log_id:%lu",
                        butil::endpoint2str(get_leader()).c_str(), _region_id, log_id);
                return;
            }
            if (validate_version(request, response) == false) {
                DB_WARNING("region version too old, region_id: %ld, log_id:%lu, "
                           "request_version:%ld, region_version:%ld",
                            _region_id, log_id, 
                            request->region_version(), get_version());
                return;
            }

            if (is_dml_op_type(op_type) && _storage_compute_separate) {
                //计算存储分离
                exec_kv_out_txn(request, response, remote_side, done_guard.release());
            } else {
                butil::IOBuf data;
                butil::IOBufAsZeroCopyOutputStream wrapper(&data);
                if (!request->SerializeToZeroCopyStream(&wrapper)) {
                    cntl->SetFailed(brpc::EREQUEST, "Fail to serialize request");
                    return;
                }
                DMLClosure* c = new DMLClosure;
                c->op_type = op_type;
                c->log_id = log_id;
                c->response = response;
                c->region = this;
                c->remote_side = remote_side;
                int64_t expected_term = _expected_term;
                if (is_dml_op_type(op_type) && _txn_pool.exec_1pc_out_fsm()) {
                    //DB_NOTICE("1PC out of fsm region_id: %ld log_id:%lu", _region_id, log_id);
                    dml_1pc(*request, request->op_type(), request->plan(), request->tuples(), 
                        *response, 0, 0, static_cast<braft::Closure*>(c));
                    if (response->errcode() != pb::SUCCESS) {
                        DB_FATAL("dml exec failed, region_id: %ld log_id:%lu", _region_id, log_id);
                        delete c;
                        return;
                    }
                } else {
                    //DB_NOTICE("1PC in of fsm region_id: %ld log_id:%lu", _region_id, log_id);
                }
                c->cost.reset();
                c->done = done_guard.release();
                braft::Task task;
                task.data = &data;
                task.done = c;
                if (is_dml_op_type(op_type)) {
                    task.expected_term = expected_term;
                }
                auto_decrease.release();
                _node.apply(task);
            }
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

void Region::exec_kv_out_txn(const pb::StoreReq* request, 
                              pb::StoreRes* response, 
                              const char* remote_side,
                              google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    TimeCost cost;

    int ret = 0;
    uint64_t db_conn_id = request->db_conn_id();
    // 兼容旧baikaldb
    if (db_conn_id == 0) {
        db_conn_id = butil::fast_rand();
    }
    
    TimeCost compute_cost;
    SmartState state_ptr = std::make_shared<RuntimeState>();
    RuntimeState& state = *state_ptr;
    state.set_resource(get_resource());
    state.set_remote_side(remote_side);
    ret = state.init(*request, request->plan(), request->tuples(), &_txn_pool, true);
    if (ret < 0) {
        response->set_errcode(pb::EXEC_FAIL);
        response->set_errmsg("RuntimeState init fail");
        DB_FATAL("RuntimeState init fail, region_id: %ld", _region_id);
        return;
    }
    state.response = response;
    _state_pool.set(db_conn_id, state_ptr);
    ON_SCOPE_EXIT(([this, db_conn_id]() {
        _state_pool.remove(db_conn_id);
    }));
    
    state.create_txn_if_null(Transaction::TxnOptions());
    state.raft_func = [this] (RuntimeState* state, SmartTransaction txn) { 
        kv_apply_raft(state, txn); 
    };
    
    auto txn = state.txn();
    if (request->plan().nodes_size() <= 0) {
        return;
    }
    
    // for single-region autocommit and force-1pc cmd, exec the real dml cmd
    {
        BAIDU_SCOPED_LOCK(_reverse_index_map_lock);
        state.set_reverse_index_map(_reverse_index_map);
    }
    ExecNode* root = nullptr;
    ret = ExecNode::create_tree(request->plan(), &root);
    if (ret < 0) {
        ExecNode::destroy_tree(root);
        response->set_errcode(pb::EXEC_FAIL);
        response->set_errmsg("create plan fail");
        DB_FATAL("create plan fail, region_id: %ld, txn_id: %lu:%d", 
                 _region_id, state.txn_id, state.seq_id);
        return;
    }
    ret = root->open(&state);
    if (ret < 0) {
        root->close(&state);
        ExecNode::destroy_tree(root);
        response->set_errcode(pb::EXEC_FAIL);
        if (state.error_code != ER_ERROR_FIRST) {
            response->set_mysql_errcode(state.error_code);
            response->set_errmsg(state.error_msg.str());
        } else {
            response->set_errmsg("plan open fail");
        }
        if (state.error_code == ER_DUP_ENTRY) {
            DB_WARNING("plan open fail, region_id: %ld, txn_id: %lu:%d, "
                       "error_code: %d, mysql_errcode:%d", 
                       _region_id, state.txn_id, state.seq_id, 
                       state.error_code, state.error_code);
        } else {
            DB_FATAL("plan open fail, region_id: %ld, txn_id: %lu:%d, "
                     "error_code: %d, mysql_errcode:%d", 
                     _region_id, state.txn_id, state.seq_id, 
                     state.error_code, state.error_code);
        }
        return;
    }
    root->close(&state);
    ExecNode::destroy_tree(root);
    
    TimeCost storage_cost;
    kv_apply_raft(&state, txn);
    
    //等待所有raft执行完成
    state.txn_cond.wait();
    
    if (response->errcode() == pb::SUCCESS) {
        response->set_affected_rows(ret);
        response->set_errcode(pb::SUCCESS);
    } else if (response->errcode() == pb::NOT_LEADER) {
        response->set_leader(butil::endpoint2str(get_leader()).c_str());
        DB_WARNING("not leader, region_id: %ld, error_msg:%s", 
                 _region_id, response->errmsg().c_str());
    } else {
        response->set_errcode(pb::EXEC_FAIL);
        DB_FATAL("txn commit failed, region_id: %ld, error_msg:%s", 
                 _region_id, response->errmsg().c_str());
    }
    
    int64_t dml_cost = cost.get_time();
    Store::get_instance()->dml_time_cost << dml_cost;
    _dml_time_cost << dml_cost;
    if (dml_cost > FLAGS_print_time_us) {
        DB_NOTICE("region_id: %ld, txn_id: %lu, num_table_lines:%ld, "
                  "affected_rows:%d, log_id:%lu,"
                  "compute_cost:%ld, storage_cost:%ld, dml_cost:%ld", 
                  _region_id, state.txn_id, _num_table_lines.load(), ret, 
                  state.log_id(), compute_cost.get_time(),
                  storage_cost.get_time(), dml_cost);
    }
}

DEFINE_int32(not_leader_alarm_print_interval_s, 60, "not leader alarm print interval(s)");
// 处理not leader 报警
// 每个region单独聚合打印报警日志，noah聚合所有region可能会误报
void Region::NotLeaderAlarm::not_leader_alarm(const braft::PeerId& leader_id) {
    // leader不是自己, 不报警
    if (!leader_id.is_empty() && leader_id != node_id) {
        reset();
        return;
    }

    // 初始状态reset更新时间
    if (type == ALARM_INIT) {
        reset();
    }

    if (leader_id.is_empty()) {
        // leader是0.0.0.0:0:0
        type = LEADER_INVALID;
    } else if (leader_id == node_id && !leader_start) {
        // leader是自己,但是没有调用on_leader_start,raft卡住
        type = LEADER_RAFT_FALL_BEHIND;
    } else if (leader_id == node_id && leader_start) {
        // leader是自己，没有real start
        type = LEADER_NOT_REAL_START;
    }

    total_count++;
    interval_count++;

    // 周期聚合,打印报警日志
    if (last_print_time.get_time() > FLAGS_not_leader_alarm_print_interval_s * 1000 * 1000L) {
        DB_FATAL("region_id: %ld, not leader. alarm_type: %d, duration %ld us, interval_count: %d, total_count: %d",
            region_id, static_cast<int>(type), alarm_begin_time.get_time(), interval_count.load(), total_count.load());

        last_print_time.reset();
        interval_count = 0;
    }

}

void Region::async_apply_log_entry(google::protobuf::RpcController* controller,
                                   const pb::BatchStoreReq* request,
                                   pb::BatchStoreRes* response,
                                   google::protobuf::Closure* done) {
    if (request == nullptr || response == nullptr) {
        return;
    }
    // stop流程最后join brpc，所以请求可能没处理完region就析构了
    _multi_thread_cond.increase();
    ON_SCOPE_EXIT([this]() {
        _multi_thread_cond.decrease_signal();
    });
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = (brpc::Controller*)controller;
    if (cntl == nullptr) {
        return;
    }
    int64_t expect_term = _expected_term;
    const auto& remote_side_tmp = butil::endpoint2str(cntl->remote_side());
    const char* remote_side = remote_side_tmp.c_str();
    uint64_t log_id = 0;
    int64_t  apply_idx = 0;
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    if (!is_leader()) {
        // 非leader才返回
        response->set_leader(butil::endpoint2str(get_leader()).c_str());
        response->set_errcode(pb::NOT_LEADER);
        response->set_errmsg("not leader");
        response->set_success_cnt(request->resend_start_pos());
        DB_WARNING("not leader alarm_type: %d, leader:%s, region_id: %ld, log_id:%lu, remote_side:%s",
                   _not_leader_alarm.type, butil::endpoint2str(get_leader()).c_str(),
                   _region_id, log_id, remote_side);
        return;
    }
    if (_async_apply_param.apply_log_failed) {
        // 之前丢进异步队列里面的dml执行失败了，分裂直接失败
        response->set_errcode(pb::EXEC_FAIL);
        response->set_errmsg("exec fail in executionQueue");
        DB_WARNING("exec fail in executionQueue, region_id: %ld, log_id:%lu, remote_side:%s",
                   _region_id, log_id, remote_side);
        return;
    }
    if (get_version() != 0) {
        response->set_errcode(pb::VERSION_OLD);
        response->set_errmsg("not allowed");
        DB_WARNING("not allowed fast_apply_log_entry, region_id: %ld, version:%ld, log_id:%lu, remote_side:%s",
                    _region_id, get_version(), log_id, remote_side);
        return;
    }                                   
    std::vector<pb::StoreRes> store_responses(request->request_lens_size());
    BthreadCond on_apply_cond;
    // Parse request
    butil::IOBuf data_buf;
    data_buf.swap(cntl->request_attachment());
    for (apply_idx = 0; apply_idx < request->request_lens_size(); ++apply_idx) {
        butil::IOBuf data;
        data_buf.cutn(&data, request->request_lens(apply_idx));
        if (apply_idx < request->resend_start_pos()) {
            continue;
        }
        if (_async_apply_param.apply_log_failed) {
            // 之前丢进异步队列里面的dml执行失败了，分裂直接失败
            store_responses[apply_idx].set_errcode(pb::EXEC_FAIL);
            store_responses[apply_idx].set_errmsg("exec fail in executionQueue");
            DB_WARNING("exec fail in executionQueue, region_id: %ld, log_id:%lu, remote_side:%s",
                       _region_id, log_id, remote_side);
            break;
        }

        if (!is_leader()) {
            // 非leader才返回
            store_responses[apply_idx].set_errcode(pb::NOT_LEADER);
            store_responses[apply_idx].set_errmsg("not leader");
            DB_WARNING("not leader alarm_type: %d, leader:%s, region_id: %ld, log_id:%lu, remote_side:%s",
                       _not_leader_alarm.type, butil::endpoint2str(get_leader()).c_str(),
                       _region_id, log_id, remote_side);
            break;
        }
        // 只有分裂的时候采用这个rpc, 此时新region version一定是0, 分裂直接失败
        if (get_version() != 0) {
            store_responses[apply_idx].set_errcode(pb::VERSION_OLD);
            store_responses[apply_idx].set_errmsg("not allowed");
            DB_WARNING("not allowed fast_apply_log_entry, region_id: %ld, version:%ld, log_id:%lu, remote_side:%s",
                       _region_id, get_version(), log_id, remote_side);
            break;
        }
        // 异步执行走follower逻辑，DMLClosure不传transaction
        // 减少序列化/反序列化开销，DMLClosure不传op_type
        DMLClosure *c = new DMLClosure(&on_apply_cond);
        on_apply_cond.increase();
        c->cost.reset();
        c->log_id = log_id;
        c->response = &store_responses[apply_idx];
        c->done = nullptr;
        c->region = this;
        c->remote_side = remote_side;
        braft::Task task;
        task.data = &data;
        task.done = c;
        c->is_sync = true;
        task.expected_term = expect_term;
        _real_writing_cond.increase();
        _node.apply(task);
    }
    on_apply_cond.wait();

    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("success");

    // 判断是否有not leader
    int64_t success_applied_cnt = request->resend_start_pos();
    for (; success_applied_cnt < request->request_lens_size(); ++success_applied_cnt) {
        if (store_responses[success_applied_cnt].errcode() != pb::SUCCESS) {
            response->set_errcode(store_responses[success_applied_cnt].errcode());
            response->set_errmsg(store_responses[success_applied_cnt].errmsg());
            break;
        }
    }
    response->set_leader(butil::endpoint2str(get_leader()).c_str());
    response->set_success_cnt(success_applied_cnt);
    response->set_applied_index(_applied_index);
    response->set_braft_applied_index(_braft_apply_index);
    response->set_dml_latency(get_dml_latency());
    _async_apply_param.start_adjust_stall();
    return;
}

void Region::query(google::protobuf::RpcController* controller,
                   const pb::StoreReq* request,
                   pb::StoreRes* response,
                   google::protobuf::Closure* done) {
    // stop流程最后join brpc，所以请求可能没处理完region就析构了
    _multi_thread_cond.increase();
    ON_SCOPE_EXIT([this]() {
        _multi_thread_cond.decrease_signal();
    });
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = (brpc::Controller*)controller;
    uint64_t log_id = 0;
    if (cntl->has_log_id()) { 
        log_id = cntl->log_id();
    }
    if (request->op_type() == pb::OP_TXN_QUERY_STATE) {
        exec_txn_query_state(controller, request, response, done_guard.release());
        return;
    } else if (request->op_type() == pb::OP_TXN_COMPLETE && request->force()) {
        exec_txn_complete(controller, request, response, done_guard.release());
        return;
    }
    const auto& remote_side_tmp = butil::endpoint2str(cntl->remote_side());
    const char* remote_side = remote_side_tmp.c_str();
    if (!is_leader()) {
        if (!is_learner()) {
            _not_leader_alarm.not_leader_alarm(_node.leader_id());
        }
        // 非leader才返回
        response->set_leader(butil::endpoint2str(get_leader()).c_str());
        //为了性能，支持非一致性读
        if (request->select_without_leader() && is_learner() && !learner_ready_for_read()) {
            response->set_errcode(pb::LEARNER_NOT_READY);
            response->set_errmsg("learner not ready");
            DB_WARNING("not leader alarm_type: %d, leader:%s, region_id: %ld, log_id:%lu, remote_side:%s",
                    _not_leader_alarm.type, butil::endpoint2str(get_leader()).c_str(), 
                    _region_id, log_id, remote_side);
            return;
        }
        if (!request->select_without_leader() || _shutdown || !_init_success || 
            (is_learner() && !learner_ready_for_read())) {
            response->set_errcode(pb::NOT_LEADER);
            response->set_errmsg("not leader");
            DB_WARNING("not leader alarm_type: %d, leader:%s, region_id: %ld, log_id:%lu, remote_side:%s",
                    _not_leader_alarm.type, butil::endpoint2str(get_leader()).c_str(), 
                    _region_id, log_id, remote_side);
            return;
        }

    }
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
                   " request_version:%ld, region_version:%ld optype:%s remote_side:%s",
                    _region_id, log_id, 
                    request->region_version(), get_version(),
                    pb::OpType_Name(request->op_type()).c_str(), remote_side);
        return;
    }
    // 启动时，或者follow落后太多，需要读leader
    if (request->op_type() == pb::OP_SELECT && request->region_version() > get_version()) {
        response->set_errcode(pb::NOT_LEADER);
        response->set_leader(butil::endpoint2str(get_leader()).c_str());
        response->set_errmsg("not leader");
        DB_WARNING("not leader, leader:%s, region_id: %ld, version:%ld, log_id:%lu, remote_side:%s",
                        butil::endpoint2str(get_leader()).c_str(), 
                        _region_id, get_version(), log_id, remote_side);
        return;
    }
    // int ret = 0;
    // TimeCost cost;
    switch (request->op_type()) {
        case pb::OP_KILL:
            exec_out_txn_query(controller, request, response, done_guard.release());
            break;
        case pb::OP_TXN_QUERY_PRIMARY_REGION:
            exec_txn_query_primary_region(controller, request, response, done_guard.release());
            break;
        case pb::OP_TXN_COMPLETE:
            exec_txn_complete(controller, request, response, done_guard.release());
            break;
        case pb::OP_SELECT:
        case pb::OP_INSERT:
        case pb::OP_DELETE:
        case pb::OP_UPDATE:
        case pb::OP_PREPARE:
        case pb::OP_COMMIT:
        case pb::OP_ROLLBACK:
        case pb::OP_TRUNCATE_TABLE:
        case pb::OP_SELECT_FOR_UPDATE: {
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
        case pb::OP_UPDATE_PRIMARY_TIMESTAMP:
        case pb::OP_NONE: {
            if (request->op_type() == pb::OP_NONE) {
                if (_split_param.split_slow_down) {
                    DB_WARNING("region is spliting, slow down time:%ld, region_id: %ld, remote_side: %s",
                                _split_param.split_slow_down_cost, _region_id, remote_side);
                    bthread_usleep(_split_param.split_slow_down_cost);
                }
                //TODO
                int64_t disable_write_wait = get_split_wait_time();
                int ret = _disable_write_cond.timed_wait(disable_write_wait);
                if (ret != 0) {
                    response->set_errcode(pb::DISABLE_WRITE_TIMEOUT);
                    response->set_errmsg("_disable_write_cond wait timeout");
                    DB_FATAL("_disable_write_cond wait timeout, ret:%d, region_id: %ld", ret, _region_id);
                    return;
                }
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
            c->log_id = log_id;
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
        case pb::OP_ADJUSTKEY_AND_ADD_VERSION: {
            adjustkey_and_add_version_query(controller, 
                                            request, 
                                            response, 
                                            done_guard.release());
            break;
        }
        default:
            response->set_errcode(pb::UNSUPPORT_REQ_TYPE);
            response->set_errmsg("unsupport request type");
            DB_WARNING("not support op_type when dml request,op_type:%s region_id: %ld, log_id:%lu",
                        pb::OpType_Name(request->op_type()).c_str(), _region_id, log_id);
    }
    return;
}

void Region::dml(const pb::StoreReq& request, pb::StoreRes& response,
                 int64_t applied_index, int64_t term, bool need_txn_limit) {
    bool optimize_1pc = false;
    int32_t seq_id = 0;
    if (request.txn_infos_size() > 0) {
        optimize_1pc = request.txn_infos(0).optimize_1pc();
        seq_id = request.txn_infos(0).seq_id();
    }
    if ((request.op_type() == pb::OP_PREPARE) && optimize_1pc) {
        dml_1pc(request, request.op_type(), request.plan(), request.tuples(), 
            response, applied_index, term, nullptr);
    } else {
        dml_2pc(request, request.op_type(), request.plan(), request.tuples(), 
            response, applied_index, term, seq_id, need_txn_limit);
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
        int32_t seq_id, bool need_txn_limit) {
    QosType type = QOS_DML;
    uint64_t sign = 0;
    if (request.has_sql_sign()) {
        sign = request.sql_sign();
    }
    
    int64_t index_id = 0;
    StoreQos::get_instance()->create_bthread_local(type, sign, index_id);
    ON_SCOPE_EXIT(([this]() {
        StoreQos::get_instance()->destroy_bthread_local();
    }));
    // 只有leader有事务情况才能在raft外执行
    if (applied_index == 0 && term == 0 && !is_leader()) {
        // 非leader才返回
        response.set_leader(butil::endpoint2str(get_leader()).c_str());
        response.set_errcode(pb::NOT_LEADER);
        response.set_errmsg("not leader");
        DB_WARNING("not in raft, not leader, leader:%s, region_id: %ld, log_id:%lu",
                butil::endpoint2str(get_leader()).c_str(), _region_id, request.log_id());
        return;
    }

    TimeCost cost;
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
    uint64_t rocksdb_txn_id = 0;
    auto txn = _txn_pool.get_txn(txn_id);
    // txn may be rollback by transfer leader thread
    if (op_type != pb::OP_BEGIN && (txn == nullptr || txn->is_rolledback())) {
        response.set_errcode(pb::NOT_LEADER);
        response.set_leader(butil::endpoint2str(get_leader()).c_str());
        response.set_errmsg("not leader, maybe transfer leader");
        DB_WARNING("no txn found: region_id: %ld, txn_id: %lu:%d, applied_index: %ld-%ld op_type: %d",
            _region_id, txn_id, seq_id, term, applied_index, op_type);
        return;
    }
    bool need_write_rollback = false;
    if (op_type != pb::OP_BEGIN && txn != nullptr) {
        // rollback already executed cmds
        for (auto it = need_rollback_seq.rbegin(); it != need_rollback_seq.rend(); ++it) {
            int seq = *it;
            txn->rollback_to_point(seq);
            // DB_WARNING("rollback seq_id: %d region_id: %ld, txn_id: %lu, seq_id: %d, req_seq: %d", 
            //     seq, _region_id, txn_id, txn->seq_id(), seq_id);
        }
        // if current cmd need rollback, simply not execute
        if (need_rollback_seq.count(seq_id) != 0) {
            DB_WARNING("need rollback, not executed and cached. region_id: %ld, txn_id: %lu, seq_id: %d, req_seq: %d",
                _region_id, txn_id, txn->seq_id(), seq_id);
            txn->set_seq_id(seq_id);
            return;
        }
        // 提前更新txn的当前seq_id，防止dml执行失败导致seq_id更新失败
        // 而导致当前region为follow_up, 每次都需要从baikaldb拉取cached命令
        txn->set_seq_id(seq_id);
        // set checkpoint for current DML operator
        if (op_type != pb::OP_PREPARE && op_type != pb::OP_COMMIT && op_type != pb::OP_ROLLBACK) {
            txn->set_save_point();
        }
        // 提前保存txn->num_increase_rows，以便事务提交/回滚时更新num_table_lines
        if (op_type == pb::OP_COMMIT) {
            txn_num_increase_rows = txn->num_increase_rows;
        }
        if (txn_info.has_primary_region_id()) {
            txn->set_primary_region_id(txn_info.primary_region_id());
        }
        need_write_rollback = txn->need_write_rollback(op_type);
        rocksdb_txn_id = txn->rocksdb_txn_id();
    }

    int ret = 0;
    uint64_t db_conn_id = request.db_conn_id();
    if (db_conn_id == 0) {
        db_conn_id = butil::fast_rand();
    }
    if (op_type == pb::OP_COMMIT || op_type == pb::OP_ROLLBACK) { 
        int64_t num_table_lines = _num_table_lines;
        if (op_type == pb::OP_COMMIT) {
            num_table_lines += txn_num_increase_rows; 
        }
        _commit_meta_mutex.lock();  
        _meta_writer->write_pre_commit(_region_id, txn_id, num_table_lines, applied_index);
        //DB_WARNING("region_id: %ld lock and write_pre_commit success,"
        //            " num_table_lines: %ld, applied_index: %ld , txn_id: %lu, op_type: %s",
        //            _region_id, num_table_lines, applied_index, txn_id, pb::OpType_Name(op_type).c_str());
    }
    ON_SCOPE_EXIT(([this, op_type, applied_index, txn_id, txn_info, need_write_rollback]() {
        if (op_type == pb::OP_COMMIT || op_type == pb::OP_ROLLBACK) {
            auto ret = _meta_writer->write_meta_after_commit(_region_id, _num_table_lines,
                            applied_index, _data_index, txn_id, need_write_rollback);
            //DB_WARNING("write meta info wheen commit or rollback,"
            //            " region_id: %ld, applied_index: %ld, num_table_line: %ld, txn_id: %lu"
            //            "op_type: %s", 
            //            _region_id, applied_index, _num_table_lines.load(), 
            //            txn_id, pb::OpType_Name(op_type).c_str()); 
            if (ret < 0) {
                DB_FATAL("write meta info fail, region_id: %ld, txn_id: %lu, log_index: %ld", 
                            _region_id, txn_id, applied_index);
            }
            if (txn_info.has_open_binlog() && txn_info.open_binlog() && op_type == pb::OP_COMMIT
                && txn_info.primary_region_id() == _region_id) {
                put_commit_ts(txn_id, txn_info.commit_ts());
            }
            //DB_WARNING("region_id: %ld relase commit meta mutex,"
            //            "applied_index: %ld , txn_id: %lu",
            //            _region_id, applied_index, txn_id);
            _commit_meta_mutex.unlock();
        }        
    }));
    SmartState state_ptr = std::make_shared<RuntimeState>();
    RuntimeState& state = *state_ptr;
    state.set_resource(get_resource());
    bool is_separate = _storage_compute_separate;
    if (is_dml_op_type(op_type) && _factory->has_fulltext_index(get_table_id())) {
        is_separate = false;
    }
    ret = state.init(request, plan, tuples, &_txn_pool, is_separate);
    if (ret < 0) {
        response.set_errcode(pb::EXEC_FAIL);
        response.set_errmsg("RuntimeState init fail");
        DB_FATAL("RuntimeState init fail, region_id: %ld, txn_id: %lu", _region_id, txn_id);
        return;
    }
    state.need_condition_again = (applied_index > 0) ? false : true;
    state.need_txn_limit = need_txn_limit;
    _state_pool.set(db_conn_id, state_ptr);
    ON_SCOPE_EXIT(([this, db_conn_id]() {
        _state_pool.remove(db_conn_id);
    }));
    if (seq_id > 0) {
        // when executing cache query, use the seq_id of corresponding cache query (passed by user)
        state.seq_id = seq_id;
    }
    {
        BAIDU_SCOPED_LOCK(_reverse_index_map_lock);
        state.set_reverse_index_map(_reverse_index_map);
    }
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
        if (txn != nullptr) {
            txn->err_code = pb::EXEC_FAIL;
        }
        if (state.error_code != ER_ERROR_FIRST) {
            response.set_mysql_errcode(state.error_code);
            response.set_errmsg(state.error_msg.str());
        } else {
            response.set_errmsg("plan open failed");
        }
        if (state.error_code == ER_DUP_ENTRY) {
            DB_WARNING("plan open fail, region_id: %ld, txn_id: %lu:%d, "
                    "applied_index: %ld, error_code: %d, log_id:%lu, mysql_errcode:%d",
                    _region_id, state.txn_id, state.seq_id, applied_index,
                    state.error_code, state.log_id(), state.error_code);
        } else {
            DB_FATAL("plan open fail, region_id: %ld, txn_id: %lu:%d, "
                    "applied_index: %ld, error_code: %d, log_id:%lu mysql_errcode:%d",
                    _region_id, state.txn_id, state.seq_id, applied_index,
                    state.error_code, state.log_id(), state.error_code);
        }
        return;
    }
    int affected_rows = ret;

    auto& return_records = root->get_return_records();
    auto& return_old_records = root->get_return_old_records();
    for (auto& record_pair : return_records) {
        int64_t index_id = record_pair.first;
        auto r_pair = response.add_records();
        r_pair->set_local_index_binlog(root->local_index_binlog());
        r_pair->set_index_id(index_id);
        for (auto& record : record_pair.second) {
            auto r = r_pair->add_records();
            ret = record->encode(*r);
            if (ret < 0) {
                root->close(&state);
                ExecNode::destroy_tree(root);
                response.set_errcode(pb::EXEC_FAIL); 
                if (txn != nullptr) {
                    txn->err_code = pb::EXEC_FAIL;
                }
                response.set_errmsg("decode record failed");
                return;
            }
        }
        auto iter = return_old_records.find(index_id);
        if (iter != return_old_records.end()) {
            for (auto& record : iter->second) {
                auto r = r_pair->add_old_records();
                ret = record->encode(*r);
                if (ret < 0) {
                    root->close(&state);
                    ExecNode::destroy_tree(root);
                    response.set_errcode(pb::EXEC_FAIL); 
                    if (txn != nullptr) {
                        txn->err_code = pb::EXEC_FAIL;
                    }
                    response.set_errmsg("decode record failed");
                    return;
                }
            }
        }
    }
    if (txn != nullptr) {
        txn->err_code = pb::SUCCESS;
    }
    response.set_affected_rows(affected_rows);
    if (state.last_insert_id != INT64_MIN) {
        response.set_last_insert_id(state.last_insert_id);
    }
    response.set_scan_rows(state.num_scan_rows());
    response.set_errcode(pb::SUCCESS);

    txn = _txn_pool.get_txn(txn_id);
    if (txn != nullptr) {
        txn->set_seq_id(seq_id);
        txn->set_resource(state.resource());
        is_separate = txn->is_separate();
        // DB_WARNING("seq_id: %d, %d, op:%d", seq_id, plan_map.count(seq_id), op_type);
        // commit/rollback命令不加缓存
        if (op_type != pb::OP_COMMIT && op_type != pb::OP_ROLLBACK) {
            pb::CachePlan plan_item;
            plan_item.set_op_type(op_type);
            plan_item.set_seq_id(seq_id);
            if (is_dml_op_type(op_type) && txn->is_separate()) {
                pb::StoreReq* raft_req = txn->get_raftreq();
                plan_item.set_op_type(pb::OP_KV_BATCH);
                for (auto& kv_op : raft_req->kv_ops()) {
                    plan_item.add_kv_ops()->CopyFrom(kv_op);
                }
            } else {
                for (auto& tuple : tuples) {
                    plan_item.add_tuples()->CopyFrom(tuple);
                }
                plan_item.mutable_plan()->CopyFrom(plan);
            }
            txn->push_cmd_to_cache(seq_id, plan_item);
            //DB_WARNING("put txn cmd to cache: region_id: %ld, txn_id: %lu:%d", _region_id, txn_id, seq_id);
            txn->save_last_response(response);
        }
    } else if (op_type != pb::OP_COMMIT && op_type != pb::OP_ROLLBACK) {
        // after commit or rollback, txn will be deleted
        root->close(&state);
        ExecNode::destroy_tree(root);
        response.set_errcode(pb::NOT_LEADER);
        response.set_leader(butil::endpoint2str(get_leader()).c_str());
        response.set_errmsg("not leader, maybe transfer leader");
        DB_WARNING("no txn found: region_id: %ld, txn_id: %lu:%d, op_type: %d", _region_id, txn_id, seq_id, op_type);
        return;
    }
    if (/*txn_info.autocommit() && */(op_type == pb::OP_UPDATE || op_type == pb::OP_INSERT || op_type == pb::OP_DELETE)) {
        txn->dml_num_affected_rows = affected_rows;
    }

    root->close(&state);
    ExecNode::destroy_tree(root);

    if (op_type == pb::OP_TRUNCATE_TABLE) {
        ret = _num_table_lines;
        _num_table_lines = 0;
        // truncate后主动执行compact
        // DB_WARNING("region_id: %ld, truncate do compact in queue", _region_id);
        // compact_data_in_queue();
    } else if (op_type != pb::OP_COMMIT && op_type != pb::OP_ROLLBACK) {
        txn->num_increase_rows += state.num_increase_rows();
    } else if (op_type == pb::OP_COMMIT) {
        // 事务提交/回滚时更新num_table_line
        _num_table_lines += txn_num_increase_rows;
        if (txn_num_increase_rows < 0) {
            _num_delete_lines -= txn_num_increase_rows;
        }
    }

    int64_t dml_cost = cost.get_time();

    bool auto_commit = false;
    if (request.txn_infos_size() > 0) {
        auto_commit = request.txn_infos(0).autocommit();
    }
    if ((op_type == pb::OP_PREPARE) && auto_commit && txn != nullptr) {
        Store::get_instance()->dml_time_cost << (dml_cost + txn->get_exec_time_cost());
        _dml_time_cost << (dml_cost + txn->get_exec_time_cost());
    } else if (auto_commit && txn != nullptr) {
        txn->add_exec_time_cost(dml_cost);
    } else if (op_type == pb::OP_INSERT || op_type == pb::OP_DELETE || op_type == pb::OP_UPDATE) {
        Store::get_instance()->dml_time_cost << dml_cost;
        _dml_time_cost << dml_cost;
    }
    if (dml_cost > FLAGS_print_time_us ||
        //op_type == pb::OP_BEGIN ||
        //op_type == pb::OP_COMMIT ||
        //op_type == pb::OP_PREPARE ||
        op_type == pb::OP_ROLLBACK) {
        DB_NOTICE("dml type: %s, is_separate:%d time_cost:%ld, region_id: %ld, txn_id: %lu:%d:%lu, num_table_lines:%ld, "
                  "affected_rows:%d, applied_index:%ld, term:%ld, txn_num_rows:%ld,"
                  " log_id:%lu", 
                pb::OpType_Name(op_type).c_str(), is_separate, dml_cost, _region_id, txn_id, seq_id, rocksdb_txn_id,
                _num_table_lines.load(), affected_rows, applied_index, term, txn_num_increase_rows, 
                state.log_id());
    }
}

void Region::dml_1pc(const pb::StoreReq& request, pb::OpType op_type,
        const pb::Plan& plan, const RepeatedPtrField<pb::TupleDescriptor>& tuples, 
        pb::StoreRes& response, int64_t applied_index, int64_t term, braft::Closure* done) {
    //DB_WARNING("_num_table_lines:%ld region_id: %ld", _num_table_lines.load(), _region_id);
    QosType type = QOS_DML;
    uint64_t sign = 0;
    if (request.has_sql_sign()) {
        sign = request.sql_sign();
    } 
    int64_t index_id = 0;
    StoreQos::get_instance()->create_bthread_local(type, sign, index_id);
    ON_SCOPE_EXIT(([this]() {
        StoreQos::get_instance()->destroy_bthread_local();
    }));
    TimeCost cost;
    if (FLAGS_open_service_write_concurrency && (op_type == pb::OP_INSERT ||
        op_type == pb::OP_UPDATE ||
        op_type == pb::OP_DELETE)) {
        Concurrency::get_instance()->service_write_concurrency.increase_wait();
    }
    ON_SCOPE_EXIT([op_type]() {
            if (FLAGS_open_service_write_concurrency && (op_type == pb::OP_INSERT ||
                op_type == pb::OP_UPDATE ||
                op_type == pb::OP_DELETE)) {
                Concurrency::get_instance()->service_write_concurrency.decrease_broadcast();
            }
        });
    int64_t wait_cost = cost.get_time();
    pb::TraceNode trace_node;
    bool is_trace = false;
    if (request.has_is_trace()) {
        is_trace = request.is_trace();
    }
    if (is_trace) {
        trace_node.set_instance(_address);
        trace_node.set_region_id(_region_id);
        std::string desc = "dml_1pc";
        ScopeGuard auto_update_trace([&]() {
            desc += " " + pb::ErrCode_Name(response.errcode());
            trace_node.set_description(desc);
            trace_node.set_total_time(cost.get_time());
            std::string string_trace;
            if (response.errcode() == pb::SUCCESS) {
                if (!trace_node.SerializeToString(&string_trace)) {
                    DB_FATAL("trace_node: %s serialize to string fail",
                             trace_node.ShortDebugString().c_str());
                } else {
                    response.set_errmsg(string_trace);
                }
            }
        });
    }

    int ret = 0;
    uint64_t db_conn_id = request.db_conn_id();
    // 兼容旧baikaldb
    if (db_conn_id == 0) {
        db_conn_id = butil::fast_rand();
    }
    SmartState state_ptr = std::make_shared<RuntimeState>();
    RuntimeState& state = *state_ptr;
    state.set_resource(get_resource());
    ret = state.init(request, plan, tuples, &_txn_pool, false);
    if (ret < 0) {
        response.set_errcode(pb::EXEC_FAIL);
        response.set_errmsg("RuntimeState init fail");
        DB_FATAL("RuntimeState init fail, region_id: %ld, applied_index: %ld", 
                    _region_id, applied_index);
        return;
    }
    state.need_condition_again = (applied_index > 0) ? false : true;
    _state_pool.set(db_conn_id, state_ptr);
    ON_SCOPE_EXIT(([this, db_conn_id]() {
        _state_pool.remove(db_conn_id);
    }));
    // for out-txn dml query, create new txn.
    // for single-region 2pc query, simply fetch the txn created before.
    bool is_new_txn = !((request.op_type() == pb::OP_PREPARE) && request.txn_infos(0).optimize_1pc());
    if (is_new_txn) {
        Transaction::TxnOptions txn_opt;
        txn_opt.dml_1pc = is_dml_op_type(op_type);
        txn_opt.in_fsm = (done == nullptr);
        if (!txn_opt.in_fsm) {
            txn_opt.lock_timeout = FLAGS_exec_1pc_out_fsm_timeout_ms;
        }
        state.create_txn_if_null(txn_opt);
    }
    bool commit_succ = false;
    ScopeGuard auto_rollback([&]() {
        if (state.txn() == nullptr) {
            return;
        }
        // rollback if not commit succ
        if (!commit_succ) {
            state.txn()->rollback();
        }
        // if txn in pool (new_txn == false), remove it from pool
        // else directly delete it
        if (!is_new_txn) {
            _txn_pool.remove_txn(state.txn_id, true);
        }
    });
    auto txn = state.txn();
    if (txn == nullptr) {
        response.set_errcode(pb::EXEC_FAIL);
        response.set_errmsg("txn is null");
        DB_FATAL("some wrong txn is null, is_new_txn:%d region_id: %ld, term:%ld applied_index: %ld txn_id:%lu log_id:%lu",
                    is_new_txn, _region_id, term, applied_index, state.txn_id, state.log_id());
        return;
    }
    txn->set_resource(state.resource());
    if (!is_new_txn && request.txn_infos_size() > 0) {
        const pb::TransactionInfo& txn_info = request.txn_infos(0);
        int seq_id = txn_info.seq_id();
        std::set<int> need_rollback_seq;
        for (int rollback_seq : txn_info.need_rollback_seq()) {
            need_rollback_seq.insert(rollback_seq);
        }
        for (auto it = need_rollback_seq.rbegin(); it != need_rollback_seq.rend(); ++it) {
            int seq = *it;
            txn->rollback_to_point(seq);
            DB_WARNING("rollback seq_id: %d region_id: %ld, txn_id: %lu, "
                   "seq_id: %d, req_seq: %d", seq, _region_id, txn->txn_id(),
                   txn->seq_id(), seq_id);
        }
        txn->set_seq_id(seq_id);
    }
    int64_t tmp_num_table_lines = _num_table_lines;
    if (plan.nodes_size() > 0) {
        // for single-region autocommit and force-1pc cmd, exec the real dml cmd
        {
            BAIDU_SCOPED_LOCK(_reverse_index_map_lock);
            state.set_reverse_index_map(_reverse_index_map);
        }
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
        
        if (is_trace) {
            pb::TraceNode* root_trace = trace_node.add_child_nodes();
            root_trace->set_node_type(root->node_type());
            root->set_trace(root_trace);        
            root->create_trace();
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
            if (state.error_code == ER_DUP_ENTRY) {
                DB_WARNING("plan open fail, region_id: %ld, txn_id: %lu:%d, "
                        "applied_index: %ld, error_code: %d", 
                        _region_id, state.txn_id, state.seq_id, applied_index, 
                        state.error_code);
            } else if (state.error_code == ER_LOCK_WAIT_TIMEOUT && done == nullptr) { 
                response.set_errcode(pb::RETRY_LATER);
                DB_WARNING("1pc in fsm Lock timeout region_id: %ld, txn_id: %lu:%d"
                        "applied_index: %ld, error_code: %d", 
                        _region_id, state.txn_id, state.seq_id, applied_index, 
                        state.error_code);
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
        ret = tmp_num_table_lines;
        //全局索引行数返回0
        if (_is_global_index) {
            ret = 0;
        }
        tmp_num_table_lines = 0;
        // truncate后主动执行compact
        // DB_WARNING("region_id: %ld, truncate do compact in queue", _region_id);
        // compact_data_in_queue();
    }
    int64_t txn_num_increase_rows = txn->num_increase_rows;
    tmp_num_table_lines += txn_num_increase_rows;
    //follower 记录applied_index
    if (state.txn_id == 0 && done == nullptr) {
        _meta_writer->write_meta_index_and_num_table_lines(_region_id, applied_index, 
                _data_index, tmp_num_table_lines, txn);
        //DB_WARNING("write meta info when dml_1pc,"
        //            " region_id: %ld, num_table_line: %ld, applied_index: %ld", 
        //            _region_id, tmp_num_table_lines, applied_index);
    }
    if (state.txn_id != 0) {
        // pre_commit 与 commit 之间不能open snapshot
        _commit_meta_mutex.lock();
        _meta_writer->write_pre_commit(_region_id, state.txn_id, tmp_num_table_lines, applied_index); 
        //DB_WARNING("region_id: %ld lock and write_pre_commit success,"
        //            " num_table_lines: %ld, applied_index: %ld , txn_id: %lu",
        //            _region_id, tmp_num_table_lines, _applied_index, state.txn_id);
    }
    uint64_t txn_id = state.txn_id;
    ON_SCOPE_EXIT(([this, txn_id, tmp_num_table_lines, applied_index]() {
        if (txn_id != 0) {
            //DB_WARNING("region_id: %ld release commit meta mutex, "
            //    " num_table_lines: %ld, applied_index: %ld , txn_id: %lu",
            //    _region_id, tmp_num_table_lines, applied_index, txn_id);
            _commit_meta_mutex.unlock();
        }        
    }));
    if (done == nullptr) {
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
            
        }
    } else {
        commit_succ = true;
        ((DMLClosure*)done)->transaction = txn;
        ((DMLClosure*)done)->txn_num_increase_rows = txn_num_increase_rows;
    }
    if (commit_succ) {
        response.set_affected_rows(ret);
        response.set_scan_rows(state.num_scan_rows());
        response.set_filter_rows(state.num_filter_rows());
        response.set_errcode(pb::SUCCESS);
        if (state.last_insert_id != INT64_MIN) {
            response.set_last_insert_id(state.last_insert_id);
        }
    } else {
        response.set_errcode(pb::EXEC_FAIL);
        response.set_errmsg("txn commit failed.");
        DB_FATAL("txn commit failed, region_id: %ld, txn_id: %lu, applied_index: %ld", 
                    _region_id, state.txn_id, applied_index);
    }
    bool need_write_rollback = false;
    if (txn->is_primary_region() && !commit_succ) {
        need_write_rollback = true;
    }
    if (state.txn_id != 0) {
        auto ret = _meta_writer->write_meta_after_commit(_region_id, _num_table_lines,
            applied_index, _data_index, state.txn_id, need_write_rollback);
        //DB_WARNING("write meta info wheen commit"
        //            " region_id: %ld, applied_index: %ld, num_table_line: %ld, txn_id: %lu", 
        //            _region_id, applied_index, _num_table_lines.load(), state.txn_id); 
        if (ret < 0) {
            DB_FATAL("Write Metainfo fail, region_id: %ld, txn_id: %lu, log_index: %ld", 
                        _region_id, state.txn_id, applied_index);
        }
    }

    
    int64_t dml_cost = cost.get_time();
    bool auto_commit = false;
    if (request.txn_infos_size() > 0) {
        auto_commit = request.txn_infos(0).autocommit();
    }
    if ((op_type == pb::OP_PREPARE) && auto_commit) {
        Store::get_instance()->dml_time_cost << (dml_cost + txn->get_exec_time_cost());
        _dml_time_cost << (dml_cost + txn->get_exec_time_cost());
    } else if (op_type == pb::OP_INSERT || op_type == pb::OP_DELETE || op_type == pb::OP_UPDATE) {
        Store::get_instance()->dml_time_cost << dml_cost;
        _dml_time_cost << dml_cost;
    }
    if (dml_cost > FLAGS_print_time_us ||
        op_type == pb::OP_COMMIT ||
        op_type == pb::OP_ROLLBACK ||
        op_type == pb::OP_PREPARE) {
        DB_NOTICE("dml type: %s, time_cost:%ld, region_id: %ld, txn_id: %lu, num_table_lines:%ld, "
                  "affected_rows:%d, applied_index:%ld, term:%ld, txn_num_rows:%ld,"
                  " log_id:%lu, wait_cost:%ld", 
                pb::OpType_Name(op_type).c_str(), dml_cost, _region_id,
                state.txn_id, _num_table_lines.load(), ret, applied_index, term,
                txn_num_increase_rows, state.log_id(), wait_cost);
    }
}

void Region::kv_apply_raft(RuntimeState* state, SmartTransaction txn) {
    pb::StoreReq* raft_req = txn->get_raftreq();
    raft_req->set_op_type(pb::OP_KV_BATCH);
    raft_req->set_region_id(state->region_id());
    raft_req->set_region_version(state->region_version());
    raft_req->set_num_increase_rows(txn->batch_num_increase_rows);
    butil::IOBuf data;
    butil::IOBufAsZeroCopyOutputStream wrapper(&data);
    if (!raft_req->SerializeToZeroCopyStream(&wrapper)) {
        DB_FATAL("Fail to serialize request");
        return;
    }
    DMLClosure* c = new DMLClosure(&state->txn_cond);
    c->response = state->response;
    c->region = this;
    c->log_id = state->log_id();
    c->op_type = pb::OP_KV_BATCH;
    c->remote_side = state->remote_side();
    c->is_sync = true;
    c->transaction = txn;
    c->cost.reset();
    braft::Task task;
    task.data = &data;
    task.done = c;
    c->cond->increase();
    _real_writing_cond.increase();
    _node.apply(task); 
}

//
void deal_learner_plan(pb::Plan& plan) {
    for (auto& node : *plan.mutable_nodes()) {
        bool has_learner_index = false;
        if (node.derive_node().has_scan_node()) {
            auto scan_node = node.mutable_derive_node()->mutable_scan_node();
            if (scan_node->has_learner_index()) {
                *(scan_node->mutable_indexes(0)) = scan_node->learner_index();
                has_learner_index = true;
            }
        }

        if (has_learner_index && node.derive_node().has_filter_node()) {
            pb::FilterNode filter_node;
            filter_node.ParseFromString(node.derive_node().filter_node());
            filter_node.mutable_conjuncts()->Swap(filter_node.mutable_conjuncts_learner());
            node.mutable_derive_node()->mutable_raw_filter_node()->CopyFrom(filter_node);
        }
    }
}

int Region::select(const pb::StoreReq& request, pb::StoreRes& response) {
    QosType type = QOS_SELECT;
    uint64_t sign = 0;
    if (request.has_sql_sign()) {
        sign = request.sql_sign();
    } 
    int64_t index_id = 0;
    for (const auto& node : request.plan().nodes()) {
        if (node.node_type() == pb::SCAN_NODE) {
            // todo 兼容代码
            pb::PossibleIndex pos_index;
            pos_index.ParseFromString(node.derive_node().scan_node().indexes(0));
            index_id = pos_index.index_id();
            //index_id = node.derive_node().scan_node().use_indexes(0);
            break;
        }
    }
    StoreQos::get_instance()->create_bthread_local(type, sign, index_id);
    if (StoreQos::get_instance()->need_reject()) {
        response.set_errcode(pb::RETRY_LATER);
        response.set_errmsg("qos reject");
        StoreQos::get_instance()->destroy_bthread_local();
        DB_WARNING("sign: %lu, reject", sign);
        return -1;
    }

    TimeCost cost;
    bool is_new_sign = false;
    if (FLAGS_open_new_sign_read_concurrency && (StoreQos::get_instance()->is_new_sign())) {
        is_new_sign = true;
        Concurrency::get_instance()->new_sign_read_concurrency.increase_wait();
    }
    int64_t wait_cost = cost.get_time();
    ON_SCOPE_EXIT([&]() {
        if (FLAGS_open_new_sign_read_concurrency && is_new_sign) {
            Concurrency::get_instance()->new_sign_read_concurrency.decrease_broadcast();
            if (wait_cost > FLAGS_print_time_us) {
                DB_NOTICE("select type: %s, region_id: %ld, "
                        "time_cost: %ld, log_id: %lu, sign: %lu, rows: %ld, scan_rows: %ld",
                        pb::OpType_Name(request.op_type()).c_str(), _region_id,
                        cost.get_time(), request.log_id(), sign,
                        response.affected_rows(), response.scan_rows());
            }
        }
    });


    int ret = 0;
    if (_is_learner) {
        // learner集群可能涉及到降级，需要特殊处理scan node和filter node
        // 替换learner集群使用的索引和过滤条件
        auto& plan = const_cast<pb::Plan&>(request.plan());
        deal_learner_plan(plan);
        DB_WARNING("region_id: %ld, plan: %s => %s", 
            _region_id, request.plan().ShortDebugString().c_str(), plan.ShortDebugString().c_str());
        ret = select(request, plan, request.tuples(), response);
    } else {
        ret = select(request, request.plan(), request.tuples(), response);
    }
    StoreQos::get_instance()->destroy_bthread_local();
    return ret;
}

int Region::select(const pb::StoreReq& request, 
        const pb::Plan& plan,
        const RepeatedPtrField<pb::TupleDescriptor>& tuples,
        pb::StoreRes& response) {
    //DB_WARNING("req:%s", request.DebugString().c_str());
    pb::TraceNode trace_node;
    std::string desc = "baikalStore select";
    TimeCost cost;
    bool is_trace = false;
    if (request.has_is_trace()) {
        is_trace = request.is_trace();
    }

    ScopeGuard auto_update_trace([&]() {
        if (is_trace) {
            desc += " " + pb::ErrCode_Name(response.errcode());
            trace_node.set_description(desc);
            trace_node.set_total_time(cost.get_time());
            trace_node.set_instance(_address);
            trace_node.set_region_id(_region_id);
            std::string string_trace;
            if (response.errcode() == pb::SUCCESS) {
                if (!trace_node.SerializeToString(&string_trace)) {
                    DB_FATAL("trace_node: %s serialize to string fail",
                             trace_node.ShortDebugString().c_str());
                } else {
                    response.set_errmsg(string_trace);
                }
            }
            
        }
    });
    
    int ret = 0;
    uint64_t db_conn_id = request.db_conn_id();
    if (db_conn_id == 0) {
        db_conn_id = butil::fast_rand();
    }
    
    SmartState state_ptr = std::make_shared<RuntimeState>();
    RuntimeState& state = *state_ptr;
    state.set_resource(get_resource());
    ret = state.init(request, plan, tuples, &_txn_pool, false, _is_binlog_region);
    if (ret < 0) {
        response.set_errcode(pb::EXEC_FAIL);
        response.set_errmsg("RuntimeState init fail");
        DB_FATAL("RuntimeState init fail, region_id: %ld", _region_id);
        return -1;
    }
    _state_pool.set(db_conn_id, state_ptr);
    ON_SCOPE_EXIT(([this, db_conn_id]() {
        _state_pool.remove(db_conn_id);
    }));
    // double check, ensure resource match the req version
    if (validate_version(&request, &response) == false) {
        DB_WARNING("double check region version too old, region_id: %ld,"
                   " request_version:%ld, region_version:%ld",
                    _region_id, request.region_version(), get_version());
        return -1;
    }
    const pb::TransactionInfo& txn_info = request.txn_infos(0);
    bool is_new_txn = false;
    auto txn = state.txn();
    if (txn_info.txn_id() != 0 && (txn == nullptr || txn->is_rolledback())) {
        response.set_errcode(pb::NOT_LEADER);
        response.set_leader(butil::endpoint2str(get_leader()).c_str());
        response.set_errmsg("not leader, maybe transfer leader");
        DB_WARNING("no txn found: region_id: %ld, txn_id: %lu:%d", _region_id, txn_info.txn_id(), txn_info.seq_id());
        return -1;
    }
    if (txn != nullptr) {
        auto op_type = request.op_type();
        std::set<int> need_rollback_seq;
        for (int rollback_seq : txn_info.need_rollback_seq()) {
            need_rollback_seq.insert(rollback_seq);
        }
        // rollback already executed cmds
        for (auto it = need_rollback_seq.rbegin(); it != need_rollback_seq.rend(); ++it) {
            int seq = *it;
            txn->rollback_to_point(seq);
            DB_WARNING("rollback seq_id: %d region_id: %ld, txn_id: %lu, seq_id: %d", 
                seq, _region_id, txn->txn_id(), txn->seq_id());
        }
        if (op_type == pb::OP_SELECT_FOR_UPDATE) {
            auto seq_id = txn_info.seq_id();
            txn->set_seq_id(seq_id);
            txn->set_save_point();
            txn->set_resource(state.resource());
            if (txn_info.has_primary_region_id()) {
                txn->set_primary_region_id(txn_info.primary_region_id());
            }
        }
    } else {
        // DB_WARNING("create tmp txn for select cmd: %ld", _region_id)
        is_new_txn = true;
        txn = state.create_txn_if_null(Transaction::TxnOptions());
    }
    ScopeGuard auto_rollback([&]() {
        if (is_new_txn) {
            txn->rollback();
        }
    });

    {
        BAIDU_SCOPED_LOCK(_reverse_index_map_lock);
        state.set_reverse_index_map(_reverse_index_map);
    }
    ExecNode* root = nullptr; 
    ret = ExecNode::create_tree(plan, &root);
    if (ret < 0) {
        ExecNode::destroy_tree(root);
        response.set_errcode(pb::EXEC_FAIL);
        response.set_errmsg("create plan fail");
        DB_FATAL("create plan fail, region_id: %ld", _region_id);
        return -1;
    }
    if (is_trace) {
        pb::TraceNode* root_trace = trace_node.add_child_nodes();
        root_trace->set_node_type(root->node_type());
        root->set_trace(root_trace);
        root->create_trace();
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
        return -1;
    }
    int rows = 0;
    for (auto& tuple : state.tuple_descs()) {
        if (tuple.has_tuple_id()) {
            response.add_tuple_ids(tuple.tuple_id());
        }
    }
    
    if (request.has_analyze_info()) {
        rows = select_sample(state, root, request.analyze_info(), response);
    } else {
        rows = select_normal(state, root, response);
    }
    if (rows < 0) {
        root->close(&state);
        ExecNode::destroy_tree(root);
        response.set_errcode(pb::EXEC_FAIL);
        if (state.error_code != ER_ERROR_FIRST) {
            response.set_mysql_errcode(state.error_code);
            response.set_errmsg(state.error_msg.str());
        } else {
            response.set_errmsg("plan exec failed");
        }
        DB_FATAL("plan exec fail, region_id: %ld", _region_id);
        return -1;
    }
    response.set_errcode(pb::SUCCESS);
    // 非事务select，不用commit。
    //if (is_new_txn) {
    //    txn->commit(); // no write & lock, no failure
    //    auto_rollback.release();
    //}
    response.set_affected_rows(rows);
    response.set_scan_rows(state.num_scan_rows());
    response.set_filter_rows(state.num_filter_rows());
    if (!is_new_txn && txn != nullptr && request.op_type() == pb::OP_SELECT_FOR_UPDATE) {
        auto seq_id = txn_info.seq_id();
        pb::CachePlan plan_item;
        plan_item.set_op_type(request.op_type());
        plan_item.set_seq_id(seq_id);
        plan_item.mutable_plan()->CopyFrom(plan);
        for (auto& tuple : tuples) {
            plan_item.add_tuples()->CopyFrom(tuple);
        }
        txn->push_cmd_to_cache(seq_id, plan_item);
        //DB_WARNING("put txn cmd to cache: region_id: %ld, txn_id: %lu:%d", _region_id, txn_info.txn_id(), seq_id);
        txn->save_last_response(response);
    }

    //DB_NOTICE("select rows:%d", rows);
    root->close(&state);
    ExecNode::destroy_tree(root);
    desc += " rows:" + std::to_string(rows);    
    return 0;
}

int Region::select_normal(RuntimeState& state, ExecNode* root, pb::StoreRes& response) {
    bool eos = false;
    int rows = 0;
    int ret = 0;
    MemRowDescriptor* mem_row_desc = state.mem_row_desc();

    while (!eos) {
        RowBatch batch;
        batch.set_capacity(state.row_batch_capacity());
        ret = root->get_next(&state, &batch, &eos);
        if (ret < 0) {
            DB_FATAL("plan get_next fail, region_id: %ld", _region_id);
            return -1;
        }
        bool global_ddl_with_ttl = false;
        if (!state.ttl_timestamp_vec.empty()) {
            if (state.ttl_timestamp_vec.size() == batch.size()) {
                global_ddl_with_ttl = true;
            } else {
                DB_FATAL("region_id: %ld, batch size diff vs ttl timestamp size %ld vs %ld", 
                    _region_id, batch.size(), state.ttl_timestamp_vec.size());
            }
        }

        int ttl_idx = 0;
        for (batch.reset(); !batch.is_traverse_over(); batch.next()) {
            MemRow* row = batch.get_row().get();
            rows++;
            ttl_idx++;
            if (row == NULL) {
                DB_FATAL("row is null; region_id: %ld, rows:%d", _region_id, rows);
                continue;
            }
            pb::RowValue* row_value = response.add_row_values();
            for (const auto& iter : mem_row_desc->id_tuple_mapping()) {
                std::string* tuple_value = row_value->add_tuple_values();
                row->to_string(iter.first, tuple_value);
            }

            if (global_ddl_with_ttl) {
                response.add_ttl_timestamp(state.ttl_timestamp_vec[ttl_idx - 1]);
            }
        }
    }

    return rows;
}

//抽样采集
int Region::select_sample(RuntimeState& state, ExecNode* root, const pb::AnalyzeInfo& analyze_info, pb::StoreRes& response) {
    bool eos = false;
    int ret = 0;
    int count = 0;
    MemRowDescriptor* mem_row_desc = state.mem_row_desc();
    RowBatch sample_batch;
    int sample_cnt = 0;
    if (analyze_info.sample_rows() >= analyze_info.table_rows()) {
        //采样行数大于表行数，全部采样
        sample_cnt = _num_table_lines;
    } else {
        sample_cnt = analyze_info.sample_rows() * 1.0 / analyze_info.table_rows() * _num_table_lines.load();
    }
    if (sample_cnt < 1) {
        sample_cnt = 1;
    }
    sample_batch.set_capacity(sample_cnt);
    pb::TupleDescriptor* tuple_desc = state.get_tuple_desc(0);
    if (tuple_desc == nullptr) {
        return -1;
    }
    CMsketch cmsketch(analyze_info.depth(), analyze_info.width());

    while (!eos) {
        RowBatch batch;
        batch.set_capacity(state.row_batch_capacity());
        ret = root->get_next(&state, &batch, &eos);
        if (ret < 0) {
            DB_FATAL("plan get_next fail, region_id: %ld", _region_id);
            return -1;
        }
        for (batch.reset(); !batch.is_traverse_over(); batch.next()) {
            for (auto& slot : tuple_desc->slots()) {
                if (!slot.has_field_id()) {
                    continue;
                }
                ExprValue value = batch.get_row()->get_value(0, slot.slot_id());
                if (value.is_null()) {
                    continue;
                }
                value.cast_to(slot.slot_type());
                cmsketch.set_value(slot.field_id(), value.hash());
            }
            count++;
            if (count <= sample_cnt) {
                sample_batch.move_row(std::move(batch.get_row()));
            } else {
                if (count > 0) {
                    int32_t random = butil::fast_rand() % count; 
                    if (random < sample_cnt) {
                        state.memory_limit_release(count, sample_batch.get_row(random)->used_size());
                        sample_batch.replace_row(std::move(batch.get_row()), random);
                    } else {
			            state.memory_limit_release(count, batch.get_row()->used_size());
		            }
                }
            }
        }
    }
    
    if (count == 0) {
        return 0;
    }
    int rows = 0;
    for (sample_batch.reset(); !sample_batch.is_traverse_over(); sample_batch.next()) {
        MemRow* row = sample_batch.get_row().get();
        if (row == NULL) {
            DB_FATAL("row is null; region_id: %ld, rows:%d", _region_id, rows);
            continue;
        }
        rows++;
        pb::RowValue* row_value = response.add_row_values();
        for (const auto& iter : mem_row_desc->id_tuple_mapping()) {
            std::string* tuple_value = row_value->add_tuple_values();
            row->to_string(iter.first, tuple_value);
        }
    }

    pb::CMsketch* cmsketch_pb = response.mutable_cmsketch();
    cmsketch.to_proto(cmsketch_pb);
    return rows;
}

void Region::construct_peers_status(pb::LeaderHeartBeat* leader_heart) {
    braft::NodeStatus status;
    _node.get_status(&status);
    pb::PeerStateInfo* peer_info = leader_heart->add_peers_status();
    peer_info->set_peer_status(pb::STATUS_NORMAL);
    peer_info->set_peer_id(_address);
    for (auto iter : status.stable_followers) {
        pb::PeerStateInfo* peer_info = leader_heart->add_peers_status();
        peer_info->set_peer_id(butil::endpoint2str(iter.first.addr).c_str());
        if (iter.second.consecutive_error_times > braft::FLAGS_raft_election_heartbeat_factor) {
            DB_WARNING("node:%s_%s peer:%s is faulty",
                _node.node_id().group_id.c_str(),
                _node.node_id().peer_id.to_string().c_str(),
                iter.first.to_string().c_str());
            peer_info->set_peer_status(pb::STATUS_ERROR);
            continue;
        }
        peer_info->set_peer_status(pb::STATUS_NORMAL);
    }
    for (auto iter : status.unstable_followers) {
        pb::PeerStateInfo* peer_info = leader_heart->add_peers_status();
        peer_info->set_peer_id(butil::endpoint2str(iter.first.addr).c_str());
        peer_info->set_peer_status(pb::STATUS_UNSTABLE);
    }
}

void Region::construct_heart_beat_request(pb::StoreHeartBeatRequest& request, bool need_peer_balance) {
    if (_shutdown || !_can_heartbeat || _removed) {
        return;
    }

    _multi_thread_cond.increase();
    ON_SCOPE_EXIT([this]() {
        _multi_thread_cond.decrease_signal();
    });

    if (_num_delete_lines > FLAGS_compact_delete_lines) {
        DB_WARNING("region_id: %ld, delete %ld rows, do compact in queue",
                _region_id, _num_delete_lines.load());
        // 删除大量数据后做compact
        compact_data_in_queue();
    }
    pb::RegionInfo copy_region_info;
    copy_region(&copy_region_info);
    // learner 在版本0时（拉取首个snapshot）可以上报心跳，防止meta重复创建 learner。
    if (copy_region_info.version() == 0 && !is_learner()) {
        DB_WARNING("region version is 0, region_id: %ld", _region_id);
        return;
    }
    _region_info.set_num_table_lines(_num_table_lines.load());
    //增加peer心跳信息
    if ((need_peer_balance || is_merged()) 
        // addpeer过程中，还没走到on_configuration_committed，此时删表，会导致peer清理不掉
            && (_report_peer_info || !_factory->exist_tableid(get_table_id()))) {
        pb::PeerHeartBeat* peer_info = request.add_peer_infos();
        peer_info->set_table_id(copy_region_info.table_id());
        peer_info->set_main_table_id(get_table_id());
        peer_info->set_region_id(_region_id);
        peer_info->set_log_index(_applied_index);
        peer_info->set_start_key(copy_region_info.start_key());
        peer_info->set_end_key(copy_region_info.end_key());
        peer_info->set_is_learner(is_learner());
        if (get_leader().ip != butil::IP_ANY) {
            peer_info->set_exist_leader(true);    
        } else {
            peer_info->set_exist_leader(false); 
        }
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
        //leader_region->set_used_size(_region_info.used_size());
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
        construct_peers_status(leader_heart);
    }

    if (is_learner()) {
        pb::LearnerHeartBeat* learner_heart = request.add_learner_regions();
        learner_heart->set_state(_region_status);
        pb::RegionInfo* learner_region =  learner_heart->mutable_region();
        copy_region(learner_region);
        learner_region->set_status(_region_control.get_status());
    }
}

void Region::set_can_add_peer() {
    if (!_region_info.can_add_peer()) {
        pb::RegionInfo region_info_mem;
        copy_region(&region_info_mem);
        region_info_mem.set_can_add_peer(true);
        if (_meta_writer->update_region_info(region_info_mem) != 0) {
            DB_FATAL("update can add peer fail, region_id: %ld", _region_id); 
        } else {
            DB_WARNING("update can add peer success, region_id: %ld", _region_id);
        }
        _region_info.set_can_add_peer(true);
    }
}

void Region::do_apply(int64_t term, int64_t index, const pb::StoreReq& request, braft::Closure* done) {
    if (index <= _applied_index) {
        if (get_version() == 0) {
            DB_WARNING("this log entry has been executed, log_index:%ld, applied_index:%ld, region_id: %ld",
                       index, _applied_index, _region_id);
        }
        return;
    }
    pb::OpType op_type = request.op_type();
    _region_info.set_log_index(index);
    _applied_index = index;
    reset_timecost();
    pb::StoreRes res;
    switch (op_type) {
        //binlog op
        case pb::OP_PREWRITE_BINLOG:
        case pb::OP_COMMIT_BINLOG:
        case pb::OP_ROLLBACK_BINLOG:
        case pb::OP_FAKE_BINLOG: {
            if (!_is_binlog_region) {
                DB_FATAL("region_id: %ld, is not binlog region can't process binlog op", _region_id);
                break;
            }
            _data_index = _applied_index;
            _meta_writer->update_apply_index(_region_id, _applied_index, _data_index);
            apply_binlog(request, done);
            break;
        }

        //kv操作,存储计算分离时使用
        case pb::OP_KV_BATCH: {
            // 在on_apply里赋值，有些函数可能不在状态机里调用
            _data_index = _applied_index;
            uint64_t txn_id = request.txn_infos_size() > 0 ? request.txn_infos(0).txn_id():0;
            if (txn_id == 0) {
                apply_kv_out_txn(request, done, _applied_index, term);
            } else {
                apply_kv_in_txn(request, done, _applied_index, term);
            }
            break;
        }
        case pb::OP_PREPARE:
        case pb::OP_COMMIT:
        case pb::OP_ROLLBACK: {
            _data_index = _applied_index;
            apply_txn_request(request, done, _applied_index, term);
            break;
        }
        // 兼容老版本无事务功能时的log entry, 以及强制1PC的DML query(如灌数据时使用)
        case pb::OP_KILL:
        case pb::OP_INSERT:
        case pb::OP_DELETE:
        case pb::OP_UPDATE: 
        case pb::OP_TRUNCATE_TABLE:
        case pb::OP_SELECT_FOR_UPDATE: {
            _data_index = _applied_index;
            uint64_t txn_id = request.txn_infos_size() > 0 ? request.txn_infos(0).txn_id():0;
            //事务流程中DML处理
            if (txn_id != 0 && (is_dml_op_type(op_type))) {
                apply_txn_request(request, done, _applied_index, term);
                break;
            }
            if (done != nullptr && ((DMLClosure*)done)->transaction != nullptr && (is_dml_op_type(op_type))) {
                bool commit_succ = false;
                int64_t tmp_num_table_lines = _num_table_lines + ((DMLClosure*)done)->txn_num_increase_rows;
                _meta_writer->write_meta_index_and_num_table_lines(_region_id, _applied_index, _data_index,
                                tmp_num_table_lines, ((DMLClosure*)done)->transaction);
                auto res = ((DMLClosure*)done)->transaction->commit();
                if (res.ok()) {
                    commit_succ = true;
                } else if (res.IsExpired()) {
                    DB_WARNING("txn expired, region_id: %ld, applied_index: %ld", 
                               _region_id, _applied_index);
                    commit_succ = false;
                } else {
                    DB_WARNING("unknown error: region_id: %ld, errcode:%d, msg:%s", 
                            _region_id, res.code(), res.ToString().c_str());
                    commit_succ = false;
                }
                if (commit_succ) {
                    if (((DMLClosure*)done)->txn_num_increase_rows < 0) {
                        _num_delete_lines -= ((DMLClosure*)done)->txn_num_increase_rows;
                    }
                    _num_table_lines = tmp_num_table_lines;
                } else {
                    ((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
                    ((DMLClosure*)done)->response->set_errmsg("txn commit failed.");
                    DB_FATAL("txn commit failed, region_id: %ld, applied_index: %ld", 
                             _region_id, _applied_index);
                }
                ((DMLClosure*)done)->applied_index = _applied_index;
                break;
            } else {
                dml_1pc(request, request.op_type(), request.plan(), request.tuples(), 
                        res, index, term, nullptr);
            }
            if (get_version() == 0 && res.errcode() != pb::SUCCESS) {
                DB_WARNING("dml_1pc EXEC FAIL %s", res.DebugString().c_str());
                _async_apply_param.apply_log_failed = true;
            }
            if (done != nullptr) {
                ((DMLClosure*)done)->applied_index = _applied_index;
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
                if (res.has_scan_rows()) {
                    ((DMLClosure*)done)->response->set_scan_rows(res.scan_rows());
                }
                if (res.has_filter_rows()) {
                    ((DMLClosure*)done)->response->set_filter_rows(res.filter_rows());
                }
                if (res.has_last_insert_id()) {
                    ((DMLClosure*)done)->response->set_last_insert_id(res.last_insert_id());
                }
            }
            //DB_WARNING("dml_1pc %s", res.trace_nodes().DebugString().c_str());
            break;
        }
        case pb::OP_CLEAR_APPLYING_TXN: {
            clear_orphan_transactions(done, _applied_index, term);
            _meta_writer->update_apply_index(_region_id, _applied_index, _data_index);
            if (done != nullptr) {
                leader_start(term);
            }
            break;
        }
        case pb::OP_UPDATE_PRIMARY_TIMESTAMP: {
            exec_update_primary_timestamp(request, done, _applied_index, term);
            _meta_writer->update_apply_index(_region_id, _applied_index, _data_index);
            break;
        }
        //split的各类请求传进的来的done类型各不相同，不走下边的if(done)逻辑，直接处理完成，然后continue
        case pb::OP_NONE: {
            _meta_writer->update_apply_index(_region_id, _applied_index, _data_index);
            if (done != nullptr) {
                ((DMLClosure*)done)->response->set_errcode(pb::SUCCESS);
            }
            DB_NOTICE("op_type=%s, region_id: %ld, applied_index:%ld, term:%ld",
                      pb::OpType_Name(request.op_type()).c_str(), _region_id, _applied_index, term);
            break;
        }
        case pb::OP_START_SPLIT: {
            start_split(done, _applied_index, term);
            DB_NOTICE("op_type: %s, region_id: %ld, applied_index:%ld, term:%ld",
                      pb::OpType_Name(request.op_type()).c_str(), _region_id, _applied_index, term);
            break;
        }
        case pb::OP_START_SPLIT_FOR_TAIL: {
            start_split_for_tail(done, _applied_index, term);
            DB_NOTICE("op_type: %s, region_id: %ld, applied_index:%ld, term:%ld",
                      pb::OpType_Name(request.op_type()).c_str(), _region_id, _applied_index, term);
            break;
        }
        case pb::OP_ADJUSTKEY_AND_ADD_VERSION: {
            _data_index = _applied_index;
            adjustkey_and_add_version(request, done, _applied_index, term);
            DB_NOTICE("op_type: %s, region_id :%ld, applied_index:%ld, term:%ld",
                      pb::OpType_Name(request.op_type()).c_str(), _region_id, _applied_index, term);
            break;
        }
        case pb::OP_VALIDATE_AND_ADD_VERSION: {
            _data_index = _applied_index;
            validate_and_add_version(request, done, _applied_index, term);
            DB_NOTICE("op_type: %s, region_id: %ld, applied_index:%ld, term:%ld",
                      pb::OpType_Name(request.op_type()).c_str(), _region_id, _applied_index, term);
            break;
        }
        case pb::OP_ADD_VERSION_FOR_SPLIT_REGION: {
            _data_index = _applied_index;
            add_version_for_split_region(request, done, _applied_index, term);
            DB_NOTICE("op_type: %s, region_id: %ld, applied_index:%ld, term:%ld",
                      pb::OpType_Name(request.op_type()).c_str(), _region_id, _applied_index, term);
            break;
        }
        default:
            _meta_writer->update_apply_index(_region_id, _applied_index, _data_index);
            DB_FATAL("unsupport request type, op_type:%d, region_id: %ld",
                     request.op_type(), _region_id);
            if (done != nullptr) {
                ((DMLClosure*)done)->response->set_errcode(pb::UNSUPPORT_REQ_TYPE);
                ((DMLClosure*)done)->response->set_errmsg("unsupport request type");
            }
            DB_NOTICE("op_type: %s, region_id: %ld, applied_index:%ld, term:%ld",
                      pb::OpType_Name(request.op_type()).c_str(), _region_id, _applied_index, term);
            break;
    }
}

void Region::on_apply(braft::Iterator& iter) {
    for (; iter.valid(); iter.next()) {
        braft::Closure* done = iter.done();
        brpc::ClosureGuard done_guard(done);
        butil::IOBuf data = iter.data();
        butil::IOBufAsZeroCopyInputStream wrapper(data);
        std::shared_ptr<pb::StoreReq> request = std::make_shared<pb::StoreReq>();
        if (!request->ParseFromZeroCopyStream(&wrapper)) {
            DB_FATAL("parse from protobuf fail, region_id: %ld", _region_id);
            if (done != nullptr) {
                ((DMLClosure*)done)->response->set_errcode(pb::PARSE_FROM_PB_FAIL);
                ((DMLClosure*)done)->response->set_errmsg("parse from protobuf fail");
                braft::run_closure_in_bthread(done_guard.release());
            }
            continue;
        }
        reset_timecost();
        auto term = iter.term();
        auto index = iter.index();
        _braft_apply_index = index;
        if (request->op_type() == pb::OP_ADD_VERSION_FOR_SPLIT_REGION ||
                (get_version() == 0 && request->op_type() == pb::OP_CLEAR_APPLYING_TXN)) {
            // 异步队列排空
            DB_WARNING("braft_apply_index: %lu, applied_index: %lu, region_id: %lu",
                       _braft_apply_index, _applied_index, _region_id);
            wait_async_apply_log_queue_empty();
            DB_WARNING("wait async finish, region_id: %lu", _region_id);
        }
        // 分裂的情况下，region version为0，都走异步的逻辑;
        // OP_ADD_VERSION_FOR_SPLIT_REGION及分裂结束后，走正常同步的逻辑
        // version为0时，on_leader_start会提交一条pb::OP_CLEAR_APPLYING_TXN日志，这个同步
        if (get_version() == 0
            && request->op_type() != pb::OP_CLEAR_APPLYING_TXN
            && request->op_type() != pb::OP_ADD_VERSION_FOR_SPLIT_REGION) {
            auto func = [this, term, index, request]() mutable {
                pb::StoreReq& store_req = *request;
                if (store_req.op_type() != pb::OP_INSERT
                    && store_req.op_type() != pb::OP_DELETE
                    && store_req.op_type() != pb::OP_UPDATE
                    && store_req.op_type() != pb::OP_PREPARE
                    && store_req.op_type() != pb::OP_ROLLBACK
                    && store_req.op_type() != pb::OP_COMMIT
                    && store_req.op_type() != pb::OP_NONE
                    && store_req.op_type() != pb::OP_KV_BATCH
                    && store_req.op_type() != pb::OP_SELECT_FOR_UPDATE
                    && store_req.op_type() != pb::OP_UPDATE_PRIMARY_TIMESTAMP) {
                    DB_WARNING("unexpected store_req:%s, region_id: %ld",
                               pb2json(store_req).c_str(), _region_id);
                    _async_apply_param.apply_log_failed = true;
                    return;
                }
                store_req.set_region_id(_region_id);
                store_req.set_region_version(0);
                // for tail splitting new region replay txn
                if (store_req.has_start_key() && !store_req.start_key().empty()) {
                    pb::RegionInfo region_info_mem;
                    copy_region(&region_info_mem);
                    region_info_mem.set_start_key(store_req.start_key());
                    set_region_with_update_range(region_info_mem);
                }
                do_apply(term, index, store_req, nullptr);
            };
            _async_apply_log_queue.run(func);
            if (done != nullptr && ((DMLClosure*)done)->response != nullptr) {
                ((DMLClosure*)done)->response->set_errcode(pb::SUCCESS);
                ((DMLClosure*)done)->response->set_errmsg("success");
            }
        } else {
            do_apply(term, index, *request, done);
        }
        if (done != nullptr) {
            braft::run_closure_in_bthread(done_guard.release());
        }
    }
}

bool Region::check_key_fits_region_range(SmartIndex pk_info, SmartTransaction txn,
        const pb::RegionInfo& region_info, const pb::KvOp& kv_op) {
    rocksdb::Slice key_slice(kv_op.key());
    int64_t index_id = KeyEncoder::decode_i64(KeyEncoder::to_endian_u64(*(uint64_t*)(key_slice.data() + 8)));
    key_slice.remove_prefix(2 * sizeof(int64_t));
    auto index_ptr = _factory->get_index_info_ptr(index_id);
    if (index_ptr == nullptr) {
        DB_WARNING("region_id: %ld index_id:%ld not exist", _region_id, index_id);
        return false;
    }
    if (index_ptr->type == pb::I_PRIMARY || _is_global_index) {
        if (key_slice.compare(region_info.start_key()) < 0) {
            return false;
        }
        if (!region_info.end_key().empty() && key_slice.compare(region_info.end_key()) >= 0) {
            return false;
        }
    } else if (index_ptr->type == pb::I_UNIQ || index_ptr->type == pb::I_KEY) {
        // kv_op.value() 不包含ttl，ttl在kv_op.ttl_timestamp_us()
        if (!Transaction::fits_region_range(key_slice, kv_op.value(),
                                            &region_info.start_key(), &region_info.end_key(), 
                                            *pk_info, *index_ptr)) {
            return false;
        }
    }
    return true;
}

void Region::apply_kv_in_txn(const pb::StoreReq& request, braft::Closure* done, 
                             int64_t index, int64_t term) {
    TimeCost cost;
    pb::StoreRes res;
    const pb::TransactionInfo& txn_info = request.txn_infos(0);
    int seq_id = txn_info.seq_id();
    uint64_t txn_id = txn_info.txn_id();
    SmartTransaction txn = _txn_pool.get_txn(txn_id);
    // seq_id within a transaction should be continuous regardless of failure or success
    int last_seq = (txn == nullptr)? 0 : txn->seq_id();
    bool apply_success = true;
    int64_t num_increase_rows = 0;
    //DB_WARNING("index:%ld request:%s", index, request.ShortDebugString().c_str());
    ScopeGuard auto_rollback_current_request([this, &txn, &apply_success]() {
        if (txn != nullptr && !apply_success) {
            txn->rollback_current_request();
        }
        if (txn != nullptr) {
            txn->set_in_process(false);
            txn->clear_raftreq();
        }
        if (get_version() == 0 && !apply_success) {
            _async_apply_param.apply_log_failed = true;
        }
    });
    // for tail splitting new region replay txn
    if (request.has_start_key() && !request.start_key().empty()) {
        pb::RegionInfo region_info_mem;
        copy_region(&region_info_mem);
        region_info_mem.set_start_key(request.start_key());
        set_region_with_update_range(region_info_mem);
    }
    auto resource = get_resource();
    int ret = 0;
    if (txn == nullptr) {
        // exec BEGIN
        auto& cache_item = txn_info.cache_plans(0);
        const pb::OpType op_type = cache_item.op_type();
        const pb::Plan& plan = cache_item.plan();
        const RepeatedPtrField<pb::TupleDescriptor>& tuples = cache_item.tuples();
        if (op_type != pb::OP_BEGIN) {
            DB_FATAL("unexpect op_type: %s, region:%ld, txn_id:%lu", pb::OpType_Name(op_type).c_str(),
                _region_id, txn_id);
            return;
        }
        int seq_id = cache_item.seq_id();
        dml_2pc(request, op_type, plan, tuples, res, index, term, seq_id, false);
        if (res.has_errcode() && res.errcode() != pb::SUCCESS) {
            DB_FATAL("TransactionError: txn: %ld_%lu:%d executed failed.", _region_id, txn_id, seq_id);
            return;
        }
        txn = _txn_pool.get_txn(txn_id);
    }
    if (ret != 0) {
        apply_success = false;
        DB_FATAL("execute cached cmd failed, region:%ld, txn_id:%lu", _region_id, txn_id);
        return;
    }
    bool write_begin_index = false;
    if (index != 0 && txn != nullptr && txn->write_begin_index()) {
        // 需要记录首条事务指令
        write_begin_index = true;
    }
    if (write_begin_index) {
        auto ret = _meta_writer->write_meta_begin_index(_region_id, index, _data_index, txn_id);
        //DB_WARNING("write meta info when prepare, region_id: %ld, applied_index: %ld, txn_id: %ld", 
        //            _region_id, index, txn_id);
        if (ret < 0) {
            apply_success = false;
            res.set_errcode(pb::EXEC_FAIL);
            res.set_errmsg("Write Metainfo fail");
            DB_FATAL("Write Metainfo fail, region_id: %ld, txn_id: %lu, log_index: %ld", 
                        _region_id, txn_id, index);
            return;
        }
    }

    if (txn != nullptr) {
        txn->set_write_begin_index(false);
        txn->set_applying(false);
        txn->set_applied_seq_id(seq_id);
        txn->set_resource(resource);
    }
    
    if (done == nullptr) {
        // follower
        if (last_seq >= seq_id) {
            DB_WARNING("Transaction exec before, region_id: %ld, txn_id: %lu, seq_id: %d, req_seq: %d",
                _region_id, txn_id, txn->seq_id(), seq_id);
            return;
        }
        // rollback already executed cmds
        std::set<int> need_rollback_seq;
        for (int rollback_seq : txn_info.need_rollback_seq()) {
            need_rollback_seq.insert(rollback_seq);
        }
        for (auto it = need_rollback_seq.rbegin(); it != need_rollback_seq.rend(); ++it) {
            int seq = *it;
            txn->rollback_to_point(seq);
            //DB_WARNING("rollback seq_id: %d region_id: %ld, txn_id: %lu, seq_id: %d, req_seq: %d", 
            //    seq, _region_id, txn_id, txn->seq_id(), seq_id);
        }
        // if current cmd need rollback, simply not execute
        if (need_rollback_seq.count(seq_id) != 0) {
            DB_WARNING("need rollback, not executed and cached. region_id: %ld, txn_id: %lu, seq_id: %d, req_seq: %d",
                _region_id, txn_id, txn->seq_id(), seq_id);
            txn->set_seq_id(seq_id);
            return;
        }
        txn->set_seq_id(seq_id);
        // set checkpoint for current DML operator
        txn->set_save_point();
        txn->set_primary_region_id(txn_info.primary_region_id());
        auto pk_info = _factory->get_index_info_ptr(_table_id);
        for (auto& kv_op : request.kv_ops()) {
            pb::OpType op_type = kv_op.op_type();
            int ret = 0;
            MutTableKey key(kv_op.key());
            key.replace_i64(_region_id, 0);
            // follower
            if (get_version() != 0) {
                if (op_type == pb::OP_PUT_KV) {
                    ret = txn->put_kv(key.data(), kv_op.value(), kv_op.ttl_timestamp_us());
                } else {
                    ret = txn->delete_kv(key.data());
                }
            } else {
                // 分裂/add_peer
                if (!check_key_fits_region_range(pk_info, txn, resource->region_info, kv_op)) {
                    continue;
                }
                bool is_key_exist = check_key_exist(txn, kv_op);
                int scope_write_lines = 0;
                if (op_type == pb::OP_PUT_KV) {          
                    ret = txn->put_kv(key.data(), kv_op.value(), kv_op.ttl_timestamp_us());
                    if (!is_key_exist) {
                        scope_write_lines++;
                    }
                } else {
                    ret = txn->delete_kv(key.data());
                    if (is_key_exist) {
                        scope_write_lines--;
                    }
                }
                if (kv_op.is_primary_key()) {
                    num_increase_rows += scope_write_lines;
                }
            }
            if (ret < 0) {
                apply_success = false;
                DB_FATAL("kv operation fail, op_type:%s, region_id: %ld, txn_id:%lu:%d "
                        "applied_index: %ld, term:%ld",
                        pb::OpType_Name(op_type).c_str(), _region_id, txn_id, seq_id, index, term);
                return;
            }
            txn->clear_current_req_point_seq();
        }
        if (get_version() != 0 && term != 0) {
            num_increase_rows = request.num_increase_rows();
        }
        txn->num_increase_rows += num_increase_rows;
        pb::CachePlan plan_item;
        plan_item.set_op_type(request.op_type());
        plan_item.set_seq_id(seq_id);
        for (auto& kv_op : request.kv_ops()) {
            plan_item.add_kv_ops()->CopyFrom(kv_op);
        }
        txn->push_cmd_to_cache(seq_id, plan_item);
    } else {
        // leader
        ((DMLClosure*)done)->response->set_errcode(pb::SUCCESS);
        ((DMLClosure*)done)->applied_index = index;
    }
    if (done == nullptr) {
        //follower在此更新
        int64_t dml_cost = cost.get_time();
        Store::get_instance()->dml_time_cost << dml_cost;
        if (dml_cost > FLAGS_print_time_us) {
            DB_NOTICE("op_type:%s time_cost:%ld, region_id: %ld, txn_id:%lu:%d table_lines:%ld, "
                       "increase_lines:%ld, applied_index:%ld, term:%ld",
                       pb::OpType_Name(request.op_type()).c_str(), cost.get_time(), _region_id, txn_id, seq_id,
                       _num_table_lines.load(),
                       num_increase_rows, index, term);
        }
    }
}

void Region::apply_kv_out_txn(const pb::StoreReq& request, braft::Closure* done, 
                              int64_t index, int64_t term) {
    TimeCost cost;
    SmartTransaction txn = nullptr;
    bool commit_succ = false;
    int64_t num_table_lines = 0;
    auto resource = get_resource();
    int64_t num_increase_rows = 0;
    ScopeGuard auto_rollback([this, &txn, &commit_succ, done]() {
        // rollback if not commit succ
        if (!commit_succ) {
            if (txn != nullptr) {
                txn->rollback();
            }
            if (done != nullptr) {
                ((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
                ((DMLClosure*)done)->response->set_errmsg("commit failed in fsm");
            }
            if (get_version() == 0) {
                _async_apply_param.apply_log_failed = true;
            }
        } else {
            if (done != nullptr) {
                ((DMLClosure*)done)->response->set_errcode(pb::SUCCESS);
                ((DMLClosure*)done)->response->set_errmsg("success");
            }
        }
    });
    if (done == nullptr) {
        // follower
        txn = SmartTransaction(new Transaction(0, &_txn_pool));
        txn->set_resource(resource);
        txn->begin(Transaction::TxnOptions());
        int64_t table_id = get_table_id();
        auto pk_info = _factory->get_index_info_ptr(table_id);
        for (auto& kv_op : request.kv_ops()) {
            pb::OpType op_type = kv_op.op_type();
            int ret = 0;
            if (get_version() != 0) {
                if (op_type == pb::OP_PUT_KV) {          
                    ret = txn->put_kv(kv_op.key(), kv_op.value(), kv_op.ttl_timestamp_us());
                } else {
                    ret = txn->delete_kv(kv_op.key());
                }
            } else {
                // 分裂流程
                bool is_key_exist = check_key_exist(txn, kv_op);
                int scope_write_lines = 0;
                if (!check_key_fits_region_range(pk_info, txn, resource->region_info, kv_op)) {
                    continue;
                }
                MutTableKey key(kv_op.key());
                key.replace_i64(_region_id, 0);
                if (op_type == pb::OP_PUT_KV) {          
                    ret = txn->put_kv(key.data(), kv_op.value(), kv_op.ttl_timestamp_us());
                    if (!is_key_exist) {
                        scope_write_lines++;
                    }
                } else {
                    ret = txn->delete_kv(key.data());
                    if (is_key_exist) {
                        scope_write_lines--;
                    }
                }
                
                if (kv_op.is_primary_key()) {
                    num_increase_rows += scope_write_lines;
                }
            }
            if (ret < 0) {
                DB_FATAL("kv operation fail, op_type:%s, region_id: %ld, "
                        "applied_index: %ld, term:%ld", 
                        pb::OpType_Name(op_type).c_str(), _region_id, index, term);
                return;
            }
        }
        num_table_lines = _num_table_lines + num_increase_rows;
    } else {
        // leader
        txn = ((DMLClosure*)done)->transaction;
    }
    if (get_version() != 0) {
        num_increase_rows = request.num_increase_rows();
        num_table_lines = _num_table_lines + num_increase_rows;
    }
    _meta_writer->write_meta_index_and_num_table_lines(_region_id, index, _data_index, num_table_lines, txn);
    auto res = txn->commit();
    if (res.ok()) {
        commit_succ = true;
        if (num_increase_rows < 0) {
            _num_delete_lines -= num_increase_rows;
        }
        _num_table_lines = num_table_lines;
    } else {
        DB_FATAL("commit fail, region_id:%ld, applied_index: %ld, term:%ld ", 
                _region_id, index, term);
        return;
    }
    
    int64_t dml_cost = cost.get_time();
    if (done == nullptr) {
        //follower在此更新
        Store::get_instance()->dml_time_cost << dml_cost;
        if (dml_cost > FLAGS_print_time_us) {
            DB_NOTICE("time_cost:%ld, region_id: %ld, table_lines:%ld, "
                       "increase_lines:%ld, applied_index:%ld, term:%ld",
                       cost.get_time(), _region_id, _num_table_lines.load(), 
                       num_increase_rows, index, term);
        }
    }
}

void Region::apply_txn_request(const pb::StoreReq& request, braft::Closure* done, int64_t index, int64_t term) {
    uint64_t txn_id = request.txn_infos_size() > 0 ? request.txn_infos(0).txn_id():0;
    if (txn_id == 0) {
        if (done != nullptr) {
            ((DMLClosure*)done)->response->set_errcode(pb::INPUT_PARAM_ERROR);
            ((DMLClosure*)done)->response->set_errmsg("txn control cmd out-of-txn");
        }
        if (get_version() == 0) {
            _async_apply_param.apply_log_failed = true;
        }
        return;
    }
    // for tail splitting new region replay txn
    if (request.has_start_key() && !request.start_key().empty()) {
        pb::RegionInfo region_info_mem;
        copy_region(&region_info_mem);
        region_info_mem.set_start_key(request.start_key());
        set_region_with_update_range(region_info_mem);
    }
    pb::StoreRes res;
    pb::OpType op_type = request.op_type();
    const pb::TransactionInfo& txn_info = request.txn_infos(0);
    int seq_id = txn_info.seq_id();
    SmartTransaction txn = _txn_pool.get_txn(txn_id);
    // seq_id within a transaction should be continuous regardless of failure or success
    int last_seq = (txn == nullptr)? 0 : txn->seq_id();
    bool apply_success = true;
    ScopeGuard auto_rollback_current_request([this, &txn, &apply_success]() {
        if (txn != nullptr && !apply_success) {
            txn->rollback_current_request();
        }
        if (txn != nullptr) {
            txn->set_in_process(false);
        }
        if (get_version() == 0 && !apply_success) {
            _async_apply_param.apply_log_failed = true;
        }
    });
    int ret = 0;
    if (last_seq < seq_id - 1) {
        ret = execute_cached_cmd(request, res, txn_id, txn, index, term, request.log_id());
    }
    if (ret != 0) {
        apply_success = false;
        DB_FATAL("on_prepare execute cached cmd failed, region:%ld, txn_id:%lu", _region_id, txn_id);
        if (done != nullptr) {
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
    bool write_begin_index = false;
    if (index != 0 && txn != nullptr && txn->write_begin_index()) {
        // 需要记录首条事务指令
        write_begin_index = true;
    }
    if (write_begin_index) {
        auto ret = _meta_writer->write_meta_begin_index(_region_id, index, _data_index, txn_id);
        //DB_WARNING("write meta info when prepare, region_id: %ld, applied_index: %ld, txn_id: %ld", 
        //            _region_id, index, txn_id);
        if (ret < 0) {
            apply_success = false;
            res.set_errcode(pb::EXEC_FAIL);
            res.set_errmsg("Write Metainfo fail");
            DB_FATAL("Write Metainfo fail, region_id: %ld, txn_id: %lu, log_index: %ld", 
                        _region_id, txn_id, index);
            return;
        }
    }

    if (txn != nullptr) {
        txn->set_write_begin_index(false);
        txn->set_applying(false);
        txn->set_applied_seq_id(seq_id);
    }
    if (txn == nullptr) {
        // 由于raft日志apply慢导致事务反查primary region先执行，导致事务提交
        // leader执行第一条DML失败，提交ROLLBACK命令时
        if (op_type == pb::OP_ROLLBACK || op_type == pb::OP_COMMIT) {
            DB_FATAL("Transaction finish: txn has exec before, "
                    "region_id: %ld, txn_id: %lu, applied_index:%ld, op_type: %s",
                _region_id, txn_id, index, pb::OpType_Name(op_type).c_str());
            if (done != nullptr) {
                ((DMLClosure*)done)->response->set_errcode(pb::SUCCESS);
            }
            return;
        }
    }
    // rollback is executed only if txn is not null (since we do not execute
    // cached cmd for rollback, the txn handler may be nullptr)
    if (op_type != pb::OP_ROLLBACK || txn != nullptr) {
        if (last_seq < seq_id) {
            // follower
            if (op_type != pb::OP_SELECT_FOR_UPDATE) {
                dml(request, res, index, term, false);
            } else {
                select(request, res);
            }
            if (txn != nullptr) {
                txn->clear_current_req_point_seq();
            }
        } else {
            // leader
            if (done != nullptr) {
                // DB_NOTICE("dml type: %s, region_id: %ld, txn_id: %lu:%d, applied_index:%ld, term:%d", 
                // pb::OpType_Name(op_type).c_str(), _region_id, txn->txn_id(), txn->seq_id(), index, term);
                ((DMLClosure*)done)->response->set_errcode(pb::SUCCESS);
                ((DMLClosure*)done)->applied_index = index;
            }
            return;
        }
    } else {
        DB_WARNING("rollback a not started txn, region_id: %ld, txn_id: %lu",
            _region_id, txn_id);
    }
    if (get_version() == 0 && res.errcode() != pb::SUCCESS) {
        DB_WARNING("_async_apply_param.apply_log_failed, res: %s", res.ShortDebugString().c_str());
        _async_apply_param.apply_log_failed = true;
    }
    if (done != nullptr) {
        ((DMLClosure*)done)->response->set_errcode(res.errcode());
        ((DMLClosure*)done)->applied_index = index;
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
        if (res.has_last_insert_id()) {
            ((DMLClosure*)done)->response->set_last_insert_id(res.last_insert_id());
        }
    }
}
void Region::start_split(braft::Closure* done, int64_t applied_index, int64_t term) {
    static bvar::Adder<int> bvar_split; 
    static bvar::Window<bvar::Adder<int>> split_count("split_count_minite", &bvar_split, 60);
    _meta_writer->update_apply_index(_region_id, applied_index, _data_index);
    //只有leader需要处理split请求，记录当前的log_index, term和迭代器
    if (done != nullptr) {
        bvar_split << 1;
        int64_t get_split_key_term = _split_param.split_term;
        _split_param.split_start_index = applied_index + 1;
        _split_param.split_term = term;
        _split_param.snapshot = _rocksdb->get_db()->GetSnapshot();
        _txn_pool.get_prepared_txn_info(_split_param.applied_txn, false);

        ((SplitClosure*)done)->ret = 0;
        if (_split_param.snapshot == nullptr) {
            ((SplitClosure*)done)->ret = -1;
        }
        if (get_split_key_term != term) {
            // 获取split_key到现在，term发生了改变
            ((SplitClosure*)done)->ret = -1;
        }
        DB_WARNING("begin start split, region_id: %ld, split_start_index:%ld, term:%ld, num_prepared: %lu",
                    _region_id, applied_index + 1, term, _split_param.applied_txn.size());
    } else {
        DB_WARNING("only leader process start split request, region_id: %ld", _region_id);
    }
}

void Region::start_split_for_tail(braft::Closure* done, int64_t applied_index, int64_t term) {    
    _meta_writer->update_apply_index(_region_id, applied_index, _data_index);
    if (done != nullptr) {
        _split_param.split_end_index = applied_index;
        _split_param.split_term = term;
        int64_t tableid = get_global_index_id();
        if (tableid < 0) {
            DB_WARNING("invalid tableid: %ld, region_id: %ld", 
                        tableid, _region_id);
            ((SplitClosure*)done)->ret = -1;
            return;
        }
        rocksdb::ReadOptions read_options;
        read_options.total_order_seek = true;
        read_options.prefix_same_as_start = false;
        read_options.fill_cache = false;
        std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _data_cf));
        _txn_pool.get_prepared_txn_info(_split_param.applied_txn, false);

        MutTableKey key;
        //不够精确，但暂且可用。不允许主键是FFFFF
        key.append_i64(_region_id).append_i64(tableid).append_u64(UINT64_MAX);
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
                   _region_id, _split_param.applied_txn.size());
    } else {
        DB_WARNING("only leader process start split for tail, region_id: %ld", _region_id);
    }
}

void Region::adjustkey_and_add_version_query(google::protobuf::RpcController* controller,
                               const pb::StoreReq* request, 
                               pb::StoreRes* response, 
                               google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = (brpc::Controller*)controller;
    uint64_t log_id = 0;
    if (cntl->has_log_id()) { 
        log_id = cntl->log_id();
    }
    
    pb::RegionStatus expected_status = pb::IDLE; 
    if (!_region_control.compare_exchange_strong(expected_status, pb::DOING)) {
        response->set_errcode(pb::EXEC_FAIL);
        response->set_errmsg("region status is not idle");
        DB_FATAL("merge dst region fail, region status is not idle when start merge,"
                 " region_id: %ld, log_id:%lu", _region_id, log_id);
        return;
    }  
    //doing之后再检查version
    if (validate_version(request, response) == false) {
        reset_region_status();
        return;
    }
    DB_WARNING("merge dst region region_id: %ld, log_id:%lu", _region_id, log_id);
    butil::IOBuf data;
    butil::IOBufAsZeroCopyOutputStream wrapper(&data);
    // 强制更新start_key、end_key
    // 用作手工修复场景
    if (request->force()) {
        if (!request->SerializeToZeroCopyStream(&wrapper)) {
            cntl->SetFailed(brpc::EREQUEST, "Fail to serialize request");
            return;
        }
    } else {
        // merge场景，不更新end_key(连续merge和分裂)
        pb::StoreReq add_version_request;
        add_version_request.set_op_type(pb::OP_ADJUSTKEY_AND_ADD_VERSION);
        add_version_request.set_region_id(_region_id);
        add_version_request.set_start_key(request->start_key());
        add_version_request.set_end_key(get_end_key());
        add_version_request.set_region_version(get_version() + 1);
        if (!add_version_request.SerializeToZeroCopyStream(&wrapper)) {
            cntl->SetFailed(brpc::EREQUEST, "Fail to serialize request");
            return;
        }
    }
    MergeClosure* c = new MergeClosure;
    c->is_dst_region = true;
    c->response = response;
    c->done = done_guard.release();
    c->region = this;
    braft::Task task;
    task.data = &data;
    task.done = c;
    _node.apply(task);    
}

void Region::adjustkey_and_add_version(const pb::StoreReq& request, 
                                       braft::Closure* done, 
                                       int64_t applied_index, 
                                       int64_t term) {
    rocksdb::WriteBatch batch;
    batch.Put(_meta_writer->get_handle(), 
              _meta_writer->applied_index_key(_region_id), 
              _meta_writer->encode_applied_index(applied_index, _data_index));
    ON_SCOPE_EXIT(([this, &batch]() {
        _meta_writer->write_batch(&batch, _region_id);
        DB_WARNING("write metainfo when adjustkey and add version, region_id: %ld", 
                   _region_id); 
    }));

    //持久化数据到rocksdb
    pb::RegionInfo region_info_mem;
    copy_region(&region_info_mem);
    region_info_mem.set_version(request.region_version());
    region_info_mem.set_start_key(request.start_key());
    region_info_mem.set_end_key(request.end_key());
    batch.Put(_meta_writer->get_handle(), 
              _meta_writer->region_info_key(_region_id), 
              _meta_writer->encode_region_info(region_info_mem)); 
    if (request.has_new_region_info()) {
        _merge_region_info.CopyFrom(request.new_region_info());
    }
    DB_WARNING("region id:%ld adjustkey and add version (version, start_key"
               "end_key):(%ld, %s, %s)=>(%ld, %s, %s), applied_index:%ld, term:%ld", 
               _region_id, get_version(), 
               str_to_hex(get_start_key()).c_str(),
               str_to_hex(get_end_key()).c_str(),
               request.region_version(), 
               str_to_hex(request.start_key()).c_str(),
               str_to_hex(request.end_key()).c_str(), 
               applied_index, term);
    set_region_with_update_range(region_info_mem);   
    _last_split_time_cost.reset();
}

void Region::validate_and_add_version(const pb::StoreReq& request, 
                                      braft::Closure* done, 
                                      int64_t applied_index, 
                                      int64_t term) {
    rocksdb::WriteBatch batch;
    batch.Put(_meta_writer->get_handle(), 
                _meta_writer->applied_index_key(_region_id), 
                _meta_writer->encode_applied_index(applied_index, _data_index));
    ON_SCOPE_EXIT(([this, &batch]() {
            _meta_writer->write_batch(&batch, _region_id);
            DB_WARNING("write metainfo when add version, region_id: %ld", _region_id); 
        }));
    if (request.split_term() != term || request.split_end_index() + 1 != applied_index) {
        DB_FATAL("split fail, region_id: %ld, new_region_id: %ld, split_term:%ld, "
                "current_term:%ld, split_end_index:%ld, current_index:%ld, disable_write:%d",
                _region_id, _split_param.new_region_id,
                request.split_term(), term, request.split_end_index(), 
                applied_index, _disable_write_cond.count());
        if (done != nullptr) {
            start_thread_to_remove_region(_split_param.new_region_id, _split_param.instance);
            for(auto instance : _split_param.add_peer_instances) {
                start_thread_to_remove_region(_split_param.new_region_id, instance);
            }
            ((SplitClosure*)done)->ret = -1;
            // 如果新region先上报了心跳，但是这里失败，需要手动删除新region
            print_log_entry(request.split_end_index(), applied_index);
        }
        return;
    }
    //持久化数据到rocksdb
    pb::RegionInfo region_info_mem;
    copy_region(&region_info_mem);
    region_info_mem.set_version(request.region_version());
    region_info_mem.set_end_key(request.end_key());
    batch.Put(_meta_writer->get_handle(), _meta_writer->region_info_key(_region_id), _meta_writer->encode_region_info(region_info_mem));
    _new_region_infos.push_back(request.new_region_info());
    if (done != nullptr) {
        ((SplitClosure*)done)->ret = 0;
    }
    DB_WARNING("update region info for all peer,"
                " region_id: %ld, add version %ld=>%ld, number_table_line:%ld, delta_number_table_line:%ld, "
                "applied_index:%ld, term:%ld",
                _region_id, 
                get_version(), request.region_version(),
                _num_table_lines.load(), request.reduce_num_lines(),
                applied_index, term);
    set_region_with_update_range(region_info_mem);
    _last_split_time_cost.reset();
    _approx_info.last_version_region_size = _approx_info.region_size;
    _approx_info.last_version_table_lines = _approx_info.table_lines;
    _approx_info.last_version_time_cost.reset();

    // 分裂后的老region需要删除范围
    _reverse_remove_range.store(true);
    _reverse_unsafe_remove_range.store(true);
    _num_table_lines -= request.reduce_num_lines();
    batch.Put(_meta_writer->get_handle(), _meta_writer->num_table_lines_key(_region_id), _meta_writer->encode_num_table_lines(_num_table_lines));
    std::vector<pb::TransactionInfo> txn_infos;
    txn_infos.reserve(request.txn_infos_size());
    for (auto& txn_info : request.txn_infos()) {
        txn_infos.push_back(txn_info);
    }
    _txn_pool.update_txn_num_rows_after_split(txn_infos);
    // 分裂后主动执行compact
    DB_WARNING("region_id: %ld, new_region_id: %ld, split do compact in queue", 
            _region_id, _split_param.new_region_id);
    compact_data_in_queue();
}

void Region::transfer_leader_after_split() {
    if (_shutdown) {
        return;
    }
    _multi_thread_cond.increase();
    ON_SCOPE_EXIT([this]() {
        _multi_thread_cond.decrease_signal();
    });
    std::vector<braft::PeerId> peers;
    if (!is_leader() || !_node.list_peers(&peers).ok()) {
        return;
    }
    std::string new_leader = _address;
    // 需要apply的logEntry数 * dml_latency
    int64_t min_catch_up_time = INT_FAST64_MAX;
    int64_t new_leader_applied_index = 0;
    for (auto& peer : peers) {
        std::string peer_string = butil::endpoint2str(peer.addr).c_str();
        if (peer_string == _address) {
            continue;
        }
        int64_t peer_applied_index = 0;
        int64_t peer_dml_latency = 0;
        RpcSender::get_peer_applied_index(peer_string, _region_id, peer_applied_index, peer_dml_latency);
        DB_WARNING("region_id: %ld, peer:%s, applied_index:%ld, dml_latency:%ld after split",
                   _region_id, peer_string.c_str(), peer_applied_index, peer_dml_latency);
        int64_t peer_catchup_time = (_applied_index - peer_applied_index) * peer_dml_latency;
        if (peer_catchup_time < min_catch_up_time) {
            new_leader = peer_string;
            min_catch_up_time = peer_catchup_time;
            new_leader_applied_index = peer_applied_index;
        }
    }
    if (new_leader == _address) {
        DB_WARNING("split region: random new leader is equal with address, region_id: %ld", _region_id);
        return;
    }
    if (min_catch_up_time > FLAGS_transfer_leader_catchup_time_threshold) {
        DB_WARNING("split region: peer min catch up time: %ld is too long", min_catch_up_time);
        return;
    }
    pb::RegionStatus expected_status = pb::IDLE;
    if (!_region_control.compare_exchange_strong(expected_status, pb::DOING)) {
        return;
    }
    int ret = transfer_leader_to(new_leader);
    _region_control.reset_region_status();
    if (ret != 0) {
        DB_WARNING("split region: node:%s %s transfer leader fail"
                   " original_leader_applied_index:%ld, new_leader_applied_index:%ld",
                   _node.node_id().group_id.c_str(),
                   _node.node_id().peer_id.to_string().c_str(),
                   _applied_index,
                   new_leader_applied_index);
    } else {
        DB_WARNING("split region: node:%s %s transfer leader success after split,"
                   " original_leader_applied_index:%ld, new_leader:%s new_leader_applied_index:%ld",
                   _node.node_id().group_id.c_str(),
                   _node.node_id().peer_id.to_string().c_str(),
                   _applied_index,
                   new_leader.c_str(),
                   new_leader_applied_index);
    }
}

void Region::add_version_for_split_region(const pb::StoreReq& request, braft::Closure* done, int64_t applied_index, int64_t term) {
    _async_apply_param.stop_adjust_stall();
    if (_async_apply_param.apply_log_failed) {
        // 异步队列执行dml失败，本次分裂失败，马上会删除这个region
        DB_FATAL("add version for split region fail: async apply log failed, region_id %lu", _region_id);
        if (done != nullptr && ((DMLClosure*)done)->response != nullptr) {
            ((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
            ((DMLClosure*)done)->response->set_errmsg("exec failed in execution queue");
        }
        return;
    }
    rocksdb::WriteBatch batch;
    batch.Put(_meta_writer->get_handle(), _meta_writer->applied_index_key(_region_id), 
            _meta_writer->encode_applied_index(applied_index, _data_index));
    pb::RegionInfo region_info_mem;
    copy_region(&region_info_mem);
    region_info_mem.set_version(1);
    region_info_mem.set_status(pb::IDLE);
    region_info_mem.set_start_key(request.start_key());
    batch.Put(_meta_writer->get_handle(), _meta_writer->region_info_key(_region_id), _meta_writer->encode_region_info(region_info_mem));
    int ret = _meta_writer->write_batch(&batch, _region_id);
    //DB_WARNING("write meta info for new split region, region_id: %ld", _region_id);
    if (ret != 0) {
        DB_FATAL("add version for new region when split fail, region_id: %ld", _region_id);
        //回滚一下，上边的compare会把值置为1, 出现这个问题就需要手工删除这个region
        _region_info.set_version(0);
        if (done != nullptr) {
            ((DMLClosure*)done)->response->set_errcode(pb::INTERNAL_ERROR);
            ((DMLClosure*)done)->response->set_errmsg("write region to rocksdb fail");
        }
    } else {
        DB_WARNING("new region add verison, region status was reset, region_id: %ld, "
                    "applied_index:%ld, term:%ld", 
                    _region_id, _applied_index, term);
        _region_control.reset_region_status();
        set_region_with_update_range(region_info_mem);
        if (!compare_and_set_legal()) {
            DB_FATAL("split timeout, region was set split fail, region_id: %ld", _region_id);
            if (done != nullptr) {
                ((DMLClosure*)done)->response->set_errcode(pb::SPLIT_TIMEOUT);
                ((DMLClosure*)done)->response->set_errmsg("split timeout");
            }
            return;
        }
        _last_split_time_cost.reset();
        // 分裂后的新region需要删除范围
        _reverse_remove_range.store(true);
        _reverse_unsafe_remove_range.store(true);
        std::unordered_map<uint64_t, pb::TransactionInfo> prepared_txn;
        _txn_pool.get_prepared_txn_info(prepared_txn, true);
        if (done != nullptr) {
            ((DMLClosure*)done)->response->set_errcode(pb::SUCCESS);
            ((DMLClosure*)done)->response->set_errmsg("success");
            ((DMLClosure*)done)->response->set_affected_rows(_num_table_lines.load());
            ((DMLClosure*)done)->response->clear_txn_infos();
            for (auto &pair : prepared_txn) {
                auto txn_info = ((DMLClosure*)done)->response->add_txn_infos();
                txn_info->CopyFrom(pair.second);
            }

            //分裂完成之后主动做一次transfer_leader, 机器随机选一个
            auto transfer_leader_func = [this] {
                this->transfer_leader_after_split();
            };
            Bthread bth;
            bth.run(transfer_leader_func);
        }
    }
    
}

void Region::clear_orphan_transactions(braft::Closure* done, int64_t applied_index, int64_t term) {
    TimeCost time_cost;
    _txn_pool.clear_orphan_transactions();
    DB_WARNING("region_id: %ld leader clear orphan txn applied_index: %ld  term:%ld cost: %ld",
        _region_id, applied_index, term, time_cost.get_time());
    if (done != nullptr) {
        ((DMLClosure*)done)->response->set_errcode(pb::SUCCESS);
        ((DMLClosure*)done)->response->set_errmsg("success");
    }
}

// leader切换时确保事务状态一致，提交OP_CLEAR_APPLYING_TXN指令清理不一致事务
void Region::apply_clear_transactions_log() {
    _multi_thread_cond.increase();
    ON_SCOPE_EXIT([this]() {
        _multi_thread_cond.decrease_signal();
    });
    BthreadCond clear_applying_txn_cond;
    pb::StoreRes response;
    pb::StoreReq clear_applying_txn_request;
    clear_applying_txn_request.set_op_type(pb::OP_CLEAR_APPLYING_TXN);
    clear_applying_txn_request.set_region_id(_region_id);
    clear_applying_txn_request.set_region_version(get_version());
    butil::IOBuf data;
    butil::IOBufAsZeroCopyOutputStream wrapper(&data);
    if (!clear_applying_txn_request.SerializeToZeroCopyStream(&wrapper)) {
        DB_FATAL("clear log serializeToString fail, region_id: %ld", _region_id);
        return;
    }
    DMLClosure* c = new DMLClosure(&clear_applying_txn_cond);
    clear_applying_txn_cond.increase();
    c->response = &response;
    c->region = this;
    c->cost.reset();
    c->op_type = pb::OP_CLEAR_APPLYING_TXN;
    c->is_sync = true;
    c->remote_side = "127.0.0.1";
    braft::Task task;
    task.data = &data;
    task.done = c;
    _real_writing_cond.increase();
    _node.apply(task);
    clear_applying_txn_cond.wait();
}

void Region::on_shutdown() {
    DB_WARNING("shut down, region_id: %ld", _region_id);
}

void Region::on_leader_start(int64_t term) {
    DB_WARNING("leader start at term:%ld, region_id: %ld", term, _region_id);
    _not_leader_alarm.set_leader_start();
    _region_info.set_leader(butil::endpoint2str(get_leader()).c_str());
    if (!_is_binlog_region) {
        auto clear_applying_txn_fun = [this] {
                this->apply_clear_transactions_log();
            };
        Bthread bth;
        bth.run(clear_applying_txn_fun);
    } else {
        leader_start(term);
    }
}

void Region::on_leader_stop() {
    DB_WARNING("leader stop at term, region_id: %ld", _region_id);
    _is_leader.store(false);
    _not_leader_alarm.reset();
    //只读事务清理
    _txn_pool.on_leader_stop_rollback();
}

void Region::on_leader_stop(const butil::Status& status) {   
    DB_WARNING("leader stop, region_id: %ld, error_code:%d, error_des:%s",
                _region_id, status.error_code(), status.error_cstr());
    _is_leader.store(false);
    _txn_pool.on_leader_stop_rollback();
}

void Region::on_error(const ::braft::Error& e) {
    DB_FATAL("raft node meet error, is_learner:%d, region_id: %ld, error_type:%d, error_desc:%s",
                is_learner(), _region_id, e.type(), e.status().error_cstr());
    _region_status = pb::STATUS_ERROR;
}

void Region::on_configuration_committed(const::braft::Configuration& conf) {
    on_configuration_committed(conf, 0);
}

void Region::on_configuration_committed(const::braft::Configuration& conf, int64_t index) {
    if (get_version() == 0) {
        wait_async_apply_log_queue_empty();
    }
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
    braft::PeerId leader;
    if (!is_learner()) {
        leader = _node.leader_id();
    }
    tmp_region.set_leader(butil::endpoint2str(leader.addr).c_str());
    set_region(tmp_region);
    if (_meta_writer->update_region_info(tmp_region) != 0) {
        DB_FATAL("update region info failed, region_id: %ld", _region_id); 
    } else {
        DB_WARNING("update region info success, region_id: %ld", _region_id);
    }
    DB_WARNING("region_id: %ld, configurantion:%s leader:%s, log_index: %ld",
                _region_id, conf_str.c_str(),
                butil::endpoint2str(get_leader()).c_str(), index); 
}

void Region::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
    TimeCost time_cost;
    brpc::ClosureGuard done_guard(done);
    if (get_version() == 0) {
        wait_async_apply_log_queue_empty();
    }
    if (writer->add_file(SNAPSHOT_META_FILE) != 0 
            || writer->add_file(SNAPSHOT_DATA_FILE) != 0) {
        done->status().set_error(EINVAL, "Fail to add snapshot");
        DB_WARNING("Error while adding extra_fs to writer, region_id: %ld", _region_id);
        return;
    }
    DB_WARNING("region_id: %ld snapshot save complete, time_cost: %ld", 
                _region_id, time_cost.get_time());
    reset_snapshot_status();
}

void Region::reset_snapshot_status() {
    if (_snapshot_time_cost.get_time() > FLAGS_snapshot_interval_s * 1000 * 1000) {
        _snapshot_num_table_lines = _num_table_lines.load();
        _snapshot_index = _applied_index;
        _snapshot_time_cost.reset();
    }
}
void Region::snapshot(braft::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    bool need_snapshot = false;
    if (_shutdown) {
        return;
    }
    // 如果在进行操作，不进行snapshot
    if (_region_control.get_status() != pb::IDLE) {
        DB_WARNING("region_id: %ld status is not idle", _region_id);
        return;
    }
    //add peer进行中的snapshot，导致learner数据不全
    if (get_version() == 0) {
        DB_WARNING("region_id: %ld split or addpeer", _region_id);
        return;
    }
    if (_snapshot_time_cost.get_time() < FLAGS_snapshot_interval_s * 1000 * 1000) {
        return;
    }
    int64_t average_cost = _dml_time_cost.latency();
    if (_applied_index - _snapshot_index > FLAGS_snapshot_diff_logs) {
        need_snapshot = true;
    } else if (abs(_snapshot_num_table_lines - _num_table_lines.load()) > FLAGS_snapshot_diff_lines) {
        need_snapshot = true;
    } else if ((_applied_index - _snapshot_index) * average_cost
                > FLAGS_snapshot_log_exec_time_s * 1000 * 1000) {
        need_snapshot = true;
    } else if (_snapshot_time_cost.get_time() > 2 * FLAGS_snapshot_interval_s * 1000 * 1000
            && _applied_index > _snapshot_index) {
        need_snapshot = true;
    }
    if (!need_snapshot) {
        return;
    }
    DB_WARNING("region_id: %ld do snapshot, snapshot_num_table_lines:%ld, num_table_lines:%ld "
            "snapshot_index:%ld, applied_index:%ld, snapshot_inteval_s:%ld",
            _region_id, _snapshot_num_table_lines, _num_table_lines.load(),
            _snapshot_index, _applied_index, _snapshot_time_cost.get_time() / 1000 / 1000);
    done_guard.release();
    if (is_learner()) {
        _learner->snapshot(done);
    } else {
        _node.snapshot(done);
    }
}
void Region::on_snapshot_load_for_restart(braft::SnapshotReader* reader, 
        std::map<int64_t, std::string>& prepared_log_entrys) {
     //不管是哪种启动方式，没有commit的日志都通过log_entry恢复, 所以prepared事务要回滚
    TimeCost time_cost;
    _txn_pool.clear();
    std::unordered_map<uint64_t, int64_t> prepared_log_indexs;
    int64_t start_log_index = INT64_MAX;
    std::set<uint64_t> txn_ids;
    _meta_writer->read_applied_index(_region_id, &_applied_index, &_data_index);
    _num_table_lines = _meta_writer->read_num_table_lines(_region_id);

    int64_t snapshot_index = parse_snapshot_index_from_path(reader->get_path(), false);

    if (FLAGS_force_clear_txn_for_fast_recovery) {
        _meta_writer->clear_txn_log_index(_region_id);
        DB_WARNING("region_id: %ld force clear txn info", _region_id);
        return;
    }

    //没有commited事务的log_index
    
    _meta_writer->parse_txn_log_indexs(_region_id, prepared_log_indexs);
    for (auto log_index_pair : prepared_log_indexs) {
        uint64_t txn_id = log_index_pair.first;
        int64_t log_index = log_index_pair.second;
        int64_t num_table_lines = 0;
        int64_t applied_index = 0;
        bool is_rollback = false;
        std::string log_entry;
        //存在pre_commit日志，但不存在prepared的事务，说明系统停止在commit 和 write_meta之间
        if (_meta_writer->read_pre_commit_key(_region_id, txn_id, num_table_lines, applied_index) == 0
                && (!Store::get_instance()->exist_prepared_log(_region_id, txn_id))) {
            ON_SCOPE_EXIT(([this, txn_id]() {
                _meta_writer->clear_error_pre_commit(_region_id, txn_id);
            }));
            if (applied_index < _applied_index) {
                DB_FATAL("pre_commit applied_index, _region_id: %ld, pre_applied_index: %ld, applied_index: %ld",
                            _region_id, applied_index, _applied_index);
                continue;
            }
            int ret = LogEntryReader::get_instance()->read_log_entry(_region_id, applied_index, log_entry);
            if (ret < 0) {
                DB_FATAL("read committed log entry fail, _region_id: %ld, log_index: %ld",
                            _region_id, applied_index);
                continue;
            }
            pb::StoreReq store_req;
            if (!store_req.ParseFromString(log_entry)) {
                DB_FATAL("parse commit exec plan fail from log entry, region_id: %ld log_index: %ld",
                            _region_id, applied_index);
                continue;
            } else if (store_req.op_type() != pb::OP_COMMIT && store_req.op_type() != pb::OP_ROLLBACK) {
                DB_FATAL("op_type is not commit when parse log entry, region_id: %ld log_index: %ld enrty: %s",
                            _region_id, applied_index, store_req.ShortDebugString().c_str());
                continue;
            } else if (store_req.op_type() == pb::OP_ROLLBACK) {
                is_rollback = true;
            }
            //手工erase掉meta信息，applied_index + 1, 系统挂在commit事务和write meta信息之间，事务已经提交，不需要重放
            ret = _meta_writer->write_meta_after_commit(_region_id, num_table_lines, applied_index,
                    _data_index, txn_id, is_rollback);
            DB_WARNING("write meta info wheen on snapshot load for restart"
                        " region_id: %ld, applied_index: %ld, txn_id: %lu", 
                        _region_id, applied_index, txn_id); 
            if (ret < 0) {
                DB_FATAL("Write Metainfo fail, region_id: %ld, txn_id: %lu, log_index: %ld", 
                            _region_id, txn_id, applied_index);
            }
        } else {
        //系统在执行commit之前重启
            if (log_index < start_log_index) {
                start_log_index = log_index;
            }
            txn_ids.insert(txn_id);
            
        }
    }
    int64_t max_applied_index = std::max(snapshot_index, _applied_index);
    int ret = LogEntryReader::get_instance()->read_log_entry(_region_id, start_log_index, max_applied_index, txn_ids, prepared_log_entrys);
    if (ret < 0) {
        DB_FATAL("read prepared and not commited log entry fail, _region_id: %ld, log_index: %ld",
                    _region_id, start_log_index);
        return;
    }
    DB_WARNING("success load snapshot, snapshot file not exist, "
                "region_id: %ld, prepared_log_size: %ld,"
                "applied_index:%ld data_index:%ld, raft snapshot index: %ld"
                " prepared_log_entrys_size: %ld, time_cost: %ld",
                _region_id, prepared_log_indexs.size(), 
                _applied_index, _data_index, snapshot_index,
                prepared_log_entrys.size(), time_cost.get_time());
}

int Region::on_snapshot_load(braft::SnapshotReader* reader) {
    reset_timecost();
    TimeCost time_cost;
    DB_WARNING("region_id: %ld start to on snapshot load", _region_id);
    ON_SCOPE_EXIT([this]() {
        _meta_writer->clear_doing_snapshot(_region_id);
        DB_WARNING("region_id: %ld on snapshot load over", _region_id);
    });
    std::string data_sst_file = reader->get_path() + SNAPSHOT_DATA_FILE_WITH_SLASH;
    std::string meta_sst_file = reader->get_path() + SNAPSHOT_META_FILE_WITH_SLASH;
    boost::filesystem::path snapshot_meta_file = meta_sst_file;
    std::map<int64_t, std::string> prepared_log_entrys; 
    //本地重启， 不需要加载snasphot
    if (_restart && !Store::get_instance()->doing_snapshot_when_stop(_region_id)) {
        DB_WARNING("region_id: %ld, restart no snapshot sst", _region_id);
        on_snapshot_load_for_restart(reader, prepared_log_entrys);
        if (_is_binlog_region) {
            int ret_binlog = binlog_reset_on_snapshot_load_restart();
            if (ret_binlog != 0) {
                return -1;
            }
        }
    } else if (!boost::filesystem::exists(snapshot_meta_file)) {
        DB_FATAL(" region_id: %ld, no meta_sst file", _region_id);
        return -1;
    } else {
        //正常snapshot过程中没加载完，重启需要重新ingest sst。
        _meta_writer->write_doing_snapshot(_region_id);
        DB_WARNING("region_id: %ld doing on snapshot load", _region_id);
        int ret = 0;
        if (is_addpeer()) {
            ret = Concurrency::get_instance()->snapshot_load_concurrency.increase_wait();
            DB_WARNING("snapshot load, region_id: %ld, wait_time:%ld, ret:%d", 
                    _region_id, time_cost.get_time(), ret);
        }
        ON_SCOPE_EXIT(([this]() {
            if (is_addpeer()) {
                Concurrency::get_instance()->snapshot_load_concurrency.decrease_broadcast();
                if (_need_decrease) {
                    _need_decrease = false;
                    Concurrency::get_instance()->recieve_add_peer_concurrency.decrease_broadcast();
                }
            }
        }));
        //不管是哪种启动方式，没有commit的日志都通过log_entry恢复, 所以prepared事务要回滚
        _txn_pool.clear();
        //清空数据
        if (_region_info.version() != 0) {
            int64_t old_data_index = _data_index;
            DB_WARNING("region_id: %ld, clear_data on_snapshot_load", _region_id);
            _meta_writer->clear_meta_info(_region_id);
            int ret_meta = RegionControl::ingest_meta_sst(meta_sst_file, _region_id);
            if (ret_meta < 0) {
                DB_FATAL("ingest sst fail, region_id: %ld", _region_id);
                return -1;
            }
            _meta_writer->read_applied_index(_region_id, &_applied_index, &_data_index);
            boost::filesystem::path snapshot_first_data_file = data_sst_file + "0";
            //如果新的_data_index和old_data_index一样，则不需要清理数据，而且leader也不会发送数据
            //如果存在data_sst并且是重启过程，则清理后走重启故障恢复
            if (_data_index > old_data_index || 
                (_restart && boost::filesystem::exists(snapshot_first_data_file))) {
                //删除preapred 但没有committed的事务
                _txn_pool.clear();
                RegionControl::remove_data(_region_id);
                // ingest sst
                ret = ingest_snapshot_sst(reader->get_path());
                if (ret != 0) {
                    DB_FATAL("ingest sst fail when on snapshot load, region_id: %ld", _region_id);
                    return -1;
                }
            } else {
                DB_WARNING("region_id: %ld, no need clear_data, data_index:%ld, old_data_index:%ld",
                        _region_id, _data_index, old_data_index);
            }
        } else {
            // check snapshot size
            if (is_learner()) {
                if (check_learner_snapshot() != 0) {
                    DB_FATAL("region %ld check learner snapshot error.", _region_id);
                    return -1;
                }
            } else {
                if (check_follower_snapshot(butil::endpoint2str(get_leader()).c_str()) != 0) {
                    DB_FATAL("region %ld check follower snapshot error.", _region_id);
                    return -1;
                }
            }
            // 先ingest data再ingest meta
            // 这样add peer遇到重启时，直接靠version=0可以清理掉残留region
            ret = ingest_snapshot_sst(reader->get_path());
            if (ret != 0) {
                DB_FATAL("ingest sst fail when on snapshot load, region_id: %ld", _region_id);
                return -1;
            }
            int ret_meta = RegionControl::ingest_meta_sst(meta_sst_file, _region_id);
            if (ret_meta < 0) {
                DB_FATAL("ingest sst fail, region_id: %ld", _region_id);
                return -1;
            }
        }
        _meta_writer->parse_txn_infos(_region_id, prepared_log_entrys);
        ret = _meta_writer->clear_txn_infos(_region_id);
        if (ret != 0) {
            DB_FATAL("clear txn infos from rocksdb fail when on snapshot load, region_id: %ld", _region_id);
            return -1;
        }
        if (_is_binlog_region) {
            int ret_binlog = binlog_reset_on_snapshot_load();
            if (ret_binlog != 0) {
                return -1;
            }
        }
        DB_WARNING("success load snapshot, ingest sst file, region_id: %ld", _region_id);
    }
    // 读出来applied_index, 重放事务指令会把applied index覆盖, 因此必须在回放指令之前把applied index提前读出来
    //恢复内存中applied_index 和number_table_line
    _meta_writer->read_applied_index(_region_id, &_applied_index, &_data_index);
    _num_table_lines = _meta_writer->read_num_table_lines(_region_id);
    pb::RegionInfo region_info;
    int ret = _meta_writer->read_region_info(_region_id, region_info);
    if (ret < 0) {
        DB_FATAL("read region info fail on snapshot load, region_id: %ld", _region_id);
        return -1;
    }
    if (_applied_index <= 0) {
        DB_FATAL("recovery applied index or num table line fail,"
                    " _region_id: %ld, applied_index: %ld",
                    _region_id, _applied_index);
        return -1;
    }
    if (_num_table_lines < 0) {
        DB_WARNING("num table line fail,"
                    " _region_id: %ld, num_table_line: %ld",
                    _region_id, _num_table_lines.load());
        _meta_writer->update_num_table_lines(_region_id, 0);
        _num_table_lines = 0;
    }
    region_info.set_can_add_peer(true);
    set_region_with_update_range(region_info);
    if (!compare_and_set_legal()) {
        DB_FATAL("region is not illegal, should be removed, region_id: %ld", _region_id);
        return -1;
    }
    _new_region_infos.clear();
    _snapshot_num_table_lines = _num_table_lines.load();
    _snapshot_index = _applied_index;
    _snapshot_time_cost.reset();
    copy_region(&_resource->region_info);

    //回放没有commit的事务
    if (!FLAGS_force_clear_txn_for_fast_recovery) {
        for (auto log_entry_pair : prepared_log_entrys) {
            int64_t log_index = log_entry_pair.first;
            pb::StoreReq store_req;
            if (!store_req.ParseFromString(log_entry_pair.second)) {
                DB_FATAL("parse prepared exec plan fail from log entry, region_id: %ld", _region_id);
                return -1;
            }
            if (store_req.op_type() == pb::OP_KV_BATCH) {
                apply_kv_in_txn(store_req, nullptr, log_index, 0);
            } else {
                apply_txn_request(store_req, nullptr, log_index, 0);
            }
            const pb::TransactionInfo& txn_info = store_req.txn_infos(0);
            DB_WARNING("recovered not committed transaction, region_id: %ld,"
                " log_index: %ld op_type: %s txn_id: %lu seq_id: %d primary_region_id:%ld",
                _region_id, log_index, pb::OpType_Name(store_req.op_type()).c_str(),
                txn_info.txn_id(), txn_info.seq_id(), txn_info.primary_region_id());
        }
    }
    //如果有回放请求，apply_index会被覆盖，所以需要重新写入
    if (prepared_log_entrys.size() != 0) {
        _meta_writer->update_apply_index(_region_id, _applied_index, _data_index);
        DB_WARNING("update apply index when on_snapshot_load, region_id: %ld, apply_index: %ld",
                    _region_id, _applied_index);
    }
    _last_split_time_cost.reset();

    DB_WARNING("snapshot load success, region_id: %ld, num_table_lines: %ld,"
                " applied_index:%ld, data_index:%ld, region_info: %s, cost:%ld _restart:%d",
                _region_id, _num_table_lines.load(), _applied_index, _data_index,
                region_info.ShortDebugString().c_str(), time_cost.get_time(), _restart);
    // 分裂的时候不能做snapshot
    if (!_restart && !is_learner() && region_info.version() != 0) {
        auto run_snapshot = [this] () {
            _multi_thread_cond.increase();
            ON_SCOPE_EXIT([this]() {
                    _multi_thread_cond.decrease_signal();
            });
            // 延迟做snapshot，等到snapshot_load结束，懒得搞条件变量了
            bthread_usleep(5 * 1000 * 1000LL);
            _region_control.sync_do_snapshot();
            bthread_usleep_fast_shutdown(FLAGS_store_heart_beat_interval_us * 10, _shutdown);
            _report_peer_info = true;
        };
        Bthread bth;
        bth.run(run_snapshot);
    }
    _restart = false;
    _learner_ready_for_read = true;
    return 0;
}

int Region::ingest_snapshot_sst(const std::string& dir) {
    typedef boost::filesystem::directory_iterator dir_iter;
    dir_iter iter(dir);
    dir_iter end;
    int cnt = 0;
    for (; iter != end; ++iter) {
        std::string child_path = iter->path().c_str();
        std::vector<std::string> split_vec;
        boost::split(split_vec, child_path, boost::is_any_of("/"));
        std::string out_path = split_vec.back();
        if (boost::istarts_with(out_path, SNAPSHOT_DATA_FILE)) {
            std::string link_path = dir + "/link." + out_path;
            // 建一个硬链接，通过ingest采用move方式，可以不用修改异常恢复流程
            // 失败了说明之前创建过，可忽略
            link(child_path.c_str(), link_path.c_str());
            DB_WARNING("region_id: %ld, ingest file:%s", _region_id, link_path.c_str());
            // 重启过程无需等待
            if (is_addpeer() && !_restart) {
                bool wait_success = wait_rocksdb_normal(3600 * 1000 * 1000LL);
                if (!wait_success) {
                    DB_FATAL("ingest sst fail, wait timeout, region_id: %ld", _region_id);
                    return -1;
                }
            }
            int ret_data = RegionControl::ingest_data_sst(link_path, _region_id, true);
            if (ret_data < 0) {
                DB_FATAL("ingest sst fail, region_id: %ld", _region_id);
                return -1;
            }

            cnt++;
        }
    }
    if (cnt == 0) {
        DB_WARNING("region_id: %ld is empty when on snapshot load", _region_id);
    }
    return 0;
}

int Region::ingest_sst(const std::string& data_sst_file, const std::string& meta_sst_file) {
    if (boost::filesystem::exists(boost::filesystem::path(data_sst_file))) {
        int ret_data = RegionControl::ingest_data_sst(data_sst_file, _region_id, false);
        if (ret_data < 0) {
            DB_FATAL("ingest sst fail, region_id: %ld", _region_id);
            return -1;
        }
 
    } else {
        DB_WARNING("region_id: %ld is empty when on snapshot load", _region_id);
    }

    if (boost::filesystem::exists(boost::filesystem::path(meta_sst_file)) && 
        boost::filesystem::file_size(boost::filesystem::path(meta_sst_file)) > 0) {
        int ret_meta = RegionControl::ingest_meta_sst(meta_sst_file, _region_id);
        if (ret_meta < 0) {
            DB_FATAL("ingest sst fail, region_id: %ld", _region_id);
            return -1;
        }
    }
    return 0;
}
/*
int Region::clear_data() {
    //删除preapred 但没有committed的事务
    _txn_pool.clear();
    RegionControl::remove_data(_region_id);
    _meta_writer->clear_meta_info(_region_id);
    // 单线程执行compact
    DB_WARNING("region_id: %ld, clear_data do compact in queue", _region_id);
    compact_data_in_queue();
    return 0;
}
*/
int Region::check_learner_snapshot() {
    std::vector<std::string> peers;
    peers.reserve(3);
    {
        std::lock_guard<std::mutex> lock(_region_lock);
        for (auto& peer : _region_info.peers()) {
            peers.emplace_back(peer);
        }
    }
    uint64_t peer_data_size = 0;
    uint64_t peer_meta_size = 0;
    int64_t snapshot_index = 0;
    for (auto& peer : peers) {
        RpcSender::get_peer_snapshot_size(peer, 
            _region_id, &peer_data_size, &peer_meta_size, &snapshot_index);
        DB_WARNING("region_id: %ld, peer %s snapshot_index: %ld "
            "send_data_size:%lu, recieve_data_size:%lu "
            "send_meta_size:%lu, recieve_meta_size:%lu "
            "region_info: %s",
                _region_id, peer.c_str(), snapshot_index, peer_data_size, snapshot_data_size(),
                peer_meta_size, snapshot_meta_size(),
                _region_info.ShortDebugString().c_str());
        if (snapshot_index == 0) {
            // 兼容未上线该版本的store.
            DB_WARNING("region_id: %ld peer %s snapshot is 0.", _region_id, peer.c_str());
            return 0;
        }
        if (peer_data_size == snapshot_data_size() && peer_meta_size == snapshot_meta_size()) {
            return 0;
        }
    }
    return -1;    
}

int Region::check_follower_snapshot(const std::string& peer) {
    uint64_t peer_data_size = 0;
    uint64_t peer_meta_size = 0;
    int64_t snapshot_index = 0;
    RpcSender::get_peer_snapshot_size(peer, 
            _region_id, &peer_data_size, &peer_meta_size, &snapshot_index);
    DB_WARNING("region_id: %ld is new, no need clear_data, "
            "send_data_size:%lu, recieve_data_size:%lu."
            "send_meta_size:%lu, recieve_meta_size:%lu."
            "region_info: %s",
                _region_id, peer_data_size, snapshot_data_size(),
                peer_meta_size, snapshot_meta_size(),
                _region_info.ShortDebugString().c_str());
    if (peer_data_size != 0 && snapshot_data_size() != 0 && peer_data_size != snapshot_data_size()) {
        DB_FATAL("check snapshot size fail, send_data_size:%lu, recieve_data_size:%lu, region_id: %ld", 
                peer_data_size, snapshot_data_size(), _region_id);
        return -1;
    }
    if (peer_meta_size != 0 && snapshot_meta_size() != 0 && peer_meta_size != snapshot_meta_size()) {
        DB_FATAL("check snapshot size fail, send_data_size:%lu, recieve_data_size:%lu, region_id: %ld", 
                peer_meta_size, snapshot_meta_size(), _region_id);
        return -1;
    }
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
    // split后只做一次
    bool remove_range = false;
    remove_range = _reverse_remove_range.load();
    _reverse_remove_range.store(false);
    // 功能先不上，再追查下reverse_merge性能
    //remove_range = false;

    std::map<int64_t, ReverseIndexBase*> reverse_merge_index_map {};
    {
        BAIDU_SCOPED_LOCK(_reverse_index_map_lock);
        if (_reverse_index_map.empty()) {
            return;
        }
        reverse_merge_index_map = _reverse_index_map;
    }
    update_unsafe_reverse_index_map(reverse_merge_index_map);
    
    auto resource = get_resource();
    for (auto& pair : reverse_merge_index_map) {
        int64_t reverse_index_id = pair.first;
        {
            BAIDU_SCOPED_LOCK(_reverse_unsafe_index_map_lock);
            if (_reverse_unsafe_index_map.count(reverse_index_id) == 1) {
                continue;
            }
        }
        pair.second->reverse_merge_func(resource->region_info, remove_range);
    }
    //DB_WARNING("region_id: %ld reverse merge:%lu", _region_id, cost.get_time());
}

void Region::reverse_merge_doing_ddl() {
    if (_shutdown) {
        return;
    }
    _multi_thread_cond.increase();
    ON_SCOPE_EXIT([this]() {
        _multi_thread_cond.decrease_signal();
    });
    
    // split后只做一次
    bool remove_range = false;
    remove_range = _reverse_unsafe_remove_range.load();
    _reverse_unsafe_remove_range.store(false);

    std::map<int64_t, ReverseIndexBase*> reverse_unsafe_index_map;
    {
        BAIDU_SCOPED_LOCK(_reverse_unsafe_index_map_lock);
        if (_reverse_unsafe_index_map.empty()) {
            return;
        }
        reverse_unsafe_index_map = _reverse_unsafe_index_map;
    }

    auto resource = get_resource();
    for (auto& pair : reverse_unsafe_index_map) {
        int64_t reverse_index_id = pair.first;
        auto index_info = _factory->get_index_info(reverse_index_id);
        if (index_info.state == pb::IS_PUBLIC) {
            pair.second->reverse_merge_func(resource->region_info, remove_range);
            BAIDU_SCOPED_LOCK(_reverse_unsafe_index_map_lock);
            _reverse_unsafe_index_map.erase(reverse_index_id);
        } else {
            pair.second->reverse_merge_func(resource->region_info, remove_range);
        }
    }
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
    rocksdb::ReadOptions read_options;
    read_options.fill_cache = false;
    //read_option.prefix_same_as_start = true;
    std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, RocksWrapper::DATA_CF));

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

bool Region::has_sst_data(int64_t* seek_table_lines) {
    int64_t global_index_id = get_global_index_id();
    MutTableKey table_prefix;
    table_prefix.append_i64(_region_id).append_i64(global_index_id);
    std::string end_key = get_end_key();
    MutTableKey upper_bound;
    upper_bound.append_i64(_region_id).append_i64(global_index_id);
    if (end_key.empty()) {
        upper_bound.append_u64(UINT64_MAX);
        upper_bound.append_u64(UINT64_MAX);
        upper_bound.append_u64(UINT64_MAX);
    } else {
        upper_bound.append_string(end_key);
    }
    rocksdb::Slice upper_bound_slice = upper_bound.data();
    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    read_options.total_order_seek = false;
    read_options.fill_cache = false;
    // TODO iterate_upper_bound边界判断，其他地方也需要改写
    read_options.iterate_upper_bound = &upper_bound_slice;
    std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _data_cf));
    if (seek_table_lines == nullptr) {
        iter->Seek(table_prefix.data());
        return iter->Valid();
    } else {
        for (iter->Seek(table_prefix.data()); iter->Valid(); iter->Next()) {
            ++(*seek_table_lines);
        }
        return *seek_table_lines > 0;
    }
}

//region处理merge的入口方法
void Region::start_process_merge(const pb::RegionMergeResponse& merge_response) {
    int ret = 0;
    if (_shutdown) {
        return;
    }
    _multi_thread_cond.increase();
    ON_SCOPE_EXIT([this]() {
        _multi_thread_cond.decrease_signal();
    });
    if (!is_leader()) {
        DB_FATAL("leader transfer when merge, merge fail, region_id: %ld", _region_id);
        return;
    }
    pb::RegionStatus expected_status = pb::IDLE; 
    if (!_region_control.compare_exchange_strong(expected_status, pb::DOING)) {
        DB_FATAL("merge fail, region status is not idle when start merge,"
                 " region_id: %ld", _region_id);
        return;
    }   
    //设置禁写 并且等待正在写入任务提交
    _disable_write_cond.increase();
    int64_t disable_write_wait = get_split_wait_time();
    ScopeMergeStatus merge_status(this);
    ret = _real_writing_cond.timed_wait(disable_write_wait);
    if (ret != 0) {
        DB_FATAL("_real_writing_cond wait timeout, region_id: %ld", _region_id);
        return;
    }
    //等待写结束之后，判断_applied_index,如果有写入则不可继续执行
    if (_applied_index != _applied_index_lastcycle) {
        DB_WARNING("region_id:%ld merge fail, apply index %ld change to %ld",
                  _region_id, _applied_index_lastcycle, _applied_index);
        return;
    }

    // 插入一步，读取主表确认是否真的没数据
    int64_t seek_table_lines = 0;
    has_sst_data(&seek_table_lines);

    if (seek_table_lines > 0) {
        DB_FATAL("region_id: %ld merge fail, seek_table_lines:%ld > 0", 
                _region_id, seek_table_lines);
        // 有数据就更新_num_table_lines
        _num_table_lines = seek_table_lines;
        return;
    }

    DB_WARNING("start merge (id, version, start_key, end_key), src (%ld, %ld, %s, %s) "
               "vs dst (%ld, %ld, %s, %s)", _region_id, get_version(), 
               str_to_hex(get_start_key()).c_str(), 
               str_to_hex(get_end_key()).c_str(), 
               merge_response.dst_region_id(), merge_response.version(), 
               str_to_hex(merge_response.dst_start_key()).c_str(), 
               str_to_hex(merge_response.dst_end_key()).c_str());
    if (get_start_key() == get_end_key() 
       || merge_response.dst_start_key() == merge_response.dst_end_key()
       || get_end_key() < merge_response.dst_start_key()
       || merge_response.dst_start_key() < get_start_key()
       || end_key_compare(get_end_key(), merge_response.dst_end_key()) > 0) {
        DB_WARNING("src region_id:%ld, dst region_id:%ld can`t merge", 
                  _region_id, merge_response.dst_region_id());
        return;
    }
    TimeCost time_cost;  
    int retry_times = 0;
    pb::StoreReq request;
    pb::StoreRes response;
    std::string dst_instance;
    if (merge_response.has_dst_region() && merge_response.dst_region().region_id() != 0) {
        request.set_op_type(pb::OP_ADJUSTKEY_AND_ADD_VERSION);
        request.set_start_key(get_start_key());
        request.set_end_key(merge_response.dst_region().end_key());
        request.set_region_id(merge_response.dst_region().region_id());
        request.set_region_version(merge_response.dst_region().version());
        dst_instance = merge_response.dst_region().leader();
    } else {
        request.set_op_type(pb::OP_ADJUSTKEY_AND_ADD_VERSION);
        request.set_start_key(get_start_key());
        request.set_end_key(merge_response.dst_end_key());
        request.set_region_id(merge_response.dst_region_id());
        request.set_region_version(merge_response.version());
        dst_instance = merge_response.dst_instance();
    }
    uint64_t log_id = butil::fast_rand();
    do {
        response.Clear();
        StoreInteract store_interact(dst_instance);
        ret = store_interact.send_request_for_leader(log_id, "query", request, response);
        if (ret == 0) {
            break;
        }
        DB_FATAL("region merge fail when add version for merge, "
                 "region_id: %ld, dst_region_id:%ld, instance:%s",
                 _region_id, merge_response.dst_region_id(),
                 merge_response.dst_instance().c_str());
        if (response.errcode() == pb::NOT_LEADER) {
            if (++retry_times > 3) {
                return;
            }
            if (response.leader() != "0.0.0.0:0") {
                dst_instance = response.leader();
                DB_WARNING("region_id: %ld, dst_region_id:%ld, send to new leader:%s", 
                        _region_id, merge_response.dst_region_id(), dst_instance.c_str());
                continue;
            } else if (merge_response.has_dst_region() && merge_response.dst_region().peers_size() > 1) {
                for (auto& is : merge_response.dst_region().peers()) {
                    if (is != dst_instance) {
                        dst_instance = is;
                        DB_WARNING("region_id: %ld, dst_region_id:%ld, send to new leader:%s", 
                                _region_id, merge_response.dst_region_id(), dst_instance.c_str());
                        break;
                    }
                }
                continue;
            }
        }
        if (response.errcode() == pb::VERSION_OLD) {
            if (++retry_times > 3) {
                return;
            }
            bool find = false;
            pb::RegionInfo store_region;
            for (auto& region : response.regions()) {
                if (region.region_id() == merge_response.dst_region_id()) {
                    store_region = region;
                    find = true;
                    break;
                }
            }
            if (!find) {
                DB_FATAL("can`t find dst region id:%ld", merge_response.dst_region_id());
                return;
            }
            DB_WARNING("start merge again (id, version, start_key, end_key), "
                       "src (%ld, %ld, %s, %s) vs dst (%ld, %ld, %s, %s)", 
                       _region_id, get_version(), 
                       str_to_hex(get_start_key()).c_str(), 
                       str_to_hex(get_end_key()).c_str(), 
                       store_region.region_id(), store_region.version(), 
                       str_to_hex(store_region.start_key()).c_str(), 
                       str_to_hex(store_region.end_key()).c_str());
            if (get_start_key() == get_end_key() 
                    || store_region.start_key() == store_region.end_key()
                    || get_end_key() < store_region.start_key()
                    || store_region.start_key() < get_start_key()
                    || end_key_compare(get_end_key(), store_region.end_key()) > 0) {
                DB_WARNING("src region_id:%ld, dst region_id:%ld can`t merge", 
                           _region_id, store_region.region_id());
                return;
            }
            if (get_start_key() == store_region.start_key()) {
                break;
            }
            request.set_region_version(store_region.version());
            request.set_start_key(get_start_key());
            request.set_end_key(store_region.end_key());
            continue;
        }
        return;
    } while (true);
    DB_WARNING("region merge success when add version for merge, "
             "region_id: %ld, dst_region_id:%ld, instance:%s, time_cost:%ld",
             _region_id, merge_response.dst_region_id(),
             merge_response.dst_instance().c_str(), time_cost.get_time());
    //check response是否正确
    pb::RegionInfo dst_region_info;
    if (response.regions_size() > 0) {
        bool find = false;
        for (auto& region : response.regions()) {
            if (region.region_id() == merge_response.dst_region_id()) {
                dst_region_info = region;
                find = true;
                break;
            }
        }
        if (!find) {
            DB_FATAL("can`t find dst region id:%ld", merge_response.dst_region_id());
            return;
        }
        if (dst_region_info.region_id() == merge_response.dst_region_id() 
            && dst_region_info.start_key() == get_start_key()) {
            DB_WARNING("merge get dst region success, region_id:%ld, version:%ld", 
                      dst_region_info.region_id(), dst_region_info.version());
        } else {
            DB_FATAL("get dst region fail, expect dst region id:%ld, start key:%s, version:%ld, "
                     "but the response is id:%ld, start key:%s, version:%ld", 
                     merge_response.dst_region_id(), 
                     str_to_hex(get_start_key()).c_str(), 
                     merge_response.version() + 1, 
                     dst_region_info.region_id(), 
                     str_to_hex(dst_region_info.start_key()).c_str(), 
                     dst_region_info.version());
            return;
        }
    } else {
        DB_FATAL("region:%ld, response fetch dst region fail", _region_id);
        return;
    }
    
    pb::StoreReq add_version_request;
    add_version_request.set_op_type(pb::OP_ADJUSTKEY_AND_ADD_VERSION);
    add_version_request.set_region_id(_region_id);
    add_version_request.set_start_key(get_start_key());
    add_version_request.set_end_key(get_start_key());
    add_version_request.set_region_version(get_version() + 1);
    *(add_version_request.mutable_new_region_info()) = dst_region_info;
    butil::IOBuf data;
    butil::IOBufAsZeroCopyOutputStream wrapper(&data);
    if (!add_version_request.SerializeToZeroCopyStream(&wrapper)) {
        //把状态切回来
        DB_FATAL("start merge fail, serializeToString fail, region_id: %ld", _region_id);
        return;
    }
    merge_status.reset();
    MergeClosure* c = new MergeClosure;
    c->is_dst_region = false;
    c->response = nullptr;
    c->done = nullptr;
    c->region = this;
    braft::Task task;
    task.data = &data;
    task.done = c;
    _node.apply(task);
}

//region处理split的入口方法
//该方法构造OP_SPLIT_START请求，收到请求后，记录分裂开始时的index, 迭代器等一系列状态
void Region::start_process_split(const pb::RegionSplitResponse& split_response,
                                 bool tail_split,
                                 const std::string& split_key,
                                 int64_t key_term) {
    if (_shutdown) {
        baikaldb::Store::get_instance()->sub_split_num();
        return;
    }
    if (!tail_split && split_key.compare(get_end_key()) > 0) {
        baikaldb::Store::get_instance()->sub_split_num();
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
    for(auto& instance : split_response.add_peer_instance()) {
        _split_param.add_peer_instances.emplace_back(instance);
    }
    if (!tail_split) {
        _split_param.split_key = split_key;
        _split_param.split_term = key_term;
    }
    DB_WARNING("start split, region_id: %ld, version:%ld, new_region_id: %ld, "
            "split_key:%s, start_key:%s, end_key:%s, instance:%s",
                _region_id, get_version(),
                _split_param.new_region_id,
                rocksdb::Slice(_split_param.split_key).ToString(true).c_str(),
                str_to_hex(get_start_key()).c_str(), 
                str_to_hex(get_end_key()).c_str(),
                _split_param.instance.c_str());
    
    //分裂的第一步修改为新建region
    ScopeProcStatus split_status(this);
    //构建init_region请求，创建一个数据为空，peer只有一个，状态为DOING, version为0的空region
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
    region_info->set_partition_num(get_partition_num());
    _new_region_info = *region_info;
    init_region_request.set_is_split(true);
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
    int64_t average_cost = _dml_time_cost.latency();
    if (average_cost == 0) {
        average_cost = 50000;
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
    split_request.set_region_version(get_version());
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

int Region::split_region_add_peer(int64_t new_region_id, std::string instance, std::string& new_region_leader, 
                                  std::vector<std::string> add_peer_instances, bool async) {
    // 串行add peer补齐副本
    TimeCost ts;
    pb::AddPeer add_peer_request;
    add_peer_request.set_region_id(new_region_id);
    add_peer_request.add_new_peers(instance);
    add_peer_request.add_old_peers(instance);
    // 设置is_split，过add_peer的检查
    add_peer_request.set_is_split(true);
    for(uint64_t i = 0; i < add_peer_instances.size(); ++i) {
        add_peer_request.add_new_peers(add_peer_instances[i]);
        pb::StoreRes add_peer_response;
        StoreReqOptions req_options;
        req_options.request_timeout = 3600000;
        bool add_peer_success = false;
        for(int j = 0; j < 5; ++j) {
            StoreInteract store_interact(new_region_leader, req_options);
            DB_WARNING("split region_id: %ld is going to add peer to instance: %s, req: %s",
                       new_region_id, add_peer_instances[i].c_str(),
                       add_peer_request.ShortDebugString().c_str());
            auto ret = store_interact.send_request("add_peer", add_peer_request, add_peer_response);
            if (ret == 0) {
                add_peer_success = true;
                _new_region_info.add_peers(add_peer_instances[i]);
            } else if (add_peer_response.errcode() == pb::CANNOT_ADD_PEER) {
                DB_WARNING("region_id %ld: can not add peer", new_region_id);
                bthread_usleep(1 * 1000 * 1000);
                continue;
            }
            else if (add_peer_response.errcode() == pb::NOT_LEADER
                        && add_peer_response.has_leader()
                        && add_peer_response.leader() != ""
                        && add_peer_response.leader() != "0.0.0.0:0") {
                DB_WARNING("region_id %ld: leader change to %s when add peer",
                           new_region_id, add_peer_response.leader().c_str());
                new_region_leader = add_peer_response.leader();
                bthread_usleep(1 * 1000 * 1000);
                continue;
            }
            break;
        }
        if (!add_peer_success) {
            DB_FATAL("split region_id: %ld add peer fail, request: %s, new_leader: %s, new_region_id: %ld, async: %d",
                     _region_id,
                     add_peer_request.ShortDebugString().c_str(),
                     instance.c_str(), new_region_id, async);
            if (async) {
                return -1;
            }
            // 失败删除所有的peer
            start_thread_to_remove_region(new_region_id, instance);
            for(auto remove_idx = 0; remove_idx < i && remove_idx < add_peer_instances.size(); ++remove_idx) {
                start_thread_to_remove_region(new_region_id, add_peer_instances[remove_idx]);
            }
            return -1;
        }
        add_peer_request.add_old_peers(add_peer_instances[i]);
        DB_WARNING("split region_id: %ld add peer success: %s",
                   new_region_id, add_peer_instances[i].c_str());
    }
    DB_WARNING("split region_id: %ld, add_peer cost: %ld", new_region_id, ts.get_time());
    return 0;
}

void Region::get_split_key_for_tail_split() {
    ScopeProcStatus split_status(this);
    TimeCost time_cost;
    if (!is_leader()) {
        start_thread_to_remove_region(_split_param.new_region_id, _split_param.instance);
        DB_FATAL("leader transfer when split, split fail, region_id: %ld", _region_id);
        return;
    }
    std::string new_region_leader = _split_param.instance;
    int ret = split_region_add_peer(_split_param.new_region_id, _split_param.instance, 
                                    new_region_leader, _split_param.add_peer_instances, false);
    if (ret < 0) {
        return;
    }
    TimeCost total_wait_time;
    TimeCost add_slow_down_ticker;
    int64_t average_cost = _dml_time_cost.latency();
    int64_t pre_digest_time = 0;
    int64_t digest_time = _real_writing_cond.count() * average_cost;
    int64_t disable_write_wait = std::max(FLAGS_disable_write_wait_timeout_us, _split_param.split_slow_down_cost);
    while (digest_time > disable_write_wait / 2) {
        if (!is_leader()) {
            DB_WARNING("leader stop, region_id: %ld, new_region_id:%ld, instance:%s",
                        _region_id, _split_param.new_region_id, _split_param.instance.c_str());
            split_remove_new_region_peers();
            return;
        }
        if (total_wait_time.get_time() > FLAGS_tail_split_wait_threshold) {
            // 10分钟强制分裂
            disable_write_wait = 3600 * 1000 * 1000LL;
            break;
        }
        if (add_slow_down_ticker.get_time() > 30 * 1000 * 1000LL) {
            adjust_split_slow_down_cost(digest_time, pre_digest_time);
            add_slow_down_ticker.reset();
        }
        DB_WARNING("tail split wait, region_id: %lu, average_cost: %lu, digest_time: %lu,"
                   "disable_write_wait: %lu, slow_down: %lu", 
                    _region_id, average_cost, digest_time, disable_write_wait, 
                    _split_param.split_slow_down_cost);
        bthread_usleep(std::min(digest_time, (int64_t)1 * 1000 * 1000));
        pre_digest_time = digest_time;
        average_cost = _dml_time_cost.latency();
        digest_time = _real_writing_cond.count() * average_cost;
        disable_write_wait = std::max(FLAGS_disable_write_wait_timeout_us, _split_param.split_slow_down_cost);
    }
    //设置禁写 并且等待正在写入任务提交
    _split_param.no_write_time_cost.reset();
    _disable_write_cond.increase();
    usleep(100);
    ret = _real_writing_cond.timed_wait(disable_write_wait);
    if (ret != 0) {
        split_remove_new_region_peers();
        DB_FATAL("_real_writing_cond wait timeout, region_id: %ld new_region_id:%ld ", _region_id, _split_param.new_region_id);
        return;
    }
    DB_WARNING("start not allow write, region_id: %ld, time_cost:%ld, _real_writing_cond: %d", 
            _region_id, time_cost.get_time(), _real_writing_cond.count());
    _split_param.write_wait_cost = time_cost.get_time();
    
    _split_param.op_start_split_for_tail.reset();
    pb::StoreReq split_request;
    //尾分裂开始, get end index, get_split_key
    split_request.set_op_type(pb::OP_START_SPLIT_FOR_TAIL);
    split_request.set_region_id(_region_id);
    split_request.set_region_version(get_version());
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
    c->add_peer_instance = _split_param.add_peer_instances;
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
        start_thread_to_remove_region(_split_param.new_region_id, _split_param.instance);
        DB_FATAL("leader transfer when split, split fail, region_id: %ld", _region_id);
        return;
    }
    SmartRegion new_region = Store::get_instance()->get_region(_split_param.new_region_id);
    if (!new_region) {
        DB_FATAL("new region is null, split fail. region_id: %ld, new_region_id:%ld, instance:%s",
                  _region_id, _split_param.new_region_id, _split_param.instance.c_str());
        return;
    }
    //write to new sst
    int64_t global_index_id = get_global_index_id();
    int64_t main_table_id = get_table_id();
    std::vector<int64_t> indices;
    TableInfo table_info = _factory->get_table_info(main_table_id);
    if (_is_global_index) {
        indices.push_back(global_index_id);
    } else {
        for (auto index_id: table_info.indices) {
            if (_factory->is_global_index(index_id)) {
                continue;
            }
            indices.push_back(index_id);
        }
    }
    //MutTableKey table_prefix;
    //table_prefix.append_i64(_region_id).append_i64(table_id);
    std::atomic<int64_t> write_sst_lines(0);
    _split_param.reduce_num_lines = 0;

    IndexInfo pk_info = _factory->get_index_info(main_table_id);

    ConcurrencyBthread copy_bth(5, &BTHREAD_ATTR_SMALL);
    for (int64_t index_id : indices) {
        auto read_and_write = [this, &pk_info, &write_sst_lines, 
                                index_id, new_region] () {
            std::unique_ptr<SstFileWriter> writer(new SstFileWriter(
                        _rocksdb->get_options(_rocksdb->get_data_handle())));
            MutTableKey table_prefix;
            table_prefix.append_i64(_region_id).append_i64(index_id);
            rocksdb::WriteOptions write_options;
            TimeCost cost;
            int64_t num_write_lines = 0;
            int64_t skip_write_lines = 0;
            // reverse index lines
            int64_t level1_lines = 0;
            int64_t level2_lines = 0;
            int64_t level3_lines = 0;
            rocksdb::ReadOptions read_options;
            read_options.prefix_same_as_start = true;
            read_options.total_order_seek = false;
            read_options.fill_cache = false;
            read_options.snapshot = _split_param.snapshot;
           
            IndexInfo index_info = _factory->get_index_info(index_id);
            std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _data_cf));
            if (index_info.type == pb::I_PRIMARY || _is_global_index) {
                table_prefix.append_index(_split_param.split_key);
            }
            std::string end_key = get_end_key();
            int64_t count = 0;
            std::ostringstream os;
            // 使用FLAGS_db_path，保证ingest能move成功
            os << FLAGS_db_path << "/" << "region_split_ingest_sst." << _region_id << "." 
                << _split_param.new_region_id << "." << index_id;
            std::string path = os.str();

            ScopeGuard auto_fail_guard([path, this]() {
                _split_param.err_code = -1;
                butil::DeleteFile(butil::FilePath(path), false);
            });

            auto s = writer->open(path);
            if (!s.ok()) {
                DB_FATAL("open sst file path: %s failed, err: %s, region_id: %ld", path.c_str(), s.ToString().c_str(), _region_id);
                return;
            }
            for (iter->Seek(table_prefix.data()); iter->Valid(); iter->Next()) {
                ++count;
                if (count % 1000 == 0) {
                    // 大region split中重置time_cost，防止version=0超时删除
                    new_region->reset_timecost();
                }
                if (count % 1000 == 0 && (!is_leader() || _shutdown)) {
                    DB_WARNING("index %ld, old region_id: %ld write to new region_id: %ld failed, not leader",
                                index_id, _region_id, _split_param.new_region_id);
                    return;
                }
                //int ret1 = 0; 
                rocksdb::Slice key_slice(iter->key());
                key_slice.remove_prefix(2 * sizeof(int64_t));
                if (index_info.type == pb::I_PRIMARY || _is_global_index) {
                    // check end_key
                    // tail split need not send rocksdb
                    if (key_slice.compare(end_key) >= 0) {
                        break;
                    }
                } else if (index_info.type == pb::I_UNIQ || index_info.type == pb::I_KEY) {
                    rocksdb::Slice tmp_value = iter->value();
                    if (_use_ttl) {
                        ttl_decode(tmp_value, &index_info, _online_ttl_base_expire_time_us);
                    }
                    if (!Transaction::fits_region_range(key_slice, tmp_value,
                            &_split_param.split_key, &end_key, 
                            pk_info, index_info)) {
                        skip_write_lines++;
                        continue;
                    }
                } else if (index_info.type == pb::I_FULLTEXT) {
                    uint8_t level = key_slice.data_[0];
                    if (level == 1) {
                        ++level1_lines;
                    } else if (level == 2) {
                        ++level2_lines; 
                    } else if (level == 3) {
                        ++level3_lines; 
                    }
                }
                MutTableKey key(iter->key());
                key.replace_i64(_split_param.new_region_id, 0);
                s = writer->put(key.data(), iter->value());
                if (!s.ok()) {
                    DB_FATAL("index %ld, old region_id: %ld write to new region_id: %ld failed, status: %s", 
                    index_id, _region_id, _split_param.new_region_id, s.ToString().c_str());
                    return;
                }
                num_write_lines++;
            }
            s = writer->finish();
            uint64_t file_size = writer->file_size();
            if (num_write_lines > 0) {
                if (!s.ok()) {
                    DB_FATAL("finish sst file path: %s failed, err: %s, region_id: %ld, index %ld", 
                            path.c_str(), s.ToString().c_str(), _region_id, index_id);
                    return;
                }
                int ret_data = RegionControl::ingest_data_sst(path, _region_id, true);
                if (ret_data < 0) {
                    DB_FATAL("ingest sst fail, path:%s, region_id: %ld", path.c_str(), _region_id);
                    return;
                }
            } else {
                butil::DeleteFile(butil::FilePath(path), false);
            }
            write_sst_lines += num_write_lines;
            if (index_info.type == pb::I_PRIMARY || _is_global_index) {
                _split_param.reduce_num_lines = num_write_lines;
            }
            auto_fail_guard.release();
            DB_WARNING("scan index:%ld, cost=%ld, file_size=%lu, lines=%ld, skip:%ld, region_id: %ld "
                    "level lines=[%ld,%ld,%ld]", 
                    index_id, cost.get_time(), file_size, num_write_lines, skip_write_lines, _region_id,
                    level1_lines, level2_lines, level3_lines);

        };
        copy_bth.run(read_and_write); 
    }
    if (table_info.engine == pb::ROCKSDB_CSTORE && !_is_global_index) {
        // write all non-pk column values to cstore
        std::set<int32_t> pri_field_ids;
        std::string end_key = get_end_key();
        for (auto& field_info : pk_info.fields) {
            pri_field_ids.insert(field_info.id);
        }
        for (auto& field_info : table_info.fields) {
            int32_t field_id = field_info.id;
            // skip pk fields
            if (pri_field_ids.count(field_id) != 0) {
                continue;
            }
            auto read_and_write_column = [this, &pk_info, &write_sst_lines, end_key,
                 field_id, new_region] () {
                std::unique_ptr<SstFileWriter> writer(new SstFileWriter(
                            _rocksdb->get_options(_rocksdb->get_data_handle())));
                MutTableKey table_prefix;
                table_prefix.append_i64(_region_id);
                table_prefix.append_i32(get_global_index_id()).append_i32(field_id);
                rocksdb::WriteOptions write_options;
                TimeCost cost;
                int64_t num_write_lines = 0;
                int64_t skip_write_lines = 0;
                rocksdb::ReadOptions read_options;
                read_options.prefix_same_as_start = true;
                read_options.total_order_seek = false;
                read_options.fill_cache = false;
                read_options.snapshot = _split_param.snapshot;

                std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _data_cf));
                table_prefix.append_index(_split_param.split_key);
                int64_t count = 0;
                std::ostringstream os;
                // 使用FLAGS_db_path，保证ingest能move成功
                os << FLAGS_db_path << "/" << "region_split_ingest_sst." << _region_id << "."
                    << _split_param.new_region_id << "." << field_id;
                std::string path = os.str();

                ScopeGuard auto_fail_guard([path, this]() {
                    _split_param.err_code = -1;
                    butil::DeleteFile(butil::FilePath(path), false);
                });

                auto s = writer->open(path);
                if (!s.ok()) {
                    DB_FATAL("open sst file path: %s failed, err: %s, region_id: %ld, field_id: %d", 
                            path.c_str(), s.ToString().c_str(), _region_id, field_id);
                    _split_param.err_code = -1;
                    return;
                }

                for (iter->Seek(table_prefix.data()); iter->Valid(); iter->Next()) {
                    ++count;
                    if (count % 1000 == 0) {
                        // 大region split中重置time_cost，防止version=0超时删除
                        new_region->reset_timecost();
                    }
                    if (count % 1000 == 0 && (!is_leader() || _shutdown)) {
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
                    if (key_slice.compare(end_key) >= 0) {
                        break;
                    }
                    MutTableKey key(iter->key());
                    key.replace_i64(_split_param.new_region_id, 0);
                    auto s = writer->put(key.data(), iter->value());
                    if (!s.ok()) {
                        DB_FATAL("field %d, old region_id: %ld write to new region_id: %ld failed, status: %s",
                                field_id, _region_id, _split_param.new_region_id, s.ToString().c_str());
                        _split_param.err_code = -1;
                        return;
                    }
                    num_write_lines++;
                }
                s = writer->finish();
                uint64_t file_size = writer->file_size();
                if (num_write_lines > 0) {
                    if (!s.ok()) {
                        DB_FATAL("finish sst file path: %s failed, err: %s, region_id: %ld, field_id %d",
                                path.c_str(), s.ToString().c_str(), _region_id, field_id);
                        _split_param.err_code = -1;
                        return;
                    }
                    int ret_data = RegionControl::ingest_data_sst(path, _region_id, true);
                    if (ret_data < 0) {
                        DB_FATAL("ingest sst fail, path:%s, region_id: %ld", path.c_str(), _region_id);
                        _split_param.err_code = -1;
                        return;
                    }
                } else {
                    butil::DeleteFile(butil::FilePath(path), false);
                }
                write_sst_lines += num_write_lines;
                auto_fail_guard.release();
                DB_WARNING("scan field:%d, cost=%ld, file_size=%lu, lines=%ld, skip:%ld, region_id: %ld",
                            field_id, cost.get_time(), file_size, num_write_lines, skip_write_lines, _region_id);

            };
            copy_bth.run(read_and_write_column);
        }
    }
    copy_bth.join();
    if (_split_param.err_code != 0) {
        start_thread_to_remove_region(_split_param.new_region_id, _split_param.instance);
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
    new_region->set_num_table_lines(_split_param.reduce_num_lines);

    //snapshot 之前发送5个NO_OP请求
    int ret = RpcSender::send_no_op_request(_split_param.instance, _split_param.new_region_id, 0);
    if (ret < 0) {
        DB_FATAL("new region request fail, send no_op reqeust,"
                 " region_id: %ld, new_reigon_id:%ld, instance:%s",
                _region_id, _split_param.new_region_id, 
                _split_param.instance.c_str());
        start_thread_to_remove_region(_split_param.new_region_id, _split_param.instance);
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

// replay txn commands on local or remote peer
// start_key is used when sending request to tail splitting new region,
// whose start_key is not set yet.
int Region::replay_applied_txn_for_recovery(
            int64_t region_id,
            const std::string& instance,
            std::string start_key,
            const std::unordered_map<uint64_t, pb::TransactionInfo>& applied_txn) {
    std::vector<pb::BatchStoreReq> requests;
    std::vector<butil::IOBuf> attachment_datas;
    requests.reserve(10);
    attachment_datas.reserve(10);
    pb::BatchStoreReq batch_request;
    batch_request.set_region_id(region_id);
    batch_request.set_resend_start_pos(0);
    butil::IOBuf attachment_data;
    for (auto& pair : applied_txn) {
        uint64_t txn_id = pair.first;
        const pb::TransactionInfo& txn_info = pair.second;
        auto plan_size = txn_info.cache_plans_size();
        if (plan_size == 0) {
            DB_FATAL("TransactionError: invalid command type, region_id: %ld, txn_id: %lu", _region_id, txn_id);
            return -1;
        }
        auto& begin_plan = txn_info.cache_plans(0);
        auto& prepare_plan = txn_info.cache_plans(plan_size - 1);
        if (!txn_info.has_primary_region_id()) {
            DB_FATAL("TransactionError: invalid txn state, region_id: %ld, txn_id: %lu, op_type: %d", 
                    _region_id, txn_id, prepare_plan.op_type());
            return -1;
        }
        for (auto& cache_plan : txn_info.cache_plans()) {
            // construct prepare request to send to new_plan
            pb::StoreReq request;
            pb::StoreRes response;
            if (cache_plan.op_type() == pb::OP_BEGIN) {
                continue;
            }
            request.set_op_type(cache_plan.op_type());
            for (auto& tuple : cache_plan.tuples()) {
                request.add_tuples()->CopyFrom(tuple);
            }
            request.set_region_id(region_id);
            request.set_region_version(0);
            if (cache_plan.op_type() != pb::OP_KV_BATCH) {
                request.mutable_plan()->CopyFrom(cache_plan.plan());
            } else {
                for (auto& kv_op : cache_plan.kv_ops()) {
                    request.add_kv_ops()->CopyFrom(kv_op);
                }
            }
            if (start_key.size() > 0) {
                // send new start_key to new_region, only once
                // tail split need send start_key at this place
                request.set_start_key(start_key);
                start_key.clear();
            }
            pb::TransactionInfo* txn = request.add_txn_infos();
            txn->set_txn_id(txn_id);
            txn->set_seq_id(cache_plan.seq_id());
            txn->set_optimize_1pc(false);
            txn->set_start_seq_id(1);
            for (auto seq_id : txn_info.need_rollback_seq()) {
                txn->add_need_rollback_seq(seq_id);
            }
            txn->set_primary_region_id(txn_info.primary_region_id());
            pb::CachePlan* pb_cache_plan = txn->add_cache_plans();
            pb_cache_plan->CopyFrom(begin_plan);

            butil::IOBuf data;
            butil::IOBufAsZeroCopyOutputStream wrapper(&data);
            if (!request.SerializeToZeroCopyStream(&wrapper)) {
                DB_FATAL("Fail to serialize request");
                return -1;
            }
            batch_request.add_request_lens(data.size());
            attachment_data.append(data);
            if (batch_request.request_lens_size() == FLAGS_split_send_log_batch_size) {
                requests.emplace_back(batch_request);
                attachment_datas.emplace_back(attachment_data);
                batch_request.clear_request_lens();
                attachment_data.clear();
            }
            DB_WARNING("replaying txn, request op_type:%s region_id: %ld, target_region_id: %ld,"
            "txn_id: %lu, primary_region_id:%ld", pb::OpType_Name(request.op_type()).c_str(),
            _region_id, region_id, txn_id, txn_info.primary_region_id());
        }
    }
    if (batch_request.request_lens_size() > 0) {
        requests.emplace_back(batch_request);
        attachment_datas.emplace_back(attachment_data);
    }
    int64_t idx = 0;
    for (auto& request : requests) {
        int retry_time = 0;
        bool send_success = false;
        std::string address = instance;
        do {
            pb::BatchStoreRes response;
            int ret = RpcSender::send_async_apply_log(request,
                                                        response,
                                                        address,
                                                        &attachment_datas[idx]);
            if (ret < 0) {
                ++retry_time;
                if (response.errcode() == pb::NOT_LEADER) {
                    if (response.has_leader() && response.leader() != "" && response.leader() != "0.0.0.0:0") {
                        address = response.leader();
                        request.set_resend_start_pos(response.success_cnt());
                    }
                    DB_WARNING("leader transferd when send log entry, region_id: %ld, new_region_id:%ld",
                                _region_id, region_id);
                } else {
                    DB_FATAL("new region request fail, send log entry fail before not allow write,"
                        " region_id: %ld, new_region_id:%ld, instance:%s",
                        _region_id, region_id, address.c_str());
                    return -1;
                }
            } else {
                send_success = true;
                break;
            }
        } while (retry_time < 3);
        if (!send_success) {
            DB_FATAL("new region request fail, send log entry fail before not allow write,"
                        " region_id: %ld, new_region_id:%ld, instance:%s",
                    _region_id, region_id, instance.c_str());
            return -1;
        }
        ++idx;
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
        start_thread_to_remove_region(_split_param.new_region_id, _split_param.instance);
        DB_FATAL("leader transfer when split, split fail, region_id: %ld, new_region_id: %ld",
                  _region_id, _split_param.new_region_id);
        return;
    }
    std::string new_region_leader = _split_param.instance;
    if (FLAGS_split_add_peer_asyc) {
        Bthread bth;
        bth.run([this, new_region_leader](){
            _multi_thread_cond.increase();
            ON_SCOPE_EXIT([this]() {
                _multi_thread_cond.decrease_signal();
            });
            std::string region_leader = new_region_leader;
            split_region_add_peer(_split_param.new_region_id, _split_param.instance, 
                                    region_leader, _split_param.add_peer_instances, true);
        });
    } else {
        int ret = split_region_add_peer(_split_param.new_region_id, _split_param.instance, 
                                    new_region_leader, _split_param.add_peer_instances, false);
        if (ret < 0) {
            return;
        }
    }
    // replay txn commands on new region by network write
    // 等价于add_peer复制已经applied了的txn_log
    if (0 != replay_applied_txn_for_recovery(_split_param.new_region_id,
                                            _split_param.instance,
                                            _split_param.split_key,
                                            _split_param.applied_txn)) {
        DB_WARNING("replay_applied_txn_for_recovery failed: region_id: %ld, new_region_id: %ld",
                _region_id, _split_param.new_region_id);
        split_remove_new_region_peers();
        return;
    }
    TimeCost send_first_log_entry_time;
    TimeCost every_minute;
    //禁写之前先读取一段log_entry
    int64_t start_index = _split_param.split_start_index;
    std::vector<pb::BatchStoreReq> requests;
    std::vector<butil::IOBuf> attachment_datas;
    requests.reserve(10);
    attachment_datas.reserve(10);
    int64_t average_cost = _dml_time_cost.latency();
    int while_count = 0;
    int64_t new_region_braft_index = 0;
    int64_t new_region_apply_index = 0;
    int64_t new_region_dml_latency = 0;
    // 统计每一个storeReq从rocksdb读出来，发async rpc到收到response的平均时间，用来算禁写阈值
    int64_t send_time_per_store_req = 0;
    int64_t left_log_entry = 0;
    int64_t left_log_entry_threshold = 1;
    int64_t adjust_value = 1;
    int64_t queued_logs_pre_min = 0; // 上一分钟，新region queued日志数
    int64_t queued_logs_now = 0;     // 当前，新region queued日志数
    int64_t real_writing_cnt_threshold = 0;
    do {
        TimeCost time_cost_one_pass;
        int64_t end_index = 0;
        requests.clear();
        attachment_datas.clear();
        int ret = get_log_entry_for_split(start_index,
                                          _split_param.split_term,
                                          requests,
                                          attachment_datas,
                                          end_index);
        if (ret < 0) {
            DB_FATAL("get log split fail before not allow when region split, "
                      "region_id: %ld, new_region_id:%ld",
                       _region_id, _split_param.new_region_id);
            split_remove_new_region_peers();
            return;
        }
        if (ret == 0) {
            ++while_count;
        }
        int64_t idx = 0;
        if (new_region_braft_index != 0 && requests.empty()) {
            // 没有日志，发一个空的req，拿到分裂region leader当前的applied_index和braft_index
            pb::BatchStoreReq req;
            req.set_region_id(_split_param.new_region_id);
            requests.emplace_back(req);
            bthread_usleep(100 * 1000);
        }
        for (auto& request : requests) {
            if (idx % 10 == 0 && !is_leader()) {
                DB_WARNING("leader stop when send log entry,"
                            " region_id: %ld, new_region_id:%ld, instance:%s",
                            _region_id, _split_param.new_region_id,
                            _split_param.instance.c_str());
                split_remove_new_region_peers();
                return;
            }
            int retry_time = 0;
            bool send_success = false;
            do {
                pb::BatchStoreRes response;
                int ret = RpcSender::send_async_apply_log(request,
                                                          response,
                                                          new_region_leader,
                                                          &attachment_datas[idx]);
                if (ret < 0) {
                    ++retry_time;
                    if (response.errcode() == pb::NOT_LEADER) {
                        if (response.has_leader() && response.leader() != "" && response.leader() != "0.0.0.0:0") {
                            new_region_leader = response.leader();
                            request.set_resend_start_pos(response.success_cnt());
                        }
                        DB_WARNING("leader transferd when send log entry, region_id: %ld, new_region_id:%ld",
                                   _region_id, _split_param.new_region_id);
                        bthread_usleep(1 * 1000 * 1000);
                    } else {
                        DB_FATAL("new region request fail, send log entry fail before not allow write,"
                            " region_id: %ld, new_region_id:%ld, instance:%s",
                            _region_id, _split_param.new_region_id, 
                            _split_param.instance.c_str());
                        split_remove_new_region_peers();
                        return;
                    }
                } else {
                    send_success = true;
                    new_region_apply_index = response.applied_index();
                    new_region_braft_index = response.braft_applied_index();
                    new_region_dml_latency = response.dml_latency();
                    if (new_region_dml_latency == 0) {
                        new_region_dml_latency = 50000;
                    }
                    break;
                }
            } while (retry_time < 10);
            if (!send_success) {
                DB_FATAL("new region request fail, send log entry fail before not allow write,"
                         " region_id: %ld, new_region_id:%ld, instance:%s",
                         _region_id, _split_param.new_region_id,
                         _split_param.instance.c_str());
                split_remove_new_region_peers();
                return;
            }
            ++idx;
        }
        int64_t tm = time_cost_one_pass.get_time();
        int64_t qps_send_log_entry = 0;
        int64_t log_entry_send_num = end_index + 1 - start_index;
        if (log_entry_send_num > 0 && tm > 0) {
            send_time_per_store_req = tm / log_entry_send_num;
            qps_send_log_entry = 1000000L * log_entry_send_num / tm;
        }
        int64_t qps = _dml_time_cost.qps();
        /*
        if (log_entry_send_num > FLAGS_split_adjust_slow_down_cost && qps_send_log_entry < 2 * qps && qps_send_log_entry != 0) {
            _split_param.split_slow_down_cost = 
                _split_param.split_slow_down_cost * 2 * qps / qps_send_log_entry;
            _split_param.split_slow_down_cost = std::min(
                    _split_param.split_slow_down_cost, (int64_t)5000000);
        }
         */
        average_cost = _dml_time_cost.latency();
        if (every_minute.get_time() > 60 * 1000 * 1000) {
            queued_logs_now = new_region_braft_index - new_region_apply_index;
            adjust_split_slow_down_cost(queued_logs_now, queued_logs_pre_min);
            every_minute.reset();
            queued_logs_pre_min = queued_logs_now;
        }
        start_index = end_index + 1;
        DB_WARNING("qps:%ld for send log entry, qps:%ld for region_id: %ld, split_slow_down:%ld, "
                    "average_cost: %lu, old_region_left_log: %ld, "
                    "new_region_braft_index: %ld, new_region_queued_log: %ld, "
                    "send_time_per_store_req: %ld, new_region_dml_latency: %ld, tm: %ld, req: %ld",
                   qps_send_log_entry, qps, _region_id, _split_param.split_slow_down_cost, average_cost,
                   _applied_index - start_index,
                   new_region_braft_index, new_region_braft_index - new_region_apply_index,
                   send_time_per_store_req, new_region_dml_latency, tm, log_entry_send_num);
        // 计算新region的left log阈值
        // 异步队列 + pending的日志数
        left_log_entry = (new_region_braft_index - new_region_apply_index) + _real_writing_cond.count();
        if (_applied_index - start_index > 0) {
            // 还未发送的日志数
            left_log_entry += _applied_index - start_index;
        }
        if (send_time_per_store_req + new_region_dml_latency != 0) {
            left_log_entry_threshold =
                std::max(FLAGS_disable_write_wait_timeout_us, _split_param.split_slow_down_cost) / 2 / (send_time_per_store_req + new_region_dml_latency);
        }
        if (left_log_entry_threshold == 0) {
            left_log_entry_threshold = 1;
        }
        // 计算老region的real writing阈值
        if (average_cost > 0) {
            real_writing_cnt_threshold =
                std::max(FLAGS_disable_write_wait_timeout_us, _split_param.split_slow_down_cost) / 2 / average_cost;
        }
        adjust_value = send_first_log_entry_time.get_time() / (600 * 1000 * 1000) + 1;
    } while ((left_log_entry > left_log_entry_threshold * adjust_value
                || left_log_entry > FLAGS_no_write_log_entry_threshold * adjust_value 
                || _real_writing_cond.count() > real_writing_cnt_threshold * adjust_value)
                && send_first_log_entry_time.get_time() < FLAGS_split_send_first_log_entry_threshold);
    DB_WARNING("send log entry before not allow success when split, "
                "region_id: %ld, new_region_id:%ld, instance:%s, time_cost:%ld, "
                "start_index:%ld, end_index:%ld, applied_index:%ld, while_count:%d, average_cost: %ld "
                "new_region_dml_latency: %ld, new_region_log_index: %ld, new_region_applied_index: %ld",
                _region_id, _split_param.new_region_id,
                _split_param.instance.c_str(), send_first_log_entry_time.get_time(),
                _split_param.split_start_index, start_index, _applied_index, while_count, average_cost,
                new_region_dml_latency, new_region_braft_index, new_region_apply_index);
    _split_param.no_write_time_cost.reset();
    //设置禁写 并且等待正在写入任务提交
    TimeCost write_wait_cost;
    _disable_write_cond.increase();
    DB_WARNING("start not allow write, region_id: %ld, new_region_id: %ld, _real_writing_cond: %d",
               _region_id, _split_param.new_region_id, _real_writing_cond.count());
    _split_param.send_first_log_entry_cost = send_first_log_entry_time.get_time();
    int64_t disable_write_wait = get_split_wait_time();
    if (_split_param.send_first_log_entry_cost >= FLAGS_split_send_first_log_entry_threshold) {
        // 超一小时，强制分裂成功
        disable_write_wait = 3600 * 1000 * 1000LL;
    }
    usleep(100);
    int ret = _real_writing_cond.timed_wait(disable_write_wait);
    if (ret != 0) {
        split_remove_new_region_peers();
        DB_FATAL("_real_writing_cond wait timeout, region_id: %ld", _region_id);
        return;
    }
    DB_WARNING("wait real_writing finish, region_id: %ld, new_region_id: %ld, time_cost:%ld, _real_writing_cond: %d",
               _region_id, _split_param.new_region_id, write_wait_cost.get_time(), _real_writing_cond.count());
    _split_param.write_wait_cost = write_wait_cost.get_time();
    //读取raft_log
    TimeCost send_second_log_entry_cost;
    bool seek_end = false;
    int seek_retry = 0;
    do {
        TimeCost single_cost;
        requests.clear();
        attachment_datas.clear();
        ret = get_log_entry_for_split(start_index, 
                _split_param.split_term, 
                requests,
                attachment_datas,
                _split_param.split_end_index);
        if (ret < 0) {
            DB_FATAL("get log split fail when region split, region_id: %ld, new_region_id: %ld",
                    _region_id, _split_param.new_region_id);
            split_remove_new_region_peers();
            return;
        }
        // 偶发差一两天读不到日志导致后面校验失败，强制读到当前的apply_index
        if (ret == 0) {
            if (_split_param.split_end_index == _applied_index) {
                seek_end = true;
            } else {
                if (++seek_retry > 50) {
                    DB_FATAL("region: %ld split fail, seek retry failed after 20 times, split_end_index: %lu, _applied_index: %lu", 
                            _region_id, _split_param.split_end_index, _applied_index);
                    split_remove_new_region_peers();
                    return;
                }
            }
        }
        int64_t idx = 0;
        //发送请求到新region
        for (auto& request : requests) {
            if (idx % 10 == 0 && !is_leader()) {
                DB_WARNING("leader stop when send log entry,"
                        " region_id: %ld, new_region_id:%ld, instance:%s",
                        _region_id, _split_param.new_region_id,
                        _split_param.instance.c_str());
                split_remove_new_region_peers();
                return;
            }
            int retry_time = 0;
            bool send_success = false;
            do {
                pb::BatchStoreRes response;
                int ret = RpcSender::send_async_apply_log(request,
                                                          response,
                                                          new_region_leader,
                                                          &attachment_datas[idx]);
                if (ret < 0) {
                    ++retry_time;
                    if (response.errcode() == pb::NOT_LEADER) {
                        if (response.has_leader() && response.leader() != "" && response.leader() != "0.0.0.0:0") {
                            new_region_leader = response.leader();
                            request.set_resend_start_pos(response.success_cnt());
                        }
                        DB_WARNING("leader transferd when send log entry, region_id: %ld, new_region_id:%ld",
                                   _region_id, _split_param.new_region_id);
                    } else {
                        DB_FATAL("new region request fail, send log entry fail before not allow write,"
                            " region_id: %ld, new_region_id:%ld, instance:%s",
                            _region_id, _split_param.new_region_id, 
                            _split_param.instance.c_str());
                        split_remove_new_region_peers();
                        return;
                    }
                } else {
                    send_success = true;
                    break;
                }
            } while (retry_time < 3);
            if (!send_success) {
                DB_FATAL("new region request fail, send log entry fail before not allow write,"
                         " region_id: %ld, new_region_id:%ld, instance:%s",
                         _region_id, _split_param.new_region_id,
                         _split_param.instance.c_str());
                split_remove_new_region_peers();
                return;
            }
            ++idx;
        }
        start_index = _split_param.split_end_index + 1;
        DB_WARNING("region split single when send second log entry to new region,"
                "region_id: %ld, new_region_id:%ld, split_end_index:%ld, instance:%s, time_cost:%ld",
                _region_id, 
                _split_param.new_region_id, 
                _split_param.split_end_index,
                _split_param.instance.c_str(),
                single_cost.get_time());
    } while (!seek_end);
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
        split_remove_new_region_peers();
        DB_FATAL("leader transfer when split, split fail, region_id: %ld", _region_id);
        return;
    }

    if (_split_param.tail_split) {
        // replay txn commands on new region
        if (0 != replay_applied_txn_for_recovery(
                _split_param.new_region_id,
                _split_param.instance,
                _split_param.split_key,
                _split_param.applied_txn)) {
            DB_FATAL("replay_applied_txn_for_recovery failed: region_id: %ld, new_region_id: %ld",
                _region_id, _split_param.new_region_id);
            split_remove_new_region_peers();
            return;
        }
    }

    int retry_times = 0;
    TimeCost time_cost;
    pb::StoreRes response;
    //给新region发送更新完成请求，version 0 -> 1, 状态由Splitting->Normal, start->end
    do {
        brpc::Channel channel;
        brpc::ChannelOptions channel_opt;
        channel_opt.timeout_ms = FLAGS_store_request_timeout * 10 * 2;
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
             DB_WARNING("send complete fail when serilize to iobuf for split fail,"
                        " region_id: %ld, request:%s",
                        _region_id, pb2json(request).c_str());
             ++retry_times;
             continue;
        }
        response.Clear();
        pb::StoreService_Stub(&channel).query(&cntl, &request, &response, NULL);
        if (cntl.Failed()) {
            DB_WARNING("region split fail when add version for split, region_id: %ld, err:%s", 
                        _region_id, cntl.ErrorText().c_str());
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
        split_remove_new_region_peers();
        return;
    }

    if (!is_leader()) {
        DB_FATAL("leader transfer when split, split fail, region_id: %ld", _region_id);
        split_remove_new_region_peers();
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
    add_version_request.set_region_version(get_version() + 1);
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
    static bvar::LatencyRecorder split_cost("split_cost");
    split_cost << _split_param.total_cost.get_time();
    _split_param.op_add_version_cost = _split_param.op_add_version.get_time();
    DB_WARNING("split complete,  table_id: %ld, region_id: %ld new_region_id: %ld, total_cost:%ld, no_write_time_cost:%ld,"
               " new_region_cost:%ld, op_start_split_cost:%ld, op_start_split_for_tail_cost:%ld, write_sst_cost:%ld,"
               " send_first_log_entry_cost:%ld, write_wait_cost:%ld, send_second_log_entry_cost:%ld,"
               " send_complete_to_new_region_cost:%ld, op_add_version_cost:%ld, is_tail_split: %d",
                _region_info.table_id(), _region_id, _split_param.new_region_id,
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
                _split_param.op_add_version_cost,
                _split_param.tail_split);
    {
        ScopeProcStatus split_status(this);
    }
    
    //分离完成后立即发送一次心跳
    baikaldb::Store::get_instance()->send_heart_beat();

    //主动transfer_leader
    auto transfer_leader_func = [this] {
        this->transfer_leader_after_split();
    };
    Bthread bth;
    bth.run(transfer_leader_func);
}

void Region::print_log_entry(const int64_t start_index, const int64_t end_index) {
    MutTableKey log_data_key;
    log_data_key.append_i64(_region_id).append_u8(MyRaftLogStorage::LOG_DATA_IDENTIFY).append_i64(start_index);
    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    read_options.total_order_seek = false;
    read_options.fill_cache = false;
    std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, RocksWrapper::RAFT_LOG_CF));
    iter->Seek(log_data_key.data());
    for (int i = 0; iter->Valid() && i < 100; iter->Next(), i++) {
        TableKey key(iter->key());
        int64_t log_index = key.extract_i64(sizeof(int64_t) + 1);
        if (log_index > end_index) {
            break;
        }
        rocksdb::Slice value_slice(iter->value());
        LogHead head(iter->value());
        value_slice.remove_prefix(MyRaftLogStorage::LOG_HEAD_SIZE);
        if ((braft::EntryType)head.type != braft::ENTRY_TYPE_DATA) {
            DB_FATAL("log entry is not data, log_index:%ld, region_id: %ld", log_index, _region_id);
            continue;
        }
        pb::StoreReq store_req;
        if (!store_req.ParseFromArray(value_slice.data(), value_slice.size())) {
            DB_FATAL("Fail to parse request fail, split fail, region_id: %ld", _region_id);
            continue;
        }
        DB_WARNING("region: %ld, log_index: %ld, term: %ld, req: %s",
                _region_id, log_index, head.term, store_req.ShortDebugString().c_str());
    }
}

int Region::get_log_entry_for_split(const int64_t split_start_index, 
                                    const int64_t expected_term,
                                    std::vector<pb::BatchStoreReq>& requests,
                                    std::vector<butil::IOBuf>& attachment_datas,
                                    int64_t& split_end_index) {
    TimeCost cost;
    int64_t start_index = split_start_index;
    int64_t ori_apply_index = _applied_index;
    pb::BatchStoreReq batch_request;
    batch_request.set_region_id(_split_param.new_region_id);
    batch_request.set_resend_start_pos(0);
    butil::IOBuf attachment_data;
    MutTableKey log_data_key;
    log_data_key.append_i64(_region_id).append_u8(MyRaftLogStorage::LOG_DATA_IDENTIFY).append_i64(split_start_index);
    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    read_options.total_order_seek = false;
    read_options.fill_cache = false;
    std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, RocksWrapper::RAFT_LOG_CF));
    iter->Seek(log_data_key.data());
    // 小batch发送
    for (int i = 0; iter->Valid() && i < 10000; iter->Next(), i++) {
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
            ++start_index;
            DB_WARNING("log entry is not data, log_index:%ld, region_id: %ld, type: %d", log_index, _region_id, (braft::EntryType)head.type);
            continue;
        }
        // TODO 后续上线可以不序列化反序列化
        pb::StoreReq store_req;
        if (!store_req.ParseFromArray(value_slice.data(), value_slice.size())) {
            DB_FATAL("Fail to parse request fail, split fail, region_id: %ld", _region_id);
            return -1;
        }
        if (store_req.op_type() != pb::OP_INSERT
            && store_req.op_type() != pb::OP_DELETE
            && store_req.op_type() != pb::OP_UPDATE
            && store_req.op_type() != pb::OP_PREPARE
            && store_req.op_type() != pb::OP_ROLLBACK
            && store_req.op_type() != pb::OP_COMMIT
            && store_req.op_type() != pb::OP_NONE
            && store_req.op_type() != pb::OP_KV_BATCH
            && store_req.op_type() != pb::OP_SELECT_FOR_UPDATE
            && store_req.op_type() != pb::OP_UPDATE_PRIMARY_TIMESTAMP) {
            DB_WARNING("unexpected store_req:%s, region_id: %ld",
                       pb2json(store_req).c_str(), _region_id);
            return -1;
        }
        store_req.set_region_id(_split_param.new_region_id);
        store_req.set_region_version(0);

        butil::IOBuf data;
        butil::IOBufAsZeroCopyOutputStream wrapper(&data);
        if (!store_req.SerializeToZeroCopyStream(&wrapper)) {
            return -1;
        }

        batch_request.add_request_lens(data.size());
        attachment_data.append(data);
        if (batch_request.request_lens_size() == FLAGS_split_send_log_batch_size) {
            requests.emplace_back(batch_request);
            attachment_datas.emplace_back(attachment_data);
            batch_request.clear_request_lens();
            attachment_data.clear();
        }
        ++start_index;
    }
    if (batch_request.request_lens_size() > 0) {
        requests.emplace_back(batch_request);
        attachment_datas.emplace_back(attachment_data);
    }
    split_end_index = start_index - 1;
    DB_WARNING("get_log_entry_for_split_time:%ld, region_id: %ld, "
            "split_start_index:%ld, split_end_index:%ld, ori_apply_index: %ld,"
            " applied_index:%ld, _real_writing_cond: %d",
            cost.get_time(), _region_id, split_start_index, split_end_index,
            ori_apply_index, _applied_index, _real_writing_cond.count());
    // ture还有数据，false没数据了
    return iter->Valid() ? 1 : 0;
}

void Region::adjust_num_table_lines() {
    TimeCost cost;
    MutTableKey start;
    MutTableKey end;
    start.append_i64(_region_id);
    start.append_i64(get_table_id());
    end.append_i64(_region_id);
    end.append_i64(get_table_id());
    end.append_u64(UINT64_MAX);
    end.append_u64(UINT64_MAX);
    end.append_u64(UINT64_MAX);

    // approximates both number of records and size of memtables.
    uint64_t memtable_count;
    uint64_t memtable_size;
    auto db = _rocksdb->get_db();
    if (db == nullptr) {
        return;
    }
    db->GetApproximateMemTableStats(_data_cf,
                                    rocksdb::Range(start.data(), end.data()),
                                    &memtable_count,
                                    &memtable_size);

    // get TableProperties of sstables, which contains num_entries
    std::vector<rocksdb::Range> ranges;
    ranges.emplace_back(get_rocksdb_range());
    rocksdb::TablePropertiesCollection props;
    db->GetPropertiesOfTablesInRange(_data_cf, &ranges[0], ranges.size(), &props);
    uint64_t sstable_count = 0;
    for (const auto& item : props) {
        sstable_count += item.second->num_entries;
    }

    int64_t new_table_lines = memtable_count + sstable_count;
    DB_WARNING("approximate_num_table_lines, time_cost:%ld, region_id: %ld, "
               "num_table_lines:%ld, new num_table_lines: %ld, in_mem:%lu, in_sst:%lu",
               cost.get_time(),
               _region_id,
               _num_table_lines.load(),
               new_table_lines,
               memtable_count,
               sstable_count);
    _num_table_lines = new_table_lines;
    return;
}

int Region::get_split_key(std::string& split_key, int64_t& split_key_term) {
    int64_t tableid = get_global_index_id();
    if (tableid < 0) {
        DB_WARNING("invalid tableid: %ld, region_id: %ld", 
                    tableid, _region_id);
        return -1;
    }
    rocksdb::ReadOptions read_options;
    read_options.total_order_seek = false;
    read_options.prefix_same_as_start = true;
    read_options.fill_cache = false;
    std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _data_cf));
    MutTableKey key;

    // 尾部插入优化, 非尾部插入可能会导致分裂两次
    //if (!_region_info.has_end_key() || _region_info.end_key() == "") {
    //    key.append_i64(_region_id).append_i64(tableid).append_u64(UINT64_MAX);
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
    int64_t random_skew_lines = 1;
    int64_t skew_lines = pk_cnt * FLAGS_skew / 100;
    if (skew_lines > 0) {
        random_skew_lines = butil::fast_rand() % skew_lines;
    }
    
    int64_t lower_bound = pk_cnt / 2 - random_skew_lines;
    int64_t upper_bound = pk_cnt / 2 + random_skew_lines;

    std::string prev_key;
    std::string min_diff_key;
    uint32_t min_diff = UINT32_MAX;

    // 拿到term,之后开始分裂会校验term
    braft::NodeStatus s;
    _node.get_status(&s);
    split_key_term = s.term;
    std::string end_key = get_end_key();

    for (iter->Seek(key.data()); iter->Valid() 
            && iter->key().starts_with(key.data()); iter->Next()) {
        rocksdb::Slice pk_slice(iter->key());
        pk_slice.remove_prefix(2 * sizeof(int64_t));
        // check end_key
        if (pk_slice.compare(end_key) >= 0) {
            break;
        }

        cur_idx++;
        if (cur_idx < lower_bound) {
            continue;
        }
        //如果lower_bound 和 upper_bound 相同情况下走这个分支
        if (cur_idx > upper_bound) {
            if (min_diff_key.empty()) {
                min_diff_key = iter->key().ToString();
            }
            break;
        }
        if (prev_key.empty()) {
            prev_key = std::string(iter->key().data(), iter->key().size());
            continue;
        }
        uint32_t diff = rocksdb::Slice(prev_key).difference_offset(iter->key());
        //DB_WARNING("region_id: %ld, pre_key: %s, iter_key: %s, diff: %u", 
        //    _region_id,
        //    rocksdb::Slice(prev_key).ToString(true).c_str(),
        //    iter->key().ToString(true).c_str(),
        //    diff);
        if (diff < min_diff) {
            min_diff = diff;
            min_diff_key = iter->key().ToString();
            DB_WARNING("region_id: %ld, min_diff_key: %s", _region_id, min_diff_key.c_str());
        }
        if (min_diff == 2 * sizeof(int64_t)) {
            break;
        }
        prev_key = std::string(iter->key().data(), iter->key().size());
    }
    if (min_diff_key.size() < 16) {
        if (iter->Valid()) {
            DB_WARNING("min_diff_key is: %ld, %d, %ld, %ld, %ld, %lu, %s, %s, %s",
                    _num_table_lines.load(), iter->Valid(), cur_idx, lower_bound, upper_bound, min_diff_key.size(),
                    min_diff_key.c_str(),
                    iter->key().ToString(true).c_str(), 
                    iter->value().ToString(true).c_str());
        } else {
            DB_WARNING("min_diff_key is: %ld, %d, %ld, %ld, %ld, %lu, %s",
                    _num_table_lines.load(), iter->Valid(), cur_idx, lower_bound, upper_bound, min_diff_key.size(),
                    min_diff_key.c_str());
        }
        _split_param.split_term = 0;
        _num_table_lines = cur_idx;
        return -1;
    }
    split_key = min_diff_key.substr(16);
    DB_WARNING("table_id:%ld, split_pos:%ld, split_key:%s, region_id: %ld", 
        tableid, cur_idx, rocksdb::Slice(split_key).ToString(true).c_str(), _region_id);
    return 0;
}

int Region::add_reverse_index(int64_t table_id, const std::set<int64_t>& index_ids) {
    if (_is_global_index || table_id != get_table_id()) {
        return 0;
    }
    for (auto index_id : index_ids) {
        IndexInfo index = _factory->get_index_info(index_id);
        pb::SegmentType segment_type = index.segment_type;
        if (index.type == pb::I_FULLTEXT) {
            BAIDU_SCOPED_LOCK(_reverse_index_map_lock);
            if (_reverse_index_map.count(index.id) > 0) {
                DB_DEBUG("reverse index index_id:%ld already exist.", index_id);
                continue;
            }
            if (index.fields.size() != 1 || index.id < 1) {
                DB_WARNING("I_FULLTEXT field must be 1 index_id:%ld %ld", index_id, index.id);
                continue;
            }
            if (index.fields[0].type != pb::STRING) {
                segment_type = pb::S_NO_SEGMENT;
            }
            if (segment_type == pb::S_DEFAULT) {
    #ifdef BAIDU_INTERNAL
                segment_type = pb::S_WORDRANK;
    #else
                segment_type = pb::S_UNIGRAMS;
    #endif
            }

            DB_NOTICE("region_%ld index[%ld] type[FULLTEXT] add reverse_index", _region_id, index_id);
            
            if (index.storage_type == pb::ST_PROTOBUF_OR_FORMAT1) {
                DB_WARNING("create pb schema region_%ld index[%ld]", _region_id, index_id);
                _reverse_index_map[index.id] = new ReverseIndex<CommonSchema>(
                        _region_id, 
                        index.id,
                        FLAGS_reverse_level2_len,
                        _rocksdb,
                        segment_type,
                        false, // common need not cache
                        true
                );
            } else {
                DB_WARNING("create arrow schema region_%ld index[%ld]", _region_id, index_id);
                _reverse_index_map[index.id] = new ReverseIndex<ArrowSchema>(
                        _region_id, 
                        index.id,
                        FLAGS_reverse_level2_len,
                        _rocksdb,
                        segment_type,
                        false, // common need not cache
                        true
                );
            }
        } else {
            DB_DEBUG("index type[%s] not add reverse_index", pb::IndexType_Name(index.type).c_str());
        }
    }
    return 0;
}

void Region::remove_local_index_data() {
    if (_is_global_index || _removed || _shutdown || _is_binlog_region) {
        return;
    }
    int64_t main_table_id = get_table_id();
    auto table_info = _factory->get_table_info_ptr(main_table_id);
    if (table_info == nullptr) {
        return;
    }
    for (auto index_id: table_info->indices) {
        auto index_info = _factory->get_index_info_ptr(index_id);
        if (index_info == nullptr) {
            continue;
        }
        if (index_info->state == pb::IS_DELETE_LOCAL) {
            delete_local_rocksdb_for_ddl(main_table_id, index_id);
        }
    }
}

void Region::delete_local_rocksdb_for_ddl(int64_t table_id, int64_t index_id) {
    if (_shutdown) {
        return;
    }
    std::string value;
    std::string remove_key = _meta_writer->encode_removed_ddl_key(_region_id, index_id);
    rocksdb::ReadOptions options;
    auto status = _rocksdb->get(options, _meta_cf, rocksdb::Slice(remove_key), &value);
    if (status.ok()) {
        DB_DEBUG("DDL_LOG index data has been removed region_id:%ld, table_id:%ld index_id:%ld",
            _region_id, table_id, index_id);
        return;
    } else if (!status.ok() && !status.IsNotFound()) {
        DB_FATAL("DDL_LOG failed while read remove ddl key, Error %s, region_id: %ld table_id:%ld index_id:%ld",
            status.ToString().c_str(), _region_id, table_id, index_id);
        return;
    }
    rocksdb::WriteOptions write_options;
    MutTableKey begin_key;
    MutTableKey end_key;
    begin_key.append_i64(_region_id).append_i64(index_id);
    end_key.append_i64(_region_id).append_i64(index_id).append_u64(UINT64_MAX);
    status = _rocksdb->remove_range(write_options, _data_cf, begin_key.data(), end_key.data(), true);
    if (!status.ok()) {
        DB_FATAL("DDL_LOG remove_index failed: code=%d, msg=%s, region_id: %ld table_id:%ld index_id:%ld", 
            status.code(), status.ToString().c_str(), _region_id, table_id, index_id);
        return;
    }
    status = _rocksdb->put(write_options, _meta_cf,
                rocksdb::Slice(remove_key),
                rocksdb::Slice(value.data()));
    if (!status.ok()) {
        DB_FATAL("DDL_LOG write remove ddl key failed, err_msg: %s, region_id: %ld, table_id:%ld index_id:%ld",
            status.ToString().c_str(), _region_id, table_id, index_id);
        return;
    }
    DB_NOTICE("DDL_LOG remove index data region_id:%ld, table_id:%ld index_id:%ld",_region_id, table_id, index_id);
}

bool Region::can_use_approximate_split() {
    if (FLAGS_use_approximate_size &&
        FLAGS_use_approximate_size_to_split && 
            get_num_table_lines() > FLAGS_min_split_lines &&
            get_last_split_time_cost() > 60 * 60 * 1000 * 1000LL &&
            _approx_info.region_size > 512 * 1024 * 1024LL) {
        if (_approx_info.last_version_region_size > 0 && 
                _approx_info.last_version_table_lines > 0 && 
                _approx_info.region_size > 0 &&
                _approx_info.table_lines > 0) {
            double avg_size = 1.0 * _approx_info.region_size / _approx_info.table_lines;
            double last_avg_size = 1.0 * _approx_info.last_version_region_size / _approx_info.last_version_table_lines;
            // avg_size差不多时才能做
            // 严格点判断，减少分裂后不做compaction导致的再次分裂问题
            if (avg_size / last_avg_size < 1.2) {
                return true;
            } else if (_approx_info.region_size > 1024 * 1024 * 1024LL) {
                return true;
            } else {
                return false;
            }
        } else {
            return true;
        }
    } else {
        return false;
    }
}
// 后续要用compaction filter 来维护，现阶段主要有num_table_lines维护问题
void Region::ttl_remove_expired_data() {
    if (!_use_ttl) {
        return;
    }
    if (_shutdown) {
        return;
    } 
    _multi_thread_cond.increase();
    ON_SCOPE_EXIT([this]() {
        _multi_thread_cond.decrease_signal();
    });
    TimeCost time_cost;
    pb::RegionStatus expected_status = pb::IDLE; 
    if (!_region_control.compare_exchange_strong(expected_status, pb::DOING)) {
        DB_WARNING("ttl_remove_expired_data fail, region status is not idle,"
                 " region_id: %ld", _region_id);
        return;
    }   
    ON_SCOPE_EXIT([this]() {
        reset_region_status();
    });
    //遍历snapshot，写入索引。
    DB_WARNING("start ttl_remove_expired_data region_id: %ld ", _region_id);

    int64_t global_index_id = get_global_index_id();
    int64_t main_table_id = get_table_id();
    int64_t read_timestamp_us = butil::gettimeofday_us();
    std::vector<int64_t> indices;
    if (_is_global_index) {
        indices.push_back(global_index_id);
    } else {
        TableInfo table_info = _factory->get_table_info(main_table_id);
        for (auto index_id: table_info.indices) {
            if (_factory->is_global_index(index_id)) {
                continue;
            }
            indices.push_back(index_id);
        }
    }
    std::atomic<int64_t> write_sst_lines(0);

    IndexInfo pk_info = _factory->get_index_info(main_table_id);
    auto resource = get_resource();
    bool is_cstore = _factory->get_table_engine(main_table_id) == pb::ROCKSDB_CSTORE;
    int64_t del_pk_num = 0;
    for (int64_t index_id : indices) {
        MutTableKey table_prefix;
        table_prefix.append_i64(_region_id).append_i64(index_id);
        rocksdb::WriteOptions write_options;
        TimeCost cost;
        int64_t num_remove_lines = 0;
        rocksdb::ReadOptions read_options;
        read_options.prefix_same_as_start = true;
        read_options.total_order_seek = false;
        read_options.fill_cache = false;

        std::string end_key = get_end_key();
        IndexInfo index_info = _factory->get_index_info(index_id);
        std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _data_cf));
        rocksdb::ReadOptions read_opt;
        read_opt.fill_cache = false;
        rocksdb::TransactionOptions txn_opt;
        txn_opt.lock_timeout = 100;
        int64_t count = 0;
        for (iter->Seek(table_prefix.data()); iter->Valid(); iter->Next()) {
            if (FLAGS_stop_ttl_data) { 
                break;
            }
            ++count;
            rocksdb::Slice key_slice(iter->key());
            key_slice.remove_prefix(2 * sizeof(int64_t));
            if (index_info.type == pb::I_PRIMARY || _is_global_index) {
                // check end_key
                if (end_key_compare(key_slice, end_key) >= 0) {
                    break;
                }
            }
            rocksdb::Slice value_slice1(iter->value());
            if (ttl_decode(value_slice1, &index_info, _online_ttl_base_expire_time_us) > read_timestamp_us) {
                //未过期
                continue;
            }
            // 内部txn，不提交出作用域自动析构
            SmartTransaction txn(new Transaction(0, nullptr));
            txn->begin(txn_opt);
            rocksdb::Status s;
            std::string value;
            s = txn->get_txn()->GetForUpdate(read_opt, _data_cf, iter->key(), &value);
            if (!s.ok()) {
                DB_WARNING("index %ld, region_id: %ld GetForUpdate failed, status: %s", 
                        index_id, _region_id, s.ToString().c_str());
                continue;
            }
            rocksdb::Slice value_slice2(value);
            if (ttl_decode(value_slice2, &index_info, _online_ttl_base_expire_time_us) > read_timestamp_us) {
                //加锁校验未过期
                continue;
            }
            s = txn->get_txn()->Delete(_data_cf, iter->key());
            if (!s.ok()) {
                DB_FATAL("index %ld, region_id: %ld Delete failed, status: %s", 
                        index_id, _region_id, s.ToString().c_str());
                continue;
            }
            // for cstore only, remove_columns
            if (is_cstore && index_info.type == pb::I_PRIMARY && !_is_global_index) {
                txn->set_resource(resource);
                txn->remove_columns(iter->key());
            }
            // 维护num_table_lines
            // 不好维护，走raft的话，切主后再做ttl删除可能多删
            // 不走raft，发送snapshot后，follow也会多删
            // leader做过期，然后同步给follower，实现可能太复杂
            // 最好的办法是预估而不是维护精准的num_table_lines
            if (index_info.type == pb::I_PRIMARY || _is_global_index) {
                ++_num_delete_lines;
                ++del_pk_num;
            }
            s = txn->commit();
            if (!s.ok()) {
                DB_FATAL("index %ld, region_id: %ld commit failed, status: %s", 
                        index_id, _region_id, s.ToString().c_str());
                continue;
            }
            ++num_remove_lines;
        }
        DB_WARNING("scan index:%ld, cost: %ld, scan count: %ld, remove lines: %ld, region_id: %ld", 
                index_id, cost.get_time(), count, num_remove_lines, _region_id);
        if (index_id == global_index_id) {
            // num_table_lines维护不准，用来空region merge
            if (count == 0 && _num_table_lines < 200) {
                _num_table_lines = 0;
            }
        }
    }
    if (del_pk_num > 0) {
        // 更新num_table_lines
        int64_t new_num_table_lines = _num_table_lines - del_pk_num;
        if (new_num_table_lines < 0) {
            new_num_table_lines = 0;
        }
        set_num_table_lines(new_num_table_lines);
    }
    DB_WARNING("end ttl_remove_expired_data, cost: %ld region_id: %ld, num_table_lines: %ld ", 
            time_cost.get_time(), _region_id, _num_table_lines.load());
}

void Region::process_download_sst(brpc::Controller* cntl, 
    std::vector<std::string>& request_vec, SstBackupType backup_type) {

    BAIDU_SCOPED_LOCK(_backup_lock);
    if (_shutdown || !_init_success) {
        DB_WARNING("region[%ld] is shutdown or init_success.", _region_id);
        return;
    } 

    _multi_thread_cond.increase();
    ON_SCOPE_EXIT([this]() {
        _multi_thread_cond.decrease_signal();
    });

    DB_NOTICE("backup region_id[%ld]", _region_id);
    _backup.process_download_sst(cntl, request_vec, backup_type);
}

void Region::process_upload_sst(brpc::Controller* cntl, bool ingest_store_latest_sst) {
    BAIDU_SCOPED_LOCK(_backup_lock);
    if (_shutdown || !_init_success) {
        DB_WARNING("region[%ld] is shutdown or init_success.", _region_id);
        return;
    } 

    _multi_thread_cond.increase();
    ON_SCOPE_EXIT([this]() {
        _multi_thread_cond.decrease_signal();
    });

    DB_NOTICE("backup region[%ld] process upload sst.", _region_id);
    _backup.process_upload_sst(cntl, ingest_store_latest_sst);
}

void Region::process_download_sst_streaming(brpc::Controller* cntl, 
    const pb::BackupRequest* request,
    pb::BackupResponse* response) {
    BAIDU_SCOPED_LOCK(_backup_lock);
    if (_shutdown || !_init_success) {
        DB_WARNING("region[%ld] is shutdown or init_success.", _region_id);
        response->set_errcode(pb::BACKUP_ERROR);
        return;
    }

    _multi_thread_cond.increase();
    ON_SCOPE_EXIT([this]() {
        _multi_thread_cond.decrease_signal();
    });

    DB_NOTICE("backup region_id[%ld]", _region_id);
    _backup.process_download_sst_streaming(cntl, request, response);

}

void Region::process_upload_sst_streaming(brpc::Controller* cntl, bool ingest_store_latest_sst,
    const pb::BackupRequest* request,
    pb::BackupResponse* response) {

    BAIDU_SCOPED_LOCK(_backup_lock);
    if (_shutdown || !_init_success) {
        DB_WARNING("region[%ld] is shutdown or init_success.", _region_id);
        return;
    } 

    _multi_thread_cond.increase();
    ON_SCOPE_EXIT([this]() {
        _multi_thread_cond.decrease_signal();
    });

    DB_NOTICE("backup region[%ld] process upload sst.", _region_id);
    _backup.process_upload_sst_streaming(cntl, ingest_store_latest_sst, request, response);

}

void Region::process_query_peers(brpc::Controller* cntl, const pb::BackupRequest* request,
    pb::BackupResponse* response) {
    if (_shutdown || !_init_success) {
        response->set_errcode(pb::BACKUP_ERROR);
        DB_WARNING("region[%ld] is shutdown or init_success.", _region_id);
        return;
    } 

    if (!is_leader()) {
        response->set_errcode(pb::NOT_LEADER);
        response->set_leader(butil::endpoint2str(get_leader()).c_str());
        DB_WARNING("not leader old version, leader:%s, region_id: %ld",
                butil::endpoint2str(get_leader()).c_str(), _region_id);
        return;
    }

    braft::NodeStatus status;
    _node.get_status(&status);
    response->set_errcode(pb::SUCCESS);
    response->add_peers(butil::endpoint2str(get_leader()).c_str());
    for (const auto& iter : status.stable_followers) {
        response->add_peers(butil::endpoint2str(iter.first.addr).c_str());
    }

    for (const auto& iter : status.unstable_followers) {
        response->add_unstable_followers(butil::endpoint2str(iter.first.addr).c_str());
    }
}

void Region::process_query_streaming_result(brpc::Controller *cntl, const pb::BackupRequest *request,
                                            pb::BackupResponse *response) {
    if (_shutdown || !_init_success) {
        response->set_errcode(pb::BACKUP_ERROR);
        DB_WARNING("region[%ld] is shutdown or init_success.", _region_id);
        return;
    }
    brpc::StreamId id = request->streaming_id();
    BAIDU_SCOPED_LOCK(_streaming_result.mutex);
    if (_streaming_result.state.find(id) == _streaming_result.state.end()) {
        response->set_streaming_state(pb::StreamState::SS_INIT);
    } else {
        response->set_streaming_state(_streaming_result.state[id]);
    }
    response->set_errcode(pb::SUCCESS);
}

} // end of namespace
