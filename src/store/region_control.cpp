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

#include "region_control.h"
#include <boost/filesystem.hpp>
#include "rpc_sender.h"
#include "store.h"
#include "concurrency.h"
#include "region.h"
#include "mut_table_key.h"
#include "my_raft_log_storage.h"
#include "closure.h"
#include "raft_control.h"

namespace baikaldb {
DECLARE_string(snapshot_uri);
DECLARE_string(stable_uri);
DECLARE_int64(transfer_leader_catchup_time_threshold);
DECLARE_int64(store_heart_beat_interval_us);
DEFINE_int32(compact_interval, 1, "compact_interval xx (s)");
DEFINE_bool(allow_compact_range, true, "allow_compact_range");
DEFINE_bool(allow_blocking_flush, true, "allow_blocking_flush");
int RegionControl::remove_data(int64_t drop_region_id) {
    rocksdb::WriteOptions options;
    MutTableKey start_key;
    MutTableKey end_key;
    start_key.append_i64(drop_region_id);

    end_key.append_i64(drop_region_id);
    end_key.append_u64(UINT64_MAX);

    auto rocksdb = RocksWrapper::get_instance();
    auto data_cf = rocksdb->get_data_handle();
    if (data_cf == nullptr) {
        DB_WARNING("get rocksdb data column family failed, region_id: %ld", drop_region_id);
        return -1;
    }
    TimeCost cost;
    auto res = rocksdb->remove_range(options, data_cf, 
            start_key.data(), end_key.data(), true);
    if (!res.ok()) {
        DB_WARNING("remove_range error: code=%d, msg=%s, region_id: %ld", 
            res.code(), res.ToString().c_str(), drop_region_id);
        return -1;
    }
    DB_WARNING("region clear data, remove_range cost:%ld, region_id: %ld", cost.get_time(), drop_region_id);
    return 0;
}

void RegionControl::compact_data(int64_t region_id) {
    MutTableKey start_key;
    MutTableKey end_key;
    start_key.append_i64(region_id);

    end_key.append_i64(region_id);
    end_key.append_u64(UINT64_MAX);

    auto rocksdb = RocksWrapper::get_instance();
    auto data_cf = rocksdb->get_data_handle();
    if (data_cf == nullptr) {
        DB_WARNING("get rocksdb data column family failed, region_id: %ld", region_id);
        return;
    }
    TimeCost cost;
    rocksdb::Slice start(start_key.data());
    rocksdb::Slice end(end_key.data());
    rocksdb::CompactRangeOptions compact_options;
    compact_options.exclusive_manual_compaction = false;
    auto res = rocksdb->compact_range(compact_options, data_cf, &start, &end);
    if (!res.ok()) {
        DB_WARNING("region_id:%ld, compact_range error: code=%d, msg=%s", 
                region_id, res.code(), res.ToString().c_str());
    }
    DB_WARNING("region_id: %ld, compact_range cost:%ld", region_id, cost.get_time());
}
void RegionControl::compact_data_in_queue(int64_t region_id) {
    static ThreadSafeMap<int64_t, bool> in_compact_regions;
    if (in_compact_regions.count(region_id) == 1) {
        DB_WARNING("region_id: %ld have be put in queue before", region_id);
        return;
    }
    in_compact_regions[region_id] = true;
    Store::get_instance()->compact_queue().run([region_id]() {
        if (in_compact_regions.count(region_id) == 1) {
            if (!Store::get_instance()->is_shutdown() && FLAGS_allow_compact_range) {
                RegionControl::compact_data(region_id);
                in_compact_regions.erase(region_id);
                bthread_usleep(FLAGS_compact_interval * 1000 * 1000LL);
            }
        }
    });
}

int RegionControl::remove_meta(int64_t drop_region_id) {
    return MetaWriter::get_instance()->clear_all_meta_info(drop_region_id);
}

int RegionControl::remove_log_entry(int64_t drop_region_id) {
    TimeCost cost;
    rocksdb::WriteOptions options;
    MutTableKey start_key;
    MutTableKey end_key;
    start_key.append_i64(drop_region_id);

    end_key.append_i64(drop_region_id);
    end_key.append_u64(UINT64_MAX);
    auto rocksdb = RocksWrapper::get_instance();
    // sleep会，等待异步日志刷盘
    bthread_usleep(1000 * 1000);
    auto status = rocksdb->remove_range(options,
                                    rocksdb->get_raft_log_handle(),
                                    start_key.data(),
                                    end_key.data(),
                                    true);
    if (!status.ok()) {
        DB_WARNING("remove_range error: code=%d, msg=%s, region_id: %ld",
            status.code(), status.ToString().c_str(), drop_region_id);
        return -1;
    }
    DB_WARNING("remove raft log entry, region_id: %ld, cost: %ld", drop_region_id, cost.get_time());
    MutTableKey log_data_key;
    log_data_key.append_i64(drop_region_id).append_u8(MyRaftLogStorage::LOG_DATA_IDENTIFY).append_i64(1);
    rocksdb::ReadOptions opt;
    opt.prefix_same_as_start = true;
    opt.total_order_seek = false;
    opt.fill_cache = false;
    std::unique_ptr<rocksdb::Iterator> iter(rocksdb->new_iterator(opt, rocksdb->get_raft_log_handle()));
    iter->Seek(log_data_key.data());
    if (iter->Valid()) {
        int64_t log_index = TableKey(iter->key()).extract_i64(sizeof(int64_t) + 1);
        rocksdb::Slice value(iter->value());
        LogHead head(value);
        value.remove_prefix(MyRaftLogStorage::LOG_HEAD_SIZE); 
        pb::StoreReq req;
        req.ParseFromArray(value.data(), value.size());
        DB_WARNING("remove raft log entry, region_id: %ld, cost:%ld, log_index:%ld, type:%d, %s", 
                drop_region_id, cost.get_time(), log_index, head.type, req.ShortDebugString().c_str());
    }
    return 0;
}

//todo 测一下如果目录为空，删除返回值时啥
int RegionControl::remove_snapshot_path(int64_t drop_region_id) {
    std::string snapshot_path_str(FLAGS_snapshot_uri, FLAGS_snapshot_uri.find("//") + 2);
    snapshot_path_str += "/region_" + std::to_string(drop_region_id);
    std::string stable_path_str(FLAGS_stable_uri, FLAGS_stable_uri.find("//") + 2);
    stable_path_str += "/region_" + std::to_string(drop_region_id);
    try {
        boost::filesystem::path snapshot_path(snapshot_path_str);
        boost::filesystem::path stable_path(stable_path_str);
        boost::filesystem::remove_all(snapshot_path);
        boost::filesystem::remove_all(stable_path);
    } catch (boost::filesystem::filesystem_error& e) {
        DB_FATAL("remove error:%s, region_id: %ld", e.what(), drop_region_id);
        return -1;
    }
    DB_WARNING("drop snapshot directory, region_id: %ld", drop_region_id);
    return 0;
}
int RegionControl::clear_all_infos_for_region(int64_t drop_region_id) {
    DB_WARNING("region_id: %ld, clear_all_infos_for_region do compact in queue", drop_region_id);
    //compact_data_in_queue(drop_region_id);
    remove_data(drop_region_id);
    remove_meta(drop_region_id);
    remove_snapshot_path(drop_region_id);
    remove_log_entry(drop_region_id);
    return 0;
}
int RegionControl::ingest_data_sst(const std::string& data_sst_file, int64_t region_id, bool move_files) {
    auto rocksdb = RocksWrapper::get_instance();
    rocksdb::IngestExternalFileOptions ifo;
    // snapshot恢复流程需要保留原始文件
    // TODO 修改恢复流程，可以减少一次文件copy
    ifo.move_files = move_files;
    ifo.write_global_seqno = false;
    ifo.allow_blocking_flush = FLAGS_allow_blocking_flush;
    auto data_cf = rocksdb->get_data_handle();
    auto res = rocksdb->ingest_external_file(data_cf, {data_sst_file}, ifo);
    if (!res.ok()) {
        DB_WARNING("ingest file %s fail, Error %s, region_id: %ld",
            data_sst_file.c_str(), res.ToString().c_str(), region_id);
        if (!FLAGS_allow_blocking_flush) {
            rocksdb::FlushOptions flush_options;
            res = rocksdb->flush(flush_options, data_cf);
            if (!res.ok()) {
                DB_WARNING("flush data to rocksdb fail, err_msg:%s", res.ToString().c_str());
                return -1;
            }
            res = rocksdb->ingest_external_file(data_cf, {data_sst_file}, ifo);
            if (!res.ok()) {
                DB_WARNING("Error while adding file %s, Error %s, region_id: %ld",
                    data_sst_file.c_str(), res.ToString().c_str(), region_id);
                return -1;
            }
            return 0;
        }
        return -1;
    }
    return 0;
}
int RegionControl::ingest_meta_sst(const std::string& meta_sst_file, int64_t region_id) {
    return MetaWriter::get_instance()->ingest_meta_sst(meta_sst_file, region_id);
}

int RegionControl::sync_do_snapshot() {
    DB_WARNING("region_id: %ld sync_do_snapshot start", _region_id);
    std::string address = Store::get_instance()->address();
    butil::EndPoint leader = _region->get_leader();
    if (leader.ip != butil::IP_ANY) {
        address = butil::endpoint2str(leader).c_str();
    }
    auto ret = RpcSender::send_no_op_request(address,
                       _region_id,
                       _region->_region_info.version());
    if (ret < 0) {
        DB_WARNING("send no op fail, region_id:%ld", _region_id);  
        return -1; 
    }
    BthreadCond sync_sign;
    sync_sign.increase();
    ConvertToSyncClosure* done = new ConvertToSyncClosure(sync_sign, _region_id);
    if (!_region->is_learner()) {
        _region->_node.snapshot(done);
    } else {
        if (_region->_learner != nullptr) {
            _region->_learner->snapshot(done);
        } else {
            DB_FATAL("region_id %ld learner is nullptr.", _region_id);
        }
    }
    sync_sign.wait(); 
    DB_WARNING("region_id: %ld sync_do_snapshot success", _region_id);
    return 0;
}

void RegionControl::raft_control(google::protobuf::RpcController* controller,
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
    if (!_region->is_leader() && !request->force()) {
        DB_WARNING("node is not leader when add_peer, region_id: %ld", _region_id);
        if (response != NULL) {
            response->set_errcode(pb::NOT_LEADER);
            response->set_errmsg("not leader");
            response->set_leader(butil::endpoint2str(_region->get_leader()).c_str());
        }
        return;
    }
    //每个region同时只能进行一个动作
    if (make_region_status_doing() == 0) {
        DB_WARNING("region status to doing becase of raft control, region_id: %ld, request:%s",
                    _region_id, pb2json(*request).c_str());
        common_raft_control(controller, request, response, done_guard.release(), &_region->_node);
    } else {
        DB_FATAL("region status is not idle, region_id: %ld, lod_id:%lu", _region_id, log_id);
        response->set_errcode(pb::REGION_ERROR_STATUS);
        response->set_errmsg("status not idle");
    }
}

void RegionControl::add_peer(const pb::AddPeer& add_peer, SmartRegion region, ExecutionQueue& queue) {
    //这是按照在metaServer里add_peer的构造规则解析出来的newew——instance,
    //是解析new_instance的偷懒版本
    std::string new_instance = add_peer.new_peers(add_peer.new_peers_size() - 1);
    // 先校验，减少doing状态
    // 该方法会多次check
    if (legal_for_add_peer(add_peer, NULL) != 0) {
        return;
    }
    //在leader_send_init_region将状态置为doing, 在add_peer结束时将region状态置回来
    if (make_region_status_doing() != 0) {
        DB_WARNING("make region status doing fail, region_id: %ld", _region_id);
        return;
    }
    TimeCost cost;
    DB_WARNING("region status to doing becase of add peer of heartbeat response, "
            "region_id: %ld new_instance: %s", _region_id, new_instance.c_str());
    if (legal_for_add_peer(add_peer, NULL) != 0) {
        reset_region_status();
        return;
    }
    auto init_and_add_peer = [add_peer, new_instance, region, cost]() {
        RegionControl& control = region->get_region_control();
        if (region->_shutdown) {
            DB_WARNING("region_id:%ld has REMOVE", region->get_region_id());
            control.reset_region_status();
            return;
        }
        DB_WARNING("start init_region, region_id: %ld, wait_time:%ld", 
                region->get_region_id(), cost.get_time());
        if (cost.get_time() > FLAGS_store_heart_beat_interval_us * 5) {
            DB_WARNING("region_id: %ld add peer timeout", region->get_region_id());
            control.reset_region_status();
            return;
        }
        if (control.legal_for_add_peer(add_peer, NULL) != 0) {
            control.reset_region_status();
            return;
        }
        pb::InitRegion init_request;
        control.construct_init_region_request(init_request);
        if (control.init_region_to_store(new_instance, init_request, NULL) != 0) {
            control.reset_region_status();
            return;
        }
        if (control.legal_for_add_peer(add_peer, NULL) != 0) {
            control.reset_region_status();
            region->start_thread_to_remove_region(region->get_region_id(), new_instance);
            return;
        }
        control.node_add_peer(add_peer, new_instance, NULL, NULL);
    };
    queue.run(init_and_add_peer);
}

void RegionControl::add_peer(const pb::AddPeer* request,
                                pb::StoreRes* response,
                                google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
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
    // 先校验，减少doing状态
    // 该方法会多次check
    if (legal_for_add_peer(*request, response) != 0) {
        return;
    }
    if (!request->is_split() && make_region_status_doing() != 0) {
        response->set_errcode(pb::INTERNAL_ERROR);
        response->set_errmsg("new region fail");
        return;
    } 
    if (request->is_split() && add_doing_cnt() != 0) {
        response->set_errcode(pb::INTERNAL_ERROR);
        response->set_errmsg("new region fail");
        return;
    }
    DB_WARNING("region status to doing because of add peer of request, region_id: %ld",
                _region_id);
    if (legal_for_add_peer(*request, response) != 0) {
        reset_region_status();
        return;
    }
    pb::InitRegion init_request;
    construct_init_region_request(init_request);
    init_request.set_is_split(request->is_split());
    if (request->is_split()) {
        init_request.mutable_region_info()->set_can_add_peer(false);
    }
    if (init_region_to_store(new_instance, init_request, response) != 0) {
        reset_region_status();
        return;
    }
    if (legal_for_add_peer(*request, response) != 0) {
        reset_region_status();
        _region->start_thread_to_remove_region(_region_id, new_instance);
        return;
    }
    node_add_peer(*request, new_instance, response, done_guard.release());
}

int RegionControl::transfer_leader(const pb::TransLeaderRequest& trans_leader_request, 
        SmartRegion region, ExecutionQueue& queue) {
    std::string new_leader = trans_leader_request.new_leader();
    auto transfer_call = [this, region, new_leader]() {
        int64_t peer_applied_index = 0;
        int64_t peer_dml_latency = 0;
        if (region == nullptr) {
            return;
        }
        int ret = RpcSender::get_peer_applied_index(new_leader, _region_id, peer_applied_index, peer_dml_latency);
        if (ret != 0) {
            DB_WARNING("get_peer_applied_index fail,region_id: %ld", region->get_region_id());
            return;
        }
        if ((region->_applied_index - peer_applied_index) * peer_dml_latency
                > (FLAGS_transfer_leader_catchup_time_threshold)) {
            DB_WARNING("peer applied index: %ld is less than applied index: %ld, peer_dml_latency: %ld",
                    peer_applied_index, region->_applied_index, peer_dml_latency);
            return;
        }
        if (region->_shutdown) {
            DB_WARNING("region_id:%ld has REMOVE", region->get_region_id());
            reset_region_status();
            return;
        }
        if (make_region_status_doing() != 0) {
            DB_FATAL("region status is not idle when transfer leader, region_id: %ld", _region_id);
            return;
        } else {
            DB_WARNING("region status to doning becase of transfer leader of heartbeat response,"
                    " region_id: %ld", _region_id);
        }
        ret = region->transfer_leader_to(new_leader);
        reset_region_status();
        if (ret != 0) {
            DB_WARNING("node:%s %s transfer leader fail",
                    region->_node.node_id().group_id.c_str(),
                    region->_node.node_id().peer_id.to_string().c_str());
            return;
        }
    };
    queue.run(transfer_call);
    return 0;
}

int RegionControl::init_region_to_store(const std::string instance_address,
                         const pb::InitRegion& init_region_request,
                         pb::StoreRes* store_response) {
    TimeCost cost;
    pb::StoreRes response;
    RpcSender::send_init_region_method(instance_address, init_region_request, response);
    if (response.errcode() == pb::CONNECT_FAIL) {
        DB_FATAL("add peer fail when connect new instance, region_id: %ld, instance:%s",
                  _region_id, instance_address.c_str());
        if (store_response != NULL) {
            store_response->set_errcode(pb::CONNECT_FAIL);
            store_response->set_errmsg("connet with " + instance_address + " fail");
        }
        return -1;
    }
    int64_t region_id = init_region_request.region_info().region_id();
    if (response.errcode() == pb::EXEC_FAIL) {
        DB_WARNING("add peer fail when init region,"
                   " region_id: %ld, instance:%s, cntl.Failed",
                   region_id, instance_address.c_str());
        if (store_response != NULL) {
             store_response->set_errcode(pb::EXEC_FAIL);
             store_response->set_errmsg("exec fail");
        }
        return -1;
    }
    if (store_response != NULL) {
        store_response->set_errcode(response.errcode());
        store_response->set_errmsg(response.errmsg());
    }
    if (response.errcode() != pb::SUCCESS) {
        DB_WARNING("add peer fail when init region,"
                   " region_id: %ld, instance:%s, errcode:%s",
                   region_id, instance_address.c_str(),
                   pb::ErrCode_Name(response.errcode()).c_str());
        return -1;
    }
    DB_WARNING("send init region to store:%s success, region_id: %ld, cost: %ld",
                instance_address.c_str(), _region_id, cost.get_time());
    return 0;
}


void RegionControl::construct_init_region_request(pb::InitRegion& init_request) {
    pb::RegionInfo* region_info = init_request.mutable_region_info();
    _region->copy_region(region_info);
    region_info->set_log_index(0);
    region_info->clear_peers();//初始peer为空
    region_info->set_leader(_region->_address);
    region_info->set_status(pb::IDLE);
    region_info->set_can_add_peer(true);
    //add_peer的region的version设为0，如果长时间没有通过on_snapshot_load把version置位其他，自动删除
    region_info->set_version(0);
    init_request.set_snapshot_times(0);
}

int RegionControl::legal_for_add_peer(const pb::AddPeer& add_peer, pb::StoreRes* response) {
    DB_WARNING("start legal_for_add_peer, region_id: %ld", _region_id);
    //判断收到请求的合法性，包括该peer是否是leader，list_peer值跟add_peer的old_peer是否相等，该peer的状态是否是IDEL
    if (!_region->is_leader()) {
        DB_WARNING("node is not leader when add_peer, region_id: %ld", _region_id);
        if (response != NULL) {
            response->set_errcode(pb::NOT_LEADER);
            response->set_errmsg("not leader");
            response->set_leader(butil::endpoint2str(_region->get_leader()).c_str());
        }
        return -1;
    }
    if (_region->_shutdown) {
        DB_WARNING("node is shutdown when add_peer, region_id: %ld", _region_id);
        if (response != NULL) {
            response->set_errcode(pb::NOT_LEADER);
            response->set_errmsg("not leader");
        }
        return -1;
    }
    if (!_region->_region_info.can_add_peer() || (!add_peer.is_split() && _region->_region_info.version() < 1)) {
        DB_WARNING("region_id: %ld can not add peer, can_add_peer:%d, region_version:%ld",
                  _region_id, _region->_region_info.can_add_peer(), _region->_region_info.version());
        if (response != NULL) {
            response->set_errcode(pb::CANNOT_ADD_PEER);
            response->set_errmsg("can not add peer");
        }
        return -1;
    }
    std::vector<braft::PeerId> peers;
    std::vector<std::string> str_peers;
    if (!_region->_node.list_peers(&peers).ok()) {
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

void RegionControl::node_add_peer(const pb::AddPeer& add_peer,
                              const std::string& new_instance,
                              pb::StoreRes* response,
                              google::protobuf::Closure* done) {
    DB_WARNING("_leader_add_peer, region_id: %ld ,new_instance: %s",
        _region_id, new_instance.c_str());
    //send add_peer to self
    Concurrency::get_instance()->add_peer_concurrency.increase_wait();
    DB_WARNING("_leader_add_peer, region_id: %ld ,new_instance: %s wait sucess",
                _region_id, new_instance.c_str());
    AddPeerClosure* add_peer_done = new AddPeerClosure(
            Concurrency::get_instance()->add_peer_concurrency);
    add_peer_done->region = _region;
    add_peer_done->done = done;
    add_peer_done->response = response;
    add_peer_done->new_instance = new_instance;
    if (add_peer.is_split()) {
        add_peer_done->is_split = true;
    }
    //done方法中会把region_status重置为IDLE
    braft::PeerId add_peer_instance;
    add_peer_instance.parse(new_instance);
    _region->_node.add_peer(add_peer_instance, add_peer_done);
    DB_WARNING("_leader_add_peer, region_id: %ld ,new_instance: %s send sucess",
                _region_id, new_instance.c_str());
}

}

