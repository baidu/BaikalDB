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

#include <store.h>
#include <boost/lexical_cast.hpp>
#include <boost/scoped_array.hpp>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <gflags/gflags.h>
#include "rocksdb/utilities/memory_util.h"
#include "mut_table_key.h"
#include "my_raft_log_storage.h"
#include "rocksdb/cache.h"
#include "rocksdb/utilities/write_batch_with_index.h"

namespace bthread {
DECLARE_int32(bthread_concurrency); //bthread.cpp
}

namespace baikaldb {
DECLARE_int32(store_heart_beat_interval_us);
DECLARE_int32(balance_periodicity);
DECLARE_string(stable_uri);
DECLARE_string(snapshot_uri);
DECLARE_string(save_applied_index_path);
DEFINE_int32(reverse_merge_interval_us, 2 * 1000 * 1000,  "reverse_merge_interval(2 s)");
//DEFINE_int32(update_status_interval_us, 2 * 1000 * 1000,  "update_status_interval(2 s)");
DEFINE_int32(store_port, 8110, "Server port");
DEFINE_string(db_path, "./rocks_db", "rocksdb path");
DEFINE_string(resource_tag, "", "resource tag");
DEFINE_int32(update_used_size_interval_us, 10 * 1000 * 1000, "update used size interval (10 s)");
DEFINE_int64(capacity, 100 * 1024 * 1024 * 1024LL, "store memory size, defalut:100G");
DEFINE_int32(split_threshold , 150, "split_thread, defalut: 150 * region_size / 100");
DEFINE_int32(byte_size_per_record, 500, "byte size per record, 100");
DEFINE_int64(flush_region_interval_us, 10 * 60 * 1000 * 1000LL, 
            "flush region interval, defalut(10 min)");
DEFINE_int64(transaction_clear_interval_ms, 1000LL,
            "transaction clear interval, defalut(1s)");
DEFINE_int32(max_split_concurrency, 2, "max split region concurrency, default:2");
DEFINE_string(quit_gracefully_file, "./quit_gracefully", "quit gracefully file");
const std::string Store::SCHEMA_IDENTIFY(1, 0x02);
const std::string Store::REGION_SCHEMA_IDENTIFY(1, 0x05);

Store::~Store() {
    // if (_db_status_run) {
    //     _db_status_thread.join();
    //     _db_status_run = false;
    // }
}

int Store::init_before_listen(std::vector<std::int64_t>& init_region_ids) {
    butil::EndPoint addr;
    addr.ip = butil::my_ip();
    addr.port = FLAGS_store_port; 
    _address = endpoint2str(addr).c_str(); 
    if (_meta_server_interact.init() != 0) {
        DB_FATAL("meta server interact init fail");
        return -1;
    }
    //int ret = get_physical_room(_address, _physical_room);
    //if (ret < 0) {
    //    DB_FATAL("get physical room fail");
    //    return -1;
    //}
    boost::trim(FLAGS_resource_tag);
    _resource_tag = FLAGS_resource_tag;
    // init rocksdb handler
    _rocksdb = RocksWrapper::get_instance();
    if (!_rocksdb) {
        DB_FATAL("create rocksdb handler failed");
        return -1;
    }
    int32_t res = _rocksdb->init(FLAGS_db_path);
    if (res != 0) {
        DB_FATAL("rocksdb init failed: code:%d", res);
        return -1;
    }

    _region_handle = _rocksdb->get_meta_info_handle();
    // init val 
    _factory = SchemaFactory::get_instance();
    _rocksdb->get_db()->GetAllPreparedTransactions(&_recovered_txns);
    for (auto iter = _recovered_txns.begin(); iter != _recovered_txns.end(); ) {
        DB_WARNING("recovered_txn: %s", (*iter)->GetName().c_str());
        if ((*iter)->GetWriteBatch()->GetWriteBatch()->Count() == 0) {
            DB_WARNING("rollback no_write transaction: %s", (*iter)->GetName().c_str());
            (*iter)->Rollback();
            iter = _recovered_txns.erase(iter);
            continue;
        }
        iter++;
    }

    int64_t used_size_sum = 0;
    pb::StoreHeartBeatRequest request;
    pb::StoreHeartBeatResponse response;
    //1、构造心跳请求, 重启时的心跳包除了实例信息外，其他都为空
    _construct_heart_beat_request(request);
    //重启时region信息为空，所以需要特殊处理一下used_size
    request.mutable_instance_info()->set_used_size(used_size_sum);
    DB_WARNING("heart beat request:%s when init store", request.ShortDebugString().c_str());
    
    //2、发送请求
    if (_meta_server_interact.send_request("store_heartbeat", request, response) == 0) {
        DB_WARNING("heart beat response:%s when init store", response.ShortDebugString().c_str());
        //处理心跳, 重启拉到的第一个心跳包只有schema信息
        _process_heart_beat_response(response);
    } else {
        DB_FATAL("send heart beat request to meta server fail");
        return -1;
    }
    DB_WARNING("get schema info from meta server success");

    //系统重启之前有哪些reigon
    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _region_handle));
    
    std::string region_prefix = SCHEMA_IDENTIFY;
    region_prefix += REGION_SCHEMA_IDENTIFY;

    bool all_has_applied_path = true;
    for (iter->Seek(region_prefix); iter->Valid(); iter->Next()) {
        pb::RegionInfo region_info;
        if (!region_info.ParseFromString(iter->value().ToString())) {
            DB_FATAL("parse from pb fail when load region snapshot, key:%s", 
                      iter->value().ToString().c_str());
            return -1;
        }
        DB_WARNING("region_info:%s when init store", region_info.ShortDebugString().c_str());
        int64_t region_id = region_info.region_id();
        TableInfo table_info = _factory->get_table_info(region_info.table_id());
        if (table_info.id == -1) {
            if (_drop_region_from_rocksdb(region_id) != 0) {
                DB_FATAL("drop region from rocksdb fail, region_id: %ld", region_id);
                continue;
            }
            std::string applied_path = FLAGS_save_applied_index_path + "/region_" + std::to_string(region_id);
            boost::filesystem::path remove_path(applied_path); 
            boost::filesystem::remove_all(remove_path);
            //删除region 数据
            _remove_region_data(region_id);
            //删除snapshot目录
            _remove_snapshot_path(region_id);
            //删除log_entry
            _remove_log_entry(region_id);
            DB_FATAL("drop region from store, region_id: %ld, region_info:%s, table_id:%ld", 
                        region_id, region_info.ShortDebugString().c_str(), table_info.id);
            continue;
        }
        //construct region
        braft::GroupId groupId(std::string("region_")
                + boost::lexical_cast<std::string>(region_id));
        butil::EndPoint addr;
        str2endpoint(_address.c_str(), &addr);
        braft::PeerId peerId(addr, 0);
        //重启的region初始化时peer要为空
        region_info.clear_peers();
        SmartRegion region(new(std::nothrow) Region(_rocksdb, 
                    _factory,
                    _address,
                    groupId,
                    peerId,
                    region_info, 
                    region_id));
        if (region == NULL) {
            DB_FATAL("new region fail. mem accolate fail. region_info:%s", 
                    region_info.ShortDebugString().c_str());
            return -1; 
        }
        //初始化单线程不需要加锁
        //std::unique_lock<std::mutex> region_lock(_region_mutex);
        _region_mapping.set(region_id, region);
        init_region_ids.push_back(region_id);
        //判断是否存在applied_index文件
        std::string applied_path = FLAGS_save_applied_index_path + "/region_" + 
                                    std::to_string(region_id);
        if (!boost::filesystem::exists(boost::filesystem::path(applied_path))) {
            all_has_applied_path = false;        
        }
    }
    //判断上次是否是优雅退出
    //为了兼容之前的服务，即使没有quit_gracefully文件，如果applied_path目录下region_*目录的数量
    //等于region数量，也认为是优雅退出
    if (boost::filesystem::exists( boost::filesystem::path(FLAGS_quit_gracefully_file))) {
        //优雅退出, 删除优雅退出标志文件
        DB_WARNING("last quit is gracefully");
        boost::filesystem::remove(boost::filesystem::path(FLAGS_quit_gracefully_file));
    } else if (all_has_applied_path) {
        DB_WARNING("last quit is gracefully");
    } else {
        //非优雅退出
        //如果store没有优雅退出，则不需要判断reigon是否有applied文件，必须走snapshot+log_entry删除
        //删除applied_path下左右的文件
        DB_WARNING("last quit is not gracefully");
        boost::filesystem::path applied_path(FLAGS_save_applied_index_path);
        if (boost::filesystem::exists(applied_path)) {
            boost::filesystem::remove_all(applied_path); 
        }

        // rollback all recovered transactions
        for (auto txn : _recovered_txns) {
            DB_WARNING("rollback transaction, txn: %s", txn->GetName().c_str());
            txn->Rollback();
            delete txn;
        }
        _recovered_txns.clear();

        if (0 != _rocksdb->delete_column_family(RocksWrapper::DATA_CF)) {
            DB_WARNING("delete data column_family failed");
            return -1;
        }
        if (0 != _rocksdb->create_column_family(RocksWrapper::DATA_CF)) {
            DB_WARNING("create data column_family failed");
            return -1; 
        }
        DB_WARNING("re-create data column_family success");

        //数据已全部删除，不再需要clear_data
        traverse_region_map([](SmartRegion& region) {
            region->set_need_clear_data(false);
        });
    }
    start_db_statistics();
    DB_WARNING("store init_before_listen success");
    return 0;
}

int Store::init_after_listen(const std::vector<int64_t>& init_region_ids) {
    //重启region
    //开始上报心跳线程
    _heart_beat_bth.run([this]() {report_heart_beat();});
    
    //从本地的rocksdb中恢复该机器上有哪些实例
    for (auto& region_id : init_region_ids) {
        SmartRegion region = get_region(region_id);
        if (region == NULL) {
            DB_FATAL("no region is store, region_id: %ld", region_id);
            continue;
        }
        //region raft node init
        int ret = region->init(false, 0);
        if (ret < 0) {
            //_drop_region_from_rocksdb(region_id);
            //{
            //    std::unique_lock<std::mutex> lock(_region_mutex);
            //    _region_mapping.erase(region_id);
            //}
            DB_FATAL("region init fail when store init, region_id: %ld", region_id);
            continue;
        }
    }
    DB_WARNING("remove FLAGS_save_applied_index_path");
    boost::filesystem::path applied_path(FLAGS_save_applied_index_path);
    if (boost::filesystem::exists(applied_path)) {
        boost::filesystem::remove_all(applied_path);
    }
    
    _split_check_bth.run([this]() {update_and_whether_split_for_region();});
    _merge_bth.run([this]() {reverse_merge_thread();});
    _flush_bth.run([this]() {flush_region_thread();});
    _snapshot_bth.run([this]() {snapshot_thread();});
    _txn_clear_bth.run([this]() {txn_clear_thread();});
    // //计算store状态统计信息的线程
    // _db_status_run = true;
    // _db_status_thread.run([this]() {update_store_status();});

    DB_WARNING("store init_after_listen success, init success");
    return 0;
}

void Store::init_region(google::protobuf::RpcController* controller,
                       const pb::InitRegion* request,
                       pb::StoreRes* response,
                       google::protobuf::Closure* done) {
    TimeCost time_cost;
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl =static_cast<brpc::Controller*>(controller);
    if (!_factory) {
        cntl->SetFailed(EINVAL, "record encoder not set");
        return;
    }
    if (_shutdown) {
        DB_WARNING("store has entered shutdown"); 
        response->set_errcode(pb::INPUT_PARAM_ERROR);
        response->set_errmsg("store has shutdown");
        return;
    }
    uint64_t log_id = 0;
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    const pb::RegionInfo& region_info = request->region_info();
    int64_t table_id = region_info.table_id();
    int64_t region_id = region_info.region_id();
    const char* remote_side = butil::endpoint2str(cntl->remote_side()).c_str();

    //新增table信息
    if (_factory->whether_exist_tableid(table_id) != 0) {
        if (request->has_schema_info()) {
            update_schema_info(request->schema_info());
        } else {
            DB_FATAL("table info missing when add region, table_id:%lu, region_id: %ld, log_id:%lu",
                       table_id, region_info.region_id(), log_id);
            response->set_errcode(pb::INPUT_PARAM_ERROR);
            response->set_errmsg("table info is missing when add region");
            return;
        }
    }
    auto orgin_region = get_region(region_id);
    if (orgin_region != NULL) {
        //自动化处理，直接删除这个region
        DB_FATAL("region id has existed when add region, region_id: %ld, log_id:%lu, remote_side:%s",
                        region_id, log_id,  remote_side);
        response->set_errcode(pb::REGION_ALREADY_EXIST);
        response->set_errmsg("region id has existed and drop fail when init region");
        return;
        //if (_drop_region_from_store(region_id) != 0) {
        //    DB_FATAL("drop region fail when init region, region_id: %ld", region_id);
        //    response->set_errcode(pb::INPUT_PARAM_ERROR);
        //    response->set_errmsg("region id has existed and drop fail when init region");
        //    return;
        //}
    }
    //construct region
    braft::GroupId groupId(std::string("region_") 
                          + boost::lexical_cast<std::string>(region_id));
    butil::EndPoint addr;
    if (str2endpoint(_address.c_str(), &addr) != 0) {
        DB_FATAL("address:%s transfer to endpoint fail", _address.c_str());
        response->set_errcode(pb::INTERNAL_ERROR);
        response->set_errmsg("address is illegal");
        return;
    }
    braft::PeerId peerId(addr, 0);
    SmartRegion region(new(std::nothrow) Region(_rocksdb, 
                                                _factory,
                                                _address,
                                                groupId,
                                                peerId,
                                                request->region_info(), 
                                                region_id));
    if (region == NULL) {
        DB_FATAL("new region fail. mem accolate fail. logid:%lu", log_id);
        response->set_errcode(pb::INTERNAL_ERROR);
        response->set_errmsg("new region fail");
        return; 
    }
    DB_WARNING("new region_info:%s. logid:%lu remote_side: %s", 
            request->ShortDebugString().c_str(), log_id, remote_side);
    //写内存
    set_region(region);
    //region raft node init
    int ret = region->init(true, request->snapshot_times());
    if (ret < 0) {
        _drop_region_from_rocksdb(region_id);
        erase_region(region_id);
        DB_FATAL("region init fail when add region, region_id: %ld, log_id:%lu",
                    region_id, log_id);
        response->set_errcode(pb::INTERNAL_ERROR);
        response->set_errmsg("region init fail when add region");
        return;
    }
    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("add region success");
    if (request->has_split_start() && request->split_start() == true) {
        Bthread bth(&BTHREAD_ATTR_SMALL);
        std::function<void()> check_split_complete_fun = 
            [this, region_id] () { _check_split_complete(region_id);};
        bth.run(check_split_complete_fun);
    }
    DB_WARNING("init region sucess, region_id: %ld, log_id:%lu, time_cost:%ld remote_side: %s",
                region_id, log_id, time_cost.get_time(), remote_side);
}

void Store::region_raft_control(google::protobuf::RpcController* controller,
                    const pb::RaftControlRequest* request,
                    pb::RaftControlResponse* response,
                    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    uint64_t log_id = 0;
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    response->set_region_id(request->region_id());
    SmartRegion region = get_region(request->region_id());
    if (region == NULL) {
        response->set_region_id(request->region_id());
        response->set_errcode(pb::INPUT_PARAM_ERROR);
        response->set_errmsg("region_id not exist in store");
        DB_FATAL("region id:%lu not exist in store, logid:%lu",
                    request->region_id(), log_id);
        return;
    }
    region->raft_control(controller,
                         request,
                         response,
                         done_guard.release());
}

void Store::query(google::protobuf::RpcController* controller,
                  const pb::StoreReq* request,
                  pb::StoreRes* response,
                  google::protobuf::Closure* done) {
    //TRACEPRINTF("recv req region %ld", request->region_id());
    bthread_usleep(20);
    //TRACEPRINTF("recv req region for sleep");
    //DB_NOTICE("%d recv req region_id: %ld", bthread::FLAGS_bthread_concurrency, request->region_id());
    TimeCost cost;
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl =
            static_cast<brpc::Controller*>(controller);
    uint64_t log_id = 0;
    const char* remote_side = butil::endpoint2str(cntl->remote_side()).c_str();
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    SmartRegion region = get_region(request->region_id());
    if (region == NULL) {
        response->set_errcode(pb::REGION_NOT_EXIST);
        response->set_errmsg("region_id not exist in store");
        DB_FATAL("region_id: %ld not exist in store, logid:%lu, remote_side: %s",
                request->region_id(), log_id, remote_side);
        return;
    }
    region->query(controller,
                  request,
                  response,
                  done_guard.release());
    //DB_NOTICE("select region_id: %ld time:%ld", request->region_id(), cost.get_time());
}

void Store::remove_region(google::protobuf::RpcController* controller,
                       const pb::RemoveRegion* request,
                       pb::StoreRes* response,
                       google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl =
            static_cast<brpc::Controller*>(controller);
    DB_WARNING("receive remove region request, remote_side:%s, request:%s",
                butil::endpoint2str(cntl->remote_side()).c_str(),
                request->ShortDebugString().c_str());
    if (_shutdown) {
        DB_WARNING("store has entered shutdown"); 
        response->set_errcode(pb::INPUT_PARAM_ERROR);
        response->set_errmsg("store has shutdown");
        return;
    }
    if (!request->has_force() || request->force() != true) {
            DB_WARNING("drop region fail, input param error, region_id: %ld", request->region_id());
            response->set_errcode(pb::INPUT_PARAM_ERROR);
            response->set_errmsg("input param error");
            return;
    }
    _drop_region_from_store(request->region_id());
    response->set_errcode(pb::SUCCESS);
}
void Store::add_peer(google::protobuf::RpcController* controller,
                       const pb::AddPeer* request,
                       pb::StoreRes* response, 
                       google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("success");
    if (_shutdown) {
        DB_WARNING("store has entered shutdown"); 
        response->set_errcode(pb::INPUT_PARAM_ERROR);
        response->set_errmsg("store has shutdown");
        return;
    }
    SmartRegion region = get_region(request->region_id());
    if (region == NULL) {
        DB_FATAL("region_id: %ld not exist, may be removed", request->region_id());
        response->set_errcode(pb::REGION_NOT_EXIST);
        response->set_errmsg("region not exist");
        return;
    }
    region->add_peer(request, response, done_guard.release());
}
void Store::get_applied_index(google::protobuf::RpcController* controller,
                              const baikaldb::pb::GetAppliedIndex* request,
                              pb::StoreRes* response,
                              google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("success");
    SmartRegion region = get_region(request->region_id());
    if (region == NULL) {
        DB_FATAL("region_id: %ld not exist, may be removed", request->region_id());
        response->set_errcode(pb::REGION_NOT_EXIST);
        response->set_errmsg("region not exist");
        return;
    }
    response->set_applied_index(region->get_log_index());
}
//store上报心跳到meta_server
void Store::report_heart_beat() {
    //static int64_t count = 0;
    while (_is_running) {
        send_heart_beat();
        bthread_usleep(FLAGS_store_heart_beat_interval_us);
    }
}
void Store::send_heart_beat() {
    pb::StoreHeartBeatRequest request;
    pb::StoreHeartBeatResponse response;
    //1、构造心跳请求
    _construct_heart_beat_request(request);
    SELF_TRACE("heart beat request(instance_info):%s, need_leader_balance: %d, need_peer_balance: %d", 
                request.instance_info().ShortDebugString().c_str(), 
                request.need_leader_balance(), 
                request.need_peer_balance());
    std::string str_schema;
    for (auto& schema_info : request.schema_infos()) {
        str_schema += schema_info.ShortDebugString() + ", ";
    }
    SELF_TRACE("heart beat request(schema_infos):%s", str_schema.c_str());
    int count = 0;
    std::string str_leader;
    for (auto& leader_region : request.leader_regions()) {
        str_leader += leader_region.ShortDebugString() + ", ";
        ++count;
        if (count % 10 == 0) {
            SELF_TRACE("heart beat request(leader_regions):%s", str_leader.c_str());
            str_leader.clear();
        }
    }
    if (!str_leader.empty()) {
        SELF_TRACE("heart beat request(leader_regions):%s", str_leader.c_str());
    }
    count = 0;
    std::string str_peer;
    for (auto& peer_info : request.peer_infos()) {
        str_peer += peer_info.ShortDebugString() + ", ";
        ++count;
        if (count % 10 == 0) {
            SELF_TRACE("heart beat request(peer_infos):%s", str_peer.c_str());
            str_peer.clear();
        }
    }
    if (!str_peer.empty()) {
        SELF_TRACE("heart beat request(peer_infos):%s", str_peer.c_str());
    }
    //2、发送请求
    if (_meta_server_interact.send_request("store_heartbeat", request, response) != 0) {
        DB_WARNING("send heart beat request to meta server fail");
    } else {
        //处理心跳
        _process_heart_beat_response(response);
    }
    SELF_TRACE("heart beat response:%s", response.ShortDebugString().c_str());
}
void Store::reverse_merge_thread() {
    while (_is_running) {
        traverse_copy_region_map([](SmartRegion& region) {
            region->reverse_merge();
        });
        bthread_usleep(FLAGS_reverse_merge_interval_us);
    }
}

void Store::flush_region_thread() {
   int64_t sleep_time_count = FLAGS_flush_region_interval_us / 10000; //10ms为单位        
    while (_is_running) {
        int time = 0;
        while (time < sleep_time_count) {
            if (!_is_running) {
                return;
            }
            bthread_usleep(10000);
            ++time;
        }
        rocksdb::FlushOptions flush_options;
        auto status = _rocksdb->flush(flush_options, _region_handle);
        if (!status.ok()) {
            DB_WARNING("flush region handle info to rocksdb fail, err_msg:%s", status.ToString().c_str());
        }
    }
}

void Store::snapshot_thread() {
    BthreadCond concurrency_cond(-5); // -n就是并发跑n个bthread
    while (_is_running) {
        bthread_usleep(10 * 1000 * 1000);
        traverse_copy_region_map([&concurrency_cond](SmartRegion& region) {
            concurrency_cond.increase();
            concurrency_cond.wait();
            SnapshotClosure* done = new SnapshotClosure(concurrency_cond, region.get());
            // 每个region自己会控制是否做snapshot
            region->snapshot(done);
        });
    }
    // 等待全部snapshot都结束
    concurrency_cond.wait(-5);
}

void Store::txn_clear_thread() {
    while (_is_running) {
        bthread_usleep(FLAGS_transaction_clear_interval_ms * 1000);
        traverse_copy_region_map([](SmartRegion& region) {
            // clear prepared and expired transactions
            region->clear_transactions();
        });
    }
}

// void Store::update_store_status() {
//     while (_db_status_run) {
//         std::string data_stat;
//         std::string raft_log_stat;

//         bool ret = false;
//         ret = _rocksdb->get_db()->GetProperty(_rocksdb->get_data_handle(), "rocksdb.stats", &data_stat);
//         if (ret) {
//             DB_WARNING("store data status: %s", data_stat.c_str());
//         }
//         ret = _rocksdb->get_db()->GetProperty(_rocksdb->get_raft_log_handle(), "rocksdb.stats", &raft_log_stat);
//         if (ret) {
//             DB_WARNING("store raftlog status: %s", raft_log_stat.c_str());
//         }
//         bthread_usleep(FLAGS_update_status_interval_us);
//     }
// }
void Store::process_split_request(int64_t table_id, int64_t region_id, bool tail_split, std::string split_key) {
    ++_split_num;
    //构造请求 
    pb::MetaManagerRequest request;
    pb::MetaManagerResponse response;
    request.set_op_type(pb::OP_SPLIT_REGION);
    pb::RegionSplitRequest* region_split = request.mutable_region_split();
    region_split->set_region_id(region_id);
    region_split->set_split_key(split_key);
    region_split->set_new_instance(_address);
    region_split->set_resource_tag(_resource_tag);
    region_split->set_table_id(table_id);
    if (tail_split) {
        region_split->set_tail_split(true);
    }
    //发送请求，收到响应
    if (_meta_server_interact.send_request("meta_manager", request, response) == 0) {
        //处理响应
        SmartRegion region = get_region(region_id);
        if (region == NULL) {
            DB_FATAL("region id:%ld has been deleted", region_id);
            sub_split_num(); 
            return;
        }
        DB_WARNING("store process split request:%s, response:%s", 
                    request.ShortDebugString().c_str(), response.ShortDebugString().c_str());
        region->start_process_split(response.split_response(), tail_split, split_key);
    } else {
        sub_split_num();
        DB_WARNING("store process split request:%s, response:%s", 
                    request.ShortDebugString().c_str(), response.ShortDebugString().c_str());
        DB_FATAL("send split request to metaserver fail");
    }
}

void Store::reset_region_status(int64_t region_id) {
    SmartRegion region = get_region(region_id);
    if (region == NULL) {
        DB_FATAL("region id not existed region_id: %ld", region_id);
        return;
    }
    DB_WARNING("region status was set in store, region_id: %ld", region_id);
    region->reset_region_status();
}

int64_t Store::get_split_index_for_region(int64_t region_id) {
    SmartRegion region = get_region(region_id);
    if (region == NULL) {
        DB_WARNING("region id not existed region_id: %ld", region_id);
        return INT64_MAX;
    }
    return region->get_split_index();
}
void Store::set_can_add_peer_for_region(int64_t region_id) {
    SmartRegion region = get_region(region_id);
    if (region == NULL) {
        DB_FATAL("region id not existed region_id: %ld", region_id);
        return;
    }
    region->set_can_add_peer();
}

// void GetCachePointersFromTableFactory(const rocksdb::TableFactory* factory, 
//         std::unordered_set<const rocksdb::Cache*>& cache_set) {

//     const rocksdb::BlockBasedTableFactory* bbtf 
//         = dynamic_cast<const rocksdb::BlockBasedTableFactory*>(factory);
//     if (bbtf != nullptr) {
//         const auto bbt_opts = bbtf->table_options();
//         cache_set.insert(bbt_opts.block_cache.get());
//         cache_set.insert(bbt_opts.block_cache_compressed.get());
//     }
// }

// void GetCachePointers(const rocksdb::DB* db, std::unordered_set<const rocksdb::Cache*>& cache_set) {
//     cache_set.clear();

//     // Cache from DBImpl
//     rocksdb::StackableDB* sdb = dynamic_cast<rocksdb::StackableDB*>(db);
//     rocksdb::DBImpl* db_impl = dynamic_cast<rocksdb::DBImpl*>(sdb ? sdb->GetBaseDB() : db);
//     if (db_impl != nullptr) {
//         cache_set.insert(db_impl->TEST_table_cache());
//     }

//     // Cache from DBOptions
//     cache_set.insert(db->GetDBOptions().row_cache.get());

//     // Cache from table factories
//     std::unordered_map<std::string, const ImmutableCFOptions*> iopts_map;
//     if (db_impl != nullptr) {
//         db_impl->TEST_GetAllImmutableCFOptions(&iopts_map);
//     }
//     for (auto pair : iopts_map) {
//         GetCachePointersFromTableFactory(pair.second->table_factory, cache_set);
//     }
// }
void Store::print_properties(const std::string& name) {
    auto db = _rocksdb->get_db();
    uint64_t value_data = 0;
    uint64_t value_log = 0;
    db->GetIntProperty(_rocksdb->get_data_handle(), name, &value_data);
    db->GetIntProperty(_rocksdb->get_raft_log_handle(), name, &value_log);
    SELF_TRACE("db_property: %s, data_cf:%lu, log_cf:%lu", name.c_str(), value_data, value_log);
}

void Store::monitor_memory() {
    /*
    std::vector<rocksdb::DB*> dbs;
    std::unordered_set<const rocksdb::Cache*> cache_set;
    std::map<rocksdb::MemoryUtil::UsageType, uint64_t> usage_by_type;
    
    auto db = _rocksdb->get_db();
    dbs.push_back(db);
    // GetCachePointers(db, cache_set);
    // DB_WARNING("cache_set size: %lu", cache_set.size());
    cache_set.insert(_rocksdb->get_cache());
    rocksdb::MemoryUtil::GetApproximateMemoryUsageByType(dbs, cache_set, &usage_by_type);
    for (auto kv : usage_by_type) {
        SELF_TRACE("momery type: %d, size: %lu, %lu", 
            kv.first, kv.second, _rocksdb->get_cache()->GetPinnedUsage());
    }
    */
}

void Store::update_and_whether_split_for_region() {
    static int64_t count = 0;
    while (_is_running) {
        std::vector<int64_t> region_ids;
        traverse_region_map([&region_ids](SmartRegion& region) {
            region_ids.push_back(region->get_region_id());
        });
        boost::scoped_array<int64_t> region_sizes(new(std::nothrow)int64_t[region_ids.size()]);
        int ret = get_used_size_per_region(region_ids, region_sizes.get());
        if (ret != 0) {
            DB_WARNING("get userd size per region fail");
            return; 
        }
        //for (size_t i = 0; i < region_ids.size(); ++i) {
        //    SELF_TRACE("region_id: %ld, region_size:%lu", region_ids[i], region_sizes[i]);
        //}
        for (size_t i = 0; i < region_ids.size(); ++i) {
            SmartRegion ptr_region = get_region(region_ids[i]);
            if (ptr_region == NULL) {
                continue;
            }
            //update region_used_size
            ptr_region->set_used_size(region_sizes[i]);
            
            //判断是否需要分裂，分裂的标准是used_size > 1.5 * region_capacity
            int64_t region_capacity = 0;
            int ret = _factory->get_region_capacity(ptr_region->get_table_id(), region_capacity);
            if (ret != 0) {
                DB_FATAL("table info not exist, region_id: %ld", region_ids[i]);
                continue;
            }
            std::string split_key;
            //如果是尾部分裂
            if (ptr_region->is_leader() 
                    && ptr_region->is_tail() 
                    && (int64_t)region_sizes[i] >= region_capacity
                    && ptr_region->get_status() == pb::IDLE
                    && _split_num.load() < FLAGS_max_split_concurrency) {
                process_split_request(ptr_region->get_table_id(), region_ids[i], true, split_key);
                continue;
            }
            if (ptr_region->is_leader() 
                    && !ptr_region->is_tail() 
                    && (int64_t)region_sizes[i] >= FLAGS_split_threshold * region_capacity / 100
                    && ptr_region->get_status() == pb::IDLE
                    && _split_num.load() < FLAGS_max_split_concurrency) {
                if (0 != ptr_region->get_split_key(split_key)) {
                    DB_WARNING("get_split_key failed: region=%ld", region_ids[i]);
                    continue;
                }
                process_split_request(ptr_region->get_table_id(), region_ids[i], false, split_key);
            }
        }
        SELF_TRACE("upate used size count:%ld", ++count);
        bthread_usleep(FLAGS_update_used_size_interval_us);
    }
}

void Store::start_db_statistics() {
    Bthread bth(&BTHREAD_ATTR_SMALL);
    std::function<void()> dump_options = [this] () {
        while (get_is_running()) {
            TimeCost cost;
            auto db_options = get_db()->get_db_options();
            std::string str = db_options.statistics->ToString();
            std::vector<std::string> items;
            boost::split(items, str, boost::is_any_of("\n"));
            for (auto& item : items) {
                SELF_TRACE("statistics: %s", item.c_str());
            }
            monitor_memory();
            print_properties("rocksdb.num-immutable-mem-table");
            print_properties("rocksdb.mem-table-flush-pending");
            print_properties("rocksdb.compaction-pending");
            print_properties("rocksdb.cur-size-active-mem-table");
            print_properties("rocksdb.cur-size-all-mem-tables");
            print_properties("rocksdb.size-all-mem-tables");
            print_properties("rocksdb.estimate-table-readers-mem");
            print_properties("rocksdb.num-snapshots");
            print_properties("rocksdb.oldest-snapshot-time");
            print_properties("rocksdb.is-write-stopped");
            print_properties("rocksdb.num-live-versions");
            print_properties("rocksdb.estimate-pending-compaction-bytes");
            SELF_TRACE("get properties cost: %ld", cost.get_time());
            bthread_usleep(10 * 1000 * 1000);
        }
     };
    bth.run(dump_options);
}

int Store::get_used_size_per_region(const std::vector<int64_t>& region_ids,
                                                int64_t* region_sizes) {
    for (size_t i = 0; i < region_ids.size(); ++i) {
        auto region = get_region(region_ids[i]);
        int64_t table_id = 0;
        if (region == NULL) {
            DB_WARNING("region_id: %ld not exist", region_ids[i]);
            region_sizes[i] = 0;
            continue;
        } else {
            table_id = region->get_table_id();
        }
        int64_t byte_size_per_record = 1;
        if (_factory->get_byte_size_per_record(table_id, byte_size_per_record) != 0) {
            DB_WARNING("table_id:%ld get byte size per record fail, region_id: %ld",
                    table_id, region_ids[i]);
            region_sizes[i] = 0;
            continue;
        }
        int64_t num_table_lines = region->get_num_table_lines();
        int32_t num_prepared = region->num_prepared();
        int32_t num_began = region->num_began();

        region_sizes[i] = num_table_lines * byte_size_per_record;
        SELF_TRACE("region_id: %ld, num_table_lines:%ld, region_used_size:%ld, "
                    "num_prepared:%d, num_began:%d",
                    region_ids[i], num_table_lines, region_sizes[i], 
                    num_prepared, num_began);
    }

    /*boost::scoped_array<uint64_t> approximate_sizes(new(std::nothrow)uint64_t[region_ids.size()]);
    auto data_cf = _rocksdb->get_data_handle();
    if (nullptr == data_cf) {
        return -1;
    }
    int64_t region_count = region_ids.size();
    boost::scoped_array<rocksdb::Range> ranges(new (std::nothrow)rocksdb::Range[region_count]);
    if (ranges.get() == nullptr) {
        DB_FATAL("update_used_size failed");
        return -1;
    }
    std::vector<std::string> start_keys;
    std::vector<std::string> end_keys;
    for (size_t i = 0; i < region_ids.size(); ++i) {
        MutTableKey start;
        MutTableKey end;
        int64_t table_max = -1;
        start.append_i64(region_ids[i]);
        end.append_i64(region_ids[i]);
        end.append_i64(table_max);
        ranges[i].start = start.data();
        ranges[i].limit =  end.data();
        SELF_TRACE("region_id: %ld, range_start:%s, rang_end:%s", 
                    region_ids[i], rocksdb::Slice(start.data()).ToString(true).c_str(), 
                    rocksdb::Slice(end.data()).ToString(true).c_str());
    }

    for (size_t i = 0; i < region_ids.size(); ++i) {
        ranges[i].start = start_keys[i];
        ranges[i].limit =  end_keys[i];
    }
    _rocksdb->get_db()->GetApproximateSizes(data_cf, ranges.get(), region_count, approximate_sizes.get(), uint8_t(3));
    for (size_t i = 0; i < region_ids.size(); ++i) {
        SELF_TRACE("region_id: %ld, size:%lu", region_ids[i], approximate_sizes[i]);
    }*/
    return 0;
}

void Store::update_schema_info(const pb::SchemaInfo& request) {
    //锁住的是update_table和table_info_mapping, table_info锁的位置不能改
    int ret = _factory->update_table(request);
    if (ret != 0) {
        DB_FATAL("reload schema fail when add table, schema_info:%s", 
                    request.ShortDebugString().c_str());
        return;
    }
}

void Store::_check_split_complete(int64_t region_id) {
    DB_WARNING("start to check whether split complete, region_id: %ld", region_id);
    auto region = get_region(region_id);
    if (region == NULL) {
        DB_WARNING("region_id: %ld not exist", region_id);
        return;
    }
    //检查并且置为失败
    if (region->check_split_complete()) {
        DB_WARNING("split complete. region_id: %ld", region_id); 
    } else {
        DB_FATAL("split not complete, timeout. reigon_id:%ld", region_id);
        _drop_region_from_store(region_id);
    }
}

void Store::_construct_heart_beat_request(pb::StoreHeartBeatRequest& request) {
    static int64_t count = 0;
    request.set_need_leader_balance(false);
    ++count;
    bool need_peer_balance = false;
    if (count % FLAGS_balance_periodicity == 0) {
        request.set_need_leader_balance(true);
    }
    if (count % FLAGS_balance_periodicity == (FLAGS_balance_periodicity / 2)) {
        request.set_need_peer_balance(true);
        need_peer_balance = true;
    }
    //构造instance信息
    pb::InstanceInfo* instance_info = request.mutable_instance_info();
    instance_info->set_address(_address);
    instance_info->set_physical_room(_physical_room);
    instance_info->set_resource_tag(_resource_tag);
    instance_info->set_capacity(FLAGS_capacity);

    //构造schema version信息
    std::unordered_map<int64_t, int64_t> table_id_version_map;
    _factory->get_all_table_version(table_id_version_map);
    for (auto table_info : table_id_version_map) { 
        pb::SchemaHeartBeat* schema = request.add_schema_infos();
        schema->set_table_id(table_info.first);
        schema->set_version(table_info.second);
    }

    int64_t total_used_size = 0;
    //构造所有region的version信息
    traverse_region_map([&total_used_size, &request, need_peer_balance](SmartRegion& region) {
        region->construct_heart_beat_request(request, need_peer_balance);
        total_used_size += region->get_used_size();
    });
    instance_info->set_used_size(total_used_size);
}

void Store::_process_heart_beat_response(const pb::StoreHeartBeatResponse& response) {
    for (auto& schema_info : response.schema_change_info()) {
        update_schema_info(schema_info);
    }
    if (response.schema_change_info_size() > 0) {
        // 只要有表更新就把region中的信息全部更新一遍
        traverse_region_map([](SmartRegion& region) {
                region->update_resource_table();
                });
    }
    for (auto& add_peer_request : response.add_peers()) {
        SmartRegion region = get_region(add_peer_request.region_id());
        if (region == NULL) {
            DB_FATAL("region_id: %ld not exist, may be removed", add_peer_request.region_id());
            continue;
        }
        region->add_peer(add_peer_request);
    }
    std::unordered_map<int64_t, int64_t> table_trans_leader_count;
    for (int i = 0; i < response.trans_leader_table_id_size(); ++i) {
        table_trans_leader_count[response.trans_leader_table_id(i)] = 
            response.trans_leader_count(i);
    }
    for (auto& transfer_leader_request : response.trans_leader()) {
        if (!transfer_leader_request.has_table_id()) {
            SmartRegion region = get_region(transfer_leader_request.region_id()); 
            if (region == NULL) {
                DB_FATAL("region_id: %ld not exist, may be removed", transfer_leader_request.region_id());
                continue;
            }
            region->transfer_leader(transfer_leader_request); 
        }
    }
    for (auto& transfer_leader_request : response.trans_leader()) {
        if (!transfer_leader_request.has_table_id()) {
            continue;
        }
        int64_t table_id = transfer_leader_request.table_id();
        if (table_trans_leader_count[table_id] <= 0) {
            continue;
        }
        SmartRegion region = get_region(transfer_leader_request.region_id());
        if (region == NULL) {
            DB_FATAL("region_id: %ld not exist, may be removed", transfer_leader_request.region_id());
            continue;
        }
        auto ret = region->transfer_leader(transfer_leader_request);
        if (ret == 0) {
            table_trans_leader_count[table_id]--;
        }
    }
    //删除region数据，该region已不在raft组内
    for (auto& delete_region_id : response.delete_region_ids()) {
        DB_WARNING("receive delete region response from meta server heart beat, delete_region_id:%ld",
                   delete_region_id);
        _drop_region_from_store(delete_region_id);        
    }
}

int Store::_drop_region_from_store(int64_t drop_region_id) {
    if (_drop_region_from_rocksdb(drop_region_id) != 0) {
        DB_FATAL("drop region from rocksdb fail, region_id: %ld", drop_region_id);
        return -1;
    }
    DB_WARNING("drop region from store, region_id: %ld", drop_region_id);
    SmartRegion region = get_region(drop_region_id);
    if (region == NULL) {
        DB_FATAL("region_id: %ld not exist, may be removed", drop_region_id);
        return -1;
    }
    pb::RegionInfo region_info;
    region->set_removed(true);
    region->get_region_info(region_info);
    region_info.set_deleted(true);
    _factory->update_region(region_info);
    region->shutdown();
    region->join();
    DB_WARNING("region node close, region_id: %ld", drop_region_id);
    if (region->clear_data() != 0) {
        DB_FATAL("region_id: %ld clear data faile", drop_region_id);
        return -1;
    }
    DB_WARNING("region clear data, region_id: %ld", drop_region_id);
    //删除snapshot目录
    _remove_snapshot_path(drop_region_id);
    //删除log_entry
    if (_remove_log_entry(drop_region_id) == 0) {
        DB_WARNING("region remove log entry, region_id: %ld", drop_region_id);
        erase_region(drop_region_id);
        return 0; 
    } else {
        return -1;
    }
}
int Store::_remove_log_entry(int64_t drop_region_id) {
    MutTableKey log_meta_key;
    log_meta_key.append_i64(drop_region_id).append_u8((uint8_t)MyRaftLogStorage::LOG_META_IDENTIFY);
    rocksdb::WriteOptions options;
    auto status = _rocksdb->remove(options, 
                                   _rocksdb->get_raft_log_handle(), 
                                   log_meta_key.data());
    if (!status.ok()) {
        DB_WARNING("remove log meta key error: code=%d, msg=%s, region_id: %ld",
            status.code(), status.ToString().c_str(), drop_region_id);
        return -1;
    }
    DB_WARNING("region remove snapshot path, region_id: %ld", drop_region_id);
    MutTableKey log_data_key_start;
    MutTableKey log_data_key_end;
    log_data_key_start.append_i64(drop_region_id);
    log_data_key_start.append_u8((uint8_t)MyRaftLogStorage::LOG_DATA_IDENTIFY);
    log_data_key_start.append_i64(0);

    log_data_key_end.append_i64(drop_region_id);
    log_data_key_end.append_u8((uint8_t)MyRaftLogStorage::LOG_DATA_IDENTIFY);
    log_data_key_end.append_u64(0xFFFFFFFFFFFFFFFF);
    status = _rocksdb->remove_range(options, 
                                    _rocksdb->get_raft_log_handle(),
                                    log_data_key_start.data(), 
                                    log_data_key_end.data());
    if (!status.ok()) {
        DB_WARNING("remove_range error: code=%d, msg=%s, region_id: %ld",
            status.code(), status.ToString().c_str(), drop_region_id);
        return -1;
    }
    return 0;
}

void Store::_remove_snapshot_path(int64_t drop_region_id) {
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
    }
    DB_WARNING("drop snapshot directorey, region_id: %ld", drop_region_id);
}

void Store::_remove_region_data(int64_t drop_region_id) {
    rocksdb::WriteOptions options;

    MutTableKey start_key;
    MutTableKey end_key;
    start_key.append_i64(drop_region_id);

    end_key.append_i64(drop_region_id);
    end_key.append_u64(0xFFFFFFFFFFFFFFFF);
    auto data_cf = _rocksdb->get_data_handle();

    if (data_cf == nullptr) {
        DB_WARNING("get rocksdb data column family failed, region_id: %ld", drop_region_id);
        return;
    }
    TimeCost cost;
    auto res = _rocksdb->remove_range(options, data_cf, 
            start_key.data(), end_key.data());
    if (!res.ok()) {
        DB_WARNING("remove_range error: code=%d, msg=%s, region_id: %ld", 
            res.code(), res.ToString().c_str(), drop_region_id);
        return;
    }
    DB_WARNING("remove_range cost:%ld, region_id: %ld", cost.get_time(), drop_region_id);
}

int Store::_drop_region_from_rocksdb(int64_t region_id) {
    std::string region_key = SCHEMA_IDENTIFY;
    region_key += REGION_SCHEMA_IDENTIFY;
    region_key.append((char*)&region_id, sizeof(int64_t));
    rocksdb::WriteBatch batch;
    rocksdb::WriteOptions options;
    //options.sync = true;
    //options.disableWAL = true;
    batch.Delete(_region_handle, region_key);
    auto status = _rocksdb->write(options, &batch);
    if (!status.ok()) {
        DB_WARNING("drop region fail, region_id: %ld", region_id);
        return -1;
    }
    //rocksdb::FlushOptions flush_options;
    //status = _rocksdb->flush(flush_options, _region_handle);
    //if (!status.ok()) {
    //    DB_WARNING("drop region_id: %ld info to rocksdb fail when flush, err_msg:%s",
    //                region_id, status.ToString().c_str());
    //    return -1;
    //}

    return 0;
}

} //namespace
