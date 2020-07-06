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

#include <store.h>
#include <boost/filesystem.hpp>
#include <sys/vfs.h>
#include <boost/lexical_cast.hpp>
#include <boost/scoped_array.hpp>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <gflags/gflags.h>
#include "rocksdb/utilities/memory_util.h"
#include "mut_table_key.h"
#include "closure.h"
#include "my_raft_log_storage.h"
#include "log_entry_reader.h"
#include "rocksdb/cache.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "concurrency.h"
#include "mut_table_key.h"
#include "my_raft_log_storage.h"

namespace baikaldb {
DECLARE_int64(store_heart_beat_interval_us);
DECLARE_int32(balance_periodicity);
DECLARE_string(stable_uri);
DECLARE_string(snapshot_uri);
DEFINE_int64(reverse_merge_interval_us, 2 * 1000 * 1000,  "reverse_merge_interval(2 s)");
DEFINE_int64(ttl_remove_interval_s, 24 * 3600,  "ttl_remove_interval_s(24h)");
DEFINE_int64(delay_remove_region_interval_s, 600,  "delay_remove_region_interval");
//DEFINE_int32(update_status_interval_us, 2 * 1000 * 1000,  "update_status_interval(2 s)");
DEFINE_int32(store_port, 8110, "Server port");
DEFINE_string(db_path, "./rocks_db", "rocksdb path");
DEFINE_string(resource_tag, "", "resource tag");
DEFINE_int32(update_used_size_interval_us, 10 * 1000 * 1000, "update used size interval (10 s)");
DEFINE_int32(init_region_concurrency, 10, "init region concurrency when start");
DEFINE_int32(split_threshold , 150, "split_threshold, defalut: 150% * region_size / 100");
DEFINE_int64(min_split_lines, 200000, "min_split_lines, protected when wrong param put in table");
DEFINE_int64(flush_region_interval_us, 10 * 60 * 1000 * 1000LL, 
            "flush region interval, defalut(10 min)");
DEFINE_int64(transaction_clear_interval_ms, 5000LL,
            "transaction clear interval, defalut(5s)");
DECLARE_int64(flush_memtable_interval_us);
DEFINE_int32(max_split_concurrency, 2, "max split region concurrency, default:2");
DEFINE_int64(none_region_merge_interval_us, 5 * 60 * 1000 * 1000LL, 
             "none region merge interval, defalut(5 min)");
DEFINE_int64(region_delay_remove_timeout_s, 3600 * 24LL, 
             "region_delay_remove_time_s, defalut(1d)");
Store::~Store() {}

int Store::init_before_listen(std::vector<std::int64_t>& init_region_ids) {
    butil::EndPoint addr;
    addr.ip = butil::my_ip();
    addr.port = FLAGS_store_port; 
    _address = endpoint2str(addr).c_str(); 
    if (_meta_server_interact.init() != 0) {
        DB_FATAL("meta server interact init fail");
        return -1;
    }

    int ret = get_physical_room(_address, _physical_room);
    if (ret < 0) {
        DB_FATAL("get physical room fail");
        return -1;
    }
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
    // init val 
    _factory = SchemaFactory::get_instance();
    std::vector<rocksdb::Transaction*> recovered_txns;
    _rocksdb->get_db()->GetAllPreparedTransactions(&recovered_txns);
    if (recovered_txns.empty()) {
        DB_WARNING("has no prepared transcation");
        _has_prepared_tran = false;
    }
    for (auto txn : recovered_txns) {
        std::string txn_name = txn->GetName().c_str();
        std::vector<std::string> split_vec;
        boost::split(split_vec, txn_name, boost::is_any_of("_"));
        int64_t region_id = boost::lexical_cast<int64_t>(split_vec[0]);
        uint64_t txn_id = boost::lexical_cast<uint64_t>(split_vec[1]);
        prepared_txns[region_id].insert(txn_id);
        DB_WARNING("rollback transaction, txn: %s, region_id: %ld, txn_id: %lu", 
                    txn->GetName().c_str(), region_id, txn_id);
        txn->Rollback();
        delete txn;
    }
    recovered_txns.clear();

    _meta_writer = MetaWriter::get_instance();
    _meta_writer->init(_rocksdb, _rocksdb->get_meta_info_handle());
    
    LogEntryReader* reader = LogEntryReader::get_instance();
    reader->init(_rocksdb, _rocksdb->get_raft_log_handle());

    pb::StoreHeartBeatRequest request;
    pb::StoreHeartBeatResponse response;
    //1、构造心跳请求, 重启时的心跳包除了实例信息外，其他都为空
    construct_heart_beat_request(request);
    DB_WARNING("heart beat request:%s when init store", request.ShortDebugString().c_str());
    TimeCost step_time_cost;
    //2、发送请求
    if (_meta_server_interact.send_request("store_heartbeat", request, response) == 0) {
        DB_WARNING("heart beat response:%s when init store", response.ShortDebugString().c_str());
        //同步处理心跳, 重启拉到的第一个心跳包只有schema信息
        _factory->update_tables_double_buffer_sync(response.schema_change_info());
    } else {
        DB_FATAL("send heart beat request to meta server fail");
        return -1;
    }
    int64_t heartbeat_process_time = step_time_cost.get_time();
    step_time_cost.reset();
    DB_WARNING("get schema info from meta server success");

    //系统重启之前有哪些reigon
    std::vector<pb::RegionInfo> region_infos;
    ret = _meta_writer->parse_region_infos(region_infos);
    if (ret < 0) {
        DB_FATAL("read region_infos from rocksdb fail");
        return ret;
    }
    for (auto& region_info : region_infos) {
        DB_WARNING("region_info:%s when init store", region_info.ShortDebugString().c_str());
        int64_t region_id = region_info.region_id();
        if (region_info.version() == 0) {
            DB_WARNING("region_id: %ld version is 0, dropped. region_info: %s",
                    region_id, region_info.ShortDebugString().c_str() );
            RegionControl::clear_all_infos_for_region(region_id);
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
    }
    int64_t new_region_process_time = step_time_cost.get_time();
    //重启的region跟新建的region或者正常运行情况下的region有两点区别
    //1、重启region的on_snapshot_load不受并发数的限制
    //2、重启region的on_snapshot_load不加载sst文件
    traverse_region_map([](SmartRegion& region) {
        region->set_restart(true);
    });
    ret = _meta_writer->parse_doing_snapshot(doing_snapshot_regions);
    if (ret < 0) {
        DB_FATAL("read doing snapshot regions from rocksdb fail");
        return ret;
    } else {
        for (auto region_id : doing_snapshot_regions) {
            DB_WARNING("region_id: %ld is doing snapshot load when store stop", region_id);
        }
    }

    start_db_statistics();
    DB_WARNING("store init_before_listen success heartbeat_process_time:%ld new_region_process_time:%ld",
           heartbeat_process_time, new_region_process_time);
    return 0;
}

int Store::init_after_listen(const std::vector<int64_t>& init_region_ids) {
    //开始上报心跳线程
    _heart_beat_bth.run([this]() {heart_beat_thread();});
    TimeCost step_time_cost; 
    ConcurrencyBthread init_bth(FLAGS_init_region_concurrency);
    //从本地的rocksdb中恢复该机器上有哪些实例
    for (auto& region_id : init_region_ids) {
        auto init_call = [this, region_id]() {
            SmartRegion region = get_region(region_id);
            if (region == NULL) {
                DB_FATAL("no region is store, region_id: %ld", region_id);
                return;
            }
            //region raft node init
            int ret = region->init(false, 0);
            if (ret < 0) {
                DB_FATAL("region init fail when store init, region_id: %ld", region_id);
                return;
            }
        };
        init_bth.run(init_call);
    }
    init_bth.join();
    
    _split_check_bth.run([this]() {whether_split_thread();});
    _merge_bth.run([this]() {reverse_merge_thread();});
    _ttl_bth.run([this]() {ttl_remove_thread();});
    _delay_remove_region_bth.run([this]() {delay_remove_region_thread();});
    _flush_bth.run([this]() {flush_memtable_thread();});
    _snapshot_bth.run([this]() {snapshot_thread();});
    _txn_clear_bth.run([this]() {txn_clear_thread();});
    _has_prepared_tran = true;
    prepared_txns.clear();
    doing_snapshot_regions.clear();
    DB_WARNING("store init_after_listen success, init success init_region_time:%ld", step_time_cost.get_time());
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
    const auto& remote_side_tmp = butil::endpoint2str(cntl->remote_side());
    const char* remote_side = remote_side_tmp.c_str();

    //新增table信息
    if (!_factory->exist_tableid(table_id)) {
        if (request->has_schema_info()) {
            update_schema_info(request->schema_info());
            bthread_usleep(200 * 1000); // 延迟更新了
        } else {
            DB_FATAL("table info missing when add region, table_id:%lu, region_id: %ld, log_id:%lu",
                       table_id, region_info.region_id(), log_id);
            response->set_errcode(pb::INPUT_PARAM_ERROR);
            response->set_errmsg("table info is missing when add region");
            return;
        }
    }
    auto orgin_region = get_region(region_id);
    // 已经软删，遇到需要新建就马上删除让位
    if (orgin_region != nullptr && orgin_region->removed()) {
        drop_region_from_store(region_id, false);
    }
    orgin_region = get_region(region_id);
    if (orgin_region != nullptr) {
        //自动化处理，直接删除这个region
        DB_FATAL("region id has existed when add region, region_id: %ld, log_id:%lu, remote_side:%s",
                region_id, log_id,  remote_side);
        response->set_errcode(pb::REGION_ALREADY_EXIST);
        response->set_errmsg("region id has existed and drop fail when init region");
        return;
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
    Concurrency::get_instance()->init_region_concurrency.increase_wait();
    int ret = region->init(true, request->snapshot_times());
    Concurrency::get_instance()->init_region_concurrency.decrease_broadcast();
    if (ret < 0) {
        //删除该region相关的全部信息
        RegionControl::clear_all_infos_for_region(region_id);
        erase_region(region_id);
        DB_FATAL("region init fail when add region, region_id: %ld, log_id:%lu",
                    region_id, log_id);
        response->set_errcode(pb::INTERNAL_ERROR);
        response->set_errmsg("region init fail when add region");
        return;
    }
    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("add region success");
    if (request->region_info().version() == 0) {
        Bthread bth(&BTHREAD_ATTR_SMALL);
        std::function<void()> check_region_legal_fun = 
            [this, region_id] () { check_region_legal_complete(region_id);};
        bth.run(check_region_legal_fun);
        DB_WARNING("init region verison is 0, should check region legal. region_id: %ld, log_id: %lu", 
                    region_id, log_id);
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
    bthread_usleep(20);
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl =
            static_cast<brpc::Controller*>(controller);
    uint64_t log_id = 0;
    const char* remote_side = butil::endpoint2str(cntl->remote_side()).c_str();
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    //DB_WARNING("region_id: %ld before get_region, logid:%lu, remote_side: %s", request->region_id(), log_id, remote_side);
    SmartRegion region = get_region(request->region_id());
    if (region == nullptr || region->removed()) {
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
    DB_WARNING("call remove region_id: %ld, need_delay_drop:%d", 
            request->region_id(), request->need_delay_drop());
    drop_region_from_store(request->region_id(), request->need_delay_drop());
    response->set_errcode(pb::SUCCESS);
}

void Store::restore_region(google::protobuf::RpcController* controller,
                              const baikaldb::pb::RegionIds* request,
                              pb::StoreRes* response,
                              google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("success");
    TimeCost cost;
    auto call = [](SmartRegion region) {
        // 需要等table info才能init成功
        region->wait_table_info();
        // 恢复相当于重启region
        region->set_restart(true);
        region->init(false, 0);
        region->set_removed(false);
        DB_WARNING("restore region_id: %ld success ", region->get_region_id());
    };
    if (request->region_ids_size() == 0) {
        traverse_copy_region_map([request, call](SmartRegion& region) {
            if (region->removed()) {
                if (!request->has_table_id()) {
                    call(region);
                } else if (request->table_id() == region->get_table_id()) {
                    call(region);
                }
            }
        });
    } else {
        for (auto region_id : request->region_ids()) {
            auto region = get_region(region_id);
            if (region != nullptr && region->removed()) {
                call(region);
            }
        }
    }
    DB_WARNING("restore region success cost:%ld", cost.get_time());
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
    if (region == nullptr || region->removed()) {
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
    if (region == nullptr) {
        DB_FATAL("region_id: %ld not exist, may be removed", request->region_id());
        response->set_errcode(pb::REGION_NOT_EXIST);
        response->set_errmsg("region not exist");
        return;
    }
    response->set_applied_index(region->get_log_index());
    response->set_leader(butil::endpoint2str(region->get_leader()).c_str());
}

void Store::compact_region(google::protobuf::RpcController* controller,
                              const baikaldb::pb::RegionIds* request,
                              pb::StoreRes* response,
                              google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("success");
    TimeCost cost;
    if (request->region_ids_size() == 0) {
        auto cf = _rocksdb->get_data_handle();
        if (request->compact_raft_log()) {
            cf = _rocksdb->get_raft_log_handle();
        } else if (request->has_compact_type()) {
            auto type = request->compact_type();
            if (type == 1) { // data_cf
                cf = _rocksdb->get_data_handle();
            } else if (type == 2) { // meta_cf
                cf = _rocksdb->get_meta_info_handle();
            } else { // raft_log_cf
                cf = _rocksdb->get_raft_log_handle();
            }
        }
        rocksdb::CompactRangeOptions compact_options;
        compact_options.exclusive_manual_compaction = false;
        auto res = _rocksdb->compact_range(compact_options, cf, nullptr, nullptr);
        if (!res.ok()) {
            DB_WARNING("compact_range error: code=%d, msg=%s", 
                    res.code(), res.ToString().c_str());
        }
    } else {
        for (auto region_id : request->region_ids()) {
            RegionControl::compact_data(region_id);
        }
    }
    DB_WARNING("compact_db cost:%ld", cost.get_time());
}

void Store::snapshot_region(google::protobuf::RpcController* controller,
                            const baikaldb::pb::RegionIds* request,
                            pb::StoreRes* response,
                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("success");
    std::vector<int64_t> region_ids;
    if (request->region_ids_size() == 0) {
        traverse_region_map([&region_ids](SmartRegion& region) {
            region_ids.push_back(region->get_region_id());
        });
    } else {
        for (auto region_id : request->region_ids()) {
            region_ids.push_back(region_id);
        }
    }
    auto snapshot_fun = [this, region_ids]() {
        for (auto& region_id: region_ids) {
            SmartRegion region = get_region(region_id);
            if (region == nullptr) {
                DB_FATAL("region_id: %ld not exist, may be removed", region_id);
            } else {
                region->do_snapshot();
            }
        }
        DB_WARNING("all region sync_do_snapshot finish");
    };
    Bthread bth(&BTHREAD_ATTR_SMALL);
    bth.run(snapshot_fun);
}

void Store::query_region(google::protobuf::RpcController* controller,
                            const baikaldb::pb::RegionIds* request,
                            pb::StoreRes* response,
                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("success");
    std::map<int64_t, std::string> id_leader_map;
    response->set_leader(_address);
    if (request->region_ids_size() == 0) {
        traverse_region_map([&id_leader_map](SmartRegion& region) {
            id_leader_map[region->get_region_id()] = 
                butil::endpoint2str(region->get_leader()).c_str();
        });
        for (auto& id_pair : id_leader_map) {
            auto ptr_region_leader = response->add_region_leaders();
            ptr_region_leader->set_region_id(id_pair.first);
            ptr_region_leader->set_leader(id_pair.second);
        }
        response->set_region_count(response->region_leaders_size());
        return;
    }

    for (auto region_id : request->region_ids()) {
        SmartRegion region = get_region(region_id);
        if (region == nullptr) {
            DB_FATAL("region_id: %ld not exist, may be removed", region_id);
        } else {
            auto ptr_region_info = response->add_regions();
            region->copy_region(ptr_region_info);
            ptr_region_info->set_leader(butil::endpoint2str(region->get_leader()).c_str());
            ptr_region_info->set_log_index(region->get_log_index());
        }
    }
    response->set_region_count(response->regions_size());
}
void Store::query_illegal_region(google::protobuf::RpcController* controller,
                            const baikaldb::pb::RegionIds* request,
                            pb::StoreRes* response,
                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("success");
    std::map<int64_t, std::string> id_leader_map;
    response->set_leader(_address);
    if (request->region_ids_size() == 0) {
        traverse_region_map([&id_leader_map](SmartRegion& region) {
            if (region->get_leader().ip == butil::IP_ANY) {
                id_leader_map[region->get_region_id()] = 
                    butil::endpoint2str(region->get_leader()).c_str();
            }
        });
        for (auto& id_pair : id_leader_map) {
            auto ptr_region_leader = response->add_region_leaders();
            ptr_region_leader->set_region_id(id_pair.first);
            ptr_region_leader->set_leader(id_pair.second);
        }
        response->set_region_count(response->region_leaders_size());
        return;
    }

    for (auto region_id : request->region_ids()) {
        SmartRegion region = get_region(region_id);
        if (region == nullptr) {
            DB_FATAL("region_id: %ld not exist, may be removed", region_id);
        } else {
            if (region->get_leader().ip == butil::IP_ANY) {
                auto ptr_region_info = response->add_regions();
                region->copy_region(ptr_region_info);
                ptr_region_info->set_leader(butil::endpoint2str(region->get_leader()).c_str());
            }
        }
    }
    response->set_region_count(response->regions_size());
}
//store上报心跳到meta_server
void Store::heart_beat_thread() {
    //static int64_t count = 0;
    while (!_shutdown) {
        send_heart_beat();
        bthread_usleep_fast_shutdown(FLAGS_store_heart_beat_interval_us, _shutdown);
    }
}

void Store::send_heart_beat() {
    pb::StoreHeartBeatRequest request;
    pb::StoreHeartBeatResponse response;
    //1、构造心跳请求
    construct_heart_beat_request(request);
    print_heartbeat_info(request);
    //2、发送请求
    if (_meta_server_interact.send_request("store_heartbeat", request, response) != 0) {
        DB_WARNING("send heart beat request to meta server fail");
    } else {
        //处理心跳
        process_heart_beat_response(response);
    }
    DB_WARNING("heart beat");
}

void Store::reverse_merge_thread() {
    while (!_shutdown) {
        TimeCost cost;
        traverse_copy_region_map([](SmartRegion& region) {
            region->reverse_merge();
        });
        DB_WARNING("all merge cost: %ld", cost.get_time());
        bthread_usleep_fast_shutdown(FLAGS_reverse_merge_interval_us, _shutdown);
    }
}

void Store::ttl_remove_thread() {
    while (!_shutdown) {
        bthread_usleep_fast_shutdown(FLAGS_ttl_remove_interval_s * 1000 * 1000, _shutdown);
        if (_shutdown) {
            return;
        }
        traverse_copy_region_map([](SmartRegion& region) {
            region->ttl_remove_expired_data();
        });
    }
}

void Store::delay_remove_region_thread() {
    while (!_shutdown) {
        bthread_usleep_fast_shutdown(FLAGS_delay_remove_region_interval_s * 1000 * 1000, _shutdown);
        if (_shutdown) {
            return;
        }
        traverse_copy_region_map([this](SmartRegion& region) {
            if (region->removed() && 
                region->removed_time_cost() > FLAGS_region_delay_remove_timeout_s * 1000 * 1000LL) {
                region->shutdown();
                region->join();
                int64_t drop_region_id = region->get_region_id();
                DB_WARNING("region node remove permanently, region_id: %ld", drop_region_id);
                RegionControl::clear_all_infos_for_region(drop_region_id);
                erase_region(drop_region_id);
            }
        });
    }
}

void Store::flush_memtable_thread() {
    while (!_shutdown) {
        bthread_usleep_fast_shutdown(FLAGS_flush_memtable_interval_us, _shutdown);
        if (_shutdown) {
            return;
        }
        rocksdb::FlushOptions flush_options;
        auto status = _rocksdb->flush(flush_options, _rocksdb->get_meta_info_handle());
        if (!status.ok()) {
            DB_WARNING("flush meta info to rocksdb fail, err_msg:%s", status.ToString().c_str());
        }
        status = _rocksdb->flush(flush_options, _rocksdb->get_data_handle());
        if (!status.ok()) {
            DB_WARNING("flush data to rocksdb fail, err_msg:%s", status.ToString().c_str());
        }
        status = _rocksdb->flush(flush_options, _rocksdb->get_raft_log_handle());
        if (!status.ok()) {
            DB_WARNING("flush log_cf to rocksdb fail, err_msg:%s", status.ToString().c_str());
        }
    }
}

void Store::snapshot_thread() {
    BthreadCond concurrency_cond(-5); // -n就是并发跑n个bthread
    while (!_shutdown) {
        traverse_copy_region_map([&concurrency_cond](SmartRegion& region) {
            concurrency_cond.increase_wait();
            SnapshotClosure* done = new SnapshotClosure(concurrency_cond, region.get());
            // 每个region自己会控制是否做snapshot
            region->snapshot(done);
        });
        bthread_usleep_fast_shutdown(10 * 1000 * 1000, _shutdown);
    }
    // 等待全部snapshot都结束
    concurrency_cond.wait(-5);
}

void Store::txn_clear_thread() {
    while (!_shutdown) {
        traverse_copy_region_map([](SmartRegion& region) {
            // clear prepared and expired transactions
            region->clear_transactions();
        });
        bthread_usleep_fast_shutdown(FLAGS_transaction_clear_interval_ms * 1000, _shutdown);

    }
}

void Store::process_merge_request(int64_t table_id, int64_t region_id) {
    //请求meta查询空region的下一个region
    //构造请求
    pb::MetaManagerRequest request;
    pb::MetaManagerResponse response;
    SmartRegion ptr_region = get_region(region_id);
    if (ptr_region == NULL) {
        return;
    }
    request.set_op_type(pb::OP_MERGE_REGION);
    pb::RegionMergeRequest* region_merge = request.mutable_region_merge();
    region_merge->set_src_region_id(region_id);
    region_merge->set_src_start_key(ptr_region->get_start_key());
    region_merge->set_src_end_key(ptr_region->get_end_key());
    region_merge->set_table_id(table_id);
    //发送请求，收到响应
    if (_meta_server_interact.send_request("meta_manager", request, response) == 0) {
        //处理响应
        if (response.errcode() != pb::SUCCESS) {
            DB_FATAL("store process merge fail request:%s, response:%s", 
                       request.ShortDebugString().c_str(), 
                       response.ShortDebugString().c_str());            
            return;
        }
        SmartRegion region = get_region(region_id);
        if (region == NULL) {
            DB_FATAL("region id:%ld has been deleted", region_id);
            return;
        }
        DB_WARNING("store process merge request:%s, response:%s", 
                   request.ShortDebugString().c_str(), response.ShortDebugString().c_str());
        region->start_process_merge(response.merge_response());
    } else {
        DB_WARNING("store process merge request:%s, response:%s", 
                   request.ShortDebugString().c_str(), response.ShortDebugString().c_str());
        DB_FATAL("send merge request to metaserver fail");
    }
}

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

void Store::whether_split_thread() {
    static int64_t count = 0;
    while (!_shutdown) {
        std::vector<int64_t> region_ids;
        traverse_region_map([&region_ids](SmartRegion& region) {
            region_ids.push_back(region->get_region_id());
        });
        boost::scoped_array<uint64_t> region_sizes(new(std::nothrow)uint64_t[region_ids.size()]);
        boost::scoped_array<int64_t> region_num_lines(new(std::nothrow)int64_t[region_ids.size()]);
        int ret = get_used_size_per_region(region_ids, region_sizes.get(), region_num_lines.get());
        if (ret != 0) {
            DB_WARNING("get used size per region fail");
            return; 
        }
        for (size_t i = 0; i < region_ids.size(); ++i) {
            SmartRegion ptr_region = get_region(region_ids[i]);
            if (ptr_region == NULL) {
                continue;
            }
            if (ptr_region->removed()) {
                DB_WARNING("region_id: %ld has be removed", region_ids[i]);
                continue;
            }
            //设置计算存储分离开关
            ptr_region->set_separate_switch(_factory->get_separate_switch(ptr_region->get_table_id()));
            //update region_used_size
            ptr_region->set_used_size(region_sizes[i]);
            
            //判断是否需要分裂，分裂的标准是used_size > 1.5 * region_capacity
            int64_t region_capacity = 10000000;
            int ret = _factory->get_region_capacity(ptr_region->get_global_index_id(), region_capacity);
            if (ret != 0) {
                DB_FATAL("table info not exist, region_id: %ld", region_ids[i]);
                continue;
            }
            region_capacity = std::max(FLAGS_min_split_lines, region_capacity);
            //DB_WARNING("region_id: %ld, split_capacity: %ld", region_ids[i], region_capacity);
            std::string split_key;
            //如果是尾部分
            if (ptr_region->is_leader() 
                    && ptr_region->is_tail() 
                    && region_num_lines[i] >= region_capacity
                    && ptr_region->get_status() == pb::IDLE
                    && _split_num.load() < FLAGS_max_split_concurrency) {
                process_split_request(ptr_region->get_global_index_id(), region_ids[i], true, split_key);
                continue;
            }
            if (ptr_region->is_leader() 
                    && !ptr_region->is_tail() 
                    && region_num_lines[i] >= FLAGS_split_threshold * region_capacity / 100
                    && ptr_region->get_status() == pb::IDLE
                    && _split_num.load() < FLAGS_max_split_concurrency) {
                if (0 != ptr_region->get_split_key(split_key)) {
                    DB_WARNING("get_split_key failed: region=%ld", region_ids[i]);
                    continue;
                }
                process_split_request(ptr_region->get_global_index_id(), region_ids[i], false, split_key);
                continue;
            }
            
            if (!_factory->get_merge_switch(ptr_region->get_table_id())) {
                continue;
            }
            //简化特殊处理，首尾region不merge
            if (ptr_region->is_tail() || ptr_region->is_head()) {
                continue;
            }
            
            //空region超过两个心跳周期之后触发删除，主从均执行
            if (ptr_region->empty()) {
                if (ptr_region->get_status() == pb::IDLE
                    && ptr_region->get_timecost() > FLAGS_store_heart_beat_interval_us * 2) {
                    //删除region
                    DB_WARNING("region:%ld has been merged, drop it", region_ids[i]);
                    //空region通过心跳上报，由meta触发删除，不在此进行
                    //drop_region_from_store(region_ids[i]);
                }
                continue;
            }
                   
            //region无数据，超过5min触发回收
            if (ptr_region->is_leader()
                    && region_num_lines[i] == 0
                    && ptr_region->get_status() == pb::IDLE) {
                if (ptr_region->get_log_index() != ptr_region->get_log_index_lastcycle()) {
                    ptr_region->reset_log_index_lastcycle();
                    DB_WARNING("region:%ld is none, log_index:%ld reset time", 
                               region_ids[i], ptr_region->get_log_index());
                    continue;
                }
                if (ptr_region->get_log_index() == ptr_region->get_log_index_lastcycle()
                   && ptr_region->get_lastcycle_timecost() > FLAGS_none_region_merge_interval_us) {
                    DB_WARNING("region:%ld is none, log_index:%ld, process merge", 
                              region_ids[i], ptr_region->get_log_index());
                    process_merge_request(ptr_region->get_global_index_id(), region_ids[i]);
                    continue;
                }
            }
        }
        SELF_TRACE("upate used size count:%ld", ++count);
        bthread_usleep_fast_shutdown(FLAGS_update_used_size_interval_us, _shutdown);
    }
}

void Store::start_db_statistics() {
    Bthread bth(&BTHREAD_ATTR_SMALL);
    std::function<void()> dump_options = [this] () {
        while (!_shutdown) {
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
                                                uint64_t* region_sizes, int64_t* region_num_lines) {
    for (size_t i = 0; i < region_ids.size(); ++i) {
        auto region = get_region(region_ids[i]);
        if (region == NULL) {
            DB_WARNING("region_id: %ld not exist", region_ids[i]);
            region_sizes[i] = 0;
            continue;
        }
        int64_t num_table_lines = region->get_num_table_lines();
        int32_t num_prepared = region->num_prepared();
        int32_t num_began = region->num_began();

        //region_sizes[i] = num_table_lines * byte_size_per_record;
        //rocksdb 不准 TODO:获取更准确的信息
        region_sizes[i] = num_table_lines * 50;
        region_num_lines[i] = num_table_lines;
        //DB_NOTICE("region_id: %ld, num_table_lines:%ld, region_used_size:%lu, "
        //            "num_prepared:%d, num_began:%d",
        //            region_ids[i], num_table_lines, region_sizes[i], 
        //            num_prepared, num_began);
    }

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
    //GetApproximateSizes 得不到准确的数
    //_rocksdb->get_db()->GetApproximateSizes(data_cf, ranges.get(), region_count, region_sizes, uint8_t(3));
    for (size_t i = 0; i < region_ids.size(); ++i) {
        SELF_TRACE("region_id: %ld, size:%lu", region_ids[i], region_sizes[i]);
    }
    return 0;
}

void Store::update_schema_info(const pb::SchemaInfo& request) {
    //锁住的是update_table和table_info_mapping, table_info锁的位置不能改
    _factory->update_table(request);
}

void Store::check_region_legal_complete(int64_t region_id) {
    DB_WARNING("start to check whether split or add peer complete, region_id: %ld", region_id);
    auto region = get_region(region_id);
    if (region == NULL) {
        DB_WARNING("region_id: %ld not exist", region_id);
        return;
    }
    //检查并且置为失败
    if (region->check_region_legal_complete()) {
        DB_WARNING("split or add_peer complete. region_id: %ld", region_id); 
    } else {
        DB_WARNING("split or add_peer not complete, timeout. region_id: %ld", region_id);
        drop_region_from_store(region_id, false);
    }
}

void Store::construct_heart_beat_request(pb::StoreHeartBeatRequest& request) {
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
    // 读取硬盘参数
    struct statfs sfs;
    statfs(FLAGS_db_path.c_str(), &sfs);
    int64_t disk_capacity = sfs.f_blocks * sfs.f_bsize;
    int64_t left_size = sfs.f_bavail * sfs.f_bsize;
    _disk_total.set_value(disk_capacity);
    _disk_used.set_value(disk_capacity - left_size);
    instance_info->set_capacity(disk_capacity);
    instance_info->set_used_size(disk_capacity - left_size);

    //构造schema version信息
    std::unordered_map<int64_t, int64_t> table_id_version_map;
    _factory->get_all_table_version(table_id_version_map);
    for (auto table_info : table_id_version_map) { 
        pb::SchemaHeartBeat* schema = request.add_schema_infos();
        schema->set_table_id(table_info.first);
        schema->set_version(table_info.second);
    }
    //记录正在doing的table_id，所有region不上报ddlwork进展
    std::set<int64_t> ddl_wait_doing_table_ids;
    traverse_copy_region_map([&ddl_wait_doing_table_ids](SmartRegion& region) {
        if (region->is_wait_ddl()) {
            ddl_wait_doing_table_ids.insert(region->get_global_index_id());
        }
    });

    //构造所有region的version信息
    traverse_copy_region_map([&request, need_peer_balance, &ddl_wait_doing_table_ids](SmartRegion& region) {
        region->construct_heart_beat_request(request, need_peer_balance, ddl_wait_doing_table_ids);
    });

}

void Store::process_heart_beat_response(const pb::StoreHeartBeatResponse& response) {
    for (auto& schema_info : response.schema_change_info()) {
        update_schema_info(schema_info);
    }
    for (auto& add_peer_request : response.add_peers()) {
        SmartRegion region = get_region(add_peer_request.region_id());
        if (region == NULL) {
            DB_FATAL("region_id: %ld not exist, may be removed", add_peer_request.region_id());
            continue;
        }
        region->add_peer(add_peer_request, region, _add_peer_queue);
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
    const auto& delete_region_ids = response.delete_region_ids();
    auto remove_func = [this, delete_region_ids]() {
        //删除region数据，该region已不在raft组内
        for (auto& delete_region_id : delete_region_ids) {
            if (_shutdown) {
                return;
            }
            DB_WARNING("receive delete region response from meta server heart beat, delete_region_id:%ld",
                    delete_region_id);
            drop_region_from_store(delete_region_id, true);
        }
    };
    _remove_region_queue.run(remove_func);
    //ddl work
    std::set<int64_t> ddlwork_table_ids;
    for (auto& ddlwork_info : response.ddlwork_infos()) {
        ddlwork_table_ids.insert(ddlwork_info.table_id());
        traverse_copy_region_map([&ddlwork_info](SmartRegion& region) {
            if (region->get_global_index_id() == ddlwork_info.table_id()) {
                DB_DEBUG("DDL_LOG region_id [%lld] table_id: %lld start ddl work.", 
                    region->get_region_id(), ddlwork_info.table_id());
                auto ret = region->ddlwork_process(ddlwork_info);
            }
        });
    }
    traverse_copy_region_map([&ddlwork_table_ids](SmartRegion& region) {
        region->ddlwork_finish_check_process(ddlwork_table_ids);
    });
}

int Store::drop_region_from_store(int64_t drop_region_id, bool need_delay_drop) {
    SmartRegion region = get_region(drop_region_id);
    if (region == nullptr) {
        DB_FATAL("region_id: %ld not exist, may be removed", drop_region_id);
        return -1;
    }
    // 防止一直更新时间导致物理删不掉
    if (!region->removed()) {
        region->set_removed(true);
        region->shutdown();
        region->join();
        DB_WARNING("region node close for removed, region_id: %ld", drop_region_id);
    }
    if (!need_delay_drop) {
        RegionControl::clear_all_infos_for_region(drop_region_id);
        erase_region(drop_region_id);
    }
    DB_WARNING("region node removed, region_id: %ld, need_delay_drop:%d", drop_region_id, need_delay_drop);
    return 0; 
}

void Store::print_heartbeat_info(const pb::StoreHeartBeatRequest& request) {
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
}

void Store::backup_region(google::protobuf::RpcController* controller,
    const pb::BackUpReq* request,
    pb::BackUpRes* response,
    google::protobuf::Closure* done) {
    
    auto backup_type = SstBackupType::UNKNOWN_BACKUP;
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl =static_cast<brpc::Controller*>(controller);

    const std::string& req_info = cntl->http_request().unresolved_path();
    DB_NOTICE("backup request[%s]", req_info.c_str());

    std::vector<std::string> request_vec;
    boost::split(request_vec, req_info, boost::is_any_of("/"));

    if (request_vec.size() < 3) {
        DB_WARNING("backup request info error[%s]", req_info.c_str());
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        return;
    }

    if (request_vec[2] == "data") {
        backup_type = SstBackupType::DATA_BACKUP;
    } else {
        DB_WARNING("region_%lld noknown backup request [%s].", request_vec[2].c_str());
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        return;
    }

    int64_t region_id = -1;
    try {
        region_id = std::stol(request_vec[1]);
    } catch (std::exception& exp) {
        DB_WARNING("backup parse region id exp[%s]", exp.what());
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        return;
    }
    SmartRegion region = get_region(region_id);
    if (region == nullptr) {
        DB_WARNING("backup no region in store, region_id: %ld", region_id);
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        return;
    }
    //request_vec[3]=="1"，表示需要ingest最新的sst.
    bool ingest_store_latest_sst = request_vec.size() > 3 && request_vec[3] == "1";

    if (request_vec[0] == "download") {
        DB_NOTICE("backup download sst region[%lld]", region_id)
        region->process_download_sst(cntl, request_vec, backup_type);
    } else if (request_vec[0] == "upload") {
        DB_NOTICE("backup upload sst region[%lld]", region_id)
        region->process_upload_sst(cntl, ingest_store_latest_sst);
    }
}

void Store::backup(google::protobuf::RpcController* controller,
    const pb::BackupRequest* request,
    pb::BackupResponse* response,
    google::protobuf::Closure* done) {

    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    auto region_id = request->region_id();
    SmartRegion region = get_region(region_id);
    if (region == nullptr) {
        DB_WARNING("backup no region in store, region_id: %ld", region_id);
        return;
    }
    if (request->backup_op() == pb::BACKUP_DOWNLOAD) {
        DB_NOTICE("backup download sst region[%lld]", region_id)
        region->process_download_sst_streaming(cntl, request, response);
    } else if (request->backup_op() == pb::BACKUP_UPLOAD) {
        DB_NOTICE("backup upload sst region[%lld]", region_id)
        region->process_upload_sst_streaming(cntl, request->ingest_store_latest_sst(), 
            request, response);
    } else {
        DB_WARNING("unknown sst backup streaming op.");
    }
}
} //namespace
