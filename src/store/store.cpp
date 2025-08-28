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
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"
#include "mut_table_key.h"
#include "closure.h"
#include "my_raft_log_storage.h"
#include "log_entry_reader.h"
#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "concurrency.h"
#include "mut_table_key.h"
#include "my_raft_log_storage.h"
//#include <jemalloc/jemalloc.h>
#include "qos.h"
#include "rocksdb_filesystem.h"
#include "rocksdb/statistics.h"

namespace baikaldb {
DECLARE_int64(store_heart_beat_interval_us);
DECLARE_int32(balance_periodicity);
DECLARE_string(stable_uri);
DECLARE_string(snapshot_uri);
DEFINE_int64(reverse_merge_interval_us, 2 * 1000 * 1000,  "reverse_merge_interval(2 s)");
DEFINE_int64(ttl_remove_interval_s, 24 * 3600,  "ttl_remove_interval_s(24h)");
DEFINE_string(ttl_remove_interval_period, "",  "ttl_remove_interval_period hour(0-23)");
DEFINE_int64(delay_remove_region_interval_s, 600,  "delay_remove_region_interval");
//DEFINE_int32(update_status_interval_us, 2 * 1000 * 1000,  "update_status_interval(2 s)");
DEFINE_string(db_path, "./rocks_db", "rocksdb path");
DEFINE_string(resource_tag, "", "resource tag");
DEFINE_int32(update_used_size_interval_us, 10 * 1000 * 1000, "update used size interval (10 s)");
DEFINE_int32(init_region_concurrency, 10, "init region concurrency when start");
DEFINE_int32(split_threshold , 150, "split_threshold, default: 150% * region_size / 100");
DEFINE_int64(min_split_lines, 200000, "min_split_lines, protected when wrong param put in table");
DEFINE_int64(flush_region_interval_us, 10 * 60 * 1000 * 1000LL, 
            "flush region interval, default(10 min)");
DEFINE_int64(transaction_clear_interval_ms, 5000LL,
            "transaction clear interval, default(5s)");
DEFINE_int64(binlog_timeout_check_ms, 10 * 1000LL,
            "binlog timeout check interval, default(10s)");
DEFINE_int64(binlog_fake_ms, 1 * 1000LL,
            "fake binlog interval, default(1s)");
DEFINE_int64(oldest_binlog_ts_interval_s, 3600LL,
            "oldest_binlog_ts_interval_s, default(1h)");
DECLARE_int64(flush_memtable_interval_us);
DEFINE_int32(max_split_concurrency, 2, "max split region concurrency, default:2");
DEFINE_int64(none_region_merge_interval_us, 5 * 60 * 1000 * 1000LL, 
             "none region merge interval, default(5 min)");
DEFINE_int64(region_delay_remove_timeout_s, 3600 * 24LL, 
             "region_delay_remove_time_s, default(1d)");
DEFINE_bool(use_approximate_size, true, 
             "use_approximate_size");
DEFINE_bool(use_approximate_size_to_split, false, 
             "if approximate_size > 512M, then split");
DEFINE_int64(gen_tso_interval_us, 500 * 1000LL, "gen_tso_interval_us, default(500ms)");
DEFINE_int64(gen_tso_count, 100, "gen_tso_count, default(500)");
DEFINE_int64(rocks_cf_flush_remove_range_times, 10, "rocks_cf_flush_remove_range_times, default(10)");
DEFINE_int64(rocks_force_flush_max_wals, 100, "rocks_force_flush_max_wals, default(100)");
DEFINE_string(network_segment, "", "network segment of store set by user");
DEFINE_string(container_id, "", "container_id for zoombie instance");
DEFINE_int32(rocksdb_perf_level, rocksdb::kDisable, "rocksdb_perf_level");
DEFINE_bool(stop_ttl_data, false, "stop ttl data");
DEFINE_bool(stop_cold_region_flush, false, "stop_cold_region_flush");
DEFINE_bool(olap_region_split_enable, false, "olap_region_split_enable");
DEFINE_int64(check_peer_delay_min, 1, "check peer delay min");
DEFINE_bool(process_delete_regions_when_init, false, "process delete regions when init");
DECLARE_bool(store_rocks_hang_check);
DECLARE_int32(store_rocks_hang_check_timeout_s);
DECLARE_int32(store_rocks_hang_cnt_limit);
DEFINE_int32(cold_region_flush_concurrency, 5, "cold_region_flush_concurrency");
DEFINE_int32(column_minor_compact_concurrency, 5, "column_minor_compact_concurrency");
DEFINE_int32(column_major_compact_concurrency, 5, "column_major_compact_concurrency");
DEFINE_int32(column_flush_concurrency, 2, "column_flush_concurrency");
DEFINE_int32(region_size_alarm_threshold_G, 5, "default 5G");
DEFINE_int32(compact_delete_sst_keys, 0, "compact region when total delete keys > compact_delete_sst_keys");
DEFINE_int32(compact_delete_sst_keys_interval_s, 600, "compact region when total delete keys interval(s)");
DECLARE_string(cold_rocksdb_afs_infos);
DECLARE_string(meta_server_bns);
DECLARE_bool(auto_update_meta_list);
DECLARE_int32(target_file_size_base);
BRPC_VALIDATE_GFLAG(rocksdb_perf_level, brpc::NonNegativeInteger);

Store::~Store() {
}

int create_dir_if_not_exist(const std::string& path) {
    butil::FilePath output_path(path);
    if (!butil::DirectoryExists(output_path)) {
        if (!butil::CreateDirectory(output_path)) {
            DB_FATAL("FATAL create output_path fail.");
            return -1;
        }
    } 
    return 0;
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
    if (_tso_server_interact.init() != 0) {
        DB_FATAL("tso server interact init fail");
        return -1;
    }

    int ret = get_physical_room(_address, _physical_room);
    if (ret < 0) {
        DB_FATAL("get physical room fail");
        return -1;
    }
    boost::trim(FLAGS_resource_tag);
    _resource_tag = FLAGS_resource_tag;

#ifdef BAIDU_INTERNAL
    // 初始化外部文件系统，用于olap
    std::vector<AfsExtFileSystem::AfsUgi> ugi_infos;
    ret = get_afs_infos(ugi_infos);
    if (ret < 0) {
        DB_FATAL("get afs infos failed");
        return -1;
    }
    std::shared_ptr<ExtFileSystem> ext_fs(new AfsExtFileSystem(ugi_infos));

    ret = ext_fs->init();
    if (ret < 0) {
        DB_FATAL("init external filesystem failed");
        return -1;
    }

    ret = SstExtLinker::get_instance()->init(ext_fs, FLAGS_db_path + "_cold");
    if (ret < 0) {
        DB_FATAL("init sst ext linker failed");
        return -1;
    }
#endif
    // init rocksdb handler
    _rocksdb = RocksWrapper::get_instance();
    if (!_rocksdb) {
        DB_FATAL("create rocksdb handler failed");
        return -1;
    }
    if (0 != create_dir_if_not_exist(FLAGS_db_path + "_tmp") || 0 != create_dir_if_not_exist(FLAGS_db_path + "_column")) {
        DB_FATAL("create dir failed");
        return -1;
    }
    int32_t res = _rocksdb->init(FLAGS_db_path);
    if (res != 0) {
        DB_FATAL("rocksdb init failed: code:%d", res);
        return -1;
    }
    _meta_writer = MetaWriter::get_instance();
    _meta_writer->init(_rocksdb, _rocksdb->get_meta_info_handle());
    
    LogEntryReader* reader = LogEntryReader::get_instance();
    reader->init(_rocksdb, _rocksdb->get_raft_log_handle());

    //系统重启之前有哪些reigon
    std::vector<pb::RegionInfo> region_infos;
    ret = _meta_writer->parse_region_infos(region_infos);
    if (ret < 0) {
        DB_FATAL("read region_infos from rocksdb fail");
        return ret;
    }

    _factory = SchemaFactory::get_instance();
    pb::StoreHeartBeatRequest request;
    pb::StoreHeartBeatResponse response;
    //1、构造心跳请求, 重启时的心跳包除了实例信息外，其他都为空
    construct_heart_beat_request(request, &region_infos);
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

    // 心跳成功后开启rocksdb compaction
    auto s = _rocksdb->get_db()->SetOptions(_rocksdb->get_data_handle(), {{"disable_auto_compactions", "false"}});
    if (!s.ok()) {
        DB_FATAL("set rocksdb options fail");
        return -1;
    }
    int64_t heartbeat_process_time = step_time_cost.get_time();
    step_time_cost.reset();
    DB_WARNING("get schema info from meta server success");

    std::unordered_set<int64_t> delete_region_ids;
    if (FLAGS_process_delete_regions_when_init) {
        for (const auto& region_id : response.delete_region_ids()) {
            delete_region_ids.insert(region_id);
        }
    }

    // init val 
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

    for (auto& region_info : region_infos) {
        DB_WARNING("region_info:%s when init store", region_info.ShortDebugString().c_str());
        int64_t region_id = region_info.region_id();
        //construct region
        braft::GroupId groupId(std::string("region_")
                + boost::lexical_cast<std::string>(region_id));
        butil::EndPoint addr;
        str2endpoint(_address.c_str(), &addr);
        braft::PeerId peerId(addr, 0);
        DB_DEBUG("is_learner : %d", _meta_writer->read_learner_key(region_id));
        bool is_learner = _meta_writer->read_learner_key(region_id) ==  1 ? true : false;
        DB_DEBUG("region_id %ld is_learner %d", region_id, is_learner);
        //重启的region初始化时peer要为空，learner region需要根据peers初始化init_conf，不能置空。
        if (!is_learner) {
            region_info.clear_peers();
        }
        SmartRegion region(new(std::nothrow) Region(_rocksdb, 
                    _factory,
                    _address,
                    groupId,
                    peerId,
                    region_info, 
                    region_id,
                    is_learner));
        if (region == NULL) {
            DB_FATAL("new region fail. mem accolate fail. region_info:%s", 
                    region_info.ShortDebugString().c_str());
            return -1; 
        }
        if (delete_region_ids.find(region_id) != delete_region_ids.end()) {
            DB_FATAL("region_info:%s in delete", region_info.ShortDebugString().c_str());
            set_region(region);
            drop_region_from_store(region_id, true);
            continue;
        }

        if (region_info.has_is_binlog_region() && region_info.is_binlog_region()) {
            _has_binlog_region = true;
        }

        //重启的region跟新建的region或者正常运行情况下的region有两点区别
        //1、重启region的on_snapshot_load不受并发数的限制
        //2、重启region的on_snapshot_load不加载sst文件
        region->set_restart(true);
        set_region(region);
        init_region_ids.push_back(region_id);
        // version=0可能是分裂后addpeer的，通用延迟删除
        if (region_info.version() == 0) {
            Bthread bth(&BTHREAD_ATTR_SMALL);
            std::function<void()> check_region_legal_fun = 
                [this, region_id] () { check_region_legal_complete(region_id);};
            bth.run(check_region_legal_fun);
            DB_WARNING("init region verison is 0, should check region legal. region_id: %ld", 
                        region_id);
        }
    }
    int64_t new_region_process_time = step_time_cost.get_time();
    ret = _meta_writer->parse_doing_snapshot(doing_snapshot_regions);
    if (ret < 0) {
        DB_FATAL("read doing snapshot regions from rocksdb fail");
        return ret;
    } else {
        for (auto region_id : doing_snapshot_regions) {
            DB_WARNING("region_id: %ld is doing snapshot load when store stop", region_id);
        }
    }

    _db_statistic_bth.run([this]() {start_db_statistics();});
    DB_WARNING("store init_before_listen success, region_size:%lu, doing_snapshot_regions_size:%lu"
            "heartbeat_process_time:%ld new_region_process_time:%ld",
           init_region_ids.size(), doing_snapshot_regions.size(), heartbeat_process_time, new_region_process_time);
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
                DB_WARNING("no region is store, region_id: %ld", region_id);
                return;
            }
            //region raft node init
            int ret = region->init(false, 0);
            if (ret < 0) {
                DB_WARNING("region init fail when store init, region_id: %ld", region_id);
                return;
            }
        };
        init_bth.run(init_call);
    }
    init_bth.join();
    DB_WARNING("init all region success init_region_time:%ld", step_time_cost.get_time());
    
    _split_check_bth.run([this]() {whether_split_thread();});
    _vector_compact_bth.run([this]() {vector_compact_thread();});
    _merge_bth.run([this]() {reverse_merge_thread();});
    _merge_unsafe_bth.run([this]() {unsafe_reverse_merge_thread();});
    _ttl_bth.run([this]() {ttl_remove_thread();});
    _delay_remove_data_bth.run([this]() {delay_remove_data_thread();});
    _flush_bth.run([this]() {flush_memtable_thread();});
    _cold_region_flush_bth.run([this]() {cold_region_flush_thread();});
    _cold_region_check_bth.run([this]() {olap_region_check_thread();});
    _column_minor_compact_bth.run([this]() {column_minor_compact_thread();});
    _column_major_compact_bth.run([this]() {column_major_compact_thread();});
    _column_flush_bth.run([this]() {column_flush_thread();});
    _snapshot_bth.run([this]() {snapshot_thread();});
    _offline_binlog_backup_bth.run([this]() {binlog_region_backup_thread();});
    _txn_clear_bth.run([this]() {txn_clear_thread();});
    _binlog_timeout_check_bth.run([this]() {binlog_timeout_check_thread();});
    _binlog_fake_bth.run([this]() {binlog_fake_thread();});
    _region_peer_delay_bth.run([this]() {check_region_peer_delay();});
    _compact_region_by_total_sst_delete_keys.run([this]() {compact_region_by_sst_delete_keys_thread();});
    _has_prepared_tran = true;
    prepared_txns.clear();
    doing_snapshot_regions.clear();
    _is_init = true;
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

    // 只有addpeer操作，can_add_peer置位true
    bool is_addpeer = request->region_info().can_add_peer();

    //只限制addpeer
    if (_rocksdb->is_any_stall() && is_addpeer) {
        DB_WARNING("addpeer rocksdb is stall, log_id:%lu, remote_side:%s", log_id, remote_side); 
        response->set_errcode(pb::CANNOT_ADD_PEER);
        response->set_errmsg("rocksdb is stall");
        return;
    }
    // recieve_add_peer_concurrency后，直到on_snapshot_load后才释放
    // 如果没有收到on_snapshot_load，那么在shutdown时候(leader发送remove或者超时remove)才释放
    ScopeGuard auto_decrease([is_addpeer]() {
        if (is_addpeer) {
            Concurrency::get_instance()->recieve_add_peer_concurrency.decrease_signal();
        }
    });
    if (is_addpeer) {
        int ret = Concurrency::get_instance()->recieve_add_peer_concurrency.increase_timed_wait(1000 * 1000 * 10);
        if (ret != 0) {
            DB_WARNING("recieve_add_peer_concurrency timeout, count:%d, log_id:%lu, remote_side:%s", 
                    Concurrency::get_instance()->recieve_add_peer_concurrency.count(), log_id, remote_side); 
            response->set_errcode(pb::CANNOT_ADD_PEER);
            response->set_errmsg("recieve_add_peer_concurrency timeout");
            return;
        }
    }

    //新增table信息
    if (!_factory->exist_tableid(table_id)) {
        if (request->has_schema_info()) {
            update_schema_info(request->schema_info(), nullptr);
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
                                                region_id, request->region_info().is_learner()));
    if (region == NULL) {
        DB_FATAL("new region fail. mem accolate fail. logid:%lu", log_id);
        response->set_errcode(pb::INTERNAL_ERROR);
        response->set_errmsg("new region fail");
        return; 
    }

    if (region_info.has_is_binlog_region() && region_info.is_binlog_region()) {
        _has_binlog_region = true;
    }

    DB_WARNING("new region_info:%s. logid:%lu remote_side: %s", 
            request->ShortDebugString().c_str(), log_id, remote_side);

    //写内存
    set_region(region);
    //region raft node init
    int ret = region->init(true, request->snapshot_times());
    if (ret < 0) {
        //删除该region相关的全部信息
        RegionControl::clear_all_infos_for_region(region_id, table_id);
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
    auto_decrease.release();
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
    if (request->op_type() == pb::SetPeer && request->force() && region->removed()) {
        // 需要等table info才能init成功
        region->wait_table_info(); // 死循环 !
        // 恢复相当于重启region
        region->set_restart(true);
        region->init(false, 0);
        region->set_removed(false);
        DB_WARNING("restore region_id: %ld success ", region->get_region_id());
    }
    region->raft_control(controller,
                         request,
                         response,
                         done_guard.release());
    if (request->op_type() == pb::TransLeader && response->errcode() == pb::SUCCESS) {
        region->transfer_leader_set_is_leader();
    }
}

void Store::health_check(google::protobuf::RpcController* controller,
                  const pb::HealthCheck* request,
                  pb::StoreRes* response,
                  google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    response->set_errcode(pb::SUCCESS);
/*
    // 判断是否BUSY
    int64_t qps = RocksdbVars::get_instance()->rocksdb_get_time_cost_qps.get_value(bvar::FLAGS_bvar_dump_interval) + 
                  RocksdbVars::get_instance()->rocksdb_seek_time_cost_qps.get_value(bvar::FLAGS_bvar_dump_interval) + 
                  RocksdbVars::get_instance()->rocksdb_scan_time_cost_qps.get_value(bvar::FLAGS_bvar_dump_interval) / FLAGS_get_token_weight;
    int64_t fetch_token_qps = RocksdbVars::get_instance()->qos_fetch_tokens_qps.get_value(bvar::FLAGS_bvar_dump_interval);
    bool match_reject_condition = StoreQos::get_instance()->match_reject_condition();
    static TimeCost last_print_time;
    // 三个条件满足其一就认为是BUSY
    if (match_reject_condition || 
        (qps * 100) > (FLAGS_max_tokens_per_second * 70) || 
        (fetch_token_qps * 100) > (FLAGS_max_tokens_per_second * 70)) {
        if (last_print_time.get_time() > 60 * 1000 * 1000LL) {
            DB_WARNING("qps: %ld, fetch_token_qps: %ld, match_reject_condition: %d, store busy", 
                    qps, fetch_token_qps, match_reject_condition);
            last_print_time.reset();
        }
        response->set_errcode(pb::STORE_BUSY);
    }
*/
    if (FLAGS_store_rocks_hang_check) {
        if (last_rocks_hang_check_ok.get_time() > 30 * 1000 * 1000LL 
             || (last_rocks_hang_check_cost >= FLAGS_store_rocks_hang_check_timeout_s * 1000 * 1000LL 
                    && rocks_hang_continues_cnt >= FLAGS_store_rocks_hang_cnt_limit)) {
            response->set_errcode(pb::STORE_ROCKS_HANG);
        }
    }
}

void Store::async_apply_log_entry(google::protobuf::RpcController* controller,
                                 const pb::BatchStoreReq* request,
                                 pb::BatchStoreRes* response,
                                 google::protobuf::Closure* done) {
    bthread_usleep(20);
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl =
            static_cast<brpc::Controller*>(controller);
    uint64_t log_id = 0;
    if (request == nullptr || response == nullptr || cntl == nullptr) {
        return;
    }
    const auto& remote_side_tmp = butil::endpoint2str(cntl->remote_side());
    const char* remote_side = remote_side_tmp.c_str();
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    SmartRegion region = get_region(request->region_id());
    if (region == nullptr || region->removed()) {
        response->set_errcode(pb::REGION_NOT_EXIST);
        response->set_errmsg("region_id not exist in store");
        DB_WARNING("region_id: %ld not exist in store, logid:%lu, remote_side: %s",
                   request->region_id(), log_id, remote_side);
        return;
    }
    region->async_apply_log_entry(controller, request, response, done_guard.release());
}

void Store::query(google::protobuf::RpcController* controller,
                  const pb::StoreReq* request,
                  pb::StoreRes* response,
                  google::protobuf::Closure* done) {
    bthread_usleep(20);
    static thread_local TimeCost last_perf;
    if (FLAGS_rocksdb_perf_level > rocksdb::kDisable && last_perf.get_time() > 1000 * 1000) {
        DB_WARNING("perf_context:%s", rocksdb::get_perf_context()->ToString(true).c_str());
        DB_WARNING("iostats_context:%s", rocksdb::get_iostats_context()->ToString(true).c_str());
        rocksdb::SetPerfLevel((rocksdb::PerfLevel)FLAGS_rocksdb_perf_level);
        rocksdb::get_perf_context()->Reset();
        rocksdb::get_iostats_context()->Reset();
        last_perf.reset();
    }
    if (FLAGS_rocksdb_perf_level == rocksdb::kDisable) {
        rocksdb::SetPerfLevel(rocksdb::kDisable);
    }
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl =
            static_cast<brpc::Controller*>(controller);
    uint64_t log_id = 0;
    const auto& remote_side_tmp = butil::endpoint2str(cntl->remote_side());
    const char* remote_side = remote_side_tmp.c_str();
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    //DB_WARNING("region_id: %ld before get_region, logid:%lu, remote_side: %s", request->region_id(), log_id, remote_side);
    SmartRegion region = get_region(request->region_id());
    if (region == nullptr || region->removed()) {
        response->set_errcode(pb::REGION_NOT_EXIST);
        response->set_errmsg("region_id not exist in store");
        DB_WARNING("region_id: %ld not exist in store, logid:%lu, remote_side: %s",
                request->region_id(), log_id, remote_side);
        return;
    }
    region->query(controller,
                  request,
                  response,
                  done_guard.release());
}

void Store::query_batch(google::protobuf::RpcController* controller,
                  const pb::BatchRegionStoreReq* request,
                  pb::BatchRegionStoreRes* batch_response,
                  google::protobuf::Closure* done) {
    bthread_usleep(20);
    static thread_local TimeCost last_perf;
    if (FLAGS_rocksdb_perf_level > rocksdb::kDisable && last_perf.get_time() > 1000 * 1000) {
        DB_WARNING("perf_context:%s", rocksdb::get_perf_context()->ToString(true).c_str());
        DB_WARNING("iostats_context:%s", rocksdb::get_iostats_context()->ToString(true).c_str());
        rocksdb::SetPerfLevel((rocksdb::PerfLevel)FLAGS_rocksdb_perf_level);
        rocksdb::get_perf_context()->Reset();
        rocksdb::get_iostats_context()->Reset();
        last_perf.reset();
    }
    if (FLAGS_rocksdb_perf_level == rocksdb::kDisable) {
        rocksdb::SetPerfLevel(rocksdb::kDisable);
    }
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl =
            static_cast<brpc::Controller*>(controller);
    uint64_t log_id = 0;
    const auto& remote_side_tmp = butil::endpoint2str(cntl->remote_side());
    const char* remote_side = remote_side_tmp.c_str();
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    ConcurrencyBthread batch_query_bth(request->limit_single_store_concurrency());
    int n = request->store_req_size();
    batch_response->mutable_store_res()->Reserve(n);
    for (int i = 0; i < n; i++) {
        pb::StoreRes* single_res = batch_response->add_store_res();
    }

    pb::BatchRegionStoreReq* non_const_request = const_cast<pb::BatchRegionStoreReq*>(request);
    for (int i = 0; i < n; i++) {
        auto batch_query_call = [this, &controller, &batch_response, &non_const_request, i, n, log_id, remote_side]() {
            pb::StoreRes single_response;
            pb::StoreReq* single_region_req = non_const_request->mutable_store_req(i);
            // 指针指向BatchRegionStoreReq中的plan
            if (non_const_request->plan_size() == 1) {
                single_region_req->set_allocated_plan(non_const_request->mutable_plan(0));
            } else if (non_const_request->plan_size() == n) {
                single_region_req->set_allocated_plan(non_const_request->mutable_plan(i));
            } else {
                DB_FATAL("plan size error, %d != %d", non_const_request->plan_size(), n);
                single_response.set_errcode(pb::INPUT_PARAM_ERROR);
                single_response.set_errmsg("plan size error");
                return;
            }
            SmartRegion region = get_region(single_region_req->region_id());
            if (region == nullptr || region->removed()) {
                single_response.set_errcode(pb::REGION_NOT_EXIST);
                single_response.set_errmsg("region_id not exist in store");
                single_response.set_orig_region_id(single_region_req->region_id());
                batch_response->mutable_store_res(i)->Swap(&single_response);
                single_region_req->release_plan();
                return;
            }
            region->query(controller,
                        single_region_req,
                        &single_response,
                        nullptr);
            single_response.set_orig_region_id(single_region_req->region_id());
            batch_response->mutable_store_res(i)->Swap(&single_response);
            single_region_req->release_plan();
            return;
        };
        batch_query_bth.run(batch_query_call);
    }
    batch_query_bth.join();
}

void Store::query_binlog(google::protobuf::RpcController* controller,
                  const pb::StoreReq* request,
                  pb::StoreRes* response,
                  google::protobuf::Closure* done) {
    bthread_usleep(20);
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl =
            static_cast<brpc::Controller*>(controller);
    uint64_t log_id = 0;
    const auto& remote_side_tmp = butil::endpoint2str(cntl->remote_side());
    const char* remote_side = remote_side_tmp.c_str();
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    //DB_WARNING("region_id: %ld before get_region, logid:%lu, remote_side: %s", request->region_id(), log_id, remote_side);
    SmartRegion region = get_region(request->region_id());
    if (region == nullptr || region->removed()) {
        response->set_errcode(pb::REGION_NOT_EXIST);
        response->set_errmsg("region_id not exist in store");
        DB_WARNING("region_id: %ld not exist in store, logid:%lu, remote_side: %s",
                request->region_id(), log_id, remote_side);
        return;
    }
    region->query_binlog(controller,
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
    _multi_thread_cond.increase();
    ON_SCOPE_EXIT([this]() {
        _multi_thread_cond.decrease_signal();
    });
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
        traverse_copy_region_map([request, call](const SmartRegion& region) {
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
    if (request->use_read_idx()) {
        region->get_read_index(request, response);
        return;
    }
    response->set_region_status(region->region_status());
    response->set_applied_index(region->get_log_index());
    response->mutable_region_raft_stat()->set_applied_index(region->get_log_index());
    response->mutable_region_raft_stat()->set_snapshot_data_size(region->snapshot_data_size());
    response->mutable_region_raft_stat()->set_snapshot_meta_size(region->snapshot_meta_size());
    response->mutable_region_raft_stat()->set_snapshot_index(region->snapshot_index());
    response->mutable_region_raft_stat()->set_dml_latency(region->get_dml_latency());
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
            } else if (type == 3) { // raft_log_cf
                cf = _rocksdb->get_raft_log_handle();
            } else {
                return;
            }
        }

        // 异步执行
        Bthread bth;
        bth.run([this, cf]() {
            TimeCost tc;
            rocksdb::CompactRangeOptions compact_options;
            compact_options.exclusive_manual_compaction = false;
            auto res = _rocksdb->compact_range(compact_options, cf, nullptr, nullptr);
            if (!res.ok()) {
                DB_WARNING("compact_range error: code=%d, msg=%s", 
                        res.code(), res.ToString().c_str());
            }
            DB_WARNING("compact_db cost:%ld", tc.get_time());
        });
    } else {
        for (auto region_id : request->region_ids()) {
            auto type = request->compact_type();
            if (type == 4 || type == 5 || type == 6) {
                SmartRegion region = get_region(region_id);
                if (region == nullptr) {
                    continue;
                }
                if (type == 4) {
                    region->column_manual_base_compaction();
                } else if (type == 5) {
                    region->column_manual_row2column();
                } else if (type == 6) {
                    region->vector_compaction(true);
                } else {
                    DB_WARNING("invalid type: %d", type);
                    continue;
                }
            } else {
                RegionControl::compact_data(region_id);
            }
        }
    }
    DB_WARNING("compact_db cost:%ld", cost.get_time());
}

void Store::manual_split_region(google::protobuf::RpcController* controller,
                            const baikaldb::pb::RegionIds* request,
                            pb::StoreRes* response,
                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("success");
    std::vector<int64_t> region_ids;
    std::vector<int64_t> userids;
    region_ids.reserve(1);
    userids.reserve(1);
    DB_WARNING("request: %s", request->ShortDebugString().c_str());
    if (request->userids_size() > 0 && request->userids_size() != request->region_ids_size()) {
        response->set_errcode(pb::EXEC_FAIL);
        response->set_errmsg("diff region_id with userid");
        DB_FATAL("userid size diff with region_id size");
        return;
    }
    for (int64_t region_id : request->region_ids()) {
        region_ids.emplace_back(region_id);
        userids.emplace_back(0);
    }

    if (request->userids_size() > 0) {
        userids.clear();
        for (int64_t userid : request->userids()) {
            userids.emplace_back(userid);
        }
    }

    auto fun = [this, region_ids, userids, response]() {
        if (region_ids.size() != userids.size()) {
            return;
        }
        for (int i = 0; i < region_ids.size(); i++) {
            int64_t region_id = region_ids[i];
            int64_t userid = userids[i];
            SmartRegion region = get_region(region_id);
            if (region == nullptr || region->get_version() == 0 || region->is_binlog_region() || region->is_learner() || 
                    region->removed() || region->olap_state() != pb::OLAP_ACTIVE || !region->is_leader()) {
                response->set_errcode(pb::EXEC_FAIL);
                response->set_errmsg("cant do manual split");
                DB_WARNING("region_id: %ld cant do manual split", region_id);
                continue;
            }
            // 分裂异步执行分裂
            std::string split_key;
            int64_t split_key_term = 0;
            if (region->is_tail()) {
                process_split_request(region->get_global_index_id(), region_id, true, split_key, split_key_term);
                continue;
            }
            if (userid == 0) {
                bool is_cold = false;
                bool is_olap_table = _factory->is_olap_table(region->get_table_id(), region->get_partition_id(), &is_cold);
                if (is_olap_table) {
                    if (is_cold) {
                        continue;
                    }
                    int ret = region->modify_olap_region_num_table_lines();
                    if (ret < 0) {
                        continue;
                    }
                    int64_t region_capacity = 10000000;
                    ret = _factory->get_region_capacity(region->get_global_index_id(), region_capacity);
                    if (ret != 0) {
                        DB_DEBUG("table info not exist, region_id: %ld", region_id);
                        continue;
                    }
                    region_capacity = std::max(FLAGS_min_split_lines, region_capacity);
                    if (region->get_num_table_lines() < FLAGS_split_threshold * region_capacity / 100) {
                        continue;
                    }
                } 

                if (0 != region->get_split_key(split_key, split_key_term)) {
                    DB_WARNING("get_split_key failed: region=%ld", region_id);
                    continue;
                }
            } else {
                if (0 != region->get_split_key(userid, split_key, split_key_term)) {
                    continue;
                }
            }
            process_split_request(region->get_global_index_id(), region_id, false, split_key, split_key_term);
        }
        DB_WARNING("all region finish");
    };
    Bthread bth(&BTHREAD_ATTR_SMALL);
    bth.run(fun);
    bth.join();
}

void Store::manual_link_external_sst(google::protobuf::RpcController* controller,
                            const baikaldb::pb::RegionIds* request,
                            pb::StoreRes* response,
                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("success");
    DB_WARNING("request: %s", request->ShortDebugString().c_str());
    for (int64_t region_id : request->region_ids()) {
        SmartRegion region = get_region(region_id);
        if (region == nullptr || region->get_version() == 0 || region->is_binlog_region() || region->is_learner() || 
                region->removed() || !region->is_leader()) {
            response->set_errcode(pb::EXEC_FAIL);
            response->set_errmsg("cant do manual link");
            if (region != nullptr && !region->is_leader()) {
                response->set_errcode(pb::NOT_LEADER);
                response->set_leader(butil::endpoint2str(region->get_leader()).c_str());
                response->set_errmsg("not leader");
            }
            
            DB_WARNING("region_id: %ld cant do manual link", region_id);
            break;
        }

        int ret = region->manual_link_external_sst();
        if (ret != 0) {
            response->set_errcode(pb::EXEC_FAIL);
            response->set_errmsg("cant do manual link");
            DB_WARNING("region_id: %ld cant do manual link", region_id);
            break;
        }
    }
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
        traverse_region_map([&region_ids](const SmartRegion& region) {
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
                // 分裂异步执行dml，不能做snapshot
                if (region->get_version() != 0) {
                    region->do_snapshot();
                }
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
    
    if (request->clear_all_txns()) {
        DB_WARNING("rollback all txns req:%s", request->ShortDebugString().c_str());
        int64_t timeout = request->txn_timeout();
        for (auto region_id : request->region_ids()) {
            SmartRegion region = get_region(region_id);
            if (region != nullptr) {
                region->rollback_txn_before(timeout);
            }
        }
        if (!request->has_table_id()) {
            return;
        }
        int64_t table_id = request->table_id();
        traverse_region_map([table_id, timeout](const SmartRegion& region) {
            if (region->get_table_id() == table_id) {
                DB_WARNING("rollback all txns region_id:%ld", region->get_region_id());
                region->rollback_txn_before(timeout);
            }
        });
        return;
    }
    if (request->query_apply_index()) {
        auto extra_res = response->mutable_extra_res();

        std::vector<rocksdb::LiveFileMetaData> metadata;
        _rocksdb->get_cold_live_files(&metadata);
        std::map<int64_t, std::set<std::string>> region_sst_map;
        for (const auto& md : metadata) {
            if (md.column_family_name != RocksWrapper::COLD_DATA_CF) {
                continue;
            }
            TableKey smallestkey(md.smallestkey);
            TableKey largestkey(md.largestkey);
            int64_t smallest_region_id = smallestkey.extract_i64(0);
            int64_t largest_region_id  = largestkey.extract_i64(0);
            if (smallest_region_id != largest_region_id) {
                DB_FATAL("cold sst: %s has diff region_id %ld vs %ld", md.relative_filename.c_str(), smallest_region_id, largest_region_id);
                continue;
            }
            region_sst_map[smallest_region_id].insert(md.relative_filename);
        }

        std::map<std::string, SstExtLinker::ExtFileInfo> sst_ext_map;
        SstExtLinker::get_instance()->sst_ext_map(sst_ext_map);
        std::map<int64_t, int64_t> hot_region_size_map;
        get_hot_region_size(hot_region_size_map);

        std::vector<int64_t> region_ids;
        region_ids.reserve(1024);
        if (request->region_ids_size() <= 0) {
            traverse_region_map([&region_ids](const SmartRegion& region) {
                region_ids.emplace_back(region->get_region_id());
            });
        } else {
            for (auto region_id : request->region_ids()) {
                region_ids.emplace_back(region_id);
            }
        }
        for (int64_t region_id : region_ids) {
            auto info = extra_res->add_infos();
            info->set_region_id(region_id);
            info->set_resource_tag(FLAGS_resource_tag);
            SmartRegion region = get_region(region_id);
            if (region == nullptr) {
                info->set_table_id(-1);
                info->set_version(-1);
                info->set_apply_index(-1);
                info->set_status("NOTFOUND");
                DB_FATAL("region_id: %ld not exist, may be removed", region_id);
                continue;
            } 

            info->set_table_id(region->get_table_id());
            info->set_version(region->get_version());
            info->set_apply_index(region->get_log_index());
            if (region->is_leader()) {
                info->set_status("LEADER");
            } else if (region->is_learner()) {
                info->set_status("LEARNER");
            } else {
                info->set_status("FOLLOWER");
            }

            pb::RegionColumnFiles column_info;
            int ret = _meta_writer->read_column_file_info(region->get_region_id(), column_info);
            if (ret < 0) {
                info->set_column_info("");
            } else {
                // 清理startkey endkey，避免冗余打印
                for (int i = 0; i < column_info.active_files_size(); ++i) {
                    column_info.mutable_active_files(i)->clear_start_key();
                    column_info.mutable_active_files(i)->clear_end_key();
                }
                for (int i = 0; i < column_info.delete_files_size(); ++i) {
                    column_info.mutable_delete_files(i)->clear_start_key();
                    column_info.mutable_delete_files(i)->clear_end_key();
                }
                info->set_column_info(column_info.ShortDebugString());
            }

            pb::OlapRegionInfo olap_info;
            ret = _meta_writer->read_olap_info(region->get_region_id(), olap_info);
            if (ret < 0) {
                DB_WARNING("region_id: %ld read olap info failed", region->get_region_id());
                continue;
            }

            info->set_olap_state(olap_info.state());

            if (olap_info.state() < pb::OLAP_FLUSHED || olap_info.external_full_path_size() <= 0) {
                DB_WARNING("region_id: %ld diff state info: %s", region->get_region_id(), olap_info.ShortDebugString().c_str());
                info->set_region_size(hot_region_size_map[region_id]);
                continue;
            }

            for (int i = 0; i < olap_info.olap_index_info_list_size(); i++) {
                info->add_olap_index_info_list()->Swap(olap_info.mutable_olap_index_info_list(i));
            }

            uint64_t total_size = 0;
            std::set<std::string> rocks_external_files;
            for (const auto& f : olap_info.external_full_path()) {
                rocks_external_files.emplace(f);
                info->add_external_full_path(f);
                uint64_t size;
                int ret = get_size_by_external_file_name(&size, nullptr, f);
                if (ret != 0) {
                    continue;
                }
                total_size += size;
            }
            info->set_region_size(total_size);

            auto iter = region_sst_map.find(region->get_region_id());
            if (iter == region_sst_map.end()) {
                info->set_path_diff(true);
                DB_FATAL("region_id: %ld cant find cold sst, olap_info: %s", region->get_region_id(), olap_info.ShortDebugString().c_str());
                continue;
            }

            if (rocks_external_files.size() != iter->second.size()) {
                info->set_path_diff(true);
                DB_FATAL("region_id: %ld diff size rocks vs json", region->get_region_id());
                continue;
            }

            std::set<std::string> json_external_files;
            for (const std::string& f : iter->second) {
                auto it = sst_ext_map.find(f);
                if (it == sst_ext_map.end()) {
                    info->set_path_diff(true);
                    DB_FATAL("region_id: %ld cant find sst: %s", region->get_region_id(), f.c_str());
                } else {
                    json_external_files.emplace(it->second.full_name);
                }
            }

            std::set<std::string> diff;
            std::set_difference(rocks_external_files.begin(), rocks_external_files.end(), 
                                json_external_files.begin(), json_external_files.end(),
                                std::inserter(diff, diff.begin()));

            if (!diff.empty()) {
                info->set_path_diff(true);
                DB_FATAL("region_id: %ld diff sst rocks vs json", region->get_region_id());
            }

            info->set_path_diff(false);
        }
        return;
    }
    if (request->query_all_afs_file()) {
        auto extra_res = response->mutable_extra_res();
        std::map<std::string, SstExtLinker::ExtFileInfo> sst_ext_map;
        SstExtLinker::get_instance()->sst_ext_map(sst_ext_map);
        for (const auto& pair : sst_ext_map) {
            std::string* full_name = extra_res->add_afs_full_names();
            *full_name = pair.second.full_name;
        }
        extra_res->set_get_afs_path_succ(true);
        return;
    }
    if (request->query_olap_keypoint()) {
        std::map<int64_t, int64_t> userid_count;
        auto extra_res = response->mutable_extra_res();
        for (int64_t region_id : request->region_ids()) {
            SmartRegion region = get_region(region_id);
            if (region == nullptr) {
                DB_WARNING("region_id: %ld not exist", region_id);
                return;
            }
            int64_t table_id = region->get_table_id();
            auto table_info = _factory->get_table_info_ptr(table_id);
            auto pri_index = _factory->get_index_info_ptr(table_id);
            if (pri_index == nullptr || table_info == nullptr) {
                return;
            }
            if (!is_int(pri_index->fields[0].type)) {
                return;
            }
            SmartRecord record = _factory->new_record(table_id);
            if (record == nullptr) {
                return;
            }
            std::string region_start_key = region->get_start_key();
            std::string region_end_key = region->get_end_key();
            MutTableKey rocksdb_start_key;
            MutTableKey rocksdb_end_key;
            rocksdb_start_key.append_i64(region_id);
            rocksdb_start_key.append_i64(table_id);
            rocksdb_start_key.append_string(region_start_key);
            rocksdb_end_key.append_i64(region_id);
            rocksdb_end_key.append_i64(table_id);
            if (region_end_key.empty()) {
                rocksdb_end_key.append_u64(UINT64_MAX);
            } else {
                rocksdb_end_key.append_string(region_end_key);
            }

            rocksdb::TablePropertiesCollection props;
            if (region->olap_state() >= pb::OLAP_FLUSHED) {
                _rocksdb->get_cold_sst_properties(rocksdb_start_key.data(), rocksdb_end_key.data(), props);
            } else {
                _rocksdb->get_sst_properties(rocksdb_start_key.data(), rocksdb_end_key.data(), props);
            }
            std::vector<rocksdb::Slice> keys;
            _rocksdb->decode_key_points(region_start_key, region_end_key, rocksdb_start_key.data().substr(0, 16), props, keys);
            for (const auto& key : keys) {
                SmartRecord record = _factory->new_record(*table_info);
                if (record == nullptr) {
                    return;
                }
                int ret = record->decode_key(*pri_index, key);
                if (ret != 0) {
                    return;
                }

                auto desc = record->get_field_by_idx(pri_index->fields[0].pb_idx);
                if (desc == nullptr) {
                    return;
                }

                ExprValue value = record->get_value(desc);
                if (value.is_null()) {
                    return;
                }

                int64_t userid = value.get_numberic<int64_t>();
                if (userid_count.count(userid) > 0) {
                    userid_count[userid] += 1;
                } else {    
                    userid_count[userid] = 1;
                }
            }
        }

        extra_res->set_query_keypoint_succ(true);
        for (const auto& pair : userid_count) {
            auto info = extra_res->add_userid_count();
            info->set_userid(pair.first);
            info->set_count(pair.second);
        }

        return;
    }
    if (request->region_ids_size() == 0) {
        traverse_region_map([&id_leader_map](const SmartRegion& region) {
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

void Store::query_file_system(google::protobuf::RpcController* controller,
                        const pb::CompactionFileRequest* request,
                        pb::CompactionFileResponse* response,
                        google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    int ret = 0;
    switch (request->op_type()) {
        case pb::OP_READ:
            ret = read_file(request, response);
            break;
        case pb::OP_WRITE:
            ret = write_file(request, response);
            break;
        case pb::OP_READ_DIR:
            ret = read_dir(request, response);
            break;
        case pb::OP_CREATE_DIR:
            ret = create_dir(request, response);
            break;
        case pb::OP_PATH_EXISTS:
            ret = path_exists(request, response);
            break;
        case pb::OP_GET_FILE_INFO_LIST:
            ret = get_file_info_list(request, response);
            break;
        case pb::OP_DELETE_PATH:
            ret = delete_path(request, response);
            break;
        default:
            break;
    }
    if (response->has_errcode()) {
        return;
    }
    if (ret != 0) {
        DB_FATAL("remote_compaction_id: %s fail to do file operation, op: %s",
                request->remote_compaction_id().c_str(),
                pb::CompactionOpType_Name(request->op_type()).c_str());
        response->set_errcode(pb::COMPACTION_FILE_SYSTEM_ERROR);
        return;
    }
    response->set_errcode(pb::SUCCESS);
}

int Store::read_file(const pb::CompactionFileRequest* request,
                                    pb::CompactionFileResponse* response) {
    TimeCost cost;
    static bvar::LatencyRecorder remote_compaction_read_rate("remote_compaction_read_rate");
    std::vector<std::string> split_vec;
    boost::split(split_vec, request->file_name(), boost::is_any_of("/"));
    const std::string& file_name = split_vec.back();
    std::string file_path_str = FLAGS_db_path + "/" + file_name;
    
    auto try_open_file = [](const std::string& path) {
        butil::FilePath file_path(path);
        return butil::File(file_path, butil::File::FLAG_OPEN | butil::File::FLAG_READ);
    };

    butil::File file = try_open_file(file_path_str);

    // WAL有可能被move到archive目录下
    if (!file.IsValid() && file_name.find(".log") != std::string::npos) {
        file_path_str = FLAGS_db_path + "/archive/" + file_name;
        file = try_open_file(file_path_str);
    }

    if (!file.IsValid()) {
        // DB_FATAL("File does not exist: %s, remote_compaction_id: %s", 
        //         file_path_str.c_str(), request->remote_compaction_id().c_str());
        return -1;
    }

    int64_t offset = request->offset();
    int size = request->count();
    std::vector<char> buffer(size);

    int bytes_read = file.Read(offset, buffer.data(), size);
    if (bytes_read < 0) {
        DB_FATAL("Failed to read file: %s, remote_compaction_id: %s", 
            file_path_str.c_str(), request->remote_compaction_id().c_str());
        return -1;
    }
    response->set_data(buffer.data(), bytes_read);
    int64_t read_cost = cost.get_time();
    if (read_cost != 0) {
        remote_compaction_read_rate << (size * 1000000LL / read_cost);
    }
    return 0;
}

int Store::write_file(const pb::CompactionFileRequest* request,
                            pb::CompactionFileResponse* response) {
    TimeCost cost;
    static bvar::LatencyRecorder remote_compaction_write_rate("remote_compaction_write_rate");
    std::vector<std::string> split_vec;
    boost::split(split_vec, request->file_name(), boost::is_any_of("/"));
    const std::string& file_path_str = FLAGS_db_path + "/" + 
                        FLAGS_secondary_db_path + "/" + 
                        request->remote_compaction_id() + "/" + split_vec.back();
    butil::FilePath file_path(file_path_str);
    butil::FilePath directory_path = file_path.DirName();
    // 检查并创建目录（如果不存在）
    if (!butil::PathExists(directory_path)) {
        if (!butil::CreateDirectory(directory_path)) {
            DB_FATAL("Failed to create directory: %s, remote_compaction_id: %s", 
                     directory_path.value().c_str(), request->remote_compaction_id().c_str());
            return -1;
        }
    }
    // 刚开始写, 先将文件清空
    if (request->offset() == 0) {
        bool success = butil::DeleteFile(file_path, true);
        if (!success) {
            DB_FATAL("Failed to delete file: %s, remote_compaction_id: %s", 
                file_path_str.c_str(), request->remote_compaction_id().c_str());
            return -1;
        }
    }
    // 打开文件以进行读写操作，必须存在。
    butil::File file(file_path, butil::File::FLAG_OPEN_ALWAYS | butil::File::FLAG_READ | butil::File::FLAG_WRITE);
    if (!file.IsValid()) {
        DB_FATAL("File does not exist: %s, remote_compaction_id: %s", 
            file_path_str.c_str(), request->remote_compaction_id().c_str());
        return -1;
    }

    int64_t offset = request->offset();
    const std::string& data = request->data();
    int size = data.size();

    int bytes_written = file.Write(offset, data.data(), size);
    if (bytes_written < 0 || bytes_written != size) {
        DB_FATAL("Failed to write file: %s, remote_compaction_id: %s", 
            file_path_str.c_str(), request->remote_compaction_id().c_str());
        return -1;
    }
    int64_t write_cost = cost.get_time();
    if (write_cost != 0) {
        remote_compaction_write_rate << (size * 1000000LL / write_cost);
    }
    return 0;
}

int Store::create_dir(const pb::CompactionFileRequest* request,
                                    pb::CompactionFileResponse* response) {
    const std::string& dir = FLAGS_db_path + "/" 
                            + FLAGS_secondary_db_path + "/" 
                            + request->remote_compaction_id();
    if (0 != create_dir_if_not_exist(dir)) {
        DB_FATAL("create dir: %s failed", dir.c_str());
        return -1;
    }
    return 0;
}

int Store::read_dir(const pb::CompactionFileRequest* request,
                    pb::CompactionFileResponse* response) {
    butil::FilePath file_path(FLAGS_db_path);
    // 检查目录是否存在
    if (!butil::DirectoryExists(file_path)) {
        DB_WARNING("File does not exist: %s, remote_compaction_id: %s", 
            FLAGS_db_path.c_str(), request->remote_compaction_id().c_str());
        response->set_errcode(pb::COMPACTION_FILE_NOT_EXIST);
        return 0;
    }
    butil::FileEnumerator enumerator(file_path, false,  butil::FileEnumerator::FILES);
    for (butil::FilePath file = enumerator.Next(); !file.empty(); file = enumerator.Next()) {
        pb::CompactionFileInfo file_info;
        file_info.set_file_path(file.BaseName().value());
        response->add_file_info()->Swap(&file_info);
    }
    // DB_WARNING("remote_compaction_id: %s, response debug: %s", request->remote_compaction_id().c_str(), response->ShortDebugString().c_str());
    return 0;
}

int Store::path_exists(const pb::CompactionFileRequest* request,
                    pb::CompactionFileResponse* response) {
    std::vector<std::string> split_vec;
    boost::split(split_vec, request->file_name(), boost::is_any_of("/"));
    const std::string& file_path_str = FLAGS_db_path + "/" + split_vec.back();
    butil::FilePath file_path(file_path_str);
    // 检查文件是否存在
    if (!butil::PathExists(file_path)) {
        DB_WARNING("File does not exist: %s, remote_compaction_id: %s", 
            file_path_str.c_str(), request->remote_compaction_id().c_str());
        response->set_errcode(pb::COMPACTION_FILE_NOT_EXIST);
        return 0;
    }
    pb::CompactionFileInfo file_info;
    file_info.set_file_path(split_vec.back());
    response->add_file_info()->Swap(&file_info);
    return 0;
}

int Store::get_file_info_list(const pb::CompactionFileRequest* request,
                    pb::CompactionFileResponse* response) {
    butil::FilePath file_path(FLAGS_db_path);
    // 检查目录是否存在
    if (!butil::DirectoryExists(file_path)) {
        DB_WARNING("File does not exist: %s, remote_compaction_id: %s", 
            FLAGS_db_path.c_str(), request->remote_compaction_id().c_str());
        response->set_errcode(pb::COMPACTION_FILE_NOT_EXIST);
        return 0;
    }
    butil::FileEnumerator enumerator(file_path, false,  butil::FileEnumerator::FILES);
    for (butil::FilePath file = enumerator.Next(); !file.empty(); file = enumerator.Next()) {
        butil::File::Info info;
        if (!butil::GetFileInfo(file, &info)) {
            // 中间可能会被删除
            continue;
        }

        pb::CompactionFileInfo file_info;
        file_info.set_file_path(file.BaseName().value());
        file_info.set_file_size(info.size);
        response->add_file_info()->Swap(&file_info);
    }
    return 0;
}

int Store::delete_path(const pb::CompactionFileRequest* request,
                                    pb::CompactionFileResponse* response) {
    const std::string& dir_str = FLAGS_db_path + "/" + 
                        FLAGS_secondary_db_path + "/" + 
                        request->remote_compaction_id();
    butil::FilePath dir_path(dir_str);
    if (!butil::PathExists(dir_path)) {
        DB_FATAL("File does not exist: %s", dir_str.c_str());
        return -1;
    }

    bool success = butil::DeleteFile(dir_path, request->recursive());
    if (!success) {
        DB_FATAL("Failed to delete file: %s", dir_str.c_str());
        return -1;
    }
    return 0;
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
        traverse_region_map([&id_leader_map](const SmartRegion& region) {
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
        traverse_copy_region_map([](const SmartRegion& region) {
            region->cancel_all_blacklist_sign();
        });
        bthread_usleep_fast_shutdown(FLAGS_store_heart_beat_interval_us, _shutdown);
    }
}

void Store::send_heart_beat() {
    pb::StoreHeartBeatRequest request;
    pb::StoreHeartBeatResponse response;
    //1、构造心跳请求
    heart_beat_count << 1;
    construct_heart_beat_request(request);
    print_heartbeat_info(request);
    //2、发送请求
    if (_meta_server_interact.send_request("store_heartbeat", request, response) != 0) {
        DB_WARNING("send heart beat request to meta server fail");
    } else {
        //处理心跳
        process_heart_beat_response(response);
    }
    DB_DEBUG("meta request %s", request.ShortDebugString().c_str());
    DB_DEBUG("meta response %s", response.ShortDebugString().c_str());
    _last_heart_time.reset();
    heart_beat_count << -1;
    DB_WARNING("heart beat");
    if (FLAGS_auto_update_meta_list) {
        update_meta_list();
    }
}

void Store::update_meta_list() {
    pb::RaftControlRequest req;
    req.set_op_type(pb::GetPeerList);
    pb::RaftControlResponse res;
    if (_meta_server_interact.send_request("raft_control", req, res) == 0) {
        std::string meta_list = "";
        for (auto i = 0; i < res.peers_size(); i ++) {
            if (i != 0) {
                meta_list += ",";
            }
            meta_list += res.peers(i);
        }
        if (meta_list != "" && meta_list != FLAGS_meta_server_bns) {
            DB_WARNING("meta list %s change to:%s", FLAGS_meta_server_bns.c_str(), meta_list.c_str());
            if ( _meta_server_interact.reset_bns_channel(meta_list) == 0) {
                FLAGS_meta_server_bns = meta_list;
            }
        }
    }
}

void Store::vector_compact_thread() {
    while (!_shutdown) {
        TimeCost cost;
        static bvar::LatencyRecorder vector_compact_time_cost("vector_compact_time_cost");
        traverse_copy_region_map([](const SmartRegion& region) {
            if (!region->is_binlog_region()) {
                region->vector_compaction();
            }
        });
        vector_compact_time_cost << cost.get_time();
        bthread_usleep_fast_shutdown(FLAGS_reverse_merge_interval_us, _shutdown);
    }
}


void Store::reverse_merge_thread() {
    while (!_shutdown) {
        TimeCost cost;
        static bvar::LatencyRecorder reverse_merge_time_cost("reverse_merge_time_cost");
        traverse_copy_region_map([](const SmartRegion& region) {
            if (!region->is_binlog_region()) {
                region->reverse_merge();
            }
        });
        reverse_merge_time_cost << cost.get_time();
        bthread_usleep_fast_shutdown(FLAGS_reverse_merge_interval_us, _shutdown);
    }
}

void Store::unsafe_reverse_merge_thread() {
    while (!_shutdown) {
        TimeCost cost;
        static bvar::LatencyRecorder unsafe_reverse_merge_time_cost("unsafe_reverse_merge_time_cost");
        traverse_copy_region_map([](const SmartRegion& region) {
            if (!region->is_binlog_region()) {
                region->reverse_merge_doing_ddl();
            }
        });
        unsafe_reverse_merge_time_cost << cost.get_time();
        bthread_usleep_fast_shutdown(FLAGS_reverse_merge_interval_us, _shutdown);
    }  
}

void Store::ttl_remove_thread() {
    TimeCost time;
    while (!_shutdown) {
        bthread_usleep_fast_shutdown(30 * 1000 * 1000LL, _shutdown);
        if (_shutdown) {
            return;
        }
        
        traverse_copy_region_map([](const SmartRegion& region) {
            region->update_ttl_info();
        });
        // 控制ttl在ttl_remove_interval_period指定时间范围,典型是晚上流量低峰
        std::vector<std::string> periods = string_split(FLAGS_ttl_remove_interval_period, '-');
        if (periods.size() == 2) {
            try {
                int start_hour = std::stoi(periods[0]);
                int end_hour = std::stoi(periods[1]);
                if (start_hour >= 0 && start_hour <= 23 && end_hour >= 0 && end_hour <= 23) {
                    TimePeriodChecker tc(start_hour, end_hour);
                    if (!tc.now_in_interval_period()) {
                        continue;
                    }
                }
            } catch (...) {
                DB_WARNING("FLAGS_ttl_remove_interval_period %s", FLAGS_ttl_remove_interval_period.c_str());
            }
        }

        if (time.get_time() > FLAGS_ttl_remove_interval_s * 1000 * 1000LL) {
            traverse_copy_region_map([](const SmartRegion& region) {
                if (!FLAGS_stop_ttl_data) {
                    region->ttl_remove_expired_data();
                }
            });
            time.reset();
        }
    }
}

void Store::delay_remove_data_thread() {
    while (!_shutdown) {
        bthread_usleep_fast_shutdown(FLAGS_delay_remove_region_interval_s * 1000 * 1000, _shutdown);
        if (_shutdown) {
            return;
        }
        traverse_copy_region_map([this](const SmartRegion& region) {
            //加个随机数，删除均匀些
            int64_t random_remove_data_timeout = (FLAGS_region_delay_remove_timeout_s + 
                    (int64_t)(butil::fast_rand() % FLAGS_region_delay_remove_timeout_s)) * 1000 * 1000LL;
            if (region->removed() && 
                region->removed_time_cost() > random_remove_data_timeout) {
                DB_WARNING("remove data now, region_id: %ld, removed_time_cost: %ld, random_remove_data_timeout: %ld", 
                        region->get_region_id(), region->removed_time_cost(), random_remove_data_timeout);
                auto region_now = get_region(region->get_region_id());
                if (region_now != nullptr && region_now != region) {
                    // 删了之后又收到init_region rpc
                    DB_WARNING("region_id: %ld, receive init region again, do not remove data", region->get_region_id());
                    return;
                }
                region->shutdown();
                region->join();
                int64_t drop_region_id = region->get_region_id();
                DB_WARNING("region node remove permanently, region_id: %ld", drop_region_id);
                RegionControl::clear_all_infos_for_region(drop_region_id, region->get_table_id());
                erase_region(drop_region_id);
            }
        });
        traverse_copy_region_map([this](const SmartRegion& region) {
            region->remove_local_index_data();
        });
    }
}

void Store::flush_memtable_thread() {
    static bvar::Status<uint64_t> rocksdb_num_snapshots("rocksdb_num_snapshots", 0);
    static bvar::Status<int64_t> rocksdb_snapshot_difftime("rocksdb_snapshot_difftime", 0);
    static bvar::Status<uint64_t> rocksdb_num_wals("rocksdb_num_wals", 0);
    uint64_t last_file_number = 0;
    while (!_shutdown) {
        bthread_usleep_fast_shutdown(FLAGS_flush_memtable_interval_us, _shutdown);
        if (_shutdown) {
            return;
        }
        uint64_t num_snapshots = 0;
        _rocksdb->get_db()->GetAggregatedIntProperty("rocksdb.num-snapshots", &num_snapshots);
        rocksdb_num_snapshots.set_value(num_snapshots);
        uint64_t snapshot_time = 0;
        _rocksdb->get_db()->GetIntProperty(_rocksdb->get_data_handle(), "rocksdb.oldest-snapshot-time", &snapshot_time);
        if (snapshot_time > 0) {
            rocksdb_snapshot_difftime.set_value((int64_t)time(NULL) - (int64_t)snapshot_time);
        }
        rocksdb::VectorLogPtr vec;
        _rocksdb->get_db()->GetSortedWalFiles(vec);
        rocksdb_num_wals.set_value(vec.size());

        // wal 个数超过阈值100强制flush所有cf
        bool force_flush = vec.size() > FLAGS_rocks_force_flush_max_wals ? true : false;

        int64_t raft_count = RocksWrapper::raft_cf_remove_range_count.load();
        int64_t data_count = RocksWrapper::data_cf_remove_range_count.load();
        int64_t mata_count = RocksWrapper::mata_cf_remove_range_count.load();
        if (force_flush || raft_count > FLAGS_rocks_cf_flush_remove_range_times) {
            RocksWrapper::raft_cf_remove_range_count = 0;
            rocksdb::FlushOptions flush_options;
            auto status = _rocksdb->flush(flush_options, _rocksdb->get_raft_log_handle());
            if (!status.ok()) {
                DB_WARNING("flush log_cf to rocksdb fail, err_msg:%s", status.ToString().c_str());
            }
        }
        if (force_flush || data_count > FLAGS_rocks_cf_flush_remove_range_times) {
            RocksWrapper::data_cf_remove_range_count = 0;
            rocksdb::FlushOptions flush_options;
            auto status = _rocksdb->flush(flush_options, _rocksdb->get_data_handle());
            if (!status.ok()) {
                DB_WARNING("flush data to rocksdb fail, err_msg:%s", status.ToString().c_str());
            }
        }
        //last_file_number发生变化，data cf有新的flush数据需要flush meta，gc wal
        if (force_flush || mata_count > FLAGS_rocks_cf_flush_remove_range_times || last_file_number != _rocksdb->flush_file_number()) {
            last_file_number = _rocksdb->flush_file_number();
            RocksWrapper::mata_cf_remove_range_count = 0;
            rocksdb::FlushOptions flush_options;
            auto status = _rocksdb->flush(flush_options, _rocksdb->get_meta_info_handle());
            if (!status.ok()) {
                DB_WARNING("flush mata to rocksdb fail, err_msg:%s", status.ToString().c_str());
            }
        }

        if (_has_binlog_region && force_flush) {
            rocksdb::FlushOptions flush_options;
            auto status = _rocksdb->flush(flush_options, _rocksdb->get_bin_log_handle());
            if (!status.ok()) {
                DB_WARNING("flush bin_log_cf to rocksdb fail, err_msg:%s", status.ToString().c_str());
            }
        }
    }
}

void Store::cold_region_flush_thread() {
    while (!_shutdown) {
        TimeCost time;
        // 后续可以使用令牌桶精细控制速度 OLAPTODO
        int32_t concurrency = FLAGS_cold_region_flush_concurrency;
        BthreadCond concurrency_cond(-concurrency);
        std::vector<SmartRegion> regions;
        if (!FLAGS_stop_cold_region_flush && _rocksdb->has_init_cold_rocksdb()) {
            traverse_copy_region_map([&regions](const SmartRegion& region) {
                if (region->need_flush_to_cold_rocksdb()) {
                    regions.emplace_back(region);
                }
            });
        }

        for (SmartRegion region : regions) {
            auto flush_func = [&concurrency_cond, region] {
                region->flush_to_cold_rocksdb();
                concurrency_cond.decrease_signal();
            };
            Bthread bth; 
            concurrency_cond.increase_wait();
            if (_shutdown) {
                break;
            }
            bth.run(flush_func);
            if (concurrency != FLAGS_cold_region_flush_concurrency) {
                // 外部修改了并发，及时跳出，下一轮使用新的并发度
                break;
            }
        }

        concurrency_cond.wait(-concurrency);
        if (time.get_time() > 10 * 1000 * 1000LL) {
            // 已经超过定时周期直接继续
            continue;
        }
        bthread_usleep_fast_shutdown(10 * 1000 * 1000, _shutdown);
    }
}

void Store::cold_region_check() {
    std::vector<SmartRegion> regions;
    traverse_copy_region_map([&regions](const SmartRegion& region) {
        if (region->olap_state() > pb::OLAP_IMMUTABLE) {
            regions.emplace_back(region);
        }
    });

    std::vector<rocksdb::LiveFileMetaData> metadata;
    _rocksdb->get_cold_live_files(&metadata);
    std::map<int64_t, std::set<std::string>> region_sst_map;
    for (const auto& md : metadata) {
        print_metadata_info(md);
        if (md.column_family_name != RocksWrapper::COLD_DATA_CF) {
            continue;
        }
        TableKey smallestkey(md.smallestkey);
        TableKey largestkey(md.largestkey);
        int64_t smallest_region_id = smallestkey.extract_i64(0);
        int64_t largest_region_id  = largestkey.extract_i64(0);
        if (smallest_region_id != largest_region_id) {
            // 报警
            DB_FATAL("cold sst: %s has diff region_id %ld vs %ld", md.relative_filename.c_str(), smallest_region_id, largest_region_id);
            continue;
        }
        region_sst_map[smallest_region_id].insert(md.relative_filename);
    }

    std::map<std::string, SstExtLinker::ExtFileInfo> sst_ext_map;
    SstExtLinker::get_instance()->sst_ext_map(sst_ext_map);
    DB_WARNING("region cnt in sst: %ld, in store: %ld rocksdb sst cnt: %ld, ext sst cnt: %ld", 
            region_sst_map.size(), regions.size(), metadata.size(), sst_ext_map.size());
    for (SmartRegion region : regions) {
        pb::OlapRegionInfo olap_info;
        int ret = _meta_writer->read_olap_info(region->get_region_id(), olap_info);
        if (ret < 0) {
            DB_WARNING("region_id: %ld read olap info failed", region->get_region_id());
            continue;
        }

        if (olap_info.state() < pb::OLAP_FLUSHED || olap_info.external_full_path_size() <= 0) {
            DB_WARNING("region_id: %ld diff state info: %s", region->get_region_id(), olap_info.ShortDebugString().c_str());
            continue;
        }

        std::set<std::string> rocks_external_files;
        uint64_t total_size = 0;
        uint64_t total_lines = 0;
        for (const auto& f : olap_info.external_full_path()) {
            rocks_external_files.emplace(f);
            uint64_t size;
            uint64_t lines;
            int ret = get_size_by_external_file_name(&size, &lines, f);
            if (ret != 0) {
                continue;
            }
            total_size += size;
            total_lines += lines;
        }
        
        int64_t index_lines = 0;
        if (olap_info.olap_index_info_list_size() > 0) {
            for (const auto& info : olap_info.olap_index_info_list()) {
                if (region->get_table_id() == info.index_id()) {
                    continue;
                }
                for (const auto& f : info.external_path()) {
                    uint64_t size;
                    uint64_t lines;
                    int ret = get_size_by_external_file_name(&size, &lines, f);
                    if (ret != 0) {
                        continue;
                    }
                    index_lines += lines;
                }
            }
        }

        if (region->column_status() == pb::CS_COLD) {
            int64_t column_lines = region->column_lines();
            if (total_lines != column_lines && (total_lines - index_lines) != column_lines) {
                DB_COLUMN_FATAL("region_id: %ld diff lines row vs column total_lines:%lu, index_lines:%ld, column_lines:%ld", 
                    region->get_region_id(), total_lines, index_lines, column_lines);
            }
        }

        // 更新行数和大小
        region->set_used_size(total_size);
        if (total_lines != region->get_num_table_lines()) {
            region->set_num_table_lines(total_lines);
        }

        auto iter = region_sst_map.find(region->get_region_id());
        if (iter == region_sst_map.end()) {
            DB_FATAL("region_id: %ld cant find cold sst, olap_info: %s", region->get_region_id(), olap_info.ShortDebugString().c_str());
            continue;
        }

        if (rocks_external_files.size() != iter->second.size()) {
            DB_FATAL("region_id: %ld diff size rocks vs json", region->get_region_id());
            continue;
        }

        std::set<std::string> json_external_files;
        for (const std::string& f : iter->second) {
            auto it = sst_ext_map.find(f);
            if (it == sst_ext_map.end()) {
                DB_FATAL("region_id: %ld cant find sst: %s", region->get_region_id(), f.c_str());
            } else {
                json_external_files.emplace(it->second.full_name);
            }
        }

        std::set<std::string> diff;
        std::set_difference(rocks_external_files.begin(), rocks_external_files.end(), 
                            json_external_files.begin(), json_external_files.end(),
                            std::inserter(diff, diff.begin()));

        if (!diff.empty()) {
            DB_FATAL("region_id: %ld diff sst rocks vs json", region->get_region_id());
        }

    }
}

void Store::hot_region_check() {
    // 使用bvar来统计region size的分位值
    static bvar::LatencyRecorder region_size("region_size");
    std::map<int64_t, SmartRegion> regions;
    traverse_copy_region_map([&regions](const SmartRegion& region) {
        if (region->olap_state() == pb::OLAP_ACTIVE) {
            regions[region->get_region_id()] = region;
        }
    });

    std::vector<rocksdb::LiveFileMetaData> metadata;
    _rocksdb->get_db()->GetLiveFilesMetaData(&metadata);
    std::map<int64_t, int64_t> region_size_map;
    std::map<int64_t, EstimateLines> tmp_region_lines;
    for (const auto& md : metadata) {
        if (md.column_family_name == RocksWrapper::DATA_CF) {
            print_metadata_info(md);
        }
        if (md.column_family_name == RocksWrapper::DATA_CF && md.level == 6) {
            TableKey smallestkey(md.smallestkey);
            TableKey largestkey(md.largestkey);
            int64_t smallest_region_id = smallestkey.extract_i64(0);
            int64_t largest_region_id  = largestkey.extract_i64(0);
            if (smallest_region_id != largest_region_id) {
                region_size_map[smallest_region_id] += md.size;
                region_size_map[largest_region_id]  += md.size;
            } else {
                region_size_map[smallest_region_id] += md.size;
            }

            if (smallest_region_id == largest_region_id) {
                if (regions.count(smallest_region_id) <= 0) {
                    continue;
                }
                auto region = regions[smallest_region_id];
                std::string left_key;
                std::string right_key;
                if (md.smallestkey.size() > 16) {
                    left_key = md.smallestkey.substr(sizeof(int64_t) * 2);
                }
                if (md.largestkey.size() > 16) {
                    right_key = md.largestkey.substr(sizeof(int64_t) * 2);
                }
                if (left_key >= region->get_start_key() && end_key_compare(right_key, region->get_end_key()) < 0) {
                    auto iter = tmp_region_lines.find(smallest_region_id);
                    if (iter == tmp_region_lines.end()) {
                        EstimateLines lines;
                        lines.region_version = region->get_version();
                        lines.region_lines   = md.num_entries;
                    } else {
                        iter->second.region_lines += md.num_entries;
                    }
                }
            }
        }
    }

    {
        std::unique_lock<bthread::Mutex> l(_lock);
        _region_lines.swap(tmp_region_lines);
    }

    for (const auto& iter : region_size_map) {
        region_size << iter.second;
        if (iter.second > FLAGS_region_size_alarm_threshold_G * 1024 * 1024 * 1024LL) {
            auto it = regions.find(iter.first);
            if (it != regions.end()) {
                DB_WARNING("table_id: %ld region_id: %ld size: %ld too big", it->second->get_table_id(), iter.first, iter.second);
            }
        }
    }

}

void Store::get_hot_region_size(std::map<int64_t, int64_t>& region_size_map) {
    std::vector<rocksdb::LiveFileMetaData> metadata;
    _rocksdb->get_db()->GetLiveFilesMetaData(&metadata);
    for (const auto& md : metadata) {
        if (md.column_family_name == RocksWrapper::DATA_CF && md.level == 6) {
            TableKey smallestkey(md.smallestkey);
            TableKey largestkey(md.largestkey);
            int64_t smallest_region_id = smallestkey.extract_i64(0);
            int64_t largest_region_id  = largestkey.extract_i64(0);
            if (smallest_region_id != largest_region_id) {
                region_size_map[smallest_region_id] += md.size;
                region_size_map[largest_region_id]  += md.size;
            } else {
                region_size_map[smallest_region_id] += md.size;
            }
        }
    }
}

void Store::column_minor_compact_thread() {
    while (!_shutdown) {
        TimeCost time;
        int32_t concurrency = FLAGS_column_minor_compact_concurrency;
        BthreadCond concurrency_cond(-concurrency);
        std::vector<SmartRegion> regions;
        traverse_copy_region_map([&regions](const SmartRegion& region) {
            if (region->can_do_column_compact() && !region->is_shutdown()) {
                regions.emplace_back(region);
            }
        });

        for (SmartRegion region : regions) {
            auto flush_func = [&concurrency_cond, region] {
                region->column_minor_compact();
                region->column_snapshot_save();
                concurrency_cond.decrease_signal();
            };
            Bthread bth; 
            concurrency_cond.increase_wait();
            bth.run(flush_func);
            if (concurrency != FLAGS_column_minor_compact_concurrency) {
                // 外部修改了并发，及时跳出，下一轮使用新的并发度
                break;
            }
            if (_shutdown) {
                break;
            }
        }

        concurrency_cond.wait(-concurrency);
        if (time.get_time() > 10 * 1000 * 1000LL) {
            // 已经超过定时周期直接继续
            continue;
        }
        bthread_usleep_fast_shutdown(10 * 1000 * 1000, _shutdown);
    }
}

void Store::column_major_compact_thread() {
    while (!_shutdown) {
        TimeCost time;
        int32_t concurrency = FLAGS_column_major_compact_concurrency;
        BthreadCond concurrency_cond(-concurrency);
        std::vector<SmartRegion> regions;
        traverse_copy_region_map([&regions](const SmartRegion& region) {
            if (region->can_do_column_compact() && !region->is_shutdown()) {
                regions.emplace_back(region);
            }
        });

        for (SmartRegion region : regions) {
            auto flush_func = [&concurrency_cond, region] {
                region->column_major_compact(false);
                region->column_major_compact(true);
                concurrency_cond.decrease_signal();
            };
            Bthread bth; 
            concurrency_cond.increase_wait();
            bth.run(flush_func);
            if (concurrency != FLAGS_column_major_compact_concurrency) {
                // 外部修改了并发，及时跳出，下一轮使用新的并发度
                break;
            }
            if (_shutdown) {
                break;
            }
        }

        concurrency_cond.wait(-concurrency);
        if (time.get_time() > 10 * 1000 * 1000LL) {
            // 已经超过定时周期直接继续
            continue;
        }
        bthread_usleep_fast_shutdown(10 * 1000 * 1000, _shutdown);
    }
}

void Store::column_flush_thread() {
    while (!_shutdown) {
        TimeCost time;
        int32_t concurrency = FLAGS_column_flush_concurrency;
        BthreadCond concurrency_cond(-concurrency);
        std::vector<SmartRegion> regions;
        traverse_copy_region_map([&regions](const SmartRegion& region) {
            if (!region->is_shutdown()) {
                regions.emplace_back(region);
            }
        });

        for (SmartRegion region : regions) {
            auto flush_func = [&concurrency_cond, region] {
                region->column_flush();
                concurrency_cond.decrease_signal();
            };
            Bthread bth; 
            concurrency_cond.increase_wait();
            bth.run(flush_func);
            if (concurrency != FLAGS_column_flush_concurrency) {
                // 外部修改了并发，及时跳出，下一轮使用新的并发度
                break;
            }
            if (_shutdown) {
                break;
            }
        }

        concurrency_cond.wait(-concurrency);
        if (time.get_time() > 10 * 1000 * 1000LL) {
            // 已经超过定时周期直接继续
            continue;
        }
        bthread_usleep_fast_shutdown(10 * 1000 * 1000, _shutdown);
    }
}

void Store::binlog_region_backup_thread() {
    while (!_shutdown) {
        bthread_usleep_fast_shutdown(3600 * 1000 * 1000ULL, _shutdown);
        
        std::vector<SmartRegion> regions;
        std::vector<SmartRegion> need_clear_offline_binlog_regions;
        if (!FLAGS_stop_cold_region_flush && _rocksdb->has_init_cold_rocksdb()) {
            traverse_copy_region_map([&regions, &need_clear_offline_binlog_regions](const SmartRegion& region) {
                if (region->get_binlog_backup_days() > 0) {
                    regions.emplace_back(region);
                }
                if (region->need_clear_offline_binlog_sst()) {
                    need_clear_offline_binlog_regions.emplace_back(region);
                }
            });
        }
        for (SmartRegion region : regions) {
            if (region == nullptr) {
                continue;
            }
            region->do_backup_binlog();
        }
        // ttl afs file
        for (SmartRegion region : regions) {
            if (region == nullptr) {
                continue;
            }
            region->delete_remote_expired_file();
        }
        for (SmartRegion region : need_clear_offline_binlog_regions) {
            if (region == nullptr) {
                continue;
            }
            // binlog表关闭了离线备份,直接删除所有的cold rocksdb link,重置时间戳范围[0,0]
            region->clear_offline_binlog(0, 0);
        }
    }
}

void Store::olap_region_check_thread() {
    while (!_shutdown) {
        cold_region_check();
        hot_region_check();
        bthread_usleep_fast_shutdown(1800 * 1000 * 1000LL, _shutdown);
    }
}

void Store::snapshot_thread() {
    BthreadCond concurrency_cond(-5); // -n就是并发跑n个bthread
    while (!_shutdown) {
        traverse_copy_region_map([&concurrency_cond](const SmartRegion& region) {
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
    int64_t count = 0;
    while (!_shutdown) {
        traverse_copy_region_map([](const SmartRegion& region) {
            // clear prepared and expired transactions
            region->clear_transactions();
        });
        bthread_usleep_fast_shutdown(FLAGS_transaction_clear_interval_ms * 1000, _shutdown);
        DB_WARNING("txn_clear_thread, count:%ld", ++count);

    }
}

void Store::binlog_timeout_check_thread() {
    while (!_shutdown) {
        traverse_copy_region_map([this](const SmartRegion& region) {
            if (region->is_binlog_region() && !region->is_shutdown()) {
                region->binlog_timeout_check();
            }
        });
        bthread_usleep_fast_shutdown(FLAGS_binlog_timeout_check_ms * 1000, _shutdown);
    }
}

void Store::binlog_fake_thread() {
    TimeCost time;
    while (!_shutdown) {
        BthreadCond cond(-40);
        traverse_copy_region_map([this, &cond](const SmartRegion& region) {
            if (region->is_binlog_region() && region->is_leader()) {
                int64_t ts = get_tso();
                if (ts < 0) {
                    return;
                }
                region->binlog_fake(ts, cond);
            }
        });

        cond.wait(-40);
        if (time.get_time() > FLAGS_oldest_binlog_ts_interval_s * 1000 * 1000LL) {
            // 更新oldest ts
            RocksWrapper::get_instance()->update_oldest_ts_in_binlog_cf();
            time.reset();
        }
        bthread_usleep_fast_shutdown(FLAGS_binlog_fake_ms * 1000, _shutdown);
    }
}

int64_t Store::get_tso() {
    _get_tso_cond.increase_wait();
    ON_SCOPE_EXIT(([this]() {
        _get_tso_cond.decrease_signal();
    }));

    if (tso_physical != 0 && tso_logical != 0 && tso_count > 0 && gen_tso_time.get_time() < FLAGS_gen_tso_interval_us) {
        return (tso_physical << tso::logical_bits) + tso_logical + FLAGS_gen_tso_count - (tso_count--);
    }

    pb::TsoRequest request;
    pb::TsoResponse response;
    request.set_op_type(pb::OP_GEN_TSO);
    request.set_count(FLAGS_gen_tso_count);

    //发送请求，收到响应
    if (_tso_server_interact.send_request("tso_service", request, response) == 0) {
        //处理响应
        if (response.errcode() != pb::SUCCESS) {
            DB_FATAL("store get tso fail request:%s, response:%s", 
                       request.ShortDebugString().c_str(), 
                       response.ShortDebugString().c_str());            
            return -1;
        }
        DB_WARNING("store get tso request:%s, response:%s", 
                   request.ShortDebugString().c_str(), response.ShortDebugString().c_str());
    } else {
        DB_WARNING("store get tso request:%s, response:%s", 
                   request.ShortDebugString().c_str(), response.ShortDebugString().c_str());
        return -1;
    }

    gen_tso_time.reset();
    tso_count = FLAGS_gen_tso_count;
    tso_physical = response.start_timestamp().physical();
    tso_logical  = response.start_timestamp().logical();
    return (tso_physical << tso::logical_bits) + tso_logical + FLAGS_gen_tso_count - (tso_count--);
}

int64_t Store::get_last_commit_ts() {
    pb::TsoRequest request;
    pb::TsoResponse response;
    request.set_op_type(pb::OP_GEN_TSO);
    request.set_count(1);

    //发送请求，收到响应
    if (_tso_server_interact.send_request("tso_service", request, response) == 0) {
        //处理响应
        if (response.errcode() != pb::SUCCESS) {
            DB_FATAL("store get tso fail request:%s, response:%s", 
                       request.ShortDebugString().c_str(), 
                       response.ShortDebugString().c_str());            
            return -1;
        }
        DB_WARNING("store get tso request:%s, response:%s", 
                   request.ShortDebugString().c_str(), response.ShortDebugString().c_str());
    } else {
        DB_WARNING("store get tso request:%s, response:%s", 
                   request.ShortDebugString().c_str(), response.ShortDebugString().c_str());
        return -1;
    }
    gen_tso_time.reset();
    auto&  tso = response.start_timestamp();
    int64_t timestamp = (tso.physical() << tso::logical_bits) + tso.logical();
    return timestamp;
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
    region_merge->set_partition_id(ptr_region->get_partition_id());

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

void Store::process_split_request(int64_t table_id, int64_t region_id, bool tail_split, const std::string& split_key, int64_t key_term) {
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
        int region_num = _factory->get_tail_split_nums(table_id);
        region_split->set_new_region_num(region_num);
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
        region->start_process_split(response.split_response(), tail_split, split_key, key_term);
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
    uint64_t value_bin_log = 0;
    db->GetIntProperty(_rocksdb->get_data_handle(), name, &value_data);
    db->GetIntProperty(_rocksdb->get_raft_log_handle(), name, &value_log);
    db->GetIntProperty(_rocksdb->get_bin_log_handle(), name, &value_bin_log);
    DB_WARNING("db_property: %s, data_cf:%lu, log_cf:%lu, binlog:%lu", name.c_str(), value_data, value_log, value_bin_log);
}

void Store::monitor_memory() {
    
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
        if (_rocksdb->get_cache() != nullptr) {
            DB_WARNING("momery type: %d, size: %lu, %lu", 
                kv.first, kv.second, _rocksdb->get_cache()->GetPinnedUsage());
        }
    }
    
}

void Store::check_region_peer_delay() {
    while (!_shutdown) {
        TimeCost t;
        std::vector<int64_t> region_ids;
        traverse_region_map([&region_ids](const SmartRegion& region) {
            region_ids.emplace_back(region->get_region_id());
        });
        for (size_t i = 0; i < region_ids.size(); ++i) {
            SmartRegion ptr_region = get_region(region_ids[i]);
            if (ptr_region != nullptr) {
                ptr_region->check_peer_latency();
            }
        }
        DB_WARNING("finish check_region_peer_delay, cost: %ld", t.get_time());
        bthread_usleep_fast_shutdown(FLAGS_check_peer_delay_min * 60 * 1000 * 1000ULL, _shutdown);
    }
}

void Store::whether_split_thread() {
    static int64_t count = 0;
    (void)count;
    while (!_shutdown) {
        std::vector<int64_t> region_ids;
        traverse_region_map([&region_ids](const SmartRegion& region) {
            region_ids.push_back(region->get_region_id());
        });
        boost::scoped_array<uint64_t> region_sizes(new(std::nothrow)uint64_t[region_ids.size()]);
        int ret = get_used_size_per_region(region_ids, region_sizes.get());
        if (ret != 0) {
            DB_WARNING("get used size per region fail");
            return; 
        }
        if (count % 100 == 0) {
            std::map<uint64_t, int64_t> cp_size;
            for (size_t i = 0; i < region_ids.size(); i++) {
                cp_size[region_sizes[i]] = region_ids[i];
            }
            int i = 0;
            for (auto iter = cp_size.rbegin(); iter != cp_size.rend() && i < 10; iter++, i++) {
                DB_NOTICE("top_10_size, size: %lu, region_id: %ld", iter->first, iter->second);
            }

        }
        for (size_t i = 0; i < region_ids.size(); ++i) {
            SmartRegion ptr_region = get_region(region_ids[i]);
            if (ptr_region == NULL) {
                continue;
            }
            if (ptr_region->is_binlog_region() || ptr_region->is_learner()) {
                continue;
            }
            if (ptr_region->removed()) {
                //DB_WARNING("region_id: %ld has be removed", region_ids[i]);
                continue;
            }

            //判断是否需要分裂，分裂的标准是used_size > 1.5 * region_capacity
            int64_t region_capacity = 10000000;
            int ret = _factory->get_region_capacity(ptr_region->get_global_index_id(), region_capacity);
            if (ret != 0) {
                DB_DEBUG("table info not exist, region_id: %ld", region_ids[i]);
                continue;
            }
            region_capacity = std::max(FLAGS_min_split_lines, region_capacity);

            bool is_cold = false;
            bool is_olap_table = _factory->is_olap_table(ptr_region->get_table_id(), ptr_region->get_partition_id(), &is_cold);
            //设置计算存储分离开关
            if (is_olap_table) {
                // olap表默认开启kv分离
                ptr_region->set_separate_switch(true);
                if (is_cold) {
                    continue;
                }
                if (ptr_region->get_num_table_lines() < region_capacity) {
                    continue;
                }
                int ret = ptr_region->modify_olap_region_num_table_lines();
                if (ret < 0) {
                    continue;
                }
                if (ptr_region->get_num_table_lines() < region_capacity) {
                    continue;
                }
                if (!FLAGS_olap_region_split_enable) {
                    continue;
                }
            } else {
                ptr_region->set_separate_switch(_factory->get_separate_switch(ptr_region->get_table_id()));
            }
            //update region_used_size
            ptr_region->set_used_size(region_sizes[i]);

            if (_factory->is_in_fast_importer(ptr_region->get_table_id())) {
                DB_DEBUG("region_id: %ld is in fast importer", region_ids[i]);
                continue;
            }
            if (ptr_region->olap_state() != pb::OLAP_ACTIVE) {
                DB_WARNING("region_id: %ld is not active", ptr_region->get_region_id());
                continue;
            }
            //分区region，不分裂、不merge
            //if (ptr_region->get_partition_num() > 1) {
            //    DB_NOTICE("partition region %ld not split.", region_ids[i]);
            //    continue;
            //}
            
            //DB_WARNING("region_id: %ld, split_capacity: %ld", region_ids[i], region_capacity);
            std::string split_key;
            int64_t split_key_term = 0;
            //如果是尾部分
            if (ptr_region->is_leader() 
                    && ptr_region->get_status() == pb::IDLE
                    && _split_num.load() < FLAGS_max_split_concurrency) {
                if (ptr_region->is_tail() 
                    && ptr_region->get_num_table_lines() >= region_capacity) {
                    process_split_request(ptr_region->get_global_index_id(), region_ids[i], true, split_key, split_key_term);
                    continue;
                } else if (!ptr_region->is_tail() 
                    && ptr_region->get_num_table_lines() >= FLAGS_split_threshold * region_capacity / 100) {
                    if (0 != ptr_region->get_split_key(split_key, split_key_term)) {
                        DB_WARNING("get_split_key failed: region=%ld", region_ids[i]);
                        continue;
                    }
                    process_split_request(ptr_region->get_global_index_id(), region_ids[i], false, split_key, split_key_term);
                    continue;
                } else if (ptr_region->can_use_approximate_split()) {
                    DB_WARNING("start split by approx size:%ld region_id: %ld num_table_lines:%ld",
                            region_sizes[i], region_ids[i], ptr_region->get_num_table_lines());
                    //split或add peer后，预估的空间有段时间不够准确
                    //由于已经有num_table_lines判断，region_sizes判断不需要太及时
                    //TODO split可以根据前后版本的大小进一步预估，add peer可以根据发来的sst预估
                    if (ptr_region->is_tail()) {
                        process_split_request(ptr_region->get_global_index_id(), region_ids[i], true, split_key, split_key_term);
                        continue;
                    } else {
                        if (0 != ptr_region->get_split_key(split_key, split_key_term)) {
                            DB_WARNING("get_split_key failed: region=%ld", region_ids[i]);
                            continue;
                        }
                        process_split_request(ptr_region->get_global_index_id(), region_ids[i], false, split_key, split_key_term);
                        continue;
                    }
                }
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
                    && (ptr_region->get_num_table_lines() == 0 || 
                        ptr_region->get_used_size() == 0)
                    && ptr_region->get_status() == pb::IDLE) {
                if (ptr_region->get_log_index() != ptr_region->get_log_index_lastcycle()) {
                    ptr_region->reset_log_index_lastcycle();
                    DB_WARNING("region:%ld is none, num_table_lines:%ld, used_size:%ld, log_index:%ld reset time", 
                               region_ids[i], ptr_region->get_num_table_lines(), ptr_region->get_used_size(), ptr_region->get_log_index());
                    continue;
                }
                if (ptr_region->get_log_index() == ptr_region->get_log_index_lastcycle()
                   && ptr_region->get_lastcycle_timecost() > FLAGS_none_region_merge_interval_us) {
                    DB_WARNING("region:%ld is none, num_table_lines:%ld, used_size:%ld, log_index:%ld process merge", 
                               region_ids[i], ptr_region->get_num_table_lines(), ptr_region->get_used_size(), ptr_region->get_log_index());
                    int64_t seek_table_lines = 0;
                    int64_t used_size = 0;
                    ptr_region->has_sst_data(&seek_table_lines, &used_size);
                    if (seek_table_lines > 0) {
                        ptr_region->set_num_table_lines(seek_table_lines);
                        ptr_region->set_used_size(used_size);
                        continue;
                    }
                    process_merge_request(ptr_region->get_global_index_id(), region_ids[i]);
                    continue;
                }
            }
        }
        SELF_TRACE("upate used size count:%ld", ++count);
        bthread_usleep_fast_shutdown(FLAGS_update_used_size_interval_us, _shutdown);
    }
}

void Store::compact_region_by_sst_delete_keys_thread() {
    while (!_shutdown) {
        bthread_usleep_fast_shutdown(1000ULL * 1000 * FLAGS_compact_delete_sst_keys_interval_s, _shutdown);

        if (FLAGS_compact_delete_sst_keys > 0) {
            TimeCost cost;
            int compact_sst_cnt = 0;
            rocksdb::CompactionOptions compact_options;
            compact_options.compression = rocksdb::kLZ4Compression;
            compact_options.output_file_size_limit = FLAGS_target_file_size_base;

            // 获取所有sst文件元信息, metadata里拿到的num_deletion不准
            std::vector<rocksdb::LiveFileMetaData> metadata;
            _rocksdb->get_db()->GetLiveFilesMetaData(&metadata);
            std::unordered_map<std::string, int> sst_levels;
            for (const auto& md : metadata) {
                if (md.column_family_name == RocksWrapper::DATA_CF) {
                    sst_levels[md.db_path + md.name] = md.level;
                }
            }

            rocksdb::TablePropertiesCollection props;
            MutTableKey start;
            MutTableKey end;
            start.append_u64(0);
            end.append_u64(UINT64_MAX);
            // 拿不到sst对应的level
            RocksWrapper::get_instance()->get_sst_properties(start.data(), end.data(), props);
            rocksdb::ColumnFamilyHandle* data_cf = RocksWrapper::get_instance()->get_data_handle();
            for (const auto& [sst_file_name, prop] : props) {
                if (prop == nullptr) {
                    continue;
                }
                if (FLAGS_compact_delete_sst_keys > 0
                        && prop->num_deletions > FLAGS_compact_delete_sst_keys) {
                    TimeCost cost;
                    if (sst_levels.count(sst_file_name) == 0) {
                        DB_WARNING("sst file: %s not found in sst_levels", sst_file_name.c_str());
                        continue;
                    }
                    int sst_level = sst_levels[sst_file_name];
                    int target_level = sst_level + 1;
                    if (target_level > 6) {
                        target_level = 6;
                    }
                    DB_WARNING("want compact sst file: %s, num_deletions: %ld, from level#%d to level#%d", 
                                sst_file_name.c_str(), prop->num_deletions, sst_level, target_level);
                    RocksWrapper::get_instance()->compact_files(compact_options, data_cf, {sst_file_name}, target_level);
                    compact_sst_cnt++;
                    DB_WARNING("compact sst file: %s finish, num_deletions: %ld, from level#%d to level#%d, cost: %ld", 
                                sst_file_name.c_str(), prop->num_deletions, sst_level, target_level, cost.get_time());
                }
            }
            DB_WARNING("compact_region_by_sst_delete_keys, compact_sst_cnt: %d cost: %ld", compact_sst_cnt, cost.get_time());
        }
    }
    return;
}

void Store::start_db_statistics() {
    int64_t idx = 0;
    while (!_shutdown) {
        bthread_usleep(10 * 1000 * 1000);
        if (FLAGS_store_rocks_hang_check) {
            // 每10s向meta cf写固定key，检测store是否hang了
            TimeCost cost;
            int ret = _meta_writer->rocks_hang_check();
            if (ret == 0) {
                last_rocks_hang_check_ok.reset();
                last_rocks_hang_check_cost = cost.get_time();
            }
            if (ret == 0 && last_rocks_hang_check_cost < FLAGS_store_rocks_hang_check_timeout_s * 1000 * 1000LL) {
                // 恢复正常
                rocks_hang_continues_cnt = 0;
            } else {
                // 连续不正常次数++
                rocks_hang_continues_cnt += 1;
            }
            DB_WARNING("store hang check: last_rocks_hang_check_cost: %ld, ret: %d, rocks_hang_continues_cnt: %d", 
                last_rocks_hang_check_cost, ret, rocks_hang_continues_cnt);
        }
        if (++idx < 6) {
            continue;
        }
        idx = 0;
        // 每60s进行print_properties
        TimeCost cost;
        auto db_options = get_db()->get_db_options();
        std::string str = db_options.statistics->ToString();
        std::vector<std::string> items;
        boost::split(items, str, boost::is_any_of("\n"));
        for (auto& item : items) {
            (void)item;
            DB_WARNING("statistics: %s", item.c_str());
        }
        db_options.statistics->Reset();
        monitor_memory();
        print_properties("rocksdb.num-immutable-mem-table");
        print_properties("rocksdb.mem-table-flush-pending");
        print_properties("rocksdb.compaction-pending");
        print_properties("rocksdb.estimate-pending-compaction-bytes");
        print_properties("rocksdb.num-running-compactions");
        print_properties("rocksdb.num-running-flushes");
        print_properties("rocksdb.cur-size-active-mem-table");
        print_properties("rocksdb.cur-size-all-mem-tables");
        print_properties("rocksdb.size-all-mem-tables");
        print_properties("rocksdb.estimate-table-readers-mem");
        print_properties("rocksdb.actual-delayed-write-rate");
        print_properties("rocksdb.is-write-stopped");
        print_properties("rocksdb.num-snapshots");
        print_properties("rocksdb.oldest-snapshot-time");
        print_properties("rocksdb.is-write-stopped");
        print_properties("rocksdb.num-live-versions");
        print_properties("rocksdb.block-cache-usage");
        print_properties("rocksdb.block-cache-pinned-usage");
        print_properties("rocksdb.sstables");
        auto& con = *Concurrency::get_instance();
        DB_WARNING("get properties cost: %ld, concurrency:"
                "snapshot:%d, recieve_add_peer:%d, add_peer:%d, service_write:%d, new_sign_read:%d",
                cost.get_time(), con.snapshot_load_concurrency.count(),
                con.recieve_add_peer_concurrency.count(), con.add_peer_concurrency.count(),
                con.service_write_concurrency.count(), con.new_sign_read_concurrency.count());

        // 获取和打印level0数量
        uint64_t level0_ssts = 0;
        uint64_t pending_compaction_size = 0;
        int ret = RocksWrapper::get_instance()->get_rocks_statistic(level0_ssts, pending_compaction_size);
        if (ret < 0) {
            DB_WARNING("get_rocks_statistic failed");
        }
        DB_WARNING("level0: %lu, compaction: %lu", level0_ssts, pending_compaction_size);
    }
}

int Store::get_used_size_per_region(const std::vector<int64_t>& region_ids, uint64_t* region_sizes) {

    std::vector<rocksdb::Range> ranges;
    std::vector<uint64_t> approx_sizes;

    for (size_t i = 0; i < region_ids.size(); ++i) {
        auto region = get_region(region_ids[i]);
        if (region == NULL) {
            DB_WARNING("region_id: %ld not exist", region_ids[i]);
            region_sizes[i] = 0;
            continue;
        }

        region_sizes[i] = region->get_approx_size();
        if (!FLAGS_use_approximate_size) {
            region_sizes[i] = 100000000;
        }
        if (region_sizes[i] == UINT64_MAX) {
            ranges.emplace_back(region->get_rocksdb_range());
            approx_sizes.emplace_back(UINT64_MAX);
        }
    }

    auto data_cf = _rocksdb->get_data_handle();
    if (nullptr == data_cf) {
        return -1;
    }
    if (!ranges.empty() && approx_sizes.size() == ranges.size()) {
#if ROCKSDB_MAJOR >= 7
        _rocksdb->get_db()->GetApproximateSizes(data_cf, &ranges[0], ranges.size(), &approx_sizes[0], 
                rocksdb::DB::SizeApproximationFlags::INCLUDE_MEMTABLES | rocksdb::DB::SizeApproximationFlags::INCLUDE_FILES);
#else
        _rocksdb->get_db()->GetApproximateSizes(data_cf, &ranges[0], ranges.size(), &approx_sizes[0], 
                rocksdb::DB::SizeApproximationFlags::INCLUDE_MEMTABLES | rocksdb::DB::SizeApproximationFlags::INCLUDE_FILES);
#endif
        size_t idx = 0;
        for (size_t i = 0; i < region_ids.size(); ++i) {
            if (region_sizes[i] == UINT64_MAX && idx < approx_sizes.size()) {
                auto region = get_region(region_ids[i]);
                if (region == NULL) {
                    DB_WARNING("region_id: %ld not exist", region_ids[i]);
                    region_sizes[i] = 0;
                    continue;
                }
                region_sizes[i] = approx_sizes[idx++];
                region->set_approx_size(region_sizes[i]);
                DB_NOTICE("region_id: %ld, size:%lu region_num_line:%ld", 
                        region_ids[i], region_sizes[i], region->get_num_table_lines());
            }
        }
    }
    return 0;
}

void Store::update_schema_info(const pb::SchemaInfo& table, 
                               std::map<int64_t, std::set<int64_t>>* reverse_index_map,
                               std::unordered_set<int64_t>* vector_table_set) {
    //锁住的是update_table和table_info_mapping, table_info锁的位置不能改
    _factory->update_table(table);
    if (table.has_deleted() && table.deleted()) {
        return;
    }
    for (size_t idx = 0; idx < table.indexs_size(); ++idx) {
        const pb::IndexInfo& index_info = table.indexs(idx);
        if (index_info.index_type() == pb::I_FULLTEXT && index_info.state() != pb::IS_PUBLIC) {
            if (reverse_index_map != nullptr) {
                (*reverse_index_map)[table.table_id()].emplace(index_info.index_id());
            }
        }
        if (index_info.index_type() == pb::I_VECTOR) {
            if (vector_table_set != nullptr) {
                vector_table_set->emplace(table.table_id());
            }
        }
    }
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

void Store::construct_heart_beat_request(
        pb::StoreHeartBeatRequest& request, const std::vector<pb::RegionInfo>* region_infos) {
    static int64_t count = 0;
    request.set_need_leader_balance(false);
    ++count;
    bool need_peer_balance = false;
    if (count % FLAGS_balance_periodicity == 0) {
        request.set_need_leader_balance(true);
    }
    // init_before_listen时会上报一次心跳
    // 重启后第二次心跳或长时间心跳未上报时上报peer信息，保证无效region尽快shutdown
    if (count == 2 || _last_heart_time.get_time() > FLAGS_store_heart_beat_interval_us * 4) {
        need_peer_balance = true;
    }
    if (count % FLAGS_balance_periodicity == (FLAGS_balance_periodicity / 2)) {
        request.set_need_peer_balance(true);
        need_peer_balance = true;
    }
    //构造instance信息
    pb::InstanceInfo* instance_info = request.mutable_instance_info();
    // init_before_listen的心跳不加入meta
    // 解决迁移后老实例没删掉问题
    if (count == 1) {
        instance_info->set_address("");
    } else {
        instance_info->set_address(_address);
    }
    instance_info->set_physical_room(_physical_room);
    instance_info->set_resource_tag(_resource_tag);
    instance_info->set_dml_latency(dml_time_cost.latency(60));
    instance_info->set_dml_qps(dml_time_cost.qps(60));
    instance_info->set_raft_total_latency(raft_total_cost.latency(60));
    instance_info->set_raft_total_qps(raft_total_cost.qps(60));
    instance_info->set_select_latency(select_time_cost.latency(60));
    instance_info->set_select_qps(select_time_cost.qps(60));
    instance_info->set_network_segment(FLAGS_network_segment);
    instance_info->set_container_id(FLAGS_container_id);
    int64_t rocks_hang_check_cost = 0;
    if (FLAGS_store_rocks_hang_check && count > 2) {
        if (last_rocks_hang_check_ok.get_time() > 30 * 1000 * 1000LL) {
            // hang check卡住了, 状态置为slow
            rocks_hang_check_cost = FLAGS_store_rocks_hang_check_timeout_s * 1000 * 1000LL;
        } else {
            // hang check调度正常，连续三次测试读写都超时，才认为是slow
            if (last_rocks_hang_check_cost >= FLAGS_store_rocks_hang_check_timeout_s * 1000 * 1000LL 
                && rocks_hang_continues_cnt >= FLAGS_store_rocks_hang_cnt_limit) {
                rocks_hang_check_cost = last_rocks_hang_check_cost;
            }
        }
        if (rocks_hang_check_cost >= FLAGS_store_rocks_hang_check_timeout_s * 1000 * 1000LL) {
            // 报警
            DB_WARNING("store rocks hang, last_check_ok_time: %ld, last_check_ok_cost: %ld", 
                last_rocks_hang_check_ok.get_time(), last_rocks_hang_check_cost);
        }
    }
    instance_info->set_rocks_hang_check_cost(rocks_hang_check_cost);
    // 读取硬盘参数
    struct statfs sfs;
    statfs(FLAGS_db_path.c_str(), &sfs);
    int64_t disk_capacity = sfs.f_blocks * sfs.f_bsize;
    int64_t left_size = sfs.f_bavail * sfs.f_bsize;
    _disk_total.set_value(disk_capacity);
    _disk_used.set_value(disk_capacity - left_size);
    instance_info->set_capacity(disk_capacity);
    instance_info->set_used_size(disk_capacity - left_size);
#ifdef BAIKALDB_REVISION
    instance_info->set_version(BAIKALDB_REVISION);
#endif

    //构造schema version信息
    std::unordered_map<int64_t, int64_t> table_id_version_map;
    _factory->get_all_table_version(table_id_version_map);
    for (auto table_info : table_id_version_map) { 
        pb::SchemaHeartBeat* schema = request.add_schema_infos();
        schema->set_table_id(table_info.first);
        schema->set_version(table_info.second);
    }

    if (count == 1 && region_infos != nullptr) {
        instance_info->set_init_address(_address);
        for (const auto& region_info : *region_infos) {
            int64_t region_id = region_info.region_id();
            bool is_learner = _meta_writer->read_learner_key(region_id) ==  1 ? true : false;
            int64_t applied_index = 0;
            int64_t data_index = 0;
            _meta_writer->read_applied_index(region_id, &applied_index, &data_index);
            Region::add_peer_info(request, region_info, applied_index, is_learner, true);
        }
    }

    //构造所有region的version信息
    traverse_copy_region_map([&request, need_peer_balance, this](const SmartRegion& region) {
        region->construct_heart_beat_request(request, need_peer_balance, _meta_need_report);
    });

}

int64_t Store::get_region_estimate_lines(int64_t region_id, int64_t region_version) {
    std::unique_lock<bthread::Mutex> l(_lock);
    auto iter = _region_lines.find(region_id);
    if (iter != _region_lines.end()) {
        if (iter->second.region_version == region_version) {
            return iter->second.region_lines;
        } else {
            DB_WARNING("region_id: %ld lines: %ld diff version %ld vs %ld", iter->first, iter->second.region_lines,
                iter->second.region_version, region_version);
            return -1;
        }
    }

    return 0;
}

void Store::process_heart_beat_response(const pb::StoreHeartBeatResponse& response) {
    {
        std::unique_lock<bthread::Mutex> l(_lock);
        std::unordered_map<std::string, std::string> rocks_options;
        for (auto& param : response.instance_params()) {
            for (auto& item : param.params()) {
                if (!item.is_meta_param()) {
                    _param_map[item.key()] = item.value();
                }
            }
        }
        // 更新到qos param
        for (auto& iter : _param_map) {
            update_param(iter.first, iter.second);
        }
        RocksWrapper::get_instance()->adjust_option(_param_map);
    }
    _meta_need_report = response.need_report();
    std::unordered_set<int64_t> vector_table_set;
    std::map<int64_t, std::set<int64_t>> reverse_index_map;
    for (auto& schema_info : response.schema_change_info()) {
        update_schema_info(schema_info, &reverse_index_map, &vector_table_set);
    }
    if (!reverse_index_map.empty()) {
        traverse_copy_region_map([this, &reverse_index_map](const SmartRegion& region) {
            if (!region->removed()) {
                auto iter = reverse_index_map.find(region->get_table_id());
                if (iter != reverse_index_map.end()) {
                    region->add_reverse_index(iter->first, iter->second);
                }
            }
        });
    }
    if (!vector_table_set.empty()) {
        traverse_copy_region_map([this, &vector_table_set](const SmartRegion& region) {
            if (!region->removed()) {
                auto iter = vector_table_set.find(region->get_table_id());
                if (iter != vector_table_set.end()) {
                    region->vector_schema_change();
                }
            }
        });
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
            region->transfer_leader(transfer_leader_request, region, _transfer_leader_queue); 
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
        auto ret = region->transfer_leader(transfer_leader_request, region, _transfer_leader_queue);
        if (ret == 0) {
            table_trans_leader_count[table_id]--;
        }
    }
    const auto& delete_region_ids = response.delete_region_ids();
    std::vector<RemoveQueueItem> remove_queue_items;
    remove_queue_items.reserve(delete_region_ids.size());
    for (auto& delete_region_id : delete_region_ids) {
        if (_shutdown) {
            return;
        }
        SmartRegion region = get_region(delete_region_id);
        if (region == nullptr) {
            continue;
        }
        remove_queue_items.emplace_back(RemoveQueueItem(region->region_uuid(), region->get_region_id()));
        DB_WARNING("receive delete region response from meta server heart beat, delete_region_id:%ld uuid:%lu",
                delete_region_id, region->region_uuid());
    }

    auto remove_func = [this, remove_queue_items]() {
        //删除region数据，该region已不在raft组内
        for (auto& remove_item : remove_queue_items) {
            if (_shutdown) {
                return;
            }
            SmartRegion region = get_region(remove_item.drop_region_id());
            if (region == nullptr) {
                continue;
            }
            if (region->region_uuid() != remove_item.region_uuid()) {
                DB_WARNING("delete queue region not match delete_region_id:%ld",
                    remove_item.drop_region_id());
                continue;
            }
            DB_WARNING("real begin delete region from meta server heart beat, delete_region_id:%ld %lu",
                    remove_item.drop_region_id(), region->region_uuid());
            drop_region_from_store(remove_item.drop_region_id(), true);
        }
    };
    _remove_region_queue.run(remove_func);

}

int Store::drop_region_from_store(int64_t drop_region_id, bool need_delay_drop) {
    SmartRegion region = get_region(drop_region_id);
    if (region == nullptr) {
        DB_WARNING("region_id: %ld not exist, may be removed", drop_region_id);
        return -1;
    }
    // 防止一直更新时间导致物理删不掉
    if (!region->removed()) {
        region->shutdown();
        region->join();
        region->set_removed(true);
        DB_WARNING("region node close for removed, region_id: %ld", drop_region_id);
    }
    if (region->is_binlog_region()) {
        RegionControl::remove_cold_data(drop_region_id);
        RegionControl::remove_cold_binlog(drop_region_id);
        DB_WARNING("binlog region clean cold data, region_id: %ld", drop_region_id);
    }
    if (!need_delay_drop) {
        // 重置删除时间，防止add peer的过程中遇到region延迟删除，导致ingest sst失败
        region->set_removed(true);
        RegionControl::clear_all_infos_for_region(drop_region_id, region->get_table_id());
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
        DB_WARNING("noknown backup request [%s].", request_vec[2].c_str());
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
        DB_NOTICE("backup download sst region[%ld]", region_id);
        region->process_download_sst(cntl, request_vec, backup_type);
    } else if (request_vec[0] == "upload") {
        DB_NOTICE("backup upload sst region[%ld]", region_id);
        region->process_upload_sst(cntl, ingest_store_latest_sst);
    }
}

void Store::backup(google::protobuf::RpcController* controller,
    const pb::BackupRequest* request,
    pb::BackupResponse* response,
    google::protobuf::Closure* done) {

    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    int64_t region_id = request->region_id();
    SmartRegion region = get_region(region_id);
    if (region == nullptr) {
        response->set_errcode(pb::REGION_NOT_EXIST);
        DB_WARNING("backup no region in store, region_id: %ld", region_id);
        return;
    }
    if (request->backup_op() == pb::BACKUP_DOWNLOAD) {
        DB_NOTICE("backup download sst region[%ld]", region_id);
        region->process_download_sst_streaming(cntl, request, response);
    } else if (request->backup_op() == pb::BACKUP_UPLOAD) {
        DB_NOTICE("backup upload sst region[%ld]", region_id);
        region->process_upload_sst_streaming(cntl, request->ingest_store_latest_sst(), 
            request, response);
    } else if (request->backup_op() == pb::BACKUP_QUERY_PEERS) {
        region->process_query_peers(cntl, request, response);
    } else if (request->backup_op() == pb::BACKUP_QUERY_STREAMING){
        region->process_query_streaming_result(cntl, request, response);
    } else {
        DB_WARNING("unknown sst backup streaming op.");
    }
}

void Store::get_rocks_statistic(google::protobuf::RpcController* controller,
                         const pb::RocksStatisticReq* request,
                         pb::RocksStatisticRes* response,
                         google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    _multi_thread_cond.increase();
    ON_SCOPE_EXIT([this]() {
        _multi_thread_cond.decrease_signal();
    });
    uint64_t level0_ssts = 0;
    uint64_t pending_compaction_size = 0;
    int ret = RocksWrapper::get_instance()->get_rocks_statistic(level0_ssts, pending_compaction_size);
    if (ret < 0) {
        response->set_errcode(pb::EXEC_FAIL);
        DB_WARNING("get_rocks_statistic failed");
    }
    response->set_level0_sst_num(level0_ssts);
    response->set_compaction_data_size(pending_compaction_size);
    DB_WARNING("level0: %lu, compaction: %lu", level0_ssts, pending_compaction_size);
    for (auto& key : request->keys()) {
        std::string value;
        if (!google::GetCommandLineOption(key.c_str(), &value)) {
            response->set_errcode(pb::EXEC_FAIL);
            DB_WARNING("get command line: %s failed", key.c_str());
            return;
        }
        response->add_key(key);
        response->add_value(value);
    }
    response->set_errcode(pb::SUCCESS);
}
} //namespace
