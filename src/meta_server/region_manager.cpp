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

#include "region_manager.h"
#include <boost/lexical_cast.hpp>
#include "cluster_manager.h"
#include "common.h"
#include "store_interact.hpp"
#include "common_state_machine.h"
#include "meta_util.h"
#include "table_manager.h"
#include "meta_rocksdb.h"

namespace baikaldb {
DECLARE_int32(concurrency_num);
DECLARE_int64(store_heart_beat_interval_us);
DECLARE_int32(store_dead_interval_times);
DECLARE_int32(region_faulty_interval_times);

//增加或者更新region信息
//如果是增加，则需要更新表信息, 只有leader的上报会调用该接口
void RegionManager::update_region(const pb::MetaManagerRequest& request,
                                  const int64_t apply_index, 
                                  braft::Closure* done) {
    TimeCost time_cost;
    std::vector<std::string> put_keys;
    std::vector<std::string> put_values;
    std::vector<bool> is_new;
    std::string min_start_key;
    std::string max_end_key;
    int64_t g_table_id = 0;
    bool key_init = false;
    bool old_pb = false;
    std::vector<pb::RegionInfo> region_infos;
    if (request.has_region_info()) {
        DB_WARNING("use optional region_info region_id:%ld", 
                  request.region_info().region_id());
        region_infos.push_back(request.region_info());
        old_pb = true;
    } else if (request.region_infos().size() > 0) {
        for (auto& region_info : request.region_infos()) {
            region_infos.push_back(region_info);
        }
    } else {
        return;
    }
    std::map<std::string, int64_t> key_id_map;
    for (auto& region_info : region_infos) {
        int64_t region_id = region_info.region_id();
        int64_t table_id = region_info.table_id();
        auto ret = TableManager::get_instance()->whether_exist_table_id(table_id); 
        if (ret < 0) {
            DB_WARNING("table name:%s not exist, region_info:%s", 
                       region_info.table_name().c_str(),
                       region_info.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
            return;
        }
        bool new_add = true;
        SmartRegionInfo region_ptr = _region_info_map.get(region_id);
        if (region_ptr != nullptr) {
            auto& mutable_region_info = const_cast<pb::RegionInfo&>(region_info); 
            mutable_region_info.set_conf_version(region_ptr->conf_version() + 1);
            new_add = false;
        }
        std::string region_value;
        if (!region_info.SerializeToString(&region_value)) {
            DB_WARNING("request serializeToArray fail, request:%s",
                       request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
            return;
        }
        is_new.push_back(new_add);
        put_keys.push_back(construct_region_key(region_id));
        put_values.push_back(region_value);
        if (!key_init) {
            min_start_key = region_info.start_key();
            max_end_key = region_info.end_key();
            g_table_id = table_id;
            key_init = true;
        } else {
            if (g_table_id != table_id) {
                DB_FATAL("two region has different table id %ld vs %ld", 
                        g_table_id, table_id);
                return;
            }
            min_start_key = (min_start_key < region_info.start_key())?
                            min_start_key : region_info.start_key();
            max_end_key = (end_key_compare(max_end_key, region_info.end_key()) > 0)?
                          max_end_key : region_info.end_key();
        }
        if (region_info.start_key() != region_info.end_key() || 
           (region_info.start_key().empty() && region_info.end_key().empty())) {
            key_id_map[region_info.start_key()] = region_id;
        }
    }
    bool add_delete_region = false;
    if (request.has_add_delete_region()) {
        add_delete_region = request.add_delete_region();
    }
    if (!old_pb && !add_delete_region) {
        //兼容旧的pb，old_pb不检查区间
        bool check_ok = TableManager::get_instance()->check_region_when_update(
                            g_table_id, min_start_key, max_end_key);
        if (!check_ok) {
            DB_FATAL("table_id:%ld, min_start_key:%s, max_end_key:%s check fail", 
                     g_table_id, str_to_hex(min_start_key).c_str(), 
                     str_to_hex(max_end_key).c_str());
            return;
        } 
    }

    int ret = MetaRocksdb::get_instance()->put_meta_info(put_keys, put_values);
    if (ret < 0) {
        DB_WARNING("update to rocksdb fail");
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    //更新startkey_regionid_map，先删除旧的startkey，然后将新的插入map
    if (old_pb || add_delete_region) {
        //旧的pb结构直接使用start_key更新map
        TableManager::get_instance()->update_startkey_regionid_map_old_pb(
            g_table_id, key_id_map);
    } else {
        TableManager::get_instance()->update_startkey_regionid_map(g_table_id, 
                min_start_key, 
                max_end_key,
                key_id_map);
    }
    //更新内存值
    int i = 0;
    for (auto& region_info : region_infos) {
        int64_t region_id = region_info.region_id();
        int64_t table_id = region_info.table_id();
        int64_t partition_id = region_info.partition_id();
        set_region_info(region_info);
        RegionStateInfo region_state;
        region_state.timestamp = butil::gettimeofday_us();
        region_state.status = pb::NORMAL;
        set_region_state(region_id, region_state);
        if (is_new[i++]) {
            TableManager::get_instance()->add_region_id(table_id, partition_id, region_id);
        }
    }
    put_incremental_regioninfo(apply_index, region_infos);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("update region success, request:%s, time_cost:%ld", 
              request.ShortDebugString().c_str(), time_cost.get_time());
}
//根据region_id恢复store上误删除的region
//如果待回复的reigon信息存在则直接新建，如果不存在，则根据前后region恢复
void RegionManager::restore_region(const pb::MetaManagerRequest& request, pb::MetaManagerResponse*
response) {
    int64_t region_id = request.restore_region().restore_region_id();
    int64_t lower_region_id = request.restore_region().lower_region_id();
    int64_t upper_region_id = request.restore_region().upper_region_id();
    pb::RegionInfo region_info;
    SmartRegionInfo region_ptr = _region_info_map.get(region_id);
    SmartRegionInfo lower_region_ptr = _region_info_map.get(lower_region_id);
    SmartRegionInfo upper_region_ptr = _region_info_map.get(upper_region_id);
    if (region_ptr != nullptr) {
        region_info = *region_ptr;
    } else if (lower_region_ptr != nullptr && upper_region_ptr != nullptr) {
        region_info = *lower_region_ptr;
        region_info.clear_peers();
        region_info.add_peers(region_info.leader());
        region_info.set_start_key(lower_region_ptr->end_key());
        region_info.set_end_key(upper_region_ptr->start_key());
        region_info.set_region_id(region_id);
        region_info.set_version(1);
        region_info.set_conf_version(1);
        region_info.set_used_size(0);
        region_info.set_log_index(0);
        region_info.set_status(pb::IDLE);
        region_info.set_can_add_peer(false);
        region_info.set_parent(0);
        region_info.set_timestamp(time(NULL));
    } else {
        DB_WARNING("region_id: %ld not exist", region_id);
        response->set_errcode(pb::INPUT_PARAM_ERROR);
        response->set_op_type(request.op_type());
        response->set_errmsg("region not exist");
        return;
    }
    pb::InitRegion init_region_request;
    init_region_request.set_snapshot_times(2);
    region_info.set_can_add_peer(false);
    *(init_region_request.mutable_region_info()) = region_info;
    //leader发送请求
    StoreInteract store_interact(init_region_request.region_info().leader().c_str());
    pb::StoreRes res; 
    auto ret = store_interact.send_request("init_region", init_region_request, res);
    if (ret < 0) { 
        DB_FATAL("create table fail, address:%s, region_id: %ld", 
                init_region_request.region_info().leader().c_str(),
                region_id);
        response->set_errcode(pb::INTERNAL_ERROR);
        response->set_op_type(request.op_type());
        response->set_errmsg("new region fail");
        return;
    }
    DB_NOTICE("new region_id: %ld success, table_name:%s", 
            region_id, region_info.table_name().c_str());
}
//删除region_id的操作只会在表已经删除或创建失败的情况下才会调用
//所以在删除region时表信息已经不存在，不在需要更新表信息
void RegionManager::drop_region(const pb::MetaManagerRequest& request,
                                const int64_t apply_index, 
                                braft::Closure* done) {
    std::vector<std::string> drop_region_keys;
    std::vector<std::int64_t> drop_region_ids;
    for (auto region_id : request.drop_region_ids()) {
        drop_region_keys.push_back(construct_region_key(region_id));
        drop_region_ids.push_back(region_id);
    }
    auto ret = MetaRocksdb::get_instance()->delete_meta_info(drop_region_keys);
    if (ret < 0) {
        DB_WARNING("drop region fail, region_info：%s", 
                    request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    //删除内存中的值
    std::vector<int64_t> result_table_ids;
    std::vector<int64_t> result_partition_ids;
    std::vector<int64_t> result_region_ids;
    std::vector<std::string> result_start_keys;
    std::vector<std::string> result_end_keys;
    erase_region_info(drop_region_ids, result_region_ids, result_partition_ids, 
                      result_table_ids, result_start_keys, result_end_keys);
    erase_region_state(drop_region_ids);
    std::vector<pb::RegionInfo> region_infos;
    for (uint32_t i = 0; i < result_region_ids.size() && 
            i < result_table_ids.size() && 
            i < result_start_keys.size() && 
            i < result_end_keys.size(); i++) {
        pb::RegionInfo region_info;
        region_info.set_region_id(result_region_ids[i]);
        region_info.set_deleted(true);
        region_info.set_table_id(result_table_ids[i]);
        region_info.set_start_key(result_start_keys[i]);
        region_info.set_end_key(result_end_keys[i]);
        region_info.set_table_name("deleted");
        region_info.set_partition_id(0);
        region_info.set_replica_num(0);
        region_info.set_version(0);
        region_info.set_conf_version(0);
        region_infos.push_back(region_info);
    }
    put_incremental_regioninfo(apply_index, region_infos);
    TableManager::get_instance()->delete_region_ids(result_table_ids, result_partition_ids, result_region_ids);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("drop region success, request:%s", request.ShortDebugString().c_str());
}

void RegionManager::split_region(const pb::MetaManagerRequest& request, braft::Closure* done) {
    auto& region_split_info = request.region_split();
    int64_t region_id = region_split_info.region_id();
    int64_t new_region_id = _max_region_id + 1; //新分配的region_id
    std::string instance = region_split_info.new_instance();

    //更新max_region_id
    std::string max_region_id_value;
    max_region_id_value.append((char*)&new_region_id, sizeof(int64_t));
    
    // write date to rocksdb
    auto ret = MetaRocksdb::get_instance()->put_meta_info(construct_max_region_id_key(), max_region_id_value);
    if (ret != 0) {
        DB_WARNING("add max_region_id to rocksdb fail when split region:%s",
                        request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    //更新内存
    set_max_region_id(new_region_id);
    if (done && ((MetaServerClosure*)done)->response) {
        ((MetaServerClosure*)done)->response->set_errcode(pb::SUCCESS);
        ((MetaServerClosure*)done)->response->set_op_type(request.op_type());
        ((MetaServerClosure*)done)->response->mutable_split_response()->set_old_region_id(region_id);
        ((MetaServerClosure*)done)->response->mutable_split_response()->set_new_region_id(new_region_id);
        ((MetaServerClosure*)done)->response->set_errmsg("SUCCESS");
    } 
    DB_NOTICE("split region success, _max_region_id:%ld, new_region_id: %ld, request:%s", 
                _max_region_id, new_region_id, request.ShortDebugString().c_str());
}

void RegionManager::send_remove_region_request(const std::vector<int64_t>& drop_region_ids) {
    BthreadCond concurrency_cond(-FLAGS_concurrency_num);
    uint64_t log_id = butil::fast_rand();
    for (auto& drop_region_id : drop_region_ids) {
        _region_peer_state_map.erase(drop_region_id);
        std::vector<std::string> peers;
        get_region_peers(drop_region_id, peers);
        for (auto& peer : peers) {
            auto drop_region_fun = [&concurrency_cond, log_id, peer, drop_region_id] {
                std::shared_ptr<BthreadCond> auto_decrease(&concurrency_cond,
                            [](BthreadCond* cond) {cond->decrease_signal();});
                pb::RemoveRegion request;
                // 删表时候调用，需要延迟删除
                request.set_need_delay_drop(true);
                request.set_force(true);
                request.set_region_id(drop_region_id);
                StoreInteract store_interact(peer.c_str());
                pb::StoreRes response; 
                auto ret = store_interact.send_request("remove_region", request, response);
                if (ret < 0) { 
                    DB_FATAL("drop region fail, peer: %s, drop_region_id: %ld", peer.c_str(), drop_region_id);
                    return;
                }
                DB_WARNING("send remove region request:%s, response:%s, peer_address:%s, region_id:%ld",
                            request.ShortDebugString().c_str(),
                            response.ShortDebugString().c_str(),
                            peer.c_str(),
                            drop_region_id);
            };
            Bthread bth; 
            concurrency_cond.increase();
            concurrency_cond.wait();
            bth.run(drop_region_fun);
        }
    }
    concurrency_cond.wait(-FLAGS_concurrency_num);
    pb::MetaManagerRequest request;
    request.set_op_type(pb::OP_DROP_REGION);
    for (auto& drop_region_id : drop_region_ids) {
        request.add_drop_region_ids(drop_region_id);
    }
    SchemaManager::get_instance()->process_schema_info(NULL, &request, NULL, NULL);
    //erase_region_info(drop_region_ids);
}

// default for MIGRATE
void RegionManager::add_peer_for_store(const std::string& instance, 
        InstanceStateInfo status) {
    DB_WARNING("add peer all region for migrate store start, store:%s", instance.c_str());
    std::string resource_tag = ClusterManager::get_instance()->get_instance(instance).resource_tag;
    //实例上已经没有reigon了，直接删除该实例即可
    std::vector<int64_t> region_ids;
    get_region_ids(instance, region_ids);
    if (region_ids.size() == 0) {
        // DEAD状态直接删除，MIGRATE状态等待无心跳一段时间后删除(真正被迁移走了)
        int64_t last_timestamp = status.timestamp;
        if ((butil::gettimeofday_us() - last_timestamp) > 
                FLAGS_store_heart_beat_interval_us * FLAGS_store_dead_interval_times) {
            pb::MetaManagerRequest request;
            request.set_op_type(pb::OP_DROP_INSTANCE);
            pb::InstanceInfo* instance_info = request.mutable_instance();
            instance_info->set_address(instance);
            ClusterManager::get_instance()->process_cluster_info(NULL, &request, NULL, NULL);
            DB_WARNING("dead instance has no region, drop instance:%s", instance.c_str());
        }
        return;
    }
    {
        BAIDU_SCOPED_LOCK(_doing_mutex);
        if (_doing_migrate.find(instance) != _doing_migrate.end()) {
            DB_WARNING("instance: %s is doing migrating", instance.c_str());
            return;
        } else {
            _doing_migrate.insert(instance);
        }
    }
    auto asyn_add_peer = [this, instance, status] () {
        std::unordered_map<std::string, std::vector<pb::AddPeer>> add_peer_requests;
        pre_process_add_peer_for_store(instance, status.state, add_peer_requests);
        ConcurrencyBthread concur_bth(add_peer_requests.size());
        for (auto& add_peer_per_instance : add_peer_requests) {
            auto add_peer_fun_per_instance = [this, add_peer_per_instance] ()  {
                ConcurrencyBthread sub_bth(4);
                std::string leader = add_peer_per_instance.first;
                for (auto& add_peer_request : add_peer_per_instance.second) {
                    auto add_peer_fun = [this, leader, add_peer_request] () {
                        StoreInteract store_interact(leader.c_str());
                        pb::StoreRes response; 
                        auto ret = store_interact.send_request("add_peer", add_peer_request, response);
                        DB_WARNING("send add peer leader: %s, request:%s, response:%s, ret: %d",
                                leader.c_str(),
                                add_peer_request.ShortDebugString().c_str(),
                                response.ShortDebugString().c_str(), ret);
                        bthread_usleep(5 * 1000 * 1000LL);
                    };
                    sub_bth.run(add_peer_fun);
                }
                sub_bth.join();
            };
            concur_bth.run(add_peer_fun_per_instance);
        }
        concur_bth.join();
        BAIDU_SCOPED_LOCK(_doing_mutex);
        _doing_migrate.erase(instance);
        DB_WARNING("add all region for migrate store end, store:%s", instance.c_str());
    };
    Bthread bth;
    bth.run(asyn_add_peer);
}

// default for DEAD
void RegionManager::delete_all_region_for_store(const std::string& instance, 
        InstanceStateInfo status) {
    DB_WARNING("delete all region for dead store start, dead_store:%s", instance.c_str());
    std::string resource_tag = ClusterManager::get_instance()->get_instance(instance).resource_tag;
    //实例上已经没有reigon了，直接删除该实例即可
    std::vector<int64_t> region_ids;
    get_region_ids(instance, region_ids);
    if (region_ids.size() == 0) {
        // DEAD状态直接删除，MIGRATE状态等待无心跳一段时间后删除(真正被迁移走了)
        int64_t last_timestamp = status.timestamp;
        if ((butil::gettimeofday_us() - last_timestamp) > 
                FLAGS_store_heart_beat_interval_us * FLAGS_store_dead_interval_times) {
            pb::MetaManagerRequest request;
            request.set_op_type(pb::OP_DROP_INSTANCE);
            pb::InstanceInfo* instance_info = request.mutable_instance();
            instance_info->set_address(instance);
            ClusterManager::get_instance()->process_cluster_info(NULL, &request, NULL, NULL);
            DB_WARNING("dead instance has no region, drop instance:%s", instance.c_str());
        }
        return;
    }
    std::vector<pb::RaftControlRequest> requests;
    pre_process_remove_peer_for_store(instance, status.state, requests);
    BthreadCond concurrency_cond(-FLAGS_concurrency_num);
    for (auto request : requests) {
        auto remove_peer_fun = [this, request, &concurrency_cond] () {
            StoreInteract store_interact(request.new_leader().c_str());
            pb::RaftControlResponse response; 
            store_interact.send_request_for_leader("region_raft_control", request, response);
            DB_WARNING("send remove peer request:%s, response:%s",
                        request.ShortDebugString().c_str(),
                        response.ShortDebugString().c_str());
            concurrency_cond.decrease_signal();
        };
        Bthread bth;
        concurrency_cond.increase();
        concurrency_cond.wait();
        bth.run(remove_peer_fun);
        bthread_usleep(1000 * 10);
    }
    concurrency_cond.wait(-FLAGS_concurrency_num);
    DB_WARNING("delete all region for dead store end, dead_store:%s", instance.c_str());
}

void RegionManager::pre_process_remove_peer_for_store(const std::string& instance, 
        pb::Status status, std::vector<pb::RaftControlRequest>& requests) {
    std::vector<int64_t> region_ids;
    get_region_ids(instance, region_ids);
    std::string resource_tag = ClusterManager::get_instance()->get_instance(instance).resource_tag;
    std::string logical_room = ClusterManager::get_instance()->get_logical_room(instance);
    for (auto& region_id : region_ids) {
        auto ptr_region = get_region_info(region_id);
        if (ptr_region == nullptr) {
            continue;
        }
        int64_t table_id = ptr_region->table_id();
        int64_t replica_num;
        auto ret = TableManager::get_instance()->get_replica_num(table_id, replica_num);
        // MIGRATE 对于其他instance状态判断，如果有非正常状态则暂停迁移region
        if (status == pb::MIGRATE) {
            bool has_error = false;
            for (auto& peer : ptr_region->peers()) {
                if (peer != instance) {
                    auto st = ClusterManager::get_instance()->get_instance_status(peer);
                    if (st == pb::DEAD || st == pb::FAULTY) {
                        has_error = true;
                        break;
                    }
                }
            }
            if (has_error) {
                continue;
            }
        }
        std::string leader = ptr_region->leader();
        // 尝试add_peer
        if (ret < 0 || ptr_region->peers_size() < replica_num) {
            DB_WARNING("region_info: %s peers less than replica_num, can not been remove, instance%s",
                        ptr_region->ShortDebugString().c_str(), instance.c_str());
            std::string new_instance;
            std::set<std::string> peers;
            for (auto& peer : ptr_region->peers()) {
                peers.insert(peer);
            }
            std::string candicate_logical_room;
            if (TableManager::get_instance()->whether_replica_dists(table_id)) {
                candicate_logical_room = logical_room;
            }
            // 故障需要尽快恢复，轮询最均匀
            auto ret = ClusterManager::get_instance()->select_instance_rolling(
                    resource_tag,
                    peers,
                    candicate_logical_room,
                    new_instance);
            if (ret < 0) {
                DB_FATAL("select store from cluster fail, region_id:%ld", region_id);
                return;
            }
            pb::AddPeer add_peer;
            add_peer.set_region_id(region_id);
            for (auto& peer : ptr_region->peers()) {
                add_peer.add_old_peers(peer);
                add_peer.add_new_peers(peer);
            }
            add_peer.add_new_peers(new_instance);
            Bthread bth(&BTHREAD_ATTR_SMALL);
            auto add_peer_fun = 
                [add_peer, leader]() {
                    StoreInteract store_interact(leader.c_str());
                    pb::StoreRes response; 
                    auto ret = store_interact.send_request("add_peer", add_peer, response);
                    DB_WARNING("send add peer leader: %s, request:%s, response:%s, ret: %d",
                            leader.c_str(),
                            add_peer.ShortDebugString().c_str(),
                            response.ShortDebugString().c_str(), ret);
                };
            bth.run(add_peer_fun);
            continue;
        }
        pb::Status status = pb::NORMAL;
        ret = get_region_status(region_id, status);
        if (ret < 0 || status != pb::NORMAL) {
            DB_WARNING("region_id:%ld status:%s is not normal, can not been remove, instance:%s",
                    region_id, pb::Status_Name(status).c_str(), instance.c_str());
            continue;
        }
        pb::RaftControlRequest request;
        request.set_op_type(pb::SetPeer);
        request.set_region_id(region_id);
        for (auto peer : ptr_region->peers()) {
            request.add_old_peers(peer);
            if (peer != instance) {
                request.add_new_peers(peer);
            }
        }
        request.set_new_leader(leader);
        requests.push_back(request);
    }
}
void RegionManager::pre_process_add_peer_for_store(const std::string& instance, pb::Status status, 
                std::unordered_map<std::string, std::vector<pb::AddPeer>>& add_peer_requests) {
    std::vector<int64_t> region_ids;
    get_region_ids(instance, region_ids);
    std::string resource_tag = ClusterManager::get_instance()->get_instance(instance).resource_tag;
    std::string logical_room = ClusterManager::get_instance()->get_logical_room(instance);
    for (auto& region_id : region_ids) {
        auto ptr_region = get_region_info(region_id);
        if (ptr_region == nullptr) {
            continue;
        }
        int64_t table_id = ptr_region->table_id();
        int64_t replica_num;
        auto ret = TableManager::get_instance()->get_replica_num(table_id, replica_num);
        if (ret < 0 || ptr_region->peers_size() > replica_num) {
            DB_WARNING("region_id: %ld has been added peer, region_info: %s",
                        region_id, ptr_region->ShortDebugString().c_str());
            continue;
        }

        // MIGRATE 对于其他instance状态判断，如果有非正常状态则暂停迁移region
        if (status == pb::MIGRATE) {
            bool has_error = false;
            for (auto& peer : ptr_region->peers()) {
                if (peer != instance) {
                    auto st = ClusterManager::get_instance()->get_instance_status(peer);
                    if (st == pb::DEAD || st == pb::FAULTY) {
                        has_error = true;
                        break;
                    }
                }
            }
            if (has_error) {
                continue;
            }
        }

        std::string new_instance;
        std::set<std::string> peers;
        for (auto& peer : ptr_region->peers()) {
            peers.insert(peer);
        }
        std::string candicate_logical_room;
        if (TableManager::get_instance()->whether_replica_dists(table_id)) {
            candicate_logical_room = logical_room;
        }
        // 故障需要尽快恢复，轮询最均匀
        ret = ClusterManager::get_instance()->select_instance_rolling(
                resource_tag,
                peers,
                candicate_logical_room,
                new_instance);
        if (ret < 0) {
            DB_FATAL("select store from cluster fail, region_id:%ld", region_id);
            continue;
        }
        pb::AddPeer add_peer;
        add_peer.set_region_id(region_id);
        for (auto& peer : ptr_region->peers()) {
            add_peer.add_old_peers(peer);
            add_peer.add_new_peers(peer);
        }
        add_peer.add_new_peers(new_instance);
        std::string leader = ptr_region->leader();
        add_peer_requests[leader].push_back(add_peer);
        DB_WARNING("add peer request: %s", add_peer.ShortDebugString().c_str());
    }
}

void RegionManager::check_update_region(const pb::BaikalHeartBeatRequest* request,
            pb::BaikalHeartBeatResponse* response) {
    for (auto& schema_heart_beat : request->schema_infos()) { 
        for (auto& region_info : schema_heart_beat.regions()) {
            int64_t region_id = region_info.region_id();
            SmartRegionInfo region_ptr = _region_info_map.get(region_id);
            if (region_ptr == nullptr) {
                continue;
            }
            //这种场景出现在分裂的时候，baikal会先从store获取新的region信息，不需要更新
            if (region_info.version() > region_ptr->version()) {
                continue;
            }
            if (region_info.version() < region_ptr->version()
                    || region_ptr->conf_version() > region_info.conf_version()) {
                *(response->add_region_change_info()) = *region_ptr;
            }
        } 
    }
}

void RegionManager::add_region_info(const std::vector<int64_t>& new_add_region_ids,
                                    pb::BaikalHeartBeatResponse* response) {
    for (auto& region_id : new_add_region_ids) {
        SmartRegionInfo region_ptr = _region_info_map.get(region_id); 
        if (region_ptr != nullptr) {
            *(response->add_region_change_info()) = *region_ptr;
        }
    }
}
void RegionManager::leader_load_balance(bool whether_can_decide,
            bool load_balance,
            const pb::StoreHeartBeatRequest* request,
            pb::StoreHeartBeatResponse* response) {
    std::string instance = request->instance_info().address();
    std::unordered_map<int64_t, int64_t> table_leader_counts;
    for (auto& leader_region : request->leader_regions()) {
        int64_t table_id = leader_region.region().table_id();
        table_leader_counts[table_id]++;
    }
    set_instance_leader_count(instance, table_leader_counts);
  
    if (!request->need_leader_balance()) {
        return;
    }
    std::string resource_tag = request->instance_info().resource_tag();
    if (!whether_can_decide) {
        DB_WARNING("meta state machine can not decide, resource_tag: %s, instance: %s",
                    resource_tag.c_str(), instance.c_str());
        return;
    }
    if (!load_balance) {
        DB_WARNING("meta state machine close leader load balance, resource_tag: %s, instance: %s", 
                    resource_tag.c_str(), instance.c_str());
        return;
    }
    DB_WARNING("leader load balance, resource_tag: %s, instance: %s", 
                resource_tag.c_str(), instance.c_str());
    int64_t instance_count = ClusterManager::get_instance()->get_instance_count(resource_tag); //机器数量
    //记录以表的维度出发，每个表应该transfer leader的数量
    std::unordered_map<int64_t, int64_t> transfer_leader_count;
    //记录以表的维度出发，每台机器上应有的平均leader的数量
    std::unordered_map<int64_t, int64_t> average_leader_counts;
    for (auto& table_leader_count : table_leader_counts) {
        int64_t average_leader_count = INT_FAST64_MAX;
        int64_t table_id = table_leader_count.first;
        int64_t region_count = TableManager::get_instance()->get_region_count(table_id);
        if (instance_count != 0) {
            average_leader_count = region_count / instance_count;
        }
        if (instance_count != 0 && region_count % instance_count != 0) {
            average_leader_count += 1;
        }
        average_leader_counts[table_id] = average_leader_count;
        if (table_leader_count.second > (average_leader_count + average_leader_count * 5 / 100)) {
            transfer_leader_count[table_id] = 
                2 * (table_leader_count.second - average_leader_count);
            response->add_trans_leader_table_id(table_id);
            response->add_trans_leader_count(table_leader_count.second - average_leader_count);
        }
    }
    for (auto& table_count : transfer_leader_count) {
        DB_WARNING("transfer lead for instance: %s, table_id: %ld,"
                    " average_leader_count: %ld, should transfer leader count: %ld",
                    instance.c_str(), table_count.first, 
                    average_leader_counts[table_count.first], table_count.second);
    }
    if (transfer_leader_count.size() == 0) {
        DB_WARNING("instance: %s  has been leader_load_balance, no need transfer", instance.c_str());
        return;
    }
    //todo 缺点是迁移总在前边几台机器上进行，待改进
    for (auto& leader_region : request->leader_regions()) {
        int64_t table_id = leader_region.region().table_id();
        int64_t region_id = leader_region.region().region_id();
        int64_t replica_num = 0;
        auto ret = TableManager::get_instance()->get_replica_num(table_id, replica_num);
        if (ret < 0) {
            DB_WARNING("table_id: %ld region_id: %ld", table_id, region_id);
            continue;
        }

        if (leader_region.status() != pb::IDLE) {
            continue;
        }
        if (leader_region.region().peers_size() != replica_num) {
            continue;
        }
        if (transfer_leader_count.find(table_id) == transfer_leader_count.end()
                || transfer_leader_count[table_id] == 0) {
            continue;
        }
        int64_t leader_count_for_transfer_peer = INT_FAST64_MAX;
        std::string transfer_to_peer;
        for (auto& peer : leader_region.region().peers()) {
            if (peer == instance) {
                continue;
            }
            auto st = ClusterManager::get_instance()->get_instance_status(peer);
            if (st != pb::NORMAL) {
                break;
            }
            int64_t leader_count = get_leader_count(peer, table_id);
            if (leader_count < average_leader_counts[table_id]
                    && leader_count < leader_count_for_transfer_peer) {
                transfer_to_peer = peer;
                leader_count_for_transfer_peer = leader_count;
            }
        }
        if (transfer_to_peer.size() != 0) { 
            pb::TransLeaderRequest transfer_request;
            transfer_request.set_table_id(table_id);
            transfer_request.set_region_id(region_id);
            transfer_request.set_old_leader(instance);
            transfer_request.set_new_leader(transfer_to_peer);
            transfer_leader_count[table_id]--;
            *(response->add_trans_leader()) = transfer_request;
            add_leader_count(transfer_to_peer, table_id);
        } 
    }
}
// add_peer_count: 每个表需要add_peer的region数量, key: table_id
// instance_regions： add_peer的region从这个候选集中选择, key: table_id
void RegionManager::peer_load_balance(const std::unordered_map<int64_t, int64_t>& add_peer_counts,
        std::unordered_map<int64_t, std::vector<int64_t>>& instance_regions,
        const std::string& instance,
        const std::string& resource_tag,
        std::unordered_map<int64_t, std::string>& logical_rooms,
        std::unordered_map<int64_t, int64_t>& table_average_counts) {
    std::vector<std::pair<std::string, pb::AddPeer>> add_peer_requests;
    for (auto& add_peer_count : add_peer_counts) {
        int64_t table_id = add_peer_count.first;
        int64_t replica_num = 0;
        auto ret = TableManager::get_instance()->get_replica_num(table_id, replica_num);
        if (ret < 0) {
            DB_WARNING("table_id: %ld not exist", table_id);
            continue;
        }
        int64_t count = add_peer_count.second;
        if (instance_regions.find(table_id) == instance_regions.end()) {
            continue;
        }
        size_t total_region_count = instance_regions[table_id].size();
        if (total_region_count == 0) {
            continue;
        }
        size_t index = butil::fast_rand() % total_region_count;
        for (size_t i = 0; i < total_region_count; ++i, ++index) {
            int64_t candicate_region = instance_regions[table_id][index % total_region_count];
            auto master_region_info = get_region_info(candicate_region);
            if (master_region_info == nullptr) {
                continue;
            }
            if (master_region_info->leader() == instance) {
                continue;
            }
            if (master_region_info->peers_size() != replica_num) {
                continue;
            }
            pb::Status status = pb::NORMAL;
            auto ret = get_region_status(candicate_region, status);
            if (ret < 0 || status != pb::NORMAL) {
                DB_WARNING("region status is not normal, region_id: %ld", candicate_region);
                continue;
            }
            std::set<std::string> exclude_stores;
            for (auto& peer : master_region_info->peers()) {
                exclude_stores.insert(peer);
            }
            if (exclude_stores.find(instance) == exclude_stores.end()) {
                continue;
            }
            std::string new_instance;
            ret = ClusterManager::get_instance()->select_instance_min(resource_tag, 
                                                                      exclude_stores, 
                                                                      table_id, 
                                                                      logical_rooms[table_id],
                                                                      new_instance,
                                                                      table_average_counts[table_id]); 
            if (ret < 0) {
                continue;
            }
            pb::AddPeer add_peer;
            add_peer.set_region_id(candicate_region);
            for (auto& peer : master_region_info->peers()) {
                add_peer.add_old_peers(peer);
                add_peer.add_new_peers(peer);
            }
            add_peer.add_new_peers(new_instance);
            add_peer_requests.push_back(std::pair<std::string, pb::AddPeer>(master_region_info->leader(), add_peer));
            --count;
            if (count <= 0) {
                break;
            }
            if (add_peer_requests.size() > 10) {
                break;
            }
        } 
    }
    if (add_peer_requests.size() == 0) {
        return;
    }
    Bthread bth(&BTHREAD_ATTR_SMALL);
    auto add_peer_fun = 
        [add_peer_requests, instance]() {
            for (auto request : add_peer_requests) {
                    StoreInteract store_interact(request.first.c_str());
                    pb::StoreRes response; 
                    auto ret = store_interact.send_request("add_peer", request.second, response);
                    DB_WARNING("instance: %s peer load balance, send add peer leader: %s, request:%s, response:%s, ret: %d",
                                instance.c_str(),
                                request.first.c_str(),
                                request.second.ShortDebugString().c_str(),
                                response.ShortDebugString().c_str(), ret);
                }
        };
    bth.run(add_peer_fun);
}

void RegionManager::update_leader_status(const pb::StoreHeartBeatRequest* request) {
    for (auto& leader_region : request->leader_regions()) {
        int64_t region_id = leader_region.region().region_id();
        int64_t table_id = leader_region.region().table_id();
        RegionStateInfo region_state;
        region_state.timestamp = butil::gettimeofday_us();
        region_state.status = pb::NORMAL;
        _region_state_map.set(region_id, region_state);
        if (leader_region.peers_status_size() > 0) {
            RegionPeerState peer_state =  _region_peer_state_map.get(region_id);
            peer_state.legal_peers_state.clear();
            for (auto peer_status : leader_region.peers_status()) {
                peer_status.set_timestamp(region_state.timestamp);
                peer_status.set_table_id(table_id);
                std::string peer_id = peer_status.peer_id();
                peer_status.clear_peer_id();
                peer_state.legal_peers_state[peer_id] = peer_status;
                peer_state.ilegal_peers_state.erase(peer_id);
                //DB_WARNING("region_id:%ld peer status :%s %s", region_id, peer_status.peer_id().c_str(),
                //    pb::PeerStatus_Name(peer_status.peer_status()).c_str());
            }
            _region_peer_state_map.set(region_id, peer_state);
        }
    }
}

void RegionManager::put_incremental_regioninfo(const int64_t apply_index, std::vector<pb::RegionInfo>& region_infos) {
    _incremental_region_info.put_incremental_info(apply_index, region_infos);
}

bool RegionManager::check_and_update_incremental(const pb::BaikalHeartBeatRequest* request,
                         pb::BaikalHeartBeatResponse* response, int64_t applied_index) {
    int64_t last_updated_index = request->last_updated_index();
    auto update_func = [response](const std::vector<pb::RegionInfo>& region_infos) {
        for (auto info : region_infos) {
            *(response->add_region_change_info()) = info;
        }
    };

    bool need_upd = _incremental_region_info.check_and_update_incremental(update_func, last_updated_index, applied_index);
    if (need_upd) {
        return true;
    }

    if (response->last_updated_index() < last_updated_index) {
        response->set_last_updated_index(last_updated_index);
    }

    return false;
}

bool RegionManager::add_region_is_exist(int64_t table_id, const std::string& start_key, 
                                       const std::string& end_key) {
    if (start_key.empty()) {
        int64_t cur_regionid = TableManager::get_instance()->get_startkey_regionid(table_id, start_key);
        if (cur_regionid < 0) {
            //startkey为空且不在map中，说明已经存在
            return true;
        }
    } else {
        int64_t pre_regionid = TableManager::get_instance()->get_pre_regionid(table_id, start_key);
        if (pre_regionid > 0) {
            auto pre_region_info = get_region_info(pre_regionid);
            if (pre_region_info != nullptr) {
                if (!pre_region_info->end_key().empty() && pre_region_info->end_key() <= start_key) {
                    return true;
                } 
            }
        }
    }
    return false;
}

void RegionManager::leader_heartbeat_for_region(const pb::StoreHeartBeatRequest* request,
                                                pb::StoreHeartBeatResponse* response) {
    TimeCost step_time_cost; 
    std::string instance = request->instance_info().address();
    std::vector<std::pair<std::string, pb::RaftControlRequest>> remove_peer_requests;
   
    std::set<int64_t> related_table_ids;
    std::set<std::string> related_peers;
    for (auto& leader_region : request->leader_regions()) {
        const pb::RegionInfo& leader_region_info = leader_region.region();
        related_table_ids.insert(leader_region_info.table_id());
        for (auto& peer : leader_region_info.peers()) {
            related_peers.insert(peer);
        }
    }
    std::unordered_map<int64_t, int64_t> table_replica_nums;
    std::unordered_map<int64_t, std::string> table_resource_tags;
    std::unordered_map<int64_t, std::unordered_map<std::string, int64_t>> table_replica_dists_maps;
    std::unordered_map<std::string, std::string> peer_resource_tags;
    TableManager::get_instance()->get_table_info(related_table_ids, 
                table_replica_nums, 
                table_resource_tags,
                table_replica_dists_maps);
    ClusterManager::get_instance()->get_resource_tag(related_peers, peer_resource_tags);
    int64_t pre_time_cost = step_time_cost.get_time();
    step_time_cost.reset();

    int64_t get_region_cost = 0;
    int64_t update_region_cost = 0;
    int64_t peer_count_cost = 0;
    for (auto& leader_region : request->leader_regions()) {
        const pb::RegionInfo& leader_region_info = leader_region.region();
        int64_t region_id = leader_region_info.region_id();
        TimeCost sub_step_time_cost;
        auto master_region_info = get_region_info(region_id);
        get_region_cost += sub_step_time_cost.get_time();
        sub_step_time_cost.reset();
        //新增region, 在meta_server中不存在，加入临时map等待分裂region整体更新
        if (master_region_info == nullptr) {
            if (leader_region_info.start_key().empty() && 
               leader_region_info.end_key().empty()) {
                //该region为第一个region直接添加
                DB_WARNING("region_info: %s is new ", leader_region_info.ShortDebugString().c_str());
                pb::MetaManagerRequest request;
                request.set_op_type(pb::OP_UPDATE_REGION);
                *(request.add_region_infos()) = leader_region_info;
                SchemaManager::get_instance()->process_schema_info(NULL, &request, NULL, NULL);
            } else if (true == add_region_is_exist(leader_region_info.table_id(),
                                                   leader_region_info.start_key(), 
                                                   leader_region_info.end_key())) {
                DB_WARNING("region_info: %s is exist ", leader_region_info.ShortDebugString().c_str());
                pb::MetaManagerRequest request;
                request.set_op_type(pb::OP_UPDATE_REGION);
                request.set_add_delete_region(true);
                *(request.add_region_infos()) = leader_region_info;
                SchemaManager::get_instance()->process_schema_info(NULL, &request, NULL, NULL);
            } else {
                DB_WARNING("region_info: %s is new ", 
                           leader_region_info.ShortDebugString().c_str());
                TableManager::get_instance()->add_new_region(leader_region_info);
            }
            continue;
        }
        std::set<std::string> peers_in_heart;
        for (auto& peer : leader_region_info.peers()) {
            peers_in_heart.insert(peer);
        }
        std::set<std::string> peers_in_master;
        for (auto& peer: master_region_info->peers()) {
            peers_in_master.insert(peer);
        }
        check_whether_update_region(region_id, leader_region, master_region_info, peers_in_heart, peers_in_master);
        update_region_cost += sub_step_time_cost.get_time();
        sub_step_time_cost.reset();
        check_peer_count(region_id, 
                        leader_region,
                        peers_in_heart,
                        peers_in_master,
                        table_replica_nums,
                        table_resource_tags,
                        table_replica_dists_maps,
                        peer_resource_tags,
                        remove_peer_requests, 
                        response);
        peer_count_cost = sub_step_time_cost.get_time();
    }

    int64_t check_leader_time = step_time_cost.get_time();
    DB_NOTICE("store: %s leader heartbeat for region, pre_time_cost: %ld, check_leader_time: %ld, "
                "get_region_cost: %ld, update_region_cost: %ld, peer_count_cost: %ld",
                instance.c_str(), pre_time_cost, check_leader_time, get_region_cost, update_region_cost, peer_count_cost);
    if (remove_peer_requests.size() == 0) {
        return;
    }
    Bthread bth(&BTHREAD_ATTR_SMALL);
    auto remove_peer_fun = 
        [this, remove_peer_requests]() {
            for (auto request : remove_peer_requests) {
                    StoreInteract store_interact(request.second.new_leader().c_str());
                    pb::RaftControlResponse response; 
                    auto ret = store_interact.send_request("region_raft_control", request.second, response);
                    DB_WARNING("send remove peer request:%s, response:%s, ret: %d",
                                request.second.ShortDebugString().c_str(),
                                response.ShortDebugString().c_str(), ret);
                    if (ret == 0) {
                        pb::RemoveRegion remove_region_request;
                        // 负载均衡及时删除
                        remove_region_request.set_need_delay_drop(false);
                        remove_region_request.set_force(true);
                        remove_region_request.set_region_id(request.second.region_id());
                        StoreInteract store_interact(request.first.c_str());
                        pb::StoreRes remove_region_response; 
                        ret = store_interact.send_request("remove_region", remove_region_request, remove_region_response);
                        DB_WARNING("send remove region to store:%s request: %s, resposne: %s, ret: %d",
                                    request.first.c_str(),
                                    remove_region_request.ShortDebugString().c_str(),
                                    remove_region_response.ShortDebugString().c_str(), ret);
                    }
                }
        };
    bth.run(remove_peer_fun);
}
void RegionManager::check_whether_update_region(int64_t region_id,  
                                                const pb::LeaderHeartBeat& leader_region,
                                                const SmartRegionInfo& master_region_info, 
                                                const std::set<std::string>& peers_in_heart, 
                                                const std::set<std::string>& peers_in_master) {
    const pb::RegionInfo& leader_region_info = leader_region.region();
    if (leader_region_info.log_index() < master_region_info->log_index()) {
        DB_WARNING("leader: %s log_index:%ld in heart is less than in master:%ld, region_id: %ld",
                    leader_region_info.leader().c_str(), 
                    leader_region_info.log_index(), 
                    master_region_info->log_index(),
                    region_id);
        return;
    }
    //如果version没有变，但是start_key 或者end_key变化了，说明有问题，报警追查, 同时不更新
    if (leader_region_info.version() == master_region_info->version()
            && (leader_region_info.start_key() != master_region_info->start_key()
                || leader_region_info.end_key() != master_region_info->end_key())) {
        DB_FATAL("version not change, but start_key or end_key change,"
                    " old_region_info: %s, new_region_info: %s",
                    master_region_info->ShortDebugString().c_str(),
                    leader_region_info.ShortDebugString().c_str());
        return;
    }
    bool version_changed = false;
    bool peer_changed = false;
    //version发生变化，说明分裂或合并
    if (leader_region_info.version() > master_region_info->version()
            || leader_region_info.start_key() != master_region_info->start_key()
            || leader_region_info.end_key() != master_region_info->end_key()) {
        version_changed = true;
    }
    //peer发生变化
    if (leader_region.status() == pb::IDLE && peers_in_master != peers_in_heart) {
        peer_changed = true;
    }
    // 前两种情况都是通过raft更新
    if (version_changed) {
        TableManager::get_instance()->check_update_region(leader_region, 
                                                         master_region_info);
    } else if (peer_changed) {
        //仅peer_changed直接走raft状态机修改
        pb::MetaManagerRequest request;
        request.set_op_type(pb::OP_UPDATE_REGION);
        pb::RegionInfo* tmp_region_info = request.add_region_infos();
        *tmp_region_info = *master_region_info;
        tmp_region_info->set_leader(leader_region_info.leader());
        tmp_region_info->clear_peers();
        for (auto& peer : peers_in_heart) {
            tmp_region_info->add_peers(peer);
        }
        tmp_region_info->set_used_size(leader_region_info.used_size());
        tmp_region_info->set_log_index(leader_region_info.log_index());
        tmp_region_info->set_conf_version(master_region_info->conf_version());
        DB_WARNING("region_id: %ld, peer_changed: %d, version_changed:%d", 
                    region_id, peer_changed, version_changed);
        SchemaManager::get_instance()->process_schema_info(NULL, &request, NULL, NULL);
    } else {
        //只更新内存
        if (leader_region.status() == pb::IDLE &&  leader_region_info.leader() != master_region_info->leader()) {
            set_region_leader(region_id, leader_region_info.leader());
        }
        if (leader_region.status() == pb::IDLE && 
                (leader_region_info.log_index() != master_region_info->log_index()
                 || leader_region_info.used_size() != master_region_info->used_size()
                 || leader_region_info.num_table_lines() != master_region_info->num_table_lines()
                )) {
            set_region_mem_info(region_id, 
                    leader_region_info.log_index(), 
                    leader_region_info.used_size(),
                    leader_region_info.num_table_lines());
        } 
    }
}

void RegionManager::check_peer_count(int64_t region_id,
        const pb::LeaderHeartBeat& leader_region,
        const std::set<std::string>& peers_in_heart,
        const std::set<std::string>& peers_in_master,
        std::unordered_map<int64_t, int64_t>& table_replica_nums,
        std::unordered_map<int64_t, std::string>& table_resource_tags,
        std::unordered_map<int64_t, std::unordered_map<std::string, int64_t>>& table_replica_dists_maps,
        std::unordered_map<std::string, std::string>& peer_resource_tags,
        std::vector<std::pair<std::string, pb::RaftControlRequest>>& remove_peer_requests, 
        pb::StoreHeartBeatResponse* response) {
    if (leader_region.status() != pb::IDLE) {
        return;
    }
    if (peers_in_heart != peers_in_master) {
        return;
    }  
    const pb::RegionInfo& leader_region_info = leader_region.region(); 
    int64_t table_id = leader_region_info.table_id();
    if (table_replica_nums.find(table_id) == table_replica_nums.end()) {
        DB_WARNING("table_id: %ld not exist, may be delete", table_id);
        return;
    }
    int64_t replica_num = table_replica_nums[table_id];
    
    // add_peer
    bool need_add_peer = false;
    std::string table_resource_tag = table_resource_tags[table_id];
    std::unordered_map<std::string, int> resource_tag_count;
    for (auto& peer : peers_in_heart) {
        auto iter = peer_resource_tags.find(peer);
        // meta找不到peer，可以等下一轮上报
        // 否则会导致resource_tag_count[table_resource_tag]不足
        if (iter == peer_resource_tags.end()) {
            return;
        }
        std::string peer_resource_tag = iter->second;
        resource_tag_count[peer_resource_tag]++;
    }
    std::string candicate_logical_room;
    //选出逻辑机房
    std::unordered_map<std::string, int64_t> schema_logical_room_count_map = table_replica_dists_maps[table_id];
    std::unordered_map<std::string, int64_t> current_logical_room_count_map;
    //如果用户修个了resource_tag, 先加后删
    if (resource_tag_count[table_resource_tag] < replica_num) {
        DB_WARNING("resource_tag count:%d < replica_num:%d", resource_tag_count[table_resource_tag], replica_num);
        need_add_peer = true;
    }
    //没有指定机房分布的表，只按照replica_num计算
    if (schema_logical_room_count_map.size() == 0 && leader_region_info.peers_size() < replica_num) {
        need_add_peer = true;
    }
    //指定机房信息的表特殊处理
    if (schema_logical_room_count_map.size() > 0) {
        for (auto& peer : leader_region_info.peers()) {
            std::string logical_room = ClusterManager::get_instance()->get_logical_room(peer);
            if (peer_resource_tags[peer] == table_resource_tag && logical_room.size() > 0) {
                current_logical_room_count_map[logical_room]++; 
            }
        }
        for (auto& schema_count : schema_logical_room_count_map) {
            std::string logical_room = schema_count.first;
            if (schema_logical_room_count_map[logical_room] > current_logical_room_count_map[logical_room]) {
                candicate_logical_room = logical_room;
                DB_WARNING("candicate_logical_room:%s", candicate_logical_room.c_str());
                need_add_peer = true;
                break;
            }
        }
    }
    if (need_add_peer) {
        std::string new_instance;
        // 故障需要尽快恢复，轮询最均匀
        auto ret = ClusterManager::get_instance()->select_instance_rolling(
                table_resource_tag,
                peers_in_heart,
                candicate_logical_room,
                new_instance);
        if (ret < 0) {
            DB_FATAL("select store from cluster fail, region_id:%ld, table_resource_tag: %s, "
                        "peer_size:%d, replica_num:%d candicate_logical_room: %s", 
                        region_id, table_resource_tag.c_str(), 
                        leader_region_info.peers_size(), 
                        replica_num, candicate_logical_room.c_str());
            return;
        }
        pb::AddPeer* add_peer = response->add_add_peers();
        add_peer->set_region_id(region_id);
        for (auto& peer : leader_region_info.peers()) {
            add_peer->add_old_peers(peer);
            add_peer->add_new_peers(peer);
        }
        add_peer->add_new_peers(new_instance);
        DB_WARNING("add_peer request:%s", add_peer->ShortDebugString().c_str());
        return;
    }
    //选择一个peer被remove
    if (leader_region_info.peers_size() > replica_num) {
        pb::RaftControlRequest remove_peer_request;
        int64_t min_peer_count = 0;
        remove_peer_request.set_op_type(pb::SetPeer);
        remove_peer_request.set_region_id(region_id);
        std::string remove_peer;
        std::set<std::string> candicate_remove_peers = peers_in_heart;
        for (auto& candicate_remove_peer : candicate_remove_peers) {
            pb::Status status = ClusterManager::get_instance()->get_instance_status(candicate_remove_peer);
            //先判断这些peer中是否有peer所在的实例状态不是NORMAL
            if (status != pb::NORMAL) {
                remove_peer = candicate_remove_peer;
                DB_WARNING("remove peer: %s because of peers_size:%lu status is: %s, region_info: %s",
                            remove_peer.c_str(), 
                            leader_region_info.peers_size(),
                            pb::Status_Name(status).c_str(),
                            leader_region_info.ShortDebugString().c_str());
                break;
            }
        }
        if (remove_peer.empty()) {
            for (auto& candicate_remove_peer : candicate_remove_peers) {
                if (peer_resource_tags[candicate_remove_peer] != table_resource_tag) {
                    remove_peer = candicate_remove_peer;
                    DB_WARNING("remove peer: %s because of peers_size:%lu resource tag is: %s, table_resource_tag: %s",
                            remove_peer.c_str(), 
                            leader_region_info.peers_size(),
                            peer_resource_tags[candicate_remove_peer].c_str(), 
                            table_resource_tag.c_str());
                    break;
                }
            }
        }
        if (remove_peer.empty()) {
            //得到region 副本分布情况 
            std::unordered_map<std::string, int64_t> schema_replica_dists;
            auto ret = TableManager::get_instance()->get_replica_dists(table_id, schema_replica_dists);
            if (ret < 0) {
                DB_WARNING("get replica dists fail, region_id: %ld, table_id: %ld", region_id, table_id);
                return;
            }
            //需要按照用户指定的副本分布来做remove_peer
            if (schema_replica_dists.size() > 0) { 
                //计算当前的副本分布情况
                std::unordered_map<std::string, std::set<std::string>> current_replica_dists;
                for (auto& peer: peers_in_heart) {
                    std::string logical_room = ClusterManager::get_instance()->get_logical_room(peer);
                    if (logical_room.size() > 0) {
                        current_replica_dists[logical_room].insert(peer); 
                    }
                }
                
                //选择逻辑机房副本数量大于table 副本分布作为待删除peer
                for (auto& current_replica_dist : current_replica_dists) {
                    std::string logical_room = current_replica_dist.first;
                    if (current_replica_dist.second.size() > (size_t)schema_replica_dists[logical_room]) {
                        DB_WARNING("candicate remove peer logical room is : %s, region_id: %ld, table_id: %ld", 
                                    logical_room.c_str(), region_id, table_id);
                        candicate_remove_peers = current_replica_dist.second;
                        break;
                    }
                } 
            }
            for (auto& peer : candicate_remove_peers) {
                /*
                if (peer == leader_region_info.leader()) {
                    continue;
                }*/
                int64_t peer_count = ClusterManager::get_instance()->get_peer_count(peer, table_id);
                DB_WARNING("cadidate remove peer, peer_count: %ld, instance: %s, table_id: %ld", 
                            peer_count, peer.c_str(), table_id);
                if (peer_count >= min_peer_count) {
                    remove_peer = peer;
                    min_peer_count = peer_count;
                }
            }
        }
        if (remove_peer.empty()) {
            return;
        }
        DB_WARNING("remove peer, peer_count: %ld, instance: %s, table_id: %ld", 
                    min_peer_count, remove_peer.c_str(), table_id);
        for (auto& peer : peers_in_heart) {
            remove_peer_request.add_old_peers(peer);
            if (peer != remove_peer) {
                remove_peer_request.add_new_peers(peer);
            }
        }
        ClusterManager::get_instance()->sub_peer_count(remove_peer, table_id);
        remove_peer_request.set_new_leader(leader_region_info.leader());
        remove_peer_requests.push_back(std::pair<std::string, pb::RaftControlRequest>(remove_peer,remove_peer_request));
    }
}

void RegionManager::check_whether_illegal_peer(const pb::StoreHeartBeatRequest* request,
            pb::StoreHeartBeatResponse* response) {
    std::string instance = request->instance_info().address();
    for (auto& peer_info : request->peer_infos()) {
        int64_t region_id = peer_info.region_id();
        auto master_region_info = get_region_info(region_id);
        RegionPeerState peer_state =  _region_peer_state_map.get(region_id);
        pb::PeerStateInfo new_peer_info;
        new_peer_info.set_timestamp(butil::gettimeofday_us());
        //new_peer_info.set_peer_id(instance);
        new_peer_info.set_table_id(peer_info.table_id());
        if (master_region_info == nullptr) {
            //这种情况在以下场景中会出现
            //1.新创建的region_id，该region的leader还没上报心跳，follower先上报了
            //2.空region merge过程中region 区间已经置空但是还未删除store上的region，此时meta重启此region不会再次读入内存
            if (peer_info.has_start_key() 
                    && peer_info.has_end_key() 
                    && !peer_info.start_key().empty()
                    && !peer_info.end_key().empty()) {
                if (peer_info.start_key() == peer_info.end_key()) {
                    DB_WARNING("region_id:%ld is none peer, "
                            " master_peer_info: null, peer_info:%s, peer_address:%s should be delete",
                            region_id,
                            peer_info.ShortDebugString().c_str(),
                            instance.c_str());
                    response->add_delete_region_ids(region_id);
                    _region_peer_state_map.erase(region_id);
                }
            }
            continue;
        }
        int64_t table_id = master_region_info->table_id();
        int64_t replica_num = 0;
        auto ret = TableManager::get_instance()->get_replica_num(table_id, replica_num);
        if (ret < 0) {
            DB_WARNING("table_id: %ld not exist", table_id);
            continue;
        }
        auto check_legal_peer = [&instance](SmartRegionInfo master) -> bool {
            for (auto& peer : master->peers()) {
                if (peer == instance) {
                    return true;
                }
            }
            return false;
        };
        if (master_region_info->peers_size() >= replica_num
                //&& (peer_info.log_index() != 0 || SchemaManager::get_instance()->get_unsafe_decision())
                && master_region_info->log_index() > peer_info.log_index()) {
            //判断该实例上的peer是不是该region的有效peer，如不是，则删除
            bool legal_peer = check_legal_peer(master_region_info);
            if (!legal_peer) {
                DB_WARNING("region_id:%ld is not legal peer, log_index:%ld,"
                            " master_peer_info: %s, peer_info:%s, peer_address:%s should be delete",
                            region_id, master_region_info->log_index(),
                            master_region_info->ShortDebugString().c_str(),
                            peer_info.ShortDebugString().c_str(),
                            instance.c_str());
                response->add_delete_region_ids(region_id);
                _region_peer_state_map.erase(region_id);
            } else {
                if (peer_info.has_exist_leader() && !peer_info.exist_leader()) {
                    new_peer_info.set_peer_status(pb::STATUS_NO_LEADER);
                    peer_state.legal_peers_state[instance] = new_peer_info;
                    _region_peer_state_map.set(region_id, peer_state);
                }
            }
            continue;
        }
        if (peer_info.has_start_key() 
                && peer_info.has_end_key() 
                && !peer_info.start_key().empty()
                && !peer_info.end_key().empty()) {
            if (peer_info.start_key() == peer_info.end_key()
                && master_region_info->start_key() == master_region_info->end_key()) {
                DB_WARNING("region_id:%ld is none peer, log_index:%ld,"
                           " master_peer_info: %s, peer_info:%s, peer_address:%s should be delete",
                           region_id, master_region_info->log_index(),
                           master_region_info->ShortDebugString().c_str(),
                           peer_info.ShortDebugString().c_str(),
                           instance.c_str());
                response->add_delete_region_ids(region_id);
                _region_peer_state_map.erase(region_id);
            }
            continue;
        }
        // peer没有leader
        if (peer_info.has_exist_leader() && !peer_info.exist_leader()) {
            //&& (master_region_info->log_index() <= peer_info.log_index())) {
            DB_WARNING("region_id:%ld meta_log:%ld peer_log:%ld", region_id, master_region_info->log_index(),
            peer_info.log_index());
            bool legal_peer = check_legal_peer(master_region_info);
            if (!legal_peer) {
                new_peer_info.set_peer_status(pb::STATUS_ILLEGAL_PEER);
                peer_state.ilegal_peers_state[instance] = new_peer_info;
            } else {
                new_peer_info.set_peer_status(pb::STATUS_NO_LEADER);
                peer_state.legal_peers_state[instance] = new_peer_info;
            }
            _region_peer_state_map.set(region_id, peer_state);
        }
    }
}

int RegionManager::load_region_snapshot(const std::string& value) {
    pb::RegionInfo region_pb;
    if (!region_pb.ParseFromString(value)) {
        DB_FATAL("parse from pb fail when load region snapshot, value: %s", value.c_str());
        return -1;
    }
    if (region_pb.start_key() == region_pb.end_key()
       && !region_pb.start_key().empty()) {
        //空region不再读取
        return 0;
    }
    set_region_info(region_pb);
    RegionStateInfo region_state;
    region_state.timestamp = butil::gettimeofday_us();
    region_state.status = pb::NORMAL;
    set_region_state(region_pb.region_id(), region_state);
    TableManager::get_instance()->add_region_id(region_pb.table_id(), 
                region_pb.partition_id(),
                region_pb.region_id());
    TableManager::get_instance()->add_startkey_regionid_map(region_pb);
    RegionPeerState peer_state =  _region_peer_state_map.get(region_pb.region_id());
    for (auto peer : region_pb.peers()) {
        pb::PeerStateInfo new_peer_info;
        new_peer_info.set_timestamp(region_state.timestamp);
        new_peer_info.set_table_id(region_pb.table_id());
        new_peer_info.set_peer_status(pb::STATUS_NORMAL);
        peer_state.legal_peers_state[peer] = new_peer_info;
    }
    _region_peer_state_map.set(region_pb.region_id(), peer_state);
    return 0;
}

void RegionManager::migirate_region_for_store(const std::string& instance) {
    //暂时不做操作，只报警
    //todo 自动化迁移
    std::vector<int64_t> region_ids;
    get_region_ids(instance, region_ids);
    std::string regions_string;
    for (auto region_id : region_ids) {
        regions_string += boost::lexical_cast<std::string>(region_id) + ":";
    }
    DB_FATAL("instance used size exceed 60\% of capacity, please migirate," 
             "instance:%s, regions:%s", instance.c_str(), regions_string.c_str());
}

//报警，需要人工处理
void RegionManager::region_healthy_check_function() {
    std::vector<int64_t> region_ids;
    auto check_func = [this, &region_ids](const int64_t region_id, RegionStateInfo& region_state) {
        if (butil::gettimeofday_us() - region_state.timestamp >
            FLAGS_store_heart_beat_interval_us * FLAGS_region_faulty_interval_times) {
            region_ids.push_back(region_id);
            region_state.status = pb::FAULTY;
        } else {
                region_state.status = pb::NORMAL;
        }};

    _region_state_map.traverse_with_key_value(check_func);
    std::vector<int64_t> drop_region_ids;
    for (auto& region_id : region_ids) {
        auto region_info = get_region_info(region_id);
        if (region_info == nullptr) {
            continue; 
        }
        if (region_info->start_key() == region_info->end_key() 
                && !region_info->start_key().empty()) {
            //长时间没有收到空region的心跳，说明store已经删除，此时meta也可删除
            DB_WARNING("region_id:%ld, table_id: %ld leader:%s maybe erase", 
                       region_id, region_info->table_id(), region_info->leader().c_str());
            drop_region_ids.push_back(region_id);
            continue;
        }
        DB_WARNING("region_id:%ld not recevie heartbeat for a long time, table_id: %ld leader:%s", 
                region_id, region_info->table_id(), region_info->leader().c_str());
        // 长时间未上报心跳的region，特别是所有副本都被误删的情况
        RegionPeerState peer_state =  _region_peer_state_map.get(region_id);
        for (auto& peer : region_info->peers()) {
            auto iter = peer_state.legal_peers_state.find(peer);
            if (iter != peer_state.legal_peers_state.end()) {
                if (butil::gettimeofday_us() - iter->second.timestamp() >
                    FLAGS_store_heart_beat_interval_us * FLAGS_region_faulty_interval_times) {
                    iter->second.set_peer_status(pb::STATUS_NOT_HEARTBEAT);
                }
            } else {
                pb::PeerStateInfo peer_state_info;
                peer_state_info.set_timestamp(butil::gettimeofday_us());
                peer_state_info.set_peer_status(pb::STATUS_NOT_HEARTBEAT);
                peer_state_info.set_table_id(region_info->table_id());
                //peer_state_info.set_peer_id(peer);
                peer_state.legal_peers_state[peer] = peer_state_info;
            }
        }
        _region_peer_state_map.set(region_id, peer_state);
    }
    erase_region_info(drop_region_ids);
    std::map<std::string, int64_t> uniq_instance;
    {
        BAIDU_SCOPED_LOCK(_instance_region_mutex);
        auto iter = _instance_region_map.begin();
        while (iter != _instance_region_map.end()) {
            std::string peer = iter->first;
            if (iter->second.size() > 0) {
                int64_t table_id = iter->second.begin()->first;
                uniq_instance[peer] = table_id;
                ++iter; 
            } else {
                iter = _instance_region_map.erase(iter);
            }
        }
    }
    whether_add_instance(uniq_instance);
}
void RegionManager::reset_region_status() {
    auto reset_func = [this](RegionStateInfo& region_state) {
        region_state.timestamp = butil::gettimeofday_us();
        region_state.status = pb::NORMAL;        
    };
    _region_state_map.traverse(reset_func);
    BAIDU_SCOPED_LOCK(_count_mutex);
    _instance_leader_count.clear();
}

SmartRegionInfo RegionManager::get_region_info(int64_t region_id) {
    return _region_info_map.get(region_id);
}

void RegionManager::get_region_info(const std::vector<int64_t>& region_ids, 
            std::vector<SmartRegionInfo>& region_infos) {
    for (auto& region_id : region_ids) {
        SmartRegionInfo region_ptr = _region_info_map.get(region_id); 
        if (region_ptr == nullptr) {
            DB_WARNING("region_id: %ld not exist", region_id);
            continue;
        }
        region_infos.push_back(region_ptr);
    }
}

void RegionManager::erase_region_info(const std::vector<int64_t>& drop_region_ids,
                    std::vector<int64_t>& result_region_ids,
                    std::vector<int64_t>& result_partition_ids,
                    std::vector<int64_t>& result_table_ids, 
                    std::vector<std::string>& result_start_keys,
                    std::vector<std::string>& result_end_keys) {
    for (auto drop_region_id : drop_region_ids) {
        SmartRegionInfo region_ptr = _region_info_map.get(drop_region_id);
        if (region_ptr == nullptr) {
            continue;
        }
        result_region_ids.push_back(drop_region_id);
        result_partition_ids.push_back(region_ptr->partition_id());
        int64_t table_id = region_ptr->table_id();
        TableManager::get_instance()->erase_region(table_id, drop_region_id, 
                region_ptr->start_key());
        result_table_ids.push_back(table_id);
        result_start_keys.push_back(region_ptr->start_key());
        result_end_keys.push_back(region_ptr->end_key());
        for (auto peer : region_ptr->peers()) {
            {
                BAIDU_SCOPED_LOCK(_instance_region_mutex);
                if (_instance_region_map.find(peer) != _instance_region_map.end()
                        && _instance_region_map[peer].find(table_id) != _instance_region_map[peer].end()) {
                    _instance_region_map[peer][table_id].erase(drop_region_id);
                    if (_instance_region_map[peer][table_id].size() == 0) {
                        _instance_region_map[peer].erase(table_id);
                    }
                    if (_instance_region_map[peer].size() == 0) {
                        _instance_region_map.erase(peer);
                    }
                }
            }
            _region_info_map.erase(drop_region_id);
        }
    }
    for (auto drop_region_id : drop_region_ids) {
        _region_state_map.erase(drop_region_id);
    }
}

void RegionManager::recovery_single_region_by_set_peer(const int64_t region_id, 
                    const std::set<std::string>& resource_tags,
                    std::set<std::string> peers,
                    std::map<std::string, std::set<int64_t>>& not_alive_regions,
                    std::vector<pb::PeerStateInfo>& recover_region_way) {
    SmartRegionInfo region_ptr = _region_info_map.get(region_id);
    std::string resource_tag;
    if (region_ptr != nullptr) {
        for (auto& peer : region_ptr->peers()) {
            peers.insert(peer);
        }
        int ret = TableManager::get_instance()->get_resource_tag(region_ptr->table_id(), resource_tag);
        if (ret < 0) {
            DB_WARNING("tag not exist table_id:%ld region_id:%ld", region_ptr->table_id(), region_id);
            return;
        }
        if (resource_tags.size() > 0 && (resource_tags.count(resource_tag) == 0)) {
            // 不在需求的resource_tag内
            return;
        }
    } else {
        std::string peer_str;
        for (auto& peer : peers) {
            peer_str += peer;
            peer_str += ";";
        }
        DB_WARNING("region_id:%ld not found in meta peer_str:%s", region_id, peer_str.c_str());
        // TODO meta没有region信息如何处理
        return;
    }
    int64_t max_applied_index = 0;
    std::string selected_peer;
    bool has_alive_peer = false;
    for (auto& peer : peers) {
        // 选出applied_index最大的peer
        pb::GetAppliedIndex store_request;
        store_request.set_region_id(region_id);
        StoreInteract store_interact(peer);
        pb::StoreRes res; 
        auto ret = store_interact.send_request("get_applied_index", store_request, res);
        DB_WARNING("send get_applied_index request:%s, response:%s",
                    store_request.ShortDebugString().c_str(),
                    res.ShortDebugString().c_str());
        if (ret == 0) {
            if (max_applied_index <= res.applied_index()) {
                max_applied_index = res.applied_index();
                selected_peer = peer;
                has_alive_peer = true;
            }
            // 有leader不操作
            if (res.leader() != "0.0.0.0:0") {
                DB_WARNING("region_id:%ld has leader:%s no need recovery", region_id, res.leader().c_str());
                return;
            }
        }
    }
    if (has_alive_peer) {
        pb::RaftControlRequest request;
        request.set_op_type(pb::SetPeer);
        request.set_region_id(region_id);
        request.add_new_peers(selected_peer);
        request.set_force(true);
        StoreInteract store_interact(selected_peer);
        pb::RaftControlResponse response; 
        int ret = store_interact.send_request_for_leader("region_raft_control", request, response);
        DB_WARNING("send SetPeer request:%s, response:%s",
                    request.ShortDebugString().c_str(),
                    response.ShortDebugString().c_str());
        if (ret == 0) {
            // 重置meta记录的log_index以便心跳上报更新
            region_ptr->set_log_index(max_applied_index);
            _region_peer_state_map.erase(region_id);
            pb::PeerStateInfo peer_status;
            peer_status.set_region_id(region_id);
            peer_status.set_peer_id(selected_peer);
            peer_status.set_table_id(region_ptr->table_id());
            peer_status.set_peer_status(pb::STATUS_SET_PEER);
            BAIDU_SCOPED_LOCK(_doing_mutex);
            recover_region_way.push_back(peer_status);
        } else {
            DB_FATAL("send SetPeer failed, request:%s, response:%s",
                    request.ShortDebugString().c_str(),
                    response.ShortDebugString().c_str());
        }
    } else {
        DB_WARNING("region_id:%ld not alive peer need query all instance", region_id);
        BAIDU_SCOPED_LOCK(_doing_mutex);
        not_alive_regions[resource_tag].insert(region_id);
    }
}

void RegionManager::recovery_single_region_by_init_region(const std::set<int64_t> region_ids,
                    std::vector<std::string>& instances,
                    std::vector<pb::PeerStateInfo>& recover_region_way) {
    pb::RegionIds query_region_request;
    for (auto region_id : region_ids) {
        query_region_request.add_region_ids(region_id);
    }
    // 1. 查询所有store确认region没有
    // 2. restore_region
    std::map<int64_t, std::map<std::string, pb::RegionInfo>> exist_regions;
    ConcurrencyBthread query_bth(5, &BTHREAD_ATTR_SMALL);
    for (auto& instance : instances) {
        auto query_region_func = [&query_region_request, &instance, &exist_regions]() {
            StoreInteract store_interact(instance);
            pb::StoreRes res; 
            store_interact.send_request("query_region", query_region_request, res);
            DB_WARNING("send query_region to %s request:%s", instance.c_str(), 
                query_region_request.ShortDebugString().c_str());
            for (auto region : res.regions()) {
                exist_regions[region.region_id()][instance] = region;
            }
        };
        query_bth.run(query_region_func);
    }
    query_bth.join();

    for (int64_t region_id : region_ids) {
        pb::RegionInfo select_info;
        auto iter = exist_regions.find(region_id);
        if (iter != exist_regions.end()) {
            DB_WARNING("region_id:%ld has %d peer alive %s", region_id, iter->second.size());
            int64_t max_log_index = 0;
            bool has_leader = false;
            for (auto region_info_pair : iter->second) {
                if (region_info_pair.second.leader() != "0.0.0.0:0") {
                    DB_WARNING("region_id:%ld has leader:%s", region_id,
                        region_info_pair.second.ShortDebugString().c_str());
                    has_leader = true;
                    break;
                } else if (region_info_pair.second.log_index() > max_log_index) {
                    select_info = region_info_pair.second;
                    max_log_index = region_info_pair.second.log_index();
                    select_info.set_leader(region_info_pair.first);
                }
            }
            if (has_leader) {
                continue;
            }
            pb::RaftControlRequest request;
            request.set_op_type(pb::SetPeer);
            request.set_region_id(region_id);
            request.add_new_peers(select_info.leader());
            request.set_force(true);
            StoreInteract store_interact(select_info.leader());
            pb::RaftControlResponse response; 
            int ret = store_interact.send_request_for_leader("region_raft_control", request, response);
            DB_WARNING("send SetPeer request:%s, response:%s",
                        request.ShortDebugString().c_str(),
                        response.ShortDebugString().c_str());
            if (ret == 0) {
                _region_peer_state_map.erase(region_id);
                pb::PeerStateInfo peer_status;
                peer_status.set_region_id(region_id);
                peer_status.set_peer_id(select_info.leader());
                peer_status.set_table_id(select_info.table_id());
                peer_status.set_peer_status(pb::STATUS_SET_PEER);
                BAIDU_SCOPED_LOCK(_doing_mutex);
                recover_region_way.push_back(peer_status);
            } else {
                DB_FATAL("send SetPeer failed, request:%s, response:%s",
                        request.ShortDebugString().c_str(),
                        response.ShortDebugString().c_str());
            }
            continue;
        } else {
            SmartRegionInfo region_ptr = _region_info_map.get(region_id);
            if (region_ptr == nullptr) {
                DB_WARNING("region_id: %ld not found in meta", region_id);
                continue;
            } else {
                region_ptr->set_log_index(0);
                pb::RegionInfo region_info = *region_ptr;
                pb::InitRegion init_region_request;
                init_region_request.set_snapshot_times(2);
                region_info.set_can_add_peer(false);
                region_info.clear_peers();
                region_info.add_peers(region_info.leader());
                region_info.set_status(pb::IDLE);
                region_info.set_can_add_peer(false);
                region_info.set_timestamp(time(NULL));
                *(init_region_request.mutable_region_info()) = region_info;
                //leader发送请求
                StoreInteract store_interact(init_region_request.region_info().leader().c_str());
                pb::StoreRes res; 
                auto ret = store_interact.send_request("init_region", init_region_request, res);
                DB_WARNING("send init_region request:%s, response:%s",
                            init_region_request.ShortDebugString().c_str(),
                            res.ShortDebugString().c_str());
                if (ret < 0) { 
                    DB_FATAL("init region fail, address:%s, region_id: %ld", 
                            init_region_request.region_info().leader().c_str(), region_id);
                } else {
                    _region_peer_state_map.erase(region_id);
                    pb::PeerStateInfo peer_status;
                    peer_status.set_region_id(region_id);
                    peer_status.set_peer_id(region_info.leader());
                    peer_status.set_table_id(region_ptr->table_id());
                    peer_status.set_peer_status(pb::STATUS_INITED);
                    BAIDU_SCOPED_LOCK(_doing_mutex);
                    recover_region_way.push_back(peer_status);
                }
            }
        }
    }
}

void RegionManager::recovery_all_region(const pb::MetaManagerRequest& request,
            pb::MetaManagerResponse* response) {
    std::set<std::string> resource_tags;
    for (int32_t i = 0; i < request.resource_tags_size(); i++) {
        // 按指定resource_tags恢复
        std::string resource_tag = request.resource_tags(i);
        int ret = ClusterManager::get_instance()->check_resource_tag_exist(resource_tag);
        if (ret < 0) {
            response->set_errcode(pb::INPUT_PARAM_ERROR);
            response->set_op_type(request.op_type());
            response->set_errmsg(resource_tag + "not exist");
            DB_WARNING("resource_tag: %s not exist", resource_tag.c_str());
            return;
        }
        resource_tags.insert(resource_tag);
    }
    std::map<int64_t, std::set<std::string>> region_peers_map;
    auto get_peers_func = [&region_peers_map](const int64_t region_id, RegionPeerState& region_state) {
        for (auto& pair : region_state.legal_peers_state) {
            if (pair.second.peer_status() == pb::STATUS_NORMAL 
                && (butil::gettimeofday_us() - pair.second.timestamp() >
                FLAGS_store_heart_beat_interval_us * FLAGS_region_faulty_interval_times)) {
                pair.second.set_peer_status(pb::STATUS_NOT_HEARTBEAT);
            }
        }
        bool heathly = false;
        for (auto& pair : region_state.legal_peers_state) {
            if (pair.second.peer_status() == pb::STATUS_NORMAL) {
                heathly = true;
            }
        }
        if (!heathly) {
            for (auto& pair : region_state.legal_peers_state) {
                region_peers_map[region_id].insert(pair.second.peer_id());
            }
            for (auto& pair : region_state.ilegal_peers_state) {
                region_peers_map[region_id].insert(pair.second.peer_id());
            }
        }
    };
    _region_peer_state_map.traverse_with_key_value(get_peers_func);
    ConcurrencyBthread recovery_bth(30, &BTHREAD_ATTR_SMALL);
    auto iter = region_peers_map.cbegin();
    std::vector<pb::PeerStateInfo> recover_region_way;
    std::map<std::string, std::set<int64_t>> not_alive_regions;
    while (iter != region_peers_map.cend()) {
        int64_t region_id = iter->first;
        std::set<std::string> peers = iter->second;
        auto recovery_func = [this, region_id, &resource_tags, &peers,
                            &not_alive_regions, &recover_region_way]() {
            recovery_single_region_by_set_peer(region_id, resource_tags, peers,
                           not_alive_regions, recover_region_way);
        };
        recovery_bth.run(recovery_func);
        ++iter;
    }
    recovery_bth.join();
    std::map<std::string, std::vector<std::string>> instances;
    ClusterManager::get_instance()->get_instance_by_resource_tags(instances);
    for (auto iter : not_alive_regions) {
        auto it = instances.find(iter.first);
        if (it != instances.end()) {
            recovery_single_region_by_init_region(iter.second, it->second, recover_region_way);
        }
    }

    if (recover_region_way.size() > 0) {
        pb::RegionRecoverResponse* recover_res = response->mutable_recover_response();
        for (auto& peer : recover_region_way) {
            if (peer.peer_status() == pb::STATUS_SET_PEER) {
                recover_res->add_set_peer_regions()->CopyFrom(peer); 
            } else {
                recover_res->add_inited_regions()->CopyFrom(peer);
            }
        }
    }
}

}//namespace 

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
