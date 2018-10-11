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
DECLARE_int32(store_heart_beat_interval_us);
DECLARE_int32(region_faulty_interval_times);
//增加或者更新region信息
//如果是增加，则需要更新表信息, 只有leader的上报会调用该接口
void RegionManager::update_region(const pb::MetaManagerRequest& request, braft::Closure* done) {
    int64_t region_id = request.region_info().region_id();
    int64_t table_id = request.region_info().table_id();
    int64_t partition_id = request.region_info().partition_id();
    auto ret = TableManager::get_instance()->whether_exist_table_id(table_id); 
    if (ret < 0) {
        DB_WARNING("table name:%s not exist, region_info:%s", 
                    request.region_info().table_name().c_str(),
                    request.region_info().ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
        return;
    }
    bool new_add = true;
    if (_region_info_map.find(region_id) != _region_info_map.end()) {
        auto& mutable_region_info = const_cast<pb::RegionInfo&>(request.region_info()); 
        mutable_region_info.set_conf_version(_region_info_map[region_id]->conf_version() + 1);
        new_add = false;
    }
    std::string region_value;
    if (!request.region_info().SerializeToString(&region_value)) {
        DB_WARNING("request serializeToArray fail, request:%s",
                    request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
        return;
    }
    ret = MetaRocksdb::get_instance()->put_meta_info(construct_region_key(region_id), region_value);
    if (ret < 0) {
        DB_WARNING("update region_id: %ld to rocksdb fail",
                        region_id);
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    //更新内存值
    set_region_info(request.region_info());
    RegionStateInfo region_state;
    region_state.timestamp = butil::gettimeofday_us();
    region_state.status = pb::NORMAL;
    set_region_state(region_id, region_state);
    if (new_add) {
        DB_WARNING("region id: %ld is new", region_id);
        TableManager::get_instance()->add_region_id(table_id, partition_id, region_id);
    }
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("update region success, request:%s", request.ShortDebugString().c_str());
}

//根据region_id恢复store上误删除的region
//如果待回复的reigon信息存在则直接新建，如果不存在，则根据前后region恢复
void RegionManager::restore_region(const pb::MetaManagerRequest& request, braft::Closure* done) {
    int64_t region_id = request.restore_region().restore_region_id();
    int64_t lower_region_id = request.restore_region().lower_region_id();
    int64_t upper_region_id = request.restore_region().upper_region_id();
    pb::RegionInfo region_info;
    if (_region_info_map.count(region_id) != 0) {
        region_info = *(_region_info_map[region_id]);
    } else if (_region_info_map.count(lower_region_id) != 0 && 
            _region_info_map.count(upper_region_id) != 0) {
        region_info = *(_region_info_map[lower_region_id]);
        region_info.clear_peers();
        region_info.add_peers(region_info.leader());
        region_info.set_start_key(_region_info_map[lower_region_id]->end_key());
        region_info.set_end_key(_region_info_map[upper_region_id]->start_key());
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
        DB_WARNING("region id:%ld not exist", region_id);
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "region not exist");
        return;
    }
    pb::InitRegion init_region_request;
    init_region_request.set_snapshot_times(2);
    region_info.set_can_add_peer(false);
    *(init_region_request.mutable_region_info()) = region_info;
    //leader发送请求
    if (done) {
        StoreInteract store_interact(init_region_request.region_info().leader().c_str());
        pb::StoreRes res; 
        auto ret = store_interact.send_request("init_region", init_region_request, res);
        if (ret < 0) { 
            DB_FATAL("create table fail, address:%s, region_id: %ld", 
                        init_region_request.region_info().leader().c_str(),
                        region_id);
            IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "new region fail");
            return;
        }    
        IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
        DB_NOTICE("new region_id: %ld success, table_name:%s", 
                region_id, region_info.table_name().c_str());
    }
}
//删除region_id的操作只会在表已经删除或创建失败的情况下才会调用
//所以在删除region时表信息已经不存在，不在需要更新表信息
void RegionManager::drop_region(const pb::MetaManagerRequest& request, braft::Closure* done) {
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
    erase_region_info(drop_region_ids, result_region_ids, result_partition_ids, result_table_ids);
    erase_region_state(drop_region_ids);
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
        std::vector<std::string> peers;
        get_region_peers(drop_region_id, peers);
        for (auto& peer : peers) {
            auto drop_region_fun = [&concurrency_cond, log_id, peer, drop_region_id] {
                std::shared_ptr<BthreadCond> auto_decrease(&concurrency_cond,
                            [](BthreadCond* cond) {cond->decrease_signal();});
                pb::RemoveRegion request;
                request.set_force(true);
                request.set_region_id(drop_region_id);
                StoreInteract store_interact(peer.c_str());
                pb::StoreRes response; 
                auto ret = store_interact.send_request("remove_region", request, response);
                if (ret < 0) { 
                    DB_FATAL("drop region fail, peer: %s, drop_region_id: %ld", peer.c_str(), drop_region_id);
                    return;
                }
                DB_NOTICE("send remove region request:%s, response:%s, peer_address:%s, region_id:%ld",
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
void RegionManager::delete_all_region_for_store(const std::string& instance, pb::Status status) {
    DB_WARNING("delete all region for dead store start, dead_store:%s", instance.c_str());
    std::string resource_tag = ClusterManager::get_instance()->get_instance(instance).resource_tag;
    //实例上已经没有reigon了，直接删除该实例即可
    std::vector<int64_t> region_ids;
    get_region_ids(instance, region_ids);
    if (region_ids.size() == 0) {
        {
            BAIDU_SCOPED_LOCK(_resource_tag_mutex);
            _resource_tag_delete_region_map[resource_tag] = false;
        }
        if (status == pb::DEAD) {
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
        BAIDU_SCOPED_LOCK(_resource_tag_mutex);
        if (_resource_tag_delete_region_map[resource_tag] == true) {
            DB_WARNING("resoruce_tag:%s, instance:%s has doing by other instance",
                    resource_tag.c_str(), instance.c_str());
            return;
        }
    }
    std::vector<pb::RaftControlRequest> requests;
    pre_process_remove_peer_for_store(instance, requests);
    BthreadCond concurrency_cond(-FLAGS_concurrency_num);
    for (auto request : requests) {
        auto remove_peer_fun = [this, request, &concurrency_cond] () {
            StoreInteract store_interact(request.new_leader().c_str());
            pb::RaftControlResponse response; 
            store_interact.send_request("region_raft_control", request, response);
            DB_WARNING("send remove peer request:%s, response:%s",
                        request.ShortDebugString().c_str(),
                        response.ShortDebugString().c_str());
            concurrency_cond.decrease_signal();
        };
        Bthread bth;
        concurrency_cond.increase();
        concurrency_cond.wait();
        bth.run(remove_peer_fun);
    }
    concurrency_cond.wait(-FLAGS_concurrency_num);
    DB_WARNING("delete all region for dead store end, dead_store:%s", instance.c_str());
}

void RegionManager::pre_process_remove_peer_for_store(const std::string& instance,
                std::vector<pb::RaftControlRequest>& requests) {
    std::vector<int64_t> region_ids;
    get_region_ids(instance, region_ids);
    std::string resource_tag = ClusterManager::get_instance()->get_instance(instance).resource_tag;
    for (auto& region_id : region_ids) {
        auto ptr_region = get_region_info(region_id);
        if (ptr_region == nullptr) {
            continue;
        }
        // TODO liguoqiang 尝试add_peer
        if (ptr_region->peers_size() <= 1) {
            DB_FATAL("region_id:%ld has only one peer, can not been remove, instance%s",
                        region_id, instance.c_str());
            std::string new_instance;
            std::set<std::string> peers;
            peers.insert(ptr_region->peers(0));
            // 故障需要尽快恢复，轮询最均匀
            auto ret = ClusterManager::get_instance()->select_instance_rolling(
                    resource_tag,
                    peers,
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
                [add_peer]() {
                    StoreInteract store_interact(add_peer.old_peers(0).c_str());
                    pb::StoreRes response; 
                    auto ret = store_interact.send_request("add_peer", add_peer, response);
                    DB_WARNING("send add peer leader: %s, request:%s, response:%s, ret: %d",
                            add_peer.old_peers(0).c_str(),
                            add_peer.ShortDebugString().c_str(),
                            response.ShortDebugString().c_str(), ret);
                };
            bth.run(add_peer_fun);
            continue;
        }
        pb::Status status;
        auto ret = get_region_status(region_id, status);
        if (ret < 0 || status != pb::NORMAL) {
            DB_FATAL("region_id:%ld status is not normal, can not been remove, instance:%s",
                    region_id, instance.c_str());
            continue;
        }
        std::string leader = ptr_region->leader();
        pb::RaftControlRequest request;
        request.set_op_type(pb::SetPeer);
        request.set_region_id(region_id);
        for (auto peer : ptr_region->peers()) {
            request.add_old_peers(peer);
            if (peer != instance) {
                request.add_new_peers(peer);
            }
        }
        // TODO 尝试切主
        if (leader == instance) {
            leader = request.new_peers(0);
        }
        request.set_new_leader(leader);
        requests.push_back(request);
    }
}
void RegionManager::check_update_region(const pb::BaikalHeartBeatRequest* request,
            pb::BaikalHeartBeatResponse* response) {
    BAIDU_SCOPED_LOCK(_region_mutex);
    for (auto& schema_heart_beat : request->schema_infos()) { 
        for (auto& region_info : schema_heart_beat.regions()) {
            int64_t region_id = region_info.region_id();
            if (_region_info_map.find(region_id) == _region_info_map.end()) {
                continue;
            }
            //这种场景出现在分裂的时候，baikal会先从store获取新的region信息，不需要更新
            if (region_info.version() > _region_info_map[region_id]->version()) {
                continue;
            }
            if (region_info.version() < _region_info_map[region_id]->version()
                    || _region_info_map[region_id]->conf_version() > region_info.conf_version()) {
                *(response->add_region_change_info()) = *(_region_info_map[region_id]);
            }
        } 
    }
}

void RegionManager::add_region_info(const std::vector<int64_t>& new_add_region_ids,
                                    pb::BaikalHeartBeatResponse* response) {
    BAIDU_SCOPED_LOCK(_region_mutex);
    for (auto& region_id : new_add_region_ids) {
        if (_region_info_map.find(region_id) == _region_info_map.end()) {
            continue;
        }
        *(response->add_region_change_info()) = *(_region_info_map[region_id]);
    }
}
void RegionManager::leader_load_balance(bool whether_can_decide,
            bool close_load_balance,
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
    if (!whether_can_decide) {
        DB_WARNING("meta state machine can not decide");
        return;
    }
    if (close_load_balance) {
        DB_WARNING("meta state machine close load balance");
        return;
    }
    std::string resource_tag = request->instance_info().resource_tag();
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
    DB_WARNING("transfer lead for instance: %s", instance.c_str());
    for (auto& table_count : transfer_leader_count) {
        DB_WARNING("table_id: %ld, average_leader_count: %ld, should transfer leader count: %ld",
                    table_count.first, average_leader_counts[table_count.first], table_count.second);
    }
    for (auto& leader_region : request->leader_regions()) {
        int64_t table_id = leader_region.region().table_id();
        int64_t region_id = leader_region.region().region_id();
        if (leader_region.status() != pb::IDLE) {
            continue;
        }
        if (leader_region.region().peers_size() != leader_region.region().replica_num()) {
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
// add_peer_count: 每个表需要add_peer的region数量
// instance_regions： add_peer的region从这个候选集中选择
void RegionManager::peer_load_balance(const std::unordered_map<int64_t, int64_t>& add_peer_counts,
        std::unordered_map<int64_t, std::vector<int64_t>>& instance_regions,
        const std::string& instance,
        const std::string& resource_tag) {
    std::vector<std::pair<std::string, pb::AddPeer>> add_peer_requests;
    for (auto& add_peer_count : add_peer_counts) {
        int64_t table_id = add_peer_count.first;
        int64_t replica_num = TableManager::get_instance()->get_replica_num(table_id);
        int64_t count = add_peer_count.second;
        if (instance_regions.find(table_id) == instance_regions.end()) {
            continue;
        }
        size_t total_region_count = instance_regions[table_id].size();
        size_t index = butil::fast_rand() % total_region_count;
        for (size_t i = 0; i < total_region_count; ++i, ++index) {
            int64_t candicate_region = instance_regions[table_id][index];
            auto master_region_info = get_region_info(candicate_region);
            if (master_region_info == nullptr) {
                continue;
            }
            DB_WARNING("master region info: %s", master_region_info->ShortDebugString().c_str());
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
            ret = ClusterManager::get_instance()->select_instance_min(resource_tag, exclude_stores, table_id, new_instance); 
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
        [add_peer_requests]() {
            for (auto request : add_peer_requests) {
                    StoreInteract store_interact(request.first.c_str());
                    pb::StoreRes response; 
                    auto ret = store_interact.send_request("add_peer", request.second, response);
                    DB_WARNING("send add peer leader: %s, request:%s, response:%s, ret: %d",
                                request.first.c_str(),
                                request.second.ShortDebugString().c_str(),
                                response.ShortDebugString().c_str(), ret);
                }
        };
    bth.run(add_peer_fun);
}

void RegionManager::update_leader_status(const pb::StoreHeartBeatRequest* request) {
    BAIDU_SCOPED_LOCK(_region_state_mutex);
    for (auto& leader_region : request->leader_regions()) {
        int64_t region_id = leader_region.region().region_id();
        RegionStateInfo region_state;
        region_state.timestamp = butil::gettimeofday_us();
        region_state.status = pb::NORMAL;
        _region_state_map[region_id] = region_state;
    }
}

void RegionManager::leader_heartbeat_for_region(const pb::StoreHeartBeatRequest* request,
                                                pb::StoreHeartBeatResponse* response) {
    std::string instance = request->instance_info().address();
    std::string resource_tag = request->instance_info().resource_tag();
    std::vector<std::pair<std::string, pb::RaftControlRequest>> remove_peer_requests;
    for (auto& leader_region : request->leader_regions()) {
        const pb::RegionInfo& leader_region_info = leader_region.region();
        int64_t region_id = leader_region_info.region_id();
        auto master_region_info = get_region_info(region_id);
        //新增region, 在meta_server中不存在
        if (master_region_info == nullptr) {
            DB_WARNING("region_info: %s is new ", leader_region_info.ShortDebugString().c_str());
            pb::MetaManagerRequest request;
            request.set_op_type(pb::OP_UPDATE_REGION);
            *(request.mutable_region_info()) = leader_region.region();
            SchemaManager::get_instance()->process_schema_info(NULL, &request, NULL, NULL);
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
        check_peer_count(region_id, 
                        resource_tag, 
                        leader_region, 
                        peers_in_heart,
                        peers_in_master, 
                        remove_peer_requests, 
                        response);
    }
    
    if (remove_peer_requests.size() == 0) {
        return;
    }
    Bthread bth(&BTHREAD_ATTR_SMALL);
    auto remove_peer_fun = 
        [remove_peer_requests]() {
            for (auto request : remove_peer_requests) {
                    StoreInteract store_interact(request.second.new_leader().c_str());
                    pb::RaftControlResponse response; 
                    auto ret = store_interact.send_request("region_raft_control", request.second, response);
                    DB_WARNING("send remove peer request:%s, response:%s, ret: %d",
                                request.second.ShortDebugString().c_str(),
                                response.ShortDebugString().c_str(), ret);
                    if (ret == 0) {
                        pb::RemoveRegion remove_region_request;
                        remove_region_request.set_force(true);
                        remove_region_request.set_region_id(request.second.region_id());
                        StoreInteract store_interact(request.first.c_str());
                        pb::StoreRes remove_region_response; 
                        ret = store_interact.send_request("remove_region", remove_region_request, remove_region_response);
                        DB_WARNING("send remove region request: %s, resposne: %s, ret: %d",
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
        DB_WARNING("log_index:%ld in heart is less than in master:%ld",
                    leader_region_info.log_index(), master_region_info->log_index());
        return;
    }
    bool version_changed = false;
    bool peer_changed = false;
    //version发生变化，说明分裂
    if (leader_region_info.version() > master_region_info->version()
            || leader_region_info.start_key() != master_region_info->start_key()
            || leader_region_info.end_key() != master_region_info->end_key()) {
        version_changed = true;
    }
    //peer发生变化
    if (leader_region.status() == pb::IDLE && peers_in_master != peers_in_heart) {
        peer_changed = true;
    }
    if (version_changed || peer_changed) {
        pb::MetaManagerRequest request;
        request.set_op_type(pb::OP_UPDATE_REGION);
        pb::RegionInfo* tmp_region_info = request.mutable_region_info();
        *tmp_region_info = *master_region_info;
        if (version_changed) {
            tmp_region_info->set_version(leader_region_info.version());
            tmp_region_info->set_start_key(leader_region_info.start_key());
            tmp_region_info->set_end_key(leader_region_info.end_key());
        }
        if (peer_changed) {
            tmp_region_info->set_leader(leader_region_info.leader());
            tmp_region_info->clear_peers();
            for (auto& peer : peers_in_heart) {
                tmp_region_info->add_peers(peer);
            }
            tmp_region_info->set_used_size(leader_region_info.used_size());
            tmp_region_info->set_log_index(leader_region_info.log_index());
        }
        tmp_region_info->set_conf_version(master_region_info->conf_version());
        SchemaManager::get_instance()->process_schema_info(NULL, &request, NULL, NULL);
        return;
    }
    //只更新内存
    if (leader_region.status() == pb::IDLE &&  leader_region_info.leader() != master_region_info->leader()) {
        set_region_leader(region_id, leader_region_info.leader());
    }
    if (leader_region.status() == pb::IDLE) {
        set_region_mem_info(region_id, 
                            leader_region_info.log_index(), 
                            leader_region_info.used_size());
    } 
}

void RegionManager::check_peer_count(int64_t region_id,
                                    const std::string& resource_tag,
                                    const pb::LeaderHeartBeat& leader_region,
                                    const std::set<std::string>& peers_in_heart, 
                                    const std::set<std::string>& peers_in_master, 
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
    int64_t replica_num = leader_region_info.replica_num();
    
    // add_peer
    if (leader_region_info.peers_size() < replica_num) {
        std::string new_instance;
        // 故障需要尽快恢复，轮询最均匀
        auto ret = ClusterManager::get_instance()->select_instance_rolling(
                resource_tag,
                peers_in_heart,
                new_instance);
        if (ret < 0) {
            DB_FATAL("select store from cluster fail, region_id:%ld", region_id);
            return;
        }
        pb::AddPeer* add_peer = response->add_add_peers();
        add_peer->set_region_id(region_id);
        for (auto& peer : leader_region_info.peers()) {
            add_peer->add_old_peers(peer);
            add_peer->add_new_peers(peer);
        }
        add_peer->add_new_peers(new_instance);
        return;
    }
    //选择一个peer被remove
    if (leader_region_info.peers_size() > replica_num) {
        pb::RaftControlRequest remove_peer_request;
        int64_t min_peer_count = 0;
        remove_peer_request.set_op_type(pb::SetPeer);
        remove_peer_request.set_region_id(region_id);
        std::string remove_peer;
        for (auto& peer : peers_in_heart) {
            if (peer == leader_region_info.leader()) {
                continue;
            }
            int64_t peer_count = ClusterManager::get_instance()->get_peer_count(peer, table_id);
            DB_WARNING("cadidate remove peer, peer_count: %ld, instance: %s, table_id: %ld", peer_count, peer.c_str(), table_id);
            if (peer_count > min_peer_count) {
                remove_peer = peer;
                min_peer_count = peer_count;
            }
        }
        if (remove_peer.empty()) {
            return;
        }
        DB_WARNING("remove peer, peer_count: %ld, instance: %s, table_id: %ld", min_peer_count, remove_peer.c_str(), table_id);
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
        if (master_region_info == nullptr) {
            //这种情况在以下场景中会出现
            //新创建的region_id，该region的leader还没上报心跳，follower先上报了
            continue;
        }  
        if (master_region_info->peers_size() >= master_region_info->replica_num()
                && (peer_info.log_index() != 0 || SchemaManager::get_instance()->get_unsafe_decision())
                && master_region_info->log_index() > peer_info.log_index()) {
            //判断该实例上的peer是不是该region的有效peer，如不是，则删除
            bool legal_peer = false;
            for (auto& peer : master_region_info->peers()) {
                if (peer == instance) {
                    legal_peer = true;
                    break;
                }
            }
            if (!legal_peer) {
                DB_WARNING("region_id:%ld is not legal peer, log_index:%ld,"
                            " master_peer_info: %s, peer_info:%s, peer_address:%s should be delete",
                            region_id, master_region_info->log_index(),
                            master_region_info->ShortDebugString().c_str(),
                            peer_info.ShortDebugString().c_str(),
                            instance.c_str());
                response->add_delete_region_ids(region_id);
            }
        }
    }
}

int RegionManager::load_region_snapshot(const std::string& value) {
    pb::RegionInfo region_pb;
    if (!region_pb.ParseFromString(value)) {
        DB_FATAL("parse from pb fail when load region snapshot, value: %s", value.c_str());
        return -1;
    }
    set_region_info(region_pb);
    RegionStateInfo region_state;
    region_state.timestamp = butil::gettimeofday_us();
    region_state.status = pb::NORMAL;
    set_region_state(region_pb.region_id(), region_state);
    TableManager::get_instance()->add_region_id(region_pb.table_id(), 
                region_pb.partition_id(),
                region_pb.region_id());
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
    BAIDU_SCOPED_LOCK(_region_state_mutex);
    for (auto& region_state : _region_state_map) {
        if (butil::gettimeofday_us() - region_state.second.timestamp > 
                FLAGS_store_heart_beat_interval_us * FLAGS_region_faulty_interval_times) {
            auto region_info = get_region_info(region_state.first);
            if (region_info == nullptr) {
                continue; 
            }
            DB_FATAL("region_id:%ld not recevie heartbeat for a long time, leader:%s", 
                     region_state.first, region_info->leader().c_str());
            region_state.second.status = pb::FAULTY;
        } else {
            region_state.second.status = pb::NORMAL;
        }
    }
}
void RegionManager::reset_region_status() {
    BAIDU_SCOPED_LOCK(_region_state_mutex);
    for (auto& region_state_pair : _region_state_map) {
        region_state_pair.second.timestamp = butil::gettimeofday_us();
        region_state_pair.second.status = pb::NORMAL;
    }
    _instance_leader_count.clear();
}

SmartRegionInfo RegionManager::get_region_info(int64_t region_id) {
    BAIDU_SCOPED_LOCK(_region_mutex);
    if (_region_info_map.find(region_id) == _region_info_map.end()) {
        static SmartRegionInfo tmp;
        return tmp;
    }
    return _region_info_map[region_id];
}

void RegionManager::get_region_info(const std::vector<int64_t>& region_ids, 
            std::vector<SmartRegionInfo>& region_infos) {
    BAIDU_SCOPED_LOCK(_region_mutex);
    for (auto& region_id : region_ids) {
        if (_region_info_map.find(region_id) == _region_info_map.end()) {
            DB_WARNING("region_id: %ld not exist", region_id);
            continue;
        }
        region_infos.push_back(_region_info_map[region_id]);
    }
}

}//namespace 

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
