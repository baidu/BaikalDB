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

#pragma once

#include <unordered_map>
#include <set>
#include <mutex>
#include "proto/meta.interface.pb.h"
#include "proto/store.interface.pb.h"
#include "meta_server.h"
#include "schema_manager.h"
#include "cluster_manager.h"
#include "table_manager.h"

namespace baikaldb {
struct RegionStateInfo { 
    int64_t timestamp; //上次收到该实例心跳的时间戳
    pb::Status status; //实例状态
};
struct RegionPeerState {
    std::map<std::string, pb::PeerStateInfo> legal_peers_state;  // peer in raft-group
    std::map<std::string, pb::PeerStateInfo> ilegal_peers_state; // peer not in raft-group
};
typedef std::shared_ptr<RegionStateInfo> SmartRegionStateInfo;
class RegionManager {
public:
    ~RegionManager() {
        bthread_mutex_destroy(&_instance_region_mutex);
        bthread_mutex_destroy(&_count_mutex);
        bthread_mutex_destroy(&_doing_mutex);
    }
    static RegionManager* get_instance() {
        static RegionManager instance;
        return &instance;
    }
    friend class QueryRegionManager;
    void update_region(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void restore_region(const pb::MetaManagerRequest& request, pb::MetaManagerResponse* response);
    void drop_region(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void split_region(const pb::MetaManagerRequest& request, braft::Closure* done);
    void send_remove_region_request(const std::vector<int64_t>& drop_region_ids);

    // MIGRATE/DEAD可以任意使用add_peer_for_store/delete_all_region_for_store
    // 内部都判断了状态
    // default for MIGRATE
    void add_peer_for_store(const std::string& instance, InstanceStateInfo status);
    // default for DEAD
    void delete_all_region_for_store(const std::string& instance, InstanceStateInfo status);
    void pre_process_remove_peer_for_store(const std::string& instance,
                                                pb::Status status, std::vector<pb::RaftControlRequest>& requests); 
    void pre_process_add_peer_for_store(const std::string& instance, pb::Status status,
            std::unordered_map<std::string, std::vector<pb::AddPeer>>& add_peer_requests);
    bool add_region_is_exist(int64_t table_id, const std::string& start_key, 
                                            const std::string& end_key);
    void check_update_region(const pb::BaikalHeartBeatRequest* request,
                pb::BaikalHeartBeatResponse* response);
    void add_region_info(const std::vector<int64_t>& new_add_region_ids, 
                         pb::BaikalHeartBeatResponse* response);
    SmartRegionInfo get_region_info(int64_t region_id);
    void get_region_info(const std::vector<int64_t>& region_ids,
                         std::vector<SmartRegionInfo>& region_infos);

    void update_leader_status(const pb::StoreHeartBeatRequest* request);
    void leader_heartbeat_for_region(const pb::StoreHeartBeatRequest* request, 
                                      pb::StoreHeartBeatResponse* response);
    void check_whether_update_region(int64_t region_id,
                                     const pb::LeaderHeartBeat& leader_region,
                                     const SmartRegionInfo& master_region_info,
                                     const std::set<std::string>& peers_in_heart,
                                     const std::set<std::string>& peers_in_master);
    //是否有超过replica_num数量的region, 这种region需要删掉多余的peer
    void check_peer_count(int64_t region_id,
                        const pb::LeaderHeartBeat& leader_region,
                        const std::set<std::string>& peers_in_heart,
                        const std::set<std::string>& peers_in_master,
                        std::unordered_map<int64_t, int64_t>& table_replica_nums,
                        std::unordered_map<int64_t, std::string>& table_resource_tags,
                        std::unordered_map<int64_t, std::unordered_map<std::string, int64_t>>& table_replica_dists_maps,
                        std::unordered_map<std::string, std::string>& peer_resource_tags,
                        std::vector<std::pair<std::string, pb::RaftControlRequest>>& remove_peer_requests,
                        pb::StoreHeartBeatResponse* response);
    
    void check_whether_illegal_peer(const pb::StoreHeartBeatRequest* request,
                pb::StoreHeartBeatResponse* response);

    void leader_load_balance(bool whether_can_decide,
                    bool load_balance,
                    const pb::StoreHeartBeatRequest* request,
                    pb::StoreHeartBeatResponse* response);
    
    void peer_load_balance(const std::unordered_map<int64_t, int64_t>& add_peer_counts,
                std::unordered_map<int64_t, std::vector<int64_t>>& instance_regions,
                const std::string& instance,
                const std::string& resouce_tag,
                std::unordered_map<int64_t, std::string>& logical_rooms,
                std::unordered_map<int64_t, int64_t>& table_average_counts);
   
    int load_region_snapshot(const std::string& value);
    void migirate_region_for_store(const std::string& instance);
    void region_healthy_check_function();
    void reset_region_status();
    
    void clear() {
        _region_info_map.clear();
        _region_state_map.clear();
        _region_peer_state_map.clear();
        _instance_region_map.clear();
        _instance_leader_count.clear();
        _incremental_region_info.clear();
    }

    void clear_region_peer_state_map() {
        _region_peer_state_map.clear();
    }

public:
    void set_max_region_id(int64_t max_region_id) {
        _max_region_id = max_region_id;
    }
    int64_t get_max_region_id() {
        return _max_region_id;
    }
    
    void get_region_peers(int64_t region_id, std::vector<std::string>& peers) {
        SmartRegionInfo region_ptr = _region_info_map.get(region_id);
        if (region_ptr != nullptr) {
            for (auto peer : region_ptr->peers()) {
                peers.push_back(peer);
            }
        }
    }
    void get_region_ids(const std::string& instance, std::vector<int64_t>& region_ids) {
        BAIDU_SCOPED_LOCK(_instance_region_mutex);
        if (_instance_region_map.find(instance) ==  _instance_region_map.end()) {
            return;
        }
        for (auto& table_ids : _instance_region_map[instance]) {
            for (auto& region_id : table_ids.second) {
                region_ids.push_back(region_id);
            }
        }
    }
    void print_region_ids(const std::string& instance) {
        BAIDU_SCOPED_LOCK(_instance_region_mutex);
        if (_instance_region_map.find(instance) ==  _instance_region_map.end()) {
            return;
        }
        for (auto& table_id : _instance_region_map[instance]) {
            for (auto& region_id : table_id.second) {
                DB_WARNING("table_id: %ld, region_id: %ld in store: %s", 
                        table_id.first, region_id, instance.c_str());
            }
        }
    }
    int get_region_status(int64_t region_id, pb::Status& status) {
        RegionStateInfo region_state = _region_state_map.get(region_id);
        if (region_state.timestamp == 0) {
            return -1;
        }
        status = region_state.status;
        return 0;
    }

    void recovery_single_region_by_set_peer(const int64_t region_id,
                                const std::set<std::string>& resource_tags,
                                std::set<std::string> peers,
                                std::map<std::string, std::set<int64_t>>& not_alive_regions,
                                std::vector<pb::PeerStateInfo>& recover_region_way);
    void recovery_single_region_by_init_region(const std::set<int64_t> region_ids,
                    std::vector<std::string>& instances,
                    std::vector<pb::PeerStateInfo>& recover_region_way);
    void recovery_all_region(const pb::MetaManagerRequest& request, pb::MetaManagerResponse* response);

    void whether_add_instance(const std::map<std::string, int64_t>& uniq_instance) {
        std::map<std::string, int64_t> add_instances;
        for (auto& peer_pair : uniq_instance) {
            bool exist = ClusterManager::get_instance()->instance_exist(peer_pair.first);
            if (!exist) {
                DB_WARNING("peer: %s not exist in meta server", peer_pair.first.c_str());
                add_instances[peer_pair.first] = peer_pair.second;
            }
        }
        if (add_instances.size() > 0) {
            auto add_instance_fun = [add_instances] {
                for (auto peer_pair :add_instances) {
                    std::string resource_tag;
                    TableManager::get_instance()->get_resource_tag(peer_pair.second, resource_tag);
                    pb::MetaManagerRequest request;
                    request.set_op_type(pb::OP_ADD_INSTANCE);
                    pb::InstanceInfo* instance_info = request.mutable_instance();
                    instance_info->set_address(peer_pair.first);
                    instance_info->set_resource_tag(resource_tag);
                    instance_info->set_status(pb::FAULTY);
                    ClusterManager::get_instance()->process_cluster_info(NULL, &request, NULL, NULL);
                }
            };
            Bthread bth;
            bth.run(add_instance_fun);
        }
    }

    void set_region_info(const pb::RegionInfo& region_info) {
        int64_t table_id = region_info.table_id();
        int64_t region_id = region_info.region_id();
        SmartRegionInfo region_ptr = _region_info_map.get(region_id);
        {
            BAIDU_SCOPED_LOCK(_instance_region_mutex);
            if (region_ptr != nullptr) {
                for (auto& peer : region_ptr->peers()) {
                    _instance_region_map[peer][table_id].erase(region_id);
                }
            }
            for (auto& peer : region_info.peers()) {
                _instance_region_map[peer][table_id].insert(region_id);
            }
            //如果原peer下已不存在region，则erase这个table_id
            if (region_ptr != nullptr) {
                for (auto& peer : region_ptr->peers()) {
                    if (_instance_region_map[peer][table_id].size() <= 0) {
                        _instance_region_map[peer].erase(table_id);
                    } 
                }
            }
         }
        auto ptr_region = std::make_shared<pb::RegionInfo>(region_info);
        _region_info_map.set(region_id, ptr_region);
    }
    
    void set_region_state(int64_t region_id, const RegionStateInfo& region_state) {
        _region_state_map.set(region_id, region_state);
    }
   
    void erase_region_state(const std::vector<std::int64_t>& drop_region_ids) {
        for (auto& region_id : drop_region_ids) {
            _region_state_map.erase(region_id);
        }
    }
    void set_region_mem_info(int64_t region_id, 
                             int64_t new_log_index,
                             int64_t used_size,
                             int64_t num_table_lines) {
        SmartRegionInfo region_ptr = _region_info_map.get(region_id);
        if (region_ptr != nullptr) {
            region_ptr->set_log_index(new_log_index);
            region_ptr->set_used_size(used_size);
            region_ptr->set_num_table_lines(num_table_lines);
        }
    }
    void set_region_leader(int64_t region_id,
                            const std::string& new_leader) {
        SmartRegionInfo region_ptr = _region_info_map.get(region_id);
        if (region_ptr != nullptr) {
            auto new_region_ptr = std::make_shared<pb::RegionInfo>(*region_ptr);
            new_region_ptr->set_leader(new_leader);
            _region_info_map.set(region_id, new_region_ptr);
        }
    }
    void erase_region_info(const std::vector<int64_t>& drop_region_ids, 
                            std::vector<int64_t>& result_region_ids,
                            std::vector<int64_t>& result_partition_ids,
                            std::vector<int64_t>& result_table_ids,
                            std::vector<std::string>& result_start_keys,
                            std::vector<std::string>& result_end_keys);
    
    void erase_region_info(const std::vector<int64_t>& drop_region_ids) {
        std::vector<int64_t> result_region_ids;
        std::vector<int64_t> result_partition_ids;
        std::vector<int64_t> result_table_ids;
        std::vector<std::string> result_start_keys;
        std::vector<std::string> result_end_keys;
        erase_region_info(drop_region_ids, result_region_ids, result_partition_ids, 
                          result_table_ids, result_start_keys, result_end_keys);
    }
   
    void set_instance_leader_count(const std::string& instance, const std::unordered_map<int64_t, int64_t>& table_leader_count) {
        BAIDU_SCOPED_LOCK(_count_mutex);
        _instance_leader_count[instance] = table_leader_count;
    }

    int64_t get_leader_count(const std::string& instance, int64_t table_id) {
        BAIDU_SCOPED_LOCK(_count_mutex);
        if (_instance_leader_count.find(instance) == _instance_leader_count.end()
                || _instance_leader_count[instance].find(table_id) == _instance_leader_count[instance].end()) {
            return 0;
        }
        return _instance_leader_count[instance][table_id];
    }
    void add_leader_count(const std::string& instance, int64_t table_id) {
        BAIDU_SCOPED_LOCK(_count_mutex);
        _instance_leader_count[instance][table_id]++;
    }
    std::string construct_region_key(int64_t region_id) {
        std::string region_key = MetaServer::SCHEMA_IDENTIFY + MetaServer::REGION_SCHEMA_IDENTIFY;
        region_key.append((char*)&region_id, sizeof(int64_t));
        return region_key;
    }
    
    std::string construct_max_region_id_key() {
        std::string max_region_id_key = MetaServer::SCHEMA_IDENTIFY 
                            + MetaServer::MAX_ID_SCHEMA_IDENTIFY
                            + SchemaManager::MAX_REGION_ID_KEY;
        return max_region_id_key;
    }
 
    void traverse_region_map(const std::function<void(SmartRegionInfo& region)>& call) {
        _region_info_map.traverse(call);
    }
    void traverse_copy_region_map(const std::function<void(SmartRegionInfo& region)>& call) {
        _region_info_map.traverse_copy(call);
    }
    ThreadSafeMap<int64_t, RegionPeerState>&  region_peer_state_map() {
        return _region_peer_state_map;
    }
    void put_incremental_regioninfo(const int64_t apply_index, std::vector<pb::RegionInfo>& region_infos);
    bool check_and_update_incremental(const pb::BaikalHeartBeatRequest* request,
                         pb::BaikalHeartBeatResponse* response, int64_t applied_index); 
    
private:
    RegionManager(): _max_region_id(0) {
        bthread_mutex_init(&_instance_region_mutex, NULL);
        bthread_mutex_init(&_count_mutex, NULL);
        bthread_mutex_init(&_doing_mutex, NULL);
    }
private:
    int64_t                                             _max_region_id;
    
    //region_id 与table_id的映射关系, key:region_id, value:table_id
    ThreadSafeMap<int64_t, SmartRegionInfo>             _region_info_map;
    
    bthread_mutex_t                                     _instance_region_mutex;
    //实例和region_id的映射关系，在需要主动发送迁移实例请求时需要
    std::unordered_map<std::string, std::unordered_map<int64_t, std::set<int64_t>>>  _instance_region_map;

    ThreadSafeMap<int64_t, RegionStateInfo>        _region_state_map;
    ThreadSafeMap<int64_t, RegionPeerState>        _region_peer_state_map;
    //该信息只在meta_server的leader中内存保存, 该map可以单用一个锁
    bthread_mutex_t                                     _count_mutex;
    std::unordered_map<std::string, std::unordered_map<int64_t, int64_t>> _instance_leader_count;

    bthread_mutex_t                                     _doing_mutex;
    std::set<std::string>                               _doing_migrate; 
    IncrementalUpdate<std::vector<pb::RegionInfo>> _incremental_region_info;
}; //class

}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
