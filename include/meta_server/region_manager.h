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

#pragma once

#include <unordered_map>
#include <set>
#include <mutex>
#include "proto/meta.interface.pb.h"
#include "proto/store.interface.pb.h"
#include "meta_server.h"
#include "schema_manager.h"

namespace baikaldb {
typedef std::shared_ptr<pb::RegionInfo> SmartRegionInfo;
struct RegionStateInfo { 
    int64_t timestamp; //上次收到该实例心跳的时间戳
    pb::Status status; //实例状态
}; 
     
class RegionManager {
public:
    ~RegionManager() {
        bthread_mutex_destroy(&_region_mutex);
        bthread_mutex_destroy(&_count_mutex);
        bthread_mutex_destroy(&_region_state_mutex);
    }
    static RegionManager* get_instance() {
        static RegionManager instance;
        return &instance;
    }
    friend class QueryRegionManager;
    void update_region(const pb::MetaManagerRequest& request, braft::Closure* done);
    void restore_region(const pb::MetaManagerRequest& request, braft::Closure* done);
    void drop_region(const pb::MetaManagerRequest& request, braft::Closure* done);
    void split_region(const pb::MetaManagerRequest& request, braft::Closure* done);
    void send_remove_region_request(const std::vector<int64_t>& drop_region_ids);

    void delete_all_region_for_dead_store(const std::string& instance);
    void pre_process_remove_peer_for_dead_store(const std::string& instance,
                                    std::vector<pb::RaftControlRequest>& requests); 

    void check_update_region(const pb::BaikalHeartBeatRequest* request,
                pb::BaikalHeartBeatResponse* response);
    //void add_region_info(const std::set<int64_t>& new_add_region_ids, 
    //                     pb::BaikalHeartBeatResponse* response);
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
                        const std::string& resource_tag,
                        const pb::LeaderHeartBeat& leader_region,
                        const std::set<std::string>& peers_in_heart,
                        const std::set<std::string>& peers_in_master,
                        std::vector<std::pair<std::string, pb::RaftControlRequest>>& remove_peer_requests,
                        pb::StoreHeartBeatResponse* response);
    
    void check_whether_illegal_peer(const pb::StoreHeartBeatRequest* request,
                pb::StoreHeartBeatResponse* response);

    void leader_load_balance(bool whether_can_decide,
                    bool close_load_balance,
                    const pb::StoreHeartBeatRequest* request,
                    pb::StoreHeartBeatResponse* response);
    
    void peer_load_balance(const std::unordered_map<int64_t, int64_t>& add_peer_counts,
                std::unordered_map<int64_t, std::vector<int64_t>>& instance_regions,
                const std::string& instance,
                const std::string& resouce_tag);
   
    int load_region_snapshot(const std::string& value);
    void migirate_region_for_store(const std::string& instance);
    void region_healthy_check_function();
    void reset_region_status();
    
    void clear() {
        _region_info_map.clear();
        _region_state_map.clear();
        _instance_region_map.clear();
        _instance_leader_count.clear();
    }
public:
    void set_max_region_id(int64_t max_region_id) {
        _max_region_id = max_region_id;
    }
    int64_t get_max_region_id() {
        return _max_region_id;
    }
    
    void get_region_peers(int64_t region_id, std::vector<std::string>& peers) {
        BAIDU_SCOPED_LOCK(_region_mutex);
        if (_region_info_map.find(region_id) != _region_info_map.end()) {
            for (auto peer : _region_info_map[region_id]->peers()) {
                peers.push_back(peer);                
            }
        }
    }
    void get_region_ids(const std::string& instance, std::vector<int64_t>& region_ids) {
        BAIDU_SCOPED_LOCK(_region_mutex);
        if (_instance_region_map.find(instance) ==  _instance_region_map.end()) {
            return;
        }
        for (auto& table_ids : _instance_region_map[instance]) {
            for (auto& region_id : table_ids.second) {
                region_ids.push_back(region_id);
            }
        }
    }
    int get_region_status(int64_t region_id, pb::Status& status) {
        BAIDU_SCOPED_LOCK(_region_state_mutex);
        if (_region_state_map.find(region_id) == _region_state_map.end()) {
            return -1;
        }
        status = _region_state_map[region_id].status;
        return 0;
    }
    void set_region_info(const pb::RegionInfo& region_info) {
        int64_t table_id = region_info.table_id();
        int64_t region_id = region_info.region_id();
        BAIDU_SCOPED_LOCK(_region_mutex);
        if (_region_info_map.find(region_id) != _region_info_map.end()) {
            for (auto peer : _region_info_map[region_id]->peers()) {
                _instance_region_map[peer][table_id].erase(region_id);                
            }
        }
        for (auto peer : region_info.peers()) {
            _instance_region_map[peer][table_id].insert(region_id);
        }
        auto ptr_region = std::make_shared<pb::RegionInfo>(region_info);
        _region_info_map[region_id] = ptr_region;
    }
    
    void set_region_state(int64_t region_id, const RegionStateInfo& region_state) {
        BAIDU_SCOPED_LOCK(_region_state_mutex);
        _region_state_map[region_id] = region_state;
    }
   
    void erase_region_state(const std::vector<std::int64_t>& drop_region_ids) {
        BAIDU_SCOPED_LOCK(_region_state_mutex);
        for (auto& region_id : drop_region_ids) {
            _region_state_map.erase(region_id);
        }
    }
    void set_region_mem_info(int64_t region_id, 
                             int64_t new_log_index,
                             int64_t used_size) {
        BAIDU_SCOPED_LOCK(_region_mutex);
        if (_region_info_map.find(region_id) != _region_info_map.end()) {
            _region_info_map[region_id]->set_log_index(new_log_index);
            _region_info_map[region_id]->set_used_size(used_size);
        }
    }
    void set_region_leader(int64_t region_id,
                            const std::string& new_leader) {
        BAIDU_SCOPED_LOCK(_region_mutex);
        if (_region_info_map.find(region_id) != _region_info_map.end()) {
            auto new_region_ptr = std::make_shared<pb::RegionInfo>(*(_region_info_map[region_id]));
            new_region_ptr->set_leader(new_leader);
            _region_info_map[region_id] = new_region_ptr;
        }
    }
    void erase_region_info(const std::vector<int64_t>& drop_region_ids, 
                            std::vector<int64_t>& result_region_ids,
                            std::vector<int64_t>& result_partition_ids,
                            std::vector<int64_t>& result_table_ids) {
        {
            BAIDU_SCOPED_LOCK(_region_mutex);
            for (auto drop_region_id : drop_region_ids) {
                if (_region_info_map.find(drop_region_id) == _region_info_map.end()) {
                    continue;
                }
                result_region_ids.push_back(drop_region_id);
                result_partition_ids.push_back(_region_info_map[drop_region_id]->partition_id());
                int64_t table_id = _region_info_map[drop_region_id]->table_id();
                result_table_ids.push_back(table_id);
                for (auto peer : _region_info_map[drop_region_id]->peers()) {
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
        BAIDU_SCOPED_LOCK(_region_state_mutex);
        for (auto drop_region_id : drop_region_ids) {
            _region_state_map.erase(drop_region_id);
        }
    }
    
    void erase_region_info(const std::vector<int64_t>& drop_region_ids) {
        std::vector<int64_t> result_region_ids;
        std::vector<int64_t> result_partition_ids;
        std::vector<int64_t> result_table_ids;
        erase_region_info(drop_region_ids, result_region_ids, result_partition_ids, result_table_ids);
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
private:
    RegionManager(): _max_region_id(0) {
        bthread_mutex_init(&_region_mutex, NULL);
        bthread_mutex_init(&_count_mutex, NULL);
        bthread_mutex_init(&_region_state_mutex, NULL);
    }
private:
    //std::mutex                                          _region_mutex;
    bthread_mutex_t                                          _region_mutex;
    int64_t                                             _max_region_id;
    
    //region_id 与table_id的映射关系, key:region_id, value:table_id
    std::unordered_map<int64_t, SmartRegionInfo>         _region_info_map;
    //实例和region_id的映射关系，在需要主动发送迁移实例请求时需要
    std::unordered_map<std::string, std::unordered_map<int64_t, std::set<int64_t>>>  _instance_region_map;

    bthread_mutex_t                                     _region_state_mutex;
    std::unordered_map<int64_t, RegionStateInfo>        _region_state_map;
    //该信息只在meta_server的leader中内存保存, 该map可以单用一个锁
    bthread_mutex_t                                          _count_mutex;
    std::unordered_map<std::string, std::unordered_map<int64_t, int64_t>> _instance_leader_count;
}; //class

}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
