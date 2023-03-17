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
#include "meta_util.h"
#include "schema_manager.h"
#include "cluster_manager.h"
#include "table_manager.h"

namespace baikaldb {

DECLARE_int64(modify_learner_peer_interval_us);

struct RegionStateInfo { 
    int64_t timestamp; //上次收到该实例心跳的时间戳
    pb::Status status; //实例状态
};
struct RegionPeerState {
    std::vector<pb::PeerStateInfo> legal_peers_state;  // peer in raft-group
    std::vector<pb::PeerStateInfo> ilegal_peers_state; // peer not in raft-group
};

struct RegionLearnerState {
    std::map<std::string, pb::PeerStateInfo> learner_state_map;
    TimeCost tc;
};

typedef std::shared_ptr<RegionStateInfo> SmartRegionStateInfo;
class RegionManager {
public:
    ~RegionManager() {
        bthread_mutex_destroy(&_instance_region_mutex);
        bthread_mutex_destroy(&_instance_learner_mutex);
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

    void add_all_learner_for_store(const std::string& instance, 
                                   const IdcInfo& instance_idc, 
                                   const std::vector<int64_t>& learner_ids);
    void pre_process_remove_peer_for_store(const std::string& instance,
            const IdcInfo& instance_idc, pb::Status status, 
            std::vector<pb::RaftControlRequest>& requests); 
    void pre_process_add_peer_for_store(const std::string& instance, 
            const IdcInfo& instance_idc, pb::Status status,
            std::unordered_map<std::string, std::vector<pb::AddPeer>>& add_peer_requests);
    bool add_region_is_exist(int64_t table_id, const std::string& start_key, 
                                            const std::string& end_key, int64_t partition_id);
    void check_update_region(const pb::BaikalHeartBeatRequest* request,
                pb::BaikalHeartBeatResponse* response);
    void add_region_info(const std::vector<int64_t>& new_add_region_ids, 
                         pb::BaikalHeartBeatResponse* response);
    SmartRegionInfo get_region_info(int64_t region_id);
    void get_region_info(const std::vector<int64_t>& region_ids,
                         std::vector<SmartRegionInfo>& region_infos);

    void update_leader_status(const pb::StoreHeartBeatRequest* request, int64_t timestamp);

    void leader_heartbeat_for_region(const pb::StoreHeartBeatRequest* request, 
                                      pb::StoreHeartBeatResponse* response);
    void check_whether_update_region(int64_t region_id,
                                     bool peer_changed,
                                     const pb::LeaderHeartBeat& leader_region,
                                     const SmartRegionInfo& master_region_info);
    //是否有超过replica_num数量的region, 这种region需要删掉多余的peer
    void check_peer_count(int64_t region_id,
                        const pb::LeaderHeartBeat& leader_region,
                        std::unordered_map<int64_t, int64_t>& table_replica_nums,
                        std::unordered_map<int64_t, std::unordered_map<std::string, int>>& table_replica_dists_maps,
                        std::vector<std::pair<std::string, pb::RaftControlRequest>>& remove_peer_requests,
                        int32_t table_pk_prefix_dimension,
                        pb::StoreHeartBeatResponse* response);
    

    void add_learner_peer(int64_t region_id,
        std::vector<std::pair<std::string, pb::InitRegion>>& modify_learner_requests,
        pb::RegionInfo* master_region_info,
        const std::string& learner_resource_tag
    );
    void remove_learner_peer(int64_t region_id,
        std::vector<std::pair<std::string, pb::RemoveRegion>>& modify_learner_requests,
        pb::RegionInfo* master_region_info,
        const std::set<std::string>& candicate_remove_learners
    );
    void remove_learner_peer(int64_t region_id,
        std::vector<std::pair<std::string, pb::RemoveRegion>>& modify_learner_requests,
        const std::string& remove_learner
    );
    void check_whether_illegal_peer(const pb::StoreHeartBeatRequest* request,
                pb::StoreHeartBeatResponse* response);

    void leader_load_balance(bool whether_can_decide,
                    bool load_balance,
                    const pb::StoreHeartBeatRequest* request,
                    pb::StoreHeartBeatResponse* response);
    
    void leader_main_logical_room_check(const pb::StoreHeartBeatRequest* request,
                    pb::StoreHeartBeatResponse* response,
                    IdcInfo& leader_idc,
                    std::unordered_map<int64_t, int64_t>& table_replica,
                    std::unordered_map<int64_t, IdcInfo>& table_main_idc,
                    std::set<int64_t>& trans_leader_region_ids);
    
    void leader_load_balance_on_pk_prefix(const std::string& instance,
                                          const pb::StoreHeartBeatRequest* request,
                                          std::unordered_map<int64_t, int64_t>& table_total_instance_counts,
                                          std::unordered_map<int64_t, std::unordered_map<std::string, std::vector<int64_t>>>& pk_prefix_leader_region_map,
                                          const std::set<int64_t>& trans_leader_region_ids,
                                          std::unordered_map<int64_t, int64_t>& table_transfer_leader_count,
                                          std::unordered_map<std::string, int64_t>& pk_prefix_leader_count,
                                          std::unordered_map<int64_t, int64_t>& table_replica,
                                          std::unordered_map<int64_t, IdcInfo>& table_main_idc,
                                          pb::StoreHeartBeatResponse* response);
    
    void pk_prefix_load_balance(const std::unordered_map<std::string, int64_t>& add_peer_counts,
                           std::unordered_map<std::string, std::vector<int64_t>>& instance_regions,
                           const std::string& instance,
                           std::unordered_map<int64_t, IdcInfo>& table_balance_idc,
                           std::unordered_map<std::string, int64_t>& pk_prefix_average_counts,
                           std::unordered_map<int64_t, int64_t>& table_average_counts);
    
    void peer_load_balance(const std::unordered_map<int64_t, int64_t>& add_peer_counts,
                std::unordered_map<int64_t, std::vector<int64_t>>& instance_regions,
                const std::string& instance,
                std::unordered_map<int64_t, IdcInfo>& table_balance_idc,
                std::unordered_map<int64_t, int64_t>& table_average_counts,
                std::unordered_map<int64_t, int32_t>& table_pk_prefix_dimension,
                std::unordered_map<std::string, int64_t>& pk_prefix_average_counts);
   
    void learner_load_balance(const std::unordered_map<int64_t, int64_t>& add_peer_counts,
                std::unordered_map<int64_t, std::vector<int64_t>>& instance_regions,
                const std::string& instance,
                const std::string& resource_tag,
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
        _instance_learner_map.clear();
        _instance_leader_count.clear();
        _instance_pk_prefix_leader_count.clear();
        _remove_region_peer_on_pk_prefix.clear();
        _incremental_region_info.clear();
        _region_learner_peer_state_map.clear();
    }

    void clear_region_peer_state_map() {
        _region_peer_state_map.clear();
    }

    void clear_region_learner_peer_state_map() {
        _region_learner_peer_state_map.clear();
    }

public:
    void set_max_region_id(int64_t max_region_id) {
        _max_region_id = max_region_id;
    }
    int64_t get_max_region_id() {
        return _max_region_id;
    }
    
    // include all peer and leaner
    void get_all_region_peers(int64_t region_id, std::vector<std::string>& peers) {
        SmartRegionInfo region_ptr = _region_info_map.get(region_id);
        if (region_ptr != nullptr) {
            for (auto& peer : region_ptr->peers()) {
                peers.emplace_back(peer);
            }
            for (auto& peer : region_ptr->learners()) {
                peers.emplace_back(peer);
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
                region_ids.emplace_back(region_id);
            }
        }
    }
    void get_learner_ids(const std::string& instance, std::vector<int64_t>& region_ids) {
        BAIDU_SCOPED_LOCK(_instance_learner_mutex);
        if (_instance_learner_map.find(instance) ==  _instance_learner_map.end()) {
            return;
        }
        for (auto& table_ids : _instance_learner_map[instance]) {
            for (auto& region_id : table_ids.second) {
                region_ids.emplace_back(region_id);
            }
        }
    }
    void get_region_ids(const std::string& instance, 
            std::unordered_map<int64_t, std::set<int64_t>>& table_region_ids) {
        BAIDU_SCOPED_LOCK(_instance_region_mutex);
        if (_instance_region_map.find(instance) ==  _instance_region_map.end()) {
            return;
        }
        table_region_ids = _instance_region_map[instance];
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

    void remove_error_peer(const int64_t region_id,
                                const std::set<std::string>& resource_tags,
                                std::set<std::string> peers,
                                std::vector<pb::PeerStateInfo>& recover_region_way);
    void remove_illegal_peer(const int64_t region_id,
                                const std::set<std::string>& resource_tags,
                                std::set<std::string> peers,
                                std::vector<pb::PeerStateInfo>& recover_region_way);
    void recovery_single_region_by_set_peer(const int64_t region_id,
                                const std::set<std::string>& resource_tags,
                                const pb::RecoverOpt recover_opt,
                                std::set<std::string> peers,
                                std::map<std::string, std::set<int64_t>>& not_alive_regions,
                                std::vector<pb::PeerStateInfo>& recover_region_way);
    void recovery_single_region_by_init_region(const std::set<int64_t> region_ids,
                    std::vector<Instance>& instances,
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
         {
            BAIDU_SCOPED_LOCK(_instance_learner_mutex);
            if (region_ptr != nullptr) {
                for (auto& learner : region_ptr->learners()) {
                    _instance_learner_map[learner][table_id].erase(region_id);
                }
            }
            for (auto& learner : region_info.learners()) {
                DB_DEBUG("set region learner instance %s table_id %ld region_id %ld", 
                    learner.c_str(), table_id, region_id);
                _instance_learner_map[learner][table_id].insert(region_id);
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
            _region_peer_state_map.erase(region_id);
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
   
    void set_instance_leader_count(const std::string& instance,
                                   const std::unordered_map<int64_t, int64_t>& table_leader_count,
                                   const std::unordered_map<std::string, int64_t>& pk_prefix_leader_count) {
        BAIDU_SCOPED_LOCK(_count_mutex);
        _instance_leader_count[instance] = table_leader_count;
        _instance_pk_prefix_leader_count[instance] = pk_prefix_leader_count;
    }
    int64_t get_pk_prefix_leader_count(const std::string& instance, const std::string& pk_prefix_key) {
        BAIDU_SCOPED_LOCK(_count_mutex);
        if (_instance_pk_prefix_leader_count.find(instance) == _instance_pk_prefix_leader_count.end()
                || _instance_pk_prefix_leader_count[instance].count(pk_prefix_key) == 0) {
            return 0;
        }
        return _instance_pk_prefix_leader_count[instance][pk_prefix_key];
    }
    void add_remove_peer_on_pk_prefix(const int64_t& region_id, const IdcInfo& idc) {
        BAIDU_SCOPED_LOCK(_count_mutex);
        _remove_region_peer_on_pk_prefix[region_id] = idc;
    }
    bool need_remove_peer_on_pk_prefix(const int64_t& region_id, IdcInfo& idc) {
        BAIDU_SCOPED_LOCK(_count_mutex);
        if (_remove_region_peer_on_pk_prefix.count(region_id) <= 0) {
            return false;
        }
        idc = _remove_region_peer_on_pk_prefix[region_id];
        return true;
    }
    void clear_remove_peer_on_pk_prefix(const int64_t region_id) {
        BAIDU_SCOPED_LOCK(_count_mutex);
        _remove_region_peer_on_pk_prefix.erase(region_id);
    }
    int64_t get_leader_count(const std::string& instance, int64_t table_id) {
        BAIDU_SCOPED_LOCK(_count_mutex);
        if (_instance_leader_count.find(instance) == _instance_leader_count.end()
                || _instance_leader_count[instance].find(table_id) == _instance_leader_count[instance].end()) {
            return 0;
        }
        return _instance_leader_count[instance][table_id];
    }
    int64_t get_leader_count(const std::string& instance) {
        BAIDU_SCOPED_LOCK(_count_mutex);
        if (_instance_leader_count.find(instance) == _instance_leader_count.end()) {
            return 0;
        }
        int64_t cnt = 0;
        for (auto& pair :  _instance_leader_count[instance]) {
            cnt += pair.second;
        }
        return cnt;
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

    bool can_modify_learner(int64_t region_id) {
        bool can_modify_learner = false;
        bool exist = _region_learner_peer_state_map.init_if_not_exist_else_update(
            region_id, false, [&can_modify_learner](RegionLearnerState& state) {
            DB_DEBUG("check can add learner time %ld", state.tc.get_time());
            can_modify_learner = state.tc.get_time() > FLAGS_modify_learner_peer_interval_us;
            if (can_modify_learner) {
                state.tc.reset();
            }
        });

        return !exist || can_modify_learner;
    }

    bool get_learner_health_status(const std::string& learner, int64_t region_id) {
        bool status = true;
        _region_learner_peer_state_map.update(region_id, [&status, &learner](RegionLearnerState& rls) {
            auto learn_iter = rls.learner_state_map.find(learner);
            if (learn_iter != rls.learner_state_map.end()) {
                status = (learn_iter->second.peer_status() == pb::STATUS_NORMAL);
            }
        });
        return status;
    }
    void put_incremental_regioninfo(const int64_t apply_index, std::vector<pb::RegionInfo>& region_infos);
    bool check_and_update_incremental(
            const pb::BaikalHeartBeatRequest* request, pb::BaikalHeartBeatResponse* response, 
            int64_t applied_index, const std::unordered_set<int64_t>& heartbeat_table_ids); 

    bool check_table_in_resource_tags(int64_t table_id, const std::set<std::string>& resource_tags);
    
private:
    RegionManager(): _max_region_id(0) {
        _doing_recovery = false;
        _last_opt_times = butil::gettimeofday_us();
        bthread_mutex_init(&_instance_region_mutex, NULL);
        bthread_mutex_init(&_instance_learner_mutex, NULL);
        bthread_mutex_init(&_count_mutex, NULL);
        bthread_mutex_init(&_doing_mutex, NULL);
    }
private:
    int64_t                                             _max_region_id;
    int64_t                                             _last_opt_times;
    std::atomic<bool>                                   _doing_recovery;
    //region_id 与table_id的映射关系, key:region_id, value:table_id
    ThreadSafeMap<int64_t, SmartRegionInfo>             _region_info_map;
    
    bthread_mutex_t                                     _instance_region_mutex;
    bthread_mutex_t                                     _instance_learner_mutex;
    //实例和region_id的映射关系，在需要主动发送迁移实例请求时需要
    std::unordered_map<std::string, std::unordered_map<int64_t, std::set<int64_t>>>  _instance_region_map;
    std::unordered_map<std::string, std::unordered_map<int64_t, std::set<int64_t>>>  _instance_learner_map;

    ThreadSafeMap<int64_t, RegionStateInfo>        _region_state_map;
    ThreadSafeMap<int64_t, RegionPeerState>        _region_peer_state_map;
    ThreadSafeMap<int64_t, RegionLearnerState>        _region_learner_peer_state_map;
    //该信息只在meta_server的leader中内存保存, 该map可以单用一个锁
    bthread_mutex_t                                     _count_mutex;
    std::unordered_map<std::string, std::unordered_map<int64_t, int64_t>> _instance_leader_count;
    // instance_tableID -> pk_prefix -> leader region count
    std::unordered_map<std::string, std::unordered_map<std::string, int64_t>> _instance_pk_prefix_leader_count;
    // region_id -> logical_room，处理store心跳发现大户不均，标记需要迁移的region_id及其候选store需要在的logical room
    // check_peer_count发现region_id在map里，直接按照大户的维度删除peer数最多的candidate，否则按照table维度删除peer
    std::unordered_map<int64_t, IdcInfo>            _remove_region_peer_on_pk_prefix;

    bthread_mutex_t                                     _doing_mutex;
    std::set<std::string>                               _doing_migrate; 
    IncrementalUpdate<std::vector<pb::RegionInfo>> _incremental_region_info;
}; //class

}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
