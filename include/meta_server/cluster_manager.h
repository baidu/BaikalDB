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
#include <bthread/mutex.h>
#include "proto/meta.interface.pb.h"
#include "meta_server.h"
#include "meta_state_machine.h"

namespace baikaldb {
DECLARE_string(default_logical_room);
DECLARE_string(default_physical_room);
struct InstanceStateInfo {
    int64_t timestamp; //上次收到该实例心跳的时间戳
    pb::Status state; //实例状态
};

struct Instance {
    std::string address;
    int64_t capacity;
    int64_t used_size;
    //std::vector<int64_t> regions;
    std::string resource_tag;
    std::string physical_room;
    std::string logical_room;
    InstanceStateInfo instance_status;
    Instance() {
       instance_status.state = pb::NORMAL;
       instance_status.timestamp = butil::gettimeofday_us();
    }
    Instance(const pb::InstanceInfo& instance_info) : 
        address(instance_info.address()),
        capacity(instance_info.capacity()),
        //若请求中没有该字段，为了安全起见
        used_size(instance_info.capacity()),
        resource_tag(instance_info.resource_tag()),
        physical_room(instance_info.physical_room()),
        logical_room(instance_info.logical_room()) {
        if (instance_info.has_used_size()) {
            used_size = instance_info.used_size();
        }
        if (instance_info.has_status() && instance_info.status() == pb::FAULTY) {
            instance_status.state = pb::FAULTY;
        } else {
            instance_status.state = pb::NORMAL;
        }
        instance_status.timestamp = butil::gettimeofday_us();
    }
};

class ClusterManager {
public:
    ~ClusterManager() {
        bthread_mutex_destroy(&_physical_mutex);
        bthread_mutex_destroy(&_instance_mutex);
    }
    static ClusterManager* get_instance() {
        static ClusterManager instance;
        return &instance;
    }
    friend class QueryClusterManager;
    void process_cluster_info(google::protobuf::RpcController* controller,
                              const pb::MetaManagerRequest* request, 
                              pb::MetaManagerResponse* response,
                              google::protobuf::Closure* done);
    void add_logical(const pb::MetaManagerRequest& request, braft::Closure* done);
    void drop_logical(const pb::MetaManagerRequest& request, braft::Closure* done);

    void add_physical(const pb::MetaManagerRequest& request, braft::Closure* done); 
    void drop_physical(const pb::MetaManagerRequest& request, braft::Closure* done);
   
    void add_instance(const pb::MetaManagerRequest& request, braft::Closure* done); 
    void drop_instance(const pb::MetaManagerRequest& request, braft::Closure* done); 
    void update_instance(const pb::MetaManagerRequest& request, braft::Closure* done);

    void move_physical(const pb::MetaManagerRequest& request, braft::Closure* done); 
    
    void set_instance_migrate(const pb::MetaManagerRequest* request,
                             pb::MetaManagerResponse* response,
                             uint64_t log_id); 
    void set_instance_full(const pb::MetaManagerRequest* request,
                             pb::MetaManagerResponse* response,
                             uint64_t log_id); 
    void set_instance_no_full(const pb::MetaManagerRequest* request,
                             pb::MetaManagerResponse* response,
                             uint64_t log_id); 
    void process_baikal_heartbeat(const pb::BaikalHeartBeatRequest* request,
            pb::BaikalHeartBeatResponse* response); 
    void process_instance_heartbeat_for_store(const pb::InstanceInfo& request);
    void process_peer_heartbeat_for_store(const pb::StoreHeartBeatRequest* request, 
                pb::StoreHeartBeatResponse* response);
    void store_healthy_check_function();
    //从集群中选择可用的实例
    //排除状态不为normal, 如果输入有resource_tag会优先选择resource_tag
    //排除exclued
    int select_instance_rolling(const std::string& resource_tag, 
                        const std::set<std::string>& exclude_stores,
                        const std::string& logical_room, 
                        std::string& selected_instance);
    int select_instance_min(const std::string& resource_tag,
                            const std::set<std::string>& exclude_stores,
                            int64_t table_id,
                            const std::string& logical_room,
                            std::string& selected_instance,
                            int64_t average_count = 0);
    int load_snapshot();
    bool logical_room_exist(const std::string& logical_room) {
        BAIDU_SCOPED_LOCK(_physical_mutex);
        if (_logical_physical_map.find(logical_room) != _logical_physical_map.end()
                && _logical_physical_map[logical_room].size() != 0) {
            return true;
        }
        return false;
    }
public:
    void get_instances(const std::string& resource_tag, 
                        std::set<std::string>& instances) {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        for (auto& instance_info : _instance_info) {
            if (instance_info.second.resource_tag == resource_tag) {
                instances.insert(instance_info.second.address);
            }
        }
    }
    int64_t get_instance_count(const std::string& resource_tag, const std::string& logical_room) {
        int64_t count = 0; 
        BAIDU_SCOPED_LOCK(_instance_mutex);
        for (auto& instance_info : _instance_info) {
            if (instance_info.second.resource_tag == resource_tag
                    && instance_info.second.logical_room == logical_room
                    && (instance_info.second.instance_status.state == pb::NORMAL
                    || instance_info.second.instance_status.state == pb::FAULTY)) {
                ++count;
            }
        }
        return count;
    }
    
    int64_t get_instance_count(const std::string& resource_tag) {
        int64_t count = 0; 
        BAIDU_SCOPED_LOCK(_instance_mutex);
        for (auto& instance_info : _instance_info) {
            if (instance_info.second.resource_tag == resource_tag 
                && (instance_info.second.instance_status.state == pb::NORMAL
                || instance_info.second.instance_status.state == pb::FAULTY)) {
                ++count;
            }
        }
        return count;
    }

    bool check_resource_tag_exist(const std::string& resource_tag) {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        for (auto& instance_info : _instance_info) {
            if (instance_info.second.resource_tag == resource_tag) {
                return true;
            }
        }
        return false;
    }

    int64_t get_peer_count(int64_t table_id, const std::string& logical_room) {
        int64_t count = 0;
        BAIDU_SCOPED_LOCK(_instance_mutex);
        for (auto& region_count: _instance_regions_count_map) {
            std::string instance = region_count.first;
            if (_instance_info.find(instance) != _instance_info.end()
                    && _instance_info[instance].logical_room != logical_room) {
                continue;
            }
            if (region_count.second.find(table_id) != region_count.second.end()) {
                count += region_count.second[table_id];
            }
        }
        return count;
    }

    int64_t get_peer_count(int64_t table_id) {
        int64_t count = 0;
        BAIDU_SCOPED_LOCK(_instance_mutex);
        for (auto& region_count: _instance_regions_count_map) {
            if (region_count.second.find(table_id) != region_count.second.end()) {
                count += region_count.second[table_id];
            }
        }
        return count;
    }
    int64_t get_peer_count(const std::string& instance, int64_t table_id) {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        if (_instance_regions_count_map.find(instance) == _instance_regions_count_map.end()
                || _instance_regions_count_map[instance].find(table_id) == _instance_regions_count_map[instance].end()) {
            return 0;
        }
        return _instance_regions_count_map[instance][table_id];
    }

    void sub_peer_count(const std::string& instance, int64_t table_id) {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        if (_instance_regions_count_map.find(instance) == _instance_regions_count_map.end()
                || _instance_regions_count_map[instance].find(table_id) == _instance_regions_count_map[instance].end()) {
            return ;
        }
        _instance_regions_count_map[instance][table_id]--;
    }
    //切主时主动调用，恢复状态为正常
    void reset_instance_status() {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        for (auto& instance_pair : _instance_info) {
            instance_pair.second.instance_status.state = pb::NORMAL;
            instance_pair.second.instance_status.timestamp = butil::gettimeofday_us();
            _instance_regions_map[instance_pair.first] = 
                    std::unordered_map<int64_t, std::vector<int64_t>>{};
            _instance_regions_count_map[instance_pair.first] = 
                    std::unordered_map<int64_t, int64_t>{};
        }
    }
    pb::Status get_instance_status(std::string instance) {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        if (_instance_info.find(instance) == _instance_info.end()) {
            return pb::NORMAL;
        }
        return _instance_info[instance].instance_status.state;
    }
    Instance get_instance(std::string instance) {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        if (_instance_info.find(instance) == _instance_info.end()) {
            return Instance();
        }
        return _instance_info[instance];
    }
    void get_instance_by_resource_tags(std::map<std::string, std::vector<std::string>>& instances) {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        for (auto& iter : _instance_info) {
            instances[iter.second.resource_tag].push_back(iter.first);
        }
    }
    std::string get_resource_tag(const std::string& instance) {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        if (_instance_info.find(instance) != _instance_info.end()) {
            return _instance_info[instance].resource_tag;
        }
        DB_WARNING("instance: %s not exist", instance.c_str());
        return "";
    }
    void get_resource_tag(const std::set<std::string>& related_peers,
                std::unordered_map<std::string, std::string>& peer_resource_tags) {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        for (auto& peer : related_peers) {
            if (_instance_info.find(peer) != _instance_info.end()) {
                peer_resource_tags[peer] = _instance_info[peer].resource_tag;
            } else {
                DB_WARNING("instance: %s not exist", peer.c_str());
            }
        }
    }
    bool instance_exist(std::string instance) {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        if (_instance_info.find(instance) == _instance_info.end()) {
            return false;
        }
        return true;
    } 
    void set_instance_regions(const std::string& instance, 
                    const std::unordered_map<int64_t, std::vector<int64_t>>& instance_regions,
                    const std::unordered_map<int64_t, int64_t>& instance_regions_count) {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        _instance_regions_map[instance] = instance_regions;
        _instance_regions_count_map[instance] = instance_regions_count;
    }
    
    int update_instance_info(const pb::InstanceInfo& instance_info) {
        std::string instance = instance_info.address();
        BAIDU_SCOPED_LOCK(_instance_mutex);
        if (_instance_info.find(instance) == _instance_info.end()) {
            return -1;
        }
        _instance_info[instance].capacity = instance_info.capacity();
        _instance_info[instance].used_size = instance_info.used_size();
        _instance_info[instance].resource_tag = instance_info.resource_tag();
        _instance_info[instance].instance_status.timestamp = butil::gettimeofday_us();
        if (_instance_info[instance].instance_status.state != pb::MIGRATE) {
            if (_instance_info[instance].instance_status.state != pb::NORMAL) {
                DB_WARNING("instance:%s status return NORMAL, resource_tag: %s",
                    instance.c_str(), _instance_info[instance].resource_tag.c_str());
                _instance_info[instance].instance_status.state = pb::NORMAL;
            }
        }
        return 0;
    }
    
    int set_migrate_for_instance(const std::string& instance) {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        if (_instance_info.find(instance) == _instance_info.end()) {
            return -1;
        }
        _instance_info[instance].instance_status.state = pb::MIGRATE;
        return 0;
    }
   
    int set_full_for_instance(const std::string& instance) {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        if (_instance_info.find(instance) == _instance_info.end()) {
            return -1;
        }
        _instance_info[instance].instance_status.state = pb::FULL;
        return 0;
    }

    int set_no_full_for_instance(const std::string& instance) {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        if (_instance_info.find(instance) == _instance_info.end()) {
            return -1;
        }
        if (_instance_info[instance].instance_status.state == pb::FULL) {
            _instance_info[instance].instance_status.state = pb::NORMAL;
        }
        return 0;
    }    
    void set_meta_state_machine(MetaStateMachine* meta_state_machine) {
        _meta_state_machine = meta_state_machine;
    }
    std::string get_logical_room(const std::string& instance) {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        if (_instance_info.find(instance) != _instance_info.end()) {
            return _instance_info[instance].logical_room;
        }
        DB_WARNING("instance: %s not exist", instance.c_str());
        return "";
    }
    static std::string get_ip(const std::string& instance) {
        std::string ip = "";
        std::string::size_type position = instance.find_first_of(":");
        if (position != instance.npos) {
            return instance.substr(0, position);
        }
        DB_FATAL("find instance: %s ip error", instance.c_str());
        return "";
    }
private:
    ClusterManager() {
        bthread_mutex_init(&_physical_mutex, NULL);
        bthread_mutex_init(&_instance_mutex, NULL);
        {
            BAIDU_SCOPED_LOCK(_physical_mutex);
            _physical_info[FLAGS_default_physical_room] = 
                FLAGS_default_logical_room;
            _logical_physical_map[FLAGS_default_logical_room] = 
                    std::set<std::string>{FLAGS_default_physical_room};
        }
        {
            BAIDU_SCOPED_LOCK(_instance_mutex);
            _physical_instance_map[FLAGS_default_logical_room] = std::set<std::string>();
        }
    }
    bool whether_legal_for_select_instance(
                const std::string& candicate_instance,
                const std::string& resource_tag,
                const std::set<std::string>& exclude_stores,
                const std::string& logical_room);
    std::string construct_logical_key() {
        return MetaServer::CLUSTER_IDENTIFY
                + MetaServer::LOGICAL_CLUSTER_IDENTIFY
                + MetaServer::LOGICAL_KEY;
    }
    std::string construct_physical_key(const std::string& logical_key) {
        return MetaServer::CLUSTER_IDENTIFY
                + MetaServer::PHYSICAL_CLUSTER_IDENTIFY
                + logical_key;
    }
    std::string construct_instance_key(const std::string& instance) {
        return MetaServer::CLUSTER_IDENTIFY
                + MetaServer::INSTANCE_CLUSTER_IDENTIFY
                + instance;
    }
    int load_instance_snapshot(const std::string& instance_prefix,
                                 const std::string& key, 
                                 const std::string& value);
    int load_physical_snapshot(const std::string& physical_prefix,
                                 const std::string& key, 
                                 const std::string& value);
    int load_logical_snapshot(const std::string& logical_prefix,
                                const std::string& key, 
                                const std::string& value);
private:
    bthread_mutex_t                                             _physical_mutex;
    //物理机房与逻辑机房对应关系 , key:物理机房， value:逻辑机房
    std::unordered_map<std::string, std::string>                _physical_info;
    //物理机房与逻辑机房对应关系 , key:逻辑机房， value:物理机房组合
    std::unordered_map<std::string, std::set<std::string>>      _logical_physical_map;
    
    bthread_mutex_t                                             _instance_mutex;
    //物理机房与实例对应关系, key:实例， value:物理机房
    std::unordered_map<std::string, std::string>                _instance_physical_map;
    //物理机房与实例对应关系, key:物理机房， value:实例
    std::unordered_map<std::string, std::set<std::string>>      _physical_instance_map;
    //实例信息
    std::unordered_map<std::string, Instance>                   _instance_info;
    std::string                                                 _last_rolling_instance;

    //下边信息只在leader中保存，切换leader之后需要一段时间来收集数据，会出现暂时的数据不准情况
    typedef std::unordered_map<int64_t, std::vector<int64_t>>      TableRegionMap;
    typedef std::unordered_map<int64_t, int64_t>                TableRegionCountMap;
    //每个实例上，保存的每个表的哪些reigon
    std::unordered_map<std::string, TableRegionMap>             _instance_regions_map; 
    //每个实例上。保存每个表的region的个数
    std::unordered_map<std::string, TableRegionCountMap>        _instance_regions_count_map;

    MetaStateMachine*                                           _meta_state_machine = NULL;
}; //class ClusterManager

}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
