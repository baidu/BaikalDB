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
#include <boost/algorithm/string.hpp>
#include <bitset>
#include "proto/meta.interface.pb.h"
#include "meta_server.h"
#include "meta_util.h"
#include "meta_state_machine.h"

namespace baikaldb {
DECLARE_string(default_logical_room);
DECLARE_string(default_physical_room);
DECLARE_bool(need_check_slow);
struct InstanceStateInfo {
    int64_t timestamp; //上次收到该实例心跳的时间戳
    pb::Status state; //实例状态
    TimeCost state_duration; //目前状态持续时间，主要给MIGRATE状态用
};

struct InstanceSchedulingInfo {
    //每个实例上，保存的每个表的哪些region
    std::unordered_map<int64_t, std::vector<int64_t>>       regions_map;
    //每个实例上。保存每个表的region的个数
    std::unordered_map<int64_t, int64_t>                    regions_count_map;
    // tableID_pk_prefix -> leader_count
    std::unordered_map<std::string, int64_t>                pk_prefix_region_count;
    // raft, 机房信息
    IdcInfo                                                 idc;
};
using DoubleBufferedSchedulingInfo = butil::DoublyBufferedData<std::unordered_map<std::string, InstanceSchedulingInfo>>;


struct Instance {
    std::string address;
    int64_t capacity;
    int64_t used_size;
    std::string resource_tag;
    std::string physical_room;
    std::string logical_room;
    std::string version;
    std::string network_segment;
    // store自定义的网段，如果为"", 则meta会自适应进行网段划分, 否则其网段保持与store自定义的一样
    std::string network_segment_self_defined;
    int64_t dml_latency = 0;
    int64_t dml_qps = 0;
    int64_t raft_total_latency = 0;
    int64_t raft_total_qps = 0;
    int64_t select_latency = 0;
    int64_t select_qps = 0;
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
        logical_room(instance_info.logical_room()),
        version(instance_info.version()) {
        if (instance_info.has_used_size()) {
            used_size = instance_info.used_size();
        }
        if (instance_info.has_status() && instance_info.status() == pb::FAULTY) {
            instance_status.state = pb::FAULTY;
        } else {
            instance_status.state = pb::NORMAL;
        }
        instance_status.timestamp = butil::gettimeofday_us();
        if (instance_info.has_network_segment() && !instance_info.network_segment().empty()) {
            // if store's network_segment is set by gflag:
            network_segment_self_defined = instance_info.network_segment();
            network_segment = instance_info.network_segment();
        } 
    }
};

class ClusterManager {
public:
    ~ClusterManager() {
        bthread_mutex_destroy(&_physical_mutex);
        bthread_mutex_destroy(&_instance_mutex);
        bthread_mutex_destroy(&_instance_param_mutex);
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

    void update_instance_param(const pb::MetaManagerRequest& request, braft::Closure* done);

    void move_physical(const pb::MetaManagerRequest& request, braft::Closure* done); 
    
    void set_instance_migrate(const pb::MetaManagerRequest* request,
                             pb::MetaManagerResponse* response,
                             uint64_t log_id); 
    void set_instance_status(const pb::MetaManagerRequest* request,
                             pb::MetaManagerResponse* response,
                             uint64_t log_id); 
    void process_baikal_heartbeat(const pb::BaikalHeartBeatRequest* request,
            pb::BaikalHeartBeatResponse* response); 
    void process_instance_heartbeat_for_store(const pb::InstanceInfo& request);
    void process_instance_param_heartbeat_for_store(const pb::StoreHeartBeatRequest* request, 
                pb::StoreHeartBeatResponse* response);
    void process_instance_param_heartbeat_for_baikal(const pb::BaikalOtherHeartBeatRequest* request,
                pb::BaikalOtherHeartBeatResponse* response);
    void process_peer_heartbeat_for_store(const pb::StoreHeartBeatRequest* request, 
                pb::StoreHeartBeatResponse* response);
    void process_pk_prefix_load_balance(std::unordered_map<std::string, int64_t>& pk_prefix_region_counts,
            std::unordered_map<int64_t, IdcInfo>& table_balance_idc,
            std::unordered_map<std::string, int64_t>& idc_instance_count,
            std::unordered_map<int64_t, int64_t>& table_add_peer_counts,
            std::unordered_map<std::string, int64_t>& pk_prefix_add_peer_counts,
            std::unordered_map<std::string, int64_t>& pk_prefix_average_counts);
    void get_switch(const pb::QueryRequest* request, pb::QueryResponse* response);
    void store_healthy_check_function();
    // just for 单测使用
    void get_network_segment_count(const std::string& resource_tag, size_t & count, size_t& prefix) {
        count = _resource_tag_instances_by_network[resource_tag].size();
        prefix = _resource_tag_network_prefix[resource_tag];
    }
    //从集群中选择可用的实例
    //排除状态不为normal, 如果输入有resource_tag会优先选择resource_tag
    //排除exclude
    int select_instance_rolling(const IdcInfo& idc,
                        const std::set<std::string>& exclude_stores,
                        std::string& selected_instance);
    int select_instance_min(const IdcInfo& idc,
                            const std::set<std::string>& exclude_stores,
                            const int64_t& table_id,
                            std::string& selected_instance,
                            const int64_t& average_count = 0);
    int select_instance_min_on_pk_prefix(const IdcInfo& idc_str,
                                      const std::set<std::string>& exclude_stores,
                                      const int64_t& table_id,
                                      const std::string& pk_prefix_key,
                                      std::string& selected_instance,
                                      const int64_t& pk_prefix_average_count,
                                      const int64_t& table_average_count,
                                      bool need_both_below_average = false);
    void auto_network_segments_division(std::string resource_tag);
    int load_snapshot();
    bool logical_and_physical_room_valid(const std::string& logical_room, const std::string& physical_room) {
        BAIDU_SCOPED_LOCK(_physical_mutex);
        auto logical_iter = _logical_physical_map.find(logical_room);
        if (logical_iter == _logical_physical_map.end()) {
            return false;
        }
        if (!physical_room.empty() 
            && logical_iter->second.find(physical_room) == logical_iter->second.end()) {
            return false;
        }
        return true;
    }
public:
    void get_instances(const std::string& resource_tag, 
                        std::set<std::string>& instances) {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        if (_resource_tag_instance_map.count(resource_tag) == 1) {
            instances = _resource_tag_instance_map[resource_tag];
        }
    }

    int64_t get_instance_count(const std::string& resource_tag, const std::string& logical_room) {
        int64_t count = 0; 
        BAIDU_SCOPED_LOCK(_instance_mutex);
        if (_resource_tag_instance_map.count(resource_tag) == 0) {
            return count;
        }
        for (auto& address : _resource_tag_instance_map[resource_tag]) {
            if (_instance_info.count(address) == 0) {
                continue;
            }
            if (_instance_info[address].resource_tag == resource_tag &&
                    _instance_info[address].logical_room == logical_room &&
                    (_instance_info[address].instance_status.state == pb::NORMAL || 
                     _instance_info[address].instance_status.state == pb::FAULTY)) {
                ++count;
            }
        }
        return count;
    }

    // 获取同集群、同逻辑机房、同物理机房的store实例数
    void get_instance_count_for_all_level(const IdcInfo& idc, 
                                          std::unordered_map<std::string, int64_t>& idc_instance_count) {
        idc_instance_count.clear();
        int instances_in_resource_tag = 0;
        int instances_in_logical_room = 0;
        int instances_in_physical_room = 0;
        BAIDU_SCOPED_LOCK(_instance_mutex);
        if (_resource_tag_instance_map.count(idc.resource_tag) == 0) {
            return;
        }
        for (auto& address : _resource_tag_instance_map[idc.resource_tag]) {
            if (_instance_info.count(address) == 0 
                    || _instance_info[address].resource_tag != idc.resource_tag) {
                continue;
            }
            if (_instance_info[address].instance_status.state != pb::NORMAL 
                    && _instance_info[address].instance_status.state != pb::FAULTY) {
                continue;
            }
            instances_in_resource_tag++;
            if (_instance_info[address].logical_room == idc.logical_room) {
                instances_in_logical_room++;
                if (_instance_info[address].physical_room == idc.physical_room) {
                    instances_in_physical_room++;
                }
            }
        }
        idc_instance_count[idc.resource_tag_level()] = instances_in_resource_tag;
        idc_instance_count[idc.logical_room_level()] = instances_in_logical_room;
        idc_instance_count[idc.to_string()] = instances_in_physical_room;
        return;
    }

    int64_t get_instance_count(const std::string& resource_tag, 
            std::map<std::string, int64_t>* room_count = nullptr) {
        int64_t count = 0; 
        BAIDU_SCOPED_LOCK(_instance_mutex);
        if (_resource_tag_instance_map.count(resource_tag) == 0) {
            return count;
        }
        for (auto& address : _resource_tag_instance_map[resource_tag]) {
            if (_instance_info.count(address) == 0) {
                continue;
            }
            if (_instance_info[address].resource_tag == resource_tag &&
                    (_instance_info[address].instance_status.state == pb::NORMAL || 
                     _instance_info[address].instance_status.state == pb::FAULTY)) {
                ++count;
                if (room_count != nullptr) {
                    (*room_count)[_instance_info[address].logical_room]++;
                }
            }
        }
        return count;
    }

    bool check_resource_tag_exist(const std::string& resource_tag) {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        return _resource_tag_instance_map.count(resource_tag) == 1 &&
            !_resource_tag_instance_map[resource_tag].empty();
    }

    template<typename RepeatedType>
    void get_resource_tag_count(const RepeatedType& instances, const std::string& resource_tag, 
        std::set<std::string>& current_instances) {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        for (auto& instance : instances) {
            if (_instance_info.find(instance) != _instance_info.end()
                    && _instance_info[instance].resource_tag == resource_tag) {
                current_instances.insert(instance);
            }
        }
        return;
    }

    int64_t get_instance_pk_prefix_peer_count(const std::string& instance, const std::string& pk_prefix) {
        DoubleBufferedSchedulingInfo::ScopedPtr schedule_info_ptr;
        if (_scheduling_info.Read(&schedule_info_ptr) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return 0;
        }
        auto instance_iter = schedule_info_ptr->find(instance);
        if (instance_iter == schedule_info_ptr->end()) {
            return 0;
        }
        const InstanceSchedulingInfo& scheduling_info = instance_iter->second;
        auto pk_iter = scheduling_info.pk_prefix_region_count.find(pk_prefix);
        if (pk_iter == scheduling_info.pk_prefix_region_count.end()) {
            return 0;
        }
        return pk_iter->second;
    }

    // 获取idc维度下pk_prefix对应的peer总数
    int64_t get_pk_prefix_peer_count(const std::string& pk_prefix_key, const IdcInfo& idc) {
        int64_t count = 0;
        DoubleBufferedSchedulingInfo::ScopedPtr schedule_info_ptr;
        if (_scheduling_info.Read(&schedule_info_ptr) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return 0;
        }
        for (const auto& instance_schedule_info: *schedule_info_ptr) {
            if (!instance_schedule_info.second.idc.match(idc)) {
                continue;
            }
            const InstanceSchedulingInfo& scheduling_info = instance_schedule_info.second;
            auto pk_iter = scheduling_info.pk_prefix_region_count.find(pk_prefix_key);
            if (pk_iter != scheduling_info.pk_prefix_region_count.end()) {
                count += pk_iter->second;
            }
        }
        return count;
    }
    
    // 获取idc维度下对应的总peer总数
    int64_t get_peer_count(int64_t table_id, const IdcInfo& idc) {
        int64_t count = 0;
        DoubleBufferedSchedulingInfo::ScopedPtr schedule_info_ptr;
        if (_scheduling_info.Read(&schedule_info_ptr) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return 0;
        }
        for (const auto &instance_schedule_info: *schedule_info_ptr) {
            const InstanceSchedulingInfo &scheduling_info = instance_schedule_info.second;
            if (!idc.match(scheduling_info.idc)) {
                continue;
            }
            auto region_iter = scheduling_info.regions_count_map.find(table_id);
            if (region_iter != scheduling_info.regions_count_map.end()) {
                count += region_iter->second;
            }
        }
        return count;
    }

    int64_t get_peer_count(int64_t table_id) {
        int64_t count = 0;
        DoubleBufferedSchedulingInfo::ScopedPtr schedule_info_ptr;
        if (_scheduling_info.Read(&schedule_info_ptr) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return 0;
        }
        for (const auto &instance_schedule_info: *schedule_info_ptr) {
            const InstanceSchedulingInfo &scheduling_info = instance_schedule_info.second;
            auto region_iter = scheduling_info.regions_count_map.find(table_id);
            if (region_iter != scheduling_info.regions_count_map.end()) {
                count += region_iter->second;
            }
        }
        return count;
    }

    int64_t get_peer_count(const std::string& instance, int64_t table_id) {
        DoubleBufferedSchedulingInfo::ScopedPtr schedule_info_ptr;
        if (_scheduling_info.Read(&schedule_info_ptr) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return 0;
        }
        auto instance_schedule_info = schedule_info_ptr->find(instance);
        if (instance_schedule_info == schedule_info_ptr->end()) {
            return 0;
        }
        const InstanceSchedulingInfo& scheduling_info = instance_schedule_info->second;
        auto count_iter = scheduling_info.regions_count_map.find(table_id);
        if (count_iter == scheduling_info.regions_count_map.end()) {
            return 0;
        }
        return count_iter->second;
    }

    void sub_peer_count(const std::string& instance, int64_t table_id) {
        auto call_func = [instance, table_id](
                std::unordered_map<std::string, InstanceSchedulingInfo>& scheduling_info) -> int {
            scheduling_info[instance].regions_count_map[table_id]--;
            return 1;
        };
        _scheduling_info.Modify(call_func);
    }

    void sub_peer_count_on_pk_prefix(const std::string& instance, int64_t table_id, const std::string& pk_prefix) {
        auto call_func = [instance, table_id, pk_prefix](
                std::unordered_map<std::string, InstanceSchedulingInfo>& scheduling_info) -> int {
            scheduling_info[instance].regions_count_map[table_id]--;
            scheduling_info[instance].pk_prefix_region_count[pk_prefix]--;
            return 1;
        };
        _scheduling_info.Modify(call_func);
    }

    void add_peer_count(const std::string& instance, int64_t table_id) {
        auto call_func = [instance, table_id](
                std::unordered_map<std::string, InstanceSchedulingInfo>& scheduling_info) -> int {
            scheduling_info[instance].regions_count_map[table_id]++;
            return 1;
        };
        _scheduling_info.Modify(call_func);
    }

    void add_peer_count_on_pk_prefix(const std::string& instance, int64_t table_id, const std::string& pk_prefix) {
        auto call_func = [instance, table_id, pk_prefix](
                std::unordered_map<std::string, InstanceSchedulingInfo>& scheduling_info) -> int {
            scheduling_info[instance].regions_count_map[table_id]++;
            scheduling_info[instance].pk_prefix_region_count[pk_prefix]++;
            return 1;
        };
        _scheduling_info.Modify(call_func);
    }

    //切主时主动调用，恢复状态为正常
    void reset_instance_status() {
        auto call_func = [](std::unordered_map<std::string, InstanceSchedulingInfo>& scheduling_info) -> int {
            for (auto& instance_info : scheduling_info) {
                instance_info.second.pk_prefix_region_count = std::unordered_map<std::string, int64_t >{};
                instance_info.second.regions_count_map = std::unordered_map<int64_t, int64_t>{};
                instance_info.second.regions_map = std::unordered_map<int64_t, std::vector<int64_t>>{};
            }
            return 1;
        };
        _scheduling_info.Modify(call_func);

        BAIDU_SCOPED_LOCK(_instance_mutex);
        for (auto& instance_pair : _instance_info) {
            instance_pair.second.instance_status.state = pb::NORMAL;
            instance_pair.second.instance_status.timestamp = butil::gettimeofday_us();
            instance_pair.second.instance_status.state_duration.reset();
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

    void get_instance_by_resource_tags(std::map<std::string, std::vector<Instance>>& instances) {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        for (auto& iter : _instance_info) {
            instances[iter.second.resource_tag].emplace_back(iter.second);
        }
    }

    bool get_resource_tag(const std::string& instance, std::string& resource_tag) {
        DoubleBufferedSchedulingInfo::ScopedPtr info_iter;
        if (_scheduling_info.Read(&info_iter) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return false;
        }
        auto iter = info_iter->find(instance);
        if (iter != info_iter->end()) {
            resource_tag = iter->second.idc.resource_tag;
            return true;
        }
        return false;
    }

    // 获取一个region所有peer的机房信息, instance->idcInfo
    int get_instances_idc_info(const ::google::protobuf::RepeatedPtrField< ::std::string>& peers, 
                                std::unordered_map<std::string, IdcInfo>& peer_dist_info) {
        int ret = 0;
        DoubleBufferedSchedulingInfo::ScopedPtr info_iter;
        if (_scheduling_info.Read(&info_iter) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return -2;
        }
        for (const auto& peer : peers) {
            auto iter = info_iter->find(peer);
            if (iter == info_iter->end()) {
                peer_dist_info[peer] = IdcInfo();
                ret = -1;
            } else {
                peer_dist_info[peer] = iter->second.idc;
            }
        }
        return ret;
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
                              const std::unordered_map<int64_t, int64_t>& instance_regions_count,
                              const std::unordered_map<std::string, int64_t>& pk_prefix_region_counts) {
        auto call_func = [&instance_regions, &instance_regions_count, &pk_prefix_region_counts](
                            std::unordered_map<std::string, InstanceSchedulingInfo>& scheduling_info,
                            const std::string& instance) -> int {
            scheduling_info[instance].regions_map = instance_regions;
            scheduling_info[instance].regions_count_map = instance_regions_count;
            scheduling_info[instance].pk_prefix_region_count = pk_prefix_region_counts;
            return 1;
        };
        _scheduling_info.Modify(call_func, instance);
    }
    
    // return -1: add instance -2: update instance
    int update_instance_info(const pb::InstanceInfo& instance_info);
    
    int set_migrate_for_instance(const std::string& instance) {
        return set_status_for_instance(instance, pb::MIGRATE);
    }
   
    int set_status_for_instance(const std::string& instance, const pb::Status& status) {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        if (_instance_info.find(instance) == _instance_info.end()) {
            return -1;
        }
        if (_instance_info[instance].instance_status.state != status) {
            _instance_info[instance].instance_status.state = status;
            _instance_info[instance].instance_status.state_duration.reset();
        }
        return 0;
    }

    void set_meta_state_machine(MetaStateMachine* meta_state_machine) {
        _meta_state_machine = meta_state_machine;
    }

    int get_instance_idc(const std::string& instance, IdcInfo& idc) {
        DoubleBufferedSchedulingInfo::ScopedPtr info_iter;
        if (_scheduling_info.Read(&info_iter) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return -1;
        }
        auto iter = info_iter->find(instance);
        if (iter == info_iter->end()) {
            return -1;
        }
        idc = iter->second.idc;
        return 0;
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

    static std::string get_ip_bit_set(const std::string& address, int prefix) {
        std::string ip, ip_set;
        std::string::size_type position = address.find_first_of(':');
        if (position == std::string::npos) {
            return "";
        }
        ip = address.substr(0, position);
        std::vector<std::string> split_num;
        boost::split(split_num, ip, boost::is_any_of("."), boost::token_compress_on);
        for(auto & num_str : split_num) {
            int64_t num = strtoll(num_str.c_str(), NULL, 10);
            std::bitset<8> num_bitset = num;
            ip_set += num_bitset.to_string();
        }
        return ip_set.substr(0, prefix);
    }
private:
    ClusterManager() {
        bthread_mutex_init(&_physical_mutex, NULL);
        bthread_mutex_init(&_instance_mutex, NULL);
        bthread_mutex_init(&_instance_param_mutex, NULL);
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
    bool is_legal_for_select_instance(
                const IdcInfo& idc,
                const std::string& candicate_instance,
                const std::set<std::string>& exclude_stores);
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
    std::string construct_instance_param_key(const std::string& resource_tag_or_address) {
        return MetaServer::CLUSTER_IDENTIFY
                + MetaServer::INSTANCE_PARAM_CLUSTER_IDENTIFY
                + resource_tag_or_address;
    }
    int load_instance_snapshot(const std::string& instance_prefix,
                                 const std::string& key, 
                                 const std::string& value);
    int load_instance_param_snapshot(const std::string& instance_param_prefix,
                                 const std::string& key, 
                                 const std::string& value);
    int load_physical_snapshot(const std::string& physical_prefix,
                                 const std::string& key, 
                                 const std::string& value);
    int load_logical_snapshot(const std::string& logical_prefix,
                                const std::string& key, 
                                const std::string& value);
    int get_meta_param(const std::string& resource_tag, const std::string& key, int64_t* value) {
        BAIDU_SCOPED_LOCK(_instance_param_mutex);
        auto iter = _instance_param_map.find(resource_tag);
        if (iter == _instance_param_map.end()) {
            return -1;
        }
        for (auto& param : iter->second.params()) {
            if (param.is_meta_param() && param.key() == key) {
                *value = strtoll(param.value().c_str(), NULL, 10);
                return 0;
            }
        }
        return -1;
    }
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
    //resource_tag与实例对应关系, key:resource_tag， value:实例
    std::unordered_map<std::string, std::set<std::string>>      _resource_tag_instance_map;

    //实例信息
    std::unordered_map<std::string, Instance>                   _instance_info;
    std::unordered_map<std::string, TimeCost>                   _tombstone_instance;
    std::unordered_set<std::string>                             _slow_instances;

    bthread_mutex_t                                             _instance_param_mutex;
    // 集群或实例的配置
    std::unordered_map<std::string, pb::InstanceParam>          _instance_param_map;

    // 调度相关的信息（leader balance, peer balance, pk_prefix balance)
    DoubleBufferedSchedulingInfo     _scheduling_info;

    MetaStateMachine*                                           _meta_state_machine = NULL;

    // resource tag -> network_segment prefix
    std::unordered_map<std::string, int>                        _resource_tag_network_prefix;
    // resource tag -> network segment -> instance
    typedef std::unordered_map<std::string, std::vector<std::string>>     NetworkInstanceMap;
    std::unordered_map<std::string, NetworkInstanceMap>         _resource_tag_instances_by_network;
    // resource tag -> last rolling network
    std::unordered_map<std::string, std::string>                _resource_tag_rolling_network;
    // resource tag -> position in last rolling network
    std::unordered_map<std::string, size_t>                     _resource_tag_rolling_position;
}; //class ClusterManager

}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
