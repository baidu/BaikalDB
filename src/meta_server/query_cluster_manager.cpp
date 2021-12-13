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

#include "query_cluster_manager.h"
#include <boost/algorithm/string.hpp>
#include "query_region_manager.h"

namespace baikaldb {
void QueryClusterManager::get_logical_info(const pb::QueryRequest* request,
                                      pb::QueryResponse* response) {
    ClusterManager* manager = ClusterManager::get_instance();
    BAIDU_SCOPED_LOCK(manager->_physical_mutex);
    if (!request->has_logical_room()) {
        for (auto& logical_physical_pair : manager->_logical_physical_map) {
            auto room = response->add_physical_rooms();
            room->set_logical_room(logical_physical_pair.first);
            for (auto& physical_room : logical_physical_pair.second) {
                room->add_physical_rooms(physical_room);
            }
        }
        return;
    }
    std::string logical_room = request->logical_room();
    if (manager->_logical_physical_map.find(logical_room) == manager->_logical_physical_map.end()) {
        response->set_errcode(pb::INPUT_PARAM_ERROR);
        response->set_errmsg("instance not exist");
        return;
    }
    auto room = response->add_physical_rooms(); 
    room->set_logical_room(logical_room);
    for (auto& physical_room : manager->_logical_physical_map[logical_room]) {
        room->add_physical_rooms(physical_room);
    }
}
void QueryClusterManager::get_physical_info(const pb::QueryRequest* request,
                                        pb::QueryResponse* response) {
    ClusterManager* manager = ClusterManager::get_instance();
    {
        BAIDU_SCOPED_LOCK(manager->_physical_mutex);
        if (!request->has_physical_room()) {
            for (auto& physical_info : manager->_physical_info) {
                auto res = response->add_physical_instances();
                res->set_physical_room(physical_info.first);
                res->set_logical_room(physical_info.second);
            }
        } else {
            std::string physical_room = request->physical_room();
            if (manager->_physical_info.find(physical_room) == manager->_physical_info.end()) {
                response->set_errcode(pb::INPUT_PARAM_ERROR);
                response->set_errmsg("instance not exist");
                return;
            }
            auto res = response->add_physical_instances();
            res->set_physical_room(physical_room);
            res->set_logical_room(manager->_physical_info[physical_room]);
        }
    }
    {
        BAIDU_SCOPED_LOCK(manager->_instance_mutex);
        {
            for (auto& physical_instance : *(response->mutable_physical_instances())) {
                std::string physical_room = physical_instance.physical_room();
                if (manager->_physical_instance_map.find(physical_room) 
                        == manager->_physical_instance_map.end()) {
                    continue;
                }
                for (auto& instance : manager->_physical_instance_map[physical_room]) {
                    physical_instance.add_instances(instance);
                }
            } 
        }
    }
}
void QueryClusterManager::get_instance_info(const pb::QueryRequest* request,
                                        pb::QueryResponse* response) {
    ClusterManager* manager = ClusterManager::get_instance();
    {
        BAIDU_SCOPED_LOCK(manager->_instance_mutex);
        if (!request->has_instance_address()) {
            for (auto& instance_info : manager->_instance_info) {
                auto instance_pb =  response->add_instance_infos();
                mem_instance_to_pb(instance_info.second, instance_pb);
            }
        } else {
            std::string instance_address = request->instance_address();
            if (manager->_instance_info.find(instance_address) == manager->_instance_info.end()) {
                response->set_errcode(pb::INPUT_PARAM_ERROR);
                response->set_errmsg("instance not exist");
                return;
            }
            auto instance_pb =  response->add_instance_infos();
            mem_instance_to_pb(manager->_instance_info[instance_address], instance_pb);
        }
    }
    for (auto& instance_info : response->instance_infos()) {
        QueryRegionManager::get_instance()->get_region_info_per_instance(instance_info.address(), response);
    }
}

void QueryClusterManager::get_instance_param(const pb::QueryRequest* request,
                                        pb::QueryResponse* response) {
    ClusterManager* manager = ClusterManager::get_instance();
    {
        BAIDU_SCOPED_LOCK(manager->_instance_param_mutex);
        if (!request->has_resource_tag() || request->resource_tag() == "") {
            for (auto iter : manager->_instance_param_map) {
                *(response->add_instance_params()) = iter.second;
            }
        } else {
            auto iter = manager->_instance_param_map.find(request->resource_tag());
            if (iter != manager->_instance_param_map.end()) {
                *(response->add_instance_params()) = iter->second;
            }
        }
    }
}

void QueryClusterManager::get_flatten_instance(const pb::QueryRequest* request,
                                        pb::QueryResponse* response) {
    ClusterManager* manager = ClusterManager::get_instance(); 
    std::string logical_room = request->logical_room();
    boost::trim(logical_room);
    std::string physical_room = request->physical_room();
    boost::trim(physical_room);
    std::string instance_address = request->instance_address();
    boost::trim(instance_address);
    std::string resource_tag = request->resource_tag();
    boost::trim(resource_tag);
    std::vector<Instance> instance_mems;
    {
        BAIDU_SCOPED_LOCK(manager->_instance_mutex);
        for (auto& instance : manager->_instance_info) {
            if (instance_address.size() != 0 && instance.second.address != instance_address) {
                continue;
            }
            if (logical_room.size() != 0 ) {
                std::string physical_room = instance.second.physical_room;
                if (manager->_physical_info[physical_room] != logical_room) {
                    continue;
                }
            }
            if (physical_room.size() != 0 && instance.second.physical_room != physical_room) {
                continue;
            }
            if (resource_tag.size() != 0 && instance.second.resource_tag != resource_tag) {
                continue;
            }
            instance_mems.push_back(instance.second);
        }
    }
    std::map<std::string, std::multimap<std::string, pb::QueryInstance>> logical_instance_infos;
    for (auto& instance_mem : instance_mems) {
        construct_query_response_for_instance(instance_mem, logical_instance_infos);
    }
    for (auto& logical_instance : logical_instance_infos) {
        for (auto& query_instance : logical_instance.second) {
            auto query_instance_ptr = response->add_flatten_instances();
            *query_instance_ptr = query_instance.second;
        }
    }
}

void QueryClusterManager::process_console_heartbeat(const pb::ConsoleHeartBeatRequest* request,
            pb::ConsoleHeartBeatResponse* response) {
    TimeCost cost;
    ClusterManager* manager = ClusterManager::get_instance(); 
    std::vector<Instance> instance_mems;
    {   
        BAIDU_SCOPED_LOCK(manager->_instance_mutex);
        for (auto& instance : manager->_instance_info) {
            instance_mems.push_back(instance.second);
        }
    }
    SELF_TRACE("cluster_info mutex cost time:%ld", cost.get_time());
    cost.reset();
    
    std::map<std::string, std::multimap<std::string, pb::QueryInstance>> logical_instance_infos;
    for (auto& instance_mem : instance_mems) {
        construct_query_response_for_instance(instance_mem, logical_instance_infos);
    }   
    
    for (auto& logical_instance : logical_instance_infos) {
        for (auto& query_instance : logical_instance.second) {
            auto query_instance_ptr = response->add_flatten_instances();
            *query_instance_ptr = query_instance.second;
        }
    }   
    SELF_TRACE("cluster_info update cost time:%ld", cost.get_time());
}

void QueryClusterManager::get_diff_region_ids(const pb::QueryRequest* request, 
                pb::QueryResponse* response) {
    if (!request->has_instance_address()) {
        response->set_errcode(pb::INPUT_PARAM_ERROR);
        response->set_errmsg("input has no instance_address");
        return;
    }
    std::string instance = request->instance_address();
    response->set_leader(instance);
    std::set<int64_t> region_ids;
    get_region_ids_per_instance(instance, region_ids);
    std::set<int64_t> peer_ids;
    QueryRegionManager::get_instance()->get_peer_ids_per_instance(instance, 
                                 peer_ids);
    if (region_ids == peer_ids) {
        std::string str = "peer_ids equal to region_ids, size: "  + to_string(region_ids.size());
        response->set_errmsg(str);
        return;
    }
    std::string str = "peer_ids not equal to region_ids, peer_id_size: "  
                        + to_string(peer_ids.size()) 
                        + ", region_id_size: "
                        + to_string(region_ids.size());
    response->set_errmsg(str);
    for (auto& region_id : region_ids) {
        if (peer_ids.find(region_id) == peer_ids.end()) {
            response->add_region_ids(region_id);
        }
    }
    for (auto& peer_id : peer_ids) {
        if (region_ids.find(peer_id) == region_ids.end()) {
            response->add_peer_ids(peer_id);
        }
    } 
}
void QueryClusterManager::get_region_ids(const pb::QueryRequest* request, 
                pb::QueryResponse* response) {
    if (!request->has_instance_address()) {
        response->set_errcode(pb::INPUT_PARAM_ERROR);
        response->set_errmsg("input has no instance_address");
        return;
    }
    std::string instance = request->instance_address();
    std::set<int64_t> region_ids;
    get_region_ids_per_instance(instance, region_ids);
    std::set<int64_t> peer_ids;
    QueryRegionManager::get_instance()->get_peer_ids_per_instance(instance, 
                                 peer_ids);
    for (auto& region_id : region_ids) {
        response->add_region_ids(region_id);
    }
    for (auto& peer_id : peer_ids) {
        response->add_peer_ids(peer_id);
    } 
}

void QueryClusterManager::get_network_segment(const pb::QueryRequest* request,
                              pb::QueryResponse* response) {

    if (!request->has_resource_tag()) {
        response->set_errcode(pb::INPUT_PARAM_ERROR);
        response->set_errmsg("input has no resource_tag");
        return;
    }
    ClusterManager* manager = ClusterManager::get_instance();
    auto get_network_segment_per_resource_tag = [this, manager, response](std::string resource_tag) {
        if (manager->_resource_tag_instances_by_network.find(resource_tag) ==
            manager->_resource_tag_instances_by_network.end()) {
            return;
        }
        for (auto& network_segment_to_instances : manager->_resource_tag_instances_by_network[resource_tag]) {
            for (auto& instance : network_segment_to_instances.second) {
                auto info = response->add_instance_infos();
                info->set_resource_tag(resource_tag);
                info->set_address(instance);
                info->set_network_segment(network_segment_to_instances.first + "/" +
                                          std::to_string(network_segment_to_instances.first.size()));
            }
        } 
    };
    BAIDU_SCOPED_LOCK(manager->_instance_mutex);
    if (!request->has_resource_tag() || request->resource_tag().empty()) {
        for(auto& resource_tag_pair : manager->_resource_tag_instances_by_network) {
            get_network_segment_per_resource_tag(resource_tag_pair.first); 
        }
    } else {
        get_network_segment_per_resource_tag(request->resource_tag());
    }
}

void QueryClusterManager::mem_instance_to_pb(const Instance& instance_mem, pb::InstanceInfo* instance_pb) {
    instance_pb->set_address(instance_mem.address);
    instance_pb->set_capacity(instance_mem.capacity);
    instance_pb->set_used_size(instance_mem.used_size);
    instance_pb->set_resource_tag(instance_mem.resource_tag);
    instance_pb->set_physical_room(instance_mem.physical_room);
    instance_pb->set_status(instance_mem.instance_status.state);
}

void QueryClusterManager::get_region_ids_per_instance(
                    const std::string& instance,
                    std::set<int64_t>& region_ids) {
    ClusterManager* manager = ClusterManager::get_instance();
    DoubleBufferedSchedulingInfo ::ScopedPtr info_iter;
    if (manager->_scheduling_info.Read(&info_iter) != 0) {
        DB_WARNING("read double_buffer_table error.");
        return;
    }
    auto instance_region_map_iter = info_iter->find(instance);
    if (instance_region_map_iter == info_iter->end()) {
        return;
    }
    for (auto& table_regions : instance_region_map_iter->second.regions_map) {
        for (auto& region_id : table_regions.second) {
            region_ids.insert(region_id);
        }
    }
}

void QueryClusterManager::get_region_count_per_instance(
            const std::string& instance,
            int64_t& count) {
    ClusterManager* manager = ClusterManager::get_instance();
    DoubleBufferedSchedulingInfo::ScopedPtr info_iter;
    if (manager->_scheduling_info.Read(&info_iter) != 0) {
        DB_WARNING("read double_buffer_table error.");
        return;
    }
    auto instance_region_map_iter = info_iter->find(instance);
    if (instance_region_map_iter == info_iter->end()) {
        return;
    }
    for (auto& table_regions : instance_region_map_iter->second.regions_map) {
        count += table_regions.second.size(); 
    }
}
void QueryClusterManager::construct_query_response_for_instance(const Instance& instance_info, 
     std::map<std::string, std::multimap<std::string, pb::QueryInstance>>& logical_instance_infos) {
    ClusterManager* manager = ClusterManager::get_instance();
    pb::QueryInstance query_info;
    std::string physical_room = instance_info.physical_room;
    std::string logical_room = manager->_physical_info[physical_room];
    query_info.set_physical_room(physical_room);
    query_info.set_logical_room(logical_room);
    query_info.set_address(instance_info.address);
    query_info.set_capacity(instance_info.capacity / 1024 / 1024 / 1024);
    query_info.set_used_size(instance_info.used_size / 1024 / 1024 / 1024);
    query_info.set_resource_tag(instance_info.resource_tag);
    query_info.set_physical_room(instance_info.physical_room);
    query_info.set_status(instance_info.instance_status.state);
    query_info.set_version(instance_info.version);
    int64_t peer_count = 0;
    int64_t region_leader_count = 0;
    QueryRegionManager::get_instance()->get_region_count_per_instance(instance_info.address, 
                                 peer_count, 
                                 region_leader_count); 
    query_info.set_peer_count(peer_count);
    query_info.set_region_leader_count(region_leader_count);
    int64_t count = 0;
    get_region_count_per_instance(instance_info.address, count);
    query_info.set_region_count(count);
    std::multimap<std::string, pb::QueryInstance> physical_instance_infos;
    if (logical_instance_infos.find(logical_room) != logical_instance_infos.end()) {
        physical_instance_infos = logical_instance_infos[logical_room];
    }
    physical_instance_infos.insert(std::pair<std::string, pb::QueryInstance>(physical_room, query_info));
    logical_instance_infos[logical_room] = physical_instance_infos; 
}

}//namespace
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
