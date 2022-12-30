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

#include "query_region_manager.h"
#include <boost/algorithm/string.hpp>
#include "store_interact.hpp"
#include "table_manager.h"
#include "query_table_manager.h"

#include <cstdint>

namespace baikaldb {

DECLARE_int64(store_heart_beat_interval_us);
DECLARE_int32(store_dead_interval_times);
DECLARE_int32(region_faulty_interval_times);

void QueryRegionManager::construct_query_region(const pb::RegionInfo* region_info, 
                     pb::QueryRegion* query_region_info) {
    int64_t table_id = region_info->table_id();
    query_region_info->set_region_id(region_info->region_id());
    query_region_info->set_table_id(table_id);
    query_region_info->set_table_name(region_info->table_name());
    query_region_info->set_partition_id(region_info->partition_id());
    query_region_info->set_replica_num(region_info->replica_num());
    query_region_info->set_version(region_info->version());
    query_region_info->set_conf_version(region_info->conf_version());
    query_region_info->set_parent(region_info->parent());
    query_region_info->set_num_table_lines(region_info->num_table_lines());
    time_t t = region_info->timestamp();
    struct tm t_result;
    localtime_r(&t, &t_result);
    char s[100];
    strftime(s, sizeof(s), "%F %T", &t_result);
    query_region_info->set_create_time(s);
    std::string start_key_string;
    std::string end_key_string;
    std::string raw_start_key = str_to_hex(std::string(region_info->start_key()));;
    QueryTableManager* qtmanager = QueryTableManager::get_instance();
    if (region_info->start_key() == "") {
        start_key_string = "-oo";
    } else {
        qtmanager->decode_key(table_id, TableKey(region_info->start_key()), start_key_string);
    }
    if (region_info->end_key() == "") {
        end_key_string = "+oo";
    } else {
        qtmanager->decode_key(table_id, TableKey(region_info->end_key()), end_key_string);
    }
    query_region_info->set_start_key(start_key_string);
    query_region_info->set_end_key(end_key_string);
    query_region_info->set_raw_start_key(raw_start_key);
    std::string peers;
    for (auto& peer : region_info->peers()) {
        peers = peers + peer + ", ";
    }
    if (peers.size() > 2) {
        peers.pop_back();
        peers.pop_back();
    } else {
        peers.clear();
    }

    //console展示region的peer信息时，补充learner信息
    std::string learner_peers, total_peers;
    if (region_info->is_learner()) {
        for (const auto& learner_peer : region_info->learners()) {
            learner_peers = learner_peers + learner_peer + "(learner), ";
        }
        if (learner_peers.size() > 2) {
            learner_peers.pop_back();//pop空格
            learner_peers.pop_back();//pop逗号
        } else {
            learner_peers.clear();
        }
        total_peers = peers + "," + learner_peers; //拼接得到所有peers的信息
    }
    query_region_info->set_peers(total_peers);
    query_region_info->set_leader(region_info->leader());
    query_region_info->set_status(region_info->status());
    query_region_info->set_used_size(region_info->used_size());
    query_region_info->set_log_index(region_info->log_index());
    if (!region_info->has_deleted()) {
        query_region_info->set_deleted(false);
    } else {
        query_region_info->set_deleted(region_info->deleted());
    }   
    query_region_info->set_can_add_peer(region_info->can_add_peer());
    std::string primary_key_string;
    qtmanager->get_primary_key_string(table_id, primary_key_string);
    query_region_info->set_primary_key(primary_key_string);
}

void QueryRegionManager::get_flatten_region(const pb::QueryRequest* request, pb::QueryResponse* response) {
    RegionManager* manager = RegionManager::get_instance();
    std::vector<int64_t> query_region_ids;
    std::string input_table_name = request->table_name();
    boost::trim(input_table_name);
    std::string input_database = request->database();
    boost::trim(input_database);
    std::string input_namespace_name = request->namespace_name();
    boost::trim(input_namespace_name);
    
    std::string instance = request->instance_address();
    boost::trim(instance);
    
    int64_t region_id = 0; 
    if (request->str_region_id().size() > 0) {
        region_id = strtoll(request->str_region_id().c_str(), NULL, 10);
    }
    if (region_id != 0) {
        query_region_ids.push_back(region_id);
    } else if (instance.size() != 0){
        manager->get_region_ids(instance, query_region_ids);
    } else if (input_namespace_name.size() == 0
                || input_database.size() == 0
                || input_table_name.size() == 0) {
        DB_WARNING("input is invalid query, request: %s", request->ShortDebugString().c_str());
        return;
    } else {
        std::string full_table_name = input_namespace_name + "\001" 
                + input_database + "\001"
                + input_table_name;
        TableManager::get_instance()->get_region_ids(full_table_name, query_region_ids);
    }
    
    std::vector<SmartRegionInfo> region_infos;
    for (auto& region_id : query_region_ids) {
        DB_WARNING("region_id: %ld", region_id);
    }
    manager->get_region_info(query_region_ids, region_infos);
    for (auto& region_info : region_infos) {
        DB_WARNING("region_info: %s", region_info->ShortDebugString().c_str());
    }

    std::map<std::string, std::multimap<std::string, pb::QueryRegion>> table_regions;
    for (auto& region_info : region_infos) {
        pb::QueryRegion query_region_info;
        construct_query_region(region_info.get(), &query_region_info);

        table_regions[region_info->table_name()].insert(std::pair<std::string, pb::QueryRegion>
                (region_info->start_key(), query_region_info));
    }
    for (auto& table_region : table_regions) {
        for (auto& id_regions : table_region.second) {
            auto* region = response->add_flatten_regions();
            *region = id_regions.second;
        }
    }
}

void QueryRegionManager::check_region_and_update(const std::unordered_map<int64_t, 
            pb::RegionHeartBeat>&  region_version_map,
            pb::ConsoleHeartBeatResponse* response) {
    RegionManager* manager = RegionManager::get_instance();
    std::vector<SmartRegionInfo> region_infos;
    TimeCost step_time_cost;     
    manager->traverse_region_map([&region_infos](SmartRegionInfo& region_info) {
            region_infos.push_back(region_info);
        });
    int64_t mutex_time = step_time_cost.get_time();
    step_time_cost.reset();
    for (auto& region_info : region_infos) {
        int64_t region_id = region_info->region_id();
        std::string leader = region_info->leader();
        bool need_update_region = false;
        bool need_update_stat = false;
        auto search = region_version_map.find(region_id);
        if (search != region_version_map.end()) {
            if (region_info->version() > search->second.version()) {
                need_update_region = true;
            } else if (region_info->conf_version() > search->second.conf_version()) {
                need_update_region = true;
            } else if (leader.compare(search->second.leader()) != 0) {
                need_update_region = true;
            } else if (region_info->num_table_lines() != search->second.num_table_lines()){
                need_update_stat = true;
            } else if (region_info->used_size() != search->second.used_size()){
                need_update_stat = true;
            }
        } else {
            need_update_region = true;
        }
        if (need_update_region) {
            auto change_info = response->add_region_change_infos(); 
            *(change_info->mutable_region_info()) = *(region_info.get());
        } else if (need_update_stat){
            auto change_info = response->add_region_change_infos(); 
            change_info->set_region_id(region_id);
            change_info->set_used_size(region_info->used_size());
            change_info->set_num_table_lines(region_info->num_table_lines());
        }
    }
    DB_NOTICE("mutex_time:%ld process_time:%ld", mutex_time, step_time_cost.get_time());
}

void QueryRegionManager::get_region_info(const pb::QueryRequest* request,
                                            pb::QueryResponse* response) {
    RegionManager* manager = RegionManager::get_instance();
    if (request->region_ids_size() == 0) {
        int64_t table_id = request->table_id();
        std::vector<SmartRegionInfo> region_infos;
        manager->traverse_region_map([&region_infos, &table_id](SmartRegionInfo& region_info) {
            if (table_id == 0 || table_id == region_info->table_id()) {
                region_infos.push_back(region_info);
            }
        });
        for (auto& region_info : region_infos) {
            auto region_pb = response->add_region_infos();
            *region_pb = *region_info;
        }    
    } else {
        for (int64_t region_id : request->region_ids()) {
            SmartRegionInfo region_ptr = manager->_region_info_map.get(region_id);
            if (region_ptr != nullptr) {
                auto region_pb = response->add_region_infos();
                *region_pb = *region_ptr;
            } else {
                response->set_errmsg("region info not exist");
                response->set_errcode(pb::REGION_NOT_EXIST);
            }    
        }
    }    
}
void QueryRegionManager::get_region_info_per_instance(const std::string& instance, 
                                                 pb::QueryResponse* response) {
    RegionManager* manager = RegionManager::get_instance();
    std::unordered_map<int64_t, std::set<int64_t>> table_region_map;
    {
        BAIDU_SCOPED_LOCK(manager->_instance_region_mutex);
        auto iter = manager->_instance_region_map.find(instance); 
        if (iter == manager->_instance_region_map.end()) {
            return;
        }
        table_region_map = iter->second;
    }
    for (auto& table_region_ids : table_region_map) {
        for (auto& region_id : table_region_ids.second) {
            SmartRegionInfo region_ptr = manager->_region_info_map.get(region_id);
            if (region_ptr != nullptr) {
                auto region_pb = response->add_region_infos();
                *region_pb = *region_ptr;
            }
        }
    }
}
void QueryRegionManager::get_region_count_per_instance(
            const std::string& instance,
            int64_t& region_count,
            int64_t& region_leader_count) {
    RegionManager* manager = RegionManager::get_instance();
    {
        BAIDU_SCOPED_LOCK(manager->_instance_region_mutex);
        if (manager->_instance_region_map.find(instance) != manager->_instance_region_map.end()) {
            for (auto& table_region_ids : manager->_instance_region_map[instance]) {
                region_count += table_region_ids.second.size();
            }
        }
    }
    {
        BAIDU_SCOPED_LOCK(manager->_instance_learner_mutex);
        if (manager->_instance_learner_map.find(instance) != manager->_instance_learner_map.end()) {
            for (auto& table_region_ids : manager->_instance_learner_map[instance]) {
                region_count += table_region_ids.second.size();
            }
        }
    }
    {
        BAIDU_SCOPED_LOCK(manager->_count_mutex);
        if (manager->_instance_leader_count.find(instance) != manager->_instance_leader_count.end()) {
            for (auto& table_leader : manager->_instance_leader_count[instance]) {
                region_leader_count += table_leader.second;
            }
        }
    }
}
void QueryRegionManager::get_peer_ids_per_instance(
            const std::string& instance,
            std::set<int64_t>& peer_ids) {
    RegionManager* manager = RegionManager::get_instance();
    std::unordered_map<int64_t, std::set<int64_t>> table_region_map;
    {
        BAIDU_SCOPED_LOCK(manager->_instance_region_mutex);
        auto iter = manager->_instance_region_map.find(instance);
        if (iter == manager->_instance_region_map.end()) {
            return;
        }
        table_region_map = iter->second;
    }

    for (auto& table_region_ids : table_region_map) {
        for (auto& region_id : table_region_ids.second) {
            peer_ids.insert(region_id);
        } 
    }
}

void QueryRegionManager::get_region_peer_status(const pb::QueryRequest* request, pb::QueryResponse* response) {
    RegionManager* manager = RegionManager::get_instance();
    std::map<int64_t, std::string> table_id_name_map;

    if (request->has_resource_tag()) {
        TableManager::get_instance()->get_table_by_resource_tag(request->resource_tag(), table_id_name_map);
    } else {
        TableManager::get_instance()->get_table_by_resource_tag("", table_id_name_map);
    }
    
    auto func = [&table_id_name_map, response](const int64_t& region_id, RegionPeerState& region_state) {
        int64_t table_id = 0;
        std::vector<pb::PeerStateInfo> region_peer_status_vec;
        for (auto& peer_status : region_state.legal_peers_state) {
            if (peer_status.has_table_id()) {
                table_id = peer_status.table_id();
                if (table_id_name_map.count(table_id) == 0) {
                    return;
                }
            } else {
                return;
            }

            if (peer_status.peer_status() == pb::STATUS_NORMAL
                && (butil::gettimeofday_us() - peer_status.timestamp() >
                FLAGS_store_heart_beat_interval_us * FLAGS_region_faulty_interval_times)) {
                peer_status.set_peer_status(pb::STATUS_NOT_HEARTBEAT);
            }
        }
        bool healthy = false;
        bool has_bad_peer = false;
        for (auto& peer_status : region_state.legal_peers_state) {
            if (peer_status.peer_status() == pb::STATUS_NORMAL) {
                healthy = true;
            } else {
                has_bad_peer = true;
            }
        }
        if (!healthy || has_bad_peer || !region_state.ilegal_peers_state.empty()) {
            for (auto& peer_status : region_state.legal_peers_state) {
                peer_status.set_region_id(region_id);
                region_peer_status_vec.emplace_back(peer_status);
            }  
        }
        for (auto& peer_status : region_state.ilegal_peers_state) {
            if (peer_status.has_table_id()) {
                table_id = peer_status.table_id();
                if (table_id_name_map.count(table_id) == 0) {
                    return;
                }
            } else {
                return;
            }
            peer_status.set_region_id(region_id);
            region_peer_status_vec.emplace_back(peer_status);
        }
        if (!region_peer_status_vec.empty()) {
            pb::RegionStateInfo* region_info = response->add_region_status_infos();
            region_info->set_table_id(region_peer_status_vec[0].table_id());
            region_info->set_region_id(region_peer_status_vec[0].region_id());
            region_info->set_table_name(table_id_name_map[table_id]);
            region_info->set_is_healthy(healthy);
            for (auto peer_status : region_peer_status_vec) {
                pb::PeerStateInfo* peer_info = region_info->add_peer_status_infos();
                *peer_info = peer_status;
            }
        }
    };
    manager->region_peer_state_map().traverse_with_key_value(func);
}

void QueryRegionManager::get_region_learner_status(const pb::QueryRequest* request, pb::QueryResponse* response) {
    RegionManager* manager = RegionManager::get_instance();
    std::map<int64_t, std::string> table_id_name_map;

    TableManager::get_instance()->get_table_by_learner_resource_tag(request->resource_tag(), table_id_name_map);
    
    auto func = [&table_id_name_map, response](const int64_t& region_id, RegionLearnerState& region_state) {
        int64_t table_id = 0;
        std::vector<pb::PeerStateInfo> region_peer_status_vec;
        region_peer_status_vec.reserve(100);
        bool healthy = false;
        bool has_bad_peer = false;
        for (auto& pair : region_state.learner_state_map) {
            auto& peer_id = pair.first;
            auto& peer_status = pair.second;
            if (peer_status.has_table_id()) {
                table_id = peer_status.table_id();
                if (table_id_name_map.count(table_id) == 0) {
                    return;
                }
            } else {
                return;
            }

            if (peer_status.peer_status() == pb::STATUS_NORMAL
                && (butil::gettimeofday_us() - peer_status.timestamp() >
                FLAGS_store_heart_beat_interval_us * FLAGS_region_faulty_interval_times * 4)) {
                peer_status.set_peer_status(pb::STATUS_NOT_HEARTBEAT);
                has_bad_peer = true;
            }
            if (peer_status.peer_status() == pb::STATUS_NORMAL) {
                healthy = true;
            } else {
                has_bad_peer = true;
            }
            peer_status.set_region_id(region_id);
            peer_status.set_peer_id(peer_id);
            region_peer_status_vec.emplace_back(peer_status);
        }
        if (!healthy || has_bad_peer) {
            if (!region_peer_status_vec.empty()) {
                pb::RegionStateInfo* region_info = response->add_region_status_infos();
                region_info->set_table_id(region_peer_status_vec[0].table_id());
                region_info->set_region_id(region_peer_status_vec[0].region_id());
                region_info->set_table_name(table_id_name_map[table_id]);
                region_info->set_is_healthy(healthy);
                for (auto peer_status : region_peer_status_vec) {
                    pb::PeerStateInfo* peer_info = region_info->add_peer_status_infos();
                    *peer_info = peer_status;
                }
            }
        }
    };
    manager->_region_learner_peer_state_map.traverse_with_key_value(func);
}

void QueryRegionManager::send_transfer_leader(const pb::QueryRequest* request, pb::QueryResponse* response) {
    std::string old_leader = request->old_leader();
    boost::trim(old_leader);
    std::string new_leader = request->new_leader();
    boost::trim(new_leader);
    int64_t region_id = request->region_ids_size() == 1 ? request->region_ids(0) : 0;
    if (region_id == 0 || old_leader.size() == 0 || new_leader.size() == 0) {
        DB_FATAL("input param error, request:%s", request->ShortDebugString().c_str());
        response->set_errcode(pb::INPUT_PARAM_ERROR);
        response->set_errmsg("input param error");
        return;
    }   
    pb::RaftControlRequest raft_control_request;
    pb::RaftControlResponse raft_control_response;
    raft_control_request.set_op_type(pb::TransLeader);
    raft_control_request.set_region_id(region_id);
    raft_control_request.set_new_leader(new_leader);
    StoreInteract store_interact(old_leader);
    auto ret = store_interact.send_request("region_raft_control", raft_control_request, raft_control_response);
    if (ret < 0) {
        DB_FATAL("transfer leader fail, old_leader: %s, new_leader: %s, region_id: %ld",
                    old_leader.c_str(), new_leader.c_str(), region_id);
    }
    response->set_errcode(raft_control_response.errcode());
    response->set_errmsg(raft_control_response.errmsg());
    response->set_leader(raft_control_response.leader());
}
void QueryRegionManager::send_set_peer(const pb::QueryRequest* request, pb::QueryResponse* response) {
    std::string leader = request->old_leader();
    boost::trim(leader);
    std::string new_peers = request->new_peers();
    boost::trim(new_peers);
    std::string old_peers = request->old_peers();
    boost::trim(old_peers);
    int64_t region_id = request->region_ids_size() == 1 ? request->region_ids(0) : 0;
    if (region_id == 0 || new_peers.size() == 0 || old_peers.size() == 0 || leader.size() == 0) {
        DB_FATAL("input param error, request:%s", request->ShortDebugString().c_str());
        response->set_errcode(pb::INPUT_PARAM_ERROR);
        response->set_errmsg("input param error");
        return;
    }
    std::vector<std::string> list_old_peers;
    boost::split(list_old_peers, old_peers, boost::is_any_of(","));
    for (auto& old_peer : list_old_peers) {
        boost::trim(old_peer);
    }
    std::vector<std::string> list_new_peers;
    boost::split(list_new_peers, new_peers, boost::is_any_of(","));
    for (auto& new_peer : list_new_peers) {
         boost::trim(new_peer);
    }
    if (list_old_peers.size() == list_new_peers.size()) {
        DB_FATAL("input old peers size equal to new old peers");
        response->set_errcode(pb::INPUT_PARAM_ERROR);
        response->set_errmsg("input param error");
        return;
    }
    //remove_peer
    if ((list_old_peers.size() - list_new_peers.size()) == 1) {
        pb::RaftControlRequest raft_control_request;
        raft_control_request.set_op_type(pb::SetPeer);
        raft_control_request.set_region_id(region_id);
        std::string remove_old_peer;
        for (auto& old_peer : list_old_peers) {
            auto iter = std::find(list_new_peers.begin(), list_new_peers.end(), old_peer);
            if (iter == list_new_peers.end()) {
                remove_old_peer = old_peer;
            }
            raft_control_request.add_old_peers(old_peer);
        }
        for (auto& new_peer : list_new_peers) {
           auto iter = std::find(list_old_peers.begin(), list_old_peers.end(), new_peer);
           if (iter == list_old_peers.end()) {
                DB_FATAL("input old peers size equal to new old peers");
                response->set_errcode(pb::INPUT_PARAM_ERROR);
                response->set_errmsg("input param error");
                return; 
           }
           raft_control_request.add_new_peers(new_peer);
        }
        pb::RaftControlResponse raft_control_response;
        StoreInteract store_interact(leader);
        auto ret = store_interact.send_request("region_raft_control", raft_control_request, raft_control_response);
        if (ret < 0) {
            DB_FATAL("remove peer fail, request: %s, new_leader: %s, region_id: %ld",
                        raft_control_request.ShortDebugString().c_str(), 
                        leader.c_str(), region_id);
        }
        response->set_errcode(raft_control_response.errcode());
        response->set_errmsg(raft_control_response.errmsg());
        response->set_leader(raft_control_response.leader());
        if (ret == 0) {
            Bthread bth(&BTHREAD_ATTR_SMALL);
            auto remove_function = [this, remove_old_peer, region_id]() {
                send_remove_region_request(remove_old_peer, region_id);
            };
            bth.run(remove_function);
        }
        return;
    }
    //add_peer
    if ((list_new_peers.size() - list_old_peers.size()) == 1) {
        pb::AddPeer add_peer_request;
        add_peer_request.set_region_id(region_id);
        for (auto& old_peer : list_old_peers) {
            add_peer_request.add_new_peers(old_peer);
            add_peer_request.add_old_peers(old_peer);
        }
        for (auto& new_peer : list_new_peers) {
            auto iter = std::find(list_old_peers.begin(), list_old_peers.end(), new_peer);
            if (iter == list_old_peers.end()) {
                add_peer_request.add_new_peers(new_peer); 
            }
        }
        pb::StoreRes add_peer_response;
        StoreInteract store_interact(leader);
        auto ret = store_interact.send_request("add_peer", add_peer_request, add_peer_response);
        if (ret < 0) {
            DB_FATAL("add peer fail, request: %s, new_leader: %s, region_id: %ld",
                        add_peer_request.ShortDebugString().c_str(), 
                        leader.c_str(), region_id);
        }
        response->set_errcode(add_peer_response.errcode());
        response->set_errmsg(add_peer_response.errmsg());
        response->set_leader(add_peer_response.leader());
        return;
    }
    response->set_errcode(pb::INPUT_PARAM_ERROR);
    response->set_errmsg("input param error");
}
void QueryRegionManager::send_remove_region_request(std::string instance_address, int64_t region_id) {
    pb::RemoveRegion remove_region;
    remove_region.set_region_id(region_id);
    remove_region.set_force(true);
    pb::StoreRes response;
    StoreInteract store_interact(instance_address);
    auto ret = store_interact.send_request("remove_region", remove_region, response);
    if (ret < 0) {
        DB_FATAL("remove region fail, request: %s, instance_address: %s, region_id: %ld",
                    remove_region.ShortDebugString().c_str(),
                    instance_address.c_str(),
                    region_id);
    }
}
}//namespace 

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
