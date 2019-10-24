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

#include <functional>
#include <unordered_set>

#include "schema_manager.h"
#include "meta_server.h"
#include "table_key.h"
#include "ddl_common.h"

namespace baikaldb {
enum MergeStatus {
    MERGE_IDLE   = 0, //空闲
    MERGE_SRC    = 1,  //用于merge源
    MERGE_DST    = 2   //用于merge目标
};
struct RegionDesc {
    int64_t region_id;
    MergeStatus merge_status;
};

struct TableMem {
    bool whether_level_table;
    pb::SchemaInfo schema_pb;
    std::unordered_map<int64_t, std::set<int64_t>> partition_regions;//该信息只保存在内存中
    std::unordered_map<std::string, int32_t> field_id_map;
    std::unordered_map<std::string, int64_t> index_id_map;
    //start_key=>regionid
    std::map<std::string, RegionDesc>  startkey_regiondesc_map;
    //发生split或merge时，用以下三个map暂存心跳上报的region信息，保证整体更新
    //start_key => region 存放new region，new region为分裂出来的region
    std::map<std::string, SmartRegionInfo> startkey_newregion_map;
    //region id => none region 存放空region
    std::map<int64_t, SmartRegionInfo> id_noneregion_map;
    //region id => region 存放key发生变化的region，以该region为基准，查找merge或split所涉及到的所有region
    std::map<int64_t, SmartRegionInfo> id_keyregion_map;
    bool is_global_index = false;
    int64_t global_index_id = 0;
    int64_t main_table_id = 0;
    void print() {
        /*
        DB_WARNING("whether_level_table: %d, schema_pb: %s, is_global_index: %d, main_table_id:%ld, global_index_id: %ld",
                    whether_level_table, schema_pb.ShortDebugString().c_str(), is_global_index,  main_table_id, global_index_id);
        return;
        for (auto& partition_region : partition_regions) {
            for (auto& region : partition_region.second) {
                DB_WARNING("table_id: %ld region_id: %ld", global_index_id, region);
            }
        }
        */
    }
};
struct DdlPeerMem {
    pb::IndexState   workstate;
    std::string     peer;
};

struct DdlRegionMem {
    int64_t region_id;
    pb::IndexState workstate;
    std::unordered_map<std::string, DdlPeerMem> peer_infos;
};

struct DdlWorkMem {
    uint64_t table_id;
    pb::DdlWorkInfo work_info; //持久化、与store交互更新
    bool is_rollback = false;
    std::unordered_map<int64_t, DdlRegionMem> region_ddl_infos;
    int32_t check_del_region_num = 0;
    std::unordered_map<pb::IndexState, int, std::hash<int>> state_count;
    int32_t all_peer_num = 0;
    std::string resource_tag;
};

class TableManager {
public:
    ~TableManager() {
        bthread_mutex_destroy(&_table_mutex);
        bthread_mutex_destroy(&_table_ddlinfo_mutex);
        bthread_mutex_destroy(&_all_table_ddlinfo_mutex);
        bthread_mutex_destroy(&_log_entry_mutex);
    }
    static TableManager* get_instance()  {
        static TableManager instance;
        return &instance;
    }
    friend class QueryTableManager;
    void update_table_internal(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done,
    std::function<void(const pb::MetaManagerRequest& request, pb::SchemaInfo& mem_schema_pb)> update_callback);
    void create_table(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void drop_table(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void rename_table(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void update_byte_size(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void update_split_lines(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void update_schema_conf(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void update_dists(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void update_resource_tag(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);

    void add_field(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void add_index(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void drop_index(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void drop_field(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void rename_field(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void modify_field(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);

    void update_index_status(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void delete_ddlwork(const pb::MetaManagerRequest& request, braft::Closure* done);

    void process_schema_heartbeat_for_store(
                std::unordered_map<int64_t, int64_t>& store_table_id_version,
                pb::StoreHeartBeatResponse* response);
    void check_update_or_drop_table(const pb::BaikalHeartBeatRequest* request,
                pb::BaikalHeartBeatResponse* response);
    void check_table_exist_for_peer(
                    const pb::StoreHeartBeatRequest* request,
                    pb::StoreHeartBeatResponse* response);
    void check_add_table(std::set<int64_t>& report_table_ids,
            std::vector<int64_t>& new_add_region_ids,
            pb::BaikalHeartBeatResponse* response);

    void check_add_region(const std::set<std::int64_t>& report_table_ids,
                        std::unordered_map<int64_t, std::set<std::int64_t>>& report_region_ids, 
                        pb::BaikalHeartBeatResponse* response);

    int load_table_snapshot(const std::string& value);
    int erase_region(int64_t table_id, int64_t region_id, std::string start_key);
    int64_t get_next_region_id(int64_t table_id, std::string start_key, 
            std::string end_key);
    int add_startkey_regionid_map(const pb::RegionInfo& region_info);
    bool check_region_when_update(int64_t table_id, std::string min_start_key, 
            std::string max_end_key);
    int check_startkey_regionid_map();
    void update_startkey_regionid_map_old_pb(int64_t table_id, 
            std::map<std::string, int64_t>& key_id_map);
    void update_startkey_regionid_map(int64_t table_id, std::string min_start_key, 
                                      std::string max_end_key, 
                                      std::map<std::string, int64_t>& key_id_map);
    int64_t get_pre_regionid(int64_t table_id, const std::string& start_key);
    int64_t get_startkey_regionid(int64_t table_id, const std::string& start_key);
    void add_new_region(const pb::RegionInfo& leader_region_info);
    void add_update_region(const pb::RegionInfo& leader_region_info, bool is_none);
    int get_merge_regions(int64_t table_id, 
                          std::string new_start_key, std::string origin_start_key, 
                          std::map<std::string, RegionDesc>& startkey_regiondesc_map,
                          std::map<int64_t, SmartRegionInfo>& id_noneregion_map,
                          std::vector<SmartRegionInfo>& regions);
    int get_split_regions(int64_t table_id, 
                          std::string new_end_key, std::string origin_end_key, 
                          std::map<std::string, SmartRegionInfo>& key_newregion_map,
                          std::vector<SmartRegionInfo>& regions);
    int get_presplit_regions(int64_t table_id, 
                                           std::map<std::string, SmartRegionInfo>& key_newregion_map,
                                           pb::MetaManagerRequest& request);
    void get_update_region_requests(int64_t table_id, 
                                    std::vector<pb::MetaManagerRequest>& requests);
    void recycle_update_region();
    void check_update_region(const pb::LeaderHeartBeat& leader_region,
                             const SmartRegionInfo& master_region_info);
    

    void process_ddl_heartbeat_for_store(const pb::StoreHeartBeatRequest* request,
                                             pb::StoreHeartBeatResponse* response,
                                             uint64_t log_id);

   
public:
    void set_max_table_id(int64_t max_table_id) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        _max_table_id = max_table_id;
    }
    int64_t get_max_table_id() {
        BAIDU_SCOPED_LOCK(_table_mutex);
        return _max_table_id;
    }

    void set_table_pb(const pb::SchemaInfo& schema_pb) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        _table_info_map[schema_pb.table_id()].schema_pb = schema_pb;
        _table_info_map[schema_pb.table_id()].print();
        for (auto& index_info : schema_pb.indexs()) {
            if (is_global_index(index_info)) {
                _table_info_map[index_info.index_id()].schema_pb = schema_pb;
                _table_info_map[index_info.index_id()].print(); 
            }
        }
    }
    int64_t get_table_id(const std::string& table_name) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_id_map.find(table_name) != _table_id_map.end()) {
            return _table_id_map[table_name];
        }
        return 0;
    }
    void add_field_mem(int64_t table_id, 
            std::unordered_map<std::string, int32_t>& add_field_id_map) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_info_map.find(table_id) == _table_info_map.end()) {
            return;
        }
        for (auto& add_field : add_field_id_map) {
            _table_info_map[table_id].field_id_map[add_field.first] = add_field.second;
        }
    }
    void drop_field_mem(int64_t table_id, std::vector<std::string>& drop_field_names) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_info_map.find(table_id) == _table_info_map.end()) {
            return;
        }
        for (auto& drop_name : drop_field_names) {
            _table_info_map[table_id].field_id_map.erase(drop_name);
        }
    }
    void set_table_info(const TableMem& table_mem) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        std::string table_name = table_mem.schema_pb.namespace_name()
                                    + "\001" + table_mem.schema_pb.database()
                                    + "\001" + table_mem.schema_pb.table_name();
        _table_info_map[table_mem.schema_pb.table_id()] = table_mem;
        _table_id_map[table_name] = table_mem.schema_pb.table_id();
        _table_info_map[table_mem.schema_pb.table_id()].print();
        //全局二级索引有region信息，所以需要独立为一项
        for (auto& index_info : table_mem.schema_pb.indexs()) {
            if (!is_global_index(index_info)) {
                continue;
            }
            std::string index_table_name = table_name + "\001" + index_info.index_name();
            _table_info_map[index_info.index_id()] = table_mem;
            _table_info_map[index_info.index_id()].is_global_index = true;
            _table_info_map[index_info.index_id()].main_table_id = table_mem.schema_pb.table_id();
            _table_info_map[index_info.index_id()].global_index_id = index_info.index_id();
            _table_id_map[index_table_name] = index_info.index_id();
            _table_info_map[index_info.index_id()].print();
        } 
    }
    void erase_table_info(int64_t table_id) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_info_map.find(table_id) == _table_info_map.end()) {
            return;
        }
        std::string table_name = _table_info_map[table_id].schema_pb.namespace_name()
                                    + "\001" + _table_info_map[table_id].schema_pb.database()
                                    + "\001" + _table_info_map[table_id].schema_pb.table_name();
        //处理全局二级索引
        for (auto& index_info : _table_info_map[table_id].schema_pb.indexs()) {
            if (!is_global_index(index_info)) {
                continue;
            }
            std::string index_table_name = table_name + "\001" + index_info.index_name();
            _table_info_map.erase(index_info.index_id());
            _table_id_map.erase(index_table_name);  
        }
        _table_id_map.erase(table_name);
        _table_info_map.erase(table_id);
    }
    void swap_table_name(const std::string& old_table_name, const std::string new_table_name) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_id_map.find(old_table_name) == _table_id_map.end()) {
            return;
        }
        int64_t table_id = _table_id_map[old_table_name];
        for (auto& index_info : _table_info_map[table_id].schema_pb.indexs()) {
            if (!is_global_index(index_info)) {
                continue;
            }
            std::string old_index_table_name = old_table_name + "\001" + index_info.index_name();
            std::string new_index_table_name = new_table_name + "\001" + index_info.index_name();
            _table_id_map.erase(old_index_table_name);
            _table_id_map[new_index_table_name] = index_info.index_id();
        }
        _table_id_map.erase(old_table_name);
        _table_id_map[new_table_name] = table_id;
    }
    int whether_exist_table_id(int64_t table_id) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_info_map.find(table_id) == _table_info_map.end()) {
            return -1;
        }
        return 0;
    }

    void add_region_id(int64_t table_id, int64_t partition_id, int64_t region_id) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_info_map.find(table_id) == _table_info_map.end()) {
            DB_WARNING("table_id: %ld not exist", table_id);
            return;
        }
        _table_info_map[table_id].partition_regions[partition_id].insert(region_id);
        _table_info_map[table_id].print();
    }
    void delete_region_ids(const std::vector<int64_t>& table_ids,
                          const std::vector<int64_t>& partition_ids,
                          const std::vector<int64_t>& region_ids) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (table_ids.size() != partition_ids.size()
                || partition_ids.size() != region_ids.size()) {
             DB_WARNING("input param not legal, "
                        "table_ids_size:%ld partition_ids_size:%ls, region_ids_size:%ld",
                        table_ids.size(), partition_ids.size(), region_ids.size());
             return;
        }
        for (size_t i = 0; i < table_ids.size(); ++i) {
            if (_table_info_map.find(table_ids[i]) != _table_info_map.end()) {
                _table_info_map[table_ids[i]].partition_regions[partition_ids[i]].erase(region_ids[i]);
                _table_info_map[table_ids[i]].print();
            }
        }
    }
    int get_table_info(const std::string& table_name, pb::SchemaInfo& table_info) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_id_map.find(table_name) == _table_id_map.end()) {
            return -1;
        }
        int64_t table_id = _table_id_map[table_name];
        if (_table_info_map.find(table_id) == _table_info_map.end()) {
            return -1;
        }
        table_info = _table_info_map[table_id].schema_pb;
        return 0;
    }
    int get_table_info(int64_t table_id, pb::SchemaInfo& table_info) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_info_map.find(table_id) == _table_info_map.end()) {
            return -1;
        }
        table_info = _table_info_map[table_id].schema_pb;
        return 0;
    }
    int get_resource_tag(int64_t table_id, std::string& resource_tag) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_info_map.find(table_id) == _table_info_map.end()) {
            return -1;
        }
        resource_tag = _table_info_map[table_id].schema_pb.resource_tag();
        return 0;
    }

    void get_table_info(const std::set<int64_t> table_ids, 
            std::unordered_map<int64_t, int64_t>& table_replica_nums,
            std::unordered_map<int64_t, std::string>& table_resource_tags,
            std::unordered_map<int64_t, std::unordered_map<std::string, int64_t>>& table_replica_dists_maps) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        for (auto& table_id : table_ids) {
            if (_table_info_map.find(table_id) != _table_info_map.end()) {
                table_replica_nums[table_id] = _table_info_map[table_id].schema_pb.replica_num();
                table_resource_tags[table_id] = _table_info_map[table_id].schema_pb.resource_tag();
                //没有指定机房分布的表，也在map中有key
                table_replica_dists_maps[table_id];
                for (auto& replica_dist : _table_info_map[table_id].schema_pb.dists()) {
                    if (replica_dist.count() != 0) {
                        table_replica_dists_maps[table_id][replica_dist.logical_room()] = replica_dist.count();
                    }
                }
            }
        }
    }
    int get_main_logical_room(int64_t table_id, std::string& main_logical_room) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_info_map.find(table_id) == _table_info_map.end()) {
            return -1;
        }
        main_logical_room = _table_info_map[table_id].schema_pb.main_logical_room();
        return 0;
    }
    int64_t get_replica_dists(int64_t table_id, std::unordered_map<std::string, int64_t>& replica_dists_map) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_info_map.find(table_id) == _table_info_map.end()) {
            return -1;
        }
        for (auto& replica_dist : _table_info_map[table_id].schema_pb.dists()) {
            if (replica_dist.count() != 0) {
                replica_dists_map[replica_dist.logical_room()] = replica_dist.count();
            }
        }
        return 0;
    }
    bool whether_replica_dists(int64_t table_id) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_info_map.find(table_id) == _table_info_map.end()) {
            return false;
        }
        if (_table_info_map[table_id].schema_pb.dists_size() > 0) {
            return true;
        }
        return false;
    }
    int64_t get_region_count(int64_t table_id) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_info_map.find(table_id) == _table_info_map.end()) {
            return 0;
        }
        int64_t count = 0;
        std::unordered_map<int64_t, std::set<int64_t>> partition_regions;
        for (auto& partition_region : _table_info_map[table_id].partition_regions) {
            count += partition_region.second.size();
        }
        return count;
    }
    void get_region_count(const std::set<std::int64_t>& table_ids,
                        std::unordered_map<int64_t, int64_t>& table_region_count) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        for (auto& table_info : _table_info_map) {
            int64_t table_id = table_info.first;
            int64_t count = 0;
            for (auto& partition_region : table_info.second.partition_regions) {
                count += partition_region.second.size(); 
            }
            table_region_count[table_id] = count;
        }    
    }
    int get_replica_num(int64_t table_id, int64_t& replica_num) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_info_map.find(table_id) == _table_info_map.end()) {
            return -1;
        }
        replica_num = _table_info_map[table_id].schema_pb.replica_num();
        return 0;
    }
    void get_region_ids(const std::string& full_table_name, std::vector<int64_t>& query_region_ids) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_id_map.find(full_table_name) == _table_id_map.end()) {
            return;
        }
        int64_t table_id = _table_id_map[full_table_name];
        if (_table_info_map.find(table_id) == _table_info_map.end()) {
            return;
        }
        std::set<int64_t> global_indexs;
        global_indexs.insert(table_id);
        for (auto& index_info : _table_info_map[table_id].schema_pb.indexs()) {
            if (is_global_index(index_info)) {
                global_indexs.insert(index_info.index_id());
            }
        }
        for (auto& index_id: global_indexs) {
            for (auto& partition_regions : _table_info_map[index_id].partition_regions) {
                for (auto& region_id :  partition_regions.second) {
                    query_region_ids.push_back(region_id);    
                }
            }
        }
    }
    void get_region_ids(int64_t table_id, std::vector<int64_t>& region_ids) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_info_map.find(table_id) == _table_info_map.end()) {
            return;
        }
        for (auto& partition_regions : _table_info_map[table_id].partition_regions) {
            for (auto& region_id :  partition_regions.second) {
                region_ids.push_back(region_id);    
            }
        }
    }
    int64_t get_row_count(int64_t table_id);
    void get_region_ids(const std::vector<int64_t>& table_ids,
                         std::unordered_map<int64_t,std::vector<int64_t>>& region_ids) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        for (auto& table_id : table_ids) {
            for (auto& partition_regions : _table_info_map[table_id].partition_regions) {
                for (auto& region_id :  partition_regions.second) {
                    region_ids[table_id].push_back(region_id);
                }
            }
        }
    }
    void clear() {
        _table_id_map.clear();
        _table_info_map.clear();
        SchemaIncrementalMap* background = _incremental_schemainfo_map.read_background();
        background->clear();
        SchemaIncrementalMap* frontground = _incremental_schemainfo_map.read();
        frontground->clear();
    }

    int load_ddl_snapshot(const std::string& value);

    int get_ddlwork_info(int64_t table_id, std::vector<pb::DdlWorkInfo>& ddlwork_infos) {
        {
            BAIDU_SCOPED_LOCK(_table_ddlinfo_mutex);
            if (_table_ddlinfo_map.find(table_id) != _table_ddlinfo_map.end()) {
                ddlwork_infos.push_back(_table_ddlinfo_map[table_id].work_info);
            }
        }
        {
            BAIDU_SCOPED_LOCK(_all_table_ddlinfo_mutex);
            auto range = _all_table_ddlinfo_map.equal_range(table_id);
            for (auto i = range.first; i != range.second; ++i) {
                ddlwork_infos.push_back(i->second.work_info);
            }
        }
        
        return 0;
    }

    int get_index_state(int64_t table_id, int64_t index_id, pb::IndexState& index_state) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_info_map.find(table_id) == _table_info_map.end()) {
            return -1;
        }
        auto& table_info = _table_info_map[table_id].schema_pb;
        for (const auto& index_info : table_info.indexs()) {
            if (index_info.index_id() == index_id) {
                index_state = index_info.state();
                return 0;
            }
        }
        return -1; 
    }
    bool check_and_update_incremental(const pb::BaikalHeartBeatRequest* request,
                         pb::BaikalHeartBeatResponse* response, int64_t applied_index);
private:
    TableManager(): _max_table_id(0) {
        bthread_mutex_init(&_table_mutex, NULL);
        bthread_mutex_init(&_table_ddlinfo_mutex, NULL);
        bthread_mutex_init(&_all_table_ddlinfo_mutex, NULL);
        bthread_mutex_init(&_log_entry_mutex, NULL);
    }
    int write_schema_for_not_level(TableMem& table_mem,
                                    braft::Closure* done,
                                    int64_t max_table_id_tmp,
                                     bool has_auto_increment);

    int send_auto_increment_request(const pb::MetaManagerRequest& request);
    void send_create_table_request(const std::string& namespace_name,
                                    const std::string& database,
                                    const std::string& table_name,
                                    std::shared_ptr<std::vector<pb::InitRegion>> init_regions);

    int write_schema_for_level(const TableMem& table_mem,
                               const int64_t apply_index,
                                braft::Closure* done,
                                int64_t max_table_id_tmp,
                                bool has_auto_increment);
    int update_schema_for_rocksdb(int64_t table_id, 
                                    const pb::SchemaInfo& schema_info, 
                                    braft::Closure* done);
    
    void send_drop_table_request(const std::string& namespace_name,
                                const std::string& database,
                                const std::string& table_name);

    int check_table_exist(const pb::SchemaInfo& schema_info,
                            int64_t& namespace_id,
                            int64_t& database_id,
                            int64_t& table_id);

    bool check_field_exist(const std::string& field_name,
                        int64_t table_id);
    
    int check_index(const pb::IndexInfo& index_info_to_check,
                   const pb::SchemaInfo& schema_info, int64_t& index_id);

    int check_table_exist(const pb::SchemaInfo& schema_info,
                            int64_t& table_id) {
        int64_t namespace_id = 0;
        int64_t database_id = 0;
        return check_table_exist(schema_info, namespace_id, database_id, table_id);
    }
    int alloc_field_id(pb::SchemaInfo& table_info, bool& has_auto_increment, TableMem& table_mem);
    int alloc_index_id(pb::SchemaInfo& table_info, TableMem& table_mem, int64_t& max_table_id_tmp);
    void construct_common_region(pb::RegionInfo* region_info, int32_t replica_num) {
        region_info->set_version(1);
        region_info->set_conf_version(1);
        region_info->set_replica_num(replica_num);
        region_info->set_used_size(0);
        region_info->set_log_index(0);
        region_info->set_status(pb::IDLE);
        region_info->set_can_add_peer(false);
        region_info->set_parent(0);

        region_info->set_timestamp(time(NULL));
    }
    std::string construct_table_key(int64_t table_id) {
        std::string table_key;
        table_key = MetaServer::SCHEMA_IDENTIFY + MetaServer::TABLE_SCHEMA_IDENTIFY;
        table_key.append((char*)&table_id, sizeof(int64_t));
        return table_key;
    }

    std::string construct_ddl_key(int64_t table_id) {
        std::string ddl_key;
        ddl_key = MetaServer::SCHEMA_IDENTIFY + MetaServer::DDLWORK_IDENTIFY;
        ddl_key.append((char*)&table_id, sizeof(int64_t));
        return ddl_key;
    }

    std::string construct_max_table_id_key() {
        std::string max_table_id_key = MetaServer::SCHEMA_IDENTIFY
                            + MetaServer::MAX_ID_SCHEMA_IDENTIFY
                            + SchemaManager::MAX_TABLE_ID_KEY;
        return max_table_id_key;
    }

    int update_ddlwork_for_rocksdb(int64_t table_id, 
                                    const pb::DdlWorkInfo& _info, 
                                    braft::Closure* done);

    void common_update_ddlwork_info_heartbeat_for_store(const pb::StoreHeartBeatRequest* request);

    void process_ddl_add_index_process(
        pb::StoreHeartBeatResponse* response,
        DdlWorkMem& meta_work);

    void process_ddl_common_init(
        pb::StoreHeartBeatResponse* response,
        const pb::DdlWorkInfo& work_info);

    void process_ddl_del_index_process(
        pb::StoreHeartBeatResponse* response,
        DdlWorkMem& meta_work);

    void update_ddlwork_info(const pb::DdlWorkInfo& ddl_work, 
        pb::OpType update_op);

    int init_ddlwork(const pb::MetaManagerRequest& request, DdlWorkMem& ddl_work_mem);
    int init_ddlwork_drop_index(const pb::MetaManagerRequest& request, DdlWorkMem& ddl_work_mem);
    int init_ddlwork_add_index(const pb::MetaManagerRequest& request, DdlWorkMem& ddl_work_mem, pb::IndexInfo& index_info);
    void update_index_status(const pb::DdlWorkInfo& ddl_work);

    bool process_ddl_update_job_index(DdlWorkMem& meta_work_info, pb::IndexState expected_state,
        pb::IndexState state, pb::StoreHeartBeatResponse* response);
    void drop_index_request(const pb::DdlWorkInfo& ddl_work);
    void rollback_ddlwork(DdlWorkMem& ddlwork_mem);

    bool is_global_index(const pb::IndexInfo& index_info) {
        return index_info.is_global() == true && 
            (index_info.index_type() == pb::I_UNIQ || index_info.index_type() == pb::I_KEY);
    }
    void delete_ddl_region_info(DdlWorkMem& ddlwork, std::vector<int64_t>& region_ids);
    void add_ddl_region_info(const pb::StoreHeartBeatRequest& store_ddlinfo_req);
    int init_region_ddlwork(DdlWorkMem& ddl_work_mem);

    void check_delete_ddl_region_info(DdlWorkMem& ddlwork);
    
    void init_ddlwork_for_store(const pb::StoreHeartBeatRequest* request,
        pb::StoreHeartBeatResponse* response);
    void put_incremental_schemainfo(const int64_t apply_index, std::vector<pb::SchemaInfo>& schema_infos);

    int get_ddlwork_state(int64_t table_id, pb::IndexState& state, pb::OpType& op_type) {
        BAIDU_SCOPED_LOCK(_table_ddlinfo_mutex);
        auto table_ddlinfo_iter = _table_ddlinfo_map.find(table_id);
        if (table_ddlinfo_iter == _table_ddlinfo_map.end()) {
            DB_DEBUG("table_ddlinfo_map doesn't have table_id[%lld]", table_id);
            return -1;
        }
        state = table_ddlinfo_iter->second.work_info.job_state();
        op_type = table_ddlinfo_iter->second.work_info.op_type(); 
        return 0;
    }

    bool exist_ddlwork_region(int64_t table_id, int64_t region_id) {
        BAIDU_SCOPED_LOCK(_table_ddlinfo_mutex);
        if (_table_ddlinfo_map.count(table_id) == 1) {
            if (_table_ddlinfo_map[table_id].region_ddl_infos.count(region_id) == 1) {
                return true;
            }
        }
        return false;
    }

    bool exist_ddlwork_peer(int64_t table_id, int64_t region_id, const std::string& peer) {
        BAIDU_SCOPED_LOCK(_table_ddlinfo_mutex);
        if (_table_ddlinfo_map.count(table_id) == 1) {
            if (_table_ddlinfo_map[table_id].region_ddl_infos.count(region_id) == 1) {
                if (_table_ddlinfo_map[table_id].region_ddl_infos[region_id].peer_infos.count(peer) == 1) {
                    return true;
                }
            }
        }
        return false;
    }

    void add_ddlwork_region(int64_t table_id, int64_t region_id, const std::string& peer) {
        DdlRegionMem ddl_region_mem;
        ddl_region_mem.region_id = region_id;
        ddl_region_mem.workstate = pb::IS_UNKNOWN;
        ddl_region_mem.peer_infos.emplace(peer, DdlPeerMem{pb::IS_UNKNOWN, peer});
        BAIDU_SCOPED_LOCK(_table_ddlinfo_mutex);
        if (_table_ddlinfo_map.count(table_id) == 1) {
            _table_ddlinfo_map[table_id].region_ddl_infos.emplace(region_id, ddl_region_mem);
            ++_table_ddlinfo_map[table_id].all_peer_num;
        }
    }

    void add_ddlwork_peer(int64_t table_id, int64_t region_id, const std::string& peer) {
        BAIDU_SCOPED_LOCK(_table_ddlinfo_mutex);
        auto table_iter = _table_ddlinfo_map.find(table_id);
        if (table_iter != _table_ddlinfo_map.end()) {
            auto region_iter = table_iter->second.region_ddl_infos.find(region_id);
            if (region_iter != table_iter->second.region_ddl_infos.end()) {
                region_iter->second.peer_infos.emplace(peer, DdlPeerMem{pb::IS_UNKNOWN, peer});   
                ++_table_ddlinfo_map[table_id].all_peer_num;
            }
        }
    }

    int init_ddlwork_region_info(
        DdlRegionMem& region_ddl_info,
        const pb::RegionInfo& region_info,
        pb::IndexState work_state
    ) {
        auto init_peer_num = 0;
        region_ddl_info.region_id = region_info.region_id();
        region_ddl_info.workstate = work_state;
        for (const auto& peer : region_info.peers()) {
            DdlPeerMem ddl_peer_mem;
            ddl_peer_mem.workstate = work_state;
            ddl_peer_mem.peer = peer;
            region_ddl_info.peer_infos.emplace(peer, ddl_peer_mem);
            ++init_peer_num;
            DB_NOTICE("add_ddl_region region[%lld] peer[%s] state[%s]", region_ddl_info.region_id, peer.c_str(),
                        pb::IndexState_Name(work_state).c_str());
        }
        return init_peer_num;    
    }

    void update_ddlwork_peer_state(int64_t table_id, int64_t region_id, const std::string& peer,
        pb::IndexState store_job_state, bool& debug_flag) {
        BAIDU_SCOPED_LOCK(_table_ddlinfo_mutex);

        auto table_iter = _table_ddlinfo_map.find(table_id);
        if (table_iter == _table_ddlinfo_map.end()) {
            DB_WARNING("table_id[%ld] not in ddlinfo_map", table_id);
            return;
        }

        auto region_iter = table_iter->second.region_ddl_infos.find(region_id);
        if (region_iter == table_iter->second.region_ddl_infos.end()) {
            DB_WARNING("table_id[%ld] region_id[%ld] not in ddlinfo_map", table_id, region_id);
            return;
        }
        auto& region_info = region_iter->second;
        auto peer_iter = region_info.peer_infos.find(peer);
        if (peer_iter != region_info.peer_infos.end()) {

            auto& peer_state = peer_iter->second.workstate;
            if (store_job_state != peer_state) {
                table_iter->second.state_count[store_job_state]++;
                peer_state = store_job_state;
                auto all_peer_done = std::all_of(region_info.peer_infos.begin(),
                    region_info.peer_infos.end(), 
                    [store_job_state](typename std::unordered_map<std::string, DdlPeerMem>::const_reference r){
                        return r.second.workstate == store_job_state;
                });
        
                if (all_peer_done && region_info.workstate != store_job_state) {
                    DB_NOTICE("region_%lld all peer state[%s]", region_id, 
                        pb::IndexState_Name(store_job_state).c_str());
                    region_info.workstate = store_job_state;
                }
                //debug
                if (debug_flag && !all_peer_done) {
                    debug_flag = false;
                    for (const auto& peer_ddl_info : region_info.peer_infos) {
                        DB_NOTICE("wait for region[%lld] peer[%s] state[%s]", region_info.region_id,
                            peer_ddl_info.second.peer.c_str(), 
                            pb::IndexState_Name(peer_ddl_info.second.workstate).c_str());
                    }
                }
            }
        }
    }

    int get_pb_ddlwork_info(int64_t table_id, pb::DdlWorkInfo& pb_ddlwork_info) {
        BAIDU_SCOPED_LOCK(_table_ddlinfo_mutex);
        if (_table_ddlinfo_map.count(table_id) == 1) {
            pb_ddlwork_info = _table_ddlinfo_map[table_id].work_info;
            return 0;
        }
        return -1;
    }

    void get_ddlwork_table_ids(std::unordered_set<int64_t>& table_ids) {
        BAIDU_SCOPED_LOCK(_table_ddlinfo_mutex);
        for (const auto& ddlwork : _table_ddlinfo_map) {
            table_ids.insert(ddlwork.first);
        }
    }
private:
    //std::mutex                                          _table_mutex;
    bthread_mutex_t                                          _table_mutex;
    bthread_mutex_t                                          _table_ddlinfo_mutex;
    bthread_mutex_t                                          _all_table_ddlinfo_mutex;
    bthread_mutex_t                                     _log_entry_mutex;
    int64_t                                             _max_table_id;
    //table_name 与op映射关系， name: namespace\001\database\001\table_name
    std::unordered_map<std::string, int64_t>            _table_id_map;
    std::unordered_map<int64_t, TableMem>               _table_info_map;
    std::unordered_map<int64_t, DdlWorkMem> _table_ddlinfo_map;
    std::multimap<int64_t, DdlWorkMem> _all_table_ddlinfo_map;

    typedef std::map<int64_t, std::vector<pb::SchemaInfo>> SchemaIncrementalMap;
    DoubleBuffer<SchemaIncrementalMap> _incremental_schemainfo_map;
    TimeCost _gc_time_cost;
    
}; //class

}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
