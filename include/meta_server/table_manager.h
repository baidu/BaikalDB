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
#include <google/protobuf/descriptor.pb.h>
#include <set>

#include "schema_manager.h"
#include "meta_server.h"
#include "table_key.h"
#include "ddl_manager.h"
#ifdef BAIDU_INTERNAL
#include <raft/repeated_timer_task.h>
#else
#include <braft/repeated_timer_task.h>
#endif

namespace baikaldb {
DECLARE_int64(table_tombstone_gc_time_s);
DECLARE_int64(store_heart_beat_interval_us);

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
    std::map<int64_t, std::map<std::string, RegionDesc>>  startkey_regiondesc_map;
    //发生split或merge时，用以下三个map暂存心跳上报的region信息，保证整体更新
    //start_key => region 存放new region，new region为分裂出来的region
    std::map<int64_t, std::map<std::string, SmartRegionInfo>> startkey_newregion_map;
    //region id => none region 存放空region
    std::map<int64_t, SmartRegionInfo> id_noneregion_map;
    //region id => region 存放key发生变化的region，以该region为基准，查找merge或split所涉及到的所有region
    std::map<int64_t, SmartRegionInfo> id_keyregion_map;
    bool is_global_index = false;
    int64_t global_index_id = 0;
    int64_t main_table_id = 0;
    bool is_partition = false;
    //binlog表使用
    std::set<int64_t> binlog_target_ids;
    //普通表使用
    bool is_linked = false;
    bool is_binlog = false;
    std::vector<std::string> learner_resource_tag;
    int64_t binlog_id = 0;
    std::vector<pb::Expr> range_infos;
    bool exist_global_index(int64_t global_index_id) {
        for (auto& index : schema_pb.indexs()) {
            if (index.is_global() && index.index_id() == global_index_id) {
                return true;
            }
        }
        return false;
    }

    void clear_regions() {
        partition_regions.clear();
        startkey_regiondesc_map.clear();
        startkey_newregion_map.clear();
        id_noneregion_map.clear();
        id_keyregion_map.clear();
    }
    int64_t statistics_version = 0;
    void print() {
        //DB_WARNING("whether_level_table: %d, schema_pb: %s, is_global_index: %d, main_table_id:%ld, global_index_id: %ld",
        //            whether_level_table, schema_pb.ShortDebugString().c_str(), is_global_index,  main_table_id, global_index_id);
        //for (auto& partition_region : partition_regions) {
        //    for (auto& region : partition_region.second) {
        //        DB_WARNING("table_id: %ld region_id: %ld", global_index_id, region);
        //    }
        //}
    }
};

struct TableSchedulingInfo {
    // 快速导入
    std::unordered_map<int64_t, std::string> table_in_fast_importer; // table_id -> resource_tag
    // pk prefix balance
    std::unordered_map<int64_t, std::vector<pb::PrimitiveType>> table_pk_types;
    std::unordered_map<int64_t, int32_t> table_pk_prefix_dimension;
    int64_t table_pk_prefix_timestamp;
};
using DoubleBufferedTableSchedulingInfo = butil::DoublyBufferedData<TableSchedulingInfo>;


class TableTimer : public braft::RepeatedTimerTask {
public:
    TableTimer() {}
    virtual ~TableTimer() {}
    int init(int timeout_ms) {
        return RepeatedTimerTask::init(timeout_ms);
    }
    virtual void run();
protected:
    virtual void on_destroy() {}
};

using VirtualIndexInfo = std::unordered_map<std::string, std::set<std::pair<std::string,std::string>>>;

class TableManager {
public:
    ~TableManager() {
        bthread_mutex_destroy(&_table_mutex);
        bthread_mutex_destroy(&_load_virtual_to_memory_mutex);
        _table_timer.stop();
        _table_timer.destroy();
    }
    static TableManager* get_instance()  {
        static TableManager instance;
        return &instance;
    }
    friend class QueryTableManager;
    friend class SchemaManager;
    void update_table_internal(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done,
    std::function<void(const pb::MetaManagerRequest& request, pb::SchemaInfo& mem_schema_pb, braft::Closure* done)> update_callback);
    void create_table(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void drop_table(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void drop_table_tombstone(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void drop_table_tombstone_gc_check();
    void restore_table(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void rename_table(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void swap_table(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void update_byte_size(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void update_split_lines(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void set_main_logical_room(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void update_schema_conf(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void update_statistics(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void update_dists(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void update_ttl_duration(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void update_resource_tag(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void update_table_comment(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);

    void add_field(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void add_index(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    int do_add_index(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done,  
        const int64_t table_id, pb::IndexInfo& index_info);
    void drop_index(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void drop_field(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void rename_field(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void modify_field(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void link_binlog(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void unlink_binlog(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void set_index_hint_status(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void add_learner(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void drop_learner(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);

    void update_index_status(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void delete_ddlwork(const pb::MetaManagerRequest& request, braft::Closure* done);

    void process_schema_heartbeat_for_store(
                std::unordered_map<int64_t, int64_t>& store_table_id_version,
                pb::StoreHeartBeatResponse* response);

    void check_update_statistics(const pb::BaikalOtherHeartBeatRequest* request,
        pb::BaikalOtherHeartBeatResponse* response);
    int get_statistics(const int64_t table_id, pb::Statistics& stat_pb);
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
    int load_statistics_snapshot(const std::string& value);
    int erase_region(int64_t table_id, int64_t region_id, std::string start_key, int64_t partition);
    int64_t get_next_region_id(int64_t table_id, std::string start_key, 
            std::string end_key, int64_t partition);
    int add_startkey_regionid_map(const pb::RegionInfo& region_info);
    bool check_region_when_update(int64_t table_id, std::map<int64_t, std::string>& min_start_key, 
            std::map<int64_t, std::string>& max_end_key);
    int check_startkey_regionid_map();
    void update_startkey_regionid_map_old_pb(int64_t table_id, 
            std::map<int64_t, std::map<std::string, int64_t>>& key_id_map);
    void update_startkey_regionid_map(int64_t table_id, std::map<int64_t, std::string>& min_start_key, 
                                      std::map<int64_t, std::string>& max_end_key, 
                                      std::map<int64_t, std::map<std::string, int64_t>>& key_id_map);
    void partition_update_startkey_regionid_map(int64_t table_id, std::string min_start_key, 
        std::string max_end_key, 
        std::map<std::string, int64_t>& key_id_map,
        std::map<std::string, RegionDesc>& startkey_regiondesc_map);
    int64_t get_pre_regionid(int64_t table_id, const std::string& start_key, int64_t partition);
    int64_t get_startkey_regionid(int64_t table_id, const std::string& start_key, int64_t partition);
    void add_new_region(const pb::RegionInfo& leader_region_info);
    void add_update_region(const pb::RegionInfo& leader_region_info, bool is_none);
    int get_merge_regions(int64_t table_id, 
                          std::string new_start_key, std::string origin_start_key, 
                          std::map<int64_t, std::map<std::string, RegionDesc>>& startkey_regiondesc_map,
                          std::map<int64_t, SmartRegionInfo>& id_noneregion_map,
                          std::vector<SmartRegionInfo>& regions, int64_t partition_id);
    int get_split_regions(int64_t table_id, 
                          std::string new_end_key, std::string origin_end_key, 
                          std::map<std::string, SmartRegionInfo>& key_newregion_map,
                          std::vector<SmartRegionInfo>& regions);
    int get_presplit_regions(int64_t table_id, 
                                           std::map<int64_t, std::map<std::string, SmartRegionInfo>>& key_newregion_map,
                                           pb::MetaManagerRequest& request);
                                          
    void get_update_region_requests(int64_t table_id, TableMem& table_info,
                                    std::vector<pb::MetaManagerRequest>& requests);
    void recycle_update_region();
    void get_update_regions_apply_raft();
    void check_update_region(const pb::LeaderHeartBeat& leader_region,
                             const SmartRegionInfo& master_region_info);

    void on_leader_start();
    void on_leader_stop();
   
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
                _table_info_map[index_info.index_id()].is_global_index = true;
                _table_info_map[index_info.index_id()].main_table_id = schema_pb.table_id();
                _table_info_map[index_info.index_id()].global_index_id = index_info.index_id();
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
        int64_t table_id = table_mem.schema_pb.table_id();
        _table_info_map[table_id] = table_mem;
        _table_id_map[table_name] = table_id;
        _table_info_map[table_id].print();
        if (_table_tombstone_map.count(table_id) == 1) {
            _table_tombstone_map.erase(table_id);
        }
        //全局二级索引有region信息，所以需要独立为一项
        for (auto& index_info : table_mem.schema_pb.indexs()) {
            if (!is_global_index(index_info)) {
                continue;
            }
            std::string index_table_name = table_name + "\001" + index_info.index_name();
            _table_info_map[index_info.index_id()] = table_mem;
            _table_info_map[index_info.index_id()].is_global_index = true;
            _table_info_map[index_info.index_id()].main_table_id = table_id;
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
        _table_tombstone_map[table_id] = _table_info_map[table_id];
        _table_tombstone_map[table_id].schema_pb.set_deleted(true);
        // tombstone靠这个time来gc
        _table_tombstone_map[table_id].schema_pb.set_timestamp(time(NULL));
        // region相关信息清理，只保留表元信息
        _table_tombstone_map[table_id].clear_regions();
        _table_id_map.erase(table_name);
        _table_info_map.erase(table_id);
    }

    int find_last_table_tombstone(const pb::SchemaInfo& table_info, TableMem* table_mem) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        const std::string& namespace_name = table_info.namespace_name();
        const std::string& database = table_info.database();
        const std::string& table_name = table_info.table_name();
        for (auto iter = _table_tombstone_map.rbegin(); iter != _table_tombstone_map.rend(); iter++) {
            auto& schema_pb = iter->second.schema_pb;
            if (schema_pb.namespace_name() == namespace_name && 
                schema_pb.database() == database &&
                schema_pb.table_name() == table_name) {
                *table_mem = iter->second;
                return 0;
            }
        }
        return -1;
    }

    size_t get_region_size(int64_t table_id) {
        size_t ret = 0;
        BAIDU_SCOPED_LOCK(_table_mutex);
        auto it = _table_info_map.find(table_id);
        if (it != _table_info_map.end()) {
            for (auto& region_it : it->second.partition_regions) {
                ret += region_it.second.size();
            }
        }
        return ret;
    }
    void erase_table_tombstone(int64_t table_id) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_tombstone_map.count(table_id) == 1) {
            _table_tombstone_map.erase(table_id);
        }
    }
    void set_new_table_name(const std::string& old_table_name, const std::string& new_table_name) {
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
    void swap_table_name(const std::string& old_table_name, const std::string& new_table_name) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_id_map.find(old_table_name) == _table_id_map.end()) {
            return;
        }
        if (_table_id_map.find(new_table_name) == _table_id_map.end()) {
            return;
        }
        int64_t table_id = _table_id_map[old_table_name];
        int64_t new_table_id = _table_id_map[new_table_name];
        // globalindex名称映射需要先删后加
        for (auto& index_info : _table_info_map[table_id].schema_pb.indexs()) {
            if (!is_global_index(index_info)) {
                continue;
            }
            std::string old_index_table_name = old_table_name + "\001" + index_info.index_name();
            _table_id_map.erase(old_index_table_name);
        }
        for (auto& index_info : _table_info_map[new_table_id].schema_pb.indexs()) {
            if (!is_global_index(index_info)) {
                continue;
            }
            std::string new_index_table_name = new_table_name + "\001" + index_info.index_name();
            _table_id_map.erase(new_index_table_name);
        }
        for (auto& index_info : _table_info_map[table_id].schema_pb.indexs()) {
            if (!is_global_index(index_info)) {
                continue;
            }
            std::string new_index_table_name = new_table_name + "\001" + index_info.index_name();
            _table_id_map[new_index_table_name] = index_info.index_id();
        }
        for (auto& index_info : _table_info_map[new_table_id].schema_pb.indexs()) {
            if (!is_global_index(index_info)) {
                continue;
            }
            std::string old_index_table_name = old_table_name + "\001" + index_info.index_name();
            _table_id_map[old_index_table_name] = index_info.index_id();
        }
        _table_id_map[old_table_name] = new_table_id;
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
                        "table_ids_size:%lu partition_ids_size:%lu, region_ids_size:%lu",
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

    //if resource_tag is "" return all tables
    void get_table_by_resource_tag(const std::string& resource_tag, std::map<int64_t, std::string>& table_id_name_map) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        for (auto& pair : _table_info_map) {
            if (pair.second.schema_pb.has_resource_tag() 
                && (pair.second.schema_pb.resource_tag() == resource_tag || resource_tag == "")) {
                std::string name = pair.second.schema_pb.database() + "." + pair.second.schema_pb.table_name();
                table_id_name_map.insert(std::make_pair(pair.first, name));
            }
        }
    }

    //if resource_tag is "" return all tables
    void get_table_by_learner_resource_tag(const std::string& resource_tag, std::map<int64_t, std::string>& table_id_name_map) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        for (auto& pair : _table_info_map) {
            if (resource_tag == "") {
                std::string name = pair.second.schema_pb.database() + "." + pair.second.schema_pb.table_name();
                table_id_name_map.insert(std::make_pair(pair.first, name));
            } else {
                for (auto& t : pair.second.schema_pb.learner_resource_tags()) {
                    if (t == resource_tag) {
                        std::string name = pair.second.schema_pb.database() + "." + pair.second.schema_pb.table_name();
                        table_id_name_map.insert(std::make_pair(pair.first, name));
                        break;
                    }
                }
            }
        }
    }

    void get_table_info(const std::set<int64_t> table_ids, 
            std::unordered_map<int64_t, int64_t>& table_replica_nums,
            std::unordered_map<int64_t, std::string>& table_resource_tags,
            std::unordered_map<int64_t, std::unordered_map<std::string, int64_t>>& table_replica_dists_maps,
            std::unordered_map<int64_t, std::vector<std::string>>& table_learner_resource_tags) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        for (auto& table_id : table_ids) {
            if (_table_info_map.find(table_id) != _table_info_map.end()) {
                table_replica_nums[table_id] = _table_info_map[table_id].schema_pb.replica_num();
                table_resource_tags[table_id] = _table_info_map[table_id].schema_pb.resource_tag();
                for (auto& learner_resource : _table_info_map[table_id].schema_pb.learner_resource_tags()) {
                    table_learner_resource_tags[table_id].emplace_back(learner_resource);
                }
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
    void get_clusters_in_fast_importer(std::set<std::string>& clusters_in_fast_importer) {
        DoubleBufferedTableSchedulingInfo::ScopedPtr info;
        if (_table_scheduling_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return;
        }
        for (auto& pair : info->table_in_fast_importer) {
            clusters_in_fast_importer.insert(pair.second);
        }
    }
    bool is_cluster_in_fast_importer(const std::string &resource_tag) {
        DoubleBufferedTableSchedulingInfo::ScopedPtr info;
        if (_table_scheduling_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return false;
        }
        for (auto& pair : info->table_in_fast_importer) {
            if (pair.second == resource_tag) {
                return true;
            }
        }
        return false;
    }
    bool update_tables_in_fast_importer(const pb::MetaManagerRequest& request, bool in_fast_importer) {
        auto call_func = [request, in_fast_importer](TableSchedulingInfo& infos) -> int {
            int64_t  table_id = request.table_info().table_id();
            std::string resource_tag = request.table_info().resource_tag();
            int tables_cnt = 0;
            for (auto& pair : infos.table_in_fast_importer) {
                if (pair.second == resource_tag) {
                    tables_cnt++;
                }
            }
            if (in_fast_importer) {
                if (infos.table_in_fast_importer.find(table_id) == infos.table_in_fast_importer.end()) {
                    if (tables_cnt == 0) {
                        ClusterManager::get_instance()->update_instance_param(request, nullptr);
                    }
                    infos.table_in_fast_importer[table_id] = resource_tag;
                }
            } else {
                if (infos.table_in_fast_importer.find(table_id) != infos.table_in_fast_importer.end()) {
                    if (tables_cnt == 1) {
                        ClusterManager::get_instance()->update_instance_param(request, nullptr);
                    }
                    infos.table_in_fast_importer.erase(table_id);
                }
            }
            return 1;
        };
        _table_scheduling_infos.Modify(call_func);
        return true;
    }
    bool update_pk_prefix_balance_timestamp(int64_t table_id, int32_t pk_prefix_dimension) {
        auto call_func = [table_id, pk_prefix_dimension](TableSchedulingInfo& infos) -> int {
            if (pk_prefix_dimension == 0) {
                infos.table_pk_prefix_dimension.erase(table_id);
            } else if (infos.table_pk_prefix_dimension[table_id] != pk_prefix_dimension) {
                // 重置时间
                infos.table_pk_prefix_timestamp = butil::gettimeofday_us();
                infos.table_pk_prefix_dimension[table_id] = pk_prefix_dimension;
            }
            return 1;
        };
        _table_scheduling_infos.Modify(call_func);
        return true;
    }
    bool get_pk_prefix_key(int64_t table_id, int32_t pk_prefix_dimension, const std::string& start_key, std::string& key) {
        {
            DoubleBufferedTableSchedulingInfo::ScopedPtr info;
            if (_table_scheduling_infos.Read(&info) != 0) {
                DB_WARNING("read double_buffer_table error.");
                return false;
            }
            auto iter = info->table_pk_types.find(table_id);
            if (iter != info->table_pk_types.end() && !iter->second.empty()) {
                // 之前解析过表主键，则直接从双buffer获取表主键解析key即可
                TableKey tableKey(start_key, true);
                key = std::to_string(table_id) + "_" +
                      tableKey.decode_start_key_string(iter->second, pk_prefix_dimension);
                return true;
            }
        }
        // 第一次解析key，需要解析表的主键，并加入双buffer
        std::vector<pb::PrimitiveType> pk_types;
        pk_types.reserve(1);
        {
            BAIDU_SCOPED_LOCK(_table_mutex);
            if (_table_info_map.find(table_id) == _table_info_map.end()) {
                return false;
            }
            auto& schema_info = _table_info_map[table_id].schema_pb;
            std::unordered_map<int64_t, pb::PrimitiveType> filed_types;
            for(auto& field : schema_info.fields()) {
                filed_types[field.field_id()] = field.mysql_type();
            }
            for(auto& idx : schema_info.indexs()) {
                if (idx.index_type() != pb::I_PRIMARY) {
                    continue;
                }
                for (auto& id : idx.field_ids()) {
                    if (filed_types.find(id) == filed_types.end()) {
                        DB_WARNING("find PrimitiveType failed, table_id: %ld, field_id: %d", table_id, id);
                        return false;
                    }
                    pk_types.emplace_back(filed_types[id]);
                }
            }
        }
        if (pk_types.empty()) {
            return false;
        }
        TableKey tableKey(start_key, true);
        key = std::to_string(table_id) + "_" + tableKey.decode_start_key_string(pk_types, pk_prefix_dimension);
        auto call_func = [table_id, pk_types](TableSchedulingInfo& infos) -> int {
            infos.table_pk_types[table_id] = pk_types;
            return 1;
        };
        _table_scheduling_infos.Modify(call_func);
        return true;
    }
    void get_pk_prefix_dimensions(std::unordered_map<int64_t, int32_t>& pk_prefix_dimension) {
        DoubleBufferedTableSchedulingInfo::ScopedPtr info;
        if (_table_scheduling_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return;
        }
        pk_prefix_dimension = info->table_pk_prefix_dimension;
    }
    bool can_do_pk_prefix_balance() {
        DoubleBufferedTableSchedulingInfo::ScopedPtr info;
        if (_table_scheduling_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return false;
        }
        return (butil::gettimeofday_us() - info->table_pk_prefix_timestamp) >=
                2LL * FLAGS_balance_periodicity * FLAGS_store_heart_beat_interval_us;
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
        _incremental_schemainfo.clear();
        _virtual_index_sql_map.clear();
        _just_add_virtual_index_info.clear();
        auto call_func = [](TableSchedulingInfo& infos) -> int {
            infos.table_pk_prefix_dimension.clear();
            infos.table_pk_prefix_timestamp = 0;
            return 1;
        };
        _table_scheduling_infos.Modify(call_func);
    }

    int load_ddl_snapshot(const std::string& value);

    bool check_table_has_ddlwork(int64_t table_id) {
        return DDLManager::get_instance()->check_table_has_ddlwork(table_id);
    }

    bool check_table_is_linked(int64_t table_id) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        auto table_iter = _table_info_map.find(table_id);
        if (table_iter == _table_info_map.end()) {
            return false;
        }
        return table_iter->second.is_linked || table_iter->second.binlog_target_ids.size() > 0;
    }

    bool check_filed_is_linked(int64_t table_id, int32_t field_id) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        auto table_iter = _table_info_map.find(table_id);
        if (table_iter == _table_info_map.end()) {
            return false;
        }
        if (table_iter->second.is_linked && table_iter->second.schema_pb.has_link_field()) {
            return  table_iter->second.schema_pb.link_field().field_id() == field_id;
        }
        return false;
    }

    bool check_field_is_compatible_type(pb::PrimitiveType src_type, pb::PrimitiveType target_type) {
        int s = primitive_to_proto_type(src_type);
        int t = primitive_to_proto_type(target_type);
        if (s == t) return true;
        if (s == FieldDescriptorProto::TYPE_SINT32 && t == FieldDescriptorProto::TYPE_SINT64) return true;
        if (s == FieldDescriptorProto::TYPE_SINT64 && t == FieldDescriptorProto::TYPE_SINT32) return true;
        if (s == FieldDescriptorProto::TYPE_UINT32 && t == FieldDescriptorProto::TYPE_UINT64) return true;
        if (s == FieldDescriptorProto::TYPE_UINT64 && t == FieldDescriptorProto::TYPE_UINT32) return true;
        return false;
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

    void update_index_status(const pb::DdlWorkInfo& ddl_work);

    void remove_global_index_data(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);

    void drop_index_request(const pb::DdlWorkInfo& ddl_work);

    void get_delay_delete_index(std::vector<pb::SchemaInfo>& index_to_delete, std::vector<pb::SchemaInfo>& index_to_clear) {
        auto current_time = butil::gettimeofday_us();
        BAIDU_SCOPED_LOCK(_table_mutex);
        for (auto& table_info : _table_info_map) {
            if (table_info.second.is_global_index) {
                continue;
            }
            if (check_table_has_ddlwork(table_info.first)) {
                continue;
            }
            auto& schema_pb = table_info.second.schema_pb; 
            for (auto& index : schema_pb.indexs()) {
                if (index.hint_status() == pb::IHS_DISABLE &&
                    index.state() != pb::IS_DELETE_LOCAL &&
                    index.drop_timestamp() != 0 &&
                    index.drop_timestamp() < current_time) {
                    pb::SchemaInfo delete_schema = schema_pb;
                    delete_schema.clear_indexs();
                    auto index_ptr = delete_schema.add_indexs();
                    index_ptr->CopyFrom(index);
                    index_to_delete.emplace_back(delete_schema);
                    DB_NOTICE("delete index start %s", delete_schema.ShortDebugString().c_str());
                    break;
                } else if (index.hint_status() == pb::IHS_DISABLE &&
                    index.state() == pb::IS_DELETE_LOCAL &&
                    index.drop_timestamp() != 0 &&
                    index.drop_timestamp() < current_time) {
                    pb::SchemaInfo delete_schema = schema_pb;
                    delete_schema.clear_indexs();
                    auto index_ptr = delete_schema.add_indexs();
                    index_ptr->CopyFrom(index);
                    index_to_clear.emplace_back(delete_schema);
                    DB_NOTICE("clear local index start %s", delete_schema.ShortDebugString().c_str());
                    break;
                }
            }
        }
    }
    int check_table_exist(const pb::SchemaInfo& schema_info,
                            int64_t& table_id) {
        int64_t namespace_id = 0;
        int64_t database_id = 0;
        return check_table_exist(schema_info, namespace_id, database_id, table_id);
    }
    
private:
    TableManager(): _max_table_id(0) {
        bthread_mutex_init(&_table_mutex, NULL);
        bthread_mutex_init(&_load_virtual_to_memory_mutex, NULL);
        _table_timer.init(FLAGS_table_tombstone_gc_time_s * 1000);
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
    int update_statistics_for_rocksdb(int64_t table_id, 
                                    const pb::Statistics& stat_info, 
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

    std::string construct_statistics_key(int64_t table_id) {
        std::string table_key;
        table_key = MetaServer::SCHEMA_IDENTIFY + MetaServer::STATISTICS_IDENTIFY;
        table_key.append((char*)&table_id, sizeof(int64_t));
        return table_key;
    }

    std::string construct_max_table_id_key() {
        std::string max_table_id_key = MetaServer::SCHEMA_IDENTIFY
                            + MetaServer::MAX_ID_SCHEMA_IDENTIFY
                            + SchemaManager::MAX_TABLE_ID_KEY;
        return max_table_id_key;
    }

    bool is_global_index(const pb::IndexInfo& index_info) {
        return index_info.is_global() == true && 
            (index_info.index_type() == pb::I_UNIQ || index_info.index_type() == pb::I_KEY);
    }

    int init_global_index_region(TableMem& table_mem, braft::Closure* done, pb::IndexInfo& index_info);

    void put_incremental_schemainfo(const int64_t apply_index, std::vector<pb::SchemaInfo>& schema_infos);

    bool partition_check_region_when_update(int64_t table_id, 
        std::string min_start_key, 
        std::string max_end_key, std::map<std::string, RegionDesc>& partition_region_map);
    //虚拟索引影响面信息更新至TableManager管理的内存中
    void load_virtual_indextosqls_to_memory(const pb::BaikalHeartBeatRequest* request);
    void drop_virtual_index(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    VirtualIndexInfo get_virtual_index_id_set();
private:
    bthread_mutex_t                                     _table_mutex;
    bthread_mutex_t                                     _load_virtual_to_memory_mutex;
    int64_t                                             _max_table_id;
    VirtualIndexInfo _virtual_index_sql_map;//虚拟索引和sql的影响相互对应情况
    //table_name 与op映射关系， name: namespace\001\database\001\table_name
    std::unordered_map<std::string, int64_t>            _table_id_map;
    std::unordered_map<int64_t, TableMem>               _table_info_map;
    // table_id => TableMem 
    std::map<int64_t, TableMem>               _table_tombstone_map;
    std::set<int64_t>                  _need_apply_raft_table_ids;
    //用一个set<std::string>保存刚被删除的虚拟索引记录
    std::set<int64_t>                          _just_add_virtual_index_info;
    IncrementalUpdate<std::vector<pb::SchemaInfo>> _incremental_schemainfo;
    TableTimer _table_timer;

    DoubleBufferedTableSchedulingInfo             _table_scheduling_infos;
}; //class

}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
