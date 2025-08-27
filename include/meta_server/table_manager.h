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

#include <atomic>
#include <functional>
#include <unordered_set>
#include <google/protobuf/descriptor.pb.h>
#include <set>

#include "schema_manager.h"
#include "meta_server.h"
#include "meta_util.h"
#include "table_key.h"
#include "ddl_manager.h"
#include "mysql_interact.h"
#ifdef BAIDU_INTERNAL
#include <raft/repeated_timer_task.h>
#else
#include <braft/repeated_timer_task.h>
#endif

namespace baikaldb {
DECLARE_int64(table_tombstone_gc_time_s);
DECLARE_int64(store_heart_beat_interval_us);
DECLARE_int32(pre_split_threashold);
DECLARE_int32(baikal_heartbeat_interval_us);

enum MergeStatus {
    MERGE_IDLE   = 0, //空闲
    MERGE_SRC    = 1,  //用于merge源
    MERGE_DST    = 2   //用于merge目标
};
struct RegionDesc {
    int64_t region_id;
    MergeStatus merge_status;
};
struct TableTmpRegion {
    //start_key=>regionid
    std::map<int64_t, std::map<std::string, RegionDesc>>  startkey_regiondesc_map;
    //发生split或merge时，用以下三个map暂存心跳上报的region信息，保证整体更新
    //start_key => region 存放new region，new region为分裂出来的region
    std::map<int64_t, std::map<std::string, SmartRegionInfo>> startkey_newregion_map;
    //region id => none region 存放空region
    std::map<int64_t, SmartRegionInfo> id_noneregion_map;
    //region id => region 存放key发生变化的region，以该region为基准，查找merge或split所涉及到的所有region
    std::map<int64_t, SmartRegionInfo> id_keyregion_map;
    void clear_regions() {
        startkey_regiondesc_map.clear();
        startkey_newregion_map.clear();
        id_noneregion_map.clear();
        id_keyregion_map.clear();
    }
};
struct TableMem {
    pb::SchemaInfo schema_pb;
    // 维护table、partition、region的关系，使用前需先判断partition_id是否存在
    std::unordered_map<int64_t, std::set<int64_t>> partition_regions;
    std::unordered_map<std::string, int32_t> field_id_map;
    bool is_view = false; // 是否是视图
    bool is_global_index = false;
    int64_t global_index_id = 0;
    int64_t main_table_id = 0;
    bool is_partition = false;
    //binlog表使用
    std::set<int64_t> binlog_target_ids;
    //普通表使用
    bool is_linked = false;
    bool is_binlog = false;

    // Partition
    // <partition_id, drop_ts>, 存放分区删除的时间点
    std::map<int64_t, int64_t> drop_partition_ts_map;

    // 业务表linked的binlog表id集合
    std::set<int64_t> binlog_ids;
    bool exist_global_index(int64_t global_index_id) const {
        for (auto& index : schema_pb.indexs()) {
            if (index.is_global() && index.index_id() == global_index_id) {
                return true;
            }
        }
        return false;
    }

    void clear_regions() {
        partition_regions.clear();
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

struct TableMemMapping {
    //table_name 与op映射关系， name: namespace\001\database\001\table_name
    std::unordered_map<std::string, int64_t>            table_id_map;
    std::unordered_map<int64_t, TableMem>               table_info_map;
    // table_id => TableMem 
    std::map<int64_t, TableMem>               table_tombstone_map;
};
using DoubleBufferedTableMemMapping = butil::DoublyBufferedData<TableMemMapping>;

struct TableSchedulingInfo {
    // 快速导入
    std::unordered_map<int64_t, std::string> table_in_fast_importer;  // table_id -> resource_tag
    std::unordered_map<int64_t, TimeCost> table_start_fast_import_ts; // table_id -> start_time, 持续太长报警
    // pk prefix balance
    std::unordered_map<int64_t, std::vector<pb::PrimitiveType>> table_pk_types;
    std::unordered_map<int64_t, int32_t> table_pk_prefix_dimension;
    int64_t table_pk_prefix_timestamp;
    // for binlog peer balance
    std::set<int64_t> binlog_table_ids;
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
private:
    std::atomic<bool> _is_last_dynamic_change_done = true;
};

class DBLinkMysqlTableTimer : public braft::RepeatedTimerTask {
public:
    DBLinkMysqlTableTimer() {}
    virtual ~DBLinkMysqlTableTimer() {}
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
        _dblink_mysql_table_timer.stop();
        _dblink_mysql_table_timer.destroy();
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
    int do_create_table_sync_req(pb::SchemaInfo& schema_pb, 
                            std::shared_ptr<std::vector<pb::InitRegion>> init_regions,
                            bool has_auto_increment,
                            int64_t start_region_id,
                            pb::MetaManagerResponse* response);
    int do_create_view_sync_req(pb::SchemaInfo& schema_pb,
                            pb::MetaManagerResponse* response);
    void drop_table(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void drop_table_tombstone(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void create_view(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    int replace_view(const std::string& view_name,
            TableMem& table_mem,
            pb::SchemaInfo& view_info, 
            const int64_t apply_index, 
            braft::Closure* done);
    int create_view_internal(int64_t database_id,
            const std::string& view_name,
            TableMem& table_mem,
            pb::SchemaInfo& view_info, 
            const int64_t apply_index, 
            braft::Closure* done);
    void drop_table_tombstone_gc_check();
    void restore_table(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void rename_table(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void swap_table(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void update_byte_size(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void update_split_lines(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void update_charset(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void add_partition(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void drop_partition(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void modify_partition(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void convert_partition(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void set_main_logical_room(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void update_schema_conf(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void update_statistics(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void update_dists(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void update_ttl_duration(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void update_resource_tag(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void update_table_comment(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void update_dynamic_partition_attr(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void drop_partition_ts(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void specify_split_keys(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);

    void add_field(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    void add_index(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    int do_add_index(const pb::SchemaInfo& mem_schema_pb, const int64_t apply_index, braft::Closure* done,  
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
    void check_add_table(
            const std::set<int64_t>& report_table_ids, 
            std::vector<int64_t>& new_add_region_ids, 
            const pb::BaikalHeartBeatRequest* request,
            pb::BaikalHeartBeatResponse* response, 
            const std::unordered_map<int64_t, std::unordered_set<int64_t>>& heartbeat_table_partition_map,
            const std::unordered_set<int64_t>& heartbeat_binlog_main_table_ids);

    void check_add_region(
            const std::set<std::int64_t>& report_table_ids,
            const pb::BaikalHeartBeatRequest* request,
            pb::BaikalHeartBeatResponse* response,
            const std::unordered_map<int64_t, std::unordered_set<int64_t>>& heartbeat_table_partition_map);
                        
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
                                           std::map<std::string, SmartRegionInfo>& key_newregion_map,
                                           pb::MetaManagerRequest& request);
                                          
    void get_update_region_requests(int64_t table_id, TableTmpRegion& table_info,
                                    std::vector<pb::MetaManagerRequest>& requests);
    void recycle_update_region();
    void get_update_regions_apply_raft();
    void check_update_region(const pb::LeaderHeartBeat& leader_region,
                             const SmartRegionInfo& master_region_info);

    void on_leader_start();
    void on_leader_stop();

    bool is_create_table_support_engine(pb::Engine engine) {
        return (engine == pb::ROCKSDB || engine == pb::ROCKSDB_CSTORE || engine == pb::BINLOG);
    }
   
public:
    void set_max_table_id(int64_t max_table_id) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        _max_table_id = max_table_id;
    }
    int64_t get_max_table_id() {
        BAIDU_SCOPED_LOCK(_table_mutex);
        return _max_table_id;
    }
    //在双buf内修改
    void set_partition_regions(TableMem& table_mem) {
        // 判断逻辑在whether_exist_table_partition函数中，当前仅对range分区表进行判断
        const pb::SchemaInfo& schema_pb = table_mem.schema_pb;
        if (!schema_pb.has_partition_info()) {
            // 非分区表
            const int64_t parititon_id = 0;
            if (table_mem.partition_regions.find(parititon_id) == 
                    table_mem.partition_regions.end()) {
                table_mem.partition_regions[parititon_id] = std::set<int64_t>();
            }
        } else {
            if (schema_pb.partition_info().type() == pb::PT_HASH) {
                // Hash分区表
                for (int i = 0; i < schema_pb.partition_num(); ++i) {
                    const int64_t parititon_id = i;
                    if (table_mem.partition_regions.find(parititon_id) == 
                            table_mem.partition_regions.end()) {
                        table_mem.partition_regions[parititon_id] = std::set<int64_t>();
                    }
                }
            } else if (schema_pb.partition_info().type() == pb::PT_RANGE) {
                // Range分区表
                for (const auto& range_partition_info : schema_pb.partition_info().range_partition_infos()) {
                    const int64_t parititon_id = range_partition_info.partition_id();
                    if (table_mem.partition_regions.find(parititon_id) == 
                            table_mem.partition_regions.end()) {
                        table_mem.partition_regions[parititon_id] = std::set<int64_t>();
                    }
                }
            }  else {
                // do nothing
            }
        }
    }

    void set_table_pb(const pb::SchemaInfo& schema_pb) {
        auto call_func = [schema_pb, this](TableMemMapping& infos) -> int {
            infos.table_info_map[schema_pb.table_id()].schema_pb = schema_pb;
            set_partition_regions(infos.table_info_map[schema_pb.table_id()]);
            infos.table_info_map[schema_pb.table_id()].print();
            for (auto& index_info : schema_pb.indexs()) {
                if (is_global_index(index_info)) {
                    infos.table_info_map[index_info.index_id()].schema_pb = schema_pb;
                    infos.table_info_map[index_info.index_id()].is_global_index = true;
                    infos.table_info_map[index_info.index_id()].main_table_id = schema_pb.table_id();
                    infos.table_info_map[index_info.index_id()].global_index_id = index_info.index_id();
                    set_partition_regions(infos.table_info_map[index_info.index_id()]);
                    infos.table_info_map[index_info.index_id()].print(); 
                }
            }
            return 1;
        };
        _table_mem_infos.Modify(call_func);
    }
    
    int64_t get_table_id(const std::string& table_name) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return 0;
        }
        if (info->table_id_map.find(table_name) != info->table_id_map.end()) {
            return info->table_id_map.at(table_name);
        }
        return 0;
    }
    int32_t get_field_id(int64_t table_id, const std::string& field_name) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return 0;
        }
        auto table_mem_iter = info->table_info_map.find(table_id);
        if (table_mem_iter == info->table_info_map.end()) {
            DB_WARNING("table_id:[%ld] not exist.", table_id);
            return 0;
        }
        auto& field_id_map = table_mem_iter->second.field_id_map;
        auto iter = field_id_map.find(field_name);
        if (iter != field_id_map.end()) {
            return iter->second;
        }
        return 0;
    }
    void add_field_mem(int64_t table_id, 
            std::unordered_map<std::string, int32_t>& add_field_id_map) {
        auto call_func = [&](TableMemMapping& infos) -> int {
            if (infos.table_info_map.find(table_id) == infos.table_info_map.end()) {
                return 0;
            }
            for (auto& add_field : add_field_id_map) {
                infos.table_info_map[table_id].field_id_map[add_field.first] = add_field.second;
            }
            return 1;
        };
        _table_mem_infos.Modify(call_func);
    }
    void drop_field_mem(int64_t table_id, std::vector<std::string>& drop_field_names) {
        auto call_func = [&](TableMemMapping& infos) -> int {
            if (infos.table_info_map.find(table_id) == infos.table_info_map.end()) {
                return 0;
            }
            for (auto& drop_name : drop_field_names) {
                infos.table_info_map[table_id].field_id_map.erase(drop_name);
            }
            return 1;
        };
        _table_mem_infos.Modify(call_func);
    }
    void set_table_info(const TableMem& table_mem) {
        auto call_func = [table_mem, this](TableMemMapping& infos) -> int {
            std::string table_name = table_mem.schema_pb.namespace_name()
                                        + "\001" + table_mem.schema_pb.database()
                                        + "\001" + table_mem.schema_pb.table_name();
            int64_t table_id = table_mem.schema_pb.table_id();
            infos.table_info_map[table_id] = table_mem;
            infos.table_id_map[table_name] = table_id;
            set_partition_regions(infos.table_info_map[table_id]);
            infos.table_info_map[table_id].print();
            if (infos.table_tombstone_map.count(table_id) == 1) {
                infos.table_tombstone_map.erase(table_id);
            }
            //全局二级索引有region信息，所以需要独立为一项
            for (auto& index_info : table_mem.schema_pb.indexs()) {
                if (!is_global_index(index_info)) {
                    continue;
                }
                std::string index_table_name = table_name + "\001" + index_info.index_name();
                infos.table_info_map[index_info.index_id()] = table_mem;
                infos.table_info_map[index_info.index_id()].is_global_index = true;
                infos.table_info_map[index_info.index_id()].main_table_id = table_id;
                infos.table_info_map[index_info.index_id()].global_index_id = index_info.index_id();
                infos.table_id_map[index_table_name] = index_info.index_id();
                set_partition_regions(infos.table_info_map[index_info.index_id()]);
                infos.table_info_map[index_info.index_id()].print();
            }
            return 1;
        };
        _table_mem_infos.Modify(call_func);
    }
    void erase_table_info(int64_t table_id) {
        auto call_func = [table_id, this](TableMemMapping& infos) -> int {
            if (infos.table_info_map.find(table_id) == infos.table_info_map.end()) {
                return 0;
            }
            std::string table_name = infos.table_info_map[table_id].schema_pb.namespace_name()
                                        + "\001" + infos.table_info_map[table_id].schema_pb.database()
                                        + "\001" + infos.table_info_map[table_id].schema_pb.table_name();
            //处理全局二级索引
            for (auto& index_info : infos.table_info_map[table_id].schema_pb.indexs()) {
                if (!is_global_index(index_info)) {
                    continue;
                }
                std::string index_table_name = table_name + "\001" + index_info.index_name();
                infos.table_info_map.erase(index_info.index_id());
                infos.table_id_map.erase(index_table_name);  
            }
            infos.table_tombstone_map[table_id] = infos.table_info_map[table_id];
            infos.table_tombstone_map[table_id].schema_pb.set_deleted(true);
            // tombstone靠这个time来gc
            infos.table_tombstone_map[table_id].schema_pb.set_timestamp(time(NULL));
            // region相关信息清理，只保留表元信息
            infos.table_tombstone_map[table_id].clear_regions();
            infos.table_id_map.erase(table_name);
            infos.table_info_map.erase(table_id);
            return 1;
        };
        _table_mem_infos.Modify(call_func);
        BAIDU_SCOPED_LOCK(_table_mutex);
        _table_tmp_regions.erase(table_id);
    }

    void erase_global_index_info(int64_t index_id) {
        auto call_func = [index_id](TableMemMapping& infos) -> int {
            if (infos.table_info_map.find(index_id) == infos.table_info_map.end()) {
                return 0;
            }
            infos.table_info_map.erase(index_id);
            return 1;
        };
        _table_mem_infos.Modify(call_func);
    }

    int find_last_table_tombstone(const pb::SchemaInfo& table_info, TableMem* table_mem) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return -1;
        }
        const std::string& namespace_name = table_info.namespace_name();
        const std::string& database = table_info.database();
        const std::string& table_name = table_info.table_name();
        for (auto iter = info->table_tombstone_map.rbegin(); iter != info->table_tombstone_map.rend(); iter++) {
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
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return 0;
        }
        auto it = info->table_info_map.find(table_id);
        if (it != info->table_info_map.end()) {
            for (auto& region_it : it->second.partition_regions) {
                ret += region_it.second.size();
            }
        }
        return ret;
    }
    void erase_table_tombstone(int64_t table_id) {
        auto call_func = [table_id](TableMemMapping& infos) -> int {
            if (infos.table_tombstone_map.count(table_id) == 1) {
                infos.table_tombstone_map.erase(table_id);
            }
            return 1;
        };
        _table_mem_infos.Modify(call_func);
    }
    void set_new_table_name(const std::string& old_table_name, const std::string& new_table_name) {
        auto call_func = [&](TableMemMapping& infos) -> int {
            if (infos.table_id_map.find(old_table_name) == infos.table_id_map.end()) {
                return 0;
            }
            int64_t table_id = infos.table_id_map[old_table_name];
            for (auto& index_info : infos.table_info_map[table_id].schema_pb.indexs()) {
                if (!is_global_index(index_info)) {
                    continue;
                }
                std::string old_index_table_name = old_table_name + "\001" + index_info.index_name();
                std::string new_index_table_name = new_table_name + "\001" + index_info.index_name();
                infos.table_id_map.erase(old_index_table_name);
                infos.table_id_map[new_index_table_name] = index_info.index_id();
            }
            infos.table_id_map.erase(old_table_name);
            infos.table_id_map[new_table_name] = table_id;
            return 1;
        };
        _table_mem_infos.Modify(call_func);
    }
    void swap_table_name(const std::string& old_table_name, const std::string& new_table_name) {
        auto call_func = [=](TableMemMapping& infos) -> int {
            if (infos.table_id_map.find(old_table_name) == infos.table_id_map.end()) {
                return 0 ;
            }
            if (infos.table_id_map.find(new_table_name) == infos.table_id_map.end()) {
                return 0;
            }
            int64_t table_id = infos.table_id_map[old_table_name];
            int64_t new_table_id = infos.table_id_map[new_table_name];
            // globalindex名称映射需要先删后加
            for (auto& index_info : infos.table_info_map[table_id].schema_pb.indexs()) {
                if (!is_global_index(index_info)) {
                    continue;
                }
                std::string old_index_table_name = old_table_name + "\001" + index_info.index_name();
                infos.table_id_map.erase(old_index_table_name);
            }
            for (auto& index_info : infos.table_info_map[new_table_id].schema_pb.indexs()) {
                if (!is_global_index(index_info)) {
                    continue;
                }
                std::string new_index_table_name = new_table_name + "\001" + index_info.index_name();
                infos.table_id_map.erase(new_index_table_name);
            }
            for (auto& index_info : infos.table_info_map[table_id].schema_pb.indexs()) {
                if (!is_global_index(index_info)) {
                    continue;
                }
                std::string new_index_table_name = new_table_name + "\001" + index_info.index_name();
                infos.table_id_map[new_index_table_name] = index_info.index_id();
            }
            for (auto& index_info : infos.table_info_map[new_table_id].schema_pb.indexs()) {
                if (!is_global_index(index_info)) {
                    continue;
                }
                std::string old_index_table_name = old_table_name + "\001" + index_info.index_name();
                infos.table_id_map[old_index_table_name] = index_info.index_id();
            }
            infos.table_id_map[old_table_name] = new_table_id;
            infos.table_id_map[new_table_name] = table_id;
            return 1;
        };
        _table_mem_infos.Modify(call_func);
    }
    bool exist_table_id(int64_t table_id) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return false;
        }
        auto iter = info->table_info_map.find(table_id);
        if (iter == info->table_info_map.end()) {
            return false;
        }
        return true;
    }
    int whether_exist_table_partition(int64_t table_id, int64_t partition_id) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return -1;
        }
        auto iter = info->table_info_map.find(table_id);
        if (iter == info->table_info_map.end()) {
            return -1;
        }
        // Hash分区表和非分区表不进行分区是否存在判断
        const pb::SchemaInfo& schema_info = iter->second.schema_pb;
        if (schema_info.has_partition_info() && schema_info.partition_info().type() == pb::PT_RANGE) {
            const auto& partition_regions = iter->second.partition_regions;
            if (partition_regions.find(partition_id) == partition_regions.end()) {
                return -1;
            }
        }
        return 0;
    }
    void add_mem_table_partition(const int64_t table_id, const int64_t partition_id) {
        auto call_func = [table_id, partition_id, this](TableMemMapping& infos) -> int {
            if (infos.table_info_map.find(table_id) == infos.table_info_map.end()) {
                DB_WARNING("table_id: %ld not exist", table_id);
                return 0;
            }
            // 主表
            auto& table_partition_regions = infos.table_info_map[table_id].partition_regions;
            if (table_partition_regions.find(partition_id) == table_partition_regions.end()) {
                table_partition_regions[partition_id] = std::set<int64_t>();
            }
            // 全局索引
            for (auto& index_info : infos.table_info_map[table_id].schema_pb.indexs()) {
                if (!is_global_index(index_info)) {
                    continue;
                }
                auto& index_partition_regions = infos.table_info_map[index_info.index_id()].partition_regions;
                if (index_partition_regions.find(partition_id) == index_partition_regions.end()) {
                    index_partition_regions[partition_id] = std::set<int64_t>();
                }
            }
            return 1;
        };
        _table_mem_infos.Modify(call_func);
    }
    void delete_mem_table_partition(const int64_t table_id, const int64_t partition_id) {
        auto call_func = [table_id, partition_id, this](TableMemMapping& infos) -> int {
            if (infos.table_info_map.find(table_id) == infos.table_info_map.end()) {
                DB_WARNING("table_id: %ld not exist", table_id);
                return 0;
            }
            // 主表
            auto& table_partition_regions = infos.table_info_map[table_id].partition_regions;
            if (table_partition_regions.find(partition_id) != table_partition_regions.end()) {
                table_partition_regions.erase(partition_id);
            }
            // 全局索引
            for (auto& index_info : infos.table_info_map[table_id].schema_pb.indexs()) {
                if (!is_global_index(index_info)) {
                    continue;
                }
                auto& index_partition_regions = infos.table_info_map[index_info.index_id()].partition_regions;
                if (index_partition_regions.find(partition_id) != index_partition_regions.end()) {
                    index_partition_regions.erase(partition_id);
                }
            }
            return 1;
        };
        _table_mem_infos.Modify(call_func);
    }
    void add_region_id(int64_t table_id, int64_t partition_id, int64_t region_id) {
        auto call_func = [table_id, partition_id, region_id](TableMemMapping& infos) -> int {
            if (infos.table_info_map.find(table_id) == infos.table_info_map.end()) {
                DB_WARNING("table_id: %ld not exist", table_id);
                return 0;
            }
            const auto& partition_regions = infos.table_info_map[table_id].partition_regions;
            if (partition_regions.find(partition_id) == partition_regions.end()) {
                DB_WARNING("table_id: %ld, partition_id: %ld not exist", table_id, partition_id);
                return 0;
            }
            infos.table_info_map[table_id].partition_regions[partition_id].insert(region_id);
            infos.table_info_map[table_id].print();
            return 1;
        };
        _table_mem_infos.Modify(call_func);
    }
    void delete_region_ids(const std::vector<int64_t>& table_ids,
                          const std::vector<int64_t>& partition_ids,
                          const std::vector<int64_t>& region_ids) {
        auto call_func = [=](TableMemMapping& infos) -> int {
            if (table_ids.size() != partition_ids.size()
                    || partition_ids.size() != region_ids.size()) {
                 DB_WARNING("input param not legal, "
                            "table_ids_size:%lu partition_ids_size:%lu, region_ids_size:%lu",
                            table_ids.size(), partition_ids.size(), region_ids.size());
                 return 0;
            }
            for (size_t i = 0; i < table_ids.size(); ++i) {
                if (infos.table_info_map.find(table_ids[i]) != infos.table_info_map.end()) {
                    const auto& partition_regions = infos.table_info_map[table_ids[i]].partition_regions;
                    if (partition_regions.find(partition_ids[i]) != partition_regions.end()) {
                        infos.table_info_map[table_ids[i]].partition_regions[partition_ids[i]].erase(region_ids[i]);
                        infos.table_info_map[table_ids[i]].print();
                    }
                }
            }
            return 1;
        };
        _table_mem_infos.Modify(call_func);
    }
    bool table_name_exist(const std::string& table_name) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return false;
        }
        if (info->table_id_map.find(table_name) != info->table_id_map.end()) {
            return true;
        }
        return false;
    }
    int view_name_exist(const std::string& view_name, bool& is_view) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return false;
        }
        if (info->table_id_map.find(view_name) != info->table_id_map.end()) {
            int64_t table_id = info->table_id_map.at(view_name);
            is_view = info->table_info_map.at(table_id).is_view;
            return true;
        }
        return false;
    }
    int get_table_info(const std::string& table_name, pb::SchemaInfo& table_info) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return -1;
        }
        if (info->table_id_map.find(table_name) == info->table_id_map.end()) {
            return -1;
        }
        int64_t table_id = info->table_id_map.at(table_name);
        auto iter = info->table_info_map.find(table_id);
        if (iter == info->table_info_map.end()) {
            return -1;
        }
        table_info = iter->second.schema_pb;
        return 0;
    }
    int get_table_info(int64_t table_id, pb::SchemaInfo& table_info) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return -1;
        }
        auto iter = info->table_info_map.find(table_id);
        if (iter == info->table_info_map.end()) {
            return -1;
        }
        table_info = iter->second.schema_pb;
        return 0;
    }
    bool is_global_index(int64_t table_id) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return false;
        }
        auto iter = info->table_info_map.find(table_id);
        if (iter == info->table_info_map.end()) {
            return false;
        }
        return iter->second.is_global_index;
    }
    int get_resource_tag(int64_t table_id, std::string& resource_tag) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return -1;
        }
        auto iter = info->table_info_map.find(table_id);
        if (iter == info->table_info_map.end()) {
            return -1;
        }
        resource_tag = iter->second.schema_pb.resource_tag();
        return 0;
    }

    //if resource_tag is "" return all tables
    void get_table_by_resource_tag(const std::string& resource_tag, std::map<int64_t, std::string>& table_id_name_map) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return;
        }
        for (auto& pair : info->table_info_map) {
            if (pair.second.schema_pb.has_resource_tag() 
                && (pair.second.schema_pb.resource_tag() == resource_tag || resource_tag == "")) {
                std::string name = pair.second.schema_pb.database() + "." + pair.second.schema_pb.table_name();
                table_id_name_map.insert(std::make_pair(pair.first, name));
            }
        }
    }

    //if resource_tag is "" return all tables
    void get_table_by_learner_resource_tag(const std::string& resource_tag, std::map<int64_t, std::string>& table_id_name_map) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return;
        }
        for (auto& pair : info->table_info_map) {
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

    // table_replica_dists_maps: table_id -> 表副本分布{resource_tag:logical_room:phyiscal_room} -> count
    void get_table_info(const std::set<int64_t> table_ids, 
            std::unordered_map<int64_t, int64_t>& table_replica_nums,
            std::unordered_map<int64_t, std::unordered_map<std::string, int>>& table_replica_dists_maps,
            std::unordered_map<int64_t, std::set<std::string>>& table_learner_resource_tags) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return;
        }
        for (auto& table_id : table_ids) {
            auto iter = info->table_info_map.find(table_id);
            if (iter != info->table_info_map.end()) {
                table_replica_nums[table_id] = iter->second.schema_pb.replica_num();
                for (auto& learner_resource : iter->second.schema_pb.learner_resource_tags()) {
                    table_learner_resource_tags[table_id].insert(learner_resource);
                }
                if (iter->second.schema_pb.dists_size() > 0) {
                    for (const auto& idc : iter->second.schema_pb.dists()) {
                        std::string key = idc.resource_tag();
                        if (key.empty()) {
                            // 兼容性，dist里resource_tag可能为空
                            key = iter->second.schema_pb.resource_tag();
                        }
                        key += ":" + idc.logical_room() + ":"  + idc.physical_room(); 
                        table_replica_dists_maps[table_id][key] = idc.count();
                    }
                } else {
                    // 没有指定副本分布
                    std::string key = iter->second.schema_pb.resource_tag() + "::"; 
                    table_replica_dists_maps[table_id][key] = iter->second.schema_pb.replica_num();
                }
            }
        }
    }
    int get_main_logical_room(int64_t table_id, IdcInfo& idc) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return -1;
        }
        auto iter = info->table_info_map.find(table_id);
        if (iter == info->table_info_map.end()) {
            return -1;
        }
        idc = {iter->second.schema_pb.resource_tag(), iter->second.schema_pb.main_logical_room(), ""};
        return 0;
    }
    // 获取表副本分布，表副本分布{resource_tag:logical_room:phyiscal_room} -> count
    int64_t get_replica_dist_idcs(int64_t table_id, std::vector<std::string>& idcs, std::vector<int>& replices) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return -1;
        }
        auto iter = info->table_info_map.find(table_id);
        if (iter == info->table_info_map.end()) {
            return -1;
        }
        const std::string& table_resource_tag = iter->second.schema_pb.resource_tag();
        if (iter->second.schema_pb.dists_size() > 0) {
            for (const auto& idc : iter->second.schema_pb.dists()) {
                if (idc.resource_tag() != "" && idc.resource_tag() != table_resource_tag) {
                    continue;
                }
                idcs.emplace_back(table_resource_tag + ":" + idc.logical_room() + ":" + idc.physical_room());
                replices.emplace_back(idc.count());
            }
            for (const auto& idc : iter->second.schema_pb.dists()) {
                if (idc.resource_tag() != "" && idc.resource_tag() != table_resource_tag) {
                    idcs.emplace_back(idc.resource_tag() + ":" + idc.logical_room() + ":" + idc.physical_room());
                    replices.emplace_back(idc.count());
                }
            }
        } else {
            // 没指定副本分布
            idcs.emplace_back(table_resource_tag + "::");
            replices.emplace_back(iter->second.schema_pb.replica_num());
        }
        return 0;
    }
    // 获取instance在table dists所属的调度粒度, peer balance/migreat/dead/split用
    int get_table_dist_belonged(int64_t table_id, const IdcInfo& instance_idc, IdcInfo& balance_idc) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return -1;
        }
        auto iter = info->table_info_map.find(table_id);
        if (iter == info->table_info_map.end()) {
            return -1;
        }
        if (iter->second.schema_pb.dists_size() == 0) {
            if (instance_idc.resource_tag != iter->second.schema_pb.resource_tag()) {
                return -1;
            }
            balance_idc = {iter->second.schema_pb.resource_tag(), "", ""};
            return 0;
        }
        for (const auto& dist : iter->second.schema_pb.dists()) {
            IdcInfo dist_idc(dist.resource_tag(), dist.logical_room(), dist.physical_room());
            if (dist_idc.resource_tag.empty()) {
                dist_idc.resource_tag = iter->second.schema_pb.resource_tag();
            }
            if (instance_idc.match(dist_idc)) {
                balance_idc = dist_idc;
                return 0;
            }
        }
        return -1;
    }

    bool whether_replica_dists(int64_t table_id) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return false;
        }
        auto iter = info->table_info_map.find(table_id);
        if (iter == info->table_info_map.end()) {
            return false;
        }
        if (iter->second.schema_pb.dists_size() > 0) {
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
    int get_binlog_table_ids(std::set<int64_t>& binlog_table_ids) {
        DoubleBufferedTableSchedulingInfo::ScopedPtr info;
        if (_table_scheduling_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return -1;
        }
        binlog_table_ids = info->binlog_table_ids;
        return 0;
    }
    bool cancel_in_fast_importer(const int64_t& table_id) {
        auto call_func = [table_id](TableSchedulingInfo& infos) -> int {
            infos.table_in_fast_importer.erase(table_id);
            infos.table_start_fast_import_ts.erase(table_id);
            return 1;
        };
        _table_scheduling_infos.Modify(call_func);
        return true;
    }
    bool is_table_in_fast_importer(const int64_t table_id) {
        DoubleBufferedTableSchedulingInfo::ScopedPtr info;
        if (_table_scheduling_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return false;
        }
        if (info->table_in_fast_importer.find(table_id) == info->table_in_fast_importer.end()) {
            return false;
        }
        return true;
    }
    void get_table_fast_importer_ts(std::unordered_map<int64_t, int64_t>& tables_ts) {
        DoubleBufferedTableSchedulingInfo::ScopedPtr info;
        if (_table_scheduling_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return;
        }
        for (auto& pair : info->table_start_fast_import_ts) {
            tables_ts[pair.first] = pair.second.get_time();
        }
        return;
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
                    TimeCost now;
                    infos.table_start_fast_import_ts[table_id] = now;
                }
            } else {
                if (infos.table_in_fast_importer.find(table_id) != infos.table_in_fast_importer.end()) {
                    if (tables_cnt == 1) {
                        ClusterManager::get_instance()->update_instance_param(request, nullptr);
                    }
                    infos.table_in_fast_importer.erase(table_id);
                    infos.table_start_fast_import_ts.erase(table_id);
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
            DoubleBufferedTableMemMapping::ScopedPtr info;
            if (_table_mem_infos.Read(&info) != 0) {
                DB_WARNING("read double_buffer_table error.");
                return false;
            }
            auto iter = info->table_info_map.find(table_id);
            if (iter == info->table_info_map.end()) {
                return false;
            }
            auto& schema_info = iter->second.schema_pb;
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
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return 0;
        }
        auto iter = info->table_info_map.find(table_id);
        if (iter == info->table_info_map.end()) {
            return 0;
        }
        int64_t count = 0;
        for (auto& partition_region : iter->second.partition_regions) {
            count += partition_region.second.size();
        }
        return count;
    }
    void get_region_count(const std::set<std::int64_t>& table_ids,
                        std::unordered_map<int64_t, int64_t>& table_region_count) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return ;
        }
        for (auto& table_info : info->table_info_map) {
            int64_t table_id = table_info.first;
            int64_t count = 0;
            for (auto& partition_region : table_info.second.partition_regions) {
                count += partition_region.second.size(); 
            }
            table_region_count[table_id] = count;
        }    
    }
    int get_replica_num(int64_t table_id, int64_t& replica_num) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return -1;
        }
        auto iter = info->table_info_map.find(table_id);
        if (iter == info->table_info_map.end()) {
            return -1;
        }
        replica_num = iter->second.schema_pb.replica_num();
        return 0;
    }

    void get_region_ids(const std::string& full_table_name, std::vector<int64_t>& query_region_ids) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return;
        }
        if (info->table_id_map.find(full_table_name) == info->table_id_map.end()) {
            return;
        }
        int64_t table_id = info->table_id_map.at(full_table_name);
        auto iter = info->table_info_map.find(table_id);
        if (iter == info->table_info_map.end()) {
            return;
        }
        std::set<int64_t> global_indexs;
        global_indexs.insert(table_id);
        for (auto& index_info : iter->second.schema_pb.indexs()) {
            if (is_global_index(index_info)) {
                global_indexs.insert(index_info.index_id());
            }
        }
        for (auto& index_id: global_indexs) {
            auto iter = info->table_info_map.find(index_id);
            if (iter == info->table_info_map.end()) {
                continue;
            }
            for (auto& partition_regions : iter->second.partition_regions) {
                for (auto& region_id :  partition_regions.second) {
                    query_region_ids.push_back(region_id);    
                }
            }
        }
    }
    void get_region_ids(int64_t table_id, std::vector<int64_t>& region_ids) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return;
        }
        auto iter = info->table_info_map.find(table_id);
        if (iter == info->table_info_map.end()) {
            return;
        }
        for (auto& partition_regions : iter->second.partition_regions) {
            for (auto& region_id :  partition_regions.second) {
                region_ids.push_back(region_id);    
            }
        }
    }
    void get_partition_regions(int64_t table_id, std::unordered_map<int64_t, std::set<int64_t>>& partition_regions) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return;
        }
        auto iter = info->table_info_map.find(table_id);
        if (iter == info->table_info_map.end()) {
            return;
        }
        partition_regions = iter->second.partition_regions;
    }
    int64_t get_row_count(int64_t table_id);

    void clear() {
        auto call_func2 = [](TableMemMapping& infos) -> int {
            infos.table_id_map.clear();
            infos.table_info_map.clear();
            return 1;
        };
        _table_mem_infos.Modify(call_func2);
        _incremental_schemainfo.clear();
        _virtual_index_sql_map.clear();
        {
           BAIDU_SCOPED_LOCK(_load_virtual_to_memory_mutex);
           _just_add_virtual_index_info.clear();
           _virtual_index_sql_map.clear();
        }
        {
            BAIDU_SCOPED_LOCK(_table_mutex);
            _table_tmp_regions.clear();
        }
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

    void link_binlog_table(int64_t table_id, int64_t binlog_table_id) {
        auto call_func = [=](TableMemMapping& infos) -> int {
            infos.table_info_map[table_id].binlog_ids.insert(binlog_table_id);
            infos.table_info_map[table_id].is_linked = true;
            infos.table_info_map[binlog_table_id].binlog_target_ids.insert(table_id);
            return 1;
        };
        _table_mem_infos.Modify(call_func);
    }
    void unlink_binlog_table(int64_t table_id, int64_t binlog_table_id) {
        auto call_func = [=](TableMemMapping& infos) -> int {
            infos.table_info_map[table_id].binlog_ids.erase(binlog_table_id);
            if (infos.table_info_map[table_id].binlog_ids.empty()) {
                infos.table_info_map[table_id].is_linked = false;
            }
            infos.table_info_map[binlog_table_id].binlog_target_ids.erase(table_id);
            return 1;
        };
        _table_mem_infos.Modify(call_func);
    }

    bool check_table_is_linked(int64_t table_id) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return false;
        }
        auto table_iter = info->table_info_map.find(table_id);
        if (table_iter == info->table_info_map.end()) {
            return false;
        }
        return table_iter->second.is_linked || table_iter->second.binlog_target_ids.size() > 0;
    }

    bool check_field_is_linked(int64_t table_id, int32_t field_id) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return false;
        }
        auto table_iter = info->table_info_map.find(table_id);
        if (table_iter == info->table_info_map.end()) {
            return false;
        }
        if (table_iter->second.is_linked) {
            if (table_iter->second.schema_pb.has_link_field()) {
                if (table_iter->second.schema_pb.link_field().field_id() == field_id) {
                    return true;
                }
            }
            if (table_iter->second.schema_pb.binlog_infos_size() > 0) {
                for (int i = 0; i < table_iter->second.schema_pb.binlog_infos_size();i++) {
                    if (table_iter->second.schema_pb.binlog_infos(i).link_field().field_id() == field_id) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    bool check_rollup_field_all_in_pk(int64_t table_id, const pb::IndexInfo& rollup_index_info) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return false;
        }
        auto table_iter = info->table_info_map.find(table_id);
        if (table_iter == info->table_info_map.end()) {
            DB_WARNING("table_id: %lu not in table_info_map", table_id);
            return false;
        }
        std::unordered_set<std::string> pk_field_name_set;
        for (const pb::IndexInfo& index : table_iter->second.schema_pb.indexs()) {
            if (index.index_type() == pb::I_PRIMARY) {
                for (const std::string&  pk_field_name : index.field_names()) {
                    pk_field_name_set.insert(pk_field_name);
                }
            }
        }
        for (const pb::FieldInfo& f : table_iter->second.schema_pb.fields()) {
            if (f.is_unique_indicator()) {
                pk_field_name_set.insert(f.field_name());
            }
        }
        for (const std::string& rollup_field_name : rollup_index_info.field_names()) {
            if (pk_field_name_set.count(rollup_field_name) == 0) {
                return false;
            }
        }
        return true;
    }
    // 是否是天级表
    bool check_daily_partition(int64_t table_id) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return false;
        }
        auto table_iter = info->table_info_map.find(table_id);
        if (table_iter == info->table_info_map.end()) {
            DB_WARNING("table_id: %ld not in table_info_map", table_id);
            return false;
        }
        if (table_iter->second.schema_pb.has_partition_info()
            && table_iter->second.schema_pb.partition_info().has_dynamic_partition_attr()
            && boost::algorithm::iequals(table_iter->second.schema_pb.partition_info().dynamic_partition_attr().time_unit(), "DAY")) {
                return true;
        }
        return false;
    }
    bool check_rollup_field_has_snapshot(int64_t table_id, const pb::IndexInfo& rollup_index_info) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return false;
        }
        auto table_iter = info->table_info_map.find(table_id);
        if (table_iter == info->table_info_map.end()) {
            DB_WARNING("table_id: %lu not in table_info_map", table_id);
            return false;
        }
        bool has_snapshot = false;
        for (const pb::FieldInfo& f : table_iter->second.schema_pb.fields()) {
            if (f.field_name() == "__snapshot__") {
                has_snapshot = true;
            }
        }
        if (!has_snapshot) {
            return true;
        }
        for (const std::string& rollup_field_name : rollup_index_info.field_names()) {
            if (rollup_field_name == "__snapshot__") {
                return true;
            }
        }
        return false;
    }
    bool check_field_is_compatible_type(const pb::FieldInfo& src_field, const pb::FieldInfo& target_field) {
        auto src_type = src_field.mysql_type();
        auto target_type = target_field.mysql_type();
        if (src_type == target_type) {
            if (src_type == pb::DATETIME) {
                if (src_field.float_precision_len() > target_field.float_precision_len()) {
                    return false;
                }
            }
            return true;
        }
        switch (src_type) {
            case pb::DATETIME:
            case pb::TIMESTAMP:
            case pb::DATE:
            case pb::TIME:
            case pb::HLL:
            case pb::BOOL:
            case pb::TDIGEST:
            case pb::NULL_TYPE:
            case pb::BITMAP:
                return false;
            default:
                break;
        }
        int s = primitive_to_proto_type(src_type);
        int t = primitive_to_proto_type(target_type);
        if (s != t) return false;
        if (primitive_type_bytes_len(src_type) <= primitive_type_bytes_len(target_type)) {
            return true;
        }
        // if (s == FieldDescriptorProto::TYPE_SINT32 && t == FieldDescriptorProto::TYPE_SINT64) return true;
        // if (s == FieldDescriptorProto::TYPE_SINT64 && t == FieldDescriptorProto::TYPE_SINT32) return true;
        // if (s == FieldDescriptorProto::TYPE_UINT32 && t == FieldDescriptorProto::TYPE_UINT64) return true;
        // if (s == FieldDescriptorProto::TYPE_UINT64 && t == FieldDescriptorProto::TYPE_UINT32) return true;
        return false;
    }
    int get_index_state(int64_t table_id, int64_t index_id, pb::IndexState& index_state) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return -1;
        }
        auto iter = info->table_info_map.find(table_id);
        if (iter == info->table_info_map.end()) {
            return -1;
        }
        auto& table_info = iter->second.schema_pb;
        for (const auto& index_info : table_info.indexs()) {
            if (index_info.index_id() == index_id) {
                index_state = index_info.state();
                return 0;
            }
        }
        return -1; 
    }
    bool check_and_update_incremental(
            const pb::BaikalHeartBeatRequest* request, pb::BaikalHeartBeatResponse* response, int64_t applied_index, 
            const std::unordered_map<int64_t, std::unordered_set<int64_t>>& heartbeat_table_partition_map,
            const std::unordered_set<int64_t>& heartbeat_binlog_main_table_ids);

    void update_index_status(const pb::DdlWorkInfo& ddl_work);

    void remove_global_index_data(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);

    void drop_index_request(const pb::DdlWorkInfo& ddl_work);

    void get_delay_delete_index(std::vector<pb::SchemaInfo>& index_to_delete, std::vector<pb::SchemaInfo>& index_to_clear) {
        auto current_time = butil::gettimeofday_us();
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return;
        }
        for (const auto& table_info : info->table_info_map) {
            if (table_info.second.is_global_index) {
                continue;
            }
            if (check_table_has_ddlwork(table_info.first)) {
                continue;
            }
            const auto& schema_pb = table_info.second.schema_pb; 
            for (const auto& index : schema_pb.indexs()) {
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

    // Dynamic Partition
    // 获取需要分区变更的表
    void get_change_partition_schemas(std::vector<pb::SchemaInfo>& add_partition_schemas, 
                                      std::vector<pb::SchemaInfo>& del_partition_schemas,
                                      std::vector<pb::SchemaInfo>& cold_partition_schemas);
    void get_change_partition_schema(const pb::SchemaInfo& schema, 
                                     pb::SchemaInfo& add_partition_schema, 
                                     pb::SchemaInfo& del_partition_schema,
                                     pb::SchemaInfo& cold_partition_schema);
    // 判断副本所在分区是否存在
    void check_partition_exist_for_peer(
            const pb::StoreHeartBeatRequest* request, pb::StoreHeartBeatResponse* response);

    // DBLink Mysql
    // 获取DBLink Mysql的表
    void get_dblink_mysql_schemas(std::vector<pb::SchemaInfo>& schemas);
    // 获取DBLink Mysql的表字段
    int get_fields_from_mysql(const pb::MysqlInfo& mysql_info, std::vector<pb::FieldInfo>& fields);

    // 获取主表id
    void get_table_ids(std::set<int64_t>& table_ids) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return;
        }
        for (const auto& table_info : info->table_info_map) {
            if (!table_info.second.is_global_index) {
                table_ids.insert(table_info.first);
            }
        }
    }

    // 获取指定表分区的region_ids
    void get_region_ids(const int64_t table_id, const std::unordered_set<int64_t>& partition_ids, std::set<int64_t>& region_ids) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return;
        }
        auto iter = info->table_info_map.find(table_id);
        if (iter == info->table_info_map.end()) {
            return;
        }
        region_ids.clear();
        const auto& partition_regions = iter->second.partition_regions;
        for (const auto& partition_id : partition_ids) {
            if (partition_regions.find(partition_id) != partition_regions.end()) {
                region_ids.insert(iter->second.partition_regions.at(partition_id).begin(),
                                  iter->second.partition_regions.at(partition_id).end());
            }
        }
    }
    
    // 获取指定表分区的副本分布
    void get_partition_info(
            const int64_t table_id, const int64_t partition_id, 
            int64_t& replica_num, std::string& resource_tag) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return;
        }
        auto iter = info->table_info_map.find(table_id);
        if (iter == info->table_info_map.end()) {
            return;
        }
        const auto& schema_pb = iter->second.schema_pb;
        if (!schema_pb.has_partition_info() || schema_pb.partition_info().type() != pb::PT_RANGE) {
            return;
        }
        // 分区副本数量直接使用表副本数量
        replica_num = schema_pb.replica_num();
        for (const auto& range_partition_info : schema_pb.partition_info().range_partition_infos()) {
            if (range_partition_info.partition_id() == partition_id && range_partition_info.has_resource_tag()) {
                resource_tag = range_partition_info.resource_tag() + "::";
                break;
            }
        }
    }

    // 获取partition的对应的时间
    void get_partition_range_info(
        const int64_t table_id, const int64_t partition_id,
        pb::PartitionRange& partiton_range) {
        DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return;
        }
        auto iter = info->table_info_map.find(table_id);
        if (iter == info->table_info_map.end()) {
            return;
        }
        const auto& schema_pb = iter->second.schema_pb;
        if (!schema_pb.has_partition_info() || schema_pb.partition_info().type() != pb::PT_RANGE) {
            return;
        }
        // 分区副本数量直接使用表副本数量
        for (const auto& range_partition_info : schema_pb.partition_info().range_partition_infos()) {
            if (range_partition_info.partition_id() == partition_id && range_partition_info.has_range()) {
                partiton_range.CopyFrom(range_partition_info.range());
                break;
            }
        }
    }

    int send_init_regions_request(const std::string& namespace_name,
                                  const std::string& database,
                                  const std::string& table_name,
                                  std::shared_ptr<std::vector<pb::InitRegion>> init_regions,
                                  bool is_partition = false,
                                  const pb::MetaManagerRequest& drop_request = pb::MetaManagerRequest());

    void put_incremental_schemainfo(const int64_t apply_index, std::vector<pb::SchemaInfo>& schema_infos);

private:
    TableManager(): _max_table_id(0) {
        bthread_mutex_init(&_table_mutex, NULL);
        bthread_mutex_init(&_load_virtual_to_memory_mutex, NULL);
        _table_timer.init(3600 * 1000); // 1h
        _dblink_mysql_table_timer.init(FLAGS_baikal_heartbeat_interval_us / 1000);
    }
    int write_schema_for_not_level(TableMem& table_mem,
                                    braft::Closure* done,
                                    int64_t max_table_id_tmp,
                                     bool has_auto_increment);

    int send_auto_increment_request(const pb::MetaManagerRequest& request);
    int update_schema_for_rocksdb(int64_t table_id, 
                                    const pb::SchemaInfo& schema_info, 
                                    braft::Closure* done);
    int update_statistics_for_rocksdb(int64_t table_id, 
                                    const pb::Statistics& stat_info, 
                                    braft::Closure* done);
    
    void send_drop_table_request(const std::string& namespace_name,
                                const std::string& database,
                                const std::string& table_name);
    void send_drop_view_request(const std::string& namespace_name,
                                const std::string& database,
                                const std::string& view_name);
    void send_link_binlog_request(const pb::SchemaInfo& schema_info, 
                                  const google::protobuf::RepeatedPtrField<pb::BinlogInfo>& binlog_infos);
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

    int init_global_index_region(const pb::SchemaInfo& schema_info, int64_t table_id, braft::Closure* done, pb::IndexInfo& index_info);

    bool partition_check_region_when_update(int64_t table_id, 
        std::string min_start_key, 
        std::string max_end_key, std::map<std::string, RegionDesc>& partition_region_map);
    //虚拟索引影响面信息更新至TableManager管理的内存中
    void load_virtual_indextosqls_to_memory(const pb::BaikalHeartBeatRequest* request);
    void drop_virtual_index(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done);
    VirtualIndexInfo get_virtual_index_id_set();

    // Dynamic Partition
    // 获取分区的预分裂SplitKey
    int get_partition_split_key(
        const pb::SchemaInfo& table_info, 
        const pb::RangePartitionInfo& range_partition_info, 
        const time_t normalized_current_ts, 
        const int offset,
        TimeUnit unit,
        ::google::protobuf::RepeatedPtrField<pb::SplitKey>& split_key);

    // 从SchemaInfo移除指定分区
    int remove_partitions(pb::SchemaInfo& table_info, const std::set<int64_t>& partition_ids);

    // 设置分区删除时间
    void set_drop_partition_timestamp(const int64_t table_id, const int64_t partition_id, const int64_t drop_ts) {
        auto call_func = [=](TableMemMapping& infos) -> int {
            if (infos.table_info_map.find(table_id) == infos.table_info_map.end()) {
                return 0;
            }
            auto& drop_partition_ts_map = infos.table_info_map[table_id].drop_partition_ts_map;
            drop_partition_ts_map[partition_id] = drop_ts;
            return 1;
        };
        _table_mem_infos.Modify(call_func);
    }

    // 判断分区表的partition_id对应分区是否存在
    bool is_range_partition_exist(const pb::SchemaInfo& schema_pb, const int64_t partition_id) {
        bool is_partition_exist = false;
        for (const auto& range_partition_info : schema_pb.partition_info().range_partition_infos()) {
            if (range_partition_info.partition_id() == partition_id) {
                is_partition_exist = true;
                break;
            }
        }
        return is_partition_exist;
    }

    void get_main_logical_room(const pb::SchemaInfo& table_info, std::string& main_logical_room);

    void drop_partition_internal(pb::SchemaInfo& mem_schema_pb, 
                                 const std::vector<std::string>& range_partition_names_vec, 
                                 const bool is_dynamic_change,
                                 const int64_t apply_index, 
                                 braft::Closure* done);

    void drop_privilege_for_table(const pb::MetaManagerRequest& request, const int64_t table_id);

    bool check_vector_index(const pb::SchemaInfo& mem_schema_pb, const pb::IndexInfo& index_info);

private:
    bthread_mutex_t                                     _table_mutex;
    bthread_mutex_t                                     _load_virtual_to_memory_mutex;
    int64_t                                             _max_table_id;
    VirtualIndexInfo _virtual_index_sql_map;//虚拟索引和sql的影响相互对应情况
    DoubleBufferedTableMemMapping      _table_mem_infos;
    std::unordered_map<int64_t, TableTmpRegion> _table_tmp_regions;
    std::set<int64_t>                  _need_apply_raft_table_ids;
    //用一个set<std::string>保存刚被删除的虚拟索引记录
    std::set<int64_t>                          _just_add_virtual_index_info;
    IncrementalUpdate<std::vector<pb::SchemaInfo>> _incremental_schemainfo;
    TableTimer _table_timer;
    DBLinkMysqlTableTimer _dblink_mysql_table_timer;

    DoubleBufferedTableSchedulingInfo             _table_scheduling_infos;
}; //class

}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
