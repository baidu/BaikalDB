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
#include "schema_manager.h"
#include "meta_server.h"
#include "table_key.h"

namespace baikaldb {
struct TableMem {
    bool whether_level_table;
    pb::SchemaInfo schema_pb;
    std::unordered_map<int64_t, std::set<int64_t>> partition_regions;//该信息只保存在内存中
    std::unordered_map<std::string, int32_t> field_id_map;
    std::unordered_map<std::string, int64_t> index_id_map;
};

class TableManager {
public:
    ~TableManager() {
        bthread_mutex_destroy(&_table_mutex);
    }
    static TableManager* get_instance()  {
        static TableManager instance;
        return &instance;
    }
    friend class QueryTableManager;
    void update_table_internal(const pb::MetaManagerRequest& request, braft::Closure* done,
    std::function<void(const pb::MetaManagerRequest& request, pb::SchemaInfo& mem_schema_pb)> update_callback);
    void create_table(const pb::MetaManagerRequest& request, braft::Closure* done);
    void drop_table(const pb::MetaManagerRequest& request, braft::Closure* done);
    void rename_table(const pb::MetaManagerRequest& request, braft::Closure* done);
    void update_byte_size(const pb::MetaManagerRequest& request, braft::Closure* done);
    void update_split_lines(const pb::MetaManagerRequest& request, braft::Closure* done);
    void update_dists(const pb::MetaManagerRequest& request, braft::Closure* done);

    void add_field(const pb::MetaManagerRequest& request, braft::Closure* done);
    void drop_field(const pb::MetaManagerRequest& request, braft::Closure* done);
    void rename_field(const pb::MetaManagerRequest& request, braft::Closure* done);
    void modify_field(const pb::MetaManagerRequest& request, braft::Closure* done);

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
    
public:
    void set_max_table_id(int64_t max_table_id) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        _max_table_id = max_table_id;
    }
    int64_t get_max_table_id() {
        BAIDU_SCOPED_LOCK(_table_mutex);
        return _max_table_id;
    }

    void set_table_info(const TableMem& table_mem) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        std::string table_name = table_mem.schema_pb.namespace_name()
                                    + "\001" + table_mem.schema_pb.database()
                                    + "\001" + table_mem.schema_pb.table_name();
        _table_info_map[table_mem.schema_pb.table_id()] = table_mem;
        _table_id_map[table_name] = table_mem.schema_pb.table_id();
    }
    void set_table_pb(const pb::SchemaInfo& schema_pb) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        _table_info_map[schema_pb.table_id()].schema_pb = schema_pb;
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
    void erase_table_info(const std::string& table_name) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        int64_t table_id = _table_id_map[table_name];
        _table_id_map.erase(table_name);
        _table_info_map.erase(table_id);
    }
    void erase_table_info(int64_t table_id) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_info_map.find(table_id) == _table_info_map.end()) {
            return;
        }
        std::string table_name = _table_info_map[table_id].schema_pb.namespace_name()
                                    + "\001" + _table_info_map[table_id].schema_pb.database()
                                    + "\001" + _table_info_map[table_id].schema_pb.table_name();
        _table_id_map.erase(table_name);
        _table_info_map.erase(table_id);
    }
    void swap_table_name(const std::string& old_table_name, const std::string new_table_name) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_id_map.find(old_table_name) != _table_id_map.end()) {
            int64_t table_id = _table_id_map[old_table_name];
            _table_id_map.erase(old_table_name);
            _table_id_map[new_table_name] = table_id;
        }
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
        for (auto& partition_regions : _table_info_map[table_id].partition_regions) {
            for (auto& region_id :  partition_regions.second) {
                query_region_ids.push_back(region_id);    
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
    }
private:
    TableManager(): _max_table_id(0) {
        bthread_mutex_init(&_table_mutex, NULL);
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
    
    int check_table_exist(const pb::SchemaInfo& schema_info,
                            int64_t& table_id) {
        int64_t namespace_id = 0;;
        int64_t database_id = 0;
        return check_table_exist(schema_info, namespace_id, database_id, table_id);
    }
    int alloc_field_id(pb::SchemaInfo& table_info, bool& has_auto_increment, TableMem& table_mem);
    int alloc_index_id(pb::SchemaInfo& table_info, TableMem& table_mem, int64_t& max_table_id_tmp);

    std::string construct_table_key(int64_t table_id) {
        std::string table_key;
        table_key = MetaServer::SCHEMA_IDENTIFY + MetaServer::TABLE_SCHEMA_IDENTIFY;
        table_key.append((char*)&table_id, sizeof(int64_t));
        return table_key;
    }
    
    std::string construct_max_table_id_key() {
        std::string max_table_id_key = MetaServer::SCHEMA_IDENTIFY
                            + MetaServer::MAX_ID_SCHEMA_IDENTIFY
                            + SchemaManager::MAX_TABLE_ID_KEY;
        return max_table_id_key;
    }
private:
    //std::mutex                                          _table_mutex;
    bthread_mutex_t                                          _table_mutex;
    int64_t                                             _max_table_id;
    //table_name 与op映射关系， name: namespace\001\database\001\table_name
    std::unordered_map<std::string, int64_t>            _table_id_map;
    std::unordered_map<int64_t, TableMem>               _table_info_map;

}; //class

}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
