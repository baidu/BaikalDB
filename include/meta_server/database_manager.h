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
#include "meta_server.h"
#include "schema_manager.h"
#include "proto/meta.interface.pb.h"

namespace baikaldb {
class DatabaseManager {
public:
    friend class QueryDatabaseManager;
    ~DatabaseManager() {
        bthread_mutex_destroy(&_database_mutex);
    }
    static DatabaseManager* get_instance() {
        static DatabaseManager instance;
        return &instance;
    }
    void create_database(const pb::MetaManagerRequest& request, braft::Closure* done);
    void drop_database(const pb::MetaManagerRequest& request, braft::Closure* done);
    void modify_database(const pb::MetaManagerRequest& request, braft::Closure* done);
    int load_database_snapshot(const std::string& value);
public:
    void set_max_database_id(int64_t max_database_id) {
        BAIDU_SCOPED_LOCK(_database_mutex);
        _max_database_id = max_database_id;
    }
    int64_t get_max_database_id() {
        BAIDU_SCOPED_LOCK(_database_mutex);
        return _max_database_id;
    }

    void set_database_info(const pb::DataBaseInfo& database_info) {
        BAIDU_SCOPED_LOCK(_database_mutex);
        std::string database_name = database_info.namespace_name() 
                                    + "\001" 
                                    + database_info.database();
        _database_id_map[database_name] = database_info.database_id();
        _database_info_map[database_info.database_id()] = database_info;        
    }
    
    void erase_database_info(const std::string& database_name) {
        BAIDU_SCOPED_LOCK(_database_mutex);
        int64_t database_id = _database_id_map[database_name];
        _database_id_map.erase(database_name);
        _database_info_map.erase(database_id);
        _table_ids.erase(database_id);
    }
    
    void add_table_id(int64_t database_id, int64_t table_id) {
        BAIDU_SCOPED_LOCK(_database_mutex);
        _table_ids[database_id].insert(table_id);
    }

    void delete_table_id(int64_t database_id, int64_t table_id) {
        BAIDU_SCOPED_LOCK(_database_mutex);
        if (_table_ids.find(database_id) != _table_ids.end()) {
            _table_ids[database_id].erase(table_id);
        }
    }

    int64_t get_database_id(const std::string& database_name) {
        BAIDU_SCOPED_LOCK(_database_mutex);
        if (_database_id_map.find(database_name) != _database_id_map.end()) {
            return _database_id_map[database_name];
        }
        return 0;
    }

    int get_database_info(const int64_t& database_id, pb::DataBaseInfo& database_info) {
        BAIDU_SCOPED_LOCK(_database_mutex);
        if (_database_info_map.find(database_id) == _database_info_map.end()) {
            return -1;
        }
        database_info = _database_info_map[database_id];
        return 0;
    }

    int get_table_ids(const int64_t& database_id, std::set<int64_t>& table_ids) {
        BAIDU_SCOPED_LOCK(_database_mutex);
        if (_table_ids.find(database_id) == _table_ids.end()) {
            return -1;
        }
        table_ids = _table_ids[database_id];
        return 0;
    }

    void clear() {
        _database_id_map.clear();
        _database_info_map.clear();
        _table_ids.clear();
    }    
    void process_baikal_heartbeat(const pb::BaikalHeartBeatRequest* request, 
            pb::BaikalHeartBeatResponse* response);
private:
    DatabaseManager(): _max_database_id(0) {
        bthread_mutex_init(&_database_mutex, NULL);
    }
    std::string construct_database_key(int64_t database_id) {
        std::string database_key = MetaServer::SCHEMA_IDENTIFY 
                + MetaServer::DATABASE_SCHEMA_IDENTIFY;
        database_key.append((char*)&database_id, sizeof(int64_t));
        return database_key;
    }
    std::string construct_max_database_id_key() {
        std::string max_database_id_key = MetaServer::SCHEMA_IDENTIFY 
                                + MetaServer::MAX_ID_SCHEMA_IDENTIFY
                                + SchemaManager::MAX_DATABASE_ID_KEY;
        return max_database_id_key;
    }

private:
    //std::mutex                                          _database_mutex;
    bthread_mutex_t                                          _database_mutex;
    int64_t                                             _max_database_id;
    //databae name 与id 映射关系，name: namespace\001database
    std::unordered_map<std::string, int64_t>            _database_id_map;
    std::unordered_map<int64_t, pb::DataBaseInfo>       _database_info_map;
    std::unordered_map<int64_t, std::set<int64_t>>      _table_ids;
}; //class
}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
