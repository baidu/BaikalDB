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
#include "meta_server.h"
#include "schema_manager.h"

namespace baikaldb {
class NamespaceManager {
public:
    friend class QueryNamespaceManager;
    ~NamespaceManager() {
        bthread_mutex_destroy(&_namespace_mutex);
    }
    
    static NamespaceManager* get_instance() {
        static NamespaceManager instance;
        return &instance;
    }
    //raft串行调用写入类接口, 但与查询接口并发访问
    void create_namespace(const pb::MetaManagerRequest& request, braft::Closure* done);
    void drop_namespace(const pb::MetaManagerRequest& request, braft::Closure* done);
    void modify_namespace(const pb::MetaManagerRequest& request, braft::Closure* done);
    int load_namespace_snapshot(const std::string& value);
  
    void set_max_namespace_id(int64_t max_namespace_id) {
        BAIDU_SCOPED_LOCK(_namespace_mutex);
        _max_namespace_id = max_namespace_id;
    }
    int64_t get_max_namespace_id() {
        BAIDU_SCOPED_LOCK(_namespace_mutex);
        return _max_namespace_id;
    }

    void set_namespace_info(const pb::NameSpaceInfo& namespace_info) {
        BAIDU_SCOPED_LOCK(_namespace_mutex);
        _namespace_id_map[namespace_info.namespace_name()] = namespace_info.namespace_id();
        _namespace_info_map[namespace_info.namespace_id()] = namespace_info;
    }
   
    void erase_namespace_info(const std::string& namespace_name) {
        BAIDU_SCOPED_LOCK(_namespace_mutex);
        int64_t namespace_id = _namespace_id_map[namespace_name];
        _namespace_id_map.erase(namespace_name);
        _namespace_info_map.erase(namespace_id);
        _database_ids.erase(namespace_id);
    }
    void add_database_id(int64_t namespace_id, int64_t database_id) {
        BAIDU_SCOPED_LOCK(_namespace_mutex);
        _database_ids[namespace_id].insert(database_id);    
    }
    void delete_database_id(int64_t namespace_id, int64_t database_id) {
        BAIDU_SCOPED_LOCK(_namespace_mutex);
        if (_database_ids.find(namespace_id) != _database_ids.end()) {
            _database_ids[namespace_id].erase(database_id);
        }       
    }
    int64_t get_namespace_id(const std::string& namespace_name) {
        BAIDU_SCOPED_LOCK(_namespace_mutex);
        if (_namespace_id_map.find(namespace_name) == _namespace_id_map.end()) {
            return 0;
        }
        return _namespace_id_map[namespace_name];
    }
    const std::string get_resource_tag(const int64_t& namespace_id) {
        BAIDU_SCOPED_LOCK(_namespace_mutex);
        if (_namespace_info_map.find(namespace_id) == _namespace_info_map.end()) {
            return "";
        }
        return _namespace_info_map[namespace_id].resource_tag();
    }
    int get_namespace_info(const int64_t& namespace_id, pb::NameSpaceInfo& namespace_info) {
        BAIDU_SCOPED_LOCK(_namespace_mutex);
        if (_namespace_info_map.find(namespace_id) == _namespace_info_map.end()) {
            return -1;
        }
        namespace_info = _namespace_info_map[namespace_id];
        return 0;
    }

    void clear() {
        _namespace_id_map.clear();
        _namespace_info_map.clear();
        _database_ids.clear();
    }
private:
    NamespaceManager(): _max_namespace_id(0) {
        bthread_mutex_init(&_namespace_mutex, NULL);
    }
    
    std::string construct_namespace_key(int64_t namespace_id) {
        std::string namespace_key = MetaServer::SCHEMA_IDENTIFY 
                + MetaServer::NAMESPACE_SCHEMA_IDENTIFY; 
        namespace_key.append((char*)&namespace_id, sizeof(int64_t));
        return namespace_key;
    }
    
    std::string construct_max_namespace_id_key() {
        std::string max_namespace_id_key = MetaServer::SCHEMA_IDENTIFY 
                                + MetaServer::MAX_ID_SCHEMA_IDENTIFY
                                + SchemaManager::MAX_NAMESPACE_ID_KEY;
        return max_namespace_id_key;
    }
private:
    //std::mutex                                          _namespace_mutex;
    bthread_mutex_t                                          _namespace_mutex;

    int64_t                                             _max_namespace_id; 
    // namespace层级name与id的映射关系
    std::unordered_map<std::string, int64_t>            _namespace_id_map;
    // namespace层级，id与info的映射关系
    std::unordered_map<int64_t, pb::NameSpaceInfo>      _namespace_info_map;
    std::unordered_map<int64_t, std::set<int64_t>>      _database_ids; //only in memory, not in rocksdb
};

}//namespace
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
