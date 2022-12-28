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

#include "table_manager.h"
namespace baikaldb {
class QueryTableManager {
public:
    ~QueryTableManager() {}
    static QueryTableManager* get_instance() {
        static QueryTableManager instance;
        return &instance;
    }
    void get_schema_info(const pb::QueryRequest* request, pb::QueryResponse* response);    
    void get_flatten_schema(const pb::QueryRequest* request, pb::QueryResponse* response); 
    void get_flatten_table(const pb::QueryRequest* request, pb::QueryResponse* response);

    void get_primary_key_string(int64_t table_id, std::string& primary_key_string);
    void decode_key(int64_t table_id, const TableKey& start_key, std::string& start_key_string);
    pb::PrimitiveType get_field_type(int64_t table_id, 
                                     int32_t field_id, 
                                     const pb::SchemaInfo& table_info);
    void process_console_heartbeat(const pb::ConsoleHeartBeatRequest* request,
            pb::ConsoleHeartBeatResponse* response, uint64_t log_id);

    void get_ddlwork_info(const pb::QueryRequest* request, pb::QueryResponse* response);    
    void get_virtual_index_influence_info(const pb::QueryRequest* request, pb::QueryResponse* response);
    void get_table_in_fast_importer(const pb::QueryRequest* request, pb::QueryResponse* response);
    void clean_cache() {
        BAIDU_SCOPED_LOCK(_mutex);
        _table_info_cache_time.clear();
        _table_infos_cache.clear();
    }
private:
    QueryTableManager() {}
    void check_table_and_update(
                  const std::unordered_map<int64_t, std::tuple<pb::SchemaInfo, int64_t, int64_t>> table_schema_map,
                  std::unordered_map<int64_t, int64_t>& report_table_map,
                  pb::ConsoleHeartBeatResponse* response, uint64_t log_id);  
    void construct_query_table(const TableMem& table, pb::QueryTable* query_table);
    // 由于show table status太重了，进行cache
    bthread::Mutex _mutex;
    std::unordered_map<std::string, int64_t> _table_info_cache_time;
    // db -> table -> table info
    std::unordered_map<std::string, std::map<std::string, pb::QueryTable>> _table_infos_cache;
}; //class

}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
