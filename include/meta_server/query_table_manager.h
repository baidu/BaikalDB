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

private:
    QueryTableManager() {}
}; //class

}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
