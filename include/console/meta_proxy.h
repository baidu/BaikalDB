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

#include "proto/meta.interface.pb.h"
#include "proto/console.pb.h"
#include "table_key.h"
#include "meta_server_interact.hpp"
#include "baikaldb_proxy.h"

namespace baikaldb {

class MetaProxy {
public:
    virtual ~MetaProxy() {}

    static MetaProxy* get_instance() {
        static MetaProxy meta;
        return &meta;
    }

    int init(BaikalProxy* baikaldb, std::vector<std::string>& platform_tags);
    void stop();
    int query_schema_info(const pb::QueryParam* param, pb::ConsoleResponse* response);

private:
    MetaProxy() {}
    MetaProxy& operator=(const MetaProxy& m);

    void construct_heart_beat_request(const std::string& meta_server, pb::ConsoleHeartBeatRequest& request);
    void process_heart_beat_response(const std::string& meta_server, const pb::ConsoleHeartBeatResponse& response);
    void construct_query_table(const pb::TableInfo& table_info,
             pb::QueryTable* flatten_table_info);
    void construct_query_region(const std::string& meta_server_tag, const pb::RegionInfo& region_info,
             pb::QueryRegion* query_region_info);
    pb::PrimitiveType get_field_type(
                    const int32_t field_id,
                    const pb::SchemaInfo& table_info);
    void generate_key(const pb::IndexInfo& index_info,
                      const pb::SchemaInfo& table_info,
                      const TableKey& start_key,
                      std::string& start_key_string,
                      std::vector<int32_t>& field_ids,
                      int32_t& pos);
    void decode_key(const int64_t table_id, const pb::SchemaInfo& table_info, const TableKey& start_key,
        std::string& start_key_string);

    void report_heart_beat();   

    std::map<std::string, MetaServerInteract*> _meta_interact_map;
    BaikalProxy* _baikaldb = nullptr;

    // k:v -> <region_id, tuple<version, conf_version, leader>>
    typedef std::unordered_map<int64_t, std::tuple<int64_t, int64_t, std::string, int64_t>> TableRegionVersionMap;
    std::map<std::string, TableRegionVersionMap> _table_region_version_mapping;
    //std::unordered_map<int64_t, std::tuple<int64_t, int64_t, std::string, int64_t>> _table_region_version_mapping;
    typedef std::unordered_map<int64_t, int64_t> TableVersionMap;
    std::map<std::string, TableVersionMap> _table_version_mapping;
    typedef std::unordered_map<int64_t, pb::SchemaInfo> TableSchemaMap;
    std::map<std::string, TableSchemaMap> _table_schema_mapping;
    int64_t         _cur_step = 0;
    int64_t         _heartbeat_times = 1;
    bool            _shutdown = false; 
    Bthread         _heartbeat_bth;
};
} // namespace baikaldb