
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

#include <net/if.h>
#include <sys/ioctl.h>
#include <signal.h>
#include <stdio.h>
#include <string>
#include <Configure.h>
#include <baidu/rpc/server.h>
#include <gflags/gflags.h>
#include "common.h"
#include "schema_factory.h"
#include "meta_server_interact.hpp"
#include "mut_table_key.h"

namespace baikaldb {
DEFINE_string(namespace_name, "FENGCHAO", "FENGCHAO");
DEFINE_string(database, "", "database");
DEFINE_string(table_name, "", "table_name");

int create_table(const std::string& namespace_name, const std::string& database, 
                const std::string& table_name) {
    MetaServerInteract interact;
    if (interact.init() != 0) {
        DB_WARNING("init fail");
        return -1;
    }
    //先导出table的scheme_info信息
    pb::QueryRequest request;
    request.set_op_type(pb::QUERY_SCHEMA);
    request.set_namespace_name(namespace_name);
    request.set_database(database);
    request.set_table_name(table_name);
    
    pb::QueryResponse response;
    if (interact.send_request("query", request, response) != 0) {
        DB_WARNING("send_request fail");
        return -1;
    }
    DB_WARNING("region size:%d", response.region_infos_size());
    //DB_WARNING("req:%s  \nres:%s", 
     //           request.DebugString().c_str(), 
       //         response.ShortDebugString().c_str());
    
    if (response.errcode() != pb::SUCCESS) {
        DB_WARNING("err:%s", response.errmsg().c_str());
        return -1;
    }
    
    if (response.schema_infos_size() != 1) {
        DB_WARNING("has no schemainfo");
        return -1;
    }
    const pb::SchemaInfo& schema_info = response.schema_infos(0);
    std::map<int64_t, std::string> index_name_ids;
    for (auto& index_info : schema_info.indexs()) {
        index_name_ids[index_info.index_id()] = index_info.index_name();
    }
    std::map<std::string, std::vector<pb::RegionInfo>> index_region_infos;
    for (auto& region_info : response.region_infos()) {
        std::string index_name = index_name_ids[region_info.table_id()];
        index_region_infos[index_name].push_back(region_info);
    }
    for (auto& region_infos_per_index : index_region_infos) {
        std::string index_name = region_infos_per_index.first;
        std::map<std::string, const pb::RegionInfo*> table_key_map;
        for (auto& region_info : region_infos_per_index.second) {
            if (table_key_map.count(region_info.start_key()) == 0) {
                table_key_map[region_info.start_key()] = &region_info;
            } else {
                std::cout<< "err 2 index_name: " << index_name << "region_id:" << region_info.region_id() << "\n";
            }
        }
        std::string last_end_key = "";
        for (auto kv : table_key_map) {
            if (kv.second->start_key() != last_end_key) {
                std::cout<< "err index_name: " << index_name << "region_id:" << kv.second->region_id() << "\n";
            }
            last_end_key = kv.second->end_key();
        }
    }
    DB_WARNING("end");
    return 0;
}

} // namespace baikaldb

int main(int argc, char **argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    baikaldb::create_table(baikaldb::FLAGS_namespace_name, 
                           baikaldb::FLAGS_database, 
                           baikaldb::FLAGS_table_name);

    return 0;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
