
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
DEFINE_string(resource_tag, "", "resouce_tag");
DEFINE_string(suffix, "_tmp", "_tmp");

int create_table(const std::string& namespace_name, const std::string& database, 
                const std::string& table_name, const std::string& resource_tag,
                const std::string& suffix) {
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

    //根据返回的结果创建新的建表请求
    pb::MetaManagerRequest create_table_request;
    create_table_request.set_op_type(pb::OP_CREATE_TABLE);
    create_table_request.mutable_table_info()->set_table_name(schema_info.table_name() + suffix);
    create_table_request.mutable_table_info()->set_database(schema_info.database());
    create_table_request.mutable_table_info()->set_namespace_name(schema_info.namespace_name());
    create_table_request.mutable_table_info()->set_replica_num(schema_info.replica_num());
    create_table_request.mutable_table_info()->set_resource_tag(schema_info.resource_tag());
    if (resource_tag.size() > 0) {
        create_table_request.mutable_table_info()->set_resource_tag(resource_tag);
    }
    create_table_request.mutable_table_info()->set_byte_size_per_record(schema_info.byte_size_per_record());
    for (auto& field_info : schema_info.fields()) {
        auto add_field = create_table_request.mutable_table_info()->add_fields();
        *add_field = field_info;
        add_field->clear_new_field_name();
        add_field->clear_field_id();
    }
    for (auto& index_info : schema_info.indexs()) {
        auto add_index = create_table_request.mutable_table_info()->add_indexs();
        *add_index = index_info;
        add_index->clear_new_index_name();
        add_index->clear_field_ids();
        add_index->clear_index_id();
    }
    std::vector<std::string> split_keys;
    std::map<std::string, const pb::RegionInfo*> table_key_map;
    for (auto& region_info : response.region_infos()) {
        if (table_key_map.count(region_info.start_key()) == 0) {
            table_key_map[region_info.start_key()] = &region_info;
        } else {
            std::cout<< "err 2 region_id:" << region_info.region_id() << "\n";
        }
    }
    std::string last_end_key = "";
    for (auto kv : table_key_map) {
        if (kv.second->start_key() != last_end_key) {
            std::cout<< "err region_id:" << kv.second->region_id() << "\n";
        }
        last_end_key = kv.second->end_key();
    }
    DB_WARNING("end");
    return 0;
    std::sort(split_keys.begin(), split_keys.end());
    for (auto& split_key : split_keys) {
        create_table_request.mutable_table_info()->add_split_keys(split_key);
    }

    pb::MetaManagerResponse create_table_response;
    if (interact.send_request("meta_manager", create_table_request, create_table_response) != 0) {
        DB_WARNING("send_request fail");
        return -1;
    }
    DB_WARNING("req:%s", create_table_request.ShortDebugString().c_str());
    DB_WARNING("create table split_key_size:%d", split_keys.size());
    DB_WARNING("res:%s", create_table_response.ShortDebugString().c_str());

    return 0;
}

} // namespace baikaldb

int main(int argc, char **argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    baikaldb::create_table(baikaldb::FLAGS_namespace_name, 
                           baikaldb::FLAGS_database, 
                           baikaldb::FLAGS_table_name, 
                           baikaldb::FLAGS_resource_tag, 
                           baikaldb::FLAGS_suffix);

    return 0;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
