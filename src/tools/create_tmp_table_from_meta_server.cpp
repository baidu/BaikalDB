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
DEFINE_string(database, "db", "database");
DEFINE_string(database2, "", "database");
DEFINE_string(table_name, "tb", "table_name");
DEFINE_string(table_name2, "", "table_name");
DEFINE_string(resource_tag, "", "resouce_tag");
DEFINE_string(suffix, "_tmp", "_tmp");
DECLARE_string(meta_server_bns);
DEFINE_string(meta_server_bns2, "", "meta server bns");

int create_table(const std::string& namespace_name, 
                const std::string& resource_tag,
                const std::string& suffix) {
    MetaServerInteract interact;
    if (interact.init() != 0) {
        DB_WARNING("init fail");
        return -1;
    }
    if (FLAGS_table_name2.empty()) {
        FLAGS_table_name2 = FLAGS_table_name;
    }
    if (FLAGS_database2.empty()) {
        FLAGS_database2 = FLAGS_database;
    }
    if (FLAGS_meta_server_bns2.empty()) {
        FLAGS_meta_server_bns2 = FLAGS_meta_server_bns;
    }
    //先导出table的scheme_info信息
    pb::QueryRequest request;
    request.set_op_type(pb::QUERY_SCHEMA);
    request.set_namespace_name(namespace_name);
    request.set_database(FLAGS_database);
    request.set_table_name(FLAGS_table_name);
    
    pb::QueryResponse response;
    if (interact.send_request("query", request, response) != 0) {
        DB_WARNING("send_request fail");
        return -1;
    }
    DB_WARNING("region size:%d", response.region_infos_size());
    DB_WARNING("req:%s  \nres:%s", 
                request.DebugString().c_str(), 
                response.ShortDebugString().c_str());
    
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
    std::cout << FLAGS_database2 << " " << FLAGS_table_name2 << "\n";
    create_table_request.set_op_type(pb::OP_CREATE_TABLE);
    create_table_request.mutable_table_info()->set_table_name(FLAGS_table_name2 + suffix);
    create_table_request.mutable_table_info()->set_database(FLAGS_database2);
    create_table_request.mutable_table_info()->set_namespace_name(schema_info.namespace_name());
    create_table_request.mutable_table_info()->set_replica_num(schema_info.replica_num());
    if (resource_tag.size() > 0) {
        create_table_request.mutable_table_info()->set_resource_tag(resource_tag);
    }
    //create_table_request.mutable_table_info()->set_byte_size_per_record(50);

    create_table_request.mutable_table_info()->set_byte_size_per_record(schema_info.byte_size_per_record());
    create_table_request.mutable_table_info()->set_region_split_lines(schema_info.region_split_lines());

    //create_table_request.mutable_table_info()->set_ttl_duration(172800);

    for (auto& field_info : schema_info.fields()) {
        /*
        if (
            field_info.field_name() == "bidprefer" ||
            field_info.field_name() == "unitbid" ||
            field_info.field_name() == "planMpricefactor" ||
            field_info.field_name() == "planPcpricefactor" ||
            field_info.field_name() == "unitMpricefactor" ||
            field_info.field_name() == "unitPcpricefactor"
            ) {
            continue;
        }*/
        auto add_field = create_table_request.mutable_table_info()->add_fields();
        *add_field = field_info;
        if (field_info.field_name() == "unitid") {
            add_field->set_mysql_type(pb::UINT64);
        }
        add_field->clear_new_field_name();
        add_field->clear_field_id();
    }
    std::map<int64_t, std::string> index_name_ids;
    for (auto& index_info : schema_info.indexs()) {
        index_name_ids[index_info.index_id()] = index_info.index_name(); 
        auto add_index = create_table_request.mutable_table_info()->add_indexs();
        *add_index = index_info;
        add_index->clear_new_index_name();
        add_index->clear_field_ids();
        add_index->clear_index_id();
        /*
        if (add_index->index_type() == pb::I_FULLTEXT) {
            add_index->set_segment_type(pb::S_UNIGRAMS);
        }
        */
    }

    for (auto& dist : schema_info.dists()) {
        auto add_dist = create_table_request.mutable_table_info()->add_dists();
        *add_dist = dist;
    } 
    /*
    auto add_index = create_table_request.mutable_table_info()->add_indexs();
    add_index->set_index_name("showword_in");
    add_index->add_field_names("userid");
    add_index->add_field_names("showword");
    add_index->set_index_type(pb::I_KEY);
    */
    std::map<std::string, std::set<std::string>> index_split_keys;
    for (auto& region_info : response.region_infos()) {
        std::string index_name = index_name_ids[region_info.table_id()];
        if (region_info.has_start_key() && region_info.start_key().size() != 0) {
            index_split_keys[index_name].insert(region_info.start_key());
        }
    }

    for (auto& split_keys : index_split_keys) {
        std::string index_name = split_keys.first;
        auto pb_split_keys = create_table_request.mutable_table_info()->add_split_keys();
        pb_split_keys->set_index_name(index_name);
        int n = 0;
        for (auto& split_key : split_keys.second) {
           // if (n++%10==0) {
            pb_split_keys->add_split_keys(split_key);
            //}

        }
    }

    pb::MetaManagerResponse create_table_response;
    MetaServerInteract interact2;
    FLAGS_meta_server_bns=FLAGS_meta_server_bns2;
    if (interact2.init() != 0) {
        DB_WARNING("init fail");
        return -1;
    }
    if (interact2.send_request("meta_manager", create_table_request, create_table_response) != 0) {
        DB_WARNING("send_request fail");
        DB_WARNING("res:%s", create_table_response.ShortDebugString().c_str());
        return -1;
    }
    DB_WARNING("req:%s", create_table_request.ShortDebugString().c_str());
    for (auto& split_keys : index_split_keys) {
        DB_WARNING("create index_name: %s split_key_size:%d", split_keys.first.c_str(), split_keys.second.size());
    }
    DB_WARNING("res:%s", create_table_response.ShortDebugString().c_str());

    return 0;
}

} // namespace baikaldb

int main(int argc, char **argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    baikaldb::create_table(baikaldb::FLAGS_namespace_name, 
                           baikaldb::FLAGS_resource_tag, 
                           baikaldb::FLAGS_suffix);

    return 0;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
