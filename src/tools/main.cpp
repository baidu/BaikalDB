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
DEFINE_string(table_name, "tb", "table_name");
DEFINE_int64(ttl_duration, 0, "ttl_duration");
void create_schema(pb::SchemaInfo& table) 
{
    table.set_table_name(FLAGS_table_name);
    table.set_database("holmes");
    table.set_namespace_name("HOLMES");
    table.set_partition_num(1);
    table.set_resource_tag("holmes");
    table.set_region_split_lines(2500000);
    table.set_replica_num(1);
    if (FLAGS_ttl_duration > 0) {
        table.set_ttl_duration(FLAGS_ttl_duration);
    }
    auto* dist = table.add_dists();
    dist->set_logical_room("yq");
    dist->set_count(1);

    pb::FieldInfo* field;
    field = table.add_fields();
    field->set_field_name("sign");
    field->set_mysql_type(pb::UINT64);

    field = table.add_fields();
    field->set_field_name("text");
    field->set_mysql_type(pb::STRING);

    field = table.add_fields();
    field->set_field_name("access_time");
    field->set_mysql_type(pb::UINT32);

    pb::IndexInfo* pk_idx = table.add_indexs();
    pk_idx->set_index_name("primary_key");
    pk_idx->set_index_type(pb::I_PRIMARY);
    pk_idx->add_field_names("sign");
    auto split_keys = table.add_split_keys();
    split_keys->set_index_name("primary_key");
    for (uint8_t i = 0; i < 255; i++) {
        for (uint8_t j = 0; j < 255; j++) {
            if (i != 0 && j != 0) {
                MutTableKey key;
                key.append_u8(i);
                key.append_u8(j);
                key.append_u8(0);
                key.append_u8(0);
                key.append_u32(0);
                split_keys->add_split_keys(key.data());
            }
            {
                MutTableKey key;
                key.append_u8(i);
                key.append_u8(j);
                key.append_u8(128);
                key.append_u8(0);
                key.append_u32(0);
                split_keys->add_split_keys(key.data());
            }
        }
    }

}

int create_table() {
    MetaServerInteract interact;
    if (interact.init() != 0) {
        DB_WARNING("init fail");
        return -1;
    }
    pb::MetaManagerRequest request;
    request.set_op_type(pb::OP_CREATE_TABLE);
    create_schema(*request.mutable_table_info());
    pb::MetaManagerResponse response;
    if (interact.send_request("meta_manager", request, response) != 0) {
        DB_WARNING("send_request fail");
        return -1;
    }
    DB_WARNING("req:%s  \nres:%s", request.DebugString().c_str(), response.DebugString().c_str());
    if (response.errcode() != pb::SUCCESS) {
        DB_WARNING("err:%s", response.errmsg().c_str());
        return -1;
    }
    return 0;
}

} // namespace baikaldb

int main(int argc, char **argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    baikaldb::create_table();
    sleep(30);

    return 0;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
