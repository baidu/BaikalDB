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
void create_schema(pb::SchemaInfo& table) 
{
    table.set_table_name("pvstat3");
    table.set_database("FC_Content");
    table.set_namespace_name("FENGCHAO");
    table.set_partition_num(1);
    table.set_resource_tag("");
    pb::FieldInfo* field;
    field = table.add_fields();
    field->set_field_name("query_sign");
    field->set_mysql_type(pb::UINT64);

    field = table.add_fields();
    field->set_field_name("date");
    field->set_mysql_type(pb::UINT32);

    field = table.add_fields();
    field->set_field_name("pid");
    field->set_mysql_type(pb::UINT8);

    field = table.add_fields();
    field->set_field_name("cid");
    field->set_mysql_type(pb::UINT16);

    field = table.add_fields();
    field->set_field_name("channel");
    field->set_mysql_type(pb::UINT16);

    field = table.add_fields();
    field->set_field_name("pv");
    field->set_mysql_type(pb::UINT32);

    field = table.add_fields();
    field->set_field_name("epv");
    field->set_mysql_type(pb::UINT32);

    field = table.add_fields();
    field->set_field_name("pvshow");
    field->set_mysql_type(pb::UINT32);

    field = table.add_fields();
    field->set_field_name("pid_cid");
    field->set_mysql_type(pb::UINT16);


    pb::IndexInfo* pk_idx = table.add_indexs();
    pk_idx->set_index_name("PRIMARY");
    pk_idx->set_index_type(pb::I_PRIMARY);
    pk_idx->add_field_names("query_sign");
    pk_idx->add_field_names("date");
    pk_idx->add_field_names("pid");
    pk_idx->add_field_names("cid");
    pk_idx->add_field_names("channel");

    auto split_keys = table.add_split_keys();
    for (uint16_t i = 1; i < 2550; i++) {
        MutTableKey key;
        key.append_u16((65535/2550)*i);
        split_keys->add_split_keys(key.data());
        //table.add_split_keys(key.data());
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
