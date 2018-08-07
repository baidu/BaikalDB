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

#include "auto_inc.h"
#include "exec_node.h"
#include "query_context.h"
#include "schema_factory.h"
#include "insert_node.h"
#include "meta_server_interact.hpp"

namespace baikaldb {
int AutoInc::analyze(QueryContext* ctx) {
    ExecNode* plan = ctx->root;
    if (ctx->insert_records.size() == 0) {
        return 0;
    }
    InsertNode* insert_node = static_cast<InsertNode*>(plan->get_node(pb::INSERT_NODE));
    InsertNode* replace_node = static_cast<InsertNode*>(plan->get_node(pb::REPLACE_NODE));
    int64_t table_id = -1;
    if (insert_node != NULL) {
        table_id = insert_node->table_id();
    } else if (replace_node != NULL) {
        table_id = replace_node->table_id();
    }
    SchemaFactory* schema_factory = SchemaFactory::get_instance();
    TableInfo table_info = schema_factory->get_table_info(table_id);
    if (table_info.auto_inc_field_id == -1) {
        return 0;
    }
    int auto_id_count = 0;
    int64_t max_id = 0;
    for (auto& record : ctx->insert_records) {
            auto field = record->get_field_by_tag(table_info.auto_inc_field_id);
            ExprValue value = record->get_value(field);
            if (value.is_null()) {
                ++auto_id_count;
            } else {
                int64_t int_val = value.get_numberic<int64_t>();
                if (int_val > max_id) {
                    max_id = int_val;
                }
            }
    }
    if (auto_id_count == 0 && max_id == 0) {
        return 0;
    }
    // 请求meta来获取自增id
    pb::MetaManagerRequest request;
    pb::MetaManagerResponse response;
    request.set_op_type(pb::OP_GEN_ID_FOR_AUTO_INCREMENT);
    auto auto_increment_ptr = request.mutable_auto_increment();
    auto_increment_ptr->set_table_id(table_id);
    auto_increment_ptr->set_count(auto_id_count);
    auto_increment_ptr->set_start_id(max_id);
    if (MetaServerInteract::get_instance()->send_request("meta_manager", 
                                                          request, 
                                                          response) != 0) {
        DB_FATAL("gen id from meta_server fail, sql:%s", ctx->sql.c_str());
        return -1; 
    }
    
    if (auto_id_count == 0) {
        return 0;
    }
    int64_t start_id = response.start_id();
    for (auto& record : ctx->insert_records) {
        auto field = record->get_field_by_tag(table_info.auto_inc_field_id);
        ExprValue value = record->get_value(field);
        if (value.is_null()) {
            value.type = pb::INT64;
            value._u.int64_val = start_id++;
            record->set_value(field, value);
        }
    }
    if (start_id != (int64_t)response.end_id()) {
        DB_FATAL("gen id count not equal to request id count, sql:%s", ctx->sql.c_str());
        return -1;
    }
    return 0;
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
