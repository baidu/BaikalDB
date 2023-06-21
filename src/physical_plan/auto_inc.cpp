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

#include "auto_inc.h"
#include <boost/algorithm/string.hpp>
#include "exec_node.h"
#include "query_context.h"
#include "schema_factory.h"
#include "insert_node.h"
#include "network_socket.h"
#include "meta_server_interact.hpp"

namespace baikaldb {
DECLARE_bool(meta_tso_autoinc_degrade);

bool AutoInc::need_degrade = false;
TimeCost AutoInc::last_degrade_time;
bvar::Adder<int64_t> AutoInc::auto_inc_count;
bvar::Adder<int64_t> AutoInc::auto_inc_error;

int AutoInc::analyze(QueryContext* ctx) {
    ExecNode* plan = ctx->root;
    if (ctx->insert_records.size() == 0) {
        return 0;
    }
    InsertNode* insert_node = static_cast<InsertNode*>(plan->get_node(pb::INSERT_NODE));
    int64_t table_id = -1;
    if (insert_node != NULL) {
        table_id = insert_node->table_id();
    }
    SchemaFactory* schema_factory = SchemaFactory::get_instance();
    auto table_info_ptr = schema_factory->get_table_info_ptr(table_id);
    if (table_info_ptr == nullptr || table_info_ptr->auto_inc_field_id == -1) {
        return 0;
    }
    
    return update_auto_inc(table_info_ptr, ctx->client_conn, ctx->use_backup, ctx->insert_records);
}

int AutoInc::update_auto_inc(SmartTable table_info_ptr,
                           NetworkSocket* client_conn,
                           bool use_backup,
                           std::vector<SmartRecord>& insert_records) {
    int64_t auto_id_count = 0;
    int64_t max_id = 0;
    for (auto& record : insert_records) {
            auto field = record->get_field_by_tag(table_info_ptr->auto_inc_field_id);
            ExprValue value = record->get_value(field);
            // 兼容mysql，值为0会分配自增id
            if (value.is_null() || value.get_numberic<int64_t>() == 0) {
                ++auto_id_count;
            } else {
                int64_t int_val = value.get_numberic<int64_t>();
                if (int_val > max_id) {
                    max_id = int_val;
                }
            }
    }
    auto field_info = table_info_ptr->get_field_ptr(table_info_ptr->auto_inc_field_id);
    // 通过字段注释可以标示:自增id不会随着插入id进行跳号,当插入的自增id来自于异构数据源时,用于避免自增id的双写冲突问题.
    if (field_info != nullptr && field_info->noskip) {
        max_id = 0;
    }
    if (auto_id_count == 0 && max_id == 0) {
        return 0;
    }
    // 表不需要生成自增id，只需要给meta更新最大的key，降级可以不出错
    if (auto_id_count == 0 && FLAGS_meta_tso_autoinc_degrade && need_degrade) {
        return 0;
    }
    int64_t start_id = 0;
    bool degrade = false;
    if (FLAGS_meta_tso_autoinc_degrade && need_degrade && table_info_ptr->auto_inc_rand_max > 0) {
        // 降级获取自增id
        start_id = butil::fast_rand() % table_info_ptr->auto_inc_rand_max;
        degrade = true;
        DB_WARNING("meta_tso_autoinc_degrade, auto inc, table_id:%ld, start_id:%ld, auto_id_count:%ld", 
                table_info_ptr->id, start_id, auto_id_count);
    } else {
        // 请求meta来获取自增id
        TimeCost cost;
        pb::MetaManagerRequest request;
        pb::MetaManagerResponse response;
        request.set_op_type(pb::OP_GEN_ID_FOR_AUTO_INCREMENT);
        auto auto_increment_ptr = request.mutable_auto_increment();
        auto_increment_ptr->set_table_id(table_info_ptr->id);
        auto_increment_ptr->set_count(auto_id_count);
        auto_increment_ptr->set_start_id(max_id);
        MetaServerInteract* interact = MetaServerInteract::get_auto_incr_instance();
        if (use_backup) {
            interact = MetaServerInteract::get_backup_instance();
        }
        int ret = interact->send_request("meta_manager", request, response);
        auto_inc_count << 1;
        if (ret != 0) {
            auto_inc_error << 1;
            DB_FATAL("gen id from meta_server fail");
            return -1; 
        }

        if (auto_id_count == 0) {
            if (max_id > 0) {
                client_conn->last_insert_id = max_id;
            }
            return 0;
        }
        int64_t cost_time = cost.get_time();
        if (cost_time > 100 * 1000ULL) {
            DB_WARNING("gen id too long, table_id: %ld, cost: %ld", table_info_ptr->id, cost_time);
        }
        start_id = response.start_id();
    }
    
    client_conn->last_insert_id = start_id;
    for (auto& record : insert_records) {
        auto field = record->get_field_by_tag(table_info_ptr->auto_inc_field_id);
        ExprValue value = record->get_value(field);
        if (value.is_null() || value.get_numberic<int64_t>() == 0) {
            value.type = pb::INT64;
            value._u.int64_val = start_id++;
            record->set_value(field, value);
        }
    }
    return 0;
}

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
