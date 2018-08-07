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

#include "update_planner.h"
#include "network_socket.h"

namespace baikaldb {
int UpdatePlanner::plan() {
    if (_ctx->stmt == nullptr) {
        DB_WARNING("no sql command set");
        return -1;
    }
    _update = (parser::UpdateStmt*)_ctx->stmt;
    if (_update->table_refs == nullptr
            || _update->table_refs->node_type == parser::NT_JOIN) {
        DB_WARNING("unsupport mutile update");
        return -1;
    }
    if (_update->table_refs->node_type == parser::NT_TABLE) {
        if (0 != parse_db_tables((parser::TableName*)_update->table_refs)) {
            DB_WARNING("parse db table fail");
            return -1;
        }
    } 
    if (_update->table_refs->node_type == parser::NT_TABLE_SOURCE) {
        if (0 != parse_db_tables((parser::TableSource*)_update->table_refs)) {
            DB_WARNING("parse db table fail");
            return -1;
        }
    } 
    if (0 != parse_kv_list()) {
        return -1;
    }
    if (0 != parse_where()) {
        return -1;
    }
    if (0 != parse_orderby()) {
        return -1;
    }
    parse_limit();
    create_packet_node(pb::OP_UPDATE);

    if (0 != create_update_node()) {
        return -1;
    }
    if (0 != create_sort_node()) {
        return -1;
    }
    if (0 != create_filter_node(_where_filters, pb::WHERE_FILTER_NODE)) {
        return -1;
    }
    create_scan_tuple_descs();
    create_order_by_tuple_desc();
    if (0 != create_scan_nodes()) {
        return -1;
    }
    set_dml_txn_state();
    return 0;
}

void UpdatePlanner::parse_limit() {
    if (_update->limit == nullptr) {
        return;
    }
    _limit_count = _update->limit->count;
}

int UpdatePlanner::create_update_node() {
    if (_table_tuple_mapping.size() != 1) {
        DB_WARNING("no database name, specify database by USE cmd");
        return -1;
    }
    auto iter = _table_tuple_mapping.begin();

    pb::PlanNode* update_node = _ctx->add_plan_node();
    update_node->set_node_type(pb::UPDATE_NODE);
    update_node->set_limit(_limit_count);
    update_node->set_num_children(1); //TODO 
    pb::DerivePlanNode* derive = update_node->mutable_derive_node();
    pb::UpdateNode* update = derive->mutable_update_node();
    update->set_table_id(iter->first);

    // add slots and exprs
    for (uint32_t idx = 0; idx < _update_slots.size(); ++idx) {
        update->add_update_slots()->CopyFrom(_update_slots[idx]);
        update->add_update_exprs()->CopyFrom(_update_values[idx]);
    }

    IndexInfo pk = _factory->get_index_info(iter->first);
    if (pk.id == -1) {
        DB_WARNING("no pk found with id: %ld", iter->first);
        return -1;
    }
    for (auto& field : pk.fields) {
        auto& slot = get_scan_ref_slot(iter->first, field.id, field.type);
        update->add_primary_slots()->CopyFrom(slot);
    }
    return 0;
}

int UpdatePlanner::parse_kv_list() {
    parser::Vector<parser::Assignment*> set_list = _update->set_list;
    for (int idx = 0; idx < set_list.size(); ++idx) {
        if (set_list[idx] == nullptr) {
            DB_WARNING("set item is nullptr");
            return -1;
        }
        std::string full_name = get_field_full_name(set_list[idx]->name);
        if (full_name.empty()) {
            DB_WARNING("get full field name failed");
            return -1;
        }
        FieldInfo* field_info = nullptr;
        if (nullptr == (field_info = get_field_info(full_name))) {
            DB_WARNING("invalid field name in");
            return -1;
        }
        auto slot = get_scan_ref_slot(field_info->table_id, field_info->id, field_info->type);
        _update_slots.push_back(slot);

        pb::Expr value_expr;
        if (0 != create_expr_tree(set_list[idx]->expr, value_expr)) {
            DB_WARNING("create update value expr failed");
            return -1;
        }
        _update_values.push_back(value_expr);
    }
    return 0;
}

int UpdatePlanner::parse_where() {
    if (_update->where == nullptr) {
        return 0;
    }
    if (0 != flatten_filter(_update->where, _where_filters)) {
        DB_WARNING("flatten_filter failed");
        return -1;
    }
    return 0;
}

int UpdatePlanner::parse_orderby() {
    if (_update->order != nullptr) {
        DB_WARNING("update doesnot support orderby");
        return -1;
    }
    return 0;
}

} // end of namespace bailaldb
