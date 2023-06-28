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

#include "update_planner.h"
#include "network_socket.h"

namespace baikaldb {
DEFINE_bool(open_non_where_sql_forbid, false, "open non where conjunct sql forbid switch default:false");
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
        if (0 != parse_db_tables(_update->table_refs, &_join_root)) {
            DB_WARNING("parse db table fail");
            return -1;
        }
    } 
    if (_update->table_refs->node_type == parser::NT_TABLE_SOURCE) {
        if (0 != parse_db_tables(_update->table_refs, &_join_root)) {
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
    if (0 != parse_limit()) {
        return -1;
    }
    create_packet_node(pb::OP_UPDATE);
    pb::PlanNode* update_node = _ctx->add_plan_node();
    if (0 != create_update_node(update_node)) {
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
    ScanTupleInfo& info = _plan_table_ctx->table_tuple_mapping[try_to_lower(_current_tables[0])];
    int64_t table_id = info.table_id;
    _ctx->prepared_table_id = table_id;
    set_dml_txn_state(table_id);
    // 局部索引binlog处理标记
    if (_ctx->open_binlog && !_factory->has_global_index(table_id)) {
        update_node->set_local_index_binlog(true);
    }
    return 0;
}

int UpdatePlanner::parse_limit() {
    if (_update->limit != nullptr) {
        _ctx->stat_info.error_code = ER_SYNTAX_ERROR;
        _ctx->stat_info.error_msg << "syntax error! update does not support limit";
        return -1;
    }
    return 0;
}

int UpdatePlanner::create_update_node(pb::PlanNode* update_node) {
    if (_plan_table_ctx->table_tuple_mapping.size() != 1) {
        DB_WARNING("no database name, specify database by USE cmd");
    }
    if (_current_tables.size() != 1 || _plan_table_ctx->table_tuple_mapping.count(try_to_lower(_current_tables[0])) == 0) {
        DB_WARNING("invalid sql format: %s", _ctx->sql.c_str());
        return -1;
    }
    ScanTupleInfo& info = _plan_table_ctx->table_tuple_mapping[try_to_lower(_current_tables[0])];
    int64_t table_id = info.table_id;

    if (_apply_root != nullptr) {
        DB_WARNING("not support correlation subquery sql format: %s", _ctx->sql.c_str());
        return -1;
    }

    update_node->set_node_type(pb::UPDATE_NODE);
    update_node->set_limit(_limit_count);
    update_node->set_is_explain(_ctx->is_explain);
    update_node->set_num_children(1); //TODO 
    pb::DerivePlanNode* derive = update_node->mutable_derive_node();
    pb::UpdateNode* update = derive->mutable_update_node();
    update->set_table_id(table_id);
    // add slots and exprs
    for (uint32_t idx = 0; idx < _update_slots.size(); ++idx) {
        update->add_update_slots()->CopyFrom(_update_slots[idx]);
        update->add_update_exprs()->CopyFrom(_update_values[idx]);
    }

    auto pk = _factory->get_index_info_ptr(table_id);
    if (pk == nullptr) {
        DB_WARNING("no pk found with id: %ld", table_id);
        return -1;
    }
    for (auto& field : pk->fields) {
        auto& slot = get_scan_ref_slot(try_to_lower(_current_tables[0]), table_id, field.id, field.type);
        update->add_primary_slots()->CopyFrom(slot);
    }
    return 0;
}

int UpdatePlanner::parse_kv_list() {
    ScanTupleInfo& info = _plan_table_ctx->table_tuple_mapping[try_to_lower(_current_tables[0])];
    int64_t table_id = info.table_id;
    auto table_info_ptr = _factory->get_table_info_ptr(table_id); 
    if (table_info_ptr == nullptr) {
        DB_WARNING("table:%ld is nullptr", table_id);
        return -1;
    }
    TableInfo& table_info = *table_info_ptr;
    parser::Vector<parser::Assignment*> set_list = _update->set_list;
    std::set<int32_t> pk_field_ids;
    auto pk = _factory->get_index_info_ptr(table_id);
    if (pk == nullptr) {
        DB_WARNING("no pk found with id: %ld", table_id);
        return -1;
    }
    for (auto& field : pk->fields) {
        pk_field_ids.emplace(field.id);
    }
    std::set<int32_t> update_field_ids;
    for (int idx = 0; idx < set_list.size(); ++idx) {
        if (set_list[idx] == nullptr) {
            DB_WARNING("set item is nullptr");
            return -1;
        }
        std::string alias_name = get_field_alias_name(set_list[idx]->name);
        if (alias_name.empty()) {
            DB_WARNING("get_field_alias_name failed: %s", set_list[idx]->name->to_string().c_str());
            return -1;
        }
        std::string full_name = alias_name;
        full_name += ".";
        full_name += set_list[idx]->name->name.to_lower();
        FieldInfo* field_info = nullptr;
        if (nullptr == (field_info = get_field_info_ptr(full_name))) {
            DB_WARNING("invalid field name in");
            return -1;
        }
        auto slot = get_scan_ref_slot(alias_name, field_info->table_id, field_info->id, field_info->type);
        _update_slots.push_back(slot);
        update_field_ids.insert(field_info->id);
        // 更新分区键,走全局索引流程
        if (table_info.partition_ptr != nullptr && table_info.partition_ptr->partition_field_id() == field_info->id) {
            _ctx->execute_global_flow = true;
        }
        // 更新主键，走全局索引流程
        if (pk_field_ids.count(field_info->id) > 0) {
            _ctx->execute_global_flow = true;
        }

        pb::Expr value_expr;
        if (0 != create_expr_tree(set_list[idx]->expr, value_expr, CreateExprOptions())) {
            DB_WARNING("create update value expr failed");
            return -1;
        }
        if (field_info->on_update_value == "(current_timestamp())" 
                || field_info->default_value == "(current_timestamp())") {
            if (value_expr.nodes(0).node_type() == pb::NULL_LITERAL) {
                auto node = value_expr.mutable_nodes(0);
                node->set_num_children(0);
                node->set_node_type(pb::STRING_LITERAL);
                node->set_col_type(pb::STRING);
                node->mutable_derive_node()->set_string_val(ExprValue::Now().get_string());
            }
        } else if (value_expr.nodes(0).node_type() == pb::NULL_LITERAL
            && !field_info->can_null) {
            auto node = value_expr.mutable_nodes(0);
            node->set_num_children(0);
            node->set_node_type(pb::STRING_LITERAL);
            node->set_col_type(pb::STRING);
            node->mutable_derive_node()->set_string_val(field_info->default_value);
        }
        _update_values.push_back(value_expr);
    }
    for (auto& field : table_info.fields) {
        if (update_field_ids.count(field.id) != 0) {
            continue;
        }
        if (field.on_update_value == "(current_timestamp())") {
            pb::Expr value_expr;
            auto node = value_expr.add_nodes();
            node->set_num_children(0);
            node->set_node_type(pb::STRING_LITERAL);
            node->set_col_type(pb::STRING);
            node->mutable_derive_node()->set_string_val(ExprValue::Now().get_string());
            auto slot = get_scan_ref_slot(table_info.name, field.table_id, field.id, field.type);
            _update_slots.push_back(slot);
            _update_values.push_back(value_expr);
        }
    }
    return 0;
}

int UpdatePlanner::parse_where() {
    if (_update->where == nullptr) {
        DB_WARNING("update sql [%s] does not contain where conjunct", _ctx->sql.c_str());
        if (FLAGS_open_non_where_sql_forbid) {
            _ctx->stat_info.error_code = ER_SQL_REFUSE;
            _ctx->stat_info.error_msg << "update sql no where conditions";
            return -1;
        }
        return 0;
    }
    if (0 != flatten_filter(_update->where, _where_filters, CreateExprOptions())) {
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
