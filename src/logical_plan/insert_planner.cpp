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

#include "insert_planner.h"
#include "union_planner.h"
#include "select_planner.h"
#include "expr_node.h"
#include "network_socket.h"
#include "hll_common.h"

namespace baikaldb {
int InsertPlanner::plan() {
    create_packet_node(pb::OP_INSERT);
    pb::PlanNode* insert_node = _ctx->add_plan_node();

    _insert_stmt = (parser::InsertStmt*)(_ctx->stmt);
    insert_node->set_node_type(pb::INSERT_NODE);
    
    insert_node->set_limit(-1);
    insert_node->set_is_explain(_ctx->is_explain);
    insert_node->set_num_children(0); //TODO

    pb::DerivePlanNode* derive = insert_node->mutable_derive_node();
    pb::InsertNode* insert = derive->mutable_insert_node();
    insert->set_need_ignore(_insert_stmt->is_ignore);
    insert->set_is_replace(_insert_stmt->is_replace);
    if (_ctx->row_ttl_duration > 0) {
        insert->set_row_ttl_duration(_ctx->row_ttl_duration);
        DB_DEBUG("row_ttl_duration: %ld", _ctx->row_ttl_duration);
    }
    for (int i = 0; i < _insert_stmt->partition_names.size(); ++i) {
        std::string lower_name = _insert_stmt->partition_names[i].value;
        std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(), ::tolower);
        _partition_names.emplace_back(lower_name);
    }
    // parse db.table in insert SQL
    if (0 != parse_db_table(insert)) {
        return -1;
    }
    if (0 != parse_kv_list()) {
        return -1;
    }
    create_scan_tuple_descs();
    create_values_tuple_desc();
    // add slots and exprs
    for (uint32_t idx = 0; idx < _update_slots.size(); ++idx) {
        insert->add_update_slots()->CopyFrom(_update_slots[idx]);
        insert->add_update_exprs()->CopyFrom(_update_values[idx]);
    }
    if (_scan_tuples.size() > 0) {
        insert->set_tuple_id(_scan_tuples[0].tuple_id());
    } else {
        insert->set_tuple_id(-1);
    }
    insert->set_values_tuple_id(_values_tuple_info.tuple_id);
    //parse field list corresponds to the values
    if (0 != parse_field_list(insert)) {
        return -1;
    }
    // parse records to be inserted
    if (0 != parse_values_list(insert)) {
        return -1;
    }
    if (0 != gen_select_plan()) {
        return -1;
    }

    _ctx->prepared_table_id = _table_id;
    set_dml_txn_state(_table_id);
    // 局部索引binlog处理标记
    if (_ctx->open_binlog && !_factory->has_global_index(_table_id)) {
        insert_node->set_local_index_binlog(true);
    }
    return 0;
}

int InsertPlanner::gen_select_plan() {
    if (_insert_stmt->subquery_stmt != nullptr) {
        parser::DmlNode* subquery = _insert_stmt->subquery_stmt;
        int ret = gen_subquery_plan(subquery, _plan_table_ctx, ExprParams());
        if (ret < 0) {
            return -1;
        }
        if (_fields.size() != _select_names.size()) {
            _ctx->stat_info.error_code = ER_WRONG_VALUE_COUNT_ON_ROW;
            _ctx->stat_info.error_msg << "Column count doesn't match value count at row 1";
            return -1;            
        }
        _ctx->add_sub_ctx(_cur_sub_ctx);
    }
    return 0;
}

int InsertPlanner::parse_db_table(pb::InsertNode* node) {
    std::string database;
    std::string table;
    std::string alias;
    if (!_insert_stmt->table_name->db.empty()) {
        database = _insert_stmt->table_name->db.value;
    } else if (!_ctx->cur_db.empty()) {
        database = _ctx->cur_db;
    } else {
        _ctx->stat_info.error_code = ER_NO_DB_ERROR;
        _ctx->stat_info.error_msg << "No database selected";
        DB_WARNING("db name is empty,sql:%s", _ctx->sql.c_str());
        return -1;
    }
    if (!_insert_stmt->table_name->table.empty()) {
        table = _insert_stmt->table_name->table.value;
    } else {
        return -1;
    }
    _ctx->stat_info.family = database;
    _ctx->stat_info.table = table;
    if (0 != add_table(database, table, alias, false)) {
        DB_WARNING("invalid database or table:%s.%s", database.c_str(), table.c_str());
        return -1;
    }
    _table_id = _plan_table_ctx->table_info[try_to_lower(database + "." + table)]->id;
    node->set_table_id(_table_id);
    //DB_WARNING("db:%s, tbl:%s, tbl_id:%lu", database.c_str(), table.c_str(), _table_id);
    return 0;
}

int InsertPlanner::parse_kv_list() {
    if (_insert_stmt->on_duplicate.size() == 0) {
        return 0;
    }
    auto tbl_ptr = _factory->get_table_info_ptr(_table_id);
    if (tbl_ptr == nullptr) {
        DB_WARNING("no table found with id: %ld", _table_id);
        return -1;
    }
    std::set<int32_t> update_field_ids;
    for (int i = 0; i < _insert_stmt->on_duplicate.size(); ++i) {
        if (_insert_stmt->on_duplicate[i]->name == nullptr) {
            DB_WARNING("on_duplicate name[%d] is enmty", i);
            return -1;
        }
        std::string alias_name = get_field_alias_name(_insert_stmt->on_duplicate[i]->name);
        if (alias_name.empty()) {
            DB_WARNING("get_field_alias_name failed: %s", 
                    _insert_stmt->on_duplicate[i]->name->to_string().c_str());
            return -1;
        }
        std::string full_name = alias_name;
        full_name += ".";
        full_name += _insert_stmt->on_duplicate[i]->name->name.to_lower();

        FieldInfo* field_info = nullptr;
        if (nullptr == (field_info = get_field_info_ptr(full_name))) {
            DB_WARNING("invalid field name in: %s", full_name.c_str());
            return -1;
        }
        auto slot = get_scan_ref_slot(alias_name, 
                field_info->table_id, field_info->id, field_info->type);
        _update_slots.emplace_back(slot);
        update_field_ids.insert(field_info->id);

        pb::Expr value_expr;
        if (0 != create_expr_tree(_insert_stmt->on_duplicate[i]->expr, value_expr, CreateExprOptions())) {
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
        }
        _update_values.emplace_back(value_expr);
    }
    for (auto& field : tbl_ptr->fields) {
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
            auto slot = get_scan_ref_slot(tbl_ptr->name, field.table_id, field.id, field.type);
            _update_slots.emplace_back(slot);
            _update_values.emplace_back(value_expr);
        }
    }
    return 0;
}

int InsertPlanner::parse_field_list(pb::InsertNode* node) {
    auto tbl_ptr = _factory->get_table_info_ptr(_table_id);
    if (tbl_ptr == nullptr) {
        DB_WARNING("no table found with id: %ld", _table_id);
        return -1;
    }
    auto& tbl = *tbl_ptr;
    if (_insert_stmt->columns.size() == 0) {
        _fields = tbl.fields;
        for (auto& field : _fields) {
            node->add_field_ids(field.id);
        }
        return 0;
    }
    std::set<int32_t> field_ids;
    for (int i = 0; i < _insert_stmt->columns.size(); ++i) {
        std::string alias_name = get_field_alias_name(_insert_stmt->columns[i]);
        if (alias_name.empty()) {
            DB_WARNING("get_field_alias_name failed: %s", _insert_stmt->columns[i]->to_string().c_str());
            return -1;
        }
        std::string full_name = alias_name;
        full_name += ".";
        full_name += _insert_stmt->columns[i]->name.to_lower();

        FieldInfo* field_info = nullptr;
        if (nullptr == (field_info = get_field_info_ptr(full_name))) {
            DB_WARNING("invalid field name in: %s", full_name.c_str());
            return -1;
        }
        _fields.emplace_back(*field_info);
        field_ids.insert(field_info->id);
        node->add_field_ids(field_info->id);
    }
    for (auto& field : tbl.fields) {
        if (field_ids.count(field.id) == 0) {
            _default_fields.emplace_back(field);
        }
    }
    //DB_WARNING("insert_node:%s", node->DebugString().c_str());
    return 0;
}

int InsertPlanner::parse_values_list(pb::InsertNode* node) {
    for (int i = 0; i < _insert_stmt->lists.size(); ++i) {
        parser::RowExpr* row_expr = _insert_stmt->lists[i];
        if ((size_t)row_expr->children.size() != _fields.size()) {
            _ctx->stat_info.error_code = ER_WRONG_VALUE_COUNT_ON_ROW;
            _ctx->stat_info.error_msg << "Column count doesn't match value count";
            DB_WARNING("values do not match with field_list");
            return -1;
        }
        if (_ctx->new_prepared) {
            for (size_t idx = 0; idx < (size_t)row_expr->children.size(); ++idx) {
                pb::Expr* expr = node->add_insert_values();
                if (0 != create_expr_tree(row_expr->children[idx], *expr, CreateExprOptions())) {
                    DB_WARNING("create insertion value expr failed");
                    return -1;
                }
                if (expr->nodes_size() <= 0) {
                    DB_WARNING("expr is empty");
                    return -1;
                }
            }
        } else {
            SmartRecord row = _factory->new_record(_table_id);
            if (row == nullptr) {
                DB_WARNING("table :%ld is deleted", _table_id);
                _ctx->stat_info.error_code = ER_NO_SUCH_TABLE;
                _ctx->stat_info.error_msg << "table not exist";
                return -1;
            }
            for (size_t idx = 0; idx < (size_t)row_expr->children.size(); ++idx) {
                if (0 != fill_record_field((parser::ExprNode*)row_expr->children[idx], row, _fields[idx])) {
                    DB_WARNING("fill_record_field fail, field_id:%d", _fields[idx].id);
                    return -1;
                }
            }
            for (auto& field : _default_fields) {
                if (0 != _factory->fill_default_value(row, field)) {
                    return -1;
                }
            }
            _ctx->insert_records.emplace_back(row);
        }
    }
    return 0;
}

int InsertPlanner::fill_record_field(const parser::ExprNode* parser_expr, SmartRecord record, FieldInfo& field) {
    pb::Expr value_expr;
    if (0 != create_expr_tree(parser_expr, value_expr, CreateExprOptions())) {
        DB_WARNING("create insertion value expr failed");
        return -1;
    }
    if (value_expr.nodes_size() <= 0) {
        DB_WARNING("node size = 0");
        return -1;
    }
    ExprNode* expr = nullptr;
    if (0 != ExprNode::create_tree(value_expr, &expr)) {
        DB_WARNING("create insertion mem expr failed");
        return -1;
    }
    if (0 != expr->type_inferer()) {
        DB_WARNING("expr type_inferer fail");
        return -1;
    }
    if (!expr->is_constant()) {
        DB_WARNING("expr must be constant");
        return -1;
    }
    if (0 != expr->open()) {
        DB_WARNING("expr open fail");
        return -1;
    }
    ExprValue value = expr->get_value(nullptr);
    // 20190101101112 这种转换现在只支持string类型
    if (is_datetime_specic(field.type) && value.is_numberic()) {
        value.cast_to(pb::STRING).cast_to(field.type);
    } else {
        value.cast_to(field.type);
    }
    expr->close();
    delete expr;
    // fill default
    if (value.is_null()) {
        return _factory->fill_default_value(record, field);
    }
    if (field.type == pb::HLL) {
        if (hll::hll_raw_to_sparse(value.str_val) < 0) {
            DB_WARNING("hll raw to sparse failed");
            return -1;
        }
    }
    if (0 != record->set_value(record->get_field_by_tag(field.id), value)) {
        DB_WARNING("fill insert value failed");
        return -1;
    }
    return 0;
}
} //namespace baikaldb
