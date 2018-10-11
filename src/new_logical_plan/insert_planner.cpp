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

#include "insert_planner.h"
#include "expr_node.h"
#include "network_socket.h"

namespace baikaldb {
int InsertPlanner::plan() {
    create_packet_node(pb::OP_INSERT);
    pb::PlanNode* insert_node = _ctx->add_plan_node();

    _insert_stmt = (parser::InsertStmt*)(_ctx->stmt);
    insert_node->set_node_type(pb::INSERT_NODE);
    
    insert_node->set_limit(-1);
    insert_node->set_num_children(0); //TODO

    pb::DerivePlanNode* derive = insert_node->mutable_derive_node();
    pb::InsertNode* insert = derive->mutable_insert_node();
    insert->set_need_ignore(_insert_stmt->is_ignore);
    insert->set_is_replace(_insert_stmt->is_replace);
    
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
    set_dml_txn_state();
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
        DB_WARNING("db name is empty,sql:%s", _ctx->sql.c_str());
        return -1;
    }
    _ctx->stat_info.family = database;
    if (!_insert_stmt->table_name->table.empty()) {
        table = _insert_stmt->table_name->table.value;
    } else {
        return -1;
    }
    if (0 != add_table(database, table, alias)) {
        DB_WARNING("invalid database or table:%s.%s", database.c_str(), table.c_str());
        return -1;
    }
    TableInfo tbl_info = _table_info[database + "." + table];
    _table_id = tbl_info.id;
    node->set_table_id(_table_id);
    DB_DEBUG("db:%s, tbl:%s, tbl_id:%lu", database.c_str(), table.c_str(), _table_id);
    return 0;
}

int InsertPlanner::parse_kv_list() {
    for (int i = 0; i < _insert_stmt->on_duplicate.size(); ++i) {
        std::string full_name;
        if (_insert_stmt->on_duplicate[i]->name != nullptr) {
            full_name = get_field_full_name(_insert_stmt->on_duplicate[i]->name);
        }
        if (full_name.empty()) {
            DB_WARNING("get full field name failed: %s", _insert_stmt->on_duplicate[i]->name->name.value);
            return -1;
        }
        FieldInfo* field_info = nullptr;
        if (nullptr == (field_info = get_field_info(full_name))) {
            DB_WARNING("invalid field name in: %s", full_name.c_str());
            return -1;
        }
        auto slot = get_scan_ref_slot(field_info->table_id, field_info->id, field_info->type);
        _update_slots.push_back(slot);

        pb::Expr value_expr;
        if (0 != create_expr_tree(_insert_stmt->on_duplicate[i]->expr, value_expr)) {
            DB_WARNING("create update value expr failed");
            return -1;
        }
        _update_values.push_back(value_expr);
    }
    return 0;
}

int InsertPlanner::parse_field_list(pb::InsertNode* node) {
    TableInfo tbl = _factory->get_table_info(_table_id);
    if (tbl.id == -1) {
        DB_WARNING("no table found with id: %ld", _table_id);
        return -1;
    }
    if (_insert_stmt->columns.size() == 0) {
        _fields = tbl.fields;
        return 0;
    }
    std::set<int32_t> field_ids;
    for (int i = 0; i < _insert_stmt->columns.size(); ++i) {
        std::string full_name = get_field_full_name(_insert_stmt->columns[i]);
        if (full_name.empty()) {
            DB_WARNING("get full field name failed: %s", _insert_stmt->columns[i]->name.value);
            return -1;
        }
        FieldInfo* field_info = nullptr;
        if (nullptr == (field_info = get_field_info(full_name))) {
            DB_WARNING("invalid field name in: %s", full_name.c_str());
            return -1;
        }
        _fields.push_back(*field_info);
        field_ids.insert(field_info->id);
    }
    for (auto& field : tbl.fields) {
        if (field_ids.count(field.id) == 0) {
            _default_fields.push_back(field);
        }
    }
    return 0;
}

int InsertPlanner::parse_values_list(pb::InsertNode* node) {
    for (int i = 0; i < _insert_stmt->lists.size(); ++i) {
        parser::RowExpr* row_expr = _insert_stmt->lists[i];
        SmartRecord row = _factory->new_record(_table_id);
        uint32_t field_cnt = 0;
        for (; field_cnt < (uint32_t)row_expr->children.size(); ++field_cnt) {
            if (field_cnt >= _fields.size()) {
                DB_WARNING("more values than fields");
                break;
            }
            if (0 != fill_record_field((parser::ExprNode*)row_expr->children[field_cnt], row, _fields[field_cnt])) {
                DB_WARNING("fill_record_field fail, field_id:%d", _fields[field_cnt].id);
                return -1;
            }
        }
        if (field_cnt != _fields.size()) {
            DB_WARNING("values do not match with field_list");
            return -1;
        }
        for (auto& field : _default_fields) {
            if (field.default_value == "(current_timestamp())") {
                field.default_expr_value = ExprValue::Now();
                field.default_expr_value.cast_to(field.type);
            }
            if (0 != row->set_value(
                        row->get_field_by_tag(field.id), field.default_expr_value)) {
                DB_WARNING("fill insert value failed");
                return -1;
            }
        }
        _ctx->insert_records.push_back(row);
    }
    return 0;
}

int InsertPlanner::fill_record_field(const parser::ExprNode* parser_expr, SmartRecord record, FieldInfo& field) {
    pb::Expr value_expr;
    if (0 != create_expr_tree(parser_expr, value_expr)) {
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
    if (0 != record->set_value(record->get_field_by_tag(field.id), 
            expr->get_value(nullptr).cast_to(field.type))) {
        DB_WARNING("fill insert value failed");
        delete expr;
        return -1;
    }
    expr->close();
    delete expr;
    return 0;
}
} //namespace baikaldb
