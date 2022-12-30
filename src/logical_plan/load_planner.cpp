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

#include "load_planner.h"
#include "union_planner.h"
#include "select_planner.h"
#include "expr_node.h"
#include "network_socket.h"

#include <sys/stat.h>  
#include <unistd.h>

namespace baikaldb {

int LoadPlanner::plan() {
    if (_ctx->client_conn->txn_id != 0) {
        if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
            _ctx->stat_info.error_code = ER_NOT_ALLOWED_COMMAND;
            _ctx->stat_info.error_msg.str("not allow load data in transaction");
        }
        DB_FATAL("not allowed load data in txn connection txn_id:%lu",
            _ctx->client_conn->txn_id);
        return -1;
    }
    _ctx->client_conn->not_in_load_data = false;
    create_packet_node(pb::OP_LOAD);
    pb::PlanNode* plan_node = _ctx->add_plan_node();

    _load_stmt = (parser::LoadDataStmt*)(_ctx->stmt);
    plan_node->set_node_type(pb::LOAD_NODE);
    
    plan_node->set_limit(-1);
    plan_node->set_is_explain(_ctx->is_explain);
    plan_node->set_num_children(1);

    pb::DerivePlanNode* derive = plan_node->mutable_derive_node();
    pb::LoadNode* load_node = derive->mutable_load_node();


    pb::PlanNode* insert_node = _ctx->add_plan_node();
    insert_node->set_node_type(pb::INSERT_NODE);
    insert_node->set_limit(-1);
    insert_node->set_is_explain(_ctx->is_explain);
    insert_node->set_num_children(0);
    derive = insert_node->mutable_derive_node();
    pb::InsertNode* insert = derive->mutable_insert_node();

    if (_load_stmt->on_duplicate_handle == parser::ON_DUPLICATE_KEY_ERROR
       || _load_stmt->on_duplicate_handle == parser::ON_DUPLICATE_KEY_IGNORE) {
        insert->set_need_ignore(true);
    } else if (_load_stmt->on_duplicate_handle == parser::ON_DUPLICATE_KEY_REPLACE) {
        insert->set_is_replace(true);
        insert->set_need_ignore(false);
    }
    insert->set_values_tuple_id(_values_tuple_info.tuple_id);
    if (0 != parse_load_info(load_node, insert)) {
        return -1;
    }
    create_scan_tuple_descs();
    create_values_tuple_desc();
    // add slots and exprs
    for (uint32_t idx = 0; idx < _set_slots.size(); ++idx) {
        load_node->add_set_slots()->CopyFrom(_set_slots[idx]);
        load_node->add_set_exprs()->CopyFrom(_set_values[idx]);
    }
    if (_scan_tuples.size() > 0) {
        insert->set_tuple_id(_scan_tuples[0].tuple_id());
    } else {
        insert->set_tuple_id(-1);
    }
 
    if (0 != parse_field_list(load_node, insert)) {
        return -1;
    }

    if (0 != parse_set_list(load_node)) {
        return -1;
    }

    set_dml_txn_state(_table_id);
    // 局部索引binlog处理标记
    if (_ctx->open_binlog && !_factory->has_global_index(_table_id)) {
        insert_node->set_local_index_binlog(true);
    }
    return 0;
}

int LoadPlanner::parse_load_info(pb::LoadNode* node, pb::InsertNode* insert_node) {
    std::string database;
    std::string table;
    std::string alias;
    if (!_load_stmt->table_name->db.empty()) {
        database = _load_stmt->table_name->db.value;
    } else if (!_ctx->cur_db.empty()) {
        database = _ctx->cur_db;
    } else {
        _ctx->stat_info.error_code = ER_NO_DB_ERROR;
        _ctx->stat_info.error_msg << "No database selected";
        DB_WARNING("db name is empty,sql:%s", _ctx->sql.c_str());
        return -1;
    }
    if (!_load_stmt->table_name->table.empty()) {
        table = _load_stmt->table_name->table.value;
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
    insert_node->set_table_id(_table_id);
    std::string tmp_path = _load_stmt->path.to_string();
    struct stat stat_buf;
    int ret = stat(tmp_path.c_str(), &stat_buf);
    if (ret < 0) {
        DB_FATAL("path:%s not exists", tmp_path.c_str());
        _ctx->stat_info.error_code = ER_NO_DB_ERROR;
        _ctx->stat_info.error_msg << "file " << tmp_path << " not exists";
        return -1;
    }
    if (!S_ISREG(stat_buf.st_mode)) {
        DB_FATAL("path:%s not a file", tmp_path.c_str());
        _ctx->stat_info.error_code = ER_NO_DB_ERROR;
        _ctx->stat_info.error_msg << "path " << tmp_path << " not a file";
        return -1;
    }
    node->set_file_size(stat_buf.st_size);
    node->set_data_path(tmp_path);
    
    node->set_char_set(pb::UTF8);
    if (!_load_stmt->char_set.empty()) {
        if (_load_stmt->char_set.to_lower() == "gbk") {
            node->set_char_set(pb::GBK);
        }
    }

    if (_load_stmt->fields_info != nullptr) {
        parser::FieldsClause* fields_info = _load_stmt->fields_info;
        if (!fields_info->terminated.empty()) {
            node->set_terminated(fields_info->terminated.to_string());
        }
        if (!fields_info->enclosed.empty()) {
            node->set_enclosed(fields_info->enclosed.to_string());
        }
        if (!fields_info->escaped.empty()) {
            node->set_escaped(fields_info->escaped.to_string());
        }
        node->set_opt_enclosed(fields_info->opt_enclosed);
    }
    if (_load_stmt->lines_info != nullptr) {
        parser::LinesClause* lines_info = _load_stmt->lines_info;
        if (!lines_info->starting.empty()) {
            node->set_line_starting(lines_info->starting.to_string());
        }
        if (!lines_info->terminated.empty()) {
            node->set_line_terminated(lines_info->terminated.to_string());
        }
    }
    node->set_ignore_lines(_load_stmt->ignore_lines);
    return 0;
}

int LoadPlanner::parse_field_list(pb::LoadNode* node, pb::InsertNode* insert_node) {
    auto tbl_ptr = _factory->get_table_info_ptr(_table_id);
    if (tbl_ptr == nullptr) {
        DB_WARNING("no table found with id: %ld", _table_id);
        return -1;
    }
    auto& tbl = *tbl_ptr;
    if (_load_stmt->columns.size() == 0) {
        for (auto& field : tbl.fields) {
            node->add_field_ids(field.id);
            insert_node->add_field_ids(field.id);
        }
        return 0;
    }
    std::set<int32_t> field_ids;
    for (int i = 0; i < _load_stmt->columns.size(); ++i) {
        // 用@dummy标记需要丢弃的列
        if (_load_stmt->columns[i]->name.to_lower() == "@dummy") {
            node->add_ingore_field_indexes(i);
            continue;
        }
        std::string alias_name = get_field_alias_name(_load_stmt->columns[i]);
        if (alias_name.empty()) {
            DB_WARNING("get_field_alias_name failed: %s", _load_stmt->columns[i]->to_string().c_str());
            return -1;
        }
        std::string full_name = alias_name;
        full_name += ".";
        full_name += _load_stmt->columns[i]->name.to_lower();

        FieldInfo* field_info = nullptr;
        if (nullptr == (field_info = get_field_info_ptr(full_name))) {
            DB_WARNING("invalid field name in: %s", full_name.c_str());
            return -1;
        }
        node->add_field_ids(field_info->id);
        insert_node->add_field_ids(field_info->id);
        field_ids.emplace(field_info->id);
    }
    for (auto& field : tbl.fields) {
        if (field_ids.count(field.id) == 0) {
            node->add_default_field_ids(field.id);
        }
    }
    return 0;
}

int LoadPlanner::parse_set_list(pb::LoadNode* node) {
    for (int i = 0; i < _load_stmt->set_list.size(); ++i) {
        if (_load_stmt->set_list[i]->name == nullptr) {
            DB_WARNING("set_list name[%d] is enmty", i);
            return -1;
        }
        std::string alias_name = get_field_alias_name(_load_stmt->set_list[i]->name);
        if (alias_name.empty()) {
            DB_WARNING("get_field_alias_name failed: %s", 
                    _load_stmt->set_list[i]->name->to_string().c_str());
            return -1;
        }
        std::string full_name = alias_name;
        full_name += ".";
        full_name += _load_stmt->set_list[i]->name->name.to_lower();

        FieldInfo* field_info = nullptr;
        if (nullptr == (field_info = get_field_info_ptr(full_name))) {
            DB_WARNING("invalid field name in: %s", full_name.c_str());
            return -1;
        }
        auto slot = get_scan_ref_slot(alias_name, 
                field_info->table_id, field_info->id, field_info->type);
        _set_slots.emplace_back(slot);

        pb::Expr set_expr;
        if (0 != create_expr_tree(_load_stmt->set_list[i]->expr, set_expr, CreateExprOptions())) {
            DB_WARNING("create set value expr failed");
            return -1;
        }
        _set_values.emplace_back(set_expr);
    }
    return 0;
}

} //namespace baikaldb
