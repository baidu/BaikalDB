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

#include "insert_node.h"
#include "runtime_state.h"
#include <unordered_set>

namespace baikaldb {
int InsertNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    const pb::InsertNode& insert_node = node.derive_node().insert_node();
    _table_id = insert_node.table_id();
    _global_index_id = _table_id;//?
    _tuple_id = insert_node.tuple_id();
    _values_tuple_id = insert_node.values_tuple_id();
    _is_replace = insert_node.is_replace();
    _is_merge = insert_node.is_merge();
    _row_ttl_duration = insert_node.row_ttl_duration();
    DB_DEBUG("_row_ttl_duration:%ld", _row_ttl_duration);
    _need_ignore = insert_node.need_ignore();
    _ddl_need_write = insert_node.ddl_need_write();
    _ddl_index_id = insert_node.ddl_index_id();
    for (auto& slot : insert_node.update_slots()) {
        _update_slots.emplace_back(slot);
    }
    for (auto& expr : insert_node.update_exprs()) {
        ExprNode* up_expr = nullptr;
        ret = ExprNode::create_tree(expr, &up_expr);
        if (ret < 0) {
            return ret;
        }
        _update_exprs.emplace_back(up_expr);
    }
    for (auto id : insert_node.field_ids()) {
        _selected_field_ids.emplace_back(id);
    }

    for (auto& expr : insert_node.insert_values()) {
        ExprNode* value_expr = nullptr;
        ret = ExprNode::create_tree(expr, &value_expr);
        if (ret < 0) {
            return ret;
        }
        _insert_values.emplace_back(value_expr);
    }
    // insert_values 只在db上使用
    // pb清掉没关系，不会发给store
    if (!_insert_values.empty()) {
        pb::DerivePlanNode* derive = _pb_node.mutable_derive_node();
        pb::InsertNode* insert = derive->mutable_insert_node();
        insert->clear_insert_values();
    }
    _on_dup_key_update = _update_slots.size() > 0;
    _local_index_binlog = node.local_index_binlog();
    return 0;
}

int InsertNode::open(RuntimeState* state) {
    int num_affected_rows = 0;
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), OPEN_TRACE, ([this, &num_affected_rows](TraceLocalNode& local_node) {
        local_node.set_affect_rows(num_affected_rows);
        local_node.append_description() << " increase_rows:" + _num_increase_rows;
    }));
    
    int ret = 0;
    ret = ExecNode::open(state);
    if (ret < 0) {
        DB_WARNING_STATE(state, "ExecNode::open fail:%d", ret);
        return ret;
    }
    if (_is_explain) {
        return 0;
    }
    for (auto expr : _update_exprs) {
        ret = expr->open();
        if (ret < 0) {
            DB_WARNING_STATE(state, "expr open fail, ret:%d", ret);
            return ret;
        }
    }
    ret = init_schema_info(state);
    if (ret == -1) {
        DB_WARNING_STATE(state, "init schema failed fail:%d", ret);
        return ret;
    }

    // cstore下只更新涉及列
    if (_is_replace && _table_info->engine == pb::ROCKSDB_CSTORE) {
        for (auto& field_info : _table_info->fields) {
            if (_pri_field_ids.count(field_info.id) == 0 &&
                    _update_field_ids.count(field_info.id) == 0) {
                _update_field_ids.insert(field_info.id);
            }
        }
    }
    int cnt = 0;
    // TODO init阶段？
    for (auto& pb_record : _pb_node.derive_node().insert_node().records()) {
        SmartRecord record = _factory->new_record(*_table_info);
        record->decode(pb_record);
        _records.push_back(record);
        cnt++;
    }
    //DB_WARNING_STATE(state, "insert_size:%d", cnt);
    if (_on_dup_key_update) {
        _dup_update_row = state->mem_row_desc()->fetch_mem_row();
        if (_tuple_id >= 0) {
            _tuple_desc = state->get_tuple_desc(_tuple_id);
        }
        if (_values_tuple_id >= 0) {
            _values_tuple_desc = state->get_tuple_desc(_values_tuple_id);
        }
    }

    for (auto& record : _records) {
        ret = insert_row(state, record);
        if (ret < 0) {
            DB_WARNING_STATE(state, "insert_row fail");
            return -1;
        }
        num_affected_rows += ret;
    }
    // auto_rollback.release();
    // txn->commit();
    _txn->batch_num_increase_rows = _num_increase_rows;
    state->set_num_increase_rows(_num_increase_rows);
    return num_affected_rows;
}

void InsertNode::transfer_pb(int64_t region_id, pb::PlanNode* pb_node) {
    ExecNode::transfer_pb(region_id, pb_node);
    auto insert_node = pb_node->mutable_derive_node()->mutable_insert_node();
    insert_node->clear_update_exprs();
    for (auto expr : _update_exprs) {
        ExprNode::create_pb_expr(insert_node->add_update_exprs(), expr);
    }
    if (region_id == 0 || _insert_records_by_region.count(region_id) == 0) {
        return;
    }
    std::vector<SmartRecord>& records = _insert_records_by_region[region_id];
    insert_node->clear_records();
    for (auto& record : records) {
        std::string* str = insert_node->add_records();
        record->encode(*str);
    }
    auto table_info = _factory->get_table_info_ptr(_table_id);
    if (table_info == nullptr) {
        DB_WARNING("no table found with table_id: %ld", _table_id);
        return;
    }
    for (const auto index_id : table_info->indices) {
        auto info_ptr = _factory->get_index_info_ptr(index_id);
        if (info_ptr == nullptr) {
            DB_WARNING("no index info found with index_id: %ld", index_id);
            continue;
        }
        if (!info_ptr->is_global && (info_ptr->state == pb::IS_WRITE_ONLY || info_ptr->state == pb::IS_WRITE_LOCAL)) {
            insert_node->set_ddl_need_write(true);
            insert_node->set_ddl_index_id(index_id);
        }
    }
}

int InsertNode::expr_optimize(QueryContext* ctx) {
    int ret = 0;
    ret = DMLNode::expr_optimize(ctx);
    if (ret < 0) {
        DB_WARNING("expr type_inferer fail:%d", ret);
        return ret;
    }
    for (auto expr : _insert_values) {
        ret = expr->expr_optimize();
        if (ret < 0) {
            DB_WARNING("expr type_inferer fail:%d", ret);
            return ret;
        }
        if (!expr->is_constant()) {
            DB_WARNING("insert expr must be constant");
            return -1;
        }
    }
    return 0;
}

int InsertNode::insert_values_for_prepared_stmt(std::vector<SmartRecord>& insert_records) {
    if (_selected_field_ids.size() == 0) {
        DB_WARNING("not execute a prepared stmt");
        return 0;
    }
    if ((_insert_values.size() % _selected_field_ids.size()) != 0) {
        DB_WARNING("_selected_field_ids should not be empty()");
        return -1;
    }
    auto tbl_ptr = _factory->get_table_info_ptr(_table_id);
    if (tbl_ptr == nullptr) {
        DB_WARNING("no table found with table_id: %ld", _table_id);
        return -1;
    }
    auto& tbl = *tbl_ptr;
    std::unordered_map<int32_t, FieldInfo*> table_field_map;
    std::unordered_set<int32_t> insert_prepared_field_ids;
    std::vector<FieldInfo*>  insert_fields;
    std::vector<FieldInfo*>  default_fields;

    for (auto& field : tbl.fields) {
        table_field_map.insert({field.id, &field});
    }
    for (auto id : _selected_field_ids) {
        if (table_field_map.count(id) == 0) {
            DB_WARNING("No field for field id: %d", id);
            return -1;
        }
        insert_prepared_field_ids.insert(id);
        insert_fields.push_back(table_field_map[id]);
    }
    for (auto& field : tbl.fields) {
        if (insert_prepared_field_ids.count(field.id) == 0) {
            default_fields.push_back(&field);
        }
    }
    size_t row_size = _insert_values.size() / _selected_field_ids.size();
    for (size_t row_idx = 0; row_idx < row_size; ++row_idx) {
        SmartRecord row = _factory->new_record(_table_id);
        for (size_t col_idx = 0; col_idx < _selected_field_ids.size(); ++col_idx) {
            size_t idx = row_idx * _selected_field_ids.size() + col_idx;
            ExprNode* expr = _insert_values[idx];
            if (0 != expr->open()) {
                DB_WARNING("expr open fail");
                return -1;
            }
            if (0 != row->set_value(row->get_field_by_idx(insert_fields[col_idx]->pb_idx),
                  expr->get_value(nullptr).cast_to(insert_fields[col_idx]->type))) {
                DB_WARNING("fill insert value failed");
                expr->close();
                return -1;
            }
            //DB_WARNING("expr type:%d field type: %d", expr->node_type(), insert_fields[col_idx].type);
            expr->close();
        }
        for (auto& field : default_fields) {
            if (0 != _factory->fill_default_value(row, *field)) {
                    return -1;
            }
        }

        //DB_WARNING("DEBUG row: %s", row->to_string().c_str());
        insert_records.push_back(row);
    }
    /*
    for (auto expr : _insert_values) {
        ExprNode::destroy_tree(expr);
    }
    _insert_values.clear();
    */
    return 0;
}

void InsertNode::find_place_holder(std::unordered_multimap<int, ExprNode*>& placeholders) {
    DMLNode::find_place_holder(placeholders);
    for (auto& expr : _insert_values) {
        expr->find_place_holder(placeholders);
    }
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
