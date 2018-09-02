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

#include "insert_node.h"
#include "runtime_state.h"

namespace baikaldb {
DECLARE_bool(disable_writebatch_index);
int InsertNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    _table_id = node.derive_node().insert_node().table_id();
    _tuple_id = node.derive_node().insert_node().tuple_id();
    _values_tuple_id = node.derive_node().insert_node().values_tuple_id();
    _need_ignore = node.derive_node().insert_node().need_ignore();
    for (auto& slot : node.derive_node().insert_node().update_slots()) {
        _update_slots.push_back(slot);
    }
    for (auto& expr : node.derive_node().insert_node().update_exprs()) {
        ExprNode* up_expr = nullptr;
        ret = ExprNode::create_tree(expr, &up_expr);
        if (ret < 0) {
            return ret;
        }
        _update_exprs.push_back(up_expr);
    }
    _on_dup_key_update = _update_slots.size() > 0;
    return 0;
}
int InsertNode::open(RuntimeState* state) {
    int ret = 0;
    ret = ExecNode::open(state);
    if (ret < 0) {
        DB_WARNING_STATE(state, "ExecNode::open fail:%d", ret);
        return ret;
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
    int cnt = 0;
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

    int num_affected_rows = 0;
    AtomicManager<std::atomic<long>> ams[state->reverse_index_map().size()];
    int i = 0;
    for (auto& pair : state->reverse_index_map()) {
        pair.second->sync(ams[i]);
        i++;
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
    state->set_num_increase_rows(_num_increase_rows);
    return num_affected_rows;
}

void InsertNode::transfer_pb(pb::PlanNode* pb_node) {
    ExecNode::transfer_pb(pb_node);
    auto insert_node = pb_node->mutable_derive_node()->mutable_insert_node();
    insert_node->clear_update_exprs();
    for (auto expr : _update_exprs) {
        ExprNode::create_pb_expr(insert_node->add_update_exprs(), expr);
    }
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
