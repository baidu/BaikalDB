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

#pragma once
#include "exec_node.h"
#include "joiner.h"
#include "mut_table_key.h"
#ifdef BAIDU_INTERNAL 
#include <base/containers/flat_map.h>
#else
#include <butil/containers/flat_map.h>
#endif
#include "slot_ref.h"

namespace baikaldb {

class JoinNode : public Joiner {
public:
    JoinNode() {
    }
    virtual  ~JoinNode() { }
    virtual int init(const pb::PlanNode& node);
    virtual int predicate_pushdown(std::vector<ExprNode*>& input_exprs);
    virtual void transfer_pb(int64_t region_id, pb::PlanNode* pb_node);
    virtual int open(RuntimeState* state);
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos);

    void convert_to_inner_join(std::vector<ExprNode*>& input_exprs);
    int get_next_for_hash_outer_join(RuntimeState* state, RowBatch* batch, bool* eos);
    int get_next_for_hash_inner_join(RuntimeState* state, RowBatch* batch, bool* eos);
    int get_next_for_loop_hash_inner_join(RuntimeState* state, RowBatch* batch, bool* eos);
    int get_next_for_nested_loop_join(RuntimeState* state, RowBatch* batch, bool* eos);
    bool outer_contains_expr(ExprNode* expr) {
        return expr_in_tuple_ids(_outer_tuple_ids, expr);
    }
    bool inner_contains_expr(ExprNode* expr) {
        return expr_in_tuple_ids(_inner_tuple_ids, expr);
    }
    bool contains_expr(ExprNode* expr) {
        std::unordered_set<int32_t> tuple_ids = _outer_tuple_ids;
        for (auto tuple_id : _inner_tuple_ids) {
            tuple_ids.insert(tuple_id);
        }
        return expr_in_tuple_ids(tuple_ids, expr);
    }
    bool expr_in_tuple_ids(std::unordered_set<int32_t>& tuple_ids, ExprNode* expr) {
        std::unordered_set<int32_t> related_tuple_ids;
        expr->get_all_tuple_ids(related_tuple_ids);
        for (auto& related_tuple_id : related_tuple_ids) {
            if (tuple_ids.count(related_tuple_id) == 0) {
                return false;
            }
        }
        return true;
    }
    ExecNode* get_inner_node() {
        if (_children.size() < 2) {
            return nullptr;
        }
        ExecNode* inner_node = _children[1];
        if (join_type() == pb::RIGHT_JOIN) {
            inner_node = _children[0];
        }
        return inner_node;
    }
    int hash_join(RuntimeState* state);
    int loop_hash_join(RuntimeState* state);

    int nested_loop_join(RuntimeState* state);

    void reorder_clear() {
        _conditions.clear();
        for (auto& child : _children) {
            if (child->node_type() == pb::JOIN_NODE) {
                static_cast<JoinNode*>(child)->reorder_clear();
            } else {
                child = nullptr;
            }
        }
    }

    bool need_reorder(
            std::map<int32_t, ExecNode*>& tuple_join_child_map,
            std::map<int32_t, std::set<int32_t>>& tuple_equals_map, 
            std::vector<int32_t>& tuple_order,
            std::vector<ExprNode*>& conditions);
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
