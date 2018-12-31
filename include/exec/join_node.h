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

#pragma once
#include "exec_node.h"
#include "mut_table_key.h"
#ifdef BAIDU_INTERNAL 
#include <base/containers/flat_map.h>
#else
#include <butil/containers/flat_map.h>
#endif
#include "slot_ref.h"

namespace baikaldb {
class JoinNode : public ExecNode {
public:
    JoinNode() : _child_eos(false) {
    }
    virtual  ~JoinNode() {
        for (auto& condition : _conditions) {
            ExprNode::destroy_tree(condition);
        }
        for (auto& condition : _have_removed) {
            ExprNode::destroy_tree(condition);
        }
    }
    virtual int init(const pb::PlanNode& node); 
    virtual int expr_optimize(std::vector<pb::TupleDescriptor>* tuple_descs);
    virtual void find_place_holder(std::map<int, ExprNode*>& placeholders);
    virtual int predicate_pushdown(std::vector<ExprNode*>& input_exprs);
    virtual void transfer_pb(int64_t region_id, pb::PlanNode* pb_node);
    virtual int open(RuntimeState* state);
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos);
    virtual void close(RuntimeState* state);
    virtual std::vector<ExprNode*>* mutable_conjuncts() {
        return &_conditions; 
    }
    void convert_to_inner_join(std::vector<ExprNode*>& input_exprs);
    int get_next_for_other_join(RuntimeState* state, RowBatch* batch, bool* eos);
    int get_next_for_inner_join(RuntimeState* state, RowBatch* batch, bool* eos);
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
    
    pb::JoinType join_type() {
        return _join_type;
    }
    void set_join_type(pb::JoinType join_type) {
        _join_type = join_type;
        _pb_node.mutable_derive_node()->mutable_join_node()->set_join_type(join_type);
    }
    //virtual void print_exec_node() {
    //    ExecNode::print_exec_node();
    //    DB_WARNING("join_node, join_type:%s", pb::JoinType_Name(_join_type).c_str());
    //    pb::PlanNode join_node;
    //    transfer_pb(&join_node);
    //    DB_WARNING("join_node:%s", join_node.DebugString().c_str());
    //    for (auto left_tuple_id : _left_tuple_ids) {
    //        DB_WARNING("join node, left_tuple_id:%d", left_tuple_id);
    //    }   
    //    for (auto right_tuple_id : _right_tuple_ids) {
    //        DB_WARNING("join node, right_tuple_id:%d", right_tuple_id);
    //    }   
    //}  
private:
    bool _satisfy_filter(MemRow* row);
    int _fill_equal_slot();
    bool _is_equal_condition(ExprNode* expr);
    int _construct_in_condition(std::vector<ExprNode*>& slot_refs,
                                  std::vector<std::vector<ExprValue>>& in_values,
                                  std::vector<ExprNode*>& in_exprs);
    int _fetcher_join_table(RuntimeState* state, ExecNode* child_node,
                            std::vector<MemRow*>& tuple_data);
    void _construct_hash_map(const std::vector<MemRow*>& tuple_data,
                             const std::vector<ExprNode*>& slot_refs);
    void _encode_hash_key(MemRow* row,
                          const std::vector<ExprNode*>& slot_ref_exprs,
                          MutTableKey& key);
    void _save_join_value(const std::vector<MemRow*>& tuple_data,
                          const std::vector<ExprNode*>& slot_ref_exprs);

    int _construct_result_batch(RowBatch* batch, 
                               MemRow* outer_mem_row, 
                               MemRow* inner_mem_row,
                               bool inner_join);
    int _construct_null_result_batch(RowBatch* batch, MemRow* outer_mem_row);
private:
    pb::JoinType _join_type;
    std::vector<ExprNode*> _conditions;
    std::vector<ExprNode*> _have_removed;
    std::unordered_set<int32_t> _left_tuple_ids;
    std::unordered_set<int32_t> _right_tuple_ids;

    ExecNode* _outer_node; //驱动表
    ExecNode* _inner_node;
    std::unordered_set<int32_t> _outer_tuple_ids;
    std::unordered_set<int32_t> _inner_tuple_ids;
    //join相等条件的slot_ref
    std::vector<ExprNode*> _outer_equal_slot;
    std::vector<ExprNode*> _inner_equal_slot;

    //从左边取到的等值条件的value
    std::vector<std::vector<ExprValue>> _outer_join_values;
    
    std::vector<MemRow*> _outer_tuple_data;
    std::vector<MemRow*> _inner_tuple_data;

    //目前只支持等值join（a.id = b.id and a.name = b.name）
    butil::FlatMap<std::string, std::vector<MemRow*>> _hash_map;
    size_t _hash_mapped_index = 0;

    std::vector<MemRow*>::iterator _outer_iter;
    
    MemRowDescriptor* _mem_row_desc;
    bool _outer_table_is_null = false;
    
    RowBatch _inner_row_batch;
    bool    _child_eos = false;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
