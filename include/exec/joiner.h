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
#include "mut_table_key.h"
#ifdef BAIDU_INTERNAL 
#include <base/containers/flat_map.h>
#else
#include <butil/containers/flat_map.h>
#endif
#include "slot_ref.h"

namespace baikaldb {
struct ExprValueVec {
    std::vector<ExprValue> vec;

    bool operator==(const ExprValueVec& other) const {
        if (this->vec.size() != other.vec.size()) {
            return false;
        }

        for (int i = 0; i < other.vec.size(); i++) {
            int64_t c = other.vec[i].compare(this->vec[i]);
            if (c != 0) {
                return false;
            }
        }

        return true;
    }

    struct HashFunction {
        size_t operator()(const ExprValueVec& ev_vec) const {
            size_t offset = 0;
            size_t hash = 0;
            for (const auto& ev : ev_vec.vec) {
                hash ^= (ev.hash() << offset);
                offset++;
            }

            return hash;
        }
    };
};
using ExprValueSet = butil::FlatSet<ExprValueVec, ExprValueVec::HashFunction>;
class Joiner : public ExecNode {
public:
    Joiner() : _child_eos(false) { 

    }
    virtual  ~Joiner() {
        for (auto& condition : _conditions) {
            ExprNode::destroy_tree(condition);
        }
        for (auto& condition : _have_removed) {
            ExprNode::destroy_tree(condition);
        }
    }
    virtual int init(const pb::PlanNode& node);
    virtual int expr_optimize(QueryContext* ctx);
    virtual void find_place_holder(std::map<int, ExprNode*>& placeholders);
    virtual int predicate_pushdown(std::vector<ExprNode*>& input_exprs);
    virtual void close(RuntimeState* state);
    virtual std::vector<ExprNode*>* mutable_conjuncts() {
        return &_conditions; 
    }
    void convert_to_inner_join(std::vector<ExprNode*>& input_exprs);
    int get_next_for_hash_other_join(RuntimeState* state, RowBatch* batch, bool* eos);
    int get_next_for_hash_inner_join(RuntimeState* state, RowBatch* batch, bool* eos);
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

    void do_plan_router(RuntimeState* state, std::vector<ExecNode*>& scan_nodes, bool& index_has_null);
    
    pb::JoinType join_type() {
        return _join_type;
    }
    void set_join_type(const pb::JoinType join_type) {
        _join_type = join_type;
        _pb_node.mutable_derive_node()->mutable_join_node()->set_join_type(join_type);
    }
    virtual void show_explain(std::vector<std::map<std::string, std::string>>& output);

    bool is_satisfy_filter(MemRow* row);
    int strip_out_equal_slots();
    bool expr_is_equal_condition(ExprNode* expr);
    bool is_slot_ref_equal_condition(ExprNode* left, ExprNode* right);
    int construct_in_condition(std::vector<ExprNode*>& slot_refs,
                                  const ExprValueSet& in_values,
                                  std::vector<ExprNode*>& in_exprs);
    int fetcher_full_table_data(RuntimeState* state, ExecNode* child_node,
                            std::vector<MemRow*>& tuple_data);
    int fetcher_inner_table_data(RuntimeState* state,
                            const std::vector<MemRow*>& outer_tuple_data,
                            std::vector<MemRow*>& inner_tuple_data);
    void construct_hash_map(const std::vector<MemRow*>& tuple_data,
                             const std::vector<ExprNode*>& slot_refs);
    void encode_hash_key(MemRow* row,
                          const std::vector<ExprNode*>& slot_ref_exprs,
                          MutTableKey& key);
    void construct_equal_values(const std::vector<MemRow*>& tuple_data,
                          const std::vector<ExprNode*>& slot_ref_exprs);
    int construct_result_batch(RowBatch* batch, 
                               MemRow* outer_mem_row, 
                               MemRow* inner_mem_row,
                               bool& matched);
    int construct_null_result_batch(RowBatch* batch, MemRow* outer_mem_row);
    std::unordered_set<int32_t>* left_tuple_ids() {
        return &_left_tuple_ids;
    }
    std::unordered_set<int32_t>* right_tuple_ids() {
        return &_right_tuple_ids;
    }

protected:
    pb::JoinType _join_type;
    pb::CompareType _compare_type = pb::CMP_NULL;
    std::vector<ExprNode*> _conditions;
    std::vector<ExprNode*> _have_removed;
    std::unordered_set<int32_t> _left_tuple_ids;
    std::unordered_set<int32_t> _right_tuple_ids;

    ExecNode* _outer_node = nullptr; //驱动表
    ExecNode* _inner_node = nullptr;
    std::unordered_set<int32_t> _outer_tuple_ids;
    std::unordered_set<int32_t> _inner_tuple_ids;
    //join相等条件的slot_ref
    std::vector<ExprNode*> _outer_equal_slot;
    std::vector<ExprNode*> _inner_equal_slot;

    //从左边取到的等值条件的value
    ExprValueSet _outer_join_values;
    
    std::vector<MemRow*> _outer_tuple_data;
    std::vector<MemRow*> _inner_tuple_data;

    //目前只支持等值join（a.id = b.id and a.name = b.name）
    butil::FlatMap<std::string, std::vector<MemRow*>> _hash_map;

    std::vector<MemRow*>::iterator _outer_iter;
    std::vector<MemRow*>::iterator _inner_iter;
    size_t _result_row_index = 0;
    
    MemRowDescriptor* _mem_row_desc = nullptr;
    bool _outer_table_is_null = false;
    
    bool    _use_hash_map = true;
    bool    _child_eos = false;
    bool    _is_apply = false;
    bool    _conditions_has_agg = false;
    bool    _use_loop_hash_map = false;
    size_t  _loops = 0;
    RowBatch _inner_row_batch;
    std::map<int32_t, std::set<int32_t>> _inner_equal_field_ids; // 用于检查内查询的等值条件是否具有唯一性
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
