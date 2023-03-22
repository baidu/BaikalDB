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

#include "join_node.h"
#include "filter_node.h"
#include "full_export_node.h"
#include "expr_node.h"
#include "rocksdb_scan_node.h"
#include "scalar_fn_call.h"
#include "index_selector.h"
#include "plan_router.h"
#include "logical_planner.h"
#include "literal.h"

namespace baikaldb {
int JoinNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = Joiner::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    } 
    const pb::JoinNode& join_node = node.derive_node().join_node();
    _join_type = join_node.join_type();
    
    for (auto& expr : join_node.conditions()) {
        ExprNode* condition = NULL;
        ret = ExprNode::create_tree(expr, &condition);
        if (ret < 0) {
            //如何释放资源
            return ret;
        }
        _conditions.push_back(condition);
    }
    for (auto& tuple_id : join_node.left_tuple_ids()) {
        _left_tuple_ids.insert(tuple_id); 
    }
    for (auto& tuple_id : join_node.right_tuple_ids()) {
        _right_tuple_ids.insert(tuple_id);
    }
    return 0;
}

int JoinNode::predicate_pushdown(std::vector<ExprNode*>& input_exprs) {
    //DB_WARNING("node:%ld is pushdown", this);
    convert_to_inner_join(input_exprs);
    std::vector<ExprNode*> outer_push_exprs;
    std::vector<ExprNode*> inner_push_exprs;
    std::vector<ExprNode*> correlate_exprs;
    auto iter = input_exprs.begin(); 
    while (iter != input_exprs.end()) {
        std::unordered_set<int32_t> related_tuple_ids;
        (*iter)->get_all_tuple_ids(related_tuple_ids);
        if (related_tuple_ids.size() > 0 && !contains_expr(*iter)) {
            correlate_exprs.emplace_back(*iter);
            iter = input_exprs.erase(iter);
            continue;
        }
        ++iter;
    }
    if (_join_type == pb::INNER_JOIN) {
        auto iter = _conditions.begin(); 
        while (iter != _conditions.end()) {
            if (outer_contains_expr(*iter)) {
                outer_push_exprs.push_back(*iter);
                iter = _conditions.erase(iter);
                continue;
            }
            if (inner_contains_expr(*iter)) {
                inner_push_exprs.push_back(*iter);
                iter = _conditions.erase(iter);
                continue;
            }
            ++iter;
        }
        for (auto& expr : input_exprs) {
            if (outer_contains_expr(expr)) {
                outer_push_exprs.push_back(expr);
                continue;
            } 
            if (inner_contains_expr(expr)) {
                inner_push_exprs.push_back(expr);
                continue;
            }
            _conditions.push_back(expr);
        }
        input_exprs.clear();
        input_exprs = correlate_exprs;
    }
    if (_join_type == pb::LEFT_JOIN || _join_type == pb::RIGHT_JOIN) {
        auto iter = input_exprs.begin();
        while (iter != input_exprs.end()) {
            if (outer_contains_expr(*iter)) {
                outer_push_exprs.push_back(*iter);
                iter = input_exprs.erase(iter);
                continue;
            }
            ++iter;
        }
        iter = _conditions.begin();
        while (iter != _conditions.end()) {
            if (inner_contains_expr(*iter)) {
                inner_push_exprs.push_back(*iter);
                iter = _conditions.erase(iter);
                continue;
            }
            ++iter;
        }
    }    
    _outer_node->predicate_pushdown(outer_push_exprs);
    if (outer_push_exprs.size() > 0) {
        _outer_node->add_filter_node(outer_push_exprs);
    }
    _inner_node->predicate_pushdown(inner_push_exprs);
    if (inner_push_exprs.size() > 0) {
        _inner_node->add_filter_node(inner_push_exprs);
    }
    return 0;
}
void JoinNode::convert_to_inner_join(std::vector<ExprNode*>& input_exprs) {
    //inner_join默认情况下都是左边是驱动表, left_join只能左边做驱动表
    //outer_node 和 inner_node在做完seperate之后会变化，所以在join node open的时候要重新赋值一次
    _outer_node = _children[0];
    _inner_node = _children[1];
    _outer_tuple_ids = _left_tuple_ids;
    _inner_tuple_ids = _right_tuple_ids;
    
    if (_join_type == pb::RIGHT_JOIN) {
        _outer_node = _children[1];
        _inner_node = _children[0];
        _outer_tuple_ids = _right_tuple_ids;
        _inner_tuple_ids = _left_tuple_ids;
    }
    
    std::vector<ExprNode*> full_exprs = input_exprs;
    for (auto& expr : _conditions) {
        full_exprs.push_back(expr);
    }
    if (_inner_node->node_type() == pb::JOIN_NODE) {
        ((JoinNode*)_inner_node)->convert_to_inner_join(full_exprs);
    }
    if (_outer_node->node_type() == pb::JOIN_NODE) {
        ((JoinNode*)_outer_node)->convert_to_inner_join(full_exprs);
    }
    if (_join_type == pb::INNER_JOIN) {
        return;
    }

    for (auto& expr : input_exprs) {
        if (outer_contains_expr(expr)) {
            continue;
        }
        if (inner_contains_expr(expr)
                && !expr->contains_null_function()) {
            set_join_type(pb::INNER_JOIN);
            return;
        }
        if (contains_expr(expr) && !expr->contains_null_function()
                && !expr->contains_special_operator(pb::OR_PREDICATE)) {
            set_join_type(pb::INNER_JOIN);
            return;
        }
    }
}

void JoinNode::transfer_pb(int64_t region_id, pb::PlanNode* pb_node) {
    ExecNode::transfer_pb(region_id, pb_node);
    auto join_node = pb_node->mutable_derive_node()->mutable_join_node();
    join_node->set_join_type(_join_type);
    join_node->clear_conditions();
    for (auto expr : _conditions) {
       ExprNode::create_pb_expr(join_node->add_conditions(), expr);
    }
}

int JoinNode::hash_join(RuntimeState* state) {
    SortNode* sort_node = static_cast<SortNode*>(_outer_node->get_node(pb::SORT_NODE));
    if (sort_node != nullptr) {
        JoinNode* join_node = static_cast<JoinNode*>(sort_node->get_node(pb::JOIN_NODE));
        if (join_node == nullptr) {
            std::vector<ExecNode*> scan_nodes;
            _outer_node->get_node(pb::SCAN_NODE, scan_nodes);
            bool index_has_null = false;
            do_plan_router(state, scan_nodes, index_has_null);
        }
    }
    int ret = _outer_node->open(state);
    if (ret < 0) {
        DB_WARNING("ExecNode:: left table open fail");
        return ret;
    }
    ret = fetcher_full_table_data(state, _outer_node, _outer_tuple_data);
    if (ret < 0) {
        DB_WARNING("ExecNode::join open fail when fetch left table");
        return ret;
    }
    if (_outer_tuple_data.size() == 0) {
        _outer_table_is_null = true;
        return 0;
    }
    // watt基准循环过滤
    if (_outer_node->get_node(pb::FULL_EXPORT_NODE) != nullptr) {
        _use_loop_hash_map = true;
        return loop_hash_join(state);
    }
    construct_equal_values(_outer_tuple_data, _outer_equal_slot);
    std::vector<ExprNode*> in_exprs;
    ret = construct_in_condition(_inner_equal_slot, _outer_join_values, in_exprs);
    if (ret < 0) {
        DB_WARNING("ExecNode::create in condition for right table fail");
        return ret;
    }

    //表达式下推，下推的那个节点重新做索引选择，路由选择
    _inner_node->predicate_pushdown(in_exprs);
    if (in_exprs.size() > 0) {
        DB_WARNING("inner node add filter node");
        _inner_node->add_filter_node(in_exprs);
    }

    std::vector<ExecNode*> scan_nodes;
    _inner_node->get_node(pb::SCAN_NODE, scan_nodes);
    bool index_has_null = false;
    do_plan_router(state, scan_nodes, index_has_null);
    if (index_has_null) {
        _inner_node->set_return_empty();
    }
    //谓词下推后可能生成新的plannode重新生成tracenode
    _inner_node->create_trace();
    ret = _inner_node->open(state);
    if (ret < 0) {
        DB_WARNING("ExecNode::inner table open fial");
        return -1;
    }
    if (_join_type == pb::LEFT_JOIN 
            || _join_type == pb::RIGHT_JOIN) {
        ret = fetcher_full_table_data(state, _inner_node, _inner_tuple_data);
        if (ret < 0) {
            DB_WARNING("fetcher inner node fail");
            return ret;
        }
        construct_hash_map(_inner_tuple_data, _inner_equal_slot);
        _outer_iter = _outer_tuple_data.begin();
    } else {
        construct_hash_map(_outer_tuple_data, _outer_equal_slot);
    }
    return 0;
}

int JoinNode::loop_hash_join(RuntimeState* state) {
    int ret = fetcher_inner_table_data(state, _outer_tuple_data, _inner_tuple_data);
    if (ret < 0) {
        DB_WARNING("fetcher inner node fail");
        return ret;
    }
    construct_hash_map(_inner_tuple_data, _inner_equal_slot);
    _outer_iter = _outer_tuple_data.begin();
    return 0;
}

int JoinNode::nested_loop_join(RuntimeState* state) {
    _mem_row_desc = state->mem_row_desc();
    SortNode* sort_node = static_cast<SortNode*>(_outer_node->get_node(pb::SORT_NODE));
    if (sort_node != nullptr) {
        JoinNode* join_node = static_cast<JoinNode*>(sort_node->get_node(pb::JOIN_NODE));
        if (join_node == nullptr) {
            std::vector<ExecNode*> scan_nodes;
            _outer_node->get_node(pb::SCAN_NODE, scan_nodes);
            bool index_has_null = false;
            do_plan_router(state, scan_nodes, index_has_null);
        }
    }
    int ret = _outer_node->open(state);
    if (ret < 0) {
        DB_WARNING("ExecNode:: left table open fail");
        return ret;
    }
    ret = fetcher_full_table_data(state, _outer_node, _outer_tuple_data);
    if (ret < 0) {
        DB_WARNING("ExecNode::join open fail when fetch left table");
        return ret;
    }
    if (_outer_tuple_data.size() == 0) {
        _outer_table_is_null = true;
        DB_WARNING("not data");
        return 0;
    }
    construct_equal_values(_outer_tuple_data, _outer_equal_slot);
    std::vector<ExprNode*> in_exprs;
    ret = construct_in_condition(_inner_equal_slot, _outer_join_values, in_exprs);
    if (ret < 0) {
        DB_WARNING("ExecNode::create in condition for right table fail");
        return ret;
    }
    //表达式下推，下推的那个节点重新做索引选择，路由选择
    _inner_node->predicate_pushdown(in_exprs);
    if (in_exprs.size() > 0) {
        DB_WARNING("inner node add filter node");
        _inner_node->add_filter_node(in_exprs);
    }
    std::vector<ExecNode*> scan_nodes;
    _inner_node->get_node(pb::SCAN_NODE, scan_nodes);
    bool index_has_null = false;
    do_plan_router(state, scan_nodes, index_has_null);
    if (index_has_null) {
        _inner_node->set_return_empty();
    }
    //谓词下推后可能生成新的plannode重新生成tracenode
    _inner_node->create_trace();
    ret = _inner_node->open(state);
    if (ret < 0) {
        DB_WARNING("ExecNode::inner table open fial");
        return -1;
    }
    ret = fetcher_full_table_data(state, _inner_node, _inner_tuple_data);
    if (ret < 0) {
        DB_WARNING("fetcher inner node fail");
        return ret;
    }
    _outer_iter = _outer_tuple_data.begin();
    _inner_iter = _inner_tuple_data.begin();
    return 0;
}

int JoinNode::open(RuntimeState* state) {
    TimeCost join_time_cost;
    _outer_node = _children[0];
    _inner_node = _children[1];
    _outer_tuple_ids = _left_tuple_ids;
    _inner_tuple_ids = _right_tuple_ids;
    if (_join_type == pb::RIGHT_JOIN) {
        _outer_node = _children[1];
        _inner_node = _children[0];
        _outer_tuple_ids = _right_tuple_ids;
        _inner_tuple_ids = _left_tuple_ids;
    }
    _mem_row_desc = state->mem_row_desc();
    int ret = strip_out_equal_slots();
    if (ret < 0) {
        DB_WARNING("fill equal slot fail");
        return -1;
    }
    if (_use_hash_map) {
        return hash_join(state);
    } else {
        return nested_loop_join(state);
    }
}

int JoinNode::get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
    if (_outer_table_is_null) {
        *eos = true;
        return 0;
    }
    if (_use_loop_hash_map) {
        return get_next_for_loop_hash_inner_join(state, batch, eos);
    }
    if (_use_hash_map) {
        if (_join_type == pb::INNER_JOIN) {
            return get_next_for_hash_inner_join(state, batch, eos);
        }
        return get_next_for_hash_outer_join(state, batch, eos);
    }
    return get_next_for_nested_loop_join(state, batch, eos);
}

int JoinNode::get_next_for_nested_loop_join(RuntimeState* state, RowBatch* batch, bool* eos) {
    TimeCost get_next_time;
    while (1) {
        if (_outer_iter == _outer_tuple_data.end()) {
            DB_WARNING("when join, outer iter is end, time_cost:%ld", get_next_time.get_time());
            *eos = true;
            return 0;
        }
        bool matched = false;
        
        while (_inner_iter != _inner_tuple_data.end()) {
            if (reached_limit()) {
                DB_WARNING("when join, reach limit size:%lu, time_cost:%ld", 
                            batch->size(), get_next_time.get_time());
                *eos = true;
                return 0;
            }
            if (batch->is_full()) {
                return 0;
            }
            int ret = construct_result_batch(batch, *_outer_iter, *_inner_iter, matched);
            if (ret < 0) {
                DB_WARNING("construct result batch fail");
                return ret;
            }
            ++_num_rows_returned;
            if (matched && _join_type == pb::INNER_JOIN) {
                break;
            } else {
                ++_inner_iter;
            }
        }
        if (!matched) {
            switch (_join_type) {
            case pb::LEFT_JOIN:
            case pb::RIGHT_JOIN: {
                int ret = construct_null_result_batch(batch, *_outer_iter);
                if (ret < 0) {
                    DB_WARNING("construct result batch fail");
                    return ret;
                }
                ++_num_rows_returned;
                break;
            }
            default:
                break;
            }
        }
        ++_outer_iter;
        _inner_iter = _inner_tuple_data.begin();
    }
    return 0;
}

int JoinNode::get_next_for_hash_outer_join(RuntimeState* state, RowBatch* batch, bool* eos) {
    TimeCost get_next_time;
    while (1) {
        if (_outer_iter == _outer_tuple_data.end()) {
            DB_WARNING("when join, outer iter is end, time_cost:%ld", get_next_time.get_time());
            *eos = true;
            return 0;
        }
        MutTableKey outer_key;
        encode_hash_key(*_outer_iter, _outer_equal_slot, outer_key);
        auto inner_mem_rows = _hash_map.seek(outer_key.data());
        if (inner_mem_rows != NULL) {
            for (; _result_row_index < inner_mem_rows->size(); ++_result_row_index) {
                if (reached_limit()) {
                    DB_WARNING("when join, reach limit size:%lu, time_cost:%ld", 
                                    batch->size(), get_next_time.get_time());
                    *eos = true;
                    return 0;
                }
                if (batch->is_full()) {
                    //DB_WARNING("when join, batch is full, time_cost:%ld", get_next_time.get_time());
                    return 0;
                }
                bool matched = false;
                int ret = construct_result_batch(batch, *_outer_iter, (*inner_mem_rows)[_result_row_index], matched);
                if (ret < 0) {
                    DB_WARNING("construct result batch fail");
                    return ret;
                }
                ++_num_rows_returned;
            }
        } else {
            if (reached_limit()) {
                DB_WARNING("when join, reach limit size:%lu, time_cost:%ld", 
                                batch->size(), get_next_time.get_time());
                *eos = true;
                return 0;
            }
            if (batch->is_full()) {
                //DB_WARNING("when join, batch is full, time_cost:%ld", get_next_time.get_time());
                return 0;
            }
            //fill NULL
            int ret = construct_null_result_batch(batch, *_outer_iter);
            if (ret < 0) {
                DB_WARNING("construct result batch fail");
                return ret;
            }
            ++_num_rows_returned;
        }
        _result_row_index = 0;
        ++_outer_iter;
    }
    return 0;
}

int JoinNode::get_next_for_hash_inner_join(RuntimeState* state, RowBatch* batch, bool* eos) {
    TimeCost get_next_time;
    while (1) {
        if (_inner_row_batch.is_traverse_over()) {
            if (_child_eos) {
                *eos = true;
                DB_WARNING("when join, get next complete, child eos, time_cost:%ld", 
                            get_next_time.get_time());
                return 0;
            } else {
                _inner_row_batch.clear();
                int ret = _inner_node->get_next(state, &_inner_row_batch, &_child_eos);
                if (ret < 0) {
                    DB_WARNING("_children get_next fail");
                    return ret;
                }
                continue;
            }
        }
        std::unique_ptr<MemRow>& inner_mem_row = _inner_row_batch.get_row();
        MutTableKey inner_key;
        encode_hash_key(inner_mem_row.get(), _inner_equal_slot, inner_key);
        auto outer_mem_rows = _hash_map.seek(inner_key.data());
        if (outer_mem_rows != NULL) {
            for (; _result_row_index < outer_mem_rows->size(); ++_result_row_index) {
                if (reached_limit()) {
                    DB_WARNING("when join, reach limit size:%lu, time_cost:%ld", 
                                batch->size(), get_next_time.get_time());
                    *eos = true;
                    return 0;
                }
                if (batch->is_full()) {
                    return 0;
                }
                bool matched = false;
                int ret = construct_result_batch(batch, (*outer_mem_rows)[_result_row_index], inner_mem_row.get(), matched);
                if (ret < 0) {
                    DB_WARNING("construct result batch fail");
                    return ret;
                }
                ++_num_rows_returned;
            }
        }
        _result_row_index = 0;
        _inner_row_batch.next();
    }
    return 0;
}

int JoinNode::get_next_for_loop_hash_inner_join(RuntimeState* state, RowBatch* batch, bool* eos) {
    TimeCost get_next_time;
    if (_outer_iter == _outer_tuple_data.end()) {
        DB_WARNING("when join, outer iter is end, time_cost:%ld", get_next_time.get_time());
        // clear previous
        for (auto& mem_row : _outer_tuple_data) {
            delete mem_row;
        }
        _outer_tuple_data.clear();
        for (auto& mem_row : _inner_tuple_data) {
            delete mem_row;
        }
        _inner_tuple_data.clear();
        _hash_map.clear();

        FullExportNode* full_export = static_cast<FullExportNode*>(_outer_node->get_node(pb::FULL_EXPORT_NODE));
        if (full_export == nullptr) {
            DB_WARNING("full_export is null");
            return -1;
        }
        full_export->reset_num_rows_returned();

        int ret = fetcher_full_table_data(state, _outer_node, _outer_tuple_data);
        if (ret < 0) {
            DB_WARNING("fetcher outer node fail");
            return ret;
        }
        if (_outer_tuple_data.size() == 0) {
            _outer_table_is_null = true;
            *eos = true;
            DB_WARNING("not data");
            return 0;
        }

        ret = fetcher_inner_table_data(state, _outer_tuple_data, _inner_tuple_data);
        if (ret < 0) {
            DB_WARNING("fetcher inner node fail");
            return ret;
        }
        construct_hash_map(_inner_tuple_data, _inner_equal_slot);
        _outer_iter = _outer_tuple_data.begin();
    }

    while (1) {
        if (_outer_iter == _outer_tuple_data.end()) {
            DB_WARNING("when join, outer iter is end, time_cost:%ld", get_next_time.get_time());
            *eos = true;
            return 0;
        }
        MutTableKey outer_key;
        encode_hash_key(*_outer_iter, _outer_equal_slot, outer_key);
        auto inner_mem_rows = _hash_map.seek(outer_key.data());
        if (inner_mem_rows != NULL) {
            for (; _result_row_index < inner_mem_rows->size(); ++_result_row_index) {
                if (reached_limit()) {
                    DB_WARNING("when join, reach limit size:%lu, time_cost:%ld", 
                                    batch->size(), get_next_time.get_time());
                    *eos = true;
                    return 0;
                }
                if (batch->is_full()) {
                    //DB_WARNING("when join, batch is full, time_cost:%ld", get_next_time.get_time());
                    return 0;
                }
                bool matched = false;
                int ret = construct_result_batch(batch, *_outer_iter, (*inner_mem_rows)[_result_row_index], matched);
                if (ret < 0) {
                    DB_WARNING("construct result batch fail");
                    return ret;
                }
                ++_num_rows_returned;
            }
        }
        _result_row_index = 0;
        ++_outer_iter;
    }
    return 0;
}

bool JoinNode::need_reorder(
        std::map<int32_t, ExecNode*>& tuple_join_child_map,
        std::map<int32_t, std::set<int32_t>>& tuple_equals_map, 
        std::vector<int32_t>& tuple_order,
        std::vector<ExprNode*>& conditions) {
    if (_join_type != pb::INNER_JOIN) {
        return false;
    }
    for (auto& child : _children) {
        if (child->node_type() == pb::JOIN_NODE) {
            if (!static_cast<JoinNode*>(child)->need_reorder(
                        tuple_join_child_map, tuple_equals_map, tuple_order, conditions)) {
                return false;
            }
        } else {
            ExecNode* scan_node = child->get_node(pb::SCAN_NODE);
            if (scan_node == nullptr) {
                return false;
            }
            int32_t tuple_id = static_cast<ScanNode*>(scan_node)->tuple_id();
            tuple_join_child_map[tuple_id] = child;
            tuple_order.push_back(tuple_id);
        }
    }
    for (auto& expr : _conditions) {
        expr_is_equal_condition(expr);
        conditions.push_back(expr);
    }
    for (size_t i = 0; i < _outer_equal_slot.size(); i++) {
        int32_t left_tuple_id = static_cast<SlotRef*>(_outer_equal_slot[i])->tuple_id();
        int32_t right_tuple_id = static_cast<SlotRef*>(_inner_equal_slot[i])->tuple_id();
        tuple_equals_map[left_tuple_id].insert(right_tuple_id);
        tuple_equals_map[right_tuple_id].insert(left_tuple_id);
    }
    return true;
}

}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
