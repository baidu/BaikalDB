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

#include "apply_node.h"
#include "filter_node.h"
#include "expr_node.h"
#include "rocksdb_scan_node.h"
#include "scalar_fn_call.h"
#include "index_selector.h"
#include "plan_router.h"
#include "logical_planner.h"
#include "agg_node.h"
#include "literal.h"
#include "math.h"

namespace baikaldb {
int ApplyNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = Joiner::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    } 
    const pb::ApplyNode& apply_node = node.derive_node().apply_node();
    _join_type = apply_node.join_type();
    _max_one_row = apply_node.max_one_row();
    _compare_type = apply_node.compare_type();
    _is_select_field = apply_node.is_select_field();
    _is_apply = true;
    for (auto& expr : apply_node.conditions()) {
        ExprNode* condition = NULL;
        ret = ExprNode::create_tree(expr, &condition);
        if (ret < 0) {
            //如何释放资源
            return ret;
        }
        _conditions.emplace_back(condition);
    }
    for (auto& tuple_id : apply_node.left_tuple_ids()) {
        _left_tuple_ids.emplace(tuple_id); 
    }
    for (auto& tuple_id : apply_node.right_tuple_ids()) {
        _right_tuple_ids.emplace(tuple_id);
    }
    return 0;
}

void ApplyNode::extract_eq_inner_slots(ExprNode* expr_node) {
    if (expr_node->node_type() != pb::SLOT_REF) {
        return;
    }
    int32_t tuple_id = static_cast<SlotRef*>(expr_node)->tuple_id();
    if (_right_tuple_ids.count(tuple_id) == 1) {
        _inner_eq_slot_refs.emplace_back(static_cast<SlotRef*>(expr_node));
    }
}

bool ApplyNode::is_correlate_expr(ExprNode* expr, bool& is_equal) {
    if (expr->node_type() == pb::FUNCTION_CALL 
        && static_cast<ScalarFnCall*>(expr)->fn().fn_op() == parser::FT_EQ) {
        is_equal = true;
    }
    std::unordered_set<int32_t> related_tuple_ids;
    expr->get_all_tuple_ids(related_tuple_ids);
    bool in_left = false;
    bool in_right = false;
    for (auto& related_tuple_id : related_tuple_ids) {
        if (_inner_tuple_ids.count(related_tuple_id) == 1) {
            in_right = true;
        }
        if (_outer_tuple_ids.count(related_tuple_id) == 1) {
            in_left = true;
        }
    }
    if (is_equal && in_left && in_right) {
        if (expr->children_size() == 2) {
            ExprNode* left_child = expr->children(0);
            ExprNode* right_child = expr->children(1);
            if (left_child->is_row_expr() && right_child->is_row_expr()) {
                for (size_t i = 0; i < left_child->children_size(); i++) {
                    extract_eq_inner_slots(left_child->children(i));
                    extract_eq_inner_slots(right_child->children(i));
                }
            } else {
                extract_eq_inner_slots(left_child);
                extract_eq_inner_slots(right_child);
            }
        }
    }
    return in_left && in_right;
}

void ApplyNode::decorrelate() {
    ExecNode* inner_node = _children[1];
    ExecNode* parent_node = this;
    ExecNode*  agg_node = inner_node->get_specified_node(pb::AGG_NODE);
    ExecNode*  merge_agg_node = inner_node->get_specified_node(pb::MERGE_AGG_NODE);
    if (merge_agg_node != nullptr) {
        _max_one_row = false;
    }
    _outer_tuple_ids = _left_tuple_ids;
    _inner_tuple_ids = _right_tuple_ids;
    while (1) {
        //DB_WARNING("NODE TYPE:%s", pb::PlanNodeType_Name(inner_node->node_type()).c_str());
        if (inner_node->node_type() == pb::WHERE_FILTER_NODE ||
            inner_node->node_type() == pb::HAVING_FILTER_NODE) {
            std::vector<ExprNode*>* conjuncts = inner_node->mutable_conjuncts();
            auto expr_iter = conjuncts->begin();
            while (expr_iter != conjuncts->end()) {
                bool is_equal = false;
                //(*expr_iter)->print_expr_info();
                if (is_correlate_expr(*expr_iter, is_equal)) {
                    if (is_equal) {
                        _conditions.emplace_back(*expr_iter);
                        expr_iter = conjuncts->erase(expr_iter);
                        if (merge_agg_node != nullptr && agg_node != nullptr) {
                            for (auto& ref : _inner_eq_slot_refs) {
                                pb::Expr slot_expr;
                                pb::ExprNode* node = slot_expr.add_nodes();
                                ref->transfer_pb(node);
                                static_cast<AggNode*>(agg_node)->add_group_expr(slot_expr, ref->slot_id());
                                static_cast<AggNode*>(merge_agg_node)->add_group_expr(slot_expr, ref->slot_id());
                            }
                        }
                    } else {
                        ++expr_iter;
                    }
                } else {
                    ++expr_iter;
                }
            }
            ExecNode* new_child = inner_node->children(0);
            if (conjuncts->size() == 0) {
                ExecNode* old_child = inner_node;
                parent_node->replace_child(old_child, new_child);
                inner_node->clear_children();
                delete inner_node;
            }
            inner_node = new_child;
            continue;
        }
        if (inner_node->node_type() == pb::SORT_NODE
            || inner_node->node_type() == pb::LIMIT_NODE) {
            ExecNode* new_child = inner_node->children(0);
            // 有agg_node时sort/limit对结果无影响
            if (merge_agg_node != nullptr) {
                ExecNode* old_child = inner_node;
                parent_node->replace_child(old_child, new_child);
                inner_node->clear_children();
                _children[1]->reset_limit(-1);
                delete inner_node;
                DB_WARNING("delete sort/limit");
            } else {
                parent_node= inner_node;
            }
            inner_node = new_child;
            continue;
        }
        if (inner_node->node_type() == pb::MERGE_AGG_NODE) {
            if (_is_select_field) {
                // 类似 select *,(select min(height) from t2 where t1.height=t2.height) as h from t1;暂未优化
                return;
            }
            // count(*)暂不优化
            for (auto& expr : inner_node->pb_node().derive_node().agg_node().agg_funcs()) {
                if (expr.nodes(0).fn().name() == "count_star") {
                    return;
                }
            }
        }
        if (inner_node->node_type() == pb::AGG_NODE
            || inner_node->node_type() == pb::SELECT_MANAGER_NODE
            || inner_node->node_type() == pb::MERGE_AGG_NODE) {
            parent_node= inner_node;
            inner_node = inner_node->children(0);
            continue;
        }
        break;
    }
}

int ApplyNode::predicate_pushdown(std::vector<ExprNode*>& input_exprs) {
    _children[0]->predicate_pushdown(input_exprs);
    std::vector<ExprNode*> empty_exprs;
    _children[1]->predicate_pushdown(empty_exprs);
    return 0;
}

void ApplyNode::transfer_pb(int64_t region_id, pb::PlanNode* pb_node) {
    ExecNode::transfer_pb(region_id, pb_node);
    auto apply_node = pb_node->mutable_derive_node()->mutable_apply_node();
    apply_node->set_join_type(_join_type);
    apply_node->clear_conditions();
    for (auto expr : _conditions) {
       ExprNode::create_pb_expr(apply_node->add_conditions(), expr);
    }
}

int ApplyNode::check_unique_key(RuntimeState* state, const std::map<int32_t, std::set<int32_t>>& tuple_field_ids) {
    if (!_max_one_row) {
        return 0;
    }
    for (auto& iter : tuple_field_ids) {
        pb::TupleDescriptor* tuple_desc = state->get_tuple_desc(iter.first);
        int ret = SchemaFactory::get_instance()->is_unique_field_ids(tuple_desc->table_id(), iter.second);
        if (ret < 0) {
            DB_WARNING("process table info failed desc:%s", tuple_desc->ShortDebugString().c_str());
            return -1;
        } else if (ret > 0) {
            _max_one_row = false;
            _children[1]->reset_limit(-1);
            DB_WARNING("is uniq");
            break;
        }
    }
    return 0;
}


int ApplyNode::open(RuntimeState* state) {
    _outer_node = _children[0];
    _inner_node = _children[1];
    _outer_tuple_ids = _left_tuple_ids;
    _inner_tuple_ids = _right_tuple_ids;
    _mem_row_desc = state->mem_row_desc();
    int ret = strip_out_equal_slots();
    if (ret < 0) {
        DB_WARNING("fill equal slot fail");
        return -1;
    }
    ret = check_unique_key(state, _inner_equal_field_ids);
    if (ret < 0) {
        return -1;
    }
    DB_WARNING("_use_hash_map:%d _max_one_row:%d _conditions_has_agg:%d", _use_hash_map, _max_one_row, _conditions_has_agg);
    if (_use_hash_map && !_max_one_row && !_conditions_has_agg) {
        // inner_node会被多次调用
        if (_parent->get_limit() > 0 && _join_type != pb::LEFT_JOIN && _join_type != pb::ANTI_SEMI_JOIN) {
            _use_loop_hash_map = true;
            return loop_hash_apply(state);
        }
        return hash_apply(state);
    } else {
        _use_hash_map = false;
        return nested_loop_apply(state);
    }
}

void ApplyNode::get_slot_ref_sign_set(RuntimeState* state, std::set<int64_t>& sign_set) {
    for (auto tuple_id : _left_tuple_ids) {
        auto tuple_desc = state->get_tuple_desc(tuple_id);
        if (tuple_desc != nullptr) {
            for (auto& slot : tuple_desc->slots()) {
                sign_set.emplace(tuple_id << 16 | slot.slot_id());
            }
        }
    }
}

int ApplyNode::nested_loop_apply(RuntimeState* state) {
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
    if (_is_explain) {
        _inner_node->create_trace();
        ret = _inner_node->open(state);
        if (ret < 0) {
            DB_WARNING("ExecNode::inner table open fail");
            return -1;
        }
        return 0;
    }
    if (_outer_tuple_data.size() == 0) {
        _outer_table_is_null = true;
        return 0;
    }
    DB_WARNING("data size:%ld", _outer_tuple_data.size());
    get_slot_ref_sign_set(state, _slot_ref_sign_set);
    _inner_node->replace_slot_ref_to_literal(_slot_ref_sign_set, _literal_maps);
    _outer_iter = _outer_tuple_data.begin();
    _inner_node->get_node(pb::SCAN_NODE, _scan_nodes);
    ret = fetcher_inner_table_data(state, *_outer_iter, _scan_nodes, _inner_tuple_data);
    if (ret < 0) {
        return -1;
    }
    _inner_iter = _inner_tuple_data.begin();
    return 0;
}

int ApplyNode::hash_apply(RuntimeState* state) {
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
        DB_WARNING("no data");
        _outer_table_is_null = true;
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
        DB_WARNING("ExecNode::inner table open fail");
        return -1;
    }
    if (_join_type == pb::LEFT_JOIN || _join_type == pb::ANTI_SEMI_JOIN) {
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

int ApplyNode::loop_hash_apply(RuntimeState* state) {
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
//    DB_WARNING("data size:%ld", _outer_tuple_data.size());
    _outer_iter = _outer_tuple_data.begin();

    std::vector<MemRow*> outer_tuple_data;
    outer_tuple_data.reserve(get_loop_num());
    while (_outer_iter != _outer_tuple_data.end() && outer_tuple_data.size() < get_loop_num()) {
        outer_tuple_data.emplace_back(*_outer_iter);
        _outer_iter++;
    }
    if (outer_tuple_data.size() == 0) {
        DB_WARNING("no data");
        _outer_table_is_null = true;
        return 0;
    }
    _hash_map.clear();
    construct_hash_map(outer_tuple_data, _outer_equal_slot);

    if (_is_explain) {
        _inner_node->create_trace();
        ret = _inner_node->open(state);
        if (ret < 0) {
            DB_WARNING("ExecNode::inner table open fail");
            return -1;
        }
        return 0;
    }
    _loops = 0;
    _inner_node->get_node(pb::SCAN_NODE, _scan_nodes);
    ret = Joiner::fetcher_inner_table_data(state, outer_tuple_data, _inner_tuple_data);
    if (ret < 0) {
        return -1;
    }
    _inner_iter = _inner_tuple_data.begin();
    return 0;
}
int ApplyNode::get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
    if (_outer_table_is_null) {
        *eos = true;
        return 0;
    }
    if (_use_loop_hash_map) {
        return get_next_via_loop_outer_hash_map(state, batch, eos);
    }
    if (_use_hash_map) {
        if (_join_type == pb::LEFT_JOIN || _join_type == pb::ANTI_SEMI_JOIN) {
            return get_next_via_inner_hash_map(state, batch, eos);
        }
        return get_next_via_outer_hash_map(state, batch, eos);
    }
    return get_next_for_nested_loop_join(state, batch, eos);
}

int ApplyNode::fetcher_inner_table_data(RuntimeState* state,
                            MemRow* outer_tuple_data,
                            std::vector<ExecNode*>& scan_nodes,
                            std::vector<MemRow*>& inner_tuple_data) {
    _outer_join_values.clear();
    std::vector<MemRow*> tuple_data;
    //DB_WARNING("inter row:%s ", outer_tuple_data->debug_string(0).c_str());
    tuple_data.emplace_back(outer_tuple_data);
    construct_equal_values(tuple_data, _outer_equal_slot);
    std::vector<ExprNode*> in_exprs;
    int ret = construct_in_condition(_inner_equal_slot, _outer_join_values, in_exprs);
    if (ret < 0) {
        DB_WARNING("ExecNode::create in condition for right table fail");
        return ret;
    }
    std::vector<ExprNode*> in_exprs_back = in_exprs;
    //表达式下推，下推的那个节点重新做索引选择，路由选择
    _inner_node->predicate_pushdown(in_exprs);
    if (in_exprs.size() > 0) {
        DB_WARNING("inner node add filter node");
        _inner_node->add_filter_node(in_exprs);
    }
    std::map<int64_t, ExprValue> join_values;
    for (auto& sign : _slot_ref_sign_set) {
        ExprValue value = outer_tuple_data->get_value(sign >> 16, sign & 0xffff);
        join_values.emplace(sign, value);
    }
    for (auto& iter : _literal_maps) {
        ExprValue& value = join_values[iter.first];
        for (auto& literal : iter.second) {
            ((Literal*)literal)->init(value);
        }
    }
    bool index_has_null = false;
    do_plan_router(state, scan_nodes, index_has_null);
    if (index_has_null) {
        _inner_node->set_return_empty();
    }
    _inner_node->create_trace();
    ret = _inner_node->open(state);
    if (ret < 0) {
        DB_WARNING("ExecNode::inner table open fail");
        return -1;
    }
    ret = fetcher_full_table_data(state, _inner_node, inner_tuple_data);
    if (ret < 0) {
        DB_WARNING("fetcher inner node fail");
        return ret;
    }
    if (_max_one_row && inner_tuple_data.size() > 1) {
        state->error_code = ER_SUBQUERY_NO_1_ROW;
        state->error_msg << "Subquery returns more than 1 row";
        DB_WARNING("Subquery returns more than 1 row");
        return -1;
    }
    
    _inner_node->remove_additional_predicate(in_exprs_back);
    _inner_node->close(state);
    return 0;
}

int ApplyNode::get_next_for_nested_loop_join(RuntimeState* state, RowBatch* batch, bool* eos) {
    TimeCost get_next_time;
    while (1) {
        if (_outer_iter == _outer_tuple_data.end()) {
            DB_WARNING("when join, outer iter is end, time_cost:%ld", get_next_time.get_time());
            *eos = true;
            return 0;
        }
        if (_inner_iter == _inner_tuple_data.end()) {
            for (auto& mem_row : _inner_tuple_data) {
                delete mem_row;
            }
            _inner_tuple_data.clear();
            int ret = fetcher_inner_table_data(state, *_outer_iter, _scan_nodes, _inner_tuple_data);
            if (ret < 0) {
                return -1;
            }
            _inner_iter = _inner_tuple_data.begin();
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
            if (matched && _join_type == pb::SEMI_JOIN) {
                _inner_iter = _inner_tuple_data.end();
                break;
            } else {
                ++_inner_iter;
            }
        }
        if (!matched) {
            switch (_join_type) {
            case pb::LEFT_JOIN:
            case pb::ANTI_SEMI_JOIN: {
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
    }
    return 0;
}

int ApplyNode::get_next_via_inner_hash_map(RuntimeState* state, RowBatch* batch, bool* eos) {
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
            if (_join_type == pb::ANTI_SEMI_JOIN) {
                ++_outer_iter;
                _result_row_index = 0;
                continue;
            }
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
                _has_matched |= matched;
                _all_matched &= matched;
                ++_num_rows_returned;
            }
            if (_has_matched && _join_type == pb::LEFT_JOIN) {
                if (_compare_type == pb::CMP_ALL && _all_matched) {
                    int ret = construct_null_result_batch(batch, *_outer_iter);
                    if (ret < 0) {
                        DB_WARNING("construct result batch fail");
                        return ret;
                    }
                }
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
        _has_matched = false;
        _all_matched = true;
    }
    return 0;
}

int ApplyNode::get_next_via_outer_hash_map(RuntimeState* state, RowBatch* batch, bool* eos) {
    while (1) {
        if (_inner_row_batch.is_traverse_over()) {
            if (_child_eos) {
                *eos = true;
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
                _has_matched |= matched;
                _all_matched &= matched;
                ++_num_rows_returned;
            }
            if (_has_matched && _join_type == pb::SEMI_JOIN) {
                if (_compare_type == pb::CMP_ALL && _all_matched) {
                    int ret = construct_null_result_batch(batch, (*outer_mem_rows)[_result_row_index]);
                    if (ret < 0) {
                        DB_WARNING("construct result batch fail");
                        return ret;
                    }
                }
                _hash_map.erase(inner_key.data());
            }
        }
        _result_row_index = 0;
        _has_matched = false;
        _all_matched = true;
        _inner_row_batch.next();
    }
    return 0;
}

int ApplyNode::get_next_via_loop_outer_hash_map(RuntimeState* state, RowBatch* batch, bool* eos) {
    if (_inner_iter == _inner_tuple_data.end()) {
        // clear previous inner
        for (auto& mem_row : _inner_tuple_data) {
            delete mem_row;
        }
        _inner_tuple_data.clear();

        // construct outer
        if (_outer_iter == _outer_tuple_data.end()) {
            DB_WARNING("when join, outer iter is end, loops:%lu", _loops);
            *eos = true;
            return 0;
        }
        std::vector<MemRow*> outer_tuple_data;
        outer_tuple_data.reserve(get_loop_num());
        while (_outer_iter != _outer_tuple_data.end() && outer_tuple_data.size() < get_loop_num()) {
            outer_tuple_data.emplace_back(*_outer_iter);
            _outer_iter++;
        }
        _hash_map.clear();
        construct_hash_map(outer_tuple_data, _outer_equal_slot);

        // fetcher inner
        int ret = Joiner::fetcher_inner_table_data(state, outer_tuple_data, _inner_tuple_data);
        if (ret < 0) {
            return -1;
        }
        _inner_iter = _inner_tuple_data.begin();
    }

    while (_inner_iter != _inner_tuple_data.end()) {
        MemRow* inner_mem_row = *_inner_iter;
        MutTableKey inner_key;
        encode_hash_key(inner_mem_row, _inner_equal_slot, inner_key);
        auto outer_mem_rows = _hash_map.seek(inner_key.data());
        if (outer_mem_rows != NULL) {
            for (; _result_row_index < outer_mem_rows->size(); ++_result_row_index) {
                if (reached_limit()) {
                    *eos = true;
                    return 0;
                }
                if (batch->is_full()) {
                    return 0;
                }
                bool matched = false;
                int ret = construct_result_batch(batch, (*outer_mem_rows)[_result_row_index], inner_mem_row, matched);
                if (ret < 0) {
                    DB_WARNING("construct result batch fail");
                    return ret;
                }
                _has_matched |= matched;
                _all_matched &= matched;
                ++_num_rows_returned;
            }
            if (_has_matched && _join_type == pb::SEMI_JOIN) {
                if (_compare_type == pb::CMP_ALL && _all_matched) {
                    int ret = construct_null_result_batch(batch, (*outer_mem_rows)[_result_row_index]);
                    if (ret < 0) {
                        DB_WARNING("construct result batch fail");
                        return ret;
                    }
                }
                _hash_map.erase(inner_key.data());
            }
        }
        _result_row_index = 0;
        _has_matched = false;
        _all_matched = true;
        _inner_iter++;
    }

    return 0;
}
}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
