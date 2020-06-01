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
    ret = ExecNode::init(node);
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
    _hash_map.init(12301);
    return 0;
}
int JoinNode::expr_optimize(std::vector<pb::TupleDescriptor>* tuple_descs) {
    int ret = 0;
    ret = ExecNode::expr_optimize(tuple_descs);
    if (ret < 0) {
        DB_WARNING("ExecNode::optimize fail, ret:%d", ret);
        return ret;
    }
    auto iter = _conditions.begin();
    while (iter != _conditions.end()) {
        auto expr = *iter;
        //类型推导 
        ret = expr->expr_optimize();
        if (ret < 0) {
            DB_WARNING("expr type_inferer fail:%d", ret);
            return ret;
        }
        if (expr->is_constant()) {
            expr->open();
            ExprValue value = expr->get_value(nullptr);
            expr->close();
            if (value.is_null() || value.get_numberic<bool>() == false) {
                // todo, 三种不同的join优化方式不同
            } else {
                ExprNode::destroy_tree(expr);
                iter = _conditions.erase(iter);
                continue;
            }
        }
        ++iter;
    }
    return 0; 
}

int JoinNode::predicate_pushdown(std::vector<ExprNode*>& input_exprs) {
    //DB_WARNING("node:%ld is pushdown", this);
    convert_to_inner_join(input_exprs);
    
    std::vector<ExprNode*> outer_push_exprs;
    std::vector<ExprNode*> inner_push_exprs;
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
                && !expr->contains_special_operator(pb::IS_NULL_PREDICATE)) {
            set_join_type(pb::INNER_JOIN);
            return;
        }
        if (contains_expr(expr) && !expr->contains_special_operator(pb::IS_NULL_PREDICATE)
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
    auto ret = _fill_equal_slot();
    if (ret < 0) {
        DB_WARNING("fill equal slot fail");
        return -1;
    }
    if (_outer_equal_slot.size() == 0) {
        DB_WARNING("has no equal condition in join");
        return -1;
    }
    //DB_WARNING("_outer_node:%ld _inner_node:%ld", _outer_node, _inner_node);
    //for (auto& tuple_id : _outer_tuple_ids) {
    //    DB_WARNING("_outer tuple_id:%d", tuple_id);
    //}
    //for (auto& tuple_id : _inner_tuple_ids) {
    //    DB_WARNING("_inner tuple_id:%d", tuple_id);
    //}
    //for (auto& expr_node : _outer_equal_slot) {
    //    DB_WARNING("outer join condition, slot_id:%d, tuple_id:%d", 
    //                    static_cast<SlotRef*>(expr_node)->slot_id(),
    //                    static_cast<SlotRef*>(expr_node)->tuple_id());
    //}
    //for (auto& expr_node : _inner_equal_slot) {
    //    DB_WARNING("inner join condition, slot_id:%d, tuple_id:%d",
    //                static_cast<SlotRef*>(expr_node)->slot_id(),
    //                static_cast<SlotRef*>(expr_node)->tuple_id());
    //}
    _mem_row_desc = state->mem_row_desc();
    //DB_WARNING("when join, init join open, time_cost:%ld", join_time_cost.get_time());
    join_time_cost.reset();
    ret = _outer_node->open(state);
    if (ret < 0) {
        DB_WARNING("ExecNode:: left table open fail");
        return ret;
    }
    //DB_WARNING("when join, outer join open(fetcher data), time_cost:%ld", join_time_cost.get_time());
    join_time_cost.reset();
    //从左表中把全部数据拿出
    ret = _fetcher_join_table(state, _outer_node, _outer_tuple_data);
    if (ret < 0) {
        DB_WARNING("ExecNode::join open fail when fetch left table");
        return ret;
    }
    if (_outer_tuple_data.size() == 0) {
        _outer_table_is_null = true;
        return 0;
    }
    //DB_WARNING("when join, fetch outer data size:%d, time_cost:%ld", 
    //            _outer_tuple_data.size(), join_time_cost.get_time());
    join_time_cost.reset();
    _save_join_value(_outer_tuple_data, _outer_equal_slot);
    //DB_WARNING("when join, save join value, time_cost:%ld",
    //            join_time_cost.get_time());
    join_time_cost.reset();
    std::vector<ExprNode*> in_exprs;
    //驱动表表返回的join条件下推 todo
    ret = _construct_in_condition(_inner_equal_slot, _outer_join_values, in_exprs);
    if (ret < 0) {
        DB_WARNING("ExecNode::create in condition for right table fail");
        return ret;
    }
    //DB_WARNING("when join, _construct_in_condition, time_cost:%ld",
    //            join_time_cost.get_time());
    join_time_cost.reset();
    //表达式下推，下推的那个节点重新做索引选择，路由选择
    _inner_node->predicate_pushdown(in_exprs);
    if (in_exprs.size() > 0) {
        DB_WARNING("inner node add filter node");
        _inner_node->add_filter_node(in_exprs);
    }
    //DB_WARNING("when join,  re predicate pushdown, time_cost:%ld",
    //            join_time_cost.get_time());
    join_time_cost.reset();

    std::vector<ExecNode*> scan_nodes;
    _inner_node->get_node(pb::SCAN_NODE, scan_nodes);
    //重新做路由选择
    for (auto& exec_node : scan_nodes) {
        RocksdbScanNode* scan_node = static_cast<RocksdbScanNode*>(exec_node);
        ExecNode* parent_node_ptr = scan_node->get_parent();
        FilterNode* filter_node = nullptr;
        if (parent_node_ptr->node_type() == pb::WHERE_FILTER_NODE
                || parent_node_ptr->node_type() == pb::TABLE_FILTER_NODE) {
            filter_node = static_cast<FilterNode*>(parent_node_ptr);
        }

        auto get_slot_id = [state](int32_t tuple_id, int32_t field_id) ->
                int32_t {return state->get_slot_id(tuple_id, field_id);};

        auto get_tuple_desc = [state] (int32_t tuple_id)->
        pb::TupleDescriptor* { return state->get_tuple_desc(tuple_id);};
        
        scan_node->clear_possible_indexes();
        //索引选择
        IndexSelector().index_selector(state->tuple_descs(),
                                        scan_node, 
                                        filter_node,
                                        NULL,
                                        NULL,
                                        NULL);
        if (!_is_explain) {
            //路由选择,
            //这一块做完索引选择之后如果命中二级索引需要重构mem_row的结构，mem_row已经在run_time
            //init中构造了，需要销毁重新搞(todo)
            PlanRouter().scan_plan_router(scan_node, get_slot_id, get_tuple_desc, false);
            SelectManagerNode* related_manager_node = scan_node->get_related_manager_node();
            auto region_infos = scan_node->region_infos();
            //更改scan_node对应的fethcer_node的region信息
            related_manager_node->set_region_infos(region_infos);
        }
    }
    //DB_WARNING("when join, index_selector and scan plan, time_cost:%ld",
    //            join_time_cost.get_time());
    join_time_cost.reset();
    //_inner_node->print_all_exec_node();
    //谓词下推后可能生成新的plannode重新生成tracenode
    _inner_node->create_trace();
    ret = _inner_node->open(state);
    if (ret < 0) {
        DB_WARNING("ExecNode::inner table open fial");
        return -1;
    }
    //DB_WARNING("when join, _inner_node open(fetcher data), time_cost:%ld",
    //            join_time_cost.get_time());
    join_time_cost.reset();
    if (_join_type == pb::LEFT_JOIN 
            || _join_type == pb::RIGHT_JOIN) {
        join_time_cost.reset();
        ret = _fetcher_join_table(state, _inner_node, _inner_tuple_data);
        if (ret < 0) {
            DB_WARNING("fetcher inner node fail");
            return ret;
        }
        //DB_WARNING("when join, fetch inner data size:%d, time_cost:%ld", 
        //            _outer_tuple_data.size(), join_time_cost.get_time());
        join_time_cost.reset();
        _construct_hash_map(_inner_tuple_data, _inner_equal_slot);
        //DB_WARNING("when join, _construct_hash_map time_cost:%ld", join_time_cost.get_time());
        _outer_iter = _outer_tuple_data.begin();
    } else {
        join_time_cost.reset();
        _construct_hash_map(_outer_tuple_data, _outer_equal_slot);
        //DB_WARNING("when join, _construct_hash_map time_cost:%ld", join_time_cost.get_time());
    } 
    return 0;
}

int JoinNode::_fill_equal_slot() {
    auto iter = _conditions.begin();
    //因为目前不支持filed_a, filed_b in ((1, "str"), (2,
    //"str"))这种in操作，所以剪枝只能剪掉一个等值操作符
    bool remove_condition = false; //限制只能剪掉一次
    _outer_equal_slot.clear();
    _inner_equal_slot.clear();

    while (iter != _conditions.end()) {
        auto expr = *iter;
        if (_is_equal_condition(expr) && !remove_condition) {
            iter = _conditions.erase(iter);
            remove_condition = true;
            _have_removed.push_back(expr);
        } else {
            auto ret = expr->open();
            if (ret < 0) {
                DB_WARNING("expr open fail, ret:%d", ret);
                return ret;
            }
            ++iter;
        } 
    }
    return 0;
}
bool JoinNode::_is_equal_condition(ExprNode* expr) {
    if (expr->node_type() != pb::FUNCTION_CALL 
        || static_cast<ScalarFnCall*>(expr)->fn().fn_op() != parser::FT_EQ) {
        return false;
    }
    if (expr->children_size() != 2) {
        return false;
    }
    ExprNode* left_child = expr->children(0);
    ExprNode* right_child = expr->children(1);
    if (left_child->node_type() != pb::SLOT_REF
            || right_child->node_type() != pb::SLOT_REF) {
        return false;
    }
    int32_t left_tuple_id = static_cast<SlotRef*>(left_child)->tuple_id();
    int32_t right_tuple_id = static_cast<SlotRef*>(right_child)->tuple_id();
    if (_outer_tuple_ids.count(left_tuple_id) == 1 
            && _inner_tuple_ids.count(right_tuple_id) == 1) {
        _outer_equal_slot.push_back(left_child);
        _inner_equal_slot.push_back(right_child);
        return true;
    } else if (_outer_tuple_ids.count(right_tuple_id) == 1
            && _inner_tuple_ids.count(left_tuple_id) == 1) {
        _outer_equal_slot.push_back(right_child);
        _inner_equal_slot.push_back(left_child);
        return true;
    }
    return false;
}

int JoinNode::_construct_in_condition(std::vector<ExprNode*>& slot_refs, 
                             std::vector<std::vector<ExprValue>>& in_values, 
                             std::vector<ExprNode*>& in_exprs) {
    //手工构造pb格式的表达式，再转为内存结构的表达式
    if (slot_refs.size() == 0) {
        return 0;
    } else if (slot_refs.size() == 1) {
        pb::Expr expr;
        ExprNode* conjunct = nullptr;
        //增加一个in
        pb::ExprNode* in_node = expr.add_nodes();
        in_node->set_node_type(pb::IN_PREDICATE);
        in_node->set_col_type(pb::BOOL);
        pb::Function* func = in_node->mutable_fn();
        func->set_name("in");
        func->set_fn_op(parser::FT_IN);
        in_node->set_num_children(1);
        //增加一个slot_ref
        pb::ExprNode* slot_node = expr.add_nodes();
        slot_node->set_node_type(pb::SLOT_REF);
        slot_node->set_col_type(slot_refs[0]->col_type());
        slot_node->set_num_children(0);
        slot_node->mutable_derive_node()->set_tuple_id(static_cast<SlotRef*>(slot_refs[0])->tuple_id());
        slot_node->mutable_derive_node()->set_slot_id(static_cast<SlotRef*>(slot_refs[0])->slot_id());
        slot_node->mutable_derive_node()->set_field_id(static_cast<SlotRef*>(slot_refs[0])->field_id());
        auto ret = ExprNode::create_tree(expr, &conjunct);
        if (ret < 0) {
            //如何释放资源
            DB_WARNING("create in condition fail");
            return ret;
        }
        for (auto& in_value : in_values) {
            ExprNode* literal_node = new Literal(in_value[0]);
            conjunct->add_child(literal_node); 
        }
        conjunct->type_inferer();
        in_exprs.push_back(conjunct);
        return 0;
    } else {
        pb::Expr expr;
        ExprNode* conjunct = nullptr;
        //增加一个in
        pb::ExprNode* in_node = expr.add_nodes();
        in_node->set_node_type(pb::IN_PREDICATE);
        in_node->set_col_type(pb::BOOL);
        pb::Function* func = in_node->mutable_fn();
        func->set_name("in");
        func->set_fn_op(parser::FT_IN);
        in_node->set_num_children(0);
        auto ret = ExprNode::create_tree(expr, &conjunct);
        if (ret < 0) {
            //如何释放资源
            DB_WARNING("create in condition fail");
            return ret;
        }
        //增加一个row_expr
        RowExpr* row_expr = new RowExpr;
        for (auto& slot : slot_refs) {
            row_expr->add_child(static_cast<SlotRef*>(slot)->clone());
        }
        conjunct->add_child(row_expr);
        for (auto& in_value : in_values) {
            //增加一个row_expr
            RowExpr* row_expr = new RowExpr;
            for (auto val : in_value) {
                ExprNode* literal_node = new Literal(val);
                row_expr->add_child(literal_node);
            }
            conjunct->add_child(row_expr); 
        }
        conjunct->type_inferer();
        in_exprs.push_back(conjunct);
        return 0;
    }
    return 0;
}

int JoinNode::_fetcher_join_table(RuntimeState* state, ExecNode* child_node,
                                  std::vector<MemRow*>& tuple_data) {
    bool eos = false;
    do {
        RowBatch batch;
        auto ret = child_node->get_next(state, &batch, &eos);
        if (ret < 0) {
            DB_WARNING("children:get_next fail:%d", ret);
            return ret;
        }
        for (batch.reset(); !batch.is_traverse_over(); batch.next()) {
            tuple_data.push_back(batch.get_row().release());
        }
    } while (!eos);
    return 0;
}

void JoinNode::_construct_hash_map(const std::vector<MemRow*>& tuple_data, 
                                  const std::vector<ExprNode*>& slot_refs) {
    for (auto& mem_row : tuple_data) {
        MutTableKey key;
        _encode_hash_key(mem_row, slot_refs, key);
        _hash_map[key.data()].push_back(mem_row);
    } 
}

void JoinNode::_save_join_value(const std::vector<MemRow*>& tuple_data,
                                const std::vector<ExprNode*>& slot_refs) {
    for (auto& mem_row : tuple_data) {
        std::vector<ExprValue> join_values;
        for (auto& slot_ref_expr : slot_refs) {
            ExprValue value = mem_row->get_value(static_cast<SlotRef*>(slot_ref_expr)->tuple_id(), 
                                             static_cast<SlotRef*>(slot_ref_expr)->slot_id());
            join_values.push_back(value);
        }
        _outer_join_values.push_back(join_values);
    }
}

void JoinNode::_encode_hash_key(MemRow* row, 
                     const std::vector<ExprNode*>& slot_ref_exprs,
                     MutTableKey& key) {
    for (auto& slot_ref_expr : slot_ref_exprs) {
        ExprValue value = row->get_value(static_cast<SlotRef*>(slot_ref_expr)->tuple_id(), 
                                         static_cast<SlotRef*>(slot_ref_expr)->slot_id());
        key.append_value(value.cast_to(pb::STRING)); 
    }
}

int JoinNode::get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
    if (_outer_table_is_null) {
        *eos = true;
        return 0;
    }
    if (_join_type == pb::INNER_JOIN) {
        return get_next_for_inner_join(state, batch, eos);
    } else {
        return get_next_for_other_join(state, batch, eos);
    }
}

int JoinNode::get_next_for_other_join(RuntimeState* state, RowBatch* batch, bool* eos) {
    TimeCost get_next_time;
    while (1) {
        if (_outer_iter == _outer_tuple_data.end()) {
            DB_WARNING("when join, outer iter is end, time_cost:%ld", get_next_time.get_time());
            *eos = true;
            return 0;
        }
        MutTableKey outer_key;
        _encode_hash_key(*_outer_iter, _outer_equal_slot, outer_key);
        auto inner_mem_rows = _hash_map.seek(outer_key.data());
        if (inner_mem_rows != NULL) {
            for (; _hash_mapped_index < inner_mem_rows->size(); ++_hash_mapped_index) {
                if (reached_limit()) {
                    DB_WARNING("when join, reach limit size:%u, time_cost:%ld", 
                            batch->size(), get_next_time.get_time());
                    *eos = true;
                    return 0;
                }
                if (batch->is_full()) {
                     DB_WARNING("when join, batch is full, time_cost:%ld", get_next_time.get_time());
                    return 0;
                }
                //DB_WARNING("construct result batch");
                auto ret = _construct_result_batch(batch,
                                                   *_outer_iter,
                                                   (*inner_mem_rows)[_hash_mapped_index],
                                                   false);
                if (ret < 0) {
                    DB_WARNING("construct result batch fail");
                    return ret;
                }
                ++_num_rows_returned;
            }
        } else {
            //fill NULL
            if (reached_limit()) {
                DB_WARNING("when join, reach limit size:%u, time_cost:%ld", 
                            batch->size(), get_next_time.get_time());
                *eos = true;
                return 0;
            }
            if (batch->is_full()) {
                DB_WARNING("when join, batch is full, time_cost:%ld", get_next_time.get_time());
                return 0;
            }
            auto ret = _construct_result_batch(batch, *_outer_iter, NULL, false);
            if (ret < 0) {
                DB_WARNING("construct result batch fail");
                return ret;
            }
            ++_num_rows_returned;
        }
        _hash_mapped_index = 0;
        ++_outer_iter;
    }
    return 0;
}
int JoinNode::get_next_for_inner_join(RuntimeState* state, RowBatch* batch, bool* eos) {
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
                auto ret = _inner_node->get_next(state, &_inner_row_batch, &_child_eos);
                if (ret < 0) {
                    DB_WARNING("_children get_next fail");
                    return ret;
                }
                DB_WARNING("when join, get_row from inner table success, batch_size:%d, time_cost:%ld", 
                        _inner_row_batch.size(), get_next_time.get_time());
                continue;
            }
        }
        std::unique_ptr<MemRow>& inner_mem_row = _inner_row_batch.get_row();
        MutTableKey inner_key;
        _encode_hash_key(inner_mem_row.get(), _inner_equal_slot, inner_key);
        auto outer_mem_rows = _hash_map.seek(inner_key.data());
        if (outer_mem_rows != NULL) {
            for (; _hash_mapped_index < outer_mem_rows->size(); ++_hash_mapped_index) {
                if (reached_limit()) {
                    DB_WARNING("when join, reach limit size:%u, time_cost:%ld", 
                                batch->size(), get_next_time.get_time());
                    *eos = true;
                    return 0;
                }
                if (batch->is_full()) {
                    DB_WARNING("when join, batch is full, time_cost:%ld", 
                                get_next_time.get_time());
                    return 0;
                }
                //DB_WARNING("construct reslut batch");
                //(*outer_mem_rows)[_hash_mapped_index]->print_content();
                //inner_mem_row.get()->print_content();
                auto ret = _construct_result_batch(batch, (*outer_mem_rows)[_hash_mapped_index], inner_mem_row.get(), true);
                if (ret < 0) {
                    DB_WARNING("construct result batch fail");
                    return ret;
                }
                ++_num_rows_returned;
            }
        }
        //DB_WARNING("outer mem rows trarvers over");
        _hash_mapped_index = 0; 
        _inner_row_batch.next();
    }
    return 0;
}
inline bool JoinNode::_satisfy_filter(MemRow* row) {
    for (auto& condition : _conditions) {
        ExprValue value = condition->get_value(row);
        if (value.is_null() || value.get_numberic<bool>() == false) {
            return false;
        }
    }
    return true;
}
int JoinNode::_construct_result_batch(RowBatch* batch, 
                                      MemRow* outer_mem_row, 
                                      MemRow* inner_mem_row,
                                      bool  inner_join) {
    //if (outer_mem_row != NULL) {
    //    outer_mem_row->print_content();
    //}
    //if (inner_mem_row != NULL) {
    //    inner_mem_row->print_content();
    //}
    std::unique_ptr<MemRow> row = _mem_row_desc->fetch_mem_row();
    int ret = 0;
    if (outer_mem_row != NULL) {
        ret = row->copy_from(_outer_tuple_ids, outer_mem_row);
        if (ret < 0) {
            DB_WARNING("copy from left row fail");
            return -1;
        }
    }
    if (inner_mem_row != NULL) {
        ret = row->copy_from(_inner_tuple_ids, inner_mem_row);
        if (ret < 0) {
            DB_WARNING("copy from  row fail");
            return -1;
        }
    }
    if (_satisfy_filter(row.get())) {
        batch->move_row(std::move(row));
        //row->print_content();
    } else if (!inner_join) {
        return _construct_null_result_batch(batch, outer_mem_row);
    } 
    return 0;
}
int JoinNode:: _construct_null_result_batch(RowBatch* batch, MemRow* outer_mem_row) {
    std::unique_ptr<MemRow> row = _mem_row_desc->fetch_mem_row();
    int ret = 0;
    if (outer_mem_row != NULL) {
        ret = row->copy_from(_outer_tuple_ids, outer_mem_row);
        if (ret < 0) {
            DB_WARNING("copy from left row fail");
            return -1;
        }
    }
    batch->move_row(std::move(row));
    return 0;
}

void JoinNode::close(RuntimeState* state) {
    ExecNode::close(state);
    _have_removed.clear();
    _outer_join_values.clear();
    for (auto expr : _conditions) {
        expr->close();
    }
    for (auto& mem_row : _outer_tuple_data) {
        delete mem_row;
    }
    _outer_tuple_data.clear();
    for (auto& mem_row : _inner_tuple_data) {
        delete mem_row;
    }
    _inner_tuple_data.clear();
    _hash_map.clear();
    _hash_mapped_index = 0;
    _outer_table_is_null = false;
    _inner_row_batch.clear();
    _child_eos = false;
}

void JoinNode::find_place_holder(std::map<int, ExprNode*>& placeholders) {
    ExecNode::find_place_holder(placeholders);
    for (auto& expr : _conditions) {
        expr->find_place_holder(placeholders);
    }
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
        _is_equal_condition(expr);
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
void JoinNode::show_explain(std::vector<std::map<std::string, std::string>>& output) {
    _outer_node->show_explain(output);
    _inner_node->show_explain(output);
}
}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
