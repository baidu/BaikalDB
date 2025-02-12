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
#include "dual_scan_node.h"
#include "filter_node.h"
#include "expr_node.h"
#include "rocksdb_scan_node.h"
#include "scalar_fn_call.h"
#include "index_selector.h"
#include "plan_router.h"
#include "logical_planner.h"
#include "literal.h"
#include "vectorize_helpper.h"

namespace baikaldb {
DECLARE_bool(use_arrow_vector);

int Joiner::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    _outer_join_values.init(12301);
    _hash_map.init(12301);
    return 0;
}
int Joiner::expr_optimize(QueryContext* ctx) {
    int ret = 0;
    ret = ExecNode::expr_optimize(ctx);
    if (ret < 0) {
        DB_WARNING("ExecNode::optimize fail, ret:%d", ret);
        return ret;
    }
    auto iter = _conditions.begin();
    while (iter != _conditions.end()) {
        auto expr = *iter;
        //类型推导
        if (_is_apply) {
            expr->disable_replace_agg_to_slot();
        }
        ret = expr->expr_optimize();
        if (ret < 0) {
            DB_WARNING("expr type_inferer fail:%d", ret);
            return ret;
        }
        if (expr->is_constant()) {
            expr->open();
            ExprValue value = expr->get_value(nullptr);
            expr->close();
            if (value.is_null() || !value.get_numberic<bool>()) {
                // todo, 三种不同的join优化方式不同
            } else {
                ExprNode::destroy_tree(expr);
                iter = _conditions.erase(iter);
                continue;
            }
        }
        if (expr->has_agg()) {
            _conditions_has_agg = true;
        }
        ++iter;
    }
    return 0; 
}

bool Joiner::is_slot_ref_equal_condition(ExprNode* left, ExprNode* right) {
    if (left->node_type() != pb::SLOT_REF
            || right->node_type() != pb::SLOT_REF) {
        return false;
    }
    int32_t left_tuple_id = static_cast<SlotRef*>(left)->tuple_id();
    int32_t right_tuple_id = static_cast<SlotRef*>(right)->tuple_id();
    if (_outer_tuple_ids.count(left_tuple_id) == 1 
            && _inner_tuple_ids.count(right_tuple_id) == 1) {
        _outer_equal_slot.emplace_back(left);
        _inner_equal_slot.emplace_back(right);
        _inner_equal_field_ids[right_tuple_id].emplace(static_cast<SlotRef*>(right)->field_id());
        return true;
    } else if (_outer_tuple_ids.count(right_tuple_id) == 1
            && _inner_tuple_ids.count(left_tuple_id) == 1) {
        _outer_equal_slot.emplace_back(right);
        _inner_equal_slot.emplace_back(left);
        _inner_equal_field_ids[left_tuple_id].emplace(static_cast<SlotRef*>(left)->field_id());
        return true;
    }
    return false;
}

bool Joiner::expr_is_equal_condition_and_build_slot(ExprNode* expr) {
    if (expr->node_type() != pb::FUNCTION_CALL 
        || static_cast<ScalarFnCall*>(expr)->fn().fn_op() != parser::FT_EQ) {
        return false;
    }
    if (expr->children_size() != 2) {
        return false;
    }
    ExprNode* left_child = expr->children(0);
    ExprNode* right_child = expr->children(1);
    if (left_child->is_row_expr() && right_child->is_row_expr()) {
        for (size_t i = 0; i < left_child->children_size(); i++) {
            if (!is_slot_ref_equal_condition(left_child->children(i), right_child->children(i))) {
                return false;
            }
        }
        return true;
    }
    return is_slot_ref_equal_condition(left_child, right_child);
}

void Joiner::construct_in_condition_placeholder(ExecNode* child, 
                                                std::vector<ExprNode*>& equal_slot, 
                                                std::unordered_map<ExecNode*, std::vector<ExprNode*>>& condition_filter) {
    if (child == nullptr) {
        return;
    }
    if (child->node_type() == pb::JOIN_NODE) {
        static_cast<JoinNode*>(child)->get_join_on_condition_filter(condition_filter);
    } 
    if (!FLAGS_use_arrow_vector) {
        return;
    }
    std::vector<ExecNode*> scan_nodes;
    child->get_node(pb::SCAN_NODE, scan_nodes);
    if (scan_nodes.size() != 1) {
        return;
    }
    std::vector<ExprNode*> in_expr;
    ExprValueVec vec_values;
    ExprValueSet in_values;
    in_values.init(1);
    vec_values.vec.reserve(equal_slot.size());
    for (auto& slot : equal_slot) {
        ExprValue value(pb::PLACE_HOLDER);
        vec_values.vec.emplace_back(value);
    }
    in_values.insert(vec_values);
    if (construct_in_condition(equal_slot, in_values, in_expr)) {
        DB_FATAL("construct left table in condition fail");
        return;
    }
    if (in_expr.size() == 1) {
        condition_filter[scan_nodes[0]].emplace_back(in_expr[0]);
    }
}

void Joiner::get_join_on_condition_filter(std::unordered_map<ExecNode*, std::vector<ExprNode*>>& condition_filter) {
    if (_inner_node == nullptr || _outer_node == nullptr) {
        // 子查询没做PredicatePushDown
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
    }
    for (auto& expr : _conditions) {
        expr_is_equal_condition_and_build_slot(expr);
    }
    construct_in_condition_placeholder(_inner_node, _inner_equal_slot, condition_filter);
    construct_in_condition_placeholder(_outer_node, _outer_equal_slot, condition_filter);
}

int Joiner::strip_out_equal_slots() {
    auto iter = _conditions.begin();
    _outer_equal_slot.clear();
    _inner_equal_slot.clear();
    while (iter != _conditions.end()) {
        auto expr = *iter;
        if (expr_is_equal_condition_and_build_slot(expr)) {
            iter = _conditions.erase(iter);
            _have_removed.emplace_back(expr);
        } else {
            auto ret = expr->open();
            if (ret < 0) {
                DB_WARNING("expr open fail, ret:%d", ret);
                return ret;
            }
            ++iter;
        }
    }
    if (_outer_equal_slot.size() == 0) {
        _use_hash_map = false;
        DB_WARNING("has not eq");
    }
    return 0;
}

int Joiner::do_plan_router(RuntimeState* state, std::vector<ExecNode*>& scan_nodes, bool& index_has_null) {
    QueryContext* ctx = state->ctx();
    if (ctx == nullptr) {
        DB_FATAL("ctx is nullptr");
        return -1;
    }
    //重新做路由选择
    for (auto& exec_node : scan_nodes) {
        RocksdbScanNode* scan_node = static_cast<RocksdbScanNode*>(exec_node);
        if (scan_node->engine() == pb::INFORMATION_SCHEMA) {
            continue;
        }
        ExecNode* parent_node_ptr = scan_node->get_parent();
        FilterNode* filter_node = nullptr;
        if (parent_node_ptr->node_type() == pb::WHERE_FILTER_NODE
                || parent_node_ptr->node_type() == pb::TABLE_FILTER_NODE) {
            filter_node = static_cast<FilterNode*>(parent_node_ptr);
            // FIXME: a in (1, 2) and a > 0优化成a in (1, 2)，filter_node需要进行expr_optimize
        }
        SortNode* sort_node = nullptr;
        while (parent_node_ptr != nullptr
                && parent_node_ptr->node_type() != pb::SELECT_MANAGER_NODE
                && parent_node_ptr->node_type() != pb::JOIN_NODE) {
            if (parent_node_ptr->node_type() == pb::SORT_NODE) {
                sort_node = static_cast<SortNode*>(parent_node_ptr);
                break;
            }
            parent_node_ptr = parent_node_ptr->get_parent();
        }
        auto get_slot_id = [ctx](int32_t tuple_id, int32_t field_id) ->
                int32_t {return ctx->get_slot_id(tuple_id, field_id);};
        auto get_tuple_desc = [ctx] (int32_t tuple_id)->
                pb::TupleDescriptor* { return ctx->get_tuple_desc(tuple_id);};
        scan_node->clear_possible_indexes();
        //索引选择
        std::map<int32_t, int> field_range_type;
        IndexSelector(ctx).index_selector(ctx->tuple_descs(),
                                        scan_node, 
                                        filter_node,
                                        sort_node,
                                        NULL,
                                        NULL,
                                        NULL,
                                        NULL,
                                        &index_has_null, field_range_type, "");
        if (!_is_explain && !index_has_null) {
            //路由选择,
            //这一块做完索引选择之后如果命中二级索引需要重构mem_row的结构，mem_row已经在run_time
            //init中构造了，需要销毁重新搞(todo)
            PlanRouter().scan_plan_router(scan_node, get_slot_id, get_tuple_desc, false, {});
            if (state->reset_tuple_descs_and_mem_row_descriptor(ctx->tuple_descs()) != 0) {
                DB_FATAL("Fail to reset_tuple_descs_and_mem_row_descriptor");
                return -1;
            }
            ExecNode* related_manager_node = scan_node->get_related_manager_node();
            if (related_manager_node == nullptr) {
                DB_WARNING("related_manager_node is null, scan_node:%p", scan_node);
                return -1;
            }
            auto region_infos = scan_node->region_infos();
            //更改scan_node对应的fethcer_node的region信息
            related_manager_node->set_region_infos(region_infos);
        }
    }
    return 0;
}

int Joiner::runtime_filter(RuntimeState* state, ExecNode* node, std::vector<ExprNode*>* in_exprs_back) {
    if (node == nullptr) {
        DB_WARNING("node is nullptr");
        return -1;
    }

    // 构造in条件
    std::vector<ExprNode*> in_exprs;
    int ret = construct_in_condition(_inner_equal_slot, _outer_join_values, in_exprs);
    if (ret < 0) {
        DB_WARNING("ExecNode::create in condition for right table fail");
        return ret;
    }
    if (in_exprs_back != nullptr) {
        *in_exprs_back = in_exprs;
    }
    // 表达式下推
    node->predicate_pushdown(in_exprs);
    if (in_exprs.size() > 0) {
        DB_WARNING("inner node add filter node");
        node->add_filter_node(in_exprs);
    }
    // 重新做索引选择、路由选择
    std::vector<ExecNode*> scan_nodes;
    node->get_node(pb::SCAN_NODE, scan_nodes);
    bool index_has_null = false;
    if (do_plan_router(state, scan_nodes, index_has_null) != 0) {
        DB_WARNING("Fail to do_plan_router");
        return -1;
    }
    if (index_has_null) {
        node->set_return_empty();
    }
    // 谓词下推后可能生成新的plannode重新生成tracenode
    node->create_trace();
    // 节点包含的所有子查询都重新做索引选择、路由选择
    std::vector<ExecNode*> dual_scan_nodes;
    node->get_all_dual_scan_node(dual_scan_nodes);
    for (auto node : dual_scan_nodes) {
        DualScanNode* dual_scan_node = static_cast<DualScanNode*>(node);
        // 不可以下推的子查询不重新进行索引选择、路由选择
        if (!dual_scan_node->can_predicate_pushdown()) {
            continue;
        }
        auto sub_query_plan = dual_scan_node->sub_query_node();
        auto sub_query_runtime_state = dual_scan_node->sub_query_runtime_state();
        if (sub_query_plan == nullptr) {
            DB_WARNING("sub_query_plan is nullptr");
            return -1;
        }
        if (sub_query_runtime_state == nullptr) {
            DB_WARNING("sub_query_runtime_state is nullptr");
            return -1;
        }
        std::vector<ExecNode*> derived_scan_nodes;
        sub_query_plan->get_node(pb::SCAN_NODE, derived_scan_nodes);
        bool index_has_null = false;
        if (do_plan_router(sub_query_runtime_state, derived_scan_nodes, index_has_null) != 0) {
            DB_WARNING("Fail to do_plan_router");
            return -1;
        }
        sub_query_plan->create_trace();
    }
    return 0;
}

void Joiner::find_place_holder(std::unordered_multimap<int, ExprNode*>& placeholders) {
    ExecNode::find_place_holder(placeholders);
    for (auto& expr : _conditions) {
        expr->find_place_holder(placeholders);
    }
}

int Joiner::predicate_pushdown(std::vector<ExprNode*>& input_exprs) {
    return 0;
}

int Joiner::fetcher_full_table_data(RuntimeState* state, ExecNode* child_node,
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
            tuple_data.emplace_back(batch.get_row().release());
        }
    } while (!eos);
    return 0;
}

int Joiner::fetcher_inner_table_data(RuntimeState* state,
                                        const std::vector<MemRow*>& outer_tuple_data,
                                        std::vector<MemRow*>& inner_tuple_data) {
    TimeCost time_cost;
    _outer_join_values.clear();
    construct_equal_values(outer_tuple_data, _outer_equal_slot);
    std::vector<ExprNode*> in_exprs_back;
    int ret = runtime_filter(state, _inner_node, &in_exprs_back);
    if (ret < 0) {
        DB_WARNING("Fail to runtime_filter");
        return ret;
    }
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
    _inner_node->remove_additional_predicate(in_exprs_back);
    _inner_node->close(state);

    _loops++;
    DB_WARNING("fetcher_inner_table_data, loops:%lu, outer:%ld, inner:%ld, time_cost:%ld",
               _loops,  outer_tuple_data.size(), inner_tuple_data.size(), time_cost.get_time());
    return 0;
}

int Joiner::construct_in_condition(std::vector<ExprNode*>& slot_refs, 
                             const ExprValueSet& in_values, 
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
            ExprNode* literal_node = new Literal(in_value.vec[0]);
            conjunct->add_child(literal_node); 
        }
        conjunct->type_inferer();
        in_exprs.emplace_back(conjunct);
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
            for (auto val : in_value.vec) {
                ExprNode* literal_node = new Literal(val);
                row_expr->add_child(literal_node);
            }
            conjunct->add_child(row_expr); 
        }
        conjunct->type_inferer();
        in_exprs.emplace_back(conjunct);
        return 0;
    }
    return 0;
}

void Joiner::construct_equal_values(const std::vector<MemRow*>& tuple_data,
                                const std::vector<ExprNode*>& slot_refs) {
    for (auto& mem_row : tuple_data) {
        ExprValueVec join_values;
        join_values.vec.reserve(slot_refs.size());
        for (auto& slot_ref_expr : slot_refs) {
            ExprValue value = mem_row->get_value(static_cast<SlotRef*>(slot_ref_expr)->tuple_id(), 
                                             static_cast<SlotRef*>(slot_ref_expr)->slot_id());
            join_values.vec.emplace_back(value);
        }
        _outer_join_values.insert(join_values);
    }
}

void Joiner::adjudge_join_type() {
    std::vector<ExecNode*> scan_nodes;
    if (_inner_node->node_type() == pb::JOIN_NODE) {
        static_cast<JoinNode*>(_inner_node)->adjudge_join_type();
        _use_index_join = static_cast<JoinNode*>(_inner_node)->is_use_index_join();
    } else {
        _inner_node->get_node(pb::SCAN_NODE, scan_nodes);
        if (scan_nodes.size() != 1) {
            return;
        }
        ScanNode* scan_node = static_cast<ScanNode*>(scan_nodes[0]);
        if (FLAGS_use_arrow_vector
                && scan_node->can_use_no_index_join()) {
            _use_index_join = false;
        }
    }
    return;
}

void Joiner::construct_equal_values_for_vectorized(std::shared_ptr<arrow::Table> outer_table,
                                const std::vector<ExprNode*>& slot_refs) {
    // arrow table -> ExprValueVec
    std::vector<arrow::ChunkedArray*> arrow_arrays;
    for (auto& slot_ref : slot_refs) {
        std::string arrow_field_name = static_cast<SlotRef*>(slot_ref)->arrow_field_name();
        std::shared_ptr<arrow::ChunkedArray> field = outer_table->GetColumnByName(arrow_field_name);
        arrow_arrays.emplace_back(field.get());
    }   
    for (auto row = 0; row < outer_table->num_rows(); ++row) {
        ExprValueVec join_values;
        join_values.vec.reserve(slot_refs.size());
        for (auto& array : arrow_arrays) {
            ExprValue value = VectorizeHelpper::get_vectorized_value(array, row); 
            join_values.vec.emplace_back(value);
        }
        _outer_join_values.insert(join_values);
    }
}

void Joiner::construct_equal_values_for_vectorized(const std::vector<RegionReturnData>& outer_data,
                                const std::vector<ExprNode*>& slot_refs) {
    TimeCost t;
    int row = 0;
    int pb_rows = 0;
    for (auto& region_data : outer_data) {
        if (region_data.row_data != nullptr) {
            // row pb -> ExprValueVec
            for (region_data.row_data->reset(); !region_data.row_data->is_traverse_over(); region_data.row_data->next()) {
                ExprValueVec join_values;
                join_values.vec.reserve(slot_refs.size());
                for (auto& slot_ref_expr : slot_refs) {
                    ExprValue value = region_data.row_data->get_row()->get_value(static_cast<SlotRef*>(slot_ref_expr)->tuple_id(), 
                                                    static_cast<SlotRef*>(slot_ref_expr)->slot_id());
                    join_values.vec.emplace_back(value);
                }
                _outer_join_values.insert(join_values);
            }
            pb_rows += region_data.row_data->size();
        } else if (region_data.arrow_data != nullptr) {
            // arrow record batch -> ExprValueVec
            std::vector<arrow::ChunkedArray> arrow_arrays;
            for (auto& slot_ref : slot_refs) {
                std::string arrow_field_name = static_cast<SlotRef*>(slot_ref)->arrow_field_name();
                arrow_arrays.emplace_back(region_data.arrow_data->GetColumnByName(arrow_field_name));
            }
            for (auto row = 0; row < region_data.arrow_data->num_rows(); ++row) {
                ExprValueVec join_values;
                join_values.vec.reserve(slot_refs.size());
                for (auto& array : arrow_arrays) {
                    ExprValue value = VectorizeHelpper::get_vectorized_value(&array, row); 
                    join_values.vec.emplace_back(value);
                }
                _outer_join_values.insert(join_values);
            }
            row += region_data.arrow_data->num_rows();
        }
    }
    DB_WARNING("get in value pb_rows: %d, vec_rows: %d, cost: %ld", pb_rows, row, t.get_time());
}

bool Joiner::is_satisfy_filter(MemRow* row) {
    for (auto& condition : _conditions) {
        ExprValue value = condition->get_value(row);
        if (value.is_null() || !value.get_numberic<bool>()) {
            return false;
        }
    }
    return true;
}

void Joiner::encode_hash_key(MemRow* row, 
                     const std::vector<ExprNode*>& slot_ref_exprs,
                     MutTableKey& key) {
    for (auto& slot_ref_expr : slot_ref_exprs) {
        ExprValue value = row->get_value(static_cast<SlotRef*>(slot_ref_expr)->tuple_id(), 
                                         static_cast<SlotRef*>(slot_ref_expr)->slot_id());
        // TODO: 同类型可以不转string
        key.append_value(value.cast_to(pb::STRING)); 
    }
}

void Joiner::construct_hash_map(const std::vector<MemRow*>& tuple_data, 
                                  const std::vector<ExprNode*>& slot_refs) {
    for (auto& mem_row : tuple_data) {
        MutTableKey key;
        encode_hash_key(mem_row, slot_refs, key);
        _hash_map[key.data()].emplace_back(mem_row);
    } 
}

int Joiner::construct_result_batch(RowBatch* batch, 
                                      MemRow* outer_mem_row, 
                                      MemRow* inner_mem_row,
                                      bool& matched) {
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
    switch (_join_type) {
    case pb::INNER_JOIN:
    case pb::SEMI_JOIN: {
        if (is_satisfy_filter(row.get())) {
            matched = true;
            batch->move_row(std::move(row));
        }
        break;
    }
    case pb::LEFT_JOIN:
    case pb::RIGHT_JOIN: {
        if (is_satisfy_filter(row.get())) {
            matched = true;
            if (_compare_type != pb::CMP_ALL) {
                batch->move_row(std::move(row));
            }
        } else if (_use_hash_map && _compare_type != pb::CMP_ALL) {
            return construct_null_result_batch(batch, outer_mem_row);
        }
        break;
    }
    case pb::ANTI_SEMI_JOIN: {
        if (is_satisfy_filter(row.get())) {
            matched = true;
        } else {
            matched = false;
        }
        break;
    }
    default:
        break;
    }
    return 0;
}

int Joiner::construct_null_result_batch(RowBatch* batch, MemRow* outer_mem_row) {
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

void Joiner::close(RuntimeState* state) {
    ExecNode::close(state);
    _conditions.insert(_conditions.end(), _have_removed.begin(), _have_removed.end());
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
    _result_row_index = 0;
    _hash_map.clear();
    _outer_table_is_null = false;
    _inner_row_batch.clear();
    _child_eos = false;
    _outer_intermediate_join_result_table.reset();
    _inner_intermediate_join_result_table.reset();
}

void Joiner::show_explain(std::vector<std::map<std::string, std::string>>& output) {
    _outer_node->show_explain(output);
    _inner_node->show_explain(output);
}

}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
