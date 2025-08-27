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

#include "dual_scan_node.h"
#include "filter_node.h"
#include "limit_node.h"
#include "query_context.h"
#include "union_node.h"
#include "arrow_io_excutor.h"
#include "packet_node.h"
#include "query_context.h"
#include "window_node.h"
#include "arrow_function.h"
#include "vectorize_helpper.h"
#include <arrow/acero/options.h>

namespace baikaldb {
DEFINE_bool(union_predicate_pushdown, true, "union predicate push_down");

int DualScanNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    _tuple_id = node.derive_node().scan_node().tuple_id();
    _table_id = node.derive_node().scan_node().table_id();
    _node_type = pb::DUAL_SCAN_NODE;
    for (auto& expr : node.derive_node().dual_scan_node().derived_table_projections()) {
        ExprNode* derived_table_projection = nullptr;
        ret = ExprNode::create_tree(expr, &derived_table_projection);
        if (ret < 0) {
            DB_FATAL("create derived table projection fail");
            ExprNode::destroy_tree(derived_table_projection);
            return ret;
        }
        _derived_table_projections.emplace_back(derived_table_projection);
    }
    for (bool iter : node.derive_node().dual_scan_node().derived_table_projections_agg_or_window_vec()) {
        _derived_table_projections_agg_or_window_vec.push_back(iter);
    }
    if (node.derive_node().dual_scan_node().has_runtime_state()) {
        _need_delete_runtime_state = true;
        _sub_query_runtime_state = new RuntimeState();
        ret = _sub_query_runtime_state->init(node.derive_node().dual_scan_node().runtime_state());
        if (ret < 0) {
            DB_FATAL("sub_query runtime state init fail");
            return -1; 
        }
        _sub_query_runtime_state->set_from_subquery(true);
        _sub_query_runtime_state->use_mpp = true;
    }
    if (node.derive_node().dual_scan_node().has_sub_query_node()) {
        _has_subquery = true;
        ret = create_tree(node.derive_node().dual_scan_node().sub_query_node(), &_sub_query_node, CreateExecOptions());
        if (ret < 0) {
            ExecNode::destroy_tree(_sub_query_node);
            DB_FATAL("dual scan node create sub query fail");
            return -1;
        }
    }
    if (node.derive_node().dual_scan_node().slot_column_size() > 0) {
        for (auto& slot_column : node.derive_node().dual_scan_node().slot_column()) {
            _slot_column_mapping.insert(std::make_pair(slot_column.slot_id(), slot_column.column_id()));
        }
    }
    if (node.derive_node().dual_scan_node().agg_tuple_id_pair().size() == 2) {
        const int32_t agg_tuple_id = node.derive_node().dual_scan_node().agg_tuple_id_pair(0);
        const int32_t subquery_agg_tuple_id = node.derive_node().dual_scan_node().agg_tuple_id_pair(1);
        _agg_tuple_id_pair = std::make_pair(agg_tuple_id, subquery_agg_tuple_id);
        for (const auto& slot_id : node.derive_node().dual_scan_node().agg_slot_ids()) {
            _agg_slot_ids.emplace_back(slot_id);
        }
        for (const auto& slot_id : node.derive_node().dual_scan_node().multi_distinct_agg_slot_ids()) {
            _multi_distinct_agg_slot_ids.emplace(slot_id);
        }
    }
    return 0;
}

int DualScanNode::open(RuntimeState* state) {
    if (!_has_subquery) {
        return 0;
    }
    if (state->is_explain && state->explain_type == EXPLAIN_NULL) {
        return 0;
    }
    int ret = 0;
    for (auto expr : _derived_table_projections) {
        ret = expr->open();
        if (ret < 0) {
            DB_WARNING("Expr::open fail:%d", ret);
            return ret;
        }
    }
    if (_sub_query_node == nullptr) {
        DB_WARNING("_sub_query_node is nullptr");
        return -1;
    }
    if (_sub_query_runtime_state == nullptr) {
        DB_WARNING("_sub_query_runtime_state is nullptr");
        return -1;
    }
    ExecNode* join = _sub_query_node->get_node(pb::JOIN_NODE);
    _sub_query_runtime_state->is_simple_select = (join == nullptr);
    _sub_query_node->set_delay_fetcher_store(_delay_fetcher_store);

    ret = _sub_query_node->open(_sub_query_runtime_state);
    if (ret < 0) {
        DB_WARNING("Fail to open _sub_query_node");
        return ret;
    }
    set_node_exec_type(_sub_query_node->node_exec_type());
    return 0;
}

int DualScanNode::get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
    if (!_has_subquery) {
        std::unique_ptr<MemRow> row = state->mem_row_desc()->fetch_mem_row();
        batch->move_row(std::move(row));
        ++_num_rows_returned;
        *eos = true;
        return 0;
    }
    if (state->is_explain && state->explain_type == EXPLAIN_NULL) {
        *eos = true;
        return 0;
    }
    const int32_t tuple_id = slot_tuple_id();
    pb::TupleDescriptor* tuple_desc = state->get_tuple_desc(tuple_id);
    if (tuple_desc == nullptr) {
        DB_WARNING("tuple_desc is nullptr, slot_tuple_id: %d", tuple_id);
        return -1;
    }
    MemRowDescriptor* mem_row_desc = state->mem_row_desc();
    if (mem_row_desc == nullptr) {
        DB_WARNING("mem_row_desc is nullptr");
        return -1;
    }
    int ret = 0;
    do {
        RowBatch derived_batch;
        ret = _sub_query_node->get_next(_sub_query_runtime_state, &derived_batch, eos);
        if (ret < 0) {
            DB_WARNING("children::get_next fail:%d", ret);
            return ret;
        }
        set_node_exec_type(_sub_query_node->node_exec_type());
        if (_node_exec_type == pb::EXEC_ARROW_ACERO) {
            break;
        }
        for (derived_batch.reset(); !derived_batch.is_traverse_over(); derived_batch.next()) {
            MemRow* inner_row = derived_batch.get_row().get();
            std::unique_ptr<MemRow> outer_row = mem_row_desc->fetch_mem_row();
            for (auto iter : _slot_column_mapping) {
                int32_t outer_slot_id = iter.first;
                int32_t inter_column_id = iter.second;
                auto expr = _derived_table_projections[inter_column_id];
                ExprValue result = expr->get_value(inner_row).cast_to(expr->col_type());
                int slot_idx = get_slot_idx(*tuple_desc, outer_slot_id);
                if (slot_idx < 0 || slot_idx >= tuple_desc->slots().size()) {
                    DB_WARNING("Invalid slot_idx, slot_idx: %d, slots size: %d", 
                                slot_idx, tuple_desc->slots().size());
                    return -1;
                }
                auto slot = tuple_desc->slots(slot_idx);
                outer_row->set_value(slot.tuple_id(), slot.slot_id(), result);
            }
            // 获取聚合下推结果，内层agg tuple映射到外层agg tuple
            if (_agg_tuple_id_pair.first != -1) {
                const int32_t agg_tuple_id = _agg_tuple_id_pair.first;
                const int32_t subquery_agg_tuple_id = _agg_tuple_id_pair.second;
                if (inner_row == nullptr) {
                    DB_WARNING("inner_row is nullptr");
                    return -1;
                }
                for (auto slot_id : _agg_slot_ids) {
                    outer_row->set_value(agg_tuple_id, slot_id, inner_row->get_value(subquery_agg_tuple_id, slot_id));
                }
            }
            batch->move_row(std::move(outer_row));
        }
    } while (!*eos);
    // 更新子查询信息到外层的state，放在close()中执行
    // state->inc_num_returned_rows(_sub_query_runtime_state->num_returned_rows());
    // state->inc_num_affected_rows(_sub_query_runtime_state->num_affected_rows());
    // state->inc_num_scan_rows(_sub_query_runtime_state->num_scan_rows());
    // state->inc_num_filter_rows(_sub_query_runtime_state->num_filter_rows());
    return 0;
}

void DualScanNode::get_all_dual_scan_node(std::vector<ExecNode*>& exec_nodes) {
    if (!_has_subquery) {
        return;
    }
    exec_nodes.emplace_back(this);
    if (_sub_query_node != nullptr) {
        _sub_query_node->get_all_dual_scan_node(exec_nodes);
    }
}

int DualScanNode::predicate_pushdown(std::vector<ExprNode*>& input_exprs) {
    if (_has_subquery) {
        if (_sub_query_node == nullptr) {
            DB_WARNING("_sub_query_node is nullptr");
            return -1;
        }
        if (can_predicate_pushdown()) {
            bool subquery_has_agg = false;
            bool subquery_has_window = false;
            std::set<std::pair<int32_t, int32_t>> group_by_slots;
            std::set<std::pair<int32_t, int32_t>> partition_by_slots;
            get_subquery_group_by_slots(subquery_has_agg, group_by_slots);
            get_subquery_window_partition_by_slots(subquery_has_window, partition_by_slots);

            const int32_t tuple_id = slot_tuple_id();
            std::vector<ExprNode*> subquery_where_input_exprs;
            auto iter = input_exprs.begin();
            while (iter != input_exprs.end()) {
                bool can_expr_pushdown = false;
                if (can_expr_predicate_pushdown(subquery_has_agg, subquery_has_window, 
                                                group_by_slots, partition_by_slots, *iter, can_expr_pushdown) != 0) {
                    return -1;
                }
                if (can_expr_pushdown) {
                    (*iter)->replace_slot_ref_to_expr(tuple_id, _slot_column_mapping, _derived_table_projections, *iter);
                    subquery_where_input_exprs.emplace_back(*iter);
                    iter = input_exprs.erase(iter);
                    continue;
                }
                ++iter;
            }
            // 谓词下推到子查询后，子查询不再使用全量导出模式
            if (subquery_where_input_exprs.size() > 0 && _sub_query_ctx != nullptr) {
                _sub_query_ctx->is_full_export = false;
            }
            _sub_query_node->predicate_pushdown(subquery_where_input_exprs);
        } else {
            std::vector<ExprNode*> empty_exprs;
            _sub_query_node->predicate_pushdown(empty_exprs);
        }
    }
    if (_parent != nullptr && 
            (_parent->node_type() == pb::WHERE_FILTER_NODE || _parent->node_type() == pb::TABLE_FILTER_NODE)) {
        return 0;
    }
    if (input_exprs.size() > 0) {
        add_filter_node(input_exprs);
        input_exprs.clear();
    }
    return 0;
}

int DualScanNode::prune_columns(QueryContext* ctx, const std::unordered_set<int32_t>& invalid_column_ids) {
    if (!_has_subquery) {
        return 0;
    }
    if (ctx == nullptr) {
        DB_WARNING("ctx is nullptr");
        return -1;
    }
    if (_sub_query_node == nullptr) {
        DB_WARNING("_sub_query_node is nullptr");
        return -1;
    }
    int32_t tuple_id = slot_tuple_id();
    pb::TupleDescriptor* tuple_desc = ctx->get_tuple_desc(tuple_id);
    if (tuple_desc == nullptr) {
        DB_WARNING("tuple_id:%d not exist", tuple_id);
        return -1;
    }
    std::unordered_set<int32_t> valid_slot_id_set;
    for (const auto& slot : tuple_desc->slots()) {
        valid_slot_id_set.insert(slot.slot_id());
    }
    std::unordered_set<int32_t> valid_column_id_set;
    for (auto [outer_slot_id, inter_column_id] : _slot_column_mapping) {
        if (valid_slot_id_set.find(outer_slot_id) != valid_slot_id_set.end()) {
            valid_column_id_set.insert(inter_column_id);
        }
    }
    // 更新_derived_table_projections,_derived_table_projections_agg_or_window_vec
    if (_derived_table_projections.size() != _derived_table_projections_agg_or_window_vec.size()) {
        DB_WARNING("derived_table_projections size[%lu] not equal to "
                   "derived_table_projections_agg_or_window_vec size[%lu]",
                   _derived_table_projections.size(),
                   _derived_table_projections_agg_or_window_vec.size());
        return -1;
    }
    std::unordered_set<int32_t> invalid_column_id_set;
    std::vector<ExprNode*> new_derived_table_projections;
    new_derived_table_projections.reserve(_derived_table_projections.size());
    std::vector<bool> new_derived_table_projections_agg_or_window_vec;
    new_derived_table_projections_agg_or_window_vec.reserve(_derived_table_projections_agg_or_window_vec.size());
    int new_projection_idx = 0;
    std::vector<int32_t> projection_idxes(_derived_table_projections.size()); // 记录新旧投影列的映射关系
    for (int i = 0; i < _derived_table_projections.size(); ++i) {
        if (_derived_table_projections[i] == nullptr) {
            DB_WARNING("_derived_table_projections[%d] is nullptr", i);
            return -1;
        }
        if (valid_column_id_set.find(i) != valid_column_id_set.end()) {
            new_derived_table_projections.emplace_back(
                                            _derived_table_projections[i]);
            new_derived_table_projections_agg_or_window_vec.emplace_back(
                                            _derived_table_projections_agg_or_window_vec[i]);
            projection_idxes[i] = new_projection_idx++;
        } else {
            invalid_column_id_set.insert(i);
            _derived_table_projections[i]->close();
            ExprNode::destroy_tree(_derived_table_projections[i]);
        }
    }
    std::swap(_derived_table_projections, new_derived_table_projections);
    std::swap(_derived_table_projections_agg_or_window_vec, new_derived_table_projections_agg_or_window_vec);
    // 更新_slot_column_mapping
    std::map<int32_t, int32_t> new_slot_column_mapping;
    for (const auto& [outer_slot_id, inter_column_id] : _slot_column_mapping) {
        if (inter_column_id < 0 || inter_column_id >= projection_idxes.size()) {
            DB_WARNING("Invalid inter_column_id: %d", inter_column_id);
            return -1;
        }
        if (invalid_column_id_set.find(inter_column_id) == invalid_column_id_set.end()) {
            new_slot_column_mapping[outer_slot_id] = projection_idxes[inter_column_id];
        }
    }
    std::swap(_slot_column_mapping, new_slot_column_mapping);
    // Union子查询场景会重复更新slot_column_mapping
    ctx->slot_column_mapping[tuple_id] = _slot_column_mapping;
    return _sub_query_node->prune_columns(_sub_query_ctx, invalid_column_id_set);
}

int DualScanNode::agg_pushdown(QueryContext* ctx, ExecNode* node) {
    if (_has_subquery) {
        if (ctx == nullptr) {
            DB_WARNING("ctx is nullptr");
            return -1;
        }
        if (_sub_query_ctx == nullptr) {
            DB_WARNING("subquery ctx is nullptr");
            return -1;
        }
        if (_sub_query_node == nullptr) {
            DB_WARNING("_sub_query_node is nullptr");
            return -1;
        }
        AggNode* agg_node = static_cast<AggNode*>(node);
        if (agg_node != nullptr && can_agg_pushdown()) {
            int ret = 0;
            // Step1. 内层子查询创建Agg Tuple，并建立内外层Agg Tuple映射
            ret = create_subquery_agg_tuple(ctx, _sub_query_ctx, agg_node);
            if (ret != 0) {
                DB_WARNING("Fail to create_subquery_agg_tuple");
                return -1;
            }
            // Step2. 内层子查询创建AggNode
            PacketNode* packet_node = static_cast<PacketNode*>(_sub_query_node->get_node(pb::PACKET_NODE));
            if (packet_node == nullptr) {
                DB_WARNING("packet_node is nullptr");
                return -1;
            }
            if (packet_node->children_size() == 0 || packet_node->children(0) == nullptr) {
                DB_WARNING("packet_node's children is null");
                return -1;
            }
            AggNode* subquery_agg_node = nullptr;
            ret = create_subquery_agg_node(_sub_query_ctx, agg_node, subquery_agg_node);
            if (ret != 0) {
                DB_WARNING("Fail to create_subquery_agg_node");
                return -1;
            }
            ExecNode* packet_child_node = packet_node->children(0);
            packet_node->clear_children();
            subquery_agg_node->add_child(packet_child_node);
            packet_node->add_child(subquery_agg_node);
            // step3. union子查询外层agg_node转化成MERGE_AGG_NDOE；普通子查询删除外层agg_node
            if (is_union_subquery()) {
                agg_node->transfer_to_merge_agg();
            } else {
                ret = agg_node->delete_self();
                if (ret < 0) {
                    return -1;
                }
            }
            // 聚合下推到子查询后，子查询不再使用全量导出模式
            if (_sub_query_ctx != nullptr) {
                _sub_query_ctx->is_full_export = false;
            }
        }
        ExecNode* dual = _sub_query_node->get_node(pb::DUAL_SCAN_NODE);
        if (dual != nullptr) {
            // 触发内层子查询的agg_pushdown
            _sub_query_node->agg_pushdown(_sub_query_ctx, nullptr);
        }
    }
    return 0;
}

int DualScanNode::create_subquery_agg_tuple(
        QueryContext* ctx, QueryContext* subquery_ctx, AggNode* agg_node) {
    if (ctx == nullptr) {
        DB_WARNING("ctx is nullptr");
        return -1;
    }
    if (subquery_ctx == nullptr) {
        DB_WARNING("subquery ctx is nullptr");
        return -1;
    }
    if (subquery_ctx->unique_id_ctx == nullptr) {
        DB_WARNING("subquery_ctx->unique_id_ctx is nullptr");
        return -1;
    }
    if (agg_node == nullptr) {
        DB_WARNING("agg_node is nullptr");
        return -1;
    }
    const pb::AggNode& pb_agg_node = agg_node->pb_node().derive_node().agg_node();
    const int32_t agg_tuple_id = pb_agg_node.agg_tuple_id();
    const auto& tuple_descs = ctx->tuple_descs();
    if (agg_tuple_id < 0 || agg_tuple_id >= tuple_descs.size()) {
        DB_WARNING("Invalid agg_tuple_id: %d", agg_tuple_id);
        return -1;
    }
    const pb::TupleDescriptor& agg_tuple = tuple_descs[agg_tuple_id];

    // 直接复用外层的tuple_desc，修改tuple_desc中的tuple_id为内层的tuple_id
    const int32_t subquery_agg_tuple_id = subquery_ctx->unique_id_ctx->tuple_cnt++;
    pb::TupleDescriptor subquery_agg_tuple;
    subquery_agg_tuple.CopyFrom(agg_tuple);
    subquery_agg_tuple.set_tuple_id(subquery_agg_tuple_id);
    for (auto& slot_desc : *subquery_agg_tuple.mutable_slots()) {
        slot_desc.set_tuple_id(subquery_agg_tuple_id);
    }
    // 建立内外层Agg Tuple映射
    // 如果是distinct场景，只需要处理非distinct和multi distinct函数
    _agg_tuple_id_pair = std::make_pair(agg_tuple_id, subquery_agg_tuple.tuple_id());
    for (auto* agg_func : *(agg_node->mutable_agg_fn_calls())) {
        if (agg_func == nullptr) {
            DB_WARNING("agg_func is nullptr");
            return -1;
        }
        // 普通聚合函数行、列都需要映射
        // 单distinct函数行、列都不需要映射
        // 多distinct函数行需要映射、列不需要映射
        if (!agg_func->is_distinct() || agg_func->is_multi_distinct()) {
            _agg_slot_ids.emplace_back(agg_func->final_slot_id());
            if (agg_func->final_slot_id() != agg_func->intermediate_slot_id()) {
                _agg_slot_ids.emplace_back(agg_func->intermediate_slot_id());
            }
            if (agg_func->is_multi_distinct()) {
                _multi_distinct_agg_slot_ids.emplace(agg_func->final_slot_id());
                if (agg_func->final_slot_id() != agg_func->intermediate_slot_id()) {
                    _multi_distinct_agg_slot_ids.emplace(agg_func->intermediate_slot_id());
                }
            }
        }
    }
    subquery_ctx->add_tuple(subquery_agg_tuple);
    return 0;
}

int DualScanNode::create_subquery_agg_node(
        QueryContext* subquery_ctx, AggNode* agg_node, AggNode*& subquery_agg_node) {
    if (agg_node == nullptr) {
        DB_WARNING("agg_node is nullptr");
        return -1;
    }
    if (subquery_ctx == nullptr) {
        DB_WARNING("subquery_ctx is nullptr");
        return -1;
    }
    const int32_t tuple_id = slot_tuple_id();
    const int32_t subquery_agg_tuple_id = _agg_tuple_id_pair.second;
    pb::PlanNode pb_subquery_plan_node;
    pb_subquery_plan_node.set_node_type(pb::AGG_NODE);
    pb_subquery_plan_node.set_limit(-1);
    pb_subquery_plan_node.set_is_explain(subquery_ctx->is_explain);
    pb_subquery_plan_node.set_num_children(1);
    pb::AggNode* pb_subquery_agg_node = pb_subquery_plan_node.mutable_derive_node()->mutable_agg_node(); 
    pb_subquery_agg_node->set_agg_tuple_id(subquery_agg_tuple_id);
    pb_subquery_agg_node->set_is_pushdown(true);

    int ret = 0;
    const pb::AggNode& pb_agg_node = agg_node->pb_node().derive_node().agg_node();
    for (const auto& expr : pb_agg_node.group_exprs()) {
        ExprNode* group_expr = nullptr;
        ret = ExprNode::create_tree(expr, &group_expr);
        ON_SCOPE_EXIT(([&group_expr]() {
            SAFE_DELETE(group_expr);
        }));
        if (ret < 0) {
            return ret;
        }
        if (group_expr == nullptr) {
            DB_WARNING("group_expr is nullptr");
            return -1;
        }
        ret = group_expr->replace_slot_ref_to_expr(
                tuple_id, _slot_column_mapping, _derived_table_projections, group_expr);
        if (ret < 0) {
            DB_WARNING("Fail ot replace_slot_ref_to_expr");
            return -1;
        }
        pb::Expr pb_group_expr;
        ExprNode::create_pb_expr(&pb_group_expr, group_expr);
        pb_subquery_agg_node->add_group_exprs()->Swap(&pb_group_expr);
    }
    for (const auto& expr : pb_agg_node.agg_funcs()) {
        ExprNode* agg_func = nullptr;
        ret = ExprNode::create_tree(expr, &agg_func);
        ON_SCOPE_EXIT(([&agg_func]() {
            SAFE_DELETE(agg_func);
        }));
        if (ret < 0) {
            return ret;
        }
        agg_func->set_tuple_id(subquery_agg_tuple_id);
        ret = agg_func->replace_slot_ref_to_expr(
                tuple_id, _slot_column_mapping, _derived_table_projections, agg_func);
        if (ret < 0) {
            DB_WARNING("Fail ot replace_slot_ref_to_expr");
            return -1;
        }
        pb::Expr pb_agg_func;
        ExprNode::create_pb_expr(&pb_agg_func, agg_func);
        pb_subquery_agg_node->add_agg_funcs()->Swap(&pb_agg_func);
    }
    subquery_agg_node = new (std::nothrow) AggNode();
    if (subquery_agg_node == nullptr) {
        DB_WARNING("new subquery_agg_node failed");
        return -1;
    }
    ret = subquery_agg_node->init(pb_subquery_plan_node);
    if (ret < 0) {
        DB_WARNING("Fail to init subquery_agg_node");
        SAFE_DELETE(subquery_agg_node);
        return -1;
    }
    return 0;
}

int DualScanNode::transfer_outer_slot_to_inner(ExprNode** input_expr) {
    if (can_predicate_pushdown()) {
        bool can_expr_pushdown = false;
        bool subquery_has_agg = false;
        bool subquery_has_window = false;
        std::set<std::pair<int32_t, int32_t>> group_by_slots;
        std::set<std::pair<int32_t, int32_t>> partition_by_slots;
        get_subquery_group_by_slots(subquery_has_agg, group_by_slots);
        get_subquery_window_partition_by_slots(subquery_has_window, partition_by_slots);
        if (can_expr_predicate_pushdown(subquery_has_agg, subquery_has_window, 
                                        group_by_slots, partition_by_slots, *input_expr, can_expr_pushdown) != 0) {
            return -1;
        }
        if (can_expr_pushdown) {
            const int32_t tuple_id = slot_tuple_id();
            return (*input_expr)->replace_slot_ref_to_expr(tuple_id, _slot_column_mapping, _derived_table_projections, *input_expr);
        }
    }
    // 没法下推   
    return -1;
}

void DualScanNode::get_subquery_group_by_slots(
        bool& subquery_has_agg, std::set<std::pair<int32_t, int32_t>>& group_by_slots) {
    subquery_has_agg = false;
    group_by_slots.clear();
    if (_sub_query_node == nullptr) {
        return;
    }
    AggNode* agg_node = static_cast<AggNode*>(_sub_query_node->get_node(pb::AGG_NODE));
    if (agg_node == nullptr) {
        return;
    }
    subquery_has_agg = true;
    std::vector<ExprNode*>* mutable_group_exprs = agg_node->mutable_group_exprs();
    if (mutable_group_exprs != nullptr) {
        for (auto* expr : *mutable_group_exprs) {
            if (expr != nullptr && expr->is_slot_ref()) {
                group_by_slots.emplace(expr->tuple_id(), expr->slot_id());
            } 
        }
    }
    return;
}

void DualScanNode::get_subquery_window_partition_by_slots(
        bool& subquery_has_window, std::set<std::pair<int32_t, int32_t>>& partition_by_slots) {
    subquery_has_window = false;
    partition_by_slots.clear();
    if (_sub_query_node == nullptr) {
        return;
    }
    std::vector<ExecNode*> window_nodes;
    _sub_query_node->get_node(pb::WINDOW_NODE, window_nodes);
    if (window_nodes.empty()) {
        return;
    }
    subquery_has_window = true;
    auto get_partition_by_slots = [&](ExecNode* window_node) -> std::set<std::pair<int32_t, int32_t>> {
        std::set<std::pair<int32_t, int32_t>> partition_by_slots;
        if (window_node == nullptr) {
            return partition_by_slots;
        }
        std::vector<ExprNode*>* mutable_partition_exprs = 
            static_cast<WindowNode*>(window_node)->mutable_partition_exprs();
        if (mutable_partition_exprs != nullptr) {
            for (auto* expr : *mutable_partition_exprs) {
                if (expr != nullptr && expr->is_slot_ref()) {
                    partition_by_slots.emplace(expr->tuple_id(), expr->slot_id());
                } 
            }
        }
        return partition_by_slots;
    };
    // 获取第一个window节点的partition_by_slots
    partition_by_slots = get_partition_by_slots(window_nodes[0]);
    // 依次获取其他window节点的partition_by_slots，取交集
    for (int i = 1; i < window_nodes.size(); ++i) {
        std::set<std::pair<int32_t, int32_t>> tmp_set;
        std::set<std::pair<int32_t, int32_t>> cur_partition_by_slots = get_partition_by_slots(window_nodes[i]);
        std::set_intersection(cur_partition_by_slots.begin(), cur_partition_by_slots.end(), 
                              partition_by_slots.begin(), partition_by_slots.end(),
                              std::inserter(tmp_set, tmp_set.begin()));
        partition_by_slots = tmp_set;
    }
    return;
}

int DualScanNode::can_expr_predicate_pushdown(
        bool subquery_has_agg, 
        bool subquery_has_window,
        const std::set<std::pair<int32_t, int32_t>>& group_by_slots, 
        const std::set<std::pair<int32_t, int32_t>>& partition_by_slots,
        ExprNode* expr,
        bool& can_pushdown) {
    can_pushdown = false;
    if (expr == nullptr) {
        DB_WARNING("expr is nullptr");
        return -1;
    }
    const int32_t tuple_id = slot_tuple_id();
    std::unordered_set<int32_t> tuple_ids;
    expr->get_all_tuple_ids(tuple_ids);
    if (tuple_ids.size() == 1 && *tuple_ids.begin() == tuple_id) {
        if (expr->has_agg() || expr->has_window()) {
            // 表达式自身包含agg或window，不可下推
            return 0;
        }
        bool has_agg_or_window = false;
        if (expr->has_agg_or_window_projection(
                _slot_column_mapping, _derived_table_projections_agg_or_window_vec, has_agg_or_window) != 0) {
            DB_WARNING("Fail to check has_agg_or_window_projection");
            return -1;
        }
        if (has_agg_or_window) {
            // slot_ref替换后的expr有agg或window，不可下推
            return 0;
        }
        // 外层表达式涉及到的内层SlotRef集合，使用<内层tuple_id，内层slot_id>表示内层的SlotRef
        std::set<std::pair<int32_t, int32_t>> tuple_slot_ids;
        expr->get_all_subquery_tuple_slot_ids(tuple_id, _slot_column_mapping, _derived_table_projections, tuple_slot_ids);
        if (subquery_has_agg) {
            for (const auto& tuple_slot : tuple_slot_ids) {
                if (group_by_slots.find(tuple_slot) == group_by_slots.end()) {
                    // slot_ref替换后的expr包含不在group_by_slots的slot_ref，会破坏聚合窗口，不可下推；
                    // 下面示例col1 > 0可以下推，col3 > 0不能下推
                    // SELECT * 
                    // FROM (
                    //     SELECT col1, col2, col3, sum(col4) 
                    //     FROM tbl
                    //     GROUP BY col1, col2) t
                    // WHERE col1 > 0 and col3 > 0;
                    return 0;
                }
            }
        }
        if (subquery_has_window) {
            for (const auto& tuple_slot : tuple_slot_ids) {
                if (partition_by_slots.find(tuple_slot) == partition_by_slots.end()) {
                    // slot_ref替换后的expr包含不在partition_by_slots的slot_ref，会破坏分区窗口，不可下推；
                    // 下面示例col1 > 0可以下推，col2 > 0不能下推
                    // SELECT * 
                    // FROM (
                    //     SELECT col1,
                    //         RANK() OVER (PARTITION BY col1, col2) AS rank1,
                    //         RANK() OVER (PARTITION BY col1) AS rank2
                    //     FROM tbl) t
                    // WHERE col1 > 0 and col2 > 0; // 破环了rank2对应的窗口数据
                    return 0;
                }
            }
        }
    } else {
        // 不可下推
        return 0;
    }
    // 可以下推
    can_pushdown = true;
    return 0;
}

bool DualScanNode::can_predicate_pushdown() {
    if (_sub_query_node == nullptr) {
        return false;
    }
    // 后续开放UNION可下推时，需要注意UNION的tuple_descs
    if (!FLAGS_union_predicate_pushdown) {
        UnionNode* union_node = static_cast<UnionNode*>(_sub_query_node->get_node(pb::UNION_NODE));
        if (union_node != nullptr) {
            return false;
        }
    }
    LimitNode* limit_node = static_cast<LimitNode*>(_sub_query_node->get_node(pb::LIMIT_NODE));
    if (limit_node != nullptr) {
        return false;
    }
    return true;
}

bool DualScanNode::can_agg_pushdown() {
    if (_sub_query_node == nullptr) {
        return false;
    }
    std::unordered_set<pb::PlanNodeType> node_types = {
        pb::PACKET_NODE, 
        pb::UNION_NODE, 
        pb::TABLE_FILTER_NODE, 
        pb::WHERE_FILTER_NODE, 
        pb::SCAN_NODE, 
        pb::DUAL_SCAN_NODE
    };
    bool ret = _sub_query_node->only_has_specified_node(node_types);
    if (!ret) {
        return false;
    }
    return true;
}

bool DualScanNode::can_use_arrow_vector(RuntimeState* state) {
    if (_sub_query_node != nullptr) {
        if (!_sub_query_node->can_use_arrow_vector(state)) {
            return false;
        }
        for (auto expr : _derived_table_projections) {
            if (!expr->can_use_arrow_vector()) {
                return false;
            }
        }
    }
    for (auto& c : _children) {
        if (!c->can_use_arrow_vector(state)) {
            return false;
        }
    }
    return true;
}

int DualScanNode::set_partition_property_and_schema(QueryContext* ctx) {
    // 如select 1; 没有from的子查询
    // dualscannode -> packetnode(select 1) -> dualscannode(sub_query_node = nullptr)
    if (_sub_query_node == nullptr) {
        _partition_property.set_any_partition();
        _partition_property.has_no_input_data = true;
        return 0;
    }
    // 外层数据schema
    const int32_t tuple_id = slot_tuple_id();
    pb::TupleDescriptor* tuple_desc = ctx->get_tuple_desc(tuple_id);
    if (tuple_desc == nullptr) {
        DB_WARNING("tuple_id:%d not exist", tuple_id);
        return -1;
    }
    if (_sub_query_node->partition_property()->has_no_input_data) {
        _partition_property.has_no_input_data = true;
    }

    // inner(tupleId_slotId) -> <expr, outer(slotId)>
    std::unordered_map<std::string, std::pair<ExprNode*, int>> column_transfer;
    for (auto& [outer_slot_id, inter_column_id] : _slot_column_mapping) {    
        int slot_idx = get_slot_idx(*tuple_desc, outer_slot_id);
        if (slot_idx < 0 || slot_idx >= tuple_desc->slots().size()) {
            DB_WARNING("Invalid slot_idx, slot_idx: %d, slots size: %d", 
                        slot_idx, tuple_desc->slots().size());
            return -1;
        } 
        auto slot = tuple_desc->slots(slot_idx);
        _data_schema.insert(ColumnInfo{tuple_id, outer_slot_id, slot.slot_type()});
        if (_derived_table_projections.size() <= inter_column_id) {
            DB_WARNING("no inter_column_id: %d project expr", inter_column_id);
            return -1;
        }

        auto& expr = _derived_table_projections[inter_column_id];
        if (expr->is_slot_ref()) {
            std::string inner_name = std::to_string(expr->tuple_id()) + "_" + std::to_string(expr->slot_id());
            column_transfer[inner_name] = std::make_pair(expr, outer_slot_id);
        }
    }
    if (_agg_tuple_id_pair.first != -1) {
        const int32_t agg_tuple_id = _agg_tuple_id_pair.first;
        if (agg_tuple_id < 0 || agg_tuple_id >= ctx->tuple_descs().size()) {
            DB_WARNING("agg tuple id invalid: %d", agg_tuple_id);
            return -1;
        }
        auto agg_tuple_desc = ctx->get_tuple_desc(agg_tuple_id);
        if (agg_tuple_desc == nullptr) {
            DB_WARNING("agg_tuple_desc is nullptr");
            return -1;
        }
        for (auto slot_id : _agg_slot_ids) {
            if (_multi_distinct_agg_slot_ids.find(slot_id) != _multi_distinct_agg_slot_ids.end()) {
                continue;
            }
            if (slot_id <= 0 || slot_id > agg_tuple_desc->slots().size()) {
                DB_WARNING("agg slot id invalid: %d", slot_id);
                return -1;
            }
            auto slot = agg_tuple_desc->slots(slot_id - 1);
            _data_schema.insert(ColumnInfo{agg_tuple_id, slot_id, slot.slot_type()});
        }
    }
    // 设置分区属性
    auto sub_query_property = _sub_query_node->partition_property();
    _partition_property.type = sub_query_property->type;

    for (auto& hash_partition_property : sub_query_property->hash_partition_propertys) {
        // 内层分区属性转换为外层
        std::shared_ptr<HashPartitionColumns> outer_property = std::make_shared<HashPartitionColumns>();
        for (auto& inner_name : hash_partition_property->ordered_hash_columns) {
            if (column_transfer.count(inner_name) == 0) {
                // 后续外层再遇到任何HashPartition, 都直接加exchange
                // case1: 内层的分区列,不在外层里, 如
                //        select b from (select b group by a,b) group by b;
                // case2: 内层hash列是个expr, 如select from (select group by a+b) group by xx;
                // TODO 优化
                outer_property->child_hash_column_missed = true;
            } else {
                auto& [expr, outer_slot_id] = column_transfer[inner_name];
                SlotRef* new_column = static_cast<SlotRef*>(expr)->clone();
                new_column->set_tuple_id(tuple_id);
                new_column->set_slot_id(outer_slot_id);
                outer_property->add_slot_ref(new_column);
                std::string outer_name = new_column->arrow_field_name();
                if (sub_query_property->need_cast_string_columns.count(inner_name) > 0) {
                    _partition_property.need_cast_string_columns.insert(outer_name);
                }
                _mpp_hash_slot_refs_mapping[inner_name] = outer_name;
                _mpp_hash_slot_refs_mapping[outer_name] = inner_name;
            }
        }
        _partition_property.hash_partition_propertys.emplace_back(std::move(outer_property));
    }
    return 0;
}

int DualScanNode::set_sub_query_node_partition_property(NodePartitionProperty* outer_property) {
    if (_sub_query_node == nullptr) {
        DB_FATAL("sub_query_node is nullptr");
        return -1;
    }
    const int32_t tuple_id = slot_tuple_id();
    auto sub_query_property = _sub_query_node->partition_property();
    if (outer_property->type == pb::HashPartitionType && sub_query_property->type == pb::AnyType) {
        if (outer_property->hash_partition_propertys.size() != 1) {
            DB_FATAL("outter partition property invalid size: %ld", outer_property->hash_partition_propertys.size());
            return -1;
        }
        // 仅当外层有分区要求, 子查询内没有分区要求, 需要将外层分区列映射到内层
        sub_query_property->hash_partition_propertys.emplace_back(std::make_shared<HashPartitionColumns>());
        int tmp_cnt = 0;
        for (auto& name : outer_property->hash_partition_propertys[0]->ordered_hash_columns) {
            ExprNode* expr = outer_property->hash_partition_propertys[0]->hash_columns[name];
            std::string inner_name;
            if (expr->is_slot_ref() && expr->tuple_id() == tuple_id) {
                // outer slot ref to inner project expr
                int outer_slot_id = expr->slot_id();
                if (_slot_column_mapping.find(outer_slot_id) == _slot_column_mapping.end()) {
                    DB_WARNING("Fail to get slot column, outer_slot_id: %d, tuple_id: %d", outer_slot_id, expr->tuple_id());
                    return -1;
                }
                const int32_t inner_column_id = _slot_column_mapping.at(outer_slot_id);
                if (inner_column_id < 0 || inner_column_id >= _derived_table_projections.size()) {
                    DB_WARNING("Invalid inner_column_id: %d, projections_size: %d", 
                                inner_column_id, (int)_derived_table_projections.size());
                    return -1;
                }
                if (_derived_table_projections[inner_column_id] == nullptr) {
                    DB_WARNING("derived_table_projections[%d] is nullptr", inner_column_id);
                    return -1;
                }
                pb::Expr pb_expr;
                ExprNode::create_pb_expr(&pb_expr, _derived_table_projections[inner_column_id]);
                ExprNode* expr_node = nullptr;
                int ret = ExprNode::create_tree(pb_expr, &expr_node);
                if (ret < 0) {
                    DB_WARNING("Fail to create_tree");
                    return -1;
                }
                if (expr_node->is_slot_ref()) {
                    inner_name = static_cast<SlotRef*>(expr_node)->arrow_field_name();
                    sub_query_property->hash_partition_propertys[0]->add_slot_ref(expr_node);
                } else {
                    inner_name = "hash_" + std::to_string(tmp_cnt++);
                    sub_query_property->hash_partition_propertys[0]->add_need_projection_column(name, expr_node);
                }
            } else {
                // hash by scalar expr
                expr->replace_slot_ref_to_expr(tuple_id, _slot_column_mapping, _derived_table_projections, expr);
                inner_name = "hash_" + std::to_string(tmp_cnt++);
                sub_query_property->hash_partition_propertys[0]->add_need_projection_column(name, expr);
                expr = nullptr;
            }
            if (outer_property->need_cast_string_columns.count(name) > 0) {
                sub_query_property->need_cast_string_columns.insert(inner_name);
            }
        }
    } else {
        // 内外层都是一致的分区属性, 不需要转换, 但是需要处理need_cast_string_columns
        for (auto& outer_name : outer_property->need_cast_string_columns) {
            if (outer_property->hash_partition_propertys.empty()
                    || outer_property->hash_partition_propertys[0]->hash_columns.count(outer_name) == 0) {
                continue;
            }
            if (_mpp_hash_slot_refs_mapping.count(outer_name) == 0) {
                DB_FATAL("mpp execute fail: hash slot refs mapping not found: %s", outer_name.c_str());
                return -1;
            }
            sub_query_property->need_cast_string_columns.insert(_mpp_hash_slot_refs_mapping[outer_name]);
        }
    }
    sub_query_property->type = outer_property->type;
    return 0;
}

int DualScanNode::build_arrow_declaration(RuntimeState* state) {
    START_LOCAL_TRACE_WITH_PARTITION_PROPERTY(get_trace(), state->get_trace_cost(), &_partition_property, OPEN_TRACE, nullptr);
    if (_sub_query_node == nullptr) {
        return 0;
    }
    if (state->is_explain && state->explain_type == EXPLAIN_NULL) {
        return 0;
    }
    // 跳过subquery的packetnode
    if (_sub_query_node->children_size() == 0) {
        DB_WARNING("subquery has no children");
        return -1;
    }
    if (0 != _sub_query_node->children(0)->build_arrow_declaration(_sub_query_runtime_state)) {
        return -1;
    }
    bool no_input_node = _sub_query_runtime_state->acero_declarations.empty();
    state->acero_declarations.insert(state->acero_declarations.end(), 
            _sub_query_runtime_state->acero_declarations.begin(), 
            _sub_query_runtime_state->acero_declarations.end());
    _sub_query_runtime_state->acero_declarations.clear();
    const int32_t tuple_id = slot_tuple_id();
    pb::TupleDescriptor* tuple_desc = state->get_tuple_desc(tuple_id);
    if (tuple_desc == nullptr) {
        return -1;
    }
    // add projectnode for schema transfer(inner subquery->outer)
    std::vector<arrow::compute::Expression> exprs;
    std::vector<std::string> names;
    for (auto iter : _slot_column_mapping) {
        int32_t outer_slot_id = iter.first;
        int32_t inter_column_id = iter.second;
        if (0 != _derived_table_projections[inter_column_id]->transfer_to_arrow_expression()) {
            DB_FATAL_STATE(state, "dual_scan_node projection transfer_to_arrow_expression fail");
            return -1;
        }
        int slot_idx = get_slot_idx(*tuple_desc, outer_slot_id);
        if (slot_idx < 0 || slot_idx >= tuple_desc->slots().size()) {
            DB_WARNING_STATE(state, "Invalid slot_idx, slot_idx: %d, slots size: %d", 
                             slot_idx, tuple_desc->slots().size());
            return -1;
        }
        auto slot = tuple_desc->slots(slot_idx);
        if (is_union_subquery()) {
            // union每个子查询的列需要转化成tuple对应slot的类型
            if (0 != build_arrow_expr_with_cast(_derived_table_projections[inter_column_id], slot.slot_type(), true)) {
                DB_FATAL_STATE(state, "projection transfer_to_arrow_expression fail");
                return -1;
            }
        }
        exprs.emplace_back(_derived_table_projections[inter_column_id]->arrow_expr());
        names.emplace_back(std::to_string(slot.tuple_id()) + "_" + std::to_string(slot.slot_id()));
    }
    if (_agg_tuple_id_pair.first != -1) {
        const int32_t agg_tuple_id = _agg_tuple_id_pair.first;
        const int32_t subquery_agg_tuple_id = _agg_tuple_id_pair.second;
        pb::TupleDescriptor* agg_tuple_desc = state->get_tuple_desc(agg_tuple_id);
        if (agg_tuple_desc == nullptr) {
            DB_WARNING_STATE(state, "agg_tuple_desc is nullptr");
            return -1;
        }
        pb::TupleDescriptor* subquery_agg_tuple_desc = _sub_query_runtime_state->get_tuple_desc(subquery_agg_tuple_id);
        if (subquery_agg_tuple_desc == nullptr) {
            DB_WARNING_STATE(state, "subquery_agg_tuple_desc is nullptr");
            return -1;
        }
        for (auto slot_id : _agg_slot_ids) {
            if (_multi_distinct_agg_slot_ids.find(slot_id) != _multi_distinct_agg_slot_ids.end()) {
                continue;
            }
            arrow::compute::Expression expr = 
                arrow::compute::field_ref(std::to_string(subquery_agg_tuple_id) + "_" + std::to_string(slot_id));
            if (is_union_subquery()) {
                // union每个子查询的列需要转化成外层agg tuple对应slot的类型
                if (slot_id - 1 < 0 || 
                        slot_id - 1 >= agg_tuple_desc->slots().size() || 
                        slot_id - 1 >= subquery_agg_tuple_desc->slots().size()) {
                    DB_FATAL_STATE(state, "invalid slot id: %d", slot_id);
                    return -1;
                }
                pb::PrimitiveType col_type = agg_tuple_desc->slots(slot_id - 1).slot_type();
                pb::PrimitiveType subquery_col_type = subquery_agg_tuple_desc->slots(slot_id - 1).slot_type();
                if (col_type != subquery_col_type) {
                    expr = arrow_cast(expr, subquery_col_type, col_type);
                }
            }
            exprs.emplace_back(expr);
            names.emplace_back(std::to_string(agg_tuple_id) + "_" + std::to_string(slot_id));
        }
    }
    if (no_input_node) {
        // ProjectNode要求必须有InputNode
        // 如子查询是select 1时,需要构建临时SourceNode
        std::shared_ptr<RowVectorizedReader> vectorized_reader = std::make_shared<RowVectorizedReader>();
        if (0 != vectorized_reader->init(state)) {
            return -1;
        } 
        std::function<arrow::Iterator<std::shared_ptr<arrow::RecordBatch>>()> iter_maker = [vectorized_reader] () {
            arrow::Iterator<std::shared_ptr<arrow::RecordBatch>> batch_it = arrow::MakeIteratorFromReader(vectorized_reader);
            return batch_it;
        };
        arrow::acero::Declaration scan_dec;
        if (state->vectorlized_parallel_execution) {
            auto executor = BthreadArrowExecutor::Make(1);
            _arrow_io_executor = *executor;
            scan_dec = arrow::acero::Declaration {"record_batch_source",
                    arrow::acero::RecordBatchSourceNodeOptions{vectorized_reader->schema(), std::move(iter_maker), _arrow_io_executor.get()}}; 
        } else {
            scan_dec = arrow::acero::Declaration {"record_batch_source",
                    arrow::acero::RecordBatchSourceNodeOptions{vectorized_reader->schema(), std::move(iter_maker)}}; 
        }
        state->append_acero_declaration(scan_dec);
        LOCAL_TRACE_ARROW_PLAN_WITH_SCHEMA(scan_dec, vectorized_reader->schema(), nullptr);
    }
    arrow::acero::Declaration dec{"project", arrow::acero::ProjectNodeOptions{exprs, names}};
    LOCAL_TRACE_ARROW_PLAN(dec);
    state->append_acero_declaration(dec);
    return 0;
}

void DualScanNode::transfer_pb(int64_t region_id, pb::PlanNode* pb_node) {
    ExecNode::transfer_pb(region_id, pb_node);
    pb::DerivePlanNode* derive = pb_node->mutable_derive_node();
    pb::DualScanNode* dual_scan_node = derive->mutable_dual_scan_node();
    dual_scan_node->clear_derived_table_projections();
    dual_scan_node->clear_derived_table_projections_agg_or_window_vec();

    for (ExprNode* expr : _derived_table_projections) {
        ExprNode::create_pb_expr(dual_scan_node->add_derived_table_projections(), expr);
    }
    for (bool iter : _derived_table_projections_agg_or_window_vec) {
        dual_scan_node->add_derived_table_projections_agg_or_window_vec(iter);
    }
    if (_sub_query_node != nullptr) {
        create_pb_plan(region_id, dual_scan_node->mutable_sub_query_node(), _sub_query_node);
    }
    if (_sub_query_runtime_state != nullptr) {
        pb::RuntimeState* pb_rs = dual_scan_node->mutable_runtime_state();
        _sub_query_runtime_state->to_proto(pb_rs);
    }
    for (auto& iter : _slot_column_mapping) {
        pb::SlotColumn* slot_column  = dual_scan_node->add_slot_column();
        slot_column->set_slot_id(iter.first);
        slot_column->set_column_id(iter.second);
    }
    if (_agg_tuple_id_pair.first != -1) {
        dual_scan_node->add_agg_tuple_id_pair(_agg_tuple_id_pair.first);
        dual_scan_node->add_agg_tuple_id_pair(_agg_tuple_id_pair.second);
        for (const auto& slot_id : _agg_slot_ids) {
            dual_scan_node->add_agg_slot_ids(slot_id);
        }
        for (const auto& slot_id : _multi_distinct_agg_slot_ids) {
            dual_scan_node->add_multi_distinct_agg_slot_ids(slot_id);
        }
    }
    return;
}

int DualScanNode::child_show_explain(QueryContext* ctx, std::vector<std::map<std::string, std::string>>& output, int& next_id, int display_id) {
    PacketNode* packet = static_cast<PacketNode*>(_sub_query_node->get_node(pb::PACKET_NODE));
    if (packet == nullptr) {
        // should never reach here
        DB_FATAL("when explain packet_node of subquery is nullptr");
        return -1;
    }
    return packet->show_explain(ctx, output, next_id, display_id);
}

int DualScanNode::show_explain(QueryContext* ctx, std::vector<std::map<std::string, std::string>>& output, int& next_id, int display_id) {
    display_id = (display_id != -1 ? display_id : (next_id++));
    std::map<std::string, std::string> explain_info = {
        {"id", std::to_string(display_id)},
        {"select_type", "PRIMARY"},
        {"table", "<derived>"},
        {"partitions", "NULL"},
        {"type", "NULL"},
        {"possible_keys", "NULL"},
        {"key", "NULL"},
        {"key_len", "NULL"},
        {"ref", "NULL"},
        {"rows", "NULL"},
        {"Extra", ""},
        {"sort_index", "0"}
    };

    if (_sub_query_node == nullptr) {
        // 无表的select
        explain_info["table"] = "NULL";
        explain_info["Extra"] = "No tables used";
        output.emplace_back(explain_info);
        return display_id;
    }

    int child_id = child_show_explain(ctx, output, next_id, -1);
    for (auto& it: output) {
        if (strtoll(it["id"].c_str(), NULL, 10) == child_id) {
            it["select_type"] = "DERIVED";
        }
    }

    std::string derived_table_name = "<derived" + std::to_string(child_id)+">";
    explain_info["table"] = derived_table_name;
    output.emplace_back(explain_info);
    return display_id;
}

} // namespace baikaldb
