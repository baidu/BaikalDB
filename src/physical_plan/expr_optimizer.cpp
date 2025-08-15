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
#include "select_manager_node.h"
#include "agg_node.h"
#include "dual_scan_node.h"
#include "sort_node.h"
#include "join_node.h"
#include "union_node.h"
#include "window_node.h"
#include "expr_optimizer.h"

namespace baikaldb {

int ExprOptimize::analyze_union(QueryContext* ctx, PacketNode* packet_node) {
    if (ctx == nullptr) {
        DB_WARNING("ctx is nullptr");
        return -1;
    }
    ExecNode* plan = ctx->root;
    std::vector<ExprNode*>& union_projections = packet_node->mutable_projections();
    // 类型按第一个select语句的列类型赋值
    // todo：根据所有select的列进行类型推导
    auto select_ctx = ctx->sub_query_plans[0];
    ExecNode* select_plan = select_ctx->root;
    UnionNode* union_node = static_cast<UnionNode*>(plan->get_node(pb::UNION_NODE));
    PacketNode* select_packet_node = static_cast<PacketNode*>(select_plan->get_node(pb::PACKET_NODE));
    auto select_projections = select_packet_node->mutable_projections();
    int32_t union_tuple_id = union_node->union_tuple_id();
    pb::TupleDescriptor* tuple_desc = ctx->get_tuple_desc(union_tuple_id);
    const auto& slot_column_map = ctx->slot_column_mapping[union_tuple_id];
    
    std::vector<ExecNode*> agg_nodes;
    plan->get_node(pb::AGG_NODE, agg_nodes);
    plan->get_node(pb::MERGE_AGG_NODE, agg_nodes);

    std::vector<ExecNode*> filter_nodes;
    plan->get_node(pb::TABLE_FILTER_NODE, filter_nodes);
    plan->get_node(pb::WHERE_FILTER_NODE, filter_nodes);
    plan->get_node(pb::HAVING_FILTER_NODE, filter_nodes);

    for (const auto& [outer_slot_id, inter_column_id] : slot_column_map) {
        if (inter_column_id < 0 || inter_column_id >= select_projections.size()) {
            DB_WARNING("Invalid inter_column_id: %d, select_projections.size(): %lu", 
                        inter_column_id, select_projections.size());
            return -1;
        }
        pb::PrimitiveType col_type = select_projections[inter_column_id]->col_type();
        // 特殊处理null类型
        if (select_projections[inter_column_id]->node_type() == pb::NULL_LITERAL) {
            for (auto plan_id = 1; plan_id < ctx->sub_query_plans.size(); ++plan_id) {
                ExecNode* another_select_plan = ctx->sub_query_plans[plan_id]->root;
                PacketNode* another_select_packet_node = static_cast<PacketNode*>(another_select_plan->get_node(pb::PACKET_NODE));
                auto& another_select_projections = another_select_packet_node->mutable_projections();
                if (another_select_projections.size() > inter_column_id 
                        && another_select_projections[inter_column_id]->node_type() != pb::NULL_LITERAL) {
                    col_type = another_select_projections[inter_column_id]->col_type();
                    break;
                }
            }
        }
        int slot_idx = get_slot_idx(*tuple_desc, outer_slot_id);
        if (slot_idx < 0 || slot_idx >= tuple_desc->slots().size()) {
            DB_WARNING("Invalid slot_idx: %d, slots size: %d", slot_idx, tuple_desc->slots().size());
            return -1;
        }
        auto slot = tuple_desc->mutable_slots(slot_idx);
        slot->set_slot_type(col_type);
        for (auto expr : union_projections) {
            if (expr == nullptr) {
                DB_WARNING("expr is nullptr");
                return -1;
            }
            expr->set_slot_col_type(union_tuple_id, outer_slot_id, col_type);
        }
        for (auto* node : agg_nodes) {
             AggNode* agg_node = static_cast<AggNode*>(node);
            if (agg_node == nullptr) {
                DB_WARNING("agg_node is nullptr");
                return -1;
            }
            for (auto* expr : *(agg_node->mutable_group_exprs())) {
                if (expr == nullptr) {
                    DB_WARNING("expr is nullptr");
                    return -1;
                }
                expr->set_slot_col_type(union_tuple_id, outer_slot_id, col_type);
            }
            for (auto* expr : *(agg_node->mutable_agg_fn_calls())) {
                if (expr == nullptr) {
                   DB_WARNING("expr is nullptr");
                    return -1;
                }
                expr->set_slot_col_type(union_tuple_id, outer_slot_id, col_type);
            }
        }
        for (auto* node : filter_nodes) {
            FilterNode* filter_node = static_cast<FilterNode*>(node);
            if (filter_node == nullptr) {
                DB_WARNING("filter_node is nullptr");
                return -1;
            }
            for (auto* expr : *(filter_node->mutable_conjuncts())) {
                if (expr == nullptr) {
                    DB_WARNING("expr is nullptr");
                    return -1;
                }
                expr->set_slot_col_type(union_tuple_id, outer_slot_id, col_type);
            }
        }
    }
    return 0;
}
int ExprOptimize::analyze_derived_table(QueryContext* ctx, PacketNode* packet_node) {
    ExecNode* plan = ctx->root;
    std::vector<ExecNode*> join_nodes;
    plan->get_node(pb::JOIN_NODE, join_nodes);
    std::vector<ExecNode*> apply_nodes;
    plan->get_node(pb::APPLY_NODE, apply_nodes);
    std::vector<ExecNode*> filter_nodes;
    plan->get_node(pb::TABLE_FILTER_NODE, filter_nodes);
    plan->get_node(pb::WHERE_FILTER_NODE, filter_nodes);
    plan->get_node(pb::HAVING_FILTER_NODE, filter_nodes);
    std::vector<ExecNode*> sort_nodes;
    plan->get_node(pb::SORT_NODE, sort_nodes);
    std::vector<ExecNode*> agg_nodes;
    plan->get_node(pb::AGG_NODE, agg_nodes);
    plan->get_node(pb::MERGE_AGG_NODE, agg_nodes);
    std::vector<ExecNode*> window_nodes;
    plan->get_node(pb::WINDOW_NODE, window_nodes);
    std::vector<ExecNode*> dual_nodes;
    plan->get_node(pb::DUAL_SCAN_NODE, dual_nodes);
    std::vector<ExprNode*>& outer_projections = packet_node->mutable_projections();
    for (auto& iter : ctx->derived_table_ctx_mapping) {
        auto& subquery_ctx = iter.second;
        ExecNode* select_plan = subquery_ctx->root;
        PacketNode* select_packet_node = static_cast<PacketNode*>(select_plan->get_node(pb::PACKET_NODE));
        auto select_projections = select_packet_node->mutable_projections();
        int32_t tuple_id = iter.first;
        auto& slot_column_map = ctx->slot_column_mapping[tuple_id];
        pb::TupleDescriptor* tuple_desc = ctx->get_tuple_desc(tuple_id);
        for (auto& iter : slot_column_map) {
            int32_t outer_slot_id = iter.first;
            int32_t inter_column_id = iter.second;
            if (inter_column_id >= (int)select_projections.size()) {
                DB_WARNING("plan illegal");
                return -1;
            } 
            pb::PrimitiveType type = select_projections[inter_column_id]->col_type();
            for (auto expr : outer_projections) {
                 expr->set_slot_col_type(tuple_id, outer_slot_id, type);
            }
            int slot_idx = get_slot_idx(*tuple_desc, outer_slot_id);
            if (slot_idx < 0 || slot_idx >= tuple_desc->slots().size()) {
                DB_WARNING("Invalid slot_idx: %d, slots size: %d", slot_idx, tuple_desc->slots().size());
                return -1;
            }
            auto slot = tuple_desc->mutable_slots(slot_idx);
            slot->set_slot_type(type);
            for (auto s : sort_nodes) {
                auto* sort_node = static_cast<SortNode*>(s);
                for (auto expr : *(sort_node->mutable_order_exprs())) {
                    expr->set_slot_col_type(tuple_id, outer_slot_id, type);
                }
                for (auto expr : *(sort_node->mutable_slot_order_exprs())) {
                    expr->set_slot_col_type(tuple_id, outer_slot_id, type);
                }
            }
            for (auto table_filter : filter_nodes) {
                FilterNode* table_filter_node = static_cast<FilterNode*>(table_filter); 
                for (auto expr : *(table_filter_node->mutable_conjuncts())) {
                    expr->set_slot_col_type(tuple_id, outer_slot_id, type);
                }
            }
            for (auto a : agg_nodes) {
                auto agg_node = static_cast<AggNode*>(a);
                for (auto expr : *(agg_node->mutable_group_exprs())) {
                    expr->set_slot_col_type(tuple_id, outer_slot_id, type);
                }
                for (auto expr : *(agg_node->mutable_agg_fn_calls())) {
                    expr->set_slot_col_type(tuple_id, outer_slot_id, type);
                }
            }
            for (auto join_node : join_nodes) {
                JoinNode* join = static_cast<JoinNode*>(join_node); 
                for (auto expr : *(join->mutable_conjuncts())) {
                    expr->set_slot_col_type(tuple_id, outer_slot_id, type);
                }
            }
            for (auto apply_node : apply_nodes) {
                ApplyNode* apply = static_cast<ApplyNode*>(apply_node); 
                for (auto expr : *(apply->mutable_conjuncts())) {
                    expr->set_slot_col_type(tuple_id, outer_slot_id, type);
                }
            }
            for (auto window_node : window_nodes) {
                WindowNode* window = static_cast<WindowNode*>(window_node);
                for (auto expr : *(window->mutable_partition_exprs())) {
                    expr->set_slot_col_type(tuple_id, outer_slot_id, type);
                }
                auto window_processor = window->mutable_window_processor();
                if (window_processor != nullptr) {
                    for (auto expr : *(window_processor->mutable_window_fn_calls())) {
                        expr->set_slot_col_type(tuple_id, outer_slot_id, type);
                    }
                    for (auto expr : *(window_processor->mutable_slot_order_exprs())) {
                        expr->set_slot_col_type(tuple_id, outer_slot_id, type);
                    }
                }
            }
        }
    }
    // 聚合下推场景，下推普通子查询后，外层agg节点会被删掉，外层agg tuple需要在这里进行类型推导
    for (auto dual_node : dual_nodes) {
        DualScanNode* dual = static_cast<DualScanNode*>(dual_node);
        if (dual == nullptr) {
            DB_WARNING("dual is nullptr");
            return -1;
        }
        if (dual->is_union_subquery()) {
            continue;
        }
        const std::pair<int32_t, int32_t>& agg_tuple_id_pair = dual->agg_tuple_id_pair();
        if (agg_tuple_id_pair.first != -1) {
            QueryContext* subquery_ctx = dual->sub_query_ctx();
            if (subquery_ctx == nullptr) {
                DB_WARNING("subquery_ctx is nullptr");
                return -1;
            }
            const std::vector<int32_t>& agg_slot_ids = dual->agg_slot_ids();
            const int32_t agg_tuple_id = agg_tuple_id_pair.first;
            const int32_t subquery_agg_tuple_id = agg_tuple_id_pair.second;
            pb::TupleDescriptor* tuple_desc = ctx->get_tuple_desc(agg_tuple_id);
            pb::TupleDescriptor* subquery_tuple_desc = subquery_ctx->get_tuple_desc(subquery_agg_tuple_id);
            if (tuple_desc == nullptr || subquery_tuple_desc == nullptr) {
                DB_WARNING("tuple desc is nullptr");
                return -1;
            }
            for (auto slot_id : agg_slot_ids) {
                if (slot_id <= 0 || 
                        slot_id > subquery_tuple_desc->slots().size() || 
                        slot_id > tuple_desc->slots().size()) {
                    DB_WARNING("invalid slot_id: %d", slot_id);
                    return -1;
                }
                auto slot = tuple_desc->mutable_slots(slot_id - 1);
                const auto& subquery_slot = subquery_tuple_desc->slots(slot_id - 1);
                slot->set_slot_type(subquery_slot.slot_type());
            }
        }
    }
    return 0;
}

} // namespace baikaldb
