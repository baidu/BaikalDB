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
#include "sort_node.h"
#include "join_node.h"
#include "union_node.h"
#include "expr_optimizer.h"

namespace baikaldb {

void ExprOptimize::analyze_union(QueryContext* ctx, PacketNode* packet_node) {
    ExecNode* plan = ctx->root;
    std::vector<ExprNode*>& union_projections = packet_node->mutable_projections();
    // 类型按第一个select语句的列类型赋值
    // todo：根据所有select的列进行类型推导
    auto select_ctx = ctx->sub_query_plans[0];
    ExecNode* select_plan = select_ctx->root;
    UnionNode* union_node = static_cast<UnionNode*>(plan->get_node(pb::UNION_NODE));
    PacketNode* select_packet_node = static_cast<PacketNode*>(select_plan->get_node(pb::PACKET_NODE));
    AggNode* agg_node = static_cast<AggNode*>(plan->get_node(pb::AGG_NODE));
    std::vector<ExprNode*> group_exprs;
    if (agg_node != nullptr) {
        group_exprs = *(agg_node->mutable_group_exprs());
    }
    auto select_projections = select_packet_node->mutable_projections();
    pb::TupleDescriptor* tuple_desc = ctx->get_tuple_desc(union_node->union_tuple_id());
    for (size_t i = 0; i < union_projections.size(); i++) {
        union_projections[i]->set_col_type(select_projections[i]->col_type());
        auto slot = tuple_desc->mutable_slots(i);
        slot->set_slot_type(select_projections[i]->col_type());
        if (group_exprs.size() > 0) {
            group_exprs[i]->set_col_type(select_projections[i]->col_type());
        }
    }
    // 条件下推可能生成FilterNode
    std::vector<ExecNode*> filter_nodes;
    plan->get_node(pb::TABLE_FILTER_NODE, filter_nodes);
    plan->get_node(pb::WHERE_FILTER_NODE, filter_nodes);
    plan->get_node(pb::HAVING_FILTER_NODE, filter_nodes);
    for (auto* node : filter_nodes) {
        FilterNode* filter_node = static_cast<FilterNode*>(node);
        if (filter_node == nullptr) {
            DB_WARNING("filter_node is nullptr");
            return;
        }
        for (auto* expr : *(filter_node->mutable_conjuncts())) {
            if (expr == nullptr) {
                DB_WARNING("expr is nullptr");
                return;
            }
            for (int i = 0; i < select_projections.size(); ++i) {
                expr->set_slot_col_type(tuple_desc->tuple_id(), i + 1, select_projections[i]->col_type());
            }
        }
    }
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
            auto slot = tuple_desc->mutable_slots(outer_slot_id - 1);
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
        }
    }
   return 0;
}

} // namespace baikaldb
