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
}
int ExprOptimize::analyze_derived_table(QueryContext* ctx, PacketNode* packet_node) {
    ExecNode* plan = ctx->root;
    std::vector<ExecNode*> join_nodes;
    plan->get_node(pb::JOIN_NODE, join_nodes);
    SortNode* sort_node = static_cast<SortNode*>(plan->get_node(pb::SORT_NODE));
    FilterNode* filter_node = static_cast<FilterNode*>(plan->get_node(pb::WHERE_FILTER_NODE));
    FilterNode* having_node = static_cast<FilterNode*>(plan->get_node(pb::HAVING_FILTER_NODE));
    AggNode* agg_node = static_cast<AggNode*>(plan->get_node(pb::AGG_NODE));
    AggNode* merge_agg_node = static_cast<AggNode*>(plan->get_node(pb::MERGE_AGG_NODE));
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
                DB_WARNING("plan ilegal");
                return -1;
            } 
            pb::PrimitiveType type = select_projections[inter_column_id]->col_type();
            for (auto expr : outer_projections) {
                 expr->set_slot_col_type(tuple_id, outer_slot_id, type);
            }
            auto slot = tuple_desc->mutable_slots(outer_slot_id - 1);
            slot->set_slot_type(type);
            if (sort_node != nullptr) {
                for (auto expr : *(sort_node->mutable_order_exprs())) {
                    expr->set_slot_col_type(tuple_id, outer_slot_id, type);
                }
                for (auto expr : *(sort_node->mutable_slot_order_exprs())) {
                    expr->set_slot_col_type(tuple_id, outer_slot_id, type);
                }
            }
            if (filter_node != nullptr) {
                for (auto expr : *(filter_node->mutable_conjuncts())) {
                    expr->set_slot_col_type(tuple_id, outer_slot_id, type);
                }
            }
            if (having_node != nullptr) {
                for (auto expr : *(having_node->mutable_conjuncts())) {
                    expr->set_slot_col_type(tuple_id, outer_slot_id, type);
                }
            }
            if (agg_node != nullptr) {
                for (auto expr : *(agg_node->mutable_group_exprs())) {
                    expr->set_slot_col_type(tuple_id, outer_slot_id, type);
                }
                for (auto expr : *(agg_node->mutable_agg_fn_calls())) {
                    expr->set_slot_col_type(tuple_id, outer_slot_id, type);
                }
            }
            if (merge_agg_node != nullptr) {
                for (auto expr : *(merge_agg_node->mutable_group_exprs())) {
                    expr->set_slot_col_type(tuple_id, outer_slot_id, type);
                }
                for (auto expr : *(merge_agg_node->mutable_agg_fn_calls())) {
                    expr->set_slot_col_type(tuple_id, outer_slot_id, type);
                }
            }
            for (auto join_node : join_nodes) {
                JoinNode* join = static_cast<JoinNode*>(join_node); 
                for (auto expr : *(join->mutable_conjuncts())) {
                    expr->set_slot_col_type(tuple_id, outer_slot_id, type);
                }
            }
        }
    }
   return 0;
}

} // namespace baikaldb
