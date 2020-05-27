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
#include "packet_node.h"
#include "agg_node.h"
#include "sort_node.h"
#include "query_context.h"

namespace baikaldb {
class ExprOptimize {
public:
    /* 表达式类型推导
     * agg tuple类型推导
     * const表达式求值
     */
    int analyze(QueryContext* ctx) {
        ExecNode* plan = ctx->root;
        PacketNode* packet_node = static_cast<PacketNode*>(plan->get_node(pb::PACKET_NODE));
        if (packet_node == nullptr) {
            return -1;
        }
        if (packet_node->op_type() == pb::OP_UNION) {
            std::vector<ExprNode*>& union_projections = packet_node->mutable_projections();
            auto select_ctx = ctx->union_select_plans[0];
            ExecNode* select_plan = select_ctx->root;
            PacketNode* select_packet_node = static_cast<PacketNode*>(select_plan->get_node(pb::PACKET_NODE));
            AggNode* agg_node = static_cast<AggNode*>(plan->get_node(pb::AGG_NODE));
            std::vector<ExprNode*> group_exprs;
            if (agg_node != nullptr) {
                group_exprs = agg_node->group_exprs();
            }
            auto select_projections = select_packet_node->mutable_projections();
            pb::TupleDescriptor* tuple_desc = ctx->get_tuple_desc(0);
            for (int i = 0; i < union_projections.size(); i++) {
                union_projections[i]->set_col_type(select_projections[i]->col_type());
                auto slot = tuple_desc->mutable_slots(i);
                slot->set_slot_type(select_projections[i]->col_type());
                if (group_exprs.size()) {
                    group_exprs[i]->set_col_type(select_projections[i]->col_type());
                }
            }
            SortNode* sort_node = static_cast<SortNode*>(plan->get_node(pb::SORT_NODE));
            if (sort_node != nullptr) {
                std::vector<ExprNode*>& order_exprs = sort_node->order_exprs();
                std::vector<ExprNode*>& slot_order_exprs = sort_node->slot_order_exprs();
                for (int i = 0; i < order_exprs.size(); i++) {
                    auto slot_id = order_exprs[i]->slot_id();
                    for (auto slot : tuple_desc->slots()) {
                        if (slot.slot_id() == slot_id) {
                            order_exprs[i]->set_col_type(slot.slot_type());
                            slot_order_exprs[i]->set_col_type(slot.slot_type());
                        }
                    }
                }
            }

        }
        int ret = plan->expr_optimize(ctx->mutable_tuple_descs());
        if (ret == -2) {
            DB_WARNING("filter always false");
            ctx->return_empty = true;
            return 0;
        } else {
            return ret;
        }
    }
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
