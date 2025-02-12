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

#include "apply_node.h"
#include "dual_scan_node.h"
#include "exec_node.h"
#include "filter_node.h"
#include "join_node.h"
#include "query_context.h"

namespace baikaldb {
class PredicatePushDown {
public:
    int analyze(QueryContext* ctx) {
        // FROM型或UNION型子查询只在最外层查询进行一次谓词下推
        if (ctx->is_from_subquery || ctx->is_union_subquery) {
            return 0;
        }
        // 目前只有包含join节点或者子查询才做谓词下推
        ExecNode* plan = ctx->root;
        JoinNode* join = static_cast<JoinNode*>(plan->get_node(pb::JOIN_NODE));
        ApplyNode* apply = static_cast<ApplyNode*>(plan->get_node(pb::APPLY_NODE));
        DualScanNode* dual = static_cast<DualScanNode*>(plan->get_node(pb::DUAL_SCAN_NODE));
        if (join == NULL && apply == NULL && dual == NULL) {
            //DB_WARNING("has no join or dual, not predicate")
            return 0;
        }
        if (analyze_subquery(ctx) != 0) {
            DB_WARNING("Fail to analyze_subquery");
            return -1;
        }
        std::vector<ExprNode*> empty_exprs;
        if (0 != plan->predicate_pushdown(empty_exprs)) {
            DB_WARNING("predicate push down fail");
            return -1;
        }
        return 0;
    }

private:
    // 用于配置所有DualScanNode属性
    int analyze_subquery(QueryContext* ctx) {
        if (ctx == nullptr) {
            DB_WARNING("ctx is nullptr");
            return -1;
        }

        std::map<int32_t, std::shared_ptr<QueryContext>> all_derived_table_ctx_mapping;
        ctx->get_all_derived_table_ctx_mapping(all_derived_table_ctx_mapping);
        std::map<int32_t, std::map<int32_t, int32_t>> all_slot_column_mapping;
        ctx->get_all_slot_column_mapping(all_slot_column_mapping);

        std::vector<ExecNode*> dual_scan_nodes;
        dual_scan_nodes.reserve(10);

        ExecNode* plan = ctx->root;
        if (plan == nullptr) {
            DB_WARNING("plan is nullptr");
            return -1;
        }
        plan->get_node(pb::DUAL_SCAN_NODE, dual_scan_nodes);
        for (auto& kv : all_derived_table_ctx_mapping) {
            auto derived_table_ctx = kv.second;
            if (derived_table_ctx == nullptr) {
                DB_WARNING("derived_table_ctx is nullptr");
                return -1;
            }
            ExecNode* derived_plan = derived_table_ctx->root;
            if (derived_plan == nullptr) {
                DB_WARNING("derived_plan is nullptr");
                return -1;
            }
            std::vector<ExecNode*> dual_scan_nodes_tmp;
            derived_plan->get_node(pb::DUAL_SCAN_NODE, dual_scan_nodes_tmp);
            dual_scan_nodes.insert(dual_scan_nodes.end(), dual_scan_nodes_tmp.begin(), dual_scan_nodes_tmp.end());
        }

        for (auto* node : dual_scan_nodes) {
            DualScanNode* dual_scan_node = static_cast<DualScanNode*>(node);
            if (dual_scan_node == nullptr) {
                DB_WARNING("dual_scan_node is nullptr");
                return -1;
            }
            if (!dual_scan_node->pb_node().has_derive_node()) {
                // SELECT xxx FROM DUAL场景
                continue;
            }
            const int32_t derived_tuple_id = dual_scan_node->tuple_id();
            auto iter = all_derived_table_ctx_mapping.find(derived_tuple_id);
            if (iter == all_derived_table_ctx_mapping.end()) {
                DB_WARNING("Fail to find subquery ctx, tuple_id: %d", derived_tuple_id);
                continue;
            }
            auto sub_query_ctx = iter->second;
            if (sub_query_ctx == nullptr) {
                DB_WARNING("sub_query_ctx is nullptr");
                return -1;
            }
            ExecNode* sub_query_plan = sub_query_ctx->root;
            if (sub_query_plan == nullptr) {
                DB_WARNING("sub_query_plan is nullptr");
                return -1;
            }
            PacketNode* packet_node = static_cast<PacketNode*>(sub_query_plan->get_node(pb::PACKET_NODE));
            if (packet_node == nullptr) {
                DB_WARNING("packet_node is nullptr");
                return -1;
            }
            // Union节点下的所有子查询共用一个slot_column_mapping
            const int32_t slot_tuple_id = dual_scan_node->slot_tuple_id();
            dual_scan_node->set_slot_column_mapping(all_slot_column_mapping[slot_tuple_id]);
            dual_scan_node->set_sub_query_node(sub_query_ctx->root);
            dual_scan_node->set_has_subquery(true);
            if (dual_scan_node->create_derived_table_projections(packet_node->pb_node()) != 0) {
                DB_WARNING("Fail to create_derived_table_projections");
                return -1;
            }
        }
        return 0;
    }
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
