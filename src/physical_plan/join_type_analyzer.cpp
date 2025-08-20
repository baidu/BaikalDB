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

#include "plan.pb.h"
#include "index_selector.h"
#include "join_type_analyzer.h"
#include "join_node.h"
#include "scan_node.h"
#include "dual_scan_node.h"

namespace baikaldb {
DECLARE_bool(use_arrow_vector);
int JoinTypeAnalyzer::analyze(QueryContext* ctx) {
    if (!FLAGS_use_arrow_vector) {
        return 0;
    }
    if (ctx->is_insert_select_subquery) {
        return 0;
    }
    if (0 != pushdown_in_condition_and_choose_index(ctx, ctx->root)) {
        return -1;
    }
    return 0;
}

ExecNode* JoinTypeAnalyzer::get_need_pushdown_scan_node(QueryContext* ctx, 
                                                        ExecNode* node, 
                                                        ExprNode** condition, 
                                                        QueryContext** scannode_ctx,
                                                        bool* expr_can_pushdown) {
    if (ctx == nullptr || node == nullptr) {
        return nullptr;
    }
    if (node->node_type() == pb::SCAN_NODE) {
        return node;
    } else if (node->node_type() == pb::JOIN_NODE) {
        return get_need_pushdown_scan_node(ctx, static_cast<JoinNode*>(node)->get_outter_node(), condition, scannode_ctx, expr_can_pushdown);
    } else if (node->node_type() == pb::DUAL_SCAN_NODE) {
        DualScanNode* dual_scan = static_cast<DualScanNode*>(node);
        const int32_t derived_tuple_id = dual_scan->tuple_id();
        auto iter = ctx->derived_table_ctx_mapping.find(derived_tuple_id);
        if (iter == ctx->derived_table_ctx_mapping.end()) {
            DB_WARNING("Fail to find subquery, tuple_id: %d", derived_tuple_id);
            return nullptr;
        }
        if (iter->second == nullptr) {
            DB_WARNING("sub_query_ctx is nullptr");
            return nullptr;
        }
        *scannode_ctx = iter->second.get();
        if (0 != dual_scan->transfer_outer_slot_to_inner(condition)) {
            if (expr_can_pushdown != nullptr) {
                *expr_can_pushdown = false;
            }
            return nullptr;
        }
        return get_need_pushdown_scan_node(dual_scan->sub_query_ctx(), dual_scan->sub_query_node(), condition, scannode_ctx, expr_can_pushdown);
    } else if (node->node_type() == pb::UNION_NODE) {
        return nullptr;
    }
    if (node->children_size() > 0) {
        return get_need_pushdown_scan_node(ctx, node->children(0), condition, scannode_ctx, expr_can_pushdown);
    }
    return nullptr;
}

int JoinTypeAnalyzer::pushdown_in_condition_and_choose_index(QueryContext* ctx, ExecNode* node) {
    if (node == nullptr) {
        return 0;
    }
    JoinNode* join_node = static_cast<JoinNode*>(node->get_node(pb::JOIN_NODE));
    if (join_node == nullptr) {
        return 0;
    }
    ExecNode* outer_node = join_node->get_outter_node();
    ExecNode* inner_node = join_node->get_inner_node();
    if (outer_node != nullptr) {
        if (0 != pushdown_in_condition_and_choose_index(ctx, outer_node)) {
            return -1;
        }
    }
    if (inner_node == nullptr) {
        return 0;
    }
    if (0 != pushdown_in_condition_and_choose_index(ctx, inner_node)) {
        return -1;
    }

    // 构建非驱动表的in条件
    ExprNode* in_condition = nullptr;
    SmartState state = ctx->get_runtime_state();
    join_node->get_join_on_condition_filter(state.get(), &in_condition);
    if (in_condition == nullptr) {
        DB_WARNING("join on condition is null");
        return 0;
    }
    ScopeGuard scope_guard([&in_condition]() {
        if (in_condition != nullptr) {
            ExprNode::destroy_tree(in_condition);
        }
    });

    // 找到对应的scannode
    QueryContext* scan_ctx = ctx;
    bool expr_can_pushdown = true;
    ExecNode* scan_node = get_need_pushdown_scan_node(ctx, inner_node, &in_condition, &scan_ctx, &expr_can_pushdown);
    if (scan_node == nullptr) {
        DB_WARNING("can't find scan node");
        return 0;
    }
    ScanNode* scan_node_ptr = static_cast<ScanNode*>(scan_node);
    // join构造的in条件不能下推, 可以用no index join
    if (!expr_can_pushdown) {
        join_node->set_use_index_join(false);
        return 0;
    }

    // 如果in条件的tuple包含了非scannode的tuple, 不能下推
    std::unordered_set<int32_t> in_condition_tuple_ids;
    in_condition->get_all_tuple_ids(in_condition_tuple_ids);
    if (in_condition_tuple_ids.size() != 1 
        || in_condition_tuple_ids.count(scan_node_ptr->tuple_id()) == 0) {
        join_node->set_use_index_join(false);
        return 0;
    }

    // 对scannode重新选择索引
    bool index_has_null = false;
    IndexSelector().analyze_join_index(scan_ctx, scan_node_ptr, in_condition);
    // 判断并设置join类型
    if (scan_node_ptr->can_use_no_index_join()) {
        join_node->set_use_index_join(false);
    }
    return 0;
}
}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
