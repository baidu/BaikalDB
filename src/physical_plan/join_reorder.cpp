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

#include "join_reorder.h"
#include "exec_node.h"
#include "join_node.h"
#include "scan_node.h"
#include "query_context.h"

namespace baikaldb {
int JoinReorder::analyze(QueryContext* ctx) {
    JoinNode* join = static_cast<JoinNode*>(ctx->root->get_node(pb::JOIN_NODE));
    if (join == nullptr) {
        return 0;
    }
    std::map<int32_t, ExecNode*> tuple_join_child_map;  // join的所有非join孩子
    std::map<int32_t, std::set<int32_t>> tuple_equals_map; // 等值条件信息
    std::vector<int32_t> tuple_order; // 目前join顺序
    std::vector<ExprNode*> conditions; // join的全部条件,reorder需要重新下推
    // 获取所有信息
    if (!join->need_reorder(tuple_join_child_map, tuple_equals_map, tuple_order, conditions)) {
        return 0;
    }
    ScanNode* first_node = static_cast<ScanNode*>(
            tuple_join_child_map[tuple_order[0]]->get_node(pb::SCAN_NODE));
    bool first_has_index = false;
    bool is_equal_join = true;
    if (first_node->has_index()) {
        first_has_index = true;
    }
    for (size_t i = 1; i < tuple_order.size(); i++) {
        std::set<int32_t>& tuple_equal_set = tuple_equals_map[tuple_order[i]];
        bool single_equal = false;
        // 与之前的任意tuple等值就行
        for (size_t j = 0; j < i; j++) {
            if (tuple_equal_set.count(tuple_order[j]) == 1) {
                single_equal = true;
                break;
            }
        }
        if (!single_equal) {
            is_equal_join = false;
            break;
        }
    }
    // 第一驱动表有索引并且符合等值join的暂不做reorder
    if (first_has_index && is_equal_join) {
        return 0;
    }

    // do reorder
    // 选出有index的tuple
    std::vector<int32_t> tuple_reorder;
    for (auto& pair : tuple_join_child_map) {
        int32_t tuple_id = pair.first;
        ScanNode* scan_node = static_cast<ScanNode*>(
            pair.second->get_node(pb::SCAN_NODE));
        if (scan_node->has_index()) {
            tuple_reorder.push_back(tuple_id);
            tuple_equals_map.erase(tuple_id);
            break;
        }
    }
    if (tuple_reorder.empty()) {
        if (is_equal_join) {
            return 0;
        }
        tuple_reorder.push_back(tuple_order[0]);
        tuple_equals_map.erase(tuple_order[0]);
    }
    // 根据等值join配对
    while (tuple_equals_map.size() > 0) {
        int32_t select_tuple = -1;
        for (auto& tuple : tuple_reorder) {
            for (auto& pair : tuple_equals_map) {
                if (pair.second.count(tuple) == 1) {
                    select_tuple = pair.first;
                    break;
                }
            }
            if (select_tuple != -1) {
                break;
            }
        }
        if (select_tuple == -1) {
            // no equal join
            DB_WARNING("has no equal condition in join");
            return 0;
        }
        tuple_reorder.push_back(select_tuple);
        tuple_equals_map.erase(select_tuple);
    }
    // 创建新的join节点
    ExecNode* last_node = tuple_join_child_map[tuple_reorder[0]];
    for (size_t i = 1; i < tuple_reorder.size(); i++) {
        pb::PlanNode pb;
        pb.set_node_type(pb::JOIN_NODE);
        pb.set_limit(-1);
        pb.set_is_explain(ctx->is_explain);
        pb.set_num_children(2);
        pb::JoinNode* pb_join = pb.mutable_derive_node()->mutable_join_node();
        pb_join->set_join_type(pb::INNER_JOIN);
        for (size_t j = 0; j < i; j++) {
            pb_join->add_left_tuple_ids(tuple_reorder[j]);
        }
        pb_join->add_right_tuple_ids(tuple_reorder[i]);
        JoinNode* join_node = new JoinNode;
        join_node->init(pb);
        join_node->add_child(last_node);
        join_node->add_child(tuple_join_child_map[tuple_reorder[i]]);
        last_node = join_node;
    }
    last_node->predicate_pushdown(conditions);
    if (!conditions.empty()) {
        DB_FATAL("join reorder predicate_pushdown fail, size:%lu", conditions.size());
        return -1;
    }
    DB_WARNING("join has reordered");
    //pb::Plan plan;
    //ExecNode::create_pb_plan(&plan, ctx->root);
    //DB_NOTICE("before: %s", plan.DebugString().c_str());
    join->get_parent()->replace_child(join, last_node);
    join->reorder_clear();
    delete join;
    //pb::Plan plan2;
    //ExecNode::create_pb_plan(&plan2, ctx->root);
    //DB_NOTICE("after: %s", plan2.DebugString().c_str());
    return 0;
}

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
