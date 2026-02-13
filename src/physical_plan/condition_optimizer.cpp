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
#include "condition_optimizer.h"
#include "vectorize_helpper.h"
#include "filter_node.h"
#include "agg_node.h"
#include "limit_node.h"
#include "sort_node.h"
#include "rocksdb_scan_node.h"
#include "select_manager_node.h"
#include "schema_factory.h"

namespace baikaldb {
int ConditionOptimizer::analyze(QueryContext* ctx) {
    if (ctx->is_from_subquery
         || ctx->is_union_subquery
         || ctx->is_insert_select_subquery) {
        return 0;
    }
    if (ctx->stmt_type != parser::NT_SELECT
         && ctx->stmt_type != parser::NT_UNION) {
        return 0;
    }
    _ctx = ctx;
    // 当in filter很大时, 判断是否要调整成in filter不下推的计划, 避免fetcher_store oom
    std::vector<ExecNode*> scan_nodes;
    ctx->root->get_node_pass_subquery(pb::SCAN_NODE, scan_nodes);
    for (auto scan_node : scan_nodes) {
        adjust_huge_in_condition(static_cast<ScanNode*>(scan_node), false);
    }
    return 0;
}

// 单拎出来为, join下推的in和普通in条件处理逻辑一致
int ConditionOptimizer::adjust_huge_in_condition(ScanNode* scan_node, bool in_acero) {
    if (_ctx == nullptr || scan_node == nullptr) {
        return 0;
    }
    if (!scan_node->is_rocksdb_scan_node()) {
        return 0;
    }
    ExecNode* parent_node = scan_node->get_parent();
    if (parent_node->node_type() != pb::WHERE_FILTER_NODE 
        && parent_node->node_type() != pb::TABLE_FILTER_NODE) {
        return 0;
    }
    FilterNode* filter_node = static_cast<FilterNode*>(parent_node);
    if (!filter_node->has_huge_in_condition()) {
        return 0;
    }

    ScanIndexInfo* scan_index_info = scan_node->main_scan_index();
    int64_t router_index_id = scan_index_info->router_index_id;
    int64_t main_table_id = scan_node->table_id();
    if (router_index_id != main_table_id/* && !scan_index_info->covering_index*/) {
        // 先不支持全局索引
        return 0;
    }

    // sort agg需要调到selectmanager上面
    ExecNode* parent = scan_node->get_parent();
    SortNode* sort_node = nullptr;
    AggNode* agg_node = nullptr;
    SelectManagerNode* select_manager = nullptr;
    bool has_limit = false;
    while (parent != nullptr) {
        if (parent->node_type() == pb::SORT_NODE) {
            sort_node = static_cast<SortNode*>(parent);
        } else if (parent->node_type() == pb::AGG_NODE) {
            agg_node = static_cast<AggNode*>(parent);
        } else if (parent->node_type() == pb::SELECT_MANAGER_NODE) {
            select_manager = static_cast<SelectManagerNode*>(parent);
            break;
        }
        parent = parent->get_parent();
    }
    if (select_manager == nullptr) {
        return 0;
    }

    if (agg_node != nullptr) {
        if (in_acero) {
            // join in条件runtime filter, 此时db acero计划已经定了并启动了
            return 0;
        }
    } else if (sort_node != nullptr && sort_node->need_projection()) {
        // 遇到case,可以考虑在selectmanaeger那先做projection.行列都需要
        return 0;
    }

    // filter干掉huge in, 并重新序列化filter_str
    std::vector<ExprNode*> huge_in_exprs;
    filter_node->cut_huge_in_condition(huge_in_exprs);
    if (huge_in_exprs.empty()) {
        return 0;
    }
    DB_WARNING("log_id: %lu, huge in condition size: %ld, cut huge in condition", 
            _ctx->stat_info.log_id, huge_in_exprs.size());
    if (select_manager->get_limit() != -1) {
        has_limit = true;
    }
    if (agg_node != nullptr) {
        // merge_agg_parent -> merge_agg -> selectmangaer -> agg -> filter  ====>
        // merge_agg_parent -> merge_agg transfer agg_node -> selectmanager(filter) -> filter  (delete agg_node)
        auto merge_agg_node = select_manager->get_parent();
        if (merge_agg_node == nullptr 
                || merge_agg_node->node_type() != pb::MERGE_AGG_NODE) {
            return 0;
        }
        static_cast<AggNode*>(merge_agg_node)->transfer_to_agg();
        auto agg_child = agg_node->children(0);
        select_manager->replace_child(agg_node, agg_child);
        agg_node->clear_children();
        delete agg_node;
    } else if (sort_node != nullptr) {  
        // parent -> selectmanager(sort) -> sort -> filter  ====>
        // parent -> selectmanager(filter -> sort) -> filter (delete sort_node)
        auto sort_child = sort_node->children(0);
        select_manager->replace_child(sort_node, sort_child);
        select_manager->steal_slot_order_exprs(sort_node);
        sort_node->clear_children();
        delete sort_node;
    } else if (has_limit) {
        // limit -> selectmanager(limit) -> filter(limit)  ====>
        // limit -> selectmanager(filter -> limit) -> filter(limit:-1) 
        ExecNode* node = select_manager->children(0);
        while (node != nullptr) {
            node->set_limit(-1);
            if (node->children_size() > 0) {
                node = node->children(0);
            } else {
                break;
            }
        }
    }
    select_manager->add_conditions(huge_in_exprs);
    return 0;
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
