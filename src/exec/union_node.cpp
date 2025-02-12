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

#include "runtime_state.h"
#include "union_node.h"
#include "network_socket.h"
#include "arrow_function.h"
#include "dual_scan_node.h"

namespace baikaldb {
int UnionNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    _node_type = pb::UNION_NODE;
    const pb::UnionNode& union_node = node.derive_node().union_node();
    _union_tuple_id = union_node.union_tuple_id();
    return 0;
}

int UnionNode::open(RuntimeState* state) {
    int ret = 0;
    auto client_conn = state->client_conn();
    if (client_conn == nullptr) {
        DB_WARNING("connection is nullptr: %lu", state->txn_id);
        return -1;
    }
    for (auto expr : _slot_order_exprs) {
        ret = expr->open();
        if (ret < 0) {
            DB_WARNING("Expr::open fail:%d", ret);
            return ret;
        }
    }
    _mem_row_desc = state->mem_row_desc();
    _mem_row_compare = std::make_shared<MemRowCompare>(_slot_order_exprs, _is_asc, _is_null_first);
    _sorter = std::make_shared<Sorter>(_mem_row_compare.get());
    for (size_t i = 0; i < _children.size(); i++) {
        if (state->execute_type == pb::EXEC_ARROW_ACERO) {
            // UnionNode和DualScanNode之间可能有FilterNode
            DualScanNode* dual_scan_node = static_cast<DualScanNode*>(_children[i]->get_node(pb::DUAL_SCAN_NODE));
            if (dual_scan_node == nullptr) {
                DB_WARNING("dual_scan_node is nullptr");
                return -1;
            }
            // 向量化union并发执行
            dual_scan_node->set_delay_fetcher_store(true);
        }
        ret = _children[i]->open(state);
        if (ret < 0) {
            DB_WARNING("ExecNode::open fail:%d", ret);
            return ret;
        }
        if (_children[i]->node_exec_type() == pb::EXEC_ARROW_ACERO) {
            set_node_exec_type(pb::EXEC_ARROW_ACERO);
        }
    }
    if (_node_exec_type == pb::EXEC_ARROW_ACERO) {
        return 0;
    }
    for (size_t i = 0; i < _children.size(); i++) {
        bool eos = false;
        do {
            std::shared_ptr<RowBatch> batch = std::make_shared<RowBatch>();
            ret = _children[i]->get_next(state, batch.get(), &eos);
            if (ret < 0) {
                DB_WARNING("children::get_next fail:%d", ret);
                return ret;
            }
            if (batch != nullptr && batch->size() > 0) {
                _sorter->add_batch(batch);
            }
        } while (!eos);
    }
    return 0;
}

int UnionNode::get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
    if (state->is_cancelled()) {
        DB_WARNING_STATE(state, "cancelled");
        *eos = true;
        return 0;
    }
    if (reached_limit()) {
        *eos = true;
        return 0;
    }
    int ret = 0;
    ret = _sorter->get_next(batch, eos);
    if (ret < 0) {
        DB_WARNING("sort get_next fail");
        return ret;
    }
    _num_rows_returned += batch->size();
    if (reached_limit()) {
        *eos = true;
        _num_rows_returned = _limit;
        return 0;
    }
    return 0;
}

bool UnionNode::can_use_arrow_vector() {
    for (auto expr : _slot_order_exprs) {
        if (!expr->can_use_arrow_vector()) {
            return false;
        }
    }
    for (auto& c : _children) {
        if (!c->can_use_arrow_vector()) {
            return false;
        }
    }
    return true;
}

int UnionNode::build_arrow_declaration(RuntimeState* state) {
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), OPEN_TRACE, nullptr);
    arrow::acero::Declaration union_node{"union", arrow::acero::ExecNodeOptions{}};
    for (int i = 0; i < _children.size(); ++i) {
        if (_children[i] == nullptr) {
            DB_WARNING("_children[%d] is nullptr", i);
            return -1;
        }
        if (_children[i]->build_arrow_declaration(state) != 0) {
            return -1;
        }
        union_node.inputs.emplace_back(arrow::acero::Declaration::Sequence(std::move(state->acero_declarations)));
        state->acero_declarations.clear();
    }
    LOCAL_TRACE_ARROW_PLAN(union_node);
    state->append_acero_declaration(union_node);
    // add SortNode
    if (!_slot_order_exprs.empty()) {
        std::vector<arrow::compute::SortKey> sort_keys;
        sort_keys.reserve(_slot_order_exprs.size());
        for (int i = 0; i < _slot_order_exprs.size(); ++i) {
            int ret = _slot_order_exprs[i]->transfer_to_arrow_expression();
            if (ret != 0 || _slot_order_exprs[i]->arrow_expr().field_ref() == nullptr) {
                DB_FATAL_STATE(state, "get sort field ref fail, maybe is not slot ref");
                return -1;
            }
            auto field_ref = _slot_order_exprs[i]->arrow_expr().field_ref();
            sort_keys.emplace_back(*field_ref, _is_asc[i] ? arrow::compute::SortOrder::Ascending : arrow::compute::SortOrder::Descending);
        }
        arrow::compute::Ordering ordering{sort_keys, 
                                      _is_asc[0] ? arrow::compute::NullPlacement::AtStart : arrow::compute::NullPlacement::AtEnd};
        arrow::acero::Declaration dec{"order_by", arrow::acero::OrderByNodeOptions{ordering}};
        LOCAL_TRACE_ARROW_PLAN(dec);
        state->append_acero_declaration(dec);
    }
    return 0;
}

int UnionNode::predicate_pushdown(std::vector<ExprNode*>& input_exprs) {
    // TODO - 不同子查询如果返回类型不一致，过滤可能有问题
    // UnionNode谓词会下推到多个子查询，每个子查询需要维护一份谓词，谓词指针不能共享
    std::vector<pb::Expr> input_pb_exprs;
    auto iter = input_exprs.begin();
    while (iter != input_exprs.end()) {
        std::unordered_set<int32_t> tuple_ids;
        (*iter)->get_all_tuple_ids(tuple_ids);
        if (tuple_ids.size() == 1 && *tuple_ids.begin() == _union_tuple_id) {
            pb::Expr pb_expr;
            ExprNode::create_pb_expr(&pb_expr, *iter);
            input_pb_exprs.emplace_back(pb_expr);
            delete *iter;
            iter = input_exprs.erase(iter);
            continue;
        }
        ++iter;
    }
    if (!input_exprs.empty()) {
        DB_WARNING("input_exprs should be empty");
        return -1;
    }
    for (int i = 0; i < _children.size(); ++i) {
        if (_children[i] == nullptr) {
            DB_WARNING("_children[%d] is nullptr", i);
            return -1;
        }
        std::vector<ExprNode*> input_exprs_tmp;
        ScopeGuard guard([&input_exprs_tmp] () {
            // 异常退出时，避免内存泄露
            for (auto* expr_node : input_exprs_tmp) {
                delete expr_node;
            }
        });
        for (const auto& pb_expr : input_pb_exprs) {
            ExprNode* expr_node = nullptr;
            int ret = ExprNode::create_tree(pb_expr, &expr_node);
            if (ret < 0) {
                DB_WARNING("Fail to create_tree");
                return -1;
            }
            input_exprs_tmp.emplace_back(expr_node);
        }
        if (_children[i]->predicate_pushdown(input_exprs_tmp) != 0) {
            DB_WARNING("fail to predicate push down");
            return -1;
        }
        if (!input_exprs_tmp.empty()) {
            DB_WARNING("input exprs should be empty");
            return -1;
        }
    }
    return 0;
}

}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
