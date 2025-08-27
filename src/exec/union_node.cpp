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
#include "query_context.h"
#include "dual_scan_node.h"
#include "arrow_exec_node_manager.h"

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

    for (auto& expr : union_node.slot_order_exprs()) {
        ExprNode* slot_order_expr = nullptr;
        ret = ExprNode::create_tree(expr, &slot_order_expr);
        if (ret < 0) {
            DB_FATAL("create slot order expr fail");
            ExprNode::destroy_tree(slot_order_expr);
            return ret;
        }
        _slot_order_exprs.push_back(slot_order_expr);
    }
    for (bool is_asc : union_node.is_asc()) {
        _is_asc.push_back(is_asc);
    }
    for (bool is_null_first : union_node.is_null_first()) {
        _is_null_first.push_back(is_null_first);
    }
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
            if (dual_scan_node != nullptr) {
                // 向量化union并发执行, mpp情况下可能没有dualscannode
                dual_scan_node->set_delay_fetcher_store(true);
            }
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

bool UnionNode::can_use_arrow_vector(RuntimeState* state) {
    for (auto expr : _slot_order_exprs) {
        if (!expr->can_use_arrow_vector()) {
            return false;
        }
    }
    for (auto& c : _children) {
        if (!c->can_use_arrow_vector(state)) {
            return false;
        }
    }
    state->vectorlized_parallel_execution = true;
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
        if (_limit < 0) {
            arrow::acero::Declaration dec{"order_by", arrow::acero::OrderByNodeOptions{ordering}};
            LOCAL_TRACE_ARROW_PLAN(dec);
            state->append_acero_declaration(dec);
        } else {
            arrow::acero::Declaration dec{"topk", TopKNodeOptions{ordering, _limit}};
            LOCAL_TRACE_ARROW_PLAN(dec);
            state->append_acero_declaration(dec);
        }
    }
    return 0;
}

int UnionNode::set_partition_property_and_schema(QueryContext* ctx) {
    if (0 != ExecNode::set_partition_property_and_schema(ctx)) {
        return -1;
    }
    _partition_property.set_single_partition();
    return 0;
}

void UnionNode::transfer_pb(int64_t region_id, pb::PlanNode* pb_node) {
    ExecNode::transfer_pb(region_id, pb_node);
    pb::DerivePlanNode* derive = pb_node->mutable_derive_node();
    pb::UnionNode* union_node = derive->mutable_union_node();
    union_node->clear_slot_order_exprs();
    union_node->clear_is_asc();
    union_node->clear_is_null_first();

    for (ExprNode* expr : _slot_order_exprs) {
        ExprNode::create_pb_expr(union_node->add_slot_order_exprs(), expr);
    }
    for (bool asc : _is_asc) {
        union_node->add_is_asc(asc);
    }
    for (bool null_first : _is_null_first) {
        union_node->add_is_null_first(null_first);
    }
    return;
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

int UnionNode::agg_pushdown(QueryContext* ctx, ExecNode* agg_node) {
    if (can_agg_pushdown()) {
        // 所有子节点都可以聚合下推时，UnionNode才进行聚合下推；
        // TODO - 后续可以考虑部分子节点聚合下推
        for (int i = 0; i < _children.size(); ++i) {
            if (_children[i] == nullptr) {
                DB_WARNING("_children[%d] is nullptr", i);
                return -1;
            }
            if (_children[i]->agg_pushdown(ctx, agg_node) != 0) {
                DB_WARNING("Fail to agg pushdown");
                return -1;
            }
        }
    } else {
        for (int i = 0; i < _children.size(); ++i) {
            if (_children[i] == nullptr) {
                DB_WARNING("_children[%d] is nullptr", i);
                return -1;
            }
            if (_children[i]->agg_pushdown(ctx, nullptr) != 0) {
                DB_WARNING("Fail to agg pushdown");
                return -1;
            }
        }
    }
    return 0;
}

int UnionNode::show_explain(QueryContext* ctx, std::vector<std::map<std::string, std::string>>& output, int& next_id, int display_id) {    
    // 第一个child的select_type 为 PRIMARY，其余为UNION
    std::vector<int> children_ids;
    for (auto it: _children) {
        DualScanNode* dual_scan_node = static_cast<DualScanNode*>(it->get_node(pb::DUAL_SCAN_NODE));
        children_ids.emplace_back(dual_scan_node->child_show_explain(ctx, output, next_id, -1));
    }

    for (auto& it: output) {
        if (it["id"] == std::to_string(children_ids[0])) {
            it["select_type"] = "PRIMARY";
        } else if (children_ids.end() != std::find(children_ids.begin(), children_ids.end(), strtoll(it["id"].c_str(), NULL, 10))) {
            it["select_type"] = "UNION";
        }
    }

    // 传入的 display_id 应该永远为-1
    display_id = (display_id != -1 ? display_id : (next_id++));
    std::map<std::string, std::string> explain_info = {
        {"id", std::to_string(display_id)},
        {"select_type", "UNION RESULT"},
        {"table", "<union>"},
        {"partitions", "NULL"},
        {"type", "NULL"},
        {"possible_keys", "NULL"},
        {"key", "NULL"},
        {"key_len", "NULL"},
        {"ref", "NULL"},
        {"rows", "NULL"},
        {"Extra", "Using temporary"},
        {"sort_index", "0"}
    };

    std::string derived_table_name = "<union";
    for (auto it: children_ids) {
        derived_table_name.append(std::to_string(it)).append(",");
    }
    derived_table_name.back() = '>';
    explain_info["table"] = derived_table_name;
    output.emplace_back(explain_info);
    return display_id;
}

}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
