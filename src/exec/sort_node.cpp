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

#include "sort_node.h"
#include "runtime_state.h"
#include "query_context.h"

namespace baikaldb {
int SortNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    for (auto& expr : node.derive_node().sort_node().order_exprs()) {
        ExprNode* order_expr = nullptr;
        ret = ExprNode::create_tree(expr, &order_expr);
        if (ret < 0) {
            //如何释放资源
            return ret;
        }
        _order_exprs.push_back(order_expr);
    }
    _tuple_id = node.derive_node().sort_node().tuple_id();
    int slot_id = 1;
    for (auto& expr : node.derive_node().sort_node().order_exprs()) {
        ExprNode* order_expr = nullptr;
        if (expr.nodes(0).node_type() != pb::SLOT_REF && expr.nodes(0).node_type() != pb::AGG_EXPR) {
            //create slot ref
            pb::Expr slot_expr;
            pb::ExprNode* node = slot_expr.add_nodes(); 
            node->set_node_type(pb::SLOT_REF);
            node->set_col_type(expr.nodes(0).col_type());
            node->set_num_children(0);
            node->mutable_derive_node()->set_tuple_id(_tuple_id);
            node->mutable_derive_node()->set_slot_id(slot_id++);
            ret = ExprNode::create_tree(slot_expr, &order_expr);
            if (ret < 0) {
                //如何释放资源
                return ret;
            }
        } else {
            ret = ExprNode::create_tree(expr, &order_expr);
            if (ret < 0) {
                //如何释放资源
                return ret;
            }
        }
        _slot_order_exprs.push_back(order_expr);
    }
    for (auto asc : node.derive_node().sort_node().is_asc()) {
        _is_asc.push_back(asc);
    }
    for (auto null_first : node.derive_node().sort_node().is_null_first()) {
        _is_null_first.push_back(null_first);
    }
    for (size_t idx = 1; idx < _is_asc.size(); ++idx) {
        if (_is_asc[idx] != _is_asc[0]) {
            _monotonic = false;
        }
    }
    return 0;
}

int SortNode::expr_optimize(QueryContext* ctx) {
    int ret = 0;
    ret = ExecNode::expr_optimize(ctx);
    if (ret < 0) {
        return ret;
    }
    pb::TupleDescriptor* tuple_desc = nullptr;
    if (_tuple_id >= 0) {
        tuple_desc =  ctx->get_tuple_desc(_tuple_id);
    }
    int slot_idx = 0;
    int idx = 0;
    for (auto expr : _order_exprs) {
        //类型推导
        ret = expr->expr_optimize();
        if (ret < 0) {
            return ret;
        }
        if (expr->node_type() != pb::SLOT_REF && expr->node_type() != pb::AGG_EXPR) {
            if (tuple_desc == nullptr) {
                DB_WARNING("tuple_desc is null, but have !SLOT_REF !AGG_EXPR");
                return -1;
            }
            if (slot_idx < tuple_desc->slots_size()) {
                tuple_desc->mutable_slots(slot_idx++)->set_slot_type(expr->col_type());
                _slot_order_exprs[idx]->set_col_type(expr->col_type());
            } else {
                DB_WARNING("slot_idx:%d < slot_size:%d", slot_idx, tuple_desc->slots_size());
            }
        }
        // union中数据都放到tuple:0里了，函数也额外放到tuple:0占了新slot
        // 目前union中的order by不支持函数
        if (_tuple_id == 0 && tuple_desc != nullptr && expr->node_type() == pb::SLOT_REF) {
            for (int i = 0; i < tuple_desc->slots_size(); i++) {
                auto& slot = tuple_desc->slots(i);
                if (slot.slot_id() == expr->slot_id()) {
                    expr->set_col_type(slot.slot_type());
                    _slot_order_exprs[idx]->set_col_type(expr->col_type());
                }
            }
        }
        idx++;
    }
    for (auto expr : _slot_order_exprs) {
        if (expr->node_type() == pb::AGG_EXPR) {
            ret = expr->expr_optimize();
            if (ret < 0) {
                return ret;
            }
        }
    }
    return 0;
}

int SortNode::open(RuntimeState* state) {
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), OPEN_TRACE, nullptr);
    int ret = 0;
    ret = ExecNode::open(state);
    if (ret < 0) {
        DB_WARNING_STATE(state, "ExecNode::open fail, ret:%d", ret);
        return ret;
    }
    for (auto expr : _order_exprs) {
        ret = expr->open();
        if (ret < 0) {
            DB_WARNING_STATE(state, "expr open fail, ret:%d", ret);
            return ret;
        }
    }
    for (auto expr : _slot_order_exprs) {
        ret = expr->open();
        if (ret < 0) {
            DB_WARNING_STATE(state, "expr open fail, ret:%d", ret);
            return ret;
        }
    }
    if (state->sort_use_index()) {
        //DB_WARNING_STATE(state, "sort use index, limit:%ld", _limit);
        return 0;
    }
    _mem_row_desc = state->mem_row_desc();
    _mem_row_compare = std::make_shared<MemRowCompare>(
            _slot_order_exprs, _is_asc, _is_null_first);
    _sorter = std::make_shared<Sorter>(_mem_row_compare.get());

    bool eos = false;
    int count = 0;
    do {
        if (state->is_cancelled()) {
            DB_WARNING_STATE(state, "cancelled");
            return 0;
        }
        std::shared_ptr<RowBatch> batch = std::make_shared<RowBatch>();
        ret = _children[0]->get_next(state, batch.get(), &eos);
        if (ret < 0) {
            DB_WARNING_STATE(state, "child->get_next fail, ret:%d", ret);
            return ret;
        }
        //照理不会出现拿到0行数据
        if (batch->size() == 0) {
            break;
        }
        count += batch->size();
        fill_tuple(batch.get());
        _sorter->add_batch(batch);
    } while (!eos);
    //DB_WARNING_STATE(state, "sort_size:%d", count);
    TimeCost sort_time;
    _sorter->sort();
    LOCAL_TRACE_DESC <<  "sort time cost:" << sort_time.get_time() << " rows:" << count;  
    return 0;
}

int SortNode::get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), GET_NEXT_TRACE, ([this](TraceLocalNode& local_node) {
        local_node.set_affect_rows(_num_rows_returned);
    }));
    
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
    TimeCost cost;
    if (state->sort_use_index()) {
        ret = _children[0]->get_next(state, batch, eos);
    } else {
        ret = _sorter->get_next(batch, eos);
    }
    if (ret < 0) {
        DB_WARNING_STATE(state, "_sorter->get_next fail, ret:%d", ret);
        return ret;
    }
    _num_rows_returned += batch->size();
    if (reached_limit()) {
        *eos = true;
        batch->keep_first_rows(batch->size() - (_num_rows_returned - _limit));
        _num_rows_returned = _limit;
        return 0;
    }
    return 0;
}

void SortNode::close(RuntimeState* state) {
    ExecNode::close(state);
    for (auto expr : _order_exprs) {
        expr->close();
    }
    for (auto expr : _slot_order_exprs) {
        expr->close();
    }
    _sorter = nullptr;
}

int SortNode::fill_tuple(RowBatch* batch) {
    if (_tuple_id < 0) {
        return 0;
    }
    //后续还要修改
    for (batch->reset(); !batch->is_traverse_over(); batch->next()) {
        MemRow* row = batch->get_row().get();
        int slot_id = 1;
        for (auto expr : _order_exprs) {
            if (expr->node_type() != pb::SLOT_REF && expr->node_type() != pb::AGG_EXPR) {
                row->set_value(_tuple_id, slot_id++, expr->get_value(row));
            }
        }
    }
    batch->reset();
    return 0;
}

void SortNode::find_place_holder(std::unordered_multimap<int, ExprNode*>& placeholders) {
    ExecNode::find_place_holder(placeholders);
    for (auto& expr : _order_exprs) {
        expr->find_place_holder(placeholders);
    }
    for (auto& expr : _slot_order_exprs) {
        expr->find_place_holder(placeholders);
    }
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
