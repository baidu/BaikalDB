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
#include "topn_sorter.h"
#include "runtime_state.h"
#include "query_context.h"
#include <arrow/acero/options.h>
#include "arrow_exec_node_manager.h"

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

bool SortNode::can_use_arrow_vector(RuntimeState* state) {
    // arrow sort目前排序所有列对于null的处理只能整体AtStart/AtEnd,不能配置单个列的规则
    if (!_monotonic) {
        return false;
    }
    // 只支持slot ref的order by
    for (auto& expr : _order_exprs) {
        if (!expr->can_use_arrow_vector()) {
            return false;
        }
    }
    for (auto& expr : _slot_order_exprs) {
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

int SortNode::build_arrow_declaration(RuntimeState* state) {
    START_LOCAL_TRACE_WITH_PARTITION_PROPERTY(get_trace(), state->get_trace_cost(), &_partition_property, OPEN_TRACE, nullptr);
    int ret = 0;
    for (auto c : _children) {
        ret = c->build_arrow_declaration(state);
        if (ret < 0) {
            return ret;
        }
    }
    if (state->sort_use_index()) {
        // 要求store不开acero并发
        return 0;
    }
    if (0 != build_sort_arrow_declaration(state, get_trace())) {
        return -1;
    }
    return 0;
}

int SortNode::build_sort_arrow_declaration(RuntimeState* state, pb::TraceNode* trace_node) {
    if (state->acero_declarations.size() > 0 
        && (state->acero_declarations.back().factory_name == "order_by"
                || state->acero_declarations.back().factory_name == "topk")) {
        return 0;
    }
    // sortNode提到最后在packetNode里处理: otherNode build -> sortNode build -> packetNode build -> handle limit(slice)
    // 解决如 a join b join c join d order by a.id,
    // a.id的sortNode可能推到最下面的join上, join1->sort->join2->join3, 后面的join会导致乱序
    START_LOCAL_TRACE(trace_node, state->get_trace_cost(), OPEN_TRACE, nullptr);
    std::vector<arrow::compute::SortKey> sort_keys;
    sort_keys.reserve(_slot_order_exprs.size());
    std::vector<arrow::compute::Expression> generate_projection_exprs;
    std::vector<std::string> generate_projection_exprs_names;

    int slot_id = 1;
    for (auto& expr : _order_exprs) {
        if (expr->node_type() != pb::SLOT_REF && expr->node_type() != pb::AGG_EXPR) {
            if (0 != expr->transfer_to_arrow_expression()) {
                DB_FATAL_STATE(state, "build order by expr fail");
                return -1;
            }
            // project
            generate_projection_exprs.emplace_back(expr->arrow_expr());
            std::string tmp_name = std::to_string(_tuple_id) + "_" + std::to_string(slot_id++);
            generate_projection_exprs_names.emplace_back(tmp_name);
        }
    }
    
    for (int i = 0; i < _slot_order_exprs.size(); ++i) {
        int ret = _slot_order_exprs[i]->transfer_to_arrow_expression();
        if (ret != 0 || _slot_order_exprs[i]->arrow_expr().field_ref() == nullptr) {
            DB_FATAL_STATE(state, "get arrow sort field ref fail, maybe is not slot ref");
            return -1;
        }
        auto field_ref = _slot_order_exprs[i]->arrow_expr().field_ref(); 
        sort_keys.emplace_back(*field_ref, _is_asc[i] ? arrow::compute::SortOrder::Ascending : arrow::compute::SortOrder::Descending);
    }
    if (generate_projection_exprs.size() > 0) {
        // projection
        for (auto& tuple : state->tuple_descs()) {
            if (tuple.tuple_id() == _tuple_id) {
                continue;
            }
            for (const auto& slot : tuple.slots()) {
                std::string name = std::to_string(tuple.tuple_id()) + "_" + std::to_string(slot.slot_id());
                generate_projection_exprs.emplace_back(arrow::compute::field_ref(name));
                generate_projection_exprs_names.emplace_back(name);
            }
        }
        arrow::acero::Declaration dec{"project", arrow::acero::ProjectNodeOptions{std::move(generate_projection_exprs), std::move(generate_projection_exprs_names)}};
        LOCAL_TRACE_ARROW_PLAN(dec);
        state->append_acero_declaration(dec);
    }
    // NullPlacement: Whether nulls and NaNs are placed at the start or at the end
    arrow::compute::NullPlacement null_placement = arrow::compute::NullPlacement::AtStart;
    if (_is_asc.size() > 0 && !_is_asc[0]) {
        null_placement = arrow::compute::NullPlacement::AtEnd;
    }
    arrow::compute::Ordering ordering{sort_keys, null_placement};
    if (_limit < 0) {
        // sort
        arrow::acero::Declaration dec{"order_by", arrow::acero::OrderByNodeOptions{ordering}};
        LOCAL_TRACE_ARROW_PLAN(dec);
        state->append_acero_declaration(dec);
    } else {
        arrow::acero::Declaration dec{"topk", TopKNodeOptions{ordering, _limit}};
        LOCAL_TRACE_ARROW_PLAN(dec);
        state->append_acero_declaration(dec);
    }
    return 0;
}

int SortNode::set_partition_property_and_schema(QueryContext* ctx) {
    if (0 != ExecNode::set_partition_property_and_schema(ctx)) {
        return -1;
    }
    if (_tuple_id > 0) {
         auto tuple_desc = ctx->get_tuple_desc(_tuple_id);
        if (tuple_desc == nullptr) {
            DB_WARNING("get tuple desc failed, tuple_id:%d", _tuple_id);
            return -1;
        }
        for (auto slot : tuple_desc->slots()) {
            _data_schema.insert(ColumnInfo{slot.tuple_id(), slot.slot_id(), slot.slot_type()});
        }
    }
    _partition_property.set_single_partition();
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
        if (_limit > 0) {
            _children[0]->set_limit(_limit);
        }
        return 0;
    }
    _mem_row_desc = state->mem_row_desc();
    _mem_row_compare = std::make_shared<MemRowCompare>(
            _slot_order_exprs, _is_asc, _is_null_first);
    if (_limit == -1) {
        _sorter = std::make_shared<Sorter>(_mem_row_compare.get());
    } else {
        _sorter = std::make_shared<TopNSorter>(_mem_row_compare.get(), _limit);
    }

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
        set_node_exec_type(_children[0]->node_exec_type());
        if (_node_exec_type == pb::EXEC_ARROW_ACERO) {
            return 0;
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
    set_node_exec_type(_children[0]->node_exec_type());
    if (_node_exec_type == pb::EXEC_ARROW_ACERO) {
        return 0;
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
