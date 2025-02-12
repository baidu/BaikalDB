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

#include "agg_node.h"
#include "dual_scan_node.h"
#include "filter_node.h"
#include "limit_node.h"
#include "query_context.h"
#include "union_node.h"
#include "arrow_io_excutor.h"
#include "arrow_function.h"

namespace baikaldb {

DEFINE_bool(union_predicate_pushdown, true, "union predicate push_down");

int DualScanNode::open(RuntimeState* state) {
    if (!_has_subquery) {
        return 0;
    }
    if (state->is_explain && state->explain_type == EXPLAIN_NULL) {
        return 0;
    }
    int ret = 0;
    for (auto expr : _derived_table_projections) {
        ret = expr->open();
        if (ret < 0) {
            DB_WARNING("Expr::open fail:%d", ret);
            return ret;
        }
    }
    if (_sub_query_node == nullptr) {
        DB_WARNING("_sub_query_node is nullptr");
        return -1;
    }
    if (_sub_query_runtime_state == nullptr) {
        DB_WARNING("_sub_query_runtime_state is nullptr");
        return -1;
    }
    ExecNode* join = _sub_query_node->get_node(pb::JOIN_NODE);
    _sub_query_runtime_state->is_simple_select = (join == nullptr);
    _sub_query_runtime_state->execute_type = state->execute_type;
    _sub_query_runtime_state->sign_exec_type = state->sign_exec_type;
    _sub_query_node->set_delay_fetcher_store(_delay_fetcher_store);
    ret = _sub_query_node->open(_sub_query_runtime_state);
    if (ret < 0) {
        DB_WARNING("Fail to open _sub_query_node");
        return ret;
    }
    set_node_exec_type(_sub_query_node->node_exec_type());
    if (!_sub_query_runtime_state->vectorlized_parallel_execution) {
        state->vectorlized_parallel_execution = false;
    }
    return 0;
}

int DualScanNode::get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
    if (!_has_subquery) {
        std::unique_ptr<MemRow> row = state->mem_row_desc()->fetch_mem_row();
        batch->move_row(std::move(row));
        ++_num_rows_returned;
        *eos = true;
        return 0;
    }
    if (state->is_explain && state->explain_type == EXPLAIN_NULL) {
        *eos = true;
        return 0;
    }
    const int32_t tuple_id = slot_tuple_id();
    pb::TupleDescriptor* tuple_desc = state->get_tuple_desc(tuple_id);
    if (tuple_desc == nullptr) {
        DB_WARNING("tuple_desc is nullptr, slot_tuple_id: %d", tuple_id);
        return -1;
    }
    MemRowDescriptor* mem_row_desc = state->mem_row_desc();
    if (mem_row_desc == nullptr) {
        DB_WARNING("mem_row_desc is nullptr");
        return -1;
    }
    int ret = 0;
    do {
        RowBatch derived_batch;
        ret = _sub_query_node->get_next(_sub_query_runtime_state, &derived_batch, eos);
        if (ret < 0) {
            DB_WARNING("children::get_next fail:%d", ret);
            return ret;
        }
        set_node_exec_type(_sub_query_node->node_exec_type());
        if (_node_exec_type == pb::EXEC_ARROW_ACERO) {
            break;
        }
        for (derived_batch.reset(); !derived_batch.is_traverse_over(); derived_batch.next()) {
            MemRow* inner_row = derived_batch.get_row().get();
            std::unique_ptr<MemRow> outer_row = mem_row_desc->fetch_mem_row();
            for (auto iter : _slot_column_mapping) {
                int32_t outer_slot_id = iter.first;
                int32_t inter_column_id = iter.second;
                auto expr = _derived_table_projections[inter_column_id];
                ExprValue result = expr->get_value(inner_row).cast_to(expr->col_type());
                auto slot = tuple_desc->slots(outer_slot_id - 1);
                outer_row->set_value(slot.tuple_id(), slot.slot_id(), result);
            }
            batch->move_row(std::move(outer_row));
        }
    } while (!*eos);

    // 更新子查询信息到外层的state
    state->inc_num_returned_rows(_sub_query_runtime_state->num_returned_rows());
    state->inc_num_affected_rows(_sub_query_runtime_state->num_affected_rows());
    state->inc_num_scan_rows(_sub_query_runtime_state->num_scan_rows());
    state->inc_num_filter_rows(_sub_query_runtime_state->num_filter_rows());
    state->region_count += _sub_query_runtime_state->region_count;

    return 0;
}

void DualScanNode::get_all_dual_scan_node(std::vector<ExecNode*>& exec_nodes) {
    if (!_has_subquery) {
        return;
    }
    exec_nodes.emplace_back(this);
    if (_sub_query_node != nullptr) {
        _sub_query_node->get_all_dual_scan_node(exec_nodes);
    }
}

int DualScanNode::predicate_pushdown(std::vector<ExprNode*>& input_exprs) {
    if (_has_subquery) {
        if (_sub_query_node == nullptr) {
            DB_WARNING("_sub_query_node is nullptr");
            return -1;
        }
        if (can_predicate_pushdown()) {
            const int32_t tuple_id = slot_tuple_id();
            std::vector<ExprNode*> subquery_where_input_exprs;
            auto iter = input_exprs.begin();
            while (iter != input_exprs.end()) {
                std::unordered_set<int32_t> tuple_ids;
                (*iter)->get_all_tuple_ids(tuple_ids);
                if (tuple_ids.size() == 1 && *tuple_ids.begin() == tuple_id) {
                    // 判断自身是否有agg
                    bool has_agg = (*iter)->has_agg();
                    if (!has_agg) {
                        // 判断slot_ref替换后的expr是否有agg
                        if ((*iter)->has_agg_projection(_slot_column_mapping, _derived_table_projections_agg_vec, has_agg) != 0) {
                            DB_WARNING("Fail to check has_agg_projection");
                            return -1;
                        }
                    }
                    if (!has_agg) {
                        (*iter)->replace_slot_ref_to_expr(tuple_id, _slot_column_mapping, _derived_table_projections);
                        subquery_where_input_exprs.emplace_back(*iter);
                        iter = input_exprs.erase(iter);
                        continue;
                    }
                }
                ++iter;
            }
            _sub_query_node->predicate_pushdown(subquery_where_input_exprs);
        } else {
            std::vector<ExprNode*> empty_exprs;
            _sub_query_node->predicate_pushdown(empty_exprs);
        }
    }
    if (_parent != nullptr && 
            (_parent->node_type() == pb::WHERE_FILTER_NODE || _parent->node_type() == pb::TABLE_FILTER_NODE)) {
        return 0;
    }
    if (input_exprs.size() > 0) {
        add_filter_node(input_exprs);
        input_exprs.clear();
    }
    return 0;
}

bool DualScanNode::can_predicate_pushdown() {
    if (_sub_query_node == nullptr) {
        return false;
    }
    // 后续开放UNION可下推时，需要注意UNION的tuple_descs
    if (!FLAGS_union_predicate_pushdown) {
        UnionNode* union_node = static_cast<UnionNode*>(_sub_query_node->get_node(pb::UNION_NODE));
        if (union_node != nullptr) {
            return false;
        }
    }
    LimitNode* limit_node = static_cast<LimitNode*>(_sub_query_node->get_node(pb::LIMIT_NODE));
    if (limit_node != nullptr) {
        return false;
    }
    return true;
}

bool DualScanNode::can_use_arrow_vector() {
    if (_sub_query_node != nullptr) {
        if (!_sub_query_node->can_use_arrow_vector()) {
            return false;
        }
        for (auto expr : _derived_table_projections) {
            if (!expr->can_use_arrow_vector()) {
                return false;
            }
        }
    }
    for (auto& c : _children) {
        if (!c->can_use_arrow_vector()) {
            return false;
        }
    }
    return true;
}

int DualScanNode::build_arrow_declaration(RuntimeState* state) {
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), OPEN_TRACE, nullptr);
    if (_sub_query_node == nullptr) {
        return 0;
    }
    if (state->is_explain && state->explain_type == EXPLAIN_NULL) {
        return 0;
    }
    if (state->vectorlized_parallel_execution == false) {
        _sub_query_runtime_state->vectorlized_parallel_execution = false;
    }
    // 跳过subquery的packetnode
    if (_sub_query_node->children_size() == 0) {
        DB_WARNING("subquery has no children");
        return -1;
    }
    if (0 != _sub_query_node->children(0)->build_arrow_declaration(_sub_query_runtime_state)) {
        return -1;
    }
    LimitNode* limit_node = static_cast<LimitNode*>(_sub_query_node->vec_get_limit_node());
    if (limit_node != nullptr) {
        // 有limit先执行计算拿到结果，进行slice往上吐
        // [ARROW TODO] 支持unordered fetchnode
        arrow::Result<std::shared_ptr<arrow::Table>> final_table;
        GlobalArrowExecutor::execute(_sub_query_runtime_state, &final_table);
        if (final_table.ok()) {
            _intermediate_table = *final_table;
        } else {
            DB_FATAL("arrow acero run fail, status: %s", final_table.status().ToString().c_str());
            return -1;
        }
        if (limit_node->get_limit() > 0) {
            if (limit_node->get_offset() >= _intermediate_table->num_rows()) {
                _intermediate_table = _intermediate_table->Slice(0, 0);
            } else {
                _intermediate_table = _intermediate_table->Slice(limit_node->get_offset(), limit_node->get_limit());
            }
        }
        _sub_query_runtime_state->acero_declarations.clear();
        auto vectorized_reader = std::make_shared<arrow::TableBatchReader>(_intermediate_table);
        std::function<arrow::Iterator<std::shared_ptr<arrow::RecordBatch>>()> iter_maker = [vectorized_reader] () {
            arrow::Iterator<std::shared_ptr<arrow::RecordBatch>> batch_it = arrow::MakeIteratorFromReader(vectorized_reader);
            return batch_it;
        };
        arrow::acero::Declaration dec = arrow::acero::Declaration{"record_batch_source",
            arrow::acero::RecordBatchSourceNodeOptions{_intermediate_table->schema(), std::move(iter_maker)}};
        _sub_query_runtime_state->append_acero_declaration(dec);
        LOCAL_TRACE_ARROW_PLAN_WITH_SCHEMA(dec, _intermediate_table->schema(), nullptr);
    }
    state->acero_declarations.insert(state->acero_declarations.end(), 
            _sub_query_runtime_state->acero_declarations.begin(), 
            _sub_query_runtime_state->acero_declarations.end());
    _sub_query_runtime_state->acero_declarations.clear();
    if (!_sub_query_runtime_state->vectorlized_parallel_execution) {
        state->vectorlized_parallel_execution = false;
    }
    const int32_t tuple_id = slot_tuple_id();
    pb::TupleDescriptor* tuple_desc = state->get_tuple_desc(tuple_id);
    if (tuple_desc == nullptr) {
        return -1;
    }
    // add projectnode for schema transfer(inner subquery->outer)
    std::vector<arrow::compute::Expression> exprs;
    std::vector<std::string> names;
    for (auto iter : _slot_column_mapping) {
        int32_t outer_slot_id = iter.first;
        int32_t inter_column_id = iter.second;
        if (0 != _derived_table_projections[inter_column_id]->transfer_to_arrow_expression()) {
            DB_FATAL_STATE(state, "dual_scan_node projection transfer_to_arrow_expression fail");
            return -1;
        }
        auto slot = tuple_desc->slots(outer_slot_id - 1);
        if (is_union_subquery()) {
            // union每个子查询的列需要转化成tuple对应slot的类型
            if (0 != build_arrow_expr_with_cast(_derived_table_projections[inter_column_id], slot.slot_type(), true)) {
                DB_FATAL_STATE(state, "projection transfer_to_arrow_expression fail");
                return -1;
            }
        }
        exprs.emplace_back(_derived_table_projections[inter_column_id]->arrow_expr());
        names.emplace_back(std::to_string(slot.tuple_id()) + "_" + std::to_string(slot.slot_id()));
    }
    arrow::acero::Declaration dec{"project", arrow::acero::ProjectNodeOptions{exprs, names}};
    LOCAL_TRACE_ARROW_PLAN(dec);
    state->append_acero_declaration(dec);
    return 0;
}

} // namespace baikaldb
