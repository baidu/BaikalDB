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

#include "join_node.h"
#include "filter_node.h"
#include "full_export_node.h"
#include "expr_node.h"
#include "rocksdb_scan_node.h"
#include "scalar_fn_call.h"
#include "index_selector.h"
#include "plan_router.h"
#include "logical_planner.h"
#include "literal.h"
#include "vectorize_helpper.h"
#include <arrow/compute/cast.h>

namespace baikaldb {
int JoinNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = Joiner::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    } 
    const pb::JoinNode& join_node = node.derive_node().join_node();
    _join_type = join_node.join_type();
    
    for (auto& expr : join_node.conditions()) {
        ExprNode* condition = NULL;
        ret = ExprNode::create_tree(expr, &condition);
        if (ret < 0) {
            //如何释放资源
            return ret;
        }
        _conditions.push_back(condition);
    }
    for (auto& tuple_id : join_node.left_tuple_ids()) {
        _left_tuple_ids.insert(tuple_id); 
    }
    for (auto& tuple_id : join_node.right_tuple_ids()) {
        _right_tuple_ids.insert(tuple_id);
    }
    return 0;
}

int JoinNode::predicate_pushdown(std::vector<ExprNode*>& input_exprs) {
    //DB_WARNING("node:%ld is pushdown", this);
    convert_to_inner_join(input_exprs);
    if (_join_type == pb::FULL_JOIN) {
        return 0;
    }

    std::vector<ExprNode*> outer_push_exprs;
    std::vector<ExprNode*> inner_push_exprs;
    std::vector<ExprNode*> correlate_exprs;
    auto iter = input_exprs.begin(); 
    while (iter != input_exprs.end()) {
        std::unordered_set<int32_t> related_tuple_ids;
        (*iter)->get_all_tuple_ids(related_tuple_ids);
        if (related_tuple_ids.size() > 0 && !contains_expr(*iter)) {
            correlate_exprs.emplace_back(*iter);
            iter = input_exprs.erase(iter);
            continue;
        }
        ++iter;
    }
    if (_join_type == pb::INNER_JOIN) {
        auto iter = _conditions.begin(); 
        while (iter != _conditions.end()) {
            if (outer_contains_expr(*iter)) {
                outer_push_exprs.push_back(*iter);
                iter = _conditions.erase(iter);
                continue;
            }
            if (inner_contains_expr(*iter)) {
                inner_push_exprs.push_back(*iter);
                iter = _conditions.erase(iter);
                continue;
            }
            ++iter;
        }
        for (auto& expr : input_exprs) {
            if (outer_contains_expr(expr)) {
                outer_push_exprs.push_back(expr);
                continue;
            } 
            if (inner_contains_expr(expr)) {
                inner_push_exprs.push_back(expr);
                continue;
            }
            _conditions.push_back(expr);
        }
        input_exprs.clear();
        input_exprs = correlate_exprs;
    }
    if (_join_type == pb::LEFT_JOIN || _join_type == pb::RIGHT_JOIN) {
        auto iter = input_exprs.begin();
        while (iter != input_exprs.end()) {
            if (outer_contains_expr(*iter)) {
                outer_push_exprs.push_back(*iter);
                iter = input_exprs.erase(iter);
                continue;
            }
            ++iter;
        }
        iter = _conditions.begin();
        while (iter != _conditions.end()) {
            if (inner_contains_expr(*iter)) {
                inner_push_exprs.push_back(*iter);
                iter = _conditions.erase(iter);
                continue;
            }
            ++iter;
        }
    }    
    _outer_node->predicate_pushdown(outer_push_exprs);
    if (outer_push_exprs.size() > 0) {
        _outer_node->add_filter_node(outer_push_exprs);
    }
    _inner_node->predicate_pushdown(inner_push_exprs);
    if (inner_push_exprs.size() > 0) {
        _inner_node->add_filter_node(inner_push_exprs);
    }
    return 0;
}
void JoinNode::convert_to_inner_join(std::vector<ExprNode*>& input_exprs) {
    //inner_join默认情况下都是左边是驱动表, left_join只能左边做驱动表
    //outer_node 和 inner_node在做完seperate之后会变化，所以在join node open的时候要重新赋值一次
    _outer_node = _children[0];
    _inner_node = _children[1];
    _outer_tuple_ids = _left_tuple_ids;
    _inner_tuple_ids = _right_tuple_ids;

    if (_join_type == pb::RIGHT_JOIN) {
        _outer_node = _children[1];
        _inner_node = _children[0];
        _outer_tuple_ids = _right_tuple_ids;
        _inner_tuple_ids = _left_tuple_ids;
    }

    std::vector<ExprNode*> full_exprs = input_exprs;
    for (auto& expr : _conditions) {
        full_exprs.push_back(expr);
    }
    if (_inner_node->node_type() == pb::JOIN_NODE) {
        ((JoinNode*)_inner_node)->convert_to_inner_join(full_exprs);
    }
    if (_outer_node->node_type() == pb::JOIN_NODE) {
        ((JoinNode*)_outer_node)->convert_to_inner_join(full_exprs);
    }
    if (_join_type == pb::INNER_JOIN) {
        return;
    }
    if (_join_type == pb::FULL_JOIN) {
        bool outer_has_not_null_expr = false;
        bool inner_has_not_null_expr = false;
        for (auto& expr : input_exprs) {
            bool is_expr_contains_null_function = expr->contains_null_function();
            bool is_outer_contains_expr = outer_contains_expr(expr);
            bool is_inner_contains_expr = inner_contains_expr(expr);
            if (is_outer_contains_expr && !is_expr_contains_null_function) {
                outer_has_not_null_expr = true;
            } else if (is_inner_contains_expr && !is_expr_contains_null_function) {
                inner_has_not_null_expr = true;
            } else if (!is_outer_contains_expr 
                        && !is_inner_contains_expr
                        && contains_expr(expr) 
                        && !is_expr_contains_null_function 
                        && !expr->contains_special_operator(pb::OR_PREDICATE)) {
                inner_has_not_null_expr = true;
                outer_has_not_null_expr = true;
                break;
            }
        }
        if (outer_has_not_null_expr && inner_has_not_null_expr) {
            set_join_type(pb::INNER_JOIN);
        } else if (outer_has_not_null_expr) {
            set_join_type(pb::LEFT_JOIN);
        } else if (inner_has_not_null_expr) {
            set_join_type(pb::RIGHT_JOIN);
            _outer_node = _children[1];
            _inner_node = _children[0];
            _outer_tuple_ids = _right_tuple_ids;
            _inner_tuple_ids = _left_tuple_ids;
        }
        return;
    }
    
    for (auto& expr : input_exprs) {
        if (outer_contains_expr(expr)) {
            continue;
        }
        if (inner_contains_expr(expr)
                && !expr->contains_null_function()) {
            set_join_type(pb::INNER_JOIN);
            return;
        }
        if (contains_expr(expr) && !expr->contains_null_function()
                && !expr->contains_special_operator(pb::OR_PREDICATE)) {
            set_join_type(pb::INNER_JOIN);
            return;
        }
    }
}

void JoinNode::transfer_pb(int64_t region_id, pb::PlanNode* pb_node) {
    ExecNode::transfer_pb(region_id, pb_node);
    auto join_node = pb_node->mutable_derive_node()->mutable_join_node();
    join_node->set_join_type(_join_type);
    join_node->clear_conditions();
    for (auto expr : _conditions) {
       ExprNode::create_pb_expr(join_node->add_conditions(), expr);
    }
}

bool JoinNode::can_use_arrow_vector() {
    if (_outer_equal_slot.size() == 0 
            || _outer_equal_slot.size() != _inner_equal_slot.size()) {
        // 必须有等值条件
        return false;
    }
    for (int i = 0; i < _outer_equal_slot.size(); ++i) {
        if (_use_index_join && is_double(_outer_equal_slot[i]->col_type())) {
            // 暂不支持浮点数等值条件
            return false;
        }
    }
    for (auto& expr : _conditions) {
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

int JoinNode::build_table_arrow_declaration(RuntimeState* state, 
                                            arrow::acero::Declaration& dec,
                                            ExecNode* node, 
                                            std::unordered_set<int32_t>& tuple_ids, 
                                            std::vector<MemRow*>& mem_rows,
                                            std::shared_ptr<arrow::Table>& intermediate_table,
                                            const std::unordered_map<int32_t, std::set<int32_t>>& cast_string_slot_ids) {
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), OPEN_TRACE, nullptr);
    if (node->node_exec_type() == pb::EXEC_ROW) {
        // 返回行, 行转source node
        if (tuple_ids.size() == 0) {
            DB_FATAL_STATE(state, "tuple ids is empty");
            return 0;
        }
        std::shared_ptr<RowVectorizedReader> vectorized_reader = std::make_shared<RowVectorizedReader>();
        if (0 != vectorized_reader->init(state, &mem_rows, tuple_ids)) {
            return -1;
        } 
        std::function<arrow::Iterator<std::shared_ptr<arrow::RecordBatch>>()> iter_maker = [vectorized_reader] () {
            arrow::Iterator<std::shared_ptr<arrow::RecordBatch>> batch_it = arrow::MakeIteratorFromReader(vectorized_reader);
            return batch_it;
        };
        dec = arrow::acero::Declaration{"record_batch_source",
            arrow::acero::RecordBatchSourceNodeOptions{vectorized_reader->schema(), std::move(iter_maker)}}; 
        state->append_acero_declaration(dec);
        LOCAL_TRACE_ARROW_PLAN_WITH_SCHEMA(dec, vectorized_reader->schema(), nullptr);
    } else {
        if (intermediate_table != nullptr) {
            // 中间结果arrow table转source node
            auto vectorized_reader = std::make_shared<arrow::TableBatchReader>(intermediate_table);
            std::function<arrow::Iterator<std::shared_ptr<arrow::RecordBatch>>()> iter_maker = [vectorized_reader] () {
                arrow::Iterator<std::shared_ptr<arrow::RecordBatch>> batch_it = arrow::MakeIteratorFromReader(vectorized_reader);
                return batch_it;
            };
            dec = arrow::acero::Declaration{"record_batch_source",
                arrow::acero::RecordBatchSourceNodeOptions{intermediate_table->schema(), std::move(iter_maker)}};
            state->append_acero_declaration(dec);
            LOCAL_TRACE_ARROW_PLAN_WITH_SCHEMA(dec, intermediate_table->schema(), nullptr);
        } else {
            if (node->build_arrow_declaration(state) != 0) {
                DB_FATAL_STATE(state, "outer join node build arrow declaration failed");
                return -1;
            }
        }
    }
    if (cast_string_slot_ids.size() > 0) {
        std::vector<arrow::compute::Expression> exprs;
        std::vector<std::string> names;
        for (auto tuple_id : tuple_ids) {
            auto tuple = state->get_tuple_desc(tuple_id);
            if (tuple == nullptr) {
                DB_FATAL_STATE(state, "get tuple desc failed");
                return -1;
            }
            for (auto& slot : tuple->slots()) {
                std::string name = std::to_string(tuple_id) + "_" + std::to_string(slot.slot_id());
                exprs.emplace_back(arrow::compute::field_ref(name));
                names.emplace_back(name);
                auto iter = cast_string_slot_ids.find(tuple_id);
                if (iter != cast_string_slot_ids.end() && iter->second.find(slot.slot_id()) != iter->second.end()) {
                    // 额外加cast string列
                    exprs.emplace_back(arrow::compute::call("cast", {arrow::compute::field_ref(name)}, 
                                                            arrow::compute::CastOptions::Unsafe(arrow::large_binary())));
                    names.emplace_back(name + "_cast");
                }
            }
        }
        arrow::acero::Declaration dec{"project", arrow::acero::ProjectNodeOptions{exprs, names}};
        LOCAL_TRACE_ARROW_PLAN(dec);
        state->append_acero_declaration(dec);
    }
    dec = arrow::acero::Declaration::Sequence(state->acero_declarations);
    state->acero_declarations.clear();
    return 0;
}

int JoinNode::build_arrow_declaration(RuntimeState* state) {
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), OPEN_TRACE, nullptr);
    std::unordered_map<int32_t, std::set<int32_t>> outer_cast_slot_ids;
    std::unordered_map<int32_t, std::set<int32_t>> inner_cast_slot_ids;
    for (int i = 0; i < _outer_equal_slot.size(); ++i) {
        if (_outer_equal_slot[i]->col_type() != _inner_equal_slot[i]->col_type()) {
            outer_cast_slot_ids[_outer_equal_slot[i]->tuple_id()].insert(_outer_equal_slot[i]->slot_id());
            inner_cast_slot_ids[_inner_equal_slot[i]->tuple_id()].insert(_inner_equal_slot[i]->slot_id());
        }
    }

    arrow::acero::Declaration outer_dec;
    arrow::acero::Declaration inner_dec;
    if (0 != build_table_arrow_declaration(state, outer_dec, _outer_node, _outer_tuple_ids, _outer_tuple_data, _outer_intermediate_join_result_table, outer_cast_slot_ids)) {
        DB_FATAL_STATE(state, "outer join node build arrow declaration failed");
        return -1;
    }
    if (0 != build_table_arrow_declaration(state, inner_dec, _inner_node, _inner_tuple_ids, _inner_tuple_data, _inner_intermediate_join_result_table, inner_cast_slot_ids)) {
        DB_FATAL_STATE(state, "inner join node build arrow declaration failed");
        return -1;
    }

    arrow::acero::JoinType join_type;
    std::vector<arrow::FieldRef> outer_keys;
    std::vector<arrow::FieldRef> inner_keys;
    for (auto slot_ref : _outer_equal_slot) {
        int ret = slot_ref->transfer_to_arrow_expression();
        if (ret < 0) {
            DB_FATAL_STATE(state, "expr transfer arrow fail, ret:%d", ret);
            return ret;
        }
        std::string name = std::to_string(slot_ref->tuple_id()) + "_" + std::to_string(slot_ref->slot_id());
        auto iter = outer_cast_slot_ids.find(slot_ref->tuple_id());
        if (iter != outer_cast_slot_ids.end() && iter->second.find(slot_ref->slot_id()) != iter->second.end()) {
            outer_keys.emplace_back(arrow::FieldRef(name + "_cast"));
        } else {
            outer_keys.emplace_back(arrow::FieldRef(name));
        }
    }
    for (auto slot_ref : _inner_equal_slot) {
        int ret = slot_ref->transfer_to_arrow_expression();
        if (ret < 0) {
            DB_FATAL_STATE(state, "expr transfer arrow fail, ret:%d", ret);
            return ret;
        }
        std::string name = std::to_string(slot_ref->tuple_id()) + "_" + std::to_string(slot_ref->slot_id());
        auto iter = inner_cast_slot_ids.find(slot_ref->tuple_id());
        if (iter != inner_cast_slot_ids.end() && iter->second.find(slot_ref->slot_id()) != iter->second.end()) {
            inner_keys.emplace_back(arrow::FieldRef(name + "_cast"));
        } else {
            inner_keys.emplace_back(arrow::FieldRef(name));
        }
    }
    std::vector<arrow::compute::Expression> sub_exprs;
    for (auto& condition : _conditions) {
        int ret = condition->transfer_to_arrow_expression();
        if (ret < 0) {
            DB_FATAL_STATE(state, "expr transfer arrow fail, ret:%d", ret);
            return ret;
        }
        sub_exprs.emplace_back(condition->arrow_expr());
    }

    switch (_join_type) {
        case pb::LEFT_JOIN:
        case pb::RIGHT_JOIN:
            join_type = arrow::acero::JoinType::LEFT_OUTER;
            break;
        case pb::INNER_JOIN:
            join_type = arrow::acero::JoinType::INNER;
            break;
        case pb::SEMI_JOIN:
            join_type = arrow::acero::JoinType::LEFT_SEMI;
            break;
        case pb::ANTI_SEMI_JOIN:
            join_type = arrow::acero::JoinType::LEFT_ANTI;
            break;
        case pb::FULL_JOIN:
            join_type = arrow::acero::JoinType::FULL_OUTER;
            break;
        default:
            DB_FATAL_STATE(state, "UNSATISFIED JOIN TYPE:%d", _join_type);
            return -1;
    }
    arrow::acero::HashJoinNodeOptions join_opts{join_type, outer_keys, inner_keys, /*filter=*/arrow::compute::and_(sub_exprs)};
    arrow::acero::Declaration dec{"hashjoin", {outer_dec, inner_dec}, std::move(join_opts)};
    LOCAL_TRACE_ARROW_PLAN_WITH_INFO(dec, &_use_index_join);
    state->append_acero_declaration(dec);
    return 0;
}

// 只支持向量化执行非index join
int JoinNode::no_index_hash_join(RuntimeState* state) {
    _outer_node->set_delay_fetcher_store(true);
    int ret = _outer_node->open(state);
    if (ret < 0) {
        DB_FATAL_STATE(state, "ExecNode::outer table open fail");
        return ret;
    }
    if (_outer_node->node_exec_type() == pb::EXEC_ROW) {
        // 驱动表是index join且走行模式, 这里需要获取驱动表行数据
        // 实际上还是先查驱动表数据, 没有并行
        ret = fetcher_full_table_data(state, _outer_node, _outer_tuple_data);
        if (ret < 0) {
            DB_WARNING("ExecNode::join open fail when fetch left table");
            return ret;
        }
        if (_outer_tuple_data.size() == 0) {
            _outer_table_is_null = true;
            return 0;
        }
    }
    _inner_node->set_delay_fetcher_store(true);
    ret = _inner_node->open(state);
    if (ret < 0) {
        DB_FATAL_STATE(state, "ExecNode::inner table open fail");
        return -1;
    }
    if (_inner_node->node_exec_type() == pb::EXEC_ROW) {
        ret = fetcher_full_table_data(state, _inner_node, _inner_tuple_data);
        if (ret < 0) {
            DB_WARNING("fetcher inner node fail");
            return ret;
        }
    }
    set_node_exec_type(pb::EXEC_ARROW_ACERO);
    return 0;
}

int JoinNode::hash_join(RuntimeState* state) {
    SortNode* sort_node = static_cast<SortNode*>(_outer_node->get_node(pb::SORT_NODE));
    if (sort_node != nullptr) {
        JoinNode* join_node = static_cast<JoinNode*>(sort_node->get_node(pb::JOIN_NODE));
        if (join_node == nullptr) {
            std::vector<ExecNode*> scan_nodes;
            _outer_node->get_node(pb::SCAN_NODE, scan_nodes);
            bool index_has_null = false;
            if (do_plan_router(state, scan_nodes, index_has_null) != 0) {
                DB_WARNING("Fail to do_plan_router");
                return -1;
            }
        }
    }
    if (_join_type == pb::FULL_JOIN) {
        return no_index_hash_join(state);
    }
    if (state->execute_type == pb::EXEC_ARROW_ACERO 
            && (!_use_index_join || state->sign_exec_type == SignExecType::SIGN_EXEC_ARROW_FORCE_NO_INDEX_JOIN)) {
        return no_index_hash_join(state);
    }
    int ret = _outer_node->open(state);
    if (ret < 0) {
        DB_WARNING("ExecNode::outer table open fail");
        return ret;
    }
    bool outer_use_arrow = (_outer_node->node_exec_type() == pb::EXEC_ARROW_ACERO);
    if (!outer_use_arrow) {
        // 驱动表是行
        ret = fetcher_full_table_data(state, _outer_node, _outer_tuple_data);
        if (ret < 0) {
            DB_WARNING("ExecNode::join open fail when fetch left table");
            return ret;
        }
        if (_outer_tuple_data.size() == 0) {
            _outer_table_is_null = true;
            return 0;
        }
        // watt基准循环过滤
        if (_outer_node->get_node(pb::FULL_EXPORT_NODE) != nullptr) {
            _use_loop_hash_map = true;
            return loop_hash_join(state);
        }
        construct_equal_values(_outer_tuple_data, _outer_equal_slot);
    } else {
        // 驱动表是列, 可能是个简单scan, 可能是个复杂join等
        set_node_exec_type(pb::EXEC_ARROW_ACERO);

        ExecNode* join_node = _outer_node->get_node(pb::JOIN_NODE);
        ExecNode* dual_scan_node = _outer_node->get_node(pb::DUAL_SCAN_NODE);
        SelectManagerNode* fetcher_store_node = static_cast<SelectManagerNode*>(_outer_node->get_node(pb::SELECT_MANAGER_NODE));
        if (join_node != nullptr || dual_scan_node != nullptr || (fetcher_store_node != nullptr && fetcher_store_node->need_sorter())) {
            // 驱动表是个join, index_join需要先列式执行驱动表join拿到拼in数据
            START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), OPEN_TRACE, nullptr);
            ret = _outer_node->build_arrow_declaration(state);
            if (ret != 0) {
                DB_FATAL_STATE(state, "build arrow declaration fail");
                return -1;
            }
            // execute
            arrow::Result<std::shared_ptr<arrow::Table>> final_table;
            GlobalArrowExecutor::execute(state, &final_table);
            if (final_table.ok()) {
                // 驱动表join后的结果
                _outer_intermediate_join_result_table = *final_table;
            } else {
                DB_FATAL("arrow acero run fail, status: %s", final_table.status().ToString().c_str());
                return -1;
            }
            if (_outer_node->get_limit() > 0) {
                _outer_intermediate_join_result_table = _outer_intermediate_join_result_table->Slice(0, _outer_node->get_limit());
            }
            state->acero_declarations.clear();
            if (_outer_intermediate_join_result_table != nullptr) {
                construct_equal_values_for_vectorized(_outer_intermediate_join_result_table, _outer_equal_slot);
            }
        } else {
            // 驱动表是一个简单的table scan
            construct_equal_values_for_vectorized(fetcher_store_node->get_region_batches(), _outer_equal_slot);
        }
        if (_outer_join_values.size() == 0) {
            _outer_table_is_null = true;
            return 0;
        }
    }
    ret = runtime_filter(state, _inner_node, nullptr);
    if (ret < 0) {
        DB_WARNING("Fail to runtime_filter");
        return ret;
    }
    ret = _inner_node->open(state);
    if (ret < 0) {
        DB_WARNING("ExecNode::inner table open fial");
        return -1;
    }
    bool inner_use_arrow = (_inner_node->node_exec_type() == pb::EXEC_ARROW_ACERO);
    if (inner_use_arrow) {
        set_node_exec_type(pb::EXEC_ARROW_ACERO);
    } 
    if (_node_exec_type == pb::EXEC_ARROW_ACERO) {
        if (!inner_use_arrow) {
            ret = fetcher_full_table_data(state, _inner_node, _inner_tuple_data);
            if (ret < 0) {
                DB_WARNING("fetcher inner node fail");
                return ret;
            }
        }
        return 0;
    }
    if (_join_type == pb::LEFT_JOIN 
            || _join_type == pb::RIGHT_JOIN) {
        ret = fetcher_full_table_data(state, _inner_node, _inner_tuple_data);
        if (ret < 0) {
            DB_WARNING("fetcher inner node fail");
            return ret;
        }
        construct_hash_map(_inner_tuple_data, _inner_equal_slot);
        _outer_iter = _outer_tuple_data.begin();
    } else {
        construct_hash_map(_outer_tuple_data, _outer_equal_slot);
    }
    return 0;
}

int JoinNode::loop_hash_join(RuntimeState* state) {
    int ret = fetcher_inner_table_data(state, _outer_tuple_data, _inner_tuple_data);
    if (ret < 0) {
        DB_WARNING("fetcher inner node fail");
        return ret;
    }
    construct_hash_map(_inner_tuple_data, _inner_equal_slot);
    _outer_iter = _outer_tuple_data.begin();
    return 0;
}

int JoinNode::nested_loop_join(RuntimeState* state) {
    _mem_row_desc = state->mem_row_desc();
    SortNode* sort_node = static_cast<SortNode*>(_outer_node->get_node(pb::SORT_NODE));
    if (sort_node != nullptr) {
        JoinNode* join_node = static_cast<JoinNode*>(sort_node->get_node(pb::JOIN_NODE));
        if (join_node == nullptr) {
            std::vector<ExecNode*> scan_nodes;
            _outer_node->get_node(pb::SCAN_NODE, scan_nodes);
            bool index_has_null = false;
            if (do_plan_router(state, scan_nodes, index_has_null) != 0) {
                DB_WARNING("Fail to do_plan_router");
                return -1;
            }
        }
    }
    int ret = _outer_node->open(state);
    if (ret < 0) {
        DB_WARNING("ExecNode:: left table open fail");
        return ret;
    }
    ret = fetcher_full_table_data(state, _outer_node, _outer_tuple_data);
    if (ret < 0) {
        DB_WARNING("ExecNode::join open fail when fetch left table");
        return ret;
    }
    if (_outer_tuple_data.size() == 0) {
        _outer_table_is_null = true;
        DB_WARNING("not data");
        return 0;
    }
    construct_equal_values(_outer_tuple_data, _outer_equal_slot);
    ret = runtime_filter(state, _inner_node, nullptr);
    if (ret < 0) {
        DB_WARNING("Fail to runtime_filter");
        return -1;
    }
    ret = _inner_node->open(state);
    if (ret < 0) {
        DB_WARNING("ExecNode::inner table open fial");
        return -1;
    }
    ret = fetcher_full_table_data(state, _inner_node, _inner_tuple_data);
    if (ret < 0) {
        DB_WARNING("fetcher inner node fail");
        return ret;
    }
    _outer_iter = _outer_tuple_data.begin();
    _inner_iter = _inner_tuple_data.begin();
    return 0;
}

int JoinNode::open(RuntimeState* state) {
    TimeCost join_time_cost;
    _outer_node = _children[0];
    _inner_node = _children[1];
    _outer_tuple_ids = _left_tuple_ids;
    _inner_tuple_ids = _right_tuple_ids;
    if (_join_type == pb::RIGHT_JOIN) {
        _outer_node = _children[1];
        _inner_node = _children[0];
        _outer_tuple_ids = _right_tuple_ids;
        _inner_tuple_ids = _left_tuple_ids;
    }
    _mem_row_desc = state->mem_row_desc();
    int ret = strip_out_equal_slots();
    if (ret < 0) {
        DB_WARNING("fill equal slot fail");
        return -1;
    }
    set_node_exec_type(pb::EXEC_ROW);
    if (_use_hash_map) {
        return hash_join(state);
    } else {
        return nested_loop_join(state);
    }
}

int JoinNode::get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
    if (_outer_table_is_null) {
        *eos = true;
        return 0;
    }
    if (node_exec_type() == pb::EXEC_ARROW_ACERO) {
        return 0;
    }
    if (_use_loop_hash_map) {
        return get_next_for_loop_hash_inner_join(state, batch, eos);
    }
    if (_use_hash_map) {
        if (_join_type == pb::INNER_JOIN) {
            return get_next_for_hash_inner_join(state, batch, eos);
        }
        return get_next_for_hash_outer_join(state, batch, eos);
    }
    return get_next_for_nested_loop_join(state, batch, eos);
}

int JoinNode::get_next_for_nested_loop_join(RuntimeState* state, RowBatch* batch, bool* eos) {
    TimeCost get_next_time;
    while (1) {
        if (_outer_iter == _outer_tuple_data.end()) {
            DB_WARNING("when join, outer iter is end, time_cost:%ld", get_next_time.get_time());
            *eos = true;
            return 0;
        }
        bool matched = false;
        
        while (_inner_iter != _inner_tuple_data.end()) {
            if (reached_limit()) {
                DB_WARNING("when join, reach limit size:%lu, time_cost:%ld", 
                            batch->size(), get_next_time.get_time());
                *eos = true;
                return 0;
            }
            if (batch->is_full()) {
                return 0;
            }
            int ret = construct_result_batch(batch, *_outer_iter, *_inner_iter, matched);
            if (ret < 0) {
                DB_WARNING("construct result batch fail");
                return ret;
            }
            ++_num_rows_returned;
            if (matched && _join_type == pb::INNER_JOIN) {
                break;
            } else {
                ++_inner_iter;
            }
        }
        if (!matched) {
            switch (_join_type) {
            case pb::LEFT_JOIN:
            case pb::RIGHT_JOIN: {
                int ret = construct_null_result_batch(batch, *_outer_iter);
                if (ret < 0) {
                    DB_WARNING("construct result batch fail");
                    return ret;
                }
                ++_num_rows_returned;
                break;
            }
            default:
                break;
            }
        }
        ++_outer_iter;
        _inner_iter = _inner_tuple_data.begin();
    }
    return 0;
}

int JoinNode::get_next_for_hash_outer_join(RuntimeState* state, RowBatch* batch, bool* eos) {
    TimeCost get_next_time;
    while (1) {
        if (_outer_iter == _outer_tuple_data.end()) {
            DB_WARNING("when join, outer iter is end, time_cost:%ld", get_next_time.get_time());
            *eos = true;
            return 0;
        }
        MutTableKey outer_key;
        encode_hash_key(*_outer_iter, _outer_equal_slot, outer_key);
        auto inner_mem_rows = _hash_map.seek(outer_key.data());
        if (inner_mem_rows != NULL) {
            for (; _result_row_index < inner_mem_rows->size(); ++_result_row_index) {
                if (reached_limit()) {
                    DB_WARNING("when join, reach limit size:%lu, time_cost:%ld", 
                                    batch->size(), get_next_time.get_time());
                    *eos = true;
                    return 0;
                }
                if (batch->is_full()) {
                    //DB_WARNING("when join, batch is full, time_cost:%ld", get_next_time.get_time());
                    return 0;
                }
                bool matched = false;
                int ret = construct_result_batch(batch, *_outer_iter, (*inner_mem_rows)[_result_row_index], matched);
                if (ret < 0) {
                    DB_WARNING("construct result batch fail");
                    return ret;
                }
                // match=false会补null行
                ++_num_rows_returned;
            }
        } else {
            if (reached_limit()) {
                DB_WARNING("when join, reach limit size:%lu, time_cost:%ld", 
                                batch->size(), get_next_time.get_time());
                *eos = true;
                return 0;
            }
            if (batch->is_full()) {
                //DB_WARNING("when join, batch is full, time_cost:%ld", get_next_time.get_time());
                return 0;
            }
            //fill NULL
            int ret = construct_null_result_batch(batch, *_outer_iter);
            if (ret < 0) {
                DB_WARNING("construct result batch fail");
                return ret;
            }
            ++_num_rows_returned;
        }
        _result_row_index = 0;
        ++_outer_iter;
    }
    return 0;
}

int JoinNode::get_next_for_hash_inner_join(RuntimeState* state, RowBatch* batch, bool* eos) {
    TimeCost get_next_time;
    while (1) {
        if (_inner_row_batch.is_traverse_over()) {
            if (_child_eos) {
                *eos = true;
                DB_WARNING("when join, get next complete, child eos, time_cost:%ld", 
                            get_next_time.get_time());
                return 0;
            } else {
                _inner_row_batch.clear();
                int ret = _inner_node->get_next(state, &_inner_row_batch, &_child_eos);
                if (ret < 0) {
                    DB_WARNING("_children get_next fail");
                    return ret;
                }
                continue;
            }
        }
        std::unique_ptr<MemRow>& inner_mem_row = _inner_row_batch.get_row();
        MutTableKey inner_key;
        encode_hash_key(inner_mem_row.get(), _inner_equal_slot, inner_key);
        auto outer_mem_rows = _hash_map.seek(inner_key.data());
        if (outer_mem_rows != NULL) {
            for (; _result_row_index < outer_mem_rows->size(); ++_result_row_index) {
                if (reached_limit()) {
                    DB_WARNING("when join, reach limit size:%lu, time_cost:%ld", 
                                batch->size(), get_next_time.get_time());
                    *eos = true;
                    return 0;
                }
                if (batch->is_full()) {
                    return 0;
                }
                bool matched = false;
                int ret = construct_result_batch(batch, (*outer_mem_rows)[_result_row_index], inner_mem_row.get(), matched);
                if (ret < 0) {
                    DB_WARNING("construct result batch fail");
                    return ret;
                }
                if (matched) {
                    ++_num_rows_returned;
                }
            }
        }
        _result_row_index = 0;
        _inner_row_batch.next();
    }
    return 0;
}

int JoinNode::get_next_for_loop_hash_inner_join(RuntimeState* state, RowBatch* batch, bool* eos) {
    TimeCost get_next_time;
    if (_outer_iter == _outer_tuple_data.end()) {
        DB_WARNING("when join, outer iter is end, time_cost:%ld", get_next_time.get_time());
        // clear previous
        for (auto& mem_row : _outer_tuple_data) {
            delete mem_row;
        }
        _outer_tuple_data.clear();
        for (auto& mem_row : _inner_tuple_data) {
            delete mem_row;
        }
        _inner_tuple_data.clear();
        _hash_map.clear();

        FullExportNode* full_export = static_cast<FullExportNode*>(_outer_node->get_node(pb::FULL_EXPORT_NODE));
        if (full_export == nullptr) {
            DB_WARNING("full_export is null");
            return -1;
        }
        full_export->reset_num_rows_returned();

        int ret = fetcher_full_table_data(state, _outer_node, _outer_tuple_data);
        if (ret < 0) {
            DB_WARNING("fetcher outer node fail");
            return ret;
        }
        if (_outer_tuple_data.size() == 0) {
            _outer_table_is_null = true;
            *eos = true;
            DB_WARNING("not data");
            return 0;
        }

        ret = fetcher_inner_table_data(state, _outer_tuple_data, _inner_tuple_data);
        if (ret < 0) {
            DB_WARNING("fetcher inner node fail");
            return ret;
        }
        construct_hash_map(_inner_tuple_data, _inner_equal_slot);
        _outer_iter = _outer_tuple_data.begin();
    }

    while (1) {
        if (_outer_iter == _outer_tuple_data.end()) {
            DB_WARNING("when join, outer iter is end, time_cost:%ld", get_next_time.get_time());
            *eos = true;
            return 0;
        }
        MutTableKey outer_key;
        encode_hash_key(*_outer_iter, _outer_equal_slot, outer_key);
        auto inner_mem_rows = _hash_map.seek(outer_key.data());
        if (inner_mem_rows != NULL) {
            for (; _result_row_index < inner_mem_rows->size(); ++_result_row_index) {
                if (reached_limit()) {
                    DB_WARNING("when join, reach limit size:%lu, time_cost:%ld", 
                                    batch->size(), get_next_time.get_time());
                    *eos = true;
                    return 0;
                }
                if (batch->is_full()) {
                    //DB_WARNING("when join, batch is full, time_cost:%ld", get_next_time.get_time());
                    return 0;
                }
                bool matched = false;
                int ret = construct_result_batch(batch, *_outer_iter, (*inner_mem_rows)[_result_row_index], matched);
                if (ret < 0) {
                    DB_WARNING("construct result batch fail");
                    return ret;
                }
                if (matched) {
                    ++_num_rows_returned;
                }
            }
        }
        _result_row_index = 0;
        ++_outer_iter;
    }
    return 0;
}

bool JoinNode::need_reorder(
        std::map<int32_t, ExecNode*>& tuple_join_child_map,
        std::map<int32_t, std::set<int32_t>>& tuple_equals_map, 
        std::vector<int32_t>& tuple_order,
        std::vector<ExprNode*>& conditions) {
    if (_join_type != pb::INNER_JOIN) {
        return false;
    }
    for (auto& child : _children) {
        if (child->node_type() == pb::JOIN_NODE) {
            if (!static_cast<JoinNode*>(child)->need_reorder(
                        tuple_join_child_map, tuple_equals_map, tuple_order, conditions)) {
                return false;
            }
        } else {
            // Join节点和Join节点之间存在其他类型节点，不进行reorder
            ExecNode* join_node = child->get_node(pb::JOIN_NODE);
            if (join_node != nullptr) {
                return false;
            }
            ExecNode* scan_node = child->get_node(pb::SCAN_NODE);
            if (scan_node == nullptr) {
                return false;
            }
            int32_t tuple_id = static_cast<ScanNode*>(scan_node)->tuple_id();
            tuple_join_child_map[tuple_id] = child;
            tuple_order.push_back(tuple_id);
        }
    }
    for (auto& expr : _conditions) {
        // expr_is_equal_condition_and_build_slot(expr);
        conditions.push_back(expr);
    }
    for (size_t i = 0; i < _outer_equal_slot.size(); i++) {
        int32_t left_tuple_id = static_cast<SlotRef*>(_outer_equal_slot[i])->tuple_id();
        int32_t right_tuple_id = static_cast<SlotRef*>(_inner_equal_slot[i])->tuple_id();
        tuple_equals_map[left_tuple_id].insert(right_tuple_id);
        tuple_equals_map[right_tuple_id].insert(left_tuple_id);
    }
    return true;
}

}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
