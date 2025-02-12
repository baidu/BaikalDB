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
#include "runtime_state.h"
#include "query_context.h"

namespace baikaldb {
DECLARE_int32(arrow_multi_threads);
int AggNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    _is_merger = node.node_type() == pb::MERGE_AGG_NODE;
    for (auto& expr : node.derive_node().agg_node().group_exprs()) {
        ExprNode* group_expr = nullptr;
        ret = ExprNode::create_tree(expr, &group_expr);
        if (ret < 0) {
            //如何释放资源
            return ret;
        }
        _group_exprs.emplace_back(group_expr);
    }
    //DB_NOTICE("_group_exprs: %d %s", node.derive_node().agg_node().group_exprs_size(), node.DebugString().c_str());
    for (auto& expr : node.derive_node().agg_node().agg_funcs()) {
        if (expr.nodes_size() < 1 || expr.nodes(0).node_type() != pb::AGG_EXPR) {
            DB_WARNING("AggNode::init fail, expr.nodes_size:%d", expr.nodes_size());
            return -1;
        }
        int slot_id = expr.nodes(0).derive_node().slot_id();
        if (_agg_slot_set.count(slot_id) == 1) {
            continue;
        }
        _agg_slot_set.insert(slot_id);
        ExprNode* agg_call = nullptr;
        ret = ExprNode::create_tree(expr, &agg_call);
        if (ret < 0) {
            //如何释放资源
            return ret;
        }
        _agg_fn_calls.emplace_back(static_cast<AggFnCall*>(agg_call));
    }
    //_group_tuple_id = node.derive_node().agg_node().group_tuple_id();
    _agg_tuple_id = node.derive_node().agg_node().agg_tuple_id();
    _hash_map.init(12301);
    _iter = _hash_map.end();
    if (node.derive_node().agg_node().has_arrow_ignore_tuple_id()) {
        _arrow_ignore_tuple_id = node.derive_node().agg_node().arrow_ignore_tuple_id();
    }
    return 0;
}

int AggNode::expr_optimize(QueryContext* ctx) {
    int ret = 0;
    ret = ExecNode::expr_optimize(ctx);
    if (ret < 0) {
        DB_WARNING("ExecNode::optimize fail, ret:%d", ret);
        return ret;
    }
    ret = common_expr_optimize(&_group_exprs);
    if (ret < 0) {
        DB_WARNING("common_expr_optimize fail");
        return ret;
    }
    if (_agg_tuple_id < 0) {
        return 0;
    }
    pb::TupleDescriptor* agg_tuple_desc = ctx->get_tuple_desc(_agg_tuple_id);
    if (agg_tuple_desc == nullptr) {
        return -1;
    }
    //_intermediate_slot_id != _final_slot_id则type为pb::STRING
    //_final_slot_id type在AggExpr里会设置
    for (auto& slot : *agg_tuple_desc->mutable_slots()) {
        slot.set_slot_type(pb::STRING);
    }
    for (auto expr : _agg_fn_calls) {
        //类型推导
        ret = expr->type_inferer(agg_tuple_desc);
        if (ret < 0) {
            DB_WARNING("expr type_inferer fail:%d", ret);
            return ret;
        }
        //常量表达式计算
        expr->const_pre_calc();
    }
    return 0;
}

bool AggNode::can_use_arrow_vector() {
    for (auto& expr : _group_exprs) {
        if (!expr->can_use_arrow_vector()) {
            return false;
        }
        if (!expr->is_slot_ref()) {
            continue;
        }
    }
    for (auto& expr : _agg_fn_calls) {
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

int AggNode::vectorize_build_group_by_exprs(RuntimeState* state, 
                                            ExprNode* group_by_expr, 
                                            std::vector<arrow::FieldRef>& group_by_fields,
                                            std::set<std::string>& key_field_names,
                                            std::vector<arrow::compute::Expression>& generate_projection_exprs,
                                            std::vector<std::string>& generate_projection_exprs_names) {
    int ret = group_by_expr->transfer_to_arrow_expression();
    if (ret != 0) {
        DB_FATAL_STATE(state, "get arrow groupby field ref fail");
        return -1;
    }
    if (group_by_expr->arrow_expr().field_ref() != nullptr) {
        auto field_ref = group_by_expr->arrow_expr().field_ref();
        if (key_field_names.count(*(field_ref->name())) > 0) {
            // 去重, 已经在group by字段里, 不需要额外添加, 重复添加会导致arrow执行失败
            return 0;
        }
        group_by_fields.emplace_back(*field_ref);
        key_field_names.insert(*(field_ref->name()));
    } else {
        // 需要构建临时列
        generate_projection_exprs.emplace_back(group_by_expr->arrow_expr());
        std::string tmp_name = "tmp_" + std::to_string(generate_projection_exprs.size());
        generate_projection_exprs_names.emplace_back(tmp_name);
        group_by_fields.emplace_back(arrow::FieldRef(tmp_name));
    }
    return 0;
}

int AggNode::build_arrow_declaration(RuntimeState* state) {
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), OPEN_TRACE, nullptr);
    int ret = 0;
    std::vector<arrow::FieldRef> group_by_fields;
    std::vector<arrow::compute::Aggregate> aggregates;
    // group by列
    std::set<std::string> key_field_names;
    std::vector<arrow::compute::Expression> generate_projection_exprs;
    std::vector<std::string> generate_projection_exprs_names;
    group_by_fields.reserve(_group_exprs.size());
    aggregates.reserve(_agg_fn_calls.size());
    for (auto& group_field : _group_exprs) {
        if (0 != vectorize_build_group_by_exprs(state, group_field, group_by_fields, key_field_names, generate_projection_exprs, generate_projection_exprs_names)) {
            return -1;
        }
    }
    // 特殊处理multi distinct
    // 如select count(distinct a), count(distinct b) from t group by c;
    // merge agg node(multi_distinct expr) group by c -> merge agg node(multi_distinct expr) group by c -> agg node(multi_distinct expr) group by c
    // ====>
    // merge agg node(multi_distinct expr) group by c -> merge agg node(ignore multi_distinct expr) group by a,b,c -> agg node(ignore multi_distinct expr) group by a,b,c
    // 同时store必须开启向量化执行避免行列混合
    bool transfer_multi_distinct_to_group_by = false;
    bool has_multi_distinct = false;
    if (_node_type == pb::AGG_NODE || (_node_type == pb::MERGE_AGG_NODE && _parent->node_type() == pb::MERGE_AGG_NODE)) {
        transfer_multi_distinct_to_group_by = true;
    }
    for (int i = 0; i < _agg_fn_calls.size(); ++i) {
        if (_agg_fn_calls[i]->is_multi_distinct_agg()) { 
            has_multi_distinct = true;
        }
    }

    bool has_group_by = group_by_fields.size() > 0 || (transfer_multi_distinct_to_group_by && has_multi_distinct);
    for (int i = 0; i < _agg_fn_calls.size(); ++i) {
        if (transfer_multi_distinct_to_group_by && _agg_fn_calls[i]->is_multi_distinct_agg()) {
            for (auto& c : _agg_fn_calls[i]->children()) {
                if (0 != vectorize_build_group_by_exprs(state, c, group_by_fields, key_field_names, generate_projection_exprs, generate_projection_exprs_names)) {
                    return -1;
                }
            }
            continue;
        }
        ret = _agg_fn_calls[i]->transfer_to_arrow_agg_function(aggregates, 
                                                               has_group_by, 
                                                               _is_merger, 
                                                               generate_projection_exprs, 
                                                               generate_projection_exprs_names);
        if (ret != 0) {
            DB_FATAL_STATE(state, "get arrow agg function fail");
            return ret;
        }
    }
    bool need_add_projections = (generate_projection_exprs.size() > 0);
    // aggnode很恶心, node输出schema是{groupBy key_fields list, agg_func_fields}
    // 所以不在group by里的key, 需要额外加first聚合取第一个值
    // 并且node输出会重排列顺序, 需要注意
    for (auto& tuple : state->tuple_descs()) {
        if (tuple.tuple_id() == _arrow_ignore_tuple_id
                || tuple.tuple_id() == _agg_tuple_id) {
            continue;
        }
        for (const auto& slot : tuple.slots()) {
            std::string name = std::to_string(tuple.tuple_id()) + "_" + std::to_string(slot.slot_id());
            if (key_field_names.count(name) == 0) {
                if (FLAGS_arrow_multi_threads > 0 && group_by_fields.size() > 0) {
                    aggregates.emplace_back("hash_one", 
                                            /*options*/nullptr, 
                                            arrow::FieldRef(name), 
                                            /*new field name*/name);
                } else {
                    // AGG first需要arrow 13.0版本, first不能parallel
                    aggregates.emplace_back(group_by_fields.empty() ? "first" : "hash_first", 
                                            /*options*/nullptr, 
                                            arrow::FieldRef(name), 
                                            /*new field name*/name);
                    state->vectorlized_parallel_execution = false;
                }
            }
            if (need_add_projections) {
                generate_projection_exprs.emplace_back(arrow::compute::field_ref(name));
                generate_projection_exprs_names.emplace_back(name);
            }
        }
    }
    for (auto c : _children) {
        ret = c->build_arrow_declaration(state);
        if (ret < 0) {
            return ret;
        }
    }
    if (need_add_projections) {
        for (int i = 0; i < _agg_fn_calls.size(); ++i) {
            _agg_fn_calls[i]->add_agg_projection_slot_ref(_is_merger, generate_projection_exprs, generate_projection_exprs_names);
        }
        arrow::acero::Declaration dec{"project", arrow::acero::ProjectNodeOptions{std::move(generate_projection_exprs), std::move(generate_projection_exprs_names)}};
        LOCAL_TRACE_ARROW_PLAN(dec);
        state->append_acero_declaration(dec);
    }
    arrow::acero::Declaration dec{"aggregate",
            arrow::acero::AggregateNodeOptions{aggregates, group_by_fields}};
    LOCAL_TRACE_ARROW_PLAN(dec);
    state->append_acero_declaration(dec);
    return 0;
}

int AggNode::open(RuntimeState* state) {
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), OPEN_TRACE, nullptr);
    int ret = 0;
    ret = ExecNode::open(state);
    if (ret < 0) {
        DB_WARNING_STATE(state, "ExecNode::open fail, ret:%d", ret);
        return ret;
    }
    for (auto expr : _group_exprs) {
        ret = expr->open();
        if (ret < 0) {
            DB_WARNING_STATE(state, "expr open fail, ret:%d", ret);
            return ret;
        }
    }
    for (auto agg : _agg_fn_calls) {
        ret = agg->open();
        if (ret < 0) {
            DB_WARNING_STATE(state, "agg open fail, ret:%d", ret);
            return ret;
        }
        if (agg->is_multi_distinct_agg() && state->execute_type == pb::EXEC_ARROW_ACERO) {
            state->force_vectorize = true; // store only, for rocksdb scan node
        }
    }
    ON_SCOPE_EXIT(([this]() {
        _iter = _hash_map.begin();
    }));
    _mem_row_desc = state->mem_row_desc();
    TimeCost cost;
    int64_t agg_time = 0;
    int64_t scan_time = 0;
    int row_cnt = 0;
    for (auto child : _children) {
        bool eos = false;
        do {
            if (state->is_cancelled()) {
                DB_WARNING_STATE(state, "cancelled");
                return 0;
            }
            TimeCost cost;
            RowBatch batch;
            ret = child->get_next(state, &batch, &eos);
            if (ret < 0) {
                DB_WARNING_STATE(state, "child->get_next fail, ret:%d", ret);
                return ret;
            }
            set_node_exec_type(child->node_exec_type());
            if (_node_exec_type == pb::EXEC_ARROW_ACERO) {
                return 0;
            }
            scan_time += cost.get_time();
            cost.reset();
            int64_t used_size = 0;
            int64_t release_size = 0;
            process_row_batch(state, batch, used_size, release_size);
            agg_time += cost.get_time();
            row_cnt += batch.size();
            state->memory_limit_release(row_cnt, release_size);
            if (state->memory_limit_exceeded(row_cnt, used_size) != 0) {
                DB_WARNING_STATE(state, "memory limit exceeded");
                return -1;
            }
            // 对于用order by分组的特殊优化
            //if (_agg_tuple_id == -1 && _limit != -1 && (int64_t)_hash_map.size() >= _limit) {
            //    break;
            //}
        } while (!eos);
    }
    LOCAL_TRACE_DESC << "agg time cost:" << agg_time << 
        " scan time cost:" << scan_time << " rows:" << row_cnt;

    // 兼容mysql: select count(*) from t; 无数据时返回0
    if (_hash_map.size() == 0 && _group_exprs.size() == 0) {
        ExecNode* packet = get_parent_node(pb::PACKET_NODE);
        // baikaldb才有packet_node;只在baikaldb上产生数据
        // TODB:join和子查询后续如果要完全推到store运行得注意
        if (packet != nullptr) {
            std::unique_ptr<MemRow> row = _mem_row_desc->fetch_mem_row();
            uint8_t null_flag = 0;
            MutTableKey key;
            key.append_u8(null_flag);
            int64_t used_size= 0;
            AggFnCall::initialize_all(_agg_fn_calls, key.data(), row.get(), used_size, true);
            _hash_map.insert(key.data(), row.release());
        }
    }
    return 0;
}

void AggNode::encode_agg_key(MemRow* row, MutTableKey& key) {
    uint8_t null_flag = 0;
    key.append_u8(null_flag);
    for (uint32_t i = 0; i < _group_exprs.size(); i++) {
        ExprValue value = _group_exprs[i]->get_value(row);
        if (value.is_null()) {
            null_flag |= (0x01 << (7 - i));
            continue;
        }
        key.append_value(value);
    }
    key.replace_u8(null_flag, 0);
}

void AggNode::process_row_batch(RuntimeState* state, RowBatch& batch, int64_t& used_size, int64_t& release_size) {
    for (batch.reset(); !batch.is_traverse_over(); batch.next()) {
        std::unique_ptr<MemRow>& row = batch.get_row();
        MutTableKey key;
        MemRow* cur_row = row.get();
        encode_agg_key(cur_row, key);
        MemRow** agg_row = _hash_map.seek(key.data());
        
        if (agg_row == nullptr) { //不存在则新建
            cur_row = row.release();
            agg_row = &cur_row;
            // fix bug: 多个store agg，有无数据会造条空数据(L157)
            // merge多个store时，去除这种造的数据
            // 以便于 select id,count(*) from t where id>1;这种sql时id不会时造出来的null
            if (_is_merger && _group_exprs.size() == 0) {
                if (AggFnCall::all_is_initialize(_agg_fn_calls, key.data(), *agg_row)) {
                    delete cur_row;
                    continue;
                }
            }
            AggFnCall::initialize_all(_agg_fn_calls, key.data(), *agg_row, used_size, false);
            used_size += cur_row->used_size();
            used_size += key.size();
            // 可能会rehash
            _hash_map.insert(key.data(), *agg_row);
        } else {
            release_size += cur_row->used_size();
        }
        if (*agg_row == nullptr) {
            DB_FATAL("seek nullptr, key:%s", str_to_hex(key.data()).c_str());
            return;
        }
        if (_is_merger) {
            AggFnCall::merge_all(_agg_fn_calls, key.data(), cur_row, *agg_row, used_size);
        } else {
            AggFnCall::update_all(_agg_fn_calls, key.data(), cur_row, *agg_row, used_size);
        }
    }
}

int AggNode::get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), GET_NEXT_TRACE, ([this](TraceLocalNode& local_node) {
        local_node.set_affect_rows(_num_rows_returned);
    }));
    if (node_exec_type() == pb::EXEC_ARROW_ACERO) {
        return 0;
    }
    while (1) {
        if (state->is_cancelled()) {
            DB_WARNING_STATE(state, "cancelled");
            *eos = true;
            return 0;
        }
        if (reached_limit() || _iter == _hash_map.end()) {
            *eos = true;
            return 0;
        }
        if (batch->is_full()) {
            return 0;
        }
        AggFnCall::finalize_all(_agg_fn_calls, _iter->first, _iter->second, _is_merger);
        batch->move_row(std::move(std::unique_ptr<MemRow>(_iter->second)));
        _num_rows_returned++;
        _iter->second = nullptr;
        _iter++;
    }
}

void AggNode::close(RuntimeState* state) {
    ExecNode::close(state);
    for (auto expr : _group_exprs) {
        expr->close();
    }
    for (auto agg : _agg_fn_calls) {
        agg->close();
    }
    for (; _iter != _hash_map.end(); _iter++) {
        delete _iter->second;
    }
    _hash_map.clear();
}
void AggNode::transfer_pb(int64_t region_id, pb::PlanNode* pb_node) {
    ExecNode::transfer_pb(region_id, pb_node);
    auto agg_node = pb_node->mutable_derive_node()->mutable_agg_node();
    agg_node->clear_group_exprs();
    for (auto expr : _group_exprs) {
        ExprNode::create_pb_expr(agg_node->add_group_exprs(), expr);
    }
    agg_node->clear_agg_funcs();
    for (auto agg : _agg_fn_calls) {
        ExprNode::create_pb_expr(agg_node->add_agg_funcs(), agg);
    }
}
void AggNode::find_place_holder(std::unordered_multimap<int, ExprNode*>& placeholders) {
    ExecNode::find_place_holder(placeholders);
    for (auto& expr : _group_exprs) {
        expr->find_place_holder(placeholders);
    }
    for (auto& expr : _agg_fn_calls) {
        expr->find_place_holder(placeholders);
    }
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
