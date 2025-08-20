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

#include "select_planner.h"
#include "union_planner.h"
#include "dual_scan_node.h"
#include "network_socket.h"
#include <boost/algorithm/string.hpp>
#include "row_expr.h"
#include "slot_ref.h"
#include "scalar_fn_call.h"
#include "common.h"
#include "packet_node.h"

namespace baikaldb {

int SelectPlanner::plan() {
    if (!_ctx->stmt) {
        DB_WARNING("no sql command set");
        return -1;
    }
    auto client = _ctx->client_conn;
    if (client->txn_id == 0) {
        _ctx->get_runtime_state()->set_single_sql_autocommit(true);
    } else {
        _ctx->get_runtime_state()->set_single_sql_autocommit(false);
    }
    _select = (parser::SelectStmt*)_ctx->stmt;
    if (0 != check_multi_distinct()) {
        return -1;
    }
    if (_select->table_refs == nullptr) {
        if (0 != parse_select_fields()) {
            return -1;        
        }
        if (_agg_funcs.empty() && _distinct_agg_funcs.empty() && _group_exprs.empty() && _window_nodes.empty()) {
            create_packet_node(pb::OP_SELECT);
            create_dual_scan_node();
        } else {
            create_agg_tuple_desc();
            if (0 != create_window_tuple_desc()) {
               return -1;
            }
            create_packet_node(pb::OP_SELECT);
            if (0 != create_window_and_sort_nodes()) {
                return -1;
            }
            // create_agg_node
            if (0 != create_agg_node()) {
                return -1;
            }
            create_dual_scan_node();
        }
        return 0;
    }
    if (_select->select_opt != nullptr) {
        _ctx->is_straight_join = _select->select_opt->straight_join;
    }

    if (_select->lock == parser::SL_FOR_UPDATE && client->txn_id != 0) {
        _ctx->select_for_update = true;
    }

    if (0 != parse_with()) {
        return -1;
    }
    // parse from
    if (0 != parse_db_tables(_select->table_refs, &_join_root)) {
        return -1;
    }

    // 获取编码转换信息
    if (get_convert_charset_info() != 0) {
        return -1;
    }

    // parse select
    if (0 != parse_select_fields()) {
        return -1;
    }
    // parse where (scan) filter, get scan node and filter node
    if (0 != parse_where()) {
        return -1;
    }
    // parse group by
    if (0 != parse_groupby()) {
        return -1;        
    }
    // parse having filter
    if (0 != _parse_having()) {
        return -1;        
    }
    // parse order by
    if (0 != parse_orderby()) {
        return -1;        
    }
    // parse limit
    if (0 != parse_limit()) {
        return -1;        
    }
    // 非相关子查询优化
    if (0 != subquery_rewrite()) {
        return -1;
    }

    if (0 != minmax_remove()) {
        return -1;
    }

    if (_ctx->is_base_subscribe) {
        if (0 != get_base_subscribe_scan_ref_slot()) {
            return -1;
        }
    }
    if (0 != merge_window_node()) {
        return -1;
    }

    create_scan_tuple_descs();
    create_agg_tuple_desc();
    create_order_by_tuple_desc();
    //print_debug_log();
    //_create_group_tuple_desc();
    if (0 != create_window_tuple_desc()) {
        return -1;
    }

    if (!_ctx->is_full_export && is_full_export()) {
        _ctx->is_full_export = true;
    }
    get_slot_column_mapping();

    // exec node: scan -> filter -> group by -> having -> order by -> limit -> select
    create_packet_node(pb::OP_SELECT);
    // create limit node
    if (0 != create_limit_node()) {
        return -1;
    }
    // create_sort_node
    if (0 != create_sort_node()) {
        return -1;
    }
    // create_window_and_sort_nodes
    if (0 != create_window_and_sort_nodes()) {
        return -1;
    }
    // create_having_filter_node
    if (0 != create_filter_node(_having_filters, pb::HAVING_FILTER_NODE)) {
        return -1;
    }
    // create_agg_node
    if (0 != create_agg_node()) {
        return -1;
    }
    // create_filter_node
    if (0 != create_filter_node(_where_filters, pb::WHERE_FILTER_NODE)) {
        return -1;
    }
    // join节点的叶子节点是scan_node
    if (0 != create_join_and_scan_nodes(_join_root, _apply_root)) {
        return -1;
    }
    // for (uint32_t idx = 0; idx < _ctx->plan.nodes_size(); ++idx) {
    //     DB_WARNING("plan_node: %s", _ctx->plan.nodes(idx).DebugString().c_str());
    // }
    return 0;
}

bool SelectPlanner::is_full_export() {
    if (_ctx->is_union_subquery) {
        // Union子语句不支持全量导出
        return false;
    }
    // 包含DBLink Mysql的表不支持全量导出
    if (_ctx->has_dblink_mysql) {
        return false;
    }
    //代价信息统计时不走full export流程
    if (_ctx->explain_type != EXPLAIN_NULL) {
        return false;
    }
    if (_ctx->is_prepared) {
        return false;
    }
    if (_ctx->debug_region_id != -1) {
        return false;
    }
    if (_ctx->has_window_func) {
        return false;
    }
    if (_ctx->has_derived_table || _ctx->has_information_schema) {
        return false;
    }
    if (_select->group != nullptr) {
        return false;
    } 
    if (_select->having != nullptr) {
        return false;
    } 
    if (_select->order != nullptr || !_order_ascs.empty()) {
        return false;
    }
    //if (_select->limit != nullptr) {
    //    return false;
    //}
    if (_select->table_refs->node_type == parser::NT_JOIN) {
        return false;
    }
    if (_apply_root != nullptr) {
        return false;
    }
    if (_select->select_opt != nullptr 
        && _select->select_opt->distinct == true) {
        return false;
    }
    if ((_agg_funcs.size() > 0) || (_distinct_agg_funcs.size() > 0) 
         || (_group_exprs.size() > 0)) {
        return false;
    }
    if (_select->where != nullptr) {
        if (_select->limit == nullptr) {
            return false;
        }
        if (!is_fullexport_condition()) {
            return false;
        }
    }
    if (_ctx->query_cache > 0) {
        return false;
    }
    return true;
}

void SelectPlanner::get_conjuncts_condition(std::vector<ExprNode*>& conjuncts) {
    conjuncts.reserve(_where_filters.size());
    for (auto& expr : _where_filters) {
        ExprNode* conjunct = nullptr;
        int ret = ExprNode::create_tree(expr, &conjunct);
        if (ret < 0) {
            //如何释放资源
            return ;
        }
        conjuncts.emplace_back(conjunct);
    }
}

bool SelectPlanner::is_fullexport_condition() {
    std::vector<ExprNode*> conjuncts;
    get_conjuncts_condition(conjuncts);
    //释放内存
    ON_SCOPE_EXIT(([this, &conjuncts]() {
        for (auto& conjunct : conjuncts) {
            ExprNode::destroy_tree(conjunct);
        }
    }));
    if (conjuncts.size() > 2) {
        return false;
    }
    if (!check_conjuncts(conjuncts)) {
        return false;
    }
    return true;
}

bool SelectPlanner::check_conjuncts(std::vector<ExprNode*>& conjuncts) {
    for (auto& conjunct : conjuncts) {
        if (!check_single_conjunct(conjunct)) {
            return false;
        }
    }
    return true;
}

bool SelectPlanner::check_single_conjunct(ExprNode* conjunct) {
    //conjunct的size为1同时含有limit节点
    auto& expr_head_node = conjunct;
    if (!is_range_compare(expr_head_node)) {
        return false;
    }

    int64_t& factory_table_id = _ctx->stat_info.table_id;
    auto pk_field_info_ptr = _factory->get_index_info_ptr(factory_table_id);
    if (pk_field_info_ptr == nullptr) {
        return false;
    }
    auto& pk_fields_info = pk_field_info_ptr->fields;
    if (expr_head_node->children_size() == 0) {
        return false;
    }
    //判断row_expr情况下的主键情况
    auto expr_node_about_primary = expr_head_node->children(0);
    if (expr_node_about_primary->is_row_expr()) {
        RowExpr* row_expr = static_cast<RowExpr*>(expr_node_about_primary);
        std::map<size_t, SlotRef*> slots;
        row_expr->get_all_slot_ref(&slots);
        std::vector<int32_t> pk_field_ids;
        pk_field_ids.reserve(slots.size());
        for (auto& pair : slots) {
            pk_field_ids.emplace_back(pair.second->field_id());
        }
        if (pk_fields_info.size() != pk_field_ids.size()) {
            return false;
        }
        if (is_pk_consistency(pk_fields_info, pk_field_ids)) {
            return true;
        } else {
            return false;
        }
    } else if (expr_node_about_primary->is_slot_ref()) {
        //判断为slot_ref情况下的主键情况
        SlotRef* slot_expr = static_cast<SlotRef*>(expr_node_about_primary);
        auto slot_expr_pk_field_id = slot_expr->field_id();
        if (pk_fields_info.size() != 1) {
            return false;
        }
        if (slot_expr_pk_field_id != pk_fields_info[0].id) {
            return false;
        } else {
            return true;
        }
    }
    return true;
}

bool SelectPlanner::is_pk_consistency(const std::vector<FieldInfo>& pk_fields_in_factory, const std::vector<int32_t>& select_pk_fields) {
    if (pk_fields_in_factory.size() != select_pk_fields.size()) {
        return false;
    }
    for (size_t i = 0;i < pk_fields_in_factory.size();i++) {
        if (pk_fields_in_factory[i].id != select_pk_fields[i]) {
            return false;
        }
    }
    return true;
}

bool SelectPlanner::is_range_compare(ExprNode* expr) {
    switch (expr->node_type()) {
        case pb::FUNCTION_CALL: {
            int32_t fn_op = static_cast<ScalarFnCall*>(expr)->fn().fn_op();
            switch (fn_op) {
                case parser::FT_EQ:
                case parser::FT_GE:
                case parser::FT_GT:
                case parser::FT_LE:
                case parser::FT_LT:
                    return true;
                default:
                    return false;
            }
        }
        default:
            return false;
    }
    return false;
}

void SelectPlanner::get_slot_column_mapping() {
    if (_ctx->has_derived_table) {
        auto& outer_ref_map = _ctx->ref_slot_id_mapping;
        for (auto& iter_out : outer_ref_map) {
            auto it = _plan_table_ctx->derived_table_ctx_mapping.find(iter_out.first);
            if (it == _plan_table_ctx->derived_table_ctx_mapping.end()) {
                continue;
            }
            auto& sub_ctx = it->second;
            auto& inter_column_map = sub_ctx->field_column_id_mapping;
            for (auto& field_slot : iter_out.second) {
                int32_t outer_slot_id = field_slot.second;
                auto iter = inter_column_map.find(field_slot.first);
                if (iter == inter_column_map.end()) {
                    DB_WARNING("field not found:%s", field_slot.first.c_str());
                    continue;
                }
                int32_t inter_column_id = iter->second;
                _ctx->slot_column_mapping[iter_out.first][outer_slot_id] = inter_column_id;
                //DB_WARNING("tuple_id:%d outer_slot_id:%d inter_column_id:%d", iter_out.first, outer_slot_id, inter_column_id);
            }
        }
    }    
}

int SelectPlanner::minmax_remove() {
    if (!_distinct_agg_funcs.empty() || ! _group_exprs.empty()) {
        return 0;
    }
    if (_select_exprs.size() != 1 || _select_exprs[0].nodes(0).node_type() != pb::AGG_EXPR) {
        return 0;
    }
    if (_group_slots.size() != 0 || _order_exprs.size() != 0 || _group_exprs.size() != 0) {
        return 0;
    }
    pb::Expr select_expr = _select_exprs[0];
    if (select_expr.nodes_size() != 2) {
        return 0;
    }
    std::string fn_name = select_expr.nodes(0).fn().name();
    if (fn_name != "max" && fn_name != "min") {
        return 0;
    }
    pb::ExprNode slot = select_expr.nodes(1);
    if (slot.node_type() != pb::SLOT_REF) {
        return 0;
    }
    _select_exprs.clear();
    _group_exprs.clear();
    _agg_funcs.clear();
    pb::Expr new_select;
    new_select.set_database(select_expr.database());
    new_select.set_table(select_expr.table());
    auto add_node = new_select.add_nodes();
    *add_node = slot;
    _select_exprs.push_back(new_select);
    pb::Expr order_expr;
    order_expr.set_database(select_expr.database());
    order_expr.set_table(select_expr.table());
    add_node = order_expr.add_nodes();
    *add_node = slot;
    _order_exprs.push_back(order_expr);
    if (fn_name == "max") {
        _order_ascs.push_back(false);
    } else {
        _order_ascs.push_back(true);
    }
    _ctx->get_runtime_state()->must_have_one = true;
    _limit_offset.clear_nodes();
    auto offset = _limit_offset.add_nodes();
    offset->mutable_derive_node()->set_int_val(0);
    offset->set_node_type(pb::INT_LITERAL);
    offset->set_col_type(pb::INT64);
    _limit_count.clear_nodes();
    auto limit = _limit_count.add_nodes();
    limit->mutable_derive_node()->set_int_val(1);
    limit->set_node_type(pb::INT_LITERAL);
    limit->set_col_type(pb::INT64);
    return 0;
}

int SelectPlanner::subquery_rewrite() {
    if (!_ctx->expr_params.is_expr_subquery) {
        return 0;
    }
    // 相关子查询判断，current_tuple_ids包含当前sql涉及的slot的tuple
    for (auto tuple_id : _ctx->current_tuple_ids) {
        if (_ctx->current_table_tuple_ids.count(tuple_id) == 0) {
            _ctx->expr_params.is_correlated_subquery = true;
            _ctx->has_derived_table = true;
            return 0;
        }
    }
    // any/all只支持单值
    if (_select_exprs.size() != 1) {
        return 0;
    }
    
    pb::Expr expr = _select_exprs[0];
    pb::Expr rewrite_expr;
    // create agg expr node
    pb::ExprNode* node = rewrite_expr.add_nodes();
    node->set_node_type(pb::AGG_EXPR);
    node->set_col_type(pb::INVALID_TYPE);
    pb::Function* func = node->mutable_fn();
    func->set_fn_op(_ctx->expr_params.func_type);
    func->set_has_var_args(false);
    node->set_num_children(1);
    for (auto& old_node : expr.nodes()) {
        rewrite_expr.add_nodes()->CopyFrom(old_node);
    }
    pb::DeriveExprNode* derive_node = node->mutable_derive_node();
    std::vector<pb::SlotDescriptor> slots;

    // 如果子查询不包含WindowNode或AggNode，则在子查询最上层添加MIN/MAX(slot)聚合函数；
    // 如果子查询包含WindowNode或AggNode，则在子查询最上层添加MIN/MAX(slot) OVER ()窗口函数；
    // TODO - 后续都统一成使用WindowNode的方式，当前不直接改成WindowNode，因为WindowNode无法下推到Store执行
    bool use_window_node = false;
    if (_ctx->has_window_func ||
            (_select->select_opt != nullptr && _select->select_opt->distinct == true) ||
            (!_agg_funcs.empty() || !_distinct_agg_funcs.empty() || !_group_exprs.empty())) {
        use_window_node = true;
    }
    if (use_window_node) {
        node->set_node_type(pb::WINDOW_EXPR);
    }
    bool new_slot = true;
    // > >=
    if (_ctx->expr_params.func_type == parser::FT_GE
        || _ctx->expr_params.func_type == parser::FT_GT) {
        if (_ctx->expr_params.cmp_type == parser::CMP_ALL) {
            // t1.id > all (select t2.id from t2) -> t1.id > (select max(t2.id) from t2)
            if (_ctx->has_window_func) {
                slots = get_window_func_slot(_select_names[0]);
            } else {
                slots = get_agg_func_slot(_select_names[0], "max", new_slot);
            }
            func->set_name("max");
        } else {
            // t1.id > any (select t2.id from t2) -> t1.id > (select min(t2.id) from t2)
            if (_ctx->has_window_func) {
                slots = get_window_func_slot(_select_names[0]);
            } else {
                slots = get_agg_func_slot(_select_names[0], "min", new_slot);
            }
            func->set_name("min");
        }
    // < <=
    } else if (_ctx->expr_params.func_type == parser::FT_LE
        || _ctx->expr_params.func_type == parser::FT_LT) {
        if (_ctx->expr_params.cmp_type == parser::CMP_ALL) {
            // t1.id < all (select t2.id from t2) -> t1.id < (select min(t2.id) from t2)
            if (_ctx->has_window_func) {
                slots = get_window_func_slot(_select_names[0]);
            } else {
                slots = get_agg_func_slot(_select_names[0], "min", new_slot);
            }
            func->set_name("min");
        } else {
            // t1.id < any (select t2.id from t2) -> t1.id < (select max(t2.id) from t2)
            if (_ctx->has_window_func) {
                slots = get_window_func_slot(_select_names[0]);
            } else {
                slots = get_agg_func_slot(_select_names[0], "max", new_slot);
            }
            func->set_name("max");
        }
    // =
    } else if (_ctx->expr_params.func_type == parser::FT_EQ) {
        // = any (xxx) 改写为 in (xxx)
        return 0;
    // !=
    } else {
        // != all (xxx) 改写为 not in (xxx)
        return 0;
    }
    derive_node->set_tuple_id(slots[0].tuple_id());
    derive_node->set_slot_id(slots[0].slot_id());
    derive_node->set_intermediate_slot_id(slots[0].slot_id());
    _select_exprs[0] = rewrite_expr;
    if (use_window_node) {
        pb::WindowNode window_node;
        window_node.add_func_exprs()->CopyFrom(rewrite_expr);
        _subquery_rewrite_window_nodes.emplace_back(std::move(window_node));
    } else {
        _agg_funcs.emplace_back(rewrite_expr);
    }
    return 0;
}

void SelectPlanner::create_dual_scan_node() {
    pb::PlanNode* scan_node = _ctx->add_plan_node();
    scan_node->set_node_type(pb::DUAL_SCAN_NODE);
    scan_node->set_limit(1);
    scan_node->set_is_explain(_ctx->is_explain);
    scan_node->set_is_get_keypoint(_ctx->is_get_keypoint);
    scan_node->set_num_children(0); 
}

int SelectPlanner::create_limit_node() {
//    if (_select->limit == nullptr && 
    if (_limit_offset.nodes_size() == 0) {
        return 0;
    }
    pb::PlanNode* limit_node = _ctx->add_plan_node();
    limit_node->set_node_type(pb::LIMIT_NODE);
    limit_node->set_limit(-1);
    limit_node->set_is_explain(_ctx->is_explain);
    limit_node->set_num_children(1); //TODO

    pb::DerivePlanNode* derive = limit_node->mutable_derive_node();
    pb::LimitNode* limit = derive->mutable_limit_node();
    if (_limit_offset.nodes_size() > 0) {
        limit->mutable_offset_expr()->CopyFrom(_limit_offset);
        limit->set_offset(0);
    } else {
        limit->set_offset(0);
    }

    if (_limit_count.nodes_size() > 0) {
        limit->mutable_count_expr()->CopyFrom(_limit_count);
    }
    return 0;
}

int SelectPlanner::create_agg_node() {
    if (_select->select_opt != nullptr && _select->select_opt->distinct == true) {
        // select distinct ()xxx, xxx from xx.xx 
        if (_agg_funcs.empty() && _distinct_agg_funcs.empty() && _group_exprs.empty()) {
            //如果没有agg和group by， 将select列加入到group by中
            for (uint32_t idx = 0; idx < _select_exprs.size(); ++idx) {
                _group_exprs.push_back(_select_exprs[idx]);
                // 增加ref_count
                if (inc_slot_ref_cnt(_select_exprs[idx]) != 0) {
                    DB_WARNING("Fail to inc_slot_ref_cnt, expr: %s", _select_exprs[idx].ShortDebugString().c_str());
                    return -1;
                }
            }
        } else if(!_group_exprs.empty()) {
            DB_WARNING("distinct query doesnot support group by");
            return -1;
        }
    }
    if (_agg_funcs.empty() && _distinct_agg_funcs.empty() && _group_exprs.empty()) {
        return 0;
    }
    pb::PlanNode* agg_node = _ctx->add_plan_node();
    agg_node->set_node_type(pb::AGG_NODE);
    if (!_distinct_agg_funcs.empty() || !_orderby_agg_exprs.empty()) {
        agg_node->set_node_type(pb::MERGE_AGG_NODE);
    }
    agg_node->set_limit(-1);
    agg_node->set_is_explain(_ctx->is_explain);
    agg_node->set_num_children(1); //TODO 
    pb::DerivePlanNode* derive = agg_node->mutable_derive_node();
    pb::AggNode* agg = derive->mutable_agg_node();

    for (uint32_t idx = 0; idx < _group_exprs.size(); ++idx) {
        pb::Expr* expr = agg->add_group_exprs();
        expr->CopyFrom(_group_exprs[idx]);
    }
    for (uint32_t idx = 0; idx < _agg_funcs.size(); ++idx) {
        pb::Expr* expr = agg->add_agg_funcs();
        expr->CopyFrom(_agg_funcs[idx]);
    }
    for (uint32_t idx = 0; idx < _distinct_agg_funcs.size(); ++idx) {
        pb::Expr* expr = agg->add_agg_funcs();
        expr->CopyFrom(_distinct_agg_funcs[idx]);
    }
    agg->set_agg_tuple_id(_agg_tuple_id);
    agg->set_arrow_ignore_tuple_id(_order_tuple_id);
        
    if (!_distinct_agg_funcs.empty() || !_orderby_agg_exprs.empty()) {
        pb::PlanNode* agg_node2 = _ctx->add_plan_node();
        agg_node2->set_node_type(pb::AGG_NODE);
        agg_node2->set_limit(-1);
        agg_node2->set_is_explain(_ctx->is_explain);
        agg_node2->set_num_children(1); //TODO 
        pb::DerivePlanNode* derive = agg_node2->mutable_derive_node();
        pb::AggNode* agg2 = derive->mutable_agg_node();

        for (uint32_t idx = 0; idx < _group_exprs.size(); ++idx) {
            pb::Expr* expr = agg2->add_group_exprs();
            expr->CopyFrom(_group_exprs[idx]);
        }

        for (auto& distinct_func : _distinct_agg_funcs) {
            if (distinct_func.nodes(0).fn().name() == "group_concat_distinct") {
                int expr_idx = 2;
                for (int i = 0; i < distinct_func.nodes(1).num_children(); i++) {
                    pb::Expr* expr = agg2->add_group_exprs();
                    ExprNode::get_pb_expr(distinct_func, &expr_idx, expr);
                }
            } else if (!_need_multi_distinct) {
                int expr_idx = 1;
                while (expr_idx < distinct_func.nodes_size()) {
                    pb::Expr* expr = agg2->add_group_exprs();
                    ExprNode::get_pb_expr(distinct_func, &expr_idx, expr);
                }
            } else if (distinct_func.nodes(0).fn().name() == "multi_count_distinct"
                    || distinct_func.nodes(0).fn().name() == "multi_sum_distinct"
                    || distinct_func.nodes(0).fn().name() == "multi_group_concat_distinct") {
                pb::Expr* expr = agg2->add_agg_funcs();
                expr->CopyFrom(distinct_func);
            }
        }

        for (uint32_t idx = 0; idx < _orderby_agg_exprs.size(); ++idx) {
            pb::Expr* expr = agg2->add_group_exprs();
            expr->CopyFrom(_orderby_agg_exprs[idx]);
        }
        for (uint32_t idx = 0; idx < _agg_funcs.size(); ++idx) {
            if (_agg_funcs[idx].nodes(0).fn().name() == "group_concat" &&
                    _agg_funcs[idx].nodes(0).num_children() > 2) {
                int expr_idx = 1; // expr_list
                ExprNode::get_pb_expr(_agg_funcs[idx], &expr_idx, nullptr); // expr_list
                ExprNode::get_pb_expr(_agg_funcs[idx], &expr_idx, nullptr); // separator

                // 保留expr row与separate, 去掉by_expr_row 与 is_desc_row
                pb::Expr* expr = agg2->add_agg_funcs();
                for (int i = 0; i < expr_idx; i++) {
                    expr->add_nodes()->CopyFrom(_agg_funcs[idx].nodes(i));
                }
                expr->mutable_nodes(0)->set_num_children(2); // agg_node

                continue;
            }
            pb::Expr* expr = agg2->add_agg_funcs();
            expr->CopyFrom(_agg_funcs[idx]);
        }
        agg2->set_agg_tuple_id(_agg_tuple_id);
        agg2->set_arrow_ignore_tuple_id(_order_tuple_id);
    }
    return 0;
}

int SelectPlanner::create_window_and_sort_nodes() {
    std::vector<pb::WindowNode> window_nodes;
    window_nodes.reserve(_subquery_rewrite_window_nodes.size() + _window_nodes.size());
    // 非相关子查询重写后，使用WindowNode进行min/max操作
    window_nodes.insert(window_nodes.end(),
                        _subquery_rewrite_window_nodes.begin(), _subquery_rewrite_window_nodes.end());
    window_nodes.insert(window_nodes.end(), 
                        _window_nodes.begin(), _window_nodes.end());
    for (int i = 0; i < window_nodes.size(); ++i) {
        const pb::WindowNode& window_node = window_nodes[i];
        // 创建WindowNode
        pb::PlanNode* plan_node = _ctx->add_plan_node();
        plan_node->set_node_type(pb::WINDOW_NODE);
        plan_node->set_limit(-1);
        if (i < _subquery_rewrite_window_nodes.size()) {
            // 非相关子查询，只取一条，相当于聚合操作
            plan_node->set_limit(1);
        }
        plan_node->set_is_explain(_ctx->is_explain);
        plan_node->set_num_children(1);
        pb::DerivePlanNode* derive = plan_node->mutable_derive_node();
        pb::WindowNode* window = derive->mutable_window_node();
        window->CopyFrom(window_node);
        // 删掉window_spec中order_exprs/is_asc中的partition_exprs部分
        const auto& window_spec = window_node.window_spec();
        if (window_spec.order_exprs().size() != window_spec.is_asc().size()) {
            DB_WARNING("order expr format error");
            return -1;
        }
        window->mutable_window_spec()->clear_order_exprs();
        window->mutable_window_spec()->clear_is_asc();
        for (int i = window_spec.partition_exprs().size(); i < window_spec.order_exprs().size(); ++i) {
            window->mutable_window_spec()->add_order_exprs()->CopyFrom(window_spec.order_exprs(i));
            window->mutable_window_spec()->add_is_asc(window_spec.is_asc(i));
        }
        // 创建SortNode
        // TODO - 可以优化为没有order_exprs时，不创建SortNode，注意separate分离时，SortNode和WindowNode的先后顺序
        plan_node = _ctx->add_plan_node();
        plan_node->set_node_type(pb::SORT_NODE);
        plan_node->set_limit(-1);
        plan_node->set_is_explain(_ctx->is_explain);
        plan_node->set_num_children(1);
        derive = plan_node->mutable_derive_node();
        pb::SortNode* sort = derive->mutable_sort_node();
        for (int i = 0; i < window_spec.order_exprs().size(); ++i) {
            pb::Expr* order_expr = sort->add_order_exprs();
            pb::Expr* slot_order_expr = sort->add_slot_order_exprs();
            order_expr->CopyFrom(window_spec.order_exprs(i));
            slot_order_expr->CopyFrom(window_spec.order_exprs(i));
            sort->add_is_asc(window_spec.is_asc(i));
            sort->add_is_null_first(window_spec.is_asc(i));
        }
        sort->set_tuple_id(window_spec.tuple_id());
    }
    return 0;
}

void SelectPlanner::add_single_table_columns(const std::string& table_name, TableInfo* table_info) {
    for (auto& field : table_info->fields) {
        if (field.deleted) {
            continue;
        }

        pb::Expr select_expr;
        std::string db = table_info->name.substr(0, table_info->name.find("."));
        std::string tbl = table_info->short_name;
        TableInfo* dblink_table_info = get_dblink_table_info_ptr(table_info->id);
        if (dblink_table_info != nullptr) {
            // 外部表，使用主meta的dblink表信息
            db = dblink_table_info->name.substr(0, dblink_table_info->name.find("."));
            tbl = dblink_table_info->short_name;
        }
        select_expr.set_database(db);
        select_expr.set_table(tbl);

        pb::SlotDescriptor slot = get_scan_ref_slot(table_name, table_info->id, field.id, field.type);
        pb::ExprNode* node = select_expr.add_nodes();
        node->set_node_type(pb::SLOT_REF);
        node->set_col_type(field.type);
        node->set_num_children(0);
        node->mutable_derive_node()->set_tuple_id(slot.tuple_id()); //TODO
        node->mutable_derive_node()->set_slot_id(slot.slot_id());
        node->mutable_derive_node()->set_field_id(slot.field_id());
        node->set_col_flag(field.flag);

        std::string& select_name = field.short_name;
        _select_exprs.push_back(select_expr);
        _select_names.push_back(select_name);
//        std::transform(select_name.begin(), select_name.end(), select_name.begin(), ::tolower);
        _ctx->ref_slot_id_mapping[slot.tuple_id()][field.lower_short_name] = slot.slot_id();
        _ctx->field_column_id_mapping[field.lower_short_name] = _column_id++;
    }
}

// TODO: select * from multiple tables or join clause
int SelectPlanner::parse_select_star(parser::SelectField* field) {
    parser::WildCardField* wild_card = field->wild_card;
    if (wild_card->db_name.empty() && wild_card->table_name.empty()) {
        // select * ...
        for (auto& table_name : _table_names) {
            auto table_info = get_table_info_ptr(table_name);
            if (table_info == nullptr) {
                if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                    _ctx->stat_info.error_code = ER_WRONG_TABLE_NAME;
                    _ctx->stat_info.error_msg << "Incorrect table name \'" << table_name << "\'";
                }
                DB_WARNING("no table found for select field: %s", field->to_string().c_str());
                return -1;
            }
            add_single_table_columns(table_name, table_info);
        }
    } else {
        // select db.table.* / table.*
        if (wild_card->table_name.empty()) {
            DB_WARNING("table name is empty");
            return -1;
        }
        std::string table_name = wild_card->table_name.value;
        std::string db_name;
        std::string full_name;
        // try to search alias table
        if (!wild_card->db_name.empty()) {
            db_name = wild_card->db_name.value;
            full_name = db_name + "." + table_name;
        } else {
            //table.field_name
            auto dbs = get_possible_databases(table_name);
            if (dbs.size() == 0) {
                if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                    _ctx->stat_info.error_code = ER_WRONG_TABLE_NAME;
                    _ctx->stat_info.error_msg << "Incorrect table name \'" << table_name << "\'";
                }
                DB_WARNING("no database found for field: %s", table_name.c_str());
                return -1;
            } else if (dbs.size() > 1 && !FLAGS_disambiguate_select_name) {
                if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                    _ctx->stat_info.error_code = ER_AMBIGUOUS_FIELD_TERM;
                    _ctx->stat_info.error_msg << "table  \'" << table_name << "\' is ambiguous";
                }
                DB_WARNING("ambiguous table_name: %s", table_name.c_str());
                return -1;
            }
            full_name = *dbs.begin() + "." + table_name;
        }
        auto table_info = get_table_info_ptr(full_name);
        if (table_info == nullptr) {
            if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                _ctx->stat_info.error_code = ER_WRONG_TABLE_NAME;
                _ctx->stat_info.error_msg << "Incorrect table name \'" << table_name << "\'";
            }
            DB_WARNING("no table found for select field: %s", field->to_string().c_str());
            return -1;
        }
        add_single_table_columns(full_name, table_info);
    }
    return 0;
}

int SelectPlanner::parse_select_field(parser::SelectField* field) {
    pb::Expr select_expr;
    if (field->expr == nullptr) {
        DB_WARNING("field expr is nullptr");
        return -1;
    }
    CreateExprOptions options;
    options.can_agg = true;
    options.is_select_field = true;
    options.max_one_row = true;
    options.can_window = true;

    if (field->expr != nullptr 
            && field->expr->node_type == parser::NT_EXPR
            && static_cast<const parser::ExprNode*>(field->expr)->expr_type == parser::ET_ROW_EXPR) {
        // select (a,b) from xxx
        _ctx->stat_info.error_code = ER_OPERAND_COLUMNS;
        _ctx->stat_info.error_msg << "Operand should contain 1 column(s)";
        return -1;
    }
    if (0 != create_expr_tree(field->expr, select_expr, options)) {
        DB_WARNING("create select expr failed");
        return -1;
    }
    std::string select_name;
    if (parse_select_name(field, select_name) == -1) {
        DB_WARNING("Fail to parse_select_name");
        return -1;
    }
    _select_names.emplace_back(select_name);
    std::transform(select_name.begin(), select_name.end(), select_name.begin(), ::tolower);
    if (!field->as_name.empty()) {
        _select_alias_mapping.insert({select_name, (_select_names.size() - 1)});
    }

    if (_ctx->is_base_subscribe) {
        if (!field->as_name.empty() && field->expr->expr_type == parser::ET_COLUMN) {
            parser::ColumnName* column = static_cast<parser::ColumnName*>(field->expr);
            _ctx->base_subscribe_select_name_alias_map[field->as_name.c_str()] = column->name.c_str();
        }
    }

    _select_exprs.emplace_back(select_expr);
    _ctx->field_column_id_mapping[select_name] = _column_id++;
    return 0;
}

int SelectPlanner::parse_with() {
    if (_select->with == nullptr) {
        return 0;
    }
    parser::WithClause* with = _select->with;
    std::unordered_set<std::string> cte_name_set;
    for (int i = 0; i < with->ctes.size(); ++i) {
        parser::CommonTableExpr* cte = with->ctes[i];
        if (cte == nullptr || cte->query_expr == nullptr) {
            DB_WARNING("cte is nullptr or query_expr is nullptr");
            return -1;
        }
        parser::DmlNode* query_stmt = cte->query_expr->query_stmt;
        pb::SchemaInfo view;
        if (cte_name_set.count(cte->name.value) > 0) {
            _ctx->stat_info.error_code = ER_NONUNIQ_TABLE;
            _ctx->stat_info.error_msg << "Not unique table/alias: \'" << cte->name.value << "\'";
            DB_WARNING("with cte name %s is duplicated", cte->name.value);
            return -1;
        }
        cte_name_set.insert(cte->name.value);
        view.set_table_name(cte->name.value);

        ExprParams expr_params;
        std::shared_ptr<QueryContext> parse_with_ctx = std::make_shared<QueryContext>();
        auto client = _ctx->client_conn;
        parse_with_ctx->stmt = query_stmt;
        parse_with_ctx->expr_params = expr_params;
        parse_with_ctx->stmt_type = query_stmt->node_type;
        parse_with_ctx->cur_db = _ctx->cur_db;
        parse_with_ctx->is_explain = _ctx->is_explain;
        parse_with_ctx->stat_info.log_id = _ctx->stat_info.log_id;
        parse_with_ctx->user_info = _ctx->user_info;
        parse_with_ctx->row_ttl_duration = _ctx->row_ttl_duration;
        parse_with_ctx->is_complex = _ctx->is_complex;
        parse_with_ctx->get_runtime_state()->set_client_conn(client);
        parse_with_ctx->client_conn = client;
        parse_with_ctx->sql = query_stmt->to_string();
        parse_with_ctx->charset = _ctx->charset;
        parse_with_ctx->is_with = true;
        std::unique_ptr<LogicalPlanner> planner;
        planner.reset(new SelectPlanner(parse_with_ctx.get()));
        if (0 != planner->parse_view_select(query_stmt, 
                                    cte->column_names,
                                    view)) {
            DB_WARNING("parse view select failed");
            _ctx->stat_info.error_code = parse_with_ctx->stat_info.error_code;
            _ctx->stat_info.error_msg << parse_with_ctx->stat_info.error_msg.str();
            return -1;
        }
        _ctx->table_with_clause_mapping[view.table_name()] = view.view_select_stmt();
    }
    return 0;
}

// The ALL and DISTINCT modifiers specify whether duplicate rows should be returned. 
// ALL (the default) specifies that all matching rows should be returned, including duplicates. 
// DISTINCT specifies removal of duplicate rows from the result set. 
// It is an error to specify both modifiers. 
// DISTINCTROW is a synonym for DISTINCT.
// ref: https://dev.mysql.com/doc/refman/5.7/en/select.html
int SelectPlanner::parse_select_fields() {
    parser::Vector<parser::SelectField*> fields = _select->fields;
    for (int idx = 0; idx < fields.size(); ++idx) {
        if (fields[idx] == nullptr) {
            DB_WARNING("cur_item->data is nullptr");
            return -1;
        }
        if (fields[idx]->wild_card != nullptr) {
            if (-1 == parse_select_star(fields[idx])) {
                return -1;
            }
        } else {
            if (0 != parse_select_field(fields[idx])) {
                return -1;
            }
        }
    }
    return 0;
}

int SelectPlanner::parse_where() {
    // 隐式__snapshot__条件，用于olap快照回滚，暂时没用
    add_snapshot_blacklist_to_where_filters();
    if (_select->where == nullptr) {
        return 0;
    }
    _where_filters.reserve(8);
    if (0 != flatten_filter(_select->where, _where_filters, CreateExprOptions())) {
        DB_WARNING("flatten_filter failed");
        return -1;
    }
    return 0;
}

int SelectPlanner::_parse_having() {
    if (_select->having == nullptr) {
        return 0;
    }
    CreateExprOptions options;
    options.can_agg = true;
    options.use_alias = true;
    if (0 != flatten_filter(_select->having, _having_filters, options)) {
        DB_WARNING("flatten_filter failed");
        return -1;
    }
    return 0;
}

int SelectPlanner::parse_groupby() {
    if (_select->group == nullptr) {
        return 0;
    }
    parser::Vector<parser::ByItem*> by_items = _select->group->items;
    CreateExprOptions options;
    options.use_alias = true;
    options.can_agg = false;
    for (int idx = 0; idx < by_items.size(); ++idx) {
        if (by_items[idx]->node_type != parser::NT_BY_ITEM) {
            DB_WARNING("un-supported group-by item type: %d", by_items[idx]->node_type);
            return -1;
        }
        // create group by expr node
        pb::Expr group_expr;
        int ret = 0;
        if (0 != (ret =  create_expr_tree(by_items[idx]->expr, group_expr, options))) {
            DB_WARNING("create group expr failed");
            if (ret == -3) {
                _ctx->stat_info.error_code = ER_WRONG_GROUP_FIELD;
                _ctx->stat_info.error_msg.str("");
                _ctx->stat_info.error_msg << "Can't group on '"<< by_items[idx]->expr->to_string() << "'";
            }
            return -1;
        }
        _group_exprs.push_back(group_expr);
        // creat slot ref node (referring the group by expr temp result slot)
        // auto& slot = _get_group_expr_slot();
        // slot.set_slot_type(group_expr.nodes(0).col_type());
    }
    return 0;
}

int SelectPlanner::parse_orderby() {
    if (_select->order == nullptr) {
        DB_DEBUG("orderby is null");
        return 0;
    }
    return create_orderby_exprs(_select->order);
}

int SelectPlanner::parse_limit() {
    if (_select->limit == nullptr) {
        return 0;
    }
    parser::LimitClause* limit = _select->limit;
    if (limit->offset != nullptr && 0 != create_expr_tree(limit->offset, _limit_offset, CreateExprOptions())) {
        DB_WARNING("create limit offset expr failed");
        return -1;
    }
    if (limit->count != nullptr && 0 != create_expr_tree(limit->count, _limit_count, CreateExprOptions())) {
        DB_WARNING("create limit count expr failed");
        return -1;
    }
    return 0;
}

void SelectPlanner::create_agg_tuple_desc() {
    if (_agg_tuple_id == -1) {
        return;
    }
    // slot_id => slot desc mapping
    std::map<int32_t, pb::SlotDescriptor> id_slot_mapping;
    
    pb::TupleDescriptor agg_tuple;
    agg_tuple.set_tuple_id(_agg_tuple_id);
    for (auto& iter : _agg_slot_mapping) {
        //reorder the slot descriptors by slot id
        for (auto& slot : iter.second) {
            id_slot_mapping.insert(std::make_pair(slot.slot_id(), slot));
        }
    }
    for (auto& id_slot : id_slot_mapping) {
        const pb::SlotDescriptor& desc = id_slot.second;
        pb::SlotDescriptor* slot = agg_tuple.add_slots();
        slot->CopyFrom(desc);
    }
    _ctx->add_tuple(agg_tuple);
    return;
}

int SelectPlanner::get_base_subscribe_scan_ref_slot() {
    auto pk_field_info_ptr = _factory->get_index_info_ptr(_ctx->base_subscribe_table_id);
    if (pk_field_info_ptr == nullptr) {
        DB_WARNING("Fail to get_index_info_ptr, index_id: %ld", _ctx->base_subscribe_table_id);
        return -1;
    }
    for (const auto& field_info : pk_field_info_ptr->fields) {
        get_scan_ref_slot(_ctx->base_subscribe_table_name, field_info.table_id, field_info.id, field_info.type);
    }

    auto table_info_ptr = _factory->get_table_info_ptr(_ctx->base_subscribe_table_id);
    if (table_info_ptr == nullptr) {
        DB_WARNING("Fail to get_table_info_ptr, table_id: %ld", _ctx->base_subscribe_table_id);
        return -1;
    }
    for (const auto& field_info : table_info_ptr->fields) {
        if (field_info.short_name == _ctx->base_subscribe_filter_field) {
            get_scan_ref_slot(_ctx->base_subscribe_table_name, field_info.table_id, field_info.id, field_info.type);
            break;
        }
    }
    return 0;
}

void SelectPlanner::add_snapshot_blacklist_to_where_filters() {
    auto tables = get_possible_tables("__snapshot__");
    for (auto table_name : tables) {
        TableInfo* table_ptr = get_table_info_ptr(table_name);
        int sb_size = table_ptr->snapshot_blacklist.size();
        if (sb_size == 0) {
            continue;
        }
        FieldInfo snapshot_field;
        for (const FieldInfo& f : table_ptr->fields) {
            if (f.short_name == "__snapshot__") {
                snapshot_field = f;
                break;
            }
        }
        // __snapshot__ not in (xx, xx)
        pb::Expr root;
        pb::ExprNode* not_node = root.add_nodes();
        not_node->set_node_type(pb::NOT_PREDICATE);
        not_node->set_col_type(pb::BOOL);
        not_node->set_num_children(1);
        pb::Function* not_func = not_node->mutable_fn();
        not_func->set_fn_op(parser::FT_LOGIC_NOT);
        not_func->set_name("logic_not");

        pb::ExprNode* in_predicate_node = root.add_nodes();
        in_predicate_node->set_node_type(pb::IN_PREDICATE);
        in_predicate_node->set_col_type(pb::BOOL);
        in_predicate_node->set_num_children(1 + sb_size);
        pb::Function* func = in_predicate_node->mutable_fn();
        func->set_fn_op(parser::FT_IN);
        func->set_name("in");

        pb::ExprNode* slot_ref_node = root.add_nodes();
        slot_ref_node->set_node_type(pb::SLOT_REF);
        slot_ref_node->set_col_type(snapshot_field.type);
        slot_ref_node->set_num_children(0);
        pb::SlotDescriptor slot = get_scan_ref_slot(table_name, table_ptr->id, snapshot_field.id, snapshot_field.type);
        slot_ref_node->mutable_derive_node()->set_tuple_id(slot.tuple_id());
        slot_ref_node->mutable_derive_node()->set_slot_id(slot.slot_id());
        slot_ref_node->mutable_derive_node()->set_field_id(slot.field_id());
        slot_ref_node->set_col_flag(snapshot_field.flag);
        _ctx->ref_slot_id_mapping[slot.tuple_id()][snapshot_field.lower_short_name] = slot.slot_id();

        for (uint64_t snapshot : table_ptr->snapshot_blacklist) {
            pb::ExprNode* literal_node = root.add_nodes();
            literal_node->set_node_type(pb::INT_LITERAL);
            literal_node->set_col_type(pb::UINT64);
            literal_node->set_num_children(0);
            literal_node->mutable_derive_node()->set_int_val(snapshot);
        }
        std::vector<std::string> vec;
        boost::split(vec, table_ptr->name, boost::is_any_of("."));
        if (vec.size() != 2) {
            continue;
        }
        root.set_database(vec[0]);
        root.set_table(vec[1]);
        _where_filters.push_back(root);
    }
    return;
}

int SelectPlanner::plan_cache_get() {
    if (!enable_plan_cache()) {
        return 0;
    }
    _ctx->stat_info.hit_cache = false;
    const auto& client = _ctx->client_conn;
    if (client == nullptr) {
        DB_WARNING("client is nullptr");
        return -1;
    }
    if (client->user_info == nullptr) {
        DB_WARNING("client->user_info is nullptr");
        return -1;
    }

    // SQL参数化
    std::ostringstream os;
    parser::PlanCacheParam cache_param;
    cache_param.parser_placeholders.reserve(5000);
    _ctx->stmt->set_cache_param(&cache_param);
    _ctx->stmt->to_stream(os);
    _ctx->stmt->set_cache_param(nullptr);

    _ctx->cache_key.namespace_name = client->user_info->namespace_;
    _ctx->cache_key.db_name = _ctx->cur_db;
    _ctx->cache_key.parameterized_sql = std::move(os.str());

    std::shared_ptr<QueryContext> cache_ctx;
    if (client->non_prepared_plans.find(_ctx->cache_key, &cache_ctx) == 0) {
        if (cache_ctx == nullptr) {
            DB_WARNING("cache_ctx is nullptr");
            return -1;
        }
        if (cache_ctx->root != nullptr && cache_ctx->root->has_optimized()) {
            // 优化过的计划，认为缓存失效
            return 0;
        }
        if (cache_ctx->use_backup) {
            if (!MetaServerInteract::get_backup_instance()->is_inited()) {
                // 认为缓存失效
                DB_WARNING("MetaServerInteract is not inited");
                return 0;
            }
            SchemaFactory::use_backup.set_bthread_local(true);
        } else {
            SchemaFactory::use_backup.set_bthread_local(false);
        }
        _factory = SchemaFactory::get_instance();
        bool is_cache_invalid = false;
        for (const auto& kv : cache_ctx->table_version_map) {
            const int64_t table_id = kv.first;
            const int64_t table_version = kv.second;
            SmartTable tbl_ptr = _factory->get_table_info_ptr(table_id);
            if (tbl_ptr == nullptr) {
                DB_WARNING("tbl_ptr is nullptr, table_id: %ld", table_id);
                is_cache_invalid = true;
                break;
            }
            const int64_t cur_table_version = tbl_ptr->version;
            if (table_version != cur_table_version) {
                is_cache_invalid = true;
                break;
            }
        }
        if (!is_cache_invalid) {
            _ctx->stat_info.hit_cache = true;
            if (!cache_ctx->has_find_placeholder) {
                cache_ctx->has_find_placeholder = true;
                if (cache_ctx->root == nullptr) {
                    DB_WARNING("cache_ctx->root is nullptr");
                    return -1;
                }
                cache_ctx->root->find_place_holder(cache_ctx->placeholders);
            }
            if (_ctx->copy_query_context(cache_ctx.get()) != 0) {
                DB_WARNING("Fail to copy_query_context_for_plan_cache, %s", _ctx->sql.c_str());
                return -1;
            }
            if (fill_placeholders(cache_ctx->placeholders, cache_param.parser_placeholders) != 0) {
                DB_WARNING("Fail to fill_placeholders, %s", _ctx->sql.c_str());
                return -1;
            }  
            // 输出的字段名重新生成
            if (replace_select_names() != 0) {
                DB_WARNING("Fail to replace_select_names, %s", _ctx->sql.c_str());
                return -1;
            }
            if (generate_sql_sign(_ctx, _ctx->stmt) < 0) {
                DB_WARNING("Fail to generate_sql_sign, %s", _ctx->sql.c_str());
                return -1;
            }
            return 0;
        }
    }

    return 0;
}

int SelectPlanner::plan_cache_add() {
    if (!enable_plan_cache()) {
        return 0;
    }
    const auto& client = _ctx->client_conn;
    if (client == nullptr) {
        DB_WARNING("client is nullptr");
        return -1;
    }
    if (_ctx->create_plan_tree() < 0) {
        DB_FATAL_CLIENT(client, "Failed to pb_plan to execnode: %s", _ctx->sql.c_str());
        return -1;
    }
    _ctx->is_plan_cache = true;
    client->non_prepared_plans.add(_ctx->cache_key, client->query_ctx);
    return 0;
}

int SelectPlanner::replace_select_names() {
    _select = (parser::SelectStmt*)_ctx->stmt;
    if (_select == nullptr) {
        DB_WARNING("SelectStmt is nullptr");
        return -1;
    }
    PacketNode* packet_node = static_cast<PacketNode*>(_ctx->root->get_node(pb::PACKET_NODE));
    if (packet_node == nullptr) {
        DB_WARNING("packet_node is nullptr");
        return -1;
    }

    // 获取包含placeholder的projection
    std::vector<ExprNode*>& projections = packet_node->mutable_projections();
    std::vector<int> projections_idx_vec;
    for (int i = 0; i < projections.size(); ++i) {
        if (projections[i] == nullptr) {
            DB_WARNING("packet_node is nullptr");
            return -1;
        }
        std::unordered_multimap<int, ExprNode*> placeholders;
        projections[i]->find_place_holder(placeholders);
        if (placeholders.empty()) {
            continue;
        }
        projections_idx_vec.emplace_back(i);
    }

    // 获取包含placeholder的SelectField
    const parser::Vector<parser::SelectField*>& parser_fields = _select->fields;
    std::vector<int> parser_idx_vec;
    for (int i = 0; i < parser_fields.size(); ++i) {
        if (parser_fields[i] == nullptr) {
            DB_WARNING("parser_fields[%d] is nullptr", i);
            return -1;
        }
        if (parser_fields[i]->wild_card != nullptr) {
            continue;
        }
        std::unordered_set<int> parser_placeholders;
        parser_fields[i]->find_placeholder(parser_placeholders);
        if (parser_placeholders.empty()) {
            continue;
        }
        parser_idx_vec.emplace_back(i);
    }

    if (projections_idx_vec.size() != parser_idx_vec.size()) {
        DB_WARNING("projections_idx_vec.size: %d != parser_idx_vec.size: %d", 
                    (int)projections_idx_vec.size(), (int)parser_idx_vec.size());
        return -1;
    }

    // 替换PacketNode中的Field
    std::vector<ResultField>& fields = packet_node->mutable_fields();
    for (int i = 0; i < parser_idx_vec.size(); ++i) {
        std::string select_name;
        const int parser_field_idx = parser_idx_vec[i];
        if (parse_select_name(parser_fields[parser_field_idx], select_name) != 0) {
            DB_WARNING("Fail to parse_select_name");
            return -1;
        }
        const int field_idx = projections_idx_vec[i];
        if (field_idx >= fields.size()) {
            DB_WARNING("fields_idx: %d is bigger than fields.size: %d", field_idx, (int)fields.size());
            return -1;
        }
        fields[field_idx].name = select_name;
        fields[field_idx].org_name = select_name;
    }
    return 0;
}

int SelectPlanner::check_multi_distinct() {
    int multi_distinct_cnt = 0;
    bool multi_col_single_child = false;
    std::set<std::string> name_set;
    check_multi_distinct_in_select(multi_distinct_cnt, 
                                multi_col_single_child,
                                name_set);
    check_multi_distinct_in_having(multi_distinct_cnt, 
                                multi_col_single_child,
                                name_set);
    check_multi_distinct_in_orderby(multi_distinct_cnt, 
                                multi_col_single_child,
                                name_set);
    if (multi_distinct_cnt > 1) {
        if (multi_col_single_child) {
            _ctx->stat_info.error_msg << "The query contains multi count/sum/group_concat distinct, each can't have multi columns.";
            return -1;
        }
        if (name_set.size() > 1) {
            _need_multi_distinct = true;
        }
    }
    return 0;
}

void SelectPlanner::check_multi_distinct_in_select(int& multi_distinct_cnt, 
                                                    bool& multi_col_single_child,
                                                    std::set<std::string>& name_set) {
    for (int idx = 0; idx < _select->fields.size(); ++idx) {
        const parser::ExprNode* expr_item = _select->fields[idx]->expr;
        check_multi_distinct_in_node(expr_item,
                            multi_distinct_cnt,
                            multi_col_single_child,
                            name_set);
    }
}

void SelectPlanner::check_multi_distinct_in_having(int& multi_distinct_cnt, 
                                                    bool& multi_col_single_child,
                                                    std::set<std::string>& name_set) {
    if (_select->having == nullptr) {
        return;
    }
    check_multi_distinct_in_node(_select->having,
                            multi_distinct_cnt,
                            multi_col_single_child,
                            name_set);
}

void SelectPlanner::check_multi_distinct_in_orderby(int& multi_distinct_cnt, 
                                                    bool& multi_col_single_child,
                                                    std::set<std::string>& name_set) {
    if (_select->order == nullptr) {
        return;
    }
    parser::Vector<parser::ByItem*> order_items = _select->order->items;
    for (int idx = 0; idx < order_items.size(); ++idx) {
        check_multi_distinct_in_node(order_items[idx]->expr,
                            multi_distinct_cnt,
                            multi_col_single_child,
                            name_set);
    }
}

void SelectPlanner::check_multi_distinct_in_node(const parser::ExprNode* item, 
                                                    int& multi_distinct_cnt, 
                                                    bool& multi_col_single_child,
                                                    std::set<std::string>& name_set) {
    if (item == nullptr) {
        return;
    }
    for (int i = 0; i < item->children.size(); i++) {
        const parser::ExprNode* expr_item = (const parser::ExprNode*) item->children[i];
        check_multi_distinct_in_node(expr_item, multi_distinct_cnt, multi_col_single_child, name_set);
    }
    if (item->expr_type == parser::ET_FUNC) {
        parser::FuncExpr* func = (parser::FuncExpr*) item;
        if (func->distinct == true && 
                (func->fn_name.to_lower() == "sum" || func->fn_name.to_lower() == "count")) {
            multi_distinct_cnt ++;
            if (func->children.size() > 1) {
                multi_col_single_child = true;
            }
            if (func->children.size() == 1) {
                const parser::ExprNode* expr_item = (const parser::ExprNode*) func->children[0];
                std::ostringstream os;
                expr_item->to_stream(os);
                name_set.insert(os.str());
            }
        } else if (func->distinct == true && func->fn_name.to_lower() == "group_concat") {
            multi_distinct_cnt ++;
            if (func->children.size() == 2) {
                const parser::ExprNode* expr_item = (const parser::ExprNode*) func->children[0];
                if (expr_item->children.size() != 1) {
                    multi_col_single_child = true;
                    return;
                }
            }
            if (func->children.size() == 4) {
                const parser::ExprNode* expr_item1 = (const parser::ExprNode*) func->children[0];
                const parser::ExprNode* expr_item2 = (const parser::ExprNode*) func->children[2];
                // 不允许多列 group_concat(distinct col1,col2 order by col2)
                // 不允许多列 group_concat(distinct col1 order by col1, col2)
                if (expr_item1->children.size() != expr_item2->children.size()) {
                    multi_col_single_child = true;
                    return;
                }

                // 不允许多列 group_concat(distinct col1 order by col2)
                std::ostringstream os1;
                expr_item1->to_stream(os1);
                std::ostringstream os2;
                expr_item2->to_stream(os2);
                if (os1.str() != os2.str()) {
                    multi_col_single_child = true;
                    return;
                }
            }
            std::ostringstream os;
            func->to_stream(os);
            name_set.insert(os.str());
        }
    }
}

int SelectPlanner::merge_window_node() {
    if (_window_nodes.empty()) {
        return 0;
    }
    if (_window_nodes.size() != _window_specs.size()) {
        DB_WARNING("window_nodes.size[%lu] != _window_specs.size[%lu]", _window_nodes.size(), _window_specs.size());
        return -1;
    }
    std::vector<pb::WindowNode> window_nodes_tmp;
    _window_nodes.swap(window_nodes_tmp);

    std::unordered_map<std::string, std::vector<pb::WindowNode>> window_node_mapping;
    for (int i = 0; i < window_nodes_tmp.size(); ++i) {
        window_node_mapping[_window_specs[i]].emplace_back(std::move(window_nodes_tmp[i]));
    }
    for (auto& kv : window_node_mapping) {
        bool has_set_window_spec = false;
        pb::WindowNode merge_window_node;
        for (auto& window_node : kv.second) {
            for (auto& func_expr : *window_node.mutable_func_exprs()) {
                merge_window_node.add_func_exprs()->Swap(&func_expr);
            }
            if (!has_set_window_spec) {
                merge_window_node.mutable_window_spec()->Swap(window_node.mutable_window_spec());
                has_set_window_spec = true;
            }
        }
        _window_nodes.emplace_back(std::move(merge_window_node));
    }
    return 0;
}

int SelectPlanner::create_window_tuple_desc() {
    // window tuple
    if (_window_tuple_id == -1) {
        return 0;
    }
    // slot_id => slot desc mapping
    std::map<int32_t, pb::SlotDescriptor> id_slot_mapping;
    pb::TupleDescriptor window_tuple;
    window_tuple.set_tuple_id(_window_tuple_id);
    for (const auto& [_, slots] : _window_slot_mapping) {
        // reorder the slot descriptors by slot id
        for (auto& slot : slots) {
            id_slot_mapping.insert(std::make_pair(slot.slot_id(), slot));
        }
    }
    for (auto& [_, slot] : id_slot_mapping) {
        *window_tuple.add_slots() = slot;
    }
    _ctx->add_tuple(window_tuple);
    // window sort tuple
    for (const auto& window_node : _window_nodes) {
        if (window_node.has_window_spec() && 
                window_node.window_spec().has_tuple_id() &&
                window_node.window_spec().tuple_id() != -1) {
            const int32_t order_tuple_id = window_node.window_spec().tuple_id();
            if (_window_sort_slot_mapping.find(order_tuple_id) == _window_sort_slot_mapping.end()) {
                DB_WARNING("Fail to find order_tuple_id, %d", order_tuple_id);
                return -1;
            }
            pb::TupleDescriptor order_tuple;
            order_tuple.set_tuple_id(order_tuple_id);
            for (const auto& slot : _window_sort_slot_mapping[order_tuple_id]) {
                *order_tuple.add_slots() = slot;
            }
            _ctx->add_tuple(order_tuple);
        }
    }
    return 0;
}

// pb::SlotDescriptor& SelectPlanner::_get_group_expr_slot() {
//     if (_group_tuple_id == -1) {
//         _group_tuple_id = _plan_table_ctx->tuple_cnt++;
//     }
//     _group_slots.push_back(pb::SlotDescriptor());
//     pb::SlotDescriptor& slot = _group_slots.back();
//     slot.set_tuple_id(_group_tuple_id);
//     slot.set_slot_id(_group_slot_cnt++);
//     slot.set_slot_type(pb::INVALID_TYPE);
//     return slot;
// }

// void SelectPlanner::_create_group_tuple_desc() {
//     if (_group_tuple_id == -1) {
//         return;
//     }
//     group_tuple.set_tuple_id(_group_tuple_id);
//     for (auto& iter : _group_slots) {
//         pb::SlotDescriptor* slot = group_tuple.add_slots();
//         slot->CopyFrom(iter);
//     }
//     _ctx->add_tuple(group_tuple);
//     return;
// }

} // namespace bailaldb
