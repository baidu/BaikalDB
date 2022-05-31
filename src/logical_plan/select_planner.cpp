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
    if (_select->table_refs == nullptr) {
        if (0 != parse_select_fields()) {
            return -1;        
        }
        if (_agg_funcs.empty() && _distinct_agg_funcs.empty() && _group_exprs.empty()) {
            create_packet_node(pb::OP_SELECT);
            create_dual_scan_node();
        } else {
            create_agg_tuple_desc();
            create_packet_node(pb::OP_SELECT);
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

    // parse from
    if (0 != parse_db_tables(_select->table_refs, &_join_root)) {
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

    create_scan_tuple_descs();
    create_agg_tuple_desc();
    create_order_by_tuple_desc();
    //print_debug_log();
    //_create_group_tuple_desc();

    if (is_full_export()) {
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
    //代价信息统计时不走full export流程
    if (_ctx->explain_type != EXPLAIN_NULL) {
        return false;
    }
    if (_ctx->has_derived_table || _ctx->has_information_schema) {
        return false;
    }
    if (_select->where != nullptr) {
        if (!is_fullexport_condition()) {
            return false;
        }
    } 
    if (_select->group != nullptr) {
        return false;
    } 
    if (_select->having != nullptr) {
        return false;
    } 
    if (_select->order != nullptr) {
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
    if (conjuncts.size() > 2 || _select->limit == nullptr) {
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
            auto it = _ctx->derived_table_ctx_mapping.find(iter_out.first);
            if (it == _ctx->derived_table_ctx_mapping.end()) {
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

int SelectPlanner::subquery_rewrite() {
    if (!_ctx->expr_params.is_expr_subquery) {
        return 0;
    }
    // 相关子查询判断，current_tuple_ids包含当前sql涉及的slot的tuple
    for (auto tuple_id : _ctx->current_tuple_ids) {
        if (_ctx->current_table_tuple_ids.count(tuple_id) == 0) {
            _ctx->expr_params.is_correlated_subquery = true;
            return 0;
        }
    }
    // any/all只支持单值
    if (_select_exprs.size() != 1) {
        return 0;
    }
    pb::Expr expr = _select_exprs[0];
    pb::Expr agg_expr;
    // create agg expr node
    pb::ExprNode* node = agg_expr.add_nodes();
    node->set_node_type(pb::AGG_EXPR);
    node->set_col_type(pb::INVALID_TYPE);
    pb::Function* func = node->mutable_fn();
    func->set_fn_op(_ctx->expr_params.func_type);
    func->set_has_var_args(false);
    node->set_num_children(1);
    for (auto& old_node : expr.nodes()) {
        agg_expr.add_nodes()->CopyFrom(old_node);
    }
    pb::DeriveExprNode* derive_node = node->mutable_derive_node();
    std::vector<pb::SlotDescriptor> slots;
    // > >=
    if (_ctx->expr_params.func_type == parser::FT_GE
        || _ctx->expr_params.func_type == parser::FT_GT) {
        if (_ctx->expr_params.cmp_type == parser::CMP_ALL) {
            // t1.id > all (select t2.id from t2) -> t1.id > (select max(t2.id) from t2)
            bool new_slot = true;
            slots = get_agg_func_slot(_select_names[0], "max", new_slot);
            func->set_name("max");
        } else {
            // t1.id > any (select t2.id from t2) -> t1.id > (select min(t2.id) from t2)
            bool new_slot = true;
            slots = get_agg_func_slot(_select_names[0], "min", new_slot);
            func->set_name("min");
        }
    // < <=
    } else if (_ctx->expr_params.func_type == parser::FT_LE
        || _ctx->expr_params.func_type == parser::FT_LT) {
        if (_ctx->expr_params.cmp_type == parser::CMP_ALL) {
            // t1.id < all (select t2.id from t2) -> t1.id < (select min(t2.id) from t2)
            bool new_slot = true;
            slots = get_agg_func_slot(_select_names[0], "min", new_slot);
            func->set_name("min");
        } else {
            // t1.id < any (select t2.id from t2) -> t1.id < (select max(t2.id) from t2)
            bool new_slot = true;
            slots = get_agg_func_slot(_select_names[0], "max", new_slot);
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
    _select_exprs[0] = agg_expr;
    _agg_funcs.emplace_back(agg_expr);
    return 0;
}

void SelectPlanner::create_dual_scan_node() {
    pb::PlanNode* scan_node = _ctx->add_plan_node();
    scan_node->set_node_type(pb::DUAL_SCAN_NODE);
    scan_node->set_limit(1);
    scan_node->set_is_explain(_ctx->is_explain);
    scan_node->set_num_children(0); 
}

int SelectPlanner::create_limit_node() {
    if (_select->limit == nullptr) {
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
        // select distinct ()xxx, xxx from xx.xx (no group by)
        if (!_agg_funcs.empty() || !_distinct_agg_funcs.empty() || !_group_exprs.empty()) {
            DB_WARNING("distinct query doesnot support group by");
            return -1;
        }
        pb::PlanNode* agg_node = _ctx->add_plan_node();
        agg_node->set_node_type(pb::AGG_NODE);
        agg_node->set_limit(-1);
        agg_node->set_is_explain(_ctx->is_explain);
        agg_node->set_num_children(1); //TODO 
        pb::DerivePlanNode* derive = agg_node->mutable_derive_node();
        pb::AggNode* agg = derive->mutable_agg_node();

        for (uint32_t idx = 0; idx < _select_exprs.size(); ++idx) {
            pb::Expr* expr = agg->add_group_exprs();
            expr->CopyFrom(_select_exprs[idx]);
//            if (_select_exprs[idx].nodes_size() != 1) {
//                DB_WARNING("invalid distinct expr");
//                return -1;
//            }
//            expr->add_nodes()->CopyFrom(_select_exprs[idx].nodes(0));
        }
        agg->set_agg_tuple_id(-1);
        return 0;
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
        for (uint32_t idx = 0; idx < _distinct_agg_funcs.size(); ++idx) {
            if (_distinct_agg_funcs[idx].nodes(0).fn().name() == "group_concat_distinct") {
                int expr_idx = 2;
                for (int i = 0; i < _distinct_agg_funcs[idx].nodes(1).num_children(); i++) {
                    pb::Expr* expr = agg2->add_group_exprs();
                    ExprNode::get_pb_expr(_distinct_agg_funcs[idx], &expr_idx, expr);
                }
                continue;
            }
            int expr_idx = 1;
            while (expr_idx < _distinct_agg_funcs[idx].nodes_size()) {
                pb::Expr* expr = agg2->add_group_exprs();
                ExprNode::get_pb_expr(_distinct_agg_funcs[idx], &expr_idx, expr);
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
    }
    return 0;
}

void SelectPlanner::add_single_table_columns(const std::string& table_name, TableInfo* table_info) {
    for (auto& field : table_info->fields) {
        if (field.deleted) {
            continue;
        }

        pb::SlotDescriptor slot = get_scan_ref_slot(table_name, table_info->id, field.id, field.type);
        pb::Expr select_expr;
        select_expr.set_database(table_info->name.substr(0, table_info->name.find(".")));
        select_expr.set_table(table_info->short_name);
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
    if (0 != create_expr_tree(field->expr, select_expr, options)) {
        DB_WARNING("create select expr failed");
        return -1;
    }
    std::string select_name;
    if (!field->as_name.empty()) {
        select_name = field->as_name.value;
        _select_names.emplace_back(select_name);
        std::transform(select_name.begin(), select_name.end(), select_name.begin(), ::tolower);
        _select_alias_mapping.insert({select_name, (_select_names.size() - 1)});
    } else {
        if (field->expr->expr_type == parser::ET_COLUMN) {
            parser::ColumnName* column = static_cast<parser::ColumnName*>(field->expr);
            select_name = column->name.c_str();
        } else if (!field->org_name.empty()) {
            select_name = field->org_name.c_str();
        } else {
            select_name = field->expr->to_string();
        }
        _select_names.emplace_back(select_name);
        std::transform(select_name.begin(), select_name.end(), select_name.begin(), ::tolower);
    }
    
    _select_exprs.emplace_back(select_expr);
    _ctx->field_column_id_mapping[select_name] = _column_id++;
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
    if (_select->where == nullptr) {
        return 0;
    }
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
    for (int idx = 0; idx < by_items.size(); ++idx) {
        if (by_items[idx]->node_type != parser::NT_BY_ITEM) {
            DB_WARNING("un-supported group-by item type: %d", by_items[idx]->node_type);
            return -1;
        }
        // create group by expr node
        pb::Expr group_expr;
        if (0 != create_expr_tree(by_items[idx]->expr, group_expr, options)) {
            DB_WARNING("create group expr failed");
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
