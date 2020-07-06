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
#include "dual_scan_node.h"
#include "network_socket.h"
#include <boost/algorithm/string.hpp>

namespace baikaldb {

int SelectPlanner::plan() {
    if (!_ctx->stmt) {
        DB_WARNING("no sql command set");
        return -1;
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

    create_scan_tuple_descs();
    create_agg_tuple_desc();
    create_order_by_tuple_desc();
    //print_debug_log();
    //_create_group_tuple_desc();

    if (is_full_export()) {
        _ctx->is_full_export = true;
    }

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
    // greate_filter_node
    if (0 != create_filter_node(_where_filters, pb::WHERE_FILTER_NODE)) {
        return -1;
    }
    // join节点的叶子节点是scan_node
    if (0 != create_join_and_scan_nodes(_join_root)) {
        return -1;
    }
    auto client = _ctx->client_conn;
    if (client->txn_id == 0) {
        _ctx->get_runtime_state()->set_single_sql_autocommit(true);
    } else {
        _ctx->get_runtime_state()->set_single_sql_autocommit(false);
    }
    return 0;
}

bool SelectPlanner::is_full_export() {
    //代价信息统计时不走full export流程
    if (_ctx->explain_type != EXPLAIN_NULL) {
        return false;
    }
    if (_select->where != nullptr) {
        return false;
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
            if (_select_exprs[idx].nodes_size() != 1) {
                DB_WARNING("invalid distinct expr");
                return -1;
            }
            expr->add_nodes()->CopyFrom(_select_exprs[idx].nodes(0));
        }
        agg->set_agg_tuple_id(-1);
        return 0;
    }
    if (_agg_funcs.empty() && _distinct_agg_funcs.empty() && _group_exprs.empty()) {
        return 0;
    }
    pb::PlanNode* agg_node = _ctx->add_plan_node();
    agg_node->set_node_type(pb::AGG_NODE);
    if (!_distinct_agg_funcs.empty()) {
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

    if (!_distinct_agg_funcs.empty()) {
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
            for (int expr_idx = 1; expr_idx < _distinct_agg_funcs[idx].nodes_size(); expr_idx++) {
                pb::Expr* expr = agg2->add_group_exprs();
                expr->add_nodes()->CopyFrom(_distinct_agg_funcs[idx].nodes(expr_idx));
            }
        }
        for (uint32_t idx = 0; idx < _agg_funcs.size(); ++idx) {
            pb::Expr* expr = agg2->add_agg_funcs();
            expr->CopyFrom(_agg_funcs[idx]);
        }
        agg2->set_agg_tuple_id(_agg_tuple_id);
    }
    return 0;
}

void SelectPlanner::add_single_table_columns(TableInfo* table_info) {
    for (uint32_t idx = 0; idx < table_info->fields.size(); ++idx) {
        auto field = table_info->fields[idx];
        if (field.deleted) {
            continue;
        }
        std::string& field_name = field.name;
        std::vector<std::string> items;
        boost::split(items, field_name, boost::is_any_of("."));

        pb::SlotDescriptor slot = get_scan_ref_slot(table_info->id, field.id, field.type);
        pb::Expr select_expr;
        pb::ExprNode* node = select_expr.add_nodes();
        node->set_node_type(pb::SLOT_REF);
        node->set_col_type(field.type);
        node->set_num_children(0);
        node->mutable_derive_node()->set_tuple_id(slot.tuple_id()); //TODO
        node->mutable_derive_node()->set_slot_id(slot.slot_id());
        node->mutable_derive_node()->set_field_id(slot.field_id());

        std::string& select_name = items[items.size() - 1];
        _select_exprs.push_back(select_expr);
        _select_names.push_back(select_name);

        std::transform(select_name.begin(), select_name.end(), select_name.begin(), ::tolower);
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
                DB_WARNING("no table found for select field: %s", field->to_string().c_str());
                return -1;
            }
            add_single_table_columns(table_info);
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
        if (_table_alias_mapping.count(table_name) == 1) {
            full_name = _table_alias_mapping[table_name];
        } else if (!wild_card->db_name.empty()) {
            db_name = wild_card->db_name.value;
            full_name = db_name + "." + table_name;
        } else if (!_ctx->cur_db.empty()) {
            db_name = _ctx->cur_db;
            full_name = db_name + "." + table_name;
        } else {
            DB_WARNING("no db selected, please specify db name");
            return -1;
        }
        auto table_info = get_table_info_ptr(full_name);
        if (table_info == nullptr) {
            DB_WARNING("no table found for select field: %s", field->to_string().c_str());
            return -1;
        }
        add_single_table_columns(table_info);
    }
    return 0;
}

int SelectPlanner::parse_select_field(parser::SelectField* field) {
    pb::Expr select_expr;
    if (field->expr == nullptr) {
        DB_WARNING("field expr is nullptr");
        return -1;
    }
    if (0 != create_expr_tree(field->expr, select_expr, false)) {
        DB_WARNING("create select expr failed");
        return -1;
    }
    std::string select_name;
    bool has_alias = false;
    if (!field->as_name.empty()) {
        select_name = field->as_name.value;
        has_alias = true;
    } else {
        if (field->expr->expr_type == parser::ET_COLUMN) {
            parser::ColumnName* column = static_cast<parser::ColumnName*>(field->expr);
            select_name = column->name.c_str();
        } else {
            select_name = field->expr->to_string();
        }
    }
    _select_names.push_back(select_name);
    _select_exprs.push_back(select_expr);

    std::transform(select_name.begin(), select_name.end(), select_name.begin(), ::tolower);
    if (has_alias) {
        _select_alias_mapping.insert({select_name, (_select_names.size() - 1)});
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
    if (_select->where == nullptr) {
        return 0;
    }
    if (0 != flatten_filter(_select->where, _where_filters, false)) {
        DB_WARNING("flatten_filter failed");
        return -1;
    }
    return 0;
}

int SelectPlanner::_parse_having() {
    if (_select->having == nullptr) {
        return 0;
    }
    if (0 != flatten_filter(_select->having, _having_filters, true)) {
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
    for (int idx = 0; idx < by_items.size(); ++idx) {
        if (by_items[idx]->node_type != parser::NT_BY_ITEM) {
            DB_WARNING("un-supported group-by item type: %d", by_items[idx]->node_type);
            return -1;
        }
        // create group by expr node
        pb::Expr group_expr;
        if (0 != create_expr_tree(by_items[idx]->expr, group_expr, true)) {
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
    if (limit->offset != nullptr && 0 != create_expr_tree(limit->offset, _limit_offset, false)) {
        DB_WARNING("create limit offset expr failed");
        return -1;
    }
    if (limit->count != nullptr && 0 != create_expr_tree(limit->count, _limit_count, false)) {
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
//         _group_tuple_id = _tuple_cnt++;
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
