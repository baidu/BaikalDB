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

#include "union_planner.h"
#include "select_planner.h"
#include "dual_scan_node.h"
#include "network_socket.h"
#include "parser.h"

namespace baikaldb {

int UnionPlanner::plan() {
    if (!_ctx->stmt) {
        DB_WARNING("no sql command set");
        return -1;
    }
    _union_stmt = (parser::UnionStmt*)_ctx->stmt;
    if (_union_stmt->select_stmts.size() <= 0) {
        DB_WARNING("parse select sql failed");
        return -1;
    }
    auto client = _ctx->client_conn;
    if (0 != gen_select_stmts_plan()) {
        return -1;
    }

    // parse select fields
    parse_dual_fields();

    // parse order by
    if (0 != parse_dual_order_by()) {
        return -1;
    }
    // parse limit
    if (0 != parse_limit()) {
        return -1;        
    }

    if (0 != create_common_plan_node()) {
        return -1;
    }

    create_union_node();

    if (client->txn_id == 0) {
        _ctx->get_runtime_state()->set_single_sql_autocommit(true);
    } else {
        _ctx->get_runtime_state()->set_single_sql_autocommit(false);
    }
    return 0;
}

int UnionPlanner::gen_select_stmts_plan() {
    _is_distinct = _union_stmt->distinct;
    std::vector<std::string>    final_select_names;
    _union_tuple_id = _plan_table_ctx->tuple_cnt;
    for (int stmt_idx = 0; stmt_idx < _union_stmt->select_stmts.size(); stmt_idx++) {
        parser::SelectStmt* select = _union_stmt->select_stmts[stmt_idx];
        ExprParams expr_params;
        int ret = gen_subquery_plan(select, _plan_table_ctx, expr_params);
        if (ret < 0) {
            return -1;
        }
        if (stmt_idx == 0) {
            final_select_names = _select_names;
        }
        // union的每个select的column个数必须一样
        if (final_select_names.size() != _cur_sub_ctx->expr_params.row_filed_number) {
            _ctx->stat_info.error_code = ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT;
            _ctx->stat_info.error_msg << "The used SELECT statements have a different number of columns";
            DB_WARNING("have a different number of columns %zu %d", final_select_names.size(), _cur_sub_ctx->expr_params.row_filed_number);
            return -1;
        }
        _ctx->add_sub_ctx(_cur_sub_ctx);
    }
    _select_names.swap(final_select_names);
    return 0;
}

void UnionPlanner::parse_dual_fields() {
    int32_t slot_id = 1;
    int32_t tuple_id = _union_tuple_id; // 复用第一个select的tuple
    pb::TupleDescriptor tuple_desc;
    tuple_desc.set_tuple_id(tuple_id);
    tuple_desc.set_table_id(1);
    for (auto& field_name : _select_names) {
        pb::Expr select_expr;
        pb::SlotDescriptor slot_desc;
        slot_desc.set_slot_id(slot_id++);
        slot_desc.set_tuple_id(tuple_id);
        slot_desc.set_slot_type(pb::INVALID_TYPE);
        slot_desc.set_ref_cnt(1);
        pb::SlotDescriptor* slot = tuple_desc.add_slots();
        slot->CopyFrom(slot_desc);
        pb::ExprNode* node = select_expr.add_nodes();
        node->set_node_type(pb::SLOT_REF);
        node->set_col_type(pb::INVALID_TYPE);
        node->set_num_children(0);
        node->mutable_derive_node()->set_tuple_id(slot_desc.tuple_id());
        node->mutable_derive_node()->set_slot_id(slot_desc.slot_id());
        _name_slot_id_mapping[field_name] = slot_desc.slot_id();
        std::string select_name = field_name; 
        std::transform(select_name.begin(), select_name.end(), select_name.begin(), ::tolower);
        _ctx->field_column_id_mapping[select_name] = _column_id++;
        _select_exprs.push_back(select_expr);
    }
    _ctx->add_tuple(tuple_desc);
}

int UnionPlanner::parse_dual_order_by() {
    if (_union_stmt->order == nullptr) {
        DB_DEBUG("orderby is null");
        return 0;
    }
    parser::Vector<parser::ByItem*> order_items = _union_stmt->order->items;
    for (int idx = 0; idx < order_items.size(); ++idx) {
        bool is_asc = !order_items[idx]->is_desc;
        const parser::ExprNode* expr_item = (const parser::ExprNode*)order_items[idx]->expr;
        pb::Expr order_expr;
        if (expr_item->expr_type == parser::ET_COLUMN) {
            const parser::ColumnName* col_expr = static_cast<const parser::ColumnName*>(expr_item);
            std::string column_name(col_expr->name.c_str());
            if (std::find(_select_names.begin(), _select_names.end(), column_name) == _select_names.end()) {
                _ctx->stat_info.error_code = ER_BAD_FIELD_ERROR;
                _ctx->stat_info.error_msg << "Unknown column "<< column_name <<" in 'order clause'";
                return -1;
            }
            pb::ExprNode* node = order_expr.add_nodes();
            node->set_node_type(pb::SLOT_REF);
            node->set_col_type(pb::INVALID_TYPE);
            node->set_num_children(0);
            node->mutable_derive_node()->set_tuple_id(_union_tuple_id);
            node->mutable_derive_node()->set_slot_id(_name_slot_id_mapping[column_name]);
        } else {
            _ctx->stat_info.error_code = ER_WRONG_COLUMN_NAME;
            _ctx->stat_info.error_msg << "only support column in 'order clause'";
            return -1;
        }
        _order_exprs.push_back(order_expr);
        _order_ascs.push_back(is_asc);
    }
    return 0;
}

void UnionPlanner::create_union_node() {
    pb::PlanNode* plan_node = _ctx->add_plan_node();
    plan_node->set_node_type(pb::UNION_NODE);
    plan_node->set_limit(-1);
    plan_node->set_is_explain(_ctx->is_explain);
    plan_node->set_num_children(0);
    pb::DerivePlanNode* derive = plan_node->mutable_derive_node();
    pb::UnionNode* union_node = derive->mutable_union_node();
    union_node->set_union_tuple_id(_union_tuple_id);
}

// create packet_node/sort_node/limit_node/agg_node
int UnionPlanner::create_common_plan_node() {
    pb::PlanNode* pack_node = _ctx->add_plan_node();
    pack_node->set_node_type(pb::PACKET_NODE);
    pack_node->set_limit(-1);
    pack_node->set_is_explain(_ctx->is_explain);
    pack_node->set_num_children(1);
    pb::DerivePlanNode* derive = pack_node->mutable_derive_node();
    pb::PacketNode* pack = derive->mutable_packet_node();
    pack->set_op_type(pb::OP_UNION);
    for (auto& expr : _select_exprs) {
        auto proj = pack->add_projections();
        proj->CopyFrom(expr);
    }
    for (auto& name : _select_names) {
        pack->add_col_names(name);
    }
    if (_union_stmt->limit == nullptr && _order_exprs.size() == 0 && !_is_distinct) {
        return 0;
    }
    if (_union_stmt->limit != nullptr) {
        pb::PlanNode* limit_node = _ctx->add_plan_node();
        limit_node->set_node_type(pb::LIMIT_NODE);
        limit_node->set_limit(-1);
        limit_node->set_is_explain(_ctx->is_explain);
        limit_node->set_num_children(1);
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
    }
    if (_order_exprs.size() > 0) {
        pb::PlanNode* sort_node = _ctx->add_plan_node();
        sort_node->set_node_type(pb::SORT_NODE);
        sort_node->set_limit(-1);
        sort_node->set_is_explain(_ctx->is_explain);
        sort_node->set_num_children(1);
        pb::DerivePlanNode* derive = sort_node->mutable_derive_node();
        pb::SortNode* sort = derive->mutable_sort_node();
        
        if (_order_exprs.size() != _order_ascs.size()) {
            DB_WARNING("order expr format error");
            return -1;
        }
        for (uint32_t idx = 0; idx < _order_exprs.size(); ++idx) {
            pb::Expr* order_expr = sort->add_order_exprs();
            pb::Expr* slot_order_expr = sort->add_slot_order_exprs();
            order_expr->CopyFrom(_order_exprs[idx]);
            slot_order_expr->CopyFrom(_order_exprs[idx]);
            sort->add_is_asc(_order_ascs[idx]);
            sort->add_is_null_first(_order_ascs[idx]);
        }
        sort->set_tuple_id(_union_tuple_id);
    }
    if (_is_distinct) {
        pb::PlanNode* agg_node = _ctx->add_plan_node();
        agg_node->set_node_type(pb::AGG_NODE);
        agg_node->set_limit(-1);
        agg_node->set_is_explain(_ctx->is_explain);
        agg_node->set_num_children(1);
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
    }
    return 0;
}

int UnionPlanner::parse_limit() {
    if (_union_stmt->limit == nullptr) {
        return 0;
    }
    parser::LimitClause* limit = _union_stmt->limit;
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

} // namespace bailaldb
