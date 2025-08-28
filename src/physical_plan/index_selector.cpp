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
#include "index_selector.h"
#include "slot_ref.h"
#include "scalar_fn_call.h"
#include "predicate.h"
#include "join_node.h"
#include "limit_node.h"
#include "parser.h"

namespace baikaldb {
DEFINE_bool(use_column_storage, false, "whether use column storage");
using namespace range;

DEFINE_bool(use_index_merge, false, "if use index merge in index select");

int get_field_hit_type_weight(RangeType &ty) {
    if (ty == EQ || ty == LIKE_EQ) {
        return 10;
    }
    if (ty == IN) {
        return 9;
    }
    if (ty == RANGE || ty == LIKE_PREFIX) {
        return 5;
    }
    return 0;
}
static std::unordered_map<std::string, pb::RollupType> name_to_rollup_type_map = {
    {"sum", pb::SUM},
};

int IndexSelector::analyze(QueryContext* ctx) {
    ExecNode* root = ctx->root;
    _ctx = ctx;
    bool can_use_column_storage = false;
    if (ctx->runtime_state != nullptr && ctx->runtime_state->execute_type == pb::EXEC_ARROW_ACERO) {
        can_use_column_storage = true;
    }

    std::vector<ExecNode*> scan_nodes;
    root->get_node(pb::SCAN_NODE, scan_nodes);
    if (scan_nodes.size() == 0) {
        return 0;
    }
    LimitNode* limit_node = static_cast<LimitNode*>(root->get_node(pb::LIMIT_NODE));
    AggNode* agg_node = static_cast<AggNode*>(root->get_node(pb::AGG_NODE));
    PacketNode* packet_node = static_cast<PacketNode*>(root->get_node(pb::PACKET_NODE));
    FilterNode* having_filter_node = static_cast<FilterNode*>(root->get_node(pb::HAVING_FILTER_NODE));
    JoinNode* join_node = static_cast<JoinNode*>(root->get_node(pb::JOIN_NODE));
    WindowNode* window_node = static_cast<WindowNode*>(root->get_node(pb::WINDOW_NODE));
    SortNode* sort_node = static_cast<SortNode*>(root->get_last_node(pb::SORT_NODE)); // 窗口函数场景可能包含多个sort节点，需要获取最后一个

    // TODO sort可以增加topN
    if (limit_node != nullptr && sort_node != nullptr && window_node == nullptr) {
        sort_node->set_limit(limit_node->other_limit());
    }
    for (auto& scan_node_ptr : scan_nodes) {
        if (!static_cast<ScanNode*>(scan_node_ptr)->is_rocksdb_scan_node()) {
            continue;
        }
        ExecNode* parent_node_ptr = scan_node_ptr->get_parent();
        if (parent_node_ptr == NULL) {
            continue;
        }
        FilterNode* filter_node = nullptr;
        if (parent_node_ptr->node_type() == pb::WHERE_FILTER_NODE
                || parent_node_ptr->node_type() == pb::TABLE_FILTER_NODE) {
            filter_node = static_cast<FilterNode*>(parent_node_ptr);
        }
        //有join节点暂时不考虑sort索引优化
        int ret = 0;
        std::map<int32_t, int> field_range_type;
        bool index_has_null = false;
        if (join_node != NULL || agg_node != NULL) {
            IndexSelectorOptions options;
            options.execute_type = ctx->runtime_state == nullptr ? pb::EXEC_ROW : ctx->runtime_state->execute_type;
            ret = index_selector(ctx->tuple_descs(),
                            static_cast<ScanNode*>(scan_node_ptr), 
                            filter_node, 
                            NULL,
                            join_node,
                            packet_node,
                            agg_node,
                            having_filter_node,
                            window_node, 
                            &index_has_null,
                            field_range_type,
                            ctx->stat_info.sample_sql.str(), options);
        } else {
            IndexSelectorOptions options;
            options.execute_type = ctx->runtime_state == nullptr ? pb::EXEC_ROW : ctx->runtime_state->execute_type;
            ret = index_selector(ctx->tuple_descs(),
                           static_cast<ScanNode*>(scan_node_ptr), 
                           filter_node, 
                           sort_node,
                           join_node,
                           packet_node,
                           agg_node,
                           having_filter_node,
                           window_node,
                           &index_has_null,
                           field_range_type, 
                           ctx->stat_info.sample_sql.str(), options);
        }
        if (index_has_null) {
            ctx->return_empty = true;
            DB_WARNING("normal predicate compare with null");
            return 0;
        }
        if (ret < 0) {
            return ret;
        }
        if (ret > 0) {
            ctx->index_ids.insert(ret);
            ctx->field_range_type = field_range_type;
            // 子查询下推后可能选中全局索引，该场景下不使用全量导出
            if (ret != static_cast<ScanNode*>(scan_node_ptr)->table_id()) {
                ctx->is_full_export = false;
            }
        }

        if (FLAGS_use_index_merge && (ctx->is_select || ctx->execute_global_flow)) {
            int32_t r = index_merge_selector(ctx->tuple_descs(),
                                 static_cast<ScanNode*>(scan_node_ptr),
                                 filter_node,
                                 (join_node != NULL || agg_node != NULL) ? NULL: sort_node,
                                 join_node,
                                 packet_node,
                                 agg_node,
                                 having_filter_node,
                                 &index_has_null,
                                 field_range_type,
                                 ctx->stat_info.sample_sql.str());
            if (r > 0) {
                scan_node_ptr->set_has_optimized(true);
            }
        }
    }
    return 0;
}

void IndexSelector::analyze_join_index(QueryContext* ctx, ScanNode* scan_node, ExprNode* in_condition) {
    if (!ctx || !scan_node || !in_condition) {
        return;
    }
    _ctx = ctx;
    // 和joiner真正下推in一样的逻辑
    if (!scan_node->is_rocksdb_scan_node()) {
        return;
    }
    ExecNode* parent_node_ptr = scan_node->get_parent();
    FilterNode* filter_node = nullptr;
    if (parent_node_ptr != nullptr
            && (parent_node_ptr->node_type() == pb::WHERE_FILTER_NODE
                || parent_node_ptr->node_type() == pb::TABLE_FILTER_NODE)) {
        filter_node = static_cast<FilterNode*>(parent_node_ptr);
        // FIXME: a in (1, 2) and a > 0优化成a in (1, 2)，filter_node需要进行expr_optimize
    }
    SortNode* sort_node = nullptr;
    while (parent_node_ptr != nullptr
            && parent_node_ptr->node_type() != pb::SELECT_MANAGER_NODE
            && parent_node_ptr->node_type() != pb::JOIN_NODE) {
        if (parent_node_ptr->node_type() == pb::SORT_NODE) {
            sort_node = static_cast<SortNode*>(parent_node_ptr);
            break;
        }
        parent_node_ptr = parent_node_ptr->get_parent();
    }
    std::map<int32_t, int> field_range_type;
    bool index_has_null = false;
    IndexSelectorOptions options;
    options.join_on_conditions = in_condition;
    options.execute_type = ctx->runtime_state == nullptr ? pb::EXEC_ROW : ctx->runtime_state->execute_type;
    index_selector(ctx->tuple_descs(),
                    scan_node, 
                    filter_node,
                    sort_node,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    &index_has_null, field_range_type, "", options);
    return;
}

inline bool is_index_predicate(ExprNode* expr) {
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
        case pb::IN_PREDICATE: 
        case pb::LIKE_PREDICATE: 
            return true;
        default:
            return false;
    }
}

// https://dev.mysql.com/doc/refman/5.6/en/comparison-operators.html#operator_less-than-or-equal
void IndexSelector::hit_row_field_range(ExprNode* expr, 
        std::map<int32_t, FieldRange>& field_range_map, bool* index_predicate_is_null) {
    RowExpr* row_expr = static_cast<RowExpr*>(expr->children(0));
    std::map<size_t, SlotRef*> slots;
    std::map<size_t, std::vector<ExprValue>> values;
    row_expr->get_all_slot_ref(&slots);
    // TODO 对于非全部slot的也可以处理
    if (slots.size() != row_expr->children_size()) {
        return;
    }
    for (uint32_t i = 1; i < expr->children_size(); i++) {
        if (!expr->children(i)->is_constant()) {
            return;
        }
        if (!expr->children(i)->is_row_expr()) {
            return;
        }
        bool has_null = false;
        for (auto& pair : slots) {
            size_t idx = pair.first;
            ExprValue val = static_cast<RowExpr*>(expr->children(i))->get_value(nullptr, idx);
            if (val.is_null()) {
                has_null = true;
            }
        }
        if (!has_null) {
            for (auto& pair : slots) {
                size_t idx = pair.first;
                values[idx].push_back(static_cast<RowExpr*>(expr->children(i))->get_value(nullptr, idx));
            }
        }
    }
    if (values.empty() || values.begin()->second.size() == 0) {
        *index_predicate_is_null = is_index_predicate(expr);
        return;
    }
    std::vector<int32_t> field_ids;
    for (auto& pair : slots) {
        field_ids.push_back(pair.second->field_id());
    }
    RangeType tmp_type;
    switch (expr->node_type()) {
        case pb::FUNCTION_CALL: {
            int32_t fn_op = static_cast<ScalarFnCall*>(expr)->fn().fn_op();
            switch (fn_op) {
                case parser::FT_EQ: {
                    for (auto& pair : slots) {
                        size_t idx = pair.first;
                        SlotRef* slot = pair.second;
                        int32_t field_id = slot->field_id();
                        field_range_map[field_id].eq_in_values = values[idx];
                        field_range_map[field_id].conditions.insert(expr);
                        field_range_map[field_id].type = EQ;
                        field_range_map[field_id].is_row_expr = true;
                        field_range_map[field_id].left_row_field_ids = field_ids;
                    }
                    return;
                }
                case parser::FT_GE:
                case parser::FT_GT: {
                    if (slots.count(0) == 0) {
                        return;
                    }
                    int32_t field_id = slots[0]->field_id();
                    // 修复 (a,b) > (1,1) and (a,b)>(2,2)
                    if (!field_range_map[field_id].left.empty()) {
                        return;
                    }
                    tmp_type = RANGE;
                    if (get_field_hit_type_weight(field_range_map[field_id].type) > get_field_hit_type_weight(tmp_type)) {
                        return;
                    }
                    if (tmp_type != field_range_map[field_id].type) {
                        field_range_map[field_id] = FieldRange();
                    }
                    for (auto pair : values) {
                        field_range_map[field_id].left.push_back(pair.second[0]);
                    }
                    field_range_map[field_id].left_open = fn_op == parser::FT_GT;
                    field_range_map[field_id].left_expr = expr;
                    field_range_map[field_id].type = RANGE;
                    field_range_map[field_id].is_row_expr = true;
                    field_range_map[field_id].left_row_field_ids = field_ids;
                    return;
                }
                case parser::FT_LE:
                case parser::FT_LT: {
                    if (slots.count(0) == 0) {
                        return;
                    }
                    int32_t field_id = slots[0]->field_id();
                    if (!field_range_map[field_id].right.empty()) {
                        return;
                    }
                    tmp_type = RANGE;
                    if (get_field_hit_type_weight(field_range_map[field_id].type) > get_field_hit_type_weight(tmp_type)) {
                        return;
                    }
                    if (tmp_type != field_range_map[field_id].type) {
                        field_range_map[field_id] = FieldRange();
                    }
                    for (auto pair : values) {
                        field_range_map[field_id].right.push_back(pair.second[0]);
                    }
                    field_range_map[field_id].right_open = fn_op == parser::FT_LT;
                    field_range_map[field_id].right_expr = expr;
                    field_range_map[field_id].type = RANGE;
                    field_range_map[field_id].is_row_expr = true;
                    field_range_map[field_id].right_row_field_ids = field_ids;
                    return;
                }
                default:
                    return;
            }
        }
        case pb::IN_PREDICATE: {
            for (auto& pair : slots) {
                size_t idx = pair.first;
                SlotRef* slot = pair.second;
                int32_t field_id = slot->field_id();
                if (field_range_map[field_id].type != NONE
                    && field_range_map[field_id].eq_in_values.size() <= values[idx].size()) {
                    continue;
                }
                field_range_map[field_id].eq_in_values = values[idx];
                field_range_map[field_id].conditions.clear();
                field_range_map[field_id].conditions.insert(expr);
                field_range_map[field_id].is_row_expr = true;
                field_range_map[field_id].left_row_field_ids = field_ids;
                if (values[idx].size() == 1) {
                    field_range_map[field_id].type = EQ;
                } else {
                    field_range_map[field_id].type = IN;
                }
            }
            return;
        }
        default:
            return;
    }
}

void IndexSelector::hit_field_or_like_range(ExprNode* expr, std::map<int32_t, FieldRange>& field_range_map, 
    int64_t table_id, FulltextInfoNode* fulltext_index_node) {
    bool new_fulltext_flag = true;
    std::vector<ExprNode*> or_exprs;
    expr->flatten_or_expr(&or_exprs);
    bool match_against = false;
    RangeType match_type = MATCH_LANGUAGE;
    for (auto sub_expr : or_exprs) {
        match_against = false;
        if (sub_expr->node_type() != pb::LIKE_PREDICATE) {
            if (sub_expr->node_type() == pb::FUNCTION_CALL) {
                int32_t fn_op = static_cast<ScalarFnCall*>(sub_expr)->fn().fn_op();
                if (fn_op != parser::FT_MATCH_AGAINST) {
                    return;
                }
                match_against = true;
            } else {
                return;
            }
        }
        if (match_against) {
            if (!sub_expr->children(0)->children(0)->is_slot_ref()) {
                return;
            }
        } else if (!sub_expr->children(0)->is_slot_ref()) {
            return;
        }
        if (!sub_expr->children(1)->is_literal()) {
            return;
        }
        if (match_against) {
            ExprValue mode = sub_expr->children(2)->get_value(nullptr);
            if (mode.get_string() == "IN BOOLEAN MODE") {
                match_type = MATCH_BOOLEAN;
            } else if (mode.get_string() == "IN VECTOR MODE") {
                match_type = MATCH_VECTOR;
            }
        }
    }
    std::vector<int64_t> index_ids; 
    index_ids.reserve(2);
    {
        // or 只能全部是倒排索引才能选择。
        // 倒排索引 or 普通索引时，不选择倒排索引（针对该 expr，不排除其他 expr 选择该倒排索引）。
        auto table_info_ptr = _factory->get_table_info_ptr(table_id);
        if (table_info_ptr == nullptr) {
            return;
        }
        int64_t index_id = 0;
        for (auto& sub_expr : or_exprs) {
            SlotRef* slot_ref = match_against ? 
                static_cast<SlotRef*>(sub_expr->children(0)->children(0)) : 
                static_cast<SlotRef*>(sub_expr->children(0));
            int32_t field_id = slot_ref->field_id();
            // 所有字段都有arrow索引，才建立 or节点。
            // TODO 删除所有pb类型之后 删除该逻辑
            new_fulltext_flag = new_fulltext_flag && is_field_has_reverse_index(table_id, field_id, &index_id);
            index_ids.push_back(index_id);
            if (table_info_ptr->reverse_fields.count(field_id) == 0 &&
                table_info_ptr->arrow_reverse_fields.count(field_id) == 0) {
                DB_DEBUG("table_id %ld field_id %d not all reverse list", table_id, field_id);
                return;
            }
        }
    }
    auto& inner_node = boost::get<FulltextInfoNode::FulltextChildType>(fulltext_index_node->info);
    if (new_fulltext_flag) {
        inner_node.children.emplace_back(new FulltextInfoNode);
        auto& back_node = inner_node.children.back();
        FulltextInfoNode::FulltextChildType or_node;
        back_node->info = or_node;
        back_node->type = pb::FNT_OR;
    }

    size_t index_ids_index = 0;
    for (auto sub_expr : or_exprs) {
        SlotRef* slot_ref = match_against ?
            static_cast<SlotRef*>(sub_expr->children(0)->children(0)) :
            static_cast<SlotRef*>(sub_expr->children(0));
        int32_t field_id = slot_ref->field_id();
        field_range_map[field_id].like_values.push_back(sub_expr->children(1)->get_value(nullptr));
        field_range_map[field_id].conditions.insert(expr);
        field_range_map[field_id].type = OR_LIKE;
        if (static_cast<ScalarFnCall*>(sub_expr)->fn().fn_op() == parser::FT_EXACT_LIKE) {
            field_range_map[field_id].is_exact_like = true;
        }

        if (new_fulltext_flag) {
            auto& back_node = inner_node.children.back();
            range::FieldRange fulltext_or_range;
            fulltext_or_range.left_row_field_ids.push_back(field_id);
            fulltext_or_range.is_exact_like = true;
            fulltext_or_range.type = OR_LIKE;
            if (match_against) {
                fulltext_or_range.type = match_type;
            }
            fulltext_or_range.conditions.insert(expr);
            fulltext_or_range.like_values.push_back(sub_expr->children(1)->get_value(nullptr));

            auto& or_node = boost::get<FulltextInfoNode::FulltextChildType>(back_node->info);
            or_node.children.emplace_back(new FulltextInfoNode);
            or_node.children.back()->info = std::make_pair(index_ids[index_ids_index++], std::move(fulltext_or_range));
            or_node.children.back()->type = pb::FNT_TERM;
        }

    }
    return;
}

void IndexSelector::hit_match_against_field_range(ExprNode* expr,
    std::map<int32_t, FieldRange>& field_range_map, FulltextInfoNode* fulltext_index_node, int64_t table_id) {
    SlotRef* slot_ref = static_cast<SlotRef*>(expr->children(0)->children(0));
    int32_t field_id = slot_ref->field_id();
    if (!expr->children(1)->is_constant()) {
        return;
    }
    ExprValue value = expr->children(1)->get_value(nullptr);
    ExprValue mode = expr->children(2)->get_value(nullptr);
    RangeType type = MATCH_LANGUAGE;
    if (mode.get_string() == "IN BOOLEAN MODE") {
        type = MATCH_BOOLEAN;
    } else if (mode.get_string() == "IN VECTOR MODE") {
        type = MATCH_VECTOR;
    }
    field_range_map[field_id].type = type;
    field_range_map[field_id].like_values.push_back(value);
    field_range_map[field_id].conditions.insert(expr);

    int64_t index_id = 0;
    if (is_field_has_reverse_index(table_id, field_id, &index_id)) {
        range::FieldRange fulltext_match_range;
        fulltext_match_range.left_row_field_ids.push_back(field_id);
        fulltext_match_range.type = type;
        fulltext_match_range.conditions.insert(expr);
        fulltext_match_range.like_values.push_back(value);

        auto& inner_node = boost::get<FulltextInfoNode::FulltextChildType>(fulltext_index_node->info);
        inner_node.children.emplace_back(new FulltextInfoNode);
        inner_node.children.back()->info = std::make_pair(index_id, std::move(fulltext_match_range));
        inner_node.children.back()->type = pb::FNT_TERM;
    }

    return;
}

void IndexSelector::hit_field_range(ExprNode* expr,
        std::map<int32_t, FieldRange>& field_range_map, bool* index_predicate_is_null,
            int64_t table_id, FulltextInfoNode* fulltext_index_node) {
    if (expr->node_type() == pb::OR_PREDICATE) {
        // 倒排索引情况下，目前or里只能是like或者match against，不支持in和=
        return hit_field_or_like_range(expr, field_range_map, table_id, fulltext_index_node);
    }
    if (expr->node_type() == pb::FUNCTION_CALL) {
        int32_t fn_op = static_cast<ScalarFnCall*>(expr)->fn().fn_op();
        if (fn_op == parser::FT_MATCH_AGAINST) {
            return hit_match_against_field_range(expr, field_range_map, fulltext_index_node, table_id);
        }
    }
    if (expr->children_size() < 2) {
        return;
    }
    if (expr->children(0)->is_row_expr()) {
        return hit_row_field_range(expr, field_range_map, index_predicate_is_null);
    } else if (!expr->children(0)->is_slot_ref()) {
        return;
    }
    std::vector<ExprValue> values;
    SlotRef* slot_ref = static_cast<SlotRef*>(expr->children(0));
    int32_t field_id = slot_ref->field_id();
    pb::PrimitiveType col_type = expr->children(0)->col_type();
    for (uint32_t i = 1; i < expr->children_size(); i++) {
        if (!expr->children(i)->is_constant()) {
            return;
        }
        expr->children(i)->open();
        ExprValue val = expr->children(i)->get_value(nullptr);
        // 目前索引不允许为null
        if (!val.is_null()) {
            values.push_back(val.cast_to(col_type));
        }
    }
    if (values.size() == 0) {
        *index_predicate_is_null = is_index_predicate(expr);
        return;
    }
    RangeType tmp_type;

    auto try_add_into_fulltext = [this, field_id](FulltextInfoNode* index_node, range::FieldRange&& range, int64_t index_id) {
        if (index_node != nullptr && index_id > 0) {
            auto& inner_node = boost::get<FulltextInfoNode::FulltextChildType>(index_node->info);
            inner_node.children.emplace_back(new FulltextInfoNode);
            range.left_row_field_ids.emplace_back(field_id);
            inner_node.children.back()->info = std::make_pair(index_id, std::move(range));
            inner_node.children.back()->type = pb::FNT_TERM;
        }
    };

    switch (expr->node_type()) {
        case pb::FUNCTION_CALL: {
            int32_t fn_op = static_cast<ScalarFnCall*>(expr)->fn().fn_op();
            switch (fn_op) {
                case parser::FT_EQ:
                    tmp_type = EQ;
                    if (get_field_hit_type_weight(field_range_map[field_id].type) > get_field_hit_type_weight(tmp_type)) {
                        return;
                    }
                    field_range_map[field_id] = FieldRange();
                    field_range_map[field_id].eq_in_values = values;
                    field_range_map[field_id].conditions.clear();
                    field_range_map[field_id].conditions.insert(expr);
                    field_range_map[field_id].type = EQ;
                    if (fulltext_index_node != nullptr) {
                        int64_t index_id = 0;
                        if (is_field_has_reverse_index(table_id, field_id, &index_id)) {
                            auto field_range_cpy = field_range_map[field_id];
                            try_add_into_fulltext(fulltext_index_node, std::move(field_range_cpy), index_id);
                        }
                    }
                    return;
                case parser::FT_GE:
                case parser::FT_GT:
                    tmp_type = RANGE;
                    if (get_field_hit_type_weight(field_range_map[field_id].type) > get_field_hit_type_weight(tmp_type)) {
                        return;
                    }
                    if (tmp_type != field_range_map[field_id].type) {
                        field_range_map[field_id] = FieldRange();
                    }

                    if (!field_range_map[field_id].left.empty()) {
                        return;
                    }
                    field_range_map[field_id].left.push_back(values[0]);
                    field_range_map[field_id].left_open = fn_op == parser::FT_GT;
                    field_range_map[field_id].left_expr = expr;
                    field_range_map[field_id].type = RANGE;
                    return;
                case parser::FT_LE:
                case parser::FT_LT:
                    tmp_type = RANGE;
                    if (get_field_hit_type_weight(field_range_map[field_id].type) > get_field_hit_type_weight(tmp_type)) {
                        return;
                    }
                    if (tmp_type != field_range_map[field_id].type) {
                        field_range_map[field_id] = FieldRange();
                    }

                    if (!field_range_map[field_id].right.empty()) {
                        return;
                    }
                    field_range_map[field_id].right.push_back(values[0]);
                    field_range_map[field_id].right_open = fn_op == parser::FT_LT;
                    field_range_map[field_id].right_expr = expr;
                    field_range_map[field_id].type = RANGE;
                    return;
                default:
                    return;
            }
        }
        case pb::IN_PREDICATE: {
            bool do_this = false;
            if (field_range_map[field_id].type == NONE) {
                do_this = true;
            } else if ((field_range_map[field_id].type == EQ || field_range_map[field_id].type == IN)
                && !field_range_map[field_id].is_row_expr
                && values.size() < field_range_map[field_id].eq_in_values.size()) {
                // 1个字段对应多个in preds时 例如: a in ("a1", "a2") and (a, b) in (("a1","b1")),取数量少的pred
                do_this = true;
            }
            if  (!do_this) {
                return;
            }
            tmp_type = IN;
            if (get_field_hit_type_weight(field_range_map[field_id].type) > get_field_hit_type_weight(tmp_type)) {
                return ;
            }
            field_range_map[field_id] = FieldRange();
            field_range_map[field_id].eq_in_values = values;
            field_range_map[field_id].conditions.clear();
            field_range_map[field_id].conditions.insert(expr);
            if (values.size() == 1) {
                field_range_map[field_id].type = EQ;
            } else {
                field_range_map[field_id].type = IN;
            }
            int64_t index_id = 0;
            if (fulltext_index_node != nullptr
                    && is_field_has_reverse_index(table_id, field_id, &index_id)) {
                // in多个需要展开为一个or下多个term
                FulltextInfoNode* in_fulltext_node = fulltext_index_node;
                if (values.size() > 1) {
                    auto& inner_node = boost::get<FulltextInfoNode::FulltextChildType>(fulltext_index_node->info);
                    in_fulltext_node = new FulltextInfoNode;
                    in_fulltext_node->type = pb::FNT_OR;
                    inner_node.children.emplace_back(in_fulltext_node);
                }

                for (const auto& in_expr: values) {
                    auto field_range_cpy = field_range_map[field_id];
                    field_range_cpy.type = EQ;
                    field_range_cpy.eq_in_values = {in_expr};
                    try_add_into_fulltext(in_fulltext_node, std::move(field_range_cpy), index_id);
                }
            }
            return;
        }
        case pb::LIKE_PREDICATE: {
            bool is_eq = false;
            bool is_prefix = false;
            ExprValue prefix_value(pb::STRING);
            static_cast<LikePredicate*>(expr)->hit_index(&is_eq, &is_prefix, &(prefix_value.str_val));
            if (is_eq) {
                tmp_type = LIKE_EQ;
            } else if (is_prefix) {
                tmp_type = LIKE_PREFIX;
            } else {
                tmp_type = LIKE;
            }
            if (get_field_hit_type_weight(field_range_map[field_id].type) > get_field_hit_type_weight(tmp_type)) {
                return;
            }
            field_range_map[field_id] = FieldRange();
            range::FieldRange fulltext_and_range;
            field_range_map[field_id].like_values.push_back(values[0]);
            fulltext_and_range.like_values.push_back(values[0]);
            if (static_cast<ScalarFnCall*>(expr)->fn().fn_op() == parser::FT_EXACT_LIKE) {
                field_range_map[field_id].is_exact_like = true;
                fulltext_and_range.is_exact_like = true;
            }
            field_range_map[field_id].conditions.insert(expr);
            fulltext_and_range.conditions.insert(expr);
            if (is_eq) {
                field_range_map[field_id].eq_in_values.push_back(prefix_value);
                fulltext_and_range.eq_in_values.push_back(prefix_value);
                field_range_map[field_id].type = LIKE_EQ;
                fulltext_and_range.type = LIKE_EQ;
            } else if (is_prefix) {
                field_range_map[field_id].eq_in_values.push_back(prefix_value);
                fulltext_and_range.eq_in_values.push_back(prefix_value);
                field_range_map[field_id].type = LIKE_PREFIX;
                fulltext_and_range.type = LIKE_PREFIX;
            } else {
                field_range_map[field_id].type = LIKE;
                fulltext_and_range.type = LIKE;
            }
            int64_t index_id = 0;
            if (is_field_has_reverse_index(table_id, field_id, &index_id)) {
                try_add_into_fulltext(fulltext_index_node, std::move(fulltext_and_range), index_id);
            }

            return;
         }
        default:
            return;
    }
}

int64_t IndexSelector::index_selector(const std::vector<pb::TupleDescriptor>& tuple_descs,
                                    ScanNode* scan_node, 
                                    FilterNode* filter_node, 
                                    SortNode* sort_node,
                                    JoinNode* join_node,
                                    PacketNode* packet_node,
                                    AggNode* agg_node,
                                    FilterNode* having_filter_node,
                                    WindowNode* window_node,
                                    bool* index_has_null,
                                    std::map<int32_t, int>& field_range_type,
                                    const std::string& sample_sql,
                                    const IndexSelectorOptions& options) {
    int64_t table_id = scan_node->table_id();
    int32_t tuple_id = scan_node->tuple_id();
    auto table_info = _factory->get_table_info_ptr(table_id);
    if (table_info == nullptr) {
        DB_WARNING("table info not found:%ld", table_id);
        return -1;
    }

    bool can_use_column_storage = false;
    if (FLAGS_use_column_storage && options.execute_type == pb::EXEC_ARROW_ACERO && table_info->schema_conf.use_column_storage()) {
        // db侧判断是否使用列存:
        //     1. FLAGS_use_column_storage是否为true
        //     2. 是否走列式执行
        //     3. 表是否支持列存
        // store侧判断是否使用列存:
        //     1. db判断是否使用列存
        //     2. store region是否支持列存
        // db侧如果判断使用列存，需要将scan_node的use_column_storage设置为true；
        // 并且由于列存场景主键及索引过滤非精准过滤，所以需要把主键过滤条件以及索引过滤条件添加到FilterNode中进行一次精准过滤；
        // 注意: 如果db判断走列存，store判断走行存，则行存场景可能执行两次主键/索引过滤条件；
        can_use_column_storage = true;
    }

    if (table_info->schema_conf.use_column_storage() && options.execute_type != pb::EXEC_ARROW_ACERO) {
        // 加报警统计无法走列存的请求
        DB_WARNING("table:%ld use column storage but execute type is not exec_arrow_acero", table_id);
    }

    pb::ScanNode* pb_scan_node = scan_node->mutable_pb_node()->
        mutable_derive_node()->mutable_scan_node();
    pb_scan_node->set_use_column_storage(can_use_column_storage);

    std::vector<ExprNode*>* conjuncts = filter_node ? filter_node->mutable_conjuncts() : nullptr;
    
    // 重构思路：先计算单个field的range范围，然后index根据field的范围情况进一步计算
    // field_id => range映射
    std::shared_ptr<std::map<int32_t, FieldRange>> field_range_map = std::make_shared<std::map<int32_t, FieldRange>>();
    FulltextInfoTree fulltext_index_tree;
    //最外一层 and
    fulltext_index_tree.root.reset(new FulltextInfoNode);
    FulltextInfoNode::FulltextChildType and_node;
    fulltext_index_tree.root->info = and_node;
    fulltext_index_tree.root->type = pb::FNT_AND;
    // expr => field_ids
    std::map<ExprNode*, std::unordered_set<int32_t>> expr_field_map;
    if (conjuncts != nullptr) {
        for (auto expr : *conjuncts) {
            bool index_predicate_is_null = false;
            std::unordered_set<int32_t> tuple_ids;
            expr->get_all_tuple_ids(tuple_ids);
            tuple_ids.erase(tuple_id);
            if (!tuple_ids.empty()) {
                continue;
            }
            hit_field_range(expr, *field_range_map, &index_predicate_is_null, table_id, fulltext_index_tree.root.get());
            if (index_predicate_is_null) {
                if (index_has_null != nullptr) {
                    *index_has_null = true;
                }
                break;
            }
            expr->get_all_field_ids(expr_field_map[expr]);
        }
    }
    bool pushed_join_on_condition = false;
    if (options.join_on_conditions != nullptr) {
        pushed_join_on_condition = true;
        bool index_predicate_is_null = false;
        hit_field_range(options.join_on_conditions, *field_range_map, &index_predicate_is_null, table_id, fulltext_index_tree.root.get());
    }

    for (auto& pair : *field_range_map) {
        field_range_type[pair.first] = pair.second.type;
    }

    std::vector<int64_t> index_ids = table_info->indices;
    std::set<int64_t> force_indexs;
    force_indexs.insert(std::begin(pb_scan_node->force_indexes()),
        std::end(pb_scan_node->force_indexes()));
    if (pb_scan_node->force_indexes_size() != 0){
        index_ids.clear();
        for (auto& index_id : pb_scan_node->force_indexes()) {
            index_ids.emplace_back(index_id);
        }
        if (force_indexs.count(table_id) == 0){
            index_ids.emplace_back(table_id);
        }
    } else if (pb_scan_node->use_indexes_size() != 0) {
        index_ids.clear();
        for (auto& index_id : pb_scan_node->use_indexes()) {
            index_ids.emplace_back(index_id);
        }
    }
    std::set<int64_t> ignore_indexs;
    ignore_indexs.insert(std::begin(pb_scan_node->ignore_indexes()),
            std::end(pb_scan_node->ignore_indexes()));

    // ignore explain hint 中指定的 index
    if (_ctx->is_explain
            && _ctx->explain_hint != nullptr
            && _ctx->explain_hint->get_flag<ExplainHint::HintType::IGNORE_INDEX>()) {
        auto factory = SchemaFactory::get_instance();
        const std::vector<std::string>& blocked_indexes = _ctx->explain_hint->blocked_indexes;
        for (auto index_id: table_info->indices) {
            const std::string& index_name = factory->get_index_info(index_id).short_name;
            if (std::find(blocked_indexes.begin(), blocked_indexes.end(), index_name) != blocked_indexes.end()) {
                ignore_indexs.insert(index_id);
            }
        }
    }

    if (_ctx->is_explain && _ctx->explain_hint != nullptr) {
        scan_node->set_explain_hint(_ctx->explain_hint);
    }

    auto pri_ptr = _factory->get_index_info_ptr(table_id); 
    if (pri_ptr == nullptr) {
        DB_WARNING("pk info not found:%ld", table_id);
        return -1;
    }
    SmartRecord record_template = _factory->new_record(table_id);
    std::map<int64_t, bool> fulltext_fields_exact_like_map;
    std::vector<SmartPath> access_paths;
    access_paths.reserve(2);
    for (auto index_id : index_ids) {
        if (ignore_indexs.count(index_id) == 1 && index_id != table_id) {
            continue;
        }
        auto info_ptr = _factory->get_index_info_ptr(index_id); 
        if (info_ptr == nullptr) {
            continue;
        }
        // 不在此处跳过disable的索引，在add_path处判断，这样learner可以使用
        // if (info_ptr->index_hint_status == pb::IHS_DISABLE) {
        //     DB_DEBUG("index[%s] is disabled", info_ptr->name.c_str());
        //     continue;
        // }
        IndexInfo& index_info = *info_ptr;
        pb::IndexType index_type = index_info.type;
        auto index_state = index_info.state;
        if (index_state != pb::IS_PUBLIC) {
            DB_DEBUG("DDL_LOG skip index [%ld] state [%s] ", 
                index_id, pb::IndexState_Name(index_state).c_str());
            continue;
        }
        
        if (index_info.type == pb::I_ROLLUP && !check_rollup_index_valid(
                                                        table_info, 
                                                        index_info, 
                                                        filter_node, 
                                                        sort_node, 
                                                        packet_node, 
                                                        agg_node,
                                                        having_filter_node,
                                                        window_node,
                                                        *field_range_map)) {
            continue;
        }

        SmartPath access_path = std::make_shared<AccessPath>();
        if (force_indexs.count(index_id) == 1) {
            access_path->hint = AccessPath::FORCE_INDEX;
        }
        // 只有primary会走到这里
        if (ignore_indexs.count(index_id) == 1) {
            access_path->hint = AccessPath::IGNORE_INDEX;
        }
        access_path->field_range_map = field_range_map;
        access_path->table_info_ptr = table_info;
        access_path->index_info_ptr = info_ptr;
        access_path->pri_info_ptr = pri_ptr;
        access_path->index_type = index_type;
        access_path->tuple_id = tuple_id;
        access_path->table_id = table_id;
        access_path->index_id = index_id;
        if (index_info.index_hint_status == pb::IHS_VIRTUAL) {
            access_path->is_virtual = true;
        } 
        Property sort_property;
        if (sort_node != nullptr) {
            sort_property = sort_node->sort_property();
        }
        if (_ctx != nullptr && _ctx->efsearch != -1) {
            sort_property.efsearch = _ctx->efsearch;
        }
        access_path->calc_index_match(sort_property);

        if (index_info.type == pb::I_FULLTEXT && index_info.index_hint_status == pb::IHS_NORMAL && access_path->is_possible) {
            if (index_info.fields.size() == 1) {
                // TODO: 两种类型倒排统一后类型不再需要index_info.storage_type
                if (fulltext_fields_exact_like_map.count((index_info.fields[0].id << 5) + index_info.storage_type) == 1) {
                    DB_WARNING("skip fulltext index %ld", index_id);
                    continue;
                } else {
                    fulltext_fields_exact_like_map[(index_info.fields[0].id << 5) + index_info.storage_type] = access_path->is_exact_like;
                }
            } else {
                DB_FATAL("index %ld fulltext fields number error.", index_id);
                continue;
            }
        }

        access_path->set_pushed_join_on_condition(pushed_join_on_condition);
        access_path->insert_no_cut_condition(expr_field_map, scan_node->is_get_keypoint(), can_use_column_storage);
        access_paths.emplace_back(access_path);
    }

    std::set<int32_t> slot_ids;
    // 非相关子查询时，内层SQL使用的tuple_descs包含了外层SQL的字段，导致计算covering_index错误。
    // 当使用的是全局索引时，会导致无效的回表。
    // 解决：select语句使用ref_slot_id_mapping计算是否为covering index
    // TODO: 当外层为UPDATE或DELETE时，计算covering_index可能错误
    if (_ctx != nullptr && (_ctx->is_select || _ctx->expr_params.is_expr_subquery)) {
        auto& required_slot_map = _ctx->ref_slot_id_mapping[tuple_id];
        for (auto& iter : required_slot_map) {
            slot_ids.insert(iter.second);
        }
    }
    for (auto access_path: access_paths) {
        access_path->calc_is_covering_index(tuple_descs[tuple_id], slot_ids, fulltext_fields_exact_like_map);
        scan_node->add_access_path(access_path);
    }
    if (pushed_join_on_condition) {
        return scan_node->select_join_index_in_baikaldb(sample_sql); 
    }
    // 分区表解析分区信息
    if (select_partition(table_info, scan_node, *field_range_map) != 0) {
        DB_WARNING("Fail to select_partition, table_id: %ld", table_id);
        return -1;
    }
    scan_node->set_fulltext_index_tree(std::move(fulltext_index_tree));
    return scan_node->select_index_in_baikaldb(sample_sql); 
}

int IndexSelector::select_partition(SmartTable& table_info, ScanNode* scan_node,
    std::map<int32_t, range::FieldRange>& field_range_map) {
    if (table_info->partition_ptr != nullptr) {
        std::set<int64_t> partition_ids;
        int64_t table_id = table_info->id;
        if (_ctx != nullptr) {
            auto name_iter = _ctx->table_partition_names.find(table_id);
            if (name_iter != _ctx->table_partition_names.end()) {
                if (0 != _factory->get_partition_ids_by_name(table_id, name_iter->second, partition_ids)) {
                    DB_WARNING("get partition failed.");
                    return -1;
                }
                scan_node->replace_partition(partition_ids, true);
                return 0;
            }
        }
        
        bool is_read = false;
        std::shared_ptr<UserInfo> user_info = nullptr;
        if (_ctx != nullptr) {
            if (_ctx->stmt_type == parser::NT_SELECT || _ctx->stmt_type == parser::NT_UNION) {
                is_read = true;
            }
            auto client_conn = _ctx->client_conn;
            if (client_conn == nullptr) {
                DB_WARNING("client_conn is nullptr");
                return -1;
            }
            user_info = client_conn->user_info;
        }

        scan_node->set_partition_field_id(table_info->partition_ptr->partition_field_id());
        auto partition_type = table_info->partition_ptr->partition_type();
        auto field_iter = field_range_map.find(table_info->partition_ptr->partition_field_id());

        if (partition_type != pb::PT_HASH && partition_type != pb::PT_RANGE) {
            DB_WARNING("Invalid partition type, %d", partition_type);
            return -1;
        }

        if (partition_type == pb::PT_HASH) {
            if (field_iter != field_range_map.end()
                && (field_iter->second.type == EQ || field_iter->second.type == IN || field_iter->second.type == LIKE_EQ) 
                && !field_iter->second.eq_in_values.empty()) {
                ExprValueFlatSet eq_in_values_set;
                eq_in_values_set.init(ajust_flat_size(field_iter->second.eq_in_values.size()));
                for (auto& value : field_iter->second.eq_in_values) {
                    eq_in_values_set.insert(value);
                }
                if (table_info->partition_num != 1) {
                    for (auto& value : eq_in_values_set) {
                        int64_t partition_index = 0;
                        partition_index = table_info->partition_ptr->calc_partition(user_info, value);
                        if (partition_index < 0) {
                            DB_WARNING("get partition number error, value:%s", value.get_string().c_str());
                            return -1;
                        }
                        scan_node->add_expr_partition_pair(value.get_string(), partition_index);
                        partition_ids.emplace(partition_index);
                    }
                    scan_node->replace_partition(partition_ids, false);
                }
            } else {
                DB_WARNING("table_id:%ld pattern not supported.", table_id);
                for (int64_t i = 0; i < table_info->partition_num; ++i) {
                    partition_ids.emplace(i);
                }
                scan_node->replace_partition(partition_ids, false);
            }
        } else if (partition_type == pb::PT_RANGE) {
            RangePartition* partition_ptr = static_cast<RangePartition*>(table_info->partition_ptr.get());
            if (field_iter != field_range_map.end()) {
                if (!field_iter->second.eq_in_values.empty()) {
                    // 等值条件
                    const size_t MAX_FLATSET_INIT_VALUE = 12501; // 10000 / 0.8 + 1
                    size_t flatset_init_value = field_iter->second.eq_in_values.size() / 0.8 + 1;
                    if (flatset_init_value > MAX_FLATSET_INIT_VALUE) {
                        flatset_init_value = MAX_FLATSET_INIT_VALUE;
                    }
                    scan_node->set_partition_field_id(field_iter->first);
                    ExprValueFlatSet eq_in_values_set;
                    eq_in_values_set.init(flatset_init_value);
                    for (auto& value : field_iter->second.eq_in_values) {
                        eq_in_values_set.insert(value);
                    }
                    for (auto& value : eq_in_values_set) {
                        // 如果命中2个分区，第一个分区id作为返回值，第二个分区id存储在another_partition_id中；
                        // 如果命中1个分区，这个分区id直接作为返回值；
                        // 如果命中0个分区，则直接返回-1；
                        int64_t another_partition_id = -1;
                        int64_t partition_index = partition_ptr->calc_partition(user_info, value, is_read, &another_partition_id);
                        if (partition_index < 0) {
                            // 未找到partition，则跳过
                            continue;
                        }
                        scan_node->add_expr_partition_pair(value.get_string(), partition_index);
                        partition_ids.emplace(partition_index);
                        if (another_partition_id != -1) {
                            scan_node->add_expr_partition_pair(value.get_string(), another_partition_id);
                            partition_ids.emplace(another_partition_id);
                        }
                    }
                } else {
                    // 范围条件
                    bool left_open = false;
                    bool right_open = false;
                    ExprValue left_value;
                    ExprValue right_value;
                    // 行值表达式第一个Slot如果不是分区列，会退化成获取所有分区
                    if (field_iter != field_range_map.end()) {
                        if (!field_iter->second.left.empty()) {
                            left_value = field_iter->second.left[0];
                            left_open = field_iter->second.left_open;
                        }
                        if (!field_iter->second.right.empty()) {
                            right_value = field_iter->second.right[0];
                            right_open = field_iter->second.right_open;
                        }
                    }
                    if (partition_ptr->calc_partitions(user_info, 
                            left_value, left_open, right_value, right_open, partition_ids, is_read) != 0) {
                        DB_WARNING("Fail to calc_partitions");
                        return -1;
                    }
                }
            } else {
                if (partition_ptr->get_specified_partition_ids(user_info, partition_ids, is_read) != 0) {
                    DB_WARNING("Fail to calc_partitions");
                    return -1;
                }
            }
            scan_node->replace_partition(partition_ids, false);
        }
    }
    return 0;
}

// 检查ROLLUP索引是否要选择
bool IndexSelector::check_rollup_index_valid(SmartTable& table_info,
                                    const IndexInfo& index_info, 
                                    FilterNode* filter_node, 
                                    SortNode* sort_node,
                                    PacketNode* packet_node,
                                    AggNode* agg_node,
                                    FilterNode* having_filter_node,
                                    WindowNode* window_node,
                                    std::map<int32_t, range::FieldRange>& field_range_map) {
    if (packet_node == nullptr || packet_node->op_type() != pb::OP_SELECT) {
        return false;
    }
    if (window_node != nullptr) {
        // 窗口函数不支持rollup索引
        return false;
    }

    std::unordered_set<int32_t> rollup_key_check_set;   // query中需要在指标中的列
    std::unordered_set<int32_t> rollup_value_check_set; // query中需要在维度中的列
    std::unordered_set<int32_t> rollup_kv_check_set;    // query中需要在指标 + 维度中的列

    std::unordered_set<int32_t> fields_value_set;       // rollup索引中的指标列
    std::unordered_set<int32_t> fields_key_set;         // rollup索引中的维度列
    std::unordered_set<int32_t> fields_kv_set;          // rollup索引中的指标列 + 维度列

    for (ExprNode* projection: packet_node->mutable_projections()) {
        if (projection->is_slot_ref()) {
            projection->get_all_field_ids(rollup_key_check_set);
        }
    }
    if (agg_node == nullptr) {
        return false;
    }
    for (ExprNode* group_expr: *(agg_node->mutable_group_exprs())) {
        group_expr->get_all_field_ids(rollup_key_check_set);
    }
    for (AggFnCall* agg_fn_call: *(agg_node->mutable_agg_fn_calls())) {
        if (name_to_rollup_type_map[agg_fn_call->agg_func_name()] != index_info.rollup_type) {
            return false;
        }
        if (agg_fn_call->is_children_single_slot_ref()) {
            agg_fn_call->get_all_field_ids(rollup_value_check_set);
        } else {
            return false;
        }
    }
    if (sort_node != nullptr) {
        for (ExprNode* order_expr: *(sort_node->mutable_order_exprs())) {
            order_expr->get_all_field_ids(rollup_kv_check_set);
        }
    }
    if (filter_node != nullptr) {
        for (ExprNode* conjunct: *(filter_node->mutable_conjuncts())) {
            conjunct->get_all_field_ids(rollup_key_check_set);
        }
    }
    if (having_filter_node != nullptr) {
        for (ExprNode* conjunct: *(having_filter_node->mutable_conjuncts())) {
            // filter表达式优化后, field_id会丢失
            // 如 select id2,sum(score) as score_sum from t27  group by id2 having score_sum > 1, 【score_sum > 1】的field会变成0 
            std::unordered_set<int32_t> tmp_id_set;
            conjunct->get_all_field_ids(tmp_id_set);
            for (int32_t field_id: tmp_id_set) {
                if (field_id != 0) {
                    rollup_kv_check_set.insert(field_id);
                }
            }
        }
    }
    // ROLLUP指标列
    for (const FieldInfo& value_field: table_info->fields_need_sum) {
        fields_value_set.insert(value_field.id);
        fields_kv_set.insert(value_field.id);
    }
    // ROLLUP维度列
    for (const FieldInfo& key_field: index_info.fields) {
        fields_key_set.insert(key_field.id);
        fields_kv_set.insert(key_field.id);    
    }
    // 是否都在ROLLUP维度列中
    for (int32_t check_key: rollup_key_check_set) {
        if (fields_key_set.count(check_key) == 0) {
            return false;
        }
    }
    // 是否都在指标列中
    for (int32_t check_value: rollup_value_check_set) {
        if (fields_value_set.count(check_value) == 0) {
            return false;
        }
    }
    // 是否都在纬度列 + 指标列中
    for (int32_t check_kv: rollup_kv_check_set) {
        if (fields_kv_set.count(check_kv) == 0) {
            return false;
        }
    }
    return true;
}

int64_t IndexSelector::index_merge_selector(const std::vector<pb::TupleDescriptor>& tuple_descs,
                                    ScanNode* scan_node,
                                    FilterNode* filter_node,
                                    SortNode* sort_node,
                                    JoinNode* join_node,
                                    PacketNode* packet_node,
                                    AggNode* agg_node,
                                    FilterNode* having_filter_node,
                                    bool* index_has_null,
                                    std::map<int32_t, int>& field_range_type,
                                    const std::string& sample_sql) {
    if (join_node != nullptr) {
        // join_node 暂不处理
        return 0;
    }
    if (filter_node == nullptr) {
        return 0;
    }
    if (!scan_node->need_index_merge()) {
        return 0;
    }
    uint32_t select_index_score = scan_node->select_path()->prefix_ratio_index_score;
    scan_node->swap_index_info(scan_node->origin_index_info());
    std::vector<ExprNode*>& conjuncts_without_or = scan_node->conjuncts_without_or();
    std::vector<ExprNode*>& or_sub_conjuncts = scan_node->or_sub_conjuncts();
    bool use_index_merge = true;
    for (auto or_sub_conjunct: or_sub_conjuncts) {
        std::vector<ExprNode*> conjuncts = conjuncts_without_or;
        std::vector<ExprNode*> sub_conjuncts;
        or_sub_conjunct->flatten_and_expr(&sub_conjuncts);

        conjuncts.insert(conjuncts.end(), sub_conjuncts.begin(), sub_conjuncts.end());
        filter_node->modify_conjuncts(conjuncts); // filter_node's conjuncts are modified
        IndexSelectorOptions options;
        index_selector(tuple_descs, scan_node, filter_node, sort_node, join_node, packet_node, agg_node, having_filter_node, nullptr,
                       index_has_null, field_range_type, sample_sql, options); // filter_node's pruned_conjuncts are modified
        auto path = scan_node->select_path();
        if (scan_node->select_path()->prefix_ratio_index_score <= select_index_score &&
                scan_node->select_path()->prefix_ratio_index_score != UINT32_MAX) {
            use_index_merge = false;
            break;
        }
        if (!scan_node->has_index()) {
            use_index_merge = false;
            break;
        }
        bool select_index_contain_any_sub_conjunct = false;
        for (auto expr : sub_conjuncts) {
            if (scan_node->select_path()->need_cut_index_range_condition.count(expr) != 0) {
                select_index_contain_any_sub_conjunct = true;
                break;
            }
        }
        if (!select_index_contain_any_sub_conjunct) {
            use_index_merge = false;
            break;
        }
        scan_node->add_merge_index_info();
    }
    // filter_node->modifiy_pruned_conjuncts_by_index(scan_node->origin_index_info()._pruned_conjuncts); // 还原filter_node
    // scan_node中交换filter_node的_pruned_conjuncts
    scan_node->swap_index_info(scan_node->origin_index_info()); // 还原scan_node
    if (!use_index_merge) {
        scan_node->clear_merge_index_info();
    } else {
        scan_node->scan_indexs().clear(); // 避免scan_plan_router
    }
    return 0;
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
