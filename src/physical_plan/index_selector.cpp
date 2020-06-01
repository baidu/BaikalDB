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
#include "agg_node.h"
#include "parser.h"

namespace baikaldb {
using namespace range;
int IndexSelector::analyze(QueryContext* ctx) {
    ExecNode* root = ctx->root;
    std::vector<ExecNode*> scan_nodes;
    root->get_node(pb::SCAN_NODE, scan_nodes);
    if (scan_nodes.size() == 0) {
        return 0;
    }
    AggNode* agg_node = static_cast<AggNode*>(root->get_node(pb::AGG_NODE));
    SortNode* sort_node = static_cast<SortNode*>(root->get_node(pb::SORT_NODE));
    JoinNode* join_node = static_cast<JoinNode*>(root->get_node(pb::JOIN_NODE));
    for (auto& scan_node_ptr : scan_nodes) {
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
        if (join_node != NULL || agg_node != NULL) {
            ret =index_selector(ctx->tuple_descs(),
                            static_cast<ScanNode*>(scan_node_ptr), 
                            filter_node, 
                            NULL,
                            join_node,
                            &ctx->has_recommend);
        } else {
            ret = index_selector(ctx->tuple_descs(),
                           static_cast<ScanNode*>(scan_node_ptr), 
                           filter_node, 
                           sort_node,
                           join_node,
                           &ctx->has_recommend);
        }
        if (ret == -2) {
            ctx->return_empty = true;
            DB_WARNING("normal predicate compare whih null");
            return 0;
        } else if (ret < 0) {
            return ret;
        }
        if (ret > 0) {
            ctx->index_ids.insert(ret);
        }
        pb::ScanNode* pb_scan_node = static_cast<ScanNode*>(scan_node_ptr)->mutable_pb_node()->
            mutable_derive_node()->mutable_scan_node();
        if (pb_scan_node->indexes_size() == 0) {
            //主键扫描
            pb::PossibleIndex* pos_index = pb_scan_node->add_indexes();
            pos_index->set_index_id(static_cast<ScanNode*>(scan_node_ptr)->table_id());
            ctx->index_ids.insert(pos_index->index_id());
            pos_index->add_ranges();
        }
    }
    return 0;
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
        std::map<size_t, ExprValue> row_value;
        for (auto& pair : slots) {
            size_t idx = pair.first;
            values[idx].push_back(static_cast<RowExpr*>(expr->children(i))->get_value(nullptr, idx));
        }
    }
    if (values.begin()->second.size() == 0) {
        *index_predicate_is_null = is_index_predicate(expr);
        return;
    }
    std::vector<int32_t> field_ids;
    for (auto& pair : slots) {
        field_ids.push_back(pair.second->field_id());
    }
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
                field_range_map[field_id].eq_in_values = values[idx];
                field_range_map[field_id].conditions.insert(expr);
                field_range_map[field_id].type = IN;
                field_range_map[field_id].is_row_expr = true;
                field_range_map[field_id].left_row_field_ids = field_ids;
            }
            return;
        }
        default:
            return;
    }
}

void IndexSelector::hit_field_or_like_range(ExprNode* expr, std::map<int32_t, FieldRange>& field_range_map) {
    std::vector<ExprNode*> or_exprs;
    expr->flatten_or_expr(&or_exprs);
    for (auto sub_expr : or_exprs) {
        if (sub_expr->node_type() != pb::LIKE_PREDICATE) {
            return;
        }
        if (!sub_expr->children(0)->is_slot_ref()) {
            return;
        }
        if (!sub_expr->children(1)->is_literal()) {
            return;
        }
    }
    for (auto sub_expr : or_exprs) {
        int32_t field_id = static_cast<SlotRef*>(sub_expr->children(0))->field_id();
        field_range_map[field_id].like_values.push_back(sub_expr->children(1)->get_value(nullptr));
        field_range_map[field_id].conditions.insert(expr);
        field_range_map[field_id].type = OR_LIKE;
        if (static_cast<ScalarFnCall*>(sub_expr)->fn().fn_op() == parser::FT_EXACT_LIKE) {
            field_range_map[field_id].is_exact_like = true;
        }
    }
    return;
}

void IndexSelector::hit_match_against_field_range(ExprNode* expr, std::map<int32_t, FieldRange>& field_range_map) {
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
    }
    field_range_map[field_id].type = type;
    field_range_map[field_id].like_values.push_back(value);
    field_range_map[field_id].conditions.insert(expr);
    return;
}

void IndexSelector::hit_field_range(ExprNode* expr, 
        std::map<int32_t, FieldRange>& field_range_map, bool* index_predicate_is_null) {
    if (expr->node_type() == pb::OR_PREDICATE) { 
        return hit_field_or_like_range(expr, field_range_map);
    }
    if (expr->node_type() == pb::FUNCTION_CALL) {
        int32_t fn_op = static_cast<ScalarFnCall*>(expr)->fn().fn_op();
        if (fn_op == parser::FT_MATCH_AGAINST) {
            return hit_match_against_field_range(expr, field_range_map);
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
    switch (expr->node_type()) {
        case pb::FUNCTION_CALL: {
            int32_t fn_op = static_cast<ScalarFnCall*>(expr)->fn().fn_op();
            switch (fn_op) {
                case parser::FT_EQ:
                    field_range_map[field_id].eq_in_values = values;
                    field_range_map[field_id].conditions.insert(expr);
                    field_range_map[field_id].type = EQ;
                    return;
                case parser::FT_GE:
                case parser::FT_GT:
                    field_range_map[field_id].left.push_back(values[0]);
                    field_range_map[field_id].left_open = fn_op == parser::FT_GT;
                    field_range_map[field_id].left_expr = expr;
                    field_range_map[field_id].type = RANGE;
                    return;
                case parser::FT_LE:
                case parser::FT_LT:
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
            field_range_map[field_id].eq_in_values = values;
            field_range_map[field_id].conditions.insert(expr);
            if (values.size() == 1) {
                field_range_map[field_id].type = EQ;
            } else {
                field_range_map[field_id].type = IN;
            }
            return;
        }
        case pb::LIKE_PREDICATE: {
            field_range_map[field_id].like_values.push_back(values[0]);
            if (static_cast<ScalarFnCall*>(expr)->fn().fn_op() == parser::FT_EXACT_LIKE) {
                field_range_map[field_id].is_exact_like = true;
            }
            field_range_map[field_id].conditions.insert(expr);
            bool is_eq = false;
            bool is_prefix = false;
            ExprValue prefix_value(pb::STRING);
            static_cast<LikePredicate*>(expr)->hit_index(&is_eq, &is_prefix, &(prefix_value.str_val));
            if (is_eq) {
                field_range_map[field_id].eq_in_values.push_back(prefix_value);
                field_range_map[field_id].type = LIKE_EQ;
            } else if (is_prefix) {
                field_range_map[field_id].eq_in_values.push_back(prefix_value);
                field_range_map[field_id].type = LIKE_PREFIX;
            } else {
                field_range_map[field_id].type = LIKE;
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
                                    bool* has_recommend) {
    int64_t table_id = scan_node->table_id();
    int32_t tuple_id = scan_node->tuple_id();
    SchemaFactory* schema_factory = SchemaFactory::get_instance();
    auto table_info = schema_factory->get_table_info_ptr(table_id);
    if (table_info == nullptr) {
        DB_WARNING("table info not found:%ld", table_id);
        return -1;
    }

    pb::ScanNode* pb_scan_node = scan_node->mutable_pb_node()->
        mutable_derive_node()->mutable_scan_node();

    std::vector<ExprNode*>* conjuncts = filter_node ? filter_node->mutable_conjuncts() : nullptr;
    
    // 重构思路：先计算单个field的range范围，然后index根据field的范围情况进一步计算
    // field_id => range映射
    std::map<int32_t, FieldRange> field_range_map;
    // expr => field_ids
    std::map<ExprNode*, std::unordered_set<int32_t>> expr_field_map;
    if (conjuncts != nullptr) {
        for (auto expr : *conjuncts) {
            bool index_predicate_is_null = false;
            hit_field_range(expr, field_range_map, &index_predicate_is_null);
            if (index_predicate_is_null) {
                return -2;
            }
            expr->get_all_field_ids(expr_field_map[expr]);
        }
    }

    std::vector<int64_t> index_ids = table_info->indices;
    if (pb_scan_node->use_indexes_size() != 0) {
        index_ids.clear();
        for (auto& index_id : pb_scan_node->use_indexes()) {
            index_ids.push_back(index_id);
        }
    }
    std::set<int64_t> ignore_indexs;
    ignore_indexs.insert(std::begin(pb_scan_node->ignore_indexes()), 
            std::end(pb_scan_node->ignore_indexes()));

    auto pri_ptr = schema_factory->get_index_info_ptr(table_id); 
    if (pri_ptr == nullptr) {
        DB_WARNING("pk info not found:%ld", table_id);
        return -1;
    }
    SmartRecord record_template = schema_factory->new_record(table_id);
    for (auto index_id : index_ids) {
        if (ignore_indexs.count(index_id) == 1 && index_id != table_id) {
            continue;
        }
        auto info_ptr = schema_factory->get_index_info_ptr(index_id); 
        if (info_ptr == nullptr) {
            continue;
        }
        IndexInfo& index_info = *info_ptr;
        pb::IndexType index_type = index_info.type;
        auto index_state = index_info.state;
        if (index_state != pb::IS_PUBLIC) {
            DB_DEBUG("DDL_LOG index_selector skip index [%lld] state [%s] ", 
                index_id, pb::IndexState_Name(index_state).c_str());
            continue;
        }
        SmartPath access_path = std::make_shared<AccessPath>();
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
        Property sort_property;
        if (sort_node != nullptr) {
            sort_property = sort_node->sort_property();
        }
        access_path->calc_index_range(sort_property);
        access_path->insert_no_cut_condition(expr_field_map);
        access_path->calc_is_covering_index(tuple_descs[tuple_id]);
        scan_node->add_access_path(access_path);
        if (index_type == pb::I_RECOMMEND && access_path->is_possible) {
            if (has_recommend != NULL) {
                *has_recommend = true;
            }
        }
    }
    return scan_node->select_index_in_baikaldb(); 
}

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
