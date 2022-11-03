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
#include "limit_node.h"
#include "parser.h"

namespace baikaldb {
using namespace range;
int IndexSelector::analyze(QueryContext* ctx) {
    ExecNode* root = ctx->root;
    _ctx = ctx;
    std::vector<ExecNode*> scan_nodes;
    root->get_node(pb::SCAN_NODE, scan_nodes);
    if (scan_nodes.size() == 0) {
        return 0;
    }
    LimitNode* limit_node = static_cast<LimitNode*>(root->get_node(pb::LIMIT_NODE));
    AggNode* agg_node = static_cast<AggNode*>(root->get_node(pb::AGG_NODE));
    SortNode* sort_node = static_cast<SortNode*>(root->get_node(pb::SORT_NODE));
    JoinNode* join_node = static_cast<JoinNode*>(root->get_node(pb::JOIN_NODE));
    // TODO sort可以增加topN
    if (limit_node != nullptr && sort_node != nullptr) {
        sort_node->set_limit(limit_node->other_limit());
    }
    for (auto& scan_node_ptr : scan_nodes) {
        if (static_cast<ScanNode*>(scan_node_ptr)->engine() == pb::INFORMATION_SCHEMA) {
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
            ret =index_selector(ctx->tuple_descs(),
                            static_cast<ScanNode*>(scan_node_ptr), 
                            filter_node, 
                            NULL,
                            join_node,
                            &index_has_null,
                            field_range_type,
                            ctx->stat_info.sample_sql.str());
        } else {
            ret = index_selector(ctx->tuple_descs(),
                           static_cast<ScanNode*>(scan_node_ptr), 
                           filter_node, 
                           sort_node,
                           join_node,
                           &index_has_null,
                           field_range_type, 
                           ctx->stat_info.sample_sql.str());
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
        if (!expr->children(i)->is_row_expr()) {
            return;
        }
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
                if (field_range_map[field_id].type != NONE &&
                    field_range_map[field_id].eq_in_values.size() <= values[idx].size()) {
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
            int32_t field_id = static_cast<SlotRef*>(sub_expr->children(0))->field_id();
            // 所有字段都有arrow索引，才建立 or节点。
            // TODO 删除所有pb类型之后 删除该逻辑
            new_fulltext_flag = new_fulltext_flag && is_field_has_arrow_reverse_index(table_id, field_id, &index_id);
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

        int32_t field_id = static_cast<SlotRef*>(sub_expr->children(0))->field_id();
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
    }
    field_range_map[field_id].type = type;
    field_range_map[field_id].like_values.push_back(value);
    field_range_map[field_id].conditions.insert(expr);

    int64_t index_id = 0;
    if (is_field_has_arrow_reverse_index(table_id, field_id, &index_id)) {
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
            if (field_range_map[field_id].type != NONE &&
                field_range_map[field_id].eq_in_values.size() <= values.size()) {
                // 1个字段对应多个in preds时 例如: a in ("a1", "a2") and (a, b) in (("a1","b1")),取数量少的pred
                return;
            }
            field_range_map[field_id].eq_in_values = values;
            field_range_map[field_id].conditions.clear();
            field_range_map[field_id].conditions.insert(expr);
            if (values.size() == 1) {
                field_range_map[field_id].type = EQ;
            } else {
                field_range_map[field_id].type = IN;
            }
            return;
        }
        case pb::LIKE_PREDICATE: {
            range::FieldRange fulltext_and_range;
            field_range_map[field_id].like_values.push_back(values[0]);
            fulltext_and_range.like_values.push_back(values[0]);
            if (static_cast<ScalarFnCall*>(expr)->fn().fn_op() == parser::FT_EXACT_LIKE) {
                field_range_map[field_id].is_exact_like = true;
                fulltext_and_range.is_exact_like = true;
            }
            field_range_map[field_id].conditions.insert(expr);
            fulltext_and_range.conditions.insert(expr);
            bool is_eq = false;
            bool is_prefix = false;
            ExprValue prefix_value(pb::STRING);
            static_cast<LikePredicate*>(expr)->hit_index(&is_eq, &is_prefix, &(prefix_value.str_val));
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
            if (is_field_has_arrow_reverse_index(table_id, field_id, &index_id)) {
                auto& inner_node = boost::get<FulltextInfoNode::FulltextChildType>(fulltext_index_node->info);
                inner_node.children.emplace_back(new FulltextInfoNode);
                fulltext_and_range.left_row_field_ids.push_back(field_id);
                inner_node.children.back()->info = std::make_pair(index_id, std::move(fulltext_and_range));
                inner_node.children.back()->type = pb::FNT_TERM;
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
                                    bool* index_has_null,
                                    std::map<int32_t, int>& field_range_type,
                                    const std::string& sample_sql) {
    int64_t table_id = scan_node->table_id();
    int32_t tuple_id = scan_node->tuple_id();
    auto table_info = _factory->get_table_info_ptr(table_id);
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
            hit_field_range(expr, field_range_map, &index_predicate_is_null, table_id, fulltext_index_tree.root.get());
            if (index_predicate_is_null) {
                if (index_has_null != nullptr) {
                    *index_has_null = true;
                }
                break;
            }
            expr->get_all_field_ids(expr_field_map[expr]);
        }
    }

    for (auto& pair : field_range_map) {
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

    auto pri_ptr = _factory->get_index_info_ptr(table_id); 
    if (pri_ptr == nullptr) {
        DB_WARNING("pk info not found:%ld", table_id);
        return -1;
    }
    SmartRecord record_template = _factory->new_record(table_id);
    std::unordered_set<int64_t> fulltext_fields;
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
        if (index_info.type == pb::I_FULLTEXT && index_info.index_hint_status == pb::IHS_NORMAL) {
            if (index_info.fields.size() == 1) {
                if (fulltext_fields.count((index_info.fields[0].id << 5) + index_info.storage_type) == 1) {
                    DB_WARNING("skip fulltext index %ld", index_id);
                    continue;
                } else {
                    fulltext_fields.insert((index_info.fields[0].id << 5) + index_info.storage_type);
                }
            } else {
                DB_FATAL("index %ld fulltext fields number error.", index_id);
                continue;
            }
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
        access_path->calc_index_match(sort_property);
        std::set<int32_t>* calc_covering_user_slots = nullptr;
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
            if (!slot_ids.empty()) {
                calc_covering_user_slots = &slot_ids;
            }
        }
        access_path->calc_is_covering_index(tuple_descs[tuple_id], calc_covering_user_slots);
        scan_node->add_access_path(access_path);
    }
    scan_node->set_fulltext_index_tree(std::move(fulltext_index_tree));
    scan_node->set_expr_field_map(&expr_field_map);
    return scan_node->select_index_in_baikaldb(sample_sql); 
}

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
