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
        auto get_slot_id = [ctx](int32_t tuple_id, int32_t field_id)-> 
                int32_t {return ctx->get_slot_id(tuple_id, field_id);};
        //有join节点暂时不考虑sort索引优化
        if (join_node != NULL || agg_node != NULL) {
            index_selector(get_slot_id, 
                            ctx,
                            static_cast<ScanNode*>(scan_node_ptr), 
                            filter_node, 
                            NULL,
                            join_node,
                            &ctx->has_recommend);
        } else {
            index_selector(get_slot_id,
                           ctx,
                           static_cast<ScanNode*>(scan_node_ptr), 
                           filter_node, 
                           sort_node,
                           join_node,
                           &ctx->has_recommend);
        }
        pb::ScanNode* pb_scan_node = static_cast<ScanNode*>(scan_node_ptr)->mutable_pb_node()->
            mutable_derive_node()->mutable_scan_node();
        if (pb_scan_node->indexes_size() == 0) {
            //主键扫描
            pb::PossibleIndex* pos_index = pb_scan_node->add_indexes();
            pos_index->set_index_id(static_cast<ScanNode*>(scan_node_ptr)->table_id());
            pos_index->add_ranges();
        }
    }
    return 0;
}

void IndexSelector::index_selector(const std::function<int32_t(int32_t, int32_t)>& get_slot_id, 
                                    QueryContext* ctx,
                                    ScanNode* scan_node, 
                                    FilterNode* filter_node, 
                                    SortNode* sort_node,
                                    JoinNode* join_node,
                                    bool* has_recommend) {
    int64_t table_id = scan_node->table_id();
    int32_t tuple_id = scan_node->tuple_id();
    pb::ScanNode* pb_scan_node = scan_node->mutable_pb_node()->
        mutable_derive_node()->mutable_scan_node();

    std::vector<ExprNode*>* conjuncts = filter_node?filter_node->mutable_conjuncts():nullptr;
    if (conjuncts != nullptr) {
        // join时fetch完左表后会复用FilterNode, 需要重新获取possible index id
        for (auto expr : *conjuncts) {
            expr->clear_filter_index();
        }
    }
    SchemaFactory* schema_factory = SchemaFactory::get_instance();
    std::vector<int64_t> index_ids = schema_factory->get_table_info_ptr(table_id)->indices;
    if (pb_scan_node->use_indexes_size() != 0) {
        index_ids.clear();
        for (auto& index_id : pb_scan_node->use_indexes()) {
            index_ids.push_back(index_id);
        }
    }
    std::set<int64_t> ignore_indexs;
    for (auto& ignore_index_id : pb_scan_node->ignore_indexes()) {
        ignore_indexs.insert(ignore_index_id);
    }
    SmartRecord record_template = schema_factory->new_record(table_id);
    for (auto index_id : index_ids) {
        if (ignore_indexs.count(index_id) != 0 && index_id != table_id) {
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

        int left_field_cnt = 0;
        int right_field_cnt = 0;
        bool left_open = false;
        bool right_open = false;
        bool like_prefix = false;
        SmartRecord left_record = record_template->clone(false);
        SmartRecord right_record = record_template->clone(false);
        std::vector<SmartRecord> in_records;
        int field_cnt = 0;

        bool range_pred = false; //是否是范围(= > < >= <=)条件
        if (index_type == pb::I_PRIMARY ||
            index_type == pb::I_UNIQ ||
            index_type == pb::I_KEY) {
            range_pred = true;
        }
        bool in_pred = false;   //是否是in条件
        bool bool_and = false;   //是否LIKE AND, OR_LIKE是bool or
        bool in_part_pred = false; //是否RowExpr IN

        RangeType last_rg_type = NONE;
        for (auto field : index_info.fields) {
            ++field_cnt;
            int32_t slot_id = get_slot_id(tuple_id, field.id);
            bool field_break = true;
            if (!conjuncts) {
                break;
            }
            //扫描where条件看是否可以使用索引
            for (auto expr : *conjuncts) {
                std::vector<ExprValue> values;
                bool expr_break = true;
                RangeType rg_type = index_expr_type(last_rg_type, expr, tuple_id, slot_id, 
                        index_info, field_cnt, &values);
                // a,b联合索引，a in (1,2) and b=3也能使用索引
                if (in_pred && rg_type != EQ && !in_part_pred) {
                    rg_type = NONE;
                }
                if (rg_type != NONE) {
                    last_rg_type = rg_type;
                }
                switch (rg_type) {
                    case NONE:
                        expr_break = false;
                        field_break = true;
                        break;
                    case EQ_PART:
                    case EQ:
                        left_field_cnt = field_cnt;
                        right_field_cnt = field_cnt;
                        left_open = false;
                        right_open = false;
                        if (in_pred) {
                            for (auto record : in_records) {
                                record->set_value(record->get_field_by_tag(field.id), values[0]);
                            }
                            range_pred = false;
                        } else {
                            left_record->set_value(left_record->get_field_by_tag(field.id), values[0]);
                            right_record->set_value(right_record->get_field_by_tag(field.id), values[0]);
                            range_pred = true;
                        }
                        expr_break = true;
                        field_break = false;
                        if (index_type != pb::I_FULLTEXT && rg_type == EQ) {
                            expr->add_filter_index(index_id);
                        } else {
                            in_records.push_back(left_record);
                            left_record = left_record->clone(true);
                        }
                        break;
                    case LEFT_OPEN_PART:
                    case LEFT_OPEN:
                        left_open = true;
                        left_field_cnt = field_cnt;
                        left_record->set_value(left_record->get_field_by_tag(field.id), values[0]);
                        expr_break = false;
                        field_break = true;
                        if (rg_type == LEFT_OPEN_PART) {
                            field_break = false;
                        }
                        range_pred = true;
                        if (rg_type == LEFT_OPEN) {
                            expr->add_filter_index(index_id);
                        }
                        break;
                    case LEFT_CLOSE_PART:
                    case LEFT_CLOSE:
                        left_open = false;
                        left_field_cnt = field_cnt;
                        left_record->set_value(left_record->get_field_by_tag(field.id), values[0]);
                        expr_break = false;
                        field_break = true;
                        if (rg_type == LEFT_CLOSE_PART) {
                            field_break = false;
                        }
                        range_pred = true;
                        if (rg_type == LEFT_CLOSE) {
                            expr->add_filter_index(index_id);
                        }
                        break;
                    case RIGHT_OPEN_PART:
                    case RIGHT_OPEN:
                        right_open = true;
                        right_field_cnt = field_cnt;
                        right_record->set_value(right_record->get_field_by_tag(field.id), values[0]);
                        expr_break = false;
                        field_break = true;
                        if (rg_type == RIGHT_OPEN_PART) {
                            field_break = false;
                        }
                        range_pred = true;
                        if (rg_type == RIGHT_OPEN) {
                            expr->add_filter_index(index_id);
                        }
                        break;
                    case RIGHT_CLOSE_PART:
                    case RIGHT_CLOSE:
                        right_open = false;
                        right_field_cnt = field_cnt;
                        right_record->set_value(right_record->get_field_by_tag(field.id), values[0]);
                        expr_break = false;
                        field_break = true;
                        if (rg_type == RIGHT_CLOSE_PART) {
                            field_break = false;
                        }
                        range_pred = true;
                        if (rg_type == RIGHT_CLOSE) {
                            expr->add_filter_index(index_id);
                        }
                        break;
                    case LIKE_PART:
                    case LIKE:
                        //todo
                        left_open = false;
                        left_field_cnt = 1;
                        // planname LIKE 'a' AND planname LIKE 'b'
                        left_record = left_record->clone(true);
                        left_record->set_value(left_record->get_field_by_tag(field.id), values[0]);
                        in_records.push_back(left_record);
                        expr_break = false;
                        field_break = true;
                        if (index_type == pb::I_RECOMMEND) {
                            if (has_recommend != NULL) {
                                *has_recommend = true;
                            }
                        }
                        range_pred = false;
                        bool_and = true;
                        if (rg_type == LIKE) {
                            expr->add_filter_index(index_id);
                        }
                        break;
                    case LIKE_PREFIX:
                        left_open = false;
                        right_open = false;
                        left_field_cnt = field_cnt;
                        right_field_cnt = field_cnt;
                        left_record->set_value(left_record->get_field_by_tag(field.id), values[0]);
                        right_record->set_value(right_record->get_field_by_tag(field.id), values[0]);
                        like_prefix = true;
                        range_pred = true;
                        expr_break = true;
                        field_break = true;
                        break;
                    case OR_LIKE_PART:
                    case OR_LIKE:
                        left_open = false;
                        left_field_cnt = field_cnt;
                        left_record = left_record->clone(true);
                        left_record->set_value(left_record->get_field_by_tag(field.id), values[0]);
                        in_records.push_back(left_record);
                        expr_break = true;
                        field_break = true;
                        range_pred = false;
                        if (rg_type == OR_LIKE) {
                            expr->add_filter_index(index_id);
                        }
                        break;
                    case IN_PART:
                        in_part_pred = true;
                    case IN: {
                        // 特殊逻辑
                        left_field_cnt = field_cnt;
                        right_field_cnt = field_cnt;
                        range_pred = false;
                        in_pred = true;
                        if (in_records.empty()) {
                            for (auto value : values) {
                                auto record = left_record->clone(true);
                                record->set_value(record->get_field_by_tag(field.id), value);
                                in_records.push_back(record);
                            }
                        } else {
                            if (in_records.size() >= values.size()) {
                                for (size_t vi = 0; vi < values.size(); vi++) {
                                    auto record = in_records[vi];
                                    record->set_value(record->get_field_by_tag(field.id), values[vi]);
                                }
                            } else {
                                DB_FATAL("inx:%ld in_records.size() %lu values.size() %lu ", 
                                        index_id, in_records.size(), values.size());
                            }
                        }
                        expr_break = true;
                        field_break = false;
                        if (index_type != pb::I_FULLTEXT && rg_type == IN) {
                            in_part_pred = false;
                            expr->add_filter_index(index_id);
                        }
                        break;
                    }
                    case INDEX_HAS_NULL: {
                        if (ctx != nullptr) {
                            ctx->return_empty = true;
                        }
                        return;
                    }
                }
                if (expr_break) {
                    break;
                }
            }

            if (field_break) {
                break;
            }
        }
        //DB_WARNING("index:%ld range_pred:%d", index_id, range_pred);
        if (left_field_cnt == 0 && right_field_cnt == 0 && !range_pred) {
            continue;
        }
        //如果是IN条件，不考虑sort index.
        if (in_pred || index_type == pb::I_FULLTEXT || index_type == pb::I_RECOMMEND) {
            std::set<std::string> filter;
            auto pos_index = pb_scan_node->add_indexes();
            pos_index->set_index_id(index_id);
            pos_index->set_bool_and(bool_and);
            for (auto record : in_records) {
                std::string str;
                record->encode(str);
                if (filter.count(str) == 1) {
                    continue;
                }
                filter.insert(str);
                auto range = pos_index->add_ranges();
                range->set_left_pb_record(str);
                range->set_right_pb_record(str);
                range->set_left_field_cnt(left_field_cnt);
                range->set_right_field_cnt(right_field_cnt);
                range->set_left_open(false);
                range->set_right_open(false);
            }
            continue;
        }
        bool between = false;
        std::string str1;
        std::string str2;
        left_record->encode(str1);
        right_record->encode(str2);
        
        if (left_field_cnt == right_field_cnt && str1 != str2) {
            between = true;
        }
        // 扫描order by列表看是否可以使用索引
        // order by能用索引的条件(参考https://dev.mysql.com/doc/refman/5.7/en/order-by-optimization.html)
        // order by不能使用索引的条件:
        // 1) order by中混合升序和降序
        // 2) order by中的列跨越多个索引
        bool use_by_sort = false;
        bool is_asc = false;
        if (sort_node != nullptr && sort_node->is_monotonic() && range_pred) {
            const std::vector<ExprNode*>& order_exprs = sort_node->slot_order_exprs();
            is_asc = sort_node->is_asc()[0];

            if (left_field_cnt == right_field_cnt) { // (EQ)*(EQ|BETWEEN|null)
                if (between) {
                    use_by_sort = check_sort_use_index(get_slot_id, index_info, order_exprs, 
                        tuple_id, left_field_cnt - 1);
                } else {
                    use_by_sort = check_sort_use_index(get_slot_id, index_info, order_exprs, 
                        tuple_id, left_field_cnt);
                }
            } else if (left_field_cnt > right_field_cnt) { // (EQ)*(GE/GT)
                use_by_sort = check_sort_use_index(get_slot_id, index_info, order_exprs, tuple_id, right_field_cnt);
            } else if (left_field_cnt < right_field_cnt) { // (EQ)*(LE/LT)
                use_by_sort = check_sort_use_index(get_slot_id, index_info, order_exprs, tuple_id, left_field_cnt);
            }
            //DB_WARNING("index: %ld, field_count:%d, %d, sort_index:%d", index_id, left_field_cnt, right_field_cnt, use_by_sort);
        }
        if (left_field_cnt == 0 && right_field_cnt == 0 && !use_by_sort) {
            continue;
        }
        auto pos_index = pb_scan_node->add_indexes();
        pos_index->set_index_id(index_id);
        auto range = pos_index->add_ranges();
        range->set_left_pb_record(str1);
        range->set_right_pb_record(str2);
        range->set_left_field_cnt(left_field_cnt);
        range->set_right_field_cnt(right_field_cnt);
        range->set_left_open(left_open);
        range->set_right_open(right_open);
        range->set_like_prefix(like_prefix);
        if (use_by_sort && sort_node != nullptr) {
            auto sort_index = pos_index->mutable_sort_index();
            sort_index->set_is_asc(is_asc);
            sort_index->set_sort_limit(sort_node->get_limit());
        }
        //DB_WARNING("is_monotonic: %d, range_pred: %d", sort_node && sort_node->is_monotonic(), range_pred);
    }
    if (pb_scan_node->indexes_size() == 0) {
        //主键扫描
        //DB_WARNING("no index is selected, use primary key");
        pb::PossibleIndex* pos_index = pb_scan_node->add_indexes();
        pos_index->set_index_id(table_id);
        pos_index->add_ranges();
    }
    // 单表纯kv类优化，只主键索引时候过滤掉in条件
    if (join_node == NULL &&
        pb_scan_node->indexes_size() == 1 && 
        pb_scan_node->indexes(0).index_id() == table_id) {
        if (filter_node != nullptr) {
            filter_node->remove_primary_conjunct(table_id);
        }
    }
    //DB_WARNING("pb_scan_node: %s", pb_scan_node->DebugString().c_str());
}

bool IndexSelector::check_sort_use_index(const std::function<int32_t(int32_t, int32_t)>& get_slot_id, 
        IndexInfo& index_info, 
        const std::vector<ExprNode*>& order_exprs, 
        int32_t tuple_id, 
        uint32_t field_cnt) {
    bool sort_use_index = true;
    SlotRef* slot_ref = static_cast<SlotRef*>(order_exprs[0]);
    uint32_t idx = 0;
    // 搜索index field中匹配order by第一列的位置
    for (; idx < index_info.fields.size(); ++idx) {
        int32_t slot_id = get_slot_id(tuple_id, index_info.fields[idx].id);
        if (tuple_id == slot_ref->tuple_id() && slot_id == slot_ref->slot_id()) {
            break;
        }
    }
    //找到了, 且位置能够和索引的前缀条件对接上
    if (idx < index_info.fields.size() && idx <= field_cnt) { // left_field_cnt == right_field_cnt
        uint32_t order_idx = 0;
        for (; order_idx < order_exprs.size(); ++order_idx) {
            if (idx + order_idx >= index_info.fields.size()) {
                break;
            }
            SlotRef* slot_ref = static_cast<SlotRef*>(order_exprs[order_idx]);
            int32_t slot_id = get_slot_id(tuple_id, index_info.fields[idx + order_idx].id);
            if (tuple_id != slot_ref->tuple_id() || slot_id != slot_ref->slot_id()) {
                break;
            }
        }
        if (order_idx < order_exprs.size()) {
            sort_use_index = false;
        }
    } else {
        sort_use_index = false;
    }
    return sort_use_index;
}

IndexSelector::RangeType IndexSelector::or_like_index_type(
        ExprNode* expr, int32_t tuple_id, int32_t slot_id, ExprValue* value) {
    if (expr->node_type() != pb::OR_PREDICATE) {
        return NONE;
    }
    ExprNode* slot = expr->get_slot_ref(tuple_id, slot_id);
    if (slot == nullptr) {
        return NONE;
    }
    ExprNode* parent = expr->get_parent(slot);
    if (parent == nullptr || parent->node_type() != pb::LIKE_PREDICATE) {
        return NONE;
    }
    if (!parent->children(1)->is_literal()) {
        return NONE;
    }
    *value = parent->children(1)->get_value(nullptr);
    if (static_cast<ScalarFnCall*>(parent)->fn().fn_op() == parser::FT_EXACT_LIKE) {
        return OR_LIKE_PART;
    } else {
        return OR_LIKE;
    }
}

IndexSelector::RangeType IndexSelector::index_expr_type(RangeType last_rg_type, ExprNode* expr, 
        int32_t tuple_id, int32_t slot_id,
        const IndexInfo& index_info, int field_cnt, std::vector<ExprValue>* values) {
    auto index_type = index_info.type;
    if (index_type == pb::I_FULLTEXT) {
        ExprValue value(pb::NULL_TYPE);
        auto type = or_like_index_type(expr, tuple_id, slot_id, &value);
        if (type == OR_LIKE || type == OR_LIKE_PART) {
            values->push_back(value);
            return type;
        }
    }
    if (expr->children_size() < 2) {
        return NONE;
    }
    if (expr->children(0)->is_row_expr()) {
        // a > 2 and (a,b) > (1,1) 
        // a > 2命中索引后，(a,b) > (1,1)的后半段不能接着
        // TODO:更好的做法是可以把条件(a,b)>(1,1)裁剪掉
        switch (last_rg_type) {
            case LEFT_OPEN:
            case LEFT_CLOSE:
            case RIGHT_OPEN:
            case RIGHT_CLOSE:
                return NONE;
            default:
                break;
        }
        return index_row_expr_type(expr, tuple_id, slot_id, index_info, field_cnt, values);
    }
    if (!expr->children(0)->is_slot_ref()) {
        return NONE;
    }
    // b > 2 and (a,b) > (1,1)
    // b > 2 不能接着前一个LEFT_CLOSE_PART条件
    switch (last_rg_type) {
        case EQ_PART:
        case IN_PART:
        case LEFT_OPEN_PART:
        case LEFT_CLOSE_PART:
        case RIGHT_OPEN_PART:
        case RIGHT_CLOSE_PART:
            return NONE;
        default:
            break;
    }
    SlotRef* slot_ref = static_cast<SlotRef*>(expr->children(0));
    if (slot_ref->tuple_id() != tuple_id || slot_ref->slot_id() != slot_id) {
        return NONE;
    }
    pb::PrimitiveType col_type = expr->children(0)->col_type();
    for (uint32_t i = 1; i < expr->children_size(); i++) {
        if (!expr->children(i)->is_constant()) {
            return NONE;
        }
        expr->children(i)->open();
        ExprValue val = expr->children(i)->get_value(nullptr);
        // 目前索引不允许为null
        if (!val.is_null()) {
            values->push_back(val.cast_to(col_type));
        }
    }
    if (values->size() == 0) {
        return INDEX_HAS_NULL;
    }
    if (index_type == pb::I_RECOMMEND) {
        if (expr->node_type() == pb::LIKE_PREDICATE) {
            return LIKE;
        } else {
            return NONE;
        }
    }
    // fulltext support normal type
    if (index_type == pb::I_FULLTEXT) {
        if (expr->node_type() == pb::LIKE_PREDICATE) {
            if (static_cast<ScalarFnCall*>(expr)->fn().fn_op() == parser::FT_EXACT_LIKE) {
                return LIKE_PART;
            } else {
                return LIKE;
            }
        }
        // only NO_SEGMENT can use in and =
        if (index_info.segment_type == pb::S_NO_SEGMENT) {
            switch (expr->node_type()) {
                case pb::FUNCTION_CALL: {
                    int32_t fn_op = static_cast<ScalarFnCall*>(expr)->fn().fn_op();
                    if (fn_op == parser::FT_EQ) {
                        return EQ;
                    } else {
                        return NONE;
                    }
                }
                case pb::IN_PREDICATE:
                    if (values->size() == 1) { 
                        return EQ;
                    } else {
                        return IN;
                    }
                default:
                    return NONE;
            }
        }
        return NONE;
    }
    switch (expr->node_type()) {
        case pb::FUNCTION_CALL: {
            int32_t fn_op = static_cast<ScalarFnCall*>(expr)->fn().fn_op();
            switch (fn_op) {
                case parser::FT_EQ:
                    return EQ;
                case parser::FT_GE:
                    return LEFT_CLOSE;
                case parser::FT_GT:
                    return LEFT_OPEN;
                case parser::FT_LE:
                    return RIGHT_CLOSE;
                case parser::FT_LT:
                    return RIGHT_OPEN;
                default:
                    return NONE;
            }
        }
        case pb::IN_PREDICATE:
            if (values->size() == 1) { 
                return EQ;
            } else {
                return IN;
            }
        case pb::LIKE_PREDICATE: {
            bool is_eq = false;
            bool is_prefix = false;
            ExprValue prefix_value(pb::STRING);
            static_cast<LikePredicate*>(expr)->hit_index(&is_eq, &is_prefix, &(prefix_value.str_val));
            if (is_eq) {
                return EQ;
            } else if (is_prefix) {
                values->clear();
                values->push_back(prefix_value);
                return LIKE_PREFIX;
            } else {
                return NONE;
            }
        }
        default:
            return NONE;
    }
}
// https://dev.mysql.com/doc/refman/5.6/en/comparison-operators.html#operator_less-than-or-equal
IndexSelector::RangeType IndexSelector::index_row_expr_type(ExprNode* expr, 
        int32_t tuple_id, int32_t slot_id,
        const IndexInfo& index_info, int field_cnt, std::vector<ExprValue>* values) {
    if (index_info.type == pb::I_FULLTEXT || index_info.type == pb::I_RECOMMEND) {
        return NONE;
    }
    RowExpr* row_expr = static_cast<RowExpr*>(expr->children(0));
    row_expr->open();
    int idx = row_expr->get_slot_ref_idx(tuple_id, slot_id);
    if (idx == -1) {
        return NONE;
    }
    size_t row_size = row_expr->children_size();
    pb::PrimitiveType col_type = row_expr->children(idx)->col_type();
    for (uint32_t i = 1; i < expr->children_size(); i++) {
        if (!expr->children(i)->is_constant()) {
            return NONE;
        }
        auto val = static_cast<RowExpr*>(expr->children(i))->get_value(nullptr, idx);
        if (!val.is_null()) {
            values->push_back(val.cast_to(col_type));
        }
    }
    if (values->size() == 0) {
        return INDEX_HAS_NULL;
    }
    switch (expr->node_type()) {
        case pb::FUNCTION_CALL: {
            int32_t fn_op = static_cast<ScalarFnCall*>(expr)->fn().fn_op();
            switch (fn_op) {
                case parser::FT_EQ:
                // 全部匹配才能裁剪条件
                if (row_size == field_cnt) {
                    return EQ;
                } else {
                    return EQ_PART;
                }
                case parser::FT_GE:
                    if (idx + 1 != field_cnt) {
                        return NONE;
                    } else if (row_size == field_cnt) {
                        return LEFT_CLOSE;
                    } else {
                        return LEFT_CLOSE_PART;
                    }
                case parser::FT_GT:
                    if (idx + 1 != field_cnt) {
                        return NONE;
                    } else if (row_size == field_cnt) {
                        return LEFT_OPEN;
                    } else {
                        // 前缀匹配时只能闭区间
                        // 例如:索引a, (a,b) > (1,1)只能命中a索引闭区间，因为a=1时(1,0)也能满足条件 < (1,1)
                        return LEFT_CLOSE_PART;
                    }
                case parser::FT_LE:
                    if (idx + 1 != field_cnt) {
                        return NONE;
                    } else if (row_size == field_cnt) {
                        return RIGHT_CLOSE;
                    } else {
                        return RIGHT_CLOSE_PART;
                    }
                case parser::FT_LT:
                    if (idx + 1 != field_cnt) {
                        return NONE;
                    } else if (row_size == field_cnt) {
                        return RIGHT_OPEN;
                    } else {
                        return RIGHT_CLOSE_PART;
                    }
                default:
                    return NONE;
            }
        }
        case pb::IN_PREDICATE:
            if (values->size() == 1) { 
                if (row_size == field_cnt) {
                    return EQ;
                } else {
                    return EQ_PART;
                }
            } else {
                if (row_size == field_cnt) {
                    return IN;
                } else {
                    return IN_PART;
                }
            }
        default:
            return NONE;
    }
}

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
