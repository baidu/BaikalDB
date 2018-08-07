// Copyright (c) 2018 Baidu, Inc. All Rights Reserved.
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
#include "join_node.h"
#include "parser.h"

namespace baikaldb {
int IndexSelector::analyze(QueryContext* ctx) {
    ExecNode* root = ctx->root;
    std::vector<ExecNode*> scan_nodes;
    root->get_node(pb::SCAN_NODE, scan_nodes);
    if (scan_nodes.size() == 0) {
        return 0;
    }
    SortNode* sort_node = static_cast<SortNode*>(root->get_node(pb::SORT_NODE));
    JoinNode* join_node = static_cast<JoinNode*>(root->get_node(pb::JOIN_NODE));
    for (auto& scan_node_ptr : scan_nodes) {
        ExecNode* parent_node_ptr = scan_node_ptr->get_parent();
        if (parent_node_ptr == NULL) {
            continue;
        }
        if (parent_node_ptr->get_node_type() == pb::WHERE_FILTER_NODE
                || parent_node_ptr->get_node_type() == pb::TABLE_FILTER_NODE) {
            auto get_slot_id = [ctx](int32_t tuple_id, int32_t field_id)-> 
                    int32_t {return ctx->get_slot_id(tuple_id, field_id);};
            //有join节点暂时不考虑sort索引优化
            if (join_node != NULL) {
                index_selector(get_slot_id, 
                                ctx,
                                static_cast<ScanNode*>(scan_node_ptr), 
                                static_cast<FilterNode*>(parent_node_ptr), 
                                NULL,
                                &ctx->has_recommend);
            } else {
                index_selector(get_slot_id,
                               ctx,
                               static_cast<ScanNode*>(scan_node_ptr), 
                               static_cast<FilterNode*>(parent_node_ptr), 
                               sort_node,
                               &ctx->has_recommend);
            }
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
                                    bool* has_recommend) {
    int64_t table_id = scan_node->table_id();
    int32_t tuple_id = scan_node->tuple_id();
    pb::ScanNode* pb_scan_node = scan_node->mutable_pb_node()->
        mutable_derive_node()->mutable_scan_node();

    std::vector<ExprNode*>* conjuncts = filter_node->mutable_conjuncts();
    SchemaFactory* schema_factory = SchemaFactory::get_instance();
    std::vector<int64_t> index_ids = schema_factory->get_table_info(table_id).indices;
    if (pb_scan_node->use_indexes_size() != 0) {
        index_ids.clear();
        for (auto& index_id : pb_scan_node->use_indexes()) {
            index_ids.push_back(index_id);
        }
    }
    SmartRecord record_template = schema_factory->new_record(table_id);
    for (auto index_id : index_ids) {
        IndexInfo index_info = schema_factory->get_index_info(index_id); 
        pb::IndexType index_type = index_info.type;
        int left_field_cnt = 0;
        int right_field_cnt = 0;
        bool left_open = false;
        bool right_open = false;
        SmartRecord left_record = record_template->clone(false);
        SmartRecord right_record = record_template->clone(false);
        //SmartRecord eq_record = schema_factory->new_record(table_id);
        int field_cnt = 0;

        bool range_pred = true; //是否是范围(= > < >= <=)条件
        bool in_pred = false;   //是否是in条件

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
                switch (index_expr_type(expr, tuple_id, slot_id, index_type, &values)) {
                    case NONE:
                        expr_break = false;
                        field_break = true;
                        break;
                    case EQ:
                        left_field_cnt = field_cnt;
                        right_field_cnt = field_cnt;
                        left_open = false;
                        right_open = false;
                        left_record->set_value(left_record->get_field_by_tag(field.id), values[0]);
                        right_record->set_value(right_record->get_field_by_tag(field.id), values[0]);
                        expr_break = true;
                        field_break = false;
                        range_pred = true;
                        expr->add_filter_index(index_id);
                        break;
                    case LEFT_OPEN:
                        left_open = true;
                        left_field_cnt = field_cnt;
                        left_record->set_value(left_record->get_field_by_tag(field.id), values[0]);
                        expr_break = false;
                        field_break = true;
                        range_pred = true;
                        expr->add_filter_index(index_id);
                        break;
                    case LEFT_CLOSE:
                        left_open = false;
                        left_field_cnt = field_cnt;
                        left_record->set_value(left_record->get_field_by_tag(field.id), values[0]);
                        expr_break = false;
                        field_break = true;
                        range_pred = true;
                        expr->add_filter_index(index_id);
                        break;
                    case RIGHT_OPEN:
                        right_open = true;
                        right_field_cnt = field_cnt;
                        right_record->set_value(right_record->get_field_by_tag(field.id), values[0]);
                        expr_break = false;
                        field_break = true;
                        range_pred = true;
                        expr->add_filter_index(index_id);
                        break;
                    case RIGHT_CLOSE:
                        right_open = false;
                        right_field_cnt = field_cnt;
                        right_record->set_value(right_record->get_field_by_tag(field.id), values[0]);
                        expr_break = false;
                        field_break = true;
                        range_pred = true;
                        expr->add_filter_index(index_id);
                        break;
                    case LIKE:
                        //todo
                        left_open = false;
                        left_field_cnt = field_cnt;
                        left_record->set_value(left_record->get_field_by_tag(field.id), values[0]);
                        expr_break = true;
                        field_break = true;
                        if (index_type == pb::I_RECOMMEND) {
                            if (has_recommend != NULL) {
                                *has_recommend = true;
                            }
                        }
                        range_pred = false;
                        expr->add_filter_index(index_id);
                        break;
                    case OR_LIKE:
                        left_open = false;
                        left_field_cnt = field_cnt;
                        left_record->set_value(left_record->get_field_by_tag(field.id), values[0]);
                        expr_break = true;
                        field_break = true;
                        range_pred = false;
                        expr->add_filter_index(index_id);
                        break;
                    case IN: {
                        // 特殊逻辑
                        // left_field_cnt = 0;
                        // right_field_cnt = 0;
                        range_pred = false;
                        in_pred = true;
                        auto pos_index = pb_scan_node->add_indexes();
                        pos_index->set_index_id(index_id);
                        for (auto value : values) {
                            left_record->set_value(left_record->get_field_by_tag(field.id), value);
                            auto range = pos_index->add_ranges();
                            std::string str;
                            left_record->encode(str);
                            range->set_left_pb_record(str);
                            range->set_right_pb_record(str);
                            range->set_left_field_cnt(field_cnt);
                            range->set_right_field_cnt(field_cnt);
                            range->set_left_open(false);
                            range->set_right_open(false);
                        }
                        expr_break = true;
                        field_break = true;
                        expr->add_filter_index(index_id);
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

        if (left_field_cnt == 0 && right_field_cnt == 0 && !range_pred) {
            continue;
        }
        //如果是IN条件，PossibleIndex已经生成，且不考虑sort index.
        if (in_pred) {
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
        if (use_by_sort) {
            auto sort_index = pos_index->mutable_sort_index();
            sort_index->set_is_asc(is_asc);
            sort_index->set_sort_limit(sort_node->get_limit());
        }
        //DB_WARNING("is_monotonic: %d, range_pred: %d", sort_node && sort_node->is_monotonic(), range_pred);
    }
    if (pb_scan_node->indexes_size() == 0) {
        //主键扫描
        DB_WARNING("no index is selected, use primary key");
        pb::PossibleIndex* pos_index = pb_scan_node->add_indexes();
        pos_index->set_index_id(table_id);
        pos_index->add_ranges();
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
    return OR_LIKE;
}
#ifdef NEW_PARSER
IndexSelector::RangeType IndexSelector::index_expr_type(ExprNode* expr, int32_t tuple_id, int32_t slot_id,
        pb::IndexType index_type, std::vector<ExprValue>* values) {
    if (index_type == pb::I_FULLTEXT) {
        ExprValue value(pb::NULL_TYPE);
        auto type = or_like_index_type(expr, tuple_id, slot_id, &value);
        if (type == OR_LIKE) {
            values->push_back(value);
            return OR_LIKE;
        }
    }
    if (expr->children_size() < 2) {
        return NONE;
    }
    if (expr->children(0)->node_type() != pb::SLOT_REF) {
        return NONE;
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
        switch (expr->node_type()) {
            case pb::LIKE_PREDICATE:
                return LIKE;
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
        case pb::IS_NULL_PREDICATE:
            return EQ;
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
#else
IndexSelector::RangeType IndexSelector::index_expr_type(ExprNode* expr, int32_t tuple_id, int32_t slot_id,
        pb::IndexType index_type, std::vector<ExprValue>* values) {
    if (index_type == pb::I_FULLTEXT) {
        ExprValue value(pb::NULL_TYPE);
        auto type = or_like_index_type(expr, tuple_id, slot_id, &value);
        if (type == OR_LIKE) {
            values->push_back(value);
            return OR_LIKE;
        }
    }
    if (expr->children_size() < 2) {
        return NONE;
    }
    if (expr->children(0)->node_type() != pb::SLOT_REF) {
        return NONE;
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
        switch (expr->node_type()) {
            case pb::LIKE_PREDICATE:
                return LIKE;
            case pb::FUNCTION_CALL: {
                int32_t fn_op = static_cast<ScalarFnCall*>(expr)->fn().fn_op();
                if (fn_op == OP_EQ) {
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
    switch (expr->node_type()) {
        case pb::FUNCTION_CALL: {
            int32_t fn_op = static_cast<ScalarFnCall*>(expr)->fn().fn_op();
            switch (fn_op) {
                case OP_EQ:
                    return EQ;
                case OP_GE:
                    return LEFT_CLOSE;
                case OP_GT:
                    return LEFT_OPEN;
                case OP_LE:
                    return RIGHT_CLOSE;
                case OP_LT:
                    return RIGHT_OPEN;
                default:
                    return NONE;
            }
        }
        case pb::IS_NULL_PREDICATE:
            return EQ;
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
#endif
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
