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

#include "access_path.h"
#include "slot_ref.h"

namespace baikaldb {
using namespace range;

void AccessPath::calc_row_expr_range(std::vector<int32_t>& range_fields, ExprNode* expr, bool in_open,
        std::vector<ExprValue>& values, SmartRecord record, size_t field_idx, bool* out_open, int* out_field_cnt) {
    if (expr == nullptr) {
        return;
    }
    size_t row_idx = 0;
    for (; row_idx < range_fields.size() && 
            field_idx < index_info_ptr->fields.size(); row_idx++, field_idx++) {
        if (index_info_ptr->fields[field_idx].id == range_fields[row_idx]) {
            record->set_value(record->get_field_by_tag(range_fields[row_idx]), values[row_idx]);
        } else {
            break;
        }
    }
    *out_field_cnt = field_idx;
    if (row_idx == range_fields.size()) {
        *out_open = in_open;
        need_cut_index_range_condition.insert(expr);
    } else {
        *out_open = false;
    }
}

// 这块暂时复用之前的，之后要考虑先过滤掉等于符号再判断
// 例如索引a,b,c,d; where a=1 and b=1 and c=1 order by a,d 可以不用排序，现在不行，还会走次排序
bool AccessPath::check_sort_use_index(Property& sort_property) {
    if (sort_property.slot_order_exprs.empty()) {
        return false;
    }
    std::vector<ExprNode*>& order_exprs = sort_property.slot_order_exprs;
    SlotRef* slot_ref = static_cast<SlotRef*>(order_exprs[0]);
    size_t idx = 0;
    auto& fields = index_info_ptr->fields;
    for (; idx < fields.size(); ++idx) {
        if (tuple_id == slot_ref->tuple_id() && fields[idx].id == slot_ref->field_id()) {
            break;
        }
    }
    if (idx < fields.size() && idx <= eq_count) {
        size_t order_idx = 0;
        for (; order_idx < order_exprs.size() && idx < fields.size(); order_idx++, idx++) {
            SlotRef* slot_ref = static_cast<SlotRef*>(order_exprs[order_idx]);
            if (tuple_id != slot_ref->tuple_id() || fields[idx].id != slot_ref->field_id()) {
                break;
            }
        }
        if (order_idx < order_exprs.size()) {
            return false;
        }
        return true;
    } else {
        return false;
    }
}
// 现在只支持CNF，DNF怎么做?
// 普通索引按照range匹配，匹配到EQ可以往下走，匹配到RANGE、LIKE_PREFIX停止
// 匹配到IN，如果之前是IN停止（row_expr除外）
// 条件裁剪，对于普通条件，除了LIKE_PREFIX都裁剪
// 对于row_expr，满足全命中索引range后裁剪
void AccessPath::calc_normal(Property& sort_property) {
    int left_field_cnt = 0;
    int right_field_cnt = 0;
    bool left_open = false;
    bool right_open = false;
    bool like_prefix = false;
    bool in_pred = false;
    ExprNode* in_row_expr = nullptr;
    SmartRecord record_template = SchemaFactory::get_instance()->new_record(table_id);
    SmartRecord left_record = record_template->clone(false);
    SmartRecord right_record = record_template->clone(false);
    std::vector<SmartRecord> in_records;
    int field_cnt = 0;
    for (auto& field : index_info_ptr->fields) {
        bool field_break = false;
        auto iter = field_range_map.find(field.id);
        if (iter == field_range_map.end()) {
            break;
        }
        FieldRange& range = iter->second;
        switch (range.type) {
            case RANGE: 
                field_break = true;
                if (in_pred) {
                    break;
                }
                ++field_cnt;
                hit_index_field_ids.insert(field.id);
                if (range.is_row_expr) {
                    size_t field_idx = field_cnt - 1;
                    calc_row_expr_range(range.left_row_field_ids, range.left_expr, 
                            range.left_open, range.left, left_record, field_idx, &left_open, &left_field_cnt);
                    calc_row_expr_range(range.right_row_field_ids, range.right_expr, 
                            range.right_open, range.right, right_record, field_idx, &right_open, &right_field_cnt);
                } else {
                    if (range.left_expr != nullptr) {
                        left_record->set_value(left_record->get_field_by_tag(field.id), range.left[0]);
                        left_open = range.left_open;
                        left_field_cnt = field_cnt;
                        need_cut_index_range_condition.insert(range.left_expr);
                    }
                    if (range.right_expr != nullptr) {
                        right_record->set_value(right_record->get_field_by_tag(field.id), range.right[0]);
                        right_open = range.right_open;
                        right_field_cnt = field_cnt;
                        need_cut_index_range_condition.insert(range.right_expr);
                    }
                }
                break;
            case EQ:
            case LIKE_EQ:
                ++eq_count;
            case LIKE_PREFIX:
                ++field_cnt;
                hit_index_field_ids.insert(field.id);
                left_field_cnt = field_cnt;
                right_field_cnt = field_cnt;
                left_open = false;
                right_open = false;
                if (in_records.empty()) {
                    left_record->set_value(left_record->get_field_by_tag(field.id), range.eq_in_values[0]);
                    right_record->set_value(right_record->get_field_by_tag(field.id), range.eq_in_values[0]);
                } else {
                    for (auto record : in_records) {
                        record->set_value(record->get_field_by_tag(field.id), range.eq_in_values[0]);
                    }
                }
                if (range.type == LIKE_PREFIX) {
                    like_prefix = true;
                    field_break = true;
                } else if (range.is_row_expr) {
                    if (all_in_index(range.left_row_field_ids, index_field_ids)) {
                        need_cut_index_range_condition.insert(range.conditions.begin(), range.conditions.end());
                    }
                } else {
                    need_cut_index_range_condition.insert(range.conditions.begin(), range.conditions.end());
                }
                break;
            case IN:
                if (in_pred && *range.conditions.begin() != in_row_expr) {
                    field_break = true;
                    break;
                }
                if (in_records.empty()) {
                    for (auto value : range.eq_in_values) {
                        auto record = left_record->clone(true);
                        record->set_value(record->get_field_by_tag(field.id), value);
                        in_records.push_back(record);
                    }
                } else {
                    if (in_records.size() == range.eq_in_values.size()) {
                        for (size_t vi = 0; vi < range.eq_in_values.size(); vi++) {
                            auto record = in_records[vi];
                            record->set_value(record->get_field_by_tag(field.id), range.eq_in_values[vi]);
                        }
                    } else {
                        DB_FATAL("inx:%ld in_records.size() %lu != values.size() %lu ", 
                                index_id, in_records.size(), range.eq_in_values.size());
                        field_break = true;
                        break;
                    }
                }
                ++field_cnt;
                hit_index_field_ids.insert(field.id);
                left_field_cnt = field_cnt;
                right_field_cnt = field_cnt;
                left_open = false;
                right_open = false;
                in_pred = true;
                if (range.is_row_expr) {
                    in_row_expr = *range.conditions.begin();
                    if (all_in_index(range.left_row_field_ids, index_field_ids)) {
                        need_cut_index_range_condition.insert(range.conditions.begin(), range.conditions.end());
                    }
                } else {
                    need_cut_index_range_condition.insert(range.conditions.begin(), range.conditions.end());
                }
                break;
            default:
                break;
        }
        if (field_break) {
            break;
        }
    }
    if (!in_pred) {
        is_sort_index = check_sort_use_index(sort_property);
    }
    DB_DEBUG("is_sort_index:%d, eq_count:%d, sort_property:%lu", 
        is_sort_index, eq_count, sort_property.slot_order_exprs.size());
    pos_index.set_index_id(index_id);
    if (is_sort_index) {
        auto sort_index = pos_index.mutable_sort_index();
        sort_index->set_is_asc(sort_property.is_asc[0]);
        sort_index->set_sort_limit(sort_property.expected_cnt);
    }
    if (left_field_cnt == 0 && right_field_cnt == 0) {
        pos_index.add_ranges();
    } else if (in_pred) {
        is_possible = true;
        std::set<std::string> filter;
        for (auto record : in_records) {
            std::string str;
            record->encode(str);
            if (filter.count(str) == 1) {
                continue;
            }
            filter.insert(str);
            auto range = pos_index.add_ranges();
            range->set_left_pb_record(str);
            range->set_right_pb_record(str);
            range->set_left_field_cnt(left_field_cnt);
            range->set_right_field_cnt(right_field_cnt);
            range->set_left_open(false);
            range->set_right_open(false);
            range->set_like_prefix(like_prefix);
        }
    } else {
        is_possible = true;
        auto range = pos_index.add_ranges();
        std::string str1;
        std::string str2;
        left_record->encode(str1);
        right_record->encode(str2);
        range->set_left_pb_record(str1);
        range->set_right_pb_record(str2);
        range->set_left_field_cnt(left_field_cnt);
        range->set_right_field_cnt(right_field_cnt);
        range->set_left_open(left_open);
        range->set_right_open(right_open);
        range->set_like_prefix(like_prefix);
    }
}

void AccessPath::calc_fulltext() {
    int32_t field_id = index_info_ptr->fields[0].id;
    auto iter = field_range_map.find(field_id);
    if (iter == field_range_map.end()) {
        return;
    }
    FieldRange& range = iter->second;
    bool hit_index = false;
    std::vector<ExprValue>* values = nullptr;
    auto range_type = range.type;
    switch (range.type) {
        case MATCH_LANGUAGE:
        case MATCH_BOOLEAN:
        case LIKE_PREFIX:
        case LIKE_EQ:
        case LIKE:
        case OR_LIKE:
            hit_index = true;
            if (!range.is_exact_like) {
                need_cut_index_range_condition.insert(range.conditions.begin(), range.conditions.end());
            }
            values = &range.like_values;
            if (range.type != OR_LIKE) {
                pos_index.set_bool_and(true);
            }
            break;
        case EQ:
        case IN:
            if (index_type == pb::I_FULLTEXT && index_info_ptr->segment_type == pb::S_NO_SEGMENT) {
                hit_index = true;
                need_cut_index_range_condition.insert(range.conditions.begin(), range.conditions.end());
                values = &range.eq_in_values;
            }
            break;
        default:
            break;
    }
    if (hit_index && values != nullptr) {
        SmartRecord record_template = SchemaFactory::get_instance()->new_record(table_id);
        is_possible = true;
        pos_index.set_index_id(index_id);
        std::set<std::string> filter;
        for (auto value : *values) {
            record_template->set_value(record_template->get_field_by_tag(field_id), value);
            std::string str;
            record_template->encode(str);
            if (filter.count(str) == 1) {
                continue;
            }
            filter.insert(str);
            auto range = pos_index.add_ranges();
            range->set_left_pb_record(str);
            range->set_right_pb_record(str);
            range->set_left_field_cnt(1);
            range->set_right_field_cnt(1);
            range->set_left_open(false);
            range->set_right_open(false);
            if (range_type == MATCH_LANGUAGE) {
                range->set_match_mode(pb::M_NARUTAL_LANGUAGE);
            } else if (range_type == MATCH_BOOLEAN) {
                range->set_match_mode(pb::M_BOOLEAN);
            }
        }
    } else {
        pos_index.set_index_id(index_id);
        pos_index.add_ranges();
    }
}
double AccessPath::calc_field_selectivity(int32_t field_id, FieldRange& range) {
    switch (range.type) {
        case RANGE: {
            ExprValue left;
            ExprValue right;
            if (range.left_expr != nullptr) {
                left = range.left[0];
            } 
            if (range.right_expr != nullptr) {
                right = range.right[0];
            }
            //DB_WARNING("left:%d %s right:%d %s", left.type, left.get_string().c_str(), right.type, right.get_string().c_str());
            return SchemaFactory::get_instance()->get_histogram_ratio(table_id, field_id, left, right);
        }
        case LIKE_PREFIX: {
            ExprValue left = range.eq_in_values[0];
            ExprValue right = range.eq_in_values[0];
            // 计算机里的值都是离散的，右闭区间相当于末尾++后的右开区间，例如[abc, abc]等价于[abc,abd)
            // TODO后续把末尾加FF的都改成这种方式
            right.str_val.back()++;
            return SchemaFactory::get_instance()->get_histogram_ratio(table_id, field_id, left, right);
        }
        case EQ:
        case LIKE_EQ: 
        case IN: {
            double in_selectivity = 0.0;
            for (auto& value : range.eq_in_values) {
                in_selectivity += SchemaFactory::get_instance()->get_cmsketch_ratio(table_id, field_id, value);
            }
            return in_selectivity;
        }
        default:
            break;
    }
    return 1.0;
}

// TODO 后续做成index的统计信息，现在只是单列统计聚合
void AccessPath::calc_cost() {
    if (cost > 0.0) {
        return;
    }
    double selectivity = 1.0;
    //没有统计信息，固定给个值
    if (index_type == pb::I_FULLTEXT) {
        selectivity = 0.1;
    } else {
        for (auto& field_id : hit_index_field_ids) {
            auto iter = field_range_map.find(field_id);
            if (iter == field_range_map.end()) {
                continue;
            }
            double field_sel = calc_field_selectivity(field_id, iter->second);
            DB_DEBUG("field_id:%d selectivity:%f", field_id, field_sel);
            if (field_sel > 1.0) {
                continue;
            }
            selectivity *= field_sel;
        }
    }
    index_read_rows = selectivity * TOTAL_ROWS;
    // TODO index_other_condition过滤
    if (!is_covering_index && index_type != pb::I_PRIMARY) {
        table_read_rows = selectivity * TOTAL_ROWS;
    }
    cost = index_read_rows * INDEX_SEEK_FACTOR + table_read_rows * TABLE_GET_FACTOR;
}

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
