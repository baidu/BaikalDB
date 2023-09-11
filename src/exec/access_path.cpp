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
#ifdef BAIDU_INTERNAL 
#include <base/containers/flat_map.h>
#else
#include <butil/containers/flat_map.h>
#endif

namespace baikaldb {
using namespace range;
DEFINE_uint64(max_in_records_num, 10000, "max_in_records_num");
DEFINE_int64(index_use_for_learner_delay_s, 3600, "1h");
DEFINE_bool(date_range_to_in, false, "date range to in");

bool AccessPath::need_add_to_learner_paths() {
    int64_t _1h = FLAGS_index_use_for_learner_delay_s * 1000 * 1000LL;
    int64_t _2h = 2 * _1h;
    if (table_info_ptr->learner_resource_tags.size() > 0) {
        if (index_info_ptr->index_hint_status == pb::IHS_NORMAL) {
            if (butil::gettimeofday_us() - index_info_ptr->disable_time < _2h && butil::gettimeofday_us() - index_info_ptr->restore_time < _1h) {
                // 关闭以后马上打开，可以使用
                return true;
            } else if (butil::gettimeofday_us() - index_info_ptr->restore_time > _1h) {
                // 正常打开超过1h可以使用
                return true;
            }
            return false;
        } else if (index_info_ptr->index_hint_status == pb::IHS_DISABLE) {
            if (butil::gettimeofday_us() - index_info_ptr->restore_time < _2h && butil::gettimeofday_us() - index_info_ptr->disable_time < _1h) {
                // 打开以后马上关闭，不可以使用
                return false;
            } else if (butil::gettimeofday_us() - index_info_ptr->disable_time > _1h) {
                // 关闭超过1h不可以使用
                return false;
            }
            return true;
        }
    }

    return false;
}

bool AccessPath::need_select_learner_index() {
    if (butil::gettimeofday_us() - index_info_ptr->restore_time < FLAGS_index_use_for_learner_delay_s * 1000 * 1000LL
            && table_info_ptr->learner_resource_tags.size() > 0) {
        return true;
    }

    return false;
}

void AccessPath::calc_row_expr_range(std::vector<int32_t>& range_fields, ExprNode* expr, bool in_open,
        std::vector<ExprValue>& values, MutTableKey& key, size_t field_idx) {
    if (expr == nullptr) {
        return;
    }
    if (range_fields.empty()) {
        return;
    }
    size_t row_idx = 0;
    for (; row_idx < range_fields.size() && 
            field_idx < index_info_ptr->fields.size(); row_idx++, field_idx++) {
        if (index_info_ptr->fields[field_idx].id == range_fields[row_idx]) {
            key.append_value(values[row_idx].cast_to(index_info_ptr->fields[field_idx].type));
            hit_index_field_ids.insert(range_fields[row_idx]);
        } else {
            break;
        }
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
    if (idx < fields.size() && (int)idx <= eq_count) {
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

struct RecordRange {
    MutTableKey left_key;
    MutTableKey right_key;
    int64_t partition_id = -1;
};
// 现在只支持CNF，DNF怎么做?
// 普通索引按照range匹配，匹配到EQ可以往下走，匹配到RANGE、LIKE_PREFIX停止
// 匹配到IN，如果之前是IN停止（row_expr除外）
// 条件裁剪，对于普通条件，除了LIKE_PREFIX都裁剪
// 对于row_expr，满足全命中索引range后裁剪
void AccessPath::calc_normal(Property& sort_property) {
    int field_cnt = 0;
    // offset: in条件组合展开后的步长,用于非首字段的对应
    // hit_fields_cnt: in_row_expr谓词匹配的字段个数,用于判断是否可以剪切
    std::map< ExprNode*, std::pair<uint32_t, uint32_t>> in_row_expr_map; // <in_row_expr,<offset, hit_fields_cnt>
    size_t in_records_size = 1;
    for (auto& field : index_info_ptr->fields) {
        bool field_break = false;
        auto iter = field_range_map->find(field.id);
        if (iter == field_range_map->end()) {
            break;
        }
        FieldRange& range = iter->second;
        if (FLAGS_date_range_to_in 
                && range.type == RANGE 
                && field.type == pb::PrimitiveType::DATE 
                && !range.is_row_expr
                && range.left.size() == 1 && range.right.size() == 1) {
            get_date_in_values(range.left[0], range.left_open, range.right[0], range.right_open, range.eq_in_values);
            range.type = IN;
            // will fall throught to case IN to handle
            // 可能会导致seek压力大
        } 
        switch (range.type) {
            case RANGE: {
                field_break = true;
                ++field_cnt;
                _is_eq_or_in = false;
                hit_index_field_ids.insert(field.id);
                size_t field_idx = field_cnt - 1;
                if (range.left.size() == 1) {
                    _left_open = range.left_open;
                    _left_field_cnt = field_cnt;
                    need_cut_index_range_condition.insert(range.left_expr);
                } else if (range.left.size() > 1) {
                    size_t row_idx = 0;
                    while (row_idx < range.left_row_field_ids.size() && field_idx < index_info_ptr->fields.size()) {
                        if (index_info_ptr->fields[field_idx].id == range.left_row_field_ids[row_idx]) {
                            hit_index_field_ids.insert(range.left_row_field_ids[row_idx]);
                        } else {
                            break;
                        }
                        row_idx++;
                        field_idx++;
                    }
                    _left_field_cnt = field_idx;
                    if (row_idx == range.left_row_field_ids.size()) {
                        _left_open = range.left_open;
                        need_cut_index_range_condition.insert(range.left_expr);
                    } else {
                        _left_open = false;
                    }
                }
                if (range.right.size() == 1) {
                    _right_open = range.right_open;
                    _right_field_cnt = field_cnt;
                    need_cut_index_range_condition.insert(range.right_expr);
                } else if (range.right.size() > 1) {
                    size_t row_idx = 0;
                    while (row_idx < range.right_row_field_ids.size() && field_idx < index_info_ptr->fields.size()) {
                        if (index_info_ptr->fields[field_idx].id == range.right_row_field_ids[row_idx]) {
                            hit_index_field_ids.insert(range.right_row_field_ids[row_idx]);
                        } else {
                            break;
                        }
                        row_idx++;
                        field_idx++;
                    }
                    _right_field_cnt = field_idx;
                    if (row_idx == range.right_row_field_ids.size()) {
                        _right_open = range.right_open;
                        need_cut_index_range_condition.insert(range.right_expr);
                    } else {
                        _right_open = false;
                    }
                }
                break;
            }
            case EQ:
            case LIKE_EQ:
                ++eq_count;
            case LIKE_PREFIX:
                // 数字 like不能命中索引
                if (range.type == LIKE_PREFIX && field.type != pb::STRING) {
                    field_break = true;
                    break;
                }
                ++field_cnt;
                hit_index_field_ids.insert(field.id);
                _left_field_cnt = field_cnt;
                _right_field_cnt = field_cnt;
                _left_open = false;
                _right_open = false;
                if (range.type == LIKE_PREFIX) {
                    _like_prefix = true;
                    field_break = true;
                    _is_eq_or_in = false;
                } else if (range.is_row_expr) {
                    if (all_in_index(range.left_row_field_ids, hit_index_field_ids)) {
                        need_cut_index_range_condition.insert(range.conditions.begin(), range.conditions.end());
                    }
                } else {
                    need_cut_index_range_condition.insert(range.conditions.begin(), range.conditions.end());
                }
                break;
            case LIKE:
                field_break = true;
                break;
            case IN:
                ++eq_count;
                ++field_cnt;
                hit_index_field_ids.insert(field.id);
                _left_field_cnt = field_cnt;
                _right_field_cnt = field_cnt;
                _left_open = false;
                _right_open = false;
                _in_pred = true;
                if (range.is_row_expr && in_row_expr_map.count(*range.conditions.begin()) == 1) {
                    // in_row_expr的非首个字段不组合展开,按offset填充,例如(a, b) in ((1,2))的b
                    // flat fill other row_expr field value exclude the first.
                    if (range.eq_in_values.size() > 0 && in_records_size % range.eq_in_values.size() == 0) {
                        // do nothing
                    } else {
                        DB_FATAL("inx:%ld in_records.size() %lu != values.size()'s multiples %lu ",
                                index_id, in_records_size, range.eq_in_values.size());
                        field_break = true;
                        break;
                    }
                    in_row_expr_map[*range.conditions.begin()].second++;
                } else {
                    // 第一个in不限制FLAGS_max_in_records_num
                    if (in_records_size > 1 && in_records_size * range.eq_in_values.size() > FLAGS_max_in_records_num) {
                        field_break = true;
                        break;
                    }
                    if (range.is_row_expr) {
                        // in_row_expr的首个字段进行组合展开,并记录展开offset,用于后续字段映射
                        // 例如: a in ("a1", "a2") and (b, c) in (("b1","c1")), 则b与a组合展开后,如下
                        // (("a1", "b1") ("a2", "b1")),则b的offset = in_records.size();
                        in_row_expr_map[*range.conditions.begin()] = std::pair<uint32_t, uint32_t>(in_records_size, 1);
                    }
                    in_records_size = range.eq_in_values.size() * in_records_size;
                }
                if (range.is_row_expr) {
                    if (all_in_index(range.left_row_field_ids, hit_index_field_ids) &&
                            in_row_expr_map[*range.conditions.begin()].second == range.left_row_field_ids.size()) {
                        // (a,b) IN (("a1", "b1")) and (b,c) IN (("b1","c2")) hit_index_field_ids=(a,b,c),
                        // (a,b)与(b,c)都包含于hit_index_field_ids中,需要进一步判断字段b是那个pred命中的.
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
    if (hit_index_field_ids.size() < field_range_map->size()) {
        //除cut contition外有过滤条件
        _need_filter = true;
    }
    is_sort_index = check_sort_use_index(sort_property);
    DB_DEBUG("is_sort_index:%d, eq_count:%d, sort_property:%lu", 
        is_sort_index, eq_count, sort_property.slot_order_exprs.size());
    pos_index.set_index_id(index_id);
    if (is_sort_index) {
        is_possible = true;
        auto sort_index = pos_index.mutable_sort_index();
        sort_index->set_is_asc(sort_property.is_asc[0]);
        sort_index->set_sort_limit(sort_property.expected_cnt);
    }
    if (_left_field_cnt != 0 || _right_field_cnt != 0) {
        is_possible = true;
    }
    index_other_condition_count = field_range_map->size() - hit_index_field_ids.size();
}

void AccessPath::get_date_in_values(const ExprValue& left, bool left_open, const ExprValue& right, bool right_open, std::vector<ExprValue>& dates) {
    ExprValue begin = left;
    ExprValue end = right;
    begin.cast_to(pb::TIMESTAMP);
    end.cast_to(pb::TIMESTAMP);
    if (left_open) {
        begin._u.uint32_val += 24 * 3600;
    }
    if (right_open) {
        end._u.uint32_val -= 24 * 3600;
    }
    while (begin.compare(end) <= 0) {
        ExprValue tmp = begin;
        tmp.cast_to(pb::DATE);
        dates.emplace_back(tmp);
        begin._u.uint32_val += 24 * 3600;
    }
    return;
}

// 填充索引的range
void AccessPath::calc_index_range(int64_t partition_field_id, const std::map<std::string, int64_t>& expr_partition_map) {
    if (index_type == pb::I_FULLTEXT || index_type == pb::I_VECTOR) {
        return;
    }
    MutTableKey left_key;
    MutTableKey right_key;
    int64_t partition_id = -1;
    if (index_type == pb::I_KEY || index_type == pb::I_UNIQ) {
        uint8_t null_flag = 0;
        left_key.append_u8(null_flag);
        right_key.append_u8(null_flag);
    }
    std::vector<RecordRange> in_records;
    // offset: in条件组合展开后的步长,用于非首字段的对应
    // hit_fields_cnt: in_row_expr谓词匹配的字段个数,用于判断是否可以剪切
    std::map< ExprNode*, std::pair<uint32_t, uint32_t>> in_row_expr_map; // <in_row_expr,<offset, hit_fields_cnt>
    int field_cnt = 0;
    for (auto& field : index_info_ptr->fields) {
        auto iter = field_range_map->find(field.id);
        field_cnt ++;
        if (field_cnt > hit_index_field_ids.size()) {
            break;
        }
        if (iter == field_range_map->end()) {
            break;
        }
        FieldRange& range = iter->second;
        switch (range.type) {
            case RANGE: {
                auto range_func = [&range, this, field_cnt, field](
                        MutTableKey& left_key, MutTableKey& right_key) {
                    size_t field_idx = field_cnt - 1;
                    if (range.left.size() == 1) {
                        // join on 条件两表字段类型可能不同，导致slotref的类型不准，需要处理
                        left_key.append_value(range.left[0].cast_to(field.type));
                    } else if (range.left.size() > 1) {
                        calc_row_expr_range(range.left_row_field_ids, range.left_expr, 
                            range.left_open, range.left, left_key, field_idx);
                    }
                    if (range.right.size() == 1) {
                        right_key.append_value(range.right[0].cast_to(field.type));
                    } else if (range.right.size() > 1) {
                        calc_row_expr_range(range.right_row_field_ids, range.right_expr, 
                            range.right_open, range.right, right_key, field_idx);
                    }
                };
                if (in_records.size() > 0) {
                    for (auto& rg : in_records) {
                        rg.right_key = rg.left_key;
                        range_func(rg.left_key, rg.right_key);
                    }
                } else {
                    range_func(left_key, right_key);
                }
                break;
            }
            case EQ:
            case LIKE_EQ:
            case LIKE_PREFIX:
                if (_like_prefix && range.type == LIKE_PREFIX) {
                    if (in_records.empty()) {
                        left_key.append_string_prefix(range.eq_in_values[0].get_string());
                        right_key.append_string_prefix(range.eq_in_values[0].get_string());
                    } else {
                        for (auto& rg : in_records) {
                            rg.left_key.append_string_prefix(range.eq_in_values[0].get_string());
                        }
                    }
                } else {
                    if (in_records.empty()) {
                        if (range.type == EQ && field.id == partition_field_id) {
                            auto iter = expr_partition_map.find(range.eq_in_values[0].get_string());
                            if (iter != expr_partition_map.end()) {
                                partition_id = iter->second;
                            }
                        }
                        left_key.append_value(range.eq_in_values[0].cast_to(field.type));
                        right_key.append_value(range.eq_in_values[0].cast_to(field.type));
                    } else {
                        for (auto& rg : in_records) {
                            if (range.type == EQ && field.id == partition_field_id) {
                                auto iter = expr_partition_map.find(range.eq_in_values[0].get_string());
                                if (iter != expr_partition_map.end()) {
                                    rg.partition_id = iter->second;
                                }
                            }
                            rg.left_key.append_value(range.eq_in_values[0].cast_to(field.type));
                        }
                    }
                }
                break;
            case IN:
                if (range.is_row_expr && in_row_expr_map.count(*range.conditions.begin()) == 1) {
                    // in_row_expr的非首个字段不组合展开,按offset填充,例如(a, b) in ((1,2))的b
                    uint32_t offset = in_row_expr_map[*range.conditions.begin()].first;
                    // flat fill other row_expr field value exclude the first.
                    if (range.eq_in_values.size() > 0 && in_records.size() % range.eq_in_values.size() == 0) {
                        size_t vs = range.eq_in_values.size();
                        size_t vi = 0;
                        size_t i = 0;
                        for (auto& rg : in_records) {
                            if (field.id == partition_field_id) {
                                auto iter = expr_partition_map.find(range.eq_in_values[vi].get_string());
                                if (iter != expr_partition_map.end()) {
                                    rg.partition_id = iter->second;
                                }
                            }
                            rg.left_key.append_value(range.eq_in_values[vi].cast_to(field.type));
                            if ((++i) == offset) {
                                i = 0;
                                vi = ((vi + 1) % vs);
                            }
                        }
                    } else {
                        DB_FATAL("inx:%ld in_records.size() %lu != values.size()'s multiples %lu ",
                                index_id, in_records.size(), range.eq_in_values.size());
                        return;
                    }
                    in_row_expr_map[*range.conditions.begin()].second++;
                } else {
                    // 第一次in_records size为0,不受限制
                    if (in_records.empty()) {
                        RecordRange rg;
                        rg.left_key = left_key;
                        in_records.emplace_back(rg);
                    }
                    if (range.is_row_expr) {
                        // in_row_expr的首个字段进行组合展开,并记录展开offset,用于后续字段映射
                        // 例如: a in ("a1", "a2") and (b, c) in (("b1","c1")), 则b与a组合展开后,如下
                        // (("a1", "b1") ("a2", "b1")),则b的offset = in_records.size();
                        in_row_expr_map[*range.conditions.begin()] = std::pair<uint32_t, uint32_t>(in_records.size(), 1);
                    }
                    std::vector<RecordRange> comb_in_records;
                    comb_in_records.reserve(range.eq_in_values.size() * in_records.size());
                    for (auto value : range.eq_in_values) {
                        // 为保持前面已处理字段步长稳定性, 当前字段需要写在外层循环与in_records进行展开.
                        for (auto record : in_records) {
                            RecordRange rg;
                            if (field.id == partition_field_id) {
                                auto iter = expr_partition_map.find(value.get_string());
                                if (iter != expr_partition_map.end()) {
                                    rg.partition_id = iter->second;
                                }
                            }
                            rg.left_key = record.left_key;
                            rg.left_key.append_value(value.cast_to(field.type));
                            comb_in_records.emplace_back(rg);
                        }
                    }
                    in_records.swap(comb_in_records);
                    comb_in_records.clear();
                }
                /*
                if (range.is_row_expr) {
                    in_row_expr = *range.conditions.begin();
                    if (all_in_index(range.left_row_field_ids, hit_index_field_ids) &&
                            in_row_expr_map[*range.conditions.begin()].second == range.left_row_field_ids.size()) {
                        // (a,b) IN (("a1", "b1")) and (b,c) IN (("b1","c2")) hit_index_field_ids=(a,b,c),
                        // (a,b)与(b,c)都包含于hit_index_field_ids中,需要进一步判断字段b是那个pred命中的.
                        need_cut_index_range_condition.insert(range.conditions.begin(), range.conditions.end());
                    }
                } else {
                    need_cut_index_range_condition.insert(range.conditions.begin(), range.conditions.end());
                }
                */
                break;
            default:
                break;
        }
    }
    pos_index.set_is_eq(_is_eq_or_in);
    if (_left_field_cnt == 0 && _right_field_cnt == 0) {
        pos_index.clear_is_eq();//无命中条件非eq
        pos_index.add_ranges();
    } else if (in_records.size() > 0) {
        is_possible = true;
        butil::FlatSet<std::string> filter;
        filter.init(ajust_flat_size(in_records.size()));
        for (auto& rg : in_records) {
            if (filter.seek(rg.left_key.data()) != nullptr) {
                continue;
            }
            filter.insert(rg.left_key.data());
            auto range = pos_index.add_ranges();
            if (_left_field_cnt == index_info_ptr->fields.size()
                && (index_type == pb::I_PRIMARY || index_type == pb::I_UNIQ)
                && !_like_prefix) {
                rg.left_key.set_full(true);
            }
            if (rg.partition_id != -1) {
                range->set_partition_id(rg.partition_id);
            }
            range->set_left_key(rg.left_key.data());
            range->set_left_full(rg.left_key.get_full());
            if (!_is_eq_or_in) {
                if (_right_field_cnt == index_info_ptr->fields.size()
                    && (index_type == pb::I_PRIMARY || index_type == pb::I_UNIQ)
                    && !_like_prefix) {
                    rg.right_key.set_full(true);
                }
                range->set_right_key(rg.right_key.data());
                range->set_right_full(rg.right_key.get_full());
            } else {
                // eq通过标记判断，后续可以删掉
                range->set_right_key(rg.left_key.data());
                range->set_right_full(rg.left_key.get_full());
            }
            range->set_left_field_cnt(_left_field_cnt);
            range->set_right_field_cnt(_right_field_cnt);
            range->set_left_open(_left_open);
            range->set_right_open(_right_open);
            range->set_like_prefix(_like_prefix);
        }
    } else {
        is_possible = true;
        auto range = pos_index.add_ranges();
        if (_left_field_cnt == index_info_ptr->fields.size()
            && (index_type == pb::I_PRIMARY || index_type == pb::I_UNIQ)
            && !_like_prefix) {
            left_key.set_full(true);
        }
        if (_right_field_cnt == index_info_ptr->fields.size()
            && (index_type == pb::I_PRIMARY || index_type == pb::I_UNIQ)
            && !_like_prefix) {
            right_key.set_full(true);
        }
        if (partition_id != -1) {
            range->set_partition_id(partition_id);
        }
        range->set_left_key(left_key.data());
        range->set_left_full(left_key.get_full());
        range->set_right_key(right_key.data());
        range->set_right_full(right_key.get_full());
        range->set_left_field_cnt(_left_field_cnt);
        range->set_right_field_cnt(_right_field_cnt);
        range->set_left_open(_left_open);
        range->set_right_open(_right_open);
        range->set_like_prefix(_like_prefix);
    }
}

void AccessPath::calc_fulltext(Property& sort_property) {
    int32_t field_id = index_info_ptr->fields[0].id;
    auto iter = field_range_map->find(field_id);
    if (iter == field_range_map->end()) {
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
        case MATCH_VECTOR:
            hit_index = true;
            need_cut_index_range_condition.insert(range.conditions.begin(), range.conditions.end());
            values = &range.like_values;
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
        hit_index_field_ids.emplace(field_id);
        is_possible = true;
        pos_index.set_index_id(index_id);
        butil::FlatSet<std::string> filter;
        filter.init(ajust_flat_size(values->size()));
        for (auto value : *values) {
            std::string str = value.get_string();
            if (filter.seek(str) != nullptr) {
                continue;
            }
            filter.insert(str);
            auto range = pos_index.add_ranges();
            range->set_left_key(str);
            range->set_left_field_cnt(1);
            range->set_left_open(false);
            if (range_type == MATCH_LANGUAGE) {
                range->set_match_mode(pb::M_NARUTAL_LANGUAGE);
            } else if (range_type == MATCH_BOOLEAN) {
                range->set_match_mode(pb::M_BOOLEAN);
            }
            range->set_topk(sort_property.expected_cnt);
        }
    } else {
        pos_index.set_index_id(index_id);
        pos_index.add_ranges();
    }
    if (hit_index_field_ids.size() < field_range_map->size()) {
        //除cut contition外有过滤条件
        _need_filter = true;
    }
    index_other_condition_count = field_range_map->size() - hit_index_field_ids.size();
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

double AccessPath::fields_to_selectivity(const std::unordered_set<int32_t>& field_ids, std::map<int32_t, double>& filed_selectivity) {
    double selectivity = 1.0;
    for (auto& field_id : field_ids) {
        auto sel_iter = filed_selectivity.find(field_id);
        if (sel_iter != filed_selectivity.end()) {
            //从map中找到直接使用
            if (sel_iter->second > 1.0 || sel_iter->second < 0.0) {
                continue;
            }
            selectivity *= sel_iter->second;
            continue;
        }
        auto iter = field_range_map->find(field_id);
        if (iter == field_range_map->end()) {
            continue;
        }
        double field_sel = calc_field_selectivity(field_id, iter->second);
        DB_DEBUG("field_id:%d selectivity:%f", field_id, field_sel);
        filed_selectivity[field_id] = field_sel;
        // selectivity < 0 代表超过统计信息范围
        // TODO 针对递增/时间列按1.0计算有意义，后续是否按表配置区分
        if (field_sel > 1.0 || field_sel < 0.0) {
            continue;
        }
        selectivity *= field_sel;
    }
    return selectivity;
}

// TODO 后续做成index的统计信息，现在只是单列统计聚合
void AccessPath::calc_cost(std::map<std::string, std::string>* cost_info, std::map<int32_t, double>& filed_selectivity) {
    if (cost > 0.0 && cost_info == nullptr) {
        return;
    }
    int64_t table_rows = SchemaFactory::get_instance()->get_total_rows(table_id);
    selectivity = 1.0;
    //没有统计信息，固定给个值
    if (index_type == pb::I_FULLTEXT) {
        selectivity = 0.1;
    } else {
        selectivity = fields_to_selectivity(hit_index_field_ids, filed_selectivity);
    }
    index_read_rows = selectivity * table_rows;
    double index_other_condition_selectivity = fields_to_selectivity(index_other_field_ids, filed_selectivity);
    double other_condition_selectivity = fields_to_selectivity(other_field_ids, filed_selectivity);
    if (cost_info != nullptr) {
        std::ostringstream os;
        for (auto field_id : hit_index_field_ids) {
            os << field_id << ":" << filed_selectivity[field_id] << ";";
        }
        (*cost_info)["hit_index_fields"] = os.str();
        os.clear();

        for (auto field_id : index_other_field_ids) {
            os << field_id << ":" << filed_selectivity[field_id] << ";";
        }
        (*cost_info)["index_other_fields"] = os.str();
        os.clear();

        for (auto field_id : other_field_ids) {
            os << field_id << ":" << filed_selectivity[field_id] << ";";
        }
        (*cost_info)["other_fields"] = os.str();
    }

    if (is_sort_index && index_other_condition_selectivity > 0 and other_condition_selectivity > 0) {
        int64_t expected_cnt = pos_index.sort_index().sort_limit();
        expected_cnt = expected_cnt / index_other_condition_selectivity / other_condition_selectivity;
        if (expected_cnt >= 0 && expected_cnt < index_read_rows) {
            index_read_rows = expected_cnt;
        }
    }
    if (!is_covering_index && index_type != pb::I_PRIMARY) {
        table_get_rows = index_read_rows * index_other_condition_selectivity;
    }
    cost = index_read_rows * INDEX_SEEK_FACTOR + table_get_rows * TABLE_GET_FACTOR;
    DB_DEBUG("table_rows:%ld index_read_rows:%ld, selectivity:%f index_other_condition_selectivity:%f other_condition_selectivity:%f cost:%f", 
            table_rows,index_read_rows,selectivity,index_other_condition_selectivity,other_condition_selectivity,cost);
    if (cost_info != nullptr) {
        (*cost_info)["cost"] = std::to_string(cost);
        (*cost_info)["selectivity"] = std::to_string(selectivity);
        (*cost_info)["index_other_sel"] = std::to_string(index_other_condition_selectivity);
        (*cost_info)["other_sel"] = std::to_string(other_condition_selectivity);
        (*cost_info)["index_read_rows"] = std::to_string(index_read_rows);
        (*cost_info)["table_rows"] = std::to_string(table_rows);
        (*cost_info)["is_sort"] = std::to_string(is_sort_index);
        (*cost_info)["is_possible"] = std::to_string(is_possible);
        (*cost_info)["is_cover"] = std::to_string(is_covering_index);
        (*cost_info)["index_name"] = index_info_ptr->short_name;
    }
}

void AccessPath::show_cost(std::map<std::string, std::string>* cost_info, std::map<int32_t, double>& filed_selectivity) {
    calc_cost(cost_info, filed_selectivity);
}

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
