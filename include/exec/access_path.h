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

#pragma once

#include "exec_node.h"
#include "schema_factory.h"
#include "property.h"
#include "range.h"

namespace baikaldb {
struct AccessPath {
public:
enum IndexHint {
    NO_HINT = 0,
    USE_INDEX,
    FORCE_INDEX,
    IGNORE_INDEX
};

    AccessPath() {
    }
    virtual ~AccessPath() {
    }

    template<typename T>
    bool all_in_index(T& field_ids, std::unordered_set<int32_t>& all_fields) {
        for (auto field_id : field_ids) {
            if (all_fields.count(field_id) == 0) {
                return false;
            }
        }
        return true;
    }

    void calc_index_range();

    void calc_index_match(Property& sort_property) {
        fetch_field_ids();
        switch (index_type) {
            case pb::I_FULLTEXT:
                calc_fulltext();
                break;
            default:
                calc_normal(sort_property);
                break;
        }
    }

    bool need_add_to_learner_paths();
    bool need_select_learner_index();
    
    void calc_row_expr_range(std::vector<int32_t>& range_fields, ExprNode* expr, bool in_open,
            std::vector<ExprValue>& values, SmartRecord record, size_t field_idx);

    bool check_sort_use_index(Property& sort_property);

    void calc_normal(Property& sort_property);

    void calc_fulltext();
    
    void fetch_field_ids() {
        if (index_type == pb::I_KEY || index_type == pb::I_UNIQ || index_type == pb::I_PRIMARY) {
            for (auto& field : index_info_ptr->fields) {
                index_field_ids.insert(field.id);
            }
        }
        cover_field_ids.insert(index_field_ids.begin(), index_field_ids.end());
        for (auto& field : pri_info_ptr->fields) {
            cover_field_ids.insert(field.id);
        }
        if (index_type == pb::I_FULLTEXT) {
            cover_field_ids.insert(get_field_id_by_name(table_info_ptr->fields, "__weight"));
        }
    }
    
    double calc_field_selectivity(int32_t field_id, range::FieldRange& range);

    double fields_to_selectivity(const std::unordered_set<int32_t>& field_ids, std::map<int32_t, double>& filed_selectivity);
    // TODO 后续做成index的统计信息，现在只是单列统计聚合
    void calc_cost(std::map<std::string, std::string>* cost_info, std::map<int32_t, double>& filed_selectivity);
    void show_cost(std::map<std::string, std::string>* cost_info, std::map<int32_t, double>& filed_selectivity);

    void insert_no_cut_condition(const std::map<ExprNode*, std::unordered_set<int32_t>>& expr_field_map) {
        for (auto& pair : expr_field_map) {
            const auto& expr = pair.first;
            const auto& expr_field_ids = pair.second;
            if (need_cut_index_range_condition.count(expr) == 0) {
                // primary 没有过滤index_conjuncts，后续store会增加这个过滤
                if (all_in_index(expr_field_ids, cover_field_ids) && index_type != pb::I_PRIMARY) {
                    index_other_condition.insert(expr);
                    index_other_field_ids.insert(expr_field_ids.begin(), expr_field_ids.end());
                } else {
                    other_condition.insert(expr);
                    other_field_ids.insert(expr_field_ids.begin(), expr_field_ids.end());
                }
            }
        }
    }
    void calc_is_covering_index(const pb::TupleDescriptor& desc, std::set<int32_t>* slot_ids = nullptr) {
        for (auto& slot : desc.slots()) {
            if ((slot_ids != nullptr) && (slot_ids->count(slot.slot_id()) == 0)) {
                continue;
            }
            if (cover_field_ids.count(slot.field_id()) == 0) {
                // I_FULLTEXT; 获取不到索引的field信息
                // 但是select count(*) from full like '%a%';是能够索引覆盖的
                if (slot.ref_cnt() == 1 &&
                        !need_cut_index_range_condition.empty() &&
                        slot.field_id() == index_info_ptr->fields[0].id) {
                    continue;
                } else {
                    is_covering_index = false;
                    break;
                }
            }
        }
        pos_index.set_is_covering_index(is_covering_index);
    }

    bool is_cover_index() {
        return is_covering_index || index_type == pb::I_PRIMARY;
    }

    // 参考choose_index函数
    bool is_eq_or_in() {
        return _is_eq_or_in;
        if (pos_index.ranges_size() > 0) {
            const auto& range = pos_index.ranges(0);
            bool is_eq = true;
            if (range.has_left_key()) {
                if (range.left_key() != range.right_key()) {
                    is_eq = false;
                }
            } else {
                if (range.left_pb_record() != range.right_pb_record()) {
                    is_eq = false;
                }
            }

            if (range.left_field_cnt() != range.right_field_cnt()) {
                is_eq = false;
            }

            if (range.left_open() || range.right_open()) {
                is_eq = false;
            }

            return is_eq && !range.like_prefix();
        }

        return false;
    }
    
public:
    //TODO 先mock一个，后面从schema读取
    static const int64_t INDEX_SEEK_FACTOR = 1;
    static const int64_t TABLE_GET_FACTOR = 5;
    pb::IndexType index_type = pb::I_NONE;
    // 目前主要primary会用到IGNORE_INDEX
    IndexHint hint = NO_HINT;
    int32_t tuple_id = 0;
    int64_t table_id = -1;
    int64_t index_id = -1;
    // 全部field的field id
    std::unordered_set<int32_t> index_field_ids;
    std::unordered_set<int32_t> cover_field_ids;
    std::unordered_set<int32_t> hit_index_field_ids;
    std::unordered_set<int32_t> index_other_field_ids;
    std::unordered_set<int32_t> other_field_ids;
    // 不分配内存，只获取filter节点的指针
    // 这些set是互斥的
    std::unordered_set<ExprNode*> need_cut_index_range_condition;
    std::unordered_set<ExprNode*> index_other_condition;
    std::unordered_set<ExprNode*> other_condition;
    std::map<int32_t, range::FieldRange> field_range_map;
    int64_t index_read_rows = 0;
    int64_t table_get_rows = 0;
    double cost = 0.0;
    double selectivity = 0.0;
    pb::PossibleIndex pos_index;
    int  eq_count = 0;
    bool is_covering_index = true;
    bool is_possible = false;
    bool is_sort_index = false;
    bool is_virtual = false;
    int index_other_condition_count = 0;
    SmartTable table_info_ptr;
    SmartIndex index_info_ptr;
    SmartIndex pri_info_ptr;
    int _left_field_cnt = 0;
    int _right_field_cnt = 0;
    bool _left_open = false;
    bool _right_open = false;
    bool _like_prefix = false;
    bool _in_pred = false;
    bool _is_eq_or_in = true;
};
typedef std::shared_ptr<AccessPath> SmartPath;
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
