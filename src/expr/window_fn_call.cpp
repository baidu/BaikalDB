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

#include "window_fn_call.h"

namespace baikaldb {

int WindowFnCall::init(const pb::ExprNode& node) {
    static std::unordered_map<std::string, WindowType> name_type_map = {
        {"count_star", COUNT_STAR},
        {"count", COUNT},
        {"sum", SUM},
        {"avg", AVG},
        {"min", MIN},
        {"max", MAX},
        {"row_number", ROW_NUMBER},
        {"rank", RANK},
        {"dense_rank", DENSE_RANK},
        {"percent_rank", PERCENT_RANK},
        {"cume_dist", CUME_DIST},
        {"ntile", NTILE},
        {"lead", LEAD},
        {"lag", LAG},
        {"first_value", FIRST_VALUE},
        {"last_value", LAST_VALUE},
        {"nth_value", NTH_VALUE}
    };
    int ret = 0;
    ret = ExprNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExprNode::init fail:%d", ret);
        return ret;
    }
    if (!node.has_fn()) {
        DB_WARNING("node has no fn");
        return -1;
    }
    _is_constant = false;
    _tuple_id = node.derive_node().tuple_id();
    _slot_id = node.derive_node().slot_id();
    if (name_type_map.find(node.fn().name()) == name_type_map.end()) {
        DB_WARNING("Invalid fn: %s", node.fn().name().c_str());
        return -1;
    }
    _window_type = name_type_map[node.fn().name()];
    // 初始化WindowIntermediate
    if (initialize() != 0) {
        DB_WARNING("initialize failed");
        return -1;
    }
    return 0;
}

int WindowFnCall::open() {
    int ret = 0;
    ret = ExprNode::open();
    if (ret < 0) {
        DB_WARNING("ExprNode::open fail:%d", ret);
        return ret;
    }
    switch (_window_type) {
    case COUNT:
    case SUM:
    case AVG:
    case MIN:
    case MAX: {
        if (_children.size() == 0) {
            DB_WARNING("_window_type:%d , _children.size() == 0", _window_type);
            return -1;
        }
        if (_children[0] == nullptr) {
            DB_WARNING("_children[0] is nullptr");
            return -1;
        }
        break;
    }
    case RANK:
    case DENSE_RANK:
    case PERCENT_RANK: 
    case CUME_DIST: {
        WindowRankIntermediate* window_intermediate = (WindowRankIntermediate*)_window_intermediate.c_str();
        window_intermediate->mem_row_compare = _mem_row_compare;
        break;
    }
    case NTILE: {
        if (_children.size() == 0) {
            DB_WARNING("_window_type:%d , _children.size() == 0", _window_type);
            return -1;
        }
        if (_children[0] == nullptr) {
            DB_WARNING("_children[0] is nullptr");
            return -1;
        }
        if (_children[0]->node_type() != pb::INT_LITERAL) {
            DB_WARNING("_window_type: %d, invalid children node_type: %d", 
                        _window_type, _children[0]->node_type());
            return -1;
        }
        WindowNtileIntermediate* window_intermediate = (WindowNtileIntermediate*)_window_intermediate.c_str();
        window_intermediate->n = _children[0]->get_value(nullptr).cast_to(pb::INT64).get_numberic<int64_t>();
        if (window_intermediate->n <= 0) {
            DB_WARNING("Invalid n: %ld", window_intermediate->n);
            return -1;
        }
        break;
    }
    case LEAD:
    case LAG: {
        if (_children.size() == 0) {
            DB_WARNING("_window_type:%d , _children.size() == 0", _window_type);
            return -1;
        }
        if (_children[0] == nullptr) {
            DB_WARNING("_children[0] is nullptr");
            return -1;
        }
        if (_children.size() > 1) {
            if (_children[1] == nullptr) {
                DB_WARNING("_children[1] is nullptr");
                return -1;
            }
            if (_children[1]->node_type() != pb::INT_LITERAL) {
                DB_WARNING("_window_type: %d, invalid children node_type: %d", 
                            _window_type, _children[1]->node_type());
                return -1;
            }
            WindowLeadLagIntermediate* window_intermediate = (WindowLeadLagIntermediate*)_window_intermediate.c_str();
            window_intermediate->offset = _children[1]->get_value(nullptr).cast_to(pb::INT64).get_numberic<int64_t>();
            if (window_intermediate->offset < 0) {
                DB_WARNING("Invalid offset: %ld", window_intermediate->offset);
                return -1;
            }
            if (_children.size() > 2) {
                if (_children[2] == nullptr) {
                    DB_WARNING("_children[2] is nullptr");
                    return -1;
                }
                window_intermediate->default_expr = _children[2];
            }
        }
        break;
    }
    case FIRST_VALUE:
    case LAST_VALUE: {
        if (_children.size() == 0) {
            DB_WARNING("_window_type:%d , _children.size() == 0", _window_type);
            return -1;
        }
        if (_children[0] == nullptr) {
            DB_WARNING("_children[0] is nullptr");
            return -1;
        }
        break;
    }
    case NTH_VALUE: {
        if (_children.size() < 2) {
            DB_WARNING("_window_type: %d, invalid children size: %lu", _window_type, _children.size());
            return -1;
        }
        if (_children[0] == nullptr) {
            DB_WARNING("_children[0] is nullptr");
            return -1;
        }
        if (_children[1] == nullptr) {
            DB_WARNING("_children[1] is nullptr");
            return -1;
        }
        if (_children[1]->node_type() != pb::INT_LITERAL) {
            DB_WARNING("_window_type: %d, invalid children node_type: %d", 
                        _window_type, _children[1]->node_type());
            return -1;
        }
        WindowValueIntermediate* window_intermediate = (WindowValueIntermediate*)_window_intermediate.c_str();
        window_intermediate->n = _children[1]->get_value(nullptr).cast_to(pb::INT64).get_numberic<int64_t>();
        if (window_intermediate->n <= 0) {
            DB_WARNING("Invalid n: %ld", window_intermediate->n);
            return -1;
        }
        break;
    }
    default: {
        break;
    }
    }
    return 0;
}

void WindowFnCall::close() {
    ExprNode::close();
    _tuple_id = -1;
    _slot_id = -1;
    _window_intermediate.clear();
    _mem_row_compare = nullptr;
}

int WindowFnCall::type_inferer() {
    int ret = 0;
    ret = ExprNode::type_inferer();
    if (ret < 0) {
        DB_FATAL("ExprNode::type_inferer error");
        return ret;
    }
    switch (_window_type) {
    case COUNT_STAR:
    case COUNT: {
        _col_type = pb::INT64;
        break;
    }
    case SUM: {
        if (_children.size() == 0) {
            DB_WARNING("has no child");
            return -1;
        }
        if (is_double(_children[0]->col_type()) ||
                is_string(_children[0]->col_type())) {
            _col_type = pb::DOUBLE;
        } else {
            _col_type = pb::INT64;
        }
        break;
    }
    case AVG: {
        _col_type = pb::DOUBLE;
        break;
    }
    case MIN: 
    case MAX: {
        if (_children.size() == 0) {
            DB_WARNING("has no child");
            return -1;
        }
        _col_type = _children[0]->col_type();
        break;
    }
    case ROW_NUMBER:
    case RANK: 
    case DENSE_RANK: {
        _col_type = pb::INT64;
        break;
    }
    case PERCENT_RANK: 
    case CUME_DIST: {
        _col_type = pb::DOUBLE;
        break;
    }
    case NTILE: {
        _col_type = pb::INT64;
        break;
    }
    case LEAD:
    case LAG: {
        if (_children.size() == 0) {
            DB_WARNING("has no child");
            return -1;
        }
        _col_type = _children[0]->col_type();
        break;
    }
    case FIRST_VALUE:
    case LAST_VALUE:
    case NTH_VALUE: {
        if (_children.size() == 0) {
            DB_WARNING("has no child");
            return -1;
        }
        _col_type = _children[0]->col_type();
        break;
    }
    default: {
        DB_WARNING("Invalid window type : %d", _window_type);
        return -1;
    }
    }
    return 0;
}

int WindowFnCall::type_inferer(QueryContext* ctx) {
    int ret = type_inferer();
    if (ret < 0) {
        return ret;
    }
    if (ctx == nullptr) {
        DB_WARNING("ctx is nullptr");
        return -1;
    }
    auto tuple_desc = ctx->get_tuple_desc(_tuple_id);
    if (tuple_desc == nullptr) {
        DB_WARNING("tuple_desc is nullptr, tuple_id: %d", _tuple_id);
        return -1;
    }
    for (auto& slot : *tuple_desc->mutable_slots()) {
        if (slot.slot_id() == _slot_id) {
            slot.set_slot_type(_col_type);
            break;
        }
    }
    return 0;
}

int WindowFnCall::initialize() {
    switch (_window_type) {
    case COUNT_STAR:
    case COUNT: {
        WindowCountIntermediate window_intermediate;
        _window_intermediate.assign((char*)&window_intermediate, sizeof(WindowCountIntermediate));
        break;
    }
    case SUM: {
        WindowSumIntermediate window_intermediate;
        _window_intermediate.assign((char*)&window_intermediate, sizeof(WindowSumIntermediate));
        break;
    }
    case AVG: {
        WindowAvgIntermediate window_intermediate;
        _window_intermediate.assign((char*)&window_intermediate, sizeof(WindowAvgIntermediate));
        break;
    }
    case MIN:
    case MAX: {
        WindowMinMaxIntermediate window_intermediate;
        _window_intermediate.assign((char*)&window_intermediate, sizeof(WindowMinMaxIntermediate));
        break;
    }
    case ROW_NUMBER: {
        WindowRowNumberIntermediate window_intermediate;
        _window_intermediate.assign((char*)&window_intermediate, sizeof(WindowRowNumberIntermediate));
        break;
    }
    case RANK:
    case DENSE_RANK:
    case PERCENT_RANK: 
    case CUME_DIST: {
        WindowRankIntermediate window_intermediate;
        _window_intermediate.assign((char*)&window_intermediate, sizeof(WindowRankIntermediate));
        break;
    }
    case NTILE: {
        WindowNtileIntermediate window_intermediate;
        _window_intermediate.assign((char*)&window_intermediate, sizeof(WindowNtileIntermediate));
        break;
    }
    case LEAD:
    case LAG: {
        WindowLeadLagIntermediate window_intermediate;
        _window_intermediate.assign((char*)&window_intermediate, sizeof(WindowLeadLagIntermediate));
        break;
    }
    case FIRST_VALUE:
    case LAST_VALUE:
    case NTH_VALUE: {
        WindowValueIntermediate window_intermediate;
        _window_intermediate.assign((char*)&window_intermediate, sizeof(WindowValueIntermediate));
        break;
    }
    default: {
        DB_WARNING("Invalid window type : %d", _window_type);
        return -1;
    }
    }
    return 0;
}

int WindowFnCall::call(RowBatch* batch, const int start, const int end) {
    if (batch == nullptr) {
        DB_WARNING("batch is nullptr");
        return -1;
    }
    if (start < 0 || start >= batch->size() || end <= 0 || end > batch->size()) {
        DB_WARNING("invalid param: start[%d], end[%d]", start, end);
        return -1;
    }
    switch (_window_type) {
    case COUNT_STAR: {
        WindowCountIntermediate* window_intermediate = (WindowCountIntermediate*)_window_intermediate.c_str();
        window_intermediate->cnt += (end - start);
        break;
    }
    case COUNT: {
        WindowCountIntermediate* window_intermediate = (WindowCountIntermediate*)_window_intermediate.c_str();
        for (int i = start; i < end; ++i) {
            bool can_count = true;
            for (auto child : _children) {
                if (child->get_value(batch->get_row(i).get()).is_null()) {
                    can_count = false;
                    break;
                }
            }
            if (can_count) {
               window_intermediate->cnt++;
            }
        }
        break;
    }
    case SUM: {
        WindowSumIntermediate* window_intermediate = (WindowSumIntermediate*)_window_intermediate.c_str();
        for (int i = start; i < end; ++i) {
            ExprValue value = _children[0]->get_value(batch->get_row(i).get()).cast_to(_col_type);
            if (!value.is_null()) {
                if (window_intermediate->sum.is_null()) {
                    window_intermediate->sum = value;
                } else {
                    window_intermediate->sum.add(value);
                }
            }
        }
        break;
    }
    case AVG: {
        WindowAvgIntermediate* window_intermediate = (WindowAvgIntermediate*)_window_intermediate.c_str();
        for (int i = start; i < end; ++i) {
            ExprValue value = _children[0]->get_value(batch->get_row(i).get()).cast_to(_col_type);
            if (!value.is_null()) {
                window_intermediate->sum += value.get_numberic<double>();
                window_intermediate->cnt++;
            }
        }
        break;
    }
    case MIN: {
        WindowMinMaxIntermediate* window_intermediate = (WindowMinMaxIntermediate*)_window_intermediate.c_str();
        for (int i = start; i < end; ++i) {
            ExprValue value = _children[0]->get_value(batch->get_row(i).get()).cast_to(_col_type);
            if (!value.is_null()) {
                if (window_intermediate->min_max.is_null() ||
                        window_intermediate->min_max.compare(value) > 0) {
                    window_intermediate->min_max = value;
                }
            }
        }
        break;
    }
    case MAX: {
        WindowMinMaxIntermediate* window_intermediate = (WindowMinMaxIntermediate*)_window_intermediate.c_str();
        for (int i = start; i < end; ++i) {
            ExprValue value = _children[0]->get_value(batch->get_row(i).get()).cast_to(_col_type);
            if (!value.is_null()) {
                if (window_intermediate->min_max.is_null() ||
                        window_intermediate->min_max.compare(value) < 0) {
                    window_intermediate->min_max = value;
                }
            }
        }
        break;
    }
    case ROW_NUMBER: {
        break;
    }
    case RANK:
    case DENSE_RANK:
    case PERCENT_RANK: 
    case CUME_DIST: {
        WindowRankIntermediate* window_intermediate = (WindowRankIntermediate*)_window_intermediate.c_str();
        window_intermediate->cur_batch = batch;
        break;
    }
    case NTILE: {
        WindowNtileIntermediate* window_intermediate = (WindowNtileIntermediate*)_window_intermediate.c_str();
        if (window_intermediate->n <= 0) {
            DB_WARNING("invalid n: %ld", window_intermediate->n);
            return -1;
        }
        int64_t num_rows = batch->size();
        window_intermediate->quotient = num_rows / window_intermediate->n;
        window_intermediate->remainder = num_rows % window_intermediate->n;
        break;
    }
    case LEAD:
    case LAG: {
        WindowLeadLagIntermediate* window_intermediate = (WindowLeadLagIntermediate*)_window_intermediate.c_str();
        window_intermediate->cur_batch = batch;
        break;
    }
    case FIRST_VALUE: {
        WindowValueIntermediate* window_intermediate = (WindowValueIntermediate*)_window_intermediate.c_str();
        window_intermediate->value = _children[0]->get_value(batch->get_row(start).get()).cast_to(_col_type);
        break;
    }
    case LAST_VALUE: {
        WindowValueIntermediate* window_intermediate = (WindowValueIntermediate*)_window_intermediate.c_str();
        window_intermediate->value = _children[0]->get_value(batch->get_row(end - 1).get()).cast_to(_col_type);
        break;
    }
    case NTH_VALUE: {
        WindowValueIntermediate* window_intermediate = (WindowValueIntermediate*)_window_intermediate.c_str();
        int idx = start + window_intermediate->n - 1;
        if (idx >= start && idx < end) {
            window_intermediate->value = _children[0]->get_value(batch->get_row(idx).get()).cast_to(_col_type);
        }
        break;
    }
    default: {
        DB_WARNING("Invalid window type : %d", _window_type);
        return -1;
    }
    }
    return 0;
}

int WindowFnCall::get_result(ExprValue& expr_value) {
    switch (_window_type) {
    case COUNT_STAR:
    case COUNT: {
        WindowCountIntermediate* window_intermediate = (WindowCountIntermediate*)_window_intermediate.c_str();
        expr_value.type = _col_type;
        expr_value._u.int64_val = window_intermediate->cnt;
        break;
    }
    case SUM: {
        WindowSumIntermediate* window_intermediate = (WindowSumIntermediate*)_window_intermediate.c_str();
        expr_value = window_intermediate->sum;
        break;
    }
    case AVG: {
        WindowAvgIntermediate* window_intermediate = (WindowAvgIntermediate*)_window_intermediate.c_str();
        if (window_intermediate->cnt != 0) {
            expr_value.type = _col_type;
            expr_value._u.double_val = window_intermediate->sum / window_intermediate->cnt;
        } else {
            expr_value = ExprValue::Null();
        }
        break;
    }
    case MIN:
    case MAX: {
        WindowMinMaxIntermediate* window_intermediate = (WindowMinMaxIntermediate*)_window_intermediate.c_str();
        expr_value = window_intermediate->min_max;
        break;
    }
    case ROW_NUMBER: {
        WindowRowNumberIntermediate* window_intermediate = (WindowRowNumberIntermediate*)_window_intermediate.c_str();
        expr_value.type = _col_type;
        window_intermediate->row_number++;
        expr_value._u.int64_val = window_intermediate->row_number;
        break;
    }
    case RANK: // 1、1、3，跳过重复序号
    case DENSE_RANK: { // 1、1、2，不跳过重复序号
        if (_mem_row_compare->need_not_compare()) {
            // 没有排序列，直接返回1
            expr_value.type = _col_type;
            expr_value._u.int64_val = 1;
        } else {
            WindowRankIntermediate* window_intermediate = (WindowRankIntermediate*)_window_intermediate.c_str();
            window_intermediate->cur_idx++;
            if (window_intermediate->cur_idx == 1) {
                window_intermediate->last_rank = 1;
            } else {
                if (window_intermediate->cur_idx < 2 || 
                        window_intermediate->cur_idx > window_intermediate->cur_batch->size()) {
                    DB_WARNING("Invalid window index : %ld, batch size: %lu", 
                                window_intermediate->cur_idx, window_intermediate->cur_batch->size());
                    return -1;
                }
                MemRow* last_row = window_intermediate->cur_batch->get_row(window_intermediate->cur_idx - 2).get();
                MemRow* cur_row = window_intermediate->cur_batch->get_row(window_intermediate->cur_idx - 1).get();
                if (_mem_row_compare->compare(last_row, cur_row) != 0) {
                    if (_window_type == DENSE_RANK) {
                        window_intermediate->last_rank++;
                    } else {
                        window_intermediate->last_rank = window_intermediate->cur_idx;
                    }
                }
            }
            expr_value.type = _col_type;
            expr_value._u.int64_val = window_intermediate->last_rank;
        }
        break;
    }
    case PERCENT_RANK: {
        // 计算「当前行排名 - 1」/ 「总行数 - 1」 
        if (_mem_row_compare->need_not_compare()) {
            // 没有排序列，直接返回0
            expr_value.type = _col_type;
            expr_value._u.double_val = 0;
        } else {
            WindowRankIntermediate* window_intermediate = (WindowRankIntermediate*)_window_intermediate.c_str();
            int32_t num_rows = window_intermediate->cur_batch->size();
            if (num_rows == 0) {
                DB_WARNING("empty batch");
                return -1;
            }
            window_intermediate->cur_idx++;
            if (window_intermediate->cur_idx == 1) {
                window_intermediate->last_rank = 1;
            } else {
                if (window_intermediate->cur_idx < 2 || 
                        window_intermediate->cur_idx > window_intermediate->cur_batch->size()) {
                    DB_WARNING("Invalid window index : %ld, batch size: %lu", 
                                window_intermediate->cur_idx, window_intermediate->cur_batch->size());
                    return -1;
                }
                MemRow* last_row = window_intermediate->cur_batch->get_row(window_intermediate->cur_idx - 2).get();
                MemRow* cur_row = window_intermediate->cur_batch->get_row(window_intermediate->cur_idx - 1).get();
                if (_mem_row_compare->compare(last_row, cur_row) != 0) {
                    window_intermediate->last_rank = window_intermediate->cur_idx;
                }
            }
            expr_value.type = _col_type;
            if (num_rows == 1) {
                expr_value._u.double_val = 0;
            } else {
                expr_value._u.double_val = static_cast<double>(window_intermediate->last_rank - 1) / (num_rows - 1);
            }
        }
        break;
    }
    case CUME_DIST: {
        // 计算「小于等于当前行值的行数量」/「总行数」
        if (_mem_row_compare->need_not_compare()) {
            // 没有排序列，直接返回1
            expr_value.type = _col_type;
            expr_value._u.double_val = 1;
        } else {
            WindowRankIntermediate* window_intermediate = (WindowRankIntermediate*)_window_intermediate.c_str();
            int32_t num_rows = window_intermediate->cur_batch->size();
            if (num_rows == 0) {
                DB_WARNING("empty batch");
                return -1;
            }
            if (window_intermediate->cur_idx < 0 || 
                    window_intermediate->cur_idx >= window_intermediate->cur_batch->size()) {
                DB_WARNING("Invalid window index : %ld, batch size: %lu", 
                            window_intermediate->cur_idx, window_intermediate->cur_batch->size());
                return -1;
            }
            while (true) {
                if (window_intermediate->last_rank < 0) {
                    DB_WARNING("Invalid window last rank: %ld, batch size: %lu", 
                                window_intermediate->last_rank, window_intermediate->cur_batch->size());
                    return -1;
                }
                if (window_intermediate->last_rank >= window_intermediate->cur_batch->size()) {
                    break;
                }
                MemRow* cur_row = window_intermediate->cur_batch->get_row(window_intermediate->cur_idx).get();
                MemRow* last_row = window_intermediate->cur_batch->get_row(window_intermediate->last_rank).get();
                if (_mem_row_compare->compare(cur_row, last_row) != 0) {
                    break;
                }
                ++window_intermediate->last_rank;
            }
            expr_value.type = _col_type;
            expr_value._u.double_val = static_cast<double>(window_intermediate->last_rank) / num_rows;
            ++window_intermediate->cur_idx;
        }
        break;
    }
    case NTILE: {
        // 将所有行数尽可能均匀地分到n个桶中；
        // e.g
        //  - 10行分4个桶
        //      - 1号桶: 3行
        //      - 2号桶: 3行
        //      - 3号桶: 2行
        //      - 4号桶: 2行
        WindowNtileIntermediate* window_intermediate = (WindowNtileIntermediate*)_window_intermediate.c_str();
        expr_value.type = _col_type;
        expr_value._u.int64_val = window_intermediate->cur_group_idx;
        ++window_intermediate->cur_idx;
        int64_t cur_max_idx = window_intermediate->quotient;
        if (window_intermediate->cur_group_idx <= window_intermediate->remainder) {
            ++cur_max_idx;
        }
        if (window_intermediate->cur_idx == cur_max_idx) {
            window_intermediate->cur_idx = 0;
            ++window_intermediate->cur_group_idx;
        }
        break;
    }
    case LEAD:
    case LAG: {
        // LEAD: 返回当前行下方指定偏移量的表达式值
        // LAG: 返回当前行上方指定偏移量的表达式值
        WindowLeadLagIntermediate* window_intermediate = (WindowLeadLagIntermediate*)_window_intermediate.c_str();
        int64_t idx = window_intermediate->cur_idx;
        if (_window_type == LEAD) {
            idx += window_intermediate->offset;
        } else {
            idx -= window_intermediate->offset;
        }
        if (idx >= 0 && idx < window_intermediate->cur_batch->size()) {
            expr_value = 
                _children[0]->get_value(window_intermediate->cur_batch->get_row(idx).get()).cast_to(_col_type);
        } else {
            if (window_intermediate->default_expr != nullptr) {
                int64_t cur_idx = window_intermediate->cur_idx;
                if (cur_idx < 0 || cur_idx >= window_intermediate->cur_batch->size()) {
                    DB_WARNING("Invalid cur_idx: %ld, size: %lu", cur_idx, window_intermediate->cur_batch->size());
                    return -1;
                }
                expr_value = window_intermediate->default_expr->get_value(
                                window_intermediate->cur_batch->get_row(cur_idx).get()).cast_to(_col_type);
            }
        }
        ++window_intermediate->cur_idx;
        break;
    }
    case FIRST_VALUE:
    case LAST_VALUE: 
    case NTH_VALUE: {
        WindowValueIntermediate* window_intermediate = (WindowValueIntermediate*)_window_intermediate.c_str();
        expr_value = window_intermediate->value;
        break;
    }
    default: {
        DB_WARNING("Invalid window type : %d", _window_type);
        return -1;
    }
    }
    return 0;
}

int WindowFnCall::frame_reset() {
    switch (_window_type) {
    case COUNT_STAR:
    case COUNT: {
        WindowCountIntermediate* window_intermediate = (WindowCountIntermediate*)_window_intermediate.c_str();
        window_intermediate->cnt = 0;
        break;
    }
    case SUM: {
        WindowSumIntermediate* window_intermediate = (WindowSumIntermediate*)_window_intermediate.c_str();
        window_intermediate->sum = ExprValue::Null();
        break;
    }
    case AVG: {
        WindowAvgIntermediate* window_intermediate = (WindowAvgIntermediate*)_window_intermediate.c_str();
        window_intermediate->sum = 0;
        window_intermediate->cnt = 0;
        break;
    }
    case MIN:
    case MAX: {
        WindowMinMaxIntermediate* window_intermediate = (WindowMinMaxIntermediate*)_window_intermediate.c_str();
        window_intermediate->min_max = ExprValue::Null();
        break;
    }
    case ROW_NUMBER:
    case RANK:
    case DENSE_RANK:
    case PERCENT_RANK: 
    case CUME_DIST:
    case NTILE:
    case LEAD:
    case LAG: {
        // 这几类函数只在partition粒度使用，frame粒度不需要重置
        break;
    }
    case FIRST_VALUE:
    case LAST_VALUE: 
    case NTH_VALUE: {
        WindowValueIntermediate* window_intermediate = (WindowValueIntermediate*)_window_intermediate.c_str();
        window_intermediate->value = ExprValue::Null();
        break;
    }
    default: {
        DB_WARNING("Invalid window type : %d", _window_type);
        return -1;
    }
    }
    return 0;
}

int WindowFnCall::partition_reset() {
    switch (_window_type) {
    case COUNT_STAR:
    case COUNT: {
        WindowCountIntermediate* window_intermediate = (WindowCountIntermediate*)_window_intermediate.c_str();
        window_intermediate->cnt = 0;
        break;
    }
    case SUM: {
        WindowSumIntermediate* window_intermediate = (WindowSumIntermediate*)_window_intermediate.c_str();
        window_intermediate->sum = ExprValue::Null();
        break;
    }
    case AVG: {
        WindowAvgIntermediate* window_intermediate = (WindowAvgIntermediate*)_window_intermediate.c_str();
        window_intermediate->sum = 0;
        window_intermediate->cnt = 0;
        break;
    }
    case MIN:
    case MAX: {
        WindowMinMaxIntermediate* window_intermediate = (WindowMinMaxIntermediate*)_window_intermediate.c_str();
        window_intermediate->min_max = ExprValue::Null();
        break;
    }
    case ROW_NUMBER: {
        WindowRowNumberIntermediate* window_intermediate = (WindowRowNumberIntermediate*)_window_intermediate.c_str();
        window_intermediate->row_number = 0;
        break;
    }
    case RANK:
    case DENSE_RANK:
    case PERCENT_RANK:
    case CUME_DIST: {
        WindowRankIntermediate* window_intermediate = (WindowRankIntermediate*)_window_intermediate.c_str();
        window_intermediate->cur_idx = 0;
        window_intermediate->last_rank = 0;
        window_intermediate->cur_batch = nullptr;
        break;
    }
    case NTILE: {
        WindowNtileIntermediate* window_intermediate = (WindowNtileIntermediate*)_window_intermediate.c_str();
        window_intermediate->cur_idx = 0;
        window_intermediate->cur_group_idx = 1;
        window_intermediate->quotient = 0;
        window_intermediate->remainder = 0;
        break;
    }
    case LEAD:
    case LAG: {
        WindowLeadLagIntermediate* window_intermediate = (WindowLeadLagIntermediate*)_window_intermediate.c_str();
        window_intermediate->cur_idx = 0;
        window_intermediate->cur_batch = nullptr;
        break;
    }
    case FIRST_VALUE:
    case LAST_VALUE:
    case NTH_VALUE: {
        WindowValueIntermediate* window_intermediate = (WindowValueIntermediate*)_window_intermediate.c_str();
        window_intermediate->value = ExprValue::Null();
        break;
    }
    default: {
        DB_WARNING("Invalid window type : %d", _window_type);
        return -1;
    }
    }
    return 0;
}

} // namespace baikaldb