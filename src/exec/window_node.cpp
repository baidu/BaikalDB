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

#include "window_node.h"

namespace baikaldb {
    
// WindowNode
int WindowNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    const pb::WindowNode& window_node = node.derive_node().window_node();
    const pb::WindowSpec& window_spec = window_node.window_spec();
    for (const auto& expr : window_spec.partition_exprs()) {
        ExprNode* partition_expr = nullptr;
        ret = ExprNode::create_tree(expr, &partition_expr);
        if (ret < 0) {
            DB_WARNING("create_tree fail, ret:%d", ret);
            return ret;
        }
        _partition_exprs.emplace_back(partition_expr);
    }
    // 构造WindowProcessor
    if (!window_spec.has_window_frame()) {
        _window_processor = std::make_shared<NonFrameWindowProcessor>();
    } else {
        const pb::WindowFrame& window_frame = window_spec.window_frame();
        const pb::FrameBound& frame_start = window_frame.frame_extent().frame_start();
        const pb::FrameBound& frame_end = window_frame.frame_extent().frame_end();
        if (frame_start.is_unbounded() && frame_start.bound_type() == pb::BT_PRECEDING &&
                frame_end.is_unbounded() && frame_end.bound_type() == pb::BT_FOLLOWING) {
            // ROWS/RNAGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDE FOLLOWING转化为非frame模式
            _window_processor = std::make_shared<NonFrameWindowProcessor>();
        } else if (window_frame.frame_type() == pb::FT_ROWS) {
            _window_processor = std::make_shared<RowFrameWindowProcessor>();
            _window_processor->set_need_parse_frame(true);
        } else if (window_frame.frame_type() == pb::FT_RANGE) {
            _window_processor = std::make_shared<RangeFrameWindowProcessor>();
            _window_processor->set_need_parse_frame(true);
        } else {
            DB_WARNING("frame type: %d not supported", window_frame.frame_type());
            return -1;
        }
    }
    ret = _window_processor->init(window_node);
    if (ret < 0) {
        DB_WARNING("Fail to init window processor");
        return ret;
    }
    _window_processor->set_limit(_limit);
    return 0;
}

int WindowNode::open(RuntimeState* state) {
    int ret = 0;
    ret = ExecNode::open(state);
    if (ret < 0) {
        DB_WARNING_STATE(state, "ExecNode::open fail, ret:%d", ret);
        return ret;
    }
    if (_children.size() == 0) {
        DB_WARNING("children size is 0");
        return -1;
    }
    for (auto& expr : _partition_exprs) {
        ret = expr->open();
        if (ret < 0) {
            DB_WARNING_STATE(state, "expr open fail, ret:%d", ret);
            return ret;
        }
    }
    ret = _window_processor->open();
    if (ret < 0) {
        DB_WARNING_STATE(state, "_window_processor open fail, ret:%d", ret);
        return ret;
    }
    return 0;
}

int WindowNode::get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), GET_NEXT_TRACE, ([this](TraceLocalNode& local_node) {
        local_node.set_affect_rows(_num_rows_returned);
    }));
    int ret = 0;
    while (true) {
        if (state->is_cancelled()) {
            DB_WARNING_STATE(state, "cancelled");
            *eos = true;
            return 0;
        }
        if (reached_limit()) {
            *eos = true;
            return 0;
        }
        if (batch->is_full()) {
            return 0;
        }
        // 如果get_next()只获取到分区的部分数据，则再一次调用get_next()，直到获取到整个分区数据；
        // 如果get_next()获取到多个分区的数据，则将多个分区结束位置缓存，并从第一个分区开始处理；
        if (_child_eos && !has_cache_partition()) {
            *eos = true;
            break;
        }
        bool is_first_partition_belong_to_prev = false;
        if (!has_cache_partition()) {
            _child_row_batch.clear();
            ret = _children[0]->get_next(state, &_child_row_batch, &_child_eos);
            if (ret < 0) {
                DB_WARNING_STATE(state, "get next fail, ret:%d", ret);
                return ret;
            }
            if (_child_row_batch.size() == 0) {
                continue;
            }
            ret = split_into_partitions(&_child_row_batch, is_first_partition_belong_to_prev);
            if (ret < 0) {
                DB_WARNING_STATE(state, "split into partition fail, ret:%d", ret);
                return ret;
            }
        }
        int32_t start = -1;
        int32_t end = -1;
        get_next_partition(start, end);
        if (start < 0 || end > _child_row_batch.size() || start >= end) {
            DB_WARNING_STATE(state, "get next partition fail, start:%d end:%d", start, end);
            return -1;
        }
        RowBatch cur_partition_batch;
        for (int i = start; i < end; ++i) {
            cur_partition_batch.move_row(std::move(_child_row_batch.get_row(i)));
        }
        // 判断end是否为当前RowBatch的结尾
        // 如果是当前RowBatch的结尾，则继续获取下一个RowBatch，探索下一个RowBatch是否有当前分区的数据
        while (end == _child_row_batch.size() && !_child_eos) {
            _child_row_batch.clear();
            ret = _children[0]->get_next(state, &_child_row_batch, &_child_eos);
            if (ret < 0) {
                DB_WARNING_STATE(state, "get next fail, ret:%d", ret);
                return ret;
            }
            if (_child_row_batch.size() == 0) {
                continue;
            }
            ret = split_into_partitions(&_child_row_batch, is_first_partition_belong_to_prev);
            if (ret < 0) {
                DB_WARNING_STATE(state, "split into partition fail, ret:%d", ret);
                return ret;
            }
            if (is_first_partition_belong_to_prev) {
                get_next_partition(start, end);
                if (start < 0 || end > _child_row_batch.size() || start >= end) {
                    DB_WARNING_STATE(state, "get next partition fail, start:%d end:%d", start, end);
                    return -1;
                }
                for (int i = start; i < end; ++i) {
                    cur_partition_batch.move_row(std::move(_child_row_batch.get_row(i)));
                }
            }
        }
        ret = _window_processor->process_one_partition(&cur_partition_batch);
        if (ret < 0) {
            DB_WARNING_STATE(state, "process_one_partition fail, ret:%d", ret);
            return ret;
        }
        for (int i = 0; i < cur_partition_batch.size(); ++i) {
            batch->move_row(std::move(cur_partition_batch.get_row(i)));
            ++_num_rows_returned;
            if (reached_limit()) {
                DB_WARNING_STATE(state, "reach limit size:%lu", batch->size());
                *eos = true;
                return 0;
            }
        }
    }
    return 0;
}

void WindowNode::close(RuntimeState* state) {
    ExecNode::close(state);
    for (auto expr : _partition_exprs) {
        expr->close();
    }
    _window_processor->close();
    _child_eos = false;
}

int WindowNode::expr_optimize(QueryContext* ctx) {
    int ret = 0;
    ret = ExecNode::expr_optimize(ctx);
    if (ret < 0) {
        DB_WARNING("ExecNode::optimize fail, ret:%d", ret);
        return ret;
    }
    ret = common_expr_optimize(&_partition_exprs);
    if (ret < 0) {
        DB_WARNING("common_expr_optimize fail");
        return ret;
    }
    ret = _window_processor->expr_optimize(ctx);
    if (ret < 0) {
        DB_WARNING("expr_optimize fail");
        return ret;
    }
    return 0;
}

void WindowNode::find_place_holder(std::unordered_multimap<int, ExprNode*>& placeholders) {
    ExecNode::find_place_holder(placeholders);
    for (auto& expr : _partition_exprs) {
        expr->find_place_holder(placeholders);
    }
    _window_processor->find_place_holder(placeholders);
}

void WindowNode::transfer_pb(int64_t region_id, pb::PlanNode* pb_node) {
    ExecNode::transfer_pb(region_id, pb_node);
    auto window_node = pb_node->mutable_derive_node()->mutable_window_node();
    // func_exprs
    window_node->clear_func_exprs();
    for (auto& expr : *(_window_processor->mutable_window_fn_calls())) {
        ExprNode::create_pb_expr(window_node->add_func_exprs(), expr);
    }
    auto window_spec = window_node->mutable_window_spec();
    // partition_exprs
    window_spec->clear_partition_exprs();
    for (auto& expr : _partition_exprs) {
        ExprNode::create_pb_expr(window_spec->add_partition_exprs(), expr);
    }
    // order_exprs
    window_spec->clear_order_exprs();
    for (auto& expr : *(_window_processor->mutable_slot_order_exprs())) {
        ExprNode::create_pb_expr(window_spec->add_order_exprs(), expr);
    }
}

int WindowNode::split_into_partitions(RowBatch* batch, bool& is_first_partition_belong_to_prev) {
    _next_partition_id = 0;
    _partition_cnt = 0;
    _partition_offset_vec.clear();
    is_first_partition_belong_to_prev = false;

    const int num_rows = batch->size();
    // 没有分区键，则所有行属于一个分区
    if (_partition_exprs.empty()) {
        _partition_offset_vec.emplace_back(num_rows);
        _partition_cnt = 1;
        is_first_partition_belong_to_prev = true;
        return 0;
    }
    // 判断当前RowBatch的第一个分区是否和上一个RowBatch的最后一个分区属于相同的分区
    MutTableKey first_key;
    MutTableKey last_key;
    encode_exprs_key(_partition_exprs, batch->get_row(0).get(), first_key);
    encode_exprs_key(_partition_exprs, batch->get_row(num_rows - 1).get(), last_key);
    if (_last_partition_key.size() == 0) {
        is_first_partition_belong_to_prev = false;
    } else {
        if (first_key.data() == _last_partition_key.data()) {
            is_first_partition_belong_to_prev = true;
        } else {
            is_first_partition_belong_to_prev = false;
        }
    }
    _last_partition_key = last_key;
    // RowBatch属于一个分区
    if (first_key.data() == last_key.data()) {
        _partition_offset_vec.emplace_back(num_rows);
        _partition_cnt = 1;
        return 0;
    }
    // RowBatch属于多个分区
    // TODO - 二分优化
    last_key = first_key;
    for (int i = 1; i < num_rows; ++i) {
        MutTableKey cur_key;
        encode_exprs_key(_partition_exprs, batch->get_row(i).get(), cur_key);
        if (cur_key.data() != last_key.data()) {
            _partition_offset_vec.emplace_back(i);
            last_key = cur_key;
        }
    }
    _partition_offset_vec.emplace_back(num_rows);
    _partition_cnt = _partition_offset_vec.size();
    return 0;
}

// WindowProcessor
int WindowProcessor::init(const pb::WindowNode& window_node) {
    int ret = 0;
    for (const auto& func_expr : window_node.func_exprs()) {
        if (func_expr.nodes_size() < 1 || func_expr.nodes(0).node_type() != pb::WINDOW_EXPR) {
            DB_WARNING("WindowNode::init fail, func_expr.nodes_size:%d", func_expr.nodes_size());
            return -1;
        }
        ExprNode* window_call = nullptr;
        ret = ExprNode::create_tree(func_expr, &window_call);
        if (ret < 0) {
            return ret;
        }
        _window_fn_calls.emplace_back(static_cast<WindowFnCall*>(window_call));
    }
    const pb::WindowSpec& window_spec = window_node.window_spec();
    if (window_spec.order_exprs().size() != window_spec.is_asc().size()) {
        DB_WARNING("WindowNode::init fail, order_exprs.size:%d is_asc.size:%d",
                    window_spec.order_exprs().size(), window_spec.is_asc().size());
        return -1;
    }
    for (int i = 0; i < window_spec.order_exprs().size(); ++i) {
        ExprNode* order_expr = nullptr;
        ret = ExprNode::create_tree(window_spec.order_exprs(i), &order_expr);
        if (ret < 0) {
            DB_WARNING("create order expr fail");
            return ret;
        }
        _slot_order_exprs.emplace_back(order_expr);
        _is_asc.emplace_back(window_spec.is_asc(i));
        _is_null_first.emplace_back(window_spec.is_asc(i));
    }

    // 解析WindowFrame
    if (_need_parse_frame && window_node.window_spec().has_window_frame()) {
        const pb::FrameExtent& frame_extent = window_node.window_spec().window_frame().frame_extent();
        if (!frame_extent.has_frame_start() || !frame_extent.has_frame_end()) {
            DB_WARNING("frame_extent has no frame_start or frame_end");
            return -1;
        }
        const pb::FrameBound& frame_start = frame_extent.frame_start();
        _frame_start.bound_type = frame_start.bound_type();
        _frame_start.is_unbounded = frame_start.is_unbounded();
        if (frame_start.has_expr()) {
            ExprNode* expr_node = nullptr;
            ret = ExprNode::create_tree(frame_start.expr(), &expr_node);
            if (ret < 0) {
                DB_WARNING("Fail to create_tree");
                return -1;
            } 
            if (expr_node == nullptr) {
                DB_WARNING("expr_node is nullptr");
                return -1;
            }
            if (expr_node->node_type() != pb::INT_LITERAL) {
                DB_WARNING("_frame_start->expr_node is not INT_LITERAL");
                return -1;
            }
            const int num = expr_node->get_value(nullptr).cast_to(pb::INT32).get_numberic<int32_t>();
            if (num < 0) {
                DB_WARNING("frame start num should not be negative, %d", num);
                return -1;
            }
            _frame_start.num = num;
        }
        const pb::FrameBound& frame_end = frame_extent.frame_end();
        _frame_end.bound_type = frame_end.bound_type();
        _frame_end.is_unbounded = frame_end.is_unbounded();
        if (frame_end.has_expr()) {
            ExprNode* expr_node = nullptr;
            ret = ExprNode::create_tree(frame_end.expr(), &expr_node);
            if (ret < 0) {
                DB_WARNING("Fail to create_tree");
                return -1;
            } 
            if (expr_node == nullptr) {
                DB_WARNING("expr_node is nullptr");
                return -1;
            }
            if (expr_node->node_type() != pb::INT_LITERAL) {
                DB_WARNING("expr_node is not INT_LITERAL");
                return -1;
            }
            const int num = expr_node->get_value(nullptr).cast_to(pb::INT32).get_numberic<int32_t>();
            if (num < 0) {
                DB_WARNING("frame start num should not be negative, %d", num);
                return -1;
            }
            _frame_end.num = num;
        }
    }
    return 0;
}

int WindowProcessor::open() {
    int ret = 0;
    for (auto& expr : _slot_order_exprs) {
        ret = expr->open();
        if (ret < 0) {
            DB_WARNING("expr open fail, ret: %d", ret);
            return ret;
        }
    }
    _mem_row_compare = std::make_shared<MemRowCompare>(_slot_order_exprs, _is_asc, _is_null_first);
    for (auto& window_fn : _window_fn_calls) {
        // rank类函数使用_mem_row_compare
        ret = window_fn->open(_mem_row_compare);
        if (ret < 0) {
            DB_WARNING("window open fail, ret: %d", ret);
            return ret;
        }
    }
    return 0;
}

int WindowProcessor::expr_optimize(QueryContext* ctx) {
    int ret = 0;
    for (auto& window_fn : _window_fn_calls) {
        // 类型推导
        ret = window_fn->type_inferer(ctx);
        if (ret < 0) {
            DB_WARNING("expr type_inferer fail: %d", ret);
            return ret;
        }
        // 常量表达式计算
        window_fn->const_pre_calc();
    }
    for (auto& expr : _slot_order_exprs) {
        ret = expr->expr_optimize();
        if (ret < 0) {
            DB_WARNING("expr const_pre_calc fail: %d", ret);
            return ret;
        }
    }
    return 0;
}

void WindowProcessor::find_place_holder(std::unordered_multimap<int, ExprNode*>& placeholders) {
    for (auto& window_fn : _window_fn_calls) {
        window_fn->find_place_holder(placeholders);
    }
    for (auto& expr : _slot_order_exprs) {
        expr->find_place_holder(placeholders);
    }
}

void WindowProcessor::close() {
    for (auto& window_fn : _window_fn_calls) {
        window_fn->close();
    }
    for (auto& expr : _slot_order_exprs) {
        expr->close();
    }
}

int NonFrameWindowProcessor::process_one_partition(RowBatch* batch) {
    if (batch == nullptr) {
        DB_WARNING("batch is nullptr");
        return -1;
    }
    if (batch->size() == 0) {
        DB_WARNING("batch size is 0");
        return 0;
    }
    int ret = 0;
    for (auto& window_fn : _window_fn_calls) {
        ret = window_fn->partition_reset();
        if (ret != 0) {
            DB_WARNING("Fail to partition_reset window function");
            return -1;
        }
    }
    // 没有frame的场景，只调用一次call
    for (auto& window_fn : _window_fn_calls) {
        ret = window_fn->call(batch, 0, batch->size());
        if (ret != 0) {
            DB_WARNING("Fail to call window function");
            return -1;
        }
    }
    for (int i = 0; i < batch->size(); ++i) {
        for (auto& window_fn : _window_fn_calls) {
            ExprValue expr_value;
            ret = window_fn->get_result(expr_value);
            if (ret != 0) {
                DB_WARNING("Fail to get_result window function");
                return -1;
            }
            batch->get_row(i)->set_value(window_fn->tuple_id(), window_fn->slot_id(), expr_value);
            ++_num_rows_returned;
            if (reached_limit()) {
                return 0;
            }
        }
    }
    return 0;
}

// FrameWindowProcessor 
int FrameWindowProcessor::process_one_partition(RowBatch* batch) {
    if (batch == nullptr) {
        DB_WARNING("batch is nullptr");
        return -1;
    }
    if (batch->size() == 0) {
        DB_WARNING("batch size is 0");
        return 0;
    }
    int ret = 0;
    for (auto& window_fn : _window_fn_calls) {
        ret = window_fn->partition_reset();
        if (ret != 0) {
            DB_WARNING("Fail to partition_reset window function");
            return -1;
        }
    }
    for (int i = 0; i < batch->size(); ++i) {
        for (auto& window_fn : _window_fn_calls) {
            ret = window_fn->frame_reset();
            if (ret != 0) {
                DB_WARNING("Fail to frame_reset window function");
                return -1;
            }
            const int start_row = get_start_row(i, batch);
            const int end_row = get_end_row(i, batch);
            if (start_row < 0 || end_row < 0) {
                return -1;
            }
            // 包含frame的场景，每个frame调用一次call
            if (start_row < end_row) {
                ret = window_fn->call(batch, start_row, end_row);
                if (ret != 0) {
                    DB_WARNING("Fail to call window function");
                    return -1;
                }
            }
            ExprValue expr_value;
            ret = window_fn->get_result(expr_value);
            if (ret != 0) {
                DB_WARNING("Fail to get_result window function");
                return -1;
            }
            batch->get_row(i)->set_value(window_fn->tuple_id(), window_fn->slot_id(), expr_value);
            ++_num_rows_returned;
            if (reached_limit()) {
                return 0;
            }
        }
    }
    return 0;
}

// RowFrameWindowProcessor 
int RowFrameWindowProcessor::get_start_row(const int32_t cur_row, RowBatch* batch) {
    if (batch == nullptr) {
        DB_WARNING("batch is nullptr");
        return -1;
    }
    if (cur_row < 0 || cur_row >= batch->size()) {
        DB_WARNING("invalid cur_row: %d", cur_row);
        return -1;
    }
    const int32_t num_rows = batch->size();
    if (_frame_start.is_unbounded && _frame_start.bound_type != pb::BT_PRECEDING) {
        DB_WARNING("Unbounded frame start only support preceding type");
        return -1;
    }
    if (_frame_start.is_unbounded) {
        // frame start只能是unbounded preceding，因此可以直接返回0
        return 0;
    }
    switch (_frame_start.bound_type) {
    case pb::BT_PRECEDING: {
        if (cur_row >= _frame_start.num) {
            return cur_row - _frame_start.num;
        }
        return 0;
    }
    case pb::BT_FOLLOWING: {
        if (cur_row + _frame_start.num < num_rows) {
            return cur_row + _frame_start.num;
        }
        return num_rows;
    }
    case pb::BT_CURRENT_ROW: {
        return cur_row;
    }
    default: {
        DB_WARNING("invalid frame_start bound_type: %d", _frame_start.bound_type);
        return -1;
    }
    }
    return -1;
}

int RowFrameWindowProcessor::get_end_row(const int32_t cur_row, RowBatch* batch) {
    if (batch == nullptr) {
        DB_WARNING("batch is nullptr");
        return -1;
    }
    if (cur_row < 0 || cur_row >= batch->size()) {
        DB_WARNING("invalid cur_row: %d", cur_row);
        return -1;
    }
    const int32_t num_rows = batch->size();
    if (_frame_end.is_unbounded && _frame_end.bound_type != pb::BT_FOLLOWING) {
        DB_WARNING("Unbounded frame end only support following type");
        return -1;
    }
    if (_frame_end.is_unbounded) {
        // frame end只能是unbounded following，因此可以直接返回num_rows
        return num_rows;
    }
    // 加1表示开区间
    switch (_frame_end.bound_type) {
    case pb::BT_PRECEDING: {
        if (cur_row >= _frame_end.num) {
            return cur_row - _frame_end.num + 1;
        }
        return 0;
    }
    case pb::BT_FOLLOWING: {
        if (cur_row + _frame_end.num < num_rows) {
            return cur_row + _frame_end.num + 1;
        }
        return num_rows;
    }
    case pb::BT_CURRENT_ROW: {
        return cur_row + 1;
    }
    default: {
        DB_WARNING("invalid frame_end bound_type: %d", _frame_end.bound_type);
        return -1;
    }
    }
    return -1;
}

// RangeFrameWindowProcessor
int RangeFrameWindowProcessor::get_start_row(const int32_t cur_row, RowBatch* batch) {
    if (batch == nullptr) {
        DB_WARNING("batch is nullptr");
        return -1;
    }
    if (cur_row < 0 || cur_row >= batch->size()) {
        DB_WARNING("invalid cur_row: %d", cur_row);
        return -1;
    }
    if (_frame_start.is_unbounded && _frame_start.bound_type != pb::BT_PRECEDING) {
        DB_WARNING("Unbounded frame start only support preceding type");
        return -1;
    }
    if (_frame_start.is_unbounded) {
        // frame start只能是unbounded preceding，因此可以直接返回0
        return 0;
    }
    switch (_frame_start.bound_type) {
    case pb::BT_CURRENT_ROW: {
        // Range模式下，CURRENT ROW需要将与当前行相同的行都包含进来
        int peer_row_start = cur_row;
        for (int i = cur_row - 1; i >= 0; --i) {
            if (_mem_row_compare->compare(batch->get_row(cur_row).get(), batch->get_row(i).get()) != 0) {
                break;
            }
            peer_row_start = i;
        }
        return peer_row_start;
    }
    case pb::BT_PRECEDING:
    case pb::BT_FOLLOWING: 
    default: {
        DB_WARNING("invalid frame_start bound_type: %d", _frame_end.bound_type);
        return -1;
    }
    }
    return -1;
}

int RangeFrameWindowProcessor::get_end_row(const int32_t cur_row, RowBatch* batch) {
    if (batch == nullptr) {
        DB_WARNING("batch is nullptr");
        return -1;
    }
    if (cur_row < 0 || cur_row >= batch->size()) {
        DB_WARNING("invalid cur_row: %d", cur_row);
        return -1;
    }
    const int32_t num_rows = batch->size();
    if (_frame_end.is_unbounded && _frame_end.bound_type != pb::BT_FOLLOWING) {
        DB_WARNING("Unbounded frame end only support following type");
        return -1;
    }
    if (_frame_end.is_unbounded) {
        // frame end只能是unbounded following，因此可以直接返回num_rows
        return num_rows;
    }
    // 加1表示开区间
    switch (_frame_end.bound_type) {
    case pb::BT_CURRENT_ROW: {
        // Range模式下，CURRENT ROW需要将与当前行相同的行都包含进来
        int peer_row_end = cur_row;
        for (int i = cur_row + 1; i < num_rows; ++i) {
            if (_mem_row_compare->compare(batch->get_row(cur_row).get(), batch->get_row(i).get()) != 0) {
                break;
            }
            peer_row_end = i;
        }
        return peer_row_end + 1;
    }
    case pb::BT_PRECEDING:
    case pb::BT_FOLLOWING: 
    default: {
        DB_WARNING("invalid frame_end bound_type: %d", _frame_end.bound_type);
        return -1;
    }
    }
    return -1;
}


} // namespace baikaldb
