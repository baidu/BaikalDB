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
#include "window_fn_call.h"

namespace baikaldb {

struct WindowProcessor;
class WindowNode : public ExecNode {
public:
    WindowNode() {
    }
    virtual ~WindowNode() {
        for (auto& expr : _partition_exprs) {
            ExprNode::destroy_tree(expr);
        }
    }
    virtual int init(const pb::PlanNode& node) override;
    virtual int open(RuntimeState* state) override;
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos) override;
    virtual void close(RuntimeState* state) override;
    virtual int expr_optimize(QueryContext* ctx) override;
    virtual void find_place_holder(std::unordered_multimap<int, ExprNode*>& placeholders) override;
    virtual void transfer_pb(int64_t region_id, pb::PlanNode* pb_node) override;
    virtual bool can_use_arrow_vector(RuntimeState* state) override {
        return false;
    }
    std::vector<ExprNode*>* mutable_partition_exprs() {
        return &_partition_exprs;
    }
    std::shared_ptr<WindowProcessor> mutable_window_processor() {
        return _window_processor;
    }

private:
    // @brief 将RowBatch按照分区键拆分成多个分区，并将每个分区的结束位置保存在_partition_offset_vec中
    // @param batch 输入的RowBatch
    // @param is_first_partition_belong_to_prev 当前RowBatch的第一个分区是否和上一个RowBatch的最后一个分区属于同一个分区
    int split_into_partitions(RowBatch* batch, bool& is_first_partition_belong_to_prev);

    // @brief 获取下一个分区的起始位置和结束位置，左闭右开
    void get_next_partition(int32_t& start, int32_t& end) {
        start = -1;
        end = -1;
        if (_next_partition_id < 0 || _next_partition_id >= _partition_offset_vec.size()) {
            DB_WARNING("Invalid _next_partition_id: %d", _next_partition_id);
            return;
        }
        if (_next_partition_id == 0) {
            start = 0;
        } else {
            start = _partition_offset_vec[_next_partition_id - 1];
        }
        end = _partition_offset_vec[_next_partition_id];
        _next_partition_id++;
    }

    // @brief 判断是否还有未处理的缓存的分区数据
    bool has_cache_partition() const {
        return _next_partition_id < _partition_cnt;
    }

private:
    std::vector<ExprNode*> _partition_exprs;
    std::shared_ptr<WindowProcessor> _window_processor;

    // 缓存分区数据
    RowBatch _child_row_batch;                  // 当前从子节点获取的需要处理的分区数据
    std::vector<int32_t> _partition_offset_vec; // 当前RowBatch拆分成多个分区，每个分区的结束位置
    int32_t _next_partition_id = 0;             // 下一个需要处理的分区id
    int32_t _partition_cnt = 0;                 // RowBatch拆分成的分区数量
    MutTableKey _last_partition_key;            // 上一个RowBatch最后一个分区的分区键
    bool _child_eos = false;
};

struct FrameBound {
    pb::BoundType bound_type;
    bool is_unbounded = false;
    int32_t num = -1;
};

class WindowProcessor {
public:
    WindowProcessor() {
    }
    virtual ~WindowProcessor() {
        for (auto* expr : _slot_order_exprs) {
            ExprNode::destroy_tree(expr);
        }
    }
    std::vector<WindowFnCall*>* mutable_window_fn_calls() {
        return &_window_fn_calls;
    }
    std::vector<ExprNode*>* mutable_slot_order_exprs() {
        return &_slot_order_exprs;
    }

    virtual int init(const pb::WindowNode& window_node);
    virtual int open();
    virtual int expr_optimize(QueryContext* ctx);
    virtual void find_place_holder(std::unordered_multimap<int, ExprNode*>& placeholders);
    virtual void close();
    virtual int process_one_partition(RowBatch* batch) = 0;
    void set_need_parse_frame(bool need_parse_frame) {
        _need_parse_frame = need_parse_frame;
    }
    void set_limit(int64_t limit) {
        _limit = limit;
    }
    bool reached_limit() {
        return _limit != -1 && _num_rows_returned >= _limit;
    }

protected:
    std::vector<WindowFnCall*> _window_fn_calls;
    std::vector<ExprNode*> _slot_order_exprs;
    std::vector<bool> _is_asc;
    std::vector<bool> _is_null_first;
    std::shared_ptr<MemRowCompare> _mem_row_compare;
    FrameBound _frame_start;
    FrameBound _frame_end;
    bool _need_parse_frame = false;
    int64_t _limit = -1;
    int64_t _num_rows_returned = 0;
};

// 整个分区作为Frame
class NonFrameWindowProcessor : public WindowProcessor {
public:
    NonFrameWindowProcessor() {
    }
    virtual ~NonFrameWindowProcessor() {
    }
    virtual int process_one_partition(RowBatch* batch) override;
};

class FrameWindowProcessor : public WindowProcessor {
public:
    FrameWindowProcessor() {
    }
    virtual ~FrameWindowProcessor() {
    }
    virtual int process_one_partition(RowBatch* batch) override;

protected:
    // 左闭右开
    virtual int get_start_row(const int32_t cur_row, RowBatch* batch) = 0;
    virtual int get_end_row(const int32_t cur_row, RowBatch* batch) = 0;
};

// Rows Frame模式
class RowFrameWindowProcessor : public FrameWindowProcessor {
public:
    RowFrameWindowProcessor() {
    }
    virtual ~RowFrameWindowProcessor() {
    }

protected:
    // 左闭右开
    int get_start_row(const int32_t cur_row, RowBatch* batch) override;
    int get_end_row(const int32_t cur_row, RowBatch* batch) override;
};

// Range Frame模式
class RangeFrameWindowProcessor : public FrameWindowProcessor {
public:
    RangeFrameWindowProcessor() {
    }
    virtual ~RangeFrameWindowProcessor() {
    }

protected:
    // 左闭右开
    int get_start_row(const int32_t cur_row, RowBatch* batch) override;
    int get_end_row(const int32_t cur_row, RowBatch* batch) override;
};

} // namespace baikaldb