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

#include "query_context.h"
#include "row_batch.h"

namespace baikaldb {

struct WindowCountIntermediate {
    int64_t cnt = 0;
};

struct WindowSumIntermediate {
    ExprValue sum;
};

struct WindowAvgIntermediate {
    double sum = 0;
    int64_t cnt = 0;
};

struct WindowMinMaxIntermediate {
    ExprValue min_max;
};

struct WindowRowNumberIntermediate {
    int64_t row_number = 0;
};

struct WindowRankIntermediate {
    int64_t cur_idx = 0;
    int64_t last_rank = 0;
    RowBatch* cur_batch = nullptr;
    std::shared_ptr<MemRowCompare> mem_row_compare;
};

struct WindowNtileIntermediate {
    int64_t cur_idx = 0;       // 当前桶元素数量
    int64_t cur_group_idx = 1; // 当前桶的序号
    int64_t n = 0;             // 分桶数
    int64_t quotient = 0;      // 商
    int64_t remainder = 0;     // 余数
};

struct WindowLeadLagIntermediate {
    int64_t offset = 1;
    ExprNode* default_expr = nullptr;
    int64_t cur_idx = 0;
    RowBatch* cur_batch = nullptr;
};

struct WindowValueIntermediate {
    int64_t n = 0;
    ExprValue value;
};

class WindowFnCall : public ExprNode {
public:
    enum WindowType {
        COUNT_STAR,
        COUNT,
        SUM,
        AVG,
        MIN,
        MAX,
        ROW_NUMBER,
        RANK,
        DENSE_RANK,
        PERCENT_RANK,
        CUME_DIST,
        NTILE,
        LEAD,
        LAG,
        FIRST_VALUE,
        LAST_VALUE,
        NTH_VALUE
    };

    WindowFnCall() {
    }
    ~WindowFnCall() {
    }

    virtual int init(const pb::ExprNode& node) override;
    virtual int open() override;
    virtual void close() override;
    virtual int type_inferer() override;
    int type_inferer(QueryContext* ctx);

    int open(std::shared_ptr<MemRowCompare> mem_row_compare) {
        _mem_row_compare = mem_row_compare;
        return open();
    }
    virtual bool can_use_arrow_vector() override {
        return false;
    }
    virtual ExprValue get_value(MemRow* row) override {
        if (row == nullptr) {
            return ExprValue::Null();
        }
        return row->get_value(_tuple_id, _slot_id);
    }
    // @brief 在当前窗口调用函数
    int call(RowBatch* batch, const int start, const int end);
    // @brief 获取当前窗口的计算值
    int get_result(ExprValue& expr_value);
    // @brief frame级重置成员变量
    int frame_reset();
    // @brief partition级重置成员变量
    int partition_reset();

private:
    int initialize();

private:
    WindowType _window_type;
    std::string _window_intermediate; // 窗口函数中间结果
    std::shared_ptr<MemRowCompare> _mem_row_compare;
};

} // namespace baikaldb