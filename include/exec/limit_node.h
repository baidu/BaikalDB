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

namespace baikaldb {
class LimitNode : public ExecNode {
public:
    LimitNode() : _offset(0), _num_rows_skipped(0) {}
    virtual ~LimitNode() {
        ExprNode::destroy_tree(_offset_expr);
        ExprNode::destroy_tree(_count_expr);
    }
    virtual int init(const pb::PlanNode& node);
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos);
    virtual void close(RuntimeState* state) {
        ExecNode::close(state);
        _num_rows_skipped = 0;
    }
    virtual int expr_optimize(QueryContext* ctx);
    virtual void find_place_holder(std::map<int, ExprNode*>& placeholders);
    virtual void transfer_pb(int64_t region_id, pb::PlanNode* pb_node);

    int64_t other_limit() {
        return _offset + _limit;
    }
    int64_t get_offset() {
        return _offset;
    }
    int64_t get_num_rows_skipped() {
        return _num_rows_skipped;
    }
    void add_num_rows_skipped(int64_t num) {
        _num_rows_skipped += num;
    }
    int64_t get_limit() {
	return _limit;
    }
private:
    int64_t _offset;
    int64_t _num_rows_skipped;

    ExprNode*   _offset_expr = nullptr;
    ExprNode*   _count_expr  = nullptr;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
