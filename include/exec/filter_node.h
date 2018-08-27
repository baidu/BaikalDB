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

#pragma once

#include "exec_node.h"

namespace baikaldb {
class FilterNode : public ExecNode {
public:
    FilterNode() : _child_row_idx(0), _child_eos(false) {
    }
    virtual  ~FilterNode() {
        for (auto conjunct : _conjuncts) {
            ExprNode::destory_tree(conjunct);
        }
    }
    virtual int init(const pb::PlanNode& node);

    virtual int expr_optimize(std::vector<pb::TupleDescriptor>* tuple_descs);

    virtual int predicate_pushdown(std::vector<ExprNode*>& input_exprs);

    virtual std::vector<ExprNode*>* mutable_conjuncts() {
        return &_conjuncts;
    }
    void add_conjunct(ExprNode* conjunct) {
        _conjuncts.push_back(conjunct);
    }
    virtual int open(RuntimeState* state);
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos);
    virtual void close(RuntimeState* state);
    virtual void transfer_pb(pb::PlanNode* pb_node);
private:
    bool need_copy(MemRow* row);

private:
    std::vector<ExprNode*> _conjuncts;
    std::vector<ExprNode*> _pruned_conjuncts;
    RowBatch _child_row_batch;
    size_t  _child_row_idx;
    bool    _child_eos;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
