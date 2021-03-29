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
// Brief:  update table exec node

#pragma once

#include "exec_node.h"
#include "table_record.h"
#include "sorter.h"
#include "mem_row_compare.h"
#include "fetcher_store.h"

namespace baikaldb {

class UnionNode : public ExecNode {
public:
    UnionNode() {}
    virtual ~UnionNode() {
        for (auto expr : _slot_order_exprs) {
            ExprNode::destroy_tree(expr);
        }
        for (auto exprs : _select_projections) {
            for (auto expr : exprs) {
                ExprNode::destroy_tree(expr);
            }
        }
    }

    virtual int init(const pb::PlanNode& node) override;
    virtual int open(RuntimeState* state) override;
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos);
    virtual void close(RuntimeState* state) {
        ExecNode::close(state);
        for (auto expr : _slot_order_exprs) {
            expr->close();
        }
        for (auto exprs : _select_projections) {
            for (auto expr : exprs) {
                expr->close();
            }
        }
        _sorter = nullptr;
    }
    
    std::vector<RuntimeState*>* mutable_select_runtime_states() {
        return &_select_runtime_states;
    }
    void steal_projections(std::vector<ExprNode*>& projections) {
        std::vector<ExprNode*> tmp;
        tmp.swap(projections);
        _select_projections.push_back(tmp);
    }
    int32_t union_tuple_id() const {
        return _union_tuple_id;
    }
private:
    std::vector<ExprNode*> _slot_order_exprs;
    std::vector<bool> _is_asc;
    std::vector<bool> _is_null_first;
    int32_t           _union_tuple_id = -1;
    MemRowDescriptor* _mem_row_desc = nullptr;
    pb::TupleDescriptor* _tuple_desc = nullptr;
    std::shared_ptr<Sorter> _sorter;
    std::shared_ptr<MemRowCompare> _mem_row_compare;
    std::vector<RuntimeState*> _select_runtime_states;
    std::vector<std::vector<ExprNode*>>  _select_projections;
};
}
/* vim: set ts=4 sw=4 sts=4 tw=100 */