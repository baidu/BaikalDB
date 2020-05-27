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
class FilterNode : public ExecNode {
public:
    FilterNode() {
    }
    virtual  ~FilterNode() {
        for (auto conjunct : _conjuncts) {
            ExprNode::destroy_tree(conjunct);
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
    virtual void transfer_pb(int64_t region_id, pb::PlanNode* pb_node);

    virtual void find_place_holder(std::map<int, ExprNode*>& placeholders) {
        ExecNode::find_place_holder(placeholders);
        for (auto& expr : _conjuncts) {
            expr->find_place_holder(placeholders);
        }
    }
    void modifiy_pruned_conjuncts_by_index(const std::unordered_set<ExprNode*>& other_condition) {
        // 先清理，后续transfer pb会填充 _pruned_conjuncts
        mutable_pb_node()->mutable_derive_node()->mutable_filter_node()->clear_conjuncts();
        _pruned_conjuncts.clear();
        _pruned_conjuncts.insert(_pruned_conjuncts.end(), other_condition.begin(), other_condition.end());
    }
    virtual void show_explain(std::vector<std::map<std::string, std::string>>& output);
private:
    bool need_copy(MemRow* row);

private:
    std::vector<ExprNode*> _conjuncts;
    std::vector<ExprNode*> _pruned_conjuncts;
    RowBatch _child_row_batch;
    size_t  _child_row_idx = 0;
    bool    _child_eos = false;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
