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
    virtual ~FilterNode() {
        for (auto conjunct : _conjuncts) {
            ExprNode::destroy_tree(conjunct);
        }
    }
    virtual int init(const pb::PlanNode& node);

    virtual int expr_optimize(QueryContext* ctx);

    virtual int predicate_pushdown(std::vector<ExprNode*>& input_exprs);

    virtual std::vector<ExprNode*>* mutable_conjuncts() {
        return &_conjuncts;
    }

    void add_conjunct(ExprNode* conjunct) {
        _conjuncts.push_back(conjunct);
    }
    const std::vector<ExprNode*>& pruned_conjuncts() {
        return _pruned_conjuncts;
    }

    bool check_satisfy_condition(MemRow* row) override {
        if (!need_copy(row)) {
            return false;
        }
        for (auto e : _children) {
            if (!e->check_satisfy_condition(row)) {
                return false;
            }
        }
        return true;
    }

    virtual int open(RuntimeState* state);
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos);
    virtual void close(RuntimeState* state);
    virtual void transfer_pb(int64_t region_id, pb::PlanNode* pb_node);

    virtual void find_place_holder(std::unordered_multimap<int, ExprNode*>& placeholders) {
        ExecNode::find_place_holder(placeholders);
        for (auto& expr : _conjuncts) {
            expr->find_place_holder(placeholders);
        }
    }
    virtual void remove_additional_predicate(std::vector<ExprNode*>& input_exprs);
    virtual void replace_slot_ref_to_literal(const std::set<int64_t>& sign_set,
                    std::map<int64_t, std::vector<ExprNode*>>& literal_maps) {
        ExecNode::replace_slot_ref_to_literal(sign_set, literal_maps);
        for (auto& expr : _conjuncts) {
            expr->replace_slot_ref_to_literal(sign_set, literal_maps);
        }
    }
    void modifiy_pruned_conjuncts_by_index(std::vector<ExprNode*>& filter_condition) {
        _pruned_conjuncts.clear();
        _raw_filter_node.Clear();
        _filter_node.clear();
        _pruned_conjuncts.swap(filter_condition);
        if (!_pruned_conjuncts.empty()) {
            for (auto expr : _pruned_conjuncts) {
                ExprNode::create_pb_expr(_raw_filter_node.add_conjuncts(), expr);
            }
        }
        _raw_filter_node.SerializeToString(&_filter_node);
    }
    void modifiy_pruned_conjuncts_by_index_learner(std::vector<ExprNode*>& filter_condition) {
        _pruned_conjuncts_learner.clear();
        _pruned_conjuncts_learner.swap(filter_condition);
        if (!_pruned_conjuncts_learner.empty()) {
            for (auto expr : _pruned_conjuncts_learner) {
                ExprNode::create_pb_expr(_raw_filter_node.add_conjuncts_learner(), expr);
            }
        }
    }
    virtual void show_explain(std::vector<std::map<std::string, std::string>>& output);

    void reset(RuntimeState* state) override {
        _child_eos = false;
        _child_row_idx = 0;
        _raw_filter_node.Clear();
        _filter_node.clear();
        for (auto e : _children) {
            e->reset(state);
        }
    }

private:
    bool need_copy(MemRow* row);
private:
    std::vector<ExprNode*> _conjuncts;
    std::vector<ExprNode*> _pruned_conjuncts;
    std::vector<ExprNode*> _pruned_conjuncts_learner; // learner集群使用
    RowBatch _child_row_batch;
    size_t  _child_row_idx = 0;
    bool    _child_eos = false;
    pb::FilterNode _raw_filter_node;
    std::string    _filter_node;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
