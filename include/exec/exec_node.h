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

#include <vector>
#include "expr_node.h"
#include "row_batch.h"
#include "proto/plan.pb.h"
#include "mem_row_descriptor.h"

namespace baikaldb { 
class RuntimeState;
class ExecNode {
public:
    ExecNode() : _limit(-1), _num_rows_returned(0) {
    }
    virtual ~ExecNode() {
        for (auto& e : _children) {
            delete e;
            e = nullptr;
        }
    }
    virtual int init(const pb::PlanNode& node);
    /* ret: 0 - SUCCESS
     *     -1 - ERROR
     *     -2 - EMPTY_RESULT
     */
    virtual int expr_optimize(std::vector<pb::TupleDescriptor>* tuple_descs);
    virtual int predicate_pushdown() {
        //DB_WARNING("node:%ld is predicating pushdown", this);
        for (auto& e : _children) {
            if (0 != e->predicate_pushdown()) {
                DB_WARNING("child condition optimize fail");
                return -1;
            }
        }
        return 0;
    }
    //增加一个表达式，将该表达式一直下推到最底层节点，返回节点
    virtual int add_or_pushdown(ExprNode* expr, 
                                 ExecNode** exec_node) {
        for (auto& e : _children) {
            if (0 != e->add_or_pushdown(expr, exec_node)) {
                DB_WARNING("child add and predicate fail");
                return -1;
            }
        }
        return 0;
    }
    pb::PlanNodeType get_node_type() const {
        return _node_type;
    }
    void get_node(pb::PlanNodeType node_type, std::vector<ExecNode*>& exec_nodes);
    ExecNode* get_node(pb::PlanNodeType node_type);
    ExecNode* get_parent() {
        return _parent;
    }
    bool need_seperate();
    virtual int open(RuntimeState* state);
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
        return 0;
    }
    virtual void close(RuntimeState* state) {
        for (auto e : _children) {
            e->close(state);
        }
    }
    virtual std::vector<ExprNode*>* mutable_conjuncts() {
        return NULL;
    }
    void set_parent(ExecNode* parent_node) {
        _parent = parent_node;
    }
    void add_child(ExecNode* exec_node) {
        _children.push_back(exec_node);
        exec_node->set_parent(this);
    }
    void clear_children() {
        for (auto child : _children) {
            if (child->_parent == this) {
                child->_parent = NULL;
            }
        }
        _children.clear();
    }
    int replace_child(ExecNode* old_child, ExecNode* new_child) {
        for (auto& child : _children) {
            if (child == old_child) {
                new_child->set_parent(this);
                if (old_child->_parent == this) {
                    old_child->_parent = NULL;
                }
                child = new_child;
                return 0;
            }
        }
        return -1;
    }
    size_t children_size() {
        return _children.size();
    }
    ExecNode* children(size_t idx) {
        return _children[idx];
    }
    std::vector<ExecNode*> children() {
        return _children;
    }
    std::vector<ExecNode*>* mutable_children() {
        return &_children;
    }
    bool reached_limit() {
        return _limit != -1 && _num_rows_returned >= _limit;
    }
    void set_limit(int64_t limit) {
        _limit = limit;
    }
    int64_t get_limit() {
        return _limit;
    }
    pb::PlanNode* mutable_pb_node() {
        return &_pb_node;
    }
    const pb::PlanNode& pb_node() {
        return _pb_node;
    }
    pb::PlanNodeType node_type() {
        return _node_type;
    }

    //除了表达式外大部分直接沿用保存的pb
    virtual void transfer_pb(pb::PlanNode* pb_node);
    static void create_pb_plan(pb::Plan* plan, ExecNode* root);
    static int create_tree(const pb::Plan& plan, ExecNode** root);
    static void destory_tree(ExecNode* root) {
        delete root;
    }
    
protected:
    int64_t _limit;
    int64_t _num_rows_returned;
    pb::PlanNodeType _node_type;
    std::vector<ExecNode*> _children;
    ExecNode* _parent = nullptr;
    pb::PlanNode _pb_node;

private:
    static int create_tree(const pb::Plan& plan, int* idx, ExecNode* parent, ExecNode** root);
    static int create_exec_node(const pb::PlanNode& node, ExecNode** exec_node);
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
