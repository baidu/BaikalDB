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

#include <vector>
#include "table_record.h"
#include "expr_node.h"
#include "row_batch.h"
#include "proto/plan.pb.h"
#include "proto/meta.interface.pb.h"
#include "mem_row_descriptor.h"
#include "runtime_state.h"

namespace baikaldb { 

class RuntimeState;
class QueryContext;
class ExecNode {
public:
    ExecNode() {
    }
    virtual ~ExecNode() {
        for (auto& e : _children) {
            SAFE_DELETE(e);
        }
    }
    virtual int init(const pb::PlanNode& node);
    /* ret: 0 - SUCCESS
     *     -1 - ERROR
     *     -2 - EMPTY_RESULT
     */
    virtual int expr_optimize(QueryContext* ctx);
    int common_expr_optimize(std::vector<ExprNode*>* exprs);

    //input: 需要下推的条件
    //input_exprs 既是输入参数，也是输出参数
    //output:能推的条件尽量下推，不能推的条件做一个filter node, 连接到节点的上边
    virtual int predicate_pushdown(std::vector<ExprNode*>& input_exprs);

    virtual void remove_additional_predicate(std::vector<ExprNode*>& input_exprs);
    void add_filter_node(const std::vector<ExprNode*>& input_exprs);
    
    void get_node(const pb::PlanNodeType node_type, std::vector<ExecNode*>& exec_nodes);
    ExecNode* get_node(const pb::PlanNodeType node_type);
    ExecNode* get_parent() {
        return _parent;
    }
    ExecNode* get_parent_node(const pb::PlanNodeType node_type) {
        if (_node_type == node_type) {
            return this;
        }

        ExecNode* parent = _parent;
        while (parent != nullptr && parent != this) {
            if (parent->_node_type == node_type) {
                return parent;
            } else {
                parent = parent->_parent;
            }
        }
        return nullptr;
    }
    void join_get_scan_nodes(const pb::PlanNodeType node_type, std::vector<ExecNode*>& exec_nodes);
    bool need_seperate();
    virtual int open(RuntimeState* state);
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
        *eos = true;
        return 0;
    }
    virtual void close(RuntimeState* state) {
        _num_rows_returned = 0;
        _return_empty = false;
        for (auto e : _children) {
            e->close(state);
        }
    }

    virtual void reset(RuntimeState* state) {
        _num_rows_returned = 0;
        _return_empty = false;
        for (auto e : _children) {
            e->reset(state);
        }
    }
    virtual std::vector<ExprNode*>* mutable_conjuncts() {
        return NULL;
    }
    virtual void find_place_holder(std::map<int, ExprNode*>& placeholders) {
        for (size_t idx = 0; idx < _children.size(); ++idx) {
            _children[idx]->find_place_holder(placeholders);
        }
    }
    virtual void replace_slot_ref_to_literal(const std::set<int64_t>& sign_set, std::map<int64_t, std::vector<ExprNode*>>& literal_maps) {
        for (auto child : _children) {
            if (child->node_type() == pb::JOIN_NODE || child->node_type() == pb::APPLY_NODE) {
                continue;
            }
            child->replace_slot_ref_to_literal(sign_set, literal_maps);
        }
    }
    virtual void show_explain(std::vector<std::map<std::string, std::string>>& output) {
        for (auto child : _children) {
            child->show_explain(output);
        }
    }

    virtual bool check_satisfy_condition(MemRow* row) {
        for (auto child : _children) {
            if (!child->check_satisfy_condition(row)) {
                return false;
            }
        }
        return true;
    }
    ExecNode* get_specified_node(const pb::PlanNodeType node_type) {
        if (node_type == _node_type) {
            return this;
        }
        for (auto child : _children) {
            if (child->node_type() == pb::JOIN_NODE || child->node_type() == pb::APPLY_NODE) {
                return nullptr;
            }
            ExecNode* exec_node = child->get_specified_node(node_type);
            if (exec_node != nullptr) {
                return exec_node;
            }
        }
        return nullptr;
    }
    void set_parent(ExecNode* parent_node) {
        _parent = parent_node;
    }
    void create_trace();
    void set_trace(pb::TraceNode* trace) {
        _trace = trace;
    }
    pb::TraceNode* get_trace() {
        return _trace;
    }
    void add_child(ExecNode* exec_node) {
        if (exec_node == nullptr) {
            return ;
        }
        _children.push_back(exec_node);
        exec_node->set_parent(this);
    }
    void add_child(ExecNode* exec_node, size_t idx) {
        if (exec_node == nullptr || idx > _children.size()) {
            return ;
        }
        _children.insert(_children.begin() + idx, exec_node);
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
        if (_children.size() == 0) {
            return nullptr;
        }
        return _children[idx];
    }
    std::vector<ExecNode*> children() {
        return _children;
    }
    std::vector<ExecNode*>* mutable_children() {
        return &_children;
    }
    void set_return_empty() {
        _return_empty = true;
        for (auto child : _children) {
            child->set_return_empty();
        }
    }
    bool reached_limit() {
        return _limit != -1 && _num_rows_returned >= _limit;
    }
    void set_limit(int64_t limit) {
        _limit = limit;
    }
    virtual void reset_limit(int64_t limit) {
        _limit = limit;
        for (auto child : _children) {
            child->reset_limit(limit);
        }
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
    bool is_filter_node() {
        return _node_type == pb::TABLE_FILTER_NODE ||
            _node_type == pb::WHERE_FILTER_NODE ||
            _node_type == pb::HAVING_FILTER_NODE;
    }
    std::map<int64_t, pb::RegionInfo>& region_infos() {
        return _region_infos;
    }
    void set_region_infos(std::map<int64_t, pb::RegionInfo> region_infos) {
        _region_infos.swap(region_infos);
    }
    //除了表达式外大部分直接沿用保存的pb
    virtual void transfer_pb(int64_t region_id, pb::PlanNode* pb_node);
    static void create_pb_plan(int64_t region_id, pb::Plan* plan, ExecNode* root);
    static int create_tree(const pb::Plan& plan, ExecNode** root);
    static void destroy_tree(ExecNode* root) {
        delete root;
    }
    virtual int push_cmd_to_cache(RuntimeState* state,
                                  pb::OpType op_type,
                                  ExecNode* store_request,
                                  int seq_id);
    virtual int push_cmd_to_cache(RuntimeState* state,
                                  pb::OpType op_type,
                                  ExecNode* store_request);
    std::map<int64_t, std::vector<SmartRecord>>& get_return_records() {
        return _return_records;
    }
    std::map<int64_t, std::vector<SmartRecord>>& get_return_old_records() {
        return _return_old_records;
    }
    virtual pb::LockCmdType lock_type() { return pb::LOCK_INVALID; }
    bool local_index_binlog() const {
        return _local_index_binlog;
    }
protected:
    int64_t _limit = -1;
    int64_t _num_rows_returned = 0;
    bool _is_explain = false;
    bool _return_empty = false;
    pb::PlanNodeType _node_type;
    std::vector<ExecNode*> _children;
    ExecNode* _parent = nullptr;
    pb::PlanNode _pb_node;
    std::map<int64_t, pb::RegionInfo> _region_infos;
    pb::TraceNode* _trace = nullptr;
    bool  _local_index_binlog = false;
    
    //返回给baikaldb的结果
    std::map<int64_t, std::vector<SmartRecord>> _return_old_records;
    std::map<int64_t, std::vector<SmartRecord>> _return_records;
private:
    static int create_tree(const pb::Plan& plan, int* idx, ExecNode* parent, 
                           ExecNode** root);
    static int create_exec_node(const pb::PlanNode& node, ExecNode** exec_node);
};
typedef std::shared_ptr<pb::TraceNode> SmartTrace;
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
