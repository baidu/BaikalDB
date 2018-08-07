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

#include "exec_node.h"
#include "agg_node.h"
#include "filter_node.h"
#include "fetcher_node.h"
#include "insert_node.h"
#include "update_node.h"
#include "delete_node.h"
#include "join_node.h"
#include "scan_node.h"
#include "sort_node.h"
#include "packet_node.h"
#include "limit_node.h"
#include "truncate_node.h"
#include "transaction_node.h"
#include "runtime_state.h"

namespace baikaldb {
int ExecNode::init(const pb::PlanNode& node) {
    _pb_node = node;
    //_id = node.node_id();
    _limit = node.limit();
    _num_rows_returned = 0;
    _node_type = node.node_type();
    return 0;
}

int ExecNode::expr_optimize(std::vector<pb::TupleDescriptor>* tuple_descs) {
    if (_limit == 0) {
        return -2;
    }
    for (auto c : _children) {
        int ret = c->expr_optimize(tuple_descs);
        if (ret < 0) {
            return ret;
        }
    }
    return 0;
}

void ExecNode::get_node(pb::PlanNodeType node_type, std::vector<ExecNode*>& exec_nodes) {
    if (_node_type == node_type) {
        exec_nodes.push_back(this);
    } 
    for (auto c : _children) {
        c->get_node(node_type, exec_nodes);
    }
}

ExecNode* ExecNode::get_node(pb::PlanNodeType node_type) {
    if (_node_type == node_type) {
        return this;
    } else {
        for (auto c : _children) {
            ExecNode* node = c->get_node(node_type);
            if (node != nullptr) {
                return node;
            }
        }
        return nullptr;
    }
}

bool ExecNode::need_seperate() {
    switch (_node_type) {
        case pb::SCAN_NODE:
        case pb::INSERT_NODE:
        case pb::UPDATE_NODE:
        case pb::DELETE_NODE:
        case pb::REPLACE_NODE:
        case pb::TRUNCATE_NODE:
        case pb::TRANSACTION_NODE:
            return true;
        default:
            break;
    }
    for (auto c : _children) {
        if (c->need_seperate()) {
            return true;
        }
    }
    return false;
}

int ExecNode::open(RuntimeState* state) {
    int num_affected_rows = 0;
    for (auto c : _children) {
        int ret = 0;
        ret = c->open(state);
        if (ret < 0) {
            return ret;
        }
        num_affected_rows += ret;
    }
    return num_affected_rows;
}

void ExecNode::transfer_pb(pb::PlanNode* pb_node) {
    _pb_node.set_node_type(_node_type);
    _pb_node.set_limit(_limit);
    _pb_node.set_num_children(_children.size());
    pb_node->CopyFrom(_pb_node);
    return;
}

void ExecNode::create_pb_plan(pb::Plan* plan, ExecNode* root) {
    pb::PlanNode* pb_node = plan->add_nodes();
    root->transfer_pb(pb_node);
    for (size_t i = 0; i < root->children_size(); i++) {
        create_pb_plan(plan, root->children(i));
    }
}

int ExecNode::create_tree(const pb::Plan& plan, ExecNode** root) {
    int ret = 0;
    int idx = 0;
    if (plan.nodes_size() == 0) {
        *root = nullptr;
        return 0;
    }
    ret = ExecNode::create_tree(plan, &idx, nullptr, root);
    if (ret < 0) {
        return -1;
    }
    return 0;
}

int ExecNode::create_tree(const pb::Plan& plan, int* idx, ExecNode* parent, ExecNode** root) {
    if (*idx >= plan.nodes_size()) {
        DB_FATAL("idx %d > size %d", *idx, plan.nodes_size());
        return -1;
    }
    int num_children = plan.nodes(*idx).num_children();
    ExecNode* exec_node = nullptr;
    int ret = 0;
    ret = create_exec_node(plan.nodes(*idx), &exec_node);
    if (ret < 0) {
        DB_FATAL("create_exec_node fail:%s", plan.nodes(*idx).DebugString().c_str());
        return ret;
    }
    if (parent != nullptr) {
        parent->add_child(exec_node);
    } else if (root != nullptr) { 
        *root = exec_node;
    } else {
        DB_FATAL("parent is null");
        return -1;
    }
    for (int i = 0; i < num_children; i++) {
        ++(*idx);
        ret = create_tree(plan, idx, exec_node, nullptr);
        if (ret < 0) {
            DB_FATAL("sub create_tree fail, idx:%d", *idx);
            return -1;
        }
    }
    return 0;
}

int ExecNode::create_exec_node(const pb::PlanNode& node, ExecNode** exec_node) {
    switch (node.node_type()) {
        case pb::SCAN_NODE:
            *exec_node = new ScanNode;
            return (*exec_node)->init(node);
        case pb::SORT_NODE:
            *exec_node = new SortNode;
            return (*exec_node)->init(node);
        case pb::AGG_NODE:
        case pb::MERGE_AGG_NODE:
            *exec_node = new AggNode;
            return (*exec_node)->init(node);
        case pb::TABLE_FILTER_NODE:
        case pb::WHERE_FILTER_NODE:
        case pb::HAVING_FILTER_NODE:
            *exec_node = new FilterNode;
            return (*exec_node)->init(node);
        case pb::FETCHER_NODE:
            *exec_node = new FetcherNode;
            return (*exec_node)->init(node);
        case pb::UPDATE_NODE:
            *exec_node = new UpdateNode;
            return (*exec_node)->init(node);
        case pb::INSERT_NODE:
            *exec_node = new InsertNode;
            return (*exec_node)->init(node);
        case pb::REPLACE_NODE:
            *exec_node = new InsertNode;
            return (*exec_node)->init(node);
        case pb::DELETE_NODE:
            *exec_node = new DeleteNode;
            return (*exec_node)->init(node);
        case pb::PACKET_NODE:
            *exec_node = new PacketNode;
            return (*exec_node)->init(node);
        case pb::LIMIT_NODE:
            *exec_node = new LimitNode;
            return (*exec_node)->init(node);
        case pb::TRUNCATE_NODE:
            *exec_node = new TruncateNode;
            return (*exec_node)->init(node);
        case pb::TRANSACTION_NODE:
            *exec_node = new TransactionNode;
            return (*exec_node)->init(node);
        case pb::JOIN_NODE:
            *exec_node = new JoinNode;
            return (*exec_node)->init(node);
        default:
            DB_FATAL("create_exec_node failed: %s", node.DebugString().c_str());
            return -1;
    }
    return -1;
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
