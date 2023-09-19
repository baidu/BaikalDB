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

#include "exec_node.h"
#include "agg_node.h"
#include "filter_node.h"
#include "insert_node.h"
#include "update_node.h"
#include "delete_node.h"
#include "join_node.h"
#include "scan_node.h"
#include "dual_scan_node.h"
#include "rocksdb_scan_node.h"
#include "sort_node.h"
#include "packet_node.h"
#include "limit_node.h"
#include "truncate_node.h"
#include "kill_node.h"
#include "transaction_node.h"
#include "begin_manager_node.h"
#include "commit_manager_node.h"
#include "rollback_manager_node.h"
#include "lock_primary_node.h"
#include "lock_secondary_node.h"
#include "full_export_node.h"
#include "union_node.h"
#include "apply_node.h"
#include "load_node.h"
#include "runtime_state.h"

namespace baikaldb {

int ExecNode::init(const pb::PlanNode& node) {
    _pb_node = node;
    _limit = node.limit();
    _num_rows_returned = 0;
    _node_type = node.node_type();
    _is_explain = node.is_explain();
    _is_get_keypoint = node.is_get_keypoint();
    return 0;
}

int ExecNode::expr_optimize(QueryContext* ctx) {
    int ret = 0;
    for (auto c : _children) {
        int ret2 = c->expr_optimize(ctx);
        if (ret2 < 0) {
            ret = ret2;
        }
    }
    return ret;
}

int ExecNode::common_expr_optimize(std::vector<ExprNode*>* exprs) {
    int ret = 0;
    for (auto& expr : *exprs) {
        //类型推导
        ret = expr->expr_optimize();
        if (ret < 0) {
            DB_WARNING("type_inferer fail");
            return ret;
        }
        if (expr->is_row_expr()) {
            continue;
        }
        if (!expr->is_constant()) {
            continue;
        }
        if (expr->is_literal()) {
            continue;
        }
        // place holder被替换会导致下一次exec参数对不上
        // TODO 后续得考虑普通查询计划复用，表达式如何对上
        if (expr->has_place_holder()) {
            continue;
        }
        ret = expr->open();
        if (ret < 0) {
            return ret;
        }
        ExprValue value = expr->get_value(nullptr);
        expr->close();
        delete expr;
        expr = new Literal(value);
    }
    return ret;
}

int ExecNode::predicate_pushdown(std::vector<ExprNode*>& input_exprs) {
    if (_children.size() > 0) {
        _children[0]->predicate_pushdown(input_exprs); 
    }
    if (input_exprs.size() > 0) {
        add_filter_node(input_exprs);      
    }
    input_exprs.clear();
    return 0;
}

void ExecNode::remove_additional_predicate(std::vector<ExprNode*>& input_exprs) {
    for (auto c : _children) {
        c->remove_additional_predicate(input_exprs);
    }
}

void ExecNode::add_filter_node(const std::vector<ExprNode*>& input_exprs) {
    pb::PlanNode pb_plan_node;
    pb_plan_node.set_node_type(pb::TABLE_FILTER_NODE);
    pb_plan_node.set_num_children(1);
    pb_plan_node.set_is_explain(_is_explain);
    pb_plan_node.set_limit(-1);
    auto filter_node = new FilterNode();
    filter_node->init(pb_plan_node);
    _parent->replace_child(this, filter_node);
    filter_node->add_child(this);
    for (auto& expr : input_exprs) {
        filter_node->add_conjunct(expr);
    }
}

void ExecNode::get_node(const pb::PlanNodeType node_type, std::vector<ExecNode*>& exec_nodes) {
    if (_node_type == node_type) {
        exec_nodes.emplace_back(this);
    } 
    for (auto c : _children) {
        c->get_node(node_type, exec_nodes);
    }
}

void ExecNode::join_get_scan_nodes(const pb::PlanNodeType node_type, std::vector<ExecNode*>& exec_nodes) {
    if (_node_type == node_type) {
        exec_nodes.emplace_back(this);
    }
    for (auto c : _children) {
        if (c->node_type() == pb::JOIN_NODE || c->node_type() == pb::APPLY_NODE) {
            continue;
        }
        c->join_get_scan_nodes(node_type, exec_nodes);
    }
}

ExecNode* ExecNode::get_node(const pb::PlanNodeType node_type) {
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
        case pb::INSERT_NODE:
        case pb::UPDATE_NODE:
        case pb::DELETE_NODE:
        case pb::TRUNCATE_NODE:
        case pb::KILL_NODE:
        case pb::TRANSACTION_NODE:
        case pb::BEGIN_MANAGER_NODE:
        case pb::COMMIT_MANAGER_NODE:
        case pb::UNION_NODE:
        case pb::ROLLBACK_MANAGER_NODE:
            return true;
        case pb::SCAN_NODE:
            //DB_NOTICE("engine:%d", static_cast<ScanNode*>(this)->engine());
            if (static_cast<ScanNode*>(this)->engine() == pb::ROCKSDB) {
                return true;
            }
            if (static_cast<ScanNode*>(this)->engine() == pb::BINLOG) {
                return true;
            }
            if (static_cast<ScanNode*>(this)->engine() == pb::ROCKSDB_CSTORE) {
                return true;
            }
            break;
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
void ExecNode::create_trace() {
    if (_trace != nullptr) {
        for (auto c : _children) {
            if (c->get_trace() == nullptr) {
                pb::TraceNode* trace_node = _trace->add_child_nodes();
                trace_node->set_node_type(c->node_type());
                c->set_trace(trace_node);
            }
            c->create_trace();
        }
    }
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

void ExecNode::transfer_pb(int64_t region_id, pb::PlanNode* pb_node) {
    _pb_node.set_node_type(_node_type);
    _pb_node.set_limit(_limit);
    _pb_node.set_num_children(_children.size());
    pb_node->CopyFrom(_pb_node);
}

void ExecNode::create_pb_plan(int64_t region_id, pb::Plan* plan, ExecNode* root) {
    pb::PlanNode* pb_node = plan->add_nodes();
    root->transfer_pb(region_id, pb_node);
    for (size_t i = 0; i < root->children_size(); i++) {
        create_pb_plan(region_id, plan, root->children(i));
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

int ExecNode::create_tree(const pb::Plan& plan, int* idx, ExecNode* parent, 
                          ExecNode** root) {
    if (*idx >= plan.nodes_size()) {
        DB_FATAL("idx %d >= size %d", *idx, plan.nodes_size());
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
            *exec_node = ScanNode::create_scan_node(node);
            if (*exec_node == nullptr) {
                return -1;
            }
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
        case pb::UPDATE_NODE:
            *exec_node = new UpdateNode;
            return (*exec_node)->init(node);
        case pb::INSERT_NODE:
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
        case pb::KILL_NODE:
            *exec_node = new KillNode;
            return (*exec_node)->init(node);
        case pb::TRANSACTION_NODE:
            *exec_node = new TransactionNode;
            return (*exec_node)->init(node);
        case pb::BEGIN_MANAGER_NODE:
            *exec_node = new BeginManagerNode;
            return (*exec_node)->init(node);
        case pb::COMMIT_MANAGER_NODE:
            *exec_node = new CommitManagerNode;
            return (*exec_node)->init(node);
        case pb::ROLLBACK_MANAGER_NODE:
            *exec_node = new RollbackManagerNode;
            return (*exec_node)->init(node);
        case pb::JOIN_NODE:
            *exec_node = new JoinNode;
            return (*exec_node)->init(node);
        case pb::LOCK_PRIMARY_NODE:
            *exec_node = new LockPrimaryNode;
            return (*exec_node)->init(node);
        case pb::LOCK_SECONDARY_NODE:
            *exec_node = new LockSecondaryNode;
            return (*exec_node)->init(node);
        case pb::FULL_EXPORT_NODE:
            *exec_node = new FullExportNode;
            return (*exec_node)->init(node);
        case pb::DUAL_SCAN_NODE:
            *exec_node = new DualScanNode;
            return (*exec_node)->init(node);
        case pb::UNION_NODE:
            *exec_node = new UnionNode;
            return (*exec_node)->init(node);
        case pb::APPLY_NODE:
            *exec_node = new ApplyNode;
            return (*exec_node)->init(node);
        case pb::LOAD_NODE:
            *exec_node = new LoadNode;
            return (*exec_node)->init(node);
        default:
            DB_FATAL("create_exec_node failed: %s", node.DebugString().c_str());
            return -1;
    }
    return -1;
}
//>0代表放到cache里，==0代表不需要放到cache里, 单语句事务node与prepare一起发送
int ExecNode::push_cmd_to_cache(RuntimeState* state,
                                pb::OpType op_type,
                                ExecNode* store_request,
                                int seq_id) {
    DB_DEBUG("txn_id: %lu op_type: %s, seq_id: %d, exec_node:%p", 
        state->txn_id, pb::OpType_Name(op_type).c_str(), seq_id, store_request);
    if (state->txn_id == 0) {
        return 0;
    }
    auto client = state->client_conn();
    // cache dml cmd in baikaldb before sending to store
    if (!is_dml_op_type(op_type) && op_type != pb::OP_BEGIN) {
        return 0;
    }
    if (client->cache_plans.count(seq_id)) {
        DB_WARNING("seq_id duplicate seq_id:%d", seq_id);
    }
    CachePlan& plan_item = client->cache_plans[seq_id];
    plan_item.op_type = op_type;
    plan_item.sql_id = seq_id;
    plan_item.root = store_request;
    store_request->set_parent(nullptr);
    plan_item.tuple_descs = state->tuple_descs();
    return 1;
}

}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
