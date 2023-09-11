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

#include "single_txn_manager_node.h"
#include "network_socket.h"

namespace baikaldb {
int SingleTxnManagerNode::open(RuntimeState* state) {
    uint64_t log_id = state->log_id();
    int ret = 0;
    int affected_rows = 0;
    bool has_global_index = false;
    auto client_conn = state->client_conn();
    if (client_conn == nullptr) {
        DB_WARNING("connection is nullptr: %lu, %d, log_id: %lu", 
                state->txn_id, state->client_conn()->seq_id, log_id);
        return -1;
    }
    ON_SCOPE_EXIT([client_conn]() {
        client_conn->on_commit_rollback();        
    });
    ExecNode* begin_node = _children[0];
    ExecNode* dml_manager_node = _children[1];
    ExecNode* prepared_node = _children[2];
    ExecNode* commit_node   = _children[3];
    ExecNode* rollback_node = _children[4];
    //begin请求只需要放入cache中
    client_conn->seq_id++;
    client_conn->primary_region_id = -1;
    ret = exec_begin_node(state, begin_node);
    if (ret < 0) {
        DB_WARNING("exec begin node fail, log_id: %lu, txn_id: %lu", log_id, state->txn_id);
        return -1;
    }
    _children.erase(_children.begin());
    //没有全局二级索引的情况下，dmlmanagerNode下的dmlNode直接cache,不执行
    if (dml_manager_node->children_size() <= 1 
        && !state->single_txn_need_separate_execute()
        && !state->open_binlog()) {
        client_conn->seq_id++;
        //dml请求放入cache, 同时更新client_conn上的region_info信息
        state->client_conn()->region_infos = dml_manager_node->region_infos();
        push_cmd_to_cache(state, _op_type, dml_manager_node->children(0), client_conn->seq_id);
        dml_manager_node->clear_children();
        state->set_single_txn_cached();
    } else {
        has_global_index = true;
        ret = dml_manager_node->open(state);
        affected_rows = ret;
    }
    if (ret < 0) {
        DB_WARNING_STATE(state, "TransactionNote: exec dml_node fail: log_id: %lu, txn_id: %lu, ",
            log_id, state->txn_id);
        client_conn->seq_id++;
        ret = exec_rollback_node(state, rollback_node);
        return -1;
    }
    //prepare指令执行真正的发送动作，将begin和dml作为缓存的cache发送到store上
    client_conn->seq_id++;
    if (!has_global_index) {
        ret = exec_prepared_node(state, prepared_node, 1);
        affected_rows = ret;
    } else {
        ret = exec_prepared_node(state, prepared_node, client_conn->seq_id);
    }
    if (ret < 0) {
        DB_WARNING("TransactionNote: exec prepare fail: log_id: %lu, txn_id: %lu, ",
            log_id, state->txn_id);
        client_conn->seq_id++;
        ret = exec_rollback_node(state, rollback_node);
        return -1;
    }
    //不是优化的1pc 需要无限次的rollback 或者 commit
    if (state->optimize_1pc() == false) {
        client_conn->seq_id++;
        ret = exec_commit_node(state, commit_node);
        if (ret < 0) {
            DB_WARNING("TransactionNote: commit failed: log_id: %lu, txn_id: %lu, ",
                log_id, state->txn_id);
            ret = exec_rollback_node(state, rollback_node);
            return -1;
        }
    } else {
        DB_WARNING("TransactionNote: optimize_1pc, no commit: log_id: %lu, txn_id: %lu, ",
                log_id, state->txn_id);
    }
    return affected_rows;
}

void SingleTxnManagerNode::reset(RuntimeState* state) {
    auto client_conn = state->client_conn();
    // add begin back
    CachePlan& begin_item = client_conn->cache_plans[1];
    this->add_child(begin_item.root, 0);
    client_conn->cache_plans.erase(1);
    auto iter = client_conn->cache_plans.find(2);
    if (iter != client_conn->cache_plans.end()) {
        ExecNode* dml_manager_node = _children[1];
        CachePlan& plan_item = iter->second;
        dml_manager_node->add_child(plan_item.root);
        client_conn->cache_plans.erase(2);
    }
    client_conn->on_begin();
    state->txn_id = client_conn->txn_id;
    state->seq_id = 1;
    ExecNode::reset(state);
}

}
