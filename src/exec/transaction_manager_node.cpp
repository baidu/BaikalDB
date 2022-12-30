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

#include "runtime_state.h"
#include "transaction_manager_node.h"
#include "network_socket.h"
#include "network_server.h"

namespace baikaldb {
DEFINE_int32(wait_after_prepare_us, 0, "wait time after prepare(us)");

int TransactionManagerNode::exec_begin_node(RuntimeState* state, ExecNode* begin_node) {
    auto client_conn = state->client_conn();
    if (client_conn->txn_start_time == 0) {
        client_conn->txn_start_time = butil::gettimeofday_us();
    }
    return push_cmd_to_cache(state, pb::OP_BEGIN, begin_node);
}

int TransactionManagerNode::exec_prepared_node(RuntimeState* state, ExecNode* prepared_node, int start_seq_id) {
    uint64_t log_id = state->log_id();
    auto client_conn = state->client_conn();
    if (client_conn->region_infos.size() <= 1 && !state->open_binlog()) {
        state->set_optimize_1pc(true);
        DB_WARNING("enable optimize_1pc: txn_id: %lu, start_seq_id: %d seq_id: %d, log_id: %lu", 
                state->txn_id, start_seq_id, client_conn->seq_id, log_id);
    }
    return _fetcher_store.run(state, client_conn->region_infos, prepared_node, start_seq_id,
                   client_conn->seq_id, pb::OP_PREPARE); 
}

int TransactionManagerNode::exec_commit_node(RuntimeState* state, ExecNode* commit_node) {
    //DB_WARNING("TransactionNote: prepare success, txn_id: %lu log_id:%lu", state->txn_id, state->log_id());
    auto client_conn = state->client_conn();
    if (client_conn == nullptr) {
        DB_WARNING("connection is nullptr: %lu", state->txn_id);
        return -1;
    }
    if (FLAGS_wait_after_prepare_us != 0) {
        bthread_usleep(FLAGS_wait_after_prepare_us);
    }
    int seq_id = client_conn->seq_id;
    int ret = _fetcher_store.run(state, client_conn->region_infos, commit_node, seq_id, seq_id, pb::OP_COMMIT);
    if (ret < 0) {
        // un-expected case since infinite retry of commit after prepare
        DB_WARNING("TransactionError: commit failed. txn_id: %lu log_id:%lu ", state->txn_id, state->log_id());
        return -1;
    }
    return 0;
}
int TransactionManagerNode::exec_rollback_node(RuntimeState* state, ExecNode* rollback_node) {
    //DB_WARNING("rollback for single-sql trnsaction with optimize_1pc");
    auto client_conn = state->client_conn();
    if (client_conn == nullptr) {
        DB_WARNING("connection is nullptr: %lu", state->txn_id);
        return -1;
    }
    int seq_id = client_conn->seq_id;
    int ret = _fetcher_store.run(state, client_conn->region_infos, rollback_node, seq_id, seq_id, pb::OP_ROLLBACK);
    if (ret < 0) {
        // un-expected case since infinite retry of commit after prepare
        DB_WARNING("TransactionError: rollback failed. txn_id: %lu log_id:%lu",
                state->txn_id, state->log_id());
        return -1;
    }
    return 0;
}
}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
