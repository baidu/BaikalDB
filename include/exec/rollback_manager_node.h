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

// Brief:  truncate table exec node
#pragma once

#include "transaction_manager_node.h"
namespace baikaldb {
class RollbackManagerNode : public TransactionManagerNode {
public:
    RollbackManagerNode() {
    }
    virtual ~RollbackManagerNode() {
    }
    virtual int init(const pb::PlanNode& node) {
        int ret = 0;
        ret = ExecNode::init(node);
        if (ret < 0) {
            DB_WARNING("ExecNode::init fail, ret:%d", ret);
            return ret;
        }
        _txn_cmd = node.derive_node().transaction_node().txn_cmd();
        return 0;
    }
    virtual int open(RuntimeState* state) {
        auto client_conn = state->client_conn();
        client_conn->seq_id++;
        int ret = exec_rollback_node(state, _children[0]);
        if (ret < 0) {
            // un-expected case since infinite retry of commit after prepare
            DB_WARNING("TransactionError: rollback failed. txn_id: %lu log_id:%lu",
                    state->txn_id, state->log_id());
            client_conn->on_commit_rollback();
            return -1;
        }
        uint64_t old_txn_id = client_conn->txn_id;
        client_conn->on_commit_rollback();

        // start the new txn
        if (_txn_cmd == pb::TXN_ROLLBACK_BEGIN) {
            client_conn->on_begin();
            state->txn_id = client_conn->txn_id;
            client_conn->seq_id = 1;
            state->seq_id = 1;
            //DB_WARNING("client txn_id:%lu new_txn_id: %lu, %d log_id:%lu",
            //        old_txn_id, client_conn->txn_id, state->client_conn()->seq_id, state->log_id());
            ret = exec_begin_node(state,  _children[1]);
            _children.pop_back();
            if (ret < 0) {
                DB_WARNING("begin new txn failed after rollback, txn_id: %lu, new_txn_id: %lu log_id:%lu",
                    old_txn_id, client_conn->txn_id, state->log_id());
                return -1;
            }
        }
        return 0;
    }
    pb::TxnCmdType txn_cmd() {
        return _txn_cmd;
    }
private:
    pb::TxnCmdType     _txn_cmd = pb::TXN_INVALID;
};

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
