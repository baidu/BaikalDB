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
class BeginManagerNode : public TransactionManagerNode {
public:
    BeginManagerNode() {
    }
    virtual ~BeginManagerNode() {
    }
    virtual int open(RuntimeState* state) {
        uint64_t log_id = state->log_id();
        int ret = 0;
        auto client_conn = state->client_conn();
        if (client_conn == nullptr) {
            DB_WARNING("connection is nullptr: %lu", state->txn_id);
            return -1;
        }
        //DB_WARNING("client_conn: %ld, seq_id: %d", client_conn, state->client_conn()->seq_id);
        ExecNode* begin_node = _children[0];
        client_conn->seq_id++;
        //DB_WARNING("client_conn: %ld, seq_id: %d", client_conn, state->client_conn()->seq_id);
        ret = exec_begin_node(state, begin_node);
        if (ret < 0) {
            DB_WARNING("exec begin node fail, log_id: %lu", log_id);
            return -1;
        }
        _children.clear();
        return 0;
    }
};

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
