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

// Brief:  the class for executing Transaction Control cmds
#pragma once

#include "exec_node.h"
#include "proto/plan.pb.h"

namespace baikaldb {
class TransactionNode : public ExecNode {
public:
    TransactionNode() {
    }
    virtual ~TransactionNode() {
    }
    virtual int init(const pb::PlanNode& node);
    virtual int open(RuntimeState* state);
    virtual void transfer_pb(int64_t region_id, pb::PlanNode* pb_node);

    pb::TxnCmdType txn_cmd() {
        return _txn_cmd;
    }

    void set_txn_cmd(pb::TxnCmdType cmd) {
        _txn_cmd = cmd;
    }

    void set_txn_timeout(int64_t timeout) {
        _txn_timeout = timeout;
    }

    void set_txn_lock_timeout(int64_t timeout) {
        _txn_lock_timeout = timeout;
    }

    int64_t get_txn_timeout() const {
        return _txn_timeout;
    }
private:
    pb::TxnCmdType     _txn_cmd = pb::TXN_INVALID;
    int64_t            _txn_timeout = 0;
    int64_t            _txn_lock_timeout = -1;
};
}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
