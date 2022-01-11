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
#include "fetcher_store.h"
#include "proto/plan.pb.h"

namespace baikaldb {
class TransactionManagerNode : public ExecNode {
public:
    TransactionManagerNode() {
    }
    virtual ~TransactionManagerNode() {
    }
    void set_op_type(pb::OpType op_type) {
        _op_type = op_type;
    }
    virtual int exec_begin_node(RuntimeState* state, ExecNode* begin_node);
    virtual int exec_prepared_node(RuntimeState* state, ExecNode* prepared_node, int start_seq_id);
    virtual int exec_commit_node(RuntimeState* state, ExecNode* commit_node);
    virtual int exec_rollback_node(RuntimeState* state, ExecNode* rollback_node);

protected:
    FetcherStore _fetcher_store;
    pb::OpType _op_type = pb::OP_NONE;
};
}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
