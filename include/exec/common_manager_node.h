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

#include "exec_node.h"
#include "fetcher_store.h"

namespace baikaldb {
class CommonManagerNode : public ExecNode {
public:
    CommonManagerNode() {
    }
    virtual ~CommonManagerNode() {
    }
    virtual int open(RuntimeState* state) {
        int ret = 0;
        auto client_conn = state->client_conn();
        if (client_conn == nullptr) {
            DB_WARNING("connection is nullptr: %lu, %d", state->txn_id, client_conn->seq_id);
           return -1; 
        }
        ExecNode* common_node = _children[0];
        ret = _fetcher_store.run(state, _region_infos, common_node, client_conn->seq_id, client_conn->seq_id, _op_type);
        if (ret < 0) {
            DB_WARNING("exec common node fail");
        }
        return ret;
    }
    void set_op_type(pb::OpType op_type) {
        _op_type = op_type;
    }
protected:
    pb::OpType _op_type = pb::OP_NONE;
    FetcherStore _fetcher_store;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
