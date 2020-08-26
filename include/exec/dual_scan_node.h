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

#include "exec_node.h"
#include "runtime_state.h"

namespace baikaldb {
class DualScanNode : public ExecNode {
public:
    DualScanNode() {
    }
    virtual ~DualScanNode() {
    }
    int init(const pb::PlanNode& node) {
        int ret = 0;
        ret = ExecNode::init(node);
        if (ret < 0) {
            DB_WARNING("ExecNode::init fail, ret:%d", ret);
            return ret;
        }
        _tuple_id = node.derive_node().scan_node().tuple_id();
        _table_id = node.derive_node().scan_node().table_id();
        _node_type = pb::DUAL_SCAN_NODE;
        return 0;
    }
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
        std::unique_ptr<MemRow> row = state->mem_row_desc()->fetch_mem_row();
        batch->move_row(std::move(row));
        ++_num_rows_returned;
        *eos = true;
        return 0;
    }
    int64_t table_id() const {
        return _table_id;
    }
    int32_t tuple_id() const {
        return _tuple_id;
    }
private:
    int32_t _tuple_id = 0;
    int64_t _table_id = 0;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
