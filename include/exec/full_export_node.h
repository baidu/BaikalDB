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
#include "fetcher_store.h"

namespace baikaldb {
class FullExportNode : public ExecNode {
public:
    FullExportNode() {
    }
    virtual ~FullExportNode() {
    }

    virtual int init(const pb::PlanNode& node);
    virtual int open(RuntimeState* state);
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos);
    virtual void close(RuntimeState* state) {
        ExecNode::close(state);
        _send_region_ids.clear();
        _sent_region_ids.clear();
        _start_key_sort.clear();
        _error = E_OK;
    }
    bool get_batch(RowBatch* batch);

private:
    FetcherStore _fetcher_store;
    std::vector<int64_t> _send_region_ids;
    std::vector<int64_t> _sent_region_ids;
    std::map<std::string, int64_t> _start_key_sort;
    ErrorType _error = E_OK;
    pb::OpType _op_type;
};

} // namespace baikaldb

/* vim: set ts=4 sw=4 sts=4 tw=100 */
