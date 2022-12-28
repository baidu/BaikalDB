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
#include "rocksdb_scan_node.h"

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
        _last_router_key.clear();
        _start_key_sort.clear();
        _error = E_OK;
    }
    bool get_batch(RowBatch* batch);
    // fullexport分批获取region
    int get_next_region_infos();
    // 达到limit后，记录最后一条数据，下次查询可以接着上次查询
    // 用于inner join后数据变少，可以再次获取数据
    int calc_last_key(RuntimeState* state, MemRow* mem_row);
    // 达到limit后，重置_num_rows_returned可以做下一轮请求
    void reset_num_rows_returned() {
        _num_rows_returned = 0;
    }

private:
    FetcherStore _fetcher_store;
    std::deque<int64_t> _send_region_ids;
    // <partition_id, <start_key,region_id>>
    std::map<int64_t, std::map<std::string, int64_t>> _start_key_sort;
    RocksdbScanNode* _scan_node = nullptr;
    std::string _last_router_key;
    ErrorType _error = E_OK;
    pb::OpType _op_type;
    bool _no_regions = false;
};

} // namespace baikaldb

/* vim: set ts=4 sw=4 sts=4 tw=100 */
