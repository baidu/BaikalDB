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
#include "table_record.h"
#include "proto/store.interface.pb.h"
#include "sorter.h"
#include "mem_row_compare.h"

namespace baikaldb {
class FetcherNode : public ExecNode {
public:
enum ErrorType {
    E_OK = 0,
    E_WARNING,
    E_FATAL,
    E_BIG_SQL
};
    FetcherNode() {
        bthread_mutex_init(&_region_lock, NULL);
    }
    virtual ~FetcherNode() {
        bthread_mutex_destroy(&_region_lock);
        for (auto expr : _slot_order_exprs) {
            ExprNode::destroy_tree(expr);
        }
    }

    // send (cached) cmds with seq_id >= start_seq_id
    ErrorType send_request(RuntimeState* state, pb::RegionInfo& info, 
        std::vector<SmartRecord>* records, int64_t old_region_id, 
        int64_t region_id, uint64_t log_id, int retry_times, int start_seq_id);

    virtual int init(const pb::PlanNode& node); 
    virtual int open(RuntimeState* state);
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos);
    virtual void close(RuntimeState* state) {
        //ExecNode::close(state);
        for (auto expr : _slot_order_exprs) {
            expr->close();
        }
    }
    void choose_opt_instance(pb::RegionInfo& info, std::string& addr);

protected:
    std::map<int64_t, std::shared_ptr<RowBatch>> _region_batch;
    std::map<std::string, int64_t> _start_key_sort;
    bthread_mutex_t _region_lock;
    ErrorType _error = E_OK;
    pb::OpType _op_type;

private:
    int push_cache(RuntimeState* state);
    //允许fetcher回来后排序
    std::vector<ExprNode*> _slot_order_exprs;
    std::vector<bool> _is_asc;
    std::vector<bool> _is_null_first;
    std::shared_ptr<MemRowCompare> _mem_row_compare;
    std::shared_ptr<Sorter> _sorter;
    std::atomic<int> _affected_rows;
    // 因为split会导致多region出来,加锁保护公共资源
    int64_t _row_cnt = 0;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
