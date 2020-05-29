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

#include "table_record.h"
#include "schema_factory.h"
#include "runtime_state.h"
#include "exec_node.h"
#include "proto/store.interface.pb.h"

namespace baikaldb {
enum ErrorType {
    E_OK = 0,
    E_WARNING,
    E_FATAL,
    E_BIG_SQL,
    E_RETURN    // primary region已经rollback是使用
};

struct TraceDesc {
    int64_t region_id;
    std::shared_ptr<pb::TraceNode> trace_node = nullptr;
};

class FetcherStore {
public:
    FetcherStore() {
        bthread_mutex_init(&region_lock, NULL);
    }
    virtual ~FetcherStore() {
        bthread_mutex_destroy(&region_lock);
    }
    
    void clear() {
        region_batch.clear();
        index_records.clear();
        start_key_sort.clear();
        error = E_OK;
        skip_region_set.clear();
        affected_rows = 0;
        scan_rows = 0;
        filter_rows = 0;
        row_cnt = 0;
    }

    // send (cached) cmds with seq_id >= start_seq_id
    ErrorType send_request(RuntimeState* state,
                            ExecNode* store_request, 
                            pb::RegionInfo& info, 
                            pb::TraceNode* trace_node,
                            int64_t old_region_id, 
                            int64_t region_id, 
                            uint64_t log_id, 
                            int retry_times, 
                            int start_seq_id,
                            int current_seq_id,
                            pb::OpType op_type);
    ErrorType send_request(RuntimeState* state,
                           ExecNode* store_request, 
                           pb::RegionInfo& info, 
                           int64_t old_region_id, 
                           int64_t region_id, 
                           uint64_t log_id, 
                           int retry_times, 
                           int start_seq_id,
                           int current_seq_id,
                           pb::OpType op_type) {
        return send_request(state, store_request, info, nullptr, old_region_id, region_id,
                     log_id, retry_times, start_seq_id, current_seq_id, op_type);
    }

    int run(RuntimeState* state, 
            std::map<int64_t, pb::RegionInfo>& region_infos,
            ExecNode* store_request,
            int start_seq_id,
            int current_seq_id,
            pb::OpType op_type);
    int run(RuntimeState* state,
            std::map<int64_t, pb::RegionInfo>& region_infos,
            ExecNode* store_request,
            int start_seq_id,
            pb::OpType op_type) {
        return run(state, region_infos, store_request, start_seq_id, start_seq_id, op_type);
    }
    void choose_opt_instance(pb::RegionInfo& info, std::string& addr);
public:
    std::map<int64_t, std::shared_ptr<RowBatch>> region_batch;
    std::map<int64_t, std::vector<SmartRecord>>  index_records; //key: index_id

    std::map<std::string, int64_t> start_key_sort;
    bthread_mutex_t region_lock;
    std::set<int64_t> skip_region_set;
    ErrorType error = E_OK;
    // 因为split会导致多region出来,加锁保护公共资源
    int64_t row_cnt = 0;
    std::atomic<int> affected_rows;
    std::atomic<int> scan_rows;
    std::atomic<int> filter_rows;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
