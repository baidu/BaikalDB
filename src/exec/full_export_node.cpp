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

#include "runtime_state.h"
#include "full_export_node.h"
#include "network_socket.h"

namespace baikaldb {

DECLARE_int32(retry_interval_us);
DEFINE_int32(region_per_batch, 4, "request region number in a batch");
DECLARE_int64(print_time_us);

int FullExportNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    _op_type = pb::OP_SELECT;
    return 0;
}

int FullExportNode::open(RuntimeState* state) {
    auto client_conn = state->client_conn();
    if (client_conn == nullptr) {
        DB_WARNING("connection is nullptr: %lu", state->txn_id);
        return -1;
    }
    client_conn->seq_id++;
    state->seq_id = client_conn->seq_id;
    for (auto& pair : _region_infos) {
        auto& info = pair.second;
        _start_key_sort[info.start_key()] = info.region_id();
    }
    for (auto& pair : _start_key_sort) {
        _send_region_ids.push_back(pair.second);
    } 
    DB_WARNING("region_count:%ld", _send_region_ids.size());
    return 0;
}

bool FullExportNode::get_batch(RowBatch* batch) {
    auto iter = _sent_region_ids.begin();
    if (iter != _sent_region_ids.end()) {
        auto iter2 = _fetcher_store.region_batch.find(*iter);
        _sent_region_ids.erase(iter);
        if (iter2 != _fetcher_store.region_batch.end()) {
            batch->swap(*iter2->second);
            _fetcher_store.region_batch.erase(iter2);
            return true;
        } else {
            return false;
        }
    } else {
        return false;
    }
}

int FullExportNode::get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
    if (state->is_cancelled()) {
        DB_WARNING_STATE(state, "cancelled");
        state->set_eos();
        *eos = true;
        return 0;
    }

    if (reached_limit()) {
        state->set_eos();
        *eos = true;
        return 0;
    }
    
    if (get_batch(batch)) {
        _num_rows_returned += batch->size();
        if (reached_limit()) {
            state->set_eos();
            *eos = true;
            _num_rows_returned = _limit;
            return 0;
        }
        return 0;
    }
    uint64_t log_id = state->log_id();

    if (_send_region_ids.size() == 0) {
        state->set_eos();
        DB_WARNING("process full select done log_id:%lu", log_id);
        *eos = true;
        return 0;
    }
    
    ConcurrencyBthread region_bth(FLAGS_region_per_batch, &BTHREAD_ATTR_SMALL);
    int region_per_batch = 0;
    _fetcher_store.scan_rows = 0;
    for (auto id_iter = _send_region_ids.begin(); id_iter != _send_region_ids.end();) {
        if (region_per_batch >= FLAGS_region_per_batch) {
            break;
        }
        pb::RegionInfo* info = nullptr;
        int64_t region_id = *id_iter;
        auto iter = _region_infos.find(region_id);
        if (iter != _region_infos.end()) {
            info = &iter->second;
        } else {
            id_iter = _send_region_ids.erase(id_iter);
            continue;
        }
        DB_WARNING("send region_id:%ld", region_id);
        region_per_batch++;
        auto region_thread = [this, state, region_id, info, log_id]() {
            auto ret = _fetcher_store.send_request(state, _children[0], *info, region_id, region_id, 
                log_id, 0, state->seq_id, state->seq_id, _op_type);
            if (ret != E_OK) {
                DB_WARNING("rpc error, region_id:%ld, log_id:%lu", region_id, log_id);
                _error = ret;
            }
        };
        region_bth.run(region_thread);
        id_iter = _send_region_ids.erase(id_iter);
        _sent_region_ids.push_back(region_id);
    }

    region_bth.join();

    if (_error != E_OK) {
        return -1;
    }
    state->set_num_scan_rows(state->num_scan_rows() + _fetcher_store.scan_rows.load());
    state->set_num_filter_rows(state->num_filter_rows() + _fetcher_store.filter_rows.load());
    return 0;
}
} 

/* vim: set ts=4 sw=4 sts=4 tw=100 */
