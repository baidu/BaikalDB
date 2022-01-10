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
#include "network_socket.h"
#include "proto/store.interface.pb.h"

namespace baikaldb {
enum ErrorType {
    E_OK = 0,
    E_WARNING,
    E_FATAL,
    E_BIG_SQL,
    E_RETURN
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
        split_region_batch.clear();
        index_records.clear();
        start_key_sort.clear();
        split_start_key_sort.clear();
        error = E_OK;
        skip_region_set.clear();
        affected_rows = 0;
        scan_rows = 0;
        filter_rows = 0;
        row_cnt = 0;
        analyze_fail_cnt = 0;
        used_bytes = 0;
        primary_timestamp_updated = false;
        no_copy_cache_plan_set.clear();
        dynamic_timeout_ms = -1;
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

    template<typename Repeated>
    void choose_opt_instance(int64_t region_id, Repeated&& peers, std::string& addr, std::string* backup) {
        SchemaFactory* schema_factory = SchemaFactory::get_instance();
        std::string baikaldb_logical_room = schema_factory->get_logical_room();
        if (baikaldb_logical_room.empty()) {
            return;
        }
        std::vector<std::string> candicate_peers;
        std::vector<std::string> normal_peers;
        bool addr_in_candicate = false;
        bool addr_in_normal = false;
        for (auto& peer: peers) {
            auto status = schema_factory->get_instance_status(peer);
            if (status.status != pb::NORMAL) {
                continue;
            } else if (!status.logical_room.empty() && status.logical_room == baikaldb_logical_room) {
                if (addr == peer) {
                    addr_in_candicate = true;
                } else {
                    candicate_peers.emplace_back(peer);
                }
            } else {
                if (addr == peer) {
                    addr_in_normal = true;
                } else {
                    normal_peers.emplace_back(peer);
                }
            }
        }
        if (addr_in_candicate) {
            if (backup != nullptr) {
                if (candicate_peers.size() > 0) {
                    *backup = candicate_peers[0];
                } else if (normal_peers.size() > 0) {
                    *backup = normal_peers[0];
                }
            }
            return;
        }
        if (candicate_peers.size() > 0) {
            addr = candicate_peers[0];
            if (backup != nullptr) {
                if (candicate_peers.size() > 1) {
                    *backup = candicate_peers[1];
                } else if (normal_peers.size() > 0) {
                    *backup = normal_peers[0];
                }
            }
            return;
        }
        if (addr_in_normal) { 
            if (backup != nullptr) {
                if (normal_peers.size() > 0) {
                    *backup = normal_peers[0];
                }
            }
            return;
        } 
        if (normal_peers.size() > 0) {
            addr = normal_peers[0];
            if (normal_peers.size() > 1) {
                addr = normal_peers[1];
            }
        } else {
            DB_DEBUG("all peer faulty, %ld", region_id);
        }
    }
    void choose_other_if_faulty(pb::RegionInfo& info, std::string& addr);
    void other_normal_peer_to_leader(pb::RegionInfo& info, std::string& addr);
    bool need_process_binlog(RuntimeState* state, pb::OpType op_type) {
        if (op_type == pb::OP_PREPARE
            || op_type == pb::OP_COMMIT) {
            if (client_conn->need_send_binlog()) {
                return true;
            }
        } else if (op_type == pb::OP_ROLLBACK) {
            if (state->open_binlog() && binlog_prepare_success) {
                return true;
            }
        }
        return false;
    }
    ErrorType process_binlog_start(RuntimeState* state, pb::OpType op_type);
    void process_binlog_done(RuntimeState* state, pb::OpType op_type) {
        binlog_cond.wait();
    }
    ErrorType write_binlog(RuntimeState* state,
                           const pb::OpType op_type,
                           const uint64_t log_id);
    int64_t get_commit_ts();
    int memory_limit_exceeded(RuntimeState* state, MemRow* row);
    void memory_limit_release(RuntimeState* state) {
        state->memory_limit_release(used_bytes);
        used_bytes = 0;
    }
public:
    std::map<int64_t, std::vector<SmartRecord>>  index_records; //key: index_id
    std::map<int64_t, std::shared_ptr<RowBatch>> region_batch;
    std::map<int64_t, std::shared_ptr<RowBatch>> split_region_batch;
    bthread::Mutex  ttl_timestamp_mutex;
    std::map<int64_t, std::vector<int64_t>> region_id_ttl_timestamp_batch;

    std::multimap<std::string, int64_t> start_key_sort;
    std::multimap<std::string, int64_t> split_start_key_sort;
    bthread_mutex_t region_lock;
    std::set<int64_t> skip_region_set;
    ErrorType error = E_OK;
    // 因为split会导致多region出来,加锁保护公共资源
    std::atomic<int64_t> row_cnt = {0};
    std::atomic<int64_t> affected_rows = {0};
    std::atomic<int64_t> scan_rows = {0};
    std::atomic<int64_t> filter_rows = {0};
    std::atomic<int> analyze_fail_cnt = {0};
    BthreadCond binlog_cond;
    NetworkSocket* client_conn = nullptr;
    bool  binlog_prepare_success = false;
    bool  need_get_binlog_region = true;
    std::atomic<int64_t> used_bytes = {0};
    std::atomic<bool> primary_timestamp_updated{false};
    std::set<int64_t> no_copy_cache_plan_set;
    int64_t dynamic_timeout_ms = -1;
    TimeCost binlog_prewrite_time;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
