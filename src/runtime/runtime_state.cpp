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
#include "runtime_state_pool.h"
#include "query_context.h"
#include "network_socket.h"

namespace baikaldb {
DEFINE_int32(per_txn_max_num_locks, 1000000, "max num locks per txn default 100w");
RuntimeState::~RuntimeState() {}

int RuntimeState::init(const pb::StoreReq& req,
        const pb::Plan& plan, 
        const RepeatedPtrField<pb::TupleDescriptor>& tuples,
        TransactionPool* pool,
        bool store_compute_separate) {
    for (auto& tuple : tuples) {
        _tuple_descs.push_back(tuple);
    }
    if (_tuple_descs.size() > 0) {
        int ret = _mem_row_desc.init(_tuple_descs);
        if (ret < 0) {
            DB_WARNING("_mem_row_desc init fail");
            return -1;
        }
    }
    _region_id = req.region_id();
    _region_version = req.region_version();
    if (req.has_not_check_region()) {
        _need_check_region = !req.not_check_region();
    }
    int64_t limit = req.plan().nodes(0).limit();
    //DB_WARNING("limit:%ld", limit);
    if (limit > 0) {
        _row_batch_capacity = limit / 2 + 1;
    }
    if (req.txn_infos_size() > 0) {
        const pb::TransactionInfo& txn_info = req.txn_infos(0);
        if (txn_info.has_txn_id()) {
            txn_id = txn_info.txn_id();
        }
        if (txn_info.has_seq_id()) {
            seq_id = txn_info.seq_id();
        }
        // if (txn_info.has_autocommit()) {
        //     _autocommit = txn_info.autocommit();
        // }
        if (txn_info.has_primary_region_id()) {
            set_primary_region_id(txn_info.primary_region_id());
        }
    }
    if (pool == nullptr) {
        DB_WARNING("error: txn pool is null: %ld", _region_id);
        return -1;
    }
    is_separate = store_compute_separate;
    _log_id = req.log_id();
    _txn_pool = pool;
    _txn = _txn_pool->get_txn(txn_id);
    use_ttl = _txn_pool->use_ttl();
    if (_txn != nullptr) {
        _txn->set_region_info(&(_resource->region_info));
        _txn->set_ddl_state(_resource->ddl_param_ptr);
    }
    return 0;
}

int RuntimeState::init(QueryContext* ctx, DataBuffer* send_buf) {
    _num_increase_rows = 0; 
    _num_affected_rows = 0; 
    _num_returned_rows = 0; 
    _num_scan_rows     = 0; 
    _num_filter_rows   = 0; 
    set_client_conn(ctx->client_conn);
    if (_client_conn == nullptr) {
        return -1;
    }
    txn_id = _client_conn->txn_id;
    _log_id = ctx->stat_info.log_id;
    // prepare 复用runtime
    if (_is_inited) {
        return 0;
    }
    _send_buf = send_buf;
    _tuple_descs = ctx->tuple_descs();
    if (_tuple_descs.size() > 0) {
        int ret = _mem_row_desc.init(_tuple_descs);
        if (ret < 0) {
            DB_WARNING("_mem_row_desc init fail");
            return -1;
        }
    }
    _is_inited = true;
    return 0;
}
/*
int RuntimeState::init(const pb::CachePlan& commit_plan) {
    txn_id = _client_conn->txn_id;
    seq_id = _client_conn->seq_id;
    int ret = SchemaFactory::get_instance()->get_region_by_key(commit_plan.regions(), _client_conn->region_infos);
    if (ret != 0) {
        // region may be removed by truncate table
        DB_FATAL("TransactionWarn: get_region_by_key failed, txn_id: %lu", txn_id);
        return -1;
    }
    return 0;
}
*/
void RuntimeState::conn_id_cancel(uint64_t db_conn_id) {
    if (_pool != nullptr) {
        auto s = _pool->get(db_conn_id);
        if (s != nullptr) {
            s->cancel();
        }
    }
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
