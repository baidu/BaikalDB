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

#include <stdint.h>
#include "mem_row_descriptor.h"
#include "data_buffer.h"
#include "proto/store.interface.pb.h"
#include "transaction_pool.h"
#include "transaction.h"
#include "reverse_index.h"
#include "reverse_interface.h"
#include "row_batch.h"
#include "mysql_err_code.h"
#include "trace_state.h"
#include "statistics.h"
//#include "region_resource.h"

using google::protobuf::RepeatedPtrField;

namespace baikaldb {
DECLARE_int32(per_txn_max_num_locks);
struct TxnLimitMap {
    static TxnLimitMap* get_instance() {
        static TxnLimitMap _instance;
        return &_instance;
    }

    bool check_txn_limit(const uint64_t txn_id, const int row_count) {
        bool too_many = false;
        auto update_func = [&too_many, &row_count] (int& count) {
            count += row_count;
            if (count > FLAGS_per_txn_max_num_locks) {
                too_many = true;
            }
        };
        _txn_limit_mapping.init_if_not_exist_else_update(txn_id, update_func, row_count);
        return too_many;
    }

    void erase(const uint64_t txn_id) {
        _txn_limit_mapping.erase(txn_id);
    }

private:
    TxnLimitMap() {}
    ThreadSafeMap<uint64_t, int> _txn_limit_mapping;
};

class QueryContext;
class NetworkSocket;

// 不同region资源隔离，不需要每次从SchemaFactory加锁获取
struct RegionResource {
    pb::RegionInfo region_info;
    DllParam* ddl_param_ptr;
};

class RuntimeStatePool;
class RuntimeState {

public:
    ~RuntimeState();

    // baikalStore init
    int init(const pb::StoreReq& req,
        const pb::Plan& plan, 
        const RepeatedPtrField<pb::TupleDescriptor>& tuples,
        TransactionPool* pool,
        bool store_compute_separate);

    // baikaldb init
    int init(QueryContext* ctx, DataBuffer* send_buf);

    // for prepared txn recovery in BaikalDB
    //int init(const pb::CachePlan& commit_plan);

    void set_reverse_index_map(const std::map<int64_t, ReverseIndexBase*>& reverse_index_map) {
        _reverse_index_map = reverse_index_map;
    }
    std::map<int64_t, ReverseIndexBase*>& reverse_index_map() {
        return _reverse_index_map;
    }
    void conn_id_cancel(uint64_t db_conn_id);
    void cancel() {
        _is_cancelled = true;
    }
    bool is_cancelled() {
        return _is_cancelled;
    }
    pb::TupleDescriptor* get_tuple_desc(int tuple_id) {
        return &_tuple_descs[tuple_id];
    }
    std::vector<pb::TupleDescriptor>* mutable_tuple_descs() {
        return &_tuple_descs;
    }
    int32_t get_slot_id(int32_t tuple_id, int32_t field_id) {
        for (const auto& slot_desc : _tuple_descs[tuple_id].slots()) {
            if (slot_desc.field_id() == field_id) {
                return slot_desc.slot_id();
            }
        }
        return -1;
    }
    const std::vector<pb::TupleDescriptor>& tuple_descs() {
        return _tuple_descs;
    }
    MemRowDescriptor* mem_row_desc() {
        return &_mem_row_desc;
    }
    int64_t region_id() {
        return _region_id;
    }
    int64_t region_version() {
        return _region_version;
    }
    int64_t table_id() {
        if (_resource != nullptr) {
            return _resource->region_info.table_id();
        }
        return 0;
    }
    DataBuffer* send_buf() {
        return _send_buf;
    }
    SmartTransaction txn() {
        return _txn;
    }

    void set_txn(SmartTransaction txn) {
        _txn = txn;
    }

    SmartTransaction create_txn_if_null() {
        if (_txn != nullptr) {
            return _txn;
        }
        _txn = SmartTransaction(new Transaction(0, _txn_pool, use_ttl));
        _txn->set_region_info(&(_resource->region_info));
        _txn->set_ddl_state(_resource->ddl_param_ptr);
        _txn->_is_separate = is_separate;
        _txn->begin();
        return _txn;
    }
    SmartTransaction create_batch_txn() {
        auto txn = SmartTransaction(new Transaction(0, _txn_pool, use_ttl));
        txn->set_region_info(&(_resource->region_info));
        txn->set_ddl_state(_resource->ddl_param_ptr);
        txn->_is_separate = is_separate;
        txn->begin();
        return txn;
    }
    void set_num_increase_rows(int num) {
        _num_increase_rows = num;
    }
    int num_increase_rows() const {
        return _num_increase_rows;
    }
    bool need_check_region() const {
        return _need_check_region;
    }

    void set_num_affected_rows(int num) {
        _num_affected_rows = num;
    }

    void inc_num_returned_rows(int num) {
        _num_returned_rows += num;
    }

    int num_affected_rows() {
        return _num_affected_rows;
    }

    int num_returned_rows() {
        return _num_returned_rows;
    }
    void set_num_scan_rows(int num) {
        _num_scan_rows = num;
    }
    int num_scan_rows() {
        return _num_scan_rows;
    }

    void set_num_filter_rows(int num) {
        _num_filter_rows = num;
    }
    void inc_num_filter_rows() {
        _num_filter_rows++;
    }

    int num_filter_rows() {
        return _num_filter_rows;
    }

    void set_log_id(uint64_t logid) {
        _log_id = logid;
    }

    void set_sort_use_index() {
        _sort_use_index = true;
    }

    bool sort_use_index() {
        return _sort_use_index;
    }

    uint64_t log_id() {
        return _log_id;
    }

    size_t multiple_row_batch_capacity() {
        if (_row_batch_capacity * _multiple < ROW_BATCH_CAPACITY) {
            //两倍扩散
            _multiple *= 2;
            return _row_batch_capacity * _multiple;
        } else {
            return ROW_BATCH_CAPACITY;
        }
    }

    size_t row_batch_capacity() {
        return std::min(_row_batch_capacity * 2, ROW_BATCH_CAPACITY);
    }

    void add_scan_index(int64_t scan_index) {
        _scan_indices.push_back(scan_index);
    }

    std::vector<int64_t>& scan_indices() {
        return _scan_indices;
    }

    // Only used on baikaldb side
    void set_client_conn(NetworkSocket* socket) {
        _client_conn = socket;
    }

    NetworkSocket* client_conn() {
        return _client_conn;
    }

    TransactionPool* txn_pool() {
        return _txn_pool;
    }

    void set_resource(std::shared_ptr<RegionResource> resource) {
        _resource = resource;
    }
    void set_pool(RuntimeStatePool* pool) {
        _pool = pool;
    }
    // runtime release at last
    RegionResource* resource() {
        return _resource.get();
    }

    void set_single_sql_autocommit(bool single_sql_autocommit) {
        _single_sql_autocommit = single_sql_autocommit;
    }

    bool single_sql_autocommit() {
        return _single_sql_autocommit;
    }

    void set_optimize_1pc(bool optimize) {
        _optimize_1pc = optimize;
    }

    bool optimize_1pc() {
        return _optimize_1pc;
    }

    bool is_eos() { 
        return _eos;
    }

    void set_eos() {
        _eos = true;
    }
    std::vector<TraceTimeCost>* get_trace_cost() {
        return &_trace_cost_vec;
    } 

    void set_primary_region_id(int64_t region_id) {
        _primary_region_id = region_id;
    }

    int64_t primary_region_id() const {
        return _primary_region_id;
    }

public:
    uint64_t          txn_id = 0;
    int32_t           seq_id = 0;
    MysqlErrCode      error_code = ER_ERROR_FIRST;
    std::ostringstream error_msg;
    bool              is_full_export = false;
    bool              is_separate = false; //是否为计算存储分离模式
    bool              use_ttl = false;
    BthreadCond       txn_cond;
    std::function<void(RuntimeState* state, SmartTransaction txn)> raft_func;
    bool              is_fail = false;
    bool              need_txn_limit = false;
    std::string       raft_error_msg;
    ExplainType       explain_type = EXPLAIN_NULL;
    std::shared_ptr<CMsketch> cmsketch = nullptr;

private:
    bool _is_inited    = false;
    bool _is_cancelled = false;
    bool _eos          = false;
    std::vector<pb::TupleDescriptor> _tuple_descs;
    MemRowDescriptor _mem_row_desc;
    int64_t          _region_id = 0;
    int64_t          _region_version = 0;
    // index_id => ReverseIndex
    std::map<int64_t, ReverseIndexBase*> _reverse_index_map;
    DataBuffer*     _send_buf= nullptr;

    bool _need_check_region = true;

    int _num_increase_rows = 0; //存储净新增行数
    int _num_affected_rows = 0; //存储baikaldb写影响的行数
    int _num_returned_rows = 0; //存储baikaldb读返回的行数
    int _num_scan_rows     = 0; //存储baikalStore扫描行数
    int _num_filter_rows   = 0; //存储过滤行数
    int64_t _log_id = 0;

    bool              _single_sql_autocommit = true;     // used for baikaldb and store
    bool              _optimize_1pc = false;  // 2pc de-generates to 1pc when autocommit=true and
                                              // there is only 1 region.
    NetworkSocket*    _client_conn = nullptr; // used for baikaldb
    TransactionPool*  _txn_pool = nullptr;    // used for store
    SmartTransaction  _txn = nullptr;         // used for store
    std::shared_ptr<RegionResource> _resource;// used for store
    int64_t           _primary_region_id = -1;// used for store

    // 如果用了排序列做索引，就不需要排序了
    bool _sort_use_index = false;
    std::vector<int64_t> _scan_indices;
    size_t _row_batch_capacity = ROW_BATCH_CAPACITY;
    int _multiple = 1;
    RuntimeStatePool* _pool = nullptr;
    //trace使用
    std::vector<TraceTimeCost> _trace_cost_vec;
};
typedef std::shared_ptr<RuntimeState> SmartState;
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
