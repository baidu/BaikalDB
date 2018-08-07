// Copyright (c) 2018 Baidu, Inc. All Rights Reserved.
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
//#include "region_resource.h"

using google::protobuf::RepeatedPtrField;

namespace baikaldb {
class QueryContext;
class NetworkSocket;

// 不同region资源隔离，不需要每次从SchemaFactory加锁获取
struct RegionResource {
    IndexInfo& get_index_info(int64_t index_id) {
        return index_infos[index_id];
    }
    int64_t        region_id;
    int64_t        table_id;
    pb::RegionInfo region_info;
    TableInfo      table_info;
    IndexInfo      pri_info;
    // 包含primary
    std::map<int64_t, IndexInfo>  index_infos;
};

class RuntimeState {

public:
    ~RuntimeState();

    int init(const pb::StoreReq& req,
        const pb::Plan& plan, 
        const RepeatedPtrField<pb::TupleDescriptor>& tuples,
        TransactionPool* pool);

    int init(const pb::StoreReq& req, TransactionPool* pool);

    int init(QueryContext* ctx, DataBuffer* send_buf);

    // for prepared txn recovery in BaikalDB
    int init(const pb::CachePlan& commit_plan);

    void set_reverse_index_map(const std::map<int64_t, ReverseIndexBase*>& reverse_index_map) {
        _reverse_index_map = reverse_index_map;
    }
    std::map<int64_t, ReverseIndexBase*>& reverse_index_map() {
        return _reverse_index_map;
    }
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
    DataBuffer* send_buf() {
        return _send_buf;
    }
    Transaction* txn() {
        return _txn;
    }

    void set_txn(Transaction* txn) {
        _txn = txn;
    }

    Transaction* create_txn_if_null(pb::RegionInfo* _region_info) {
        if (_txn == nullptr) {
            _txn = new Transaction(0, _region_info, _txn_pool);
            _txn->begin();
        }
        return _txn;
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
    // runtime release at last
    RegionResource* resource() {
        return _resource.get();
    }

    void set_autocommit(bool autocommit) {
        _autocommit = autocommit;
    }

    bool autocommit() {
        return _autocommit;
    }

    void set_optimize_1pc(bool optimize) {
        _optimize_1pc = optimize;
    }

    bool optimize_1pc() {
        return _optimize_1pc;
    }

public:
    uint64_t          txn_id = 0;
    int32_t           seq_id = 0;
    MysqlErrCode      error_code = ER_ERROR_FIRST;
    std::ostringstream error_msg;

private:
    bool _is_cancelled = false;
    std::vector<pb::TupleDescriptor> _tuple_descs;
    MemRowDescriptor _mem_row_desc;
    int64_t          _region_id = 0;
    // index_id => ReverseIndex
    std::map<int64_t, ReverseIndexBase*> _reverse_index_map;
    DataBuffer*     _send_buf= nullptr;

    bool _need_check_region = true;

    int _num_increase_rows = 0; //存储净新增行数
    int _num_affected_rows = 0; //存储baikaldb写影响的行数
    int _num_returned_rows = 0; //存储baikaldb读返回的行数
    int64_t _log_id;

    bool              _autocommit = true;     // used for baikaldb and store
    bool              _optimize_1pc = false;  // 2pc de-generates to 1pc when autocommit=true and
                                              // there is only 1 region.
    NetworkSocket*    _client_conn = nullptr; // used for baikaldb
    TransactionPool*  _txn_pool = nullptr;    // used for store
    Transaction*      _txn = nullptr;         // used for store
    std::shared_ptr<RegionResource> _resource;// used for store

    // 如果用了排序列做索引，就不需要排序了
    bool _sort_use_index = false;
    std::vector<int64_t> _scan_indices;
    size_t _row_batch_capacity = ROW_BATCH_CAPACITY;
    int _multiple = 1;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
