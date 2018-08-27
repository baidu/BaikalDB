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
 
#include <memory>
#include "common.h"
#include "schema_factory.h"
#include "table_key.h"
#include "table_iterator.h"
#include "rocks_wrapper.h"
#include "mut_table_key.h"
#include "proto/meta.interface.pb.h"
#include "proto/store.interface.pb.h" 

namespace baikaldb {
DECLARE_bool(disable_wal);

typedef std::map<int, pb::CachePlan> CachePlanMap;

class TransactionPool;
class Transaction {
public:
    Transaction(uint64_t txn_id, pb::RegionInfo* region_info, TransactionPool* pool) : 
            _txn_id(txn_id),
            _region_info(region_info),
            _pool(pool) {
        _write_opt.disableWAL = FLAGS_disable_wal;
        bthread_mutex_init(&_txn_mutex, nullptr);
    }

    virtual ~Transaction() {
        bthread_mutex_destroy(&_txn_mutex);
        delete _txn;
        _txn = nullptr;
    }

    // Begin a new transaction
    int begin();

    // Wrap the close recovered (prepared) rocksdb Transaction after db restart
    int begin(rocksdb::Transaction* txn);

    rocksdb::Status prepare();

    rocksdb::Status commit();
    
    rocksdb::Status rollback();

    // Set a savepoint for partial rollback in case of some DML execute failed on other store
    int set_save_point();

    // rollback the txn to a specific sequence id
    void rollback_to_point(int seq_id);

    // Key format: region_id(8 bytes) + table_id(8 bytes) + primary_key_fields;
    // Value format: protobuf of all non-primary key fields;
    // First encode key with @record, and then erase the key fields from @record;
    int put_primary(int64_t region, IndexInfo& pk_index, SmartRecord record);

    // UNIQUE INDEX format: <region_id + index_id + null_flag + index_fields, primary_key>
    // NON-UNIQUE INDEX format: <region_id + index_id + null_flag + index_fields + primary_key, NULL>
    int put_secondary(int64_t region, IndexInfo& index, SmartRecord record);

    // TODO: update return status
    // Return -2 if key not found
    // If get succ, value is encoded into "key"
    int get_update_primary(
            int64_t     region, 
            IndexInfo&  pk_index, 
            SmartRecord key, 
            std::vector<int32_t>& fields,
            GetMode     mode,
            bool        check_region);

    int get_update_primary(int64_t region, 
            IndexInfo&      pk_index, 
            const TableKey& key,
            GetMode         mode, 
            SmartRecord     val,
            std::vector<int32_t>& fields,
            bool            check_region);

    // TODO: update return status
    // Return -2 if key not found
    int get_update_secondary(
            int64_t         region,
            IndexInfo&      pk_index,
            IndexInfo&      index,
            SmartRecord     key, 
            GetMode         mode,
            bool            check_region);

    int get_update_secondary(
            int64_t           region, 
            IndexInfo&        pk_index,
            IndexInfo&        index,
            const SmartRecord key, 
            GetMode           mode, 
            MutTableKey&      pk_val,
            bool              check_region);

    int remove(int64_t region, IndexInfo& index, const SmartRecord key);
    int remove(int64_t region, IndexInfo& index, const TableKey&   key);

    rocksdb::Transaction* get_txn() {
        return _txn;
    }

    uint64_t txn_id() {
        return _txn_id;
    }

    int seq_id() {
        return _seq_id;
    }

    void set_seq_id(int seq_id) {
        _seq_id = seq_id;
    }

    bool is_prepared() {
        return _is_prepared;
    }

    bool is_rolledback() {
        return _is_rolledback;
    }

    bool prepare_apply() {
        return _prepare_apply;
    }

    void set_prepare_apply() {
        _prepare_apply = true;
    }

    int64_t prepare_time_us() {
        return _prepare_time_us;
    }

    CachePlanMap& cache_plan_map() {
        return _cache_plan_map;
    }

    void reset_active_time() {
        last_active_time = butil::gettimeofday_us();
    }

    bool has_write() {
        for (auto& pair : _cache_plan_map) {
            pb::OpType type = pair.second.op_type();
            if (type == pb::OP_INSERT || type == pb::OP_DELETE || type == pb::OP_UPDATE) {
                return true;
            }
        }
        return false;
    }

    // 
    static int get_full_primary_key(
            rocksdb::Slice  index_bytes, 
            rocksdb::Slice  pk_bytes,
            IndexInfo&      pk_index, 
            IndexInfo&      index_info, 
            char*           pk_buf);

    // Check whether the pair <key, value> resides within the region range [start, end);
    // (start == nullptr) denotes ignoring range start key checking;
    // (end == nullptr) denotes ignoring range end key checking;
    // The key to be checked does not include (region_id + index_id) prefix;
    static bool fits_region_range(
            rocksdb::Slice      key, 
            rocksdb::Slice      value,
            const std::string*  start, 
            const std::string*  end, 
            IndexInfo&          pk_index,
            IndexInfo&          index_info);

public:
    int64_t     num_increase_rows = 0;
    int64_t     last_active_time = 0;
    int         dml_num_affected_rows = 0; //for autocommit dml return

private:
    int get_update_primary(
            int64_t         region, 
            IndexInfo&      pk_index, 
            const TableKey& key, 
            GetMode         mode, 
            SmartRecord     val, 
            std::vector<int32_t>& fields,
            bool            parse_key,
            bool            check_region);

    // seq_id should be updated after each query execution regardless of success or failure 
    int                             _seq_id = 0;
    uint64_t                        _txn_id = 0;
    bool                            _is_prepared = false;
    bool                            _is_finished = false;
    bool                            _is_rolledback = false;
    bool                            _prepare_apply = false;
    int64_t                         _prepare_time_us = 0;
    std::stack<int>                 _save_point_seq;
    std::stack<int64_t>             _save_point_increase_rows;

    // store the query cmd from BEGIN to PREPARE
    CachePlanMap                    _cache_plan_map;

    rocksdb::ReadOptions            _read_opt;
    rocksdb::WriteOptions           _write_opt;
    rocksdb::TransactionOptions     _txn_opt;
    
    rocksdb::Transaction*           _txn = nullptr;
    rocksdb::ColumnFamilyHandle*    _data_cf = nullptr;
    pb::RegionInfo*                 _region_info = nullptr;
    RocksWrapper*                   _db = nullptr;
    TransactionPool*                _pool = nullptr;

    bthread_mutex_t                 _txn_mutex;
};

typedef std::shared_ptr<Transaction> SmartTransaction;
}
