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
 
#include <memory>
#include <stack>
#include "common.h"
#include "ddl_common.h"
#include "schema_factory.h"
#include "table_key.h"
#include "table_iterator.h"
#include "rocks_wrapper.h"
#include "mut_table_key.h"
#include "proto/meta.interface.pb.h"
#include "proto/store.interface.pb.h" 
#include "trace_state.h"

namespace baikaldb {
DECLARE_bool(disable_wal);

typedef std::map<int, pb::CachePlan> CachePlanMap;

struct DllTransactionState {
    DllParam* ddl_ptr {nullptr};
    pb::IndexState index_state;
    bool is_ok {false};
};

using SmartDllTransactionState = std::shared_ptr<DllTransactionState>;

inline uint64_t ttl_encode(int64_t ttl_timestamp_us) {
    return KeyEncoder::to_endian_u64(
            KeyEncoder::encode_i64(ttl_timestamp_us));
}

inline int64_t ttl_decode(rocksdb::Slice value) {
    return KeyEncoder::decode_i64(
            KeyEncoder::to_endian_u64(
                *reinterpret_cast<const uint64_t*>(value.data())));
}

class TransactionPool;
class Transaction {
public:
    Transaction(uint64_t txn_id, TransactionPool* pool, bool use_ttl) : 
            _txn_id(txn_id),
            _pool(pool),
            _use_ttl(use_ttl) {
        _write_opt.disableWAL = FLAGS_disable_wal;
        bthread_mutex_init(&_txn_mutex, nullptr);
    }

    virtual ~Transaction() {
        if (!_is_finished) {
            rollback();
        }
        if (_db != nullptr && _snapshot != nullptr) {
            _db->relase_snapshot(_snapshot);
        }
        if (_ddl_state != nullptr && _ddl_state->is_ok) {
            DB_DEBUG("txn_id: %lu Transaction atomic index[%s] --", 
                   _txn_id, pb::IndexState_Name(_ddl_state->index_state).c_str());
            switch (_ddl_state->index_state) {
                case pb::IS_NONE:
                    _ddl_state->ddl_ptr->none_count--;
                    break;
                case pb::IS_DELETE_ONLY:
                    _ddl_state->ddl_ptr->delete_only_count--;
                    break;
                case pb::IS_WRITE_ONLY:
                    _ddl_state->ddl_ptr->write_only_count--;
                    break;
                case pb::IS_PUBLIC:
                    _ddl_state->ddl_ptr->public_count--;
                    break;
                case pb::IS_DELETE_LOCAL:
                    _ddl_state->ddl_ptr->delete_local_count--;
                    break;
                case pb::IS_WRITE_LOCAL:
                    _ddl_state->ddl_ptr->write_local_count--;
                    break;
                default:
                    DB_WARNING("unknown state");
            }
        }
        delete _txn;
        _txn = nullptr;
        _ddl_state = nullptr;
        bthread_mutex_destroy(&_txn_mutex);
    }

    // Begin a new transaction
    int begin();
    int begin(rocksdb::TransactionOptions txn_opt);
    // Wrap the close recovered (prepared) rocksdb Transaction after db restart
    int begin(rocksdb::Transaction* txn);
    const rocksdb::Snapshot* get_snapshot() {
        return _snapshot;
    }

    rocksdb::Status prepare();

    rocksdb::Status commit();
    
    rocksdb::Status rollback();

    // Set a savepoint for partial rollback in case of some DML execute failed on other store
    int set_save_point();

    // rollback the txn to a specific sequence id
    void rollback_to_point(int seq_id);

    // Key format: region_id(8 bytes) + table_id(8 bytes) + primary_key_fields;
    // Value format: protobuf of all non-primary key fields;
    // Value is null if engine = rocksdb_cstore;
    // First encode key with @record, and then erase the key fields from @record;
    int put_primary(int64_t region, IndexInfo& pk_index, SmartRecord record,
                    std::set<int32_t>* update_fields = nullptr);

    // Key format: region_id(8 bytes) + table_id(4 bytes) + field_id(4 bytes) + primary_key_fields;
    // Value format: non-primary key fields encode value;
    // update_fields: null for new row, not null for old row
    int put_primary_columns(const TableKey& primary_key, SmartRecord record,
                            std::set<int32_t>* update_fields);

    // UNIQUE INDEX format: <region_id + index_id + null_flag + index_fields, primary_key>
    // NON-UNIQUE INDEX format: <region_id + index_id + null_flag + index_fields + primary_key, NULL>
    int put_secondary(int64_t region, IndexInfo& index, SmartRecord record);
    int put_meta_info(const std::string& key, const std::string& value);
    
    int remove_meta_info(const std::string& key);
    // TODO: update return status
    // Return -2 if key not found
    // If get succ, value is encoded into "key"
    int get_update_primary(
            int64_t     region, 
            IndexInfo&  pk_index, 
            SmartRecord key, 
            std::map<int32_t, FieldInfo*>& fields,
            GetMode     mode,
            bool        check_region);

    int get_update_primary(int64_t region, 
            IndexInfo&      pk_index, 
            const TableKey& key,
            GetMode         mode, 
            SmartRecord     val,
            std::map<int32_t, FieldInfo*>& fields,
            bool            check_region);

    int get_update_primary_columns(
            const TableKey& primary_key,
            GetMode         mode,
            SmartRecord     val,
            std::map<int32_t, FieldInfo*>& fields);

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
    int get_for_update(const std::string& key, std::string* value);
    rocksdb::Status put_kv_without_lock(const std::string& key, const std::string& value, int64_t ttl_timestamp_us);
    int put_kv(const std::string& key, const std::string& value);
    int delete_kv(const std::string& key);
    
    int remove(int64_t region, IndexInfo& index, const SmartRecord key);
    int remove(int64_t region, IndexInfo& index, const TableKey&   key);
    int remove_columns(const TableKey& primary_key);

    rocksdb::Transaction* get_txn() {
        return _txn;
    }

    uint64_t txn_id() {
        return _txn_id;
    }

    uint64_t rocksdb_txn_id() {
        return _txn->GetID();
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

    bool is_finished() {
        return _is_finished;
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

    void push_cmd_to_cache(int seq_id, pb::CachePlan plan_item) {
        BAIDU_SCOPED_LOCK(_txn_mutex);
        _seq_id = seq_id;
        if (_cache_plan_map.count(seq_id) > 0) {
            return;
        }
        _cache_plan_map.insert(std::make_pair(seq_id, plan_item));
    }

    bool has_write() {
        return _has_write;
    }

    void set_has_write(bool flag) {
        _has_write = flag;
    }

    bool write_begin_index() {
        return _write_begin_index;
    }

    void set_write_begin_index(bool flag) {
        _write_begin_index = flag;
    }
    // return -1表示没有cache plan
    int get_cache_plan_infos(pb::TransactionInfo& txn_info) {
        BAIDU_SCOPED_LOCK(_txn_mutex);
        if (_cache_plan_map.size() == 0) {
            return -1;
        }
        if (!_has_write) {
            return -2;
        }
        txn_info.set_txn_id(_txn_id);
        txn_info.set_seq_id(_seq_id);
        txn_info.set_start_seq_id(1);
        txn_info.set_optimize_1pc(false);
        for (auto seq_id : _need_rollback_seq) {
            txn_info.add_need_rollback_seq(seq_id);
        }
        for (auto& cache_plan : _cache_plan_map) {
            txn_info.add_cache_plans()->CopyFrom(cache_plan.second);
        }
        txn_info.set_num_rows(num_increase_rows);
        txn_info.set_primary_region_id(_primary_region_id);
        return 0;
    }

    void set_region_info(pb::RegionInfo* region_info) {
        _region_info = region_info;
        if (_region_info == nullptr) {
            DB_WARNING("no region_info");
            return;
        }
        // _is_global_index
        if (_region_info->has_main_table_id() && _region_info->main_table_id() != 0 &&
                    _region_info->table_id() != _region_info->main_table_id()) {
            return;
        }
        _table_info = SchemaFactory::get_instance()->get_table_info_ptr(_region_info->table_id());
        _pri_info = SchemaFactory::get_instance()->get_index_info_ptr(_region_info->table_id());
        if (is_cstore()) {
           _pri_field_ids.clear();
           for (auto& field_info : _pri_info->fields) {
                 _pri_field_ids.insert(field_info.id);
           }
       }
    }
    bool is_cstore() {
        if (_table_info.get() == nullptr) {
            // _is_global_index
            if (_region_info->has_main_table_id() && _region_info->main_table_id() != 0 &&
                        _region_info->table_id() != _region_info->main_table_id()) {
                return false;
            }
            DB_FATAL("error: no table_info");
            return false;
        }
        return _table_info->engine == pb::ROCKSDB_CSTORE;
    }

    void set_write_ttl_timestamp_us(int64_t write_ttl_timestamp_us) {
        _write_ttl_timestamp_us = write_ttl_timestamp_us;
    }
    int64_t write_ttl_timestamp_us() const {
        return _write_ttl_timestamp_us;
    }
    int64_t read_ttl_timestamp_us() const {
        return _read_ttl_timestamp_us;
    }
    bool use_ttl() const {
        return _use_ttl;
    }

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
    int fits_region_range_for_global_index(IndexInfo& pk_index, 
            IndexInfo& index_info, 
            SmartRecord record, 
            bool& result);
    int fits_region_range_for_primary(IndexInfo& pk_index,
        SmartRecord record,
        bool& result);
    
    pb::StoreReq* get_raftreq() {
        return &_store_req;
    }

    void set_ddl_state(DllParam* ddl_param) {
        if (ddl_param != nullptr && ddl_param->is_doing && _ddl_state == nullptr) {
            //当is_doing时，该ddl在工作中。
            _ddl_state = std::make_shared<DllTransactionState>();
            _ddl_state->ddl_ptr = ddl_param;
            _ddl_state->is_ok = false; 
            update_ddl_state();
        }
    }

    void update_ddl_state() {
        if (_ddl_state != nullptr && _region_info != nullptr) {
            auto table_id = _region_info->table_id();
            pb::IndexState state = pb::IS_PUBLIC;
            int ret = SchemaFactory::get_instance()->get_table_state(table_id, state);
            _ddl_state->is_ok = !ret;
            if (_ddl_state->is_ok) {
                _ddl_state->index_state = state;
                DB_DEBUG("region_%lld, txn_id: %lu Transaction atomic index[%s] ++", 
                       _region_info->region_id(), _txn_id, pb::IndexState_Name(state).c_str());
                switch (state) {
                    case pb::IS_NONE:
                        _ddl_state->ddl_ptr->none_count++;
                        break;
                    case pb::IS_DELETE_ONLY:
                        _ddl_state->ddl_ptr->delete_only_count++;
                        break;
                    case pb::IS_WRITE_ONLY:
                        _ddl_state->ddl_ptr->write_only_count++;
                        break;
                    case pb::IS_DELETE_LOCAL:
                        _ddl_state->ddl_ptr->delete_local_count++;
                        break;
                    case pb::IS_WRITE_LOCAL:
                        _ddl_state->ddl_ptr->write_local_count++;
                        break;
                    case pb::IS_PUBLIC:
                        _ddl_state->ddl_ptr->public_count++;
                        break;
                    default:
                        DB_WARNING("unknown state");
                }
            }
        }
    }

    void set_primary_region_id(int64_t region_id) {
        _primary_region_id = region_id;
    }

    int64_t primary_region_id() const {
        return _primary_region_id;
    }

    /* baikaldb执行insert/delete/update时才会设置primary_region_id
       只读事务_primary_region_id == -1 */
    bool primary_region_id_seted() {
        if (_primary_region_id != -1) {
            return true;
        }
        return false;
    }

    bool is_primary_region() {
        if (!primary_region_id_seted()) {
            return true;
        }
        if (_region_info != nullptr) {
            return (_region_info->region_id() == _primary_region_id);
        }
        return false;
    }

    bool need_write_rollback(pb::OpType op_type) {
        if (op_type == pb::OP_ROLLBACK && (_primary_region_id != -1)) {
            return true;
        }
        return false;
    }

    void clear_current_req_point_seq() {
        _current_req_point_seq.clear();
    }

    size_t save_point_seq_size() {
        return _save_point_seq.size();
    }

    void rollback_current_request();

public:
    bool        _is_separate = false; //是否存储计算分离
    int64_t     num_increase_rows = 0;
    int64_t     last_active_time = 0;
    int64_t     begin_time = 0;
    int         dml_num_affected_rows = 0; //for autocommit dml return
    int64_t     batch_num_increase_rows = 0;//用于batch txn
    pb::ErrCode err_code = pb::SUCCESS;

private:
    int get_update_primary(
            int64_t         region, 
            IndexInfo&      pk_index, 
            const TableKey& key, 
            GetMode         mode, 
            SmartRecord     val, 
            std::map<int32_t, FieldInfo*>& fields,
            bool            parse_key,
            bool            check_region);
    
    void add_kvop_put(std::string& key, std::string& value, int64_t ttl_timestamp_us) {
        //DB_WARNING("txn:%p, add kvop put key:%s, value:%s", this,
        //           str_to_hex(key).c_str(), str_to_hex(value).c_str());
        pb::KvOp* kv_op = _store_req.add_kv_ops();
        kv_op->set_op_type(pb::OP_PUT_KV);
        kv_op->set_key(key);
        kv_op->set_value(value);
        kv_op->set_ttl_timestamp_us(ttl_timestamp_us);
    }
    
    void add_kvop_delete(std::string& key) {
        //DB_WARNING("txn:%p, add kvop delete key:%s", this, str_to_hex(key).c_str());
        pb::KvOp* kv_op = _store_req.add_kv_ops();
        kv_op->set_op_type(pb::OP_DELETE_KV);
        kv_op->set_key(key);
    }
    
    // seq_id should be updated after each query execution regardless of success or failure 
    int                             _seq_id = 0;
    uint64_t                        _txn_id = 0;
    bool                            _is_prepared = false;
    bool                            _is_finished = false;
    bool                            _is_rolledback = false;
    bool                            _prepare_apply = false;
    bool                            _has_write = false;
    bool                            _write_begin_index = true;
    int64_t                         _prepare_time_us = 0;
    std::stack<int>                 _save_point_seq;
    std::stack<int64_t>             _save_point_increase_rows;
    pb::StoreReq                    _store_req;
    int64_t                         _primary_region_id = -1;
    std::set<int>                   _current_req_point_seq;
    std::set<int>                   _need_rollback_seq;
    // store the query cmd from BEGIN to PREPARE
    CachePlanMap                    _cache_plan_map;

    rocksdb::WriteOptions           _write_opt;
    rocksdb::TransactionOptions     _txn_opt;
    
    rocksdb::Transaction*           _txn = nullptr;
    rocksdb::ColumnFamilyHandle*    _data_cf = nullptr;
    rocksdb::ColumnFamilyHandle*    _meta_cf = nullptr;
    const rocksdb::Snapshot*        _snapshot = nullptr;
    pb::RegionInfo*                 _region_info = nullptr;
    RocksWrapper*                   _db = nullptr;
    TransactionPool*                _pool = nullptr;
    SmartTable                      _table_info; // for cstore
    SmartIndex                      _pri_info;  // for cstore
    std::set<int32_t>               _pri_field_ids; // for cstore

    bthread_mutex_t                 _txn_mutex;
    SmartDllTransactionState        _ddl_state = nullptr;
    bool                            _use_ttl = false;
    int64_t                         _read_ttl_timestamp_us = 0; //ttl读取时间
    int64_t                         _write_ttl_timestamp_us = 0; //ttl写入时间
};

typedef std::shared_ptr<Transaction> SmartTransaction;
}
