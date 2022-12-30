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

#include "transaction.h"
#include "transaction_pool.h"
#include "tuple_record.h"
#include <boost/scoped_array.hpp>
#include <gflags/gflags.h>
#include "reverse_index.h"

namespace baikaldb {
DEFINE_bool(disable_wal, false, "disable rocksdb interanal WAL log, only use raft log");
DECLARE_int32(rocks_transaction_lock_timeout_ms);
DEFINE_int64(exec_1pc_out_fsm_timeout_ms, 5 * 1000, "exec 1pc out of fsm, timeout");
DEFINE_int64(exec_1pc_in_fsm_timeout_ms, 100, "exec 1pc in fsm, timeout");

// value 出参，会remove prefix
int64_t ttl_decode(rocksdb::Slice& value, const IndexInfo* const index_info, int64_t base_expire_time_us) {
    if (base_expire_time_us == 0) {
        // 非online TTL
        int64_t time = decode_first_8bytes2int64(value);
        value.remove_prefix(sizeof(uint64_t));
        return  time;
    }

    if (value.size() < sizeof(uint64_t)) {
        DB_DEBUG("value size < 8, index id: %ld, name: %s, ttl time: %ld", 
            index_info->id, pb::IndexType_Name(index_info->type).c_str(), base_expire_time_us);
        return base_expire_time_us;
    }

    // 时间戳无效则认为没有时间前缀
    int64_t time = decode_first_8bytes2int64(value);
    if (!valid_timestamp_us(time)) {
        DB_DEBUG("invalid timestamp, index id: %ld, name: %s, ttl time: %ld, data size: %lu, invalid time: %ld", 
            index_info->id, pb::IndexType_Name(index_info->type).c_str(), base_expire_time_us, value.size(), time);
        return base_expire_time_us;
    } 

    //时间戳有效，需要验证编码是否匹配
    if (index_info->type == pb::I_KEY) {        
        if (value.size() == sizeof(uint64_t)) {
            value.remove_prefix(sizeof(uint64_t));
            DB_DEBUG("has prefix timestamp index id: %ld, name: %s, ttl time: %ld", 
                index_info->id, pb::IndexType_Name(index_info->type).c_str(), time);
        } else {
            // 报警
            DB_WARNING("ttl_decode fail, value size != 8, index id: %ld, name: %s, ttl time: %ld", 
                index_info->id, pb::IndexType_Name(index_info->type).c_str(), time);
        }
        return time;
    } else if (index_info->type == pb::I_PRIMARY) {
        TupleRecord tuple_record(value);
        if (tuple_record.verification_fields(index_info->max_field_id) != 0) {
            DB_DEBUG("has prefix timestamp index id: %ld, name: %s, ttl time: %ld, value size: %lu", 
                index_info->id, pb::IndexType_Name(index_info->type).c_str(), time, value.size());
            value.remove_prefix(sizeof(uint64_t));
        } else {  
            // 报警
            DB_WARNING("ttl_decode fail, index id: %ld, name: %s, ttl time: %ld, value size: %lu, base_expire_time_us: %ld, value: %s", 
                    index_info->id, pb::IndexType_Name(index_info->type).c_str(), time, value.size(), base_expire_time_us, value.ToString(true).c_str());          
            // 使用临时Slice删除8字节前缀然后再次解析确认
            rocksdb::Slice tmp_slice_value(value);
            tmp_slice_value.remove_prefix(sizeof(uint64_t));
            TupleRecord tmp_tuple_record(tmp_slice_value);
            if (tmp_tuple_record.verification_fields(index_info->max_field_id) != 0) {
                // 去掉前缀后解析不通过，则说明该数据确实没有时间前缀
                DB_WARNING("has no prefix timestamp index id: %ld, name: %s, ttl time: %ld, value size: %lu", 
                    index_info->id, pb::IndexType_Name(index_info->type).c_str(), time, value.size());
                return base_expire_time_us;
            } else {
                // 去掉前缀解析通过则认为时间前缀是合法的，通过报警提示跟进为什么有无前缀都能解析通过
                value.remove_prefix(sizeof(uint64_t));
            }
        }
        return time;
    } else if (index_info->type == pb::I_UNIQ) {
        value.remove_prefix(sizeof(uint64_t));
        int pos = 0;
        bool succ = true;
        TableKey key(value);
        for (auto& field_info : index_info->pk_fields) {
            if (0 != key.skip_field(field_info, pos)) {
                DB_WARNING("decode index field error: field_id: %d, type: %d", 
                    field_info.id, field_info.type);
                succ = false; 
                break;
            }
            if (pos > key.size()) {
                succ = false;
                break;
            }
        }

        // 不能严格验证 TODO
        if (succ && pos == value.size()) {
            DB_DEBUG("has prefix timestamp index id: %ld, name: %s, ttl time: %ld, pos: %d, value size: %lu", 
                index_info->id, pb::IndexType_Name(index_info->type).c_str(), time, pos, value.size());
        } else {
            DB_WARNING("ttl_decode fail, index id: %ld, name: %s, ttl time: %ld, pos: %d, value size: %lu", 
                index_info->id, pb::IndexType_Name(index_info->type).c_str(), time, pos, value.size());
        }

        return time;
    } else {
        // 报警
        DB_WARNING("ttl_decode fail, index id: %ld, name: %s, ttl time: %ld, value size: %lu", 
            index_info->id, pb::IndexType_Name(index_info->type).c_str(), time, value.size());
        return time;
    }
}

int Transaction::begin(const Transaction::TxnOptions& txn_opt) {
    rocksdb::TransactionOptions rocks_txn_opt;
    if (txn_opt.dml_1pc && txn_opt.in_fsm) {
        // 1pc在状态机内执行设置较小的lock_timeout
        rocks_txn_opt.lock_timeout = FLAGS_exec_1pc_in_fsm_timeout_ms;
    } else {
        rocks_txn_opt.lock_timeout = txn_opt.lock_timeout;
    }
    return begin(rocks_txn_opt);
}

int Transaction::begin(const rocksdb::TransactionOptions& txn_opt) {
    if (nullptr == (_db = RocksWrapper::get_instance())) {
        DB_WARNING("get rocksdb instance failed");
        return -1;
    }
    if (nullptr == (_data_cf = _db->get_data_handle())) {
        DB_WARNING("get rocksdb data column family failed");
        return -1;
    }
    if (nullptr == (_meta_cf = _db->get_meta_info_handle())) {
        DB_WARNING("get rocksdb data column family failed");
        return -1;
    }
    _txn_opt = txn_opt;
    if (_txn_opt.lock_timeout == -1) {
        _txn_opt.lock_timeout = FLAGS_rocks_transaction_lock_timeout_ms +
            butil::fast_rand_less_than(FLAGS_rocks_transaction_lock_timeout_ms);
    }
    auto txn = _db->begin_transaction(_write_opt, _txn_opt);
    if (txn == nullptr) {
        DB_WARNING("start_trananction failed");
        return -1;
    }

    _txn = new myrocksdb::Transaction(txn);
    if (_pool != nullptr) {
        _use_ttl = _pool->use_ttl();
        _online_ttl_base_expire_time_us = _pool->online_ttl_base_expire_time_us();
        DB_DEBUG();
    }
    last_active_time = butil::gettimeofday_us();
    begin_time = last_active_time;
    if (_use_ttl) {
        _read_ttl_timestamp_us = last_active_time;
    }
    _in_process = true;
    _current_req_point_seq.insert(1);
    _snapshot = _db->get_snapshot();
    return 0; 
}

int Transaction::get_full_primary_key(
        rocksdb::Slice index_bytes, 
        rocksdb::Slice pk_bytes,
        IndexInfo& pk_index,
        IndexInfo& index_info, 
        char* pk_buf) {
    //remove null flag byte
    //index_bytes.remove_prefix(1);
    int pos = 0;
    for (uint32_t i = 0; i < index_info.pk_pos.size(); ++i) {
        if (index_info.pk_pos[i].first == 1) {
            memcpy(pk_buf + pos, index_bytes.data() + index_info.pk_pos[i].second, pk_index.fields[i].size);
            pos += pk_index.fields[i].size;
        } else if (index_info.pk_pos[i].first == -1) {
            memcpy(pk_buf + pos, pk_bytes.data() + index_info.pk_pos[i].second, pk_index.fields[i].size);
            pos += pk_index.fields[i].size;
        } else {
            DB_WARNING("error.");
            return -1;
        }
    }
    return 0;
}

int Transaction::fits_region_range_for_global_index(IndexInfo& pk_index, 
        IndexInfo& index_info, 
        SmartRecord record,
        bool& result) {
    MutTableKey _key;
    if (0 != _key.append_index(index_info, record.get(), -1, false)) {
        DB_FATAL("Fail to append_index, reg:%ld, tab:%ld", 
            _region_info->region_id(), index_info.id);
        return -1;
    }
    if (index_info.type == pb::I_KEY) {
        if (0 != record->encode_primary_key(index_info, _key, -1)) {
            DB_FATAL("Fail to append_pk_index, reg:%ld,tab:%ld", 
                _region_info->region_id(), index_info.id);
            return -1;
        }
    }
    result = fits_region_range(rocksdb::Slice(_key.data()), 
            rocksdb::Slice(""), 
            &_region_info->start_key(), 
            &_region_info->end_key(), 
            pk_index, 
            index_info);
    return 0;
}
int Transaction::fits_region_range_for_primary(IndexInfo& pk_index,
        SmartRecord record,
        bool& result) {
    result = true;
    MutTableKey _key;
    if (0 != _key.append_index(pk_index, record.get(), -1, false)) {
        DB_FATAL("Fail to append_index, reg:%ld, tab:%ld", 
            _region_info->region_id(), pk_index.id);
        return -1;
    }
    result = fits_region_range(rocksdb::Slice(_key.data()), 
            rocksdb::Slice(""), 
            &_region_info->start_key(), 
            &_region_info->end_key(), 
            pk_index, 
            pk_index);
    return 0;
}
// start指针为空表示不用判断region start_key
// end指针为空表示不用判断region end_key
// 传入待测试的key和value不包含regionid+table前缀
bool Transaction::fits_region_range(rocksdb::Slice key, rocksdb::Slice value,
        const std::string* start, const std::string* end, 
        IndexInfo& pk_index, IndexInfo& index_info) {
    if (!start && !end) {
        return true;
    }
    int ret1 = 1;
    int ret2 = -1;
    //全局二级索引
    if (index_info.type == pb::I_PRIMARY || index_info.is_global) {
        if (start) {
            //DB_WARNING("index_id: %ld, start_key: %s, key: %s", 
            //    index_info.id,
            //    rocksdb::Slice(*start).ToString(true).c_str(), key.ToString(true).c_str());
            ret1 = key.compare(*start);
            if (ret1 < 0) {
                return false;
            }
        }
        if (end && !end->empty()) {
            //DB_WARNING("index_id: %ld, start_key: %s, key: %s",
            //    index_info.id,
            //    rocksdb::Slice(*end).ToString(true).c_str(), key.ToString(true).c_str());
            ret2 = key.compare(*end);
        }
        //DB_WARNING("ret1: %d, ret2: %d", ret1, ret2);
    } else if (index_info.type == pb::I_UNIQ) {
        if (pk_index.length > 0 && index_info.length > 0 && index_info.overlap) {
            boost::scoped_array<char> pk_buf(new(std::nothrow)char[pk_index.length]);
            int ret = get_full_primary_key(key, value, pk_index, index_info, pk_buf.get());
            if (ret != 0) {
                return false;
            }
            rocksdb::Slice primary_key(pk_buf.get(), pk_index.length);
            if (start) {
                ret1 = primary_key.compare(*start);
                if (ret1 < 0) {
                    return false;
                }
            }
            if (end && !end->empty()) {
                ret2 = primary_key.compare(*end);
            }
        } else {
            if (start) {
                ret1 = value.compare(*start);
                if (ret1 < 0) {
                    return false;
                }
            }
            if (end && !end->empty()) {
                ret2 = value.compare(*end);
            }
        }
    } else if (index_info.type == pb::I_KEY) {
        if (pk_index.length > 0 && index_info.length > 0 && index_info.overlap) {
            rocksdb::Slice _value(key);
            if ((int)_value.size() < index_info.length) {
                DB_FATAL("index:%ld value_size:%lu len:%d", index_info.id, _value.size(), index_info.length);
                return false;
            }
            _value.remove_prefix(index_info.length);
            boost::scoped_array<char> pk_buf(new(std::nothrow)char[pk_index.length]);
            int ret = get_full_primary_key(key, _value, pk_index, index_info, pk_buf.get());
            if (ret != 0) {
                return false;
            }
            rocksdb::Slice primary_key(pk_buf.get(), pk_index.length);
            if (start) {
                ret1 = primary_key.compare(*start);
                if (ret1 < 0) {
                    return false;
                }
            }
            if (end && !end->empty()) {
                ret2 = primary_key.compare(*end);
            }
        } else {
            if (index_info.length > 0) {
                if ((int)key.size() < index_info.length) {
                    DB_FATAL("index:%ld value_size:%lu len:%d", index_info.id, key.size(), index_info.length);
                    return false;
                }
                // index_info为定长
                key.remove_prefix(index_info.length);
            } else {
                // todo, index_info为变长或有Null字段
                TableKey table_key(key);
                uint8_t null_flag = table_key.extract_u8(0);
                int pos = 1;
                for (uint32_t idx = 0; idx < index_info.fields.size(); ++idx) {
                    //flagbit为1且can_null为true同时成立，该字段才真正不存储
                    if (((null_flag >> (7 - idx)) & 0x01) 
                            && index_info.fields[idx].can_null) {
                        continue;
                    }
                    if (index_info.fields[idx].type == pb::STRING) {
                        // string 编码后面有\0
                        pos += strlen(key.data() + pos) + 1;
                    } else {
                        pos += index_info.fields[idx].size;
                    }
                }
                if ((int)key.size() < pos) {
                    DB_FATAL("index:%ld value_size:%lu pos:%d", index_info.id, key.size(), pos);
                    return false;
                }
                key.remove_prefix(pos);
            }
            if (start) {
                ret1 = key.compare(*start);
                if (ret1 < 0) {
                    return false;
                }
            }
            if (end && !end->empty()) {
                ret2 = key.compare(*end);
            }
        }
    }
    
    if (ret1 >= 0 && ret2 < 0) {
        return true;
    }
    return false;
}

//TODO: finer return status
//return -3 when region not match
int Transaction::put_primary(int64_t region, IndexInfo& pk_index, SmartRecord record,
                             std::set<int32_t>* update_fields) {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    last_active_time = butil::gettimeofday_us();
    if (_is_rolledback) {
        DB_WARNING("TransactionWarn: write a rolledback txn: %lu", _txn_id);
        return -1;
    }
    MutTableKey key;
    int ret = -1;
    key.append_i64(region).append_i64(pk_index.id);
    //encode key (not allowing prefix, do post field_clear)
    if (0 != key.append_index(pk_index, record.get(), -1, true)) {
        DB_FATAL("Fail to append_index, reg=%ld, tab=%ld", region, pk_index.id);
        return -1;
    }
    std::string value;
    if (!is_cstore()) {
        ret = record->encode(value);
        if (ret != 0) {
            DB_WARNING("encode record failed: reg=%ld, tab=%ld", region, pk_index.id);
            return -1;
        }
    } else {
        value = "";
    }
    auto res = put_kv_without_lock(key.data(), value, _write_ttl_timestamp_us);
    if (res.IsTimedOut()) {
        print_txninfo_holding_lock(key.data());        
        int64_t region_id =  _region_info != nullptr ? _region_info->region_id() : 0;
        DB_WARNING("lock failed, region_id: %ld, txn:%ld, timedout: %s, key:%s", 
                region_id, _txn_id, res.ToString().c_str(), record->debug_string().c_str());
        return -1;
    } else if (!res.ok()) {
        DB_FATAL("put primary fail, error: %s", res.ToString().c_str());
        return -1;
    }
    if (_is_separate) {
        add_kvop_put(key.data(), value, _write_ttl_timestamp_us, true);
    }
    // cstore, put non-pk columns values to db
    if (is_cstore()) {
        return put_primary_columns(key, record, update_fields);
    }
    //DB_WARNING("put primary, region_id: %ld, index_id: %ld, put_key: %s, put_value: %s",
    //    region, pk_index.id, rocksdb::Slice(key.data()).ToString(true).c_str(), rocksdb::Slice(value).ToString(true).c_str());
    return 0;
}

//TODO: finer return status
//txt->Put always return OK when using OptimisticTransactionDB
int Transaction::put_secondary(int64_t region, IndexInfo& index, SmartRecord record) {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    last_active_time = butil::gettimeofday_us();
    if (_is_rolledback) {
        DB_WARNING("TransactionWarn: write a rolledback txn: %lu", _txn_id);
        return -1;
    }
    if (index.type != pb::I_KEY && index.type != pb::I_UNIQ) {
        DB_WARNING("invalid index type, region_id: %ld, table_id: %ld, index_type:%d", region, index.id, index.type);
        return -1;
    }
    MutTableKey key;
    key.append_i64(region).append_i64(index.id);

    //encode key (not allowing prefix, no clear)
    if(0 != key.append_index(index, record.get(), -1, false)) {
        DB_FATAL("Fail to append_index, reg:%ld, tab:%ld", region, index.id);
        return -1;
    }
    rocksdb::Status res;
    MutTableKey pk;
    if (index.type == pb::I_KEY) {
        if (0 != record->encode_primary_key(index, key, -1)) {
            DB_FATAL("Fail to append_index, reg:%ld, tab:%ld", region, index.pk);
            return -1;
        }
        if (_is_separate) {
            std::string value = "";
            add_kvop_put(key.data(), value, _write_ttl_timestamp_us, index.is_global);
        }
        res = put_kv_without_lock(key.data(), "", _write_ttl_timestamp_us);
        //DB_FATAL("data:%s", str_to_hex(key.data()).c_str());
    } else if (index.type == pb::I_UNIQ) {
        //MutTableKey pk;
        if (0 != record->encode_primary_key(index, pk, -1)) {
            DB_FATAL("Fail to append_index, reg:%ld, tab:%ld", region, index.pk);
            return -1;
        }
        if (_is_separate) {
            add_kvop_put(key.data(), pk.data(), _write_ttl_timestamp_us, index.is_global);
        }
        res = put_kv_without_lock(key.data(), pk.data(), _write_ttl_timestamp_us);
    }
    if (res.IsTimedOut()) {
        print_txninfo_holding_lock(key.data());        
        int64_t region_id =  _region_info != nullptr ? _region_info->region_id() : 0;
        DB_WARNING("lock failed, region_id: %ld, txn:%ld, timedout: %s, key:%s", 
                region_id, _txn_id, res.ToString().c_str(), record->debug_string().c_str());
        return -1;
    } else if (!res.ok()) {
        DB_FATAL("put secondary fail, error: %s", res.ToString().c_str());
        return -1;
    }
    //DB_WARNING("put secondary, region_id: %ld, index_id: %ld, put_key: %s, put_value: %s",
    //    region, index.id, rocksdb::Slice(key.data()).ToString(true).c_str(), rocksdb::Slice(pk.data()).ToString(true).c_str());
    return 0;
}

int Transaction::get_for_update(const std::string& key, std::string* value) {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    rocksdb::ReadOptions read_opt;
    auto res = _txn->GetForUpdate(read_opt, _data_cf, key, value);
    if (res.ok()) {
        return 0;
    } else if (res.IsNotFound()) {
        //DB_WARNING("lock ok but key not exist");
        return -2;
    } else {
        DB_WARNING("get_for_update error: %d, %s", res.code(), res.ToString().c_str());
        return -1;
    }
}

rocksdb::Status Transaction::put_kv_without_lock(const std::string& key, const std::string& value, int64_t ttl_timestamp_us) {
    TimeCost cost;
    // support ttl
    rocksdb::Slice key_slice(key);
    rocksdb::Slice value_slices[2];
    rocksdb::SliceParts key_slice_parts(&key_slice, 1);
    rocksdb::SliceParts value_slice_parts;
    uint64_t ttl_storage = ttl_encode(ttl_timestamp_us);
    value_slices[0].data_ = reinterpret_cast<const char*>(&ttl_storage);
    value_slices[0].size_ = sizeof(uint64_t);
    value_slices[1].data_ = value.data();
    value_slices[1].size_ = value.size();
    DB_DEBUG("use_ttl:%d ttl_timestamp_us:%ld", _use_ttl, ttl_timestamp_us);
    if (_use_ttl && ttl_timestamp_us > 0) {
        value_slice_parts.parts = value_slices;
        value_slice_parts.num_parts = 2;
    } else {
        value_slice_parts.parts = value_slices + 1;
        value_slice_parts.num_parts = 1;
    }
    auto res = _txn->Put(_data_cf, key_slice_parts, value_slice_parts);
    return res;
}

int Transaction::put_kv(const std::string& key, const std::string& value, int64_t ttl_timestamp_us) {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    auto res = put_kv_without_lock(key, value, ttl_timestamp_us);
    if (!res.ok()) {
        DB_FATAL("put kv info fail, error: %s", res.ToString().c_str());

        return -1;
    }
    return 0;
}

int Transaction::delete_kv(const std::string& key) {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    auto res = _txn->Delete(_data_cf, rocksdb::Slice(key));
    if (!res.ok()) {
        DB_FATAL("delete kv info fail, error: %s", res.ToString().c_str());
        return -1;
    }
    return 0;
}

int Transaction::put_meta_info(const std::string& key, const std::string& value) {
    auto res = _txn->Put(_meta_cf, rocksdb::Slice(key), rocksdb::Slice(value));
    if (!res.ok()) {
        DB_FATAL("put meta info fail, error: %s", res.ToString().c_str());
        return -1;
    }
    return 0;
}
int Transaction::remove_meta_info(const std::string& key) {
    auto res = _txn->Delete(_meta_cf, rocksdb::Slice(key));
    if (!res.ok()) {
        DB_FATAL("remove meta info fail, error: %s", res.ToString().c_str());
        return -1;
    }
    return 0;
}
//val should be nullptr (create inside the func if the key is found)
//TODO: update return status
int Transaction::get_update_primary(
        int64_t             region, 
        IndexInfo&          pk_index,
        const SmartRecord&         key, 
        std::map<int32_t, FieldInfo*>& fields,
        GetMode             mode,
        bool                check_region,
        int64_t&             ttl_ts) {
    MutTableKey         _key;
    //full key, no prefix allowed
    if (0 != _key.append_index(pk_index, key.get(), -1, false)) {
        DB_WARNING("Fail to append_index, reg:%ld, tab:%ld", region, pk_index.id);
        return -1;
    }
    return get_update_primary(region, pk_index, TableKey(_key), 
            mode, key, fields, false, check_region, ttl_ts);
}

//TODO: update return status
int Transaction::get_update_primary(
        int64_t         region,
        IndexInfo&      pk_index,
        const TableKey& key,
        const SmartRecord&   val,
        std::map<int32_t, FieldInfo*>& fields,
        GetMode         mode,
        bool            check_region,
        int64_t&             ttl_ts) {
    return get_update_primary(region, pk_index, key, 
            mode, val, fields, true, check_region, ttl_ts);
}

//TODO: update return status
//if val == null, don't read value
//return -3 when region not match
//
int Transaction::get_update_primary(
        int64_t         region,
        IndexInfo&      pk_index,
        const TableKey& key,
        GetMode         mode,
        const SmartRecord& val,
        std::map<int32_t, FieldInfo*>& fields,
        bool            parse_key,
        bool            check_region,
        int64_t&             ttl_ts) {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    if (_region_info == nullptr) {
        DB_WARNING("no region_info");
        return -1;
    }
    last_active_time = butil::gettimeofday_us();
    if (_is_rolledback) {
        DB_WARNING("TransactionWarn: write a rolledback txn: %lu", _txn_id);
        return -1;
    }
    int ret = -1;
    if (pk_index.type != pb::I_PRIMARY) {
        DB_WARNING("invalid index type: %d", pk_index.type);
        return -1;
    }
    // region_info.end_key() == "" 是最后一个region
    if (/*_need_check_region &&*/check_region) {
        rocksdb::Slice pure_key(key.data());
        rocksdb::Slice value;
        if (!fits_region_range(pure_key, value,
            &_region_info->start_key(), &_region_info->end_key(), pk_index, pk_index)) {
            //DB_WARNING("fail to fit: %s", pure_key.ToString(true).c_str());
            return -3;
        }
    }
    MutTableKey _key;
    _key.append_i64(region).append_i64(pk_index.id).append_index(key);

    rocksdb::PinnableSlice pin_slice;
    rocksdb::Status res;
    TimeCost cost;
    if (mode == GET_ONLY) {
        //TimeCost cost;
        rocksdb::ReadOptions read_opt;
        read_opt.snapshot = _snapshot;
        res = _txn->Get(read_opt, _data_cf, _key.data(), &pin_slice);
        //DB_NOTICE("txn get time:%ld", cost.get_time());
    } else if (mode == LOCK_ONLY || mode == GET_LOCK) {
        rocksdb::ReadOptions read_opt;
        res = _txn->GetForUpdate(read_opt, _data_cf, _key.data(), &pin_slice);
        //DB_WARNING("data: %s %d", _value.c_str(), _value.size());
    } else {
        DB_WARNING("invalid GetMode: %d", mode);
        return -1;
    }

    if (res.ok()) {
        DB_DEBUG("lock ok and key exist");
        if (mode == GET_ONLY || mode == GET_LOCK) {
            rocksdb::Slice value_slice(pin_slice);
            if (_use_ttl && _read_ttl_timestamp_us > 0) {
                int64_t row_ttl_timestamp_us = ttl_decode(value_slice, &pk_index, _online_ttl_base_expire_time_us);
                if (_read_ttl_timestamp_us > row_ttl_timestamp_us) {
                    DB_DEBUG("expired _read_ttl_timestamp_us:%ld row_ttl_timestamp_us:%ld",
                            _read_ttl_timestamp_us, row_ttl_timestamp_us);
                    //expired
                    return -4;
                }
                ttl_ts = row_ttl_timestamp_us;
            }
            //TimeCost cost;
            if (!is_cstore()) {
                TupleRecord tuple_record(value_slice);
                // only decode the required field (field_ids stored in fields)
                if (0 != tuple_record.decode_fields(fields, val)) {
                    DB_WARNING("decode value failed: %ld", pk_index.id);
                    return -1;
                }
            } else {
                // cstore, get non-pk columns value from db.
                if (0 != get_update_primary_columns(_key, mode, val, nullptr, 0, nullptr, fields)) {
                    DB_WARNING("get_update_primary_columns failed: %ld", pk_index.id);
                    return -1;
                }
            }
            if (parse_key) {
                ret = val->decode_key(pk_index, key);
                if (ret != 0) {
                    DB_WARNING("decode primary index failed: %ld", pk_index.id);
                    return -1;
                }
            }
            //DB_NOTICE("decode time:%ld", cost.get_time());
            //val->merge_from(tmp_val);
        }
    } else if (res.IsNotFound()) {
        DB_DEBUG("lock ok but key not exist");
        return -2;
    } else if (res.IsBusy()) {
        DB_WARNING("lock failed, busy: %s", res.ToString().c_str());
        return -1;
    } else if (res.IsTimedOut()) {
        print_txninfo_holding_lock(_key.data());        
        int64_t region_id =  _region_info != nullptr ? _region_info->region_id() : 0;
        DB_WARNING("lock failed, region_id: %ld, txn:%ld, timedout: %s, key:%s", 
                region_id, _txn_id, res.ToString().c_str(), key.decode_start_key_string(pk_index).c_str());
        return -5;
    } else {
        DB_WARNING("unknown error: %d, %s", res.code(), res.ToString().c_str());
        return -1;
    }
    return 0;
}

int Transaction::multiget_primary(
        int64_t region,
        IndexInfo&      pk_index,
        std::vector<MutTableKey>& raw_read_keys,
        std::vector<rocksdb::Slice>& rocksdb_keys,
        int32_t     tuple_id,
        MemRowDescriptor* mem_row_desc,
        RowBatch* row_batch,
        std::map<int32_t, FieldInfo*>& fields,
        std::vector<int32_t>& field_slot,
        bool sorted_input) {
    int64_t num_keys = rocksdb_keys.size();
    std::vector<rocksdb::PinnableSlice> values(num_keys);
    std::vector<rocksdb::Status> statuses(num_keys);
    TimeCost cost;
    rocksdb::ReadOptions read_opt;
    read_opt.fill_cache = true;
    read_opt.snapshot = _snapshot;
    _txn->MultiGet(read_opt, _data_cf, rocksdb_keys, values, statuses, sorted_input);
    for (int i = 0; i < num_keys; i++) {
        if (statuses[i].ok()) {
            rocksdb::Slice value_slice(values[i]);
            if (_use_ttl && _read_ttl_timestamp_us > 0) {
                int64_t row_ttl_timestamp_us = ttl_decode(value_slice, &pk_index, _online_ttl_base_expire_time_us);
                if (_read_ttl_timestamp_us > row_ttl_timestamp_us) {
                    DB_DEBUG("expired _read_ttl_timestamp_us:%ld row_ttl_timestamp_us:%ld",
                            _read_ttl_timestamp_us, row_ttl_timestamp_us);
                    //expired
                    continue;
                }
            }
            std::unique_ptr<MemRow> mem_row = mem_row_desc->fetch_mem_row();
            if (!is_cstore()) {
                TupleRecord tuple_record(value_slice);
                // only decode the required field (field_ids stored in fields)
                if (0 != tuple_record.decode_fields(fields, &field_slot, nullptr, tuple_id, &mem_row)) {
                    DB_WARNING("decode value failed: %ld, _use_ttl:%d", pk_index.id, _use_ttl);
                    continue;
                }
            } else {
                // cstore, get non-pk columns value from db.
                if (0 != get_update_primary_columns(raw_read_keys[i], GET_ONLY, nullptr, mem_row.get(),
                                tuple_id, &field_slot, fields)) {
                    DB_WARNING("get_update_primary_columns failed: %ld", pk_index.id);
                    continue;
                }
            }
            int prefix_len = 2 * sizeof(int64_t);
            TableKey key(rocksdb_keys[i], true);
            if (0 != mem_row->decode_key(tuple_id, pk_index, field_slot, key, prefix_len)) {
                DB_WARNING("decode primary index failed: %ld", pk_index.id);
                continue;
            }
            row_batch->move_row(std::move(mem_row));
        } else if (statuses[i].IsNotFound()) {
            DB_DEBUG("lock ok but key not exist");
            continue;
        } else {
            DB_WARNING("unknown expect error: %d, %s", statuses[i].code(), statuses[i].ToString().c_str());
            return -1;
        }
    }
    return 0;
}

int Transaction::multiget_primary(
        int64_t region,
        IndexInfo&      pk_index,
        int32_t     tuple_id,
        MemRowDescriptor* mem_row_desc,
        RowBatch* row_batch,
        BatchRecord& read_records,
        std::map<int32_t, FieldInfo*>& fields,
        std::vector<int32_t>& field_slot,
        bool sorted_input) {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    if (_region_info == nullptr) {
        DB_WARNING("no region_info");
        return -1;
    }
    last_active_time = butil::gettimeofday_us();
    if (_is_rolledback) {
        DB_WARNING("TransactionWarn: write a rolledback txn: %lu", _txn_id);
        return -1;
    }
    if (pk_index.type != pb::I_PRIMARY) {
        DB_WARNING("invalid index type: %d", pk_index.type);
        return -1;
    }
    std::vector<MutTableKey> raw_read_keys;
    std::vector<rocksdb::Slice> rocksdb_keys;
    raw_read_keys.reserve(read_records.size());
    rocksdb_keys.reserve(read_records.size());
    int64_t num_keys = 0;
    while (!read_records.is_traverse_over()) {
        MutTableKey  pk_key;
        auto record = read_records.get_next();
        //full key, no prefix allowed
        if (0 != pk_key.append_index(pk_index, record.get(), -1, false)) {
            DB_WARNING("Fail to append_index, reg:%ld, tab:%ld", region, pk_index.id);
            continue;
        }
        MutTableKey  _key;
        _key.append_i64(region).append_i64(pk_index.id).append_index(pk_key);
        raw_read_keys.emplace_back(_key);
        rocksdb_keys.emplace_back(_key.data());
        ++num_keys;
    }
    read_records.clear();
    return multiget_primary(region, pk_index, raw_read_keys, rocksdb_keys, tuple_id,
        mem_row_desc, row_batch, fields, field_slot, sorted_input);
}

int Transaction::multiget_primary(
        int64_t region, 
        IndexInfo&      pk_index, 
        const std::vector<TableKeyPair*>& read_keys,
        int32_t     tuple_id,
        MemRowDescriptor* mem_row_desc,
        RowBatch* row_batch,
        std::map<int32_t, FieldInfo*>& fields,
        std::vector<int32_t>& field_slot,
        bool            check_region,
        bool sorted_input) {
   BAIDU_SCOPED_LOCK(_txn_mutex);
    if (_region_info == nullptr) {
        DB_WARNING("no region_info");
        return -1;
    }
    last_active_time = butil::gettimeofday_us();
    if (_is_rolledback) {
        DB_WARNING("TransactionWarn: write a rolledback txn: %lu", _txn_id);
        return -1;
    }
    if (pk_index.type != pb::I_PRIMARY) {
        DB_WARNING("invalid index type: %d", pk_index.type);
        return -1;
    }
    std::vector<MutTableKey> raw_read_keys;
    std::vector<rocksdb::Slice> rocksdb_keys;
    raw_read_keys.reserve(read_keys.size());
    rocksdb_keys.reserve(read_keys.size());
    int64_t num_keys = 0;
    for (auto key : read_keys) {
        auto left_key = key->left_key();
        if (check_region) {
            rocksdb::Slice pure_key(left_key.data());
            rocksdb::Slice value;
            if (!fits_region_range(pure_key, value,
                &_region_info->start_key(), &_region_info->end_key(), pk_index, pk_index)) {
                continue;
            }
        }
        MutTableKey _key;
        _key.append_i64(region).append_i64(pk_index.id).append_index(left_key);
        raw_read_keys.emplace_back(_key);
        rocksdb_keys.emplace_back(_key.data());
        ++num_keys;
    }

    return multiget_primary(region, pk_index, raw_read_keys, rocksdb_keys, tuple_id,
        mem_row_desc, row_batch, fields, field_slot, sorted_input);
}

//TODO: update return status
int Transaction::get_update_secondary(
        int64_t             region, 
        IndexInfo&          pk_index,
        IndexInfo&          index,
        const SmartRecord&  key,
        GetMode             mode,
        bool                check_region) {
    last_active_time = butil::gettimeofday_us();

    MutTableKey pk_val;
    int ret = -1;
    if (index.type != pb::I_UNIQ) {
        //DB_WARNING("invalid index type: %d", index.type);
        return -2;
    }
    MutTableKey secondary_key;
    //full key, no prefix allowed
    if (0 != secondary_key.append_index(index, key.get(), -1, false)) {
        DB_WARNING("Fail to append_index, reg:%ld, tab:%ld", region, pk_index.id);
        return -1;
    }
    int res = get_update_secondary(region, pk_index, index, secondary_key, key, mode, pk_val, check_region);
    if (res == 0 && index.type == pb::I_UNIQ && (mode == GET_ONLY || mode == GET_LOCK)) {
        int pos = 0;
        ret = key->decode_primary_key(index, pk_val, pos);
        if (ret != 0) {
            DB_WARNING("decode value failed: %ld", index.pk);
            return -1;
        }
    }
    return res;
}

int Transaction::get_update_secondary(
            int64_t             region,
            IndexInfo&          pk_index,
            IndexInfo&          index,
            const TableKey&     key,
            const SmartRecord&  val, 
            GetMode             mode,
            bool                check_region) {
    last_active_time = butil::gettimeofday_us();

    MutTableKey pk_val;
    int ret = -1;
    if (index.type != pb::I_UNIQ) {
        //DB_WARNING("invalid index type: %d", index.type);
        return -2;
    }
    int res = get_update_secondary(region, pk_index, index, key, val, mode, pk_val, check_region);
    if (res == 0 && index.type == pb::I_UNIQ && (mode == GET_ONLY || mode == GET_LOCK)) {
        int pos = 0;
        ret = val->decode_primary_key(index, pk_val, pos);
        if (ret != 0) {
            DB_WARNING("decode value failed: %ld", index.pk);
            return -1;
        }
    }
    return res;
}

int Transaction::multiget_secondary(
        int64_t         region,
        IndexInfo&      pk_index,
        IndexInfo&      index,
        const std::vector<TableKeyPair*>& read_keys,
        const SmartRecord& record_templete,
        BatchRecord& read_records,
        int32_t     tuple_id,
        MemRowDescriptor* mem_row_desc,
        RowBatch* row_batch,
        std::vector<int32_t>& field_slot,
        bool  parse_to_record,
        bool            check_region,
        bool sorted_input) {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    if (_region_info == nullptr) {
        DB_WARNING("no region_info");
        return -1;
    }
    last_active_time = butil::gettimeofday_us();
    if (_is_rolledback) {
        DB_WARNING("TransactionWarn: write a rolledback txn: %lu", _txn_id);
        return -1;
    }
    if (index.type != pb::I_UNIQ) {
        //DB_WARNING("invalid index type: %d", index.type);
        return -2;
    }
    std::vector<MutTableKey> raw_read_keys;
    std::vector<rocksdb::Slice> rocksdb_keys;
    raw_read_keys.reserve(read_keys.size());
    rocksdb_keys.reserve(read_keys.size());
    int64_t num_keys = 0;
    for (auto key : read_keys) {
        auto left_key = key->left_key();
        MutTableKey _key;
        _key.append_i64(region).append_i64(index.id).append_index(left_key);
        raw_read_keys.emplace_back(_key);
        rocksdb_keys.emplace_back(_key.data());
        ++num_keys;
    }

    std::vector<rocksdb::PinnableSlice> values(num_keys);
    std::vector<rocksdb::Status> statuses(num_keys);
    TimeCost cost;
    rocksdb::ReadOptions read_opt;
    read_opt.fill_cache = true;
    read_opt.snapshot = _snapshot;
    _txn->MultiGet(read_opt, _data_cf, rocksdb_keys, values, statuses, sorted_input);
    for (int i = 0; i < num_keys; i++) {
        if (statuses[i].ok()) {
            rocksdb::Slice value_slice(values[i]);
            if (_use_ttl && _read_ttl_timestamp_us > 0) {
                int64_t row_ttl_timestamp_us = ttl_decode(value_slice, &index, _online_ttl_base_expire_time_us);
                if (_read_ttl_timestamp_us > row_ttl_timestamp_us) {
                    //expired
                    continue;
                }
            }
            rocksdb_keys[i].remove_prefix(2 * sizeof(int64_t));
            if (!fits_region_range(rocksdb_keys[i], value_slice,
                &_region_info->start_key(), &_region_info->end_key(), pk_index, index)) {
                continue;
            }
            if (parse_to_record) {
                SmartRecord record = record_templete->clone(true);
                int pos = 0;
                MutTableKey pk_val(value_slice);
                int ret = record->decode_primary_key(index, pk_val, pos);
                if (ret != 0) {
                    DB_WARNING("decode primary key value failed: %ld", index.pk);
                    continue;
                }
                ret = record->decode_key(index, rocksdb_keys[i]);
                if (ret != 0) {
                    DB_WARNING("decode index key failed: %ld", index.id);
                    continue;
                }
                read_records.emplace_back(record);
            } else {
                std::unique_ptr<MemRow> mem_row = mem_row_desc->fetch_mem_row();
                TableKey pkey(value_slice, true);
                int pos = 0;
                if (0 != mem_row->decode_primary_key(tuple_id, index, field_slot, pkey, pos)) {
                    DB_WARNING("decode primary key failed: %ld", index.id);
                    continue;
                }
                TableKey key(rocksdb_keys[i], true);
                pos = 0;
                if (0 != mem_row->decode_key(tuple_id, index, field_slot, key, pos)) {
                    DB_WARNING("decode second key failed: %ld", index.id);
                    continue;
                }
                row_batch->move_row(std::move(mem_row));
            }
        } else if (statuses[i].IsNotFound()) {
            DB_DEBUG("lock ok but key not exist");
            continue;
        } else {
            DB_WARNING("unknown expect error: %d, %s", statuses[i].code(), statuses[i].ToString().c_str());
            return -1;
        }
    }
    return 0;
}

int Transaction::get_update_secondary(
        int64_t             region, 
        IndexInfo&          pk_index,
        IndexInfo&          index,
        const TableKey&     key,
        const SmartRecord&  val,
        GetMode             mode,
        MutTableKey&        pk,
        bool                check_region) {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    if (_region_info == nullptr) {
        DB_WARNING("no region_info");
        return -1;
    }
    last_active_time = butil::gettimeofday_us();
    if (_is_rolledback) {
        DB_WARNING("TransactionWarn: write a rolledback txn: %lu", _txn_id);
        return -1;
    }
    if (index.type != pb::I_UNIQ) {
        //DB_WARNING("invalid index type: %d", index.type);
        return -2;
    }
    MutTableKey _key;
    _key.append_i64(region).append_i64(index.id).append_index(key);

    rocksdb::PinnableSlice pin_slice;
    rocksdb::Status res;
    TimeCost cost;
    if (mode == GET_ONLY) {
        rocksdb::ReadOptions read_opt;
        read_opt.snapshot = _snapshot;
        res = _txn->Get(read_opt, _data_cf, _key.data(), &pin_slice);
    } else if (mode == LOCK_ONLY || mode == GET_LOCK) {
        rocksdb::ReadOptions read_opt;
        res = _txn->GetForUpdate(read_opt, _data_cf, _key.data(), &pin_slice);
    } else {
        DB_WARNING("invalid GetMode: %d", mode);
        return -1;
    }
    if (res.ok()) {
        DB_DEBUG("lock ok and key exist");
    } else if (res.IsNotFound()) {
        DB_DEBUG("lock ok but key not exist");
        return -2;
    } else if (res.IsBusy()) {
        DB_WARNING("lock failed, busy: %s", res.ToString().c_str());
        return -1;
    } else if (res.IsTimedOut()) {
        print_txninfo_holding_lock(_key.data());        
        int64_t region_id =  _region_info != nullptr ? _region_info->region_id() : 0;
        DB_WARNING("lock failed, region_id: %ld, txn:%ld, timedout: %s, key:%s", 
                region_id, _txn_id, res.ToString().c_str(), val->debug_string().c_str());
        return -5;
    } else {
        DB_WARNING("unknown error: %d, %s", res.code(), res.ToString().c_str());
        return -1;
    }

    rocksdb::Slice value(pin_slice);
    if (_use_ttl && _read_ttl_timestamp_us > 0) {
        int64_t row_ttl_timestamp_us = ttl_decode(value, &index, _online_ttl_base_expire_time_us);
        if (_read_ttl_timestamp_us > row_ttl_timestamp_us) {
            //expired
            return -4;
        }
    }
    pk.data().assign(value.data(), value.size());
    if (/*_need_check_region &&*/check_region && (mode == GET_ONLY || mode == GET_LOCK)) {
        rocksdb::Slice pure_key(_key.data());
        pure_key.remove_prefix(2 * sizeof(int64_t));
        if (!fits_region_range(pure_key, value,
            &_region_info->start_key(), &_region_info->end_key(), pk_index, index)) {
            return -3;
        }
    }
    return 0;
}

int Transaction::remove(int64_t region, IndexInfo& index, /*IndexInfo& pk_index,*/ 
            const SmartRecord key) {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    last_active_time = butil::gettimeofday_us();
    if (_is_rolledback) {
        DB_WARNING("TransactionWarn: write a rolledback txn: %lu", _txn_id);
        return -1;
    }
    MutTableKey _key;
    _key.append_i64(region).append_i64(index.id);
    if (0 != _key.append_index(index, key.get(), -1, false)) {
        DB_FATAL("Fail to append_index, reg:%ld,tab:%ld", region, index.id);
        return -1;
    }
    if (index.type == pb::I_KEY) {
        if (0 != key->encode_primary_key(index, _key, -1)) {
            DB_FATAL("Fail to append_pk_index, reg:%ld,tab:%ld", region, index.id);
            return -1;
        }
    }

    auto res = _txn->Delete(_data_cf, _key.data());
    DB_DEBUG("delete key=%s", str_to_hex(_key.data()).c_str());
    if (res.IsTimedOut()) {
        print_txninfo_holding_lock(_key.data());        
        int64_t region_id =  _region_info != nullptr ? _region_info->region_id() : 0;
        DB_WARNING("lock failed, region_id: %ld, txn:%ld, timedout: %s, key:%s", 
                region_id, _txn_id, res.ToString().c_str(), key->debug_string().c_str());
        return -1;
    } else if (!res.ok()) {
        DB_WARNING("delete error: code=%d, msg=%s", res.code(), res.ToString().c_str());
        return -1;
    }
    if (_is_separate) {
        add_kvop_delete(_key.data(), index.type == pb::I_PRIMARY || index.is_global);
    }
    // for cstore only, remove_columns
    if (index.type == pb::I_PRIMARY && is_cstore()) {
        return remove_columns(_key);
    }
    return 0;
}

int Transaction::remove(int64_t region, IndexInfo& index, const TableKey& key) {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    last_active_time = butil::gettimeofday_us();
    if (_is_rolledback) {
        DB_WARNING("TransactionWarn: write a rolledback txn: %lu", _txn_id);
        return -1;
    }
    MutTableKey _key;
    _key.append_i64(region).append_i64(index.id).append_index(key);
    if (index.type == pb::I_KEY) {
        // cannot append primary index
        DB_WARNING("cannot delete type KEY index");
        return -1;
    }

    auto res = _txn->Delete(_data_cf, _key.data());
    if (res.IsTimedOut()) {
        print_txninfo_holding_lock(_key.data());        
        int64_t region_id =  _region_info != nullptr ? _region_info->region_id() : 0;
        DB_WARNING("lock failed, region_id: %ld, txn:%ld, timedout: %s, key:%s", 
                region_id, _txn_id, res.ToString().c_str(), key.decode_start_key_string(index).c_str());
        return -1;
    } else if (!res.ok()) {
        DB_WARNING("delete error: code=%d, msg=%s", res.code(), res.ToString().c_str());
        return -1;
    }
    if (_is_separate) {
        add_kvop_delete(_key.data(), index.type == pb::I_PRIMARY || index.is_global);
    }
    // for cstore only, remove_columns
    if (index.type == pb::I_PRIMARY && is_cstore()) {
        return remove_columns(_key);
    }
    return 0;
}

rocksdb::Status Transaction::prepare() {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    if (_is_prepared) {
        return rocksdb::Status();
    }
    if (_is_rolledback) {
        DB_WARNING("TransactionWarn: prepare a rolledback txn: %lu", _txn_id);
        return rocksdb::Status::Expired();
    }
    last_active_time = butil::gettimeofday_us();
    auto res = _txn->Prepare();
    if (res.ok()) {
        _is_prepared = true;
        _prepare_time_us = butil::gettimeofday_us();
    }
    return res;
}

rocksdb::Status Transaction::commit() {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    last_active_time = butil::gettimeofday_us();
    if (_is_rolledback) {
        DB_WARNING("TransactionWarn: commit a rolledback txn: %lu", _txn_id);
        return rocksdb::Status::Expired();
    }
    if (_is_finished) {
        DB_WARNING("TransactionWarn: commit a finished txn: %lu", _txn_id);
        return rocksdb::Status();
    }
    if (_txn->GetName().size() != 0 && !_is_prepared) {
        DB_FATAL("TransactionError: commit a un-prepare txn: %lu", _txn_id);
        return rocksdb::Status::Aborted("commit a un-prepare txn");
    }
    auto res = _txn->Commit();
    if (res.ok()) {
        _is_finished = true;
    }
    for (auto& base : _reverse_set) {
        base->add_write_count();
    }
    return res;
}

rocksdb::Status Transaction::rollback() {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    last_active_time = butil::gettimeofday_us();
    if (_is_finished) {
        DB_WARNING("rollback a finished txn: %lu", _txn_id);
        return rocksdb::Status();
    }
    auto res = _txn->Rollback();
    if (res.ok()) {
        _is_finished = true;
        _is_rolledback = true;
    }
    return res;
}

int Transaction::set_save_point() {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    last_active_time = butil::gettimeofday_us();
    //if (_save_point_seq.empty()) {
    //    DB_WARNING("txn:%s seq_id:%d top_seq:%d",_txn->GetName().c_str(),  _seq_id, -1);
    //} else {
    //    DB_WARNING("txn:%s seq_id:%d top_seq:%d", _txn->GetName().c_str(), _seq_id, _save_point_seq.top());
    //}
    if (_save_point_seq.empty() || _save_point_seq.top() < _seq_id) {
        _txn->SetSavePoint();
        _save_point_seq.push(_seq_id);
        _save_point_increase_rows.push(num_increase_rows);
        _current_req_point_seq.insert(_seq_id);
    }
    return _seq_id;
}

void Transaction::rollback_to_point(int seq_id) {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    last_active_time = butil::gettimeofday_us();
    // 不管是否需要Rollback,_cache_plan_map对应的条目都需要erase
    // 因为_cache_plan_map会发给follower
    if (_save_point_seq.empty()) {
        //DB_WARNING("txn:%s seq_id:%d top_seq:%d", _txn->GetName().c_str(), seq_id, -1);
    } else {
        //DB_WARNING("txn:%s seq_id:%d top_seq:%d", _txn->GetName().c_str(), seq_id, _save_point_seq.top());
    }
    _need_rollback_seq.insert(seq_id);
    {
        BAIDU_SCOPED_LOCK(_cache_map_mutex);
        _cache_plan_map.erase(seq_id);
    }
    if (!_save_point_seq.empty() && _save_point_seq.top() == seq_id) {
        num_increase_rows = _save_point_increase_rows.top();
        _save_point_seq.pop();
        _save_point_increase_rows.pop();
        _txn->RollbackToSavePoint();
        // DB_WARNING("txn:%s rollback cmd seq_id: %d, num_increase_rows: %ld", 
        //     _txn->GetName().c_str(), seq_id, num_increase_rows);
    }
}

void Transaction::rollback_current_request() {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    last_active_time = butil::gettimeofday_us();
    if (_current_req_point_seq.size() == 1) {
        DB_WARNING("txn_id:%lu seq_id:%d no need rollback", _txn_id, _seq_id);
        return;
    }
    int first_seq_id = *_current_req_point_seq.begin();
    for (auto it = _current_req_point_seq.rbegin(); it != _current_req_point_seq.rend(); ++it) {
        int seq_id = *it;
        if (first_seq_id == seq_id) {
            break;
        }
        {
            BAIDU_SCOPED_LOCK(_cache_map_mutex);
            _cache_plan_map.erase(seq_id);
        }
        if (!_save_point_seq.empty() && _save_point_seq.top() == seq_id) {
            num_increase_rows = _save_point_increase_rows.top();
            _save_point_seq.pop();
            _save_point_increase_rows.pop();
            _txn->RollbackToSavePoint();
            DB_WARNING("txn:%s first_seq_id:%d rollback cmd seq_id: %d, num_increase_rows: %ld",
                _txn->GetName().c_str(), first_seq_id, seq_id, num_increase_rows);
        }
    }
    _seq_id = first_seq_id;
    _store_req.Clear();
    _is_applying = false;
    _current_req_point_seq.clear();
    _current_req_point_seq.insert(_seq_id);
}

// for cstore only, only put column which HasField in record
int Transaction::put_primary_columns(const TableKey& primary_key, SmartRecord record,
                                     std::set<int32_t>* update_fields) {
    if (_table_info.get() == nullptr) {
        DB_WARNING("no table_info");
        return -1;
    }
    int32_t table_id = primary_key.extract_i64(sizeof(int64_t));
    for (auto& field_info : _table_info->fields) {
        int32_t field_id = field_info.id;
        // skip pk fields
        if (_pri_field_ids.count(field_id) != 0) {
            continue;
        }
        // skip non reference fields when update
        if (update_fields != nullptr // an old row
                && update_fields->count(field_id) == 0) { // no need to update
            continue;
        }
        std::string value;
        bool update_by_delete_old = false;
        int ret = record->encode_field_for_cstore(field_info, value);
        // if the field value is null or default_value
        if (ret != 0) {
            if (update_fields != nullptr) { // delete when update or replace
                update_by_delete_old = true;
                DB_DEBUG("update_by_delete_old field=%d, value=%s", field_id,
                         field_info.default_expr_value.get_string().c_str());
            } else { // skip when insert
                DB_DEBUG("skip insert field=%d, value=%s", field_id,
                         field_info.default_expr_value.get_string().c_str());
                continue;
            }
        }
        MutTableKey key(primary_key);
        key.replace_i32(table_id, sizeof(int64_t));
        key.replace_i32(field_id, sizeof(int64_t) + sizeof(int32_t));
        if (update_by_delete_old) {
            auto res = _txn->Delete(_data_cf, key.data());
            DB_DEBUG("del key=%s, res=%s", str_to_hex(key.data()).c_str(),
                     res.ToString().c_str());
            if (!res.ok()) {
                return -1;
            }
            if (_is_separate) {
                add_kvop_delete(key.data(), false);
            }
            continue;
        }
        auto res = _txn->Put(_data_cf, key.data(), value);
        DB_DEBUG("put key=%s,val=%s,res=%s", str_to_hex(key.data()).c_str(),
                 record->get_value(record->get_field_by_tag(field_id)).get_string().c_str(),
                 res.ToString().c_str());
        if (!res.ok()) {
            return -1;
        }
        if (_is_separate) {
            add_kvop_put(key.data(), value, _write_ttl_timestamp_us, false);
        }
    }
    return 0;
}
// get required and non-pk field value from cstore
int Transaction::get_update_primary_columns(
        const TableKey& primary_key,
        GetMode         mode,
        const SmartRecord&  val,
        MemRow*         mem_row,
        int32_t         tuple_id,
        std::vector<int32_t>* field_slot,
        std::map<int32_t, FieldInfo*>& fields) {
    if (_table_info.get() == nullptr) {
       DB_WARNING("no table_info");
       return -1;
    }
    if (fields.size() == 0) {
        return 0;
    }
    int32_t table_id = primary_key.extract_i64(sizeof(int64_t));
    for (auto& field_info : _table_info->fields) {
        int32_t field_id = field_info.id;
        // skip pk fields
        if (_pri_field_ids.count(field_id) != 0) {
            continue;
        }
        // skip no required field
        if (fields.count(field_id) == 0) {
           continue;
        }
        MutTableKey key(primary_key);
        key.replace_i32(table_id, sizeof(int64_t));
        key.replace_i32(field_id, sizeof(int64_t) + sizeof(int32_t));
        std::string value;
        rocksdb::ReadOptions read_opt;
        if (mode == GET_ONLY) {
            read_opt.snapshot = _snapshot;
        }
        rocksdb::Status res = _txn->Get(read_opt, _data_cf, key.data(), &value);
        if (res.ok()){
            int ret = 0;
            if (val != nullptr) {
                ret = val->decode_field(field_info, value);
            } else {
                ret = mem_row->decode_field(tuple_id, (*field_slot)[field_id], field_info.type, value);
            }
            if (ret < 0) {
                DB_WARNING("decode value tuple_id: %d failed: %d", tuple_id, field_id);
                return -1;
            }
        } else if (res.IsNotFound()) {
            if (val != nullptr) {
                const FieldDescriptor* field = val->get_field_by_tag(field_id);
                val->set_value(field, field_info.default_expr_value);
            } else {
                mem_row->set_value(tuple_id, (*field_slot)[field_id], field_info.default_expr_value);
            }
        } else if (res.IsBusy()) {
            DB_WARNING("get failed, busy: %s", res.ToString().c_str());
            return -1;
        } else if (res.IsTimedOut()) {
            print_txninfo_holding_lock(key.data());
            DB_WARNING("timedout: %s", res.ToString().c_str());
            return -1;
        } else {
            DB_WARNING("unknown error: %d, %s", res.code(), res.ToString().c_str());
            return -1;
        }
    }
    return 0;
}

// for cstore only, delete non-pk columns.
int Transaction::remove_columns(const TableKey& primary_key) {
    if (_table_info.get() == nullptr) {
       DB_WARNING("no table_info");
       return -1;
    }
    int32_t table_id = primary_key.extract_i64(sizeof(int64_t));
    for (auto& field_info : _table_info->fields) {
        int32_t field_id = field_info.id;
        // skip pk fields
        if (_pri_field_ids.count(field_id) != 0) {
            continue;
        }
        MutTableKey key(primary_key);
        key.replace_i32(table_id, sizeof(int64_t));
        key.replace_i32(field_id, sizeof(int64_t) + sizeof(int32_t));
        auto res = _txn->Delete(_data_cf, key.data());
        DB_DEBUG("del key=%s, res=%s", str_to_hex(key.data()).c_str(), res.ToString().c_str());
        if (res.IsTimedOut()) {
            print_txninfo_holding_lock(key.data());        
            DB_WARNING("timedout: %s", res.ToString().c_str());
            return -1;
        } else if (!res.ok()) {
           DB_WARNING("delete error: code=%d, msg=%s", res.code(), res.ToString().c_str());
           return -1;
        }
        if (_is_separate) {
            add_kvop_delete(key.data(), false);
        }
    }
    return 0;
}

static TimeCost print_lock_last_time;
void Transaction::print_txninfo_holding_lock(const std::string& key) {
    //内部有pthread锁
    if (print_lock_last_time.get_time() > 10 * 1000 * 1000) {
        auto lock_info = _db->get_db()->GetLockStatusData();
        for (auto& it : lock_info) {
            if (it.second.key.size() == key.size() && it.second.key == key) {
                for (auto txn_id : it.second.ids) {
                    DB_WARNING("holding lock, txn_id: %lu, cf_id: %u, key_hex: %s", 
                            txn_id, it.first, str_to_hex(key).c_str());
                }
                break;
            }
        }
        print_lock_last_time.reset();
    }
}
} //nanespace baikaldb
