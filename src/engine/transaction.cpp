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

namespace baikaldb {
DEFINE_bool(disable_wal, false, "disable rocksdb interanal WAL log, only use raft log");
// DEFINE_int32(rocks_transaction_expiration_ms, 600 * 1000, 
//         "rocksdb transaction_expiration timeout(us)");

int Transaction::begin() {
    rocksdb::TransactionOptions txn_opt;
    return begin(txn_opt);
}

int Transaction::begin(rocksdb::TransactionOptions txn_opt) {
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
    if (nullptr == (_txn = _db->begin_transaction(_write_opt, _txn_opt))) {
        DB_WARNING("start_trananction failed");
        return -1;
    }
    last_active_time = butil::gettimeofday_us();
    _snapshot = _db->get_snapshot();
    return 0; 
}

int Transaction::begin(rocksdb::Transaction* txn) {
    if (txn == nullptr) {
        DB_WARNING("txn is nullptr");
        return -1;
    }
    if (nullptr == (_db = RocksWrapper::get_instance())) {
        DB_WARNING("get rocksdb instance failed");
        return -1;
    }
    if (nullptr == (_data_cf = _db->get_data_handle())) {
        DB_WARNING("get rocksdb data column family failed");
        return -1;
    }
    _txn = txn;
    last_active_time = butil::gettimeofday_us();
    _is_prepared = true;
    _prepare_time_us = butil::gettimeofday_us();
    _snapshot = _db->get_snapshot();
    //_pool->increase_prepared();
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
            if (_value.size() < index_info.length) {
                DB_FATAL("index:%ld value_size:%d len:%d", index_info.id, _value.size(), index_info.length);
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
                if (key.size() < index_info.length) {
                    DB_FATAL("index:%ld value_size:%d len:%d", index_info.id, key.size(), index_info.length);
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
                if (key.size() < pos) {
                    DB_FATAL("index:%ld value_size:%d pos:%d", index_info.id, key.size(), pos);
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
int Transaction::put_primary(int64_t region, IndexInfo& pk_index, SmartRecord record, bool is_update) {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    last_active_time = butil::gettimeofday_us();
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
    if (_is_separate) {
        add_kvop_put(key.data(), value);
    } else {
        auto res = _txn->Put(_data_cf, key.data(), value);
        if (!res.ok()) {
            DB_FATAL("put primary fail, error: %s", res.ToString().c_str());
            return -1;
        }
        // cstore, put non-pk columns values to db
        if (is_cstore()) {
            return put_primary_columns(key, record, is_update);
        }
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
            add_kvop_put(key.data(), value);
            return 0;
        }
        res = _txn->Put(_data_cf, key.data(), "");
        //DB_FATAL("data:%s", str_to_hex(key.data()).c_str());
    } else if (index.type == pb::I_UNIQ) {
        //MutTableKey pk;
        if (0 != record->encode_primary_key(index, pk, -1)) {
            DB_FATAL("Fail to append_index, reg:%ld, tab:%ld", region, index.pk);
            return -1;
        }
        if (_is_separate) {
            add_kvop_put(key.data(), pk.data());
            return 0;
        }
        res = _txn->Put(_data_cf, key.data(), pk.data());
    }
    if (!res.ok()) {
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

int Transaction::put_kv(const std::string& key, const std::string& value) {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    auto res = _txn->Put(_data_cf, rocksdb::Slice(key), rocksdb::Slice(value));
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
        SmartRecord         key, 
        std::map<int32_t, FieldInfo*>& fields,
        GetMode             mode,
        bool                check_region) {
    MutTableKey         _key;
    //full key, no prefix allowed
    if (0 != _key.append_index(pk_index, key.get(), -1, false)) {
        DB_WARNING("Fail to append_index, reg:%ld, tab:%ld", region, pk_index.id);
        return -1;
    }
    return get_update_primary(region, pk_index, TableKey(_key), 
            mode, key, fields, false, check_region);
}

//TODO: update return status
int Transaction::get_update_primary(
        int64_t         region,
        IndexInfo&      pk_index,
        const TableKey& key,
        GetMode         mode,
        SmartRecord     val,
        std::map<int32_t, FieldInfo*>& fields,
        bool            check_region) {
    return get_update_primary(region, pk_index, key, 
            mode, val, fields, true, check_region);
}

//TODO: update return status
//if val == null, don't read value
//return -3 when region not match
int Transaction::get_update_primary(
        int64_t         region,
        IndexInfo&      pk_index,
        const TableKey& key,
        GetMode         mode,
        SmartRecord     val,
        std::map<int32_t, FieldInfo*>& fields,
        bool            parse_key,
        bool            check_region) {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    if (_region_info == nullptr) {
        DB_WARNING("no region_info");
        return -1;
    }
    last_active_time = butil::gettimeofday_us();
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

    std::string _value;
    std::string* val_ptr = nullptr;
    if (mode == GET_ONLY || mode == GET_LOCK) {
        val_ptr = &_value;
    }
    rocksdb::Status res;
    if (mode == GET_ONLY) {
        //TimeCost cost;
        rocksdb::ReadOptions read_opt;
        read_opt.snapshot = _snapshot;
        res = _txn->Get(read_opt, _data_cf, _key.data(), val_ptr);
        //DB_NOTICE("txn get time:%ld", cost.get_time());
    } else if (mode == LOCK_ONLY || mode == GET_LOCK) {
        rocksdb::ReadOptions read_opt;
        res = _txn->GetForUpdate(read_opt, _data_cf, _key.data(), val_ptr);
        //DB_WARNING("data: %s %d", _value.c_str(), _value.size());
    } else {
        DB_WARNING("invalid GetMode: %d", mode);
        return -1;
    }

    if (res.ok()) {
        DB_DEBUG("lock ok and key exist");
        if (mode == GET_ONLY || mode == GET_LOCK) {
            //TimeCost cost;
            if (!is_cstore()) {
                TupleRecord tuple_record(_value);
                // only decode the required field (field_ids stored in fields)
                if (0 != tuple_record.decode_fields(fields, val)) {
                    DB_WARNING("decode value failed: %d", pk_index.id);
                    return -1;
                }
            } else {
                // cstore, get non-pk columns value from db.
                if (0 != get_update_primary_columns(_key, mode, val, fields)) {
                    DB_WARNING("get_update_primary_columns failed: %d", pk_index.id);
                    return -1;
                }
            }
            // // 外部传来的record可能包含一些额外的信息需要保留
            // SmartRecord tmp_val = val->clone();
            // ret = tmp_val->decode(_value);
            // if (ret != 0) {
            //     DB_WARNING("decode value failed: %ld", pk_index.id);
            //     return -1;
            // }
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
        uint32_t cf_id = _data_cf->GetID();
        std::vector<uint64_t> txn_ids = _txn->GetWaitingTxns(&cf_id, &_key.data());
        if (txn_ids.size() > 0) {
            for (auto txn_id : txn_ids) {
                DB_WARNING("locked by id: %lu", txn_id);
            }
        }
        DB_WARNING("lock failed, timedout: %s", res.ToString().c_str());
        return -1;
    } else {
        DB_WARNING("unknown error: %d, %s", res.code(), res.ToString().c_str());
        return -1;
    }
    return 0;
}

//TODO: update return status
int Transaction::get_update_secondary(
        int64_t             region, 
        IndexInfo&          pk_index,
        IndexInfo&          index,
        SmartRecord         key,
        GetMode             mode,
        bool                check_region) {
    last_active_time = butil::gettimeofday_us();

    MutTableKey pk_val;
    int ret = -1;
    if (index.type != pb::I_UNIQ) {
        //DB_WARNING("invalid index type: %d", index.type);
        return -2;
    }
    int res = get_update_secondary(region, pk_index, index, key, mode, pk_val, check_region);
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
        const SmartRecord   key,
        GetMode             mode,
        MutTableKey&        pk,
        bool                check_region) {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    if (_region_info == nullptr) {
        DB_WARNING("no region_info");
        return -1;
    }
    last_active_time = butil::gettimeofday_us();
    std::string& pk_val = pk.data();
    if (index.type != pb::I_UNIQ) {
        //DB_WARNING("invalid index type: %d", index.type);
        return -2;
    }
    MutTableKey _key;
    _key.append_i64(region).append_i64(index.id);

    //full key, no prefix allowed
    if (0 != _key.append_index(index, key.get(), -1, false)) {
        DB_FATAL("Fail to append_index, region:%ld,tab:%ld", region, index.id);
        return -1;
    }
    // if (index.type == pb::I_KEY) {
    //     if (0 != _key.append_index(pk_index, key.get(), -1, false)) {
    //         DB_FATAL("Fail to append_pk_index, region:%ld,tab:%ld", region, index.id);
    //         return -1;
    //     }
    // }
    std::string* val_ptr = nullptr;
    if (mode == GET_ONLY || mode == GET_LOCK) {
        val_ptr = &pk_val;
    }
    rocksdb::Status res;
    if (mode == GET_ONLY) {
        rocksdb::ReadOptions read_opt;
        read_opt.snapshot = _snapshot;
        res = _txn->Get(read_opt, _data_cf, _key.data(), &pk_val);
    } else if (mode == LOCK_ONLY || mode == GET_LOCK) {
        rocksdb::ReadOptions read_opt;
        res = _txn->GetForUpdate(read_opt, _data_cf, _key.data(), val_ptr);
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
        DB_WARNING("lock failed, timedout: %s", res.ToString().c_str());
        return -1;
    } else {
        DB_WARNING("unknown error: %d, %s", res.code(), res.ToString().c_str());
        return -1;
    }

    if (/*_need_check_region &&*/check_region && (mode == GET_ONLY || mode == GET_LOCK)) {
        rocksdb::Slice pure_key(_key.data());
        pure_key.remove_prefix(2 * sizeof(int64_t));
        rocksdb::Slice value(pk_val);
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
    MutTableKey _key;
    last_active_time = butil::gettimeofday_us();
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
    
    if (_is_separate) {
        add_kvop_delete(_key.data());
    } else {
        auto res = _txn->Delete(_data_cf, _key.data());
        DB_DEBUG("delete key=%s", str_to_hex(_key.data()).c_str());
        if (!res.ok()) {
            DB_WARNING("delete error: code=%d, msg=%s", res.code(), res.ToString().c_str());
            return -1;
        }
        // for cstore only, remove_columns
        if (is_cstore() && index.type == pb::I_PRIMARY) {
            return remove_columns(_key);
        }
    }
    return 0;
}

int Transaction::remove(int64_t region, IndexInfo& index, const TableKey& key) {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    last_active_time = butil::gettimeofday_us();
    MutTableKey _key;
    _key.append_i64(region).append_i64(index.id).append_index(key);
    if (index.type == pb::I_KEY) {
        // cannot append primary index
        DB_WARNING("cannot delete type KEY index");
        return -1;
    }
    
    if (_is_separate) {
        add_kvop_delete(_key.data());
    } else {
        auto res = _txn->Delete(_data_cf, _key.data());
        if (!res.ok()) {
            DB_WARNING("delete error: code=%d, msg=%s", res.code(), res.ToString().c_str());
            return -1;
        }
        // for cstore only, remove_columns
        if (is_cstore() && index.type == pb::I_PRIMARY) {
            return remove_columns(_key);
        }
    }

    return 0;
}

rocksdb::Status Transaction::prepare() {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    if (_is_prepared) {
        return rocksdb::Status();
    }
    last_active_time = butil::gettimeofday_us();
    auto res = _txn->Prepare();
    if (res.ok()) {
        /*
        if (_pool && !_is_prepared) {
            _pool->increase_prepared();
            //DB_WARNING("increase_prepared: %d", _pool->num_prepared());
        }
        */
        _is_prepared = true;
        _prepare_time_us = butil::gettimeofday_us();
    }
    return res;
}

rocksdb::Status Transaction::commit() {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    last_active_time = butil::gettimeofday_us();
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
        /*
        if (_pool && _is_prepared && !_is_finished) {
            _pool->decrease_prepared();
        }
        */
        _is_finished = true;
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
        /*
        if (_pool && _is_prepared && !_is_finished) {
            _pool->decrease_prepared();
        }
        */
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
    }
    return _seq_id;
}

void Transaction::rollback_to_point(int seq_id) {
    BAIDU_SCOPED_LOCK(_txn_mutex);
    last_active_time = butil::gettimeofday_us();
    // 不管是否需要Rollback,_cache_plan_map对应的条目都需要erase
    // 因为_cache_plan_map会发给follower
    if (_save_point_seq.empty()) {
        DB_WARNING("txn:%s seq_id:%d top_seq:%d", _txn->GetName().c_str(), seq_id, -1);
    } else {
        DB_WARNING("txn:%s seq_id:%d top_seq:%d", _txn->GetName().c_str(), seq_id, _save_point_seq.top());
    }
    _cache_plan_map.erase(seq_id);
    if (!_save_point_seq.empty() && _save_point_seq.top() == seq_id) {
        num_increase_rows = _save_point_increase_rows.top();
        _save_point_seq.pop();
        _save_point_increase_rows.pop();
        _txn->RollbackToSavePoint();
        DB_WARNING("txn:%s rollback cmd seq_id: %d, num_increase_rows: %ld", 
            _txn->GetName().c_str(), seq_id, num_increase_rows);
    }
//    if (!_save_point_seq.empty() && _save_point_seq.top() >= seq_id) {
//        DB_FATAL("TransactionError: need to rollback to earlier point: top: %d, seq_id: %d",
//            _save_point_seq.top(), seq_id);
//    }
}
// for cstore only, only put column which HasField in record
int Transaction::put_primary_columns(const TableKey& primary_key, SmartRecord record, bool is_update) {
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
        std::string value;
        bool update_by_delete_old = false;
        // if the field value is null or default_value
        int ret = record->encode_field(field_info, value);
        if (ret != 0) {
            if (is_update && ret == -3) { // delete old when update default
                update_by_delete_old = true;
            } else { // skip null or default_value fields when insert
                DB_DEBUG("no value for field=%d", field_id);
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
            continue;
        }
        auto res = _txn->Put(_data_cf, key.data(), value);
        DB_DEBUG("put key=%s,val=%s,res=%s", str_to_hex(key.data()).c_str(),
                 record->get_value(record->get_field_by_tag(field_id)).get_string().c_str(),
                 res.ToString().c_str());
        if (!res.ok()) {
            return -1;
        }
    }
    return 0;
}
// get required and non-pk field value from cstore
int Transaction::get_update_primary_columns(
        const TableKey& primary_key,
        GetMode         mode,
        SmartRecord     val,
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
            const FieldDescriptor* field = val->get_field_by_tag(field_id);
            if (0 != val->decode_field(field_info, value)) {
                DB_WARNING("decode value failed: %d", field_id);
                return -1;
            }
            DB_DEBUG("get key=%s,val=%s,res=%s", str_to_hex(key.data()).c_str(),
                     val->get_value(field).get_string().c_str(), res.ToString().c_str());
        } else if (res.IsNotFound()) {
            const FieldDescriptor* field = val->get_field_by_tag(field_id);
            val->set_value(field, field_info.default_expr_value);
            DB_DEBUG("cell not exist, default value: %s",
                     field_info.default_value.c_str());
        } else if (res.IsBusy()) {
            DB_WARNING("get failed, busy: %s", res.ToString().c_str());
            return -1;
        } else if (res.IsTimedOut()) {
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
        if (!res.ok()) {
           DB_WARNING("delete error: code=%d, msg=%s", res.code(), res.ToString().c_str());
           return -1;
        }
    }
    return 0;
}
} //nanespace baikaldb
