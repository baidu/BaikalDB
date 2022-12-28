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

#include <bitset>
#include "rocks_wrapper.h"
#include "schema_factory.h"
#include "mut_table_key.h"
#include "table_key.h"
#include "table_record.h"
#include "item_batch.hpp"
#include "my_rocksdb.h"

namespace baikaldb {
class Transaction;
class TableIterator;
class IndexIterator;
typedef std::shared_ptr<Transaction> SmartTransaction;
typedef std::bitset<ROW_BATCH_CAPACITY> FiltBitSet;

//前缀的=传 [key, key],双闭区间
struct IndexRange {
    // input bound in TableRecord format
    TableRecord* left = nullptr;
    TableRecord* right = nullptr;

    // input bound in TableKey format
    TableKey left_key;
    TableKey right_key;

    // region & table & index info for the current scan
    IndexInfo*  index_info = nullptr;
    IndexInfo*  pri_info   = nullptr;
    pb::RegionInfo* region_info = nullptr;

    // left and right bound field count
    int left_field_cnt = 0;
    int right_field_cnt = 0;

    // left/right bound included/exluded
    bool left_open = false;
    bool right_open = false;

    bool like_prefix = false;

    IndexRange() {}

    IndexRange(TableRecord* _left, 
        TableRecord* _right,
        IndexInfo*  _index_info,
        IndexInfo*  _pri_info,
        pb::RegionInfo* _region_info,
        int left_cnt,
        int right_cnt,
        bool _l_open,
        bool _r_open,
        bool _like_prefix) :
        left(_left),
        right(_right),
        index_info(_index_info),
        pri_info(_pri_info),
        region_info(_region_info),
        left_field_cnt(left_cnt),
        right_field_cnt(right_cnt),
        left_open(_l_open),
        right_open(_r_open),
        like_prefix(_like_prefix) {}
    
    IndexRange(const MutTableKey& _left,
        const MutTableKey& _right,
        IndexInfo*  _index_info,
        IndexInfo*  _pri_info,
        pb::RegionInfo* _region_info,
        int left_cnt,
        int right_cnt,
        bool _l_open,
        bool _r_open,
        bool _like_prefix) :
        left_key(_left),
        right_key(_right),
        index_info(_index_info),
        pri_info(_pri_info),
        region_info(_region_info),
        left_field_cnt(left_cnt),
        right_field_cnt(right_cnt),
        left_open(_l_open),
        right_open(_r_open),
        like_prefix(_like_prefix) {}
};

class Iterator {
public:
    Iterator(bool need_check_region, bool forward) : 
        _need_check_region(need_check_region),
        _forward(forward) {}

    virtual ~Iterator() {
        delete _iter;
        _iter = nullptr;
        for (auto& iter : _column_iters) {
            delete iter.second;
            iter.second = nullptr;
        }
    }

    virtual int open(const IndexRange& range, std::map<int32_t, FieldInfo*>& fields, 
            std::vector<int32_t>& field_slot,
            SmartTransaction txn = nullptr);

    virtual int open_columns(std::map<int32_t, FieldInfo*>& fields, SmartTransaction txn = nullptr);

    virtual bool valid() const {
        return _valid;
    }

    static TableIterator* scan_primary(
        SmartTransaction        txn,
        const IndexRange&       range, 
        std::map<int32_t, FieldInfo*>&   fields, 
        std::vector<int32_t>& field_slot,
        bool                    check_region, 
        bool                    forward);

    static IndexIterator* scan_secondary(
        SmartTransaction    txn,
        const IndexRange&   range, 
        std::vector<int32_t>& field_slot,
        bool                check_region, 
        bool                forward);

    bool is_cstore() {
        return _is_cstore;
    }
    void reset_primary_keys() {
        _primary_keys.clear();
        _primary_keys.reserve(ROW_BATCH_CAPACITY);
    }

protected:
    MutTableKey             _start;
    MutTableKey             _end;
    MutTableKey             _lower_bound;
    MutTableKey             _upper_bound;
    rocksdb::Slice          _lower_bound_slice;
    rocksdb::Slice          _upper_bound_slice;

    bool                    _left_open;
    bool                    _right_open;

    bool                    _lower_is_start = false; //lower bound is region start_key
    bool                    _upper_is_end = false; // upper bound is region end_key

    int                     _lower_suffix = 0;
    int                     _upper_sufix = 0;

    bool                    _valid = true;
    bool                    _use_ttl = false;
    bool                    _is_cstore = false;
    int64_t                 _read_ttl_timestamp_us = 0;
    int64_t                 _online_ttl_base_expire_time_us = 0; // 存量数据过期时间，仅online TTL的表使用
    int64_t                 _region;
    pb::RegionInfo*         _region_info;
    IndexInfo*              _index_info;
    IndexInfo*              _pri_info;
    pb::IndexType           _idx_type;
    myrocksdb::Iterator*    _iter = nullptr;
    RocksWrapper*           _db;
    SchemaFactory*          _schema;
    myrocksdb::Transaction* _txn = nullptr;
    bool                    _need_check_region;
    bool                    _forward;
    rocksdb::ColumnFamilyHandle* _data_cf;
    std::map<int32_t, FieldInfo*>    _fields;
    std::vector<int32_t> _field_slot;

    std::vector<std::string>                _primary_keys;
    std::map<int32_t, myrocksdb::Iterator*>   _column_iters;

    int _prefix_len = sizeof(int64_t) * 2;

    bool _fits_left_bound(const rocksdb::Slice& key);

    bool _fits_right_bound(const rocksdb::Slice& key);

    bool _fits_region(rocksdb::Slice key, rocksdb::Slice value);

    bool _fits_prefix(const rocksdb::Slice& key, int32_t field_id); // cstore
};

class TableIterator : public Iterator {
public:
    TableIterator(bool need_check_region, bool forward, KVMode mode = KEY_VAL) : 
        Iterator(need_check_region, forward), _mode(mode) {}

    virtual ~TableIterator() {}

    int get_next(SmartRecord& record) {
        return get_next_internal(&record, 0, nullptr);
    }
    int get_next(int32_t tuple_id, std::unique_ptr<MemRow>& mem_row) {
        return get_next_internal(nullptr, tuple_id, &mem_row);
    }

    void set_mode(KVMode mode) {
        _mode = mode;
    }
    int get_column(int32_t tuple_id, const FieldInfo& field, const FiltBitSet* filter, RowBatch* batch);
private:
    int get_next_internal(SmartRecord* record, int32_t tuple_id, std::unique_ptr<MemRow>* mem_row);
    KVMode  _mode;
};

class IndexIterator : public Iterator {
public:
    IndexIterator(bool need_check_region, bool forward) : 
        Iterator(need_check_region, forward) {}

    virtual ~IndexIterator() {}

    int get_next(SmartRecord& record) {
        return get_next_internal(&record, 0, nullptr);
    }
    int get_next(int32_t tuple_id, std::unique_ptr<MemRow>& mem_row) {
        return get_next_internal(nullptr, tuple_id, &mem_row);
    }

    // get the index slice and primary key slice
    // primary key slice is used for primary table query
    int get_next(rocksdb::Slice& index, rocksdb::Slice& pk) {
        return -1;
    }
private:
    int get_next_internal(SmartRecord* record, int32_t tuple_id, std::unique_ptr<MemRow>* mem_row);
};
} // end of namespace
