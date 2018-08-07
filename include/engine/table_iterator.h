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
 
#include "rocks_wrapper.h"
#include "schema_factory.h"
#include "mut_table_key.h"
#include "table_record.h"
#include "item_batch.hpp"

namespace baikaldb {
class Transaction;

//前缀的=传 [key, key],双闭区间
struct IndexRange {
    // input bound in TableRecord format
    TableRecord* left = nullptr;
    TableRecord* right = nullptr;

    // input bound in TableKey format
    TableKey* left_key = nullptr;
    TableKey* right_key = nullptr;

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

    IndexRange() {}

    IndexRange(TableRecord* _left, 
        TableRecord* _right,
        IndexInfo*  _index_info,
        IndexInfo*  _pri_info,
        pb::RegionInfo* _region_info,
        int left_cnt,
        int right_cnt,
        bool _l_open,
        bool _r_open) :
        left(_left),
        right(_right),
        index_info(_index_info),
        pri_info(_pri_info),
        region_info(_region_info),
        left_field_cnt(left_cnt),
        right_field_cnt(right_cnt),
        left_open(_l_open),
        right_open(_r_open) {}

    IndexRange(TableKey* _left, 
        TableKey* _right,
        IndexInfo*  _index_info,
        IndexInfo*  _pri_info,
        pb::RegionInfo* _region_info,
        int left_cnt,
        int right_cnt,
        bool _l_open,
        bool _r_open) :
        left_key(_left),
        right_key(_right),
        index_info(_index_info),
        pri_info(_pri_info),
        region_info(_region_info),
        left_field_cnt(left_cnt),
        right_field_cnt(right_cnt),
        left_open(_l_open),
        right_open(_r_open) {}
};

class Iterator {
public:
    Iterator(bool need_check_region, bool forward) : 
        _valid(true), 
        _need_check_region(need_check_region),
        _forward(forward) {}

    virtual ~Iterator() {
        delete _iter;
        _iter = nullptr;
    }

    virtual int open(const IndexRange& range, std::vector<int32_t>& fields, Transaction* txn = nullptr);

    virtual bool valid() const {
        return _valid;
    }

protected:
    MutTableKey             _start;
    MutTableKey             _end;
    MutTableKey             _lower_bound;
    MutTableKey             _upper_bound;

    bool                    _left_open;
    bool                    _right_open;

    bool                    _lower_is_start = false; //lower bound is region start_key
    bool                    _upper_is_end = false; // upper bound is region end_key

    int                     _lower_suffix = 0;
    int                     _upper_sufix = 0;

    bool                    _valid;
    //int64_t               _index;
    //int64_t               _pk_index;
    int64_t                 _region;
    pb::RegionInfo*          _region_info;
    IndexInfo*               _index_info;
    IndexInfo*               _pri_info;
    pb::IndexType           _idx_type;
    rocksdb::Iterator*      _iter = nullptr;
    RocksWrapper*           _db;
    SchemaFactory*          _schema;
    rocksdb::Transaction*   _txn;
    bool                    _need_check_region;
    bool                    _forward;
    rocksdb::ColumnFamilyHandle* _data_cf;
    std::vector<int32_t>    _fields;

    int _prefix_len = sizeof(int64_t) * 2;

    bool _fits_left_bound();

    bool _fits_right_bound();

    bool _fits_region();
};

class TableIterator : public Iterator {
public:
    TableIterator(bool need_check_region, bool forward, KVMode mode = KEY_VAL) : 
        Iterator(need_check_region, forward), _mode(mode) {}

    virtual ~TableIterator() {}

    int get_next(SmartRecord record);

    void set_mode(KVMode mode) {
        _mode = mode;
    }

private:
    KVMode  _mode;
};

class IndexIterator : public Iterator {
public:
    IndexIterator(bool need_check_region, bool forward) : 
        Iterator(need_check_region, forward) {}

    virtual ~IndexIterator() {}

    int get_next(SmartRecord index);

    // get the index slice and primary key slice
    // primary key slice is used for primary table query
    int get_next(rocksdb::Slice& index, rocksdb::Slice& pk) {
        return -1;
    }
};
} // end of namespace
