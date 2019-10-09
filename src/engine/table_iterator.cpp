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

#include "table_iterator.h"
#include "transaction.h"
#include "tuple_record.h"

namespace baikaldb {

TableIterator* Iterator::scan_primary(
        SmartTransaction        txn,
        const IndexRange&       range, 
        std::vector<int32_t>&   fields, 
        bool                    check_region, 
        bool                    forward) {
    txn->reset_active_time();
    TableIterator* iter = new (std::nothrow)TableIterator(check_region, forward);
    if (nullptr == iter) {
        return nullptr;
    }
    if (0 != iter->open(range, fields, txn)) {
        DB_WARNING("open table iterator failed");
        delete iter;
        return nullptr;
    }
    return iter;
}

IndexIterator* Iterator::scan_secondary(
        SmartTransaction    txn,
        const IndexRange&   range, 
        bool                check_region, 
        bool                forward) {
    txn->reset_active_time();
    IndexIterator* iter = new (std::nothrow)IndexIterator(check_region, forward);
    if (nullptr == iter) {
        return nullptr;
    }
    std::vector<int32_t> dummy;
    if (0 != iter->open(range, dummy, txn)) {
        DB_WARNING("open index iterator failed");
        delete iter;
        return nullptr;
    }
    return iter;
}

int Iterator::open(const IndexRange& range, std::vector<int32_t>& fields, SmartTransaction txn) {
    _left_open   = range.left_open;
    _right_open  = range.right_open;
    //_index       = range.index_info->id;
    //_pk_index    = range.index_info->pk;
    _index_info  = range.index_info;
    _pri_info    = range.pri_info;
    _region_info = range.region_info;
    _region      = range.region_info->region_id();
    _txn         = txn ? txn->get_txn() : nullptr;
    _fields      = fields;

    int64_t index_id = _index_info->id;
    if (pb::I_NONE == (_idx_type = _index_info->type)) {
        DB_WARNING("get index_type failed: %ld", index_id);
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
    if (nullptr == (_schema = SchemaFactory::get_instance())) {
        DB_WARNING("get schema factory failed");
        return -1;
    }
    _start.append_i64(_region).append_i64(index_id);
    _end.append_i64(_region).append_i64(index_id);

    int col_cnt = _index_info->fields.size();
    int left_secondary_field_cnt = std::min(col_cnt, range.left_field_cnt);
    int right_secondary_field_cnt = std::min(col_cnt, range.right_field_cnt);
    int left_primary_field_cnt = -1;
    int right_primary_field_cnt = -1;

    if (_idx_type == pb::I_KEY) {
        left_primary_field_cnt = std::max(0, (range.left_field_cnt - col_cnt));
        right_primary_field_cnt = std::max(0, (range.right_field_cnt - col_cnt));
    }
    if (range.left) {
        int ret = range.left->encode_key(*_index_info, _start, left_secondary_field_cnt, false);
        if (-2 == ret) {
            DB_WARNING("left key has null fields: %ld", index_id);
            _valid = false;
            return 0;
        } else if (0 != ret) {
            DB_FATAL("Fail to encode_key, table: %ld", index_id);
            return -1;
        }
        if (_idx_type == pb::I_KEY && left_primary_field_cnt > 0) {
            if (0 != range.left->encode_primary_key(*_index_info, _start, left_primary_field_cnt)) {
                DB_FATAL("Fail to append_index, reg:%ld, tab:%ld", _region, _index_info->pk);
                return -1;
            }
        }
    } else if (range.left_key) {
        _start.append_index(*range.left_key);
    } else {
        //没有指定left bound时，forward遍历从seek region+table开始，backward遍历到左边界停止
        _left_open = false;
    }

    if (range.right) {
        int ret = range.right->encode_key(*_index_info, _end, right_secondary_field_cnt, false);
        if (-2 == ret) {
            DB_WARNING("right key has null fields: %ld", index_id);
            _valid = false;
            return 0;
        } else if (0 != ret) {
            DB_FATAL("Fail to encode_key, table: %ld", index_id);
            return -1;
        }
        if (_idx_type == pb::I_KEY && right_primary_field_cnt > 0) {
            if (0 != range.right->encode_primary_key(*_index_info, _end, right_primary_field_cnt)) {
                DB_FATAL("Fail to append_index, reg:%ld, tab:%ld", _region, _index_info->pk);
                return -1;
            }
        }
    } else if (range.right_key) {
        _end.append_index(*range.right_key);
    } else {
        _right_open = false;
    }

    //取[left_key, right_key]和[start_key, end_key)的交集
    if (_need_check_region && _idx_type == pb::I_PRIMARY) {
        std::string left_key = _start.data().substr(sizeof(int64_t) * 2);
        std::string right_key = _end.data().substr(sizeof(int64_t) * 2);
        std::string lower_bound;
        std::string upper_bound;
        
        //left_key和start_key取交集
        if (left_key >= range.region_info->start_key()) {
            // region = [(1,2), (1,2,5))
            // left_key = (1,2) or (1,2,3)
            lower_bound = left_key;
        } else {
            // case1: region = [(1,2), (1,2,5)), left_key = ""
            // case2: region = [(1,2), (1,2,5)), left_key = (0)
            // case3: region = [(1,2), (1,2,5)), left_key = (1)
            // case4: region = [(1,2), (1,2,5)), left_key = (1,1)
            lower_bound = range.region_info->start_key();
            if (left_key.size() != 0) {
                int cmp = range.region_info->start_key().compare(0, left_key.size(), left_key);
                if (cmp != 0) {
                    //left_key is not a prefix of start_key (case2, 4), reset _left_open
                    _left_open = false;
                } else {
                    //left_key is a prefix of start_key (case3), keep _left_open unchange
                }
            } else {
                // case1, _left_open is always false
                _left_open = false;
            }
        }
        // DB_WARNING("region: %ld, right_key: %s, end_key:%s", _region, 
        //     rocksdb::Slice(right_key).ToString(true).c_str(),
        //     rocksdb::Slice(range.region_info->end_key()).ToString(true).c_str());
        //right_key和end_key取交集
        if (range.region_info->end_key().size() == 0 || right_key < range.region_info->end_key()) {
            //case1: region = [(1,2), (1,2,5)), right_key = (1,2)
            //case2: region = [(1,2), (1,2,5)), right_key = (1,1)
            //case3: region = [(1,2), (1,2,5)), right_key = ""
            //case4: region = [(1,2), ""), right_key = (1,1)
            if (right_key.size() != 0) {
                int cmp = range.region_info->end_key().compare(0, right_key.size(), right_key);
                if (cmp == 0) {
                    //right_key is a prefix of end_key (case1)
                    if (!_right_open) {
                        upper_bound = range.region_info->end_key();
                    } else {
                        upper_bound = right_key;
                    }
                } else {
                    upper_bound = right_key;
                }
            } else {
                upper_bound = range.region_info->end_key();
            }
        } else {
            //case-x: region = [(1,2), (1,2,5)), right_key = (1,2,5,6)
            //case-y: region = [(1,2), (1,2,5)), right_key = (1,2,6)
            upper_bound = range.region_info->end_key();
        }

        if (lower_bound > upper_bound 
                && lower_bound.compare(0, upper_bound.size(), upper_bound) != 0) {
            _valid = false;
            return 0;
        }
        if (lower_bound == range.region_info->start_key()) {
            _lower_is_start = true;
        }
        if (upper_bound == range.region_info->end_key()) {
            _upper_is_end = true;
        }

        _lower_bound.append_i64(_region).append_i64(index_id).append_index(lower_bound);
        _upper_bound.append_i64(_region).append_i64(index_id).append_index(upper_bound);
    } else {
        _lower_bound.append_index(_start);
        _upper_bound.append_index(_end);
    }
    // DB_WARNING("region: %ld, forward:%d, lower:%s, upper:%s, l_open:%d, r_open:%d, lower_is_start:%d, upper_is_end:%d",
    //      _region, _forward,
    //     rocksdb::Slice(_lower_bound.data()).ToString(true).c_str(),
    //     rocksdb::Slice(_upper_bound.data()).ToString(true).c_str(),
    //     _left_open, _right_open,
    //     _lower_is_start, _upper_is_end);

    rocksdb::ReadOptions read_options;
    if (_forward) {
        read_options.prefix_same_as_start = true;
        read_options.total_order_seek = false;
    } else {
        read_options.prefix_same_as_start = false;
        read_options.total_order_seek = true;
    }

    if (_txn != nullptr) {
        _iter = _txn->GetIterator(read_options, _data_cf);
    } else {
        _iter = _db->new_iterator(read_options, RocksWrapper::DATA_CF);
    }
    if (!_iter) {
        DB_FATAL("create iterator failed: %ld", index_id);
        return -1;
    }

    if (_forward) {
        //append an 0xFF for left open range
        if (_left_open) {
            _lower_bound.append_u64(0xFFFFFFFFFFFFFFFF);
            _lower_suffix = 8;
        }
        TimeCost cost;
        _iter->Seek(_lower_bound.data());
        DB_DEBUG("region:%ld, Seek cost:%ld", _region, cost.get_time());
        //skip left bound if _left_open
        if (_left_open) {
            while (_iter->Valid() && !_fits_left_bound() && _fits_right_bound()) {
                _iter->Next();
            }
        }
    } else {
        if (!_right_open) {
            //右闭区间时，_upper_bound有可能不是逻辑上的上边界
            //这种情况下有可能会漏数据(key为FFFF...时)，暂时没有更好的解决方案
            _upper_bound.append_u64(0xFFFFFFFFFFFFFFFF);
            _upper_bound.append_u64(0xFFFFFFFFFFFFFFFF);
            _upper_bound.append_u64(0xFFFFFFFFFFFFFFFF);
            _upper_sufix = 8;
        }
        TimeCost cost;
        _iter->SeekForPrev(_upper_bound.data());
        DB_DEBUG("region:%ld, SeekForPrev cost:%ld", _region, cost.get_time());
        while (_iter->Valid() && !_fits_right_bound() && _fits_left_bound()) {
            _iter->Prev();
        }
    }
    _valid = _iter->Valid();
    // for cstore, open iters for non-pk fields
    if (is_cstore() && _idx_type == pb::I_PRIMARY && _valid) {
        if (0 != open_columns(read_options, fields, txn)) {
            DB_FATAL("create column iterators failed: %ld", index_id);
            return -1;
        }
    }
    return 0;
}
// for cstore only
int Iterator::open_columns(const rocksdb::ReadOptions& read_options,
                           std::vector<int32_t>& fields, SmartTransaction txn) {
    std::set<int32_t>    pri_field_ids;
    for (auto& field_info : _pri_info->fields) {
        pri_field_ids.insert(field_info.id);
    }
    std::set<int32_t>    non_pk_field_ids;
    for (auto& field_id : _fields) {
        if (pri_field_ids.count(field_id) == 0) {
            non_pk_field_ids.insert(field_id);
        }
    }
    if (non_pk_field_ids.size() == 0) {
        return 0;
    }
    const TableKey& primary_key = _iter->key();
    int64_t table_id = _pri_info->id;
    for (auto& field_info : _schema->get_table_info(table_id).fields) {
        // primary key => primary column key. column key may be not exists.
        // replace field_id of format <regionid+tableid+fieldid> + pure_pk
        int32_t field_id = field_info.id;
        // skip no required field
        if (non_pk_field_ids.count(field_id) == 0) {
           continue;
        }
        MutTableKey key(primary_key);
        key.replace_i32(table_id, sizeof(int64_t));
        key.replace_i32(field_id, sizeof(int64_t) + sizeof(int32_t));
        rocksdb::Iterator* iter;
        if (_txn != nullptr) {
            iter = _txn->GetIterator(read_options, _data_cf);
        } else {
            iter = _db->new_iterator(read_options, RocksWrapper::DATA_CF);
        }
        if (!iter) {
            DB_FATAL("create iterator failed: %ld", field_id);
            return -1;
        }
        if (_forward) {
            TimeCost cost;
            iter->Seek(key.data());
            DB_DEBUG("region:%ld, field:%d, Seek cost:%ld, valid=%d",
                     _region, field_id, cost.get_time(), iter->Valid());
        } else {
        TimeCost cost;
            iter->SeekForPrev(key.data());
            DB_DEBUG("region:%ld, field:%d, SeekForPrev cost:%ld, valid=%d",
                     _region, field_id, cost.get_time(), iter->Valid());
        }
        _non_pk_fields.push_back(field_info.id);
        _non_pk_types.push_back(field_info.type);
        _column_iters.push_back(iter);
    }
    return 0;
}

bool Iterator::_fits_left_bound() {
    rocksdb::Slice key = _iter->key();
    rocksdb::Slice lower(_lower_bound.data().c_str(), _lower_bound.size() - _lower_suffix);
    rocksdb::Slice left_key(_start.data());

    if (_forward) {
        // forward iterator use prefix_extractor, skip <regionid+tableid> part
        key.remove_prefix(_prefix_len);
        lower.remove_prefix(_prefix_len);
        left_key.remove_prefix(_prefix_len);
    }
    bool fits = false;
    auto cmp = key.compare(lower);
    if (!_left_open) {
        fits = (cmp >= 0);
    } else {
        fits = (cmp > 0 && !key.starts_with(left_key));
    }
    return fits;
}

//仅用于二级索引判断，主键region在open中判断
bool Iterator::_fits_region() {
    if (!_need_check_region) {
        return true;
    }
    //check range end_key
    rocksdb::Slice key(_iter->key().data() + _prefix_len, _iter->key().size() - _prefix_len);
    bool ret = Transaction::fits_region_range(key, _iter->value(), nullptr, 
        &_region_info->end_key(), *_pri_info, *_index_info);
    return ret;
}

bool Iterator::_fits_right_bound() {
    //check range end_key
    rocksdb::Slice key = _iter->key();
    rocksdb::Slice upper(_upper_bound.data().c_str(), _upper_bound.size() - _upper_sufix);
    rocksdb::Slice right_key(_end.data());

    if (_forward) {
        // forward iterator use prefix_extractor, skip <regionid+tableid> part
        key.remove_prefix(_prefix_len);
        upper.remove_prefix(_prefix_len);
        right_key.remove_prefix(_prefix_len);
    }
    bool fits = false;
    auto cmp = key.compare(upper);
    if (_right_open) {
        fits = (cmp < 0);
    } else if (_upper_is_end) {
        if (upper.size() == 0) { //无穷大
            fits = true;
        } else {
            //上边界为region end_key, 不能越界
            fits = (cmp < 0);
        }
    } else {
        //上边界为Range right_key，前缀相同时可以越界，但不能超越end_key
        fits = (cmp <= 0 || key.starts_with(right_key));
    }
    return fits;
}
bool Iterator::_fits_prefix(rocksdb::Iterator* iter, int32_t field_id) {
    MutTableKey  prefix_key;
    prefix_key.append_i64(_region);
    if (field_id) {
        prefix_key.append_i32(_index_info->id);
        prefix_key.append_i32(field_id);
    } else {
        prefix_key.append_i64(_index_info->id);
    }
    return iter->key().starts_with(prefix_key.data());
}
bool Iterator::is_cstore() {
    if (nullptr == _schema) {
        DB_WARNING("get schema factory failed");
        return false;
     }
    return _schema->get_table_engine(_pri_info->id) == pb::ROCKSDB_CSTORE;
}
int TableIterator::get_next(SmartRecord record) {
    if (!_valid) {
        return -1;
    }
    if ((_forward && !_fits_right_bound()) || (!_forward && !_fits_left_bound())) {
        _valid = false;
        return -1;
    }

    //create a record and parse key and value
    if (VAL_ONLY == _mode || KEY_VAL == _mode) {
        if (!is_cstore()) {
            TupleRecord tuple_record(_iter->value());
            // only decode the required field (field_ids stored in fields)
            if (0 != tuple_record.decode_fields(_fields, record)) {
                DB_WARNING("decode value failed: %ld", _index_info->id);
                _valid = false;
                return -1;
            }
        } else {
            // for cstore, column value may be null.
            if (0 != get_next_columns(record)) {
                DB_WARNING("get non-pk cloumn value failed table_id: %ld", _index_info->id);
                _valid = false;
                return -1;
            }
        }
    }
    if (KEY_ONLY == _mode || KEY_VAL == _mode) {
        int pos = _prefix_len;
        TableKey key(_iter->key(), true);
        if (0 != record->decode_key(*_index_info, key, pos)) {
            DB_WARNING("decode key failed: %ld", _index_info->id);
            _valid = false;
            return -1;
        }
    }
    if (_forward) {
        _iter->Next();
    } else {
        _iter->Prev();
    }
    
    //DB_WARNING("parse:%ld add_batch:%ld nexttime:%ld", parse, add_batch,next_time);
    _valid = _valid && _iter->Valid();
    return 0;
}
// for cstore only
int TableIterator::get_next_columns(SmartRecord record) {
    if (!_fits_prefix(_iter)) {
        DB_DEBUG("not match prefix, field_id=%d, key=%s",
                 0, _iter->key().ToString(true).c_str());
        return -1;
    }
    TableKey primary_key(_iter->key(), true);
    rocksdb::Slice pk = _iter->key();
    pk.remove_prefix(_prefix_len);
    int64_t table_id = _pri_info->id;

    for (size_t i = 0; i < _non_pk_fields.size(); i++) {
        int32_t field_id = _non_pk_fields[i];
        pb::PrimitiveType field_type = _non_pk_types[i];
        rocksdb::Iterator* iter = _column_iters[i];
        const FieldDescriptor* field = record->get_field_by_tag(field_id);
        // total valid is depend on pk's _iter, column iter's valid is not necessary
        if (!iter->Valid()) {
            DB_DEBUG("iter not valid, field_id=%d, pk=%s", field_id,pk.ToString(true).c_str());
            continue;
        }
        if (!_fits_prefix(iter, field_id)) {
            DB_DEBUG("not match prefix, field_id=%d, key=%s",
                     field_id, iter->key().ToString(true).c_str());
            continue;
        }
        MutTableKey key(primary_key);
        key.replace_i32(table_id, sizeof(int64_t));
        key.replace_i32(field_id, sizeof(int64_t) + sizeof(int32_t));
        rocksdb::Slice column_key = iter->key();
        column_key.remove_prefix(_prefix_len);
        auto cmp = pk.compare(column_key);
        // when column pure key is equal to pk's pure key, get column value to record.
        if (cmp == 0) {
            std::string value(iter->value().data_, iter->value().size_);
            if (0 != record->decode_field(field_id, field_type, value)) {
                DB_WARNING("decode value failed: %d", field_id);
                return -1;
            }
            DB_DEBUG("key=%s,val=%s", iter->key().ToString(true).c_str(),
                     record->get_value(field).get_string().c_str());
        } else {
            DB_DEBUG("field_id=%d, pk=%s, key=%s, cmp=%d", field_id,
                            pk.ToString(true).c_str(),
                            column_key.ToString(true).c_str(),
                            cmp);
        }
        // as the pure key maybe not exists in column iter,
        // only need to move iter when pk are greater or equal.
        if (_forward && cmp >= 0) {
            iter->Next();
        }
        if(!_forward && cmp <= 0)  {
            iter->Prev();
        }
    }
    return 0;
}
int IndexIterator::get_next(SmartRecord index) {
    while (_valid) {
        if ((_forward && !_fits_right_bound()) || (!_forward && !_fits_left_bound())) {
            _valid = false;
            return -1;
        }
        if (!_fits_region()) {
            if (_forward) {
                _iter->Next();
            } else {
                _iter->Prev();
            }
            _valid = _valid && _iter->Valid();
            continue;
        }

        TableKey key(_iter->key(), true);
        //create a record and parse index and primary key
        //index.reset(TableRecord::new_record(_pk_table).get());
        int pos = 0;
        key.skip_region_prefix(pos);
        key.skip_table_prefix(pos);
        if (0 != index->decode_key(*_index_info, key, pos)) {
            DB_WARNING("decode secondary index failed: %ld", _index_info->id);
            _valid = false;
            return -1;
        }
        if (_idx_type == pb::I_UNIQ) {
            TableKey pkey(_iter->value(), true);
            pos = 0;
            if (0 != index->decode_primary_key(*_index_info, pkey, pos)) {
                DB_WARNING("decode primary index failed: %ld", _index_info->pk);
                _valid = false;
                return -1;
            }
        } else if (_idx_type == pb::I_KEY) {
            if (0 != index->decode_primary_key(*_index_info, key, pos)) {
                DB_WARNING("decode primary index failed: %ld, %d, %ld", 
                    _index_info->pk, pos, _iter->key().size());
                _valid = false;
                return -1;
            }
        }
        if (_forward) {
            _iter->Next();
        } else {
            _iter->Prev();
        }
        _valid = _valid && _iter->Valid();
        return 0;
    }
    return -1;
}
} // nanespace baikaldb
