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

#include "table_iterator.h"
#include "transaction.h"
#include "tuple_record.h"

namespace baikaldb {
DEFINE_bool(cstore_scan_fill_cache, true, "cstore_scan_fill_cache");
DEFINE_bool(scan_fill_cache, true, "iterator_prefix_same_as_start");

TableIterator* Iterator::scan_binlog_primary(
        const IndexRange&       range, 
        std::map<int32_t, FieldInfo*>&   fields, 
        std::vector<int32_t>& field_slot,
        bool is_offline_binlog) {
    // check_region=false, forward=true, txn=nullptr
    TableIterator* iter = new (std::nothrow)TableIterator(false, true);
    if (nullptr == iter) {
        return nullptr;
    }
    iter->set_is_offline_binlog(is_offline_binlog);
    if (0 != iter->open(range, fields, field_slot, nullptr)) {
        DB_WARNING("open table iterator failed");
        delete iter;
        return nullptr;
    }
    return iter;
}

TableIterator* Iterator::scan_primary(
        SmartTransaction        txn,
        const IndexRange&       range, 
        std::map<int32_t, FieldInfo*>&   fields, 
        std::vector<int32_t>& field_slot,
        bool                    check_region, 
        bool                    forward) {
    if (txn != nullptr) {
        txn->reset_active_time();
    }
    TableIterator* iter = new (std::nothrow)TableIterator(check_region, forward);
    if (nullptr == iter) {
        return nullptr;
    }
    if (0 != iter->open(range, fields, field_slot, txn)) {
        DB_WARNING("open table iterator failed");
        delete iter;
        return nullptr;
    }
    return iter;
}

IndexIterator* Iterator::scan_secondary(
        SmartTransaction    txn,
        const IndexRange&   range, 
        std::vector<int32_t>& field_slot,
        bool                check_region, 
        bool                forward) {
    if (txn != nullptr) {
        txn->reset_active_time();
    }
    IndexIterator* iter = new (std::nothrow)IndexIterator(check_region, forward);
    if (nullptr == iter) {
        return nullptr;
    }
    std::map<int32_t, FieldInfo*> dummy;
    if (0 != iter->open(range, dummy, field_slot, txn)) {
        DB_WARNING("open index iterator failed");
        delete iter;
        return nullptr;
    }
    return iter;
}

int Iterator::open(const IndexRange& range, std::map<int32_t, FieldInfo*>& fields, 
        std::vector<int32_t>& field_slot, SmartTransaction txn) {
    _left_open   = range.left_open;
    _right_open  = range.right_open;
    //_index       = range.index_info->id;
    //_pk_index    = range.index_info->pk;
    _index_info  = range.index_info;
    _pri_info    = range.pri_info;
    _region_info = range.region_info;
    _region      = range.region_info->region_id();
    _fields      = fields;
    _field_slot  = field_slot;
    if (txn != nullptr) {
        _use_ttl = txn->use_ttl();
        _read_ttl_timestamp_us = txn->read_ttl_timestamp_us();
        _online_ttl_base_expire_time_us = txn->online_ttl_base_expire_time_us();
        _txn = txn->get_txn();
    }
    bool like_prefix = range.like_prefix;

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
    _is_cstore = _schema->get_table_engine(_pri_info->id) == pb::ROCKSDB_CSTORE;

    _start.append_i64(_region).append_i64(index_id);
    _end.append_i64(_region).append_i64(index_id);

    int col_cnt = _index_info->fields.size();
    int left_secondary_field_cnt = std::min(col_cnt, range.left_field_cnt);
    int right_secondary_field_cnt = std::min(col_cnt, range.right_field_cnt);
    int left_primary_field_cnt = -1;
    int right_primary_field_cnt = -1;

    if (_idx_type == pb::I_KEY) {
        // region_id+index_id+索引字段+主键字段，这里考虑了把主键字段编码进来的情况，baikaldb侧目前未实现
        left_primary_field_cnt = std::max(0, (range.left_field_cnt - col_cnt));
        right_primary_field_cnt = std::max(0, (range.right_field_cnt - col_cnt));
    }
    if (range.left) {
        int ret = range.left->encode_key(*_index_info, _start, left_secondary_field_cnt, false, like_prefix);
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
    } else if (range.left_key.size() > 0) {
        _start.append_index(range.left_key);
    } else {
        //没有指定left bound时，forward遍历从seek region+table开始，backward遍历到左边界停止
        _left_open = false;
    }

    if (range.right) {
        int ret = range.right->encode_key(*_index_info, _end, right_secondary_field_cnt, false, like_prefix);
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
    } else if (range.right_key.size() > 0) {
        _end.append_index(range.right_key);
    } else {
        _right_open = false;
    }

    //取[left_key, right_key]和[start_key, end_key)的交集
    if (_need_check_region && (_idx_type == pb::I_PRIMARY || _index_info->is_global)) {
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
    if (_left_open) {
        _lower_bound.append_u64(UINT64_MAX);
        _lower_suffix = 8;
    }
    _lower_bound_slice = _lower_bound.data();
    if (!_right_open) {
        //右闭区间时，_upper_bound有可能不是逻辑上的上边界
        //这种情况下有可能会漏数据(key为FFFF...时)，暂时没有更好的解决方案
        _upper_bound.append_u64(UINT64_MAX);
        _upper_bound.append_u64(UINT64_MAX);
        _upper_bound.append_u64(UINT64_MAX);
        _upper_sufix = 24;
    }
    _upper_bound_slice = _upper_bound.data();
    // 通过rocksdb来过滤边界
    // TODO 自己判断边界是否可以省略
    if (_forward) {
        read_options.prefix_same_as_start = true;
        read_options.total_order_seek = false;
        read_options.iterate_upper_bound = &_upper_bound_slice;
        if (_is_cstore) {
            read_options.fill_cache = FLAGS_cstore_scan_fill_cache;
        }
    } else {
        read_options.prefix_same_as_start = false;
        read_options.total_order_seek = true;
        read_options.iterate_lower_bound = &_lower_bound_slice;
        if (_is_cstore) {
            read_options.fill_cache = FLAGS_cstore_scan_fill_cache;
        }
    }


    if (txn != nullptr) {
        // 查询请求txn都不为空
        read_options.snapshot = txn->get_snapshot();
        myrocksdb::Transaction* my_txn = txn->get_txn();
        bool same_prefix = false;
        if (my_txn->use_cold_db()) {
            same_prefix = _db->cold_data_cf_same_prefix(_lower_bound_slice, _upper_bound_slice);
        } else {
            same_prefix = _db->data_cf_same_prefix(_lower_bound_slice, _upper_bound_slice);
        }

        if (_forward && same_prefix) {
            read_options.prefix_same_as_start = true;
            read_options.total_order_seek = false;
        } else {
            read_options.prefix_same_as_start = false;
            read_options.total_order_seek = true;
        }
        read_options.fill_cache = FLAGS_scan_fill_cache;
        _iter = new myrocksdb::Iterator(my_txn->GetIterator(read_options, _data_cf));
    } else {
        if (!_is_offline_binlog) {
            _iter = new myrocksdb::Iterator(_db->new_iterator(read_options, RocksWrapper::DATA_CF));
        } else {
            _iter = new myrocksdb::Iterator(_db->new_cold_iterator(read_options, RocksWrapper::COLD_DATA_CF));
        }
    }
    if (!_iter) {
        DB_FATAL("create iterator failed: %ld", index_id);
        return -1;
    }

    if (_forward) {
        //append an 0xFF for left open range
        TimeCost cost;
        _iter->Seek(_lower_bound.data());
        DB_DEBUG("region:%ld, Seek cost:%ld", _region, cost.get_time());
        //skip left bound if _left_open
        if (_left_open) {
            while (_iter->Valid() && !_fits_left_bound(_iter->key()) && _fits_right_bound(_iter->key())) {
                //DB_WARNING("open, left bound filter, region_id: %ld", _region);
                _iter->Next();
            }
        }
    } else {
        TimeCost cost;
        _iter->SeekForPrev(_upper_bound.data());
        //DB_DEBUG("region:%ld, SeekForPrev cost:%ld", _region, cost.get_time());
        while (_iter->Valid() && !_fits_right_bound(_iter->key()) && _fits_left_bound(_iter->key())) {
            _iter->Prev();
        }
    }
    _valid = _iter->Valid();
    // for cstore, open iters for non-pk fields
    if (_is_cstore && _idx_type == pb::I_PRIMARY && _valid) {
       if (0 != open_columns(fields, txn)) {
           DB_FATAL("create column iterators failed: %ld", index_id);
           return -1;
       }
    }
    return 0;
}

// for cstore only
int Iterator::open_columns(std::map<int32_t, FieldInfo*>& fields, SmartTransaction txn) {
    rocksdb::ReadOptions read_options;
    if (_forward) {
        read_options.prefix_same_as_start = true;
        read_options.total_order_seek = false;
        read_options.fill_cache = FLAGS_cstore_scan_fill_cache;
    } else {
        read_options.prefix_same_as_start = false;
        read_options.total_order_seek = true;
        read_options.fill_cache = FLAGS_cstore_scan_fill_cache;
    }
    std::set<int32_t>    pri_field_ids;
    for (auto& field_info : _pri_info->fields) {
        pri_field_ids.insert(field_info.id);
    }
    const TableKey& primary_key = _iter->key();
    int64_t table_id = _pri_info->id;
    for (auto& pair : fields) {
        // primary key => primary column key. column key may be not exists.
        // replace field_id of format <regionid+tableid+fieldid> + pure_pk
        int32_t field_id = pair.first;
        MutTableKey key(primary_key);
        key.replace_i32(table_id, sizeof(int64_t));
        key.replace_i32(field_id, sizeof(int64_t) + sizeof(int32_t));
        myrocksdb::Iterator* iter;
        if (_txn != nullptr) {
            read_options.snapshot = txn->get_snapshot();
            iter = new myrocksdb::Iterator(_txn->GetIterator(read_options, _data_cf));
        } else {
            iter = new myrocksdb::Iterator(_db->new_iterator(read_options, RocksWrapper::DATA_CF));
        }
        if (!iter) {
            DB_FATAL("create iterator failed: %d", field_id);
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
        _column_iters[field_id] = iter;
//        pair.second->can_null && pair.second->default_expr_value.is_null();
    }
    return 0;
}

bool Iterator::_fits_left_bound(const rocksdb::Slice& key) {
    rocksdb::Slice lower(_lower_bound.data().c_str(), _lower_bound.size() - _lower_suffix);
    rocksdb::Slice left_key(_start.data());

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
//必须处理过ttl
bool Iterator::_fits_region(rocksdb::Slice key, rocksdb::Slice value) {
    if (!_need_check_region) {
        return true;
    }
    //check range end_key
    key.remove_prefix(_prefix_len);
    // 按理说start_key不需要判断，之前global index
    // 有bug，分裂后put_row有在start_key之前的记录写入
    bool ret = Transaction::fits_region_range(key, value, &_region_info->start_key(), 
        &_region_info->end_key(), *_pri_info, *_index_info);
    return ret;
}

bool Iterator::_fits_right_bound(const rocksdb::Slice& key) {
    //check range end_key
    rocksdb::Slice upper(_upper_bound.data().c_str(), _upper_bound.size() - _upper_sufix);
    rocksdb::Slice right_key(_end.data());
    bool fits = false;
    auto cmp = key.compare(upper);
    if (_right_open) {
        fits = (cmp < 0);
    } else if (_upper_is_end) {
        if (upper.size() == (size_t)_prefix_len) {//无穷大  
            // https://github.com/facebook/rocksdb/issues/5100
            // rocksdb事务前缀搜索有bug
            fits = key.starts_with(upper);
        } else {
            //上边界为region end_key, 不能越界
            fits = (cmp < 0);
        }
    } else {
        //上边界为Range right_key，前缀相同时可以越界，但不能超越end_key
        fits = (cmp <= 0 || key.starts_with(right_key));
    }
    //DB_WARNING("_fits_right_bound: %d", fits);
    return fits;
}
bool Iterator::_fits_prefix(const rocksdb::Slice& key, int32_t field_id) {
    MutTableKey prefix_key;
    prefix_key.append_i64(_region);
    prefix_key.append_i32(_index_info->id);
    prefix_key.append_i32(field_id);
    return key.starts_with(prefix_key.data());
}

int TableIterator::get_next_internal(SmartRecord* record, int32_t tuple_id, std::unique_ptr<MemRow>* mem_row) {
    if (!_valid) {
        return -1;
    }
    rocksdb::Slice iter_key = _iter->key();
    if ((_forward && !_fits_right_bound(iter_key)) || (!_forward && !_fits_left_bound(iter_key))) {
        _valid = false;
        return -1;
    }
    rocksdb::Slice value_slice;
    if (_use_ttl || _mode != KEY_ONLY) {
        value_slice = _iter->value();
    }
    if (_use_ttl) {
        int64_t row_ttl_timestamp_us = ttl_decode(value_slice, _index_info, _online_ttl_base_expire_time_us);
        if (_read_ttl_timestamp_us > row_ttl_timestamp_us) {
            //expired
            if (_forward) {
                _iter->Next();
            } else {
                _iter->Prev();
            }
            _valid = _valid && _iter->Valid();
            return -4;
        }
    }
    //create a record and parse key and value
    if (VAL_ONLY == _mode || KEY_VAL == _mode) {
        if (!_is_cstore) {
            TupleRecord tuple_record(value_slice);
            // only decode the required field (field_ids stored in fields)
            if (0 != tuple_record.decode_fields(_fields, &_field_slot, record, tuple_id, mem_row)) {
                DB_WARNING("decode value failed: %ld, _use_ttl:%d", _index_info->id, _use_ttl);
                _valid = false;
                return -1;
            }
        } else {
            _primary_keys.push_back(std::string(_iter->key().data() + _prefix_len,
                                                _iter->key().size() - _prefix_len));
        }
    }
    if (KEY_ONLY == _mode || KEY_VAL == _mode) {
        int pos = _prefix_len;
        TableKey key(iter_key, true);
        if (record != nullptr) {
            if (0 != (*record)->decode_key(*_index_info, key, pos)) {
                DB_WARNING("decode key failed: %ld", _index_info->id);
                _valid = false;
                return -1;
            } 
        } else {
            if (0 != (*mem_row)->decode_key(tuple_id, *_index_info, _field_slot, key, pos)) {
                DB_WARNING("decode key failed: %ld", _index_info->id);
                _valid = false;
                return -1;
            }
        }
    }
    if (_forward) {
        _iter->Next();
    } else {
        _iter->Prev();
    }
    _valid = _valid && _iter->Valid();
    
    //DB_WARNING("parse:%ld add_batch:%ld nexttime:%ld", parse, add_batch,next_time);
    return 0;
}

int TableIterator::get_column(int32_t tuple_id, const FieldInfo& field, const FiltBitSet* filter, RowBatch* batch) {

    int32_t field_id = field.id;
    int32_t slot_id = _field_slot[field_id];
    if (slot_id == 0) {
        return -1;
    }
    myrocksdb::Iterator* iter = _column_iters[field_id];
    MutTableKey prefix_key;
    prefix_key.append_i64(_region);
    prefix_key.append_i32(_pri_info->id);
    prefix_key.append_i32(field_id);

    int filter_num = 0;
    for (size_t i = 0; i < batch->size(); ++i) {
        if (filter != nullptr && filter->test(i)) {
            filter_num++;
            continue;
        }
        if (filter_num > 63) {
            MutTableKey key;
            key.append_index(prefix_key);
            key.append_index(_primary_keys[i]);
            if (_forward) {
                iter->Seek(key.data());
            } else {
                iter->SeekForPrev(key.data());
            }
        }
        std::unique_ptr<MemRow>& mem_row = batch->get_row(i);
        rocksdb::Slice primary_key = _primary_keys[i];
        int32_t cmp = 0;
        while (true) {
            if (!iter->Valid()) {
                mem_row->set_value(tuple_id, slot_id, field.default_expr_value);
                break;
            }
            rocksdb::Slice column_key = iter->key();
            if (!column_key.starts_with(prefix_key.data())){
                mem_row->set_value(tuple_id, slot_id, field.default_expr_value);
                DB_DEBUG("not match prefix, field_id=%d, key=%s, default_value=%s",
                         field_id, iter->key().ToString(true).c_str(), field.default_value.c_str());
                break;
            }
            column_key.remove_prefix(_prefix_len);
            cmp = primary_key.compare(column_key);
            if (_forward) {
                if (cmp == 0) {
                    mem_row->decode_field(tuple_id, slot_id, field.type, iter->value());
                    iter->Next();
                    break;
                } else if (cmp < 0) {
                    mem_row->set_value(tuple_id, slot_id, field.default_expr_value);
                    break;
                } else {
                    iter->Next();
                }
            } else {
                if (cmp == 0) {
                    mem_row->decode_field(tuple_id, slot_id, field.type, iter->value());
                    iter->Prev();
                    break;
                } else if (cmp > 0) {
                    mem_row->set_value(tuple_id, slot_id, field.default_expr_value);
                    break;
                } else {
                    iter->Prev();
                }
            }
        }
        filter_num = 0;
    }
    return 0;
}

int IndexIterator::get_next_internal(SmartRecord* record, int32_t tuple_id, std::unique_ptr<MemRow>* mem_row) {
    while (_valid) {
        rocksdb::Slice iter_key = _iter->key();
        if ((_forward && !_fits_right_bound(iter_key)) || (!_forward && !_fits_left_bound(iter_key))) {
            _valid = false;
            //DB_WARNING("get next, right bound filter, region_id: %ld, record: %s", _region, 
            //    record->debug_string().c_str());
            return -1;
        }
        rocksdb::Slice iter_value;
        if (_idx_type == pb::I_UNIQ || _use_ttl) {
            iter_value = _iter->value();
        }
        if (_use_ttl) {
            int64_t row_ttl_timestamp_us = ttl_decode(iter_value, _index_info, _online_ttl_base_expire_time_us);
            if (_read_ttl_timestamp_us > row_ttl_timestamp_us) {
                //expired
                if (_forward) {
                    _iter->Next();
                } else {
                    _iter->Prev();
                }
                _valid = _valid && _iter->Valid();
                continue;
            }
        }
        if (!_fits_region(iter_key, iter_value)) {
            if (_forward) {
                _iter->Next();
            } else {
                _iter->Prev();
            }
            _valid = _valid && _iter->Valid();
            //DB_WARNING("get next, fits region filter, region_id: %ld, record: %s", 
            //    _region, record->debug_string().c_str());
            continue;
        }
        //DB_WARNING("get next, in range, region_id: %ld, ke: %s",
        //    _region, _iter->key().ToString(true).c_str());
        TableKey key(iter_key, true);
        //create a record and parse record and primary key
        //record.reset(TableRecord::new_record(_pk_table).get());
        int pos = 0;
        key.skip_region_prefix(pos);
        key.skip_table_prefix(pos);
        if (record != nullptr) {
            if (0 != (*record)->decode_key(*_index_info, key, pos)) {
                DB_WARNING("decode secondary record failed: %ld", _index_info->id);
                _valid = false;
                return -1;
            }
        } else {
            if (0 != (*mem_row)->decode_key(tuple_id, *_index_info, _field_slot, key, pos)) {
                DB_WARNING("decode secondary record failed: %ld", _index_info->id);
                _valid = false;
                return -1;
            }
        }
        if (_idx_type == pb::I_UNIQ) {
            TableKey pkey(iter_value, true);
            pos = 0;
            if (record != nullptr) {
                if (0 != (*record)->decode_primary_key(*_index_info, pkey, pos)) {
                    DB_WARNING("decode primary record failed: %ld", _index_info->pk);
                    _valid = false;
                    return -1;
                }
            } else {
                if (0 != (*mem_row)->decode_primary_key(tuple_id, *_index_info, _field_slot, pkey, pos)) {
                    DB_WARNING("decode primary record failed: %ld", _index_info->pk);
                    _valid = false;
                    return -1;
                }
            }
        } else if (_idx_type == pb::I_KEY) {
            if (record != nullptr) {
                if (0 != (*record)->decode_primary_key(*_index_info, key, pos)) {
                    DB_WARNING("decode primary record failed: %ld, %d, %ld", 
                            _index_info->pk, pos, iter_key.size());
                    _valid = false;
                    return -1;
                }
            } else {
                if (0 != (*mem_row)->decode_primary_key(tuple_id, *_index_info, _field_slot, key, pos)) {
                    DB_WARNING("decode primary record failed: %ld, %d, %ld", 
                            _index_info->pk, pos, iter_key.size());
                    _valid = false;
                    return -1;
                }
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
}
