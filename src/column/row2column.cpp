#include "row2column.h"

namespace baikaldb {
DEFINE_int32(column_minor_compact_read_raft_rows, 1000000, "column_minor_compact_read_raft_rows(100w)"); 
ExprValue Row2ColumnReader::get_default_value(const FieldInfo& field) {
    // 复杂统计类型不能设置默认值，按理说也不应该not null
    if (field.type == pb::HLL || field.type == pb::BITMAP || field.type == pb::TDIGEST) {
        return ExprValue::Null();
    }
    if (field.default_expr_value.is_null() && field.can_null) {
        return ExprValue::Null();
    }
    ExprValue default_value = field.default_expr_value;
    if (field.default_value == "(current_timestamp())") {
        default_value = ExprValue::Now();
        default_value.cast_to(field.type);
    }
    // mysql非strict mode，不填not null字段会补充空串/0等
    if (field.default_expr_value.is_null() && !field.can_null) {
        default_value.type = pb::STRING;
    }

    return default_value;
}

int Row2ColumnReader::row2col(rocksdb::Slice key, rocksdb::Slice value, ColumnKeyType key_type, int64_t raft_index, int batch_pos) {
    SmartRecord record = SchemaFactory::get_instance()->new_record(*_schema_info->table_info);
    if (key_type != COLUMN_KEY_DELETE) {
        if (0 != record->decode(value.data(), value.size())) {
            DB_WARNING("decode value failed");
            return -1;
        }
    }

    TableKey table_key(key);
    if (0 != record->decode_key(*_schema_info->index_info, table_key)) {
        DB_WARNING("decode key failed");
        return -1;
    }

    int pos = 0;
    for (const auto& field : _schema_info->key_fields) {
        ExprValue v = record->get_value(record->get_field_by_idx(field.pb_idx));
        if (v.is_null()) {
            // key不可能为null
            return -1;
        }
        // 从proto中读取的类型需要转换成pb类型
        v.cast_to(field.type);
        _column_record->append_value(pos++, v);
    }   

    for (const auto& field : _schema_info->value_fields) {
        if (key_type == COLUMN_KEY_DELETE) {
            // delete没有value
            _column_record->append_value(pos++, ExprValue::Null());
        } else {
            ExprValue v = record->get_value(record->get_field_by_idx(field.pb_idx));
            if (v.is_null()) {
                v = get_default_value(field);
            }
            v.cast_to(field.type);
            _column_record->append_value(pos++, v);
        }
    } 

    ExprValue expr_key_type(pb::INT32);
    expr_key_type._u.int32_val = key_type;
    _column_record->append_value(pos++, expr_key_type);

    if (raft_index != -1) {
        ExprValue expr_raft_index(pb::INT64);
        expr_raft_index._u.int64_val = raft_index;
        _column_record->append_value(pos++, expr_raft_index);
    }

    if (batch_pos != -1) {
        ExprValue expr_batch_pos(pb::INT32);
        expr_batch_pos._u.int32_val = batch_pos;
        _column_record->append_value(pos++, expr_batch_pos);
    }
    _column_record->inc_row_length();
    return 0;
}

int RocksdbBaseReader::init() {
    if (_init) {
        return 0;
    }
    int ret = Row2ColumnReader::init(false);
    if (ret < 0) {
        return -1;
    }
    
    MutTableKey key;
    key.append_i64(_options.region_id).append_i64(_options.table_id);
    _prefix = key.data();
    key.append_u64(UINT64_MAX);
    _end = key.data();
    rocksdb::Slice upper_bound_slice(_end);
    _upper_bound_slice = upper_bound_slice;

    rocksdb::ReadOptions options;
    options.total_order_seek = true;
    options.prefix_same_as_start = true;
    options.fill_cache = false;
    options.readahead_size = 2 * 1024 * 1024;
    options.iterate_upper_bound = &_upper_bound_slice;
    options.snapshot = _options.snapshot;
    rocksdb::Iterator* iter = nullptr;
    if (_options.is_cold_rocksdb) {
        iter = RocksWrapper::get_instance()->new_cold_iterator(options, RocksWrapper::COLD_DATA_CF);
    } else {
        iter = RocksWrapper::get_instance()->new_iterator(options, RocksWrapper::DATA_CF);
    }
    if (iter == nullptr) {
        DB_FATAL("new iterator fail, region_id: %ld", _options.region_id);
        return -1;
    }
    _iter.reset(iter);
    _iter->Seek(_prefix);
    _init = true;
    return 0;
}

arrow::Status RocksdbBaseReader::ReadNext(std::shared_ptr<arrow::RecordBatch>* out) {
    int ret = init();
    if (ret < 0) {
        return arrow::Status::IOError("RocksdbBaseReader init failed");
    }
    _read_times++;
    if (_is_finish) {
        out->reset();
        DB_NOTICE("read base rocksdb is finish, region_id: %ld, need_index[%ld, %ld], read times: %d, total rows: %d, cost: %ld", 
            _options.region_id, _options.start_index, _options.end_index, _read_times, _total_row_nums, _cost.get_time());
        return arrow::Status::OK();
    }
    std::string prefix;
    MutTableKey key;
    key.append_i64(_options.region_id).append_i64(_options.table_id);
    prefix = key.data();

    while (_iter->Valid()) {
        if (!_iter->key().starts_with(prefix)) {
            DB_FATAL("not starts with: %ld", _options.region_id);
            break;
        }

        int prefix_len = 2 * sizeof(int64_t);
        rocksdb::Slice key(_iter->key());
        key.remove_prefix(prefix_len);
        ++_total_row_nums;
        int ret = row2col(key, _iter->value(), COLUMN_KEY_PUT, -1, -1);
        if (ret < 0) {
            return arrow::Status::IOError("read rocksdb failed.");
        }

        _iter->Next();

        if (_column_record->is_full()) {
            break;
        }
    }

    if (!_column_record->is_full()) {
        _is_finish = true;
    }

    if (_column_record->size() == 0) {
        out->reset();
        DB_NOTICE("read base rocksdb is finish, region_id: %ld, need_index[%ld, %ld], read times: %d, total rows: %d, cost: %ld", 
            _options.region_id, _options.start_index, _options.end_index, _read_times, _total_row_nums, _cost.get_time());
        return arrow::Status::OK();
    }

    ret = _column_record->finish_and_make_record_batch(out);
    if (ret < 0) {
        DB_FATAL("arrow chunk finish and make record batch fail");
        return arrow::Status::IOError("make record batch fail");
    }

    return arrow::Status::OK();
}

int RaftLogReader::init() {
    if (_init) {
        return 0;
    }
    int ret = Row2ColumnReader::init(true);
    if (ret < 0) {
        return -1;
    }
    TimeCost cost;
    std::string log_entry;
    MutTableKey log_data_key;
    log_data_key.append_i64(_options.region_id).append_u8(MyRaftLogStorage::LOG_DATA_IDENTIFY).append_i64(_options.start_index);
    MutTableKey prefix;
    MutTableKey end;
    prefix.append_i64(_options.region_id).append_u8(MyRaftLogStorage::LOG_DATA_IDENTIFY);
    end.append_i64(_options.region_id).append_u8(MyRaftLogStorage::LOG_DATA_IDENTIFY).append_i64(_options.end_index + 1);
    std::string log_value;
    rocksdb::ReadOptions options;
    rocksdb::Slice upper_bound_slice = end.data();
    options.iterate_upper_bound = &upper_bound_slice;
    options.prefix_same_as_start = true;
    options.total_order_seek = false;
    options.fill_cache = false;
    auto iter_ptr = RocksWrapper::get_instance()->new_iterator(options, RocksWrapper::RAFT_LOG_CF);
    if (iter_ptr == nullptr) {
        return -1;
    }
    std::unique_ptr<rocksdb::Iterator> iter(iter_ptr);
    iter->Seek(log_data_key.data());
    for (; iter->Valid(); iter->Next()) {
        if (!iter->key().starts_with(prefix.data())) {
            DB_WARNING("read end info, region_id: %ld, key:%s", _options.region_id, iter->key().ToString(true).c_str());
            return -1;
        }
        int64_t log_index = TableKey(iter->key()).extract_i64(sizeof(int64_t) + 1);
        if (log_index > _options.end_index) {
            DB_WARNING("region_id:%ld, log_index:%ld, end_log_index:%ld", _options.region_id, log_index, _options.end_index);
            break;
        }

        if (_first_index == -1) {
            _first_index = log_index;
        }

        if (_last_index < log_index) {
            _last_index = log_index;
        }

        rocksdb::Slice value_slice(iter->value());
        LogHead head(value_slice);
        value_slice.remove_prefix(MyRaftLogStorage::LOG_HEAD_SIZE); 
        if (head.type != braft::ENTRY_TYPE_DATA) {
            ++_skip_count;
            DB_WARNING("log entry is not data, region_id: %ld head.type: %d, raft index: %ld", _options.region_id, head.type, log_index);
            continue;
        }

        pb::StoreReq request;
        if (!request.ParseFromArray(value_slice.data(), value_slice.size())) {
            DB_FATAL("Fail to parse request fail, region_id: %ld", _options.region_id);
            return -1;
        }

        if (request.op_type() == pb::OP_KV_BATCH) {
            auto s = MetaWriter::get_instance()->get_skip_watt_stats_version(_options.region_id, log_index);
            if (s.ok()) {
                ++_skip_count;
                DB_WARNING("region_id: %ld, column skip raft log index: %ld", _options.region_id, log_index);
                // 找到说明已经这个点被跳过
                continue;
            }
            int idx = 0;
            for (auto& kv_op : request.kv_ops()) {
                int ret = 0;
                pb::OpType op_type = kv_op.op_type();
                int prefix_len = 2 * sizeof(int64_t);
                rocksdb::Slice key(kv_op.key());
                TableKey table_key(key);
                int64_t table_id = table_key.extract_i64(sizeof(int64_t));
                if (table_id != _options.table_id) {
                    continue;
                }
                ++_total_row_nums;
                key.remove_prefix(prefix_len);
                if (op_type == pb::OP_PUT_KV) {
                    ++_put_count;
                    ret = row2col(key, kv_op.value(), COLUMN_KEY_PUT, log_index, idx++);
                } else if (op_type == pb::OP_MERGE_KV) {
                    ++_merge_count;
                    ret = row2col(key, kv_op.value(), COLUMN_KEY_MERGE, log_index, idx++);
                } else {
                    ++_delete_count;
                    ret = row2col(key, kv_op.value(), COLUMN_KEY_DELETE, log_index, idx++);
                }
                if (ret < 0) {
                    DB_FATAL("row2col fail, region_id: %ld", _options.region_id);
                    return -1;
                }
            }
        }

        if (_column_record->is_full()) {
            std::shared_ptr<arrow::RecordBatch> out;
            ret = _column_record->finish_and_make_record_batch(&out);
            if (ret < 0) {
                DB_FATAL("arrow chunk finish and make record batch fail");
                return -1;
            }
            _batchs.push_back(out);
            if ( _total_row_nums >= FLAGS_column_minor_compact_read_raft_rows) {
                break;
            }
        }
    }

    if (_column_record->size() > 0) {
        std::shared_ptr<arrow::RecordBatch> out;
        ret = _column_record->finish_and_make_record_batch(&out);
        if (ret < 0) {
            DB_FATAL("arrow chunk finish and make record batch fail");
            return -1;
        }
        _batchs.push_back(out);
    }

    if (_first_index != _options.start_index && _total_row_nums > 0) {
        // 可能丢数据，报警
        DB_COLUMN_FATAL("column read raft log is not start index, region_id: %ld, start_index: %ld, first_index: %ld", _options.region_id, _options.start_index, _first_index);
    }

    DB_NOTICE("read raft log is finish, region_id: %ld, need_index[%ld, %ld], read_index[%ld, %ld], total rows: %d, cost: %ld, skip_count: %ld", 
            _options.region_id, _options.start_index, _options.end_index, _first_index, _last_index, _total_row_nums, cost.get_time(), _skip_count);

    _init = true;
    return 0;
}

arrow::Status RaftLogReader::ReadNext(std::shared_ptr<arrow::RecordBatch>* out) {
    int ret = init();
    if (ret < 0) {
        return arrow::Status::IOError("RaftLogReader init failed");
    }

    if (_batchs.size() == 0) {
        out->reset();
        return arrow::Status::OK();
    }

    *out = *_batchs.begin();
    _batchs.erase(_batchs.begin());
    
    return arrow::Status::OK();
}

} // namespace baikaldb