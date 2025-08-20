#pragma once

#include "schema_factory.h"
#include "table_record.h"
#include "mut_table_key.h"
#include "rocks_wrapper.h"
#include "my_raft_log_storage.h"
#include "meta_writer.h"
#include "column_record.h"

namespace baikaldb {
struct Row2ColOptions {
     // 行存相关参数
    int64_t                             region_id;
    int64_t                             table_id;
    int                                 read_batch_size = 1024; // default 1024
    bool                                is_cold_rocksdb = false;
    const rocksdb::Snapshot*            snapshot = nullptr;
    int64_t                             start_index;
    int64_t                             end_index;
    std::shared_ptr<ColumnSchemaInfo>   schema_info;
}; 

class Row2ColumnReader : public arrow::RecordBatchReader {
public:
    Row2ColumnReader(const Row2ColOptions& options) : _options(options), _schema_info(options.schema_info)  { }
    virtual ~Row2ColumnReader() {}
    int init(bool schema_with_order_info) {
        if (schema_with_order_info) {
            _column_record = std::make_shared<ColumnRecord>(_schema_info->schema_with_order_info, _options.read_batch_size);
        } else {
            _column_record = std::make_shared<ColumnRecord>(_schema_info->schema, _options.read_batch_size);
        }
        
        int ret = _column_record->init();
        if (ret < 0) {
            return -1;
        } 
        _column_record->reserve(_options.read_batch_size);
        return 0;
    }
    ExprValue get_default_value(const FieldInfo& field);
    int row2col(rocksdb::Slice key, rocksdb::Slice value, ColumnKeyType key_type, int64_t raft_index, int batch_pos);
    virtual arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override {
        return arrow::Status::Invalid("Not Implemented");;
    }
    virtual std::shared_ptr<::arrow::Schema> schema() const override {
        return nullptr;
    }

protected:
    Row2ColOptions _options;
    std::shared_ptr<ColumnSchemaInfo> _schema_info;
    std::shared_ptr<ColumnRecord> _column_record = nullptr;
    TimeCost _cost;
    int _read_times = 0;
    int _total_row_nums = 0;
};

class RocksdbBaseReader : public Row2ColumnReader {
public:
    RocksdbBaseReader(const Row2ColOptions& options) : Row2ColumnReader(options) {

    }
    virtual ~RocksdbBaseReader() {}
    int init();
    virtual arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override;
    virtual std::shared_ptr<::arrow::Schema> schema() const override {
        return nullptr;
    }
private:
    std::string _prefix;
    std::string _end;
    rocksdb::Slice _upper_bound_slice;
    std::shared_ptr<rocksdb::Iterator> _iter;
    bool _is_finish = false;
    bool _init = false;
};

class RaftLogReader : public Row2ColumnReader {
public:
    RaftLogReader(const Row2ColOptions& options) : Row2ColumnReader(options) {
        _column_record = std::make_shared<ColumnRecord>(_schema_info->schema_with_order_info, _options.read_batch_size);
    }
    virtual ~RaftLogReader() {}
    int init();
    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override;
    std::shared_ptr<::arrow::Schema> schema() const override {
        return _schema_info->schema_with_order_info;
    }
    int64_t get_last_raft_index() const {
        return _last_index;
    }
    int64_t row_count() const {
        return _total_row_nums;
    }
    int64_t put_count() const {
        return _put_count;
    }
    int64_t delete_count() const {
        return _delete_count;
    }

private:
    int64_t _first_index = -1;
    int64_t _last_index = -1;
    int64_t _skip_count = 0;
    int64_t _put_count = 0;
    int64_t _delete_count = 0;
    int64_t _merge_count = 0;
    bool _init = false;
    std::vector<std::shared_ptr<arrow::RecordBatch>> _batchs;
};

} // baikaldb