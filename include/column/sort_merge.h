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
#include <iostream>
#include <istream>
#include <streambuf>
#include <string>
#include <vector>
#include <queue>
#include "column_record.h"

// 风险！！！compact涉及多次行列转换和数据拷贝，如何优化性能 TODO
namespace baikaldb {
struct SlotIndex {
    int slot = -1;
};

struct RecordBatch2 {
    RecordBatch2(const std::shared_ptr<arrow::RecordBatch>& batch, ColumnSchemaInfo* schema) : 
        batch_ptr(batch), columns(batch->columns()), schema_info(schema) {

    }

    ExprValue get_value(int column_idx, int row_idx) {
        return ColumnRecord::get_vectorized_value(columns[column_idx], row_idx);
    }

    int64_t num_rows() const {
        return batch_ptr->num_rows();
    }

    void get_full_row(std::vector<ExprValue>& values, int row_idx) {
        if (values.size() < schema_info->schema_with_order_info->num_fields()) {
            values.resize(schema_info->schema_with_order_info->num_fields());
        }
        for (int i = 0; i < schema_info->schema_with_order_info->num_fields(); ++i) {
            values[i] = get_value(i, row_idx);
        }
    }

    std::shared_ptr<arrow::RecordBatch> batch_ptr = nullptr;
    const std::vector<std::shared_ptr<arrow::Array>>& columns;
    ColumnSchemaInfo* schema_info = nullptr;
};

struct UnorderRow {
    int pos = -1;
    RecordBatch2* batch = nullptr;

    int compare(const UnorderRow& other, int column_idx) const {
        ExprValue value = batch->get_value(column_idx, pos);
        ExprValue other_value = other.batch->get_value(column_idx, other.pos);
        int64_t ret = value.compare(other_value);
        if (ret < 0) {
            return -1;
        } else if (ret > 0) {
            return 1;
        } else {
            return 0;
        }
    }

    bool operator<(const UnorderRow& other) const {
        // 是否有更高效的比较方法 TODO
        auto schema_info = this->batch->schema_info;
        for (int i = 0; i < schema_info->uniq_size; i++) {
            int ret = compare(other, i);
            if (ret < 0) {
                return true;
            } else if (ret > 0) {
                return false;
            }
        }

        if (schema_info->raft_index_idx != -1) {
            int ret = compare(other, schema_info->raft_index_idx);
            if (ret < 0) {
                return true;
            } else if (ret > 0) {
                return false;
            }
        }

        if (schema_info->batch_pos_idx != -1) {
            int ret = compare(other, schema_info->batch_pos_idx);
            if (ret < 0) {
                return true;
            } else if (ret > 0) {
                return false;
            }
        }
        
        return true;
    }
};

struct CacheRowPool;
struct CacheRow {
    std::vector<ExprValue> values;
    CacheRowPool* pool = nullptr;
};

struct CacheRowPool {
    CacheRowPool(ColumnSchemaInfo* schema, int size) : schema_info(schema) {
        cache_rows.reserve(size);
        for (int i = 0; i < size; ++i) {
            auto row = new CacheRow;
            row->pool = this;
            row->values.resize(schema_info->schema_with_order_info->num_fields());
            cache_rows.push_back(row);
        }
    }
    ~CacheRowPool() {
        for (auto row : cache_rows) {
            delete row;
        }
        cache_rows.clear();
    }

    CacheRow* alloc() {
        if (cache_rows.empty()) {
            auto row = new CacheRow;
            row->pool = this;
            row->values.resize(schema_info->schema_with_order_info->num_fields());
            return row;
        } else {
            auto row = cache_rows.back();
            cache_rows.pop_back();
            return row;
        }
    }
    void free(CacheRow* row) {
        cache_rows.push_back(row);
    }
    std::vector<CacheRow*> cache_rows;
    ColumnSchemaInfo* schema_info = nullptr;
};

struct SingleRow : public SlotIndex {
public:
    SingleRow() {}
    ~SingleRow() {
        if (_row != nullptr) {
            _row->pool->free(_row);
            _row = nullptr;
        }
    }

    void alloc_row(CacheRowPool* pool) {
        if (_row == nullptr) {
            _row = pool->alloc();
        }
    }

    SingleRow(SingleRow&& other) noexcept {
        slot = other.slot;
        _row = other._row;
        other._row = nullptr;
    }

    SingleRow& operator=(SingleRow&& other) noexcept {
        slot = other.slot;
        if (this != &other) {
            if (_row != nullptr) {
                _row->pool->free(_row);
                _row = nullptr;
            }
            _row = other._row;
            other._row = nullptr;
        }
        return *this;
    }

    bool is_valid() const {
        return _row != nullptr;
    }

    void set_invalid() {
        if (_row != nullptr) {
            _row->pool->free(_row);
            _row = nullptr;
        }
    }

    int get_keytype() const {
        auto schema_info = _row->pool->schema_info;
        return _row->values[schema_info->keytype_idx].get_numberic<int>();
    }

    void set_keytype(int keytype) {
        auto schema_info = _row->pool->schema_info;
        _row->values[schema_info->keytype_idx].set_numeric<int>(keytype);
    }

    void get_value(ExprValue& value, int column_idx) const {
        value = _row->values[column_idx];
    }

    std::vector<ExprValue>& get_values() {
        return _row->values;
    }

    void swap(SingleRow& other) {
        int s = slot;
        slot = other.slot;
        other.slot = s;
        auto tmp = _row;
        _row = other._row;
        other._row = tmp;
    }

    void sum_columns(const SingleRow& other) const {
        auto schema_info = _row->pool->schema_info;
        for (int i : schema_info->need_sum_idx) {
            _row->values[i].add(other._row->values[i]);
        }
    }

    int merge(SingleRow& other) {
        int keytype = get_keytype();
        int other_keytype = other.get_keytype();
        if (other_keytype == COLUMN_KEY_MERGE) {
            if (keytype == COLUMN_KEY_MERGE || keytype == COLUMN_KEY_PUT) {
                sum_columns(other);
            } else if (keytype == COLUMN_KEY_DELETE) {
                swap(other);
                set_keytype(COLUMN_KEY_PUT);
            } else {
                return -1;
            }
        } else if (other_keytype == COLUMN_KEY_PUT || other_keytype == COLUMN_KEY_DELETE) {
            swap(other);
        } else {
            return -1;
        }
        return 0;
    }

    int compare(const SingleRow& other, int column_idx) const {
        int64_t ret = _row->values[column_idx].compare(other._row->values[column_idx]);
        if (ret < 0) {
            return -1;
        } else if (ret > 0) {
            return 1;
        } else {
            return 0;
        }
    }

    int compare_by_primary_key(const SingleRow& other) const {
        auto schema_info = _row->pool->schema_info;
        for (int i = 0; i < schema_info->uniq_size; i++) {
            int ret = compare(other, i);
            if (ret < 0) {
                return -1;
            } else if (ret > 0) {
                return 1;
            }
        }
        return 0;
    }

    bool operator<(const SingleRow& other) const {
        if (!is_valid()) {
            return false;
        }
        if (!other.is_valid()) {
            return true;
        }
        auto schema_info = _row->pool->schema_info;
        for (int i = 0; i < schema_info->uniq_size; i++) {
            int ret = compare(other, i);
            if (ret < 0) {
                return true;
            } else if (ret > 0) {
                return false;
            }
        }

        if (schema_info->raft_index_idx != -1) {
            int ret = compare(other, schema_info->raft_index_idx);
            if (ret < 0) {
                return true;
            } else if (ret > 0) {
                return false;
            }
        }

        if (schema_info->batch_pos_idx != -1) {
            int ret = compare(other, schema_info->batch_pos_idx);
            if (ret < 0) {
                return true;
            } else if (ret > 0) {
                return false;
            }
        }
        
        return true;
    }
private:
    CacheRow* _row = nullptr;
    DISALLOW_COPY_AND_ASSIGN(SingleRow);
};

class SingleRowReader {
public:
    virtual int get_next(SingleRow& row) = 0;
    virtual ~SingleRowReader() {}
};

class UnOrderSingleRowReader : public SingleRowReader {
public:
    UnOrderSingleRowReader(const std::shared_ptr<arrow::RecordBatchReader>& reader, ColumnSchemaInfo* schema_info, int64_t estimate_size) : 
        _reader(reader), _schema_info(schema_info) {
        _sort.reserve(estimate_size);
    }

    ~UnOrderSingleRowReader() {

    }

    // 需要init时在内存中排序完成
    int init();
    int get_next(SingleRow& row) override;

private:
    std::shared_ptr<arrow::RecordBatchReader> _reader = nullptr;
    std::vector<UnorderRow> _sort;
    int64_t _sort_pos = 0;
    ColumnSchemaInfo* _schema_info = nullptr;
    bool _init = false;
    std::vector<std::shared_ptr<RecordBatch2>> _record_batches; // 暂存，析构时释放
};

class OrderSingleRowReader : public SingleRowReader {
public:
    OrderSingleRowReader(const std::shared_ptr<arrow::RecordBatchReader>& reader, ColumnSchemaInfo* schema_info) : 
        _reader(reader), _schema_info(schema_info) {

    }
    ~OrderSingleRowReader() {

    }

    int init() {
        if (_init) {
            return 0;
        }
        std::shared_ptr<arrow::RecordBatch> rb;
        auto s = _reader->ReadNext(&rb);
        if (!s.ok()) {
            return -1;
        }

        if (rb != nullptr) {
            _record_batch2 = std::make_shared<RecordBatch2>(rb, _schema_info);
        }

        _init = true;
        return 0;
    }
    int get_next(SingleRow& row) override;
private:
    std::shared_ptr<arrow::RecordBatchReader> _reader = nullptr;
    std::shared_ptr<RecordBatch2> _record_batch2 = nullptr;
    int _pos = 0;
    ColumnSchemaInfo* _schema_info = nullptr;
    bool _init = false;
};

struct SortMergeOptions {
    int  batch_size = 10000; // 每次读取的行数，和parquet文件稀疏索引间隔一致
    bool is_base_compact = false;
    std::shared_ptr<ColumnSchemaInfo>   schema_info = nullptr;
};

class SortMerge : public arrow::RecordBatchReader {
public:
    explicit SortMerge(const SortMergeOptions& options, const std::vector<std::shared_ptr<SingleRowReader>>& readers) : 
            _options(options), _readers(readers), _cache_row_pool(options.schema_info.get(), 2 * readers.size()) {
        _column_record = std::make_shared<ColumnRecord>(_options.schema_info->schema, _options.batch_size);
    }

    virtual ~SortMerge() { }

    int init() {
        if (_init) {
            return 0;
        }
        int ret = _column_record->init();
        if (ret < 0) {
            return -1;
        } 
        _column_record->reserve(_options.batch_size);
        _init = true;
        return 0;
    }

    int get_next(std::shared_ptr<arrow::RecordBatch>* out);

    virtual arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override {
        int ret = init();
        if (ret < 0) {
            return arrow::Status::Invalid("SortMerge init failed");
        }

        // 每次读取前清空统计信息
        _merge_count = 0;
        _put_count = 0;
        _delete_count = 0;
        ret = get_next(out);
        if (ret < 0) {
            return arrow::Status::Invalid("SortMerge get_next failed");
        }

        return arrow::Status::OK();
    }
    void get_last_batch_count(int& merge_count, int& put_count, int& delete_count) {
        merge_count = _merge_count;
        put_count = _put_count;
        delete_count = _delete_count;
    }
    virtual std::shared_ptr<::arrow::Schema> schema() const override {
        return nullptr;
    }

    // 实现sorted merge
    int multi_reader_deal_first_row();
    int multi_reader_get_row(SingleRow& row);

    int single_reader_get_row(SingleRow& row) {
        row.alloc_row(&_cache_row_pool);
        return _readers[0]->get_next(row);
    }

    int get_sorted_row(SingleRow& row) {
        if (_readers.size() == 1) {
            return single_reader_get_row(row);
        } else {
            return multi_reader_get_row(row);
        }
    }

    int get_merged_row(SingleRow& row);

private:
    SortMergeOptions _options;
    std::vector<std::shared_ptr<SingleRowReader>> _readers;
    Heap<SingleRow> _heap;
    std::queue<SingleRow> _merge_queue;
    std::shared_ptr<ColumnRecord> _column_record;
    bool _init = false;
    CacheRowPool _cache_row_pool;
    int64_t _merge_count = 0;
    int64_t _put_count = 0;
    int64_t _delete_count = 0;
};

struct AceroMergeOptions {
    int  batch_size = 10000; // 每次读取的行数，和parquet文件稀疏索引间隔一致
    std::shared_ptr<ColumnSchemaInfo>   schema_info = nullptr;
};

class AceroMergeReader : public arrow::RecordBatchReader {
public:
    explicit AceroMergeReader(std::vector<std::shared_ptr<arrow::RecordBatchReader>> readers) : _readers(readers) { 

    }
    virtual ~AceroMergeReader() { }

    virtual arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override {
        while (_pos < _readers.size()) {
            std::shared_ptr<arrow::RecordBatch> rb = nullptr;
            arrow::Status status = _readers[_pos]->ReadNext(&rb);
            if (!status.ok()) {
                return arrow::Status::Invalid("AceroMergeReader ReadNext failed");
            }
            if (rb == nullptr || rb->num_rows() <= 0) {
                _pos++;
                continue;
            } else {
                *out = rb;
                return arrow::Status::OK();
            }
        }
        out->reset();
        return arrow::Status::OK();
    }

    virtual std::shared_ptr<::arrow::Schema> schema() const override {
        return _readers[0]->schema();
    }
private:
    std::vector<std::shared_ptr<arrow::RecordBatchReader>> _readers;
    int _pos = 0;
};

class AceroMerge : public arrow::RecordBatchReader {
public:
    explicit AceroMerge(const AceroMergeOptions& options, std::vector<std::shared_ptr<arrow::RecordBatchReader>> readers) : _options(options), _readers(readers) { }
    virtual ~AceroMerge() { }

    int init();

    virtual arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override;
    virtual std::shared_ptr<::arrow::Schema> schema() const override {
        return nullptr;
    }
private:
    AceroMergeOptions _options;
    std::vector<std::shared_ptr<arrow::RecordBatchReader>> _readers;
    bool _init = false;
    std::shared_ptr<arrow::RecordBatch> _record_batch;
    int64_t _pos = 0;
};
} // namespace baikaldb