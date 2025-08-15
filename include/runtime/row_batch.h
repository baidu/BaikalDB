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

#include <stdint.h>
#include <vector>
#include <memory>
#include "mem_row_compare.h"
#include "chunk.h"

namespace baikaldb {
class RowBatch {
public:
    RowBatch() : _idx(0) {
        _rows.reserve(_capacity);
    }
    void set_capacity(size_t capacity) {
        _capacity = capacity;
    }
    size_t capacity() {
        return _capacity;
    }
    size_t size() {
        if (_use_memrow) {
            return _rows.size();
        } else {
            return _chunk->size();
        }
    }
    void reset() {
        _idx = 0;
    }
    void clear() {
        _rows.clear();
        _idx = 0;
    }
    bool is_full() {
        return size() >= _capacity;
    }
    bool chunk_exceed_max_size() {
        if (_use_memrow || _chunk == nullptr) {
            return false;
        }
        return _chunk->exceed_max_size();
    }
    bool is_traverse_over() {
        return _idx >= size();
    }
    void skip_rows(int num_skip_rows) {
        if (num_skip_rows <= 0) {
            return;
        }
        if (num_skip_rows >= (int)size()) {
            _rows.clear();
            return;
        }
        std::vector<std::unique_ptr<MemRow>> tmp(
                std::make_move_iterator(_rows.begin() + num_skip_rows),
                std::make_move_iterator(_rows.end())
                );
        _rows.swap(tmp);
        _idx = 0;
    }
    void keep_first_rows(int num_keep_rows) {
        if (num_keep_rows >= (int)size()) {
            return;
        }
        if (num_keep_rows <= 0) {
            _rows.clear();
            return;
        }
        _rows.resize(num_keep_rows);
    }
    //move_row会转移所有权
    void move_row(std::unique_ptr<MemRow> row) {
        _rows.emplace_back(std::move(row));
    }
    // 使用者保证idx < size()
    void replace_row(std::unique_ptr<MemRow> row, size_t idx) {
        if (size() == 0) {
            _rows.emplace_back(std::move(row));
        } else {
            _rows[idx] = std::move(row);
        }
    }
    std::unique_ptr<MemRow>& get_row() {
        return _rows[_idx];
    }
    std::unique_ptr<MemRow>& get_row(size_t i) {
        return _rows[i];
    }
    std::unique_ptr<MemRow>& back() {
        return _rows.back();
    }
    void next() {
        _idx++;
    }
    void sort(MemRowCompare* comp) {
        std::sort(_rows.begin(), _rows.end(), 
                comp->get_less_func());
    }
    void swap(RowBatch& batch) {
        _rows.swap(batch._rows);
    }

    int64_t used_bytes_size() {
        int64_t used_size = 0;
        if (_use_memrow) {
            for (size_t i = 0; i < size(); i++) {
                used_size += _rows[i]->used_size();
            }
        } else {
            used_size = _chunk->used_bytes_size();
        }
        return used_size;
    }

    size_t index() {
        return _idx;
    }

    bool use_memrow() {
        return _use_memrow;
    }
    /*
     *  以下是列存专用
     */ 
    std::shared_ptr<Chunk> get_chunk() {
        return _chunk;
    }
    void reserve_chunk(int row_len) {
        _chunk->reserve(row_len);
    }
    int finish_and_make_record_batch(std::shared_ptr<arrow::RecordBatch>* batch, std::shared_ptr<arrow::Schema> schema = nullptr) {
        return _chunk->finish_and_make_record_batch(batch, schema);
    }
    int init_chunk(const std::vector<const pb::TupleDescriptor*> tuples, std::shared_ptr<arrow::Schema>* schema) {
        _use_memrow = false;
        _chunk = std::make_shared<Chunk>();
        int ret = _chunk->init(tuples);
        if (ret != 0) {
            DB_FATAL("chunk init failed");
            return 0;
        }
        *schema = _chunk->get_arrow_schema();
        return 0;
    }
    int set_chunk_tmp_row_value(int tuple_id, int slot_id, const ExprValue& value) {
        if (_chunk == nullptr) {
            DB_FATAL("chunk is null");
            return -1;
        }
        ExprValue* tmp_value = _chunk->get_tmp_field_value(tuple_id, slot_id);
        if (tmp_value == nullptr) {
            DB_FATAL("tmp value is null");
            return -1;
        }
        *tmp_value = value;
        return 0;
    }
    int add_chunk_row() {
        return _chunk->add_tmp_row();
    }

    int transfer_rowbatch_to_arrow(const std::vector<const pb::TupleDescriptor*>& tuples, 
                                   std::shared_ptr<Chunk> chunk, 
                                   std::shared_ptr<arrow::Schema> schema, 
                                   std::shared_ptr<arrow::RecordBatch>* out) {
        if (chunk == nullptr) {
            return -1;
        }
        if (!chunk->has_init()) {
            if (0 != chunk->init(tuples)) {
                return -1;
            }
        }
        if (0 != add_row_batch_to_chunk(tuples, chunk)) {
            return -1;
        }
        if (0 != chunk->finish_and_make_record_batch(out, schema)) {
            return -1;
        }
        return 0;
    }
    int add_row_batch_to_chunk(const std::vector<const pb::TupleDescriptor*>& tuples, std::shared_ptr<Chunk> chunk) {
        if (chunk == nullptr) {
            return -1;
        }
        if (_rows.size() == 0) {
            return 0;
        }
        for (_idx = 0; _idx < _rows.size(); ++_idx) {
            MemRow* row = get_row().get();
            if (row == nullptr) {
                continue;
            }
            int ret = chunk->add_row(tuples, row);
            if (ret != 0) {
                DB_FATAL("add row to chunk failed");
                return -1;
            }
        }
        _idx = 0;
        return 0;
    }
private:
    //采用unique_ptr来维护内存，减少内存占用
    //后续考虑直接用MemRow，因为MemRow内部也只有几个指针
    std::vector<std::unique_ptr<MemRow> > _rows;
    size_t _idx;
    size_t _capacity = ROW_BATCH_CAPACITY;
    std::shared_ptr<Chunk> _chunk;
    bool _use_memrow = true;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
