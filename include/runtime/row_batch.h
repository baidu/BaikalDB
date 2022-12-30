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
        return _rows.size();
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
        for (size_t i = 0; i < size(); i++) {
            used_size += _rows[i]->used_size();
        }
        return used_size;
    }

    size_t index() {
        return _idx;
    }
private:
    //采用unique_ptr来维护内存，减少内存占用
    //后续考虑直接用MemRow，因为MemRow内部也只有几个指针
    std::vector<std::unique_ptr<MemRow> > _rows;
    size_t _idx;
    size_t _capacity = ROW_BATCH_CAPACITY;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
