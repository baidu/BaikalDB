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

namespace baikaldb {
template <typename Item>
class ItemBatch {
public:
    ItemBatch(size_t capacity = 1024) : _idx(0) , _batch_capacity(capacity) {
        _items.reserve(capacity);
    }

    size_t size() {
        return _items.size();
    }

    void reset() {
        _idx = 0;
    }

    void clear() {
        _items.clear();
        _idx = 0;
    }

    bool is_full() {
        return size() >= _batch_capacity;
    }

    bool is_traverse_over() {
        return _idx >= size();
    }

    void keep_last_records(int num_keep) {
        if (num_keep >= size()) {
            return;
        }
        std::vector<Item> tmp(_items.end() - num_keep, _items.end());
        _items.swap(tmp);
    }

    void keep_first_records(int num_keep) {
        if (num_keep >= size()) {
            return;
        }
        _items.resize(num_keep);
    }

    void add(const Item& item) {
        _items.push_back(item);
    }

    Item& get() {
        return _items[_idx];
    }

    void next() {
        _idx++;
    }

private:
    //先用shared_ptr来维护内存，后续看是否需要优化
    std::vector<Item> _items;
    size_t      _idx;
    size_t      _batch_capacity;
};
} //namespace baikaldb

