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

#include "topn_sorter.h"

namespace baikaldb {

void TopNSorter::add_batch(std::shared_ptr<RowBatch>& batch){ 
    while (!batch->is_traverse_over()) {
        if (_current_count < _limit) {
            _mem_min_heap.push_back(std::move(batch->get_row()));
            _current_count ++;
            if (!_comp->need_not_compare()) {
                shiftup(_current_count - 1, true);
            }
        } else {
            auto& row = batch->get_row();
            if (!_comp->need_not_compare()) {
                if (_comp->less(row.get(), _mem_min_heap[0].get())) {
                    _mem_min_heap[0] = std::move(row);
                    shiftdown(0, true);
                }
            }
        }
        batch->next();
    }
}

int TopNSorter::get_next(RowBatch* batch, bool* eos) {
    while (1) {
        if (batch->is_full()) {
            return 0;
        }
        if (!_current_count) {
            *eos = true;
            return 0;
        }
        if (_comp->need_not_compare()) {
            auto row = _mem_min_heap[_current_count - 1].get();
            batch->move_row(std::unique_ptr<baikaldb::MemRow>(row));
            _current_count --;
            _mem_min_heap[_current_count].reset();
            continue;
        }
        batch->move_row(std::move(_mem_min_heap[0]));
        _current_count --;
        _mem_min_heap[0] = std::move(_mem_min_heap[_current_count]);
        shiftdown(0, false);
    }
    return 0;
}
void TopNSorter::shiftdown(size_t index, bool flag) {
    size_t left_index = index * 2 + 1;
    size_t right_index = left_index + 1;
    if (left_index >= _current_count) {
        return;
    }
    size_t min_index = index;
    if (left_index < _current_count) {
        int64_t com = _comp->compare(_mem_min_heap[left_index].get(),
                _mem_min_heap[min_index].get());
        if (flag) {
            com = -com;
        }
        if (com < 0) {
            min_index = left_index;
        }
    }
    if (right_index < _current_count) {
        int64_t com = _comp->compare(_mem_min_heap[right_index].get(),
                _mem_min_heap[min_index].get());
        if (flag) {
            com = -com;
        }
        if (com < 0) {
            min_index = right_index;  
        }
    }
    if (min_index != index) {
        std::iter_swap(_mem_min_heap.begin() + min_index, _mem_min_heap.begin() + index);
        shiftdown(min_index, flag);
    }
}

void TopNSorter::shiftup(size_t index, bool flag) {
    if (index == 0) {
        return;
    }
    size_t parent = (index - 1) / 2;
    auto com = _comp->compare(_mem_min_heap[index].get(), _mem_min_heap[parent].get());
    if (flag) {
        com = -com;
    }
    if (com < 0) {
        std::iter_swap(_mem_min_heap.begin() + index, _mem_min_heap.begin() + parent);
        shiftup(parent, flag);
    }
}

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
