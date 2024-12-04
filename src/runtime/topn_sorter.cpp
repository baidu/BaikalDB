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
        _current_idx ++;
        if (_current_count < _limit) {
            _mem_row_heap.push_back(TopNHeapItem{std::move(batch->get_row()), _current_idx});
            _current_count ++;
            if (!_comp->need_not_compare()) {
                shiftup(_current_count - 1);
            }
        } else {
            auto& row = batch->get_row();
            if (!_comp->need_not_compare()) {
                if (_comp->less(row.get(), _mem_row_heap[0].row.get())) {
                    _mem_row_heap[0] = TopNHeapItem{std::move(row), _current_idx};
                    shiftdown(0);
                }
            }
        }
        batch->next();
    }
}

void TopNSorter::sort() {
    _current_idx = 0;
    if (_comp->need_not_compare()) {
        return;
    }
    auto compare_func = [&](const TopNHeapItem& left, const TopNHeapItem& right) {
        auto comp = _comp->compare(left.row.get(), right.row.get());
        if (comp < 0) {
            return true;
        } else if (comp == 0 && left.idx < right.idx) {
            return true;
        }
        return false;
    };
    std::sort(_mem_row_heap.begin(), _mem_row_heap.end(), compare_func);
}

int TopNSorter::get_next(RowBatch* batch, bool* eos) {
    while (1) {
        if (batch->is_full()) {
            return 0;
        }
        if (_current_idx >= _mem_row_heap.size()) {
            *eos = true;
            return 0;
        }
        batch->move_row(std::move(_mem_row_heap[_current_idx].row));
        _current_idx ++;
    }
    return 0;
}

void TopNSorter::shiftdown(size_t index) {
    size_t left_index = index * 2 + 1;
    size_t right_index = left_index + 1;
    if (left_index >= _current_count) {
        return;
    }
    size_t min_index = index;
    if (left_index < _current_count) {
        int64_t com = _comp->compare(_mem_row_heap[left_index].row.get(),
                _mem_row_heap[min_index].row.get());
        if (com > 0) {
            min_index = left_index;
        }
    }
    if (right_index < _current_count) {
        int64_t com = _comp->compare(_mem_row_heap[right_index].row.get(),
                _mem_row_heap[min_index].row.get());
        if (com > 0) {
            min_index = right_index;  
        }
    }
    if (min_index != index) {
        std::iter_swap(_mem_row_heap.begin() + min_index, _mem_row_heap.begin() + index);
        shiftdown(min_index);
    }
}

void TopNSorter::shiftup(size_t index) {
    if (index == 0) {
        return;
    }
    size_t parent = (index - 1) / 2;
    auto com = _comp->compare(_mem_row_heap[index].row.get(), _mem_row_heap[parent].row.get());
    if (com > 0) {
        std::iter_swap(_mem_row_heap.begin() + index, _mem_row_heap.begin() + parent);
        shiftup(parent);
    }
}

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
