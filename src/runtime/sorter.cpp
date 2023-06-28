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

#include "sorter.h"

namespace baikaldb {
int Sorter::get_next(RowBatch* batch, bool* eos) {
    if (_min_heap.size() == 0) {
        *eos = true;
        return 0;
    }
    if (_comp->need_not_compare()) {
        if (_idx == _min_heap.size() - 1) {
            *eos = true;
        }
        batch->swap(*_min_heap[_idx]);
        ++_idx;
        return 0;
    }
    while (1) {
        if (batch->is_full()) {
            return 0;
        }
        if (_min_heap.empty()) {
            *eos = true;
            return 0;
        }
        batch->move_row(std::move(_min_heap[0]->get_row()));
        _min_heap[0]->next();
        //堆顶batch遍历完后，pop出去
        if (_min_heap[0]->is_traverse_over()) {
            std::iter_swap(_min_heap.begin(), _min_heap.end() - 1);
            _min_heap.pop_back();
            if (!_min_heap.empty()) {
                shiftdown(0);
            }
        } else {
            shiftdown(0);
        }
    }
    return 0;
}
void Sorter::sort() {
    if (_comp->need_not_compare()) {
        return;
    }
    if (_min_heap.size() == 1) {
        _min_heap[0]->sort(_comp);
    } else if (_min_heap.size() > 1) {
        multi_sort();
        make_heap();
    }
}
void Sorter::merge_sort() {
    if (_comp->need_not_compare()) {
        return;
    }
    if (_min_heap.size() > 1) {
        make_heap();
    }
}
void Sorter::make_heap() {
    for (int i = static_cast<int>(_min_heap.size()) / 2 - 1; i >= 0; i--) {
        shiftdown(i);
    }
}

void Sorter::multi_sort() {
    TimeCost cost;
    BthreadCond cond(_min_heap.size());
    for (size_t i = 0; i < _min_heap.size(); i++) {
        Bthread bth(&BTHREAD_ATTR_SMALL);
        bth.run([this, i, &cond]() {
            _min_heap[i]->sort(_comp);
            cond.decrease_signal();
        });
    }
    cond.wait();
    DB_WARNING("sort time:%ld", cost.get_time());
}

void Sorter::shiftdown(size_t index) {
    size_t left_index = index * 2 + 1;
    size_t right_index = left_index + 1;
    if (left_index >= _min_heap.size()) {
        return;
    }
    size_t min_index = index;
    if (left_index < _min_heap.size() &&
            _comp->less(_min_heap[left_index]->get_row().get(),
                _min_heap[min_index]->get_row().get())) {
        min_index = left_index;
    }
    if (right_index < _min_heap.size() && 
            _comp->less(_min_heap[right_index]->get_row().get(),
                _min_heap[min_index]->get_row().get())) {
        min_index = right_index;  
    }
    if (min_index != index) {
        std::iter_swap(_min_heap.begin() + min_index, _min_heap.begin() + index);
        shiftdown(min_index);
    }
}

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
