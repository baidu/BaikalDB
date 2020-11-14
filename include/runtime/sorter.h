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

#include <algorithm> 
#include <vector>
#include "common.h"
#include "row_batch.h"
#include "mem_row_compare.h"

namespace baikaldb {
//对每个batch并行的做sort后，再用heap做归并
class Sorter {
public:
    Sorter(MemRowCompare* comp) : _comp(comp), _idx(0) {
    }
    void add_batch(std::shared_ptr<RowBatch>& batch) {
        batch->reset();
        _min_heap.push_back(batch);
    }
    void sort();
    void merge_sort();
    int get_next(RowBatch* batch, bool* eos);

    size_t batch_size() {
        return _min_heap.size();
    }
private:
    void multi_sort();
    void make_heap();
    void shiftdown(size_t index);

private:
    MemRowCompare* _comp;
    std::vector<std::shared_ptr<RowBatch>> _min_heap;
    size_t _idx;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
