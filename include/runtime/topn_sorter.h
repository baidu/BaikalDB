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
#include "sorter.h"

namespace baikaldb {
//对每个batch并行的做sort后，再用heap做归并
class TopNSorter : public Sorter {
public:
    TopNSorter(MemRowCompare* comp) : Sorter(comp), _limit(1) {
    }
    void set_limit(int limit) {
        if (limit > 1) {
            _limit = limit;
        }
    }
    virtual void add_batch(std::shared_ptr<RowBatch>& batch);
    virtual int get_next(RowBatch* batch, bool* eos);
    virtual void sort(){
        for (size_t i = 1; i < _current_count; ++ i) {
            shiftup(i);
        }
    }
    virtual void merge_sort(){}
private:
    void shiftdown(size_t index, bool flag = false);
    void shiftup(size_t index, bool flag = false);

private:
    std::vector<std::unique_ptr<MemRow>> _mem_min_heap;
    int _limit = 1;
    int _current_count = 0;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
