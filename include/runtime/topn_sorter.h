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

struct TopNHeapItem {
    std::unique_ptr<baikaldb::MemRow> row;
    int64_t idx;
};

class TopNSorter : public Sorter {
public:
    TopNSorter(MemRowCompare* comp, int64_t limit) : Sorter(comp), _limit(limit) {
    }
    virtual void add_batch(std::shared_ptr<RowBatch>& batch);
    virtual void sort();
    virtual void merge_sort(){}
    virtual int get_next(RowBatch* batch, bool* eos);
private:
    virtual void shiftdown(size_t index);
    virtual void shiftup(size_t index);
    std::function<bool(const TopNHeapItem& left, const TopNHeapItem& right)>
    get_less_func() {
        return [this](const TopNHeapItem& left, const TopNHeapItem& right) {
            auto comp = _comp->compare(left.row.get(), right.row.get());
            if (comp < 0) {
                return true;
            } else if (comp == 0 && left.idx < right.idx) {
                return true;
            }
            return false;
        };
    }

private:
    std::vector<TopNHeapItem> _mem_row_heap;
    int64_t _limit = -1;
    int64_t _current_count = 0;
    int64_t _current_idx = 0;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
