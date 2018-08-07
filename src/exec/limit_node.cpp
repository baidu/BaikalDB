// Copyright (c) 2018 Baidu, Inc. All Rights Reserved.
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

#include "limit_node.h"
#include "runtime_state.h"
 
namespace baikaldb {
int LimitNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    _offset = node.derive_node().limit_node().offset();
    return 0;
}

int LimitNode::get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
    if (reached_limit()) {
        *eos = true;
        return 0;
    }
    int ret = 0;
    ret = _children[0]->get_next(state, batch, eos);
    if (ret < 0) {
        DB_WARNING("_children get_next fail");
        return ret;
    }
    while (_num_rows_skipped < _offset) {
        if (_num_rows_skipped + (int)batch->size() <= _offset) {
            _num_rows_skipped += batch->size();
            batch->clear();
            if (*eos) {
                return 0;
            }
            ret = _children[0]->get_next(state, batch, eos);
            if (ret < 0) {
                DB_WARNING("_children get_next fail");
                return ret;
            }
        } else {
            int num_skip_rows = _offset - _num_rows_skipped;
            _num_rows_skipped = _offset;
            batch->skip_rows(num_skip_rows);
            break;
        }
    }
    _num_rows_returned += batch->size();
    if (reached_limit()) {
        *eos = true;
        int keep_nums = batch->size() - (_num_rows_returned - _limit);
        batch->keep_first_rows(keep_nums);
        _num_rows_returned = _limit;
        return 0;
    }
    return 0;
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
