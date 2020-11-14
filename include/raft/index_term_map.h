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

#include <deque>
#include <algorithm>
#ifdef BAIDU_INTERNAL
#include <raft/log_entry.h>
#else
#include <braft/log_entry.h>
#endif
#include "common.h"

namespace baikaldb {

class IndexTermMap {
public:
    IndexTermMap() {}

    struct Cmp {
        bool operator()(int64_t index, const braft::LogId& log_id) const {
            return index < log_id.index;
        }   
    };

    // Get term the log at |index|, return the term exactly or 0 if it's unknown
    int64_t get_term(int64_t index) const {
        if (_q.empty()) {
            return 0;
        }   
        if (index >= _q.back().index) {
            return _q.back().term;
        }   
        if (_q.size() < 15ul/*FIXME: it's not determined by benchmark*/) {
            // In most case it's true, term doesn't change frequently.
            for (std::deque<braft::LogId>::const_reverse_iterator
                    iter = _q.rbegin(); iter != _q.rend(); ++iter) {
                if (index >= iter->index) {
                    return iter->term;
                }
            }
        } else {
            std::deque<braft::LogId>::const_iterator
                iter = std::upper_bound(_q.begin(), _q.end(), index, Cmp());
            if (iter == _q.begin()) {
                return 0;
            }
            --iter;
            return iter->term;
        }
        // The term of |index| is unknown
        return 0;
    }
    
    int append(const braft::LogId& log_id) {
        if (_q.empty()) {
            _q.push_back(log_id);
            return 0;
        }
        if (log_id.index <= _q.back().index || log_id.term < _q.back().term) {
            DB_FATAL("Invalid log_id=%ld:%ld while _q.back()=%ld:%ld,"
                            " do you forget to call truncate_suffix() or reset()",
                            log_id.index, log_id.term,
                            _q.back().index, _q.back().term);
            return -1;
        }
        if (log_id.term != _q.back().term) {
            _q.push_back(log_id);
        }
        return 0;
    }

    void truncate_prefix(int64_t first_index_kept) {
        if (_q.empty()) {
            // TODO: Print log if it's an exception
            DB_WARNING("term map has no logid, first_index_kept:%ld", first_index_kept);
            return;
        }
        size_t num_pop = 0;
        for (std::deque<braft::LogId>::const_iterator
                iter = _q.begin() + 1; iter != _q.end(); ++iter) {
            if (iter->index <= first_index_kept) {
                ++num_pop;
            } else {
                break;
            }
        }
        _q.erase(_q.begin(), _q.begin() + num_pop);
    }

    void truncate_suffix(int64_t last_index_kept) {
        while (!_q.empty() && _q.back().index > last_index_kept) {
            _q.pop_back();
        }
    }

    void reset() {
        _q.clear();
    }
private:
    std::deque<braft::LogId> _q;
}; //class

} //namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
