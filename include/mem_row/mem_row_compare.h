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

#include <vector>
#include <memory>
#include "expr_node.h"

namespace baikaldb {
class MemRowCompare {
public:
    MemRowCompare(std::vector<ExprNode*>& slot_order_exprs,
                  std::vector<bool>& is_asc,
                  std::vector<bool>& is_null_first) : 
                  _slot_order_exprs(slot_order_exprs),
                  _is_asc(is_asc),
                  _is_null_first(is_null_first) {}
    bool need_not_compare() {
        return _slot_order_exprs.size() == 0;
    }
    int64_t compare(MemRow* left, MemRow* right);

    bool less(MemRow* left, MemRow* right) {
        return compare(left, right) < 0;
    }
    
    std::function<bool(const std::unique_ptr<MemRow>& left, const std::unique_ptr<MemRow>& right)> 
    get_less_func() {
        return [this](const std::unique_ptr<MemRow>& left, 
                const std::unique_ptr<MemRow>& right) {
            return less(left.get(), right.get());
        };
    }

private:
    std::vector<ExprNode*>& _slot_order_exprs;
    std::vector<bool>& _is_asc;
    std::vector<bool>& _is_null_first;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
