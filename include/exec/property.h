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

#include "exec_node.h"

namespace baikaldb {
struct Property {
    std::vector<ExprNode*> slot_order_exprs;
    std::vector<bool> is_asc;
    int64_t expected_cnt = INT64_MAX;
    Property() {
    }
    Property(const std::vector<ExprNode*>& slot_order_exprs, 
            const std::vector<bool>& is_asc, 
            int64_t expected_cnt) : 
            slot_order_exprs(slot_order_exprs),
            is_asc(is_asc),
            expected_cnt(expected_cnt) {
            }

};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
