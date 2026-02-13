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
#include "query_context.h"
#include "scan_node.h"

namespace baikaldb {
class ConditionOptimizer {
public:
    ConditionOptimizer() {}
    ~ConditionOptimizer() {}
    ConditionOptimizer(QueryContext* ctx) : _ctx(ctx) {}
    int analyze(QueryContext* ctx);
    int adjust_huge_in_condition(ScanNode* scan_node, bool in_acero);
private:
    QueryContext* _ctx = nullptr;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
