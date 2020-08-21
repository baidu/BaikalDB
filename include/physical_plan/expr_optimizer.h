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
#include "packet_node.h"
#include "query_context.h"

namespace baikaldb {
class ExprOptimize {
public:
    /* 表达式类型推导
     * agg tuple类型推导
     * const表达式求值
     */
    int analyze(QueryContext* ctx);
    void analyze_union(QueryContext* ctx, PacketNode* packet_node);
    int analyze_derived_table(QueryContext* ctx, PacketNode* packet_node);
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
