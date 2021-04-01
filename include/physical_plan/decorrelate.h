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
#include "apply_node.h"
#include "query_context.h"

namespace baikaldb {
class DeCorrelate {
public:
    /* 相关子查询去相关
     */
    int analyze(QueryContext* ctx) {
        ExecNode* plan = ctx->root;
        std::vector<ExecNode*> apply_nodes;
        plan->get_node(pb::APPLY_NODE, apply_nodes);
        if (apply_nodes.size() == 0) {
            return 0;
        }
        for (auto& apply_node : apply_nodes) {
            ApplyNode* apply = static_cast<ApplyNode*>(apply_node);
            apply->decorrelate();
        }
        return 0;
    }
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */