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
#include "filter_node.h"
#include "join_node.h"
#include "apply_node.h"
#include "query_context.h"

namespace baikaldb {
class PredicatePushDown {
public:
    int analyze(QueryContext* ctx) {
        ExecNode* plan = ctx->root;
        //目前只要有join节点的话才做谓词下推
        JoinNode* join = static_cast<JoinNode*>(plan->get_node(pb::JOIN_NODE));
        ApplyNode* apply = static_cast<ApplyNode*>(plan->get_node(pb::APPLY_NODE));
        if (join == NULL && apply == NULL) {
            //DB_WARNING("has no join, not predicate")
            return 0;
        }
        std::vector<ExprNode*> empty_exprs;
        if (0 != plan->predicate_pushdown(empty_exprs)) {
            DB_WARNING("predicate push down fail");
            return -1;
        }
        return 0;
    }
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
