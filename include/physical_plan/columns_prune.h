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

#include "dual_scan_node.h"
#include "query_context.h"

namespace baikaldb {

DECLARE_bool(enable_columns_prune);

class ColumnsPrune {
public:
    int analyze(QueryContext* ctx) {
        if (!FLAGS_enable_columns_prune) {
            return 0;
        }
        if (ctx->is_from_subquery || ctx->is_union_subquery) {
            return 0;
        }
        ExecNode* plan = ctx->root;
        if (plan == nullptr) {
            DB_WARNING("plan is nullptr");
            return -1;
        }
        ExecNode* dual = plan->get_node(pb::DUAL_SCAN_NODE);
        if (dual == nullptr) {
            return 0;
        }
        std::unordered_set<int32_t> invalid_column_ids;
        return plan->prune_columns(ctx, invalid_column_ids);
    }
};

} // namespace baikaldb