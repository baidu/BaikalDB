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
    int analyze(QueryContext* ctx) {
        ExecNode* plan = ctx->root;
        PacketNode* packet_node = static_cast<PacketNode*>(plan->get_node(pb::PACKET_NODE));
        if (packet_node == nullptr) {
            return -1;
        }
        int ret = 0;
        if (packet_node->op_type() == pb::OP_UNION) {
            analyze_union(ctx, packet_node);
        }
        
        if (ctx->has_derived_table) {
            ret = analyze_derived_table(ctx, packet_node);
            if (ret < 0) {
                return ret;
            }
        }
        ret = plan->expr_optimize(ctx);
        if (ret == NOT_BOOL_ERRCODE) {
            ctx->stat_info.error_code = ER_SQL_REFUSE;
            ctx->stat_info.error_msg << "expression with non bool type";
        }
        return ret;
    }
    static void analyze_union(QueryContext* ctx, PacketNode* packet_node);
    static int analyze_derived_table(QueryContext* ctx, PacketNode* packet_node);
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
