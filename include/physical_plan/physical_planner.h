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

#pragma once

#include "expr_optimizer.h"
#include "index_selector.h"
#include "limit_calc.h"
#include "physical_planner.h"
#include "plan_router.h"
#include "predicate_pushdown.h"
#include "separate.h"
#include "auto_inc.h"

namespace baikaldb {
class PhysicalPlanner {
public:
    PhysicalPlanner() {}
    static int analyze(QueryContext* ctx) {
        int ret = 0;
        auto return_empty_func = [ctx]() {
            ExecNode* plan = ctx->root;
            PacketNode* packet_node = static_cast<PacketNode*>(plan->get_node(pb::PACKET_NODE));
            for (size_t i = 0; i < packet_node->children_size(); i++) {
                ExecNode::destory_tree(packet_node->children(i));
            }
            packet_node->clear_children();
            packet_node->set_limit(0);
        };
        // 包括类型推导与常量表达式计算
        ret = ExprOptimize().analyze(ctx);
        if (ret < 0) {
            return ret;
        }
        if (ctx->return_empty) {
            return_empty_func();
            return 0;
        }
        // 生成自增id
        ret = AutoInc().analyze(ctx);
        if (ret < 0) {
            return ret;
        }
        // 谓词下推,可能生成新的table_filter
        ret = PredicatePushDown().analyze(ctx);
        if (ret < 0) {
            return ret;
        }
        // 根据表达式分析可能的索引
        ret = IndexSelector().analyze(ctx);
        if (ret < 0) {
            return ret;
        }
        if (ctx->return_empty) {
            DB_WARNING("index field can not be null");
            return_empty_func();
            return 0;
        }
        // 查询region路由表，得到需要推送到哪些region
        ret = PlanRouter().analyze(ctx);
        if (ret < 0) {
            return ret;
        }
        // F1与store的计划分离，生成FetcherNode，并且根据region数量做不同决策
        ret = Separate().analyze(ctx);
        if (ret < 0) {
            return ret;
        }
        // 计算每个节点所需要的limit值
        ret = LimitCalc().analyze(ctx);
        if (ret < 0) {
            return ret;
        }
        return 0;
    }

    static int execute(QueryContext* ctx, DataBuffer* send_buf) {
        int ret = 0;
        RuntimeState& state = ctx->runtime_state;
        ret = state.init(ctx, send_buf);
        if (ret < 0) {
            DB_FATAL("RuntimeState init fail");
             ctx->stat_info.error_code = state.error_code;
            return ret;
        }
        ret = ctx->root->open(&state);
        ctx->root->close(&state);
        if (ret < 0) {
            DB_FATAL("plan open fail: %d, %s", state.error_code, state.error_msg.str().c_str());
             ctx->stat_info.error_code = state.error_code;
             ctx->stat_info.error_msg.str(state.error_msg.str());
            return ret;
        }
        ctx->stat_info.num_returned_rows = state.num_returned_rows();
        ctx->stat_info.num_affected_rows = state.num_affected_rows();
        ctx->stat_info.error_code = state.error_code;
        
        return 0;
    }

    static int execute_recovered_commit(NetworkSocket* client, const pb::CachePlan& commit_plan) {
        int ret = 0;
        RuntimeState& state = client->query_ctx->runtime_state;
        state.set_client_conn(client);
        ret = state.init(commit_plan);
        if (ret < 0) {
            DB_FATAL("RuntimeState init fail");
            return ret;
        }
        ExecNode* root = nullptr;
        ret = ExecNode::create_tree(commit_plan.plan(), &root);
        if (ret < 0) {
            ExecNode::destory_tree(root);
            DB_FATAL("create plan tree failed, txn_id: %lu", state.txn_id);
            return ret;
        }
        ret = root->open(&state);
        if (ret < 0) {
            root->close(&state);
            ExecNode::destory_tree(root);
            DB_FATAL("open plan tree failed, txn_id: %lu", state.txn_id);
            return ret;
        }
        root->close(&state);
        return 0;
    }

private:
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
