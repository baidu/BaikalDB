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

#include "plan.pb.h"
#include "exec_type_analyzer.h"
#include "join_node.h"

namespace baikaldb {
DECLARE_bool(enable_plan_cache);
DECLARE_bool(use_arrow_vector);
DEFINE_int32(mpp_hash_partition_num, 0, "mpp hash partition num");
DEFINE_bool(enable_decide_mpp_by_db_statistics, false, "enable decide mpp by db statistics");
DECLARE_int64(mpp_min_statistics_rows);
DECLARE_int64(mpp_min_statistics_bytes);

int ExecTypeAnalyzer::analyze(QueryContext* ctx) {
    if (ctx->is_from_subquery
         || ctx->is_union_subquery
         || ctx->is_insert_select_subquery) {
        return 0;
    }
    bool can_vectorize = can_use_arrow_vector(ctx);
    if (!can_vectorize) {
        ctx->use_mpp = false;
        return 0;
    }
    bool sql_can_mpp = can_use_mpp(ctx);
    if (ctx->use_mpp) {
        // sql注释指定或者通过sign指定
        ctx->use_mpp = sql_can_mpp;
    } else if (sql_can_mpp && FLAGS_enable_decide_mpp_by_db_statistics) {
        // 根据统计信息判断是否走mpp
        ctx->use_mpp = adjust_mpp_by_statistics(ctx);
    }
    set_subquery_exec_type(ctx);
    return 0;
}

bool ExecTypeAnalyzer::has_full_export(QueryContext* ctx) {
    if (ctx->is_full_export) {
        return true;
    }
    for (auto sub_ctx : ctx->sub_query_plans) {
        if (has_full_export(sub_ctx.get())) {
            return true;
        }
    }
    return false;
}

bool ExecTypeAnalyzer::can_use_arrow_vector(QueryContext* ctx) {
    RuntimeState& state = *ctx->get_runtime_state();
    // reset first
    state.reset_vectorize_info();

    // full join必须走向量化, 即使集群没开向量化开关
    bool must_vectorize = false;
    // sql注释或者sign配置指定走向量化
    bool defined_vectorize = false;
    std::vector<ExecNode*> joins;
    ctx->root->get_node_pass_subquery(pb::JOIN_NODE, joins);
    for (auto& join : joins) {
        if (static_cast<JoinNode*>(join)->join_type() == pb::FULL_JOIN) {
            must_vectorize = true;
            break;
        }
    }
    if (ctx->client_conn != nullptr && ctx->client_conn->txn_id != 0) {
        if (joins.size() > 0 
            || ctx->root->get_node_pass_subquery(pb::UNION_NODE) != nullptr) {
            // union和join可能会并发查表, 在事务里有问题
            // 行: seq_id++, join查左表, seq_id++, join查右表, 串行
            // no index join: seq_id++, seq_id++, 开启acero, 并发查两个表
            return false;
        }
    }
    // set exec type
    if (!ctx->table_can_use_arrow_vectorize) {
        state.sign_exec_type = SignExecType::SIGN_EXEC_ROW;
    } else {
        if (ctx->sign_exec_type.count(ctx->stat_info.sign) > 0) {
            state.sign_exec_type = ctx->sign_exec_type[ctx->stat_info.sign];
        } else if (ctx->sign_exec_type.count(0) > 0) {
            // sign0表示全表配置
            state.sign_exec_type = ctx->sign_exec_type[0];
        }
    }
    // sql注释指定
    if (ctx->sql_exec_type_defined != SignExecType::SIGN_EXEC_NOT_SET) {
        state.sign_exec_type = ctx->sql_exec_type_defined;
    }
    defined_vectorize = is_vectorized_exec_type(state.sign_exec_type);
    if (!FLAGS_use_arrow_vector && !must_vectorize && !defined_vectorize) {
        return false;
    }
    if (has_full_export(ctx)) {
        return false;
    }
    if (must_vectorize || 
            (state.sign_exec_type != SignExecType::SIGN_EXEC_ROW
            && (ctx->explain_type == EXPLAIN_NULL 
                    || ctx->explain_type == SHOW_TRACE 
                    || ctx->explain_type == SHOW_PLAN))) {
        if (ctx->root->can_use_arrow_vector(&state)) {
            state.execute_type = pb::EXEC_ARROW_ACERO;
            // TODO优化
            // is_simple_select是为了确定selectmanagernode的schema
            // 现在mpp都定好了每阶段的schema, 后续单机向量化也可以统一逻辑, 不用region返回的schema
            ExecNode* join = ctx->root->get_node(pb::JOIN_NODE);
            state.is_simple_select = (join == nullptr);

            if (state.sign_exec_type == SignExecType::SIGN_EXEC_MPP) {
                ctx->use_mpp = true;
            } else if (state.sign_exec_type == SignExecType::SIGN_EXEC_MPP_FORCE_NO_INDEX_JOIN) {
                ctx->use_mpp = true;
                state.sign_exec_type = SignExecType::SIGN_EXEC_ARROW_FORCE_NO_INDEX_JOIN;
            } 
        } else {
            if (must_vectorize) {
                ctx->stat_info.error_code = ER_NOT_SUPPORTED_YET;
                ctx->stat_info.error_msg << "FULL JOIN only support in vectorize mode.";
                return -1;
            }
            state.execute_type = pb::EXEC_ROW;
        }
    }
    return state.execute_type == pb::EXEC_ARROW_ACERO;
}

bool ExecTypeAnalyzer::can_use_mpp(QueryContext* ctx) {
    if (FLAGS_mpp_hash_partition_num <= 0) {
        return false;
    }
    if (ctx->client_conn != nullptr && ctx->client_conn->txn_id != 0) {
        // 事务中不能使用mpp
        return false;
    }
    ExecNode* join_node = ctx->root->get_node_pass_subquery(pb::JOIN_NODE);
    ExecNode* agg_node = ctx->root->get_node_pass_subquery(pb::AGG_NODE);
    if (join_node == nullptr && agg_node == nullptr) {
        // 没有join/agg的没必要走mpp
        return false;
    }
    if (ctx->is_explain && ctx->explain_type != SHOW_PLAN) {
        return false;
    }
    return true;
}

bool ExecTypeAnalyzer::adjust_mpp_by_statistics(QueryContext* ctx) {
    SchemaFactory* factory = SchemaFactory::get_instance();
    std::shared_ptr<SqlStatistics> sql_info = factory->get_sql_stat(ctx->stat_info.sign);
    if (sql_info == nullptr) {
        return false;
    }
    int64_t db_handle_rows = 0;
    int64_t db_handle_bytes = 0;
    int64_t cnt = 0;
    sql_info->get_db_handle_stats(db_handle_rows, db_handle_bytes, cnt);
    if (db_handle_rows > FLAGS_mpp_min_statistics_rows
        || db_handle_bytes > FLAGS_mpp_min_statistics_bytes) {
        return true;
    }
    return false;
}

void ExecTypeAnalyzer::set_subquery_exec_type(QueryContext* ctx) {
    auto main_state = ctx->get_runtime_state();
    for (auto& sub_ctx : ctx->sub_query_plans) {
        if (sub_ctx->expr_params.is_expr_subquery
             && !sub_ctx->expr_params.is_correlated_subquery) {
            // 单独执行的非相关子查询, 设置过了, 不用再设置
            continue;
        }
        sub_ctx->use_mpp = ctx->use_mpp;
        sub_ctx->explain_type = ctx->explain_type;

        auto sub_state = sub_ctx->get_runtime_state();
        sub_state->execute_type = main_state->execute_type;
        sub_state->vectorlized_parallel_execution = main_state->vectorlized_parallel_execution;
        sub_state->sign_exec_type = main_state->sign_exec_type;
        sub_state->use_mpp = ctx->use_mpp;
        if (sub_state->use_mpp) {
            sub_ctx->need_destroy_tree = false;
        }
        // 递归
        set_subquery_exec_type(sub_ctx.get());
    }
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
