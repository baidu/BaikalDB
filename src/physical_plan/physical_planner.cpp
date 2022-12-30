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

#include "physical_planner.h"
#include "query_context.h"
namespace baikaldb {
DEFINE_int32(cmsketch_depth, 5, "cmsketch_depth");
DEFINE_int32(cmsketch_width, 2048, "cmsketch_width");
DEFINE_int32(sample_rows, 1000000, "sample rows 100w");
int PhysicalPlanner::analyze(QueryContext* ctx) {
    int ret = 0;
    for (auto sub_query_ctx : ctx->sub_query_plans) {
        ret = analyze(sub_query_ctx.get());
        if (ret < 0) {
            return ret;
        }
    }
    // 包括类型推导与常量表达式计算
    ret = ExprOptimize().analyze(ctx);
    if (ret < 0) {
        return ret;
    }

    ret = PartitionAnalyze().analyze(ctx);
    if (ret < 0) {
        return ret;
    }
    
    // for INSERT/REPLACE statements
    // insert user variables to records for prepared stmt
    ret = insert_values_to_record(ctx);
    if (ret < 0) {
        return ret;
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
    if (!ctx->is_straight_join) { // straight join
        // 目前只对纯inner join重排序，方便使用索引和构造等值join
        ret = JoinReorder().analyze(ctx);
        if (ret < 0) {
            return ret;
        }
    }
    // 查询region路由表，得到需要推送到哪些region
    ret = PlanRouter().analyze(ctx);
    if (ret < 0) {
        return ret;
    }
    // db与store的计划分离，生成FetcherNode，并且根据region数量做不同决策
    ret = Separate().analyze(ctx);
    if (ret < 0) {
        return ret;
    }
    // 计算每个节点所需要的limit值
    ret = LimitCalc().analyze(ctx);
    if (ret < 0) {
        return ret;
    }
    // 子查询去相关
    ret = DeCorrelate().analyze(ctx);
    if (ret < 0) {
        return ret;
    }
    if (ctx->return_empty) {
        ctx->root->set_return_empty();
    }
    return 0;
}

int64_t PhysicalPlanner::get_table_rows(QueryContext* ctx) {
    int64_t table_id = 0;
    if (ctx->get_tuple_desc(0)->has_table_id()) {
        table_id = ctx->get_tuple_desc(0)->table_id();
    } else {
        return -1;
    }
    SchemaFactory* factory = SchemaFactory::get_instance();
    TableInfo info = factory->get_table_info(table_id);
    pb::QueryRequest req;
    req.set_op_type(pb::QUERY_TABLE_FLATTEN);
    req.set_namespace_name(info.namespace_);
    req.set_database(ctx->cur_db);
    req.set_table_name(info.short_name);
    pb::QueryResponse res;
    MetaServerInteract::get_instance()->send_request("query", req, res);
    int64_t row_count = -1;
    if (res.flatten_tables_size() == 1) {
        row_count = res.flatten_tables(0).row_count();
    }
    DB_WARNING("table_id:%ld, namespace:%s, db:%s, table:%s, row_count:%ld", 
        table_id, info.namespace_.c_str(), ctx->cur_db.c_str(), info.short_name.c_str(), row_count);
    return row_count;
}

int PhysicalPlanner::execute(QueryContext* ctx, DataBuffer* send_buf) {
    int ret = 0;
    RuntimeState& state = *ctx->get_runtime_state();
    //DB_WARNING("state.client_conn(): %ld ,seq_id: %d", state.client_conn(), state.client_conn()->seq_id);
    ret = state.init(ctx, send_buf);
    if (ret < 0) {
        DB_FATAL("RuntimeState init fail");
         ctx->stat_info.error_code = state.error_code;
        return ret;
    }
    state.explain_type = ctx->explain_type;
    if (state.explain_type == ANALYZE_STATISTICS) {
        //如果为analyze模式需要初始化cmsketch
        state.cmsketch = std::make_shared<CMsketch>(FLAGS_cmsketch_depth, FLAGS_cmsketch_width);
        state.cmsketch->set_sample_rows(FLAGS_sample_rows);
        //为了获取准确的行数，给meta发请求
        int64_t table_rows = get_table_rows(ctx);
        if (table_rows < 0) {
            return -1;
        }
        state.cmsketch->set_table_rows(table_rows);
    }
    if (explain_is_trace(ctx->explain_type)) {
        ctx->trace_node.set_node_type(ctx->root->node_type());
        ctx->root->set_trace(&ctx->trace_node);
        ctx->root->create_trace();
    }
    
    ret = ctx->root->open(&state);
    if (ctx->root->get_trace() != nullptr) {
        DB_WARNING("execute:%s", ctx->root->get_trace()->ShortDebugString().c_str());
    }
    ctx->root->close(&state);
    if (ret < 0) {
        DB_WARNING("plan open fail: %d, %s", state.error_code, state.error_msg.str().c_str());
        ctx->stat_info.error_code = state.error_code;
        ctx->stat_info.error_msg.str(state.error_msg.str());
        ctx->stat_info.region_count = state.region_count;
        ctx->stat_info.num_scan_rows = state.num_scan_rows();
        return ret;
    }
    ctx->stat_info.error_code = state.error_code;
    ctx->update_ctx_stat_info(&state, ctx->get_ctx_total_time());
    return 0;
}

int PhysicalPlanner::full_export_start(QueryContext* ctx, DataBuffer* send_buf) {
    int ret = 0;
    RuntimeState& state = *ctx->get_runtime_state();
    
    ret = state.init(ctx, send_buf);
    if (ret < 0) {
        DB_FATAL("RuntimeState init fail");
        ctx->stat_info.error_code = state.error_code;
        return ret;
    }
    state.is_full_export = true;
    ret = ctx->root->open(&state);
    if (ret < 0) {
        DB_WARNING("plan open fail: %d, %s", state.error_code, state.error_msg.str().c_str());
        ctx->stat_info.error_code = state.error_code;
        ctx->stat_info.error_msg.str(state.error_msg.str());
        ctx->root->close(&state);
        return ret;
    }
    ret = full_export_next(ctx, send_buf, false);
    return ret;
}

int PhysicalPlanner::full_export_next(QueryContext* ctx, DataBuffer* send_buf, bool shutdown) {
    int ret = 0;
    RuntimeState& state = *ctx->get_runtime_state();
    PacketNode* root = (PacketNode*)(ctx->root);
    ret = root->get_next(&state);
    if (ret < 0) {
        root->close(&state);
        DB_WARNING("plan get_next fail: %d, %s", state.error_code, state.error_msg.str().c_str());
        ctx->stat_info.error_code = state.error_code;
        ctx->stat_info.error_msg.str(state.error_msg.str());
        return ret;
    }
    if (state.is_eos() || shutdown) {
        root->close(&state);
        ctx->stat_info.error_code = state.error_code;
        ctx->stat_info.error_msg.str(state.error_msg.str());
        ctx->update_ctx_stat_info(&state, ctx->get_ctx_total_time());
    }
    return 0;            
}
// insert user variables to record for prepared stmt
int PhysicalPlanner::insert_values_to_record(QueryContext* ctx) {
    if (ctx->stmt_type != parser::NT_INSERT || ctx->exec_prepared == false) {
        return 0;
    }
    ExecNode* plan = ctx->root;
    InsertNode* insert_node = static_cast<InsertNode*>(plan->get_node(pb::INSERT_NODE));
    if (insert_node == nullptr) {
        DB_WARNING("insert_node is null");
        return -1;
    }
    ctx->insert_records.clear();
    return insert_node->insert_values_for_prepared_stmt(ctx->insert_records);
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
