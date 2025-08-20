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
#include "db_service.h"
#include <boost/algorithm/string.hpp>
namespace baikaldb {
DEFINE_int32(cmsketch_depth, 5, "cmsketch_depth");
DEFINE_int32(cmsketch_width, 2048, "cmsketch_width");
DEFINE_int32(sample_rows, 1000000, "sample rows 100w");
DECLARE_bool(use_arrow_vector);
DECLARE_int64(print_time_us);
DEFINE_bool(enable_agg_pushdown, false, "enable agg pushdown");
DEFINE_bool(enable_columns_prune, false, "enable columns prune");
int PhysicalPlanner::analyze(QueryContext* ctx) {
    int ret = 0;
    // 配置dual_scan_node属性
    ret = config_dual_scan_nodes(ctx);
    if (ret < 0) {
        return ret;
    }
    // 无效列裁剪
    ret = ColumnsPrune().analyze(ctx);
    if (ret < 0) {
        return ret;
    }
    // 谓词下推需要先于子查询analyze执行, 可能生成新的table_filter
    ret = PredicatePushDown().analyze(ctx);
    if (ret < 0) {
        return ret;
    }
    // 聚合下推
    ret = AggPushDown().analyze(ctx);
    if (ret < 0) {
        return ret;
    }
    // 判断sql执行类型, 是否支持向量化\mpp等, 必须先于子查询analyze执行
    ret = ExecTypeAnalyzer().analyze(ctx);
    if (ret < 0) {
        return ret;
    }
    for (auto& kv : ctx->sub_query_plans) {
        ret = analyze(kv.get());
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
    // 判断join类型, 是否走no index join
    ret = JoinTypeAnalyzer().analyze(ctx);
    if (ret < 0) {
        return ret;
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
    // mpp添加Exchange等
    ret = MppAnalyzer().analyze(ctx);
    if (ret < 0) {
        return ret;
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

int PhysicalPlanner::execute_mpp(QueryContext* ctx, DataBuffer* send_buf) {
    int ret = 0;
    std::vector<std::shared_ptr<FragmentInfo>> fragment_need_exec;
    ON_SCOPE_EXIT(([&ret, &ctx]() {
        if (ret != 0) {
            // 发请求给非主db停止执行
            stop_mpp(ctx);
        }
        // 直接destory非fragment_0的plan tree
        for (auto& [fragment_id, fragment] : ctx->fragments) {
            if (fragment_id != 0 
                    && fragment->root != nullptr
                    && fragment->receivers.size() > 0) {
                fragment->root->close(fragment->runtime_state);
                ExecNode::destroy_tree(fragment->root);
            }
        }
        // 处理fragment 0
        SmartFragment fragment = ctx->fragments[0];
        RuntimeState* state = fragment->runtime_state;
        if (fragment->root != nullptr && !state->is_expr_subquery()) {
            fragment->root->close(state);
        }
        if (ret < 0) {
            std::string str = state->error_msg.str();
            DB_WARNING("plan open fail: %d, %s", state->error_code, str.c_str());
            ctx->stat_info.error_msg.str(str);
            ctx->stat_info.region_count = state->region_count;
            ctx->stat_info.num_scan_rows = state->num_scan_rows();
            ctx->stat_info.db_handle_bytes = state->db_handle_bytes();
            ctx->stat_info.db_handle_rows = state->db_handle_rows();
        } else {
            ctx->stat_info.error_code = state->error_code;
            ctx->update_ctx_stat_info(state, ctx->get_ctx_total_time());
        }
        if (ctx->client_conn != nullptr 
                && (ctx->is_plan_cache || ctx->stat_info.hit_cache)) {
            // 走mpp, 必须从缓存中删掉, 不然会core
            if (ctx->stat_info.hit_cache) {
                // 用的是缓存ctx中的plan, 设置need_destroy_tree为false, cache ctx析构时候不析构plan
                std::shared_ptr<QueryContext> cache_ctx;
                if (ctx->client_conn->non_prepared_plans.find(ctx->cache_key, &cache_ctx) == 0) {
                    if (cache_ctx != nullptr) {
                        cache_ctx->need_destroy_tree = false;
                    }
                }
            }
            ctx->client_conn->non_prepared_plans.del(ctx->cache_key);
        }
    }));

    // step1. 将计划发给其他db执行
    ret = send_fragment_to_other_db(ctx);
    if (ret != 0) {
        return ret;
    }
    // step2. 从fragment 0->max执行计划db fragment
    for (auto& [fragment_id, fragment] : ctx->fragments) {
        fragment->time_cost.reset();
        if (fragment->receivers.empty()) {
            continue;
        }
        if (fragment_id > 0) {
            // 同一个runtimestate跨fragment执行, 构建新的runtimestate, 避免复用有坑
            // 新runtimestate析构随着fragmentInfo析构
            SmartState new_state = std::make_shared<RuntimeState>();
            ret = new_state->init(fragment->runtime_state->ctx(), fragment->runtime_state->send_buf());
            if (ret != 0) {
                DB_FATAL("mpp execute fail: fragment:%d new state init fail", fragment->fragment_id);
                return ret;
            }
            new_state->execute_type = fragment->runtime_state->execute_type;
            new_state->vectorlized_parallel_execution = fragment->runtime_state->vectorlized_parallel_execution;
            new_state->is_simple_select = fragment->runtime_state->is_simple_select;
            new_state->sign_exec_type = fragment->runtime_state->sign_exec_type;
            fragment->smart_state = new_state;
            fragment->runtime_state = new_state.get();
        }
        if (fragment->root == nullptr || fragment->runtime_state == nullptr) {
            DB_FATAL("mpp execute fail: fragment root or runtime state is nullptr, log_id: %lu, idx: %d", fragment->log_id, fragment_id);
            return -1;
        }
        fragment->runtime_state->use_mpp = true;
        ret = fragment->root->open(fragment->runtime_state); 
        if (ret != 0) {
            DB_FATAL("mpp execute fail: fragment:%d open fail, ret: %d", fragment_id, ret);
            return ret;
        }
        ret = fragment->root->build_arrow_declaration(fragment->runtime_state);
        if (ret != 0) {
            DB_FATAL("mpp execute fail: fragment:%d build arrow declaration fail", fragment_id);
            return ret;
        }
        fragment_need_exec.push_back(fragment);
        fragment->set_open_cost();
    }
    DbService::get_instance()->fragment_internal_exec(fragment_need_exec, true);

    // step3. 等待fragment执行结束
    for (auto& [fragment_id, fragment] : ctx->fragments) {
        if (fragment->receivers.empty()) {
            continue;
        }
        // 从fragment0开始wait
        RuntimeState* state = fragment->runtime_state;
        auto result = fragment->wait();
        if (!result.ok()) {
            DB_FATAL("mpp execute fail: fragment: %d, result: %s", fragment_id, result.status().ToString().c_str());
        }
        if (fragment_id == 0) {
            if (result.ok()) {
                std::shared_ptr<arrow::Table> data = *result;
                ret = static_cast<PacketNode*>(fragment->root)->mpp_pack_rows(state, data);
            } else {
                ret = -1;
            }
        }
    }
    return ret;
}

int PhysicalPlanner::send_fragment_to_other_db(QueryContext* ctx) {
    RuntimeState& cur_state = *ctx->get_runtime_state();
    TimeCost time_cost;
    int64_t pack_time = 0;
    int64_t send_time = 0;
    for (auto iter : ctx->db_to_fragments) {
        const std::string& ith_db = iter.first;

        if (ith_db == SchemaFactory::get_instance()->get_address()) {
            continue;
        }
        time_cost.reset();
        pb::DAGFragmentRequest request;
        request.set_op(pb::OP_FRAGMENT_START);
        request.set_log_id(cur_state.log_id());
        request.set_sql_sign(cur_state.sign);
        // add fragment infos
        for (auto& fragment_id : iter.second) {
            pb::Plan fragment_plan;
            std::shared_ptr<FragmentInfo> fragment_info = ctx->fragments[fragment_id];
            pb::FragmentInfo* fragment_info_pb = request.add_fragments();
            fragment_info_pb->set_fragment_id(fragment_id);
            pb::RuntimeState* pb_rs = fragment_info_pb->mutable_runtime_state();
            fragment_info->runtime_state->to_proto(pb_rs);
            ExecNode::create_pb_plan(0, &fragment_plan, fragment_info->root);
            fragment_info_pb->mutable_plan()->CopyFrom(fragment_plan);
        }
        // add userinfo, 副db planrouter需要
        if (ctx->user_info != nullptr) {
            request.set_username(ctx->user_info->username);
        }
        pack_time += time_cost.get_time();
        time_cost.reset();
        int ret = DBInteract::get_instance()->handle_mpp_dag_fragment(request, ith_db);
        if (0 != ret) {
            DB_FATAL("logid: %lu, send request to other db fail, ret: %d", cur_state.log_id(), ret);
            return -1;
        } 
        send_time += time_cost.get_time();
    }
    if (pack_time > FLAGS_print_time_us || send_time > FLAGS_print_time_us) {
        DB_WARNING("send other db fragment, log_id: %lu, pack time: %ld, send time: %ld, db cnt: %lu", 
                    ctx->stat_info.log_id, pack_time, send_time, ctx->db_to_fragments.size());
    }
    return 0;
}

// 只有mpp主db执行失败/超时调用，停止所有db的fragment执行
int PhysicalPlanner::stop_mpp(QueryContext* ctx) {
    RuntimeState& cur_state = *ctx->get_runtime_state();
    for (auto iter : ctx->db_to_fragments) {
        pb::DAGFragmentRequest request;
        request.set_op(pb::OP_FRAGMENT_STOP);
        request.set_log_id(cur_state.log_id());
        if (ctx->user_info != nullptr) {
            request.set_username(ctx->user_info->username);
        }
        int ret = DBInteract::get_instance()->handle_mpp_dag_fragment(request, iter.first);
        if (0 != ret) {
            DB_FATAL("send request to other db for stop fail, ret: %d", ret);
            return -1;
        } 
    }
    return 0;
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
        state.statistics_types = std::make_shared<std::set<pb::StatisticType>>();
        std::vector<std::string> statistics_types;
        boost::split(statistics_types, ctx->format, boost::is_any_of(" "));
        for (int i = 1; i < statistics_types.size(); ++i) {
            const std::string& type = statistics_types[i];
            if (boost::iequals(type, "histogram")) {
                state.statistics_types->insert(pb::StatisticType::ST_HISTOGRAM);
            } else if (boost::iequals(type, "cmsketch")) {
                state.statistics_types->insert(pb::StatisticType::ST_CMSKETCH);
            } else if (boost::iequals(type, "hyperloglog")) {
                state.statistics_types->insert(pb::StatisticType::ST_HYPERLOGLOG);
            } else if (boost::iequals(type, "clear")) {
                // 清除meta的统计数据
                state.statistics_types->insert(pb::StatisticType::ST_CLEAR);
            } else if (boost::iequals(type, "all")) {
                state.statistics_types->insert(pb::StatisticType::ST_HISTOGRAM);
                state.statistics_types->insert(pb::StatisticType::ST_CMSKETCH);
                state.statistics_types->insert(pb::StatisticType::ST_HYPERLOGLOG);
            }
        }
        // 兜底，如果state.statistics_types 为空就默认选hist和cms
        if (state.statistics_types->empty()) {
            state.statistics_types->insert(pb::StatisticType::ST_HISTOGRAM);
            state.statistics_types->insert(pb::StatisticType::ST_CMSKETCH);
        }
        if (0 != state.statistics_types->count(pb::StatisticType::ST_CMSKETCH)) {
            //如果为analyze模式需要初始化cmsketch
            state.cmsketch = std::make_shared<CMsketch>(FLAGS_cmsketch_depth, FLAGS_cmsketch_width);
            int64_t table_rows = get_table_rows(ctx);
            if (table_rows < 0) {
                return -1;
            }
            state.cmsketch->set_table_rows(table_rows);
            state.cmsketch->set_sample_rows(FLAGS_sample_rows);
        }
        if (0 != state.statistics_types->count(pb::StatisticType::ST_HISTOGRAM)) {
            //为了获取准确的行数，给meta发请求
            int64_t table_rows = get_table_rows(ctx);
            if (table_rows < 0) {
                return -1;
            }
            state.table_rows = table_rows;
            state.sample_rows = FLAGS_sample_rows;
        }
        if (0 != state.statistics_types->count(pb::StatisticType::ST_HYPERLOGLOG)) {
            state.hll = std::make_shared<HyperLogLog>();
        }
    }
    if (explain_is_trace(ctx->explain_type)) {
        ctx->trace_node.set_node_type(ctx->root->node_type());
        ctx->root->set_trace(&ctx->trace_node);
        ctx->root->create_trace();
        for (auto& subquery : ctx->sub_query_plans) {
            subquery->root->set_trace(&ctx->trace_node);
            subquery->root->create_trace();
        }
    }
    if (ctx->use_mpp) {
        return execute_mpp(ctx, send_buf);
    }
    ret = ctx->root->open(&state);
    if (ctx->root->get_trace() != nullptr) {
        DB_WARNING("execute:%s", ctx->root->get_trace()->ShortDebugString().c_str());
    }
    ctx->root->close(&state);
    if (ret < 0) {
        std::string str = state.error_msg.str();
        DB_WARNING("plan open fail: %d, %s", state.error_code, str.c_str());
        ctx->stat_info.error_code = state.error_code;
        ctx->stat_info.error_msg.str(str);
        ctx->stat_info.region_count = state.region_count;
        ctx->stat_info.num_scan_rows = state.num_scan_rows();
        ctx->stat_info.db_handle_bytes = state.db_handle_bytes();
        ctx->stat_info.db_handle_rows = state.db_handle_rows();
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
        std::string str = state.error_msg.str();
        DB_WARNING("plan open fail: %d, %s", state.error_code, str.c_str());
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
        std::string str = state.error_msg.str();
        DB_WARNING("plan get_next fail: %d, %s", state.error_code, str.c_str());
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
// 用于配置所有DualScanNode属性
int PhysicalPlanner::config_dual_scan_nodes(QueryContext* ctx) {
    if (ctx == nullptr) {
        DB_WARNING("ctx is nullptr");
        return -1;
    }
    // 只在最外层查询配置DualScanNode属性
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

    std::map<int32_t, std::shared_ptr<QueryContext>> all_derived_table_ctx_mapping;
    ctx->get_all_derived_table_ctx_mapping(all_derived_table_ctx_mapping);
    std::map<int32_t, std::map<int32_t, int32_t>> all_slot_column_mapping;
    ctx->get_all_slot_column_mapping(all_slot_column_mapping);

    std::vector<ExecNode*> dual_scan_nodes;
    dual_scan_nodes.reserve(10);
    plan->get_node(pb::DUAL_SCAN_NODE, dual_scan_nodes);
    for (auto& kv : all_derived_table_ctx_mapping) {
        auto derived_table_ctx = kv.second;
        if (derived_table_ctx == nullptr) {
            DB_WARNING("derived_table_ctx is nullptr");
            return -1;
        }
        ExecNode* derived_plan = derived_table_ctx->root;
        if (derived_plan == nullptr) {
            DB_WARNING("derived_plan is nullptr");
            return -1;
        }
        std::vector<ExecNode*> dual_scan_nodes_tmp;
        derived_plan->get_node(pb::DUAL_SCAN_NODE, dual_scan_nodes_tmp);
        dual_scan_nodes.insert(dual_scan_nodes.end(), dual_scan_nodes_tmp.begin(), dual_scan_nodes_tmp.end());
    }

    for (auto* node : dual_scan_nodes) {
        DualScanNode* dual_scan_node = static_cast<DualScanNode*>(node);
        if (dual_scan_node == nullptr) {
            DB_WARNING("dual_scan_node is nullptr");
            return -1;
        }
        if (!dual_scan_node->pb_node().has_derive_node()) {
            // SELECT xxx FROM DUAL场景
            continue;
        }
        const int32_t derived_tuple_id = dual_scan_node->tuple_id();
        auto iter = all_derived_table_ctx_mapping.find(derived_tuple_id);
        if (iter == all_derived_table_ctx_mapping.end()) {
            DB_WARNING("Fail to find subquery ctx, tuple_id: %d", derived_tuple_id);
            continue;
        }
        auto sub_query_ctx = iter->second;
        if (sub_query_ctx == nullptr) {
            DB_WARNING("sub_query_ctx is nullptr");
            return -1;
        }
        ExecNode* sub_query_plan = sub_query_ctx->root;
        if (sub_query_plan == nullptr) {
            DB_WARNING("sub_query_plan is nullptr");
            return -1;
        }
        PacketNode* packet_node = static_cast<PacketNode*>(sub_query_plan->get_node(pb::PACKET_NODE));
        if (packet_node == nullptr) {
            DB_WARNING("packet_node is nullptr");
            return -1;
        }
        // Union节点下的所有子查询共用一个slot_column_mapping
        const int32_t slot_tuple_id = dual_scan_node->slot_tuple_id();
        dual_scan_node->set_slot_column_mapping(all_slot_column_mapping[slot_tuple_id]);
        dual_scan_node->set_sub_query_node(sub_query_ctx->root);
        dual_scan_node->set_sub_query_ctx(sub_query_ctx.get());
        dual_scan_node->set_has_subquery(true);
        if (dual_scan_node->create_derived_table_projections(packet_node->pb_node()) != 0) {
            DB_WARNING("Fail to create_derived_table_projections");
            return -1;
        }
    }
    return 0;
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
