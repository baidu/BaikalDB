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

#include "logical_planner.h"
#include <memory>
#include <algorithm>
#include <iterator>
#include <boost/algorithm/string.hpp>
#include "common.h"
#include "select_planner.h"
#include "insert_planner.h"
#include "delete_planner.h"
#include "update_planner.h"
#include "union_planner.h"
#include "ddl_planner.h"
#include "load_planner.h"
#include "setkv_planner.h"
#include "transaction_planner.h"
#include "kill_planner.h"
#include "prepare_planner.h"
#include "predicate.h"
#include "parser.h"
#include "mysql_err_code.h"
#include "physical_planner.h"
#include "vectorize_helpper.h"

namespace bthread {
DECLARE_int32(bthread_concurrency); //bthread.cpp
}

namespace baikaldb {
DEFINE_bool(enable_plan_cache, false, "enable plan cache");
DEFINE_bool(enable_convert_charset, false, "enable convert charset");
DECLARE_string(log_plat_name);
DECLARE_bool(enable_dblink);

std::map<parser::JoinType, pb::JoinType> LogicalPlanner::join_type_mapping {
        { parser::JT_NONE, pb::NULL_JOIN},    
        { parser::JT_INNER_JOIN, pb::INNER_JOIN},
        { parser::JT_LEFT_JOIN, pb::LEFT_JOIN},
        { parser::JT_RIGHT_JOIN, pb::RIGHT_JOIN},
        { parser::JT_FULL_JOIN, pb::FULL_JOIN}
    };

int LogicalPlanner::create_n_ary_predicate(const parser::FuncExpr* func_item, 
        pb::Expr& expr,
        pb::ExprNodeType type,
        const CreateExprOptions& options) {
    if (func_item == nullptr) {
        DB_FATAL("func_item is null");
        return -1;
    }
    if (func_item->has_subquery()) {
        return handle_scalar_subquery(func_item, expr, options);
    }
    pb::ExprNode* node = expr.add_nodes();
    node->set_col_type(pb::BOOL);
    //split NOT_NULL/NOT_IN/NOT_LIKE into two predicates
    if (func_item->is_not) {
        node->set_node_type(pb::NOT_PREDICATE);
        node->set_num_children(1);
        pb::Function* item = node->mutable_fn();
        item->set_name("logic_not");
        item->set_fn_op(parser::FT_LOGIC_NOT);

        //add real predicate (OP_IS_NULL/OP_IN/OP_LIKE) node
        node = expr.add_nodes();
        node->set_col_type(pb::BOOL);
    }
    if (type == pb::LIKE_PREDICATE) {
        node->set_charset(_ctx->charset);
    }
    node->set_node_type(type);
    pb::Function* func = node->mutable_fn();
    if (func_item->fn_name.empty()) {
        DB_WARNING("op:%d fn_name is empty", func_item->func_type);
        return -1;
    }
    func->set_name(func_item->fn_name.c_str());
    func->set_fn_op(func_item->func_type);

    if (type == pb::OR_PREDICATE || type == pb::AND_PREDICATE) {
        // put node into node_list
        std::vector<parser::Node*> node_list;
        std::stack<parser::Node*> node_stack;
        for (int32_t idx = 0; idx < func_item->children.size(); ++idx) {
            node_stack.push(func_item->children[idx]);
        }

        while (!node_stack.empty()) {
            parser::Node* item = node_stack.top();
            node_stack.pop();
            const parser::ExprNode* child_expr_item = static_cast<const parser::ExprNode*>(item);
            if (child_expr_item == nullptr) {
                DB_FATAL("child_expr_item is null");
                return -1;
            }
            if (child_expr_item->expr_type == parser::ET_FUNC) {
                parser::FuncExpr* child_func_item = (parser::FuncExpr*)child_expr_item;
                if ((type == pb::OR_PREDICATE && child_func_item->func_type == parser::FT_LOGIC_OR)
                || (type == pb::AND_PREDICATE && child_func_item->func_type == parser::FT_LOGIC_AND)) {
                    for (int32_t idx = 0; idx < child_func_item->children.size(); ++idx) {
                        node_stack.push(child_func_item->children[idx]);
                    }
                } else {
                    node_list.emplace_back(item);
                }
            } else {
                node_list.emplace_back(item);
            }
        }
        // 从后往前加保证顺序一致
        for (int i = node_list.size() - 1; i >= 0; i--) {
            if (0 != create_expr_tree(node_list[i], expr, options)) {
                DB_WARNING("create ornode child expr failed");
                return -1;
            }
        }
        node->set_num_children(node_list.size());

    } else {
        for (int32_t idx = 0; idx < func_item->children.size(); ++idx) {
            if (0 != create_expr_tree(func_item->children[idx], expr, options)) {
                DB_WARNING("create child expr failed");
                return -1;
            }
        }
        node->set_num_children(func_item->children.size());
    }
    return 0;
}

int LogicalPlanner::construct_in_predicate_node(const parser::FuncExpr* func_item, pb::Expr& expr, pb::ExprNode** node) {
    pb::ExprNode* node_val = *node;
    node_val->set_col_type(pb::BOOL);
    //split NOT_NULL/NOT_IN/NOT_LIKE into two predicates
    if (func_item->is_not) {
        node_val->set_node_type(pb::NOT_PREDICATE);
        node_val->set_num_children(1);
        pb::Function* item = node_val->mutable_fn();
        item->set_name("logic_not");
        item->set_fn_op(parser::FT_LOGIC_NOT);

        //add real predicate (OP_IS_NULL/OP_IN/OP_LIKE) node
        node_val = expr.add_nodes();
        node_val->set_col_type(pb::BOOL);
    }
    node_val->set_node_type(pb::IN_PREDICATE);
    pb::Function* func = node_val->mutable_fn();
    if (func_item->fn_name.empty()) {
        DB_WARNING("op:%d fn_name is empty", func_item->func_type);
        return -1;
    }
    func->set_name(func_item->fn_name.value); // in
    func->set_fn_op(func_item->func_type); // FT_IN
    *node = node_val;
    return 0;
}

int LogicalPlanner::handle_in_subquery(const parser::FuncExpr* func_item,
        pb::Expr& expr,
        const CreateExprOptions& options) {
    int row_expr_size = 1;
    parser::ExprNode* arg1 = (parser::ExprNode*)func_item->children[0];
    if (arg1->expr_type == parser::ET_ROW_EXPR) {
        row_expr_size = arg1->children.size();
    }
    parser::ExprNode* arg2 = (parser::ExprNode*)func_item->children[1];
    if (arg1->is_subquery() || !arg2->is_subquery()) {
        if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
            _ctx->stat_info.error_code = ER_NOT_SUPPORTED_YET;
            _ctx->stat_info.error_msg << "only support subquery at right side, e.g: expr in (subquery)";
        }
        return -1;
    }
    pb::Expr tmp_expr;
    int ret = create_common_subquery_expr((parser::SubqueryExpr*)arg2, tmp_expr, options, _is_correlate_subquery_expr);
    if (ret < 0) {
        DB_WARNING("create child expr failed");
        return -1;
    }
    if (!_is_correlate_subquery_expr) {
        pb::ExprNode* node = expr.add_nodes();
        ret = construct_in_predicate_node(func_item, expr, &node);
        if (ret < 0) {
            return -1;
        }

        if (0 != create_expr_tree(arg1, expr, options)) {
            DB_WARNING("create child 1 expr failed");
            return -1;
        }
        int row_count = 0;
        for (int i = 0; i < tmp_expr.nodes_size(); i++) {
            pb::ExprNode* child_node = expr.add_nodes();
            child_node->CopyFrom(tmp_expr.nodes(i));
            if (tmp_expr.nodes(i).node_type() == pb::ROW_EXPR) {
                if (tmp_expr.nodes(i).num_children() != row_expr_size) {
                    DB_WARNING("Operand should contain %d column(s)", row_expr_size);
                    if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                        _ctx->stat_info.error_code = ER_OPERAND_COLUMNS;
                        _ctx->stat_info.error_msg << "Operand should contain " << row_expr_size << " column(s)";
                    }
                    return -1;
                }
                row_count++;
            }
        }
        // pb是打平的node，需要计数
        if (row_count > 0) {
            node->set_num_children(row_count + 1);
        } else {
            if (row_expr_size != 1) {
                DB_WARNING("Operand should contain %d column(s)", row_expr_size);
                if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                    _ctx->stat_info.error_code = ER_OPERAND_COLUMNS;
                    _ctx->stat_info.error_msg << "Operand should contain " << row_expr_size << " column(s)";
                }
                return -1;
            }
            node->set_num_children(tmp_expr.nodes_size() + 1);
        }
    } else {
        pb::ExprNode* node = expr.add_nodes();
        node->set_node_type(pb::FUNCTION_CALL);
        node->set_col_type(pb::INVALID_TYPE);
        node->set_num_children(2);
        pb::JoinType join_type = pb::SEMI_JOIN;
        pb::Function* func = node->mutable_fn();
        func->set_name("eq");
        func->set_fn_op(parser::FT_EQ);
        if (func_item->is_not) {
            join_type = pb::ANTI_SEMI_JOIN;
        }
        if (0 != create_expr_tree(arg1, expr, options)) {
            DB_WARNING("create child 1 expr failed");
            return -1;
        }
        CreateExprOptions tm_options = options;
        tm_options.row_expr_size = row_expr_size;
        ret = construct_apply_node(_cur_sub_ctx.get(), expr, join_type, tm_options);
        if (ret < 0) {
            DB_WARNING("construct apply node failed");
            return -1;
        }
    }
    return 0;
}

int LogicalPlanner::create_in_predicate(const parser::FuncExpr* func_item, 
        pb::Expr& expr,
        const CreateExprOptions& options) {
    if (func_item->has_subquery()) {
        return handle_in_subquery(func_item, expr, options);
    }
    pb::ExprNode* node = expr.add_nodes();
    int ret = construct_in_predicate_node(func_item, expr, &node);
    if (ret < 0) {
        return -1;
    }

    parser::ExprNode* arg1 = (parser::ExprNode*)func_item->children[0];
    if (0 != create_expr_tree(arg1, expr, options)) {
        DB_WARNING("create child 1 expr failed");
        return -1;
    }
    int row_expr_size = 1;
    if (arg1->expr_type == parser::ET_ROW_EXPR) {
        row_expr_size = arg1->children.size();
    }
    parser::ExprNode* arg2 = (parser::ExprNode*)func_item->children[1];
    for (int i = 0; i < arg2->children.size(); i++) {
        auto node = static_cast<const parser::ExprNode*>(arg2->children[i]);
        if (node->expr_type == parser::ET_ROW_EXPR) {
            if (row_expr_size != arg2->children[i]->children.size()) {
                DB_WARNING("Operand should contain %d column(s)", row_expr_size);
                if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                    _ctx->stat_info.error_code = ER_OPERAND_COLUMNS;
                    _ctx->stat_info.error_msg << "Operand should contain " << row_expr_size << " column(s)";
                }
                return -1;
            }
        } else if (row_expr_size != 1) {
            DB_WARNING("Operand should contain %d column(s)", row_expr_size);
            if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                _ctx->stat_info.error_code = ER_OPERAND_COLUMNS;
                _ctx->stat_info.error_msg << "Operand should contain " << row_expr_size << " column(s)";
            }
            return -1;
        }
        if (0 != create_expr_tree(arg2->children[i], expr, options)) {
            DB_WARNING("create child expr failed");
            return -1;
        }
    }
    node->set_num_children(arg2->children.size() + 1);
    return 0;
}

// TODO move to a caller
// static, planner entrance 
int LogicalPlanner::analyze(QueryContext* ctx) {
    if (!ctx) {
        return -1;
    }
    TimeCost cost;
    if (ctx->mysql_cmd == COM_STMT_PREPARE 
            || ctx->mysql_cmd == COM_STMT_EXECUTE 
            || ctx->mysql_cmd == COM_STMT_CLOSE) {

        if (ctx->mysql_cmd == COM_STMT_PREPARE) {
            ctx->stmt_type = parser::NT_NEW_PREPARE;
        } else if (ctx->mysql_cmd == COM_STMT_EXECUTE) {
            ctx->stmt_type = parser::NT_EXEC_PREPARE;
        } else if (ctx->mysql_cmd == COM_STMT_CLOSE) {
            ctx->stmt_type = parser::NT_DEALLOC_PREPARE;
        }
        std::unique_ptr<LogicalPlanner> planner;
        planner.reset(new PreparePlanner(ctx));
        if (planner->plan() != 0) {
            DB_WARNING("gen stmt plan failed, mysql_type:%d, stmt_type:%d", ctx->mysql_cmd, ctx->stmt_type);
            return -1;
        }
        return 0;
    }
    parser::SqlParser parser;
    parser.charset = ctx->charset;
    parser.parse(ctx->sql);
    if (parser.error != parser::SUCC) {
        ctx->stat_info.error_code = ER_SYNTAX_ERROR;
        ctx->stat_info.error_msg << "syntax error! errno: " << parser.error
                                 << " errmsg: " << parser.syntax_err_str;
        DB_WARNING("parsing error! errno: %d, errmsg: %s, sql: %s", 
            parser.error, 
            parser.syntax_err_str.c_str(),
            ctx->sql.c_str());
        return -1;
    }
    if (parser.result.size() != 1) {
        DB_WARNING("multi-stmt is not supported, sql: %s", ctx->sql.c_str());
        ctx->stat_info.error_code = ER_NOT_SUPPORTED_YET;
        ctx->stat_info.error_msg << "multi-stmt is not supported";
        return -1;
    }
    if (parser.result[0] == nullptr) {
        DB_WARNING("sql parser stmt is null, sql: %s", ctx->sql.c_str());
        return -1;
    }
    if (parser.result[0]->node_type == parser::NT_EXPLAIN) {
        if (ctx->client_conn->txn_id != 0) {
            ctx->stat_info.error_code = ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG;
            ctx->stat_info.error_msg << "explain statement not allow in transaction";
            DB_WARNING("explain statement not allow in transaction txn_id:%lu", ctx->client_conn->txn_id);
            return -1;
        }
        parser::ExplainStmt* stmt = static_cast<parser::ExplainStmt*>(parser.result[0]);
        ctx->stmt = stmt->stmt;
        std::string format(stmt->format.c_str());
        ctx->format = format;
        if (format == "trace") {
            ctx->explain_type = SHOW_TRACE;
        } else if (format == "trace2") {
            ctx->explain_type = SHOW_TRACE2;
        } else if (format == "plan") {
            ctx->explain_type = SHOW_PLAN;
            ctx->is_explain = true;
        } else if (boost::istarts_with(format, "analyze")) { 
            ctx->explain_type = ANALYZE_STATISTICS;
        } else if (format == "histogram") {
            ctx->explain_type = SHOW_HISTOGRAM;
            ctx->is_explain = true;
        } else if (format == "hyperloglog") {
            ctx->explain_type = SHOW_HLL;
            ctx->is_explain = true;
        } else if (format == "cmsketch") {
            ctx->explain_type = SHOW_CMSKETCH;
            ctx->is_explain = true;
        } else if (format == "show_cost") {
            ctx->explain_type = EXPLAIN_SHOW_COST;
        } else if (format == "sign") {
            ctx->client_conn->is_explain_sign = true;
            ctx->explain_type = SHOW_SIGN;
            ctx->is_explain = true;
        } else if (format == "keypoint") {
            ctx->explain_type = SHOW_KEYPOINT;
            ctx->is_get_keypoint = true;
        } else if (format == "sample_keypoint") {
            ctx->explain_type = SAMPLE_KEYPOINT;
            ctx->is_get_keypoint = true;
        } else if (boost::istarts_with(format, "hint")) {
            ctx->is_explain = true;
            ctx->explain_hint = std::make_shared<ExplainHint>(format);
        } else {
            ctx->is_explain = true;
        }
        //DB_WARNING("stmt format:%s", format.c_str());
    } else {
        ctx->stmt = parser.result[0];
        ctx->is_complex = ctx->stmt->is_complex_node();
    }
    ctx->stmt_type = ctx->stmt->node_type;
    if (ctx->stmt_type != parser::NT_SELECT
         && ctx->stmt_type != parser::NT_UNION
         && ctx->explain_type != EXPLAIN_NULL 
         && ctx->explain_type != SHOW_PLAN 
         && ctx->explain_type != SHOW_SIGN) {
        ctx->stat_info.error_code = ER_NOT_SUPPORTED_YET;
        ctx->stat_info.error_msg << "dml only support normal explain";
        DB_WARNING("dml only support normal explain");
        return -1;
    }

    std::unique_ptr<LogicalPlanner> planner;
    switch (ctx->stmt_type) {
    case parser::NT_SELECT:
        ctx->is_select = true;
        planner.reset(new SelectPlanner(ctx));
        break;
    case parser::NT_INSERT:
        planner.reset(new InsertPlanner(ctx));
        break;
    case parser::NT_UPDATE:
        planner.reset(new UpdatePlanner(ctx));
        break;
    case parser::NT_DELETE:
    case parser::NT_TRUNCATE:
        planner.reset(new DeletePlanner(ctx));
        break;
    case parser::NT_CREATE_TABLE:
    case parser::NT_CREATE_VIEW:
    case parser::NT_CREATE_DATABASE:
    case parser::NT_DROP_TABLE:
    case parser::NT_DROP_VIEW:
    case parser::NT_RESTORE_TABLE:
    case parser::NT_DROP_DATABASE:
    case parser::NT_ALTER_TABLE:
    case parser::NT_CREATE_NAMESPACE:
    case parser::NT_DROP_NAMESPACE:
    case parser::NT_ALTER_NAMESPACE:
    case parser::NT_CREATE_USER:
    case parser::NT_DROP_USER:
    case parser::NT_ALTER_USER:
    case parser::NT_GRANT:
    case parser::NT_REVOKE:
    case parser::NT_ALTER_VIEW:
        planner.reset(new DDLPlanner(ctx));
        ctx->succ_after_logical_plan = true;
        break;
    case parser::NT_SET_CMD:
        planner.reset(new SetKVPlanner(ctx));
        break;
    case parser::NT_START_TRANSACTION:
    case parser::NT_COMMIT_TRANSACTION:
    case parser::NT_ROLLBACK_TRANSACTION:
        planner.reset(new TransactionPlanner(ctx));
        break;
    case parser::NT_NEW_PREPARE:
    case parser::NT_EXEC_PREPARE:
    case parser::NT_DEALLOC_PREPARE:
        planner.reset(new PreparePlanner(ctx));
        break;
    case parser::NT_KILL:
        planner.reset(new KillPlanner(ctx));
        break;
    case parser::NT_UNION:
        planner.reset(new UnionPlanner(ctx));
        break;
    case parser::NT_LOAD_DATA:
        planner.reset(new LoadPlanner(ctx));
        break;
    default:
        DB_WARNING("invalid command type: %d", ctx->stmt_type);
        return -1;
    }
    if ((FLAGS_enable_plan_cache || ctx->user_info->enable_plan_cache) && 
            parser.result[0]->node_type != parser::NT_EXPLAIN && ctx->stmt_type == parser::NT_SELECT) {
        if (planner->plan_cache_get() != 0) {
            DB_WARNING("Fail to plan_cache_get");
            return -1;
        }
        if (ctx->stat_info.hit_cache) {
            return 0;
        }
    }
    if (planner->plan() != 0) {
        DB_WARNING("gen plan failed, type:%d", ctx->stmt_type);
        return -1;
    }
    if (ctx->stat_info.sign == 0) {
        int ret = planner->generate_sql_sign(ctx, ctx->stmt);
        if (ret < 0) {
            return -1;
        }
    }
    if ((FLAGS_enable_plan_cache || ctx->user_info->enable_plan_cache) && 
            parser.result[0]->node_type != parser::NT_EXPLAIN && ctx->stmt_type == parser::NT_SELECT) {
        if (planner->plan_cache_add() != 0) {
            DB_WARNING("Fail to plan_cache_get");
            return -1;
        }
    }

    return 0;
}

int LogicalPlanner::generate_sql_sign(QueryContext* ctx, parser::StmtNode* stmt) {
    pb::OpType op_type = pb::OP_NONE;
    if (ctx->plan.nodes_size() > 0 && ctx->plan.nodes(0).node_type() == pb::PACKET_NODE) {
        op_type = ctx->plan.nodes(0).derive_node().packet_node().op_type();
    }
    auto stat_info = &(ctx->stat_info);
    if (!stat_info->family.empty() && !stat_info->table.empty()) {
        std::string str;
        if (stat_info->sign == 0) {
            stmt->set_print_sample(true);
            stat_info->sample_sql << "family_table_tag_optype_plat=[" << stat_info->family << "\t"
                << stat_info->table << "\t" << stat_info->resource_tag << "\t" << op_type << "\t"
                << FLAGS_log_plat_name << "] sql=[" << stmt << "]";
            uint64_t out[2];
            str = stat_info->sample_sql.str();
            butil::MurmurHash3_x64_128(str.c_str(), str.size(), 0x1234, out);
            stat_info->sign = out[0];
        }
        if (!ctx->sign_blacklist.empty()) {
            if (ctx->sign_blacklist.count(stat_info->sign) > 0) {
                DB_WARNING("sql sign[%lu] in blacklist, sample_sql[%s]", stat_info->sign, 
                    str.c_str());
                ctx->stat_info.error_code = ER_SQL_REFUSE;
                ctx->stat_info.error_msg << "sql sign: " << stat_info->sign << " in blacklist";
                return -1;
            }
        }
        if (!ctx->need_learner_backup && !ctx->sign_forcelearner.empty()) {
            if (ctx->sign_forcelearner.count(stat_info->sign) > 0) {
                ctx->need_learner_backup = true;
                DB_WARNING("sql sign[%lu] in forcelearner, sample_sql[%s]", stat_info->sign, 
                    str.c_str());
            }
        }
        if (!ctx->sign_forceindex.empty()
                && (!ctx->is_explain
                        || ctx->explain_hint == nullptr
                        || !ctx->explain_hint->get_flag<ExplainHint::HintType::NO_FORCE>())) {
            if (ctx->sign_forceindex.count(stat_info->sign) > 0) {
                auto& table_index_map = ctx->sign_forceindex[stat_info->sign];
                for (int i = 0; i < ctx->plan.nodes_size(); i++) {
                    auto node = ctx->plan.mutable_nodes(i);
                    if (node->node_type() != pb::SCAN_NODE) {
                        continue;
                    }
                    pb::DerivePlanNode* derive = node->mutable_derive_node();
                    pb::ScanNode* scan = derive->mutable_scan_node();
                    int64_t table_id = scan->table_id();
                    if (table_index_map.count(table_id) > 0) {
                        for (auto& index_name: table_index_map[table_id]) {
                            int64_t index_id = 0;
                            auto ret = _factory->get_index_id(table_id, index_name, index_id);
                            if (ret != 0) {
                                DB_WARNING("index_name: %s in table:%s not exist", index_name.c_str(),
                                           _factory->get_table_info_ptr(table_id)->name.c_str());
                                continue;
                            }
                            auto indexInfoPtr = _factory->get_index_info_ptr(index_id);
                            if (indexInfoPtr == nullptr || indexInfoPtr->state != pb::IS_PUBLIC || indexInfoPtr->index_hint_status != pb::IHS_NORMAL) {
                                continue;
                            }
                            scan->add_force_indexes(index_id);
                        }
                    }
                }
            }
        }
    }
    return 0;
}

int LogicalPlanner::gen_subquery_plan(parser::DmlNode* subquery, SmartPlanTableCtx plan_state,
        const ExprParams& expr_params) {
    _cur_sub_ctx = std::make_shared<QueryContext>();
    auto client = _ctx->client_conn;
    _cur_sub_ctx->stmt = subquery;
    _cur_sub_ctx->expr_params = expr_params;
    _cur_sub_ctx->stmt_type = subquery->node_type;
    _cur_sub_ctx->cur_db = _ctx->cur_db;
    _cur_sub_ctx->is_explain = _ctx->is_explain;
    _cur_sub_ctx->explain_hint = _ctx->explain_hint;
    _cur_sub_ctx->explain_type = _ctx->explain_type;
    _cur_sub_ctx->stat_info.log_id = _ctx->stat_info.log_id;
    _cur_sub_ctx->user_info = _ctx->user_info;
    _cur_sub_ctx->row_ttl_duration = _ctx->row_ttl_duration;
    _cur_sub_ctx->is_complex = _ctx->is_complex;
    _cur_sub_ctx->get_runtime_state()->set_client_conn(client);
    _cur_sub_ctx->client_conn = client;
    _cur_sub_ctx->sql = subquery->to_string();
    _cur_sub_ctx->charset = _ctx->charset;
    _cur_sub_ctx->is_from_subquery = expr_params.is_from_subquery;
    _cur_sub_ctx->is_full_export= _ctx->is_full_export;
    _cur_sub_ctx->sub_query_level = ++_unique_id_ctx->sub_query_level; //不同层级的子查询同名表不冲突
    _cur_sub_ctx->is_with = _ctx->is_with;
    _cur_sub_ctx->is_create_view = _ctx->is_create_view;
    _cur_sub_ctx->table_with_clause_mapping = _ctx->table_with_clause_mapping;
    _cur_sub_ctx->is_union_subquery = expr_params.is_union_subquery;
    _cur_sub_ctx->efsearch = _ctx->efsearch;
    // from子查询完全ctx完全独立
    if (expr_params.is_from_subquery || expr_params.is_union_subquery) {
        plan_state.reset(new (std::nothrow)PlanTableContext);
    }
    std::unique_ptr<LogicalPlanner> planner;
    if (_cur_sub_ctx->stmt_type == parser::NT_SELECT) {
        planner.reset(new SelectPlanner(_cur_sub_ctx.get(), _unique_id_ctx, plan_state));
    } else if (_cur_sub_ctx->stmt_type == parser::NT_UNION) {
        planner.reset(new UnionPlanner(_cur_sub_ctx.get(), _unique_id_ctx, plan_state));
    }
    if (planner->plan() != 0) {
        _ctx->stat_info.error_code = _cur_sub_ctx->stat_info.error_code;
        _ctx->stat_info.error_msg << _cur_sub_ctx->stat_info.error_msg.str();
        DB_WARNING("gen plan failed, type:%d", _cur_sub_ctx->stmt_type);
        return -1;
    }

    if (_ctx->stat_info.family.empty()) {
        _ctx->stat_info.family = _cur_sub_ctx->stat_info.family;
    }
    if (_ctx->stat_info.table.empty()) {
        _ctx->stat_info.table = _cur_sub_ctx->stat_info.table;
    }
    if (_ctx->stat_info.resource_tag.empty()) {
        _ctx->stat_info.resource_tag = _cur_sub_ctx->stat_info.resource_tag;
    }
    _ctx->cur_db = _cur_sub_ctx->cur_db;
    if (!expr_params.is_expr_subquery) {
        if (_select_names.size() == 0) {
            _select_names = planner->select_names();
        }
    }
    _cur_sub_ctx->expr_params.row_filed_number = planner->select_names().size();
    _ctx->set_kill_ctx(_cur_sub_ctx);
    auto stat_info = &(_cur_sub_ctx->stat_info);

    int ret = 0;
    if (!_ctx->is_with && !_ctx->is_create_view) {
        ret = generate_sql_sign(_cur_sub_ctx.get(), subquery);
        if (ret < 0) {
            return -1;
        }
    }
    auto& client_conn = _ctx->client_conn;
    client_conn->insert_subquery_sign(stat_info->sign);
    ret = _cur_sub_ctx->create_plan_tree();
    if (ret < 0) {
        DB_WARNING("Failed to pb_plan to execnode");
        return -1;
    }

    if (_cur_sub_ctx->table_can_use_arrow_vectorize == false) {
        _ctx->table_can_use_arrow_vectorize = false;
    }
    _ctx->sign_blacklist.insert(_cur_sub_ctx->sign_blacklist.begin(), _cur_sub_ctx->sign_blacklist.end());
    _ctx->sign_forcelearner.insert(_cur_sub_ctx->sign_forcelearner.begin(), _cur_sub_ctx->sign_forcelearner.end());
    _ctx->sign_rolling.insert(_cur_sub_ctx->sign_rolling.begin(), _cur_sub_ctx->sign_rolling.end());
    _ctx->sign_forceindex.insert(_cur_sub_ctx->sign_forceindex.begin(), _cur_sub_ctx->sign_forceindex.end());
    _ctx->sign_exec_type.insert(_cur_sub_ctx->sign_exec_type.begin(), _cur_sub_ctx->sign_exec_type.end());
    return 0;
}

int LogicalPlanner::add_derived_table(const std::string& database, const std::string& table,
        const std::string& alias) {
    DatabaseInfo db;
    db.name = database;
    db.namespace_ = _ctx->user_info->namespace_;
    if (_plan_table_ctx->table_dbs_mapping[try_to_lower(table)].count(database) == 0) {
        _plan_table_ctx->table_dbs_mapping[try_to_lower(table)].emplace(database);
    }
    _plan_table_ctx->database_info.emplace(try_to_lower(database), db);
    SmartTable tbl_info_ptr = std::make_shared<TableInfo>();
    TableInfo& tbl_info = *tbl_info_ptr;
    tbl_info.id = --_unique_id_ctx->derived_table_id;
    tbl_info.namespace_ = _ctx->user_info->namespace_;
    std::string table_name = database + "." + table;
    tbl_info.name = table_name;
    bool ok = _plan_table_ctx->table_info.emplace(try_to_lower(table_name), tbl_info_ptr).second;
    if (ok) {
        ScanTupleInfo* tuple_info = get_scan_tuple(table_name, tbl_info.id);
        _ctx->derived_table_ctx_mapping[tuple_info->tuple_id] = _ctx->sub_query_plans.back();
        _plan_table_ctx->derived_table_ctx_mapping[tuple_info->tuple_id] = _ctx->sub_query_plans.back();
        _ctx->current_tuple_ids.emplace(tuple_info->tuple_id);
        _ctx->current_table_tuple_ids.emplace(tuple_info->tuple_id);
        _table_names.emplace(table_name);
        //_table_alias_mapping.emplace(table, table_name);
        int field_id = 1;
        for (auto& field_name : _select_names) {
            FieldInfo field_info;
            field_info.id = field_id++;
            field_info.table_id = tbl_info.id;
            field_info.type = pb::INVALID_TYPE;
            field_info.name = table_name + "." + field_name;
            field_info.short_name = field_name;
            std::string lower_short_name = field_name;
            std::transform(lower_short_name.begin(), lower_short_name.end(), lower_short_name.begin(), ::tolower);
            field_info.lower_name = table_name + "." + lower_short_name;
            field_info.lower_short_name = lower_short_name;
            _plan_table_ctx->field_tbls_mapping[lower_short_name].emplace(table_name);
            tbl_info.fields.push_back(field_info);
        }
        for (auto& field : tbl_info.fields) {
            _plan_table_ctx->field_info.emplace(try_to_lower(field.lower_name), &field);
        }
    } else if (_table_names.count(table_name) != 0) {
        DB_WARNING("Not unique table/alias, db:%s, table:%s alias:%s", 
                database.c_str(), table.c_str(), alias.c_str());
        _ctx->stat_info.error_code = ER_NONUNIQ_TABLE;
        _ctx->stat_info.error_msg << "Not unique table/alias: '" << alias << "'";
        return -1;
    } else {
        _table_names.emplace(table_name);
    }
    // 清除derived_table解析出的fields
    _select_names.clear();
    return 0;
}

//TODO, add table alias
int LogicalPlanner::add_table(const std::string& database, const std::string& table,
        const std::string& alias, bool is_derived_table) {
    std::string _namespace = _ctx->user_info->namespace_;
    std::string _username =  _ctx->user_info->username;
    // information_schema忽略大小写
    std::string database_name = database;
    std::transform(database_name.begin(), database_name.end(), database_name.begin(), ::tolower);
    if (database_name == "information_schema") {
        _namespace = "INTERNAL";
        _ctx->has_information_schema = true;
    } else {
        database_name = database;
    }
    
    if (is_derived_table) {
        return add_derived_table(database, table, alias);
    }

    DatabaseInfo db;
    if (_plan_table_ctx->database_info.count(try_to_lower(database)) == 0) {
        int64_t databaseid = -1;
        if (0 != _factory->get_database_id(_namespace + "." + database_name, databaseid)) {
            DB_WARNING("unknown _namespace:%s database: %s", _namespace.c_str(), database.c_str());
            _ctx->stat_info.error_code = ER_BAD_DB_ERROR;
            _ctx->stat_info.error_msg << "database " << database << " not exist";
            return -1;
        }
        db = _factory->get_database_info(databaseid);
        if (db.id == -1) {
            DB_WARNING("no database found with id: %ld", databaseid);
            _ctx->stat_info.error_code = ER_BAD_DB_ERROR;
            _ctx->stat_info.error_msg << "database " << database << " not exist";
            return -1;
        }
        _plan_table_ctx->database_info.emplace(try_to_lower(database), db);
    } else {
        db = _plan_table_ctx->database_info[try_to_lower(database)];
    }
    // 后续的计算，全部以alias为准，无alias则使用表名
    std::string alias_name = alias.empty() ? table : alias;
    std::string alias_full_name = database + "." + alias_name;
    std::string org_table_full_name = database + "." + table;
    int64_t tableid = -1;
    if (_plan_table_ctx->table_info.count(try_to_lower(alias_full_name)) == 0) {
        std::string table_name = table;
        if (_ctx->has_information_schema) {
            std::transform(table_name.begin(), table_name.end(), table_name.begin(), ::toupper);
        }
        if (0 != _factory->get_table_id(_namespace + "." + database_name + "." + table_name, tableid)) {
            DB_WARNING("unknown table: %s.%s", database.c_str(), table.c_str());
            _ctx->stat_info.error_code = ER_NO_SUCH_TABLE;
            _ctx->stat_info.error_msg << "table: " << database << "." << table << " not exist";
            return -1;
        }
        auto tbl_ptr = _factory->get_table_info_ptr(tableid);
        if (tbl_ptr == nullptr) {
            DB_WARNING("no table found with id: %ld", tableid);
            _ctx->stat_info.error_code = ER_NO_SUCH_TABLE;
            _ctx->stat_info.error_msg << "table: " << database << "." << table << " not exist";
            return -1;
        }
        // DBLINK表和外部映射表权限相同
        int64_t privilege_db_id = db.id;
        int64_t privilege_table_id = tbl_ptr->id;
        // DBLINK表处理
        bool is_dblink = false;
        if (tbl_ptr->engine == pb::DBLINK) {
            is_dblink = true;
            if (tbl_ptr->dblink_info.type() == pb::LT_BAIKALDB) {
                SmartTable orig_tbl_ptr = tbl_ptr;
                const std::string& meta_name = tbl_ptr->dblink_info.meta_name();
                const std::string& namespace_name = tbl_ptr->dblink_info.namespace_name();
                const std::string& database_name = tbl_ptr->dblink_info.database_name();
                const std::string& table_name = tbl_ptr->dblink_info.table_name();
                int64_t meta_id = 0;
                if (_factory->get_meta_id(meta_name, meta_id) != 0) {
                    DB_WARNING("unknown meta: %s", meta_name.c_str());
                    return -1;
                }
                std::string external_table_name = namespace_name + "." + database_name + "." + table_name;
                external_table_name = ::baikaldb::get_add_meta_name(meta_id, external_table_name);
                if (_factory->get_table_id(external_table_name, tableid) != 0) {
                    DB_WARNING("unknown external_table_name: %s", external_table_name.c_str());
                    _ctx->stat_info.error_code = ER_NO_SUCH_TABLE;
                    _ctx->stat_info.error_msg << "table: " << database << "." << table 
                                            << "(@dblink_table:" << external_table_name << ") not exist";
                    return -1;
                }
                tbl_ptr = _factory->get_table_info_ptr(tableid);
                if (tbl_ptr == nullptr) {
                    DB_WARNING("no table found with id: %ld", tableid);
                    _ctx->stat_info.error_code = ER_NO_SUCH_TABLE;
                    _ctx->stat_info.error_msg << "table: " << database << "." << table 
                                            << "(@dblink_table:" << external_table_name << ") not exist";
                    return -1;
                }
                // 外部表到主meta的dblink表的映射
                _plan_table_ctx->dblink_table_mapping[tableid] = orig_tbl_ptr;
            } else if (tbl_ptr->dblink_info.type() == pb::LT_MYSQL) {
                _ctx->has_dblink_mysql = true;
            } else {
                DB_WARNING("unknown dblink type: %d", tbl_ptr->dblink_info.type());
                return -1;
            }
            if (can_use_dblink(tbl_ptr) != 0) {
                DB_WARNING("can not use dblink, tableid: %ld", tableid);
                return -1;                
            }
        }
        if (tbl_ptr->engine == pb::ROCKSDB_CSTORE || tbl_ptr->has_vector_index) {
            _ctx->table_can_use_arrow_vectorize = false; 
        }
        _ctx->stat_info.resource_tag = tbl_ptr->resource_tag;
        // learner降级
        if (tbl_ptr->need_learner_backup) {
            _ctx->need_learner_backup = true;
        }
        if (!_ctx->client_conn->user_info->resource_tag.empty()) {
            for (const auto& tag : tbl_ptr->learner_resource_tags) {
                if (_ctx->client_conn->user_info->resource_tag == tag) {
                    _ctx->need_learner_backup = true;
                    break;
                }
            }
        }

        _ctx->sign_blacklist.insert(tbl_ptr->sign_blacklist.begin(), tbl_ptr->sign_blacklist.end());
        _ctx->sign_forcelearner.insert(tbl_ptr->sign_forcelearner.begin(), tbl_ptr->sign_forcelearner.end());
        _ctx->sign_rolling.insert(tbl_ptr->sign_rolling.begin(), tbl_ptr->sign_rolling.end());
        for (auto& sign_index : tbl_ptr->sign_forceindex) {
            std::vector<std::string> vec;
            boost::split(vec, sign_index, boost::is_any_of(":"));
            if (vec.size() != 2) {
                continue;
            }
            uint64_t sign_num = strtoull(vec[0].c_str(), nullptr, 10);
            auto& table_index_map = _ctx->sign_forceindex[sign_num];
            auto& force_index_set = table_index_map[tableid];
            force_index_set.insert(vec[1]);
        }
        for (auto& sign_exec_type : tbl_ptr->sign_exec_type) {
            std::vector<std::string> vec;
            boost::split(vec, sign_exec_type, boost::is_any_of(":"));
            if (vec.size() != 2) {
                continue;
            }
            uint64_t sign_num = strtoull(vec[0].c_str(), nullptr, 10);
            uint64_t exec_type = strtoull(vec[1].c_str(), nullptr, 10);
            _ctx->sign_exec_type[sign_num] = to_sign_exec_type(exec_type);
        }


        // 通用降级路由
        // 复杂sql(join和子查询)不降级
        if (MetaServerInteract::get_backup_instance()->is_inited() && tbl_ptr->have_backup && !_ctx->is_complex &&
            (tbl_ptr->need_write_backup || (tbl_ptr->need_read_backup && _ctx->is_select)) && !is_dblink) {
            _ctx->use_backup = true;
            SchemaFactory::use_backup.set_bthread_local(true);
            _factory = SchemaFactory::get_backup_instance();
            if (0 != SchemaFactory::get_backup_instance()->get_table_id(_namespace + "." + database_name + "." + table_name, tableid)) {
                DB_WARNING("unknown table: %s.%s", database.c_str(), table.c_str());
                _ctx->stat_info.error_code = ER_NO_SUCH_TABLE;
                _ctx->stat_info.error_msg << "table: " << database << "." << table << " not exist";
                return -1;
            }
            tbl_ptr = SchemaFactory::get_backup_instance()->get_table_info_ptr(tableid);
            if (tbl_ptr == nullptr) {
                DB_WARNING("no table found with id: %ld", tableid);
                _ctx->stat_info.error_code = ER_NO_SUCH_TABLE;
                _ctx->stat_info.error_msg << "table: " << database << "." << table << " not exist";
                return -1;
            }
            privilege_db_id = db.id;
            privilege_table_id = tbl_ptr->id;
        }

        auto& tbl = *tbl_ptr;
        // validate user permission
//        pb::OpType op_type = pb::OP_INSERT;
//        if (_ctx->stmt_type == parser::NT_SELECT) {
//            op_type = pb::OP_SELECT;
//        }
        pb::OpType op_type = pb::OP_NONE;
        switch (_ctx->stmt_type) {
        case parser::NT_SELECT:
            op_type = pb::OP_SELECT;
            break;
        case parser::NT_UNION:
            op_type = pb::OP_UNION;
            break;
        case parser::NT_INSERT:
            op_type = pb::OP_INSERT;
            break;
        case parser::NT_UPDATE:
            op_type = pb::OP_UPDATE;
            break;
        case parser::NT_DELETE:
        case parser::NT_TRUNCATE:
            op_type = pb::OP_DELETE;
            break;
        case parser::NT_LOAD_DATA:
            op_type = pb::OP_LOAD;
            break;
        default:
            break;
        }
        if (!_ctx->user_info->allow_op(op_type, privilege_db_id, privilege_table_id, table)) {
            DB_WARNING("user %s has no permission to access: %s.%s, db.id:%ld, tbl.id:%ld", 
                _username.c_str(), database.c_str(), table.c_str(), privilege_db_id, privilege_table_id);
            _ctx->stat_info.error_code = ER_TABLEACCESS_DENIED_ERROR;
            _ctx->stat_info.error_msg << "user " << _username 
                                      << " has no permission to access " 
                                      << database << "." << table;
            return -1;
        }
        for (auto& field : tbl.fields) {
            std::string& col_name = field.lower_short_name;
            _plan_table_ctx->field_tbls_mapping[col_name].emplace(alias_full_name);
            _plan_table_ctx->field_info.emplace(try_to_lower(alias_full_name + "." + col_name), &field);
        }

        _plan_table_ctx->table_info.emplace(try_to_lower(alias_full_name), tbl_ptr);
        _table_names.emplace(alias_full_name);
        if (_plan_table_ctx->table_dbs_mapping[try_to_lower(alias_name)].count(database) == 0) {
            _plan_table_ctx->table_dbs_mapping[try_to_lower(alias_name)].emplace(database);
        }
    } else if (_table_names.count(alias_full_name) != 0) {
        DB_WARNING("Not unique table/alias, db:%s, table:%s alias:%s", 
                database.c_str(), table.c_str(), alias.c_str());
        _ctx->stat_info.error_code = ER_NONUNIQ_TABLE;
        _ctx->stat_info.error_msg << "Not unique table/alias: '" << alias_name << "'";
        return -1;
    } else {
        _table_names.emplace(alias_full_name);
    }
    tableid = _plan_table_ctx->table_info[try_to_lower(alias_full_name)]->id;
    if (!_partition_names.empty()) {
        _ctx->table_partition_names[tableid] = _partition_names;
        _partition_names.clear();
    }
    _ctx->stat_info.table_id = tableid;

    ScanTupleInfo* tuple_info = get_scan_tuple(alias_full_name, tableid);
    _ctx->current_tuple_ids.emplace(tuple_info->tuple_id);
    _ctx->current_table_tuple_ids.emplace(tuple_info->tuple_id);
    //_table_alias_mapping.emplace(use_table_name, org_table_full_name);
    //_table_alias_mapping.emplace(use_table_full_name, org_table_full_name);

    // non-prepare plan cache
    _ctx->table_version_map[tableid] = _plan_table_ctx->table_info[try_to_lower(alias_full_name)]->version;

    return 0;
}

int LogicalPlanner::parse_db_tables(const parser::TableName* table_name) {
    std::string database;
    std::string table;
    if (parse_db_name_from_table_name(table_name, database, table) < 0) {
        DB_WARNING("parse db name from table name fail");
        return -1;
    }
    std::string alias = table;
    if (0 != add_table(database, table, alias, false)) {
        DB_WARNING("invalid database or table:%s.%s", database.c_str(), table.c_str());
        return -1;
    }
    return 0;
}

int LogicalPlanner::parse_db_tables(const parser::TableSource* table_source) {
    std::string database;
    std::string table;
    std::string alias;
    bool is_derived_table = false;
    if (parse_db_name_from_table_source(table_source, database, table, alias, is_derived_table) < 0) {
        DB_WARNING("parse db name from table name fail");
        return -1;
    }
    if (0 != add_table(database, table, alias, is_derived_table)) {
        DB_WARNING("invalid database or table:%s.%s", database.c_str(), table.c_str());
        return -1;
    }
    return 0;
}

int LogicalPlanner::parse_db_tables(const parser::Node* table_refs, JoinMemTmp** join_root_ptr) {
    if (table_refs == nullptr) {
        DB_WARNING("table item is null");
        return -1;
    }
    if (table_refs->node_type == parser::NT_TABLE) {
        //DB_WARNING("tbls item is table name");
        if (0 != create_join_node_from_table_name((parser::TableName*)table_refs, join_root_ptr)) {
            DB_WARNING("parse_db_table_item failed from linked list");
            return -1;
        }
    } else if (table_refs->node_type == parser::NT_TABLE_SOURCE) {
        // DB_WARNING("tbls item is table source");
        if (0 != create_join_node_from_table_source((parser::TableSource*)table_refs, join_root_ptr)) {
            DB_WARNING("parse_db_table_item failed from linked list");
            return -1;
        }
    } else if (table_refs->node_type == parser::NT_JOIN) {
        //DB_WARNING("tbls item is item join");
        parser::JoinNode* join_item = (parser::JoinNode*)(table_refs);
        if (0 != create_join_node_from_item_join(join_item, join_root_ptr)) {
            DB_WARNING("parse_db_table_item failed from item join");
            return -1;
        }     
    } else {
        DB_WARNING("unsupport table type");
        return -1;
    }
    return 0;
}

//一个join中解析table, 直接生成join节点
int LogicalPlanner::create_join_node_from_item_join(const parser::JoinNode* join_item, 
                                                    JoinMemTmp** join_root_ptr) {
    std::unique_ptr<JoinMemTmp> join_node_mem(new JoinMemTmp);
    join_node_mem->join_node.set_join_type(join_type_mapping[join_item->join_type]); 
    
    if (join_item->left  == nullptr) {
        DB_WARNING("joint node must have left node");
        return -1;
    }
    if (0 != parse_db_tables(join_item->left, &(join_node_mem->left_node))) {
        DB_WARNING("parse left node fail");
        return -1;
    }
    if (join_item->right == nullptr) {
        DB_WARNING("joint node must have right node");
        return -1;
    }
    if (0 != parse_db_tables(join_item->right, &(join_node_mem->right_node))) {
        DB_WARNING("parse right node fail");
        return -1;
    }
    auto ret = fill_join_table_infos(join_node_mem.get());    

    if (ret < 0) {
        DB_WARNING("fill join table info fail");
        return ret;
    }
    parser::ExprNode* condition = join_item->expr;
    /*
    //暂时不支持没有条件的join
    if (condition == nullptr && join_item->using_col.size() == 0) {
        DB_WARNING("has no join condition");
        return -1;
    }
    */
    
    if (condition != nullptr) {
        std::vector<pb::Expr> join_filters;
        if (0 != flatten_filter(condition, join_filters, CreateExprOptions())) {
            DB_WARNING("flatten_filter failed");
            return -1;
        }
        for (uint32_t idx = 0; idx < join_filters.size(); ++idx) {
            pb::Expr* expr = join_node_mem->join_node.add_conditions();
            expr->CopyFrom(join_filters[idx]);
        }
    }
    ret = parse_using_cols(join_item->using_col, join_node_mem.get());
    if (ret < 0) {
        DB_WARNING("_parse using cols fail");
        return ret;
    }
    *join_root_ptr = join_node_mem.release();
    return 0;
}

int LogicalPlanner::parse_using_cols(const parser::Vector<parser::ColumnName*>& using_col, JoinMemTmp* join_node_mem) {
    //解析using cols
    for (int i = 0; i < using_col.size(); ++i) {
        std::string field_name = using_col[i]->name.value;
        std::unordered_set<std::string> possible_table_names = get_possible_tables(field_name);
        if (possible_table_names.size() < 2) {
            DB_WARNING("using filed not in more than 2 table");
            return -1;
        }
        int32_t related_table_count = 0;
        std::string left_db;
        std::string left_table;
        std::string right_db;
        std::string right_table;
        for (auto& table_name : possible_table_names) {
            if (join_node_mem->left_full_table_names.count(table_name) != 0) {
                auto pos = table_name.find(".");
                if (pos != std::string::npos) {
                    ++related_table_count;
                    left_db = table_name.substr(0, pos);
                    left_table = table_name.substr(pos + 1);
                }
            }
        }
        if (related_table_count != 1) {
            DB_WARNING("related table count not equal to 2");
            return -1;
        }

        related_table_count = 0;
        for (auto& table_name : possible_table_names) {
            if (join_node_mem->right_full_table_names.count(table_name) != 0) {
                auto pos = table_name.find(".");
                if (pos != std::string::npos) {
                    ++related_table_count;
                    right_db = table_name.substr(0, pos);
                    right_table = table_name.substr(pos + 1);
                }
            }
        }
        if (related_table_count != 1) {
            DB_WARNING("related table count not equal to 2");
            return -1;
        }

        pb::Expr expr;
        pb::ExprNode* node = expr.add_nodes();
        node->set_node_type(pb::FUNCTION_CALL);
        node->set_col_type(pb::INVALID_TYPE);
        pb::Function* func = node->mutable_fn();
        func->set_fn_op(parser::FT_EQ);
        func->set_name("eq");
        node->set_num_children(2);
        CreateExprOptions options;
        options.use_alias = false;
        // 临时使用
        using_col[i]->db = left_db.c_str();
        using_col[i]->table = left_table.c_str();
        if (0 != create_term_slot_ref_node(using_col[i], expr, options)) {
            DB_WARNING("left_full_field using field create term slot fail");
            return -1;
        }
        // 临时使用
        using_col[i]->db = right_db.c_str();
        using_col[i]->table = right_table.c_str();
        if (0 != create_term_slot_ref_node(using_col[i], expr, options)) {
            DB_WARNING("left_full_field using field create term slot fail");
            return -1;
        }
        pb::Expr* join_expr = join_node_mem->join_node.add_conditions();
        join_expr->CopyFrom(expr);
    }
    return 0;
}
int LogicalPlanner::fill_join_table_infos(JoinMemTmp* join_node_mem) {
    for (auto& table_name : join_node_mem->left_node->left_full_table_names) {
        int64_t table_id = _plan_table_ctx->table_info[try_to_lower(table_name)]->id;
        int32_t tuple_id = _plan_table_ctx->table_tuple_mapping[try_to_lower(table_name)].tuple_id;
        if (join_node_mem->left_full_table_names.count(try_to_lower(table_name)) != 0) {
            DB_WARNING("not support self join");
            return -1;
        }
        join_node_mem->join_node.add_left_tuple_ids(tuple_id);
        join_node_mem->join_node.add_left_table_ids(table_id);
        join_node_mem->left_full_table_names.insert(try_to_lower(table_name));
    } 
    for (auto& table_name : join_node_mem->left_node->right_full_table_names) {
        int64_t table_id = _plan_table_ctx->table_info[try_to_lower(table_name)]->id;
        int32_t tuple_id = _plan_table_ctx->table_tuple_mapping[try_to_lower(table_name)].tuple_id;
        if (join_node_mem->left_full_table_names.count(try_to_lower(table_name)) != 0) {
            DB_WARNING("not support self join");
            return -1;
        }
        join_node_mem->join_node.add_left_tuple_ids(tuple_id);
        join_node_mem->join_node.add_left_table_ids(table_id);
        join_node_mem->left_full_table_names.insert(try_to_lower(table_name));
    }

    for (auto& table_name : join_node_mem->right_node->left_full_table_names) {
        int64_t table_id = _plan_table_ctx->table_info[try_to_lower(table_name)]->id;
        int32_t tuple_id = _plan_table_ctx->table_tuple_mapping[try_to_lower(table_name)].tuple_id;
        if (join_node_mem->right_full_table_names.count(try_to_lower(table_name)) != 0) {
            DB_WARNING("not support self join");
            return -1;
        }
        join_node_mem->join_node.add_right_tuple_ids(tuple_id);
        join_node_mem->join_node.add_right_table_ids(table_id);
        join_node_mem->right_full_table_names.insert(try_to_lower(table_name));
    }
    for (auto& table_name : join_node_mem->right_node->right_full_table_names) {
        int64_t table_id = _plan_table_ctx->table_info[try_to_lower(try_to_lower(table_name))]->id;
        int32_t tuple_id = _plan_table_ctx->table_tuple_mapping[try_to_lower(table_name)].tuple_id;
        if (join_node_mem->right_full_table_names.count(try_to_lower(table_name)) != 0) {
            DB_WARNING("not support self join");
            return -1;
        }
        join_node_mem->join_node.add_right_tuple_ids(tuple_id);
        join_node_mem->join_node.add_right_table_ids(table_id);
        join_node_mem->right_full_table_names.insert(try_to_lower(table_name));
    }
    return 0;
}

//join节点的叶子节点
int LogicalPlanner::create_join_node_from_table_name(const parser::TableName* table_name, JoinMemTmp** join_root_ptr) {
    std::string db;
    std::string table;
    if (parse_db_name_from_table_name(table_name, db, table) < 0) {
        DB_WARNING("parser db name from table name fail");
        return -1;
    }
    return create_join_node_from_terminator(db, table, 
            std::string(), false,
            std::vector<std::string>{}, 
            std::vector<std::string>{},
            std::vector<std::string>{},
            join_root_ptr);
}

int LogicalPlanner::create_join_node_from_table_source(const parser::TableSource* table_source, JoinMemTmp** join_root_ptr) {
    std::string db;
    std::string table;
    std::string alias;
    bool is_derived_table = false;
    if (parse_db_name_from_table_source(table_source, db, table, alias, is_derived_table) < 0) {
        DB_WARNING("parser db name from table name fail");
        return -1;
    }
    std::vector<std::string> use_index_names;
    std::vector<std::string> force_index_names;
    std::vector<std::string> ignore_index_names;
    for (int i = 0; i < table_source->index_hints.size(); ++i) {
        if (table_source->index_hints[i]->hint_type == parser::IHT_HINT_USE) {
            for (int j = 0; j < table_source->index_hints[i]->index_name_list.size(); ++j) {
                use_index_names.emplace_back(table_source->index_hints[i]->index_name_list[j].value);
            }
        }
        if (table_source->index_hints[i]->hint_type == parser::IHT_HINT_FORCE) {
            for (int j = 0; j < table_source->index_hints[i]->index_name_list.size(); ++j) {
                force_index_names.emplace_back(table_source->index_hints[i]->index_name_list[j].value);
            }
        }
        if (table_source->index_hints[i]->hint_type == parser::IHT_HINT_IGNORE) {
            for (int j = 0; j < table_source->index_hints[i]->index_name_list.size(); ++j) {
                ignore_index_names.emplace_back(table_source->index_hints[i]->index_name_list[j].value);
            }
        }
    }
    return create_join_node_from_terminator(db, table, alias, is_derived_table, use_index_names,
                                            force_index_names, ignore_index_names, join_root_ptr);
}

int LogicalPlanner::create_join_node_from_terminator(const std::string db, 
            const std::string table, 
            const std::string alias, 
            const bool is_derived_table,
            const std::vector<std::string>& use_index_names,
            const std::vector<std::string>& force_index_names,
            const std::vector<std::string>& ignore_index_names, 
            JoinMemTmp** join_root_ptr) {
    if (0 != add_table(db, table, alias, is_derived_table)) {
        DB_WARNING("invalid database or table:%s.%s", db.c_str(), table.c_str());
        return -1;
    }
    std::string alias_name = alias.empty() ? table : alias;
    std::string alias_full_name = db + "." + alias_name;
    std::unique_ptr<JoinMemTmp> join_node_mem(new JoinMemTmp);
    //叶子节点
    join_node_mem->join_node.set_join_type(pb::NULL_JOIN);
    //特殊处理一下，把跟节点的表名直接放在左子树上
    join_node_mem->left_full_table_names.insert(alias_full_name);
    join_node_mem->is_derived_table = is_derived_table;
    //这些表在这些map中肯定存在，不需要再判断
    int64_t table_id = _plan_table_ctx->table_info[try_to_lower(alias_full_name)]->id;
    int32_t tuple_id = _plan_table_ctx->table_tuple_mapping[try_to_lower(alias_full_name)].tuple_id;
    join_node_mem->join_node.add_left_tuple_ids(tuple_id);
    join_node_mem->join_node.add_left_table_ids(table_id);
    *join_root_ptr = join_node_mem.release();
    for (auto& index_name : use_index_names) {
        int64_t index_id = 0;
        auto ret = _factory->get_index_id(table_id, index_name, index_id);
        if (ret != 0) {
            DB_WARNING("index_name: %s in table:%s not exist", 
                        index_name.c_str(), alias_full_name.c_str());
            return -1;
        }
        (*join_root_ptr)->use_indexes.insert(index_id);
    }
    //如果用户指定了use_index, 则主键永远放到use_index里
    if ((*join_root_ptr)->use_indexes.size() != 0) {
        (*join_root_ptr)->use_indexes.insert(table_id);
    }
    for (auto& index_name : force_index_names) {
        int64_t index_id = 0;
        auto ret = _factory->get_index_id(table_id, index_name, index_id);
        if (ret != 0) {
            DB_WARNING("index_name: %s in table:%s not exist",
                        index_name.c_str(), alias_full_name.c_str());
            return -1;
        }
        auto indexInfoPtr = _factory->get_index_info_ptr(index_id);
        if (indexInfoPtr == nullptr || indexInfoPtr->state != pb::IS_PUBLIC || indexInfoPtr->index_hint_status != pb::IHS_NORMAL) {
            continue;
        }
        (*join_root_ptr)->force_indexes.insert(index_id);
    }
    for (auto& index_name : ignore_index_names) {
        int64_t index_id = 0;
        auto ret = _factory->get_index_id(table_id, index_name, index_id);
        if (ret != 0) {
            DB_WARNING("index_name: %s in table:%s not exist",
                    index_name.c_str(), alias_full_name.c_str());
            return -1;
        }
        (*join_root_ptr)->ignore_indexes.insert(index_id);
    }
    return 0;
}

int LogicalPlanner::parse_db_name_from_table_name(
            const parser::TableName* table_name, 
            std::string& db, 
            std::string& table) {
    if (table_name == nullptr) {
        DB_WARNING("table name is null");
        return -1;
    }
    if (table_name->table.empty()) {
        DB_WARNING("empty tbl name,sql:%s", _ctx->sql.c_str());
        return -1;
    }
    table = table_name->table.value;
    if (!table_name->db.empty()) {
        db = table_name->db.value;
    } else if (!_ctx->cur_db.empty()) {
        db = _ctx->cur_db;
    } else {
        _ctx->stat_info.error_code = ER_NO_DB_ERROR;
        _ctx->stat_info.error_msg << "No database selected";
        return -1;
    }
    _ctx->stat_info.family = db;
    _ctx->stat_info.table = table;
    _current_tables.emplace_back(db + "." + table);
    if (_ctx->stmt_type == parser::NT_CREATE_VIEW) {
        ((parser::TableName*)table_name)->db = _ctx->stat_info.family.c_str();

        // 由于重复表名有问题, 暂时不支持在视图上新建视图
        std::string table_full_name = _ctx->user_info->namespace_ + "." + db + "." + table;
        SmartTable table_ptr = SchemaFactory::get_instance()->get_table_info_ptr_by_name(table_full_name);
        if (table_ptr != nullptr && table_ptr->is_view) {
            _ctx->stat_info.error_code = ER_VIEW_INVALID;
            _ctx->stat_info.error_msg << "cant create view on a view, " << table_full_name << " is a view";
            return -1;
        }
    }
    return 0;
}

int LogicalPlanner::parse_db_name_from_table_source(const parser::TableSource* table_source, 
                                                    std::string& db, 
                                                    std::string& table,
                                                    std::string& alias,
                                                    bool& is_derived_table) {
    if (table_source == nullptr) {
        DB_WARNING("table source is null");
        return -1;
    }
    // check查询的表是否是在with中
    if (table_source->table_name != nullptr ) {
        auto table_with_clause_iter = _ctx->table_with_clause_mapping.find(table_source->table_name->table.value);
        if (table_with_clause_iter != _ctx->table_with_clause_mapping.end()) {
            // 查询的表在with中
            std::string with_select_stmt = "SELECT * FROM (" + table_with_clause_iter->second + ") " + table_source->table_name->table.value;
            parser::SqlParser parser;
            parser.parse(with_select_stmt);
            if (parser.result.size() != 1) {
                DB_WARNING("with rewrite sql view %s cant parse", with_select_stmt.c_str());
                return -1;
            }
            parser::TableSource* with_table_source = (parser::TableSource*) ((parser::SelectStmt*) parser.result[0])->table_refs;
            if (with_table_source != nullptr) {
                // with解析逻辑同子查询
                if (0 != parse_table_source(with_table_source, db, table, alias, is_derived_table)) {
                    DB_WARNING("parse view table source failed");
                    return -1;
                }
                return 0;
            }
        }
    }
    // check查询的表是否是视图
    std::string table_full_name = "";
    if (table_source->table_name != nullptr && !table_source->table_name->db.empty()) {
        table_full_name = _ctx->user_info->namespace_ + "." + table_source->table_name->db.value + "." + table_source->table_name->table.value;
    } else if (table_source->table_name != nullptr && !_ctx->cur_db.empty()) {
        table_full_name = _ctx->user_info->namespace_ + "." + _ctx->cur_db + "." + table_source->table_name->table.value;
    }
    if (table_full_name != "" 
        && table_source->derived_table == nullptr
        && _ctx->stmt_type != parser::NT_CREATE_VIEW) {
        SmartTable table_ptr = SchemaFactory::get_instance()->get_table_info_ptr_by_name(table_full_name);
        if (table_ptr != nullptr && table_ptr->is_view) {
            if (_ctx->is_with == true) {
                _ctx->stat_info.error_code = ER_VIEW_INVALID;
                _ctx->stat_info.error_msg << "cant create with-clause on a view, " << table_full_name << " is a view";
                return -1;
            }

            std::string view_select_stmt = "SELECT * FROM (" + table_ptr->view_select_stmt + ") " + table_ptr->short_name;
            parser::SqlParser parser;
            parser.parse(view_select_stmt);
            if (parser.result.size() != 1) {
                DB_WARNING("view rewrite sql view %s cant parse", view_select_stmt.c_str());
                return -1;
            }
            parser::TableSource* view_table_source = (parser::TableSource*) ((parser::SelectStmt*) parser.result[0])->table_refs;
            if (view_table_source != nullptr) {
                // 视图解析逻辑同子查询
                if (0 != parse_table_source(view_table_source, db, table, alias, is_derived_table)) {
                    DB_WARNING("parse view table source failed");
                    return -1;
                }
                return 0;
            }
        }
    }

    if (table_source->derived_table != nullptr) {
        if (0 != parse_table_source(table_source, db, table, alias, is_derived_table)) {
            DB_WARNING("parse table source failed");
            return -1;
        }
        return 0;
    }
    for (int i = 0; i < table_source->partition_names.size(); ++i) {
        std::string lower_name = table_source->partition_names[i].value;
        std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(), ::tolower);
        _partition_names.emplace_back(lower_name);
    }
    parser::TableName* table_name = table_source->table_name;
    if (parse_db_name_from_table_name(table_name, db, table) < 0) {
        DB_WARNING("parser db name from table name fail");
        return -1;
    }
    if (!table_source->as_name.empty()) {
        alias = table_source->as_name.value;
        _current_tables.back() = db + "." + alias;
    }
    return 0;
}

int LogicalPlanner::parse_table_source(const parser::TableSource* table_source, 
                                                    std::string& db, 
                                                    std::string& table,
                                                    std::string& alias,
                                                    bool& is_derived_table) {
    if (table_source->as_name.empty()) {
        _ctx->stat_info.error_code = ER_DERIVED_MUST_HAVE_ALIAS;
        _ctx->stat_info.error_msg << "Every derived table must have its own alias";
        return -1;
    }
    _ctx->has_derived_table = true;
    is_derived_table = true;
    table = table_source->as_name.value;
    alias = table;
    db = VIRTUAL_DATABASE_NAME;
    //不同层级的子查询同名表不能冲突
    if (_ctx->sub_query_level > 0) {
        db += std::to_string(_ctx->sub_query_level);
    }
    _ctx->stat_info.family = db;
    _ctx->stat_info.table = table;
    _current_tables.emplace_back(db + "." + table);
    parser::DmlNode* derived_table = table_source->derived_table;
    ExprParams expr_params;
    expr_params.is_from_subquery = true;
    int ret = gen_subquery_plan(derived_table, _plan_table_ctx, expr_params);
    if (ret < 0) {
        DB_WARNING("gen subquery plan failed");
        return -1;
    }
    _ctx->add_sub_ctx(_cur_sub_ctx);
    return 0;
}

int LogicalPlanner::parse_view_select(parser::DmlNode* view_select_stmt,
                        const parser::Vector<parser::ColumnName*>& column_names,
                        pb::SchemaInfo& view) {
    if (view_select_stmt->node_type == parser::NT_SELECT) {
        std::string view_select_field_str = "";
        if (0 != parse_view_select_fields((parser::SelectStmt*) view_select_stmt, 
                column_names, view_select_field_str, view)) {
            DB_WARNING("parse view select field failed");
            return -1;
        }
        if (0 != add_view_select_stmt((parser::SelectStmt*) view_select_stmt, 
                        column_names,
                        view_select_field_str,
                        view)) {
            DB_WARNING("add view_select_stmt to view failed.");
            return -1;
        }
    } else if (view_select_stmt->node_type == parser::NT_UNION) {
        parser::UnionStmt* union_stmt = (parser::UnionStmt*) view_select_stmt;
        std::vector<pb::SchemaInfo> view_list;
        std::string union_str = "";
        for (int stmt_idx = 0; stmt_idx < union_stmt->select_stmts.size(); stmt_idx++) {
            std::string view_select_field_str = "";
            pb::SchemaInfo tmp_view = view;
             std::unique_ptr<LogicalPlanner> planner;
            planner.reset(new SelectPlanner(_ctx));
            if (0 != planner->parse_view_select(union_stmt->select_stmts[stmt_idx], 
                                        column_names,
                                        tmp_view)) {
                DB_WARNING("parse view select failed");
                return -1;
            }
            view_list.push_back(tmp_view);
        }
        if (union_stmt->is_in_braces) {
            union_str += "(";
        }
        for (int i = 0; i < view_list.size() - 1; ++i) {
            if (view_list[i].fields_size() != view_list[i + 1].fields_size()) {
                _ctx->stat_info.error_code = ER_VIEW_INVALID;
                _ctx->stat_info.error_msg << "The used SELECT statements have a different number of columns";
                DB_WARNING("have a different number of columns %u and %u", view_list[i].fields_size(), view_list[i+1].fields_size());
                return -1;
            }
            for (int j = 0; j < view_list[i].fields_size(); ++j) {
                if (view_list[i].fields(j).mysql_type() != view_list[i + 1].fields(j).mysql_type()) {
                    _ctx->stat_info.error_code = ER_VIEW_INVALID;
                    _ctx->stat_info.error_msg << "The used SELECT statements have different type";
                    DB_WARNING("have a different type of columns %s and %s", 
                                    pb::PrimitiveType_Name(view_list[i].fields(j).mysql_type()).c_str(),
                                    pb::PrimitiveType_Name(view_list[i+1].fields(j).mysql_type()).c_str());

                    return -1;
                }
            }
            if (union_stmt->distinct) {
                union_str += view_list[i].view_select_stmt() + " UNION ";
            } else {
                union_str += view_list[i].view_select_stmt() + " UNION ALL ";
            }
        }
        if (view_list.size() > 0) {
            union_str += view_list[view_list.size() - 1].view_select_stmt();
        }
        std::ostringstream os;
        if (union_stmt->order != nullptr) {
            os << " ORDER BY" << union_stmt->order;
        }
        if (union_stmt->limit != nullptr) {
            os << " LIMIT" << union_stmt->limit;
        }
        os << parser::for_lock_str[union_stmt->lock];
        if (union_stmt->is_in_braces) {
            os << ")";
        }
        if (os.str().size() > 0) {
            union_str += os.str();
        }
        if (union_str.size() > 0) {
            view.set_view_select_stmt(union_str);
            view.mutable_fields()->CopyFrom(view_list[0].fields());
        }
    }
    return 0;
}

int LogicalPlanner::parse_view_select_fields(parser::SelectStmt* view_select_stmt,
                        const parser::Vector<parser::ColumnName*>& column_names,
                        std::string& view_select_field_str,
                        pb::SchemaInfo& view) {
    std::vector<std::string> select_field_names;
    int column_name_idx = 0;
    std::set<std::string> uniq_view_select_field_alias;

    // 用于添加表信息
    if (0 != parse_db_tables(view_select_stmt->table_refs, &_join_root)) {
        return -1;
    }
    for (int i = 0; i < view_select_stmt->fields.size(); ++i) {
        parser::SelectField* select_field = view_select_stmt->fields[i];
        if (select_field->wild_card != nullptr) {
            if (-1 == parse_view_select_star(select_field, 
                                column_names, 
                                column_name_idx, 
                                uniq_view_select_field_alias,
                                view_select_field_str,
                                view)) {
                DB_WARNING("parse view select star failed");
                return -1;
            }
        } else {
            if (-1 == parse_view_select_field(select_field, 
                                column_names,
                                column_name_idx, 
                                uniq_view_select_field_alias,
                                view_select_field_str,
                                view)) {
                DB_WARNING("parse view select field failed");
                return -1;
            };
        }
    }
    if (view_select_field_str.size() > 0 && view_select_field_str[view_select_field_str.size() - 1] == ',') {
        view_select_field_str.pop_back();
    }
    return 0;
}

int LogicalPlanner::parse_view_select_star(parser::SelectField* select_field, 
            const parser::Vector<parser::ColumnName*>& column_names, 
            int& column_name_idx, 
            std::set<std::string>& uniq_view_select_field_alias,
            std::string& view_select_field_str,
            pb::SchemaInfo& view) {
    parser::WildCardField* wild_card = select_field->wild_card;
    // select * ...
    if (wild_card->db_name.empty() && wild_card->table_name.empty()) {
        for (auto& table_name : _table_names) {
            auto table_info = get_table_info_ptr(table_name);
            if (table_info == nullptr) {
                if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                    _ctx->stat_info.error_code = ER_WRONG_TABLE_NAME;
                    _ctx->stat_info.error_msg << "Incorrect table name \'" << table_name << "\'";
                }
                DB_WARNING("no table found for select field: %s", select_field->to_string().c_str());
                return -1;
            }
            if (0 != add_single_table_columns_for_view(table_info, 
                                column_names,
                                uniq_view_select_field_alias,
                                view_select_field_str, 
                                column_name_idx,
                                view)) {
                DB_WARNING("add single table: %s columns fail", table_info->short_name.c_str());
                return -1;
            }
        }
    } else {
        // select db.table.* / table.* ....
        if (wild_card->table_name.empty()) {
            DB_WARNING("table name is empty");
            return -1;
        }
        std::string table_name = wild_card->table_name.value;
        std::string db_name;
        std::string full_name;
        // try to search alias table
        if (!wild_card->db_name.empty()) {
            db_name = wild_card->db_name.value;
            full_name = db_name + "." + table_name;
        } else {
            //table.field_name
            auto dbs = get_possible_databases(table_name);
            if (dbs.size() == 0) {
                if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                    _ctx->stat_info.error_code = ER_WRONG_TABLE_NAME;
                    _ctx->stat_info.error_msg << "Incorrect table name \'" << table_name << "\'";
                }
                DB_WARNING("no database found for field: %s", table_name.c_str());
                return -1;
            } else if (dbs.size() > 1 && !FLAGS_disambiguate_select_name) {
                if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                    _ctx->stat_info.error_code = ER_AMBIGUOUS_FIELD_TERM;
                    _ctx->stat_info.error_msg << "table  \'" << table_name << "\' is ambiguous";
                }
                DB_WARNING("ambiguous table_name: %s", table_name.c_str());
                return -1;
            }
            full_name = *dbs.begin() + "." + table_name;
        }
        auto table_info = get_table_info_ptr(full_name);
        if (table_info == nullptr) {
            if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                _ctx->stat_info.error_code = ER_WRONG_TABLE_NAME;
                _ctx->stat_info.error_msg << "Incorrect table name \'" << table_name << "\'";
            }
            DB_WARNING("no table found for select field: %s", select_field->to_string().c_str());
            return -1;
        }
        if (0 != add_single_table_columns_for_view(table_info, 
                                column_names,
                                uniq_view_select_field_alias,
                                view_select_field_str,
                                column_name_idx,
                                view)) {
            DB_WARNING("add single table: %s columns fail", table_info->short_name.c_str());
            return -1;
        }
    }
    return 0;
}

int LogicalPlanner::parse_view_select_field(parser::SelectField* select_field, 
            const parser::Vector<parser::ColumnName*>& column_names, 
            int& column_name_idx, 
            std::set<std::string>& uniq_view_select_field_alias,
            std::string& view_select_field_str,
            pb::SchemaInfo& view) {
    // 主要是做一些检查, 列名是否存在等
    pb::Expr select_expr;
    if (select_field->expr == nullptr) {
        DB_WARNING("field expr is nullptr");
        return -1;
    }
    CreateExprOptions options;
    options.can_agg = true;
    options.is_select_field = true;
    options.max_one_row = true;
    if (0 != create_expr_tree(select_field->expr, select_expr, options)) {
        DB_WARNING("create select expr failed");
        return -1;
    }
    ExprNode* select_expr_node = nullptr;
    if (0 != ExprNode::create_tree(select_expr, &select_expr_node)) {
        DB_WARNING("create insertion mem expr failed");
        return -1;
    }
    if (0 != select_expr_node->type_inferer()) {
        DB_WARNING("expr type_inferer fail");
        return -1;
    }
    pb::PrimitiveType mysql_type = select_expr_node->col_type();
    select_expr_node->close();
    delete select_expr_node;
    // 不影响正常流程, create view 在ddl_planner里面需要一个正常的mysql_type才能成功update_table
    if (mysql_type == pb::INVALID_TYPE) {
        mysql_type = pb::STRING;
    }
    
    std::string select_name;
    if (parse_select_name(select_field, select_name) == -1) {
        DB_WARNING("Fail to parse_select_name");
        return -1;
    }

    // 解析select expr
    if (column_names.size() > 0 && column_names.size() <= column_name_idx) {
        DB_WARNING("column names size %d not equal to column_name_idx size %d", column_names.size(), column_name_idx);
        return -1;
    }

    const std::string& alias_name = column_names.size() > 0 ? 
                    column_names[column_name_idx++]->name.value : select_name;       
    if (uniq_view_select_field_alias.count(alias_name)) {
        DB_WARNING("column name %s is duplicate", alias_name.c_str());
        if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
            _ctx->stat_info.error_code = ER_VIEW_INVALID;
            _ctx->stat_info.error_msg << "duplicate view column name \'" << alias_name << "\'";
        }
        return -1;
    }
    for (int i = 0; i < alias_name.size(); i++) {
        if ((alias_name[i] < 'a' || 'z' < alias_name[i]) &&
                (alias_name[i] < 'A' || 'Z' < alias_name[i]) &&
                (alias_name[i] < '0' || '9' < alias_name[i]) &&
                (alias_name[i] != '_')) {
            // 类似sum(a),a+1 这种列名会在schema_factory中构建proto时报错, 需要指定列名
            DB_WARNING("column name %s need alias name", alias_name.c_str());
            if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                _ctx->stat_info.error_code = ER_VIEW_INVALID;
                _ctx->stat_info.error_msg << "column name \'"  << alias_name << "' need alias name";
            }
            return -1;
        }
    }
   
    std::string real_alias_name = "`" + alias_name + "`";
    char* as_name_ptr = select_field->as_name.value;
    select_field->as_name = real_alias_name.c_str();
    std::ostringstream os;
    select_field->to_stream(os);

    view_select_field_str += os.str() + ",";
    uniq_view_select_field_alias.insert(alias_name);
    pb::FieldInfo* view_field = view.add_fields();
    view_field->set_field_name(alias_name);
    view_field->set_mysql_type(mysql_type);
    return 0;
}

int LogicalPlanner::add_single_table_columns_for_view(
    TableInfo* table_info,
    const parser::Vector<parser::ColumnName*>& column_names,
    std::set<std::string>& uniq_view_select_field_alias,
    std::string& view_select_field_str,
    int& column_name_idx,
    pb::SchemaInfo& view) {
    for (auto& field : table_info->fields) {
        if (field.deleted) {
            continue;
        }
        if (column_names.size() > 0 && column_names.size() <= column_name_idx) {
            DB_WARNING("column names size %d not equal to column_name_idx size %d", column_names.size(), column_name_idx);
            if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                _ctx->stat_info.error_code = ER_VIEW_INVALID;
                _ctx->stat_info.error_msg << "view column size invaild";
            }
            return -1;
        }
        std::string& select_name = field.name;
        const std::string& alias_name =  column_names.size() > 0 ? 
            column_names[column_name_idx++]->name.value : field.short_name;       
        if (uniq_view_select_field_alias.count(alias_name)) {
            DB_WARNING("column name %s is duplicate", alias_name.c_str());
            if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                _ctx->stat_info.error_code = ER_VIEW_INVALID;
                _ctx->stat_info.error_msg << "duplicate view column name \'" << alias_name << "\'";
            }
            return -1;
        }
        // 取出列名 table.column_name
        size_t dot_pos = field.name.find('.'); 
        if (dot_pos == std::string::npos) {
            DB_WARNING("view column name %s invaild", field.name.c_str());
            if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                _ctx->stat_info.error_code = ER_VIEW_INVALID;
                _ctx->stat_info.error_msg << "view column " << field.name << " invaild";
            }
            return -1;
        }
        std::string select_full_name = field.name.substr(dot_pos + 1); 
        view_select_field_str += select_full_name + " AS `" + alias_name + "`,";
        uniq_view_select_field_alias.insert(alias_name);
        pb::FieldInfo* view_field = view.add_fields();
        view_field->set_field_name(alias_name);
        view_field->set_mysql_type(field.type);
    }
    
    return 0;
}

int LogicalPlanner::add_view_select_stmt(parser::SelectStmt* view_select_stmt,
                parser::Vector<parser::ColumnName*>  column_names, 
                const std::string& view_select_field_str,
                pb::SchemaInfo& view) {
    std::ostringstream os;        
    if (view_select_stmt->is_in_braces) {
        os << "(";
    }
    os << "SELECT ";
    view_select_stmt->select_opt->to_stream(os);
    // 检查column_names和view_select_stmt的字段个数是否一致

    if (!view_select_field_str.empty()) {
        os << view_select_field_str;
    }
    if (view_select_stmt->table_refs != nullptr) {
        os << " FROM" << view_select_stmt->table_refs;
    }
    if (view_select_stmt->where != nullptr) {
        os << " WHERE " << view_select_stmt->where;
    }
    if (view_select_stmt->group != nullptr) {
        os << " GROUP BY" << view_select_stmt->group;
    }
    if (view_select_stmt->having != nullptr) {
        os << " HAVING" << view_select_stmt->having;
    }
    if (view_select_stmt->order != nullptr) {
        os << " ORDER BY" << view_select_stmt->order;
    }
    if (view_select_stmt->limit != nullptr) {
        os << " LIMIT" << view_select_stmt->limit;
    }
    os << parser::for_lock_str[view_select_stmt->lock];
    if (view_select_stmt->is_in_braces) {
        os << ")";
    }
    view.set_view_select_stmt(os.str());
    return 0;
}

int LogicalPlanner::parse_select_name(parser::SelectField* field, std::string& select_name) {
    if (field == nullptr) {
        DB_WARNING("field is nullptr");
        return -1;
    }
    if (field->expr == nullptr) {
        DB_WARNING("field->expr is nullptr");
        return -1;
    }
    if (!field->as_name.empty()) {
        select_name = field->as_name.value;
    } else {
        if (field->expr->expr_type == parser::ET_COLUMN) {
            parser::ColumnName* column = static_cast<parser::ColumnName*>(field->expr);
            select_name = column->name.c_str();
        } else if (!field->org_name.empty()) {
            select_name = field->org_name.c_str();
        } else {
            select_name = field->expr->to_string();
        }
    }
    return 0;
}

int LogicalPlanner::flatten_filter(const parser::ExprNode* item, std::vector<pb::Expr>& filters, 
        const CreateExprOptions& options) {
    if (item->expr_type == parser::ET_FUNC) {
        parser::FuncExpr* func = (parser::FuncExpr*)item;
        if (func->func_type == parser::FT_LOGIC_AND) {
            for (int i = 0; i < func->children.size(); i++) {
                if (func->children[i]->node_type != parser::NT_EXPR) {
                    DB_FATAL("type error");
                    return -1;
                }
                parser::ExprNode* child = (parser::ExprNode*)func->children[i];
                if (flatten_filter(child, filters, options) != 0) {
                    DB_WARNING("parse AND child error");
                    return -1;
                }
            }
            return 0;
        }
        if (func->func_type == parser::FT_BETWEEN) {
            pb::Expr ge_expr;
            pb::Expr le_expr;
            if (0 != create_scala_func_expr(func, ge_expr, parser::FT_GE, options)) {
                DB_WARNING("create_scala_func_expr failed");
                return -1;
            }
            if (0 != create_scala_func_expr(func, le_expr, parser::FT_LE, options)) {
                DB_WARNING("create_scala_func_expr failed");
                return -1;
            }
            filters.push_back(ge_expr);
            filters.push_back(le_expr);
            return 0;
        }
    }
    pb::Expr root;
    int ret = create_expr_tree(item, root, options);
    if (ret < 0) {
        DB_WARNING("error pasing common expression");
        return -1;
    }
    if (!_is_correlate_subquery_expr) {
        filters.emplace_back(root);
    } else {
        pb::Expr* expr = _apply_root->apply_node.add_conditions();
        expr->CopyFrom(root);
        _is_correlate_subquery_expr = false;
    }
    return 0;
}

void LogicalPlanner::create_order_func_slot(
        int32_t& order_tuple_id, int32_t& order_slot_cnt, std::vector<pb::SlotDescriptor>& order_slots) {
    if (order_tuple_id == -1) {
        order_tuple_id = _unique_id_ctx->tuple_cnt++;
    }
    pb::SlotDescriptor slot;
    slot.set_slot_id(order_slot_cnt++);
    slot.set_tuple_id(order_tuple_id);
    slot.set_slot_type(pb::INVALID_TYPE);
    order_slots.push_back(slot);
}

std::vector<pb::SlotDescriptor>& LogicalPlanner::get_agg_func_slot(
        const std::string& agg, const std::string& fn_name, bool& new_slot) {
    if (_agg_tuple_id == -1) {
        _agg_tuple_id = _unique_id_ctx->tuple_cnt++;
        _ctx->current_table_tuple_ids.emplace(_agg_tuple_id);
    }
    static std::unordered_set<std::string> need_intermediate_slot_agg = {
        "avg", "rb_or_cardinality_agg", "rb_and_cardinality_agg", "rb_xor_cardinality_agg", "multi_count_distinct", "multi_sum_distinct",
        "multi_group_concat_distinct"
    };
    std::vector<pb::SlotDescriptor>* slots = nullptr;
    std::string agg_lower = agg;
    std::transform(agg.begin(), agg.end(), agg_lower.begin(), ::tolower);
    auto iter = _agg_slot_mapping.find(agg_lower);
    if (iter != _agg_slot_mapping.end()) {
        slots = &iter->second;
        new_slot = false;
    } else {
        slots = &_agg_slot_mapping[agg_lower];
        slots->resize(1);
        (*slots)[0].set_slot_id(_agg_slot_cnt++);
        (*slots)[0].set_tuple_id(_agg_tuple_id);
        (*slots)[0].set_slot_type(pb::STRING);
        if (need_intermediate_slot_agg.count(fn_name)) {
            // create intermediate slot
            slots->push_back((*slots)[0]);
            (*slots)[1].set_slot_id(_agg_slot_cnt++);
        }
        new_slot = true;
    }
    return *slots;
}

int LogicalPlanner::create_agg_expr(const parser::FuncExpr* expr_item, pb::Expr& expr, const CreateExprOptions& options) {
    static std::unordered_set<std::string> support_agg = {
        "count", "sum", "avg", "min", "max", "hll_add_agg", "hll_merge_agg",
        "rb_or_agg", "rb_and_agg", "rb_xor_agg", "rb_build_agg", "tdigest_agg",
        "tdigest_build_agg", "group_concat"
    };
    if (support_agg.count(expr_item->fn_name.to_lower()) == 0) {
        DB_WARNING("un-supported agg op or func: %s", expr_item->fn_name.c_str());
        _ctx->stat_info.error_code = ER_NOT_SUPPORTED_YET;
        _ctx->stat_info.error_msg << "func \'" << expr_item->fn_name.to_string() << "\' not support";
        return -1;
    }
    bool new_slot = true;

    std::string fn_name = expr_item->fn_name.to_lower();
    if (_need_multi_distinct && expr_item->distinct) {
        if (fn_name == "count") {
            fn_name = "multi_count_distinct";
        } else if (fn_name == "sum") {
            fn_name = "multi_sum_distinct";
        } else if (fn_name == "group_concat") {
            fn_name = "multi_group_concat_distinct";
        }
    }
    // prepare模式模式显示'?'号，不同item进行to_string()后可能相同
    auto& slots = get_agg_func_slot(
            expr_item->to_string(), fn_name, new_slot);
    if (slots.size() < 1) {
        DB_WARNING("wrong number of agg slots");
        return -1;
    }

    // create agg expr node
    pb::Expr agg_expr;
    pb::ExprNode* node = agg_expr.add_nodes();
    node->set_node_type(pb::AGG_EXPR);
    node->set_col_type(pb::INVALID_TYPE);
    pb::Function* func = node->mutable_fn();
    func->set_name(fn_name);
    func->set_fn_op(expr_item->func_type);
    func->set_has_var_args(false);

    pb::DeriveExprNode* derive_node = node->mutable_derive_node();
    derive_node->set_tuple_id(slots[0].tuple_id());
    derive_node->set_slot_id(slots[0].slot_id());
    if (slots.size() == 1) {
        derive_node->set_intermediate_slot_id(slots[0].slot_id());
    } else if (slots.size() == 2) {
        derive_node->set_intermediate_slot_id(slots[1].slot_id());
    } else {
        return -1;
    }
    
    bool count_star = expr_item->is_star;
    //count_star 参数为空
    if (!count_star) {
        // 禁止child 继续为agg
        CreateExprOptions options_cannot_agg = options;
        options_cannot_agg.can_agg = false;
        // count_distinct 参数为expr list，其他聚合参数为单一expr或slot ref
        for (int i = 0; i < expr_item->children.size(); i++) {
            auto item = expr_item->children[i];
            if (item != nullptr 
                    && func->name() != "group_concat" 
                    && func->name() != "multi_group_concat_distinct"
                    && item->node_type == parser::NT_EXPR
                    && static_cast<const parser::ExprNode*>(item)->expr_type == parser::ET_ROW_EXPR) {
                // 除去group concat, 其他agg聚合函数不支持row expr, 如 count(distinct(a,b))
                _ctx->stat_info.error_code = ER_OPERAND_COLUMNS;
                _ctx->stat_info.error_msg << "Operand should contain 1 column(s)";
                return -1;
            }
            if (0 != create_expr_tree(expr_item->children[i], agg_expr, options_cannot_agg)) {
                DB_WARNING("create child expr failed");
                return -1;
            }
        }
    } else {
        func->set_name(func->name() + "_star");
    }
    // min max无需distinct
    if (expr_item->distinct && func->name() != "max" && func->name() != "min"
        && func->name() != "multi_count_distinct" && func->name() != "multi_sum_distinct"
        && func->name() != "multi_group_concat_distinct") {
        func->set_name(func->name() + "_distinct");
    }
    node->set_num_children(expr_item->children.size());
    expr.MergeFrom(agg_expr);
    if (new_slot) {
        if (expr_item->distinct && func->name() != "max" && func->name() != "min") {
            _distinct_agg_funcs.push_back(agg_expr);
        } else {
            _agg_funcs.push_back(agg_expr);
        }
        if (func->name() == "group_concat") {
            // ref: https://dev.mysql.com/doc/refman/5.6/en/aggregate-functions.html#function_group-concat
            // children 0: required RowExpr expr_list, 1: required LiteralExpr separator
            // children 2: optional RowExpr order_by_expr, 3: optional RowExpr order_by_is_desc
            if (expr_item->children.size() == 4) {
                int expr_idx = 1;
                ExprNode::get_pb_expr(agg_expr, &expr_idx, nullptr); // expr_list
                ExprNode::get_pb_expr(agg_expr, &expr_idx, nullptr); // separator
                int num_children = agg_expr.nodes(expr_idx++).num_children(); // order_by_expr_row.num_children
                for (int i = 0; i < num_children; i++) {
                    pb::Expr orderby_expr;
                    ExprNode::get_pb_expr(agg_expr, &expr_idx, &orderby_expr); // order_by_expr
                    _orderby_agg_exprs.push_back(orderby_expr);
                }
            }
        }
    }
    return 0;
}

std::vector<pb::SlotDescriptor>& LogicalPlanner::get_window_func_slot(const std::string& window) {    
    if (_window_tuple_id == -1) {
        _window_tuple_id = _unique_id_ctx->tuple_cnt++;
        _ctx->current_table_tuple_ids.emplace(_window_tuple_id);
    }
    std::vector<pb::SlotDescriptor>* slots = nullptr;
    auto iter = _window_slot_mapping.find(window);
    if (iter != _window_slot_mapping.end()) {
        slots = &iter->second;
    } else {
        slots = &_window_slot_mapping[window];
        slots->resize(1);
        (*slots)[0].set_slot_id(_window_slot_cnt++);
        (*slots)[0].set_tuple_id(_window_tuple_id);
        (*slots)[0].set_slot_type(pb::STRING);
    }
    return *slots;
}

int LogicalPlanner::check_window_expr_valid(const parser::WindowFuncExpr* item) {
    static std::unordered_set<std::string> support_window = {
        "count", "sum", "avg", "min", "max", "row_number", "rank", "dense_rank", "percent_rank", 
        "cume_dist", "ntile", "lead", "lag", "first_value", "last_value", "nth_value"
    };
    if (item == nullptr) {
        DB_WARNING("item is nullptr");
        return -1;
    }
    if (item->func_expr == nullptr) {
        DB_WARNING("func_expr is nullptr");
        return -1;
    }
    if (item->window_spec == nullptr) {
        DB_WARNING("window_spec is nullptr");
        return -1;
    }
    const parser::FuncExpr* func_expr = item->func_expr;
    const std::string& fn_name = func_expr->fn_name.to_lower();
    if (support_window.count(fn_name) == 0) {
        DB_WARNING("not support window function: %s", func_expr->fn_name.c_str());
        _ctx->stat_info.error_code = ER_NOT_SUPPORTED_YET;
        _ctx->stat_info.error_msg << "func \'" << func_expr->fn_name.to_string() << "\' not support";
        return -1;
    }
    // SQLParser支持解析IGNORE NULLS，但实际执行时不支持，与MySQL一致
    if (item->ignore_null) {
        DB_WARNING("Window function not support IGNORE NULLS");
        _ctx->stat_info.error_code = ER_NOT_SUPPORTED_YET;
        _ctx->stat_info.error_msg << "Window function not support IGNORE NULLS";
        return -1;
    }
    // SQLParser支持解析FROM LAST，但实际执行时不支持，与MySQL一致
    if (item->from_last) {
        DB_WARNING("Window function not support FROM LAST");
        _ctx->stat_info.error_code = ER_NOT_SUPPORTED_YET;
        _ctx->stat_info.error_msg << "Window function not support FROM LAST";
        return -1;
    }
    // 开窗函数不支持distinct语法
    if (func_expr->distinct) {
        DB_WARNING("window function not support distinct");
        _ctx->stat_info.error_code = ER_NOT_SUPPORTED_YET;
        _ctx->stat_info.error_msg << "Window function not support distinct";
        return -1;
    }
    const parser::WindowSpec* window_spec = item->window_spec;
    if (window_spec->frame != nullptr) {
        // 如果包含Frame，则必须包含Order by
        if (window_spec->order_by == nullptr) {
            DB_WARNING("Windowing clause requires ORDER BY clause");
            _ctx->stat_info.error_code = ER_NOT_SUPPORTED_YET;
            _ctx->stat_info.error_msg << "Windowing clause requires ORDER BY clause";
            return -1;
        }
        if (window_spec->frame->frame_extent != nullptr) {
            // Frame start不支持UNBOUNDED FOLLOWING
            const parser::FrameBound* frame_start = window_spec->frame->frame_extent->frame_start;
            const parser::FrameBound* frame_end = window_spec->frame->frame_extent->frame_end;
            if (frame_start != nullptr &&
                    frame_start->bound_type == parser::BT_FOLLOWING && frame_start->is_unbounded) {
                DB_WARNING("Frame start can not be UNBOUNDED FOLLOWING");
                _ctx->stat_info.error_code = ER_NOT_SUPPORTED_YET;
                _ctx->stat_info.error_msg << "Frame start can not be UNBOUNDED FOLLOWING";
                return -1;
            }
            // Frame end不支持UNBOUNDED PRECEDING
            if (frame_end != nullptr &&
                    frame_end->bound_type == parser::BT_PRECEDING && frame_end->is_unbounded) {
                DB_WARNING("Frame end can not be UNBOUNDED PRECEDING");
                _ctx->stat_info.error_code = ER_NOT_SUPPORTED_YET;
                _ctx->stat_info.error_msg << "Frame end can not be UNBOUNDED PRECEDING";
                return -1;
            }
            if (window_spec->frame->frame_type == parser::FT_RANGE) {
                // 目前RANGE模式，只支持以下四种形式:
                //      RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                //      RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                //      RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                //      RANGE BETWEEN CURRENT ROW AND CURRENT ROW
                if (frame_start != nullptr &&
                        (!frame_start->is_unbounded && frame_start->bound_type != parser::BT_CURRENT_ROW)) {
                    DB_WARNING("RANGE is only supported with both the lower and upper bounds "
                               "UNBOUNDED or CURRENT ROW");
                    _ctx->stat_info.error_code = ER_NOT_SUPPORTED_YET;
                    _ctx->stat_info.error_msg << "RANGE is only supported with both the lower and upper bounds "
                                                 "UNBOUNDED or CURRENT ROW";
                    return -1;
                }
                if (frame_end != nullptr &&
                        (!frame_end->is_unbounded && frame_end->bound_type != parser::BT_CURRENT_ROW)) {
                    DB_WARNING("RANGE is only supported with both the lower and upper bounds "
                               "UNBOUNDED or CURRENT ROW");
                    _ctx->stat_info.error_code = ER_NOT_SUPPORTED_YET;
                    _ctx->stat_info.error_msg << "RANGE is only supported with both the lower and upper bounds "
                                                 "UNBOUNDED or CURRENT ROW";
                    return -1;
                }
            }
        }
        if (fn_name == "row_number" || fn_name == "rank" || fn_name == "dense_rank" || fn_name == "percent_rank" ||
                fn_name == "cume_dist" || fn_name == "ntile" || fn_name == "lead" || fn_name == "lag") {
            DB_WARNING("Windowing clause not allowed with '%s'", fn_name.c_str());
            _ctx->stat_info.error_code = ER_NOT_SUPPORTED_YET;
            _ctx->stat_info.error_msg << "Windowing clause not allowed with \'" << fn_name.c_str() << "\'";
            return -1;
        }
    }
    return 0;
}

int LogicalPlanner::create_window_expr(const parser::WindowFuncExpr* item, pb::Expr& expr, const CreateExprOptions& options) {
    if (check_window_expr_valid(item) != 0) {
        DB_WARNING("Fail to check window expr valid");
        return -1;
    }
    // prepare模式模式显示'?'号，不同item进行to_string()后可能相同
    const auto& slots = get_window_func_slot(item->to_string());
    if (slots.size() != 1) {
        DB_WARNING("wrong number of window slots");
        return -1;
    }
    const parser::FuncExpr* func_expr = item->func_expr;
    const parser::WindowSpec* window_spec = item->window_spec;
    std::string fn_name = func_expr->fn_name.to_lower();
    if (fn_name == "count" && func_expr->is_star) {
        fn_name = "count_star";
    }
    pb::Expr window_expr;
    pb::ExprNode* node = window_expr.add_nodes();
    node->set_node_type(pb::WINDOW_EXPR);
    node->set_col_type(pb::INVALID_TYPE);
    pb::Function* func = node->mutable_fn();
    func->set_name(fn_name);
    func->set_fn_op(func_expr->func_type);
    func->set_has_var_args(false);
    pb::DeriveExprNode* derive_node = node->mutable_derive_node();
    derive_node->set_tuple_id(slots[0].tuple_id());
    derive_node->set_slot_id(slots[0].slot_id());
    for (int i = 0; i < func_expr->children.size(); ++i) {
        if (create_expr_tree(func_expr->children[i], window_expr, options)) {
            DB_WARNING("Fail to create_expr_tree");
            return -1;
        }
    }
    node->set_num_children(func_expr->children.size());
    expr.MergeFrom(window_expr);

    pb::WindowNode window_node;
    // func expr
    window_node.add_func_exprs()->CopyFrom(window_expr);
    // window spec
    pb::WindowSpec* pb_window_spec = window_node.mutable_window_spec();
    if (create_window_spec(fn_name, window_spec, *pb_window_spec, options) != 0) {
        DB_WARNING("Fail to create window spec");
        return -1;
    }
    _window_nodes.emplace_back(std::move(window_node));
    // prepare模式模式显示'?'号，不同窗口进行to_string()后可能相同，先不支持prepare模式
    _window_specs.emplace_back(window_spec->to_string());
    _ctx->has_window_func = true;
    _ctx->has_unable_cache_expr = true;
    return 0;
}

int LogicalPlanner::create_window_spec(
        const std::string& fn_name, const parser::WindowSpec* item, 
        pb::WindowSpec& window_spec, const CreateExprOptions& options) {
    if (item == nullptr) {
        DB_WARNING("item is nullptr");
        return -1;
    }
    // 数据集按照(partition_by_list, order_by_list)进行排序
    int32_t order_tuple_id = -1;
    int32_t order_slot_cnt = 1;
    std::vector<pb::SlotDescriptor> order_slots;
    std::vector<pb::Expr> order_exprs;
    std::vector<bool> order_ascs;
    // partition by
    parser::PartitionByClause* partition_by = item->partition_by;
    if (partition_by != nullptr) {
        for (int i = 0; i < partition_by->items.size(); ++i) {
            const parser::ByItem* by_item = partition_by->items[i];
            if (by_item == nullptr) {
                DB_WARNING("by_item is nullptr");
                return -1;
            }
            if (by_item->node_type != parser::NT_BY_ITEM) {
                DB_WARNING("un-supported partition-by item type: %d", by_item->node_type);
                return -1;
            }
            pb::Expr* expr = window_spec.add_partition_exprs();
            if (create_expr_tree(by_item->expr, *expr, options) != 0) {
                DB_WARNING("Fail to create_expr_tree");
                return -1;
            }
        }
        if (create_orderby_exprs(
                partition_by->items, order_tuple_id, order_slot_cnt, order_slots, order_exprs, order_ascs) != 0) {
            DB_WARNING("Fail to create order by exprs");
            return -1;
        }
    }
    // order by
    parser::OrderByClause* order_by = item->order_by;
    if (order_by != nullptr) {
        if (create_orderby_exprs(
                order_by->items, order_tuple_id, order_slot_cnt, order_slots, order_exprs, order_ascs) != 0) {
            DB_WARNING("Fail to create order by exprs");
            return -1;
        }
    }
    if (order_exprs.size() != 0) {
        _window_sort_slot_mapping[order_tuple_id].swap(order_slots);
        for (auto& order_expr : order_exprs) {
            pb::Expr* expr = window_spec.add_order_exprs();
            expr->Swap(&order_expr);
        }
        for (auto order_asc : order_ascs) {
            window_spec.add_is_asc(order_asc);
        }
        window_spec.set_tuple_id(order_tuple_id);
    }
    if (fn_name == "row_number" || fn_name == "rank" || fn_name == "dense_rank" || fn_name == "percent_rank" ||
            fn_name == "cume_dist" || fn_name == "ntile" || fn_name == "lead" || fn_name == "lag") {
        // 这些窗口函数不需要Frame
        return 0;
    }
    // window frame
    parser::WindowFrameClause* frame = item->frame;
    if (frame != nullptr) {
        pb::WindowFrame* window_frame = window_spec.mutable_window_frame();
        if (frame->frame_type == parser::FT_ROWS) {
            window_frame->set_frame_type(pb::FT_ROWS);
        } else if (frame->frame_type == parser::FT_RANGE) {
            window_frame->set_frame_type(pb::FT_RANGE);
        } else {
            DB_WARNING("not frame type: %d", frame->frame_type);
            _ctx->stat_info.error_code = ER_NOT_SUPPORTED_YET;
            _ctx->stat_info.error_msg << "Windowing clause only support ROWS/RANGE frame";
            return -1;
        }
        const parser::FrameExtent* frame_extent = frame->frame_extent;
        if (frame_extent == nullptr) {
            DB_WARNING("frame_extent is nullptr");
            return -1;
        }
        pb::FrameExtent* pb_frame_extent = window_frame->mutable_frame_extent();
        if (create_window_frame_bound(
                frame_extent->frame_start, *pb_frame_extent->mutable_frame_start(), options) != 0) {
            DB_WARNING("Fail to create window frame start");
            return -1;
        }
        if (create_window_frame_bound(
                frame_extent->frame_end, *pb_frame_extent->mutable_frame_end(), options) != 0) {
            DB_WARNING("Fail to create window frame end");
            return -1;
        }
    } else {
        // 没有FRAME场景
        //      如果有ORDER BY，则FRAME设置为: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        //      如果没有ORDER BY，则FRAME设置为: RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        pb::WindowFrame* window_frame = window_spec.mutable_window_frame();
        window_frame->set_frame_type(pb::FT_RANGE);
        pb::FrameExtent* pb_frame_extent = window_frame->mutable_frame_extent();
        pb::FrameBound* pb_frame_start = pb_frame_extent->mutable_frame_start();
        pb_frame_start->set_bound_type(pb::BT_PRECEDING);
        pb_frame_start->set_is_unbounded(true);
        pb::FrameBound* pb_frame_end = pb_frame_extent->mutable_frame_end();
        if (order_by != nullptr) {
            pb_frame_end->set_bound_type(pb::BT_CURRENT_ROW);
        } else {
            pb_frame_end->set_bound_type(pb::BT_FOLLOWING);
            pb_frame_end->set_is_unbounded(true);
        }
    }
    return 0;
}

int LogicalPlanner::create_window_frame_bound(
        const parser::FrameBound* bound, pb::FrameBound& frame_bound, const CreateExprOptions& options) {
    if (bound == nullptr) {
        DB_WARNING("bound is nullptr");
        return -1;
    }
    switch (bound->bound_type) {
    case parser::BT_PRECEDING: {
        frame_bound.set_bound_type(pb::BT_PRECEDING);
        break;
    }
    case parser::BT_FOLLOWING: {
        frame_bound.set_bound_type(pb::BT_FOLLOWING);
        break;
    }
    case parser::BT_CURRENT_ROW: {
        frame_bound.set_bound_type(pb::BT_CURRENT_ROW);
        break;
    }
    default: {
        DB_WARNING("Invalid frame bound type: %d", bound->bound_type);
        return -1;
    }
    }
    frame_bound.set_is_unbounded(bound->is_unbounded);
    if (bound->expr != nullptr) {
        if (create_expr_tree(bound->expr, *frame_bound.mutable_expr(), options) != 0) {
            DB_WARNING("Fail to create_expr_tree");
            return -1;
        }
    }
    return 0;
}

int LogicalPlanner::create_between_expr(const parser::FuncExpr* item, pb::Expr& expr, const CreateExprOptions& options) {
    pb::ExprNode* and_node = expr.add_nodes();
    and_node->set_col_type(pb::BOOL);
    and_node->set_node_type(pb::AND_PREDICATE);
    and_node->set_num_children(2);
    pb::Function* func = and_node->mutable_fn();
    func->set_name("logic_and");
    func->set_fn_op(parser::FT_LOGIC_AND);

    if (0 != create_scala_func_expr(item, expr, parser::FT_GE, options)) {
        DB_WARNING("create_scala_func_expr failed");
        return -1;
    }
    if (0 != create_scala_func_expr(item, expr, parser::FT_LE, options)) {
        DB_WARNING("create_scala_func_expr failed");
        return -1;
    }
    return 0;
}

int LogicalPlanner::create_values_expr(const parser::FuncExpr* func_item, pb::Expr& expr) {
    if (_ctx->stmt_type != parser::NT_INSERT) {
        pb::ExprNode* node = expr.add_nodes();
        node->set_node_type(pb::NULL_LITERAL);
        node->set_col_type(pb::NULL_TYPE);
        return 0;
    }
    // values param must be ColumnName
    CreateExprOptions options;
    options.is_values = true;
    if (0 != create_term_slot_ref_node((parser::ColumnName*)func_item->children[0], expr, options)) {
        DB_WARNING("create_term_slot_ref_node failed");
        return -1;
    }

    return 0;
}

int LogicalPlanner::construct_apply_node(QueryContext* sub_ctx,
            pb::Expr& expr,
            const pb::JoinType join_type,
            const CreateExprOptions& options) {
    if (sub_ctx == nullptr) {
        DB_WARNING("sub_ctx is nullptr");
        return -1;
    }
    if (sub_ctx->has_window_func) {
        DB_WARNING("Not support correlated subquery when subquery has window func");
        _ctx->stat_info.error_code = ER_NOT_SUPPORTED_YET;
        _ctx->stat_info.error_msg << "Not support correlated subquery when subquery has window func";
        return -1;
    }
    for (auto& kv : sub_ctx->derived_table_ctx_mapping) {
        _ctx->derived_table_ctx_mapping[kv.first] = kv.second;
        _ctx->add_sub_ctx(kv.second);
        _ctx->has_derived_table = true;
    }
    for (auto& kv : sub_ctx->slot_column_mapping) {
        _ctx->slot_column_mapping[kv.first] = kv.second;
    }
    std::unique_ptr<ApplyMemTmp> apply_node_mem(new ApplyMemTmp);
    apply_node_mem->apply_node.set_join_type(join_type);
    apply_node_mem->apply_node.set_max_one_row(options.max_one_row);
    apply_node_mem->apply_node.set_is_select_field(options.is_select_field);
    apply_node_mem->outer_node = _join_root;
    std::set<int64_t> left_tuple_ids;
    for (auto& tuple_id : _join_root->join_node.left_tuple_ids()) {
        apply_node_mem->apply_node.add_left_tuple_ids(tuple_id);
        left_tuple_ids.insert(tuple_id);
    }
    for (auto& tuple_id : _join_root->join_node.right_tuple_ids()) {
        apply_node_mem->apply_node.add_left_tuple_ids(tuple_id);
        left_tuple_ids.insert(tuple_id);
    }
    for (auto& table_id : _join_root->join_node.left_table_ids()) {
        apply_node_mem->apply_node.add_left_table_ids(table_id);
    }
    for (auto& table_id : _join_root->join_node.right_table_ids()) {
        apply_node_mem->apply_node.add_left_table_ids(table_id);
    }

    const pb::PacketNode& packet_node = sub_ctx->plan.nodes(0).derive_node().packet_node();
    if (options.row_expr_size > 1) {
        pb::ExprNode* row_node = expr.add_nodes();
        row_node->set_node_type(pb::ROW_EXPR);
        row_node->set_num_children(options.row_expr_size);
        if (options.row_expr_size != packet_node.projections_size()) {
            if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                _ctx->stat_info.error_code = ER_OPERAND_COLUMNS;
                _ctx->stat_info.error_msg << "Operand should contain " << options.row_expr_size << " column(s)";
            }
            return -1;
        }
    }
    for (int i = 0; i < packet_node.projections_size(); i++) {
        const pb::Expr& child_expr = packet_node.projections(i);
        for (int j = 0; j < child_expr.nodes_size(); j++) {
            pb::ExprNode* node = expr.add_nodes();
            node->CopyFrom(child_expr.nodes(j));
        }
    }

    pb::Plan inner_plan;
    // 消除 packet node
    for (int i = 1; i < sub_ctx->plan.nodes_size(); i++) {
        pb::PlanNode* node = inner_plan.add_nodes();
        node->CopyFrom(sub_ctx->plan.nodes(i));
    }
    apply_node_mem->inner_plan = inner_plan;
    for (auto& tuple : sub_ctx->tuple_descs()) {
        if (sub_ctx->current_table_tuple_ids.count(tuple.tuple_id()) > 0) {
            apply_node_mem->apply_node.add_right_tuple_ids(tuple.tuple_id());
            if(tuple.has_table_id()) {
                apply_node_mem->apply_node.add_right_table_ids(tuple.table_id());
            }
        }
        _ctx->add_tuple(tuple);
    }
    if (_apply_root == nullptr) {
        _apply_root = apply_node_mem.release();
    } else {
        if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
            _ctx->stat_info.error_code = ER_NOT_SUPPORTED_YET;
            _ctx->stat_info.error_msg << "multiple correlate subquery not support yet";
        };
        return -1;
    }
    return 0;
 }

int LogicalPlanner::handle_scalar_subquery(const parser::FuncExpr* func_item,
        pb::Expr& expr,
        const CreateExprOptions& options) {
    parser::ExprNode* arg1 = (parser::ExprNode*)func_item->children[0];
    int row_expr_size = 1;
    if (arg1->expr_type == parser::ET_ROW_EXPR) {
        row_expr_size = arg1->children.size();
    }
    parser::ExprNode* arg2 = (parser::ExprNode*)func_item->children[1];
    if (func_item->children.size() != 2 || arg1->is_subquery() || !arg2->is_subquery()) {
        if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
            _ctx->stat_info.error_code = ER_NOT_SUPPORTED_YET;
            _ctx->stat_info.error_msg << "only support binary_op subquery at right side, e.g: expr op (subquery)";
        }
        return -1;
    }
    if (0 != create_expr_tree(arg1, expr, options)) {
        DB_WARNING("create child 1 expr failed");
        return -1;
    }
    pb::Expr tmp_expr;
    int ret = create_common_subquery_expr((parser::SubqueryExpr*)arg2, tmp_expr, options, _is_correlate_subquery_expr);
    if (ret < 0) {
        DB_WARNING("create child expr failed");
        return -1;
    }
    if (!_is_correlate_subquery_expr) {
        if (tmp_expr.nodes(0).node_type() == pb::ROW_EXPR) {
            if (tmp_expr.nodes(0).num_children() != row_expr_size) {
                DB_WARNING("Operand should contain %d column(s)", row_expr_size);
                if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                    _ctx->stat_info.error_code = ER_OPERAND_COLUMNS;
                    _ctx->stat_info.error_msg << "Operand should contain " << row_expr_size << " column(s)";
                }
                return -1;
            }
        }
        for (int i = 0; i < tmp_expr.nodes_size(); i++) {
            pb::ExprNode* child_node = expr.add_nodes();
            child_node->CopyFrom(tmp_expr.nodes(i));
        }
    } else {
        CreateExprOptions tm_options = options;
        tm_options.row_expr_size = row_expr_size;
        ret = construct_apply_node(_cur_sub_ctx.get(), expr, pb::SEMI_JOIN, tm_options);
        if (ret < 0) {
            DB_WARNING("construct apply node failed");
            return -1;
        }
    }
    return 0;
}

// TODO in next stage: fill func name
// fill arg_types, return_type(col_type) and has_var_args
int LogicalPlanner::create_scala_func_expr(const parser::FuncExpr* item, 
        pb::Expr& expr, parser::FuncType op, const CreateExprOptions& options) {
    if (op == parser::FT_COMMON) {
        std::string lower_fn_name = item->fn_name.to_lower();
        if (lower_fn_name == "last_insert_id" && item->children.size() == 0) {
            pb::ExprNode* node = expr.add_nodes();
            node->set_node_type(pb::INT_LITERAL);
            node->set_col_type(pb::INT64);
            node->set_num_children(0);
            node->mutable_derive_node()->set_int_val(_ctx->client_conn->last_insert_id);
            _ctx->has_unable_cache_expr = true;
            return 0;
        }
        if (lower_fn_name == "database" ||
            lower_fn_name == "schema") {
            pb::ExprNode* node = expr.add_nodes();
            if (_ctx->client_conn->current_db == "") {
                node->set_node_type(pb::NULL_LITERAL);
                node->set_col_type(pb::NULL_TYPE);
            } else{
                node->set_node_type(pb::STRING_LITERAL);
                node->set_col_type(pb::STRING);
                node->set_num_children(0);
                node->mutable_derive_node()->set_string_val(_ctx->client_conn->current_db);
            }
            _ctx->has_unable_cache_expr = true;
            return 0;
        }
        if (lower_fn_name == "user" ||
            lower_fn_name == "session_user" ||
            lower_fn_name == "system_user") {
            pb::ExprNode* node = expr.add_nodes();
            node->set_node_type(pb::STRING_LITERAL);
            node->set_col_type(pb::STRING);
            node->set_num_children(0);
            node->mutable_derive_node()->set_string_val(_ctx->client_conn->username
                + "@" + _ctx->client_conn->ip);
            _ctx->has_unable_cache_expr = true;
            return 0;
        }

        if (item->fn_name.to_lower() == "default" && item->children.size() == 1) {
            auto col_expr = (parser::ColumnName*)(item->children[0]);
            std::string alias_name = get_field_alias_name(col_expr);
            if (alias_name.empty()) {
                DB_WARNING("get_field_alias_name failed: %s", col_expr->to_string().c_str());
                return -1;
            }
            std::string full_name = alias_name;
            full_name += ".";
            full_name += col_expr->name.to_lower();
            FieldInfo* field_info = nullptr;
            if (nullptr == (field_info = get_field_info_ptr(full_name))) {
                DB_WARNING("invalid field name: %s full_name: %s", col_expr->to_string().c_str(), full_name.c_str());
                return -1;
            }
            if (field_info->default_expr_value.is_null()) {
                if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                    _ctx->stat_info.error_code = ER_NO_DEFAULT_FOR_FIELD;
                    _ctx->stat_info.error_msg << "Field '" << col_expr->name.to_lower() << "' doesn't have a default value";
                }
                return -1;
            }
            pb::ExprNode* node = expr.add_nodes();
            Literal literal = Literal(field_info->default_expr_value);
            literal.transfer_pb(node);
            _ctx->has_unable_cache_expr = true;
            return 0;
        }
        if (FunctionManager::instance()->get_object(item->fn_name.to_lower()) == nullptr) {
            if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                _ctx->stat_info.error_code = ER_NOT_SUPPORTED_YET;
                _ctx->stat_info.error_msg << "func: \'" << item->fn_name.c_str() << "\' not support";
            }
            DB_WARNING("un-supported scala op or func: %s", item->fn_name.c_str());
            return -1;
        } 
    }

    pb::ExprNode* node = expr.add_nodes();
    node->set_node_type(pb::FUNCTION_CALL);
    node->set_col_type(pb::INVALID_TYPE);
    node->set_num_children(item->children.size());
    pb::Function* func = node->mutable_fn();
    if (item->fn_name.empty()) {
        DB_WARNING("op:%d fn_name is empty", op);
    }
    func->set_name(item->fn_name.to_lower());
    func->set_fn_op(op);

    if (item->has_subquery()) {
        return handle_scalar_subquery(item, expr, options);
    }
    // between => (>= && <=)
    if (item->func_type == parser::FT_BETWEEN && item->children.size() == 3) {
        create_expr_tree(item->children[0], expr, options);
        node->set_num_children(2);
        if (op == parser::FT_GE) {
            func->set_name("ge");
            create_expr_tree(item->children[1], expr, options);
        } else if (op == parser::FT_LE) {
            func->set_name("le");
            create_expr_tree(item->children[2], expr, options);
        }
        return 0;
    }
    int row_expr_size = 1;
    for (int32_t idx = 0; idx < item->children.size(); ++idx) {
        auto node = static_cast<const parser::ExprNode*>(item->children[idx]);
        if (idx == 0) {
            if (node->expr_type == parser::ET_ROW_EXPR) {
                row_expr_size = item->children[idx]->children.size();
            }
        } else {
            if (node->expr_type == parser::ET_ROW_EXPR) {
                if (row_expr_size != item->children[idx]->children.size()) {
                    DB_WARNING("Operand should contain %d column(s)", row_expr_size);
                    if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                        _ctx->stat_info.error_code = ER_OPERAND_COLUMNS;
                        _ctx->stat_info.error_msg << "Operand should contain " << row_expr_size << " column(s)";
                    }
                    return -1;
                }
            } else if (row_expr_size != 1) {
                DB_WARNING("Operand should contain %d column(s)", row_expr_size);
                if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                    _ctx->stat_info.error_code = ER_OPERAND_COLUMNS;
                    _ctx->stat_info.error_msg << "Operand should contain " << row_expr_size << " column(s)";
                }
                return -1;
            }
        }
        if (0 != create_expr_tree(item->children[idx], expr, options)) {
            DB_WARNING("create child expr failed");
            return -1;
        }
    }
    return 0;
}

void LogicalPlanner::construct_literal_expr(const ExprValue& value, pb::ExprNode* node) {
    Literal literal = Literal(value);
    node->set_num_children(0);
    node->set_node_type(literal.node_type());
    node->set_col_type(literal.col_type());
    switch (literal.node_type()) {
        case pb::INT_LITERAL:
            node->mutable_derive_node()->set_int_val(value.get_numberic<int64_t>());
            break;
        case pb::DOUBLE_LITERAL:
            node->mutable_derive_node()->set_double_val(value.get_numberic<double>());
            break;
        case pb::STRING_LITERAL:
            node->mutable_derive_node()->set_string_val(value.get_string());
            break;
        case pb::BOOL_LITERAL:
            node->mutable_derive_node()->set_bool_val(value.get_numberic<bool>());
            break;
        case pb::NULL_LITERAL:
            break;
        case pb::TIMESTAMP_LITERAL:
        case pb::TIME_LITERAL:
        case pb::DATE_LITERAL:
            node->mutable_derive_node()->set_int_val(value.get_numberic<int32_t>());
            break;
        case pb::DATETIME_LITERAL:
            node->mutable_derive_node()->set_int_val(value.get_numberic<int64_t>());
            break;
        default:
            DB_WARNING("expr:%s", value.get_string().c_str());
            break;
    }    
}

int LogicalPlanner::exec_subquery_expr(std::shared_ptr<QueryContext> smart_sub_ctx, QueryContext* ctx) {
    QueryContext* sub_ctx = smart_sub_ctx.get();
    // sub_ctx->sql_exec_type_defined = SignExecType::SIGN_EXEC_ROW;
    int ret = PhysicalPlanner::analyze(sub_ctx);
    if (ret < 0) {
        DB_WARNING("exec PhysicalPlanner failed");
        return -1;
    }
    RuntimeState& state = *sub_ctx->get_runtime_state();
    ret = state.init(sub_ctx, nullptr);
    if (ret < 0) {
        DB_WARNING("init RuntimeState failed www");
        return -1;
    }
    state.set_is_expr_subquery(true);
    if (ctx->is_explain && ctx->explain_type == EXPLAIN_NULL) {
        // 如果是普通explain (非explain format='plan')
        // 把生成的物理计划暂存在runtime_state内，后面再explain
        // 需要把fake data回填到对应的查询内 
        state.is_explain = true;
        auto packet_node = smart_sub_ctx->root->get_node(pb::PACKET_NODE);
        std::vector<ExprNode*>& projections = dynamic_cast<PacketNode*>(packet_node)->mutable_projections();
        auto subquery_exprs_vec_ptr = state.mutable_subquery_exprs();
        std::vector<ExprValue> single_row;
        for(int i = 0; i < projections.size(); ++i) {
            pb::PrimitiveType field_type = projections[i]->col_type();
            if (field_type == pb::DATETIME || field_type == pb::TIMESTAMP 
                    || field_type == pb::DATE || field_type == pb::TIME) {
                single_row.emplace_back(field_type, "2024-01-01");
            } else {
                single_row.emplace_back(field_type, "0");
            }
        }
        subquery_exprs_vec_ptr->emplace_back(single_row);
        ctx->add_noncorrelated_subquerys(smart_sub_ctx);
        return 0;
    }

    if (sub_ctx->use_mpp) {
        return PhysicalPlanner::execute_mpp(sub_ctx, nullptr);
    }
    ret = sub_ctx->root->open(&state);
    ctx->update_ctx_stat_info(&state, sub_ctx->get_ctx_total_time());
    if (ret < 0) {
        return -1;
    }
    return 0;   
}

// in (SubSelect) or select (SubSelect)
int LogicalPlanner::create_common_subquery_expr(const parser::SubqueryExpr* item, pb::Expr& expr,
    const CreateExprOptions& options, bool& is_correlate) {
    ExprParams expr_params;
    expr_params.is_expr_subquery = true;
    int ret = gen_subquery_plan(item->query_stmt, _plan_table_ctx, expr_params);
    if (ret < 0) {
        DB_WARNING("gen subquery plan failed");
        return -1;
    }
    if (!_cur_sub_ctx->expr_params.is_correlated_subquery) {
        ret = exec_subquery_expr(_cur_sub_ctx, _ctx);
        SmartState state = _cur_sub_ctx->get_runtime_state();
        ON_SCOPE_EXIT(([this, state]() {
            // 列必须要在使用完结果才能close
            if (state->is_expr_subquery() && _cur_sub_ctx->root != nullptr 
                    && (!_cur_sub_ctx->is_explain || _cur_sub_ctx->explain_type != EXPLAIN_NULL)) {
                _cur_sub_ctx->root->close(state.get());
            }
        }));
        if (ret < 0) {
            DB_WARNING("exec subquery failed");
            return -1;
        }
        bool run_memrow = (state->execute_type == pb::EXEC_ROW);
        auto& subquery_exprs_vec = state->get_subquery_exprs();
        std::shared_ptr<arrow::Table> vec_result_table = state->subquery_result_table;
        if (!run_memrow && vec_result_table == nullptr) {
            DB_FATAL("run vectorized but subquery result table is null");
            return -1;
        }
        int64_t row_cnt = subquery_exprs_vec.size();
        if (!run_memrow) {
            row_cnt = vec_result_table->num_rows();
        }
        if (options.max_one_row && row_cnt > 1) {
            _ctx->stat_info.error_code = ER_SUBQUERY_NO_1_ROW;
            _ctx->stat_info.error_msg << "Subquery returns more than 1 row";
            DB_WARNING("Subquery returns more than 1 row");
            return -1;
        }
        // select (SubSelect)
        if (options.is_select_field) {
            if (row_cnt == 0) {
                pb::ExprNode* node = expr.add_nodes();
                node->set_node_type(pb::NULL_LITERAL);
            } else {
                if (run_memrow) {
                    if (subquery_exprs_vec.size() != 1 || subquery_exprs_vec[0].size() != 1) {
                        _ctx->stat_info.error_code = ER_SUBQUERY_NO_1_ROW;
                        _ctx->stat_info.error_msg << "Subquery returns more than 1 row";
                        DB_WARNING("Subquery returns more than 1 row");
                        return -1;
                    }
                    pb::ExprNode* node = expr.add_nodes();
                    construct_literal_expr(subquery_exprs_vec[0][0], node); 
                } else {
                    if (vec_result_table->num_rows() != 1 || vec_result_table->num_columns() != 1) {
                        _ctx->stat_info.error_code = ER_SUBQUERY_NO_1_ROW;
                        _ctx->stat_info.error_msg << "Subquery returns more than 1 row";
                        DB_WARNING("Subquery returns more than 1 row");
                        return -1;
                    }
                    pb::ExprNode* node = expr.add_nodes();
                    construct_literal_expr(VectorizeHelpper::get_vectorized_value(vec_result_table->column(0).get(), 0), node);
                }
            }
        // in (SubSelect)
        } else {
            if (row_cnt > 0) {
                if (run_memrow) {
                    for (auto& rows : subquery_exprs_vec) {
                        if (rows.size() > 1) {
                            pb::ExprNode* row_node = expr.add_nodes();
                            row_node->set_node_type(pb::ROW_EXPR);
                            row_node->set_num_children(rows.size());
                            for (auto& row : rows) {
                                pb::ExprNode* node = expr.add_nodes();
                                construct_literal_expr(row, node);
                            }
                        } else {
                            pb::ExprNode* node = expr.add_nodes();
                            construct_literal_expr(rows[0], node);
                        }
                    }
                } else {
                    const int column_size = vec_result_table->num_columns();
                    for (int64_t row_idx = 0; row_idx < vec_result_table->num_rows(); ++row_idx) {
                        // 列转行
                        if (column_size > 1) {
                            pb::ExprNode* row_node = expr.add_nodes();
                            row_node->set_node_type(pb::ROW_EXPR);
                            row_node->set_num_children(column_size);
                            for (int column_idx = 0; column_idx < column_size; ++column_idx) {
                                pb::ExprNode* node = expr.add_nodes();
                                construct_literal_expr(VectorizeHelpper::get_vectorized_value(vec_result_table->column(column_idx).get(), row_idx), node);
                            }
                        } else {
                            pb::ExprNode* node = expr.add_nodes();
                            construct_literal_expr(VectorizeHelpper::get_vectorized_value(vec_result_table->column(0).get(), row_idx), node);
                        }
                    }
                }
            } else {
                DB_WARNING("not data row_filed_number:%d", _cur_sub_ctx->expr_params.row_filed_number);
                if (_cur_sub_ctx->expr_params.row_filed_number > 1) {
                    pb::ExprNode* row_node = expr.add_nodes();
                    row_node->set_node_type(pb::ROW_EXPR);
                    row_node->set_num_children(_cur_sub_ctx->expr_params.row_filed_number);
                    for (int i = 0; i < _cur_sub_ctx->expr_params.row_filed_number; i++) {
                        pb::ExprNode* node = expr.add_nodes();
                        node->set_node_type(pb::NULL_LITERAL);
                        node->set_col_type(pb::NULL_TYPE);
                    }
                } else {
                    pb::ExprNode* node = expr.add_nodes();
                    node->set_node_type(pb::NULL_LITERAL);
                    node->set_col_type(pb::NULL_TYPE);
                }
            }
        }
        return 0;
    }
    is_correlate = true;
    return 0;
}

int LogicalPlanner::handle_compare_subquery(const parser::ExprNode* expr_item, pb::Expr& expr,
    const CreateExprOptions& options) {
    parser::CompareSubqueryExpr* sub_query_expr = (parser::CompareSubqueryExpr*)expr_item;
    pb::Expr compare_expr;
    pb::ExprNode* node = compare_expr.add_nodes();
    node->set_node_type(pb::FUNCTION_CALL);
    node->set_col_type(pb::INVALID_TYPE);
    pb::Function* func = node->mutable_fn();
    func->set_name(sub_query_expr->get_func_name());
    func->set_fn_op(sub_query_expr->func_type);
    node->set_num_children(2);

    pb::Expr left_expr;
    parser::ExprNode* arg1 = (parser::ExprNode*)sub_query_expr->left_expr;
    if (arg1->expr_type == parser::ET_ROW_EXPR && arg1->children.size() > 1) {
        _ctx->stat_info.error_code = ER_OPERAND_COLUMNS;
        _ctx->stat_info.error_msg << "Operand should contain 1 column(s)";
        DB_WARNING("Operand should contain 1 column(s)");
        return -1;
    }
    int ret = create_expr_tree(sub_query_expr->left_expr, left_expr, options);
    if (ret < 0) {
        DB_WARNING("create expr failed");
        return -1;
    }
    ExprParams expr_params;
    expr_params.is_expr_subquery = true;
    expr_params.func_type = sub_query_expr->func_type;
    expr_params.cmp_type = sub_query_expr->cmp_type;
    ret = gen_subquery_plan(sub_query_expr->right_expr->query_stmt, _plan_table_ctx, expr_params);
    if (ret < 0) {
        return -1;
    }
    if (_cur_sub_ctx->field_column_id_mapping.size() != 1) {
        _ctx->stat_info.error_code = ER_OPERAND_COLUMNS;
        _ctx->stat_info.error_msg << "Operand should contain 1 column(s)";
        DB_WARNING("Operand should contain 1 column(s)");
        return -1;
    }
    // 非相关子查询表达式，直接执行获取结果
    if (!_cur_sub_ctx->expr_params.is_correlated_subquery) {
        ret = exec_subquery_expr(_cur_sub_ctx, _ctx);
        SmartState state = _cur_sub_ctx->get_runtime_state();
        ON_SCOPE_EXIT(([this, state]() {
            // 列必须要在使用完结果才能close
            if (state->is_expr_subquery() && _cur_sub_ctx->root != nullptr 
                    && (!_cur_sub_ctx->is_explain || _cur_sub_ctx->explain_type != EXPLAIN_NULL)) {
                _cur_sub_ctx->root->close(state.get());
            }
        }));
        if (ret < 0) {
            return -1;
        }
        bool run_memrow = (state->execute_type == pb::EXEC_ROW);
        auto& subquery_exprs_vec = state->get_subquery_exprs();
        std::shared_ptr<arrow::Table> vec_result_table = state->subquery_result_table;
        bool is_eq_all = false;
        ExprValue first_val;
        bool always_false = false;
        bool is_neq_any = false;
        if (!run_memrow && vec_result_table == nullptr) {
            DB_FATAL("run vectorized but subquery result table is null");
            return -1;
        }
        auto row_cnt = subquery_exprs_vec.size();
        if (!run_memrow) {
            row_cnt = vec_result_table->num_rows();
        }
        // =
        if (_cur_sub_ctx->expr_params.func_type == parser::FT_EQ) {
            pb::ExprNode* node = compare_expr.mutable_nodes(0);
            node->set_col_type(pb::BOOL);
            node->set_node_type(pb::IN_PREDICATE);
            pb::Function* func = node->mutable_fn();
            func->set_name("in");
            func->set_fn_op(parser::FT_IN);
            // = all (xxx) 子查询返回的不同值最多1个，否则恒为false
            if (_cur_sub_ctx->expr_params.cmp_type == parser::CMP_ALL) {
                is_eq_all = true;
            }
            if (row_cnt > 0) {
                if (is_eq_all) {
                    if (run_memrow) { 
                        first_val = subquery_exprs_vec[0][0];
                        for (uint32_t i = 1; i < subquery_exprs_vec.size(); i++) {
                            // = all返回多个不同值
                            if (first_val.compare(subquery_exprs_vec[i][0]) != 0) {
                                DB_WARNING("always_false");
                                always_false = true;
                            }
                        }
                    } else {
                        // 行转列
                        first_val = VectorizeHelpper::get_vectorized_value(vec_result_table->column(0).get(), 0);
                        for (uint32_t i = 1; i < vec_result_table->num_rows(); i++) {
                            // = all返回多个不同值
                            if (first_val.compare(VectorizeHelpper::get_vectorized_value(vec_result_table->column(0).get(), i)) != 0) {
                                DB_WARNING("always_false");
                                always_false = true;
                            }
                        }
                    }
                    node->set_num_children(2);
                } else {
                    node->set_num_children(subquery_exprs_vec.size() + 1);
                }
            } else {
                node->set_num_children(2);
            }
        // != all -> not in (xxx)
        } else if (_cur_sub_ctx->expr_params.func_type == parser::FT_NE) {
            pb::ExprNode* node = compare_expr.mutable_nodes(0);
            node->set_col_type(pb::BOOL);
            node->set_node_type(pb::NOT_PREDICATE);
            node->set_num_children(1);
            pb::Function* item = node->mutable_fn();
            item->set_name("logic_not");
            item->set_fn_op(parser::FT_LOGIC_NOT);

            //add real predicate (OP_IS_NULL/OP_IN/OP_LIKE) node
            node = compare_expr.add_nodes();
            node->set_col_type(pb::BOOL);
            node->set_node_type(pb::IN_PREDICATE);
            pb::Function* func = node->mutable_fn();
            func->set_name("in");
            func->set_fn_op(parser::FT_IN);
            if (_cur_sub_ctx->expr_params.cmp_type != parser::CMP_ALL) {
                is_neq_any = true;
            }
            
            if (row_cnt > 0) {
                if (is_neq_any) {
                    if (run_memrow) {
                        first_val = subquery_exprs_vec[0][0];
                        for (uint32_t i = 1; i < subquery_exprs_vec.size(); i++) {
                            // = all返回多个不同值
                            if (first_val.compare(subquery_exprs_vec[i][0]) != 0) {
                                DB_WARNING("always_false");
                                always_false = true;
                            }
                        }
                    } else {
                        first_val = VectorizeHelpper::get_vectorized_value(vec_result_table->column(0).get(), 0);
                        for (uint32_t i = 1; i < vec_result_table->num_rows(); i++) {
                            // = all返回多个不同值
                            if (first_val.compare(VectorizeHelpper::get_vectorized_value(vec_result_table->column(0).get(), i)) != 0) {
                                DB_WARNING("always_false");
                                always_false = true;
                            }
                        }
                    }
                    node->set_num_children(2);
                } else {
                    node->set_num_children(subquery_exprs_vec.size() + 1);
                }
            } else {
                node->set_num_children(2);
            }
        } else if (_cur_sub_ctx->expr_params.func_type == parser::FT_GT
            || _cur_sub_ctx->expr_params.func_type == parser::FT_GE) {
            if (_cur_sub_ctx->expr_params.cmp_type == parser::CMP_ALL && row_cnt == 1) {
                bool is_null = false;
                if (run_memrow 
                        && subquery_exprs_vec[0][0].is_null()) {
                    is_null = true;
                }
                if (!run_memrow
                        && VectorizeHelpper::get_vectorized_value(vec_result_table->column(0).get(), 0).is_null()) {
                    is_null = true;
                }
                if (is_null) {
                    // > >= all (null) 恒为true
                    pb::ExprNode* node = expr.add_nodes();
                    node->set_node_type(pb::BOOL_LITERAL);
                    node->set_col_type(pb::BOOL);
                    node->mutable_derive_node()->set_bool_val(true);
                    return 0;
                }
            }
        }
        for (auto& old_node : left_expr.nodes()) {
            compare_expr.add_nodes()->CopyFrom(old_node);
        }
        if (row_cnt > 0 && !always_false) {
            if (is_neq_any || is_eq_all) {
                pb::ExprNode* node = compare_expr.add_nodes();
                construct_literal_expr(first_val, node);
            } else {
                if (run_memrow) {
                    for (auto& rows : subquery_exprs_vec) {
                        pb::ExprNode* node = compare_expr.add_nodes();
                        construct_literal_expr(rows[0], node);
                    }
                } else {
                    for (auto row_idx = 0; row_idx < vec_result_table->num_rows(); row_idx++) {
                        pb::ExprNode* node = compare_expr.add_nodes();
                        construct_literal_expr(VectorizeHelpper::get_vectorized_value(vec_result_table->column(0).get(), row_idx), node);
                    }
                }
            }
        } else if (row_cnt == 0 && sub_query_expr->cmp_type == parser::CMP_ALL) {
            compare_expr.clear_nodes();
            pb::ExprNode* true_node = compare_expr.add_nodes();
            true_node->set_node_type(pb::BOOL_LITERAL);
            true_node->set_col_type(pb::BOOL);
            true_node->mutable_derive_node()->set_bool_val(true);
        } else {
            compare_expr.clear_nodes();
            pb::ExprNode* false_node = compare_expr.add_nodes();
            false_node->set_node_type(pb::BOOL_LITERAL);
            false_node->set_col_type(pb::BOOL);
            false_node->mutable_derive_node()->set_bool_val(false);
        }
        for (auto& old_node : compare_expr.nodes()) {
            expr.add_nodes()->CopyFrom(old_node);
        }
    } else {
        // field op any (select x where conditions) <=> exists (select ... where conditions and (field op x))
        // field op all (select x where conditions) <=> not field anti-op any (select x where conditions)
        //                                          <=> not exists (select ... where conditions and (field anti-op x))
        _is_correlate_subquery_expr = true;

        CreateExprOptions tmp_options = options;
        pb::JoinType joinType = pb::NULL_JOIN;
        switch (sub_query_expr->cmp_type) {
            case parser::CMP_ALL:{
                static std::map<parser::FuncType, std::pair<parser::FuncType, std::string>> anti_op_map = {
                        {parser::FT_EQ, {parser::FT_NE, "ne"}},
                        {parser::FT_NE, {parser::FT_EQ, "eq"}},
                        {parser::FT_GT, {parser::FT_LE, "le"}},
                        {parser::FT_GE, {parser::FT_LT, "lt"}},
                        {parser::FT_LT, {parser::FT_GE, "ge"}},
                        {parser::FT_LE, {parser::FT_GT, "gt"}},
                };
                auto op = (parser::FuncType) func->fn_op();
                auto iter = anti_op_map.find(op);
                if (iter == anti_op_map.end()) {
                    DB_FATAL("Unexpected OP while handle_compare_subquery: %d", op);
                    return -1;
                }
                func->set_name(iter->second.second);
                func->set_fn_op(iter->second.first);
                joinType = pb::ANTI_SEMI_JOIN;
                break;
            }
            case parser::CMP_ANY:
            case parser::CMP_SOME:
                joinType = pb::SEMI_JOIN;
                break;
            default:
                break;
        }
        for (auto& old_node : compare_expr.nodes()) {
            expr.add_nodes()->CopyFrom(old_node);
        }
        for (auto& old_node : left_expr.nodes()) {
            expr.add_nodes()->CopyFrom(old_node);
        }
        ret = construct_apply_node(_cur_sub_ctx.get(), expr, joinType, tmp_options);
        if (ret < 0) {
            DB_WARNING("construct apply node failed");
            return -1;
        }
    }
    return 0;
}

int LogicalPlanner::handle_exists_subquery(const parser::ExprNode* expr_item, pb::Expr& expr,
        const CreateExprOptions& options) {
    parser::ExistsSubqueryExpr* sub_query_expr = (parser::ExistsSubqueryExpr*)expr_item;
    bool is_not = sub_query_expr->is_not;

    ExprParams expr_params;
    expr_params.is_expr_subquery = true;
    int ret = gen_subquery_plan(sub_query_expr->query_expr->query_stmt, _plan_table_ctx, expr_params);
    if (ret < 0) {
        DB_WARNING("gen subquery plan failed");
        return -1;
    }

    // 非相关子查询表达式，直接执行获取结果
    if (!_cur_sub_ctx->expr_params.is_correlated_subquery) {
        ret = exec_subquery_expr(_cur_sub_ctx, _ctx);
        SmartState state = _cur_sub_ctx->get_runtime_state();
        ON_SCOPE_EXIT(([this, state]() {
            // 列必须要在使用完结果才能close
            if (state->is_expr_subquery() && _cur_sub_ctx->root != nullptr 
                    && (!_cur_sub_ctx->is_explain || _cur_sub_ctx->explain_type != EXPLAIN_NULL)) {
                _cur_sub_ctx->root->close(state.get());
            }
        }));
        if (ret < 0) {
            return -1;
        }
        auto& subquery_exprs_vec = state->get_subquery_exprs();
        bool run_memrow = (state->execute_type == pb::EXEC_ROW);
        std::shared_ptr<arrow::Table> vec_result_table = state->subquery_result_table;
        if (!run_memrow && vec_result_table == nullptr) {
            DB_FATAL("run vectorized but subquery result table is null");
            return -1;
        }
        pb::ExprNode* node = expr.add_nodes();
        node->set_node_type(pb::BOOL_LITERAL);
        node->set_col_type(pb::BOOL);
        int64_t row_cnt = subquery_exprs_vec.size();
        if (!run_memrow) {
            row_cnt = vec_result_table->num_rows();
        }
        if (row_cnt > 0 && !is_not) {
            node->mutable_derive_node()->set_bool_val(true);
        } else if (row_cnt == 0 && is_not) {
            node->mutable_derive_node()->set_bool_val(true);
        } else {
            node->mutable_derive_node()->set_bool_val(false);
        }
    } else {
        _is_correlate_subquery_expr = true;
        pb::JoinType join_type = pb::SEMI_JOIN;
        if (is_not) {
            join_type = pb::ANTI_SEMI_JOIN;
        }
        ret = construct_apply_node(_cur_sub_ctx.get(), expr, join_type, options);
        if (ret < 0) {
            DB_WARNING("construct apply node failed");
            return -1;
        }
    }
    return 0;
}

int LogicalPlanner::handle_common_subquery(const parser::ExprNode* expr_item,
            pb::Expr& expr,
            const CreateExprOptions& options) {
    int ret = create_common_subquery_expr((parser::SubqueryExpr*)expr_item, expr, options, _is_correlate_subquery_expr);
    if (ret < 0) {
        return -1;
    }
    if (_is_correlate_subquery_expr && options.is_select_field) {
        ret = construct_apply_node(_cur_sub_ctx.get(), expr, pb::LEFT_JOIN, options);
        if (ret < 0) {
            DB_WARNING("construct apply node failed");
            return -1;
        }
    } else if (_is_correlate_subquery_expr) {
        DB_WARNING("not support");
        return -1;
    }
    _is_correlate_subquery_expr = false;
    return 0;
}

int LogicalPlanner::create_expr_tree(const parser::Node* item, pb::Expr& expr, const CreateExprOptions& options) {
    if (item == nullptr) {
        DB_FATAL("item is null");
        return -1;
    }
    if (item->node_type != parser::NT_EXPR) {
        DB_FATAL("type error:%d", item->node_type);
        return -1;
    }
    const parser::ExprNode* expr_item = (const parser::ExprNode*)item;
    if (expr_item->expr_type == parser::ET_LITETAL) {
        if (0 != create_term_literal_node((parser::LiteralExpr*)expr_item, expr, CreateExprOptions())) {
            if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                _ctx->stat_info.error_code = ER_BAD_FIELD_ERROR;
                _ctx->stat_info.error_msg << "Invalid literal '" << expr_item->to_string() << "'";
            }
            DB_WARNING("create_term_literal_node failed");
            return -1;
        }
    } else if (expr_item->expr_type == parser::ET_COLUMN) {
        int ret = -1;
        if (options.use_alias) {
            // TODO：目前都是别名优先，后续需要关注mysql别名和列名的优先级
            ret = create_alias_node(static_cast<const parser::ColumnName*>(expr_item), expr, options.can_agg);
        }
        if (ret == -2) {
            if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                _ctx->stat_info.error_code = ER_NON_UNIQ_ERROR;
                _ctx->stat_info.error_msg << "Column \'" << expr_item->to_string() << "\' is ambiguous";
            }
            return -1;
        } else if (ret == -1 || ret == -3) {
            if (0 != create_term_slot_ref_node(static_cast<const parser::ColumnName*>(expr_item), expr, options)) {
                if (ret == -1 && _ctx->stat_info.error_code == ER_ERROR_FIRST) {
                    _ctx->stat_info.error_code = ER_BAD_FIELD_ERROR;
                    _ctx->stat_info.error_msg << "Unknown column \'" << expr_item->to_string() << "\'";
                }
                if (ret == -3) {
                    _ctx->stat_info.error_msg.str("");
                    _ctx->stat_info.error_code = ER_INVALID_GROUP_FUNC_USE;
                    _ctx->stat_info.error_msg << "Invalid use of group function";
                }
                return ret;
            }
        }
    } else if (expr_item->expr_type == parser::ET_ROW_EXPR) {
        if (create_row_expr_node(static_cast<const parser::RowExpr*>(expr_item), expr, options) != 0) {
            DB_WARNING("create_row_expr_node failed");
            return -1;
        }
    } else if (expr_item->expr_type == parser::ET_FUNC) {
        parser::FuncExpr* func = (parser::FuncExpr*)expr_item;
        switch (func->func_type) {
            case parser::FT_LOGIC_NOT:
                return create_n_ary_predicate(func, expr, pb::NOT_PREDICATE, options);
            case parser::FT_LOGIC_AND:
                return create_n_ary_predicate(func, expr, pb::AND_PREDICATE, options);
            case parser::FT_LOGIC_OR:
                return create_n_ary_predicate(func, expr, pb::OR_PREDICATE, options);
            case parser::FT_LOGIC_XOR:
                return create_n_ary_predicate(func, expr, pb::XOR_PREDICATE, options);
            case parser::FT_IS_NULL:
            case parser::FT_IS_UNKNOWN:
                return create_n_ary_predicate(func, expr, pb::IS_NULL_PREDICATE, options);
            case parser::FT_IS_TRUE:
                return create_n_ary_predicate(func, expr, pb::IS_TRUE_PREDICATE, options);
            case parser::FT_IN:
                return create_in_predicate(func, expr, options);
            case parser::FT_LIKE:
            case parser::FT_EXACT_LIKE:
                return create_n_ary_predicate(func, expr, pb::LIKE_PREDICATE, options);
            case parser::FT_REGEXP:
                return create_n_ary_predicate(func, expr, pb::REGEXP_PREDICATE, options);
            case parser::FT_BETWEEN:
                return create_between_expr(func, expr, options);
            case parser::FT_AGG:
                if (!options.can_agg) {
                    if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                        _ctx->stat_info.error_code = ER_INVALID_GROUP_FUNC_USE;
                        _ctx->stat_info.error_msg << "Invalid use of group function";
                    }
                    DB_WARNING("Invalid use of group function");
                    return -1;
                }
                return create_agg_expr(func, expr, options);
            case parser::FT_VALUES:
                return create_values_expr(func, expr);
            // same as scala func
            case parser::FT_BIT_NOT:
            case parser::FT_UMINUS:
            case parser::FT_ADD:
            case parser::FT_MINUS:
            case parser::FT_MULTIPLIES:
            case parser::FT_DIVIDES:
            case parser::FT_MOD:
            case parser::FT_LS:
            case parser::FT_RS:
            case parser::FT_BIT_AND:
            case parser::FT_BIT_OR:
            case parser::FT_BIT_XOR:
            case parser::FT_MATCH_AGAINST:
            case parser::FT_COMMON:
                return create_scala_func_expr(func, expr, func->func_type, options);
            case parser::FT_EQ:
            case parser::FT_NE:
            case parser::FT_GT:
            case parser::FT_GE:
            case parser::FT_LT:
            case parser::FT_LE: {
                CreateExprOptions tmp_options = options;
                tmp_options.max_one_row = true;
                return create_scala_func_expr(func, expr, func->func_type, tmp_options);
            }
            // todo:support
            default:
                DB_WARNING("un-supported func_type:%d fn_name:%s", 
                        func->func_type, func->fn_name.c_str());
                return -1;
        }
    } else if (expr_item->expr_type == parser::ET_SUB_QUERY_EXPR) {
        return handle_common_subquery(expr_item, expr, options);
    } else if (expr_item->expr_type == parser::ET_CMP_SUB_QUERY_EXPR) {
        return handle_compare_subquery(expr_item, expr, options);
    } else if (expr_item->expr_type == parser::ET_EXISTS_SUB_QUERY_EXPR) {
        return handle_exists_subquery(expr_item, expr, options);
    } else if (expr_item->expr_type == parser::ET_WINDOW) {
        if (!options.can_window) {
            if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                _ctx->stat_info.error_code = ER_NOT_SUPPORTED_YET;
                _ctx->stat_info.error_msg << "Only support window function in select field";
            }
            DB_WARNING("Only support window function in select field");
            return -1;
        }
        return create_window_expr(static_cast<const parser::WindowFuncExpr*>(expr_item), expr, options);
    } else {
        DB_WARNING("un-supported expr_type: %d", expr_item->expr_type);
        return -1;
    }
    return 0;
}

std::string LogicalPlanner::get_field_alias_name(const parser::ColumnName* column) {
    std::string alias_name;
    if (!column->db.empty()) {
        alias_name +=column->db.c_str();
        alias_name += ".";
        alias_name +=column->table.c_str();
        return alias_name;
    } else if (!column->table.empty()) {
        //table.field_name
        auto dbs = get_possible_databases(column->table.c_str());
        if (dbs.size() == 0) {
            if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                _ctx->stat_info.error_code = ER_WRONG_TABLE_NAME;
                _ctx->stat_info.error_msg << "Incorrect table name \'" << column->to_string() << "\'";
            }
            DB_WARNING("no database found for field: %s", column->to_string().c_str());
            return "";
        } else if (dbs.size() > 1) {
            // _current_tables是同层join表, 内外层同名表以内表为准，支持下面的
            // select *,(select a.area_id from (select area_id from dim_public_area_basic_df) as a where area_id=3 limit 1) from  (select area_id from dim_public_area_basic_df) as  a limit 1;
            auto tables = get_possible_tables(column->name.c_str());
            if (tables.size() == 1) {
                alias_name += *tables.begin();
                return alias_name;
            }
            std::vector<std::string> meet_tables;
            meet_tables.reserve(2);
            for (const auto& db : dbs) {
                auto t = db + "." + column->table.c_str();
                if (std::find(_current_tables.cbegin(), _current_tables.cend(), t) != _current_tables.cend()) {
                    meet_tables.emplace_back(t);
                }
            }
            if (meet_tables.size() == 1) {
                return meet_tables[0];
            }
            if (!FLAGS_disambiguate_select_name) {
                if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                    _ctx->stat_info.error_code = ER_AMBIGUOUS_FIELD_TERM;
                    _ctx->stat_info.error_msg << "column  \'" << column->name << "\' is ambiguous";
                }
                DB_WARNING("ambiguous field_name: %s", column->to_string().c_str());
                return "";
            }
        }
        alias_name += *dbs.begin();
        alias_name += ".";
        alias_name += column->table.c_str();
        return alias_name;
    } else if (!column->name.empty()) {
        //field_name
        auto tables = get_possible_tables(column->name.c_str());
        if (tables.size() == 0) {
            if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                _ctx->stat_info.error_code = ER_WRONG_COLUMN_NAME;
                _ctx->stat_info.error_msg << "Incorrect column name \'" << column->to_string() << "\'";
            }
            DB_WARNING("no table found for field: %s", column->to_string().c_str());
            return "";
        } else if (tables.size() > 1) {
            // select id from t1 as tt1 join (select id from t2) as tt2 where tt1.id=tt2.id;
            std::vector<std::string> meet_tables;
            meet_tables.reserve(2);
            for (auto& t : tables) {
                if (std::find(_current_tables.cbegin(), _current_tables.cend(), t) != _current_tables.cend()) {
                    meet_tables.emplace_back(t);
                }
            }
            if (meet_tables.size() == 1) {
                return meet_tables[0];
            }
            if (!FLAGS_disambiguate_select_name) {
                for (auto& t : meet_tables) {
                    DB_WARNING("ambiguous field_name: %s t:%s", column->to_string().c_str(),
                        t.c_str());
                }
                if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                    _ctx->stat_info.error_code = ER_AMBIGUOUS_FIELD_TERM;
                    _ctx->stat_info.error_msg << "column \'" << column->name << "\' is ambiguous";
                }
                return "";
            }
        }
        alias_name += *tables.begin();
        return alias_name;
    } else {
        DB_FATAL("column.name is null");
        return "";
    }
    return alias_name;
}

ScanTupleInfo* LogicalPlanner::get_scan_tuple(const std::string& table_name, int64_t table_id) {
    ScanTupleInfo* tuple_info = nullptr;
    auto iter = _plan_table_ctx->table_tuple_mapping.find(try_to_lower(table_name));
    if (iter != _plan_table_ctx->table_tuple_mapping.end()) {
        tuple_info = &(iter->second);
    } else {
        tuple_info = &(_plan_table_ctx->table_tuple_mapping[try_to_lower(table_name)]);
        tuple_info->tuple_id = _unique_id_ctx->tuple_cnt++;
        tuple_info->table_id = table_id;
        tuple_info->slot_cnt = 1;
    }
    //DB_WARNING("table_name:%s tuple_id:%d table_id:%ld", table_name.c_str(), tuple_info->tuple_id, table_id);
    return tuple_info;
}

pb::SlotDescriptor& LogicalPlanner::get_scan_ref_slot(
        const std::string& alias_name, int64_t table,
        int32_t field, pb::PrimitiveType type) {
    ScanTupleInfo* tuple_info = get_scan_tuple(alias_name, table);
    _ctx->current_tuple_ids.emplace(tuple_info->tuple_id);
    pb::SlotDescriptor *slot_desc = nullptr;

    auto& inner_map = tuple_info->field_slot_mapping;
    auto inner_iter = inner_map.find(field);
    if (inner_iter != inner_map.end()) {
        slot_desc = &(inner_iter->second);
        slot_desc->set_ref_cnt(slot_desc->ref_cnt() + 1);
    } else {
        slot_desc = &inner_map[field];
        slot_desc->set_slot_id(tuple_info->slot_cnt++);
        slot_desc->set_slot_type(type);
        slot_desc->set_tuple_id(tuple_info->tuple_id);
        slot_desc->set_field_id(field);
        slot_desc->set_table_id(table);
        slot_desc->set_ref_cnt(1);
    }
    return *slot_desc;
}

pb::SlotDescriptor& LogicalPlanner::get_values_ref_slot(int64_t table,
        int32_t field, pb::PrimitiveType type) {
    auto& tuple_info = _values_tuple_info;
    if (tuple_info.tuple_id == -1) {
        tuple_info.tuple_id = _unique_id_ctx->tuple_cnt++;
        tuple_info.table_id = table;
        tuple_info.slot_cnt = 1;
    }
    tuple_info.table_id = table;
    pb::SlotDescriptor *slot_desc = nullptr;

    auto& inner_map = tuple_info.field_slot_mapping;
    auto inner_iter = inner_map.find(field);
    if (inner_iter != inner_map.end()) {
        slot_desc = &(inner_iter->second);
    } else {
        slot_desc = &inner_map[field];
        slot_desc->set_slot_id(tuple_info.slot_cnt++);
        slot_desc->set_slot_type(type);
        slot_desc->set_tuple_id(tuple_info.tuple_id);
        slot_desc->set_field_id(field);
        slot_desc->set_table_id(table);
    }
    return *slot_desc;
}

// -2 means alias name ambiguous
int LogicalPlanner::create_alias_node(const parser::ColumnName* column, pb::Expr& expr, bool can_agg) {
    if (!column->db.empty()) {
        DB_WARNING("alias no db");
        return -1;
    }
    if (!column->table.empty()) {
        //DB_WARNING("alias no table");
        return -1;
    }
    std::string lower_name = column->name.to_lower();
    int match_count = _select_alias_mapping.count(lower_name);
    if (match_count > 1 && !FLAGS_disambiguate_select_name) {
        DB_WARNING("column name: %s is ambiguous", column->name.c_str());
        return -2;
    } else if (match_count == 0) {
        //DB_WARNING("invalid column name: %s", column->name.c_str());
        return -1;
    } else {
        auto iter = _select_alias_mapping.find(lower_name);
        if (iter != _select_alias_mapping.end() && iter->second >= _select_exprs.size()) {
            //DB_WARNING("invalid column name: %s", column->name.c_str());
            return -1;
        }
        if (!can_agg) {
            // group by内的别名不允许包含agg函数
            const auto& sub_exprs = _select_exprs[iter->second].nodes();
            for (const auto & sub_expr : sub_exprs) {
                if (sub_expr.node_type() == pb::AGG_EXPR) {
                    return -3;
                }
            }
        }
        expr.MergeFrom(_select_exprs[iter->second]);
    }
    return 0;
}

//TODO: error_code
int LogicalPlanner::create_term_slot_ref_node(
        const parser::ColumnName* col_expr, 
        pb::Expr& expr,
        const CreateExprOptions& options) {
    std::stringstream ss;
    col_expr->to_stream(ss);
    std::string origin_name = ss.str();
    if (options.partition_expr) {
        pb::ExprNode* node = expr.add_nodes();
        node->set_node_type(pb::SLOT_REF);
        node->set_col_type(pb::NULL_TYPE);
        node->set_num_children(0);
        node->mutable_derive_node()->set_field_name(origin_name);
        return 0;
    }
    std::unordered_map<std::string, pb::ExprNode>* vars = nullptr;
    std::string var_name;
    auto client = _ctx->client_conn;
    if (boost::algorithm::istarts_with(origin_name, "@@global.")) {
//        // TODO handle set global variable
//        return -1;
        var_name = origin_name.substr(strlen("@@global."));
        vars = &client->session_vars;
    } else if (boost::algorithm::istarts_with(origin_name, "@@session.")) {
        var_name = origin_name.substr(strlen("@@session."));
        vars = &client->session_vars;
    } else if (boost::algorithm::istarts_with(origin_name, "@@local.")) {
        var_name = origin_name.substr(strlen("@@local."));
        vars = &client->session_vars;
    } else if (boost::algorithm::istarts_with(origin_name, "@@")) {
        var_name = origin_name.substr(2);
        vars = &client->session_vars;
    } else if (boost::algorithm::istarts_with(origin_name, "@")) {
        // user variable term
        var_name = origin_name.substr(1);
        vars = &client->user_vars;
    }
    if (vars != nullptr) {
        auto iter = vars->find(var_name);
        pb::ExprNode* node = expr.add_nodes();
        if (iter != vars->end()) {
            node->CopyFrom(iter->second);
        } else {
            node->set_num_children(0);
            node->set_node_type(pb::NULL_LITERAL);
            node->set_col_type(pb::NULL_TYPE);
        }
        return 0;
    }
    std::string alias_name = get_field_alias_name(col_expr);
    if (alias_name.empty()) {
        DB_WARNING("get_field_alias_name failed: %s", col_expr->to_string().c_str());
        return -1;
    }
    std::string full_name = alias_name;
    full_name += ".";
    full_name += col_expr->name.to_lower();
    FieldInfo* field_info = nullptr;
    if (nullptr == (field_info = get_field_info_ptr(full_name))) {
        //_ctx->set_error_code(-1);
        //_ctx->set_error_msg(std::string("invalid field name in: ").append(term));
        DB_WARNING("invalid field name: %s full_name: %s", col_expr->to_string().c_str(), full_name.c_str());
        return -1;
    }
    pb::SlotDescriptor slot;
    if (options.is_values) {
        slot = get_values_ref_slot(field_info->table_id, field_info->id, field_info->type);
    } else {
        slot = get_scan_ref_slot(alias_name, field_info->table_id, field_info->id, field_info->type);
    }

    pb::ExprNode* node = expr.add_nodes();
    auto table_info_ptr = SchemaFactory::get_instance()->get_table_info_ptr(field_info->table_id);
    if (table_info_ptr != nullptr) {
        expr.set_database(table_info_ptr->name.substr(0, table_info_ptr->name.find(".")));
        expr.set_table(table_info_ptr->short_name);
    }
    node->set_node_type(pb::SLOT_REF);
    node->set_col_type(field_info->type);
    node->set_num_children(0);
    node->mutable_derive_node()->set_tuple_id(slot.tuple_id()); //TODO
    node->mutable_derive_node()->set_slot_id(slot.slot_id());
    node->mutable_derive_node()->set_field_id(slot.field_id());
    node->set_col_flag(field_info->flag);
    _ctx->ref_slot_id_mapping[slot.tuple_id()][col_expr->name.to_lower()] = slot.slot_id();
    return 0;
}

//TODO: primitive len for STRING, BOOL and NULL
int LogicalPlanner::create_term_literal_node(const parser::LiteralExpr* literal, pb::Expr& expr, const CreateExprOptions& options) {
    pb::ExprNode* node = nullptr;
    if (options.is_plan_cache) {
        if (expr.nodes_size() == 0) {
            DB_WARNING("expr has no node");
            return -1;
        }
        node = expr.mutable_nodes(0);
    } else {
        node = expr.add_nodes();
        node->set_num_children(0);
    }
    if (enable_plan_cache() && literal->placeholder_literal_type == parser::LT_PLACE_HOLDER) {
        node->mutable_derive_node()->set_placeholder_id(literal->placeholder_id);
    }
    switch (literal->literal_type) {
        case parser::LT_INT:
            node->set_node_type(pb::INT_LITERAL);
            node->set_col_type(pb::INT64);
            node->mutable_derive_node()->set_int_val(literal->_u.int64_val);
            break;
        case parser::LT_DOUBLE:
            node->set_node_type(pb::DOUBLE_LITERAL);
            node->set_col_type(pb::DOUBLE);
            node->mutable_derive_node()->set_double_val(literal->_u.double_val);
            break;
        case parser::LT_STRING: 
            node->set_node_type(pb::STRING_LITERAL);
            node->set_col_type(pb::STRING);
            node->mutable_derive_node()->set_string_val(literal->_u.str_val.to_string());
            break;
        case parser::LT_HEX: 
            node->set_node_type(pb::HEX_LITERAL);
            node->set_col_type(pb::HEX);
            node->mutable_derive_node()->set_string_val(literal->_u.str_val.to_string());
            break;
        case parser::LT_BOOL:
            node->set_node_type(pb::BOOL_LITERAL);
            node->set_col_type(pb::BOOL);
            node->mutable_derive_node()->set_bool_val(literal->_u.bool_val);
            break;
        case parser::LT_NULL:
            node->set_node_type(pb::NULL_LITERAL);
            node->set_col_type(pb::NULL_TYPE);
            break;
        case parser::LT_PLACE_HOLDER:
            node->set_node_type(pb::PLACE_HOLDER_LITERAL);
            node->set_col_type(pb::PLACE_HOLDER);
            node->mutable_derive_node()->set_placeholder_id(literal->placeholder_id);
            break;
        case parser::LT_MAXVALUE:
            node->set_node_type(pb::MAXVALUE_LITERAL);
            node->set_col_type(pb::MAXVALUE_TYPE);
            break;
        default:
            DB_WARNING("create_term_literal_node failed: %d", literal->literal_type);
            return -1;
    }

    if (_ctx != nullptr && _ctx->need_convert_charset && literal->literal_type == parser::LT_STRING) {
        std::string convert_str;
        if (convert_charset(_ctx->charset, node->derive_node().string_val(),
                            _ctx->table_charset, convert_str) != 0) {
            DB_FATAL("Fail to convert_charset, connection_charset: %d, table_charset: %d, value: %s",
                      _ctx->charset, _ctx->table_charset, node->derive_node().string_val().c_str());
            return -1;
        }
        node->mutable_derive_node()->set_string_val(convert_str);
    }

    return 0;
}

int LogicalPlanner::create_row_expr_node(const parser::RowExpr* item, pb::Expr& expr, const CreateExprOptions& options) {
    pb::ExprNode* node = expr.add_nodes();
    node->set_col_type(pb::INVALID_TYPE);
    node->set_node_type(pb::ROW_EXPR);
    for (int32_t idx = 0; idx < item->children.size(); ++idx) {
        if (0 != create_expr_tree(item->children[idx], expr, options)) {
            DB_WARNING("create child expr failed");
            return -1;
        }
    }
    node->set_num_children(item->children.size());
    return 0;
}

int LogicalPlanner::create_orderby_exprs(parser::OrderByClause* order) {
    return create_orderby_exprs(
                order->items, _order_tuple_id, _order_slot_cnt, _order_slots, _order_exprs, _order_ascs);
}

int LogicalPlanner::create_orderby_exprs(parser::Vector<parser::ByItem*>& order_items, 
                                         int32_t& order_tuple_id, 
                                         int32_t& order_slot_cnt,
                                         std::vector<pb::SlotDescriptor>& order_slots,
                                         std::vector<pb::Expr>& order_exprs, 
                                         std::vector<bool>& order_ascs) {
    CreateExprOptions options;
    options.use_alias = true;
    options.can_agg = true;
    for (int idx = 0; idx < order_items.size(); ++idx) {
        bool is_asc = !order_items[idx]->is_desc;
        // create order by expr node
        pb::Expr order_expr;
        if (0 != create_expr_tree(order_items[idx]->expr, order_expr, options)) {
            DB_WARNING("create group expr failed");
            return -1;
        }
#ifdef BAIDU_INTERNAL
        const pb::ExprNode& node = order_expr.nodes(0);
        //如果此处是字符串常量(如'userid')，则看做是slot_ref, 按照对应的列名排序
        if (node.node_type() == pb::STRING_LITERAL) {
            parser::ColumnName tmp_col;
            std::string tmp_str = node.derive_node().string_val();
            tmp_col.name = tmp_str.c_str();
            order_expr.Clear();
            if (create_term_slot_ref_node(&tmp_col, order_expr, options) != 0) {
                DB_WARNING("create group term expr failed");
                return -1;
            }
        } else {
            if (node.node_type() != pb::SLOT_REF && node.node_type() != pb::AGG_EXPR) {
                //DB_WARNING("un-supported order-by node type: %d", node.node_type());
                //return -1;
                create_order_func_slot(order_tuple_id, order_slot_cnt, order_slots);
            }
        } 
#else
        const pb::ExprNode& node = order_expr.nodes(0);
        if (node.node_type() != pb::SLOT_REF && node.node_type() != pb::AGG_EXPR) {
            //DB_WARNING("un-supported order-by node type: %d", node.node_type());
            //return -1;
            create_order_func_slot(order_tuple_id, order_slot_cnt, order_slots);
        }
#endif
        order_exprs.push_back(order_expr);
        order_ascs.push_back(is_asc);
    }
    return 0;
}

void LogicalPlanner::create_scan_tuple_descs() {
    for (auto& pair : _plan_table_ctx->table_tuple_mapping) {
        auto& tuple_info = pair.second;
        int64_t tableid = tuple_info.table_id;
        auto& slot_map = tuple_info.field_slot_mapping;
        if (_ctx->current_tuple_ids.count(tuple_info.tuple_id) == 0) {
            continue;
        }
        pb::TupleDescriptor tuple_desc;
        tuple_desc.set_tuple_id(tuple_info.tuple_id);
        tuple_desc.set_table_id(tableid);

        // slot_id => slot desc mapping
        std::map<int32_t, pb::SlotDescriptor> id_slot_mapping;
        //reorder the slot descriptors by slot id
        for (auto& kv : slot_map) {
            const pb::SlotDescriptor& desc = kv.second;
            id_slot_mapping.insert(std::make_pair(desc.slot_id(), desc));
        }
        for (auto& kv : id_slot_mapping) {
            const pb::SlotDescriptor& desc = kv.second;
            pb::SlotDescriptor* slot = tuple_desc.add_slots();
            slot->CopyFrom(desc);
            // 只对表的Tuple的slot_idxes赋值，对于agg/window/order/values tuple不赋值slot_idxes
            // 后续无效列裁剪如果升级处理Agg/Window表达式，需要对相应tuple的slot_idxes进行赋值
            tuple_desc.add_slot_idxes(slot->slot_id() - 1);
        }
        _scan_tuples.emplace_back(tuple_desc);
        _ctx->add_tuple(tuple_desc);
    }
}

void LogicalPlanner::create_values_tuple_desc() {
    if (_values_tuple_info.tuple_id == -1) {
        return;
    }
    _values_tuple.set_tuple_id(_values_tuple_info.tuple_id);
    _values_tuple.set_table_id(_values_tuple_info.table_id);

    // slot_id => slot desc mapping
    std::map<int32_t, pb::SlotDescriptor> id_slot_mapping;
    auto& slot_map = _values_tuple_info.field_slot_mapping;
    //reorder the slot descriptors by slot id
    for (auto& kv : slot_map) {
        const pb::SlotDescriptor& desc = kv.second;
        id_slot_mapping.insert(std::make_pair(desc.slot_id(), desc));
    }
    for (auto& kv : id_slot_mapping) {
        const pb::SlotDescriptor& desc = kv.second;
        pb::SlotDescriptor* slot = _values_tuple.add_slots();
        slot->CopyFrom(desc);
    }
    _ctx->add_tuple(_values_tuple);
}

void LogicalPlanner::create_order_by_tuple_desc() {
    if (_order_tuple_id == -1) {
        return;
    }
    pb::TupleDescriptor order_tuple;
    order_tuple.set_tuple_id(_order_tuple_id);
    for (auto& slot : _order_slots) {
        *order_tuple.add_slots() = slot;
    }
    _ctx->add_tuple(order_tuple);
}

int LogicalPlanner::create_packet_node(pb::OpType op_type) {
    pb::PlanNode* pack_node = _ctx->add_plan_node();
    pack_node->set_node_type(pb::PACKET_NODE);
    pack_node->set_limit(-1);
    pack_node->set_is_explain(_ctx->is_explain);
    pack_node->set_num_children(1); //TODO 
    pb::DerivePlanNode* derive = pack_node->mutable_derive_node();
    pb::PacketNode* pack = derive->mutable_packet_node();

    pack->set_op_type(op_type);
    if (op_type != pb::OP_SELECT) {
        return 0;
    }
    for (auto& expr : _select_exprs) {
        auto proj = pack->add_projections();
        proj->CopyFrom(expr);
    }
    for (auto& name : _select_names) {
        pack->add_col_names(name);
    }
    return 0;
}

int LogicalPlanner::create_filter_node(std::vector<pb::Expr>& filters, 
        pb::PlanNodeType type) {
    if (filters.size() == 0) {
        return 0;
    }
    pb::PlanNode* where_node = _ctx->add_plan_node();
    where_node->set_node_type(type);
    where_node->set_limit(-1);
    where_node->set_is_explain(_ctx->is_explain);
    where_node->set_num_children(1); //TODO 
    pb::DerivePlanNode* derive = where_node->mutable_derive_node();
    pb::FilterNode* filter = derive->mutable_raw_filter_node();
    
    for (uint32_t idx = 0; idx < filters.size(); ++idx) {
        pb::Expr* expr = filter->add_conjuncts();
        expr->CopyFrom(filters[idx]);
    }
    //DB_NOTICE("create_filter_node:%s", where_node->ShortDebugString().c_str());
    return 0;
}

int LogicalPlanner::create_sort_node() {
    if (_order_exprs.size() == 0) {
        return 0;
    }
    pb::PlanNode* sort_node = _ctx->add_plan_node();
    sort_node->set_node_type(pb::SORT_NODE);
    sort_node->set_limit(-1);
    sort_node->set_is_explain(_ctx->is_explain);
    sort_node->set_num_children(1); //TODO
    pb::DerivePlanNode* derive = sort_node->mutable_derive_node();
    pb::SortNode* sort = derive->mutable_sort_node();
    
    if (_order_exprs.size() != _order_ascs.size()) {
        DB_WARNING("order expr format error");
        return -1;
    }
    for (uint32_t idx = 0; idx < _order_exprs.size(); ++idx) {
        pb::Expr* order_expr = sort->add_order_exprs();
        pb::Expr* slot_order_expr = sort->add_slot_order_exprs();
        order_expr->CopyFrom(_order_exprs[idx]);
        slot_order_expr->CopyFrom(_order_exprs[idx]);
        sort->add_is_asc(_order_ascs[idx]);
        sort->add_is_null_first(_order_ascs[idx]);
    }
    sort->set_tuple_id(_order_tuple_id);

    return 0;
}
int LogicalPlanner::create_join_and_scan_nodes(JoinMemTmp* join_root, ApplyMemTmp* apply_root) {
    if (join_root == nullptr) {
        DB_WARNING("join root is null");
        return -1;
    }
    // apply_root存在则join_root是apply_root的左节点
    if (apply_root != nullptr) {
        pb::PlanNode* apply_node = _ctx->add_plan_node();
        apply_node->set_node_type(pb::APPLY_NODE);
        apply_node->set_limit(-1);
        apply_node->set_is_explain(_ctx->is_explain);
        apply_node->set_num_children(2);
        pb::DerivePlanNode* derive = apply_node->mutable_derive_node();
        pb::ApplyNode* join = derive->mutable_apply_node();
        *join = apply_root->apply_node;
        if (0 != create_join_and_scan_nodes(apply_root->outer_node, nullptr)) {
            DB_WARNING("create left child when join node fail");
            return -1;
        }
        for (int i = 0; i < apply_root->inner_plan.nodes_size(); i++) {
            pb::PlanNode* node = _ctx->add_plan_node();
            node->CopyFrom(apply_root->inner_plan.nodes(i));
        }
        return 0;
    }

    if (join_root->is_derived_table) {
        pb::PlanNode* scan_node = _ctx->add_plan_node();
        scan_node->set_node_type(pb::DUAL_SCAN_NODE);
        scan_node->set_limit(-1);
        scan_node->set_is_explain(_ctx->is_explain);
        scan_node->set_is_get_keypoint(_ctx->is_get_keypoint);
        scan_node->set_num_children(0);
        pb::DerivePlanNode* derive = scan_node->mutable_derive_node();
        pb::ScanNode* scan = derive->mutable_scan_node();
        scan->set_tuple_id(join_root->join_node.left_tuple_ids(0));
        scan->set_table_id(join_root->join_node.left_table_ids(0));
        return 0;
    }
    //叶子节点
    if (join_root->join_node.join_type() == pb::NULL_JOIN) {
        pb::PlanNode* scan_node = _ctx->add_plan_node();
        scan_node->set_node_type(pb::SCAN_NODE);
        scan_node->set_limit(-1);
        scan_node->set_is_explain(_ctx->is_explain);
        scan_node->set_is_get_keypoint(_ctx->is_get_keypoint);
        scan_node->set_num_children(0);
        pb::DerivePlanNode* derive = scan_node->mutable_derive_node();
        pb::ScanNode* scan = derive->mutable_scan_node();
        scan->set_tuple_id(join_root->join_node.left_tuple_ids(0));
        scan->set_table_id(join_root->join_node.left_table_ids(0));
        scan->set_engine(_factory->get_table_engine(scan->table_id()));
        //DB_WARNING("get_table_engine :%d", scan->engine());
        if (!_ctx->is_explain || _ctx->explain_hint == nullptr
            || !_ctx->explain_hint->get_flag<ExplainHint::HintType::NO_USE>()) {
            for (auto index_id : join_root->use_indexes) {
                scan->add_use_indexes(index_id);
            }
        }
        if (!_ctx->is_explain || _ctx->explain_hint == nullptr
                || !_ctx->explain_hint->get_flag<ExplainHint::HintType::NO_FORCE>()) {
            for (auto index_id : join_root->force_indexes) {
                scan->add_force_indexes(index_id);
            }
        }
        if (!_ctx->is_explain || _ctx->explain_hint == nullptr
                || !_ctx->explain_hint->get_flag<ExplainHint::HintType::NO_IGNORE>()) {
            for (auto index_id : join_root->ignore_indexes) {
                scan->add_ignore_indexes(index_id);
            }
        }
        if (_ctx->select_for_update) {
            // 仅加主表行锁
            scan->set_lock(pb::LOCK_GET_ONLY_PRIMARY);
        }
        return 0;
    }
    //如果不是根节点必须是左右孩子都有
    if (join_root->left_node == nullptr || join_root->right_node == nullptr) {
        DB_WARNING("create join node fail");
        return -1;
    }
    pb::PlanNode* join_node = _ctx->add_plan_node();
    join_node->set_node_type(pb::JOIN_NODE);
    join_node->set_limit(-1);
    join_node->set_is_explain(_ctx->is_explain);
    join_node->set_num_children(2);
    pb::DerivePlanNode* derive = join_node->mutable_derive_node();
    pb::JoinNode* join = derive->mutable_join_node();
    *join = join_root->join_node;
    if (0 != create_join_and_scan_nodes(join_root->left_node, nullptr)) {
        DB_WARNING("create left child when join node fail");
        return -1;
    }
    if (0 != create_join_and_scan_nodes(join_root->right_node, nullptr)) {
        DB_WARNING("create right child when join node fail");
        return -1;
    }
    return 0;
}
int LogicalPlanner::create_scan_nodes() {
    for (auto& tuple_desc : _scan_tuples) {
        pb::PlanNode* scan_node = _ctx->add_plan_node();
        scan_node->set_node_type(pb::SCAN_NODE);
        scan_node->set_limit(-1);
        scan_node->set_is_explain(_ctx->is_explain);
        scan_node->set_is_get_keypoint(_ctx->is_get_keypoint);
        scan_node->set_num_children(0); //TODO 
        pb::DerivePlanNode* derive = scan_node->mutable_derive_node();
        pb::ScanNode* scan = derive->mutable_scan_node();
        scan->set_tuple_id(tuple_desc.tuple_id());
        scan->set_table_id(tuple_desc.table_id());
        scan->set_engine(_factory->get_table_engine(scan->table_id()));
        //DB_WARNING("get_table_engine :%d", scan->engine());
    }
    return 0;
}

int LogicalPlanner::need_dml_txn(const int64_t table_id) {
    if (_ctx == nullptr || _factory == nullptr) {
        DB_FATAL("_ctx or _factory is nullptr");
        return -1;
    }
    if (_ctx->enable_2pc ||
            _factory->need_begin_txn(table_id) ||
            (!_ctx->no_binlog && _factory->has_open_binlog(table_id))) {
        return 0;
    } else {
        return -1;
    }
}

void LogicalPlanner::set_dml_txn_state(int64_t table_id) {
    if (_ctx->is_explain) {
        return;
    }
    auto client = _ctx->client_conn;
    //is_gloabl_ddl 打开时，该连接处理全局二级索引增量数据，不需要处理binlog。
    if (!_ctx->no_binlog && _factory->has_open_binlog(table_id) && !client->is_index_ddl) {
        client->open_binlog = true;
        _ctx->open_binlog = true;
    }
    if (_ctx->is_prepared) {
        return;
    }
    if (client->txn_id == 0) {
        DB_DEBUG("enable_2pc %d global index %d, binlog %d", 
            _ctx->enable_2pc, _factory->has_global_index(table_id), _factory->has_open_binlog(table_id));
        if (need_dml_txn(table_id) == 0) {
            client->on_begin();
            DB_DEBUG("get txn %ld", client->txn_id);
            client->seq_id = 0;
        } else {
            client->txn_id = 0;
            client->seq_id = 0;
        }
        //DB_WARNING("DEBUG client->txn_id:%ld client->seq_id: %d", client->txn_id, client->seq_id);
        _ctx->get_runtime_state()->set_single_sql_autocommit(true);
    } else {
        //DB_WARNING("DEBUG client->txn_id:%ld client->seq_id: %d", client->txn_id, client->seq_id);
        _ctx->get_runtime_state()->set_single_sql_autocommit(false);
    }
    if (client->txn_id != 0) {
        client->insert_txn_tid(table_id);
    }
}

void LogicalPlanner::plan_begin_txn() {
    create_packet_node(pb::OP_BEGIN);
    pb::PlanNode* plan_node = _ctx->add_plan_node();
    plan_node->set_node_type(pb::BEGIN_MANAGER_NODE);
    plan_node->set_limit(-1);
    plan_node->set_num_children(0);
    pb::DerivePlanNode* derived_node = plan_node->mutable_derive_node();
    pb::TransactionNode* txn_node = derived_node->mutable_transaction_node();
    
    txn_node->set_txn_cmd(pb::TXN_BEGIN);

    auto client = _ctx->client_conn;
    client->on_begin();
    return;
}

void LogicalPlanner::plan_commit_txn() {
    create_packet_node(pb::OP_COMMIT);
    pb::PlanNode* plan_node = _ctx->add_plan_node();
    plan_node->set_limit(-1);
    plan_node->set_num_children(0);
    plan_node->set_node_type(pb::COMMIT_MANAGER_NODE);
    pb::DerivePlanNode* derived_node = plan_node->mutable_derive_node();
    pb::TransactionNode* txn_node = derived_node->mutable_transaction_node();
    
    //txn_node->set_txn_id(_ctx->txn_id);
    txn_node->set_txn_cmd(pb::TXN_COMMIT);
}

void LogicalPlanner::plan_commit_and_begin_txn() {
    create_packet_node(pb::OP_COMMIT);
    pb::PlanNode* plan_node = _ctx->add_plan_node();
    plan_node->set_node_type(pb::COMMIT_MANAGER_NODE);
    plan_node->set_limit(-1);
    plan_node->set_num_children(0);
    pb::DerivePlanNode* derived_node = plan_node->mutable_derive_node();
    pb::TransactionNode* txn_node = derived_node->mutable_transaction_node();
    
    txn_node->set_txn_cmd(pb::TXN_COMMIT_BEGIN);
    return;
}

void LogicalPlanner::plan_rollback_txn() {
    create_packet_node(pb::OP_ROLLBACK);
    pb::PlanNode* plan_node = _ctx->add_plan_node();
    plan_node->set_node_type(pb::ROLLBACK_MANAGER_NODE);
    plan_node->set_limit(-1);
    plan_node->set_num_children(0);
    pb::DerivePlanNode* derived_node = plan_node->mutable_derive_node();
    pb::TransactionNode* txn_node = derived_node->mutable_transaction_node();
    
    DB_WARNING("plan_rollback_txn");
    //txn_node->set_txn_id(_ctx->txn_id);
    txn_node->set_txn_cmd(pb::TXN_ROLLBACK);
}

void LogicalPlanner::plan_rollback_and_begin_txn() {
    create_packet_node(pb::OP_ROLLBACK);
    pb::PlanNode* plan_node = _ctx->add_plan_node();
    plan_node->set_node_type(pb::ROLLBACK_MANAGER_NODE);
    plan_node->set_limit(-1);
    plan_node->set_num_children(0);
    pb::DerivePlanNode* derived_node = plan_node->mutable_derive_node();
    pb::TransactionNode* txn_node = derived_node->mutable_transaction_node();
    
    txn_node->set_txn_cmd(pb::TXN_ROLLBACK_BEGIN);
}

int LogicalPlanner::fill_placeholders(
        std::unordered_multimap<int, ExprNode*>& placeholders,
        const std::vector<const parser::Node*>& parser_placeholders) {
    pb::Expr expr;
    pb::ExprNode* p_node = expr.add_nodes();
    if (p_node == nullptr) {
        DB_WARNING("p_node is nullptr");
        return -1;
    }
    p_node->set_num_children(0);

    int max_placeholder_id = -1;
    for (auto& kv : placeholders) {
        const int placeholder_id = kv.first;
        Literal* p_placeholder = static_cast<Literal*>(kv.second);
        if (BAIDU_UNLIKELY(placeholder_id == -1)) {
            DB_WARNING("Invalid placeholder_id");
            return -1;
        }
        if (BAIDU_UNLIKELY(p_placeholder == nullptr)) {
            DB_WARNING("p_placeholder is nullptr");
            return -1;
        }
        if (BAIDU_UNLIKELY(parser_placeholders.size() <= placeholder_id)) {
            DB_WARNING("parser_placeholders.size: %d is smaller than placeholder_id: %d",
                        (int)parser_placeholders.size(), placeholder_id);
            return -1;
        }
        const parser::LiteralExpr* p_parser_placeholder = 
            static_cast<const parser::LiteralExpr*>(parser_placeholders[placeholder_id]);
        if (BAIDU_UNLIKELY(p_parser_placeholder == nullptr)) {
            DB_WARNING("p_parser_placeholder is nullptr");
            return -1;
        }
        CreateExprOptions options;
        options.is_plan_cache = true;
        if (BAIDU_UNLIKELY(create_term_literal_node(p_parser_placeholder, expr, options) != 0)) {
            DB_WARNING("Fail to create_term_literal_node");
            return -1;
        }
        p_placeholder->init(*p_node);
        if (placeholder_id > max_placeholder_id) {
            max_placeholder_id = placeholder_id;
        }
    }
    if (max_placeholder_id + 1 != parser_placeholders.size()) {
        DB_WARNING("placeholder_id num: %d is not equal to parser_placeholders.size: %d",
                    (max_placeholder_id + 1), (int)parser_placeholders.size());
        return -1;
    }
    return 0;
}

int LogicalPlanner::set_dml_local_index_binlog(const int64_t table_id) {
    if (table_id == -1) {
	return 0;
    }
    if (_ctx->open_binlog && !_factory->has_global_index(table_id)) {
        // plan的第二个节点为dml节点
        if (_ctx->plan.nodes_size() < 2) {
            DB_WARNING("Invalid nodes size: %d", (int)_ctx->plan.nodes_size());
            return -1;
        }
        pb::PlanNode* dml_node = _ctx->plan.mutable_nodes(1);
        if (dml_node == nullptr) {
            DB_WARNING("dml_node is nullptr");
            return -1;
        }
        switch (_ctx->stmt_type) {
        case parser::NT_INSERT: 
        case parser::NT_UPDATE:
        case parser::NT_DELETE:
            dml_node->set_local_index_binlog(true);
            break;
        default:
            DB_WARNING("un-supported command type: %d", _ctx->stmt_type);
            return -1;
        }
    }
    return 0;
}

int LogicalPlanner::get_convert_charset_info() {
    if (!FLAGS_enable_convert_charset) {
        return 0;
    }
    if (_ctx->is_complex) {
        // 当前只支持简单SQL编码转换
        return 0;
    }
    if (_plan_table_ctx->table_info.size() == 1) {
        SmartTable table_info = _plan_table_ctx->table_info.begin()->second;
        if (table_info == nullptr) {
            DB_WARNING("table_info is nullptr");
            return -1;
        }
        _ctx->table_charset = table_info->charset;
        if (_ctx->charset == _ctx->table_charset) {
            return 0;
        }
        if (_ctx->charset == pb::CS_UNKNOWN || _ctx->table_charset == pb::CS_UNKNOWN) {
            DB_WARNING("Invalid charset[%d] or table_charset[%d]", _ctx->charset, _ctx->table_charset);
            return -1;
        }
        _ctx->need_convert_charset = true;
    }
    return 0;
}

int LogicalPlanner::can_use_dblink(SmartTable table) {
    if (table == nullptr) {
        DB_WARNING("table is nullptr");
        return -1;
    }
    if (_ctx == nullptr || _ctx->client_conn == nullptr || _factory == nullptr) {
        DB_WARNING("ctx or client_conn or factory is nullptr");
        return -1;
    }
    if (!FLAGS_enable_dblink) {
        DB_WARNING("not support dblink");
        _ctx->stat_info.error_code = ER_BAD_TABLE_ERROR;
        _ctx->stat_info.error_msg << "not support dblink";
        return -1;
    }
    if (table->dblink_info.type() == pb::LT_BAIKALDB) {
        // dblink BaikalDB表只支持INSERT/DELETE/UPDATE/SELECT/UNION
        if (_ctx->stmt_type != parser::NT_INSERT 
                && _ctx->stmt_type != parser::NT_DELETE
                && _ctx->stmt_type != parser::NT_UPDATE 
                && _ctx->stmt_type != parser::NT_SELECT
                && _ctx->stmt_type != parser::NT_UNION) {
            DB_WARNING("dblink table not support stmt type: %d", _ctx->stmt_type);
            _ctx->stat_info.error_code = ER_BAD_TABLE_ERROR;
            _ctx->stat_info.error_msg << "dblink table only supprt INSERT/DELETE/UPDATE/SELECT/UNION";
            return -1;
        }
    } else if (table->dblink_info.type() == pb::LT_MYSQL){
        // dblink MySQL表只支持SELECT/UNION
        if (_ctx->stmt_type != parser::NT_SELECT 
                && _ctx->stmt_type != parser::NT_UNION) {
            DB_WARNING("dblink table not support stmt type: %d", _ctx->stmt_type);
            _ctx->stat_info.error_code = ER_BAD_TABLE_ERROR;
            _ctx->stat_info.error_msg << "dblink mysql table only supprt SELECT/UNION";
            return -1;
        }
    } else {
        DB_WARNING("dblink table not support, %d", table->dblink_info.type());
        _ctx->stat_info.error_code = ER_BAD_TABLE_ERROR;
        _ctx->stat_info.error_msg << "dblink table only supprt BAIKALDB/MYSQL";
        return -1;
    }
    // dblink表不支持事务
    if (_ctx->client_conn->txn_id != 0) {
        DB_WARNING("dblink table not support txn");
        _ctx->stat_info.error_code = ER_BAD_TABLE_ERROR;
        _ctx->stat_info.error_msg << "dblink table not support txn";
        return -1;
    }
    if (_ctx->stmt_type != parser::NT_SELECT) {
        if (need_dml_txn(table->id) == 0) {
            DB_WARNING("dblink table not support dml txn");
            _ctx->stat_info.error_code = ER_BAD_TABLE_ERROR;
            _ctx->stat_info.error_msg << "dblink table not support dml txn, maybe table has binlog or global index";
            return -1;
        }
    }
    // dblink表不支持外部映射表为代价表
    if (_factory->is_switch_open(table->id, TABLE_SWITCH_COST)) {
        DB_WARNING("dblink table not support statistics table");
        _ctx->stat_info.error_code = ER_BAD_TABLE_ERROR;
        _ctx->stat_info.error_msg << "dblink table not support statistics table";
        return -1;
    }
    return 0;
}

int LogicalPlanner::inc_slot_ref_cnt(const pb::Expr& expr) {
    if (_ctx == nullptr) {
        DB_WARNING("_ctx is nullptr");
        return -1;
    }
    // 遍历pb获取<tuple_id, slot_id>
    std::vector<std::pair<int32_t, int32_t>> tuple_slot_ids;
    tuple_slot_ids.reserve(expr.nodes().size());
    for (const auto& node : expr.nodes()) {
        if (node.node_type() == pb::SLOT_REF) {
            std::pair<int32_t, int32_t> tuple_slot = 
                std::make_pair(node.derive_node().tuple_id(), node.derive_node().slot_id());
            tuple_slot_ids.emplace_back(tuple_slot);
        }
    }
    for (const auto& [tuple_id, slot_id] : tuple_slot_ids) {
        pb::TupleDescriptor* tuple = _ctx->get_tuple_desc(tuple_id);
        if (tuple == nullptr) {
            DB_WARNING("tuple is nullptr");
            return -1;
        }
        if (slot_id - 1 < 0 || slot_id - 1 >= tuple->slot_idxes_size()) {
            DB_WARNING("Invalid slot_id: %d, tuple slot_idxes size: %d", slot_id, tuple->slot_idxes_size());
            return -1;
        }
        int slot_idx = tuple->slot_idxes(slot_id - 1);
        if (slot_idx < 0 || slot_idx >= tuple->slots_size()) {
            DB_WARNING("Invalid slot_idx: %d, tuple slots size: %d", slot_id, tuple->slots_size());
            return -1;
        }
        int ref_cnt = tuple->slots(slot_idx).ref_cnt();
        tuple->mutable_slots(slot_idx)->set_ref_cnt(ref_cnt + 1);
    }
    return 0;
}

} //namespace
