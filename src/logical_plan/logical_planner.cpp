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
#include "network_socket.h"
#include "parser.h"
#include "mysql_err_code.h"
#include "physical_planner.h"

namespace bthread {
DECLARE_int32(bthread_concurrency); //bthread.cpp
}

namespace baikaldb {
DECLARE_string(log_plat_name);

std::map<parser::JoinType, pb::JoinType> LogicalPlanner::join_type_mapping {
        { parser::JT_NONE, pb::NULL_JOIN},    
        { parser::JT_INNER_JOIN, pb::INNER_JOIN},
        { parser::JT_LEFT_JOIN, pb::LEFT_JOIN},
        { parser::JT_RIGHT_JOIN, pb::RIGHT_JOIN}
    };


int LogicalPlanner::create_n_ary_predicate(const parser::FuncExpr* func_item, 
        pb::Expr& expr,
        pb::ExprNodeType type,
        const CreateExprOptions& options) {
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
        node->set_charset(_ctx->charset == "gbk" ? pb::GBK : pb::UTF8);
    }
    node->set_node_type(type);
    pb::Function* func = node->mutable_fn();
    if (func_item->fn_name.empty()) {
        DB_WARNING("op:%d fn_name is empty", func_item->func_type);
        return -1;
    }
    func->set_name(func_item->fn_name.c_str());
    func->set_fn_op(func_item->func_type);

    for (int32_t idx = 0; idx < func_item->children.size(); ++idx) {
        if (0 != create_expr_tree(func_item->children[idx], expr, options)) {
            DB_WARNING("create child expr failed");
            return -1;
        }
    }
    node->set_num_children(func_item->children.size());
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
        if (!func_item->is_not) {
            func->set_name("eq");
            func->set_fn_op(parser::FT_EQ);
        } else {
            func->set_name("ne");
            func->set_fn_op(parser::FT_NE);
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
        if (format == "trace") {
            ctx->explain_type = SHOW_TRACE;
        } else if (format == "trace2") {
            ctx->explain_type = SHOW_TRACE2;
        } else if (format == "plan") {
            ctx->explain_type = SHOW_PLAN;
            ctx->is_explain = true;
        } else if (format == "analyze") { 
            ctx->explain_type = ANALYZE_STATISTICS;
        } else if (format == "histogram") {
            ctx->explain_type = SHOW_HISTOGRAM;
            ctx->is_explain = true;
        } else if (format == "cmsketch") {
            ctx->explain_type = SHOW_CMSKETCH;
            ctx->is_explain = true;
        } else if (format == "show_cost") {
            ctx->explain_type = EXPLAIN_SHOW_COST;
        } else if (format == "sign") {
            ctx->client_conn->is_explain_sign = true;
            ctx->explain_type = SHOW_SIGN;
        } else {
            ctx->is_explain = true;
        }
        //DB_WARNING("stmt format:%s", format.c_str());
    } else {
        ctx->stmt = parser.result[0];
        ctx->is_complex = ctx->stmt->is_complex_node();
    }
    ctx->stmt_type = ctx->stmt->node_type;
    if (ctx->stmt_type != parser::NT_SELECT && ctx->explain_type != EXPLAIN_NULL &&
        ctx->explain_type != SHOW_PLAN) {
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
    case parser::NT_CREATE_DATABASE:
    case parser::NT_DROP_TABLE:
    case parser::NT_RESTORE_TABLE:
    case parser::NT_DROP_DATABASE:
    case parser::NT_ALTER_TABLE:
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
    if (planner->plan() != 0) {
        DB_WARNING("gen plan failed, type:%d", ctx->stmt_type);
        return -1;
    }
    int ret = planner->generate_sql_sign(ctx, ctx->stmt);
    if (ret < 0) {
        return -1;
    }
    return 0;
}

int LogicalPlanner::generate_sql_sign(QueryContext* ctx, parser::StmtNode* stmt) {
    pb::OpType op_type = pb::OP_NONE;
    if (ctx->plan.nodes_size() > 0 && ctx->plan.nodes(0).node_type() == pb::PACKET_NODE) {
        op_type = ctx->plan.nodes(0).derive_node().packet_node().op_type();
    }
    stmt->set_print_sample(true);
    auto stat_info = &(ctx->stat_info);
    if (!stat_info->family.empty() && !stat_info->table.empty() ) {
        stat_info->sample_sql << "family_table_tag_optype_plat=[" << stat_info->family << "\t"
            << stat_info->table << "\t" << stat_info->resource_tag << "\t" << op_type << "\t"
            << FLAGS_log_plat_name << "] sql=[" << stmt << "]";
        uint64_t out[2];
        butil::MurmurHash3_x64_128(stat_info->sample_sql.str().c_str(), stat_info->sample_sql.str().size(), 0x1234, out);
        stat_info->sign = out[0];
        if (!ctx->sign_blacklist.empty()) {
            if (ctx->sign_blacklist.count(stat_info->sign) > 0) {
                DB_WARNING("sql sign[%lu] in blacklist, sample_sql[%s]", stat_info->sign, 
                    stat_info->sample_sql.str().c_str());
                ctx->stat_info.error_code = ER_SQL_REFUSE;
                ctx->stat_info.error_msg << "sql sign: " << stat_info->sign << " in blacklist";
                return -1;
            }
        }
        if (!ctx->need_learner_backup && !ctx->sign_forcelearner.empty()) {
            if (ctx->sign_forcelearner.count(stat_info->sign) > 0) {
                ctx->need_learner_backup = true;
                DB_WARNING("sql sign[%lu] in forcelearner, sample_sql[%s]", stat_info->sign, 
                    stat_info->sample_sql.str().c_str());
            }
        }
    }

    return 0;
}

int LogicalPlanner::gen_subquery_plan(parser::DmlNode* subquery, const SmartPlanTableCtx& plan_state,
        const ExprParams& expr_params) {
    _cur_sub_ctx = std::make_shared<QueryContext>();
    auto client = _ctx->client_conn;
    _cur_sub_ctx->stmt = subquery;
    _cur_sub_ctx->expr_params = expr_params;
    _cur_sub_ctx->stmt_type = subquery->node_type;
    _cur_sub_ctx->cur_db = _ctx->cur_db;
    _cur_sub_ctx->is_explain = _ctx->is_explain;
    _cur_sub_ctx->stat_info.log_id = _ctx->stat_info.log_id;
    _cur_sub_ctx->user_info = _ctx->user_info;
    _cur_sub_ctx->row_ttl_duration = _ctx->row_ttl_duration;
    _cur_sub_ctx->is_complex = _ctx->is_complex;
    _cur_sub_ctx->get_runtime_state()->set_client_conn(client);
    _cur_sub_ctx->client_conn = client;
    _cur_sub_ctx->sql = subquery->to_string();
    _cur_sub_ctx->charset = _ctx->charset;
    std::unique_ptr<LogicalPlanner> planner;
    if (_cur_sub_ctx->stmt_type == parser::NT_SELECT) {
        planner.reset(new SelectPlanner(_cur_sub_ctx.get(), plan_state));
    } else if (_cur_sub_ctx->stmt_type == parser::NT_UNION) {
        planner.reset(new UnionPlanner(_cur_sub_ctx.get(), plan_state));
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
    int ret = _cur_sub_ctx->create_plan_tree();
    if (ret < 0) {
        DB_WARNING("Failed to pb_plan to execnode");
        return -1;
    }
    _ctx->set_kill_ctx(_cur_sub_ctx);
    auto stat_info = &(_cur_sub_ctx->stat_info);
    ret = generate_sql_sign(_cur_sub_ctx.get(), subquery);
    if (ret < 0) {
        return -1;
    }
    auto& client_conn = _ctx->client_conn;
    if (client_conn->is_explain_sign) {
        client_conn->insert_subquery_sign(stat_info->sign);
    }
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
    tbl_info.id = --_plan_table_ctx->derived_table_id;
    tbl_info.namespace_ = _ctx->user_info->namespace_;
    std::string table_name = database + "." + table;
    tbl_info.name = table_name;
    bool ok = _plan_table_ctx->table_info.emplace(try_to_lower(table_name), tbl_info_ptr).second;
    if (ok) {
        ScanTupleInfo* tuple_info = get_scan_tuple(table_name, tbl_info.id);
        _ctx->derived_table_ctx_mapping[tuple_info->tuple_id] = _ctx->sub_query_plans.back();
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
        _ctx->stat_info.resource_tag = tbl_ptr->resource_tag;
        // learner降级
        if (tbl_ptr->need_learner_backup) {
            _ctx->need_learner_backup = true;
        }

        _ctx->sign_blacklist.insert(tbl_ptr->sign_blacklist.begin(), tbl_ptr->sign_blacklist.end());
        _ctx->sign_forcelearner.insert(tbl_ptr->sign_forcelearner.begin(), tbl_ptr->sign_forcelearner.end());

        // 通用降级路由
        // 复杂sql(join和子查询)不降级
        if (MetaServerInteract::get_backup_instance()->is_inited() && tbl_ptr->have_backup && !_ctx->is_complex &&
            (tbl_ptr->need_write_backup || (tbl_ptr->need_read_backup && _ctx->is_select))) {
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
        }
        auto& tbl = *tbl_ptr;
        // validate user permission
        pb::OpType op_type = pb::OP_INSERT;
        if (_ctx->stmt_type == parser::NT_SELECT) {
            op_type = pb::OP_SELECT;
        }
        if (!_ctx->user_info->allow_op(op_type, db.id, tbl.id)) {
            DB_WARNING("user %s has no permission to access: %s.%s, db.id:%ld, tbl.id:%ld", 
                _username.c_str(), database.c_str(), table.c_str(), db.id, tbl.id);
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
    _ctx->stat_info.table_id = tableid;
    ScanTupleInfo* tuple_info = get_scan_tuple(alias_full_name, tableid);
    _ctx->current_tuple_ids.emplace(tuple_info->tuple_id);
    _ctx->current_table_tuple_ids.emplace(tuple_info->tuple_id);
    //_table_alias_mapping.emplace(use_table_name, org_table_full_name);
    //_table_alias_mapping.emplace(use_table_full_name, org_table_full_name);
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
    //暂时不支持没有条件的join 
    /*
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
    /*
    for (int i = 0; i < using_col.size(); ++i) {
        std::string field_name = using_col[i]->name.value;
        std::unordered_set<std::string> possible_table_names = get_possible_tables(field_name);
        if (possible_table_names.size() < 2) {
            DB_WARNING("using filed not in more than 2 table");
            return -1;
        }
        int32_t related_table_count = 0;
        std::string left_full_field;
        std::string right_full_field;
        for (auto& table_name : possible_table_names) {
            if (join_node_mem->left_full_table_names.count(table_name) != 0) {
                ++related_table_count;
                left_full_field = table_name + "." + field_name;
            }
        }
        if (related_table_count != 1) {
            DB_WARNING("related table count not equal to 2");
            return -1;
        }

        related_table_count = 0;
        for (auto& table_name : possible_table_names) {
            if (join_node_mem->right_full_table_names.count(table_name) != 0) {
                ++related_table_count;
                right_full_field = table_name + "." + field_name;
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
        func->set_fn_op(OP_EQ);
        func->set_name(scala_func_mapping[OP_EQ]);
        node->set_num_children(2);
        if (0 != create_term_slot_ref_node(left_full_field.c_str(), expr, false)) {
            DB_WARNING("left_full_field using field create term slot fail");
            return -1;
        }
        if (0 != create_term_slot_ref_node(right_full_field.c_str(), expr, false)) {
            DB_WARNING("left_full_field using field create term slot fail");
            return -1;
        }
        pb::Expr* join_expr = join_node_mem->join_node.add_conditions();
        join_expr->CopyFrom(expr);
    }*/
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

    if (table_source->derived_table != nullptr) {
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
        _ctx->stat_info.family = db;
        _ctx->stat_info.table = table;
        _current_tables.emplace_back(db + "." + table);
        parser::DmlNode* derived_table = table_source->derived_table;
        int ret = gen_subquery_plan(derived_table, _plan_table_ctx, ExprParams());
        if (ret < 0) {
            DB_WARNING("gen subquery plan failed");
            return -1;
        }
        _ctx->add_sub_ctx(_cur_sub_ctx);
        return 0;
    }
    parser::TableName* table_name = table_source->table_name;
    if (parse_db_name_from_table_name(table_name, db, table) < 0) {
        DB_WARNING("parser db name from table name fail");
        return -1;
    }
    if (!table_source->as_name.empty()) {
        alias = table_source->as_name.value;
        _current_tables.emplace_back(db + "." + alias);
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

void LogicalPlanner::create_order_func_slot() {
    if (_order_tuple_id == -1) {
        _order_tuple_id = _plan_table_ctx->tuple_cnt++;
    }
    pb::SlotDescriptor slot;
    slot.set_slot_id(_order_slot_cnt++);
    slot.set_tuple_id(_order_tuple_id);
    slot.set_slot_type(pb::INVALID_TYPE);
    _order_slots.push_back(slot);
}

std::vector<pb::SlotDescriptor>& LogicalPlanner::get_agg_func_slot(
        const std::string& agg, const std::string& fn_name, bool& new_slot) {
    if (_agg_tuple_id == -1) {
        _agg_tuple_id = _plan_table_ctx->tuple_cnt++;
        _ctx->current_table_tuple_ids.emplace(_agg_tuple_id);
    }
    static std::unordered_set<std::string> need_intermediate_slot_agg = {
        "avg", "rb_or_cardinality_agg", "rb_and_cardinality_agg", "rb_xor_cardinality_agg"
    };
    std::vector<pb::SlotDescriptor>* slots = nullptr;
    auto iter = _agg_slot_mapping.find(agg);
    if (iter != _agg_slot_mapping.end()) {
        slots = &iter->second;
        new_slot = false;
    } else {
        slots = &_agg_slot_mapping[agg];
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
    auto& slots = get_agg_func_slot(
            expr_item->to_string(), expr_item->fn_name.to_lower(), new_slot);
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
    func->set_name(expr_item->fn_name.to_lower());
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
        // count_distinct 参数为expr list，其他聚合参数为单一expr或slot ref
        for (int i = 0; i < expr_item->children.size(); i++) {
            if (0 != create_expr_tree(expr_item->children[i], agg_expr, options)) {
                DB_WARNING("create child expr failed");
                return -1;
            }
        }
    } else {
        func->set_name(func->name() + "_star");
    }
    // min max无需distinct
    if (expr_item->distinct && func->name() != "max" && func->name() != "min") {
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
    std::unique_ptr<ApplyMemTmp> apply_node_mem(new ApplyMemTmp);
    apply_node_mem->apply_node.set_join_type(join_type);
    apply_node_mem->apply_node.set_max_one_row(options.max_one_row);
    apply_node_mem->apply_node.set_compare_type(options.compare_type);
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
        if (item->fn_name.to_lower() == "last_insert_id" && item->children.size() == 0) {
            pb::ExprNode* node = expr.add_nodes();
            node->set_node_type(pb::INT_LITERAL);
            node->set_col_type(pb::INT64);
            node->set_num_children(0);
            node->mutable_derive_node()->set_int_val(_ctx->client_conn->last_insert_id);
            return 0;
        }
        if (item->fn_name.to_lower() == "database") {
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
        default:
            DB_WARNING("expr:%s", value.get_string().c_str());
            break;
    }    
}


int LogicalPlanner::exec_subquery_expr(QueryContext* sub_ctx, QueryContext* ctx) {
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
    if (sub_ctx->stmt_type == parser::NT_SELECT) {
        state.set_single_store_concurrency();
    }
    ret = sub_ctx->root->open(&state);
    sub_ctx->root->close(&state);
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
        ret = exec_subquery_expr(_cur_sub_ctx.get(), _ctx);
        if (ret < 0) {
            DB_WARNING("exec subquery failed");
            return -1;
        }
        auto state = _cur_sub_ctx->get_runtime_state();
        auto& subquery_exprs_vec = state->get_subquery_exprs();
        if (options.max_one_row && subquery_exprs_vec.size() > 1) {
            _ctx->stat_info.error_code = ER_SUBQUERY_NO_1_ROW;
            _ctx->stat_info.error_msg << "Subquery returns more than 1 row";
            DB_WARNING("Subquery returns more than 1 row");
            return -1;
        }
        // select (SubSelect)
        if (options.is_select_field) {
            if (subquery_exprs_vec.size() == 0) {
                pb::ExprNode* node = expr.add_nodes();
                node->set_node_type(pb::NULL_LITERAL);
            } else {
                if (subquery_exprs_vec.size() != 1 || subquery_exprs_vec[0].size() != 1) {
                    _ctx->stat_info.error_code = ER_SUBQUERY_NO_1_ROW;
                    _ctx->stat_info.error_msg << "Subquery returns more than 1 row";
                    DB_WARNING("Subquery returns more than 1 row");
                    return -1;
                }
                pb::ExprNode* node = expr.add_nodes();
                construct_literal_expr(subquery_exprs_vec[0][0], node);
            }
        // in (SubSelect)
        } else {
            if (subquery_exprs_vec.size() > 0) {
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
                DB_WARNING("not data row_filed_number:%d", _cur_sub_ctx->expr_params.row_filed_number);
                if (_cur_sub_ctx->expr_params.row_filed_number > 1) {
                    pb::ExprNode* row_node = expr.add_nodes();
                    row_node->set_node_type(pb::ROW_EXPR);
                    row_node->set_num_children(_cur_sub_ctx->expr_params.row_filed_number);
                    for (int i = 0; i < _cur_sub_ctx->expr_params.row_filed_number; i++) {
                        pb::ExprNode* node = expr.add_nodes();
                        node->set_node_type(pb::BOOL_LITERAL);
                        node->set_col_type(pb::BOOL);
                        node->mutable_derive_node()->set_bool_val(false);
                    }
                } else {
                    pb::ExprNode* node = expr.add_nodes();
                    node->set_node_type(pb::BOOL_LITERAL);
                    node->set_col_type(pb::BOOL);
                    node->mutable_derive_node()->set_bool_val(false);
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
        _ctx->stat_info.error_msg << "Operand should contain 1 column(s)s";
        DB_WARNING("Operand should contain 1 column(s)s");
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
    // 非相关子查询表达式，直接执行获取结果
    if (!_cur_sub_ctx->expr_params.is_correlated_subquery) {
        ret = exec_subquery_expr(_cur_sub_ctx.get(), _ctx);
        if (ret < 0) {
            return -1;
        }
        auto state = _cur_sub_ctx->get_runtime_state();
        auto& subquery_exprs_vec = state->get_subquery_exprs();
        bool is_eq_all = false;
        ExprValue first_val;
        bool always_false = false;
        bool is_neq_any = false;
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
            
            if (subquery_exprs_vec.size() > 0) {
                if (is_eq_all) {
                    first_val = subquery_exprs_vec[0][0];
                    for (uint32_t i = 1; i < subquery_exprs_vec.size(); i++) {
                        // = all返回多个不同值
                        if (first_val.compare(subquery_exprs_vec[i][0]) != 0) {
                            DB_WARNING("always_false");
                            always_false = true;
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
            if (subquery_exprs_vec.size() > 0) {
                if (is_neq_any) {
                    first_val = subquery_exprs_vec[0][0];
                    for (uint32_t i = 1; i < subquery_exprs_vec.size(); i++) {
                        // = all返回多个不同值
                        if (first_val.compare(subquery_exprs_vec[i][0]) != 0) {
                            DB_WARNING("always_false");
                            always_false = true;
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
            if (_cur_sub_ctx->expr_params.cmp_type == parser::CMP_ALL 
                && subquery_exprs_vec.size() == 1 && subquery_exprs_vec[0][0].is_null()) {
                // > >= all (null) 恒为true
                pb::ExprNode* node = expr.add_nodes();
                node->set_node_type(pb::BOOL_LITERAL);
                node->set_col_type(pb::BOOL);
                node->mutable_derive_node()->set_bool_val(true);
                return 0;
            }
        }
        for (auto& old_node : left_expr.nodes()) {
                compare_expr.add_nodes()->CopyFrom(old_node);
        }
        if (subquery_exprs_vec.size() > 0 && !always_false) {
            if (is_neq_any || is_eq_all) {
               pb::ExprNode* node = compare_expr.add_nodes();
                construct_literal_expr(first_val, node);
            } else {
                for (auto& rows : subquery_exprs_vec) {
                    pb::ExprNode* node = compare_expr.add_nodes();
                    construct_literal_expr(rows[0], node);
                }
            }
        } else {
            pb::ExprNode* node = compare_expr.add_nodes();
            node->set_node_type(pb::BOOL_LITERAL);
            node->set_col_type(pb::BOOL);
            node->mutable_derive_node()->set_bool_val(false);
        }
        for (auto& old_node : compare_expr.nodes()) {
            expr.add_nodes()->CopyFrom(old_node);
        }
    } else {
        _is_correlate_subquery_expr = true;
        for (auto& old_node : compare_expr.nodes()) {
            expr.add_nodes()->CopyFrom(old_node);
        }
        for (auto& old_node : left_expr.nodes()) {
                expr.add_nodes()->CopyFrom(old_node);
        }
        CreateExprOptions tmp_options = options;
        switch (sub_query_expr->cmp_type) {
            case parser::CMP_ANY:
                tmp_options.compare_type = pb::CMP_ANY;
                break;
            case parser::CMP_ALL:
                tmp_options.compare_type = pb::CMP_ALL;
                break;
            case parser::CMP_SOME:
                tmp_options.compare_type = pb::CMP_SOME;
                break;
            default:
                break;
        }
        ret = construct_apply_node(_cur_sub_ctx.get(), expr, pb::LEFT_JOIN, tmp_options);
        if (ret < 0) {
            DB_WARNING("construct apply node failed");
            return -1;
        }
    }
    return 0;
}

int LogicalPlanner::handle_exists_subquery(const parser::ExprNode* expr_item, pb::Expr& expr,
        const CreateExprOptions& options) {
    parser::ExistsSubqueryExpr* sub_query_expr = nullptr;
    bool is_not = false;
    if (expr_item->expr_type == parser::ET_FUNC) {
        parser::FuncExpr* func = (parser::FuncExpr*)expr_item;
        sub_query_expr = (parser::ExistsSubqueryExpr*)func->children[0];
        is_not = true;
    } else {
        sub_query_expr = (parser::ExistsSubqueryExpr*)expr_item;
    }

    ExprParams expr_params;
    expr_params.is_expr_subquery = true;
    int ret = gen_subquery_plan(sub_query_expr->query_expr->query_stmt, _plan_table_ctx, expr_params);
    if (ret < 0) {
        DB_WARNING("gen subquery plan failed");
        return -1;
    }

    // 非相关子查询表达式，直接执行获取结果
    if (!_cur_sub_ctx->expr_params.is_correlated_subquery) {
        ret = exec_subquery_expr(_cur_sub_ctx.get(), _ctx);
        if (ret < 0) {
            return -1;
        }
        auto state = _cur_sub_ctx->get_runtime_state();
        auto& subquery_exprs_vec = state->get_subquery_exprs();
        pb::ExprNode* node = expr.add_nodes();
        node->set_node_type(pb::BOOL_LITERAL);
        node->set_col_type(pb::BOOL);
        if (subquery_exprs_vec.size() > 0 && !is_not) {
            node->mutable_derive_node()->set_bool_val(true);
        } else if (subquery_exprs_vec.size() == 0 && is_not) {
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
        if (0 != create_term_literal_node((parser::LiteralExpr*)expr_item, expr)) {
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
            ret = create_alias_node(static_cast<const parser::ColumnName*>(expr_item), expr);
        }
        if (ret == -2) {
            if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                _ctx->stat_info.error_code = ER_NON_UNIQ_ERROR;
                _ctx->stat_info.error_msg << "Column \'" << expr_item->to_string() << "\' is ambiguous";
            }
            return -1;
        } else if (ret == -1) {
            if (0 != create_term_slot_ref_node(static_cast<const parser::ColumnName*>(expr_item), expr, options)) {
                if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                    _ctx->stat_info.error_code = ER_BAD_FIELD_ERROR;
                    _ctx->stat_info.error_msg << "Unknown column \'" << expr_item->to_string() << "\'";
                }
                return -1;
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
        } else if (dbs.size() > 1 && !FLAGS_disambiguate_select_name) {
            if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                _ctx->stat_info.error_code = ER_AMBIGUOUS_FIELD_TERM;
                _ctx->stat_info.error_msg << "column  \'" << column->name << "\' is ambiguous";
            }
            DB_WARNING("ambiguous field_name: %s", column->to_string().c_str());
            return "";
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
        tuple_info->tuple_id = _plan_table_ctx->tuple_cnt++;
        tuple_info->table_id = table_id;
        tuple_info->slot_cnt = 1;
    }
    //DB_WARNING("table_name:%s tuple_id:%d table_id:%d", table_name.c_str(), tuple_info->tuple_id, table_id);
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
        tuple_info.tuple_id = _plan_table_ctx->tuple_cnt++;
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
int LogicalPlanner::create_alias_node(const parser::ColumnName* column, pb::Expr& expr) {
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
int LogicalPlanner::create_term_literal_node(const parser::LiteralExpr* literal, pb::Expr& expr) {
    pb::ExprNode* node = expr.add_nodes();
    node->set_num_children(0);

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
            node->mutable_derive_node()->set_int_val(literal->_u.int64_val);
            break;
        default:
            DB_WARNING("create_term_literal_node failed: %d", literal->literal_type);
            return -1;
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
    parser::Vector<parser::ByItem*> order_items = order->items;
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
                create_order_func_slot();
            }
        } 
#else
        const pb::ExprNode& node = order_expr.nodes(0);
        if (node.node_type() != pb::SLOT_REF && node.node_type() != pb::AGG_EXPR) {
            //DB_WARNING("un-supported order-by node type: %d", node.node_type());
            //return -1;
            create_order_func_slot();
        }
#endif
        _order_exprs.push_back(order_expr);
        _order_ascs.push_back(is_asc);
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
    //DB_NOTICE("filter:%s", filter->ShortDebugString().c_str());
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
        scan_node->set_num_children(0);
        pb::DerivePlanNode* derive = scan_node->mutable_derive_node();
        pb::ScanNode* scan = derive->mutable_scan_node();
        scan->set_tuple_id(join_root->join_node.left_tuple_ids(0));
        scan->set_table_id(join_root->join_node.left_table_ids(0));
        scan->set_engine(_factory->get_table_engine(scan->table_id()));
        //DB_WARNING("get_table_engine :%d", scan->engine());
        for (auto index_id : join_root->use_indexes) {
            scan->add_use_indexes(index_id);
        }
        for (auto index_id : join_root->force_indexes) {
            scan->add_force_indexes(index_id);
        }
        for (auto index_id : join_root->ignore_indexes) {
            scan->add_ignore_indexes(index_id);
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

void LogicalPlanner::set_dml_txn_state(int64_t table_id) {
    auto client = _ctx->client_conn;
    if (client->txn_id == 0) {
        DB_DEBUG("enable_2pc %d global index %d, binlog %d", 
            _ctx->enable_2pc, _factory->has_global_index(table_id), _factory->has_open_binlog(table_id));
        if (_ctx->enable_2pc
            || _factory->need_begin_txn(table_id)
            || _factory->has_open_binlog(table_id)) {
            client->on_begin();
            DB_DEBUG("get txn %ld", client->txn_id);
            client->seq_id = 0;
            //is_gloabl_ddl 打开时，该连接处理全局二级索引增量数据，不需要处理binlog。
            if (_factory->has_open_binlog(table_id) && !client->is_index_ddl) {
                client->open_binlog = true;
                _ctx->open_binlog = true;
            }
        } else {
            client->txn_id = 0;
            client->seq_id = 0;
        }
        //DB_WARNING("DEBUG client->txn_id:%ld client->seq_id: %d", client->txn_id, client->seq_id);
        _ctx->get_runtime_state()->set_single_sql_autocommit(true);
    } else {
        if (_factory->has_open_binlog(table_id) && !client->is_index_ddl) {
                client->open_binlog = true;
                _ctx->open_binlog = true;
        }
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
} //namespace
