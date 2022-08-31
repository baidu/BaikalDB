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

#include "network_socket.h"
#include "prepare_planner.h"
#include "select_planner.h"
#include "insert_planner.h"
#include "delete_planner.h"
#include "update_planner.h"
#include "transaction_planner.h"
#include "exec_node.h"
#include "packet_node.h"
#include "literal.h"
#include "expr_optimizer.h"

namespace baikaldb {
DECLARE_string(log_plat_name);

int PreparePlanner::plan() {
    auto client = _ctx->client_conn;
    if (_ctx->stmt_type == parser::NT_NEW_PREPARE) {
        std::string stmt_name;
        std::string stmt_sql;
        if (_ctx->mysql_cmd == COM_STMT_PREPARE) {
            client->stmt_id++;
            stmt_name = std::to_string(client->stmt_id);
            stmt_sql = _ctx->sql;
        } else if (_ctx->mysql_cmd == COM_QUERY) {
            parser::NewPrepareStmt* prepare = (parser::NewPrepareStmt*)_ctx->stmt;
            stmt_name = prepare->name.value;
            stmt_sql = prepare->sql.value;
        } else {
            DB_WARNING("invalid stmt type: %d", _ctx->mysql_cmd);
            return -1;
        }
        _ctx->prepare_stmt_name = stmt_name;
        // DB_WARNING("stmt_name: %s, stmt_sql: %s", stmt_name.c_str(), stmt_sql.c_str());
        if (0 != stmt_prepare(stmt_name, stmt_sql)) {
            DB_WARNING("create prepare stmt failed: %s", stmt_sql.c_str());
            return -1;
        }
        _ctx->succ_after_logical_plan = true;
    } else if (_ctx->stmt_type == parser::NT_EXEC_PREPARE) {
        if (_ctx->mysql_cmd == COM_STMT_EXECUTE) {
            if (0 != stmt_execute(_ctx->prepare_stmt_name, _ctx->param_values)) {
                DB_WARNING("execute paepared stmt failed");
                return -1;
            }
        } else if (_ctx->mysql_cmd == COM_QUERY) {
            parser::ExecPrepareStmt* exec = (parser::ExecPrepareStmt*)_ctx->stmt;
            std::string stmt_name = exec->name.value;
            // size_t num_params = exec->param_list.size();
            // if (num_params != _ctx->placeholders.size()) {
            //     _ctx->stat_info.error_code = ER_WRONG_ARGUMENTS;
            //     _ctx->stat_info.error_msg << "Incorrect arguments to EXECUTE: " 
            //                               << num_params << ", " 
            //                               << _ctx->placeholders.size();
            //     return -1;
            // }
            std::vector<pb::ExprNode> params;
            for (int idx = 0; idx < exec->param_list.size(); ++idx) {
                std::string var_name(exec->param_list[idx].c_str());
                auto var_iter = client->user_vars.find(var_name.substr(1));
                if (var_iter != client->user_vars.end()) {
                    params.push_back(var_iter->second);
                } else {
                    pb::ExprNode expr_node;
                    expr_node.set_node_type(pb::NULL_LITERAL);
                    expr_node.set_col_type(pb::NULL_TYPE);
                    params.push_back(expr_node);
                }
            }
            if (0 != stmt_execute(stmt_name, params)) {
                DB_WARNING("execute paepared stmt failed");
                return -1;
            }
        } else {
            DB_WARNING("invalid stmt type: %d", _ctx->mysql_cmd);
            return -1;
        }
        PacketNode* packet_node = static_cast<PacketNode*>(_ctx->root->get_node(pb::PACKET_NODE));
        if (_ctx->mysql_cmd == COM_STMT_EXECUTE) {
            packet_node->set_binary_protocol(true);
        }
    } else if (_ctx->stmt_type == parser::NT_DEALLOC_PREPARE) {
        std::string stmt_name;
        if (_ctx->mysql_cmd == COM_STMT_CLOSE) {
            stmt_name = _ctx->prepare_stmt_name;
        } else if (_ctx->mysql_cmd == COM_QUERY) {
            parser::DeallocPrepareStmt* deallocate = (parser::DeallocPrepareStmt*)_ctx->stmt;
            stmt_name = deallocate->name.value;
        } else {
            DB_WARNING("invalid mysql_cmd: %d", _ctx->mysql_cmd);
            return -1;
        }
        if (0 != stmt_close(stmt_name)) {
            DB_WARNING("close prepare stmt failed");
            return -1;
        }
        _ctx->succ_after_logical_plan = true;
    } else {
        DB_WARNING("invalid stmt type: %d", _ctx->stmt_type);
        return -1;
    }
    return 0;
}

int PreparePlanner::stmt_prepare(const std::string& stmt_name, const std::string& stmt_sql) {
    auto client = _ctx->client_conn;
    // If a prepared statement with the given name already exists, 
    // it is deallocated implicitly before the new statement is prepared. 
    auto iter = client->prepared_plans.find(stmt_name);
    if (iter != client->prepared_plans.end()) {
        client->prepared_plans.erase(iter);
        NetworkSocket::bvar_prepare_count << -1;
    }
    if (stmt_sql.size() == 0) {
        _ctx->stat_info.error_code = ER_EMPTY_QUERY;
        _ctx->stat_info.error_msg << "Query was empty";
        DB_WARNING("Query was empty");
        return -1;
    }
    //DB_WARNING("stmt_name:%s stmt_sql:%s", stmt_name.c_str(), stmt_sql.c_str());
    parser::SqlParser parser;
    parser.parse(stmt_sql);
    if (parser.error != parser::SUCC) {
        _ctx->stat_info.error_code = ER_SYNTAX_ERROR;
        _ctx->stat_info.error_msg << "syntax error! errno: " << parser.error
                                  << " errmsg: " << parser.syntax_err_str;
        DB_WARNING("parsing error! errno: %d, errmsg: %s, sql: %s", 
            parser.error, 
            parser.syntax_err_str.c_str(),
            _ctx->sql.c_str());
        return -1;
    }
    if (parser.result.size() != 1) {
        DB_WARNING("multi-stmt is not supported, sql: %s", stmt_sql.c_str());
        _ctx->stat_info.error_code = ER_NOT_SUPPORTED_YET;
        _ctx->stat_info.error_msg << "multi-stmt is not supported";
        return -1;
    }
    if (parser.result[0] == nullptr) {
        DB_WARNING("sql parser stmt is null, sql: %s", stmt_sql.c_str());
        return -1;
    }

    // create commit fetcher node
    std::shared_ptr<QueryContext> prepare_ctx(new (std::nothrow)QueryContext());
    if (prepare_ctx.get() == nullptr) {
        DB_WARNING("create prepare context failed");
        return -1;
    }
    prepare_ctx->new_prepared = true;
    prepare_ctx->is_prepared = true;
    prepare_ctx->stmt = parser.result[0];
    prepare_ctx->stmt_type = prepare_ctx->stmt->node_type;
    prepare_ctx->cur_db = _ctx->cur_db;
    prepare_ctx->user_info = _ctx->user_info;
    prepare_ctx->row_ttl_duration = _ctx->row_ttl_duration;
    prepare_ctx->is_complex = _ctx->is_complex;
    prepare_ctx->client_conn = client;
    prepare_ctx->get_runtime_state()->set_client_conn(client);
    prepare_ctx->sql = stmt_sql;

    std::unique_ptr<LogicalPlanner> planner;
    switch (prepare_ctx->stmt_type) {
    case parser::NT_SELECT:
        planner.reset(new SelectPlanner(prepare_ctx.get()));
        prepare_ctx->is_select = true;
        break;
    case parser::NT_INSERT:
        planner.reset(new InsertPlanner(prepare_ctx.get()));
        break;
    case parser::NT_UPDATE:
        planner.reset(new UpdatePlanner(prepare_ctx.get()));
        break;
    case parser::NT_DELETE:
        planner.reset(new DeletePlanner(prepare_ctx.get()));
        break;
    default:
        DB_WARNING("un-supported prepare command type: %d", prepare_ctx->stmt_type);
        return -1;
    }
    if (planner->plan() != 0) {
        _ctx->stat_info.error_code = prepare_ctx->stat_info.error_code;
        _ctx->stat_info.error_msg.str(prepare_ctx->stat_info.error_msg.str());
        DB_WARNING("gen plan failed, type:%d", prepare_ctx->stmt_type);
        return -1;
    }
    if (prepare_ctx->stat_info.sign == 0) {
        int generate_sql_sign_ret = generate_sql_sign(prepare_ctx.get(), prepare_ctx->stmt);
        if (generate_sql_sign_ret < 0) {
            return -1;
        }
    }
    int ret = prepare_ctx->create_plan_tree();
    if (ret < 0) {
        DB_WARNING("Failed to pb_plan to execnode");
        return -1;
    }
    prepare_ctx->root->find_place_holder(prepare_ctx->placeholders);
    /*
    // 包括类型推导与常量表达式计算
    ret = ExprOptimize().analyze(prepare_ctx.get());
    if (ret < 0) {
        DB_WARNING("ExprOptimize failed");
        return ret;
    }
    */
    client->prepared_plans[stmt_name] = prepare_ctx;
    NetworkSocket::bvar_prepare_count << 1;
    return 0;
}

// TODO, transaction ID, insert records, update records
int PreparePlanner::stmt_execute(const std::string& stmt_name, std::vector<pb::ExprNode>& params) {
    auto client = _ctx->client_conn;

    auto iter = client->prepared_plans.find(stmt_name);
    if (iter == client->prepared_plans.end()) {
        _ctx->stat_info.error_code = ER_UNKNOWN_STMT_HANDLER;
        _ctx->stat_info.error_msg << "Unknown prepared statement handler (" << stmt_name << ") given to EXECUTE";
        DB_WARNING("Unknown prepared statement handler (%s) given to EXECUTE", stmt_name.c_str());
        return -1;
    }

    std::shared_ptr<QueryContext> prepare_ctx = iter->second;
    _ctx->stat_info.family = prepare_ctx->stat_info.family;
    _ctx->stat_info.table = prepare_ctx->stat_info.table;
    _ctx->stat_info.sample_sql << prepare_ctx->stat_info.sample_sql.str();
    _ctx->stat_info.sign = prepare_ctx->stat_info.sign;
    if (params.size() != prepare_ctx->placeholders.size()) {
        _ctx->stat_info.error_code = ER_WRONG_ARGUMENTS;
        _ctx->stat_info.error_msg << "Incorrect arguments to EXECUTE: " 
                                  << params.size() << ", " 
                                  << prepare_ctx->placeholders.size();
        return -1;
    }
    auto& tuple_descs = prepare_ctx->tuple_descs();
    // ttl沿用prepare的注释
    DB_DEBUG("row_ttl_duration %ld", prepare_ctx->row_ttl_duration);
    _ctx->row_ttl_duration = prepare_ctx->row_ttl_duration;
    _ctx->is_complex = prepare_ctx->is_complex;
    _ctx->mutable_tuple_descs()->assign(tuple_descs.begin(), tuple_descs.end());
    // TODO dml的plan复用
    if (!prepare_ctx->is_select) {
        // enable_2pc=true or table has global index need generate txn_id
        set_dml_txn_state(prepare_ctx->prepared_table_id);
        _ctx->plan.CopyFrom(prepare_ctx->plan);
        int ret = _ctx->create_plan_tree();
        if (ret < 0) {
            DB_WARNING("Failed to pb_plan to execnode");
            return -1;
        }
        _ctx->root->find_place_holder(_ctx->placeholders);
    } else {
        if (client->txn_id == 0) {
            prepare_ctx->get_runtime_state()->set_single_sql_autocommit(true);
        } else {
            prepare_ctx->get_runtime_state()->set_single_sql_autocommit(false);
        }
        // select prepare plan复用
        _ctx->runtime_state = prepare_ctx->runtime_state;
        _ctx->root = prepare_ctx->root;
        _ctx->placeholders = prepare_ctx->placeholders;
    }

    for (size_t idx = 0; idx < params.size(); ++idx) {
        auto place_holder_iter = _ctx->placeholders.find(idx);
        if (place_holder_iter == _ctx->placeholders.end() || place_holder_iter->second == nullptr) {
            _ctx->stat_info.error_code = ER_WRONG_ARGUMENTS;
            _ctx->stat_info.error_msg << "Place holder index error";
            return -1;
        }
        Literal* place_holder = static_cast<Literal*>(place_holder_iter->second);
        place_holder->init(params[idx]);
    }
    _ctx->stmt_type = prepare_ctx->stmt_type;
    _ctx->exec_prepared = true;
    return 0;
}

int PreparePlanner::stmt_close(const std::string& stmt_name) {
    auto client = _ctx->client_conn;
    auto iter = client->prepared_plans.find(stmt_name);
    if (iter != client->prepared_plans.end()) {
        client->query_ctx->sql = iter->second->sql;
        client->prepared_plans.erase(iter);
        NetworkSocket::bvar_prepare_count << -1;
    }
    return 0;
}
} // end of namespace baikaldb
