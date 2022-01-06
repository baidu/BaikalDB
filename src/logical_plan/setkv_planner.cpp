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

#include <boost/algorithm/string.hpp>
#include "network_socket.h"
#include "setkv_planner.h"
#include "literal.h"

namespace baikaldb {
int SetKVPlanner::plan() {
    if (nullptr == (_set_stmt = (parser::SetStmt*)(_ctx->stmt))) {
        DB_WARNING("invalid setkv statement type");
        return -1;
    }
    parser::Vector<parser::VarAssign*> var_list = _set_stmt->var_list;
    int var_len = var_list.size();
    for (int idx = 0; idx < var_len; ++idx) {
        parser::VarAssign* var_assign = var_list[idx];
        std::string key(var_assign->key.value);
        if (var_assign->value == nullptr) {
            // SET variable = DEFAULT 时, value为空
            DB_WARNING("var_assign->value is null: %s", var_assign->key.value);
            _ctx->succ_after_logical_plan = true;
            return 0;
        }
        // https://dev.mysql.com/doc/refman/5.7/en/set-variable.html
        std::transform(key.begin(), key.end(), key.begin(), ::tolower);
        bool system_var = false;
        bool global_var = false;

        if (boost::algorithm::istarts_with(key, "@@global.")) {
            system_var = true;
            global_var = true;
            key = key.substr(9);
        } else if (boost::algorithm::istarts_with(key, "@@session.")) {
            system_var = true;
            global_var = false;
            key = key.substr(10);
        } else if (boost::algorithm::istarts_with(key, "@@local.")) {
            system_var = true;
            global_var = false;
            key = key.substr(8);
        } else if (boost::algorithm::istarts_with(key, "@@isolation.")) {
            //只为了兼容mysql，所有设置事务隔离级别的sql不做任何处理
            //如果后续需要完善次功能需要修改sql paser
            key = key.substr(12);
            _ctx->succ_after_logical_plan = true;
            return 0;
        } else if (boost::algorithm::istarts_with(key, "@@")) {
            system_var = true;
            global_var = false;
            key = key.substr(2);
        } else if (boost::algorithm::istarts_with(key, "@")) {
            system_var = false;
            key = key.substr(1);
        } else {
            // no modifier prefix, as a session system variable
            system_var = true;
            global_var = false;
        }
        if (system_var) {
            // @@global.autocommit not supported
            if (key == "autocommit" && !global_var) {
                //_ctx->succ_after_logical_plan = true;
                return set_autocommit(var_assign->value);
            } else if (key == "sql_mode") { 
                // ignore sql_mode: may be support in the future
                _ctx->succ_after_logical_plan = true;
                return 0;                
            } else {
                DB_WARNING("unrecoginized command: %s", _ctx->sql.c_str());
                _ctx->succ_after_logical_plan = true;
                return 0;
            }
        } else {
            // handle set user variable
            if (0 != set_user_variable(key, var_assign->value)) {
                DB_WARNING("assign user variable failed");
                return -1;
            }
        }
    }
    _ctx->succ_after_logical_plan = true;
    return 0;
}

int SetKVPlanner::set_autocommit(parser::ExprNode* expr) {
    if (expr->expr_type != parser::ET_LITETAL) {
        DB_WARNING("invalid expr type: %d", expr->expr_type);
        return -1;
    }
    parser::LiteralExpr* literal = (parser::LiteralExpr*)expr;
    if (literal->literal_type != parser::LT_INT) {
        DB_WARNING("invalid literal expr type: %d", literal->literal_type);
        return -1;
    }
    auto client = _ctx->client_conn;
    pb::ExprNode int_node;
    int_node.set_node_type(pb::INT_LITERAL);
    int_node.set_col_type(pb::INT64);
    int_node.set_num_children(0);
    if (literal->_u.int64_val == 0) {
        int_node.mutable_derive_node()->set_int_val(0);
        client->session_vars["autocommit"] = int_node;
        return set_autocommit_0();
    } else {
        int_node.mutable_derive_node()->set_int_val(1);
        client->session_vars["autocommit"] = int_node;
        return set_autocommit_1();
    }
}

int SetKVPlanner::set_autocommit_0() {
    auto client = _ctx->client_conn;
    client->autocommit = false;
    if (client->txn_id == 0) {
        plan_begin_txn();
    } else {
        _ctx->succ_after_logical_plan = true;
        return 0;
    }
    _ctx->get_runtime_state()->set_single_sql_autocommit(false);
    return 0;
}

int SetKVPlanner::set_autocommit_1() {
    auto client = _ctx->client_conn;
    client->autocommit = true;
    if (client->txn_id == 0) {
        _ctx->succ_after_logical_plan = true;
        return 0;
    }
    plan_commit_txn();
    _ctx->get_runtime_state()->set_single_sql_autocommit(false); // autocommit status before set autocommit=1
    return 0;
}

int SetKVPlanner::set_user_variable(const std::string& key, parser::ExprNode* expr) {
    auto client = _ctx->client_conn;
    pb::Expr var_expr_pb;
    if (0 != create_expr_tree(expr, var_expr_pb, CreateExprOptions())) {
        DB_WARNING("create var_expr_pb for user variable failed");
        return -1;
    }
    ExprNode* var_expr = nullptr;
    if (0 != ExprNode::create_tree(var_expr_pb, &var_expr)) {
        DB_WARNING("create var_expr for user variable failed");
        return -1;
    }
    int ret = var_expr->type_inferer();
    if (ret < 0) {
        DB_WARNING("expr type_inferer fail:%d", ret);
        return ret;
    }
    //常量表达式计算
    var_expr->const_pre_calc();
    ret = var_expr->open();
    if (ret < 0) {
        DB_WARNING("expr open fail:%d", ret);
        return ret;
    }
    ExprValue value = var_expr->get_value(nullptr);
    var_expr->close();
    ExprNode::destroy_tree(var_expr);

    if (var_expr->is_constant()) {
        var_expr = new Literal(value);
        pb::ExprNode pb_node;
        var_expr->transfer_pb(&pb_node);
        ExprNode::destroy_tree(var_expr);
        client->user_vars[key] = pb_node;
    } else {
        DB_WARNING("user variable should be constant expression");
        return -1;
    }
    _ctx->succ_after_logical_plan = true;
    return 0;
}
} // end of namespace baikaldb
