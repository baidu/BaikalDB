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
            DB_WARNING("var_assign->value is null: %s", var_assign->key.value);
            return -1;
        }
        std::transform(key.begin(), key.end(), key.begin(), ::tolower);
        if (key == "autocommit") {
            return set_autocommit(var_assign->value);
        } else if (boost::algorithm::istarts_with(key, "@@global.")) {
            // TODO handle set global variable
        } else if (boost::algorithm::istarts_with(key, "@@session.") 
                || boost::algorithm::istarts_with(key, "@@local.")
                || boost::algorithm::istarts_with(key, "@@")) {
            // TODO handle set session/local variable
        } else if (boost::algorithm::istarts_with(key, "@")) {
            // handle set user variable
            if (0 != set_user_variable(key, var_assign->value)) {
                DB_WARNING("assign user variable failed");
                return -1;
            }
        } else {
            DB_WARNING("unrecoginized command: %s", _ctx->sql.c_str());
            _ctx->succ_after_logical_plan = true;
            return 0;
        }
    }
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
    if (literal->_u.int64_val == 0) {
        return set_autocommit_0();
    } else {
        return set_autocommit_1();
    }
}

int SetKVPlanner::set_autocommit_0() {
    auto client = _ctx->runtime_state.client_conn();
    client->autocommit = false;
    if (client->txn_id == 0) {
        plan_begin_txn();
    } else {
        _ctx->succ_after_logical_plan = true;
        return 0;
    }
    _ctx->runtime_state.set_autocommit(false);
    return 0;
}

int SetKVPlanner::set_autocommit_1() {
    auto client = _ctx->runtime_state.client_conn();
    client->autocommit = true;
    if (client->txn_id == 0) {
        _ctx->succ_after_logical_plan = true;
        return 0;
    }
    plan_commit_txn();
    _ctx->runtime_state.set_autocommit(false); // autocommit status before set autocommit=1
    return 0;
}

int SetKVPlanner::set_user_variable(const std::string& key, parser::ExprNode* expr) {
    auto client = _ctx->runtime_state.client_conn();
    pb::Expr var_expr_pb;
    if (0 != create_expr_tree(expr, var_expr_pb)) {
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
        client->user_vars[key.substr(1)] = pb_node;
    } else {
        DB_WARNING("user variable should be constant expression");
        return -1;
    }
    _ctx->succ_after_logical_plan = true;
    return 0;
}
} // end of namespace baikaldb
