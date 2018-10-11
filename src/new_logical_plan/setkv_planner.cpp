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

#include "network_socket.h"
#include "setkv_planner.h"

namespace baikaldb {
int SetKVPlanner::plan() {
    if (nullptr == (_set_stmt = (parser::SetStmt*)(_ctx->stmt))) {
        DB_WARNING("no sql setkv");
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
            if (var_assign->value->expr_type != parser::ET_LITETAL) {
                DB_WARNING("invalid expr type: %d", var_assign->value->expr_type);
                return -1;
            }
            parser::LiteralExpr* literal = (parser::LiteralExpr*)(var_assign->value);
            if (literal->literal_type != parser::LT_INT) {
                DB_WARNING("invalid literal expr type: %d", literal->literal_type);
                return -1;
            }
            //DB_WARNING("KEY: %s, %d", kv->key, kv->value->item_type);
            if (literal->_u.int64_val == 0) {
                return set_autocommit_0();
            } else {
                return set_autocommit_1();
            }
        } else {
            DB_WARNING("unrecoginized command: %s", _ctx->sql.c_str());
            _ctx->succ_after_logical_plan = true;
            return 0;
        }
    }
    return -1;
}

int SetKVPlanner::set_autocommit_0() {
    auto client = _ctx->runtime_state.client_conn();
    client->autocommit = false;
    if (client->txn_id == 0) {
        plan_begin_txn();
    } else {
        _ctx->succ_after_logical_plan = true;
        DB_WARNING("set_autocommit_0 inside a txn, ignore");
    }
    _ctx->runtime_state.set_autocommit(false);
    return 0;
}

int SetKVPlanner::set_autocommit_1() {
    auto client = _ctx->runtime_state.client_conn();
    client->autocommit = true;
    if (client->txn_id == 0) {
        _ctx->succ_after_logical_plan = true;
        //DB_WARNING("set_autocommit_1 outside a txn, ignore");
        return 0;
    }
    plan_commit_txn();
    _ctx->runtime_state.set_autocommit(false); // autocommit status before set autocommit=1
    return 0;
}
} // end of namespace baikaldb
