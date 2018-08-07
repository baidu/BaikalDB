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
#include "base.h"

namespace parser {

struct StartTxnStmt : public StmtNode {
    StartTxnStmt() {
        node_type = NT_START_TRANSACTION;
    }
};

struct CommitTxnStmt : public StmtNode {
    CommitTxnStmt() {
        node_type = NT_COMMIT_TRANSACTION;
    }
};

struct RollbackTxnStmt : public StmtNode {
    RollbackTxnStmt() {
        node_type = NT_ROLLBACK_TRANSACTION;
    }
};

struct VarAssign : public Node {
    String    key;
    ExprNode* value = nullptr;
    VarAssign() {
        node_type = NT_VAR_ASSIGN;
        key = nullptr;
    }
};

struct SetStmt : public StmtNode {
    Vector<VarAssign*> var_list;
    SetStmt() {
        node_type = NT_SET_CMD;
    }
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
