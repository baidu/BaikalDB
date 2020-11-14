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

// Brief:  the class for handling SQL querys like 'set key=val'
#pragma once
#include "logical_planner.h"
#include "query_context.h"
#include "parser.h"

namespace baikaldb {

class SetKVPlanner : public LogicalPlanner {
public:

    SetKVPlanner(QueryContext* ctx) : LogicalPlanner(ctx) {}
    virtual ~SetKVPlanner() {}
    virtual int plan();

private:
    int set_autocommit_0();
    int set_autocommit_1();
    int set_autocommit(parser::ExprNode* expr);
    int set_user_variable(const std::string& key, parser::ExprNode* expr);

private:
    parser::SetStmt*  _set_stmt;
};
} //namespace baikal