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

// Brief:  the class for generating deletion SQL plan
#pragma once

#include "logical_planner.h"
#include "query_context.h"

namespace baikaldb {
class DeletePlanner : public LogicalPlanner {
public:

    DeletePlanner(QueryContext* ctx) : 
        LogicalPlanner(ctx) {}

    virtual ~DeletePlanner() {}

    virtual int plan();

private:
    // method to create plan node
    int create_delete_node(pb::PlanNode* delete_node);

    // method to create plan node
    int create_truncate_node();
    int parse_where();
    int parse_orderby();
    int parse_limit();
    int reset_auto_incr_id();

private:
    parser::DeleteStmt*         _delete_stmt;
    parser::TruncateStmt*       _truncate_stmt;
    std::vector<pb::Expr>       _where_filters;
    pb::Expr                    _limit_offset;
    pb::Expr                    _limit_count;
};
} //namespace baikal

