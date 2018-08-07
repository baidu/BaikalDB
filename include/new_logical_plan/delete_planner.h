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

// Brief:  the class for generating deletion SQL plan
#pragma once

#include "logical_planner.h"
#include "query_context.h"

namespace baikaldb {
class DeletePlanner : public LogicalPlanner {
public:

    DeletePlanner(QueryContext* ctx) : 
        LogicalPlanner(ctx), 
        _limit_count(-1) {}

    virtual ~DeletePlanner() {}

    virtual int plan();

private:
    // method to create plan node
    int create_delete_node();

    // method to create plan node
    int create_truncate_node();

    int parse_where();

    int parse_orderby();

    void parse_limit() {
        if (_delete_stmt->limit != nullptr) {
            _limit_count = _delete_stmt->limit->count;
            _offset = _delete_stmt->limit->offset;
        }
    }

private:
    //sql_cmd_delete_t*           _delete_cmd;
    //sql_cmd_truncate_table_t*   _truncate_cmd;
    
    parser::DeleteStmt*           _delete_stmt;
    parser::TruncateStmt*         _truncate_stmt;
    parser::NodeType            stmt_type;
    std::vector<pb::Expr>       _where_filters;
    int32_t                     _limit_count;
    int32_t                     _offset;
};
} //namespace baikal

