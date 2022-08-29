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

// Brief:  The wrapper of Baidu SQL Parser lib.
#pragma once

#include "logical_planner.h"
#include "query_context.h"
#include "dml.h"

namespace baikaldb {

class UpdatePlanner : public LogicalPlanner {
public:
    UpdatePlanner(QueryContext* ctx) : 
        LogicalPlanner(ctx), 
        _limit_count(-1) {}

    virtual ~UpdatePlanner() {}
    virtual int plan();

private:

    int create_update_node(pb::PlanNode* update_node);

    // method to parse SQL parts
    int parse_kv_list();
    int parse_where();
    int parse_orderby();
    int parse_limit();

private:
    parser::UpdateStmt*                 _update;
    std::vector<pb::Expr>               _where_filters;
    int32_t                             _limit_count;
    std::vector<pb::SlotDescriptor>     _update_slots;
    std::vector<pb::Expr>               _update_values;
};
} //namespace baikal

