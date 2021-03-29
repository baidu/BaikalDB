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

// Brief:  the class for generating and executing prepare statements
#pragma once
#include "logical_planner.h"
#include "query_context.h"
#include "parser.h"

namespace baikaldb {

class LoadPlanner : public LogicalPlanner {
public:

    LoadPlanner(QueryContext* ctx) : LogicalPlanner(ctx) {}

    virtual ~LoadPlanner() {}

    virtual int plan();

private:
    int parse_load_info(pb::LoadNode* load_node, pb::InsertNode* insert_node);
    int parse_field_list(pb::LoadNode* node, pb::InsertNode* insert_node);
    int parse_set_list(pb::LoadNode* node);

private:
    int64_t                   _table_id = 0;
    parser::LoadDataStmt*     _load_stmt = nullptr;
    std::vector<pb::SlotDescriptor>     _set_slots;
    std::vector<pb::Expr>               _set_values;
};
} //namespace baikal
