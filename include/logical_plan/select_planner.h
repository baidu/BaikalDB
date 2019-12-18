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

namespace baikaldb {

class SelectPlanner : public LogicalPlanner {
public:

    SelectPlanner(QueryContext* ctx) : 
        LogicalPlanner(ctx),
        _select(nullptr) {}

    virtual ~SelectPlanner() {}

    virtual int plan();

private:

    // methods to create plan nodes
    void create_dual_scan_node();

    int create_limit_node();

    int create_having_node();

    int create_agg_node();

    int parse_select_star(parser::SelectField* field);
    int parse_select_field(parser::SelectField* field);
    // method to parse SQL elements
    int parse_select_fields();

    void add_single_table_columns(TableInfo* table_info);

    int parse_where();

    int parse_groupby();

    int _parse_having();

    int parse_orderby();

    int parse_limit();
  
    bool is_full_export();

    // create tuples for table scan
    void create_agg_tuple_desc();

private:
    parser::SelectStmt*                 _select;

    std::vector<pb::SlotDescriptor>     _group_slots;

    std::vector<pb::Expr>   _where_filters;
    std::vector<pb::Expr>   _having_filters;
    std::vector<pb::Expr>   _group_exprs;

    pb::Expr                _limit_offset;
    pb::Expr                _limit_count;
};
} //namespace baikal

