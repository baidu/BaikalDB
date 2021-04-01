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

#pragma once

#include "logical_planner.h"
#include "query_context.h"

namespace baikaldb {

class UnionPlanner : public LogicalPlanner {
public:
    UnionPlanner(QueryContext* ctx) :
        LogicalPlanner(ctx),
        _union_stmt(nullptr) {}

    UnionPlanner(QueryContext* ctx, const SmartPlanTableCtx& plan_state) : 
        LogicalPlanner(ctx, plan_state),
        _union_stmt(nullptr) {}

    virtual ~UnionPlanner() {}

    virtual int plan();

private:
    int gen_select_stmts_plan();
    void parse_dual_fields();
    int parse_dual_order_by();
    int create_common_plan_node();
    void create_union_node();
    int parse_limit();
    void create_dual_tuple_descs();

    bool is_literal(pb::Expr& expr) {
        switch (expr.nodes(0).node_type()) {
            case pb::NULL_LITERAL:
            case pb::BOOL_LITERAL:
            case pb::INT_LITERAL:
            case pb::DOUBLE_LITERAL:
            case pb::STRING_LITERAL:
            case pb::HLL_LITERAL:
            case pb::BITMAP_LITERAL:
            case pb::DATE_LITERAL:
            case pb::DATETIME_LITERAL:
            case pb::TIME_LITERAL:
            case pb::TIMESTAMP_LITERAL:
            case pb::PLACE_HOLDER_LITERAL:
                return true;
            default:
                return false;
        }
        return false;
    }

private:
    parser::UnionStmt*           _union_stmt = nullptr;
    int32_t                      _union_tuple_id = -1;
    pb::Expr                     _limit_offset;
    pb::Expr                     _limit_count;
    bool                         _is_distinct = false;
    std::vector<pb::PrimitiveType> _select_fields_type;
    std::map<std::string, int32_t> _name_slot_id_mapping;
};
} //namespace baikaldb