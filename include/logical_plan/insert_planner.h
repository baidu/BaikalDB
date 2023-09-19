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

// Brief:  the class for generating insert SQL plan
#pragma once
#include "logical_planner.h"
#include "query_context.h"
#include "proto/plan.pb.h"

namespace baikaldb {

class InsertPlanner : public LogicalPlanner {
public:

    InsertPlanner(QueryContext* ctx) : LogicalPlanner(ctx) {}

    virtual ~InsertPlanner() {}

    virtual int plan();

private:
    int parse_db_table(pb::InsertNode* node);

    int parse_kv_list();

    int parse_field_list(pb::InsertNode* node);

    int parse_values_list(pb::InsertNode* node);

    int gen_select_plan();

    int parse_olap_sign_field_value(SmartRecord record);

    int fill_record_field(const parser::ExprNode* item, SmartRecord record, FieldInfo& field);

private:
    parser::InsertStmt*     _insert_stmt;
    int64_t                 _table_id;
    std::vector<FieldInfo>  _fields;
    std::vector<FieldInfo>  _default_fields;
    std::set<int>           _olap_uniq_field_ids;
    int                     _olap_sign_field_id = -1;

    std::vector<pb::SlotDescriptor>     _update_slots;
    std::vector<pb::Expr>               _update_values;
};
} //namespace baikal

