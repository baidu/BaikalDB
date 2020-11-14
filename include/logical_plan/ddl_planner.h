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

// Brief:  the class for generating and executing DDL SQL
#pragma once
#include "logical_planner.h"
#include "query_context.h"
#include "parser.h"

namespace baikaldb {

class DDLPlanner : public LogicalPlanner {
public:

    DDLPlanner(QueryContext* ctx) : LogicalPlanner(ctx) {}

    virtual ~DDLPlanner() {}

    virtual int plan();

private:
    int parse_create_table(pb::SchemaInfo& table);
    int parse_drop_table(pb::SchemaInfo& table);
    int parse_restore_table(pb::SchemaInfo& table);
    int parse_create_database(pb::DataBaseInfo& database);
    int parse_drop_database(pb::DataBaseInfo& database);
    int parse_alter_table(pb::MetaManagerRequest& alter_request);

    int add_column_def(pb::SchemaInfo& table, parser::ColumnDef* column);
    int add_constraint_def(pb::SchemaInfo& table, parser::Constraint* constraint);
    bool is_fulltext_type_constraint(pb::StorageType pb_storage_type, bool& has_arrow_type, bool& has_pb_type) const;
    pb::PrimitiveType to_baikal_type(parser::FieldType* field_type);
};
} //namespace baikal
