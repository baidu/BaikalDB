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
#include <rapidjson/reader.h>
#include <rapidjson/document.h>

namespace baikaldb {

class DDLPlanner : public LogicalPlanner {
public:

    DDLPlanner(QueryContext* ctx) : LogicalPlanner(ctx) {}

    virtual ~DDLPlanner() {}

    virtual int plan();

    static int parse_partition_pre_split_keys(const std::string& partition_split_keys, pb::SchemaInfo& table);

private:
    int parse_create_table(pb::SchemaInfo& table);
    int parse_drop_table(pb::SchemaInfo& table);
    int parse_restore_table(pb::SchemaInfo& table);
    int parse_create_database(pb::DataBaseInfo& database);
    int parse_drop_database(pb::DataBaseInfo& database);
    int parse_alter_table(pb::MetaManagerRequest& alter_request);
    int check_partition_key_constraint(pb::SchemaInfo& table, const std::string& field_name);

    int add_column_def(pb::SchemaInfo& table, parser::ColumnDef* column, bool is_unique_indicator);
    int add_constraint_def(pb::SchemaInfo& table, parser::Constraint* constraint,parser::AlterTableSpec* spec);
    bool is_fulltext_type_constraint(pb::StorageType pb_storage_type, bool& has_arrow_type, bool& has_pb_type) const;
    pb::PrimitiveType to_baikal_type(parser::FieldType* field_type);
    int parse_pre_split_keys(std::string split_start_key,
                             std::string split_end_key,
                             std::string global_start_key,
                             std::string global_end_key,
                             int32_t split_region_num, pb::SchemaInfo& table);
    int pre_split_index(const std::string& start_key,
                        const std::string& end_key,
                        int32_t region_num,
                        pb::SchemaInfo& table,
                        const pb::IndexInfo* pk_index,
                        const pb::IndexInfo* index,
                        const std::vector<const pb::FieldInfo*>& pk_fields,
                        const std::vector<const pb::FieldInfo*>& index_fields);
    int parse_modify_column(pb::MetaManagerRequest& alter_request,
                              const parser::TableName* table_name,
                              const parser::AlterTableSpec* alter_spec);
    static int fill_index_fields(const std::set<std::string>& index_field_names,
                          const std::vector<const pb::FieldInfo*>& index_fields,
                          const std::vector<const pb::FieldInfo*>& pk_index_fields,
                          const pb::IndexInfo* index,
                          MutTableKey& key);

    // Partition
    int parse_partition_range(pb::SchemaInfo& table, const parser::PartitionRange* p_partition_range);
    int parse_dynamic_partition_attr(pb::SchemaInfo& table, const rapidjson::Document& json);
    int parse_alter_partition(pb::MetaManagerRequest& alter_request, const parser::AlterTableSpec* p_alter_spec);

    std::map<std::string, bool> _column_can_null;

    static constexpr const char* PARITITON_PRE_SPLIT_FIELD = "userid";
};

} //namespace baikal
