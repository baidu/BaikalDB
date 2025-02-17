// Copyright (c) 2022 Baidu, Inc. All Rights Reserved.
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

#include <gtest/gtest.h>

#include "ddl_planner.h"
#include "parser.h"
#include "schema_factory.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace baikaldb {

// SQL Parser
TEST(test_parser, case_create_table_partition) {
    // LESS THAN
    {
        parser::SqlParser parser;
        std::string sql = 
            "CREATE TABLE `tbl_partition` ("
                "`userid` bigint(20) unsigned NOT NULL,"
                "`dt` DATE NOT NULL,"
                "`title` VARCHAR(256) NOT NULL,"
                "PRIMARY KEY (`userid`, `dt`)"
            ") ENGINE=Rocksdb DEFAULT CHARSET=gbk AVG_ROW_LENGTH=200 "
            "COMMENT='{\"comment\":\"\", \"resource_tag\":\"e0-nj\", \"namespace\":\"test_namespace\"}' "
            "PARTITION BY RANGE (dt) ("
                "PARTITION p202301 VALUES LESS THAN ('2023-02-01'),"
                "PARTITION p202302 VALUES LESS THAN ('2023-03-01')"
            ");";

        parser.parse(sql);
        ASSERT_EQ(parser.result.size(), 1);
        ASSERT_EQ(parser.result[0]->node_type, parser::NT_CREATE_TABLE);

        bool has_partition_option = false;
        parser::CreateTableStmt* stmt = (parser::CreateTableStmt*)parser.result[0];
        ASSERT_NE(stmt, nullptr);
        for (size_t i = 0; i < stmt->options.size(); ++i) {
            if (stmt->options[i] == nullptr) {
                continue;
            }
            if (stmt->options[i]->type != parser::TABLE_OPT_PARTITION) {
                continue;
            }
            parser::TablePartitionOption* p_option = static_cast<parser::TablePartitionOption*>(stmt->options[i]);
            has_partition_option = true;
            EXPECT_EQ(p_option->type, parser::PARTITION_RANGE);
            ASSERT_EQ(p_option->ranges.size(), 2);
            
            ASSERT_NE(p_option->ranges[0], nullptr);
            EXPECT_EQ(p_option->ranges[0]->name.to_string(), "p202301");
            EXPECT_EQ(p_option->ranges[0]->less_expr->to_string(), "2023-02-01");

            ASSERT_NE(p_option->ranges[1], nullptr);
            EXPECT_EQ(p_option->ranges[1]->name.to_string(), "p202302");
            EXPECT_EQ(p_option->ranges[1]->less_expr->to_string(), "2023-03-01");
        }
        EXPECT_EQ(has_partition_option, true);
    }

    // RANGE
    {
        parser::SqlParser parser;
        std::string sql = 
            "CREATE TABLE `tbl_partition` ("
                "`userid` bigint(20) unsigned NOT NULL,"
                "`dt` DATE NOT NULL,"
                "`title` VARCHAR(256) NOT NULL,"
                "PRIMARY KEY (`userid`, `dt`)"
            ") ENGINE=Rocksdb DEFAULT CHARSET=gbk AVG_ROW_LENGTH=200 "
            "COMMENT='{\"comment\":\"\", \"resource_tag\":\"e0-nj\", \"namespace\":\"test_namespace\"}' "
            "PARTITION BY RANGE (dt) ("
                "PARTITION p202301 VALUES ['2023-01-01', '2023-02-01'),"
                "PARTITION p202302 VALUES ['2023-02-01', '2023-03-01')"
            ");";

        parser.parse(sql);
        ASSERT_EQ(parser.result.size(), 1);
        ASSERT_EQ(parser.result[0]->node_type, parser::NT_CREATE_TABLE);

        bool has_partition_option = false;
        parser::CreateTableStmt* stmt = (parser::CreateTableStmt*)parser.result[0];
        ASSERT_NE(stmt, nullptr);
        for (size_t i = 0; i < stmt->options.size(); ++i) {
            if (stmt->options[i] == nullptr) {
                continue;
            }
            if (stmt->options[i]->type != parser::TABLE_OPT_PARTITION) {
                continue;
            }
            parser::TablePartitionOption* p_option = static_cast<parser::TablePartitionOption*>(stmt->options[i]);
            has_partition_option = true;
            EXPECT_EQ(p_option->type, parser::PARTITION_RANGE);
            ASSERT_EQ(p_option->ranges.size(), 2);
            
            ASSERT_NE(p_option->ranges[0], nullptr);
            EXPECT_EQ(p_option->ranges[0]->name.to_string(), "p202301");
            EXPECT_EQ(p_option->ranges[0]->range.first->to_string(), "2023-01-01");
            EXPECT_EQ(p_option->ranges[0]->range.second->to_string(), "2023-02-01");

            ASSERT_NE(p_option->ranges[1], nullptr);
            EXPECT_EQ(p_option->ranges[1]->name.to_string(), "p202302");
            EXPECT_EQ(p_option->ranges[1]->range.first->to_string(), "2023-02-01");
            EXPECT_EQ(p_option->ranges[1]->range.second->to_string(), "2023-03-01");
        }
        EXPECT_EQ(has_partition_option, true);
    }
}

TEST(test_parser, case_add_partition) {
    parser::SqlParser parser;

    std::string sql = 
        "ALTER TABLE tbl_partition ADD PARTITION p202304 VALUES ['2023-04-01', '2023-05-01') "
        "COMMENT='{\"resource_tag\":\"e0-nj\"}'";
    
    parser.parse(sql);
    ASSERT_EQ(parser.result.size(), 1);
    ASSERT_EQ(parser.result[0]->node_type, parser::NT_ALTER_TABLE);

    parser::AlterTableStmt* stmt = (parser::AlterTableStmt*)parser.result[0];
    ASSERT_NE(stmt, nullptr);
    EXPECT_EQ(stmt->alter_specs.size(), 1);
    ASSERT_NE(stmt->alter_specs[0], nullptr);
    ASSERT_NE(stmt->alter_specs[0]->partition_range, nullptr);
    EXPECT_EQ(stmt->alter_specs[0]->partition_range->name.to_string(), "p202304");
    EXPECT_EQ(stmt->alter_specs[0]->partition_range->range.first->to_string(), "2023-04-01");
    EXPECT_EQ(stmt->alter_specs[0]->partition_range->range.second->to_string(), "2023-05-01");

    ASSERT_EQ(stmt->alter_specs[0]->partition_range->options.size(), 1);
    ASSERT_NE(stmt->alter_specs[0]->partition_range->options[0], nullptr);
    EXPECT_EQ(stmt->alter_specs[0]->partition_range->options[0]->type, parser::PARTITION_OPT_COMMENT);
    EXPECT_EQ(stmt->alter_specs[0]->partition_range->options[0]->str_value.to_string(), 
              "{\"resource_tag\":\"e0-nj\"}");
}

TEST(test_parser, case_drop_partition) {
    parser::SqlParser parser;

    std::string sql = "ALTER TABLE tbl_partition DROP PARTITION p202304;";

    parser.parse(sql);
    ASSERT_EQ(parser.result.size(), 1);
    ASSERT_EQ(parser.result[0]->node_type, parser::NT_ALTER_TABLE);

    parser::AlterTableStmt* stmt = (parser::AlterTableStmt*)parser.result[0];
    ASSERT_NE(stmt, nullptr);
    ASSERT_EQ(stmt->alter_specs.size(), 1);
    ASSERT_NE(stmt->alter_specs[0], nullptr);
    ASSERT_NE(stmt->alter_specs[0]->partition_range, nullptr);
    EXPECT_EQ(stmt->alter_specs[0]->partition_range->name.to_string(), "p202304");
}

TEST(test_parser, case_modify_partition) {
    parser::SqlParser parser;

    std::string sql = 
        "ALTER TABLE tbl_partition MODIFY PARTITION p202304 COMMENT='{\"resource_tag\":\"e0-nj\"}';";
    
    parser.parse(sql);
    ASSERT_EQ(parser.result.size(), 1);
    ASSERT_EQ(parser.result[0]->node_type, parser::NT_ALTER_TABLE);

    parser::AlterTableStmt* stmt = (parser::AlterTableStmt*)parser.result[0];
    ASSERT_NE(stmt, nullptr);
    ASSERT_EQ(stmt->alter_specs.size(), 1);
    ASSERT_NE(stmt->alter_specs[0], nullptr);
    ASSERT_NE(stmt->alter_specs[0]->partition_range, nullptr);
    EXPECT_EQ(stmt->alter_specs[0]->partition_range->name.to_string(), "p202304");

    ASSERT_EQ(stmt->alter_specs[0]->partition_range->options.size(), 1);
    ASSERT_NE(stmt->alter_specs[0]->partition_range->options[0], nullptr);
    EXPECT_EQ(stmt->alter_specs[0]->partition_range->options[0]->type, parser::PARTITION_OPT_COMMENT);
    EXPECT_EQ(stmt->alter_specs[0]->partition_range->options[0]->str_value.to_string(), 
              "{\"resource_tag\":\"e0-nj\"}");
}

// DDL Planner
TEST(test_ddl_planner, case_create_table_partition) {
    // LESS THAN
    {
        parser::SqlParser parser;
        std::string sql = 
            "CREATE TABLE `tbl_partition` ("
                "`userid` bigint(20) unsigned NOT NULL,"
                "`dt` DATE NOT NULL,"
                "`title` VARCHAR(256) NOT NULL,"
                "PRIMARY KEY (`userid`,`dt`)"
            ") ENGINE=Rocksdb DEFAULT CHARSET=gbk AVG_ROW_LENGTH=200 "
            "COMMENT='{\"comment\":\"\", \"resource_tag\":\"e0-nj\", \"namespace\":\"test_namespace\"}' "
            "PARTITION BY RANGE (dt) ("
                "PARTITION p202301 VALUES LESS THAN ('2023-02-01'),"
                "PARTITION p202302 VALUES LESS THAN ('2023-03-01')"
            ");";

        parser.parse(sql);
        ASSERT_EQ(parser.result.size(), 1);
        ASSERT_EQ(parser.result[0]->node_type, parser::NT_CREATE_TABLE);

        parser::CreateTableStmt* stmt = (parser::CreateTableStmt*)parser.result[0];
        ASSERT_NE(stmt, nullptr);

        stmt->table_name->db = "test_db";
        QueryContext ctx;
        ctx.stmt = stmt;
        DDLPlanner planner(&ctx);

        pb::SchemaInfo table;
        table.set_namespace_name("test_namespace");
        planner.parse_create_table(table);
        const pb::PartitionInfo& partition_info = table.partition_info();
        ASSERT_EQ(partition_info.range_partition_infos_size(), 2);
        EXPECT_EQ(partition_info.range_partition_infos(0).partition_name(), "p202301");
        ASSERT_EQ(partition_info.range_partition_infos(0).less_value().nodes_size(), 1);
        EXPECT_EQ(partition_info.range_partition_infos(0).less_value().nodes(0).derive_node().string_val(), 
                  "2023-02-01");

        EXPECT_EQ(partition_info.range_partition_infos(1).partition_name(), "p202302");
        ASSERT_EQ(partition_info.range_partition_infos(1).less_value().nodes_size(), 1);
        EXPECT_EQ(partition_info.range_partition_infos(1).less_value().nodes(0).derive_node().string_val(), 
                  "2023-03-01");
    }
    // Range
    {
        parser::SqlParser parser;
        std::string sql = 
            "CREATE TABLE `tbl_partition` ("
                "`userid` bigint(20) unsigned NOT NULL,"
                "`dt` DATE NOT NULL,"
                "`title` VARCHAR(256) NOT NULL,"
                "PRIMARY KEY (`userid`,`dt`)"
            ") ENGINE=Rocksdb DEFAULT CHARSET=gbk AVG_ROW_LENGTH=200 "
            "COMMENT='{\"comment\":\"\", \"resource_tag\":\"e0-nj\", \"namespace\":\"test_namespace\"}' "
            "PARTITION BY RANGE (dt) ("
                "PARTITION p202301 VALUES ['2023-01-01', '2023-02-01'),"
                "PARTITION p202302 VALUES ['2023-02-01', '2023-03-01')"
            ");";

        parser.parse(sql);
        ASSERT_EQ(parser.result.size(), 1);
        ASSERT_EQ(parser.result[0]->node_type, parser::NT_CREATE_TABLE);

        parser::CreateTableStmt* stmt = (parser::CreateTableStmt*)parser.result[0];
        ASSERT_NE(stmt, nullptr);

        stmt->table_name->db = "test_db";
        QueryContext ctx;
        ctx.stmt = stmt;
        DDLPlanner planner(&ctx);

        pb::SchemaInfo table;
        table.set_namespace_name("test_namespace");
        planner.parse_create_table(table);
        const pb::PartitionInfo& partition_info = table.partition_info();
        ASSERT_EQ(partition_info.range_partition_infos_size(), 2);

        EXPECT_EQ(partition_info.range_partition_infos(0).partition_name(), "p202301");
        ASSERT_EQ(partition_info.range_partition_infos(0).range().left_value().nodes_size(), 1);
        EXPECT_EQ(partition_info.range_partition_infos(0).range().left_value().nodes(0).derive_node().string_val(), 
                  "2023-01-01");
        ASSERT_EQ(partition_info.range_partition_infos(0).range().right_value().nodes_size(), 1);
        EXPECT_EQ(partition_info.range_partition_infos(0).range().right_value().nodes(0).derive_node().string_val(), 
                  "2023-02-01");
        
        ASSERT_EQ(partition_info.range_partition_infos(1).range().left_value().nodes_size(), 1);
        EXPECT_EQ(partition_info.range_partition_infos(1).range().left_value().nodes(0).derive_node().string_val(), 
                  "2023-02-01");
        ASSERT_EQ(partition_info.range_partition_infos(1).range().right_value().nodes_size(), 1);
        EXPECT_EQ(partition_info.range_partition_infos(1).range().right_value().nodes(0).derive_node().string_val(), 
                  "2023-03-01");
    }
}

TEST(test_ddl_planner, case_add_partition) {
    parser::SqlParser parser;

    std::string sql = 
        "ALTER TABLE tbl_partition ADD PARTITION p202304 VALUES ['2023-04-01', '2023-05-01') "
        "COMMENT='{\"resource_tag\":\"e0-nj\"}'";

    parser.parse(sql);
    ASSERT_EQ(parser.result.size(), 1);
    ASSERT_EQ(parser.result[0]->node_type, parser::NT_ALTER_TABLE);

    parser::AlterTableStmt* stmt = (parser::AlterTableStmt*)parser.result[0];
    ASSERT_NE(stmt, nullptr);
    parser::AlterTableSpec* spec = stmt->alter_specs[0];
    ASSERT_NE(spec, nullptr);

    DDLPlanner planner(nullptr);
    pb::MetaManagerRequest alter_request;
    EXPECT_EQ(planner.parse_alter_partition(alter_request, spec), 0);

    const pb::PartitionInfo& partition_info = alter_request.table_info().partition_info();
    ASSERT_EQ(partition_info.range_partition_infos_size(), 1);

    EXPECT_EQ(partition_info.range_partition_infos(0).partition_name(), "p202304");
    ASSERT_EQ(partition_info.range_partition_infos(0).range().left_value().nodes_size(), 1);
    EXPECT_EQ(partition_info.range_partition_infos(0).range().left_value().nodes(0).derive_node().string_val(), 
                "2023-04-01");
    ASSERT_EQ(partition_info.range_partition_infos(0).range().right_value().nodes_size(), 1);
    EXPECT_EQ(partition_info.range_partition_infos(0).range().right_value().nodes(0).derive_node().string_val(), 
                "2023-05-01");
}

TEST(test_ddl_planner, case_drop_partition) {
    parser::SqlParser parser;

    std::string sql = "ALTER TABLE tbl_partition DROP PARTITION p202304;";

    parser.parse(sql);
    ASSERT_EQ(parser.result.size(), 1);
    ASSERT_EQ(parser.result[0]->node_type, parser::NT_ALTER_TABLE);

    parser::AlterTableStmt* stmt = (parser::AlterTableStmt*)parser.result[0];
    ASSERT_NE(stmt, nullptr);
    parser::AlterTableSpec* spec = stmt->alter_specs[0];
    ASSERT_NE(spec, nullptr);

    DDLPlanner planner(nullptr);
    pb::MetaManagerRequest alter_request;
    EXPECT_EQ(planner.parse_alter_partition(alter_request, spec), 0);

    const pb::PartitionInfo& partition_info = alter_request.table_info().partition_info();
    ASSERT_EQ(partition_info.range_partition_infos_size(), 1);
    EXPECT_EQ(partition_info.range_partition_infos(0).partition_name(), "p202304");
}

TEST(test_ddl_planner, case_modify_partition) {
    parser::SqlParser parser;

    std::string sql = 
        "ALTER TABLE tbl_partition MODIFY PARTITION p202304 COMMENT='{\"resource_tag\":\"e0-nj\"}';";

    parser.parse(sql);
    ASSERT_EQ(parser.result.size(), 1);
    ASSERT_EQ(parser.result[0]->node_type, parser::NT_ALTER_TABLE);

    parser::AlterTableStmt* stmt = (parser::AlterTableStmt*)parser.result[0];
    ASSERT_NE(stmt, nullptr);
    parser::AlterTableSpec* spec = stmt->alter_specs[0];
    ASSERT_NE(spec, nullptr);

    DDLPlanner planner(nullptr);
    pb::MetaManagerRequest alter_request;
    EXPECT_EQ(planner.parse_alter_partition(alter_request, spec), 0);

    const pb::PartitionInfo& partition_info = alter_request.table_info().partition_info();
    ASSERT_EQ(partition_info.range_partition_infos_size(), 1);
    EXPECT_EQ(partition_info.range_partition_infos(0).partition_name(), "p202304");
    EXPECT_EQ(partition_info.range_partition_infos(0).resource_tag(), "e0-nj");
}

} // namespace baikaldb
