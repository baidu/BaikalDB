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

#include "dms.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace baikaldb {

TEST(test_dms_utils, case_sql_rewrite) {
    {
        parser::SqlParser parser;
        const std::string& sql = "UPDATE tbl SET k1 = v1, k2 = v2 WHERE k3 = v3 and k4 = v4";
        parser.parse(sql);
        parser::StmtNode* stmt = parser.result[0];

        std::string rewrite_sql;
        EXPECT_EQ(SqlRewrite::rewrite_to_select(stmt, { "COUNT(*)" }, sql, rewrite_sql), 0);
        EXPECT_EQ(rewrite_sql, "SELECT COUNT(*) FROM  tbl WHERE k3 = v3 and k4 = v4");

        std::vector<std::string> select_fields {"k1", "k2"};
        EXPECT_EQ(SqlRewrite::rewrite_to_select(stmt, select_fields, sql, rewrite_sql), 0);
        EXPECT_EQ(rewrite_sql, "SELECT k1,k2 FROM  tbl WHERE k3 = v3 and k4 = v4");
    }
    {
        parser::SqlParser parser;
        const std::string& sql = "DELETE FROM tbl WHERE k3 = v3 and k4 = v4";
        parser.parse(sql);
        parser::StmtNode* stmt = parser.result[0];

        std::string rewrite_sql;
        EXPECT_EQ(SqlRewrite::rewrite_to_select(stmt, { "COUNT(*)" }, sql, rewrite_sql), 0);
        EXPECT_EQ(rewrite_sql, "SELECT COUNT(*) FROM tbl WHERE k3 = v3 and k4 = v4");

        std::vector<std::string> select_fields {"k1", "k2"};
        EXPECT_EQ(SqlRewrite::rewrite_to_select(stmt, select_fields, sql, rewrite_sql), 0);
        EXPECT_EQ(rewrite_sql, "SELECT k1,k2 FROM tbl WHERE k3 = v3 and k4 = v4");
    }
    {
        parser::SqlParser parser;
        const std::string& sql = "DELETE FROM tbl\nWHERE k3 = v3 and k4 = v4";
        parser.parse(sql);
        parser::StmtNode* stmt = parser.result[0];

        std::string rewrite_sql;
        EXPECT_EQ(SqlRewrite::rewrite_to_select(stmt, { "COUNT(*)" }, sql, rewrite_sql), 0);
        EXPECT_EQ(rewrite_sql, "SELECT COUNT(*) FROM tbl WHERE k3 = v3 and k4 = v4");
    }
    {
        parser::SqlParser parser;
        const std::string& sql = "INSERT INTO tbl values (1,2,3)";
        parser.parse(sql);
        parser::StmtNode* stmt = parser.result[0];

        std::string rewrite_sql;
        EXPECT_NE(SqlRewrite::rewrite_to_select(stmt, { "COUNT(*)" }, sql, rewrite_sql), 0);
    }
}

TEST(test_dms_utils, case_sql_split) {
    std::string text = "select * from t1; select * from t2;"
                       "select * from t3 where col = ';test;';"
                       "select * from t4 /* ;test; */;"
                       "select * from t5;"
                       "select * from t6;"
                       "select * from t7 where col = '\"test\"';"
                       "select * from t8 where col = '\\';test;\\'';"
                       "select * from t9 where col = 'test';"
                       "select * from t10 where `col` = 'test';"
                       "select * from t11; select * from t12;"
                       "# ;test;\n"               // 跳过注释
                       " -- ;test;\n"             // 跳过注释
                       "/* this is a comment */;" // 跳过注释
                       "- this is - not a- comment;"
                       "/ this // is /// not / a/ comment/;"
                       "/* this is not a comment  ;";
    std::vector<std::string> sqls;
    SqlSplit splitter;
    EXPECT_EQ(splitter.split_sql(text, sqls), 0);

    EXPECT_EQ(sqls[0], "select * from t1");
    EXPECT_EQ(sqls[1], "select * from t2");
    EXPECT_EQ(sqls[2], "select * from t3 where col = ';test;'");
    EXPECT_EQ(sqls[3], "select * from t4 /* ;test; */");
    EXPECT_EQ(sqls[4], "select * from t5");
    EXPECT_EQ(sqls[5], "select * from t6");
    EXPECT_EQ(sqls[6], "select * from t7 where col = '\"test\"'");
    EXPECT_EQ(sqls[7], "select * from t8 where col = '\\';test;\\''");
    EXPECT_EQ(sqls[8], "select * from t9 where col = 'test'");
    EXPECT_EQ(sqls[9], "select * from t10 where `col` = 'test'");
    EXPECT_EQ(sqls[10], "select * from t11");
    EXPECT_EQ(sqls[11], "select * from t12");
    EXPECT_EQ(sqls[12], "- this is - not a- comment");
    EXPECT_EQ(sqls[13], "/ this // is /// not / a/ comment/");
    EXPECT_EQ(sqls[14], "/* this is not a comment  ;");
}

TEST(test_dms_utils, case_sql_split_slash) {
    std::string text = "update tbl set col = floor(col*100.0)/100.0 where col1 = 2773080 and col2 = 70.466425;\n"
                       "update tbl set col = floor(col*100.0)/100.0 where col1 = 2773080 and col2 = 70.466426;\n"
                       "update tbl set col = floor(col*100.0)/100.0 where col1 = 2773080 and col2 = 70.466427;";
    std::vector<std::string> sqls;
    SqlSplit splitter;
    EXPECT_EQ(splitter.split_sql(text, sqls), 0);
    EXPECT_EQ((int)sqls.size(), 3);
    EXPECT_EQ(sqls[0], "update tbl set col = floor(col*100.0)/100.0 where col1 = 2773080 and col2 = 70.466425");
    EXPECT_EQ(sqls[1], "update tbl set col = floor(col*100.0)/100.0 where col1 = 2773080 and col2 = 70.466426");
    EXPECT_EQ(sqls[2], "update tbl set col = floor(col*100.0)/100.0 where col1 = 2773080 and col2 = 70.466427");
}

TEST(test_dms, case_sql_check) {
    pb::DMSRequest request;
    pb::DMSResponse response;

    TaskSession task_session(&request, &response, nullptr, nullptr);
    TaskSession::SmartTaskTableInfo task_table_info(new TaskSession::TaskTableInfo());
    ASSERT_EQ(task_table_info != nullptr, true);
    task_table_info->db = "test_db";
    task_table_info->table = "test_table";
    task_table_info->fields = {"field1", "field2", "field3"};
    task_table_info->pk_fields = {"field1", "field2"};
    task_session.set_table_info(std::make_pair("test_db", "test_table"), task_table_info);

    std::string errmsg;

    // INSERT
    {
        std::string sql = "INSERT INTO test_db.test_table (field1, field2) VALUES (\"field1_val\", \"field2_val\") "
                          "ON DUPLICATE KEY UPDATE field3 = field3 + 1;";
        parser::SqlParser parser;
        parser::StmtNode* stmt = nullptr;
        EXPECT_EQ(task_session.parse_sql(parser, sql, stmt), 0);
        ASSERT_EQ(stmt != nullptr, true);
        EXPECT_EQ(task_session.check_insert(stmt, task_table_info, errmsg), 0);
    }
    {
        // 表不存在列field4
        std::string sql = "INSERT INTO test_db.test_table (field1, field4) VALUES (\"field1_val\", \"field4_val\");";
        parser::SqlParser parser;
        parser::StmtNode* stmt = nullptr;
        EXPECT_EQ(task_session.parse_sql(parser, sql, stmt), 0);
        ASSERT_EQ(stmt != nullptr, true);
        EXPECT_NE(task_session.check_insert(stmt, task_table_info, errmsg), 0);
    }
    {
        // 表不存在列field5
        std::string sql = "INSERT INTO test_db.test_table (field1, field2) VALUES (\"field1_val\", \"field2_val\") "
                          "ON DUPLICATE KEY UPDATE field5 = field5 + 1;";
        parser::SqlParser parser;
        parser::StmtNode* stmt = nullptr;
        EXPECT_EQ(task_session.parse_sql(parser, sql, stmt), 0);
        ASSERT_EQ(stmt != nullptr, true);
        EXPECT_NE(task_session.check_insert(stmt, task_table_info, errmsg), 0);
    }
    {
        // 不支持带子查询的INSERT
        std::string sql = "INSERT INTO test_db.test_table SELECT * FROM test_db.test_table;";
        parser::SqlParser parser;
        parser::StmtNode* stmt = nullptr;
        EXPECT_EQ(task_session.parse_sql(parser, sql, stmt), 0);
        ASSERT_EQ(stmt != nullptr, true);
        EXPECT_NE(task_session.check_insert(stmt, task_table_info, errmsg), 0);
    }

    // DELETE
    {
        std::string sql = "DELETE FROM test_db.test_table WHERE field1 = \"field1_val\" AND field2 = \"field2_val\";";
        parser::SqlParser parser;
        parser::StmtNode* stmt = nullptr;
        EXPECT_EQ(task_session.parse_sql(parser, sql, stmt), 0);
        ASSERT_EQ(stmt != nullptr, true);
        EXPECT_EQ(task_session.check_delete(stmt, task_table_info, errmsg), 0);
    }
    {
        // 表不存在列field4
        std::string sql = "DELETE FROM test_db.test_table WHERE field1 = \"field1_val\" AND field4 = \"field4_val\";";
        parser::SqlParser parser;
        parser::StmtNode* stmt = nullptr;
        EXPECT_EQ(task_session.parse_sql(parser, sql, stmt), 0);
        ASSERT_EQ(stmt != nullptr, true);
        EXPECT_NE(task_session.check_delete(stmt, task_table_info, errmsg), 0);
    }
    {
        // 不支持ORDER BY / LIMIT
        std::string sql = "DELETE FROM test_db.test_table WHERE field1 = \"field1_val\" AND field2 = \"field2_val\" "
                          "ORDER BY field1 LIMIT 10;";
        parser::SqlParser parser;
        parser::StmtNode* stmt = nullptr;
        EXPECT_EQ(task_session.parse_sql(parser, sql, stmt), 0);
        ASSERT_EQ(stmt != nullptr, true);
        EXPECT_NE(task_session.check_delete(stmt, task_table_info, errmsg), 0);
    }
    {
        // 不存在WHERE条件
        std::string sql = "DELETE FROM test_db.test_table;";
        parser::SqlParser parser;
        parser::StmtNode* stmt = nullptr;
        EXPECT_EQ(task_session.parse_sql(parser, sql, stmt), 0);
        ASSERT_EQ(stmt != nullptr, true);
        EXPECT_NE(task_session.check_delete(stmt, task_table_info, errmsg), 0);
    }
    {
        // WHERE条件包含操作数都为常量
        std::string sql = "DELETE FROM test_db.test_table where 1 = 1;";
        parser::SqlParser parser;
        parser::StmtNode* stmt = nullptr;
        EXPECT_EQ(task_session.parse_sql(parser, sql, stmt), 0);
        ASSERT_EQ(stmt != nullptr, true);
        EXPECT_NE(task_session.check_delete(stmt, task_table_info, errmsg), 0);
    }
    {
        // WHERE条件包含操作数都为常数
        std::string sql = "DELETE FROM test_db.test_table where 1;";
        parser::SqlParser parser;
        parser::StmtNode* stmt = nullptr;
        EXPECT_EQ(task_session.parse_sql(parser, sql, stmt), 0);
        ASSERT_EQ(stmt != nullptr, true);
        EXPECT_NE(task_session.check_delete(stmt, task_table_info, errmsg), 0);
    }
    {
        // WHERE条件包含操作数都为常数
        std::string sql = "DELETE FROM test_db.test_table where 1 + 1;";
        parser::SqlParser parser;
        parser::StmtNode* stmt = nullptr;
        EXPECT_EQ(task_session.parse_sql(parser, sql, stmt), 0);
        ASSERT_EQ(stmt != nullptr, true);
        EXPECT_NE(task_session.check_delete(stmt, task_table_info, errmsg), 0);
    }
    {
        // WHERE条件包含操作数都为常数
        std::string sql = "DELETE FROM test_db.test_table where 1 > 0;";
        parser::SqlParser parser;
        parser::StmtNode* stmt = nullptr;
        EXPECT_EQ(task_session.parse_sql(parser, sql, stmt), 0);
        ASSERT_EQ(stmt != nullptr, true);
        EXPECT_NE(task_session.check_delete(stmt, task_table_info, errmsg), 0);
    }
 
    // UPDATE
    {
        std::string sql = "UPDATE test_db.test_table SET field3 = \"field3_val\" "
                          "WHERE field1 = \"field1_val\" AND field2 = \"field2_val\";";
        parser::SqlParser parser;
        parser::StmtNode* stmt = nullptr;
        EXPECT_EQ(task_session.parse_sql(parser, sql, stmt), 0);
        ASSERT_EQ(stmt != nullptr, true);
        EXPECT_EQ(task_session.check_update(stmt, task_table_info, errmsg), 0);
    }
    {
        // 表不存在列field4
        std::string sql = "UPDATE test_db.test_table SET field3 = \"field3_val\" "
              "WHERE field1 = \"field1_val\" AND field4 = \"field4_val\";";
        parser::SqlParser parser;
        parser::StmtNode* stmt = nullptr;
        EXPECT_EQ(task_session.parse_sql(parser, sql, stmt), 0);
        ASSERT_EQ(stmt != nullptr, true);
        EXPECT_NE(task_session.check_update(stmt, task_table_info, errmsg), 0);
    }
    {
        // 表不存在列field5
        std::string sql = "UPDATE test_db.test_table SET field5 = \"field5_val\" "
              "WHERE field1 = \"field1_val\" AND field4 = \"field4_val\";";
        parser::SqlParser parser;
        parser::StmtNode* stmt = nullptr;
        EXPECT_EQ(task_session.parse_sql(parser, sql, stmt), 0);
        ASSERT_EQ(stmt != nullptr, true);
        EXPECT_NE(task_session.check_update(stmt, task_table_info, errmsg), 0);
    }
    {
        // 不支持ORDER BY / LIMIT
        std::string sql = "UPDATE test_db.test_table SET field3 = \"field3_val\" "
              "WHERE field1 = \"field1_val\" AND field2 = \"field2_val\" "
              "ORDER BY field1 LIMIT 10;";
        parser::SqlParser parser;
        parser::StmtNode* stmt = nullptr;
        EXPECT_EQ(task_session.parse_sql(parser, sql, stmt), 0);
        ASSERT_EQ(stmt != nullptr, true);
        EXPECT_NE(task_session.check_update(stmt, task_table_info, errmsg), 0);
    }
    {
        // Update Sql幂等检查
        std::string sql = "UPDATE test_db.test_table SET field3 = \"field3_val\" "
              "WHERE field1 = \"field1_val\" AND field4 = \"field4_val\";";
        parser::SqlParser parser;
        parser::StmtNode* stmt = nullptr;
        EXPECT_EQ(task_session.parse_sql(parser, sql, stmt), 0);
        ASSERT_EQ(stmt != nullptr, true);
        EXPECT_EQ(task_session.check_idempotent(stmt, errmsg), 0);
    }
    {
        // Update Sql幂等检查
        std::string sql = "UPDATE test_db.test_table SET field3 = field3 + 1 "
              "WHERE field1 = \"field1_val\" AND field4 = \"field4_val\";";
        parser::SqlParser parser;
        parser::StmtNode* stmt = nullptr;
        parser::SqlParser new_parser;
        EXPECT_EQ(task_session.parse_sql(parser, sql, stmt), 0);
        ASSERT_EQ(stmt != nullptr, true);
        EXPECT_NE(task_session.check_idempotent(stmt, errmsg), 0);
    }
}

} // namespace baikaldb