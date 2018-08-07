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

#include <gtest/gtest.h>
#include <climits>
#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include "parser.h"

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace parser {

TEST(test_parser, case_all) {
    uint32_t xx2 = (uint32_t)-1 % 23;
    std::cout << xx2 << ":aaa\n";
    parser::SqlParser parser;
    std::string sql = "insert into t1 (a,b) values (1,2),(3,4+2);select 1;";
    std::string sql2 = "insert \n \n in to t1 (a,b) values (1,1),(now(), (1+((2+3))));";
    parser.parse(sql);
    std::cout << "sql:" << sql2 << std::endl;
    if (parser.error != parser::SUCC) {
        std::cout <<  parser.result.size() << "error:" << parser.syntax_err_str << std::endl;
        return;
    }
    InsertStmt* stmt = (InsertStmt*)parser.result[0];
    std::cout << stmt->lists.size() << std::endl;
    for (int i = 0; i < stmt->lists.size(); i++) {
        print_stmt(stmt->lists[i]);
    }
}

TEST(test_parser, case_create_table) {
    parser::SqlParser parser;
    
    std::string sql = "create table score_diary_book ("
        "`book_id` bigint(20) NOT NULL COMMENT '日记本ID',"
        "`parent_id` bigint(20) NOT NULL COMMENT '父ID',"
        "`score_type` int(11) NOT NULL COMMENT '1-日记本净分数 2-日记本总分',"
        "`score` double NOT NULL COMMENT '分数',"
        "`level` int(11) NOT NULL COMMENT '级别 0-不合格 1-普通 2-优秀 3-超优秀',"
        "`state` int(11) NOT NULL COMMENT '状态 1-审核通过 2-审核拒绝 0-审核中 ',"
        "`update_time` datetime NOT NULL,"
        "`create_time` datetime NOT NULL,"
        "PRIMARY KEY (book_id,score_type),"
        "KEY score_type (score_type),"
        "KEY level (level),"
        "KEY state (state)"
    ") ENGINE=Rocksdb DEFAULT CHARSET=gbk AVG_ROW_LENGTH=500 COMMENT='{\"comment\":"", \"resource_tag\":\"e0-nj\", \"namespace\":\"FENGCHAO\"}'";

    parser.parse(sql);
    //EXPECT_EQ(parser::SUCC, parser.error);
    printf("errormsg: %d, %s\n", parser.error, parser.syntax_err_str.c_str());

    EXPECT_EQ(1, parser.result.size());

    if (parser.result.size() != 1) {
        return;
    }
    EXPECT_EQ(parser::NT_CREATE_TABLE, parser.result[0]->node_type);
    CreateTableStmt* stmt = (CreateTableStmt*)parser.result[0];
    EXPECT_EQ(parser::NT_CREATE_TABLE, stmt->node_type);
    EXPECT_FALSE(stmt->if_not_exist);

    printf("stmt->table_name: %p", stmt->table_name);

    if (!stmt->table_name->db.empty()) {
        printf("db: %s\n", stmt->table_name->db.value);
    }
    if (stmt->table_name->table.value) {
        printf("table: %s\n", stmt->table_name->table.value);
    }
    for (int idx = 0; idx < stmt->columns.size(); ++idx) {
        stmt->columns[idx]->name->print();

    }
}

TEST(test_parser, begin_txn) {
    parser::SqlParser parser;
    std::string sql = "BEGIN;";
    parser.parse(sql);
    if (parser.error != parser::SUCC) {
        std::cout <<  parser.result.size() << " error:" << parser.syntax_err_str << std::endl;
        return;
    }
    StmtNode* stmt = (StmtNode*)parser.result[0];
    EXPECT_EQ(stmt->node_type, parser::NT_START_TRANSACTION);

    //////////////////////
    parser::SqlParser parser2;
    std::string sql2 = "begin work;";
    parser2.parse(sql2);
    if (parser2.error != parser::SUCC) {
        std::cout <<  parser2.result.size() << " error:" << parser2.syntax_err_str << std::endl;
        return;
    }
    StmtNode* stmt2 = (StmtNode*)parser2.result[0];
    EXPECT_EQ(stmt2->node_type, parser::NT_START_TRANSACTION);

    //////////////////////
    parser::SqlParser parser3;
    std::string sql3 = "start transaction;";
    parser3.parse(sql3);
    if (parser3.error != parser::SUCC) {
        std::cout <<  parser3.result.size() << " error:" << parser3.syntax_err_str << std::endl;
        return;
    }
    StmtNode* stmt3 = (StmtNode*)parser3.result[0];
    EXPECT_EQ(stmt3->node_type, parser::NT_START_TRANSACTION);
}

TEST(test_parser, commit_txn) {
    parser::SqlParser parser;
    std::string sql = "COMMIT;";
    parser.parse(sql);
    if (parser.error != parser::SUCC) {
        std::cout <<  parser.result.size() << " error:" << parser.syntax_err_str << std::endl;
        return;
    }
    StmtNode* stmt = (StmtNode*)parser.result[0];
    EXPECT_EQ(stmt->node_type, parser::NT_COMMIT_TRANSACTION);

    //////////////////////
    parser::SqlParser parser2;
    std::string sql2 = "COMMIT work;";
    parser2.parse(sql2);
    if (parser2.error != parser::SUCC) {
        std::cout <<  parser2.result.size() << " error:" << parser2.syntax_err_str << std::endl;
        return;
    }
    StmtNode* stmt2 = (StmtNode*)parser2.result[0];
    EXPECT_EQ(stmt2->node_type, parser::NT_COMMIT_TRANSACTION);
}

TEST(test_parser, rollback_txn) {
    parser::SqlParser parser;
    std::string sql = "rollback;";
    parser.parse(sql);
    if (parser.error != parser::SUCC) {
        std::cout <<  parser.result.size() << " error:" << parser.syntax_err_str << std::endl;
        return;
    }
    StmtNode* stmt = (StmtNode*)parser.result[0];
    EXPECT_EQ(stmt->node_type, parser::NT_ROLLBACK_TRANSACTION);

    //////////////////////
    parser::SqlParser parser2;
    std::string sql2 = "rollback work;";
    parser2.parse(sql2);
    if (parser2.error != parser::SUCC) {
        std::cout <<  parser2.result.size() << " error:" << parser2.syntax_err_str << std::endl;
        return;
    }
    StmtNode* stmt2 = (StmtNode*)parser2.result[0];
    EXPECT_EQ(stmt2->node_type, parser::NT_ROLLBACK_TRANSACTION);
}

TEST(test_parser, autocommit1) {
    parser::SqlParser parser;
    std::string sql = "set autocommit=1;";
    parser.parse(sql);
    if (parser.error != parser::SUCC) {
        std::cout <<  parser.result.size() << " error:" << parser.syntax_err_str << std::endl;
        return;
    }
    StmtNode* stmt = (StmtNode*)parser.result[0];
    EXPECT_EQ(stmt->node_type, parser::NT_SET_CMD);
    if (stmt->node_type != parser::NT_SET_CMD) {
        return;
    }
    SetStmt* set = (SetStmt*)stmt;
    EXPECT_EQ(set->var_list.size(), 1);
    if (set->var_list.size() != 1) {
        return;
    }
    VarAssign* assign = set->var_list[0];
    EXPECT_EQ(strcmp(assign->key.value, "autocommit"), 0);
    EXPECT_EQ(assign->value->expr_type, ET_LITETAL);

    if (assign->value->expr_type != ET_LITETAL) {
        return;
    }
    LiteralExpr* literal = (LiteralExpr*)(assign->value);
    EXPECT_EQ(literal->_u.int64_val, 1);
}

TEST(test_parser, autocommit0) {
    parser::SqlParser parser;
    std::string sql = "set autocommit=0;";
    parser.parse(sql);
    if (parser.error != parser::SUCC) {
        std::cout <<  parser.result.size() << " error:" << parser.syntax_err_str << std::endl;
        return;
    }
    StmtNode* stmt = (StmtNode*)parser.result[0];
    EXPECT_EQ(stmt->node_type, parser::NT_SET_CMD);
    if (stmt->node_type != parser::NT_SET_CMD) {
        return;
    }
    SetStmt* set = (SetStmt*)stmt;
    EXPECT_EQ(set->var_list.size(), 1);
    if (set->var_list.size() != 1) {
        return;
    }
    VarAssign* assign = set->var_list[0];
    EXPECT_EQ(strcmp(assign->key.value, "autocommit"), 0);
    EXPECT_EQ(assign->value->expr_type, ET_LITETAL);

    if (assign->value->expr_type != ET_LITETAL) {
        return;
    }
    LiteralExpr* literal = (LiteralExpr*)(assign->value);
    EXPECT_EQ(literal->_u.int64_val, 0);
}

TEST(test_parser, set_kv) {
    parser::SqlParser parser;
    std::string sql = "set key1=val1, key2=val2;";
    parser.parse(sql);
    if (parser.error != parser::SUCC) {
        std::cout <<  parser.result.size() << " error:" << parser.syntax_err_str << std::endl;
        return;
    }
    StmtNode* stmt = (StmtNode*)parser.result[0];
    EXPECT_EQ(stmt->node_type, parser::NT_SET_CMD);
    if (stmt->node_type != parser::NT_SET_CMD) {
        return;
    }
    SetStmt* set = (SetStmt*)stmt;
    EXPECT_EQ(set->var_list.size(), 2);
    if (set->var_list.size() != 2) {
        return;
    }
    VarAssign* assign0 = set->var_list[0];
    EXPECT_EQ(strcmp(assign0->key.value, "key1"), 0);
    EXPECT_EQ(assign0->value->expr_type, ET_COLUMN);

    if (assign0->value->expr_type != ET_COLUMN) {
        return;
    }
    ColumnName* name = (ColumnName*)(assign0->value);
    EXPECT_EQ(strcmp(name->name.value, "val1"), 0);

    VarAssign* assign1 = set->var_list[1];
    EXPECT_EQ(strcmp(assign1->key.value, "key2"), 0);
    EXPECT_EQ(assign1->value->expr_type, ET_COLUMN);

    if (assign1->value->expr_type != ET_COLUMN) {
        return;
    }
    name = (ColumnName*)(assign1->value);
    EXPECT_EQ(strcmp(name->name.value, "val2"), 0);
}
}  // namespace baikal
