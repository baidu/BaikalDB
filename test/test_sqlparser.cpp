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
#include <fstream>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include "baikal_client.h"
#include "parser.h"

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
#include "uconv.h"
namespace parser {

TEST(test_parser, case_all) {
    parser::SqlParser parser;
    std::string sql = "insert into t1() values (1,'aaa'),('3',4+2)";
    //std::string sql2 = "insert \n \n in to t1 (a,b) values (1,1),(now(), (1+((2+3))));";
    std::ifstream done_ifs("t");
    std::string sql2(
        (std::istreambuf_iterator<char>(done_ifs)),
        std::istreambuf_iterator<char>());
    /*
    baikal::client::Manager _manager;
    baikal::client::Service* _baikaldb; 
    _manager.init("conf", "baikal_client.conf");
    baikal::client::ResultSet result_set;
    _baikaldb = _manager.get_service("baikaldb");
    _baikaldb->query(0, sql2, &result_set);
    for (int i = 0; i < sql2.size(); i++) {
        printf("%02x", sql2[i]);
    }
    */
    int len = ::is_utf8_strict(sql2.c_str(), sql2.size(), true);
    int len2 = ::uconv_is_gbk_n(sql2.c_str(), sql2.size());
    std::cout << "\n" << sql2.size() << " " << len << " "<< len2 << std::endl;
    parser.parse(sql2);
    std::cout << "sql:" << sql2 << " " << parser.is_gbk << " " << parser.has_5c << std::endl;
    if (parser.error != parser::SUCC) {
        std::cout <<  parser.result.size() << " error:" << parser.syntax_err_str << std::endl;
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

TEST(test_parser, case_create_view) {
    parser::SqlParser parser1;
    std::string sql1 = "create or replace view aa (a,b,c,d) as select i,j,k,l from t1";
    parser1.parse(sql1);
    printf("errormsg: %d, %s\n", parser1.error, parser1.syntax_err_str.c_str());
    EXPECT_EQ(1, parser1.result.size());
    if (parser1.result.size() != 1) {
        return;
    }
    CreateViewStmt* stmt1 = (CreateViewStmt*)parser1.result[0];
    EXPECT_EQ(parser::NT_CREATE_VIEW, stmt1->node_type);
    EXPECT_EQ(true, stmt1->or_replace);
    EXPECT_EQ(nullptr, ((parser::SelectStmt*)stmt1->view_select_stmt)->fields[0]->wild_card);

    parser::SqlParser parser2;
    std::string sql2 = "create view aa(a,b,c,d) as select * from t1";
    parser2.parse(sql2);
    printf("errormsg: %d, %s\n", parser2.error, parser2.syntax_err_str.c_str());
    EXPECT_EQ(1, parser2.result.size());
    if (parser2.result.size() != 1) {
        return;
    }
    EXPECT_EQ(parser::NT_CREATE_VIEW, parser2.result[0]->node_type);
    CreateViewStmt* stmt2 = (CreateViewStmt*)parser2.result[0];
    EXPECT_EQ(false, stmt2->or_replace);
    std::ostringstream os;
    parser::SelectStmt* view_select_stmt = (parser::SelectStmt*)stmt2->view_select_stmt;
    view_select_stmt->to_stream(os);
    EXPECT_EQ("SELECT * FROM t1", os.str());
    EXPECT_EQ(true, view_select_stmt->fields[0]->wild_card->table_name.empty());
    EXPECT_EQ(true, view_select_stmt->fields[0]->wild_card->db_name.empty());

    std::ostringstream view_name_os;
    stmt2->view_name->to_stream(view_name_os);
    EXPECT_EQ("aa", view_name_os.str());
    EXPECT_EQ(4, stmt2->column_names.size());
    EXPECT_EQ(strcmp("a", stmt2->column_names[0]->name.value), 0);
    EXPECT_EQ(strcmp("b", stmt2->column_names[1]->name.value), 0);
    EXPECT_EQ(strcmp("c", stmt2->column_names[2]->name.value), 0);
    EXPECT_EQ(strcmp("d", stmt2->column_names[3]->name.value), 0);
}

TEST(test_parser, case_with_select) {
    parser::SqlParser parser1;
    std::string sql1 = "with cte1 as (select * from aaa) select * from cte1;";
    parser1.parse(sql1);
    printf("errormsg: %d, %s\n", parser1.error, parser1.syntax_err_str.c_str());
    EXPECT_EQ(1, parser1.result.size());
    if (parser1.result.size() != 1) {
        return;
    }
    std::ostringstream with_select_os;
    SelectStmt* stmt1 = (SelectStmt*)parser1.result[0];
    stmt1->to_stream(with_select_os);
    std::string with_selelct_string = "WITH cte1 AS (SELECT * FROM aaa) AS SELECT * FROM cte1";
    EXPECT_EQ(with_selelct_string, with_select_os.str());
}

TEST(test_parser, case_create_table_hll) {
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
        "`hll_field` HLL NOT NULL,"
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

TEST(test_parser, case_create_rollup_table) {
    parser::SqlParser parser;
    
    std::string sql = "create table score_diary_book ("
        "`book_id` bigint(20) NOT NULL COMMENT '日记本ID',"
        "`parent_id` bigint(20) NOT NULL COMMENT '父ID',"
        "`score_type` int(11) NOT NULL COMMENT '1-日记本净分数 2-日记本总分',"
        "`score` double NOT NULL COMMENT '分数',"
        "`__sign__` bigint(20) unsigned NOT NULL DEFAULT '0'," 
        "`__version__` bigint(20) unsigned NOT NULL DEFAULT '0'," 
        "PRIMARY KEY (book_id, score_type, __sign__),"
        "ROLLUP KEY `rollup_index` (book_id)"
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
    {
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
    // test system variable with identifier prefix
    {
        parser::SqlParser parser;
        std::string sql = "SET SESSION autocommit=ON, LOCAL autocommit=ON, @@autocommit=OFF, GLOBAL autocommit=OFF";
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
        EXPECT_EQ(set->var_list.size(), 4);
        if (set->var_list.size() != 4) {
            return;
        }
        VarAssign* assign0 = set->var_list[0];
        EXPECT_EQ(strcmp(assign0->key.value, "@@session.autocommit"), 0);
        EXPECT_EQ(assign0->value->expr_type, ET_LITETAL);
        if (assign0->value->expr_type != ET_LITETAL) {
            return;
        }
        LiteralExpr* lit = (LiteralExpr*)(assign0->value);
        EXPECT_EQ(lit->_u.int64_val, 1);

        VarAssign* assign1 = set->var_list[1];
        EXPECT_EQ(strcmp(assign1->key.value, "@@local.autocommit"), 0);
        EXPECT_EQ(assign1->value->expr_type, ET_LITETAL);
        if (assign1->value->expr_type != ET_LITETAL) {
            return;
        }
        lit = (LiteralExpr*)(assign1->value);
        EXPECT_EQ(lit->_u.int64_val, 1);

        VarAssign* assign2 = set->var_list[2];
        EXPECT_EQ(strcmp(assign2->key.value, "@@autocommit"), 0);
        EXPECT_EQ(assign2->value->expr_type, ET_LITETAL);
        if (assign2->value->expr_type != ET_LITETAL) {
            return;
        }
        lit = (LiteralExpr*)(assign2->value);
        EXPECT_EQ(lit->_u.int64_val, 0);

        VarAssign* assign3 = set->var_list[3];
        EXPECT_EQ(strcmp(assign3->key.value, "@@global.autocommit"), 0);
        EXPECT_EQ(assign3->value->expr_type, ET_LITETAL);
        if (assign3->value->expr_type != ET_LITETAL) {
            return;
        }
        lit = (LiteralExpr*)(assign3->value);
        EXPECT_EQ(lit->_u.int64_val, 0);
    }
    // test user variable
    {
        parser::SqlParser parser;
        std::string sql = "SET @user_key=userval";
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
        VarAssign* assign0 = set->var_list[0];
        EXPECT_EQ(strcmp(assign0->key.value, "@user_key"), 0);
        EXPECT_EQ(assign0->value->expr_type, ET_COLUMN);
        if (assign0->value->expr_type != ET_COLUMN) {
            return;
        }
        ColumnName* name = (ColumnName*)(assign0->value);
        EXPECT_EQ(strcmp(name->name.value, "userval"), 0);
    }
}

TEST(test_parser, alter_table) {
    {
        parser::SqlParser parser;
        std::string sql = "alter table tb add index idx(a,b)";
        parser.parse(sql);
        EXPECT_EQ(parser.error, parser::SUCC);
    }
    {
        parser::SqlParser parser;
        std::string sql = "alter table tb add index global idx(a,b)";
        parser.parse(sql);
        EXPECT_EQ(parser.error, parser::SUCC);
    }
    {
        parser::SqlParser parser;
        std::string sql = "alter table tb add fulltext index idx(a,b)";
        parser.parse(sql);
        EXPECT_EQ(parser.error, parser::SUCC);
    }
    {
        parser::SqlParser parser;
        std::string sql = "alter table tb add vector index idx(a,b)";
        parser.parse(sql);
        EXPECT_EQ(parser.error, parser::SUCC);
    }
    {
        parser::SqlParser parser;
        std::string sql = "alter table tb add unique index idx(a,b)";
        parser.parse(sql);
        EXPECT_EQ(parser.error, parser::SUCC);
    }
    {
        parser::SqlParser parser;
        std::string sql = "alter table tb add column a int(10) not null ";
        parser.parse(sql);
        EXPECT_EQ(parser.error, parser::SUCC);
    }
    {
        parser::SqlParser parser;
        std::string sql = "alter table tb add column unique a int(10) not null ";
        parser.parse(sql);
        EXPECT_EQ(parser.error, parser::SUCC);
    }
}
}  // namespace baikal
