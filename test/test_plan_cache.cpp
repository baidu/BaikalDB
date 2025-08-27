// Copyright (c) 2023 Baidu, Inc. All Rights Reserved.
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

#include "filter_node.h"
#include "exec_node.h"
#include "logical_planner.h"
#include "network_socket.h"
#include "parser.h"
#include "schema_factory.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    int ret = RUN_ALL_TESTS();
    return ret;
}

namespace baikaldb {

void sql_param(const std::string& sql, std::string& param_sql) {
    parser::SqlParser parser;
    parser.parse(sql);
    parser::StmtNode* stmt = parser.result[0];
    std::ostringstream os;
    parser::PlanCacheParam plan_cache_param;
    stmt->set_cache_param(&plan_cache_param);
    stmt->to_stream(os);
    param_sql = os.str();
    if (stmt->is_complex_node()) {
        param_sql = "";
    }
    return;
}

TEST(test_parameterized, case_all) {
    std::string sql;
    std::string param_sql;
    std::string expected_sql;
    
    sql = "SELECT 1 + 1";
    sql_param(sql, param_sql);
    expected_sql = "SELECT (@0 + @1)";
    EXPECT_EQ(param_sql, expected_sql);

    sql = "SELECT max(10)";
    sql_param(sql, param_sql);
    expected_sql = "SELECT (max(@0))";
    EXPECT_EQ(param_sql, expected_sql);

    // FILTER
    sql = "SELECT AVG(col3 + 6) "
          "FROM test_tbl "
          "WHERE col1 = 1 or col2 = 2 "
          "GROUP BY (col3 + 3) "
          "HAVING SUM(col3 - 1) > 200 "
          "ORDER BY (1024 - col3) "
          "LIMIT 10";
    sql_param(sql, param_sql);
    expected_sql = "SELECT (AVG((col3 + @0))) "
                   "FROM test_tbl "
                   "WHERE ((col1 = @1) || (col2 = @2)) "
                   "GROUP BY (col3 + @3) ASC "
                   "HAVING((SUM((col3 - @4))) > @5) "
                   "ORDER BY (@6 - col3) ASC "
                   "LIMIT @7, @8";
    EXPECT_EQ(param_sql, expected_sql);

    // JOIN
    sql = "SELECT t1.col3, t2.col4 "
          "FROM test_tbl t1 LEFT JOIN test_tbl_2 t2 ON t1.col1 + 2 = t2.col1 "
          "WHERE t1.col1 = 1 or t2.col2 = 2";
    sql_param(sql, param_sql);
    expected_sql = "";
    EXPECT_EQ(param_sql, expected_sql);

    // SUBQUERY
    sql = "SELECT * "
          "FROM (SELECT * FROM test_tbl WHERE col1 = 1 and col2 = 2) t";
    sql_param(sql, param_sql);
    expected_sql = "";
    EXPECT_EQ(param_sql, expected_sql);

    sql = "SELECT * "
          "FROM test_tbl "
          "WHERE col1 < (SELECT SUM(col2) FROM test_tbl_2 WHERE col2 < 100)";
    sql_param(sql, param_sql);
    expected_sql = "";
    EXPECT_EQ(param_sql, expected_sql);

    sql = "SELECT * "
          "FROM test_tbl "
          "WHERE col1 < (SELECT SUM(col2) FROM test_tbl_2 WHERE col2 < test_tbl.col2 + 100)";
    sql_param(sql, param_sql);
    expected_sql = "";
    EXPECT_EQ(param_sql, expected_sql);
}

TEST(test_cache, case_all) {
    Cache<int64_t, std::string> test_cache;
    test_cache.init(10);
    test_cache.add(1, "test1");
    test_cache.add(2, "test2");
    test_cache.add(3, "test3");
    EXPECT_EQ(test_cache.check(2), 0);
    test_cache.del(2);
    EXPECT_EQ(test_cache.check(2), -1);
}

} // namespace baikaldb
