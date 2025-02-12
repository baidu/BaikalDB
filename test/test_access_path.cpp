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
#include "common.h"
#include "datetime.h"
#include "expr_value.h"
#include "access_path.h"

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace baikaldb {

ExprValue construct_date_expr_value(const char* str_time) {
    ExprValue expr_value(pb::DATE);
    expr_value._u.uint32_val = datetime_to_date(str_to_datetime(str_time));
    return expr_value;
}

TEST(test_date_range_to_in, case_all) {
    baikaldb::AccessPath access_path;
    {
        // left > right
        auto left = construct_date_expr_value("2024-12-04");
        auto right = construct_date_expr_value("2023-03-14");
        std::vector<ExprValue> result;
        access_path.get_date_in_values(left, false, right, false, result);
        EXPECT_EQ(result.size(), 0);
    }
    {
        // left = right
        auto left = construct_date_expr_value("2023-03-14");
        auto right = construct_date_expr_value("2023-03-14");
        std::vector<ExprValue> result;
        access_path.get_date_in_values(left, true, right, true, result);
        EXPECT_EQ(result.size(), 0);
        access_path.get_date_in_values(left, true, right, false, result);
        EXPECT_EQ(result.size(), 0);
        access_path.get_date_in_values(left, false, right, true, result);
        EXPECT_EQ(result.size(), 0);
        access_path.get_date_in_values(left, false, right, false, result);
        EXPECT_EQ(result.size(), 1);
    }
    {
        auto left = construct_date_expr_value("2023-06-04");
        auto right = construct_date_expr_value("2023-06-14");
        std::vector<ExprValue> result;
        access_path.get_date_in_values(left, false, right, false, result);
        EXPECT_EQ(result.size(), 11);
        result.clear();
        // left open
        access_path.get_date_in_values(left, true, right, false, result);
        EXPECT_EQ(result.size(), 10);
        result.clear();
        // right open
        access_path.get_date_in_values(left, false, right, true, result);
        EXPECT_EQ(result.size(), 10);
        result.clear();
        // both open
        access_path.get_date_in_values(left, true, right, true, result);
        EXPECT_EQ(result.size(), 9);
    }
    {
        // 跨月
        auto left = construct_date_expr_value("2020-02-04");
        auto right = construct_date_expr_value("2020-03-14");
        std::vector<ExprValue> result;
        access_path.get_date_in_values(left, false, right, false, result);
        EXPECT_EQ(result.size(), 40);
        result.clear();
        // 
        left = construct_date_expr_value("2021-02-04");
        right = construct_date_expr_value("2021-03-14");
        access_path.get_date_in_values(left, false, right, false, result);
        EXPECT_EQ(result.size(), 39);
    }
    {
        // 跨年
        auto left = construct_date_expr_value("2022-12-04");
        auto right = construct_date_expr_value("2023-03-14");
        std::vector<ExprValue> result;
        access_path.get_date_in_values(left, false, right, false, result);
        EXPECT_EQ(result.size(), 101);
        result.clear();
        left = construct_date_expr_value("2022-12-04");
        right = construct_date_expr_value("2023-12-04");
        access_path.get_date_in_values(left, true, right, false, result);
        EXPECT_EQ(result.size(), 365);
        result.clear();
        left = construct_date_expr_value("2019-12-04");
        right = construct_date_expr_value("2020-12-04");
        access_path.get_date_in_values(left, true, right, false, result);
        EXPECT_EQ(result.size(), 366);
    }
}

}  // namespace baikal
