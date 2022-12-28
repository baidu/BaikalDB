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
#include "internal_functions.h"
#include "fn_manager.h"
#include "proto/expr.pb.h"
#include "parser.h"
#include "proto/meta.interface.pb.h"

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace baikaldb {
TEST(round, round) {
    {
        std::vector<ExprValue> input;
        ExprValue v1(pb::DOUBLE);
        v1._u.double_val = 3.1356;
        input.push_back(v1);
        ExprValue ret = round(input);
        EXPECT_EQ(ret._u.double_val, 3);
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(pb::DOUBLE);
        v1._u.double_val = 3.1356;
        ExprValue v2(pb::INT64);
        v2._u.int64_val = 0;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = round(input);
        EXPECT_EQ(ret._u.double_val, 3);
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(pb::DOUBLE);
        v1._u.double_val = 3.1356;
        ExprValue v2(pb::INT64);
        v2._u.int64_val = 2;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = round(input);
        EXPECT_EQ(ret._u.double_val, 3.14);
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(pb::DOUBLE);
        v1._u.double_val = 3.1356;
        ExprValue v2(pb::INT64);
        v2._u.int64_val = 1;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = round(input);
        EXPECT_EQ(ret._u.double_val, 3.1);
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(pb::DOUBLE);
        v1._u.double_val = 123456.1356;
        ExprValue v2(pb::INT64);
        v2._u.int64_val = 30;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = round(input);
        EXPECT_EQ(ret._u.double_val, 123456.1356);
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(pb::DOUBLE);
        v1._u.double_val = 123456.1356;
        ExprValue v2(pb::INT64);
        v2._u.int64_val = -1;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = round(input);
        EXPECT_EQ(ret._u.double_val, 123460);
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(pb::DOUBLE);
        v1._u.double_val = 123456.1356;
        ExprValue v2(pb::INT64);
        v2._u.int64_val = -3;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = round(input);
        EXPECT_EQ(ret._u.double_val, 123000);
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(pb::DOUBLE);
        v1._u.double_val = 123456.1356;
        ExprValue v2(pb::INT64);
        v2._u.int64_val = -300;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = round(input);
        EXPECT_EQ(ret._u.double_val, 0);
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(pb::DOUBLE);
        v1._u.double_val = -3.1356;
        ExprValue v2(pb::INT64);
        v2._u.int64_val = 2;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = round(input);
        EXPECT_EQ(ret._u.double_val, -3.14);
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(pb::DOUBLE);
        v1._u.double_val = -3.1356;
        ExprValue v2(pb::INT64);
        v2._u.int64_val = 3;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = round(input);
        EXPECT_EQ(ret._u.double_val, -3.136);
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(pb::DOUBLE);
        v1._u.double_val = -123456.1356;
        ExprValue v2(pb::INT64);
        v2._u.int64_val = -2;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = round(input);
        EXPECT_EQ(ret._u.double_val, -123500);
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(pb::DOUBLE);
        v1._u.double_val = -123456.1356;
        ExprValue v2(pb::INT64);
        v2._u.int64_val = -30;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = round(input);
        EXPECT_EQ(ret._u.double_val, 0);
    }
}

TEST(substring_index, substring_index) {
    {
        std::vector<ExprValue> input;
        ExprValue v1(pb::STRING);
        v1.str_val = "www.begtut.com";
        ExprValue v2(pb::STRING);
        v2.str_val = "ut";
        ExprValue v3(pb::INT64);
        v3._u.int64_val = -1;
        input.push_back(v1);
        input.push_back(v2);
        input.push_back(v3);
        ExprValue ret = substring_index(input);
        EXPECT_STREQ(ret.str_val.c_str(), ".com");
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(pb::STRING);
        v1.str_val = "www.begtut.com";
        ExprValue v2(pb::STRING);
        v2.str_val = "ut";
        ExprValue v3(pb::INT64);
        v3._u.int64_val = -2;
        input.push_back(v1);
        input.push_back(v2);
        input.push_back(v3);
        ExprValue ret = substring_index(input);
        EXPECT_STREQ(ret.str_val.c_str(), "www.begtut.com");
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(pb::STRING);
        v1.str_val = "www.begtut.com";
        ExprValue v2(pb::STRING);
        v2.str_val = "ut";
        ExprValue v3(pb::INT64);
        v3._u.int64_val = 1;
        input.push_back(v1);
        input.push_back(v2);
        input.push_back(v3);
        ExprValue ret = substring_index(input);
        EXPECT_STREQ(ret.str_val.c_str(), "www.begt");
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(pb::STRING);
        v1.str_val = "www.begtut.com";
        ExprValue v2(pb::STRING);
        v2.str_val = "ww";
        ExprValue v3(pb::INT64);
        v3._u.int64_val = -1;
        input.push_back(v1);
        input.push_back(v2);
        input.push_back(v3);
        ExprValue ret = substring_index(input);
        EXPECT_STREQ(ret.str_val.c_str(), "w.begtut.com");
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(pb::STRING);
        v1.str_val = "www.begtut.com";
        ExprValue v2(pb::STRING);
        v2.str_val = "ww";
        ExprValue v3(pb::INT64);
        v3._u.int64_val = 1;
        input.push_back(v1);
        input.push_back(v2);
        input.push_back(v3);
        ExprValue ret = substring_index(input);
        EXPECT_STREQ(ret.str_val.c_str(), "");
    }
}

TEST(week, week) {
    std::vector<uint32_t> result {52, 53, 52, 1, 53, 53, 1, 53};
    for (int i = 0; i <=7; i++) {
        std::vector<ExprValue> input;
        ExprValue v1(pb::STRING);
        v1.str_val = "2007-12-31";
        ExprValue v2(pb::UINT32);
        v2._u.uint32_val = i;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = week(input);
        EXPECT_EQ(ret.get_numberic<uint32_t>(), result[i]);
    }
    std::vector<uint32_t> result2 {0, 1, 52, 1, 1, 0, 1, 53};
    for (int i = 0; i <=7; i++) {
        std::vector<ExprValue> input;
        ExprValue v1(pb::STRING);
        v1.str_val = "2008-01-01";
        ExprValue v2(pb::UINT32);
        v2._u.uint32_val = i;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = week(input);
        EXPECT_EQ(ret.get_numberic<uint32_t>(), result2[i]);
    }
    std::vector<uint32_t> result3 {200752, 200801, 200752, 200801, 200801, 200753, 200801, 200753};
    for (int i = 0; i <=7; i++) {
        std::vector<ExprValue> input;
        ExprValue v1(pb::STRING);
        v1.str_val = "2007-12-31";
        ExprValue v2(pb::UINT32);
        v2._u.uint32_val = i;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = yearweek(input);
        EXPECT_EQ(ret.get_numberic<uint32_t>(), result3[i]);
    }
    std::vector<uint32_t> result4 {200752, 200801, 200752, 200801, 200801, 200753, 200801, 200753};
    for (int i = 0; i <=7; i++) {
        std::vector<ExprValue> input;
        ExprValue v1(pb::STRING);
        v1.str_val = "2008-01-01";
        ExprValue v2(pb::UINT32);
        v2._u.uint32_val = i;
        input.push_back(v1);
        input.push_back(v2);
        ExprValue ret = yearweek(input);
        EXPECT_EQ(ret.get_numberic<uint32_t>(), result4[i]);
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(pb::STRING);
        v1.str_val = "2007-12-31";
        input.push_back(v1);
        ExprValue ret = weekofyear(input);
        EXPECT_EQ(ret.get_numberic<uint32_t>(), 1);
    }
    {
        std::vector<ExprValue> input;
        ExprValue v1(pb::STRING);
        v1.str_val = "2008-01-01";
        input.push_back(v1);
        ExprValue ret = weekofyear(input);
        EXPECT_EQ(ret.get_numberic<uint32_t>(), 1);
    }
}

}  // namespace baikal
