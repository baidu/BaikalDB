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

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace baikaldb {

//extern std::string timestamp_to_str(time_t timestamp);
//extern time_t str_to_timestamp(const char* str_time);
//extern std::string datetime_to_str(uint64_t datetime);
//extern uint64_t str_to_datetime(const char* str_time);
//extern time_t datetime_to_timestamp(uint64_t datetime);
//extern uint64_t timestamp_to_datetime(time_t timestamp);

TEST(test_stamp_to_str, case_all) {
    EXPECT_EQ(timestamp_to_str(1512300524), "2017-12-03 19:28:44");
    EXPECT_EQ(timestamp_to_str(1512300480), "2017-12-03 19:28:00");
    EXPECT_EQ(timestamp_to_str(1512230400), "2017-12-03 00:00:00");
}

TEST(test_str_to_stamp, case_all) {
    EXPECT_EQ(str_to_timestamp("2017-12-03 19:28:44hahahaha"), 1512300524);
    EXPECT_EQ(str_to_timestamp("2017-12-03 19:28:44"), 1512300524);
    EXPECT_EQ(str_to_timestamp("2017-12-03 19:28:"), 1512300480);
    EXPECT_EQ(str_to_timestamp("2017-12-03"), 1512230400);
    EXPECT_NE(str_to_timestamp("hahahahah"), 1512259200);
}

TEST(test_str_stamp, case_all) {
    EXPECT_EQ(timestamp_to_str(str_to_timestamp("2017-12-03 19:28:4400")), "2017-12-03 19:28:44");
    EXPECT_EQ(timestamp_to_str(str_to_timestamp("2017-12-03 19:283:44")), "2017-12-03 19:28:00");
    EXPECT_EQ(timestamp_to_str(str_to_timestamp("2017-12-03 192:28:44")), "2017-12-03 19:00:00");
    //EXPECT_EQ(timestamp_to_str(str_to_timestamp("2017-122-03 19:28:44")), "2017-12-00 00:00:00");
    //EXPECT_EQ(timestamp_to_str(str_to_timestamp("20179-12-03 19:28:44")), "2017-12-00 00:00:00");
    EXPECT_EQ(timestamp_to_str(str_to_timestamp("2017-12-03 19:28:")), "2017-12-03 19:28:00");
    EXPECT_EQ(timestamp_to_str(str_to_timestamp("2017-12-03 19:")), "2017-12-03 19:00:00");
}

TEST(test_datetime_str, case_all) {
    EXPECT_EQ(datetime_to_str(str_to_datetime("2017-12-03 19:28:44")), "2017-12-03 19:28:44.000000");
    EXPECT_EQ(datetime_to_str(str_to_datetime("2017-12-03 19:28:44.")), "2017-12-03 19:28:44.000000");
    EXPECT_EQ(datetime_to_str(str_to_datetime("2017-12-03 19:")), "2017-12-03 19:00:00.000000");
    EXPECT_EQ(datetime_to_str(str_to_datetime("2017-12-03 19")), "2017-12-03 19:00:00.000000");
    EXPECT_EQ(datetime_to_str(str_to_datetime("2017-12-03 19:28:44.000")), "2017-12-03 19:28:44.000000");
    EXPECT_EQ(datetime_to_str(str_to_datetime("2017-12-03 19:28:44.1234567")), "2017-12-03 19:28:44.123456");
    EXPECT_EQ(datetime_to_str(str_to_datetime("2017-12-03 19:28:44.123")), "2017-12-03 19:28:44.123000");

    EXPECT_EQ(datetime_to_str(str_to_datetime("2017-12-03 19:28:44.000123")), "2017-12-03 19:28:44.000123");
    EXPECT_EQ(datetime_to_str(str_to_datetime("2017-12-03 19:28:44.000123456")), "2017-12-03 19:28:44.000123");

    EXPECT_EQ(datetime_to_str(str_to_datetime("2017-12-03 192:28:44")), "2017-12-03 19:00:00.000000");
    EXPECT_EQ(datetime_to_str(str_to_datetime("2017-12-03 19:284:44")), "2017-12-03 19:28:00.000000");
}

TEST(test_datetime_timestamp, case_all) {
    EXPECT_EQ(timestamp_to_str(datetime_to_timestamp(str_to_datetime("2017-12-03 19:28:44.123456"))), "2017-12-03 19:28:44");
    EXPECT_EQ(datetime_to_str(timestamp_to_datetime(str_to_timestamp("2017-12-03 19:28:44"))), "2017-12-03 19:28:44.000000");
}

}  // namespace baikal
