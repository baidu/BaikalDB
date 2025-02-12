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
#include "internal_functions.h"

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
    uint64_t year = 0;
    uint64_t mon = 0;
    uint64_t mday = 0;
    uint64_t hour = 0;
    uint64_t min = 0;
    uint64_t sec = 0;
    uint64_t macrosec = 0;
    char str_time[100] = "2018-01-11T10:10:10";
    sscanf(str_time, "%4lu%*[^0-9a-z]%2lu%*[^0-9a-z]%2lu"
            "%*[^0-9a-z]%2lu%*[^0-9a-z]%2lu%*[^0-9a-z]%2lu.%6lu",
           &year, &mon, &mday,  
           &hour, &min, &sec, &macrosec);
    std::cout << year << " " << mon << " " << mday << " " << hour << " " << min << " " << sec << " " << macrosec << std::endl;
    EXPECT_EQ(timestamp_to_str(1512300524), "2017-12-03 19:28:44");
    EXPECT_EQ(timestamp_to_str(1512300480), "2017-12-03 19:28:00");
    EXPECT_EQ(timestamp_to_str(1512230400), "2017-12-03 00:00:00");
    std::cout << timestamp_to_str(-100) << std::endl;
}
/*
TEST(test_str_to_stamp, case_all) {
    //EXPECT_EQ(str_to_timestamp("2017-12-03 19:28:44hahahaha"), 1512300524);
    EXPECT_EQ(str_to_timestamp("2017-12-03 19:28:44"), 1512300524);
    EXPECT_EQ(str_to_timestamp("2017:12:03 19/28/44"), 1512300524);
    EXPECT_EQ(str_to_timestamp("2017-12-03 19:28:"), 1512300480);
    EXPECT_EQ(str_to_timestamp("2017-12-03"), 1512230400);
    EXPECT_NE(str_to_timestamp("hahahahah"), 1512259200);
}
*/
/*
TEST(test_str_stamp, case_all) {
    EXPECT_EQ(timestamp_to_str(str_to_timestamp("2017-12-03 19:28:4400")), "2017-12-03 19:28:44");
    EXPECT_EQ(timestamp_to_str(str_to_timestamp("2017-12-03 19:283:44")), "2017-12-03 19:28:00");
    EXPECT_EQ(timestamp_to_str(str_to_timestamp("2017-12-03 192:28:44")), "2017-12-03 19:00:00");
    //EXPECT_EQ(timestamp_to_str(str_to_timestamp("2017-122-03 19:28:44")), "2017-12-00 00:00:00");
    //EXPECT_EQ(timestamp_to_str(str_to_timestamp("20179-12-03 19:28:44")), "2017-12-00 00:00:00");
    EXPECT_EQ(timestamp_to_str(str_to_timestamp("2017-12-03 19:28:")), "2017-12-03 19:28:00");
    EXPECT_EQ(timestamp_to_str(str_to_timestamp("2017-12-03 19:")), "2017-12-03 19:00:00");
}
*/
TEST(test_datetime_str, case_all) {
    EXPECT_EQ(datetime_to_str(str_to_datetime("2017-12-03 19:28:44aaa")), "2017-12-03 19:28:44");
    EXPECT_EQ(datetime_to_str(str_to_datetime("2017-12-03 19:28:44")), "2017-12-03 19:28:44");
    EXPECT_EQ(datetime_to_str(str_to_datetime("2017-12-03 19:28:44.")), "2017-12-03 19:28:44");
    EXPECT_EQ(datetime_to_str(str_to_datetime("2017-12-03 19:")), "2017-12-03 19:00:00");
    EXPECT_EQ(datetime_to_str(str_to_datetime("2017-12-03 19")), "2017-12-03 19:00:00");
    EXPECT_EQ(datetime_to_str(str_to_datetime("2017-12-03 19:28:44.000")), "2017-12-03 19:28:44");
    EXPECT_EQ(datetime_to_str(str_to_datetime("2017-12-03 19:28:44.1234567"), -1), "2017-12-03 19:28:44.123456");
    EXPECT_EQ(datetime_to_str(str_to_datetime("2017-12-03 19:28:44.123"), -1), "2017-12-03 19:28:44.123000");

    EXPECT_EQ(datetime_to_str(str_to_datetime("2017-12-03 19:28:44.000123"), -1), "2017-12-03 19:28:44.000123");
    EXPECT_EQ(datetime_to_str(str_to_datetime("2017-12-03 19:28:44.000123456"), -1), "2017-12-03 19:28:44.000123");

    EXPECT_EQ(datetime_to_str(str_to_datetime("2017-12-03 192:28:44")), "2017-12-03 19:00:00");
    EXPECT_EQ(datetime_to_str(str_to_datetime("2017-12-03 19:284:44")), "2017-12-03 19:28:00");
}
TEST(test_datetime_str_other, case_all) {
    EXPECT_EQ(datetime_to_str(str_to_datetime("2017/12/03 19*28*44")), "2017-12-03 19:28:44");
    EXPECT_EQ(datetime_to_str(str_to_datetime("2017@12@03T19:28:44")), "2017-12-03 19:28:44");
    EXPECT_EQ(datetime_to_str(str_to_datetime("17-12-03 19:28:44")), "2017-12-03 19:28:44");
    EXPECT_EQ(datetime_to_str(str_to_datetime("89-12-03 19:28:44")), "1989-12-03 19:28:44");
    EXPECT_EQ(datetime_to_str(str_to_datetime("891203")), "1989-12-03 00:00:00");
    EXPECT_EQ(datetime_to_str(str_to_datetime("19891203")), "1989-12-03 00:00:00");
    EXPECT_EQ(datetime_to_str(str_to_datetime("891203192844.111"), -1), "1989-12-03 19:28:44.111000");
    EXPECT_EQ(datetime_to_str(str_to_datetime("19891203192844.111"), -1), "1989-12-03 19:28:44.111000");
    std::cout << "tm:" << str_to_datetime("20111303") << std::endl;
    std::cout << "tm2:" << str_to_datetime("0000-00-00 00:00:00") << std::endl;
    std::cout << "tm3:" << datetime_to_timestamp(0) << std::endl;
    std::cout << "tm4:" << str_to_datetime("1970-01-01 08:00:00") << std::endl;
    std::cout << "tm5:" << str_to_datetime("1970-01-01 08:00:01") << std::endl;
    std::cout << "tm6:" << str_to_datetime("1970-01-01 07:00:01") << std::endl;
    std::cout << "tm6:" << str_to_datetime("1970-00-00 00:00:00") << std::endl;
    std::cout << "tm7:" << datetime_to_timestamp(str_to_datetime("1970-01-01 08:00:00")) << std::endl;
    std::cout << "tm8:" << datetime_to_timestamp(str_to_datetime("1970-01-01 08:00:01")) << std::endl;
    std::cout << "tm9:" << datetime_to_timestamp(str_to_datetime("1970-01-01 07:00:01")) << std::endl;
    std::cout << "tm9:" << datetime_to_timestamp(str_to_datetime("1971-00-00 08:00:01")) << std::endl;
    struct tm tm;
    memset(&tm, 0, sizeof(tm));
    tm.tm_year = 90;
    tm.tm_mon = -1;
    tm.tm_mday = 0;
    tm.tm_isdst = 0;
    time_t t = mktime(&tm);
    std::cout << "mktm:" << t << std::endl;
}
int32_t str_to_time2(const char* str_time) {
    int hour = 0;
    int minute = 0;
    int second = 0;
    int32_t time = 0;
    bool minus = false;

    sscanf(str_time, "%d:%2u:%2u",
         &hour, &minute, &second);
    if (hour < 0) {
        hour = -hour;
        minus = true;
    }
    time |= second;
    time |= (minute << 6);
    time |= (hour << 12);
    if (minus) {
        time = -time;
    }
    std::cout << hour << minute << second << time << std::endl;
    return time;
}
std::string time_to_str2(int32_t time) {
    bool minus = false;
    if (time < 0) {
        minus = true;
        time = -time;
    }
    int hour = (time >> 12) & 0x3FF;
    int min = (time >> 6) & 0x3F;
    int sec = time & 0x3F;
    if (minus) {
        hour = -hour;
    }
    std::cout << hour << min << sec << std::endl;
    char buf[20] = {0};
    snprintf(buf, sizeof(buf), "%02d:%02d:%02d", hour, min, sec);
    return std::string(buf);
}

uint64_t time_to_datetime2(int32_t time) {
    ExprValue tmp(pb::TIMESTAMP);
    time_t now = ::time(NULL);
    std::cout << now << std::endl;
    now = ((now + 28800) / 86400) * 86400; // 去除时分秒
    std::cout << now << std::endl;

    bool minus = false;
    if (time < 0) {
        minus = true;
        time = -time;
    }
    uint32_t hour = (time >> 12) & 0x3FF;
    uint32_t min = (time >> 6) & 0x3F;
    uint32_t sec = time & 0x3F;
    int32_t delta_sec = hour * 3600 + min * 60 + sec;
    if (minus) {
        delta_sec = -delta_sec;
    }
    now -= 28800;
    now += delta_sec;
    std::cout << delta_sec << std::endl;
    std::cout << now << std::endl;

    return timestamp_to_datetime(now);
}

std::vector<ExprValue> construct_null_expr_value() {
    ExprValue expr_value;
    return {expr_value};
}

std::vector<ExprValue> construct_time_expr_value(const char* str_time) {
    ExprValue expr_value(pb::TIME);
    expr_value._u.int32_val = str_to_time(str_time);
    return {expr_value};
}

std::vector<ExprValue> construct_date_expr_value(const char* str_time) {
    ExprValue expr_value(pb::DATE);
    expr_value._u.uint32_val = datetime_to_date(str_to_datetime(str_time));
    return {expr_value};
}

std::vector<ExprValue> construct_datetime_expr_value(const char* str_time) {
    ExprValue expr_value(pb::DATETIME);
    expr_value._u.uint64_val = str_to_datetime(str_time);
    return {expr_value};
}

TEST(test_datetime_time, case_all) {
    int hour = 0;
    int minute = 0;
    int second = 0;
    char str_time[20] = "19:-28:44";
    sscanf(str_time, "%d:%2u:%2u",
            &hour, &minute, &second);
    std::cout << hour << " " << minute << " " << second << std::endl;
    EXPECT_EQ(time_to_str(datetime_to_time(str_to_datetime("2017-12-03 19:28:44.123456"))), "19:28:44");
    EXPECT_EQ(time_to_str(str_to_time("  19:28:44")), "19:28:44");
    EXPECT_EQ(time_to_str(str_to_time("-19:28:44")), "-19:28:44");
    EXPECT_EQ(time_to_str(str_to_time("-119:28:44")), "-119:28:44");
    EXPECT_EQ(time_to_str(str_to_time("-119:28:44.124")), "-119:28:44");
    EXPECT_EQ(time_to_str(str_to_time("199:28:44")), "199:28:44");
    EXPECT_EQ(time_to_str(str_to_time("1 19:28:44")), "43:28:44");
    EXPECT_EQ(time_to_str(str_to_time("-1 19:28:44")), "-43:28:44");
    EXPECT_EQ(time_to_str(str_to_time("192844")), "19:28:44");
    EXPECT_EQ(time_to_str(str_to_time("-1192844")), "-119:28:44");
    EXPECT_EQ(time_to_str(str_to_time("2844")), "00:28:44");
    EXPECT_EQ(time_to_str(str_to_time("844")), "00:08:44");
    EXPECT_EQ(time_to_str(str_to_time("-44")), "-00:00:44");
    EXPECT_EQ(time_to_str(str_to_time("4")), "00:00:04");
    EXPECT_EQ(datetime_to_str(time_to_datetime(str_to_time("19:28:44"))), ExprValue::Now().cast_to(pb::DATE).get_string() + " 19:28:44");
    EXPECT_EQ(datetime_to_str(time_to_datetime(str_to_time("01:28:44"))), ExprValue::Now().cast_to(pb::DATE).get_string() + " 01:28:44");
    EXPECT_EQ(time_to_str(seconds_to_time(3601)), "01:00:01");
    EXPECT_EQ(time_to_str(seconds_to_time(-3601)), "-01:00:01");
    EXPECT_EQ(datetime_to_str(str_to_datetime("2023-07-26 19:28:44")), "2023-07-26 19:28:44");
    EXPECT_EQ(datetime_to_str(str_to_datetime("223-07-26 19:28:44")), "0223-07-26 19:28:44");
    EXPECT_EQ(datetime_to_str(str_to_datetime("23-07-26 19:28:44")), "2023-07-26 19:28:44");
    EXPECT_EQ(datetime_to_str(str_to_datetime("3-07-26 19:28:44")), "0003-07-26 19:28:44");
    EXPECT_EQ(datetime_to_str(str_to_datetime("03-07-26 19:28:44")), "2003-07-26 19:28:44");
    EXPECT_EQ(datetime_to_str(str_to_datetime("0003-07-26 19:28:44")), "0003-07-26 19:28:44");
    EXPECT_EQ(datetime_to_str(str_to_datetime("00010101")), "0001-01-01 00:00:00");
    EXPECT_EQ(datetime_to_str(str_to_datetime("010101")), "2001-01-01 00:00:00");
}

TEST(test_datetime_timestamp, case_all) {
    EXPECT_EQ(timestamp_to_str(datetime_to_timestamp(str_to_datetime("2017-12-03 19:28:44.123456"))), "2017-12-03 19:28:44");
    EXPECT_EQ(timestamp_to_str(datetime_to_timestamp(str_to_datetime("0000-00-00 00:00:00"))), "0000-00-00 00:00:00");
    EXPECT_EQ(timestamp_to_str(datetime_to_timestamp(str_to_datetime("1970-01-01 08:00:00"))), "0000-00-00 00:00:00");
    EXPECT_EQ(timestamp_to_str(datetime_to_timestamp(str_to_datetime("1970-01-01 08:00:01"))), "1970-01-01 08:00:01");
    EXPECT_EQ(timestamp_to_str(datetime_to_timestamp(str_to_datetime("2040-01-01 08:00:01"))), "2040-01-01 08:00:01");
    // < 1970-01-01 08:00:01非法，都当做0
    EXPECT_EQ(timestamp_to_str(datetime_to_timestamp(str_to_datetime("1970-01-01 07:00:01"))), "0000-00-00 00:00:00");
    //EXPECT_EQ(datetime_to_str(timestamp_to_datetime(str_to_timestamp("2017-12-03 19:28:44"))), "2017-12-03 19:28:44");
}

TEST(test_hour_date, case_all) {
    EXPECT_EQ(hour(construct_null_expr_value()).get_string(), "");                          // NULL返回NULL
    EXPECT_EQ(hour(construct_time_expr_value("  19:28:44")).get_string(), "19");
    EXPECT_EQ(hour(construct_time_expr_value("-19:28:44")).get_string(), "19");             // 负数取反
    EXPECT_EQ(hour(construct_time_expr_value("-119:28:44")).get_string(), "119");
    EXPECT_EQ(hour(construct_time_expr_value("-119:28:44")).get_string(), "119");
    EXPECT_EQ(hour(construct_time_expr_value("1 19:28:44")).get_string(), "43");
    EXPECT_EQ(hour(construct_time_expr_value("-1 19:28:44")).get_string(), "43");
    EXPECT_EQ(hour(construct_time_expr_value("192844")).get_string(), "19");
    EXPECT_EQ(hour(construct_time_expr_value("-192844")).get_string(), "19");
    EXPECT_EQ(hour(construct_time_expr_value("2844")).get_string(), "0");
    EXPECT_EQ(hour(construct_time_expr_value("844")).get_string(), "0");
    EXPECT_EQ(hour(construct_time_expr_value("-44")).get_string(), "0");
    EXPECT_EQ(hour(construct_time_expr_value("4")).get_string(), "0");
    
    EXPECT_EQ(hour(construct_time_expr_value("1234567 19:28:44")).get_string(), "187");     // HOUR有范围限制
    EXPECT_EQ(hour(construct_time_expr_value("2023-06-29 19:28:44")).get_string(), "19");
    EXPECT_EQ(hour(construct_time_expr_value("2023-06-29")).get_string(), "999");           // 异常输入，str_to_time()会将2023-06-29转换成999:06:29
    EXPECT_EQ(hour(construct_time_expr_value("06-29 19:28:44")).get_string(), "0");         // 异常输入
    EXPECT_EQ(hour(construct_time_expr_value("9999-09-29 19:28:44")).get_string(), "19");
    EXPECT_EQ(hour(construct_time_expr_value("9999-13-29 19:28:44")).get_string(), "0");    // 异常输入
    EXPECT_EQ(hour(construct_time_expr_value("99999-09-29 19:28:44")).get_string(), "0");   // 异常输入

    EXPECT_EQ(date(construct_null_expr_value()).get_string(), "");
    EXPECT_EQ(date(construct_date_expr_value("2023-06-28")).get_string(), "2023-06-28");
    EXPECT_EQ(date(construct_date_expr_value("2023-16-28")).get_string(), "0000-00-00");    // 异常输入
    EXPECT_EQ(date(construct_date_expr_value("99999-06-28")).get_string(), "0000-00-00");   // 异常输入

    EXPECT_EQ(date(construct_datetime_expr_value("2023-06-28 22:35:40")).get_string(), "2023-06-28");
    EXPECT_EQ(date(construct_datetime_expr_value("2023-16-28 22:35:40")).get_string(), "0000-00-00");  // 异常输入
    EXPECT_EQ(date(construct_datetime_expr_value("99999-06-28 22:35:40")).get_string(), "0000-00-00"); // 异常输入
}

TEST(test_date_add_sub, case_all) {
    time_t ts;

    // YEAR
    ts = datetime_to_timestamp(str_to_datetime("2024-02-29 19:28:44"));
    date_add_interval(ts, -1, TimeUnit::YEAR);
    EXPECT_EQ(timestamp_to_str(ts), "2023-02-28 19:28:44");

    ts = datetime_to_timestamp(str_to_datetime("2024-02-29 19:28:44"));
    date_add_interval(ts, 1, TimeUnit::YEAR);
    EXPECT_EQ(timestamp_to_str(ts), "2025-02-28 19:28:44");

    ts = datetime_to_timestamp(str_to_datetime("2024-03-29 19:28:44"));
    date_add_interval(ts, 1, TimeUnit::YEAR);
    EXPECT_EQ(timestamp_to_str(ts), "2025-03-29 19:28:44");

    // MONTH
    ts = datetime_to_timestamp(str_to_datetime("2023-03-31 19:28:44"));
    date_add_interval(ts, -1, TimeUnit::MONTH);
    EXPECT_EQ(timestamp_to_str(ts), "2023-02-28 19:28:44");

    ts = datetime_to_timestamp(str_to_datetime("2024-03-31 19:28:44"));
    date_add_interval(ts, -1, TimeUnit::MONTH);
    EXPECT_EQ(timestamp_to_str(ts), "2024-02-29 19:28:44");

    ts = datetime_to_timestamp(str_to_datetime("2024-05-31 19:28:44"));
    date_add_interval(ts, -1, TimeUnit::MONTH);
    EXPECT_EQ(timestamp_to_str(ts), "2024-04-30 19:28:44");

    ts = datetime_to_timestamp(str_to_datetime("2024-05-31 19:28:44"));
    date_add_interval(ts, 1, TimeUnit::MONTH);
    EXPECT_EQ(timestamp_to_str(ts), "2024-06-30 19:28:44");

    ts = datetime_to_timestamp(str_to_datetime("2024-03-31 19:28:44"));
    date_add_interval(ts, -25, TimeUnit::MONTH);
    EXPECT_EQ(timestamp_to_str(ts), "2022-02-28 19:28:44");

    ts = datetime_to_timestamp(str_to_datetime("2024-03-31 19:28:44"));
    date_add_interval(ts, -49, TimeUnit::MONTH);
    EXPECT_EQ(timestamp_to_str(ts), "2020-02-29 19:28:44");

    ts = datetime_to_timestamp(str_to_datetime("2024-03-31 19:28:44"));
    date_add_interval(ts, 23, TimeUnit::MONTH);
    EXPECT_EQ(timestamp_to_str(ts), "2026-02-28 19:28:44");

    ts = datetime_to_timestamp(str_to_datetime("2024-03-31 19:28:44"));
    date_add_interval(ts, 47, TimeUnit::MONTH);
    EXPECT_EQ(timestamp_to_str(ts), "2028-02-29 19:28:44");

    // DAY
    ts = datetime_to_timestamp(str_to_datetime("2023-03-31 19:28:44"));
    date_add_interval(ts, -1, TimeUnit::DAY);
    EXPECT_EQ(timestamp_to_str(ts), "2023-03-30 19:28:44");

    ts = datetime_to_timestamp(str_to_datetime("2023-03-31 19:28:44"));
    date_add_interval(ts, 1, TimeUnit::DAY);
    EXPECT_EQ(timestamp_to_str(ts), "2023-04-01 19:28:44");

    ts = datetime_to_timestamp(str_to_datetime("2023-03-01 19:28:44"));
    date_add_interval(ts, -1, TimeUnit::DAY);
    EXPECT_EQ(timestamp_to_str(ts), "2023-02-28 19:28:44");

    ts = datetime_to_timestamp(str_to_datetime("2024-03-01 19:28:44"));
    date_add_interval(ts, -1, TimeUnit::DAY);
    EXPECT_EQ(timestamp_to_str(ts), "2024-02-29 19:28:44");

    // HOUR
    ts = datetime_to_timestamp(str_to_datetime("2024-03-01 01:28:44"));
    date_add_interval(ts, -1, TimeUnit::HOUR);
    EXPECT_EQ(timestamp_to_str(ts), "2024-03-01 00:28:44");

    ts = datetime_to_timestamp(str_to_datetime("2024-03-01 01:28:44"));
    date_add_interval(ts, -2, TimeUnit::HOUR);
    EXPECT_EQ(timestamp_to_str(ts), "2024-02-29 23:28:44");

    ts = datetime_to_timestamp(str_to_datetime("2023-03-01 01:28:44"));
    date_add_interval(ts, -2, TimeUnit::HOUR);
    EXPECT_EQ(timestamp_to_str(ts), "2023-02-28 23:28:44");

    // MINUTE
    ts = datetime_to_timestamp(str_to_datetime("2023-03-01 01:28:44"));
    date_add_interval(ts, -20, TimeUnit::MINUTE);
    EXPECT_EQ(timestamp_to_str(ts), "2023-03-01 01:08:44");

    ts = datetime_to_timestamp(str_to_datetime("2023-03-01 01:28:44"));
    date_add_interval(ts, -30, TimeUnit::MINUTE);
    EXPECT_EQ(timestamp_to_str(ts), "2023-03-01 00:58:44");

    // SECOND
    ts = datetime_to_timestamp(str_to_datetime("2023-03-01 01:28:44"));
    date_add_interval(ts, -40, TimeUnit::SECOND);
    EXPECT_EQ(timestamp_to_str(ts), "2023-03-01 01:28:04");

    ts = datetime_to_timestamp(str_to_datetime("2023-03-01 01:28:44"));
    date_add_interval(ts, -50, TimeUnit::SECOND);
    EXPECT_EQ(timestamp_to_str(ts), "2023-03-01 01:27:54");

    // date_sub_interval
    time_t add_ts = datetime_to_timestamp(str_to_datetime("2023-03-01 01:28:44"));
    time_t sub_ts = datetime_to_timestamp(str_to_datetime("2023-03-01 01:28:44"));
    date_add_interval(add_ts, -100, TimeUnit::MONTH);
    date_sub_interval(sub_ts,  100, TimeUnit::MONTH);
    EXPECT_EQ(add_ts, sub_ts);
}

TEST(test_snapshot_timestamp, case_all) {
    EXPECT_EQ(timestamp_to_str(snapshot_to_timestamp(35472024070100)), "2024-07-01 00:00:00");
    EXPECT_EQ(timestamp_to_str(snapshot_to_timestamp(35471970010100)), "0000-00-00 00:00:00");
    EXPECT_EQ(timestamp_to_str(snapshot_to_timestamp(35470000010100)), "0000-00-00 00:00:00");
    EXPECT_EQ(timestamp_to_str(snapshot_to_timestamp(35471971010100)), "1971-01-01 00:00:00");
}
}  // namespace baikal
