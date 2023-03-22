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
    EXPECT_EQ(datetime_to_str(str_to_datetime("2017-12-03 19:28:44.1234567")), "2017-12-03 19:28:44.123456");
    EXPECT_EQ(datetime_to_str(str_to_datetime("2017-12-03 19:28:44.123")), "2017-12-03 19:28:44.123000");

    EXPECT_EQ(datetime_to_str(str_to_datetime("2017-12-03 19:28:44.000123")), "2017-12-03 19:28:44.000123");
    EXPECT_EQ(datetime_to_str(str_to_datetime("2017-12-03 19:28:44.000123456")), "2017-12-03 19:28:44.000123");

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
    EXPECT_EQ(datetime_to_str(str_to_datetime("891203192844.111")), "1989-12-03 19:28:44.111000");
    EXPECT_EQ(datetime_to_str(str_to_datetime("19891203192844.111")), "1989-12-03 19:28:44.111000");
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

}  // namespace baikal
