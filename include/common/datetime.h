// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
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

#pragma once

#include "common.h"

namespace baikaldb {
extern std::string timestamp_to_str(time_t timestamp);
extern time_t str_to_timestamp(const char* str_time);

// encode DATETIME to string format
// ref: https://dev.mysql.com/doc/internals/en/date-and-time-data-type-representation.html
extern std::string datetime_to_str(uint64_t datetime);
extern uint64_t str_to_datetime(const char* str_time);

extern time_t datetime_to_timestamp(uint64_t datetime);
extern uint64_t timestamp_to_datetime(time_t timestamp);
inline uint32_t datetime_to_day(uint64_t datetime) {
    return ((datetime >> 41) & 0x1F);
}
inline uint32_t datetime_to_month(uint64_t datetime) {
    return ((datetime >> 46) & 0x1FFFF) % 13;
}
inline uint32_t datetime_to_year(uint64_t datetime) {
    return ((datetime >> 46) & 0x1FFFF) / 13;
}

extern int32_t datetime_to_time(uint64_t datetime);
extern uint64_t time_to_datetime(int32_t time);
extern std::string time_to_str(int32_t time);
extern int32_t str_to_time(const char* str_time);
extern int32_t seconds_to_time(int32_t seconds);
struct DateTime;
extern uint64_t bin_date_to_datetime(DateTime time_struct);
extern int32_t bin_time_to_datetime(DateTime time_struct);
extern void datetime_to_time_struct(uint64_t datetime, DateTime& time_struct, uint8_t type);
extern int64_t timestamp_to_ts(uint32_t timestamp);
// inline functions
// DATE: 17 bits year*13+month  (year 0-9999, month 1-12)
//        5 bits day            (1-31)
inline uint32_t datetime_to_date(uint64_t datetime) {
    return ((datetime >> 41) & 0x3FFFFF);
}
inline uint64_t date_to_datetime(uint32_t date) {
    return (uint64_t)date << 41;
}
inline std::string date_to_str(uint32_t date) {
    int year_month = ((date >> 5) & 0x1FFFF);
    int year = year_month / 13;
    int month = year_month % 13;
    int day = (date & 0x1F);
    char buf[30] = {0};
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day);
    return std::string(buf);
}

inline std::string ts_to_datetime_str(int64_t ts) {
    if (ts <= 0) {
        return "0000-00-00 00:00:00";
    }
    return timestamp_to_str(tso::get_timestamp_internal(ts));
}

extern bool tz_to_second(const char *time_zone, int32_t& result);

//此函数处理mysql和strftime不一致的格式，进行格式转换后，继续使用strftime函数.
extern size_t date_format_internal(char* s, size_t maxsize, const char* format, const struct tm* tp);
} // namespace baikaldb

