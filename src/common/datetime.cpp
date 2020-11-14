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

#include "datetime.h"
#include <unordered_map>
#include <cstdlib>
#include "expr_value.h"

namespace baikaldb {
std::string timestamp_to_str(time_t timestamp) {
    char str_time[21] = {0};
    struct tm tm;
    localtime_r(&timestamp, &tm);  
    // 夏令时影响
    if (tm.tm_isdst == 1) {
        timestamp = timestamp - 3600;
        localtime_r(&timestamp, &tm);
    }
    strftime(str_time, sizeof(str_time), "%Y-%m-%d %H:%M:%S", &tm);
    return std::string(str_time);
}
// encode DATETIME to string format
// ref: https://dev.mysql.com/doc/internals/en/date-and-time-data-type-representation.html
std::string datetime_to_str(uint64_t datetime) {
    int year_month = ((datetime >> 46) & 0x1FFFF);
    int year = year_month / 13;
    int month = year_month % 13;
    int day = ((datetime >> 41) & 0x1F);
    int hour = ((datetime >> 36) & 0x1F);
    int minute = ((datetime >> 30) & 0x3F);
    int second = ((datetime >> 24) & 0x3F);
    int macrosec = (datetime & 0xFFFFFF);

    char buf[30] = {0};
    if (macrosec > 0) {
        snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%06d",
                year, month, day, hour, minute, second, macrosec);
    } else {
        snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d",
                year, month, day, hour, minute, second);
    }
    return std::string(buf);
}

uint64_t str_to_datetime(const char* str_time) {
    //[YY]YY-MM-DD HH:MM:SS.xxxxxx
    //[YY]YYMMDDHHMMSS.xxxxxx
    while (*str_time == ' ') {
        str_time++;
    }
    const static size_t max_time_size = 26;
    size_t len = std::min(strlen(str_time), (size_t)max_time_size);
    char buf[max_time_size + 1] = {0};
    memcpy(buf, str_time, len);
    
    bool has_delim = true;
    int delim_cnt = 0;
    if (isdigit(buf[2]) && isdigit(buf[4])) {
        has_delim = false;
    }
    uint32_t idx = 0;
    for (; idx < len; ++idx) {
        if (has_delim) {
            if (!isdigit(buf[idx])) {
                delim_cnt++;
            }
            if (delim_cnt > 5 && buf[idx] == '.') {
                break;
            }
        } else {
            if (buf[idx] == '.') {
                break;
            }
        }
    }
    
    if (idx < len) {
        for (uint32_t i = idx + 1; i <= idx + 6 && i < max_time_size; ++i) {
            if (!isdigit(buf[i])) {
                buf[i] = '0';
            }
        }
    }

    uint64_t year = 0;
    uint64_t month = 0;
    uint64_t day = 0;
    uint64_t hour = 0;
    uint64_t minute = 0;
    uint64_t second = 0;
    uint64_t macrosec = 0;
    if (has_delim) {
        sscanf(buf, "%4lu%*[^0-9a-z]%2lu%*[^0-9a-z]%2lu"
                "%*[^0-9a-z]%2lu%*[^0-9a-z]%2lu%*[^0-9a-z]%2lu.%6lu",
                &year, &month, &day, &hour, &minute, &second, &macrosec);
    } else {
        if (idx <= 6) {
            sscanf(buf, "%2lu%2lu%2lu", &year, &month, &day);
        } else if (idx == 8) {
            sscanf(buf, "%4lu%2lu%2lu", &year, &month, &day);
        } else if (idx == 12) {
            sscanf(buf, "%2lu%2lu%2lu%2lu%2lu%2lu.%6lu", 
                    &year, &month, &day, &hour, &minute, &second, &macrosec);
        } else if (idx <= 13) {
            sscanf(buf, "%2lu%2lu%2lu%2lu%2lu%2lu", &year, &month, &day, &hour, &minute, &second);
        } else if (idx >= 14) {
            sscanf(buf, "%4lu%2lu%2lu%2lu%2lu%2lu.%6lu", 
                    &year, &month, &day, &hour, &minute, &second, &macrosec);
        } else {
            return 0;
        }
    }
    if (year > 70 && year < 100) {
        year += 1900;
    } else if (year < 70) {
        year += 2000;
    }
    if (month == 0 || month > 12) {
        return 0;
    }
    if (day > 31) {
        return 0;
    }
    if (hour > 23 || minute > 59 || second > 59) {
        return 0;
    }

    //datetime中间计算时会转化成int64, 最高位必须为0
    uint64_t datetime = 0;
    uint64_t year_month = year * 13 + month;
    datetime |= (year_month << 46);
    datetime |= (day << 41);
    datetime |= (hour << 36);
    datetime |= (minute << 30);
    datetime |= (second << 24);
    datetime |= macrosec;
    return datetime;
}

uint64_t bin_date_to_datetime(DateTime time_struct) {
    uint64_t year = time_struct.year;
    uint64_t month = time_struct.month;
    uint64_t day = time_struct.day;
    uint64_t hour = time_struct.hour;
    uint64_t minute = time_struct.minute;
    uint64_t second = time_struct.second;
    uint64_t macrosec = time_struct.macrosec;

    if (year > 70 && year < 100) {
        year += 1900;
    } else if (year < 70) {
        year += 2000;
    }
    if (month == 0 || month > 12) {
        return 0;
    }
    if (day > 31) {
        return 0;
    }
    if (hour > 23 || minute > 59 || second > 59) {
        return 0;
    }

    //datetime中间计算时会转化成int64, 最高位必须为0
    uint64_t datetime = 0;
    uint64_t year_month = year * 13 + month;
    datetime |= (year_month << 46);
    datetime |= (day << 41);
    datetime |= (hour << 36);
    datetime |= (minute << 30);
    datetime |= (second << 24);
    datetime |= macrosec;
    return datetime;
}

time_t datetime_to_timestamp(uint64_t datetime) {
    struct tm tm;
    memset(&tm, 0, sizeof(tm));

    int year_month = ((datetime >> 46) & 0x1FFFF);
    tm.tm_year = year_month / 13;
    tm.tm_mon = year_month % 13;
    tm.tm_mday = ((datetime >> 41) & 0x1F);
    tm.tm_hour = ((datetime >> 36) & 0x1F);
    tm.tm_min = ((datetime >> 30) & 0x3F);
    tm.tm_sec = ((datetime >> 24) & 0x3F);
    //int macrosec = (datetime & 0xFFFFFF);

    tm.tm_year -= 1900;
    tm.tm_mon--;
    return mktime(&tm);
}

uint64_t timestamp_to_datetime(time_t timestamp) {
    uint64_t datetime = 0;

    struct tm tm = *localtime(&timestamp);
    tm.tm_year += 1900;
    tm.tm_mon++;
    uint64_t year_month = tm.tm_year * 13 + tm.tm_mon;
    uint64_t day = tm.tm_mday;
    uint64_t hour = tm.tm_hour;
    uint64_t min = tm.tm_min;
    uint64_t sec = tm.tm_sec;
    datetime |= (year_month << 46);
    datetime |= (day << 41);
    datetime |= (hour << 36);
    datetime |= (min << 30);
    datetime |= (sec << 24);
    return datetime;
}

void datetime_to_time_struct(uint64_t datetime, DateTime& time_struct, uint8_t type) {
    if (type == MYSQL_TYPE_TIME) {
        int32_t time = (int32_t)datetime;
        if (time < 0) {
            time_struct.is_negative = 1;
            time = -time;
        }
        time_struct.hour = (time >> 12) & 0x3FF;
        time_struct.day = time_struct.hour / 24;
        time_struct.hour = time_struct.hour % 24;
        time_struct.minute = (time >> 6) & 0x3F;
        time_struct.second = time & 0x3F;
    } else if (type == MYSQL_TYPE_TIMESTAMP) {
        struct tm tm;
        time_t timestamp = (time_t)datetime;
        localtime_r(&timestamp, &tm);
        // 夏令时影响
        if (tm.tm_isdst == 1) {
            timestamp = timestamp - 3600;
            localtime_r(&timestamp, &tm);
        }
        time_struct.year = tm.tm_year + 1900;
        time_struct.month = tm.tm_mon++;
        time_struct.day = tm.tm_mday;
        time_struct.hour = tm.tm_hour;
        time_struct.minute = tm.tm_min;
        time_struct.second = tm.tm_sec;
        time_struct.macrosec =0;
    } else if (type == MYSQL_TYPE_DATETIME) {
        int year_month = ((datetime >> 46) & 0x1FFFF);
        time_struct.year = year_month / 13;
        time_struct.month = year_month % 13;
        time_struct.day = ((datetime >> 41) & 0x1F);
        time_struct.hour = ((datetime >> 36) & 0x1F);
        time_struct.minute = ((datetime >> 30) & 0x3F);
        time_struct.second = ((datetime >> 24) & 0x3F);
        //time_struct.macrosec = (datetime & 0xFFFFFF);
    }
}

int32_t datetime_to_time(uint64_t datetime) {
    int tm_hour = ((datetime >> 36) & 0x1F);
    int tm_min = ((datetime >> 30) & 0x3F);
    int tm_sec = ((datetime >> 24) & 0x3F);
    int32_t time = 0;
    time |= tm_sec;
    time |= (tm_min << 6);
    time |= (tm_hour << 12);
    return time;
}
uint64_t time_to_datetime(int32_t time) {
    ExprValue tmp(pb::TIMESTAMP);
    time_t now = ::time(NULL);
    now = ((now + 28800) / 86400) * 86400; // 去除时分秒 考虑时区UTC+8

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

    return timestamp_to_datetime(now);
}
std::string time_to_str(int32_t time) {
    bool minus = false;
    if (time < 0) {
        minus = true;
        time = -time;
    }
    int hour = (time >> 12) & 0x3FF;
    int min = (time >> 6) & 0x3F;
    int sec = time & 0x3F;
    static const char* OP_STR[] = {"", "-"};
    char buf[20] = {0};
    snprintf(buf, sizeof(buf), "%s%02d:%02d:%02d", OP_STR[minus], hour, min, sec);
    return std::string(buf);
}
int32_t str_to_time(const char* str_time) {
    while (*str_time == ' ') {
        str_time++;
    }
    bool minus = false;
    if (str_time[0] == '-') {
        minus = true;
        str_time++;
    }
    const static size_t max_time_size = 20;
    size_t len = std::min(strlen(str_time), (size_t)max_time_size);
    int day = 0;
    int hour = 0;
    int minute = 0;
    int second = 0;
    int32_t time = 0;

    bool has_blank = false;
    bool has_delim = false;
    uint32_t idx = 0;
    for (; idx < len; ++idx) {
        if (str_time[idx] == ' ') {
            has_blank = true;
            has_delim = true;
        }
        if (str_time[idx] == ':') {
            has_delim = true;
        }
        if (str_time[idx] == '.') {
            break;
        }
    }

    if (has_blank) {
        sscanf(str_time, "%d %u:%2u:%2u",
                &day, &hour, &minute, &second);
    } else if (has_delim) {
        sscanf(str_time, "%d:%2u:%2u",
                &hour, &minute, &second);
    } else {
        if (idx >= 4) {
            idx -= 2;
            std::string sec_str(str_time + idx, 2);
            second = strtod(sec_str.c_str(), NULL);
            idx -= 2;
            std::string min_str(str_time + idx, 2);
            minute = strtod(min_str.c_str(), NULL);
            std::string hour_str(str_time, idx);
            hour = strtod(hour_str.c_str(), NULL);
        } else if (idx >= 2) {
            idx -= 2;
            std::string sec_str(str_time + idx, 2);
            second = strtod(sec_str.c_str(), NULL);
            std::string min_str(str_time, idx);
            minute = strtod(min_str.c_str(), NULL);
        } else {
            std::string sec_str(str_time, idx);
            second = strtod(sec_str.c_str(), NULL);
        }
    }
    if (day < 0 || hour < 0 || minute < 0 || minute > 59 || second < 0 || second > 59) {
        return 0;
    }
    hour += day * 24;
    time |= second;
    time |= (minute << 6);
    time |= (hour << 12);
    if (minus) {
        time = -time;
    }
    return time;
}

int32_t bin_time_to_datetime(DateTime time_struct) {
    int day = time_struct.day;
    int hour = time_struct.hour;
    int minute = time_struct.minute;
    int second = time_struct.second;
    int32_t time = 0;
    if (day < 0 || hour < 0 || minute < 0 || minute > 59 || second < 0 || second > 59) {
        return 0;
    }
    hour += day * 24;
    time |= second;
    time |= (minute << 6);
    time |= (hour << 12);
    if (time_struct.is_negative) {
        time = -time;
    }
    return time;
}

int32_t seconds_to_time(int32_t seconds) {
    bool minus = false;
    if (seconds < 0) {
        minus = true;
        seconds = - seconds;
    }
    int sec = seconds % 60;
    int min = (seconds / 60) % 60;
    int hour = seconds / 3600;
    int32_t time = 0;
    time |= sec;
    time |= (min << 6);
    time |= (hour << 12);
    if (minus) {
        time = -time;
    }
    return time;
}
}  // baikaldb
