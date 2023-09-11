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
std::string timestamp_to_str(time_t timestamp, bool is_utc) {
    // 内部存储采用了uint32，因此小于0的都不合法
    if (timestamp <= 0) {
        return "0000-00-00 00:00:00";
    }
    struct tm tm;
    if (is_utc) {
        gmtime_r(&timestamp, &tm);
    } else {
        localtime_r(&timestamp, &tm);  
        // 夏令时影响
        if (tm.tm_isdst == 1) {
            timestamp = timestamp - 3600;
            localtime_r(&timestamp, &tm);
        }
    }
    char str_time[21] = {0};
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

uint64_t str_to_datetime(const char* str_time, bool* is_full_datetime) {
    //[YY]YY-MM-DD HH:MM:SS.xxxxxx
    //[YY]YYMMDDHHMMSS.xxxxxx

    bool is_full = false;

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
    // 兼容YYY-MM-DD
    if (buf[3] == '-') {
        has_delim = true;
    }
    int32_t year_length = -1;
    uint32_t idx = 0;
    for (; idx < len; ++idx) {
        if (has_delim) {
            if (!isdigit(buf[idx])) {
                delim_cnt++;
                if (year_length == -1) {
                    year_length = idx;
                }
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
        is_full = true;
    } else {
        if (idx <= 6) {
            sscanf(buf, "%2lu%2lu%2lu", &year, &month, &day);
            year_length = 2;
        } else if (idx == 8) {
            sscanf(buf, "%4lu%2lu%2lu", &year, &month, &day);
        } else if (idx == 12) {
            sscanf(buf, "%2lu%2lu%2lu%2lu%2lu%2lu.%6lu", 
                    &year, &month, &day, &hour, &minute, &second, &macrosec);
            is_full = true;
            year_length = 2;
        } else if (idx <= 13) {
            sscanf(buf, "%2lu%2lu%2lu%2lu%2lu%2lu", &year, &month, &day, &hour, &minute, &second);
            is_full = true;
            year_length = 2;
        } else if (idx >= 14) {
            sscanf(buf, "%4lu%2lu%2lu%2lu%2lu%2lu.%6lu", 
                    &year, &month, &day, &hour, &minute, &second, &macrosec);
            is_full = true;
        } else {
            return 0;
        }
    }
    if (year_length == 2) {
        if (year >= 70 && year < 100) {
            year += 1900;
        } else if (year < 70 && year > 0) {
            year += 2000;
        }
    }
    if (month > 12) {
        return 0;
    }
    if (day > 31) {
        return 0;
    }
    if (hour > 23 || minute > 59 || second > 59) {
        return 0;
    }

    if (is_full_datetime != nullptr) {
        *is_full_datetime = is_full;
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
    if (datetime == 0) {
        return 0;
    }
    struct tm tm;
    memset(&tm, 0, sizeof(tm));

    int year_month = ((datetime >> 46) & 0x1FFFF);
    tm.tm_year = year_month / 13;
    tm.tm_mon = year_month % 13;
    tm.tm_mday = ((datetime >> 41) & 0x1F);
    tm.tm_hour = ((datetime >> 36) & 0x1F);
    tm.tm_min = ((datetime >> 30) & 0x3F);
    tm.tm_sec = ((datetime >> 24) & 0x3F);
    tm.tm_isdst = 0;
    //int macrosec = (datetime & 0xFFFFFF);
    if (tm.tm_mon == 0) {
        return 0;
    }
    if (tm.tm_mday == 0) {
        return 0;
    }

    tm.tm_year -= 1900;
    tm.tm_mon--;
    time_t t = mktime(&tm);
    return t <= 0 ? 0 : t;
}

uint64_t timestamp_to_datetime(time_t timestamp) {
    if (timestamp == 0) {
        return 0;
    }
    uint64_t datetime = 0;

    struct tm tm;
    localtime_r(&timestamp, &tm);
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
        time_struct.month = tm.tm_mon + 1;
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

    // 先判断是否是完整的datetime类型字符串, 12为YYMMDDHHMMSS
    if (idx >= 12) {
        bool is_full_datetime = false;
        uint64_t datetime = str_to_datetime(str_time, &is_full_datetime);
        if (is_full_datetime) {
            return datetime_to_time(datetime);
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

bool tz_to_second(const char* time_zone, int32_t& result) {
    if (time_zone == nullptr) {
        return false;
    }
    if (strcmp(time_zone, "SYSTEM") == 0) {
        result = 0;
        time_t time_utc;
        struct tm tm_local;
        time(&time_utc);
 
        localtime_r(&time_utc, &tm_local);
 
        time_t time_local;
        struct tm tm_gmt;
 
        time_local = mktime(&tm_local);
 
        gmtime_r(&time_utc, &tm_gmt);
        int hour = tm_local.tm_hour - tm_gmt.tm_hour;
        if (hour < -12) {
            hour += 24; 
        } else if (hour > 12) {
            hour -= 24;
        }
        result = hour * 3600;
        return true;
    }
    int minu = 1;
    if (strlen(time_zone) != 6) {
        return false;
    }
    char minu_char = time_zone[0];
    if (minu_char == '-' ){
        minu = -1;
    } else if (minu_char != '+') {
        return false;
    }
    if (time_zone[3] != ':') {
        return false;
    }
    if (!isdigit(time_zone[1]) || !isdigit(time_zone[2]) || !isdigit(time_zone[4]) || !isdigit(time_zone[5])) {
        return false;
    }
    int hour = 10 * (time_zone[1] - '0') + time_zone[2] - '0';
    int min = 10 * (time_zone[4] - '0') + time_zone[5] - '0';
    if (hour > 12 || min > 59) {
        return false;
    }
    result =  (hour * 3600 + min * 60 ) * minu;
    return true;
}
// 此函数处理mysql和strftime不一致的格式，进行格式转换后，继续使用strftime函数.
// @ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-format
// @ref: https://www.cplusplus.com/reference/ctime/strftime
size_t date_format_internal(char* s, size_t maxsize, const char* format, const struct tm* tp) {
    if (tp == nullptr || format == nullptr) {
        return 0;
    }
    size_t i = 0;
    std::string f = "";
    char tmp[20] = {0};
    int hour12 = tp->tm_hour % 12;
    if (hour12 == 0) {
        hour12 = 12;
    }
    while (format[i] != '\0') {
        if (format[i] != '%' ) {
            f += format[i++];
            continue;
        }
        i++;
        if (format[i] == '\0') {
            break;
        }
        switch (format[i]) {
            case 'c':
                f += std::to_string(tp->tm_mon + 1);
                break;
            case 'D':
                f += std::to_string(tp->tm_mday);
                if (tp->tm_mday == 1 || tp->tm_mday == 21 || tp->tm_mday == 31) {
                    f += "st";
                }
                else if (tp->tm_mday == 2 || tp->tm_mday == 22) {
                    f += "nd";
                }
                else if (tp->tm_mday == 3 || tp->tm_mday == 23) {
                    f += "rd";
                } else {
                    f += "th";
                }
                break;
            case 'e':
                //mysql为月的天，strftime中<10时会带个空格
                f += std::to_string(tp->tm_mday);
                break;
            case 'f':
                //微妙数
                f += "000000";
                break;
            case 'h':
            case 'I':
                if (hour12 < 10) {
                    f += "0";
                }
                f += std::to_string(hour12);
                break;
            case 'i':
                f += "%M";
                break;
            case 'l':
                f += std::to_string(hour12);
                break;
            case 'M':
                f += "%B";
                break;
            case 'p':
                if (tp->tm_hour % 24 >= 12){
                    f += "PM";
                } else {
                    f += "AM";
                }
                break;
            case 'r':
                //02:12:00 AM，
                memset(tmp, 0, sizeof(tmp));
                snprintf(tmp, sizeof(tmp), "%02d:%02d:%02d ", hour12, tp->tm_min, tp->tm_sec);
                f += tmp;
                if (tp->tm_hour % 24 >= 12) {
                    f += "PM";
                } else {
                    f += "AM";
                }
                break;
            case 'v':
                f += "%V";
                break;
            case 'W':
                f += "%A";
                break;
            case 'X':
            case 'x':
                f += "%Y";
                break;
            case 's':
                f += "%S";
                break;
            case 'k':
                f += std::to_string(tp->tm_hour);
                break;
            case 'u':
                f += "%W";
                break;
            case 'Y':
            case 'y':
            case 'j':
            case 'm':
            case 'H':
            default:
                f += "%";
                f += format[i];
        }
        i++;
    }
    return strftime(s, maxsize, f.c_str(), tp);
}

int64_t timestamp_to_ts(uint32_t  timestamp) {
    return (((int64_t)timestamp) * 1000 - tso::base_timestamp_ms) << 18;
}

// Dynamic Partition
int get_current_timestamp(time_t& current_ts) {
    current_ts = ::time(NULL);
    return 0;
}

int get_specified_timestamp(const time_t& ts, const int64_t offset, TimeUnit time_unit, time_t& specified_ts) {
    struct tm tm;
    localtime_r(&ts, &tm);

    if (time_unit == TimeUnit::DAY) {
        tm.tm_mday += offset; 
    } else if (time_unit == TimeUnit::MONTH) {
        tm.tm_mon += offset;
    } else {
        return -1;
    }

    specified_ts = mktime(&tm);
    return 0;
}

int get_current_day_timestamp(time_t& current_day_ts) {
    struct tm tm;
    time_t current_ts = ::time(NULL);
    localtime_r(&current_ts, &tm);
    
    tm.tm_hour = 0;
    tm.tm_min  = 0;
    tm.tm_sec  = 0;

    current_day_ts = mktime(&tm);
    return 0;
}

int get_current_month_timestamp(const int start_day_of_month, time_t& current_month_ts) {
    struct tm tm;
    time_t current_ts = ::time(NULL);
    localtime_r(&current_ts, &tm);
    
    tm.tm_mday = start_day_of_month;
    tm.tm_hour = 0;
    tm.tm_min  = 0;
    tm.tm_sec  = 0;

    current_month_ts = mktime(&tm);
    return 0;
}

int timestamp_to_format_str(const time_t ts, const char* format, std::string& str) {
    if (ts <= 0) {
        return -1;
    }
    struct tm tm;
    localtime_r(&ts, &tm);

    char str_time[21] = {0};
    strftime(str_time, sizeof(str_time), format, &tm);
    str = std::string(str_time);
    return 0;
}

}  // baikaldb
