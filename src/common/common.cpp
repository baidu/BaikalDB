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

#include "common.h"
#include <unordered_map>
#include <cstdlib>

#ifdef BAIDU_INTERNAL
#include <pb_to_json.h>
#include <json_to_pb.h>
#else
#include <json2pb/pb_to_json.h>
#include <json2pb/json_to_pb.h>
#endif

#include "rocksdb/slice.h"
#include <boost/algorithm/string.hpp>
#include <google/protobuf/descriptor.pb.h>
#include "rocksdb/slice.h"

using google::protobuf::FieldDescriptorProto;

namespace baikaldb {

int64_t timestamp_diff(timeval _start, timeval _end) {
    return (_end.tv_sec - _start.tv_sec) * 1000000 
        + (_end.tv_usec-_start.tv_usec); //macro second
}

std::string pb2json(const google::protobuf::Message& message) {
    std::string json;
    std::string error;
#ifdef BAIDU_INTERNAL
    if (ProtoMessageToJson(message, &json, &error)) {
#else
    if (json2pb::ProtoMessageToJson(message, &json, &error)) {
#endif
        return json;
    }
    return error;
}

std::string json2pb(const std::string& json, google::protobuf::Message* message) {
    std::string error;
#ifdef BAIDU_INTERNAL
    if (JsonToProtoMessage(json, message, &error)) {
#else
    if (json2pb::JsonToProtoMessage(json, message, &error)) {
#endif
        return "";
    }
    return error;
}

// STMPS_SUCCESS,
// STMPS_FAIL,
// STMPS_NEED_RESIZE
SerializeStatus to_string (int32_t number, char *buf, size_t size, size_t& len) {
    if (number == 0U) {
        len = 1;
        if (size < 1) {
            return STMPS_NEED_RESIZE;
        }
        buf[0] = '0';
        return STMPS_SUCCESS;
    }
    if (number == INT32_MIN) {
        len = 11;
        if (size < len) {
            return STMPS_NEED_RESIZE;
        }
        memcpy(buf, "-2147483648", len);
        return STMPS_SUCCESS;
    }
    len = 0;
    if (number < 0) {
        number = -number;
        buf[0] = '-';
        len++;
    }

    int32_t n = number;
    while (n > 0) {
        n /= 10;
        len++;
    }
    if (len > size) {
        return STMPS_NEED_RESIZE;
    }
    int length = len;
    while (number > 0) {
        buf[--length] = '0' + (number % 10);
        number /= 10;
    }
    return STMPS_SUCCESS;
}

std::string to_string(int32_t number)
{
    char buffer[16];
    size_t len = 0;
    SerializeStatus ret = to_string(number, buffer, 16, len);
    if (ret == STMPS_SUCCESS) {
        buffer[len] = '\0';
        return std::string(buffer);
    }
    return "";
}

SerializeStatus to_string (uint32_t number, char *buf, size_t size, size_t& len) {

    if (number == 0U) {
        len = 1;
        if (size < len) {
            return STMPS_NEED_RESIZE;
        }
        buf[0] = '0';
        return STMPS_SUCCESS;
    }
    len = 0;
    uint32_t n = number;

    while (n > 0) {
        n /= 10;
        len++;
    }
    if (len > size) {
        return STMPS_NEED_RESIZE;
    }
    int length = len;
    while (number > 0) {
        buf[--length] = '0' + (number % 10);
        number /= 10;
    }

    return STMPS_SUCCESS;
}

std::string to_string(uint32_t number)
{
    char buffer[16];
    size_t len = 0;
    SerializeStatus ret = to_string(number, buffer, 16, len);
    if (ret == STMPS_SUCCESS) {
        buffer[len] = '\0';
        return std::string(buffer);
    }
    return "";
}

SerializeStatus to_string (int64_t number, char *buf, size_t size, size_t& len) {
    if (number == 0UL) {
        len = 1;
        if (size < len) {
            return STMPS_NEED_RESIZE;
        }
        buf[0] = '0';
        return STMPS_SUCCESS;
    }

    if (number == INT64_MIN) {
        len = 20;
        if (size < len) {
            return STMPS_NEED_RESIZE;
        }
        memcpy(buf, "-9223372036854775808", len);
        return STMPS_SUCCESS;
    }
    len = 0;
    if (number < 0) {
        number = -number;
        buf[0] = '-';
        len++;
    }

    int64_t n = number;
    while (n > 0) {
        n /= 10;
        len++;
    }
    if (len > size) {
        return STMPS_NEED_RESIZE;
    }
    int length = len;
    while (number > 0) {
        buf[--length] = '0' + (number % 10);
        number /= 10;
    }

    return STMPS_SUCCESS;
}

std::string to_string(int64_t number)
{
    char buffer[32];
    size_t len = 0;
    SerializeStatus ret = to_string(number, buffer, 32, len);
    if (ret == STMPS_SUCCESS) {
        buffer[len] = '\0';
        return std::string(buffer);
    }
    return "";
}

SerializeStatus to_string (uint64_t number, char *buf, size_t size, size_t& len) {
    if (number == 0UL) {
        len = 1;
        if (size < len) {
            return STMPS_NEED_RESIZE;
        }
        buf[0] = '0';
        return STMPS_SUCCESS;
    }
    len = 0;
    uint64_t n = number;

    while (n > 0) {
        n /= 10;
        len++;
    }
    if (len > size) {
        return STMPS_NEED_RESIZE;
    }

    int length = len;
    while (number > 0) {
        buf[--length] = '0' + (number % 10);
        number /= 10;
    }
    return STMPS_SUCCESS;
}

std::string to_string(uint64_t number)
{
    char buffer[32];
    size_t len = 0;
    SerializeStatus ret = to_string(number, buffer, 32, len);
    if (ret == STMPS_SUCCESS) {
        buffer[len] = '\0';
        return std::string(buffer);
    }
    return "";
}


std::string print_stacktrace()
{
    std::string res = "\n";
    int size = 16;
    void * array[16];
    int stack_num = backtrace(array, size);
    char ** stacktrace = backtrace_symbols(array, stack_num);
    for (int i = 0; i < stack_num; ++i)
    {
        res += std::string(stacktrace[i]);
        res += "\n";
        //printf("%s\n", stacktrace[i]);
    }
    free(stacktrace);
    return res;
}

std::string remove_quote(const char* str, char quote) {
    uint32_t len = strlen(str);
    if (len > 2 && str[0] == quote && str[len-1] == quote) {
        return std::string(str + 1, len - 2);
    } else {
        return std::string(str);
    }
}

std::string str_to_hex(const std::string& str) {
    return rocksdb::Slice(str).ToString(true).c_str();
}

void stripslashes(std::string& str) {
    size_t slow = 0;
    size_t fast = 0;
    bool has_slash = false;
    static std::unordered_map<char, char> trans_map = {
        {'\\', '\\'},
        {'\"', '\"'},
        {'\'', '\''},
        {'r', '\r'},
        {'t', '\t'},
        {'n', '\n'},
        {'b', '\b'},
        {'Z', '\x1A'},
    };
    while (fast < str.size()) {
        if (has_slash) {
            if (trans_map.count(str[fast]) == 1) {
                str[slow++] = trans_map[str[fast++]];
            } else if (str[fast] == '%' || str[fast] == '_') {
                // like中的特殊符号，需要补全'\'
                str[slow++] = '\\';
                str[slow++] = str[fast++];
            }
            has_slash = false;
        } else {
            if (str[fast] == '\\') {
                has_slash = true;
                fast++;
            } else if ((str[fast] & 0x80) != 0) {
                //gbk中文字符处理
                str[slow++] = str[fast++];
                if (fast >= str.size()) {
                    // 去除最后半个gbk中文
                    //--slow;
                    break;
                }
                str[slow++] = str[fast++];
            } else {
                str[slow++] = str[fast++];
            }
        }
    }
    str.resize(slow);
}

int primitive_to_proto_type(pb::PrimitiveType type) {
    using google::protobuf::FieldDescriptorProto;
    static std::unordered_map<int32_t, int32_t> _mysql_pb_type_mapping = {
        { pb::INT8,         FieldDescriptorProto::TYPE_SINT32 },
        { pb::INT16,        FieldDescriptorProto::TYPE_SINT32 },
        { pb::INT32,        FieldDescriptorProto::TYPE_SINT32 },
        { pb::INT64,        FieldDescriptorProto::TYPE_SINT64 },
        { pb::UINT8,        FieldDescriptorProto::TYPE_UINT32 },
        { pb::UINT16,       FieldDescriptorProto::TYPE_UINT32 },
        { pb::UINT32,       FieldDescriptorProto::TYPE_UINT32 },
        { pb::UINT64,       FieldDescriptorProto::TYPE_UINT64 },
        { pb::FLOAT,        FieldDescriptorProto::TYPE_FLOAT  },
        { pb::DOUBLE,       FieldDescriptorProto::TYPE_DOUBLE },
        { pb::STRING,       FieldDescriptorProto::TYPE_BYTES  },
        { pb::DATETIME,     FieldDescriptorProto::TYPE_FIXED64},
        { pb::TIMESTAMP,    FieldDescriptorProto::TYPE_FIXED32},
        { pb::DATE,         FieldDescriptorProto::TYPE_FIXED32},
        { pb::HLL,          FieldDescriptorProto::TYPE_BYTES},
        { pb::BOOL,         FieldDescriptorProto::TYPE_BOOL   }
    };
    if (_mysql_pb_type_mapping.count(type) == 0) {
        DB_WARNING("mysql_type %d not supported.", type);
        return -1;
    }
    return _mysql_pb_type_mapping[type];
}

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

time_t str_to_timestamp(const char* str_time) {
    if (str_time == nullptr) {
        return 0;
    }
    struct tm tm;
    memset(&tm, 0, sizeof(tm));
    sscanf(str_time, "%4d-%2d-%2d %2d:%2d:%2d",
           &tm.tm_year, &tm.tm_mon, &tm.tm_mday,  
           &tm.tm_hour, &tm.tm_min, &tm.tm_sec);

    tm.tm_year -= 1900;
    tm.tm_mon--;
    time_t ret = mktime(&tm);
    return ret;
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
    sprintf(buf, "%04d-%02d-%02d %02d:%02d:%02d.%06d",
        year, month, day, hour, minute, second, macrosec);
    return std::string(buf);
}

uint64_t str_to_datetime(const char* str_time) {
    //YYYY-MM-DD HH:MM:SS.xxxxxx
    const static size_t max_time_size = 26;
    size_t len = std::min(strlen(str_time), (size_t)max_time_size);
    char buf[max_time_size + 1] = {0};
    memcpy(buf, str_time, len);
    uint32_t idx = 0;
    for (; idx < len; ++idx) {
        if (buf[idx] == '.') {
            break;
        }
    }
    if (idx < len) {
        for (uint32_t i = idx + 1; i <= idx + 6 && i < max_time_size; ++i) {
            if (buf[i] < '0' || buf[i] > '9') {
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

    sscanf(buf, "%4lu-%2lu-%2lu %2lu:%2lu:%2lu.%6lu",
        &year, &month, &day, &hour, &minute, &second, &macrosec);

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
}  // baikaldb
