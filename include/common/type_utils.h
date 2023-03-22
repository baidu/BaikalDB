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

#include <stdint.h>
#include <string>
#include "proto/common.pb.h"
#include "parser.h"

namespace baikaldb {

enum MysqlType : uint8_t { 
    MYSQL_TYPE_DECIMAL,   // 0
    MYSQL_TYPE_TINY,
    MYSQL_TYPE_SHORT,  
    MYSQL_TYPE_LONG,
    MYSQL_TYPE_FLOAT,  
    MYSQL_TYPE_DOUBLE,   // 5
    MYSQL_TYPE_NULL,   
    MYSQL_TYPE_TIMESTAMP,
    MYSQL_TYPE_LONGLONG,
    MYSQL_TYPE_INT24,
    MYSQL_TYPE_DATE,     // 10
    MYSQL_TYPE_TIME,
    MYSQL_TYPE_DATETIME, 
    MYSQL_TYPE_YEAR,
    MYSQL_TYPE_NEWDATE, 
    MYSQL_TYPE_VARCHAR,
    MYSQL_TYPE_BIT,
    MYSQL_TYPE_TDIGEST = 242,
    MYSQL_TYPE_BITMAP = 243,
    MYSQL_TYPE_HLL = 244,
    MYSQL_TYPE_JSON = 245,
    MYSQL_TYPE_NEWDECIMAL = 246,
    MYSQL_TYPE_ENUM = 247,
    MYSQL_TYPE_SET = 248,
    MYSQL_TYPE_TINY_BLOB = 249,
    MYSQL_TYPE_MEDIUM_BLOB = 250,
    MYSQL_TYPE_LONG_BLOB = 251,
    MYSQL_TYPE_BLOB = 252,
    MYSQL_TYPE_VAR_STRING = 253,
    MYSQL_TYPE_STRING = 254,
    MYSQL_TYPE_GEOMETRY = 255
};

struct SignedType {
    MysqlType   mysql_type = MYSQL_TYPE_NULL;
    bool        is_unsigned = false;
};

// Package mysql result field.
// This struct is same as mysql protocal.
// https://dev.mysql.com/doc/internals/en/com-query-response.html#column-definition
struct ResultField {
    ResultField() {}
    ~ResultField() {}

    std::string     catalog = "def";
    std::string     db;
    std::string     table;
    std::string     org_table;
    std::string     name;       // Name of column
    std::string     org_name;
    uint16_t        charsetnr = 0;
    uint32_t        length = 0;     // Width of column (create length).
    uint8_t         type = 0;       // Type of field. See mysql_com.h for types.
    uint16_t        flags = 1;      // Div flags.
    uint8_t         decimals = 0;   // Number of decimals in field.
}; 

struct DateTime {
    uint64_t year = 0;
    uint64_t month = 0;
    uint64_t day = 0;
    uint64_t hour = 0;
    uint64_t minute = 0;
    uint64_t second = 0;
    uint64_t macrosec = 0;
    uint64_t is_negative = 0;

    int datetype_length() {
        if (year == 0 && month == 0 && day == 0 && hour == 0
            && minute == 0 && second == 0 && macrosec == 0) {
                return 0;
        } else if (hour == 0 && minute == 0 && second == 0 && macrosec == 0) {
            return 4;
        } else if (macrosec == 0) {
            return 7;
        }
        return 11;
    }
    int timetype_length() {
        if (hour == 0 && minute == 0 && second == 0 && macrosec == 0) {
            return 0;
        } else if (macrosec == 0) {
            return 8;
        }
        return 12;
    }
};

inline bool is_double(pb::PrimitiveType type) {
    switch (type) {
        case pb::FLOAT:
        case pb::DOUBLE:
            return true;
        default:
            return false;
    }
}

inline bool has_double(pb::PrimitiveType t1, pb::PrimitiveType t2) {
    return is_double(t1) || is_double(t2);
}

inline bool has_double(std::vector<pb::PrimitiveType> types) {
    for (auto type : types) {
        if (is_double(type)) {
            return true;
        }
    }
    return false;
}

inline bool is_datetime_specic(pb::PrimitiveType type) {
    switch (type) {
        case pb::DATETIME:
        case pb::TIMESTAMP:
        case pb::DATE:
        case pb::TIME:
            return true;
        default:
            return false;
    }
}

inline bool has_timestamp(std::vector<pb::PrimitiveType> types) {
    for (auto type : types) {
        if (type == pb::TIMESTAMP) {
            return true;
        }
    }
    return false;
}

inline bool is_current_timestamp_specic(pb::PrimitiveType type) {
    switch (type) {
        case pb::DATETIME:
        case pb::TIMESTAMP:
            return true;
        default:
            return false;
    }
}

inline bool has_datetime(std::vector<pb::PrimitiveType> types) {
    for (auto type : types) {
        if (type == pb::DATETIME) {
            return true;
        }
    }
    return false;
}

inline bool has_time(std::vector<pb::PrimitiveType> types) {
    for (auto type : types) {
        if (type == pb::TIME) {
            return true;
        }
    }
    return false;
}

inline bool has_date(std::vector<pb::PrimitiveType> types) {
    for (auto type : types) {
        if (type == pb::DATE) {
            return true;
        }
    }
    return false;
}

inline bool is_int(pb::PrimitiveType type) {
    switch (type) {
        case pb::INT8:
        case pb::INT16:
        case pb::INT32:
        case pb::INT64:
        case pb::UINT8:
        case pb::UINT16:
        case pb::UINT32:
        case pb::UINT64:
            return true;
        default:
            return false;
    }
}

inline bool is_uint(pb::PrimitiveType type) {
    switch (type) {
        case pb::UINT8:
        case pb::UINT16:
        case pb::UINT32:
        case pb::UINT64:
            return true;
        default:
            return false;
    }
}

inline bool has_uint(std::vector<pb::PrimitiveType> types) {
    for (auto type : types) {
        if (is_uint(type)) {
            return true;
        }
    }
    return false;
}

inline bool has_int(std::vector<pb::PrimitiveType> types) {
    for (auto type : types) {
        if (is_int(type)) {
            return true;
        }
    }
    return false;
}

inline bool all_uint(std::vector<pb::PrimitiveType> types) {
    for (auto type : types) {
        if (!is_uint(type)) {
            return false;
        }
    }
    return true;
}

inline bool all_int(std::vector<pb::PrimitiveType> types) {
    for (auto type : types) {
        if (!is_int(type)) {
            return false;
        }
    }
    return true;
}

inline bool is_string(pb::PrimitiveType type) {
    switch (type) {
        case pb::STRING:
            return true;
        default:
            return false;
    }
}

inline bool has_string(std::vector<pb::PrimitiveType> types) {
    for (auto type : types) {
        if (is_string(type)) {
            return true;
        }
    }
    return false;
}

inline int32_t get_num_size(pb::PrimitiveType type) {
    switch (type) {
        case pb::BOOL:
            return 1;
        case pb::INT8:
        case pb::UINT8:
            return 1;
        case pb::INT16:
        case pb::UINT16:
            return 2;
        case pb::INT32:
        case pb::UINT32:
        case pb::DATE:
        case pb::TIMESTAMP:
        case pb::TIME:
            return 4;
        case pb::INT64:
        case pb::UINT64:
        case pb::DATETIME:
            return 8;
        case pb::FLOAT:
            return 4;
        case pb::DOUBLE:
            return 8;
        default:
            return -1;
    }
}

inline bool is_binary(uint32_t flag) {
    switch (flag) {
        case parser::MYSQL_FIELD_FLAG_BLOB:
        case parser::MYSQL_FIELD_FLAG_BINARY:
            return true;
        default:
            return false;
    }
}

inline uint8_t to_mysql_type(pb::PrimitiveType type) {
    switch (type) {
        case pb::BOOL:
            return MYSQL_TYPE_TINY;
        case pb::INT8:
        case pb::UINT8:
            return MYSQL_TYPE_TINY;
        case pb::INT16:
        case pb::UINT16:
            return MYSQL_TYPE_SHORT;
        case pb::INT32:
        case pb::UINT32:
            return MYSQL_TYPE_LONG;
        case pb::INT64:
        case pb::UINT64:
            return MYSQL_TYPE_LONGLONG;
        case pb::FLOAT:
            return MYSQL_TYPE_FLOAT;
        case pb::DOUBLE:
            return MYSQL_TYPE_DOUBLE;
        case pb::STRING:
            return MYSQL_TYPE_STRING;
        case pb::DATETIME:
            return MYSQL_TYPE_DATETIME;
        case pb::DATE:
            return MYSQL_TYPE_DATE;
        case pb::TIME:
            return MYSQL_TYPE_TIME;
        case pb::TIMESTAMP:
            return MYSQL_TYPE_TIMESTAMP;
        case pb::HLL:
            return MYSQL_TYPE_LONGLONG;
        case pb::BITMAP:
        case pb::TDIGEST:
            return MYSQL_TYPE_STRING;
        default:
            return MYSQL_TYPE_STRING;
    }
}

inline std::string to_mysql_type_string(pb::PrimitiveType type) {
    switch (type) {
        case pb::BOOL:
            return "tinyint";
        case pb::INT8:
        case pb::UINT8:
            return "tinyint";
        case pb::INT16:
        case pb::UINT16:
            return "smallint";
        case pb::INT32:
        case pb::UINT32:
            return "int";
        case pb::INT64:
        case pb::UINT64:
            return "bigint";
        case pb::FLOAT:
            return "float";
        case pb::DOUBLE:
            return "double";
        case pb::STRING:
            return "text";
        case pb::DATETIME:
            return "datetime";
        case pb::DATE:
            return "date";
        case pb::TIME:
            return "time";
        case pb::TIMESTAMP:
            return "timestamp";
        case pb::HLL:
        case pb::BITMAP:
        case pb::TDIGEST:
            return "binary";
        default:
            return "text";
    }
}

inline std::string to_mysql_type_full_string(pb::PrimitiveType type) {
    switch (type) {
        case pb::BOOL:
            return "tinyint(3)";
        case pb::INT8:
            return "tinyint(3)";
        case pb::UINT8:
            return "tinyint(3) unsigned";
        case pb::INT16:
            return "smallint(5)";
        case pb::UINT16:
            return "smallint(5) unsigned";
        case pb::INT32:
            return "int(11)";
        case pb::UINT32:
            return "int(11) unsigned";
        case pb::INT64:
            return "bigint(21)";
        case pb::UINT64:
            return "bigint(21) unsigned";
        case pb::FLOAT:
            return "float";
        case pb::DOUBLE:
            return "double";
        case pb::STRING:
            return "text";
        case pb::DATETIME:
            return "datetime(6)";
        case pb::DATE:
            return "date";
        case pb::TIME:
            return "time";
        case pb::TIMESTAMP:
            return "timestamp(0)";
        case pb::HLL:
        case pb::BITMAP:
        case pb::TDIGEST:
            return "binary";
        default:
            return "text";
    }
}

inline bool is_signed(pb::PrimitiveType type) {
    switch (type) {
        case pb::INT8:
        case pb::INT16:
        case pb::INT32:
        case pb::INT64:
            return true;
        default:
            return false;
    }
}

inline bool is_compatible_type(pb::PrimitiveType src_type, pb::PrimitiveType target_type, bool is_compatible) {
    if (src_type == target_type) {
        return true;
    }
    if (target_type == pb::STRING) {
        return true;
    }
    int src_size = get_num_size(src_type);
    int target_size = get_num_size(target_type);
    if (src_size > 0 && target_size > 0) {
        if (is_compatible) {
            return true;
        }
        if (src_size <= target_size) {
            return true;
        }
    }
    return false;
}

inline bool has_merged_type(std::vector<pb::PrimitiveType>& types, pb::PrimitiveType& merged_type) {
    if (types.size() == 0) {
        return false;
    }

    bool is_all_equal = true;
    bool is_all_num = true;
    bool is_all_time = true;
    bool is_all_null = true;
    bool has_double = false;
    bool has_uint64 = false;
    bool has_signed = false;
    auto first_type = *types.begin();

    for (auto type : types) {
        if (type == pb::NULL_TYPE) {
            continue;
        }
        if (is_all_null) {
            first_type = type;
            is_all_null = false;
        }
        if (is_all_equal && type != first_type) {
            is_all_equal = false;
        }
        if (is_all_num && !(is_double(type) || is_int(type) || type == pb::BOOL)) {
            is_all_num = false;
        }
        if (is_all_time && !is_datetime_specic(type)) {
            is_all_time = false;
        }
        if (is_double(type)) {
            has_double = true;
        }
        if (type == pb::UINT64) {
            has_uint64 = true;
        }
        if (is_signed(type)) {
            has_signed = true;
        }
    }
    if (is_all_null) {
        merged_type = pb::NULL_TYPE;
    } else if (is_all_equal) {
       merged_type = first_type; 
    } else if (is_all_num) {
        if (has_double) {
            merged_type = pb::DOUBLE;
        } else if (has_uint64) {
            if (has_signed) {
                merged_type = pb::DOUBLE;
            } else {
                merged_type = pb::UINT64;
            }
        } else {
            merged_type = pb::INT64;
        }
    } else if (is_all_time) {
        merged_type = pb::DATETIME;
    } else {
        merged_type = pb::STRING;
    }
    return true;
}
}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
