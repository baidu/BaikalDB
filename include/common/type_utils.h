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

#pragma once

#include <stdint.h>
#include <string>
#include "proto/common.pb.h"

namespace baikaldb {

enum MysqlType : uint8_t { 
    MYSQL_TYPE_DECIMAL, 
    MYSQL_TYPE_TINY,
    MYSQL_TYPE_SHORT,  
    MYSQL_TYPE_LONG,
    MYSQL_TYPE_FLOAT,  
    MYSQL_TYPE_DOUBLE,
    MYSQL_TYPE_NULL,   
    MYSQL_TYPE_TIMESTAMP,
    MYSQL_TYPE_LONGLONG,
    MYSQL_TYPE_INT24,
    MYSQL_TYPE_DATE,   
    MYSQL_TYPE_TIME,
    MYSQL_TYPE_DATETIME, 
    MYSQL_TYPE_YEAR,
    MYSQL_TYPE_NEWDATE, 
    MYSQL_TYPE_VARCHAR,
    MYSQL_TYPE_BIT,
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

inline bool is_double(pb::PrimitiveType type) {
    switch (type) {
        case pb::FLOAT:
        case pb::DOUBLE:
            return true;
        default:
            return false;
    }
}

inline bool has_double(std::vector<pb::PrimitiveType> types) {
    for (auto type : types) {
        if (is_double(type)) {
            return true;
        }
    }
    return false;
}

inline bool has_timestamp(std::vector<pb::PrimitiveType> types) {
    for (auto type : types) {
        if (type == pb::TIMESTAMP) {
            return true;
        }
    }
    return false;
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
        default:
            return MYSQL_TYPE_BLOB;
    }
}

}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
