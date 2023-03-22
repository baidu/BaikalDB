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
#include <type_traits>
#include <iomanip>
#include <sstream>
#include <boost/lexical_cast.hpp>
#include "proto/common.pb.h"
#include "common.h"
#include "tdigest.h"
#include "datetime.h"
#include "type_utils.h"
#include "roaring.hh"

namespace baikaldb {

struct ExprValue {
    pb::PrimitiveType type;
    union {
        bool bool_val;
        int8_t int8_val;
        int16_t int16_val;
        int32_t int32_val;
        int64_t int64_val;
        uint8_t uint8_val;
        uint16_t uint16_val;
        uint32_t uint32_val;
        uint64_t uint64_val;
        float float_val;
        double double_val;
        Roaring* bitmap;
    } _u;
    std::string str_val;

    explicit ExprValue(pb::PrimitiveType type_ = pb::NULL_TYPE) : type(type_) {
        _u.int64_val = 0;
        if (type_ == pb::BITMAP) {
            _u.bitmap = new(std::nothrow) Roaring();
        } else if (type_ == pb::TDIGEST) {
            str_val.resize(tdigest::td_required_buf_size(tdigest::COMPRESSION));
            uint8_t* buff = (uint8_t*)str_val.data();
            tdigest::td_init(tdigest::COMPRESSION, buff, str_val.size());
        }
    }

    ExprValue(const ExprValue& other) {
        type = other.type;
        _u = other._u;
        str_val = other.str_val;
        if (type == pb::BITMAP) {
            _u.bitmap = new(std::nothrow) Roaring();
            *_u.bitmap = *other._u.bitmap;
        }
    }
    ExprValue& operator=(const ExprValue& other) {
        if (this != &other) {
            if (type == pb::BITMAP) {
                delete _u.bitmap;
                _u.bitmap = nullptr;
            }
            type = other.type;
            _u = other._u;
            str_val = other.str_val;
            if (type == pb::BITMAP) {
                _u.bitmap = new(std::nothrow) Roaring();
                *_u.bitmap = *other._u.bitmap;
            }
        }
        return *this;
    }

    ExprValue(ExprValue&& other) noexcept {
        type = other.type;
        _u = other._u;
        str_val = other.str_val;
        if (type == pb::BITMAP) {
            other._u.bitmap = nullptr;
        }
    }

    ExprValue& operator=(ExprValue&& other) noexcept {
        if (this != &other) {
            if (type == pb::BITMAP) {
                delete _u.bitmap;
                _u.bitmap = nullptr;
            }
            type = other.type;
            _u = other._u;
            if (type == pb::BITMAP) {
                other._u.bitmap = nullptr;
            }
            str_val = other.str_val;
        }
        return *this;
    }

    ~ExprValue() {
        if (type == pb::BITMAP) {
            delete _u.bitmap;
            _u.bitmap = nullptr;
        }
    }
    explicit ExprValue(const pb::ExprValue& value) {
        type = value.type();
        switch (type) {
            case pb::BOOL:
                _u.bool_val = value.bool_val();
                break;
            case pb::INT8:
                _u.int8_val = value.int32_val();
                break;
            case pb::INT16:
                _u.int16_val = value.int32_val();
                break;
            case pb::INT32:
            case pb::TIME:
                _u.int32_val = value.int32_val();
                break;
            case pb::INT64:
                _u.int64_val = value.int64_val();
                break;
            case pb::UINT8:
                _u.uint8_val = value.uint32_val();
                break;
            case pb::UINT16:
                _u.uint16_val = value.uint32_val();
                break;
            case pb::UINT32:
            case pb::TIMESTAMP:
            case pb::DATE:
                _u.uint32_val = value.uint32_val();
                break;
            case pb::UINT64:
            case pb::DATETIME:
                _u.uint64_val = value.uint64_val();
                break;
            case pb::FLOAT:
                _u.float_val = value.float_val();
                break;
            case pb::DOUBLE:
                _u.double_val = value.double_val();
                break;
            case pb::STRING:
            case pb::HLL:
            case pb::HEX:
            case pb::TDIGEST:
                str_val = value.string_val();
                break;
            case pb::BITMAP: {
                _u.bitmap = new(std::nothrow) Roaring();
                if (value.string_val().size() > 0) {
                    try {
                        *_u.bitmap = Roaring::readSafe(value.string_val().data(), value.string_val().size());
                    } catch (...) {
                        DB_WARNING("bitmap read from string failed");
                    }
                }
                break;
            }
            default:
                break;
        }
    }

    explicit ExprValue(pb::PrimitiveType primitive_type, const std::string& value_str) {
        type = pb::STRING;
        str_val = value_str;
        if (primitive_type == pb::STRING 
            || primitive_type == pb::HEX 
            || primitive_type == pb::BITMAP 
            || primitive_type == pb::HLL 
            || primitive_type == pb::TDIGEST) {
            return;
        }
        cast_to(primitive_type);
    }

    int common_prefix_length(const ExprValue& other) const {
        if (type != pb::STRING || other.type != pb::STRING) {
            return 0;
        }
        int min_len = str_val.size();
        if (min_len > (int)other.str_val.size()) {
            min_len = other.str_val.size();
        }
        for (int i = 0; i < min_len; i++) {
            if (str_val[i] != other.str_val[i]) {
                return i;
            }
        }
        return min_len;
    }

    double float_value(int prefix_len) {
        uint64_t val = 0;
        switch (type) {
            case pb::BOOL:
                return static_cast<double>(_u.bool_val);
            case pb::INT8:
                return static_cast<double>(_u.int8_val);
            case pb::INT16:
                return static_cast<double>(_u.int16_val);
            case pb::INT32:
                return static_cast<double>(_u.int32_val);
            case pb::INT64:
                return static_cast<double>(_u.int64_val);
            case pb::UINT8:
                return static_cast<double>(_u.uint8_val);
            case pb::UINT16:
                return static_cast<double>(_u.uint16_val );
            case pb::UINT32:
            case pb::TIMESTAMP:
                return static_cast<double>(_u.uint32_val);
            case pb::UINT64:
                return static_cast<double>(_u.uint64_val);
            case pb::FLOAT:
                return static_cast<double>(_u.float_val);
            case pb::DOUBLE:
                return _u.double_val;
            case pb::TIME:
            case pb::DATE:
            case pb::DATETIME:
                return static_cast<double>(cast_to(pb::TIMESTAMP)._u.uint32_val);
            case pb::STRING:
            case pb::HEX:
                if (prefix_len >= (int)str_val.size()) {
                    return 0.0;
                }
                for (int i = prefix_len; i < prefix_len + 8; i++) {
                    if (i < (int)str_val.size()) {
                        val += (val << 8) + uint8_t(str_val[i]);
                    } else {
                        val += val << 8;
                    }
                }
                return static_cast<double>(val);
            default:
                return 0.0;
        }
    }
    uint64_t unit64_value(int prefix_len) {
        DB_WARNING("unit64_value, prefix: %d, str: %s", prefix_len, str_val.c_str());
        uint64_t val = 0;
        if (type == pb::STRING) {
            if (prefix_len >= (int)str_val.size()) {
                return 0;
            }
            for (int i = prefix_len; i < prefix_len + 8; i++) {
                val <<= 8;
                if (i < (int)str_val.size()) {
                    val += uint8_t(str_val[i]);
                }
                DB_WARNING("i: %d, val: %lu", i, val);
            }
            return val;
        }
        return 0;
    }
    void to_proto(pb::ExprValue* value) {
        value->set_type(type);
        switch (type) {
            case pb::BOOL:
                value->set_bool_val(_u.bool_val);
                break;
            case pb::INT8:
                value->set_int32_val(_u.int8_val);
                break;
            case pb::INT16:
                value->set_int32_val(_u.int16_val);
                break;
            case pb::INT32:
            case pb::TIME:
                value->set_int32_val(_u.int32_val);
                break;
            case pb::INT64:
                value->set_int64_val(_u.int64_val);
                break;
            case pb::UINT8:
                value->set_uint32_val(_u.uint8_val);
                break;
            case pb::UINT16:
                value->set_uint32_val(_u.uint16_val );
                break;
            case pb::UINT32:
            case pb::TIMESTAMP:
            case pb::DATE:
                value->set_uint32_val(_u.uint32_val);
                break;
            case pb::UINT64:
            case pb::DATETIME:
                value->set_uint64_val(_u.uint64_val);
                break;
            case pb::FLOAT:
                value->set_float_val(_u.float_val);
                break;
            case pb::DOUBLE:
                value->set_double_val(_u.double_val);
                break;
            case pb::STRING:
            case pb::HEX:
            case pb::BITMAP:
            case pb::TDIGEST:
                value->set_string_val(str_val);
                break;
            default:
                break;
        }
    }

    template <class T>
    T get_numberic() const {
        switch (type) {
            case pb::BOOL:
                return _u.bool_val;
            case pb::INT8:
                return _u.int8_val;
            case pb::INT16:
                return _u.int16_val;
            case pb::INT32:
                return _u.int32_val;
            case pb::INT64:
                return _u.int64_val;
            case pb::UINT8:
                return _u.uint8_val;
            case pb::UINT16:
                return _u.uint16_val;
            case pb::UINT32:
                return _u.uint32_val;
            case pb::UINT64:
                return _u.uint64_val;
            case pb::FLOAT:
                return _u.float_val;
            case pb::DOUBLE:
                return _u.double_val;
            case pb::STRING:
                if (std::is_integral<T>::value) {
                    return strtoull(str_val.c_str(), NULL, 10);
                } else if (std::is_floating_point<T>::value) {
                    return strtod(str_val.c_str(), NULL);
                } else {
                    return 0;
                }
            case pb::HEX: {
                if (std::is_integral<T>::value || std::is_floating_point<T>::value) {
                    uint64_t value = 0;
                    for (char c : str_val) {
                        value = value * 256 + (uint8_t)c;
                    }
                    return value;
                } else {
                    return 0;
                }
            }
            case pb::DATETIME: {
                return _u.uint64_val;
            }
            case pb::TIME: {
                return _u.int32_val;
            }
            case pb::TIMESTAMP: {
                // internally timestamp is stored in uint32
                return _u.uint32_val;
            }
            case pb::DATE:
                return _u.uint32_val; 
            default:
                return 0;
        }
    }

    int64_t size() const {
        switch (type) {
            case pb::BOOL:
            case pb::INT8:
            case pb::UINT8:
                return 1;
            case pb::INT16:
            case pb::UINT16:
                return 2;
            case pb::INT32:
            case pb::UINT32:
                return 4;
            case pb::INT64:
            case pb::UINT64:
                return 8;
            case pb::FLOAT:
                return 4;
            case pb::DOUBLE:
                return 8;
            case pb::STRING:
            case pb::HEX:
            case pb::HLL:
            case pb::TDIGEST:
                return str_val.length();
            case pb::DATETIME:
                return 8;
            case pb::TIME:
            case pb::DATE:
            case pb::TIMESTAMP:
                return 4;
            case pb::BITMAP: {
                if (_u.bitmap != nullptr) {
                    return _u.bitmap->getSizeInBytes();
                } 
                return 0;
            }
            default:
                return 0;
        }
    }

    ExprValue& cast_to(pb::PrimitiveType type_) {
        if (is_null() || type == type_) {
            return *this;
        }
        switch (type_) {
            case pb::BOOL:
                _u.bool_val = get_numberic<bool>();
                break;
            case pb::INT8:
                _u.int8_val = get_numberic<int8_t>();
                break;
            case pb::INT16:
                _u.int16_val = get_numberic<int16_t>();
                break;
            case pb::INT32:
                _u.int32_val = get_numberic<int32_t>();
                break;
            case pb::INT64:
                _u.int64_val = get_numberic<int64_t>();
                break;
            case pb::UINT8:
                _u.uint8_val = get_numberic<uint8_t>();
                break;
            case pb::UINT16:
                _u.uint16_val = get_numberic<uint16_t>();
                break;
            case pb::UINT32:
                _u.uint32_val = get_numberic<uint32_t>();
                break;
            case pb::UINT64:
                _u.uint64_val = get_numberic<uint64_t>();
                break;
            case pb::DATETIME: {
                if (type == pb::STRING) {
                    _u.uint64_val = str_to_datetime(str_val.c_str());
                    str_val.clear();
                } else if (type == pb::TIMESTAMP) {
                    _u.uint64_val = timestamp_to_datetime(_u.uint32_val);
                } else if (type == pb::DATE) {
                    _u.uint64_val = date_to_datetime(_u.uint32_val);
                } else if (type == pb::TIME) {
                    _u.uint64_val = time_to_datetime(_u.int32_val);
                } else {
                    _u.uint64_val = get_numberic<uint64_t>();
                }
                break;
            }
            case pb::TIMESTAMP:
                if (!is_numberic()) {
                    _u.uint32_val = datetime_to_timestamp(cast_to(pb::DATETIME)._u.uint64_val);
                } else {
                    _u.uint32_val = get_numberic<uint32_t>();
                }
                break;
            case pb::DATE: {
                if (!is_numberic()) {
                    _u.uint32_val = datetime_to_date(cast_to(pb::DATETIME)._u.uint64_val);
                } else {
                    _u.uint32_val = get_numberic<uint32_t>();
                }
                break;
            }
            case pb::TIME: {
                if (is_numberic()) {
                    _u.int32_val = get_numberic<int32_t>();
                } else if (is_string()) {
                    _u.int32_val = str_to_time(str_val.c_str());
                } else {
                    _u.int32_val = datetime_to_time(cast_to(pb::DATETIME)._u.uint64_val);
                }
                break;
            }
            case pb::FLOAT:
                _u.float_val = get_numberic<float>();
                break;
            case pb::DOUBLE:
                _u.double_val = get_numberic<double>();
                break;
            case pb::STRING:
                str_val = get_string();
                break;
            case pb::BITMAP: {
                _u.bitmap = new(std::nothrow) Roaring();
                if (str_val.size() > 0) {
                    try {
                        *_u.bitmap = Roaring::readSafe(str_val.c_str(), str_val.size());
                    } catch (...) {
                        DB_WARNING("bitmap read from string failed");
                    }
                }
                break;
            }
            case pb::TDIGEST: {
                // 如果不是就构建一个新的
                if (!tdigest::is_td_object(str_val)) {
                    str_val.resize(tdigest::td_required_buf_size(tdigest::COMPRESSION));
                    uint8_t* buff = (uint8_t*)str_val.data();
                    tdigest::td_init(tdigest::COMPRESSION, buff, str_val.size());
                }
                break;
            }
            default:
                break;
        }
        type = type_;
        return *this;
    }

   uint64_t hash(uint32_t seed = 0x110) const {
        uint64_t out[2];
        switch (type) {
            case pb::BOOL:
            case pb::INT8:
            case pb::UINT8:
                butil::MurmurHash3_x64_128(&_u, 1, seed, out);
                return out[0];
            case pb::INT16:
            case pb::UINT16:
                butil::MurmurHash3_x64_128(&_u, 2, seed, out);
                return out[0];
            case pb::INT32:
            case pb::UINT32:
            case pb::FLOAT:
            case pb::TIMESTAMP:
            case pb::DATE:
            case pb::TIME:
                butil::MurmurHash3_x64_128(&_u, 4, seed, out);
                return out[0];
            case pb::INT64:
            case pb::UINT64:
            case pb::DOUBLE:
            case pb::DATETIME: 
                butil::MurmurHash3_x64_128(&_u, 8, seed, out);
                return out[0];
            case pb::STRING: 
            case pb::HEX: {
                butil::MurmurHash3_x64_128(str_val.c_str(), str_val.size(), seed, out);
                return out[0];
            }
            default:
                return 0;
        }
    }

    std::string get_string() const {
        switch (type) {
            case pb::BOOL:
                return std::to_string(_u.bool_val);
            case pb::INT8:
                return std::to_string(_u.int8_val);
            case pb::INT16:
                return std::to_string(_u.int16_val);
            case pb::INT32:
                return std::to_string(_u.int32_val);
            case pb::INT64:
                return std::to_string(_u.int64_val);
            case pb::UINT8:
                return std::to_string(_u.uint8_val);
            case pb::UINT16:
                return std::to_string(_u.uint16_val);
            case pb::UINT32:
                return std::to_string(_u.uint32_val);
            case pb::UINT64:
                return std::to_string(_u.uint64_val);
            case pb::FLOAT: {
                std::ostringstream oss;
                oss << _u.float_val;
                return oss.str();
            }
            case pb::DOUBLE: {
                std::ostringstream oss;
                oss << std::setprecision(15) << _u.double_val;
                return oss.str();
            }
            case pb::STRING:
            case pb::HEX:
            case pb::HLL:
            case pb::TDIGEST:
                return str_val;
            case pb::DATETIME:
                return datetime_to_str(_u.uint64_val);
            case pb::TIME:
                return time_to_str(_u.int32_val);
            case pb::TIMESTAMP:
                return timestamp_to_str(_u.uint32_val);
            case pb::DATE:
                return date_to_str(_u.uint32_val);
            case pb::BITMAP: {
                std::string final_str;
                if (_u.bitmap != nullptr) {
                    _u.bitmap->runOptimize();
                    uint32_t expectedsize = _u.bitmap->getSizeInBytes();
                    final_str.resize(expectedsize);
                    char* buff = (char*)final_str.data();
                    _u.bitmap->write(buff);
                }
                return final_str;
            }
            default:
                return "";
        }
    }

    void add(ExprValue& value) {
        switch (type) {
            case pb::BOOL:
                value._u.bool_val += value.get_numberic<bool>();
                return;
            case pb::INT8:
                _u.int8_val += value.get_numberic<int8_t>();
                return;
            case pb::INT16:
                _u.int16_val += value.get_numberic<int16_t>();
                return;
            case pb::INT32:
                _u.int32_val += value.get_numberic<int32_t>();
                return;
            case pb::INT64:
                _u.int64_val += value.get_numberic<int64_t>();
                return;
            case pb::UINT8:
                _u.uint8_val += value.get_numberic<uint8_t>();
                return;
            case pb::UINT16:
                _u.uint16_val += value.get_numberic<uint16_t>();
                return;
            case pb::UINT32:
                _u.uint32_val += value.get_numberic<uint32_t>();
                return;
            case pb::UINT64:
                _u.uint64_val += value.get_numberic<uint64_t>();
                return;
            case pb::FLOAT:
                _u.float_val+= value.get_numberic<float>();
                return;
            case pb::DOUBLE:
                _u.double_val+= value.get_numberic<double>();
                return;
            case pb::NULL_TYPE:
                *this = value;
                return;
            default:
                return;
        }
    }

    int64_t compare(const ExprValue& other) const {
        switch (type) {
            case pb::BOOL:
                return _u.bool_val - other._u.bool_val;
            case pb::INT8:
                return _u.int8_val - other._u.int8_val;
            case pb::INT16:
                return _u.int16_val - other._u.int16_val;
            case pb::INT32:
            case pb::TIME:
                return (int64_t)_u.int32_val - (int64_t)other._u.int32_val;
            case pb::INT64:
                return _u.int64_val > other._u.int64_val ? 1 :
                    (_u.int64_val < other._u.int64_val ? -1 : 0);
            case pb::UINT8:
                return _u.uint8_val - other._u.uint8_val;
            case pb::UINT16:
                return _u.uint16_val - other._u.uint16_val;
            case pb::UINT32:
            case pb::TIMESTAMP:
            case pb::DATE:
                return (int64_t)_u.uint32_val - (int64_t)other._u.uint32_val;
            case pb::UINT64:
            case pb::DATETIME:
                return _u.uint64_val > other._u.uint64_val ? 1 :
                    (_u.uint64_val < other._u.uint64_val ? -1 : 0);
            case pb::FLOAT:
                return _u.float_val > other._u.float_val ? 1 : 
                    (_u.float_val < other._u.float_val ? -1 : 0);
            case pb::DOUBLE:
                return _u.double_val > other._u.double_val ? 1 : 
                    (_u.double_val < other._u.double_val ? -1 : 0);
            case pb::STRING:
            case pb::HEX:
                return str_val.compare(other.str_val);
            case pb::NULL_TYPE:
                return -1;
            default:
                return 0;
        }
    }

    int64_t compare_diff_type(ExprValue& other) {
        if (type == other.type) {
            return compare(other);
        }
        if (is_int() && other.is_int()) {
            if (is_uint() || other.is_uint()) {
                cast_to(pb::UINT64);
                other.cast_to(pb::UINT64);
            } else {
                cast_to(pb::INT64);
                other.cast_to(pb::INT64);
            }
        } else if (is_datetime() || other.is_datetime()) {
            cast_to(pb::DATETIME);
            other.cast_to(pb::DATETIME);
        } else if (is_timestamp() || other.is_timestamp()) {
            cast_to(pb::TIMESTAMP);
            other.cast_to(pb::TIMESTAMP);
        } else if (is_date() || other.is_date()) {
            cast_to(pb::DATE);
            other.cast_to(pb::DATE);
        } else if (is_time() || other.is_time()) {
            cast_to(pb::TIME);
            other.cast_to(pb::TIME);
        } else if (is_double() || other.is_double()) {
            cast_to(pb::DOUBLE);
            other.cast_to(pb::DOUBLE);
        } else if (is_int() || other.is_int()) {
            cast_to(pb::DOUBLE);
            other.cast_to(pb::DOUBLE);
        } else {
            cast_to(pb::STRING);
            other.cast_to(pb::STRING);
        }
        return compare(other);
    }
    
    bool is_null() const { 
        return type == pb::NULL_TYPE || type == pb::INVALID_TYPE;
    }

    bool is_bool() const {
        return type == pb::BOOL;
    }

    bool is_string() const {
        return type == pb::STRING || type == pb::HEX || type == pb::BITMAP || type == pb::HLL || type == pb::TDIGEST;
    }

    bool is_double() const {
        return ::baikaldb::is_double(type);
    }

    bool is_int() const {
        return ::baikaldb::is_int(type);
    }

    bool is_uint() const {
        return ::baikaldb::is_uint(type);
    }

    bool is_datetime() const {
        return type == pb::DATETIME;
    }

    bool is_time() const {
        return type == pb::TIME;
    }

    bool is_timestamp() const {
        return type == pb::TIMESTAMP;
    }

    bool is_date() const {
        return type == pb::DATE;
    }

    bool is_hll() const {
        return type == pb::HLL;
    }

    bool is_tdigest() const {
        return type == pb::TDIGEST;
    }

    bool is_bitmap() const {
        return type == pb::BITMAP;
    }

    bool is_numberic() const {
        return is_int() || is_bool() || is_double();
    }
    
    bool is_place_holder() const {
        return type == pb::PLACE_HOLDER;
    }

    SerializeStatus serialize_to_mysql_text_packet(char* buf, size_t size, size_t& len) const;

    static ExprValue Null() {
        ExprValue ret(pb::NULL_TYPE);
        return ret;
    }
    static ExprValue False() {
        ExprValue ret(pb::BOOL);
        ret._u.bool_val = false;
        return ret;
    }
    static ExprValue True() {
        ExprValue ret(pb::BOOL);
        ret._u.bool_val = true;
        return ret;
    }
    static ExprValue Now(int precision = 6) {
        ExprValue tmp(pb::TIMESTAMP);
        tmp._u.uint32_val = time(NULL);
        tmp.cast_to(pb::DATETIME);
        if (precision == 6) {
            timeval tv;
            gettimeofday(&tv, NULL);
            tmp._u.uint64_val |= tv.tv_usec;
        }
        return tmp;
    }
    static ExprValue Bitmap() {
        ExprValue ret(pb::BITMAP);
        return ret;
    }
    static ExprValue Tdigest() {
        ExprValue ret(pb::TDIGEST);
        return ret;
    }
    static ExprValue Uint64() {
        ExprValue ret(pb::UINT64);
        return ret;
    }
    static ExprValue UTC_TIMESTAMP() {
        // static int UTC_OFFSET = 8 * 60 * 60;
        time_t current_time;
        struct tm timeinfo;
        localtime_r(&current_time, &timeinfo);
        long offset = timeinfo.tm_gmtoff;

        ExprValue tmp(pb::TIMESTAMP);
        tmp._u.uint32_val = time(NULL) - offset;
        tmp.cast_to(pb::DATETIME);
        timeval tv;
        gettimeofday(&tv, NULL);
        tmp._u.uint64_val |= tv.tv_usec;
        return tmp;
    }
    // For string, end-self
    double calc_diff(const ExprValue& end, int prefix_len) {
        ExprValue tmp_end = end;
        double ret = tmp_end.float_value(prefix_len) - float_value(prefix_len);
        DB_WARNING("start:%s, end:%s, prefix_len:%d, ret:%f",
                 get_string().c_str(), end.get_string().c_str(), prefix_len, ret);
        return ret;
    }

    // For ExprValueFlatSet
    bool operator==(const ExprValue& other) const {
        int64_t ret = other.compare(*this);
        if (ret != 0) {
            return false;
        }
        return true;
    }

    struct HashFunction {
        size_t operator()(const ExprValue& ev) const {
            if (ev.type == pb::STRING || ev.type == pb::HEX) {
                return ev.hash();
            }
            return ev._u.uint64_val;
        }
    };
};

using ExprValueFlatSet = butil::FlatSet<ExprValue, ExprValue::HashFunction>;

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
