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

#include "expr_value.h"
#include "key_encoder.h"
#include "rocksdb/slice.h"

namespace baikaldb {

class TableKey;
class TableRecord;
class IndexInfo;
class MutTableKey {
public:
    virtual ~MutTableKey() {}
    MutTableKey() : _full(false) {}

    // create TableKey from a slice, use for extract fields
    MutTableKey(rocksdb::Slice key, bool full = true) : 
        _full(full), 
        _data(key.data_, key.size_) {}

    MutTableKey(const std::string& key, bool full) : 
        _full(full),
        _data(key.data(), key.size()) { }

    MutTableKey(const TableKey& key);

    MutTableKey& append_i8(int8_t val) {
        uint8_t encode = KeyEncoder::encode_i8(val);
        _data.append((char*)&encode, sizeof(uint8_t));
        return *this;
    }

    MutTableKey& append_u8(uint8_t val) {
        _data.append((char*)&val, sizeof(uint8_t));
        return *this;
    }

    MutTableKey& replace_u8(uint8_t val, int pos) {
        _data.replace(pos, 1, (char*)&val, 1);
        return *this;
    }

    MutTableKey& append_i16(int16_t val) {
        uint16_t encode = KeyEncoder::to_endian_u16(KeyEncoder::encode_i16(val));
        _data.append((char*)&encode, sizeof(uint16_t));
        return *this;
    }

    MutTableKey& append_u16(uint16_t val) {
        uint16_t encode = KeyEncoder::to_endian_u16(val);
        _data.append((char*)&encode, sizeof(uint16_t));
        return *this;
    }

    MutTableKey& append_i32(int32_t val) {
        uint32_t encode = KeyEncoder::to_endian_u32(KeyEncoder::encode_i32(val));
        _data.append((char*)&encode, sizeof(uint32_t));
        return *this;
    }

    MutTableKey& replace_i32(int32_t val, size_t pos) { // cstore
        uint32_t encode = KeyEncoder::to_endian_u32(KeyEncoder::encode_i32(val));
        size_t len = sizeof(uint32_t);
        _data.replace(pos, len, (char*)&encode, sizeof(uint32_t));
        return *this;
    }

    MutTableKey& append_u32(uint32_t val) {
        uint32_t encode = KeyEncoder::to_endian_u32(val);
        _data.append((char*)&encode, sizeof(uint32_t));
        return *this;
    }

    MutTableKey& append_i64(int64_t val) {
        uint64_t encode = KeyEncoder::to_endian_u64(KeyEncoder::encode_i64(val));
        _data.append((char*)&encode, sizeof(uint64_t));
        return *this;
    }

    MutTableKey& replace_i64(int64_t val, size_t pos) {
        uint64_t encode = KeyEncoder::to_endian_u64(KeyEncoder::encode_i64(val));
        size_t len = sizeof(uint64_t);
        _data.replace(pos, len, (char*)&encode, sizeof(uint64_t));
        return *this;
    }

    MutTableKey& append_u64(uint64_t val) {
        uint64_t encode = KeyEncoder::to_endian_u64(val);
        _data.append((char*)&encode, sizeof(uint64_t));
        return *this;
    }

    MutTableKey& append_float(float val) {
        uint32_t encode = KeyEncoder::to_endian_u32(KeyEncoder::encode_f32(val));
        _data.append((char*)&encode, sizeof(uint32_t));
        return *this;
    }

    MutTableKey& append_double(double val) {
        uint64_t encode = KeyEncoder::to_endian_u64(KeyEncoder::encode_f64(val));
        _data.append((char*)&encode, sizeof(uint64_t));
        return *this;
    }

    MutTableKey& append_string(const std::string& val) {
        _data.append(val);
        _data.append(1, '\0');
        return *this;
    }

    MutTableKey& append_string_prefix(const std::string& val) {
        _data.append(val);
        return *this;
    }

    MutTableKey& append_char(const char* data, size_t size) {
        _data.append(data, size);
        return *this;
    }

    MutTableKey& append_value(ExprValue& value) {
        switch (value.type) {
            case pb::BOOL:
                return append_boolean(value._u.bool_val);
            case pb::INT8:
                return append_i8(value._u.int8_val);
            case pb::INT16:
                return append_i16(value._u.int16_val);
            case pb::INT32:
            case pb::TIME:
                return append_i32(value._u.int32_val);
            case pb::INT64:
                return append_i64(value._u.int64_val);
            case pb::UINT8:
                return append_u8(value._u.uint8_val);
            case pb::UINT16:
                return append_u16(value._u.uint16_val);
            case pb::UINT32:
            case pb::TIMESTAMP:
            case pb::DATE:
                return append_u32(value._u.uint32_val);
            case pb::UINT64:
            case pb::DATETIME:
                return append_u64(value._u.uint64_val);
            case pb::FLOAT:
                return append_float(value._u.float_val);
            case pb::DOUBLE:
                return append_double(value._u.double_val);
            case pb::STRING:
                return append_string(value.str_val);
            default:
                return *this;
        }
    }

    MutTableKey& append_boolean(bool val) {
        uint8_t encode = val? uint8_t(1) : uint8_t(0);
        _data.append((char*)&encode, sizeof(uint8_t));
        return *this;
    }

    MutTableKey& append_index(const TableKey& key);

    MutTableKey& append_index(const MutTableKey& key) {
        _data.append(key._data);
        return *this;
    }
    
    MutTableKey& append_index(const std::string& key) {
        _data.append(key);
        return *this;
    }

    int append_index(IndexInfo& index, TableRecord* record, int field_cnt, bool clear);

    void set_full(bool full) {
        _full = full;
    }

    bool get_full() const{
        return _full;
    }

    size_t size() const {
        return _data.size();
    }

    const std::string& data() const {
        return _data;
    }

    std::string& data() {
        return _data;
    }

private:
    bool           _full;  //full key or just a prefix
    std::string    _data;

};

class TableKeyPair {
public:
    TableKeyPair(const std::string& left_key, bool left_full,
        const std::string& right_key, bool right_full) :
        _left_key(left_key, left_full), _right_key(right_key, right_full) {}
    const MutTableKey& left_key() const {
        return _left_key;
    }
    const MutTableKey& right_key() const {
        return _right_key;
    }
private:
    MutTableKey _left_key;
    MutTableKey _right_key;
};

}

