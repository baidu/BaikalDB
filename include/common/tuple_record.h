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
#include "table_record.h"
#include "schema_factory.h"
#include "rocksdb/slice.h"

namespace baikaldb {
class TupleRecord {
public:
    TupleRecord(rocksdb::Slice slice) {
        _data = slice.data();
        _size = slice.size();
    }

    ~TupleRecord() {}

    void reset_offset() {
        _offset = 0;
    }
    void skip_varint() {
        while (_offset < _size && (_data[_offset++] >> 7) != 0);
    }

    template <typename T>
    T get_varint() {
        T raw = 0;
        int pos = 0;
        while (_offset < _size) {
            raw |= (((int64_t)_data[_offset] & 0x7F) << pos);
            pos += 7;
            if ((_data[_offset++] >> 7) == 0) {
                break;
            }
        }
        return raw;
    }

    template <typename T>
    void skip_fixed() {
        _offset += sizeof(T);
    }

    template <typename T>
    T get_fixed() {
        T val = *(T*)(_data + _offset);
        _offset += sizeof(T);
        return val;
    }

    void skip_string() {
        _offset += get_varint<uint64_t>();
    }

    std::string get_string() {
        uint64_t length = get_varint<uint64_t>();
        std::string val(_data + _offset, length);
        _offset += length;
        return val;
    }

    // decode 'required' (rather than 'all') fields from serialized protobuf bytes
    // and fill to SmartRecord, if null, fill default_value
    int decode_fields(const std::map<int32_t, FieldInfo*>& fields, SmartRecord record) {
        uint64_t field_key  = 0;
        uint64_t field_num  = 0;
        int32_t  wired_type = 0;
        auto iter = fields.begin();

        while (_offset < _size && iter != fields.end()) {
            field_key = get_varint<uint64_t>();
            field_num = field_key >> 3;
            wired_type = field_key & 0x07;

            if (_offset >= _size) {
                DB_WARNING("error: %lu, %lu", _offset, _size);
                return -1;
            }

            while (iter != fields.end() && field_num > static_cast<uint64_t>(iter->first)) {
                //add default value
                auto field = record->get_field_by_tag(iter->first);
                if (field == nullptr) {
                    DB_FATAL("invalid field: %d", iter->first);
                    return -1;
                }
                record->set_value(field, iter->second->default_expr_value);
                iter++;
            }
            if (iter == fields.end()) {
                //DB_WARNING("tag1: %d");
                return 0;
            }
            if (field_num < static_cast<uint64_t>(iter->first)) {
                // skip current field in proto
                if (wired_type == 0) {
                    skip_varint();
                } else if (wired_type == 1) {
                    skip_fixed<double>();
                } else if (wired_type == 2) {
                    skip_string();
                } else if (wired_type == 5) {
                    skip_fixed<float>();
                } else {
                    DB_FATAL("invalid wired_type: %d, offset: %lu,%lu", wired_type, _offset, _size);
                    return -1;
                }
            } else if (field_num == static_cast<uint64_t>(iter->first)) {
                auto field = record->get_field_by_tag(iter->first);
                if (field == nullptr) {
                    DB_FATAL("invalid field: %d, offset: %lu,%lu", iter->first, _offset, _size);
                    return -1;
                }
                switch (field->type()) {
                case FieldDescriptor::TYPE_SINT32: {
                    //zigzag
                    int32_t value = 0;
                    uint32_t raw_val = get_varint<uint32_t>();
                    if (raw_val & 0x1) {
                        value = (raw_val << 31) | ~(raw_val >> 1);
                    } else {
                        value = (raw_val >> 1);
                    }
                    record->set_int32(field, value);
                } break;
                case FieldDescriptor::TYPE_SINT64: {
                    //zigzag
                    int64_t value = 0;
                    uint64_t raw_val = get_varint<uint64_t>();
                    if (raw_val & 0x1) {
                        value = (raw_val << 63) | ~(raw_val >> 1);
                    } else {
                        value = (raw_val >> 1);
                    }
                    record->set_int64(field, value);
                } break;
                case FieldDescriptor::TYPE_INT32: {
                    record->set_int32(field, get_varint<int32_t>());
                } break;
                case FieldDescriptor::TYPE_INT64: {
                    record->set_int64(field, get_varint<int64_t>());
                } break;
                case FieldDescriptor::TYPE_SFIXED32: {
                    record->set_int32(field, get_fixed<int32_t>()); 
                } break;
                case FieldDescriptor::TYPE_SFIXED64: {
                    record->set_int64(field, get_fixed<int64_t>()); 
                } break;
                case FieldDescriptor::TYPE_UINT32: {
                    record->set_uint32(field, get_varint<uint32_t>());
                } break;
                case FieldDescriptor::TYPE_UINT64: {
                    record->set_uint64(field, get_varint<uint64_t>());
                } break;
                case FieldDescriptor::TYPE_FIXED32: {
                    record->set_uint32(field, get_fixed<uint32_t>());
                } break;
                case FieldDescriptor::TYPE_FIXED64: {
                    record->set_uint64(field, get_fixed<uint64_t>());
                } break;
                case FieldDescriptor::TYPE_FLOAT: {
                    record->set_float(field, get_fixed<float>());
                } break;
                case FieldDescriptor::TYPE_DOUBLE: {
                    record->set_double(field, get_fixed<double>());
                } break;
                case FieldDescriptor::TYPE_BOOL: {
                    record->set_boolean(field, get_varint<uint32_t>());
                } break;
                case FieldDescriptor::TYPE_STRING: 
                case FieldDescriptor::TYPE_BYTES: {
                    record->set_string(field, get_string());
                } break;
                default: {
                    DB_FATAL("invalid TYPE: %d, offset: %lu,%lu",
                            field->type(), iter->first, _offset, _size);
                    return -1;
                } break;
                }
                iter++;
            }
        }
        while (iter != fields.end()) {
            //add default value
            auto field = record->get_field_by_tag(iter->first);
            if (field == nullptr) {
                DB_WARNING("invalid field: %d", iter->first);
                return -1;
            }
            record->set_value(field, iter->second->default_expr_value);
            iter++;
        }
        return 0;
    }
private:
    const char*   _data;
    size_t  _size;
    size_t  _offset = 0;

};
}
