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
#include "mem_row.h"
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
        return decode_fields(fields, nullptr, &record, 0, nullptr);
    }
    int decode_fields(const std::map<int32_t, FieldInfo*>& fields, 
            std::vector<int32_t>& field_slot, int32_t tuple_id, std::unique_ptr<MemRow>& mem_row) {
        return decode_fields(fields, &field_slot, nullptr, tuple_id, &mem_row);
    }
    int decode_fields(const std::map<int32_t, FieldInfo*>& fields, const std::vector<int32_t>* field_slot,
            SmartRecord* record, int32_t tuple_id, std::unique_ptr<MemRow>* mem_row);

    int verification_fields(int32_t max_field_id);
private:
    const char*   _data;
    size_t  _size;
    size_t  _offset = 0;

};
}
