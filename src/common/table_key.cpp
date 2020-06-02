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

#include "table_key.h"
#include "mut_table_key.h"
#include "table_record.h"

namespace baikaldb {

TableKey::TableKey(const MutTableKey& key) : 
        _full(key.get_full()),
        _data(key.data()) {}

int TableKey::extract_index(IndexInfo& index, TableRecord* record, int& pos) {
    return record->decode_key(index, *this, pos);
}

//TODO: secondary key
int TableKey::decode_field(Message* message,
        const Reflection* reflection,
        const FieldDescriptor* field, 
        const FieldInfo& field_info,
        int& pos) const {
    switch (field_info.type) {
        case pb::INT8: {
            if (pos + sizeof(int8_t) > size()) {
                DB_WARNING("int8_t pos out of bound: %d %d %zu", field->number(), pos, size());
                return -2;
            }
            reflection->SetInt32(message, field, extract_i8(pos));
            pos += sizeof(int8_t);
        } break;
        case pb::INT16: {
            if (pos + sizeof(int16_t) > size()) {
                DB_WARNING("int16_t pos out of bound: %d %d %zu", field->number(), pos, size());
                return -2;
            }
            reflection->SetInt32(message, field, extract_i16(pos));
            pos += sizeof(int16_t);
        } break;
        case pb::TIME:
        case pb::INT32: {
            if (pos + sizeof(int32_t) > size()) {
                DB_WARNING("int32_t pos out of bound: %d %d %zu", field->number(), pos, size());
                return -2;
            }
            reflection->SetInt32(message, field, extract_i32(pos));
            pos += sizeof(int32_t);
        } break;
        case pb::INT64: {
            if (pos + sizeof(int64_t) > size()) {
                DB_WARNING("int64_t pos out of bound: %d %d %zu", field->number(), pos, size());
                return -2;
            }
            reflection->SetInt64(message, field, extract_i64(pos));
            pos += sizeof(int64_t);
        } break;
        case pb::UINT8: {
            if (pos + sizeof(uint8_t) > size()) {
                DB_WARNING("uint8_t pos out of bound: %d %d %zu", field->number(), pos, size());
                return -2;
            }
            reflection->SetUInt32(message, field, extract_u8(pos));
            pos += sizeof(uint8_t);
        } break;
        case pb::UINT16: {
            if (pos + sizeof(uint16_t) > size()) {
                DB_WARNING("uint16_t pos out of bound: %d %d %zu", field->number(), pos, size());
                return -2;
            }
            reflection->SetUInt32(message, field, extract_u16(pos));
            pos += sizeof(uint16_t);
        } break;
        case pb::TIMESTAMP:
        case pb::DATE:
        case pb::UINT32: {
            if (pos + sizeof(uint32_t) > size()) {
                DB_WARNING("uint32_t pos out of bound: %d %d %zu", field->number(), pos, size());
                return -2;
            }
            reflection->SetUInt32(message, field, extract_u32(pos));
            pos += sizeof(uint32_t);
        } break;
        case pb::DATETIME:
        case pb::UINT64: {
            if (pos + sizeof(uint64_t) > size()) {
                DB_WARNING("uint64_t pos out of bound: %d %d %zu", field->number(), pos, size());
                return -2;
            }
            reflection->SetUInt64(message, field, extract_u64(pos));
            pos += sizeof(uint64_t);
        } break;
        case pb::FLOAT: {
            if (pos + sizeof(float) > size()) {
                DB_WARNING("float pos out of bound: %d %d %zu", field->number(), pos, size());
                return -2;
            }
            reflection->SetFloat(message, field, extract_float(pos));
            pos += sizeof(float);
        } break;
        case pb::DOUBLE: {
            if (pos + sizeof(double) > size()) {
                DB_WARNING("double pos out of bound: %d %d %zu", field->number(), pos, size());
                return -2;
            }
            reflection->SetDouble(message, field, extract_double(pos));
            pos += sizeof(double);
        } break;
        case pb::BOOL: {
            if (pos + sizeof(uint8_t) > size()) {
                DB_WARNING("bool pos out of bound: %d %d %zu", field->number(), pos, size());
                return -2;
            }
            reflection->SetBool(message, field, extract_boolean(pos));
            pos += sizeof(uint8_t);
        } break;
        case pb::STRING: {
            //TODO no string pk-field is supported
            if (pos >= (int)size()) {
                DB_WARNING("string pos out of bound: %d %d %zu", field->number(), pos, size());
                return -2;
            }
            std::string str;
            extract_string(pos, str);
            reflection->SetString(message, field, str);
            pos += (str.size() + 1);
        } break;
        default: {
            DB_WARNING("un-supported field type: %d, %d", field->number(), field_info.type);
            return -1;
        } break;
    }
    return 0;
}

//TODO: secondary key
int TableKey::skip_field(const FieldInfo& field_info, int& pos) const {
    switch (field_info.type) {
        case pb::INT8: {
            pos += sizeof(int8_t);
        } break;
        case pb::INT16: {
            pos += sizeof(int16_t);
        } break;
        case pb::TIME:
        case pb::INT32: {
            pos += sizeof(int32_t);
        } break;
        case pb::INT64: {
            pos += sizeof(int64_t);
        } break;
        case pb::UINT8: {
            pos += sizeof(uint8_t);
        } break;
        case pb::UINT16: {
            pos += sizeof(uint16_t);
        } break;
        case pb::TIMESTAMP:
        case pb::DATE:
        case pb::UINT32: {
            pos += sizeof(uint32_t);
        } break;
        case pb::DATETIME:
        case pb::UINT64: {
            pos += sizeof(uint64_t);
        } break;
        case pb::FLOAT: {
            pos += sizeof(float);
        } break;
        case pb::DOUBLE: {
            pos += sizeof(double);
        } break;
        case pb::BOOL: {
            pos += sizeof(uint8_t);
        } break;
        case pb::STRING: {
            //TODO no string pk-field is supported
            if (pos >= (int)size()) {
                DB_WARNING("string pos out of bound: %d %zu", pos, size());
                return -2;
            }
            pos += (strlen(_data.data_) + 1);
        } break;
        default: {
            DB_WARNING("un-supported field type: %d", field_info.type);
            return -1;
        } break;
    }
    return 0;
}

} // end of namespace baikaldb
