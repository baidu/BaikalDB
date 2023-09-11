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

std::string TableKey::decode_start_key_string(pb::PrimitiveType field_type, int& pos) const {
    std::string start_key_string;
    switch (field_type) {
        case pb::INT8: {
            if (pos + sizeof(int8_t) > size()) {
                start_key_string = "decode fail";
            } else {
                start_key_string = std::to_string(extract_i8(pos));
            }
            pos += sizeof(int8_t);
        } break;
        case pb::INT16: {
            if (pos + sizeof(int16_t) > size()) {
                start_key_string = "decode fail";
            } else {
                start_key_string = std::to_string(extract_i16(pos));
            }
            pos += sizeof(int16_t);
        } break;
        case pb::TIME:
        case pb::INT32: {
            if (pos + sizeof(int32_t) > size()) {
                start_key_string = "decode fail";
            } else {
                ExprValue v(field_type);
                v._u.int32_val = extract_i32(pos);
                start_key_string = v.get_string();
            }
            pos += sizeof(int32_t);
        } break;
        case pb::UINT8: {
            if (pos + sizeof(uint8_t) > size()) {
                start_key_string = "decode fail";
            } else {
                start_key_string = std::to_string(extract_u8(pos));
            }
            pos += sizeof(uint8_t);
        } break;
        case pb::UINT16: {
            if (pos + sizeof(uint16_t) > size()) {
                start_key_string = "decode fail";
            } else {
                start_key_string = std::to_string(extract_u16(pos));
            }
            pos += sizeof(uint16_t);
        } break;
        case pb::UINT32:
        case pb::TIMESTAMP:
        case pb::DATE: {
            if (pos + sizeof(uint32_t) > size()) {
                start_key_string = "decode fail";
            } else {
                ExprValue v(field_type);
                v._u.uint32_val = extract_u32(pos);
                start_key_string = v.get_string();
            }
            pos += sizeof(uint32_t);
        } break;
        case pb::INT64: {
            if (pos + sizeof(int64_t) > size()) {
                start_key_string = "decode fail";
            } else {
                start_key_string = std::to_string(extract_i64(pos));
            }
            pos += sizeof(int64_t);
        } break;
        case pb::UINT64: 
        case pb::DATETIME: {
            if (pos + sizeof(uint64_t) > size()) {
                start_key_string = "decode fail";
            } else {
                ExprValue v(field_type);
                v._u.uint64_val = extract_u64(pos);
                start_key_string = v.get_string();
            }
            pos += sizeof(uint64_t);
        } break;
        case pb::FLOAT: {
            if (pos + sizeof(float) > size()) {
                start_key_string = "decode fail";
            } else {
                start_key_string = std::to_string(extract_float(pos));
            }
            pos += sizeof(float);
        } break;
        case pb::DOUBLE: {
            if (pos + sizeof(double) > size()) {
                start_key_string = "decode fail";
            } else {
                start_key_string = std::to_string(extract_double(pos));
            }
            pos += sizeof(double);
        } break;
        case pb::STRING: {
            if (pos >= (int)size()) {
                start_key_string = "";
            } else {
                extract_string(pos, start_key_string);
            }
            pos += (start_key_string.size() + 1);
        } break;
        case pb::BOOL: {
            if (pos + sizeof(uint8_t) > size()) {
                start_key_string = "decode fail";
            } else {
                start_key_string = std::to_string(extract_boolean(pos));
            }
            pos += sizeof(uint8_t);
        } break;
        default: {
            DB_WARNING("unsupport type:%d", field_type);
            start_key_string = "unsupport type";
            break;
        }
    }
    return start_key_string;
}

std::string TableKey::decode_start_key_string(const IndexInfo& index) const {
    std::string start_key_string;
    int pos = 0;
    for (auto& field : index.fields) {
        start_key_string += decode_start_key_string(field.type, pos);
        start_key_string += ",";
    }
    if (start_key_string.size() > 0) {
        start_key_string.pop_back();
    }
    return start_key_string;
}
    
std::string TableKey::decode_start_key_string(const std::vector<pb::PrimitiveType>& types, int32_t dimension) const {
    std::string start_key_string;
    int pos = 0;
    for (auto& field : types) {
        start_key_string += decode_start_key_string(field, pos);
        start_key_string += ",";
        dimension--;
        if (dimension <= 0) {
            break;
        }
    }
    if (start_key_string.size() > 0) {
        start_key_string.pop_back();
    }
    return start_key_string;
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
            pos += (strlen(_data.data_ + pos) + 1);
        } break;
        default: {
            DB_WARNING("un-supported field type: %d", field_info.type);
            return -1;
        } break;
    }
    return 0;
}

// Dynamic Partition
void TableKey::get_partition_col_pos(
        const std::vector<pb::PrimitiveType>& types,
        const size_t partition_col_num,
        int& pos) {
    if (types.size() < partition_col_num) {
        pos = -1;
        return;
    }
    for (size_t i = 0; i < partition_col_num; ++i) {
        get_partition_col_pos(types[i], pos);
        if (pos < 0) {
            return;
        }
    }
}

void TableKey::get_partition_col_pos(const pb::PrimitiveType& type, int& pos) {
    switch (type) {
        case pb::INT8: {
            if (pos + sizeof(int8_t) > size()) {
                pos = -1;
                return;
            }
            pos += sizeof(int8_t);
        } break;
        case pb::INT16: {
            if (pos + sizeof(int16_t) > size()) {
                pos = -1;
                return;
            }
            pos += sizeof(int16_t);
        } break;
        case pb::TIME:
        case pb::INT32: {
            if (pos + sizeof(int32_t) > size()) {
                pos = -1;
                return;
            }
            pos += sizeof(int32_t);
        } break;
        case pb::UINT8: {
            if (pos + sizeof(uint8_t) > size()) {
                pos = -1;
                return;
            }
            pos += sizeof(uint8_t);
        } break;
        case pb::UINT16: {
            if (pos + sizeof(uint16_t) > size()) {
                pos = -1;
                return;
            }
            pos += sizeof(uint16_t);

        } break;
        case pb::UINT32:
        case pb::TIMESTAMP:
        case pb::DATE: {
            if (pos + sizeof(uint32_t) > size()) {
                pos = -1;
                return;
            }
            pos += sizeof(uint32_t);
        } break;
        case pb::INT64: {
            if (pos + sizeof(int64_t) > size()) {
                pos = -1;
                return;
            }
            pos += sizeof(int64_t);
        } break;
        case pb::UINT64: 
        case pb::DATETIME: {
            if (pos + sizeof(uint64_t) > size()) {
                pos = -1;
                return;
            }
            pos += sizeof(uint64_t);
        } break;
        case pb::FLOAT: {
            if (pos + sizeof(float) > size()) {
                pos = -1;
                return;
            }
            pos += sizeof(float);
        } break;
        case pb::DOUBLE: {
            if (pos + sizeof(double) > size()) {
                pos = -1;
                return;
            }
            pos += sizeof(double);
        } break;
        case pb::STRING: {
            if (pos >= (int)size()) {
                pos = -1;
                return;
            }
            pos += (strlen(_data.data_ + pos) + 1);
        } break;
        case pb::BOOL: {
            if (pos + sizeof(uint8_t) > size()) {
                pos = -1;
                return;
            }
            pos += sizeof(uint8_t);
        } break;
        default: {
            pos = -1;
        } break;
    }
}

} // end of namespace baikaldb
