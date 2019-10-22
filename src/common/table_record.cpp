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
#include "schema_factory.h"
#include "key_encoder.h"
#include "table_record.h"

namespace baikaldb {

TableRecord::TableRecord(Message* _m) : _message(_m) {}

SmartRecord TableRecord::new_record(int64_t tableid) {
    auto factory = SchemaFactory::get_instance();
    return factory->new_record(tableid);
}

std::string TableRecord::get_index_value(IndexInfo& index) {
    std::string tmp;
    for (auto& field : index.fields) {
        auto tag_field = get_field_by_tag(field.id);
        tmp += get_value(tag_field).get_string();
        tmp += "-";
    }
    if (!tmp.empty()) {
        tmp.pop_back();
    }
    return tmp;
}

bool TableRecord::is_null(const FieldDescriptor* field) {
    const Reflection* _reflection = _message->GetReflection();
    if (!_reflection->HasField(*_message, field)) {
        return true;
    }
    return false;
}

int TableRecord::get_int32(const FieldDescriptor* field, int32_t& val) {
    const Reflection* _reflection = _message->GetReflection();
    if (!_reflection->HasField(*_message, field)) {
        DB_WARNING("missing field-value idx=%u", field->number());
        return -2;
    }
    val = _reflection->GetInt32(*_message, field);
    return 0;
}

void TableRecord::set_int32(const FieldDescriptor* field, int32_t val) {
    const Reflection* _reflection = _message->GetReflection();
    _reflection->SetInt32(_message, field, val);
}

int TableRecord::get_uint32(const FieldDescriptor* field, uint32_t& val) {
    const Reflection* _reflection = _message->GetReflection();
    if (!_reflection->HasField(*_message, field)) {
        DB_WARNING("missing field-value idx=%d", field->number());
        return -2;
    }
    val = _reflection->GetUInt32(*_message, field);
    return 0;
}

void TableRecord::set_uint32(const FieldDescriptor* field, uint32_t val) {
    const Reflection* _reflection = _message->GetReflection();
    _reflection->SetUInt32(_message, field, val);
}

int TableRecord::get_int64(const FieldDescriptor* field, int64_t& val) {
    const Reflection* _reflection = _message->GetReflection();
    if (!_reflection->HasField(*_message, field)) {
        DB_WARNING("missing field-value idx=%d", field->number());
        return -2;
    }
    val = _reflection->GetInt64(*_message, field);
    return 0;
}

void TableRecord::set_int64(const FieldDescriptor* field, int64_t val) {
    const Reflection* _reflection = _message->GetReflection();
    _reflection->SetInt64(_message, field, val);
}

int TableRecord::get_uint64(const FieldDescriptor* field, uint64_t& val) {
    const Reflection* _reflection = _message->GetReflection();
    if (!_reflection->HasField(*_message, field)) {
        DB_WARNING("missing field-value idx=%d", field->number());
        return -2;
    }
    val = _reflection->GetUInt64(*_message, field);
    return 0;
}

void TableRecord::set_uint64(const FieldDescriptor* field, uint64_t val) {
    const Reflection* _reflection = _message->GetReflection();
    _reflection->SetUInt64(_message, field, val);
}

int TableRecord::get_float(const FieldDescriptor* field, float& val) {
    const Reflection* _reflection = _message->GetReflection();
    if (!_reflection->HasField(*_message, field)) {
        DB_WARNING("missing field-value idx=%d", field->number());
        return -2;
    }
    val = _reflection->GetFloat(*_message, field);
    return 0;
}

void TableRecord::set_float(const FieldDescriptor* field, float val) {
    const Reflection* _reflection = _message->GetReflection();
    _reflection->SetFloat(_message, field, val);
}

int TableRecord::get_double(const FieldDescriptor* field, double& val) {
    const Reflection* _reflection = _message->GetReflection();
    if (!_reflection->HasField(*_message, field)) {
        DB_WARNING("missing field-value idx=%d", field->number());
        return -2;
    }
    val = _reflection->GetDouble(*_message, field);
    return 0;
}

void TableRecord::set_double(const FieldDescriptor* field, double val) {
    const Reflection* _reflection = _message->GetReflection();
    _reflection->SetDouble(_message, field, val);
}

int TableRecord::get_string(const FieldDescriptor* field, std::string& val) {
    const Reflection* _reflection = _message->GetReflection();
    if (!_reflection->HasField(*_message, field)) {
        DB_WARNING("missing field-value idx=%d", field->number());
        return -2;
    }
    val = _reflection->GetString(*_message, field);
    return 0;
}

void TableRecord::set_string(const FieldDescriptor* field, std::string val) {
    const Reflection* _reflection = _message->GetReflection();
    _reflection->SetString(_message, field, val);
}

int TableRecord::get_boolean(const FieldDescriptor* field, bool& val) {
    const Reflection* _reflection = _message->GetReflection();
    if (!_reflection->HasField(*_message, field)) {
        DB_WARNING("missing field-value idx=%d", field->number());
        return -2;
    }
    val = _reflection->GetBool(*_message, field);
    return 0;
}

void TableRecord::set_boolean(const FieldDescriptor* field, bool val) {
    const Reflection* _reflection = _message->GetReflection();
    _reflection->SetBool(_message, field, val);
}

// by_tag default true
ExprValue TableRecord::get_value(const FieldDescriptor* field) {
    if (field == nullptr) {
        return ExprValue::Null();
    }
    const Reflection* _reflection = _message->GetReflection();
    if (!_reflection->HasField(*_message, field)) {
        return ExprValue::Null();
    }
    auto type = field->cpp_type();
    switch (type) {
        case FieldDescriptor::CPPTYPE_INT32: {
            ExprValue value(pb::INT32);
            value._u.int32_val = _reflection->GetInt32(*_message, field);
            return value;
        } break;
        case FieldDescriptor::CPPTYPE_UINT32: {
            ExprValue value(pb::UINT32);
            value._u.uint32_val = _reflection->GetUInt32(*_message, field);
            return value;
        } break;
        case FieldDescriptor::CPPTYPE_INT64: {
            ExprValue value(pb::INT64);
            value._u.int64_val = _reflection->GetInt64(*_message, field);
            return value;
        } break;
        case FieldDescriptor::CPPTYPE_UINT64: {
            ExprValue value(pb::UINT64);
            value._u.uint64_val = _reflection->GetUInt64(*_message, field);
            return value;
        } break;
        case FieldDescriptor::CPPTYPE_FLOAT: {
            ExprValue value(pb::FLOAT);
            value._u.float_val = _reflection->GetFloat(*_message, field);
            return value;
        } break;
        case FieldDescriptor::CPPTYPE_DOUBLE: {
            ExprValue value(pb::DOUBLE);
            value._u.double_val = _reflection->GetDouble(*_message, field);
            return value;
        } break;
        case FieldDescriptor::CPPTYPE_BOOL: {
            ExprValue value(pb::BOOL);
            value._u.bool_val = _reflection->GetBool(*_message, field);
            return value;
        } break;
        case FieldDescriptor::CPPTYPE_STRING: {
            ExprValue value(pb::STRING);
            value.str_val = _reflection->GetString(*_message, field);
            return value;
        } 
        default: {
            return ExprValue::Null();
        }
    }
    return ExprValue::Null();
}

// by_tag default true
int TableRecord::set_value(const FieldDescriptor* field, const ExprValue& value) {
    if (field == nullptr) {
        DB_WARNING("Invalid Field Descriptor");
        return -1;
    }
    const Reflection* _reflection = _message->GetReflection();
    if (value.is_null()) {
        _reflection->ClearField(_message, field);
        return 0;
    }
    auto type = field->cpp_type();
    switch (type) {
        case FieldDescriptor::CPPTYPE_INT32: {
            _reflection->SetInt32(_message, field, value.get_numberic<int32_t>());
        } break;
        case FieldDescriptor::CPPTYPE_UINT32: {
            _reflection->SetUInt32(_message, field, value.get_numberic<uint32_t>());
        } break;
        case FieldDescriptor::CPPTYPE_INT64: {
            _reflection->SetInt64(_message, field, value.get_numberic<int64_t>());
        } break;
        case FieldDescriptor::CPPTYPE_UINT64: {
            _reflection->SetUInt64(_message, field, value.get_numberic<uint64_t>());
        } break;
        case FieldDescriptor::CPPTYPE_FLOAT: {
            _reflection->SetFloat(_message, field, value.get_numberic<float>());
        } break;
        case FieldDescriptor::CPPTYPE_DOUBLE: {
            _reflection->SetDouble(_message, field, value.get_numberic<double>());
        } break;
        case FieldDescriptor::CPPTYPE_BOOL: {
            _reflection->SetBool(_message, field, value.get_numberic<bool>());
        } break;
        case FieldDescriptor::CPPTYPE_STRING: {
            _reflection->SetString(_message, field, value.get_string());
        } break;
        default: {
            return -1;
        }
    }
    return 0;
}

int TableRecord::get_reverse_word(IndexInfo& index_info, std::string& word) {
    //int ret = 0;
    auto field = get_field_by_tag(index_info.fields[0].id);
    //DB_WARNING("index_info:%d id:%d", index_info.fields[0].type, index_info.fields[0].id);
    if (index_info.fields[0].type == pb::STRING) {
        return get_string(field, word);
    } else {
        /*
        MutTableKey index_key;
        ret = encode_key(index_info, index_key, -1, false);
        if (ret < 0) {
            return ret;
        }
        word = index_key.data();
        */
        word = get_value(field).get_string();
    }
    return 0;
}

void TableRecord::clear_field(const FieldDescriptor* field) {
    const Reflection* _reflection = _message->GetReflection();
    if (field != nullptr) {
        _reflection->ClearField(_message, field);
    }
}

int TableRecord::encode_field(const Reflection* _reflection,
        const FieldDescriptor* field, 
        const FieldInfo& field_info,
        MutTableKey& key, 
        bool clear, bool like_prefix) {
    switch (field_info.type) {
        case pb::INT8: {
            int32_t val = _reflection->GetInt32(*_message, field);
            key.append_i8((int8_t)val);
        } break;
        case pb::INT16: {
            int32_t val = _reflection->GetInt32(*_message, field);
            key.append_i16((int16_t)val);
        } break;
        case pb::TIME:
        case pb::INT32: {
            int32_t val = _reflection->GetInt32(*_message, field);
            key.append_i32(val);
        } break;
        case pb::INT64: {
            int64_t val = _reflection->GetInt64(*_message, field);
            key.append_i64(val);
        } break;
        case pb::UINT8: {
            uint32_t val = _reflection->GetUInt32(*_message, field);
            key.append_u8((uint8_t)val);
        } break;
        case pb::UINT16: {
            uint32_t val = _reflection->GetUInt32(*_message, field);
            key.append_u16((uint16_t)val);
        } break;
        case pb::TIMESTAMP:
        case pb::DATE: 
        case pb::UINT32: {
            uint32_t val = _reflection->GetUInt32(*_message, field);
            key.append_u32(val);
        } break;
        case pb::DATETIME:
        case pb::UINT64: {
            uint64_t val = _reflection->GetUInt64(*_message, field);
            key.append_u64(val);
        } break;
        case pb::FLOAT: {
            float val = _reflection->GetFloat(*_message, field);
            key.append_float(val);
        } break;
        case pb::DOUBLE: {
            double val = _reflection->GetDouble(*_message, field);
            key.append_double(val);
        } break;
        case pb::BOOL: {
            bool val = _reflection->GetBool(*_message, field);
            key.append_boolean(val);
        } break;
        case pb::STRING: {
            //TODO no string pk-field is supported
            std::string val = _reflection->GetString(*_message, field);;
            if (like_prefix) {
                key.append_string_prefix(val);   
            } else {
                key.append_string(val);
            }
        } break;
        default: {
            DB_WARNING("un-supported field type: %d, %d", field->number(), field_info.type);
            return -1;
        } break;

    }
    if (clear) {
        _reflection->ClearField(_message, field);
    }
    return 0;
}

//TODO: secondary key
int TableRecord::decode_field(const Reflection* _reflection,
        const FieldDescriptor* field, 
        const FieldInfo& field_info,
        const TableKey& key, 
        int& pos) {
    switch (field_info.type) {
        case pb::INT8: {
            if (pos + sizeof(int8_t) > key.size()) {
                DB_WARNING("int8_t pos out of bound: %d %d %zu", field->number(), pos, key.size());
                return -2;
            }
            _reflection->SetInt32(_message, field, key.extract_i8(pos));
            pos += sizeof(int8_t);
        } break;
        case pb::INT16: {
            if (pos + sizeof(int16_t) > key.size()) {
                DB_WARNING("int16_t pos out of bound: %d %d %zu", field->number(), pos, key.size());
                return -2;
            }
            _reflection->SetInt32(_message, field, key.extract_i16(pos));
            pos += sizeof(int16_t);
        } break;
        case pb::TIME:
        case pb::INT32: {
            if (pos + sizeof(int32_t) > key.size()) {
                DB_WARNING("int32_t pos out of bound: %d %d %zu", field->number(), pos, key.size());
                return -2;
            }
            _reflection->SetInt32(_message, field, key.extract_i32(pos));
            pos += sizeof(int32_t);
        } break;
        case pb::INT64: {
            if (pos + sizeof(int64_t) > key.size()) {
                DB_WARNING("int64_t pos out of bound: %d %d %zu", field->number(), pos, key.size());
                return -2;
            }
            _reflection->SetInt64(_message, field, key.extract_i64(pos));
            pos += sizeof(int64_t);
        } break;
        case pb::UINT8: {
            if (pos + sizeof(uint8_t) > key.size()) {
                DB_WARNING("uint8_t pos out of bound: %d %d %zu", field->number(), pos, key.size());
                return -2;
            }
            _reflection->SetUInt32(_message, field, key.extract_u8(pos));
            pos += sizeof(uint8_t);
        } break;
        case pb::UINT16: {
            if (pos + sizeof(uint16_t) > key.size()) {
                DB_WARNING("uint16_t pos out of bound: %d %d %zu", field->number(), pos, key.size());
                return -2;
            }
            _reflection->SetUInt32(_message, field, key.extract_u16(pos));
            pos += sizeof(uint16_t);
        } break;
        case pb::TIMESTAMP:
        case pb::DATE:
        case pb::UINT32: {
            if (pos + sizeof(uint32_t) > key.size()) {
                DB_WARNING("uint32_t pos out of bound: %d %d %zu", field->number(), pos, key.size());
                return -2;
            }
            _reflection->SetUInt32(_message, field, key.extract_u32(pos));
            pos += sizeof(uint32_t);
        } break;
        case pb::DATETIME:
        case pb::UINT64: {
            if (pos + sizeof(uint64_t) > key.size()) {
                DB_WARNING("uint64_t pos out of bound: %d %d %zu", field->number(), pos, key.size());
                return -2;
            }
            _reflection->SetUInt64(_message, field, key.extract_u64(pos));
            pos += sizeof(uint64_t);
        } break;
        case pb::FLOAT: {
            if (pos + sizeof(float) > key.size()) {
                DB_WARNING("float pos out of bound: %d %d %zu", field->number(), pos, key.size());
                return -2;
            }
            _reflection->SetFloat(_message, field, key.extract_float(pos));
            pos += sizeof(float);
        } break;
        case pb::DOUBLE: {
            if (pos + sizeof(double) > key.size()) {
                DB_WARNING("double pos out of bound: %d %d %zu", field->number(), pos, key.size());
                return -2;
            }
            _reflection->SetDouble(_message, field, key.extract_double(pos));
            pos += sizeof(double);
        } break;
        case pb::BOOL: {
            if (pos + sizeof(uint8_t) > key.size()) {
                DB_WARNING("bool pos out of bound: %d %d %zu", field->number(), pos, key.size());
                return -2;
            }
            _reflection->SetBool(_message, field, key.extract_boolean(pos));
            pos += sizeof(uint8_t);
        } break;
        case pb::STRING: {
            //TODO no string pk-field is supported
            if (pos >= (int)key.size()) {
                DB_WARNING("string pos out of bound: %d %d %zu", field->number(), pos, key.size());
                return -2;
            }
            std::string str;
            key.extract_string(pos, str);
            _reflection->SetString(_message, field, str);
            pos += (str.size() + 1);
        } break;
        default: {
            DB_WARNING("un-supported field type: %d, %d", field->number(), field_info.type);
            return -1;
        } break;
    }
    return 0;
}

int TableRecord::encode_key(IndexInfo& index, MutTableKey& key, int field_cnt, bool clear, bool like_prefx) {
    uint8_t null_flag = 0;
    int pos = (int)key.size();
    if (index.type == pb::I_NONE) {
        DB_WARNING("unknown table index type: %ld", index.id);
        return -1;
    }
    if (index.type == pb::I_KEY || index.type == pb::I_UNIQ) {
        key.append_u8(null_flag);
    }
    uint32_t col_cnt = (field_cnt == -1)? index.fields.size() : field_cnt;
    if (col_cnt > index.fields.size() || col_cnt > 8) {
        DB_WARNING("field_cnt out of bound: %ld, %d, %lu", 
            index.id, col_cnt, index.fields.size());
        return -1;
    }
    const Descriptor* _descriptor = _message->GetDescriptor();
    const Reflection* _reflection = _message->GetReflection();
    for (uint32_t idx = 0; idx < col_cnt; ++idx) {
        bool last_field_like_prefx = like_prefx && idx == (col_cnt - 1);
        auto& info = index.fields[idx];
        const FieldDescriptor* field = _descriptor->FindFieldByNumber(info.id);
        if (field == nullptr) {
            DB_WARNING("invalid field: %d", info.id);
            return -1;
        }
        int res = 0;
        if (index.type == pb::I_PRIMARY || index.type == pb::I_FULLTEXT) {
            if (!_reflection->HasField(*_message, field)) {
                DB_WARNING("missing pk field: %d", field->number());
                return -2;
            }
            res = encode_field(_reflection, field, info, key, clear, last_field_like_prefx);
        } else if (index.type == pb::I_KEY || index.type == pb::I_UNIQ) {
            if (!_reflection->HasField(*_message, field)) {
                // this field is null
                //DB_DEBUG("missing index field: %u, set null-flag", idx);
                if (!info.can_null) {
                    //DB_WARNING("encode not_null field");
                    res = encode_field(_reflection, field, info, key, clear, last_field_like_prefx);
                } else {
                    null_flag |= (0x01 << (7 - idx));
                }
            } else {
                res = encode_field(_reflection, field, info, key, clear, last_field_like_prefx);
            }
        } else {
            DB_WARNING("invalid index type: %u", index.type);
            return -1;
        }
        if (0 != res) {
            DB_WARNING("encode index field error: %u, %d", idx, res);
            return -1;
        }
    }
    if (index.type == pb::I_KEY || index.type == pb::I_UNIQ) {
        key.replace_u8(null_flag, pos);
    }
    key.set_full((index.type == pb::I_PRIMARY || index.type == pb::I_UNIQ)
        && col_cnt == index.fields.size());
    //DB_WARNING("key size: %ld, %s", index.id, rocksdb::Slice(key.data()).ToString(true).c_str());
    return 0;
}

// this func is only used for secondary index
int TableRecord::encode_primary_key(IndexInfo& index, MutTableKey& key, int field_cnt) {
    if (index.type != pb::I_KEY && index.type != pb::I_UNIQ) {
        DB_WARNING("invalid secondary index type: %ld", index.id);
        return -1;
    }
    uint32_t col_cnt = (field_cnt == -1)? index.pk_fields.size() : field_cnt;

    int res = 0;
    const Descriptor* _descriptor = _message->GetDescriptor();
    const Reflection* _reflection = _message->GetReflection();
    for (uint32_t idx = 0; idx < col_cnt; ++idx) {
        auto& info = index.pk_fields[idx];
        const FieldDescriptor* field = _descriptor->FindFieldByNumber(info.id);
        if (field == nullptr) {
            DB_WARNING("invalid field: %d", info.id);
            return -1;
        }
        if (!_reflection->HasField(*_message, field)) {
            DB_WARNING("missing pk field: %d", field->number());
            return -2;
        }
        res = encode_field(_reflection, field, info, key, false, false);
        if (0 != res) {
            DB_WARNING("encode index field error: %u, %d", idx, res);
            return -1;
        }
    }
    return 0;
}

//decode and fill into *this (primary/secondary) starting from 0
int TableRecord::decode_key(IndexInfo& index, const std::string& key) {
    TableKey pkey(key, true);
    return decode_key(index, pkey);
}

int TableRecord::decode_key(IndexInfo& index, const std::string& key, int& pos) {
    TableKey pkey(key, true);
    return decode_key(index, pkey, pos);
}

int TableRecord::decode_key(IndexInfo& index, const TableKey& key) {
    int pos = 0;
    return decode_key(index, key, pos);
}

int TableRecord::decode_key(IndexInfo& index, const TableKey& key, int& pos) {
    if (index.type == pb::I_NONE) {
        DB_WARNING("unknown table index type: %ld", index.id);
        return -1;
    }
    uint8_t null_flag = 0;
    const Descriptor* _descriptor = _message->GetDescriptor();
    const Reflection* _reflection = _message->GetReflection();
    if (index.type == pb::I_KEY || index.type == pb::I_UNIQ) {
        null_flag = key.extract_u8(pos);
        pos += sizeof(uint8_t);
    }
    for (uint32_t idx = 0; idx < index.fields.size(); ++idx) {
        const FieldDescriptor* field = _descriptor->FindFieldByNumber(index.fields[idx].id);
        if (field == nullptr) {
            DB_WARNING("invalid field: %d", index.fields[idx].id);
            return -1;
        }
        // DB_WARNING("null_flag: %ld, %u, %d, %d, %s", 
        //     index.id, null_flag, pos, index.fields[idx].can_null, 
        //     key.data().ToString(true).c_str());
        if (((null_flag >> (7 - idx)) & 0x01) && index.fields[idx].can_null) {
            //DB_DEBUG("field is null: %d", idx);
            continue;
        }
        if (0 != decode_field(_reflection, field, index.fields[idx], key, pos)) {
            DB_WARNING("decode index field error");
            return -1;
        }
    }
    return 0;
}

int TableRecord::decode_primary_key(IndexInfo& index, const TableKey& key, int& pos) {
    if (index.type != pb::I_KEY && index.type != pb::I_UNIQ) {
        DB_WARNING("invalid secondary index type: %ld", index.id);
        return -1;
    }
    const Descriptor* _descriptor = _message->GetDescriptor();
    const Reflection* _reflection = _message->GetReflection();
    for (uint32_t idx = 0; idx < index.pk_fields.size(); ++idx) {
        const FieldDescriptor* field = _descriptor->FindFieldByNumber(index.pk_fields[idx].id);
        if (field == nullptr) {
            DB_WARNING("invalid field: %d", index.pk_fields[idx].id);
            return -1;
        }
        if (0 != decode_field(_reflection, field, index.pk_fields[idx], key, pos)) {
            DB_WARNING("decode index field error: field_id: %d, type: %d", 
                index.pk_fields[idx].id, index.pk_fields[idx].type);
            return -1;
        }
    }
    return 0;
}
// for cstore
// return -3 when equals to default value
int TableRecord::encode_field(const FieldInfo& field_info, std::string& out) {
    const Descriptor* _descriptor = _message->GetDescriptor();
    const Reflection* _reflection = _message->GetReflection();
    int32_t field_id = field_info.id;
    pb::PrimitiveType field_type = field_info.type;
    const FieldDescriptor* field = _descriptor->FindFieldByNumber(field_id);
    if (field == nullptr) {
        DB_WARNING("invalid field: %d", field_id);
        return -1;
    }
    if (!_reflection->HasField(*_message, field)) {
        DB_WARNING("missing field: %d", field->number());
        return -2;
    }
    // skip default value
    switch (field_type) {
        case pb::INT8: {
            int8_t val = _reflection->GetInt32(*_message, field);
            if (!field_info.default_expr_value.is_null()) {
                if (val == field_info.default_expr_value._u.int8_val) {
                    return -3;
                }
            }
            out.append((char*)&val, sizeof(int8_t));
        } break;
        case pb::INT16: {
            int16_t val = _reflection->GetInt32(*_message, field);
            if (!field_info.default_expr_value.is_null()) {
                if (val == field_info.default_expr_value._u.int16_val) {
                    return -3;
                }
            }
            uint16_t encode = KeyEncoder::to_little_endian_u16(static_cast<uint16_t>(val));
            out.append((char*)&encode, sizeof(uint16_t));
        } break;
        case pb::TIME:
        case pb::INT32: {
            int32_t val = _reflection->GetInt32(*_message, field);
            if (!field_info.default_expr_value.is_null()) {
                if (val == field_info.default_expr_value._u.int32_val) {
                    return -3;
                }
            }
            uint32_t encode = KeyEncoder::to_little_endian_u32(static_cast<uint32_t>(val));
            out.append((char*)&encode, sizeof(uint32_t));
        } break;
        case pb::INT64: {
            int64_t val = _reflection->GetInt64(*_message, field);
            if (!field_info.default_expr_value.is_null()) {
                if (val == field_info.default_expr_value._u.int64_val) {
                    return -3;
                }
            }
            uint64_t encode = KeyEncoder::to_little_endian_u64(static_cast<uint64_t>(val));
            out.append((char*)&encode, sizeof(uint64_t));
        } break;
        case pb::UINT8: {
            uint8_t val = _reflection->GetUInt32(*_message, field);
            if (!field_info.default_expr_value.is_null()) {
                if (val == field_info.default_expr_value._u.uint8_val) {
                    return -3;
                }
            }
            out.append((char*)&val, sizeof(uint8_t));
        } break;
        case pb::UINT16: {
            uint16_t val = _reflection->GetUInt32(*_message, field);
            if (!field_info.default_expr_value.is_null()) {
                if (val == field_info.default_expr_value._u.uint16_val) {
                    return -3;
                }
            }
            uint16_t encode = KeyEncoder::to_little_endian_u16(val);
            out.append((char*)&encode, sizeof(uint16_t));
        } break;
        case pb::TIMESTAMP:
        case pb::DATE:
        case pb::UINT32: {
            uint32_t val = _reflection->GetUInt32(*_message, field);
            if (!field_info.default_expr_value.is_null()) {
                if (val == field_info.default_expr_value._u.uint32_val) {
                    return -3;
                }
            }
            uint32_t encode = KeyEncoder::to_little_endian_u32(val);
            out.append((char*)&encode, sizeof(uint32_t));
        } break;
        case pb::DATETIME:
        case pb::UINT64: {
            uint64_t val = _reflection->GetUInt64(*_message, field);
            if (!field_info.default_expr_value.is_null()) {
                if (val == field_info.default_expr_value._u.uint64_val) {
                    return -3;
                }
            }
            uint64_t encode = KeyEncoder::to_little_endian_u64(val);
            out.append((char*)&encode, sizeof(uint64_t));
        } break;
        case pb::FLOAT: {
            float val = _reflection->GetFloat(*_message, field);
            if (!field_info.default_expr_value.is_null()) {
                if (val == field_info.default_expr_value._u.float_val) {
                    return -3;
                }
            }
            uint32_t encode = KeyEncoder::to_little_endian_u32(*reinterpret_cast<uint32_t*>(&val));
            out.append((char*)&encode, sizeof(uint32_t));
        } break;
        case pb::DOUBLE: {
           double val = _reflection->GetDouble(*_message, field);
           if (!field_info.default_expr_value.is_null()) {
               if (val == field_info.default_expr_value._u.double_val) {
                   return -3;
               }
           }
           uint64_t encode = KeyEncoder::to_little_endian_u64(*reinterpret_cast<uint64_t*>(&val));
           out.append((char*)&encode, sizeof(uint64_t));
        } break;
        case pb::BOOL: {
            uint8_t  val = _reflection->GetBool(*_message, field);
            if (!field_info.default_expr_value.is_null()) {
                if (val == field_info.default_expr_value._u.bool_val) {
                    return -3;
                }
            }
            out.append((char*)&val, sizeof(uint8_t ));
        } break;
        case pb::STRING: {
            std::string val = _reflection->GetString(*_message, field);;
            if (!field_info.default_expr_value.is_null()) {
                if (val == field_info.default_expr_value.str_val) {
                    return -3;
                }
            }
            out.append(val.data(), val.size());
        } break;
        default: {
            DB_WARNING("un-supported field type: %d, %d", field->number(), field_type);
            return -1;
        } break;
    }
    return 0;
}

// for cstore
int TableRecord::decode_field(const FieldInfo& field_info, const std::string& in) {
    const Descriptor* _descriptor = _message->GetDescriptor();
    const Reflection* _reflection = _message->GetReflection();
    int32_t field_id = field_info.id;
    pb::PrimitiveType field_type = field_info.type;
    const FieldDescriptor* field = _descriptor->FindFieldByNumber(field_id);
    if (field == nullptr) {
        DB_WARNING("invalid field: %d", field_id);
        return -1;
    }
    // set default value
    if (in.size() == 0) {
        set_value(field, field_info.default_expr_value);
        return 0;
    }
    char* c = const_cast<char*>(in.data());
    switch (field_type) {
        case pb::INT8: {
            if (sizeof(int8_t) > in.size()) {
                DB_WARNING("int8_t out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            _reflection->SetInt32(_message, field, *reinterpret_cast<int8_t*>(c));
        } break;
        case pb::INT16: {
            if (sizeof(int16_t) > in.size()) {
                DB_WARNING("int16_t out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            _reflection->SetInt32(_message, field, static_cast<int16_t>(
                    KeyEncoder::to_little_endian_u16(*reinterpret_cast<uint16_t*>(c))));
        } break;
        case pb::TIME:
        case pb::INT32: {
            if (sizeof(int32_t) > in.size()) {
                DB_WARNING("int32_t out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            _reflection->SetInt32(_message, field, static_cast<int32_t>(
                    KeyEncoder::to_little_endian_u32(*reinterpret_cast<uint32_t*>(c))));
        } break;
        case pb::INT64: {
            if (sizeof(int64_t) > in.size()) {
                DB_WARNING("int64_t out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            _reflection->SetInt64(_message, field, static_cast<int64_t>(
                    KeyEncoder::to_little_endian_u64(*reinterpret_cast<uint64_t*>(c))));
        } break;
        case pb::UINT8: {
            if (sizeof(uint8_t) > in.size()) {
                DB_WARNING("uint8_t out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            _reflection->SetUInt32(_message, field, *reinterpret_cast<uint8_t*>(c));
        } break;
        case pb::UINT16: {
            if (sizeof(uint16_t) > in.size()) {
                DB_WARNING("uint16_t out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            _reflection->SetUInt32(_message, field,
                                   KeyEncoder::to_little_endian_u16(*reinterpret_cast<uint16_t*>(c)));
        } break;
        case pb::TIMESTAMP:
        case pb::DATE:
        case pb::UINT32: {
            if (sizeof(uint32_t) > in.size()) {
                DB_WARNING("uint32_t out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            _reflection->SetUInt32(_message, field,
                                   KeyEncoder::to_little_endian_u32(*reinterpret_cast<uint32_t*>(c)));
        } break;
        case pb::DATETIME:
        case pb::UINT64: {
            if (sizeof(uint64_t) > in.size()) {
                DB_WARNING("uint64_t out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            _reflection->SetUInt64(_message, field,
                                   KeyEncoder::to_little_endian_u64(*reinterpret_cast<uint64_t*>(c)));
        } break;
        case pb::FLOAT: {
            if (sizeof(float) > in.size()) {
                DB_WARNING("float out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            uint32_t val = KeyEncoder::to_little_endian_u32(*reinterpret_cast<uint32_t*>(c));
            _reflection->SetFloat(_message, field, *reinterpret_cast<float*>(&val));
        } break;
        case pb::DOUBLE: {
            if (sizeof(double) > in.size()) {
                DB_WARNING("double out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            uint64_t val = KeyEncoder::to_little_endian_u64(*reinterpret_cast<uint64_t*>(c));
            _reflection->SetDouble(_message, field, *reinterpret_cast<double*>(&val));
        } break;
        case pb::BOOL: {
            if (sizeof(uint8_t) > in.size()) {
                DB_WARNING("bool out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            _reflection->SetBool(_message, field, *reinterpret_cast<uint8_t*>(c));
        } break;
        case pb::STRING: {
            _reflection->SetString(_message, field, in);
        } break;
        default: {
            DB_WARNING("un-supported field type: %d, %d", field->number(), field_type);
            return -1;
        } break;
    }
    return 0;
}
}
