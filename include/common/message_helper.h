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
#include "key_encoder.h"
#include "rocksdb/slice.h"
#include "expr_value.h"

namespace baikaldb {
class MessageHelper {
using FieldDescriptor = google::protobuf::FieldDescriptor;
using Message = google::protobuf::Message;
using Reflection = google::protobuf::Reflection;
public:
static bool is_null(const FieldDescriptor* field, Message* message) {
    const Reflection* reflection = message->GetReflection();
    if (!field->is_repeated() && !reflection->HasField(*message, field)) {
        return true;
    }
    return false;
}

static int get_int32(const FieldDescriptor* field, Message* message, int32_t& val) {
    const Reflection* reflection = message->GetReflection();
    if (field->is_repeated()) {
        DB_FATAL("repeated field not support idx=%d", field->number());
        return -1;
    }
    if (!reflection->HasField(*message, field)) {
        DB_WARNING("missing field-value idx=%u", field->number());
        return -2;
    }
    val = reflection->GetInt32(*message, field);
    return 0;
}

static void set_int32(const FieldDescriptor* field, Message* message, int32_t val) {
    const Reflection* reflection = message->GetReflection();
    if (!field->is_repeated()) {
        reflection->SetInt32(message, field, val);
    } else {
        reflection->AddInt32(message, field, val);
    }
}

static int get_uint32(const FieldDescriptor* field, Message* message, uint32_t& val) {
    const Reflection* reflection = message->GetReflection();
    if (field->is_repeated()) {
        DB_FATAL("repeated field not support idx=%d", field->number());
        return -1;
    }
    if (!reflection->HasField(*message, field)) {
        DB_WARNING("missing field-value idx=%d", field->number());
        return -2;
    }
    val = reflection->GetUInt32(*message, field);
    return 0;
}

static void set_uint32(const FieldDescriptor* field, Message* message, uint32_t val) {
    const Reflection* reflection = message->GetReflection();
    if (!field->is_repeated()) {
        reflection->SetUInt32(message, field, val);
    } else {
        reflection->AddUInt32(message, field, val);
    }
}

static int get_int64(const FieldDescriptor* field, Message* message, int64_t& val) {
    const Reflection* reflection = message->GetReflection();
    if (field->is_repeated()) {
        DB_FATAL("repeated field not support idx=%d", field->number());
        return -1;
    }
    if (!reflection->HasField(*message, field)) {
        DB_WARNING("missing field-value idx=%d", field->number());
        return -2;
    }
    val = reflection->GetInt64(*message, field);
    return 0;
}

static void set_int64(const FieldDescriptor* field, Message* message, int64_t val) {
    const Reflection* reflection = message->GetReflection();
    if (!field->is_repeated()) {
        reflection->SetInt64(message, field, val);
    } else {
        reflection->AddInt64(message, field, val);
    }
}

static int get_uint64(const FieldDescriptor* field, Message* message, uint64_t& val) {
    const Reflection* reflection = message->GetReflection();
    if (field->is_repeated()) {
        DB_FATAL("repeated field not support idx=%d", field->number());
        return -1;
    }
    if (!reflection->HasField(*message, field)) {
        DB_WARNING("missing field-value idx=%d", field->number());
        return -2;
    }
    val = reflection->GetUInt64(*message, field);
    return 0;
}

static void set_uint64(const FieldDescriptor* field, Message* message, uint64_t val) {
    const Reflection* reflection = message->GetReflection();
    if (!field->is_repeated()) {
        reflection->SetUInt64(message, field, val);
    } else {
        reflection->AddUInt64(message, field, val);
    }
}

static int get_float(const FieldDescriptor* field, Message* message, float& val) {
    const Reflection* reflection = message->GetReflection();
    if (field->is_repeated()) {
        DB_FATAL("repeated field not support idx=%d", field->number());
        return -1;
    }
    if (!reflection->HasField(*message, field)) {
        DB_WARNING("missing field-value idx=%d", field->number());
        return -2;
    }
    val = reflection->GetFloat(*message, field);
    return 0;
}

static void set_float(const FieldDescriptor* field, Message* message, float val) {
    const Reflection* reflection = message->GetReflection();
    if (!field->is_repeated()) {
        reflection->SetFloat(message, field, val);
    } else {
        reflection->AddFloat(message, field, val);
    }
}

static int get_double(const FieldDescriptor* field, Message* message, double& val) {
    const Reflection* reflection = message->GetReflection();
    if (field->is_repeated()) {
        DB_FATAL("repeated field not support idx=%d", field->number());
        return -1;
    }
    if (!reflection->HasField(*message, field)) {
        DB_WARNING("missing field-value idx=%d", field->number());
        return -2;
    }
    val = reflection->GetDouble(*message, field);
    return 0;
}

static void set_double(const FieldDescriptor* field, Message* message, double val) {
    const Reflection* reflection = message->GetReflection();
    if (!field->is_repeated()) {
        reflection->SetDouble(message, field, val);
    } else {
        reflection->AddDouble(message, field, val);
    }
}

static int get_string(const FieldDescriptor* field, Message* message, std::string& val) {
    const Reflection* reflection = message->GetReflection();
    if (field->is_repeated()) {
        DB_FATAL("repeated field not support idx=%d", field->number());
        return -1;
    }
    if (!reflection->HasField(*message, field)) {
        DB_WARNING("missing field-value idx=%d", field->number());
        return -2;
    }
    val = reflection->GetString(*message, field);
    return 0;
}

static void set_string(const FieldDescriptor* field, Message* message, std::string val) {
    const Reflection* reflection = message->GetReflection();
    if (!field->is_repeated()) {
        reflection->SetString(message, field, val);
    } else {
        reflection->AddString(message, field, val);
    }
}

static int get_boolean(const FieldDescriptor* field, Message* message, bool& val) {
    const Reflection* reflection = message->GetReflection();
    if (field->is_repeated()) {
        DB_FATAL("repeated field not support idx=%d", field->number());
        return -1;
    }
    if (!reflection->HasField(*message, field)) {
        DB_WARNING("missing field-value idx=%d", field->number());
        return -2;
    }
    val = reflection->GetBool(*message, field);
    return 0;
}

static void set_boolean(const FieldDescriptor* field, Message* message, bool val) {
    const Reflection* reflection = message->GetReflection();
    if (!field->is_repeated()) {
        reflection->SetBool(message, field, val);
    } else {
        reflection->AddBool(message, field, val);
    }
}

static int set_value(const FieldDescriptor* field, Message* message, const ExprValue& value) {
    if (field == nullptr) {
        return -1;
    }
    const Reflection* reflection = message->GetReflection();
    if (value.is_null()) {
        reflection->ClearField(message, field);
        return 0;
    }
    if (field->is_repeated()) {
        return add_array_value(field, message, value);
    }
    auto type = field->cpp_type();
    switch (type) {
        case FieldDescriptor::CPPTYPE_INT32: {
            reflection->SetInt32(message, field, value.get_numberic<int32_t>());
        } break;
        case FieldDescriptor::CPPTYPE_UINT32: {
            reflection->SetUInt32(message, field, value.get_numberic<uint32_t>());
        } break;
        case FieldDescriptor::CPPTYPE_INT64: {
            reflection->SetInt64(message, field, value.get_numberic<int64_t>());
        } break;
        case FieldDescriptor::CPPTYPE_UINT64: {
            reflection->SetUInt64(message, field, value.get_numberic<uint64_t>());
        } break;
        case FieldDescriptor::CPPTYPE_FLOAT: {
            reflection->SetFloat(message, field, value.get_numberic<float>());
        } break;
        case FieldDescriptor::CPPTYPE_DOUBLE: {
            reflection->SetDouble(message, field, value.get_numberic<double>());
        } break;
        case FieldDescriptor::CPPTYPE_BOOL: {
            reflection->SetBool(message, field, value.get_numberic<bool>());
        } break;
        case FieldDescriptor::CPPTYPE_STRING: {
            reflection->SetString(message, field, value.get_string());
        } break;
        default: {
            return -1;
        }
    }
    return 0;
}

static ExprValue get_repeated_value(const Reflection* reflection, const FieldDescriptor* field, Message* message) {
    auto type = field->cpp_type();
    int count = reflection->FieldSize(*message, field);
    switch (type) {
        case FieldDescriptor::CPPTYPE_INT64: {
            ExprValue value(pb::ARRAY_INT64);
            ArrayValue<int64_t>* values = static_cast<ArrayValue<int64_t>*>(value._u.array_ptr);
            for (int i = 0; i < count; ++i) {
                values->add_value(reflection->GetRepeatedInt64(*message, field, i));
            }
            return value;
        } break;
        case FieldDescriptor::CPPTYPE_UINT64: {
            ExprValue value(pb::ARRAY_UINT64);
            ArrayValue<uint64_t>* values = static_cast<ArrayValue<uint64_t>*>(value._u.array_ptr);
            for (int i = 0; i < count; ++i) {
                values->add_value(reflection->GetRepeatedUInt64(*message, field, i));
            }
            return value;
        } break;
        case FieldDescriptor::CPPTYPE_FLOAT: {
            ExprValue value(pb::ARRAY_FLOAT);
            ArrayValue<float>* values = static_cast<ArrayValue<float>*>(value._u.array_ptr);
            for (int i = 0; i < count; ++i) {
                values->add_value(reflection->GetRepeatedFloat(*message, field, i));
            }
            return value;
        } break;
        case FieldDescriptor::CPPTYPE_DOUBLE: {
            ExprValue value(pb::ARRAY_DOUBLE);
            ArrayValue<double>* values = static_cast<ArrayValue<double>*>(value._u.array_ptr);
            for (int i = 0; i < count; ++i) {
                values->add_value(reflection->GetRepeatedDouble(*message, field, i));
            }
            return value;
        } break;
        case FieldDescriptor::CPPTYPE_BOOL: {
            ExprValue value(pb::ARRAY_BOOL);
            ArrayValue<bool>* values = static_cast<ArrayValue<bool>*>(value._u.array_ptr);
            for (int i = 0; i < count; ++i) {
                values->add_value(reflection->GetRepeatedBool(*message, field, i));
            }
            return value;
        } break;
        case FieldDescriptor::CPPTYPE_STRING: {
            ExprValue value(pb::ARRAY_STRING);
            ArrayValue<std::string>* values = static_cast<ArrayValue<std::string>*>(value._u.array_ptr);
            for (int i = 0; i < count; ++i) {
                values->add_value(reflection->GetRepeatedString(*message, field, i));
            }
            return value;
        } 
        default: {
            return ExprValue::Null();
        }
    }
    return ExprValue::Null();
}

static ExprValue get_value(const FieldDescriptor* field, Message* message) {
    if (field == nullptr) {
        return ExprValue::Null();
    }
    const Reflection* reflection = message->GetReflection();
    if (field->is_repeated()) {
        return get_repeated_value(reflection, field, message);
    }
    // HasField不支持repeated, 会core
    if (!reflection->HasField(*message, field)) {
        return ExprValue::Null();
    }
    auto type = field->cpp_type();
    switch (type) {
        case FieldDescriptor::CPPTYPE_INT32: {
            ExprValue value(pb::INT32);
            value._u.int32_val = reflection->GetInt32(*message, field);
            return value;
        } break;
        case FieldDescriptor::CPPTYPE_UINT32: {
            ExprValue value(pb::UINT32);
            value._u.uint32_val = reflection->GetUInt32(*message, field);
            return value;
        } break;
        case FieldDescriptor::CPPTYPE_INT64: {
            ExprValue value(pb::INT64);
            value._u.int64_val = reflection->GetInt64(*message, field);
            return value;
        } break;
        case FieldDescriptor::CPPTYPE_UINT64: {
            ExprValue value(pb::UINT64);
            value._u.uint64_val = reflection->GetUInt64(*message, field);
            return value;
        } break;
        case FieldDescriptor::CPPTYPE_FLOAT: {
            ExprValue value(pb::FLOAT);
            value._u.float_val = reflection->GetFloat(*message, field);
            return value;
        } break;
        case FieldDescriptor::CPPTYPE_DOUBLE: {
            ExprValue value(pb::DOUBLE);
            value._u.double_val = reflection->GetDouble(*message, field);
            return value;
        } break;
        case FieldDescriptor::CPPTYPE_BOOL: {
            ExprValue value(pb::BOOL);
            value._u.bool_val = reflection->GetBool(*message, field);
            return value;
        } break;
        case FieldDescriptor::CPPTYPE_STRING: {
            ExprValue value(pb::STRING);
            value.str_val = reflection->GetString(*message, field);
            return value;
        } 
        default: {
            return ExprValue::Null();
        }
    }
    return ExprValue::Null();
}

// 仅对int型进行add, 只用于rollup
static void add_value(const FieldDescriptor* field, Message* message, const ExprValue& value) {
    const Reflection* reflection = message->GetReflection();
    if (field->is_repeated()) {
        DB_FATAL("repeated field not support idx=%d", field->number());
        return;
    }
    if (!reflection->HasField(*message, field)) {
        ExprValue default_val(pb::INT64);
        set_value(field, message, default_val);
    }
    auto type = field->cpp_type();
    switch (type) {
        case FieldDescriptor::CPPTYPE_INT32: {
            int32_t val = reflection->GetInt32(*message, field);
            val += value._u.int32_val;
            reflection->SetInt32(message, field, val);
        } break;
        case FieldDescriptor::CPPTYPE_UINT32: {
            uint32_t val = reflection->GetUInt32(*message, field);
            val += value._u.uint32_val;
            reflection->SetUInt32(message, field, val);
        } break;
        case FieldDescriptor::CPPTYPE_INT64: {
            int64_t val = reflection->GetInt64(*message, field);
            val += value._u.int64_val;
            reflection->SetInt64(message, field, val);
        } break;
        case FieldDescriptor::CPPTYPE_UINT64: {
            uint64_t val = reflection->GetUInt64(*message, field);
            val += value._u.uint64_val;
            reflection->SetUInt64(message, field, val);
        } break;
        default: {
            return;
        }
    }
    return;
}

template <typename T>
static int add_raw_values(const FieldDescriptor* field, Message* message, ArrayValue<T>* array_data) {
    const Reflection* reflection = message->GetReflection();
    if (array_data == nullptr) {
        DB_WARNING("array_data is nullptr");
        return -1;
    }
    reflection->ClearField(message, field);
    auto type = field->cpp_type();
    switch (type) {
        case FieldDescriptor::CPPTYPE_BOOL: {
            for (const auto& val : array_data->data) {
                reflection->AddBool(message, field, val);
            }
        } break;
        case FieldDescriptor::CPPTYPE_INT32: {
            for (const auto& val : array_data->data) {
                reflection->AddInt32(message, field, val);
            }
        } break;
        case FieldDescriptor::CPPTYPE_INT64: {
            for (const auto& val : array_data->data) {
                reflection->AddInt64(message, field, val);
            }
        } break;
        case FieldDescriptor::CPPTYPE_UINT64: {
            for (const auto& val : array_data->data) {
                reflection->AddUInt64(message, field, val);
            }
        } break;
        case FieldDescriptor::CPPTYPE_FLOAT: {
            for (const auto& val : array_data->data) {
                reflection->AddFloat(message, field, val);
            }
        } break;
        case FieldDescriptor::CPPTYPE_DOUBLE: {
            for (const auto& val : array_data->data) {
                reflection->AddDouble(message, field, val);
            }
        } break;
        default: 
            break;
    }
    return 0;
}

// 处理 std::string 类型的重载版本（优先于模板匹配）
static int add_raw_values(const FieldDescriptor* field, Message* message, ArrayValue<std::string>* array_data) {
    const Reflection* reflection = message->GetReflection();
    if (array_data == nullptr) {
        DB_WARNING("array_data is nullptr");
        return -1;
    }
    reflection->ClearField(message, field);
    for (const auto& val : array_data->data) {
        reflection->AddString(message, field, val);
    }
    return 0;
}

static int decode_field(const FieldDescriptor* field, pb::PrimitiveType field_type, 
        Message* message, const rocksdb::Slice& in) {
    const Reflection* reflection = message->GetReflection();
    char* c = const_cast<char*>(in.data());
    switch (field_type) {
        case pb::INT8: {
            if (sizeof(int8_t) > in.size()) {
                DB_WARNING("int8_t out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            set_int32(field, message, *reinterpret_cast<int8_t*>(c));
        } break;
        case pb::INT16: {
            if (sizeof(int16_t) > in.size()) {
                DB_WARNING("int16_t out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            set_int32(field, message, static_cast<int16_t>(
                    KeyEncoder::to_little_endian_u16(*reinterpret_cast<uint16_t*>(c))));
        } break;
        case pb::TIME:
        case pb::INT32: {
            if (sizeof(int32_t) > in.size()) {
                DB_WARNING("int32_t out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            set_int32(field, message, static_cast<int32_t>(
                    KeyEncoder::to_little_endian_u32(*reinterpret_cast<uint32_t*>(c))));
        } break;
        case pb::INT64: {
            if (sizeof(int64_t) > in.size()) {
                DB_WARNING("int64_t out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            set_int64(field, message, static_cast<int64_t>(
                    KeyEncoder::to_little_endian_u64(*reinterpret_cast<uint64_t*>(c))));
        } break;
        case pb::UINT8: {
            if (sizeof(uint8_t) > in.size()) {
                DB_WARNING("uint8_t out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            set_uint32(field, message, *reinterpret_cast<uint8_t*>(c));
        } break;
        case pb::UINT16: {
            if (sizeof(uint16_t) > in.size()) {
                DB_WARNING("uint16_t out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            set_uint32(field, message,
                    KeyEncoder::to_little_endian_u16(*reinterpret_cast<uint16_t*>(c)));
        } break;
        case pb::TIMESTAMP:
        case pb::DATE:
        case pb::UINT32: {
            if (sizeof(uint32_t) > in.size()) {
                DB_WARNING("uint32_t out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            set_uint32(field, message,
                    KeyEncoder::to_little_endian_u32(*reinterpret_cast<uint32_t*>(c)));
        } break;
        case pb::DATETIME:
        case pb::UINT64: {
            if (sizeof(uint64_t) > in.size()) {
                DB_WARNING("uint64_t out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            set_uint64(field, message,
                    KeyEncoder::to_little_endian_u64(*reinterpret_cast<uint64_t*>(c)));
        } break;
        case pb::FLOAT: {
            if (sizeof(float) > in.size()) {
                DB_WARNING("float out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            uint32_t val = KeyEncoder::to_little_endian_u32(*reinterpret_cast<uint32_t*>(c));
            set_float(field, message, *reinterpret_cast<float*>(&val));
        } break;
        case pb::DOUBLE: {
            if (sizeof(double) > in.size()) {
                DB_WARNING("double out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            uint64_t val = KeyEncoder::to_little_endian_u64(*reinterpret_cast<uint64_t*>(c));
            set_double(field, message, *reinterpret_cast<double*>(&val));
        } break;
        case pb::BOOL: {
            if (sizeof(uint8_t) > in.size()) {
                DB_WARNING("bool out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            set_boolean(field, message, *reinterpret_cast<uint8_t*>(c) != 0);
        } break;
        case pb::STRING: {
            set_string(field, message, std::string(in.data(), in.size()));
        } break;
        default: {
            DB_WARNING("un-supported field type: %d, %d", field->number(), field_type);
            return -1;
        } break;
    }
    return 0;
}

static int add_array_value(const FieldDescriptor* field, Message* message, const ExprValue& value) {
    if (!is_array(value.type)) {
        DB_FATAL("Not an array type: %s", pb::PrimitiveType_Name(value.type).c_str());
        return -1;
    }
    switch (value.type) {
        case pb::ARRAY_BOOL: {
            ArrayValue<bool>* array_data = value.get_array<bool>();
            return add_raw_values<bool>(field, message, array_data);
        }
        case pb::ARRAY_INT64: {
            ArrayValue<int64_t>* array_data = value.get_array<int64_t>();
            return add_raw_values<int64_t>(field, message, array_data);
        }
        case pb::ARRAY_UINT64: {
            ArrayValue<uint64_t>* array_data = value.get_array<uint64_t>();
            return add_raw_values<uint64_t>(field, message, array_data);
        }
        case pb::ARRAY_FLOAT: {
            ArrayValue<float>* array_data = value.get_array<float>();
            return add_raw_values<float>(field, message, array_data);
        }
        case pb::ARRAY_DOUBLE: {
            ArrayValue<double>* array_data = value.get_array<double>();
            return add_raw_values<double>(field, message, array_data);
        }
        case pb::ARRAY_STRING: {
            ArrayValue<std::string>* array_data = value.get_array<std::string>();
            return add_raw_values(field, message, array_data);  // 不使用显式模板参数，优先匹配非模板重载
        }
        default: {
            DB_WARNING("Not supported type: %s", pb::PrimitiveType_Name(value.type).c_str());
            return -1;
        }
    }
    return 0;
}
};
}
