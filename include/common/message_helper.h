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
    if (!reflection->HasField(*message, field)) {
        return true;
    }
    return false;
}

static int get_int32(const FieldDescriptor* field, Message* message, int32_t& val) {
    const Reflection* reflection = message->GetReflection();
    if (!reflection->HasField(*message, field)) {
        DB_WARNING("missing field-value idx=%u", field->number());
        return -2;
    }
    val = reflection->GetInt32(*message, field);
    return 0;
}

static void set_int32(const FieldDescriptor* field, Message* message, int32_t val) {
    const Reflection* reflection = message->GetReflection();
    reflection->SetInt32(message, field, val);
}

static int get_uint32(const FieldDescriptor* field, Message* message, uint32_t& val) {
    const Reflection* reflection = message->GetReflection();
    if (!reflection->HasField(*message, field)) {
        DB_WARNING("missing field-value idx=%d", field->number());
        return -2;
    }
    val = reflection->GetUInt32(*message, field);
    return 0;
}

static void set_uint32(const FieldDescriptor* field, Message* message, uint32_t val) {
    const Reflection* reflection = message->GetReflection();
    reflection->SetUInt32(message, field, val);
}

static int get_int64(const FieldDescriptor* field, Message* message, int64_t& val) {
    const Reflection* reflection = message->GetReflection();
    if (!reflection->HasField(*message, field)) {
        DB_WARNING("missing field-value idx=%d", field->number());
        return -2;
    }
    val = reflection->GetInt64(*message, field);
    return 0;
}

static void set_int64(const FieldDescriptor* field, Message* message, int64_t val) {
    const Reflection* reflection = message->GetReflection();
    reflection->SetInt64(message, field, val);
}

static int get_uint64(const FieldDescriptor* field, Message* message, uint64_t& val) {
    const Reflection* reflection = message->GetReflection();
    if (!reflection->HasField(*message, field)) {
        DB_WARNING("missing field-value idx=%d", field->number());
        return -2;
    }
    val = reflection->GetUInt64(*message, field);
    return 0;
}

static void set_uint64(const FieldDescriptor* field, Message* message, uint64_t val) {
    const Reflection* reflection = message->GetReflection();
    reflection->SetUInt64(message, field, val);
}

static int get_float(const FieldDescriptor* field, Message* message, float& val) {
    const Reflection* reflection = message->GetReflection();
    if (!reflection->HasField(*message, field)) {
        DB_WARNING("missing field-value idx=%d", field->number());
        return -2;
    }
    val = reflection->GetFloat(*message, field);
    return 0;
}

static void set_float(const FieldDescriptor* field, Message* message, float val) {
    const Reflection* reflection = message->GetReflection();
    reflection->SetFloat(message, field, val);
}

static int get_double(const FieldDescriptor* field, Message* message, double& val) {
    const Reflection* reflection = message->GetReflection();
    if (!reflection->HasField(*message, field)) {
        DB_WARNING("missing field-value idx=%d", field->number());
        return -2;
    }
    val = reflection->GetDouble(*message, field);
    return 0;
}

static void set_double(const FieldDescriptor* field, Message* message, double val) {
    const Reflection* reflection = message->GetReflection();
    reflection->SetDouble(message, field, val);
}

static int get_string(const FieldDescriptor* field, Message* message, std::string& val) {
    const Reflection* reflection = message->GetReflection();
    if (!reflection->HasField(*message, field)) {
        DB_WARNING("missing field-value idx=%d", field->number());
        return -2;
    }
    val = reflection->GetString(*message, field);
    return 0;
}

static void set_string(const FieldDescriptor* field, Message* message, std::string val) {
    const Reflection* reflection = message->GetReflection();
    reflection->SetString(message, field, val);
}

static int get_boolean(const FieldDescriptor* field, Message* message, bool& val) {
    const Reflection* reflection = message->GetReflection();
    if (!reflection->HasField(*message, field)) {
        DB_WARNING("missing field-value idx=%d", field->number());
        return -2;
    }
    val = reflection->GetBool(*message, field);
    return 0;
}

static void set_boolean(const FieldDescriptor* field, Message* message, bool val) {
    const Reflection* reflection = message->GetReflection();
    reflection->SetBool(message, field, val);
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

static ExprValue get_value(const FieldDescriptor* field, Message* message) {
    if (field == nullptr) {
        return ExprValue::Null();
    }
    const Reflection* reflection = message->GetReflection();
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

// 仅对int型进行add
static void add_value(const FieldDescriptor* field, Message* message, const ExprValue& value) {
    const Reflection* reflection = message->GetReflection();
    if (!reflection->HasField(*message, field)) {
        return;
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
            reflection->SetInt32(message, field, *reinterpret_cast<int8_t*>(c));
        } break;
        case pb::INT16: {
            if (sizeof(int16_t) > in.size()) {
                DB_WARNING("int16_t out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            reflection->SetInt32(message, field, static_cast<int16_t>(
                    KeyEncoder::to_little_endian_u16(*reinterpret_cast<uint16_t*>(c))));
        } break;
        case pb::TIME:
        case pb::INT32: {
            if (sizeof(int32_t) > in.size()) {
                DB_WARNING("int32_t out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            reflection->SetInt32(message, field, static_cast<int32_t>(
                    KeyEncoder::to_little_endian_u32(*reinterpret_cast<uint32_t*>(c))));
        } break;
        case pb::INT64: {
            if (sizeof(int64_t) > in.size()) {
                DB_WARNING("int64_t out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            reflection->SetInt64(message, field, static_cast<int64_t>(
                    KeyEncoder::to_little_endian_u64(*reinterpret_cast<uint64_t*>(c))));
        } break;
        case pb::UINT8: {
            if (sizeof(uint8_t) > in.size()) {
                DB_WARNING("uint8_t out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            reflection->SetUInt32(message, field, *reinterpret_cast<uint8_t*>(c));
        } break;
        case pb::UINT16: {
            if (sizeof(uint16_t) > in.size()) {
                DB_WARNING("uint16_t out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            reflection->SetUInt32(message, field,
                                   KeyEncoder::to_little_endian_u16(*reinterpret_cast<uint16_t*>(c)));
        } break;
        case pb::TIMESTAMP:
        case pb::DATE:
        case pb::UINT32: {
            if (sizeof(uint32_t) > in.size()) {
                DB_WARNING("uint32_t out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            reflection->SetUInt32(message, field,
                                   KeyEncoder::to_little_endian_u32(*reinterpret_cast<uint32_t*>(c)));
        } break;
        case pb::DATETIME:
        case pb::UINT64: {
            if (sizeof(uint64_t) > in.size()) {
                DB_WARNING("uint64_t out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            reflection->SetUInt64(message, field,
                                   KeyEncoder::to_little_endian_u64(*reinterpret_cast<uint64_t*>(c)));
        } break;
        case pb::FLOAT: {
            if (sizeof(float) > in.size()) {
                DB_WARNING("float out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            uint32_t val = KeyEncoder::to_little_endian_u32(*reinterpret_cast<uint32_t*>(c));
            reflection->SetFloat(message, field, *reinterpret_cast<float*>(&val));
        } break;
        case pb::DOUBLE: {
            if (sizeof(double) > in.size()) {
                DB_WARNING("double out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            uint64_t val = KeyEncoder::to_little_endian_u64(*reinterpret_cast<uint64_t*>(c));
            reflection->SetDouble(message, field, *reinterpret_cast<double*>(&val));
        } break;
        case pb::BOOL: {
            if (sizeof(uint8_t) > in.size()) {
                DB_WARNING("bool out of bound: %d %zu", field->number(), in.size());
                return -2;
            }
            reflection->SetBool(message, field, *reinterpret_cast<uint8_t*>(c));
        } break;
        case pb::STRING: {
            reflection->SetString(message, field, std::string(in.data(), in.size()));
        } break;
        default: {
            DB_WARNING("un-supported field type: %d, %d", field->number(), field_type);
            return -1;
        } break;
    }
    return 0;
}

};
}

