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

#include "mem_row_descriptor.h"
#include "mem_row.h"

namespace baikaldb {

void MemRow::set_tuple(int32_t tuple_id, MemRowDescriptor* desc) {
    if (_tuples[tuple_id] == nullptr) {
        _tuples[tuple_id] = desc->new_tuple_message(tuple_id);
    }
}

void MemRow::to_string(int32_t tuple_id, std::string* out) {
    if (_tuples[tuple_id] != nullptr) {
        _tuples[tuple_id]->SerializeToString(out);
    }
}

std::string MemRow::debug_string(int32_t tuple_id) {
    if (_tuples[tuple_id] != nullptr) {
        return _tuples[tuple_id]->ShortDebugString();
    }
    return "";
}

std::string* MemRow::mutable_string(int32_t tuple_id, int32_t slot_id) {
    auto tuple = _tuples[tuple_id];
    if (tuple == nullptr) {
        return nullptr;
    }
    const google::protobuf::Reflection* _reflection = tuple->GetReflection();
    const google::protobuf::Descriptor* _descriptor = tuple->GetDescriptor();
    auto field = _descriptor->FindFieldByNumber(slot_id);
    if (field == nullptr) {
        return nullptr;
    }
    if (!_reflection->HasField(*tuple, field)) {
        return nullptr;
    }
    std::string tmp;
    return (std::string*)&_reflection->GetStringReference(*tuple, field, &tmp);
}

// slot start with 1
ExprValue MemRow::get_value(int32_t tuple_id, int32_t slot_id) {
    auto tuple = _tuples[tuple_id];
    if (tuple == nullptr) {
        return ExprValue::Null();
    }
    const google::protobuf::Reflection* _reflection = tuple->GetReflection();
    const google::protobuf::Descriptor* _descriptor = tuple->GetDescriptor();
    auto field = _descriptor->FindFieldByNumber(slot_id);
    if (field == nullptr) {
        return ExprValue::Null();
    }
    if (!_reflection->HasField(*tuple, field)) {
        return ExprValue::Null();
    }
    auto type = field->cpp_type();
    switch (type) {
        case FieldDescriptor::CPPTYPE_INT32: {
            ExprValue value(pb::INT32);
            value._u.int32_val = _reflection->GetInt32(*tuple, field);
            return value;
        } break;
        case FieldDescriptor::CPPTYPE_UINT32: {
            ExprValue value(pb::UINT32);
            value._u.uint32_val = _reflection->GetUInt32(*tuple, field);
            return value;
        } break;
        case FieldDescriptor::CPPTYPE_INT64: {
            ExprValue value(pb::INT64);
            value._u.int64_val = _reflection->GetInt64(*tuple, field);
            return value;
        } break;
        case FieldDescriptor::CPPTYPE_UINT64: {
            ExprValue value(pb::UINT64);
            value._u.uint64_val = _reflection->GetUInt64(*tuple, field);
            return value;
        } break;
        case FieldDescriptor::CPPTYPE_FLOAT: {
            ExprValue value(pb::FLOAT);
            value._u.float_val = _reflection->GetFloat(*tuple, field);
            return value;
        } break;
        case FieldDescriptor::CPPTYPE_DOUBLE: {
            ExprValue value(pb::DOUBLE);
            value._u.double_val = _reflection->GetDouble(*tuple, field);
            return value;
        } break;
        case FieldDescriptor::CPPTYPE_BOOL: {
            ExprValue value(pb::BOOL);
            value._u.bool_val = _reflection->GetBool(*tuple, field);
            return value;
        } break;
        case FieldDescriptor::CPPTYPE_STRING: {
            ExprValue value(pb::STRING);
            value.str_val = _reflection->GetString(*tuple, field);
            return value;
        } default: {
            return ExprValue::Null();
        }
    }
    return ExprValue::Null();
}
int MemRow::set_value(int32_t tuple_id, int32_t slot_id, const ExprValue& value) {
    auto tuple = _tuples[tuple_id];
    if (tuple == nullptr) {
        return -1;
    }
    const google::protobuf::Reflection* _reflection = tuple->GetReflection();
    const google::protobuf::Descriptor* _descriptor = tuple->GetDescriptor();
    auto field = _descriptor->FindFieldByNumber(slot_id);
    if (value.is_null()) {
        _reflection->ClearField(tuple, field);
        return 0;
    }

    auto type = field->cpp_type();
    switch (type) {
        case FieldDescriptor::CPPTYPE_INT32: {
            _reflection->SetInt32(tuple, field, value.get_numberic<int32_t>());
        } break;
        case FieldDescriptor::CPPTYPE_UINT32: {
            _reflection->SetUInt32(tuple, field, value.get_numberic<uint32_t>());
        } break;
        case FieldDescriptor::CPPTYPE_INT64: {
            _reflection->SetInt64(tuple, field, value.get_numberic<int64_t>());
        } break;
        case FieldDescriptor::CPPTYPE_UINT64: {
            _reflection->SetUInt64(tuple, field, value.get_numberic<uint64_t>());
        } break;
        case FieldDescriptor::CPPTYPE_FLOAT: {
            _reflection->SetFloat(tuple, field, value.get_numberic<float>());
        } break;
        case FieldDescriptor::CPPTYPE_DOUBLE: {
            _reflection->SetDouble(tuple, field, value.get_numberic<double>());
        } break;
        case FieldDescriptor::CPPTYPE_BOOL: {
            _reflection->SetBool(tuple, field, value.get_numberic<bool>());
        } break;
        case FieldDescriptor::CPPTYPE_STRING: {
            _reflection->SetString(tuple, field, value.get_string());
        } break;
        default: {
            return -1;
        }
    }
    return 0;
}
}
