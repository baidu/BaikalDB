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

#include "tuple_record.h"
#include "message_helper.h"

namespace baikaldb {
inline const google::protobuf::FieldDescriptor* get_field(
        FieldInfo* field_info, 
        const std::vector<int32_t>* field_slot, 
        SmartRecord* record,
        int32_t tuple_id, std::unique_ptr<MemRow>* mem_row) { 
    if (mem_row == nullptr) { 
        return (*record)->get_field_by_idx(field_info->pb_idx);
    } else {
        int32_t slot = (*field_slot)[field_info->id];
        if (slot == 0) {
            DB_FATAL("field:%d, slot is 0, can't be here", field_info->id);
            return nullptr;
        }
        return (*mem_row)->get_field_by_slot(tuple_id, slot);
    }
}

// 验证pb与fields是否匹配，for online TTL
int TupleRecord::verification_fields(int32_t max_field_id) {
    uint64_t field_key  = 0;
    uint64_t field_num  = 0;
    int32_t  wired_type = 0;
    
    while (_offset < _size) {
        field_key = get_varint<uint64_t>();
        field_num = field_key >> 3;
        wired_type = field_key & 0x07;
        
        if (_offset >= _size) {
            DB_DEBUG("error: %lu, %lu", _offset, _size);
            return -1;
        }

        DB_DEBUG("field_num: %lu, wired_type: %d, max_field_id: %d _offset:%ld", 
            field_num, wired_type, max_field_id, _offset);

        if (field_num > max_field_id || field_num == 0) {
            DB_DEBUG("error: field_num: %lu, wired_type: %d, max_field_id: %d", 
                field_num, wired_type, max_field_id);
            return -1;
        }

        switch (wired_type) {
            case 0:
                skip_varint();
                break;
            case 1:
                skip_fixed<double>();
                break;
            case 2:
                skip_string();
                break;
            case 5:
                skip_fixed<float>();
                break;
            default:
                DB_DEBUG("invalid wired_type: %d, offset: %lu,%lu", wired_type, _offset, _size);
                return -1;
        }
    }

    if (_offset > _size) {
        return -1;
    }

    return 0;
}

int TupleRecord::decode_fields(const std::map<int32_t, FieldInfo*>& fields, 
        const std::vector<int32_t>* field_slot,
        SmartRecord* record, int32_t tuple_id, 
        std::unique_ptr<MemRow>* mem_row) {
    google::protobuf::Message* message = nullptr;
    if (record != nullptr) {
        message = (*record)->get_raw_message();
    } else {
        message = (*mem_row)->get_tuple(tuple_id);
    }
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
            auto field = get_field(iter->second, field_slot, record, tuple_id, mem_row);
            //add default value
            MessageHelper::set_value(field, message, iter->second->default_expr_value);
            if (record == nullptr) {
                (*mem_row)->update_used_size(iter->second->default_expr_value.size());
            }
            iter++;
        }
        if (iter == fields.end()) {
            //DB_WARNING("tag1: %d");
            return 0;
        }
        int str_size = 0;
        if (field_num < static_cast<uint64_t>(iter->first)) {
            // skip current field in proto
            switch (wired_type) {
                case 0:
                    skip_varint();
                    break;
                case 1:
                    skip_fixed<double>();
                    break;
                case 2:
                    skip_string();
                    break;
                case 5:
                    skip_fixed<float>();
                    break;
                default:
                    DB_FATAL("invalid wired_type: %d, offset: %lu,%lu", wired_type, _offset, _size);
                    return -1;
            }
        } else if (field_num == static_cast<uint64_t>(iter->first)) {
            auto field = get_field(iter->second, field_slot, record, tuple_id, mem_row);
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
                MessageHelper::set_int32(field, message, value);
                break;
            }
            case FieldDescriptor::TYPE_SINT64: {
                //zigzag
                int64_t value = 0;
                uint64_t raw_val = get_varint<uint64_t>();
                if (raw_val & 0x1) {
                    value = (raw_val << 63) | ~(raw_val >> 1);
                } else {
                    value = (raw_val >> 1);
                }
                MessageHelper::set_int64(field, message, value);
                break;
            }
            case FieldDescriptor::TYPE_INT32: {
                MessageHelper::set_int32(field, message, get_varint<int32_t>());
                break;
            }
            case FieldDescriptor::TYPE_INT64: {
                MessageHelper::set_int64(field, message, get_varint<int64_t>());
                break;
            }
            case FieldDescriptor::TYPE_SFIXED32: {
                MessageHelper::set_int32(field, message, get_fixed<int32_t>());
                break;
            }
            case FieldDescriptor::TYPE_SFIXED64: {
                MessageHelper::set_int64(field, message, get_fixed<int64_t>());
                break;
            }
            case FieldDescriptor::TYPE_UINT32: {
                MessageHelper::set_uint32(field, message, get_varint<uint32_t>());
                break;
            }
            case FieldDescriptor::TYPE_UINT64: {
                MessageHelper::set_uint64(field, message, get_varint<uint64_t>());
                break;
            }
            case FieldDescriptor::TYPE_FIXED32: {
                MessageHelper::set_uint32(field, message, get_fixed<uint32_t>());
                break;
            }
            case FieldDescriptor::TYPE_FIXED64: {
                MessageHelper::set_uint64(field, message, get_fixed<uint64_t>());
                break;
            }
            case FieldDescriptor::TYPE_FLOAT: {
                MessageHelper::set_float(field, message, get_fixed<float>());
                break;
            }
            case FieldDescriptor::TYPE_DOUBLE: {
                MessageHelper::set_double(field, message, get_fixed<double>());
                break;
            }
            case FieldDescriptor::TYPE_BOOL: {
                MessageHelper::set_boolean(field, message, get_varint<uint32_t>());
                break;
            }
            case FieldDescriptor::TYPE_STRING: 
            case FieldDescriptor::TYPE_BYTES: {
                std::string tmp = get_string();
                str_size = tmp.size();
                MessageHelper::set_string(field, message, tmp);
                break;
            }
            default: {
                DB_FATAL("invalid TYPE: %d, field_id:%d, offset: %lu,%lu",
                        field->type(), iter->first, _offset, _size);
                return -1;
            }
            }
            if (record == nullptr) {
                switch (field->type()) {
                case FieldDescriptor::TYPE_SINT32:
                case FieldDescriptor::TYPE_INT32:
                case FieldDescriptor::TYPE_SFIXED32:
                case FieldDescriptor::TYPE_UINT32:
                case FieldDescriptor::TYPE_FIXED32: {
                    (*mem_row)->update_used_size(4);
                    break;
                }
                case FieldDescriptor::TYPE_SINT64:
                case FieldDescriptor::TYPE_INT64:
                case FieldDescriptor::TYPE_SFIXED64:
                case FieldDescriptor::TYPE_UINT64:
                case FieldDescriptor::TYPE_FIXED64:
                case FieldDescriptor::TYPE_FLOAT: {
                    (*mem_row)->update_used_size(8);
                    break;
                }
                case FieldDescriptor::TYPE_DOUBLE: {
                    (*mem_row)->update_used_size(16);
                    break;
                }
                case FieldDescriptor::TYPE_STRING: 
                case FieldDescriptor::TYPE_BYTES: {
                    (*mem_row)->update_used_size(str_size);
                    break;
                }
                case FieldDescriptor::TYPE_BOOL: {
                    (*mem_row)->update_used_size(1);
                    break;
                }
                default: {
                    break;
                }
                }
            }
            iter++;
        }
    }
    while (iter != fields.end()) {
        auto field = get_field(iter->second, field_slot, record, tuple_id, mem_row);
        //add default value
        MessageHelper::set_value(field, message, iter->second->default_expr_value);
        if (record == nullptr) {
            (*mem_row)->update_used_size(iter->second->default_expr_value.size());
        }
        iter++;
    }
    return 0;
}

} //namespace baikaldb
