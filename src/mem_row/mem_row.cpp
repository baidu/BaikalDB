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
#include "table_key.h"
#include "mut_table_key.h"
#include "schema_factory.h"
#include "mem_row.h"

namespace baikaldb {
using google::protobuf::FieldDescriptor;
using google::protobuf::Descriptor;
using google::protobuf::Message;
using google::protobuf::Reflection;

void MemRow::to_string(int32_t tuple_id, std::string* out) {
        auto tuple = get_tuple(tuple_id);
        if (tuple == nullptr) {
            return ;
        }
        tuple->SerializeToString(out);
}

std::string MemRow::debug_string(int32_t tuple_id) {
    auto tuple = get_tuple(tuple_id);
    if (tuple == nullptr) {
        return "";
    }
    return tuple->ShortDebugString();
}

std::string* MemRow::mutable_string(int32_t tuple_id, int32_t slot_id) {
    auto tuple = get_tuple(tuple_id);
    if (tuple == nullptr) {
        return nullptr;
    }
    const google::protobuf::Reflection* reflection = tuple->GetReflection();
    const google::protobuf::Descriptor* descriptor = tuple->GetDescriptor();
    auto field = descriptor->field(slot_id - 1);
    if (field == nullptr) {
        return nullptr;
    }
    if (!reflection->HasField(*tuple, field)) {
        return nullptr;
    }
    std::string tmp;
    return (std::string*)&reflection->GetStringReference(*tuple, field, &tmp);
}

int MemRow::decode_key(int32_t tuple_id, IndexInfo& index, 
        std::vector<int32_t>& field_slot, const TableKey& key, int& pos) {
    if (index.type == pb::I_NONE) {
        DB_WARNING("unknown table index type: %ld", index.id);
        return -1;
    }
    auto tuple = get_tuple(tuple_id);
    if (tuple == nullptr) {
        DB_FATAL("unknown tuple: %d", tuple_id);
        return -1;
    }
    uint8_t null_flag = 0;
    const Descriptor* descriptor = tuple->GetDescriptor();
    const Reflection* reflection = tuple->GetReflection();
    if (index.type == pb::I_KEY || index.type == pb::I_UNIQ) {
        null_flag = key.extract_u8(pos);
        pos += sizeof(uint8_t);
    }
    for (uint32_t idx = 0; idx < index.fields.size(); ++idx) {
        // DB_WARNING("null_flag: %ld, %u, %d, %d, %s", 
        //     index.id, null_flag, pos, index.fields[idx].can_null, 
        //     key.data().ToString(true).c_str());
        if (((null_flag >> (7 - idx)) & 0x01) && index.fields[idx].can_null) {
            //DB_DEBUG("field is null: %d", idx);
            continue;
        }
        int32_t slot = field_slot[index.fields[idx].id];
        //说明不需要解析
        //pos需要更新，容易出bug
        if (slot == 0) {
            if (0 != key.skip_field(index.fields[idx], pos)) {
                DB_WARNING("skip index field error");
                return -1;
            }
            continue;
        }
        const FieldDescriptor* field = descriptor->field(slot - 1);
        if (field == nullptr) {
            DB_WARNING("invalid field: %d slot: %d", index.fields[idx].id, slot);
            return -1;
        }
        if (0 != key.decode_field(tuple, reflection, field, index.fields[idx], pos)) {
            DB_WARNING("decode index field error");
            return -1;
        }
    }
    return 0;
}

int MemRow::decode_primary_key(int32_t tuple_id, IndexInfo& index, std::vector<int32_t>& field_slot, 
        const TableKey& key, int& pos) {
    if (index.type != pb::I_KEY && index.type != pb::I_UNIQ) {
        DB_WARNING("invalid secondary index type: %ld", index.id);
        return -1;
    }
    auto tuple = get_tuple(tuple_id);
    if (tuple == nullptr) {
        DB_FATAL("unknown tuple: %d", tuple_id);
        return -1;
    }
    const Descriptor* descriptor = tuple->GetDescriptor();
    const Reflection* reflection = tuple->GetReflection();
    for (auto& field_info : index.pk_fields) {
        int32_t slot = field_slot[field_info.id];
        //说明不需要解析
        //pos需要更新，容易出bug
        if (slot == 0) {
            if (0 != key.skip_field(field_info, pos)) {
                DB_WARNING("skip index field error");
                return -1;
            }
            continue;
        }
        const FieldDescriptor* field = descriptor->field(slot - 1);
        if (field == nullptr) {
            DB_WARNING("invalid field: %d slot: %d", field_info.id, slot);
            return -1;
        }
        if (0 != key.decode_field(tuple, reflection, field, field_info, pos)) {
            DB_WARNING("decode index field error: field_id: %d, type: %d", 
                field_info.id, field_info.type);
            return -1;
        }
    }
    return 0;
}
}
