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

#include "mem_row.h"
#include "mem_row_descriptor.h"

namespace baikaldb {

int32_t MemRowDescriptor::init(std::vector<pb::TupleDescriptor>& tuple_desc) {
    if (tuple_desc.size() == 0) {
        return 0;
    }
    if (nullptr == (_factory 
            = new (std::nothrow)google::protobuf::DynamicMessageFactory(&_pool))) {
        return -1;
    }
    if (nullptr == (_proto = new (std::nothrow)google::protobuf::FileDescriptorProto)) {
        DB_WARNING("create FileDescriptorProto failed");
        return -1;
    }
    _proto->set_name("mem_row.proto");
    std::vector<int32_t> tuples;
    for (auto& tuple : tuple_desc) {
        if (!tuple.has_tuple_id()) {
            continue;
        }
        int32_t tuple_id = tuple.tuple_id();
        google::protobuf::DescriptorProto* tuple_proto = _proto->add_message_type();
        tuple_proto->set_name("tuple_" + std::to_string(tuple_id));
        tuples.push_back(tuple_id);

        //DB_WARNING("desc:%s", tuple.ShortDebugString().c_str());
        int slot_cnt = tuple.slots_size();
        for (int idx = 0; idx < slot_cnt; ++idx) {
            const pb::SlotDescriptor& slot = tuple.slots(idx);
            int32_t slot_id = slot.slot_id();
            google::protobuf::FieldDescriptorProto *field = tuple_proto->add_field();
            field->set_name("slot_" + std::to_string(slot_id));
            auto pb_type = primitive_to_proto_type(slot.slot_type());
            if (pb_type == -1) {
                DB_WARNING("un-supported mysql type: %d", slot.slot_type());
                return -1;
            }
            field->set_type((FieldDescriptorProto::Type)pb_type);
            field->set_number(slot_id);
            field->set_label(FieldDescriptorProto::LABEL_OPTIONAL);
        }
    }
    const google::protobuf::FileDescriptor *memrow_desc = _pool.BuildFile(*_proto);
    if (!memrow_desc) {
        DB_WARNING("build memrow_desc failed.");
        return -1;
    }
    for (auto tuple : tuples) {
        const google::protobuf::Descriptor *descriptor 
            = memrow_desc->FindMessageTypeByName("tuple_" + std::to_string(tuple));
        if (!descriptor) {
            DB_WARNING("FindMessageTypeByName [%d] failed.", tuple);
            return -1;
        }
        const google::protobuf::Message *message = _factory->GetPrototype(descriptor);
        if (!message) {
            DB_WARNING("create dynamic message failed.");
            return -1;
        }
        _id_tuple_mapping.insert(std::make_pair(tuple, message));
    }
    return 0;
}

google::protobuf::Message* MemRowDescriptor::new_tuple_message(int32_t tuple_id) {
    auto iter = _id_tuple_mapping.find(tuple_id);
    if (iter == _id_tuple_mapping.end()) {
        DB_WARNING("no tuple found: %d", tuple_id);
        return nullptr;
    }
    if (iter->second == nullptr) {
        DB_WARNING("message is NULL: %d", tuple_id);
        return nullptr;
    }
    return iter->second->New();
}

std::unique_ptr<MemRow> MemRowDescriptor::fetch_mem_row() {
    int32_t size = _id_tuple_mapping.size();
    int32_t largest = 1;
    if (size > 0) {
        largest = _id_tuple_mapping.rbegin()->first;
    }
    std::unique_ptr<MemRow> tmp(new MemRow(largest + 1));
    for (auto& pair : _id_tuple_mapping) {
        tmp->_tuples[pair.first] = pair.second->New();
    }
    return tmp;
}
}
