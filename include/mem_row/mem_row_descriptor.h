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

#include <unordered_map>
#include <memory>
#include "common.h"
#include "proto/common.pb.h"
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/descriptor.pb.h>

using google::protobuf::FieldDescriptorProto;

namespace baikaldb {
class MemRow;
//internal memory row meta-data for a query
class MemRowDescriptor {
public:
    MemRowDescriptor() : _factory(nullptr), _proto(nullptr) {}

    virtual ~MemRowDescriptor() {
        delete _proto;
        _proto = nullptr;
        delete _factory;
        _factory = nullptr;
    }

    int32_t init(std::vector<pb::TupleDescriptor>& tuple_desc);

    google::protobuf::Message* new_tuple_message(int32_t tuple_id);

    std::unique_ptr<MemRow> fetch_mem_row();

    int tuple_size() {
        return _id_tuple_mapping.size();
    }

    const std::map<int32_t, const google::protobuf::Message*>& id_tuple_mapping() const {
        return _id_tuple_mapping;
    }

private:
    google::protobuf::DescriptorPool          _pool;
    google::protobuf::DynamicMessageFactory*  _factory;
    google::protobuf::FileDescriptorProto*    _proto;
    
    // kv: tuple_id => DescriptorProto (message, tuple)
    std::map<int32_t, const google::protobuf::Message*> _id_tuple_mapping;

};
}

