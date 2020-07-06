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
#include <unordered_set>
#include "expr_value.h"
#include "message_helper.h"
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/descriptor.h>


namespace baikaldb {
class TableKey;
class IndexInfo;
class MemRowDescriptor;
//internal memory row meta-data for a query
class MemRow final {
friend MemRowDescriptor;
public:
    explicit MemRow(int size) : _tuples(size) {
    }

    ~MemRow() {
        for (auto& t : _tuples) {
            delete t;
            t = nullptr;
        }
    }
    google::protobuf::Message* get_tuple(int32_t tuple_id) {
        return _tuples[tuple_id];
    }

    const google::protobuf::FieldDescriptor* get_field_by_slot(int32_t tuple_id, int32_t slot_id) {
        auto tuple = _tuples[tuple_id];
        if (tuple == nullptr) {
            return nullptr;
        }
        const google::protobuf::Descriptor* descriptor = tuple->GetDescriptor();
        return descriptor->field(slot_id - 1);
    }

    void set_tuple(int32_t tuple_id, MemRowDescriptor* desc);
    void from_string(int32_t tuple_id, const std::string& in) {
        if (_tuples[tuple_id] != nullptr && in.size() > 0) {
            _tuples[tuple_id]->ParseFromString(in);
        }
    }

    void to_string(int32_t tuple_id, std::string* out);
    std::string debug_string(int32_t tuple_id);

    void clear() {
        for (auto& t : _tuples) {
            t->Clear();
        }
    }

    std::string* mutable_string(int32_t tuple_id, int32_t slot_id);
    // slot start with 1
    ExprValue get_value(int32_t tuple_id, int32_t slot_id) {
        auto tuple = _tuples[tuple_id];
        if (tuple == nullptr) {
            return ExprValue::Null();
        }
        const google::protobuf::Descriptor* descriptor = tuple->GetDescriptor();
        // logical plan保证下标肯定是slot-1
        auto field = descriptor->field(slot_id - 1);
        return MessageHelper::get_value(field, tuple);
    }

    int set_value(int32_t tuple_id, int32_t slot_id, const ExprValue& value) {
        auto tuple = _tuples[tuple_id];
        if (tuple == nullptr) {
            return -1;
        }
        const google::protobuf::Descriptor* descriptor = tuple->GetDescriptor();
        auto field = descriptor->field(slot_id - 1);

        return MessageHelper::set_value(field, tuple, value);
    }

    int copy_from(std::unordered_set<int32_t>& tuple_ids, const MemRow* mem_row) {
        for (auto& tuple_id : tuple_ids) {
            if ((int32_t)(_tuples.size()) <= tuple_id) {
                DB_WARNING("tuple not in memrow");
                return -1;
            }
            _tuples[tuple_id]->CopyFrom(*(mem_row->_tuples[tuple_id]));
        }
        return 0;
    }
    //void print_content() {
    //    for (auto& tuple : _tuples) {
    //        DB_WARNING("tuple:%s", tuple->DebugString().c_str());
    //    }
    //}
    int decode_key(int32_t tuple_id, IndexInfo& index,
            std::vector<int32_t>& field_slot, const TableKey& key, int& pos);
    int decode_primary_key(int32_t tuple_id, IndexInfo& index, std::vector<int32_t>& field_slot, 
            const TableKey& key, int& pos);

    // for cstore
    int decode_field(int32_t tuple_id, int32_t slot_id, pb::PrimitiveType field_type, const rocksdb::Slice& in) {
        auto tuple = _tuples[tuple_id];
        if (tuple == nullptr) {
            return -1;
        }
        auto descriptor = tuple->GetDescriptor();
        auto field = descriptor->field(slot_id - 1);
        if (field == nullptr) {
            DB_WARNING("invalid field: %d", slot_id);
            return -1;
        }
        return MessageHelper::decode_field(field, field_type, tuple, in);
    }

    private:
    std::vector<google::protobuf::Message*> _tuples;
};
}

