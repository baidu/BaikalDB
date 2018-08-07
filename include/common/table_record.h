// Copyright (c) 2018 Baidu, Inc. All Rights Reserved.
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

#include <memory>
#include "common.h"
#include "expr_value.h"
#include "proto/meta.interface.pb.h"
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
 
using google::protobuf::FieldDescriptor;
using google::protobuf::Descriptor;
using google::protobuf::Message;
using google::protobuf::Reflection;

namespace baikaldb {
class TableKey;
class MutTableKey;
class TableRecord;
class IndexInfo;
class FieldInfo;
typedef std::shared_ptr<TableRecord> SmartRecord;
class TableRecord {
public:
    ~TableRecord() {
        delete _message;
        _message = nullptr;
    }

    TableRecord() {}

    TableRecord(Message* _m); // : _message(_m) {}

    // by_tag=true  ==> search field by tag
    // by_tag=false ==> search field by index (0-idx_cnt-1)
    bool is_null(const FieldDescriptor* field);

    //get_int32 value and store in val
    int get_int32(const FieldDescriptor* field, int32_t& val);

    void set_int32(const FieldDescriptor* field, int32_t val);

    int get_uint32(const FieldDescriptor* field, uint32_t& val);

    void set_uint32(const FieldDescriptor* field, uint32_t val);

    int get_int64(const FieldDescriptor* field, int64_t& val);

    void set_int64(const FieldDescriptor* field, int64_t val);

    int get_uint64(const FieldDescriptor* field, uint64_t& val);

    void set_uint64(const FieldDescriptor* field, uint64_t val);

    int get_float(const FieldDescriptor* field, float& val);

    void set_float(const FieldDescriptor* field, float val);

    int get_double(const FieldDescriptor* field, double& val);

    void set_double(const FieldDescriptor* field, double val);

    int get_string(const FieldDescriptor* field, std::string& val);

    void set_string(const FieldDescriptor* field, std::string val);

    int get_boolean(const FieldDescriptor* field, bool& val);

    void set_boolean(const FieldDescriptor* field, bool val);

    void clear_field(const FieldDescriptor* field);

    ExprValue get_value(const FieldDescriptor* field);

    int set_value(const FieldDescriptor* field, const ExprValue& value);

    int get_reverse_word(IndexInfo& index_info, std::string& word);

    int encode(std::string& out) {
        if (_message->SerializeToString(&out)) {
            return 0;
        }
        return -1;
    }

    int encode(char* data, int size) {
        if (_message->SerializeToArray(data, size)) {
            return 0;
        }
        return -1;
    }

    int decode(const std::string& in) {
        if (_message->ParseFromString(in)) {
            return 0;
        }
        return -1;
    }

    int decode(const char* data, int size) {
        if (_message->ParseFromArray(data, size)) {
            return 0;
        }
        return -1;
    }

    //clear: whether clear the key field in proto after encode
    //clear is true for pk field, false for secondary key field
    int encode_field(const Reflection* _reflection, 
            const FieldDescriptor* field, 
            const FieldInfo& field_info,
            MutTableKey& key, bool clear);

    //TODO: secondary key
    int decode_field(const Reflection* _reflection,
            const FieldDescriptor* field, 
            const FieldInfo& field_info,
            const TableKey& key, int& pos);

    //TODO: secondary key
    //(field_cnt == -1) means encode all key field
    int encode_key(IndexInfo& index, MutTableKey& key, int field_cnt, bool clear);

    // decode and fill into *this (primary/secondary) starting from 0
    int decode_key(IndexInfo& index, const TableKey& key);
    int decode_key(IndexInfo& index, const std::string& key);

    // decode and fill *this (primary/secondary) starting from pos,
    // and pos moves forward after decode
    int decode_key(IndexInfo& index, const TableKey& key, int& pos);
    int decode_key(IndexInfo& index, const std::string& key, int& pos);

    // these two funcs are only used for encode/decode pk fields after secondary index
    int encode_primary_key(IndexInfo& index, MutTableKey& key, int field_cnt);
    int decode_primary_key(IndexInfo& index, const TableKey& key, int& pos);

    const FieldDescriptor* get_field_by_idx(int32_t idx) {
        auto descriptor = _message->GetDescriptor();
        return descriptor->field(idx);
    }

    const FieldDescriptor* get_field_by_tag(int32_t idx) {
        auto descriptor = _message->GetDescriptor();
        return descriptor->FindFieldByNumber(idx);
    }

    const FieldDescriptor* get_field_by_name(const std::string& name) {
        auto descriptor = _message->GetDescriptor();
        return descriptor->FindFieldByName(name);
    }

    void merge_from(SmartRecord other) {
        _message->MergeFrom(*other->_message);
    }

    SmartRecord clone(bool merge_data = true) {
        SmartRecord record(new TableRecord(_message->New()));
        if (merge_data) {
            record->_message->MergeFrom(*_message);
        }
        return record;
    }
 
    Message* get_raw_message() {
        return _message;
    }

    std::string to_string() {
        return pb2json(*_message);
    }

    std::string debug_string() {
        if (_message) {
            return _message->ShortDebugString();
        }
        return "";
    }

    void clear() {
        if (_message) {
            _message->Clear();
        }
    }

    static SmartRecord new_record(int64_t tableid);

private:
    Message* _message = nullptr;

};
}
