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

#include <memory>
#include "common.h"
#include "expr_value.h"
#include "schema_factory.h"
#include "message_helper.h"
#include "proto/meta.interface.pb.h"
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>

namespace baikaldb {
using google::protobuf::FieldDescriptor;
using google::protobuf::Descriptor;
using google::protobuf::Message;
using google::protobuf::Reflection;
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
    bool is_null(const FieldDescriptor* field) {
        return MessageHelper::is_null(field, _message);
    }

    //get_int32 value and store in val
    int get_int32(const FieldDescriptor* field, int32_t& val) {
        return MessageHelper::get_int32(field, _message, val);
    }

    void set_int32(const FieldDescriptor* field, int32_t val) {
        MessageHelper::set_int32(field, _message, val);
    }

    int get_uint32(const FieldDescriptor* field, uint32_t& val) {
        return MessageHelper::get_uint32(field, _message, val);
    }

    void set_uint32(const FieldDescriptor* field, uint32_t val) {
        MessageHelper::set_uint32(field, _message, val);
    }

    int get_int64(const FieldDescriptor* field, int64_t& val) {
        return MessageHelper::get_int64(field, _message, val);
    }

    void set_int64(const FieldDescriptor* field, int64_t val) {
        MessageHelper::set_int64(field, _message, val);
    }

    int get_uint64(const FieldDescriptor* field, uint64_t& val) {
        return MessageHelper::get_uint64(field, _message, val);
    }

    void set_uint64(const FieldDescriptor* field, uint64_t val) {
        MessageHelper::set_uint64(field, _message, val);
    }

    int get_float(const FieldDescriptor* field, float& val) {
        return MessageHelper::get_float(field, _message, val);
    }

    void set_float(const FieldDescriptor* field, float val) {
        MessageHelper::set_float(field, _message, val);
    }

    int get_double(const FieldDescriptor* field, double& val) {
        return MessageHelper::get_double(field, _message, val);
    }

    void set_double(const FieldDescriptor* field, double val) {
        MessageHelper::set_double(field, _message, val);
    }

    int get_string(const FieldDescriptor* field, std::string& val) {
        return MessageHelper::get_string(field, _message, val);
    }

    void set_string(const FieldDescriptor* field, std::string val) {
        MessageHelper::set_string(field, _message, val);
    }

    int get_boolean(const FieldDescriptor* field, bool& val) {
        return MessageHelper::get_boolean(field, _message, val);
    }

    void set_boolean(const FieldDescriptor* field, bool val) {
        MessageHelper::set_boolean(field, _message, val);
    }

    ExprValue get_value(const FieldDescriptor* field) {
        return MessageHelper::get_value(field, _message);
    }

    // by_tag default true
    int set_value(const FieldDescriptor* field, const ExprValue& value) {
        if (field == nullptr) {
            DB_WARNING("Invalid Field Descriptor");
            return -1;
        }
        return MessageHelper::set_value(field, _message, value);
    }
/*
    // by_tag default true
    int set_default_value(const FieldInfo& field_info) {
        auto field = get_field_by_idx(field_info.pb_idx);
        if (field == nullptr) {
            DB_WARNING("Invalid Field Descriptor");
            return -1;
        }
        return MessageHelper::set_value(field, _message, field_info.default_expr_value);
    }
*/
    void clear_field(const FieldDescriptor* field);

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
            _used_size += in.size();
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
            MutTableKey& key, bool clear, bool like_prefix);

    int field_to_string(const FieldInfo& field_info, std::string* out, bool* is_null);

    //TODO: secondary key
    int decode_field(const Reflection* _reflection,
            const FieldDescriptor* field, 
            const FieldInfo& field_info,
            const TableKey& key, int& pos);

    //TODO: secondary key
    //(field_cnt == -1) means encode all key field
    int encode_key(IndexInfo& index, MutTableKey& key, int field_cnt, 
            bool clear, bool like_prefix = false);

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

    // those two funcs are only used for encode/decode non-pk fields after primary, for cstore
    int encode_field_for_cstore(const FieldInfo& field_info, std::string& out);
    int decode_field(const FieldInfo& field_info, const rocksdb::Slice& in);

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

    std::string get_index_value(IndexInfo& index);

    void clear() {
        if (_message) {
            _message->Clear();
        }
    }

    int64_t used_size() {
        return sizeof(TableRecord) + _used_size + get_protobuf_space_size(*_message);
    }

    static SmartRecord new_record(int64_t tableid);

private:
    Message* _message = nullptr;
    int64_t  _used_size = 0;
};

class BatchRecord {
public:
    BatchRecord() {
        _batch.reserve(_capacity);
    }
    size_t size() {
        return _batch.size();
    }
    void emplace_back(const SmartRecord& record) {
        _batch.emplace_back(record);
    }
    SmartRecord& get_next() {
        return _batch[_idx++];
    }
    bool is_full() {
        return size() >= _capacity;
    }
    bool is_traverse_over() {
        return _idx >= size();
    }
    void set_capacity(size_t capacity) {
        _capacity = capacity;
    }
    void clear() {
        _batch.clear();
        _idx = 0;
    }
private:
    size_t _idx = 0;
    size_t _capacity = ROW_BATCH_CAPACITY;
    std::vector<SmartRecord> _batch;
    DISALLOW_COPY_AND_ASSIGN(BatchRecord);
};

}
