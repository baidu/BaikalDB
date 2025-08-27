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

#include <stdint.h>
#include <vector>
#include <memory>
#include "chunk.h"
#include "common.h"
#include "schema_factory.h"
#include "table_key.h"
#include "mem_row_descriptor.h"
#include <arrow/type.h>
#include <arrow/api.h> 

namespace baikaldb {
DEFINE_int32(chunk_max_size_mb, 100, "chunk max size mb");

bool Chunk::exceed_max_size() {
    return (used_bytes_size() > FLAGS_chunk_max_size_mb * 1024 * 1024ULL);
}

int Chunk::init_tuple_info(const pb::TupleDescriptor* tuple) {
    int tuple_id = tuple->tuple_id();
    for (const auto& slot : tuple->slots()) {
        std::string name = std::to_string(tuple_id) + "_" + std::to_string(slot.slot_id());
        auto pb_type = primitive_to_proto_type(slot.slot_type());
        if (pb_type == -1) {
            return -1;
        }
        switch (pb_type)
        {
        case FieldDescriptorProto::TYPE_BOOL: // pb::NULL_TYPE, pb::BOOL
            _fields.emplace_back(std::make_shared<arrow::Field>(name, arrow::boolean()));
            _builders.emplace_back(std::make_shared<arrow::BooleanBuilder>());
            _size_per_row += sizeof(bool);
            break;
        case FieldDescriptorProto::TYPE_SINT32:   // pb::INT8, pb::INT16, pb::INT32
        case FieldDescriptorProto::TYPE_SFIXED32: // pb::TIME
            _fields.emplace_back(std::make_shared<arrow::Field>(name, arrow::int32()));
            _builders.emplace_back(std::make_shared<arrow::Int32Builder>());
            _size_per_row += sizeof(int32_t);
            break;
        case FieldDescriptorProto::TYPE_SINT64: // pb::INT64
            _fields.emplace_back(std::make_shared<arrow::Field>(name, arrow::int64()));
            _builders.emplace_back(std::make_shared<arrow::Int64Builder>());
            _size_per_row += sizeof(int64_t);
            break;
        case FieldDescriptorProto::TYPE_UINT32:  // pb::UINT8, pb::UINT16, pb::UINT32
        case FieldDescriptorProto::TYPE_FIXED32: // pb::TIMESTAMP, pb::DATE
            _fields.emplace_back(std::make_shared<arrow::Field>(name, arrow::uint32()));
            _builders.emplace_back(std::make_shared<arrow::UInt32Builder>());
            _size_per_row += sizeof(int32_t);
            break;
        case FieldDescriptorProto::TYPE_UINT64:  // pb::UINT64
        case FieldDescriptorProto::TYPE_FIXED64: // pb::DATETIME
            _fields.emplace_back(std::make_shared<arrow::Field>(name, arrow::uint64()));
            _builders.emplace_back(std::make_shared<arrow::UInt64Builder>());
            _size_per_row += sizeof(int64_t);
            break;
        case FieldDescriptorProto::TYPE_FLOAT: // pb::FLOAT
            _fields.emplace_back(std::make_shared<arrow::Field>(name, arrow::float32()));
            _builders.emplace_back(std::make_shared<arrow::FloatBuilder>());
            _size_per_row += sizeof(float);
            break;
        case FieldDescriptorProto::TYPE_DOUBLE: // pb::DOUBLE
            _fields.emplace_back(std::make_shared<arrow::Field>(name, arrow::float64()));
            _builders.emplace_back(std::make_shared<arrow::DoubleBuilder>());
            _size_per_row += sizeof(double);
            break;
        case FieldDescriptorProto::TYPE_BYTES: // pb::STRING, pb::HLL, pb::BITMAP, pb::TDIGEST
            _fields.emplace_back(std::make_shared<arrow::Field>(name, arrow::large_binary()));
            _builders.emplace_back(std::make_shared<arrow::LargeBinaryBuilder>());
            break;
        default:
            DB_FATAL("unkown mysql type: %d", pb_type);
            return -1;
        }
        _field_types.emplace_back((FieldDescriptorProto::Type)pb_type);
    }
    return 0;
}

int Chunk::init(const std::vector<const pb::TupleDescriptor*>& tuples) {
    // {<tuple2, filed 1...5> <tuple3, field 1...3>} -> offset: 0, 0, 0, 5
    // 外部保证tuples按照tuple_id从小到大排序
    for (auto& tuple : tuples) {
        if (tuple == nullptr) {
            continue;
        }
        if (!tuple->has_tuple_id()) {
            continue;
        }
        int last_field_pos = _tuple_offsets.empty() ? 0 : _tuple_offsets.back();
        while (_tuple_offsets.size() <= tuple->tuple_id()) {
            _tuple_offsets.emplace_back(last_field_pos);
        }
        _field_num += tuple->slots_size();
        _tuple_offsets.emplace_back(_field_num);
        if (tuple->tuple_id() >= _slot_idxes_mapping.size()) {
            _slot_idxes_mapping.resize(tuple->tuple_id() + 1);
        }
        std::vector<int32_t> slot_idxes(tuple->slot_idxes().begin(), tuple->slot_idxes().end());
        _slot_idxes_mapping[tuple->tuple_id()] = slot_idxes;
    }
    _field_types.reserve(_field_num);
    _fields.reserve(_field_num);
    _builders.reserve(_field_num);
    _tmp_row_values.resize(_field_num);
    for (auto& tuple : tuples) {
        if (init_tuple_info(tuple) < 0) {
            return -1;
        }
    }
    _schema = std::make_shared<arrow::Schema>(_fields);
    _init = true;
    return 0;
}

int Chunk::decode_key(int32_t tuple_id, IndexInfo& index, 
        std::vector<int32_t>& field_slot, const TableKey& key, int& pos) {
    if (index.type == pb::I_NONE) {
        DB_WARNING("unknown table index type: %ld", index.id);
        return -1;
    }
    uint8_t null_flag = 0;
    if (index.type == pb::I_KEY || index.type == pb::I_UNIQ || index.type == pb::I_ROLLUP) {
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
        int tmp_pos = get_field_pos(tuple_id, slot);
        if (tmp_pos < 0) {
            DB_WARNING("invalid tmp pos, tuple_id: %d, slot: %d", tuple_id, slot);
            return -1;
        }
        if (0 != key.decode_field_for_chunk(&_tmp_row_values[tmp_pos], index.fields[idx], pos)) {
            DB_WARNING("decode index field error");
            return -1;
        }
    }
    return 0;
}

int Chunk::decode_primary_key(int32_t tuple_id, IndexInfo& index, std::vector<int32_t>& field_slot, 
        const TableKey& key, int& pos) {
    if (index.type != pb::I_KEY && index.type != pb::I_UNIQ) {
        DB_WARNING("invalid secondary index type: %ld", index.id);
        return -1;
    }
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
        int tmp_pos = get_field_pos(tuple_id, slot);
        if (tmp_pos < 0) {
            DB_WARNING("invalid tmp pos, tuple_id: %d, slot: %d", tuple_id, slot);
            return -1;
        }
        if (0 != key.decode_field_for_chunk(&_tmp_row_values[tmp_pos], field_info, pos)) {
            DB_WARNING("decode index field error");
            return -1;
        }
    }
    return 0;
}

int Chunk::finish_and_make_record_batch(std::shared_ptr<arrow::RecordBatch>* out, std::shared_ptr<arrow::Schema> schema) {
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    if (schema == nullptr) {
        arrays.reserve(_builders.size());
        for (int idx = 0; idx < _builders.size(); ++idx) {
            int ret = finish_and_reset_one_column(arrays, idx);
            if (ret != 0) {
                DB_WARNING("finish_and_reset failed %d", idx);
                return ret;
            }
        }
        *out = arrow::RecordBatch::Make(_schema, _row_length, arrays);
    } else {
        std::vector<std::string> vec;
        arrays.reserve(schema->num_fields());
        for (const auto& f_name : schema->field_names()) {
            vec.clear();
            boost::split(vec, f_name, boost::is_any_of("_"));
            if (vec.size() != 2) {
                DB_WARNING("invalid field name: %s", f_name.c_str());
                return -1;
            }
            int tuple_id = strtoll(vec[0].c_str(), NULL, 10);
            int slot_id = strtoll(vec[1].c_str(), NULL, 10);
            int idx = get_field_pos(tuple_id, slot_id);
            if (idx < 0) {
                DB_WARNING("invalid field name: %s", f_name.c_str());
                return -1;
            }
            int ret = finish_and_reset_one_column(arrays, idx);
            if (ret != 0) {
                DB_WARNING("finish_and_reset failed %d", idx);
                return ret;
            }
        }
        *out = arrow::RecordBatch::Make(schema, _row_length, arrays);
    }
    _row_length = 0;
    _string_value_size = 0;
    return 0;
}

int Chunk::finish_and_reset_one_column(std::vector<std::shared_ptr<arrow::Array>>& arrays, int& idx) {
    switch (_field_types[idx]) {
        case FieldDescriptorProto::TYPE_BOOL: { // pb::NULL_TYPE, pb::BOOL
            auto status = static_cast<arrow::BooleanBuilder*>(_builders[idx].get())->Finish();
            static_cast<arrow::BooleanBuilder*>(_builders[idx].get())->Reset();
            if (!status.ok()) {
                DB_FATAL("array finish fail");
                return -1;
            }
            arrays.emplace_back(*status);
            break;
        }
        case FieldDescriptorProto::TYPE_SINT32:     // pb::INT8, pb::INT16, pb::INT32
        case FieldDescriptorProto::TYPE_SFIXED32: { // pb::TIME
            auto status = static_cast<arrow::Int32Builder*>(_builders[idx].get())->Finish();
            static_cast<arrow::Int32Builder*>(_builders[idx].get())->Reset();
            if (!status.ok()) {
                DB_FATAL("array finish fail");
                return -1;
            }
            arrays.emplace_back(*status);
            break;
        }
        case FieldDescriptorProto::TYPE_SINT64: { // pb::INT64
            auto status = static_cast<arrow::Int64Builder*>(_builders[idx].get())->Finish();
            static_cast<arrow::Int64Builder*>(_builders[idx].get())->Reset();
            if (!status.ok()) {
                DB_FATAL("array finish fail");
                return -1;
            }
            arrays.emplace_back(*status);
            break;
        }
        case FieldDescriptorProto::TYPE_UINT32:    // pb::UINT8, pb::UINT16, pb::UINT32
        case FieldDescriptorProto::TYPE_FIXED32: { // pb::TIMESTAMP, pb::DATE
            auto status = static_cast<arrow::UInt32Builder*>(_builders[idx].get())->Finish();
            static_cast<arrow::UInt32Builder*>(_builders[idx].get())->Reset();
            if (!status.ok()) {
                DB_FATAL("array finish fail");
                return -1;
            }
            arrays.emplace_back(*status);
            break;
        }
        case FieldDescriptorProto::TYPE_UINT64:    // pb::UINT64
        case FieldDescriptorProto::TYPE_FIXED64: { // pb::DATETIME
            auto status = static_cast<arrow::UInt64Builder*>(_builders[idx].get())->Finish();
            static_cast<arrow::UInt64Builder*>(_builders[idx].get())->Reset();
            if (!status.ok()) {
                DB_FATAL("array finish fail");
                return -1;
            }
            arrays.emplace_back(*status);
            break;
        }
        case FieldDescriptorProto::TYPE_FLOAT: { // pb::FLOAT
            auto status = static_cast<arrow::FloatBuilder*>(_builders[idx].get())->Finish();
            static_cast<arrow::FloatBuilder*>(_builders[idx].get())->Reset();
            if (!status.ok()) {
                DB_FATAL("array finish fail");
                return -1;
            }
            arrays.emplace_back(*status);
            break;
        }
        case FieldDescriptorProto::TYPE_DOUBLE: { // pb::DOUBLE
            auto status = static_cast<arrow::DoubleBuilder*>(_builders[idx].get())->Finish();
            static_cast<arrow::DoubleBuilder*>(_builders[idx].get())->Reset();
            if (!status.ok()) {
                DB_FATAL("array finish fail");
                return -1;
            }
            arrays.emplace_back(*status);
            break;
        }
        case FieldDescriptorProto::TYPE_BYTES: { // pb::STRING, pb::HLL, pb::BITMAP, pb::TDIGEST
            auto status = static_cast<arrow::LargeBinaryBuilder*>(_builders[idx].get())->Finish();
            static_cast<arrow::LargeBinaryBuilder*>(_builders[idx].get())->Reset();
            if (!status.ok()) {
                DB_FATAL("array finish fail");
                return -1;
            }
            arrays.emplace_back(*status);
            break;
        }
        default:
            DB_FATAL("unkown pb type: %d", _field_types[idx]);
    }
    return 0;
}

int Chunk::append_value(int field_pos, const ExprValue& value) {
    // [ARROW TODO]直接append_arrow;每列resize？
    if (field_pos < 0 || field_pos > _field_num) {
        return -1;
    } 
    auto pb_type = _field_types[field_pos];
    if (value.type == pb::NULL_TYPE) {
        _builders[field_pos]->AppendNull();
        return 0;
    }
    arrow::Status s;
    switch (pb_type) {
        case FieldDescriptorProto::TYPE_BOOL: // pb::NULL_TYPE, pb::BOOL
            s = static_cast<arrow::BooleanBuilder*>(_builders[field_pos].get())->Append(value.get_numberic<bool>());
            break;
        case FieldDescriptorProto::TYPE_SINT32:   // pb::INT8, pb::INT16, pb::INT32
        case FieldDescriptorProto::TYPE_SFIXED32: // pb::TIME
            s = static_cast<arrow::Int32Builder*>(_builders[field_pos].get())->Append(value._u.int32_val);
            break;
        case FieldDescriptorProto::TYPE_SINT64: // pb::INT64
            s = static_cast<arrow::Int64Builder*>(_builders[field_pos].get())->Append(value._u.int64_val);
            break;
        case FieldDescriptorProto::TYPE_UINT32:  // pb::UINT8, pb::UINT16, pb::UINT32
        case FieldDescriptorProto::TYPE_FIXED32: // pb::TIMESTAMP, pb::DATE
            s = static_cast<arrow::UInt32Builder*>(_builders[field_pos].get())->Append(value._u.uint32_val);
            break;
        case FieldDescriptorProto::TYPE_UINT64:  // pb::UINT64
        case FieldDescriptorProto::TYPE_FIXED64: // pb::DATETIME
            s = static_cast<arrow::UInt64Builder*>(_builders[field_pos].get())->Append(value._u.uint64_val);
            break;
        case FieldDescriptorProto::TYPE_FLOAT: // pb::FLOAT
            s = static_cast<arrow::FloatBuilder*>(_builders[field_pos].get())->Append(value._u.float_val);
            break;
        case FieldDescriptorProto::TYPE_DOUBLE: // pb::DOUBLE
            s = static_cast<arrow::DoubleBuilder*>(_builders[field_pos].get())->Append(value._u.double_val);
            break;
        case FieldDescriptorProto::TYPE_BYTES: // pb::STRING, pb::HLL, pb::BITMAP, pb::TDIGEST
            s = static_cast<arrow::LargeBinaryBuilder*>(_builders[field_pos].get())->Append(value.str_val);
            _string_value_size += value.size();
            break;
        default:
            DB_FATAL("unkown pb type: %d", pb_type);
            return -1;
    }
    if (!s.ok()) {
        DB_FATAL("array append error: %s", s.ToString().c_str());
        return -1;
    }
    return 0;
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */