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

#include <stdint.h>
#include <vector>
#include <memory>
#include "mem_row_descriptor.h"
#include "mem_row_compare.h"
#include <arrow/type.h>
#include <arrow/api.h> 

namespace baikaldb {
DECLARE_int32(chunk_size);
class Chunk {
public:
    Chunk() {};
    virtual ~Chunk() {};
    int init(const std::vector<const pb::TupleDescriptor*>& tuples); //db
    int init_tuple_info(const pb::TupleDescriptor* tuple);

    std::shared_ptr<arrow::Schema> get_arrow_schema() {
        return _schema;
    }

    inline int get_field_pos(int tuple_id, int slot_id) {
        if (tuple_id < 0 || tuple_id >= _tuple_offsets.size() || tuple_id >= _slot_idxes_mapping.size()) {
            return -1;
        }
        const auto& slot_idxes = _slot_idxes_mapping[tuple_id];
        if (slot_idxes.empty()) {
            return _tuple_offsets[tuple_id] + slot_id - 1;
        }
        if (slot_id - 1 < 0 || slot_id - 1 >= slot_idxes.size()) {
            return -1;
        }
        return _tuple_offsets[tuple_id] + slot_idxes[slot_id - 1];
    }
    bool has_init() {
        return _init;
    }
    void reserve(int row_size) {
        for (auto& builder : _builders) {
            builder->Reserve(row_size);
        }
    }

    size_t size() {
        return _row_length;
    }

    bool is_full() {
        return _row_length >= _capacity;
    }

    int64_t used_bytes_size() {
        return _size_per_row * _row_length + _string_value_size;
    }
    
    bool exceed_max_size();

    int append_value(const int tuple_id, const int slot_id, const ExprValue& value) {
        return append_value(get_field_pos(tuple_id, slot_id), value);
    }

    int add_tmp_row() {
        for (int i = 0; i < _field_num; ++i) {
            int ret = append_value(i, _tmp_row_values[i]);
            if (ret != 0) {
                DB_FATAL("add tmp row fail, field_idx: %d", i);
                return -1;
            }
        }
        ++_row_length;
        return 0;
    }

    int add_row(const std::vector<const pb::TupleDescriptor*>& tuples, MemRow* row) {
        for (auto& tuple : tuples) {
            for (auto& slot : tuple->slots()) {
                int pos = get_field_pos(tuple->tuple_id(), slot.slot_id());
                auto value = row->get_value(tuple->tuple_id(), slot.slot_id());
                int ret = append_value(pos, value);
                if (ret != 0) {
                    DB_FATAL("add memrow to chunk fail, tuple_id: %d, slot: %d", tuple->tuple_id(), slot.slot_id());
                    return -1;
                }
            }
        }
        ++_row_length;
        return 0;
    }

    int append_value(int field_pos, const ExprValue& value);
    int finish_and_make_record_batch(std::shared_ptr<arrow::RecordBatch>* out, std::shared_ptr<arrow::Schema> schema = nullptr);
    int finish_and_reset_one_column(std::vector<std::shared_ptr<arrow::Array>>& arrays, int& idx);

    ExprValue* get_tmp_field_value(int tuple_id, int slot_id) {
        int pos = get_field_pos(tuple_id, slot_id);
        if (pos < 0 || pos >= _tmp_row_values.size()) {
            return nullptr;
        }
        return &_tmp_row_values[pos];
    }

    int set_tmp_field_value(int tuple_id, int slot_id, const ExprValue& value) {
        ExprValue* tmp_v = get_tmp_field_value(tuple_id, slot_id);
        if (tmp_v == nullptr) {
            return -1;
        }
        *tmp_v = value;
        return 0;
    }

    int get_field_type_by_slot(int32_t tuple_id, int32_t slot_id, FieldDescriptorProto::Type& type) {
        int pos = get_field_pos(tuple_id, slot_id);
        if (pos < 0 || pos >= _tmp_row_values.size()) {
            return -1;
        }
        type = _field_types[pos];
        return 0;
    }
    int decode_key(int32_t tuple_id, IndexInfo& index, 
                   std::vector<int32_t>& field_slot, const TableKey& key, int& pos);
    int decode_primary_key(int32_t tuple_id, IndexInfo& index, std::vector<int32_t>& field_slot, 
                   const TableKey& key, int& pos);
private:
    int _field_num = 0;
    int _row_length = 0;
    int _capacity = FLAGS_chunk_size;
    int _size_per_row = 0;          // 数值型列单行size
    int64_t _string_value_size = 0; // 字符串列总size
    
    // 将tuples打平, 如
    // tuple1[int,bool,string] tuple2[int] -> arrow fields[int,bool,string,int]
    std::vector<std::shared_ptr<arrow::Field>> _fields;
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> _builders;
    std::vector<int> _tuple_offsets;
    std::vector<std::vector<int32_t>> _slot_idxes_mapping;
    std::vector<FieldDescriptorProto::Type> _field_types;
    std::vector<ExprValue> _tmp_row_values;
    std::shared_ptr<arrow::Schema> _schema;
    bool _init = false;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */