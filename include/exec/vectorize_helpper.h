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
#include "expr_value.h"
#include <arrow/type.h>
#include <arrow/acero/options.h>
#include <arrow/stl_iterator.h>
namespace baikaldb {
class VectorizeHelpper {
public:
    VectorizeHelpper() {};
    ~VectorizeHelpper() {};

    static ExprValue get_vectorized_value(const arrow::ChunkedArray* chunked_array, int row_idx) {
        ExprValue ret;
        ret.type = pb::NULL_TYPE;
        switch (chunked_array->type()->id()) {
            case arrow::Type::BOOL: {
                arrow::stl::ChunkedArrayIterator<arrow::BooleanArray> iter(*chunked_array);
                if (iter[row_idx].has_value()) {
                    ret.type = pb::BOOL;
                    ret._u.bool_val = iter[row_idx].value();
                }
                break;
            }
            case arrow::Type::UINT8: {
                arrow::stl::ChunkedArrayIterator<arrow::UInt8Array> iter(*chunked_array);
                if (iter[row_idx].has_value()) {
                    ret.type = pb::UINT8;
                    ret._u.uint8_val = iter[row_idx].value();
                }
                break;
            }
            case arrow::Type::UINT16: {
                arrow::stl::ChunkedArrayIterator<arrow::UInt16Array> iter(*chunked_array);
                if (iter[row_idx].has_value()) {
                    ret.type = pb::UINT16;
                    ret._u.uint16_val = iter[row_idx].value();
                }
                break;
            }
            case arrow::Type::UINT32: {
                arrow::stl::ChunkedArrayIterator<arrow::UInt32Array> iter(*chunked_array);
                if (iter[row_idx].has_value()) {
                    ret.type = pb::UINT32;
                    ret._u.uint32_val = iter[row_idx].value();
                }
                break;
            }
            case arrow::Type::UINT64: {
                arrow::stl::ChunkedArrayIterator<arrow::UInt64Array> iter(*chunked_array);
                if (iter[row_idx].has_value()) {
                    ret.type = pb::UINT64;
                    ret._u.uint64_val = iter[row_idx].value();
                }
                break;
            }
            case arrow::Type::INT8: {
                arrow::stl::ChunkedArrayIterator<arrow::Int8Array> iter(*chunked_array);
                if (iter[row_idx].has_value()) {
                    ret.type = pb::INT8;
                    ret._u.int8_val = iter[row_idx].value();
                }
                break;
            }
            case arrow::Type::INT16: {
                arrow::stl::ChunkedArrayIterator<arrow::Int16Array> iter(*chunked_array);
                if (iter[row_idx].has_value()) {
                    ret.type = pb::INT16;
                    ret._u.int16_val = iter[row_idx].value();
                }
                break;
            }
            case arrow::Type::INT32: {
                arrow::stl::ChunkedArrayIterator<arrow::Int32Array> iter(*chunked_array);
                if (iter[row_idx].has_value()) {
                    ret.type = pb::INT32;
                    ret._u.int32_val = iter[row_idx].value();
                }
                break;
            }
            case arrow::Type::INT64: {
                arrow::stl::ChunkedArrayIterator<arrow::Int64Array> iter(*chunked_array);
                if (iter[row_idx].has_value()) {
                    ret.type = pb::INT64;
                    ret._u.int64_val = iter[row_idx].value();
                }
                break;
            }
            case arrow::Type::FLOAT: {
                arrow::stl::ChunkedArrayIterator<arrow::FloatArray> iter(*chunked_array);
                if (iter[row_idx].has_value()) {
                    ret.type = pb::FLOAT;
                    ret._u.float_val = iter[row_idx].value();
                }
                break;
            }
            case arrow::Type::DOUBLE: {
                arrow::stl::ChunkedArrayIterator<arrow::DoubleArray> iter(*chunked_array);
                if (iter[row_idx].has_value()) {
                    ret.type = pb::DOUBLE;
                    ret._u.double_val = iter[row_idx].value();
                }
                break;
            }
            case arrow::Type::BINARY: {
                arrow::stl::ChunkedArrayIterator<arrow::BinaryArray> iter(*chunked_array);
                if (iter[row_idx].has_value()) {
                    ret.type = pb::STRING;
                    ret.str_val = iter[row_idx].value();
                }
                break;
            }
            case arrow::Type::LARGE_BINARY: {
                arrow::stl::ChunkedArrayIterator<arrow::LargeBinaryArray> iter(*chunked_array);
                if (iter[row_idx].has_value()) {
                    ret.type = pb::STRING;
                    ret.str_val = iter[row_idx].value();
                }
                break;
            }
            case arrow::Type::STRING: {
                arrow::stl::ChunkedArrayIterator<arrow::StringArray> iter(*chunked_array);
                if (iter[row_idx].has_value()) {
                    ret.type = pb::STRING;
                    ret.str_val = iter[row_idx].value();
                }
                break;
            }
            case arrow::Type::LARGE_STRING: {
                arrow::stl::ChunkedArrayIterator<arrow::LargeStringArray> iter(*chunked_array);
                if (iter[row_idx].has_value()) {
                    ret.type = pb::STRING;
                    ret.str_val = iter[row_idx].value();
                }
                break;
            }
            default:
                break;
        }
        return ret;
    }

    static std::shared_ptr<arrow::Table> build_empty_table(std::vector<pb::TupleDescriptor>* tuple_decs, const std::vector<int32_t>& tuple_ids) {
        if (tuple_decs == nullptr) {
            return nullptr;
        }
        std::vector<std::shared_ptr<arrow::Field>> fields;
        for (auto tuple_id : tuple_ids) {
            const auto& tuple_dec = (*tuple_decs)[tuple_id];
            for (const auto& slot : tuple_dec.slots()) {
                std::string name = std::to_string(tuple_id) + "_" + std::to_string(slot.slot_id());
                auto pb_type = primitive_to_proto_type(slot.slot_type());
                if (pb_type == -1) {
                    return nullptr;
                }
                switch (pb_type) {
                    case FieldDescriptorProto::TYPE_BOOL: // pb::NULL_TYPE, pb::BOOL
                        fields.emplace_back(std::make_shared<arrow::Field>(name, arrow::boolean()));
                        break;
                    case FieldDescriptorProto::TYPE_SINT32:   // pb::INT8, pb::INT16, pb::INT32
                    case FieldDescriptorProto::TYPE_SFIXED32: // pb::TIME
                        fields.emplace_back(std::make_shared<arrow::Field>(name, arrow::int32()));
                        break;
                    case FieldDescriptorProto::TYPE_SINT64: // pb::INT64
                        fields.emplace_back(std::make_shared<arrow::Field>(name, arrow::int64()));
                        break;
                    case FieldDescriptorProto::TYPE_UINT32:  // pb::UINT8, pb::UINT16, pb::UINT32
                    case FieldDescriptorProto::TYPE_FIXED32: // pb::TIMESTAMP, pb::DATE
                        fields.emplace_back(std::make_shared<arrow::Field>(name, arrow::uint32()));
                        break;
                    case FieldDescriptorProto::TYPE_UINT64:  // pb::UINT64
                    case FieldDescriptorProto::TYPE_FIXED64: // pb::DATETIME
                        fields.emplace_back(std::make_shared<arrow::Field>(name, arrow::uint64()));
                        break;
                    case FieldDescriptorProto::TYPE_FLOAT: // pb::FLOAT
                        fields.emplace_back(std::make_shared<arrow::Field>(name, arrow::float32()));
                        break;
                    case FieldDescriptorProto::TYPE_DOUBLE: // pb::DOUBLE
                        fields.emplace_back(std::make_shared<arrow::Field>(name, arrow::float64()));
                        break;
                    case FieldDescriptorProto::TYPE_BYTES: // pb::STRING, pb::HLL, pb::BITMAP, pb::TDIGEST
                        fields.emplace_back(std::make_shared<arrow::Field>(name, arrow::binary()));
                        break;
                    default:
                        DB_FATAL("unkown mysql type: %d", pb_type);
                        return nullptr;
                }
            }
        }
        auto schema = std::make_shared<arrow::Schema>(fields);
        auto empty_table = arrow::Table::MakeEmpty(schema);
        if (empty_table.ok()) {
            return *empty_table;
        }
        return nullptr;
    }

    // schema: db构建的
    // in: store 返回的recordbatch
    static int change_arrow_record_batch_schema(std::shared_ptr<arrow::Schema> schema, std::shared_ptr<arrow::RecordBatch> in, std::shared_ptr<arrow::RecordBatch>* out) {
        std::vector<std::shared_ptr<arrow::Array>> columns;
        for (auto& f : schema->fields()) {
            std::shared_ptr<arrow::Array> array = in->GetColumnByName(f->name());
            if (array == nullptr) {
                DB_DEBUG("arrow schema not match, db schema: %s:  store schema: %s", 
                        schema->ToString().c_str(),
                        in->schema()->ToString().c_str());
                // 如multi count生成的set列, 构建一列null, 避免schema不一致失败
                auto null_array = arrow::MakeArrayOfNull(f->type(), in->num_rows());
                if (!null_array.ok()) {
                    DB_FATAL("arrow make null array fail, %s", null_array.status().ToString().c_str());
                    return -1;
                }
                array = *null_array;
            }
            columns.emplace_back(array);
        }
        *out = arrow::RecordBatch::Make(schema, in->num_rows(), columns);
        return 0;
    }

    static int vectorize_filter(std::shared_ptr<arrow::RecordBatch> record_batch, 
                arrow::Expression* conjuncts,
                std::shared_ptr<arrow::RecordBatch>* out) {
        if (conjuncts == nullptr) {
            return -1;
        }
        arrow::ExecBatch exec_batch(*record_batch);
        arrow::Result<arrow::Datum> filter_mask = arrow::compute::ExecuteScalarExpression(*conjuncts, exec_batch, /*ExecContext* = */nullptr);
        if (!filter_mask.ok()) {
            DB_FATAL("arrow filter fail, %s", filter_mask.status().ToString().c_str());
            return -1;
        }
        if (filter_mask->is_scalar()) {
            const auto& mask_scalar = filter_mask->scalar_as<arrow::BooleanScalar>();
            if (mask_scalar.is_valid && mask_scalar.value == true) {
                *out = record_batch;
            } else {
                *out = record_batch->Slice(0, 0);
            }
            return 0;
        }
        auto mask = filter_mask->array_as<arrow::BooleanArray>();
        arrow::Datum record_batch_datum(record_batch);
        arrow::Result<arrow::Datum> filter_data = arrow::compute::Filter(record_batch_datum, mask->data());
        *out = filter_data->record_batch();
        return 0;
    }
};

// [ARROW TODO, 复用FetcherStoreVectorizedReader]
class RowVectorizedReader : public arrow::RecordBatchReader {
public:
    // for streaming output use
    std::shared_ptr<arrow::Schema> schema() const override { 
        return _chunk->get_arrow_schema();
    }

    int init(RuntimeState* state, std::vector<MemRow*>* rows, const std::unordered_set<int32_t>& tuple_ids) {
        _state = state;
        _chunk = std::make_shared<Chunk>();
        for (auto tuple_id : tuple_ids) {
            _tuples.emplace_back(_state->get_tuple_desc(tuple_id));
        }
        std::sort(_tuples.begin(), _tuples.end(), [](const pb::TupleDescriptor* a, const pb::TupleDescriptor* b) {
            return a->tuple_id() < b->tuple_id();
        });
        if (0 != _chunk->init(_tuples)) {
            return -1;
        }
        _rows = rows;
        return 0;
    }
    
    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override {
        if (_rows == nullptr || _row_idx >= _rows->size()) {
            out->reset();
            return arrow::Status::OK();
        }
        for (; _row_idx < _rows->size(); ) {
            if (_chunk->add_row(_tuples, _rows->at(_row_idx))) {
                DB_FATAL_STATE(_state, "add row to chunk failed");
                return arrow::Status::IOError("add row to chunk failed");
            }
            ++_row_idx;
            if (_chunk->is_full()) {
                if (0 != _chunk->finish_and_make_record_batch(out)) {
                    return arrow::Status::IOError("make record batch failed");
                }
                return arrow::Status::OK();
            }
        }
        if (_chunk->size() > 0) {
            if (0 != _chunk->finish_and_make_record_batch(out)) {
                return arrow::Status::IOError("make record batch failed");
            }
            return arrow::Status::OK();
        }
        out->reset();
        return arrow::Status::OK();
    }

private:
    RuntimeState* _state = nullptr;
    std::vector<const pb::TupleDescriptor*> _tuples;
    std::vector<MemRow*>* _rows = nullptr;
    std::shared_ptr<Chunk> _chunk;
    int64_t _row_idx = 0;
};
}