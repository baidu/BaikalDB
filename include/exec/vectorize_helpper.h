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
#include "fetcher_store.h"
#include "expr_value.h"
#include <arrow/type.h>
#include <arrow/acero/options.h>
#include <arrow/compute/cast.h>
#include <arrow/stl_iterator.h>
namespace baikaldb {
class VectorizeHelpper {
public:
    VectorizeHelpper() {};
    ~VectorizeHelpper() {};

    static std::shared_ptr<arrow::Field> construct_arrow_field(
            const std::string& name, 
            pb::PrimitiveType type,
            std::shared_ptr<const arrow::KeyValueMetadata> metadata = nullptr) {
        auto pb_type = primitive_to_proto_type(type);
        if (pb_type == -1) {
            DB_WARNING("Fail to get pb_type, %d", type);
            return nullptr;
        }
        std::shared_ptr<arrow::Field> field;
        switch (pb_type) {
        case FieldDescriptorProto::TYPE_BOOL: // pb::NULL_TYPE, pb::BOOL
            field = std::make_shared<arrow::Field>(name, arrow::boolean(), true, metadata);
            break;
        case FieldDescriptorProto::TYPE_SINT32:   // pb::INT8, pb::INT16, pb::INT32
        case FieldDescriptorProto::TYPE_SFIXED32: // pb::TIME
            field = std::make_shared<arrow::Field>(name, arrow::int32(), true, metadata);
            break;
        case FieldDescriptorProto::TYPE_SINT64: // pb::INT64
            field = std::make_shared<arrow::Field>(name, arrow::int64(), true, metadata);
            break;
        case FieldDescriptorProto::TYPE_UINT32:  // pb::UINT8, pb::UINT16, pb::UINT32
        case FieldDescriptorProto::TYPE_FIXED32: // pb::TIMESTAMP, pb::DATE
            field = std::make_shared<arrow::Field>(name, arrow::uint32(), true, metadata);
            break;
        case FieldDescriptorProto::TYPE_UINT64:  // pb::UINT64
        case FieldDescriptorProto::TYPE_FIXED64: // pb::DATETIME
            field = std::make_shared<arrow::Field>(name, arrow::uint64(), true, metadata);
            break;
        case FieldDescriptorProto::TYPE_FLOAT: // pb::FLOAT
            field = std::make_shared<arrow::Field>(name, arrow::float32(), true, metadata);
            break;
        case FieldDescriptorProto::TYPE_DOUBLE: // pb::DOUBLE
            field = std::make_shared<arrow::Field>(name, arrow::float64(), true, metadata);
            break;
        case FieldDescriptorProto::TYPE_BYTES: // pb::STRING, pb::HLL, pb::BITMAP, pb::TDIGEST
            field = std::make_shared<arrow::Field>(name, arrow::large_binary(), true, metadata);
            break;
        default:
            DB_FATAL("unkown mysql type: %d", pb_type);
            return nullptr;
        }
        return field;
    }

    static std::shared_ptr<arrow::Schema> get_arrow_schema(std::set<ColumnInfo>& columns) {
        std::vector<std::shared_ptr<arrow::Field>> fields;
        for (const auto& column : columns) {
            std::string name = std::to_string(column.tuple_id) + "_" + std::to_string(column.slot_id);
            std::shared_ptr<arrow::Field> field = construct_arrow_field(name, column.pb_type);
            if (field == nullptr) {
                DB_WARNING("Fail to construct arrow field, name: %s", name.c_str());
                return nullptr;
            }
            fields.emplace_back(field);
        }
        return std::make_shared<arrow::Schema>(fields);
    }

    static ExprValue get_vectorized_value(const arrow::ChunkedArray* chunked_array, int row_idx, int32_t float_precision_len = -1) {
        ExprValue ret;
        ret.type = pb::NULL_TYPE;
        ret.float_precision_len = float_precision_len;
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

    static std::shared_ptr<arrow::Table> build_empty_table(
            std::vector<pb::TupleDescriptor>* tuple_decs, const std::vector<int32_t>& tuple_ids) {
        if (tuple_decs == nullptr) {
            return nullptr;
        }
        std::vector<std::shared_ptr<arrow::Field>> fields;
        for (auto tuple_id : tuple_ids) {
            const auto& tuple_dec = (*tuple_decs)[tuple_id];
            for (const auto& slot : tuple_dec.slots()) {
                std::string name = std::to_string(tuple_id) + "_" + std::to_string(slot.slot_id());
                std::shared_ptr<arrow::Field> field = construct_arrow_field(name, slot.slot_type());
                if (field == nullptr) {
                    DB_WARNING("Fail to construct arrow field, name: %s", name.c_str());
                    return nullptr;
                }
                fields.emplace_back(field);
            }
        }
        auto schema = std::make_shared<arrow::Schema>(fields);
        auto empty_table = arrow::Table::MakeEmpty(schema);
        if (empty_table.ok()) {
            return *empty_table;
        }
        return nullptr;
    }

    static bool has_new_column(const std::shared_ptr<arrow::Schema> schema, 
                               const std::shared_ptr<arrow::RecordBatch> old_record_batch) {
        if (schema == nullptr || old_record_batch == nullptr) {
            return false;
        }
        for (const auto& f : schema->fields()) {
            if (f == nullptr) {
                return false;
            }
            std::shared_ptr<arrow::Array> array = old_record_batch->GetColumnByName(f->name());
            if (array == nullptr) {
                return true;
            }
        }
        return false;
    }

    // schema: db构建的
    // in: store 返回的recordbatch
    static int change_arrow_record_batch_schema(std::shared_ptr<arrow::Schema> schema, 
            std::shared_ptr<arrow::RecordBatch> in, std::shared_ptr<arrow::RecordBatch>* out,
            bool need_cast = false) {
        std::vector<std::shared_ptr<arrow::Array>> columns;
        for (auto& f : schema->fields()) {
            std::shared_ptr<arrow::Array> array = in->GetColumnByName(f->name());
            if (array == nullptr) {
                DB_DEBUG("arrow schema not match, db schema: %s:  store schema: %s", 
                        schema->ToString().c_str(),
                        in->schema()->ToString().c_str());
                // 如multi count生成的set列, 构建一列null, 避免schema不一致失败
                std::shared_ptr<arrow::Array> default_array;
                if (f->metadata() != nullptr) {
                    auto get_res = f->metadata()->Get("default_value");
                    if (get_res.status().ok()) {
                        const std::string& default_value = *get_res;
                        default_array = make_array_from_str(f->type(), default_value, in->num_rows());
                        if (default_array == nullptr) {
                            DB_WARNING("Fail to make_array_from_str");
                            return -1;
                        }
                    }
                }
                if (default_array == nullptr) {
                    auto null_array = arrow::MakeArrayOfNull(f->type(), in->num_rows());
                    if (!null_array.ok()) {
                        DB_FATAL("arrow make null array fail, %s", null_array.status().ToString().c_str());
                        return -1;
                    }
                    default_array = *null_array;
                }
                array = default_array;
                columns.emplace_back(array);
            } else {
                if (need_cast && array->type()->id() != f->type()->id()) {
                    // 整数大类型转小类型，比如int32转int8，需要避免溢出返回错误
                    ::arrow::compute::CastOptions cast_options;
                    cast_options.allow_int_overflow = true;
                    auto cast_array_ret = ::arrow::compute::Cast(*array, f->type(), cast_options);
                    if (!cast_array_ret.ok()) {
                        DB_WARNING("cast array fail, %s", cast_array_ret.status().ToString().c_str());
                        return -1;
                    }
                    auto cast_array = *cast_array_ret;
                    columns.emplace_back(cast_array);
                } else {
                    columns.emplace_back(array);
                }
            }
        }
        *out = arrow::RecordBatch::Make(schema, in->num_rows(), columns);
        return 0;
    }

    static int change_arrow_record_batch_schema(const std::unordered_map<std::string, std::string>& column_name_map, 
                                                std::shared_ptr<arrow::Schema> schema, 
                                                std::shared_ptr<arrow::RecordBatch> in, 
                                                std::shared_ptr<arrow::RecordBatch>* out) {
        std::vector<std::shared_ptr<arrow::Array>> columns;
        for (const auto& f : schema->fields()) {
            std::shared_ptr<arrow::Array> array;
            if (column_name_map.find(f->name()) != column_name_map.end()) {
                array = in->GetColumnByName(column_name_map.at(f->name()));
            }
            if (array == nullptr) {
                // 兼容加列场景db/store心跳不一致
                auto null_array = arrow::MakeArrayOfNull(f->type(), in->num_rows());
                if (!null_array.ok()) {
                    DB_FATAL("arrow make null array fail, %s", null_array.status().ToString().c_str());
                    return -1;
                }
                array = *null_array;
                columns.emplace_back(array);
            } else {
                // 新schema的列类型和旧schema的列类型可能需要转换，比如列类型发生变更
                if (array->type()->id() != f->type()->id()) {
                    // 整数大类型转小类型，比如int32转int8，需要避免溢出返回错误
                    ::arrow::compute::CastOptions cast_options;
                    cast_options.allow_int_overflow = true;
                    auto cast_array_ret = ::arrow::compute::Cast(*array, f->type(), cast_options);
                    if (!cast_array_ret.ok()) {
                        DB_WARNING("cast array fail, %s", cast_array_ret.status().ToString().c_str());
                        return -1;
                    }
                    auto cast_array = *cast_array_ret;
                    columns.emplace_back(cast_array);
                } else {
                    columns.emplace_back(array);
                }
            }
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
        if (!filter_data.ok()) {
            // TODO 类似 where a这种会filter会报错: Filter argment不是boolean类型, 加上is true
            DB_FATAL("arrow filter fail, %s", filter_data.status().ToString().c_str());
            return -1;
        }
        *out = filter_data->record_batch();
        return 0;
    }

    static int serialize_record_batch(
                        std::shared_ptr<arrow::RecordBatch>& concatenate_record_batch,
                        std::shared_ptr<arrow::Buffer>& schema_buffer,
                        std::shared_ptr<arrow::Buffer>& data_buffer,
                        arrow::Compression::type compression_type = arrow::Compression::UNCOMPRESSED) {
        arrow::Result<std::shared_ptr<arrow::Buffer>> schema_ret =
                arrow::ipc::SerializeSchema(*(concatenate_record_batch->schema()), arrow::default_memory_pool());
        if (!schema_ret.ok()) {
            DB_FATAL("Fail to SerializeSchema");
            return -1;
        }
        schema_buffer = *schema_ret;
        arrow::ipc::IpcWriteOptions options;
        if (compression_type != arrow::Compression::UNCOMPRESSED &&
                compression_type != arrow::Compression::ZSTD &&
                compression_type != arrow::Compression::LZ4_FRAME) {
            DB_FATAL("compression type[%d] is not supported", compression_type);
            return -1;
        }
        auto codec = arrow::util::Codec::Create(compression_type);
        if (!codec.ok()) {
            DB_FATAL("Fail to Create Codec with compression type[%d]", compression_type);
            return -1;
        }
        options.codec = std::move(*codec);
        options.use_threads = false; //默认是true, 使用global executors并发压缩, 开启会卡查询
        arrow::Result<std::shared_ptr<arrow::Buffer>> data_ret = 
                arrow::ipc::SerializeRecordBatch(*concatenate_record_batch, options);
        if (!data_ret.ok()) {
            DB_FATAL("Fail to SerializeRecordBatch");
            return -1;
        }
        data_buffer = *data_ret;
        return 0;
    }
    
    static int concatenate_record_batches(
            std::shared_ptr<arrow::Schema> schema, 
            std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
            std::shared_ptr<arrow::RecordBatch>& out) {
        if (schema == nullptr) {
            DB_WARNING("schema is nullptr");
            return -1;
        }
        arrow::Result<std::shared_ptr<arrow::Table>> build_table = arrow::Table::FromRecordBatches(schema, batches);
        if (!build_table.ok()) {
            DB_FATAL("FromRecordBatches fail: %s", build_table.status().ToString().c_str());
            return -1;
        }
        std::shared_ptr<arrow::Table> table = *build_table;
        arrow::Result<std::shared_ptr<arrow::RecordBatch>> record_batch_result = table->CombineChunksToBatch();
        if (!record_batch_result.ok()) {
            DB_FATAL("CombineChunksToBatch fail: %s", record_batch_result.status().ToString().c_str());
            return -1;
        }
        out = *record_batch_result;
        return 0;
    }

    static std::shared_ptr<arrow::RecordBatch> construct_arrow_record_batch(
            std::shared_ptr<arrow::Schema> schema, const SmartRecord record, 
            std::unordered_set<int32_t>& need_not_cache_field_ids) {
        if (schema == nullptr) {
            DB_WARNING("schema is nullptr");
            return nullptr;
        }
        if (record == nullptr) {
            DB_WARNING("record is nullptr");
            return nullptr;
        }
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        fields.reserve(schema->fields().size());
        arrays.reserve(schema->fields().size());
        for (const auto& field : schema->fields()) {
            if (field == nullptr) {
                DB_WARNING("field is nullptr");
                return nullptr;
            }
            ExprValue field_value;
            int32_t field_id = -1;
            try {
                field_id = std::stoi(field->name());
            } catch (...) {
                DB_WARNING("Invalid field_name: %s", field->name().c_str());
                return nullptr;
            }
            const FieldDescriptor* field_desc = record->get_field_by_tag(field_id);
            if (field_desc != nullptr) {
                // field_desc为空对应删列场景，赋值NULL
                field_value = record->get_value(field_desc);
            }
            std::string field_value_str = field_value.get_string();
            if (field_value_str.size() > 1024) {
                // 如果某一行改字段超过1024Bytes，则将该字段从标量缓存中删除，涉及该行的过滤走后过滤方式；
                need_not_cache_field_ids.insert(field_id);
                continue;
            }
            auto array_ret = arrow::MakeArrayFromScalar(
                field_value.type == pb::NULL_TYPE ? 
                    arrow::LargeBinaryScalar(): 
                    arrow::LargeBinaryScalar(arrow::Buffer::FromString(field_value_str)), 
                1);
            if (!array_ret.ok()) {
                DB_WARNING("arrow make array from scalar fail, %s", array_ret.status().ToString().c_str());
                return nullptr;
            }
            std::shared_ptr<arrow::Array> array = *array_ret;
            ::arrow::compute::CastOptions cast_options;
            cast_options.allow_int_overflow = true;
            auto cast_array_ret = ::arrow::compute::Cast(*array, field->type(), cast_options);
            if (!cast_array_ret.ok()) {
                DB_WARNING("arrow cast array fail, %s", cast_array_ret.status().message().c_str());
                return nullptr;
            }
            fields.emplace_back(field);
            arrays.emplace_back(*cast_array_ret);
        }
        return arrow::RecordBatch::Make(arrow::schema(fields), 1, arrays);
    }
    
    static std::shared_ptr<arrow::Schema> construct_arrow_schema(
            const SmartTable table_info, const std::unordered_set<int32_t>& need_not_cache_fields) {
        if (table_info == nullptr) {
            DB_WARNING("table_info is nullptr");
            return nullptr;
        }
        std::vector<std::shared_ptr<arrow::Field>> fields;
        for (const auto& field : table_info->fields) {
            // 跳过__weight字段、向量字段以及不需要缓存的标量字段
            if (field.short_name == "__weight" || 
                    table_info->vector_fields.find(field.id) != table_info->vector_fields.end() ||
                    need_not_cache_fields.find(field.id) != need_not_cache_fields.end()) {
                continue;
            }
            const std::string& name = std::to_string(field.id);
            std::shared_ptr<arrow::KeyValueMetadata> arrow_field_metadata;
            if (field.default_expr_value.type != pb::NULL_TYPE) {
                arrow_field_metadata = std::make_shared<arrow::KeyValueMetadata>(
                    std::vector<std::string>{"default_value"}, 
                    std::vector<std::string>{field.default_expr_value.get_string()});
            }
            std::shared_ptr<arrow::Field> arrow_field = construct_arrow_field(name, field.type, arrow_field_metadata);
            if (arrow_field == nullptr) {
                DB_WARNING("Fail to construct arrow field, name: %s", name.c_str());
                return nullptr;
            }
            fields.emplace_back(arrow_field);
        }
        return arrow::schema(fields);
    }

    static std::shared_ptr<arrow::Array> make_array_from_str(
            std::shared_ptr<arrow::DataType> type, const std::string& str, const int32_t length) {
        auto array_ret = arrow::MakeArrayFromScalar(arrow::LargeBinaryScalar(arrow::Buffer::FromString(str)), length);
        if (!array_ret.ok()) {
            DB_WARNING("arrow make array from scalar fail, %s", array_ret.status().ToString().c_str());
            return nullptr;
        }
        std::shared_ptr<arrow::Array> array = *array_ret;
        ::arrow::compute::CastOptions cast_options;
        cast_options.allow_int_overflow = true;
        auto cast_array_ret = ::arrow::compute::Cast(*array, type, cast_options);
        if (!cast_array_ret.ok()) {
            DB_WARNING("arrow cast array fail, %s", cast_array_ret.status().message().c_str());
            return nullptr;
        }
        return *cast_array_ret;
    }
};

// [ARROW TODO, 复用FetcherStoreVectorizedReader]
class RowVectorizedReader : public arrow::RecordBatchReader {
public:
    std::shared_ptr<arrow::Schema> schema() const override { 
        return _schema;
    }

    // dualscannode用, 对于一些没有scannode的子查询, 构建一个一行的临时table作为SoureNode
    int init(RuntimeState* state) {
        _state = state;
        
        std::vector<std::shared_ptr<arrow::Field>> fields;
        fields.emplace_back(std::make_shared<arrow::Field>("scan_tmp", arrow::int64()));
        _schema = std::make_shared<arrow::Schema>(fields);

        std::vector<std::shared_ptr<arrow::Array>> array_list;
        arrow::Int64Builder builder;
        std::shared_ptr<arrow::Array> a;
        auto s = builder.Append(0);
        if (!s.ok()) {
            DB_FATAL("fail to make tmp scan table, append int64 fail, %s", s.ToString().c_str());
            return -1;
        } 
        s = builder.Finish(&a);
        if (!s.ok()) {
            DB_FATAL("fail to make tmp scan table, finish int64 array fail, %s", s.ToString().c_str());
            return -1;
        } 
        array_list.emplace_back(a);
        _tmp_one_row = arrow::RecordBatch::Make(_schema, 1, array_list);
        return 0;
    }

    // join用, 返回行存转换为列存
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
        _schema = _chunk->get_arrow_schema();
        return 0;
    }
    
    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override {
        // duanscannode
        if (_chunk == nullptr) {
            if (_has_send_tmp_row) {
                out->reset();
            } else {
                _has_send_tmp_row = true;
                *out = _tmp_one_row;
            }
            return arrow::Status::OK();
        }

        // join
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
    std::shared_ptr<arrow::Schema> _schema;

    // for join
    std::vector<const pb::TupleDescriptor*> _tuples;
    std::vector<MemRow*>* _rows = nullptr;
    std::shared_ptr<Chunk> _chunk;
    int64_t _row_idx = 0;

    // for dualscannode
    bool _has_send_tmp_row = false;
    std::shared_ptr<arrow::RecordBatch> _tmp_one_row;
};
}