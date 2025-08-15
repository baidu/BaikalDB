#include "mut_table_key.h"
#include "column_record.h"

namespace baikaldb {
void ColumnRecord::TEST_print_record_batch(const std::shared_ptr<arrow::RecordBatch>& record_batch) {
    if (record_batch == nullptr) {
        return;
    }
    std::ostringstream os;
    os << "{ column_name: {";
    for (int i = 0; i < record_batch->num_columns(); ++i) {
        if (i != record_batch->num_columns() - 1) {
            os << record_batch->schema()->field(i)->name() << ",";
        } else {
            os << record_batch->schema()->field(i)->name() << "} ";
        }
    }
    for (int64_t row_idx = 0; row_idx < record_batch->num_rows(); ++row_idx) {
        os << "row_" << row_idx << ": {";
        for (int col_idx = 0; col_idx < record_batch->num_columns(); ++col_idx) {
            auto column = record_batch->column(col_idx);
            ExprValue value = get_vectorized_value(column, row_idx);
            if (col_idx != record_batch->num_columns() - 1) {
                os << value.get_string() << ",";
            } else {
                os << value.get_string() << "} ";
            }
        }
    }
    os << " }";
    DB_WARNING("record_batch: %s", os.str().c_str());
}

bool ColumnRecord::TEST_record_batch_diff(const std::shared_ptr<arrow::RecordBatch>& rb1, const std::shared_ptr<arrow::RecordBatch>& rb2) {
    if (rb1 == nullptr || rb2 == nullptr) {
        return true;
    }
    if (!rb1->schema()->Equals(rb2->schema(), false)) {
        DB_WARNING("schema not equal, schema1: %s, schema2: %s",rb1->schema()->ToString().c_str(), rb2->schema()->ToString().c_str());
        return true;
    }

    for (int col_idx = 0; col_idx < rb1->num_columns(); ++col_idx) {
        auto c1 = rb1->column(col_idx);
        auto c2 = rb2->column(col_idx);
        for (int row_idx = 0; row_idx < rb1->num_rows(); ++row_idx) {
            ExprValue v1 = get_vectorized_value(c1, row_idx);
            ExprValue v2 = get_vectorized_value(c2, row_idx);
            if (v1.compare(v2) != 0) {
                DB_WARNING("column: %d, row: %d, value not equal, v1: %s, v2: %s", col_idx, row_idx, v1.get_string().c_str(), v2.get_string().c_str());
                return true;
            }
        }
    }
    return false;
}

std::string ColumnRecord::encode_row_key(std::shared_ptr<arrow::RecordBatch> record_batch, int row_index, const std::vector<FieldInfo>& fields, int64_t& userid) {
    MutTableKey key;
    userid = -1;
    for (int column_idx = 0; column_idx < record_batch->num_columns(); ++column_idx) {
        const FieldInfo& field = fields[column_idx];
        auto column = record_batch->column(column_idx);
        ExprValue value = get_vectorized_value(column, row_index);
        if (value.is_null()) {
            DB_COLUMN_FATAL("column: %d, row: %d, value is null", column_idx, row_index);
            return "";
        }
        if (column_idx == 0 && value.is_numberic() && field.lower_short_name == "userid") {
            userid = value.get_numberic<int64_t>();
        }
        key.append_value(value);
    }
    return key.data();
}

ExprValue ColumnRecord::get_vectorized_value(const std::shared_ptr<arrow::Array>& array, int row_idx) {
    ExprValue ret;
    ret.type = pb::NULL_TYPE;
    switch (array->type_id()) {
        case arrow::Type::BOOL: {
            arrow::BooleanArray* values_array = static_cast<arrow::BooleanArray*>(array.get());
            ret.type = pb::BOOL;
            ret._u.bool_val = values_array->Value(row_idx);
            break;
        }
        case arrow::Type::INT8: {
            arrow::Int8Array* values_array = static_cast<arrow::Int8Array*>(array.get());
            ret.type = pb::INT8;
            ret._u.int8_val = values_array->Value(row_idx);
            break;
        }
        case arrow::Type::UINT8: {
            arrow::UInt8Array* values_array = static_cast<arrow::UInt8Array*>(array.get());
            ret.type = pb::UINT8;
            ret._u.uint8_val = values_array->Value(row_idx);
            break;
        }
        case arrow::Type::INT16: {
            arrow::Int16Array* values_array = static_cast<arrow::Int16Array*>(array.get());
            ret.type = pb::INT16;
            ret._u.int16_val = values_array->Value(row_idx);
            break;
        }
        case arrow::Type::UINT16: {
            arrow::UInt16Array* values_array = static_cast<arrow::UInt16Array*>(array.get());
            ret.type = pb::UINT16;
            ret._u.uint16_val = values_array->Value(row_idx);
            break;
        }
        case arrow::Type::INT32: {
            arrow::Int32Array* values_array = static_cast<arrow::Int32Array*>(array.get());
            ret.type = pb::INT32;
            ret._u.int32_val = values_array->Value(row_idx);
            break;
        }
        case arrow::Type::UINT32: {
            arrow::UInt32Array* values_array = static_cast<arrow::UInt32Array*>(array.get());
            ret.type = pb::UINT32;
            ret._u.uint32_val = values_array->Value(row_idx);
            break;
        }
        case arrow::Type::INT64: {
            arrow::Int64Array* values_array = static_cast<arrow::Int64Array*>(array.get());
            ret.type = pb::INT64;
            ret._u.int64_val = values_array->Value(row_idx);
            break;
        }
        case arrow::Type::UINT64: {
            arrow::UInt64Array* values_array = static_cast<arrow::UInt64Array*>(array.get());
            ret.type = pb::UINT64;
            ret._u.uint64_val = values_array->Value(row_idx);
            break;
        }
        case arrow::Type::FLOAT: {
            arrow::FloatArray* values_array = static_cast<arrow::FloatArray*>(array.get());
            ret.type = pb::FLOAT;
            ret._u.float_val = values_array->Value(row_idx);
            break;
        }
        case arrow::Type::DOUBLE: {
            arrow::DoubleArray* values_array = static_cast<arrow::DoubleArray*>(array.get());
            ret.type = pb::DOUBLE;
            ret._u.double_val = values_array->Value(row_idx);
            break;
        }
        case arrow::Type::BINARY: {
            arrow::BinaryArray* values_array = static_cast<arrow::BinaryArray*>(array.get());
            ret.type = pb::STRING;
            ret.str_val = values_array->Value(row_idx);
            break;
        }
        case arrow::Type::LARGE_BINARY: {
            arrow::LargeBinaryArray* values_array = static_cast<arrow::LargeBinaryArray*>(array.get());
            ret.type = pb::STRING;
            ret.str_val = values_array->Value(row_idx);
            break;
        }
        case arrow::Type::STRING: {
            arrow::StringArray* values_array = static_cast<arrow::StringArray*>(array.get());
            ret.type = pb::STRING;
            ret.str_val = values_array->Value(row_idx);
            break;
        }
        case arrow::Type::LARGE_STRING: {
            arrow::LargeStringArray* values_array = static_cast<arrow::LargeStringArray*>(array.get());
            ret.type = pb::STRING;
            ret.str_val = values_array->Value(row_idx);
            break;
        }
        default:
            break;
    }
    return ret;
}

std::shared_ptr<arrow::Field> ColumnRecord::make_schema(const std::string& name, arrow::Type::type type) {
    switch (type) {
    case arrow::Type::type::BOOL:
        return std::make_shared<arrow::Field>(name, arrow::boolean());
    case arrow::Type::type::INT8:
        return std::make_shared<arrow::Field>(name, arrow::int8());
    case arrow::Type::type::UINT8:
        return std::make_shared<arrow::Field>(name, arrow::uint8());
    case arrow::Type::type::INT16:
        return std::make_shared<arrow::Field>(name, arrow::int16());
    case arrow::Type::type::UINT16:
        return std::make_shared<arrow::Field>(name, arrow::uint16());
    case arrow::Type::type::INT32:
        return std::make_shared<arrow::Field>(name, arrow::int32());
    case arrow::Type::type::UINT32:
        return std::make_shared<arrow::Field>(name, arrow::uint32());
    case arrow::Type::type::INT64:
        return std::make_shared<arrow::Field>(name, arrow::int64());
    case arrow::Type::type::UINT64:
        return std::make_shared<arrow::Field>(name, arrow::uint64());
    case arrow::Type::type::FLOAT:
        return std::make_shared<arrow::Field>(name, arrow::float32());
    case arrow::Type::type::DOUBLE:
        return std::make_shared<arrow::Field>(name, arrow::float64());
    case arrow::Type::type::LARGE_BINARY:
        return std::make_shared<arrow::Field>(name, arrow::large_binary());
    default:
        return nullptr;
    }
}

std::shared_ptr<arrow::Array> ColumnRecord::make_array_from_exprvalue(
        const pb::PrimitiveType type, const ExprValue& expr_value, const int length) {
    bool is_null = expr_value.is_null();
    switch (type) {
    case pb::BOOL: {
        arrow::BooleanScalar scalar;
        if (!is_null) {
            scalar = arrow::BooleanScalar(expr_value.get_numberic<bool>());
        }
        auto array_ret = arrow::MakeArrayFromScalar(scalar, length);
        if (!array_ret.ok()) {
            DB_WARNING("arrow make array from scalar fail, %s", array_ret.status().ToString().c_str());
            return nullptr;
        } else {
            return *array_ret;
        }
        break;
    }
    case pb::INT8: {
        arrow::Int8Scalar scalar;
        if (!is_null) {
            scalar = arrow::Int8Scalar(expr_value.get_numberic<int8_t>());
        }
        auto array_ret = arrow::MakeArrayFromScalar(scalar, length);
        if (!array_ret.ok()) {
            DB_WARNING("arrow make array from scalar fail, %s", array_ret.status().ToString().c_str());
            return nullptr;
        } else {
            return *array_ret;
        }
        break;
    }
    case pb::UINT8: {
        arrow::UInt8Scalar scalar;
        if (!is_null) {
            scalar = arrow::UInt8Scalar(expr_value.get_numberic<uint8_t>());
        }
        auto array_ret = arrow::MakeArrayFromScalar(scalar, length);
        if (!array_ret.ok()) {
            DB_WARNING("arrow make array from scalar fail, %s", array_ret.status().ToString().c_str());
            return nullptr;
        } else {
            return *array_ret;
        }
        break;
    }
    case pb::INT16: {
        arrow::Int16Scalar scalar;
        if (!is_null) {
            scalar = arrow::Int16Scalar(expr_value.get_numberic<int16_t>());
        }
        auto array_ret = arrow::MakeArrayFromScalar(scalar, length);
        if (!array_ret.ok()) {
            DB_WARNING("arrow make array from scalar fail, %s", array_ret.status().ToString().c_str());
            return nullptr;
        } else {
            return *array_ret;
        }
        break;
    }
    case pb::UINT16: {
        arrow::UInt16Scalar scalar;
        if (!is_null) {
            scalar = arrow::UInt16Scalar(expr_value.get_numberic<uint16_t>());
        }
        auto array_ret = arrow::MakeArrayFromScalar(scalar, length);
        if (!array_ret.ok()) {
            DB_WARNING("arrow make array from scalar fail, %s", array_ret.status().ToString().c_str());
            return nullptr;
        } else {
            return *array_ret;
        }
        break;
    }
    case pb::INT32:
    case pb::TIME: {
        arrow::Int32Scalar scalar;
        if (!is_null) {
            scalar = arrow::Int32Scalar(expr_value.get_numberic<int32_t>());
        }
        auto array_ret = arrow::MakeArrayFromScalar(scalar, length);
        if (!array_ret.ok()) {
            DB_WARNING("arrow make array from scalar fail, %s", array_ret.status().ToString().c_str());
            return nullptr;
        } else {
            return *array_ret;
        }
        break;
    }
    case pb::UINT32:
    case pb::DATE:
    case pb::TIMESTAMP: {
        arrow::UInt32Scalar scalar;
        if (!is_null) {
            scalar = arrow::UInt32Scalar(expr_value.get_numberic<uint32_t>());
        }
        auto array_ret = arrow::MakeArrayFromScalar(scalar, length);
        if (!array_ret.ok()) {
            DB_WARNING("arrow make array from scalar fail, %s", array_ret.status().ToString().c_str());
            return nullptr;
        } else {
            return *array_ret;
        }
        break;
    }
    case pb::INT64: {
        arrow::Int64Scalar scalar;
        if (!is_null) {
            scalar = arrow::Int64Scalar(expr_value.get_numberic<int64_t>());
        }
        auto array_ret = arrow::MakeArrayFromScalar(scalar, length);
        if (!array_ret.ok()) {
            DB_WARNING("arrow make array from scalar fail, %s", array_ret.status().ToString().c_str());
            return nullptr;
        } else {
            return *array_ret;
        }
        break;
    }
    case pb::UINT64:
    case pb::DATETIME: {
        arrow::UInt64Scalar scalar;
        if (!is_null) {
            scalar = arrow::UInt64Scalar(expr_value.get_numberic<uint8_t>());
        }
        auto array_ret = arrow::MakeArrayFromScalar(scalar, length);
        if (!array_ret.ok()) {
            DB_WARNING("arrow make array from scalar fail, %s", array_ret.status().ToString().c_str());
            return nullptr;
        } else {
            return *array_ret;
        }
        break;
    }
    case pb::FLOAT: {
        arrow::FloatScalar scalar;
        if (!is_null) {
            scalar = arrow::FloatScalar(expr_value.get_numberic<float>());
        }
        auto array_ret = arrow::MakeArrayFromScalar(scalar, length);
        if (!array_ret.ok()) {
            DB_WARNING("arrow make array from scalar fail, %s", array_ret.status().ToString().c_str());
            return nullptr;
        } else {
            return *array_ret;
        }
        break;
    }
    case pb::DOUBLE: {
        arrow::DoubleScalar scalar;
        if (!is_null) {
            scalar = arrow::DoubleScalar(expr_value.get_numberic<double>());
        }
        auto array_ret = arrow::MakeArrayFromScalar(scalar, length);
        if (!array_ret.ok()) {
            DB_WARNING("arrow make array from scalar fail, %s", array_ret.status().ToString().c_str());
            return nullptr;
        } else {
            return *array_ret;
        }
        break;
    }
    case pb::STRING:
    case pb::HLL:
    case pb::BITMAP:
    case pb::TDIGEST: {
        auto array_ret = arrow::MakeArrayFromScalar(
                is_null ? arrow::LargeBinaryScalar(): 
                        arrow::LargeBinaryScalar(arrow::Buffer::FromString(expr_value.get_string())), 
                length);
        if (!array_ret.ok()) {
            DB_WARNING("arrow make array from scalar fail, %s", array_ret.status().ToString().c_str());
            return nullptr;
        } else {
            return *array_ret;
        }
        break;
    }
    default:
        DB_WARNING("Invalid type: %d", type);
        return nullptr;
    }
    return nullptr;
}

int ColumnRecord::init() {
    _field_num = _schema->num_fields();
    _builders.reserve(_field_num);
    for (int i = 0; i < _field_num; ++i) {
        auto field = _schema->field(i);
        auto arrow_type = field->type()->id();
        switch (arrow_type) {
        case arrow::Type::type::BOOL:
            _builders.emplace_back(std::make_shared<arrow::BooleanBuilder>());
            break;
        case arrow::Type::type::INT8:
            _builders.emplace_back(std::make_shared<arrow::Int8Builder>());
            break;
        case arrow::Type::type::UINT8:
            _builders.emplace_back(std::make_shared<arrow::UInt8Builder>());
            break;
        case arrow::Type::type::INT16:
            _builders.emplace_back(std::make_shared<arrow::Int16Builder>());
            break;
        case arrow::Type::type::UINT16:
            _builders.emplace_back(std::make_shared<arrow::UInt16Builder>());
            break;
        case arrow::Type::type::INT32:
            _builders.emplace_back(std::make_shared<arrow::Int32Builder>());
            break;
        case arrow::Type::type::UINT32:
            _builders.emplace_back(std::make_shared<arrow::UInt32Builder>());
            break;
        case arrow::Type::type::INT64:
            _builders.emplace_back(std::make_shared<arrow::Int64Builder>());
            break;
        case arrow::Type::type::UINT64:
            _builders.emplace_back(std::make_shared<arrow::UInt64Builder>());
            break;
        case arrow::Type::type::FLOAT:
            _builders.emplace_back(std::make_shared<arrow::FloatBuilder>());
            break;
        case arrow::Type::type::DOUBLE:
            _builders.emplace_back(std::make_shared<arrow::DoubleBuilder>());
            break;
        case arrow::Type::type::LARGE_BINARY:
            _builders.emplace_back(std::make_shared<arrow::LargeBinaryBuilder>());
            break;
        default:
            DB_FATAL("Unsupported arrow type, %d", arrow_type);
            return -1;
        }
    }
    return 0;
}

int ColumnRecord::append_value(int field_pos, const ExprValue& value) {
    if (field_pos < 0 || field_pos > _field_num) {
        DB_COLUMN_FATAL("invalid field pos, %d, field num:%d", field_pos, _field_num);
        return -1;
    } 
    auto field = _schema->field(field_pos);
    auto arrow_type = field->type()->id();
    if (value.type == pb::NULL_TYPE) {
        _builders[field_pos]->AppendNull();
        return 0;
    }
    // 影响性能后续去掉 COLUMNTODO
    // if (primitive_to_arrow_type(value.type) != field->type()->id()) {
    //     DB_COLUMN_FATAL("value type not match, name: %s, type: %s != %s", 
    //         field->name().c_str(), pb::PrimitiveType_Name(value.type).c_str(), field->type()->ToString().c_str());
    //     return -1;
    // }
    arrow::Status s;
    switch (arrow_type) {
    case arrow::Type::type::BOOL:
        s = static_cast<arrow::BooleanBuilder*>(_builders[field_pos].get())->Append(value.get_numberic<bool>());
        break;
    case arrow::Type::type::INT8:
        s = static_cast<arrow::Int8Builder*>(_builders[field_pos].get())->Append(value._u.int8_val);
        break;
    case arrow::Type::type::UINT8:
        s = static_cast<arrow::UInt8Builder*>(_builders[field_pos].get())->Append(value._u.uint8_val);
        break;
    case arrow::Type::type::INT16:
        s = static_cast<arrow::Int16Builder*>(_builders[field_pos].get())->Append(value._u.int16_val);
        break;
    case arrow::Type::type::UINT16:
        s = static_cast<arrow::UInt16Builder*>(_builders[field_pos].get())->Append(value._u.uint16_val);
        break;
    case arrow::Type::type::INT32:
        s = static_cast<arrow::Int32Builder*>(_builders[field_pos].get())->Append(value._u.int32_val);
        break;
    case arrow::Type::type::UINT32:
        s = static_cast<arrow::UInt32Builder*>(_builders[field_pos].get())->Append(value._u.uint32_val);
        break;
    case arrow::Type::type::INT64:
        s = static_cast<arrow::Int64Builder*>(_builders[field_pos].get())->Append(value._u.int64_val);
        break;
    case arrow::Type::type::UINT64:
        s = static_cast<arrow::UInt64Builder*>(_builders[field_pos].get())->Append(value._u.uint64_val);
        break;
    case arrow::Type::type::FLOAT:
        s = static_cast<arrow::FloatBuilder*>(_builders[field_pos].get())->Append(value._u.float_val);
        break;
    case arrow::Type::type::DOUBLE:
        s = static_cast<arrow::DoubleBuilder*>(_builders[field_pos].get())->Append(value._u.double_val);
        break;
    case arrow::Type::type::LARGE_BINARY:
        s = static_cast<arrow::LargeBinaryBuilder*>(_builders[field_pos].get())->Append(value.str_val);
        break;
    default:
        DB_FATAL("unkown arrow type: %d", arrow_type);
        return -1;
    }
    if (!s.ok()) {
        DB_FATAL("array append error: %s", s.ToString().c_str());
        return -1;
    }
    return 0;
}

int ColumnRecord::finish_and_reset_one_column(std::vector<std::shared_ptr<arrow::Array>>& arrays, int& idx) {
    if (idx < 0 || idx > _field_num) {
        return -1;
    } 
    auto field = _schema->field(idx);
    auto arrow_type = field->type()->id();
    switch (arrow_type) {
        case arrow::Type::type::BOOL: {
            auto status = static_cast<arrow::BooleanBuilder*>(_builders[idx].get())->Finish();
            static_cast<arrow::BooleanBuilder*>(_builders[idx].get())->Reset();
            if (!status.ok()) {
                DB_FATAL("array finish fail");
                return -1;
            }
            arrays.emplace_back(*status);
            break;
        }
        case arrow::Type::type::INT8: {
            auto status = static_cast<arrow::Int8Builder*>(_builders[idx].get())->Finish();
            static_cast<arrow::Int8Builder*>(_builders[idx].get())->Reset();
            if (!status.ok()) {
                DB_FATAL("array finish fail");
                return -1;
            }
            arrays.emplace_back(*status);
            break;
        }
        case arrow::Type::type::UINT8: {
            auto status = static_cast<arrow::UInt8Builder*>(_builders[idx].get())->Finish();
            static_cast<arrow::UInt8Builder*>(_builders[idx].get())->Reset();
            if (!status.ok()) {
                DB_FATAL("array finish fail");
                return -1;
            }
            arrays.emplace_back(*status);
            break;
        }
        case arrow::Type::type::INT16: {
            auto status = static_cast<arrow::Int16Builder*>(_builders[idx].get())->Finish();
            static_cast<arrow::Int16Builder*>(_builders[idx].get())->Reset();
            if (!status.ok()) {
                DB_FATAL("array finish fail");
                return -1;
            }
            arrays.emplace_back(*status);
            break;
        }
        case arrow::Type::type::UINT16: {
            auto status = static_cast<arrow::UInt16Builder*>(_builders[idx].get())->Finish();
            static_cast<arrow::UInt16Builder*>(_builders[idx].get())->Reset();
            if (!status.ok()) {
                DB_FATAL("array finish fail");
                return -1;
            }
            arrays.emplace_back(*status);
            break;
        }
        case arrow::Type::type::INT32: { 
            auto status = static_cast<arrow::Int32Builder*>(_builders[idx].get())->Finish();
            static_cast<arrow::Int32Builder*>(_builders[idx].get())->Reset();
            if (!status.ok()) {
                DB_FATAL("array finish fail");
                return -1;
            }
            arrays.emplace_back(*status);
            break;
        }
        case arrow::Type::type::UINT32: {
            auto status = static_cast<arrow::UInt32Builder*>(_builders[idx].get())->Finish();
            static_cast<arrow::UInt32Builder*>(_builders[idx].get())->Reset();
            if (!status.ok()) {
                DB_FATAL("array finish fail");
                return -1;
            }
            arrays.emplace_back(*status);
            break;
        }
        case arrow::Type::type::INT64: { 
            auto status = static_cast<arrow::Int64Builder*>(_builders[idx].get())->Finish();
            static_cast<arrow::Int64Builder*>(_builders[idx].get())->Reset();
            if (!status.ok()) {
                DB_FATAL("array finish fail");
                return -1;
            }
            arrays.emplace_back(*status);
            break;
        }
        case arrow::Type::type::UINT64: {
            auto status = static_cast<arrow::UInt64Builder*>(_builders[idx].get())->Finish();
            static_cast<arrow::UInt64Builder*>(_builders[idx].get())->Reset();
            if (!status.ok()) {
                DB_FATAL("array finish fail");
                return -1;
            }
            arrays.emplace_back(*status);
            break;
        }
        case arrow::Type::type::FLOAT: {
            auto status = static_cast<arrow::FloatBuilder*>(_builders[idx].get())->Finish();
            static_cast<arrow::FloatBuilder*>(_builders[idx].get())->Reset();
            if (!status.ok()) {
                DB_FATAL("array finish fail");
                return -1;
            }
            arrays.emplace_back(*status);
            break;
        }
        case arrow::Type::type::DOUBLE: { // pb::DOUBLE
            auto status = static_cast<arrow::DoubleBuilder*>(_builders[idx].get())->Finish();
            static_cast<arrow::DoubleBuilder*>(_builders[idx].get())->Reset();
            if (!status.ok()) {
                DB_FATAL("array finish fail");
                return -1;
            }
            arrays.emplace_back(*status);
            break;
        }
        case arrow::Type::type::LARGE_BINARY: {
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
            DB_FATAL("unkown arrow type: %d", arrow_type);
            return -1;
    }
    return 0;
}

int ColumnRecord::finish_and_make_record_batch(std::shared_ptr<arrow::RecordBatch>* out) {
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(_field_num);
    for (int idx = 0; idx < _field_num; ++idx) {
        int ret = finish_and_reset_one_column(arrays, idx);
        if (ret != 0) {
            DB_WARNING("finish_and_reset failed %d", idx);
            return ret;
        }
    }
    *out = arrow::RecordBatch::Make(_schema, _row_length, arrays);
    _row_length = 0;
    return 0;
}
} // namespace baikaldb