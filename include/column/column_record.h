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
#include <iostream>
#include <istream>
#include <streambuf>
#include <string>
#include <vector>
#include <arrow/type.h>
#include <arrow/api.h> 
#include "expr_value.h"
#include "schema_factory.h"

// 能否复用Chunk TODO
namespace baikaldb {
struct RecordBatchInfo {
    int                 put_count = 0;    
    int                 delete_count = 0;
    int                 merge_count = 0; 
    int                 row_count = 0;
    std::string         start_key;
    std::string         end_key;
    int64_t             first_userid = -1;
    int64_t             last_userid = -1;
};

enum ColumnKeyType {
    COLUMN_KEY_PUT    = 0,
    COLUMN_KEY_DELETE = 1,
    COLUMN_KEY_MERGE  = 2,
};

struct ColumnSchemaInfo {
    std::vector<FieldInfo> key_fields;
    std::vector<FieldInfo> value_fields;
    SmartIndex index_info = nullptr;
    SmartTable table_info = nullptr;
    int uniq_size = 0; // 主键或索引key的字段个数key_fields.size(); 在parquet中的idx为0~uniq_size-1
    int keytype_idx = -1; // key_fields.size() + value_fields.size();
    int raft_index_idx = -1; // keytype_idx + 1
    int batch_pos_idx = -1; // raft_index_idx + 1
    std::set<int> need_sum_idx;
    std::shared_ptr<arrow::Schema> schema = nullptr; // 包含表中所有字段 + __key_type__; 顺序为key + value + __key_type__; key为主键序，value为非主键字段按field_idx顺序
    std::shared_ptr<arrow::Schema> schema_with_order_info = nullptr;  // 比schema多包含__raft_index__, __batch_pos__放在__key_type__后面
};

const std::string KEY_TYPE_NAME = "__key_type__";
const std::string RAFT_INDEX_NAME = "__raft_index__";
const std::string BATCH_POS_NAME = "__batch_pos__";

class ColumnRecord {
public:
    ColumnRecord(const std::shared_ptr<arrow::Schema>& schema, int capacity) : _schema(schema), _capacity(capacity) {};
    virtual ~ColumnRecord() {};

    static void TEST_print_record_batch(const std::shared_ptr<arrow::RecordBatch>& record_batch);
    static bool TEST_record_batch_diff(const std::shared_ptr<arrow::RecordBatch>& rb1, const std::shared_ptr<arrow::RecordBatch>& rb2);
    static std::string encode_row_key(std::shared_ptr<arrow::RecordBatch> record_batch, 
        int row_index, const std::vector<FieldInfo>& fields, int64_t& userid);
    static std::string get_frist_row_key(std::shared_ptr<arrow::RecordBatch> record_batch, 
        const std::vector<FieldInfo>& fields, int64_t& userid) {
        return encode_row_key(record_batch, 0, fields, userid);
    }

    static std::string get_last_row_key(std::shared_ptr<arrow::RecordBatch> record_batch, 
        const std::vector<FieldInfo>& fields, int64_t& userid) {
        return encode_row_key(record_batch, record_batch->num_rows() - 1, fields, userid);
    }

    static std::shared_ptr<arrow::Field> make_schema(const std::string& name, arrow::Type::type type);
    static ExprValue get_vectorized_value(const std::shared_ptr<arrow::Array>& array, int row_idx);
    static std::shared_ptr<arrow::Array> make_array_from_exprvalue(
        const pb::PrimitiveType type, const ExprValue& expr_value, const int length);

    int init();

    int append_value(int field_pos, const ExprValue& value);

    void inc_row_length() {
        ++_row_length;
    }

    int append_row(const std::vector<ExprValue>& row) {
        if (row.size() < _field_num) {
            DB_COLUMN_FATAL("append_row failed, column num mismatch [%ld vs %d]", row.size(), _field_num);
            return -1;
        }
        int ret = 0;
        for (size_t i = 0; i < _field_num; ++i) {
            ret = append_value(i, row[i]);
            if (ret != 0) {
                return -1;
            }
        }
        ++_row_length;
        return 0;
    }

    int finish_and_reset_one_column(std::vector<std::shared_ptr<arrow::Array>>& arrays, int& idx);

    int finish_and_make_record_batch(std::shared_ptr<arrow::RecordBatch>* out);

    std::shared_ptr<arrow::Schema> get_arrow_schema() {
        return _schema;
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

private:
    int _field_num = 0;
    int _row_length = 0;
    int _capacity = 0;
    
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> _builders;
    std::shared_ptr<arrow::Schema> _schema;
};
} //namespace baikaldb