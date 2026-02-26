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

#include "scan_node.h"
#include "file_manager.h"

namespace baikaldb {
class ParquetScanNode : public ScanNode {
public:
    ParquetScanNode() {}
    virtual ~ParquetScanNode() {}

    virtual int init(const pb::PlanNode& node) override;
    virtual int open(RuntimeState* state) override;
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos) override;
    virtual void close(RuntimeState* state) override;
    virtual int build_arrow_declaration(RuntimeState* state) override;
    virtual bool can_use_arrow_vector(RuntimeState* state) override {
        return true;
    }

    std::unordered_map<int32_t, FieldInfo*>* get_field_id2info_map() {
        return &_field_id2info_map;
    }
    std::vector<std::shared_ptr<::arrow::RecordBatchReader>>* get_parquet_file_readers() {
        return &_parquet_file_readers;
    }

private:
    int process_index(RuntimeState* state);

    // 获取每个parquet文件符合条件数据的RecordBatchReader
    // 增删列场景下，每个parquet文件的schema可能不相同，需要记录查询需要但是parquet文件中不存在的列，对这些列需要填充null或默认值
    int get_qualified_record_batch_readers(std::vector<std::shared_ptr<::arrow::RecordBatchReader>>& parquet_file_readers);
    void get_qualified_parquet_file_readers(std::vector<std::shared_ptr<ParquetFileReader>>& parquet_file_readers, 
            const std::vector<std::shared_ptr<ParquetFile>>& parquet_files, std::shared_ptr<arrow::Schema> schema);

private:
    ParquetFileManager* _file_manager = nullptr;
    SchemaFactory* _factory = nullptr;
    SmartTable _table_info  = nullptr;
    SmartIndex _pri_info    = nullptr;
    int64_t _region_id = -1;
    pb::PossibleIndex _possible_index;
    // key: field_id, value: field_info
    std::unordered_map<int32_t, FieldInfo*> _field_id2info_map;
    std::unordered_map<std::string, FieldInfo*> _field_name2info_map;
    std::vector<std::shared_ptr<::arrow::RecordBatchReader>> _parquet_file_readers;
};

class ParquetVectorizedReader : public arrow::RecordBatchReader {
public:
    ParquetVectorizedReader() {}
    virtual ~ParquetVectorizedReader() {}
    int init(RuntimeState* state, ParquetScanNode* parquet_scan_node);
    ::arrow::Status ReadNext(std::shared_ptr<::arrow::RecordBatch>* out) override;

    std::shared_ptr<::arrow::Schema> schema() const override {
        return _arrow_schema;
    }

private:
    RuntimeState* _state = nullptr;
    ParquetScanNode* _parquet_scan_node = nullptr;
    std::unordered_map<int32_t, FieldInfo*>* _field_id2info_map = nullptr;
    std::unordered_map<std::string, FieldInfo*>* _field_name2info_map = nullptr;
    std::vector<std::shared_ptr<::arrow::RecordBatchReader>>* _parquet_file_readers = nullptr;

    std::shared_ptr<::arrow::Schema> _arrow_schema;
    // key: baikaldb column name, value: parquet column name
    std::unordered_map<std::string, std::string> _column_name_map;
    // key: baikaldb column name, value: baikaldb column type
    std::unordered_map<std::string, pb::PrimitiveType> _column_type_map;
    std::vector<std::shared_ptr<::arrow::RecordBatchReader>>::iterator _reader_iter;

    std::shared_ptr<::arrow::RecordBatch> _record_batch;
    int64_t _row_idx_in_record_batch = 0;
    int64_t _processed_row_cnt = 0;
};

} // namespace baikaldb