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

struct ReaderInfo{
    std::shared_ptr<ReadContents> read_contents;
    std::shared_ptr<::arrow::RecordBatchReader> reader;
};

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
    std::unordered_map<std::string, ReaderInfo>* get_parquet_file2reader_map() {
        return &_parquet_file2reader_map;
    }
    std::unordered_map<std::string, std::vector<FieldInfo*>>* get_parquet_file_not_exist_column_map() {
        return &_parquet_file_not_exist_column_map;
    }

private:
    int process_index(RuntimeState* state);

    // 获取每个parquet文件符合条件数据的RecordBatchReader
    // 增删列场景下，每个parquet文件的schema可能不相同，需要记录查询需要但是parquet文件中不存在的列，对这些列需要填充null或默认值
    int get_qualified_record_batch_readers(
            std::unordered_map<std::string, ReaderInfo>& parquet_file2reader_map,
            std::unordered_map<std::string, std::vector<FieldInfo*>>& parquet_file_not_exist_column_map);

private:
    ParquetFileManager* _file_manager = nullptr;
    SchemaFactory* _factory = nullptr;
    SmartTable _table_info;
    int64_t _region_id = -1;
    std::vector<pb::PossibleIndex::Range> _key_ranges;
    std::vector<std::shared_ptr<ParquetFile>> _parquet_files;
    // key: field_id, value: field_info
    std::unordered_map<int32_t, FieldInfo*> _field_id2info_map;
    // key: parquet_file_name, value: parquet RecordBatchReader
    std::unordered_map<std::string, ReaderInfo> _parquet_file2reader_map;
    // key: parquet_file_name, value: 本次需要获取的但parquet文件中不存在的列
    std::unordered_map<std::string, std::vector<FieldInfo*>> _parquet_file_not_exist_column_map;
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
    std::unordered_map<std::string, ReaderInfo>* _parquet_file2reader_map = nullptr;
    std::unordered_map<std::string, std::vector<FieldInfo*>>* _parquet_file_not_exist_column_map = nullptr;

    std::shared_ptr<::arrow::Schema> _arrow_schema;
    // key: baikaldb column name, value: parquet column name
    std::unordered_map<std::string, std::string> _column_name_map;
    std::unordered_map<std::string, ReaderInfo>::iterator _reader_iter;

    std::shared_ptr<::arrow::RecordBatch> _record_batch;
    int64_t _row_idx_in_record_batch = 0;
    int64_t _processed_row_cnt = 0;
};

} // namespace baikaldb