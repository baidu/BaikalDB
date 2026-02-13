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

#include "arrow_io_excutor.h"
#include "file_system.h"
#include "scan_node.h"

namespace baikaldb {

DECLARE_int32(inner_file_scan_concurrency);

class FileVectorizedReader;
class FileScanNode;

class FileScanner {
public:
    FileScanner(FileSystem* fs, const pb::PartitionFile& partition_file) : _fs(fs) {
        _partition_vals.assign(partition_file.partition_vals().begin(), partition_file.partition_vals().end());
        _file_path = partition_file.file_path();
    }
    virtual ~FileScanner() {}
    virtual int init(FileVectorizedReader* vectorized_reader, FileScanNode* scan_node);
    virtual int run() = 0;

    const std::vector<std::string>& get_partition_vals() {
        return _partition_vals;
    }
    FileVectorizedReader* get_vectorized_reader() {
        return _vectorized_reader;
    }
    FileScanNode* get_scan_node() {
        return _scan_node;
    }

protected:
    FileSystem* _fs = nullptr;
    std::vector<std::string> _partition_vals;
    std::string _file_path;
    FileVectorizedReader* _vectorized_reader = nullptr;
    FileScanNode* _scan_node = nullptr;
};

class CSVScanner : public FileScanner {
    class BlockImpl;
public:
    using FileScanner::FileScanner;
    virtual ~CSVScanner() {}
    int init(FileVectorizedReader* vectorized_reader, FileScanNode* scan_node) override;
    int run() override;

private:
    FileInfo _file_info;
};

class ParquetScanner : public FileScanner {
public:
    using FileScanner::FileScanner;
    virtual ~ParquetScanner() {}
    int init(FileVectorizedReader* vectorized_reader, FileScanNode* scan_node) override;
    int run() override;

private:
    int process_record_batch(std::shared_ptr<::arrow::RecordBatch>& record_batch);
    int get_file_reader(std::unique_ptr<::parquet::arrow::FileReader>& reader);

private:
    std::shared_ptr<::parquet::FileMetaData> _file_metadata;
    // 本次需要获取，且parquet文件中存在的列
    std::vector<int32_t> _exist_column_indices;
    // 本次需要获取，但parqeut文件中不存在的列
    std::vector<FieldInfo*> _not_exist_columns;
    // <baikaldb column name, parquet column name>
    std::unordered_map<std::string, std::string> _column_name_map;
    // key: baikaldb column name, value: baikaldb column type
    std::unordered_map<std::string, pb::PrimitiveType> _column_type_map;
    // <partition field id, partition field val>
    std::unordered_map<int32_t, std::string> _partition_id2val_map;
};

class FileVectorizedReader : public arrow::RecordBatchReader {
public:
    FileVectorizedReader() 
        : _file_concurrency(FLAGS_inner_file_scan_concurrency)
        , _file_concurrency_cond(-FLAGS_inner_file_scan_concurrency)
        , _record_batches(FLAGS_inner_file_scan_concurrency * 2) {}
    virtual ~FileVectorizedReader() {}
    int init(FileScanNode* scan_node, RuntimeState* state);
    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override;
    std::shared_ptr<arrow::Schema> schema() const override { 
        return _schema; 
    }
    void close() {
        _file_scan_bth.join();
    }

    BlockingQueue<std::shared_ptr<::arrow::RecordBatch>>& get_record_batches() {
        return _record_batches;
    }
    BthreadCond& get_file_concurrency_cond() {
        return _file_concurrency_cond;
    }
    std::atomic<bool>& get_is_succ() {
        return _is_succ;
    }
    std::atomic<bool>& get_eos() {
        return _eos;
    }

private:
    void run_file_scan_thread();

private:
    std::shared_ptr<arrow::Schema> _schema;
    FileScanNode* _scan_node = nullptr;
    RuntimeState* _state = nullptr;
    Bthread _file_scan_bth;
    BlockingQueue<std::shared_ptr<::arrow::RecordBatch>> _record_batches;
    // Block或RowGroup并发数
    int32_t _file_concurrency = -1;
    BthreadCond _file_concurrency_cond; 
    // limit功能实现
    std::shared_ptr<::arrow::RecordBatch> _record_batch;
    int64_t _row_idx_in_record_batch = 0;
    int64_t _processed_row_cnt = 0;
    // 读取状态
    std::atomic<bool> _is_succ = true;
    std::atomic<bool> _eos = false;
    // paralle模式使用
    bool _is_delay_fetch = false;
    std::shared_ptr<IndexCollectorCond> _index_cond;
};

class FileScanNode : public ScanNode {
public:
    FileScanNode() {
        _is_file_scan_node = true;
    }
    virtual ~FileScanNode() {
    }
    virtual int init(const pb::PlanNode& node) override;
    virtual int open(RuntimeState* state) override;
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos) override;
    virtual void close(RuntimeState* state) override;
    virtual void transfer_pb(int64_t region_id, pb::PlanNode* pb_node) override;
    virtual int build_arrow_declaration(RuntimeState* state) override;
    virtual bool can_use_arrow_vector(RuntimeState* state) override {
        return true;
    }
    const pb::FileInfo& get_pb_file_info() {
        return _pb_file_info;
    }
    const std::vector<int32_t>& get_field_id2slot() {
        return _field_id2slot;
    }
    const std::vector<FieldInfo*>& get_field_id2info() {
        return _field_id2info;
    }
    const std::vector<FieldInfo*>& get_partition_fields() {
        return _partition_fields;
    }
    const std::vector<FieldInfo*>& get_data_fields() {
        return _data_fields;
    }
    const std::vector<pb::PartitionFile>& get_files() {
        return _files;
    }
    void set_files(const std::vector<pb::PartitionFile>& files) {
        _files = files;
    }

private:
    pb::FileInfo _pb_file_info;
    std::vector<pb::PartitionFile> _files; // 需要访问的离线文件集合

    SmartTable _table_info;
    std::vector<int32_t> _field_id2slot; // <field_id, slot_id>
    std::vector<FieldInfo*> _field_id2info; // <field_id, field_info>

    std::vector<FieldInfo*> _partition_fields; // 分区字段
    std::vector<FieldInfo*> _data_fields; // 数据字段，去掉分区字段后按表定义顺序排列

    std::shared_ptr<FileVectorizedReader> _vectorized_reader;
    std::shared_ptr<BthreadArrowExecutor> _arrow_io_executor;
};

} // namespace baikaldb