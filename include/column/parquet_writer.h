#pragma once

#include "rocks_wrapper.h"
#include <parquet/api/writer.h>
#include <parquet/arrow/writer.h>
#include <arrow/buffer.h>
#include <arrow/api.h>
#include "schema_factory.h"
#include <arrow/io/file.h>
#include "arrow_function.h"
#include <arrow/dataset/file_parquet.h>
#include <boost/filesystem.hpp>
#include "table_record.h"
#include <parquet/file_writer.h>
#include "parquet/arrow/schema.h"
#include "column_record.h"

namespace baikaldb {

enum ColumnCompactionType {
    MINOR   = 0,
    MAJOR   = 1,
    BASE    = 2,
    ROW2COL = 3,
};

struct ColumnFileMeta {
    std::string         file_name;
    int64_t             file_size = 0;
    int                 put_count = 0;
    int                 delete_count = 0;
    int                 merge_count = 0;
    int                 row_count = 0;
    std::string         start_key;
    std::string         end_key;
    std::set<int64_t>   userids;
};

struct ParquetWriteOptions {
   // 写数据相关参数
    int64_t userid_statis_batch_count = 10;                // userid 统计batch数，默认10
    int64_t write_batch_length = 10000;                // 上游写batch的行数，也是索引间隔，默认1w，严格按照该数值写入（最后一个batch除外）
    int64_t max_file_rows = 20000000;                   // 每个文件多少行，超过就新开一个文件，默认2000w
    std::shared_ptr<ColumnSchemaInfo> schema_info = nullptr;
    
    // parquet文件参数
    int64_t max_row_group_length = 1000000;                       // 默认一个rowgroup 100w行
    int64_t page_size = 1 * 1024 * 1024;                                  // parquet page大小 默认1MB
    // bool enable_dictionary;                             // 是否启用字典
    // arrow::Compression::type compression_type;          // 压缩类型
}; 

struct UseridStatis {
    int64_t userid = -1;
    int64_t batch_count = 0;
};

class SingleFileWriter {
public:
    ~SingleFileWriter() {
        close();
        boost::filesystem::path path(_file_name);
        if (boost::filesystem::exists(path)) {
            boost::filesystem::remove(path);
        }
    }
    arrow::Status create_new_file();
    arrow::Status write_batch(const std::shared_ptr<arrow::RecordBatch>& record_batch, const RecordBatchInfo& rb_info);
    arrow::Status write_rowgroup(const std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batchs, const std::vector<RecordBatchInfo>& rb_infos);
    bool is_finish();
    arrow::Status close();

    static std::shared_ptr<SingleFileWriter> create(const ParquetWriteOptions& options);
    ColumnFileMeta get_file_info() {
        ColumnFileMeta file_info;
        file_info.file_name    = _file_name;
        file_info.file_size    = _file_size;
        file_info.put_count    = _put_count;
        file_info.delete_count = _delete_count;
        file_info.merge_count  = _merge_count;
        file_info.row_count    = _row_count;
        file_info.start_key    = _start_key;
        file_info.end_key      = _end_key;
        for (const auto& statis : _userid_statis) {
            if (statis.batch_count >= _options.userid_statis_batch_count) {
                file_info.userids.insert(statis.userid);
            }
        }
        return file_info;
    }
private:
    SingleFileWriter() {}
    explicit SingleFileWriter(const ParquetWriteOptions& options) : _options(options) {
        _key_value_metadata = std::make_shared<arrow::KeyValueMetadata>();
    }
    
    TimeCost            _cost;
    ParquetWriteOptions _options;
    std::string         _file_name;
    int                 _put_count = 0;
    int                 _delete_count = 0;
    int                 _merge_count = 0;
    int                 _row_count = 0;
    int                 _rg_idx = 0;
    int64_t             _file_size = 0;
    std::string         _start_key;
    std::string         _end_key;
    std::vector<UseridStatis> _userid_statis;
    std::vector<std::shared_ptr<arrow::RecordBatch>> _stash_record_batchs;
    std::vector<RecordBatchInfo> _stash_rb_infos;
    int64_t             _stash_row_cnt = 0;

    std::map<std::string, std::pair<int, int64_t>> _sparse_index_map;
    std::shared_ptr<arrow::KeyValueMetadata>     _key_value_metadata;
    std::unique_ptr<parquet::arrow::FileWriter>  _file_writer;
    DISALLOW_COPY_AND_ASSIGN(SingleFileWriter);
};

class ParquetWriter {
public:
    explicit ParquetWriter(const ParquetWriteOptions& options) : _options(options) {}

    arrow::Status init();

    arrow::Status write_batch(const std::shared_ptr<arrow::RecordBatch>& record_batch, const RecordBatchInfo& rb_info);

    std::vector<ColumnFileMeta> get_file_infos() {
        std::vector<ColumnFileMeta> file_infos;
        file_infos.reserve(_finsh_file_writers.size());
        for (const auto& writer : _finsh_file_writers) {
            auto file_info = writer->get_file_info();
            if (file_info.row_count <= 0) {
                // 跳过空文件
                continue;
            }
            file_infos.emplace_back(file_info);
        }
        return file_infos;
    }

    arrow::Status finish();

    arrow::Status execute(std::unique_ptr<arrow::RecordBatchReader> input, ColumnCompactionType type, const std::function<bool ()>& cancel) {
        arrow::Status s;
        PARQUET_CATCH_NOT_OK(s = execute_do(std::move(input), type, cancel));
        return s;
    }


    int64_t row_count() const {
        return _total_row_count;
    }

private:
    arrow::Status execute_do(std::unique_ptr<arrow::RecordBatchReader> input, ColumnCompactionType type, const std::function<bool ()>& cancel);
    int write_file_info();
private:
    ParquetWriteOptions                                 _options;
    bool                                                _init = false;
    std::vector<std::shared_ptr<SingleFileWriter>>      _finsh_file_writers;
    std::shared_ptr<SingleFileWriter>                   _cur_file_writer;
    int64_t                                             _total_row_count = 0;
};

} // namespace baikaldb