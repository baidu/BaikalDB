#include "parquet_writer.h"
#include "proto/column.pb.h"
#include "sort_merge.h"
#include "column_statistics.h"
namespace baikaldb {
DECLARE_string(db_path);
DEFINE_int32(parquet_compression_type, 2, "parquet_compression_type(0:snappy 1:lz4 2:zstd)"); 
DEFINE_int32(parquet_rowgroup_min_length, 100000, "parquet_rowgroup_min_length(10w)"); 
arrow::Status ParquetWriter::init() {
    if (_init) {
        return arrow::Status::OK();
    }

    auto single_writer = SingleFileWriter::create(_options);
    if (single_writer == nullptr) {
        return arrow::Status::Invalid("Fail to create parquet writer");;
    }
    _cur_file_writer = single_writer;
    _init = true;
    return arrow::Status::OK();
}

arrow::Status ParquetWriter::write_batch(const std::shared_ptr<arrow::RecordBatch>& record_batch, const RecordBatchInfo& rb_info) {
    if (_cur_file_writer == nullptr) {
        return arrow::Status::Invalid("Fail to write parquet file");
    }

    arrow::Status status = _cur_file_writer->write_batch(record_batch, rb_info);
    if (!status.ok()) {
        return status;
    }

    if (_cur_file_writer->is_finish()) {
        status = _cur_file_writer->close();
        if (!status.ok()) {
            return status;
        }
        _finsh_file_writers.emplace_back(_cur_file_writer);
        auto single_writer = SingleFileWriter::create(_options);
        if (single_writer == nullptr) {
            return arrow::Status::Invalid("Fail to create parquet writer");;
        }
        _cur_file_writer = single_writer;
    }

    return arrow::Status::OK();
}

arrow::Status ParquetWriter::finish() {
    if (_cur_file_writer != nullptr) {
        arrow::Status status = _cur_file_writer->close();
        if (!status.ok()) {
            return status;
        }
        _finsh_file_writers.emplace_back(_cur_file_writer);
        _cur_file_writer == nullptr;
    }

    return arrow::Status::OK();
}

arrow::Status ParquetWriter::execute_do(std::unique_ptr<arrow::RecordBatchReader> input, ColumnCompactionType type, const std::function<bool ()>& cancel) {
    arrow::Status status = init();
    if (!status.ok()) {
        return status;
    }

    std::unique_ptr<arrow::RecordBatchReader> input_reader = std::move(input);

    while (true) {
        std::shared_ptr<arrow::RecordBatch> rb;        
        status = input_reader->ReadNext(&rb);
        if (!status.ok()) {
            return status;
        } else if (rb == nullptr || rb->num_rows() <= 0) {
            break;
        }

        RecordBatchInfo rb_info;
        SortMerge* r = dynamic_cast<SortMerge*>(input_reader.get());
        if (r != nullptr) {
            r->get_last_batch_count(rb_info.merge_count, rb_info.put_count, rb_info.delete_count);
        }

        _total_row_count += rb->num_rows();
        rb_info.row_count = rb->num_rows();
        rb_info.start_key = ColumnRecord::get_frist_row_key(rb, _options.schema_info->key_fields, rb_info.first_userid);
        rb_info.end_key = ColumnRecord::get_last_row_key(rb, _options.schema_info->key_fields, rb_info.last_userid);
        if (rb_info.start_key.empty() || rb_info.end_key.empty()) {
            return arrow::Status::Invalid("get row key failed");
        }

        status = write_batch(rb, rb_info);
        if (!status.ok()) {
            return status;
        }

        if (type == MINOR) {
            ColumnVars::get_instance()->minor_compaction_write_rows << rb_info.row_count;
        } else if (type == MAJOR) {
            ColumnVars::get_instance()->major_compaction_write_rows << rb_info.row_count;
        } else {
            ColumnVars::get_instance()->base_compaction_write_rows << rb_info.row_count;
        }

        if (cancel()) {
            return arrow::Status::Invalid("canceled");
        }
    }

    return finish();
}

std::shared_ptr<SingleFileWriter> SingleFileWriter::create(const ParquetWriteOptions& options) {
    std::shared_ptr<SingleFileWriter> single_writer(new SingleFileWriter(options));    
    auto s = single_writer->create_new_file();
    if (!s.ok()) {
        DB_COLUMN_FATAL("create new file failed error: %s", s.message().c_str());
        return nullptr;
    }
    return single_writer;
}

// COLUMNTODO catch异常
arrow::Status SingleFileWriter::create_new_file() {
    static std::atomic<int64_t> file_prefix = 0;
    _file_name = FLAGS_db_path + "_tmp/" + std::to_string(file_prefix.fetch_add(1)) + "_" + std::to_string(butil::gettimeofday_us()) + ".parquet";
    boost::filesystem::path path(_file_name);
    if (boost::filesystem::exists(path)) {
        DB_COLUMN_FATAL("file already exists: %s", _file_name.c_str());
        return arrow::Status::Invalid("file already exists");
    }
    parquet::WriterProperties::Builder writer_props;
    writer_props.enable_dictionary();
    writer_props.data_pagesize(_options.page_size);
    writer_props.max_row_group_length(_options.max_row_group_length);
    if (FLAGS_parquet_compression_type == 0) {
        writer_props.compression(parquet::Compression::SNAPPY);
    } else if (FLAGS_parquet_compression_type == 1) {
        writer_props.compression(parquet::Compression::LZ4);
    } else {
        writer_props.compression(parquet::Compression::ZSTD);
    }
    // writer_props.version(parquet::ParquetVersion::PARQUET_2_0);
    std::shared_ptr<parquet::WriterProperties> properties = writer_props.build();
    using FileClass = ::arrow::io::FileOutputStream;
    std::shared_ptr<FileClass> outfile;
    PARQUET_ASSIGN_OR_THROW(outfile, FileClass::Open(_file_name));

    std::shared_ptr<parquet::SchemaDescriptor> schm;
    RETURN_NOT_OK(parquet::arrow::ToParquetSchema(_options.schema_info->schema.get(), *properties, &schm));
    auto schema_node = std::static_pointer_cast<parquet::schema::GroupNode>(schm->schema_root());

    auto parquet_file_writer = parquet::ParquetFileWriter::Open(outfile, schema_node, properties, _key_value_metadata);
    RETURN_NOT_OK(parquet::arrow::FileWriter::Make(
            arrow::default_memory_pool(), 
            std::move(parquet_file_writer),
            _options.schema_info->schema,
            parquet::default_arrow_writer_properties(), 
            &_file_writer));
    return arrow::Status::OK();
}

arrow::Status SingleFileWriter::write_batch(const std::shared_ptr<arrow::RecordBatch>& record_batch, const RecordBatchInfo& rb_info) {
    if (record_batch->num_rows() <= 0) {
        return arrow::Status::OK();
    }

    if (rb_info.first_userid == rb_info.last_userid && rb_info.first_userid != -1) {
        if (_userid_statis.empty() || _userid_statis.back().userid != rb_info.last_userid) {
            _userid_statis.push_back({rb_info.last_userid, 1});
        } else {
            _userid_statis.back().batch_count++;
        }
    }

    if (_stash_row_cnt == 0) {
        _stash_record_batchs.emplace_back(record_batch);
        _stash_rb_infos.emplace_back(rb_info);
        _stash_row_cnt = record_batch->num_rows();
        return arrow::Status::OK();
    } else if (_stash_row_cnt + record_batch->num_rows() >= _options.max_row_group_length) {
        auto s = write_rowgroup(_stash_record_batchs, _stash_rb_infos);
        if (!s.ok()) {
            return s;
        }
        _stash_record_batchs.clear();
        _stash_rb_infos.clear();
        _stash_record_batchs.emplace_back(record_batch);
        _stash_rb_infos.emplace_back(rb_info);
        _stash_row_cnt = record_batch->num_rows();
        return arrow::Status::OK();
    }

    int64_t pre_first_userid = _stash_rb_infos[_stash_rb_infos.size() - 1].first_userid;
    int64_t pre_last_userid  = _stash_rb_infos[_stash_rb_infos.size() - 1].last_userid;
    // 尽量将相同userid放在同一个rowgroup中
    if (rb_info.first_userid == -1 || rb_info.last_userid == -1) {
        _stash_record_batchs.emplace_back(record_batch);
        _stash_rb_infos.emplace_back(rb_info);
        _stash_row_cnt += record_batch->num_rows();
        return arrow::Status::OK();
    } else if (rb_info.first_userid != rb_info.last_userid) {
        if (pre_last_userid == pre_first_userid && pre_last_userid == rb_info.first_userid) {
            _stash_record_batchs.emplace_back(record_batch);
            _stash_rb_infos.emplace_back(rb_info);
            _stash_row_cnt += record_batch->num_rows();
            return arrow::Status::OK();
        }
    } else if (rb_info.first_userid == rb_info.last_userid) {
        if (pre_last_userid == rb_info.first_userid) {
            _stash_record_batchs.emplace_back(record_batch);
            _stash_rb_infos.emplace_back(rb_info);
            _stash_row_cnt += record_batch->num_rows();
            return arrow::Status::OK();
        }
    }

    if (_stash_row_cnt + record_batch->num_rows() > FLAGS_parquet_rowgroup_min_length) {
        auto s = write_rowgroup(_stash_record_batchs, _stash_rb_infos);
        if (!s.ok()) {
            return s;
        }
        _stash_record_batchs.clear();
        _stash_rb_infos.clear();
        _stash_row_cnt = 0;
    }

    _stash_record_batchs.emplace_back(record_batch);
    _stash_rb_infos.emplace_back(rb_info);
    _stash_row_cnt += record_batch->num_rows();
    
    return arrow::Status::OK();
}

arrow::Status SingleFileWriter::write_rowgroup(const std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batchs, const std::vector<RecordBatchInfo>& rb_infos) {
    if (record_batchs.size() != rb_infos.size() || record_batchs.empty()) {
        return arrow::Status::Invalid("record batch size not equal rb_info size");
    }

    arrow::Status status = _file_writer->NewBufferedRowGroup();
    if (!status.ok()) {
        DB_COLUMN_FATAL("file_name: %s, new buffered rowgroup failed error: %s", _file_name.c_str(), status.message().c_str());
        return status;
    }

    std::string start_key = rb_infos[0].start_key;
    std::string end_key   = rb_infos[rb_infos.size() - 1].end_key;
    if (_row_count == 0) {
        _start_key = start_key;
    }
    _end_key = end_key;

    int64_t rg_rows = 0;
    for (int i = 0; i < record_batchs.size(); ++i) {
        auto& rb = record_batchs[i];
        auto& info = rb_infos[i];
        _put_count    += info.put_count;
        _delete_count += info.delete_count;
        _merge_count  += info.merge_count;
        _row_count    += info.row_count;
        rg_rows += rb->num_rows();
        if (rg_rows >= _options.max_row_group_length) {
            return arrow::Status::Invalid("rowgroup size too large");
        }
        status = _file_writer->WriteRecordBatch(*rb);
        if (!status.ok()) {
            DB_COLUMN_FATAL("file_name: %s, write record batch failed error: %s", _file_name.c_str(), status.message().c_str());
            return status;
        }
    }

    _sparse_index_map[start_key] = {_rg_idx, 0};
    _sparse_index_map[end_key]   = {_rg_idx, rg_rows - 1};
    _rg_idx++;
    return arrow::Status::OK();
}

bool SingleFileWriter::is_finish() {
    return _row_count + _stash_row_cnt >= _options.max_file_rows;
}

arrow::Status SingleFileWriter::close() {
    if (_file_writer != nullptr) {
        if (_stash_row_cnt > 0) {
            auto s = write_rowgroup(_stash_record_batchs, _stash_rb_infos);
            if (!s.ok()) {
                return s;
            }
            _stash_record_batchs.clear();
            _stash_rb_infos.clear();
            _stash_row_cnt = 0;
        }
        // 写元信息
        pb::ColumnMeta column_meta;
        for (const auto& [k, v] : _sparse_index_map) {
            auto index_item = column_meta.add_sparse_index();
            index_item->set_key(k);
            index_item->set_rowgroup_idx(v.first);
            index_item->set_pos_in_rowgroup(v.second);
        }

        std::string value;
        if (!column_meta.SerializeToString(&value)) {
            DB_COLUMN_FATAL("column_meta: %s serialize to string fail", column_meta.ShortDebugString().c_str()); 
            return arrow::Status::Invalid("rowgroup index error");
        }
        
        _key_value_metadata->Append("BAIKALDB_META", value);
        RETURN_NOT_OK(_file_writer->Close());
        _file_writer.reset(nullptr);
        _file_size = boost::filesystem::file_size(boost::filesystem::path(_file_name));
        DB_NOTICE("file_name: %s, close file success, file_size: %ld, row_count: %d, cost: %ld", 
            _file_name.c_str(), _file_size, _row_count, _cost.get_time());
    }
    return arrow::Status::OK();
}

}   // namespace baikald