#include "file_manager.h"
#include "meta_writer.h"
#include "rocksdb_filesystem.h"

namespace baikaldb {

DEFINE_bool(use_row_ranges, false, "GetRecordBatchReader use row_ranges");
DEFINE_bool(parquet_read_pre_buffer, false, "parquet read use pre_buffer");
DEFINE_bool(parquet_read_use_threads, false, "parquet read use use_threads");
DEFINE_bool(parquet_file_enable_lru, true, "parquet_file_enable_lru");
DEFINE_int32(parquet_file_lru_cache_capacity, 1024, "parquet_file_lru_cache_capacity, default(1024)");

DECLARE_int32(chunk_size);
DECLARE_int32(file_buffer_size);
DEFINE_int64(parquet_read_hole_size_limit, 8192, "parquet_read_hole_size_limit, default(8K)");
DEFINE_int64(parquet_read_range_size_limit, 4 * 1024 * 1024, "parquet_read_range_size_limit, default(4M)");

DEFINE_int64(column_minor_compaction_interval_s, 60, "column_minor_compaction_interval_s, default(60s)");
DEFINE_int64(column_minor_compaction_raft_interval, 1000, "column_minor_compaction_raft_interval, default(1000)");
DEFINE_int64(column_major_compaction_minor_interval, 10, "column_major_compaction_minor_interval, default(10)");
DEFINE_int64(column_base_compaction_interval_s, 2 * 24 * 3600, "column_major_compaction_interval_s, default(2day)");
DEFINE_int64(column_base_compaction_threshold_mb, 256, "column_base_compaction_threshold_mb, default(256MB)");
DEFINE_int64(column_base_compaction_cumulative_threshold_mb, 128, "column_base_compaction_cumulative_threshold_mb, default(128MB)");
DEFINE_int64(column_cumulative_file_max_size_mb, 16, "column_cumulative_file_max_size_mb, default(16MB)");
DEFINE_int64(column_base_compaction_threshold_percent, 10, "column_major_compaction_threshold_percent, default(10%)");
DEFINE_int64(column_cumulative_file_max_count, 100, "column_cumulative_file_max_count, default(100)");

ParquetFile::ParquetFile(const std::shared_ptr<ColumnFileInfo>& file_info) : _file_info(file_info) {
    _file_info->inc_ref();
    ColumnVars::get_instance()->parquet_file_open_count << 1;
    DB_NOTICE("create file_info: %s", _file_info->full_path().c_str());
}

void print_parquet_metadata(const std::shared_ptr<::parquet::FileMetaData>& file_metadata, const std::string& file_name) {
    std::ostringstream os;
    os << "{ parquet: {";
    for (int rg_idx = 0; rg_idx < file_metadata->num_row_groups(); ++rg_idx) {
        auto rg_meta = file_metadata->RowGroup(rg_idx);
        for (int32_t col_idx = 0; col_idx < rg_meta->num_columns(); ++col_idx) {
            auto col_meta = rg_meta->ColumnChunk(col_idx);
            os << "rg" << rg_idx << "_col" << col_idx << ": [" << col_meta->file_offset() - col_meta->total_compressed_size();
            os << ", " << col_meta->total_compressed_size() << "] ";
        }
    }
    os << "}";
    DB_NOTICE("file: %s, file_metadata: %s", file_name.c_str(), os.str().c_str());
}

int ParquetFile::open() {
    std::string path = _file_info->full_path();
    ::arrow::Result<std::shared_ptr<::arrow::io::RandomAccessFile>> 
        res = ParquetArrowReadableFile::Open(path, _file_info->file_size, _file_info->is_afs_file);
    if (!res.ok()) {
        DB_COLUMN_FATAL("Fail to open_arrow_file, path: %s, reason: %s", 
                    path.c_str(), res.status().ToString().c_str());
        return -1;
    }
    _file_path = path;
    std::shared_ptr<::arrow::io::RandomAccessFile> infile = std::move(res).ValueOrDie();

    ::parquet::arrow::FileReaderBuilder builder;
    // Set GetRecordBatchReader batch size, default: 65536
    ::parquet::ArrowReaderProperties arrow_properties = ::parquet::default_arrow_reader_properties();
    arrow_properties.set_batch_size(FLAGS_chunk_size);
    arrow_properties.set_pre_buffer(FLAGS_parquet_read_pre_buffer);
    arrow_properties.set_use_threads(FLAGS_parquet_read_use_threads);

    ::arrow::io::CacheOptions cache_options = ::arrow::io::CacheOptions::LazyDefaults();
    cache_options.hole_size_limit  = FLAGS_parquet_read_hole_size_limit;
    cache_options.range_size_limit = FLAGS_parquet_read_range_size_limit;
    arrow_properties.set_cache_options(cache_options);
    builder.properties(arrow_properties);

    // Avoid reading whole file data into memory at once
    ::parquet::ReaderProperties read_properties = ::parquet::default_reader_properties();
    read_properties.enable_buffered_stream();
    read_properties.set_buffer_size(FLAGS_file_buffer_size * 1024 * 1024ULL);
    auto status = builder.Open(infile, read_properties);
    if (!status.ok()) {
        DB_WARNING("Fail to open file, path: %s, reason: %s", 
                    path.c_str(), status.message().c_str());
        return -1;
    }
    status = builder.Build(&_reader);
    if (!status.ok()) {
        DB_WARNING("Fail to build reader, path: %s, reason: %s", 
                    path.c_str(), status.message().c_str());
        return -1;
    }
    if (_reader == nullptr) {
        DB_WARNING("_reader is nullptr, path: %s", path.c_str());
        return -1;
    }
    if (_reader->parquet_reader() == nullptr) {
        DB_WARNING("_rreader->parquet_reader() is nullptr, path: %s", path.c_str());
        return -1;
    }
    _file_metadata = _reader->parquet_reader()->metadata();
    if (_file_metadata == nullptr) {
        DB_WARNING("_file_metadata is nullptr, path: %s", path.c_str());
        return -1;
    }
    print_parquet_metadata(_file_metadata, _file_path);
    _key_value_metadata = _file_metadata->key_value_metadata();
    if (_key_value_metadata == nullptr) {
        DB_WARNING("key_value_metadata is nullptr, path: %s", path.c_str());
        return -1;
    }
    if (init_kv_metadata(_key_value_metadata->keys(), _key_value_metadata->values()) != 0) {
        DB_WARNING("init kv metadata failed, path: %s", path.c_str());
        return -1;
    }
    const ::parquet::SchemaDescriptor* schema = _file_metadata->schema();
    if (schema == nullptr) {
        DB_WARNING("schema is nullptr, path: %s", path.c_str());
        return -1;
    }
    for (int i = 0; i < schema->num_columns(); ++i) {
        const ::parquet::ColumnDescriptor* column = schema->Column(i);
        if (column == nullptr) {
            DB_WARNING("column is nullptr, path: %s", path.c_str());
            return -1;
        }
        _column_name2index_map[column->name()] = i;
    }
    return 0;
}

int ParquetFile::init_kv_metadata(const std::vector<std::string>& keys, const std::vector<std::string>& values) {
    if (keys.size() != values.size()) {
        DB_COLUMN_FATAL("path: %s, kv meta key size diff with value size [%ld, %ld]", _file_info->full_path().c_str(), keys.size(), values.size());
        return -1;
    }

    for (size_t i = 0; i < keys.size(); ++i) {
        std::string key = keys[i];
        std::string value = values[i];
        if (key != "BAIKALDB_META") {
            continue;
        }

        pb::ColumnMeta column_meta;
        if (!column_meta.ParseFromString(value)) {
            DB_COLUMN_FATAL("parse from pb fail when read column meta info, value:%s", value.c_str());
            return -1;
        }

        for (const auto& index_item : column_meta.sparse_index()) {
            if (_sparse_index_map.find(index_item.key()) != _sparse_index_map.end()) {
                // COLUMNTODO 冲突怎么处理
                DB_COLUMN_FATAL("path: %s, index key already exist [%d, %ld]", _file_info->full_path().c_str(), index_item.rowgroup_idx(), index_item.pos_in_rowgroup());
            }
            // DB_WARNING("path: %s, index key size: %ld, rg idx: %d, row idx: %ld", _file_info->full_path().c_str(), index_key.size(), rg_idx, row_idx);
            _sparse_index_map[index_item.key()] = std::make_pair(index_item.rowgroup_idx(), index_item.pos_in_rowgroup());
        }
        return 0;
    }
    DB_COLUMN_FATAL("path: %s, no BAIKALDB_META found", _file_info->full_path().c_str());
    return -1;
}

bool ParquetFile::check_interval_overlapped(const pb::PossibleIndex::Range& index_range, 
                                                 const std::string& file_start_key, const std::string& file_end_key) { 
    // 文件区间理论上不为空，BUG
    if (file_start_key.empty() || file_end_key.empty()) {
        return true;
    }

    // 索引右区间为空
    if (index_range.right_key().empty()) {
        if (index_range.left_open()) {
            return file_end_key > index_range.left_key();
        } else {
            return file_end_key >= index_range.left_key();
        }
    }

    MutTableKey end_key(index_range.right_key());
    if (!index_range.right_open()) {
        end_key.append_u64(UINT64_MAX);
        end_key.append_u64(UINT64_MAX);
        end_key.append_u64(UINT64_MAX);
    }
    const std::string& index_start_key = index_range.left_key();
    const std::string& index_end_key = end_key.data();

    const std::string max_start_key = max(index_start_key, file_start_key);
    const std::string min_end_key = min(index_end_key, file_end_key);
    if (max_start_key < min_end_key) {
        return true;
    } else if (max_start_key == min_end_key) {
        if (index_end_key == file_start_key && index_range.right_open()) {
            return false;
        }
        if (index_start_key == file_end_key && index_range.left_open()) {
            return false;
        }
        return true;
    } else {
        return false;
    }
}

void print_rg_and_rrs(std::vector<int>& rowgroup_indices,
        std::vector<std::vector<std::pair<int64_t, int64_t>>>& rowranges) {
    std::ostringstream os;
    if (rowgroup_indices.size() != rowranges.size() || rowgroup_indices.size() == 0) {
        DB_WARNING("rowgroup_indices size: %ld, rowranges size: %ld", 
                    rowgroup_indices.size(), rowranges.size());
        return;
    }
    for (int i = 0; i < rowgroup_indices.size(); ++i) {
        os << "rowgroup_" << rowgroup_indices[i] << ": {";
        for (auto& range : rowranges[i]) {
            os << "[" << range.first << ", " << range.second << "] ";
        }
        os << "} ";
    }
    DB_WARNING("rowgroup_indices: %s", os.str().c_str());
}

int ParquetFile::get_qualified_rowgroup_and_rowranges(
        const std::vector<pb::PossibleIndex::Range>& key_ranges,
        std::vector<int>& rowgroup_indices,
        std::vector<std::vector<std::pair<int64_t, int64_t>>>& rowranges) {
    if (_sparse_index_map.empty()) {
        DB_COLUMN_FATAL("path: %s, sparse_index_map is empty", _file_info->full_path().c_str());
        return -1;
    }
    
    const std::string& file_start_key = _sparse_index_map.begin()->first;
    const std::string& file_end_key   = _sparse_index_map.rbegin()->first;
    // 获取rowgroup和rowranges
    std::map<int, std::vector<std::pair<int64_t, int64_t>>> rowgroup_rowranges_map;
    for (const auto& key_range : key_ranges) {
        // 跳过和该parquet文件未重叠的key_range
        if (!check_interval_overlapped(key_range, file_start_key, file_end_key)) {
            continue;
        }

        MutTableKey range_right_key(key_range.right_key());
        if (!key_range.right_open()) {
            range_right_key.append_u64(UINT64_MAX);
            range_right_key.append_u64(UINT64_MAX);
            range_right_key.append_u64(UINT64_MAX);
        }

        // 找到小于等于left_key的最大元素的位置
        auto left_iter = _sparse_index_map.upper_bound(key_range.left_key());
        if (left_iter != _sparse_index_map.begin()) {
            --left_iter;
        }
        // 找到大于等于right_key的最小元素的位置
        auto right_iter = _sparse_index_map.lower_bound(range_right_key.data());
        if (key_range.right_key().empty()) {
            right_iter = _sparse_index_map.end();
        }
        if (right_iter == _sparse_index_map.end()) {
            --right_iter;
        }
        if (left_iter == _sparse_index_map.end() || right_iter == _sparse_index_map.end()) {
            continue;
        }
        // 获取各个RowGroup的rowrange
        const std::pair<int, int64_t>& left_rowgroup_rowidx = left_iter->second;
        const std::pair<int, int64_t>& right_rowgroup_rowidx = right_iter->second;
        const int left_rowgroup_idx = left_rowgroup_rowidx.first;
        const int right_rowgroup_idx = right_rowgroup_rowidx.first;
        if (left_rowgroup_idx > right_rowgroup_idx) {
            DB_FATAL("Invalid sparse index, left_rowgroup_idx: %d, right_rowgroup_idx: %d", 
                        left_rowgroup_idx, right_rowgroup_idx);
            return -1;
        }
        if (left_rowgroup_idx == right_rowgroup_idx) {
            // 只有一个RowGroup
            std::pair<int64_t, int64_t> rowrange = std::make_pair(left_rowgroup_rowidx.second, right_rowgroup_rowidx.second);
            rowgroup_rowranges_map[left_rowgroup_idx].emplace_back(rowrange);
        } else {
            // 处理第一个rowgroup
            auto rowgroup_metadata = get_rowgroup_metadata(left_rowgroup_idx);
            if (rowgroup_metadata == nullptr) {
                DB_WARNING("Fail to get_rowgroup_metadata, idx: %d", left_rowgroup_idx);
                return -1;
            }
            std::pair<int64_t, int64_t> rowrange = std::make_pair(left_rowgroup_rowidx.second, rowgroup_metadata->num_rows() - 1);
            rowgroup_rowranges_map[left_rowgroup_idx].emplace_back(rowrange);
            // 处理最后一个rowgroup
            rowrange = std::make_pair(0, right_rowgroup_rowidx.second);
            rowgroup_rowranges_map[right_rowgroup_idx].emplace_back(rowrange);
            // 处理中间的rowgroup
            for (int i = left_rowgroup_idx + 1; i <= right_rowgroup_idx - 1; ++i) {
                auto rowgroup_metadata = get_rowgroup_metadata(i);
                if (rowgroup_metadata == nullptr) {
                    DB_WARNING("Fail to get_rowgroup_metadata, idx: %d", i);
                    return -1;
                }
                rowrange = std::make_pair(0, rowgroup_metadata->num_rows() - 1);
                rowgroup_rowranges_map[i].emplace_back(rowrange);
            }
        }
    }
    // 同一个rowgroup下的rowrange可能有重叠，重叠区间需要合并
    for (auto& rowgroup_rowranges : rowgroup_rowranges_map) {
        std::vector<std::pair<int64_t, int64_t>> rowrange;
        std::set<int64_t> row_set;
        for (const auto& interval : rowgroup_rowranges.second) {
            row_set.insert(interval.first);
            row_set.insert(interval.second);
        }
        rowrange.emplace_back(*row_set.begin(), *row_set.rbegin());
        rowgroup_indices.emplace_back(rowgroup_rowranges.first);
        rowranges.emplace_back(rowrange);
    }
    print_rg_and_rrs(rowgroup_indices, rowranges);
    return 0;
}

::arrow::Status ParquetFile::GetRecordBatchReader(
        const std::vector<int>& row_group_indices, 
        std::unique_ptr<::arrow::RecordBatchReader>* out) {
    return _reader->GetRecordBatchReader(row_group_indices, out);
}

::arrow::Status ParquetFile::GetRecordBatchReader(
        const std::vector<int>& row_group_indices, 
        const std::vector<int>& column_indices,
        std::unique_ptr<::arrow::RecordBatchReader>* out) {
    return _reader->GetRecordBatchReader(row_group_indices, column_indices, out);
}

::arrow::Status ParquetFile::GetRecordBatchReader(
        const std::vector<int>& row_group_indices, 
        const std::vector<int>& column_indices,
        const std::vector<std::vector<std::pair<int64_t, int64_t>>>& row_ranges,
        std::unique_ptr<::arrow::RecordBatchReader>* out) {
    if (FLAGS_use_row_ranges) {
        // TODO -
        return ::arrow::Status::OK();
    }
    return _reader->GetRecordBatchReader(row_group_indices, column_indices, out);
}

::arrow::Status ParquetFile::GetRecordBatchReader(std::unique_ptr<::arrow::RecordBatchReader>* out) {
    return _reader->GetRecordBatchReader(out);
}

int ParquetFileReader::init() {
    if (_init) {
        return 0;
    }
    int ret = _parquet_file->open();
    if (ret < 0) {
        DB_COLUMN_FATAL("Fail to open parquet file");
        return -1;
    }

    auto s = _parquet_file->GetRecordBatchReader(&_reader);
    if (!s.ok()) {
        DB_COLUMN_FATAL("Fail to get_record_batch_reader");
        return -1;
    }
    _init = true;
    return 0;
}

::arrow::Status ParquetFileReader::ReadNext(std::shared_ptr<::arrow::RecordBatch>* batch) {
    int ret = init();
    if (ret < 0) {
        return ::arrow::Status::IOError("Fail to init");
    }
    std::shared_ptr<::arrow::RecordBatch> tmp_batch;
    auto s = _reader->ReadNext(&tmp_batch);
    if (!s.ok()) {
        DB_COLUMN_FATAL("%s Fail to read next batch: %s", _parquet_file->get_file_path().c_str(), s.message().c_str());
        return s;
    }
    
    if (tmp_batch == nullptr) {
        batch->reset();
        return ::arrow::Status::OK();
    }
    const auto& key_fields = _options.schema_info->key_fields;
    const auto& value_fields = _options.schema_info->value_fields;
    const auto& schema = _options.need_order_info ? _options.schema_info->schema_with_order_info : _options.schema_info->schema;
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (int i = 0; i < schema->num_fields(); ++i) {
        const auto& f = schema->field(i);
        std::shared_ptr<arrow::Array> array;
        array = tmp_batch->GetColumnByName(f->name());
        if (array != nullptr) {
            columns.emplace_back(array);
            continue;
        }
        if (f->name() == RAFT_INDEX_NAME) {
            ExprValue raft_index;
            raft_index.type = pb::INT64;
            raft_index._u.int64_val = _options.raftindex;
            array = ColumnRecord::make_array_from_exprvalue(
                pb::INT64, raft_index, tmp_batch->num_rows());
        } else if (f->name() == BATCH_POS_NAME) {
            ExprValue batch_pos;
            batch_pos.type = pb::INT32;
            batch_pos._u.int32_val = 0;
            array = ColumnRecord::make_array_from_exprvalue(
                pb::INT32, batch_pos, tmp_batch->num_rows());
        } else {
            if (i < key_fields.size()) {
                DB_COLUMN_FATAL("%s Fail to find key field", _parquet_file->get_file_path().c_str());
                return arrow::Status::IOError("Fail to find key field");
            }
            const auto& field = value_fields[i - key_fields.size()];
            array = ColumnRecord::make_array_from_exprvalue(
                                field.type, field.default_expr_value, tmp_batch->num_rows());
        }
        if (array == nullptr) {
            DB_COLUMN_FATAL("%s Fail to make array from expr value", _parquet_file->get_file_path().c_str());
            return arrow::Status::IOError("Fail to make array from expr value");
        }
        columns.emplace_back(array);
    }
    *batch = arrow::RecordBatch::Make(schema, tmp_batch->num_rows(), columns);
    return ::arrow::Status::OK();
}

void ColumnFileManager::remove_column_file(int64_t region_id, int64_t table_id) {
    std::string column_dir = ColumnFileInfo::make_column_file_directory(region_id, table_id);
    boost::filesystem::path output_path(column_dir);
    if (boost::filesystem::exists(output_path)) {
        // 清空目录
        boost::filesystem::remove_all(output_path);
    } 
}

int ColumnFileManager::init() {
    if (_init) {
        DB_WARNING("region_id: %ld, ColumnFileManager has been inited", _region_id);
        return 0;
    }
    pb::RegionColumnFiles pb_file_info;
    int ret = MetaWriter::get_instance()->read_column_file_info(_region_id, pb_file_info);
    if (ret == -2) {
        _init = true;
        return 0;
    } else if (ret < 0) {
        DB_COLUMN_FATAL("Fail to read column file info, region_id: %ld", _region_id);
        return -1;
    }

    proto2fileinfo_unlock(pb_file_info);
    _init = true;
    return 0;
}

int ColumnFileManager::load_snapshot(bool restart) {
    if (restart) {
        return init();
    } else {
        clear();
        return init();
    }
}

void ColumnFileManager::fileinfo2proto(const std::shared_ptr<ColumnFileInfo>& mem_info, pb::ColumnFileInfo* pb_info) {
    pb_info->set_file_name(mem_info->file_name);
    pb_info->set_afs_full_name(mem_info->afs_full_name);
    pb_info->set_region_id(mem_info->region_id);
    pb_info->set_table_id(mem_info->table_id);
    pb_info->set_is_afs_file(mem_info->is_afs_file);
    pb_info->set_file_size(mem_info->file_size);
    pb_info->set_start_version(mem_info->start_version);
    pb_info->set_end_version(mem_info->end_version);
    pb_info->set_put_count(mem_info->put_count);
    pb_info->set_delete_count(mem_info->delete_count);
    pb_info->set_merge_count(mem_info->merge_count);
    pb_info->set_row_count(mem_info->row_count);
    pb_info->set_start_key(mem_info->start_key);
    pb_info->set_end_key(mem_info->end_key);
    for (const auto userid : mem_info->userids) {
        pb_info->add_userids(userid);
    }
}

void ColumnFileManager::make_snapshot(std::shared_ptr<ColumnFileSet>* column_file_set, pb::RegionColumnFiles& pb_file_info) {
    std::unique_lock<std::mutex> l(_mutex);
    *column_file_set = std::make_shared<ColumnFileSet>(_active_files);
    pb_file_info.set_max_version(_max_version);
    pb_file_info.set_column_status(_column_status);
    for (const auto& [_, f] : _active_files) {
        auto a_file = pb_file_info.add_active_files();
        fileinfo2proto(f, a_file);
    }
}

void ColumnFileManager::remove_column_data(pb::ColumnStatus column_status, int64_t max_version) {
    std::unique_lock<std::mutex> l(_mutex);
    _delete_files.insert(_active_files.begin(), _active_files.end());
    _active_files.clear();
    _base_file_count = 0;
    _base_file_size = 0;
    _cumulatives_file_count = 0;
    _cumulatives_file_size = 0;
    _max_version = max_version;
    _column_status = column_status;
    fileinfo2proto_unlock();
    DB_NOTICE("region_id: %ld, remove column data, status: %d, max_version: %ld", _region_id, column_status, max_version);
}

void ColumnFileManager::column_file_gc() {
    // COLUMNTODO 删除AFS文件
    std::vector<std::shared_ptr<ColumnFileInfo>> need_delete_file_infos;
    std::vector<std::shared_ptr<ColumnFileInfo>> file_del_lru_cache;
    {
        std::unique_lock<std::mutex> l(_mutex);
        need_delete_file_infos.reserve(_delete_files.size());
        for (auto iter = _delete_files.begin(); iter != _delete_files.end();) {
            if (iter->second->ref_count.load() <= 0) {
                need_delete_file_infos.emplace_back(iter->second);
                iter = _delete_files.erase(iter);
            } else {
                file_del_lru_cache.emplace_back(iter->second);
                iter++;
            }
        }
        if (!need_delete_file_infos.empty()) {
            fileinfo2proto_unlock();
        }
    }
    
    for (const auto& file : need_delete_file_infos) {
        if (file->is_afs_file) {
            continue;
        }
        boost::filesystem::path path(file->full_path());
        if (boost::filesystem::exists(path)) {
            DB_NOTICE("delete column file: %s", file->full_path().c_str());
            boost::filesystem::remove(path);
        }
    }

    ParquetFileManager::get_instance()->remove_parquet_file_from_cache(file_del_lru_cache);
}

int ColumnFileManager::fileinfo2proto_unlock() {
    pb::RegionColumnFiles pb_file_info;
    pb_file_info.set_max_version(_max_version);
    pb_file_info.set_column_status(_column_status);
    for (const auto& [_, file] : _active_files) {
        auto a_file = pb_file_info.add_active_files();
        fileinfo2proto(file, a_file);
    }
    for (const auto& [_, file] : _delete_files) {
        auto d_file = pb_file_info.add_delete_files();
        fileinfo2proto(file, d_file);
    }

    int ret = MetaWriter::get_instance()->write_column_file_info(_region_id, pb_file_info);
    if (ret < 0) {
        DB_COLUMN_FATAL("Fail to write column file info, region_id: %ld", _region_id);
        return -1;
    }
    DB_DEBUG("region_id: %ld, write column file info success, info: %s", _region_id, pb_file_info.ShortDebugString().c_str());
    return 0;
}

void ColumnFileManager::proto2fileinfo_unlock(const pb::RegionColumnFiles& pb_file_info) {
    _active_files.clear();
    _delete_files.clear();
    _max_version = pb_file_info.max_version();
    _column_status = pb_file_info.column_status();
    _last_minor_compact_time = butil::gettimeofday_us();
    _last_major_compact_time = butil::gettimeofday_us();
    _last_base_compact_time  = butil::gettimeofday_us();

    for (const auto& pb_file : pb_file_info.active_files()) {
        auto mem_info = std::make_shared<ColumnFileInfo>(pb_file);
        _active_files[mem_info->file_name] = mem_info;
        if (mem_info->start_version == 0) {
            _base_file_size += mem_info->file_size;
            _base_file_count++;
        } else {
            _cumulatives_file_size += mem_info->file_size;
            _cumulatives_file_count++;
        }
    }
    for (const auto& pb_file : pb_file_info.delete_files()) {
        auto mem_info = std::make_shared<ColumnFileInfo>(pb_file);
        _delete_files[mem_info->file_name] = mem_info;
    }
    DB_NOTICE("region_id: %ld, read column file info success, info: %s", _region_id, pb_file_info.ShortDebugString().c_str());
}

// 选取一个minor compaction文件，并加锁，start_version为需要读取的raftlog start index
int ColumnFileManager::pick_minor_compact_file(int64_t applied_index, int64_t& start_version) {
    std::unique_lock<std::mutex> l(_mutex);
    if (_column_status != pb::CS_NORMAL) {
        DB_WARNING("region_id: %ld, column status invalid", _region_id);
        return -1;
    }

    if (_cumulatives_file_count > FLAGS_column_cumulative_file_max_count) {
        DB_COLUMN_FATAL("region_id: %ld, too many cumulative files: %ld", _region_id, _cumulatives_file_count);
    }

    if (applied_index <= _max_version) {
        return -1;
    }

    if (butil::gettimeofday_us() - _last_minor_compact_time < FLAGS_column_minor_compaction_interval_s * 1000 * 1000LL 
        && applied_index - _max_version < FLAGS_column_minor_compaction_raft_interval) {
        return -1;
    }

    start_version = _max_version + 1;
    return 0;
}

int ColumnFileManager::pick_major_compact_file(std::vector<std::shared_ptr<ColumnFileInfo>>& file_infos) {
    std::unique_lock<std::mutex> l(_mutex);
    if (_column_status != pb::CS_NORMAL) {
        DB_WARNING("region_id: %ld, column status invalid", _region_id);
        return -1;
    }

    if (_cumulatives_file_count <= 0) {
        return -1;
    }

    std::map<int64_t, std::shared_ptr<ColumnFileInfo>> cumulative_files;
    for (const auto& [_, file] : _active_files) {
        if (file->start_version == 0) {
            continue;
        }
        cumulative_files[file->start_version] = file;
    }

    std::vector<std::shared_ptr<ColumnFileInfo>> pick_set;
    std::vector<std::vector<std::shared_ptr<ColumnFileInfo>> > pick_files;
    pick_set.reserve(cumulative_files.size());
    pick_files.reserve(cumulative_files.size());
    int64_t set_size = 0;
    for (const auto& [_, file] : cumulative_files) {
        if (file->file_size > FLAGS_column_cumulative_file_max_size_mb * 1024 * 1024LL) {
            if (!pick_set.empty()) {
                pick_files.push_back(pick_set);
                set_size = 0;
                pick_set.clear();
            }
            continue;
        }

        if (set_size + file->file_size > 2 * FLAGS_column_cumulative_file_max_size_mb * 1024 * 1024LL) {
            // 达到两倍的cumulative文件大小限制
            if (!pick_set.empty()) {
                pick_files.push_back(pick_set);
                set_size = 0;
                pick_set.clear();
            }
        }

        set_size += file->file_size;
        pick_set.push_back(file);
    }

    if (!pick_set.empty()) {
        pick_files.push_back(pick_set);
        set_size = 0;
        pick_set.clear();
    }

    if (pick_files.empty()) {
        return -1;
    }

    int total_file_cnt = 0;
    std::map<int, std::vector<std::shared_ptr<ColumnFileInfo>> > cnt_files;
    for (const auto& files : pick_files) {
        total_file_cnt += files.size();
        if (files.size() < 2) {
            continue;
        }
        cnt_files[files.size()] = files;
    }

    if (cnt_files.empty()) {
        DB_WARNING("region_id: %ld, no major compact files", _region_id);
        return -1;
    }

    if (total_file_cnt < FLAGS_column_major_compaction_minor_interval
        && butil::gettimeofday_us() - _last_major_compact_time < FLAGS_column_major_compaction_minor_interval * FLAGS_column_minor_compaction_interval_s * 1000 * 1000LL) {
        return -1;
    }

    file_infos = cnt_files.rbegin()->second;
    return 0;
}

// COLUMNTODO compaction参数每个表可定制
int ColumnFileManager::pick_base_compact_file(std::vector<std::shared_ptr<ColumnFileInfo>>& file_infos) {
    std::unique_lock<std::mutex> l(_mutex);
    if (_column_status != pb::CS_NORMAL) {
        DB_WARNING("region_id: %ld, column status invalid", _region_id);
        return -1;
    }

    if (_cumulatives_file_count <= 0) {
        return -1;
    }

    // 时间过长, 直接进行base compact
    bool need_check_cumulatives_size = true;
    if (butil::gettimeofday_us() - _last_base_compact_time > FLAGS_column_base_compaction_interval_s * 1000 * 1000LL) {
        need_check_cumulatives_size = false;
    }

    bool need_base_compact = false;
    if (need_check_cumulatives_size) {
        if (_base_file_size > FLAGS_column_base_compaction_threshold_mb * 1024 * 1024LL && 
            _cumulatives_file_size * 100 / _base_file_size > FLAGS_column_base_compaction_threshold_percent) {
            // cumulatives达到base的一定比例时触发major compact
            need_base_compact = true;
        } else if (_cumulatives_file_size > FLAGS_column_base_compaction_cumulative_threshold_mb * 1024 * 1024LL) {
            need_base_compact = true;
        }
    } else {
        need_base_compact = true;
    }

    if (_manual_base_compaction.load()) {
        _manual_base_compaction = false;
    } else if (!need_base_compact) {
        return -1;
    }

    std::map<std::string, std::shared_ptr<ColumnFileInfo>> pick_files;
    std::string cumulatives_startkey = "";
    std::string cumulatives_endkey = "";
    for (const auto& [_, file] : _active_files) {
        if (file->start_version == 0) {
            continue;
        }
        pick_files[file->file_name] = file;
        if (cumulatives_startkey.empty() || file->start_key < cumulatives_startkey) {
            cumulatives_startkey = file->start_key;
        }
        if (cumulatives_endkey.empty() || file->end_key > cumulatives_endkey) {
            cumulatives_endkey = file->end_key;
        }
    }

    if (pick_files.empty()) {
        DB_WARNING("region_id: %ld no file to major compact", _region_id);
        return -1;
    }

    for (const auto& [_, file] : _active_files) {
        if (file->start_version != 0) {
            continue;
        }

        if (file->end_key < cumulatives_startkey || file->start_key > cumulatives_endkey) {
            continue;
        }

        pick_files[file->file_name] = file;
    }

    if (pick_files.size() < 2) {
        DB_WARNING("region_id: %ld no file to major compact", _region_id);
        return -1;
    }

    // COLUMNTODO 判断没有重叠直接move到base层

    for (const auto& iter : pick_files) {
        file_infos.emplace_back(iter.second);
    }

    return 0;
}

int ColumnFileManager::finish_minor_compact(const std::shared_ptr<ColumnFileInfo>& new_file, int64_t last_max_version) {
    std::unique_lock<std::mutex> l(_mutex);
    if (_max_version != last_max_version) {
        DB_WARNING("region_id: %ld, max version %ld not match [%ld vs %ld]", _region_id, new_file->end_version, _max_version, last_max_version);
        return -1;
    }
    _max_version = new_file->end_version;
    _cumulatives_file_count++;
    _cumulatives_file_size += new_file->file_size;
    _last_minor_compact_time = butil::gettimeofday_us();
    _active_files[new_file->file_name] = new_file;
    fileinfo2proto_unlock();
    ColumnVars::get_instance()->cumulative_file_max_count << _cumulatives_file_count;
    return 0;
}

int ColumnFileManager::update_version_only(int64_t max_version, int64_t last_max_version) {
    std::unique_lock<std::mutex> l(_mutex);
    if (_max_version != last_max_version) {
        DB_WARNING("region_id: %ld, max version %ld not match [%ld vs %ld]", _region_id, max_version, _max_version, last_max_version);
        return -1;
    }
    _max_version = max_version;
    fileinfo2proto_unlock();
    return 0;
}

int ColumnFileManager::finish_major_compact(const std::vector<std::shared_ptr<ColumnFileInfo>>& old_files, 
                            const std::vector<std::shared_ptr<ColumnFileInfo>>& new_files, bool is_base) {
    std::unique_lock<std::mutex> l(_mutex);
    // 检查文件列表是否发生变动
    for (const auto& file : old_files) {
        if (_active_files.count(file->file_name) <= 0) {
            return -1;
        }
    }
    for (const auto& file : old_files) {
        if (file->start_version == 0) {
            _base_file_count--;
            _base_file_size -= file->file_size;
        } else {
            _cumulatives_file_count--;
            _cumulatives_file_size -= file->file_size;
        }
        _active_files.erase(file->file_name);
        _delete_files[file->file_name] = file;
    }

    for (const auto& file : new_files) {
        if (file->start_version == 0) {
            _base_file_count++;
            _base_file_size += file->file_size;
        } else {
            _cumulatives_file_count++;
            _cumulatives_file_size += file->file_size;
        }
        _active_files[file->file_name] = file;
    }

    if (is_base) {
        _last_base_compact_time = butil::gettimeofday_us();
    } else {
        _last_major_compact_time = butil::gettimeofday_us();
    }

    fileinfo2proto_unlock();
    return 0;
}

void ColumnFileManager::finish_row2column(const std::vector<std::shared_ptr<ColumnFileInfo>>& new_files, int64_t max_version) {
    std::unique_lock<std::mutex> l(_mutex);
    if (!_active_files.empty() || !_delete_files.empty() || _column_status != pb::CS_INVALID) {
        // 理论上不会走到  BUG
        DB_COLUMN_FATAL("base compact should be done before");
        for (auto & iter : _active_files) {
            const auto& file = iter.second;
            _delete_files[file->file_name] = file;
            DB_WARNING("base compact file:%s, start_version:%ld, end_version:%ld, file_size:%ld", 
                    file->file_name.c_str(), file->start_version, file->end_version, file->file_size);

        }
        _active_files.clear();
    }

    for (const auto& file : new_files) {
        if (file->start_version == 0) {
            _base_file_count++;
            _base_file_size += file->file_size;
        } else {
            _cumulatives_file_count++;
            _cumulatives_file_size += file->file_size;
        }
        _active_files[file->file_name] = file;
    }
    _column_status = pb::CS_NORMAL;
    _max_version = max_version;
    _last_minor_compact_time = butil::gettimeofday_us();
    _last_major_compact_time = butil::gettimeofday_us();
    _last_base_compact_time  = butil::gettimeofday_us();
    fileinfo2proto_unlock();
}

void ColumnFileManager::finish_flush_to_cold(const std::vector<std::shared_ptr<ColumnFileInfo>>& new_files, int64_t max_version) {
    std::unique_lock<std::mutex> l(_mutex);
    _delete_files.insert(_active_files.begin(), _active_files.end());
    _active_files.clear();
    _base_file_count = 0;
    _base_file_size = 0;
    _cumulatives_file_count = 0;
    _cumulatives_file_size = 0;
    _max_version = max_version;
    _column_status = pb::CS_COLD;
    for (const auto& file : new_files) {
        _active_files[file->file_name] = file;
        _base_file_count++;
        _base_file_size += file->file_size;
    }
    fileinfo2proto_unlock();
    DB_NOTICE("region_id: %ld, flush cold files success", _region_id);
}

} // namespace baikaldb
