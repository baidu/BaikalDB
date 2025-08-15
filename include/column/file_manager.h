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
#include "common.h"
#include "lru_cache.h"
#include "file_system.h"
#include "parquet_writer.h"
#include "proto/column.pb.h"
#include "column_statistics.h"
#include "parquet_cache.h"
// #include <boost/compute/detail/lru_cache.hpp>

namespace baikaldb {
DECLARE_string(db_path);
DECLARE_bool(parquet_file_enable_lru);
DECLARE_int32(parquet_file_lru_cache_capacity);

class ColumnFileInfo {
public:
    ColumnFileInfo(const std::shared_ptr<ColumnFileInfo>& file) {
        file_name     = file->file_name;
        afs_full_name = file->afs_full_name;
        region_id     = file->region_id;
        table_id      = file->table_id;
        is_afs_file   = file->is_afs_file;
        file_size     = file->file_size;
        start_version = file->start_version;
        end_version   = file->end_version;
        put_count     = file->put_count;
        delete_count  = file->delete_count;
        merge_count   = file->merge_count;
        row_count     = file->row_count;
        start_key     = file->start_key;
        end_key       = file->end_key;
        userids       = file->userids;
    }
    ColumnFileInfo(const pb::ColumnFileInfo& pb_info) {
        file_name     = pb_info.file_name();
        afs_full_name = pb_info.afs_full_name();
        region_id     = pb_info.region_id();
        table_id      = pb_info.table_id();
        is_afs_file   = pb_info.is_afs_file();
        file_size     = pb_info.file_size();
        start_version = pb_info.start_version();
        end_version   = pb_info.end_version();
        put_count     = pb_info.put_count();
        delete_count  = pb_info.delete_count();
        merge_count   = pb_info.merge_count();
        row_count     = pb_info.row_count();
        start_key     = pb_info.start_key();
        end_key       = pb_info.end_key();
        for (const auto userid : pb_info.userids()) {
            userids.insert(userid);
        }
    }

    ColumnFileInfo(int64_t _table_id, int64_t _region_id, int64_t _start_version, int64_t _end_version, int file_idx, const ColumnFileMeta& info) { 
        file_name     = ColumnFileInfo::make_file_name(_table_id, _region_id, _start_version, _end_version, file_idx, info.file_size, info.row_count);
        afs_full_name.clear();
        region_id     = _region_id;
        table_id      = _table_id;
        is_afs_file   = false;
        file_size     = info.file_size;
        start_version = _start_version;
        end_version   = _end_version;
        put_count     = info.put_count;
        delete_count  = info.delete_count;
        merge_count   = info.merge_count;
        row_count     = info.row_count;
        start_key     = info.start_key;
        end_key       = info.end_key;
        userids       = info.userids;
    }

    ~ColumnFileInfo() { }
    std::string file_name;
    std::string afs_full_name; // 兼容olap文件在afs上的完整路径。例：afs://yinglong.afs.baidu.com:9902/user/baikal/xxxx
    std::atomic<int> ref_count {0};
    int64_t region_id = 0;
    int64_t table_id = 0;
    bool is_afs_file = false;
    int64_t file_size = 0;
    int64_t start_version = 0;
    int64_t end_version = 0;
    int put_count = 0;
    int delete_count = 0;
    int merge_count = 0;
    int row_count = 0;
    std::string start_key;
    std::string end_key;
    std::set<int64_t> userids;
    virtual std::string full_path() {
        if (is_afs_file) {
            return afs_full_name;
        } else {
            return FLAGS_db_path + "_column/" + std::to_string(table_id) + "/" + std::to_string(region_id) + "/" + file_name;
        }
    }
    static std::string make_column_file_directory(int64_t region_id, int64_t table_id) {
        return FLAGS_db_path + "_column/" + std::to_string(table_id) + "/" + std::to_string(region_id);
    }
    static std::string make_file_name(int64_t table_id, int64_t region_id, int64_t start_version, int64_t end_version, int idx, int64_t size, int lines) {
        return std::to_string(region_id) + "_" + std::to_string(table_id) + "_" + 
            std::to_string(start_version) + "_" + std::to_string(end_version) + "_" + std::to_string(idx) + "_" + 
            std::to_string(size) + "_" + std::to_string(lines) + ".parquet";
    }

    static int get_file_idx(const std::string& file_name) {
        std::vector<std::string> split_vec;
        boost::split(split_vec, file_name, boost::is_any_of("_"));
        if (split_vec.size() != 7) {
            return -1;
        }
        return boost::lexical_cast<int>(split_vec[4]);
    }

    void inc_ref() {
        ++ref_count;
    }
    void dec_ref() {
        --ref_count;
    }
private:
    DISALLOW_COPY_AND_ASSIGN(ColumnFileInfo);
};

// 持有计数以防删除
class ColumnFileSet {
public:
    ColumnFileSet() = delete;
    ColumnFileSet(const std::map<std::string, std::shared_ptr<ColumnFileInfo>>& files) : column_files(files) {
        for (auto& pair : column_files) {
            pair.second->inc_ref();
        }
    }
    ~ColumnFileSet() {
        for (auto& pair : column_files) {
            pair.second->dec_ref();
        }
    }
    std::map<std::string, std::shared_ptr<ColumnFileInfo>> column_files;
};
class ParquetFile {
public:
    ParquetFile(const std::shared_ptr<ColumnFileInfo>& file_info);
    virtual ~ParquetFile() {
        _file_info->dec_ref();
        ColumnVars::get_instance()->parquet_file_open_count << -1;
        DB_NOTICE("ParquetFile::~ParquetFile %s", _file_info->full_path().c_str());
    }

    int open();

    std::string get_file_path() {
        return _file_path;
    }

    std::string get_file_short_name() {
        return _file_info->file_name;
    }

    std::shared_ptr<ColumnFileInfo> get_file_info() {
        return _file_info;
    }

    std::shared_ptr<::parquet::FileMetaData> get_file_metadata() {
        return _file_metadata;
    }

    std::shared_ptr<::parquet::RowGroupMetaData> get_rowgroup_metadata(const int rowgroup_index) {
        if (_file_metadata == nullptr) {
            return nullptr;
        }
        return _file_metadata->RowGroup(rowgroup_index);
    }

        // 获取每个parquet文件中符合条件的rowgroup和rowranges
    int get_qualified_rowgroup_and_rowranges(
            const std::vector<pb::PossibleIndex::Range>& key_ranges,
            std::vector<int>& rowgroup_indices,
            std::vector<std::vector<std::pair<int64_t, int64_t>>>& rowranges);

    int init_kv_metadata(const std::vector<std::string>& keys, const std::vector<std::string>& values);

    const std::unordered_map<std::string, int>& get_column_name2index_map() {
        return _column_name2index_map;
    }

    const std::map<std::string, std::pair<int, int64_t>>& get_sparse_index_map() {
        return _sparse_index_map;
    }

    void parser_position(const std::vector<int>& row_group_indices, 
        const std::vector<int>& column_indices, std::vector<ReadRange>& ranges) {
        for (int rg_idx : row_group_indices) {
            auto rg_meta = _file_metadata->RowGroup(rg_idx);
            for (int col_idx : column_indices) {
                auto col_meta = rg_meta->ColumnChunk(col_idx);
                ranges.push_back(ReadRange{col_meta->file_offset() - col_meta->total_compressed_size(), col_meta->total_compressed_size()});
            }
        }
    }

    ::arrow::Status GetRecordBatchReader(
        const std::vector<int>& row_group_indices, 
        std::unique_ptr<::arrow::RecordBatchReader>* out);

    ::arrow::Status GetRecordBatchReader(
        const std::vector<int>& row_group_indices, 
        const std::vector<int>& column_indices,
        std::unique_ptr<::arrow::RecordBatchReader>* out);
        
    ::arrow::Status GetRecordBatchReader(
        const std::vector<int>& row_group_indices, 
        const std::vector<int>& column_indices,
        const std::vector<std::vector<std::pair<int64_t, int64_t>>>& row_ranges,
        std::unique_ptr<::arrow::RecordBatchReader>* out);

    ::arrow::Status GetRecordBatchReader(std::unique_ptr<::arrow::RecordBatchReader>* out);
    static bool check_interval_overlapped(
            const pb::PossibleIndex::Range& index_range, const std::string& file_start_key, const std::string& file_end_key);

private:
    std::shared_ptr<ColumnFileInfo> _file_info;
    std::string _file_path;
    std::unique_ptr<::parquet::arrow::FileReader> _reader;
    std::shared_ptr<::parquet::FileMetaData> _file_metadata;
    std::shared_ptr<const ::arrow::KeyValueMetadata> _key_value_metadata;
    std::unordered_map<std::string, int> _column_name2index_map;
    std::map<std::string, std::pair<int, int64_t>> _sparse_index_map;
    DISALLOW_COPY_AND_ASSIGN(ParquetFile);
};

struct ParquetFileReaderOptions {
    bool                                need_order_info = true;
    int64_t                             raftindex   = 0;
    std::shared_ptr<ColumnSchemaInfo>   schema_info = nullptr;
    std::shared_ptr<ColumnFileInfo>     file_info   = nullptr;
};

class ParquetFileReader : public ::arrow::RecordBatchReader {
public:
    ParquetFileReader(ParquetFileReaderOptions& options) : _options(options) {
        _parquet_file = std::make_shared<ParquetFile>(_options.file_info);
    }
    virtual ~ParquetFileReader() { }

    int init();

    std::shared_ptr<arrow::Schema> schema() const override { return nullptr; }

    virtual ::arrow::Status ReadNext(std::shared_ptr<::arrow::RecordBatch>* batch) override;
private:
    ParquetFileReaderOptions _options;
    std::shared_ptr<ParquetFile> _parquet_file;
    std::unique_ptr<::arrow::RecordBatchReader> _reader;
    bool _init = false;
};

class ParquetFileManager {
public:
    static ParquetFileManager* get_instance() {
        static ParquetFileManager instance;
        return &instance;
    }

    bool link_file(const std::string& old_path, const std::string& new_path) {
        return ::link(old_path.c_str(), new_path.c_str()) == 0;
    }

    std::shared_ptr<ParquetFile> get_parquet_file(const std::shared_ptr<ColumnFileInfo>& file_info) {
        bool use_lru = FLAGS_parquet_file_enable_lru;
        if (use_lru) {
            std::unique_lock<std::mutex> l(_mutex);
            std::shared_ptr<ParquetFile> file = nullptr;
            int ret = _lru_cache.find(file_info->file_name, &file);
            if (ret == 0) {
                return file;
            }
        }

        auto file = std::make_shared<ParquetFile>(file_info);
        int ret = file->open();
        if (ret < 0) {
            DB_COLUMN_FATAL("open file:%s failed, ret:%d", file_info->full_path().c_str(), ret);
            return nullptr;
        }

        if (use_lru) {
            // 加入lrucache
            std::unique_lock<std::mutex> l(_mutex);
            _lru_cache.add(file_info->file_name, file);
        }

        return file;
    }

    void remove_parquet_file_from_cache(const std::vector<std::shared_ptr<ColumnFileInfo>>& files) {
        std::unique_lock<std::mutex> l(_mutex);
        for (auto& file : files) {
            _lru_cache.del(file->file_name);
        }
    }

private:
    ParquetFileManager() { 
        _lru_cache.init(FLAGS_parquet_file_lru_cache_capacity);
    }
    std::mutex _mutex;
    // LRU 管理ParquetFile
    // https://www.boost.org/doc/libs/1_67_0/boost/compute/detail/lru_cache.hpp
    // boost::compute::detail::lru_cache<std::string, std::shared_ptr<ParquetFile>> _lru_cache;
    Cache<std::string, std::shared_ptr<ParquetFile>> _lru_cache;
};

// 只管理单个rengion的列存文件元数据
class ColumnFileManager {
public:
    ColumnFileManager(int64_t region_id) : _region_id(region_id) { }
    ~ColumnFileManager() { }
    static void fileinfo2proto(const std::shared_ptr<ColumnFileInfo>& mem_info, pb::ColumnFileInfo* pb_info);
    static void remove_column_file(int64_t region_id, int64_t table_id);
    int init();
    int load_snapshot(bool restart);
    int pick_minor_compact_file(int64_t applied_index, int64_t& start_version);
    int pick_major_compact_file(std::vector<std::shared_ptr<ColumnFileInfo>>& file_infos);
    int pick_base_compact_file(std::vector<std::shared_ptr<ColumnFileInfo>>& file_infos);
    int finish_minor_compact(const std::shared_ptr<ColumnFileInfo>& new_file, int64_t last_max_version);
    int finish_major_compact(const std::vector<std::shared_ptr<ColumnFileInfo>>& old_files, 
                            const std::vector<std::shared_ptr<ColumnFileInfo>>& new_files, bool is_base);
    int update_version_only(int64_t max_version, int64_t last_max_version);
    void finish_row2column(const std::vector<std::shared_ptr<ColumnFileInfo>>& new_files, int64_t max_version);
    void finish_flush_to_cold(const std::vector<std::shared_ptr<ColumnFileInfo>>& new_files, int64_t max_version); 
    int fileinfo2proto_unlock();
    void proto2fileinfo_unlock(const pb::RegionColumnFiles& pb_file_info);

    void make_snapshot(std::shared_ptr<ColumnFileSet>* column_file_set, pb::RegionColumnFiles& pb_file_info);
    void remove_column_data(pb::ColumnStatus column_status, int64_t max_version);
    void column_file_gc();
    void manual_base_compaction() {
        std::unique_lock<std::mutex> l(_mutex);
        _manual_base_compaction = true;
    }

    pb::ColumnStatus column_status() {
        std::unique_lock<std::mutex> l(_mutex);
        return _column_status;
    }

    int64_t column_lines() {
        std::unique_lock<std::mutex> l(_mutex);
        int64_t lines = 0;
        for (const auto& [_, file] : _active_files) {
            lines += file->row_count;
        }
        return lines;
    }

    int64_t max_version() {
        std::unique_lock<std::mutex> l(_mutex);
        return _max_version;
    }

    // 需要配合_olap_state.load() == pb::OLAP_TRUNCATED使用
    bool can_flush_to_cold() {
        std::unique_lock<std::mutex> l(_mutex);
        return _cumulatives_file_size <= 0 && _base_file_size > 0 && _column_status == pb::CS_NORMAL;
    }

    // 持有计数，ColumnFileSet智能指针释放时减计数
    std::shared_ptr<ColumnFileSet> get_column_fileset() {
        std::unique_lock<std::mutex> l(_mutex);
        return std::make_shared<ColumnFileSet>(_active_files);
    }

    bool column_storage_valid() {
        std::unique_lock<std::mutex> l(_mutex);
        return _column_status != pb::CS_INVALID;
    }

    bool match_userid_statis(const std::set<int64_t>& userids) {
        std::unique_lock<std::mutex> l(_mutex);
        for (const auto& [_, file] : _active_files) {
            for (const int64_t userid : userids) {
                if (file->userids.count(userid) > 0) {
                    return true;
                }
            }
        }
        return false;
    }

    bool has_init() const {
        return _init;
    }

    void clear() {
        _column_status = pb::CS_INVALID;
        _init = false;
        _base_file_size = 0;
        _cumulatives_file_size = 0;
        _base_file_count = 0;
        _cumulatives_file_count = 0;
        _active_files.clear();
        _delete_files.clear();
    }

private: 
    const int64_t _region_id;
    std::mutex _mutex;
    pb::ColumnStatus _column_status = pb::CS_INVALID;
    int64_t _column_version = 0;
    bool    _init = false;

    // 统计信息
    int64_t _base_file_size = 0;
    int64_t _cumulatives_file_size = 0;
    int64_t _base_file_count = 0;
    int64_t _cumulatives_file_count = 0;

    // compaction 辅助信息
    int64_t _last_minor_compact_time = butil::gettimeofday_us();
    int64_t _last_major_compact_time = butil::gettimeofday_us();
    int64_t _last_base_compact_time = butil::gettimeofday_us();

    int64_t _max_version = 0; // 当前最大的raftlogindex

    std::map<std::string, std::shared_ptr<ColumnFileInfo>> _active_files;
    std::map<std::string, std::shared_ptr<ColumnFileInfo>> _delete_files;
    std::atomic<bool> _manual_base_compaction = {false};
};
} // namespace baikaldb
