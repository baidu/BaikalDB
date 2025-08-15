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
#include "common.h"
#include "schema_factory.h"
#include "closure.h"
#include "region.h"
#include "sort_merge.h"
#include "region_column.h"
#include "parquet_writer.h"
#include "row2column.h"
#include "rocks_wrapper.h"
#include "rocksdb_filesystem.h"

namespace baikaldb {
DEFINE_int32(parquet_userid_statis_batch_count, 5, "parquet_userid_statis_batch_count(5)");  
DEFINE_int32(parquet_write_batch_length, 10000, "parquet_write_batch_length(1w)");  
DEFINE_int32(parquet_rowgroup_max_length, 1000000, "parquet_rowgroup_max_length(100w)");  
DEFINE_int32(parquet_file_max_length, 20000000, "parquet_file_max_length(2000w)");  
DEFINE_int32(raftlog_read_batch_size, 10000, "raftlog_read_batch_size");
DEFINE_int32(column_snapshot_timeout_s, 180, "column_snapshot_timeout_s");
DEFINE_bool(column_minor_compaction_use_acero, false, "column_minor_compaction_use_acero");
DEFINE_bool(column_major_compaction_use_acero, false, "column_major_compaction_use_acero");
DEFINE_int64(column_major_compaction_use_acero_max_rows, 2000000, "column_major_compaction_use_acero_max_rows, default(200w)");
DEFINE_int64(column_automatic_judgment_max_in_count, 100, "column_automatic_judgment_max_in_count, default(100)");
DEFINE_bool(parquet_read_use_userid_statis, true, "parquet_read_use_userid_statis");
DEFINE_int64(column_row2column_flush_delay_h, 6, "column_row2column_flush_delay_h, default(6)");
DEFINE_bool(column_cold_parquet_clear, false, "column_cold_parquet_clear");
#define IF_DONE_SET_RESPONSE(done, errcode, err_message) \
    do {\
        if (done != nullptr && ((ColumnOPClosure*)done)->response != nullptr) {\
            ((ColumnOPClosure*)done)->response->set_errcode(errcode);\
            ((ColumnOPClosure*)done)->response->set_errmsg(err_message);\
        }\
    }while (0);

struct SnapshotManager {
    SnapshotManager()
        : hot_snapshot(RocksWrapper::get_instance()->get_snapshot()),
        cold_snapshot(RocksWrapper::get_instance()->get_cold_snapshot()) {}
    ~SnapshotManager() {
        if (hot_snapshot != nullptr) {
            RocksWrapper::get_instance()->release_snapshot(hot_snapshot);
        }
        if (cold_snapshot != nullptr) {
            RocksWrapper::get_instance()->release_cold_snapshot(cold_snapshot);
        }
    }
    const rocksdb::Snapshot* hot_snapshot = nullptr;
    const rocksdb::Snapshot* cold_snapshot = nullptr;
};

std::shared_ptr<ColumnSchemaInfo> make_column_schema(int64_t tableid) {
    auto table = SchemaFactory::get_instance()->get_table_info_ptr(tableid);
    auto index = SchemaFactory::get_instance()->get_index_info_ptr(tableid);
    if (table == nullptr || index == nullptr) {
        DB_FATAL("table or index is null, tableid:%ld", tableid);
        return nullptr;
    }
    auto schema_ptr = std::make_shared<ColumnSchemaInfo>();
    schema_ptr->index_info = index;
    schema_ptr->table_info = table;
    schema_ptr->key_fields = index->fields;
    
    std::set<int> key_field_ids;
    for (const auto& field : index->fields) {
        key_field_ids.insert(field.id);
    }

    std::set<int> field_ids_need_sum;
    for (const auto& field : table->fields_need_sum) {
        field_ids_need_sum.insert(field.id);
    }

    int value_idx = schema_ptr->key_fields.size();
    schema_ptr->value_fields.reserve(table->fields.size() - index->fields.size());
    for (const auto& field : table->fields) {
        if (key_field_ids.count(field.id) > 0 || field.deleted) {
            continue;
        }

        schema_ptr->value_fields.emplace_back(field);
        if (field_ids_need_sum.count(field.id) > 0) {
            schema_ptr->need_sum_idx.insert(value_idx);
        }
        value_idx++;
    }

    schema_ptr->uniq_size = schema_ptr->key_fields.size();
    schema_ptr->keytype_idx = schema_ptr->key_fields.size() + schema_ptr->value_fields.size();
    schema_ptr->raft_index_idx = schema_ptr->keytype_idx + 1;
    schema_ptr->batch_pos_idx = schema_ptr->raft_index_idx + 1;

    std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
    arrow_fields.reserve(schema_ptr->batch_pos_idx + 1);
    for (const auto& field : schema_ptr->key_fields) {
        auto arrow_type = primitive_to_arrow_type(field.type);
        if (arrow_type < 0) {
            DB_COLUMN_FATAL("field: %s primitive type:%d to arrow type failed", field.lower_short_name.c_str(), field.type);
            return nullptr;
        }
        auto arrow_field = ColumnRecord::make_schema(field.lower_short_name, arrow::Type::type(arrow_type));
        if (arrow_field == nullptr) {
            DB_COLUMN_FATAL("field: %s make arrow schema failed", field.lower_short_name.c_str());
            return nullptr;
        }
        arrow_fields.emplace_back(arrow_field);
    }

    for (const auto& field : schema_ptr->value_fields) {
        auto arrow_type = primitive_to_arrow_type(field.type);
        if (arrow_type < 0) {
            DB_COLUMN_FATAL("field: %s primitive type:%d to arrow type failed", field.lower_short_name.c_str(), field.type);
            return nullptr;
        }
        auto arrow_field = ColumnRecord::make_schema(field.lower_short_name, arrow::Type::type(arrow_type));
        if (arrow_field == nullptr) {
            DB_COLUMN_FATAL("field: %s make arrow schema failed", field.lower_short_name.c_str());
            return nullptr;
        }
        arrow_fields.emplace_back(arrow_field);
    }

    arrow_fields.emplace_back(ColumnRecord::make_schema(KEY_TYPE_NAME, arrow::Type::type::INT32));
    schema_ptr->schema = std::make_shared<arrow::Schema>(arrow_fields);
    arrow_fields.emplace_back(ColumnRecord::make_schema(RAFT_INDEX_NAME, arrow::Type::type::INT64));
    arrow_fields.emplace_back(ColumnRecord::make_schema(BATCH_POS_NAME, arrow::Type::type::INT32));
    schema_ptr->schema_with_order_info = std::make_shared<arrow::Schema>(arrow_fields);
    return schema_ptr;
}

std::string files_name(const std::vector<std::shared_ptr<ColumnFileInfo>>& files) {
    std::ostringstream os;
    os << "[";
    for (const auto& file : files) {
        if (file != nullptr && !file->file_name.empty()) {
            os << file->file_name << ",";
        }
    }
    os << "]";
    return os.str();
}

int Region::column_on_snapshot_load_restart() {
    return _column_mgr.load_snapshot(true);
}

int Region::column_on_snapshot_load(const std::string& dir) {
    std::string meta_file = dir + "/" + SNAPSHOT_PARQUET_META_FILE;
    boost::filesystem::path meta_path(meta_file);
    pb::RegionColumnFiles tmp_pb_file_info;
    bool has_rocksdb_meta = false;
    int ret = MetaWriter::get_instance()->read_column_file_info(_region_id, tmp_pb_file_info);
    if (ret == -2) {
        has_rocksdb_meta = false;
    } else if (ret == 0) {
        has_rocksdb_meta = true;
    } else {
        DB_COLUMN_FATAL("region_id: %ld, read column file info fail", _region_id);
        return -1;
    }
    if (!boost::filesystem::exists(meta_path)) {
        DB_WARNING("meata file: %s not exist", meta_file.c_str());
        if (has_rocksdb_meta && tmp_pb_file_info.column_status() != pb::CS_COLD) {
            MetaWriter::get_instance()->clear_column_file_info(_region_id);
        }
        _column_mgr.load_snapshot(false);
        return 0;
    }

    std::string column_dir = ColumnFileInfo::make_column_file_directory(_region_id, get_table_id());
    boost::filesystem::path output_path(column_dir);
    if (boost::filesystem::exists(output_path)) {
        // 清空目录
        boost::filesystem::remove_all(output_path);
    } 
    // 创建目录
    if (!boost::filesystem::create_directories(output_path)) {
        DB_COLUMN_FATAL("FATAL create output_path: %s fail.", column_dir.c_str());
        return -1;
    }

    std::ifstream meta_ifs(meta_file);
    std::string meta_string((std::istreambuf_iterator<char>(meta_ifs)),
            std::istreambuf_iterator<char>());
    
    pb::RegionColumnFiles pb_file_info;
    if (!pb_file_info.ParseFromString(meta_string)) {
        DB_FATAL("parse from pb fail when read column file info, value:%s", pb_file_info.ShortDebugString().c_str());
        return -1;
    }

    DB_NOTICE("region_id: %ld, %s", _region_id, pb_file_info.ShortDebugString().c_str());

    for (const auto& file : pb_file_info.active_files()) {
        if (file.is_afs_file()) {
            continue;
        }
        const std::string& file_name = file.file_name();
        const std::string from_file_name = dir + "/" + file_name;
        const std::string to_file_name = column_dir + "/" + file_name;
        boost::filesystem::path from_path(from_file_name);
        boost::filesystem::path to_path(to_file_name);
        if (!boost::filesystem::exists(from_path) || boost::filesystem::exists(to_path)) {
            DB_COLUMN_FATAL("file: %s", from_file_name.c_str());
            return -1;
        }

        // 替换成boost TODO
        if (::link(from_file_name.c_str(), to_file_name.c_str()) != 0) {
            DB_COLUMN_FATAL("link failed, %s to %s", from_file_name.c_str(), to_file_name.c_str());
            return -1;
        }
    }

    ret = MetaWriter::get_instance()->write_column_file_info(_region_id, pb_file_info);
    if (ret < 0) {
        DB_COLUMN_FATAL("region_id: %ld, write column file failed", _region_id);
        return -1;
    }

    return _column_mgr.load_snapshot(false);
}


void Region::column_on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (_column_mgr.column_status() == pb::CS_INVALID) {
        return;
    }

    if (_column_mgr.column_status() == pb::CS_COLD || _column_mgr.max_version() >= _applied_index) {
        std::string snapshot_path = writer->get_path();
        std::vector<std::string> files;
        if (column_snapshot_save(snapshot_path, files) != 0) {
            done->status().set_error(EINVAL, "Fail to save column snapshot");
            return;
        }

        for (const auto& file : files) {
            if (writer->add_file(file) != 0) {
                done->status().set_error(EINVAL, "Fail to add snapshot");
                DB_WARNING("Error while adding extra_fs to writer, region_id: %ld, file: %s", _region_id, file.c_str());
            }
        }
        return;
    }

    // 等待minor compaction完成
    {
        std::unique_lock<std::mutex> l(_snapshot_closure_mutex);
        done_guard.release();
        _snapshot_closure.reset(new ColumnSnapshotClosure(_region_id, _applied_index, writer, done));
    }
    DB_WARNING("region_id: %ld, async column snapshot", _region_id);
}

void Region::column_snapshot_save() {
    std::unique_ptr<ColumnSnapshotClosure> closure;
    {
        std::unique_lock<std::mutex> l(_snapshot_closure_mutex);
        if (_snapshot_closure == nullptr) {
            return;
        }

        if (_column_mgr.max_version() < _snapshot_closure->get_raft_index() && _snapshot_closure->get_time() < FLAGS_column_snapshot_timeout_s * 1000 * 1000LL) {
            // 没有达到snapshot raft index，并且没有超时，等待下一轮处理
            return;
        }

        closure = std::move(_snapshot_closure);
    }

    if (_column_mgr.max_version() < closure->get_raft_index()) {
        DB_WARNING("snapshot time out region_id: %ld, max_version: %ld < snapshot_raft_index: %ld", _region_id, _column_mgr.max_version(), closure->get_raft_index()); 
        return;
    }

    auto writer = closure->writer();
    std::vector<std::string> files;
    if (column_snapshot_save(writer->get_path(), files) != 0) {
        DB_WARNING("Fail to save column snapshot, region_id: %ld", _region_id);
        return;
    }

    for (const auto& file : files) {
        if (writer->add_file(file) != 0) {
            DB_COLUMN_FATAL("Error while adding extra_fs to writer, region_id: %ld, file: %s", _region_id, file.c_str());
            return;
        }
    }
    closure->set_success();
    return;
}

// 列存文件保存快照
int Region::column_snapshot_save(const std::string& snapshot_path, std::vector<std::string>& files) {
    std::shared_ptr<ColumnFileSet> column_file_set = nullptr;
    pb::RegionColumnFiles pb_file_info;
    _column_mgr.make_snapshot(&column_file_set, pb_file_info);
    for (const auto& [_, f] : column_file_set->column_files) {
        if (f->is_afs_file) {
            continue;
        }
        const std::string& file_path = f->full_path();
        std::vector<std::string> split_vec;
        boost::split(split_vec, file_path, boost::is_any_of("/"));
        const std::string& file_name = split_vec.back();
        // 建立硬链接
        const std::string& link_path = snapshot_path + "/" + file_name;
        if (::link(file_path.c_str(), link_path.c_str()) != 0) {
            DB_COLUMN_FATAL("Fail to link path, region_id: %ld, file_path[%s]->link_path[%s]", 
                        _region_id, file_path.c_str(), link_path.c_str());
            return -1;
        }
        files.emplace_back(file_name);
        DB_NOTICE("region_id: %ld file_path: %s, link_path: %s", _region_id, file_path.c_str(), link_path.c_str());
    }

    std::string parquet_meta_file = snapshot_path + "/" + SNAPSHOT_PARQUET_META_FILE;
    try {
        std::string value;
        if (!pb_file_info.SerializeToString(&value)) {
            DB_COLUMN_FATAL("file_info: %s serialize to string fail", pb_file_info.ShortDebugString().c_str()); 
            return -1;
        }
        std::ofstream ofs(parquet_meta_file, std::ofstream::app);
        if (!ofs.is_open()) {
            DB_COLUMN_FATAL("open file %s failed", parquet_meta_file.c_str());
            return -1;
        }

        ofs << value;
        ofs.close();
    } catch (const std::exception& e) {
        DB_COLUMN_FATAL("region_id: %ld, append file: %s failed", _region_id, parquet_meta_file.c_str());
        return -1;
    }
    files.emplace_back(SNAPSHOT_PARQUET_META_FILE);
    return 0;
}

int Region::get_column_files(const std::vector<pb::PossibleIndex::Range>& key_ranges, std::vector<std::shared_ptr<ParquetFile>>& files) {
    std::shared_ptr<ColumnFileSet> column_file_set = _column_mgr.get_column_fileset();
    DB_DEBUG("region_id: %ld, get column files, key_ranges size: %ld", _region_id, key_ranges.size());
    for (const auto& [_, info] : column_file_set->column_files) {
        if (!key_ranges.empty()) {
            bool is_overlap = false;
            for (const auto& range : key_ranges) {
                if (ParquetFile::check_interval_overlapped(range, info->start_key, info->end_key)) {
                    is_overlap = true;
                    break;
                }
            }

            if (!is_overlap) {
                continue;
            }
        }

        auto file = ParquetFileManager::get_instance()->get_parquet_file(info);
        if (file == nullptr) {
            files.clear();
            DB_WARNING("open file:%s failed", info->full_path().c_str());
            return -1;
        }
        DB_NOTICE("open file:%s success", info->full_path().c_str());
        files.emplace_back(file);
    }

    return 0;
}

bool Region::use_userid_statis(bool is_eq, const google::protobuf::RepeatedPtrField<pb::PossibleIndex::Range>& key_ranges) {
    if (!FLAGS_parquet_read_use_userid_statis) {
        return true;
    }
    auto index = SchemaFactory::get_instance()->get_index_info_ptr(get_table_id());
    if (index == nullptr) {
        DB_FATAL("index is null, tableid:%ld", get_table_id());
        return false;
    }
    const FieldInfo& field_info = index->fields[0];
    if (field_info.lower_short_name != "userid") {
        return false;
    }

    std::set<int64_t> userids;
    for (const auto& range : key_ranges) {
        TableKey left_key(range.left_key());
        TableKey right_key(is_eq && range.right_key().empty() ? range.left_key() : range.right_key());
        ExprValue left_value;
        ExprValue right_value;
        int pos = 0;
        if (0 != left_key.decode_field_for_chunk(&left_value, field_info, pos)) {
            return false;
        }
        pos = 0;
        if (0 != right_key.decode_field_for_chunk(&right_value, field_info, pos)) {
            return false;
        }
        if (!left_value.is_numberic() || !right_value.is_numberic()) {
            return false;
        }
        if (left_value.compare(right_value) != 0) {
            // 区间查询
            return true;
        }

        int64_t userid = left_value.get_numberic<int64_t>();
        userids.insert(userid);
    }

    return _column_mgr.match_userid_statis(userids);
}

bool Region::use_column_storage(const pb::Plan& plan, SmartTable table) {
    if (!_column_mgr.column_storage_valid()) {
        return false;
    } 

    for (const auto& node : plan.nodes()) {
        if (node.derive_node().has_scan_node()) {
            const auto& scan_node = node.derive_node().scan_node();
            pb::PossibleIndex pos_index;
            pos_index.ParseFromString(scan_node.indexes(0));
            if (pos_index.index_id() != get_table_id()) {
                return false;
            }

            if (table->schema_conf.force_column_storage()) {
                return true;
            }   

            if (pos_index.ranges_size() == 0) {
                return true;
            }
            bool is_eq = pos_index.is_eq();
            for (const auto& range : pos_index.ranges()) {
                if (!is_eq && (range.right_key().empty() || range.left_key().empty())) {
                    // > 或 <
                    return true;
                }
            }

            // in过多时不使用列存
            if (pos_index.ranges_size() > FLAGS_column_automatic_judgment_max_in_count) {
                return false;
            }

            return use_userid_statis(is_eq, pos_index.ranges());
        }
    }

    return false;
}

// 将parquet文件link到region目录
std::vector<std::shared_ptr<ColumnFileInfo>> Region::column_link_files(const std::vector<ColumnFileMeta>& file_infos, int64_t min_version, int64_t max_version) {
    std::vector<std::shared_ptr<ColumnFileInfo>> new_files;
    new_files.reserve(file_infos.size());
    int file_idx = 0;
    bool failed = false;
    for (const auto& info : file_infos) {
        auto new_file = std::make_shared<ColumnFileInfo>(get_table_id(), _region_id, min_version, max_version, file_idx++, info);
        boost::filesystem::path old_path(info.file_name);
        boost::filesystem::path new_path(new_file->full_path());
        if (boost::filesystem::exists(new_path) || !boost::filesystem::exists(old_path)) {
            DB_COLUMN_FATAL("old file: %s or new file: %s exist", info.file_name.c_str(), new_file->full_path().c_str());
            break;
        }
        if (!ParquetFileManager::get_instance()->link_file(info.file_name, new_file->full_path())) {
            DB_COLUMN_FATAL("link file: %s to path: %s failed", info.file_name.c_str(), new_file->full_path().c_str());
            failed = true;
            break;
        } else if (boost::filesystem::file_size(boost::filesystem::path(new_file->full_path())) != info.file_size) {
            DB_COLUMN_FATAL("file: %s size is not equal to file: %s", info.file_name.c_str(), new_file->full_path().c_str());
            failed = true;
            break;
        } else {
            DB_NOTICE("link file: %s to path: %s success", info.file_name.c_str(), new_file->full_path().c_str());
        }
        new_files.emplace_back(new_file);
    }

    if (failed) {
        // 失败则删除新文件
        for (const auto& new_file : new_files) {
            boost::filesystem::path new_path(new_file->full_path());
            boost::filesystem::remove(new_path);
        }
        return {};
    } else {
        return new_files;
    }
}

void Region::column_delete_files(const std::vector<std::shared_ptr<ColumnFileInfo>>& files) {
    for (auto file : files) {
        if (file->is_afs_file) {
            continue;
        }
        boost::filesystem::path path(file->full_path());
        if (boost::filesystem::exists(path)) {
            DB_NOTICE("delete column file: %s", file->full_path().c_str());
            boost::filesystem::remove(path);
        }
    }
}

bool Region::can_do_column_compact() {
    if (!_column_mgr.has_init()) {
        // 等待snapshot load完成
        return false;
    }

    // NORMAL状态才做column compaction
    if (_column_mgr.column_status() != pb::CS_NORMAL) {
        return false;
    }

    return true;
}

// 定时执行, 快速将raft log 刷成parquet文件
int Region::column_minor_compact() {
    TimeCost cost;
    std::shared_ptr<ColumnSchemaInfo> schema_info = make_column_schema(get_table_id());
    if (schema_info == nullptr) {
        DB_FATAL("region_id: %ld, make column schema failed", _region_id);
        return -1;
    }
    auto snapshot = std::make_shared<SnapshotManager>();
    rocksdb::ReadOptions rocksdb_options;
    rocksdb_options.snapshot = snapshot->hot_snapshot;
    int64_t applied_index;
    int64_t data_index;
    _meta_writer->read_applied_index(_region_id, rocksdb_options, &applied_index, &data_index);
    if (applied_index < 0 || data_index < 0) {
        return -1;
    }

    int64_t start_version = 0;
    if (_column_mgr.pick_minor_compact_file(applied_index, start_version) != 0) {
        return -1;
    }

    if (data_index < start_version) {
        // 仅更新version
        DB_WARNING("region_id: %ld, data index: %ld, applied index: %ld, start version: %ld only update version", 
            _region_id, data_index, applied_index, start_version);
        _column_mgr.update_version_only(applied_index, start_version - 1);
        return 0;
    }

    Row2ColOptions raftlog_options;
    raftlog_options.region_id   = _region_id;
    raftlog_options.table_id    = get_table_id();
    raftlog_options.read_batch_size = FLAGS_raftlog_read_batch_size;
    raftlog_options.snapshot    = snapshot->hot_snapshot;
    raftlog_options.start_index = start_version;
    raftlog_options.end_index   = applied_index;
    raftlog_options.schema_info = schema_info;
    auto raftlog_reader = std::make_shared<RaftLogReader>(raftlog_options);
    int ret = raftlog_reader->init();
    if (ret < 0) {
        DB_COLUMN_FATAL("region_id: %ld, init raftlog reader failed", _region_id);
        return -1;
    }

    if (raftlog_reader->row_count() <= 0) {
        DB_WARNING("region_id: %ld, no need to do minor compact", _region_id);
        // raftlog为空，不需要做minor compact，仅修改version
        _column_mgr.update_version_only(applied_index, start_version - 1);
        return 0;
    }

    int64_t raft_index = raftlog_reader->get_last_raft_index();
    if (raft_index < start_version) {
        return -1;
    }

    applied_index = raft_index;
    int64_t write_batch_length = FLAGS_parquet_write_batch_length;
    std::unique_ptr<arrow::RecordBatchReader> rb_reader;
    if (raftlog_reader->put_count() <= 0 && raftlog_reader->delete_count() <= 0 && FLAGS_column_minor_compaction_use_acero) {
        AceroMergeOptions acero_options;
        acero_options.batch_size = write_batch_length;
        acero_options.schema_info = schema_info;
        rb_reader.reset(new AceroMerge(acero_options, {raftlog_reader}));
    } else {
        auto unorder_reader = std::make_shared<UnOrderSingleRowReader>(raftlog_reader, schema_info.get(), raftlog_reader->row_count());
        SortMergeOptions merge_options;
        merge_options.batch_size = write_batch_length;
        merge_options.is_base_compact = false;
        merge_options.schema_info = schema_info;
        rb_reader.reset(new SortMerge(merge_options, {unorder_reader}));
    }

    ParquetWriteOptions parquet_options;
    parquet_options.userid_statis_batch_count = FLAGS_parquet_userid_statis_batch_count;
    parquet_options.write_batch_length = write_batch_length;
    parquet_options.max_file_rows = INT64_MAX; // 确保minor compaction生成的文件只有一个
    parquet_options.max_row_group_length = (FLAGS_parquet_rowgroup_max_length / write_batch_length) * write_batch_length; // 保证row group长度是write_batch_length的整数倍
    parquet_options.schema_info = schema_info;
    auto writer = std::make_shared<ParquetWriter>(parquet_options);
    auto s = writer->execute(std::move(rb_reader), MINOR, [this]() { return is_shutdown(); });
    if (!s.ok()) {
        DB_WARNING("region_id: %ld, execute parquet writer failed: %s", _region_id, s.message().c_str());
        return -1;
    }

    auto file_infos = writer->get_file_infos();
    // minor compaction确保输出一个文件
    if (file_infos.size() != 1) {
        DB_COLUMN_FATAL("region_id: %ld, compaction generate more than one file size: %ld", _region_id, file_infos.size());
        return -1;
    }

    auto new_files = column_link_files(file_infos, start_version, applied_index);
    if (new_files.size() != 1) {
        DB_COLUMN_FATAL("region_id: %ld, link file failed", _region_id);
        return -1;
    }

    ret = _column_mgr.finish_minor_compact(new_files[0], start_version - 1);
    if (ret != 0) {
        column_delete_files(new_files);
        return -1;
    }
    auto new_files_name = files_name(new_files);
    DB_NOTICE("region_id: %ld, minor compact success, read lines[%ld, %ld, %ld], write lines: %ld, use_acero_flag: %d, cost: %ld, new_files: %s", 
        _region_id, raftlog_reader->row_count(), raftlog_reader->put_count(), raftlog_reader->delete_count(), writer->row_count(), 
        FLAGS_column_minor_compaction_use_acero, cost.get_time(), new_files_name.c_str());
    return 0;
}

void Region::column_flush() {
    auto table = _factory->get_table_info_ptr(get_table_id());
    if (table == nullptr) {
        return;
    }

    if (!table->schema_conf.has_enable_column_engine()) {
        return;
    }

    if (!_column_mgr.has_init()) {
        return;
    }

    // 回收老版本数据
    _column_mgr.column_file_gc();

    if (!table->schema_conf.enable_column_engine()) {
        // 列存关闭，清理column_file
        if (_column_mgr.column_status() == pb::CS_COLD) {
            if (FLAGS_column_cold_parquet_clear) {
                _column_mgr.remove_column_data(pb::CS_INVALID, 0);
            }
        } else {
            _column_mgr.remove_column_data(pb::CS_INVALID, 0);
        }
        return;
    }

    if (_column_mgr.column_status() == pb::CS_INVALID) {
        if (_olap_state.load() == pb::OLAP_TRUNCATED && !is_leader()) {
            // 冷数据只有leader做行转列
            DB_WARNING("region_id: %ld, cold region only leader do row2column", _region_id);
            return;
        }
        if (table->is_range_partition && table->partition_info.has_dynamic_partition_attr()) {
            const pb::PrimitiveType& partition_col_type = table->partition_info.field_info().mysql_type();
            const auto& dynamic_partition_attr = table->partition_info.dynamic_partition_attr();
            const std::string& time_unit = dynamic_partition_attr.time_unit();
            if (boost::algorithm::iequals(time_unit, "DAY") && partition_col_type == pb::DATE) {
                RangePartition* partition_ptr = static_cast<RangePartition*>(table->partition_ptr.get());
                const auto range = partition_ptr->partition_range(get_partition_id());
                ExprValue left_value = range.left_value;
                if (!left_value.is_null()) {
                    left_value.cast_to(pb::TIMESTAMP);
                    time_t ts = left_value._u.uint32_val;
                    time_t now = time(nullptr);
                    if (now > ts && now - ts < (24 + FLAGS_column_row2column_flush_delay_h) * 3600) {
                        // 当天分区写入量大不做转列
                        return;
                    }
                }
            }
        }
        column_base_row2column();
    }

    if (_olap_state.load() == pb::OLAP_TRUNCATED && _column_mgr.can_flush_to_cold() && is_leader() ) {
        // flush to cold
        column_flush_to_cold();
    }
}

void Region::column_major_compact(bool is_base) {
    TimeCost cost;
    std::shared_ptr<ColumnSchemaInfo> schema_info = make_column_schema(get_table_id());
    if (schema_info == nullptr) {
        DB_FATAL("region_id: %ld, get schema info failed", _region_id);
        return;
    }
    std::vector<std::shared_ptr<ColumnFileInfo>> old_files;
    int ret = 0;
    if (is_base) {
        ret = _column_mgr.pick_base_compact_file(old_files);
    } else {
        ret = _column_mgr.pick_major_compact_file(old_files);
    }
    if (ret != 0) {
        return;
    }

    bool can_use_acero = true;
    int64_t total_rows = 0;
    int64_t min_version = INT64_MAX;
    int64_t max_version = -1;
    std::vector<std::shared_ptr<::arrow::RecordBatchReader>> parquet_readers;
    for (const auto& file : old_files) {
        if (file->start_version != 0 && (file->put_count > 0 || file->delete_count > 0)) {
            can_use_acero = false;
        }
        total_rows += file->row_count;
        min_version = is_base ? 0 : std::min(file->start_version, min_version);
        max_version = std::max(file->end_version, max_version);
        ParquetFileReaderOptions options;
        options.raftindex = file->end_version;
        options.schema_info = schema_info;
        options.file_info = file;
        auto parquet_reader = std::make_shared<ParquetFileReader>(options);
        parquet_readers.emplace_back(parquet_reader);
    }

    if (total_rows > FLAGS_column_major_compaction_use_acero_max_rows) {
        can_use_acero = false;
    }

    std::unique_ptr<arrow::RecordBatchReader> rb_reader;
    int64_t write_batch_length = FLAGS_parquet_write_batch_length;
    if (can_use_acero && FLAGS_column_major_compaction_use_acero) {
        AceroMergeOptions acero_options;
        acero_options.batch_size = write_batch_length;
        acero_options.schema_info = schema_info;
        rb_reader.reset(new AceroMerge(acero_options, parquet_readers));
    } else {
        std::vector<std::shared_ptr<SingleRowReader>> single_row_readers;
        SortMergeOptions merge_options;
        merge_options.batch_size = write_batch_length;
        merge_options.is_base_compact = is_base;
        merge_options.schema_info = schema_info;
        for (auto& r : parquet_readers) {
            single_row_readers.emplace_back(std::make_shared<OrderSingleRowReader>(r, schema_info.get()));
        }
        rb_reader.reset(new SortMerge(merge_options, single_row_readers));
    }

    ParquetWriteOptions parquet_options;
    parquet_options.userid_statis_batch_count = FLAGS_parquet_userid_statis_batch_count;
    parquet_options.write_batch_length = write_batch_length;
    parquet_options.max_file_rows = is_base ? FLAGS_parquet_file_max_length : INT64_MAX;
    parquet_options.max_row_group_length = (FLAGS_parquet_rowgroup_max_length / write_batch_length) * write_batch_length; // 保证row group长度是write_batch_length的整数倍
    parquet_options.schema_info = schema_info;
    auto writer = std::make_shared<ParquetWriter>(parquet_options);
    auto s = writer->execute(std::move(rb_reader), is_base ? BASE : MAJOR, [this]() { return is_shutdown(); });
    if (!s.ok()) {
        DB_WARNING("region_id: %ld, execute parquet writer failed: %s", _region_id, s.message().c_str());
        return;
    }

    auto file_infos = writer->get_file_infos();
    if (file_infos.empty() || (!is_base && file_infos.size() > 1)) {
        DB_COLUMN_FATAL("region_id: %ld, no column file generated after compaction", _region_id);
        return;
    }

    auto new_files = column_link_files(file_infos, min_version, max_version);
    if (new_files.size() != file_infos.size()) {
        DB_COLUMN_FATAL("region_id: %ld, link file failed", _region_id);
        return;
    }
    if (_column_mgr.finish_major_compact(old_files, new_files, is_base) != 0) {
        column_delete_files(new_files);
        DB_COLUMN_FATAL("region_id: %ld, finish major compact failed", _region_id);
        return;
    }
    auto new_files_name = files_name(new_files);
    auto old_files_name = files_name(old_files);
    DB_NOTICE("region_id: %ld, major compact success, is_base: %d, write lines: %ld, cost: %ld, old files: %s, new files: %s", 
        _region_id, is_base, writer->row_count(), cost.get_time(), old_files_name.c_str(), new_files_name.c_str());
}

void Region::column_base_row2column() {
    std::string column_dir = ColumnFileInfo::make_column_file_directory(_region_id, get_table_id());
    boost::filesystem::path output_path(column_dir);
    if (!boost::filesystem::exists(output_path)) {
        // 创建目录
        if (!boost::filesystem::create_directories(output_path)) {
            DB_COLUMN_FATAL("FATAL create output_path: %s fail.", column_dir.c_str());
            return;
        }
    }
    TimeCost cost;
    std::shared_ptr<ColumnSchemaInfo> schema_info = make_column_schema(get_table_id());
    if (schema_info == nullptr) {
        DB_FATAL("make column schema failed, region_id: %ld", _region_id);
        return;
    }

    auto snapshot = std::make_shared<SnapshotManager>();
    rocksdb::ReadOptions rocksdb_options;
    rocksdb_options.snapshot = snapshot->hot_snapshot;
    int64_t applied_index;
    int64_t data_index;
    _meta_writer->read_applied_index(_region_id, rocksdb_options, &applied_index, &data_index);
    if (applied_index < 0 || data_index < 0) {
        DB_FATAL("region_id: %ld, read applied index failed", _region_id);
        return;
    }

    int64_t write_batch_length = FLAGS_parquet_write_batch_length;
    auto begin_stats = _olap_state.load();
    Row2ColOptions base_options;
    base_options.region_id = _region_id;
    base_options.table_id = get_table_id();
    base_options.read_batch_size = write_batch_length;
    base_options.start_index = 0;
    base_options.end_index = applied_index;
    if (_olap_state.load() >= pb::OLAP_FLUSHED) {
        base_options.is_cold_rocksdb = true;
        base_options.snapshot = snapshot->cold_snapshot;
    } else {
        base_options.is_cold_rocksdb = false;
        base_options.snapshot = snapshot->hot_snapshot;
    }
    base_options.schema_info = schema_info;
    std::unique_ptr<arrow::RecordBatchReader> reader(new RocksdbBaseReader(base_options));

    ParquetWriteOptions parquet_options;
    parquet_options.userid_statis_batch_count = FLAGS_parquet_userid_statis_batch_count;
    parquet_options.write_batch_length = write_batch_length;
    parquet_options.max_file_rows = FLAGS_parquet_file_max_length;
    parquet_options.max_row_group_length = (FLAGS_parquet_rowgroup_max_length / write_batch_length) * write_batch_length; // 保证row group长度是write_batch_length的整数倍
    parquet_options.schema_info = schema_info;
    auto writer = std::make_shared<ParquetWriter>(parquet_options);
    auto s = writer->execute(std::move(reader), ROW2COL, [this]() { return is_shutdown(); });
    if (!s.ok()) {
        DB_WARNING("region_id: %ld, execute parquet writer failed: %s", _region_id, s.message().c_str());
        return;
    }

    if (_olap_state.load() != begin_stats) {
        // 如果olap stats发生变化则可能读取的数据不完整
        DB_COLUMN_FATAL("region_id: %ld, olap state changed during compacting", _region_id);
        return;
    }

    auto file_infos = writer->get_file_infos();
    if (file_infos.empty()) {
        _column_mgr.finish_row2column({}, applied_index);
        DB_WARNING("no column file generated after row2column, region_id: %ld", _region_id);
        return;
    }

    auto new_files = column_link_files(file_infos, 0, applied_index);
    if (new_files.size() != file_infos.size()) {
        DB_COLUMN_FATAL("region_id: %ld, link file failed", _region_id);
        return;
    }

    _column_mgr.finish_row2column(new_files, applied_index);
    auto new_files_name = files_name(new_files);
    DB_NOTICE("region_id: %ld, row2column compacted, row_count: %ld, applied_index: %ld, cost: %ld, new files: %s", 
            _region_id, writer->row_count(), applied_index, cost.get_time(), new_files_name.c_str());
}

void Region::apply_column_info(const pb::StoreReq& request, braft::Closure* done) {
    const auto& column_files = request.extra_req().column_info().column_files();
    TimeCost cost;
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");

    std::vector<std::shared_ptr<ColumnFileInfo>> new_files;
    new_files.reserve(column_files.active_files_size());
    for (const auto& file : column_files.active_files()) {
        auto new_file = std::make_shared<ColumnFileInfo>(file);
        new_files.emplace_back(new_file);
    }

    _column_mgr.finish_flush_to_cold(new_files, column_files.max_version());

   _meta_writer->update_apply_index(_region_id, _applied_index, _data_index);
    DB_NOTICE("request: %s, cost: %ld", request.ShortDebugString().c_str(), cost.get_time());
    return;
}

int Region::sync_column_info(const std::vector<std::shared_ptr<ColumnFileInfo>>& new_column_files) {
    TimeCost tc;
    pb::StoreReq req;
    pb::StoreRes res;
    req.set_op_type(pb::OP_COLUMN_INFO);
    req.set_region_id(_region_id);
    req.set_region_version(get_version());
    auto column_info = req.mutable_extra_req()->mutable_column_info();
    auto column_files = column_info->mutable_column_files();
    column_files->set_max_version(_applied_index);
    column_files->set_column_status(pb::CS_COLD);
    for (const auto& mem_file : new_column_files) {
        auto pb_file = column_files->add_active_files();
        ColumnFileManager::fileinfo2proto(mem_file, pb_file);
    }

    butil::IOBuf data;
    butil::IOBufAsZeroCopyOutputStream wrapper(&data);
    if (!req.SerializeToZeroCopyStream(&wrapper)) {
        DB_FATAL("serializeToString fail, region_id: %ld", _region_id);  
        return -1;
    }

    BthreadCond cond;
    ColumnOPClosure* c = new ColumnOPClosure(&cond);
    c->response = &res;
    braft::Task task; 
    task.data = &data; 
    task.done = c;
    cond.increase();
    _node.apply(task);
    cond.wait();
    if (res.errcode() != pb::SUCCESS) {
        DB_COLUMN_FATAL("region_id: %ld, sync column info: %s failed, response: %s", 
                _region_id, req.ShortDebugString().c_str(), res.ShortDebugString().c_str());
        return -1;
    }

    DB_NOTICE("region_id: %ld sync column info: %s cost: %ld", _region_id, req.ShortDebugString().c_str(), tc.get_time());
    return 0;
}

void Region::column_flush_to_cold() {
    std::shared_ptr<ColumnFileSet> column_file_set = _column_mgr.get_column_fileset();
    bool failed = false;
    std::vector<std::shared_ptr<ColumnFileInfo>> new_column_files;
    new_column_files.reserve(column_file_set->column_files.size());
    std::string path = make_make_relative_path_without_filename("baikal_column", get_table_id(), get_partition_id());
    if (path.empty()) {
        return;
    }
    for (const auto& [_, old_file] : column_file_set->column_files) {
        if (old_file->is_afs_file) {
            DB_COLUMN_FATAL("region_id: %ld, file:%s is already in afs", _region_id, old_file->full_path().c_str());
            failed = true;
            break;
        }

        auto new_file = std::make_shared<ColumnFileInfo>(old_file);
        new_file->is_afs_file = true;
        int ret = copy_file(old_file->full_path(), path + "/" + new_file->file_name, new_file->afs_full_name, old_file->file_size);
        if (ret != 0) {
            DB_FATAL("copy file to afs failed, region_id: %ld, old_file:%s", _region_id, old_file->full_path().c_str());
            failed = true;
            break;
        }

        new_column_files.emplace_back(new_file);
    }

    if (failed) {
        // 失败后，需要把之前拷贝到afs的文件删除
        auto fs = SstExtLinker::get_instance()->get_exteranl_filesystem();
        for (const auto& file : new_column_files) {
            DB_WARNING("delete external_file: %s", file->afs_full_name.c_str());
            fs->delete_path(file->afs_full_name, false);
        }
        return;
    }

    if (new_column_files.empty()) {
        DB_COLUMN_FATAL("region_id: %ld, no column file need to flush", _region_id);
        return;
    }

    // 同步结果到其他peer
    sync_column_info(new_column_files);
}

} // namespance baikaldb