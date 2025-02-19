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
#include "region.h"
#include <algorithm>
#include <fstream>
#include <boost/filesystem.hpp>
#include "table_key.h"
#include "runtime_state.h"
#include "mem_row_descriptor.h"
#include "exec_node.h"
#include "table_record.h"
#include "rpc_sender.h"
#include "concurrency.h"
#include "store.h"
#include "closure.h"
#include "rocksdb_filesystem.h"

namespace baikaldb {
DECLARE_string(meta_server_bns);
DECLARE_string(db_path);
DEFINE_int32(hot_rocksdb_truncate_interval_s, 600, "hot rocksdb truncate interval(s)");
DEFINE_int32(num_lines_per_sst, 10000000, "num_lines_per_sst(1000w)");

#define IF_DONE_SET_RESPONSE(done, errcode, err_message) \
    do {\
        if (done != nullptr && ((OlapOPClosure*)done)->response != nullptr) {\
            ((OlapOPClosure*)done)->response->set_errcode(errcode);\
            ((OlapOPClosure*)done)->response->set_errmsg(err_message);\
        }\
    }while (0);
#define IF_DONE_SET_RESPONSE_FATAL(done, errcode, err_message) \
    do {\
        DB_FATAL("region_id: %ld %s", _region_id, err_message);\
        if (done != nullptr && ((OlapOPClosure*)done)->response != nullptr) {\
            ((OlapOPClosure*)done)->response->set_errcode(errcode);\
            ((OlapOPClosure*)done)->response->set_errmsg(err_message);\
        }\
    }while (0);
    
bool Region::need_flush_to_cold_rocksdb() {
    if (!is_leader()) {
        return false;
    }

    if (get_version() == 0) {
        return false;
    }

    if (get_timecost() < FLAGS_hot_rocksdb_truncate_interval_s * 1000000LL) {
        return false;
    }
    if (_rollup_region_init_index != -1) {
        return false;
    }
    // 检查raft状态相当比较健康时再处理flush
    braft::NodeStatus status;
    _node.get_status(&status);
    if (!status.unstable_followers.empty() || status.stable_followers.empty()) {
        DB_WARNING("region_id: %ld has unstable followers", _region_id);
        return false;
    }

    for (const auto& iter : status.stable_followers) {
        if (iter.second.installing_snapshot) {
            DB_WARNING("region_id: %ld %s installing_snapshot", _region_id, iter.first.to_string().c_str());
            return false;
        }
    }

    auto table_ptr = _factory->get_table_info_ptr(get_table_id());
    if (table_ptr == nullptr || !table_ptr->is_range_partition || table_ptr->partition_ptr == nullptr) {
        return false;
    }

    if (table_ptr->partition_info.range_partition_infos_size() <= 0) {
        return false;
    }
    int64_t partition_id = get_partition_id();
    for (auto it = table_ptr->partition_info.range_partition_infos().rbegin(); it != table_ptr->partition_info.range_partition_infos().rend(); ++it) {
        if (it->partition_id() == partition_id) {
            if (it->is_cold()) {
                return true;
            } else {
                return false;
            }
        }
    }

    return false;
}

// struct LiveFileMetaData : SstFileMetaData {
//   std::string column_family_name;  // Name of the column family
//   int level;                       // Level at which this file resides.
//   LiveFileMetaData() : column_family_name(), level(0) {}
// };

/*
struct SstFileMetaData : public FileStorageInfo {


  SequenceNumber smallest_seqno = 0;  // Smallest sequence number in file.
  SequenceNumber largest_seqno = 0;   // Largest sequence number in file.
  std::string smallestkey;            // Smallest user defined key in the file.
  std::string largestkey;             // Largest user defined key in the file.
  uint64_t num_reads_sampled = 0;     // How many times the file is read.
  bool being_compacted =
      false;  // true if the file is currently being compacted.

  uint64_t num_entries = 0;
  uint64_t num_deletions = 0;

  // Timestamp when the SST file is created, provided by
  // SystemClock::GetCurrentTime(). 0 if the information is not available.
  uint64_t file_creation_time = 0;

  // DEPRECATED: The name of the file within its directory with a
  // leading slash (e.g. "/123456.sst"). Use relative_filename from base struct
  // instead.
  std::string name;

  // DEPRECATED: replaced by `directory` in base struct
  std::string db_path;
};
*/
/*
struct FileStorageInfo {
  // The name of the file within its directory (e.g. "123456.sst")
  std::string relative_filename;
  // The directory containing the file, without a trailing '/'. This could be
  // a DB path, wal_dir, etc.
  std::string directory;

  // The id of the file within a single DB. Set to 0 if the file does not have
  // a number (e.g. CURRENT)
  uint64_t file_number = 0;

  // File size in bytes. See also `trim_to_size`.
  uint64_t size = 0;
};
*/
template<typename T>
bool set_diff(const std::set<T>& s1, const std::set<T>& s2) {
    // typename std::set<T>::iterator it;
    if (s1.size() != s2.size()) {
        return true;
    }

    std::set<T> diff;
    // 计算s1和s2的差集，并将结果保存到diff中
    std::set_difference(s1.begin(), s1.end(), s2.begin(), s2.end(),
                        std::inserter(diff, diff.begin()));
    // 如果diff为空集，则说明s1和s2中的元素相同
    return !diff.empty();
}

void print_metadata_info(const rocksdb::LiveFileMetaData& metadata) {
    TableKey smallestkey(metadata.smallestkey);
    TableKey largestkey(metadata.largestkey);
    if (metadata.column_family_name == RocksWrapper::COLD_BINLOG_CF) {
        int64_t smallest_region_id = smallestkey.extract_i64(0);
        int64_t smallest_ts  = smallestkey.extract_i64(sizeof(int64_t));
        int64_t largest_region_id  = largestkey.extract_i64(0);
        int64_t largest_ts   = largestkey.extract_i64(sizeof(int64_t));
        DB_WARNING("LiveFileMetaData[cf_name: %s level: %d];"
            "smallestkey[region_id: %ld ts: %ld, %s]; largestkey[region_id: %ld ts: %ld, %s]; "
            "SstFileMetaData[smallest_seqno: %lu largest_seqno: %lu num_reads_sampled: %lu being_compacted: %d "
            "num_entries: %lu num_deletions: %lu file_creation_time: %lu name: %s db_path: %s]; "
            "FileStorageInfo[relative_filename: %s directory: %s file_number: %lu size: %lu].", 
            metadata.column_family_name.c_str(), metadata.level,
            smallest_region_id, smallest_ts, ts_to_datetime_str(smallest_ts).c_str(), 
            largest_region_id, largest_ts,  ts_to_datetime_str(largest_ts).c_str(),
            metadata.smallest_seqno, metadata.largest_seqno, metadata.num_reads_sampled, (int)metadata.being_compacted,
            metadata.num_entries, metadata.num_deletions, metadata.file_creation_time, metadata.name.c_str(), metadata.db_path.c_str(), 
            metadata.relative_filename.c_str(), metadata.directory.c_str(), metadata.file_number, metadata.size);
    } else {
        int64_t smallest_region_id = smallestkey.extract_i64(0);
        int64_t smallest_table_id  = smallestkey.extract_i64(sizeof(int64_t));
        int64_t largest_region_id  = largestkey.extract_i64(0);
        int64_t largest_table_id   = largestkey.extract_i64(sizeof(int64_t));
        DB_WARNING("LiveFileMetaData[cf_name: %s level: %d]; "
            "smallestkey[region_id: %ld table_id: %ld]; largestkey[region_id: %ld table_id: %ld]; "
            "SstFileMetaData[smallest_seqno: %lu largest_seqno: %lu num_reads_sampled: %lu being_compacted: %d "
            "num_entries: %lu num_deletions: %lu file_creation_time: %lu name: %s db_path: %s]; "
            "FileStorageInfo[relative_filename: %s directory: %s file_number: %lu size: %lu].", 
            metadata.column_family_name.c_str(), metadata.level,
            smallest_region_id, smallest_table_id, largest_region_id, largest_table_id,
            metadata.smallest_seqno, metadata.largest_seqno, metadata.num_reads_sampled, (int)metadata.being_compacted,
            metadata.num_entries, metadata.num_deletions, metadata.file_creation_time, metadata.name.c_str(), metadata.db_path.c_str(), 
            metadata.relative_filename.c_str(), metadata.directory.c_str(), metadata.file_number, metadata.size);
    }
}

int Region::modify_olap_region_num_table_lines() {
    // 获取L6的sst预估num_table_lines
    int64_t estimate_lines = Store::get_instance()->get_region_estimate_lines(get_region_id(), get_version());
    if (estimate_lines <= 0) {
        return -1;
    }

    DB_WARNING("region_id: %ld num_table_lines: %ld => %ld", get_region_id(), _num_table_lines.load(), estimate_lines);
    _num_table_lines = estimate_lines;
    
    return 0;
}

// 校验cold rocksdb中的sst
int Region::get_cold_sst(std::set<std::string>& sst_relative_filename) {
    TimeCost cost;
    std::vector<rocksdb::LiveFileMetaData> metadata_copy;
    metadata_copy.reserve(5);
    // 列出所有的sst
    std::vector<rocksdb::LiveFileMetaData> metadata;
    _rocksdb->get_cold_live_files(&metadata);
    for (const auto& md : metadata) {
        if (md.column_family_name != RocksWrapper::COLD_DATA_CF) {
            continue;
        }
        TableKey smallestkey(md.smallestkey);
        TableKey largestkey(md.largestkey);
        int64_t smallest_region_id = smallestkey.extract_i64(0);
        int64_t largest_region_id  = largestkey.extract_i64(0);
        if (smallest_region_id == largest_region_id && smallest_region_id == get_region_id()) {
            metadata_copy.emplace_back(md);
        } else if (smallest_region_id <= get_region_id() && get_region_id() <= largest_region_id) {
            // sst混合了该region和其他region的数据
            print_metadata_info(md);
            metadata_copy.clear();
            DB_FATAL("region_id: %ld, check sst failed, sst_name: %s", _region_id, md.relative_filename.c_str());
            return -1;
        }
    }

    for (const auto& md : metadata_copy) {
        print_metadata_info(md);
        if (md.being_compacted) {
            return -1;
        }

        if (md.level != 6) {
            return -1;
        }

        sst_relative_filename.insert(md.relative_filename);
    }

    return 0;
}


int Region::get_cold_sst(std::set<std::string>& sst_relative_filename, std::set<int64_t>& index_ids) { 
    TimeCost cost;
    std::vector<rocksdb::LiveFileMetaData> metadata_copy;
    metadata_copy.reserve(5);
    // 列出所有的sst
    std::vector<rocksdb::LiveFileMetaData> metadata;
    _rocksdb->get_cold_live_files(&metadata);
    for (const auto& md : metadata) {
        if (md.column_family_name != RocksWrapper::COLD_DATA_CF) {
            continue;
        }
        TableKey smallestkey(md.smallestkey);
        TableKey largestkey(md.largestkey);
        int64_t smallest_region_id = smallestkey.extract_i64(0);
        int64_t largest_region_id  = largestkey.extract_i64(0);
        if (smallest_region_id == largest_region_id && smallest_region_id == get_region_id()) {
            metadata_copy.emplace_back(md);
        } else if (smallest_region_id <= get_region_id() && get_region_id() <= largest_region_id) {
            // sst混合了该region和其他region的数据
            print_metadata_info(md);
            metadata_copy.clear();
            DB_FATAL("region_id: %ld, check sst failed, sst_name: %s", _region_id, md.relative_filename.c_str());
            return -1;
        }
    }

    for (const auto& md : metadata_copy) {
        print_metadata_info(md);
        if (md.being_compacted) {
            return -1;
        }

        if (md.level != 6) {
            return -1;
        }

        sst_relative_filename.insert(md.relative_filename);
        TableKey smallestkey(md.smallestkey);
        index_ids.insert(smallestkey.extract_i64(sizeof(int64_t)));
    }

    return 0;
}

int Region::ingest_cold_sst_on_snapshot_load() {
    TimeCost cost;
    pb::OlapRegionInfo olap_info;
    int ret = _meta_writer->read_olap_info(_region_id, olap_info);
    if (ret < 0) {
        DB_FATAL("region_id: %ld read olap info failed", _region_id);
        return -1;
    } 
    _olap_state.store(olap_info.state());
    if (olap_info.state() <= pb::OLAP_IMMUTABLE) {
        DB_WARNING("region_id: %ld, not need ingest cold sst", _region_id);
        return 0;
    }

    std::vector<std::string> external_files;
    external_files.reserve(5);
    for (const auto& f : olap_info.external_full_path()) {
        external_files.emplace_back(f);
    }

    if (!external_files.empty()) {
        ret = ingest_cold_sst(external_files);
        if (ret < 0) {
            DB_FATAL("region_id: %ld ingest_cold_sst failed", _region_id);
            return -1;
        }
    }

    DB_NOTICE("region_id: %ld olap_info: %s cost: %ld", _region_id, olap_info.ShortDebugString().c_str(), cost.get_time());
    return 0;
}

int Region::ingest_cold_sst(const std::vector<std::string>& external_files) {
    if (external_files.empty()) {
        DB_WARNING("region_id: %ld external_files empty", _region_id);
        return 0;
    }

    std::set<std::string> sst_relative_filename;
    int ret = get_cold_sst(sst_relative_filename);
    if (ret < 0) {
        DB_FATAL("region_id: %ld get cold sst failed", _region_id);
        return -1;
    }

    std::set<std::string> tmp_external_files;
    if (!sst_relative_filename.empty()) {
        ret = SstExtLinker::get_instance()->list_external_full_name(sst_relative_filename, tmp_external_files);
        if (ret < 0) {
            DB_FATAL("region_id: %ld list external file failed", _region_id);
            return -1;
        }
    }

    if (!tmp_external_files.empty()) {
        bool diff = (tmp_external_files.size() != external_files.size());
        for (const std::string& f : external_files) {
            if (tmp_external_files.count(f) == 0) {
                diff = true;
                break;
            }
        }

        if (diff) {
            DB_FATAL("region_id: %ld, need remove cold sst", _region_id);
            ret = RegionControl::remove_cold_data(_region_id);
            if (ret < 0) {
                DB_FATAL("region_id: %ld delete files in range failed", _region_id);
                return -1;
            }
        } else {
            DB_WARNING("region_id: %ld not need ingest", _region_id);
            return 0;
        }
    }

    auto s = _rocksdb->ingest_to_cold(external_files);
    if (!s.ok()) {
        DB_FATAL("region_id: %ld ingest files failed, err: %s", _region_id, s.ToString().c_str());
        return -1;
    }

    return 0;
}

int Region::ingest_cold_index_sst(std::vector<std::string>& external_files, int64_t index_id) {
    if (external_files.empty()) {
        DB_WARNING("region_id: %ld external_file empty", _region_id);
        return 0;
    }

    std::set<std::string> sst_relative_filename;
    std::set<int64_t> index_ids;
    int ret = get_cold_sst(sst_relative_filename, index_ids);
    if (ret < 0) {
        DB_FATAL("region_id: %ld get cold sst failed", _region_id);
        return -1;
    }

    std::set<std::string> tmp_external_files;
    if (!sst_relative_filename.empty()) {
        ret = SstExtLinker::get_instance()->list_external_full_name(sst_relative_filename, tmp_external_files);
        if (ret < 0) {
            DB_FATAL("region_id: %ld list external file failed", _region_id);
            return -1;
        }
    }
    if (index_ids.count(index_id) != 0) {
        DB_FATAL("region_id: %ld, index_id: %ld need remove cold sst", _region_id, index_id);
        ret = RegionControl::remove_cold_index_data(_region_id, index_id);
        if (ret < 0) {
            DB_FATAL("region_id: %ld, index_id: %ld delete files in range failed", _region_id, index_id);
            return -1;
        }
    }

    if (!tmp_external_files.empty()) {
        for (const std::string& external_file : external_files) {
            if (tmp_external_files.count(external_file) != 0) {
                DB_FATAL("region_id: %ld, index_id: %ld need remove cold sst", _region_id, index_id);
                ret = RegionControl::remove_cold_index_data(_region_id, index_id);
                if (ret < 0) {
                    DB_FATAL("region_id: %ld, index_id: %ld delete files in range failed", _region_id, index_id);
                    return -1;
                }
                break;
            }
        }
    }

    auto s = _rocksdb->ingest_to_cold(external_files);
    if (!s.ok()) {
        DB_FATAL("region_id: %ld ingest files failed, err: %s", _region_id, s.ToString().c_str());
        return -1;
    }

    return 0;
}

int Region::flush_to_cold_rocksdb() {
    if (!need_flush_to_cold_rocksdb()) {
        return -1;
    }

    if (make_region_status_doing() != 0) {
        DB_WARNING("region status is not idle, region_id: %ld", _region_id);
        return -1;
    }   
    ON_SCOPE_EXIT([this]() {
        reset_region_status();
    });

    TimeCost cost;
    pb::OlapRegionInfo olap_info;
    int ret = _meta_writer->read_olap_info(_region_id, olap_info);
    if (ret < 0) {
        DB_FATAL("region_id: %ld read olap info failed", _region_id);
        return -1;
    }
    if (flush_index_to_cold_rocksdb(olap_info) != 0) {
        DB_FATAL("DDL_LOG region_id: %ld flush index to cold fail", _region_id);
    }
    if (olap_info.state() == pb::OLAP_ACTIVE) {
        if (!has_sst_data(nullptr, nullptr)) {
            DB_WARNING("region_id: %ld no sst data", _region_id);
            return 0;
        }
        std::vector<std::string> external_files;
        std::map<int64_t, std::vector<std::string> > index_ext_paths_mapping;
        ret = sync_olap_info(pb::OLAP_IMMUTABLE, external_files, index_ext_paths_mapping);
        if (ret < 0) {
            return -1;
        }
    } else if (olap_info.state() == pb::OLAP_IMMUTABLE) {
        if (!has_sst_data(nullptr, nullptr)) {
            DB_FATAL("region_id: %ld no sst data", _region_id);
            return 0;
        }

        uint64_t now = (uint64_t)time(nullptr);
        if ((now - olap_info.state_time()) < FLAGS_hot_rocksdb_truncate_interval_s) {
            // 等待一个周期之后再执行下一个状态
            return 0;
        }

        std::vector<std::string> external_files;
        std::map<int64_t, std::vector<std::string> > index_ext_paths_mapping;

        ret = flush_hot_to_cold(external_files, index_ext_paths_mapping);
        if (ret < 0) {
            DB_FATAL("flush_hot_to_cold failed");
            return -1;
        }

        ret = sync_olap_info(pb::OLAP_FLUSHED, external_files, index_ext_paths_mapping);
        if (ret < 0) {
            return -1;
        }
    } else if (olap_info.state() == pb::OLAP_FLUSHED) {
        uint64_t now = (uint64_t)time(nullptr);
        if ((now - olap_info.state_time()) > FLAGS_hot_rocksdb_truncate_interval_s) {
            std::vector<std::string> external_files;
            external_files.reserve(5);
            for (const auto& file : olap_info.external_full_path()) {
                external_files.emplace_back(file);
            }
            std::map<int64_t, std::vector<std::string> > index_ext_paths_mapping;
            for (const auto& olap_index_info : olap_info.olap_index_info_list()) {
                std::vector<std::string> external_path_list;
                for (const auto& external_path: olap_index_info.external_path()) {
                    external_path_list.emplace_back(external_path);
                }
                index_ext_paths_mapping[olap_index_info.index_id()] = external_path_list;
            }
            ret = sync_olap_info(pb::OLAP_TRUNCATED, external_files, index_ext_paths_mapping);
            if (ret < 0) {
                return -1;
            }
        }
    }

    return 0;
}

int Region::doing_cold_data_rollup(int64_t index_id) {
    MutTableKey upper_bound;
    upper_bound.append_i64(_region_id).append_i64(get_table_id());
    upper_bound.append_u64(UINT64_MAX);
    upper_bound.append_u64(UINT64_MAX);
    upper_bound.append_u64(UINT64_MAX);
    rocksdb::Slice upper_bound_slice = upper_bound.data();
    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    read_options.total_order_seek = false;
    read_options.iterate_upper_bound = &upper_bound_slice;
    
    std::unique_ptr<myrocksdb::Iterator> iter(new myrocksdb::Iterator(
            RocksWrapper::get_instance()->new_cold_iterator(read_options, RocksWrapper::COLD_DATA_CF)));
    SmartTable table_info_ptr = _factory->get_table_info_ptr(get_table_id());
    if (table_info_ptr == nullptr) {
        DB_FATAL("DDL_LOG table: %ld table_info is not found", get_table_id());
        return -1;
    }
    SmartIndex pk_info = _factory->get_index_info_ptr(get_table_id());
    if (pk_info == nullptr) {
        DB_FATAL("DDL_LOG table: %ld pk_info is not found", get_table_id());
        return -1;
    }
    SmartIndex rollup_index_info_ptr =  _factory->get_index_info_ptr(index_id);
    if (rollup_index_info_ptr == nullptr) {
        DB_FATAL("DDL_LOG table: %ld rollup_index_info is not found", get_table_id());
        return -1;
    }
    IndexInfo& rollup_index_info = *rollup_index_info_ptr;

    MutTableKey key;
    key.append_i64(_region_id).append_i64(get_table_id());
    iter->Seek(key.data());
    int prefix_len = sizeof(int64_t) * 2;
    TimeCost time_cost;
    rocksdb::Status s;
    int line_nums = 0;

    // 清除索引
    rocksdb::WriteOptions write_options;
    MutTableKey begin_key;
    MutTableKey end_key;
    begin_key.append_i64(_region_id).append_i64(index_id);
    end_key.append_i64(_region_id).append_i64(index_id).append_u64(UINT64_MAX);
    s = _rocksdb->remove_range(write_options, _data_cf, begin_key.data(), end_key.data(), true);
    if (!s.ok()) {
        DB_FATAL("DDL_LOG remove_index failed: code=%d, msg=%s, region_id: %ld table_id:%ld index_id:%ld", 
            s.code(), s.ToString().c_str(), _region_id, get_table_id(), index_id);
        return -1;
    }

    std::set<int32_t> pri_field_ids;
    std::map<int32_t, FieldInfo*> field_ids;
    for (auto& field : pk_info->fields) {
        pri_field_ids.insert(field.id);
    }
    for (auto& field_info : table_info_ptr->fields) {
        if (pri_field_ids.count(field_info.id) == 0) {
            field_ids[field_info.id] = &field_info;
        }
    }
    rocksdb::WriteBatch batch;
    // 开始加冷数据的rollup索引
    while (iter->Valid()) {
        int pos = prefix_len;
        TableKey table_key(iter->key(), true);
        SmartRecord record = _factory->new_record(get_table_id());
        if (0 != record->decode_key(*pk_info, table_key, pos)) {
            DB_WARNING("DDL_LOG table_id:%ld index:%ld region_id:%ld decode key failed", get_table_id(), index_id, _region_id);
            return -1;
        }

        TupleRecord tuple_record(iter->value());
        if (0 != tuple_record.decode_fields(field_ids, record)) {
            DB_WARNING("DDL_LOG table_id:%ld index:%ld region_id:%ld decode value failed", get_table_id(), index_id, _region_id);
            return -1;
        }

        MutTableKey rollup_key;
        rollup_key.append_i64(_region_id).append_i64(rollup_index_info.id);
        if(0 != rollup_key.append_index(rollup_index_info, record.get(), -1, false)) {
            DB_FATAL("DDL_LOG table_id:%ld index:%ld region_id:%ld fail to append_index", get_table_id(), index_id, _region_id);
            return -1;
        }

        std::string rollup_value;
        SmartRecord rollup_record =  record->clone(true);
        if (rollup_record == nullptr) {
            DB_FATAL("DDL_LOG table_id:%ld index:%ld region_id:%ld fail to clone", get_table_id(), index_id, _region_id);
            return -1;
        }
        if (rollup_record->encode_value_for_rollup(rollup_index_info, table_info_ptr->fields, table_info_ptr->fields_need_sum) != 0) {
            DB_FATAL("DDL_LOG table_id:%ld index:%ld region_id:%ld fail to encode_rollup", get_table_id(), index_id, _region_id);
            return -1;
        }
        if (0 != rollup_record->encode(rollup_value)) {
            DB_FATAL("DDL_LOG table_id:%ld index:%ld region_id:%ld fail to encode_record", get_table_id(), index_id, _region_id);
            return -1;
        }
        batch.Merge(_data_cf, rollup_key.data(), rollup_value);
        if (line_nums % 100 == 0) {
            s = _rocksdb->write(write_options, &batch);
            if (!s.ok()) {
                DB_FATAL("DDL_LOG table_id:%ld index:%ld region_id:%ld merge fail", get_table_id(), index_id, _region_id);
                return -1;
            }
            batch.Clear();
        }
        iter->Next();
        line_nums++;
    }
    s = _rocksdb->write(write_options, &batch);
    if (!s.ok()) {
        DB_FATAL("DDL_LOG table_id:%ld index:%ld region_id:%ld merge fail", get_table_id(), index_id, _region_id);
        return -1;
    }
    DB_NOTICE("DDL_LOG doing cold data rollup end, cost: %ld table_id:%ld, index:%ld, region_id: %ld, num_table_lines: %d", 
            time_cost.get_time(), get_table_id(), index_id, _region_id, line_nums);
    return 0;
}

void Region::do_cold_index_ddl_work(const pb::OlapRegionInfo& olap_info) {
    if (olap_info.state() != pb::OLAP_TRUNCATED) {
        return;
    }
    std::vector<int64_t> index_ids = _factory->get_all_index_info(get_table_id());
    for (auto index_id : index_ids) {
        SmartIndex index_info = _factory->get_index_info_ptr(index_id);
        if (index_info == nullptr) {
            DB_FATAL("DDL_LOG region_id: %ld, index_id: %ld not exist", _region_id, index_id);
            continue;
        }
        if (index_info->type != pb::I_ROLLUP) {
            continue;
        }
        bool olap_index_exist = false;
        for (auto olap_index_info : olap_info.olap_index_info_list()) {
            if (olap_index_info.index_id() == index_id) {
                olap_index_exist = true;
                break;
            }
        }
        if (olap_index_exist) {
            continue;
        }
        if (doing_cold_data_rollup(index_id) != 0) {
            DB_FATAL("DDL_LOG region_id: %ld, index_id: %ld doing cold data rollup failed", _region_id, index_id);
            continue;
        }
        if (!has_index_sst_data(index_id, nullptr)) {
            DB_FATAL("DDL_LOG region_id: %ld no index %ld sst data", _region_id, index_id);
            continue;
        }
        std::map<int64_t, std::vector<std::string> > new_index_ext_paths_mapping;
        int ret = flush_hot_index_to_cold(index_id, new_index_ext_paths_mapping);
        if (ret < 0) {
            DB_FATAL("DDL_LOG region_id: %ld, index %ld flush hot index to cold fail", _region_id, index_id);
            continue;
        }
        if (new_index_ext_paths_mapping.size() > 0) {
            int ret = sync_olap_index_info(olap_info, pb::OLAP_TRUNCATED, new_index_ext_paths_mapping);
            if (ret < 0) {
                DB_FATAL("DDL_LOG region_id: %ld, index_id: %ld sync olap index info failed", _region_id, index_id);
                continue;
            }
        }
    }
}

int Region::flush_index_to_cold_rocksdb(const pb::OlapRegionInfo& olap_info) {
    int ret = 0;
    do_cold_index_ddl_work(olap_info);
    if (!olap_info.has_new_olap_index_info()) {
        return 0;
    }
    std::map<int64_t, std::vector<std::string> > new_index_ext_paths_mapping;
    if (olap_info.new_olap_index_info().state() == pb::OLAP_IMMUTABLE) {
        if (!has_index_sst_data(olap_info.new_olap_index_info().index_id(), nullptr)) {
            DB_WARNING("DDL_LOG region_id: %ld no index %ld sst data", _region_id, olap_info.new_olap_index_info().index_id());
            return -1;
        }
        ret = flush_hot_index_to_cold(olap_info.new_olap_index_info().index_id(), new_index_ext_paths_mapping);
        if (ret < 0) {
            DB_FATAL("DDL_LOG region_id: %ld, index %ld flush_hot_to_cold failed",  _region_id, olap_info.new_olap_index_info().index_id());
            return -1;
        }
    }
    if (new_index_ext_paths_mapping.size() > 0) {
        ret = sync_olap_index_info(olap_info, pb::OLAP_TRUNCATED, new_index_ext_paths_mapping);
        if (ret < 0) {
            return -1;
        }
    }
    return 0;
}

void Region::apply_olap_info(const pb::StoreReq& request, braft::Closure* done) {
    const pb::OlapRegionInfo& olap_info = request.extra_req().olap_info();
    int ret = 0;
    TimeCost cost;
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    if (olap_info.state() == pb::OLAP_FLUSHED) {
        std::vector<std::string> external_files;
        external_files.reserve(5);
        for (const auto& file : olap_info.external_full_path()) {
            external_files.emplace_back(file);
        }
        // 将external file ingest到rocksdb
        ret = ingest_cold_sst(external_files);
        if (ret < 0) {
            IF_DONE_SET_RESPONSE_FATAL(done, pb::EXEC_FAIL, "ingest cold sst failed");
            return;
        }
    } else if (olap_info.state() == pb::OLAP_TRUNCATED) {
        _meta_writer->clear_watt_stats_version(_region_id);
        ret = RegionControl::remove_data(_region_id);
        if (ret < 0) {
            IF_DONE_SET_RESPONSE_FATAL(done, pb::EXEC_FAIL, "remove range failed");
            return;
        }
    }
    std::string string_olap_info;
    if (!olap_info.SerializeToString(&string_olap_info)) {
        IF_DONE_SET_RESPONSE(done, pb::EXEC_FAIL, "serialize to string failed");
        DB_FATAL("olap_info: %s serialize to string fail, region_id: %ld", 
                olap_info.ShortDebugString().c_str(), _region_id); 
        return;
    }

    rocksdb::WriteOptions write_options;
    rocksdb::WriteBatch batch;
    batch.Put(_meta_writer->get_handle(), 
                _meta_writer->applied_index_key(_region_id), 
                _meta_writer->encode_applied_index(_applied_index, _data_index));
    batch.Put(_meta_writer->get_handle(), _meta_writer->olap_key(_region_id), string_olap_info);
    auto s = _rocksdb->write(write_options, &batch);
    if (!s.ok()) {
        IF_DONE_SET_RESPONSE(done, pb::EXEC_FAIL, "write to rocksdb failed");
        DB_FATAL("write rocksdb failed, region_id: %ld, status: %s", _region_id, s.ToString().c_str());
        return;
    }
    _olap_state.store(olap_info.state());
    DB_NOTICE("request: %s, cost: %ld", request.ShortDebugString().c_str(), cost.get_time());
    return;
}

void Region::apply_olap_index_info(const pb::StoreReq& request, braft::Closure* done) {
    pb::OlapRegionInfo olap_info = request.extra_req().olap_info();
    int ret = 0;
    TimeCost cost;
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    std::vector<std::string> external_files;
    external_files.reserve(5);
    if (olap_info.has_new_olap_index_info()
        && olap_info.new_olap_index_info().state() == pb::OLAP_TRUNCATED) {
        for (const auto& external_path: olap_info.new_olap_index_info().external_path()) {
            external_files.emplace_back(external_path);
        }
    }
    // 将external file ingest到rocksdb
    if (external_files.size() > 0) {
        ret = ingest_cold_index_sst(external_files, olap_info.new_olap_index_info().index_id());
        if (ret < 0) {
            IF_DONE_SET_RESPONSE_FATAL(done, pb::EXEC_FAIL, "ingest cold index sst failed");
            return;
        }
        // 删除索引热数据
        ret = RegionControl::remove_data(_region_id, olap_info.new_olap_index_info().index_id());
        if (ret < 0) {
            IF_DONE_SET_RESPONSE_FATAL(done, pb::EXEC_FAIL, "remove range failed");
            return;
        }
        olap_info.clear_new_olap_index_info();
    }
    std::string string_olap_info;
    if (!olap_info.SerializeToString(&string_olap_info)) {
        IF_DONE_SET_RESPONSE(done, pb::EXEC_FAIL, "serialize to string failed");
        DB_FATAL("olap_info: %s serialize to string fail, region_id: %ld", 
                olap_info.ShortDebugString().c_str(), _region_id); 
        return;
    }

    rocksdb::WriteOptions write_options;
    rocksdb::WriteBatch batch;
    batch.Put(_meta_writer->get_handle(), 
                _meta_writer->applied_index_key(_region_id), 
                _meta_writer->encode_applied_index(_applied_index, _data_index));
    batch.Put(_meta_writer->get_handle(), _meta_writer->olap_key(_region_id), string_olap_info);
    auto s = _rocksdb->write(write_options, &batch);
    if (!s.ok()) {
        IF_DONE_SET_RESPONSE(done, pb::EXEC_FAIL, "write to rocksdb failed");
        DB_FATAL("write rocksdb failed, region_id: %ld, status: %s", _region_id, s.ToString().c_str());
        return;
    }
    _olap_state.store(olap_info.state());
    DB_NOTICE("request: %s, cost: %ld", request.ShortDebugString().c_str(), cost.get_time());
    return;
}

int Region::sync_olap_info(pb::OlapRegionStat state, 
                        const std::vector<std::string>& external_files,
                        std::map<int64_t, std::vector<std::string> > index_ext_paths_mapping) {
    TimeCost tc;
    pb::StoreReq req;
    pb::StoreRes res;
    req.set_op_type(pb::OP_OLAP_INFO);
    req.set_region_id(_region_id);
    req.set_region_version(get_version());
    auto olap_info = req.mutable_extra_req()->mutable_olap_info();
    olap_info->set_state(state);
    olap_info->set_state_time((uint64_t)time(nullptr));
    for (const std::string& file : external_files) {
        olap_info->add_external_full_path(file);
    }
    for (const auto& index_ext_paths_iter : index_ext_paths_mapping) {
        pb::OlapRegionIndexInfo* olap_index_info = olap_info->add_olap_index_info_list();
        if (olap_index_info == nullptr) {
            DB_FATAL("index_ext_path is nullptr");
            return -1;
        } 
        olap_index_info->set_index_id(index_ext_paths_iter.first);
        for (const std::string& ext_path : index_ext_paths_iter.second) {
            olap_index_info->add_external_path(ext_path);
        }
    }
    butil::IOBuf data;
    butil::IOBufAsZeroCopyOutputStream wrapper(&data);
    if (!req.SerializeToZeroCopyStream(&wrapper)) {
        DB_FATAL("serializeToString fail, region_id: %ld", _region_id);  
        return -1;
    }

    BthreadCond cond;
    OlapOPClosure* c = new OlapOPClosure(&cond);
    c->response = &res;
    braft::Task task; 
    task.data = &data; 
    task.done = c;
    cond.increase();
    _node.apply(task);
    cond.wait();
    if (res.errcode() != pb::SUCCESS) {
        DB_FATAL("region_id: %ld, sync olap info: %s failed, response: %s", 
                _region_id, req.ShortDebugString().c_str(), res.ShortDebugString().c_str());
        return -1;
    }

    DB_NOTICE("region_id: %ld sync olap info: %s cost: %ld", _region_id, req.ShortDebugString().c_str(), tc.get_time());
    return 0;
}

int Region::sync_olap_index_info(const pb::OlapRegionInfo& old_olap_info, pb::OlapRegionStat state, 
                        std::map<int64_t, std::vector<std::string> > new_index_ext_paths_mapping) {
    TimeCost tc;
    pb::StoreReq req;
    pb::StoreRes res;
    req.set_op_type(pb::OP_OLAP_INDEX_INFO);
    req.set_region_id(_region_id);
    req.set_region_version(get_version());
    // 复制老的olap信息
    auto olap_info = req.mutable_extra_req()->mutable_olap_info();
    olap_info->CopyFrom(old_olap_info);
    olap_info->set_state_time((uint64_t)time(nullptr));
    olap_info->clear_new_olap_index_info();

    std::set<std::string> old_external_paths;
    std::set<uint64_t> index_ids;
    for (const auto& olap_index_info : olap_info->olap_index_info_list()) {
        for (const auto& external_path: olap_index_info.external_path()) {
            old_external_paths.insert(external_path);
        }
        index_ids.insert(olap_index_info.index_id());
    }

    // 添加新索引的信息
    for (const auto& new_index_ext_paths_iter : new_index_ext_paths_mapping) {
        if (index_ids.count(new_index_ext_paths_iter.first)) {
            DB_FATAL("new index %ld already added", new_index_ext_paths_iter.first);
            return -1;
        } 
        pb::OlapRegionIndexInfo* new_olap_index_info = olap_info->mutable_new_olap_index_info(); // 新索引列表
        if (new_olap_index_info == nullptr) {
            DB_FATAL("olap_index_info is nullptr");
            return -1;
        } 
        new_olap_index_info->set_index_id(new_index_ext_paths_iter.first);
        new_olap_index_info->set_state(state);

        pb::OlapRegionIndexInfo* olap_index_info;
        if (state == pb::OLAP_TRUNCATED) {
            olap_index_info = olap_info->add_olap_index_info_list(); // 老索引列表
            if (olap_index_info == nullptr) {
                DB_FATAL("olap_index_info is nullptr");
                return -1;
            } 
            olap_index_info->set_index_id(new_index_ext_paths_iter.first);
            olap_index_info->set_state(state);
        }
        for (const std::string& index_path : new_index_ext_paths_iter.second) {
            // 如果已经出现在老索引列表中说明有问题
            if (old_external_paths.count(index_path)) {
                DB_FATAL("new index path %s already added", index_path.c_str());
                return -1;
            }
            new_olap_index_info->add_external_path(index_path);
            if (state == pb::OLAP_TRUNCATED && olap_index_info != nullptr) {
                olap_index_info->add_external_path(index_path);
                olap_info->add_external_full_path(index_path);
            }
        }
    }
    butil::IOBuf data;
    butil::IOBufAsZeroCopyOutputStream wrapper(&data);
    if (!req.SerializeToZeroCopyStream(&wrapper)) {
        DB_FATAL("serializeToString fail, region_id: %ld", _region_id);  
        return -1;
    }

    BthreadCond cond;
    OlapOPClosure* c = new OlapOPClosure(&cond);
    c->response = &res;
    braft::Task task; 
    task.data = &data; 
    task.done = c;
    cond.increase();
    _node.apply(task);
    cond.wait();
    if (res.errcode() != pb::SUCCESS) {
        DB_FATAL("region_id: %ld, sync olap index info: %s failed, response: %s", 
                _region_id, req.ShortDebugString().c_str(), res.ShortDebugString().c_str());
        return -1;
    }

    DB_NOTICE("region_id: %ld sync olap index info: %s cost: %ld", _region_id, req.ShortDebugString().c_str(), tc.get_time());
    return 0;
}

std::string make_make_relative_path_without_filename(int64_t table_id, int64_t partition_id) {
    //                                                            partition     
    //  baikal_olap/meta_bns/database_name/table_name/table_id/20230301_20230331
    std::string path = "baikal_olap/";
    path += FLAGS_meta_server_bns + "/";
    auto table = SchemaFactory::get_instance()->get_table_info_ptr(table_id);
    if (table == nullptr) {
        DB_FATAL("cant find table_id: %ld", table_id);
        return "";
    }
    std::vector<std::string> vec;
    boost::split(vec, table->name, boost::is_any_of("."));
    if (vec.size() != 2) {
        DB_FATAL("invaild table name: %s", table->name.c_str());
        return "";
    }
    path += vec[0] + "/";
    path += vec[1] + "/";
    path += std::to_string(table_id) + "/";
    if (table->partition_ptr != nullptr) {
        path += table->partition_ptr->get_partition_uniq_str(partition_id); 
    } else {
        path += std::to_string(partition_id);
    }

    return path;
}

std::string make_relative_path(int64_t table_id, int64_t partition_id, int64_t region_id, uint64_t size, uint64_t lines) {
    //                                                            partition     regionid_lines_size_timestamp.extsst
    //  baikal_olap/meta_bns/database_name/table_name/table_id/20230301_20230331/26783_1024_1234.extsst
    std::string path = make_make_relative_path_without_filename(table_id, partition_id);
    if (path == "") {
        return "";
    }
    uint32_t now = (uint32_t)time(nullptr);    // 暂时使用时间戳，可能发生重复，后续可以改成拿自增id OLAPTODO
    path += "/" + std::to_string(region_id) + "_" + std::to_string(lines) + "_" + std::to_string(size) + "_" + std::to_string(now) + ".extsst";
    return path;
}

// 执行失败暂不删除
int copy_file(const std::string& local_file, const std::string& user_define_path, std::string& external_file, uint64_t size) {
    TimeCost cost;
    std::shared_ptr<ExtFileSystem> fs = SstExtLinker::get_instance()->get_exteranl_filesystem();
    external_file = fs->make_full_name("", false, user_define_path);
    if (external_file.empty()) {
        DB_FATAL("local_file: %s make full path failed", local_file.c_str());
        return -1;
    }
    int ret = fs->path_exists(external_file);
    if (ret < 0) {
        DB_FATAL("path_exists: %s failed", external_file.c_str());
        return -1;
    } else if (ret == 1) {
        DB_WARNING("path: %s exists", external_file.c_str());
        return -1;
    }

    ret = fs->create(external_file);
    if (ret < 0) {
        DB_FATAL("create external_file: %s failed", external_file.c_str());
        return -1;
    }

    ScopeGuard auto_decrease([&fs, &external_file]() {
        DB_WARNING("delete external_file: %s", external_file.c_str());
        fs->delete_path(external_file, false);
    });

    std::unique_ptr<ExtFileWriter> writer;
    ret = fs->open_writer(external_file, &writer);
    if (ret < 0) {
        DB_WARNING("open writer: %s filed", external_file.c_str());
        return -1;
    }

    butil::File f(butil::FilePath{local_file}, butil::File::FLAG_OPEN | butil::File::FLAG_READ);
    if (!f.IsValid()) {
        DB_WARNING("file[%s] is not valid.", local_file.c_str());
        return -1;
    }
    int64_t length = f.GetLength();
    if (length < 0 || length != size) {
        DB_FATAL("sst: %s get length failed, len: %ld vs %lu", local_file.c_str(), length, size);
        return -1;
    }
    int64_t read_ret = 0;
    int64_t write_ret = 0;
    int64_t read_offset = 0;
    const static int BUF_SIZE {4 * 1024 * 1024LL};
    std::unique_ptr<char[]> buf(new char[BUF_SIZE]);
    do {
        read_ret = f.Read(read_offset, buf.get(), BUF_SIZE);
        if (read_ret < 0) {
            DB_WARNING("read file[%s] error.", local_file.c_str());
            return -1;
        } 
        if (read_ret != 0) {
            write_ret = writer->append(buf.get(), read_ret);
            if (write_ret != read_ret) {
                DB_FATAL("append external_file: %s failed, offset: %ld, read_ret: %ld, write_ret: %ld", 
                    external_file.c_str(), read_offset, read_ret, write_ret);
                return -1;
            }
        }
        read_offset += read_ret;
    } while (read_ret == BUF_SIZE);

    int64_t write_size = writer->tell();
    if (write_size != length) {
        DB_FATAL("sst_file: %s external_file: %s diff size %ld vs %ld", local_file.c_str(), external_file.c_str(), size, write_size);
        return -1;
    }

    if (!writer->sync()) {
        DB_FATAL("external file: %s sync failed", external_file.c_str());
        return -1;
    }

    if (!writer->close()) {
        DB_FATAL("external file: %s close failed", external_file.c_str());
        return -1;
    }

    auto_decrease.release();
    DB_NOTICE("copy %s to %s size: %ld, cost: %ld", local_file.c_str(), external_file.c_str(), size, cost.get_time());
    return 0;
}

// struct ExternalSstFileInfo {
//   std::string file_path;     // external sst file path
//   std::string smallest_key;  // smallest user key in file
//   std::string largest_key;   // largest user key in file
//   std::string smallest_range_del_key;  // smallest range deletion user key in file
//   std::string largest_range_del_key;  // largest range deletion user key in file
//   std::string file_checksum;          // sst file checksum;
//   std::string file_checksum_func_name;  // The name of file checksum function
//   SequenceNumber sequence_number;     // sequence number of all keys in file
//   uint64_t file_size;                 // file size in bytes
//   uint64_t num_entries;               // number of entries in file
//   uint64_t num_range_del_entries;  // number of range deletion entries in file
//   int32_t version;                 // file version
// };

void print_external_info(const rocksdb::ExternalSstFileInfo& info) {
    TableKey smallestkey(info.smallest_key);
    TableKey largestkey(info.largest_key);
    int64_t smallest_region_id = smallestkey.extract_i64(0);
    int64_t smallest_table_id  = smallestkey.extract_i64(sizeof(int64_t));
    int64_t largest_region_id  = largestkey.extract_i64(0);
    int64_t largest_table_id   = largestkey.extract_i64(sizeof(int64_t));

    DB_NOTICE("ExternalSstFileInfo[file_path: %s]; "
        "smallestkey[region_id: %ld table_id: %ld]; largestkey[region_id: %ld table_id: %ld]; "
        "smallest_range_del_key: %s, largest_range_del_key: %s; "
        "sequence_number: %lu, file_size: %lu, num_entries: %lu, num_range_del_entries: %lu, version: %d",
        info.file_path.c_str(), 
        smallest_region_id, smallest_table_id, largest_region_id, largest_table_id,
        info.smallest_range_del_key.c_str(), info.largest_range_del_key.c_str(), 
        info.sequence_number, info.file_size, info.num_entries, info.num_range_del_entries, info.version);
}

class ColdFileSstWriter {
public:
    ColdFileSstWriter(int64_t table_id, int64_t partition_id, int64_t region_id, int64_t index_id, int64_t count_per_sst) : 
        _table_id(table_id), _partition_id(partition_id), _region_id(region_id), _index_id(index_id), _count_per_sst(count_per_sst) {
        _tmp_file_name.clear();
        _external_files.clear();
    }
    ~ColdFileSstWriter() {
        if (_writer != nullptr) {
            delete _writer;
        }

        if (!_tmp_file_name.empty()) {
            delete_file(_tmp_file_name);
        }
    }

    int write_kv(const rocksdb::Slice& key, const rocksdb::Slice& value);

    int finish();

    std::vector<std::string> external_files() {
        return _external_files;
    }
private:
    void delete_file(const std::string& file_name) {
        butil::FilePath file_path(file_name);
        if (butil::PathExists(file_path)) {
            butil::DeleteFile(file_path, true);
        }
    }
    const int64_t _table_id;
    const int64_t _partition_id;
    const int64_t _region_id;
    const int64_t _index_id;
    const int64_t _count_per_sst;
    TimeCost _cost;
    uint64_t _write_count = 0;
    std::string _tmp_file_name;
    SstFileWriter* _writer = nullptr;
    std::vector<std::string> _external_files;
};

int ColdFileSstWriter::finish() {
    if (_writer == nullptr) {
        return 0;
    }

    rocksdb::ExternalSstFileInfo sst_info;
    rocksdb::Status s = _writer->finish(&sst_info);
    if (!s.ok()) {
        DB_FATAL("region_id: %ld finish file: %s fail", _region_id, _tmp_file_name.c_str());
        return -1;
    }
    print_external_info(sst_info);

    uint64_t size = _writer->file_size();
    if (size != sst_info.file_size) {
        DB_FATAL("file: %s diff size %lu vs %lu", _tmp_file_name.c_str(), size, sst_info.file_size);
        return -1;
    }

    delete _writer;
    _writer = nullptr;

    std::string external_file;
    std::string user_define_path = make_relative_path(_table_id, _partition_id, _region_id, size, _write_count);
    if (user_define_path.empty()) {
        DB_FATAL("make_relative_path fail, table_id: %ld", _table_id);
        return -1;
    }
    int ret = copy_file(_tmp_file_name, user_define_path, external_file, size);
    if (ret < 0) {
        DB_FATAL("copy file: %s failed", _tmp_file_name.c_str());
        return -1;
    }
    // 删除临时文件
    delete_file(_tmp_file_name);
    _external_files.emplace_back(external_file);
    DB_NOTICE("local file: %s external_file: %s size: %lu cost: %ld", _tmp_file_name.c_str(), external_file.c_str(), size, _cost.get_time());
    _tmp_file_name.clear();
    return 0;
}

int ColdFileSstWriter::write_kv(const rocksdb::Slice& key, const rocksdb::Slice& value) {
    rocksdb::Status s;
    if (_writer == nullptr) {
        rocksdb::Options option = RocksWrapper::get_instance()->get_cold_options();
        option.env = rocksdb::Env::Default();
        _writer = new SstFileWriter(option, false);
        _write_count = 0;
        _cost.reset();
        _tmp_file_name = FLAGS_db_path + "_tmp/" + std::to_string(_region_id) + "_" +std::to_string(butil::gettimeofday_us()); 
        butil::FilePath file_path(_tmp_file_name);
        if (butil::PathExists(file_path)) {
            DB_FATAL("tmp file: %s exists", _tmp_file_name.c_str());
            return -1;
        } else {

        } 
        s = _writer->open(_tmp_file_name);
        if (!s.ok()) {
            DB_FATAL("region_id: %ld, index_id: %ld, open sst file: %s fail", _region_id, _index_id, _tmp_file_name.c_str());
            return -1;
        }
    }

    s = _writer->put(key, value);
    if (!s.ok()) {
        DB_FATAL("region_id: %ld, index_id: %ld, write kv fail", _region_id, _index_id);
        return -1;
    }

    if (++_write_count < _count_per_sst) {
        // 小于预估每个sst的行数，返回再次写入
        return 0;
    }

    // flush
    return finish();
}
int Region::flush_hot_to_cold(std::vector<std::string>& external_files,
                            std::map<int64_t, std::vector<std::string>>& index_ext_paths_mapping) {
    std::vector<int64_t> index_ids = _factory->get_all_index_info(get_table_id());    
    SmartTable table_info_ptr = _factory->get_table_info_ptr(get_table_id());
    SmartRecord record_template = _factory->new_record(get_table_id());
    FieldInfo snapshot_field;
    const FieldDescriptor* snapshot_desc = nullptr;
    if (fits_snapshot_blacklist()) {
        _need_snapshot_filter = true;
        for (auto& field : table_info_ptr->fields) {
            if (field.short_name == "__snapshot__") {
                snapshot_field = field;
                break;
            }
        }
        snapshot_desc = record_template->get_field_by_idx(snapshot_field.pb_idx);
    }
    for (int64_t index_id: index_ids) {
        TimeCost cost;
        std::string prefix;
        MutTableKey key;
        key.append_i64(_region_id).append_i64(index_id);
        prefix = key.data();
        key.append_u64(UINT64_MAX);
        rocksdb::Slice upper_bound_slice(key.data());

        rocksdb::ReadOptions options;
        options.total_order_seek = true;
        options.fill_cache = false;
        options.iterate_upper_bound = &upper_bound_slice;
        std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(options, _data_cf));
        int64_t total_cnt = 0;
        int64_t fail_cnt = 0;
        ColdFileSstWriter sst_writer(get_table_id(), get_partition_id(), _region_id, index_id, FLAGS_num_lines_per_sst);
        ScopeGuard auto_decrease([&sst_writer]() {
            auto fs = SstExtLinker::get_instance()->get_exteranl_filesystem();
            for (const std::string& external_file : sst_writer.external_files()) {
                DB_WARNING("delete external_file: %s", external_file.c_str());
                fs->delete_path(external_file, false);
            }
        });

        SmartRecord record_template = _factory->new_record(get_table_id());
        SmartIndex index_info = _factory->get_index_info_ptr(index_id);
        int prefix_len = sizeof(int64_t) * 2;
        for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
            total_cnt++;
            if (!iter->key().starts_with(prefix)) {
                fail_cnt++;
                DB_FATAL("not starts with: %ld", _region_id);
                continue;
            }
            if (_need_snapshot_filter && 0 != table_info_ptr->snapshot_blacklist.size() && snapshot_desc != nullptr) {
                SmartRecord record = record_template->clone(false);
                TableKey table_key(iter->key(), true);
                int pos = prefix_len;
                // __snapshot__ 可能在key中，也可能在value中
                record->decode(iter->value().ToString());
                record->decode_key(*(index_info.get()), table_key, pos);
                ExprValue val = record->get_value(snapshot_desc);
                if (table_info_ptr->snapshot_blacklist.count(val._u.uint64_val) != 0) {
                    DB_DEBUG("table: %ld, flush hot to cold snapshot_blacklist filter val: %lu", get_table_id(), val._u.uint64_val);
                    continue;
                }
            }

            int ret = sst_writer.write_kv(iter->key(), iter->value());
            if (ret < 0) {
                DB_FATAL("region_id: %ld write kv failed", _region_id);
                return -1;
            }
        }

        int ret = sst_writer.finish();
        if (ret < 0) {
            DB_FATAL("region_id: %ld finish failed", _region_id);
            return -1;
        }
        auto_decrease.release();
        for (const std::string& ext_file : sst_writer.external_files()) {
            external_files.push_back(ext_file);
        }
        index_ext_paths_mapping[index_id] = sst_writer.external_files();;
        DB_NOTICE("region_id: %ld flush total_cnt: %ld, fail_cnt: %ld, cost: %ld", _region_id, total_cnt, fail_cnt, cost.get_time());
    }
    return 0;
}

int Region::fits_snapshot_blacklist() {
    SmartTable table_info_ptr = _factory->get_table_info_ptr(get_table_id());
    if (table_info_ptr->snapshot_blacklist.size() == 0) {
        return false;
    }
    pb::PartitionRange partition_range;
    for (auto it = table_info_ptr->partition_info.range_partition_infos().rbegin(); it != table_info_ptr->partition_info.range_partition_infos().rend(); ++it) {
        if (it->partition_id() == get_partition_id()) {
            partition_range.CopyFrom(it->range());
            break;
        }
    }

    ExprNode* p_left_node = nullptr;
    ScopeGuard auto_delete([&p_left_node] () {
        SAFE_DELETE(p_left_node);
    });
    if (ExprNode::create_expr_node(partition_range.left_value().nodes(0), &p_left_node) != 0) {
        DB_FATAL("create expr node error.");
        return false;
    }
    if (p_left_node == nullptr) {
        DB_FATAL("p_left_node is nullptr");
        return false;
    }
    ExprValue left_expr_value = p_left_node->get_value(nullptr);
    left_expr_value.cast_to(pb::TIMESTAMP);

    for (uint64_t snapshot : table_info_ptr->snapshot_blacklist) {
        time_t ts = snapshot_to_timestamp(snapshot);
        ExprValue snapshot_expr_value(pb::TIMESTAMP);
        snapshot_expr_value._u.uint32_val = ts;
        if (left_expr_value.compare(snapshot_expr_value) == 0) {
            return true;
        }
    }
    return false;
}

int Region::flush_hot_index_to_cold(int64_t index_id,
                            std::map<int64_t, std::vector<std::string>>& new_index_ext_paths_mapping) {
    TimeCost cost;
    std::string prefix;
    MutTableKey key;
    key.append_i64(_region_id).append_i64(index_id);
    prefix = key.data();
    key.append_u64(UINT64_MAX);
    rocksdb::Slice upper_bound_slice(key.data());

    rocksdb::ReadOptions options;
    options.total_order_seek = true;
    options.fill_cache = false;
    options.iterate_upper_bound = &upper_bound_slice;
    std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(options, _data_cf));
    int64_t total_cnt = 0;
    int64_t fail_cnt = 0;
    int prefix_len = sizeof(int64_t) * 2;
    SmartRecord record_template = _factory->new_record(get_table_id());
    SmartIndex index_info = _factory->get_index_info_ptr(index_id);
    SmartTable table_info_ptr = _factory->get_table_info_ptr(get_table_id());
    if (index_info == nullptr) {
        DB_WARNING("index_id: %ld is nullptr", index_id);
        return -1;
    }
    FieldInfo snapshot_field;
    const FieldDescriptor* snapshot_desc = nullptr;
    if (fits_snapshot_blacklist()) {
        _need_snapshot_filter = true;
        for (auto& field : table_info_ptr->fields) {
            if (field.short_name == "__snapshot__") {
                snapshot_field = field;
                break;
            }
        }
        snapshot_desc = record_template->get_field_by_idx(snapshot_field.pb_idx);
    }
    ColdFileSstWriter sst_writer(get_table_id(), get_partition_id(), _region_id, index_id, FLAGS_num_lines_per_sst);
    ScopeGuard auto_decrease([&sst_writer]() {
        auto fs = SstExtLinker::get_instance()->get_exteranl_filesystem();
        for (const std::string& external_file : sst_writer.external_files()) {
            DB_WARNING("delete external_file: %s", external_file.c_str());
            fs->delete_path(external_file, false);
        }
    });
    for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
        total_cnt++;
        if (!iter->key().starts_with(prefix)) {
            fail_cnt++;
            DB_FATAL("not starts with: %ld", _region_id);
            continue;
        }

        if (_need_snapshot_filter && 0 != table_info_ptr->snapshot_blacklist.size() && snapshot_desc != nullptr) {
            SmartRecord record = record_template->clone(false);
            TableKey table_key(iter->key(), false);
            int pos = prefix_len;
            record->decode_key(*(index_info.get()), table_key, pos);
            ExprValue val = record->get_value(snapshot_desc);
            if (table_info_ptr->snapshot_blacklist.count(val._u.uint64_val) != 0) {
                DB_DEBUG("table: %ld, index: %ld, flush hot to cold snapshot_blacklist filter val: %lu", get_table_id(), index_id, val._u.uint64_val);
                continue;
            }
        }

        int ret = sst_writer.write_kv(iter->key(), iter->value());
        if (ret < 0) {
            DB_FATAL("region_id: %ld write kv failed", _region_id);
            return -1;
        }
    }

    int ret = sst_writer.finish();
    if (ret < 0) {
        DB_FATAL("region_id: %ld finish failed", _region_id);
        return -1;
    }
    auto_decrease.release();
    new_index_ext_paths_mapping[index_id] = sst_writer.external_files();
    DB_NOTICE("region_id: %ld, index_id: %ld, flush total_cnt: %ld, fail_cnt: %ld, cost: %ld", _region_id, index_id, total_cnt, fail_cnt, cost.get_time());
    return 0;
}

// 手动link外部sst，需要保证olap_state状态为OLAP_ACTIVE或OLAP_TRUNCATED
int Region::manual_link_external_sst() {
    if (!need_flush_to_cold_rocksdb()) {
        return -1;
    }
    if (make_region_status_doing() != 0) {
        DB_WARNING("region status is not idle, region_id: %ld", _region_id);
        return -1;
    }   
    ON_SCOPE_EXIT([this]() {
        reset_region_status();
    });

    TimeCost cost;
    pb::OlapRegionInfo olap_info;
    int ret = _meta_writer->read_olap_info(_region_id, olap_info);
    if (ret < 0) {
        DB_FATAL("region_id: %ld read olap info failed", _region_id);
        return -1;
    } 

    if (olap_info.state() != pb::OLAP_ACTIVE && olap_info.state() != pb::OLAP_TRUNCATED) {
        DB_FATAL("region_id: %ld, cant ingest", _region_id);
        return -1;
    }

    std::string done_path = make_make_relative_path_without_filename(_table_id, get_partition_id());
    if (done_path.empty()) {
        return -1;
    }

    std::string done_file = done_path + "/" + std::to_string(_region_id) + "_done";

    std::shared_ptr<ExtFileSystem> fs = SstExtLinker::get_instance()->get_exteranl_filesystem();
    std::string external_done_file = fs->make_full_name("", false, done_file);
    if (external_done_file.empty()) {
        DB_FATAL("done_file: %s make full path failed", done_file.c_str());
        return -1;
    }

    std::string done_full_path = fs->make_full_name("", false, done_path);
    if (done_full_path.empty()) {
        DB_FATAL("done_path: %s make full path failed", done_path.c_str());
        return -1;
    }

    ret = fs->path_exists(external_done_file);
    if (ret <= 0) {
        DB_FATAL("external_done_file: %s not exists", external_done_file.c_str());
        return -1;
    }

    std::shared_ptr<ExtFileReader> file_reader;
    ret = fs->open_reader(external_done_file, &file_reader);
    if (ret != 0) {
        DB_FATAL("open ext file: %s failed", external_done_file.c_str());
        return -1;
    }

struct MemBuf : std::streambuf{
    MemBuf(char* begin, char* end) {
        this->setg(begin, begin, end);
    }
};
    std::vector<std::string> external_files;
    std::map<int64_t, std::vector<std::string> > index_ext_paths_mapping;
    bool eof = false;
    uint32_t buf_len = 1024 * 1024;
    std::shared_ptr<char[]> buf(new (std::nothrow) char[buf_len]);
    if (buf == nullptr) {
        DB_FATAL("no mem");
        return -1;
    }
    int64_t len = file_reader->read(buf.get(), buf_len, 0, &eof);
    if (eof && len > 0 && len < buf_len) {
        // done文件不会太大，当前只支持一次读完
        MemBuf sbuf(buf.get(), buf.get() + len);
        std::istream f(&sbuf);
        std::string line;
        while (std::getline(f, line)) {
            std::vector<std::string> vec;
            boost::split(vec, line, boost::is_any_of("/"));
            if (vec.empty()) {
                DB_FATAL("external file: %s", line.c_str());
                return -1;
            }
            DB_NOTICE("manual link sst region_id: %ld, file: %s", _region_id, line.c_str());
            external_files.emplace_back(done_full_path + "/" + vec[vec.size() - 1]);
        }
    } else {
        DB_FATAL("read done file: %s failed, len: %ld, buf_len: %u, eof: %d", external_done_file.c_str(), len, buf_len, eof);
        return -1;
    }

    ret = sync_olap_info(pb::OLAP_FLUSHED, external_files, index_ext_paths_mapping);
    if (ret < 0) {
        DB_FATAL("region_id: %ld sync olap info failed", _region_id);
        return -1;
    }
    
    sync_olap_info(pb::OLAP_TRUNCATED, external_files, index_ext_paths_mapping);
    return 0;
}

} // namespace baikaldb
