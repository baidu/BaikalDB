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

int Region::get_hot_sst(bool do_compaction_if_need, std::vector<rocksdb::LiveFileMetaData>& sst_file_meta) {
    sst_file_meta.clear();
    TimeCost cost;
    bool need_flush = true;
    bool need_compaction = false;
    // 列出所有的sst
    std::vector<rocksdb::LiveFileMetaData> metadata;
    _rocksdb->get_db()->GetLiveFilesMetaData(&metadata);
    for (const auto& md : metadata) {
        if (md.column_family_name == RocksWrapper::DATA_CF) {
            TableKey smallestkey(md.smallestkey);
            TableKey largestkey(md.largestkey);
            int64_t smallest_region_id = smallestkey.extract_i64(0);
            int64_t largest_region_id  = largestkey.extract_i64(0);
            if (smallest_region_id == largest_region_id && smallest_region_id == get_region_id()) {
                // 只有sst数据全部属于这个region才可以拷贝
                sst_file_meta.emplace_back(md);
            } else if (smallest_region_id <= get_region_id() && get_region_id() <= largest_region_id) {
                // sst混合了该region和其他region的数据，不能flush
                sst_file_meta.emplace_back(md);
                need_compaction = true;
                need_flush = false;
            }
        }
    }

    bool doing_compaction = false;
    for (const auto& md : sst_file_meta) {
        print_metadata_info(md);
        if (md.being_compacted) {
            // 正在compaction暂时不执行
            doing_compaction = true;
            need_flush = false;
        }

        if (md.level != 6) {
            // 不是在最后一层需要触发compaction后执行
            need_compaction = true;
            need_flush = false;
        }
    }

    if (need_compaction && !doing_compaction && do_compaction_if_need) {
        compact_data_in_queue();
    }

    if (!need_flush) {
        sst_file_meta.clear();
        return -1;
    }

    return 0;
}

int Region::check_hot_sst(const std::vector<rocksdb::LiveFileMetaData>& sst_files) {
    std::vector<rocksdb::LiveFileMetaData> tmp_sst_files;
    int ret = get_hot_sst(false, tmp_sst_files);
    if (ret < 0) {
        DB_FATAL("region_id: %ld get live sst failed", _region_id);
        return -1;
    }

    std::set<std::string> set1;
    std::set<std::string> set2;
    for (const auto& info : sst_files) {
        set1.insert(info.relative_filename);
    }
    for (const auto& info : tmp_sst_files) {
        set2.insert(info.relative_filename);
    }

    return set_diff(set1, set2) ? -1 : 0;
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
        print_metadata_info(md);
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

    if (olap_info.state() == pb::OLAP_ACTIVE) {
        if (!has_sst_data(nullptr)) {
            DB_WARNING("region_id: %ld no sst data", _region_id);
            return 0;
        }
        std::vector<std::string> external_files;
        ret = sync_olap_info(pb::OLAP_IMMUTABLE, external_files);
        if (ret < 0) {
            return -1;
        }
    } else if (olap_info.state() == pb::OLAP_IMMUTABLE) {
        if (!has_sst_data(nullptr)) {
            DB_FATAL("region_id: %ld no sst data", _region_id);
            return 0;
        }

        uint64_t now = (uint64_t)time(nullptr);
        if ((now - olap_info.state_time()) < FLAGS_hot_rocksdb_truncate_interval_s) {
            // 等待一个周期之后再执行下一个状态
            return 0;
        }
        // std::vector<rocksdb::LiveFileMetaData> sst_files;
        // ret = get_hot_sst(true, sst_files);
        // if (ret < 0) {
        //     DB_WARNING("get live sst failed");
        //     return -1;
        // }

        std::vector<std::string> external_files;
        ret = flush_hot_to_cold(external_files);
        if (ret < 0) {
            DB_FATAL("flush_hot_to_cold failed");
            return -1;
        }

        ret = sync_olap_info(pb::OLAP_FLUSHED, external_files);
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
            ret = sync_olap_info(pb::OLAP_TRUNCATED, external_files);
            if (ret < 0) {
                return -1;
            }
        }
    } 

    return 0;
}

// 暂时不用
void Region::apply_kv_olap(const pb::StoreReq& request, braft::Closure* done, 
                              int64_t index, int64_t term) {
    TimeCost cost;

    rocksdb::WriteOptions write_option;
    // write_option.disableWAL = true; // 实现raft log 异常恢复后设置为true
    rocksdb::WriteBatch batch;
    bool need_replace_region_id = false;
    if (_region_id != request.region_id()) {
        DB_WARNING("diff region_id: %ld, %ld. maybe split", _region_id, request.region_id());
        need_replace_region_id = true;
    }
    int64_t put_rows = 0;
    int64_t merge_rows = 0;
    int64_t delete_rows = 0;
    for (auto& kv_op : request.kv_ops()) {
        pb::OpType op_type = kv_op.op_type();
        int ret = 0;
        if (BAIKALDB_LIKELY(!need_replace_region_id)) {
            if (BAIKALDB_LIKELY(op_type == pb::OP_MERGE_KV)) {     
                batch.Merge(_data_cf, kv_op.key(), kv_op.value());
                merge_rows++;   
            } else if (op_type == pb::OP_PUT_KV) {
                batch.Put(_data_cf, kv_op.key(), kv_op.value());
                put_rows++; 
            } else {
                batch.Delete(_data_cf, kv_op.key());
                delete_rows++;
            }
        } else {
            MutTableKey key(kv_op.key());
            key.replace_i64(_region_id, 0);
            if (BAIKALDB_LIKELY(op_type == pb::OP_MERGE_KV)) {     
                batch.Merge(_data_cf, key.data(), kv_op.value());
                merge_rows++;  
            } else if (op_type == pb::OP_PUT_KV) {
                batch.Put(_data_cf, key.data(), kv_op.value());
                put_rows++;   
            } else {
                batch.Delete(_data_cf, key.data());
                delete_rows++;   
            }
        }
    }

    _num_table_lines += request.kv_ops_size();

    batch.Put(_meta_cf, MetaWriter::get_instance()->applied_index_key(_region_id), MetaWriter::get_instance()->encode_applied_index(index, _data_index));
    batch.Put(_meta_cf, MetaWriter::get_instance()->num_table_lines_key(_region_id), MetaWriter::get_instance()->encode_num_table_lines(_num_table_lines));

    auto status = _rocksdb->write(write_option, &batch);
    if (!status.ok()) {
        DB_FATAL("region_id: %ld put batch to rocksdb fail, log_index: %ld, err_msg: %s",
                    _region_id, index, status.ToString().c_str());
        if (done != nullptr) {
            ((DMLClosure*)done)->response->set_errcode(pb::EXEC_FAIL);
            ((DMLClosure*)done)->response->set_errmsg("rocksdb put failed");
            // leader也在状态机中写入，txn直接rollback
            SmartTransaction txn = ((DMLClosure*)done)->transaction;
            if (txn != nullptr) {
                txn->rollback();
            }
        }
        return;
    }

    if (done != nullptr) {
        ((DMLClosure*)done)->response->set_errcode(pb::SUCCESS);
        ((DMLClosure*)done)->response->set_errmsg("success");
        // leader也在状态机中写入，txn直接rollback
        SmartTransaction txn = ((DMLClosure*)done)->transaction;
        if (txn != nullptr) {
            txn->rollback();
        }
    }

    int64_t dml_cost = cost.get_time();
    Store::get_instance()->dml_time_cost << dml_cost;
    // if (dml_cost > FLAGS_print_time_us) {
        DB_NOTICE("time_cost:%ld, region_id: %ld, table_lines:%ld, put_rows: %ld, merge_rows: %ld, delete_rows: %ld, "
            "applied_index:%ld, term:%ld", cost.get_time(), _region_id, _num_table_lines.load(), 
            put_rows, merge_rows, delete_rows, index, term);
    // }
    return;
}

void Region::apply_olap_info(const pb::StoreReq& request, braft::Closure* done) {
    const pb::OlapRegionInfo& olap_info = request.extra_req().olap_info();
    int ret = 0;
    TimeCost cost;
    std::vector<std::string> external_files;
    external_files.reserve(5);
    for (const auto& file : olap_info.external_full_path()) {
        external_files.emplace_back(file);
    }
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    if (olap_info.state() == pb::OLAP_FLUSHED) {
        // 将external file ingest到rocksdb
        ret = ingest_cold_sst(external_files);
        if (ret < 0) {
            IF_DONE_SET_RESPONSE_FATAL(done, pb::EXEC_FAIL, "ingest cold sst failed");
            return;
        }
    } else if (olap_info.state() == pb::OLAP_TRUNCATED) {
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

int Region::sync_olap_info(pb::OlapRegionStat state, const std::vector<std::string>& external_files) {
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

// int Region::copy_files(const std::vector<rocksdb::LiveFileMetaData>& sst_files, std::vector<std::string>& external_files) {
//     // 失败残留external文件由垃圾回收机制进行删除 OLAPTODO
//     for (const auto& sst : sst_files) {
//         std::string external_file;
//         int ret = copy_file(sst, external_file);
//         if (ret < 0) {
//             DB_FATAL("regiond_id: %ld copy local file: %s failed", _region_id, sst.relative_filename.c_str());
//             return -1;
//         }

//         if (!need_flush_to_cold_rocksdb()) {
//             DB_FATAL("region_id: %ld can not do flush", _region_id);
//             return -1;
//         }

//         // 检查状态是否处于IMMUTABLE
//         pb::OlapRegionInfo olap_info;
//         ret = _meta_writer->read_olap_info(_region_id, olap_info);
//         if (ret < 0) {
//             DB_FATAL("region_id: %ld read olap info failed", _region_id);
//             return -1;
//         } 

//         if (olap_info.state() != pb::OLAP_IMMUTABLE) {
//             DB_FATAL("region_id: %ld, olap state is not OLAP_IMMUTABLE, olap_info: %s", _region_id, olap_info.ShortDebugString().c_str());
//             return -1;
//         }

//         // 检查sst是否发生变动
//         ret = check_hot_sst(sst_files);
//         if (ret < 0) {
//             DB_FATAL("region_id: %ld check hot sst failed", _region_id);
//             return -1;
//         }

//         external_files.emplace_back(external_file);
//     }

//     return 0;
// }

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
    ColdFileSstWriter(int64_t table_id, int64_t partition_id, int64_t region_id, int64_t count_per_sst) : 
        _table_id(table_id), _partition_id(partition_id), _region_id(region_id), _count_per_sst(count_per_sst) {
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
            DB_FATAL("region_id: %ld open sst file: %s fail", _region_id, _tmp_file_name.c_str());
            return -1;
        }
    }

    s = _writer->put(key, value);
    if (!s.ok()) {
        DB_FATAL("region_id: %ld write kv fail", _region_id);
        return -1;
    }

    if (++_write_count < _count_per_sst) {
        // 小于预估每个sst的行数，返回再次写入
        return 0;
    }

    // flush
    return finish();
}
int Region::flush_hot_to_cold(std::vector<std::string>& external_files) {
    TimeCost cost;
    std::string prefix;
    MutTableKey key;
    key.append_i64(_region_id);
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
    ColdFileSstWriter sst_writer(get_table_id(), get_partition_id(), _region_id, FLAGS_num_lines_per_sst);
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
    external_files = sst_writer.external_files();
    DB_NOTICE("region_id: %ld flush total_cnt: %ld, fail_cnt: %ld, cost: %ld", _region_id, total_cnt, fail_cnt, cost.get_time());
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

    std::unique_ptr<ExtFileReader> file_reader;
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

    ret = sync_olap_info(pb::OLAP_FLUSHED, external_files);
    if (ret < 0) {
        DB_FATAL("region_id: %ld sync olap info failed", _region_id);
        return -1;
    }
    
    sync_olap_info(pb::OLAP_TRUNCATED, external_files);
    return 0;
}

} // namespace baikaldb