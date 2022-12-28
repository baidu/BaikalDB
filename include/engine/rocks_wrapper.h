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
 
#include <string>
#include "rocksdb/db.h"
#include "rocksdb/convenience.h"
#include "rocksdb/slice.h"
#include "rocksdb/cache.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "common.h"
//#include "proto/store.interface.pb.h"

namespace baikaldb {
DECLARE_int32(rocks_max_background_compactions);
DECLARE_int32(addpeer_rate_limit_level);
DECLARE_int32(level0_max_sst_num);

enum KVMode {
    KEY_ONLY,
    VAL_ONLY,
    KEY_VAL
};

enum GetMode {
    GET_ONLY,
    LOCK_ONLY,
    GET_LOCK
};

class RocksWrapper {
public:
    static const std::string RAFT_LOG_CF;
    static const std::string BIN_LOG_CF;
    static const std::string DATA_CF;
    static const std::string METAINFO_CF;
    static std::atomic<int64_t> raft_cf_remove_range_count;
    static std::atomic<int64_t> data_cf_remove_range_count;
    static std::atomic<int64_t> mata_cf_remove_range_count;

    virtual ~RocksWrapper() {}
    static RocksWrapper* get_instance() {
        static RocksWrapper _instance;
        return &_instance;
    }

    int32_t init(const std::string& path);

    rocksdb::Status write(const rocksdb::WriteOptions& options, rocksdb::WriteBatch* updates) {
        return _txn_db->Write(options, updates);  
    }
    rocksdb::Status write(const rocksdb::WriteOptions& options, 
                            rocksdb::ColumnFamilyHandle* column_family,
                            const std::vector<std::string>& keys,
                            const std::vector<std::string>& values) {
        rocksdb::WriteBatch batch;
        for (size_t i = 0; i < keys.size(); ++i) {
            batch.Put(column_family, keys[i], values[i]);
        }
        return _txn_db->Write(options, &batch);
    }

    rocksdb::Status get(const rocksdb::ReadOptions& options,
                        rocksdb::ColumnFamilyHandle* column_family, 
                        const rocksdb::Slice& key,
                        std::string* value) {
        return _txn_db->Get(options, column_family, key, value);
    }

    rocksdb::Status put(const rocksdb::WriteOptions& options,
                        rocksdb::ColumnFamilyHandle* column_family, 
                        const rocksdb::Slice& key,
                        const rocksdb::Slice& value) {
        return _txn_db->Put(options, column_family, key, value);
    }

    rocksdb::Transaction* begin_transaction(
            const rocksdb::WriteOptions& write_options,
            const rocksdb::TransactionOptions& txn_options) {
        return _txn_db->BeginTransaction(write_options, txn_options);
    }

    rocksdb::Status compact_range(const rocksdb::CompactRangeOptions& options,
                                  rocksdb::ColumnFamilyHandle* column_family,
                                  const rocksdb::Slice* begin,
                                  const rocksdb::Slice* end) {
        return _txn_db->CompactRange(options, column_family, begin, end);
    }

    rocksdb::Status flush(const rocksdb::FlushOptions& options,
                                  rocksdb::ColumnFamilyHandle* column_family) {
        return _txn_db->Flush(options, column_family);
    }

    rocksdb::Status remove(const rocksdb::WriteOptions& options,
                        rocksdb::ColumnFamilyHandle* column_family, 
                        const rocksdb::Slice& key) {
        return _txn_db->Delete(options, column_family, key);
    }

    // Consider setting ReadOptions::ignore_range_deletions = true to speed
    // up reads for key(s) that are known to be unaffected by range deletions.
    rocksdb::Status remove_range(const rocksdb::WriteOptions& options,
            rocksdb::ColumnFamilyHandle* column_family, 
            const rocksdb::Slice& begin, 
            const rocksdb::Slice& end,
            bool delete_files_in_range);

    int32_t get_binlog_value(int64_t ts, std::string& binlog_value);
    
    rocksdb::Iterator* new_iterator(const rocksdb::ReadOptions& options, 
                                    rocksdb::ColumnFamilyHandle* family) {
        return _txn_db->NewIterator(options, family);
    }

    rocksdb::Iterator* new_iterator(const rocksdb::ReadOptions& options, const std::string cf) {
        if (_column_families.count(cf) == 0) {
            return nullptr;
        }
        return _txn_db->NewIterator(options, _column_families[cf]);
    }
    rocksdb::Status ingest_external_file(rocksdb::ColumnFamilyHandle* family,
            const std::vector<std::string>& external_files,
            const rocksdb::IngestExternalFileOptions& options) {
        return _txn_db->IngestExternalFile(family, external_files, options);
    }

    rocksdb::ColumnFamilyHandle* get_raft_log_handle();
    
    rocksdb::ColumnFamilyHandle* get_bin_log_handle();

    rocksdb::ColumnFamilyHandle* get_data_handle();

    rocksdb::ColumnFamilyHandle* get_meta_info_handle();

    rocksdb::TransactionDB* get_db() {
        return _txn_db;
    }

    int32_t create_column_family(std::string cf_name);
    int32_t delete_column_family(std::string cf_name);

    rocksdb::Options get_options(rocksdb::ColumnFamilyHandle* family) {
        return _txn_db->GetOptions(family);
    }

    rocksdb::DBOptions get_db_options() {
        return _txn_db->GetDBOptions();
    }

    rocksdb::Cache* get_cache() {
        return _cache;
    }
    const rocksdb::Snapshot* get_snapshot() {
        return _txn_db->GetSnapshot();
    }
    void relase_snapshot(const rocksdb::Snapshot* snapshot) {
        _txn_db->ReleaseSnapshot(snapshot);
    }
    void close() {
        delete _txn_db;
    }
    bool is_any_stall() {
        uint64_t value = 0;
        if (FLAGS_addpeer_rate_limit_level >= 2) {
            _txn_db->GetIntProperty(get_data_handle(), "rocksdb.num-running-compactions", &value);
            if (value >= (uint64_t)FLAGS_rocks_max_background_compactions) {
                return true;
            }
            _txn_db->GetAggregatedIntProperty("rocksdb.estimate-pending-compaction-bytes", &value);
            uint64_t level0_sst = 0;
            uint64_t pending_compaction_size = 0;
            get_rocks_statistic(level0_sst, pending_compaction_size);
            if (level0_sst > _data_cf_option.level0_slowdown_writes_trigger * 0.6) {
                return true;
            }
            if (pending_compaction_size > _data_cf_option.soft_pending_compaction_bytes_limit / 2) {
                return true;
            }
        }
        if (FLAGS_addpeer_rate_limit_level >= 1) {
            _txn_db->GetAggregatedIntProperty("rocksdb.actual-delayed-write-rate", &value);
            if (value > 0) {
                return true;
            }
            _txn_db->GetAggregatedIntProperty("rocksdb.is-write-stopped", &value);
            if (value > 0) {
                return true;
            }
        }
        return false;
    }
    bool is_ingest_stall() {
        uint64_t level0_sst = 0;
        uint64_t pending_compaction_size = 0;
        get_rocks_statistic(level0_sst, pending_compaction_size);
        if (level0_sst > FLAGS_level0_max_sst_num) {
            return true;
        }
        return false;
    }
    void set_flush_file_number(const std::string& cf_name, uint64_t file_number) {
        if (cf_name == DATA_CF) {
            _flush_file_number = file_number;
        }
    }
    uint64_t flush_file_number() {
        return _flush_file_number;
    }
    void begin_split_adjust_option();
    void stop_split_adjust_option();
    void collect_rocks_options();
    void adjust_option(std::map<std::string, std::string> new_options);
    int get_rocks_statistic(uint64_t& level0_sst, uint64_t& pending_compaction_size) {
        rocksdb::ColumnFamilyMetaData cf_meta;
        _txn_db->GetColumnFamilyMetaData(get_data_handle(), &cf_meta);
        if (cf_meta.levels.size() == 0) {
            return -1;
        }
        level0_sst = cf_meta.levels[0].files.size();
        _txn_db->GetIntProperty(get_data_handle(), "rocksdb.estimate-pending-compaction-bytes", &pending_compaction_size);
        return 0;
    }
    void update_oldest_ts_in_binlog_cf();
    int64_t get_oldest_ts_in_binlog_cf() const {
        return _oldest_ts_in_binlog_cf;
    }
private:

    RocksWrapper();

    std::string _db_path;

    bool _is_init;

    rocksdb::TransactionDB* _txn_db;
    rocksdb::Cache*         _cache;

    std::map<std::string, rocksdb::ColumnFamilyHandle*> _column_families;
    rocksdb::ColumnFamilyHandle* _old_binlog_cf = nullptr;

    rocksdb::ColumnFamilyOptions _log_cf_option;
    rocksdb::ColumnFamilyOptions _binlog_cf_option;
    rocksdb::ColumnFamilyOptions _data_cf_option;
    rocksdb::ColumnFamilyOptions _meta_info_option;
    uint64_t _flush_file_number = 0;
    bvar::Adder<int64_t>     _raft_cf_remove_range_count;
    bvar::Adder<int64_t>     _data_cf_remove_range_count;
    bvar::Adder<int64_t>     _mata_cf_remove_range_count;

    std::atomic<int32_t> _split_num;
    bthread::Mutex _options_mutex;
    std::unordered_map<std::string, std::string> _rocks_options;
    std::map<std::string, std::string> _defined_options;
    int64_t _oldest_ts_in_binlog_cf = 0;
};
}
