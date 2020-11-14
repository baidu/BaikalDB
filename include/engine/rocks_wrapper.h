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
            bool delete_files_in_range) {
        if (delete_files_in_range) {
            auto s = rocksdb::DeleteFilesInRange(_txn_db, column_family, &begin, &end, false);
            if (!s.ok()) {
                return s;
            }
        }
        return _txn_db->DeleteRange(options, column_family, begin, end);
    }
    
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
    void set_is_stall(const std::string& cf_name, bool is_stall) {
        if (cf_name == RAFT_LOG_CF) {
            _log_cf_is_stall = is_stall;
        } else if (cf_name == DATA_CF) {
            _data_cf_is_stall = is_stall;
        } else if (cf_name == METAINFO_CF) {
           _meta_cf_is_stall = is_stall;
        }
    }
    bool is_any_stall() {
        return _data_cf_is_stall || _log_cf_is_stall || _meta_cf_is_stall;
    }
private:

    RocksWrapper();

    std::string _db_path;

    bool _is_init;

    rocksdb::TransactionDB* _txn_db;
    rocksdb::Cache*         _cache;

    std::map<std::string, rocksdb::ColumnFamilyHandle*> _column_families;

    rocksdb::ColumnFamilyOptions _log_cf_option;
    rocksdb::ColumnFamilyOptions _binlog_cf_option;
    rocksdb::ColumnFamilyOptions _data_cf_option;
    rocksdb::ColumnFamilyOptions _meta_info_option;
    bool _log_cf_is_stall = false;
    bool _data_cf_is_stall = false;
    bool _meta_cf_is_stall = false;
};
}
