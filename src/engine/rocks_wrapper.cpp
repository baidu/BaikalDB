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

#include "rocks_wrapper.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include <iostream>
#include "common.h"
#include "mut_table_key.h"
#include "table_key.h"
#include "my_listener.h"
#include "raft_log_compaction_filter.h"
#include "split_compaction_filter.h"
namespace baikaldb {

DEFINE_int32(rocks_transaction_lock_timeout_ms, 20000, "rocksdb transaction_lock_timeout, real lock_time is 'time + rand_less(time)' (ms)");
DEFINE_int32(rocks_default_lock_timeout_ms, 30000, "rocksdb default_lock_timeout(ms)");

DEFINE_bool(rocks_use_partitioned_index_filters, false, "rocksdb use Partitioned Index Filters");
DEFINE_int32(rocks_block_size, 64 * 1024, "rocksdb block_cache size, default: 64KB");
DEFINE_int64(rocks_block_cache_size_mb, 8 * 1024, "rocksdb block_cache_size_mb, default: 8G");
DEFINE_uint64(rocks_hard_pending_compaction_g, 256, "rocksdb hard_pending_compaction_bytes_limit , default: 256G");
DEFINE_double(rocks_level_multiplier, 10, "data_cf rocksdb max_bytes_for_level_multiplier, default: 10");
DEFINE_double(rocks_high_pri_pool_ratio, 0.5, "rocksdb cache high_pri_pool_ratio, default: 0.5");
DEFINE_int32(rocks_max_open_files, 1024, "rocksdb max_open_files, default: 1024");
DEFINE_int32(rocks_max_subcompactions, 4, "rocks_max_subcompactions");
DEFINE_int32(stop_write_sst_cnt, 40, "level0_stop_writes_trigger");
DEFINE_bool(rocks_kSkipAnyCorruptedRecords, false, 
        "We ignore any corruption in the WAL and try to salvage as much data as possible");
DEFINE_bool(rocks_data_dynamic_level_bytes, true, 
        "rocksdb level_compaction_dynamic_level_bytes for data column_family, default true");
DEFINE_int64(flush_memtable_interval_us, 10 * 60 * 1000 * 1000LL, 
            "flush memtable interval, defalut(10 min)");

DEFINE_int32(max_background_jobs, 24, "max_background_jobs");
DEFINE_int32(max_write_buffer_number, 6, "max_write_buffer_number");
DEFINE_int32(write_buffer_size, 128 * 1024 * 1024, "write_buffer_size");
DEFINE_int32(min_write_buffer_number_to_merge, 2, "min_write_buffer_number_to_merge");
DEFINE_int32(rocks_binlog_max_files_size_gb, 100, "binlog max size default 100G");
DEFINE_int32(rocks_binlog_ttl_days, 7, "binlog ttl default 7 days");

DEFINE_int32(level0_file_num_compaction_trigger, 5, "Number of files to trigger level-0 compaction");
DEFINE_int32(max_bytes_for_level_base, 1024 * 1024 * 1024, "total size of level 1.");
DEFINE_bool(enable_bottommost_compression, false, "enable zstd for bottommost_compression");

const std::string RocksWrapper::RAFT_LOG_CF = "raft_log";
const std::string RocksWrapper::BIN_LOG_CF  = "bin_log";
const std::string RocksWrapper::DATA_CF     = "data";
const std::string RocksWrapper::METAINFO_CF = "meta_info";

RocksWrapper::RocksWrapper() : _is_init(false), _txn_db(nullptr) {}
int32_t RocksWrapper::init(const std::string& path) {
    if (_is_init) {
        return 0;
    }
    std::shared_ptr<rocksdb::EventListener> my_listener = std::make_shared<MyListener>();
    rocksdb::BlockBasedTableOptions table_options;
    if (FLAGS_rocks_use_partitioned_index_filters) {
        // use Partitioned Index Filters
        // https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
        table_options.index_type = rocksdb::BlockBasedTableOptions::kTwoLevelIndexSearch;
        table_options.partition_filters = true;
        table_options.metadata_block_size = 4096;
        table_options.cache_index_and_filter_blocks = true;
        table_options.pin_top_level_index_and_filter = true;
        table_options.cache_index_and_filter_blocks_with_high_priority = true;
        table_options.pin_l0_filter_and_index_blocks_in_cache= true;
        table_options.block_cache = rocksdb::NewLRUCache(FLAGS_rocks_block_cache_size_mb * 1024 * 1024, 
            -1, false, FLAGS_rocks_high_pri_pool_ratio);
        // 通过cache控制内存，不需要控制max_open_files
        FLAGS_rocks_max_open_files = -1;
    } else {
        table_options.index_type = rocksdb::BlockBasedTableOptions::kHashSearch;
        table_options.block_cache = rocksdb::NewLRUCache(64 * 1024 * 1024);
    }
    table_options.data_block_index_type = rocksdb::BlockBasedTableOptions::kDataBlockBinaryAndHash;
    table_options.format_version = 4;
    
    table_options.block_size = FLAGS_rocks_block_size;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
    _cache = table_options.block_cache.get();
    rocksdb::Options db_options;
    db_options.IncreaseParallelism(FLAGS_max_background_jobs);
    db_options.create_if_missing = true;
    db_options.max_open_files = FLAGS_rocks_max_open_files;
    db_options.WAL_ttl_seconds = 10 * 60;
    db_options.WAL_size_limit_MB = 0;
    //打开后有些集群内存严重上涨
    //db_options.avoid_unnecessary_blocking_io = true;
    db_options.max_background_compactions = 20;
    if (FLAGS_rocks_kSkipAnyCorruptedRecords) {
        db_options.wal_recovery_mode = rocksdb::WALRecoveryMode::kSkipAnyCorruptedRecords;
    }
    db_options.statistics = rocksdb::CreateDBStatistics();
    db_options.max_subcompactions = FLAGS_rocks_max_subcompactions;
    db_options.max_background_flushes = 2;
    db_options.env->SetBackgroundThreads(2, rocksdb::Env::HIGH);
    db_options.listeners.emplace_back(my_listener);
    rocksdb::TransactionDBOptions txn_db_options;
    DB_NOTICE("FLAGS_rocks_transaction_lock_timeout_ms:%d FLAGS_rocks_default_lock_timeout_ms:%d", FLAGS_rocks_transaction_lock_timeout_ms, FLAGS_rocks_default_lock_timeout_ms);
    txn_db_options.transaction_lock_timeout = FLAGS_rocks_transaction_lock_timeout_ms;
    txn_db_options.default_lock_timeout = FLAGS_rocks_default_lock_timeout_ms;

    //todo 
    _log_cf_option.prefix_extractor.reset(
            rocksdb::NewFixedPrefixTransform(sizeof(int64_t) + 1));
    _log_cf_option.OptimizeLevelStyleCompaction();
    _log_cf_option.compaction_pri = rocksdb::kOldestLargestSeqFirst;
    _log_cf_option.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    _log_cf_option.compaction_style = rocksdb::kCompactionStyleLevel;
    _log_cf_option.level0_file_num_compaction_trigger = 5;
    _log_cf_option.level0_slowdown_writes_trigger = 10;
    _log_cf_option.level0_stop_writes_trigger = FLAGS_stop_write_sst_cnt;
    _log_cf_option.target_file_size_base = 128 * 1024 * 1024;
    _log_cf_option.max_bytes_for_level_base = 1024 * 1024 * 1024;
    _log_cf_option.level_compaction_dynamic_level_bytes = FLAGS_rocks_data_dynamic_level_bytes;

    _log_cf_option.max_write_buffer_number = FLAGS_max_write_buffer_number;
    _log_cf_option.write_buffer_size = FLAGS_write_buffer_size;
    _log_cf_option.min_write_buffer_number_to_merge = FLAGS_min_write_buffer_number_to_merge;

    if (FLAGS_rocks_use_partitioned_index_filters) {
        table_options.pin_l0_filter_and_index_blocks_in_cache= false;
        _binlog_cf_option.ttl = FLAGS_rocks_binlog_ttl_days * 24 * 60 * 60;
    }
    _binlog_cf_option.prefix_extractor.reset(
            rocksdb::NewFixedPrefixTransform(sizeof(int64_t)));
    _binlog_cf_option.OptimizeLevelStyleCompaction();
    _binlog_cf_option.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    _binlog_cf_option.compaction_style = rocksdb::kCompactionStyleFIFO;
    rocksdb::CompactionOptionsFIFO fifo_option;
    //如果观察到TTL无法让文件总数量少于配置的大小，RocksDB会暂时下降到基于大小的FIFO删除
    //https://rocksdb.org.cn/doc/FIFO-compaction-style.html
    fifo_option.max_table_files_size = FLAGS_rocks_binlog_max_files_size_gb * 1024 * 1024 * 1024LL; 
    //fifo_option.ttl = 7 * 24 * 60 * 60; // 7day
    _binlog_cf_option.compaction_options_fifo = fifo_option;

    //todo
    // prefix length: regionid(8 Bytes) tableid(8 Bytes)
    _data_cf_option.prefix_extractor.reset(
            rocksdb::NewFixedPrefixTransform(sizeof(int64_t) * 2));
    _data_cf_option.OptimizeLevelStyleCompaction();
    _data_cf_option.compaction_pri = rocksdb::kByCompensatedSize;
    _data_cf_option.compaction_filter = SplitCompactionFilter::get_instance();
    _data_cf_option.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    _data_cf_option.compaction_style = rocksdb::kCompactionStyleLevel;
//    _data_cf_option.level0_file_num_compaction_trigger = 5;
    _data_cf_option.level0_slowdown_writes_trigger = 10;
    _data_cf_option.level0_stop_writes_trigger = FLAGS_stop_write_sst_cnt;
    _data_cf_option.hard_pending_compaction_bytes_limit = FLAGS_rocks_hard_pending_compaction_g * 1073741824ull;
    _data_cf_option.target_file_size_base = 128 * 1024 * 1024;
    _data_cf_option.max_bytes_for_level_multiplier = FLAGS_rocks_level_multiplier;
    _data_cf_option.level_compaction_dynamic_level_bytes = FLAGS_rocks_data_dynamic_level_bytes;

    _data_cf_option.max_write_buffer_number = FLAGS_max_write_buffer_number;
    _data_cf_option.write_buffer_size = FLAGS_write_buffer_size;
    _data_cf_option.min_write_buffer_number_to_merge = FLAGS_min_write_buffer_number_to_merge;

    _data_cf_option.level0_file_num_compaction_trigger = FLAGS_level0_file_num_compaction_trigger;
    _data_cf_option.max_bytes_for_level_base = FLAGS_max_bytes_for_level_base;

    if (FLAGS_enable_bottommost_compression) {
        _data_cf_option.bottommost_compression_opts.enabled = true;
        _data_cf_option.bottommost_compression = rocksdb::kZSTD;
        _data_cf_option.bottommost_compression_opts.max_dict_bytes = 1 << 14; // 16KB
        _data_cf_option.bottommost_compression_opts.zstd_max_train_bytes = 1 << 18; // 256KB
    }

    //todo
    //prefix: 0x01-0xFF,分别用来存储不同的meta信息
    _meta_info_option.prefix_extractor.reset( 
            rocksdb::NewFixedPrefixTransform(1));
    _meta_info_option.OptimizeLevelStyleCompaction();
    _meta_info_option.compaction_pri = rocksdb::kOldestSmallestSeqFirst;
    _meta_info_option.level_compaction_dynamic_level_bytes = FLAGS_rocks_data_dynamic_level_bytes;

    _db_path = path;
    // List Column Family
    std::vector<std::string> column_family_names;
    rocksdb::Status s;
    s = rocksdb::DB::ListColumnFamilies(db_options, path, &column_family_names);
    //db已存在
    if (s.ok()) {
        std::vector<rocksdb::ColumnFamilyDescriptor> column_family_desc;
        std::vector<rocksdb::ColumnFamilyHandle*> handles;
        for (auto& column_family_name : column_family_names) {
            if (column_family_name == RAFT_LOG_CF) {
                column_family_desc.push_back(rocksdb::ColumnFamilyDescriptor(RAFT_LOG_CF, _log_cf_option));
            } else if (column_family_name == BIN_LOG_CF) {
                column_family_desc.push_back(rocksdb::ColumnFamilyDescriptor(BIN_LOG_CF, _binlog_cf_option));
            } else if (column_family_name == DATA_CF) {
                column_family_desc.push_back(rocksdb::ColumnFamilyDescriptor(DATA_CF, _data_cf_option));
            } else if (column_family_name == METAINFO_CF) {
                column_family_desc.push_back(rocksdb::ColumnFamilyDescriptor(METAINFO_CF, _meta_info_option));
            } else {
                column_family_desc.push_back(
                        rocksdb::ColumnFamilyDescriptor(column_family_name, 
                            rocksdb::ColumnFamilyOptions()));
            }
        }
        s = rocksdb::TransactionDB::Open(db_options,
                txn_db_options,
                path,
                column_family_desc,
                &handles,
                &_txn_db);
        if (s.ok()) {
            DB_WARNING("reopen db:%s success", path.c_str());
            for (auto& handle : handles) {
                _column_families[handle->GetName()] = handle;
                DB_WARNING("open column family:%s", handle->GetName().c_str());
            }
        } else {
            DB_FATAL("reopen db:%s fail, err_message:%s", path.c_str(), s.ToString().c_str());
            return -1;
        }
    } else {
        // new db
        s = rocksdb::TransactionDB::Open(db_options, txn_db_options, path, &_txn_db);
        if (s.ok()) {
            DB_WARNING("open db:%s success", path.c_str());
        } else {
            DB_FATAL("open db:%s fail, err_message:%s", path.c_str(), s.ToString().c_str());
            return -1;
        }
    }
    if (0 == _column_families.count(RAFT_LOG_CF)) {
        //create raft_log column_familiy
        rocksdb::ColumnFamilyHandle* raft_log_handle;
        s = _txn_db->CreateColumnFamily(_log_cf_option, RAFT_LOG_CF, &raft_log_handle);
        if (s.ok()) {
            DB_WARNING("create column family success, column family:%s", RAFT_LOG_CF.c_str());
            _column_families[RAFT_LOG_CF] = raft_log_handle;
        } else {
            DB_FATAL("create column family fail, column family:%s, err_message:%s",
                    RAFT_LOG_CF.c_str(), s.ToString().c_str());
            return -1;
        }
    }
    if (0 == _column_families.count(DATA_CF)) {
        //create data column_family
        rocksdb::ColumnFamilyHandle* data_handle;
        s =  _txn_db->CreateColumnFamily(_data_cf_option, DATA_CF, &data_handle);
        if (s.ok()) {
            DB_WARNING("create column family success, column family:%s", DATA_CF.c_str());
            _column_families[DATA_CF] = data_handle;
        } else {
            DB_FATAL("create column family fail, column family:%s, err_message:%s",
                    DATA_CF.c_str(), s.ToString().c_str());
            return -1;
        }
    }
    if (0 == _column_families.count(METAINFO_CF)) {
        rocksdb::ColumnFamilyHandle* metainfo_handle;
        s = _txn_db->CreateColumnFamily(_meta_info_option, METAINFO_CF, &metainfo_handle);
        if (s.ok()) {
            DB_WARNING("create column family success, column family:%s", METAINFO_CF.c_str());
            _column_families[METAINFO_CF] = metainfo_handle;
        } else {
            DB_FATAL("create column family fail, column family:%s, err_message:%s",
                    METAINFO_CF.c_str(), s.ToString().c_str());
            return -1;
        }
    }
    if (0 == _column_families.count(BIN_LOG_CF)) {
        //create bin_log column_familiy
        rocksdb::ColumnFamilyHandle* bin_log_handle;
        s = _txn_db->CreateColumnFamily(_binlog_cf_option, BIN_LOG_CF, &bin_log_handle);
        if (s.ok()) {
            DB_WARNING("create column family success, column family:%s", BIN_LOG_CF.c_str());
            _column_families[BIN_LOG_CF] = bin_log_handle;
        } else {
            DB_FATAL("create column family fail, column family:%s, err_message:%s",
                    BIN_LOG_CF.c_str(), s.ToString().c_str());
            return -1;
        }
    }
    _is_init = true;
    DB_WARNING("rocksdb init success");
    return 0;
}
int32_t RocksWrapper::delete_column_family(std::string cf_name) {
    if (_column_families.count(cf_name) == 0) {
        DB_FATAL("column_family: %s not exist", cf_name.c_str());
        return -1;
    }
    rocksdb::ColumnFamilyHandle* cf_handler = _column_families[cf_name];
    auto res = _txn_db->DropColumnFamily(cf_handler);
    if (!res.ok()) {
        DB_FATAL("drop column_family %s failed, err_message:%s", 
                cf_name.c_str(), res.ToString().c_str());
        return -1;
    }
    res = _txn_db->DestroyColumnFamilyHandle(cf_handler);
    if (!res.ok()) {
        DB_FATAL("destroy column_family %s failed, err_message:%s", 
                cf_name.c_str(), res.ToString().c_str());
        return -1;
    }
    _column_families.erase(cf_name);
    return 0;
}
int32_t RocksWrapper::create_column_family(std::string cf_name) {
    if (_column_families.count(cf_name) != 0) {
        DB_FATAL("column_family: %s already exist", cf_name.c_str());
        return -1;
    }
    rocksdb::ColumnFamilyHandle* cf_handler = nullptr;
    auto s = _txn_db->CreateColumnFamily(_data_cf_option, cf_name, &cf_handler);
    if (s.ok()) {
        DB_WARNING("create column family %s success", cf_name.c_str());
        _column_families[cf_name] = cf_handler;
    } else {
        DB_FATAL("create column family %s fail, err_message:%s",
                cf_name.c_str(), s.ToString().c_str());
        return -1;
    }
    _column_families[cf_name] = cf_handler;
    return 0;
}

rocksdb::ColumnFamilyHandle* RocksWrapper::get_raft_log_handle() {
    if (!_is_init) {
        DB_FATAL("rocksdb has not been inited");
        return nullptr;
    }
    if (0 == _column_families.count(RAFT_LOG_CF)) {
        DB_FATAL("rocksdb has no raft log cf");
        return nullptr;
    }
    return _column_families[RAFT_LOG_CF];
}
rocksdb::ColumnFamilyHandle* RocksWrapper::get_bin_log_handle() {
    if (!_is_init) {
        DB_FATAL("rocksdb has not been inited");
        return nullptr;
    }
    if (0 == _column_families.count(BIN_LOG_CF)) {
        DB_FATAL("rocksdb has no bin log cf");
        return nullptr;
    }
    return _column_families[BIN_LOG_CF];
}
rocksdb::ColumnFamilyHandle* RocksWrapper::get_data_handle() {
    if (!_is_init) {
        DB_FATAL("rocksdb has not been inited");
        return nullptr;
    }
    if (0 == _column_families.count(DATA_CF)) {
        DB_FATAL("rocksdb has no data column family");
        return nullptr;
    }
    return _column_families[DATA_CF];
}
rocksdb::ColumnFamilyHandle* RocksWrapper::get_meta_info_handle() {
    if (!_is_init) {
        DB_FATAL("rocksdb has not been inited");
        return nullptr;
    }
    if (0 == _column_families.count(METAINFO_CF)) {
        DB_FATAL("rocksdb has no metainfo column family");
        return nullptr;
    }
    return _column_families[METAINFO_CF];
}

}
