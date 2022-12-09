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
#include "transaction_db_bthread_mutex.h"
namespace baikaldb {

DEFINE_int32(rocks_transaction_lock_timeout_ms, 20000, "rocksdb transaction_lock_timeout, real lock_time is 'time + rand_less(time)' (ms)");
DEFINE_int32(rocks_default_lock_timeout_ms, 30000, "rocksdb default_lock_timeout(ms)");

DEFINE_bool(rocks_use_partitioned_index_filters, false, "rocksdb use Partitioned Index Filters");
DEFINE_bool(rocks_skip_stats_update_on_db_open, false, "rocks_skip_stats_update_on_db_open");
DEFINE_int32(rocks_block_size, 64 * 1024, "rocksdb block_cache size, default: 64KB");
DEFINE_int64(rocks_block_cache_size_mb, 8 * 1024, "rocksdb block_cache_size_mb, default: 8G");
DEFINE_uint64(rocks_hard_pending_compaction_g, 256, "rocksdb hard_pending_compaction_bytes_limit , default: 256G");
DEFINE_uint64(rocks_soft_pending_compaction_g, 64, "rocksdb soft_pending_compaction_bytes_limit , default: 64G");
DEFINE_uint64(rocks_compaction_readahead_size, 0, "rocksdb compaction_readahead_size, default: 0");
DEFINE_int32(rocks_data_compaction_pri, 3, "rocksdb data_cf compaction_pri, default: 3(kMinOverlappingRatio)");
DEFINE_double(rocks_level_multiplier, 10, "data_cf rocksdb max_bytes_for_level_multiplier, default: 10");
DEFINE_double(rocks_high_pri_pool_ratio, 0.5, "rocksdb cache high_pri_pool_ratio, default: 0.5");
DEFINE_int32(rocks_max_open_files, 1024, "rocksdb max_open_files, default: 1024");
DEFINE_int32(rocks_max_subcompactions, 4, "rocks_max_subcompactions");
DEFINE_int32(rocks_max_background_compactions, 20, "max_background_compactions");
DEFINE_bool(rocks_optimize_filters_for_hits, false, "rocks_optimize_filters_for_hits");
DEFINE_int32(slowdown_write_sst_cnt, 10, "level0_slowdown_writes_trigger");
DEFINE_int32(stop_write_sst_cnt, 40, "level0_stop_writes_trigger");
DEFINE_bool(rocks_kSkipAnyCorruptedRecords, false,
        "We ignore any corruption in the WAL and try to salvage as much data as possible");
DEFINE_bool(rocks_data_dynamic_level_bytes, true,
        "rocksdb level_compaction_dynamic_level_bytes for data column_family, default true");
DEFINE_int64(flush_memtable_interval_us, 10 * 60 * 1000 * 1000LL,
            "flush memtable interval, default(10 min)");
DEFINE_int32(max_background_jobs, 24, "max_background_jobs");
DEFINE_int32(max_write_buffer_number, 6, "max_write_buffer_number");
DEFINE_int32(write_buffer_size, 128 * 1024 * 1024, "write_buffer_size");
DEFINE_int32(min_write_buffer_number_to_merge, 2, "min_write_buffer_number_to_merge");
DEFINE_int32(rocks_binlog_max_files_size_gb, 100, "binlog max size default 100G");
DEFINE_int32(rocks_binlog_ttl_days, 7, "binlog ttl default 7 days");

DEFINE_int32(level0_file_num_compaction_trigger, 5, "Number of files to trigger level-0 compaction");
DEFINE_int32(max_bytes_for_level_base, 1024 * 1024 * 1024, "total size of level 1.");
DEFINE_bool(enable_bottommost_compression, false, "enable zstd for bottommost_compression");
DEFINE_int32(target_file_size_base, 128 * 1024 * 1024, "target_file_size_base");
DEFINE_int32(addpeer_rate_limit_level, 1, "addpeer_rate_limit_level; "
        "0:no limit, 1:limit when stalling, 2:limit when compaction pending. default(1)");
DEFINE_bool(delete_files_in_range, true, "delete_files_in_range");
DEFINE_bool(l0_compaction_use_lz4, true, "L0 sst compaction use lz4 or not");
DEFINE_bool(real_delete_old_binlog_cf, true, "default true");
DEFINE_bool(rocksdb_fifo_allow_compaction, false, "default false");

const std::string RocksWrapper::RAFT_LOG_CF = "raft_log";
const std::string RocksWrapper::BIN_LOG_CF  = "bin_log_new";
const std::string RocksWrapper::DATA_CF     = "data";
const std::string RocksWrapper::METAINFO_CF = "meta_info";
std::atomic<int64_t> RocksWrapper::raft_cf_remove_range_count = {0};
std::atomic<int64_t> RocksWrapper::data_cf_remove_range_count = {0};
std::atomic<int64_t> RocksWrapper::mata_cf_remove_range_count = {0};

RocksWrapper::RocksWrapper() : _is_init(false), _txn_db(nullptr)
    , _raft_cf_remove_range_count("raft_cf_remove_range_count")
    , _data_cf_remove_range_count("data_cf_remove_range_count")
    , _mata_cf_remove_range_count("mata_cf_remove_range_count") {
}
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
        table_options.block_cache = rocksdb::NewLRUCache(FLAGS_rocks_block_cache_size_mb * 1024 * 1024LL,
            8, false, FLAGS_rocks_high_pri_pool_ratio);
        // 通过cache控制内存，不需要控制max_open_files
        FLAGS_rocks_max_open_files = -1;
    } else {
        table_options.data_block_index_type = rocksdb::BlockBasedTableOptions::kDataBlockBinaryAndHash;
        table_options.block_cache = rocksdb::NewLRUCache(FLAGS_rocks_block_cache_size_mb * 1024 * 1024LL, 8);
    }
    table_options.format_version = 4;

    table_options.block_size = FLAGS_rocks_block_size;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
    _cache = table_options.block_cache.get();
    rocksdb::Options db_options;
    db_options.IncreaseParallelism(FLAGS_max_background_jobs);
    db_options.create_if_missing = true;
    db_options.max_open_files = FLAGS_rocks_max_open_files;
    db_options.skip_stats_update_on_db_open = FLAGS_rocks_skip_stats_update_on_db_open;
    db_options.compaction_readahead_size = FLAGS_rocks_compaction_readahead_size;
    db_options.WAL_ttl_seconds = 10 * 60;
    db_options.WAL_size_limit_MB = 0;
    //打开后有些集群内存严重上涨
    //db_options.avoid_unnecessary_blocking_io = true;
    db_options.max_background_compactions = FLAGS_rocks_max_background_compactions;
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
    txn_db_options.custom_mutex_factory = std::shared_ptr<rocksdb::TransactionDBMutexFactory>(
                          new TransactionDBBthreadFactory());

    //todo
    _log_cf_option.prefix_extractor.reset(
            rocksdb::NewFixedPrefixTransform(sizeof(int64_t) + 1));
    _log_cf_option.OptimizeLevelStyleCompaction();
    _log_cf_option.compaction_pri = rocksdb::kOldestLargestSeqFirst;
    _log_cf_option.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    _log_cf_option.compaction_style = rocksdb::kCompactionStyleLevel;
    _log_cf_option.level0_file_num_compaction_trigger = 5;
    _log_cf_option.level0_slowdown_writes_trigger = FLAGS_slowdown_write_sst_cnt;
    _log_cf_option.level0_stop_writes_trigger = FLAGS_stop_write_sst_cnt;
    _log_cf_option.target_file_size_base = FLAGS_target_file_size_base;
    _log_cf_option.max_bytes_for_level_base = 1024 * 1024 * 1024;
    _log_cf_option.level_compaction_dynamic_level_bytes = FLAGS_rocks_data_dynamic_level_bytes;

    _log_cf_option.max_write_buffer_number = FLAGS_max_write_buffer_number;
    _log_cf_option.max_write_buffer_number_to_maintain = _log_cf_option.max_write_buffer_number;
    _log_cf_option.write_buffer_size = FLAGS_write_buffer_size;
    _log_cf_option.min_write_buffer_number_to_merge = FLAGS_min_write_buffer_number_to_merge;

    if (FLAGS_rocks_use_partitioned_index_filters) {
        table_options.pin_l0_filter_and_index_blocks_in_cache= false;
        _binlog_cf_option.ttl = FLAGS_rocks_binlog_ttl_days * 24 * 60 * 60;
    }
    _binlog_cf_option.prefix_extractor.reset(
            rocksdb::NewFixedPrefixTransform(sizeof(int64_t)));
    _binlog_cf_option.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    _binlog_cf_option.compression = rocksdb::kLZ4Compression;
    _binlog_cf_option.compression_opts.enabled = true;
    _binlog_cf_option.compaction_style = rocksdb::kCompactionStyleFIFO;
    _binlog_cf_option.max_write_buffer_number_to_maintain = _binlog_cf_option.max_write_buffer_number;
    rocksdb::CompactionOptionsFIFO fifo_option;
    //如果观察到TTL无法让文件总数量少于配置的大小，RocksDB会暂时下降到基于大小的FIFO删除
    //https://rocksdb.org.cn/doc/FIFO-compaction-style.html
    fifo_option.max_table_files_size = FLAGS_rocks_binlog_max_files_size_gb * 1024 * 1024 * 1024LL;
    fifo_option.allow_compaction = FLAGS_rocksdb_fifo_allow_compaction;
    _binlog_cf_option.ttl = 0;
    _binlog_cf_option.periodic_compaction_seconds = 0;
    _binlog_cf_option.compaction_options_fifo = fifo_option;
    _binlog_cf_option.write_buffer_size = FLAGS_write_buffer_size;
    _binlog_cf_option.min_write_buffer_number_to_merge = FLAGS_min_write_buffer_number_to_merge;
    _binlog_cf_option.level0_file_num_compaction_trigger = FLAGS_level0_file_num_compaction_trigger;
    //todo
    // prefix length: regionid(8 Bytes) tableid(8 Bytes)
    _data_cf_option.prefix_extractor.reset(
            rocksdb::NewFixedPrefixTransform(sizeof(int64_t) * 2));
    _data_cf_option.memtable_prefix_bloom_size_ratio = 0.1;
    _data_cf_option.memtable_whole_key_filtering = true;
    _data_cf_option.OptimizeLevelStyleCompaction();
    _data_cf_option.compaction_pri = static_cast<rocksdb::CompactionPri>(FLAGS_rocks_data_compaction_pri);
    _data_cf_option.compaction_filter = SplitCompactionFilter::get_instance();
    _data_cf_option.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    _data_cf_option.compaction_style = rocksdb::kCompactionStyleLevel;
    _data_cf_option.optimize_filters_for_hits = FLAGS_rocks_optimize_filters_for_hits;
    _data_cf_option.level0_file_num_compaction_trigger = FLAGS_level0_file_num_compaction_trigger;
    _data_cf_option.level0_slowdown_writes_trigger = FLAGS_slowdown_write_sst_cnt;
    _data_cf_option.level0_stop_writes_trigger = FLAGS_stop_write_sst_cnt;
    _data_cf_option.hard_pending_compaction_bytes_limit = FLAGS_rocks_hard_pending_compaction_g * 1073741824ull;
    _data_cf_option.soft_pending_compaction_bytes_limit = FLAGS_rocks_soft_pending_compaction_g * 1073741824ull;
    _data_cf_option.target_file_size_base = FLAGS_target_file_size_base;
    _data_cf_option.max_bytes_for_level_multiplier = FLAGS_rocks_level_multiplier;
    _data_cf_option.level_compaction_dynamic_level_bytes = FLAGS_rocks_data_dynamic_level_bytes;

    _data_cf_option.max_write_buffer_number = FLAGS_max_write_buffer_number;
    _data_cf_option.max_write_buffer_number_to_maintain = _data_cf_option.max_write_buffer_number;
    _data_cf_option.write_buffer_size = FLAGS_write_buffer_size;
    _data_cf_option.min_write_buffer_number_to_merge = FLAGS_min_write_buffer_number_to_merge;

    _data_cf_option.max_bytes_for_level_base = FLAGS_max_bytes_for_level_base;
    if (FLAGS_l0_compaction_use_lz4) {
        _data_cf_option.compression_per_level = {rocksdb::CompressionType::kNoCompression,
                                                 rocksdb::CompressionType::kLZ4Compression,
                                                 rocksdb::CompressionType::kLZ4Compression,
                                                 rocksdb::CompressionType::kLZ4Compression,
                                                 rocksdb::CompressionType::kLZ4Compression,
                                                 rocksdb::CompressionType::kLZ4Compression,
                                                 rocksdb::CompressionType::kLZ4Compression};
    }

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
    _meta_info_option.max_write_buffer_number_to_maintain = _meta_info_option.max_write_buffer_number;

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
            } else if (column_family_name == "bin_log") {
                column_family_desc.push_back(rocksdb::ColumnFamilyDescriptor("bin_log", _binlog_cf_option));
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
                if (handle->GetName() == "bin_log") {
                    _old_binlog_cf = handle;
                } else {
                    _column_families[handle->GetName()] = handle;
                }
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
    if (_old_binlog_cf != nullptr) {
        // 一个小时后删除old bin_log cf
        Bthread bth;
        bth.run([this]() {
            bthread_usleep(3600 * 1000 * 1000LL);
            // 暂时假删，避免出问题
            DB_WARNING("erase bin_log cf");
            if (FLAGS_real_delete_old_binlog_cf) {
                auto handle = _old_binlog_cf;
                _old_binlog_cf = nullptr;
                // 避免删除时有并发，sleep 5分钟后真正删除
                bthread_usleep(300 * 1000 * 1000LL);
                auto res = _txn_db->DropColumnFamily(handle);
                if (!res.ok()) {
                    DB_FATAL("drop old binlog column_family failed, err_message:%s", res.ToString().c_str());
                } else {
                    res = _txn_db->DestroyColumnFamilyHandle(handle);
                    if (!res.ok()) {
                        DB_FATAL("destroy old binlog column_family failed, err_message:%s",
                                res.ToString().c_str());
                    }
                }
            } else {
                _old_binlog_cf = nullptr;
            }
        });
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
    update_oldest_ts_in_binlog_cf();
    collect_rocks_options();
    DB_WARNING("rocksdb init success");
    return 0;
}

void RocksWrapper::collect_rocks_options() {
    // gflag -> option_name, 可以通过setOption动态改的参数
    _rocks_options["level0_file_num_compaction_trigger"] = "level0_file_num_compaction_trigger";
    _rocks_options["slowdown_write_sst_cnt"] = "level0_slowdown_writes_trigger";
    _rocks_options["stop_write_sst_cnt"] = "level0_stop_writes_trigger";
    _rocks_options["rocks_hard_pending_compaction_g"] = "hard_pending_compaction_bytes_limit"; // * 1073741824ull;
    _rocks_options["rocks_soft_pending_compaction_g"] = "soft_pending_compaction_bytes_limit"; // * 1073741824ull;
    _rocks_options["target_file_size_base"] = "target_file_size_base";
    _rocks_options["rocks_level_multiplier"] = "max_bytes_for_level_multiplier";
    _rocks_options["max_write_buffer_number"] = "max_write_buffer_number";
    _rocks_options["write_buffer_size"] = "write_buffer_size";
    _rocks_options["max_bytes_for_level_base"] = "max_bytes_for_level_base";
    _rocks_options["rocks_max_background_compactions"] = "max_background_compactions";
    _rocks_options["rocks_max_subcompactions"] = "max_subcompactions";
    _rocks_options["max_background_jobs"] = "max_background_jobs";
}

rocksdb::Status RocksWrapper::remove_range(const rocksdb::WriteOptions& options,
        rocksdb::ColumnFamilyHandle* column_family,
        const rocksdb::Slice& begin,
        const rocksdb::Slice& end,
        bool delete_files_in_range) {
    auto raft_cf = get_raft_log_handle();
    auto data_cf = get_data_handle();
    auto mata_cf = get_meta_info_handle();
    if (raft_cf != nullptr && column_family->GetID() == raft_cf->GetID()) {
        _raft_cf_remove_range_count << 1;
        raft_cf_remove_range_count++;
    } else if (data_cf != nullptr && column_family->GetID() == data_cf->GetID()) {
        _data_cf_remove_range_count << 1;
        data_cf_remove_range_count++;
    } else if (mata_cf != nullptr && column_family->GetID() == mata_cf->GetID()) {
        _mata_cf_remove_range_count << 1;
        mata_cf_remove_range_count++;
    }
    if (delete_files_in_range && FLAGS_delete_files_in_range) {
        auto s = rocksdb::DeleteFilesInRange(_txn_db, column_family, &begin, &end, false);
        if (!s.ok()) {
            return s;
        }
    }
    rocksdb::TransactionDBWriteOptimizations opt;
    opt.skip_concurrency_control = true;
    opt.skip_duplicate_key_check = true;
    rocksdb::WriteBatch batch;
    batch.DeleteRange(column_family, begin, end);
    return _txn_db->Write(options, opt, &batch);
}

int32_t RocksWrapper::get_binlog_value(int64_t ts, std::string& binlog_value) {
    std::string key;
    uint64_t ts_endian = KeyEncoder::to_endian_u64(
                            KeyEncoder::encode_i64(ts));
    key.append((char*)&ts_endian, sizeof(uint64_t));

    auto binlog_handle = get_bin_log_handle();
    if (binlog_handle == nullptr) {
        DB_WARNING("no binlog handle: ts: %ld", ts);
        return -1;
    }

    rocksdb::ReadOptions option;
    auto status = _txn_db->Get(option, binlog_handle, rocksdb::Slice(key), &binlog_value);
    if (status.IsNotFound()) {
        // 没有找到则从老的binlog cf中找
        auto handle = _old_binlog_cf;
        if (handle == nullptr) {
            DB_FATAL("rocksdb has no old bin log cf, ts: %ld", ts);
            return -1;
        }
        std::string key_in_old_cf;
        key_in_old_cf.append((char*)&ts, sizeof(uint64_t));
        status = _txn_db->Get(option, handle, rocksdb::Slice(key_in_old_cf), &binlog_value);
        if (!status.ok()) {
            DB_FATAL("get binlog fail in old cf, err_msg: %s, ts: %ld", status.ToString().c_str(), ts);
            return -1;
        }
        DB_WARNING("binlog get in old cf, ts: %ld", ts);
    } else if (!status.ok()) {
        DB_WARNING("get binlog fail, err_msg: %s, ts: %ld", status.ToString().c_str(), ts);
        return -1;
    }

    return 0;
}

void RocksWrapper::update_oldest_ts_in_binlog_cf() {
    std::string start_key;
    uint64_t endian_ts = KeyEncoder::to_endian_u64(
                            KeyEncoder::encode_i64(0));
    start_key.append((char*)&endian_ts, sizeof(uint64_t));

    rocksdb::ReadOptions option;
    const uint64_t endian_max = UINT64_MAX;
    std::string end_key;
    end_key.append((char*)&endian_max, sizeof(uint64_t));
    rocksdb::Slice upper_bound_slice = end_key;
    option.iterate_upper_bound = &upper_bound_slice;
    option.total_order_seek = true;
    option.fill_cache = false;
    std::unique_ptr<rocksdb::Iterator> iter(new_iterator(option, get_bin_log_handle()));
    iter->Seek(start_key);
    bool find = false;
    for (; iter->Valid(); iter->Next()) {
        int64_t tmp_ts = TableKey(iter->key()).extract_i64(0);
        find = true;
        DB_WARNING("oldest_ts_in_binlog_cf changed: [%ld => %ld] [%s => %s]", _oldest_ts_in_binlog_cf, tmp_ts,
            ts_to_datetime_str(_oldest_ts_in_binlog_cf).c_str(), ts_to_datetime_str(tmp_ts).c_str());
        _oldest_ts_in_binlog_cf = tmp_ts;
        break;
    }

    if (!find) {
        DB_WARNING("has no data in binlog cf");
    }
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
void RocksWrapper::begin_split_adjust_option() {
    if (++_split_num > 1) {
        return;
    }
    Bthread bth;
    bth.run([this]() {
        if (_txn_db == nullptr) {
            return;
        }
        BAIDU_SCOPED_LOCK(_options_mutex);
        uint64_t value;
        std::unordered_map<std::string, std::string> new_options;
        value = _log_cf_option.max_write_buffer_number * 2;
        new_options["max_write_buffer_number"] = std::to_string(value);
        rocksdb::Status s = _txn_db->SetOptions(get_raft_log_handle(), new_options);
        if (!s.ok()) {
            DB_WARNING("begin_split_adjust_option raft_log_cf FAIL: %s", s.ToString().c_str());
        }

        value = _data_cf_option.soft_pending_compaction_bytes_limit * 2;
        new_options["soft_pending_compaction_bytes_limit"] = std::to_string(value);
        value = _data_cf_option.level0_slowdown_writes_trigger * 2;
        new_options["level0_slowdown_writes_trigger"] = std::to_string(value);
        value = _data_cf_option.max_write_buffer_number * 2;
        new_options["max_write_buffer_number"] = std::to_string(value);
        s = _txn_db->SetOptions(get_data_handle(), new_options);
        if (!s.ok()) {
            DB_WARNING("begin_split_adjust_option data_cf FAIL: %s", s.ToString().c_str());
        }
    });
}
void RocksWrapper::stop_split_adjust_option() {
    if (--_split_num > 0) {
        return;
    }
    Bthread bth;
    bth.run([this](){
        if (_txn_db == nullptr) {
            return;
        }
        BAIDU_SCOPED_LOCK(_options_mutex);
        uint64_t value;
        std::unordered_map<std::string, std::string> new_options;
        value = _log_cf_option.max_write_buffer_number;
        new_options["max_write_buffer_number"] = std::to_string(value);
        rocksdb::Status s = _txn_db->SetOptions(get_raft_log_handle(), new_options);
        if(!s.ok()) {
            DB_WARNING("stop_split_adjust_option raft_log_cf FAIL: %s", s.ToString().c_str());
        }

        value = _data_cf_option.soft_pending_compaction_bytes_limit;
        new_options["soft_pending_compaction_bytes_limit"] = std::to_string(value);
        value = _data_cf_option.level0_slowdown_writes_trigger;
        new_options["level0_slowdown_writes_trigger"] = std::to_string(value);
        value = _data_cf_option.max_write_buffer_number;
        new_options["max_write_buffer_number"] = std::to_string(value);
        s = _txn_db->SetOptions(get_data_handle(), new_options);
        if(!s.ok()) {
            DB_WARNING("stop_split_adjust_option data_cf FAIL: %s", s.ToString().c_str());
        }
    });
}
void RocksWrapper::adjust_option(std::map<std::string, std::string> new_options) {
    bool options_changed = false;
    std::unordered_map<std::string, std::string> cf_new_options;
    std::unordered_map<std::string, std::string> db_new_options;
    for (auto& pair : new_options) {
        auto& flag = pair.first;
        if (_rocks_options.find(flag) == _rocks_options.end()) {
            // 不是rocksdb的gflag参数
            continue;
        }
        auto& option_name = _rocks_options[flag];
        if (_defined_options.find(flag) == _defined_options.end()
                || _defined_options[flag] != pair.second) {
            options_changed = true;
            // 需要是SetDBOptions
            if (flag == "rocks_max_background_compactions"
                    || flag == "rocks_max_subcompactions"
                    || flag == "max_background_jobs") {
                db_new_options[option_name] = pair.second;
                continue;
            }
            // 需要是SetOptions
            if (flag == "rocks_hard_pending_compaction_g"
                    || flag == "rocks_soft_pending_compaction_g" ) {
                int64_t value = strtoull(pair.second.c_str(), nullptr, 10);
                cf_new_options[option_name] = std::to_string(value * 1073741824ull);
            } else {
                cf_new_options[option_name] = pair.second;
            }
            if (flag == "max_write_buffer_number") {
                cf_new_options["max_write_buffer_number_to_maintain"] = pair.second;
            }
        }
    }
    if (!options_changed) {
        return;
    }
    _defined_options = new_options;
    Bthread bth;
    bth.run([this, cf_new_options, db_new_options]() {
        if (_txn_db == nullptr) {
            return;
        }
        BAIDU_SCOPED_LOCK(_options_mutex);
        if (!db_new_options.empty()) {
            rocksdb::Status s = _txn_db->SetDBOptions(db_new_options);
            if (!s.ok()) {
                DB_WARNING("adjust_option data_cf FAIL: %s", s.ToString().c_str());
            }
        }
        if (!cf_new_options.empty()) {
            for (auto& kv : cf_new_options) {
                // 是否和split设置的有冲突
                if (kv.first == "soft_pending_compaction_bytes_limit") {
                    _data_cf_option.soft_pending_compaction_bytes_limit = strtoull(kv.second.c_str(), nullptr, 10);
                } else if (kv.first == "level0_slowdown_writes_trigger") {
                    _data_cf_option.level0_slowdown_writes_trigger = strtod(kv.second.c_str(), nullptr);
                } else if (kv.first == "max_write_buffer_number") {
                    _data_cf_option.max_write_buffer_number = strtod(kv.second.c_str(), nullptr);
                }
            }
            rocksdb::Status s = _txn_db->SetOptions(get_data_handle(), cf_new_options);
            if (!s.ok()) {
                DB_WARNING("adjust_option data_cf FAIL: %s", s.ToString().c_str());
            }
        }
    });
}
}
