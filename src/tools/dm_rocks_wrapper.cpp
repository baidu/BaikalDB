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
#include "dm_rocks_wrapper.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include <iostream>
#include "common.h"
#include "mut_table_key.h"
#include "table_key.h"
#include "my_listener.h"
#include "raft_log_compaction_filter.h"
#include "split_compaction_filter.h"
#include <sys/vfs.h>

namespace baikaldb {
DEFINE_bool(rocks_skip_stats_update_on_db_open, false, "rocks_skip_stats_update_on_db_open");
DEFINE_int32(rocks_block_size, 64 * 1024, "rocksdb block_cache size, default: 64KB");
DEFINE_int64(rocks_block_cache_size_mb, 8 * 1024, "rocksdb block_cache_size_mb, default: 8G");
DEFINE_double(rocks_high_pri_pool_ratio, 0.5, "rocksdb cache high_pri_pool_ratio, default: 0.5");
DEFINE_int32(rocks_max_open_files, 1024, "rocksdb max_open_files, default: 1024");
DEFINE_int32(max_background_jobs, 24, "max_background_jobs");
DEFINE_uint64(rocks_compaction_readahead_size, 0, "rocksdb compaction_readahead_size, default: 0");
DEFINE_int32(rocks_max_subcompactions, 16, "rocks_max_subcompactions");
DEFINE_int64(vector_size,  1024 * 1024, "rocksdb vector memtable count, default: 0");
DEFINE_int64(compaction_threshold_g, 100, "rocksdb compaction threshold, default: 100GB");
DEFINE_int64(max_background_flushes, 16, "max_background_flushes, default: 16");
DEFINE_int64(max_background_compactions, 16, "max_background_compactions, default: 16");
int32_t DMRocksWrapper::init(const std::string &path) {
    if (_is_init) {
        return 0;
    }
    rocksdb::BlockBasedTableOptions table_options;
    table_options.data_block_index_type = rocksdb::BlockBasedTableOptions::kDataBlockBinaryAndHash;
    table_options.block_cache = rocksdb::NewLRUCache(FLAGS_rocks_block_cache_size_mb * 1024 * 1024LL, 8);
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
    db_options.env->SetBackgroundThreads(2, rocksdb::Env::HIGH);
    db_options.memtable_factory.reset(new rocksdb::VectorRepFactory(FLAGS_vector_size));
    db_options.compression = rocksdb::kLZ4Compression;
    // buckload模式下必须将allow_concurrent_memtable_write关掉
    db_options.allow_concurrent_memtable_write = false;
    db_options.PrepareForBulkLoad();
    db_options.max_background_flushes = FLAGS_max_background_flushes;
    db_options.max_background_compactions = FLAGS_max_background_compactions;
    db_options.max_subcompactions = FLAGS_rocks_max_subcompactions;

    _db_path = path;
    rocksdb::Status s = rocksdb::DB::Open(db_options, path, &_db);
    if (s.ok()) {
        DB_WARNING("open db:%s success", path.c_str());
    } else {
        DB_FATAL("open db:%s fail, err_message:%s", path.c_str(), s.ToString().c_str());
        return -1;
    }
    _write_ops.disableWAL = true;
    _is_init = true;
    DB_WARNING("rocksdb init success");
    return 0;
}


rocksdb::Status DMRocksWrapper::clean_roscksdb() {
    // 删除rocksdb
    DB_WARNING("clean_roscksdb");
    MutTableKey start;
    MutTableKey end;
    start.append_i64(0);
    end.append_i64(INT_FAST64_MAX);
    end.append_u64(UINT64_MAX);
    rocksdb::Slice start_key(start.data());
    rocksdb::Slice end_key(end.data());
    return rocksdb::DeleteFilesInRange(_db, _db->DefaultColumnFamily(), &start_key, &end_key);
}

bool DMRocksWrapper::db_statistics() {
    struct statfs sfs;
    statfs(_db_path.c_str(), &sfs);
    int64_t disk_capacity = (int64_t)sfs.f_blocks * sfs.f_bsize;
    int64_t left_size = (int64_t)sfs.f_bavail * sfs.f_bsize;
    _used_size = disk_capacity - left_size;
    if (_used_size > FLAGS_compaction_threshold_g * 1024 * 1024 * 1024LL) {
        return true;
    }
    return false;
};
}