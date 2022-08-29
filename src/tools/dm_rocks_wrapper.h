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

#ifndef FCDATA_BAIKALDB_ROCKSWRAPPERDM_H
#define FCDATA_BAIKALDB_ROCKSWRAPPERDM_H
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
#include "common.h"

namespace baikaldb {
class DMRocksWrapper {
public:
    virtual ~DMRocksWrapper() {}
    static DMRocksWrapper* get_instance() {
        static DMRocksWrapper _instance;
        return &_instance;
    }

    int32_t init(const std::string& path);

    rocksdb::Status write(rocksdb::WriteBatch* updates) {
        return _db->Write(_write_ops, updates);
    }

    rocksdb::Status get(const rocksdb::Slice& key,
                        std::string* value) {
        return _db->Get(_read_ops, key, value);
    }

    rocksdb::Status compact_all() {
        return _db->CompactRange(_compact_ops, nullptr, nullptr);
    }

    rocksdb::Status flush(const rocksdb::FlushOptions& options) {
        return _db->Flush(options);
    }

    rocksdb::Iterator* new_iterator(const rocksdb::ReadOptions& options) {
        return _db->NewIterator(options);
    }

    void close() {
        delete _db;
    }

    rocksdb::Cache* get_cache() {
        return _cache;
    }

    bool db_statistics();

    rocksdb::Status clean_roscksdb();

    rocksdb::Options get_option() {
        return _db->GetOptions();
    }

    rocksdb::ColumnFamilyHandle* get_handle() {
        return _db->DefaultColumnFamily();
    }
private:

    DMRocksWrapper():               _is_init(false), _db(nullptr) {};

    std::string                     _db_path;
    bool                            _is_init;
    rocksdb::DB*                    _db;
    rocksdb::Cache*                 _cache;
    rocksdb::WriteOptions           _write_ops;
    rocksdb::ReadOptions            _read_ops;
    rocksdb::CompactRangeOptions    _compact_ops;

    int64_t                         _used_size;
};


};
#endif //FCDATA_BAIKALDB_ROCKSWRAPPERDM_H
