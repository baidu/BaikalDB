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

#include "meta_rocksdb.h"
#include "gflags/gflags.h"

namespace baikaldb {
DEFINE_string(db_path, "./rocks_db", "rocks db path");
int MetaRocksdb::init() {
    _rocksdb = RocksWrapper::get_instance();
    if (!_rocksdb) {
        DB_FATAL("create rocksdb handler failed");
        return -1;
    }
    int ret = _rocksdb->init(FLAGS_db_path);
    if (ret != 0) {
        DB_FATAL("rocksdb init failed: code:%d", ret);
        return -1;
    }
    _handle = _rocksdb->get_meta_info_handle();
    DB_WARNING("rocksdb init success, db_path:%s", FLAGS_db_path.c_str());
    return 0;
}

int MetaRocksdb::put_meta_info(const std::string& key, const std::string& value) {
    rocksdb::WriteOptions write_option;
    write_option.disableWAL = true;
    auto status = _rocksdb->put(write_option, _handle, rocksdb::Slice(key), rocksdb::Slice(value));
    if (!status.ok()) { 
        DB_WARNING("put rocksdb fail, err_msg: %s, key: %s, value: %s", 
                    status.ToString().c_str(), key.c_str(), value.c_str());
        return -1;
    }
    return 0;
}

int MetaRocksdb::put_meta_info(const std::vector<std::string>& keys,
                    const std::vector<std::string>& values) {
    if (keys.size() != values.size()) {
        DB_WARNING("input keys'size is not equal to values' size");
        return -1;
    }
    rocksdb::WriteOptions write_option;
    write_option.disableWAL = true;
    rocksdb::WriteBatch batch;
    for (size_t i = 0; i < keys.size(); ++i) {
        batch.Put(_handle, keys[i], values[i]);
    }
    auto status = _rocksdb->write(write_option, &batch);
    if (!status.ok()) {
        DB_WARNING("put batch to rocksdb fail, err_msg: %s",
                    status.ToString().c_str());
        return -1;
    }
    return 0;
}

int MetaRocksdb::get_meta_info(const std::string& key, std::string* value) {
    rocksdb::ReadOptions options;
    auto status = _rocksdb->get(options, _handle, rocksdb::Slice(key), value);
    if (!status.ok()) {
        return -1;
    }
    return 0;
}

int MetaRocksdb::delete_meta_info(const std::vector<std::string>& keys) {
    rocksdb::WriteOptions write_option;
    write_option.disableWAL = true;
    rocksdb::WriteBatch batch;
    for (auto& key : keys) {
        batch.Delete(_handle, key);
    }
    auto status = _rocksdb->write(write_option, &batch);
    if (!status.ok()) {
        DB_WARNING("delete batch to rocksdb fail, err_msg: %s", status.ToString().c_str());
        return -1;
    }
    return 0;
}
int MetaRocksdb::write_meta_info(const std::vector<std::string>& put_keys,
                    const std::vector<std::string>& put_values,
                    const std::vector<std::string>& delete_keys) {
    if (put_keys.size() != put_values.size()) {
        DB_WARNING("input keys'size is not equal to values' size");
        return -1;
    }
    rocksdb::WriteOptions write_option;
    write_option.disableWAL = true;
    rocksdb::WriteBatch batch;
    for (size_t i = 0; i < put_keys.size(); ++i) {
        batch.Put(_handle, put_keys[i], put_values[i]);
    }
    for (auto& delete_key : delete_keys) {
        batch.Delete(_handle, delete_key);
    }
    auto status = _rocksdb->write(write_option, &batch);
    if (!status.ok()) {
        DB_WARNING("write batch to rocksdb fail,  %s", status.ToString().c_str());
        return -1;
    }
    return 0;
}
}//namespace
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
