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

#include "my_rocksdb.h"
#include "qos.h"

namespace baikaldb {
DEFINE_int32(rocksdb_cost_sample, 100, "rocksdb_cost_sample");

namespace myrocksdb {

void Iterator::Seek(const rocksdb::Slice& target) {
    QosBthreadLocal* local = StoreQos::get_instance()->get_bthread_local();

    // 限流
    if (local != nullptr) {
        local->get_rate_limiting();
    }
    static thread_local int64_t total_time = 0;
    static thread_local int64_t count = 0;

    TimeCost cost;
    // 执行
    _iter->Seek(target);
    total_time += cost.get_time();
    if (++count >= FLAGS_rocksdb_cost_sample) {
        RocksdbVars::get_instance()->rocksdb_seek_time << total_time / count;
        RocksdbVars::get_instance()->rocksdb_seek_count << count;
        total_time = 0;
        count = 0;
    }
}

void Iterator::SeekForPrev(const rocksdb::Slice& target) {
    QosBthreadLocal* local = StoreQos::get_instance()->get_bthread_local();

    // 限流
    if (local != nullptr) {
        local->get_rate_limiting();
    }

    static thread_local int64_t total_time = 0;
    static thread_local int64_t count = 0;
    TimeCost cost;
    // 执行
    _iter->SeekForPrev(target);
    total_time += cost.get_time();
    if (++count >= FLAGS_rocksdb_cost_sample) {
        RocksdbVars::get_instance()->rocksdb_seek_time << total_time / count;
        RocksdbVars::get_instance()->rocksdb_seek_count << count;
        total_time = 0;
        count = 0;
    }
}

void Iterator::Next() { 
    QosBthreadLocal* local = StoreQos::get_instance()->get_bthread_local();

    // 限流
    if (local != nullptr) {
        local->scan_rate_limiting();
    }
    static thread_local int64_t total_time = 0;
    static thread_local int64_t count = 0;

    TimeCost cost;
    // 执行
    _iter->Next();
    total_time += cost.get_time();
    if (++count >= FLAGS_rocksdb_cost_sample) {
        RocksdbVars::get_instance()->rocksdb_scan_time << (total_time / count);
        RocksdbVars::get_instance()->rocksdb_scan_count << count;
        total_time = 0;
        count = 0;
    }

    return; 
}

void Iterator::Prev() { 
    QosBthreadLocal* local = StoreQos::get_instance()->get_bthread_local();

    // 限流
    if (local != nullptr) {
        local->scan_rate_limiting();
    }
    static thread_local int64_t total_time = 0;
    static thread_local int64_t count = 0;

    // 执行
    TimeCost cost;
    _iter->Prev();
    total_time += cost.get_time();
    if (++count >= FLAGS_rocksdb_cost_sample) {
        RocksdbVars::get_instance()->rocksdb_scan_time << total_time / count;
        RocksdbVars::get_instance()->rocksdb_scan_count << count;
        total_time = 0;
        count = 0;
    }

    return; 
}

rocksdb::Status Transaction::Get(const rocksdb::ReadOptions& options,
                    rocksdb::ColumnFamilyHandle* column_family, const rocksdb::Slice& key,
                    std::string* value) {
    QosBthreadLocal* local = StoreQos::get_instance()->get_bthread_local();

    // 限流
    if (local != nullptr) {
        local->get_rate_limiting();
    }

    static thread_local int64_t total_time = 0;
    static thread_local int64_t count = 0;
    // 执行
    TimeCost cost;
    auto s = _txn->Get(options, column_family, key, value);
    total_time += cost.get_time();
    if (++count >= FLAGS_rocksdb_cost_sample) {
        RocksdbVars::get_instance()->rocksdb_get_time << total_time / count;
        RocksdbVars::get_instance()->rocksdb_get_count << count;
        total_time = 0;
        count = 0;
    }

    return s;
}

rocksdb::Status Transaction::Get(const rocksdb::ReadOptions& options,
                    rocksdb::ColumnFamilyHandle* column_family, const rocksdb::Slice& key,
                    rocksdb::PinnableSlice* pinnable_val) {
    QosBthreadLocal* local = StoreQos::get_instance()->get_bthread_local();

    // 限流
    if (local != nullptr) {
        local->get_rate_limiting();
    }

    static thread_local int64_t total_time = 0;
    static thread_local int64_t count = 0;
    // 执行
    TimeCost cost;
    auto s = _txn->Get(options, column_family, key, pinnable_val);
    total_time += cost.get_time();
    if (++count >= FLAGS_rocksdb_cost_sample) {
        RocksdbVars::get_instance()->rocksdb_get_time << total_time / count;
        RocksdbVars::get_instance()->rocksdb_get_count << count;
        total_time = 0;
        count = 0;
    }

    return s;
}

void Transaction::MultiGet(const rocksdb::ReadOptions& options,
                     rocksdb::ColumnFamilyHandle* column_family,
                     const std::vector<rocksdb::Slice>& keys,
                     std::vector<rocksdb::PinnableSlice>& values,
                     std::vector<rocksdb::Status>& statuses,
                     bool sorted_input) {
    QosBthreadLocal* local = StoreQos::get_instance()->get_bthread_local();

    // 限流
    if (local != nullptr) {
        local->get_rate_limiting(keys.size());
    }

    static thread_local int64_t total_time = 0;
    static thread_local int64_t key_count = 0;
    // 执行
    TimeCost cost;
    _txn->MultiGet(options, column_family, keys.size(), keys.data(), values.data(), statuses.data(), sorted_input);
    total_time += cost.get_time();
    key_count += keys.size();
    if (key_count >= FLAGS_rocksdb_cost_sample) {
        RocksdbVars::get_instance()->rocksdb_multiget_time << total_time / key_count;
        RocksdbVars::get_instance()->rocksdb_multiget_count << key_count;
        total_time = 0;
        key_count = 0;
    }
}

rocksdb::Status Transaction::GetForUpdate(const rocksdb::ReadOptions& options,
                            rocksdb::ColumnFamilyHandle* column_family,
                            const rocksdb::Slice& key, std::string* value) {
    QosBthreadLocal* local = StoreQos::get_instance()->get_bthread_local();

    // 限流
    if (local != nullptr) {
        local->get_rate_limiting();
    }

    static thread_local int64_t total_time = 0;
    static thread_local int64_t count = 0;
    // 执行
    TimeCost cost;
    auto s = _txn->GetForUpdate(options, column_family, key, value);
    total_time += cost.get_time();
    if (++count >= FLAGS_rocksdb_cost_sample) {
        RocksdbVars::get_instance()->rocksdb_get_time << total_time / count;
        RocksdbVars::get_instance()->rocksdb_get_count << count;
        total_time = 0;
        count = 0;
    }

    return s;
}

rocksdb::Status Transaction::GetForUpdate(const rocksdb::ReadOptions& options,
                            rocksdb::ColumnFamilyHandle* column_family,
                            const rocksdb::Slice& key, rocksdb::PinnableSlice* pinnable_val) {
    QosBthreadLocal* local = StoreQos::get_instance()->get_bthread_local();

    // 限流
    if (local != nullptr) {
        local->get_rate_limiting();
    }

    static thread_local int64_t total_time = 0;
    static thread_local int64_t count = 0;
    // 执行
    TimeCost cost;
    auto s = _txn->GetForUpdate(options, column_family, key, pinnable_val);
    total_time += cost.get_time();
    if (++count >= FLAGS_rocksdb_cost_sample) {
        RocksdbVars::get_instance()->rocksdb_get_time << total_time / count;
        RocksdbVars::get_instance()->rocksdb_get_count << count;
        total_time = 0;
        count = 0;
    }

    return s;    
}

rocksdb::Status Transaction::Put(rocksdb::ColumnFamilyHandle* column_family, const rocksdb::Slice& key,
                    const rocksdb::Slice& value) {
    static thread_local int64_t total_time = 0;
    static thread_local int64_t count = 0;
    TimeCost cost;
    auto s = _txn->Put(column_family, key, value);
    total_time += cost.get_time();
    if (++count >= FLAGS_rocksdb_cost_sample) {
        RocksdbVars::get_instance()->rocksdb_put_time << total_time / count;
        RocksdbVars::get_instance()->rocksdb_put_count << count;
        total_time = 0;
        count = 0;
    }

    return s;
}

rocksdb::Status Transaction::Put(rocksdb::ColumnFamilyHandle* column_family, const rocksdb::SliceParts& key,
                    const rocksdb::SliceParts& value) {
    static thread_local int64_t total_time = 0;
    static thread_local int64_t count = 0;
    TimeCost cost;
    auto s = _txn->Put(column_family, key, value);
    total_time += cost.get_time();
    if (++count >= FLAGS_rocksdb_cost_sample) {
        RocksdbVars::get_instance()->rocksdb_put_time << total_time / count;
        RocksdbVars::get_instance()->rocksdb_put_count << count;
        total_time = 0;
        count = 0;
    }
    
    return s;
}
 
} // namespace myrocksdb
} // namespace baikaldb
