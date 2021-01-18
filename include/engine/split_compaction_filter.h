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

#include <rocksdb/compaction_filter.h>
#include <bthread/mutex.h>
#include "key_encoder.h"
#include "type_utils.h"
#include "schema_factory.h"
#include "transaction.h"

namespace baikaldb {
DECLARE_int32(rocks_binlog_ttl_days);
class SplitCompactionFilter : public rocksdb::CompactionFilter {
typedef butil::FlatMap<int64_t, std::string*> KeyMap;
typedef DoubleBuffer<KeyMap> DoubleBufKey;
typedef std::unordered_set<int64_t> BinlogSet;
typedef butil::DoublyBufferedData<BinlogSet> DoubleBufBinlog;
public:
    static SplitCompactionFilter* get_instance() {
        static SplitCompactionFilter _instance;
        return &_instance;
    }
    ~SplitCompactionFilter() {
    }
    const char* Name() const override {
        return "SplitCompactionFilter";
    }
    // The compaction process invokes this method for kv that is being compacted. 
    // A return value of false indicates that the kv should be preserved 
    // a return value of true indicates that this key-value should be removed from the
    // output of the compaction. 
    bool Filter(int /*level*/,
                const rocksdb::Slice& key,
                const rocksdb::Slice& value,
                std::string* /*new_value*/,
                bool* /*value_changed*/) const override {
        static int prefix_len = sizeof(int64_t) * 2;
        if ((int)key.size() < prefix_len) {
            return false;
        }
        TableKey table_key(key);
        int64_t region_id = table_key.extract_i64(0);
        std::string* end_key = get_end_key(region_id);
        if (end_key == nullptr || end_key->empty()) {
            return false;
        }
        if (get_binlog_region(region_id) == 0) {
            static int64_t ttl_ms = FLAGS_rocks_binlog_ttl_days * 24 * 60 * 60 * 1000;
            rocksdb::Slice pure_key(key);
            pure_key.remove_prefix(2 * sizeof(int64_t));
            int64_t commit_tso = ttl_decode(pure_key);
            int64_t expire_ts_ms = (commit_tso >> tso::logical_bits) + ttl_ms;
            int64_t current_ts_ms = tso::clock_realtime_ms();
//            DB_WARNING("binglog compaction filter, region_id: %ld, ttl_tso: %ld, commit_tso: %ld, expire_tso: %ld, current_tso: %ld",
//                       region_id, ttl_ms, commit_tso, expire_ts_ms, current_ts_ms);
            if (current_ts_ms > expire_ts_ms) {
                return true;
            }
            return false;
        }
        int64_t index_id = table_key.extract_i64(sizeof(int64_t));
        // cstore, primary column key format: index_id = table_id(32byte) + field_id(32byte)
        if ((index_id & SIGN_MASK_32) != 0) {
            index_id = index_id >> 32;
        }
        auto index_info = _factory->get_split_index_info(index_id);
        if (index_info == nullptr) {
            return false;
        }

        //int ret1 = 0;
        int ret2 = 0;
        if (index_info->type == pb::I_PRIMARY || index_info->is_global) {
            ret2 = end_key->compare(0, std::string::npos, 
                    key.data() + prefix_len, key.size() - prefix_len);
           // DB_WARNING("split compaction filter, region_id: %ld, index_id: %ld, end_key: %s, key: %s, ret: %d",
           //     region_id, index_id, rocksdb::Slice(end_key).ToString(true).c_str(), 
           //     key.ToString(true).c_str(), ret2);
            return (ret2 <= 0);
        } else if (index_info->type == pb::I_UNIQ || index_info->type == pb::I_KEY) {
            auto pk_info = _factory->get_split_index_info(index_info->pk);
            if (pk_info == nullptr) {
                return false;
            }
            rocksdb::Slice key_slice(key);
            key_slice.remove_prefix(sizeof(int64_t) * 2);
            return !Transaction::fits_region_range(key_slice, value, 
                nullptr, end_key, *pk_info, *index_info);
        }
        return false;
    }

    void set_end_key(int64_t region_id, const std::string& end_key) {
        std::string* old_key = get_end_key(region_id);
        // 已存在不更新
        if (old_key != nullptr && *old_key == end_key) {
            return;
        }
        auto call = [region_id, end_key](KeyMap& key_map) {
            std::string* new_key = new std::string(end_key);
            key_map[region_id] = new_key;
        };
        _range_key_map.modify(call);
    }

    std::string* get_end_key(int64_t region_id) const {
        auto iter = _range_key_map.read()->seek(region_id);
        if (iter != nullptr) {
            return *iter;
        }
        return nullptr;
    }

    void set_binlog_region(int64_t region_id) {
        auto call = [this, region_id](BinlogSet& region_id_set) -> int {
            region_id_set.insert(region_id);
            return 1;
        };
        _binlog_region_id_set.Modify(call);
    }

    int get_binlog_region(int64_t region_id) const {
        DoubleBufBinlog::ScopedPtr ptr;
        if (_binlog_region_id_set.Read(&ptr) == 0) {
            auto iter = ptr->find(region_id);
            if (iter != ptr->end()) {
                return 0;
            }
        }
        return -1;
    }

private:
    SplitCompactionFilter() {
        _factory = SchemaFactory::get_instance();
        _range_key_map.read_background()->init(12301);
        _range_key_map.read()->init(12301);
    }

    // region_id => end_key
    mutable DoubleBufKey _range_key_map;
    mutable DoubleBufBinlog _binlog_region_id_set;
    SchemaFactory* _factory;
};
}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
