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
class SplitCompactionFilter : public rocksdb::CompactionFilter {
public:
    static SplitCompactionFilter* get_instance() {
        static SplitCompactionFilter _instance;
        return &_instance;
    }
    ~SplitCompactionFilter() {
        bthread_mutex_destroy(&_mutex);
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
        //std::string start_key;
        std::string end_key;
        {
            BAIDU_SCOPED_LOCK(_mutex);
            //start_key = _range_key_map[region_id].first;
            end_key = _range_key_map[region_id].second;
        }
        if (/*start_key.empty() &&*/end_key.empty()) {
            return false;
        }
        int64_t index_id = table_key.extract_i64(sizeof(int64_t));
        // cstore, primary column key format: index_id = table_id(32byte) + field_id(32byte)
        if ((index_id & SIGN_MASK_32) != 0) {
            index_id = index_id >> 32;
        }
        auto index_info = _factory->get_index_info_ptr(index_id);
        if (index_info == nullptr) {
            return false;
        }
        auto pk_info = _factory->get_index_info_ptr(index_info->pk);
        if (pk_info == nullptr) {
            return false;
        }

        //int ret1 = 0;
        int ret2 = 0;
        if (index_info->type == pb::I_PRIMARY || index_info->is_global) {
            ret2 = end_key.empty()? 1 : end_key.compare(0, std::string::npos, 
                    key.data() + prefix_len, key.size() - prefix_len);
           // DB_WARNING("split compaction filter, region_id: %ld, index_id: %ld, end_key: %s, key: %s, ret: %d",
           //     region_id, index_id, rocksdb::Slice(end_key).ToString(true).c_str(), 
           //     key.ToString(true).c_str(), ret2);
            return (ret2 <= 0);
        } else if (index_info->type == pb::I_UNIQ || index_info->type == pb::I_KEY) {
            rocksdb::Slice key_slice(key);
            key_slice.remove_prefix(sizeof(int64_t) * 2);
            return !Transaction::fits_region_range(key_slice, value, 
                nullptr, &end_key, *pk_info, *index_info);
        }
        return false;
    }

    void set_range_key(int64_t region_id, const std::string& start_key, const std::string& end_key) {
        BAIDU_SCOPED_LOCK(_mutex);
        _range_key_map[region_id].first = start_key;
        _range_key_map[region_id].second = end_key;
    }

private:
    SplitCompactionFilter() {
        bthread_mutex_init(&_mutex, NULL);
        _factory = SchemaFactory::get_instance();
    }

    // region_id => end_key
    mutable std::unordered_map<int64_t, std::pair<std::string, std::string> > _range_key_map;
    mutable bthread_mutex_t _mutex;
    SchemaFactory* _factory;
};
}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
