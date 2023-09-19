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
#include <sys/time.h>
#include <string>
#include <unordered_map>
#include <mutex>
#ifdef BAIDU_INTERNAL
#include <base/containers/linked_list.h>
#else
#include <butil/containers/linked_list.h>
#endif 

namespace baikaldb {
    
template <typename ItemKey, typename ItemType>
struct LruNode : public butil::LinkNode<LruNode<ItemKey, ItemType>> {
    ItemType value;
    ItemKey key;
};

template <typename ItemKey, typename ItemType, typename HashType = std::hash<ItemKey>>
class Cache {
public:
    Cache() : _total_count(0), _hit_count(0), 
        _len_threshold(10000){}
    ~Cache() {
        for (auto& kv : _lru_map) {
            delete kv.second;
        }
    }

    int init(int64_t len_threshold);
    std::string get_info();
    int check(const ItemKey& key);
    int find(const ItemKey& key, ItemType* value);
    int add(const ItemKey& key, const ItemType& value);
    int del(const ItemKey& key);
private:
    //双链表，从尾部插入数据，超过阈值数据从头部删除
    butil::LinkedList<LruNode<ItemKey, ItemType>> _lru_list;
    std::unordered_map<ItemKey, LruNode<ItemKey, ItemType>*, HashType> _lru_map;
    std::mutex _mutex;
    int64_t _total_count;
    int64_t _hit_count;
    int64_t _len_threshold;
};

}
#include "lru_cache.hpp"

/* vim: set ts=4 sw=4 sts=4 tw=100 */
