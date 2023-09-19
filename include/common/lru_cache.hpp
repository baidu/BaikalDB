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

#include "lru_cache.h"
#include <dirent.h>
#include <sys/stat.h>
#include <stdio.h>

namespace baikaldb  {

template <typename ItemKey, typename ItemType, typename HashType>
int Cache<ItemKey, ItemType, HashType>::init(int64_t len_threshold) {
    _len_threshold = len_threshold;
    return 0;
}

template <typename ItemKey, typename ItemType, typename HashType>
std::string Cache<ItemKey, ItemType, HashType>::get_info() {
    char buf[100];
    snprintf(buf, sizeof(buf), "hit:%ld, total:%ld,", _hit_count, _total_count);
    return buf;
}

template <typename ItemKey, typename ItemType, typename HashType>
int Cache<ItemKey, ItemType, HashType>::check(const ItemKey& key) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (_lru_map.count(key) == 1) {
        return 0;
    }
    return -1;
}

template <typename ItemKey, typename ItemType, typename HashType>
int Cache<ItemKey, ItemType, HashType>::find(const ItemKey& key, ItemType* value) {
    std::lock_guard<std::mutex> lock(_mutex);
    ++_total_count;
    if (_lru_map.count(key) == 1) {
        ++_hit_count;
        LruNode<ItemKey, ItemType>* node = _lru_map[key];
        *value = node->value;
        node->RemoveFromList();
        _lru_list.Append(node);
        return 0;
    }
    return -1;
}

template <typename ItemKey, typename ItemType, typename HashType>
int Cache<ItemKey, ItemType, HashType>::add(const ItemKey& key, const ItemType& value) {
    std::lock_guard<std::mutex> lock(_mutex);
    LruNode<ItemKey, ItemType>* node = NULL;
    if (_lru_map.count(key) == 1) {
        node = _lru_map[key];
        node->RemoveFromList();
    } else {
        node = new LruNode<ItemKey, ItemType>();
        _lru_map[key] = node;
    }
    while ((int64_t)_lru_map.size() >= _len_threshold && !_lru_list.empty()) {
        LruNode<ItemKey, ItemType>* head = (LruNode<ItemKey, ItemType>*)_lru_list.head();
        head->RemoveFromList();
        _lru_map.erase(head->key);
        delete head;
    }
    node->value = value;
    node->key = key;
    _lru_list.Append(node);
    return 0;
}

template <typename ItemKey, typename ItemType, typename HashType>
int Cache<ItemKey, ItemType, HashType>::del(const ItemKey& key) {
    std::lock_guard<std::mutex> lock(_mutex);
    LruNode<ItemKey, ItemType>* node = NULL;
    if (_lru_map.count(key) == 1) {
        node = _lru_map[key];
        node->RemoveFromList();
        _lru_map.erase(node->key);
        delete node;
    }
    return 0;
}

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
