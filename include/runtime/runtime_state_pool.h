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
 
#include <unordered_map>
#include "common.h"
#include "runtime_state.h"

namespace baikaldb {
//class Region;

// TODO: remove locking for thread-safe codes
class RuntimeStatePool {
public:
    virtual ~RuntimeStatePool() {}

    RuntimeStatePool() {}

    void remove(uint64_t db_conn_id) {
        std::unique_lock<std::mutex> lock(_map_mutex);
        _state_map.erase(db_conn_id);
    }

    SmartState get(uint64_t db_conn_id) {
        std::unique_lock<std::mutex> lock(_map_mutex);
        if (_state_map.count(db_conn_id) == 0) {
            return nullptr;
        }
        return _state_map[db_conn_id];
    }
    void set(uint64_t db_conn_id, SmartState state) {
        std::unique_lock<std::mutex> lock(_map_mutex);
        _state_map[db_conn_id] = state;
        state->set_pool(this);
    }
    std::unordered_map<uint64_t, SmartState> get_state_map() {
        std::unique_lock<std::mutex> lock(_map_mutex);
        return _state_map;
    }

private:
    std::unordered_map<uint64_t, SmartState>  _state_map;
    std::mutex _map_mutex;
};
}
