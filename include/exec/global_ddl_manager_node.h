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
#include "dml_manager_node.h"
#include "fetcher_store.h"

namespace baikaldb {

class GlobalDDLManagerNode : public DmlManagerNode {
public:
    GlobalDDLManagerNode();
    virtual ~GlobalDDLManagerNode();

    virtual int open(RuntimeState* state);
    void set_table_id(int64_t table_id) {
        _table_id = table_id;
    }
    void set_index_id(int64_t index_id) {
        _index_id = index_id;
    }

    void set_region_infos(std::map<int64_t, pb::RegionInfo> region_infos) {
        _region_infos.swap(region_infos);
    }

    void set_task_id(const std::string& task_id) {
        _task_id = task_id;
    }

private:
    FetcherStore    _fetcher_store;
    int64_t _table_id {0};
    int64_t _index_id {0};
    std::map<int64_t, pb::RegionInfo> _region_infos;
    std::string _task_id;
};
} // namespace  baikaldbame
