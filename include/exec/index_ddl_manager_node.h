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
#include "lock_primary_node.h"
#include "lock_secondary_node.h"

namespace baikaldb {

class IndexDDLManagerNode : public DmlManagerNode {
public:
    IndexDDLManagerNode();
    virtual ~IndexDDLManagerNode();

    virtual int open(RuntimeState* state);
    void set_table_id(int64_t table_id) {
        _table_id = table_id;
    }
    void set_index_id(int64_t index_id) {
        _index_id = index_id;
    }

    void set_task_id(const std::string& task_id) {
        _task_id = task_id;
    }

    void set_is_global_index(bool flags) {
        _is_global_index = flags;
    }

    int create_lock_primary_node(int64_t table_id,
            std::unique_ptr<LockPrimaryNode>& lock_primary_node) const;

    int create_lock_secondary_node(int64_t table_id,
            std::vector<std::unique_ptr<LockSecondaryNode>>& lock_secondary_nodes) const;

    int init_lock_nodes_if_not_exist() {
        if (_lock_primary != nullptr) {
            // 已经初始化完成
            return 1;
        }
        int ret = create_lock_primary_node(_table_id, _lock_primary);
        if (ret < 0) {
            return ret;
        }
        ret = create_lock_secondary_node(_table_id, _lock_secondaries);
        if (ret < 0) {
            return ret;
        }
        _op_type = pb::OP_DELETE;
        return 0;
    }

    static bool is_ddl_delete(const pb::ScanNode& scan_node);
private:
    int64_t _table_id {0};
    int64_t _index_id {0};
    std::string _task_id;
    bool _is_global_index = false;
    bool _is_rollup_index = false;

    std::unique_ptr<LockPrimaryNode> _lock_primary = nullptr;
    std::vector<std::unique_ptr<LockSecondaryNode>> _lock_secondaries;
};
} // namespace  baikaldbame
