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

// Brief:  truncate table exec node
#pragma once

#include "dml_node.h"

namespace baikaldb {
class LockSecondaryNode : public DMLNode {
public:
    LockSecondaryNode() {}
    virtual ~LockSecondaryNode() {}
    virtual int init(const pb::PlanNode& node);
    virtual int open(RuntimeState* state);
    virtual void reset(RuntimeState* state);
    virtual void transfer_pb(int64_t region_id, pb::PlanNode* pb_node);
    void set_ttl_timestamp(const std::map<std::string, int64_t>& ttl_timestamp) {
        _record_ttl_map = ttl_timestamp;
    }
private:
    int insert_global_index(RuntimeState* state, SmartRecord record);
    int delete_global_index(RuntimeState* state, SmartRecord record);
    int put_global_index(RuntimeState* state, SmartRecord record);

    pb::LockSecondaryType _lock_secondary_type = pb::LST_COMMON;
    std::map<std::string, int64_t> _record_ttl_map; // ttl表增加全局二级索引时使用
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
