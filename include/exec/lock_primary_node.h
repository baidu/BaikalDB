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
class LockPrimaryNode : public DMLNode {
public:
    LockPrimaryNode() {}
    virtual ~LockPrimaryNode() {}
    virtual int init(const pb::PlanNode& pb_node);
    virtual int open(RuntimeState* state);
    virtual void transfer_pb(int64_t region_id, pb::PlanNode* pb_node);    
    //virtual int init_schema_info(RuntimeState* state);
private:
    int lock_get_main_table(RuntimeState* state, SmartRecord record);
    int put_row(RuntimeState* state, SmartRecord record);
};

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
