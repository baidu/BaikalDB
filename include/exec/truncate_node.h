// Copyright (c) 2018 Baidu, Inc. All Rights Reserved.
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

#include "exec_node.h"

namespace baikaldb {
class TruncateNode : public ExecNode {
public:
    TruncateNode() {
    }
    virtual ~TruncateNode() {
    }
    virtual int init(const pb::PlanNode& node);
    virtual int open(RuntimeState* state);
    virtual void transfer_pb(pb::PlanNode* pb_node);

    int64_t table_id() {
        return _table_id;
    }
private:
    int64_t _region_id = 0;
    int64_t _table_id = 0;
};

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
