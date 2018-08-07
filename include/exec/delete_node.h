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

#pragma once

#include "exec_node.h"
#include "dml_node.h"
#include "transaction.h"

namespace baikaldb {
class DeleteNode : public DMLNode {
public:
    DeleteNode() {
    }
    virtual ~DeleteNode() {
    }
    virtual int init(const pb::PlanNode& node);
    virtual int open(RuntimeState* state);

private:
    std::vector<pb::SlotDescriptor> _primary_slots;
};

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
