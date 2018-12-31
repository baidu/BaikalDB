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

#include <functional>
#include "expr_node.h"
#include "fn_manager.h"

namespace baikaldb {
class ScalarFnCall : public ExprNode {
public:
    virtual int init(const pb::ExprNode& node);
    virtual int type_inferer();
    virtual void children_swap();
    virtual int open();
    virtual ExprValue get_value(MemRow* row);
    pb::Function fn() {
        return _fn;
    }
    virtual void transfer_pb(pb::ExprNode* pb_node) {
        ExprNode::transfer_pb(pb_node);
        pb_node->mutable_fn()->CopyFrom(_fn);
    }
private:
    pb::Function _fn;
    std::function<ExprValue(const std::vector<ExprValue>&)> _fn_call;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
