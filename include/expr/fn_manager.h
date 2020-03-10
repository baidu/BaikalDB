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

#include <functional>
#include "expr_value.h"
#include "proto/expr.pb.h"
#include "object_manager.h"

namespace baikaldb {
class FunctionManager : public ObjectManager<
                        std::function<ExprValue(const std::vector<ExprValue>&)>, 
                        FunctionManager> {
public:
    int init();
    bool swap_op(pb::Function& fn);
    static int complete_fn(pb::Function& fn, std::vector<pb::PrimitiveType> types);
    static void complete_common_fn(pb::Function& fn, std::vector<pb::PrimitiveType>& types);
private:
    void register_operators();
    static void complete_fn_simple(pb::Function& fn, int num_args, 
            pb::PrimitiveType arg_type, pb::PrimitiveType ret_type);
    static void complete_fn(pb::Function& fn, int num_args, 
            pb::PrimitiveType arg_type, pb::PrimitiveType ret_type);
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
