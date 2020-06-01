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

#include "query_context.h"
#include "exec_node.h"

namespace baikaldb {
DEFINE_bool(default_2pc, false, "default enable/disable 2pc for autocommit queries");
QueryContext::~QueryContext() {
    if (need_destroy_tree) {
        ExecNode::destroy_tree(root);
    }
}

int QueryContext::create_plan_tree() {
    need_destroy_tree = true;
    return ExecNode::create_tree(plan, &root);
}

}
