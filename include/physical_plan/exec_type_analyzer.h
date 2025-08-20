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

#include "exec_node.h"
#include "query_context.h"

namespace baikaldb {
class ExecTypeAnalyzer {
public:
    int analyze(QueryContext* ctx);
private:
    bool can_use_arrow_vector(QueryContext* ctx);
    bool can_use_mpp(QueryContext* ctx);
    void set_subquery_exec_type(QueryContext* ctx);
    bool has_full_export(QueryContext* ctx);
    bool adjust_mpp_by_statistics(QueryContext* ctx);
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
