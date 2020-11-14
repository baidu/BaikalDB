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

#include "query_context.h"
#include "meta_server_interact.hpp"
namespace baikaldb {
DECLARE_string(meta_server_bns);
class AutoInc {
public:
    static MetaServerInteract auto_incr_meta_inter;
    /* 
     * 计算自增id
     */
    int analyze(QueryContext* ctx);
    static int init_meta_inter() {
        return auto_incr_meta_inter.init_internal(FLAGS_meta_server_bns);
    }
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
