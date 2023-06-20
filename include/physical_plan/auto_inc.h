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
    /* 
     * 计算自增id
     */
    int analyze(QueryContext* ctx);

    int update_auto_inc(SmartTable table_info_ptr,
                               NetworkSocket* client_conn,
                               bool use_backup,
                               std::vector<SmartRecord>& insert_records);
    static bool need_degrade;
    static TimeCost last_degrade_time;
    static bvar::Adder<int64_t> auto_inc_count;
    static bvar::Adder<int64_t> auto_inc_error;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
