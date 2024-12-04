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

#include "user_info.h"

namespace baikaldb {
DEFINE_int32(query_quota_per_user, 3000, "default user query quota by 1 second");
BRPC_VALIDATE_GFLAG(query_quota_per_user, brpc::PassValidate);

bool UserInfo::is_exceed_quota() {
    if (query_cost.get_time() > 1000000) {
        query_cost.reset();
        query_count = 0;
        return false;
    }
    int32_t quota = query_quota;
    if (quota == 0) {
        quota = FLAGS_query_quota_per_user;
    }
    return query_count++ > quota;
}

} // namespace baikaldb
