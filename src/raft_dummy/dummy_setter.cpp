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

#include "can_add_peer_setter.h"
#include "split_index_getter.h"
#include "update_region_status.h"

namespace baikaldb {
void CanAddPeerSetter::set_can_add_peer(int64_t /*region_id*/) {
}
int64_t SplitIndexGetter::get_split_index(int64_t /*region_id*/) {
    return INT_FAST64_MAX;
}
void UpdateRegionStatus::reset_region_status(int64_t /*region_id*/) {
    return; 
}
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
