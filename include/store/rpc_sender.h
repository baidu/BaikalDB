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

#include "store_interact.hpp"
#include "proto/store.interface.pb.h"

namespace baikaldb {
class RpcSender {
public:
    static int send_no_op_request(const std::string& instance,
                            int64_t recevie_region_id,
                            int64_t request_version);

    static int64_t get_peer_applied_index(const std::string& peer, int64_t region_id);
    static int send_query_method(const pb::StoreReq& request, 
                const std::string& instance, 
                int64_t receive_region_id);
    static int send_query_method(const pb::StoreReq& request,
                                 pb::StoreRes& response,
                                 const std::string& instance,
                                 int64_t receive_region_id);

    static void send_remove_region_method(int64_t drop_region_id, const std::string& instance);
    static int send_init_region_method(const std::string& instance_address, 
                const pb::InitRegion& init_region_request, 
                pb::StoreRes& response);

};
} // end of namespace
