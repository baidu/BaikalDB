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

#include "rpc_sender.h"

namespace baikaldb {
DECLARE_int64(print_time_us);
int RpcSender::send_no_op_request(const std::string& instance,
                               int64_t recevie_region_id,
                               int64_t request_version) {
    int ret = 0;
    for (int i = 0; i < 5; ++i) {
        pb::StoreReq request;
        request.set_op_type(pb::OP_NONE);
        request.set_region_id(recevie_region_id);
        request.set_region_version(request_version);
        ret = send_query_method(request, instance, recevie_region_id);
        if (ret < 0) {
            DB_WARNING("send no op fail, region_id: %ld, reqeust: %s",
                        recevie_region_id, request.ShortDebugString().c_str());
            bthread_usleep(1000 * 1000LL);
        }
    }
    return ret;
}

int64_t RpcSender::get_peer_applied_index(const std::string& peer, int64_t region_id) {
    pb::GetAppliedIndex request;
    request.set_region_id(region_id);
    pb::StoreRes response;
    
    StoreInteract store_interact(peer);
    auto ret = store_interact.send_request("get_applied_index", request, response);
    if (ret == 0) {
        return response.applied_index();
    }
    return 0;
}

int RpcSender::send_query_method(const pb::StoreReq& request,
                                        const std::string& instance,
                                        int64_t receive_region_id) {
    uint64_t log_id = butil::fast_rand();
    TimeCost time_cost;
    StoreInteract store_interact(instance);
    pb::StoreRes response;
    auto ret = store_interact.send_request_for_leader(log_id, "query", request, response);
    if (ret == 0) {
        if (time_cost.get_time() > FLAGS_print_time_us) {
            DB_WARNING("send request to new region success,"
                    " response:%s, receive_region_id: %ld, time_cost:%ld",
                    pb2json(response).c_str(),
                    receive_region_id,
                    time_cost.get_time());
        }
        return 0;
    }
    return -1;
}

int RpcSender::send_query_method(const pb::StoreReq& request,
                                 pb::StoreRes& response,
                                 const std::string& instance,
                                 int64_t receive_region_id) {
    uint64_t log_id = butil::fast_rand();
    TimeCost time_cost;
    StoreInteract store_interact(instance);
    return store_interact.send_request_for_leader(log_id, "query", request, response);
}

void RpcSender::send_remove_region_method(int64_t drop_region_id, const std::string& instance) {
    pb::StoreRes response;
    pb::RemoveRegion remove_region_request;
    remove_region_request.set_region_id(drop_region_id);
    remove_region_request.set_force(true);
    StoreInteract store_interact(instance);
    store_interact.send_request("remove_region", remove_region_request, response); 
}

int RpcSender::send_init_region_method(const std::string& instance, 
            const pb::InitRegion& init_region_request, 
            pb::StoreRes& response) {
    StoreInteract store_interact(instance);
    return store_interact.send_request("init_region", init_region_request, response);
}
}//namespace

