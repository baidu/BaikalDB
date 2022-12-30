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
                               int64_t request_version,
                               int times) {
    int ret = 0;
    for (int i = 0; i < times; ++i) {
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

int RpcSender::get_peer_applied_index(const std::string& peer, int64_t region_id,
                                       int64_t& applied_index, int64_t& dml_latency) {
    pb::GetAppliedIndex request;
    request.set_region_id(region_id);
    pb::StoreRes response;
    
    StoreInteract store_interact(peer);
    auto ret = store_interact.send_request("get_applied_index", request, response);
    if (ret == 0) {
        applied_index = response.region_raft_stat().applied_index();
        dml_latency = response.region_raft_stat().dml_latency();
        if (dml_latency == 0) {
            dml_latency = 50000;
        }
    }
    return ret;
}

void RpcSender::get_peer_snapshot_size(const std::string& peer, int64_t region_id,
        uint64_t* data_size, uint64_t* meta_size, int64_t* snapshot_index) {
    pb::GetAppliedIndex request;
    request.set_region_id(region_id);
    pb::StoreRes response;
    
    StoreInteract store_interact(peer);
    auto ret = store_interact.send_request("get_applied_index", request, response);
    if (ret == 0) {
        *data_size = response.region_raft_stat().snapshot_data_size();
        *meta_size = response.region_raft_stat().snapshot_meta_size();
        *snapshot_index = response.region_raft_stat().snapshot_index();
    }
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

int RpcSender::send_async_apply_log(const pb::BatchStoreReq& request,
                                    pb::BatchStoreRes& response,
                                    const std::string& instance,
                                    butil::IOBuf* attachment_data) {
    uint64_t log_id = butil::fast_rand();
    TimeCost time_cost;
    StoreInteract store_interact(instance);
    return store_interact.send_request_for_leader(log_id, "async_apply_log_entry", request, response, attachment_data);
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

int RpcSender::get_leader_read_index(const std::string& leader, int64_t region_id, pb::StoreRes& response) {
    pb::GetAppliedIndex request;
    request.set_region_id(region_id);
    request.set_use_read_idx(true);
    StoreReqOptions req_options;
    req_options.request_timeout = 1000; // 1s
    req_options.connect_timeout = 1000; // 1s
    StoreInteract store_interact(leader, req_options);
    return store_interact.send_request("get_applied_index", request, response);
}
}//namespace

