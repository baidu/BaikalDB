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
#ifdef BAIDU_INTERNAL
#include <base/endpoint.h>
#include <baidu/rpc/channel.h>
#include <baidu/rpc/server.h>
#include <baidu/rpc/controller.h>
#else
#include <butil/endpoint.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include <brpc/controller.h>
#endif
#include <google/protobuf/descriptor.h>
#include "proto/meta.interface.pb.h"
#include "common.h"
#include "proto_process.hpp"

namespace baikaldb {
DECLARE_int64(time_between_meta_connect_error_ms);
class MetaServerInteract {
    struct MetaInteractInfo;
public:
    static const int RETRY_TIMES = 5;
    
    static MetaServerInteract* get_instance() {
        static MetaServerInteract _instance;
        return &_instance;
    }

    static MetaServerInteract* get_auto_incr_instance() {
        static MetaServerInteract _instance;
        return &_instance;
    }

    static MetaServerInteract* get_tso_instance() {
        static MetaServerInteract _instance;
        return &_instance;
    }

    static MetaServerInteract* get_backup_instance() {
        static MetaServerInteract _instance;
        return &_instance;
    }


    MetaServerInteract() {}
    bool is_inited() {
        return _meta_interact_info.is_inited;
    }
    int init(bool is_backup = false);
    int init_internal(const std::string& meta_bns);
    int reset_bns_channel(const std::string& meta_bns);
    int init_other_meta(const int64_t meta_id);
    int init_meta_interact(const std::string& meta_bns, MetaInteractInfo& meta_interact_info);

    template<typename Request, typename Response>
    int send_request_internal(const std::string& service_name,
                     const Request& request,
                     Response& response,
                     MetaInteractInfo& meta_interact_info) {
        const ::google::protobuf::ServiceDescriptor* service_desc = pb::MetaService::descriptor();
        const ::google::protobuf::MethodDescriptor* method = service_desc->FindMethodByName(service_name);
        if (method == NULL) {
            DB_FATAL("service name not exist, service:%s", service_name.c_str());
            return -1;
        }
        int retry_time = 0;
        uint64_t log_id = butil::fast_rand();
        do {
            if (retry_time > 0 && FLAGS_time_between_meta_connect_error_ms > 0) {
                bthread_usleep(1000 * FLAGS_time_between_meta_connect_error_ms);
            }
            brpc::Controller cntl;
            cntl.set_log_id(log_id);
            butil::EndPoint leader_address = get_leader_address(meta_interact_info);
            // store has leader address
            if (leader_address.ip != butil::IP_ANY) {
                // construct short connection
                brpc::ChannelOptions channel_opt;
                channel_opt.timeout_ms = _request_timeout;
                channel_opt.connect_timeout_ms = _connect_timeout;
                brpc::Channel short_channel;
                if (short_channel.Init(leader_address, &channel_opt) != 0) {
                    DB_WARNING("connect with meta server fail. channel Init fail, leader_addr:%s",
                                butil::endpoint2str(leader_address).c_str());
                    set_leader_address(meta_interact_info, butil::EndPoint());
                    continue;
                }
                short_channel.CallMethod(method, &cntl, &request, &response, NULL);
            } else {
                std::unique_lock<bthread::Mutex> lck(meta_interact_info.bns_channel_mutex);
                meta_interact_info.bns_channel->CallMethod(method, &cntl, &request, &response, NULL);
                if (!cntl.Failed() && response.errcode() == pb::SUCCESS) {
                    set_leader_address(meta_interact_info, cntl.remote_side());
                    DB_WARNING("connet with meta server success by bns name, leader:%s",
                                butil::endpoint2str(cntl.remote_side()).c_str());
                    return 0;
                }
            }
            SELF_TRACE("meta_req[%s], meta_resp[%s]", request.ShortDebugString().c_str(), response.ShortDebugString().c_str());
            if (cntl.Failed()) {
                DB_WARNING("connect with server fail. send request fail, error:%s, log_id:%lu",
                            cntl.ErrorText().c_str(), cntl.log_id());
                set_leader_address(meta_interact_info, butil::EndPoint());
                continue;
            }
            if (response.errcode() == pb::HAVE_NOT_INIT) {
                DB_WARNING("connect with server fail. HAVE_NOT_INIT  log_id:%lu",
                        cntl.log_id());
                set_leader_address(meta_interact_info, butil::EndPoint());
                continue;
            }
            if (response.errcode() == pb::NOT_LEADER) {
                DB_WARNING("connect with meta server:%s fail. not leader, redirect to :%s, log_id:%lu",
                            butil::endpoint2str(cntl.remote_side()).c_str(),
                            response.leader().c_str(), cntl.log_id());
                butil::EndPoint leader_addr;
                butil::str2endpoint(response.leader().c_str(), &leader_addr);
                set_leader_address(meta_interact_info, leader_addr);
                continue;
            }
            if (response.errcode() != pb::SUCCESS) {
                DB_WARNING("send meta server fail, log_id:%lu, response:%s", 
                            cntl.log_id(), response.ShortDebugString().c_str());
                return -1;
            } else {
                return 0;
            }
        } while (retry_time++ < RETRY_TIMES);
        return -1;
    }

    template<typename Request, typename Response>
    int send_request(const std::string& service_name,
                     const Request& request,
                     Response& response,
                     const int64_t meta_id = 0) {
        if (meta_id != 0) {
            return send_other_meta_request(service_name, request, response, meta_id);
        }
        return send_request_internal(service_name, request, response, _meta_interact_info);
    }

    template<typename Request, typename Response>
    int send_other_meta_request(const std::string& service_name,
                                const Request& request,
                                Response& response,
                                const int64_t meta_id) {
        std::shared_ptr<MetaInteractInfo> meta_interact_info;
        if (get_meta_interact_info(meta_id, meta_interact_info) != 0) {
            // meta_interact_info不存在
            if (init_other_meta(meta_id) != 0) {
                DB_WARNING("Fail to init other meta, meta_id:%ld", meta_id);
                return -1;
            }
            if (get_meta_interact_info(meta_id, meta_interact_info) != 0) {
                DB_WARNING("Fail to get other meta, meta_id:%ld", meta_id);
                return -1;
            }
        }
        if (meta_interact_info == nullptr || meta_interact_info->is_inited == false) {
            DB_WARNING("meta_interact_info is nullptr, meta_id:%ld", meta_id);
            return -1;
        }
        int ret = del_meta_info(const_cast<Request&>(request));
        if (ret != 0) {
            DB_WARNING("Fail to del_meta_info, meta_id: %ld, request: %s", 
                        meta_id, request.ShortDebugString().c_str());
            return -1;
        }
        ret = send_request_internal(service_name, request, response, *meta_interact_info);
        if (ret != 0) {
            DB_WARNING("Fail to send_request_internal, meta_id: %ld, request: %s", 
                        meta_id, request.ShortDebugString().c_str());
            return -1;
        }
        ret = add_meta_info(response, meta_id);
        if (ret != 0) {
            DB_WARNING("Fail to add_meta_info, meta_id: %ld, response: %s", 
                        meta_id, response.ShortDebugString().c_str());
            return -1;
        }
        return 0;
    }

    void set_leader_address(MetaInteractInfo& meta_interact_info, const butil::EndPoint& addr) {
        std::lock_guard<bthread::Mutex> lk(meta_interact_info.master_leader_mutex);
        meta_interact_info.master_leader_address = addr;
    }

    butil::EndPoint get_leader_address(MetaInteractInfo& meta_interact_info) {
        std::lock_guard<bthread::Mutex> lk(meta_interact_info.master_leader_mutex);
        butil::EndPoint master_leader_address = meta_interact_info.master_leader_address;
        return master_leader_address;
    }

    int get_meta_interact_info(const int64_t& meta_id, std::shared_ptr<MetaInteractInfo>& meta_interact_info) {
        std::lock_guard<std::mutex> lk(_meta_map_mutex);
        if (_meta_interact_info_map.find(meta_id) == _meta_interact_info_map.end()) {
            return -1;
        }
        meta_interact_info = _meta_interact_info_map[meta_id];
        return 0;
    }

    MetaInteractInfo& get_meta_interact_info() {
        return _meta_interact_info;
    }

private:
    int32_t _request_timeout = 30000;
    int32_t _connect_timeout = 5000;

    struct MetaInteractInfo {
        bool is_inited = false;
        brpc::Channel* bns_channel = nullptr;
        bthread::Mutex master_leader_mutex;
        bthread::Mutex bns_channel_mutex;
        butil::EndPoint master_leader_address;
    };
    MetaInteractInfo _meta_interact_info;

    // <meta_id, meta_interact_info>
    std::mutex _meta_map_mutex;
    std::unordered_map<int64_t, std::shared_ptr<MetaInteractInfo>> _meta_interact_info_map;
};
}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
