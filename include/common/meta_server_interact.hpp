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

namespace baikaldb {
DECLARE_int64(time_between_meta_connect_error_ms);
class MetaServerInteract {
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
        return _is_inited;
    }
    int init(bool is_backup = false);
    int init_internal(const std::string& meta_bns);
    template<typename Request, typename Response>
    int send_request(const std::string& service_name,
                                     const Request& request,
                                     Response& response) {
        const ::google::protobuf::ServiceDescriptor* service_desc = pb::MetaService::descriptor();
        const ::google::protobuf::MethodDescriptor* method = 
                    service_desc->FindMethodByName(service_name);
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
            std::unique_lock<std::mutex> lck(_master_leader_mutex);
            butil::EndPoint leader_address = _master_leader_address;
            lck.unlock();
            //store has leader address
            if (leader_address.ip != butil::IP_ANY) {
                //construct short connection
                brpc::ChannelOptions channel_opt;
                channel_opt.timeout_ms = _request_timeout;
                channel_opt.connect_timeout_ms = _connect_timeout;
                brpc::Channel short_channel;
                if (short_channel.Init(leader_address, &channel_opt) != 0) {
                    DB_WARNING("connect with meta server fail. channel Init fail, leader_addr:%s",
                                butil::endpoint2str(leader_address).c_str());
                    _set_leader_address(butil::EndPoint());
                    ++retry_time;
                    continue;
                }
                short_channel.CallMethod(method, &cntl, &request, &response, NULL);
            } else {
                _bns_channel.CallMethod(method, &cntl, &request, &response, NULL);
                if (!cntl.Failed() && response.errcode() == pb::SUCCESS) {
                    _set_leader_address(cntl.remote_side());
                    DB_WARNING("connet with meta server success by bns name, leader:%s",
                                butil::endpoint2str(cntl.remote_side()).c_str());
                    return 0;
                }
            }

            SELF_TRACE("meta_req[%s], meta_resp[%s]", request.ShortDebugString().c_str(), response.ShortDebugString().c_str());
            if (cntl.Failed()) {
                DB_WARNING("connect with server fail. send request fail, error:%s, log_id:%lu",
                            cntl.ErrorText().c_str(), cntl.log_id());
                _set_leader_address(butil::EndPoint());
                ++retry_time;
                continue;
            }
            if (response.errcode() == pb::HAVE_NOT_INIT) {
                DB_WARNING("connect with server fail. HAVE_NOT_INIT  log_id:%lu",
                        cntl.log_id());
                _set_leader_address(butil::EndPoint());
                ++retry_time;
                continue;
            }
            if (response.errcode() == pb::NOT_LEADER) {
                DB_WARNING("connect with meta server:%s fail. not leader, redirect to :%s, log_id:%lu",
                            butil::endpoint2str(cntl.remote_side()).c_str(),
                            response.leader().c_str(), cntl.log_id());
                butil::EndPoint leader_addr;
                butil::str2endpoint(response.leader().c_str(), &leader_addr);
                _set_leader_address(leader_addr);
                ++retry_time;
                continue;
            }
            if (response.errcode() != pb::SUCCESS) {
                DB_WARNING("send meta server fail, log_id:%lu, response:%s", 
                        cntl.log_id(),
                        response.ShortDebugString().c_str());
                return -1;
            } else {
                return 0;
            }
        } while (retry_time < RETRY_TIMES);
        return -1;
    }

    void _set_leader_address(const butil::EndPoint& addr) {
        std::unique_lock<std::mutex> lock(_master_leader_mutex);
        _master_leader_address = addr;
    }
private:
    brpc::Channel _bns_channel;
    int32_t _request_timeout = 30000;
    int32_t _connect_timeout = 5000;
    bool _is_inited = false;
    std::mutex _master_leader_mutex;
    butil::EndPoint _master_leader_address;
};
}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
