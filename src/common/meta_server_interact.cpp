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

#include "meta_server_interact.hpp"
#include "schema_factory.h"
#include <gflags/gflags.h>

namespace baikaldb {
DEFINE_int32(meta_request_timeout, 30000, 
            "meta as server request timeout, default:30000ms");
DEFINE_int32(meta_connect_timeout, 5000, 
            "meta as server connect timeout, default:5000ms");
DEFINE_string(meta_server_bns, "group.opera-qa-baikalMeta-000-yz.FENGCHAO.all", "meta server bns");
DEFINE_string(backup_meta_server_bns, "", "backup_meta_server_bns");
DEFINE_int64(time_between_meta_connect_error_ms, 0, "time_between_meta_connect_error_ms. default(0ms)");
DEFINE_bool(auto_update_meta_list, false, "auto_update_meta_list, default false");

int MetaServerInteract::init(bool is_backup) {
    if (is_backup) {
        if (!FLAGS_backup_meta_server_bns.empty()) {
            return init_internal(FLAGS_backup_meta_server_bns);
        }
    } else {
        return init_internal(FLAGS_meta_server_bns);
    }
    return 0;
}

int MetaServerInteract::init_internal(const std::string& meta_bns) {
    _connect_timeout = FLAGS_meta_connect_timeout;
    _request_timeout = FLAGS_meta_request_timeout;
    return init_meta_interact(meta_bns, _meta_interact_info); 
}

int MetaServerInteract::init_other_meta(const int64_t meta_id) {
    std::string meta_bns;
    if (SchemaFactory::get_instance()->get_meta_name(meta_id, meta_bns) != 0) {
        DB_FATAL("get_meta_name fail. bns:%ld", meta_id);
        return -1;
    }
    std::shared_ptr<MetaInteractInfo> p_meta_interact_info = std::make_shared<MetaInteractInfo>();
    if (init_meta_interact(meta_bns, *p_meta_interact_info) != 0) {
        DB_WARNING("init_meta_interact fail. meta_id: %ld, meta_bns: %s", meta_id, meta_bns.c_str());
        return -1;
    }
    std::lock_guard<std::mutex> lock(_meta_map_mutex);
    if (_meta_interact_info_map.find(meta_id) == _meta_interact_info_map.end()) {
        _meta_interact_info_map[meta_id] = p_meta_interact_info;
    }
    return 0;
}

int MetaServerInteract::init_meta_interact(const std::string& meta_bns, MetaInteractInfo& meta_interact_info) {
    meta_interact_info.master_leader_address.ip = butil::IP_ANY;
    meta_interact_info.master_leader_address.port = 0;

    //初始化channel，但是该channel是meta_server的 bns pool，大部分时间用不到
    brpc::ChannelOptions channel_opt;
    channel_opt.timeout_ms = FLAGS_meta_request_timeout;
    channel_opt.connect_timeout_ms = FLAGS_meta_connect_timeout;
    std::string meta_server_addr = meta_bns;
    //bns
    if (meta_bns.find(":") == std::string::npos) {
        meta_server_addr = std::string("bns://") + meta_bns;
    } else {
        meta_server_addr = std::string("list://") + meta_bns;
    }
    std::unique_lock<bthread::Mutex> lck(meta_interact_info.bns_channel_mutex);
    if (meta_interact_info.bns_channel == nullptr) {
        meta_interact_info.bns_channel = new brpc::Channel();
    }
    if (meta_interact_info.bns_channel->Init(meta_server_addr.c_str(), "rr", &channel_opt) != 0) {
        DB_WARNING("meta server bns pool init fail. bns_name:%s", meta_server_addr.c_str());
        return -1;
    }
    meta_interact_info.is_inited = true;
    return 0;
}


int MetaServerInteract::reset_bns_channel(const std::string& meta_bns) {
    brpc::ChannelOptions channel_opt;
    channel_opt.timeout_ms = FLAGS_meta_request_timeout;
    channel_opt.connect_timeout_ms = FLAGS_meta_connect_timeout;
    std::string meta_server_addr = meta_bns;
    //bns
    if (meta_bns.find(":") == std::string::npos) {
        meta_server_addr = std::string("bns://") + meta_bns;
    } else {
        meta_server_addr = std::string("list://") + meta_bns;
    }
    brpc::Channel *tmp = new brpc::Channel();
    if (tmp->Init(meta_server_addr.c_str(), "rr", &channel_opt) != 0) {
        delete tmp;
        DB_FATAL("meta server bns pool init fail. bns_name:%s", meta_server_addr.c_str());
        return -1;
    }
    std::unique_lock<bthread::Mutex> lck(_meta_interact_info.bns_channel_mutex);
    SAFE_DELETE(_meta_interact_info.bns_channel);
    _meta_interact_info.bns_channel = tmp;
    return 0; 
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
