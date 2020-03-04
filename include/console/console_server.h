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

#include <google/protobuf/descriptor.h>

#include "meta_proxy.h" 
#include "baikaldb_proxy.h"

namespace baikaldb { 

class ConsoleServer : public pb::ConsoleService {

public:
    virtual ~ConsoleServer();

    static ConsoleServer* get_instance() {
        static ConsoleServer _instance;
        return &_instance;
    }

    int init();
    void stop();
 
    void watch(google::protobuf::RpcController* controller,
                       const pb::ConsoleRequest* request,
                       pb::ConsoleResponse* response,
                       google::protobuf::Closure* done);
private:
    int parser_param(brpc::Controller* controller, pb::QueryParam* param);
    BaikalProxy*     _baikaldb = nullptr;
    MetaProxy*       _meta_server = nullptr;

    bool _init_success  = false;
    std::set<pb::WatchOpType> _status_op_type;
};

} // namespace baikaldb

/* vim: set ts=4 sw=4 sts=4 tw=100 */
