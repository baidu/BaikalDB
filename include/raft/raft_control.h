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
#include <raft/raft.h>                    
#include <baidu/rpc/channel.h>
#include <baidu/rpc/server.h>
#include <baidu/rpc/controller.h>
#else
#include <braft/raft.h>                    
#include <brpc/channel.h>
#include <brpc/server.h>
#include <brpc/controller.h>
#endif
#include "proto/raft.pb.h"
#include "common.h"

namespace baikaldb {
extern void common_raft_control(google::protobuf::RpcController* controller,
                         const pb::RaftControlRequest* request,
                         pb::RaftControlResponse* response,
                         google::protobuf::Closure* done,
                         braft::Node* node);
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
