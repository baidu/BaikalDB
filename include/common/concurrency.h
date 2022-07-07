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

#include "common.h"
namespace baikaldb {
DECLARE_int32(snapshot_load_num);
DECLARE_int32(raft_write_concurrency);
DECLARE_int32(service_write_concurrency);
DECLARE_int32(new_sign_read_concurrency);
DECLARE_int32(baikal_heartbeat_concurrency);
DECLARE_int32(upload_sst_streaming_concurrency);

struct Concurrency {
    static Concurrency* get_instance() {
        static Concurrency _instance;
        return &_instance;
    }
    //全局做snapshot_load的并发控制
    BthreadCond snapshot_load_concurrency;
    BthreadCond recieve_add_peer_concurrency;
    BthreadCond add_peer_concurrency;
    BthreadCond raft_write_concurrency;
    BthreadCond service_write_concurrency;
    BthreadCond new_sign_read_concurrency; // 新sql读并发控制
    BthreadCond baikal_heartbeat_concurrency;
    BthreadCond upload_sst_streaming_concurrency;
private:
    Concurrency(): snapshot_load_concurrency(-FLAGS_snapshot_load_num), 
                   recieve_add_peer_concurrency(-FLAGS_snapshot_load_num), 
                   add_peer_concurrency(-FLAGS_snapshot_load_num), 
                   raft_write_concurrency(-FLAGS_raft_write_concurrency), 
                   service_write_concurrency(-FLAGS_service_write_concurrency),
                   new_sign_read_concurrency(-FLAGS_new_sign_read_concurrency),
                   baikal_heartbeat_concurrency(-FLAGS_baikal_heartbeat_concurrency),
                   upload_sst_streaming_concurrency(-FLAGS_upload_sst_streaming_concurrency) {
                   }
};
}
