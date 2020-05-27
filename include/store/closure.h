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
#include "region.h"

namespace baikaldb {

struct DMLClosure : public braft::Closure {
    DMLClosure() : replay_last_log_cond(nullptr) {};
    DMLClosure(BthreadCond* cond) : replay_last_log_cond(cond) {};
    virtual void Run();

    brpc::Controller* cntl = nullptr;
    pb::OpType op_type;
    pb::StoreRes* response = nullptr;
    google::protobuf::Closure* done = nullptr;
    Region* region = nullptr;
    SmartTransaction transaction = nullptr;
    TimeCost cost;
    std::string remote_side;
    BthreadCond* replay_last_log_cond;
    bool is_replay = false;
};

struct AddPeerClosure : public braft::Closure {
    AddPeerClosure(BthreadCond& cond) : cond(cond) {};
    virtual void Run(); 
    Region* region;
    std::string new_instance;
    TimeCost cost;
    google::protobuf::Closure* done = nullptr;
    pb::StoreRes* response = nullptr;
    BthreadCond& cond;
};
struct MergeClosure : public braft::Closure {
    virtual void Run();
    pb::StoreRes* response = nullptr;
    google::protobuf::Closure* done = nullptr;
    Region* region = nullptr;
    bool is_dst_region = false;
    TimeCost cost;
};
struct SplitClosure : public braft::Closure {
    virtual void Run();
    std::function<void()> next_step;
    Region* region;
    std::string new_instance;
    int64_t split_region_id;
    std::string step_message;
    pb::OpType op_type;
    int ret = 0;
    TimeCost cost;
};

struct ConvertToSyncClosure : public braft::Closure {
    ConvertToSyncClosure(BthreadCond& _sync_sign,int64_t _region_id) : 
        sync_sign(_sync_sign), region_id(_region_id) {};
    virtual void Run();
    BthreadCond& sync_sign;
    TimeCost cost;
    int64_t region_id = 0;
};

struct SnapshotClosure : public braft::Closure {
    virtual void Run() {
        if (!status().ok()) {
            DB_WARNING("region_id: %ld  status:%s, snapshot failed.",
                        region->get_region_id(), status().error_cstr());
        }
        // 遇到部分请求报has no applied logs since last snapshot
        // 不调用on_snapshot_save导致不更新_snapshot_time_cost等信息
        if (region != nullptr) {
            region->reset_snapshot_status();
        }
        cond.decrease_signal();
        delete this;
    }
    SnapshotClosure(BthreadCond& cond, Region* reg) : cond(cond), region(reg) {}
    BthreadCond& cond;
    Region* region = nullptr;
    int ret = 0;
    //int retry = 0;
};

struct Dml1pcClosure : public braft::Closure {
    Dml1pcClosure(BthreadCond& _txn_cond) : txn_cond(_txn_cond) {};

    virtual void Run();

    RuntimeState* state = nullptr;
    SmartTransaction txn = nullptr;
    BthreadCond& txn_cond;
    google::protobuf::Closure* done = nullptr;
    TimeCost cost;
};

} // end of namespace
