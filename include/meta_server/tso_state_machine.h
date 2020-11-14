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

#include "common_state_machine.h"

#ifdef BAIDU_INTERNAL
#include <raft/repeated_timer_task.h>
#else
#include <braft/repeated_timer_task.h>
#endif
#include <time.h>

namespace baikaldb {

class TSOStateMachine;
class TsoTimer : public braft::RepeatedTimerTask {
public:
    TsoTimer() : _node(NULL) {}
    virtual ~TsoTimer() {}
    int init(TSOStateMachine* node, int timeout_ms);
    virtual void run();
protected:
    virtual void on_destroy() {}
    TSOStateMachine* _node;
};

struct TsoObj {
    pb::TsoTimestamp current_timestamp;
    int64_t last_save_physical;
};

class TSOStateMachine : public baikaldb::CommonStateMachine {
public:
    TSOStateMachine(const braft::PeerId& peerId):
                CommonStateMachine(2, "tso_raft", "/tso", peerId) {
        bthread_mutex_init(&_tso_mutex, nullptr);
    }
    
    virtual ~TSOStateMachine() {
        _tso_update_timer.stop();
        _tso_update_timer.destroy();
        bthread_mutex_destroy(&_tso_mutex);
    }

    virtual int init(const std::vector<braft::PeerId>& peers);

    // state machine method
    virtual void on_apply(braft::Iterator& iter);
    void process(google::protobuf::RpcController* controller,
                               const pb::TsoRequest* request,
                               pb::TsoResponse* response,
                               google::protobuf::Closure* done);
    
    void gen_tso(const pb::TsoRequest* request, pb::TsoResponse* response);
    void reset_tso(const pb::TsoRequest& request, braft::Closure* done);
    void update_tso(const pb::TsoRequest& request, braft::Closure* done);

    int load_tso(const std::string& tso_file);
    int sync_timestamp(const pb::TsoTimestamp& current_timestamp, int64_t save_physical);
    void update_timestamp();

    virtual void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done);
    void save_snapshot(braft::Closure* done,
                       braft::SnapshotWriter* writer,
                       std::string sto_str);

    virtual int on_snapshot_load(braft::SnapshotReader* reader);

    virtual void on_leader_start();

    virtual void on_leader_stop();

    static const  std::string SNAPSHOT_TSO_FILE;;
    static const  std::string SNAPSHOT_TSO_FILE_WITH_SLASH;

private:
    TsoTimer  _tso_update_timer;
    TsoObj    _tso_obj;
    bthread_mutex_t _tso_mutex;  // 保护_tso_obj，C++20 atomic<std::shared_ptr<U>>
    bool    _is_healty = true;
};

} //namespace baikaldb

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
