// Copyright (c) 2018 Baidu, Inc. All Rights Reserved.
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

#include <rocksdb/db.h>
#include "common_state_machine.h"
#include "proto/meta.interface.pb.h"

namespace baikaldb {
class MetaStateMachine : public CommonStateMachine {
public:
    MetaStateMachine(const braft::PeerId& peerId):
                CommonStateMachine(0, "meta_raft", "/meta_server", peerId),
                _bth(&BTHREAD_ATTR_SMALL),
                _healthy_check_start(false) {}
    
    virtual ~MetaStateMachine() {}

    void store_heartbeat(google::protobuf::RpcController* controller,             
                         const pb::StoreHeartBeatRequest* request,                
                         pb::StoreHeartBeatResponse* response,                    
                         google::protobuf::Closure* done); 
    
    void baikal_heartbeat(google::protobuf::RpcController* controller,             
                         const pb::BaikalHeartBeatRequest* request,                
                         pb::BaikalHeartBeatResponse* response,                    
                         google::protobuf::Closure* done); 
    
    void healthy_check_function();
    
    // state machine method
    virtual void on_apply(braft::Iterator& iter);
    
    virtual void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done);

    virtual int on_snapshot_load(braft::SnapshotReader* reader);

    virtual void on_leader_start();

    virtual void on_leader_stop();

    //经过3个周期后才可以做决策
    bool whether_can_decide();

    void set_close_load_balance(bool close) {
        _close_load_balance = close;
    }
    bool get_close_load_balance() {
        return _close_load_balance;
    }
private:
    void save_snapshot(braft::Closure* done,
                        rocksdb::Iterator* iter,
                        braft::SnapshotWriter* writer);

    int64_t _leader_start_timestmap;
    Bthread _bth;    
    bool _healthy_check_start;
    bool _close_load_balance = true;
};

} //namespace baikaldb

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
