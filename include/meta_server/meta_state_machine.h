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

#include <rocksdb/db.h>
#include "common_state_machine.h"
#include "proto/meta.interface.pb.h"

namespace baikaldb {
class MetaStateMachine : public CommonStateMachine {
public:
    MetaStateMachine(const braft::PeerId& peerId):
                CommonStateMachine(0, "meta_raft", "/meta_server", peerId),
                _bth(&BTHREAD_ATTR_SMALL),
                _healthy_check_start(false),
                _baikal_heart_beat("baikal_heart_beat"),
                _store_heart_beat("store_heart_beat") {
        bthread_mutex_init(&_param_mutex, NULL);        
    }
    
    virtual ~MetaStateMachine() {
        bthread_mutex_destroy(&_param_mutex);
    }

    void store_heartbeat(google::protobuf::RpcController* controller,             
                         const pb::StoreHeartBeatRequest* request,                
                         pb::StoreHeartBeatResponse* response,                    
                         google::protobuf::Closure* done); 
    
    void baikal_heartbeat(google::protobuf::RpcController* controller,             
                         const pb::BaikalHeartBeatRequest* request,                
                         pb::BaikalHeartBeatResponse* response,                    
                         google::protobuf::Closure* done); 

    void baikal_other_heartbeat(google::protobuf::RpcController* controller,             
                         const pb::BaikalOtherHeartBeatRequest* request,                
                         pb::BaikalOtherHeartBeatResponse* response,                    
                         google::protobuf::Closure* done); 
    
    void console_heartbeat(google::protobuf::RpcController* controller,             
                         const pb::ConsoleHeartBeatRequest* request,                
                         pb::ConsoleHeartBeatResponse* response,                    
                         google::protobuf::Closure* done); 

    void healthy_check_function();
    
    // state machine method
    virtual void on_apply(braft::Iterator& iter);
    
    virtual void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done);

    virtual int on_snapshot_load(braft::SnapshotReader* reader);

    virtual void on_leader_start();

    virtual void on_leader_stop();

    int64_t applied_index() { return _applied_index; }

    //经过3个周期后才可以做决策
    bool whether_can_decide();

    void set_global_load_balance(bool open) {
        BAIDU_SCOPED_LOCK(_param_mutex);
        _global_load_balance = open;
        _resource_load_balance.clear();
    }
    void set_load_balance(const std::string& resource_tag, bool open) {
        BAIDU_SCOPED_LOCK(_param_mutex);
        _resource_load_balance[resource_tag] = open;
    }
    bool get_load_balance(const std::string& resource_tag) {
        BAIDU_SCOPED_LOCK(_param_mutex);
        if (_resource_load_balance.find(resource_tag) != _resource_load_balance.end()) {
                return _resource_load_balance[resource_tag];
        }
        return _global_load_balance;
    }
    void set_global_migrate(bool open) {
        BAIDU_SCOPED_LOCK(_param_mutex);
        _global_migrate = open;
        _resource_migrate.clear();
    }
    void set_migrate(const std::string& resource_tag, bool open) {
        BAIDU_SCOPED_LOCK(_param_mutex);
        _resource_migrate[resource_tag] = open;
    }
    bool get_migrate(const std::string& resource_tag) {
        BAIDU_SCOPED_LOCK(_param_mutex);
        if (_resource_migrate.find(resource_tag) != _resource_migrate.end()) {
            return _resource_migrate[resource_tag];
        }
        return _global_migrate;
    }
    void set_global_network_segment_balance(bool open) {
        BAIDU_SCOPED_LOCK(_param_mutex);
        _global_network_balance = open;
        _resource_network_balance.clear();
    }
    void set_network_segment_balance(const std::string& resource_tag, bool open) {
        BAIDU_SCOPED_LOCK(_param_mutex);
        _resource_network_balance[resource_tag] = open;
    }
    bool get_network_segment_balance(const std::string& resource_tag) {
        BAIDU_SCOPED_LOCK(_param_mutex);
        if (_resource_network_balance.find(resource_tag) != _resource_network_balance.end()) {
            return _resource_network_balance[resource_tag];
        }
        return _global_network_balance;
    }
    void set_unsafe_decision(bool open) {
        _unsafe_decision = open;
    }
    bool get_unsafe_decision() {
        return _unsafe_decision;
    }
private:
    void save_snapshot(braft::Closure* done,
                        rocksdb::Iterator* iter,
                        braft::SnapshotWriter* writer);

    int64_t _leader_start_timestmap;
    Bthread _bth;    
    bool _healthy_check_start;

    bthread_mutex_t         _param_mutex;
    bool _global_load_balance = true;
    std::map<std::string, bool> _resource_load_balance;

    bool _global_migrate = true;
    std::map<std::string, bool> _resource_migrate;

    bool _global_network_balance = true;
    std::map<std::string, bool> _resource_network_balance;

    bool _unsafe_decision = false;
    int64_t _applied_index = 0;
    bvar::LatencyRecorder   _baikal_heart_beat;
    bvar::LatencyRecorder   _store_heart_beat;
};

} //namespace baikaldb

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
