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
#else
#include <braft/raft.h>
#endif
#include "common.h"
#include "raft_control.h"
#include "proto/meta.interface.pb.h"
#include "proto/store.interface.pb.h"

namespace baikaldb {
class CommonStateMachine;
struct MetaServerClosure : public braft::Closure {
    virtual void Run();

    brpc::Controller* cntl;
    CommonStateMachine* common_state_machine;
    google::protobuf::Closure* done;
    pb::MetaManagerResponse* response;
    std::string request;
    int64_t raft_time_cost;
    int64_t total_time_cost;
    TimeCost time_cost;

    pb::SchemaInfo create_table_schema_pb;
    bool whether_level_table;
    std::shared_ptr<std::vector<pb::InitRegion>> init_regions;
    bool has_auto_increment = false;
    int64_t start_region_id;
    int create_table_ret = -1;
};

struct TsoClosure : public braft::Closure {
    TsoClosure() : sync_cond(nullptr) {};
    TsoClosure(BthreadCond* cond) : sync_cond(cond) {};
    virtual void Run();
    
    brpc::Controller* cntl;
    CommonStateMachine* common_state_machine;
    google::protobuf::Closure* done;
    pb::TsoResponse* response;
    int64_t raft_time_cost;
    int64_t total_time_cost;
    TimeCost time_cost;
    bool is_sync = false;
    BthreadCond* sync_cond;
};

struct ApplyraftClosure : public google::protobuf::Closure {
    virtual void Run() {
        cond.decrease_signal();
        delete this;
    }

    ApplyraftClosure(BthreadCond& cond) : cond(cond) {}
    BthreadCond& cond;
};

class CommonStateMachine : public braft::StateMachine {
public:

    CommonStateMachine(int64_t dummy_region_id, 
                       const std::string& identify, 
                       const std::string& file_path,
                       const braft::PeerId& peerId):
                _node(identify, peerId),
                _is_leader(false),
                _dummy_region_id(dummy_region_id),
                _file_path(file_path),
                _check_migrate(&BTHREAD_ATTR_SMALL) {}
    
    virtual ~CommonStateMachine() {}

    virtual int init(const std::vector<braft::PeerId>& peers);
    
    virtual void raft_control(google::protobuf::RpcController* controller,
                          const pb::RaftControlRequest* request,
                          pb::RaftControlResponse* response,
                          google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (!is_leader() && !request->force()) {
            DB_WARNING("node is not leader when raft control, region_id: %ld", request->region_id());
            response->set_errcode(pb::NOT_LEADER);
            response->set_region_id(request->region_id());
            response->set_leader(butil::endpoint2str(_node.leader_id().addr).c_str());
            response->set_errmsg("not leader");
            return;
        }
        common_raft_control(controller, request, response, done_guard.release(), &_node);
    }
    virtual void process(google::protobuf::RpcController* controller,
                 const pb::MetaManagerRequest* request,
                 pb::MetaManagerResponse* response,
                 google::protobuf::Closure* done);
   
    virtual void start_check_migrate();
    virtual void check_migrate();
    // state machine method
    virtual void on_apply(braft::Iterator& iter) = 0;
    
    virtual void on_shutdown() {
        DB_WARNING("raft is shut down");
    };

    virtual void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) = 0;

    virtual int on_snapshot_load(braft::SnapshotReader* reader) = 0;

    virtual void on_leader_start();

    virtual void on_leader_start(int64_t term);

    virtual void on_leader_stop();

    virtual void on_leader_stop(const butil::Status& status);

    virtual void on_error(const ::braft::Error& e);

    virtual void on_configuration_committed(const ::braft::Configuration& conf);
    
    virtual butil::EndPoint get_leader() {
        return _node.leader_id().addr;                                                    
    }
    virtual void shutdown_raft()  {
        _node.shutdown(NULL);
        DB_WARNING("raft node was shutdown"); 
        _node.join();
        DB_WARNING("raft node join completely");
    }
    void start_check_bns();
    virtual bool is_leader() const {
        return _is_leader;
    }
    bool have_data() {
        return _have_data;
    }
    void set_have_data(bool flag) {
         _have_data = flag;
    }
private:
    virtual int send_set_peer_request(bool remove_peer, const std::string& change_peer);
protected:
    braft::Node          _node;
    std::atomic<bool>   _is_leader;
    int64_t             _dummy_region_id;
    std::string         _file_path;
private:
    Bthread             _check_migrate;
    bool                _check_start = false;
    bool                _have_data = false;
};

} //namespace baikaldb

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
