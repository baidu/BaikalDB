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
#include "proto/meta.interface.pb.h"
#include "common.h"
#include "meta_server_interact.hpp"

namespace baikaldb {
class MetaStateMachine;
class AutoIncrStateMachine;
class MetaServer : public pb::MetaService {
public:
    static const std::string CLUSTER_IDENTIFY;
    static const std::string LOGICAL_CLUSTER_IDENTIFY;
    static const std::string LOGICAL_KEY;
    static const std::string PHYSICAL_CLUSTER_IDENTIFY;
    static const std::string INSTANCE_CLUSTER_IDENTIFY;
 
    static const std::string PRIVILEGE_IDENTIFY;

    static const std::string SCHEMA_IDENTIFY;
    static const std::string MAX_ID_SCHEMA_IDENTIFY;
    static const std::string NAMESPACE_SCHEMA_IDENTIFY;
    static const std::string DATABASE_SCHEMA_IDENTIFY;
    static const std::string TABLE_SCHEMA_IDENTIFY;
    static const std::string REGION_SCHEMA_IDENTIFY;
    static const std::string DDLWORK_IDENTIFY;
    static const std::string STATISTICS_IDENTIFY;
    
    static const std::string MAX_IDENTIFY;

    virtual ~MetaServer();
    
    static MetaServer* get_instance() {
        static MetaServer _instance;
        return &_instance;
    }

    int init(const std::vector<braft::PeerId>& peers); 

    //schema control method
    virtual void meta_manager(google::protobuf::RpcController* controller,
                               const pb::MetaManagerRequest* request,
                               pb::MetaManagerResponse* response,
                               google::protobuf::Closure* done);
    
    virtual void query(google::protobuf::RpcController* controller,
                       const pb::QueryRequest* request,
                       pb::QueryResponse* response,
                       google::protobuf::Closure* done);

    //raft control method
    virtual void raft_control(google::protobuf::RpcController* controller, 
                                     const pb::RaftControlRequest* request,
                                     pb::RaftControlResponse* response, 
                                     google::protobuf::Closure* done);
    
    virtual void store_heartbeat(google::protobuf::RpcController* controller,
                                 const pb::StoreHeartBeatRequest* request,
                                 pb::StoreHeartBeatResponse* response,
                                 google::protobuf::Closure* done); 

    virtual void baikal_heartbeat(google::protobuf::RpcController* controller,
                                  const pb::BaikalHeartBeatRequest* request,
                                  pb::BaikalHeartBeatResponse* response,
                                  google::protobuf::Closure* done); 

    virtual void console_heartbeat(google::protobuf::RpcController* controller,
                                  const pb::ConsoleHeartBeatRequest* request,
                                  pb::ConsoleHeartBeatResponse* response,
                                  google::protobuf::Closure* done); 

    virtual void migrate(google::protobuf::RpcController* controller,
            const pb::MigrateRequest* /*request*/,
            pb::MigrateResponse* response,
            google::protobuf::Closure* done);

    void flush_memtable_thread();
    void apply_region_thread();

    void shutdown_raft();
    bool have_data();
    void close() {
        _flush_bth.join();
        _apply_region_bth.join();
    }

private:
    MetaServerInteract* meta_proxy(const std::string& plat) {
        if (_meta_interact_map.count(plat) == 1) {
            return _meta_interact_map[plat];
        }
        return MetaServerInteract::get_instance();
    }
    MetaServer() {}
    
    MetaStateMachine* _meta_state_machine = NULL;
    AutoIncrStateMachine* _auto_incr_state_machine = NULL;
    std::map<std::string, MetaServerInteract*> _meta_interact_map;
    Bthread _flush_bth;
    //region区间修改等信息应用raft
    Bthread _apply_region_bth;
    bool _init_success = false;
    bool _shutdown = false;
}; //class

}//namespace baikaldb

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
