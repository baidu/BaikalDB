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

#include <unordered_map>
#include <bthread/mutex.h>
#include "proto/meta.interface.pb.h"
#include "meta_state_machine.h"
#include "meta_server.h"

namespace baikaldb{
class PrivilegeManager {
public:
    friend class QueryPrivilegeManager;
    static PrivilegeManager* get_instance() {
        static PrivilegeManager instance;
        return &instance;
    }
    ~PrivilegeManager() {
        bthread_mutex_destroy(&_user_mutex);
    }
    
    void process_user_privilege(google::protobuf::RpcController* controller,
                                const pb::MetaManagerRequest* request,
                                pb::MetaManagerResponse* response,
                                google::protobuf::Closure* done);
    
    void create_user(const pb::MetaManagerRequest& request, braft::Closure* done);
    void drop_user(const pb::MetaManagerRequest& request, braft::Closure* done);
    void modify_user(const pb::MetaManagerRequest& request, braft::Closure* done);
    void add_privilege(const pb::MetaManagerRequest& request, braft::Closure* done);
    void drop_privilege(const pb::MetaManagerRequest& request, braft::Closure* done);

    void process_baikal_heartbeat(const pb::BaikalHeartBeatRequest* request,
                                 pb::BaikalHeartBeatResponse* response);
    
    int load_snapshot();
    
    void set_meta_state_machine(MetaStateMachine* meta_state_machine) {
        _meta_state_machine = meta_state_machine;
    }
private:
    PrivilegeManager() {
        bthread_mutex_init(&_user_mutex, NULL);
    }
    std::string construct_privilege_key(const std::string& username) {
        return MetaServer::PRIVILEGE_IDENTIFY + username;
    }
    void insert_database_privilege(const pb::PrivilegeDatabase& privilege_database,
                                   pb::UserPrivilege& mem_privilege);
    void insert_table_privilege(const pb::PrivilegeTable& privilege_table,
                                pb::UserPrivilege& mem_privilege);
    void insert_bns(const std::string& bns, pb::UserPrivilege& mem_privilege);
    void insert_ip(const std::string& ip, pb::UserPrivilege& mem_privilege);

    void delete_database_privilege(const pb::PrivilegeDatabase& privilege_database,
                                   pb::UserPrivilege& mem_privilege);
    void delete_table_privilege(const pb::PrivilegeTable& privilege_table,
                                pb::UserPrivilege& mem_privilege);
    void delete_bns(const std::string& bns, pb::UserPrivilege& mem_privilege);
    void delete_ip(const std::string& ip, pb::UserPrivilege& mem_privilege);

    //username和privilege对应关系
    //std::mutex                                         _user_mutex;
    bthread_mutex_t                                      _user_mutex;
    std::unordered_map<std::string, pb::UserPrivilege> _user_privilege;
   
    MetaStateMachine* _meta_state_machine;
};//class
} //namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
