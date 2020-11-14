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

#include "privilege_manager.h"

namespace baikaldb{
class QueryPrivilegeManager {
public:
    static QueryPrivilegeManager* get_instance() {
        static QueryPrivilegeManager instance;
        return &instance;
    }
    ~QueryPrivilegeManager() {}
    
    void get_user_info(const pb::QueryRequest* request,
                       pb::QueryResponse* response);
    void get_flatten_privilege(const pb::QueryRequest* request, pb::QueryResponse* response);
    void process_console_heartbeat(const pb::ConsoleHeartBeatRequest* request,
                                 pb::ConsoleHeartBeatResponse* response);
private:
    QueryPrivilegeManager() {}
    void construct_query_response_for_privilege(const pb::UserPrivilege& user_privilege,
            std::map<std::string, std::multimap<std::string, pb::QueryUserPrivilege>>& namespace_privileges);
};
} //namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
