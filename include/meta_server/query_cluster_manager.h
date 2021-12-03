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

#include "cluster_manager.h"

namespace baikaldb {
class ClusterManager;
class QueryClusterManager {
public:
    ~QueryClusterManager() {}
    static QueryClusterManager* get_instance() {
        static QueryClusterManager instance;
        return &instance;
    }    
    void get_logical_info(const pb::QueryRequest* request, pb::QueryResponse* response);
    void get_physical_info(const pb::QueryRequest* request, pb::QueryResponse* response);
    void get_instance_info(const pb::QueryRequest* request, pb::QueryResponse* response);
    void get_instance_param(const pb::QueryRequest* request, pb::QueryResponse* response);
    void get_flatten_instance(const pb::QueryRequest* request, pb::QueryResponse* response);
    void get_diff_region_ids(const pb::QueryRequest* request, pb::QueryResponse* response);
    void get_region_ids(const pb::QueryRequest* request, pb::QueryResponse* response);
    void get_network_segment(const pb::QueryRequest* request, pb::QueryResponse* response);
    void process_console_heartbeat(const pb::ConsoleHeartBeatRequest* request,
            pb::ConsoleHeartBeatResponse* response);
private:
    void mem_instance_to_pb(const Instance& instance_mem, pb::InstanceInfo* instance_pb);
    void get_region_ids_per_instance(const std::string& instance, 
                                      std::set<int64_t>& region_ids);
    void get_region_count_per_instance(const std::string& instance,
                                        int64_t& count);
    void construct_query_response_for_instance(const Instance& instance_info, 
            std::map<std::string, std::multimap<std::string, pb::QueryInstance>>& logical_instance_infos);

    QueryClusterManager() {}
};

}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
