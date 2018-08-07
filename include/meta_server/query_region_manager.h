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

#include "region_manager.h"

namespace baikaldb {
class QueryRegionManager {
public:
    ~QueryRegionManager() {}
    static QueryRegionManager* get_instance() {
        static QueryRegionManager instance;
        return &instance;
    }
    void get_flatten_region(const pb::QueryRequest* request, pb::QueryResponse* response);
    void get_region_info(const pb::QueryRequest* request, pb::QueryResponse* response);

    void get_region_info_per_instance(const std::string& instance, pb::QueryResponse* response);
    void get_region_count_per_instance(
                    const std::string& instance,
                    int64_t& region_count,
                    int64_t& region_leader_count);
    
    void send_transfer_leader(const pb::QueryRequest* request, pb::QueryResponse* response);
    void send_set_peer(const pb::QueryRequest* request, pb::QueryResponse* response);
    void send_remove_region_request(std::string instance_address, int64_t region_id);
private:
    QueryRegionManager() {}
}; //class

}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
