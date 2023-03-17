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

#include <sys/ioctl.h>
#include <signal.h>
#include <stdio.h>
#include <string>
#include <Configure.h>
#include <baidu/rpc/server.h>
#include <gflags/gflags.h>
#include "common.h"
#include "schema_factory.h"
#include "meta_server_interact.hpp"
#include "mut_table_key.h"

namespace baikaldb {
DEFINE_string(namespace_name, "FENGCHAO", "FENGCHAO");
DEFINE_string(database, "", "database");
DEFINE_string(table_name, "", "table_name");
DEFINE_string(resource_tag, "", "resouce_tag");
DEFINE_string(suffix, "_tmp", "_tmp");

int fun_level(std::map<int64_t, const pb::QueryRegion*>& region_map, const pb::QueryRegion& info) {
    if (info.parent() == 0) {
        return 0;
    } else {
        /*
        if (region_map.count(info.parent()) == 1) {
            return 1 + fun_level(region_map, *region_map[info.parent()]);
        }*/
        return 0;
    }
};

int create_table(const std::string& namespace_name, const std::string& database, 
                const std::string& table_name, const std::string& resource_tag,
                const std::string& suffix) {
    MetaServerInteract interact;
    if (interact.init() != 0) {
        DB_WARNING("init fail");
        return -1;
    }
    //先导出table的scheme_info信息
    pb::QueryRequest request;
    request.set_op_type(pb::QUERY_REGION_FLATTEN);
    request.set_namespace_name(namespace_name);
    request.set_database(database);
    request.set_table_name(table_name);
    
    pb::QueryResponse response;
    if (interact.send_request("query", request, response) != 0) {
        DB_WARNING("send_request fail");
        return -1;
    }
    DB_WARNING("region size:%d", response.flatten_regions_size());
    DB_WARNING("req:%s  \nres:%s", 
                request.DebugString().c_str(), 
                response.ShortDebugString().c_str());
    
    if (response.errcode() != pb::SUCCESS) {
        DB_WARNING("err:%s", response.errmsg().c_str());
        return -1;
    }
    
    std::map<int64_t, const pb::QueryRegion*> region_map;
    for (auto& info : response.flatten_regions()) {
        region_map[info.region_id()] = &info;
        int level = fun_level(region_map, info);
        std::string offset(level, ' ');
        std::cout << offset << info.region_id() << " " << info.parent() << "\t" 
        << info.start_key() << "\t" << info.end_key() << "\t" << info.create_time() << "\t" <<
        info.leader() << "\t" << info.peers() << "\t" << info.version() << "\n";
    }

    return 0;
}

} // namespace baikaldb

int main(int argc, char **argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    baikaldb::create_table(baikaldb::FLAGS_namespace_name, 
                           baikaldb::FLAGS_database, 
                           baikaldb::FLAGS_table_name, 
                           baikaldb::FLAGS_resource_tag, 
                           baikaldb::FLAGS_suffix);

    return 0;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
