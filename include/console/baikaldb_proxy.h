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

#include "proto/meta.interface.pb.h"
#include "proto/console.pb.h"
#include "baikal_client.h"

namespace baikaldb {

typedef std::shared_ptr<pb::WatchRegionInfo> SmartWatchRegionInfo;

struct RegionStat {
    int64_t region_id;
    int64_t used_size;
    int64_t num_table_lines;
};

class BaikalProxy {

public:
    virtual ~BaikalProxy() {}

    static BaikalProxy* get_instance() {
        static BaikalProxy _instance;
        return &_instance;
    }

    int init(std::vector<std::string>& platform_tags);
    int query(std::string sql, baikal::client::ResultSet& result_set);
    int query_import(std::string sql, baikal::client::ResultSet& result_set);
    void truncate_table(const std::string& database_name, std::string table_name);
    int get_overview_info(const pb::QueryParam * param, const pb::ConsoleRequest* request,
                          pb::ConsoleResponse* response);
    int get_instance_info(const pb::QueryParam * param, const pb::ConsoleRequest* request, 
                          pb::ConsoleResponse* response);
    int get_user_info(const pb::QueryParam * param, const pb::ConsoleRequest* request,
                      pb::ConsoleResponse* response);
    int get_region_info(pb::QueryParam * param, const pb::ConsoleRequest* request, 
                       pb::ConsoleResponse* response);
    int get_table_info(const pb::QueryParam * param, const pb::ConsoleRequest* request, 
                       pb::ConsoleResponse* response);
    void insert_instance_info(const std::string& database_name, const std::vector<pb::QueryInstance>& instance_infos);
    void insert_user_info(const std::string& database_name, const std::vector<pb::QueryUserPrivilege>& user_infos);
    void insert_region_info(const std::string& database_name, const std::vector<pb::QueryRegion>& region_infos);
    void update_region_info(const std::string& database_name, const RegionStat& region_stat);
    void delete_table_and_region(const std::string& database_name, const int64_t table_id);
    void insert_table_info(const std::string& database_name, std::vector<pb::QueryTable>& table_infos);
    void update_table_count(const std::string& database_name, const int64_t table_id, const int64_t region_count);
    int get_platform_info(const pb::QueryParam * param, const pb::ConsoleRequest* request,
                        pb::ConsoleResponse* response);
    int get_namespace_info(const pb::QueryParam * param, const pb::ConsoleRequest* request,
                  pb::ConsoleResponse* response);
    int get_cluster_info(const pb::QueryParam * param, const pb::ConsoleRequest* request,
                  pb::ConsoleResponse* response);
    int get_database_info(const pb::QueryParam * param, const pb::ConsoleRequest* request,
                  pb::ConsoleResponse* response);
    int get_task_list_by_database(pb::QueryParam * param, const pb::ConsoleRequest* request,
                  pb::ConsoleResponse* response);
    int get_import_task2(pb::QueryParam * param, const pb::ConsoleRequest* request,
                  pb::ConsoleResponse* response);
    int get_import_task(pb::QueryParam * param, const pb::ConsoleRequest* request,
                  pb::ConsoleResponse* response);
    int submit_import_task(pb::QueryParam * param, const pb::ConsoleRequest* request,
                  pb::ConsoleResponse* response);
    int get_full_table_name(const pb::QueryParam * param, std::string& full_table_name);
    int get_sst_backup_task(pb::QueryParam * param, const pb::ConsoleRequest* request,
             pb::ConsoleResponse* response);
    int submit_sst_backup_task(pb::QueryParam * param, const pb::ConsoleRequest* request,
             pb::ConsoleResponse* response);


private:
    void ctor_select_region_sql(const pb::QueryParam * param, std::string& sql);
    void ctor_select_instance_sql(const pb::QueryParam * param, std::string& sql);
    int get_tableid_and_raw_start_key(pb::QueryParam * param);
    std::string mysql_escape_string(const std::string& value); 
    void ctor_region_info(baikal::client::ResultSet& result_set,
                         std::vector<SmartWatchRegionInfo>& region_infos);    
    int search_region_info(pb::QueryParam * param, const pb::ConsoleRequest* request,
                  pb::ConsoleResponse* response);
    int normal_region_info(const pb::QueryParam * param, const pb::ConsoleRequest* request,
                  pb::ConsoleResponse* response);
    int instance_region_info(const pb::QueryParam * param, const pb::ConsoleRequest* request,
                  pb::ConsoleResponse* response);                
    int database_usage_info(const pb::QueryParam * param, const pb::ConsoleRequest* request,
                  pb::ConsoleResponse* response);
    int cluster_usage_info(const pb::QueryParam * param, const pb::ConsoleRequest* request,
                  pb::ConsoleResponse* response);
    std::string transform_usage_info(int64_t used_size, int64_t capacity, bool from_region);
    
    baikal::client::Manager _manager;
    baikal::client::Service* _baikaldb;
    baikal::client::Service* _baikaldb_import;
    int64_t  _response_info_id = 1; 

public:
    std::map<std::string, std::string> plat_databases_mapping;
};

}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */