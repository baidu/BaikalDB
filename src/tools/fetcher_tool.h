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

#include <net/if.h>
#include <sys/ioctl.h>
#include <signal.h>
#include <stdio.h>
#include <string>
#include <Configure.h>
#include <fstream>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <baidu/rpc/server.h>
#include <gflags/gflags.h>
#include <json/json.h>
#include "baikal_client.h"
#include "common.h"
#include "schema_factory.h"
#include "meta_server_interact.hpp"
#include "mut_table_key.h"
#include "importer_tool.h"
#include "backup_import.h"

namespace baikaldb { 
DECLARE_string(param_db);
class Fetcher {
public:
    Fetcher() {
    }
    ~Fetcher();
    int init(baikal::client::Service* baikaldb, baikal::client::Service* baikaldb_gbk, baikal::client::Service* baikaldb_utf8);
    int querybaikaldb(std::string sql, baikal::client::ResultSet& result_set);
    int import_to_baikaldb();
    void prepare();
    void get_cluster_info(const std::string& cluster_name);
    std::string replace_path_date(int64_t version);
    int analyse_one_do(const Json::Value& node, OpType type);
    int analyse_one_donefile();
    int first_deal();
    int first_deal_update();
    int deal_replace();
    int deal_update_before_oldversion();
    int deal_update_after_oldversion();
    int analyse_multi_donefile_update();
    int analyse_multi_donefile_replace();
    int analyse_version();
    int update_task(bool is_result_table);
    int fetch_task();
    int begin_task();
    int finish_task(bool is_succ, std::string& result, std::string& time_cost);
    int select_new_task(baikal::client::ResultSet& result_set);
    int update_task_doing(int64_t id, int64_t version);
    int update_task_idle(int64_t id);
    int exec_user_sql();
    std::string user_sql_replace(std::string &sql);
    int run();

private:
    //字符串需要用单引号''
    std::string gen_insert_sql(const std::string& table_name, const std::map<std::string, std::string>& values_map) {
        return _gen_insert_sql(FLAGS_param_db, table_name, values_map);
    }

    std::string gen_select_sql(const std::string& table_name, const std::vector<std::string>& select_vec, 
                            const std::map<std::string, std::string>& where_map) {
        return _gen_select_sql(FLAGS_param_db, table_name, select_vec, where_map);
    }

    std::string gen_update_sql(const std::string& table_name, const std::map<std::string, std::string>& set_map, 
                            const std::map<std::string, std::string>& where_map) {
        return _gen_update_sql(FLAGS_param_db, table_name, set_map, where_map);
    }

    std::string _done_file;
    std::string _done_common_path; //含有通配符
    std::string _done_name;
    std::string _done_real_path;//版本号替换之后的路径
    std::string _hdfs_mnt;
    std::string _hdfs_cluster;
    std::string _cluster_name;
    std::string _table_info;
    std::string _user_sql;
    std::string _charset;
    std::string _modle;//使用者给出是replace或update模式
    std::string _import_db;
    std::string _import_tbl;
    bool _only_one_donefile;
    uint64_t _result_id; 
    int _id;
    int _ago_days;
    int _interval_days;
    uint64_t _old_bitflag;
    uint64_t _new_bitflag;
    int64_t _today_version;
    int64_t _old_version;
    int64_t _new_version;
    int64_t _import_line;

    baikaldb::Importer _importer;
    
    baikal::client::Service* _baikaldb;
    baikal::client::Service* _baikaldb_gbk;
    baikal::client::Service* _baikaldb_utf8;
};
}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
