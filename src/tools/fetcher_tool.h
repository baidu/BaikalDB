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
#include "importer_handle.h"
#include "backup_import.h"
#include "backup_tool.h"

namespace baikaldb { 
DECLARE_string(param_db);
DECLARE_string(sql_agg_tbl);
struct RuleDesc {
    int64_t pv;
    int64_t avg;
};

typedef std::map<std::string, std::map<std::string, RuleDesc>>  AlarmRule;

class Fetcher {
public:
    Fetcher(baikal::client::Service* baikaldb, const std::map<std::string, baikal::client::Service*>& baikaldb_map) :
            _baikaldb(baikaldb), _baikaldb_map(baikaldb_map) {
        _fs = nullptr;
    }
    ~Fetcher();
    int init();
    int querybaikaldb(std::string sql, baikal::client::ResultSet& result_set);
    int import_to_baikaldb();
    void prepare();
    std::string replace_path_date(int64_t version);
    int analyse_one_do(const Json::Value& node);
    int analyse_one_donefile();
    int first_deal();
    int first_deal_update();
    int deal_replace();
    int deal_update_before_oldversion();
    int deal_update_after_oldversion();
    int analyse_multi_donefile_update();
    int analyse_multi_donefile_replace();
    int analyse_version();
    int update_main_task_idle(bool is_fast_importer);
    int update_result_task_fail();
    int fetch_task(bool is_fast_importer);
    int begin_task(std::string result = "");
    int finish_task(bool is_succ, std::string& result, std::string& time_cost);
    int select_new_task(baikal::client::ResultSet& result_set, bool is_fast_importer);
    int update_task_doing(int64_t id, int64_t version);
    int update_task_idle(int64_t id);
    int update_task_progress(const std::string& progress, int64_t import_line);
    int exec_user_sql();
    std::string user_sql_replace(std::string &sql);
    int run(bool is_fast_importer);
    std::string time_print(int64_t cost);
    static void shutdown() {
        _shutdown = true;
    }

    static bool is_shutdown() {
        return _shutdown;
    }

private:
    int importer(const Json::Value& node, OpType type, const std::string& done_path, const std::string& charset);
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

    int create_filesysterm(const std::string& cluster_name, const std::string& user_name, const std::string& password);

    void destroy_filesysterm();

    int get_done_file(const std::string& done_file_name, Json::Value& done_root);

    bool file_exist(const std::string& done_file_name);
    std::string _filesystem_path_prefix;
    std::string _done_file;
    std::string _done_common_path; //含有通配符
    std::string _done_name;
    std::string _done_real_path;//版本号替换之后的路径
    std::string _table_info;
    std::string _user_sql;
    std::string _charset;
    std::string _modle;//使用者给出是replace或update模式
    std::string _import_db;
    std::string _import_tbl;
    std::string _config;
    std::string _local_done_json;
    std::string _meta_bns;
    bool _is_local_done_json = false;
    bool _only_one_donefile = false;
    uint64_t _result_id = 0; 
    int _id = 0;
    int _ago_days = 0;
    int _interval_days = 0;
    uint64_t _old_bitflag = 0;
    uint64_t _new_bitflag = 0;
    int64_t _today_version = 0;
    int64_t _old_version = 0;
    int64_t _new_version = 0;
    int64_t _import_line = 0;
    bool _need_iconv = false;
    bool _is_fast_importer = false;
    std::string _table_namespace;
    int64_t _retry_times = 0;
    std::string _baikaldb_resource;
    std::string _cluster_name;
    std::string _user_name;
    std::string _password;
    
    baikal::client::Service* _baikaldb;
    baikal::client::Service* _baikaldb_user;
    const std::map<std::string, baikal::client::Service*>& _baikaldb_map;

    ImporterFileSystermAdaptor* _fs;
    static bool _shutdown;
    std::ostringstream _import_ret;
};

class SqlAgg {
public:
    SqlAgg(baikal::client::Service* s1, baikal::client::Service* s2) : _baikaldb_info(s1), _baikaldb_task(s2) {}

    void run() {
        _timer_bth.run([this]() { run_sql_agg(); });
    }

private:
    void run_sql_agg();
    int gen_agg_task();
    int reset_legacy_task();
    bool need_to_trigger(int64_t& date);
    bool agg_task_is_doing();
    int update_task_doing(int64_t id);
    int fetch_new_task();
    int replace_into_table(const std::vector<std::string>& insert_values);
    int exec(int64_t date, AlarmRule& rules);
    int update_task_status(int64_t id, std::string status);
    int alarm(int date, const AlarmRule& rule);
    void get_sample_sql(const std::string& sign, std::string* sample_sql);
    int send_mail(const char* to, const char *message);
    void gen_mail_message(const std::string& date_str, const int64_t& pv, const int64_t& avg, const std::string& db, const std::string& tbl, 
        const std::map<std::string, RuleDesc>& sign_rule, const std::set<std::string>& shield_signs, 
    std::ostringstream& os);

    std::string gen_select_sql(const std::vector<std::string>& select_vec, 
                            const std::map<std::string, std::string>& where_map) {
        return _gen_select_sql(FLAGS_param_db, FLAGS_sql_agg_tbl, select_vec, where_map);
    }

    std::string gen_update_sql(const std::map<std::string, std::string>& set_map, 
                            const std::map<std::string, std::string>& where_map) {
        return _gen_update_sql(FLAGS_param_db, FLAGS_sql_agg_tbl, set_map, where_map);
    }

    Bthread _timer_bth;
    baikal::client::Service* _baikaldb_info;
    baikal::client::Service* _baikaldb_task;
};

}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
