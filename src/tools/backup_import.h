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
#include <atomic>
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

namespace baikaldb {
DECLARE_string(param_db);
DECLARE_string(sst_backup_tbl);
DECLARE_string(sst_backup_info_tbl);

extern int64_t get_ndays_date(int64_t date, int n);
extern int64_t get_today_date();

//字符串需要用单引号''
extern std::string _gen_insert_sql(const std::string& database_name, const std::string& table_name, 
                        const std::map<std::string, std::string>& values_map);

extern std::string _gen_select_sql(const std::string& database_name, const std::string& table_name, 
                        const std::vector<std::string>& select_vec, const std::map<std::string, std::string>& where_map);

extern std::string _gen_update_sql(const std::string& database_name, const std::string& table_name, 
                        const std::map<std::string, std::string>& set_map, const std::map<std::string, std::string>& where_map);
extern std::string _gen_delete_sql(const std::string& database_name, const std::string& table_name, 
                        const std::map<std::string, std::string>& where_map);
class BackUpImport {
public:
    BackUpImport(baikal::client::Service* baikaldb_) {
        _baikaldb = baikaldb_;
        _database = FLAGS_param_db;
        _backup_task_table = FLAGS_sst_backup_tbl;
        _backup_info_table = FLAGS_sst_backup_info_tbl;
    }

    ~BackUpImport() {}

    bool need_to_trigger(int64_t date, int64_t hour, int64_t interval_days, int64_t& now_date, int64_t table_id);
    int insert_backup_task(const std::string& database_name, 
                           const std::string& table_name, 
                           const int64_t table_id, 
                           const std::string& meta_server_bns, 
                           int64_t date,
                           const std::string& pefered_peer_resource_tag,
                           int64_t interval_days,
                           int64_t backup_times);
    int gen_backup_task();

    int reset_legacy_task();
    int update_task_finish(const std::string& result, int64_t id);
    int update_task_doing(int64_t id);
    int fetch_new_task();
    void run();

private:
    int table_is_doing_or_idle(std::set<int64_t>& table_ids);
    
    //字符串需要用单引号''
    std::string gen_insert_sql(const std::string& table_name, const std::map<std::string, std::string>& values_map) {
        return _gen_insert_sql(_database, table_name, values_map);
    }

    std::string gen_select_sql(const std::string& table_name, const std::vector<std::string>& select_vec, 
                            const std::map<std::string, std::string>& where_map) {
        return _gen_select_sql(_database, table_name, select_vec, where_map);
    }

    std::string gen_update_sql(const std::string& table_name, const std::map<std::string, std::string>& set_map, 
                            const std::map<std::string, std::string>& where_map) {
        return _gen_update_sql(_database, table_name, set_map, where_map);
    }

    int querybaikaldb(std::string sql, baikal::client::ResultSet& result_set) {
        int ret = 0;
        int retry = 0;
        //DB_NOTICE("insert sql %s", sql.c_str());
        do {
            ret = _baikaldb->query(0, sql, &result_set);
            if (ret == 0) {
                break;
            }
            //DB_NOTICE("retry %d insert sql[%s]", retry, sql.c_str());
            bthread_usleep(1000000);
        } while (++retry < 20);

        //DB_NOTICE("insert end sql[%s]", sql.c_str());
        if (ret != 0) {
            DB_FATAL("sql_len:%lu query fail : %s", sql.size(), sql.c_str());
            return ret;
        }
        DB_NOTICE("query %d times, sql:%s affected row:%lu", retry, sql.c_str(), result_set.get_affected_rows());
        return 0;
    }

    baikal::client::Service* _baikaldb;
    std::string _database;
    std::string _backup_task_table;
    std::string _backup_info_table;
};
}
