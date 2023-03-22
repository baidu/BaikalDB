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

#include <functional>
#include "backup_tool.h"
#include "backup_import.h"
using namespace boost::algorithm;
namespace baikaldb {

DECLARE_string(hostname);

int64_t get_ndays_date(int64_t date, int n)
{
    struct tm ptm;
    ptm.tm_year = date / 10000 % 10000 - 1900;
    ptm.tm_mon  = date / 100 % 100 - 1;
    ptm.tm_mday = date % 100;
    ptm.tm_hour = 0;
    ptm.tm_min  = 0;
    ptm.tm_sec  = 0;
    ptm.tm_isdst = 0;
                         
    time_t timep;
    timep = mktime(&ptm); //mktime把struct tm类型转换成time_t
    timep += n * 24 * 60 * 60;
                                          
    localtime_r(&timep, &ptm); //localtime把time_t类型转换成struct tm
                                               
    int64_t l_time = (ptm.tm_year + 1900) * 10000 + (ptm.tm_mon + 1) * 100 + ptm.tm_mday;
    return l_time;
}

//将当天的日期转换成int型返回
int64_t get_today_date() {
    struct tm ptm; 
    time_t timep = time(NULL); 
    localtime_r(&timep, &ptm); 

    int64_t l_time = (ptm.tm_year + 1900) * 10000 + (ptm.tm_mon + 1) * 100 + ptm.tm_mday;
    
    return l_time;
}

//字符串需要用单引号''
std::string _gen_insert_sql(const std::string& database_name, const std::string& table_name, 
                        const std::map<std::string, std::string>& values_map) {

    std::string sql = "REPLACE INTO " + database_name + "." + table_name;
    std::string names;
    std::string values;

    for (auto iter : values_map) {
        names += iter.first;
        names += ",";
        values += iter.second;
        values += ",";
    }

    names.pop_back();
    values.pop_back();

    sql += " (" + names + ") VALUES (" + values + ")";

    return sql;
}

std::string _gen_select_sql(const std::string& database_name, const std::string& table_name, 
                        const std::vector<std::string>& select_vec, const std::map<std::string, std::string>& where_map) {
    std::string sql = "SELECT ";
    for (auto name : select_vec) {
        sql += name;
        sql += ",";
    }
    sql.pop_back();
    sql += " FROM " + database_name + "." + table_name + " WHERE ";

    std::vector<std::string> where_vec;
    for (auto iter : where_map) {
        where_vec.push_back(iter.first + "=" + iter.second);
    }
    sql += join(where_vec, " AND ");

    return sql;
}

std::string _gen_update_sql(const std::string& database_name, const std::string& table_name, 
                        const std::map<std::string, std::string>& set_map, const std::map<std::string, std::string>& where_map) {
    std::string sql = "UPDATE " + database_name + "." + table_name;
    sql += " SET ";
    std::vector<std::string> set_vec;
    for (auto iter : set_map) {
        set_vec.push_back(iter.first + "=" + iter.second);
    }
    sql += join(set_vec, ", ");
    sql += " WHERE ";
    std::vector<std::string> where_vec;
    for (auto iter : where_map) {
        where_vec.push_back(iter.first + "=" + iter.second);
    }
    sql += join(where_vec, " AND ");

    return sql;
}

std::string _gen_delete_sql(const std::string& database_name, const std::string& table_name, 
                        const std::map<std::string, std::string>& where_map) {
    std::string sql = "DELETE from " + database_name + "." + table_name;
    sql += " WHERE ";
    std::vector<std::string> where_vec;
    where_vec.reserve(5);
    for (auto iter : where_map) {
        where_vec.emplace_back(iter.first + "=" + iter.second);
    }
    sql += join(where_vec, " AND ");
    DB_WARNING("delete sql: %s", sql.c_str());
    return sql;
}


bool BackUpImport::need_to_trigger(int64_t date, int64_t hour, int64_t interval_days, int64_t& now_date, int64_t table_id) {

    struct tm local;                  
    time_t timep;

    timep = time(NULL);                         
    localtime_r(&timep, &local); //localtime把time_t类型转换成struct tm
                                               
    int64_t l_date = (local.tm_year + 1900) * 10000 + (local.tm_mon + 1) * 100 + local.tm_mday;

    int64_t ndays_date = get_ndays_date(l_date, -interval_days);

    if ((ndays_date >= date) && (hour == local.tm_hour)) {
        now_date = l_date;
        DB_NOTICE("need_to_trigger date_%ld, hour_%ld, inter_day_%ld, now_%ld table_id_%ld ndays_date%ld local_hour_%d", 
            date, hour, interval_days, now_date, table_id, ndays_date, local.tm_hour);
        DB_NOTICE("table_id_%ld trigger ok", table_id);
        return true;
    }

    DB_NOTICE("table_id_%ld trigger fail", table_id);
    return false;
}

int BackUpImport::insert_backup_task(const std::string& database_name, 
                                     const std::string& table_name, 
                                     const int64_t table_id, 
                                     const std::string& meta_server_bns, 
                                     int64_t date, 
                                     const std::string& pefered_peer_resource_tag,
                                     int64_t interval_days,
                                     int64_t backup_times) {
    
    baikal::client::ResultSet result_set;
    std::map<std::string, std::string> values_map;

    values_map["`database`"] = "'" + database_name + "'";
    values_map["table_name"] = "'" + table_name + "'";
    values_map["table_id"] = std::to_string(table_id);
    values_map["meta_server_bns"] = "'" + meta_server_bns + "'";
    values_map["status"] = "'idle'";
    values_map["date"] = std::to_string(date);
    values_map["resource_tag"] = "'" + pefered_peer_resource_tag + "'";
    values_map["interval_days"] = std::to_string(interval_days);
    values_map["backup_times"] = std::to_string(backup_times);

    // 需要用insert，避免多个dm都tricker，导致两个DM同时做一个SST备份任务
    std::string sql = "INSERT INTO " + _database + "." + _backup_task_table;
    std::string names;
    std::string values;

    for (auto iter : values_map) {
        names += iter.first;
        names += ",";
        values += iter.second;
        values += ",";
    }

    names.pop_back();
    values.pop_back();

    sql += " (" + names + ") VALUES (" + values + ")";

    int ret = querybaikaldb(sql, result_set);
    if (ret != 0) {
        DB_FATAL("query baikaldb failed, sql:%s", sql.c_str()); 
        return -1;
    }

    DB_WARNING("insert_backup_task, sql:%s", sql.c_str());

    return 0;
}

int BackUpImport::table_is_doing_or_idle(std::set<int64_t>& table_ids) {
    int affected_rows = 0;
    baikal::client::ResultSet result_set;

    std::string sql = "SELECT table_id FROM " + _database + "." + _backup_task_table + " WHERE status='doing' OR status='idle' GROUP BY table_id";

    int ret = querybaikaldb(sql, result_set);
    if (ret != 0) {
        DB_WARNING("select table_id failed, sql:%s", sql.c_str()); 
        return -1;
    }

    affected_rows = result_set.get_row_count();
    if (affected_rows == 0) {
        return 0;
    }

    while (result_set.next()) {
        int64_t table_id = 0;
        result_set.get_int64("table_id", &table_id);
        table_ids.insert(table_id);
    }

    return 0; 

}

int BackUpImport::gen_backup_task() {

    int affected_rows = 0;
    baikal::client::ResultSet result_set;
    std::vector<std::string> select_vec;
    std::map<std::string, std::string> where_map;

    select_vec.push_back("*");
    where_map["status"] = "'idle'";
    std::string sql = gen_select_sql(_backup_info_table, select_vec, where_map);

    int ret = querybaikaldb(sql, result_set);
    if (ret != 0) {
        DB_WARNING("select new task failed, sql:%s", sql.c_str()); 
        return -1;
    }
    
    affected_rows = result_set.get_row_count();
    if (affected_rows == 0) {
        return -2;
    }
    DB_NOTICE("gen_backup_task get row count %d", affected_rows);

    std::set<int64_t> table_ids;
    ret = table_is_doing_or_idle(table_ids);
    if (ret != 0) {
        DB_WARNING("get table is doing or idle error.");
        return -1;
    }

    while (result_set.next()) {

        int64_t id  = 0;
        int64_t table_id = 0;
        std::string meta_server_bns;
        std::string database_name;
        std::string table_name;
        std::string pefered_peer_resource_tag; // 优先拉取该集群的peer的sst
        int64_t date = 0;
        int64_t hour = 0;
        int64_t interval_days = 0;
        int64_t backup_times = 0;
        int64_t now_date = 0;

        result_set.get_string("meta_server_bns", &meta_server_bns);
        result_set.get_string("database_name", &database_name);
        result_set.get_string("table_name", &table_name);
        result_set.get_int64("id", &id);
        result_set.get_int64("table_id", &table_id);
        result_set.get_int64("date", &date);
        result_set.get_int64("hour", &hour);
        result_set.get_int64("interval_days", &interval_days);
        result_set.get_int64("backup_times", &backup_times);
        result_set.get_string("resource_tag", &pefered_peer_resource_tag);

        //DB_NOTICE("try to tigger table_id_%ld", table_id);
        if (need_to_trigger(date, hour, interval_days, now_date, table_id) == false) {
            DB_NOTICE("table_id_%ld trigger false", table_id);
            continue;
        }

        if (table_ids.count(table_id) != 0) {
            DB_WARNING("table_id:%ld is doing or idle", table_id);
            continue;
        }

        ret = insert_backup_task(database_name, table_name, table_id, meta_server_bns, 
                                 now_date, pefered_peer_resource_tag, interval_days, backup_times);
        if (ret != 0) {
            DB_NOTICE("table_id_%ld insert backup task fail", table_id);
            continue;
        }

        std::map<std::string, std::string> set_map;
        std::map<std::string, std::string> where_map;
        set_map["date"]   = std::to_string(now_date);
        where_map["id"] = std::to_string(id);

        std::string sql = gen_update_sql(_backup_info_table, set_map, where_map);

        baikal::client::ResultSet tmp_result_set;
        ret = querybaikaldb(sql, tmp_result_set);
        if (ret != 0) {
            DB_FATAL("query baikaldb failed, sql:%s", sql.c_str());
            continue;
        }
        DB_NOTICE("table_id %ld trigger success", table_id);
    }

    return 0;   
}

//处理历史遗留任务，可能由于core导致进程退出
int BackUpImport::reset_legacy_task() {
    int ret = 0;
    int affected_rows = 0;
    std::vector<int64_t> ids;
    baikal::client::ResultSet result_set;
    std::vector<std::string> select_vec;
    std::map<std::string, std::string> where_map;
    select_vec.push_back("id");
    where_map["hostname"] = "'" + FLAGS_hostname + "'";
    where_map["status"] = "'doing'";
    std::string sql = gen_select_sql(_backup_task_table, select_vec, where_map);

    ret = querybaikaldb(sql, result_set);
    if (ret != 0) {
        DB_FATAL("query baikaldb failed");
        return -1;
    }
    affected_rows = result_set.get_row_count();
    if (affected_rows == 0) {
        return 0;
    }
    int64_t id = 0;
    while (result_set.next()) {
        result_set.get_int64("id", &id);
        ids.push_back(id);
    }

    for (auto& id : ids) {
        std::map<std::string, std::string> set_map;
        std::map<std::string, std::string> where_map;
        // 状态从doing置回idle，重做备份任务
        set_map["status"]   = "'idle'";
        set_map["end_time"]   = "now()";
        where_map["status"] = "'doing'";
        where_map["id"] = std::to_string(id);
        std::string sql = gen_update_sql(_backup_task_table, set_map, where_map);
        ret = querybaikaldb(sql, result_set);
        if (ret != 0) {
            DB_FATAL("query baikaldb failed, sql:%s", sql.c_str());
            continue;
        }

        DB_WARNING("update task query baikaldb succ, sql:%s", sql.c_str());
    }

    return 0;
}

int BackUpImport::update_task_finish(const std::string& result, int64_t id) {

    baikal::client::ResultSet result_set;
    std::map<std::string, std::string> set_map;
    std::map<std::string, std::string> where_map;

    set_map["status"] = "'" + result + "'";
    set_map["end_time"]   = "now()";
    where_map["id"] = std::to_string(id);
    std::string sql = gen_update_sql(_backup_task_table, set_map, where_map);
    querybaikaldb(sql, result_set);

    DB_WARNING("finsh_task:%s", sql.c_str());

    return 0;
}

int BackUpImport::update_task_doing(int64_t id) {

    baikal::client::ResultSet result_set;
    std::map<std::string, std::string> set_map;
    std::map<std::string, std::string> where_map;

    set_map["status"]    = "'doing'";
    set_map["hostname"]  = "'" + FLAGS_hostname + "'";
    set_map["start_time"] = "now()";
    where_map["id"] = std::to_string(id);
    where_map["status"]  = "'idle'";
    std::string sql = gen_update_sql(_backup_task_table, set_map, where_map);

    int ret = querybaikaldb(sql, result_set);
    if (ret != 0) {
        DB_FATAL("query baikaldb failed, sql:%s", sql.c_str()); 
        return -1;
    }

    int affected_rows = result_set.get_affected_rows();
    if (affected_rows != 1 ) {
        DB_FATAL("update taskid doing affected row:%d, sql:%s", affected_rows, sql.c_str());
        return -1;
    }

    DB_WARNING("update taskid doing, sql:%s", sql.c_str());

    return 0;
}

int BackUpImport::fetch_new_task() {

    int affected_rows = 0;
    baikal::client::ResultSet result_set;
    std::vector<std::string> select_vec;
    std::map<std::string, std::string> where_map;

    select_vec.push_back("*");
    where_map["status"] = "'idle'";
    std::string sql = gen_select_sql(_backup_task_table, select_vec, where_map);

    int ret = querybaikaldb(sql, result_set);
    if (ret != 0) {
        DB_WARNING("select new task failed, sql:%s", sql.c_str()); 
        return -1;
    }
    
    affected_rows = result_set.get_row_count();
    if (affected_rows == 0) {
        return -2;
    }

    while (result_set.next()) {

        int64_t id  = 0;
        int64_t table_id = 0;
        int64_t interval_days = 0;
        int64_t backup_times = 0;
        std::string meta_server_bns;
        std::string status;
        std::string resource_tag;

        result_set.get_string("status", &status);
        result_set.get_string("meta_server_bns", &meta_server_bns);
        result_set.get_int64("id", &id);
        result_set.get_int64("table_id", &table_id);
        result_set.get_string("resource_tag", &resource_tag);
        result_set.get_int64("interval_days", &interval_days);
        result_set.get_int64("backup_times", &backup_times);

        if (backup_times <= 0) {
            backup_times = 3;
        }
        if (update_task_doing(id) < 0) {
            continue;
        }

        baikaldb::BackUp bp(meta_server_bns, resource_tag, interval_days, backup_times);
        std::unordered_set<int64_t> table_ids;
        table_ids.insert(table_id);

        int ret = bp.run_backup(table_ids, std::bind(&baikaldb::BackUp::backup_region_streaming_with_retry, &bp, 
            std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4));
        if (ret == 0) {
            update_task_finish("successful", id);
        } else if (ret == -1) {
            update_task_finish("query meta failed", id);
        } else if (ret > 0) {
            std::string desc = std::to_string(ret) + " region failed";
            update_task_finish(desc, id);
        }

        break;
    }

    return 0;
}

void BackUpImport::run() {

    // 清理遗留任务
    reset_legacy_task();
    // 执行新任务
    Bthread first_fetch_worker {&BTHREAD_ATTR_NORMAL};
    Bthread second_fetch_worker {&BTHREAD_ATTR_NORMAL};
    first_fetch_worker.run([this](){fetch_new_task();});
    second_fetch_worker.run([this](){
        bthread_usleep(5000 * 1000);
        fetch_new_task();});
    first_fetch_worker.join();
    second_fetch_worker.join();
}
} // namespace baikaldb
