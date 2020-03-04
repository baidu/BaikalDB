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

#include "baikaldb_proxy.h"
#include "common.h"

#include <boost/algorithm/string.hpp>

#include <cstring>
#include <cstdio>

namespace baikaldb {
DEFINE_string(param_db, "HDFS_TASK_DB", "param database");
DEFINE_string(param_tbl, "HDFS_TASK_TABLE", "param table");
DEFINE_string(result_db, "HDFS_TASK_DB", "result info database");
DEFINE_string(result_tbl, "HDFS_TASK_RESULT_TABLE", "result info table");
DEFINE_string(sst_backup_db, "HDFS_TASK_DB", "sst backup info database");
DEFINE_string(sst_backup_tbl, "SST_BACKUP_TABLE", "sst backup info table");

int BaikalProxy::init(std::vector<std::string>& platform_tags) {
    int rc = 0;
    rc = _manager.init("conf", "baikal_client.conf");
    if (rc != 0) {
        DB_FATAL("baikal client init fail:%d", rc);
        return -1;
    }
    _baikaldb = _manager.get_service("baikaldb");
    if (_baikaldb == NULL) {
        DB_FATAL("baikaldb is null");
        return -1;
    }
    _baikaldb_import = _manager.get_service("baikaldb_import");
    if (_baikaldb == NULL) {
        DB_FATAL("baikaldb_import is null");
        return -1;
    }

    for (auto& tag : platform_tags) {
        plat_databases_mapping[tag] = "CLUSTER_STATUS_" + tag;
        DB_WARNING("platform_tag: %s database: %s", tag.c_str(), plat_databases_mapping[tag].c_str());
    }
    return 0;
}

int BaikalProxy::query(std::string sql, baikal::client::ResultSet& result_set) {
    TimeCost cost;
    int ret = 0;
    int retry = 0;
    do {
        ret = _baikaldb->query(0, sql, &result_set);
        if (ret == 0) {
            break;
        }
        bthread_usleep(1000000);
    }while (++retry < 20);

    if (ret != 0) {
        DB_FATAL("sql_len:%d query fail : %s", sql.size(), sql.c_str());
        sql += ";\n";
        return -1;
    }
    DB_NOTICE("affected_rows:%d, cost:%ld, sql_len:%d, sql:%s",
                result_set.get_affected_rows(), cost.get_time(), sql.size(), sql.c_str());
    return 0;
}

int BaikalProxy::query_import(std::string sql, baikal::client::ResultSet& result_set) {
    TimeCost cost;
    int ret = 0;
    int retry = 0;
    do {
        ret = _baikaldb_import->query(0, sql, &result_set);
        if (ret == 0) {
            break;
        }
        bthread_usleep(1000000);
    }while (++retry < 20);

    if (ret != 0) {
        DB_FATAL("sql_len:%d query fail : %s", sql.size(), sql.c_str());
        sql += ";\n";
        return -1;
    }
    DB_NOTICE("affected_rows:%d, cost:%ld, sql_len:%d, sql:%s",
                result_set.get_affected_rows(), cost.get_time(), sql.size(), sql.c_str());
    return 0;
}

std::string BaikalProxy::mysql_escape_string(const std::string& value) {
    char* str = new char[value.size() * 2 + 1];
    std::string escape_value;
    baikal::client::SmartConnection connection = _baikaldb->fetch_connection();
    if (connection) {
        MYSQL* RES = connection->get_mysql_handle();
        mysql_real_escape_string(RES, str, value.c_str(), value.size());
        escape_value = str;
        connection->close();
    } else {
        DB_WARNING("service fetch_connection() failed");
        delete[] str;
        return boost::replace_all_copy(value, "'", "\\'");
    }   
    delete[] str;
    return escape_value;
}

void BaikalProxy::truncate_table(const std::string& database_name, std::string table_name) {
    baikal::client::ResultSet result_set;
    int ret = 0;
    std::string sql = "TRUNCATE TABLE "+ database_name + "." + table_name;
    ret = query(sql, result_set);
    if (ret < 0) {
        DB_WARNING("truncate table sql:%s failed", sql.c_str());
    }
}

void BaikalProxy::insert_user_info(const std::string& database_name, const
    std::vector<pb::QueryUserPrivilege>& user_infos) {
    baikal::client::ResultSet result_set;
    int ret = 0;

    std::string sql = "REPLACE INTO " + database_name + ".user (`namespace`, "
        "`username`, `password`, `table_name`, `permission`) VALUES ";
    sql.reserve(4096);
    
    std::string values;
    values.reserve(4096);
    for (auto info : user_infos) {
        values += "('" + info.namespace_name() + "',";
        values += "'" + info.username() + "',";
        values += "'" + mysql_escape_string(info.password()) + "',";
        values += "'" + info.privilege() + "',";
        values += std::to_string(info.table_rw()) + "),";
    }
    values.pop_back();
    sql += values;

    ret = query(sql, result_set);
    if (ret < 0) {
        DB_WARNING("insert sql:%s failed", sql.c_str());
    }
}

std::string BaikalProxy::transform_usage_info(int64_t used_size, int64_t capacity,
                     bool from_region) {

    std::string usage = "";
    char buf[512] = {0};
    if (from_region) {
        if (used_size > 1099511627776) { // T
            sprintf(buf, "%.2fTB", used_size / 1099511627776.0);
        } else if (used_size > 1073741824){
            sprintf(buf, "%2.fGB", used_size / 1073741824.0);
        } else if (used_size > 1048576){
            sprintf(buf, "%2.fMB", used_size / 1048576.0);
        } else {
            sprintf(buf, "%ldB", used_size);
        }
    } else {
        if (used_size > 1024) { // T
            sprintf(buf, "%.2fT", used_size / 1024.0);
        } else {
            sprintf(buf, "%ldG", used_size);
        }

    }

    usage += std::string(buf);
    memset(buf, 0, sizeof(buf));

    if (capacity > 0) {
        usage += "/";
        if (capacity > 1024) {
            sprintf(buf, "%.2fT", capacity / 1024.0);
        } else {
            sprintf(buf, "%ldG", capacity);
        }
        usage += std::string(buf);
    }

    return usage;
}

int BaikalProxy::database_usage_info(const pb::QueryParam * param, const pb::ConsoleRequest* request,
                  pb::ConsoleResponse* response) {
    baikal::client::ResultSet result_set;
    int ret = 0;
    std::vector<int64_t> table_ids;
    std:: string table_id_sql = "select table_id from " + param->crud_database() + ".table_info where namespace_name='";
    table_id_sql += param->namespace_name() + "' ";
    if (param->has_database()) {
        table_id_sql += "AND `database`='" + param->database() + "'";
    }

    ret = query(table_id_sql, result_set);
    if (ret < 0) {
        DB_WARNING("sql:%s failed", table_id_sql.c_str());
        return -1;
    }

    int64_t table_id;
    while (result_set.next()) {
        result_set.get_int64("table_id", &table_id);
        table_ids.push_back(table_id);
    }

    std::string sql = "SELECT sum(used_size) AS used_size FROM " + param->crud_database() + ".region";
    if (table_ids.size() > 0) {
        sql += " WHERE table_id in (";
        for (auto& id : table_ids) {
                sql += std::to_string(id) + ",";
        }
        sql += "-1)";
    }
    ret = query(sql, result_set);
    if (ret < 0) {
        DB_WARNING("sql:%s failed", sql.c_str());
        return -1;
    }

    int64_t used_size = 0;
    while (result_set.next()) {
        result_set.get_int64("used_size", &used_size);
    }
    
    sql = "SELECT sum(region_count) AS region_count, sum(row_count) AS row_count FROM "
        + param->crud_database() + ".table_info";
    if (param->has_namespace_name()) {
        sql += " WHERE namespace_name='" + param->namespace_name() + "'";
    }
    if (param->has_database()) {
        sql += " AND `database`='" + param->database() + "'";
    }

    ret = query(sql, result_set);
    if (ret < 0) {
        DB_WARNING("sql:%s failed", sql.c_str());
        return -1;
    }

    int64_t region_count = 0;
    int64_t row_count = 0;
    while (result_set.next()) {
        result_set.get_int64("region_count", &region_count);
        result_set.get_int64("row_count", &row_count);
    }
    auto overview_info = response->mutable_overview(); 
    auto replications_status = overview_info->mutable_replications_status();
    replications_status->set_row_count(row_count);
    replications_status->set_region_count(region_count);
    overview_info->set_usage(transform_usage_info(used_size, 0, true));
    return 0;
}

int BaikalProxy::cluster_usage_info(const pb::QueryParam * param, const pb::ConsoleRequest* request,
                  pb::ConsoleResponse* response) {
    baikal::client::ResultSet result_set;
    int ret = 0;
    int32_t status = 0;
    int64_t status_stat = 0;
    int64_t used_size = 0;
    int64_t capacity = 0;

    std::string sql = "SELECT sum(used_size) AS used_size, sum(capacity) AS capacity FROM " 
        + param->crud_database() + ".instance";
    if (param->has_resource_tag()) {
        sql += " WHERE resource_tag='" + param->resource_tag() + "'";
    }

    ret = query(sql, result_set);
    if (ret < 0) {
        DB_WARNING("sql:%s failed", sql.c_str());
        return -1;
    }

    while (result_set.next()) {
        result_set.get_int64("used_size", &used_size);
        result_set.get_int64("capacity", &capacity);
    }

    sql = "SELECT status, count(status) AS status_stat FROM " + param->crud_database() + ".instance ";
    if (param->has_resource_tag()) {
        sql += "WHERE resource_tag='" + param->resource_tag() + "' ";
    }

    sql += "GROUP BY status";

    ret = query(sql, result_set);
    if (ret < 0) {
        DB_WARNING("sql:%s failed", sql.c_str());
        return -1;
    }

    const google::protobuf::EnumDescriptor *descriptor = pb::Status_descriptor();
    
    std::string instances_status;

    while (result_set.next()) {
        result_set.get_int32("status", &status);
        result_set.get_int64("status_stat", &status_stat);
        std::string name = descriptor->FindValueByNumber(status)->name();
        instances_status += name + ":";
        instances_status += std::to_string(status_stat);
        instances_status += " ";
    }
    auto overview_info = response->mutable_overview();
    overview_info->set_usage(transform_usage_info(used_size, capacity, false));
    overview_info->set_instances_status(instances_status);

    return 0;
}

int BaikalProxy::get_platform_info(const pb::QueryParam * param, const pb::ConsoleRequest* request,
                                   pb::ConsoleResponse* response) {
    pb::WatchPlatForm* wp_pb = response->mutable_plat_forms();
    for (auto& pair : plat_databases_mapping) {
        wp_pb->add_plat_form(pair.first);
    }
    return 0;
}

int BaikalProxy::get_overview_info(const pb::QueryParam * param, const pb::ConsoleRequest* request,
                                   pb::ConsoleResponse* response) {
    int ret = 0;

    if (!param->has_resource_tag()) {
         ret = database_usage_info(param, request, response);
    } else {
         ret = cluster_usage_info(param, request, response);
    }        
    if (ret < 0) {
        return -1;
    } 

    return 0;
}

int BaikalProxy::get_user_info(const pb::QueryParam * param, const pb::ConsoleRequest* request, 
                               pb::ConsoleResponse* response) {
    baikal::client::ResultSet result_set;
    int ret = 0;
    std::string sql = "SELECT * FROM " + param->crud_database() + ".user ";
    if (param->has_namespace_name()) {
        sql += "WHERE `namespace`='" + param->namespace_name() + "' ";
    }
    sql += "ORDER BY `username`";
    ret = query(sql, result_set);
    if (ret < 0) {
        DB_WARNING("sql:%s failed", sql.c_str());
        return -1;
    }

    while (result_set.next()) {
        std::string  namespace_name;
        std::string  username;
        std::string  password;
        std::string  database_table;
        int32_t      permission;
        result_set.get_string("namespace", &namespace_name);
        result_set.get_string("username", &username);
        result_set.get_int32("permission", &permission);
        result_set.get_string("password", &password);
        result_set.get_string("table_name", &database_table);

        auto user_privl_pb = response->add_user_privileges();
        user_privl_pb->set_username(username);
        user_privl_pb->set_namespace_name(namespace_name);
        user_privl_pb->set_password(password);
        user_privl_pb->set_id(_response_info_id++);
        user_privl_pb->set_permission(static_cast<pb::RW>(permission));
        user_privl_pb->set_tablename(database_table);
    }
    return 0;
}

int BaikalProxy::get_namespace_info(const pb::QueryParam * param, const pb::ConsoleRequest* request,
                    pb::ConsoleResponse* response) {
    baikal::client::ResultSet result_set;
    int ret = 0;
    auto database = plat_databases_mapping[param->platform()];
    std::string sql = "SELECT DISTINCT namespace_name FROM " + database + ".table_info";
    ret = query(sql, result_set);
    if (ret < 0) {
        DB_WARNING("sql:%s failed", sql.c_str());
        return -1;
    }

    std::string namespace_name;
    pb::WatchNameSpace* ns_pb = response->mutable_namespaces();

    while (result_set.next()) {
        result_set.get_string("namespace_name", &namespace_name);
        ns_pb->add_namespace_name(namespace_name);
    }
    return 0;
}

int BaikalProxy::get_cluster_info(const pb::QueryParam * param, const pb::ConsoleRequest* request,
                  pb::ConsoleResponse* response) {
    baikal::client::ResultSet result_set;
    int ret = 0;
    std::string sql = "SELECT DISTINCT resource_tag FROM " + param->crud_database() + ".instance"
                      " ORDER BY resource_tag";
    
    ret = query(sql, result_set);
    if (ret < 0) {
        DB_WARNING("sql:%s failed", sql.c_str());
        return -1;
    }

    int itemnum = 0;
    while (result_set.next()) {
        std::string resource_tag;
        result_set.get_string("resource_tag", &resource_tag);

        pb::WatchClusterInfo* wc_pb = response->add_cluster_infos();
        wc_pb->set_id(_response_info_id++);
        wc_pb->set_name(resource_tag);
        itemnum++;
    }
    response->set_itemnum(itemnum);
    return 0;
}

int BaikalProxy::get_database_info(const pb::QueryParam * param, const pb::ConsoleRequest* request,
                  pb::ConsoleResponse* response) {
    baikal::client::ResultSet result_set;
    int ret = 0;

    if (!param->has_namespace_name()) {
        DB_WARNING("need namespace");
        return -1;
    }

    std::string sql = "SELECT `database`,table_name,table_id FROM " + param->crud_database() + ".table_info "
                      "where namespace_name=";
    sql.reserve(512);
    std::string namespace_name = "'" + param->namespace_name() + "'" ;
    sql += namespace_name;
    sql += " ORDER BY `database`, table_name";

    ret = query(sql, result_set);
    if (ret < 0) {
        DB_WARNING("sql:%s failed", sql.c_str());
        return -1;
    }
    std::string prev_tag;
    std::string cur_tag;
    int64_t     itemnum = 0;
    bool is_first = true;
    pb::WatchDatabaseInfo* db_pb = response->add_databases();

    while (result_set.next()) {
        std::string database;
        std::string table_name;
        int64_t table_id;
        result_set.get_string("database", &database);
        result_set.get_string("table_name", &table_name);
        result_set.get_int64("table_id", &table_id);
        cur_tag = database;

        if (cur_tag.compare(prev_tag) != 0) {
            if (!is_first) {
                db_pb = response->add_databases();
             } else {
                is_first = false;
            }
            db_pb->set_id(_response_info_id++);
            db_pb->set_name(database);
            prev_tag = cur_tag;
        }
        itemnum++;
        auto namep = db_pb->add_children();
        namep->set_name(table_name);
        namep->set_id(table_id);
    }
    response->set_itemnum(itemnum);

    return 0;
}

int BaikalProxy::get_table_info(const pb::QueryParam * param, const pb::ConsoleRequest* request,
                  pb::ConsoleResponse* response) {
    baikal::client::ResultSet result_set;
    int ret = 0;
    std::string sql = "SELECT * FROM " + param->crud_database() + ".table_info WHERE namespace_name=";
    sql.reserve(2048);
    std::string tmp;

    if (param->has_namespace_name()) {
        tmp = "'" + param->namespace_name() + "' ";
        sql += tmp;
    } else {
        DB_WARNING("need namespace");
        return -1;
    } 

    if (param->has_database()) {
        tmp = "AND `database`='" + param->database()+ "' ";
        sql += tmp;
    }
    
    if (param->has_table_name()) {
        tmp = "AND table_name LIKE '%" + param->table_name()+ "%' ";
        sql += tmp;
    }

    sql += "ORDER BY table_name";
   
    ret = query(sql, result_set);
    if (ret < 0) {
        DB_WARNING("sql:%s", sql.c_str()); 
        return -1;
    }
    int64_t     table_count = 0;

    while (result_set.next()) {
        int64_t table_id;
        std::string namespace_name;
        std::string database;
        std::string table_name;
        std::string resource_tag;
        int32_t status;
        int64_t version;
        std::string create_time;
        int64_t region_count;
        int64_t row_count;
        int64_t byte_size_per_record;
        int64_t max_field_id;
        int64_t region_size;
        int64_t main_table_id;
        int64_t region_split_lines;

        result_set.get_int64("table_id", &table_id);
        result_set.get_int64("main_table_id", &main_table_id);
        result_set.get_string("namespace_name", &namespace_name);
        result_set.get_string("database", &database);
        result_set.get_string("table_name", &table_name);
        result_set.get_string("resource_tag", &resource_tag);
        result_set.get_int32("status", &status);
        result_set.get_int64("version", &version);
        result_set.get_string("create_time", &create_time);
        result_set.get_int64("region_count", &region_count);
        result_set.get_int64("row_count", &row_count);
        result_set.get_int64("byte_size_per_record", &byte_size_per_record);
        result_set.get_int64("max_field_id", &max_field_id);
        result_set.get_int64("region_size", &region_size);
        result_set.get_int64("region_split_lines", &region_split_lines);
   
        table_count++;
        pb::WatchTableInfo* table = response->add_table_infos();
        table->set_table_id(table_id);
        table->set_main_table_id(main_table_id);
        table->set_id(_response_info_id++);
        table->set_table_name(table_name);
        table->set_resource_tag(resource_tag);
        table->set_status(static_cast<pb::Status>(status));
        table->set_version(version);
        table->set_byte_size_per_record(byte_size_per_record);
        table->set_create_time(create_time);
        table->set_region_count(region_count);
        table->set_row_count(row_count);
        table->set_region_split_lines(region_split_lines);
        table->set_max_field_id(max_field_id);
        table->set_region_size(region_size);
    }
    response->set_itemnum(table_count);

    return 0;
}

void BaikalProxy::delete_table_and_region(const std::string& database_name,
                                 const int64_t table_id) {
    baikal::client::ResultSet result_set;
    int ret = 0;

    std::string sql = "DELETE FROM " + database_name + ".table_info WHERE `table_id`=";
    sql += std::to_string(table_id);
    ret = query(sql, result_set);
    if (ret < 0) {
        DB_WARNING("delete sql:%s failed", sql.c_str());
    } 
    sql = "DELETE FROM " + database_name + ".region WHERE `table_id`=";
    sql += std::to_string(table_id);
    ret = query(sql, result_set);
    if (ret < 0) {
        DB_WARNING("delete sql:%s failed", sql.c_str());
    }
}

void BaikalProxy::update_table_count(const std::string& database_name, const int64_t table_id,
                                     const int64_t region_count) {
    baikal::client::ResultSet result_set;
    int ret = 0;

    std::string num_lines_sql = "SELECT sum(num_table_lines) AS row_count FROM "
                 + database_name + ".region WHERE `table_id`=";
    num_lines_sql += std::to_string(table_id);

    ret = query(num_lines_sql, result_set);
    if (ret < 0) {
        DB_WARNING("insert sql:%s failed", num_lines_sql.c_str());
    } 

    int64_t row_count = 0;
    while (result_set.next()) {
        result_set.get_int64("row_count", &row_count);
    }
   
    std::string sql = "UPDATE " + database_name + ".table_info t SET t.region_count=";
    sql.reserve(512);
    sql += std::to_string(region_count);
    sql += ", t.row_count=";
    sql += std::to_string(row_count);
    sql += " WHERE t.table_id=";
    sql += std::to_string(table_id);

    ret = query(sql, result_set);
    if (ret < 0) {
        DB_WARNING("insert sql:%s failed", sql.c_str());
    }    
}

void BaikalProxy::insert_table_info(const std::string& database_name,
                                    std::vector<pb::QueryTable>& table_infos) {
    baikal::client::ResultSet result_set;
    int ret = 0;
    std::string insert_sql = "REPLACE INTO " + database_name + ".table_info(`table_id`, "
        "`namespace_name`, `database`, `table_name`, `resource_tag`, `status`, `version`,"
        "`create_time`, `region_count`, `row_count`, `byte_size_per_record`, `max_field_id`,"
        " `region_size`, `main_table_id`, `region_split_lines`) VALUES ";
    insert_sql.reserve(4096);
    std::string num_lines_sql = "SELECT sum(num_table_lines) AS row_count FROM "
                 + database_name + ".region WHERE `table_id`=";
    for (auto& info : table_infos) {
        std::string sql = num_lines_sql + std::to_string(info.table_id());
        ret = query(sql, result_set);
        if (ret < 0) {
            DB_WARNING("insert sql:%s failed", sql.c_str());
        }
        int64_t row_count = 0;
        while (result_set.next()) {
            result_set.get_int64("row_count", &row_count);
        }
        info.set_row_count(row_count);
    }
    std::string values;
    values.reserve(4096);
    for (auto info : table_infos) {
        values += "(" + std::to_string(info.table_id()) + ",";
        values += "'" + info.namespace_name() + "',";
        values += "'" + info.database() + "',";
        values += "'" + info.table_name() + "',";
        std::string resource_tag = info.resource_tag();
        if (resource_tag.size() == 0) {
            resource_tag = "null";
        }
        values += "'" + resource_tag + "',";
        values += std::to_string(info.status()) + ",";
        values += std::to_string(info.version()) + ",";
        values += "'" + info.create_time() + "',";
        values += std::to_string(info.region_count()) + ",";
        values += std::to_string(info.row_count()) + ",";
        values += std::to_string(info.byte_size_per_record()) + ",";
        values += std::to_string(info.max_field_id()) + ",";
        values += std::to_string(info.region_size()) + ",";
        values += std::to_string(info.main_table_id()) + ",";
        values += std::to_string(info.region_split_lines()) + "),";
    }
    values.pop_back();
    insert_sql += values;

    ret = query(insert_sql, result_set);
    if (ret < 0) {
        DB_WARNING("insert sql:%s failed", insert_sql.c_str());
    }
}

void BaikalProxy::insert_instance_info(const std::string& database_name,
                                       const std::vector<pb::QueryInstance>& instance_infos) {
    baikal::client::ResultSet result_set;
    int ret = 0;
    std::string sql = "REPLACE INTO " +database_name + ".instance (`address`, `capacity`,"
        "`used_size`, `resource_tag`, `physical_room`, `status`,`logical_room`,`region_count`, "
        "`peer_count`, `leader_count`) VALUES ";
    sql.reserve(4096);

    std::string values;
    values.reserve(4096); 
    for (auto info : instance_infos) {    
        values += "('" + info.address() +"',";
        values += std::to_string(info.capacity()) +",";
        values += std::to_string(info.used_size()) + ",";

        std::string resource_tag = info.resource_tag();
        if (resource_tag.size() == 0) {
            resource_tag = "null";
        }

        values += "'" + resource_tag + "',";
        values += "'" + info.physical_room() + "',";
        values += std::to_string(info.status()) + ",";
        values += "'" + info.logical_room() + "',";
        values += std::to_string(info.region_count()) + ",";
        values += std::to_string(info.peer_count()) + ",";
        values += std::to_string(info.region_leader_count()) + "),";
    }
    values.pop_back();
    sql += values;

    ret = query(sql, result_set);
    if (ret < 0) {
        DB_WARNING("insert sql:%s failed", sql.c_str());
    }
}

void BaikalProxy::ctor_select_instance_sql(const pb::QueryParam * param, std::string& sql) {
    bool has_logical_room = false;
    bool has_physical_room = false;
    bool has_instance = false;

    if (param->has_logical_room()) {
        sql += "WHERE logical_room='" + param->logical_room() + "' ";
        has_logical_room = true;
    }
    if (param->has_physical_room()) {
        if (has_physical_room) {
            sql += "AND physical_room='" + param->physical_room() + "' ";
        }else {
            sql += "WHERE physical_room='" + param->physical_room() + "' ";
        }
        has_physical_room = true;
    }
    if (param->has_instance()) {
        if (has_logical_room || has_physical_room) {
            sql += "AND address LIKE '" + param->instance() + "%' ";
        } else {
            sql += "WHERE address LIKE '" + param->instance() + "%' ";
        }
        has_instance = true;
    }
    if (param->has_resource_tag()) {
        if (has_logical_room || has_physical_room || has_instance) {
            sql += "AND resource_tag='" + param->resource_tag() + "' ";
        } else {
            sql += "WHERE resource_tag='" + param->resource_tag() + "' ";
        }
    }

/*  
    if (param->has_start()) {
        sql += "limit " + std::to_string((stol(param->start()) - 1) * 10) + ",";
    } else {
        sql += "limit 0,";
    }
    if (param->has_limit()) {
        sql += param->limit();
    } else {
        sql += "10";
    }
*/
}

int BaikalProxy::get_instance_info(const pb::QueryParam * param, const pb::ConsoleRequest* request,
            pb::ConsoleResponse* response) {
    baikal::client::ResultSet result_set;
    int ret = 0;

    std::string sql = "SELECT * FROM " + param->crud_database() + ".instance ";
    sql.reserve(2048);
    ctor_select_instance_sql(param, sql);
 
    ret = query(sql, result_set);
    if (ret < 0) {
        DB_WARNING("sql:%s", sql.c_str()); 
        return -1;
    }

    int itemnum = 0;
    while (result_set.next()) {
        std::string address;
        std::string resource_tag;
        std::string logical_room;
        std::string physical_room;
        int32_t status;
        int64_t used_size;
        int64_t region_count;
        int64_t peer_count;
        int64_t leader_count;
        int64_t capacity;
     
        result_set.get_string("address", &address);
        result_set.get_string("resource_tag", &resource_tag);
        result_set.get_string("logical_room", &logical_room);
        result_set.get_string("physical_room", &physical_room);
        result_set.get_int32("status", &status);
        result_set.get_int64("used_size", &used_size);
        result_set.get_int64("region_count", &region_count);
        result_set.get_int64("peer_count", &peer_count);
        result_set.get_int64("leader_count", &leader_count);
        result_set.get_int64("capacity", &capacity);
        
        itemnum++;
        pb::WatchInstanceInfo* instance = response->add_instance_infos();
        instance->set_capacity(capacity);
        instance->set_address(address);
        instance->set_used_size(used_size);
        instance->set_resource_tag(resource_tag);
        instance->set_physical_room(physical_room);
        instance->set_logical_room(logical_room);
        instance->set_region_count(region_count);
        instance->set_peer_count(peer_count);
        instance->set_leader_count(leader_count);
        instance->set_status(static_cast<pb::Status>(status));
        instance->set_id(_response_info_id++);
    } 
    response->set_itemnum(itemnum);
    return 0;
}

void BaikalProxy::update_region_info(const std::string& database_name,
                                     const RegionStat& region_stat) {
    baikal::client::ResultSet result_set;
    int ret = 0;
  
    std::string sql = "UPDATE " + database_name + ".region SET `used_size`=";
    sql.reserve(1024);
    sql += std::to_string(region_stat.used_size);
    sql += ",`num_table_lines`=" + std::to_string(region_stat.num_table_lines);
    sql += " WHERE `region_id`=" + std::to_string(region_stat.region_id);

    ret = query(sql, result_set);
    if (ret < 0) {
        DB_WARNING("update sql:%s failed", sql.c_str());
    }
}

void BaikalProxy::insert_region_info(const std::string& database_name,
                                     const std::vector<pb::QueryRegion>& region_infos) {
    baikal::client::ResultSet result_set;
    int ret = 0;

    std::string sql = "REPLACE INTO " + database_name + ".region (`region_id`,"
        "`table_id`, `parent`, `table_name`, `partition_id`, `create_time`, `start_key`, `end_key`,"
        "`peers`, `leader`, `status`, `replica_num`, `version`, `conf_version`, `log_index`,"
        " `used_size`, `num_table_lines`, `main_table_id`, `raw_start_key`) VALUES ";
    sql.reserve(4096);

    std::string values;
    values.reserve(4096);
    for (auto info : region_infos) {   
        values += "(" + std::to_string(info.region_id()) + ",";
        values += std::to_string(info.table_id()) + ",";
        values += std::to_string(info.parent()) + ",";
        values += "'" + info.table_name() + "',";
        values += std::to_string(info.partition_id()) + ",";
        values += "'" + info.create_time() + "',";
        values += "'" + mysql_escape_string(info.start_key()) + "',";
        values += "'" + mysql_escape_string(info.end_key()) + "',";
        values += "'" + info.peers() + "',";
        values += "'" + info.leader() + "',";
        values += std::to_string(info.status()) + ",";
        values += std::to_string(info.replica_num()) + ",";
        values += std::to_string(info.version()) + ",";
        values += std::to_string(info.conf_version()) + ",";
        values += std::to_string(info.log_index()) + ",";
        values += std::to_string(info.used_size()) + ",";
        values += std::to_string(info.num_table_lines()) + ",";
        values += std::to_string(info.main_table_id()) + ",";
        values += "'" + mysql_escape_string(info.raw_start_key()) + "'),";
    }
    values.pop_back();
    sql += values;

    ret = query(sql, result_set);
    if (ret < 0) {
        DB_WARNING("insert sql:%s failed", sql.c_str());
    }
}

void BaikalProxy::ctor_select_region_sql(const pb::QueryParam * param, std::string& sql) {
    if (param->has_step() && !param->has_instance()) {
        auto step = param->step();
        sql += "WHERE table_id='" + param->table_id() + "' ";
        if (!step.compare("prev")) {
            sql += "AND raw_start_key<='" + param->raw_start_key() + "' " + 
                   "ORDER BY raw_start_key DESC LIMIT 6"; 
        }else if (!step.compare("next")) {
            sql += "AND raw_start_key>'" + param->raw_start_key() + "' " + 
                   "ORDER BY raw_start_key ASC LIMIT 5"; 
        }
        return ;
    }

    if (param->has_instance()) {
        sql += "WHERE  leader='" +param->instance() + "' ";
        sql += "ORDER BY region_id ";
    } else {
        if (param->has_region_id()) {
            sql += "WHERE region_id=" + param->region_id() + " ";
            return ;
        }
        if (param->has_table_id()) {
            sql += "WHERE table_id='" + param->table_id() + "' ";
        }
        sql += "ORDER BY raw_start_key ";
    }
    if (param->has_start()) {
        sql += "limit " + std::to_string((stol(param->start()) - 1) * 20) + ",";
    } else {
        sql += "limit 0,";
    }
    if (param->has_limit()) {
        sql += param->limit();
    } else {
        sql += "20";
    }
}

int BaikalProxy::get_tableid_and_raw_start_key(pb::QueryParam * param) {
    baikal::client::ResultSet result_set;
    int ret = 0;

    std::string sql = "SELECT table_id, raw_start_key FROM " + param->crud_database()
        + ".region WHERE region_id=" + param->region_id();
    ret = query(sql, result_set);
    if (ret < 0) {
        DB_WARNING("sql:%s", sql.c_str()); 
        return -1;
    }
    
    while (result_set.next()) {
        std::string raw_start_key;
        int64_t table_id;
        result_set.get_string("raw_start_key", &raw_start_key);
        result_set.get_int64("table_id", &table_id);

        param->set_table_id(std::to_string(table_id));
        param->set_raw_start_key(raw_start_key);
    }
 
    return 0;
}

void BaikalProxy::ctor_region_info(baikal::client::ResultSet& result_set, 
        std::vector<SmartWatchRegionInfo>& region_infos) {
    int64_t table_id;
    int64_t main_table_id;
    int64_t region_id;
    int64_t parent;
    std::string table_name;
    int64_t partition_id;
    std::string create_time;
    std::string peers;
    std::string leader;
    int32_t status;
    int64_t version;
    int64_t replica_num;
    int64_t conf_version;
    int64_t log_index;
    int64_t used_size;
    int64_t num_table_lines;
    std::string start_key;
    std::string end_key;
    std::string raw_start_key;

    while (result_set.next()) {
        result_set.get_int32("status", &status);
        result_set.get_int64("table_id", &table_id);
        result_set.get_int64("main_table_id", &main_table_id);
        result_set.get_int64("region_id", &region_id);
        result_set.get_int64("parent", &parent);
        result_set.get_int64("partition_id", &partition_id);
        result_set.get_int64("num_table_lines", &num_table_lines);
        result_set.get_string("create_time", &create_time);
        result_set.get_int64("version", &version);
        result_set.get_int64("replica_num", &replica_num);
        result_set.get_int64("conf_version", &conf_version);
        result_set.get_int64("log_index", &log_index);
        result_set.get_int64("used_size", &used_size);
        result_set.get_string("start_key", &start_key);
        result_set.get_string("raw_start_key", &raw_start_key);
        result_set.get_string("end_key", &end_key);
        result_set.get_string("table_name", &table_name);
        result_set.get_string("peers", &peers);
        result_set.get_string("leader", &leader);

        SmartWatchRegionInfo region_info =  SmartWatchRegionInfo(new (std::nothrow)pb::WatchRegionInfo());
        region_info->set_region_id(region_id);
        region_info->set_table_name(table_name);
        region_info->set_table_id(table_id);
        region_info->set_main_table_id(main_table_id);
        region_info->set_partition_id(partition_id);
        region_info->set_replica_num(replica_num);
        region_info->set_version(version);
        region_info->set_conf_version(conf_version);
        region_info->set_start_key(start_key);
        region_info->set_raw_start_key(raw_start_key);
        region_info->set_end_key(end_key);
        region_info->set_peers(peers);
        region_info->set_leader(leader);
        region_info->set_used_size(used_size);
        region_info->set_num_table_lines(num_table_lines);
        region_info->set_log_index(log_index);
        region_info->set_parent(parent);
        region_info->set_create_time(create_time);
        region_info->set_status(static_cast<pb::RegionStatus>(status));
        region_info->set_id(_response_info_id++);
        region_infos.push_back(region_info);
    }
}

int BaikalProxy::get_full_table_name(const pb::QueryParam * param, std::string& full_table_name) {
    baikal::client::ResultSet result_set;
    int ret = 0;
    
    std::string sql = "SELECT `namespace_name`,`database`,`table_name` FROM " + param->crud_database() + ".table_info ";

    if (param->has_table_id()) {
        sql += "WHERE table_id=" + param->table_id();
    } else {
        DB_WARNING("no table_id set");
        return -1;
    }
    ret = query(sql, result_set);
    if (ret < 0) {
        DB_WARNING("sql:%s", sql.c_str());
        return -1;
    }
    std::string namespace_name;
    std::string database;
    std::string table_name;
    while (result_set.next()) {
        result_set.get_string("namespace_name", &namespace_name);
        result_set.get_string("database", &database);
        result_set.get_string("table_name", &table_name);
    }

    full_table_name = namespace_name + "." + database + "." + table_name;
    return 0;
}

int BaikalProxy::search_region_info(pb::QueryParam * param, const pb::ConsoleRequest* request,
                  pb::ConsoleResponse* response) {
    baikal::client::ResultSet result_set;
    int ret = 0;
    int itemnum = 0;
    
    ret = get_tableid_and_raw_start_key(param);
    if (ret < 0) {
        return -1;
    }
    
    std::string full_table_name;
    ret = get_full_table_name(param, full_table_name);
    if (ret < 0) {
        return -1;
    }
    std::vector<SmartWatchRegionInfo> region_infos;

    // get first 4 region   
    std::string select_region_sql = "SELECT * FROM " + param->crud_database() + ".region ";
    ctor_select_region_sql(param, select_region_sql);

    ret = query(select_region_sql, result_set);
    if (ret < 0) {
        DB_WARNING("sql:%s", select_region_sql.c_str());
        return -1;
    }
    itemnum += result_set.get_row_count();
    ctor_region_info(result_set, region_infos);    

    std::reverse(region_infos.begin(), region_infos.end());
    // get last 4 region
    select_region_sql = "SELECT * FROM " + param->crud_database() + ".region ";
    param->set_step("next");
    ctor_select_region_sql(param, select_region_sql);

    ret = query(select_region_sql, result_set);
    if (ret < 0) {
        DB_WARNING("sql:%s", select_region_sql.c_str());
        return -1;
    }
    itemnum += result_set.get_row_count();
    ctor_region_info(result_set, region_infos);
    response->set_itemnum(itemnum);
    for (auto& region : region_infos) {
        region->set_table_name(full_table_name);
        auto region_info =  response->add_region_infos();
        *region_info = *region;
    }

    return 0;
}

int BaikalProxy::normal_region_info(const pb::QueryParam * param, const pb::ConsoleRequest* request,
                  pb::ConsoleResponse* response) {
    baikal::client::ResultSet result_set;
    int ret = 0;
    
    std::string full_table_name;
    ret = get_full_table_name(param, full_table_name);
    if (ret < 0) {
        return -1;
    }

    std::string sql = "SELECT COUNT(*) AS itemnum FROM " + param->crud_database() + ".region ";
    if (param->has_table_name()) {
        sql += "WHERE table_name='" + param->table_name() + "' ";
    }
    if (param->has_table_id()) {
        if (param->has_table_name()) {
            sql += "AND table_id='" + param->table_id() + "' ";
        } else {
            sql += "WHERE table_id='" + param->table_id() + "' ";
        }
    }
    
    ret = query(sql, result_set);
    if (ret < 0) {
        DB_WARNING("sql:%s", sql.c_str());
        return -1;
    }

    while (result_set.next()) {
        int64_t itemnum;
        result_set.get_int64("itemnum", &itemnum);
        response->set_itemnum(itemnum);
    }

    std::string select_region_sql = "SELECT * FROM " + param->crud_database() + ".region ";
    ctor_select_region_sql(param, select_region_sql);
    std::vector<SmartWatchRegionInfo> region_infos;

    ret = query(select_region_sql, result_set);
    if (ret < 0) {
        DB_WARNING("sql:%s", select_region_sql.c_str());
        return -1;
    }

    ctor_region_info(result_set, region_infos);

    for (auto& region : region_infos) {
        region->set_table_name(full_table_name);
        auto region_info =  response->add_region_infos();
        *region_info = *region;
    }

    return 0;
}

int BaikalProxy::instance_region_info(const pb::QueryParam * param,
                            const pb::ConsoleRequest* request,
                            pb::ConsoleResponse* response) {
    baikal::client::ResultSet result_set;
    int ret = 0;

    std::string sql = "SELECT COUNT(*) AS itemnum FROM " + param->crud_database() + ".region ";
    sql += "WHERE leader='" + param->instance() + "'";

    ret = query(sql, result_set);
    if (ret < 0) {
        DB_WARNING("sql:%s", sql.c_str());
        return -1;
    }

    while (result_set.next()) {
        int64_t itemnum;
        result_set.get_int64("itemnum", &itemnum);
        response->set_itemnum(itemnum);
    }
    std::string select_region_sql = "SELECT * FROM " + param->crud_database() + ".region ";
    ctor_select_region_sql(param, select_region_sql);
    std::vector<SmartWatchRegionInfo> region_infos;

    ret = query(select_region_sql, result_set);
    if (ret < 0) {
        DB_WARNING("sql:%s", select_region_sql.c_str());
        return -1;
    }

    ctor_region_info(result_set, region_infos);

    for (auto& region : region_infos) {
        auto region_info =  response->add_region_infos();
        *region_info = *region;
    }
    return 0;
}

int BaikalProxy::get_region_info(pb::QueryParam * param, const pb::ConsoleRequest* request,
             pb::ConsoleResponse* response) {
    if (param->has_instance()) {
        instance_region_info(param, request, response);
    } else if (!param->has_step()) {
        normal_region_info(param, request, response);
    } else {
        search_region_info(param, request, response);
    }

    return 0;
}

int BaikalProxy::get_task_list_by_database(pb::QueryParam * param,
                                const pb::ConsoleRequest* request,
                                pb::ConsoleResponse* response) {
    std::string sql = "SELECT * FROM " + FLAGS_param_db + "." + FLAGS_param_tbl + " WHERE ";
    sql += "table_info LIKE '";
    if (param->has_database()) {
        sql += param->database() + "%'";
    } else {
        DB_WARNING("param hasn`t database");
        return -1;
    }
    baikal::client::ResultSet result_set;
    int ret = query_import(sql, result_set);
    if (ret < 0) {
        DB_WARNING("query sql:%s fail", sql.c_str());
        return -1;
    }
    DB_WARNING("query sql:%s", sql.c_str());

    while (result_set.next()) {
        std::string table_info;
        std::string cluster_name;
        std::string done_file;
        std::string user_sql;
        std::string charset;
        std::string modle;
        std::string status;
        result_set.get_string("table_info", &table_info);
        result_set.get_string("cluster_name", &cluster_name);
        result_set.get_string("done_file", &done_file);
        result_set.get_string("user_sql", &user_sql);
        result_set.get_string("charset", &charset);
        result_set.get_string("modle", &modle);
        result_set.get_string("status", &status);
        DB_WARNING("import task:table_info:%s, cluster_name:%s, done_file:%s, user_sql:%s,"
            "charset:%s, modle:%s, status:%s",
            table_info.c_str(), cluster_name.c_str(), done_file.c_str(),
            user_sql.c_str(), charset.c_str(), modle.c_str(), status.c_str());
        auto task_list = response->add_task_list();
        task_list->set_table_info(table_info);
        task_list->set_cluster_name(cluster_name);
        task_list->set_done_file(done_file);
        task_list->set_user_sql(user_sql);
        task_list->set_charset(charset);
        task_list->set_modle(modle);
        task_list->set_status(status);
    }
    return 0;
}

int BaikalProxy::get_import_task(pb::QueryParam * param, const pb::ConsoleRequest* request,
             pb::ConsoleResponse* response) {
    std::string sql = "SELECT * FROM " + FLAGS_result_db + "." + FLAGS_result_tbl + " WHERE ";
    sql += "table_info = '";
    if (param->has_database()) {
        sql += param->database() + ".";
    } else {
        DB_WARNING("param hasn`t database");
        return -1;
    }
    if (param->has_table_name()) {
        sql += param->table_name() + "'";
    } else {
        DB_WARNING("param hasn`t table name");
        return -1;
    }
    sql += " order by start_time desc limit 10";
    baikal::client::ResultSet result_set;
    int ret = query_import(sql, result_set);
    if (ret < 0) {
        DB_WARNING("query sql:%s fail", sql.c_str());
        return -1;
    }
    DB_WARNING("query sql:%s", sql.c_str());
    while (result_set.next()) {
        int64_t version;
        std::string start_time;
        std::string end_time;
        std::string exec_time;
        int64_t import_line;
        std::string modle;
        std::string status;
        result_set.get_int64("new_version", &version);
        result_set.get_string("start_time", &start_time);
        result_set.get_string("end_time", &end_time);
        result_set.get_string("exec_time", &exec_time);
        result_set.get_int64("import_line", &import_line);
        result_set.get_string("modle", &modle);
        result_set.get_string("status", &status);
        DB_WARNING("import task: version:%d, exec_time:%s, import_line:%lld, modle:%s, status:%s",
            version, exec_time.c_str(), import_line, modle.c_str(), status.c_str());
        auto import_task = response->add_import_task();
        import_task->set_version(version);
        import_task->set_start_time(start_time);
        import_task->set_end_time(end_time);
        import_task->set_exec_time(exec_time);
        import_task->set_import_line(import_line);
        import_task->set_modle(modle);
        import_task->set_status(status);
    }
    return 0;
}

int BaikalProxy::get_import_task2(pb::QueryParam * param, const pb::ConsoleRequest* request,
             pb::ConsoleResponse* response) {
    std::string sql = "SELECT * FROM " + FLAGS_result_db + "." + FLAGS_result_tbl + " WHERE ";
    sql += "table_info = '";
    if (param->has_database()) {
        sql += param->database() + ".";
    } else {
        DB_WARNING("param hasn`t database");
        return -1;
    }
    if (param->has_table_name()) {
        sql += param->table_name() + "'";
    } else {
        DB_WARNING("param hasn`t table name");
        return -1;
    }
    baikal::client::ResultSet result_set;
    int ret = query_import(sql, result_set);
    if (ret < 0) {
        DB_WARNING("query sql:%s fail", sql.c_str());
        return -1;
    }
    DB_WARNING("query sql:%s", sql.c_str());
    while (result_set.next()) {
        int64_t version;
        std::string start_time;
        std::string end_time;
        std::string exec_time;
        int64_t import_line;
        std::string modle;
        std::string status;
        result_set.get_int64("new_version", &version);
        result_set.get_string("exec_time", &exec_time);
        result_set.get_int64("import_line", &import_line);
        result_set.get_string("modle", &modle);
        result_set.get_string("status", &status);
        DB_WARNING("import task: version:%d, exec_time:%s, import_line:%lld, modle:%s, status:%s",
        version, exec_time.c_str(), import_line, modle.c_str(), status.c_str());
        auto import_task = response->add_import_task();
        import_task->set_version(version);
        import_task->set_exec_time(exec_time);
        import_task->set_import_line(import_line);
        import_task->set_modle(modle);
        import_task->set_status(status);
    }
    return 0;
}
int BaikalProxy::submit_import_task(pb::QueryParam * param, const pb::ConsoleRequest* request,
             pb::ConsoleResponse* response) {
    std::string database;
    std::string table_name;
    std::string done_file;
    std::string cluster_name;
    std::string ago_days;
    std::string interval_days;
    std::string user_sql;
    std::string modle;
    std::string charset;
    if (param->has_database()) {
        database = param->database();
    } else {
        DB_WARNING("param hasn`t database");
        return -1;
    }
    if (param->has_table_name()) {
        table_name = param->table_name();
    } else {
        DB_WARNING("param hasn`t table name");
        return -1;
    }
    if (param->has_done_file()) {
        done_file = url_decode(param->done_file());
    } else {
        DB_WARNING("param hasn`t done_file");
        return -1;
    }
    if (param->has_cluster_name()) {
        cluster_name = param->cluster_name();
    } else {
        DB_WARNING("param hasn`t cluster_name");
        return -1;
    }
    if (param->has_ago_days()) {
        ago_days = param->ago_days();
    } else {
        DB_WARNING("param hasn`t ago_days");
        return -1;
    }
    if (param->has_interval_days()) {
        interval_days = param->interval_days();
        if (atoll(interval_days.c_str()) > 63) {
            interval_days = "63";
            DB_WARNING("inteval_days too large use 63");
        }
    } else {
        interval_days = "7";
        DB_WARNING("param hasn`t interval_days use defourt 7");
    }
    if (param->has_user_sql()) {
        user_sql = url_decode(param->user_sql());
        user_sql = boost::replace_all_copy(user_sql, "'", "\\'");
    } else {
        DB_WARNING("param hasn`t user_sql");
        return -1;
    }
    if (param->has_modle()) {
        modle = param->modle();
    } else {
        DB_WARNING("param hasn`t modle");
        return -1;
    }
    if (param->has_charset()) {
        charset = param->charset();
        if (charset == "utf-8") {
            charset = "utf8";
        }
    } else {
        DB_WARNING("param hasn`t charset");
        return -1;
    }
    baikal::client::ResultSet result_set;
    std::string sql = "REPLACE INTO " + FLAGS_param_db + "." + FLAGS_param_tbl + " (table_info, done_file, version, ago_days, "
        "interval_days, status, cluster_name, start_time, modle, charset, user_sql)";
    sql += "VALUES('" + database + "." + table_name + "', '" + done_file + "', 0, " + ago_days 
        + ", " + interval_days + ", 'idle', '" + cluster_name + "', 0, '" + modle + "', '" + charset + "', '" + user_sql + "')"; 
    int ret = query_import(sql, result_set);
    if (ret < 0) {
        DB_WARNING("query sql:%s fail", sql.c_str());
        return -1;
    }
    DB_WARNING("query sql:%s succ", sql.c_str());
    return 0;
}

int BaikalProxy::get_sst_backup_task(pb::QueryParam * param, const pb::ConsoleRequest* request,
             pb::ConsoleResponse* response) {

    std::string sql = "SELECT * FROM " + FLAGS_sst_backup_db + "." + FLAGS_sst_backup_tbl + " WHERE ";
    
    int cnt = 0;
    if (param->has_database()) {
        sql += " database = '";
        sql += param->database() + "'";
        cnt++;
    } 

    if (param->has_table_name()) {
        sql += " AND table_name = '";
        sql += param->table_name() + "'";
        cnt++;
    }

    if (cnt == 0) {
        DB_WARNING("database and table_name may be none");
        return -1;
    }

    sql += " order by id desc limit 10";
    baikal::client::ResultSet result_set;
    int ret = query_import(sql, result_set);
    if (ret < 0) {
        DB_WARNING("query sql:%s fail", sql.c_str());
        return -1;
    }
    DB_WARNING("query sql:%s", sql.c_str());

    while (result_set.next()) {
        std::string database;
        std::string table_name;
        std::string table_id;
        std::string start_time;
        std::string end_time;
        std::string status;
 
        result_set.get_string("database", &database);
        result_set.get_string("table_name", &table_name);
        result_set.get_string("table_id", &table_id);
        result_set.get_string("start_time", &start_time);
        result_set.get_string("end_time", &end_time);
        result_set.get_string("status", &status);

        DB_WARNING("sst task: database:%s, table_name:%s, table_id:%s, start_time:%s, end_time:%s, status:%s",
            database.c_str(), table_name.c_str(), table_id.c_str(), start_time.c_str(), end_time.c_str(), status.c_str());

        auto sst_task = response->add_sst_backup_task();
        sst_task->set_database(database);
        sst_task->set_table_name(table_name);
        sst_task->set_table_id(table_id);
        sst_task->set_start_time(start_time);
        sst_task->set_end_time(end_time);
        sst_task->set_status(status);
    }

    return 0;
}

int BaikalProxy::submit_sst_backup_task(pb::QueryParam * param, const pb::ConsoleRequest* request,
             pb::ConsoleResponse* response) {
    std::string meta_server_bns;
    std::string table_id;
    std::string database;
    std::string table_name;

    if (param->has_database()) {
        database = param->database();
    } else {
        DB_WARNING("param hasn`t database");
        return -1;
    }
    if (param->has_table_name()) {
        table_name = param->table_name();
    } else {
        DB_WARNING("param hasn`t table name");
        return -1;
    }
    if (param->has_meta_server_bns()) {
        meta_server_bns = param->meta_server_bns();
    } else {
        DB_WARNING("param hasn`t meta_server_bns");
        return -1;
    }
    if (param->has_table_id()) {
        table_id = param->table_id();
    } else {
        DB_WARNING("param hasn`t table id");
        return -1;
    }
    
    baikal::client::ResultSet result_set;
    std::string sql = "INSERT INTO " + FLAGS_sst_backup_db + "." + FLAGS_sst_backup_tbl + " (database, table_name, meta_server_bns, table_id) ";
    sql += "VALUES ('" + database + "', '" + table_name + "', '" + meta_server_bns + "', '" + table_id + "')"; 
    int ret = query_import(sql, result_set);
    if (ret < 0) {
        DB_WARNING("query sql:%s fail", sql.c_str());
        return -1;
    }
    DB_WARNING("query sql:%s succ", sql.c_str());
    return 0;
}
} //namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
