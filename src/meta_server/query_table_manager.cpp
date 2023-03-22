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

#include "query_table_manager.h"
#include "query_region_manager.h"
#include <boost/algorithm/string.hpp>
#include "region_manager.h"
#include "ddl_manager.h"
#include <unordered_set>

DEFINE_int64(show_table_status_cache_time, 3600 * 1000 * 1000LL, "show table status cache time : 3600s");
namespace baikaldb {
void QueryTableManager::get_schema_info(const pb::QueryRequest* request,
        pb::QueryResponse* response) {
    TableManager* manager = TableManager::get_instance();
    std::map<int64_t, std::vector<int64_t>> table_region_ids;
    {
        BAIDU_SCOPED_LOCK(manager->_table_mutex);
        if (!request->has_table_name()) {
            for (auto& table_mem : manager->_table_info_map) {
                if (request->has_namespace_name() && request->has_database()) {
                    if (table_mem.second.schema_pb.namespace_name() == request->namespace_name() &&
                            table_mem.second.schema_pb.database() == request->database() && 
                            !table_mem.second.is_global_index) {
                        auto table_pb = response->add_schema_infos();
                        *table_pb = table_mem.second.schema_pb;
                    }
                } else {
                    auto table_pb = response->add_schema_infos();
                    *table_pb = table_mem.second.schema_pb;
                }
            }
        } else {
            std::string namespace_name = request->namespace_name();
            std::string database = namespace_name + "\001" + request->database();
            std::string table_name = database + "\001" + request->table_name();
            if (manager->_table_id_map.find(table_name) == manager->_table_id_map.end()) {
                response->set_errmsg("table not exist");
                response->set_errcode(pb::INPUT_PARAM_ERROR);
                DB_WARNING("namespace: %s database: %s table_name: %s not exist",
                            namespace_name.c_str(), database.c_str(), table_name.c_str());
                return;
            }
            int64_t main_table_id = manager->_table_id_map[table_name];
            auto table_pb = response->add_schema_infos();
            *table_pb = manager->_table_info_map[main_table_id].schema_pb;

            std::vector<int64_t> global_index_ids;
            global_index_ids.push_back(main_table_id);
            for (auto& index_info : manager->_table_info_map[main_table_id].schema_pb.indexs()) {
                if (!manager->is_global_index(index_info)) {
                    continue;
                }
                global_index_ids.push_back(index_info.index_id());
            } 
            for (auto& index_id : global_index_ids) {
                for (auto& partition_region: manager->_table_info_map[index_id].partition_regions) {
                    for (auto& region_id : partition_region.second) {
                        table_region_ids[index_id].push_back(region_id);
                    }
                 }
            }
        }
    }
    for (auto& region_ids : table_region_ids) {
        std::vector<SmartRegionInfo> region_infos;
        RegionManager::get_instance()->get_region_info(region_ids.second, region_infos);
        for (auto& region_info : region_infos) {
            *(response->add_region_infos()) = *region_info; 
        }
    }
}

void QueryTableManager::get_flatten_schema(const pb::QueryRequest* request, 
            pb::QueryResponse* response) {
    TableManager* manager = TableManager::get_instance();
    std::string input_table_name = request->table_name();
    boost::trim(input_table_name);
    std::string input_database = request->database();
    boost::trim(input_database);
    std::string input_namespace_name = request->namespace_name();
    boost::trim(input_namespace_name);
    int64_t input_table_id = request->table_id();
    if ((input_table_name.size() == 0 
            || input_database.size() == 0 
            || input_namespace_name.size() == 0)
            && !request->has_table_id()) {
        response->set_errcode(pb::INPUT_PARAM_ERROR);
        response->set_errmsg("only query one table");
        return;
    }

    pb::SchemaInfo schema_pb;
    int ret = 0;
    if (request->has_table_id()) {
        ret = manager->get_table_info(input_table_id, schema_pb);
    } else {
        std::string full_table_name = input_namespace_name + "\001"
                                    + input_database + "\001"
                                    + input_table_name;
        ret = manager->get_table_info(full_table_name, schema_pb);
    }
    if (ret < 0) {
        response->set_errcode(pb::INPUT_PARAM_ERROR);
        response->set_errmsg("table not exist");
        return;
    }
    *(response->add_schema_infos()) = schema_pb;
    for (auto& field_info : schema_pb.fields()) {
        pb::QuerySchema field_schema;
        field_schema.set_field_or_index("Field");
        field_schema.set_column_name(field_info.field_name());
        field_schema.set_column_id(field_info.field_id());
        field_schema.set_column_type(pb::PrimitiveType_Name(field_info.mysql_type()));
        if (!field_info.has_can_null()) {
            field_schema.set_can_null(false);
        } else {
            field_schema.set_can_null(field_info.can_null());
        }
        std::string extra;
        if (field_info.auto_increment()) {
            extra = "auto_increment";
        }
        field_schema.set_extra(extra);
        if (!field_info.has_deleted()) {
            field_schema.set_deleted(false);
        } else {
            field_schema.set_deleted(field_info.deleted());
            field_schema.set_extra("Deleted");
        }
        field_schema.set_default_value(field_info.default_value());
        field_schema.set_comment(field_info.comment());
        auto record_ptr = response->add_flatten_schema_infos();
        *record_ptr = field_schema;
    }
    for (auto& index_info : schema_pb.indexs()) {
        pb::QuerySchema index_schema;
        index_schema.set_field_or_index("Index");
        //index_schema.set_field_or_index("Field");
        index_schema.set_column_name(index_info.index_name());
        index_schema.set_column_id(index_info.index_id());
        std::string type = pb::IndexType_Name(index_info.index_type());
        if (index_info.is_global()) {
            type += " GLOBAL";
        }
        if (index_info.index_type() == pb::I_FULLTEXT) {
            type += "(" + pb::SegmentType_Name(index_info.segment_type()) + ")";
        }
        //index_schema.set_column_type(pb::IndexType_Name(index_info.index_type()));
        index_schema.set_column_type(type);
        index_schema.set_can_null(false);
        std::string extra = "[";
        int i = 0;
        for (auto& field_name : index_info.field_names()) {
            int32_t field_id = index_info.field_ids(i);
            auto type = QueryTableManager::get_instance()->get_field_type(schema_pb.table_id(), field_id, schema_pb); 
            extra += field_name + " " + pb::PrimitiveType_Name(type) + ", "; 
            ++i;
        }
        extra[extra.size() - 2] = ']';
        index_schema.set_extra(extra);
        index_schema.set_deleted(false);
        index_schema.set_default_value("");
        index_schema.set_comment("");
        auto record_ptr = response->add_flatten_schema_infos();
        *record_ptr = index_schema;
    }
}

void QueryTableManager::construct_query_table(const TableMem& table_info, 
             pb::QueryTable* flatten_table_info) {
    TableManager* manager = TableManager::get_instance();
    int64_t table_id = table_info.schema_pb.table_id();
    flatten_table_info->set_namespace_name(table_info.schema_pb.namespace_name());
    flatten_table_info->set_database(table_info.schema_pb.database());
    flatten_table_info->set_table_name(table_info.schema_pb.table_name());
    flatten_table_info->set_upper_table_name(table_info.schema_pb.upper_table_name());
    flatten_table_info->set_region_size(table_info.schema_pb.region_size());
    flatten_table_info->set_replica_num(table_info.schema_pb.replica_num());
    flatten_table_info->set_resource_tag(table_info.schema_pb.resource_tag());
    flatten_table_info->set_max_field_id(table_info.schema_pb.max_field_id());
    flatten_table_info->set_version(table_info.schema_pb.version());
    flatten_table_info->set_status(table_info.schema_pb.status());
    flatten_table_info->set_partition_num(table_info.schema_pb.partition_num());
    flatten_table_info->set_table_id(table_id);
    flatten_table_info->set_byte_size_per_record(
                table_info.schema_pb.byte_size_per_record());
    int64_t region_count = 0;
    for (auto&partition_region : table_info.partition_regions) {
        region_count += partition_region.second.size();
    }
    flatten_table_info->set_region_count(region_count);
    flatten_table_info->set_row_count(manager->get_row_count(table_id));
    time_t t = table_info.schema_pb.timestamp();
    struct tm t_result;
    localtime_r(&t, &t_result);
    char s[100];
    strftime(s, sizeof(s), "%F %T", &t_result);
    flatten_table_info->set_create_time(s);
    if (table_info.schema_pb.has_deleted()) {
        flatten_table_info->set_deleted(false);
    } else {
        flatten_table_info->set_deleted(table_info.schema_pb.deleted());
    }
}

void QueryTableManager::get_flatten_table(const pb::QueryRequest* request, 
        pb::QueryResponse* response) {
    std::string input_table_name = request->table_name();
    boost::trim(input_table_name);
    std::string input_database = request->database();
    boost::trim(input_database);
    std::string input_namespace_name = request->namespace_name();
    boost::trim(input_namespace_name);
    std::string key = input_namespace_name + "." + input_database + "." + input_table_name;
    TableManager* manager = TableManager::get_instance();
    std::unordered_map<int64_t, TableMem> table_info_map_tmp;
    {
        BAIDU_SCOPED_LOCK(_mutex);
        if (_table_info_cache_time.find(key) != _table_info_cache_time.end()
              && butil::gettimeofday_us() - _table_info_cache_time[key] < FLAGS_show_table_status_cache_time) {
            // use cache data
            for (auto& table_info : _table_infos_cache[key]) {
                auto table = response->add_flatten_tables();
                *table = table_info.second;
            }
            DB_WARNING("use cache for show table status on db: %s", key.c_str());
            return;
        }
    }
    {
        BAIDU_SCOPED_LOCK(manager->_table_mutex);
        table_info_map_tmp = manager->_table_info_map;
    }
    std::map<std::string, pb::QueryTable> table_infos;
    for (auto& table_info : table_info_map_tmp) {
        if (input_table_name.size() != 0
            && table_info.second.schema_pb.table_name() != input_table_name) {
            continue;
        }
        if (input_database.size() != 0 
                && table_info.second.schema_pb.database() != input_database) {
            continue;
        }
        if (input_namespace_name.size() != 0 
                &&  table_info.second.schema_pb.namespace_name() != input_namespace_name) {
            continue;
        }
        std::string full_table_name = table_info.second.schema_pb.namespace_name() + "\001"
                                        + table_info.second.schema_pb.database() + "\001"
                                        + table_info.second.schema_pb.table_name();
        pb::QueryTable flatten_table_info;
        construct_query_table(table_info.second, &flatten_table_info);
        table_infos[full_table_name] = flatten_table_info;
    }
    for (auto& table_info : table_infos) {
        auto table = response->add_flatten_tables();
        *table = table_info.second;
    }
    {
        BAIDU_SCOPED_LOCK(_mutex);
        _table_info_cache_time[key] = butil::gettimeofday_us();
        _table_infos_cache[key] = table_infos;
    }
}

void QueryTableManager::check_table_and_update(
                  const std::unordered_map<int64_t, std::tuple<pb::SchemaInfo, int64_t, int64_t>> table_schema_map, 
                  std::unordered_map<int64_t, int64_t>& report_table_map,
                  pb::ConsoleHeartBeatResponse* response, uint64_t log_id) {
    for (auto& table_info_pair : table_schema_map) {
        int64_t table_id = table_info_pair.first;
        auto iter = report_table_map.find(table_id);
        bool need_update_info = false;
        if (iter != report_table_map.end()) {
            auto schema = std::get<0>(table_info_pair.second); 
            if (schema.version() > iter->second) {
                need_update_info = true;
            }
            report_table_map.erase(table_id);
        } else {
            need_update_info = true;
        }

        if (need_update_info) {
            auto table_info = response->add_table_change_infos();
            *(table_info->mutable_schema_info()) = std::get<0>(table_info_pair.second);
            table_info->set_region_count(std::get<1>(table_info_pair.second));
            table_info->set_main_table_id(std::get<2>(table_info_pair.second));
            table_info->set_table_id(table_id);
        } else {
            auto table_info = response->add_table_change_infos();
            table_info->set_region_count(std::get<1>(table_info_pair.second));
            table_info->set_main_table_id(std::get<2>(table_info_pair.second));
            table_info->set_table_id(table_id);
        }
    }
    
    for (auto& report_pair : report_table_map) {
        auto table_info = response->add_table_change_infos();
        table_info->set_table_id(report_pair.first);
        table_info->set_deleted(true);
    }
} 
        
void QueryTableManager::process_console_heartbeat(const pb::ConsoleHeartBeatRequest* request,
            pb::ConsoleHeartBeatResponse* response, uint64_t log_id) {
    TimeCost step_time_cost; 
    std::unordered_map<int64_t, int64_t> report_table_map;
    std::unordered_map<int64_t, pb::RegionHeartBeat> region_version_map;

    for (auto& table : request->table_versions()) {
        report_table_map[table.table_id()] = table.version();
    }
    for (auto& region : request->region_versions()) {
        region_version_map[region.region_id()] = region;
    }    
    int64_t parse_time = step_time_cost.get_time();
    step_time_cost.reset();
 
    TableManager* manager = TableManager::get_instance();
    std::unordered_map<int64_t, std::tuple<pb::SchemaInfo, int64_t, int64_t>> table_schema_map;
    {
        BAIDU_SCOPED_LOCK(manager->_table_mutex);
        for (auto& table_mem_pair : manager->_table_info_map) {
            int64_t region_count = 0;
            auto table_mem = table_mem_pair.second;
            for (auto&partition_region : table_mem.partition_regions) {
                region_count += partition_region.second.size();
            }
            table_schema_map[table_mem_pair.first] = std::make_tuple(table_mem.schema_pb,
                  region_count, table_mem.main_table_id); 
        }
    }

    int64_t prepare_time = step_time_cost.get_time();
    step_time_cost.reset(); 
     
    check_table_and_update(table_schema_map, report_table_map, response, log_id);

    int64_t update_table_time = step_time_cost.get_time();
    step_time_cost.reset();
    
    QueryRegionManager::get_instance()->check_region_and_update(region_version_map, response);

    int64_t update_region_time = step_time_cost.get_time();
    DB_NOTICE("process schema info for console heartbeat, parse_time:%ld, prepare_time: %ld, "
              "update_table_time: %ld, update_region_time: %ld, log_id: %lu",
               parse_time, prepare_time, update_table_time, update_region_time, log_id);
}

pb::PrimitiveType QueryTableManager::get_field_type(
                    int64_t table_id, 
                    int32_t field_id, 
                    const pb::SchemaInfo& table_info) {
     for (auto& field_info : table_info.fields()) {
        if (field_info.field_id() == field_id) {
            return field_info.mysql_type();
        }
     }
     return pb::INVALID_TYPE;
}

void QueryTableManager::get_primary_key_string(int64_t table_id, std::string& primary_key_string) {
    pb::SchemaInfo table_info;
    auto ret = TableManager::get_instance()->get_table_info(table_id, table_info);
    if (ret < 0) {
        primary_key_string = "table has been deleted";
        return;
    }   
    for (auto& index_info : table_info.indexs()) {
        if (index_info.index_type() != pb::I_PRIMARY) {
            continue;
        }   
        for (int idx = 0; idx < index_info.field_names_size(); ++idx) {
            primary_key_string += index_info.field_names(idx); 
            pb::PrimitiveType field_type = get_field_type(table_id, index_info.field_ids(idx), table_info);
            primary_key_string += " " + pb::PrimitiveType_Name(field_type) + ","; 
        }
    }
    if (primary_key_string.size() > 1) {
        primary_key_string.erase(primary_key_string.size() - 1);
    }
}

void QueryTableManager::decode_key(int64_t table_id, const TableKey& start_key, std::string& start_key_string) {
    pb::SchemaInfo table_info;
    auto ret = TableManager::get_instance()->get_table_info(table_id, table_info);
    if (ret < 0) {
        start_key_string = "table has been deleted";
        return;
    }
    for (auto& index_info : table_info.indexs()) {
        int32_t pos = 0;
        if (index_info.index_type() != pb::I_PRIMARY) {
            continue;
        }
        for (int idx = 0; idx < index_info.field_names_size(); ++idx) {
            pb::PrimitiveType field_type = get_field_type(table_id, index_info.field_ids(idx), table_info);
            start_key_string += start_key.decode_start_key_string(field_type, pos);
            start_key_string += ",";
        }
    }
    if (start_key_string.size() > 1) {
        start_key_string.pop_back();
    }
}

void QueryTableManager::get_ddlwork_info(const pb::QueryRequest* request, pb::QueryResponse* response) {
    DDLManager::get_instance()->get_ddlwork_info(request->table_id(), response);
}

void QueryTableManager::get_virtual_index_influence_info(const pb::QueryRequest* request, pb::QueryResponse* response){
    //将meta的tableMem中影响面信息存入response同时返还给db
    TableManager* manager = TableManager::get_instance();
    auto virtual_index_sql_map = manager->get_virtual_index_id_set();
    for (auto& it1 : virtual_index_sql_map) {
        for (auto& it2 : it1.second) {
            pb::VirtualInfoAndSqls virtual_index_info_sqls;
            virtual_index_info_sqls.set_virtual_index_info(it1.first);
            virtual_index_info_sqls.set_affected_sign(it2.first);
            virtual_index_info_sqls.set_affected_sqls(it2.second);
            pb::VirtualInfoAndSqls* virtual_index_influence_info = response->add_virtual_index_influence_info();
            *virtual_index_influence_info = virtual_index_info_sqls;
        }
    }
}

void QueryTableManager::get_table_in_fast_importer(const pb::QueryRequest* request, pb::QueryResponse* response){
    TableManager* manager = TableManager::get_instance();
    std::unordered_map<int64_t, int64_t> tables_ts;
    manager->get_table_fast_importer_ts(tables_ts);
    for (auto& tb : tables_ts) {
        pb::SchemaInfo info;
        manager->get_table_info(tb.first, info);
        pb::QueryTable* table_info = response->add_flatten_tables();
        table_info->set_table_id(tb.first);
        table_info->set_namespace_name(info.namespace_name());
        table_info->set_database(info.database());
        table_info->set_table_name(info.table_name());
        table_info->set_upper_table_name(info.upper_table_name());
        table_info->set_region_size(info.region_size());
        table_info->set_replica_num(info.replica_num());
        table_info->set_resource_tag(info.resource_tag());
        table_info->set_max_field_id(info.max_field_id());
        table_info->set_version(info.version());
        table_info->set_status(info.status());
        table_info->set_deleted(info.deleted());
        table_info->set_byte_size_per_record(info.byte_size_per_record());
        table_info->set_fast_importer_ts(tb.second);
    }
}
}//namespace 

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
