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

#include "query_table_manager.h"
#include <boost/algorithm/string.hpp>
#include "region_manager.h"

namespace baikaldb {
void QueryTableManager::get_schema_info(const pb::QueryRequest* request,
        pb::QueryResponse* response) {
    TableManager* manager = TableManager::get_instance();
    std::vector<int64_t> region_ids;
    {
        BAIDU_SCOPED_LOCK(manager->_table_mutex);
        if (!request->has_table_name()) {
            for (auto& table_mem : manager->_table_info_map) {
                 auto table_pb = response->add_schema_infos();
                 *table_pb = table_mem.second.schema_pb;
            }
        } else {
            std::string namespace_name = request->namespace_name();
            std::string database = namespace_name + "\001" + request->database();
            std::string table_name = database + "\001" + request->table_name();
            if (manager->_table_id_map.find(table_name) != manager->_table_id_map.end()) {
                int64_t id = manager->_table_id_map[table_name];
                auto table_pb = response->add_schema_infos();
                *table_pb = manager->_table_info_map[id].schema_pb;
                for (auto& partition_regions : manager->_table_info_map[id].partition_regions) {
                    for (auto& region_id :  partition_regions.second) { 
                        region_ids.push_back(region_id);
                    }
                }
            } else {
                response->set_errmsg("namespace not exist");
                response->set_errcode(pb::INPUT_PARAM_ERROR);
                return;
            }
        }
    }
    std::vector<SmartRegionInfo> region_infos;
    RegionManager::get_instance()->get_region_info(region_ids, region_infos);
    for (auto& region_info : region_infos) {
        *(response->add_region_infos()) = *region_info; 
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
    if (input_table_name.size() == 0 
            || input_database.size() == 0 
            || input_namespace_name.size() == 0) {
        response->set_errcode(pb::INPUT_PARAM_ERROR);
        response->set_errmsg("only query one table");
        return;
    }
    std::string full_table_name = input_namespace_name + "\001"
                                    + input_database + "\001"
                                    + input_table_name;
    pb::SchemaInfo schema_pb;
    auto ret = manager->get_table_info(full_table_name, schema_pb);
    if (ret < 0) {
        response->set_errcode(pb::INPUT_PARAM_ERROR);
        response->set_errmsg("table not exist");
        return;
    }
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
        }
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
        if (index_info.index_type() == pb::I_FULLTEXT) {
            type += "(" + pb::SegmentType_Name(index_info.segment_type()) + ")";
        }
        index_schema.set_column_type(pb::IndexType_Name(index_info.index_type()));
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
        auto record_ptr = response->add_flatten_schema_infos();
        *record_ptr = index_schema;
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
    TableManager* manager = TableManager::get_instance();
    std::unordered_map<int64_t, TableMem> table_info_map_tmp;
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
        int64_t table_id = table_info.second.schema_pb.table_id();
        flatten_table_info.set_namespace_name(table_info.second.schema_pb.namespace_name());
        flatten_table_info.set_database(table_info.second.schema_pb.database());
        flatten_table_info.set_table_name(table_info.second.schema_pb.table_name());
        flatten_table_info.set_upper_table_name(table_info.second.schema_pb.upper_table_name());
        flatten_table_info.set_region_size(table_info.second.schema_pb.region_size());
        flatten_table_info.set_replica_num(table_info.second.schema_pb.replica_num());
        flatten_table_info.set_resource_tag(table_info.second.schema_pb.resource_tag());
        flatten_table_info.set_max_field_id(table_info.second.schema_pb.max_field_id());
        flatten_table_info.set_version(table_info.second.schema_pb.version());
        flatten_table_info.set_status(table_info.second.schema_pb.status());
        flatten_table_info.set_table_id(table_id);
        flatten_table_info.set_byte_size_per_record(
                table_info.second.schema_pb.byte_size_per_record());
        int64_t region_count = 0;
        for (auto&partition_region : table_info.second.partition_regions) {
            region_count += partition_region.second.size();
        }
        flatten_table_info.set_region_count(region_count);
        flatten_table_info.set_row_count(manager->get_row_count(table_id));
        time_t t = table_info.second.schema_pb.timestamp();
        struct tm t_result;
        localtime_r(&t, &t_result);
        char s[100];
        strftime(s, sizeof(s), "%F %T", &t_result);
        flatten_table_info.set_create_time(s);
        if (table_info.second.schema_pb.has_deleted()) {
            flatten_table_info.set_deleted(false);
        } else {
            flatten_table_info.set_deleted(table_info.second.schema_pb.deleted());
        }
        table_infos[full_table_name] = flatten_table_info;
    }
    for (auto& table_info : table_infos) {
        auto table = response->add_flatten_tables();
        *table = table_info.second;
    }
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
            switch (field_type) {
                case pb::INT8: {
                    if (pos + sizeof(int8_t) > start_key.size()) {
                        DB_WARNING("int8_t pos out of bound: %d %zu", pos, start_key.size());
                        start_key_string += "decode fail";
                        return;
                    }
                    start_key_string += std::to_string(start_key.extract_i8(pos));
                    start_key_string += ",";
                    pos += sizeof(int8_t);
                } break;
                case pb::INT16: {
                    if (pos + sizeof(int16_t) > start_key.size()) {
                        DB_WARNING("int16_t pos out of bound: %d %zu", pos, start_key.size());
                        start_key_string += "decode fail";
                        return;
                    }
                    start_key_string += std::to_string(start_key.extract_i16(pos));
                    start_key_string += ",";
                    pos += sizeof(int16_t);
                } break;
                case pb::TIME:
                case pb::INT32: {
                    if (pos + sizeof(int32_t) > start_key.size()) {
                        DB_WARNING("int32_t pos out of bound: %d %zu", pos, start_key.size());
                        start_key_string += "decode fail";
                        return;
                    }
                    start_key_string += std::to_string(start_key.extract_i32(pos));
                    start_key_string += ",";
                    pos += sizeof(int32_t);
                } break;
                case pb::UINT8: {
                    if (pos + sizeof(uint8_t) > start_key.size()) {
                        DB_WARNING("uint8_t pos out of bound: %d %zu", pos, start_key.size());
                        start_key_string += "decode fail";
                        return;
                    }
                    start_key_string += std::to_string(start_key.extract_u8(pos));
                    start_key_string += ",";
                    pos += sizeof(uint8_t);
                } break;
                case pb::UINT16: {
                    if (pos + sizeof(uint16_t) > start_key.size()) {
                        DB_WARNING("uint16_t pos out of bound: %d %zu", pos, start_key.size());
                        start_key_string += "decode fail";
                        return;
                    }
                    start_key_string += std::to_string(start_key.extract_u16(pos));
                    start_key_string += ",";
                    pos += sizeof(uint16_t);
                } break;
                case pb::UINT32:
                case pb::TIMESTAMP:
                case pb::DATE: {
                    if (pos + sizeof(uint32_t) > start_key.size()) {
                        DB_WARNING("uint32_t pos out of bound: %d %zu", pos, start_key.size());
                        start_key_string += "decode fail";
                        return;
                    }
                    start_key_string += std::to_string(start_key.extract_u32(pos));
                    start_key_string += ",";
                    pos += sizeof(uint32_t);
                } break;
                case pb::INT64: {
                    if (pos + sizeof(int64_t) > start_key.size()) {
                        DB_WARNING("int64_t pos out of bound: %d %zu", pos, start_key.size());
                        start_key_string += "decode fail";
                        return;
                    }
                    start_key_string += std::to_string(start_key.extract_i64(pos));
                    start_key_string += ",";
                    pos += sizeof(int64_t);
                } break;
                case pb::UINT64: 
                case pb::DATETIME: {
                    if (pos + sizeof(uint64_t) > start_key.size()) {
                        DB_WARNING("int64_t pos out of bound: %d %zu", pos, start_key.size());
                        start_key_string += "decode fail";
                        return;
                    }
                    start_key_string += std::to_string(start_key.extract_u64(pos));
                    start_key_string += ",";
                    pos += sizeof(uint64_t);
                } break;
                case pb::FLOAT: {
                    if (pos + sizeof(float) > start_key.size()) {
                        DB_WARNING("float pos out of bound: %d %zu", pos, start_key.size());
                        start_key_string += "decode fail";
                        return;
                    }
                    start_key_string += std::to_string(start_key.extract_float(pos));
                    pos += sizeof(float);
                } break;
                case pb::DOUBLE: {
                    if (pos + sizeof(double) > start_key.size()) {
                        DB_WARNING("double pos out of bound: %d %zu", pos, start_key.size());
                        start_key_string += "decode fail";
                        return;
                    }
                    start_key_string += std::to_string(start_key.extract_double(pos));
                    pos += sizeof(double);
                } break;
                case pb::STRING: {
                    if (pos >= (int)start_key.size()) {
                        DB_WARNING("string pos out of bound: %d %zu", pos, start_key.size());
                        start_key_string += "decode fail";
                        return;
                    }
                    std::string str;
                    start_key.extract_string(pos, str);
                    start_key_string += str;
                    pos += (str.size() + 1);
                } break;
                case pb::BOOL: {
                    if (pos + sizeof(uint8_t) > start_key.size()) {
                        DB_WARNING("string pos out of bound: %d %zu", pos, start_key.size());
                        start_key_string += "decode fail";
                        return;
                    }
                    start_key_string += std::to_string(start_key.extract_boolean(pos));
                    pos += sizeof(uint8_t);
                } break;
                default: {
                    DB_WARNING("unsupport type:%d", field_type);
                    start_key_string += "unsupport type";
                    return;
                }
            }
        }
    }
    if (start_key_string.size() > 1) {
        start_key_string.erase(start_key_string.size() - 1);
    }
}

}//namespace 

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
