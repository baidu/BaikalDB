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

#include "common.h"
#include "datetime.h"
#include "meta_proxy.h"
#include "console_util.h"

namespace baikaldb {

DEFINE_int32(console_heartbeat_interval, 20 * 1000 * 1000, 
        "connection idle timeout threshold (second)");
DEFINE_int32(truncate_table_interval, 20, 
        "truncate statistics table before # heartbeat times");
DECLARE_string(meta_server_bns);

int MetaProxy::init(BaikalProxy* baikaldb, std::vector<std::string>& platform_tags) {
    _baikaldb = baikaldb;
    std::vector<std::string> meta_bns_v = string_split(FLAGS_meta_server_bns, ';');
    if (meta_bns_v.size() != platform_tags.size()) {
        DB_WARNING("bns size != platform_tags size");
        return -1;
    }
    for (size_t i = 0; i < meta_bns_v.size(); i++) {
        auto& tag = platform_tags[i];
        _meta_interact_map[tag] = new MetaServerInteract;
        std::string& bns = meta_bns_v[i];
        if (_meta_interact_map[tag]->init_internal(bns) < 0) {
            DB_WARNING("init meta server platform_tag: %s bns: %s failed.", tag.c_str(), bns.c_str());
            return -1;
        }
        DB_WARNING("init meta server platform_tag: %s bns: %s success.", tag.c_str(), bns.c_str());
    }
    _heartbeat_bth.run([this]() {report_heart_beat();});
    return 0;
}

void MetaProxy::stop() {
    _shutdown = true;
    _heartbeat_bth.join();
}

pb::PrimitiveType MetaProxy::get_field_type(
                    int32_t field_id, 
                    const pb::SchemaInfo& table_info) {
     for (auto& field_info : table_info.fields()) {
        if (field_info.field_id() == field_id) {
            return field_info.mysql_type();
        }
     }
     return pb::INVALID_TYPE;
}

void MetaProxy::generate_key(const pb::IndexInfo& index_info,
                             const pb::SchemaInfo& table_info,
                             const TableKey& start_key,
                             std::string& start_key_string,
                             std::vector<int32_t>& field_ids,
                             int32_t& pos) {
    for (auto id : field_ids) {
        auto field_type = get_field_type(id, table_info);
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
                if (field_type == pb::TIME) {
                    start_key_string += time_to_str(start_key.extract_i32(pos));
                } else {
                    start_key_string += std::to_string(start_key.extract_i32(pos));
                }
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
                if (field_type == pb::UINT32) {
                    start_key_string += std::to_string(start_key.extract_u32(pos));
                } else if (field_type == pb::TIMESTAMP) {
                    start_key_string += std::string(timestamp_to_str(start_key.extract_u32(pos)));
                } else {
                    start_key_string += date_to_str(start_key.extract_u32(pos));
                }
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
                if (field_type == pb::UINT64) {
                    start_key_string += std::to_string(start_key.extract_u64(pos));
                } else {
                    start_key_string += datetime_to_str(start_key.extract_u64(pos));
                }
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
                start_key_string += ",";
                pos += sizeof(float);
            } break;
            case pb::DOUBLE: {
                if (pos + sizeof(double) > start_key.size()) {
                    DB_WARNING("double pos out of bound: %d %zu", pos, start_key.size());
                        start_key_string += "decode fail";
                    return;
                }
                start_key_string += std::to_string(start_key.extract_double(pos));
                start_key_string += ",";
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
                start_key_string += ",";
                pos += (str.size() + 1);
            } break;
            case pb::BOOL: {
                if (pos + sizeof(uint8_t) > start_key.size()) {
                    DB_WARNING("string pos out of bound: %d %zu", pos, start_key.size());
                        start_key_string += "decode fail";
                    return;
                }
                start_key_string += std::to_string(start_key.extract_boolean(pos));
                start_key_string += ",";
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

void MetaProxy::decode_key(const int64_t table_id, const pb::SchemaInfo& table_info,
                 const TableKey& start_key, std::string& start_key_string) {
    pb::IndexInfo pri_info;
    pb::IndexInfo comm_info;
    std::vector<int32_t> pk_field_ids;
    std::vector<int32_t> key_field_ids;
    bool variable_length_type = false;
    for (auto& index_info : table_info.indexs()) {
        if (index_info.index_type() == pb::I_PRIMARY) {
            pri_info = index_info;
            for (int idx = 0; idx < pri_info.field_names_size(); ++idx) {
                pk_field_ids.push_back(pri_info.field_ids(idx));
                auto field_type = get_field_type(pri_info.field_ids(idx), table_info);
                if (field_type == pb::STRING || field_type == pb::HLL) {
                    variable_length_type = true;
                }
            }
        } else if (index_info.is_global() && index_info.index_id() == table_id) {
            comm_info = index_info;
            for (int idx = 0; idx < comm_info.field_names_size(); ++idx) {
                key_field_ids.push_back(comm_info.field_ids(idx));
            }            
        }
    }
    int32_t pos = 0;
    if (pri_info.index_id() == table_id) {
        generate_key(pri_info, table_info, start_key, start_key_string, pk_field_ids, pos);
    } else {
        pos += sizeof(uint8_t); // null_flag
        generate_key(comm_info, table_info, start_key, start_key_string, key_field_ids, pos);
        if (comm_info.index_type() == pb::I_KEY && !variable_length_type) {
            for (auto id : key_field_ids) {
                for (auto it = pk_field_ids.begin(); it != pk_field_ids.end(); ++it) {
                    if (*it == id) {
                        pk_field_ids.erase(it);
                        break;
                    }
                }
            }
            generate_key(pri_info, table_info, start_key, start_key_string, pk_field_ids, pos);
        }
    }
    if (start_key_string.size() > 1) {
        start_key_string.erase(start_key_string.size() - 1);
    }
}

void MetaProxy::construct_query_table(const pb::TableInfo& table_info, 
             pb::QueryTable* flatten_table_info) {
    auto schema_info = table_info.schema_info();
    flatten_table_info->set_namespace_name(schema_info.namespace_name());
    flatten_table_info->set_database(schema_info.database());
    if (table_info.table_id() != table_info.main_table_id()) {
        for (auto index : schema_info.indexs()) {
            if (index.index_id() == table_info.table_id()) {
                std::string table_name = schema_info.table_name() +"_" + index.index_name();
                flatten_table_info->set_table_name(table_name);
            }
        }
    } else {
        flatten_table_info->set_table_name(schema_info.table_name());
    }
    flatten_table_info->set_upper_table_name(schema_info.upper_table_name());
    flatten_table_info->set_region_size(schema_info.region_size());
    flatten_table_info->set_replica_num(schema_info.replica_num());
    flatten_table_info->set_resource_tag(schema_info.resource_tag());
    flatten_table_info->set_max_field_id(schema_info.max_field_id());
    flatten_table_info->set_region_split_lines(schema_info.region_split_lines());
    flatten_table_info->set_version(schema_info.version());
    flatten_table_info->set_status(schema_info.status());
    flatten_table_info->set_byte_size_per_record(schema_info.byte_size_per_record());
    flatten_table_info->set_region_count(table_info.region_count());
    flatten_table_info->set_row_count(table_info.row_count());
    flatten_table_info->set_table_id(table_info.table_id());
    flatten_table_info->set_main_table_id(table_info.main_table_id());
    flatten_table_info->set_create_time(timestamp_to_str(schema_info.timestamp()));
    if (schema_info.has_deleted()) {
        flatten_table_info->set_deleted(false);
    } else {
        flatten_table_info->set_deleted(schema_info.deleted());
    }
}

void MetaProxy::construct_query_region(const std::string& meta_server_tag,
             const pb::RegionInfo& region_info,
             pb::QueryRegion* query_region_info) {
    int64_t table_id = region_info.table_id();
    query_region_info->set_region_id(region_info.region_id());
    query_region_info->set_table_id(table_id);
    query_region_info->set_main_table_id(region_info.main_table_id());
    query_region_info->set_table_name(region_info.table_name());
    query_region_info->set_partition_id(region_info.partition_id());
    query_region_info->set_replica_num(region_info.replica_num());
    query_region_info->set_version(region_info.version());
    query_region_info->set_conf_version(region_info.conf_version());
    query_region_info->set_parent(region_info.parent());
    query_region_info->set_num_table_lines(region_info.num_table_lines());
    time_t t = region_info.timestamp();
    struct tm t_result;
    localtime_r(&t, &t_result);
    char s[100];
    strftime(s, sizeof(s), "%F %T", &t_result);
    query_region_info->set_create_time(s);
    std::string start_key_string;
    std::string end_key_string;
    std::string raw_start_key = str_to_hex(std::string(region_info.start_key()));;
    auto table_schema_mapping = _table_schema_mapping[meta_server_tag];
    auto iter = table_schema_mapping.find(table_id);
    bool found = false;
    if (iter != table_schema_mapping.end()) {
        found = true;
    }
    if (region_info.start_key() == "") {
        start_key_string = "-oo";
    } else {
        if (found) {
            decode_key(table_id, iter->second, TableKey(region_info.start_key()), start_key_string);
        } else {
            start_key_string = "table has been deleted";
        }
    }
    if (region_info.end_key() == "") {
        end_key_string = "+oo";
    } else {
        if (found) {
            decode_key(table_id, iter->second, TableKey(region_info.end_key()), end_key_string);
        } else {
            end_key_string = "table has been deleted";
        }
    }
    query_region_info->set_start_key(start_key_string);
    query_region_info->set_end_key(end_key_string);
    query_region_info->set_raw_start_key(raw_start_key);
    std::string peers;
    for (auto& peer : region_info.peers()) {
        peers = peers + peer + ", ";
    }
    if (peers.size() > 2) {
        peers.pop_back();
        peers.pop_back();
    } else {
        peers.clear();
    }
    query_region_info->set_peers(peers);
    query_region_info->set_leader(region_info.leader());
    query_region_info->set_status(region_info.status());
    query_region_info->set_used_size(region_info.used_size());
    query_region_info->set_log_index(region_info.log_index());
    if (!region_info.has_deleted()) {
        query_region_info->set_deleted(false);
    } else {
        query_region_info->set_deleted(region_info.deleted());
    }   
    query_region_info->set_can_add_peer(region_info.can_add_peer());
}

void MetaProxy::construct_heart_beat_request(const std::string& meta_server_tag,
             pb::ConsoleHeartBeatRequest& request) {
    auto table_region_version_mapping = _table_region_version_mapping[meta_server_tag];
    for (auto& region_info : table_region_version_mapping) {
        auto region = request.add_region_versions();
        region->set_region_id(region_info.first);
        region->set_version(std::get<0>(region_info.second));
        region->set_conf_version(std::get<1>(region_info.second));
        region->set_leader(std::get<2>(region_info.second));
        region->set_num_table_lines(std::get<3>(region_info.second));
    }
    auto table_version_mapping = _table_version_mapping[meta_server_tag];
    for (auto& table_info : table_version_mapping) {
        auto table = request.add_table_versions();
        table->set_table_id(table_info.first);
        table->set_version(table_info.second);
    }
}

void MetaProxy::process_heart_beat_response(const std::string& meta_server_tag,
             const pb::ConsoleHeartBeatResponse& response) {
    const int batch_insert_item_number = 15;
    TimeCost step_time_cost; 
    std::vector<pb::QueryTable> table_infos;
    int64_t table_id = 0;
    ConcurrencyBthread update_bth(30, &BTHREAD_ATTR_SMALL);
    auto iter = _baikaldb->plat_databases_mapping.find(meta_server_tag);
    if (iter == _baikaldb->plat_databases_mapping.end()) {
        DB_WARNING("meta server tag:%s not found corresponding database ", meta_server_tag.c_str());
        return ;
    }

    std::string database_name  = iter->second;

    for (auto& table_info : response.table_change_infos()) {
        table_id = table_info.table_id();
        if (table_info.has_schema_info()) {
            pb::SchemaInfo schema_info = table_info.schema_info();
            _table_version_mapping[meta_server_tag][table_id] = schema_info.version();
            _table_schema_mapping[meta_server_tag][table_id] = schema_info;
            pb::QueryTable flatten_table_info;
            construct_query_table(table_info, &flatten_table_info);
            table_infos.push_back(flatten_table_info);
            if (table_infos.size() == batch_insert_item_number) {
                _baikaldb->insert_table_info(database_name, table_infos);
                table_infos.clear();
            }
        } else if (table_info.has_deleted() && table_info.deleted()) {
            _table_version_mapping[meta_server_tag].erase(table_id);
            auto delete_table_info_func = [this, database_name, table_id]() {
                _baikaldb->delete_table_and_region(database_name, table_id);
            };    
            update_bth.run(delete_table_info_func);
        } else {
            int64_t region_count = table_info.region_count();
            auto update_table_stat_func = [this, database_name, table_id, region_count]() {
                _baikaldb->update_table_count(database_name, table_id, region_count);
            };
            update_bth.run(update_table_stat_func);
        }
    }
    if (table_infos.size() > 0) {
        _baikaldb->insert_table_info(database_name, table_infos);
    }

    update_bth.join();
    int64_t table_time = step_time_cost.get_time();
    step_time_cost.reset();

    std::vector<pb::QueryRegion> region_infos;
    for (auto& region_change : response.region_change_infos()) {
        if (region_change.has_region_info()) {
            auto region = region_change.region_info();
            int64_t region_id = region.region_id();
            int64_t version = region.version();
            int64_t conf_version = region.conf_version();
            int64_t num_table_lines = region.num_table_lines();
            std::string leader = region.leader();
            _table_region_version_mapping[meta_server_tag][region_id] =
                  std::make_tuple(version, conf_version, leader, num_table_lines);
            pb::QueryRegion query_region_info;
            construct_query_region(meta_server_tag, region, &query_region_info);
            region_infos.push_back(query_region_info);
            if (region_infos.size() == batch_insert_item_number) {
                _baikaldb->insert_region_info(database_name, region_infos);
                region_infos.clear();
            }
        } else {
            RegionStat stat;
            stat.region_id = region_change.region_id();
            stat.used_size = region_change.used_size();
            stat.num_table_lines = region_change.num_table_lines();
            auto& tuple = _table_region_version_mapping[meta_server_tag][stat.region_id];
            std::get<3>(tuple) = stat.num_table_lines;
            auto update_region_stat_func = [this, database_name, stat]() {
                _baikaldb->update_region_info(database_name, stat);
            };
            update_bth.run(update_region_stat_func);
        }
    }
    if (region_infos.size() > 0) {
        _baikaldb->insert_region_info(database_name, region_infos);
    }

    update_bth.join();
    int64_t region_time = step_time_cost.get_time();
    step_time_cost.reset();

    std::vector<pb::QueryUserPrivilege> user_infos;
    for (auto& info : response.flatten_privileges()) {
        user_infos.push_back(info);
        if (user_infos.size() == batch_insert_item_number) {
            _baikaldb->insert_user_info(database_name, user_infos);
            user_infos.clear();
        }
    }
    if (user_infos.size() > 0) {
        _baikaldb->insert_user_info(database_name, user_infos);
    }

    int64_t privilege_time = step_time_cost.get_time();
    step_time_cost.reset();
    
    if (_heartbeat_times % FLAGS_truncate_table_interval == 0) {
        for (auto database_name : _baikaldb->plat_databases_mapping) {
            _baikaldb->truncate_table(database_name.second, "instance");
        }
    }
    _heartbeat_times++;
 
    std::vector<pb::QueryInstance> instance_infos;
    for (auto& info : response.flatten_instances()) {
        instance_infos.push_back(info);
        if (instance_infos.size() == batch_insert_item_number) {
            _baikaldb->insert_instance_info(database_name, instance_infos);
            instance_infos.clear();
        }
    }
    if (instance_infos.size() > 0) {
        _baikaldb->insert_instance_info(database_name, instance_infos);
    }

    int64_t instance_time = step_time_cost.get_time();
    DB_WARNING("database_name:%s table_size:%d region_size:%d privilege_size:%d instance_size:%d table_time:%ld"
               " region_time:%ld, privilege_time:%lld, instance_time:%ld", database_name.c_str(),
               response.table_change_infos_size(),
               response.region_change_infos_size(),
               response.flatten_privileges_size(),
               response.flatten_instances_size(),
               table_time, region_time, privilege_time, instance_time);
}

void MetaProxy::report_heart_beat() {
    while (!_shutdown) {
        pb::ConsoleHeartBeatRequest request;
        pb::ConsoleHeartBeatResponse response;
        for (const auto& meta_server :_meta_interact_map) {
            construct_heart_beat_request(meta_server.first, request);

            if (meta_server.second->send_request("console_heartbeat", request, response) == 0) {
                process_heart_beat_response(meta_server.first, response);
            } else {
                DB_WARNING("send heart beat request to meta server fail");
            }
        }
        bthread_usleep(FLAGS_console_heartbeat_interval);
    }
}

int MetaProxy::query_schema_info(const pb::QueryParam* param, pb::ConsoleResponse* response) {
    if (_shutdown) {
        return -1;
    }    

    std::string full_table_name;
    int ret = 0;
    ret = _baikaldb->get_full_table_name(param, full_table_name);
    if (ret < 0) {
        return ret;
    }
    
    auto table_schemas = response->mutable_table_schemas();
    table_schemas->set_full_table_name(full_table_name);
    pb::QueryRequest request;
    pb::QueryResponse meta_response;

    request.set_op_type(pb::QUERY_SCHEMA_FLATTEN);
    if (param->has_table_id()) {
        request.set_table_id(std::stol(param->table_id()));
    } else {
        request.set_namespace_name(param->namespace_name());
        request.set_database(param->database());
        request.set_table_name(param->table_name());
    }

    if (_meta_interact_map[param->platform()]->send_request("query", request, meta_response) == 0) {
        for (auto& schema : meta_response.flatten_schema_infos()) {
            *(table_schemas->add_schemas()) = schema;
        }
    } else {
        DB_WARNING("send query request to meta server fail");
        return -1;
    }

    return 0;
}


}// namespace baikaldb

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
