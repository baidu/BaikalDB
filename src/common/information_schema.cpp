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

#include "information_schema.h"
#include <boost/algorithm/string/join.hpp>
#include "runtime_state.h"
#include "meta_server_interact.hpp"
#include "schema_factory.h"
#include "network_socket.h"
#include "scalar_fn_call.h"
#include "parser.h"

namespace baikaldb {
int InformationSchema::init() {
    init_partition_split_info();
    init_region_status();
    init_columns();
    init_statistics();
    init_schemata();
    init_tables();
    init_virtual_index_influence_info();
    init_sign_list();
    init_routines();
    init_key_column_usage();
    init_referential_constraints();
    init_triggers();
    init_views();
    init_character_sets();
    init_collation_character_set_applicability();
    init_collations();
    init_column_privileges();
    init_engines();
    init_events();
    init_files();
    init_global_status();
    init_global_variables();
    init_innodb_buffer_page();
    init_innodb_buffer_page_lru();
    init_innodb_buffer_pool_stats();
    init_innodb_cmp();
    init_innodb_cmpmem();
    init_innodb_cmpmem_reset();
    init_innodb_cmp_per_index();
    init_innodb_cmp_per_index_reset();
    init_innodb_cmp_reset();
    init_innodb_ft_being_deleted();
    init_innodb_ft_config();
    init_innodb_ft_default_stopword();
    init_innodb_ft_deleted();
    init_innodb_ft_index_cache();
    init_innodb_ft_index_table();
    init_innodb_locks();
    init_innodb_lock_waits();
    init_innodb_metrics();
    init_innodb_sys_columns();
    init_innodb_sys_datafiles();
    init_innodb_sys_fields();
    init_innodb_sys_foreign();
    init_innodb_sys_foreign_cols();
    init_innodb_sys_indexes();
    init_innodb_sys_tables();
    init_innodb_sys_tablespaces();
    init_innodb_sys_tablestats();
    init_innodb_trx();
    init_optimizer_trace();
    init_parameters();
    init_partitions();
    init_plugins();
    init_processlist();
    init_profiling();
    init_schema_privileges();
    init_session_status();
    init_session_variables();
    init_table_constraints();
    init_table_privileges();
    init_tablespaces();
    init_user_privileges();
    return 0;
}

int64_t InformationSchema::construct_table(const std::string& table_name, FieldVec& fields) {
    auto& table = _tables[table_name];//_tables[table_name]取出的是Schema_info
    table.set_table_id(--_max_table_id);
    table.set_table_name(table_name);
    table.set_database("information_schema");
    table.set_database_id(_db_id);
    table.set_namespace_name("INTERNAL");
    table.set_engine(pb::INFORMATION_SCHEMA);
    int id = 0;
    for (auto& pair : fields) {
        auto* field = table.add_fields();
        field->set_field_name(pair.first);
        field->set_mysql_type(pair.second);
        field->set_field_id(++id);
    }
    SchemaFactory::get_instance()->update_table(table);
    return table.table_id();
}
void InformationSchema::init_partition_split_info() {
    // 定义字段信息
    FieldVec fields {
        {"partition_key", pb::STRING},
        {"table_name", pb::STRING},
        {"split_info", pb::STRING},
        {"split_rows", pb::STRING},
    };
    int64_t table_id = construct_table("PARTITION_SPLIT_INFO", fields);
    // 定义操作
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) -> 
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            if (state->client_conn() == nullptr) {
                return records;
            }
            std::string namespace_ = state->client_conn()->user_info->namespace_;
            std::string table_name;
            for (auto expr : conditions) {
                if (expr->node_type() != pb::FUNCTION_CALL) {
                    continue;
                }
                int32_t fn_op = static_cast<ScalarFnCall*>(expr)->fn().fn_op();
                if (fn_op != parser::FT_EQ) {
                    continue;
                }
                if (!expr->children(0)->is_slot_ref()) {
                    continue;
                }
                SlotRef* slot_ref = static_cast<SlotRef*>(expr->children(0));
                int32_t field_id = slot_ref->field_id();
                if (field_id != 2) {
                    continue;
                }
                if (expr->children(1)->is_constant()) {
                    table_name = expr->children(1)->get_value(nullptr).get_string();
                }
            }
            if (table_name.empty()) {
                return records;
            }
            auto* factory = SchemaFactory::get_instance();
            int64_t condition_table_id = 0;
            if (factory->get_table_id(namespace_ + "." + table_name, condition_table_id) != 0) {
                return records;
            }
            auto index_ptr = factory->get_index_info_ptr(condition_table_id);
            if (index_ptr == nullptr) {
                return records;
            }
            if (index_ptr->fields.size() < 2) {
                return records;
            }
            std::map<std::string, pb::RegionInfo> region_infos;
            factory->get_all_region_by_table_id(condition_table_id, &region_infos);
            std::string last_partition_key;
            std::vector<std::string> last_keys;
            std::vector<int64_t> last_region_ids;
            last_keys.reserve(3);
            last_region_ids.reserve(3);
            int64_t last_id = 0;
            std::string partition_key;
            auto type1 = index_ptr->fields[0].type;
            auto type2 = index_ptr->fields[1].type;
            records.reserve(10000);
            std::vector<std::vector<int64_t>> region_ids;
            region_ids.reserve(10000);
            pb::QueryRequest req;
            pb::QueryResponse res;
            req.set_op_type(pb::QUERY_REGION);
            for (auto& pair : region_infos) {
                TableKey start_key(pair.second.start_key());
                int pos = 0;
                partition_key = start_key.decode_start_key_string(type1, pos);
                if (partition_key != last_partition_key) {
                    if (last_keys.size() > 1) {
                        for (auto id : last_region_ids) {
                            req.add_region_ids(id);
                        }
                        region_ids.emplace_back(last_region_ids);
                        auto record = factory->new_record(table_id);
                        record->set_string(record->get_field_by_name("partition_key"), last_partition_key);
                        record->set_string(record->get_field_by_name("table_name"), table_name);
                        record->set_string(record->get_field_by_name("split_info"), boost::join(last_keys, ","));
                        //record->set_string(record->get_field_by_name("split_rows"), boost::join(rows, ","));
                        records.emplace_back(record);
                    }
                    last_partition_key = partition_key;
                    last_keys.clear();
                    last_region_ids.clear();
                    last_region_ids.emplace_back(last_id);
                }
                last_keys.emplace_back(start_key.decode_start_key_string(type2, pos));
                last_region_ids.emplace_back(pair.second.region_id());
                last_id = pair.second.region_id();
            }
            if (last_keys.size() > 1) {
                for (auto id : last_region_ids) {
                    req.set_op_type(pb::QUERY_REGION);
                }
                region_ids.emplace_back(last_region_ids);
                auto record = factory->new_record(table_id);
                record->set_string(record->get_field_by_name("partition_key"), last_partition_key);
                record->set_string(record->get_field_by_name("table_name"), table_name);
                record->set_string(record->get_field_by_name("split_info"), boost::join(last_keys, ","));
                //record->set_string(record->get_field_by_name("split_rows"), boost::join(rows, ","));
                records.emplace_back(record);
            }
            MetaServerInteract::get_instance()->send_request("query", req, res);
            std::unordered_map<int64_t, std::string> region_lines;
            for (auto& info : res.region_infos()) {
                region_lines[info.region_id()] = std::to_string(info.num_table_lines());
            }
            for (uint32_t i = 0; i < records.size(); i++) {
                std::vector<std::string> rows;
                rows.reserve(3);
                if (i < region_ids.size()) {
                    for (auto& id : region_ids[i]) {
                        rows.emplace_back(region_lines[id]);
                    }
                }
                records[i]->set_string(records[i]->get_field_by_name("split_rows"), boost::join(rows, ","));
            }
            return records;
        };
}

void InformationSchema::init_region_status() {
    // 定义字段信息
    FieldVec fields {
        {"region_id", pb::INT64},
        {"parent", pb::INT64},
        {"table_id", pb::INT64},
        {"main_table_id", pb::INT64},
        {"table_name", pb::STRING},
        {"start_key", pb::  STRING},
        {"end_key", pb::STRING},
        {"create_time", pb::STRING},
        {"peers", pb::STRING},
        {"leader", pb::STRING},
        {"version", pb::INT64},
        {"conf_version", pb::INT64},
        {"num_table_lines", pb::INT64},
        {"used_size", pb::INT64},
    };
    int64_t table_id = construct_table("REGION_STATUS", fields);
    // 定义操作
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) -> 
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            if (state->client_conn() == nullptr) {
                return records;
            }
            std::string namespace_ = state->client_conn()->user_info->namespace_;
            std::string table_name;
            for (auto expr : conditions) {
                if (expr->node_type() != pb::FUNCTION_CALL) {
                    continue;
                }
                int32_t fn_op = static_cast<ScalarFnCall*>(expr)->fn().fn_op();
                if (fn_op != parser::FT_EQ) {
                    continue;
                }
                if (!expr->children(0)->is_slot_ref()) {
                    continue;
                }
                SlotRef* slot_ref = static_cast<SlotRef*>(expr->children(0));
                int32_t field_id = slot_ref->field_id();
                if (field_id != 5) {
                    continue;
                }
                if (expr->children(1)->is_constant()) {
                    table_name = expr->children(1)->get_value(nullptr).get_string();
                }
            }
            if (table_name.empty()) {
                return records;
            }
            auto* factory = SchemaFactory::get_instance();
            int64_t condition_table_id = 0;
            if (factory->get_table_id(namespace_ + "." + table_name, condition_table_id) != 0) {
                return records;
            }
            auto index_ptr = factory->get_index_info_ptr(condition_table_id);
            if (index_ptr == nullptr) {
                return records;
            }
            std::map<std::string, pb::RegionInfo> region_infos;
            factory->get_all_region_by_table_id(condition_table_id, &region_infos);
            records.reserve(region_infos.size());
            for (auto& pair : region_infos) {
                auto& region = pair.second;
                TableKey start_key(region.start_key());
                TableKey end_key(region.end_key());
                auto record = factory->new_record(table_id);
                record->set_int64(record->get_field_by_name("region_id"), region.region_id());
                record->set_int64(record->get_field_by_name("parent"), region.parent());
                record->set_int64(record->get_field_by_name("table_id"), region.table_id());
                record->set_int64(record->get_field_by_name("main_table_id"), region.main_table_id());
                record->set_string(record->get_field_by_name("table_name"), table_name);
                record->set_int64(record->get_field_by_name("version"), region.version());
                record->set_int64(record->get_field_by_name("conf_version"), region.conf_version());
                record->set_int64(record->get_field_by_name("num_table_lines"), region.num_table_lines());
                record->set_int64(record->get_field_by_name("used_size"), region.used_size());
                record->set_string(record->get_field_by_name("leader"), region.leader());
                record->set_string(record->get_field_by_name("peers"), boost::join(region.peers(), ","));
                time_t t = region.timestamp();
                struct tm t_result;
                localtime_r(&t, &t_result);
                char s[100];
                strftime(s, sizeof(s), "%F %T", &t_result);
                record->set_string(record->get_field_by_name("create_time"), s);
                record->set_string(record->get_field_by_name("start_key"), 
                        start_key.decode_start_key_string(*index_ptr));
                record->set_string(record->get_field_by_name("end_key"), 
                        end_key.decode_start_key_string(*index_ptr));
                records.emplace_back(record);
            }
            return records;
    };
}
// MYSQL兼容表
void InformationSchema::init_columns() {
    // 定义字段信息
    FieldVec fields {
        {"TABLE_CATALOG", pb::STRING},
        {"TABLE_SCHEMA", pb::STRING},
        {"TABLE_NAME", pb::STRING},
        {"COLUMN_NAME", pb::STRING},
        {"ORDINAL_POSITION", pb::INT64},
        {"COLUMN_DEFAULT", pb::STRING},
        {"IS_NULLABLE", pb::STRING},
        {"DATA_TYPE", pb::STRING},
        {"CHARACTER_MAXIMUM_LENGTH", pb::INT64},
        {"CHARACTER_OCTET_LENGTH", pb::INT64},
        {"NUMERIC_PRECISION", pb::INT64},
        {"NUMERIC_SCALE", pb::INT64},
        {"DATETIME_PRECISION", pb::INT64},
        {"CHARACTER_SET_NAME", pb::STRING},
        {"COLLATION_NAME", pb::STRING},
        {"COLUMN_TYPE", pb::STRING},
        {"COLUMN_KEY", pb::STRING},
        {"EXTRA", pb::STRING},
        {"PRIVILEGES", pb::STRING},
        {"COLUMN_COMMENT", pb::STRING},
    };
    int64_t table_id = construct_table("COLUMNS", fields);
    // 定义操作
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) -> 
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            if (state->client_conn() == nullptr) {
                return records;
            }
            std::string namespace_ = state->client_conn()->user_info->namespace_;
            std::string table_name;
            auto* factory = SchemaFactory::get_instance();
            auto tb_vec = factory->get_table_list(namespace_, state->client_conn()->user_info.get());
            records.reserve(tb_vec.size() * 10);
            for (auto& table_info : tb_vec) {
                int i = 0;
                std::vector<std::string> items;
                boost::split(items, table_info->name, boost::is_any_of("."));
                std::string db = items[0];

                std::multimap<int32_t, IndexInfo> field_index;
                for (auto& index_id : table_info->indices) {
                    IndexInfo index_info = factory->get_index_info(index_id);
                    for (auto& field : index_info.fields) {
                        field_index.insert(std::make_pair(field.id, index_info));
                    }
                }
                for (auto& field : table_info->fields) {
                    if (field.deleted) {
                        continue;
                    }
                    auto record = factory->new_record(table_id);
                    record->set_string(record->get_field_by_name("TABLE_CATALOG"), "def");
                    record->set_string(record->get_field_by_name("TABLE_SCHEMA"), db);
                    record->set_string(record->get_field_by_name("TABLE_NAME"), table_info->short_name);
                    record->set_string(record->get_field_by_name("COLUMN_NAME"), field.short_name);
                    record->set_int64(record->get_field_by_name("ORDINAL_POSITION"), ++i);
                    record->set_string(record->get_field_by_name("COLUMN_DEFAULT"), field.default_value);
                    record->set_string(record->get_field_by_name("IS_NULLABLE"), field.can_null ? "YES" : "NO");
                    record->set_string(record->get_field_by_name("DATA_TYPE"), to_mysql_type_string(field.type));
                    switch (field.type) {
                        case pb::STRING:
                            record->set_int64(record->get_field_by_name("CHARACTER_MAXIMUM_LENGTH"), 1048576);
                            record->set_int64(record->get_field_by_name("CHARACTER_OCTET_LENGTH"), 3145728);
                            break;
                        case pb::INT8:
                        case pb::UINT8:
                            record->set_int64(record->get_field_by_name("NUMERIC_SCALE"), 3);
                            record->set_int64(record->get_field_by_name("NUMERIC_PRECISION"), 0);
                            break;
                        case pb::INT16:
                        case pb::UINT16:
                            record->set_int64(record->get_field_by_name("NUMERIC_SCALE"), 5);
                            record->set_int64(record->get_field_by_name("NUMERIC_PRECISION"), 0);
                            break;
                        case pb::INT32:
                        case pb::UINT32:
                            record->set_int64(record->get_field_by_name("NUMERIC_SCALE"), 10);
                            record->set_int64(record->get_field_by_name("NUMERIC_PRECISION"), 0);
                            break;
                        case pb::INT64:
                            record->set_int64(record->get_field_by_name("NUMERIC_SCALE"), 19);
                            record->set_int64(record->get_field_by_name("NUMERIC_PRECISION"), 0);
                            break;
                        case pb::UINT64:
                            record->set_int64(record->get_field_by_name("NUMERIC_SCALE"), 20);
                            record->set_int64(record->get_field_by_name("NUMERIC_PRECISION"), 0);
                            break;
                        case pb::FLOAT:
                            record->set_int64(record->get_field_by_name("NUMERIC_SCALE"), 38);
                            record->set_int64(record->get_field_by_name("NUMERIC_PRECISION"), 6);
                            break;
                        case pb::DOUBLE:
                            record->set_int64(record->get_field_by_name("NUMERIC_SCALE"), 308);
                            record->set_int64(record->get_field_by_name("NUMERIC_PRECISION"), 15);
                            break;
                        case pb::DATETIME:
                        case pb::TIMESTAMP:
                        case pb::DATE:
                            record->set_int64(record->get_field_by_name("DATETIME_PRECISION"), 0);
                            break;
                        default:
                            break;
                    }
                    record->set_string(record->get_field_by_name("CHARACTER_SET_NAME"), "utf8");
                    record->set_string(record->get_field_by_name("COLLATION_NAME"), "utf8_general_ci");
                    record->set_string(record->get_field_by_name("COLUMN_TYPE"), to_mysql_type_full_string(field.type));
                    std::vector<std::string> extra_vec;
                    if (field_index.count(field.id) == 0) {
                        record->set_string(record->get_field_by_name("COLUMN_KEY"), " ");
                    } else {
                        std::vector<std::string> index_types;
                        index_types.reserve(4);
                        auto range = field_index.equal_range(field.id);
                        for (auto index_iter = range.first; index_iter != range.second; ++index_iter) {
                            auto& index_info = index_iter->second;
                            std::string index = pb::IndexType_Name(index_info.type);
                            if (index_info.type == pb::I_FULLTEXT) {
                                index += "(" + pb::SegmentType_Name(index_info.segment_type) + ")";
                            }
                            index_types.push_back(index);
                            extra_vec.push_back(pb::IndexState_Name(index_info.state));
                        }
                        record->set_string(record->get_field_by_name("COLUMN_KEY"), boost::algorithm::join(index_types, "|"));
                    }
                    if (table_info->auto_inc_field_id == field.id) {
                        extra_vec.push_back("auto_increment");
                    } else {
                        //extra_vec.push_back(" ");
                    }
                    record->set_string(record->get_field_by_name("EXTRA"), boost::algorithm::join(extra_vec, "|"));
                    record->set_string(record->get_field_by_name("PRIVILEGES"), "select,insert,update,references");
                    record->set_string(record->get_field_by_name("COLUMN_COMMENT"), field.comment);
                    records.emplace_back(record);
                }
            }
            return records;
    };
}

void InformationSchema::init_referential_constraints() {
    // 定义字段信息
    FieldVec fields {
        {"CONSTRAINT_CATALOG", pb::STRING},
        {"CONSTRAINT_SCHEMA", pb::STRING},
        {"CONSTRAINT_NAME", pb::STRING},
        {"UNIQUE_CONSTRAINT_CATALOG", pb::STRING},
        {"UNIQUE_CONSTRAINT_SCHEMA", pb::STRING},
        {"UNIQUE_CONSTRAINT_NAME", pb::STRING},
        {"MATCH_OPTION", pb::STRING},
        {"UPDATE_RULE", pb::STRING},
        {"DELETE_RULE", pb::STRING},
        {"TABLE_NAME", pb::STRING},
        {"REFERENCED_TABLE_NAME", pb::STRING}
    };
    int64_t table_id = construct_table("REFERENTIAL_CONSTRAINTS", fields);
    // 定义操作
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) -> 
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_key_column_usage() {
    // 定义字段信息
    FieldVec fields {
        {"CONSTRAINT_CATALOG", pb::STRING},
        {"CONSTRAINT_SCHEMA", pb::STRING},
        {"CONSTRAINT_NAME", pb::STRING},
        {"TABLE_CATALOG", pb::STRING},
        {"TABLE_SCHEMA", pb::STRING},
        {"TABLE_NAME", pb::STRING},
        {"COLUMN_NAME", pb::STRING},
        {"ORDINAL_POSITION", pb::INT64},
        {"POSITION_IN_UNIQUE_CONSTRAINT", pb::INT64},
        {"REFERENCED_TABLE_SCHEMA", pb::STRING},
        {"REFERENCED_TABLE_NAME", pb::STRING},
        {"REFERENCED_COLUMN_NAME", pb::STRING}
    };
    int64_t table_id = construct_table("KEY_COLUMN_USAGE", fields);
    // 定义操作
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) -> 
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            if (state->client_conn() == nullptr) {
                return records;
            }
            std::string namespace_ = state->client_conn()->user_info->namespace_;
            std::string table_name;
            auto* factory = SchemaFactory::get_instance();
            auto tb_vec = factory->get_table_list(namespace_, state->client_conn()->user_info.get());
            records.reserve(tb_vec.size() * 10);
            for (auto& table_info : tb_vec) {
                int i = 0;
                std::vector<std::string> items;
                boost::split(items, table_info->name, boost::is_any_of("."));
                std::string db = items[0];

                std::multimap<int32_t, IndexInfo> field_index;
                for (auto& index_id : table_info->indices) {
                    IndexInfo index_info = factory->get_index_info(index_id);
                    auto index_type = index_info.type;
                    if (index_type != pb::I_PRIMARY && index_type != pb::I_UNIQ) {
                        continue;
                    }
                    int idx = 0;
                    for (auto& field : index_info.fields) {
                        idx ++;
                        auto record = factory->new_record(table_id);
                        record->set_string(record->get_field_by_name("CONSTRAINT_CATALOG"), "def");
                        record->set_string(record->get_field_by_name("CONSTRAINT_SCHEMA"), db);
                        record->set_string(record->get_field_by_name("CONSTRAINT_NAME"), index_type == pb::I_PRIMARY ? "PRIMARY":"name_key");
                        record->set_string(record->get_field_by_name("TABLE_CATALOG"), "def");
                        record->set_string(record->get_field_by_name("TABLE_SCHEMA"), db);
                        record->set_string(record->get_field_by_name("TABLE_NAME"), table_info->short_name);
                        record->set_string(record->get_field_by_name("COLUMN_NAME"), field.short_name);
                        record->set_int64(record->get_field_by_name("ORDINAL_POSITION"), idx);
                        records.emplace_back(record);

                    }
                }
            }
            return records;
    };
}

void InformationSchema::init_statistics() {
    // 定义字段信息
    FieldVec fields {
        {"TABLE_CATALOG", pb::STRING},
        {"TABLE_SCHEMA", pb::STRING},
        {"TABLE_NAME", pb::STRING},
        {"NON_UNIQUE", pb::STRING},
        {"INDEX_SCHEMA", pb::STRING},
        {"INDEX_NAME", pb::STRING},
        {"SEQ_IN_INDEX", pb::INT64},
        {"COLUMN_NAME", pb::STRING},
        {"COLLATION", pb::STRING},
        {"CARDINALITY", pb::INT64},
        {"SUB_PART", pb::INT64},
        {"PACKED", pb::STRING},
        {"NULLABLE", pb::STRING},
        {"INDEX_TYPE", pb::STRING},
        {"COMMENT", pb::STRING},
        {"INDEX_COMMENT", pb::STRING},
    };
    int64_t table_id = construct_table("STATISTICS", fields);
    // 定义操作
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) -> 
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            if (state->client_conn() == nullptr) {
                return records;
            }
            std::string namespace_ = state->client_conn()->user_info->namespace_;
            std::string table_name;
            auto* factory = SchemaFactory::get_instance();
            auto tb_vec = factory->get_table_list(namespace_, state->client_conn()->user_info.get());
            records.reserve(tb_vec.size() * 10);
            for (auto& table_info : tb_vec) {
                std::vector<std::string> items;
                boost::split(items, table_info->name, boost::is_any_of("."));
                std::string db = items[0];
                for (auto& index_id : table_info->indices) {
                    auto index_ptr = factory->get_index_info_ptr(index_id);
                    if (index_ptr == nullptr) {
                        continue;
                    }
                    if (index_ptr->index_hint_status != pb::IHS_NORMAL) {
                        continue;
                    }
                    int i = 0;
                    for (auto& field : index_ptr->fields) {
                        auto record = factory->new_record(table_id);
                        record->set_string(record->get_field_by_name("TABLE_CATALOG"), "def");
                        record->set_string(record->get_field_by_name("TABLE_SCHEMA"), db);
                        record->set_string(record->get_field_by_name("TABLE_NAME"), table_info->short_name);
                        record->set_string(record->get_field_by_name("INDEX_SCHEMA"), db);
                        std::string index_name = index_ptr->short_name;
                        std::string index_type = "BTREE";
                        std::string non_unique = "0";
                        if (index_ptr->type == pb::I_PRIMARY) {
                            index_name = "PRIMARY";
                        } else if (index_ptr->type == pb::I_KEY) {
                            non_unique = "1";
                        } else if (index_ptr->type == pb::I_FULLTEXT) {
                            non_unique = "1";
                            index_type = "FULLTEXT";
                        }
                        record->set_string(record->get_field_by_name("INDEX_NAME"), index_name);
                        record->set_string(record->get_field_by_name("COLUMN_NAME"), field.short_name);
                        record->set_string(record->get_field_by_name("NON_UNIQUE"), non_unique);
                        record->set_int64(record->get_field_by_name("SEQ_IN_INDEX"), i++);
                        record->set_string(record->get_field_by_name("NULLABLE"), field.can_null ? "YES" : "");
                        record->set_string(record->get_field_by_name("COLLATION"), "A");
                        record->set_string(record->get_field_by_name("INDEX_TYPE"), index_type);
                        record->set_string(record->get_field_by_name("INDEX_COMMENT"), index_ptr->comments);
                        std::ostringstream comment;
                        comment << "'{\"segment_type\":\"";
                        comment << pb::SegmentType_Name(index_ptr->segment_type) << "\", ";
                        comment << "\"storage_type\":\"";
                        comment << pb::StorageType_Name(index_ptr->storage_type) << "\", ";
                        comment << "\"is_global\":\"" << index_ptr->is_global << "\"}'";
                        record->set_string(record->get_field_by_name("COMMENT"), comment.str());
                        records.emplace_back(record);
                    }
                }
            }
            return records;
    };
}
void InformationSchema::init_schemata() {
    // 定义字段信息
    FieldVec fields {
        {"CATALOG_NAME", pb::STRING},
        {"SCHEMA_NAME", pb::STRING},
        {"DEFAULT_CHARACTER_SET_NAME", pb::STRING},
        {"DEFAULT_COLLATION_NAME", pb::STRING},
        {"SQL_PATH", pb::INT64},
    };
    int64_t table_id = construct_table("SCHEMATA", fields);
    // 定义操作
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) -> 
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            if (state->client_conn() == nullptr) {
                return records;
            }
            auto* factory = SchemaFactory::get_instance();
            std::vector<std::string> db_vec =  factory->get_db_list(state->client_conn()->user_info->all_database);
            records.reserve(db_vec.size());
            for (auto& db : db_vec) {
                auto record = factory->new_record(table_id);
                record->set_string(record->get_field_by_name("CATALOG_NAME"), "def");
                record->set_string(record->get_field_by_name("SCHEMA_NAME"), db);
                record->set_string(record->get_field_by_name("DEFAULT_CHARACTER_SET_NAME"), "utf8mb4");
                record->set_string(record->get_field_by_name("DEFAULT_COLLATION_NAME"), "utf8mb4_bin");
                records.emplace_back(record);
            }
            return records;
    };
}
void InformationSchema::init_tables() {
    // 定义字段信息
    FieldVec fields {
        {"TABLE_CATALOG", pb::STRING},
        {"TABLE_SCHEMA", pb::STRING},
        {"TABLE_NAME", pb::STRING},
        {"TABLE_TYPE", pb::STRING},
        {"ENGINE", pb::STRING},
        {"VERSION", pb::INT64},
        {"ROW_FORMAT", pb::STRING},
        {"TABLE_ROWS", pb::INT64},
        {"AVG_ROW_LENGTH", pb::INT64},
        {"DATA_LENGTH", pb::INT64},
        {"MAX_DATA_LENGTH", pb::INT64},
        {"INDEX_LENGTH", pb::INT64},
        {"DATA_FREE", pb::INT64},
        {"AUTO_INCREMENT", pb::INT64},
        {"CREATE_TIME", pb::DATETIME},
        {"UPDATE_TIME", pb::DATETIME},
        {"CHECK_TIME", pb::DATETIME},
        {"TABLE_COLLATION", pb::STRING},
        {"CHECKSUM", pb::INT64},
        {"CREATE_OPTIONS", pb::STRING},
        {"TABLE_COMMENT", pb::STRING},
        {"TABLE_ID", pb::INT64},
    };
    int64_t table_id = construct_table("TABLES", fields);
    // 定义操作
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) -> 
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            if (state->client_conn() == nullptr) {
                return records;
            }
            std::string namespace_ = state->client_conn()->user_info->namespace_;
            std::string table_name;
            auto* factory = SchemaFactory::get_instance();
            auto tb_vec = factory->get_table_list(namespace_, state->client_conn()->user_info.get());
            records.reserve(tb_vec.size());
            for (auto& table_info : tb_vec) {
                std::vector<std::string> items;
                boost::split(items, table_info->name, boost::is_any_of("."));
                std::string db = items[0];
                auto record = factory->new_record(table_id);
                record->set_string(record->get_field_by_name("TABLE_CATALOG"), "def");
                record->set_string(record->get_field_by_name("TABLE_SCHEMA"), db);
                record->set_string(record->get_field_by_name("TABLE_NAME"), table_info->short_name);
                record->set_string(record->get_field_by_name("TABLE_TYPE"), "BASE TABLE");
                record->set_string(record->get_field_by_name("ENGINE"), "Innodb");
                record->set_int64(record->get_field_by_name("VERSION"), table_info->version);
                record->set_string(record->get_field_by_name("ROW_FORMAT"), "Compact");
                record->set_int64(record->get_field_by_name("TABLE_ROWS"), 0);
                record->set_int64(record->get_field_by_name("AVG_ROW_LENGTH"), table_info->byte_size_per_record);
                record->set_int64(record->get_field_by_name("DATA_LENGTH"), 0);
                record->set_int64(record->get_field_by_name("MAX_DATA_LENGTH"), 0);
                record->set_int64(record->get_field_by_name("INDEX_LENGTH"), 0);
                record->set_int64(record->get_field_by_name("DATA_FREE"), 0);
                record->set_int64(record->get_field_by_name("AUTO_INCREMENT"), 0);
                ExprValue ct(pb::TIMESTAMP);
                ct._u.uint32_val = table_info->timestamp;
                record->set_value(record->get_field_by_name("CREATE_TIME"), ct.cast_to(pb::DATETIME));
                record->set_string(record->get_field_by_name("TABLE_COLLATION"), "utf8_bin");
                record->set_string(record->get_field_by_name("CREATE_OPTIONS"), "");
                record->set_string(record->get_field_by_name("TABLE_COMMENT"), "");
                record->set_int64(record->get_field_by_name("TABLE_ID"), table_info->id);
                records.emplace_back(record);
            }
            return records;
    };
}
void InformationSchema::init_virtual_index_influence_info() {
    //定义字段信息
    FieldVec fields {
        {"database_name",pb::STRING},
        {"table_name",pb::STRING},
        {"virtual_index_name",pb::STRING},
        {"sign",pb::STRING},
        {"sample_sql",pb::STRING},
    };
    int64_t table_id = construct_table("VIRTUAL_INDEX_AFFECT_SQL", fields);
    //定义操作
    _calls[table_id] = [table_id](RuntimeState* state,std::vector<ExprNode*>& conditions) -> 
        std::vector <SmartRecord> {
            std::vector <SmartRecord> records;
            //更新表中数据前，要交互一次，从TableMem取影响面数据
            pb::QueryRequest request;
            pb::QueryResponse response;
            //1、设定查询请求的操作类型
            request.set_op_type(pb::QUERY_SHOW_VIRINDX_INFO_SQL);
            //2、发送请求
            MetaServerInteract::get_instance()->send_request("query", request, response);
            //3.取出response中的影响面信息
            auto& virtual_index_info_sqls = response.virtual_index_influence_info();//virtual_index_info   and   affected_sqls
            if(state -> client_conn() ==  nullptr){
                return records;
            }
            std::string namespace_ = state->client_conn()->user_info->namespace_;
            std::string table_name;
            auto* factory = SchemaFactory::get_instance();
            auto tb_vec = factory->get_table_list(namespace_, state->client_conn()->user_info.get());
            records.reserve(tb_vec.size());
            for (auto& it1 : virtual_index_info_sqls) {
                std::string key = it1.virtual_index_info();
                std::string infuenced_sql = it1.affected_sqls();
                std::string sign = it1.affected_sign();
                std::vector<std::string> items1;
                boost::split(items1, key, boost::is_any_of(","));
                auto record = factory->new_record(table_id);
                record->set_string(record->get_field_by_name("database_name"), items1[0]);
                record->set_string(record->get_field_by_name("table_name"), items1[1]);
                record->set_string(record->get_field_by_name("virtual_index_name"),items1[2]);
                record->set_string(record->get_field_by_name("sign"), sign);
                record->set_string(record->get_field_by_name("sample_sql"), infuenced_sql);
                records.emplace_back(record);
            }
            return records;
    };
}

void InformationSchema::init_sign_list() {
    //定义字段信息
    FieldVec fields {
        {"namespace",pb::STRING},
        {"database_name",pb::STRING},
        {"table_name",pb::STRING},
        {"sign",pb::STRING},
    };

    int64_t blacklist_table_id = construct_table("SIGN_BLACKLIST", fields);
    int64_t forcelearner_table_id = construct_table("SIGN_FORCELEARNER", fields);
    //定义操作
    _calls[blacklist_table_id] = [blacklist_table_id](RuntimeState* state,std::vector<ExprNode*>& conditions) -> 
            std::vector <SmartRecord> {
        std::vector <SmartRecord> records;
        records.reserve(10);
        auto blacklist_table = SchemaFactory::get_instance()->get_table_info_ptr(blacklist_table_id);
        auto func = [&records, &blacklist_table](const SmartTable& table) -> bool {
            for (auto sign : table->sign_blacklist) {
                auto record = SchemaFactory::get_instance()->new_record(*blacklist_table);
                record->set_string(record->get_field_by_name("namespace"), table->namespace_);
                std::string db_name;
                std::vector<std::string> vec;
                boost::split(vec, table->name, boost::is_any_of("."));
                if (!vec.empty()) {
                    db_name = vec[0];
                }
                record->set_string(record->get_field_by_name("database_name"), db_name);
                record->set_string(record->get_field_by_name("table_name"),table->short_name);
                record->set_string(record->get_field_by_name("sign"), std::to_string(sign));
                records.emplace_back(record);
            }
            return false;
        };
        std::vector<std::string> database_table;
        std::vector<std::string> binlog_table;
        SchemaFactory::get_instance()->get_table_by_filter(database_table, binlog_table, func);
        return records;
    };

    _calls[forcelearner_table_id] = [forcelearner_table_id](RuntimeState* state,std::vector<ExprNode*>& conditions) -> 
            std::vector <SmartRecord> {
        std::vector <SmartRecord> records;
        records.reserve(10);
        auto forcelearner_table = SchemaFactory::get_instance()->get_table_info_ptr(forcelearner_table_id);
        auto func = [&records, &forcelearner_table](const SmartTable& table) -> bool {
            for (auto sign : table->sign_forcelearner) {
                auto record = SchemaFactory::get_instance()->new_record(*forcelearner_table);
                record->set_string(record->get_field_by_name("namespace"), table->namespace_);
                std::string db_name;
                std::vector<std::string> vec;
                boost::split(vec, table->name, boost::is_any_of("."));
                if (!vec.empty()) {
                    db_name = vec[0];
                }
                record->set_string(record->get_field_by_name("database_name"), db_name);
                record->set_string(record->get_field_by_name("table_name"),table->short_name);
                record->set_string(record->get_field_by_name("sign"), std::to_string(sign));
                records.emplace_back(record);
            }
            return false;
        };
        std::vector<std::string> database_table;
        std::vector<std::string> binlog_table;
        SchemaFactory::get_instance()->get_table_by_filter(database_table, binlog_table, func);
        return records;
    };
}

void InformationSchema::init_routines() {
    // 定义字段信息
    FieldVec fields {
        {"SPECIFIC_NAME", pb::STRING},
        {"ROUTINE_CATALOG", pb::STRING},
        {"ROUTINE_SCHEMA", pb::STRING},
        {"ROUTINE_NAME", pb::STRING},
        {"ROUTINE_TYPE", pb::STRING},
        {"DATA_TYPE", pb::STRING},
        {"CHARACTER_MAXIMUM_LENGTH", pb::INT64},
        {"CHARACTER_OCTET_LENGTH", pb::INT64},
        {"NUMERIC_PRECISION", pb::UINT64},
        {"NUMERIC_SCALE", pb::INT64},
        {"DATETIME_PRECISION", pb::UINT64},
        {"CHARACTER_SET_NAME", pb::STRING},
        {"COLLATION_NAME", pb::STRING},
        {"DTD_IDENTIFIER", pb::STRING},
        {"ROUTINE_BODY", pb::STRING},
        {"ROUTINE_DEFINITION" , pb::STRING},
        {"EXTERNAL_NAME" , pb::STRING},
        {"EXTERNAL_LANGUAGE" , pb::STRING},
        {"PARAMETER_STYLE", pb::STRING},
        {"IS_DETERMINISTIC", pb::STRING},
        {"SQL_DATA_ACCESS", pb::STRING},
        {"SQL_PATH", pb::STRING},
        {"SECURITY_TYPE", pb::STRING},
        {"CREATED",  pb::STRING},
        {"LAST_ALTERED",  pb::DATETIME},
        {"SQL_MODE", pb::DATETIME},
        {"ROUTINE_COMMENT", pb::STRING},
        {"DEFINER", pb::STRING},
        {"CHARACTER_SET_CLIENT", pb::STRING},
        {"COLLATION_CONNECTION", pb::STRING},
        {"DATABASE_COLLATION", pb::STRING},
    };
    int64_t table_id = construct_table("ROUTINES", fields);
    // 定义操作
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}
void InformationSchema::init_triggers() {
    // 定义字段信息
    FieldVec fields {
        {"TRIGGER_CATALOG", pb::STRING},
        {"TRIGGER_SCHEMA", pb::STRING},
        {"TRIGGER_NAME", pb::STRING},
        {"EVENT_MANIPULATION", pb::STRING},
        {"EVENT_OBJECT_CATALOG", pb::STRING},
        {"EVENT_OBJECT_SCHEMA", pb::STRING},
        {"EVENT_OBJECT_TABLE", pb::STRING},
        {"ACTION_ORDER", pb::INT64},
        {"ACTION_CONDITION", pb::STRING},
        {"ACTION_STATEMENT", pb::STRING},
        {"ACTION_ORIENTATION", pb::STRING},
        {"ACTION_TIMING", pb::STRING},
        {"ACTION_REFERENCE_OLD_TABLE", pb::STRING},
        {"ACTION_REFERENCE_NEW_TABLE", pb::STRING},
        {"ACTION_REFERENCE_OLD_ROW", pb::STRING},
        {"ACTION_REFERENCE_NEW_ROW" , pb::STRING},
        {"CREATED" , pb::DATETIME},
        {"SQL_MODE" , pb::STRING},
        {"DEFINER", pb::STRING},
        {"CHARACTER_SET_CLIENT", pb::STRING},
        {"COLLATION_CONNECTION", pb::STRING},
        {"DATABASE_COLLATION", pb::STRING},
    };
    int64_t table_id = construct_table("TRIGGERS", fields);
    // 定义操作
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}
void InformationSchema::init_views() {
    // 定义字段信息
    FieldVec fields {
        {"TABLE_CATALOG", pb::STRING},
        {"TABLE_SCHEMA", pb::STRING},
        {"TABLE_NAME", pb::STRING},
        {"VIEW_DEFINITION", pb::STRING},
        {"CHECK_OPTION", pb::STRING},
        {"IS_UPDATABLE", pb::STRING},
        {"DEFINER", pb::STRING},
        {"SECURITY_TYPE", pb::INT64},
        {"CHARACTER_SET_CLIENT", pb::STRING},
        {"COLLATION_CONNECTION", pb::STRING},
    };
    int64_t table_id = construct_table("VIEWS", fields);
    // 定义操作
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_character_sets() {
    FieldVec fields {
        {"CHARACTER_SET_NAME", pb::STRING},
        {"DEFAULT_COLLATE_NAME", pb::STRING},
        {"DESCRIPTION", pb::STRING},
        {"MAXLEN", pb::INT64},
    };
    int64_t table_id = construct_table("CHARACTER_SETS", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_collation_character_set_applicability() {
    FieldVec fields {
        {"COLLATION_NAME", pb::STRING},
        {"CHARACTER_SET_NAME", pb::STRING},
    };
    int64_t table_id = construct_table("COLLATION_CHARACTER_SET_APPLICABILITY", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_collations() {
    FieldVec fields {
        {"COLLATION_NAME", pb::STRING},
        {"CHARACTER_SET_NAME", pb::STRING},
        {"ID", pb::INT64},
        {"IS_DEFAULT", pb::STRING},
        {"IS_COMPILED", pb::STRING},
        {"SORTLEN", pb::INT64},
    };
    int64_t table_id = construct_table("COLLATIONS", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_column_privileges() {
    FieldVec fields {
        {"GRANTEE", pb::STRING},
        {"TABLE_CATALOG", pb::STRING},
        {"TABLE_SCHEMA", pb::STRING},
        {"TABLE_NAME", pb::STRING},
        {"COLUMN_NAME", pb::STRING},
        {"PRIVILEGE_TYPE", pb::STRING},
        {"IS_GRANTABLE", pb::STRING},
    };
    int64_t table_id = construct_table("COLUMN_PRIVILEGES", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_engines() {
    FieldVec fields {
        {"ENGINE", pb::STRING},
        {"SUPPORT", pb::STRING},
        {"COMMENT", pb::STRING},
        {"TRANSACTIONS", pb::STRING},
        {"XA", pb::STRING},
        {"SAVEPOINTS", pb::STRING},
    };
    int64_t table_id = construct_table("ENGINES", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_events() {
    FieldVec fields {
        {"EVENT_CATALOG", pb::STRING},
        {"EVENT_SCHEMA", pb::STRING},
        {"EVENT_NAME", pb::STRING},
        {"DEFINER", pb::STRING},
        {"TIME_ZONE", pb::STRING},
        {"EVENT_BODY", pb::STRING},
        {"EVENT_DEFINITION", pb::STRING},
        {"EVENT_TYPE", pb::STRING},
        {"EXECUTE_AT", pb::DATETIME},
        {"INTERVAL_VALUE", pb::STRING},
        {"INTERVAL_FIELD", pb::STRING},
        {"SQL_MODE", pb::STRING},
        {"STARTS", pb::DATETIME},
        {"ENDS", pb::DATETIME},
        {"STATUS", pb::STRING},
        {"ON_COMPLETION", pb::STRING},
        {"CREATED", pb::DATETIME},
        {"LAST_ALTERED", pb::DATETIME},
        {"LAST_EXECUTED", pb::DATETIME},
        {"EVENT_COMMENT", pb::STRING},
        {"ORIGINATOR", pb::INT64},
        {"CHARACTER_SET_CLIENT", pb::STRING},
        {"COLLATION_CONNECTION", pb::STRING},
        {"DATABASE_COLLATION", pb::STRING},
    };
    int64_t table_id = construct_table("EVENTS", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_files() {
    FieldVec fields {
        {"FILE_ID", pb::INT64},
        {"FILE_NAME", pb::STRING},
        {"FILE_TYPE", pb::STRING},
        {"TABLESPACE_NAME", pb::STRING},
        {"TABLE_CATALOG", pb::STRING},
        {"TABLE_SCHEMA", pb::STRING},
        {"TABLE_NAME", pb::STRING},
        {"LOGFILE_GROUP_NAME", pb::STRING},
        {"LOGFILE_GROUP_NUMBER", pb::INT64},
        {"ENGINE", pb::STRING},
        {"FULLTEXT_KEYS", pb::STRING},
        {"DELETED_ROWS", pb::INT64},
        {"UPDATE_COUNT", pb::INT64},
        {"FREE_EXTENTS", pb::INT64},
        {"TOTAL_EXTENTS", pb::INT64},
        {"EXTENT_SIZE", pb::INT64},
        {"INITIAL_SIZE", pb::UINT64},
        {"MAXIMUM_SIZE", pb::UINT64},
        {"AUTOEXTEND_SIZE", pb::UINT64},
        {"CREATION_TIME", pb::DATETIME},
        {"LAST_UPDATE_TIME", pb::DATETIME},
        {"LAST_ACCESS_TIME", pb::DATETIME},
        {"RECOVER_TIME", pb::INT64},
        {"TRANSACTION_COUNTER", pb::INT64},
        {"VERSION", pb::UINT64},
        {"ROW_FORMAT", pb::STRING},
        {"TABLE_ROWS", pb::UINT64},
        {"AVG_ROW_LENGTH", pb::UINT64},
        {"DATA_LENGTH", pb::UINT64},
        {"MAX_DATA_LENGTH", pb::UINT64},
        {"INDEX_LENGTH", pb::UINT64},
        {"DATA_FREE", pb::UINT64},
        {"CREATE_TIME", pb::DATETIME},
        {"UPDATE_TIME", pb::DATETIME},
        {"CHECK_TIME", pb::DATETIME},
        {"CHECKSUM", pb::UINT64},
        {"STATUS", pb::STRING},
        {"EXTRA", pb::STRING},
    };
    int64_t table_id = construct_table("FILES", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_global_status() {
    FieldVec fields {
        {"VARIABLE_NAME", pb::STRING},
        {"VARIABLE_VALUE", pb::STRING},
    };
    int64_t table_id = construct_table("GLOBAL_STATUS", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_global_variables() {
    FieldVec fields {
        {"VARIABLE_NAME", pb::STRING},
        {"VARIABLE_VALUE", pb::STRING},
    };
    int64_t table_id = construct_table("GLOBAL_VARIABLES", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_buffer_page() {
    FieldVec fields {
        {"POOL_ID", pb::UINT64},
        {"BLOCK_ID", pb::UINT64},
        {"SPACE", pb::UINT64},
        {"PAGE_NUMBER", pb::UINT64},
        {"PAGE_TYPE", pb::STRING},
        {"FLUSH_TYPE", pb::UINT64},
        {"FIX_COUNT", pb::UINT64},
        {"IS_HASHED", pb::STRING},
        {"NEWEST_MODIFICATION", pb::UINT64},
        {"OLDEST_MODIFICATION", pb::UINT64},
        {"ACCESS_TIME", pb::UINT64},
        {"TABLE_NAME", pb::STRING},
        {"INDEX_NAME", pb::STRING},
        {"NUMBER_RECORDS", pb::UINT64},
        {"DATA_SIZE", pb::UINT64},
        {"COMPRESSED_SIZE", pb::UINT64},
        {"PAGE_STATE", pb::STRING},
        {"IO_FIX", pb::STRING},
        {"IS_OLD", pb::STRING},
        {"FREE_PAGE_CLOCK", pb::UINT64},
    };
    int64_t table_id = construct_table("INNODB_BUFFER_PAGE", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_buffer_page_lru() {
    FieldVec fields {
        {"POOL_ID", pb::UINT64},
        {"LRU_POSITION", pb::UINT64},
        {"SPACE", pb::UINT64},
        {"PAGE_NUMBER", pb::UINT64},
        {"PAGE_TYPE", pb::STRING},
        {"FLUSH_TYPE", pb::UINT64},
        {"FIX_COUNT", pb::UINT64},
        {"IS_HASHED", pb::STRING},
        {"NEWEST_MODIFICATION", pb::UINT64},
        {"OLDEST_MODIFICATION", pb::UINT64},
        {"ACCESS_TIME", pb::UINT64},
        {"TABLE_NAME", pb::STRING},
        {"INDEX_NAME", pb::STRING},
        {"NUMBER_RECORDS", pb::UINT64},
        {"DATA_SIZE", pb::UINT64},
        {"COMPRESSED_SIZE", pb::UINT64},
        {"COMPRESSED", pb::STRING},
        {"IO_FIX", pb::STRING},
        {"IS_OLD", pb::STRING},
        {"FREE_PAGE_CLOCK", pb::UINT64},
    };
    int64_t table_id = construct_table("INNODB_BUFFER_PAGE_LRU", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_buffer_pool_stats() {
    FieldVec fields {
        {"POOL_ID", pb::UINT64},
        {"POOL_SIZE", pb::UINT64},
        {"FREE_BUFFERS", pb::UINT64},
        {"DATABASE_PAGES", pb::UINT64},
        {"OLD_DATABASE_PAGES", pb::UINT64},
        {"MODIFIED_DATABASE_PAGES", pb::UINT64},
        {"PENDING_DECOMPRESS", pb::UINT64},
        {"PENDING_READS", pb::UINT64},
        {"PENDING_FLUSH_LRU", pb::UINT64},
        {"PENDING_FLUSH_LIST", pb::UINT64},
        {"PAGES_MADE_YOUNG", pb::UINT64},
        {"PAGES_NOT_MADE_YOUNG", pb::UINT64},
        {"PAGES_MADE_YOUNG_RATE", pb::DOUBLE},
        {"PAGES_MADE_NOT_YOUNG_RATE", pb::DOUBLE},
        {"NUMBER_PAGES_READ", pb::UINT64},
        {"NUMBER_PAGES_CREATED", pb::UINT64},
        {"NUMBER_PAGES_WRITTEN", pb::UINT64},
        {"PAGES_READ_RATE", pb::DOUBLE},
        {"PAGES_CREATE_RATE", pb::DOUBLE},
        {"PAGES_WRITTEN_RATE", pb::DOUBLE},
        {"NUMBER_PAGES_GET", pb::UINT64},
        {"HIT_RATE", pb::UINT64},
        {"YOUNG_MAKE_PER_THOUSAND_GETS", pb::UINT64},
        {"NOT_YOUNG_MAKE_PER_THOUSAND_GETS", pb::UINT64},
        {"NUMBER_PAGES_READ_AHEAD", pb::UINT64},
        {"NUMBER_READ_AHEAD_EVICTED", pb::UINT64},
        {"READ_AHEAD_RATE", pb::DOUBLE},
        {"READ_AHEAD_EVICTED_RATE", pb::DOUBLE},
        {"LRU_IO_TOTAL", pb::UINT64},
        {"LRU_IO_CURRENT", pb::UINT64},
        {"UNCOMPRESS_TOTAL", pb::UINT64},
        {"UNCOMPRESS_CURRENT", pb::UINT64},
    };
    int64_t table_id = construct_table("INNODB_BUFFER_POOL_STATS", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_cmp() {
    FieldVec fields {
        {"page_size", pb::INT32},
        {"compress_ops", pb::INT32},
        {"compress_ops_ok", pb::INT32},
        {"compress_time", pb::INT32},
        {"uncompress_ops", pb::INT32},
        {"uncompress_time", pb::INT32},
    };
    int64_t table_id = construct_table("INNODB_CMP", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_cmpmem() {
    FieldVec fields {
        {"page_size", pb::INT32},
        {"buffer_pool_instance", pb::INT32},
        {"pages_used", pb::INT32},
        {"pages_free", pb::INT32},
        {"relocation_ops", pb::INT64},
        {"relocation_time", pb::INT32},
    };
    int64_t table_id = construct_table("INNODB_CMPMEM", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_cmpmem_reset() {
    FieldVec fields {
        {"page_size", pb::INT32},
        {"buffer_pool_instance", pb::INT32},
        {"pages_used", pb::INT32},
        {"pages_free", pb::INT32},
        {"relocation_ops", pb::INT64},
        {"relocation_time", pb::INT32},
    };
    int64_t table_id = construct_table("INNODB_CMPMEM_RESET", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_cmp_per_index() {
    FieldVec fields {
        {"database_name", pb::STRING},
        {"table_name", pb::STRING},
        {"index_name", pb::STRING},
        {"compress_ops", pb::INT32},
        {"compress_ops_ok", pb::INT32},
        {"compress_time", pb::INT32},
        {"uncompress_ops", pb::INT32},
        {"uncompress_time", pb::INT32},
    };
    int64_t table_id = construct_table("INNODB_CMP_PER_INDEX", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_cmp_per_index_reset() {
    FieldVec fields {
        {"database_name", pb::STRING},
        {"table_name", pb::STRING},
        {"index_name", pb::STRING},
        {"compress_ops", pb::INT32},
        {"compress_ops_ok", pb::INT32},
        {"compress_time", pb::INT32},
        {"uncompress_ops", pb::INT32},
        {"uncompress_time", pb::INT32},
    };
    int64_t table_id = construct_table("INNODB_CMP_PER_INDEX_RESET", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_cmp_reset() {
    FieldVec fields {
        {"page_size", pb::INT32},
        {"compress_ops", pb::INT32},
        {"compress_ops_ok", pb::INT32},
        {"compress_time", pb::INT32},
        {"uncompress_ops", pb::INT32},
        {"uncompress_time", pb::INT32},
    };
    int64_t table_id = construct_table("INNODB_CMP_RESET", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_ft_being_deleted() {
    FieldVec fields {
        {"DOC_ID", pb::UINT64},
    };
    int64_t table_id = construct_table("INNODB_FT_BEING_DELETED", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_ft_config() {
    FieldVec fields {
        {"KEY", pb::STRING},
        {"VALUE", pb::STRING},
    };
    int64_t table_id = construct_table("INNODB_FT_CONFIG", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_ft_default_stopword() {
    FieldVec fields {
        {"value", pb::STRING},
    };
    int64_t table_id = construct_table("INNODB_FT_DEFAULT_STOPWORD", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_ft_deleted() {
    FieldVec fields {
        {"DOC_ID", pb::UINT64},
    };
    int64_t table_id = construct_table("INNODB_FT_DELETED", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_ft_index_cache() {
    FieldVec fields {
        {"WORD", pb::STRING},
        {"FIRST_DOC_ID", pb::UINT64},
        {"LAST_DOC_ID", pb::UINT64},
        {"DOC_COUNT", pb::UINT64},
        {"DOC_ID", pb::UINT64},
        {"POSITION", pb::UINT64},
    };
    int64_t table_id = construct_table("INNODB_FT_INDEX_CACHE", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_ft_index_table() {
    FieldVec fields {
        {"WORD", pb::STRING},
        {"FIRST_DOC_ID", pb::UINT64},
        {"LAST_DOC_ID", pb::UINT64},
        {"DOC_COUNT", pb::UINT64},
        {"DOC_ID", pb::UINT64},
        {"POSITION", pb::UINT64},
    };
    int64_t table_id = construct_table("INNODB_FT_INDEX_TABLE", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_locks() {
    FieldVec fields {
        {"lock_id", pb::STRING},
        {"lock_trx_id", pb::STRING},
        {"lock_mode", pb::STRING},
        {"lock_type", pb::STRING},
        {"lock_table", pb::STRING},
        {"lock_index", pb::STRING},
        {"lock_space", pb::UINT64},
        {"lock_page", pb::UINT64},
        {"lock_rec", pb::UINT64},
        {"lock_data", pb::STRING},
    };
    int64_t table_id = construct_table("INNODB_LOCKS", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_lock_waits() {
    FieldVec fields {
        {"requesting_trx_id", pb::STRING},
        {"requested_lock_id", pb::STRING},
        {"blocking_trx_id", pb::STRING},
        {"blocking_lock_id", pb::STRING},
    };
    int64_t table_id = construct_table("INNODB_LOCK_WAITS", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_metrics() {
    FieldVec fields {
        {"NAME", pb::STRING},
        {"SUBSYSTEM", pb::STRING},
        {"COUNT", pb::INT64},
        {"MAX_COUNT", pb::INT64},
        {"MIN_COUNT", pb::INT64},
        {"AVG_COUNT", pb::DOUBLE},
        {"COUNT_RESET", pb::INT64},
        {"MAX_COUNT_RESET", pb::INT64},
        {"MIN_COUNT_RESET", pb::INT64},
        {"AVG_COUNT_RESET", pb::DOUBLE},
        {"TIME_ENABLED", pb::DATETIME},
        {"TIME_DISABLED", pb::DATETIME},
        {"TIME_ELAPSED", pb::INT64},
        {"TIME_RESET", pb::DATETIME},
        {"STATUS", pb::STRING},
        {"TYPE", pb::STRING},
        {"COMMENT", pb::STRING},
    };
    int64_t table_id = construct_table("INNODB_METRICS", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_sys_columns() {
    FieldVec fields {
        {"TABLE_ID", pb::UINT64},
        {"NAME", pb::STRING},
        {"POS", pb::UINT64},
        {"MTYPE", pb::INT32},
        {"PRTYPE", pb::INT32},
        {"LEN", pb::INT32},
    };
    int64_t table_id = construct_table("INNODB_SYS_COLUMNS", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_sys_datafiles() {
    FieldVec fields {
        {"SPACE", pb::UINT32},
        {"PATH", pb::STRING},
    };
    int64_t table_id = construct_table("INNODB_SYS_DATAFILES", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_sys_fields() {
    FieldVec fields {
        {"INDEX_ID", pb::UINT64},
        {"NAME", pb::STRING},
        {"POS", pb::UINT32},
    };
    int64_t table_id = construct_table("INNODB_SYS_FIELDS", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_sys_foreign() {
    FieldVec fields {
        {"ID", pb::STRING},
        {"FOR_NAME", pb::STRING},
        {"REF_NAME", pb::STRING},
        {"N_COLS", pb::UINT32},
        {"TYPE", pb::UINT32},
    };
    int64_t table_id = construct_table("INNODB_SYS_FOREIGN", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_sys_foreign_cols() {
    FieldVec fields {
        {"ID", pb::STRING},
        {"FOR_COL_NAME", pb::STRING},
        {"REF_COL_NAME", pb::STRING},
        {"POS", pb::UINT32},
    };
    int64_t table_id = construct_table("INNODB_SYS_FOREIGN_COLS", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_sys_indexes() {
    FieldVec fields {
        {"INDEX_ID", pb::UINT64},
        {"NAME", pb::STRING},
        {"TABLE_ID", pb::UINT64},
        {"TYPE", pb::INT32},
        {"N_FIELDS", pb::INT32},
        {"PAGE_NO", pb::INT32},
        {"SPACE", pb::INT32},
    };
    int64_t table_id = construct_table("INNODB_SYS_INDEXES", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_sys_tables() {
    FieldVec fields {
        {"TABLE_ID", pb::UINT64},
        {"NAME", pb::STRING},
        {"FLAG", pb::INT32},
        {"N_COLS", pb::INT32},
        {"SPACE", pb::INT32},
        {"FILE_FORMAT", pb::STRING},
        {"ROW_FORMAT", pb::STRING},
        {"ZIP_PAGE_SIZE", pb::UINT32},
    };
    int64_t table_id = construct_table("INNODB_SYS_TABLES", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_sys_tablespaces() {
    FieldVec fields {
        {"SPACE", pb::UINT32},
        {"NAME", pb::STRING},
        {"FLAG", pb::UINT32},
        {"FILE_FORMAT", pb::STRING},
        {"ROW_FORMAT", pb::STRING},
        {"PAGE_SIZE", pb::UINT32},
        {"ZIP_PAGE_SIZE", pb::UINT32},
    };
    int64_t table_id = construct_table("INNODB_SYS_TABLESPACES", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_sys_tablestats() {
    FieldVec fields {
        {"TABLE_ID", pb::UINT64},
        {"NAME", pb::STRING},
        {"STATS_INITIALIZED", pb::STRING},
        {"NUM_ROWS", pb::UINT64},
        {"CLUST_INDEX_SIZE", pb::UINT64},
        {"OTHER_INDEX_SIZE", pb::UINT64},
        {"MODIFIED_COUNTER", pb::UINT64},
        {"AUTOINC", pb::UINT64},
        {"REF_COUNT", pb::INT32},
    };
    int64_t table_id = construct_table("INNODB_SYS_TABLESTATS", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_innodb_trx() {
    FieldVec fields {
        {"trx_id", pb::STRING},
        {"trx_state", pb::STRING},
        {"trx_started", pb::DATETIME},
        {"trx_requested_lock_id", pb::STRING},
        {"trx_wait_started", pb::DATETIME},
        {"trx_weight", pb::UINT64},
        {"trx_mysql_thread_id", pb::UINT64},
        {"trx_query", pb::STRING},
        {"trx_operation_state", pb::STRING},
        {"trx_tables_in_use", pb::UINT64},
        {"trx_tables_locked", pb::UINT64},
        {"trx_lock_structs", pb::UINT64},
        {"trx_lock_memory_bytes", pb::UINT64},
        {"trx_rows_locked", pb::UINT64},
        {"trx_rows_modified", pb::UINT64},
        {"trx_concurrency_tickets", pb::UINT64},
        {"trx_isolation_level", pb::STRING},
        {"trx_unique_checks", pb::INT32},
        {"trx_foreign_key_checks", pb::INT32},
        {"trx_last_foreign_key_error", pb::STRING},
        {"trx_adaptive_hash_latched", pb::INT32},
        {"trx_adaptive_hash_timeout", pb::UINT64},
        {"trx_is_read_only", pb::INT32},
        {"trx_autocommit_non_locking", pb::INT32},
    };
    int64_t table_id = construct_table("INNODB_TRX", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_optimizer_trace() {
    FieldVec fields {
        {"QUERY", pb::STRING},
        {"TRACE", pb::STRING},
        {"MISSING_BYTES_BEYOND_MAX_MEM_SIZE", pb::INT32},
        {"INSUFFICIENT_PRIVILEGES", pb::INT32},
    };
    int64_t table_id = construct_table("OPTIMIZER_TRACE", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_parameters() {
    FieldVec fields {
        {"SPECIFIC_CATALOG", pb::STRING},
        {"SPECIFIC_SCHEMA", pb::STRING},
        {"SPECIFIC_NAME", pb::STRING},
        {"ORDINAL_POSITION", pb::INT32},
        {"PARAMETER_MODE", pb::STRING},
        {"PARAMETER_NAME", pb::STRING},
        {"DATA_TYPE", pb::STRING},
        {"CHARACTER_MAXIMUM_LENGTH", pb::INT32},
        {"CHARACTER_OCTET_LENGTH", pb::INT32},
        {"NUMERIC_PRECISION", pb::UINT64},
        {"NUMERIC_SCALE", pb::INT32},
        {"DATETIME_PRECISION", pb::UINT64},
        {"CHARACTER_SET_NAME", pb::STRING},
        {"COLLATION_NAME", pb::STRING},
        {"DTD_IDENTIFIER", pb::STRING},
        {"ROUTINE_TYPE", pb::STRING},
    };
    int64_t table_id = construct_table("PARAMETERS", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_partitions() {
    FieldVec fields {
        {"TABLE_CATALOG", pb::STRING},
        {"TABLE_SCHEMA", pb::STRING},
        {"TABLE_NAME", pb::STRING},
        {"PARTITION_NAME", pb::STRING},
        {"SUBPARTITION_NAME", pb::STRING},
        {"PARTITION_ORDINAL_POSITION", pb::UINT64},
        {"SUBPARTITION_ORDINAL_POSITION", pb::UINT64},
        {"PARTITION_METHOD", pb::STRING},
        {"SUBPARTITION_METHOD", pb::STRING},
        {"PARTITION_EXPRESSION", pb::STRING},
        {"SUBPARTITION_EXPRESSION", pb::STRING},
        {"PARTITION_DESCRIPTION", pb::STRING},
        {"TABLE_ROWS", pb::UINT64},
        {"AVG_ROW_LENGTH", pb::UINT64},
        {"DATA_LENGTH", pb::UINT64},
        {"MAX_DATA_LENGTH", pb::UINT64},
        {"INDEX_LENGTH", pb::UINT64},
        {"DATA_FREE", pb::UINT64},
        {"CREATE_TIME", pb::DATETIME},
        {"UPDATE_TIME", pb::DATETIME},
        {"CHECK_TIME", pb::DATETIME},
        {"CHECKSUM", pb::UINT64},
        {"PARTITION_COMMENT", pb::STRING},
        {"NODEGROUP", pb::STRING},
        {"TABLESPACE_NAME", pb::STRING},
    };
    int64_t table_id = construct_table("PARTITIONS", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_plugins() {
    FieldVec fields {
        {"PLUGIN_NAME", pb::STRING},
        {"PLUGIN_VERSION", pb::STRING},
        {"PLUGIN_STATUS", pb::STRING},
        {"PLUGIN_TYPE", pb::STRING},
        {"PLUGIN_TYPE_VERSION", pb::STRING},
        {"PLUGIN_LIBRARY", pb::STRING},
        {"PLUGIN_LIBRARY_VERSION", pb::STRING},
        {"PLUGIN_AUTHOR", pb::STRING},
        {"PLUGIN_DESCRIPTION", pb::STRING},
        {"PLUGIN_LICENSE", pb::STRING},
        {"LOAD_OPTION", pb::STRING},
    };
    int64_t table_id = construct_table("PLUGINS", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_processlist() {
    FieldVec fields {
        {"ID", pb::UINT64},
        {"USER", pb::STRING},
        {"HOST", pb::STRING},
        {"DB", pb::STRING},
        {"COMMAND", pb::STRING},
        {"TIME", pb::INT32},
        {"STATE", pb::STRING},
        {"INFO", pb::STRING},
    };
    int64_t table_id = construct_table("PROCESSLIST", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_profiling() {
    FieldVec fields {
        {"QUERY_ID", pb::INT32},
        {"SEQ", pb::INT32},
        {"STATE", pb::STRING},
        {"DURATION", pb::DOUBLE},
        {"CPU_USER", pb::DOUBLE},
        {"CPU_SYSTEM", pb::DOUBLE},
        {"CONTEXT_VOLUNTARY", pb::INT32},
        {"CONTEXT_INVOLUNTARY", pb::INT32},
        {"BLOCK_OPS_IN", pb::INT32},
        {"BLOCK_OPS_OUT", pb::INT32},
        {"MESSAGES_SENT", pb::INT32},
        {"MESSAGES_RECEIVED", pb::INT32},
        {"PAGE_FAULTS_MAJOR", pb::INT32},
        {"PAGE_FAULTS_MINOR", pb::INT32},
        {"SWAPS", pb::INT32},
        {"SOURCE_FUNCTION", pb::STRING},
        {"SOURCE_FILE", pb::STRING},
        {"SOURCE_LINE", pb::INT32},
    };
    int64_t table_id = construct_table("PROFILING", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_schema_privileges() {
    FieldVec fields {
        {"GRANTEE", pb::STRING},
        {"TABLE_CATALOG", pb::STRING},
        {"TABLE_SCHEMA", pb::STRING},
        {"PRIVILEGE_TYPE", pb::STRING},
        {"IS_GRANTABLE", pb::STRING},
    };
    int64_t table_id = construct_table("SCHEMA_PRIVILEGES", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_session_status() {
    FieldVec fields {
        {"VARIABLE_NAME", pb::STRING},
        {"VARIABLE_VALUE", pb::STRING},
    };
    int64_t table_id = construct_table("SESSION_STATUS", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_session_variables() {
    FieldVec fields {
        {"VARIABLE_NAME", pb::STRING},
        {"VARIABLE_VALUE", pb::STRING},
    };
    int64_t table_id = construct_table("SESSION_VARIABLES", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_table_constraints() {
    FieldVec fields {
        {"CONSTRAINT_CATALOG", pb::STRING},
        {"CONSTRAINT_SCHEMA", pb::STRING},
        {"CONSTRAINT_NAME", pb::STRING},
        {"TABLE_SCHEMA", pb::STRING},
        {"TABLE_NAME", pb::STRING},
        {"CONSTRAINT_TYPE", pb::STRING},
    };
    int64_t table_id = construct_table("TABLE_CONSTRAINTS", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_table_privileges() {
    FieldVec fields {
        {"GRANTEE", pb::STRING},
        {"TABLE_CATALOG", pb::STRING},
        {"TABLE_SCHEMA", pb::STRING},
        {"TABLE_NAME", pb::STRING},
        {"PRIVILEGE_TYPE", pb::STRING},
        {"IS_GRANTABLE", pb::STRING},
    };
    int64_t table_id = construct_table("TABLE_PRIVILEGES", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_tablespaces() {
    FieldVec fields {
        {"TABLESPACE_NAME", pb::STRING},
        {"ENGINE", pb::STRING},
        {"TABLESPACE_TYPE", pb::STRING},
        {"LOGFILE_GROUP_NAME", pb::STRING},
        {"EXTENT_SIZE", pb::UINT64},
        {"AUTOEXTEND_SIZE", pb::UINT64},
        {"MAXIMUM_SIZE", pb::UINT64},
        {"NODEGROUP_ID", pb::UINT64},
        {"TABLESPACE_COMMENT", pb::STRING},
    };
    int64_t table_id = construct_table("TABLESPACES", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}

void InformationSchema::init_user_privileges() {
    FieldVec fields {
        {"GRANTEE", pb::STRING},
        {"TABLE_CATALOG", pb::STRING},
        {"PRIVILEGE_TYPE", pb::STRING},
        {"IS_GRANTABLE", pb::STRING},
    };
    int64_t table_id = construct_table("USER_PRIVILEGES", fields);
    _calls[table_id] = [table_id](RuntimeState* state, std::vector<ExprNode*>& conditions) ->
        std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
    };
}
} // namespace baikaldb
