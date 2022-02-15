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
} // namespace baikaldb
