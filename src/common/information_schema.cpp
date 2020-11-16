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
    return 0;
}

int64_t InformationSchema::construct_table(const std::string& table_name, FieldVec& fields) {
    auto& table = _tables[table_name];
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
// BaikalDB专用表
void InformationSchema::init_partition_split_info() {
    // 定义字段信息
    FieldVec fields {
        {"partition_key", pb::STRING},
        {"table_name", pb::STRING},
        {"split_info", pb::STRING},
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
            std::string partition_key;
            auto type1 = index_ptr->fields[0].type;
            auto type2 = index_ptr->fields[1].type;
            records.reserve(10000);
            for (auto& pair : region_infos) {
                TableKey start_key(pair.second.start_key());
                int pos = 0;
                partition_key = start_key.decode_start_key_string(type1, pos);
                if (partition_key != last_partition_key) {
                    if (last_keys.size() > 1) {
                        auto record = factory->new_record(table_id);
                        record->set_string(record->get_field_by_name("partition_key"), last_partition_key);
                        record->set_string(record->get_field_by_name("table_name"), table_name);
                        record->set_string(record->get_field_by_name("split_info"), boost::join(last_keys, ","));
                        records.emplace_back(record);
                    }
                    last_partition_key = partition_key;
                    last_keys.clear();
                }
                last_keys.emplace_back(start_key.decode_start_key_string(type2, pos));
            }
            if (last_keys.size() > 1) {
                auto record = factory->new_record(table_id);
                record->set_string(record->get_field_by_tag(1), last_partition_key);
                record->set_string(record->get_field_by_tag(2), table_name);
                record->set_string(record->get_field_by_tag(3), boost::join(last_keys, ","));
                records.emplace_back(record);
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
        {"start_key", pb::STRING},
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
                    record->set_string(record->get_field_by_name("COLUMN_TYPE"), to_mysql_type_full_string(field.type));
                    record->set_string(record->get_field_by_name("PRIVILEGES"), "select,insert,update,references");
                    record->set_string(record->get_field_by_name("COLUMN_COMMENT"), field.comment);
                    records.emplace_back(record);
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
} // namespace baikaldb
