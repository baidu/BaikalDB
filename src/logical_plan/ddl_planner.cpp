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

#include "ddl_planner.h"
#include "expr_value.h"
#include "meta_server_interact.hpp"
#include "proto/meta.interface.pb.h"
#include "expr_node.h"
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string.hpp>
#include <rapidjson/reader.h>
#include <rapidjson/document.h>

namespace baikaldb {
DEFINE_bool(unique_index_default_global, true, "unique_index_default_global");
DEFINE_bool(normal_index_default_global, false, "normal_index_default_global");
int DDLPlanner::plan() {
    pb::MetaManagerRequest request;
    // only CREATE TABLE is supported
    MysqlErrCode error_code = ER_ERROR_COMMON;
    if (_ctx->stmt_type == parser::NT_CREATE_TABLE) {
        request.set_op_type(pb::OP_CREATE_TABLE);
        pb::SchemaInfo *table = request.mutable_table_info();
        if (0 != parse_create_table(*table)) {
            DB_WARNING("parser create table command failed");
            return -1;
        }
        error_code = ER_CANT_CREATE_TABLE;
    } else if (_ctx->stmt_type == parser::NT_DROP_TABLE) {
        request.set_op_type(pb::OP_DROP_TABLE);
        pb::SchemaInfo *table = request.mutable_table_info();
        if (0 != parse_drop_table(*table)) {
            DB_WARNING("parser drop table command failed");
            return -1;
        }
    } else if (_ctx->stmt_type == parser::NT_RESTORE_TABLE) {
        request.set_op_type(pb::OP_RESTORE_TABLE);
        pb::SchemaInfo *table = request.mutable_table_info();
        if (0 != parse_restore_table(*table)) {
            DB_WARNING("parser restore table command failed");
            return -1;
        }
    } else if (_ctx->stmt_type == parser::NT_CREATE_DATABASE) {
        request.set_op_type(pb::OP_CREATE_DATABASE);
        pb::DataBaseInfo *database = request.mutable_database_info();
        if (0 != parse_create_database(*database)) {
            DB_WARNING("parser create database command failed");
            return -1;
        }
        error_code = ER_CANT_CREATE_DB;
    } else if (_ctx->stmt_type == parser::NT_DROP_DATABASE) {
        request.set_op_type(pb::OP_DROP_DATABASE);
        pb::DataBaseInfo *database = request.mutable_database_info();
        if (0 != parse_drop_database(*database)) {
            DB_WARNING("parser drop database command failed");
            return -1;
        }
    } else if (_ctx->stmt_type == parser::NT_ALTER_TABLE) {
        int ret = parse_alter_table(request);
        if (ret == -1) {
            DB_WARNING("parser alter table command failed");
            return -1;
        } 
        error_code = ER_ALTER_OPERATION_NOT_SUPPORTED;
    } else {
        DB_WARNING("unsupported DDL command: %d", _ctx->stmt_type);
        return -1;
    }
    pb::MetaManagerResponse response;
    if (MetaServerInteract::get_instance()->send_request("meta_manager", request, response) != 0) {
        if (response.errcode() != pb::SUCCESS && _ctx->stat_info.error_code == ER_ERROR_FIRST) {
            _ctx->stat_info.error_code = error_code;
            _ctx->stat_info.error_msg << response.errmsg();
        }
        DB_WARNING("send_request fail");
        return -1;
    }

    return 0;
}

int DDLPlanner::add_column_def(pb::SchemaInfo& table, parser::ColumnDef* column) {
    pb::FieldInfo* field = table.add_fields();
    if (column->name == nullptr || column->name->name.empty()) {
        DB_WARNING("column_name is empty");
        return -1;
    }
    field->set_field_name(column->name->name.value);
    if (column->type == nullptr) {
        DB_WARNING("data_type is empty for column: %s", column->name->name.value);
        return -1;
    }
    pb::PrimitiveType data_type = to_baikal_type(column->type);
    if (data_type == pb::INVALID_TYPE) {
        DB_WARNING("data_type is unsupported: %s", column->name->name.value);
        return -1;
    }
    field->set_mysql_type(data_type);
    int option_len = column->options.size();
    for (int opt_idx = 0; opt_idx < option_len; ++opt_idx) {
        parser::ColumnOption* col_option = column->options[opt_idx];
        if (col_option->type == parser::COLUMN_OPT_NOT_NULL) {
            field->set_can_null(false);
        } else if (col_option->type == parser::COLUMN_OPT_NULL) {
            field->set_can_null(true);
            _column_can_null[column->name->name.value] = true;
        } else if (col_option->type == parser::COLUMN_OPT_AUTO_INC) {
            field->set_auto_increment(true);
        } else if (col_option->type == parser::COLUMN_OPT_PRIMARY_KEY) {
            pb::IndexInfo* index = table.add_indexs();
            index->set_index_name("primary_key");
            index->set_index_type(pb::I_PRIMARY);
            index->add_field_names(column->name->name.value);
        } else if (col_option->type == parser::COLUMN_OPT_UNIQ_KEY) {
            pb::IndexInfo* index = table.add_indexs();
            std::string col_name(column->name->name.value);
            index->set_index_name(col_name + "_key");
            index->set_index_type(pb::I_UNIQ);
            index->add_field_names(col_name);
        } else if (col_option->type == parser::COLUMN_OPT_DEFAULT_VAL && col_option->expr != nullptr) {
            if (col_option->expr->to_string() == "(current_timestamp())") {
                if (is_current_timestamp_specic(data_type)) {
                    field->set_default_literal(ExprValue::Now().get_string());
                    field->set_default_value("(current_timestamp())");
                    continue;
                } else {
                    DB_WARNING("invalid default value for '%s'", column->name->name.value);
                    _ctx->stat_info.error_code = ER_INVALID_DEFAULT;
                    _ctx->stat_info.error_msg << "invalid default value for '" << column->name->name.value << "'";
                    return -1;
                }
            } 
            if (col_option->expr->expr_type != parser::ET_LITETAL) {
                DB_WARNING("Invalid default value for '%s'", column->name->name.value);
                _ctx->stat_info.error_code = ER_INVALID_DEFAULT;
                _ctx->stat_info.error_msg << "Invalid default value for '" << column->name->name.value << "'";
                return -1;
            }
            parser::LiteralExpr* lit = static_cast<parser::LiteralExpr*>(col_option->expr);
            // default null不需要存
            if (lit->literal_type != parser::LT_NULL) {
                std::string str = lit->to_string();
                // 简单校验下类型
                if (is_int(data_type) || is_double(data_type)) {
                    bool is_int_format = std::all_of(str.begin(), str.end(), 
                            [](char i) {
                                return (i >= '0' && i <= '9') || i == '-' || i == '.';
                            });
                    if (!is_int_format) {
                        DB_WARNING("Invalid default value for '%s'", column->name->name.value);
                        _ctx->stat_info.error_code = ER_INVALID_DEFAULT;
                        _ctx->stat_info.error_msg << "Invalid default value for '" << column->name->name.value << "'";
                        return -1;
                    }
                } else if (is_datetime_specic(data_type)) {
                    bool is_datetime_format = std::all_of(str.begin(), str.end(),
                            [](char i) {
                                return (i >= '0' && i <= '9') || i == '-' || i == '.' || i == ' ' || i == ':';
                            });
                    if (!is_datetime_format) {
                        DB_WARNING("Invalid default value for '%s'", column->name->name.value);
                        _ctx->stat_info.error_code = ER_INVALID_DEFAULT;
                        _ctx->stat_info.error_msg << "Invalid default value for '" << column->name->name.value << "'";
                        return -1;
                    }
                }
                field->set_default_value(lit->to_string());
            }
        } else if (col_option->type == parser::COLUMN_OPT_COMMENT) {
            field->set_comment(col_option->expr->to_string());
        } else if (col_option->type == parser::COLUMN_OPT_ON_UPDATE) {
            field->set_on_update_value(col_option->expr->to_string());
        } else {
            DB_WARNING("unsupported column option type: %d", col_option->type);
            return -1;
        }
    }
    // can_null default is true
    if (!field->has_can_null()) {
        field->set_can_null(true);
    }
    field->set_flag(column->type->flag);
    return 0;
}


int DDLPlanner::pre_split_index(const std::string& start_key,
                                const std::string& end_key,
                                int32_t region_num,
                                pb::SchemaInfo& table,
                                const pb::IndexInfo* pk_index,
                                const pb::IndexInfo* index,
                                const std::vector<const pb::FieldInfo*>& pk_index_fields,
                                const std::vector<const pb::FieldInfo*>& index_fields) {
    if (pk_index == nullptr || index == nullptr || index_fields.size() == 0 || pk_index_fields.size() == 0) {
        return 0;
    }
    std::set<std::string> index_filed_names;
    for (auto filed : index_fields) {
        if (filed == nullptr) {
            DB_FATAL("filed nullptr");
            return -1;
        }
        index_filed_names.insert(filed->field_name());
    }

    auto fill_other_fileds = [index_fields, index, index_filed_names, pk_index_fields](MutTableKey& key) -> int {
        for(auto i = 1; i < index_fields.size(); ++i) {
            if (index_fields[i] == nullptr) {
                return -1;
            }
            ExprValue value(index_fields[i]->mysql_type(), "");
            key.append_value(value);
        }
        if (index->index_type() == pb::I_KEY) {
            // 不是unique的全局索引，需要补齐其他主键字段
            for(auto field : pk_index_fields) {
                if (field == nullptr) {
                    return -1;
                }
                if (index_filed_names.find(field->field_name()) == index_filed_names.end()) {
                    ExprValue value(field->mysql_type(), "");
                    key.append_value(value);
                }
            }
        }
        if (index->is_global()) {
            uint8_t null_flag = 0;
            key.append_u8(null_flag);
        }
        return 0;
    };
    auto split_keys = table.add_split_keys();
    split_keys->set_index_name(index->index_name());
    switch (index_fields[0]->mysql_type()) {
        case pb::INT8:
        case pb::INT16:
        case pb::INT32:
        case pb::INT64: {
                int64_t start_value_i = strtoll(start_key.c_str(), NULL, 10);
                int64_t end_value_i = strtoll(end_key.c_str(), NULL, 10);
                if (region_num <= 0 || end_value_i < start_value_i) {
                    DB_FATAL("pre split param not valid");
                    return -1;
                }
                int64_t step = (end_value_i - start_value_i) / region_num;
                int64_t split_value = start_value_i;
                if (step <= 0) {
                    return -1;
                }
                // 生成split key
                for (; split_value <= end_value_i; split_value += step) {
                    MutTableKey key;
                    if (index->is_global()) {
                        uint8_t null_flag = 0;
                        key.append_u8(null_flag);
                    }
                    ExprValue first_filed_value(index_fields[0]->mysql_type(), std::to_string(split_value));
                    key.append_value(first_filed_value);
                    if (fill_other_fileds(key) < 0) {
                        return -1;
                    }
                    split_keys->add_split_keys(key.data());
                }
            }
            break;
        case pb::UINT8:
        case pb::UINT16:
        case pb::UINT32:
        case pb::UINT64: {
                uint64_t start_value_u = strtoull(start_key.c_str(), NULL, 10);
                uint64_t end_value_u = strtoull(end_key.c_str(), NULL, 10);
                if (region_num <= 0 || end_value_u < start_value_u) {
                    DB_FATAL("pre split param not valid");
                    return -1;
                }
                uint64_t step = (end_value_u - start_value_u) / region_num;
                uint64_t split_value = start_value_u;
                if (step <= 0) {
                    return -1;
                }
                // 生成split key
                for (; split_value <= end_value_u; split_value += step) {
                    MutTableKey key;
                    if (index->is_global()) {
                        uint8_t null_flag = 0;
                        key.append_u8(null_flag);
                    }
                    ExprValue first_filed_value(index_fields[0]->mysql_type(), std::to_string(split_value));
                    key.append_value(first_filed_value);
                    if (fill_other_fileds(key) < 0) {
                        return -1;
                    }
                    split_keys->add_split_keys(key.data());
                }
            }
            break;
        case pb::FLOAT:
        case pb::DOUBLE: {
                double start_value_d = strtod(start_key.c_str(), NULL);
                double end_value_d = strtod(end_key.c_str(), NULL);
                if (region_num <= 0 || end_value_d < start_value_d) {
                    DB_FATAL("pre split param not valid");
                    return -1;
                }
                double step = (end_value_d - start_value_d) / region_num;
                double split_value = start_value_d;
                if (step <= 1e-6) {
                    return -1;
                }
                // 生成split key
                for (; split_value <= end_value_d; split_value += step) {
                    MutTableKey key;
                    if (index->is_global()) {
                        uint8_t null_flag = 0;
                        key.append_u8(null_flag);
                    }
                    ExprValue first_filed_value(index_fields[0]->mysql_type(), std::to_string(split_value));
                    key.append_value(first_filed_value);
                    if (fill_other_fileds(key) < 0) {
                        return -1;
                    }
                    split_keys->add_split_keys(key.data());
                }
            }
            break;
        case pb::STRING: {
                if (region_num <= 0 || end_key < start_key) {
                    DB_FATAL("pre split param not valid: start_key: %s, end_key: %s, region_num: %d",
                             start_key.c_str(), end_key.c_str(), region_num);
                    return -1;
                }
                ExprValue start_key_value(pb::STRING);
                ExprValue end_key_value(pb::STRING);
                start_key_value.str_val = start_key;
                end_key_value.str_val = end_key;
                // 最长公共前缀
                int prefix_len = start_key_value.common_prefix_length(end_key_value);
                // 后取8个字节，计算diff
                uint64_t start_uint64 = start_key_value.unit64_value(prefix_len);
                uint64_t end_uint64 = end_key_value.unit64_value(prefix_len);
                uint64_t step = (end_uint64 - start_uint64) / region_num;
                std::string common_prefix = start_key.substr(0, prefix_len);
                if (step <= 0) {
                    return -1;
                }
                DB_WARNING("start_uint: %lu, end_uint: %lu, step: %lu, prefix_len: %d, common_prefix: %s",
                           start_uint64, end_uint64, step, prefix_len, common_prefix.c_str());
                for (; start_uint64 <= end_uint64; start_uint64 += step) {
                    // uint64转string
                    uint64_t val = start_uint64;
                    std::string key;
                    while (val > 0) {
                        key += char(val & 0xFF);
                        val >>= 8;
                    }
                    std::reverse(key.begin(), key.end());
                    DB_WARNING("uint64: %lu, key: %s", start_uint64, key.c_str());
                    // 生成key
                    MutTableKey split_key;
                    if (index->is_global()) {
                        uint8_t null_flag = 0;
                        split_key.append_u8(null_flag);
                    }
                    ExprValue first_filed_value(index_fields[0]->mysql_type(), common_prefix + key);
                    split_key.append_value(first_filed_value);
                    if (fill_other_fileds(split_key) < 0) {
                        return -1;
                    }
                    split_keys->add_split_keys(split_key.data());
                }
            }
            break;
        default:
            DB_FATAL("not support type: %d for pre split", index_fields[0]->mysql_type());
            return -1;
    }
    return 0;
}


int DDLPlanner::parse_pre_split_keys(std::string start_key,
                                     std::string end_key,
                                     std::string global_start_key,
                                     std::string global_end_key,
                                     int32_t region_num,
                                     pb::SchemaInfo& table) {
    std::unordered_map<std::string, const pb::FieldInfo*> fields;
    std::vector<const pb::FieldInfo*> primary_index_fields;
    std::vector<const pb::FieldInfo*> global_index_fields;
    const pb::IndexInfo* primary_index = nullptr;
    const pb::IndexInfo* gloabal_index = nullptr;
    DB_WARNING("split_start_key: %s, split_end_key: %s, region_num: %d, pb: %s",
               start_key.c_str(), end_key.c_str(), region_num, table.ShortDebugString().c_str());
    for (auto& filed : table.fields()) {
        fields[filed.field_name()] = &filed;
    }
    for (auto& index_info : table.indexs()) {
        if (index_info.index_type() == pb::I_PRIMARY) {
            primary_index = &index_info;
            for (auto& field_name : index_info.field_names()) {
                if (fields.find(field_name) == fields.end()) {
                    DB_WARNING("no matching filed: %s, table: %s", field_name.c_str(), table.ShortDebugString().c_str());
                    return -1;
                }
                primary_index_fields.emplace_back(fields[field_name]);
            }
        }
        else if (index_info.is_global()) {
            gloabal_index = &index_info;
            for (auto& field_name : index_info.field_names()) {
                if (fields.find(field_name) == fields.end()) {
                    DB_WARNING("no matching filed: %s, table: %s", field_name.c_str(), table.ShortDebugString().c_str());
                    return -1;
                }
                global_index_fields.emplace_back(fields[field_name]);
            }
        }
    }
    if (primary_index_fields.size() == 0) {
        DB_FATAL("no primary index filed for pre split, table: %s", table.ShortDebugString().c_str());
        return -1;
    }

    int ret = pre_split_index(start_key, end_key, region_num, table, primary_index, primary_index, primary_index_fields, primary_index_fields);
    if (ret < 0) {
        return -1;
    }
    ret = pre_split_index(global_start_key, global_end_key, region_num, table, primary_index, gloabal_index, primary_index_fields, global_index_fields);
    if (ret < 0) {
        return -1;
    }
    return 0;
}

int DDLPlanner::parse_create_table(pb::SchemaInfo& table) {
    parser::CreateTableStmt* stmt = (parser::CreateTableStmt*)(_ctx->stmt);
    if (stmt->table_name == nullptr) {
        DB_WARNING("error: no table name specified");
        return -1;
    }
    if (stmt->table_name->db.empty()) {
        if (_ctx->cur_db.empty()) {
            _ctx->stat_info.error_code = ER_NO_DB_ERROR;
            _ctx->stat_info.error_msg << "No database selected";
            return -1;
        }
        table.set_database(_ctx->cur_db);
    } else {
        table.set_database(stmt->table_name->db.value);
    }
    table.set_table_name(stmt->table_name->table.value);
    table.set_partition_num(1);

    int columns_len = stmt->columns.size();
    for (int idx = 0; idx < columns_len; ++idx) {
        parser::ColumnDef* column = stmt->columns[idx];
        if (column == nullptr) {
            DB_WARNING("column is nullptr");
            return -1;
        }
        if (0 != add_column_def(table, column)) {
            DB_WARNING("add column to table failed.");
            return -1;
        }
    }

    bool can_support_ttl = true;
    bool has_arrow_fulltext = false;
    bool has_pb_fulltext = false;
    int constraint_len = stmt->constraints.size();
    std::string split_start_key;
    std::string split_end_key;
    std::string global_split_start_key;
    std::string global_split_end_key;
    int32_t split_region_num = 0;
    for (int idx = 0; idx < constraint_len; ++idx) {
        parser::Constraint* constraint = stmt->constraints[idx];
        pb::IndexInfo* index = table.add_indexs();
        if (constraint->type == parser::CONSTRAINT_PRIMARY) {
            index->set_index_type(pb::I_PRIMARY);
        } else if (constraint->type == parser::CONSTRAINT_INDEX) {
            index->set_index_type(pb::I_KEY);
            if (constraint->index_dist == parser::INDEX_DIST_DEFAULT) {
                index->set_is_global(FLAGS_normal_index_default_global);
            }
        } else if (constraint->type == parser::CONSTRAINT_UNIQ) {
            index->set_index_type(pb::I_UNIQ);
            if (constraint->index_dist == parser::INDEX_DIST_DEFAULT) {
                index->set_is_global(FLAGS_unique_index_default_global);
            }
        } else if (constraint->type == parser::CONSTRAINT_FULLTEXT) {
            index->set_index_type(pb::I_FULLTEXT);
            can_support_ttl = false;
        } else {
            DB_WARNING("unsupported constraint_type: %d", constraint->type);
            return -1;
        }
        if (constraint->index_dist == parser::INDEX_DIST_GLOBAL) {
            index->set_is_global(true);
        }
        if (constraint->type == parser::CONSTRAINT_PRIMARY) {
            index->set_index_name("primary_key");
        } else {
            if (constraint->name.empty()) {
                DB_WARNING("empty index name");
                return -1;
            }
            index->set_index_name(constraint->name.value);
        }
        if (constraint->index_option != nullptr) {
            rapidjson::Document root;
            const char* value = constraint->index_option->comment.c_str();
            try {
                root.Parse<0>(value);
                if (root.HasParseError()) {
                    rapidjson::ParseErrorCode code = root.GetParseError();
                    DB_WARNING("parse create table json comments error [code:%d][%s]", 
                        code, value);
                    return -1;
                }
                auto json_iter = root.FindMember("segment_type");
                if (json_iter != root.MemberEnd()) {
                    std::string segment_type = json_iter->value.GetString();
                    pb::SegmentType pb_segment_type = pb::S_DEFAULT;
                    SegmentType_Parse(segment_type, &pb_segment_type);
                    index->set_segment_type(pb_segment_type);
                }
                auto storage_type_iter = root.FindMember("storage_type");
                pb::StorageType pb_storage_type = pb::ST_ARROW;
                if (storage_type_iter != root.MemberEnd()) {
                    std::string storage_type = storage_type_iter->value.GetString();
                    StorageType_Parse(storage_type, &pb_storage_type);
                }
                if (!is_fulltext_type_constraint(pb_storage_type, has_arrow_fulltext, has_pb_fulltext)) {
                    DB_WARNING("fulltext has two types : pb&arrow"); 
                    return -1;
                }
                index->set_storage_type(pb_storage_type);
            } catch (...) {
                DB_WARNING("parse create table json comments error [%s]", value);
                return -1;
            }
        }
        for (int col_idx = 0; col_idx < constraint->columns.size(); ++col_idx) {
            parser::ColumnName* col_name = constraint->columns[col_idx];
            if (_column_can_null[col_name->name.value] && index->index_type() != pb::I_FULLTEXT) {
                DB_WARNING("index column : %s should NOT NULL", col_name->name.value);
                _ctx->stat_info.error_code = ER_NO_DB_ERROR;
                _ctx->stat_info.error_msg << "index column : " << col_name->name.value << " should NOT NULL";
                return -1;
            }
            index->add_field_names(col_name->name.value);
        }
    }

    int option_len = stmt->options.size();
    for (int idx = 0; idx < option_len; ++idx) {
        parser::TableOption* option = stmt->options[idx];
        if (option->type == parser::TABLE_OPT_ENGINE) {
            std::string str_val(option->str_value.value);
            table.set_engine(pb::ROCKSDB);
            if (boost::algorithm::iequals(str_val, "redis")) {
                table.set_engine(pb::REDIS);
                can_support_ttl = false;
            } else if (boost::algorithm::iequals(str_val, "rocksdb_cstore")) {
                table.set_engine(pb::ROCKSDB_CSTORE);
            } else if (boost::algorithm::iequals(str_val, "binlog")) {
                table.set_engine(pb::BINLOG);
                can_support_ttl = false;
            }
        } else if (option->type == parser::TABLE_OPT_CHARSET) {
            std::string str_val(option->str_value.value);
            if (boost::algorithm::iequals(str_val, "gbk")) {
                table.set_charset(pb::GBK);
            } else {
                table.set_charset(pb::UTF8);
            }
        } else if (option->type == parser::TABLE_OPT_AUTO_INC) {
            table.set_auto_increment_increment(option->uint_value);
        } else if (option->type == parser::TABLE_OPT_AVG_ROW_LENGTH) {
            table.set_byte_size_per_record(option->uint_value);
        } else if (option->type == parser::TABLE_OPT_COMMENT) {
            rapidjson::Document root;
            try {
                root.Parse<0>(option->str_value.value);
                if (root.HasParseError()) {
                    // 兼容mysql语法
                    table.set_comment(option->str_value.value);
                    rapidjson::ParseErrorCode code = root.GetParseError();
                    DB_WARNING("parse create table json comments error [code:%d][%s]", 
                        code, option->str_value.value);
                    continue;
//                    return -1;
                }
                auto json_iter = root.FindMember("resource_tag");
                if (json_iter != root.MemberEnd()) {
                    std::string resource_tag = json_iter->value.GetString();
                    table.set_resource_tag(resource_tag);
                    DB_WARNING("resource_tag: %s", resource_tag.c_str());
                }
                json_iter = root.FindMember("namespace");
                if (json_iter != root.MemberEnd()) {
                    std::string namespace_ = json_iter->value.GetString();
                    table.set_namespace_name(namespace_);
                    DB_WARNING("namespace: %s", namespace_.c_str());
                }
                json_iter = root.FindMember("replica_num");
                if (json_iter != root.MemberEnd()) {
                    int64_t replica_num = json_iter->value.GetInt64();
                    table.set_replica_num(replica_num);
                    DB_WARNING("replica_num: %ld", replica_num);
                }
                json_iter = root.FindMember("dists");
                std::set<std::string> logical_room_set;
                if (json_iter != root.MemberEnd()) {
                    if (json_iter->value.IsArray()) {
                        for (size_t i = 0; i < json_iter->value.Size(); i++) {
                            const rapidjson::Value& dist_value = json_iter->value[i];
                            auto* dist = table.add_dists();
                            std::string logical_room = dist_value["logical_room"].GetString();
                            dist->set_logical_room(logical_room);
                            dist->set_count(dist_value["count"].GetInt());
                            logical_room_set.emplace(logical_room);
                        }
                    }
                }
                json_iter = root.FindMember("main_logical_room");
                if (json_iter != root.MemberEnd()) {
                    std::string main_logical_room = json_iter->value.GetString();
                    if (logical_room_set.count(main_logical_room) == 0) {
                        _ctx->stat_info.error_code = ER_SP_LABEL_MISMATCH;
                        _ctx->stat_info.error_msg << "main_logical_room: "<<main_logical_room << " mismatch";
                        return -1;
                    }
                    table.set_main_logical_room(main_logical_room);
                    DB_WARNING("main_logical_room: %s", main_logical_room.c_str());
                }
                json_iter = root.FindMember("region_split_lines");
                if (json_iter != root.MemberEnd()) {
                    int64_t region_split_lines = json_iter->value.GetInt64();
                    table.set_region_split_lines(region_split_lines);
                    DB_WARNING("region_split_lines: %ld", region_split_lines);
                }
                json_iter = root.FindMember("ttl_duration");
                if (json_iter != root.MemberEnd()) {
                    if (!can_support_ttl) {
                        DB_FATAL("fulltext/engine!=rocksdb  can not create ttl table");
                        return -1;
                    }
                    int64_t ttl_duration = json_iter->value.GetInt64();
                    table.set_ttl_duration(ttl_duration);
                    table.set_online_ttl_expire_time_us(0);
                    DB_WARNING("ttl_duration: %ld", ttl_duration);
                }
                json_iter = root.FindMember("storage_compute_separate");
                if (json_iter != root.MemberEnd()) {
                    int64_t separate = json_iter->value.GetInt64();
                    auto* schema_conf = table.mutable_schema_conf();
                    if (separate == 0) {
                        schema_conf->set_storage_compute_separate(false);
                    } else {
                        schema_conf->set_storage_compute_separate(true);
                    }
                    DB_WARNING("storage_compute_separate: %ld", separate);
                }
                json_iter = root.FindMember("region_num");
                if (json_iter != root.MemberEnd() && root["region_num"].IsNumber()) {
                    int32_t region_num = json_iter->value.GetInt64();
                    table.set_region_num(region_num);
                }

                json_iter = root.FindMember("learner_resource_tag");
                if (json_iter != root.MemberEnd()) {
                    if (root["learner_resource_tag"].IsString()) {
                        if (table.resource_tag() == json_iter->value.GetString()) {
                            DB_FATAL("learner must use different resource tag.");
                            return -1;
                        }
                        table.add_learner_resource_tags(json_iter->value.GetString());
                    } else if (root["learner_resource_tag"].IsArray()) {
                        for (size_t i = 0; i < json_iter->value.Size(); i++) {
                            const rapidjson::Value& learner_value = json_iter->value[i];
                            if (learner_value.IsString()) {
                                table.add_learner_resource_tags(learner_value.GetString());
                            }
                        }
                    }
                }
                json_iter = root.FindMember("comment");
                if (json_iter != root.MemberEnd()) {
                    if (json_iter->value.IsString()) {
                        std::string comment = json_iter->value.GetString();
                        table.set_comment(comment);
                        DB_WARNING("comment: %s", comment.c_str());
                    }
                }
                // 预分裂相关参数
                json_iter = root.FindMember("split_start_key");
                if (json_iter != root.MemberEnd() && root["split_start_key"].IsString()) {
                    split_start_key = json_iter->value.GetString();
                }
                json_iter = root.FindMember("split_end_key");
                if (json_iter != root.MemberEnd() && root["split_end_key"].IsString()) {
                    split_end_key = json_iter->value.GetString();
                }
                json_iter = root.FindMember("global_split_start_key");
                if (json_iter != root.MemberEnd() && root["global_split_start_key"].IsString()) {
                    global_split_start_key = json_iter->value.GetString();
                }
                json_iter = root.FindMember("global_split_end_key");
                if (json_iter != root.MemberEnd() && root["global_split_end_key"].IsString()) {
                    global_split_end_key = json_iter->value.GetString();
                }
                json_iter = root.FindMember("split_region_num");
                if (json_iter != root.MemberEnd() && root["split_region_num"].IsNumber()) {
                    split_region_num = json_iter->value.GetInt64();
                }
            } catch (...) {
                // 兼容mysql语法
                table.set_comment(option->str_value.value);
                DB_WARNING("parse create table json comments error [%s]", option->str_value.value);
//                return -1;
            }
        } else if (option->type == parser::TABLE_OPT_PARTITION) {
            // range 分区这里进行配置。
            parser::PartitionOption* p_option = static_cast<parser::PartitionOption*>(option);
            auto partition_ptr = table.mutable_partition_info();

            if (p_option->expr == nullptr || p_option->expr->expr_type != parser::ET_COLUMN) {
                DB_WARNING("partition field only support column expr.");
                return -1;
            }

            if (p_option->type == parser::PARTITION_RANGE) {
                partition_ptr->set_type(pb::PT_RANGE);
                // 只能把field字段传给Meta，等schema建立好之后，创建表达式树。
                auto field_ptr = partition_ptr->mutable_field_info();
                field_ptr->set_field_name(static_cast<parser::ColumnName*>(p_option->expr)->name.value);

                for (int32_t index = 0; index < p_option->range.size(); ++index) {
                    auto range_ptr = partition_ptr->add_range_partition_values();
                    if (0 != create_expr_tree(p_option->range[index]->less_expr, *range_ptr, CreateExprOptions())) {
                        DB_WARNING("error pasing common expression");
                        return -1;
                    }
                    // 不支持多表达式，只支持字面量
                    if (range_ptr->nodes_size() != 1) {
                        DB_WARNING("only support literal");
                        return -1;
                    }
                    partition_ptr->add_partition_names(p_option->range[index]->name.c_str());
                }
                // 默认range最后为最大值MAXVALUE
                table.set_partition_num(p_option->range.size() + 1);
            } else if (p_option->type == parser::PARTITION_HASH) {
                partition_ptr->set_type(pb::PT_HASH);
                auto field_ptr = partition_ptr->mutable_field_info();
                field_ptr->set_field_name(static_cast<parser::ColumnName*>(p_option->expr)->name.value);
                table.set_partition_num(p_option->partition_num);
            }
        } else {

        }
    }
    //set default values if not specified by user
    if (!table.has_byte_size_per_record()) {
        DB_WARNING("no avg_row_length set in comments, use default:50");
        table.set_byte_size_per_record(50);
    }
    if (!table.has_namespace_name()) {
        DB_WARNING("no namespace set, use default: %s", 
            _ctx->user_info->namespace_.c_str());
        table.set_namespace_name(_ctx->user_info->namespace_);
    }
    if (split_region_num > 0) {
        int ret = parse_pre_split_keys(split_start_key, split_end_key,
                global_split_start_key, global_split_end_key, split_region_num, table);
        if (ret < 0) {
            return -1;
        }
    }
    table.set_if_exist(!stmt->if_not_exist);
    return 0;
}

int DDLPlanner::parse_drop_table(pb::SchemaInfo& table) {
    parser::DropTableStmt* stmt = (parser::DropTableStmt*)(_ctx->stmt);
    if (stmt->table_names.size() > 1) {
        DB_WARNING("drop multiple tables is not supported.");
        return -1;
    }
    parser::TableName* table_name = stmt->table_names[0];
    if (table_name == nullptr) {
        DB_WARNING("error: no table name specified");
        return -1;
    }
    if (table_name->db.empty()) {
        if (_ctx->cur_db.empty()) {
            _ctx->stat_info.error_code = ER_NO_DB_ERROR;
            _ctx->stat_info.error_msg << "No database selected";
            return -1;
        }
        table.set_database(_ctx->cur_db);
    } else {
        table.set_database(table_name->db.value);
    }
    table.set_table_name(table_name->table.value);
    table.set_namespace_name(_ctx->user_info->namespace_);
    table.set_if_exist(stmt->if_exist);
    DB_WARNING("drop table: %s.%s.%s", 
        table.namespace_name().c_str(), table.database().c_str(), table.table_name().c_str());
    return 0;
}

int DDLPlanner::parse_restore_table(pb::SchemaInfo& table) {
    parser::RestoreTableStmt* stmt = (parser::RestoreTableStmt*)(_ctx->stmt);
    if (stmt->table_names.size() > 1) {
        DB_WARNING("restore multiple tables is not supported.");
        return -1;
    }
    parser::TableName* table_name = stmt->table_names[0];
    if (table_name == nullptr) {
        DB_WARNING("error: no table name specified");
        return -1;
    }
    if (table_name->db.empty()) {
        if (_ctx->cur_db.empty()) {
            _ctx->stat_info.error_code = ER_NO_DB_ERROR;
            _ctx->stat_info.error_msg << "No database selected";
            return -1;
        }
        table.set_database(_ctx->cur_db);
    } else {
        table.set_database(table_name->db.value);
    }
    table.set_table_name(table_name->table.value);
    table.set_namespace_name(_ctx->user_info->namespace_);
    DB_WARNING("restore table: %s.%s.%s", 
        table.namespace_name().c_str(), table.database().c_str(), table.table_name().c_str());
    return 0;
}

int DDLPlanner::parse_create_database(pb::DataBaseInfo& database) {
    parser::CreateDatabaseStmt* stmt = (parser::CreateDatabaseStmt*)(_ctx->stmt);
    if (stmt->db_name.empty()) {
        _ctx->stat_info.error_code = ER_NO_DB_ERROR;
        _ctx->stat_info.error_msg << "No database selected";
        return -1;
    }
    database.set_database(stmt->db_name.value);
    database.set_namespace_name(_ctx->user_info->namespace_);
    return 0;
}

int DDLPlanner::parse_drop_database(pb::DataBaseInfo& database) {
    parser::DropDatabaseStmt* stmt = (parser::DropDatabaseStmt*)(_ctx->stmt);
    if (stmt->db_name.empty()) {
        _ctx->stat_info.error_code = ER_NO_DB_ERROR;
        _ctx->stat_info.error_msg << "No database selected";
        return -1;
    }
    database.set_database(stmt->db_name.value);
    database.set_namespace_name(_ctx->user_info->namespace_);
    return 0;
}


// return 0  : succ  
// return -1 : fail
// return -2 : virtual index op
int DDLPlanner::parse_alter_table(pb::MetaManagerRequest& alter_request) {
    parser::AlterTableStmt* stmt = (parser::AlterTableStmt*)(_ctx->stmt);
    if (stmt->table_name == nullptr) {
        DB_WARNING("error: no table name specified");
        return -1;
    }
    pb::SchemaInfo* table = alter_request.mutable_table_info();
    if (stmt->table_name->db.empty()) {
        if (_ctx->cur_db.empty()) {
            _ctx->stat_info.error_code = ER_NO_DB_ERROR;
            _ctx->stat_info.error_msg << "No database selected";
            return -1;
        }
        table->set_database(_ctx->cur_db);
    } else {
        table->set_database(stmt->table_name->db.value);
    }
    table->set_table_name(stmt->table_name->table.value);
    table->set_namespace_name(_ctx->user_info->namespace_);
    if (stmt->alter_specs.size() > 1 || stmt->alter_specs.size() == 0) {
        _ctx->stat_info.error_code = ER_ALTER_OPERATION_NOT_SUPPORTED;;
        _ctx->stat_info.error_msg << "Alter with multiple alter_specifications is not supported in this version";
        return -1;
    }
    parser::AlterTableSpec* spec = stmt->alter_specs[0];
    if (spec == nullptr) {
        _ctx->stat_info.error_code = ER_ALTER_OPERATION_NOT_SUPPORTED;;
        _ctx->stat_info.error_msg << "empty alter_specification";
        return -1;
    }
    if (spec->spec_type == parser::ALTER_SPEC_TABLE_OPTION) {
        if (spec->table_options.size() > 1) {
            _ctx->stat_info.error_code = ER_ALTER_OPERATION_NOT_SUPPORTED;;
            _ctx->stat_info.error_msg << "Alter with multiple table_options is not supported in this version";
            return -1;
        }
        parser::TableOption* table_option = spec->table_options[0];
        if (table_option->type == parser::TABLE_OPT_AVG_ROW_LENGTH) {
            alter_request.set_op_type(pb::OP_UPDATE_BYTE_SIZE);
            table->set_byte_size_per_record(table_option->uint_value);
        } else if (table_option->type == parser::TABLE_OPT_COMMENT) {
            rapidjson::Document root;
            try {
                root.Parse<0>(table_option->str_value.value);
                if (root.HasParseError()) {
                    // 兼容mysql语法
                    alter_request.set_op_type(pb::OP_UPDATE_TABLE_COMMENT);
                    table->set_comment(table_option->str_value.value);
                    rapidjson::ParseErrorCode code = root.GetParseError();
                    DB_WARNING("parse create table json comments error [code:%d][%s]",
                        code, table_option->str_value.value);
                    return 0;
                }
                auto json_iter = root.FindMember("comment");
                if (json_iter != root.MemberEnd()) {
                    alter_request.set_op_type(pb::OP_UPDATE_TABLE_COMMENT);
                    if (json_iter->value.IsString()) {
                        std::string comment = json_iter->value.GetString();
                        table->set_comment(comment);
                        DB_WARNING("comment: %s", comment.c_str());
                    }
                }
            } catch (...) {
                // 兼容mysql语法
                alter_request.set_op_type(pb::OP_UPDATE_TABLE_COMMENT);
                table->set_comment(table_option->str_value.value);
                DB_WARNING("parse alter table json comments error [%s]", table_option->str_value.value);
            }
        } else {
            _ctx->stat_info.error_code = ER_ALTER_OPERATION_NOT_SUPPORTED;;
            _ctx->stat_info.error_msg << "Alter table option type unsupported: " << table_option->type;
            return -1;
        }
    } else if (spec->spec_type == parser::ALTER_SPEC_ADD_COLUMN) {
        alter_request.set_op_type(pb::OP_ADD_FIELD);
        int column_len = spec->new_columns.size();
        for (int idx = 0; idx < column_len; ++idx) {
            parser::ColumnDef* column = spec->new_columns[idx];
            if (column == nullptr) {
                DB_WARNING("column is nullptr");
                return -1;
            }
            if (0 != add_column_def(*table, column)) {
                DB_WARNING("add column to table failed.");
                return -1;
            }
        }
        if (table->indexs_size() != 0) {
            _ctx->stat_info.error_code = ER_ALTER_OPERATION_NOT_SUPPORTED;;
            _ctx->stat_info.error_msg << "add table column with index is not supported in this version";
            return -1;
        }
    } else if (spec->spec_type == parser::ALTER_SPEC_DROP_COLUMN) {
        alter_request.set_op_type(pb::OP_DROP_FIELD);
        pb::FieldInfo* field = table->add_fields();
        if (spec->column_name.empty()) {
            _ctx->stat_info.error_code = ER_ALTER_OPERATION_NOT_SUPPORTED;;
            _ctx->stat_info.error_msg << "field_name is empty";
            return -1;
        } 
        field->set_field_name(spec->column_name.value);
    } else if (spec->spec_type == parser::ALTER_SPEC_MODIFY_COLUMN && spec->new_columns.size() > 0) {
        alter_request.set_op_type(pb::OP_MODIFY_FIELD);
        int column_len = spec->new_columns.size();
        if (column_len != 1) {
            _ctx->stat_info.error_code = ER_ALTER_OPERATION_NOT_SUPPORTED;;
            _ctx->stat_info.error_msg << "unsupported multi schema change";
            return -1;
        }
        for (int idx = 0; idx < column_len; ++idx) {
            parser::ColumnDef* column = spec->new_columns[idx];
            if (column == nullptr) {
                DB_WARNING("column is nullptr");
                return -1;
            }
            if (0 != add_column_def(*table, column)) {
                DB_WARNING("add column to table failed.");
                return -1;
            }
        }
        if (table->indexs_size() != 0) {
            _ctx->stat_info.error_code = ER_ALTER_OPERATION_NOT_SUPPORTED;;
            _ctx->stat_info.error_msg << "modify table column with index is not supported";
            return -1;
        }
    } else if (spec->spec_type == parser::ALTER_SPEC_RENAME_COLUMN) {
        alter_request.set_op_type(pb::OP_RENAME_FIELD);
        pb::FieldInfo* field = table->add_fields();
        if (spec->column_name.empty()) {
            _ctx->stat_info.error_code = ER_ALTER_OPERATION_NOT_SUPPORTED;;
            _ctx->stat_info.error_msg << "old field_name is empty";
            return -1;
        }
        field->set_field_name(spec->column_name.value);
        parser::ColumnDef* column = spec->new_columns[0];
        field->set_new_field_name(column->name->name.value);
    } else if (spec->spec_type == parser::ALTER_SPEC_RENAME_TABLE) {
        alter_request.set_op_type(pb::OP_RENAME_TABLE);
        std::string new_db_name;
        if (spec->new_table_name->db.empty()) {
            if (_ctx->cur_db.empty()) {
                _ctx->stat_info.error_code = ER_NO_DB_ERROR;
                _ctx->stat_info.error_msg << "No database selected";
                return -1;
            }
            new_db_name = _ctx->cur_db;
        } else {
            new_db_name = spec->new_table_name->db.c_str();
        }
        if (new_db_name != table->database()) {
            _ctx->stat_info.error_code = ER_ALTER_OPERATION_NOT_SUPPORTED;;
            _ctx->stat_info.error_msg << "cannot rename table to another database";
            return -1;
        }
        table->set_new_table_name(spec->new_table_name->table.value);
        DB_DEBUG("DDL_LOG schema_info[%s]", table->DebugString().c_str());
    } else if (spec->spec_type == parser::ALTER_SPEC_SWAP_TABLE) {
        alter_request.set_op_type(pb::OP_SWAP_TABLE);
        std::string new_db_name;
        if (spec->new_table_name->db.empty()) {
            if (_ctx->cur_db.empty()) {
                _ctx->stat_info.error_code = ER_NO_DB_ERROR;
                _ctx->stat_info.error_msg << "No database selected";
                return -1;
            }
            new_db_name = _ctx->cur_db;
        } else {
            new_db_name = spec->new_table_name->db.c_str();
        }
        if (new_db_name != table->database()) {
            _ctx->stat_info.error_code = ER_ALTER_OPERATION_NOT_SUPPORTED;;
            _ctx->stat_info.error_msg << "cannot swap table to another database";
            return -1;
        }
        table->set_new_table_name(spec->new_table_name->table.value);
        DB_DEBUG("DDL_LOG schema_info[%s]", table->DebugString().c_str());
    } else if (spec->spec_type == parser::ALTER_SPEC_ADD_INDEX) {
        alter_request.set_op_type(pb::OP_ADD_INDEX);
        int constraint_len = spec->new_constraints.size();
        std::string table_full_name = table->namespace_name() + "." + table->database() + "." + table->table_name();
        int64_t tableid = -1;
        if (0 != _factory->get_table_id(table_full_name, tableid)) {
            _ctx->stat_info.error_code = ER_NO_SUCH_TABLE;
            _ctx->stat_info.error_msg << "table: " << table->database() << "." << table->table_name() << " not exist";
            return -1;
        }
        auto tbl_ptr = _factory->get_table_info_ptr(tableid);
        if (tbl_ptr == nullptr) {
            DB_WARNING("no table found with id: %ld", tableid);
            _ctx->stat_info.error_code = ER_NO_SUCH_TABLE;
            _ctx->stat_info.error_msg << "table: " << table->database() << "." << table->table_name() << " not exist";
            return -1;
        }
        for (auto& field : tbl_ptr->fields) {
            if (field.can_null) {
                _column_can_null[field.short_name] = true;
            }
        }
        for (int idx = 0; idx < constraint_len; ++idx) {
            parser::Constraint* constraint = spec->new_constraints[idx];
            if (constraint == nullptr) {
                DB_WARNING("constraint is nullptr");
                return -1;
            }
            if (0 != add_constraint_def(*table, constraint,spec)) {
                DB_WARNING("add constraint to table failed.");
                return -1;
            }
        }
        DB_DEBUG("DDL_LOG schema_info[%s]", table->ShortDebugString().c_str());
    } else if (spec->spec_type == parser::ALTER_SPEC_DROP_INDEX) {
        DB_NOTICE("prepare to delete index");
        //drop index 转换成屏蔽
        if (spec->index_name.empty()) {
            DB_WARNING("index_name is null.");
            return -1;
        }
        pb::IndexInfo* index = table->add_indexs();
        index->set_index_name(spec->index_name.value);
        // 删除虚拟索引
        if (spec->is_virtual_index) {
            if (alter_request.mutable_table_info() != nullptr) {
                alter_request.mutable_table_info()->mutable_indexs(0)->set_hint_status(pb::IHS_VIRTUAL);
                alter_request.set_op_type(pb::OP_DROP_INDEX);
                return -2;
            }
        } else if (spec->force) {
            index->set_hint_status(pb::IHS_DISABLE);
            alter_request.set_op_type(pb::OP_DROP_INDEX);
        } else {
            //非虚拟索引走正常流程
            alter_request.set_op_type(pb::OP_SET_INDEX_HINT_STATUS);
            index->set_hint_status(pb::IHS_DISABLE);//设置为不可见
        }
        DB_NOTICE("drop index schema_info[%s]", table->ShortDebugString().c_str());
    } else if (spec->spec_type == parser::ALTER_SPEC_RESTORE_INDEX) {
        //restore index 解除屏蔽 
        alter_request.set_op_type(pb::OP_SET_INDEX_HINT_STATUS);
        if (spec->index_name.empty()) {
            DB_WARNING("index_name is null.");
            return -1;
        }
        pb::IndexInfo* index = table->add_indexs();
        index->set_index_name(spec->index_name.value);
        index->set_hint_status(pb::IHS_NORMAL);
        DB_NOTICE("restore index schema_info[%s]", table->ShortDebugString().c_str());
    }  else if (spec->spec_type == parser::ALTER_SPEC_ADD_LEARNER) {
        alter_request.set_op_type(pb::OP_ADD_LEARNER);
        alter_request.add_resource_tags(spec->resource_tag.value);
        DB_NOTICE("add learner schema_info[%s]", table->ShortDebugString().c_str());
    } else if (spec->spec_type == parser::ALTER_SPEC_DROP_LEARNER) { 
        alter_request.set_op_type(pb::OP_DROP_LEARNER);
        alter_request.add_resource_tags(spec->resource_tag.value);
        DB_NOTICE("drop learner schema_info[%s]", table->ShortDebugString().c_str());
    } else if (spec->spec_type == parser::ALTER_SPEC_MODIFY_COLUMN) {
        alter_request.set_op_type(pb::OP_MODIFY_FIELD);
        return parse_modify_column(alter_request, stmt->table_name, spec);
    } else {
        _ctx->stat_info.error_code = ER_ALTER_OPERATION_NOT_SUPPORTED;;
        _ctx->stat_info.error_msg << "alter_specification type (" 
                                << spec->spec_type << ") not supported in this version";
        return -1;
    }
    return 0;
}

int DDLPlanner::parse_modify_column(pb::MetaManagerRequest& alter_request,
                const parser::TableName* table_name,
                const parser::AlterTableSpec* alter_spec) {
    int ret = parse_db_tables(table_name);
    if (ret < 0) {
        return -1;
    }
    std::set<int> index_field_ids;
    auto iter = _plan_table_ctx->table_info.begin();
    if (iter == _plan_table_ctx->table_info.end()) {
        DB_WARNING("no table found");
        return -1;
    }
    auto table_ptr = iter->second;
    int64_t table_id = table_ptr->id;
    for (auto index_id : table_ptr->indices) {
        auto info_ptr = _factory->get_index_info_ptr(index_id); 
        if (info_ptr == nullptr) {
            continue;
        }
        for (auto field : info_ptr->fields) {
            index_field_ids.emplace(field.id);
            if (index_id == table_id) {
                get_scan_ref_slot(iter->first, table_ptr->id, field.id, field.type);
            }
        }
    }
    auto ddlwork_info = alter_request.mutable_ddlwork_info();
    ddlwork_info->set_opt_sql(_ctx->sql);
    ddlwork_info->set_table_id(table_id);
    ddlwork_info->set_op_type(pb::OP_MODIFY_FIELD);
    auto column_ddl_info = ddlwork_info->mutable_column_ddl_info();
    parser::Vector<parser::Assignment*> set_list = alter_spec->set_list;
    std::vector<ExprNode*> update_exprs;
    update_exprs.reserve(2);
    std::vector<pb::SlotDescriptor> update_slots;
    update_slots.reserve(2);
    for (int idx = 0; idx < set_list.size(); ++idx) {
        if (set_list[idx] == nullptr) {
            DB_WARNING("set item is nullptr");
            return -1;
        }
        std::string alias_name = get_field_alias_name(set_list[idx]->name);
        if (alias_name.empty()) {
            DB_WARNING("get_field_alias_name failed: %s", set_list[idx]->name->to_string().c_str());
            return -1;
        }
        std::string full_name = alias_name;
        full_name += ".";
        full_name += set_list[idx]->name->name.to_lower();
        FieldInfo* field_info = nullptr;
        if (nullptr == (field_info = get_field_info_ptr(full_name))) {
            DB_WARNING("invalid field name in");
            return -1;
        }
        if (index_field_ids.count(field_info->id) > 0) {
            _ctx->stat_info.error_code = ER_ALTER_OPERATION_NOT_SUPPORTED;;
            _ctx->stat_info.error_msg << "modify index column[" << full_name << "] is not supported in this version";
            return -1;
        }
        auto slot = get_scan_ref_slot(alias_name, field_info->table_id, field_info->id, field_info->type);
        update_slots.emplace_back(slot);
        pb::Expr value_expr;
        if (0 != create_expr_tree(set_list[idx]->expr, value_expr, CreateExprOptions())) {
            DB_WARNING("create update value expr failed");
            return -1;
        }
        DB_WARNING("value_expr:%s", value_expr.ShortDebugString().c_str());
        ExprNode* value_node = nullptr;
        ret = ExprNode::create_tree(value_expr, &value_node);
        if (ret < 0) {
            //如何释放资源
            return ret;
        }
        update_exprs.emplace_back(value_node);
    }
    for (auto& expr : update_exprs) {
        //类型推导
        ret = expr->expr_optimize();
    }
    for (uint32_t idx = 0; idx < update_slots.size(); ++idx) {
        ExprNode::create_pb_expr(column_ddl_info->add_update_exprs(), update_exprs[idx]);
        column_ddl_info->add_update_slots()->CopyFrom(update_slots[idx]);
    }

    if (alter_spec->where != nullptr) {
        std::vector<pb::Expr>  where_filters;
        if (0 != flatten_filter(alter_spec->where, where_filters, CreateExprOptions())) {
            DB_WARNING("flatten_filter failed");
            return -1;
        }
        std::vector<ExprNode*> conjuncts;
        conjuncts.reserve(2);
        for (auto& expr : where_filters) {
            DB_WARNING("where_expr:%s", expr.ShortDebugString().c_str());
            ExprNode* conjunct = nullptr;
            ret = ExprNode::create_tree(expr, &conjunct);
            if (ret < 0) {
                //如何释放资源
                return ret;
            }
            conjunct->expr_optimize();
            conjuncts.emplace_back(conjunct);
        }
        for (auto expr : conjuncts) {
            ExprNode::create_pb_expr(column_ddl_info->add_scan_conjuncts(), expr);
        }
    }
    create_scan_tuple_descs();
    for (auto& tuple : _ctx->tuple_descs()) {
        column_ddl_info->add_tuples()->CopyFrom(tuple);
    }
    DB_WARNING("alter_request:%s", alter_request.DebugString().c_str());
    return 0;
}

pb::PrimitiveType DDLPlanner::to_baikal_type(parser::FieldType* field_type) {
    switch (field_type->type) {
    case parser::MYSQL_TYPE_TINY: {
        if (field_type->flag & parser::MYSQL_FIELD_FLAG_UNSIGNED) {
            return pb::UINT8;
        } else {
            return pb::INT8;
        }
    } break;
    case parser::MYSQL_TYPE_SHORT: {
        if (field_type->flag & parser::MYSQL_FIELD_FLAG_UNSIGNED) {
            return pb::UINT16;
        } else {
            return pb::INT16;
        }
    } break;
    case parser::MYSQL_TYPE_INT24:
    case parser::MYSQL_TYPE_LONG: {
        if (field_type->flag & parser::MYSQL_FIELD_FLAG_UNSIGNED) {
            return pb::UINT32;
        } else {
            return pb::INT32;
        }
    } break;
    case parser::MYSQL_TYPE_LONGLONG: {
        if (field_type->flag & parser::MYSQL_FIELD_FLAG_UNSIGNED) {
            return pb::UINT64;
        } else {
            return pb::INT64;
        }
    } break;
    case parser::MYSQL_TYPE_FLOAT: {
        return pb::FLOAT;
    } break;
    case parser::MYSQL_TYPE_DECIMAL:
    case parser::MYSQL_TYPE_NEWDECIMAL:
    case parser::MYSQL_TYPE_DOUBLE: {
        return pb::DOUBLE;
    } break;
    case parser::MYSQL_TYPE_STRING:
    case parser::MYSQL_TYPE_VARCHAR:
    case parser::MYSQL_TYPE_TINY_BLOB:
    case parser::MYSQL_TYPE_BLOB:
    case parser::MYSQL_TYPE_MEDIUM_BLOB:
    case parser::MYSQL_TYPE_LONG_BLOB: {
        return pb::STRING;
    } break;
    case parser::MYSQL_TYPE_DATE: {
        return pb::DATE;
    } break;
    case parser::MYSQL_TYPE_DATETIME: {
        return pb::DATETIME;
    } break;
    case parser::MYSQL_TYPE_TIME: {
        return pb::TIME;
    } break;
    case parser::MYSQL_TYPE_TIMESTAMP: {
        return pb::TIMESTAMP;
    } break;
    case parser::MYSQL_TYPE_HLL: {
        return pb::HLL;
    } break;
    case parser::MYSQL_TYPE_BITMAP: {
        return pb::BITMAP;
    } break;
    case parser::MYSQL_TYPE_TDIGEST: {
        return pb::TDIGEST;
    } break;
    default : {
        DB_WARNING("unsupported item type: %d", field_type->type);
        return pb::INVALID_TYPE;
    }
    }
    return pb::INVALID_TYPE;
}

int DDLPlanner::add_constraint_def(pb::SchemaInfo& table, parser::Constraint* constraint,parser::AlterTableSpec* spec) {
    pb::IndexInfo* index = table.add_indexs();
    pb::IndexType index_type;
    switch (constraint->type) {
        case parser::CONSTRAINT_INDEX:
            index_type = pb::I_KEY;
            if (constraint->index_dist == parser::INDEX_DIST_DEFAULT) {
                index->set_is_global(FLAGS_normal_index_default_global);
            }
            break;
        case parser::CONSTRAINT_UNIQ:
            index_type = pb::I_UNIQ;
            if (constraint->index_dist == parser::INDEX_DIST_DEFAULT) {
                index->set_is_global(FLAGS_unique_index_default_global);
            }
            break;
        case parser::CONSTRAINT_FULLTEXT:
            index_type = pb::I_FULLTEXT;
            if (constraint->columns.size() > 1) {
                DB_WARNING("fulltext index only support one field.");
                return -1;
            }
            break;
        default:
            DB_WARNING("only support uniqe、key index type");
            return -1;
    }

    if (constraint->name.empty()) {
        DB_WARNING("lack of index name");
        return -1;
    }
    if (constraint->index_dist == parser::INDEX_DIST_GLOBAL) {
        index->set_is_global(true);
    }
    index->set_index_type(index_type);
    index->set_hint_status(pb::IHS_DISABLE);
    index->set_index_name(constraint->name.value);
//虚拟索引标志位
    if (spec->is_virtual_index) {
        if (table.mutable_indexs(0) != nullptr) {
            table.mutable_indexs(0)->set_hint_status(pb::IHS_VIRTUAL);
        }
    }

    for (int32_t column_index = 0; column_index < constraint->columns.size(); ++column_index) {
        std::string column_name = constraint->columns[column_index]->name.value;
        if (_column_can_null[column_name] && index->index_type() != pb::I_FULLTEXT) {
            DB_WARNING("index column : %s should NOT NULL", column_name.c_str());
            _ctx->stat_info.error_code = ER_NO_DB_ERROR;
            _ctx->stat_info.error_msg << "index column : " << column_name << " should NOT NULL";
            return -1;
        }
        index->add_field_names(column_name);
    }
    if (constraint->index_option != nullptr) {
        rapidjson::Document root;
        const char* value = constraint->index_option->comment.c_str();
        try {
            root.Parse<0>(value);
            if (root.HasParseError()) {
                rapidjson::ParseErrorCode code = root.GetParseError();
                DB_WARNING("parse create table json comments error [code:%d][%s]", 
                    code, value);
                return -1;
            }
            auto json_iter = root.FindMember("segment_type");
            if (json_iter != root.MemberEnd()) {
                std::string segment_type = json_iter->value.GetString();
                pb::SegmentType pb_segment_type = pb::S_DEFAULT;
                SegmentType_Parse(segment_type, &pb_segment_type);
                index->set_segment_type(pb_segment_type);
            }
            
            auto storage_type_iter = root.FindMember("storage_type");
            pb::StorageType pb_storage_type = pb::ST_ARROW;
            if (storage_type_iter != root.MemberEnd()) {
                std::string storage_type = storage_type_iter->value.GetString();
                StorageType_Parse(storage_type, &pb_storage_type);
            }
            index->set_storage_type(pb_storage_type);
        } catch (...) {
            DB_WARNING("parse create table json comments error [%s]", value);
            return -1;
        }
    }
    return 0;
}

bool DDLPlanner::is_fulltext_type_constraint(pb::StorageType pb_storage_type, bool& has_arrow_fulltext, bool& has_pb_fulltext) const {
    if (pb_storage_type == pb::ST_PROTOBUF_OR_FORMAT1) {
        has_pb_fulltext = true;
        if (has_arrow_fulltext) {
            DB_WARNING("fulltext has two types : pb&arrow"); 
            return false;
        }
        return true;
    } else if (pb_storage_type == pb::ST_ARROW) {
        has_arrow_fulltext = true;
        if (has_pb_fulltext) {
            DB_WARNING("fulltext has two types : pb&arrow"); 
            return false;
        }
        return true;
    }
    DB_WARNING("unknown storage_type");
    return false;
}

} // end of namespace baikaldb
