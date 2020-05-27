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
#include "meta_server_interact.hpp"
#include "proto/meta.interface.pb.h"
#include <boost/algorithm/string/predicate.hpp>
#include <rapidjson/reader.h>
#include <rapidjson/document.h>

namespace baikaldb {
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
        if (0 != parse_alter_table(request)) {
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
        } else if (col_option->type == parser::COLUMN_OPT_DEFAULT_VAL) {
            field->set_default_value(col_option->expr->to_string());
        } else if (col_option->type == parser::COLUMN_OPT_COMMENT) {
            field->set_comment(col_option->expr->to_string());
        } else if (col_option->type == parser::COLUMN_OPT_ON_UPDATE) {
            field->set_on_update_value(col_option->expr->to_string());
        } else {
            DB_WARNING("unsupported column option type: %d", col_option->type);
            return -1;
        }
    }
    // can_null default is false
    if (!field->has_can_null()) {
        field->set_can_null(false);
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
    for (int idx = 0; idx < constraint_len; ++idx) {
        parser::Constraint* constraint = stmt->constraints[idx];
        pb::IndexInfo* index = table.add_indexs();
        if (constraint->global == true) {
            index->set_is_global(true);
            can_support_ttl = false;
        }
        if (constraint->type == parser::CONSTRAINT_PRIMARY) {
            index->set_index_type(pb::I_PRIMARY);
        } else if (constraint->type == parser::CONSTRAINT_INDEX) {
            index->set_index_type(pb::I_KEY);
        } else if (constraint->type == parser::CONSTRAINT_UNIQ) {
            index->set_index_type(pb::I_UNIQ);
        } else if (constraint->type == parser::CONSTRAINT_FULLTEXT) {
            index->set_index_type(pb::I_FULLTEXT);
            can_support_ttl = false;
        } else {
            DB_WARNING("unsupported constraint_type: %d", constraint->type);
            return -1;
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
                pb::StorageType pb_storage_type = pb::ST_PROTOBUF;
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
                    rapidjson::ParseErrorCode code = root.GetParseError();
                    DB_WARNING("parse create table json comments error [code:%d][%s]", 
                        code, option->str_value.value);
                    return -1;
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
                if (json_iter != root.MemberEnd()) {
                    for (auto i = 0; i < json_iter->value.Size(); i++) {
                        const rapidjson::Value& dist_value = json_iter->value[i];
                        auto* dist = table.add_dists();
                        dist->set_logical_room(dist_value["logical_room"].GetString());
                        dist->set_count(dist_value["count"].GetInt());
                    }
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
                        DB_FATAL("global index/fulltext/engine!=rocksdb can not create ttl table");
                        return -1;
                    }
                    int64_t ttl_duration = json_iter->value.GetInt64();
                    table.set_ttl_duration(ttl_duration);
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
            } catch (...) {
                DB_WARNING("parse create table json comments error [%s]", option->str_value.value);
                return -1;
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
        alter_request.set_op_type(pb::OP_UPDATE_BYTE_SIZE);
        if (spec->table_options.size() > 1) {
            _ctx->stat_info.error_code = ER_ALTER_OPERATION_NOT_SUPPORTED;;
            _ctx->stat_info.error_msg << "Alter with multiple table_options is not supported in this version";
            return -1;
        }
        parser::TableOption* table_option = spec->table_options[0];
        if (table_option->type == parser::TABLE_OPT_AVG_ROW_LENGTH) {
            table->set_byte_size_per_record(table_option->uint_value);
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

    } else if (spec->spec_type == parser::ALTER_SPEC_ADD_INDEX) {
        alter_request.set_op_type(pb::OP_ADD_INDEX);
        int constraint_len = spec->new_constraints.size();

        for (int idx = 0; idx < constraint_len; ++idx) {
            parser::Constraint* constraint = spec->new_constraints[idx];
            if (constraint == nullptr) {
                DB_WARNING("constraint is nullptr");
                return -1;
            }
            if (0 != add_constraint_def(*table, constraint)) {
                DB_WARNING("add constraint to table failed.");
                return -1;
            }
        }
        DB_DEBUG("DDL_LOG schema_info[%s]", table->ShortDebugString().c_str());

    } else if (spec->spec_type == parser::ALTER_SPEC_DROP_INDEX) {
        alter_request.set_op_type(pb::OP_DROP_INDEX);
        if (spec->index_name.empty()) {
            DB_WARNING("index_name is null.");
            return -1;
        }
        pb::IndexInfo* index = table->add_indexs();
        index->set_index_name(spec->index_name.value);
        DB_DEBUG("DDL_LOG schema_info[%s]", table->ShortDebugString().c_str());
    } else {
        _ctx->stat_info.error_code = ER_ALTER_OPERATION_NOT_SUPPORTED;;
        _ctx->stat_info.error_msg << "alter_specification type (" 
                                << spec->spec_type << ") not supported in this version";
        return -1;
    }
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
    default : {
        DB_WARNING("unsupported item type: %d", field_type->type);
        return pb::INVALID_TYPE;
    }
    }
    return pb::INVALID_TYPE;
}

int DDLPlanner::add_constraint_def(pb::SchemaInfo& table, parser::Constraint* constraint) {
    pb::IndexInfo* index = table.add_indexs();
    pb::IndexType index_type;
    switch (constraint->type) {
        case parser::CONSTRAINT_INDEX:
            index_type = pb::I_KEY;
            break;
        case parser::CONSTRAINT_UNIQ:
            index_type = pb::I_UNIQ;
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
    index->set_index_type(index_type);
    index->set_index_name(constraint->name.value);

    for (int32_t column_index = 0; column_index < constraint->columns.size(); ++column_index) {
        index->add_field_names(constraint->columns[column_index]->name.value);
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
            pb::StorageType pb_storage_type = pb::ST_PROTOBUF;
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
    if (pb_storage_type == pb::ST_PROTOBUF) {
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
