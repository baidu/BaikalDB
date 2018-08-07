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
    if (_ctx->stmt_type == parser::NT_CREATE_TABLE) {
        request.set_op_type(pb::OP_CREATE_TABLE);
        pb::SchemaInfo *table = request.mutable_table_info();
        if (0 != parse_create_table(*table)) {
            DB_WARNING("parser create table command failed");
            return -1;
        }
    } else if (_ctx->stmt_type == parser::NT_DROP_TABLE) {
        request.set_op_type(pb::OP_DROP_TABLE);
        pb::SchemaInfo *table = request.mutable_table_info();
        if (0 != parse_drop_table(*table)) {
            DB_WARNING("parser drop table command failed");
            return -1;
        }
    } else if (_ctx->stmt_type == parser::NT_CREATE_DATABASE) {
        request.set_op_type(pb::OP_CREATE_DATABASE);
        pb::DataBaseInfo *database = request.mutable_database_info();
        if (0 != parse_create_database(*database)) {
            DB_WARNING("parser create database command failed");
            return -1;
        }
    } else if (_ctx->stmt_type == parser::NT_DROP_DATABASE) {
        request.set_op_type(pb::OP_DROP_DATABASE);
        pb::DataBaseInfo *database = request.mutable_database_info();
        if (0 != parse_drop_database(*database)) {
            DB_WARNING("parser drop database command failed");
            return -1;
        }
    } else {
        DB_WARNING("unsupported DDL command: %d", _ctx->stmt_type);
        return -1;
    }
    pb::MetaManagerResponse response;
    if (MetaServerInteract::get_instance()->send_request("meta_manager", request, response) != 0) {
        DB_WARNING("send_request fail");
        return -1;
    }
    if (response.errcode() != pb::SUCCESS) {
        DB_WARNING("err:%s", response.errmsg().c_str());
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
            DB_WARNING("No database selected for table: %s", stmt->table_name->table.value);
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
        pb::FieldInfo* field = table.add_fields();
        if (column->name != nullptr && !column->name->name.empty()) {
            field->set_field_name(column->name->name.value);
        }
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
                std::string default_value = col_option->expr->to_string();
                field->set_default_value(default_value);
            } else if (col_option->type == parser::COLUMN_OPT_COMMENT) {
                std::string comment = col_option->expr->to_string();
                field->set_comment(comment);
            } else {
                DB_WARNING("unsupported column option type: %d", col_option->type);
                return -1;
            }
        }
        // can_null default is false
        if (!field->has_can_null()) {
            field->set_can_null(false);
        }
    }

    int constraint_len = stmt->constraints.size();
    for (int idx = 0; idx < constraint_len; ++idx) {
        parser::Constraint* constraint = stmt->constraints[idx];
        pb::IndexInfo* index = table.add_indexs();
        if (constraint->type == parser::CONSTRAINT_PRIMARY) {
            index->set_index_type(pb::I_PRIMARY);
        } else if (constraint->type == parser::CONSTRAINT_INDEX) {
            index->set_index_type(pb::I_KEY);
        } else if (constraint->type == parser::CONSTRAINT_UNIQ) {
            index->set_index_type(pb::I_UNIQ);
        } else if (constraint->type == parser::CONSTRAINT_FULLTEXT) {
            index->set_index_type(pb::I_FULLTEXT);
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
            if (!boost::algorithm::iequals(str_val, "rocksdb")) {
                DB_WARNING("unsupported storage engine:%s, use rocksdb", str_val.c_str());
                return -1;
            }
        } else if (option->type == parser::TABLE_OPT_CHARSET) {
            std::string str_val(option->str_value.value);
            if (!boost::algorithm::iequals(str_val, "gbk")) {
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
            } catch (...) {
                DB_WARNING("parse create table json comments error [%s]", option->str_value.value);
                return -1;
            }
        } else {

        }
    }
    // user must privide a namespace
    if (!table.has_namespace_name()) {
        DB_WARNING("no namespace set in comments");
        return -1;
    }
    //set default values if not specified by user
    if (!table.has_byte_size_per_record()) {
        DB_WARNING("no avg_row_length set in comments, use default:1");
        table.set_byte_size_per_record(1);
    }
    if (!table.has_namespace_name()) {
        DB_WARNING("no namespace set in comments, use default: %s", 
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
            DB_WARNING("No database selected for table: %s", table_name->table.value);
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

int DDLPlanner::parse_create_database(pb::DataBaseInfo& database) {
    parser::CreateDatabaseStmt* stmt = (parser::CreateDatabaseStmt*)(_ctx->stmt);
    if (stmt->db_name.empty()) {
        DB_WARNING("error: no db_name specified");
        return -1;
    }
    database.set_database(stmt->db_name.value);
    database.set_namespace_name(_ctx->user_info->namespace_);
    return 0;
}

int DDLPlanner::parse_drop_database(pb::DataBaseInfo& database) {
    parser::DropDatabaseStmt* stmt = (parser::DropDatabaseStmt*)(_ctx->stmt);
    if (stmt->db_name.empty()) {
        DB_WARNING("error: no db_name specified");
        return -1;
    }
    database.set_database(stmt->db_name.value);
    database.set_namespace_name(_ctx->user_info->namespace_);
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
    case parser::MYSQL_TYPE_TIMESTAMP: {
        return pb::TIMESTAMP;
    } break;
    default : {
        DB_WARNING("unsupported item type: %d", field_type->type);
        return pb::INVALID_TYPE;
    }
    }
    return pb::INVALID_TYPE;
}
} // end of namespace baikaldb
