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

#include "show_helper.h"
#include "network_server.h"
#include "store_interact.hpp"
#include "query_context.h"
#include "re2/re2.h"

namespace baikaldb {
void ShowHelper::init() {
    _calls[SQL_SHOW_ABNORMAL_REGIONS] = std::bind(&ShowHelper::_show_abnormal_regions,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_SHOW_DATABASES] = std::bind(&ShowHelper::_show_databases,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_SHOW_TABLES] = std::bind(&ShowHelper::_show_tables,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_SHOW_CREATE_TABLE] = std::bind(&ShowHelper::_show_create_table,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_SHOW_COLLATION] = std::bind(&ShowHelper::_show_collation,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_SHOW_SOCKET] = std::bind(&ShowHelper::_show_socket,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_SHOW_WARNINGS] = std::bind(&ShowHelper::_show_warnings,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_SHOW_PROCESSLIST] = std::bind(&ShowHelper::_show_processlist,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_SHOW_COST] = std::bind(&ShowHelper::_show_cost,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_SHOW_FULL_COLUMNS] = std::bind(&ShowHelper::_show_full_columns,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_SHOW_FULL_TABLES] = std::bind(&ShowHelper::_show_full_tables,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_SHOW_SCHEMA_CONF] = std::bind(&ShowHelper::_show_schema_conf,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_SHOW_TABLE_STATUS] = std::bind(&ShowHelper::_show_table_status,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_SHOW_REGION] = std::bind(&ShowHelper::_show_region,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_SHOW_STORE_REGION] = std::bind(&ShowHelper::_show_store_region,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_SHOW_VARIABLES] = std::bind(&ShowHelper::_show_variables,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_SHOW_VIRTUAL_INDEX] = std::bind(&ShowHelper::_show_virtual_index,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_SHOW_NAMESPACE] = std::bind(&ShowHelper::_handle_client_query_template_dispatch,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_SHOW_META] = std::bind(&ShowHelper::_handle_client_query_template_dispatch,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_DISABLE_INDEXS] = std::bind(&ShowHelper::_handle_client_query_template_dispatch,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_SHOW_PRIVILEGE] = std::bind(&ShowHelper::_show_privilege,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_SHOW_USER] = std::bind(&ShowHelper::_show_user,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_SHOW_STORE_TXN] = std::bind(&ShowHelper::_show_store_txn,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_SHOW_DDL_WORK] = std::bind(&ShowHelper::_show_ddl_work,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_SHOW_GLOBAL_DDL_WORK] = std::bind(&ShowHelper::_show_global_ddl_work,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_SHOW_DIFF_REGION_SIZE] = std::bind(&ShowHelper::_show_diff_region_size,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_SHOW_NETWORK_SEGMENT] = std::bind(&ShowHelper::_show_network_segment,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_SHOW_SWITCH] = std::bind(&ShowHelper::_show_switch,
            this, std::placeholders::_1, std::placeholders::_2);
    _calls[SQL_SHOW_ALL_TABLES] = std::bind(&ShowHelper::_show_all_tables,
            this, std::placeholders::_1, std::placeholders::_2);
    _wrapper = MysqlWrapper::get_instance();
}

bool ShowHelper::execute(const SmartSocket& client) {
    if (client == nullptr || client->query_ctx == nullptr) {
        DB_FATAL("param invalid");
        //client->state = STATE_ERROR;
        return false;
    }

    std::vector<std::string> split_vec;
    boost::split(split_vec, client->query_ctx->sql,
                 boost::is_any_of(" \t\n\r"), boost::token_compress_on);
    if (split_vec.size() < 2) {
        _wrapper->make_simple_ok_packet(client);
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }
    std::transform(split_vec[1].begin(), split_vec[1].end(), split_vec[1].begin(), ::tolower);
    std::string& key = split_vec[1];
    if (key == "full") {
        if(split_vec.size() > 2 && boost::iequals(split_vec[2], "tables")) {
            key = SQL_SHOW_FULL_TABLES;
        } else if (split_vec.size() > 2 && boost::iequals(split_vec[2], "columns")) {
            key = SQL_SHOW_FULL_COLUMNS;
        } else {
            _wrapper->make_simple_ok_packet(client);
            client->state = STATE_READ_QUERY_RESULT;
            return true;
        }
    }
    auto iter = _calls.find(key);
    if (iter == _calls.end() || iter->second == nullptr) {
        _wrapper->make_simple_ok_packet(client);
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }
    return iter->second(client, split_vec);
}

bool ShowHelper::_show_abnormal_regions(const SmartSocket& client, const std::vector<std::string>& split_vec) {
    if (client == nullptr || client->query_ctx == nullptr) {
        DB_FATAL("param invalid");
        //client->state = STATE_ERROR;
        return false;
    }

    std::string resource_tag;
    bool unhealthy = false;;
    if (split_vec.size() == 5) {
        if (split_vec[4] == "unhealthy") {
            unhealthy = true;
        }
    } else if (split_vec.size() == 4) {
        resource_tag = split_vec[3];
    } else if (split_vec.size() != 3) {
        client->state = STATE_ERROR;
        return false;
    }

    pb::QueryRequest req;
    req.set_op_type(pb::QUERY_REGION_PEER_STATUS);
    if (resource_tag != "") {
        req.set_resource_tag(resource_tag);
    }
    pb::QueryResponse res;
    MetaServerInteract::get_instance()->send_request("query", req, res);
    std::vector< std::vector<std::string> > rows;
    rows.reserve(10);
    size_t max_size = 0;
    for (auto& region_info : res.region_status_infos()) {
        if (unhealthy && region_info.is_healthy()) {
            continue;
        }
        std::vector<std::string> row;
        row.reserve(5);
        row.emplace_back(region_info.table_name());
        row.emplace_back(std::to_string(region_info.table_id()));
        row.emplace_back(std::to_string(region_info.region_id()));
        if (region_info.is_healthy()) {
            row.emplace_back("healthy");
        } else {
            row.emplace_back("unhealthy");
        }
        for (auto& peer_info : region_info.peer_status_infos()) {
            row.emplace_back(peer_info.peer_id() + "@" + pb::PeerStatus_Name(peer_info.peer_status()));
        }
        if (max_size < row.size()) {
            max_size = row.size();
        }
        rows.emplace_back(row);
    }

    for (auto& row : rows) {
        if (row.size() < max_size) {
            for (size_t i = 0; i < max_size - row.size(); i++) {
                row.emplace_back("NULL");
            }
        }
    }

    std::vector<std::string> names = { "table_name", "table_id", "region_id", "region_status" };
    for (size_t i = 1; max_size > 4 && i <= max_size - 4; i++) {
        names.emplace_back("peer" + std::to_string(i));
    }

    std::vector<ResultField> fields;
    fields.reserve(4);
    for (auto& name : names) {
        ResultField field;
        field.name = name;
        field.type = MYSQL_TYPE_STRING;
        fields.emplace_back(field);
    }
    // Make mysql packet.
    if (_make_common_resultset_packet(client, fields, rows) != 0) {
        DB_FATAL_CLIENT(client, "Failed to make result packet.");
        _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
        client->state = STATE_ERROR;
        return false;
    }
    client->state = STATE_READ_QUERY_RESULT;
    return true;
}

bool ShowHelper::_show_collation(const SmartSocket& client, const std::vector<std::string>& split_vec) {
    if (client == nullptr) {
        DB_FATAL("param invalid");
        //client->state = STATE_ERROR;
        return false;
    }

    // Make result info.
    std::vector<ResultField> fields;
    fields.reserve(6);
    do {
        ResultField field;
        field.name = "Collation";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Charset";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Id";
        field.type = MYSQL_TYPE_LONGLONG;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Default";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Compiled";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Sortlen";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);

    // Make rows.
    std::vector< std::vector<std::string> > rows;
    std::vector<std::string> row;
    row.emplace_back("gbk_chinese_ci");
    row.emplace_back("gbk");
    row.emplace_back("28");
    row.emplace_back("Yes");
    row.emplace_back("Yes");
    row.emplace_back("1");
    rows.emplace_back(row);
    std::vector<std::string> row1;
    row1.emplace_back("gbk_bin");
    row1.emplace_back("gbk");
    row1.emplace_back("87");
    row1.emplace_back("   ");
    row1.emplace_back("Yes");
    row1.emplace_back("1");
    rows.emplace_back(row1);
    rows.push_back({"utf8_general_ci", "utf8", "33", "Yes", "Yes", "1"});
    rows.push_back({"utf8_bin", "utf8", "83", " ", "Yes", "1"});
    rows.push_back({"binary", "binary", "63", " ", "Yes", "1"});

    // Make mysql packet.
    if (_make_common_resultset_packet(client, fields, rows) != 0) {
        DB_FATAL_CLIENT(client, "Failed to package mysql common result.");
        _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
        client->state = STATE_ERROR;
        return false;
    }
    client->state = STATE_READ_QUERY_RESULT;
    return true;
}

bool ShowHelper::_show_databases(const SmartSocket& client, const std::vector<std::string>& split_vec) {
    if (client == nullptr || !client->user_info) {
        DB_FATAL("param invalid");
        //client->state = STATE_ERROR;
        return false;
    }

    // Make fields.
    std::vector<ResultField> fields;
    fields.reserve(1);
    do {
        ResultField field;
        field.name = "Database";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);

    // Make rows.
    std::vector< std::vector<std::string> > rows;
    rows.reserve(10);
    SchemaFactory* factory = SchemaFactory::get_instance();
    if (!factory) {
        DB_FATAL("param invalid");
        return false;
    }
    std::vector<std::string> dbs =  factory->get_db_list(client->user_info->all_database);
    for (uint32_t cnt = 0; cnt < dbs.size(); ++cnt) {
        std::vector<std::string> row;
        row.emplace_back(dbs[cnt]);
        rows.emplace_back(row);
    }

    // Make mysql packet.
    if (_make_common_resultset_packet(client, fields, rows) != 0) {
        DB_FATAL_CLIENT(client, "Failed to make result packet.");
        _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
        client->state = STATE_ERROR;
        return false;
    }
    client->state = STATE_READ_QUERY_RESULT;
    return true;
}

bool ShowHelper::_show_tables(const SmartSocket& client, const std::vector<std::string>& split_vec) {
    SchemaFactory* factory = SchemaFactory::get_instance();
    if (client == nullptr || client->query_ctx == nullptr || client->user_info == nullptr || factory == nullptr) {
        DB_FATAL("param invalid");
        //client->state = STATE_ERROR;
        return false;
    }

    std::string namespace_ = client->user_info->namespace_;
    std::string db = client->current_db;
    if (split_vec.size() == 4) {
        db = split_vec[3];
    }
    if (db == "") {
        DB_WARNING("no database selected");
        _wrapper->make_err_packet(client, ER_NO_DB_ERROR, "No database selected");
        client->state = STATE_READ_QUERY_RESULT;
        return false;
    }
    if (db == "information_schema") {
        namespace_ = "INTERNAL";
    }

    // Make fields.
    std::vector<ResultField> fields;
    fields.reserve(1);
    do {
        ResultField field;
        field.name = "Tables_in_" + db;
        field.db = db;
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);

    // Make rows.
    std::vector< std::vector<std::string> > rows;
    rows.reserve(10);
    std::vector<std::string> tables =  factory->get_table_list(
            namespace_, db, client->user_info.get());
    for (uint32_t cnt = 0; cnt < tables.size(); ++cnt) {
        //DB_NOTICE("table:%s", tables[cnt].c_str());
        std::vector<std::string> row;
        row.emplace_back(tables[cnt]);
        rows.emplace_back(row);
    }

    // Make mysql packet.
    if (_make_common_resultset_packet(client, fields, rows) != 0) {
        DB_FATAL_CLIENT(client, "Failed to make result packet.");
        _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "%s", client->query_ctx->sql.c_str());
        client->state = STATE_ERROR;
        return false;
    }
    client->state = STATE_READ_QUERY_RESULT;
    return true;
}

bool ShowHelper::_show_create_table(const SmartSocket& client, const std::vector<std::string>& split_vec) {
    SchemaFactory* factory = SchemaFactory::get_instance();
    if (client == nullptr || client->query_ctx == nullptr || client->user_info == nullptr || factory == nullptr) {
        DB_FATAL("param invalid");
        //client->state = STATE_ERROR;
        return false;
    }
    static std::map<pb::PrimitiveType, std::string> type_map = {
            {pb::BOOL, "boolean"},
            {pb::INT8, "tinyint(4)"},
            {pb::UINT8, "tinyint(4) unsigned"},
            {pb::INT16, "smallint(6)"},
            {pb::UINT16, "smallint(6) unsigned"},
            {pb::INT32, "int(10)"},
            {pb::UINT32, "int(10) unsigned"},
            {pb::INT64, "bigint(20)"},
            {pb::UINT64, "bigint(20) unsigned"},
            {pb::FLOAT, "float"},
            {pb::DOUBLE, "double"},
            {pb::STRING, "varchar(1024)"},
            {pb::DATETIME, "DATETIME"},
            {pb::TIME, "TIME"},
            {pb::TIMESTAMP, "TIMESTAMP"},
            {pb::DATE, "DATE"},
            {pb::HLL, "HLL"},
            {pb::BITMAP, "BITMAP"},
            {pb::TDIGEST, "TDIGEST"},
    };
    static std::map<pb::IndexType, std::string> index_map = {
            {pb::I_PRIMARY, "PRIMARY KEY"},
            {pb::I_UNIQ, "UNIQUE KEY"},
            {pb::I_KEY, "KEY"},
            {pb::I_FULLTEXT, "FULLTEXT KEY"},
    };
    static std::map<pb::Charset, std::string> charset_map = {
            {pb::UTF8, "utf8"},
            {pb::GBK, "gbk"},
    };
    // Make fields.
    std::vector<ResultField> fields;
    fields.reserve(2);
    do {
        ResultField field;
        field.name = "Table";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Create Table";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 10240;
        fields.emplace_back(field);
    } while (0);

    std::string db = client->current_db;
    std::string table;
    if (split_vec.size() == 4) {
        table = remove_quote(split_vec[3].c_str(), '`');
    } else if (split_vec.size() == 5) {
        db = remove_quote(split_vec[3].c_str(), '`');
        table = remove_quote(split_vec[4].c_str(), '`');
    } else {
        client->state = STATE_ERROR;
        return false;
    }

    std::string namespace_ = client->user_info->namespace_;
    if (db == "information_schema") {
        namespace_ = "INTERNAL";
    }
    std::string full_name = namespace_ + "." + db + "." + table;
    int64_t table_id = -1;
    if (factory->get_table_id(full_name, table_id) != 0) {
        client->state = STATE_ERROR;
        client->query_ctx->stat_info.error_code = ER_NO_SUCH_TABLE;
        client->query_ctx->stat_info.error_msg << "Table '" << db << "."
                                               << table << "' not exist";
        return false;
    }
    // Make rows.
    std::vector<std::vector<std::string> > rows;
    std::vector<std::string> row;
    row.emplace_back(table);
    std::ostringstream oss;
    TableInfo info = factory->get_table_info(table_id);
    oss << "CREATE TABLE `" << table << "` (\n";
    for (auto& field : info.fields) {
        if (field.deleted) {
            continue;
        }
        oss << "  " << "`" << field.short_name << "` ";
        oss << type_map[field.type] << " ";
        oss << (field.can_null ? "NULL " : "NOT NULL ");
        if (!field.default_expr_value.is_null()) {
            oss << "DEFAULT ";
            if (field.default_value == "(current_timestamp())") {
                oss << "CURRENT_TIMESTAMP ";
            } else {
                oss << "'" << field.default_value << "' ";
            }
        }
        if (!field.on_update_value.empty()) {
            if (field.on_update_value == "(current_timestamp())") {
                oss << "ON UPDATE " << "CURRENT_TIMESTAMP ";
            }
        }
        oss << (field.auto_inc ? "AUTO_INCREMENT " : "");
        if (!field.comment.empty()) {
            oss << "COMMENT '" << field.comment << "'";
        }
        oss << ",\n";
    }
    uint32_t index_idx = 0;
    for (auto& index_id : info.indices) {
        IndexInfo index_info = factory->get_index_info(index_id);
        if (index_info.is_global) {
            oss << " " << index_map[index_info.type] << " GLOBAL ";
        }  else {
            oss << "  " << index_map[index_info.type] << " ";
        }
        if (index_info.index_hint_status == pb::IHS_VIRTUAL) {
            oss << "VIRTUAL ";
        }
        if (index_info.type != pb::I_PRIMARY) {
            std::vector<std::string> split_vec;
            boost::split(split_vec, index_info.name,
                         boost::is_any_of("."), boost::token_compress_on);
            oss << "`" << split_vec[split_vec.size() - 1] << "` ";
        }
        oss << "(";
        uint32_t field_idx = 0;
        for (auto& field : index_info.fields) {
            std::vector<std::string> split_vec;
            boost::split(split_vec, field.name,
                         boost::is_any_of("."), boost::token_compress_on);
            if (++field_idx < index_info.fields.size()) {
                oss << "`" << split_vec[split_vec.size() - 1] << "`,";
            } else {
                oss << "`" << split_vec[split_vec.size() - 1] << "`";
            }
        }
        oss << ") COMMENT '{\"index_state\":\"";
        oss << pb::IndexState_Name(index_info.state) << "\", ";
        if (index_info.type == pb::I_FULLTEXT) {
            oss << "\"segment_type\":\"" << pb::SegmentType_Name(index_info.segment_type) << "\", ";
            oss << "\"storage_type\":\"" << pb::StorageType_Name(index_info.storage_type) << "\", ";
        }
        oss << "\"hint_status\":\"" << pb::IndexHintStatus_Name(index_info.index_hint_status) << "\"}'";
        if (++index_idx < info.indices.size()) {
            oss << ",\n";
        } else {
            oss << "\n";
        }
    }
    static std::map<pb::Engine, std::string> engine_map = {
            {pb::ROCKSDB, "Rocksdb"},
            {pb::REDIS, "Redis"},
            {pb::ROCKSDB_CSTORE, "Rocksdb_cstore"},
            {pb::BINLOG, "Binlog"},
            {pb::INFORMATION_SCHEMA, "MEMORY"}
    };
    oss << ") ENGINE=" << engine_map[info.engine];
    oss << " DEFAULT CHARSET=" << charset_map[info.charset];
    oss <<" AVG_ROW_LENGTH=" << info.byte_size_per_record;
    oss << " COMMENT='{\"resource_tag\":\"" << info.resource_tag << "\"";
    oss << ", \"replica_num\":" << info.replica_num;
    oss << ", \"region_split_lines\":" << info.region_split_lines;
    if (info.ttl_info.ttl_duration_s > 0) {
        oss << ", \"ttl_duration\":" << info.ttl_info.ttl_duration_s;
    }
    if (info.learner_resource_tags.size() > 0) {
        oss << ", \"learner_resource_tag\": [";
        for (size_t i = 0; i < info.learner_resource_tags.size(); i++) {
            oss << "\"" << info.learner_resource_tags[i] << "\"";
            if (i != info.learner_resource_tags.size() - 1) {
                oss << ",";
            }
        }
        oss << "]";
    }
    if (info.dists.size() > 0) {
        oss << ", \"dists\": [";
        for (size_t i = 0; i < info.dists.size(); ++i) {
            oss << " {\"logical_room\":\"" << info.dists[i].logical_room << "\", ";
            oss << "\"count\":" << info.dists[i].count << "}";
            if (i != info.dists.size() -1) {
                oss << ",";
            }
        }
        oss << "]";
    }
    if (!info.main_logical_room.empty()) {
        oss << ", \"main_logical_room\": \"" << info.main_logical_room << "\"";
    }

    if (info.region_num > 0) {
        oss << ", \"region_num\":" << info.region_num;
    }
    oss << ", \"namespace\":\"" << info.namespace_ << "\"}'";
    if (info.partition_num > 1) {
        static std::map<pb::PartitionType, std::string> p_map = {
                {pb::PT_HASH, "hash"},
                {pb::PT_RANGE, "range"},
        };
        if (info.partition_info.type() == pb::PT_HASH) {
            oss << "\nPARTITION BY HASH (" << info.partition_info.field_info().field_name();
            oss << ") \nPARTITIONS " << info.partition_num;
        } else if (info.partition_info.type() == pb::PT_RANGE) {
            oss << "\nPARTITION BY RANGE (" << info.partition_info.field_info().field_name() << ")\n";
            if (info.partition_ptr != nullptr && !info.partition_ptr->to_str().empty()) {
                oss << "(" << info.partition_ptr->to_str() << "\n)";
            }
        }
    }
    row.emplace_back(oss.str());
    rows.emplace_back(row);
    // Make mysql packet.
    if (_make_common_resultset_packet(client, fields, rows) != 0) {
        DB_FATAL_CLIENT(client, "Failed to make result packet.");
        _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "%s", client->query_ctx->sql.c_str());
        client->state = STATE_ERROR;
        return false;
    }
    client->state = STATE_READ_QUERY_RESULT;
    return true;
}

bool ShowHelper::_show_socket(const SmartSocket& client, const std::vector<std::string>& split_vec) {
    // Make fields.
    std::vector<ResultField> fields;
    fields.reserve(3);
    do {
        ResultField field;
        field.name = "ip";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "count";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "username";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    std::map<std::string, std::map<std::string, int>> ip_map;
    EpollInfo* epoll_info = NetworkServer::get_instance()->get_epoll_info();
    for (int32_t idx = 0; idx < CONFIG_MPL_EPOLL_MAX_SIZE; ++idx) {
        const SmartSocket& sock = epoll_info->get_fd_mapping(idx);
        if (sock == NULL || sock->is_free || sock->fd == -1 || sock->ip == "") {
            continue;
        }
        ip_map[sock->ip][sock->username]++;
    }
    // Make rows.
    std::vector< std::vector<std::string> > rows;
    rows.reserve(10);
    for (auto& pair : ip_map) {
        for (auto& pair2 : pair.second) {
            std::vector<std::string> row;
            row.emplace_back(pair.first);
            row.emplace_back(std::to_string(pair2.second));
            row.emplace_back(pair2.first);
            rows.emplace_back(row);
        }
    }
    std::sort(rows.begin(), rows.end(),
              [](const std::vector<std::string>& left, const std::vector<std::string>& right) {
                  int l = atoi(left[1].c_str());
                  int r = atoi(right[1].c_str());
                  return l < r;
              });
    // Make mysql packet.
    if (_make_common_resultset_packet(client, fields, rows) != 0) {
        DB_FATAL_CLIENT(client, "Failed to make result packet.");
        _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
        client->state = STATE_ERROR;
        return false;
    }
    client->state = STATE_READ_QUERY_RESULT;
    return true;
}

bool ShowHelper::_show_processlist(const SmartSocket& client, const std::vector<std::string>& split_vec) {
    // Make fields.
    std::vector<ResultField> fields;
    fields.reserve(8);
    do {
        ResultField field;
        field.name = "Id";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "User";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Host";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "db";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Command";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Time";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "State";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Info";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    // Make rows.
    std::vector< std::vector<std::string> > rows;
    rows.reserve(10);
    EpollInfo* epoll_info = NetworkServer::get_instance()->get_epoll_info();
    for (int32_t idx = 0; idx < CONFIG_MPL_EPOLL_MAX_SIZE; ++idx) {
        const SmartSocket& sock = epoll_info->get_fd_mapping(idx);
        if (sock == NULL || sock->is_free || sock->fd == -1 || sock->ip == "") {
            if (sock != NULL) {
                DB_WARNING_CLIENT(sock, "processlist, free:%d", sock->is_free);
            }
            continue;
        }
        if (!sock->user_info || !sock->query_ctx) {
            DB_FATAL("param invalid");
            return false;
        }
        DB_WARNING_CLIENT(sock, "processlist, free:%d", sock->is_free);
        std::vector<std::string> row;
        row.emplace_back(std::to_string(sock->conn_id));
        row.emplace_back(sock->user_info->username);
        row.emplace_back(sock->ip);
        row.emplace_back(sock->current_db);
        auto command = sock->query_ctx->mysql_cmd;
        if (command == COM_SLEEP) {
            row.emplace_back("Sleep");
        } else {
            row.emplace_back("Query");
        }
        row.emplace_back(std::to_string(time(NULL) - sock->last_active));
        if (command == COM_SLEEP) {
            row.emplace_back(" ");
        } else {
            row.emplace_back("executing");
        }
        if (command == COM_SLEEP) {
            row.emplace_back("");
        } else {
            row.emplace_back(sock->query_ctx->sql);
        }
        rows.emplace_back(row);
    }
    // Make mysql packet.
    if (_make_common_resultset_packet(client, fields, rows) != 0) {
        DB_FATAL_CLIENT(client, "Failed to make result packet.");
        _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
        client->state = STATE_ERROR;
        return false;
    }
    client->state = STATE_READ_QUERY_RESULT;
    return true;
}

bool ShowHelper::_show_warnings(const SmartSocket& client, const std::vector<std::string>& split_vec) {
    if (client == nullptr) {
        DB_FATAL("param invalid");
        //client->state = STATE_ERROR;
        return false;
    }

    // Make result info.
    std::vector<ResultField> fields;
    fields.reserve(3);
    do {
        ResultField field;
        field.name = "Level";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Code";
        field.type = MYSQL_TYPE_LONG;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Message";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    // Make rows.
    std::vector< std::vector<std::string> > rows;

    // Make mysql packet.
    if (_make_common_resultset_packet(client, fields, rows) != 0) {
        DB_FATAL_CLIENT(client, "Failed to package mysql common result.");
        _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
        client->state = STATE_ERROR;
        return false;
    }
    client->state = STATE_READ_QUERY_RESULT;
    return true;
}

bool ShowHelper::_show_cost(const SmartSocket& client, const std::vector<std::string>& split_vec) {
    SchemaFactory* factory = SchemaFactory::get_instance();
    if (client == nullptr || client->query_ctx == nullptr || factory == nullptr) {
        DB_FATAL("param invalid");
        //client->state = STATE_ERROR;
        return false;
    }

    if (split_vec.size() != 3) {
        client->state = STATE_ERROR;
        return false;
    }

    std::vector<std::string> database_table;
    if (split_vec[2] == "switch") {
        factory->get_cost_switch_open(database_table);
    } else {
        factory->table_with_statistics_info(database_table);
    }
    DB_WARNING("show cost");
    std::vector< std::vector<std::string> > rows;
    rows.reserve(10);
    for (auto& d_t_name : database_table) {
        DB_WARNING("%s", d_t_name.c_str());
        std::vector<std::string> split_vec;
        boost::split(split_vec, d_t_name,
                     boost::is_any_of("."), boost::token_compress_on);
        if (split_vec.size() != 3 ) {
            DB_FATAL("databae table name:%s", d_t_name.c_str());
            continue;
        }
        rows.emplace_back(split_vec);
    }

    std::vector<std::string> names = { "name_space", "database_name", "table_name" };

    std::vector<ResultField> fields;
    fields.reserve(3);
    for (auto& name : names) {
        ResultField field;
        field.name = name;
        field.type = MYSQL_TYPE_STRING;
        fields.emplace_back(field);
    }
    // Make mysql packet.
    if (_make_common_resultset_packet(client, fields, rows) != 0) {
        DB_FATAL_CLIENT(client, "Failed to make result packet.");
        _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
        client->state = STATE_ERROR;
        return false;
    }
    client->state = STATE_READ_QUERY_RESULT;
    return true;
}

bool ShowHelper::_show_full_tables(const SmartSocket& client, const std::vector<std::string>& split_vec)  {
    bool is_like_pattern = false;
    std::string like_pattern;
    re2::RE2::Options option;
    std::unique_ptr<re2::RE2> regex_ptr;
    if (client == nullptr) {
        DB_FATAL("param invalid");
        //client->state = STATE_ERROR;
        return false;
    }
    SchemaFactory* factory = SchemaFactory::get_instance();
    if (factory == nullptr || client == nullptr || client->user_info == nullptr || client->query_ctx == nullptr) {
        DB_FATAL("param invalid");
        return false;
    }

    std::string namespace_ = client->user_info->namespace_;
    std::string current_db = client->current_db;


    if (split_vec.size() == 5) {
        current_db = remove_quote(split_vec[4].c_str(), '`');
    } else if (split_vec.size() == 3) {
    } else if (split_vec.size() == 7) {
        is_like_pattern = true;
        std::string like_str;
        current_db = remove_quote(split_vec[4].c_str(), '`');
        like_str = remove_quote(split_vec[6].c_str(), '"');
        like_str = remove_quote(like_str.c_str(), '\'');
        for (auto ch : like_str) {
            if (ch == '%') {
                like_pattern.append(".*");
            } else {
                like_pattern.append(1, ch);
            }
        }
        option.set_utf8(false);
        option.set_case_sensitive(false);
        regex_ptr.reset(new re2::RE2(like_pattern, option));

    } else {
        client->state = STATE_ERROR;
        return false;
    }

    if (current_db == "") {
        DB_WARNING("no database selected");
        _wrapper->make_err_packet(client, ER_NO_DB_ERROR, "No database selected");
        client->state = STATE_READ_QUERY_RESULT;
        return false;
    }
    if (current_db == "information_schema") {
        namespace_ = "INTERNAL";
    }

    // Make fields.
    std::vector<ResultField> fields;
    fields.reserve(2);
    do {
        ResultField field;
        field.name = "Tables_in_" + current_db;
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Table_type";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);

    // Make rows.
    std::vector< std::vector<std::string> > rows;
    rows.reserve(10);
    std::vector<std::string> tables =  factory->get_table_list(
            namespace_, current_db, client->user_info.get());
    //DB_NOTICE("db:%s table.size:%d", current_db.c_str(), tables.size());
    for (uint32_t cnt = 0; cnt < tables.size(); ++cnt) {
        //DB_NOTICE("table:%s", tables[cnt].c_str());
        if (is_like_pattern) {
            if (!RE2::FullMatch(tables[cnt], *regex_ptr)) {
                DB_NOTICE("not match");
                continue;
            }
        }
        std::vector<std::string> row;
        row.emplace_back(tables[cnt]);
        row.emplace_back("BASE TABLE");
        rows.emplace_back(row);
    }

    // Make mysql packet.
    if (_make_common_resultset_packet(client, fields, rows) != 0) {
        DB_FATAL_CLIENT(client, "Failed to make result packet.");
        _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "%s", client->query_ctx->sql.c_str());
        client->state = STATE_ERROR;
        return false;
    }
    client->state = STATE_READ_QUERY_RESULT;
    return true;
}

bool ShowHelper::_show_full_columns(const SmartSocket& client, const std::vector<std::string>& split_vec) {
    SchemaFactory* factory = SchemaFactory::get_instance();
    if (client == nullptr || client->user_info == nullptr || factory == nullptr) {
        DB_FATAL("param invalid");
        //client->state = STATE_ERROR;
        return false;
    }

    // Make fields.
    std::vector<ResultField> fields;
    fields.reserve(9);
    do {
        ResultField field;
        field.name = "Field";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Type";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Collation";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Null";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Key";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "default";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Extra";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Privileges";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Comment";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);

    std::string db = client->current_db;
    std::string table;
    if (split_vec.size() == 5) {
        table = remove_quote(split_vec[4].c_str(), '`');
    } else if (split_vec.size() == 7) {
        db = remove_quote(split_vec[6].c_str(), '`');
        table = remove_quote(split_vec[4].c_str(), '`');
    } else {
        client->state = STATE_ERROR;
        return false;
    }
    std::string namespace_ = client->user_info->namespace_;
    if (db == "information_schema") {
        namespace_ = "INTERNAL";
    }
    std::string full_name = namespace_ + "." + db + "." + table;
    int64_t table_id = -1;
    if (factory->get_table_id(full_name, table_id) != 0) {
        client->state = STATE_ERROR;
        return false;
    }
    TableInfo info = factory->get_table_info(table_id);
    std::map<int32_t, IndexInfo> field_index;
    for (auto& index_id : info.indices) {
        IndexInfo index_info = factory->get_index_info(index_id);
        for (auto& field : index_info.fields) {
            if (field_index.count(field.id) == 0) {
                field_index[field.id] = index_info;
            }
        }
    }
    // Make rows.
    std::vector<std::vector<std::string> > rows;
    rows.reserve(10);
    for (auto& field : info.fields) {
        if (field.deleted) {
            continue;
        }
        std::vector<std::string> row;
        std::vector<std::string> split_vec;
        boost::split(split_vec, field.name,
                     boost::is_any_of(" \t\n\r."), boost::token_compress_on);
        row.emplace_back(split_vec[split_vec.size() - 1]);
        row.emplace_back("NULL");
        row.emplace_back(PrimitiveType_Name(field.type));
        row.emplace_back(field.can_null ? "YES" : "NO");
        if (field_index.count(field.id) == 0) {
            row.emplace_back(" ");
        } else {
            std::string index = IndexType_Name(field_index[field.id].type);
            if (field_index[field.id].type == pb::I_FULLTEXT) {
                index += "(" + pb::SegmentType_Name(field_index[field.id].segment_type) + ")";
            }
            row.emplace_back(index);
        }
        row.emplace_back(field.default_value);
        if (info.auto_inc_field_id == field.id) {
            row.emplace_back("auto_increment");
        } else {
            row.emplace_back(" ");
        }
        row.emplace_back("select,insert,update,references");
        row.emplace_back(" ");
        rows.emplace_back(row);
    }

    // Make mysql packet.
    if (_make_common_resultset_packet(client, fields, rows) != 0) {
        DB_FATAL_CLIENT(client, "Failed to make result packet.");
        _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
        client->state = STATE_ERROR;
        return false;
    }
    client->state = STATE_READ_QUERY_RESULT;
    return true;
}

bool ShowHelper::_show_table_status(const SmartSocket& client, const std::vector<std::string>& split_vec) {
    SchemaFactory* factory = SchemaFactory::get_instance();
    if (client == nullptr || client->user_info == nullptr || factory == nullptr) {
        DB_FATAL("param invalid");
        //client->state = STATE_ERROR;
        return false;
    }

    // Make fields.
    std::vector<ResultField> fields;
    fields.reserve(18);
    do {
        ResultField field;
        field.name = "Name";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Engine";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Version";
        field.type = MYSQL_TYPE_LONG;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Row_format";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Rows";
        field.type = MYSQL_TYPE_LONG;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Avg_row_length";
        field.type = MYSQL_TYPE_LONG;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Data_length";
        field.type = MYSQL_TYPE_LONG;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Max_data_length";
        field.type = MYSQL_TYPE_LONG;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Index_length";
        field.type = MYSQL_TYPE_LONG;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Data_free";
        field.type = MYSQL_TYPE_LONG;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Auto_increment";
        field.type = MYSQL_TYPE_LONG;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Create_time";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Update_time";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Check_time";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Collation";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Checksum";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Create_options";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Comment";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);

    std::string namespace_ = client->user_info->namespace_;
    std::string db = client->current_db;
    if (db == "") {
        DB_WARNING("no database selected");
        _wrapper->make_err_packet(client, ER_NO_DB_ERROR, "No database selected");
        client->state = STATE_READ_QUERY_RESULT;
        return false;
    }
    if (db == "information_schema") {
        namespace_ = "INTERNAL";
    }

    // Make rows.
    std::vector< std::vector<std::string> > rows;
    rows.reserve(10);
    std::vector<std::string> tables;
    if (split_vec.size() == 5) {
        std::string table = remove_quote(split_vec[4].c_str(), '\'');
        tables.emplace_back(table);
    } else if (split_vec.size() == 3) {
        tables =  factory->get_table_list(namespace_, db, client->user_info.get());
    } else {
        client->state = STATE_ERROR;
        return false;
    }
    for (auto table : tables) {
        std::string full_name = namespace_ + "." + db + "." + table;
        int64_t table_id = -1;
        if (factory->get_table_id(full_name, table_id) != 0) {
            client->state = STATE_ERROR;
            return false;
        }
        TableInfo info = factory->get_table_info(table_id);
        pb::QueryRequest req;
        req.set_op_type(pb::QUERY_TABLE_FLATTEN);
        req.set_namespace_name(client->user_info->namespace_);
        req.set_database(db);
        req.set_table_name(table);
        pb::QueryResponse res;
        MetaServerInteract::get_instance()->send_request("query", req, res);
        std::string create_time = "2018-08-09 15:01:40";
        int64_t avg_row_length = 0;
        int64_t row_count = 0;
        if (res.flatten_tables_size() == 1) {
            create_time = res.flatten_tables(0).create_time();
            row_count = res.flatten_tables(0).row_count();
            avg_row_length = res.flatten_tables(0).byte_size_per_record();
        }
        // Make rows.
        std::vector<std::string> row;
        row.emplace_back(table);
        row.emplace_back("Innodb");
        row.emplace_back(std::to_string(info.version));
        row.emplace_back("Compact");
        row.emplace_back(std::to_string(row_count));
        row.emplace_back(std::to_string(avg_row_length));
        row.emplace_back("0");
        row.emplace_back("0");
        row.emplace_back("0");
        row.emplace_back("0");
        row.emplace_back("0");
        row.emplace_back(create_time);
        row.emplace_back("");
        row.emplace_back("");
        row.emplace_back("utf8_general_ci");
        row.emplace_back("");
        row.emplace_back("");
        row.emplace_back("");
        rows.emplace_back(row);
    }

    // Make mysql packet.
    if (_make_common_resultset_packet(client, fields, rows) != 0) {
        DB_FATAL_CLIENT(client, "Failed to make result packet.");
        _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
        client->state = STATE_ERROR;
        return false;
    }
    client->state = STATE_READ_QUERY_RESULT;
    return true;
}

bool ShowHelper::_show_schema_conf(const SmartSocket& client, const std::vector<std::string>& split_vec) {
    SchemaFactory* factory = SchemaFactory::get_instance();
    if (client == nullptr || client->query_ctx == nullptr || factory == nullptr) {
        DB_FATAL("param invalid");
        //client->state = STATE_ERROR;
        return false;
    }

    std::unordered_set<std::string> allowed_conf = {"need_merge",
                                                    "storage_compute_separate",
                                                    "select_index_by_cost",
                                                    "pk_prefix_balance"};
    // 前三个conf按照bool解析, pk_prefix_balance按照int32来解析
    if (split_vec.size() != 3 || allowed_conf.find(split_vec[2]) == allowed_conf.end()) {
        client->state = STATE_ERROR;
        return false;
    }

    std::vector<std::string> database_table;
    factory->get_schema_conf_open(split_vec[2], database_table);
    DB_WARNING("show schema_conf: %s", split_vec[2].c_str());
    std::vector< std::vector<std::string> > rows;
    rows.reserve(10);
    for (auto& d_t_name : database_table) {
        DB_WARNING("%s", d_t_name.c_str());
        std::vector<std::string> split_vec;
        boost::split(split_vec, d_t_name,
                     boost::is_any_of("."), boost::token_compress_on);
        if (split_vec.size() != 3 && split_vec.size() != 4) {
            DB_FATAL("database table name:%s", d_t_name.c_str());
            continue;
        }
        rows.emplace_back(split_vec);
    }

    std::vector<std::string> names = { "namespace", "database_name", "table_name" };
    if (split_vec[2] == "pk_prefix_balance") {
        names.emplace_back("value");
    }

    std::vector<ResultField> fields;
    fields.reserve(3);
    for (auto& name : names) {
        ResultField field;
        field.name = name;
        field.type = MYSQL_TYPE_STRING;
        fields.emplace_back(field);
    }
    // Make mysql packet.
    if (_make_common_resultset_packet(client, fields, rows) != 0) {
        DB_FATAL_CLIENT(client, "Failed to make result packet.");
        _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
        client->state = STATE_ERROR;
        return false;
    }
    client->state = STATE_READ_QUERY_RESULT;
    return true;
}

bool ShowHelper::_show_all_tables(const SmartSocket& client, const std::vector<std::string>& split_vec) {
    SchemaFactory* factory = SchemaFactory::get_instance();
    if (client == nullptr || client->query_ctx == nullptr || factory == nullptr) {
        DB_FATAL("param invalid");
        //client->state = STATE_ERROR;
        return false;
    }
    if (split_vec.size() != 3) {
        client->state = STATE_ERROR;
        return false;
    }

    std::vector<std::string> database_table;
    std::map<std::string, std::function<bool(const SmartTable&)>> type_func_map;
    type_func_map["binlog"] = [](const SmartTable& table) {
        return table != nullptr && table->binlog_id > 0;
    };
    type_func_map["ttl"] = [](const SmartTable& table) {
        return table != nullptr && table->ttl_info.ttl_duration_s > 0;
    };

    if (type_func_map[split_vec[2]] != nullptr) {
        factory->get_table_by_filter(database_table, type_func_map[split_vec[2]]);
    } else {
        DB_WARNING("not support type:%s", split_vec[2].c_str());
        return false;
    }
    std::vector< std::vector<std::string> > rows;
    rows.reserve(10);
    for (auto& d_t_name : database_table) {
        DB_WARNING("%s", d_t_name.c_str());
        std::vector<std::string> split_vec;
        boost::split(split_vec, d_t_name,
                     boost::is_any_of("."), boost::token_compress_on);
        if (split_vec.size() != 3) {
            DB_FATAL("database table name:%s", d_t_name.c_str());
            continue;
        }
        rows.emplace_back(split_vec);
    }

    std::vector<std::string> names = { "namespace", "database_name", "table_name" };

    std::vector<ResultField> fields;
    fields.reserve(3);
    for (auto& name : names) {
        ResultField field;
        field.name = name;
        field.type = MYSQL_TYPE_STRING;
        fields.emplace_back(field);
    }
    // Make mysql packet.
    if (_make_common_resultset_packet(client, fields, rows) != 0) {
        DB_FATAL_CLIENT(client, "Failed to make result packet.");
        _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
        client->state = STATE_ERROR;
        return false;
    }
    client->state = STATE_READ_QUERY_RESULT;
    return true;
}

bool ShowHelper::_show_region(const SmartSocket& client, const std::vector<std::string>& split_vec) {
    SchemaFactory* factory = SchemaFactory::get_instance();
    if (factory == nullptr || client == nullptr) {
        DB_FATAL("param invalid");
        return false;
    }

    if (split_vec.size() < 4) {
        client->state = STATE_ERROR;
        return false;
    }
    int64_t table_id = strtoll(split_vec[2].c_str(), NULL, 10);
    int64_t region_id = strtoll(split_vec[3].c_str(), NULL, 10);
    DB_WARNING("table_id:%ld, region_id: %ld", table_id, region_id);

    // Make fields.
    std::vector<ResultField> fields;
    fields.reserve(2);
    do {
        ResultField field;
        field.name = "region_id";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "region_info";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);

    // Make rows.
    std::vector< std::vector<std::string> > rows;
    rows.reserve(10);

    pb::RegionInfo region_info;
    if (factory->get_region_info(table_id, region_id, region_info) == 0) {
        int64_t table_id = region_info.table_id();
        auto index_info = factory->get_index_info(table_id);
        if (region_info.start_key().size() > 0) {
            TableKey start_key(region_info.start_key());
            region_info.set_start_key(start_key.decode_start_key_string(index_info));
        } else {
            region_info.set_start_key("-∞");
        }
        if (region_info.end_key().size() > 0) {
            TableKey end_key(region_info.end_key());
            region_info.set_end_key(end_key.decode_start_key_string(index_info));
        } else {
            region_info.set_end_key("+∞");
        }
        std::vector<std::string> row;
        row.emplace_back(std::to_string(region_id));
        row.emplace_back(region_info.ShortDebugString().c_str());
        rows.emplace_back(row);
    } else {
        DB_WARNING("region: %ld does not exist", region_id);
    }

    // Make mysql packet.
    if (_make_common_resultset_packet(client, fields, rows) != 0) {
        DB_FATAL_CLIENT(client, "Failed to make result packet.");
        _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
        client->state = STATE_ERROR;
        return false;
    }
    client->state = STATE_READ_QUERY_RESULT;
    return true;
}

bool ShowHelper::_show_store_region(const SmartSocket& client, const std::vector<std::string>& split_vec) {
    SchemaFactory* factory = SchemaFactory::get_instance();
    if (client == nullptr || client->query_ctx == nullptr || factory == nullptr) {
        DB_FATAL("param invalid");
        //client->state = STATE_ERROR;
        return false;
    }

    if (split_vec.size() != 5) {
        client->state = STATE_ERROR;
        return false;
    }
    std::string store_addr = split_vec[3];
    int64_t region_id = strtoll(split_vec[4].c_str(), NULL, 10);
    DB_WARNING("region_id: %ld", region_id);

    // Make fields.
    std::vector<ResultField> fields;
    fields.reserve(2);
    do {
        ResultField field;
        field.name = "region_id";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);

    do {
        ResultField field;
        field.name = "region_info";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);

    // Make rows.
    std::vector< std::vector<std::string> > rows;
    rows.reserve(10);

    pb::RegionIds req;
    req.add_region_ids(region_id);
    pb::StoreRes res;
    StoreInteract interact(store_addr);
    interact.send_request("query_region", req, res);
    pb::RegionInfo region_info;
    if (res.regions_size() == 1) {
        region_info = res.regions(0);
        int64_t table_id = region_info.table_id();
        auto index_info = factory->get_index_info(table_id);
        if (region_info.start_key().size() > 0) {
            TableKey start_key(region_info.start_key());
            region_info.set_start_key(start_key.decode_start_key_string(index_info));
        } else {
            region_info.set_start_key("-∞");
        }
        if (region_info.end_key().size() > 0) {
            TableKey end_key(region_info.end_key());
            region_info.set_end_key(end_key.decode_start_key_string(index_info));
        } else {
            region_info.set_end_key("+∞");
        }
        std::vector<std::string> row;
        row.emplace_back(std::to_string(region_id));
        row.emplace_back(region_info.ShortDebugString().c_str());
        rows.emplace_back(row);
    } else {
        DB_WARNING("region: %ld does not exist", region_id);
    }

    // Make mysql packet.
    if (_make_common_resultset_packet(client, fields, rows) != 0) {
        DB_FATAL_CLIENT(client, "Failed to make result packet.");
        _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
        client->state = STATE_ERROR;
        return false;
    }
    client->state = STATE_READ_QUERY_RESULT;
    return true;
}

bool ShowHelper::_show_virtual_index(const SmartSocket& client, const std::vector<std::string>& split_vec) {
    SchemaFactory* factory = SchemaFactory::get_instance();
    if (client == nullptr || client->query_ctx == nullptr || factory == nullptr) {
        DB_FATAL("param invalid");
        //client->state = STATE_ERROR;
        return false;
    }

    if (split_vec.size() != 3) {
        client->state = STATE_ERROR;
        return false;
    }

    std::vector<std::string> database_table;
    VirtualIndexMap sample = factory->get_virtual_index_info();
    std::vector< std::vector<std::string> > rows;
    rows.reserve(10);
    for (auto& iter : sample.index_id_sample_sqls_map) {
        std::string index_name = sample.index_id_name_map[iter.first];
        for (auto& sample_sql : iter.second) {
            std::vector<std::string> row;
            std::string database;
            std::string table;
            std::string sql;
            _parse_sample_sql(sample_sql, database, table, sql);
            uint64_t out[2];
            butil::MurmurHash3_x64_128(sample_sql.c_str(), sample_sql.size(), 0x1234, out);
            std::string sign = std::to_string(out[0]);
            row.emplace_back(database);
            row.emplace_back(table);
            row.emplace_back(index_name);
            row.emplace_back(sign);
            row.emplace_back(sql);
            rows.emplace_back(row);
        }
    }

    std::vector<std::string> names = { "database_name", "table_name", "virtual_index_name", "sign", "sql" };

    std::vector<ResultField> fields;
    fields.reserve(5);
    for (auto& name : names) {
        ResultField field;
        field.name = name;
        field.type = MYSQL_TYPE_STRING;
        fields.emplace_back(field);
    }
    // Make mysql packet.
    if (_make_common_resultset_packet(client, fields, rows) != 0) {
        DB_FATAL_CLIENT(client, "Failed to make result packet.");
        _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
        client->state = STATE_ERROR;
        return false;
    }
    client->state = STATE_READ_QUERY_RESULT;
    return true;
}

bool ShowHelper::_show_variables(const SmartSocket& client, const std::vector<std::string>& split_vec) {
    // type == SQL_SHOW_NUM
    if (client == nullptr) {
        DB_FATAL("param invalid");
        //client->state = STATE_ERROR;
        return false;
    }

    // Make fields.
    std::vector<ResultField> fields;
    fields.reserve(2);
    do {
        ResultField field;
        field.name = "Variable_name";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "Value";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);

    // Make rows.
    std::vector< std::vector<std::string> > rows;
    rows.reserve(10);
    do {
        std::vector<std::string> row;
        row.emplace_back("character_set_client");
        row.emplace_back(client->charset_name);
        rows.emplace_back(row);
    } while (0);
    do {
        std::vector<std::string> row;
        row.emplace_back("character_set_connection");
        row.emplace_back(client->charset_name);
        rows.emplace_back(row);
    } while (0);
    do {
        std::vector<std::string> row;
        row.emplace_back("character_set_results");
        row.emplace_back(client->charset_name);
        rows.emplace_back(row);
    } while (0);
    do {
        std::vector<std::string> row;
        row.emplace_back("character_set_server");
        row.emplace_back(client->charset_name);
        rows.emplace_back(row);
    } while (0);
    do {
        std::vector<std::string> row;
        row.emplace_back("init_connect");
        row.emplace_back(" ");
        rows.emplace_back(row);
    } while (0);
    do {
        std::vector<std::string> row;
        row.emplace_back("interactive_timeout");
        row.emplace_back("28800");
        rows.emplace_back(row);
    } while (0);
    do {
        std::vector<std::string> row;
        row.emplace_back("language");
        row.emplace_back("/home/mysql/mysql/share/mysql/english/");
        rows.emplace_back(row);
    } while (0);
    do {
        std::vector<std::string> row;
        row.emplace_back("lower_case_table_names");
        row.emplace_back("0");
        rows.emplace_back(row);
    } while (0);
    do {
        std::vector<std::string> row;
        row.emplace_back("max_allowed_packet");
        row.emplace_back("268435456");
        rows.emplace_back(row);
    } while (0);
    do {
        std::vector<std::string> row;
        row.emplace_back("net_buffer_length");
        row.emplace_back("16384");
        rows.emplace_back(row);
    } while (0);
    do {
        std::vector<std::string> row;
        row.emplace_back("net_write_timeout");
        row.emplace_back("60");
        rows.emplace_back(row);
    } while (0);
    do {
        std::vector<std::string> row;
        row.emplace_back("query_cache_size");
        row.emplace_back("335544320");
        rows.emplace_back(row);
    } while (0);
    do {
        std::vector<std::string> row;
        row.emplace_back("query_cache_type");
        row.emplace_back("OFF");
        rows.emplace_back(row);
    } while (0);
    do {
        std::vector<std::string> row;
        row.emplace_back("sql_mode");
        row.emplace_back(" ");
        rows.emplace_back(row);
    } while (0);
    do {
        std::vector<std::string> row;
        row.emplace_back("system_time_zone");
        row.emplace_back("CST");
        rows.emplace_back(row);
    } while (0);
    do {
        std::vector<std::string> row;
        row.emplace_back("time_zone");
        row.emplace_back("SYSTEM");
        rows.emplace_back(row);
    } while (0);
    do {
        std::vector<std::string> row;
        row.emplace_back("tx_isolation");
        row.emplace_back("REPEATABLE-READ");
        rows.emplace_back(row);
    } while (0);
    do {
        std::vector<std::string> row;
        row.emplace_back("wait_timeout");
        row.emplace_back("28800");
        rows.emplace_back(row);
    } while (0);
    do {
        std::vector<std::string> row;
        row.emplace_back("auto_increment_increment");
        row.emplace_back("1");
        rows.emplace_back(row);
    } while (0);

    // Make mysql packet.
    if (_make_common_resultset_packet(client, fields, rows) != 0) {
        DB_FATAL_CLIENT(client, "Failed to make result packet.");
        _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
        client->state = STATE_ERROR;
        return false;
    }
    client->state = STATE_READ_QUERY_RESULT;
    return true;
}

bool ShowHelper::_show_user(const SmartSocket& client, const std::vector<std::string>& split_vec) {
    SchemaFactory* factory = SchemaFactory::get_instance();
    if (factory == nullptr || client == nullptr || split_vec.size() != 3) {
        DB_FATAL("param invalid");
        return false;
    }

    auto info = factory->get_user_info(split_vec[2]);
    if (info == nullptr) {
        DB_WARNING("user name not exist [%s]", split_vec[2].c_str());
        _wrapper->make_err_packet(client, ER_NO_SUCH_USER, "No Such User");
        client->state = STATE_READ_QUERY_RESULT;
        return false;
    }

    // Make fields.
    std::vector<ResultField> fields;
    fields.reserve(5);
    std::vector<std::string> names = {"Username", "Password", "Namespace Name", "Version", "Auth IPs"};
    for(auto name : names) {
        ResultField field;
        field.name = name;
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    }

    // Make rows.
    std::vector< std::vector<std::string> > rows;
    rows.reserve(1);
    std::vector<std::string> row;
    row.emplace_back(info->username);
    row.emplace_back(info->password);
    row.emplace_back(info->namespace_);
    row.emplace_back(std::to_string(info->version));
    std::string ips;
    for(auto ip : info->auth_ip_set) {
        ips.append(ip);
    }
    row.emplace_back(ips);
    rows.emplace_back(row);

    // Make mysql packet.
    if (_make_common_resultset_packet(client, fields, rows) != 0) {
        DB_FATAL_CLIENT(client, "Failed to make result packet.");
        _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
        client->state = STATE_ERROR;
        return false;
    }
    client->state = STATE_READ_QUERY_RESULT;
    return true;
}

bool ShowHelper::_show_privilege(const SmartSocket& client, const std::vector<std::string>& split_vec) {
    SchemaFactory* factory = SchemaFactory::get_instance();
    if (factory == nullptr || client == nullptr || split_vec.size() != 3) {
        DB_FATAL("param invalid");
        return false;
    }

    const std::string &username = split_vec[2];
    auto info = factory->get_user_info(username);
    if (info == nullptr) {
        DB_WARNING("user name not exist [%s]", username.c_str());
        _wrapper->make_err_packet(client, ER_NO_SUCH_USER, "No Such User");
        client->state = STATE_READ_QUERY_RESULT;
        return false;
    }

    // Make fields.
    std::vector<ResultField> fields;
    fields.reserve(5);
    std::vector<std::string> names = {"Database ID", "Database Name", "Table ID", "Table Name", "RW"};
    for(auto name : names) {
        ResultField field;
        field.name = name;
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    }

    pb::QueryRequest request;
    pb::QueryResponse response;
    request.set_op_type(pb::QUERY_USERPRIVILEG);
    request.set_user_name(username);
    MetaServerInteract::get_instance()->send_request("query", request, response);
    // Make rows.
    std::vector< std::vector<std::string> > rows;
    rows.reserve(10);
    for(auto user_privilege : response.user_privilege()) {
        for (auto db : user_privilege.privilege_database()) {
            std::vector<std::string> row;
            row.reserve(5);
            row.emplace_back(std::to_string(db.database_id()));
            row.emplace_back(db.database());
            row.emplace_back("*");
            row.emplace_back("*");
            row.emplace_back(db.database_rw() == pb::WRITE ? "write" : "read");
            rows.emplace_back(row);
        }
        for (auto table : user_privilege.privilege_table()) {
            std::vector<std::string> row;
            row.reserve(5);
            row.emplace_back(std::to_string(table.database_id()));
            row.emplace_back(table.database());
            row.emplace_back(std::to_string(table.table_id()));
            row.emplace_back(table.table_name());
            row.emplace_back(table.table_rw() == pb::WRITE ? "write" : "read");
            rows.emplace_back(row);
        }
    }

    // Make mysql packet.
    if (_make_common_resultset_packet(client, fields, rows) != 0) {
        DB_FATAL_CLIENT(client, "Failed to make result packet.");
        _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
        client->state = STATE_ERROR;
        return false;
    }
    client->state = STATE_READ_QUERY_RESULT;
    return true;
}

bool ShowHelper::_show_store_txn(const SmartSocket& client, const std::vector<std::string>& split_vec) {
    SchemaFactory* factory = SchemaFactory::get_instance();
    if (client == nullptr || client->query_ctx == nullptr || factory == nullptr) {
        DB_FATAL("param invalid");
        //client->state = STATE_ERROR;
        return false;
    }

    if (split_vec.size() != 4) {
        client->state = STATE_ERROR;
        return false;
    }
    std::string store_addr = split_vec[2];
    int64_t region_id = strtoll(split_vec[3].c_str(), NULL, 10);
    DB_WARNING("region_id: %ld", region_id);

    // Make fields.
    std::vector<ResultField> fields;
    std::vector<std::string> names = {"txn_id", "seq_id", "primary_region_id", "state"};
    std::unordered_map<pb::TxnState, std::string, std::hash<int>> state = {
            {pb::TXN_ROLLBACKED, "TXN_ROLLBACKED"},
            {pb::TXN_COMMITTED, "TXN_COMMITTED"},
            {pb::TXN_PREPARED, "TXN_PREPARED"},
            {pb::TXN_BEGINED, "TXN_BEGINED"}
    };
    fields.reserve(4);
    for(auto name : names) {
        ResultField field;
        field.name = name;
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    }

    // Make rows.
    std::vector< std::vector<std::string> > rows;
    rows.reserve(10);

    pb::StoreReq req;
    req.set_region_id(region_id);
    req.set_op_type(pb::OP_TXN_QUERY_STATE);
    req.set_region_version(0);
    pb::StoreRes res;
    StoreInteract interact(store_addr);
    interact.send_request("query", req, res);
    DB_WARNING("req:%s res:%s", req.ShortDebugString().c_str(), res.ShortDebugString().c_str());
    pb::RegionInfo region_info;
    for(auto txn_info : res.txn_infos()) {
        std::vector<std::string> row;
        row.emplace_back(std::to_string(txn_info.txn_id()));
        row.emplace_back(std::to_string(txn_info.seq_id()));
        row.emplace_back(std::to_string(txn_info.primary_region_id()));
        if (txn_info.has_txn_state() && state.find(txn_info.txn_state()) != state.end()) {
            row.emplace_back(state[txn_info.txn_state()]);
        } else {
            row.emplace_back("");
        }
        rows.emplace_back(row);
    }

    // Make mysql packet.
    if (_make_common_resultset_packet(client, fields, rows) != 0) {
        DB_FATAL_CLIENT(client, "Failed to make result packet.");
        _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
        client->state = STATE_ERROR;
        return false;
    }
    client->state = STATE_READ_QUERY_RESULT;
    return true;
}

bool ShowHelper::_show_ddl_work(const SmartSocket& client, const std::vector<std::string>& split_vec) {
    std::unordered_map<pb::DdlWorkStatus, std::string, std::hash<int>> state = {
            {pb::DdlWorkIdle,    "DdlWorkIdle"},
            {pb::DdlWorkDoing,   "DdlWorkDoing"},
            {pb::DdlWorkDone,    "DdlWorkDone"},
            {pb::DdlWorkFail,    "DdlWorkFail"},
            {pb::DdlWorkDupUniq, "DdlWorkDupUniq"},
            {pb::DdlWorkError,   "DdlWorkError"}

    };
    std::unordered_map<pb::IndexState, std::string, std::hash<int>> index_state = {
            {pb::IS_PUBLIC, "IS_PUBLIC"},
            {pb::IS_WRITE_LOCAL, "IS_WRITE_LOCAL"},
            {pb::IS_WRITE_ONLY, "IS_WRITE_ONLY"},
            {pb::IS_DELETE_ONLY, "IS_DELETE_ONLY"},
            {pb::IS_DELETE_LOCAL, "IS_DELETE_LOCAL"},
            {pb::IS_NONE, "IS_NONE"},
            {pb::IS_UNKNOWN, "IS_UNKNOWN"},
    };

    SchemaFactory* factory = SchemaFactory::get_instance();
    if (client == nullptr || client->query_ctx == nullptr || factory == nullptr) {
        DB_FATAL("param invalid");
        return false;
    }

    bool show_region = false;
    if (split_vec.size() == 4 && boost::iequals(split_vec[3], "region")) {
        show_region = true;
    }
    else if (split_vec.size() != 3) {
        DB_FATAL("param invalid");
        client->state = STATE_ERROR;
        return false;
    }
    const std::string& table_name = split_vec[2];
    int64_t table_id;
    std::string full_name = client->user_info->namespace_+ "." + client->current_db + "." + table_name;
    if (factory->get_table_id(full_name, table_id) != 0) {
        DB_FATAL("param invalid, no such table with table name: %s", full_name.c_str());
        client->state = STATE_ERROR;
        return false;
    }
    IndexInfo pri_info = factory->get_index_info(table_id);
    if (pri_info.id == -1) {
        DB_FATAL("param invalid, no such table with table name: %s", full_name.c_str());
        client->state = STATE_ERROR;
        return false;
    }
    // Make fields.
    std::vector<ResultField> fields;
    std::vector<std::string> names;
    names.reserve(10);
    fields.reserve(10);
    if (show_region) {
        names = {"index_id", "region_id", "status", "start_key", "end_key"};
    } else {
        names = {"op_type", "index_state", "index_id", "begin_timestamp",
                 "end_timestamp", "rollback", "errcode", "deleted", "status",
                 "suspend", "update_timestamp", "global"};
    }
    for(auto name : names) {
        ResultField field;
        field.name = name;
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    }

    // Make rows.
    std::vector< std::vector<std::string> > rows;
    rows.reserve(10);
    pb::QueryRequest request;
    pb::QueryResponse response;
    request.set_op_type(pb::QUERY_DDLWORK);
    request.set_table_id(table_id);
    MetaServerInteract::get_instance()->send_request("query", request, response);
    DB_WARNING("req:%s res:%s", request.ShortDebugString().c_str(), response.ShortDebugString().c_str());
    if (show_region) {
        for(auto info : response.region_ddl_infos()) {
            std::vector<std::string> row;
            row.reserve(6);
            row.emplace_back(std::to_string(info.index_id()));
            row.emplace_back(std::to_string(info.region_id()));
            row.emplace_back(state[info.status()]);
            if (info.start_key().size() > 0) {
                TableKey start_key(info.start_key());
                row.emplace_back(start_key.decode_start_key_string(pri_info));
            } else {
                row.emplace_back("-∞");
            }
            if (info.end_key().size() > 0) {
                TableKey end_key(info.end_key());
                row.emplace_back(end_key.decode_start_key_string(pri_info));
            } else {
                row.emplace_back("+∞");
            }
            rows.emplace_back(row);
        }
    } else {
        for (auto ddl : response.ddlwork_infos()) {
            std::vector<std::string> row;
            row.emplace_back(std::to_string(ddl.op_type()));
            row.emplace_back(ddl.has_job_state() ? index_state[ddl.job_state()] : "");
            row.emplace_back(std::to_string(ddl.index_id()));
            row.emplace_back(std::to_string(ddl.begin_timestamp()));
            row.emplace_back(std::to_string(ddl.end_timestamp()));
            row.emplace_back(std::to_string(ddl.rollback()));
            row.emplace_back(std::to_string(ddl.errcode()));
            row.emplace_back(std::to_string(ddl.deleted()));
            row.emplace_back(ddl.has_status() ? state[ddl.status()] : "");
            row.emplace_back(std::to_string(ddl.suspend()));
            row.emplace_back(std::to_string(ddl.update_timestamp()));
            row.emplace_back(std::to_string(ddl.global()));
            rows.emplace_back(row);
        }
    }

    // Make mysql packet.
    if (_make_common_resultset_packet(client, fields, rows) != 0) {
        DB_FATAL_CLIENT(client, "Failed to make result packet.");
        _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
        client->state = STATE_ERROR;
        return false;
    }
    client->state = STATE_READ_QUERY_RESULT;
    return true;
}

bool ShowHelper::_show_diff_region_size(const SmartSocket& client, const std::vector<std::string>& split_vec) {
    SchemaFactory* factory = SchemaFactory::get_instance();
    if (client == nullptr || client->query_ctx == nullptr || factory == nullptr) {
        DB_FATAL("param invalid");
        //client->state = STATE_ERROR;
        return false;
    }
    if (split_vec.size() < 3) {
        client->state = STATE_ERROR;
        return false;
    }
    bool ignore = false;
    double multiples = 1.5;
    if (split_vec.size() > 3) {
        multiples = strtod(split_vec[3].c_str(), NULL);
    }
    if (split_vec.size() > 4) {
        ignore = true;
    }
    int64_t table_id = strtoll(split_vec[2].c_str(), NULL, 10);
    DB_WARNING("table_id: %ld", table_id);

    // Make fields.
    std::vector<ResultField> fields;
    fields.reserve(2);
    do {
        ResultField field;
        field.name = "region_id";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);

    do {
        ResultField field;
        field.name = "lines";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);

    do {
        ResultField field;
        field.name = "peer:size";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);

    // Make rows.
    std::vector< std::vector<std::string> > rows;
    rows.reserve(10);

    std::map<std::string, pb::RegionInfo> region_infos;
    factory->get_all_region_by_table_id(table_id, &region_infos);
    std::map<std::string, std::vector<int64_t>> instance_regions;
    for (auto& pair : region_infos) {
        auto& info = pair.second;
        for (auto& ins : info.peers()) {
            instance_regions[ins].emplace_back(info.region_id());
        }
    }

    std::map<int64_t, std::vector<std::tuple<int64_t, std::string, int64_t>>> region_sizes;
    bthread::Mutex mutex;
    BthreadCond cond;
    bool err = false;
    for (auto& pair : instance_regions) {
        Bthread bth;
        cond.increase();
        bth.run([&cond, &mutex, &region_sizes, &err, pair, ignore] () {
            auto& store_addr = pair.first;
            pb::RegionIds req;
            for (auto& id : pair.second) {
                req.add_region_ids(id);
            }
            pb::StoreRes res;
            int retry = 0;
            while (++retry <= 3) {
                StoreInteract interact(store_addr);
                interact.send_request("query_region", req, res);
                if (res.errcode() == pb::SUCCESS) {
                    break;
                }
            }
            if (res.errcode() != pb::SUCCESS) {
                if (!ignore) {
                    cond.decrease_signal();
                    err = true;
                    return;
                }
            }
            std::unique_lock<bthread::Mutex> lck(mutex);
            for (auto& info : res.regions()) {
                region_sizes[info.region_id()].emplace_back(info.used_size(), store_addr, info.num_table_lines());
            }
            cond.decrease_signal();
        });
    }
    cond.wait();
    if (err) {
        DB_FATAL("error");
        client->state = STATE_ERROR;
        return false;
    }
    auto size_str = [](int64_t size) {
        std::ostringstream oss;
        if (size > 1024 * 1024) {
            oss << size / 1024 / 1024 << "m";
        } else if (size > 1024) {
            oss << size / 1024 << "k";
        } else {
            oss << size;
        }
        return oss.str();
    };
    for (auto& pair : region_sizes) {
        int64_t region_id = pair.first;
        int64_t first_size = 0;
        bool first = true;
        bool diff = false;
        for (auto pair2 : pair.second) {
            if (first) {
                first_size = std::get<0>(pair2);
                first = false;
            } else {
                if (first_size != 0) {
                    if (std::get<0>(pair2) * 1.0 / first_size > multiples || first_size * 1.0 / std::get<0>(pair2) > multiples) {
                        diff = true;
                    }
                } else {
                    if (std::get<0>(pair2) != 0) {
                        diff = true;
                    }
                }
            }
        }
        if (diff) {
            std::vector<std::string> row;
            row.reserve(3);
            row.emplace_back(std::to_string(region_id));
            std::ostringstream oss;
            int64_t lines = 0;
            for (auto pair2 : pair.second) {
                lines = std::get<2>(pair2);
                oss << std::get<1>(pair2) << ":" << size_str(std::get<0>(pair2)) << ",";
            }
            row.emplace_back(std::to_string(lines));
            row.emplace_back(oss.str());
            rows.emplace_back(row);
        }
    }

    // Make mysql packet.
    if (_make_common_resultset_packet(client, fields, rows) != 0) {
        DB_FATAL_CLIENT(client, "Failed to make result packet.");
        _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
        client->state = STATE_ERROR;
        return false;
    }
    client->state = STATE_READ_QUERY_RESULT;
    return true;
}

bool ShowHelper::_show_global_ddl_work(const SmartSocket& client, const std::vector<std::string>& split_vec) {
    SchemaFactory* factory = SchemaFactory::get_instance();
    if (client == nullptr || client->query_ctx == nullptr || factory == nullptr) {
        DB_FATAL("param invalid");
        return false;
    }

    if (split_vec.size() != 3) {
        client->state = STATE_ERROR;
        return false;
    }
    const std::string& table_name = split_vec[2];

    // Make fields.
    std::vector<ResultField> fields;
    std::vector<std::string> names = {"region_id", "start_key", "end_key", "status", "op_type", "index_id",
                                      "address", "retry_time", "update_timestamp", "partition"};
    std::unordered_map<pb::DdlWorkStatus, std::string, std::hash<int>> state = {
            {pb::DdlWorkIdle, "DdlWorkIdle"},
            {pb::DdlWorkDoing, "DdlWorkDoing"},
            {pb::DdlWorkDone, "DdlWorkDone"},
            {pb::DdlWorkFail, "DdlWorkFail"},
            {pb::DdlWorkDupUniq, "DdlWorkDupUniq"},
            {pb::DdlWorkError, "DdlWorkError"}
    };
    fields.reserve(10);
    for(auto name : names) {
        ResultField field;
        field.name = name;
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    }

    int64_t table_id;
    std::string full_name = client->user_info->namespace_+ "." + client->current_db + "." + table_name;
    if (factory->get_table_id(full_name, table_id) != 0) {
        DB_FATAL("param invalid, no such table with table name: %s", full_name.c_str());
        client->state = STATE_ERROR;
        return false;
    }

    // Make rows.
    std::vector< std::vector<std::string> > rows;
    rows.reserve(10);
    pb::QueryRequest request;
    pb::QueryResponse response;
    request.set_op_type(pb::QUERY_INDEX_DDL_WORK);
    request.set_table_id(table_id);
    MetaServerInteract::get_instance()->send_request("query", request, response);
    DB_WARNING("req:%s res:%s", request.ShortDebugString().c_str(), response.ShortDebugString().c_str());

    auto index_info = factory->get_index_info(table_id);
    for(auto& ddl : response.region_ddl_infos()) {
        std::vector<std::string> row;
        row.emplace_back(std::to_string(ddl.region_id()));
        if (ddl.start_key().size() > 0) {
            TableKey start_key(ddl.start_key());
            row.emplace_back(start_key.decode_start_key_string(index_info));
        } else {
            row.emplace_back("-∞");
        }
        if (ddl.end_key().size() > 0) {
            TableKey end_key(ddl.end_key());
            row.emplace_back(end_key.decode_start_key_string(index_info));
        } else {
            row.emplace_back("+∞");
        }
        row.emplace_back(ddl.has_status() ? state[ddl.status()] : "");
        row.emplace_back(ddl.has_op_type() ? std::to_string(ddl.op_type()) : "");
        row.emplace_back(ddl.has_index_id() ? std::to_string(ddl.index_id()) : "");
        row.emplace_back(ddl.has_address() ? ddl.address() : "");
        row.emplace_back(ddl.has_retry_time() ? std::to_string(ddl.retry_time()) : "");
        row.emplace_back(ddl.has_update_timestamp() ? std::to_string(ddl.update_timestamp()) : "");
        row.emplace_back(ddl.has_partition() ? std::to_string(ddl.partition()) : "");
        rows.emplace_back(row);
    }

    // Make mysql packet.
    if (_make_common_resultset_packet(client, fields, rows) != 0) {
        DB_FATAL_CLIENT(client, "Failed to make result packet.");
        _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
        client->state = STATE_ERROR;
        return false;
    }
    client->state = STATE_READ_QUERY_RESULT;
    return true;
}

bool ShowHelper::_show_network_segment(const SmartSocket& client, const std::vector<std::string>& split_vec) {
    if (client == nullptr || client->query_ctx == nullptr) {
        DB_FATAL("param invalid");
        //client->state = STATE_ERROR;
        return false;
    }
   
    std::string resource_tag;
    if (split_vec.size() == 3) {
        resource_tag = split_vec[2];
    } else if (split_vec.size() != 2) {
        client->state = STATE_ERROR;
        return false; 
    }
    
    std::vector<ResultField> fields;
    fields.reserve(3);
    do {
        ResultField field;
        field.name = "resource tag";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "network segment";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    do {
        ResultField field;
        field.name = "instance address";
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);
    
    // Make rows.
    std::vector< std::vector<std::string> > rows;
    rows.reserve(10);
    pb::QueryRequest request;
    pb::QueryResponse response;
    request.set_op_type(pb::QUERY_NETWORK_SEGMENT);
    request.set_resource_tag(resource_tag);
    MetaServerInteract::get_instance()->send_request("query", request, response);
    DB_WARNING("req:%s res:%s", request.ShortDebugString().c_str(), response.ShortDebugString().c_str());
    
    for (auto& info : response.instance_infos()) {
        std::vector<std::string> row = {info.resource_tag(), info.network_segment(), info.address()};
        rows.emplace_back(row);
    }
    // Make mysql packet.
    if (_make_common_resultset_packet(client, fields, rows) != 0) {
        DB_FATAL_CLIENT(client, "Failed to make result packet.");
        _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
        client->state = STATE_ERROR;
        return false;
    }
    client->state = STATE_READ_QUERY_RESULT;
    return true;
}
    
bool ShowHelper::_show_switch(const SmartSocket& client, const std::vector<std::string>& split_vec) {
    if (client == nullptr || client->query_ctx == nullptr) {
        DB_FATAL("param invalid");
        //client->state = STATE_ERROR;
        return false;
    }
    
    std::string resource_tag;
    if (split_vec.size() == 3) {
        resource_tag = split_vec[2];
    } else if (split_vec.size() != 2) {
        client->state = STATE_ERROR;
        return false;
    }
    
    std::vector<ResultField> fields;
    fields.reserve(4);
    std::vector<std::string> names = {"resource tag", "peer load balance", "migrate balance", "network segment balance"};
    for(auto& name : names) {
        ResultField field;
        field.name = name;
        field.type = MYSQL_TYPE_VARCHAR;
        field.length = 1024;
        fields.emplace_back(field);
    }
    
    // Make rows.
    std::vector< std::vector<std::string> > rows;
    rows.reserve(10);
    pb::QueryRequest request;
    pb::QueryResponse response;
    request.set_op_type(pb::QUERY_RESOURCE_TAG_SWITCH);
    request.set_resource_tag(resource_tag);
    MetaServerInteract::get_instance()->send_request("query", request, response);
    DB_WARNING("req:%s res:%s", request.ShortDebugString().c_str(), response.ShortDebugString().c_str());
    for (auto& info : response.resource_tag_infos()) {
        std::vector<std::string> row = {info.resource_tag(), 
                                        info.peer_load_balance() ? "true" : "false", 
                                        info.migrate() ? "true" : "false",
                                        info.network_segment_balance() ? "true" : "false"};
        rows.emplace_back(row);
    }
    // Make mysql packet.
    if (_make_common_resultset_packet(client, fields, rows) != 0) {
        DB_FATAL_CLIENT(client, "Failed to make result packet.");
        _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
        client->state = STATE_ERROR;
        return false;
    }
    client->state = STATE_READ_QUERY_RESULT;
    return true;
}


bool ShowHelper::_handle_client_query_template_dispatch(const SmartSocket& client, const std::vector<std::string>& split_vec) {
    if (boost::iequals(split_vec[1], "meta")) {
        return _handle_client_query_template(client, "Meta",
                                             MYSQL_TYPE_VARCHAR, {FLAGS_meta_server_bns});
    } else if (boost::iequals(split_vec[1], "namespace")) {
        return _handle_client_query_template(client, "Namespace", MYSQL_TYPE_VARCHAR,
                                             {client->user_info->namespace_});
    } else if (boost::iequals(split_vec[1], "disable")) {
        std::vector<std::string> indexs;
        indexs.reserve(10);
        if (SchemaFactory::get_instance()->get_disable_indexs(indexs) != 0) {
            DB_WARNING("get disable index error.");
        }
        return _handle_client_query_template(client, "Disable Indexs", MYSQL_TYPE_VARCHAR,
                                      indexs);
    }
    return false;
}

bool ShowHelper::_handle_client_query_template(const SmartSocket& client,
        const std::string& field_name, int32_t data_type, const std::vector<std::string>& values) {
    if (client == nullptr) {
        DB_FATAL("param invalid");
        //client->state = STATE_ERROR;
        return false;
    }

    // Make fields.
    std::vector<ResultField> fields;
    fields.reserve(1);
    do {
        ResultField field;
        field.name = field_name.c_str();
        field.type = data_type;
        field.length = 1024;
        fields.emplace_back(field);
    } while (0);

    // make rows
    std::vector<std::vector<std::string> > rows;
    rows.reserve(10);
    for (const auto& value  : values) {
        std::vector<std::string> row;
        row.reserve(1);
        row.emplace_back(value);
        rows.emplace_back(row);
    }

    // Make mysql packet.
    if (_make_common_resultset_packet(client, fields, rows) != 0) {
        DB_FATAL_CLIENT(client, "Failed to make result packet.");
        _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
        client->state = STATE_ERROR;
        return false;
    }
    client->state = STATE_READ_QUERY_RESULT;
    return true;
}

int ShowHelper::_make_common_resultset_packet(const SmartSocket& sock,
        std::vector<ResultField>& fields,
        std::vector< std::vector<std::string> >& rows) {
    if (!sock || !sock->send_buf) {
        DB_FATAL("sock == NULL.");
        return RET_ERROR;
    }
    if (fields.size() == 0) {
        DB_FATAL("Field size is 0.");
        return RET_ERROR;
    }

    //Result Set Header Packet
    int start_pos = sock->send_buf->_size;
    if (!sock->send_buf->byte_array_append_len((const uint8_t *)"\x01\x00\x00\x01", 4)) {
        DB_FATAL("byte_array_append_len failed.");
        return RET_ERROR;
    }
    if (!sock->send_buf->byte_array_append_length_coded_binary(fields.size())) {
        DB_FATAL("byte_array_append_len failed. len:[%lu]", fields.size());
        return RET_ERROR;
    }
    int packet_body_len = sock->send_buf->_size - start_pos - 4;
    sock->send_buf->_data[start_pos] = packet_body_len & 0xFF;
    sock->send_buf->_data[start_pos + 1] = (packet_body_len >> 8) & 0xFF;
    sock->send_buf->_data[start_pos + 2] = (packet_body_len >> 16) & 0xFF;
    sock->send_buf->_data[start_pos + 3] = (++sock->packet_id) & 0xFF;
    // Make field packets
    for (uint32_t cnt = 0; cnt < fields.size(); ++cnt) {
        fields[cnt].catalog = "baikal";
        if (sock->query_ctx != nullptr) {
            fields[cnt].db = sock->query_ctx->cur_db;
        }
        fields[cnt].table.clear();
        fields[cnt].org_table.clear();
        fields[cnt].org_name = fields[cnt].name;
        _wrapper->make_field_packet(sock->send_buf, &fields[cnt], ++sock->packet_id);
    }

    // Make EOF packet
    _wrapper->make_eof_packet(sock->send_buf, ++sock->packet_id);

    // Make row packets
    for (uint32_t cnt = 0; cnt < rows.size(); ++cnt) {
        // Make row data packet
        if (!_wrapper->make_row_packet(sock->send_buf, rows[cnt], ++sock->packet_id)) {
            DB_FATAL("make_row_packet failed");
            return RET_ERROR;
        }
    }
    // Make EOF packet
    _wrapper->make_eof_packet(sock->send_buf, ++sock->packet_id);
    return 0;
}

void ShowHelper::_parse_sample_sql(std::string sample_sql, std::string& database, std::string& table, std::string& sql) {
    // Remove comments.
    re2::RE2::Options option;
    option.set_utf8(false);
    option.set_case_sensitive(false);
    option.set_perl_classes(true);

    re2::RE2 reg("family_table_tag_optype_plat=\\[(.*)\t(.*)\t.*\t.*\t.*sql=\\[(.*)\\]", option);

    if (!RE2::Extract(sample_sql, reg, "\\1", &database)) {
        DB_WARNING("extract commit error.");
    }
    if (!RE2::Extract(sample_sql, reg, "\\2", &table)) {
        DB_WARNING("extract commit error.");
    }
    if (!RE2::Extract(sample_sql, reg, "\\3", &sql)) {
        DB_WARNING("extract commit error.");
    }

    DB_WARNING("sample_sql: %s, database: %s, table: %s, sql: %s", sample_sql.c_str(), database.c_str(), table.c_str(), sql.c_str());
}
}
