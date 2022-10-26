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

#pragma once

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include "common.h"
#include "network_socket.h"
#include "meta_server_interact.hpp"
#include "mysql_wrapper.h"
#include "proto/store.interface.pb.h"

const std::string SQL_SHOW_ABNORMAL_REGIONS      = "abnormal";              // show abnormal regions;
const std::string SQL_SHOW_CREATE_TABLE          = "create";                // show create table test;
const std::string SQL_SHOW_COLLATION             = "collation";             // show collation;
const std::string SQL_SHOW_DATABASES             = "databases";             // show databases;
const std::string SQL_SHOW_NAMESPACE             = "namespace";             // show namespace
const std::string SQL_SHOW_META                  = "meta";                  // show meta;
const std::string SQL_SHOW_TABLE_STATUS          = "table";                 // show table status;
const std::string SQL_SHOW_TABLES                = "tables";                // show tables;
const std::string SQL_SHOW_SOCKET                = "socket";                // show socket;
const std::string SQL_SHOW_WARNINGS              = "warnings";              // show warnings;
const std::string SQL_SHOW_PROCESSLIST           = "processlist";           // show processlist;
const std::string SQL_SHOW_COST                  = "cost";                  // show cost switch;
const std::string SQL_SHOW_FULL_TABLES           = "full_tables";           // show full tables;
const std::string SQL_SHOW_FULL_COLUMNS          = "full_columns";          // show full columns;
const std::string SQL_SHOW_SCHEMA_CONF           = "schema_conf";           // show schema_conf database_table;
const std::string SQL_SHOW_VIRTUAL_INDEX         = "virtual";               // show virtual index;
const std::string SQL_SHOW_SESSION_VARIABLES     = "show session variables";
const std::string SQL_DISABLE_INDEXS             = "disable";               // show disable indexs;
const std::string SQL_SHOW_REGION                = "region";                // show region tableID regionID
const std::string SQL_SHOW_STORE_REGION          = "store";                 // show store region storeAddress regionID
const std::string SQL_SHOW_VARIABLES             = "variables";             // show variables;
const std::string SQL_SHOW_STATUS                = "status";                // show status; same as `show variables`
const std::string SQL_SHOW_USER                  = "user";                  // show user username;
const std::string SQL_SHOW_STORE_TXN             = "store_txn";             // show store_txn storeAddress regionID;
const std::string SQL_SHOW_DDL_WORK              = "ddlwork";               // show ddlwork tableID;
const std::string SQL_SHOW_GLOBAL_DDL_WORK       = "global_ddlwork";        // show global_ddlwork tableID;
const std::string SQL_SHOW_PRIVILEGE             = "privilege";             // show privilege username;
const std::string SQL_SHOW_DIFF_REGION_SIZE      = "diff_region_size";      // show diff_region_size tableID
const std::string SQL_SHOW_NETWORK_SEGMENT       = "network_segment";       // show network_segment resourceTag;
const std::string SQL_SHOW_SWITCH                = "switch";                // show switch
const std::string SQL_SHOW_ALL_TABLES            = "all_tables";            // show all_tables [ttl/binlog/...]
const std::string SQL_SHOW_BINLOGS_INFO          = "binlogs_info";          // show binlogs_info
const std::string SQL_SHOW_INSTANCE_PARAM        = "instance_param";        // show instance_param [resource_tag/instance]
const std::string SQL_SHOW_ENGINES               = "engines";               // show engines
const std::string SQL_SHOW_CHARSET               = "charset";               // show charset
const std::string SQL_SHOW_CHARACTER_SET         = "character";             // show character set; same as `show charset`
const std::string SQL_SHOW_INDEX                 = "index";                 // show index
const std::string SQL_SHOW_INDEXES               = "indexes";               // show indexes; same as `show index`
const std::string SQL_SHOW_KEYS                  = "keys";                  // show keys; same as `show index`

namespace baikaldb {
typedef std::shared_ptr<NetworkSocket> SmartSocket;
class ShowHelper {
public:
    static ShowHelper* get_instance() {
        static ShowHelper _instance;
        return &_instance;
    }

    void init();
    bool execute(const SmartSocket& client);
    bool _handle_client_query_template(const SmartSocket& client, const std::string& field_name, int32_t data_type,
                                       const std::vector<std::string>& values);
private:
    ShowHelper() {
    }

    virtual ~ShowHelper() {
    }

    // Key -> function
    std::unordered_map<std::string, std::function<
            bool(const SmartSocket& client, const std::vector<std::string>& split_vec)>
    > _calls;

    // sql: show abnormal regions [ resource_tag ];
    bool _show_abnormal_regions(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: show databases;
    bool _show_databases(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: show tables;
    bool _show_tables(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: show create table tableName;
    bool _show_create_table(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: show collation
    bool _show_collation(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: show socket
    bool _show_socket(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: show warnings;
    bool _show_warnings(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: show processlist;
    bool _show_processlist(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: show cost switch;
    bool _show_cost(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: show full tables;
    bool _show_full_tables(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: show full columns from tableName;
    bool _show_full_columns(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: show schema_conf database_table;
    bool _show_schema_conf(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: show all_tables ttl/binlog; 
    bool _show_all_tables(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: show binlogs_info;
    bool _show_binlogs_info(const SmartSocket& client, const std::vector<std::string>& split_params);
    // sql: show table status;
    bool _show_table_status(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: show virtual index
    bool _show_virtual_index(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: show region tableID regionID;
    bool _show_region(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: show store region storeAddress regionID;
    bool _show_store_region(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: show variables;
    bool _show_variables(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: show privilege username;
    bool _show_privilege(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: show store_txn storeAddress regionID;
    bool _show_store_txn(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: show ddlwork tableID;
    bool _show_ddl_work(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: show global_ddlwork tableID;
    bool _show_global_ddl_work(const SmartSocket& client, const std::vector<std::string>& split_vec);
    bool _show_diff_region_size(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: show user username
    bool _show_user(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: show network ResourceTag;
    bool _show_network_segment(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: show switch
    bool _show_switch(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: show instance_param ResourceTag / Instance
    bool _show_instance_param(const SmartSocket& client, const std::vector<std::string>& split_vec);
    bool _show_engines(const SmartSocket& client, const std::vector<std::string>& split_vec);
    bool _show_charset(const SmartSocket& client, const std::vector<std::string>& split_vec);
    bool _show_index(const SmartSocket& client, const std::vector<std::string>& split_vec);
    bool _handle_client_query_template_dispatch(const SmartSocket& client, const std::vector<std::string>& split_vec);
    int _make_common_resultset_packet(const SmartSocket& sock, 
            std::vector<ResultField>& fields,
            const std::vector<std::vector<std::string>>& rows);
    void _parse_sample_sql(std::string sample_sql, std::string& database, std::string& table, std::string& sql);

    void _query_regions_concurrent(std::unordered_map<int64_t, std::unordered_map<int64_t, std::vector<pb::StoreRes>>>& table_id_to_binlog_info, 
        std::unordered_map<int64_t, std::unordered_map<int64_t, std::vector<pb::RegionInfo>>>& partition_binlog_region_infos);

    bool _process_partition_binlogs_info(const SmartSocket& client, std::unordered_map<int64_t, 
        std::unordered_map<int64_t, std::vector<pb::StoreRes>>>& table_id_to_query_info);

    bool _process_binlogs_info(const SmartSocket& client, std::unordered_map<int64_t, 
        std::unordered_map<int64_t, std::vector<pb::StoreRes>>>& table_id_to_query_info);

    inline ResultField make_result_field(const std::string& name, const uint8_t& type, const uint32_t& length) const {
        ResultField field;
        field.name = name;
        field.type = type;
        field.length = length;
        return field;
    }
    MysqlWrapper*   _wrapper = nullptr;

    // 由于show table status太重了，进行cache
    bthread::Mutex _mutex;
    std::unordered_map<std::string, int64_t> _table_info_cache_time;
    // db -> rows
    std::unordered_map<std::string, std::vector<std::vector<std::string>>> _table_info_cache;
};
}

