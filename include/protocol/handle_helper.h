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

// handle abnormal_region
const std::string SQL_HANDLE_ABNORMAL_REGIONS           = "abnormal";
// handle add_privilege tableName write
const std::string SQL_HANDLE_ADD_PRIVILEGE              = "add_privilege";
// handle table_resource_tag tableName resourceTag
const std::string SQL_HANDLE_TABLE_RESOURCE_TAG         = "table_resource_tag";
// handle load_balance resourceTag open/close
const std::string SQL_HANDLE_LOAD_BALANCE_SWITCH        = "load_balance";
// handle migrate resourceTag open/close
const std::string SQL_HANDLE_MIGRATE_SWITCH             = "migrate";
// handle drop_instance physicalRoom address
const std::string SQL_HANDLE_DROP_INSTANCE              = "drop_instance";
// handle main_logical_room tableName MainLogicalRoom
const std::string SQL_HANDLE_TABLE_MAIN_LOGICAL_ROOM    = "main_logical_room";
// handle drop_region regionID
const std::string SQL_HANDLE_DROP_REGION                = "meta_drop_region";
// handle split_lines tableName splitLines
const std::string SQL_HANDLE_SPLIT_LINES                = "split_lines";
// handle ttl_duration tableName ttl
const std::string SQL_HANDLE_TTL_DURATION               = "ttl_duration";
// handle split_region tableID regionId
const std::string SQL_HANDLE_SPLIT_REGION               = "split_region";
// handle rm_privilege dbname tableName
const std::string SQL_HANDLE_RM_PRIVILEGE               = "rm_privilege";
// handle delete_ddl tableID
const std::string SQL_HANDLE_DELETE_DDL                 = "delete_ddl";
// handle suspend_ddl tableID
const std::string SQL_HANDLE_SUSPEND_DDL                = "suspend_ddl";
// handle restart_ddl tableID
const std::string SQL_HANDLE_RESTART_DDL                = "restart_ddl";
// handle update_dists tableID json
const std::string SQL_HANDLE_UPDATE_DISTS               = "update_dists";
// handle store_rm_region storeAddress regionID (delay_drop) (force)
const std::string SQL_HANDLE_STORE_RM_REGION            = "store_rm_region";
// handle store_add_peer regionID address
const std::string SQL_HANDLE_STORE_ADD_PEER             = "store_add_peer";
// handle store_rm_peer regionID address
const std::string SQL_HANDLE_STORE_RM_PEER              = "store_rm_peer";
// handle store_set_peer regionID address
const std::string SQL_HANDLE_STORE_SET_PEER             = "store_set_peer";
// handle store_trans_leader regionID transLeaderAddress
const std::string SQL_HANDLE_STORE_TRANS_LEADER         = "store_trans_leader";
// handle add_user namespace username password json
const std::string SQL_HANDLE_ADD_USER                   = "add_user";
// handle copy_db db1 db2 
const std::string SQL_HANDLE_COPY_DB                    = "copy_db";
// handle link_binlog json
const std::string SQL_HANDLE_LINK_BINLOG                = "link_binlog";
// handle unlink_binlog json
const std::string SQL_HANDLE_UNLINK_BINLOG              = "unlink_binlog";
// handle instance_param resourceTagOrAddress json
const std::string SQL_HANDLE_INSTANCE_PARAM             = "instance_param";
// handle schema_conf tableName key value(bool)
const std::string SQL_HANDLE_SCHEMA_CONF                = "schema_conf";
// handle migrate_instance InstanceAddress
const std::string SQL_HANDLE_MIGRATE_INSTANCE           = "migrate_instance";
// handle instance_status InstanceAddress status
const std::string SQL_HANDLE_INSTANCE_STATUS            = "instance_status";
// handle store_compact_region RegionID compact_raft_log(true)
const std::string SQL_STORE_COMPACT_REGION              = "store_compact_region";
// handle create_namespace NamespaceName
const std::string SQL_HANDLE_CREATE_NAMESPACE           = "create_namespace";
// handle network_balance open/close
const std::string SQL_HANDLE_NETWORK_BALANCE            = "network_balance";
// handle store_rm_txn storeAddress regionID regionVersion txnID
const std::string SQL_HANDLE_STORE_RM_TXN               = "store_rm_txn";
// handle region_adjustkey tableID regionID start_key_region_id end_key_region_id 
const std::string SQL_HANDLE_REGION_ADJUSTKEY           = "region_adjustkey";
const std::string SQL_HANDLE_MODIFY_PARTITION           = "modify_partition";
// HANDLE specify_split_keys dbName tableName [split_key1 split_key2]
const std::string SQL_HANDLE_SPECIFY_SPLIT_KEYS         = "specify_split_keys";
// handle convert_partition dbName tableName primary_range_partition_type [gen_range_partition_types]
const std::string SQL_HADNLE_CONVERT_PARTITION          = "convert_partition";
// handle offline_binlog tableid regionid oldest_ts newest_ts
const std::string SQL_HANDLE_OFFLINE_BINLOG             = "offline_binlog";
// handle link_external_sst dbName tableName partitionName
const std::string SQL_HADNLE_LINK_EXTERNAL_SST          = "link_external_sst";

namespace baikaldb {
typedef std::shared_ptr<NetworkSocket> SmartSocket;
class HandleHelper {
public:
    static HandleHelper* get_instance() {
        static HandleHelper _instance;
        return &_instance;
    }

    void init();
    bool execute(const SmartSocket& client);

private:
    HandleHelper() {
    }

    virtual ~HandleHelper() {
    }

    // sql: handle abnormal_regions
    bool _handle_abnormal_regions(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: handle add_privilege
    bool _handle_add_privilege(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: handle table_resource_tag tableName newResourceTag
    bool _handle_table_resource_tag(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: handle load_balance tableName open/close
    bool _handle_load_balance_switch(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: handle drop_instance physicalRoom address
    bool _handle_drop_instance(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: handle main_logical_room tbname MainLogicalRoom
    bool _handle_table_main_logical_room(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // sql: handle drop_region regionID
    bool _handle_drop_region(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // handle split_lines tbname splitLines
    bool _handle_split_lines(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // handle ttl_duration tbname ttl
    bool _handle_ttl_duration(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // handle split_region tableID regionId
    bool _handle_split_region(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // handle rm_privilege dbname tbname
    bool _handle_rm_privilege(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // handle modify_partition dbname tbname
    bool _handle_modify_partition(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // handle delete_ddl tableName
    // handle suspend_ddl tableName
    // handle restart_ddl tableName
    bool _handle_ddlwork(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // handle update_dists json
    bool _handle_update_dists(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // handle store_rm_region storeAddress regionID (no_delay)  (no_force)
    bool _handle_store_rm_region(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // handle store_add_peer regionID address
    bool _handle_store_add_peer(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // handle store_rm_peer regionID address
    bool _handle_store_rm_peer(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // handle store_set_peer regionID address
    bool _handle_store_set_peer(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // handle store_trans_leader regionID transLeaderAddress
    bool _handle_store_trans_leader(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // handle add_user namespace username password json
    bool _handle_add_user(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // handle copy_db db1 db2 
    bool _handle_copy_db(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // handle link_binlog/unlink_binlog db table binlog_db binlog_table
    bool _handle_binlog(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // handle link_binlog/unlink_binlog db table binlog_db binlog_table
    bool _handle_instance_param(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // handle schema_conf tableName key value(bool)
    bool _handle_schema_conf(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // handle migrate_instance InstanceAddress
    bool _handle_migrate_instance(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // handle instance_status InstanceAddress status
    bool _handle_instance_status(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // handle store_compact_region type RegionID
    bool _handle_store_compact_region(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // handle store_rm_txn storeAddress regionID regionVersion txnID
    bool _handle_store_rm_txn(const SmartSocket& client, const std::vector<std::string>& split_vec);
    bool _handle_region_adjustkey(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // handle create_namespace NamespaceName
    bool _handle_create_namespace(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // handle specify_split_keys dbName tableName [split_key1 split_key2]
    bool _handle_specify_split_keys(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // handle convert_partition dbName tableName primary_range_partition_type [gen_range_partition_types]
    bool _handle_convert_partition(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // handle offline_binlog table_id region_id oldest_ts newest_ts
    bool _handle_offline_binlog(const SmartSocket& client, const std::vector<std::string>& split_vec);
    // handle link_external_sst dbName tableName partitionName
    bool _handle_link_external_sst(const SmartSocket& client, const std::vector<std::string>& split_vec);

    bool _send_store_raft_control_request(const SmartSocket& client, pb::RaftControlRequest& req, pb::RegionInfo& info);
    bool _make_response_packet(const SmartSocket& client, const std::string& response);
    void _make_handle_region_result_rows(const pb::MetaManagerRequest& request, 
        const pb::MetaManagerResponse& response, 
        std::vector<std::vector<std::string>>& rows);

    bool is_packet_learner_result_success(const SmartSocket& client, 
        const pb::QueryRequest& query_req, 
        pb::QueryResponse& query_res);

    bool is_packet_region_result_success(const SmartSocket& client, 
        const pb::MetaManagerRequest& request, 
        pb::MetaManagerResponse& response);

    void update_unhealthy_learners_schema(const pb::QueryRequest& query_req, 
        pb::QueryResponse& query_res,
        std::vector<std::vector<std::string>>& rows);
        
    int _make_common_resultset_packet(const SmartSocket& sock,
            std::vector<ResultField>& fields,
            std::vector< std::vector<std::string> >& rows);

    // Key -> function
    std::unordered_map<std::string, std::function<
                bool(const SmartSocket& client, const std::vector<std::string>& split_vec)>
            > _calls;
    MysqlWrapper*   _wrapper = nullptr;
};
}
