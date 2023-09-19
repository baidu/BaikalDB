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

#include "schema_manager.h"
#include <boost/algorithm/string.hpp>
#include "cluster_manager.h"
#include "table_manager.h"
#include "database_manager.h"
#include "namespace_manager.h"
#include "region_manager.h"
#include "ddl_manager.h"
#include "meta_util.h"
#include "rocks_wrapper.h"
#include "query_table_manager.h"

namespace baikaldb {

const std::string SchemaManager::MAX_NAMESPACE_ID_KEY = "max_namespace_id";
const std::string SchemaManager::MAX_DATABASE_ID_KEY = "max_database_id";
const std::string SchemaManager::MAX_TABLE_ID_KEY = "max_table_id";
const std::string SchemaManager::MAX_REGION_ID_KEY = "max_region_id";

/*
 *  该方法除了service层调用之外，schema自身也需要调用
 *  自身调用是response为NULL
 *  目前自身调用的操作为：OP_UPDATE_REGION OP_DROP_REGION
 */
void SchemaManager::process_schema_info(google::protobuf::RpcController* controller,
                                        const pb::MetaManagerRequest* request,
                                        pb::MetaManagerResponse* response,
                                        google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!_meta_state_machine->is_leader()) {
        if (response) {
            response->set_errcode(pb::NOT_LEADER);
            response->set_errmsg("not leader");
            response->set_leader(butil::endpoint2str(_meta_state_machine->get_leader()).c_str());
        }
        DB_WARNING("meta state machine is not leader, request: %s", request->ShortDebugString().c_str());
        return;
    }
    uint64_t log_id = 0;
    brpc::Controller* cntl = NULL;
    if (controller != NULL) {
        cntl = static_cast<brpc::Controller*>(controller);
        if (cntl->has_log_id()) {
            log_id = cntl->log_id();
        }
    }
    ON_SCOPE_EXIT(([cntl, log_id, response]() {
        if (response != nullptr && response->errcode() != pb::SUCCESS) {
            const auto& remote_side_tmp = butil::endpoint2str(cntl->remote_side());
            const char* remote_side = remote_side_tmp.c_str();
            DB_WARNING("response error, remote_side:%s, log_id:%lu", remote_side, log_id);
        }
    }));
    switch (request->op_type()) {
    case pb::OP_CREATE_NAMESPACE:
    case pb::OP_MODIFY_NAMESPACE:
    case pb::OP_DROP_NAMESPACE: {
        if (!request->has_namespace_info()) { 
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, 
                    "no namespace_info", request->op_type(), log_id);
            return;
        } 
        // if (request->op_type() == pb::OP_MODIFY_NAMESPACE
        //         && !request->namespace_info().has_quota()) {
        //     ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, 
        //             "no namespace_quota", request->op_type(), log_id);
        //     return;
        // }
        _meta_state_machine->process(controller, request, response, done_guard.release());
        return;
    }
    case pb::OP_CREATE_DATABASE:
    case pb::OP_MODIFY_DATABASE:
    case pb::OP_DROP_DATABASE:{
        if (!request->has_database_info()) { 
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, 
                    "no database_info", request->op_type(), log_id);
            return;
        } 
        // if (request->op_type() == pb::OP_MODIFY_DATABASE
        //         && !request->database_info().has_quota()) {
        //     ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, 
        //             "no databasepace quota", request->op_type(), log_id);
        //     return;
        // }
        _meta_state_machine->process(controller, request, response, done_guard.release());
        return;
    }
    case pb::OP_CREATE_TABLE:
    case pb::OP_RENAME_TABLE:
    case pb::OP_SWAP_TABLE:
    case pb::OP_DROP_TABLE: 
    case pb::OP_DROP_TABLE_TOMBSTONE: 
    case pb::OP_RESTORE_TABLE: 
    case pb::OP_ADD_FIELD:
    case pb::OP_DROP_FIELD:
    case pb::OP_MODIFY_FIELD:
    case pb::OP_RENAME_FIELD:
    case pb::OP_UPDATE_BYTE_SIZE: 
    case pb::OP_UPDATE_SPLIT_LINES: 
    case pb::OP_UPDATE_SCHEMA_CONF: 
    case pb::OP_UPDATE_DISTS:
    case pb::OP_UPDATE_TTL_DURATION:
    case pb::OP_MODIFY_RESOURCE_TAG: 
    case pb::OP_ADD_INDEX:
    case pb::OP_DROP_INDEX: 
    case pb::OP_ADD_LEARNER:
    case pb::OP_DROP_LEARNER:
    case pb::OP_LINK_BINLOG:
    case pb::OP_UNLINK_BINLOG:
    case pb::OP_SET_INDEX_HINT_STATUS:
    case pb::OP_UPDATE_MAIN_LOGICAL_ROOM:
    case pb::OP_UPDATE_TABLE_COMMENT:
    case pb::OP_ADD_PARTITION:
    case pb::OP_DROP_PARTITION:
    case pb::OP_MODIFY_PARTITION:
    case pb::OP_CONVERT_PARTITION:
    case pb::OP_UPDATE_DYNAMIC_PARTITION_ATTR:
    case pb::OP_UPDATE_CHARSET:
    case pb::OP_SPECIFY_SPLIT_KEYS: {
        if (!request->has_table_info()) { 
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, 
                    "no schema_info", request->op_type(), log_id);
            return;
        }
        if (request->op_type() == pb::OP_UPDATE_BYTE_SIZE
                && !request->table_info().has_byte_size_per_record()) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                    "no bye_size_per_record", request->op_type(), log_id);
            return;
        }
        if (request->op_type() == pb::OP_UPDATE_SPLIT_LINES
                && !request->table_info().has_region_split_lines()) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                    "no region_split_lines", request->op_type(), log_id);
            return;
        }      
        if (request->op_type() == pb::OP_UPDATE_SCHEMA_CONF
                && !request->table_info().has_schema_conf()) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                    "no schema_conf", request->op_type(), log_id);
            return;
        }
        if (request->op_type() == pb::OP_UPDATE_TABLE_COMMENT
                && !request->table_info().has_comment()) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                    "no table comment", request->op_type(), log_id);
            return;
        }
        if ((request->op_type() == pb::OP_RENAME_TABLE || request->op_type() == pb::OP_SWAP_TABLE)
                && !request->table_info().has_new_table_name()) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                    "no new table name", request->op_type(), log_id);
            return;
        }
        if (request->op_type() == pb::OP_CREATE_TABLE
                && !request->table_info().has_upper_table_name()) {
            auto ret = pre_process_for_create_table(request, response, log_id);
            if (ret < 0) {
                return;
            }
        }
        if (is_table_info_op_type(request->op_type())) {
            std::string input_database = request->table_info().database();
            boost::trim(input_database);
            std::string input_namespace_name = request->table_info().namespace_name();
            boost::trim(input_namespace_name);
            std::string key = input_namespace_name + "." + input_database;
            QueryTableManager::get_instance()->erase_cache(key);
            DB_WARNING("erase table info cache key:%s", key.c_str());
        }
        if (request->op_type() == pb::OP_UPDATE_DISTS) {
            auto mutable_request = const_cast<pb::MetaManagerRequest*>(request);
            std::string main_logical_room;
            auto ret = whether_dists_legal(mutable_request, response, main_logical_room, log_id);
            if (ret < 0) {
                return;
            } 
        }
        if (request->op_type() == pb::OP_UPDATE_MAIN_LOGICAL_ROOM) {
            auto mutable_request = const_cast<pb::MetaManagerRequest*>(request);
            auto ret = whether_main_logical_room_legal(mutable_request, response, log_id);
            if (ret < 0) {
                return;
            } 
        }
        if (request->op_type() == pb::OP_ADD_INDEX || request->op_type() == pb::OP_DROP_INDEX
            || request->op_type() == pb::OP_MODIFY_FIELD) {
            if (request->has_ddlwork_info()) {
                auto mutable_request = const_cast<pb::MetaManagerRequest*>(request);
                mutable_request->mutable_ddlwork_info()->set_begin_timestamp(butil::gettimeofday_s());
            }
        }
        if (request->op_type() == pb::OP_UPDATE_TTL_DURATION 
                && !request->table_info().has_ttl_duration()) {
            // 只能修改有ttl的表
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                    "ttl_duration must > 0", request->op_type(), log_id);
            return;
        }
        if ((request->op_type() == pb::OP_ADD_PARTITION || request->op_type() == pb::OP_DROP_PARTITION ||
             request->op_type() == pb::OP_MODIFY_PARTITION || request->op_type() == pb::OP_UPDATE_DYNAMIC_PARTITION_ATTR) && 
             !request->table_info().has_partition_info()) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                    "no partition_info", request->op_type(), log_id);
            return;
        }
        if (request->op_type() == pb::OP_UPDATE_CHARSET 
                && !request->table_info().has_charset()) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                    "no charset", request->op_type(), log_id);
            return;
        }
        _meta_state_machine->process(controller, request, response, done_guard.release());
        return;
    }
    case pb::OP_DROP_REGION: {
        if (!request->drop_region_ids_size()) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                    "no alter table info", request->op_type(), log_id);
            return;
        }
        _meta_state_machine->process(controller, request, response, done_guard.release());
        return;
    } 
    case pb::OP_UPDATE_REGION: {
        if (!request->has_region_info() && request->region_infos().size() == 0) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                    "no split region info", request->op_type(), log_id);
            return;
        }
        _meta_state_machine->process(controller, request, response, done_guard.release());
        return;
    }
    case pb::OP_SPLIT_REGION: {
        if (!request->has_region_split()) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                    "no split region info", request->op_type(), log_id);
            return;
        }
        auto ret = pre_process_for_split_region(request, response, log_id);
        if (ret < 0) {
            return;
        }
        _meta_state_machine->process(controller, request, response, done_guard.release());
        return;
    }
    case pb::OP_MERGE_REGION: {
        if (!request->has_region_merge()) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                               "no merge region info", request->op_type(), log_id);
            return;
        }
        auto ret = pre_process_for_merge_region(request, response, log_id);
        if (ret < 0) {
            return;
        }
        return;
    }
    case pb::OP_UPDATE_INDEX_STATUS:
    case pb::OP_DELETE_DDLWORK: 
    case pb::OP_REMOVE_GLOBAL_INDEX_DATA: {
        _meta_state_machine->process(controller, request, response, done_guard.release());
        return;
    }
    case pb::OP_UPDATE_INDEX_REGION_DDL_WORK:
    case pb::OP_SUSPEND_DDL_WORK:
    case pb::OP_RESTART_DDL_WORK: {
        _meta_state_machine->process(controller, request, response, done_guard.release());
        return;
    }
    case pb::OP_UPDATE_STATISTICS: {
        if (!request->has_statistics()) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                    "no statistics info", request->op_type(), log_id);
            return;
        }
        _meta_state_machine->process(controller, request, response, done_guard.release());
        return;
    }
    case pb::OP_DROP_PARTITION_TS: {
        _meta_state_machine->process(controller, request, response, done_guard.release());
        return;
    }
    default:
        ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                "invalid op_type", request->op_type(), log_id);
        return;
    }
}

void SchemaManager::process_schema_heartbeat_for_store(
                    const pb::StoreHeartBeatRequest* request, 
                    pb::StoreHeartBeatResponse* response) {
    if (!_meta_state_machine->is_leader()) {
        response->set_errcode(pb::NOT_LEADER);
        response->set_errmsg("not leader");
        response->set_leader(butil::endpoint2str(_meta_state_machine->get_leader()).c_str());
        return;
    }
    std::unordered_map<int64_t, int64_t> store_table_id_version;
    for (auto& store_schema_info : request->schema_infos()) {
        int64_t table_id = store_schema_info.table_id();
        store_table_id_version[table_id] = store_schema_info.version();
    }
    TableManager::get_instance()->process_schema_heartbeat_for_store(
                                    store_table_id_version,
                                    response);
}

void SchemaManager::process_peer_heartbeat_for_store(const pb::StoreHeartBeatRequest* request,
                                            pb::StoreHeartBeatResponse* response,
                                            uint64_t log_id) {
    //region_id所在的table_id如果不存在，则说明表创建失败，或者表已经删除
    //目前该规则对于层次表不适用
    //二期需要重新考虑层次表的数据删除问题
    if (!_meta_state_machine->is_leader()) {
        response->set_errcode(pb::NOT_LEADER);
        response->set_errmsg("not leader");
        response->set_leader(butil::endpoint2str(_meta_state_machine->get_leader()).c_str());
        return;
    }
    TimeCost step_time_cost;
    TableManager::get_instance()->check_table_exist_for_peer(request, response);
    int64_t table_exist_time = step_time_cost.get_time();
    step_time_cost.reset();
    TableManager::get_instance()->check_partition_exist_for_peer(request, response);
    int64_t partition_exist_time = step_time_cost.get_time();
    step_time_cost.reset();
    RegionManager::get_instance()->check_whether_illegal_peer(request, response);
    int64_t illegal_peer_time = step_time_cost.get_time();
    step_time_cost.get_time();
    DB_NOTICE("process peer hearbeat, table_exist_time: %ld, partition_exist_time: %ld, "
              "ilegal_peer_time: %ld, log_id: %lu",
              table_exist_time, partition_exist_time, illegal_peer_time, log_id);
}

void SchemaManager::process_leader_heartbeat_for_store(const pb::StoreHeartBeatRequest* request,
                                             pb::StoreHeartBeatResponse* response,
                                             uint64_t log_id) {
    if (!_meta_state_machine->is_leader()) {
        response->set_errcode(pb::NOT_LEADER);
        response->set_errmsg("not leader");
        response->set_leader(butil::endpoint2str(_meta_state_machine->get_leader()).c_str());
        return;
    }

    int64_t timestamp = butil::gettimeofday_us();
    TimeCost step_time_cost;
    RegionManager::get_instance()->leader_heartbeat_for_region(request, response);
    int64_t leader_region_time = step_time_cost.get_time();
    step_time_cost.reset();

    RegionManager::get_instance()->update_binlog_status(request);
    RegionManager::get_instance()->update_leader_status(request, timestamp);
    int64_t update_status_time = step_time_cost.get_time();
    step_time_cost.reset();
    step_time_cost.reset();

    std::string resource_tag = request->instance_info().resource_tag();
    RegionManager::get_instance()->leader_load_balance(_meta_state_machine->whether_can_decide(), 
            _meta_state_machine->get_load_balance(resource_tag), request, response);
    int64_t leader_balance_time = step_time_cost.get_time();
    DB_NOTICE("store: %s process leader heartbeat, update_status_time: %ld, leader_region_time: %ld"
                " leader_balance_time: %ld, log_id: %lu",
                request->instance_info().address().c_str(),
                update_status_time, leader_region_time, leader_balance_time, log_id); 
} 

void SchemaManager::process_baikal_heartbeat(const pb::BaikalHeartBeatRequest* request,
                                            pb::BaikalHeartBeatResponse* response,
                                            uint64_t log_id) {
    if (!_meta_state_machine->is_leader()) {
        response->set_errcode(pb::NOT_LEADER);
        response->set_errmsg("not leader");
        response->set_leader(butil::endpoint2str(_meta_state_machine->get_leader()).c_str());
        return;
    }
    TimeCost step_time_cost; 
    std::set<std::int64_t> report_table_ids;
    std::unordered_map<int64_t, std::set<std::int64_t>> report_region_ids;
    int64_t prepare_time =0;
    step_time_cost.reset(); 

    if (request == nullptr) {
        DB_WARNING("request is nullptr");
        return;
    }

    std::unordered_set<int64_t> heartbeat_table_ids;
    bool need_heartbeat_table  = (request->has_need_heartbeat_table() && request->need_heartbeat_table());
    bool need_binlog_heartbeat = (request->has_need_binlog_heartbeat() && request->need_binlog_heartbeat());
    if (need_heartbeat_table) {
        for (const auto& table_info : request->heartbeat_tables()) {
            if (need_binlog_heartbeat && table_info.table_name() == "*") {
                // 通配符'*'处理
                const std::string& database_name = table_info.namespace_name() + "\001" + table_info.database();
                const int64_t database_id = DatabaseManager::get_instance()->get_database_id(database_name);
                std::set<int64_t> table_ids;
                int ret = DatabaseManager::get_instance()->get_table_ids(database_id, table_ids);
                if (ret < 0) {
                    DB_WARNING("Fail to get_table_ids, database_id : %ld", database_id);
                    continue;
                }
                heartbeat_table_ids.insert(table_ids.begin(), table_ids.end());
            } else {
                const std::string& full_table_name = 
                        table_info.namespace_name() + "\001" + table_info.database() + "\001" + table_info.table_name();
                const int64_t table_id = TableManager::get_instance()->get_table_id(full_table_name);
                if (table_id == 0) {
                    DB_FATAL("Fail to get table_id, table_name: %s", full_table_name.c_str());
                    continue;
                }
                heartbeat_table_ids.insert(table_id);
            }
        }

        if (need_binlog_heartbeat) {
            std::unordered_set<int64_t> heartbeat_binlog_table_ids;
            for (const int64_t& table_id : heartbeat_table_ids) {
                pb::SchemaInfo table_info;
                int ret = TableManager::get_instance()->get_table_info(table_id, table_info);
                if (ret < 0) {
                    DB_WARNING("Fail to get_table_info, table_id: %ld", table_id);
                    continue;
                }
                if (table_info.has_binlog_info() && table_info.binlog_info().has_binlog_table_id()) {
                    heartbeat_binlog_table_ids.insert(table_info.binlog_info().binlog_table_id());
                }
                if (table_info.binlog_infos_size() > 0) {
                    for (int i = 0; i < table_info.binlog_infos_size(); i++) {
                        auto& binlog_info = table_info.binlog_infos(i);
                        if (binlog_info.has_binlog_table_id()) {
                            heartbeat_binlog_table_ids.insert(binlog_info.binlog_table_id());
                        }
                    }
                }
            }
            if (heartbeat_binlog_table_ids.empty()) {
                DB_WARNING("heartbeat_binlog_table_ids is empty");
            }
            // Binlog心跳只需要返回Binlog Table对应的Region
            std::swap(heartbeat_table_ids, heartbeat_binlog_table_ids);
        }
    }

    bool need_update_schema = false;
    bool need_update_region = false;
    int64_t last_updated_index = request->last_updated_index();
    int64_t applied_index = _meta_state_machine->applied_index();
    if (last_updated_index > 0) {
        need_update_schema = TableManager::get_instance()->check_and_update_incremental(
                                                            request, response, applied_index);
        need_update_region = RegionManager::get_instance()->check_and_update_incremental(
                                                            request, response, applied_index, heartbeat_table_ids);
        //DB_WARNING("update  update_incremental last_updated_index:%ld log_id: %lu", last_updated_index, log_id);
    }
    int64_t update_incremental_time = step_time_cost.get_time();
    step_time_cost.reset(); 
    if (last_updated_index == 0 || need_update_schema || need_update_region) {
        for (auto& schema_heart_beat : request->schema_infos()) {
            int64_t table_id = schema_heart_beat.table_id();
            report_table_ids.insert(table_id);
            report_region_ids[table_id] = std::set<std::int64_t>{};
            for (auto& region_info : schema_heart_beat.regions()) {
                report_region_ids[table_id].insert(region_info.region_id());
            }
        }      
        prepare_time = step_time_cost.get_time(); 
        step_time_cost.reset();
    }
    if (last_updated_index == 0 || need_update_schema) {
        //DB_WARNING("DEBUG schema update all applied_index:%ld log_id: %lu", applied_index, log_id);
        response->set_last_updated_index(applied_index);
        //判断上报的表是否已经更新或删除
        TableManager::get_instance()->check_update_or_drop_table(request, response); 
        //判断是否有新增的表没有下推给baikaldb
        std::vector<int64_t> new_add_region_ids;
        TableManager::get_instance()->check_add_table(
            report_table_ids, new_add_region_ids, request, response, heartbeat_table_ids);
        RegionManager::get_instance()->add_region_info(new_add_region_ids, response);
    }
    int64_t update_table_time = step_time_cost.get_time();
    step_time_cost.reset(); 
    if (last_updated_index == 0 || need_update_region) {
        //DB_WARNING("region update all applied_index:%ld log_id: %lu", applied_index, log_id);
        response->set_last_updated_index(applied_index);
        //判断上报的region是否已经更新或删除
        RegionManager::get_instance()->check_update_region(request, response);
        //判断是否有新增的region没有下推到baikaldb
        TableManager::get_instance()->check_add_region(
            report_table_ids, report_region_ids, request, response, heartbeat_table_ids);
    }
    int64_t update_region_time = step_time_cost.get_time();
    DB_NOTICE("process schema info for baikal heartbeat, prepare_time: %ld, update_incremental_time:%ld,"
                " update_table_time: %ld, update_region_time: %ld, log_id: %lu",
                prepare_time, update_incremental_time, update_table_time, update_region_time, log_id);
    //判断是否需要更新内存中虚拟索引影响面信息
    TableManager::get_instance()->load_virtual_indextosqls_to_memory(request);
}

int SchemaManager::check_and_get_for_privilege(pb::UserPrivilege& user_privilege) {
    std::string namespace_name = user_privilege.namespace_name();
    int64_t namespace_id = NamespaceManager::get_instance()->get_namespace_id(namespace_name);
    if (namespace_id == 0) {
        DB_FATAL("namespace not exist, namespace:%s, request: %s", 
                        namespace_name.c_str(),
                        user_privilege.ShortDebugString().c_str());
        return -1;
    }
    user_privilege.set_namespace_id(namespace_id);
    for (auto& pri_base : *user_privilege.mutable_privilege_database()) {
        std::string base_name = namespace_name + "\001" + pri_base.database();
        int64_t database_id = DatabaseManager::get_instance()->get_database_id(base_name); 
        if (database_id == 0) {
            DB_FATAL("database:%s not exist, namespace:%s, request: %s",
                base_name.c_str(), namespace_name.c_str(), 
                user_privilege.ShortDebugString().c_str());
            return -1;
        }
        pri_base.set_database_id(database_id);
    }
    for (auto& pri_table : *user_privilege.mutable_privilege_table()) {
        std::string base_name = namespace_name + "\001" + pri_table.database();
        std::string table_name = base_name + "\001" + pri_table.table_name();
        int64_t database_id = DatabaseManager::get_instance()->get_database_id(base_name);
        if (database_id == 0) {
            DB_FATAL("database:%s not exist, namespace:%s, request: %s",
                            base_name.c_str(), namespace_name.c_str(),
                            user_privilege.ShortDebugString().c_str());
            return -1;
        }
        int64_t table_id = TableManager::get_instance()->get_table_id(table_name); 
        if (table_id == 0) {
            DB_FATAL("table_name:%s not exist, database:%s namespace:%s, request: %s",
                        table_name.c_str(), base_name.c_str(),
                        namespace_name.c_str(), user_privilege.ShortDebugString().c_str());
            return -1;    
        }
        pri_table.set_database_id(database_id);
        pri_table.set_table_id(table_id);
    }
    return 0;
}

int SchemaManager::load_snapshot() {
    NamespaceManager::get_instance()->clear();
    DatabaseManager::get_instance()->clear();
    TableManager::get_instance()->clear();
    RegionManager::get_instance()->clear();
    //创建一个snapshot
    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    read_options.total_order_seek = false;
    RocksWrapper* db = RocksWrapper::get_instance();
    std::unique_ptr<rocksdb::Iterator> iter(
            RocksWrapper::get_instance()->new_iterator(read_options, db->get_meta_info_handle()));
    iter->Seek(MetaServer::SCHEMA_IDENTIFY);
    std::string max_id_prefix = MetaServer::SCHEMA_IDENTIFY;
    max_id_prefix += MetaServer::MAX_ID_SCHEMA_IDENTIFY;

    std::string namespace_prefix = MetaServer::SCHEMA_IDENTIFY;
    namespace_prefix += MetaServer::NAMESPACE_SCHEMA_IDENTIFY;

    std::string database_prefix = MetaServer::SCHEMA_IDENTIFY;
    database_prefix += MetaServer::DATABASE_SCHEMA_IDENTIFY;

    std::string table_prefix = MetaServer::SCHEMA_IDENTIFY;
    table_prefix += MetaServer::TABLE_SCHEMA_IDENTIFY;

    std::string region_prefix = MetaServer::SCHEMA_IDENTIFY;
    region_prefix += MetaServer::REGION_SCHEMA_IDENTIFY;

    std::string ddl_prefix = MetaServer::SCHEMA_IDENTIFY;
    ddl_prefix += MetaServer::DDLWORK_IDENTIFY;

    std::string statistics_prefix = MetaServer::SCHEMA_IDENTIFY;
    statistics_prefix += MetaServer::STATISTICS_IDENTIFY;

    std::string index_ddl_region_prefix = MetaServer::SCHEMA_IDENTIFY;
    index_ddl_region_prefix += MetaServer::INDEX_DDLWORK_REGION_IDENTIFY;
 
    for (; iter->Valid(); iter->Next()) {
        int ret = 0;
        if (iter->key().starts_with(region_prefix)) {
            ret = RegionManager::get_instance()->load_region_snapshot(iter->value().ToString());
        } else if (iter->key().starts_with(table_prefix)) {
            ret = TableManager::get_instance()->load_table_snapshot(iter->value().ToString());
        } else if (iter->key().starts_with(database_prefix)) {
            ret = DatabaseManager::get_instance()->load_database_snapshot(iter->value().ToString());
        } else if (iter->key().starts_with(namespace_prefix)) {
            ret = NamespaceManager::get_instance()->load_namespace_snapshot(iter->value().ToString());
        } else if (iter->key().starts_with(max_id_prefix)) {
            ret = load_max_id_snapshot(max_id_prefix, iter->key().ToString(), iter->value().ToString());
        } else if (iter->key().starts_with(statistics_prefix)) {
            ret = TableManager::get_instance()->load_statistics_snapshot(iter->value().ToString());
        }else if (iter->key().starts_with(ddl_prefix)) {
            ret = TableManager::get_instance()->load_ddl_snapshot(iter->value().ToString());
        } else if (iter->key().starts_with(index_ddl_region_prefix)) {
            ret = DDLManager::get_instance()->load_region_ddl_snapshot(iter->value().ToString());
        } else {
            DB_FATAL("unsupport schema info when load snapshot, key:%s", iter->key().data());
        }
        if (ret != 0) {
            DB_FATAL("load snapshot fail, key:%s, value:%s", 
                      iter->key().data(), 
                      iter->value().data());
            return -1;
        }
    }
    TableManager::get_instance()->check_startkey_regionid_map();
    return 0;
}

int SchemaManager::pre_process_for_create_table(const pb::MetaManagerRequest* request,
            pb::MetaManagerResponse* response,
            uint64_t log_id) {
    auto& table_info = const_cast<pb::SchemaInfo&>(request->table_info());
    // 校验新表所在集群是否有learner
    if (table_info.learner_resource_tags_size() > 0) {
        for (const auto& learner_resource_tag : table_info.learner_resource_tags()) {
            if(!ClusterManager::get_instance()->check_resource_tag_exist(learner_resource_tag)) {
                ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                    "learner resource_tag not exist", request->op_type(), log_id);
                return -1;
            }
        }
    }

    // Partition
    if (pre_process_for_partition(request, response, log_id) != 0) {
        ERROR_SET_RESPONSE(response, pb::INTERNAL_ERROR,
                           "Fail to pre_process_for_partition", request->op_type(), log_id);
        return -1;
    }

    std::set<std::string> indexs_name;
    std::string primary_index_name;
    //校验只有普通索引和uniq 索引可以设置全局属性
    for (auto& index_info : request->table_info().indexs()) {
        if (index_info.is_global() 
                && index_info.index_type() != pb::I_UNIQ 
                && index_info.index_type() != pb::I_KEY) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                    "global index only support I_UNIQ or I_KEY", request->op_type(), log_id);
            return -1;
        }
        if (indexs_name.find(index_info.index_name()) != indexs_name.end()) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                    "index name repeated", request->op_type(), log_id);
            return -1;
        }
        if (index_info.index_type() == pb::I_PRIMARY || index_info.is_global()) {
            primary_index_name = index_info.index_name();
            DB_NOTICE("set primary index name %s", primary_index_name.c_str());
        }
        indexs_name.insert(index_info.index_name());
    }
    // 分区表多region时，设置多split_key
    if (table_info.has_region_num() && request->table_info().split_keys_size() == 0) {
        int32_t region_num = table_info.region_num();
        if (region_num > 1) {
            auto split_keys = table_info.add_split_keys();
            split_keys->set_index_name(primary_index_name);
            for (auto index = 1; index < region_num; ++index) {
                split_keys->add_split_keys(std::string(index+1, 0x01));
            }
        }
    }

    // 校验split_key是否有序
    for (auto& split_key : request->table_info().split_keys()) {
        if (indexs_name.find(split_key.index_name()) == indexs_name.end()) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                    "index_name for split key not exist", request->op_type(), log_id);
            return -1;
        }
        for (auto i = 1; i < split_key.split_keys_size(); ++i) {
            if (split_key.split_keys(i) <= split_key.split_keys(i - 1)) {
                ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                        "split key not sorted", request->op_type(), log_id);
                return -1; 
            }
        }
    }

    for (auto i = 0; i < request->table_info().fields_size(); ++i) {
        auto field = request->table_info().fields(i);
        if (!field.has_mysql_type() || !field.has_field_name()) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                            "missing field id (type or name)", request->op_type(), log_id);         
            return -1;
        }
    }
    auto mutable_request = const_cast<pb::MetaManagerRequest*>(request);
    std::string main_logical_room;
    // 用户指定了跨机房部署
    auto ret = whether_dists_legal(mutable_request, response, main_logical_room, log_id);
    if (ret != 0) {
        return ret;
    }

    std::string resource_tag = request->table_info().resource_tag();
    boost::trim(resource_tag);

    pb::DataBaseInfo database_info;
    pb::NameSpaceInfo namespace_info;
    std::string namespace_name = table_info.namespace_name();
    std::string database_name = namespace_name + "\001" + table_info.database();
    int64_t namespace_id = NamespaceManager::get_instance()->get_namespace_id(namespace_name);
    int64_t database_id = DatabaseManager::get_instance()->get_database_id(database_name);
    int ns_ret = NamespaceManager::get_instance()->get_namespace_info(namespace_id, namespace_info);
    int db_ret  = DatabaseManager::get_instance()->get_database_info(database_id, database_info);

    auto* table_info_ptr = mutable_request->mutable_table_info();

    if (ns_ret == 0 || db_ret == 0) {
    #define SET_REQUEST_TABLE_INFO(TABLE_INFO_FIELD) \
        if (!table_info.has_##TABLE_INFO_FIELD()) { \
            if (namespace_info.has_##TABLE_INFO_FIELD()) { \
                table_info_ptr->set_##TABLE_INFO_FIELD(namespace_info.TABLE_INFO_FIELD()); \
            } \
            if (database_info.has_##TABLE_INFO_FIELD()) { \
                table_info_ptr->set_##TABLE_INFO_FIELD(database_info.TABLE_INFO_FIELD()); \
            } \
        }
        SET_REQUEST_TABLE_INFO(engine);
        SET_REQUEST_TABLE_INFO(charset);
        SET_REQUEST_TABLE_INFO(byte_size_per_record);
        SET_REQUEST_TABLE_INFO(replica_num);
        SET_REQUEST_TABLE_INFO(region_split_lines);

    #undef SET_REQUEST_TABLE_INFO

        if (!table_info.has_resource_tag()) {
            std::string ns_resource_tag = namespace_info.resource_tag();
            std::string db_resource_tag = database_info.resource_tag();
            if (db_resource_tag != "") {
                resource_tag = db_resource_tag;
            } else if (ns_resource_tag != "") {
                resource_tag = ns_resource_tag;
            }
        }
    }
    // 目前建表新建region不考虑dist分布
    table_info_ptr->set_resource_tag(resource_tag);

    return 0;
}

int SchemaManager::pre_process_for_merge_region(const pb::MetaManagerRequest* request,
        pb::MetaManagerResponse* response,
        uint64_t log_id) { 
    TimeCost time_cost;
    int64_t src_region_id = request->region_merge().src_region_id();
    if (request->region_merge().src_end_key().empty()) {
        DB_WARNING("src end key is empty request:%s log_id:%lu",
                 request->ShortDebugString().c_str(), log_id); 
        ERROR_SET_RESPONSE_WARN(response, pb::INPUT_PARAM_ERROR,
                           "src end key is empty", request->op_type(), log_id);
        return -1;
    }
    RegionManager* region_manager = RegionManager::get_instance();
    auto src_region = region_manager->get_region_info(src_region_id);
    if (src_region == nullptr) {
        DB_WARNING("can`t find src region request:%s, src region_id:%ld, log_id:%lu",
                 request->ShortDebugString().c_str(), src_region_id, log_id); 
        ERROR_SET_RESPONSE_WARN(response, pb::INPUT_PARAM_ERROR,
                           "can not find src region", request->op_type(), log_id);
        return -1;
    }
    if (src_region->start_key() != request->region_merge().src_start_key()
       || src_region->end_key() != request->region_merge().src_end_key()) {
        DB_WARNING("src region_id:%ld has diff key (satrt_key, end_key), "
                   "req:(%s, %s), local:(%s, %s)",  src_region_id,
                   str_to_hex(request->region_merge().src_start_key()).c_str(), 
                   str_to_hex(request->region_merge().src_end_key()).c_str(),
                   str_to_hex(src_region->start_key()).c_str(),
                   str_to_hex(src_region->end_key()).c_str());
        ERROR_SET_RESPONSE_WARN(response, pb::INPUT_PARAM_ERROR,
                           "src key diff with local", request->op_type(), log_id);
        return -1;
    }
    int64_t table_id = request->region_merge().table_id();

    int64_t partition_id = 0;
    if (request->region_merge().has_partition_id()) {
        partition_id = request->region_merge().partition_id();
    }
    int64_t dst_region_id = TableManager::get_instance()->get_next_region_id(
                        table_id, request->region_merge().src_start_key(), 
                        request->region_merge().src_end_key(), partition_id);
    if (dst_region_id <= 0) {
        DB_WARNING("can`t find dst merge region request: %s, src region id:%ld, log_id:%lu",
                 request->ShortDebugString().c_str(), src_region_id, log_id);
        ERROR_SET_RESPONSE_WARN(response, pb::INPUT_PARAM_ERROR,
                           "dst merge region not exist", request->op_type(), log_id);
        return -1;
    }
    auto dst_region = region_manager->get_region_info(dst_region_id);
    if (dst_region == nullptr) {
        DB_WARNING("can`t find dst merge region request: %s, src region id:%ld, "
                 "dst region id:%ld, log_id:%lu",
                 request->ShortDebugString().c_str(), 
                 src_region_id, dst_region_id, log_id); 
        ERROR_SET_RESPONSE_WARN(response, pb::INPUT_PARAM_ERROR,
                           "dst merge region not exist", request->op_type(), log_id);
        return -1;
    }
    pb::RegionMergeResponse* region_merge = response->mutable_merge_response();
    region_merge->set_dst_instance(dst_region->leader());
    region_merge->set_dst_start_key(dst_region->start_key());
    region_merge->set_dst_end_key(dst_region->end_key());
    region_merge->set_dst_region_id(dst_region->region_id());  
    region_merge->set_version(dst_region->version());
    region_merge->mutable_dst_region()->CopyFrom(*dst_region);
    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("success");
    DB_WARNING("find dst merge region instance:%s, table_id:%ld, src region id:%ld, "
               "dst region_id:%ld, version:%ld, src(%s, %s), dst(%s, %s), "
               "time_cost:%ld, log_id:%lu", 
               response->mutable_merge_response()->dst_instance().c_str(), 
               request->region_merge().table_id(), src_region_id,
               dst_region_id, response->mutable_merge_response()->version(),
               str_to_hex(src_region->start_key()).c_str(),
               str_to_hex(src_region->end_key()).c_str(),
               str_to_hex(dst_region->start_key()).c_str(),
               str_to_hex(dst_region->end_key()).c_str(),
               time_cost.get_time(), log_id);
    return 0;
}

int SchemaManager::pre_process_for_split_region(const pb::MetaManagerRequest* request,
            pb::MetaManagerResponse* response,
            uint64_t log_id) { 
    int64_t region_id = request->region_split().region_id();
    auto ptr_region = RegionManager::get_instance()->get_region_info(region_id);
    if (ptr_region == nullptr) {
        ERROR_SET_RESPONSE_WARN(response, pb::INPUT_PARAM_ERROR,
                            "table id not exist", request->op_type(), log_id);
        return -1;
    }
    if (ptr_region->peers_size() < 2) {
        ERROR_SET_RESPONSE_WARN(response, pb::INPUT_PARAM_ERROR,
                                "region not stable, cannot split",
                                request->op_type(),
                                log_id);
        DB_WARNING("region cannot split, region not stable, request: %s, region_id: %ld",
                   request->ShortDebugString().c_str(), region_id);
        return -1;
    }
    int64_t table_id = 0;
    if (request->region_split().has_table_id()) {
        table_id = request->region_split().table_id();
    } else {
        table_id = ptr_region->table_id();
    }
    if (TableManager::get_instance()->is_table_in_fast_importer(table_id)) {
        ERROR_SET_RESPONSE_WARN(response, pb::INPUT_PARAM_ERROR,
                                "region in fast importer, cannot split",
                                request->op_type(),
                                log_id);
        DB_WARNING("region cannot split, region in fast importer, request: %s, region_id: %ld",
                   request->ShortDebugString().c_str(), region_id);
        return -1;
    }

    std::string resource_tag;
    if (request->region_split().has_resource_tag()) {
        resource_tag = request->region_split().resource_tag();
    } else {
        auto ret = TableManager::get_instance()->get_resource_tag(table_id, resource_tag);
        if (ret < 0) {
            ERROR_SET_RESPONSE_WARN(response, pb::INPUT_PARAM_ERROR,
                                "table id not exist", request->op_type(), log_id);
            return -1;
        }
    }
    auto parent_region = RegionManager::get_instance()->get_region_info(region_id);
    std::set<std::string> parent_region_stores;
    if (parent_region != nullptr) {
        for(auto& peer : parent_region->peers()) {
            parent_region_stores.insert(peer);
        }
    }
    bool is_tail_split = request->region_split().has_tail_split() && request->region_split().tail_split();
    int region_num = request->region_split().new_region_num();
    if (region_num < 1) {
        region_num = 1;
    }
    int64_t instance_num = 0;
    int ret = TableManager::get_instance()->get_replica_num(table_id, instance_num);
    if (ret < 0) {
        ERROR_SET_RESPONSE_WARN(response, pb::INPUT_PARAM_ERROR,
                                "replica num not exist", request->op_type(), log_id);
        return -1;
    }
    // 副本分布 {resource_tag:logical_room:physical_room} : replica_count
    // 主resource tag优先
    std::vector<std::string> table_idcs;
    std::vector<int> idc_peer_cnts;
    ret = TableManager::get_instance()->get_replica_dist_idcs(table_id, table_idcs, idc_peer_cnts);
    if (ret < 0) {
        ERROR_SET_RESPONSE_WARN(response, pb::INPUT_PARAM_ERROR,
                                "replica idcs not exist", request->op_type(), log_id);
        return -1;
    }

    if (parent_region != nullptr) {
        const int64_t partition_id = parent_region->partition_id();
        int64_t partition_replica_num = -1;
        std::string partition_resource_tag;
        TableManager::get_instance()->get_partition_info(
                table_id, partition_id, partition_replica_num, partition_resource_tag);
        if (partition_replica_num != -1 && partition_resource_tag.size() != 0) {
            instance_num = partition_replica_num;
            table_idcs = {partition_resource_tag};
            idc_peer_cnts = {partition_replica_num};
        }
    }

    // 5副本，分裂的时候选3个peer，剩下两个peer在分裂完之后补齐
    if (instance_num > 3) {
        instance_num = 3;
    }

    //从cluster中选择store, 尾分裂选择replica个store, 中间分裂选择replica-1个store
    std::set<std::string> exclude_stores;
    std::string source_instance = request->region_split().new_instance();
    if (!is_tail_split) {
        response->mutable_split_response()->set_new_instance(source_instance);
        --instance_num;
        exclude_stores.insert(source_instance);
    }
    if (region_num == 1) {
        int selected_instance = 0;
        for (int idc_idx = 0; idc_idx < table_idcs.size(); ++idc_idx) {    
            const std::string& candidate_resouce_tag = table_idcs[idc_idx];
            int need_peer_count = idc_peer_cnts[idc_idx];
            IdcInfo replica_idc(candidate_resouce_tag);
            for (auto i = 0; i < need_peer_count; ++i) {
                std::string instance;
                // 尽量和父region的三个peer不在一个store上，同时也得保证只有三个store也能分裂
                // 选store，如果选到了父region peer所在的store，最多重试3次
                for (auto select_retry_time = 0; select_retry_time < 3; ++select_retry_time) {
                    ret = ClusterManager::get_instance()->select_instance_rolling(replica_idc,
                                                                                exclude_stores,
                                                                                instance);
                    if (ret < 0) {
                        ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                                        "select instance fail", request->op_type(), log_id);
                        return -1;
                    }
                    if (parent_region_stores.count(instance) == 0) {
                        break;
                    }
                }
                if (is_tail_split && selected_instance == 0) {
                    response->mutable_split_response()->set_new_instance(instance);
                } else {
                    response->mutable_split_response()->add_add_peer_instance(instance);
                }
                exclude_stores.insert(instance);
                ++selected_instance;
                if (selected_instance == instance_num) {
                    return 0;
                }
            }
        }
    } else if (is_tail_split && region_num > 1) {
        // 每个新region选3个peer
        for (int idx = 0; idx < region_num; ++idx) {
            int selected_instance = 0;
            auto new_region_info = response->mutable_split_response()->add_multi_new_regions();
            exclude_stores.clear();
            for (int idc_idx = 0; idc_idx < table_idcs.size(); ++idc_idx) {    
                const std::string& candidate_resouce_tag = table_idcs[idc_idx];
                int need_peer_count = idc_peer_cnts[idc_idx];
                IdcInfo replica_idc(candidate_resouce_tag);
                for (auto i = 0; i < need_peer_count; ++i) {
                    std::string instance;
                    // 直接rolling选
                    ret = ClusterManager::get_instance()->select_instance_rolling(replica_idc,
                                                                                exclude_stores,
                                                                                instance);
                    if (ret < 0) {
                        ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                                        "select instance fail", request->op_type(), log_id);
                        return -1;
                    }
                    if (selected_instance == 0) {
                        new_region_info->set_new_instance(instance);
                    } else {
                        new_region_info->add_add_peer_instance(instance);
                    }
                    ++selected_instance;
                    exclude_stores.insert(instance);
                    if (selected_instance == instance_num) {
                        break;
                    }
                }
            }
        }
        if (response->mutable_split_response()->multi_new_regions_size() != region_num) {
            DB_FATAL("region_id: %ld, is_tail_split: %d, region_num: %d != multi_new_regions_size: %d", 
                    region_id, is_tail_split, region_num, response->mutable_split_response()->multi_new_regions_size());
            return -1;
        } 
    } else {
        DB_FATAL("not support, region_id: %ld, is_tail_split: %d, region_num: %d", region_id, is_tail_split, region_num);
        return -1;
    }
    return 0;
}
int SchemaManager::load_max_id_snapshot(const std::string& max_id_prefix, 
                                          const std::string& key, 
                                          const std::string& value) {
    std::string max_key(key, max_id_prefix.size());
    int64_t* max_id = (int64_t*)(value.c_str()); 
    if (max_key == SchemaManager::MAX_NAMESPACE_ID_KEY) {
        NamespaceManager::get_instance()->set_max_namespace_id(*max_id);
        DB_WARNING("max_namespace_id:%ld", *max_id);
        return 0;
    }
    if (max_key == SchemaManager::MAX_DATABASE_ID_KEY) {
        DatabaseManager::get_instance()->set_max_database_id(*max_id);
        DB_WARNING("max_database_id:%ld", *max_id);
        return 0;
    }
    if (max_key == SchemaManager::MAX_TABLE_ID_KEY) {
        TableManager::get_instance()->set_max_table_id(*max_id);
        DB_WARNING("max_table_id:%ld", *max_id);
        return 0;
    }
    if (max_key == SchemaManager::MAX_REGION_ID_KEY) {
        RegionManager::get_instance()->set_max_region_id(*max_id); 
        DB_WARNING("max_region_id:%ld", *max_id);
        return 0;
    } 
    return 0;
}
int SchemaManager::whether_dists_legal(pb::MetaManagerRequest* request,
                        pb::MetaManagerResponse* response,
                        std::string& candidate_logical_room,
                        uint64_t log_id) {
    if (request->table_info().dists_size() == 0) {
        return 0;
    }
    int64_t total_count = 0;
    //检验逻辑机房是否存在
    for (auto& dist : request->table_info().dists()) {
        if (dist.logical_room().empty() && dist.physical_room().empty()) {
            // dist不指定逻辑机房和物理机房，则必须指定集群
            if (dist.resource_tag().empty()) {
                return false;
            }
            total_count += dist.count();
            continue;
        } 
        if (ClusterManager::get_instance()->logical_and_physical_room_valid(dist.logical_room(), dist.physical_room())) {
            total_count += dist.count();
            continue;
        }
        ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                "logical room and physical room not valid, select instance fail",
                request->op_type(), log_id);
        return -1;
    }
    if (request->table_info().main_logical_room().size() == 0) {
        int max_count = 0;
        // 副本数量最多的机房是主机房
        for (auto& dist : request->table_info().dists()) {
            if (dist.count() > max_count) {
                max_count = dist.count();
                candidate_logical_room = dist.logical_room();
            }
        }
    }
    
    if (total_count != request->table_info().replica_num()) {
        ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                "replica num not match", request->op_type(), log_id);
        return -1;
    }
    return 0;
}
int SchemaManager::whether_main_logical_room_legal(pb::MetaManagerRequest* request,
                        pb::MetaManagerResponse* response,
                        uint64_t log_id) {
    if (!request->table_info().has_main_logical_room()) {
        return 0;
    }
    int64_t table_id = 0;
    if (TableManager::get_instance()->check_table_exist(request->table_info(), table_id) != 0) {
        DB_WARNING("check table exist fail, request:%s", request->ShortDebugString().c_str());
        ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                "table not exist", request->op_type(), log_id);
        return -1;
    }
    pb::SchemaInfo schema_pb;
    if (TableManager::get_instance()->get_table_info(table_id, schema_pb) != 0) {
        DB_WARNING("check table exist fail, request:%s", request->ShortDebugString().c_str());
        ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                "table not exist", request->op_type(), log_id);
        return -1;
    }
    //检验逻辑机房是否存在
    bool found = false;
    for (auto& dist : schema_pb.dists()) {
        std::string logical_room = dist.logical_room();
        if ((dist.resource_tag() == "" || dist.resource_tag() == schema_pb.resource_tag()) 
            && logical_room == request->table_info().main_logical_room()) {
            found = true;
            break;
        }
    }
    if (!found) {
        DB_WARNING("main_logical_room not match, request:%s", request->ShortDebugString().c_str());
        ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                "main_logical_room not match", request->op_type(), log_id);
        return -1;
    }
    return 0;
}

int SchemaManager::pre_process_for_partition(const pb::MetaManagerRequest* request,
                                             pb::MetaManagerResponse* response,
                                             uint64_t log_id) {
    if (request == nullptr ||
            !request->has_table_info() || 
            !request->table_info().has_partition_info() ||
            request->table_info().partition_info().type() != pb::PT_RANGE) {
        return 0;
    }

    pb::SchemaInfo& table_info = const_cast<pb::SchemaInfo&>(request->table_info());
    pb::PartitionInfo* p_partition_info = table_info.mutable_partition_info();
    if (p_partition_info == nullptr) {
        DB_WARNING("p_partition_info is nullptr, log_id: %lu", log_id);
        return -1;
    }

    // 获取分区列的类型
    pb::PrimitiveType partition_col_type = pb::INVALID_TYPE;
    if (p_partition_info->has_field_info()) {
        const std::string& partition_field_name = p_partition_info->field_info().field_name();
        for (auto& field : table_info.fields()) {
            if (field.field_name() == partition_field_name) {
                partition_col_type = field.mysql_type();
                break;
            }
        }
    }
    if (partition_col_type != pb::DATE && partition_col_type != pb::DATETIME && !is_int(partition_col_type)) {
        DB_WARNING("Invalid partition col_type %d, log_id: %lu", partition_col_type, log_id);
        return -1;
    }
    if (p_partition_info->mutable_field_info() == nullptr) {
        DB_WARNING("p_partition_info mutable_field_info is nullptr, log_id: %lu", log_id);
        return -1;
    }
    p_partition_info->mutable_field_info()->set_mysql_type(partition_col_type);

    // Partition参数合法性检查
    std::set<std::string> partition_names;
    for (size_t i = 0; i < p_partition_info->range_partition_infos_size(); ++i) {
        pb::RangePartitionInfo* p_range_partition_info = p_partition_info->mutable_range_partition_infos(i);
        if (p_range_partition_info == nullptr) {
            DB_WARNING("p_range_partition_info is nullptr, log_id: %lu", log_id);
            return -1;
        }
        if (!p_range_partition_info->has_partition_name() || p_range_partition_info->partition_name().empty()) {
            DB_WARNING("p_range_partition_info has no partition_name, log_id: %lu", log_id);
            return -1;
        }
        const std::string& partition_name = p_range_partition_info->partition_name();
        if (partition_names.find(partition_name) != partition_names.end()) {
            DB_WARNING("repeated partition_name: %s, lod_id: %lu", partition_name.c_str(), log_id);
            return -1;
        }
        partition_names.insert(partition_name);
        // 将分区左右端点的col_type设置为分区列类型
        if (partition_utils::set_partition_col_type(partition_col_type, *p_range_partition_info) != 0) {
            DB_WARNING("Fail to set_partition_col_type, log_id: %lu", log_id);
            return -1;
        }
        if (!partition_utils::check_range_partition_info(*p_range_partition_info)) {
            DB_WARNING("Invalid range_partition_info, log_id: %lu", log_id);
            return -1;
        }
    }

    // 不同类型的Range分区各自处理
    std::unordered_map<pb::RangePartitionType, std::vector<pb::RangePartitionInfo>> range_partition_info_map;

    // 将less_value转变为range
    // 将less_value放到range的right_value中
    for (size_t i = 0; i < p_partition_info->range_partition_infos_size(); ++i) {
        pb::RangePartitionInfo* p_range_partition_info = p_partition_info->mutable_range_partition_infos(i);
        if (p_range_partition_info == nullptr) {
            DB_WARNING("p_range_partition_info is nullptr, log_id: %lu", log_id);
            return -1;
        }
        if (p_range_partition_info->has_less_value()) {
            pb::RangePartitionInfo range_partition_info;
            range_partition_info.Swap(p_range_partition_info);
            pb::PartitionRange* p_range = range_partition_info.mutable_range();
            if (p_range == nullptr) {
                DB_WARNING("p_range is nullptr, log_id: %lu", log_id);
                return -1;
            }
            pb::Expr* p_right_value = p_range->mutable_right_value();
            if (p_right_value == nullptr) {
                DB_WARNING("p_right_value is nullptr, log_id: %lu", log_id);
                return -1;
            }
            p_right_value->Swap(range_partition_info.mutable_less_value());
            range_partition_info.clear_less_value();
            range_partition_info_map[range_partition_info.type()].emplace_back(std::move(range_partition_info));
        }
    }

    for (auto& kv : range_partition_info_map) {
        std::vector<pb::RangePartitionInfo>& range_partition_info_vec = kv.second;

        // 按照range的right_value进行排序
        std::sort(range_partition_info_vec.begin(), range_partition_info_vec.end(), partition_utils::RangeComparator());

        // 将前一个range的right_value作为后一个range的left_value
        for (size_t i = 0; i < range_partition_info_vec.size(); ++i) {
            pb::PartitionRange* p_range = range_partition_info_vec[i].mutable_range();
            if (p_range == nullptr) {
                DB_WARNING("p_range is nullptr, log_id: %lu", log_id);
                return -1;
            }
            pb::Expr* p_left_value = p_range->mutable_left_value();
            if (p_left_value == nullptr) {
                DB_WARNING("p_left_value is nullptr, log_id: %lu", log_id);
                return -1;
            }
            if (i == 0) {
                // 处理第一个range的left_value
                std::string partition_str_val;
                if (partition_utils::get_min_partition_value(partition_col_type, partition_str_val) != 0) {
                    DB_WARNING("Fail to get_min_partition_value, log_id: %lu", log_id);
                    return -1;
                }
                if (partition_utils::create_partition_expr(partition_col_type, partition_str_val, *p_left_value) != 0) {
                    DB_WARNING("Fail to create_partition_expr, log_id: %lu", log_id);
                    return -1;
                }
            } else {
                p_left_value->CopyFrom(range_partition_info_vec[i-1].range().right_value());
            }
        }
    }

    // 处理区间方式的range
    for (size_t i = 0; i < p_partition_info->range_partition_infos_size(); ++i) {
        pb::RangePartitionInfo* p_range_partition_info = p_partition_info->mutable_range_partition_infos(i);
        if (p_range_partition_info == nullptr) {
            DB_WARNING("p_range_partition_info is nullptr, log_id: %lu", log_id);
            return -1;
        }
        if (p_range_partition_info->has_range()) {
            pb::RangePartitionInfo range_partition_info;
            range_partition_info.Swap(p_range_partition_info);
            range_partition_info_map[range_partition_info.type()].emplace_back(std::move(range_partition_info));
        }
    }

    p_partition_info->clear_range_partition_infos();
    for (auto& kv : range_partition_info_map) {
        std::vector<pb::RangePartitionInfo>& range_partition_info_vec = kv.second;
        for (auto& range_partition_info : range_partition_info_vec) {
            if (partition_utils::check_partition_overlapped(p_partition_info->range_partition_infos(), range_partition_info)) {
                DB_WARNING("Partition overlapped, range_partition_info: %s", range_partition_info.ShortDebugString().c_str());
                return -1;
            }
            pb::RangePartitionInfo* p_range_partition_info = p_partition_info->add_range_partition_infos();
            if (p_range_partition_info == nullptr) {
                DB_WARNING("p_range_partition_info is nullptr, log_id: %lu", log_id);
                return -1;
            }
            p_range_partition_info->Swap(&range_partition_info);
        }
    }

    // 动态分区处理
    if (pre_process_for_dynamic_partition(request, response, log_id, partition_col_type) != 0) {
        DB_WARNING("Fail to pre_process_for_dynamic_partition");
        return -1;
    }

    // 整体排序
    std::sort(p_partition_info->mutable_range_partition_infos()->pointer_begin(),
              p_partition_info->mutable_range_partition_infos()->pointer_end(),
              partition_utils::PointerRangeComparator());

    int64_t partition_id = -1;
    for (size_t i = 0; i < p_partition_info->range_partition_infos_size(); ++i) {
        partition_id = i;
        p_partition_info->mutable_range_partition_infos(i)->set_partition_id(partition_id);
    }
    p_partition_info->set_max_range_partition_id(partition_id);
    table_info.set_partition_num(p_partition_info->range_partition_infos_size());

    if (table_info.partition_num() == 0) {
        DB_WARNING("mem_schema_pb partition_num is 0");
        return -1;
    }

    return 0;
}

int SchemaManager::pre_process_for_dynamic_partition(const pb::MetaManagerRequest* request,
                                                     pb::MetaManagerResponse* response,
                                                     uint64_t log_id,
                                                     const pb::PrimitiveType partition_col_type) {
    if (request == nullptr ||
            !request->has_table_info() ||
            !request->table_info().has_partition_info() ||
            !request->table_info().partition_info().has_dynamic_partition_attr() ||
            !request->table_info().partition_info().dynamic_partition_attr().enable() ||
            request->table_info().partition_info().type() != pb::PT_RANGE) {
        return 0;
    }

    if (partition_col_type != pb::DATE && partition_col_type != pb::DATETIME) {
        DB_WARNING("Invalid dynamic partition col type, request: %s, log_id: %lu", 
                    request->ShortDebugString().c_str(), log_id);
        return -1;
    }

    // 动态分区属性检查
    pb::DynamicPartitionAttr& dynamic_partition_attr = 
            const_cast<pb::DynamicPartitionAttr&>(request->table_info().partition_info().dynamic_partition_attr());
    if (!partition_utils::check_dynamic_partition_attr(dynamic_partition_attr)) {
        DB_WARNING("Invalid dynamic_partition_attr, log_id: %lu", log_id);
        return -1;
    }

    // 填充动态分区属性默认值
    if (!dynamic_partition_attr.has_start_day_of_month()) {
        dynamic_partition_attr.set_start_day_of_month(partition_utils::START_DAY_OF_MONTH);
    }
    if (!dynamic_partition_attr.has_prefix()) {
        dynamic_partition_attr.set_prefix(partition_utils::PREFIX);
    }

    // 预创建分区
    pb::SchemaInfo& table_info = const_cast<pb::SchemaInfo&>(request->table_info());
    pb::PartitionInfo* p_partition_info = table_info.mutable_partition_info();
    if (p_partition_info == nullptr) {
        DB_WARNING("p_partition_info is nullptr, log_id: %lu", log_id);
        return -1;
    }

    std::set<std::string> partition_names;
    for (const auto& range_partition_info : p_partition_info->range_partition_infos()) {
        partition_names.insert(range_partition_info.partition_name());
    }

    time_t normalized_current_ts;
    TimeUnit unit;
    if (boost::algorithm::iequals(dynamic_partition_attr.time_unit(), "DAY")) {
        unit = TimeUnit::DAY;
        get_current_day_timestamp(normalized_current_ts);
    } else {
        unit = TimeUnit::MONTH;
        get_current_month_timestamp(dynamic_partition_attr.start_day_of_month(), normalized_current_ts);
    }

    for (size_t i = 0; i <= dynamic_partition_attr.end(); ++i) {
        pb::RangePartitionInfo range_partition_info;
        partition_utils::create_dynamic_range_partition_info(dynamic_partition_attr.prefix(), partition_col_type,
                                                             normalized_current_ts, i, unit, range_partition_info);

        std::unordered_set<pb::RangePartitionType> range_partition_type_set;
        for (const auto& type : table_info.partition_info().gen_range_partition_types()) {
            range_partition_type_set.insert(static_cast<pb::RangePartitionType>(type));
        }
        if (range_partition_type_set.empty()) {
            // 兼容旧逻辑
            range_partition_type_set.insert(pb::RPT_DEFAULT);
        }
        for (const pb::RangePartitionType& type : range_partition_type_set) {
            pb::RangePartitionInfo add_range_partition_info;
            add_range_partition_info.CopyFrom(range_partition_info);
            if (type != pb::RPT_DEFAULT) {
                // 兼容旧逻辑
                add_range_partition_info.set_type(type);
                add_range_partition_info.set_partition_name(
                    range_partition_info.partition_name() + "_" + pb::RangePartitionType_Name(type));
            }
            if (partition_utils::check_partition_overlapped(p_partition_info->range_partition_infos(), 
                                                            add_range_partition_info)) {
                // 自动创建如果分区重叠，则不创建该分区
                DB_WARNING("partition overlapping, log_id: %lu", log_id);
                continue;
            }
            const std::string& partition_name = add_range_partition_info.partition_name();
            if (partition_names.find(partition_name) != partition_names.end()) {
                // 自动创建如果名字冲突，则不创建该分区
                DB_WARNING("repeated partition_name: %s, log_id: %lu", partition_name.c_str(), log_id);
                continue;
            }
            partition_names.insert(partition_name);
            pb::RangePartitionInfo* p_range_partition_info = p_partition_info->add_range_partition_infos();
            if (p_range_partition_info == nullptr) {
                DB_WARNING("p_range_partition_info is nullptr, log_id: %lu", log_id);
                return -1;
            }
            p_range_partition_info->Swap(&add_range_partition_info);
        }
    }

    return 0;
}

}//namespace 

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
