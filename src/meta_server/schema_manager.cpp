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

#include "schema_manager.h"
#include <boost/algorithm/string.hpp>
#include "cluster_manager.h"
#include "table_manager.h"
#include "database_manager.h"
#include "namespace_manager.h"
#include "region_manager.h"
#include "meta_util.h"
#include "rocks_wrapper.h"

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
    switch (request->op_type()) {
    case pb::OP_CREATE_NAMESPACE:
    case pb::OP_MODIFY_NAMESPACE:
    case pb::OP_DROP_NAMESPACE: {
        if (!request->has_namespace_info()) { 
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, 
                    "no namespace_info", request->op_type(), log_id);
            return;
        } 
        if (request->op_type() == pb::OP_MODIFY_NAMESPACE
                && !request->namespace_info().has_quota()) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, 
                    "no namespace_quota", request->op_type(), log_id);
            return;
        }
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
        if (request->op_type() == pb::OP_MODIFY_DATABASE
                && !request->database_info().has_quota()) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, 
                    "no databasepace quota", request->op_type(), log_id);
            return;
        }
        _meta_state_machine->process(controller, request, response, done_guard.release());
        return;
    }
    case pb::OP_CREATE_TABLE:
    case pb::OP_RENAME_TABLE:
    case pb::OP_DROP_TABLE: 
    case pb::OP_ADD_FIELD:
    case pb::OP_DROP_FIELD:
    case pb::OP_MODIFY_FIELD:
    case pb::OP_RENAME_FIELD:
    case pb::OP_UPDATE_BYTE_SIZE: {
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
        if (request->op_type() == pb::OP_RENAME_TABLE
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
        if (!request->has_region_info()) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                    "no split region info", request->op_type(), log_id);
            return;
        }
        _meta_state_machine->process(controller, request, response, done_guard.release());
        return;
    }
    case pb::OP_RESTORE_REGION: {
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
    int64_t table_exist_time =step_time_cost.get_time();
    step_time_cost.reset();
    RegionManager::get_instance()->check_whether_illegal_peer(request, response);
    int64_t illegal_peer_time = step_time_cost.get_time();
    step_time_cost.get_time();
    SELF_TRACE("process peer hearbeat, table_exist_time: %ld, ilegal_peer_time: %ld, log_id: %lu",
                table_exist_time, illegal_peer_time, log_id);
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
    TimeCost step_time_cost;
    RegionManager::get_instance()->update_leader_status(request);
    int64_t update_status_time = step_time_cost.get_time();
    step_time_cost.reset();

    RegionManager::get_instance()->leader_heartbeat_for_region(request, response);
    int64_t leader_region_time = step_time_cost.get_time();
    step_time_cost.reset();

    RegionManager::get_instance()->leader_load_balance(_meta_state_machine->whether_can_decide(), 
            _meta_state_machine->get_close_load_balance(), request, response);
    int64_t leader_balance_time = step_time_cost.get_time();
    SELF_TRACE("process leader heartbeat, update_status_time: %ld, leader_region_time: %ld,"
                " leader_balance_time: %ld, log_id: %lu",
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
    for (auto& schema_heart_beat : request->schema_infos()) {
        int64_t table_id = schema_heart_beat.table_id();
        report_table_ids.insert(table_id);
        for (auto& region_info : schema_heart_beat.regions()) {
            report_region_ids[table_id].insert(region_info.region_id());
        }
    }
    int64_t prepare_time = step_time_cost.get_time();
    step_time_cost.reset(); 
    //判断上报的表是否已经更新或删除
    TableManager::get_instance()->check_update_or_drop_table(request, response);
    int64_t update_table_time = step_time_cost.get_time();
    step_time_cost.reset();
    //判断是否有新增的表没有下推给baikaldb
    //std::set<int64_t> new_add_region_ids;
    TableManager::get_instance()->check_add_table(report_table_ids, response);
    int64_t add_table_time = step_time_cost.get_time();
    step_time_cost.reset();
    //RegionManager::get_instance()->add_region_info(new_add_region_ids, response);

    //判断上报的region是否已经更新或删除
    RegionManager::get_instance()->check_update_region(request, response);
    int64_t update_region_time = step_time_cost.get_time();
    step_time_cost.reset();
    //判断是否有新增的region没有下推到baikaldb
    TableManager::get_instance()->check_add_region(report_table_ids, report_region_ids, response);
    int64_t add_region_time = step_time_cost.get_time();
    SELF_TRACE("process schema info for baikal heartbeat,"
                " prepare_time: %ld, update_table_time: %ld, add_table_time: %ld,"
                " update_region_time: %ld, add_region_time: %ld, log_id: %lu",
                prepare_time, update_table_time, add_table_time, 
                update_region_time, add_region_time, log_id);
}

int SchemaManager::check_and_get_for_privilege(pb::UserPrivilege& user_privilege) {
    std::string namespace_name = user_privilege.namespace_name();
    int64_t namespace_id = NamespaceManager::get_instance()->get_namespace_id(namespace_name);
    if (namespace_id == 0) {
        DB_FATAL("namespace not exist, namespace:%s, request：%s", 
                        namespace_name.c_str(),
                        user_privilege.ShortDebugString().c_str());
        return -1;
    }
    user_privilege.set_namespace_id(namespace_id);
    for (auto& pri_base : *user_privilege.mutable_privilege_database()) {
        std::string base_name = namespace_name + "\001" + pri_base.database();
        int64_t database_id = DatabaseManager::get_instance()->get_database_id(base_name); 
        if (database_id == 0) {
            DB_FATAL("database:%s not exist, namespace:%s, request：%s",
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
            DB_FATAL("database:%s not exist, namespace:%s, request：%s",
                            base_name.c_str(), namespace_name.c_str(),
                            user_privilege.ShortDebugString().c_str());
            return -1;
        }
        int64_t table_id = TableManager::get_instance()->get_table_id(table_name); 
        if (table_id == 0) {
            DB_FATAL("table_name:%s not exist, database:%s namespace:%s, request：%s",
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
    return 0;
}
int SchemaManager::pre_process_for_create_table(const pb::MetaManagerRequest* request,
            pb::MetaManagerResponse* response,
            uint64_t log_id) {
    int partition_num = 1;
    if (request->table_info().has_partition_num()) {
        partition_num = request->table_info().partition_num(); 
    }
    //校验split_key是否有序
    if (request->table_info().split_keys_size() > 1) { 
        for (auto i = 1; i < request->table_info().split_keys_size(); ++i) {
            if (request->table_info().split_keys(i) <= request->table_info().split_keys(i - 1)) {
                ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                            "split key not sorted", request->op_type(), log_id);
                return -1;
            }
        }
    }
    std::vector<std::string> new_instances;
    int count = partition_num * (request->table_info().split_keys_size() + 1);
    std::string resource_tag = request->table_info().resource_tag();
    boost::trim(resource_tag);
    for (auto i = 0; i < count; ++i) { 
        std::string instance;
        int ret = ClusterManager::get_instance()->select_instance_rolling(
                    resource_tag,
                    {},
                    instance);
        if (ret < 0) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                        "select instance fail", request->op_type(), log_id);
            return -1;
        }
        new_instances.push_back(instance);
    }
    auto mutable_request = const_cast<pb::MetaManagerRequest*>(request);
    for (auto& new_instance : new_instances) {
        mutable_request->mutable_table_info()->add_init_store(new_instance);
    }
    mutable_request->mutable_table_info()->set_resource_tag(resource_tag);
    return 0;
}
int SchemaManager::pre_process_for_split_region(const pb::MetaManagerRequest* request,
            pb::MetaManagerResponse* response,
            uint64_t log_id) { 
    int64_t region_id = request->region_split().region_id();
    auto ptr_region = RegionManager::get_instance()->get_region_info(region_id);
    if (ptr_region == nullptr) {
        ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                            "table id not exist", request->op_type(), log_id);
        return -1;
    }
    if (ptr_region->peers_size() != ptr_region->replica_num()) {
        ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                            "region not stable, cannot split", 
                            request->op_type(), 
                            log_id);
        DB_WARNING("region cannot split, region not stable, request: %s", 
                    request->ShortDebugString().c_str());
        return -1;
    }
    int64_t table_id = 0;
    if (request->region_split().has_table_id()) {
        table_id = request->region_split().table_id();
    } else {
        table_id = ptr_region->table_id();
    }
    std::string resource_tag;
    if (request->region_split().has_resource_tag()) {
        resource_tag = request->region_split().resource_tag();
    } else {
        auto ret = TableManager::get_instance()->get_resource_tag(table_id, resource_tag);
        if (ret < 0) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                                "table id not exist", request->op_type(), log_id);
            return -1;
        }
    }
    std::string source_instance = request->region_split().new_instance();
    response->mutable_split_response()->set_new_instance(source_instance);
    if (!request->region_split().has_tail_split()
            || !request->region_split().tail_split()) {
        return 0;
    }
    //从cluster中选择一台实例, 只有尾分裂需要，其他分裂只做本地分裂
    std::string instance;
    auto ret = 0;
    if (_meta_state_machine->whether_can_decide()) {
        ret = ClusterManager::get_instance()->select_instance_min(resource_tag, 
                                                  std::set<std::string>{source_instance},
                                                  table_id,
                                                  instance);
    } else {
        DB_WARNING("meta state machine can not make decision");
        ret = ClusterManager::get_instance()->select_instance_rolling(resource_tag, 
                                                    std::set<std::string>{source_instance},
                                                    instance);
    }
    if (ret < 0) {
        ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                    "select instance fail", request->op_type(), log_id);
        return -1;
    }
    response->mutable_split_response()->set_new_instance(instance);
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

}//namespace 

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
