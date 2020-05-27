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
    case pb::OP_MODIFY_RESOURCE_TAG: 
    case pb::OP_ADD_INDEX:
    case pb::OP_DROP_INDEX: {
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
        if (request->op_type() == pb::OP_UPDATE_DISTS) {
            auto mutable_request = const_cast<pb::MetaManagerRequest*>(request);
            auto ret = whether_dists_legal(mutable_request, response, log_id);
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
    case pb::OP_DELETE_DDLWORK: {
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
    DB_NOTICE("process peer hearbeat, table_exist_time: %ld, ilegal_peer_time: %ld, log_id: %lu",
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

    std::string resource_tag = request->instance_info().resource_tag();
    RegionManager::get_instance()->leader_load_balance(_meta_state_machine->whether_can_decide(), 
            _meta_state_machine->get_load_balance(resource_tag), request, response);
    int64_t leader_balance_time = step_time_cost.get_time();
    DB_NOTICE("store: %s process leader heartbeat, update_status_time: %ld, leader_region_time: %ld,"
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
    bool need_update_schema = false;
    bool need_update_region = false;
    int64_t last_updated_index = request->last_updated_index();
    int64_t applied_index = _meta_state_machine->applied_index();
    if (last_updated_index > 0) {
        need_update_schema = TableManager::get_instance()->check_and_update_incremental(request, response, applied_index);
        need_update_region = RegionManager::get_instance()->check_and_update_incremental(request, response, applied_index);
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
        TableManager::get_instance()->check_add_table(report_table_ids, new_add_region_ids, response);
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
        TableManager::get_instance()->check_add_region(report_table_ids, report_region_ids, response);
    }
    int64_t update_region_time = step_time_cost.get_time();
    DB_NOTICE("process schema info for baikal heartbeat, prepare_time: %ld, update_incremental_time:%ld,"
                " update_table_time: %ld, update_region_time: %ld, log_id: %lu",
                prepare_time, update_incremental_time, update_table_time, update_region_time, log_id);
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

    std::string ddl_prefix = MetaServer::SCHEMA_IDENTIFY;
    ddl_prefix += MetaServer::DDLWORK_IDENTIFY;

    std::string statistics_prefix = MetaServer::SCHEMA_IDENTIFY;
    statistics_prefix += MetaServer::STATISTICS_IDENTIFY;
   
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
        } else if (iter->key().starts_with(ddl_prefix)) {
            ret = TableManager::get_instance()->load_ddl_snapshot(iter->value().ToString());
        } else if (iter->key().starts_with(statistics_prefix)) {
            ret = TableManager::get_instance()->load_statistics_snapshot(iter->value().ToString());
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
    int partition_num = 1;
    if (request->table_info().has_partition_num()) {
        partition_num = request->table_info().partition_num(); 
    }
    std::set<std::string> indexs_name;
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
        indexs_name.insert(index_info.index_name());
    }
    //校验split_key是否有序
    int32_t total_region_count = 0;
    std::set<std::string> split_index_names;
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
        total_region_count += split_key.split_keys_size() + 1;
        split_index_names.insert(split_key.index_name());
    }
    //全局二级索引或者主键索引没有指定split_key
    for (auto& index_info : request->table_info().indexs()) {
        if (index_info.index_type() == pb::I_PRIMARY || index_info.is_global()) {
            if (split_index_names.find(index_info.index_name()) == split_index_names.end()) {
                ++total_region_count;
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
    //用户指定了跨机房部署
    auto ret = whether_dists_legal(mutable_request, response, log_id);
    if (ret != 0) {
        return ret;
    }

    main_logical_room = request->table_info().main_logical_room();
    total_region_count = partition_num * total_region_count;
    std::string resource_tag = request->table_info().resource_tag();
    boost::trim(resource_tag);
    mutable_request->mutable_table_info()->set_resource_tag(resource_tag);
    DB_WARNING("create table should select instance count: %d", total_region_count);
    for (auto i = 0; i < total_region_count; ++i) { 
        std::string instance;
        int ret = ClusterManager::get_instance()->select_instance_rolling(
                    resource_tag,
                    {},
                    main_logical_room,    
                    instance);
        if (ret < 0) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                        "select instance fail", request->op_type(), log_id);
            return -1;
        }
        mutable_request->mutable_table_info()->add_init_store(instance);
    }
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
    int64_t dst_region_id = TableManager::get_instance()->get_next_region_id(
                        table_id, request->region_merge().src_start_key(), 
                        request->region_merge().src_end_key());
    if (dst_region_id <= 0) {
        DB_WARNING("can`t find dst merge region request: %s, src region id:%ld, log_id:%ld",
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
    std::string source_instance = request->region_split().new_instance();
    if (!request->region_split().has_tail_split()
            || !request->region_split().tail_split()) {
        response->mutable_split_response()->set_new_instance(source_instance);
        return 0;
    }
    //从cluster中选择一台实例, 只有尾分裂需要，其他分裂只做本地分裂
    std::string instance;
    auto ret = 0;
    std::string main_logical_room;
    TableManager::get_instance()->get_main_logical_room(table_id, main_logical_room);
    if (_meta_state_machine->whether_can_decide()) {
        ret = ClusterManager::get_instance()->select_instance_min(resource_tag, 
                                                  std::set<std::string>{source_instance},
                                                  //std::set<std::string>{},
                                                  table_id,
                                                  main_logical_room,
                                                  instance);
    } else {
        DB_WARNING("meta state machine can not make decision");
        ret = ClusterManager::get_instance()->select_instance_rolling(resource_tag, 
                                                    std::set<std::string>{source_instance},
                                                    //std::set<std::string>{},
                                                    main_logical_room,
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
int SchemaManager::whether_dists_legal(pb::MetaManagerRequest* request,
                        pb::MetaManagerResponse* response,
                        uint64_t log_id) {
    if (request->table_info().dists_size() == 0) {
        return 0;
    }
    int64_t total_count = 0;
    //检验逻辑机房是否存在
    for (auto& dist : request->table_info().dists()) {
        std::string logical_room = dist.logical_room();
        if (ClusterManager::get_instance()->logical_room_exist(logical_room)) {
            total_count += dist.count();
            continue;
        }
        ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR,
                "logical room not exist, select instance fail",
                request->op_type(), log_id);
        return -1;
    }
    if (request->table_info().main_logical_room().size() == 0) {
        for (auto& dist : request->table_info().dists()) {
            if (dist.count() > 0) {
                request->mutable_table_info()->set_main_logical_room(dist.logical_room());
                break;
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
}//namespace 

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
