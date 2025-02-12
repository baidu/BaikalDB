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

#include "database_manager.h"
#include "meta_util.h"
#include "meta_rocksdb.h"
#include "namespace_manager.h"
#include "privilege_manager.h"

namespace baikaldb {
void DatabaseManager::create_database(const pb::MetaManagerRequest& request, braft::Closure* done) {
    //校验合法性
    auto& database_info = const_cast<pb::DataBaseInfo&>(request.database_info());
    std::string namespace_name = database_info.namespace_name();
    std::string database_name = namespace_name + "\001" + database_info.database();
    int64_t namespace_id = NamespaceManager::get_instance()->get_namespace_id(namespace_name);
    if (namespace_id == 0) {
        DB_WARNING("request namespace:%s not exist", namespace_name.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "namespace not exist");
        return;
    }
    if (_database_id_map.find(database_name) != _database_id_map.end()) {
        DB_WARNING("request database:%s already exist", database_name.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "database already exist");
        return;
    }

    std::vector<std::string> rocksdb_keys;
    std::vector<std::string> rocksdb_values;
    
    //准备database_info信息 
    int64_t tmp_database_id = _max_database_id + 1;
    database_info.set_database_id(tmp_database_id);
    database_info.set_namespace_id(namespace_id);
    
    pb::NameSpaceInfo namespace_info;
    if (NamespaceManager::get_instance()->get_namespace_info(namespace_id, namespace_info) == 0) {
        if (!database_info.has_resource_tag() && namespace_info.resource_tag() != "") {
            database_info.set_resource_tag(namespace_info.resource_tag());
        }
        if (!database_info.has_engine() && namespace_info.has_engine()) {
            database_info.set_engine(namespace_info.engine());
        } 
        if (!database_info.has_charset() && namespace_info.has_charset()) {
            database_info.set_charset(namespace_info.charset());
        }
        if (!database_info.has_byte_size_per_record() && namespace_info.has_byte_size_per_record()) {
            database_info.set_byte_size_per_record(namespace_info.byte_size_per_record());
        }
        if (!database_info.has_replica_num() && namespace_info.has_replica_num()) {
            database_info.set_replica_num(namespace_info.replica_num());
        }  
        if (!database_info.has_region_split_lines() && namespace_info.has_region_split_lines()) {
            database_info.set_region_split_lines(namespace_info.region_split_lines());
        }
        if (database_info.dists().empty() && !namespace_info.dists().empty()) {
            database_info.mutable_dists()->CopyFrom(namespace_info.dists());
        }
        if (!database_info.has_main_logical_room() && namespace_info.has_main_logical_room()) {
            database_info.set_main_logical_room(namespace_info.main_logical_room());
        }
        if (database_info.learner_resource_tags().empty() && !namespace_info.learner_resource_tags().empty()) {
            database_info.mutable_learner_resource_tags()->CopyFrom(namespace_info.learner_resource_tags());
        }
        if (database_info.binlog_infos().empty() && !namespace_info.binlog_infos().empty()) {
            database_info.mutable_binlog_infos()->CopyFrom(namespace_info.binlog_infos());
        }
    }
    database_info.set_version(1);
    
    std::string database_value;
    if (!database_info.SerializeToString(&database_value)) {
        DB_WARNING("request serializeToArray fail, request:%s",request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
        return;
    }
    rocksdb_keys.push_back(construct_database_key(tmp_database_id));
    rocksdb_values.push_back(database_value);

    //持久化分配出去database_id
    std::string max_database_id_value;
    max_database_id_value.append((char*)&tmp_database_id, sizeof(int64_t));
    rocksdb_keys.push_back(construct_max_database_id_key());
    rocksdb_values.push_back(max_database_id_value);
    
    int ret = MetaRocksdb::get_instance()->put_meta_info(rocksdb_keys, rocksdb_values); 
    if (ret < 0) {  
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail"); 
        return;
    }
    //更新内存值
    set_database_info(database_info);
    set_max_database_id(tmp_database_id);
    NamespaceManager::get_instance()->add_database_id(namespace_id, tmp_database_id);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("create database success, request:%s", request.ShortDebugString().c_str());
    // 默认给创建database的user加上权限
    add_privilege_for_database(request);

}

void DatabaseManager::add_privilege_for_database(const pb::MetaManagerRequest& request) {
    if (!request.has_user_privilege()) {
        return;
    }
    pb::MetaManagerRequest add_privilege_request;
    add_privilege_request.set_op_type(pb::OP_ADD_PRIVILEGE);
    add_privilege_request.mutable_user_privilege()->CopyFrom(request.user_privilege());
    PrivilegeManager::get_instance()->add_privilege(add_privilege_request, nullptr);
}

void DatabaseManager::drop_database(const pb::MetaManagerRequest& request, braft::Closure* done) {
    //合法性检查
    auto& database_info = request.database_info();
    std::string namespace_name = database_info.namespace_name();
    std::string database_name = namespace_name + "\001" + database_info.database();
    int64_t namespace_id = NamespaceManager::get_instance()->get_namespace_id(namespace_name);
    if (namespace_id == 0) {
        DB_WARNING("request namespace:%s not exist", namespace_name.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "namespace not exist");
        return;
    }
    if (_database_id_map.find(database_name) == _database_id_map.end()) {
        DB_WARNING("request database:%s not exist", database_name.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "database not exist");
        return;
    }
    int64_t database_id = _database_id_map[database_name];
    if (!_table_ids[database_id].empty()) {
        DB_WARNING("request database:%s has tables", database_name.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "database has table");
        return;
    }

    // 给所有包含该database的user删除该database的权限
    drop_privilege_for_database(request, database_id);

    //持久化数据
    int ret = MetaRocksdb::get_instance()->delete_meta_info(
                std::vector<std::string>{construct_database_key(database_id)});
    if (ret < 0) {
        DB_WARNING("drop datbase:%s to rocksdb fail", database_name.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    //更新内存中database信息
    erase_database_info(database_name);
    //更新内存中namespace信息
    NamespaceManager::get_instance()->delete_database_id(namespace_id, database_id);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("drop database success, request:%s", request.ShortDebugString().c_str());
}

void DatabaseManager::drop_privilege_for_database(const pb::MetaManagerRequest& request, const int64_t database_id) {
    std::unordered_set<std::string> user_set;
    if (PrivilegeManager::get_instance()->get_db_user_set(database_id, user_set) == 0) {
        for (const auto& user : user_set) {
            pb::MetaManagerRequest drop_privilege_request;
            drop_privilege_request.set_op_type(pb::OP_DROP_PRIVILEGE);
            pb::UserPrivilege* pri = drop_privilege_request.mutable_user_privilege();
            pri->set_namespace_name(request.database_info().namespace_name());
            pri->set_username(user);
            pb::PrivilegeDatabase* pri_db = pri->add_privilege_database();
            pri_db->set_database(request.database_info().database());
            PrivilegeManager::get_instance()->drop_privilege(drop_privilege_request, nullptr);
        }
    }
}

void DatabaseManager::modify_database(const pb::MetaManagerRequest& request, braft::Closure* done) {
    auto& database_info = request.database_info();
    std::string namespace_name = database_info.namespace_name();
    std::string database_name = namespace_name + "\001" + database_info.database();
    int64_t namespace_id = NamespaceManager::get_instance()->get_namespace_id(namespace_name);
    if (namespace_id == 0) {
        DB_WARNING("request namespace:%s not exist", namespace_name.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "namespace not exist");
        return;
    }
    if (_database_id_map.find(database_name) == _database_id_map.end()) {
        DB_WARNING("request database:%s not exist", database_name.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "database not exist");
        return;
    }
    int64_t database_id = _database_id_map[database_name];
    
    pb::DataBaseInfo tmp_database_info;
    if (request.is_force_setting()) {
        // 用于删除某个配置项的场景，删除时需要配置其他全部配置项
        tmp_database_info = database_info;
        tmp_database_info.set_database_id(database_id);
        tmp_database_info.set_namespace_id(namespace_id);
    } else {
        tmp_database_info = _database_info_map[database_id];
        if (database_info.has_quota()) {
            tmp_database_info.set_quota(database_info.quota());
        }
        if (database_info.has_resource_tag()) {
            tmp_database_info.set_resource_tag(database_info.resource_tag());
        }
        if (database_info.has_engine()) {
            tmp_database_info.set_engine(database_info.engine());
        }
        if (database_info.has_charset()) {
            tmp_database_info.set_charset(database_info.charset());
        }
        if (database_info.has_byte_size_per_record()) {
            tmp_database_info.set_byte_size_per_record(database_info.byte_size_per_record());
        }
        if (database_info.has_replica_num()) {
            tmp_database_info.set_replica_num(database_info.replica_num());
        }
        if (database_info.has_region_split_lines()) {
            tmp_database_info.set_region_split_lines(database_info.region_split_lines());
        }
        if (!database_info.dists().empty()) {
            tmp_database_info.mutable_dists()->CopyFrom(database_info.dists());
        }
        if (database_info.has_main_logical_room()) {
            tmp_database_info.set_main_logical_room(database_info.main_logical_room());
        }
        if (!database_info.learner_resource_tags().empty()) {
            tmp_database_info.mutable_learner_resource_tags()->CopyFrom(database_info.learner_resource_tags());
        }
        if (!database_info.binlog_infos().empty()) {
            tmp_database_info.mutable_binlog_infos()->CopyFrom(database_info.binlog_infos());
        }
        if (database_info.has_partition_info_str()) {
            tmp_database_info.set_partition_info_str(database_info.partition_info_str());
        }
    }
    tmp_database_info.set_version(_database_info_map[database_id].version() + 1);
    std::string database_value;
    if (!tmp_database_info.SerializeToString(&database_value)) {
        DB_WARNING("request serializeToArray fail, request:%s",request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
        return;
    }
    int ret = MetaRocksdb::get_instance()->put_meta_info(construct_database_key(database_id), database_value);
    if (ret < 0) { 
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    //更新内存值
    set_database_info(tmp_database_info);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("modify database success, request:%s", request.ShortDebugString().c_str());
}

int DatabaseManager::load_database_snapshot(const std::string& value) {
    pb::DataBaseInfo database_pb;
    if (!database_pb.ParseFromString(value)) {
        DB_FATAL("parse from pb fail when load database snapshot, key:%s", value.c_str());
        return -1;
    }
    DB_WARNING("database snapshot:%s", database_pb.ShortDebugString().c_str());
    set_database_info(database_pb);
    //更新内存中namespace的值
    NamespaceManager::get_instance()->add_database_id(
                database_pb.namespace_id(), 
                database_pb.database_id());
    return 0;
}
void DatabaseManager::process_baikal_heartbeat(const pb::BaikalHeartBeatRequest* request, 
                                               pb::BaikalHeartBeatResponse* response) {
    BAIDU_SCOPED_LOCK(_database_mutex);
    for (auto& db_info : _database_info_map) {
        auto db = response->add_db_info();    
        *db = db_info.second;
    }
}

}//namespace 

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
