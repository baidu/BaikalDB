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
    if (!database_info.has_resource_tag()) {
        std::string resource_tag =  NamespaceManager::get_instance()->get_resource_tag(namespace_id);
        if (resource_tag != "") {
            database_info.set_resource_tag(resource_tag);
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
    
    pb::DataBaseInfo tmp_database_info = _database_info_map[database_id];
    tmp_database_info.set_version(tmp_database_info.version() + 1);
    if (database_info.has_quota()) {
        tmp_database_info.set_quota(database_info.quota());
    }
    if (database_info.has_resource_tag()) {
        tmp_database_info.set_resource_tag(database_info.resource_tag());
    }
    
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
