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

#include "namespace_manager.h"
#include "meta_rocksdb.h"
#include "meta_util.h"

namespace baikaldb {
void NamespaceManager::create_namespace(
            const pb::MetaManagerRequest& request, 
            braft::Closure* done) {
    auto& namespace_info = const_cast<pb::NameSpaceInfo&>(request.namespace_info());
    std::string namespace_name = namespace_info.namespace_name();
    if (_namespace_id_map.find(namespace_name) != _namespace_id_map.end()) {
        DB_WARNING("request namespace:%s has been existed", namespace_name.c_str());
        if (namespace_info.if_exist()) {
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "namespace already exist");
        } else {
            IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
        }
        return;
    }
    std::vector<std::string> rocksdb_keys;
    std::vector<std::string> rocksdb_values;

    //准备namespace信息
    int64_t tmp_namespcae_id = _max_namespace_id + 1;
    namespace_info.set_namespace_id(tmp_namespcae_id);
    namespace_info.set_version(1);

    std::string namespace_value;
    if (!namespace_info.SerializeToString(&namespace_value)) {
        DB_WARNING("request serializeToArray fail, request:%s",request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
        return;
    }
    rocksdb_keys.push_back(construct_namespace_key(tmp_namespcae_id));
    rocksdb_values.push_back(namespace_value);

    //持久化分配出去id的信息
    std::string max_namespace_id_value;
    max_namespace_id_value.append((char*)&tmp_namespcae_id, sizeof(int64_t));
    rocksdb_keys.push_back(construct_max_namespace_id_key());
    rocksdb_values.push_back(max_namespace_id_value);
   
    int ret = MetaRocksdb::get_instance()->put_meta_info(rocksdb_keys, rocksdb_values);
    if (ret < 0) {
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    //更新内存值
    set_namespace_info(namespace_info);
    set_max_namespace_id(tmp_namespcae_id);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("create namespace success, request:%s", request.ShortDebugString().c_str());
} 

void NamespaceManager::drop_namespace(const pb::MetaManagerRequest& request, braft::Closure* done) {
    auto& namespace_info = request.namespace_info();
    std::string namespace_name = namespace_info.namespace_name();
    if (_namespace_id_map.find(namespace_name) == _namespace_id_map.end()) {
        DB_WARNING("request namespace:%s not exist", namespace_name.c_str());
        if (!namespace_info.if_exist()) {
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "namespace not exist");
        } else {
            IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
        }
        return;
    }

    //判断namespace下是否存在database，存在则不能删除namespace
    int64_t namespace_id = _namespace_id_map[namespace_name];
    if (!_database_ids[namespace_id].empty()) {
        DB_WARNING("request namespace:%s has database", namespace_name.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "namespace has table");
        return;
    }

    //持久化删除数据
    std::string namespace_key = construct_namespace_key(namespace_id);
    
    int ret = MetaRocksdb::get_instance()->delete_meta_info(std::vector<std::string>{namespace_key});
    if (ret < 0) {
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    
    //更新内存值
    erase_namespace_info(namespace_name);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("drop namespace success, request:%s", request.ShortDebugString().c_str());
}

void NamespaceManager::modify_namespace(const pb::MetaManagerRequest& request, braft::Closure* done) {
    auto& namespace_info = request.namespace_info();
    std::string namespace_name = namespace_info.namespace_name();
    if (_namespace_id_map.find(namespace_name) == _namespace_id_map.end()) {
        DB_WARNING("request namespace:%s not exist", namespace_name.c_str());
        if (!namespace_info.if_exist()) {
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "namespace not exist");
        } else {
            IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
        }
        return;
    }
    //目前支持改quota, resource_tag
    int64_t namespace_id = _namespace_id_map[namespace_name];
    pb::NameSpaceInfo tmp_info = _namespace_info_map[namespace_id];
    if (namespace_info.has_quota()) {
        tmp_info.set_quota(namespace_info.quota());
    }
    if (namespace_info.has_resource_tag()) {
        tmp_info.set_resource_tag(namespace_info.resource_tag());
    }
    if (namespace_info.has_engine()) {
        tmp_info.set_engine(namespace_info.engine());
    }
    if (namespace_info.has_charset()) {
        tmp_info.set_charset(namespace_info.charset());
    }
    if (namespace_info.has_byte_size_per_record()) {
        tmp_info.set_byte_size_per_record(namespace_info.byte_size_per_record());
    }
    if (namespace_info.has_replica_num()) {
        tmp_info.set_replica_num(namespace_info.replica_num());
    }
    if (namespace_info.has_region_split_lines()) {
        tmp_info.set_region_split_lines(namespace_info.region_split_lines());
    }
    tmp_info.set_version(tmp_info.version() + 1);

    //持久化新的namespace信息
    std::string namespace_value;
    if (!tmp_info.SerializeToString(&namespace_value)) {
        DB_WARNING("request serializeToArray fail, request:%s",request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
        return;
    }
    
    int ret = MetaRocksdb::get_instance()->put_meta_info(construct_namespace_key(namespace_id), namespace_value);
    if (ret < 0) {
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }

    //更新内存值
    set_namespace_info(tmp_info);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("modify namespace success, request:%s", request.ShortDebugString().c_str());
}

int NamespaceManager::load_namespace_snapshot(const std::string& value) {
    pb::NameSpaceInfo namespace_pb;
    if (!namespace_pb.ParseFromString(value)) {
        DB_FATAL("parse from pb fail when load namespace snapshot, value: %s", value.c_str());
        return -1;
    }
    DB_WARNING("namespace snapshot:%s", namespace_pb.ShortDebugString().c_str());
    set_namespace_info(namespace_pb);
    return 0;
}

}//namespace 

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
