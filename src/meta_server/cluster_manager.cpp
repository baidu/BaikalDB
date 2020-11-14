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

#include "cluster_manager.h"
#include "table_manager.h"
#include <boost/algorithm/string.hpp>  
#include <gflags/gflags.h>
#include "meta_server.h"
#include "region_manager.h"
#include "meta_util.h"
#include "meta_rocksdb.h"

namespace baikaldb {
DEFINE_int32(migrate_percent, 60, "migrate percent. default:60%");
DEFINE_int32(error_judge_percent, 10, "error judge percen. default:10");
DEFINE_int32(error_judge_number, 3, "error judge number. default:3");
DEFINE_int32(disk_used_percent, 80, "disk used percent. default:80%");

DECLARE_int64(store_heart_beat_interval_us);
DECLARE_int32(store_dead_interval_times);
DECLARE_int32(store_faulty_interval_times);
DECLARE_string(default_logical_room);
DECLARE_string(default_physical_room);

// add peer时选择策略，按ip区分可以保证peers落在不同IP上，避免单机多实例主机故障的多副本丢失
// 但在机器数较少时会造成peer分配不均，甚至无法选出足够的peers的问题。
// 假设机器数=host_num，每机器部署store实例数=n，store_max_peer = region_num / n
// 集群总peer数为 region_num * replica_num, store_avg_peer = region_num * replica_num / (host_num * n)
// store_max_peer >= store_avg_peer， 即host_num >= replica_num时能同时满足host与store均衡，总结：
// host_num >= replica_num时： 应选by_ip，能同时满足host与store均衡
// store_num >= replica_num > host_num时：by_ip，则会失败，应该扩容机器；by_ip_port，则只能满足peer均衡，不能IP均衡
// replica_num > store_num时： 会失败，应该扩容store
// 若单机单实例：by_ip 效果等价于 by_ip_port
// 若单机多实例：则 host_num >= replica_num时， 应选择by_ip，否则应选择by_ip_port
DEFINE_bool(peer_balance_by_ip, false, "default by ip:port");

void ClusterManager::process_cluster_info(google::protobuf::RpcController* controller, 
                                          const pb::MetaManagerRequest* request, 
                                          pb::MetaManagerResponse* response, 
                                          google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint64_t log_id = 0;
    if (controller != NULL) {
        brpc::Controller* cntl = 
                static_cast<brpc::Controller*>(controller);
        if (cntl->has_log_id()) {
            log_id = cntl->log_id();
        }
    }
    switch (request->op_type()) {
    case pb::OP_ADD_LOGICAL:
    case pb::OP_DROP_LOGICAL: {
        if (!request->has_logical_rooms()) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, "no logic room", request->op_type(), log_id);
            return;
        }
        _meta_state_machine->process(controller, request, response, done_guard.release());
        return;
    }
    case pb::OP_ADD_PHYSICAL:
    case pb::OP_DROP_PHYSICAL:{
        if (!request->has_physical_rooms()) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, "no physical room", request->op_type(), log_id);
            return;
        }
        _meta_state_machine->process(controller, request, response, done_guard.release());
        return;
    }
    case pb::OP_ADD_INSTANCE: 
    case pb::OP_DROP_INSTANCE:
    case pb::OP_UPDATE_INSTANCE: {
        if (!request->has_instance()) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, "no instance info", request->op_type(), log_id);
            return;
        }
        _meta_state_machine->process(controller, request, response, done_guard.release());
        return;
    }
    case pb::OP_MOVE_PHYSICAL: {
        if (!request->has_move_physical_request()) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, "no move physical request", request->op_type(), log_id);
            return;
        }
        _meta_state_machine->process(controller, request, response, done_guard.release());
        return;
    }
    default:{
        ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, "wrong op_type", request->op_type(), log_id);
        return;
    }
    }
}

void ClusterManager::add_logical(const pb::MetaManagerRequest& request, braft::Closure* done) {
    pb::LogicalRoom pb_logical;
    //校验合法性,构造rocksdb里的value
    for (auto add_room : request.logical_rooms().logical_rooms()) {
        if (_logical_physical_map.count(add_room)) {
            DB_WARNING("request logical room:%s has been existed", add_room.c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "phyical room already exist");
            return;
        }
        pb_logical.add_logical_rooms(add_room);
    }
    for (auto& already_room : _logical_physical_map) {
        pb_logical.add_logical_rooms(already_room.first);
    }
    // 构造 rocksdb的key和value
    std::string value;
    if (!pb_logical.SerializeToString(&value)) {
        DB_WARNING("request serializeToArray fail, request:%s",
                    request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
        return;
    }
    auto ret = MetaRocksdb::get_instance()->put_meta_info(construct_logical_key(), value);
    if (ret < 0) {
        DB_FATAL("add phyical room:%s to rocksdb fail", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    //更新内存值
    BAIDU_SCOPED_LOCK(_physical_mutex);
    for (auto add_room : request.logical_rooms().logical_rooms()) {
        _logical_physical_map[add_room] = std::set<std::string>();
    }
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("add logical room success, request:%s", request.ShortDebugString().c_str());
}

void ClusterManager::drop_logical(const pb::MetaManagerRequest& request, braft::Closure* done) {
    auto tmp_map = _logical_physical_map;
    for (auto drop_room : request.logical_rooms().logical_rooms()) {
        if (!_logical_physical_map.count(drop_room)) {
            DB_WARNING("request logical room:%s not existed", drop_room.c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "logical room not exist");
            return;
        }
        if (_logical_physical_map[drop_room].size() != 0) {
            DB_WARNING("request logical room:%s has physical room", drop_room.c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "logical has physical");
            return;
        }
        tmp_map.erase(drop_room);
    }
    std::vector<std::string> drop_logical_keys;
    for (auto drop_room : request.logical_rooms().logical_rooms()) {
        drop_logical_keys.push_back(construct_physical_key(drop_room));
    }

    pb::LogicalRoom pb_logical; 
    for (auto& logical_room : tmp_map) {
        pb_logical.add_logical_rooms(logical_room.first); 
    }
    std::string value;
    if (!pb_logical.SerializeToString(&value)) {
        DB_WARNING("request serializeToArray fail, request:%s",request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
        return;
    }
    auto ret = MetaRocksdb::get_instance()->write_meta_info(
                std::vector<std::string>{construct_logical_key()},
                std::vector<std::string>{value},
                drop_logical_keys);
    if (ret < 0) {
        DB_WARNING("drop logical room:%s to rocksdb fail", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    //更新内存值
    BAIDU_SCOPED_LOCK(_physical_mutex);
    _logical_physical_map = tmp_map;
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("drop logical room success, request:%s", request.ShortDebugString().c_str());
}

void ClusterManager::add_physical(const pb::MetaManagerRequest& request, braft::Closure* done) {
    auto& logical_physical_room = request.physical_rooms();
    std::string logical_room = logical_physical_room.logical_room();
    //逻辑机房不存在则报错，需要去添加逻辑机房
    if (!_logical_physical_map.count(logical_room)) {
        DB_WARNING("logical room:%s not exist", logical_room.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "logical not exist");
        return;
    }
    pb::PhysicalRoom pb_physical;
    pb_physical.set_logical_room(logical_room);
    for (auto& add_room : logical_physical_room.physical_rooms()) {
        if (_physical_info.find(add_room) != _physical_info.end()) {
            DB_WARNING("physical room:%s already exist", add_room.c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "physical already exist");
            return;
        }
        pb_physical.add_physical_rooms(add_room);
    }
    for (auto& already_room : _logical_physical_map[logical_room]) {
        pb_physical.add_physical_rooms(already_room);
    }
    //写入rocksdb中
    std::string value;
    if (!pb_physical.SerializeToString(&value)) {
        DB_WARNING("request serializeToArray fail, request: %s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
        return;
    }
    auto ret = MetaRocksdb::get_instance()->put_meta_info(construct_physical_key(logical_room), value);
    if (ret < 0) {
        DB_WARNING("add logical room: %s to rocksdb fail",
                       request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    //更新内存值
    {
        BAIDU_SCOPED_LOCK(_physical_mutex);
        for (auto& add_room : logical_physical_room.physical_rooms()) { 
            _logical_physical_map[logical_room].insert(add_room);
            _physical_info[add_room] = logical_room;
        }
    }
    {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        for (auto& add_room : logical_physical_room.physical_rooms()) {
            _physical_instance_map[add_room] = std::set<std::string>();
        }
    }
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("add physical room success, request:%s", request.ShortDebugString().c_str());
}

void ClusterManager::drop_physical(const pb::MetaManagerRequest& request, braft::Closure* done) {
    auto& logical_physical_room = request.physical_rooms();
    std::string logical_room = logical_physical_room.logical_room();
    //逻辑机房不存在则报错
    if (!_logical_physical_map.count(logical_room)) {
        DB_WARNING("logical room:%s not exist", logical_room.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "logical not exist");
        return;
    }
    auto tmp_physical_rooms = _logical_physical_map[logical_room];
    for (auto drop_room : logical_physical_room.physical_rooms()) {
        //物理机房不存在
        if (_physical_info.find(drop_room) == _physical_info.end()) {
            DB_WARNING("physical room:%s not exist", drop_room.c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "physical not exist");
            return;
        }
        if (_physical_info[drop_room] != logical_room) {
            DB_WARNING("physical room:%s not belong to logical_room:%s",
                        drop_room.c_str(), logical_room.c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "physical not exist");
            return;
        }
        //物理机房下不能有实例
        if (_physical_instance_map.count(drop_room) > 0 
                && _physical_instance_map[drop_room].size() != 0) {
            DB_WARNING("physical room:%s has instance", drop_room.c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "physical has instance");
            return;
        }
        tmp_physical_rooms.erase(drop_room);
    }
    pb::PhysicalRoom pb_physical;
    pb_physical.set_logical_room(logical_room);
    for (auto& left_room : tmp_physical_rooms) {
        pb_physical.add_physical_rooms(left_room);
    }
    //写入rocksdb中
    std::string value;
    if (!pb_physical.SerializeToString(&value)) {
        DB_WARNING("request serializeToArray fail, request:%s",request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
        return;
    }
    // write date to rocksdb
    auto ret = MetaRocksdb::get_instance()->put_meta_info(construct_physical_key(logical_room), value);
    if (ret < 0) {
        DB_WARNING("add phyical room:%s to rocksdb fail",
                       request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    //更新内存值
    {
        BAIDU_SCOPED_LOCK(_physical_mutex);
        for (auto& drop_room : logical_physical_room.physical_rooms()) {
            _physical_info.erase(drop_room);
            _logical_physical_map[logical_room].erase(drop_room);
        }
    }
    {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        for (auto& drop_room : logical_physical_room.physical_rooms()) {
            _physical_instance_map.erase(drop_room);
        }
    }
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("drop physical room success, request:%s", request.ShortDebugString().c_str());
}

//MetaServer内部自己调用自己的这个接口，也可以作为外部接口使用
void ClusterManager::add_instance(const pb::MetaManagerRequest& request, braft::Closure* done) {
    auto& instance_info = const_cast<pb::InstanceInfo&>(request.instance());
    std::string address = instance_info.address();
    std::string physical_room = instance_info.physical_room();
    if (!instance_info.has_physical_room() || instance_info.physical_room().size() == 0) {
        auto ret = get_physical_room(address, physical_room);
        if (ret < 0) {
             DB_WARNING("get physical room fail when add instance, instance:%s", address.c_str());
             IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "instance to hostname fail");
             return;
        }
    }
    instance_info.set_physical_room(physical_room);
    if (_physical_info.find(physical_room) != _physical_info.end()) {
        instance_info.set_logical_room(_physical_info[physical_room]);
    } else {
        DB_FATAL("get logical room for physical room: %s fail", physical_room.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "physical to logical fail");
        return;
    }
    //合法性检查
    //物理机房不存在
    if (_physical_info.find(physical_room) == _physical_info.end()) {
        DB_WARNING("physical room:%s not exist, instance:%s", 
                    physical_room.c_str(),
                    address.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "physical room not exist");
        return;
    }
    //实例已经存在
    if (_instance_info.find(address) != _instance_info.end()) {
        DB_WARNING("instance:%s has already exist", address.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "instance already exist");
        return;
    }
    //写入rocksdb中
    std::string value;
    if (!instance_info.SerializeToString(&value)) {
        DB_WARNING("request serializeToArray fail, request:%s",request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
        return;
    }
    // write date to rocksdb
    auto ret = MetaRocksdb::get_instance()->put_meta_info(construct_instance_key(address), value);
    if (ret < 0) {
        DB_WARNING("add instance:%s to rocksdb fail", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    //更新内存值
    BAIDU_SCOPED_LOCK(_instance_mutex);
    _instance_physical_map[address] = physical_room;
    _physical_instance_map[physical_room].insert(address);
    Instance instance_mem(instance_info);
    _instance_info[address] = instance_mem;
    if (_instance_regions_map.find(address) == _instance_regions_map.end()) {
        _instance_regions_map[address] = std::unordered_map<int64_t, std::vector<int64_t>>{};
    }
    if (_instance_regions_count_map.find(address) == _instance_regions_count_map.end()) {
        _instance_regions_count_map[address] = std::unordered_map<int64_t, int64_t>{};
    }
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("add instance success, request:%s", request.ShortDebugString().c_str());
}

void ClusterManager::drop_instance(const pb::MetaManagerRequest& request, braft::Closure* done) {
    std::string address = request.instance().address();
    //合法性检查
    //实例不存在
    if (_instance_info.find(address) == _instance_info.end()) {
        DB_WARNING("instance:%s not exist", address.c_str());
        //IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "instance not exist");
        IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
        return;
    }
    std::string physical_room = _instance_info[address].physical_room;
    
    // write date to rocksdb
    auto ret = MetaRocksdb::get_instance()->delete_meta_info(
                std::vector<std::string>{construct_instance_key(address)});
    if (ret < 0) {
        DB_WARNING("drop instance:%s to rocksdb fail, err_mes:%s", request.ShortDebugString().c_str()); 
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    //更新内存值
    BAIDU_SCOPED_LOCK(_instance_mutex);
    _instance_physical_map.erase(address);
    _instance_info.erase(address);
    _instance_regions_map.erase(address);
    _instance_regions_count_map.erase(address);
    if (_physical_instance_map.find(physical_room) != _physical_instance_map.end()) {
        _physical_instance_map[physical_room].erase(address);
    }
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("drop instance success, request:%s", request.ShortDebugString().c_str());
}

void ClusterManager::update_instance(const pb::MetaManagerRequest& request, braft::Closure* done) {
    std::string address = request.instance().address();
    //实例不存在
    if (_instance_info.find(address) == _instance_info.end()) {
        DB_WARNING("instance:%s not exist", address.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "instance not exist");
        return;
    }
    auto& instance_info = const_cast<pb::InstanceInfo&>(request.instance());
    if (!instance_info.has_capacity()) {
        instance_info.set_capacity(_instance_info[address].capacity);
    }
    if (!instance_info.has_used_size()) {
        instance_info.set_used_size(_instance_info[address].used_size);
    }
    if (!instance_info.has_resource_tag()) {
        instance_info.set_resource_tag(_instance_info[address].resource_tag);
    }
    //这两个信息不允许改
    instance_info.set_status(_instance_info[address].instance_status.state);
    instance_info.set_physical_room(_instance_info[address].physical_room);
    instance_info.set_logical_room(_physical_info[_instance_info[address].physical_room]);
    std::string value;
    if (!instance_info.SerializeToString(&value)) {
        DB_WARNING("request serializeToArray fail, request:%s",request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
        return;
    }
    // write date to rocksdb
    auto ret = MetaRocksdb::get_instance()->put_meta_info(construct_instance_key(address), value);
    if (ret < 0) {
        DB_WARNING("add phyical room:%s to rocksdb fail", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    
    BAIDU_SCOPED_LOCK(_instance_mutex);
    _instance_info[address].capacity =  instance_info.capacity();
    _instance_info[address].used_size = instance_info.used_size(); 
    _instance_info[address].resource_tag = instance_info.resource_tag();
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("modify tage success, request:%s", request.ShortDebugString().c_str());
}

void ClusterManager::move_physical(const pb::MetaManagerRequest& request, braft::Closure* done) {
    std::string physical_room = request.move_physical_request().physical_room();
    std::string new_logical_room = request.move_physical_request().new_logical_room();
    std::string old_logical_room = request.move_physical_request().old_logical_room();
    if (!_logical_physical_map.count(new_logical_room)) {
        DB_WARNING("new logical room:%s not exist", new_logical_room.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "logical not exist");
        return;
    }
    if (!_logical_physical_map.count(old_logical_room)) {
        DB_WARNING("old logical room:%s not exist", old_logical_room.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "logical not exist");
        return;
    }
    if (!_physical_info.count(physical_room)) {
        DB_WARNING("physical room:%s not exist", physical_room.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "physical room not exist");
        return;
    }
    if (_physical_info[physical_room] != old_logical_room) {
        DB_WARNING("physical room:%s not belong to old logical room:%s", 
                    physical_room.c_str(), old_logical_room.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, 
                             "physical room not belong to old logical room");
        return;
    }
    std::vector<std::string> put_keys;
    std::vector<std::string> put_values;
    pb::PhysicalRoom old_physical_pb;
    old_physical_pb.set_logical_room(old_logical_room);
    for (auto& physical : _logical_physical_map[old_logical_room]) {
        if (physical != physical_room) {
            old_physical_pb.add_physical_rooms(physical);
        }
    }
    std::string old_physical_value;
    if (!old_physical_pb.SerializeToString(&old_physical_value)) {
        DB_WARNING("request serializeToArray fail, request:%s",request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
        return;
    }
    put_keys.push_back(construct_physical_key(old_logical_room));
    put_values.push_back(old_physical_value);

    pb::PhysicalRoom new_physical_pb;
    new_physical_pb.set_logical_room(new_logical_room);
    for (auto& physical : _logical_physical_map[new_logical_room]) {
        new_physical_pb.add_physical_rooms(physical);
    }
    new_physical_pb.add_physical_rooms(physical_room);
    std::string new_physical_value;
    if (!new_physical_pb.SerializeToString(&new_physical_value)) {
        DB_WARNING("request serializeToArray fail, request:%s",request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
        return;
    }
    put_keys.push_back(construct_physical_key(new_logical_room));
    put_values.push_back(new_physical_value);
    
    auto ret = MetaRocksdb::get_instance()->put_meta_info(put_keys, put_values);
    if (ret < 0) {
        DB_WARNING("logic move room:%s to rocksdb fail", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    //更新内存信息
    BAIDU_SCOPED_LOCK(_physical_mutex);
    _physical_info[physical_room] = new_logical_room;
    _logical_physical_map[new_logical_room].insert(physical_room);
    _logical_physical_map[old_logical_room].erase(physical_room);
    DB_NOTICE("move physical success, request:%s", request.ShortDebugString().c_str());
}

void ClusterManager::set_instance_migrate(const pb::MetaManagerRequest* request,
                                        pb::MetaManagerResponse* response,
                                        uint64_t log_id) {
    response->set_op_type(request->op_type());
    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("PROCESSING");
    if (_meta_state_machine != NULL && !_meta_state_machine->is_leader()) {
        ERROR_SET_RESPONSE(response, pb::NOT_LEADER, "not leader", request->op_type(), log_id)
        response->set_leader(butil::endpoint2str(_meta_state_machine->get_leader()).c_str());
        return;
    }
    if (!request->has_instance()) {
        //ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, "no instance", request->op_type(), log_id)
        response->set_errmsg("ALLOWED");
        return;
    }
    std::string instance = request->instance().address(); 
    auto ret = set_migrate_for_instance(instance);
    if (ret < 0) {
        response->set_errmsg("ALLOWED");
        return;
    }
    std::vector<int64_t> region_ids;
    RegionManager::get_instance()->get_region_ids(instance, region_ids);
    if (region_ids.size() == 0) {
        response->set_errmsg("ALLOWED");
        return;
    }
    //RegionManager::get_instance()->delete_all_region_for_store(instance, pb::MIGRATE); 
}
void ClusterManager::set_instance_full(const pb::MetaManagerRequest* request,
            pb::MetaManagerResponse* response,
            uint64_t log_id) {
    response->set_op_type(request->op_type());
    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("sucess");
    if (_meta_state_machine != NULL && !_meta_state_machine->is_leader()) {
        ERROR_SET_RESPONSE(response, pb::NOT_LEADER, "not leader", request->op_type(), log_id)
        response->set_leader(butil::endpoint2str(_meta_state_machine->get_leader()).c_str());
        return;
    }
    if (!request->has_instance()) {
        ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, "no instance", request->op_type(), log_id)
        response->set_errmsg("no instance");
        return;
    }
    std::string instance = request->instance().address();
    auto ret = set_full_for_instance(instance);
    if (ret < 0) {
        response->set_errmsg("instance not exist");
        return;
    }
}
void ClusterManager::set_instance_no_full(const pb::MetaManagerRequest* request,
            pb::MetaManagerResponse* response,
            uint64_t log_id) {
    response->set_op_type(request->op_type());
    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("sucess");
    if (_meta_state_machine != NULL && !_meta_state_machine->is_leader()) {
        ERROR_SET_RESPONSE(response, pb::NOT_LEADER, "not leader", request->op_type(), log_id)
        response->set_leader(butil::endpoint2str(_meta_state_machine->get_leader()).c_str());
        return;
    }
    if (!request->has_instance()) {
        ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, "no instance", request->op_type(), log_id)
        response->set_errmsg("no instance");
        return;
    }
    std::string instance = request->instance().address();
    auto ret = set_no_full_for_instance(instance);
    if (ret < 0) {
        response->set_errmsg("instance not exist");
        return;
    }
}
void ClusterManager::process_baikal_heartbeat(const pb::BaikalHeartBeatRequest* /*request*/,
            pb::BaikalHeartBeatResponse* response) {
    auto idc_info_ptr = response->mutable_idc_info();
    {
        BAIDU_SCOPED_LOCK(_physical_mutex);
        for (auto& logical_physical_mapping : _logical_physical_map) {
            auto logical_physical_map = idc_info_ptr->add_logical_physical_map();
            logical_physical_map->set_logical_room(logical_physical_mapping.first);
            for (auto& physical_room : logical_physical_mapping.second) {
                logical_physical_map->add_physical_rooms(physical_room);
            }
        }
    }
    {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        for (auto& instance_physical_pair : _instance_physical_map) {
            auto instance = idc_info_ptr->add_instance_infos();
            instance->set_address(instance_physical_pair.first);
            instance->set_physical_room(instance_physical_pair.second);
        }
    }  
}

void ClusterManager::process_instance_heartbeat_for_store(const pb::InstanceInfo& instance_heart_beat) {
    auto ret = update_instance_info(instance_heart_beat);
    if (ret == 0) {
        return;
    }
    //构造请求
    pb::MetaManagerRequest request;
    request.set_op_type(pb::OP_ADD_INSTANCE);
    pb::InstanceInfo* instance_info = request.mutable_instance();
    *instance_info = instance_heart_beat;
    process_cluster_info(NULL, &request, NULL, NULL);
}

void ClusterManager::process_peer_heartbeat_for_store(const pb::StoreHeartBeatRequest* request,
        pb::StoreHeartBeatResponse* response) {
    std::string instance = request->instance_info().address();
    std::string logical_room = get_logical_room(instance);
    std::string resource_tag = request->instance_info().resource_tag();
    std::unordered_map<int64_t, std::vector<int64_t>> table_regions;
    std::unordered_map<int64_t, int64_t> table_region_counts;
    if (!request->has_need_peer_balance() || !request->need_peer_balance()) {
        return;
    }
    for (auto& peer_info : request->peer_infos()) {
        table_regions[peer_info.table_id()].push_back(peer_info.region_id());
    }
    for (auto& table_region : table_regions) {
        table_region_counts[table_region.first] = table_region.second.size();
    }
    set_instance_regions(instance, table_regions, table_region_counts);
    if (!_meta_state_machine->whether_can_decide()) {
        DB_WARNING("meta state machine can not make decision, resource_tag: %s, instance: %s",
                    resource_tag.c_str(), instance.c_str());
        return;
    }
    if (!_meta_state_machine->get_load_balance(resource_tag)) {
        DB_WARNING("meta state machine close peer load balance, resource_tag: %s, instance: %s", 
                    resource_tag.c_str(), instance.c_str());
        return;
    }
    DB_WARNING("peer load balance, instance_info: %s, resource_tag: %s", 
                instance.c_str(), resource_tag.c_str());
    int64_t instance_count_for_logical = get_instance_count(resource_tag, logical_room);
    int64_t instance_count = get_instance_count(resource_tag);
    //peer均衡是先增加后减少, 代表这个表需要有多少个region先add_peer
    std::unordered_map<int64_t, int64_t> add_peer_counts;
    std::unordered_map<int64_t, std::string> logical_rooms;
    std::unordered_map<int64_t, int64_t> table_average_counts;
    for (auto& table_region : table_regions) {
        int64_t average_peer_count = INT_FAST64_MAX;
        int64_t table_id = table_region.first;
        int64_t total_peer_count;
        bool replica_dists = TableManager::get_instance()->whether_replica_dists(table_id);
        if (replica_dists) {
            total_peer_count = get_peer_count(table_id, logical_room);
        } else {
            total_peer_count = get_peer_count(table_id);
        }
        int64_t total_instance_count = 0;
        if (replica_dists) {
            total_instance_count = instance_count_for_logical; 
        } else {
            total_instance_count = instance_count;
        }
        if (total_instance_count != 0) {
            average_peer_count = total_peer_count / total_instance_count;
        }
        if (total_instance_count != 0 && total_peer_count % total_instance_count != 0) {
             average_peer_count++;
        }
        if (table_region.second.size() > (size_t)(average_peer_count + average_peer_count * 5 / 100)) {
            add_peer_counts[table_id] = table_region.second.size() - average_peer_count;
            table_average_counts[table_id] = average_peer_count;
            if (replica_dists) {
                logical_rooms[table_id] = logical_room;
            } else {
                logical_rooms[table_id] = "";
            }
        }
    }
    for (auto& add_peer_count : add_peer_counts) {
        DB_WARNING("instance: %s should add peer count for peer_load_balance, "
                    "table_id: %ld, add_peer_count: %ld, logical_room: %s",
                    instance.c_str(), 
                    add_peer_count.first, add_peer_count.second, logical_rooms[add_peer_count.first].c_str());
    }
    if (add_peer_counts.size() > 0) {
        RegionManager::get_instance()->peer_load_balance(add_peer_counts, 
                                                         table_regions, 
                                                         instance, 
                                                         resource_tag, 
                                                         logical_rooms, 
                                                         table_average_counts);
    } else {
        DB_WARNING("instance: %s has been peer_load_balance, no need migrate", instance.c_str());
    }
}

void ClusterManager::store_healthy_check_function() {
    //判断全部以resource_tag的维度独立判断
    std::unordered_map<std::string, int64_t> total_store_num;
    std::unordered_map<std::string, int64_t> faulty_store_num;
    std::unordered_map<std::string, int64_t> dead_store_num;
    std::unordered_map<std::string, std::vector<Instance>> dead_stores;
    std::unordered_map<std::string, std::vector<Instance>> full_stores;
    std::unordered_map<std::string, std::vector<Instance>> migrate_stores;
    {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        for (auto& instance_pair : _instance_info) {
            auto& status = instance_pair.second.instance_status;
            std::string resource_tag = instance_pair.second.resource_tag;
            total_store_num[resource_tag]++;
            if (status.state == pb::MIGRATE) {
                migrate_stores[resource_tag].push_back(instance_pair.second);
                continue;
            }
            int64_t last_timestamp = status.timestamp;
            if ((butil::gettimeofday_us() - last_timestamp) > 
                    FLAGS_store_heart_beat_interval_us * FLAGS_store_dead_interval_times) {
                status.state = pb::DEAD;
                dead_stores[resource_tag].push_back(instance_pair.second);
                DB_WARNING("instance:%s is dead DEAD, resource_tag: %s", 
                        instance_pair.first.c_str(), resource_tag.c_str());
                std::vector<int64_t> region_ids;
                RegionManager::get_instance()->get_region_ids(instance_pair.first, region_ids);
                if (region_ids.size() != 0) {
                    dead_store_num[resource_tag]++;
                }
                continue;
            } 
            if ((butil::gettimeofday_us() - last_timestamp) > 
                    FLAGS_store_heart_beat_interval_us * FLAGS_store_faulty_interval_times) {
                status.state = pb::FAULTY;
                DB_WARNING("instance:%s is faulty FAULTY, resource_tag: %s", 
                        instance_pair.first.c_str(), resource_tag.c_str());
                faulty_store_num[resource_tag]++;
                continue;
            }
            //如果实例状态都正常的话，再判断是否因为容量问题需要做迁移
            //if (instance.capacity == 0) {
            //    DB_FATAL("instance:%s capactiy is 0", instance.address.c_str());
            //    continue;
            //}
            //暂时不考虑容量问题，该检查先关闭(liuhuicong)
            //if (instance.used_size * 100 / instance.capacity >= 
            //        FLAGS_migrate_percent) {
            //    DB_WARNING("instance:%s is full", instance_pair.first.c_str()); 
            //    full_stores.push_back(instance_pair.second);   
            //}
        }
    }

    //防止误判，比例过大，则暂停操作
    for (auto& dead_store_pair : dead_stores) {
        std::string resource_tag = dead_store_pair.first;
        if (total_store_num.find(resource_tag) == total_store_num.end()) {
            continue;
        } 
        if ((dead_store_num[resource_tag] + faulty_store_num[resource_tag]) * 100 
                    / total_store_num[resource_tag] >= FLAGS_error_judge_percent
                && (dead_store_num[resource_tag] + faulty_store_num[resource_tag]) >= FLAGS_error_judge_number) {
            DB_FATAL("has too much dead and faulty instance, may be error judge, resource_tag: %s", resource_tag.c_str());
            for (auto& dead_store : dead_store_pair.second) {
                RegionManager::get_instance()->print_region_ids(dead_store.address);
            }
            dead_stores[resource_tag].clear();
            migrate_stores[resource_tag].clear();
            continue;
        }
    }
    //如果store实例死掉，则删除region
    for (auto& store_pair : dead_stores) {
        for (auto& store : store_pair.second) {
            DB_WARNING("store:%s is dead, resource_tag: %s", 
                    store.address.c_str(), store_pair.first.c_str());
            if (_meta_state_machine->get_migrate(store_pair.first)) {
                RegionManager::get_instance()->delete_all_region_for_store(store.address,
                        store.instance_status);
            }
        }
    }
    for (auto& store_pair : migrate_stores) {
        for (auto& store : store_pair.second) {
            DB_WARNING("store:%s is migrating, resource_tag: %s", 
                    store.address.c_str(), store_pair.first.c_str());
            if (_meta_state_machine->get_migrate(store_pair.first)) {
                RegionManager::get_instance()->add_peer_for_store(store.address,
                        store.instance_status);
            }
        }
    }
    //若实例满，则做实例迁移
    //for (auto& full_store : full_stores) {
    //    DB_FATAL("store:%s is full, resource_tag", full_store.second.c_str(),
    //    full_store.first.c_str());
    //    SchemaManager->migirate_region_for_store(full_store);
    //}
}
//从少于平均peer数量的实例中随机选择一个
//如果average_count == 0, 则选择最少数量peer的实例返回
int ClusterManager::select_instance_min(const std::string& resource_tag,
                                        const std::set<std::string>& exclude_stores,
                                        int64_t table_id,
                                        const std::string& logical_room,
                                        std::string& selected_instance,
                                        int64_t average_count) {
    selected_instance.clear();
    BAIDU_SCOPED_LOCK(_instance_mutex);
    if (_instance_info.size() == 0) {
        DB_FATAL("there is no instance");
        return -1;
    }
    int64_t max_region_count = INT_FAST64_MAX;
    std::vector<std::string> candicate_instances; 
    for (auto& instance_count : _instance_regions_count_map) {
        std::string instance = instance_count.first;
        if (false == whether_legal_for_select_instance(instance, resource_tag, exclude_stores, logical_room)) {
            continue;
        }
        if (instance_count.second.find(table_id) == instance_count.second.end()) {
            if (average_count == 0) {
                selected_instance = instance;
                break;
            } else {
                candicate_instances.push_back(instance);
                continue;
            }
        }
        if (average_count != 0 && instance_count.second[table_id] < average_count) {
            candicate_instances.push_back(instance);
        }
        if (instance_count.second[table_id] < max_region_count) {
            selected_instance = instance;
            max_region_count = instance_count.second[table_id];
        }
    }
    //从小于平均peer数量的实例中随机选择一个
    if (candicate_instances.size() != 0) {
        size_t random_index = butil::fast_rand() % candicate_instances.size(); 
        selected_instance = candicate_instances[random_index];
    }
    if (selected_instance.size() == 0) {
        return -1;
    }
    _instance_regions_count_map[selected_instance][table_id]++;
    DB_WARNING("select instance min, resource_tag: %s, table_id: %ld, logical_room: %s,"
                " average_count: %ld, candicate_instance_size: %d, selected_instance: %s",
                resource_tag.c_str(), table_id, logical_room.c_str(), average_count, 
                candicate_instances.size(), selected_instance.c_str());
    return 0;
}

//todo, 暂时未考虑机房，后期需要考虑尽量不放在同一个机房
int ClusterManager::select_instance_rolling(const std::string& resource_tag, 
                                    const std::set<std::string>& exclude_stores,
                                    const std::string& logical_room,
                                    std::string& selected_instance) {
    selected_instance.clear();
    BAIDU_SCOPED_LOCK(_instance_mutex);
    if (_instance_info.size() == 0) {
        DB_FATAL("there is no instance");
        return -1;
    }
    auto iter = _instance_info.find(_last_rolling_instance);
    //取出迭代器的下一个元素
    if (iter == _instance_info.end() || (++iter) == _instance_info.end()) {
        iter = _instance_info.begin();
    }

    size_t instance_count = _instance_info.size();
    size_t rolling_times = 0;
    for (; rolling_times < instance_count; ++iter, ++rolling_times) {
        if (iter == _instance_info.end()) {
            iter = _instance_info.begin();
        }
        if (false == whether_legal_for_select_instance(iter->first, resource_tag, exclude_stores, logical_room)) {
            continue;
        }
        //选择该实例
        selected_instance = iter->first;
        //更新last_rolling_instance
        _last_rolling_instance = selected_instance;
        break;
    }
    if (selected_instance.empty()) {
        DB_FATAL("select instance fail, has no legal store, resource_tag:%s", resource_tag.c_str());
        return -1;
    }
    return 0;
}
int ClusterManager::load_snapshot() {
    _physical_info.clear();
    _logical_physical_map.clear();
    _instance_physical_map.clear();
    _physical_instance_map.clear();
    _instance_info.clear();
    _instance_regions_map.clear();
    _instance_regions_count_map.clear();
    DB_WARNING("cluster manager begin load snapshot");
    {
        BAIDU_SCOPED_LOCK(_physical_mutex);
        _physical_info[FLAGS_default_physical_room] = 
            FLAGS_default_logical_room;
        _logical_physical_map[FLAGS_default_logical_room] = 
                std::set<std::string>{FLAGS_default_physical_room};
    }
    {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        _physical_instance_map[FLAGS_default_logical_room] = std::set<std::string>();
    }
    //创建一个snapshot
    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    read_options.total_order_seek = false;
    RocksWrapper* db = RocksWrapper::get_instance();
    std::unique_ptr<rocksdb::Iterator> iter(
            db->new_iterator(read_options, db->get_meta_info_handle()));
    iter->Seek(MetaServer::CLUSTER_IDENTIFY);
    std::string logical_prefix = MetaServer::CLUSTER_IDENTIFY;
    logical_prefix += MetaServer::LOGICAL_CLUSTER_IDENTIFY + MetaServer::LOGICAL_KEY;

    std::string physical_prefix = MetaServer::CLUSTER_IDENTIFY;
    physical_prefix += MetaServer::PHYSICAL_CLUSTER_IDENTIFY;

    std::string instance_prefix = MetaServer::CLUSTER_IDENTIFY;
    instance_prefix += MetaServer::INSTANCE_CLUSTER_IDENTIFY;
    int ret = 0;
    for (; iter->Valid(); iter->Next()) {
        if (iter->key().starts_with(instance_prefix)) {
            ret = load_instance_snapshot(instance_prefix, iter->key().ToString(), iter->value().ToString());
        } else if (iter->key().starts_with(physical_prefix)) {
            ret = load_physical_snapshot(physical_prefix, iter->key().ToString(), iter->value().ToString());
        } else if (iter->key().starts_with(logical_prefix)) {
            ret = load_logical_snapshot(logical_prefix, iter->key().ToString(), iter->value().ToString());
        } else {
            DB_FATAL("unsupport cluster info when load snapshot, key:%s", iter->key().data());
        }
        if (ret != 0) {
            DB_FATAL("ClusterManager load snapshot fail, key:%s", iter->key().data());
            return -1;
        }
    }
    return 0;
}
bool ClusterManager::whether_legal_for_select_instance(
            const std::string& candicate_instance,
            const std::string& resource_tag,
            const std::set<std::string>& exclude_stores,
            const std::string& logical_room) {
    if (_instance_info.find(candicate_instance) == _instance_info.end()) {
        return false;
    }
    if (logical_room.size() != 0
            && _instance_info[candicate_instance].logical_room != logical_room) {
        return false;
    }
    if (_instance_info[candicate_instance].instance_status.state != pb::NORMAL
            || _instance_info[candicate_instance].resource_tag != resource_tag
            || _instance_info[candicate_instance].capacity == 0) {
        return false;
    }
    if (FLAGS_peer_balance_by_ip) {
        std::string candicate_instance_ip = get_ip(candicate_instance);
        for (auto& exclude_store : exclude_stores) {
           if (candicate_instance_ip == get_ip(exclude_store)) {
               return false;
           }
        }
    } else {
        if (exclude_stores.count(candicate_instance) != 0) {
            return false;
        }
    }
    if ((_instance_info[candicate_instance].used_size  * 100 / _instance_info[candicate_instance].capacity)  > 
                FLAGS_disk_used_percent) {
        DB_WARNING("instance:%s left size is not enough, used_size:%ld, capactity:%ld",
                    candicate_instance.c_str(), 
                    _instance_info[candicate_instance].used_size, 
                    _instance_info[candicate_instance].capacity);
        return false;
    }
    return true;
}
int ClusterManager::load_instance_snapshot(const std::string& instance_prefix,
                                             const std::string& key, 
                                             const std::string& value) {
    std::string address(key, instance_prefix.size());
    pb::InstanceInfo instance_pb;
    if (!instance_pb.ParseFromString(value)) {
        DB_FATAL("parse from pb fail when load instance snapshot, key:%s", key.c_str());
        return -1;
    }
    DB_WARNING("instance_pb:%s", instance_pb.ShortDebugString().c_str());

    std::string physical_room = instance_pb.physical_room();
    if (physical_room.size() == 0) {
        instance_pb.set_physical_room(FLAGS_default_physical_room);
    }
    if (!instance_pb.has_logical_room()) {
        if (_physical_info.find(physical_room) != _physical_info.end()) {
            instance_pb.set_logical_room(_physical_info[physical_room]);
        } else {
            //TODO 是否需要出错
            DB_FATAL("get logical room for physical room: %s fail", physical_room.c_str());
        }
    }
    BAIDU_SCOPED_LOCK(_instance_mutex);
    _instance_info[address] = Instance(instance_pb);
    _instance_physical_map[address] = instance_pb.physical_room();
    _physical_instance_map[instance_pb.physical_room()].insert(address);
    if (_instance_regions_map.find(address) == _instance_regions_map.end()) {
        _instance_regions_map[address] = std::unordered_map<int64_t, std::vector<int64_t>>{};
    }
    if (_instance_regions_count_map.find(address) == _instance_regions_count_map.end()) {
        _instance_regions_count_map[address] = std::unordered_map<int64_t, int64_t>{};
    }
    return 0;
}

int ClusterManager::load_physical_snapshot(const std::string& physical_prefix, 
                                             const std::string& key, 
                                             const std::string& value) {
    pb::PhysicalRoom physical_logical_pb;
    if (!physical_logical_pb.ParseFromString(value)) {
         DB_FATAL("parse from pb fail when load physical snapshot, key:%s", key.c_str());
         return -1;
    }
    DB_WARNING("physical_logical_info:%s", physical_logical_pb.ShortDebugString().c_str());
    BAIDU_SCOPED_LOCK(_physical_mutex);
    std::string logical_room = physical_logical_pb.logical_room();
    std::set<std::string> physical_rooms;
    for (auto& physical_room : physical_logical_pb.physical_rooms()) {
        physical_rooms.insert(physical_room);
        _physical_info[physical_room] = logical_room;
        _physical_instance_map[physical_room] = std::set<std::string>{};
    }
    _logical_physical_map[logical_room] = physical_rooms;
    return 0;
}

int ClusterManager::load_logical_snapshot(const std::string& logical_prefix, 
                                            const std::string& key, 
                                            const std::string& value) {
    pb::LogicalRoom logical_info;
    if (!logical_info.ParseFromString(value)) {
        DB_FATAL("parse from pb fail when load logical snapshot, key:%s", key.c_str());
        return -1;
    }
    DB_WARNING("logical_info:%s", logical_info.ShortDebugString().c_str());
    BAIDU_SCOPED_LOCK(_physical_mutex);
    for (auto logical_room : logical_info.logical_rooms()) {
        _logical_physical_map[logical_room] = std::set<std::string>{};
    }
    return 0;
}

}//namespace
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
