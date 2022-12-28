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
DEFINE_bool(need_check_slow, false, "need_check_slow. default:false");
DEFINE_int32(min_network_segments_per_resource_tag, 10, "min network segments per resource_tag");
DEFINE_int32(network_segment_max_stores_precent, 20, "network segment max stores precent");

DECLARE_int64(store_heart_beat_interval_us);
DECLARE_int32(store_rocks_hang_check_timeout_s);
DECLARE_bool(store_rocks_hang_check);
DECLARE_int32(store_dead_interval_times);
DECLARE_int32(store_faulty_interval_times);
DECLARE_string(default_logical_room);
DECLARE_string(default_physical_room);

bvar::Adder<int> min_fallback_count;
bvar::Adder<int> min_count;
bvar::Adder<int> rolling_fallback_count;
bvar::Adder<int> rolling_count;
bvar::Window<bvar::Adder<int> > g_baikalmeta_min_fallback_count("cluster_manager", "min_fallback_count", &min_fallback_count, 3600);
bvar::Window<bvar::Adder<int> > g_baikalmeta_min_count("cluster_manager", "min_count", &min_count, 3600);
bvar::Window<bvar::Adder<int> > g_baikalmeta_rolling_fallback_count("cluster_manager", "rolling_fallback_count", &rolling_fallback_count, 3600);
bvar::Window<bvar::Adder<int> > g_baikalmeta_rolling_count("cluster_manager", "rolling_count", &rolling_count, 3600);

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
    case pb::OP_UPDATE_INSTANCE_PARAM: {
        if (request->instance_params().size() < 1) {
            ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, "no instance params", request->op_type(), log_id);
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
    Instance instance_mem(instance_info);
    {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        _instance_physical_map[address] = physical_room;
        _physical_instance_map[physical_room].insert(address);
        _resource_tag_instance_map[instance_mem.resource_tag].insert(address);
        _instance_info[address] = instance_mem;
        // add instance，同时加入网段索引
        auto_network_segments_division(instance_mem.resource_tag);
    }
    // 更新调度相关的内存值
    auto call_func = [](std::unordered_map<std::string, InstanceSchedulingInfo>& scheduling_info, const Instance& instance_mem) -> int {
        scheduling_info[instance_mem.address].idc = {instance_mem.resource_tag, instance_mem.logical_room, instance_mem.physical_room};
        scheduling_info[instance_mem.address].pk_prefix_region_count = std::unordered_map<std::string, int64_t >{};
        scheduling_info[instance_mem.address].regions_count_map = std::unordered_map<int64_t, int64_t>{};
        scheduling_info[instance_mem.address].regions_map = std::unordered_map<int64_t, std::vector<int64_t>>{};
        return 1;
    };
    _scheduling_info.Modify(call_func, instance_mem);
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
    std::string resource_tag = _instance_info[address].resource_tag;
    
    // write date to rocksdb
    auto ret = MetaRocksdb::get_instance()->delete_meta_info(
                std::vector<std::string>{construct_instance_key(address)});
    if (ret < 0) {
        DB_WARNING("drop instance:%s to rocksdb fail ", request.ShortDebugString().c_str()); 
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    //更新内存值
    {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        // 删除instance，同时删除instance对应的网段索引
        _instance_physical_map.erase(address);
        _instance_info.erase(address);
        _tombstone_instance[address] = TimeCost();

        _resource_tag_instance_map[resource_tag].erase(address);
        if (_physical_instance_map.find(physical_room) != _physical_instance_map.end()) {
            _physical_instance_map[physical_room].erase(address);
        }
        auto_network_segments_division(resource_tag);
    }
    // 更新调度相关的内存值
    auto call_func = [](std::unordered_map<std::string, InstanceSchedulingInfo>& scheduling_info,
                        const std::string& address) -> int {
        scheduling_info.erase(address);
        return 1;
    };
    _scheduling_info.Modify(call_func, address);
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
        DB_WARNING("add physical room:%s to rocksdb fail", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    bool resource_tag_changed = false;
    {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        _instance_info[address].capacity = instance_info.capacity();
        _instance_info[address].used_size = instance_info.used_size();
        if (_instance_info[address].resource_tag != instance_info.resource_tag()) {
            // 更新instance resource tag, 同时更改instance的网段
            _resource_tag_instance_map[_instance_info[address].resource_tag].erase(address);
            auto_network_segments_division(_instance_info[address].resource_tag);
            _instance_info[address].resource_tag = instance_info.resource_tag();
            _instance_info[address].network_segment_self_defined = instance_info.network_segment();
            _resource_tag_instance_map[_instance_info[address].resource_tag].insert(address);
            auto_network_segments_division(_instance_info[address].resource_tag);
            resource_tag_changed = true;
        } else if (_instance_info[address].network_segment_self_defined != instance_info.network_segment()) {
            _instance_info[address].network_segment_self_defined = instance_info.network_segment();
            auto_network_segments_division(_instance_info[address].resource_tag);
        }
    }
    // 更新调度相关的内存值
    if (resource_tag_changed) {
        IdcInfo new_idc(instance_info.resource_tag(), instance_info.logical_room(), instance_info.physical_room());
        auto call_func = [](std::unordered_map<std::string, InstanceSchedulingInfo>& scheduling_info,
                            const std::string& address,
                            const IdcInfo& new_idc) -> int {
            scheduling_info[address].idc = new_idc;
            return 1;
        };
        _scheduling_info.Modify(call_func, address, new_idc);
    }
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("modify tag success, request:%s", request.ShortDebugString().c_str());
}

// 如果已存在则修改，不存在则新添加
inline void agg_instance_param(const pb::InstanceParam& old_param, const pb::InstanceParam& new_param, pb::InstanceParam* out_param) {
    std::map<std::string, pb::ParamDesc> kv_map;
    for (auto& iterm : old_param.params()) {
        kv_map[iterm.key()] = iterm;
    }
    for (auto& iterm : new_param.params()) {
        if (iterm.need_delete()) {
            kv_map.erase(iterm.key());
        } else {
            kv_map[iterm.key()] = iterm;
        }
    }

    for (auto iter : kv_map) {
        auto param_iterm = out_param->add_params();
        param_iterm->CopyFrom(iter.second);
    }
}

void ClusterManager::update_instance_param(const pb::MetaManagerRequest& request, braft::Closure* done) {
    std::vector<std::string> keys;
    keys.reserve(5);
    std::vector<std::string> values;
    values.reserve(5);
    std::vector<std::string> delete_keys;
    delete_keys.reserve(5);
    {
        BAIDU_SCOPED_LOCK(_instance_param_mutex);
        for (auto& new_param : request.instance_params()) {
            pb::InstanceParam old_param;
            pb::InstanceParam out_param;
            old_param.set_resource_tag_or_address(new_param.resource_tag_or_address());
            out_param.set_resource_tag_or_address(new_param.resource_tag_or_address());
            auto iter = _instance_param_map.find(new_param.resource_tag_or_address());
            if (iter != _instance_param_map.end()) {
                old_param.CopyFrom(iter->second);
            } 

            agg_instance_param(old_param, new_param, &out_param);
            if (out_param.params_size() > 0) {
                std::string value;
                if (!out_param.SerializeToString(&value)) { 
                    DB_FATAL("SerializeToString fail");
                    continue;
                }
                _instance_param_map[out_param.resource_tag_or_address()] = out_param;
                keys.emplace_back(construct_instance_param_key(out_param.resource_tag_or_address()));
                values.emplace_back(value);
                DB_WARNING("add instance param:%s", out_param.ShortDebugString().c_str());
            } else if (old_param.params_size() > 0) {
                _instance_param_map.erase(out_param.resource_tag_or_address());
                delete_keys.emplace_back(construct_instance_param_key(out_param.resource_tag_or_address()));
                DB_WARNING("erase instance param:%s", old_param.ShortDebugString().c_str());
            } else {
                // 新旧都为空，do nothing
            }
        }
    }
    if (!keys.empty()) {
        int ret = MetaRocksdb::get_instance()->put_meta_info(keys, values);
        if (ret < 0) {
            DB_WARNING("modify instance param:%s to rocksdb fail", request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
            return;
        }
    }
    if (!delete_keys.empty()) {
        int ret = MetaRocksdb::get_instance()->delete_meta_info(delete_keys);
        if (ret < 0) {
            DB_WARNING("modify instance param:%s to rocksdb fail", request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
            return;
        }
    }
    
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("modify instance param success, request:%s", request.ShortDebugString().c_str());
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
        ERROR_SET_RESPONSE_WARN(response, pb::NOT_LEADER, "not leader", request->op_type(), log_id)
        response->set_leader(butil::endpoint2str(_meta_state_machine->get_leader()).c_str());
        return;
    }
    if (!request->has_instance()) {
        DB_WARNING("reuqest no instance");
        response->set_errmsg("ALLOWED");
        return;
    }
    std::string instance = request->instance().address(); 
    auto ret = set_migrate_for_instance(instance);
    if (ret < 0) {
        DB_WARNING("instance:%s not exist", instance.c_str());
        response->set_errmsg("ALLOWED");
        return;
    }
    std::vector<int64_t> region_ids;
    region_ids.reserve(100);
    RegionManager::get_instance()->get_region_ids(instance, region_ids);
    std::vector<int64_t> learner_ids;
    learner_ids.reserve(100);
    RegionManager::get_instance()->get_learner_ids(instance, learner_ids);
    DB_WARNING("instance:%s region size:%lu, learner size:%lu", 
            instance.c_str(), region_ids.size(), learner_ids.size());
    if (region_ids.size() == 0 && learner_ids.size() == 0) {
        response->set_errmsg("ALLOWED");
        return;
    }
}
void ClusterManager::set_instance_status(const pb::MetaManagerRequest* request,
            pb::MetaManagerResponse* response,
            uint64_t log_id) {
    response->set_op_type(request->op_type());
    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("sucess");
    if (_meta_state_machine != NULL && !_meta_state_machine->is_leader()) {
        ERROR_SET_RESPONSE_WARN(response, pb::NOT_LEADER, "not leader", request->op_type(), log_id)
        response->set_leader(butil::endpoint2str(_meta_state_machine->get_leader()).c_str());
        return;
    }
    if (!request->has_instance()) {
        ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, "no instance", request->op_type(), log_id)
        response->set_errmsg("no instance");
        return;
    }
    std::string instance = request->instance().address();
    auto ret = set_status_for_instance(instance, request->instance().status());
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
            auto iter = _instance_info.find(instance_physical_pair.first);
            if (iter != _instance_info.end()) {
                instance->set_resource_tag(iter->second.resource_tag);
            }           
        }
    }  
}

void ClusterManager::process_instance_heartbeat_for_store(const pb::InstanceInfo& instance_heart_beat) {
    int ret = update_instance_info(instance_heart_beat);
    if (ret == 0) {
        return;
    }

    //构造请求 -1: add instance -2: update instance
    pb::MetaManagerRequest request;
    if (ret == -1) {
        request.set_op_type(pb::OP_ADD_INSTANCE);
    } else {
        request.set_op_type(pb::OP_UPDATE_INSTANCE);
    }
    pb::InstanceInfo* instance_info = request.mutable_instance();
    *instance_info = instance_heart_beat;
    process_cluster_info(NULL, &request, NULL, NULL);
}

// 获取实例参数
void ClusterManager::process_instance_param_heartbeat_for_store(const pb::StoreHeartBeatRequest* request,
        pb::StoreHeartBeatResponse* response) {
    std::string address = request->instance_info().address();
    std::string resource_tag = request->instance_info().resource_tag();

    BAIDU_SCOPED_LOCK(_instance_param_mutex);

    auto iter = _instance_param_map.find(resource_tag);
    if (iter != _instance_param_map.end()) {
        *(response->add_instance_params()) = iter->second;
    }

    // 实例单独配置
    iter = _instance_param_map.find(address);
    if (iter != _instance_param_map.end()) {
        *(response->add_instance_params()) = iter->second;
    }

}

// 获取实例参数
void ClusterManager::process_instance_param_heartbeat_for_baikal(const pb::BaikalOtherHeartBeatRequest* request,
        pb::BaikalOtherHeartBeatResponse* response) {
    if (request->has_baikaldb_resource_tag()) {
        BAIDU_SCOPED_LOCK(_instance_param_mutex);

        auto iter = _instance_param_map.find(request->baikaldb_resource_tag());
        if (iter != _instance_param_map.end()) {
            auto instance_param = response->mutable_instance_param();
            instance_param->CopyFrom(iter->second);
        }
    }
}
    
void ClusterManager::get_switch(const pb::QueryRequest* request, pb::QueryResponse* response) {
    if (!request->has_resource_tag()) {
        response->set_errcode(pb::INPUT_PARAM_ERROR);
        response->set_errmsg("input has no resource_tag");
        return;
    }
    BAIDU_SCOPED_LOCK(_instance_mutex);
    if (!request->has_resource_tag() || request->resource_tag().empty()) {
        for(auto& resource_tag_pair : _resource_tag_instances_by_network) {
            auto info = response->add_resource_tag_infos();
            info->set_resource_tag(resource_tag_pair.first);
            info->set_network_segment_balance(_meta_state_machine->get_network_segment_balance(resource_tag_pair.first));
            info->set_peer_load_balance(_meta_state_machine->get_load_balance(resource_tag_pair.first));
            info->set_migrate(_meta_state_machine->get_migrate(resource_tag_pair.first));
        }
    } else {
        auto info = response->add_resource_tag_infos();
        info->set_resource_tag(request->resource_tag());
        info->set_network_segment_balance(_meta_state_machine->get_network_segment_balance(request->resource_tag()));
        info->set_peer_load_balance(_meta_state_machine->get_load_balance(request->resource_tag()));
        info->set_migrate(_meta_state_machine->get_migrate(request->resource_tag()));
    }
}

void ClusterManager::process_pk_prefix_load_balance(std::unordered_map<std::string, int64_t>& pk_prefix_region_counts,
            std::unordered_map<int64_t, IdcInfo>& table_balance_idc,
            std::unordered_map<std::string, int64_t>& idc_instance_count,
            std::unordered_map<int64_t, int64_t>& table_add_peer_counts,
            std::unordered_map<std::string, int64_t>& pk_prefix_add_peer_counts,
            std::unordered_map<std::string, int64_t>& pk_prefix_average_counts) {
    std::set<int64_t> do_not_peer_balance_table;
    for(auto& pk_prefix_region : pk_prefix_region_counts) {
        int64_t table_id;
        auto table_id_end = pk_prefix_region.first.find_first_of('_');
        if (table_id_end == std::string::npos) {
            continue;
        }
        table_id = strtoll(pk_prefix_region.first.substr(0, table_id_end).c_str(), NULL, 10);
        if (table_balance_idc.find(table_id) == table_balance_idc.end()) {
            continue;
        }
        const auto& balance_idc = table_balance_idc[table_id];
        // 按照pk_prefix的维度进行计算average和差值
        int64_t average_peer_count = INT_FAST64_MAX;
        int64_t pk_prefix_total_count = get_pk_prefix_peer_count(pk_prefix_region.first, balance_idc);
        int64_t total_instance_count = idc_instance_count[balance_idc.to_string()];
        if (total_instance_count <= 0) {
            continue;
        }
        average_peer_count = pk_prefix_total_count / total_instance_count;
        if (pk_prefix_total_count % total_instance_count != 0) {
            average_peer_count++;
        }
        DB_DEBUG("handle table_id: %s, key: %s, total_peer: %lu, total_instance_count: %lu, "
                  "average_peer_count: %lu, heartbeat report: %lu, idc: %s",
                  pk_prefix_region.first.substr(0, table_id_end).c_str(),
                  pk_prefix_region.first.c_str(),
                  pk_prefix_total_count,
                  total_instance_count,
                  average_peer_count,
                  pk_prefix_region.second,
                  balance_idc.to_string().c_str());
        // 当大户维度已经均衡，进行表维度的load balance，需要pk_prefix_average_counts信息。
        pk_prefix_average_counts[pk_prefix_region.first] = average_peer_count;
        if (pk_prefix_region.second > (size_t)(average_peer_count + average_peer_count * 5 / 100)) {
            pk_prefix_add_peer_counts[pk_prefix_region.first] = pk_prefix_region.second - average_peer_count;
            // table如果要做pk_prefix load balance，那么这一轮先不做table维度的peer load balance
            do_not_peer_balance_table.insert(table_id);
            DB_DEBUG("table_id: %lu, pk_prefix: %s, need add %lu, average: %lu, table dimension need add: %lu",
                       table_id,
                       pk_prefix_region.first.c_str(),
                       pk_prefix_add_peer_counts[pk_prefix_region.first],
                       pk_prefix_average_counts[pk_prefix_region.first],
                       table_add_peer_counts[table_id]);
        }
    }
    for(auto& id : do_not_peer_balance_table) {
        table_add_peer_counts.erase(id);
    }
}
    
void ClusterManager::process_peer_heartbeat_for_store(const pb::StoreHeartBeatRequest* request,
        pb::StoreHeartBeatResponse* response) {
    std::string instance = request->instance_info().address();
    std::string resource_tag = request->instance_info().resource_tag();
    std::unordered_map<int64_t, std::vector<int64_t>> table_regions;
    std::unordered_map<int64_t, int64_t> table_region_counts;
    std::unordered_map<int64_t, bool> is_learner_tables;
    std::unordered_map<int64_t, int32_t> table_pk_prefix_dimension;
    std::unordered_map<std::string, std::vector<int64_t>> pk_prefix_regions;
    std::unordered_map<std::string, int64_t> pk_prefix_region_counts;
    std::set<std::string> clusters_in_fast_importer;
    
    if (!request->has_need_peer_balance() || !request->need_peer_balance()) {
        return;
    }
    // 一次性拿到所有开启了pk_prefix balance的表id及其维度信息
    TableManager::get_instance()->get_pk_prefix_dimensions(table_pk_prefix_dimension);
    TableManager::get_instance()->get_clusters_in_fast_importer(clusters_in_fast_importer);
    for (auto& peer_info : request->peer_infos()) {
        table_regions[peer_info.table_id()].push_back(peer_info.region_id());
        is_learner_tables[peer_info.table_id()] = peer_info.is_learner();
        if (table_pk_prefix_dimension.find(peer_info.table_id()) == table_pk_prefix_dimension.end()) {
            continue;
        }
        std::string key;
        if (!TableManager::get_instance()->get_pk_prefix_key(peer_info.table_id(),
                                                          table_pk_prefix_dimension[peer_info.table_id()],
                                                          peer_info.start_key(),
                                                          key)) {
            DB_WARNING("decode pk_prefix_key fail, table_id: %lu, region_id: %lu",
                       peer_info.table_id(), peer_info.region_id());
            continue;
        }
        pk_prefix_regions[key].emplace_back(peer_info.region_id());
        pk_prefix_region_counts[key]++;
    }
    for (auto& table_region : table_regions) {
        table_region_counts[table_region.first] = table_region.second.size();
    }
    set_instance_regions(instance, table_regions, table_region_counts, pk_prefix_region_counts);
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
    if (clusters_in_fast_importer.find(resource_tag) != clusters_in_fast_importer.end()) {
        DB_WARNING("resource_tag: %s in fast importer, stop peer load balance", resource_tag.c_str());
        return;
    }
    IdcInfo instance_idc;
    if (get_instance_idc(instance, instance_idc) < 0) {
        DB_FATAL("resource_tag: %s, instance: %s get idc fail", resource_tag.c_str(), instance.c_str());
        return;
    }
    DB_WARNING("peer load balance, instance_info: %s, idc: %s", 
                instance.c_str(), instance_idc.to_string().c_str());

    // 现在不同表可能在三个维度进行balance，同集群balance，同逻辑机房balance，同物理机房balance。
    // 获取同集群、同逻辑机房、同物理机房的store实例数
    std::unordered_map<std::string, int64_t> idc_instance_count;
    get_instance_count_for_all_level(instance_idc, idc_instance_count);
    //peer均衡是先增加后减少, 代表这个表需要有多少个region先add_peer
    std::unordered_map<int64_t, int64_t> add_peer_counts;
    std::unordered_map<int64_t, int64_t> add_learner_counts;
    // region_id -> banlance粒度 : {resource_tag:logiacl_room:physical_room}
    std::unordered_map<int64_t, IdcInfo> table_balance_idc;
    std::unordered_map<int64_t, int64_t> table_average_counts;
    // pk_prefix维度
    std::unordered_map<std::string, int64_t> pk_prefix_add_peer_counts;
    std::unordered_map<std::string, int64_t> pk_prefix_average_counts;
    for (auto& table_region : table_regions) {
        // 计算表维度的peer平均值和每个表需要balance调度的peer数
        int64_t average_peer_count = INT_FAST64_MAX;
        int64_t table_id = table_region.first;
        int64_t total_peer_count = 0;
        IdcInfo balance_idc;
        if (is_learner_tables[table_id]) {
            // learner目前是在整个集群balance
            balance_idc.resource_tag = resource_tag;
        } else {
            if (TableManager::get_instance()->get_table_dist_belonged(table_id, instance_idc, balance_idc) < 0) {
                // instance不在表副本分布里，跳过，后续会自动删掉
                continue;
            }
        }
        table_balance_idc[table_id] = balance_idc;
        total_peer_count = get_peer_count(table_id, balance_idc);
        int64_t total_instance_count = idc_instance_count[balance_idc.to_string()];
        if (total_instance_count != 0) {
            average_peer_count = total_peer_count / total_instance_count;
        }
        if (total_instance_count != 0 && total_peer_count % total_instance_count != 0) {
             average_peer_count++;
        }
        table_average_counts[table_id] = average_peer_count;
        DB_DEBUG("process tableid %ld region size %zu average cout %zu, idc: %s, total_count: %ld", 
            table_id, table_region.second.size(),
            (size_t)(average_peer_count + average_peer_count * 5 / 100), 
            balance_idc.to_string().c_str(), total_instance_count);
        
        if (table_region.second.size() > (size_t)(average_peer_count + average_peer_count * 5 / 100)) {
            if (!is_learner_tables[table_id]) {
                add_peer_counts[table_id] = table_region.second.size() - average_peer_count;
            } else {
                add_learner_counts[table_id] = table_region.second.size() - average_peer_count;
            }
        }
    }
    if (!pk_prefix_region_counts.empty() && TableManager::get_instance()->can_do_pk_prefix_balance()) {
        // 计算pk_prefix维度peer平均值和需要balance调度的peer数
        process_pk_prefix_load_balance(pk_prefix_region_counts,
                                    table_balance_idc,
                                    idc_instance_count,
                                    add_peer_counts, 
                                    pk_prefix_add_peer_counts,
                                    pk_prefix_average_counts);
    }
    if (!pk_prefix_add_peer_counts.empty()) {
        RegionManager::get_instance()->pk_prefix_load_balance(pk_prefix_add_peer_counts,
                                                        pk_prefix_regions,
                                                        instance,
                                                        table_balance_idc,
                                                        pk_prefix_average_counts,
                                                        table_average_counts);
    } else {
        DB_WARNING("instance: %s has been pk_prefix_load_balance, no need migrate", instance.c_str());
    }
    for (auto& add_peer_count : add_peer_counts) {
        DB_WARNING("instance: %s should add peer count for peer_load_balance, "
                    "table_id: %ld, add_peer_count: %ld, balance_idc: %s",
                    instance.c_str(), 
                    add_peer_count.first, 
                    add_peer_count.second, 
                    table_balance_idc[add_peer_count.first].to_string().c_str());
    }
    if (add_peer_counts.size() > 0) {
        RegionManager::get_instance()->peer_load_balance(add_peer_counts, 
                                                         table_regions, 
                                                         instance, 
                                                         table_balance_idc, 
                                                         table_average_counts,
                                                         table_pk_prefix_dimension,
                                                         pk_prefix_average_counts);
    } else {
        DB_WARNING("instance: %s has been peer_load_balance, no need migrate", instance.c_str());
    }

    for (auto& add_learner_count : add_learner_counts) {
        DB_WARNING("instance: %s should add learner count for learner_load_balance, "
                    "table_id: %ld, add_peer_count: %ld, resource_tag: %s",
                    instance.c_str(), add_learner_count.first, add_learner_count.second,
                    resource_tag.c_str());
    }
    if (add_learner_counts.size() > 0) {
        RegionManager::get_instance()->learner_load_balance(add_learner_counts, 
                                                         table_regions, 
                                                         instance, 
                                                         resource_tag, 
                                                         table_average_counts);
    } else {
        DB_WARNING("instance: %s has been learner_load_balance, no need migrate", instance.c_str());
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
                // 清空leader，后续如果被设置migrate也能正常迁移
                RegionManager::get_instance()->set_instance_leader_count(instance_pair.first,
                        std::unordered_map<int64_t, int64_t>(), std::unordered_map<std::string, int64_t>());
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
            RegionManager::get_instance()->delete_all_region_for_store(store.address,
                    store.instance_status);
        }
    }
    for (auto& store_pair : migrate_stores) {
        // 内部限制迁移并发，不需要每次让opera配
        int64_t concurrency = 2;
        get_meta_param(store_pair.first, "migrate_concurrency", &concurrency);
        int64_t delay = 0;
        get_meta_param(store_pair.first, "migrate_delay_s", &delay);
        std::sort(store_pair.second.begin(), store_pair.second.end(),
                [](const Instance& l, const Instance& r) {
                    return l.instance_status.state_duration.get_time() >
                    r.instance_status.state_duration.get_time();
                });
        for (auto& store : store_pair.second) {
            if (_meta_state_machine->get_migrate(store_pair.first)
                && !TableManager::get_instance()->is_cluster_in_fast_importer(store_pair.first)
                && store.instance_status.state_duration.get_time() > delay * 1000 * 1000
                && concurrency-- > 0) {
                DB_WARNING("store:%s is migrating, resource_tag: %s", 
                        store.address.c_str(), store_pair.first.c_str());
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
// resource_tag为空，则reset所有resource tag的网段分布
// reset_prefix，重新从prefix=16开始划分网段
void ClusterManager::auto_network_segments_division(std::string resource_tag) {
    if (_resource_tag_instance_map.find(resource_tag) == _resource_tag_instance_map.end() ||
        _resource_tag_instance_map[resource_tag].empty()) {
        DB_WARNING("no such resource tag: %s or no instance in it", resource_tag.c_str());
        return;
    }
    // 先对resource tag下的store进行划分网段，设置prefix
    std::unordered_map<std::string, std::string> ip_set;
    std::unordered_map<std::string, int> instance_count_per_network_segment;
    size_t total_instance = _resource_tag_instance_map[resource_tag].size();
    size_t max_instances_in_one_network = 0;
    int& prefix = _resource_tag_network_prefix[resource_tag];
    auto& network_segments = _resource_tag_instances_by_network[resource_tag];
    size_t max_stores_in_one_network = total_instance * FLAGS_network_segment_max_stores_precent / 100;
    if ((total_instance * FLAGS_network_segment_max_stores_precent) % 100 != 0) {
        ++max_stores_in_one_network;
    }
    for (prefix = 16; prefix <= 32; ++prefix) {
        instance_count_per_network_segment.clear();
        max_instances_in_one_network = 0;
        for(auto& address : _resource_tag_instance_map[resource_tag]) {
            auto instance_iter = _instance_info.find(address);
            if (instance_iter == _instance_info.end()) {
                DB_WARNING("no such instance: %s in _instance_info", address.c_str());
                continue;
            }
            auto& instance = instance_iter->second;
            if (ip_set.find(address) == ip_set.end()) {
                ip_set[address] = get_ip_bit_set(address, 32);
            }
            instance.network_segment = ip_set[address].substr(0, prefix);
            ++instance_count_per_network_segment[instance.network_segment];
            if (instance_count_per_network_segment[instance.network_segment] > max_instances_in_one_network) {
                max_instances_in_one_network = instance_count_per_network_segment[instance.network_segment];
            }
        }
        // TODO: 上限recheck
        if (instance_count_per_network_segment.size() >= FLAGS_min_network_segments_per_resource_tag
                && max_instances_in_one_network <= max_stores_in_one_network) {
            break;
        }
    }
    if (prefix > 32) {
        prefix = 32;
    }
    network_segments.clear();
    // 最后再按照用户自定义的网段信息进行调整，生成对应的network_segment map
    for(auto& address : _resource_tag_instance_map[resource_tag]) {
        auto instance_iter = _instance_info.find(address);
        if (instance_iter == _instance_info.end()) {
            DB_WARNING("no such instance: %s in _instance_info", address.c_str());
            continue;
        }
        auto& instance = instance_iter->second;
        if (!instance.network_segment_self_defined.empty()) {
            instance.network_segment = instance.network_segment_self_defined;
        } 
        network_segments[instance.network_segment].emplace_back(address);
    }
    DB_WARNING("finish auto network segment division for resource tag: %s, prefix: %d",
            resource_tag.c_str(), prefix);
}
    
int ClusterManager::select_instance_min_on_pk_prefix(const IdcInfo& idc,
                                                  const std::set<std::string>& exclude_stores,
                                                  const int64_t& table_id,
                                                  const std::string& pk_prefix_key,
                                                  std::string& selected_instance,
                                                  const int64_t& pk_prefix_average_count,
                                                  const int64_t& table_average_count,
                                                  bool need_both_below_average) {
    auto pick_candidate_instances = [this, idc, exclude_stores, table_id,
                                     pk_prefix_key, pk_prefix_average_count, table_average_count] (
            std::vector<std::string>& instances,
            std::vector<std::string>& candidate_instances,
            std::vector<std::string>& candidate_instances_pk_prefix_dimension) {
        DoubleBufferedSchedulingInfo::ScopedPtr info_iter;
        if (_scheduling_info.Read(&info_iter) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return;
        }
        for (auto& instance : instances) {
            if (!is_legal_for_select_instance(idc, instance, exclude_stores)) {
                continue;
            }
            auto instance_iter = info_iter->find(instance);
            if (instance_iter == info_iter->end()) {
                continue;
            }
            const InstanceSchedulingInfo& scheduling_info = instance_iter->second;
            int64_t region_count = 0;
            int64_t pk_prefix_region_count = 0;
            auto region_count_iter = scheduling_info.regions_count_map.find(table_id);
            if (region_count_iter != scheduling_info.regions_count_map.end()) {
                region_count = region_count_iter->second;
            }
            auto pk_prefix_count_iter = scheduling_info.pk_prefix_region_count.find(pk_prefix_key);
            if (pk_prefix_count_iter != scheduling_info.pk_prefix_region_count.end()) {
                pk_prefix_region_count = pk_prefix_count_iter->second;
            }
            if (pk_prefix_region_count < pk_prefix_average_count && region_count < table_average_count) {
                candidate_instances.emplace_back(instance);
            } else if (pk_prefix_region_count < pk_prefix_average_count) {
                candidate_instances_pk_prefix_dimension.emplace_back(instance);
            }
        }
    };
    selected_instance.clear();
    const std::string& resource_tag = idc.resource_tag;
    BAIDU_SCOPED_LOCK(_instance_mutex);
    if (_resource_tag_instance_map.count(resource_tag) == 0 ||
        _resource_tag_instance_map[resource_tag].empty()) {
        DB_FATAL("there is no instance, idc: %s", idc.to_string().c_str());
        return -1;
    }
    // 同时满足table和pk_prefix两个维度region数小于平均值的候选instance, 如果有候选，优先pick
    std::vector<std::string> candidate_instances;
    // 只满足pk_prefix维度region数小于平均值的候选instance
    std::vector<std::string> candidate_instances_pk_prefix_dimension;
    if(_resource_tag_instances_by_network.find(resource_tag) == _resource_tag_instances_by_network.end()) {
        DB_FATAL("no instance in _resource_tag_instances_by_network: %s", idc.to_string().c_str());
        return -1;
    }
    min_count << 1;
    auto& instances_by_network = _resource_tag_instances_by_network[resource_tag];
    if (_meta_state_machine->get_network_segment_balance(resource_tag)) {
        std::set<std::string> exclude_network_segment;
        for (auto& instance_address : exclude_stores) {
            if (_instance_info.find(instance_address) == _instance_info.end()) {
                continue;
            }
            exclude_network_segment.insert(_instance_info[instance_address].network_segment);
        }
        for(auto& network_segment : instances_by_network) {
            if (exclude_network_segment.find(network_segment.first) != exclude_network_segment.end()) {
                continue;
            }
            pick_candidate_instances(network_segment.second,
                                     candidate_instances,
                                     candidate_instances_pk_prefix_dimension);
        }
        if (candidate_instances.empty()) {
            DB_WARNING("min fallback: idc: %s", idc.to_string().c_str());
            min_fallback_count << 1;
            for (auto& network_segment : exclude_network_segment) {
                pick_candidate_instances(instances_by_network[network_segment],
                                         candidate_instances,
                                         candidate_instances_pk_prefix_dimension);
            }
        }
    } else {
        for(auto& network_segment : instances_by_network) {
            pick_candidate_instances(network_segment.second,
                                     candidate_instances,
                                     candidate_instances_pk_prefix_dimension);
        }
    }
    //从小于平均peer数量的实例中随机选择一个
    if (!candidate_instances.empty()) {
        size_t random_index = butil::fast_rand() % candidate_instances.size();
        selected_instance = candidate_instances[random_index];
    }
    if (!need_both_below_average && selected_instance.empty() && !candidate_instances_pk_prefix_dimension.empty()) {
        size_t random_index = butil::fast_rand() % candidate_instances_pk_prefix_dimension.size();
        selected_instance = candidate_instances_pk_prefix_dimension[random_index];
    }
    if (selected_instance.empty()) {
        return -1;
    }
    add_peer_count_on_pk_prefix(selected_instance, table_id, pk_prefix_key);
    DB_WARNING("select instance min on pk_prefix dimension, table_id: %ld, idc: %s, "
               "pk_prefix_average_count: %ld, table_average_count: %ld, candidate_instance_size: %lu %lu, "
               "selected_instance: %s",
               table_id, idc.to_string().c_str(), 
               pk_prefix_average_count, table_average_count,
               candidate_instances.size(), candidate_instances_pk_prefix_dimension.size(),
               selected_instance.c_str());
    return 0;
}

// 从少于平均peer数量的实例中随机选择一个
// 如果average_count == 0, 则选择最少数量peer的实例返回
// 1. peer load_balance, exclude_stores是region三个peer的store address
// 2. learner load_balance, exclude_stores是心跳上报的store address
int ClusterManager::select_instance_min(const IdcInfo& idc,
                                        const std::set<std::string>& exclude_stores,
                                        const int64_t& table_id,
                                        std::string& selected_instance,
                                        const int64_t& average_count) {
    auto pick_candidate_instances = [this, idc, exclude_stores, table_id, average_count](
            std::vector<std::string>& instances, std::vector<std::string>& candidate_instances,
            std::string &selected_instance, int64_t& max_region_count) -> bool {
        DoubleBufferedSchedulingInfo::ScopedPtr info_iter;
        if (_scheduling_info.Read(&info_iter) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return false;
        }
        for (auto& instance : instances) {
            if (!is_legal_for_select_instance(idc, instance, exclude_stores)) {
                continue;
            }
            auto instance_iter = info_iter->find(instance);
            if (instance_iter == info_iter->end()) {
                continue;
            }
            const InstanceSchedulingInfo& scheduling_info = instance_iter->second;
            int64_t region_count = 0;
            auto region_count_iter = scheduling_info.regions_count_map.find(table_id);
            if (region_count_iter != scheduling_info.regions_count_map.end()) {
                region_count = region_count_iter->second;
            }
            if (region_count == 0) {
                if (average_count == 0) {
                    selected_instance = instance;
                    return true;
                } else {
                    candidate_instances.emplace_back(instance);
                    continue;
                }
            }
            if (average_count != 0 && region_count < average_count) {
                candidate_instances.emplace_back(instance);
            }
            if (region_count < max_region_count) {
                selected_instance = instance;
                max_region_count = region_count;
            }
        } 
        return false;
    };
    selected_instance.clear();
    const std::string& resource_tag = idc.resource_tag;
    BAIDU_SCOPED_LOCK(_instance_mutex);
    if (_resource_tag_instance_map.count(resource_tag) == 0 ||
        _resource_tag_instance_map[resource_tag].empty()) {
        DB_FATAL("there is no instance, idc: %s", idc.to_string().c_str());
        return -1;
    }
    std::vector<std::string> candidate_instances;
    int64_t max_region_count = INT_FAST64_MAX;
    if(_resource_tag_instances_by_network.find(resource_tag) == _resource_tag_instances_by_network.end()) {
        DB_FATAL("no instance in _resource_tag_instances_by_network, idc: %s", idc.to_string().c_str());
        return -1;
    }
    min_count << 1;
    auto& instances_by_network = _resource_tag_instances_by_network[resource_tag];
    if (_meta_state_machine->get_network_segment_balance(resource_tag)) {
        // resource open load balance by network segment
        std::set<std::string> exclude_network_segment;
        // collect overlap network segment
        for (auto& instance_address : exclude_stores) {
            if (_instance_info.find(instance_address) == _instance_info.end()) {
                continue;
            }
            exclude_network_segment.insert(_instance_info[instance_address].network_segment);
        }
        // find in no-overlap network segment, 
        for(auto& network_segment : instances_by_network) {
            // not overlap
            if (exclude_network_segment.find(network_segment.first) == exclude_network_segment.end()) {
                bool ret = pick_candidate_instances(network_segment.second, candidate_instances, 
                        selected_instance, max_region_count);
                if (ret) {
                    break;
                }
            }
        } 
        if (selected_instance.empty() && candidate_instances.empty()) {
            // fallback, find in overlap network segment
            DB_WARNING("min fallback: idc: %s", idc.to_string().c_str());
            min_fallback_count << 1;
            for (auto& network_segment : exclude_network_segment) {
                if (instances_by_network.find(network_segment) != instances_by_network.end()) {
                    bool ret = pick_candidate_instances(instances_by_network[network_segment], candidate_instances, 
                            selected_instance, max_region_count);
                    if (ret) {
                        break;
                    }
                }
            }
        }
    } else {
        for(auto& network_segment : instances_by_network) {
            bool ret = pick_candidate_instances(network_segment.second, candidate_instances,
                    selected_instance, max_region_count);
            if (ret) {
                break;
            }
        }
    }
    //从小于平均peer数量的实例中随机选择一个
    if (!candidate_instances.empty()) {
        size_t random_index = butil::fast_rand() % candidate_instances.size();
        selected_instance = candidate_instances[random_index];
    }
    if (selected_instance.empty()) {
        return -1;
    }
    add_peer_count(selected_instance, table_id);
    DB_WARNING("select instance min, table_id: %ld, idc: %s,"
                " average_count: %ld, candidate_instance_size: %lu, selected_instance: %s",
               table_id, idc.to_string().c_str(), average_count,
               candidate_instances.size(), selected_instance.c_str());
    return 0;
}

//todo, 暂时未考虑机房，后期需要考虑尽量不放在同一个机房
// 1. dead store下线之前, 选择补副本的instance
//     1.1 peer: exclude_stores是peer's store address
//     1.2 learner: exclude_stores是null 
// 2. store进行迁移
//     2.1 peer: exclude_stores是peer's store address
//     2.2 learner: exclude_stores是null 
// 3. 处理store心跳, 补region peer, exclude_stores是peer's store address
// 4. 建表, 创建每个region的第一个peer, exclude_store是null 
// 5. region split
//     5.1 尾分裂选一个instance，exclude_stores是原region leader's store address
//     5.2 中间分裂选replica-1个instance，exclude_stores是peer's store address 
// 6. 添加全局索引, exclude_store是null
int ClusterManager::select_instance_rolling(const IdcInfo& idc, 
                                    const std::set<std::string>& exclude_stores,
                                    std::string& selected_instance) {
    selected_instance.clear();
    const std::string& resource_tag = idc.resource_tag;
    BAIDU_SCOPED_LOCK(_instance_mutex);
    if (_resource_tag_instance_map.count(resource_tag) == 0 ||
            _resource_tag_instance_map[resource_tag].empty()) {
        DB_WARNING("there is no instance: idc: %s", idc.to_string().c_str());
        return -1;
    }
    if (_resource_tag_instances_by_network.find(resource_tag) == _resource_tag_instances_by_network.end()) {
        DB_WARNING("no instance in _resource_tag_instances_by_network: idc: %s", idc.to_string().c_str());
        return -1;
    }
    rolling_count << 1;
    std::set<std::string> exclude_network_segment;
    bool filter_by_network = _meta_state_machine->get_network_segment_balance(resource_tag);
    if (filter_by_network) {
        // resource open load balance by network segment, so collect overlap network segment first
        for (auto& instance_address : exclude_stores) {
            if (_instance_info.find(instance_address) == _instance_info.end()) {
                continue;
            }
            exclude_network_segment.insert(_instance_info[instance_address].network_segment);
        }
    }
        
    auto& instances_by_network = _resource_tag_instances_by_network[resource_tag];
    auto& last_rolling_position = _resource_tag_rolling_position[resource_tag];
    auto& last_rolling_network = _resource_tag_rolling_network[resource_tag];
    auto network_iter = instances_by_network.find(last_rolling_network); 
    if (network_iter == instances_by_network.end() || (++network_iter) == instances_by_network.end()) {
        network_iter = instances_by_network.begin();
        ++last_rolling_position;
    }

    std::string fallback_network_segment;
    std::string fallback_instance;
    int fallback_position = 0;
    size_t instance_count = _resource_tag_instance_map[resource_tag].size();
    size_t rolling_times = 0;
    bool has_any_instance_on_tier = true;
    for (; rolling_times < instance_count; ++network_iter) {
        // 首先轮询网段, 每次轮询一遍网段结束，则position+1，到下一层的instance
        if (network_iter == instances_by_network.end()) {
            network_iter = instances_by_network.begin();
            if (!has_any_instance_on_tier) {
                last_rolling_position = 0; 
            } else {
                ++last_rolling_position;
            }
            has_any_instance_on_tier = false;
        }
        // 拿到网段下的instance
        auto& instances = network_iter->second;
        if(last_rolling_position >= instances.size()) {
            // 这个网段的instance已经都遍历过了
            continue;
        }
        ++rolling_times;
        has_any_instance_on_tier = true; 
        auto& instance_address = instances[last_rolling_position]; 
        if (!is_legal_for_select_instance(idc, instance_address, exclude_stores)) {
            continue;
        }
        if (filter_by_network) {
            if (exclude_network_segment.find(network_iter->first) == exclude_network_segment.end()) {
                selected_instance = instance_address;
                last_rolling_network = network_iter->first;
                break; 
            } else if (fallback_network_segment.empty()) {
                fallback_network_segment = network_iter->first;
                fallback_position = last_rolling_position;
                fallback_instance = instance_address;
            }
        } else {
            selected_instance = instance_address;
            last_rolling_network = network_iter->first;
            break;
        }
    }
    if (selected_instance.empty()) {
        if (fallback_network_segment.empty()) {
            DB_WARNING("select instance fail, has no legal store, idc:%s", idc.to_string().c_str());
            return -1;
        }
        // fallback
        rolling_fallback_count << 1;
        last_rolling_network = fallback_network_segment;
        last_rolling_position = fallback_position;
        selected_instance = fallback_instance;
        DB_WARNING("rolling fallback: idc: %s", idc.to_string().c_str());
    }
    DB_WARNING("select instance rolling, idc: %s, selected_instance: %s",
               idc.to_string().c_str(), selected_instance.c_str());
    return 0;
}
int ClusterManager::load_snapshot() {
    _physical_info.clear();
    _logical_physical_map.clear();
    _instance_physical_map.clear();
    _physical_instance_map.clear();
    _instance_info.clear();
    DB_WARNING("cluster manager begin load snapshot");
    {
        auto call_func = [](std::unordered_map<std::string, InstanceSchedulingInfo>& scheduling_info) -> int {
            scheduling_info.clear();
            return 1;
        };
        _scheduling_info.Modify(call_func);
    }
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

    std::string instance_param_prefix = MetaServer::CLUSTER_IDENTIFY;
    instance_param_prefix += MetaServer::INSTANCE_PARAM_CLUSTER_IDENTIFY;
    int ret = 0;
    for (; iter->Valid(); iter->Next()) {
        if (iter->key().starts_with(instance_prefix)) {
            ret = load_instance_snapshot(instance_prefix, iter->key().ToString(), iter->value().ToString());
        } else if (iter->key().starts_with(physical_prefix)) {
            ret = load_physical_snapshot(physical_prefix, iter->key().ToString(), iter->value().ToString());
        } else if (iter->key().starts_with(logical_prefix)) {
            ret = load_logical_snapshot(logical_prefix, iter->key().ToString(), iter->value().ToString());
        } else if (iter->key().starts_with(instance_param_prefix)) {
            ret = load_instance_param_snapshot(instance_param_prefix, iter->key().ToString(), iter->value().ToString());
        } else {
            DB_FATAL("unsupport cluster info when load snapshot, key:%s", iter->key().data());
        }
        if (ret != 0) {
            DB_FATAL("ClusterManager load snapshot fail, key:%s", iter->key().data());
            return -1;
        }
    }
    BAIDU_SCOPED_LOCK(_instance_mutex);
    for(auto& resource_tag_pair : _resource_tag_instance_map) {
        auto_network_segments_division(resource_tag_pair.first);
    }
    return 0;
}
bool ClusterManager::is_legal_for_select_instance(
            const IdcInfo& idc,
            const std::string& candicate_instance,
            const std::set<std::string>& exclude_stores) {
    if (_instance_info.find(candicate_instance) == _instance_info.end()) {
        return false;
    }
    if (!idc.logical_room.empty() 
            && _instance_info[candicate_instance].logical_room != idc.logical_room) {
        return false;
    }
    if (!idc.physical_room.empty() 
            && _instance_info[candicate_instance].physical_room != idc.physical_room) {
        return false;
    }
    if (_instance_info[candicate_instance].instance_status.state != pb::NORMAL
            || _instance_info[candicate_instance].resource_tag != idc.resource_tag
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
    {
        BAIDU_SCOPED_LOCK(_instance_mutex);
        _instance_info[address] = Instance(instance_pb);
        _instance_physical_map[address] = instance_pb.physical_room();
        _physical_instance_map[instance_pb.physical_room()].insert(address);
        _resource_tag_instance_map[_instance_info[address].resource_tag].insert(address);
    }
    auto call_func = [](std::unordered_map<std::string, InstanceSchedulingInfo>& scheduling_info,
                        const pb::InstanceInfo& instance_pb) -> int {
        scheduling_info[instance_pb.address()].idc = {instance_pb.resource_tag(), instance_pb.logical_room(), instance_pb.physical_room()};
        scheduling_info[instance_pb.address()].pk_prefix_region_count = std::unordered_map<std::string, int64_t >{};
        scheduling_info[instance_pb.address()].regions_count_map = std::unordered_map<int64_t, int64_t>{};
        scheduling_info[instance_pb.address()].regions_map = std::unordered_map<int64_t, std::vector<int64_t>>{};

        return 1;
    };
    _scheduling_info.Modify(call_func, instance_pb);

    return 0;
}

int ClusterManager::load_instance_param_snapshot(const std::string& instance_param_prefix,
                                             const std::string& key, 
                                             const std::string& value) {
    std::string resource_tag_or_address(key, instance_param_prefix.size());
    pb::InstanceParam instance_param_pb;
    if (!instance_param_pb.ParseFromString(value)) {
        DB_FATAL("parse from pb fail when load instance param snapshot, key:%s", key.c_str());
        return -1;
    }
    DB_WARNING("instance_param_pb:%s", instance_param_pb.ShortDebugString().c_str());
    if (resource_tag_or_address != instance_param_pb.resource_tag_or_address()) {
        DB_FATAL("diff resource tag: %s vs %s", resource_tag_or_address.c_str(), 
                instance_param_pb.resource_tag_or_address().c_str());
        return -1;
    }

    BAIDU_SCOPED_LOCK(_instance_param_mutex);
    _instance_param_map[resource_tag_or_address] = instance_param_pb;
    
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

// return -1: add instance -2: update instance
int ClusterManager::update_instance_info(const pb::InstanceInfo& instance_info) {
    std::string instance = instance_info.address();
    if (instance.empty()) {
        return 0;
    }
    BAIDU_SCOPED_LOCK(_instance_mutex);
    if (_instance_info.find(instance) == _instance_info.end()) {
        auto tom_iter = _tombstone_instance.find(instance);
        // 删除一小时内不能插入，解决opera交替迁移导致region迁移到旧实例
        if (tom_iter != _tombstone_instance.end() && tom_iter->second.get_time() < 3600 * 1000 * 1000LL) {
            return 0;
        }
        // 校验container_id和address是否一致，不一致则不加到meta中
        if (same_with_container_id_and_address(instance_info.container_id(),
                    instance_info.address())) {
            return -1;
        } else {
            return 0;
        }
    }
    if (_instance_info[instance].resource_tag != instance_info.resource_tag()) {
        return -2;
    }
    auto& is = _instance_info[instance];
    if(instance_info.has_network_segment() && (instance_info.network_segment() != is.network_segment_self_defined)) {
        // store gflag-network_segment changed
        return -2;
    }
    is.capacity = instance_info.capacity();
    is.used_size = instance_info.used_size();
    is.resource_tag = instance_info.resource_tag();
    is.version = instance_info.version();
    is.instance_status.timestamp = butil::gettimeofday_us();
    is.dml_latency = instance_info.dml_latency();
    is.dml_qps = instance_info.dml_qps();
    is.raft_total_latency = instance_info.raft_total_latency();
    is.raft_total_qps = instance_info.raft_total_qps();
    is.select_latency = instance_info.select_latency();
    is.select_qps = instance_info.select_qps();
    is.instance_status.timestamp = butil::gettimeofday_us();
    int64_t store_rocks_check_cost = instance_info.rocks_hang_check_cost();
    auto& status = is.instance_status.state;
    if (status == pb::NORMAL) {
        if (FLAGS_store_rocks_hang_check) {
            // check store是否hang了
            if (store_rocks_check_cost >= FLAGS_store_rocks_hang_check_timeout_s * 1000 * 1000LL) {
                status = pb::SLOW;
                _slow_instances.insert(instance);
                DB_WARNING("instance:%s status SLOW, resource_tag: %s,  store_rocks_check_cost: %ld",
                        instance.c_str(), is.resource_tag.c_str(), store_rocks_check_cost);
                return 0;
            }
        }
        if (!FLAGS_need_check_slow || is.dml_latency == 0 || is.raft_total_latency / is.dml_latency <= 10) {
            return 0;
        }
        int64_t all_raft_total_latency = 0;
        int64_t all_dml_latency = 0;
        int64_t cnt = 0;
        for (auto& pair : _instance_info) {
            if (pair.second.resource_tag == is.resource_tag &&
                pair.first != instance) {
                all_raft_total_latency += pair.second.raft_total_latency;
                all_dml_latency += pair.second.dml_latency;
                ++cnt;
            }
        }
        size_t max_slow_size = cnt * 5 / 100 + 1;
        if (cnt > 5 && is.raft_total_latency > 100 * all_raft_total_latency / cnt &&
                _slow_instances.size() < max_slow_size) {
            DB_WARNING("instance:%s status SLOW, resource_tag: %s, raft_total_latency:%ld, dml_latency:%ld, "
                    "cnt:%ld, avg_raft_total_latency:%ld, avg_dml_latency:%ld",
                    instance.c_str(), is.resource_tag.c_str(), is.raft_total_latency, is.dml_latency, 
                    cnt, all_raft_total_latency / cnt, all_dml_latency / cnt);
            status = pb::SLOW;
            _slow_instances.insert(instance);
        }
    } else if (status == pb::SLOW) {
        if (FLAGS_store_rocks_hang_check 
                && store_rocks_check_cost >= FLAGS_store_rocks_hang_check_timeout_s * 1000 * 1000LL) {
            return 0;
        }
        if (!FLAGS_need_check_slow) {
            _slow_instances.erase(instance);
            status = pb::NORMAL;
            DB_WARNING("instance:%s status NORMAL, resource_tag: %s, store_rocks_check_cost: %ld",
                        instance.c_str(), is.resource_tag.c_str(), store_rocks_check_cost);
            return 0;
        }
        if (is.dml_latency > 0 && is.raft_total_latency / is.dml_latency > 10) {
            return 0;
        }
        int64_t all_raft_total_latency = 0;
        int64_t all_dml_latency = 0;
        int64_t cnt = 0;
        for (auto& pair : _instance_info) {
            if (pair.second.resource_tag == is.resource_tag && pair.first != instance) {
                all_raft_total_latency += pair.second.raft_total_latency;
                all_dml_latency += pair.second.dml_latency;
                ++cnt;
            }
        }
        if (cnt > 0 && is.dml_latency <= 2 * all_dml_latency / cnt) {
            DB_WARNING("instance:%s status NORMAL, resource_tag: %s, raft_total_latency:%ld, dml_latency:%ld, "
                    "cnt:%ld, avg_raft_total_latency:%ld, avg_dml_latency:%ld",
                    instance.c_str(), is.resource_tag.c_str(), is.raft_total_latency, is.dml_latency, 
                    cnt, all_raft_total_latency / cnt, all_dml_latency / cnt);
            _slow_instances.erase(instance);
            status = pb::NORMAL;
        }
    } else if (status != pb::MIGRATE) {
        DB_WARNING("instance:%s status return NORMAL, resource_tag: %s",
                instance.c_str(), is.resource_tag.c_str());
        status = pb::NORMAL;
    }
    return 0;
}

}//namespace
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
