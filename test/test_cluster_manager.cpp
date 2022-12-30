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

#include "gtest/gtest.h"
#include "cluster_manager.h"
#include "meta_rocksdb.h"
#include <gflags/gflags.h>
DECLARE_string(default_logical_room);
class ClusterManagerTest : public testing::Test {
public:
    ~ClusterManagerTest() {}
protected:
    virtual void SetUp() {
        _rocksdb = baikaldb::MetaRocksdb::get_instance();
        if (!_rocksdb) {
            DB_FATAL("create rocksdb handler failed");
            return;
        }
        int ret = _rocksdb->init();
        if (ret != 0) {
            DB_FATAL("rocksdb init failed: code:%d", ret);
            return;
        }
        _cluster_manager = baikaldb::ClusterManager::get_instance();
        butil::EndPoint addr;
        addr.ip = butil::my_ip();
        addr.port = 8081;
        braft::PeerId peer_id(addr, 0);
        _state_machine = new baikaldb::MetaStateMachine(peer_id); 
        _cluster_manager->set_meta_state_machine(_state_machine);
    }
    virtual void TearDown() {}
    baikaldb::ClusterManager* _cluster_manager;
    baikaldb::MetaRocksdb*  _rocksdb;
    baikaldb::MetaStateMachine* _state_machine; 
};

/*
 * add_logic add_physical add_instance
 * 不开network balance，正常流程
 * rolling and min pick instance
 */ 
TEST_F(ClusterManagerTest, test_add_and_drop) {
    _state_machine->set_global_network_segment_balance(false);
    baikaldb::pb::MetaManagerRequest request_logical;
    request_logical.set_op_type(baikaldb::pb::OP_ADD_LOGICAL);
    //批量增加逻辑机房
    request_logical.mutable_logical_rooms()->add_logical_rooms("lg1");
    request_logical.mutable_logical_rooms()->add_logical_rooms("lg2");
    _cluster_manager->add_logical(request_logical, NULL);
    ASSERT_EQ(3, _cluster_manager->_logical_physical_map.size());
    for (auto& _logical_physical : _cluster_manager->_logical_physical_map) {
        if (_logical_physical.first == baikaldb::FLAGS_default_logical_room) {
            ASSERT_EQ(1, _logical_physical.second.size());
        } else {
            ASSERT_EQ(0, _logical_physical.second.size());
        }
    }
    //增加一个逻辑机房
    request_logical.mutable_logical_rooms()->clear_logical_rooms();
    request_logical.mutable_logical_rooms()->add_logical_rooms("lg3");
    _cluster_manager->add_logical(request_logical, NULL);
    ASSERT_EQ(4, _cluster_manager->_logical_physical_map.size());
    for (auto& _logical_physical : _cluster_manager->_logical_physical_map) {
        if (_logical_physical.first == baikaldb::FLAGS_default_logical_room) {
            ASSERT_EQ(1, _logical_physical.second.size());
        } else {
            ASSERT_EQ(0, _logical_physical.second.size());
        }
    }
    //重复添加逻辑机房，报错
    request_logical.mutable_logical_rooms()->clear_logical_rooms();
    request_logical.mutable_logical_rooms()->add_logical_rooms("lg1");
    _cluster_manager->add_logical(request_logical, NULL);
    //加载snapshot
    _cluster_manager->load_snapshot();
    ASSERT_EQ(4, _cluster_manager->_logical_physical_map.size());
    for (auto& _logical_physical : _cluster_manager->_logical_physical_map) {
        ASSERT_EQ(0, _logical_physical.second.size());
    }
    ASSERT_EQ(1, _cluster_manager->_physical_info.size());
    ASSERT_EQ(0, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_physical_instance_map.size());
    ASSERT_EQ(0, _cluster_manager->_instance_info.size());
    
    /* 开始测试新增物理机房 */
    baikaldb::pb::MetaManagerRequest request_physical;
    request_physical.set_op_type(baikaldb::pb::OP_ADD_PHYSICAL);
    //给一个不存在的逻辑机房加物理机房，应该报错
    request_physical.mutable_physical_rooms()->set_logical_room("lg4");
    request_physical.mutable_physical_rooms()->add_physical_rooms("py1");
    _cluster_manager->add_physical(request_physical, NULL);
    
    //增加物理机房
    request_physical.mutable_physical_rooms()->set_logical_room("lg1");
    request_physical.mutable_physical_rooms()->clear_physical_rooms();
    request_physical.mutable_physical_rooms()->add_physical_rooms("py2");
    request_physical.mutable_physical_rooms()->add_physical_rooms("py1");
    _cluster_manager->add_physical(request_physical, NULL);
    ASSERT_EQ(4, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(3, _cluster_manager->_physical_info.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["lg1"].size());
    ASSERT_EQ(3, _cluster_manager->_physical_instance_map.size());
    for (auto& pair : _cluster_manager->_physical_info) {
        DB_WARNING("physical_room:%s, logical_room:%s", pair.first.c_str(), pair.second.c_str());
    }
    for (auto& room : _cluster_manager->_logical_physical_map["lg1"]) {
        DB_WARNING("physical_room:%s", room.c_str());
    }
    for (auto& pair : _cluster_manager->_physical_instance_map) {
        ASSERT_EQ(0, pair.second.size());
    }
   
    //重复添加物理机房， 应该报错
    request_physical.mutable_physical_rooms()->clear_physical_rooms();
    request_physical.mutable_physical_rooms()->add_physical_rooms("py2");
    _cluster_manager->add_physical(request_physical, NULL);
   
    //再添加一个物理机房
    request_physical.mutable_physical_rooms()->clear_physical_rooms();
    request_physical.mutable_physical_rooms()->add_physical_rooms("lg401");
    _cluster_manager->add_physical(request_physical, NULL);
    ASSERT_EQ(4, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(4, _cluster_manager->_physical_info.size());
    ASSERT_EQ(3, _cluster_manager->_logical_physical_map["lg1"].size());
    ASSERT_EQ(4, _cluster_manager->_physical_instance_map.size());
    for (auto& pair : _cluster_manager->_physical_info) {
        DB_WARNING("physical_room:%s, logical_room:%s", pair.first.c_str(), pair.second.c_str());
    }
    for (auto& room : _cluster_manager->_logical_physical_map["lg1"]) {
        DB_WARNING("physical_room:%s", room.c_str());
    }
    for (auto& pair : _cluster_manager->_physical_instance_map) {
        ASSERT_EQ(0, pair.second.size());
    }
    
    //再添加一个逻辑机房的物理机房
    request_physical.mutable_physical_rooms()->clear_physical_rooms();
    request_physical.mutable_physical_rooms()->set_logical_room("lg2");
    request_physical.mutable_physical_rooms()->add_physical_rooms("py3");
    request_physical.mutable_physical_rooms()->add_physical_rooms("py4");
    _cluster_manager->add_physical(request_physical, NULL);
    
    //加载snapshot
    _cluster_manager->load_snapshot();
    ASSERT_EQ(4, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(6, _cluster_manager->_physical_info.size());
    ASSERT_EQ(0, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(6, _cluster_manager->_physical_instance_map.size());
    ASSERT_EQ(0, _cluster_manager->_instance_info.size());
   
    /* 测试增加实例 */
    baikaldb::pb::MetaManagerRequest request_instance;
    request_instance.set_op_type(baikaldb::pb::OP_ADD_INSTANCE);
    request_instance.mutable_instance()->set_address("127.0.0.1:8010");
    request_instance.mutable_instance()->set_capacity(100000);
    request_instance.mutable_instance()->set_used_size(5000);
    request_instance.mutable_instance()->set_physical_room("py2");
    _cluster_manager->add_instance(request_instance, NULL);
    //添加物理机房不存在的机器会报错
    request_instance.mutable_instance()->set_address("10.101.85.30:8010");
    request_instance.mutable_instance()->clear_physical_room();
    request_instance.mutable_instance()->set_physical_room("py_not_exist");
    _cluster_manager->add_instance(request_instance, NULL);
    //重复添加实例也会报错
    request_instance.mutable_instance()->set_address("127.0.0.1:8010");
    request_instance.mutable_instance()->clear_physical_room();
    request_instance.mutable_instance()->set_physical_room("py2");
    _cluster_manager->add_instance(request_instance, NULL);
    ASSERT_EQ(6, _cluster_manager->_physical_info.size());
    ASSERT_EQ(4, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_physical_instance_map["py2"].size());
    ASSERT_EQ(1, _cluster_manager->_instance_info.size());
    //加载snapshot
    _cluster_manager->load_snapshot();
    ASSERT_EQ(6, _cluster_manager->_physical_info.size());
    ASSERT_EQ(4, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_physical_instance_map["py2"].size());
    ASSERT_EQ(1, _cluster_manager->_instance_info.size());
    
    /* modify_tag 给实例加tag*/
    baikaldb::pb::MetaManagerRequest request_tag;
    request_tag.mutable_instance()->set_address("127.0.0.1:8010");
    request_tag.mutable_instance()->set_resource_tag("resource_tag1");
    _cluster_manager->update_instance(request_tag, NULL);
    _cluster_manager->load_snapshot();
    
    /* 添加一个南京的逻辑机房, 把lg401 从lg1机房转到lg4机房 */
    baikaldb::pb::MetaManagerRequest request_move;
    request_move.set_op_type(baikaldb::pb::OP_ADD_LOGICAL);
    request_move.mutable_logical_rooms()->add_logical_rooms("lg4");
    _cluster_manager->add_logical(request_move, NULL);
    ASSERT_EQ(6, _cluster_manager->_physical_info.size());
    ASSERT_EQ(5, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_physical_instance_map["py2"].size());
    ASSERT_EQ(1, _cluster_manager->_instance_info.size());
    _cluster_manager->load_snapshot();
    ASSERT_EQ(6, _cluster_manager->_physical_info.size());
    ASSERT_EQ(5, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_physical_instance_map["py2"].size());
    ASSERT_EQ(1, _cluster_manager->_instance_info.size());

    request_move.set_op_type(baikaldb::pb::OP_MOVE_PHYSICAL);
    request_move.mutable_move_physical_request()->set_physical_room("lg401");
    request_move.mutable_move_physical_request()->set_old_logical_room("lg1");
    request_move.mutable_move_physical_request()->set_new_logical_room("lg4");
    _cluster_manager->move_physical(request_move, NULL);
    ASSERT_EQ(6, _cluster_manager->_physical_info.size());
    ASSERT_EQ(5, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["lg1"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["lg4"].size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["lg2"].size());
    ASSERT_EQ(0, _cluster_manager->_logical_physical_map["lg3"].size());
    _cluster_manager->load_snapshot();
    ASSERT_EQ(6, _cluster_manager->_physical_info.size());
    ASSERT_EQ(5, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["lg1"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["lg4"].size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["lg2"].size());
    ASSERT_EQ(0, _cluster_manager->_logical_physical_map["lg3"].size());

    baikaldb::pb::MetaManagerRequest request_remove_logical;
    request_remove_logical.set_op_type(baikaldb::pb::OP_DROP_LOGICAL);
    //删除一个有物理机房的逻辑机房， 报错
    request_remove_logical.mutable_logical_rooms()->add_logical_rooms("lg3");
    request_remove_logical.mutable_logical_rooms()->add_logical_rooms("lg4");
    _cluster_manager->drop_logical(request_remove_logical, NULL);
   
    //删除一个空的逻辑机房
    request_remove_logical.mutable_logical_rooms()->clear_logical_rooms();
    request_remove_logical.mutable_logical_rooms()->add_logical_rooms("lg3");
    _cluster_manager->drop_logical(request_remove_logical, NULL);
    ASSERT_EQ(6, _cluster_manager->_physical_info.size());
    ASSERT_EQ(4, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["lg1"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["lg4"].size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["lg2"].size());
    _cluster_manager->load_snapshot();
    ASSERT_EQ(6, _cluster_manager->_physical_info.size());
    ASSERT_EQ(4, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["lg1"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["lg4"].size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["lg2"].size());

    //删除物理机房
    baikaldb::pb::MetaManagerRequest request_remove_physical;
    request_remove_physical.set_op_type(baikaldb::pb::OP_DROP_PHYSICAL);
    request_remove_physical.mutable_physical_rooms()->set_logical_room("lg2");
    request_remove_physical.mutable_physical_rooms()->add_physical_rooms("py4");
    _cluster_manager->drop_physical(request_remove_physical, NULL);
    ASSERT_EQ(5, _cluster_manager->_physical_info.size());
    ASSERT_EQ(4, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["lg1"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["lg4"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["lg2"].size());
    _cluster_manager->load_snapshot();
    ASSERT_EQ(5, _cluster_manager->_physical_info.size());
    ASSERT_EQ(4, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["lg1"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["lg4"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["lg2"].size());

    //删除物理机房
    request_remove_physical.mutable_physical_rooms()->clear_physical_rooms();
    request_remove_physical.mutable_physical_rooms()->add_physical_rooms("py3");
    _cluster_manager->drop_physical(request_remove_physical, NULL);
    ASSERT_EQ(4, _cluster_manager->_physical_info.size());
    ASSERT_EQ(4, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["lg1"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["lg4"].size());
    ASSERT_EQ(0, _cluster_manager->_logical_physical_map["lg2"].size());
    _cluster_manager->load_snapshot();
    ASSERT_EQ(4, _cluster_manager->_physical_info.size());
    ASSERT_EQ(4, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["lg1"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["lg4"].size());
    ASSERT_EQ(0, _cluster_manager->_logical_physical_map["lg2"].size());
    
    //删除逻辑机房
    request_remove_logical.set_op_type(baikaldb::pb::OP_DROP_LOGICAL);
    request_remove_logical.mutable_logical_rooms()->clear_logical_rooms();
    request_remove_logical.mutable_logical_rooms()->add_logical_rooms("lg2");
    _cluster_manager->drop_logical(request_remove_logical, NULL);
    ASSERT_EQ(4, _cluster_manager->_physical_info.size());
    ASSERT_EQ(3, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["lg1"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["lg4"].size());
    _cluster_manager->load_snapshot();
    ASSERT_EQ(4, _cluster_manager->_physical_info.size());
    ASSERT_EQ(3, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["lg1"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["lg4"].size());
    //删除一个有实例的物理机房，报错
    request_remove_physical.set_op_type(baikaldb::pb::OP_DROP_PHYSICAL);
    request_remove_physical.mutable_physical_rooms()->clear_physical_rooms();
    request_remove_physical.mutable_physical_rooms()->set_logical_room("lg1");
    request_remove_physical.mutable_physical_rooms()->add_physical_rooms("py2");
    _cluster_manager->drop_physical(request_remove_physical, NULL);

    //删除实例
    baikaldb::pb::MetaManagerRequest request_remove_instance;
    request_remove_instance.set_op_type(baikaldb::pb::OP_DROP_INSTANCE);
    request_remove_instance.mutable_instance()->set_address("127.0.0.1:8010");
    _cluster_manager->drop_instance(request_remove_instance, NULL);
    ASSERT_EQ(4, _cluster_manager->_physical_info.size());
    ASSERT_EQ(3, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(0, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["lg1"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["lg4"].size());
    _cluster_manager->load_snapshot();
    ASSERT_EQ(4, _cluster_manager->_physical_info.size());
    ASSERT_EQ(3, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(0, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["lg1"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["lg4"].size());
    //删除物理机房
    request_remove_physical.mutable_physical_rooms()->set_logical_room("lg1");
    request_remove_physical.mutable_physical_rooms()->clear_physical_rooms();
    request_remove_physical.mutable_physical_rooms()->add_physical_rooms("py2");
    _cluster_manager->drop_physical(request_remove_physical, NULL);
    ASSERT_EQ(3, _cluster_manager->_physical_info.size());
    ASSERT_EQ(3, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(0, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["lg1"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["lg4"].size());
    _cluster_manager->load_snapshot();
    ASSERT_EQ(3, _cluster_manager->_physical_info.size());
    ASSERT_EQ(3, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(0, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["lg1"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["lg4"].size());

    /*测试选择实例，测之前先把数据准备好*/
    request_physical.mutable_physical_rooms()->set_logical_room("lg1");
    request_physical.mutable_physical_rooms()->clear_physical_rooms();
    request_physical.mutable_physical_rooms()->add_physical_rooms("py2");
    _cluster_manager->add_physical(request_physical, NULL);
    request_instance.set_op_type(baikaldb::pb::OP_ADD_INSTANCE);
    request_instance.mutable_instance()->clear_physical_room();
    request_instance.mutable_instance()->set_address("127.0.0.1:8010");
    request_instance.mutable_instance()->set_capacity(100000);
    request_instance.mutable_instance()->set_used_size(5000);
    request_instance.mutable_instance()->set_physical_room("py2");
    _cluster_manager->add_instance(request_instance, NULL);
    request_instance.mutable_instance()->clear_physical_room();
    request_instance.mutable_instance()->set_address("127.0.0.2:8010");
    request_instance.mutable_instance()->set_capacity(100000);
    request_instance.mutable_instance()->set_used_size(3000);
    request_instance.mutable_instance()->set_physical_room("py2");
    _cluster_manager->add_instance(request_instance, NULL);
    request_instance.mutable_instance()->clear_physical_room();
    request_instance.mutable_instance()->set_address("127.0.0.3:8010");
    request_instance.mutable_instance()->set_capacity(100000);
    request_instance.mutable_instance()->set_used_size(5000);
    request_instance.mutable_instance()->set_resource_tag("resource_tag1");
    request_instance.mutable_instance()->set_physical_room("py2");
    _cluster_manager->add_instance(request_instance, NULL);
    request_instance.mutable_instance()->clear_physical_room();
    request_instance.mutable_instance()->set_address("127.0.0.4:8010");
    request_instance.mutable_instance()->set_capacity(100000);
    request_instance.mutable_instance()->set_used_size(4000);
    request_instance.mutable_instance()->set_resource_tag("resource_tag1");
    request_instance.mutable_instance()->set_physical_room("py2");
    _cluster_manager->add_instance(request_instance, NULL);
    ASSERT_EQ(4, _cluster_manager->_physical_info.size());
    ASSERT_EQ(3, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(4, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["lg1"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["lg4"].size());
    _cluster_manager->load_snapshot();
    ASSERT_EQ(4, _cluster_manager->_physical_info.size());
    ASSERT_EQ(3, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(4, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["lg1"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["lg4"].size());
   
    DB_WARNING("begin test select instance");
    //// select instance
    //// 无resource_tag, 无exclude_stores
    std::string resource_tag;
    std::set<std::string> exclude_stores;
    std::string selected_instance, select1, select2;
    int ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, exclude_stores, selected_instance);
    select1 = selected_instance; 
    DB_WARNING("selected instance:%s", selected_instance.c_str());
    ASSERT_EQ(0, ret);
    //轮询再选一次
    ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, exclude_stores, selected_instance);
    select2 = selected_instance;
    ASSERT_EQ(0, ret);
    ASSERT_TRUE(select1 != select2);
    //轮询再选一次
    ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, exclude_stores, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE(select1 == selected_instance);

    //无resource_tag, 有exclude_store
    exclude_stores.insert("127.0.0.1:8010");
    ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, exclude_stores, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("127.0.0.2:8010" == selected_instance);
    
    //无resource_tag, 有exclude_store
    exclude_stores.clear();
    exclude_stores.insert("127.0.0.2:8010");
    ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, exclude_stores, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("127.0.0.1:8010" == selected_instance);
   
    exclude_stores.insert("127.0.0.1:8010");
    ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, exclude_stores, selected_instance);
    ASSERT_EQ(-1, ret);
    
    exclude_stores.clear();
    resource_tag = "resource_tag1";
    ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, exclude_stores, selected_instance);
    select1 = selected_instance;
    ASSERT_EQ(0, ret);
    
    exclude_stores.insert(select1 == "127.0.0.3:8010" ? "127.0.0.4:8010" : "127.0.0.3:8010");
    ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, exclude_stores, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE(select1 == selected_instance);
    
    std::unordered_map<int64_t, std::vector<int64_t>> table_regions_map;
    std::unordered_map<int64_t, int64_t> table_regions_count;
    table_regions_map[1] = std::vector<int64_t>{1, 2, 3, 4};
    table_regions_map[2] = std::vector<int64_t>{5, 6};
    table_regions_count[1] = 4;
    table_regions_count[2] = 2;
    _cluster_manager->set_instance_regions("127.0.0.1:8010", table_regions_map, table_regions_count, {});

    table_regions_map.clear();
    table_regions_count.clear();
    table_regions_map[1] = std::vector<int64_t>{1, 2};
    table_regions_map[2] = std::vector<int64_t>{5, 6, 7, 8};
    table_regions_count[1] = 2;
    table_regions_count[2] = 4;
    _cluster_manager->set_instance_regions("127.0.0.2:8010", table_regions_map, table_regions_count, {});
    
    resource_tag.clear();
    int64_t count = _cluster_manager->get_instance_count(resource_tag);
    ASSERT_EQ(2, count);
    resource_tag = "resource_tag1";
    count = _cluster_manager->get_instance_count(resource_tag);
    ASSERT_EQ(2, count);

    resource_tag.clear();
    exclude_stores.clear();
    ret = _cluster_manager->select_instance_min({resource_tag, "", ""}, exclude_stores, 1, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("127.0.0.2:8010" == selected_instance);

    ret = _cluster_manager->select_instance_min({resource_tag, "", ""}, exclude_stores, 1, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("127.0.0.2:8010" == selected_instance);
    
    ret = _cluster_manager->select_instance_min({resource_tag, "", ""}, exclude_stores, 1, selected_instance);
    ASSERT_EQ(0, ret);
    select1 = selected_instance;
    
    ret = _cluster_manager->select_instance_min({resource_tag, "", ""}, exclude_stores, 1, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE(select1 != selected_instance);
    
    ret = _cluster_manager->select_instance_min({resource_tag, "", ""}, exclude_stores, 2, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("127.0.0.1:8010" == selected_instance);

    ret = _cluster_manager->select_instance_min({resource_tag, "", ""}, exclude_stores, 2, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("127.0.0.1:8010" == selected_instance);
    
    ret = _cluster_manager->select_instance_min({resource_tag, "", ""}, exclude_stores, 2, selected_instance);
    ASSERT_EQ(0, ret);
    select1 = selected_instance;

    ret = _cluster_manager->select_instance_min({resource_tag, "", ""}, exclude_stores, 2, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE(select1 != selected_instance);

    exclude_stores.insert("127.0.0.1:8010");
    ret = _cluster_manager->select_instance_min({resource_tag, "", ""}, exclude_stores, 2, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("127.0.0.2:8010" == selected_instance);
    
    count = _cluster_manager->get_peer_count(1);
    ASSERT_EQ(10, count);

    count = _cluster_manager->get_peer_count(2);
    ASSERT_EQ(11, count);

    count = _cluster_manager->get_peer_count("127.0.0.1:8010", 1);
    ASSERT_EQ(5, count);

    count = _cluster_manager->get_peer_count("127.0.0.1:8010", 2);
    ASSERT_EQ(5, count);

    ret = _cluster_manager->set_migrate_for_instance("127.0.0.1:8010");
    ASSERT_EQ(0, ret);
    
    exclude_stores.clear();
    ret = _cluster_manager->select_instance_min({resource_tag, "", ""}, exclude_stores, 2, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("127.0.0.2:8010" == selected_instance);

    _cluster_manager->reset_instance_status();
    ret = _cluster_manager->select_instance_min({resource_tag, "", ""}, exclude_stores, 2, selected_instance);
    ASSERT_EQ(0, ret);
    select1 = selected_instance;
    
    ret = _cluster_manager->select_instance_min({resource_tag, "", ""}, exclude_stores, 2, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE(select1 != selected_instance);
    
    exclude_stores.insert("127.0.0.1:8010");
    ret = _cluster_manager->select_instance_min({resource_tag, "", ""}, exclude_stores, 2, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("127.0.0.2:8010" == selected_instance);
} // TEST_F

/*
 * 测试不同intance分布下的网段划分，和rolling pick store
 */
TEST_F(ClusterManagerTest, test_load_balance_by_network_segment_rolling) {
    std::string selected_instance, resource_tag;
    std::unordered_map<std::string, int> pick_times;
    int ret = 0;
    size_t network_segment_size, prefix;
    std::unordered_map<std::string, std::vector<std::string> > resource_ips = {
            {"tag", {"127.1.1.1:8100"}},
            {"tag1", {"127.0.1.1:8010", "127.0.1.2:8010", "127.0.1.3:8010", "127.0.1.4:8010"}}, 
            {"tag2", {"127.100.0.1:8010", "127.100.0.2:8010", "127.101.0.1:8010", "127.101.0.2:8010",
                      "127.102.0.1:8010", "127.102.0.2:8010", "127.103.0.1:8010", "127.103.0.2:8010",
                      "127.104.0.1:8010", "127.104.0.2:8010", "127.105.0.1:8010", "127.105.0.2:8010",
                      "127.106.0.1:8010", "127.106.0.2:8010", "127.107.0.1:8010", "127.107.0.2:8010",
                      "127.108.0.1:8010", "127.108.0.2:8010", "127.109.0.1:8010", "127.109.0.2:8010"}},
            {"tag3", {"127.100.100.1:8010", "127.100.100.2:8010", "127.100.100.3:8010", "127.100.100.4:8010",
                      "127.100.100.5:8010", "127.100.100.6:8010", "127.100.100.7:8010", "127.100.100.8:8010",
                      "127.100.100.9:8010", "127.100.100.10:8010", "127.200.100.1:8010", "127.200.100.2:8010",
                      "127.201.100.1:8010", "127.202.100.1:8010", "127.203.100.1:8010", "127.204.100.1:8010",
                      "127.205.100.1:8010", "127.206.100.1:8010", "127.207.100.1:8010", "127.208.100.1:8010"}}
    };
    for(auto& resource_ip : resource_ips) {
        for(auto& ip : resource_ip.second) {
            baikaldb::pb::MetaManagerRequest request_instance;
            request_instance.set_op_type(baikaldb::pb::OP_ADD_INSTANCE);
            request_instance.mutable_instance()->set_address(ip);
            request_instance.mutable_instance()->set_capacity(100000);
            request_instance.mutable_instance()->set_used_size(5000);
            request_instance.mutable_instance()->set_physical_room("py2");
            request_instance.mutable_instance()->set_resource_tag(resource_ip.first);
            _cluster_manager->add_instance(request_instance, nullptr);
        }
    }
    _cluster_manager->load_snapshot();
    _state_machine->set_global_network_segment_balance(false);
    
    // case 1: 
    // resource tag只有1个实例, 不开网段balance，正常rolling
    resource_tag = "tag"; 
    _cluster_manager->get_network_segment_count(resource_tag, network_segment_size, prefix);
    ASSERT_EQ(1, network_segment_size);
    ASSERT_EQ(32, prefix);
    for(int i = 0; i < 40; ++i) {
        ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, {}, selected_instance);
        ASSERT_EQ(0, ret);
        pick_times[selected_instance]++; 
    }
    ASSERT_EQ(pick_times["127.1.1.1:8100"], 40);
    // 开启网段rolling
    _state_machine->set_network_segment_balance(resource_tag, true);
    for(int i = 0; i < 40; ++i) {
        ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, {}, selected_instance);
        ASSERT_EQ(0, ret);
        pick_times[selected_instance]++;
    }
    // exclude包含唯一instance, 则pick失败
    ASSERT_EQ(pick_times["127.1.1.1:8100"], 80);
    ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, {"127.1.1.1:8100"}, selected_instance);
    ASSERT_EQ(-1, ret);
    
    // case 2: 
    // resource tag下只有4个实例, 即使开了按照网段load balance, 也退化成按照ip来, 不开网段balance正常rolling
    resource_tag = "tag1";
    _cluster_manager->get_network_segment_count(resource_tag, network_segment_size, prefix);
    ASSERT_EQ(4, network_segment_size);
    ASSERT_EQ(32, prefix);
    pick_times.clear(); 
    for(int i = 0; i < 40; ++i) {
        ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, {}, selected_instance);
        pick_times[selected_instance]++;
    } 
    for(auto& pair : pick_times) {
        ASSERT_EQ(pair.second, 10);
    }
    // 开启了网段load balance
    _state_machine->set_network_segment_balance(resource_tag, true); 
    pick_times.clear();
    for(int i = 0; i < 40; ++i) {
        ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, {}, selected_instance);
        ASSERT_EQ(0, ret);
        pick_times[selected_instance]++;
    }
    for(auto& pair : pick_times) {
        ASSERT_EQ(pair.second, 10);
    }
    // 一个region有三个peer，进行load balance, 必定会选另一个网段下的instance
    ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, 
            {"127.0.1.1:8010", "127.0.1.2:8010", "127.0.1.3:8010"}, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(selected_instance, "127.0.1.4:8010"); 
    // 如果4变成migrate, 则pick失败
    ret = _cluster_manager->set_migrate_for_instance("127.0.1.4:8010");
    ASSERT_EQ(0, ret);
    ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, 
            {"127.0.1.1:8010", "127.0.1.2:8010", "127.0.1.3:8010"}, selected_instance);
    ASSERT_EQ(-1, ret);
    
    // case 3:
    // resource tag下有20个实例, 网段分布均匀, 不开网段balance, 正常rolling
    resource_tag = "tag2";
    _cluster_manager->get_network_segment_count(resource_tag, network_segment_size, prefix);
    ASSERT_EQ(10, network_segment_size);
    ASSERT_EQ(16, prefix); 
    pick_times.clear();
    for(int i = 0; i < 40; ++i) {
        ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, {}, selected_instance);
        ASSERT_EQ(0, ret);
        pick_times[selected_instance]++; 
    }
    for(auto& pair : pick_times) {
        ASSERT_EQ(pair.second, 2);
    }
    // 开启了网段balance
    _state_machine->set_network_segment_balance(resource_tag, true);
    for(int i = 0; i < 40; ++i) {
        ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, {}, selected_instance);
        ASSERT_EQ(0, ret);
        pick_times[selected_instance]++;
    }
    for(auto& pair : pick_times) {
        ASSERT_EQ(pair.second, 4);
    }
    // rolling一万次，每次随机挑选三个instance作为peer
    // 完全随机的情况下，应该每个instance被pick500次，最后每个instance的pick_time相对均匀
    for(int i = 0; i < 10000; ++i) {
        std::set<std::string> peer_list;
        for(auto j = 0; j < 3 && !resource_ips[resource_tag].empty(); ++j) {
            size_t random_index = butil::fast_rand() % resource_ips[resource_tag].size();
            peer_list.insert(resource_ips[resource_tag][random_index]);
        }
        ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, peer_list, selected_instance);
        ASSERT_EQ(0, ret);
        pick_times[selected_instance]++; 
    }
    for(auto& pair : pick_times) {
        ASSERT_TRUE(pair.second < 750);
    }
    // case 3:
    // resource tag下有20个实例，其中一个网段包含50%的instance
    resource_tag = "tag3";
    _cluster_manager->get_network_segment_count(resource_tag, network_segment_size, prefix);
    pick_times.clear();
    for(int i = 0; i < 40; ++i) {
        ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, {}, selected_instance);
        ASSERT_EQ(0, ret);
        pick_times[selected_instance]++;
    }
    for(auto& pair : pick_times) {
        ASSERT_EQ(pair.second, 2);
    }
    // 开启网段balance
    _state_machine->set_network_segment_balance(resource_tag, true);
    for(int i = 0; i < 40; ++i) {
        ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, {}, selected_instance);
        ASSERT_EQ(0, ret);
        pick_times[selected_instance]++;
    }
    for(auto& pair : pick_times) {
        ASSERT_EQ(pair.second, 4);
    }
    // rolling一万次，每次随机挑选三个instance作为peer
    // 完全随机的情况下，应该每个instance被pick500次，最后每个instance的pick_time相对均匀
    for(int i = 0; i < 10000; ++i) {
        std::set<std::string> peer_list;
        for(auto j = 0; j < 3 && !resource_ips[resource_tag].empty(); ++j) {
            size_t random_index = butil::fast_rand() % resource_ips[resource_tag].size();
            peer_list.insert(resource_ips[resource_tag][random_index]);
        }
        ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, peer_list, selected_instance);
        ASSERT_EQ(0, ret);
        pick_times[selected_instance]++;
    }
    for(auto& pair : pick_times) {
        ASSERT_TRUE(pair.second < 750);
    }
}

/* 
 * 测试add instance/drop instance/update instance resource tag三种情况下
 * 网段信息的动态维护
 */
TEST_F(ClusterManagerTest, test_load_balance_by_network_segment_add_drop_instance) {
    std::string selected_instance, resource_tag;
    std::unordered_map<std::string, int> pick_times;
    int ret = 0;
    size_t network_segment_size, prefix;
    
    // 加入"10.0.0.0:8010", 开启全局网段balance开关
    baikaldb::pb::MetaManagerRequest request_instance;
    request_instance.set_op_type(baikaldb::pb::OP_ADD_INSTANCE);
    request_instance.mutable_instance()->set_address("10.0.0.0:8010");
    request_instance.mutable_instance()->set_capacity(100000);
    request_instance.mutable_instance()->set_used_size(5000);
    request_instance.mutable_instance()->set_physical_room("py2");
    request_instance.mutable_instance()->set_resource_tag("add_drop_tag1");
    _cluster_manager->add_instance(request_instance, NULL);
    _cluster_manager->load_snapshot();
    _state_machine->set_global_network_segment_balance(true);
    resource_tag = "add_drop_tag1";
    // 只有一个网段，且prefix=32
    _cluster_manager->get_network_segment_count(resource_tag, network_segment_size, prefix);
    ASSERT_EQ(1, network_segment_size);
    ASSERT_EQ(32, prefix);
    // rolling 30次，唯一的instance被pick 30次
    for(int i = 0; i < 30; ++i) {
        ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, {}, selected_instance);
        ASSERT_EQ(0, ret);
        ASSERT_EQ("10.0.0.0:8010", selected_instance);
    }
    
    // 加入"10.0.0.0:8010"
    request_instance.set_op_type(baikaldb::pb::OP_ADD_INSTANCE);
    request_instance.mutable_instance()->set_address("10.222.0.1:8010");
    request_instance.mutable_instance()->set_capacity(100000);
    request_instance.mutable_instance()->set_used_size(5000);
    request_instance.mutable_instance()->set_physical_room("py2");
    request_instance.mutable_instance()->set_resource_tag(resource_tag);
    _cluster_manager->add_instance(request_instance, NULL);
    _cluster_manager->get_network_segment_count(resource_tag, network_segment_size, prefix);
    // 有两个网段，prefix=32
    ASSERT_EQ(2, network_segment_size);
    ASSERT_EQ(32, prefix);
    // rolling 30次，每个instance被pick 15次
    for(int i = 0; i < 30; ++i) {
        ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, {}, selected_instance);
        ASSERT_EQ(0, ret);
        pick_times[selected_instance]++;
    }
    ASSERT_EQ(15, pick_times["10.0.0.0:8010"]);
    ASSERT_EQ(15, pick_times["10.222.0.1:8010"]);
    
    // 再加一个instance
    request_instance.set_op_type(baikaldb::pb::OP_ADD_INSTANCE);
    request_instance.mutable_instance()->set_address("10.223.0.1:8010");
    request_instance.mutable_instance()->set_capacity(100000);
    request_instance.mutable_instance()->set_used_size(5000);
    request_instance.mutable_instance()->set_physical_room("py2");
    request_instance.mutable_instance()->set_resource_tag(resource_tag);
    _cluster_manager->add_instance(request_instance, NULL);
    _cluster_manager->get_network_segment_count(resource_tag, network_segment_size, prefix);
    // 现在有三个网段，且prefix=32
    ASSERT_EQ(3, network_segment_size);
    ASSERT_EQ(32, prefix);
    // rolling 30次，每个instance被pick 10次
    pick_times.clear();
    for(int i = 0; i < 30; ++i) {
        ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, {}, selected_instance);
        ASSERT_EQ(0, ret);
        pick_times[selected_instance]++;
    }
    ASSERT_EQ(10, pick_times["10.0.0.0:8010"]);
    ASSERT_EQ(10, pick_times["10.222.0.1:8010"]);
    ASSERT_EQ(10, pick_times["10.223.0.1:8010"]);
    
    // 删除一个instance
    baikaldb::pb::MetaManagerRequest request_remove_instance;
    request_remove_instance.set_op_type(baikaldb::pb::OP_DROP_INSTANCE);
    request_remove_instance.mutable_instance()->set_address("10.0.0.0:8010");
    _cluster_manager->drop_instance(request_remove_instance, NULL);
    // 现在只有两个网段
    _cluster_manager->get_network_segment_count(resource_tag, network_segment_size, prefix);
    ASSERT_EQ(2, network_segment_size);
    ASSERT_EQ(32, prefix);
    // rolling 30次，剩下的两个instance每个被pick 15次
    pick_times.clear();
    for(int i = 0; i < 30; ++i) {
        ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, {}, selected_instance);
        pick_times[selected_instance]++;
        ASSERT_EQ(0, ret);
        ASSERT_NE("10.0.0.0:8010", selected_instance);
    }
    ASSERT_EQ(15, pick_times["10.222.0.1:8010"]);
    ASSERT_EQ(15, pick_times["10.223.0.1:8010"]);
    
    // 更改一个instance的resource tag，同步更改network信息
    baikaldb::pb::MetaManagerRequest request_change_resource_tag1;
    request_change_resource_tag1.set_op_type(baikaldb::pb::OP_UPDATE_INSTANCE);
    request_change_resource_tag1.mutable_instance()->set_address("10.223.0.1:8010");
    request_change_resource_tag1.mutable_instance()->set_capacity(100000);
    request_change_resource_tag1.mutable_instance()->set_used_size(5000);
    request_change_resource_tag1.mutable_instance()->set_physical_room("py2");
    request_change_resource_tag1.mutable_instance()->set_resource_tag("add_drop_tag2");
    _cluster_manager->update_instance(request_change_resource_tag1, nullptr);
    // 剩下的两个instance，每个属于一个resource tag，每个instance对应的resource tag下的网段只有1个，且prefix=32
    _cluster_manager->get_network_segment_count(resource_tag, network_segment_size, prefix);
    ASSERT_EQ(1, network_segment_size);
    ASSERT_EQ(32, prefix);
    _cluster_manager->get_network_segment_count("add_drop_tag2", network_segment_size, prefix);
    ASSERT_EQ(1, network_segment_size);
    ASSERT_EQ(32, prefix);
    // 对2个resource tag，都rolling 100次，正常
    for(int i = 0; i < 100; ++i) {
        ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, {}, selected_instance);
        ASSERT_EQ(0, ret);
        ASSERT_EQ("10.222.0.1:8010", selected_instance);
        ret = _cluster_manager->select_instance_rolling({"add_drop_tag2", "", ""}, {}, selected_instance);
        ASSERT_EQ(0, ret);
        ASSERT_EQ("10.223.0.1:8010", selected_instance);
    }
}

/*
 * 测试开启了网段balance, store通过gflag自定义网段的兼容性
 */
TEST_F(ClusterManagerTest, test_load_balance_by_network_segment_with_gflag) {
    std::string selected_instance, resource_tag;
    std::unordered_map<std::string, int> pick_times;
    int ret = 0;
    size_t network_segment_size, prefix;
    resource_tag = "gflag_tag";
    
    // 初始化&add instance, 开启全局的network balance
    std::unordered_map<std::string, std::vector<std::string> > resource_ips = {
            {"gflag_tag", {"11.0.0.1:8010", "11.0.0.2:8010", "11.0.0.3:8010", "11.0.0.4:8010"}},
    };
    for(auto& resource_ip : resource_ips) {
        for(auto& ip : resource_ip.second) {
            baikaldb::pb::MetaManagerRequest request_instance;
            request_instance.set_op_type(baikaldb::pb::OP_ADD_INSTANCE);
            request_instance.mutable_instance()->set_address(ip);
            request_instance.mutable_instance()->set_capacity(100000);
            request_instance.mutable_instance()->set_used_size(5000);
            request_instance.mutable_instance()->set_physical_room("py2");
            request_instance.mutable_instance()->set_resource_tag(resource_ip.first);
            _cluster_manager->add_instance(request_instance, NULL);
        }
    }
    _cluster_manager->load_snapshot();
    _state_machine->set_network_segment_balance(resource_tag, true);
    // 一共4个网段，每个instance属于一个网段，prefix=32
    _cluster_manager->get_network_segment_count(resource_tag, network_segment_size, prefix);
    ASSERT_EQ(4, network_segment_size);
    ASSERT_EQ(32, prefix);
    // rolling 40次，每个instance被pick 10次。
    for(int i = 0; i < 40; ++i) {
        ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, {}, selected_instance);
        ASSERT_EQ(0, ret);
        pick_times[selected_instance]++;
    }
    ASSERT_EQ(10, pick_times["11.0.0.1:8010"]);
    ASSERT_EQ(10, pick_times["11.0.0.2:8010"]);
    ASSERT_EQ(10, pick_times["11.0.0.3:8010"]);
    ASSERT_EQ(10, pick_times["11.0.0.4:8010"]);
    
    // 模拟用户自定义store网段，更改两个instance的network segment gflag，自定义网段为bj-network1，模拟汇报心跳
    baikaldb::pb::MetaManagerRequest request_change_network_tag1;
    request_change_network_tag1.set_op_type(baikaldb::pb::OP_UPDATE_INSTANCE);
    request_change_network_tag1.mutable_instance()->set_address("11.0.0.1:8010");
    request_change_network_tag1.mutable_instance()->set_capacity(100000);
    request_change_network_tag1.mutable_instance()->set_used_size(5000);
    request_change_network_tag1.mutable_instance()->set_physical_room("py2");
    request_change_network_tag1.mutable_instance()->set_resource_tag("gflag_tag");
    request_change_network_tag1.mutable_instance()->set_network_segment("bj-network1");
    _cluster_manager->update_instance(request_change_network_tag1, nullptr);
    request_change_network_tag1.mutable_instance()->set_address("11.0.0.2:8010");
    _cluster_manager->update_instance(request_change_network_tag1, nullptr);
    // 测试现在3个网段，一个bj-network1(2instance), 剩下两个instance，每个属于一个网段。
    _cluster_manager->get_network_segment_count(resource_tag, network_segment_size, prefix);
    ASSERT_EQ(3, network_segment_size);
    ASSERT_EQ(32, prefix);
    // 正常rolling 40次，每个instance pick 10次
    pick_times.clear();
    for(int i = 0; i < 40; ++i) {
        ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, {}, selected_instance);
        ASSERT_EQ(0, ret);
        pick_times[selected_instance]++;
    }
    ASSERT_EQ(10, pick_times["11.0.0.1:8010"]);
    ASSERT_EQ(10, pick_times["11.0.0.2:8010"]);
    ASSERT_EQ(10, pick_times["11.0.0.3:8010"]);
    ASSERT_EQ(10, pick_times["11.0.0.4:8010"]);
    // 3，4被exclude，则在bj-network1进行pick，其中1，2每个被pick 10次
    pick_times.clear();
    for(int i = 0; i < 40; ++i) {
        ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, {"11.0.0.3:8010", "11.0.0.4:8010"}, selected_instance);
        ASSERT_EQ(0, ret);
        pick_times[selected_instance]++;
    }
    ASSERT_EQ(20, pick_times["11.0.0.1:8010"]);
    ASSERT_EQ(20, pick_times["11.0.0.2:8010"]);
    // bj-network1，4被exclude，则只能pick 3
    pick_times.clear();
    for(int i = 0; i < 40; ++i) {
        ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, {"11.0.0.1:8010", "11.0.0.4:8010"}, selected_instance);
        ASSERT_EQ(0, ret);
        ASSERT_EQ("11.0.0.3:8010", selected_instance);
    }
    // 重新load snapshot也保留用户gflag信息
    _cluster_manager->load_snapshot();
    _cluster_manager->get_network_segment_count(resource_tag, network_segment_size, prefix);
    ASSERT_EQ(3, network_segment_size);
    ASSERT_EQ(32, prefix);
    
    // 模拟用户取消store网段gflag，走update_instance
    request_change_network_tag1.set_op_type(baikaldb::pb::OP_UPDATE_INSTANCE);
    request_change_network_tag1.mutable_instance()->set_address("11.0.0.1:8010");
    request_change_network_tag1.mutable_instance()->set_capacity(100000);
    request_change_network_tag1.mutable_instance()->set_used_size(5000);
    request_change_network_tag1.mutable_instance()->set_physical_room("py2");
    request_change_network_tag1.mutable_instance()->set_resource_tag("gflag_tag");
    request_change_network_tag1.mutable_instance()->set_network_segment("");
    _cluster_manager->update_instance(request_change_network_tag1, nullptr);
    request_change_network_tag1.mutable_instance()->set_address("11.0.0.2:8010");
    _cluster_manager->update_instance(request_change_network_tag1, nullptr);
    // 恢复4个网段，prefix=32
    _cluster_manager->get_network_segment_count(resource_tag, network_segment_size, prefix);
    ASSERT_EQ(4, network_segment_size);
    ASSERT_EQ(32, prefix);
    // 现在1，2不属于同一个网段bj-network1, 模拟1，4被exclude，则2，3每个被pick 20次
    pick_times.clear();
    for(int i = 0; i < 40; ++i) {
        ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, {"11.0.0.1:8010", "11.0.0.4:8010"}, selected_instance);
        ASSERT_EQ(0, ret);
        pick_times[selected_instance]++;
    }
    ASSERT_EQ(20, pick_times["11.0.0.2:8010"]);
    ASSERT_EQ(20, pick_times["11.0.0.3:8010"]);
}

/*
 * 测试添加集群
 */
TEST_F(ClusterManagerTest, test_add_cluster) {
    std::string selected_instance, resource_tag;
    std::unordered_map<std::string, int> pick_times;
    int ret = 0;
    size_t network_segment_size, prefix;
    resource_tag = "add_cluster";
    
    // 初始化&add instance, 开启全局的network balance
    DB_WARNING("start add 100 instance");
    for(auto i = 0; i < 100; ++i) {
        std::string ip = "12.1." + std::to_string(i) + ".100:8000"; 
        baikaldb::pb::MetaManagerRequest request_instance;
        request_instance.set_op_type(baikaldb::pb::OP_ADD_INSTANCE);
        request_instance.mutable_instance()->set_address(ip);
        request_instance.mutable_instance()->set_capacity(100000);
        request_instance.mutable_instance()->set_used_size(5000);
        request_instance.mutable_instance()->set_physical_room("py2");
        request_instance.mutable_instance()->set_resource_tag(resource_tag);
        _cluster_manager->add_instance(request_instance, NULL);
    }
    DB_WARNING("end add 100 instance");
    _state_machine->set_network_segment_balance(resource_tag, true);
    _cluster_manager->get_network_segment_count(resource_tag, network_segment_size, prefix);
    ASSERT_EQ(13, network_segment_size);
    ASSERT_EQ(21, prefix);
    for(int i = 0; i < 1000; ++i) {
        ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, {}, selected_instance);
        ASSERT_EQ(0, ret);
        pick_times[selected_instance]++;
    }
    for(int i = 0; i < 100; ++i) {
        std::string ip = "12.1." + std::to_string(i) + ".100:8000";
        ASSERT_EQ(10, pick_times[ip]);
    }
    
    DB_WARNING("start add 100 instance again");
    for(auto i = 0; i < 100; ++i) {
        std::string ip = "12.1." + std::to_string(i) + ".101:8000";
        baikaldb::pb::MetaManagerRequest request_instance;
        request_instance.set_op_type(baikaldb::pb::OP_ADD_INSTANCE);
        request_instance.mutable_instance()->set_address(ip);
        request_instance.mutable_instance()->set_capacity(100000);
        request_instance.mutable_instance()->set_used_size(5000);
        request_instance.mutable_instance()->set_physical_room("py2");
        request_instance.mutable_instance()->set_resource_tag(resource_tag);
        _cluster_manager->add_instance(request_instance, NULL);
    }
    DB_WARNING("start add 100 instance again end");
    pick_times.clear();
    _cluster_manager->get_network_segment_count(resource_tag, network_segment_size, prefix);
    ASSERT_EQ(13, network_segment_size);
    ASSERT_EQ(21, prefix);
    // rolling 40次，每个instance被pick 10次。
    for(int i = 0; i < 1000; ++i) {
        ret = _cluster_manager->select_instance_rolling({resource_tag, "", ""}, {}, selected_instance);
        ASSERT_EQ(0, ret);
        pick_times[selected_instance]++;
    }
    for(int i = 0; i < 100; ++i) {
        std::string ip = "12.1." + std::to_string(i) + ".100:8000";
        std::string ip2 = "12.1." + std::to_string(i) + ".101:8000";
        ASSERT_EQ(5, pick_times[ip]);
        ASSERT_EQ(5, pick_times[ip2]);
    }
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
