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
    }
    virtual void TearDown() {}
    baikaldb::ClusterManager* _cluster_manager;
    baikaldb::MetaRocksdb*  _rocksdb;
};
// add_logic add_physical add_instance
TEST_F(ClusterManagerTest, test_add_and_drop) {
    baikaldb::pb::MetaManagerRequest request_logical;
    request_logical.set_op_type(baikaldb::pb::OP_ADD_LOGICAL);
    //批量增加逻辑机房
    request_logical.mutable_logical_rooms()->add_logical_rooms("ct");
    request_logical.mutable_logical_rooms()->add_logical_rooms("cu");
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
    request_logical.mutable_logical_rooms()->add_logical_rooms("yz");
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
    request_logical.mutable_logical_rooms()->add_logical_rooms("ct");
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
    request_physical.mutable_physical_rooms()->set_logical_room("bj");
    request_physical.mutable_physical_rooms()->add_physical_rooms("yf01");
    _cluster_manager->add_physical(request_physical, NULL);
    
    //增加物理机房
    request_physical.mutable_physical_rooms()->set_logical_room("ct");
    request_physical.mutable_physical_rooms()->clear_physical_rooms();
    request_physical.mutable_physical_rooms()->add_physical_rooms("cq01");
    request_physical.mutable_physical_rooms()->add_physical_rooms("yf01");
    _cluster_manager->add_physical(request_physical, NULL);
    ASSERT_EQ(4, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(3, _cluster_manager->_physical_info.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["ct"].size());
    ASSERT_EQ(3, _cluster_manager->_physical_instance_map.size());
    for (auto& pair : _cluster_manager->_physical_info) {
        DB_WARNING("physical_room:%s, logical_room:%s", pair.first.c_str(), pair.second.c_str());
    }
    for (auto& room : _cluster_manager->_logical_physical_map["ct"]) {
        DB_WARNING("physical_room:%s", room.c_str());
    }
    for (auto& pair : _cluster_manager->_physical_instance_map) {
        ASSERT_EQ(0, pair.second.size());
    }
   
    //重复添加物理机房， 应该报错
    request_physical.mutable_physical_rooms()->clear_physical_rooms();
    request_physical.mutable_physical_rooms()->add_physical_rooms("cq01");
    _cluster_manager->add_physical(request_physical, NULL);
   
    //再添加一个物理机房
    request_physical.mutable_physical_rooms()->clear_physical_rooms();
    request_physical.mutable_physical_rooms()->add_physical_rooms("nj01");
    _cluster_manager->add_physical(request_physical, NULL);
    ASSERT_EQ(4, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(4, _cluster_manager->_physical_info.size());
    ASSERT_EQ(3, _cluster_manager->_logical_physical_map["ct"].size());
    ASSERT_EQ(4, _cluster_manager->_physical_instance_map.size());
    for (auto& pair : _cluster_manager->_physical_info) {
        DB_WARNING("physical_room:%s, logical_room:%s", pair.first.c_str(), pair.second.c_str());
    }
    for (auto& room : _cluster_manager->_logical_physical_map["ct"]) {
        DB_WARNING("physical_room:%s", room.c_str());
    }
    for (auto& pair : _cluster_manager->_physical_instance_map) {
        ASSERT_EQ(0, pair.second.size());
    }
    
    //再添加一个逻辑机房的物理机房
    request_physical.mutable_physical_rooms()->clear_physical_rooms();
    request_physical.mutable_physical_rooms()->set_logical_room("cu");
    request_physical.mutable_physical_rooms()->add_physical_rooms("m1");
    request_physical.mutable_physical_rooms()->add_physical_rooms("cq02");
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
    request_instance.mutable_instance()->set_address("10.46.211.14:8010");
    request_instance.mutable_instance()->set_capacity(100000);
    request_instance.mutable_instance()->set_used_size(5000);
    _cluster_manager->add_instance(request_instance, NULL);
    //添加物理机房不存在的机器会报错
    request_instance.mutable_instance()->set_address("10.101.85.30:8010");
    request_instance.mutable_instance()->clear_physical_room();
    _cluster_manager->add_instance(request_instance, NULL);
    //重复添加实例也会报错
    request_instance.mutable_instance()->set_address("10.46.211.14:8010");
    request_instance.mutable_instance()->clear_physical_room();
    _cluster_manager->add_instance(request_instance, NULL);
    ASSERT_EQ(6, _cluster_manager->_physical_info.size());
    ASSERT_EQ(4, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_physical_instance_map["cq01"].size());
    ASSERT_EQ(1, _cluster_manager->_instance_info.size());
    //加载snapshot
    _cluster_manager->load_snapshot();
    ASSERT_EQ(6, _cluster_manager->_physical_info.size());
    ASSERT_EQ(4, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_physical_instance_map["cq01"].size());
    ASSERT_EQ(1, _cluster_manager->_instance_info.size());
    
    /* modify_tag 给实例加tag*/
    baikaldb::pb::MetaManagerRequest request_tag;
    request_tag.mutable_instance()->set_address("10.46.211.14:8010");
    request_tag.mutable_instance()->set_resource_tag("atom");
    _cluster_manager->update_instance(request_tag, NULL);
    _cluster_manager->load_snapshot();
    
    /* 添加一个南京的逻辑机房, 把nj01 从ct机房转到nj机房 */
    baikaldb::pb::MetaManagerRequest request_move;
    request_move.set_op_type(baikaldb::pb::OP_ADD_LOGICAL);
    request_move.mutable_logical_rooms()->add_logical_rooms("nj");
    _cluster_manager->add_logical(request_move, NULL);
    ASSERT_EQ(6, _cluster_manager->_physical_info.size());
    ASSERT_EQ(5, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_physical_instance_map["cq01"].size());
    ASSERT_EQ(1, _cluster_manager->_instance_info.size());
    _cluster_manager->load_snapshot();
    ASSERT_EQ(6, _cluster_manager->_physical_info.size());
    ASSERT_EQ(5, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_physical_instance_map["cq01"].size());
    ASSERT_EQ(1, _cluster_manager->_instance_info.size());

    request_move.set_op_type(baikaldb::pb::OP_MOVE_PHYSICAL);
    request_move.mutable_move_physical_request()->set_physical_room("nj01");
    request_move.mutable_move_physical_request()->set_old_logical_room("ct");
    request_move.mutable_move_physical_request()->set_new_logical_room("nj");
    _cluster_manager->move_physical(request_move, NULL);
    ASSERT_EQ(6, _cluster_manager->_physical_info.size());
    ASSERT_EQ(5, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["ct"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["nj"].size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["cu"].size());
    ASSERT_EQ(0, _cluster_manager->_logical_physical_map["yz"].size());
    _cluster_manager->load_snapshot();
    ASSERT_EQ(6, _cluster_manager->_physical_info.size());
    ASSERT_EQ(5, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["ct"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["nj"].size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["cu"].size());
    ASSERT_EQ(0, _cluster_manager->_logical_physical_map["yz"].size());

    baikaldb::pb::MetaManagerRequest request_remove_logical;
    request_remove_logical.set_op_type(baikaldb::pb::OP_DROP_LOGICAL);
    //删除一个有物理机房的逻辑机房， 报错
    request_remove_logical.mutable_logical_rooms()->add_logical_rooms("yz");
    request_remove_logical.mutable_logical_rooms()->add_logical_rooms("nj");
    _cluster_manager->drop_logical(request_remove_logical, NULL);
   
    //删除一个空的逻辑机房
    request_remove_logical.mutable_logical_rooms()->clear_logical_rooms();
    request_remove_logical.mutable_logical_rooms()->add_logical_rooms("yz");
    _cluster_manager->drop_logical(request_remove_logical, NULL);
    ASSERT_EQ(6, _cluster_manager->_physical_info.size());
    ASSERT_EQ(4, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["ct"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["nj"].size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["cu"].size());
    _cluster_manager->load_snapshot();
    ASSERT_EQ(6, _cluster_manager->_physical_info.size());
    ASSERT_EQ(4, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["ct"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["nj"].size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["cu"].size());

    //删除物理机房
    baikaldb::pb::MetaManagerRequest request_remove_physical;
    request_remove_physical.set_op_type(baikaldb::pb::OP_DROP_PHYSICAL);
    request_remove_physical.mutable_physical_rooms()->set_logical_room("cu");
    request_remove_physical.mutable_physical_rooms()->add_physical_rooms("cq02");
    _cluster_manager->drop_physical(request_remove_physical, NULL);
    ASSERT_EQ(5, _cluster_manager->_physical_info.size());
    ASSERT_EQ(4, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["ct"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["nj"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["cu"].size());
    _cluster_manager->load_snapshot();
    ASSERT_EQ(5, _cluster_manager->_physical_info.size());
    ASSERT_EQ(4, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["ct"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["nj"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["cu"].size());

    //删除物理机房
    request_remove_physical.mutable_physical_rooms()->clear_physical_rooms();
    request_remove_physical.mutable_physical_rooms()->add_physical_rooms("m1");
    _cluster_manager->drop_physical(request_remove_physical, NULL);
    ASSERT_EQ(4, _cluster_manager->_physical_info.size());
    ASSERT_EQ(4, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["ct"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["nj"].size());
    ASSERT_EQ(0, _cluster_manager->_logical_physical_map["cu"].size());
    _cluster_manager->load_snapshot();
    ASSERT_EQ(4, _cluster_manager->_physical_info.size());
    ASSERT_EQ(4, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["ct"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["nj"].size());
    ASSERT_EQ(0, _cluster_manager->_logical_physical_map["cu"].size());
    
    //删除逻辑机房
    request_remove_logical.set_op_type(baikaldb::pb::OP_DROP_LOGICAL);
    request_remove_logical.mutable_logical_rooms()->clear_logical_rooms();
    request_remove_logical.mutable_logical_rooms()->add_logical_rooms("cu");
    _cluster_manager->drop_logical(request_remove_logical, NULL);
    ASSERT_EQ(4, _cluster_manager->_physical_info.size());
    ASSERT_EQ(3, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["ct"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["nj"].size());
    _cluster_manager->load_snapshot();
    ASSERT_EQ(4, _cluster_manager->_physical_info.size());
    ASSERT_EQ(3, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["ct"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["nj"].size());
    //删除一个有实例的物理机房，报错
    request_remove_physical.set_op_type(baikaldb::pb::OP_DROP_PHYSICAL);
    request_remove_physical.mutable_physical_rooms()->clear_physical_rooms();
    request_remove_physical.mutable_physical_rooms()->set_logical_room("ct");
    request_remove_physical.mutable_physical_rooms()->add_physical_rooms("cq01");
    _cluster_manager->drop_physical(request_remove_physical, NULL);

    //删除实例
    baikaldb::pb::MetaManagerRequest request_remove_instance;
    request_remove_instance.set_op_type(baikaldb::pb::OP_DROP_INSTANCE);
    request_remove_instance.mutable_instance()->set_address("10.46.211.14:8010");
    _cluster_manager->drop_instance(request_remove_instance, NULL);
    ASSERT_EQ(4, _cluster_manager->_physical_info.size());
    ASSERT_EQ(3, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(0, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["ct"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["nj"].size());
    _cluster_manager->load_snapshot();
    ASSERT_EQ(4, _cluster_manager->_physical_info.size());
    ASSERT_EQ(3, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(0, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["ct"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["nj"].size());
    //删除物理机房
    request_remove_physical.mutable_physical_rooms()->set_logical_room("ct");
    request_remove_physical.mutable_physical_rooms()->clear_physical_rooms();
    request_remove_physical.mutable_physical_rooms()->add_physical_rooms("cq01");
    _cluster_manager->drop_physical(request_remove_physical, NULL);
    ASSERT_EQ(3, _cluster_manager->_physical_info.size());
    ASSERT_EQ(3, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(0, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["ct"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["nj"].size());
    _cluster_manager->load_snapshot();
    ASSERT_EQ(3, _cluster_manager->_physical_info.size());
    ASSERT_EQ(3, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(0, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["ct"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["nj"].size());

    /*测试选择实例，测之前先把数据准备好*/
    request_physical.mutable_physical_rooms()->set_logical_room("ct");
    request_physical.mutable_physical_rooms()->clear_physical_rooms();
    request_physical.mutable_physical_rooms()->add_physical_rooms("cq01");
    _cluster_manager->add_physical(request_physical, NULL);
    request_instance.set_op_type(baikaldb::pb::OP_ADD_INSTANCE);
    request_instance.mutable_instance()->clear_physical_room();
    request_instance.mutable_instance()->set_address("10.46.211.14:8010");
    request_instance.mutable_instance()->set_capacity(100000);
    request_instance.mutable_instance()->set_used_size(5000);
    _cluster_manager->add_instance(request_instance, NULL);
    request_instance.mutable_instance()->clear_physical_room();
    request_instance.mutable_instance()->set_address("10.46.213.43:8010");
    request_instance.mutable_instance()->set_capacity(100000);
    request_instance.mutable_instance()->set_used_size(3000);
    _cluster_manager->add_instance(request_instance, NULL);
    request_instance.mutable_instance()->clear_physical_room();
    request_instance.mutable_instance()->set_address("10.46.32.36:8010");
    request_instance.mutable_instance()->set_capacity(100000);
    request_instance.mutable_instance()->set_used_size(5000);
    request_instance.mutable_instance()->set_resource_tag("atom");
    _cluster_manager->add_instance(request_instance, NULL);
    request_instance.mutable_instance()->clear_physical_room();
    request_instance.mutable_instance()->set_address("10.38.167.54:8010");
    request_instance.mutable_instance()->set_capacity(100000);
    request_instance.mutable_instance()->set_used_size(4000);
    request_instance.mutable_instance()->set_resource_tag("atom");
    _cluster_manager->add_instance(request_instance, NULL);
    ASSERT_EQ(4, _cluster_manager->_physical_info.size());
    ASSERT_EQ(3, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(4, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["ct"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["nj"].size());
    _cluster_manager->load_snapshot();
    ASSERT_EQ(4, _cluster_manager->_physical_info.size());
    ASSERT_EQ(3, _cluster_manager->_logical_physical_map.size());
    ASSERT_EQ(4, _cluster_manager->_instance_physical_map.size());
    ASSERT_EQ(2, _cluster_manager->_logical_physical_map["ct"].size());
    ASSERT_EQ(1, _cluster_manager->_logical_physical_map["nj"].size());
   
    DB_WARNING("begion test select instance");
    //// select instance
    //// 无resource_tag, 无execude_stores
    std::string resource_tag;
    std::set<std::string> exclude_stores;
    std::string selected_instance;
    int ret = _cluster_manager->select_instance_rolling(resource_tag, exclude_stores, "", selected_instance);
    DB_WARNING("selected instance:%s", selected_instance.c_str());
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("10.46.213.43:8010" == selected_instance);
    //轮询再选一次
    ret = _cluster_manager->select_instance_rolling(resource_tag, exclude_stores, "", selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("10.46.211.14:8010" == selected_instance);
    //轮询再选一次
    ret = _cluster_manager->select_instance_rolling(resource_tag, exclude_stores, "", selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("10.46.213.43:8010" == selected_instance);

    //无resource_tag, 有execude_store
    exclude_stores.insert("10.46.211.14:8010");
    ret = _cluster_manager->select_instance_rolling(resource_tag, exclude_stores, "", selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("10.46.213.43:8010" == selected_instance);
    
    //无resource_tag, 有execude_store
    exclude_stores.clear();
    exclude_stores.insert("10.46.213.43:8010");
    ret = _cluster_manager->select_instance_rolling(resource_tag, exclude_stores, "", selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("10.46.211.14:8010" == selected_instance);
   
    exclude_stores.insert("10.46.211.14:8010");
    ret = _cluster_manager->select_instance_rolling(resource_tag, exclude_stores, "", selected_instance);
    ASSERT_EQ(-1, ret);
    
    exclude_stores.clear();
    resource_tag = "atom";
    ret = _cluster_manager->select_instance_rolling(resource_tag, exclude_stores, "", selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("10.38.167.54:8010" == selected_instance);
    
    exclude_stores.insert("10.38.167.54:8010");
    ret = _cluster_manager->select_instance_rolling(resource_tag, exclude_stores, "", selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("10.46.32.36:8010" == selected_instance);
    
    baikaldb::ClusterManager::TableRegionMap table_regions_map;
    baikaldb::ClusterManager::TableRegionCountMap table_regions_count;
    table_regions_map[1] = std::vector<int64_t>{1, 2, 3, 4};
    table_regions_map[2] = std::vector<int64_t>{5, 6};
    table_regions_count[1] = 4;
    table_regions_count[2] = 2;
    _cluster_manager->set_instance_regions("10.46.211.14:8010", table_regions_map, table_regions_count);

    table_regions_map.clear();
    table_regions_count.clear();
    table_regions_map[1] = std::vector<int64_t>{1, 2};
    table_regions_map[2] = std::vector<int64_t>{5, 6, 7, 8};
    table_regions_count[1] = 2;
    table_regions_count[2] = 4;
    _cluster_manager->set_instance_regions("10.46.213.43:8010", table_regions_map, table_regions_count);
    
    resource_tag.clear();
    int64_t count = _cluster_manager->get_instance_count(resource_tag);
    ASSERT_EQ(2, count);
    resource_tag = "atom";
    count = _cluster_manager->get_instance_count(resource_tag);
    ASSERT_EQ(2, count);

    resource_tag.clear();
    exclude_stores.clear();
    ret = _cluster_manager->select_instance_min(resource_tag, exclude_stores, 1, "", selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("10.46.213.43:8010" == selected_instance);

    ret = _cluster_manager->select_instance_min(resource_tag, exclude_stores, 1, "", selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("10.46.213.43:8010" == selected_instance);
    
    ret = _cluster_manager->select_instance_min(resource_tag, exclude_stores, 1, "", selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("10.46.213.43:8010" == selected_instance);
    
    ret = _cluster_manager->select_instance_min(resource_tag, exclude_stores, 1, "", selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("10.46.211.14:8010" == selected_instance);
    
    ret = _cluster_manager->select_instance_min(resource_tag, exclude_stores, 2, "", selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("10.46.211.14:8010" == selected_instance);

    ret = _cluster_manager->select_instance_min(resource_tag, exclude_stores, 2, "", selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("10.46.211.14:8010" == selected_instance);
    
    ret = _cluster_manager->select_instance_min(resource_tag, exclude_stores, 2, "", selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("10.46.213.43:8010" == selected_instance);

    ret = _cluster_manager->select_instance_min(resource_tag, exclude_stores, 2, "", selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("10.46.211.14:8010" == selected_instance);

    exclude_stores.insert("10.46.211.14:8010");
    ret = _cluster_manager->select_instance_min(resource_tag, exclude_stores, 2, "", selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("10.46.213.43:8010" == selected_instance);
    
    count = _cluster_manager->get_peer_count(1);
    ASSERT_EQ(10, count);

    count = _cluster_manager->get_peer_count(2);
    ASSERT_EQ(11, count);

    count = _cluster_manager->get_peer_count("10.46.211.14:8010", 1);
    ASSERT_EQ(5, count);

    count = _cluster_manager->get_peer_count("10.46.211.14:8010", 2);
    ASSERT_EQ(5, count);

    ret = _cluster_manager->set_migrate_for_instance("10.46.211.14:8010");
    ASSERT_EQ(0, ret);
    
    exclude_stores.clear();
    ret = _cluster_manager->select_instance_min(resource_tag, exclude_stores, 2, "", selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("10.46.213.43:8010" == selected_instance);

    _cluster_manager->reset_instance_status();
    ret = _cluster_manager->select_instance_min(resource_tag, exclude_stores, 2, "", selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("10.46.213.43:8010" == selected_instance);
    
    ret = _cluster_manager->select_instance_min(resource_tag, exclude_stores, 2, "", selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("10.46.211.14:8010" == selected_instance);
} // TEST_F
int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
