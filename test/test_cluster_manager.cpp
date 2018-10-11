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
    /*测试选择实例，测之前先把数据准备好*/
    baikaldb::pb::MetaManagerRequest request_instance;
    request_instance.set_op_type(baikaldb::pb::OP_ADD_INSTANCE);
    request_instance.mutable_instance()->clear_physical_room();
    request_instance.mutable_instance()->set_address("127.0.0.1:8010");
    request_instance.mutable_instance()->set_capacity(100000);
    request_instance.mutable_instance()->set_used_size(5000);
    _cluster_manager->add_instance(request_instance, NULL);
    request_instance.mutable_instance()->clear_physical_room();
    request_instance.mutable_instance()->set_address("127.0.0.2:8010");
    request_instance.mutable_instance()->set_capacity(100000);
    request_instance.mutable_instance()->set_used_size(3000);
    _cluster_manager->add_instance(request_instance, NULL);
    request_instance.mutable_instance()->clear_physical_room();
    request_instance.mutable_instance()->set_address("127.0.0.4:8010");
    request_instance.mutable_instance()->set_capacity(100000);
    request_instance.mutable_instance()->set_used_size(5000);
    request_instance.mutable_instance()->set_resource_tag("atom");
    _cluster_manager->add_instance(request_instance, NULL);
    request_instance.mutable_instance()->clear_physical_room();
    request_instance.mutable_instance()->set_address("127.0.0.3:8010");
    request_instance.mutable_instance()->set_capacity(100000);
    request_instance.mutable_instance()->set_used_size(4000);
    request_instance.mutable_instance()->set_resource_tag("atom");
    _cluster_manager->add_instance(request_instance, NULL);
    DB_WARNING("begion test select instance");
    //// select instance
    //// 无resource_tag, 无execude_stores
    std::string resource_tag;
    std::set<std::string> exclude_stores;
    std::string selected_instance;
    int ret = _cluster_manager->select_instance_rolling(resource_tag, exclude_stores, selected_instance);
    DB_WARNING("selected instance:%s", selected_instance.c_str());
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("127.0.0.2:8010" == selected_instance);
    //轮询再选一次
    ret = _cluster_manager->select_instance_rolling(resource_tag, exclude_stores, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("127.0.0.1:8010" == selected_instance);
    //轮询再选一次
    ret = _cluster_manager->select_instance_rolling(resource_tag, exclude_stores, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("127.0.0.2:8010" == selected_instance);

    //无resource_tag, 有execude_store
    exclude_stores.insert("127.0.0.1:8010");
    ret = _cluster_manager->select_instance_rolling(resource_tag, exclude_stores, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("127.0.0.2:8010" == selected_instance);
    
    //无resource_tag, 有execude_store
    exclude_stores.clear();
    exclude_stores.insert("127.0.0.2:8010");
    ret = _cluster_manager->select_instance_rolling(resource_tag, exclude_stores, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("127.0.0.1:8010" == selected_instance);
   
    exclude_stores.insert("127.0.0.1:8010");
    ret = _cluster_manager->select_instance_rolling(resource_tag, exclude_stores, selected_instance);
    ASSERT_EQ(-1, ret);
    
    exclude_stores.clear();
    resource_tag = "atom";
    ret = _cluster_manager->select_instance_rolling(resource_tag, exclude_stores, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("127.0.0.4:8010" == selected_instance);
    
    exclude_stores.insert("127.0.0.3:8010");
    ret = _cluster_manager->select_instance_rolling(resource_tag, exclude_stores, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("127.0.0.4:8010" == selected_instance);
    
    baikaldb::ClusterManager::TableRegionMap table_regions_map;
    baikaldb::ClusterManager::TableRegionCountMap table_regions_count;
    table_regions_map[1] = std::vector<int64_t>{1, 2, 3, 4};
    table_regions_map[2] = std::vector<int64_t>{5, 6};
    table_regions_count[1] = 4;
    table_regions_count[2] = 2;
    _cluster_manager->set_instance_regions("127.0.0.1:8010", table_regions_map, table_regions_count);

    table_regions_map.clear();
    table_regions_count.clear();
    table_regions_map[1] = std::vector<int64_t>{1, 2};
    table_regions_map[2] = std::vector<int64_t>{5, 6, 7, 8};
    table_regions_count[1] = 2;
    table_regions_count[2] = 4;
    _cluster_manager->set_instance_regions("127.0.0.2:8010", table_regions_map, table_regions_count);
    
    resource_tag.clear();
    int64_t count = _cluster_manager->get_instance_count(resource_tag);
    ASSERT_EQ(2, count);
    resource_tag = "atom";
    count = _cluster_manager->get_instance_count(resource_tag);
    ASSERT_EQ(2, count);

    resource_tag.clear();
    exclude_stores.clear();
    ret = _cluster_manager->select_instance_min(resource_tag, exclude_stores, 1, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("127.0.0.2:8010" == selected_instance);

    ret = _cluster_manager->select_instance_min(resource_tag, exclude_stores, 1, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("127.0.0.2:8010" == selected_instance);
    
    ret = _cluster_manager->select_instance_min(resource_tag, exclude_stores, 1, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("127.0.0.2:8010" == selected_instance);
    
    ret = _cluster_manager->select_instance_min(resource_tag, exclude_stores, 1, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("127.0.0.1:8010" == selected_instance);
    
    ret = _cluster_manager->select_instance_min(resource_tag, exclude_stores, 2, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("127.0.0.1:8010" == selected_instance);

    ret = _cluster_manager->select_instance_min(resource_tag, exclude_stores, 2, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("127.0.0.1:8010" == selected_instance);
    
    ret = _cluster_manager->select_instance_min(resource_tag, exclude_stores, 2, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("127.0.0.2:8010" == selected_instance);

    ret = _cluster_manager->select_instance_min(resource_tag, exclude_stores, 2, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("127.0.0.1:8010" == selected_instance);

    exclude_stores.insert("127.0.0.1:8010");
    ret = _cluster_manager->select_instance_min(resource_tag, exclude_stores, 2, selected_instance);
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
    ret = _cluster_manager->select_instance_min(resource_tag, exclude_stores, 2, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("127.0.0.2:8010" == selected_instance);

    _cluster_manager->reset_instance_status();
    ret = _cluster_manager->select_instance_min(resource_tag, exclude_stores, 2, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("127.0.0.2:8010" == selected_instance);
    
    ret = _cluster_manager->select_instance_min(resource_tag, exclude_stores, 2, selected_instance);
    ASSERT_EQ(0, ret);
    ASSERT_TRUE("127.0.0.1:8010" == selected_instance);
} // TEST_F
int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
