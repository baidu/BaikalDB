// Copyright (c) 2022 Baidu, Inc. All Rights Reserved.
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

#include <gtest/gtest.h>

#include "namespace_manager.h"
#include "database_manager.h"
#include "table_manager.h"
#include "region_manager.h"
#include "schema_manager.h"
#include "cluster_manager.h"
#include "meta_rocksdb.h"
#include "mut_table_key.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

class TestPartition : public testing::Test {
public:
    ~TestPartition() {}
protected:
    virtual void SetUp() {
        _rocksdb = baikaldb::MetaRocksdb::get_instance();
        if (!_rocksdb) {
            return;
        }
        int ret = _rocksdb->init();
        if (ret != 0) {
            return;
        }
        _namespace_manager = baikaldb::NamespaceManager::get_instance();
        _database_manager = baikaldb::DatabaseManager::get_instance();
        _table_manager = baikaldb::TableManager::get_instance();
        _region_manager = baikaldb::RegionManager::get_instance();
        _schema_manager = baikaldb::SchemaManager::get_instance();
        _cluster_manager = baikaldb::ClusterManager::get_instance();

        butil::EndPoint addr;
        addr.ip = butil::my_ip();
        addr.port = 8081;
        braft::PeerId peer_id(addr, 0);
        _state_machine = new baikaldb::MetaStateMachine(peer_id); 
        _cluster_manager->set_meta_state_machine(_state_machine);
        _schema_manager->set_meta_state_machine(_state_machine);
    }
    virtual void TearDown() {}
    baikaldb::NamespaceManager* _namespace_manager;
    baikaldb::DatabaseManager* _database_manager;
    baikaldb::TableManager* _table_manager;
    baikaldb::RegionManager* _region_manager;
    baikaldb::SchemaManager* _schema_manager;
    baikaldb::ClusterManager* _cluster_manager;
    baikaldb::MetaStateMachine* _state_machine;
    baikaldb::MetaRocksdb*  _rocksdb;
};

namespace baikaldb {

void construct_schema(pb::SchemaInfo& schema_info) {
    schema_info.set_table_name("tbl_partition");
    schema_info.set_database("test_db");
    schema_info.set_namespace_name("test_namespace");
    schema_info.set_resource_tag("e0-nj");

    // Field
    schema_info.add_fields();
    schema_info.mutable_fields(0)->set_field_name("userid");
    schema_info.mutable_fields(0)->set_mysql_type(pb::UINT64);

    schema_info.add_fields();
    schema_info.mutable_fields(1)->set_field_name("dt");
    schema_info.mutable_fields(1)->set_mysql_type(pb::DATE);

    // Index
    schema_info.add_indexs();
    schema_info.mutable_indexs(0)->set_index_type(pb::I_PRIMARY);
    schema_info.mutable_indexs(0)->set_index_name("primary_key");
    schema_info.mutable_indexs(0)->add_field_names("userid");
    schema_info.mutable_indexs(0)->add_field_names("dt");

    // Partition
    schema_info.mutable_partition_info()->set_type(pb::PT_RANGE);
    schema_info.mutable_partition_info()->mutable_field_info()->set_field_name("dt");
    schema_info.mutable_partition_info()->set_expr_string("dt");
    auto* p_range_partition_field = schema_info.mutable_partition_info()->mutable_range_partition_field();
    p_range_partition_field->add_nodes();
    p_range_partition_field->mutable_nodes(0)->set_node_type(pb::SLOT_REF);
    p_range_partition_field->mutable_nodes(0)->set_col_type(pb::NULL_TYPE);
    p_range_partition_field->mutable_nodes(0)->set_num_children(0);
    p_range_partition_field->mutable_nodes(0)->mutable_derive_node()->set_field_name("dt");

    // Range Partition
    {
        auto* p_range_partition_info = schema_info.mutable_partition_info()->add_range_partition_infos();
        p_range_partition_info->set_partition_name("p202301");
        p_range_partition_info->mutable_less_value()->add_nodes();
        p_range_partition_info->mutable_less_value()->mutable_nodes(0)->set_node_type(pb::STRING_LITERAL);
        p_range_partition_info->mutable_less_value()->mutable_nodes(0)->set_col_type(pb::STRING);
        p_range_partition_info->mutable_less_value()->mutable_nodes(0)->set_num_children(0);
        p_range_partition_info->mutable_less_value()->mutable_nodes(0)->mutable_derive_node()->set_string_val("2023-02-01");
    }
    {
        auto* p_range_partition_info = schema_info.mutable_partition_info()->add_range_partition_infos();
        p_range_partition_info->set_partition_name("p202302");
        p_range_partition_info->mutable_less_value()->add_nodes();
        p_range_partition_info->mutable_less_value()->mutable_nodes(0)->set_node_type(pb::STRING_LITERAL);
        p_range_partition_info->mutable_less_value()->mutable_nodes(0)->set_col_type(pb::STRING);
        p_range_partition_info->mutable_less_value()->mutable_nodes(0)->set_num_children(0);
        p_range_partition_info->mutable_less_value()->mutable_nodes(0)->mutable_derive_node()->set_string_val("2023-03-01");
    }
    {
        auto* p_range_partition_info = schema_info.mutable_partition_info()->add_range_partition_infos();
        p_range_partition_info->set_partition_name("p202303");
        p_range_partition_info->mutable_range()->mutable_left_value()->add_nodes();
        p_range_partition_info->mutable_range()->mutable_left_value()->mutable_nodes(0)->set_node_type(pb::STRING_LITERAL);
        p_range_partition_info->mutable_range()->mutable_left_value()->mutable_nodes(0)->set_col_type(pb::STRING);
        p_range_partition_info->mutable_range()->mutable_left_value()->mutable_nodes(0)->set_num_children(0);
        p_range_partition_info->mutable_range()->mutable_left_value()->mutable_nodes(0)->mutable_derive_node()->set_string_val("2023-03-01");

        p_range_partition_info->mutable_range()->mutable_right_value()->add_nodes();
        p_range_partition_info->mutable_range()->mutable_right_value()->mutable_nodes(0)->set_node_type(pb::STRING_LITERAL);
        p_range_partition_info->mutable_range()->mutable_right_value()->mutable_nodes(0)->set_col_type(pb::STRING);
        p_range_partition_info->mutable_range()->mutable_right_value()->mutable_nodes(0)->set_num_children(0);
        p_range_partition_info->mutable_range()->mutable_right_value()->mutable_nodes(0)->mutable_derive_node()->set_string_val("2023-04-01");
    }
}

void construct_dynamic_partition_schema(pb::SchemaInfo& schema_info) {
    schema_info.mutable_partition_info()->mutable_dynamic_partition_attr()->set_enable(true);
    schema_info.mutable_partition_info()->mutable_dynamic_partition_attr()->set_time_unit("MONTH");
    schema_info.mutable_partition_info()->mutable_dynamic_partition_attr()->set_start(-3);
    schema_info.mutable_partition_info()->mutable_dynamic_partition_attr()->set_cold(-1);
    schema_info.mutable_partition_info()->mutable_dynamic_partition_attr()->set_end(3);
    schema_info.mutable_partition_info()->mutable_dynamic_partition_attr()->set_prefix("dp");
    schema_info.mutable_partition_info()->mutable_dynamic_partition_attr()->set_start_day_of_month(1);
}

TEST_F(TestPartition, test_pre_process_partition) {
    pb::MetaManagerRequest request;
    pb::MetaManagerResponse response;
    uint64_t log_id;
    pb::SchemaInfo* p_schema_info = request.mutable_table_info();
    ASSERT_NE(p_schema_info, nullptr);
    construct_schema(*p_schema_info);
    
    EXPECT_EQ(_schema_manager->pre_process_for_partition(&request, &response, log_id), 0);
    ASSERT_EQ(p_schema_info->partition_info().range_partition_infos_size(), 3);
    {
        EXPECT_EQ(p_schema_info->partition_info().range_partition_infos(0).partition_id(), 0);
        EXPECT_EQ(p_schema_info->partition_info().range_partition_infos(0).partition_name(), "p202301");
        ExprValue left_value;
        ExprValue right_value;
        EXPECT_EQ(partition_utils::get_partition_value(
                    p_schema_info->partition_info().range_partition_infos(0).range().left_value(), left_value), 0);
        EXPECT_EQ(partition_utils::get_partition_value(
                    p_schema_info->partition_info().range_partition_infos(0).range().right_value(), right_value), 0);
        EXPECT_EQ(left_value.get_string(), "0000-01-01");
        EXPECT_EQ(right_value.get_string(), "2023-02-01");
    }
    {
        EXPECT_EQ(p_schema_info->partition_info().range_partition_infos(1).partition_id(), 1);
        EXPECT_EQ(p_schema_info->partition_info().range_partition_infos(1).partition_name(), "p202302");
        ExprValue left_value;
        ExprValue right_value;
        EXPECT_EQ(partition_utils::get_partition_value(
                    p_schema_info->partition_info().range_partition_infos(1).range().left_value(), left_value), 0);
        EXPECT_EQ(partition_utils::get_partition_value(
                    p_schema_info->partition_info().range_partition_infos(1).range().right_value(), right_value), 0);
        EXPECT_EQ(left_value.get_string(), "2023-02-01");
        EXPECT_EQ(right_value.get_string(), "2023-03-01");
    }
    {
        EXPECT_EQ(p_schema_info->partition_info().range_partition_infos(2).partition_id(), 2);
        EXPECT_EQ(p_schema_info->partition_info().range_partition_infos(2).partition_name(), "p202303");
        ExprValue left_value;
        ExprValue right_value;
        EXPECT_EQ(partition_utils::get_partition_value(
                    p_schema_info->partition_info().range_partition_infos(2).range().left_value(), left_value), 0);
        EXPECT_EQ(partition_utils::get_partition_value(
                    p_schema_info->partition_info().range_partition_infos(2).range().right_value(), right_value), 0);
        EXPECT_EQ(left_value.get_string(), "2023-03-01");
        EXPECT_EQ(right_value.get_string(), "2023-04-01");
    }
}

TEST_F(TestPartition, test_construct_env) {
    // 增加逻辑机房
    pb::MetaManagerRequest request_logical;
    request_logical.set_op_type(pb::OP_ADD_LOGICAL);
    request_logical.mutable_logical_rooms()->add_logical_rooms("test_logical");
    _cluster_manager->add_logical(request_logical, NULL);

    // 增加物理机房
    pb::MetaManagerRequest request_physical;
    request_physical.set_op_type(pb::OP_ADD_PHYSICAL);
    request_physical.mutable_physical_rooms()->set_logical_room("test_logical");
    request_physical.mutable_physical_rooms()->add_physical_rooms("test_phyical");
    _cluster_manager->add_physical(request_physical, NULL);

    // 增加实例
    {
        pb::MetaManagerRequest request_instance;
        request_instance.set_op_type(pb::OP_ADD_INSTANCE);
        request_instance.mutable_instance()->set_address("127.0.0.1:8710");
        request_instance.mutable_instance()->set_capacity(100000);
        request_instance.mutable_instance()->set_used_size(5000);
        request_instance.mutable_instance()->set_resource_tag("e0");
        request_instance.mutable_instance()->set_physical_room("test_phyical");
        _cluster_manager->add_instance(request_instance, NULL);
    }
    {
        pb::MetaManagerRequest request_instance;
        request_instance.set_op_type(pb::OP_ADD_INSTANCE);
        request_instance.mutable_instance()->set_address("127.0.0.1:8711");
        request_instance.mutable_instance()->set_capacity(100000);
        request_instance.mutable_instance()->set_used_size(5000);
        request_instance.mutable_instance()->set_resource_tag("e0");
        request_instance.mutable_instance()->set_physical_room("test_phyical");
        _cluster_manager->add_instance(request_instance, NULL);
    }
    {
        pb::MetaManagerRequest request_instance;
        request_instance.set_op_type(pb::OP_ADD_INSTANCE);
        request_instance.mutable_instance()->set_address("127.0.0.1:8712");
        request_instance.mutable_instance()->set_capacity(100000);
        request_instance.mutable_instance()->set_used_size(5000);
        request_instance.mutable_instance()->set_resource_tag("e0");
        request_instance.mutable_instance()->set_physical_room("test_phyical");
        _cluster_manager->add_instance(request_instance, NULL);
    }
    {
        pb::MetaManagerRequest request_instance;
        request_instance.set_op_type(pb::OP_ADD_INSTANCE);
        request_instance.mutable_instance()->set_address("127.0.0.1:8713");
        request_instance.mutable_instance()->set_capacity(100000);
        request_instance.mutable_instance()->set_used_size(5000);
        request_instance.mutable_instance()->set_resource_tag("e0-nj");
        request_instance.mutable_instance()->set_physical_room("test_phyical");
        _cluster_manager->add_instance(request_instance, NULL);
    }
    {
        pb::MetaManagerRequest request_instance;
        request_instance.set_op_type(pb::OP_ADD_INSTANCE);
        request_instance.mutable_instance()->set_address("127.0.0.1:8714");
        request_instance.mutable_instance()->set_capacity(100000);
        request_instance.mutable_instance()->set_used_size(5000);
        request_instance.mutable_instance()->set_resource_tag("e0-nj");
        request_instance.mutable_instance()->set_physical_room("test_phyical");
        _cluster_manager->add_instance(request_instance, NULL);
    }
    {
        pb::MetaManagerRequest request_instance;
        request_instance.set_op_type(pb::OP_ADD_INSTANCE);
        request_instance.mutable_instance()->set_address("127.0.0.1:8715");
        request_instance.mutable_instance()->set_capacity(100000);
        request_instance.mutable_instance()->set_used_size(5000);
        request_instance.mutable_instance()->set_resource_tag("e0-nj");
        request_instance.mutable_instance()->set_physical_room("test_phyical");
        _cluster_manager->add_instance(request_instance, NULL);
    }

    // 创建Namespace    
    pb::MetaManagerRequest request_add_namespace;
    request_add_namespace.set_op_type(pb::OP_CREATE_NAMESPACE);
    request_add_namespace.mutable_namespace_info()->set_namespace_name("test_namespace");
    request_add_namespace.mutable_namespace_info()->set_quota(1024 * 1024);
    _namespace_manager->create_namespace(request_add_namespace, NULL);

    // 创建Database
    pb::MetaManagerRequest request_add_database;
    request_add_database.set_op_type(pb::OP_CREATE_DATABASE);
    request_add_database.mutable_database_info()->set_database("test_db");
    request_add_database.mutable_database_info()->set_namespace_name("test_namespace");
    request_add_database.mutable_database_info()->set_quota(10 * 1024);
    _database_manager->create_database(request_add_database, NULL);
}

// Alter Partition
TEST_F(TestPartition, test_alter_partition) {
    std::unordered_map<std::string, int64_t>            table_id_map;
    std::unordered_map<int64_t, baikaldb::TableMem>               table_info_map;
    // 创建表
    pb::MetaManagerRequest request_create_table;
    pb::MetaManagerResponse response_create_table;
    pb::SchemaInfo* p_schema_info = request_create_table.mutable_table_info();
    ASSERT_NE(p_schema_info, nullptr);
    construct_schema(*p_schema_info);
    EXPECT_EQ(_schema_manager->pre_process_for_partition(&request_create_table, &response_create_table, 0), 0);
    _table_manager->create_table(request_create_table, 1, NULL);
    {
        baikaldb::DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_manager->_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return;
        }
        table_id_map = info->table_id_map;
        table_info_map = info->table_info_map;
    }
    std::cout << table_info_map[1].schema_pb.ShortDebugString() << std::endl;

    // 检查分区是否存在
    EXPECT_EQ(_table_manager->is_range_partition_exist(table_info_map[1].schema_pb, 0), true);
    EXPECT_EQ(_table_manager->is_range_partition_exist(table_info_map[1].schema_pb, 10), false);

    // ADD Partition
    pb::MetaManagerRequest request_add_partition;
    request_add_partition.mutable_table_info()->set_table_name("tbl_partition");
    request_add_partition.mutable_table_info()->set_database("test_db");
    request_add_partition.mutable_table_info()->set_namespace_name("test_namespace");
    request_add_partition.mutable_table_info()->mutable_partition_info()->set_type(pb::PT_RANGE);
    {
        auto* p_range_partition_info = request_add_partition.mutable_table_info()->mutable_partition_info()->add_range_partition_infos();
        p_range_partition_info->set_partition_name("p202304");
        p_range_partition_info->mutable_range()->mutable_left_value()->add_nodes();
        p_range_partition_info->mutable_range()->mutable_left_value()->mutable_nodes(0)->set_node_type(pb::STRING_LITERAL);
        p_range_partition_info->mutable_range()->mutable_left_value()->mutable_nodes(0)->set_col_type(pb::STRING);
        p_range_partition_info->mutable_range()->mutable_left_value()->mutable_nodes(0)->set_num_children(0);
        p_range_partition_info->mutable_range()->mutable_left_value()->mutable_nodes(0)->mutable_derive_node()->set_string_val("2023-04-01");

        p_range_partition_info->mutable_range()->mutable_right_value()->add_nodes();
        p_range_partition_info->mutable_range()->mutable_right_value()->mutable_nodes(0)->set_node_type(pb::STRING_LITERAL);
        p_range_partition_info->mutable_range()->mutable_right_value()->mutable_nodes(0)->set_col_type(pb::STRING);
        p_range_partition_info->mutable_range()->mutable_right_value()->mutable_nodes(0)->set_num_children(0);
        p_range_partition_info->mutable_range()->mutable_right_value()->mutable_nodes(0)->mutable_derive_node()->set_string_val("2023-05-01");
    }
    _table_manager->add_partition(request_add_partition, 2, NULL);
    {
        baikaldb::DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_manager->_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return;
        }
        table_id_map = info->table_id_map;
        table_info_map = info->table_info_map;
    }
    std::cout << table_info_map[1].schema_pb.ShortDebugString() << std::endl;

    // MODIFY Partition
    pb::MetaManagerRequest request_modify_partition;
    request_modify_partition.mutable_table_info()->set_table_name("tbl_partition");
    request_modify_partition.mutable_table_info()->set_database("test_db");
    request_modify_partition.mutable_table_info()->set_namespace_name("test_namespace");
    request_modify_partition.mutable_table_info()->mutable_partition_info()->set_type(pb::PT_RANGE);
    {
        auto* p_range_partition_info = request_modify_partition.mutable_table_info()->mutable_partition_info()->add_range_partition_infos();
        p_range_partition_info->set_partition_name("p202304");
        p_range_partition_info->set_resource_tag("e0");
    }
    _table_manager->modify_partition(request_modify_partition, 3, NULL);
    {
        baikaldb::DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_manager->_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return;
        }
        table_id_map = info->table_id_map;
        table_info_map = info->table_info_map;
    }
    std::cout << table_info_map[1].schema_pb.ShortDebugString() << std::endl;

    // DROP Partition
    pb::MetaManagerRequest request_drop_partition;
    request_drop_partition.mutable_table_info()->set_table_name("tbl_partition");
    request_drop_partition.mutable_table_info()->set_database("test_db");
    request_drop_partition.mutable_table_info()->set_namespace_name("test_namespace");
    request_drop_partition.mutable_table_info()->mutable_partition_info()->set_type(pb::PT_RANGE);
    {
        auto* p_range_partition_info = request_drop_partition.mutable_table_info()->mutable_partition_info()->add_range_partition_infos();
        p_range_partition_info->set_partition_name("p202302");
    }
    {
        auto* p_range_partition_info = request_drop_partition.mutable_table_info()->mutable_partition_info()->add_range_partition_infos();
        p_range_partition_info->set_partition_name("p202304");
    }
    _table_manager->drop_partition(request_drop_partition, 4, NULL);
    {
        baikaldb::DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_manager->_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return;
        }
        table_id_map = info->table_id_map;
        table_info_map = info->table_info_map;
    }
    std::cout << table_info_map[1].schema_pb.ShortDebugString() << std::endl;
}

TEST_F(TestPartition, test_dynamic_partition) {
    // 创建动态表
    std::unordered_map<std::string, int64_t>            table_id_map;
    std::unordered_map<int64_t, baikaldb::TableMem>               table_info_map;
    pb::MetaManagerRequest request_create_table;
    pb::MetaManagerResponse response_create_table;
    pb::SchemaInfo* p_schema_info = request_create_table.mutable_table_info();
    ASSERT_NE(p_schema_info, nullptr);
    construct_schema(*p_schema_info);
    construct_dynamic_partition_schema(*p_schema_info);
    p_schema_info->set_table_name("tbl_dynamic_partition");
    EXPECT_EQ(_schema_manager->pre_process_for_partition(&request_create_table, &response_create_table, 0), 0);
    _table_manager->create_table(request_create_table, 5, NULL);
    {
        baikaldb::DoubleBufferedTableMemMapping::ScopedPtr info;
        if (_table_manager->_table_mem_infos.Read(&info) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return;
        }
        table_id_map = info->table_id_map;
        table_info_map = info->table_info_map;
    }
    std::cout << table_info_map[2].schema_pb.ShortDebugString() << std::endl;
}

} // namespace baikaldb
