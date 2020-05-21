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
#include "schema_manager.h"
#include "cluster_manager.h"
namespace baikaldb {
    DECLARE_string(db_path);
}

class SchemaManagerTest : public testing::Test {
public:
    ~SchemaManagerTest() {}
protected:
    virtual void SetUp() {
        baikaldb::RocksWrapper* rocksdb = baikaldb::RocksWrapper::get_instance();
        if (!rocksdb) {
            DB_FATAL("create rocksdb handler failed");
            return;
        }
        int ret = rocksdb->init("./rocks_db");
        if (ret != 0) {
        DB_FATAL("rocksdb init failed: code:%d", ret);
            DB_FATAL("rocksdb init failed: code:%d", ret);
            return;
        }
        _cluster_manager = new baikaldb::ClusterManager(NULL); 
        _schema_manager = new baikaldb::SchemaManager(NULL);
        _schema_manager->set_cluster_manager(_cluster_manager);
    }
    virtual void TearDown() {
        delete _cluster_manager;
        delete _schema_manager;
    }
    baikaldb::ClusterManager* _cluster_manager;
    baikaldb::SchemaManager* _schema_manager;
};
// add_logic add_physical add_instance
TEST_F(SchemaManagerTest, test_create_drop_modify) {
    //测试点：添加region
    baikaldb::pb::MetaManagerRequest request_update_region_fc;
    request_update_region_fc.set_op_type(baikaldb::pb::OP_UPDATE_REGION);
    request_update_region_fc.mutable_region_info()->set_region_id(1);
    request_update_region_fc.mutable_region_info()->set_table_id(1);
    request_update_region_fc.mutable_region_info()->set_table_name("userinfo");
    request_update_region_fc.mutable_region_info()->set_partition_id(0);
    request_update_region_fc.mutable_region_info()->set_replica_num(3);
    request_update_region_fc.mutable_region_info()->set_version(1);
    request_update_region_fc.mutable_region_info()->add_peers("127.0.0.1:8010");
    request_update_region_fc.mutable_region_info()->set_leader("127.0.0.1:8010");
    request_update_region_fc.mutable_region_info()->set_status(baikaldb::pb::IDLE);
    request_update_region_fc.mutable_region_info()->set_used_size(1024);
    request_update_region_fc.mutable_region_info()->set_log_index(1);
    _schema_manager->update_region(request_update_region_fc, 1, NULL);
    //for (auto& region_info : _schema_manager->_region_info_map) {
    //    DB_WARNING("region_id:%ld, region_info:%s", 
    //            region_info.first, region_info.second.ShortDebugString().c_str());
    //}
    for (auto& instance  : _schema_manager->_instance_region_map) {
        DB_WARNING("instance:%s", instance.first.c_str());
        for (auto& region_id : instance.second) {
            DB_WARNING("region_id:%ld", region_id);
        }
    }
    for (auto& table_id : _schema_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        DB_WARNING("whether_levle_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%ld, field_name:%s", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
        }
    }
    _schema_manager->load_snapshot();
    //for (auto& region_info : _schema_manager->_region_info_map) {
    //    DB_WARNING("region_id:%ld, region_info:%s", 
    //            region_info.first, region_info.second.ShortDebugString().c_str());
    //}
    for (auto& instance  : _schema_manager->_instance_region_map) {
        DB_WARNING("instance:%s", instance.first.c_str());
        for (auto& region_id : instance.second) {
            DB_WARNING("region_id:%ld", region_id);
        }
    }
    for (auto& table_id : _schema_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        DB_WARNING("whether_levle_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%ld, field_name:%s", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
        }
    }
    //测试点：region 分离，分配一个新的region-id
    baikaldb::pb::MetaManagerRequest request_split_region_fc;
    request_split_region_fc.mutable_region_split()->set_region_id(1);
    _schema_manager->split_region(request_split_region_fc, NULL);
    ASSERT_EQ(2, _schema_manager->_max_region_id);
    for (auto& table_id : _schema_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        DB_WARNING("whether_levle_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%ld, field_name:%s", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
        }
    }
    _schema_manager->load_snapshot();
    ASSERT_EQ(2, _schema_manager->_max_region_id);
    for (auto& table_id : _schema_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        DB_WARNING("whether_levle_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%ld, field_name:%s", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
        }
    }
    //测试点：更新reigon_id : 1
    request_update_region_fc.mutable_region_info()->set_region_id(1);
    request_update_region_fc.mutable_region_info()->set_table_id(1);
    request_update_region_fc.mutable_region_info()->set_table_name("userinfo");
    request_update_region_fc.mutable_region_info()->set_partition_id(0);
    request_update_region_fc.mutable_region_info()->set_replica_num(3);
    request_update_region_fc.mutable_region_info()->set_version(1);
    request_update_region_fc.mutable_region_info()->clear_peers();
    request_update_region_fc.mutable_region_info()->add_peers("127.0.0.2:8010");
    request_update_region_fc.mutable_region_info()->set_leader("127.0.0.2:8010");
    request_update_region_fc.mutable_region_info()->set_status(baikaldb::pb::IDLE);
    request_update_region_fc.mutable_region_info()->set_used_size(1024);
    request_update_region_fc.mutable_region_info()->set_log_index(1);
    _schema_manager->update_region(request_update_region_fc, 2, NULL);
    //for (auto& region_info : _schema_manager->_region_info_map) {
    //    DB_WARNING("region_id:%ld, region_info:%s", 
    //            region_info.first, region_info.second.ShortDebugString().c_str());
    //}
    for (auto& instance  : _schema_manager->_instance_region_map) {
        DB_WARNING("instance:%s", instance.first.c_str());
        for (auto& region_id : instance.second) {
            DB_WARNING("region_id:%ld", region_id);
        }
    }
    for (auto& table_info : _schema_manager->_table_info_map) {
        DB_WARNING("table_id:%ld, table_info:%s", 
                table_info.first, table_info.second.schema_pb.ShortDebugString().c_str());
    }
    _schema_manager->load_snapshot();
    //for (auto& region_info : _schema_manager->_region_info_map) {
    //    DB_WARNING("region_id:%ld, region_info:%s", 
    //            region_info.first, region_info.second.ShortDebugString().c_str());
    //}
    for (auto& instance  : _schema_manager->_instance_region_map) {
        DB_WARNING("instance:%s", instance.first.c_str());
        for (auto& region_id : instance.second) {
            DB_WARNING("region_id:%ld", region_id);
        }
    }
    for (auto& table_info : _schema_manager->_table_info_map) {
        DB_WARNING("table_id:%ld, table_info:%s", 
                table_info.first, table_info.second.schema_pb.ShortDebugString().c_str());
    }
    //测试点：增加reigon_id : 2
    request_update_region_fc.mutable_region_info()->set_region_id(2);
    request_update_region_fc.mutable_region_info()->set_table_id(1);
    request_update_region_fc.mutable_region_info()->set_table_name("userinfo");
    request_update_region_fc.mutable_region_info()->set_partition_id(0);
    request_update_region_fc.mutable_region_info()->set_replica_num(3);
    request_update_region_fc.mutable_region_info()->set_version(1);
    request_update_region_fc.mutable_region_info()->clear_peers();
    request_update_region_fc.mutable_region_info()->add_peers("127.0.0.1:8010");
    request_update_region_fc.mutable_region_info()->set_leader("127.0.0.1:8010");
    request_update_region_fc.mutable_region_info()->set_status(baikaldb::pb::IDLE);
    request_update_region_fc.mutable_region_info()->set_used_size(102);
    request_update_region_fc.mutable_region_info()->set_log_index(1);
    _schema_manager->update_region(request_update_region_fc, 3, NULL);
    //for (auto& region_info : _schema_manager->_region_info_map) {
    //    DB_WARNING("region_id:%ld, region_info:%s", 
    //            region_info.first, region_info.second.ShortDebugString().c_str());
    //}
    for (auto& instance  : _schema_manager->_instance_region_map) {
        DB_WARNING("instance:%s", instance.first.c_str());
        for (auto& region_id : instance.second) {
            DB_WARNING("region_id:%ld", region_id);
        }
    }
    for (auto& table_info : _schema_manager->_table_info_map) {
        DB_WARNING("table_id:%ld, table_info:%s", 
                table_info.first, table_info.second.schema_pb.ShortDebugString().c_str());
    }
    _schema_manager->load_snapshot();
    //for (auto& region_info : _schema_manager->_region_info_map) {
    //    DB_WARNING("region_id:%ld, region_info:%s", 
    //            region_info.first, region_info.second.ShortDebugString().c_str());
    //}
    for (auto& instance  : _schema_manager->_instance_region_map) {
        DB_WARNING("instance:%s", instance.first.c_str());
        for (auto& region_id : instance.second) {
            DB_WARNING("region_id:%ld", region_id);
        }
    }
    for (auto& table_info : _schema_manager->_table_info_map) {
        DB_WARNING("table_id:%ld, table_info:%s", 
                table_info.first, table_info.second.schema_pb.ShortDebugString().c_str());
    }
    //测试点：删除存在表的database失败
    baikaldb::pb::MetaManagerRequest request_drop_database;
    request_drop_database.set_op_type(baikaldb::pb::OP_DROP_DATABASE);
    request_drop_database.mutable_database_info()->set_database("FC_Word");
    request_drop_database.mutable_database_info()->set_namespace_name("FengChao");
    _schema_manager->drop_database(request_drop_database, NULL);
    
    //测试点：删除存在database的namespace失败
    baikaldb::pb::MetaManagerRequest request_drop_namespace;
    request_drop_namespace.set_op_type(baikaldb::pb::OP_DROP_NAMESPACE);
    request_drop_namespace.mutable_namespace_info()->set_namespace_name("FengChao");
    _schema_manager->drop_namespace(request_drop_namespace, NULL);
    //测试点：删除层级表，应该更新最顶层表信息
    baikaldb::pb::MetaManagerRequest request_drop_level_table_fc;
    request_drop_level_table_fc.set_op_type(baikaldb::pb::OP_DROP_TABLE);
    request_drop_level_table_fc.mutable_table_info()->set_table_name("planinfo");
    request_drop_level_table_fc.mutable_table_info()->set_database("FC_Word");
    request_drop_level_table_fc.mutable_table_info()->set_namespace_name("FengChao");
    _schema_manager->drop_table(request_drop_level_table_fc, 4, NULL);
    ASSERT_EQ(2, _schema_manager->_max_namespace_id);
    ASSERT_EQ(3, _schema_manager->_max_database_id);
    ASSERT_EQ(4, _schema_manager->_max_table_id);
    ASSERT_EQ(2, _schema_manager->_max_region_id);
    ASSERT_EQ(2, _schema_manager->_namespace_id_map.size());
    ASSERT_EQ(1, _schema_manager->_namespace_id_map["FengChao"]);
    ASSERT_EQ(2, _schema_manager->_namespace_id_map["Feed"]);
    ASSERT_EQ(2, _schema_manager->_namespace_info_map[1].database_ids.size());
    ASSERT_EQ(1, _schema_manager->_namespace_info_map[2].database_ids.size());
    ASSERT_EQ(1, _schema_manager->_database_info_map[1].table_ids.size());
    ASSERT_EQ(3, _schema_manager->_database_id_map.size());
    ASSERT_EQ(1, _schema_manager->_table_info_map.size());
    ASSERT_EQ(2, _schema_manager->_region_info_map.size());
    //for (auto& region_info : _schema_manager->_region_info_map) {
    //    DB_WARNING("region_id:%ld, region_info:%s", 
    //            region_info.first, region_info.second.ShortDebugString().c_str());
    //}
    for (auto& instance  : _schema_manager->_instance_region_map) {
        DB_WARNING("instance:%s", instance.first.c_str());
        for (auto& region_id : instance.second) {
            DB_WARNING("region_id:%ld", region_id);
        }
    }
    for (auto& table_id : _schema_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        DB_WARNING("whether_levle_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_id:%ld, table_info:%s", 
                    table_mem.first, table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%ld, field_name:%s", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
        }
    }
    _schema_manager->load_snapshot();
    ASSERT_EQ(2, _schema_manager->_max_namespace_id);
    ASSERT_EQ(3, _schema_manager->_max_database_id);
    ASSERT_EQ(4, _schema_manager->_max_table_id);
    ASSERT_EQ(2, _schema_manager->_max_region_id);
    ASSERT_EQ(2, _schema_manager->_namespace_id_map.size());
    ASSERT_EQ(1, _schema_manager->_namespace_id_map["FengChao"]);
    ASSERT_EQ(2, _schema_manager->_namespace_id_map["Feed"]);
    ASSERT_EQ(2, _schema_manager->_namespace_info_map[1].database_ids.size());
    ASSERT_EQ(1, _schema_manager->_namespace_info_map[2].database_ids.size());
    ASSERT_EQ(1, _schema_manager->_database_info_map[1].table_ids.size());
    ASSERT_EQ(3, _schema_manager->_database_id_map.size());
    ASSERT_EQ(1, _schema_manager->_table_info_map.size());
    ASSERT_EQ(2, _schema_manager->_region_info_map.size());
    //for (auto& region_info : _schema_manager->_region_info_map) {
    //    DB_WARNING("region_id:%ld, region_info:%s", 
    //            region_info.first, region_info.second.ShortDebugString().c_str());
    //}
    for (auto& instance  : _schema_manager->_instance_region_map) {
        DB_WARNING("instance:%s", instance.first.c_str());
        for (auto& region_id : instance.second) {
            DB_WARNING("region_id:%ld", region_id);
        }
    }
    for (auto& table_id : _schema_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        DB_WARNING("whether_levle_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%ld, field_name:%s", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
        }
    }
    //测试点:删除非层次表
    request_drop_level_table_fc.set_op_type(baikaldb::pb::OP_DROP_TABLE);
    request_drop_level_table_fc.mutable_table_info()->set_table_name("userinfo");
    request_drop_level_table_fc.mutable_table_info()->set_database("FC_Word");
    request_drop_level_table_fc.mutable_table_info()->set_namespace_name("FengChao");
    _schema_manager->drop_table(request_drop_level_table_fc, 5, NULL);
    ASSERT_EQ(2, _schema_manager->_max_namespace_id);
    ASSERT_EQ(3, _schema_manager->_max_database_id);
    ASSERT_EQ(4, _schema_manager->_max_table_id);
    ASSERT_EQ(2, _schema_manager->_max_region_id);
    ASSERT_EQ(2, _schema_manager->_namespace_id_map.size());
    ASSERT_EQ(1, _schema_manager->_namespace_id_map["FengChao"]);
    ASSERT_EQ(2, _schema_manager->_namespace_id_map["Feed"]);
    ASSERT_EQ(2, _schema_manager->_namespace_info_map[1].database_ids.size());
    ASSERT_EQ(1, _schema_manager->_namespace_info_map[2].database_ids.size());
    ASSERT_EQ(0, _schema_manager->_database_info_map[0].table_ids.size());
    ASSERT_EQ(3, _schema_manager->_database_id_map.size());
    ASSERT_EQ(0, _schema_manager->_table_info_map.size());
    ASSERT_EQ(0, _schema_manager->_region_info_map.size());
    _schema_manager->load_snapshot();
    ASSERT_EQ(2, _schema_manager->_max_namespace_id);
    ASSERT_EQ(3, _schema_manager->_max_database_id);
    ASSERT_EQ(4, _schema_manager->_max_table_id);
    ASSERT_EQ(2, _schema_manager->_max_region_id);
    ASSERT_EQ(2, _schema_manager->_namespace_id_map.size());
    ASSERT_EQ(1, _schema_manager->_namespace_id_map["FengChao"]);
    ASSERT_EQ(2, _schema_manager->_namespace_id_map["Feed"]);
    ASSERT_EQ(2, _schema_manager->_namespace_info_map[1].database_ids.size());
    ASSERT_EQ(1, _schema_manager->_namespace_info_map[2].database_ids.size());
    ASSERT_EQ(0, _schema_manager->_database_info_map[1].table_ids.size());
    ASSERT_EQ(3, _schema_manager->_database_id_map.size());
    ASSERT_EQ(0, _schema_manager->_table_info_map.size());
    ASSERT_EQ(0, _schema_manager->_region_info_map.size());
    
    //测试点：删除database
    request_drop_database.set_op_type(baikaldb::pb::OP_DROP_DATABASE);
    request_drop_database.mutable_database_info()->set_database("FC_Word");
    request_drop_database.mutable_database_info()->set_namespace_name("FengChao");
    _schema_manager->drop_database(request_drop_database, NULL);
    ASSERT_EQ(2, _schema_manager->_namespace_id_map.size());
    ASSERT_EQ(1, _schema_manager->_namespace_id_map["FengChao"]);
    ASSERT_EQ(2, _schema_manager->_namespace_id_map["Feed"]);
    ASSERT_EQ(1, _schema_manager->_namespace_info_map[1].database_ids.size());
    ASSERT_EQ(1, _schema_manager->_namespace_info_map[2].database_ids.size());
    ASSERT_EQ(2, _schema_manager->_database_id_map.size());
    _schema_manager->load_snapshot(); 
    ASSERT_EQ(2, _schema_manager->_namespace_id_map.size());
    ASSERT_EQ(1, _schema_manager->_namespace_id_map["FengChao"]);
    ASSERT_EQ(2, _schema_manager->_namespace_id_map["Feed"]);
    ASSERT_EQ(1, _schema_manager->_namespace_info_map[1].database_ids.size());
    ASSERT_EQ(1, _schema_manager->_namespace_info_map[2].database_ids.size());
    ASSERT_EQ(2, _schema_manager->_database_id_map.size());
    
    //测试点：删除database
    request_drop_database.set_op_type(baikaldb::pb::OP_DROP_DATABASE);
    request_drop_database.mutable_database_info()->set_database("FC_Segment");
    request_drop_database.mutable_database_info()->set_namespace_name("FengChao");
    _schema_manager->drop_database(request_drop_database, NULL);
    ASSERT_EQ(2, _schema_manager->_namespace_id_map.size());
    ASSERT_EQ(1, _schema_manager->_namespace_id_map["FengChao"]);
    ASSERT_EQ(2, _schema_manager->_namespace_id_map["Feed"]);
    ASSERT_EQ(0, _schema_manager->_namespace_info_map[1].database_ids.size());
    ASSERT_EQ(1, _schema_manager->_namespace_info_map[2].database_ids.size());
    ASSERT_EQ(1, _schema_manager->_database_id_map.size());
    _schema_manager->load_snapshot();
    ASSERT_EQ(2, _schema_manager->_namespace_id_map.size());
    ASSERT_EQ(1, _schema_manager->_namespace_id_map["FengChao"]);
    ASSERT_EQ(2, _schema_manager->_namespace_id_map["Feed"]);
    ASSERT_EQ(0, _schema_manager->_namespace_info_map[1].database_ids.size());
    ASSERT_EQ(1, _schema_manager->_namespace_info_map[2].database_ids.size());
    ASSERT_EQ(1, _schema_manager->_database_id_map.size());
    
    //测试点：删除namespace
    request_drop_namespace.set_op_type(baikaldb::pb::OP_DROP_NAMESPACE);
    request_drop_namespace.mutable_namespace_info()->set_namespace_name("FengChao");
    _schema_manager->drop_namespace(request_drop_namespace, NULL);
    ASSERT_EQ(1, _schema_manager->_namespace_id_map.size());
    ASSERT_EQ(2, _schema_manager->_namespace_id_map["Feed"]);
    ASSERT_EQ(1, _schema_manager->_namespace_info_map[2].database_ids.size());
    ASSERT_EQ(1, _schema_manager->_database_id_map.size());
    _schema_manager->load_snapshot();
    ASSERT_EQ(1, _schema_manager->_namespace_id_map.size());
    ASSERT_EQ(2, _schema_manager->_namespace_id_map["Feed"]);
    ASSERT_EQ(1, _schema_manager->_namespace_info_map[2].database_ids.size());
    ASSERT_EQ(1, _schema_manager->_database_id_map.size());
    //为后续测试准备数据
    //feed库新建userinfo表
    baikaldb::pb::MetaManagerRequest request_create_table_feed;
    request_create_table_feed.set_op_type(baikaldb::pb::OP_CREATE_TABLE);
    request_create_table_feed.mutable_table_info()->set_table_name("userinfo");
    request_create_table_feed.mutable_table_info()->set_database("FC_Word");
    request_create_table_feed.mutable_table_info()->set_namespace_name("Feed");
    request_create_table_feed.mutable_table_info()->add_init_store("127.0.0.1:8010");
    field = request_create_table_feed.mutable_table_info()->add_fields();
    field->set_field_name("userid");
    field->set_mysql_type(baikaldb::pb::INT64);
    field = request_create_table_feed.mutable_table_info()->add_fields();
    field->set_field_name("username");
    field->set_mysql_type(baikaldb::pb::STRING);
    field = request_create_table_feed.mutable_table_info()->add_fields();
    field->set_field_name("type");
    field->set_mysql_type(baikaldb::pb::STRING);
    index = request_create_table_feed.mutable_table_info()->add_indexs();
    index->set_index_name("primary");
    index->set_index_type(baikaldb::pb::I_PRIMARY);
    index->add_field_names("userid");
    index = request_create_table_feed.mutable_table_info()->add_indexs();
    index->set_index_name("union_index");
    index->set_index_type(baikaldb::pb::I_KEY);
    index->add_field_names("username");
    index->add_field_names("type");
    _schema_manager->create_table(request_create_table_feed, 6, NULL);
    //验证正确性
    ASSERT_EQ(2, _schema_manager->_max_namespace_id);
    ASSERT_EQ(3, _schema_manager->_max_database_id);
    ASSERT_EQ(6, _schema_manager->_max_table_id);
    ASSERT_EQ(3, _schema_manager->_max_region_id);
    ASSERT_EQ(1, _schema_manager->_namespace_id_map.size());
    ASSERT_EQ(2, _schema_manager->_namespace_id_map["Feed"]);
    ASSERT_EQ(1, _schema_manager->_namespace_info_map[2].database_ids.size());
    ASSERT_EQ(1, _schema_manager->_database_info_map[3].table_ids.size());
    ASSERT_EQ(1, _schema_manager->_database_id_map.size());
    for (auto& ns_mem : _schema_manager->_namespace_info_map) {
        DB_WARNING("NameSpacePb:%s", ns_mem.second.namespace_pb.ShortDebugString().c_str());
    }
    for (auto& ns_id: _schema_manager->_namespace_id_map) {
        DB_WARNING("namespace_id:%ld, name:%s", ns_id.second, ns_id.first.c_str());
    }
    for (auto& db_mem : _schema_manager->_database_info_map) {
        DB_WARNING("DatabasePb:%s", db_mem.second.database_pb.ShortDebugString().c_str());
    }
    for (auto& db_id: _schema_manager->_database_id_map) {
        DB_WARNING("database_id:%ld, name:%s", db_id.second, db_id.first.c_str());
    }
    for (auto& table_id : _schema_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        DB_WARNING("whether_levle_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%ld, field_name:%s", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
        }
    }
    //for (auto& region :  _schema_manager->_region_info_map) {
    //    DB_WARNING("database_id:%ld, pb:%s", region.first, region.second.ShortDebugString().c_str());
    //}
    //做snapshot, 验证snapshot的正确性
    _schema_manager->load_snapshot();
    //验证正确性
    ASSERT_EQ(2, _schema_manager->_max_namespace_id);
    ASSERT_EQ(3, _schema_manager->_max_database_id);
    ASSERT_EQ(6, _schema_manager->_max_table_id);
    ASSERT_EQ(3, _schema_manager->_max_region_id);
    ASSERT_EQ(1, _schema_manager->_namespace_id_map.size());
    ASSERT_EQ(2, _schema_manager->_namespace_id_map["Feed"]);
    ASSERT_EQ(1, _schema_manager->_namespace_info_map[2].database_ids.size());
    ASSERT_EQ(1, _schema_manager->_database_info_map[3].table_ids.size());
    ASSERT_EQ(1, _schema_manager->_database_id_map.size());
    for (auto& ns_mem : _schema_manager->_namespace_info_map) {
        DB_WARNING("NameSpacePb:%s", ns_mem.second.namespace_pb.ShortDebugString().c_str());
    }
    for (auto& ns_id: _schema_manager->_namespace_id_map) {
        DB_WARNING("namespace_id:%ld, name:%s", ns_id.second, ns_id.first.c_str());
    }
    for (auto& db_mem : _schema_manager->_database_info_map) {
        DB_WARNING("DatabasePb:%s", db_mem.second.database_pb.ShortDebugString().c_str());
    }
    for (auto& db_id: _schema_manager->_database_id_map) {
        DB_WARNING("database_id:%ld, name:%s", db_id.second, db_id.first.c_str());
    }
    for (auto& table_id : _schema_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        DB_WARNING("whether_levle_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%ld, field_name:%s", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
        }
    }
    //添加region
    baikaldb::pb::MetaManagerRequest request_update_region_feed;
    request_update_region_feed.set_op_type(baikaldb::pb::OP_UPDATE_REGION);
    request_update_region_feed.mutable_region_info()->set_region_id(3);
    request_update_region_feed.mutable_region_info()->set_table_id(5);
    request_update_region_feed.mutable_region_info()->set_table_name("userinfo");
    request_update_region_feed.mutable_region_info()->set_partition_id(0);
    request_update_region_feed.mutable_region_info()->set_replica_num(3);
    request_update_region_feed.mutable_region_info()->set_version(1);
    request_update_region_feed.mutable_region_info()->add_peers("127.0.0.1:8010");
    request_update_region_feed.mutable_region_info()->set_leader("127.0.0.1:8010");
    request_update_region_feed.mutable_region_info()->set_status(baikaldb::pb::IDLE);
    request_update_region_feed.mutable_region_info()->set_used_size(1024);
    request_update_region_feed.mutable_region_info()->set_log_index(1);
    _schema_manager->update_region(request_update_region_feed, 7, NULL);
    //for (auto& region_info : _schema_manager->_region_info_map) {
    //    DB_WARNING("region_id:%ld, region_info:%s", 
    //            region_info.first, region_info.second.ShortDebugString().c_str());
    //}
    for (auto& instance  : _schema_manager->_instance_region_map) {
        DB_WARNING("instance:%s", instance.first.c_str());
        for (auto& region_id : instance.second) {
            DB_WARNING("region_id:%ld", region_id);
        }
    }
    for (auto& table_id : _schema_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        DB_WARNING("whether_levle_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
    }
    _schema_manager->load_snapshot();
    //for (auto& region_info : _schema_manager->_region_info_map) {
    //    DB_WARNING("region_id:%ld, region_info:%s", 
    //            region_info.first, region_info.second.ShortDebugString().c_str());
    //}
    for (auto& instance  : _schema_manager->_instance_region_map) {
        DB_WARNING("instance:%s", instance.first.c_str());
        for (auto& region_id : instance.second) {
            DB_WARNING("region_id:%ld", region_id);
        }
    }
    for (auto& table_id : _schema_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        DB_WARNING("whether_levle_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
    }
    baikaldb::pb::MetaManagerRequest request_split_region_feed;
    request_split_region_feed.mutable_region_split()->set_region_id(3);
    _schema_manager->split_region(request_split_region_feed, NULL);
    ASSERT_EQ(4, _schema_manager->_max_region_id);
    _schema_manager->load_snapshot();
    ASSERT_EQ(4, _schema_manager->_max_region_id);
    
    request_update_region_feed.set_op_type(baikaldb::pb::OP_UPDATE_REGION);
    request_update_region_feed.mutable_region_info()->set_region_id(4);
    request_update_region_feed.mutable_region_info()->set_table_id(5);
    request_update_region_feed.mutable_region_info()->set_table_name("userinfo");
    request_update_region_feed.mutable_region_info()->set_partition_id(0);
    request_update_region_feed.mutable_region_info()->set_replica_num(3);
    request_update_region_feed.mutable_region_info()->set_version(1);
    request_update_region_feed.mutable_region_info()->clear_peers();
    request_update_region_feed.mutable_region_info()->add_peers("127.0.0.2:8010");
    request_update_region_feed.mutable_region_info()->set_leader("127.0.0.2:8010");
    request_update_region_feed.mutable_region_info()->set_status(baikaldb::pb::IDLE);
    request_update_region_feed.mutable_region_info()->set_used_size(1034);
    request_update_region_feed.mutable_region_info()->set_log_index(1);
    _schema_manager->update_region(request_update_region_feed, 8, NULL);
    //for (auto& region_info : _schema_manager->_region_info_map) {
    //    DB_WARNING("region_id:%ld, region_info:%s", 
    //            region_info.first, region_info.second.ShortDebugString().c_str());
    //}
    for (auto& instance  : _schema_manager->_instance_region_map) {
        DB_WARNING("instance:%s", instance.first.c_str());
        for (auto& region_id : instance.second) {
            DB_WARNING("region_id:%ld", region_id);
        }
    }
    for (auto& table_id : _schema_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        DB_WARNING("whether_levle_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
    }
    _schema_manager->load_snapshot();
    //for (auto& region_info : _schema_manager->_region_info_map) {
    //    DB_WARNING("region_id:%ld, region_info:%s", 
    //            region_info.first, region_info.second.ShortDebugString().c_str());
    //}
    for (auto& instance  : _schema_manager->_instance_region_map) {
        DB_WARNING("instance:%s", instance.first.c_str());
        for (auto& region_id : instance.second) {
            DB_WARNING("region_id:%ld", region_id);
        }
    }
    for (auto& table_id : _schema_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        DB_WARNING("whether_levle_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
    }
    //测试点：修改表名
    baikaldb::pb::MetaManagerRequest rename_table_request;
    rename_table_request.set_op_type(baikaldb::pb::OP_RENAME_TABLE);
    rename_table_request.mutable_table_info()->set_table_name("userinfo");
    rename_table_request.mutable_table_info()->set_new_table_name("new_userinfo");
    rename_table_request.mutable_table_info()->set_database("FC_Word");
    rename_table_request.mutable_table_info()->set_namespace_name("Feed");
    _schema_manager->rename_table(rename_table_request, 9, NULL);
    for (auto& table_id : _schema_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        DB_WARNING("whether_levle_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
    }
    _schema_manager->load_snapshot();
    for (auto& table_id : _schema_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        DB_WARNING("whether_levle_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
    }
    //测试点：add_field
    baikaldb::pb::MetaManagerRequest add_field_request;
    add_field_request.set_op_type(baikaldb::pb::OP_ADD_FIELD);
    field = add_field_request.mutable_table_info()->add_fields();
    field->set_field_name("isdel");
    field->set_mysql_type(baikaldb::pb::BOOL);
    add_field_request.mutable_table_info()->set_table_name("new_userinfo");
    add_field_request.mutable_table_info()->set_database("FC_Word");
    add_field_request.mutable_table_info()->set_namespace_name("Feed");
    _schema_manager->add_field(add_field_request, 10, NULL);
    for (auto& table_id : _schema_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        DB_WARNING("whether_levle_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%ld, field_name:%s", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
        }
    }
    _schema_manager->load_snapshot();
    for (auto& table_id : _schema_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        DB_WARNING("whether_levle_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%ld, field_name:%s", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
        }
    }
    //测试点：drop_field
    baikaldb::pb::MetaManagerRequest drop_field_request;
    drop_field_request.set_op_type(baikaldb::pb::OP_DROP_FIELD);
    field = drop_field_request.mutable_table_info()->add_fields();
    field->set_field_name("type");
    drop_field_request.mutable_table_info()->set_table_name("new_userinfo");
    drop_field_request.mutable_table_info()->set_database("FC_Word");
    drop_field_request.mutable_table_info()->set_namespace_name("Feed");
    _schema_manager->drop_field(drop_field_request, 11, NULL);
    for (auto& table_id : _schema_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        DB_WARNING("whether_levle_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%ld, field_name:%s", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
        }
    }
    _schema_manager->load_snapshot();
    for (auto& table_id : _schema_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        DB_WARNING("whether_levle_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%ld, field_name:%s", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
        }
    }
    
    //测试点：rename_field
    baikaldb::pb::MetaManagerRequest rename_field_request;
    rename_field_request.set_op_type(baikaldb::pb::OP_RENAME_FIELD);
    field = rename_field_request.mutable_table_info()->add_fields();
    field->set_field_name("username");
    field->set_new_field_name("new_username");
    rename_field_request.mutable_table_info()->set_table_name("new_userinfo");
    rename_field_request.mutable_table_info()->set_database("FC_Word");
    rename_field_request.mutable_table_info()->set_namespace_name("Feed");
    _schema_manager->rename_field(rename_field_request, 12, NULL);
    for (auto& table_id : _schema_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        DB_WARNING("whether_levle_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%ld, field_name:%s", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
        }
    }
    _schema_manager->load_snapshot();
    for (auto& table_id : _schema_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _schema_manager->_table_info_map) {
        DB_WARNING("whether_levle_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%ld, field_name:%s", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
        }
    }
    
} // TEST_F
int main(int argc, char** argv) {
    baikaldb::FLAGS_db_path = "schema_manager_db";
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
