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
#include "table_manager.h"
#include "region_manager.h"
#include "namespace_manager.h"
#include "database_manager.h"
#include "meta_rocksdb.h"
#include <gflags/gflags.h>
namespace baikaldb {
    DECLARE_string(db_path);
}
class TestManagerTest : public testing::Test {
public:
    ~TestManagerTest() {}
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
        _namespace_manager = baikaldb::NamespaceManager::get_instance();
        _database_manager = baikaldb::DatabaseManager::get_instance();
        _schema_manager = baikaldb::SchemaManager::get_instance();
        _table_manager = baikaldb::TableManager::get_instance();
        _region_manager = baikaldb::RegionManager::get_instance();
    }
    virtual void TearDown() {}
    baikaldb::RegionManager* _region_manager;
    baikaldb::TableManager* _table_manager;
    baikaldb::NamespaceManager* _namespace_manager;
    baikaldb::DatabaseManager* _database_manager;
    baikaldb::SchemaManager* _schema_manager;
    baikaldb::MetaRocksdb*  _rocksdb;
};
// add_logic add_physical add_instance
TEST_F(TestManagerTest, test_create_drop_modify) {
    //测试点：增加命名空间"FengChao"
    baikaldb::pb::MetaManagerRequest request_add_namespace_fc;
    request_add_namespace_fc.set_op_type(baikaldb::pb::OP_CREATE_NAMESPACE);
    request_add_namespace_fc.mutable_namespace_info()->set_namespace_name("FengChao");
    request_add_namespace_fc.mutable_namespace_info()->set_quota(1024*1024);
    _namespace_manager->create_namespace(request_add_namespace_fc, NULL);
    
    //测试点：增加命名空间Feed
    baikaldb::pb::MetaManagerRequest request_add_namespace_feed;
    request_add_namespace_feed.set_op_type(baikaldb::pb::OP_CREATE_NAMESPACE);
    request_add_namespace_feed.mutable_namespace_info()->set_namespace_name("Feed");
    request_add_namespace_feed.mutable_namespace_info()->set_quota(2014*1024);
    _namespace_manager->create_namespace(request_add_namespace_feed, NULL);
    //验证正确性
    ASSERT_EQ(2, _namespace_manager->_max_namespace_id);
    ASSERT_EQ(2, _namespace_manager->_namespace_id_map.size());
    ASSERT_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    ASSERT_EQ(2, _namespace_manager->_namespace_id_map["Feed"]);
    ASSERT_EQ(0, _namespace_manager->_database_ids[1].size());
    ASSERT_EQ(0, _namespace_manager->_database_ids[2].size());
    ASSERT_EQ(2, _namespace_manager->_namespace_info_map.size());
    ASSERT_EQ(1, _namespace_manager->_namespace_info_map[1].version());
    ASSERT_EQ(1, _namespace_manager->_namespace_info_map[2].version());
    for (auto& ns_mem : _namespace_manager->_namespace_info_map) {
        DB_WARNING("NameSpacePb:%s", ns_mem.second.ShortDebugString().c_str());
    }
    //做snapshot, 验证snapshot的正确性
    _schema_manager->load_snapshot();
    ASSERT_EQ(2, _namespace_manager->_max_namespace_id);
    ASSERT_EQ(2, _namespace_manager->_namespace_id_map.size());
    ASSERT_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    ASSERT_EQ(2, _namespace_manager->_namespace_id_map["Feed"]);
    ASSERT_EQ(0, _namespace_manager->_database_ids[1].size());
    ASSERT_EQ(0, _namespace_manager->_database_ids[2].size());
    ASSERT_EQ(2, _namespace_manager->_namespace_info_map.size());
    ASSERT_EQ(1, _namespace_manager->_namespace_info_map[1].version());
    ASSERT_EQ(1, _namespace_manager->_namespace_info_map[2].version());
    for (auto& ns_mem : _namespace_manager->_namespace_info_map) {
        DB_WARNING("NameSpacePb:%s", ns_mem.second.ShortDebugString().c_str());
    }

    int64_t max_namespace_id = _namespace_manager->get_max_namespace_id();
    ASSERT_EQ(2, max_namespace_id); 

    int64_t namespace_id = _namespace_manager->get_namespace_id("FengChao");
    ASSERT_EQ(1, namespace_id);
    
    namespace_id = _namespace_manager->get_namespace_id("Feed");
    ASSERT_EQ(2, namespace_id);
    
    //测试点：创建database
    baikaldb::pb::MetaManagerRequest request_add_database_fc;
    request_add_database_fc.set_op_type(baikaldb::pb::OP_CREATE_DATABASE);
    request_add_database_fc.mutable_database_info()->set_database("FC_Word");
    request_add_database_fc.mutable_database_info()->set_namespace_name("FengChao");
    request_add_database_fc.mutable_database_info()->set_quota(10 * 1024);
    _database_manager->create_database(request_add_database_fc, NULL);
    
    request_add_database_fc.mutable_database_info()->set_database("FC_Segment");
    request_add_database_fc.mutable_database_info()->set_namespace_name("FengChao");
    request_add_database_fc.mutable_database_info()->set_quota(100 * 1024);
    _database_manager->create_database(request_add_database_fc, NULL);

    //测试点：创建database
    baikaldb::pb::MetaManagerRequest request_add_database_feed;
    request_add_database_feed.set_op_type(baikaldb::pb::OP_CREATE_DATABASE);
    request_add_database_feed.mutable_database_info()->set_database("FC_Word");
    request_add_database_feed.mutable_database_info()->set_namespace_name("Feed");
    request_add_database_feed.mutable_database_info()->set_quota(8 * 1024);
    _database_manager->create_database(request_add_database_feed, NULL);

    //验证正确性
    ASSERT_EQ(2, _namespace_manager->_max_namespace_id);
    ASSERT_EQ(2, _namespace_manager->_namespace_id_map.size());
    ASSERT_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    ASSERT_EQ(2, _namespace_manager->_namespace_id_map["Feed"]);
    ASSERT_EQ(2, _namespace_manager->_namespace_info_map.size());
    ASSERT_EQ(2, _namespace_manager->_database_ids[1].size());
    ASSERT_EQ(1, _namespace_manager->_database_ids[2].size());
    ASSERT_EQ(2, _namespace_manager->_database_ids.size());
    for (auto& ns_mem : _namespace_manager->_namespace_info_map) {
        DB_WARNING("NameSpacePb:%s", ns_mem.second.ShortDebugString().c_str());
    }
    for (auto& ns_id: _namespace_manager->_namespace_id_map) {
        DB_WARNING("namespace_id:%ld, name:%s", ns_id.second, ns_id.first.c_str());
    }
    ASSERT_EQ(3, _database_manager->_max_database_id);
    ASSERT_EQ(3, _database_manager->_database_id_map.size());
    ASSERT_EQ(1, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Word"]);
    ASSERT_EQ(2, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Segment"]);
    ASSERT_EQ(3, _database_manager->_database_id_map[std::string("Feed") + "\001" + "FC_Word"]);
    ASSERT_EQ(3, _database_manager->_database_info_map.size());
    ASSERT_EQ(0, _database_manager->_table_ids.size());
    ASSERT_EQ(1, _database_manager->_database_info_map[1].version());
    for (auto& db_mem : _database_manager->_database_info_map) {
        DB_WARNING("DatabasePb:%s", db_mem.second.ShortDebugString().c_str());
    }
    for (auto& db_id: _database_manager->_database_id_map) {
        DB_WARNING("database_id:%ld, name:%s", db_id.second, db_id.first.c_str());
    }
    //做snapshot, 验证snapshot的正确性
    _schema_manager->load_snapshot();
    
    ASSERT_EQ(2, _namespace_manager->_max_namespace_id);
    ASSERT_EQ(2, _namespace_manager->_namespace_id_map.size());
    ASSERT_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    ASSERT_EQ(2, _namespace_manager->_namespace_id_map["Feed"]);
    ASSERT_EQ(2, _namespace_manager->_namespace_info_map.size());
    ASSERT_EQ(2, _namespace_manager->_database_ids[1].size());
    ASSERT_EQ(1, _namespace_manager->_database_ids[2].size());
    ASSERT_EQ(2, _namespace_manager->_database_ids.size());
    for (auto& ns_mem : _namespace_manager->_namespace_info_map) {
        DB_WARNING("NameSpacePb:%s", ns_mem.second.ShortDebugString().c_str());
    }
    for (auto& ns_id: _namespace_manager->_namespace_id_map) {
        DB_WARNING("namespace_id:%ld, name:%s", ns_id.second, ns_id.first.c_str());
    }
    ASSERT_EQ(3, _database_manager->_max_database_id);
    ASSERT_EQ(3, _database_manager->_database_id_map.size());
    ASSERT_EQ(1, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Word"]);
    ASSERT_EQ(2, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Segment"]);
    ASSERT_EQ(3, _database_manager->_database_id_map[std::string("Feed") + "\001" + "FC_Word"]);
    ASSERT_EQ(3, _database_manager->_database_info_map.size());
    ASSERT_EQ(0, _database_manager->_table_ids.size());
    ASSERT_EQ(1, _database_manager->_database_info_map[1].version());
    for (auto& db_mem : _database_manager->_database_info_map) {
        DB_WARNING("DatabasePb:%s", db_mem.second.ShortDebugString().c_str());
    }
    for (auto& db_id: _database_manager->_database_id_map) {
        DB_WARNING("database_id:%ld, name:%s", db_id.second, db_id.first.c_str());
    }

    //测试点：创建table
    baikaldb::pb::MetaManagerRequest request_create_table_fc;
    request_create_table_fc.set_op_type(baikaldb::pb::OP_CREATE_TABLE);
    request_create_table_fc.mutable_table_info()->set_table_name("userinfo");
    request_create_table_fc.mutable_table_info()->set_database("FC_Word");
    request_create_table_fc.mutable_table_info()->set_namespace_name("FengChao");
    request_create_table_fc.mutable_table_info()->add_init_store("127.0.0.1:8010");
    baikaldb::pb::FieldInfo* field = request_create_table_fc.mutable_table_info()->add_fields();
    field->set_field_name("userid");
    field->set_mysql_type(baikaldb::pb::INT64);
    field = request_create_table_fc.mutable_table_info()->add_fields();
    field->set_field_name("username");
    field->set_mysql_type(baikaldb::pb::STRING);
    field = request_create_table_fc.mutable_table_info()->add_fields();
    field->set_field_name("type");
    field->set_mysql_type(baikaldb::pb::STRING);
    field = request_create_table_fc.mutable_table_info()->add_fields();
    field->set_field_name("user_type");
    field->set_mysql_type(baikaldb::pb::STRING);
    baikaldb::pb::IndexInfo* index = request_create_table_fc.mutable_table_info()->add_indexs();
    index->set_index_name("primary");
    index->set_index_type(baikaldb::pb::I_PRIMARY);
    index->add_field_names("userid");
    index = request_create_table_fc.mutable_table_info()->add_indexs();
    index->set_index_name("union_index");
    index->set_index_type(baikaldb::pb::I_KEY);
    index->add_field_names("username");
    index->add_field_names("type");
    _table_manager->create_table(request_create_table_fc, 1, NULL);
    
    ASSERT_EQ(2, _namespace_manager->_max_namespace_id);
    ASSERT_EQ(3, _database_manager->_max_database_id);
    ASSERT_EQ(2, _table_manager->_max_table_id);
    ASSERT_EQ(1, _region_manager->_max_region_id);
    ASSERT_EQ(2, _namespace_manager->_namespace_id_map.size());
    ASSERT_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    ASSERT_EQ(2, _namespace_manager->_namespace_id_map["Feed"]);
    ASSERT_EQ(2, _namespace_manager->_namespace_info_map.size());
    ASSERT_EQ(2, _namespace_manager->_database_ids[1].size());
    ASSERT_EQ(1, _namespace_manager->_database_ids[2].size());
    ASSERT_EQ(2, _namespace_manager->_database_ids.size());

    ASSERT_EQ(3, _database_manager->_database_id_map.size());
    ASSERT_EQ(1, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Word"]);
    ASSERT_EQ(2, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Segment"]);
    ASSERT_EQ(3, _database_manager->_database_id_map[std::string("Feed") + "\001" + "FC_Word"]);
    ASSERT_EQ(3, _database_manager->_database_info_map.size());
    ASSERT_EQ(1, _database_manager->_table_ids.size());
    ASSERT_EQ(1, _database_manager->_table_ids[1].size());
    ASSERT_EQ(1, _database_manager->_database_info_map[1].version());

    ASSERT_EQ(1, _table_manager->_table_id_map.size());
    ASSERT_EQ(1, _table_manager->_table_id_map[std::string("FengChao") +  "\001" + "FC_Word" + "\001" + "userinfo"]);
    ASSERT_EQ(1, _table_manager->_table_info_map.size());
    
    for (auto& table_mem : _table_manager->_table_info_map) {
        DB_WARNING("whether_level_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& partition_region : table_mem.second.partition_regions) {
            DB_WARNING("partition_id: %ld", partition_region.first);
            for (auto region_id : partition_region.second) {
                DB_WARNING("region_id: %ld", region_id);
            }
        }
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%ld, field_name:%s", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
        }
    }
    _schema_manager->load_snapshot();
    ASSERT_EQ(2, _namespace_manager->_max_namespace_id);
    ASSERT_EQ(3, _database_manager->_max_database_id);
    ASSERT_EQ(2, _table_manager->_max_table_id);
    ASSERT_EQ(1, _region_manager->_max_region_id);
    ASSERT_EQ(2, _namespace_manager->_namespace_id_map.size());
    ASSERT_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    ASSERT_EQ(2, _namespace_manager->_namespace_id_map["Feed"]);
    ASSERT_EQ(2, _namespace_manager->_namespace_info_map.size());
    ASSERT_EQ(2, _namespace_manager->_database_ids[1].size());
    ASSERT_EQ(1, _namespace_manager->_database_ids[2].size());
    ASSERT_EQ(2, _namespace_manager->_database_ids.size());

    ASSERT_EQ(3, _database_manager->_database_id_map.size());
    ASSERT_EQ(1, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Word"]);
    ASSERT_EQ(2, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Segment"]);
    ASSERT_EQ(3, _database_manager->_database_id_map[std::string("Feed") + "\001" + "FC_Word"]);
    ASSERT_EQ(3, _database_manager->_database_info_map.size());
    ASSERT_EQ(1, _database_manager->_table_ids.size());
    ASSERT_EQ(1, _database_manager->_table_ids[1].size());
    ASSERT_EQ(1, _database_manager->_database_info_map[1].version());

    ASSERT_EQ(1, _table_manager->_table_id_map.size());
    ASSERT_EQ(1, _table_manager->_table_id_map[std::string("FengChao") +  "\001" + "FC_Word" + "\001" + "userinfo"]);
    ASSERT_EQ(1, _table_manager->_table_info_map.size());
    
    for (auto& table_mem : _table_manager->_table_info_map) {
        DB_WARNING("whether_level_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%ld, field_name:%s", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
        }
        for (auto& partition_region : table_mem.second.partition_regions) {
            DB_WARNING("partition_id: %ld", partition_region.first);
            for (auto region_id : partition_region.second) {
                DB_WARNING("region_id: %ld", region_id);
            }   
        } 
    }

    //测试点：创建层次表
    baikaldb::pb::MetaManagerRequest request_create_table_fc_level;
    request_create_table_fc_level.set_op_type(baikaldb::pb::OP_CREATE_TABLE);
    request_create_table_fc_level.mutable_table_info()->set_table_name("planinfo");
    request_create_table_fc_level.mutable_table_info()->set_database("FC_Word");
    request_create_table_fc_level.mutable_table_info()->set_namespace_name("FengChao");
    request_create_table_fc_level.mutable_table_info()->add_init_store("127.0.0.1:8010");
    request_create_table_fc_level.mutable_table_info()->set_upper_table_name("userinfo");
    field = request_create_table_fc_level.mutable_table_info()->add_fields();
    field->set_field_name("userid");
    field->set_mysql_type(baikaldb::pb::INT64);
    field = request_create_table_fc_level.mutable_table_info()->add_fields();
    field->set_field_name("planid");
    field->set_mysql_type(baikaldb::pb::INT64);
    field = request_create_table_fc_level.mutable_table_info()->add_fields();
    field->set_field_name("planname");
    field->set_mysql_type(baikaldb::pb::STRING);
    field = request_create_table_fc_level.mutable_table_info()->add_fields();
    field->set_field_name("type");
    field->set_mysql_type(baikaldb::pb::STRING);
    index = request_create_table_fc_level.mutable_table_info()->add_indexs();
    index->set_index_name("primary");
    index->set_index_type(baikaldb::pb::I_PRIMARY);
    index->add_field_names("userid");
    index->add_field_names("planid");
    index = request_create_table_fc_level.mutable_table_info()->add_indexs();
    index->set_index_name("union_index");
    index->set_index_type(baikaldb::pb::I_KEY);
    index->add_field_names("planname");
    index->add_field_names("type");
    _table_manager->create_table(request_create_table_fc_level, 2, NULL);
    ASSERT_EQ(2, _namespace_manager->_max_namespace_id);
    ASSERT_EQ(3, _database_manager->_max_database_id);
    ASSERT_EQ(4, _table_manager->_max_table_id);
    ASSERT_EQ(1, _region_manager->_max_region_id);
    ASSERT_EQ(2, _namespace_manager->_namespace_id_map.size());
    ASSERT_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    ASSERT_EQ(2, _namespace_manager->_namespace_id_map["Feed"]);
    ASSERT_EQ(2, _namespace_manager->_namespace_info_map.size());
    ASSERT_EQ(2, _namespace_manager->_database_ids[1].size());
    ASSERT_EQ(1, _namespace_manager->_database_ids[2].size());
    ASSERT_EQ(2, _namespace_manager->_database_ids.size());

    ASSERT_EQ(3, _database_manager->_database_id_map.size());
    ASSERT_EQ(1, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Word"]);
    ASSERT_EQ(2, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Segment"]);
    ASSERT_EQ(3, _database_manager->_database_id_map[std::string("Feed") + "\001" + "FC_Word"]);
    ASSERT_EQ(3, _database_manager->_database_info_map.size());
    ASSERT_EQ(1, _database_manager->_table_ids.size());
    ASSERT_EQ(2, _database_manager->_table_ids[1].size());
    ASSERT_EQ(1, _database_manager->_database_info_map[1].version());

    ASSERT_EQ(2, _table_manager->_table_id_map.size());
    ASSERT_EQ(1, _table_manager->_table_id_map[std::string("FengChao") +  "\001" + "FC_Word" + "\001" + "userinfo"]);
    ASSERT_EQ(3, _table_manager->_table_id_map[std::string("FengChao") +  "\001" + "FC_Word" + "\001" + "planinfo"]);
    ASSERT_EQ(2, _table_manager->_table_info_map.size());
    ASSERT_EQ(2, _table_manager->_table_info_map[1].schema_pb.version()); 
    for (auto& table_mem : _table_manager->_table_info_map) {
        DB_WARNING("whether_level_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%ld, field_name:%s", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
        }
        for (auto& partition_region : table_mem.second.partition_regions) {
            DB_WARNING("partition_id: %ld", partition_region.first);
            for (auto region_id : partition_region.second) {
                DB_WARNING("region_id: %ld", region_id);
            }   
        } 
    }
    _schema_manager->load_snapshot();
    ASSERT_EQ(2, _namespace_manager->_max_namespace_id);
    ASSERT_EQ(3, _database_manager->_max_database_id);
    ASSERT_EQ(4, _table_manager->_max_table_id);
    ASSERT_EQ(1, _region_manager->_max_region_id);
    ASSERT_EQ(2, _namespace_manager->_namespace_id_map.size());
    ASSERT_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    ASSERT_EQ(2, _namespace_manager->_namespace_id_map["Feed"]);
    ASSERT_EQ(2, _namespace_manager->_namespace_info_map.size());
    ASSERT_EQ(2, _namespace_manager->_database_ids[1].size());
    ASSERT_EQ(1, _namespace_manager->_database_ids[2].size());
    ASSERT_EQ(2, _namespace_manager->_database_ids.size());

    ASSERT_EQ(3, _database_manager->_database_id_map.size());
    ASSERT_EQ(1, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Word"]);
    ASSERT_EQ(2, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Segment"]);
    ASSERT_EQ(3, _database_manager->_database_id_map[std::string("Feed") + "\001" + "FC_Word"]);
    ASSERT_EQ(3, _database_manager->_database_info_map.size());
    ASSERT_EQ(1, _database_manager->_table_ids.size());
    ASSERT_EQ(2, _database_manager->_table_ids[1].size());
    ASSERT_EQ(1, _database_manager->_database_info_map[1].version());

    ASSERT_EQ(2, _table_manager->_table_id_map.size());
    ASSERT_EQ(1, _table_manager->_table_id_map[std::string("FengChao") +  "\001" + "FC_Word" + "\001" + "userinfo"]);
    ASSERT_EQ(3, _table_manager->_table_id_map[std::string("FengChao") +  "\001" + "FC_Word" + "\001" + "planinfo"]);
    ASSERT_EQ(2, _table_manager->_table_info_map.size());
    ASSERT_EQ(2, _table_manager->_table_info_map[1].schema_pb.version()); 
    
    for (auto& table_mem : _table_manager->_table_info_map) {
        DB_WARNING("whether_level_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%ld, field_name:%s", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
        }
        for (auto& partition_region : table_mem.second.partition_regions) {
            DB_WARNING("partition_id: %ld", partition_region.first);
            for (auto region_id : partition_region.second) {
                DB_WARNING("region_id: %ld", region_id);
            }   
        } 
    }

    //add region
    baikaldb::pb::MetaManagerRequest request_update_region_feed;
    request_update_region_feed.set_op_type(baikaldb::pb::OP_UPDATE_REGION);
    auto region_info = request_update_region_feed.add_region_infos();
    region_info->set_region_id(1);
    region_info->set_table_id(1);
    region_info->set_table_name("userinfo");
    region_info->set_partition_id(0);
    region_info->set_replica_num(3);
    region_info->set_version(1);
    region_info->set_conf_version(1);
    region_info->add_peers("127.0.0.1:8010");
    region_info->add_peers("127.0.0.1:8011");
    region_info->add_peers("127.0.0.1:8012");
    region_info->set_leader("127.0.0.1:8010");
    region_info->set_status(baikaldb::pb::IDLE);
    region_info->set_used_size(1024);
    region_info->set_log_index(1);
    _region_manager->update_region(request_update_region_feed, 3, NULL);
    ASSERT_EQ(1, _region_manager->_region_info_map.size());
    ASSERT_EQ(3, _region_manager->_instance_region_map.size());
    ASSERT_EQ(1, _region_manager->_instance_region_map["127.0.0.1:8010"].size());
    ASSERT_EQ(1, _region_manager->_instance_region_map["127.0.0.1:8011"].size());
    ASSERT_EQ(1, _region_manager->_instance_region_map["127.0.0.1:8012"].size());
    ASSERT_EQ(1, _region_manager->_instance_region_map["127.0.0.1:8010"][1].size());
    ASSERT_EQ(1, _region_manager->_instance_region_map["127.0.0.1:8011"][1].size());
    ASSERT_EQ(1, _region_manager->_instance_region_map["127.0.0.1:8012"][1].size());
    ASSERT_EQ(1, _region_manager->_region_info_map[1]->conf_version());
    //for (auto& region_info : _region_manager->_region_info_map) {
    //    DB_WARNING("region_id: %ld", region_info.first, region_info.second->ShortDebugString().c_str());
    //}
    ASSERT_EQ(1, _table_manager->_table_info_map[1].partition_regions[0].size());
    //update region
    region_info->clear_peers();
    region_info->add_peers("127.0.0.1:8020");
    region_info->add_peers("127.0.0.1:8021");
    region_info->add_peers("127.0.0.1:8022");
    _region_manager->update_region(request_update_region_feed, 4, NULL);
    ASSERT_EQ(1, _region_manager->_region_info_map.size());
    ASSERT_EQ(6, _region_manager->_instance_region_map.size());
    ASSERT_EQ(0, _region_manager->_instance_region_map["127.0.0.1:8010"].size());
    ASSERT_EQ(0, _region_manager->_instance_region_map["127.0.0.1:8011"].size());
    ASSERT_EQ(0, _region_manager->_instance_region_map["127.0.0.1:8012"].size());
    ASSERT_EQ(1, _region_manager->_instance_region_map["127.0.0.1:8020"].size());
    ASSERT_EQ(1, _region_manager->_instance_region_map["127.0.0.1:8021"].size());
    ASSERT_EQ(1, _region_manager->_instance_region_map["127.0.0.1:8022"].size());
    ASSERT_EQ(1, _region_manager->_instance_region_map["127.0.0.1:8020"][1].size());
    ASSERT_EQ(0, _region_manager->_instance_region_map["127.0.0.1:8011"][1].size());
    ASSERT_EQ(0, _region_manager->_instance_region_map["127.0.0.1:8012"][1].size());
    ASSERT_EQ(0, _region_manager->_instance_region_map["127.0.0.1:8010"][1].size());
    ASSERT_EQ(1, _region_manager->_instance_region_map["127.0.0.1:8021"][1].size());
    ASSERT_EQ(1, _region_manager->_instance_region_map["127.0.0.1:8022"][1].size());
    ASSERT_EQ(2, _region_manager->_region_info_map[1]->conf_version());
    //for (auto& region_info : _region_manager->_region_info_map) {
    //    DB_WARNING("region_id: %ld", region_info.first, region_info.second->ShortDebugString().c_str());
    //}
    _schema_manager->load_snapshot();
    for (auto& table_mem : _table_manager->_table_info_map) {
        DB_WARNING("whether_level_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%ld, field_name:%s", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
        }
        for (auto& partition_region : table_mem.second.partition_regions) {
            DB_WARNING("partition_id: %ld", partition_region.first);
            for (auto region_id : partition_region.second) {
                DB_WARNING("region_id: %ld", region_id);
            }   
        }
    }
    //for (auto& region_info : _region_manager->_region_info_map) {
    //    DB_WARNING("region_id: %ld", region_info.first, region_info.second->ShortDebugString().c_str());
    //}
    //split_region
    baikaldb::pb::MetaManagerRequest split_region_request;
    split_region_request.set_op_type(baikaldb::pb::OP_SPLIT_REGION);
    split_region_request.mutable_region_split()->set_region_id(1);
    _region_manager->split_region(split_region_request, NULL);
    ASSERT_EQ(2, _region_manager->get_max_region_id());
    _schema_manager->load_snapshot();
    ASSERT_EQ(2, _region_manager->get_max_region_id());

    //drop_region
    baikaldb::pb::MetaManagerRequest drop_region_request;
    drop_region_request.set_op_type(baikaldb::pb::OP_DROP_REGION);
    drop_region_request.add_drop_region_ids(1);
    _region_manager->drop_region(drop_region_request, 5, NULL);
    ASSERT_EQ(0, _region_manager->_region_info_map.size());
    ASSERT_EQ(0, _region_manager->_region_state_map.size());
    ASSERT_EQ(0, _region_manager->_instance_region_map.size());
    ASSERT_EQ(0, _table_manager->_table_info_map[1].partition_regions[0].size());
    _schema_manager->load_snapshot();
    for (auto& table_mem : _table_manager->_table_info_map) {
        DB_WARNING("whether_level_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%ld, field_name:%s", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
        }
        for (auto& partition_region : table_mem.second.partition_regions) {
            DB_WARNING("partition_id: %ld", partition_region.first);
            for (auto region_id : partition_region.second) {
                DB_WARNING("region_id: %ld", region_id);
            }   
        }
    }
    //for (auto& region_info : _region_manager->_region_info_map) {
    //    DB_WARNING("region_id: %ld", region_info.first, region_info.second->ShortDebugString().c_str());
    //}
} // TEST_F
int main(int argc, char** argv) {
    baikaldb::FLAGS_db_path = "region_manager_db";
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
