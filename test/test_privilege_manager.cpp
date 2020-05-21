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
#include "meta_rocksdb.h"
#include "privilege_manager.h"
#include "query_privilege_manager.h"
#include "schema_manager.h"
#include "namespace_manager.h"
#include "database_manager.h"
#include "table_manager.h"
#include "region_manager.h"
#include <gflags/gflags.h>
namespace baikaldb {
    DECLARE_string(db_path);
}
class PrivilegeManagerTest : public testing::Test {
public:
    ~PrivilegeManagerTest() {}
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
        _query_privilege_manager = baikaldb::QueryPrivilegeManager::get_instance();
        _privilege_manager = baikaldb::PrivilegeManager::get_instance();
        _namespace_manager = baikaldb::NamespaceManager::get_instance();
        _database_manager = baikaldb::DatabaseManager::get_instance();
        _schema_manager = baikaldb::SchemaManager::get_instance(); 
        _table_manager = baikaldb::TableManager::get_instance(); 
        _region_manager = baikaldb::RegionManager::get_instance();
    }
    virtual void TearDown() {}
    baikaldb::PrivilegeManager* _privilege_manager;
    baikaldb::QueryPrivilegeManager* _query_privilege_manager;
    baikaldb::MetaRocksdb*  _rocksdb;
    baikaldb::SchemaManager* _schema_manager; 
    baikaldb::NamespaceManager* _namespace_manager;
    baikaldb::DatabaseManager* _database_manager;
    baikaldb::TableManager* _table_manager;
    baikaldb::RegionManager* _region_manager;
};
// add_logic add_physical add_instance
TEST_F(PrivilegeManagerTest, test_create_drop_modify) {
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
    
    //测试点：新增用户
    baikaldb::pb::MetaManagerRequest create_user_request;
    create_user_request.set_op_type(baikaldb::pb::OP_CREATE_USER);
    create_user_request.mutable_user_privilege()->set_username("thunder");
    create_user_request.mutable_user_privilege()->set_namespace_name("FengChao");
    create_user_request.mutable_user_privilege()->set_password("jeeI0o");
    auto table_privilege = create_user_request.mutable_user_privilege()->add_privilege_table();
    table_privilege->set_database("FC_Word");
    table_privilege->set_table_name("userinfo");
    table_privilege->set_table_rw(baikaldb::pb::WRITE);
    auto database_priviliege = create_user_request.mutable_user_privilege()->add_privilege_database();
    database_priviliege->set_database("FC_Segment");
    database_priviliege->set_database_rw(baikaldb::pb::READ);
    _privilege_manager->create_user(create_user_request, NULL);
    ASSERT_EQ(1, _privilege_manager->_user_privilege.size());
    for (auto& user : _privilege_manager->_user_privilege) {
        DB_WARNING("user_name:%s, privilege:%s", 
                user.first.c_str(), user.second.ShortDebugString().c_str());
    }
    _privilege_manager->load_snapshot();
    ASSERT_EQ(1, _privilege_manager->_user_privilege.size());
    for (auto& user : _privilege_manager->_user_privilege) {
        DB_WARNING("user_name:%s, privilege:%s", 
                user.first.c_str(), user.second.ShortDebugString().c_str());
    }
    //test_point: test_query_priviege
    baikaldb::pb::QueryRequest query_request;
    baikaldb::pb::QueryResponse response;
    query_request.set_op_type(baikaldb::pb::QUERY_USERPRIVILEG);
    _query_privilege_manager->get_user_info(&query_request, &response);
    DB_WARNING("privilege info: %s", response.DebugString().c_str());
    
    response.clear_user_privilege();
    query_request.set_user_name("thunder");
    _query_privilege_manager->get_user_info(&query_request, &response);
    DB_WARNING("privilege info: %s", response.DebugString().c_str());
    
    response.clear_user_privilege();
    query_request.set_op_type(baikaldb::pb::QUERY_PRIVILEGE_FLATTEN);
    _query_privilege_manager->get_flatten_privilege(&query_request, &response);
    DB_WARNING("privilege info: %s", response.DebugString().c_str());

    response.clear_flatten_privileges();
    query_request.set_user_name("thunder");
    _query_privilege_manager->get_flatten_privilege(&query_request, &response);
    DB_WARNING("privilege info: %s", response.DebugString().c_str());
     
    //为用户添加权限
    baikaldb::pb::MetaManagerRequest add_privilege_request;
    add_privilege_request.set_op_type(baikaldb::pb::OP_ADD_PRIVILEGE);
    add_privilege_request.mutable_user_privilege()->set_username("thunder");
    add_privilege_request.mutable_user_privilege()->set_namespace_name("FengChao");
    table_privilege = add_privilege_request.mutable_user_privilege()->add_privilege_table();
    table_privilege->set_database("FC_Word");
    table_privilege->set_table_name("planinfo");
    table_privilege->set_table_rw(baikaldb::pb::WRITE);
    
    //权限升级，读变成写
    database_priviliege = add_privilege_request.mutable_user_privilege()->add_privilege_database();
    database_priviliege->set_database("FC_Segment");
    database_priviliege->set_database_rw(baikaldb::pb::WRITE);
    
    add_privilege_request.mutable_user_privilege()->add_bns("bns");
    add_privilege_request.mutable_user_privilege()->add_bns("smartbns");
    add_privilege_request.mutable_user_privilege()->add_ip("127.0.0.1");
    add_privilege_request.mutable_user_privilege()->add_ip("127.0.0.2");
    _privilege_manager->add_privilege(add_privilege_request, NULL);
    ASSERT_EQ(1, _privilege_manager->_user_privilege.size());
    for (auto& user : _privilege_manager->_user_privilege) {
        DB_WARNING("user_name:%s, privilege:%s", 
                user.first.c_str(), user.second.ShortDebugString().c_str());
    }
    _privilege_manager->load_snapshot();
    ASSERT_EQ(1, _privilege_manager->_user_privilege.size());
    for (auto& user : _privilege_manager->_user_privilege) {
        DB_WARNING("user_name:%s, privilege:%s", 
                user.first.c_str(), user.second.ShortDebugString().c_str());
    }
    
    //为用户删除权限
    baikaldb::pb::MetaManagerRequest drop_privilege_request;
    drop_privilege_request.set_op_type(baikaldb::pb::OP_DROP_PRIVILEGE);
    drop_privilege_request.mutable_user_privilege()->set_username("thunder");
    drop_privilege_request.mutable_user_privilege()->set_namespace_name("FengChao");
    table_privilege = drop_privilege_request.mutable_user_privilege()->add_privilege_table();
    table_privilege->set_database("FC_Word");
    table_privilege->set_table_name("planinfo");
    table_privilege->set_table_rw(baikaldb::pb::WRITE);
    //权限降级，写变成读
    database_priviliege = drop_privilege_request.mutable_user_privilege()->add_privilege_database();
    database_priviliege->set_database("FC_Segment");
    database_priviliege->set_database_rw(baikaldb::pb::READ);
    drop_privilege_request.mutable_user_privilege()->add_bns("bns");
    drop_privilege_request.mutable_user_privilege()->add_ip("127.0.0.2");
    _privilege_manager->drop_privilege(drop_privilege_request, NULL);
    ASSERT_EQ(1, _privilege_manager->_user_privilege.size());
    for (auto& user : _privilege_manager->_user_privilege) {
        DB_WARNING("user_name:%s, privilege:%s", 
                user.first.c_str(), user.second.ShortDebugString().c_str());
    }
    _privilege_manager->load_snapshot();
    ASSERT_EQ(1, _privilege_manager->_user_privilege.size());
    for (auto& user : _privilege_manager->_user_privilege) {
        DB_WARNING("user_name:%s, privilege:%s", 
                user.first.c_str(), user.second.ShortDebugString().c_str());
    }
    //删除用户
    baikaldb::pb::MetaManagerRequest drop_user_request;
    drop_user_request.set_op_type(baikaldb::pb::OP_DROP_USER);
    drop_user_request.mutable_user_privilege()->set_username("thunder");
    drop_user_request.mutable_user_privilege()->set_namespace_name("FengChao");
    _privilege_manager->drop_user(drop_user_request, NULL);
    ASSERT_EQ(0, _privilege_manager->_user_privilege.size());
    for (auto& user : _privilege_manager->_user_privilege) {
        DB_WARNING("user_name:%s, privilege:%s", 
                user.first.c_str(), user.second.ShortDebugString().c_str());
    }
    _privilege_manager->load_snapshot();
    ASSERT_EQ(0, _privilege_manager->_user_privilege.size());
    for (auto& user : _privilege_manager->_user_privilege) {
        DB_WARNING("user_name:%s, privilege:%s", 
                user.first.c_str(), user.second.ShortDebugString().c_str());
    }

} // TEST_F
int main(int argc, char** argv) {
    baikaldb::FLAGS_db_path = "privilege_manager_db";
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
