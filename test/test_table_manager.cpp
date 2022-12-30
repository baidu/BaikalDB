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
#include "common.h"
#include "schema_manager.h"
#include "table_manager.h"
#include "query_table_manager.h"
#include "region_manager.h"
#include "namespace_manager.h"
#include "database_manager.h"
#include "meta_rocksdb.h"
namespace baikaldb {
    DECLARE_string(db_path);
}
class TableManagerTest : public testing::Test {
public:
    ~TableManagerTest() {}
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
        _query_table_manager = baikaldb::QueryTableManager::get_instance();
        _region_manager = baikaldb::RegionManager::get_instance();
    }
    virtual void TearDown() {}
    baikaldb::RegionManager* _region_manager;
    baikaldb::TableManager* _table_manager;
    baikaldb::QueryTableManager* _query_table_manager;
    baikaldb::NamespaceManager* _namespace_manager;
    baikaldb::DatabaseManager* _database_manager;
    baikaldb::SchemaManager* _schema_manager;
    baikaldb::MetaRocksdb*  _rocksdb;
};
// add_logic add_physical add_instance
TEST_F(TableManagerTest, test_create_drop_modify) {
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
            DB_WARNING("field_id:%d, field_name:%s", field.second, field.first.c_str());
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
            DB_WARNING("field_id:%d, field_name:%s", field.second, field.first.c_str());
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
            DB_WARNING("field_id:%d, field_name:%s", field.second, field.first.c_str());
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
            DB_WARNING("field_id:%d, field_name:%s", field.second, field.first.c_str());
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
    //test_point: test_query_table
    baikaldb::pb::QueryRequest query_request;
    baikaldb::pb::QueryResponse response;
    query_request.set_op_type(baikaldb::pb::QUERY_SCHEMA);
    _query_table_manager->get_schema_info(&query_request, &response);
    DB_WARNING("table info: %s", response.DebugString().c_str());
    ASSERT_EQ(2, response.schema_infos_size());
    response.clear_schema_infos();

    query_request.set_namespace_name("FengChao");
    query_request.set_database("FC_Word");
    query_request.set_table_name("userinfo");
    _query_table_manager->get_schema_info(&query_request, &response);
    ASSERT_EQ(1, response.schema_infos_size());
    DB_WARNING("table info: %s", response.DebugString().c_str());

    query_request.clear_namespace_name();
    query_request.clear_database();
    query_request.clear_table_name();
    query_request.set_database("FC_Word");
    response.clear_schema_infos();
    _query_table_manager->get_flatten_table(&query_request, &response);
    DB_WARNING("table info: %s", response.DebugString().c_str()); 

    response.clear_flatten_tables();
    query_request.set_namespace_name("FengChao");
    query_request.set_database("FC_Word");
    query_request.set_table_name("userinfo");
    _query_table_manager->get_flatten_schema(&query_request, &response);
    DB_WARNING("table info: %s", response.DebugString().c_str()); 

    //测试点：修改表名
    baikaldb::pb::MetaManagerRequest rename_table_request;
    rename_table_request.set_op_type(baikaldb::pb::OP_RENAME_TABLE);
    rename_table_request.mutable_table_info()->set_table_name("userinfo");
    rename_table_request.mutable_table_info()->set_new_table_name("new_userinfo");
    rename_table_request.mutable_table_info()->set_database("FC_Word");
    rename_table_request.mutable_table_info()->set_namespace_name("FengChao");
    _table_manager->rename_table(rename_table_request, 3, NULL);
    ASSERT_EQ(3, _table_manager->_table_info_map[1].schema_pb.version()); 
    for (auto& table_id : _table_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _table_manager->_table_info_map) {
        DB_WARNING("whether_level_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%d, field_name:%s", field.second, field.first.c_str());
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
    ASSERT_EQ(3, _table_manager->_table_info_map[1].schema_pb.version()); 
    for (auto& table_id : _table_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _table_manager->_table_info_map) {
        DB_WARNING("whether_level_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%d, field_name:%s", field.second, field.first.c_str());
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
    
    // 测试点: update_byte_size
    baikaldb::pb::MetaManagerRequest update_byte_size_request;
    update_byte_size_request.set_op_type(baikaldb::pb::OP_UPDATE_BYTE_SIZE);
    update_byte_size_request.mutable_table_info()->set_table_name("new_userinfo");
    update_byte_size_request.mutable_table_info()->set_database("FC_Word");
    update_byte_size_request.mutable_table_info()->set_namespace_name("FengChao");
    update_byte_size_request.mutable_table_info()->set_byte_size_per_record(1000);
    _table_manager->update_byte_size(update_byte_size_request, 4, NULL);
    ASSERT_EQ(4, _table_manager->_table_info_map[1].schema_pb.version());
    for (auto& table_id : _table_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _table_manager->_table_info_map) {
        DB_WARNING("whether_level_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%d, field_name:%s", field.second, field.first.c_str());
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
    ASSERT_EQ(4, _table_manager->_table_info_map[1].schema_pb.version()); 
    for (auto& table_id : _table_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _table_manager->_table_info_map) {
        DB_WARNING("whether_level_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%d, field_name:%s", field.second, field.first.c_str());
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
 
    //测试点：add_field
    baikaldb::pb::MetaManagerRequest add_field_request;
    add_field_request.set_op_type(baikaldb::pb::OP_ADD_FIELD);
    field = add_field_request.mutable_table_info()->add_fields();
    field->set_field_name("isdel");
    field->set_mysql_type(baikaldb::pb::BOOL);
    add_field_request.mutable_table_info()->set_table_name("new_userinfo");
    add_field_request.mutable_table_info()->set_database("FC_Word");
    add_field_request.mutable_table_info()->set_namespace_name("FengChao");
    _table_manager->add_field(add_field_request, 5, NULL);
    ASSERT_EQ(5, _table_manager->_table_info_map[1].schema_pb.version());
    for (auto& table_id : _table_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _table_manager->_table_info_map) {
        DB_WARNING("whether_level_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%d, field_name:%s", field.second, field.first.c_str());
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
    ASSERT_EQ(5, _table_manager->_table_info_map[1].schema_pb.version());
    for (auto& table_id : _table_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _table_manager->_table_info_map) {
        DB_WARNING("whether_level_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%d, field_name:%s", field.second, field.first.c_str());
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
    
    //drop_field
    baikaldb::pb::MetaManagerRequest drop_field_request;
    drop_field_request.set_op_type(baikaldb::pb::OP_DROP_FIELD);
    field = drop_field_request.mutable_table_info()->add_fields();
    field->set_field_name("user_type");
    drop_field_request.mutable_table_info()->set_table_name("new_userinfo");
    drop_field_request.mutable_table_info()->set_database("FC_Word");
    drop_field_request.mutable_table_info()->set_namespace_name("FengChao");
    _table_manager->drop_field(drop_field_request, 6, NULL);
    ASSERT_EQ(6, _table_manager->_table_info_map[1].schema_pb.version()); 
    for (auto& table_id : _table_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _table_manager->_table_info_map) {
        DB_WARNING("whether_level_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%d, field_name:%s", field.second, field.first.c_str());
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
    ASSERT_EQ(6, _table_manager->_table_info_map[1].schema_pb.version());
    for (auto& table_id : _table_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _table_manager->_table_info_map) {
        DB_WARNING("whether_level_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%d, field_name:%s", field.second, field.first.c_str());
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
    
    //rename_field
    baikaldb::pb::MetaManagerRequest rename_field_request;
    rename_field_request.set_op_type(baikaldb::pb::OP_RENAME_FIELD);
    field = rename_field_request.mutable_table_info()->add_fields();
    field->set_field_name("username");
    field->set_new_field_name("new_username");
    rename_field_request.mutable_table_info()->set_table_name("new_userinfo");
    rename_field_request.mutable_table_info()->set_database("FC_Word");
    rename_field_request.mutable_table_info()->set_namespace_name("FengChao");
    _table_manager->rename_field(rename_field_request, 7, NULL);
    ASSERT_EQ(7, _table_manager->_table_info_map[1].schema_pb.version());
    for (auto& table_id : _table_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _table_manager->_table_info_map) {
        DB_WARNING("whether_level_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%d, field_name:%s", field.second, field.first.c_str());
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
    ASSERT_EQ(7, _table_manager->_table_info_map[1].schema_pb.version());
    for (auto& table_id : _table_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _table_manager->_table_info_map) {
        DB_WARNING("whether_level_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%d, field_name:%s", field.second, field.first.c_str());
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
    
    auto ret = _table_manager->whether_exist_table_id(1);
    ASSERT_EQ(0, ret);
    ret = _table_manager->whether_exist_table_id(2);
    ASSERT_EQ(-1, ret);

    _table_manager->add_region_id(1, 0, 1);
    _table_manager->add_region_id(1, 0, 2);
    _table_manager->add_region_id(1, 0, 3);
    
    std::string resource_tag;
    ret = _table_manager->get_resource_tag(1, resource_tag);
    ASSERT_EQ(0, ret);
    DB_WARNING("resouce_tag:%s", resource_tag.c_str());

    int64_t count = _table_manager->get_region_count(1);
    ASSERT_EQ(3, count);

    int64_t replica_num = 0;
    _table_manager->get_replica_num(1, replica_num);
    ASSERT_EQ(3, replica_num);
    
    std::vector<int64_t> query_region_ids;
    _table_manager->get_region_ids(std::string("FengChao") + "\001" + "FC_Word" + "\001" + "new_userinfo", query_region_ids);
    ASSERT_EQ(3, query_region_ids.size());
    for (auto region_id : query_region_ids) {
        DB_WARNING("region_id: %ld", region_id);
    } 
    
    std::vector<int64_t> table_ids;
    std::vector<int64_t> partition_ids;
    std::vector<int64_t> region_ids;
    table_ids.push_back(1);
    table_ids.push_back(1);
    table_ids.push_back(1);
    partition_ids.push_back(0);
    partition_ids.push_back(0);
    partition_ids.push_back(0);
    region_ids.push_back(1);
    region_ids.push_back(2);
    region_ids.push_back(3);
    _table_manager->delete_region_ids(table_ids, partition_ids, region_ids);
    query_region_ids.clear();
    _table_manager->get_region_ids(std::string("FengChao") + "\001" + "FC_Word" + "\001" + "new_userinfo", query_region_ids);
    ASSERT_EQ(0, query_region_ids.size());

    //测试点：删除存在表的database失败
    baikaldb::pb::MetaManagerRequest request_drop_database;
    request_drop_database.set_op_type(baikaldb::pb::OP_DROP_DATABASE);
    request_drop_database.mutable_database_info()->set_database("FC_Word");
    request_drop_database.mutable_database_info()->set_namespace_name("FengChao");
    _database_manager->drop_database(request_drop_database, NULL);
    
    //测试点：删除存在database的namespace失败
    baikaldb::pb::MetaManagerRequest request_drop_namespace;
    request_drop_namespace.set_op_type(baikaldb::pb::OP_DROP_NAMESPACE);
    request_drop_namespace.mutable_namespace_info()->set_namespace_name("FengChao");
    _namespace_manager->drop_namespace(request_drop_namespace, NULL);

    //测试点：删除层级表，应该更新最顶层表信息
    baikaldb::pb::MetaManagerRequest request_drop_level_table_fc;
    request_drop_level_table_fc.set_op_type(baikaldb::pb::OP_DROP_TABLE);
    request_drop_level_table_fc.mutable_table_info()->set_table_name("planinfo");
    request_drop_level_table_fc.mutable_table_info()->set_database("FC_Word");
    request_drop_level_table_fc.mutable_table_info()->set_namespace_name("FengChao");
    _table_manager->drop_table(request_drop_level_table_fc, 8, NULL);
    ASSERT_EQ(8, _table_manager->_table_info_map[1].schema_pb.version());
    ASSERT_EQ(2, _namespace_manager->_max_namespace_id);
    ASSERT_EQ(3, _database_manager->_max_database_id);
    ASSERT_EQ(4, _table_manager->_max_table_id);
    ASSERT_EQ(1, _region_manager->_max_region_id);
    ASSERT_EQ(2, _namespace_manager->_namespace_id_map.size());
    ASSERT_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    ASSERT_EQ(2, _namespace_manager->_namespace_id_map["Feed"]);
    ASSERT_EQ(2, _namespace_manager->_database_ids[1].size());
    ASSERT_EQ(1, _namespace_manager->_database_ids[2].size());
    ASSERT_EQ(1, _database_manager->_table_ids[1].size());
    ASSERT_EQ(3, _database_manager->_database_id_map.size());
    ASSERT_EQ(1, _table_manager->_table_info_map.size());
    for (auto& table_id : _table_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _table_manager->_table_info_map) {
        DB_WARNING("whether_level_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_id:%ld, table_info:%s",
                    table_mem.first, table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%d, field_name:%s", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
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
    ASSERT_EQ(2, _namespace_manager->_database_ids[1].size());
    ASSERT_EQ(1, _namespace_manager->_database_ids[2].size());
    ASSERT_EQ(1, _database_manager->_table_ids[1].size());
    ASSERT_EQ(3, _database_manager->_database_id_map.size());
    ASSERT_EQ(1, _table_manager->_table_info_map.size());
    ASSERT_EQ(8, _table_manager->_table_info_map[1].schema_pb.version());
    for (auto& table_id : _table_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _table_manager->_table_info_map) {
        DB_WARNING("whether_level_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%d, field_name:%s", field.second, field.first.c_str());
        }
        for (auto& index : table_mem.second.index_id_map) {
            DB_WARNING("index_id:%ld, index_name:%s", index.second, index.first.c_str());
        }
    }

    // 测试点: update_charset
    baikaldb::pb::MetaManagerRequest update_charset_request;
    update_charset_request.set_op_type(baikaldb::pb::OP_UPDATE_CHARSET);
    update_charset_request.mutable_table_info()->set_table_name("new_userinfo");
    update_charset_request.mutable_table_info()->set_database("FC_Word");
    update_charset_request.mutable_table_info()->set_namespace_name("FengChao");
    update_charset_request.mutable_table_info()->set_charset(baikaldb::pb::GBK);
    _table_manager->update_charset(update_charset_request, 9, NULL);
    ASSERT_EQ(9, _table_manager->_table_info_map[1].schema_pb.version());
    for (auto& table_id : _table_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _table_manager->_table_info_map) {
        DB_WARNING("whether_level_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%d, field_name:%s", field.second, field.first.c_str());
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
    ASSERT_EQ(9, _table_manager->_table_info_map[1].schema_pb.version()); 
    for (auto& table_id : _table_manager->_table_id_map) {
        DB_WARNING("table_id:%ld, name:%s", table_id.second, table_id.first.c_str());
    }
    for (auto& table_mem : _table_manager->_table_info_map) {
        DB_WARNING("whether_level_table:%d", table_mem.second.whether_level_table);
        DB_WARNING("table_info:%s", table_mem.second.schema_pb.ShortDebugString().c_str());
        for (auto& field : table_mem.second.field_id_map) {
            DB_WARNING("field_id:%d, field_name:%s", field.second, field.first.c_str());
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

    //测试点:删除非层次表
    request_drop_level_table_fc.set_op_type(baikaldb::pb::OP_DROP_TABLE);
    request_drop_level_table_fc.mutable_table_info()->set_table_name("new_userinfo");
    request_drop_level_table_fc.mutable_table_info()->set_database("FC_Word");
    request_drop_level_table_fc.mutable_table_info()->set_namespace_name("FengChao");
    _table_manager->drop_table(request_drop_level_table_fc, 10, NULL);
    //测试点：删除database
    request_drop_database.set_op_type(baikaldb::pb::OP_DROP_DATABASE);
    request_drop_database.mutable_database_info()->set_database("FC_Word");
    request_drop_database.mutable_database_info()->set_namespace_name("FengChao");
    _database_manager->drop_database(request_drop_database, NULL);
    _schema_manager->load_snapshot();

    //测试点：删除database
    request_drop_database.set_op_type(baikaldb::pb::OP_DROP_DATABASE);
    request_drop_database.mutable_database_info()->set_database("FC_Segment");
    request_drop_database.mutable_database_info()->set_namespace_name("FengChao");
    _database_manager->drop_database(request_drop_database, NULL);
    _schema_manager->load_snapshot();

      //测试点：删除namespace
    request_drop_namespace.set_op_type(baikaldb::pb::OP_DROP_NAMESPACE);
    request_drop_namespace.mutable_namespace_info()->set_namespace_name("FengChao");
    _namespace_manager->drop_namespace(request_drop_namespace, NULL);
    _schema_manager->load_snapshot();
} // TEST_F
int main(int argc, char** argv) {
    baikaldb::FLAGS_db_path = "table_manager_db";
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
