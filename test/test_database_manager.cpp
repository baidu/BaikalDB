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
#include "namespace_manager.h"
#include "database_manager.h"
#include "query_database_manager.h"
#include "meta_rocksdb.h"
#include <gflags/gflags.h>
namespace baikaldb {
    DECLARE_string(db_path);
}
class DatabaseManagerTest : public testing::Test {
public:
    ~DatabaseManagerTest() {}
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
    }
    virtual void TearDown() {}
    baikaldb::NamespaceManager* _namespace_manager;
    baikaldb::DatabaseManager* _database_manager;
    baikaldb::QueryDatabaseManager* _query_database_manager;
    baikaldb::SchemaManager* _schema_manager;
    baikaldb::MetaRocksdb*  _rocksdb;
};
// add_logic add_physical add_instance
TEST_F(DatabaseManagerTest, test_create_drop_modify) {
    //测试点：增加命名空间“FengChao”
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

    //测试点：修改database的quota
    baikaldb::pb::MetaManagerRequest request_modify_database_fc;
    request_modify_database_fc.set_op_type(baikaldb::pb::OP_MODIFY_DATABASE);
    request_modify_database_fc.mutable_database_info()->set_database("FC_Word");
    request_modify_database_fc.mutable_database_info()->set_namespace_name("FengChao");
    request_modify_database_fc.mutable_database_info()->set_quota(50 * 1024);
    _database_manager->modify_database(request_modify_database_fc, NULL);
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
    ASSERT_EQ(2, _database_manager->_database_info_map[1].version());
    ASSERT_EQ(0, _database_manager->_table_ids.size());
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
    ASSERT_EQ(2, _database_manager->_database_info_map[1].version());
    for (auto& db_mem : _database_manager->_database_info_map) {
        DB_WARNING("DatabasePb:%s", db_mem.second.ShortDebugString().c_str());
    }
    for (auto& db_id: _database_manager->_database_id_map) {
        DB_WARNING("database_id:%ld, name:%s", db_id.second, db_id.first.c_str());
    }
    _database_manager->add_table_id(1, 1);
    ASSERT_EQ(1, _database_manager->_table_ids[1].size());
    _database_manager->add_table_id(1, 2);
    ASSERT_EQ(2, _database_manager->_table_ids[1].size());
    _database_manager->add_table_id(2, 3);
    ASSERT_EQ(1, _database_manager->_table_ids[2].size());

    _database_manager->delete_table_id(1, 1);
    ASSERT_EQ(1, _database_manager->_table_ids[1].size());
    _database_manager->delete_table_id(1, 2);
    ASSERT_EQ(0, _database_manager->_table_ids[1].size());
    _database_manager->delete_table_id(2, 3);
    ASSERT_EQ(0, _database_manager->_table_ids[2].size());
    
    //test_point: query_database_manager
    baikaldb::pb::QueryRequest query_request;
    baikaldb::pb::QueryResponse response;
    query_request.set_op_type(baikaldb::pb::QUERY_DATABASE);
    _query_database_manager->get_database_info(&query_request, &response);
    DB_WARNING("database info: %s", response.DebugString().c_str());
    
    response.clear_database_infos();
    query_request.set_namespace_name("FengChao");
    query_request.set_database("FC_Word");
    _query_database_manager->get_database_info(&query_request, &response);
    DB_WARNING("database info: %s", response.DebugString().c_str());

    int64_t database_id = _database_manager->get_database_id(std::string("FengChao") + "\001" + "FC_Word");
    ASSERT_EQ(database_id, 1);
    database_id = _database_manager->get_database_id(std::string("FengChao") + "\001" + "FC_Segment");
    ASSERT_EQ(database_id, 2);
    database_id = _database_manager->get_database_id(std::string("Feed") + "\001" + "FC_Word");
    ASSERT_EQ(database_id, 3);

    baikaldb::pb::MetaManagerRequest request_drop_database;
    request_drop_database.set_op_type(baikaldb::pb::OP_DROP_DATABASE);
    request_drop_database.mutable_database_info()->set_database("FC_Word");
    request_drop_database.mutable_database_info()->set_namespace_name("Feed");
    _database_manager->drop_database(request_drop_database, NULL);
    ASSERT_EQ(2, _namespace_manager->_max_namespace_id);
    ASSERT_EQ(2, _namespace_manager->_namespace_id_map.size());
    ASSERT_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    ASSERT_EQ(2, _namespace_manager->_namespace_id_map["Feed"]);
    ASSERT_EQ(2, _namespace_manager->_namespace_info_map.size());
    ASSERT_EQ(2, _namespace_manager->_database_ids[1].size());
    ASSERT_EQ(0, _namespace_manager->_database_ids[2].size());
    ASSERT_EQ(2, _namespace_manager->_database_ids.size());
    for (auto& ns_mem : _namespace_manager->_namespace_info_map) {
        DB_WARNING("NameSpacePb:%s", ns_mem.second.ShortDebugString().c_str());
    }
    for (auto& ns_id: _namespace_manager->_namespace_id_map) {
        DB_WARNING("namespace_id:%ld, name:%s", ns_id.second, ns_id.first.c_str());
    }
    ASSERT_EQ(3, _database_manager->_max_database_id);
    ASSERT_EQ(2, _database_manager->_database_id_map.size());
    ASSERT_EQ(2, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Segment"]);
    ASSERT_EQ(1, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Word"]);
    ASSERT_EQ(2, _database_manager->_database_info_map.size());
    ASSERT_EQ(2, _database_manager->_database_info_map[1].version());
    ASSERT_EQ(2, _database_manager->_table_ids.size());
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
    ASSERT_EQ(0, _namespace_manager->_database_ids[2].size());
    ASSERT_EQ(2, _namespace_manager->_database_ids.size());
    for (auto& ns_mem : _namespace_manager->_namespace_info_map) {
        DB_WARNING("NameSpacePb:%s", ns_mem.second.ShortDebugString().c_str());
    }
    for (auto& ns_id: _namespace_manager->_namespace_id_map) {
        DB_WARNING("namespace_id:%ld, name:%s", ns_id.second, ns_id.first.c_str());
    }
    ASSERT_EQ(3, _database_manager->_max_database_id);
    ASSERT_EQ(2, _database_manager->_database_id_map.size());
    ASSERT_EQ(2, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Segment"]);
    ASSERT_EQ(1, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Word"]);
    ASSERT_EQ(2, _database_manager->_database_info_map.size());
    ASSERT_EQ(0, _database_manager->_table_ids.size());
    ASSERT_EQ(2, _database_manager->_database_info_map[1].version());
    for (auto& db_mem : _database_manager->_database_info_map) {
        DB_WARNING("DatabasePb:%s", db_mem.second.ShortDebugString().c_str());
    }
    for (auto& db_id: _database_manager->_database_id_map) {
        DB_WARNING("database_id:%ld, name:%s", db_id.second, db_id.first.c_str());
    }

    baikaldb::pb::MetaManagerRequest request_drop_namespace;
    request_drop_namespace.set_op_type(baikaldb::pb::OP_DROP_NAMESPACE);
    request_drop_namespace.mutable_namespace_info()->set_namespace_name("FengChao");
    _namespace_manager->drop_namespace(request_drop_namespace, NULL);

    request_drop_namespace.mutable_namespace_info()->set_namespace_name("Feed");
    _namespace_manager->drop_namespace(request_drop_namespace, NULL);
    
    ASSERT_EQ(2, _namespace_manager->_max_namespace_id);
    ASSERT_EQ(1, _namespace_manager->_namespace_id_map.size());
    ASSERT_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    ASSERT_EQ(1, _namespace_manager->_namespace_info_map.size());
    ASSERT_EQ(2, _namespace_manager->_database_ids[1].size());
    ASSERT_EQ(1, _namespace_manager->_database_ids.size());
    for (auto& ns_mem : _namespace_manager->_namespace_info_map) {
        DB_WARNING("NameSpacePb:%s", ns_mem.second.ShortDebugString().c_str());
    }
    for (auto& ns_id: _namespace_manager->_namespace_id_map) {
        DB_WARNING("namespace_id:%ld, name:%s", ns_id.second, ns_id.first.c_str());
    }
    ASSERT_EQ(3, _database_manager->_max_database_id);
    ASSERT_EQ(2, _database_manager->_database_id_map.size());
    ASSERT_EQ(2, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Segment"]);
    ASSERT_EQ(1, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Word"]);
    ASSERT_EQ(2, _database_manager->_database_info_map.size());
    ASSERT_EQ(2, _database_manager->_database_info_map[1].version());
    ASSERT_EQ(0, _database_manager->_table_ids.size());
    for (auto& db_mem : _database_manager->_database_info_map) {
        DB_WARNING("DatabasePb:%s", db_mem.second.ShortDebugString().c_str());
    }
    for (auto& db_id: _database_manager->_database_id_map) {
        DB_WARNING("database_id:%ld, name:%s", db_id.second, db_id.first.c_str());
    }
    //做snapshot, 验证snapshot的正确性
    _schema_manager->load_snapshot();
    ASSERT_EQ(2, _namespace_manager->_max_namespace_id);
    ASSERT_EQ(1, _namespace_manager->_namespace_id_map.size());
    ASSERT_EQ(1, _namespace_manager->_namespace_id_map["FengChao"]);
    ASSERT_EQ(1, _namespace_manager->_namespace_info_map.size());
    ASSERT_EQ(2, _namespace_manager->_database_ids[1].size());
    ASSERT_EQ(1, _namespace_manager->_database_ids.size());
    for (auto& ns_mem : _namespace_manager->_namespace_info_map) {
        DB_WARNING("NameSpacePb:%s", ns_mem.second.ShortDebugString().c_str());
    }
    for (auto& ns_id: _namespace_manager->_namespace_id_map) {
        DB_WARNING("namespace_id:%ld, name:%s", ns_id.second, ns_id.first.c_str());
    }
    ASSERT_EQ(3, _database_manager->_max_database_id);
    ASSERT_EQ(2, _database_manager->_database_id_map.size());
    ASSERT_EQ(2, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Segment"]);
    ASSERT_EQ(1, _database_manager->_database_id_map[std::string("FengChao") + "\001" + "FC_Word"]);
    ASSERT_EQ(2, _database_manager->_database_info_map.size());
    ASSERT_EQ(0, _database_manager->_table_ids.size());
    ASSERT_EQ(2, _database_manager->_database_info_map[1].version());
    for (auto& db_mem : _database_manager->_database_info_map) {
        DB_WARNING("DatabasePb:%s", db_mem.second.ShortDebugString().c_str());
    }
    for (auto& db_id: _database_manager->_database_id_map) {
        DB_WARNING("database_id:%ld, name:%s", db_id.second, db_id.first.c_str());
    }
} // TEST_F
int main(int argc, char** argv) {
    baikaldb::FLAGS_db_path = "database_manager_db";
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
