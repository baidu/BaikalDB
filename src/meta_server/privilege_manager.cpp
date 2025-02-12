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

#include "privilege_manager.h" 
#include <boost/algorithm/string.hpp>
#ifdef BAIDU_INTERNAL
#include <baidu/rpc/channel.h>
#else
#include <brpc/channel.h>
#endif
#include "database_manager.h"
#include "meta_state_machine.h"
#include "schema_manager.h"
#include "meta_server.h"
#include "meta_util.h"
#include "meta_rocksdb.h"
#include "table_manager.h"

namespace baikaldb {
//合法性检查，真正的处理在state_machine中
void PrivilegeManager::process_user_privilege(google::protobuf::RpcController* controller, 
                                                const pb::MetaManagerRequest* request,
                                                pb::MetaManagerResponse* response,
                                                google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = 
            static_cast<brpc::Controller*>(controller);
    uint64_t log_id = 0;
    if (cntl->has_log_id()) { 
        log_id = cntl->log_id();
    }
    if (!request->has_user_privilege() && 
            request->op_type() != pb::OP_DROP_INVALID_PRIVILEGE) {
        ERROR_SET_RESPONSE(response, 
                           pb::INPUT_PARAM_ERROR, 
                           "no user_privilege", 
                           request->op_type(), 
                           log_id);
        return;
    }
    switch (request->op_type()) {
    case pb::OP_CREATE_USER:
    case pb::OP_MODIFY_USER: {
        if (!request->user_privilege().has_password()) {
            ERROR_SET_RESPONSE(response, 
                                pb::INPUT_PARAM_ERROR, 
                                "no password", 
                                request->op_type(), 
                                log_id);
            return;
        }
        _meta_state_machine->process(controller, request, response,  done_guard.release());
        return;
    }
    case pb::OP_DROP_USER: 
    case pb::OP_ADD_PRIVILEGE: 
    case pb::OP_DROP_PRIVILEGE: {
        _meta_state_machine->process(controller, request, response,  done_guard.release()); 
        return;
    }
    case pb::OP_DROP_INVALID_PRIVILEGE: {
        _meta_state_machine->process(controller, request, response,  done_guard.release());
        return;
    }
    default: {
        ERROR_SET_RESPONSE(response, 
                           pb::INPUT_PARAM_ERROR, 
                           "invalid op_type", 
                           request->op_type(), 
                           log_id);
        return;
    }
    }
}

void PrivilegeManager::create_user(const pb::MetaManagerRequest& request, braft::Closure* done) {
    auto& user_privilege = const_cast<pb::UserPrivilege&>(request.user_privilege());
    std::string username = user_privilege.username();
    if (_user_privilege.find(username) != _user_privilege.end()) {
        DB_WARNING("request username has been created, username:%s", 
                user_privilege.username().c_str());
        if (user_privilege.if_exist()) {
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "username has been repeated");
        } else {
            IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
        }
        return;
    }
    int ret = SchemaManager::get_instance()->check_and_get_for_privilege(user_privilege);
    if (ret < 0) {
        DB_WARNING("request not illegal, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "request invalid");
        return;
    }
    user_privilege.set_version(1);

    // 构造key 和 value
    std::string value;
    if (!user_privilege.SerializeToString(&value)) {
        DB_WARNING("request serializeToArray fail, request:%s",request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
        return;
    }
    // write date to rocksdb
    ret = MetaRocksdb::get_instance()->put_meta_info(construct_privilege_key(username), value);
    if (ret < 0) {
        DB_WARNING("add username:%s privilege to rocksdb fail", username.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    //更新内存
    BAIDU_SCOPED_LOCK(_user_mutex);
    _user_privilege[username] = user_privilege;
    insert_db_tbl_user_map(user_privilege);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("create user success, request:%s", request.ShortDebugString().c_str());
}

void PrivilegeManager::drop_user(const pb::MetaManagerRequest& request, braft::Closure* done) {
    auto& user_privilege = const_cast<pb::UserPrivilege&>(request.user_privilege());
    std::string username =  request.user_privilege().username();
    if (_user_privilege.find(username) == _user_privilege.end()) {                        
        DB_WARNING("request username not exist, username:%s", username.c_str());
        if (!user_privilege.if_exist()) {
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "username not exist");
        } else {
            IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
        }
        return;
    }
    auto ret = MetaRocksdb::get_instance()->delete_meta_info(
                std::vector<std::string>{construct_privilege_key(username)});
    if (ret < 0) {
        DB_WARNING("drop username:%s privilege to rocksdb fail", username.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "delete from db fail");
        return;
    }
    pb::UserPrivilege delete_privilege = _user_privilege[username];
    BAIDU_SCOPED_LOCK(_user_mutex);
    _user_privilege.erase(username);
    delete_db_tbl_user_map(delete_privilege);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("drop user success, request:%s", request.ShortDebugString().c_str());
}

void PrivilegeManager::modify_user(const pb::MetaManagerRequest& request, braft::Closure* done) {
    auto& user_privilege = const_cast<pb::UserPrivilege&>(request.user_privilege());
    std::string username = user_privilege.username();
    if (_user_privilege.find(username) == _user_privilege.end()) {
        DB_WARNING("request username not exist, username:%s", username.c_str());
        if (!user_privilege.if_exist()) {
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "username not exist");
        } else {
            IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
        }
        return;
    }
    int ret = SchemaManager::get_instance()->check_and_get_for_privilege(user_privilege);
    if (ret < 0) {
        DB_WARNING("request not illegal, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "request invalid");
        return;
    }
    // 目前仅支持修改密码与namespace
    pb::UserPrivilege tmp_info = _user_privilege[username];
    tmp_info.set_namespace_id(user_privilege.namespace_id());
    tmp_info.set_namespace_name(user_privilege.namespace_name());
    tmp_info.set_password(user_privilege.password());
    tmp_info.set_version(tmp_info.version() + 1);

    // 构造key 和 value
    std::string value;
    if (!tmp_info.SerializeToString(&value)) {
        DB_WARNING("request serializeToArray fail, request:%s",request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
        return;
    }
    // write date to rocksdb
    ret = MetaRocksdb::get_instance()->put_meta_info(construct_privilege_key(username), value);
    if (ret < 0) {
        DB_WARNING("add username:%s privilege to rocksdb fail", username.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    //更新内存
    BAIDU_SCOPED_LOCK(_user_mutex);
    _user_privilege[username] = tmp_info;
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("alter user success, request:%s", request.ShortDebugString().c_str());
}

void PrivilegeManager::add_privilege(const pb::MetaManagerRequest& request, braft::Closure* done) {
    auto& user_privilege = const_cast<pb::UserPrivilege&>(request.user_privilege());
    std::string username = user_privilege.username();
    if (_user_privilege.find(username) == _user_privilege.end()) {
        DB_WARNING("request username not exist, username:%s", username.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "username not exist");
        return;
    }
    int ret = SchemaManager::get_instance()->check_and_get_for_privilege(user_privilege);          
    if (ret < 0) {
        DB_WARNING("request not illegal, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "request invalid");
        return; 
    }
    pb::UserPrivilege insert_privilege;
    insert_privilege.set_username(user_privilege.username());
    pb::UserPrivilege tmp_mem_privilege = _user_privilege[username];
    for (auto& privilege_database : user_privilege.privilege_database()) {
        insert_database_privilege(privilege_database, tmp_mem_privilege, insert_privilege);
    }
    for (auto& privilege_table :  user_privilege.privilege_table()) {
        insert_table_privilege(privilege_table, tmp_mem_privilege, insert_privilege);
    }
    for (auto& bns : user_privilege.bns()) {
        insert_bns(bns, tmp_mem_privilege);
    }
    for (auto& ip : user_privilege.ip()) {
        insert_ip(ip, tmp_mem_privilege);
    }
    for (auto& switch_table : user_privilege.switch_tables()) {
        insert_switch_table(switch_table, tmp_mem_privilege);
    }
    if (user_privilege.has_need_auth_addr()) {
        tmp_mem_privilege.set_need_auth_addr(user_privilege.need_auth_addr());
    }
    if (user_privilege.has_resource_tag()) {
        tmp_mem_privilege.set_resource_tag(user_privilege.resource_tag());
    }
    if (user_privilege.has_ddl_permission()) {
        tmp_mem_privilege.set_ddl_permission(user_privilege.ddl_permission());
    }
    if (user_privilege.has_txn_lock_timeout()) {
        tmp_mem_privilege.set_txn_lock_timeout(user_privilege.txn_lock_timeout());
    }
    if (user_privilege.has_use_read_index()) {
        tmp_mem_privilege.set_use_read_index(user_privilege.use_read_index());
    }
    if (user_privilege.has_enable_plan_cache()) {
        tmp_mem_privilege.set_enable_plan_cache(user_privilege.enable_plan_cache());
    }
    if (user_privilege.has_request_range_partition_type()) {
        tmp_mem_privilege.set_request_range_partition_type(user_privilege.request_range_partition_type());
    }
    if (user_privilege.has_acl()) { // grank
        tmp_mem_privilege.set_acl(tmp_mem_privilege.acl() | user_privilege.acl());
    }
    if (user_privilege.has_is_super()) {
        tmp_mem_privilege.set_is_super(user_privilege.is_super());
    }
    if (user_privilege.has_is_request_additional()) {
        tmp_mem_privilege.set_is_request_additional(user_privilege.is_request_additional());
    }

    tmp_mem_privilege.set_version(tmp_mem_privilege.version() + 1);
    // 构造key 和 value
    std::string value;
    if (!tmp_mem_privilege.SerializeToString(&value)) {
        DB_WARNING("request serializeToArray fail, request:%s",request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
        return;
    }
    // write date to rocksdb
    ret = MetaRocksdb::get_instance()->put_meta_info(construct_privilege_key(username), value);
    if (ret != 0) {
        DB_WARNING("add username:%s privilege to rocksdb fail", username.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    BAIDU_SCOPED_LOCK(_user_mutex);
    _user_privilege[username] = tmp_mem_privilege;
    insert_db_tbl_user_map(insert_privilege);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("add privilege success, request:%s", request.ShortDebugString().c_str());
}

void PrivilegeManager::drop_privilege(const pb::MetaManagerRequest& request, braft::Closure* done) {
    auto& user_privilege = const_cast<pb::UserPrivilege&>(request.user_privilege());
    std::string username = user_privilege.username();
    if (_user_privilege.find(username) == _user_privilege.end()) {
        DB_WARNING("request username not exist, username:%s", username.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "username not exist");
        return;
    }
    int ret = SchemaManager::get_instance()->check_and_get_for_privilege(user_privilege);          
    if (ret < 0) {
        DB_WARNING("request not illegal, request:%s",request.ShortDebugString().c_str()); 
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "request invalid");
        return; 
    }
    pb::UserPrivilege delete_privilege;
    delete_privilege.set_username(user_privilege.username());
    pb::UserPrivilege tmp_mem_privilege = _user_privilege[username];
    for (auto& privilege_database : user_privilege.privilege_database()) {
        delete_database_privilege(privilege_database, tmp_mem_privilege, delete_privilege);
    }
    for (auto& privilege_table : user_privilege.privilege_table()) {
        delete_table_privilege(privilege_table, tmp_mem_privilege, delete_privilege);
    }
    for (auto& bns : user_privilege.bns()) {
        delete_bns(bns, tmp_mem_privilege);
    }
    for (auto& ip : user_privilege.ip()) {
        delete_ip(ip, tmp_mem_privilege);
    }
    for (auto& switch_table : user_privilege.switch_tables()) {
        delete_switch_table(switch_table, tmp_mem_privilege);
    }
    if (user_privilege.has_need_auth_addr()) {
        tmp_mem_privilege.set_need_auth_addr(user_privilege.need_auth_addr());
    }
    if (user_privilege.has_resource_tag() && tmp_mem_privilege.has_resource_tag() && 
        user_privilege.resource_tag() == tmp_mem_privilege.resource_tag()) {
        tmp_mem_privilege.clear_resource_tag();
    }
    if (user_privilege.has_acl()) { // revoke
        tmp_mem_privilege.set_acl(tmp_mem_privilege.acl() & ~user_privilege.acl());
    }
    tmp_mem_privilege.set_version(tmp_mem_privilege.version() + 1);
    // 构造key 和 value
    std::string value;
    if (!tmp_mem_privilege.SerializeToString(&value)) {
        DB_WARNING("request serializeToArray fail, request:%s",request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
        return;
    }
    // write date to rocksdb
    ret = MetaRocksdb::get_instance()->put_meta_info(construct_privilege_key(username), value);
    if (ret < 0) {
        DB_WARNING("add username:%s privilege to rocksdb fail", username.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    BAIDU_SCOPED_LOCK(_user_mutex);
    _user_privilege[username] = tmp_mem_privilege;
    delete_db_tbl_user_map(delete_privilege);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("drop privilege success, request:%s", request.ShortDebugString().c_str());
}

void PrivilegeManager::process_baikal_heartbeat(const pb::BaikalHeartBeatRequest* request, 
                                               pb::BaikalHeartBeatResponse* response) {
    BAIDU_SCOPED_LOCK(_user_mutex);
    for (auto& user_info : _user_privilege) {
        auto privilege = response->add_privilege_change_info();    
        *privilege = user_info.second;
    }
}

int PrivilegeManager::load_snapshot() {
    _user_privilege.clear();
    _db_user_map.clear();
    _tbl_user_map.clear();
    std::string privilege_prefix = MetaServer::PRIVILEGE_IDENTIFY;
    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    read_options.total_order_seek = false;
    RocksWrapper* db = RocksWrapper::get_instance();
    std::unique_ptr<rocksdb::Iterator> iter(
            db->new_iterator(read_options, db->get_meta_info_handle()));
    iter->Seek(privilege_prefix);
    for (; iter->Valid(); iter->Next()) {
        std::string username(iter->key().ToString(), privilege_prefix.size());
        pb::UserPrivilege user_privilege;
        if (!user_privilege.ParseFromString(iter->value().ToString())) {
            DB_FATAL("parse from pb fail when load privilege snapshot, key:%s", 
                     iter->key().data());
            return -1;
        }
        DB_WARNING("user_privilege:%s", user_privilege.ShortDebugString().c_str());
        BAIDU_SCOPED_LOCK(_user_mutex);
        _user_privilege[username] = user_privilege;
        insert_db_tbl_user_map(user_privilege);
    }
    return 0;
}

void PrivilegeManager::insert_database_privilege(const pb::PrivilegeDatabase& privilege_database,
                                                 pb::UserPrivilege& mem_privilege,
                                                 pb::UserPrivilege& insert_privilege) {
    bool whether_exist = false;
    pb::PrivilegeDatabase* mem_database_ptr = nullptr;
    for (auto& mem_database : *mem_privilege.mutable_privilege_database()) {
        if (mem_database.database_id() == privilege_database.database_id()) {
            whether_exist = true;
            mem_database_ptr = &mem_database;

            if (!privilege_database.has_database_rw()) { // for grant only
                break;
            }

            if (privilege_database.force()) {
                mem_database.set_database_rw(privilege_database.database_rw());
            } else {
                if (privilege_database.database_rw() > mem_database.database_rw()) {
                    mem_database.set_database_rw(privilege_database.database_rw());
                }
            }
            break;
        }
    }
    if (!whether_exist) {
        pb::PrivilegeDatabase* ptr_database = mem_privilege.add_privilege_database();
        *ptr_database = privilege_database;
        *(insert_privilege.add_privilege_database()) = privilege_database;
    } else if (privilege_database.has_acl()){ // for grank
        mem_database_ptr->set_acl(mem_database_ptr->acl() | privilege_database.acl());
    }
}

void PrivilegeManager::insert_table_privilege(const pb::PrivilegeTable& privilege_table,
                                              pb::UserPrivilege& mem_privilege,
                                              pb::UserPrivilege& insert_privilege) {
    bool whether_exist = false;
    int64_t database_id = privilege_table.database_id();
    int64_t table_id = privilege_table.table_id();
    pb::PrivilegeTable* mem_privilege_table_ptr = nullptr;
    for (auto& mem_privilege_table : *mem_privilege.mutable_privilege_table()) {
        if (mem_privilege_table.database_id() == database_id
                && mem_privilege_table.table_id() == table_id) {
            whether_exist = true;
            mem_privilege_table_ptr = &mem_privilege_table;

            if (!privilege_table.has_table_rw()) { // for grant only
                break;
            }

            if (privilege_table.force()) {
                mem_privilege_table.set_table_rw(privilege_table.table_rw());
            } else {
                if (privilege_table.table_rw() > mem_privilege_table.table_rw()) {
                    mem_privilege_table.set_table_rw(privilege_table.table_rw());
                }
            }
            
            break;
        }
    }
    if (!whether_exist) {
         pb::PrivilegeTable* ptr_table = mem_privilege.add_privilege_table();
         *ptr_table = privilege_table;
         *(insert_privilege.add_privilege_table()) = privilege_table;
    } else if (privilege_table.has_acl()){ // for grank
        mem_privilege_table_ptr->set_acl(mem_privilege_table_ptr->acl() | privilege_table.acl());
    }
}

void PrivilegeManager::insert_bns(const std::string& bns,
                                   pb::UserPrivilege& mem_privilege) {
    bool whether_exist = false;
    for (auto& mem_bns :  mem_privilege.bns()) {
        if (mem_bns == bns) {
            whether_exist = true;
        }
    }
    if (!whether_exist) {
        mem_privilege.add_bns(bns);
    }
}

void PrivilegeManager::insert_ip(const std::string& ip,
                                 pb::UserPrivilege& mem_privilege) {
    bool whether_exist = false;
    for (auto& mem_ip : mem_privilege.ip()) {
        if (mem_ip == ip) {
            whether_exist = true;
        }
    }
    if (!whether_exist) {
        mem_privilege.add_ip(ip);
    }
}

void PrivilegeManager::insert_switch_table(const int64_t& switch_table,
                                 pb::UserPrivilege& mem_privilege) {
    bool whether_exist = false;
    for (auto& mem_switch_table : mem_privilege.switch_tables()) {
        if (mem_switch_table == switch_table) {
            whether_exist = true;
        }
    }
    if (!whether_exist) {
        mem_privilege.add_switch_tables(switch_table);
    }
}

void PrivilegeManager::delete_database_privilege(const pb::PrivilegeDatabase& privilege_database, 
                                                 pb::UserPrivilege& mem_privilege, 
                                                 pb::UserPrivilege& delete_privilege) {
    pb::UserPrivilege copy_mem_privilege = mem_privilege;
    mem_privilege.clear_privilege_database();
    for (auto& copy_database : copy_mem_privilege.privilege_database()) {
        if (copy_database.database_id() == privilege_database.database_id()) {
            // 收回写权限
            if (privilege_database.has_database_rw() && 
                    privilege_database.database_rw() < copy_database.database_rw()) {
                auto add_database = mem_privilege.add_privilege_database();
                *add_database = privilege_database;
            } else {
                *(delete_privilege.add_privilege_database()) = privilege_database;
            }
            // 收回revoke的权限
            if (privilege_database.has_acl()) {
                auto add_database = mem_privilege.add_privilege_database();
                *add_database = copy_database;
                add_database->set_acl(add_database->acl() & ~privilege_database.acl());
            } 
        } else {
            auto add_database = mem_privilege.add_privilege_database();
            *add_database = copy_database;
        }
    }
}

void PrivilegeManager::delete_table_privilege(const pb::PrivilegeTable& privilege_table,
                                              pb::UserPrivilege& mem_privilege,
                                              pb::UserPrivilege& delete_privilege) {
    int64_t database_id = privilege_table.database_id();
    int64_t table_id = privilege_table.table_id();
    pb::UserPrivilege copy_mem_privilege = mem_privilege;
    mem_privilege.clear_privilege_table();
    for (auto& copy_table : copy_mem_privilege.privilege_table()) {
        if (database_id == copy_table.database_id() && table_id == copy_table.table_id()) {
            // 写权限收回
            if (privilege_table.has_table_rw() && 
                    privilege_table.table_rw() < copy_table.table_rw()) {
                 auto add_table = mem_privilege.add_privilege_table();
                 *add_table = privilege_table;
            } else {
                *(delete_privilege.add_privilege_table()) = privilege_table;
            }
            // 收回revoke的权限
            if (privilege_table.has_acl()) {
                auto add_table = mem_privilege.add_privilege_table();
                *add_table = copy_table;
                add_table->set_acl(add_table->acl() & ~privilege_table.acl());
            }
        } else {
            auto add_table = mem_privilege.add_privilege_table();
            *add_table = copy_table;
        }
    }
}

void PrivilegeManager::delete_bns(const std::string& bns,
                                  pb::UserPrivilege& mem_privilege) {
    pb::UserPrivilege copy_mem_privilege = mem_privilege;
    mem_privilege.clear_bns();
    for (auto copy_bns : copy_mem_privilege.bns()) {
        if (copy_bns != bns) {
            mem_privilege.add_bns(copy_bns);
        }
    }
}

void PrivilegeManager::delete_ip(const std::string& ip,
                                  pb::UserPrivilege& mem_privilege) {
    pb::UserPrivilege copy_mem_privilege = mem_privilege;
    mem_privilege.clear_ip();
    for (auto copy_ip : copy_mem_privilege.ip()) {
        if (copy_ip != ip) {
            mem_privilege.add_ip(copy_ip);
        }
    }
}

void PrivilegeManager::delete_switch_table(const int64_t& switch_table,
                                  pb::UserPrivilege& mem_privilege) {
    pb::UserPrivilege copy_mem_privilege = mem_privilege;
    mem_privilege.clear_switch_tables();
    for (auto copy_switch_table : copy_mem_privilege.switch_tables()) {
        if (copy_switch_table != switch_table) {
            mem_privilege.add_switch_tables(copy_switch_table);
        }
    }
}

void PrivilegeManager::drop_invalid_privilege(const pb::MetaManagerRequest& request, braft::Closure* done) {
    std::unordered_map<std::string, pb::UserPrivilege> user_privilege = _user_privilege;
    for (auto& kv : user_privilege) {
        bool has_invalid_privilege = false;
        auto& username = kv.first;
        auto& privilege = kv.second;

        pb::UserPrivilege delete_privilege;
        delete_privilege.set_username(username);

        // drop invalid privilege_database
        int slow = 0;
        int fast = 0;
        for (; fast < privilege.privilege_database().size(); ++fast) {
            pb::DataBaseInfo db_info;
            if (DatabaseManager::get_instance()->get_database_info(privilege.privilege_database(fast).database_id(), db_info) == 0) {
                privilege.mutable_privilege_database()->SwapElements(slow, fast);
                ++slow;
            }
        }
        int remove_num = privilege.privilege_database().size() - slow;
        if (remove_num > 0) {
            has_invalid_privilege = true;
        }
        for (int i = 0; i < remove_num; ++i) {
            delete_privilege.add_privilege_database()->CopyFrom(
                        privilege.privilege_database(privilege.privilege_database().size() - 1));
            privilege.mutable_privilege_database()->RemoveLast();
        }

        // drop invalid privilege_table
        slow = 0;
        fast = 0;
        for (; fast < privilege.privilege_table().size(); ++fast) {
            pb::SchemaInfo tbl_info;
            if (TableManager::get_instance()->get_table_info(privilege.privilege_table(fast).table_id(), tbl_info) == 0) {
                privilege.mutable_privilege_table()->SwapElements(slow, fast);
                ++slow;
            }
        }
        remove_num = privilege.privilege_table().size() - slow;
        if (remove_num > 0) {
            has_invalid_privilege = true;
        }
        for (int i = 0; i < remove_num; ++i) {
            delete_privilege.add_privilege_table()->CopyFrom(
                        privilege.privilege_table(privilege.privilege_table().size() - 1));
            privilege.mutable_privilege_table()->RemoveLast();
        }

        if (has_invalid_privilege) {
            privilege.set_version(privilege.version() + 1);
            std::string value;
            if (!privilege.SerializeToString(&value)) {
                DB_WARNING("request serializeToArray fail, request:%s",request.ShortDebugString().c_str());
                IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
                return;
            }
            int ret = MetaRocksdb::get_instance()->put_meta_info(construct_privilege_key(username), value);
            if (ret < 0) {
                DB_WARNING("add username:%s privilege to rocksdb fail", username.c_str());
                IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
                return;
            }
            {
                BAIDU_SCOPED_LOCK(_user_mutex);
                _user_privilege[username] = privilege;
                delete_db_tbl_user_map(delete_privilege);
            }
        }
    }
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("drop invalid privilege success, request: %s", request.ShortDebugString().c_str());
}

}// namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
