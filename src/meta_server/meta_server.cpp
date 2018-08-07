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

#include "meta_server.h"
#include <boost/lexical_cast.hpp>
#include "auto_incr_state_machine.h"
#include "meta_state_machine.h"
#include "cluster_manager.h"
#include "privilege_manager.h"
#include "schema_manager.h"
#include "query_cluster_manager.h"
#include "query_privilege_manager.h"
#include "query_namespace_manager.h"
#include "query_database_manager.h"
#include "query_table_manager.h"
#include "query_region_manager.h"
#include "meta_util.h"
#include "meta_rocksdb.h"

namespace baikaldb {
DEFINE_int32(meta_port, 8010, "Meta port");
DEFINE_int32(meta_replica_number, 3, "Meta replica num");
DEFINE_int32(concurrency_num, 40, "concurrency num, default: 40");

const std::string MetaServer::CLUSTER_IDENTIFY(1, 0x01);
const std::string MetaServer::LOGICAL_CLUSTER_IDENTIFY(1, 0x01);
const std::string MetaServer::LOGICAL_KEY = "logical_room";
const std::string MetaServer::PHYSICAL_CLUSTER_IDENTIFY(1, 0x02);
const std::string MetaServer::INSTANCE_CLUSTER_IDENTIFY(1, 0x03);

const std::string MetaServer::PRIVILEGE_IDENTIFY(1, 0x03);

const std::string MetaServer::SCHEMA_IDENTIFY(1, 0x02);
const std::string MetaServer::MAX_ID_SCHEMA_IDENTIFY(1, 0x01);
const std::string MetaServer::NAMESPACE_SCHEMA_IDENTIFY(1, 0x02);
const std::string MetaServer::DATABASE_SCHEMA_IDENTIFY(1, 0x03);
const std::string MetaServer::TABLE_SCHEMA_IDENTIFY(1, 0x04);
const std::string MetaServer::REGION_SCHEMA_IDENTIFY(1, 0x05);

const std::string MetaServer::MAX_IDENTIFY(1, 0xFF);

MetaServer::~MetaServer() {}
int MetaServer::init(const std::vector<braft::PeerId>& peers) {
    auto ret = MetaRocksdb::get_instance()->init();
    if (ret < 0) {
        DB_FATAL("rocksdb init fail");
        return -1;
    }
    butil::EndPoint addr;
    addr.ip = butil::my_ip();
    addr.port = FLAGS_meta_port;
    braft::PeerId peer_id(addr, 0);
    _meta_state_machine = new (std::nothrow)MetaStateMachine(peer_id);
    if (_meta_state_machine == NULL) {
        DB_FATAL("new meta_state_machine fail");
        return -1;
    }
    //state_machine初始化
    ret = _meta_state_machine->init(peers);
    if (ret != 0) {
        DB_FATAL("meta state machine init fail");
        return -1;
    }
    DB_WARNING("meta state machine init success");
    
    _auto_incr_state_machine = new (std::nothrow)AutoIncrStateMachine(peer_id);
    if (_auto_incr_state_machine == NULL) {
        DB_FATAL("new auot_incr_state_machine fail");
        return -1;
    }
    ret = _auto_incr_state_machine->init(peers);
    if (ret != 0) {
        DB_FATAL(" auot_incr_state_machine init fail");
        return -1;
    }
    DB_WARNING("auot_incr_state_machine init success");

    SchemaManager::get_instance()->set_meta_state_machine(_meta_state_machine);
    PrivilegeManager::get_instance()->set_meta_state_machine(_meta_state_machine);
    ClusterManager::get_instance()->set_meta_state_machine(_meta_state_machine);
    
    _init_success = true;
    return 0;
}

//该方法主要做请求分发
void MetaServer::meta_manager(google::protobuf::RpcController* controller,
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
    RETURN_IF_NOT_INIT(_init_success, response, log_id);
    if (request->op_type() == pb::OP_ADD_PHYSICAL
            || request->op_type() == pb::OP_ADD_LOGICAL
            || request->op_type() == pb::OP_ADD_INSTANCE
            || request->op_type() == pb::OP_DROP_PHYSICAL
            || request->op_type() == pb::OP_DROP_LOGICAL
            || request->op_type() == pb::OP_DROP_INSTANCE
            || request->op_type() == pb::OP_UPDATE_INSTANCE 
            || request->op_type() == pb::OP_MOVE_PHYSICAL) {
        ClusterManager::get_instance()->process_cluster_info(controller,
                                               request,
                                               response,
                                               done_guard.release());
        return;
    }
    if (request->op_type() == pb::OP_CREATE_USER
            || request->op_type() == pb::OP_DROP_USER
            || request->op_type() == pb::OP_ADD_PRIVILEGE
            || request->op_type() == pb::OP_DROP_PRIVILEGE) {
        PrivilegeManager::get_instance()->process_user_privilege(controller,
                                                   request,
                                                   response,
                                                   done_guard.release());
        return;
    }
    if (request->op_type() == pb::OP_CREATE_NAMESPACE 
            || request->op_type() == pb::OP_DROP_NAMESPACE 
            || request->op_type() == pb::OP_MODIFY_NAMESPACE
            || request->op_type() == pb::OP_CREATE_DATABASE 
            || request->op_type() == pb::OP_DROP_DATABASE 
            || request->op_type() == pb::OP_MODIFY_DATABASE
            || request->op_type() == pb::OP_CREATE_TABLE 
            || request->op_type() == pb::OP_DROP_TABLE 
            || request->op_type() == pb::OP_RENAME_TABLE
            || request->op_type() == pb::OP_ADD_FIELD
            || request->op_type() == pb::OP_DROP_FIELD
            || request->op_type() == pb::OP_RENAME_FIELD
            || request->op_type() == pb::OP_MODIFY_FIELD
            || request->op_type() == pb::OP_UPDATE_REGION
            || request->op_type() == pb::OP_RESTORE_REGION
            || request->op_type() == pb::OP_DROP_REGION
            || request->op_type() == pb::OP_SPLIT_REGION
            || request->op_type() == pb::OP_UPDATE_BYTE_SIZE) {
        SchemaManager::get_instance()->process_schema_info(controller,
                                             request,
                                             response,
                                             done_guard.release());
        return;
    }
    if (request->op_type() == pb::OP_GEN_ID_FOR_AUTO_INCREMENT
            || request->op_type() == pb::OP_UPDATE_FOR_AUTO_INCREMENT
            || request->op_type() == pb::OP_ADD_ID_FOR_AUTO_INCREMENT
            || request->op_type() == pb::OP_DROP_ID_FOR_AUTO_INCREMENT) {
        _auto_incr_state_machine->process(controller,
                                          request,
                                          response,
                                          done_guard.release());
        return;
    }
    if (request->op_type() == pb::OP_UNSAFE_DECISION) {
        SchemaManager::get_instance()->set_unsafe_decision();
        response->set_errcode(pb::SUCCESS);
        response->set_op_type(request->op_type());
        return;
    }
    if (request->op_type() == pb::OP_SET_INSTANCE_DEAD) {
        ClusterManager::get_instance()->set_instance_dead(request, response, log_id);
        response->set_errcode(pb::SUCCESS);
        response->set_op_type(request->op_type());
        return;
    }
    if (request->op_type() == pb::OP_CLOSE_LOAD_BALANCE) {
        _meta_state_machine->set_close_load_balance(true); 
        response->set_errcode(pb::SUCCESS);
        response->set_op_type(request->op_type());
    }
    if (request->op_type() == pb::OP_OPEN_LOAD_BALANCE) {
        _meta_state_machine->set_close_load_balance(false);
        response->set_errcode(pb::SUCCESS);
        response->set_op_type(request->op_type());
    }
    DB_FATAL("request has wrong op_type:%d , log_id:%lu", 
                    request->op_type(), log_id);
    response->set_errcode(pb::INPUT_PARAM_ERROR);
    response->set_errmsg("invalid op_type");
    response->set_op_type(request->op_type());
}

void MetaServer::query(google::protobuf::RpcController* controller,
            const pb::QueryRequest* request,
            pb::QueryResponse* response,
            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(controller);
    uint64_t log_id = 0;
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    RETURN_IF_NOT_INIT(_init_success, response, log_id);
    TimeCost time_cost;
    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("success");
    switch (request->op_type()) {
    case pb::QUERY_LOGICAL: {
        QueryClusterManager::get_instance()->get_logical_info(request, response);
        break;
    }
    case pb::QUERY_PHYSICAL: {
        QueryClusterManager::get_instance()->get_physical_info(request, response);
        break;
    }
    case pb::QUERY_INSTANCE: {
        QueryClusterManager::get_instance()->get_instance_info(request, response);
        break;
    }
    case pb::QUERY_USERPRIVILEG: {
        QueryPrivilegeManager::get_instance()->get_user_info(request, response);
        break;
    }
    case pb::QUERY_NAMESPACE: {
        QueryNamespaceManager::get_instance()->get_namespace_info(request, response);
        break;
    }
    case pb::QUERY_DATABASE: {
        QueryDatabaseManager::get_instance()->get_database_info(request, response);
        break;
    }
    case pb::QUERY_SCHEMA: {
        QueryTableManager::get_instance()->get_schema_info(request, response);
        break;
    }
    case pb::QUERY_REGION: {
        QueryRegionManager::get_instance()->get_region_info(request, response);
        break;
    }
    case pb::QUERY_INSTANCE_FLATTEN: {
        QueryClusterManager::get_instance()->get_flatten_instance(request, response);
        break;
    }
    case pb::QUERY_PRIVILEGE_FLATTEN: {
        QueryPrivilegeManager::get_instance()->get_flatten_privilege(request, response);
        break;
    }
    case pb::QUERY_REGION_FLATTEN: {
        QueryRegionManager::get_instance()->get_flatten_region(request, response); 
        break;
    }
     case pb::QUERY_TABLE_FLATTEN: {
        QueryTableManager::get_instance()->get_flatten_table(request, response); 
        break;
    }
    case pb::QUERY_SCHEMA_FLATTEN: {
        QueryTableManager::get_instance()->get_flatten_schema(request, response);
        break;
    }
    case pb::QUERY_TRANSFER_LEADER: {
        QueryRegionManager::get_instance()->send_transfer_leader(request, response);
        break;
    }
    case pb::QUERY_SET_PEER: {
        QueryRegionManager::get_instance()->send_set_peer(request, response);
        break;
    }
    default: {
        DB_WARNING("invalid op_type, request:%s logid:%lu", 
                    request->ShortDebugString().c_str(), log_id);
        response->set_errcode(pb::INPUT_PARAM_ERROR);
        response->set_errmsg("invalid op_type");
    }
    }
    DB_NOTICE("query op_type_name:%s, time_cost:%ld, request: %s", 
                pb::QueryOpType_Name(request->op_type()).c_str(), 
                time_cost.get_time(), request->ShortDebugString().c_str());
}
void MetaServer::raft_control(google::protobuf::RpcController* controller,
                              const pb::RaftControlRequest* request,
                              pb::RaftControlResponse* response,
                              google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (request->region_id() == 0) {
        _meta_state_machine->raft_control(controller, request, response, done_guard.release()); 
        return;
    }
    if (request->region_id() == 1) {
        _auto_incr_state_machine->raft_control(controller, request, response, done_guard.release());
        return;
    }
    response->set_region_id(request->region_id());
    response->set_errcode(pb::INPUT_PARAM_ERROR);
    response->set_errmsg("unmatch region id");
    DB_FATAL("unmatch region_id in meta server, request: %s", request->ShortDebugString().c_str());
}
void MetaServer::store_heartbeat(google::protobuf::RpcController* controller,
                                 const pb::StoreHeartBeatRequest* request,
                                 pb::StoreHeartBeatResponse* response,
                                 google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(controller);
    uint64_t log_id = 0;
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    RETURN_IF_NOT_INIT(_init_success, response, log_id);
    if (_meta_state_machine != NULL) {
        _meta_state_machine->store_heartbeat(controller, request, response, done_guard.release());
    }
}

void MetaServer::baikal_heartbeat(google::protobuf::RpcController* controller,
                                  const pb::BaikalHeartBeatRequest* request,
                                  pb::BaikalHeartBeatResponse* response,
                                  google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(controller);
    uint64_t log_id = 0;
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    RETURN_IF_NOT_INIT(_init_success, response, log_id);
    if (_meta_state_machine != nullptr) {
        _meta_state_machine->baikal_heartbeat(controller, request, response, done_guard.release());
    }
}

void MetaServer::shutdown_raft() {
    if (_meta_state_machine != nullptr) {
        _meta_state_machine->shutdown_raft();
    }   
    if (_auto_incr_state_machine != nullptr) {
        _auto_incr_state_machine->shutdown_raft();
    }    
}
}//namespace
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
