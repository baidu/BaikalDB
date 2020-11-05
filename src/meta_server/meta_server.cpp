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

#include "meta_server.h"
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include "auto_incr_state_machine.h"
#include "tso_state_machine.h"
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
DEFINE_int64(region_apply_raft_interval_ms, 1000LL,
            "region apply raft interval, defalut(1s)");
DECLARE_int64(flush_memtable_interval_us);
#ifdef BAIDU_INTERNAL
// for migrate
DEFINE_string(ps_meta_bns, "group.opera-ps-baikalMeta-000-bj.FENGCHAO.all", "");
DEFINE_string(e0_meta_bns, "group.opera-e0-baikalMeta-000-yz.FENGCHAO.all", "");
DEFINE_string(holmes_meta_bns, "group.opera-holmes-baikalMeta-000-yq.FENGCHAO.all", "");
DEFINE_string(holmes_product_meta_bns, "group.opera-online-baikalMeta-000-bj.HOLMES.all", "");
DEFINE_string(dmp_meta_bns, "group.opera-online-baikalMeta-000-bj.DMP.all", "");
DEFINE_string(coffline_meta_bns, "group.opera-coffline-baikalMeta-000-bj.FENGCHAO.all", "");
DEFINE_string(detect_meta_bns, "group.opera-detect-baikalMeta-000-bj.FENGCHAO.all", "");
DEFINE_string(pinpai_meta_bns, "group.opera-pinpai-baikalMeta-000-cm.FENGCHAO.all", "");
DEFINE_string(aladdin_meta_bns, "group.opera-aladdin-baikalMeta-000-bj.FENGCHAO.all", "");
DEFINE_string(qa_meta_bns, "group.opera-qa-qabaikalMeta-000-ct.FENGCHAO.all", "");
#endif

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

const std::string MetaServer::DDLWORK_IDENTIFY(1, 0x06);
const std::string MetaServer::STATISTICS_IDENTIFY(1, 0x07);
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

    _tso_state_machine = new (std::nothrow)TSOStateMachine(peer_id);
    if (_tso_state_machine == NULL) {
        DB_FATAL("new _tso_state_machine fail");
        return -1;
    }
    ret = _tso_state_machine->init(peers);
    if (ret != 0) {
        DB_FATAL(" _tso_state_machine init fail");
        return -1;
    }
    DB_WARNING("_tso_state_machine init success");

    SchemaManager::get_instance()->set_meta_state_machine(_meta_state_machine);
    PrivilegeManager::get_instance()->set_meta_state_machine(_meta_state_machine);
    ClusterManager::get_instance()->set_meta_state_machine(_meta_state_machine);
    MetaServerInteract::get_instance()->init();
#ifdef BAIDU_INTERNAL
    _meta_interact_map["e0"] = new MetaServerInteract;
    _meta_interact_map["e0"]->init_internal(FLAGS_e0_meta_bns);
    _meta_interact_map["holmes"] = new MetaServerInteract;
    _meta_interact_map["holmes"]->init_internal(FLAGS_holmes_meta_bns);
    _meta_interact_map["holmes_product"] = new MetaServerInteract;
    _meta_interact_map["holmes_product"]->init_internal(FLAGS_holmes_product_meta_bns);
    _meta_interact_map["ps"] = new MetaServerInteract;
    _meta_interact_map["ps"]->init_internal(FLAGS_ps_meta_bns);
    _meta_interact_map["dmp"] = new MetaServerInteract;
    _meta_interact_map["dmp"]->init_internal(FLAGS_dmp_meta_bns);
    _meta_interact_map["coffline"] = new MetaServerInteract;
    _meta_interact_map["coffline"]->init_internal(FLAGS_coffline_meta_bns);
    _meta_interact_map["detect"] = new MetaServerInteract;
    _meta_interact_map["detect"]->init_internal(FLAGS_detect_meta_bns);
    _meta_interact_map["pinpai"] = new MetaServerInteract;
    _meta_interact_map["pinpai"]->init_internal(FLAGS_pinpai_meta_bns);
    _meta_interact_map["aladdin"] = new MetaServerInteract;
    _meta_interact_map["aladdin"]->init_internal(FLAGS_aladdin_meta_bns);
    _meta_interact_map["qa"] = new MetaServerInteract;
    _meta_interact_map["qa"]->init_internal(FLAGS_qa_meta_bns);
#endif
    _flush_bth.run([this]() {flush_memtable_thread();});
    _apply_region_bth.run([this]() {apply_region_thread();});
    _init_success = true;
    return 0;
}

void MetaServer::apply_region_thread() {
    while (!_shutdown) {
        TableManager::get_instance()->get_update_regions_apply_raft();
        bthread_usleep_fast_shutdown(FLAGS_region_apply_raft_interval_ms * 1000, _shutdown);
    }
}

void MetaServer::flush_memtable_thread() {
    while (!_shutdown) {
        bthread_usleep_fast_shutdown(FLAGS_flush_memtable_interval_us, _shutdown);
        if (_shutdown) {
            return;
        }
        auto rocksdb = RocksWrapper::get_instance();
        rocksdb::FlushOptions flush_options;
        auto status = rocksdb->flush(flush_options, rocksdb->get_meta_info_handle());
        if (!status.ok()) {
            DB_WARNING("flush meta info to rocksdb fail, err_msg:%s", status.ToString().c_str());
        }
        status = rocksdb->flush(flush_options, rocksdb->get_raft_log_handle());
        if (!status.ok()) {
            DB_WARNING("flush log_cf to rocksdb fail, err_msg:%s", status.ToString().c_str());
        }
    }
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
            || request->op_type() == pb::OP_DROP_TABLE_TOMBSTONE
            || request->op_type() == pb::OP_RESTORE_TABLE 
            || request->op_type() == pb::OP_RENAME_TABLE
            || request->op_type() == pb::OP_ADD_FIELD
            || request->op_type() == pb::OP_DROP_FIELD
            || request->op_type() == pb::OP_RENAME_FIELD
            || request->op_type() == pb::OP_MODIFY_FIELD
            || request->op_type() == pb::OP_UPDATE_REGION
            || request->op_type() == pb::OP_DROP_REGION
            || request->op_type() == pb::OP_SPLIT_REGION
            || request->op_type() == pb::OP_MERGE_REGION
            || request->op_type() == pb::OP_UPDATE_BYTE_SIZE
            || request->op_type() == pb::OP_UPDATE_SPLIT_LINES
            || request->op_type() == pb::OP_UPDATE_SCHEMA_CONF
            || request->op_type() == pb::OP_UPDATE_DISTS
            || request->op_type() == pb::OP_UPDATE_TTL_DURATION
            || request->op_type() == pb::OP_UPDATE_STATISTICS
            || request->op_type() == pb::OP_MODIFY_RESOURCE_TAG
            || request->op_type() == pb::OP_ADD_INDEX
            || request->op_type() == pb::OP_DROP_INDEX
            || request->op_type() == pb::OP_DELETE_DDLWORK
            || request->op_type() == pb::OP_LINK_BINLOG
            || request->op_type() == pb::OP_UNLINK_BINLOG
            || request->op_type() == pb::OP_SET_INDEX_HINT_STATUS) {
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
    if (request->op_type() == pb::OP_SET_INSTANCE_MIGRATE) {
        ClusterManager::get_instance()->set_instance_migrate(request, response, log_id);
        return;
    }
    if (request->op_type() == pb::OP_SET_FULL) {
        ClusterManager::get_instance()->set_instance_full(request, response, log_id);
        return;
    }
    if (request->op_type() == pb::OP_SET_NO_FULL) {
        ClusterManager::get_instance()->set_instance_no_full(request, response, log_id);
        return;
    }
    if (request->op_type() == pb::OP_OPEN_LOAD_BALANCE) {
        response->set_errcode(pb::SUCCESS);
        response->set_op_type(request->op_type());
        if (request->resource_tags_size() == 0) {
            _meta_state_machine->set_global_load_balance(true);
            DB_WARNING("open global load balance");
            return;
        }
        for (auto& resource_tag : request->resource_tags()) {
            _meta_state_machine->set_load_balance(resource_tag, true);
            DB_WARNING("open load balance for resource_tag: %s", resource_tag.c_str());
        }
        return;
    }
    if (request->op_type() == pb::OP_CLOSE_LOAD_BALANCE) {
        response->set_errcode(pb::SUCCESS);
        response->set_op_type(request->op_type());
        if (request->resource_tags_size() == 0) {
            _meta_state_machine->set_global_load_balance(false);
            DB_WARNING("close global load balance");
            return;
        }
        for (auto& resource_tag : request->resource_tags()) {
            _meta_state_machine->set_load_balance(resource_tag, false);
            DB_WARNING("close load balance for resource_tag: %s", resource_tag.c_str());
        } 
        return;
    }
    if (request->op_type() == pb::OP_OPEN_MIGRATE) {
        response->set_errcode(pb::SUCCESS);
        response->set_op_type(request->op_type());
        if (request->resource_tags_size() == 0) {
            _meta_state_machine->set_global_migrate(true);
            DB_WARNING("open global migrate");
            return;
        }
        for (auto& resource_tag : request->resource_tags()) {
            _meta_state_machine->set_migrate(resource_tag, true);
            DB_WARNING("open migrate for resource_tag: %s", resource_tag.c_str());
        }
        return;
    }
    if (request->op_type() == pb::OP_CLOSE_MIGRATE) {
        response->set_errcode(pb::SUCCESS);
        response->set_op_type(request->op_type());
        if (request->resource_tags_size() == 0) {
            _meta_state_machine->set_global_migrate(false);
            DB_WARNING("close migrate");
            return;
        }
        for (auto& resource_tag : request->resource_tags()) {
            _meta_state_machine->set_migrate(resource_tag, false);
            DB_WARNING("close migrate for resource_tag: %s", resource_tag.c_str());
        } 
        return;
    }
    if (request->op_type() == pb::OP_OPEN_UNSAFE_DECISION) {
        _meta_state_machine->set_unsafe_decision(true);
        response->set_errcode(pb::SUCCESS);
        response->set_op_type(request->op_type());
        DB_WARNING("open unsafe decison");
        return;
    }
    if (request->op_type() == pb::OP_CLOSE_UNSAFE_DECISION) {
        _meta_state_machine->set_unsafe_decision(false);
        response->set_errcode(pb::SUCCESS);
        response->set_op_type(request->op_type());
        DB_WARNING("close unsafe decision");
        return;
    }
    if (request->op_type() == pb::OP_RESTORE_REGION) {
        response->set_errcode(pb::SUCCESS);
        response->set_op_type(request->op_type());
        RegionManager::get_instance()->restore_region(*request, response);
        return;
    }
    if (request->op_type() == pb::OP_RECOVERY_ALL_REGION) {
        response->set_errcode(pb::SUCCESS);
        response->set_op_type(request->op_type());
        RegionManager::get_instance()->recovery_all_region(*request, response);
        return;
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
    case pb::QUERY_DIFF_REGION_IDS: {
        QueryClusterManager::get_instance()->get_diff_region_ids(request, response);
        break;        
    }
    case pb::QUERY_REGION_IDS: {
        QueryClusterManager::get_instance()->get_region_ids(request, response);
        break;                           
    }
    case pb::QUERY_DDLWORK: {
        QueryTableManager::get_instance()->get_ddlwork_info(request, response);
        break;                           
    }
    case pb::QUERY_REGION_PEER_STATUS: {
        QueryRegionManager::get_instance()->get_region_peer_status(request, response);
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
    if (request->region_id() == 2) {
         _tso_state_machine->raft_control(controller, request, response, done_guard.release());
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

void MetaServer::baikal_other_heartbeat(google::protobuf::RpcController* controller,
                                  const pb::BaikalOtherHeartBeatRequest* request,
                                  pb::BaikalOtherHeartBeatResponse* response,
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
        _meta_state_machine->baikal_other_heartbeat(controller, request, response, done_guard.release());
    }
}

void MetaServer::console_heartbeat(google::protobuf::RpcController* controller,
                                  const pb::ConsoleHeartBeatRequest* request,
                                  pb::ConsoleHeartBeatResponse* response,
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
        _meta_state_machine->console_heartbeat(controller, request, response, done_guard.release());
    }
}

void MetaServer::tso_service(google::protobuf::RpcController* controller,
                                  const pb::TsoRequest* request,
                                  pb::TsoResponse* response,
                                  google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(controller);
    uint64_t log_id = 0;
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    RETURN_IF_NOT_INIT(_init_success, response, log_id);
    if (_tso_state_machine != nullptr) {
        _tso_state_machine->process(controller, request, response, done_guard.release());
    }
}

static std::string bns_to_plat(const std::string& bns) {
    static std::map<std::string, std::string> mapping = {
        {"e0", "e0"},
        {"holmes", "holmes"},
        {"hmkv", "holmes"},
        {"coffline", "coffline"},
        {"detect", "detect"},
        {"pinpai", "pinpai"},
        {"aladdin", "aladdin"},
        {"qa", "qa"},
    };
    std::vector<std::string> vec;
    boost::split(vec, bns, boost::is_any_of(".-"));
    // DMP产品线采用独立的meta
    if (vec.size() > 6 && vec[6] == "DMP") {
        return "dmp";
    } else if (vec.size() > 6 && vec[6] == "HOLMES") {
        return "holmes_product";
    } else if (vec.size() > 2) {
        if (mapping.count(vec[2]) == 1) {
            return mapping[vec[2]];
        }
    }
    return "ps";
}


void MetaServer::migrate(google::protobuf::RpcController* controller,
                                 const pb::MigrateRequest* /*request*/,
                                 pb::MigrateResponse* response,
                                 google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(controller);
    const std::string* data = cntl->http_request().uri().GetQuery("data");
    cntl->http_response().set_content_type("text/plain");
    if (!_init_success) {
        DB_WARNING("migrate have not init");
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        return;
    }
    pb::MigrateRequest request;
    DB_WARNING("start any_migrate");
    if (data != NULL) {
        std::string decode_data = url_decode(*data);
        DB_WARNING("start any_migrate %s %s", data->c_str(), decode_data.c_str());
        json2pb(decode_data, &request);
    }
    static std::map<std::string, std::string> bns_pre_ip_port;
    static std::mutex bns_mutex;
    for (auto& instance : request.targets_list().instances()) {
        std::string bns = instance.name();
        std::string plat = bns_to_plat(bns);
        std::string event = instance.event();
        auto res_instance = response->mutable_data()->mutable_targets_list()->add_instances();
        res_instance->set_name(bns);
        res_instance->set_status("PROCESSING");
        std::vector<std::string> bns_instances;
        int ret = 0;
        std::string ip_port;
        if (instance.has_pre_host() && instance.has_pre_port()) {
            ip_port = instance.pre_host() + ":" + instance.pre_port();
        } else {
            get_instance_from_bns(&ret, bns, bns_instances, false);
            if (bns_instances.size() != 1) {
                DB_WARNING("bns:%s must have 1 instance", bns.c_str());
                res_instance->set_status("PROCESSING");
                return;
            }
            ip_port = bns_instances[0];
        }
        if (event == "EXPECTED_MIGRATE") {
            DB_WARNING("bns: %s, plat: %s allowed", bns.c_str(), plat.c_str());
            pb::MetaManagerRequest internal_req;
            pb::MetaManagerResponse internal_res;
            internal_req.set_op_type(pb::OP_SET_INSTANCE_MIGRATE);
            internal_req.mutable_instance()->set_address(ip_port);
            ret = meta_proxy(plat)->send_request(
                    "meta_manager", internal_req, internal_res);
            if (ret != 0) {
                DB_WARNING("internal request fail, %s, %s", 
                        internal_req.ShortDebugString().c_str(), 
                        internal_res.ShortDebugString().c_str());
                res_instance->set_status("PROCESSING");
                return;
            }
            res_instance->set_status(internal_res.errmsg());
            BAIDU_SCOPED_LOCK(bns_mutex);
            bns_pre_ip_port[bns] = ip_port;
        } else if (event == "MIGRATED") {
            if (instance.pre_host() == instance.post_host()) {
                res_instance->set_status("SUCCESS");
                DB_WARNING("instance not migrate, request: %s", instance.ShortDebugString().c_str());
                DB_FATAL("bns: %s, plat: %s not migrate", bns.c_str(), plat.c_str());
                //return;
            }
            {
                BAIDU_SCOPED_LOCK(bns_mutex);
                if (bns != "" && bns_pre_ip_port.count(bns) == 1) {
                    ip_port = bns_pre_ip_port[bns];
                }
            }
            pb::MetaManagerRequest internal_req;
            pb::MetaManagerResponse internal_res;
            internal_req.set_op_type(pb::OP_DROP_INSTANCE);
            internal_req.mutable_instance()->set_address(ip_port);
            ret = meta_proxy(plat)->send_request(
                    "meta_manager", internal_req, internal_res);
            if (ret != 0) {
                DB_WARNING("internal request fail, %s, %s", 
                        internal_req.ShortDebugString().c_str(), 
                        internal_res.ShortDebugString().c_str());
                res_instance->set_status("PROCESSING");
                return;
            }
            res_instance->set_status("SUCCESS");
        }
    }
}

void MetaServer::shutdown_raft() {
    _shutdown = true;
    if (_meta_state_machine != nullptr) {
        _meta_state_machine->shutdown_raft();
    }   
    if (_auto_incr_state_machine != nullptr) {
        _auto_incr_state_machine->shutdown_raft();
    }
    if (_tso_state_machine != nullptr) {
        _tso_state_machine->shutdown_raft();
    }
}

bool MetaServer::have_data() {
    return _meta_state_machine->have_data() 
           && _auto_incr_state_machine->have_data()
           && _tso_state_machine->have_data();
}
}//namespace
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
