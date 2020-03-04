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

#include "console_server.h"
#include "console_util.h"

namespace baikaldb {

DEFINE_int32(console_port, 9527, "Console port");
DEFINE_string(platform_tags, "FENGCHAO", "platform tags");

int ConsoleServer::init() {
    int   rc = 0;
    std::vector<std::string> platform_tags;
    for (auto& platform_tag : string_split(FLAGS_platform_tags, ';')) {
        if (platform_tag.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890_") != std::string::npos) {
            DB_FATAL("platform_tag: %s is Invalid", platform_tag.c_str());
            return -1;
        }
        std::string tag = string_trim(platform_tag);
        std::transform(tag.begin(), tag.end(), tag.begin(), ::toupper);
        platform_tags.push_back(tag);
    }
 
    _baikaldb = BaikalProxy::get_instance();
    rc = _baikaldb->init(platform_tags);
    if (rc != 0) {
        DB_FATAL("baikal client init fail:%d", rc);
        return -1;
    }

    _meta_server = MetaProxy::get_instance(); 
    rc = _meta_server->init(_baikaldb, platform_tags);
    if (rc < 0)
    {
        DB_FATAL("mete server  init failed");
        return rc;
    }

    _status_op_type = {pb::WATCH_OVERVIEW, pb::WATCH_TABLE,
                       pb::WATCH_INSTANCE, pb::WATCH_REGION,
                       pb::WATCH_USER, pb::WATCH_NAMESPACE,
                       pb::WATCH_DATABASE, pb::WATCH_CLUSTER,
                       pb::WATCH_SCHEMA, pb::WATCH_PLATFORM};

    _init_success = true;
    return 0;
}

void ConsoleServer::stop() {
    if (_meta_server) {
        _meta_server->stop();
    }
}

ConsoleServer::~ConsoleServer() {}


int ConsoleServer::parser_param(brpc::Controller* controller,
         pb::QueryParam* param) {
    
    auto uri = controller->http_request().uri();
    const std::string* optype = uri.GetQuery("op_type");
    if (!optype || optype->size() == 0) {
        return -1;
    } 
    pb::WatchOpType optype_pb = pb::WATCH_UNKNOWN;
    if (!WatchOpType_Parse(*optype, &optype_pb)) {
        return -1;
    }
 
    param->set_optype(optype_pb);
    if (optype_pb == pb::WATCH_PLATFORM) {
        return 0;
    }

    const std::string* logical_room = nullptr;
    const std::string* physical_room = nullptr;
    const std::string* instance = nullptr;
    const std::string* resource_tag = nullptr;
    const std::string* table_name = nullptr;
    const std::string* start = nullptr;
    const std::string* limit = nullptr;
    const std::string* region_id = nullptr;
    const std::string* step = nullptr;
    const std::string* namespace_name = nullptr;
    const std::string* database = nullptr;
    const std::string* table_id = nullptr;
    const std::string* done_file = nullptr;
    const std::string* cluster_name = nullptr;
    const std::string* ago_days = nullptr;
    const std::string* interval_days = nullptr;
    const std::string* user_sql = nullptr;
    const std::string* modle = nullptr;
    const std::string* charset = nullptr;
    const std::string* platform = nullptr;

    platform = uri.GetQuery("platform");
    if (platform && platform->size() > 0) {
        param->set_platform(*platform);
        auto iter = _baikaldb->plat_databases_mapping.find(*platform);
        if (iter != _baikaldb->plat_databases_mapping.end()) {
            param->set_crud_database(iter->second);
        } else {
            DB_WARNING("wrong platform set %s", platform->c_str());
            return -1;
        }
    }

   if (_status_op_type.count(optype_pb) > 0 && !param->has_platform()) {
        DB_WARNING("optype:%s need platform set", optype->c_str());
        return -1;          
   }

    logical_room = uri.GetQuery("logical_room");
    if (logical_room && logical_room->size() > 0) {
        param->set_logical_room(*logical_room);
    }
    physical_room = uri.GetQuery("physical_room");
    if (physical_room && physical_room->size() > 0) {
        param->set_physical_room(*physical_room);
    }
    instance = uri.GetQuery("address");
    if (instance && instance->size() > 0) {
        param->set_instance(*instance);
    }
    resource_tag = uri.GetQuery("resource_tag");
    if (resource_tag && resource_tag->size() > 0) {
        param->set_resource_tag(*resource_tag);
    }
    table_id = uri.GetQuery("table_id");
    if (table_id && table_id->size() > 0 && is_digits(*table_id)) {
        param->set_table_id(*table_id);
    }
    start = uri.GetQuery("start");
    if (start && start->size() > 0 && is_digits(*start)) {
        param->set_start(*start);
    }
    limit = uri.GetQuery("limit");
    if (limit && limit->size() > 0 && is_digits(*limit)) {
        param->set_limit(*limit);
    }
    database = uri.GetQuery("database");
    if (database && database->size() > 0) {
        param->set_database(*database);
    }
    namespace_name = uri.GetQuery("namespace_name");
    if (namespace_name && namespace_name->size() > 0) {
        param->set_namespace_name(*namespace_name);
    }
    table_name = uri.GetQuery("table_name");
    if (table_name && table_name->size() > 0) {
        param->set_table_name(*table_name);
    }
    region_id = uri.GetQuery("region_id");
    if (region_id && region_id->size() > 0 && is_digits(*region_id)) {
        param->set_region_id(*region_id);
    }
    step = uri.GetQuery("step");
    if (step && step->size() > 0) {
        param->set_step(*step);
    }
    done_file = uri.GetQuery("done_file");
    if (done_file && done_file->size() > 0) {
        param->set_done_file(*done_file);
    }
    cluster_name = uri.GetQuery("cluster_name");
    if (cluster_name && cluster_name->size() > 0) {
        param->set_cluster_name(*cluster_name);
    }
    ago_days = uri.GetQuery("ago_days");
    if (ago_days && ago_days->size() > 0 && is_digits(*ago_days)) {
        param->set_ago_days(*ago_days);
    }
    interval_days = uri.GetQuery("interval_days");
    if (interval_days && interval_days->size() > 0 && is_digits(*interval_days)) {
        param->set_interval_days(*interval_days);
    }
    user_sql = uri.GetQuery("user_sql");
    if (user_sql && user_sql->size() > 0) {
        param->set_user_sql(*user_sql);
    }
    modle = uri.GetQuery("modle");
    if (modle && modle->size() > 0) {
        param->set_modle(*modle);
    }
    charset = uri.GetQuery("charset");
    if (charset && charset->size() > 0) {
        param->set_charset(*charset);
    }
   
    // query schemainfo need databse+namespace_name+table_name or table_id
    if (optype_pb == pb::WATCH_SCHEMA) {
         if ((!param->has_database()
             || !param->has_namespace_name()
             || !param->has_table_name())
             && !param->has_table_id()) {
            return -1;
        }
    }

    if (param->has_step() && !param->has_region_id() && !param->has_instance()) {
        return -1;
    }
    return 0;
}

void ConsoleServer::watch(google::protobuf::RpcController* controller,
           const pb::ConsoleRequest* request,
           pb::ConsoleResponse* response,
           google::protobuf::Closure* done) {

    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    uint64_t log_id = 0;
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    RETURN_IF_NOT_INIT(_init_success, response, log_id);
    pb::QueryParam param;
    int ret = parser_param(cntl, &param);
    if (ret < 0) {
        ERROR_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, "param is invalid", pb::WATCH_UNKNOWN, log_id);
        return ;
    }
    
    std::string message = "success";
    bool success = true;
    cntl->http_response().set_content_type("text/plain");
    DB_WARNING("query param:%s, request: %s", 
                param.ShortDebugString().c_str(), request->ShortDebugString().c_str());
    
    switch (param.optype()){
        case pb::WATCH_PLATFORM:
            if (_baikaldb->get_platform_info(&param, request, response) != 0) {
                success = false;
            }
            //DB_NOTICE("req:%s  \nres:%s", request->DebugString().c_str(), 
            //    response->DebugString().c_str());
            break;           
        case pb::WATCH_OVERVIEW:
            if (_baikaldb->get_overview_info(&param, request, response) != 0) {
                success = false;
            }
            //DB_NOTICE("req:%s  \nres:%s", request->DebugString().c_str(), 
            //    response->DebugString().c_str());
            break;
        case pb::WATCH_TABLE:
            if (_baikaldb->get_table_info(&param, request, response) != 0) {
                success = false;
            }
            //DB_NOTICE("req:%s  \nres:%s", request->DebugString().c_str(), 
            //    response->DebugString().c_str());
            break;
        case pb::WATCH_DATABASE:
            if (_baikaldb->get_database_info(&param, request, response) != 0) {
                message = "namespace needed";
                success = false;
            }
            //DB_NOTICE("req:%s  \nres:%s", request->DebugString().c_str(), 
            //    response->DebugString().c_str());
            break;
        case pb::WATCH_INSTANCE:
            if (_baikaldb->get_instance_info(&param, request, response) < 0) {
                message = "instance not exist";
                success = false;
            }
            //DB_NOTICE("req:%s  \nres:%s", request->DebugString().c_str(), 
            //    response->DebugString().c_str());
            break;
        case pb::WATCH_REGION:
            if (_baikaldb->get_region_info(&param, request, response) < 0) {
                message = "region not exist";
                success = false;
            }
            //DB_NOTICE("req:%s  \nres:%s", request->DebugString().c_str(), 
            //    response->DebugString().c_str());
            break;
        case pb::WATCH_USER:
            if (_baikaldb->get_user_info(&param, request, response) < 0) {
                message = "user not exist";
                success = false;
            }
            //DB_NOTICE("req:%s  \nres:%s", request->DebugString().c_str(), 
            //    response->DebugString().c_str());
            break;
        case pb::WATCH_SCHEMA:
            if (_meta_server->query_schema_info(&param, response) < 0) {
                message = "server error";
                success = false;
            }
            //DB_NOTICE("req:%s  \nres:%s", request->DebugString().c_str(), 
            //    response->DebugString().c_str());
            break;
        case pb::WATCH_NAMESPACE:
            if (_baikaldb->get_namespace_info(&param, request, response) < 0) {
                message = "server error";
                success = false;
            }
            //DB_NOTICE("req:%s  \nres:%s", request->DebugString().c_str(), 
            //    response->DebugString().c_str());
            break;
        case pb::WATCH_CLUSTER:
            if (_baikaldb->get_cluster_info(&param, request, response) < 0) {
                message = "server error";
                success = false;
            }
            //DB_NOTICE("req:%s  \nres:%s", request->DebugString().c_str(), 
            //    response->DebugString().c_str());
            break;
        case pb::WATCH_IMPORTTASK:
            if (_baikaldb->get_import_task(&param, request, response) < 0) {
                message = "server error";
                success = false;
            }
            DB_NOTICE("req:%s  \nres:%s", request->DebugString().c_str(), 
                response->DebugString().c_str());
            break;
        case pb::WATCH_IMPORTTASK2:
            if (_baikaldb->get_import_task2(&param, request, response) < 0) {
                message = "server error";
                success = false;
            }
            DB_NOTICE("req:%s  \nres:%s", request->DebugString().c_str(), 
                response->DebugString().c_str());
            break;
        case pb::WATCH_TASKLIST:
            if (_baikaldb->get_task_list_by_database(&param, request, response) < 0) {
                message = "server error";
                success = false;
            }
            DB_NOTICE("req:%s  \nres:%s", request->DebugString().c_str(), 
                response->DebugString().c_str());
            break;
        case pb::SUBMIT_IMPORTTASK:
            if (_baikaldb->submit_import_task(&param, request, response) < 0) {
                message = "server error";
                success = false;
            }
            DB_NOTICE("req:%s  \nres:%s", request->DebugString().c_str(), 
                response->DebugString().c_str());
            break;
        default:
            DB_FATAL("request has wrong op_type:%d , log_id:%lu", 
                    request->op_type(), log_id);
            message = "invalid op_type";
            success = false;
    }

    if (success) {
        IF_DONE_SET_RESPONSE(response, pb::SUCCESS, "success");
    } else {
        IF_DONE_SET_RESPONSE(response, pb::INPUT_PARAM_ERROR, message);
    }
}

} //namespace
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
