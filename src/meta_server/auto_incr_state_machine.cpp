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

#include "auto_incr_state_machine.h"
#include <fstream>
#include <boost/lexical_cast.hpp>
#include "rapidjson/rapidjson.h"
#include "rapidjson/reader.h"
#include "rapidjson/writer.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h" // for stringify JSON
#ifdef BAIDU_INTERNAL
#include "raft/util.h"
#include "raft/storage.h"
#else
#include <braft/util.h>
#include <braft/storage.h>
#endif
#include "meta_util.h"

namespace baikaldb {
void AutoIncrStateMachine::on_apply(braft::Iterator& iter) {
    for (; iter.valid(); iter.next()) {
        braft::Closure* done = iter.done();
        brpc::ClosureGuard done_guard(done);
        if (done) {
            ((MetaServerClosure*)done)->raft_time_cost = ((MetaServerClosure*)done)->time_cost.get_time();
        }
        butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
        pb::MetaManagerRequest request;
        if (!request.ParseFromZeroCopyStream(&wrapper)) {
            DB_FATAL("parse from protobuf fail when on_apply");
            if (done) {
                if (((MetaServerClosure*)done)->response) {
                    ((MetaServerClosure*)done)->response->set_errcode(pb::PARSE_FROM_PB_FAIL);
                    ((MetaServerClosure*)done)->response->set_errmsg("parse from protobuf fail");
                }
                braft::run_closure_in_bthread(done_guard.release());
            }
            continue;
        }
        if (done && ((MetaServerClosure*)done)->response) {
            ((MetaServerClosure*)done)->response->set_op_type(request.op_type());
        }
        DB_DEBUG("on applye, term:%ld, index:%ld, request op_type:%s", 
                    iter.term(), iter.index(), 
                    pb::OpType_Name(request.op_type()).c_str());
        switch (request.op_type()) {
        case pb::OP_ADD_ID_FOR_AUTO_INCREMENT: {
            add_table_id(request, done);
            break;                                       
        }
        case pb::OP_DROP_ID_FOR_AUTO_INCREMENT: {
            drop_table_id(request, done);
            break;                              
        }
        case pb::OP_GEN_ID_FOR_AUTO_INCREMENT: {
            gen_id(request, done);
            break;
        }
        case pb::OP_UPDATE_FOR_AUTO_INCREMENT: {
            update(request, done);
            break;
        }
        default: {
            DB_FATAL("unsupport request type, type:%d", request.op_type());
            IF_DONE_SET_RESPONSE(done, pb::UNSUPPORT_REQ_TYPE, "unsupport request type");
        }
        }
        if (done) {
            braft::run_closure_in_bthread(done_guard.release());
        }
    }
}
void AutoIncrStateMachine::add_table_id(const pb::MetaManagerRequest& request, 
        braft::Closure* done) {
    auto& increment_info = request.auto_increment();
    int64_t table_id = increment_info.table_id();
    uint64_t start_id = increment_info.start_id();
    if (_auto_increment_map.find(table_id) != _auto_increment_map.end()) {
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table id has exist");
        DB_FATAL("table_id: %ld has exist when add table id for auto increment", table_id);
        return;
    }
    _auto_increment_map[table_id] = start_id;
    if (done && ((MetaServerClosure*)done)->response) { 
        ((MetaServerClosure*)done)->response->set_errcode(pb::SUCCESS);
        ((MetaServerClosure*)done)->response->set_op_type(request.op_type());
        ((MetaServerClosure*)done)->response->set_start_id(start_id);
        ((MetaServerClosure*)done)->response->set_errmsg("SUCCESS");
    }
    DB_NOTICE("add table id for auto_increment success, request:%s", 
        request.ShortDebugString().c_str());
}
void AutoIncrStateMachine::drop_table_id(const pb::MetaManagerRequest& request,
        braft::Closure* done) {
    auto& increment_info = request.auto_increment();
    int64_t table_id = increment_info.table_id();
    if (_auto_increment_map.find(table_id) == _auto_increment_map.end()) {
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table id not exist");
        DB_WARNING("table_id: %ld not exist when drop table id for auto increment", table_id);
        return;
    }
    _auto_increment_map.erase(table_id);
    if (done && ((MetaServerClosure*)done)->response) { 
        ((MetaServerClosure*)done)->response->set_errcode(pb::SUCCESS);
        ((MetaServerClosure*)done)->response->set_op_type(request.op_type());
        ((MetaServerClosure*)done)->response->set_errmsg("SUCCESS");
    }
    DB_NOTICE("drop table id for auto_increment success, request:%s", 
        request.ShortDebugString().c_str());
}
void AutoIncrStateMachine::gen_id(const pb::MetaManagerRequest& request,
            braft::Closure* done) {
    auto& increment_info = request.auto_increment();
    int64_t table_id = increment_info.table_id();
    if (_auto_increment_map.find(table_id) == _auto_increment_map.end()) {
        DB_WARNING("table id:%ld has no auto_increment field", table_id);
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table has no auto increment");
        return;
    }    
    uint64_t old_start_id = _auto_increment_map[table_id];
    if (increment_info.has_start_id() && old_start_id < increment_info.start_id() + 1) { 
        old_start_id = increment_info.start_id() + 1; 
    }    
    _auto_increment_map[table_id] = old_start_id + increment_info.count();
    if (done && ((MetaServerClosure*)done)->response) {
        ((MetaServerClosure*)done)->response->set_errcode(pb::SUCCESS);
        ((MetaServerClosure*)done)->response->set_op_type(request.op_type());
        ((MetaServerClosure*)done)->response->set_start_id(old_start_id);
        ((MetaServerClosure*)done)->response->set_end_id(_auto_increment_map[table_id]);
        ((MetaServerClosure*)done)->response->set_errmsg("SUCCESS");
    }
    DB_DEBUG("gen_id for auto_increment success, request:%s", 
                request.ShortDebugString().c_str());
}
void AutoIncrStateMachine::update(const pb::MetaManagerRequest& request,
        braft::Closure* done) {
    auto& increment_info = request.auto_increment();
    int64_t table_id = increment_info.table_id();
    if (_auto_increment_map.find(table_id) == _auto_increment_map.end()) {
        DB_WARNING("table id:%ld has no auto_increment field", table_id);
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table has no auto increment");
        return;
    }
    if (!increment_info.has_start_id() && !increment_info.has_increment_id()) {
        DB_WARNING("star_id or increment_id all not exist, table_id:%ld", table_id);
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR,
                 "star_id or increment_id all not exist");
        return;
    }
    if (increment_info.has_start_id() && increment_info.has_increment_id()) {
        DB_WARNING("star_id and increment_id all exist, table_id:%ld", table_id);
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR,
                "star_id and increment_id all exist");
        return;
    }
    uint64_t old_start_id = _auto_increment_map[table_id];
    //请求要求回退
    if (increment_info.has_start_id()
            && old_start_id > increment_info.start_id() + 1
            && (!increment_info.has_force() || increment_info.force() == false)) {
        DB_WARNING("request not ilegal, max_id not support back, table_id:%ld", table_id);
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "not support rollbak");
        return;
    }
    if (increment_info.has_start_id()) {
        _auto_increment_map[table_id] = increment_info.start_id() + 1;
    } else {
        _auto_increment_map[table_id] += increment_info.increment_id();
    }
    if (done && ((MetaServerClosure*)done)->response) {
        ((MetaServerClosure*)done)->response->set_errcode(pb::SUCCESS);
        ((MetaServerClosure*)done)->response->set_op_type(request.op_type());
        ((MetaServerClosure*)done)->response->set_start_id(_auto_increment_map[table_id]);
        ((MetaServerClosure*)done)->response->set_errmsg("SUCCESS");
    }
    DB_NOTICE("update start_id for auto_increment success, request:%s", 
                request.ShortDebugString().c_str());
}

void AutoIncrStateMachine::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
    DB_WARNING("start on snapshot save");
    std::string max_id_string;
    save_auto_increment(max_id_string);
    Bthread bth(&BTHREAD_ATTR_SMALL);
    std::function<void()> save_snapshot_function = [this, done, writer, max_id_string]() {
            save_snapshot(done, writer, max_id_string);
        };
    bth.run(save_snapshot_function);
}

int AutoIncrStateMachine::on_snapshot_load(braft::SnapshotReader* reader) {
    DB_WARNING("start on snapshot load");
    std::vector<std::string> files;
    reader->list_files(&files);
    for (auto& file : files) {
        DB_WARNING("snapshot load file:%s", file.c_str());
        if (file == "/max_id.json") {
            std::string max_id_file = reader->get_path() + "/max_id.json";
            if (load_auto_increment(max_id_file) != 0) {
                DB_WARNING("load auto increment max_id fail");
                return -1;
            }
        }
    }
    set_have_data(true);
    return 0;
}

void AutoIncrStateMachine::save_auto_increment(std::string& max_id_string) {
    rapidjson::Document root;
    root.SetObject();
    rapidjson::Document::AllocatorType& alloc = root.GetAllocator();
    for (auto& max_id_pair : _auto_increment_map) {
        std::string table_id_string = std::to_string(max_id_pair.first);
        rapidjson::Value table_id_val(rapidjson::kStringType);
        table_id_val.SetString(table_id_string.c_str(), table_id_string.size(), alloc);
      
        rapidjson::Value max_id_value(rapidjson::kNumberType);
        max_id_value.SetUint64(max_id_pair.second);
      
        root.AddMember(table_id_val, max_id_value, alloc);
    }    
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> json_writer(buffer);
    root.Accept(json_writer);
    max_id_string = buffer.GetString();
    DB_WARNING("max id string:%s when snapshot", max_id_string.c_str());
}

void AutoIncrStateMachine::save_snapshot(braft::Closure* done,
                                    braft::SnapshotWriter* writer,
                                    std::string max_id_string) {
    brpc::ClosureGuard done_guard(done);
    std::string snapshot_path = writer->get_path();
    std::string max_id_path = snapshot_path + "/max_id.json";
    std::ofstream extra_fs(max_id_path,
            std::ofstream::out | std::ofstream::trunc);
    extra_fs.write(max_id_string.data(), max_id_string.size());
    extra_fs.close();
    if (writer->add_file("/max_id.json") != 0) {
        done->status().set_error(EINVAL, "Fail to add file");
        DB_WARNING("Error while adding file to writer");
        return;
    }
}

int AutoIncrStateMachine::load_auto_increment(const std::string& max_id_file) {
    _auto_increment_map.clear();
    std::ifstream extra_fs(max_id_file);
    std::string extra((std::istreambuf_iterator<char>(extra_fs)),
            std::istreambuf_iterator<char>());
    return parse_json_string(extra);
}

int AutoIncrStateMachine::parse_json_string(const std::string& json_string) {
    rapidjson::Document root;
    try {
        root.Parse<0>(json_string.c_str());
        if (root.HasParseError()) {
            rapidjson::ParseErrorCode code = root.GetParseError();
            DB_WARNING("parse extra file error [code:%d][%s]", code, json_string.c_str());
            return -1;
         }    
    } catch (...) {
        DB_WARNING("parse extra file error [%s]", json_string.c_str());
        return -1;
    } 
    for (auto json_iter = root.MemberBegin(); json_iter != root.MemberEnd(); ++json_iter) {
        int64_t table_id = boost::lexical_cast<int64_t>(json_iter->name.GetString());
        uint64_t max_id = json_iter->value.GetUint64();
        DB_WARNING("load auto increment, table_id:%ld, max_id:%lu", table_id, max_id);
        _auto_increment_map[table_id] = max_id;  
    }    
    return 0;
}
}//namespace
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
