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

#include "tso_state_machine.h"
#include "meta_util.h"
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

namespace baikaldb {
DECLARE_int32(election_timeout_ms);
DECLARE_string(log_uri);
DECLARE_string(stable_uri);
DECLARE_string(snapshot_uri);
DEFINE_int32(tso_snapshot_interval_s, 60, "tso raft snapshot interval(s)");

int TsoTimer::init(TSOStateMachine* node, int timeout_ms) {
    int ret = RepeatedTimerTask::init(timeout_ms);
    _node = node;
    return ret;
}

void TsoTimer::run() {
    _node->update_timestamp();
}


const std::string TSOStateMachine::SNAPSHOT_TSO_FILE              = "tso.file";
const std::string TSOStateMachine::SNAPSHOT_TSO_FILE_WITH_SLASH   = "/" + SNAPSHOT_TSO_FILE;

int TSOStateMachine::init(const std::vector<braft::PeerId>& peers) {
    _tso_update_timer.init(this, tso::update_timestamp_interval_ms);
    _tso_obj.current_timestamp.set_physical(0);
    _tso_obj.current_timestamp.set_logical(0);
    _tso_obj.last_save_physical = 0;
    //int ret = CommonStateMachine::init(peers);
    braft::NodeOptions options;
    options.election_timeout_ms = FLAGS_election_timeout_ms; 
    options.fsm = this;                                                               
    options.initial_conf = braft::Configuration(peers);                                    
    options.snapshot_interval_s = FLAGS_tso_snapshot_interval_s;
    options.log_uri = FLAGS_log_uri + std::to_string(_dummy_region_id);
    //options.stable_uri = FLAGS_stable_uri + "/meta_server";
#ifdef BAIDU_INTERNAL
    options.stable_uri = FLAGS_stable_uri + _file_path;                      
#else
    options.raft_meta_uri = FLAGS_stable_uri + _file_path;                      
#endif
    options.snapshot_uri = FLAGS_snapshot_uri + _file_path;              
    int ret = _node.init(options);                                                        
    if (ret < 0) {                                                                    
        DB_FATAL("raft node init fail");
        return ret;                
    }
    DB_WARNING("raft init success, meat state machine init success");
    return 0;
}

void TSOStateMachine::gen_tso(const pb::TsoRequest* request, pb::TsoResponse* response) {
    int64_t count = request->count();
    response->set_op_type(request->op_type());
    if (count == 0) {
        response->set_errcode(pb::INPUT_PARAM_ERROR);
        response->set_errmsg("tso count should be positive");
        return;
    }
    if (!_is_healty) {
        DB_FATAL("TSO has wrong status, retry later");
        response->set_errcode(pb::RETRY_LATER);
        response->set_errmsg("timestamp not ok, retry later");
        return;          
    }
    pb::TsoTimestamp current;
    bool need_retry = false;
    for (size_t i = 0; i < 50; i++) {
        {
            BAIDU_SCOPED_LOCK(_tso_mutex);
            int64_t physical = _tso_obj.current_timestamp.physical();
            if (physical != 0) {  
                int64_t new_logical = _tso_obj.current_timestamp.logical() + count;
                if (new_logical < tso::max_logical) {
                    current.CopyFrom(_tso_obj.current_timestamp);
                    _tso_obj.current_timestamp.set_logical(new_logical);
                    need_retry = false;
                } else {
                    // TODO 需要等50ms，physical是精确到1ms，这样会浪费50倍的buffer
                    // 可以选择physical增加1ms来利用，需要限制到上次update的50ms内
                    DB_WARNING("logical part outside of max logical interval, retry later, please check ntp time");
                    need_retry = true;
                }
            } else {
                DB_WARNING("timestamp not ok physical == 0, retry later");
                need_retry = true;
            }
        }
        if (!need_retry) {
            break;
        } else {
            bthread_usleep(tso::update_timestamp_interval_ms * 1000LL);
        }
    }
    if (need_retry) {
        response->set_errcode(pb::EXEC_FAIL);
        response->set_errmsg("gen tso failed");
        DB_FATAL("gen tso failed");
        return;
    }
    //DB_WARNING("gen tso current: (%ld, %ld)", current.physical(), current.logical());
    auto timestamp = response->mutable_start_timestamp();
    timestamp->CopyFrom(current);
    response->set_count(count);
    response->set_errcode(pb::SUCCESS);
}

void TSOStateMachine::process(google::protobuf::RpcController* controller,
                               const pb::TsoRequest* request,
                               pb::TsoResponse* response,
                               google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (request->op_type() == pb::OP_QUERY_TSO_INFO) {
        response->set_errcode(pb::SUCCESS);
        response->set_errmsg("success");
        response->set_op_type(request->op_type());
        response->set_leader(butil::endpoint2str(_node.leader_id().addr).c_str());
        response->set_system_time(tso::clock_realtime_ms());
        response->set_save_physical(_tso_obj.last_save_physical);
        auto timestamp = response->mutable_start_timestamp();
        timestamp->CopyFrom(_tso_obj.current_timestamp);
        return;
    }
    brpc::Controller* cntl = (brpc::Controller*)controller;
    uint64_t log_id = 0;
    if (cntl->has_log_id()) { 
        log_id = cntl->log_id();
    }
    const auto& remote_side_tmp = butil::endpoint2str(cntl->remote_side());
    const char* remote_side = remote_side_tmp.c_str();
    if (!_is_leader) {
        response->set_errcode(pb::NOT_LEADER);
        response->set_errmsg("not leader");
        response->set_op_type(request->op_type());
        response->set_leader(butil::endpoint2str(_node.leader_id().addr).c_str());
        DB_WARNING("state machine not leader, request: %s remote_side:%s log_id:%lu",
            request->ShortDebugString().c_str(), remote_side, log_id);
        return;
    }
    // 获取时间戳在raft外执行
    if (request->op_type() == pb::OP_GEN_TSO) {
        gen_tso(request, response);
        return;
    }
    butil::IOBuf data;
    butil::IOBufAsZeroCopyOutputStream wrapper(&data);
    if (!request->SerializeToZeroCopyStream(&wrapper)) {
        cntl->SetFailed(brpc::EREQUEST, "Fail to serialize request");
        return;
    }
    TsoClosure* closure = new TsoClosure;
    closure->cntl = cntl;
    closure->response = response;
    closure->done = done_guard.release();
    closure->common_state_machine = this;
    braft::Task task;
    task.data = &data;
    task.done = closure;
    _node.apply(task);
}

void TSOStateMachine::on_apply(braft::Iterator& iter) {
    for (; iter.valid(); iter.next()) {
        braft::Closure* done = iter.done();
        brpc::ClosureGuard done_guard(done);
        if (done) {
            ((TsoClosure*)done)->raft_time_cost = ((TsoClosure*)done)->time_cost.get_time();
        }
        butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
        pb::TsoRequest request;
        if (!request.ParseFromZeroCopyStream(&wrapper)) {
            DB_FATAL("parse from protobuf fail when on_apply");
            if (done) {
                if (((TsoClosure*)done)->response) {
                    ((TsoClosure*)done)->response->set_errcode(pb::PARSE_FROM_PB_FAIL);
                    ((TsoClosure*)done)->response->set_errmsg("parse from protobuf fail");
                }
                braft::run_closure_in_bthread(done_guard.release());
            }
            continue;
        }
        if (done && ((TsoClosure*)done)->response) {
            ((TsoClosure*)done)->response->set_op_type(request.op_type());
        }
        switch (request.op_type()) {
        case pb::OP_RESET_TSO: {
            reset_tso(request, done);
            break;                              
        }
        case pb::OP_UPDATE_TSO: {
            update_tso(request, done);
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

void TSOStateMachine::reset_tso(const pb::TsoRequest& request,
        braft::Closure* done) {
    if (request.has_current_timestamp() && request.has_save_physical()) {
        int64_t physical = request.save_physical();
        pb::TsoTimestamp current = request.current_timestamp();
        if (physical < _tso_obj.last_save_physical
            || current.physical() < _tso_obj.current_timestamp.physical()) {
            if (!request.force()) {
                DB_WARNING("time fallback save_physical:(%ld, %ld) current:(%ld, %ld, %ld, %ld)",
                    physical, _tso_obj.last_save_physical, current.physical(), _tso_obj.current_timestamp.physical(),
                    current.logical(), _tso_obj.current_timestamp.logical());
                if (done && ((TsoClosure*)done)->response) {
                    pb::TsoResponse* response = ((TsoClosure*)done)->response;
                    response->set_errcode(pb::INTERNAL_ERROR);
                    response->set_errmsg("time can't fallback");
                    auto timestamp = response->mutable_start_timestamp();
                    timestamp->CopyFrom(_tso_obj.current_timestamp);
                    response->set_save_physical(_tso_obj.last_save_physical);
                }
                return;
            }
        }
        _is_healty = true;
        DB_WARNING("reset tso save_physical: %ld current: (%ld, %ld)", physical, current.physical(), current.logical());
        {
            BAIDU_SCOPED_LOCK(_tso_mutex);
            _tso_obj.last_save_physical = physical;
            _tso_obj.current_timestamp.CopyFrom(current);
        }
        if (done && ((TsoClosure*)done)->response) {
            pb::TsoResponse* response = ((TsoClosure*)done)->response;
            response->set_save_physical(physical);
            auto timestamp = response->mutable_start_timestamp();
            timestamp->CopyFrom(current);
            response->set_errcode(pb::SUCCESS);
            response->set_errmsg("SUCCESS");
        }      
    }
}

void TSOStateMachine::update_tso(const pb::TsoRequest& request,
        braft::Closure* done) {
    int64_t physical = request.save_physical();
    pb::TsoTimestamp current = request.current_timestamp();
    // 不能回退
    if (physical < _tso_obj.last_save_physical
        || current.physical() < _tso_obj.current_timestamp.physical()) {
            DB_WARNING("time fallback save_physical:(%ld, %ld) current:(%ld, %ld, %ld, %ld)",
            physical, _tso_obj.last_save_physical, current.physical(), _tso_obj.current_timestamp.physical(),
            current.logical(), _tso_obj.current_timestamp.logical());
        if (done && ((TsoClosure*)done)->response) {
            pb::TsoResponse* response = ((TsoClosure*)done)->response;
            response->set_errcode(pb::INTERNAL_ERROR);
            response->set_errmsg("time can't fallback");
        }
        return;
    }
    {
        BAIDU_SCOPED_LOCK(_tso_mutex);
        _tso_obj.last_save_physical = physical;
        _tso_obj.current_timestamp.CopyFrom(current);
    }

    if (done && ((TsoClosure*)done)->response) {
        pb::TsoResponse* response = ((TsoClosure*)done)->response;
        response->set_errcode(pb::SUCCESS);
        response->set_errmsg("SUCCESS");
    }
}


int TSOStateMachine::sync_timestamp(const pb::TsoTimestamp& current_timestamp, int64_t save_physical) {
    pb::TsoRequest request;
    pb::TsoResponse response;
    request.set_op_type(pb::OP_UPDATE_TSO);
    auto timestamp = request.mutable_current_timestamp();
    timestamp->CopyFrom(current_timestamp);
    request.set_save_physical(save_physical);
    butil::IOBuf data;
    butil::IOBufAsZeroCopyOutputStream wrapper(&data);
    if (!request.SerializeToZeroCopyStream(&wrapper)) {
        DB_WARNING("Fail to serialize request");
        return -1;
    }
    BthreadCond sync_cond;
    TsoClosure* c = new TsoClosure(&sync_cond);
    c->response = &response;
    c->done = nullptr;
    c->common_state_machine = this;
    sync_cond.increase();
    braft::Task task;
    task.data = &data;
    task.done = c;
    _node.apply(task);
    sync_cond.wait();
    if (response.errcode() != pb::SUCCESS) {
        DB_FATAL("sync timestamp failed, request:%s response:%s",
            request.ShortDebugString().c_str(), response.ShortDebugString().c_str());
        return -1;
    }
    return 0;
}

void TSOStateMachine::update_timestamp() {
    if (!_is_leader) {
        return;
    }
    int64_t now = tso::clock_realtime_ms();
    int64_t prev_physical = 0;
    int64_t prev_logical = 0;
    int64_t last_save = 0;
    {
        BAIDU_SCOPED_LOCK(_tso_mutex);
        prev_physical = _tso_obj.current_timestamp.physical();
        prev_logical  = _tso_obj.current_timestamp.logical();
        last_save     = _tso_obj.last_save_physical;
    }
    int64_t delta = now - prev_physical;
    if (delta < 0) {
        DB_WARNING("physical time slow now:%ld prev:%ld", now, prev_physical);
    }
    int64_t next = now;
    if (delta > tso::update_timestamp_guard_ms) {
        next = now;
    } else if (prev_logical > tso::max_logical / 2) {
        next = prev_physical + tso::update_timestamp_guard_ms;
    } else {
        DB_WARNING("don't need update timestamp prev:%ld now:%ld save:%ld", prev_physical, now, last_save);
        return;
    }
    int64_t save = last_save;
    if (save - next <= tso::update_timestamp_guard_ms) {
        save =  next + tso::save_interval_ms;
    }
    pb::TsoTimestamp tp;
    tp.set_physical(next);
    tp.set_logical(0);
    sync_timestamp(tp, save);
}

void TSOStateMachine::on_leader_start() {
    start_check_bns();
    DB_WARNING("tso leader start");
    int64_t now = tso::clock_realtime_ms();
    pb::TsoTimestamp current;
    current.set_physical(now);
    current.set_logical(0);
    int64_t last_save = _tso_obj.last_save_physical;
    if (now < last_save + tso::update_timestamp_guard_ms) {
        DB_WARNING("time maybe fallback, now:%ld, last_save:%ld", now, last_save);
        current.set_physical(last_save + tso::update_timestamp_guard_ms);
    }
    last_save = current.physical() + tso::save_interval_ms;
    auto func = [this, last_save, current]() {
        DB_WARNING("leader_start current(phy:%ld,log:%ld) save:%ld", current.physical(),
        current.logical(), last_save);
        int ret = sync_timestamp(current, last_save);
        if (ret < 0) {
            _is_healty = false;
        }
        DB_WARNING("sync timestamp ok");
        _is_leader.store(true);
        _tso_update_timer.start();
    };
    Bthread bth;
    bth.run(func);
}

void TSOStateMachine::on_leader_stop() {
    _tso_update_timer.stop();
    DB_WARNING("leader stop");
    CommonStateMachine::on_leader_stop();
}

void TSOStateMachine::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
    DB_WARNING("start on snapshot save");
    std::string sto_str = std::to_string(_tso_obj.last_save_physical);
    Bthread bth(&BTHREAD_ATTR_SMALL);
    std::function<void()> save_snapshot_function = [this, done, writer, sto_str]() {
            save_snapshot(done, writer, sto_str);
        };
    bth.run(save_snapshot_function);
}

void TSOStateMachine::save_snapshot(braft::Closure* done,
                                    braft::SnapshotWriter* writer,
                                    std::string sto_str) {
    brpc::ClosureGuard done_guard(done);
    std::string snapshot_path = writer->get_path();
    std::string save_path = snapshot_path + SNAPSHOT_TSO_FILE_WITH_SLASH;
    std::ofstream extra_fs(save_path,
            std::ofstream::out | std::ofstream::trunc);
    extra_fs.write(sto_str.data(), sto_str.size());
    extra_fs.close();
    if (writer->add_file(SNAPSHOT_TSO_FILE_WITH_SLASH) != 0) {
        done->status().set_error(EINVAL, "Fail to add file");
        DB_WARNING("Error while adding file to writer");
        return;
    }
    DB_WARNING("save physical string:%s when snapshot", sto_str.c_str());
}

int TSOStateMachine::on_snapshot_load(braft::SnapshotReader* reader) {
    DB_WARNING("start on snapshot load");
    std::vector<std::string> files;
    reader->list_files(&files);
    for (auto& file : files) {
        DB_WARNING("snapshot load file:%s", file.c_str());
        if (file == SNAPSHOT_TSO_FILE_WITH_SLASH) {
            std::string tso_file = reader->get_path() + SNAPSHOT_TSO_FILE_WITH_SLASH;
            if (load_tso(tso_file) != 0) {
                DB_WARNING("load tso fail");
                return -1;
            }
            break;
        }
    }
    set_have_data(true);
    return 0;
}

int TSOStateMachine::load_tso(const std::string& tso_file) {
    std::ifstream extra_fs(tso_file);
    std::string extra((std::istreambuf_iterator<char>(extra_fs)),
            std::istreambuf_iterator<char>());
    try {
        _tso_obj.last_save_physical = std::stol(extra);
    } catch (std::invalid_argument&){
        DB_WARNING("Invalid_argument: %s", extra.c_str());
        return -1;
    }
    catch (std::out_of_range&){
        DB_WARNING("Out of range: %s", extra.c_str());
        return -1;
    }
    catch (...) {
        DB_WARNING("error happen: %s", extra.c_str());
        return -1;
    }
    return 0;
}

}//namespace
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
