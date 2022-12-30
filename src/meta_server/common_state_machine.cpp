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

#include "common_state_machine.h"
#include "meta_server_interact.hpp"
#include "store_interact.hpp"
#include "meta_util.h"
#include "region_manager.h"

namespace baikaldb {
DECLARE_string(meta_server_bns);
DECLARE_int32(meta_replica_number);

DEFINE_int32(snapshot_interval_s, 600, "raft snapshot interval(s)");
DEFINE_int32(election_timeout_ms, 1000, "raft election timeout(ms)");
DEFINE_string(log_uri, "myraftlog://my_raft_log?id=", "raft log uri");
DEFINE_string(stable_uri, "local://./raft_data/stable", "raft stable path");
DEFINE_string(snapshot_uri, "local://./raft_data/snapshot", "raft snapshot path");
DEFINE_int64(check_migrate_interval_us, 60 * 1000 * 1000LL, "check meta server migrate interval (60s)");

void MetaServerClosure::Run() {
    if (!status().ok()) {
        if (response) {
            response->set_errcode(pb::NOT_LEADER);
            response->set_leader(butil::endpoint2str(common_state_machine->get_leader()).c_str());
        }
        DB_FATAL("meta server closure fail, error_code:%d, error_mas:%s",
                status().error_code(), status().error_cstr());
    }
    total_time_cost = time_cost.get_time();
    std::string remote_side;
    if (cntl != nullptr) {
        remote_side = butil::endpoint2str(cntl->remote_side()).c_str();
    }
  
    if (response != nullptr && response->op_type() != pb::OP_GEN_ID_FOR_AUTO_INCREMENT) {
        DB_NOTICE("request:%s, response:%s, raft_time_cost:[%ld], total_time_cost:[%ld], remote_side:[%s]", 
                request.c_str(), 
                response->ShortDebugString().c_str(), 
                raft_time_cost, 
                total_time_cost, 
                remote_side.c_str());
    }
    if (response != nullptr && response->op_type() == pb::OP_CREATE_TABLE) {
        if(!whether_level_table && create_table_ret == 0){
            std::string table_name = create_table_schema_pb.table_name();
            if (init_regions->size() <= FLAGS_pre_split_threashold) {
                if (TableManager::get_instance()->do_create_table_sync_req(
                        create_table_schema_pb, init_regions, has_auto_increment, start_region_id, response) != 0) {
                    DB_FATAL("fail to create table : %s", table_name.c_str());
                } else {
                    DB_NOTICE("create table:%s completely", table_name.c_str());
                }
            } else {
                DB_NOTICE("create table:%s async completely", table_name.c_str());
            }
        }
    }
    if (done != nullptr) {
        done->Run();
    }
    delete this;
}

void TsoClosure::Run() {
    if (!status().ok()) {
        if (response) {
            response->set_errcode(pb::NOT_LEADER);
            response->set_leader(butil::endpoint2str(common_state_machine->get_leader()).c_str());
        }
        DB_FATAL("meta server closure fail, error_code:%d, error_mas:%s",
                status().error_code(), status().error_cstr());
    }
    if (sync_cond) {
        sync_cond->decrease_signal();
    }
    if (done != nullptr) {
        done->Run();
    }
    delete this;
}

int CommonStateMachine::init(const std::vector<braft::PeerId>& peers) {
    braft::NodeOptions options;
    options.election_timeout_ms = FLAGS_election_timeout_ms; 
    options.fsm = this;                                                               
    options.initial_conf = braft::Configuration(peers);                                    
    options.snapshot_interval_s = FLAGS_snapshot_interval_s; 
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

void CommonStateMachine::process(google::protobuf::RpcController* controller,
                               const pb::MetaManagerRequest* request,
                               pb::MetaManagerResponse* response,
                               google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!_is_leader) {
        if (response) {
            response->set_errcode(pb::NOT_LEADER);
            response->set_errmsg("not leader");
            response->set_leader(butil::endpoint2str(_node.leader_id().addr).c_str());
        }
        DB_WARNING("state machine not leader, request: %s", request->ShortDebugString().c_str());
        return;
    }
    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(controller);
    butil::IOBuf data;
    butil::IOBufAsZeroCopyOutputStream wrapper(&data);
    if (!request->SerializeToZeroCopyStream(&wrapper) && cntl) {
        cntl->SetFailed(brpc::EREQUEST, "Fail to serialize request");
        return;
    }
    MetaServerClosure* closure = new MetaServerClosure;
    closure->request = request->ShortDebugString();
    closure->cntl = cntl;
    closure->response = response;
    closure->done = done_guard.release();
    closure->common_state_machine = this;
    braft::Task task;
    task.data = &data;
    task.done = closure;
    _node.apply(task);
}

void CommonStateMachine::start_check_bns() {
    //bns ，自动探测是否迁移
    if (FLAGS_meta_server_bns.find(":") == std::string::npos) {
        if (!_check_start) {
            auto fun = [this]() {
                start_check_migrate();
            };
            _check_migrate.run(fun);
            _check_start = true;
        }
    }
}
void CommonStateMachine::on_leader_start() {
    start_check_bns();
    _is_leader.store(true);
}

void CommonStateMachine::on_leader_start(int64_t term) {
    DB_WARNING("leader start at term:%ld", term);
    on_leader_start();
}

void CommonStateMachine::on_leader_stop() {
    _is_leader.store(false);
    if (_check_start) {
        _check_migrate.join();
        _check_start = false;
        DB_WARNING("check migrate thread join");
    }
    DB_WARNING("leader stop");
}

void CommonStateMachine::on_leader_stop(const butil::Status& status) {
    DB_WARNING("leader stop, error_code:%d, error_des:%s",
                status.error_code(), status.error_cstr());
    on_leader_stop();
}
void CommonStateMachine::on_error(const::braft::Error& e) {
    DB_FATAL("meta state machine error, error_type:%d, error_code:%d, error_des:%s",
                    e.type(), e.status().error_code(), e.status().error_cstr());
}
void CommonStateMachine::on_configuration_committed(const ::braft::Configuration& conf) {
    std::string new_peer;
    for (auto iter = conf.begin(); iter != conf.end(); ++iter) {
        new_peer += iter->to_string() + ",";
    }
    DB_WARNING("new conf committed, new peer:%s", new_peer.c_str());
}

void CommonStateMachine::start_check_migrate() {
    DB_WARNING("start check migrate");
    static int64_t count = 0;
    int64_t sleep_time_count = FLAGS_check_migrate_interval_us / (1000 * 1000LL); //以S为单位
    while (_node.is_leader()) {
        int time = 0;
        while (time < sleep_time_count) {
            if (!_node.is_leader()) {
                return;
            }
             bthread_usleep(1000 * 1000LL);
             ++time;
        }
        SELF_TRACE("start check migrate, count: %ld", count);
        ++count;
        check_migrate();
    }
}
void CommonStateMachine::check_migrate() {
    //判断meta_server是否需要做迁移
    std::vector<std::string> instances;
    std::string remove_peer;
    std::string add_peer;
    int ret = 0;
    if (get_instance_from_bns(&ret,FLAGS_meta_server_bns, instances, false) != 0 || 
            (int32_t)instances.size() != FLAGS_meta_replica_number) {
        DB_WARNING("get instance from bns fail, bns:%s, ret:%d, instance.size:%lu",
                    FLAGS_meta_server_bns.c_str(), ret, instances.size());
        return;
    }
    std::set<std::string> instance_set;
    for (auto& instance: instances) {
        instance_set.insert(instance);
    }
    std::set<std::string> peers_in_server;
    std::vector<braft::PeerId> peers;
    if (!_node.list_peers(&peers).ok()) {
        DB_WARNING("node list peer fail");
        return;
    }
    for (auto& peer : peers) {
        peers_in_server.insert(butil::endpoint2str(peer.addr).c_str());
    }
    if (peers_in_server == instance_set) {
        return;
    }
    for (auto& peer : peers_in_server) {
        if (instance_set.find(peer) == instance_set.end()) {
            remove_peer = peer;
            DB_WARNING("remove peer: %s", remove_peer.c_str());
            break;
        }
    }
    for (auto& peer : instance_set) {
        if (peers_in_server.find(peer) == peers_in_server.end()) {
            add_peer = peer;
            DB_WARNING("add peer: %s", add_peer.c_str());
            break;
        }
    }
    ret = 0;
    if (remove_peer.size() != 0) {
        //发送remove_peer的请求
        ret = send_set_peer_request(true, remove_peer);
    }
    if (add_peer.size() != 0 && ret == 0) {
        send_set_peer_request(false, add_peer);
    }
} 
int CommonStateMachine::send_set_peer_request(bool remove_peer, const std::string& change_peer) {
    MetaServerInteract meta_server_interact;
    if (meta_server_interact.init() != 0) {
        DB_FATAL("meta server interact init fail when set peer");
        return -1;
    }
    pb::RaftControlRequest request;
    request.set_op_type(pb::SetPeer);
    request.set_region_id(_dummy_region_id);
    std::set<std::string> peers_in_server;
    std::vector<braft::PeerId> peers;
    if (!_node.list_peers(&peers).ok()) {
        DB_WARNING("node list peer fail");
        return -1;
    }
    for (auto& peer : peers) {
        request.add_old_peers(butil::endpoint2str(peer.addr).c_str());
        if (!remove_peer || (remove_peer && peer != change_peer)) {
            request.add_new_peers(butil::endpoint2str(peer.addr).c_str());
        }
    }
    if (!remove_peer) {
        request.add_new_peers(change_peer);
    }
    pb::RaftControlResponse response;
    int ret = meta_server_interact.send_request("raft_control", request, response);
    if (ret != 0) {
        DB_WARNING("set peer when meta server migrate fail, request:%s, response:%s",
                  request.ShortDebugString().c_str(), response.ShortDebugString().c_str());
    }
    return ret;
}

}//namespace
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
