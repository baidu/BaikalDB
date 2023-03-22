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

#pragma once
#ifdef BAIDU_INTERNAL
#include <bthread.h>
#else
#include <bthread/bthread.h>
#endif
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include "table_record.h"
#include "schema_factory.h"
#include "runtime_state.h"
#include "exec_node.h"
#include "network_socket.h"
#include "proto/store.interface.pb.h"
namespace baikaldb {
enum ErrorType {
    E_OK = 0,
    E_WARNING,
    E_FATAL,
    E_BIG_SQL,
    E_RETURN,
    E_ASYNC,
    E_RETRY
};

class RPCCtrl;
class FetcherStore;
#define DB_DONE(level, _fmt_, args...) \
    do {\
        DB_##level("old_region_id: %ld, region_id: %ld, retry_times: %d, start_seq_id: %d, current_seq_id: %d, "   \
                   "op_type: %s, log_id: %lu, txn_id: %lu, sign: %lu, addr: %s, backup: %s; " _fmt_,               \
            _old_region_id, _region_id, _retry_times, _start_seq_id, _current_seq_id,                              \
            pb::OpType_Name(_op_type).c_str(), _state->log_id(), _state->txn_id, _state->sign,                     \
            _addr.c_str(), _backup.c_str(), ##args);                                                               \
    } while (0);

class OnRPCDone: public google::protobuf::Closure {
public:
    OnRPCDone(FetcherStore* fetcher_store, RuntimeState* state, ExecNode* store_request, pb::RegionInfo* info_ptr, 
        int64_t old_region_id, int64_t region_id, int start_seq_id, int current_seq_id, pb::OpType op_type);
    virtual ~OnRPCDone();
    virtual void Run();
    std::string key() {
        return _store_addr;
    }

    void retry_times_inc() {
        _retry_times++;
    }
    
    void set_rpc_ctrl(RPCCtrl* ctrl) {
        _rpc_ctrl = ctrl;
    }

    std::shared_ptr<pb::TraceNode> get_trace() {
        return _trace_node;
    }

    template<typename Repeated>
    void select_valid_peers(const std::string& resource_tag, 
                            Repeated&& peers, 
                            std::vector<std::string>& valid_peers);

    bool rpc_need_retry(int32_t errcode) {
        switch (errcode) {
            case ENETDOWN:     // 100, Network is down
            case ENETUNREACH:  // 101, Network is unreachable
            case ENETRESET:    // 102, Network dropped connection because of reset
            case ECONNABORTED: // 103, Software caused connection abort
            case ECONNRESET:   // 104, Connection reset by peer
            case ENOBUFS:      // 105, No buffer space available
            case EISCONN:      // 106, Transport endpoint is already connected
            case ENOTCONN:     // 107, Transport endpoint is not connected
            case ESHUTDOWN:    // 108, Cannot send after transport endpoint shutdown
            case ETOOMANYREFS: // 109, Too many references: cannot splice 
            case ETIMEDOUT:    // 110, Connection timed out
            case ECONNREFUSED: // 111, Connection refused
            case EHOSTDOWN:    // 112, Host is down
            case EHOSTUNREACH: // 113, No route to host
            case ECANCELED:    // 125, Operation Cancelled
            case brpc::EBACKUPREQUEST: // 1007, Sending backup request
                return true;
            default:
                return false;
        }
        return false;
    }
    ErrorType check_status();
    ErrorType send_async();
    ErrorType fill_request();
    void select_addr();
    void select_resource_insulate_read_addr(const std::string& insulate_resource_tag);
    void send_request();
    ErrorType handle_version_old();
    ErrorType handle_response(const std::string& remote_side);

private:
    FetcherStore* _fetcher_store;
    RuntimeState* _state;
    ExecNode* _store_request;
    pb::RegionInfo& _info;
    int64_t _old_region_id = 0;
    int64_t _region_id = 0;
    int _retry_times = 0;
    int _start_seq_id = 0;
    int _current_seq_id = 0;
    const pb::OpType _op_type;

    TimeCost _total_cost;
    TimeCost _query_time;
    // _resource_insulate_read包括: 访问learner / 指定isolate_resource_tag 资源隔离读从
    bool _resource_insulate_read = false; 
    std::string _addr;
    std::string _backup;
    NetworkSocket* _client_conn = nullptr;

    pb::StoreReq _request;
    pb::StoreRes _response;
    bool _has_fill_request = false;
    std::shared_ptr<pb::TraceNode> _trace_node = nullptr;
    brpc::Controller _cntl;
    RPCCtrl* _rpc_ctrl = nullptr;
    std::string _store_addr;
    static bvar::Adder<int64_t>  async_rpc_region_count;
    static bvar::LatencyRecorder total_send_request;
    static bvar::LatencyRecorder add_backup_send_request;
    static bvar::LatencyRecorder has_backup_send_request;
};

// RPCCtrl只控制rpc的异步发送和并发控制，具体rpc的成功与否结果收集由fetcher_store处理
class RPCCtrl {
public:
    explicit RPCCtrl(int concurrency_per_store) : _task_concurrency_per_group(concurrency_per_store) { }
    ~RPCCtrl() { }

    // 0：获取任务成功；1：任务完成
    // 完成的情况下会等待异步任务结束返回
    int fetch_task(std::vector<OnRPCDone*>& tasks) {
        std::unique_lock<bthread::Mutex> lck(_mutex);
        while(true) {
            // 完成
            if (_todo_cnt == 0 && _doing_cnt == 0) {
                return 1;
            }

            // 获取任务 
            tasks.clear();
            if (_todo_cnt > 0) {
                for (auto& iter : _ip_task_group_map) {
                    auto task_group = iter.second;
                    while (!task_group->todo_tasks.empty()) {
                        if (task_group->doing_cnt < _task_concurrency_per_group) {
                            _doing_cnt++;
                            _todo_cnt--;
                            task_group->doing_cnt++;
                            tasks.emplace_back(task_group->todo_tasks.back());
                            task_group->todo_tasks.pop_back();
                        } else {
                            break;
                        }
                    }
                }
            }

            if (tasks.empty()) {
                // 没有获取到任务，等待唤醒
                _cv.wait(lck);
            } else {
                // 获取成功
                return 0;
            }
        }

        return 0;
    }

    void add_new_task(OnRPCDone* task) {
        std::unique_lock<bthread::Mutex> lck(_mutex);
        auto iter = _ip_task_group_map.find(task->key());
        if (iter != _ip_task_group_map.end()) {
            iter->second->todo_tasks.emplace_back(task);
        } else {
            auto ptr = std::make_shared<TaskGroup>();
            ptr->todo_tasks.emplace_back(task);
            _ip_task_group_map[task->key()] = ptr;
        }
        task->set_rpc_ctrl(this);
        _todo_cnt++;
    } 

    void task_finish(OnRPCDone* task) {
        std::unique_lock<bthread::Mutex> lck(_mutex);
        auto task_group = _ip_task_group_map.at(task->key());
        _doing_cnt--;
        _done_cnt++;
        task_group->doing_cnt--;
        task_group->done_tasks.emplace_back(task);

        // 唤醒主线程
        _cv.notify_one();
    }

    void task_retry(OnRPCDone* task) {
        task->retry_times_inc();
        std::unique_lock<bthread::Mutex> lck(_mutex);
        auto task_group = _ip_task_group_map.at(task->key());
        _doing_cnt--;
        _todo_cnt++;
        task_group->doing_cnt--;
        task_group->todo_tasks.emplace_back(task);

        // 唤醒主线程
        _cv.notify_one();
    }
    

    void execute() {
        while (true) {
            std::vector<OnRPCDone*> tasks;
            int ret = fetch_task(tasks);
            if (ret == 1) {
                return;
            }

            for (OnRPCDone* task : tasks) {
                task->send_request();
            }
        }
    }

private:
    struct TaskGroup {
        TaskGroup() { }

        ~TaskGroup() {
            if (doing_cnt != 0) {
                DB_FATAL("memory leak, doing_cnt: %d", doing_cnt);
            }

            clear_tasks(todo_tasks);
            clear_tasks(done_tasks);
        }

        void clear_tasks(std::vector<OnRPCDone*>& tasks) {
            for (auto task : tasks) {
                if (task != nullptr) {
                    delete task;
                }
            }
            tasks.clear();
        }

        int doing_cnt = 0;
        std::vector<OnRPCDone*> todo_tasks;
        std::vector<OnRPCDone*> done_tasks;
        DISALLOW_COPY_AND_ASSIGN(TaskGroup);
    };

    int _task_concurrency_per_group;
    int _todo_cnt  = 0;
    int _done_cnt  = 0;
    int _doing_cnt = 0;
    std::map<std::string, std::shared_ptr<TaskGroup>> _ip_task_group_map;
    bthread::ConditionVariable _cv;
    bthread::Mutex _mutex;
};

// 全局二级索引降级使用，将主备请求分别发往不同集群
enum GlobalBackupType {
    GBT_INIT = 0, // 默认值，不是全局索引降级的请求
    GBT_MAIN,     // 主索引强制访问主集群
    GBT_LEARNER,  // 降级backup请求强制访问learner
};

class PeerStatus {
public:
    void set_cannot_access(int64_t region_id, const std::string& peer) {
        BAIDU_SCOPED_LOCK(_lock);
        _region_id_dead_peers[region_id].insert(peer);
    }

    bool can_access(int64_t region_id, const std::string& peer) {
        BAIDU_SCOPED_LOCK(_lock);
        return _region_id_dead_peers[region_id].count(peer) <= 0;
    }

    void get_cannot_access_peer(int64_t region_id, std::set<std::string>& cannot_access_peers) {
        BAIDU_SCOPED_LOCK(_lock);
        if (_region_id_dead_peers.count(region_id) <= 0) {
            return;
        }
        cannot_access_peers = _region_id_dead_peers[region_id];
        return;
    }
private:
    bthread::Mutex  _lock;
    std::map<int64_t, std::set<std::string>> _region_id_dead_peers;
};

class FetcherStore {
public:
    FetcherStore() {
    }
    virtual ~FetcherStore() {
    }
    
    void clear() {
        region_batch.clear();
        split_region_batch.clear();
        index_records.clear();
        start_key_sort.clear();
        split_start_key_sort.clear();
        error = E_OK;
        skip_region_set.clear();
        affected_rows = 0;
        scan_rows = 0;
        filter_rows = 0;
        row_cnt = 0;
        primary_timestamp_updated = false;
        no_copy_cache_plan_set.clear();
        dynamic_timeout_ms = -1;
        callids.clear();
    }

    void cancel_rpc() {
        BAIDU_SCOPED_LOCK(region_lock);
        for (auto& callid : callids) {
            brpc::StartCancel(callid);
        }
        is_cancelled = true;
    }

    void insert_callid(const brpc::CallId& callid) {
        BAIDU_SCOPED_LOCK(region_lock);
        callids.insert(callid);
    }

    // send_request不返回执行错误，外部获取fetcher_store.error判断执行结果
    void send_request(RuntimeState* state,
                           ExecNode* store_request, 
                           pb::RegionInfo* info, 
                           int start_seq_id,
                           int current_seq_id,
                           pb::OpType op_type) {
        std::vector<pb::RegionInfo*> infos;
        infos.emplace_back(info);
        send_request(state, store_request, infos, start_seq_id, current_seq_id, op_type);
    }

    void send_request(RuntimeState* state,
                           ExecNode* store_request, 
                           std::vector<pb::RegionInfo*> infos, 
                           int start_seq_id,
                           int current_seq_id,
                           pb::OpType op_type) {
        int64_t limit_single_store_concurrency_cnts = 0;
        limit_single_store_concurrency_cnts = state->calc_single_store_concurrency(op_type);
        std::set<std::shared_ptr<pb::TraceNode>> traces;

        RPCCtrl rpc_ctrl(limit_single_store_concurrency_cnts);
        for (auto info : infos) {
            auto task = new OnRPCDone(this, state, store_request, info, 
                    info->region_id(), info->region_id(), start_seq_id, current_seq_id, op_type);
            rpc_ctrl.add_new_task(task);
            traces.insert(task->get_trace());
        }

        rpc_ctrl.execute();

        if (store_request->get_trace() != nullptr) {           
            for (auto trace : traces) {
                (*store_request->get_trace()->add_child_nodes()) = *trace;
            }
        }
    }

    int run_not_set_state(RuntimeState* state, 
            std::map<int64_t, pb::RegionInfo>& region_infos,
            ExecNode* store_request,
            int start_seq_id,
            int current_seq_id,
            pb::OpType op_type, 
            GlobalBackupType backup_type);

    int run(RuntimeState* state,
                    std::map<int64_t, pb::RegionInfo>& region_infos,
                    ExecNode* store_request,
                    int start_seq_id,
                    int current_seq_id,
                    pb::OpType op_type,
                    GlobalBackupType backup_type = GBT_INIT) {
        int ret = run_not_set_state(state, region_infos, store_request, start_seq_id, current_seq_id, op_type, backup_type);
        update_state_info(state);
        if (ret < 0) {
            state->error_code = error_code;
            state->error_msg.clear();
            state->error_msg.str("");
            state->error_msg << error_msg.str();
            return -1;
        }
        return affected_rows.load();
    }

    void update_state_info(RuntimeState* state) {
        state->region_count += region_count;
        state->set_num_scan_rows(state->num_scan_rows() + scan_rows.load());
        state->set_num_filter_rows(state->num_filter_rows() + filter_rows.load());
    }

    static bool rpc_need_retry(int32_t errcode) {
        switch (errcode) {
            case ENETDOWN:     // 100, Network is down
            case ENETUNREACH:  // 101, Network is unreachable
            case ENETRESET:    // 102, Network dropped connection because of reset
            case ECONNABORTED: // 103, Software caused connection abort
            case ECONNRESET:   // 104, Connection reset by peer
            case ENOBUFS:      // 105, No buffer space available
            case EISCONN:      // 106, Transport endpoint is already connected
            case ENOTCONN:     // 107, Transport endpoint is not connected
            case ESHUTDOWN:    // 108, Cannot send after transport endpoint shutdown
            case ETOOMANYREFS: // 109, Too many references: cannot splice 
            case ETIMEDOUT:    // 110, Connection timed out
            case ECONNREFUSED: // 111, Connection refused
            case EHOSTDOWN:    // 112, Host is down
            case EHOSTUNREACH: // 113, No route to host
            case ECANCELED:    // 125, Operation Cancelled
            case brpc::EBACKUPREQUEST: // 1007, Sending backup request
                return true;
            default:
                return false;
        }
        return false;
    } 

    template<typename Repeated>
    static void choose_opt_instance(int64_t region_id, Repeated&& peers, std::string& addr, 
                                    pb::Status& addr_status, std::string* backup,
                                    const std::set<std::string>& cannot_access_peers = {}) {
        SchemaFactory* schema_factory = SchemaFactory::get_instance();
        addr_status = pb::NORMAL;
        std::string baikaldb_logical_room = schema_factory->get_logical_room();
        if (baikaldb_logical_room.empty()) {
            return;
        }
        std::vector<std::string> candicate_peers;
        std::vector<std::string> normal_peers;
        bool addr_in_candicate = false;
        bool addr_in_normal = false;
        for (auto& peer: peers) {
            auto status = schema_factory->get_instance_status(peer);
            if (status.status != pb::NORMAL) {
                continue;
            } else if (cannot_access_peers.find(peer) != cannot_access_peers.end()) {
                continue;
            } else if (!status.logical_room.empty() && status.logical_room == baikaldb_logical_room) {
                if (addr == peer) {
                    addr_in_candicate = true;
                } else {
                    candicate_peers.emplace_back(peer);
                }
            } else {
                if (addr == peer) {
                    addr_in_normal = true;
                } else {
                    normal_peers.emplace_back(peer);
                }
            }
        }
        if (addr_in_candicate) {
            if (backup != nullptr) {
                if (candicate_peers.size() > 0) {
                    *backup = candicate_peers[0];
                } else if (normal_peers.size() > 0) {
                    *backup = normal_peers[0];
                }
            }
            return;
        }
        if (candicate_peers.size() > 0) {
            addr = candicate_peers[0];
            if (backup != nullptr) {
                if (candicate_peers.size() > 1) {
                    *backup = candicate_peers[1];
                } else if (normal_peers.size() > 0) {
                    *backup = normal_peers[0];
                }
            }
            return;
        }
        if (addr_in_normal) { 
            if (backup != nullptr) {
                if (normal_peers.size() > 0) {
                    *backup = normal_peers[0];
                }
            }
            return;
        } 
        if (normal_peers.size() > 0) {
            addr = normal_peers[0];
            if (normal_peers.size() > 1) {
                addr = normal_peers[1];
            }
        } else {
            addr_status = pb::FAULTY;
            DB_DEBUG("all peer faulty, %ld", region_id);
        }
    }

    bool need_process_binlog(RuntimeState* state, pb::OpType op_type) {
        if (op_type == pb::OP_PREPARE
            || op_type == pb::OP_COMMIT) {
            if (client_conn->need_send_binlog()) {
                return true;
            }
        } else if (op_type == pb::OP_ROLLBACK) {
            if (state->open_binlog() && binlog_prepare_success) {
                return true;
            }
        }
        return false;
    }
    static void choose_other_if_dead(pb::RegionInfo& info, std::string& addr);
    static void other_normal_peer_to_leader(pb::RegionInfo& info, std::string& addr);
    ErrorType process_binlog_start(RuntimeState* state, pb::OpType op_type);
    void process_binlog_done(RuntimeState* state, pb::OpType op_type) {
        binlog_cond.wait();
    }
    ErrorType write_binlog(RuntimeState* state,
                           const pb::OpType op_type,
                           const uint64_t log_id);
    int64_t get_commit_ts();
    static int64_t get_dynamic_timeout_ms(ExecNode* store_request, pb::OpType op_type, uint64_t sign);
    static int64_t get_sign_latency(pb::OpType op_type, uint64_t sign);

public:
    std::map<int64_t, std::vector<SmartRecord>>  index_records; //key: index_id
    std::vector<std::string>  return_str_records;
    std::vector<std::string>  return_str_old_records;
    std::map<int64_t, std::shared_ptr<RowBatch>> region_batch;
    std::map<int64_t, std::shared_ptr<RowBatch>> split_region_batch;
    std::map<int64_t, std::vector<int64_t>> region_id_ttl_timestamp_batch;

    std::multimap<std::string, int64_t> start_key_sort;
    std::multimap<std::string, int64_t> split_start_key_sort;
    bthread::Mutex region_lock;
    std::set<int64_t> skip_region_set;
    std::atomic<ErrorType> error = {E_OK};
    // 因为split会导致多region出来,加锁保护公共资源
    std::atomic<int64_t> row_cnt = {0};
    std::atomic<int64_t> affected_rows = {0};
    std::atomic<int64_t> scan_rows = {0};
    std::atomic<int64_t> filter_rows = {0};
    bool is_cancelled = false;
    BthreadCond binlog_cond;
    NetworkSocket* client_conn = nullptr;
    bool  binlog_prepare_success = false;
    bool  need_get_binlog_region = true;
    std::atomic<bool> primary_timestamp_updated{false};
    std::set<int64_t> no_copy_cache_plan_set;
    int64_t dynamic_timeout_ms = -1;
    int64_t sign_latency = -1;
    TimeCost binlog_prewrite_time;
    PeerStatus peer_status; // 包括follower和learner
    MysqlErrCode      error_code = ER_ERROR_FIRST;
    std::ostringstream error_msg;
    int32_t region_count = 0;
    std::set<brpc::CallId> callids;
    GlobalBackupType global_backup_type = GBT_INIT;
};

template<typename Repeated>
void OnRPCDone::select_valid_peers(const std::string& resource_tag, 
                        Repeated&& peers, 
                        std::vector<std::string>& valid_peers) {
    valid_peers.clear();
    SchemaFactory* schema_factory = SchemaFactory::get_instance();
    for (auto& peer : peers) {
        if (!resource_tag.empty()) {
            auto status = schema_factory->get_instance_status(peer);
            if (status.resource_tag != resource_tag) {
                continue;
            }
        } 
        if (!_fetcher_store->peer_status.can_access(_info.region_id(), peer)) {
            // learner/peer异常时报警
            DB_DONE(WARNING, "has abnormal peer/learner: %s", peer.c_str());
            continue;
        }
        valid_peers.emplace_back(peer);
    }
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
