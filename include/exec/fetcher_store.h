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

struct RegionReturnData {
    std::shared_ptr<RowBatch> row_data = nullptr;
    std::shared_ptr<arrow::RecordBatch> arrow_data = nullptr;
    void set_row_data(std::shared_ptr<RowBatch>& batch) {
        row_data = batch;
        arrow_data = nullptr;
    } 
    void set_arrow_data(std::shared_ptr<arrow::RecordBatch>& batch) {
        arrow_data = batch;
        row_data = nullptr;
    }
};

struct RegionInfoData {
    pb::RegionInfo* region_info;
    bool select_without_leader;
    bool resource_insulate_read;
    RegionInfoData() {}
};

class RPCCtrl;
class FetcherStore;
#define DB_DONE(level, _fmt_, args...) \
    do {\
        DB_##level("old_region_id: %ld, region_id: %ld,  region_ids: %s, retry_times: %d,"               \
                   "start_seq_id: %d, current_seq_id: %d, "                                              \
                   "op_type: %s, log_id: %lu, txn_id: %lu, sign: %lu, addr: %s, backup: %s; "            \
                   _fmt_, _old_region_id, _region_id, _region_ids_str.substr(0, 100).c_str(), _retry_times, \
                   _start_seq_id, _current_seq_id, pb::OpType_Name(_op_type).c_str(), _state->log_id(),  \
                   _state->txn_id, _state->sign, _addr.c_str(), _backup.c_str(),##args);                 \
    } while (0);

class OnRPCDone : public google::protobuf::Closure {
public:
    OnRPCDone(FetcherStore* fetcher_store, RuntimeState* state, 
                        ExecNode* store_request, int start_seq_id, int current_seq_id, 
                        pb::OpType op_type, bool need_check_memory);
    virtual ~OnRPCDone(); 
    void send_request();
    virtual void pre_send_async() = 0;
    std::string key() const {
        return _store_addr;
    };

    void retry_times_inc() {
        _retry_times++;
    }

    void set_rpc_ctrl(RPCCtrl* ctrl) {
        _rpc_ctrl = ctrl;
    }
    
    std::shared_ptr<pb::TraceNode> get_trace() {
        return _trace_node;
    }

    virtual ErrorType fill_request() = 0;
    ErrorType fill_single_request(pb::StoreReq& single_req, pb::RegionInfo& region_info, int64_t region_id, int64_t old_region_id);

    ErrorType send_async();

    virtual void set_region_vectorized_response(int64_t region_id, std::shared_ptr<pb::StoreRes> single_response_ptr) = 0;
    template<typename Repeated>
    void select_valid_peers(const pb::RegionInfo& info,
                            const std::string& resource_tag, 
                            Repeated&& peers, 
                            std::vector<std::string>& valid_peers);
    void select_resource_insulate_read_addr(pb::RegionInfo& info,
                            std::string& addr,
                            const std::string& insulate_resource_tag,
                            bool& select_without_leader,
                            bool& resource_insulate_read);
    void select_addr(pb::RegionInfo& info, 
                            std::string& addr,
                            bool& resource_insulate_read,
                            bool& select_without_leader);
    ErrorType handle_version_old(const pb::RegionInfo& info, pb::StoreRes& response);
    ErrorType handle_single_response(const std::string& remote_side,
                                    int64_t region_id,
                                    const std::string& addr,
                                    pb::RegionInfo& info,
                                    std::shared_ptr<pb::StoreRes> single_response);
    ErrorType check_status();

    void clear_request() {
        if (_need_check_memory && _has_multi_plan && _op_type == pb::OP_SELECT) {
            // 每个region一个plan, 没有复用
            if (_is_batch) {
                _batch_request.Clear();
            } else {
                _request.Clear();
            }
            _has_fill_request = false;
            _state->memory_limit_release(std::numeric_limits<int>::max(), _request_size);
        }
    }

    virtual void Run(const std::string& version);
    virtual void retry_region_task(const std::string& remote_side) = 0;
    virtual void retry_or_finish_task(const std::string& remote_side) = 0;

    brpc::Controller _cntl;
    ExecNode* _store_request;
    FetcherStore* _fetcher_store;
    RuntimeState* _state;
    RPCCtrl* _rpc_ctrl = nullptr;
    std::shared_ptr<pb::TraceNode> _trace_node = nullptr;
    NetworkSocket* _client_conn = nullptr;

    int64_t _old_region_id = 0;
    int64_t _region_id = 0;
    int _retry_times = 0;
    int _start_seq_id = 0;
    int _current_seq_id = 0;
    bool _has_fill_request = false;
    std::string _addr;
    std::string _store_addr;
    std::string _backup;
    std::string _region_ids_str = "";
    const pb::OpType _op_type;
    bool _is_batch = false;
    std::vector<int64_t> _region_ids;
    bool _has_multi_plan = false;

    // DBLINK外部映射表使用
    int64_t _meta_id = -1;

    TimeCost _total_cost;
    TimeCost _query_time;

    pb::BatchRegionStoreReq _batch_request;
    std::shared_ptr<pb::BatchRegionStoreRes> _batch_response_ptr = std::make_shared<pb::BatchRegionStoreRes>();
    pb::StoreReq _request;
    std::shared_ptr<pb::StoreRes> _response_ptr = std::make_shared<pb::StoreRes>();
    std::string _versions_str;

    // _resource_insulate_read包括: 访问learner / 指定isolate_resource_tag 资源隔离读从
    bool _resource_insulate_read = false;

    // 内存限制
    bool _need_check_memory = false;
    int64_t _request_size = 0;
    
    static bvar::Adder<int64_t>  async_rpc_region_count;
    static bvar::LatencyRecorder total_send_request;
    static bvar::LatencyRecorder add_backup_send_request;
    static bvar::LatencyRecorder has_backup_send_request;
};

class OnSingleRPCDone: public OnRPCDone {
public:
    OnSingleRPCDone(FetcherStore* fetcher_store, RuntimeState* state, ExecNode* store_request, pb::RegionInfo* info_ptr, 
        int64_t old_region_id, int64_t region_id, int start_seq_id, int current_seq_id, pb::OpType op_type, bool need_check_memory);
    virtual ~OnSingleRPCDone();
    virtual void Run();
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
    virtual ErrorType fill_request();
    void pre_send_async() override {
        select_addr();
    }
    void select_addr();
    ErrorType handle_response(const std::string& remote_side);
    void pick_addr_for_resource_tag(const std::string& resource_tag, std::string& addr) {
        std::string baikaldb_logical_room = SchemaFactory::get_instance()->get_logical_room();
        for (auto& peer : _info.peers()) {
            auto status = SchemaFactory::get_instance()->get_instance_status(peer);
            if (status.status == pb::NORMAL 
                    && status.resource_tag == resource_tag) {
                addr = peer;
                if (status.logical_room == baikaldb_logical_room) {
                    break;
                }
            }
        }
    }
    void set_region_vectorized_response(int64_t region_id, std::shared_ptr<pb::StoreRes> single_response_ptr) override;
    virtual void retry_region_task(const std::string& remote_side) override;
    virtual void retry_or_finish_task(const std::string& remote_side) override;
private:
    pb::RegionInfo& _info;
};

class OnBatchRPCDone: public OnRPCDone {
public:
    OnBatchRPCDone(FetcherStore* fetcher_store, 
                RuntimeState* state, 
                ExecNode* store_request, 
                std::vector<pb::RegionInfo*> infos,
                int start_seq_id, 
                int current_seq_id, 
                pb::OpType op_type,
                int64_t limit_single_store_concurrency_cnts,
                bool need_check_memory);
    OnBatchRPCDone(FetcherStore* fetcher_store, 
                RuntimeState* state, 
                ExecNode* store_request, 
                std::vector<RegionInfoData> infos,
                const std::string& store_addr,
                int start_seq_id, 
                int current_seq_id, 
                pb::OpType op_type,
                int64_t limit_single_store_concurrency_cnts,
                bool need_check_memory);
    virtual ~OnBatchRPCDone();
    virtual void Run();
    virtual ErrorType fill_request();
    ErrorType handle_response(const std::string& remote_side, std::vector<OnSingleRPCDone*>& need_retry_tasks);
    void pre_send_async() override {}
    std::vector<OnBatchRPCDone*> select_addr();
    void set_region_vectorized_response(int64_t region_id, std::shared_ptr<pb::StoreRes> single_response_ptr) override;
    virtual void retry_region_task(const std::string& remote_side) override;
    virtual void retry_or_finish_task(const std::string& remote_side) override;
private:
    std::vector<pb::RegionInfo*> _infos;
    std::vector<RegionInfoData> _info_datas;
    std::map<int64_t, pb::RegionInfo> _info_map;
    int _start_seq_id = 0;
    int _current_seq_id = 0;
    bool _is_real_exec = false;
    
    int64_t _limit_single_store_concurrency_cnts;
    static bvar::LatencyRecorder batch_rpc_region_count;
};
// RPCCtrl只控制rpc的异步发送和并发控制，具体rpc的成功与否结果收集由fetcher_store处理
class RPCCtrl {
public:
    explicit RPCCtrl(int concurrency_per_store, bool need_check_memory)
         : _task_concurrency_per_group(concurrency_per_store), _need_check_memory(need_check_memory) { }
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
        task->clear_request();
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
    bool _need_check_memory = false;
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

struct WriteBinlogParam {
    int64_t txn_id = 0;
    uint64_t log_id = 0;
    uint64_t global_conn_id = 0;
    int64_t primary_region_id = -1;
    pb::OpType op_type;
    std::string username;
    std::string ip;
    NetworkSocket* client_conn = nullptr;
    FetcherStore* fetcher_store;
};
class FetcherStore {
public:
    FetcherStore() {
    }
    virtual ~FetcherStore() {
    }
    
    void clear() {
        region_batch.clear();
        index_records.clear();
        start_key_sort.clear();
        error = E_OK;
        skip_region_set.clear();
        affected_rows = 0;
        scan_rows = 0;
        read_disk_size = 0;
        filter_rows = 0;
        row_cnt = 0;
        primary_timestamp_updated = false;
        no_copy_cache_plan_set.clear();
        dynamic_timeout_ms = -1;
        callids.clear();
        arrow_schema.reset();
        region_vectorized_response.clear();
        batch_region_vectorized_response.clear();
        shared_plan.reset();
        db_handle_bytes = 0;
        db_handle_rows = 0;
        is_full_export = false;
        received_arrow_data = false;
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
                           pb::OpType op_type);
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
        state->set_read_disk_size(state->read_disk_size() + read_disk_size.load());
        state->set_num_filter_rows(state->num_filter_rows() + filter_rows.load());
        state->inc_db_handle_bytes(db_handle_bytes.load());
        state->inc_db_handle_rows(db_handle_rows.load());
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
            case brpc::ENOMETHOD: //1002, Method not found
            case brpc::EBACKUPREQUEST: // 1007, Sending backup request
                return true;
            default:
                return false;
        }
        return false;
    } 

    template<typename Repeated>
    static void choose_opt_instance(int64_t region_id, Repeated&& peers, const std::string& resource_tag, 
                                    std::string& addr, pb::Status& addr_status, std::string* backup,
                                    const std::set<std::string>& cannot_access_peers = {}, bool rolling = false) {
        SchemaFactory* schema_factory = SchemaFactory::get_instance();
        addr_status = pb::NORMAL;
        std::string baikaldb_logical_room = schema_factory->get_logical_room();
        std::vector<std::string> candicate_peers;
        std::vector<std::string> candicate_peers2;
        std::vector<std::string> normal_peers;
        std::vector<std::string> other_peers;
        normal_peers.reserve(3);
        other_peers.reserve(3);
        bool addr_in_candicate = false;
        bool addr_in_normal = false;
        for (auto& peer: peers) {
            auto status = schema_factory->get_instance_status(peer);
            DB_DEBUG("peer:%s resource_tag:%s status.resource_tag:%s, baikaldb_logical_room:%s %s, %d", peer.c_str(), resource_tag.c_str(), status.resource_tag.c_str(), baikaldb_logical_room.c_str(), status.logical_room.c_str(), status.status);
            if (status.status == pb::DEAD) {
                continue;
            } else if (cannot_access_peers.find(peer) != cannot_access_peers.end()) {
                continue;
            } else if (status.status != pb::NORMAL) {
                other_peers.emplace_back(peer);
            } else if (rolling) {
                normal_peers.emplace_back(peer);
            } else if (!resource_tag.empty()) {
                if (status.resource_tag == resource_tag) {
                    // 倾向访问的store集群，仅第一次有效, 如pap-bj db第一次优先访问pap-bj的store
                    if (!status.logical_room.empty() && status.logical_room == baikaldb_logical_room) {
                        if (addr == peer) {
                            addr_in_candicate = true;
                        } else {
                            candicate_peers.emplace_back(peer);
                        }
                    } else {
                        candicate_peers2.emplace_back(peer);
                    }
                }
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
        if (candicate_peers.size() > 0) {
            normal_peers.insert(normal_peers.end(), candicate_peers2.begin(), candicate_peers2.end());
        } else if (candicate_peers2.size() > 0) {
            candicate_peers = candicate_peers2;
        }
        if (addr_in_candicate) {
            if (backup != nullptr) {
                if (candicate_peers.size() > 0) {
                    *backup = candicate_peers[0];
                } else if (normal_peers.size() > 0) {
                    *backup = normal_peers[0];
                } else if (other_peers.size() > 0) {
                    *backup = other_peers[0];
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
                } else if (other_peers.size() > 0) {
                    *backup = other_peers[0];
                }
            }
            return;
        }
        if (addr_in_normal) { 
            if (backup != nullptr) {
                if (normal_peers.size() > 0) {
                    *backup = normal_peers[0];
                } else if (other_peers.size() > 0) {
                    *backup = other_peers[0];
                }
            }
            return;
        } 
        if (normal_peers.size() == 0) {
            // 备选other
            normal_peers.swap(other_peers);
            addr_status = pb::FAULTY;
        }
        if (normal_peers.size() > 0) {
            uint32_t i = butil::fast_rand() % normal_peers.size();
            addr = normal_peers[i];
            if (backup != nullptr && normal_peers.size() > 1) {
                *backup = normal_peers[(i + 1) % normal_peers.size()];
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
        } else if (op_type == pb::OP_ROLLBACK && state->open_binlog()) {
            if (need_send_rollback) {
                return true;
            }
        }
        return false;
    }
    static void choose_other_if_dead(pb::RegionInfo& info, std::string& addr);
    static void other_normal_peer_to_leader(pb::RegionInfo& info, const std::string& addr);
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
    std::map<int64_t, std::vector<std::string>>  return_str_records;
    std::map<int64_t, std::vector<std::string>>  return_str_old_records;
    std::map<int64_t, RegionReturnData> region_batch;
    //std::map<int64_t, std::shared_ptr<RowBatch>> split_region_batch;
    std::map<int64_t, std::vector<int64_t>> region_id_ttl_timestamp_batch;

    std::multimap<std::string, int64_t> start_key_sort;
    //std::multimap<std::string, int64_t> split_start_key_sort;
    bthread::Mutex region_lock;
    std::set<int64_t> skip_region_set;
    std::atomic<ErrorType> error = {E_OK};
    // 因为split会导致多region出来,加锁保护公共资源
    std::atomic<int64_t> row_cnt = {0};
    std::atomic<int64_t> affected_rows = {0};
    std::atomic<int64_t> scan_rows = {0};
    std::atomic<int64_t> read_disk_size = {0};
    std::atomic<int64_t> filter_rows = {0};
    std::atomic<int64_t> db_handle_rows = {0};
    std::atomic<int64_t> db_handle_bytes = {0};
    bool is_cancelled = false;
    BthreadCond binlog_cond;
    NetworkSocket* client_conn = nullptr;
    std::atomic<bool> primary_timestamp_updated{false};
    std::set<int64_t> no_copy_cache_plan_set;
    int64_t dynamic_timeout_ms = -1;
    int64_t sign_latency = -1;
    PeerStatus peer_status; // 包括follower和learner
    MysqlErrCode      error_code = ER_ERROR_FIRST;
    std::ostringstream error_msg;
    int32_t region_count = 0;
    std::set<brpc::CallId> callids;
    bool need_send_rollback = true;
    WriteBinlogParam write_binlog_param;
    GlobalBackupType global_backup_type = GBT_INIT;
    // vectorized
    std::shared_ptr<arrow::Schema> arrow_schema;
    std::map<int64_t, std::shared_ptr<pb::StoreRes>> region_vectorized_response;
    std::map<int64_t, std::shared_ptr<pb::BatchRegionStoreRes> > batch_region_vectorized_response;
    bool received_arrow_data = false;
    // 公共plan, 减少内存
    std::shared_ptr<pb::Plan> shared_plan;
    bool is_full_export = false;
    bool need_check_memory = false;
};

template<typename Repeated>
void OnRPCDone::select_valid_peers(const pb::RegionInfo& info,
                        const std::string& resource_tag, 
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
        if (!_fetcher_store->peer_status.can_access(info.region_id(), peer)) {
            // learner/peer异常时报警
            DB_DONE(WARNING, "has abnormal peer/learner: %s", peer.c_str());
            continue;
        }
        valid_peers.emplace_back(peer);
    }
}

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
