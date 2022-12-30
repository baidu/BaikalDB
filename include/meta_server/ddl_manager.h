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

#include <string>
#include <vector>
#include "common.h"
#include "meta_util.h"
#include "meta_server.h"
#include "cluster_manager.h"
#include "proto/meta.interface.pb.h"

namespace baikaldb {
struct MemRegionDdlWork {
    pb::RegionDdlWork region_info;
    int64_t update_timestamp = 0;
};

struct MemDdlWork {
    int64_t update_timestamp = 0;
};

struct WaitTxnWorkInfo {
    bool success {false};
    bool done {false};
    pb::DdlWorkInfo work_info;
};

class StatusChangePolicy {
public:
    StatusChangePolicy() {
        bthread_mutex_init(&_mutex, NULL);
    }

    ~StatusChangePolicy() {
        bthread_mutex_destroy(&_mutex);
    }

    bool should_change(int64_t table_id, pb::IndexState status);

    //任务完成或失败，清理
    void clear(int64_t table_id) {
        BAIDU_SCOPED_LOCK(_mutex);
        _time_costs_map.erase(table_id);
    }
private:
    bthread_mutex_t                                     _mutex;
    std::unordered_map<int64_t, std::array<std::unique_ptr<TimeCost>, 8>> _time_costs_map;
};

class DBManager : public Singleton<DBManager> {
public:
    //"table_id"_"region_id"
    using TaskId = std::string;

    DBManager() {
        bthread_mutex_init(&_broadcast_mutex, NULL);
        bthread_mutex_init(&_address_instance_mutex, NULL);
    }

    ~DBManager() {
        bthread_mutex_destroy(&_broadcast_mutex);
        bthread_mutex_destroy(&_address_instance_mutex);
    }

    void set_meta_state_machine(MetaStateMachine* meta_state_machine) {
        _meta_state_machine = meta_state_machine;
    }

    void process_baikal_heartbeat(const pb::BaikalHeartBeatRequest* request,
        pb::BaikalHeartBeatResponse* response, brpc::Controller* cntl);

    void process_common_task_hearbeat(const std::string& address, const pb::BaikalHeartBeatRequest* request,
        pb::BaikalHeartBeatResponse* response);
    void process_broadcast_task_hearbeat(const std::string& address, const pb::BaikalHeartBeatRequest* request,
        pb::BaikalHeartBeatResponse* response);

    int execute_task(MemRegionDdlWork& work);
    int restore_task(const pb::RegionDdlWork& work);

    int execute_broadcast_task(pb::DdlWorkInfo& work) {
        BroadcastTaskPtr task_ptr;
        {
            BAIDU_SCOPED_LOCK(_broadcast_mutex);
            _broadcast_task_map[work.table_id()].reset(new BroadcastTask);
            task_ptr = _broadcast_task_map[work.table_id()];
        }
        int64_t number = 0;
        task_ptr->work = work;
        MemDdlWork mem_ddlwork;
        mem_ddlwork.update_timestamp = butil::gettimeofday_us();
        BAIDU_SCOPED_LOCK(_address_instance_mutex);
        for (const auto& instance_pair : _address_instance_map) {
            DB_NOTICE("insert txn task %s", instance_pair.first.c_str());
            if (instance_pair.second.instance_status.state != pb::FAULTY) {
                task_ptr->to_do_task_map.set(instance_pair.first, mem_ddlwork);
                number++;
            }
        }
        task_ptr->number = number;

        return 0;
    }

    void update_baikaldb_info(const std::string& address, const std::string& room) {
        auto current_timestamp = butil::gettimeofday_us();
        BAIDU_SCOPED_LOCK(_address_instance_mutex);
        auto iter = _address_instance_map.find(address);
        if (iter == _address_instance_map.end()) {
            _room_address_map[room].insert(address);
            Instance ins;
            ins.address = address;
            ins.physical_room = room;
            _address_instance_map[address] = ins;
            DB_NOTICE("DBManager address[%s] room[%s]", address.c_str(), room.c_str());
        } else {
            iter->second.instance_status.timestamp = current_timestamp;
            iter->second.instance_status.state = pb::NORMAL;
        }
    }

    std::vector<std::string> get_faulty_baikaldb();

    bool round_robin_select(std::string* selected_address, bool is_column_ddl);

    bool select_instance(std::string* address, bool is_column_ddl);

    void init(); 

    void shutdown() {
        _shutdown = true;
    }

    void join() {
        _bth.join();
    }

    void clear_all_task() {
        _common_task_map.clear();
        _broadcast_task_map.clear();
    }
    void update_txn_ready(int64_t table_id);

    void clear_all_tasks() {
        DB_NOTICE("DBManger clear all tasks.");
        _common_task_map.clear();
        BAIDU_SCOPED_LOCK(_broadcast_mutex);
        _broadcast_task_map.clear();
    }

    void clear_task(int64_t table_id) {
        auto task_prefix = std::to_string(table_id) + "_";
        _common_task_map.traverse([&task_prefix](CommonTaskMap& task_map){
            auto remove_func = [&task_prefix](std::unordered_map<TaskId, MemRegionDdlWork>& to_remove_map){
                for (auto iter = to_remove_map.begin(); iter != to_remove_map.end();) {
                    if (iter->first.find(task_prefix) == 0) {
                        iter = to_remove_map.erase(iter);
                    } else {
                        iter++;
                    }
                }
            };
            remove_func(task_map.to_do_task_map);
            remove_func(task_map.doing_task_map);
        });
    }

    struct BroadcastTask {
        std::atomic<size_t> number {0};
        pb::DdlWorkInfo work;
        ThreadSafeMap<std::string, MemDdlWork> to_do_task_map;
        ThreadSafeMap<std::string, MemDdlWork> doing_task_map;
    };

    struct CommonTaskMap {
        CommonTaskMap() = default;
        std::unordered_map<TaskId, MemRegionDdlWork> to_do_task_map;
        std::unordered_map<TaskId, MemRegionDdlWork> doing_task_map;
    };

private:

    std::unordered_map<std::string, std::set<std::string>> _room_address_map;
    std::unordered_map<std::string, Instance> _address_instance_map;
    bthread_mutex_t                          _address_instance_mutex;

    ThreadSafeMap<std::string, CommonTaskMap> _common_task_map;

    using BroadcastTaskPtr = std::shared_ptr<BroadcastTask>;
    //txn
    std::unordered_map<int64_t, BroadcastTaskPtr> _broadcast_task_map;
    bthread_mutex_t                          _broadcast_mutex;

    std::atomic<bool> _shutdown {false};
    MetaStateMachine* _meta_state_machine {nullptr};
    Bthread _bth;
    std::string _last_rolling_instance;
};

class DDLManager : public Singleton<DDLManager> {
public:

    DDLManager() {
        bthread_mutex_init(&_txn_mutex, NULL);
        bthread_mutex_init(&_table_mutex, NULL);
        bthread_mutex_init(&_region_mutex, NULL);
    }

    ~DDLManager() {
        bthread_mutex_destroy(&_txn_mutex);
        bthread_mutex_destroy(&_table_mutex);
        bthread_mutex_destroy(&_region_mutex);
    }
    void set_meta_state_machine(MetaStateMachine* meta_state_machine) {
        _meta_state_machine = meta_state_machine;
    }
    int init_index_ddlwork(int64_t table_id, const pb::IndexInfo& index_info, 
        std::unordered_map<int64_t, std::set<int64_t>>& partition_regions);
    
    int init_column_ddlwork(int64_t table_id, const pb::DdlWorkInfo& work_info, 
        std::unordered_map<int64_t, std::set<int64_t>>& partition_regions);

    int init_del_index_ddlwork(int64_t table_id, const pb::IndexInfo& index_info);
    // 定时线程处理所有ddl work。
    int work();

    // 定时线程处理所有ddl work。
    int launch_work();

    //处理单个ddl work
    int add_index_ddlwork(pb::DdlWorkInfo& val);
    int add_column_ddlwork(pb::DdlWorkInfo& ddl_work);

    void join() {
        _work_thread.join();
    }

    void shutdown() {
        _shutdown = true;
    }

    int drop_index_ddlwork(pb::DdlWorkInfo& val);
    int load_table_ddl_snapshot(const pb::DdlWorkInfo& val);
    int load_region_ddl_snapshot(const std::string& info);

    int update_region_ddlwork(const pb::RegionDdlWork& work);

    void set_txn_ready(int64_t table_id, bool success) {
        DB_NOTICE("set txn ready.");
        BAIDU_SCOPED_LOCK(_txn_mutex);
        auto iter = _wait_txns.find(table_id);
        if (iter != _wait_txns.end()) {
            _wait_txns[table_id].success = success;
            _wait_txns[table_id].done = true;
        }
    }
    int delete_index_ddlwork_region_info(int64_t table_id);
    int delete_index_ddlwork_info(int64_t table_id, const pb::DdlWorkInfo& work_info);
    int update_index_ddlwork_region_info(const pb::RegionDdlWork& work);

    int raft_update_info(const pb::MetaManagerRequest& request,
                            const int64_t apply_index,
                            braft::Closure* done);

    int apply_raft(const pb::MetaManagerRequest& request);

    void delete_ddlwork(const pb::MetaManagerRequest& request, braft::Closure* done); 
    void get_index_ddlwork_info(const pb::QueryRequest* request, pb::QueryResponse* response);    

    void clear_txn_info() {
        BAIDU_SCOPED_LOCK(_txn_mutex);
        _wait_txns.clear();
    }

    void get_ddlwork_info(int64_t table_id, pb::QueryResponse* query_response);

    void on_leader_start();

    bool check_table_has_ddlwork(int64_t table_id) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        return _table_ddl_mem.count(table_id) > 0;
    }

private:
    int update_ddl_status(bool is_suspend, int64_t table_id);

    int32_t get_doing_work_number(int64_t table_id) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        auto it = _table_ddl_mem.find(table_id);
        if (it == _table_ddl_mem.end()) {
            DB_FATAL("not find table %ld", table_id);
            return -1;
        }
        return it->second.doing_work;
    } 

    int32_t increase_doing_work_number(int64_t table_id) {
        return change_doing_work_number(table_id, 1);
    }

    int32_t decrease_doing_work_number(int64_t table_id) {
        return change_doing_work_number(table_id, -1);
    }

    int32_t change_doing_work_number(int64_t table_id, int32_t diff) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        auto it = _table_ddl_mem.find(table_id);
        if (it == _table_ddl_mem.end()) {
            DB_FATAL("not find table %ld", table_id);
            return 0;
        }
        auto current = it->second.doing_work;
        it->second.doing_work = current + diff;
        if (it->second.doing_work <= 0) {
            it->second.doing_work = 0;
        }
        DB_NOTICE("doing work %d", it->second.doing_work);
        return it->second.doing_work;
    }

    void update_table_ddl_mem(const baikaldb::pb::DdlWorkInfo& info) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        auto it = _table_ddl_mem.find(info.table_id());
        if (it != _table_ddl_mem.end()) {
            it->second.work_info = info;
        }
    }

    bool get_ddl_mem(int64_t table_id, baikaldb::pb::DdlWorkInfo& work_info) {
        BAIDU_SCOPED_LOCK(_table_mutex);
        auto it = _table_ddl_mem.find(table_id);
        if (it != _table_ddl_mem.end()) {
            work_info = it->second.work_info;
            return true;
        }
        return false;
    }

    bool exist_wait_txn_info(int64_t table_id) {
        BAIDU_SCOPED_LOCK(_txn_mutex);
        return _wait_txns.count(table_id) == 1;
    }

    void set_wait_txn_info(int64_t table_id, pb::DdlWorkInfo& work_info) {
        BAIDU_SCOPED_LOCK(_txn_mutex);
        WaitTxnWorkInfo info;
        info.work_info = work_info;
        _wait_txns[table_id] = info;
    }

    bool is_txn_done(int64_t table_id) {
        BAIDU_SCOPED_LOCK(_txn_mutex);
        auto iter = _wait_txns.find(table_id);
        if (iter == _wait_txns.end()) {
            return false;
        }
        return iter->second.done;
    }

    bool is_txn_success(int64_t table_id) {
        BAIDU_SCOPED_LOCK(_txn_mutex);
        auto iter = _wait_txns.find(table_id);
        if (iter == _wait_txns.end()) {
            return false;
        }
        return iter->second.success;
    }

    void erase_txn_info(int64_t table_id) {
        BAIDU_SCOPED_LOCK(_txn_mutex);
        _wait_txns.erase(table_id);
    }
    struct MemDdlInfo {
        pb::DdlWorkInfo work_info;
        int32_t doing_work = 0;
    };
    //table_id -> table ddl info
    std::unordered_map<int64_t, MemDdlInfo> _table_ddl_mem;
    std::unordered_map<int64_t, MemDdlInfo> _table_ddl_done_mem;
    bthread_mutex_t        _table_mutex;

    using MemRegionDdlWorkMapPtr = std::shared_ptr<ThreadSafeMap<int64_t, MemRegionDdlWork>>;
    // table_id -> table ddl region info
    std::unordered_map<int64_t, MemRegionDdlWorkMapPtr> _region_ddlwork;
    bthread_mutex_t        _region_mutex;

    Bthread _work_thread;
    StatusChangePolicy _update_policy;

    std::unordered_map<int64_t, WaitTxnWorkInfo> _wait_txns;
    bthread_mutex_t        _txn_mutex;
    MetaStateMachine* _meta_state_machine {nullptr};
    std::atomic<bool> _shutdown {false};
};
}