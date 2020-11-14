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

#include <stdint.h>
#include <fstream>
#include <atomic>
#include <boost/lexical_cast.hpp>
#ifdef BAIDU_INTERNAL
#include <base/iobuf.h>
#include <base/containers/bounded_queue.h>
#include <base/time.h>
#include <raft/raft.h>
#include <raft/util.h>
#include <raft/storage.h>
#else
#include <butil/iobuf.h>
#include <butil/containers/bounded_queue.h>
#include <butil/time.h>
#include <braft/raft.h>
#include <braft/util.h>
#include <braft/storage.h>
#endif
#include "common.h"
#include "schema_factory.h"
#include "table_key.h"
#include "mut_table_key.h"
#include "rocks_wrapper.h"
#include "split_compaction_filter.h"
#include "proto/common.pb.h"
#include "proto/meta.interface.pb.h"
#include "proto/store.interface.pb.h"
#include "reverse_index.h"
#include "transaction_pool.h"
//#include "region_resource.h"
#include "runtime_state.h"
#include "runtime_state_pool.h"
#include "rapidjson/document.h"
#include "rocksdb_file_system_adaptor.h"
#include "region_control.h"
#include "meta_writer.h"
#include "rpc_sender.h"
#include "ddl_common.h"
#include "exec_node.h"
#include "backup.h"

using google::protobuf::Message;
using google::protobuf::RepeatedPtrField;

namespace baikaldb {
DECLARE_int64(disable_write_wait_timeout_us);
DECLARE_int32(prepare_slow_down_wait);

static const int32_t RECV_QUEUE_SIZE = 128;
struct StatisticsInfo {
    int64_t time_cost_sum;
    int64_t end_time_us;
};

enum BinlogType {
    PREWRITE_BINLOG,
    COMMIT_BINLOG,
    ROLLBACK_BINLOG,
    FAKE_BINLOG
};

inline const char* binlog_type_name(const BinlogType type) {
    if (type == PREWRITE_BINLOG) {
        return "PREWRITE_BINLOG";
    } else if (type == COMMIT_BINLOG) {
        return "COMMIT_BINLOG";
    } else if (type == ROLLBACK_BINLOG) {
        return "ROLLBACK_BINLOG";
    } else {
        return "FAKE_BINLOG";
    }
}

struct BinlogDesc {
    int64_t primary_region_id = 0;
    int64_t txn_id;
    BinlogType binlog_type;
    TimeCost time;
};

struct ApproximateInfo {
    int64_t table_lines = 0;
    uint64_t region_size = 0;
    TimeCost time_cost;
    //上次分裂的大小，分裂后不做compaction，则新的大小不会变化
    //TODO：是否持久化存储，重启后，新老大小差不多则可以做compaction
    uint64_t last_version_region_size = 0;
    uint64_t last_version_table_lines = 0;
    TimeCost last_version_time_cost;
};

class region;
class ScopeProcStatus {
public:
    ScopeProcStatus(Region* region) : _region(region) {}
    ~ScopeProcStatus();
    void reset() {
        _region = NULL;
    }
private:
    Region* _region;
};
class ScopeMergeStatus {
public:
    ScopeMergeStatus(Region* region) : _region(region) {}
    ~ScopeMergeStatus();
    void reset() {
        _region = NULL;
    }
private:
    Region* _region;
};
class TransactionPool;
typedef std::shared_ptr<Region> SmartRegion;
class Region : public braft::StateMachine, public std::enable_shared_from_this<Region> {
friend class RegionControl;
friend class Backup;
public:
    static const uint8_t PRIMARY_INDEX_FLAG;
    static const uint8_t SECOND_INDEX_FLAG;

    virtual ~Region() {
        shutdown();
        join();
        for (auto& pair : _reverse_index_map) {
            delete pair.second;
        }
        bthread_mutex_destroy(&_commit_meta_mutex);
        bthread_mutex_destroy(&_commit_ts_map_lock);
    }

    void shutdown() {
        bool expected_status = false;
        if (_shutdown.compare_exchange_strong(expected_status, true)) {
            _node.shutdown(NULL);
            _init_success = false;
            DB_WARNING("raft node was shutdown, region_id: %ld", _region_id);
        }
    }

    void join() {
        _node.join();
        DB_WARNING("raft node join completely, region_id: %ld", _region_id);
        _multi_thread_cond.wait();
        DB_WARNING("_multi_thread_cond wait success, region_id: %ld", _region_id);
        _txn_pool.close();
    }
    void get_node_status(braft::NodeStatus* status) { 
        _node.get_status(status);
    } 

    Region(RocksWrapper* rocksdb, 
            SchemaFactory*  factory,
            const std::string& address,
            const braft::GroupId& groupId,
            const braft::PeerId& peerId,
            const pb::RegionInfo& region_info, 
            int64_t region_id) :
                _rocksdb(rocksdb),
                _factory(factory),
                _address(address),
                _region_info(region_info),
                _region_id(region_id),
                _statistics_queue(_statistics_items,
                    RECV_QUEUE_SIZE * sizeof(StatisticsInfo), butil::NOT_OWN_STORAGE),
                _qps(1),
                _average_cost(50000),
                _node(groupId, peerId),
                _is_leader(false),
                _shutdown(false),
                _num_table_lines(0),
                _num_delete_lines(0),
                _region_control(this, region_id),
                _snapshot_adaptor(new RocksdbFileSystemAdaptor(region_id)){
        //create table and add peer请求状态初始化都为IDLE, 分裂请求状态初始化为DOING
        bthread_mutex_init(&_commit_meta_mutex, NULL);
        bthread_mutex_init(&_commit_ts_map_lock, NULL);
        _region_control.store_status(_region_info.status());
        _is_global_index = _region_info.has_main_table_id() &&
                   _region_info.main_table_id() != 0 &&
                   region_info.table_id() != _region_info.main_table_id();
        if (_region_info.has_is_binlog_region()) {
            _is_binlog_region = _region_info.is_binlog_region();
        }
    }

    int init(bool new_region, int32_t snapshot_times);
    void wait_table_info() {
        while (!SchemaFactory::get_instance()->exist_tableid(get_table_id())) {
            DB_WARNING("region_id: %ld wait for table_info: %ld", _region_id, get_table_id());
            bthread_usleep(1000 * 1000);
        }
    }

    void raft_control(google::protobuf::RpcController* controller,
            const pb::RaftControlRequest* request,
            pb::RaftControlResponse* response,
            google::protobuf::Closure* done) {
        _region_control.raft_control(controller, request, response, done);
    };

    void query(google::protobuf::RpcController* controller,
            const pb::StoreReq* request,
            pb::StoreRes* response,
            google::protobuf::Closure* done);

    void query_binlog(google::protobuf::RpcController* controller,
        const pb::StoreReq* request,
        pb::StoreRes* response,
        google::protobuf::Closure* done); 

    void dml(const pb::StoreReq& request, 
            pb::StoreRes& response,
            int64_t applied_index, 
            int64_t term, bool need_txn_limit);

    void dml_2pc(const pb::StoreReq& request, 
            pb::OpType op_type, 
            const pb::Plan& plan,
            const RepeatedPtrField<pb::TupleDescriptor>& tuples, 
            pb::StoreRes& response,
            int64_t applied_index, 
            int64_t term,
            int seq_id, bool need_txn_limit);

    void dml_1pc(const pb::StoreReq& request, 
            pb::OpType op_type, 
            const pb::Plan& plan,
            const RepeatedPtrField<pb::TupleDescriptor>& tuples, 
            pb::StoreRes& response,
            int64_t applied_index,
            int64_t term);

    void select(const pb::StoreReq& request, pb::StoreRes& response);
    void select(const pb::StoreReq& request, 
            const pb::Plan& plan,
            const RepeatedPtrField<pb::TupleDescriptor>& tuples,
            pb::StoreRes& response);
    int select_normal(RuntimeState& state, ExecNode* root, pb::StoreRes& response);
    int select_sample(RuntimeState& state, ExecNode* root, const pb::AnalyzeInfo& analyze_info, pb::StoreRes& response); 
    virtual void on_apply(braft::Iterator& iter);
   
    virtual void on_shutdown();
    virtual void on_leader_start();
    virtual void on_leader_start(int64_t term);
    virtual void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done);
    
    virtual int on_snapshot_load(braft::SnapshotReader* reader);
    
    virtual void on_leader_stop();
    virtual void on_leader_stop(const butil::Status& status);
    
    virtual void on_error(const ::braft::Error& e);

    virtual void on_configuration_committed(const ::braft::Configuration& conf);
    
    virtual void on_configuration_committed(const ::braft::Configuration& conf, int64_t index);

    void snapshot(braft::Closure* done);
    void on_snapshot_load_for_restart(braft::SnapshotReader* reader,
            std::map<int64_t, std::string>& prepared_log_entrys);

    void construct_heart_beat_request(pb::StoreHeartBeatRequest& request, bool need_peer_balance, 
        std::set<int64_t>& ddl_wait_doing_table_ids); 

    void construct_peers_status(pb::LeaderHeartBeat* leader_heart);
  
    void set_can_add_peer();
    
    //leader收到从metaServer心跳包中的解析出来的add_peer请求
    void add_peer(const pb::AddPeer& add_peer, SmartRegion region, ExecutionQueue& queue) {
        _region_control.add_peer(add_peer, region, queue);
    }

    RegionControl& get_region_control() {
        return _region_control;
    }

    void remove_readonly_txn(Transaction* txn) {
        if (txn->seq_id() == 1) {
            DB_WARNING("region_id:%ld txn_id: %lu need rollback", _region_id, txn->txn_id());
            _txn_pool.remove_txn(txn->txn_id(), false);
        }
    }

    void add_peer(const pb::AddPeer* request,  
            pb::StoreRes* response, 
            google::protobuf::Closure* done) {
        _region_control.add_peer(request, response, done);
    }

    void do_snapshot() {
        _region_control.sync_do_snapshot();
    }
    int transfer_leader(const pb::TransLeaderRequest& trans_leader_request) {
        return _region_control.transfer_leader(trans_leader_request);
    }
    void reset_region_status () {
        _region_control.reset_region_status();
    }

    void reset_snapshot_status();
    
    pb::RegionStatus get_status() const {
        return _region_control.get_status();
    }

    //int clear_data();
    void compact_data_in_queue();
    int ingest_snapshot_sst(const std::string& dir); 
    int ingest_sst(const std::string& data_sst_file, const std::string& meta_sst_file); 
    // other thread
    void reverse_merge();
    // other thread
    void ttl_remove_expired_data();

    // dump the the tuples in this region in format {{k1:v1},{k2:v2},{k3,v3}...}
    // used for debug
    std::string dump_hex();
   
    //on_apply里调用的方法 
    void start_split(braft::Closure* done, int64_t applied_index, int64_t term);
    void start_split_for_tail(braft::Closure* done, int64_t applied_index, int64_t term);
    void validate_and_add_version(const pb::StoreReq& request, braft::Closure* done, int64_t applied_index, int64_t term);
    void add_version_for_split_region(const pb::StoreReq& request, braft::Closure* done, int64_t applied_index, int64_t term);
    void apply_txn_request(const pb::StoreReq& request, braft::Closure* done, int64_t index, int64_t term);
    void adjustkey_and_add_version(const pb::StoreReq& request, 
                                           braft::Closure* done, 
                                           int64_t applied_index, 
                                           int64_t term);
    
    void adjustkey_and_add_version_query(google::protobuf::RpcController* controller,
            const pb::StoreReq* request, 
            pb::StoreRes* response, 
            google::protobuf::Closure* done);
    //开始做merge操作
    void start_process_merge(const pb::RegionMergeResponse& merge_response);
    //开始做split操作
    //第一步通过raft状态机,创建迭代器，取出当前的index,自此之后的log不能再删除
    void start_process_split(const pb::RegionSplitResponse& split_response,
            bool tail_split,
            const std::string& split_key);
    void get_split_key_for_tail_split();

    void split_region_do_first_snapshot();
    
    //split第二步，发送迭代器数据
    void write_local_rocksdb_for_split();

    int replay_txn_for_recovery(
            const std::unordered_map<uint64_t, pb::TransactionInfo>& prepared_txn);

    int replay_txn_for_recovery(
            int64_t region_id,
            const std::string& instance,
            std::string start_key,
            const std::unordered_map<uint64_t, pb::TransactionInfo>& prepared_txn);

    void send_log_entry_to_new_region_for_split();
    //split 第三步， 通知被分裂出来的region分裂完成， 增加old_region的version, update end_key
    void send_complete_to_new_region_for_split(); 
    //分裂第四步完成
    void complete_split();
    
    //从split开始之后所有的entry数据作为分裂的增量部分
    // 1说明还有数据，0说明到头了
    int get_log_entry_for_split(const int64_t start_index, 
            const int64_t expected_term,
            std::vector<pb::StoreReq>& requests, 
            int64_t& split_end_index);
    
    int get_split_key(std::string& split_key);
    
    int64_t get_region_id() const {
        return _region_id;
    }

    void update_average_cost(int64_t request_time_cost);

    void reset_split_status() {
        _split_param.split_start_index = INT_FAST64_MAX;
        _split_param.split_end_index = 0;
        _split_param.split_term = 0;
        _split_param.new_region_id = 0;
        _split_param.split_slow_down = false;
        _split_param.split_slow_down_cost = 0;
        _split_param.err_code = 0;
        _split_param.split_key = "";
        _split_param.instance = "";
        _split_param.reduce_num_lines = 0;
        if (_split_param.snapshot != nullptr) {
            _rocksdb->get_db()->ReleaseSnapshot(_split_param.snapshot);
        }
        _split_param.tail_split = false;
        _split_param.snapshot = nullptr;
        _split_param.prepared_txn.clear();
    }

    void real_writing_decrease() {
        _real_writing_cond.decrease_signal();
    }
    void reset_allow_write() {
        _disable_write_cond.decrease_broadcast();
    }

    void set_disable_write() {
        _disable_write_cond.increase();
    }

    int32_t num_prepared() {
        return _txn_pool.num_prepared();
    }
    int32_t num_began() {
        return _txn_pool.num_began();
    }

    int64_t get_split_index() {
        return _split_param.split_start_index;
    }
    void set_used_size(int64_t used_size) {
        _region_info.set_used_size(used_size);
    }
    std::string get_start_key() {
        std::lock_guard<std::mutex> lock(_region_lock);
        return _region_info.start_key();
    }
    std::string get_end_key() {
        std::lock_guard<std::mutex> lock(_region_lock);
        return _region_info.end_key();
    }
    int64_t get_partition_num() {
        std::lock_guard<std::mutex> lock(_region_lock);
        if (_region_info.has_partition_num()) {
            return _region_info.partition_num();
        }
        return 1;
    }
    rocksdb::Range get_rocksdb_range() {
        return rocksdb::Range(_rocksdb_start, _rocksdb_end);
    }
    bool is_merged() {
        std::lock_guard<std::mutex> lock(_region_lock);
        if (!_region_info.start_key().empty()) {
            return _region_info.start_key() == _region_info.end_key();
        }
        return false;
    }
    int64_t get_log_index() const {
        return _applied_index;
    }
    int64_t get_data_index() const {
        return _data_index;
    }
    int64_t get_log_index_lastcycle() const {
        return _applied_index_lastcycle;
    }
    void reset_log_index_lastcycle() {
        _applied_index_lastcycle = _applied_index;
        _lastcycle_time_cost.reset();
    } 
    int64_t get_lastcycle_timecost() const {
        return _lastcycle_time_cost.get_time();
    }   
    int64_t get_last_split_time_cost() const {
        return _last_split_time_cost.get_time();
    }   
    rocksdb::ColumnFamilyHandle* get_data_cf() const {
        return _data_cf;
    }
    butil::EndPoint get_leader() {
        return _node.leader_id().addr;    
    }
    
    int64_t get_used_size() {
        return _region_info.used_size();
    }
    int64_t get_table_id() {
        if (_is_global_index) {
            return _region_info.main_table_id();
        }
        return _region_info.table_id();
    }
    int64_t get_global_index_id() {
        return _region_info.table_id();
    }
    bool is_leader() {
        return (_is_leader.load() && _node.is_leader());
    }
    void leader_start() {
        _is_leader.store(true);
        DB_WARNING("leader real start, region_id: %ld", _region_id);
    }
    int64_t get_version() {
        return _region_info.version();
    }
    pb::RegionInfo& region_info() {
        return _region_info;
    }
    bool check_region_legal_complete();

    bool compare_and_set_illegal() {
        std::unique_lock<std::mutex> lock(_legal_mutex);
        if (_region_info.version() <= 0) {
            _legal_region = false;
            return true;
        }
        return false;
    }

    bool compare_and_set_legal_for_split() {
        std::unique_lock<std::mutex> lock(_legal_mutex);
        if (_legal_region) {
            _region_info.set_version(1);
            DB_WARNING("compare and set split verison to 1, region_id: %ld", _region_id);
            return true;
        }
        return false;
    }
    bool compare_and_set_legal() {
        std::unique_lock<std::mutex> lock(_legal_mutex);
        if (_legal_region) {
            return true;
        }
        return false;
    }
    
    int64_t get_num_table_lines() {
        return _num_table_lines.load();
    }

    bool is_tail() {
        return  (!_region_info.has_end_key() || _region_info.end_key().empty());
    }
    
    bool is_head() {
        return (!_region_info.has_start_key() || _region_info.start_key().empty());
    }
    
    bool empty() {
        return (_region_info.start_key() == _region_info.end_key() && !is_tail() && !is_head());
    }
    
    int64_t get_timecost() {
        return _time_cost.get_time();
    }
    
    void reset_timecost() {
        return _time_cost.reset();
    }

    int64_t get_qps() {
        return _qps.load();
    }
    int64_t get_average_cost() {
        return _average_cost.load(); 
    }
    void set_num_table_lines(int64_t table_line) {
        MetaWriter::get_instance()->update_num_table_lines(_region_id, table_line);
        _num_table_lines.store(table_line);
        DB_WARNING("region_id: %ld, table_line:%ld", _region_id, _num_table_lines.load());
    }
    bool removed() const {
        return _removed;
    }
    bool is_binlog_region() const { return _is_binlog_region; }
    void set_removed(bool removed) {
        _removed = removed;
        _removed_time_cost.reset();
    }

    int64_t removed_time_cost() const {
        return _removed_time_cost.get_time();
    }

    int64_t get_split_wait_time() {
        int64_t wait_time = FLAGS_disable_write_wait_timeout_us;
        if (FLAGS_disable_write_wait_timeout_us < _split_param.split_slow_down_cost * 10) {
            wait_time = _split_param.split_slow_down_cost * 10;
        }
        if (wait_time > 30 * 1000 * 1000LL) {
            //DB_WARNING("split wait time exceed 30s, region_id: %ld", _region_id);
            wait_time = 30 * 1000 * 1000LL;
        }
        return wait_time;
    }

    void exec_in_txn_query(google::protobuf::RpcController* controller,
            const pb::StoreReq* request, 
            pb::StoreRes* response, 
            google::protobuf::Closure* done);

    void exec_out_txn_query(google::protobuf::RpcController* controller,
            const pb::StoreReq* request, 
            pb::StoreRes* response, 
            google::protobuf::Closure* done);

    void exec_txn_query_primary_region(google::protobuf::RpcController* controller,
            const pb::StoreReq* request,
            pb::StoreRes* response,
            google::protobuf::Closure* done);

    void exec_txn_complete(google::protobuf::RpcController* controller,
            const pb::StoreReq* request,
            pb::StoreRes* response,
            google::protobuf::Closure* done);

    void exec_txn_query_state(google::protobuf::RpcController* controller,
            const pb::StoreReq* request,
            pb::StoreRes* response,
            google::protobuf::Closure* done);
    
    void exec_dml_out_txn_query(const pb::StoreReq* request, 
                                        pb::StoreRes* response, 
                                        google::protobuf::Closure* done);
    
    int execute_cached_cmd(const pb::StoreReq& request, pb::StoreRes& response, 
            uint64_t txn_id, 
            SmartTransaction& txn, 
            int64_t applied_index, 
            int64_t term, 
            uint64_t log_id = 0);

    void clear_transactions() {
        if (_shutdown || !_init_success || get_version() <= 0) {
            return;
        }
        _multi_thread_cond.increase();
        _txn_pool.clear_transactions(this);
        _multi_thread_cond.decrease_signal();
    }
    void recovery_when_leader_start(std::map<uint64_t, SmartTransaction> replay_txns);

    TransactionPool& get_txn_pool() {
        return _txn_pool;
    }

    void start_thread_to_remove_region(int64_t drop_region_id, std::string instance_address) {
        Bthread bth(&BTHREAD_ATTR_SMALL);
        std::function<void()> remove_region_function =
            [this, drop_region_id, instance_address]() {
                _multi_thread_cond.increase();
                RpcSender::send_remove_region_method(drop_region_id, instance_address);
                _multi_thread_cond.decrease_signal();
            };
        bth.run(remove_region_function);
    }
    void set_restart(bool restart) {
        _restart = restart;
    }
    //现在支持replica_num的修改，从region_info里去replica_num已经不准确
    //bool peers_stable() {
    //    std::vector<braft::PeerId> peers;
    //    return _node.list_peers(&peers).ok() && peers.size() >= (size_t)_region_info.replica_num();
    //}
    void copy_region(pb::RegionInfo* region_info) {
        std::lock_guard<std::mutex> lock(_region_lock);
        region_info->CopyFrom(_region_info);
    }
    void kv_apply_raft(RuntimeState* state, SmartTransaction txn);
    void set_separate_switch(bool is_separate) {
        _storage_compute_separate = is_separate;
    }
    void lock_commit_meta_mutex() {
        bthread_mutex_lock(&_commit_meta_mutex); 
    }
    void unlock_commit_meta_mutex() {
        bthread_mutex_unlock(&_commit_meta_mutex);
    }

    void put_commit_ts(const uint64_t txn_id, int64_t commit_ts) {
        std::unique_lock<bthread_mutex_t> lck(_commit_ts_map_lock);
        _commit_ts_map[txn_id] = commit_ts;
        if (_commit_ts_map.size() > 100000) {
            // 一天阈值
            int64_t threshold_value = commit_ts - 86400000LL;
            auto iter = _commit_ts_map.begin();
            while (iter != _commit_ts_map.end()) {
                if (iter->second < threshold_value) {
                    iter = _commit_ts_map.erase(iter);
                } else {
                    ++iter;
                }
            }
        }
    }

    int64_t get_commit_ts(uint64_t txn_id, int64_t start_ts) {
        std::unique_lock<bthread_mutex_t> lck(_commit_ts_map_lock);
        if (_commit_ts_map.count(txn_id) == 0) {
            return -1;
        }
        return _commit_ts_map[txn_id];
    }


    int ddlwork_process(const pb::DdlWorkInfo& store_ddl_work);
    int ddlwork_common_init_process(const pb::DdlWorkInfo& store_ddl_work);
    void ddlwork_finish_check_process(std::set<int64_t>& ddlwork_table_ids);

    void start_add_index();
    void write_local_rocksdb_for_ddl();
    int ddlwork_add_index_process();

    void start_drop_index();
    void delete_local_rocksdb_for_ddl();
    int ddlwork_del_index_process();

    bool is_wait_ddl();
    int add_reverse_index();
    int ddl_schema_state(pb::IndexState& state);

    void ddlwork_rollback(pb::ErrCode errcode, bool& is_success) {
        BAIDU_SCOPED_LOCK(_region_ddl_lock);
        if (_region_ddl_info.ddlwork_infos_size() > 0) {
            _region_ddl_info.mutable_ddlwork_infos(0)->set_errcode(errcode);
            _region_ddl_info.mutable_ddlwork_infos(0)->set_rollback(true);
        } else {
            DB_FATAL("DDL_LOG region_%lld ddlwork_infos_size is zero, rollback failed.", _region_id);
        }
        is_success = false;
    }

    void process_download_sst(brpc::Controller* controller, 
        std::vector<std::string>& req_vec, SstBackupType type);
    void process_upload_sst(brpc::Controller* controller, bool is_ingest);

    void process_download_sst_streaming(brpc::Controller* controller, 
        const pb::BackupRequest* request,
        pb::BackupResponse* response);

    void process_upload_sst_streaming(brpc::Controller* controller, bool is_ingest,
        const pb::BackupRequest* request,
        pb::BackupResponse* response);
    
    std::shared_ptr<Region> get_ptr() {
        return shared_from_this();
    }
    uint64_t get_approx_size() const {
        //分裂后一段时间每超过10分钟，或者超过10%的数据量diff则需要重新获取
        if (_approx_info.time_cost.get_time() > 10 * 60 * 1000 * 1000LL && 
            _approx_info.last_version_time_cost.get_time() < 2 * 60 * 60 * 1000 * 1000LL) {
            return UINT64_MAX;
        } else {
            int64_t diff_lines = abs(_num_table_lines.load() - _approx_info.table_lines);
            if (diff_lines * 10 > _num_table_lines.load()) {
                return UINT64_MAX;
            }
        }
        return _approx_info.region_size;
    }
    void set_approx_size(uint64_t region_size) {
        _approx_info.time_cost.reset();
        _approx_info.table_lines = _num_table_lines.load();
        _approx_info.region_size = region_size;
    }

    bool can_use_approximate_split();

    void binlog_scan();

    void binlog_timeout_check(int64_t rollback_ts);
    
    void binlog_fake(int64_t ts, BthreadCond& cond);

private:
    struct SplitParam {
        int64_t split_start_index = INT_FAST64_MAX;
        int64_t split_end_index = 0;
        int64_t split_term = 0;
        int64_t new_region_id = 0;
        int64_t reduce_num_lines = 0;  //非精确，todo需要精确计数
        bool    split_slow_down = false;
        int64_t split_slow_down_cost = 0;
        int     err_code = 0;
        std::string split_key;
        //std::string old_end_key;
        std::string instance;
        TimeCost total_cost;
        TimeCost no_write_time_cost;
        int64_t new_region_cost;
        
        TimeCost op_start_split;
        int64_t op_start_split_cost;
        TimeCost op_start_split_for_tail;
        int64_t op_start_split_for_tail_cost;
        TimeCost op_snapshot;
        int64_t op_snapshot_cost;
        int64_t write_sst_cost;
        int64_t send_first_log_entry_cost;
        int64_t write_wait_cost;
        int64_t send_second_log_entry_cost;
        int64_t send_complete_to_new_region_cost;
        TimeCost op_add_version;
        int64_t op_add_version_cost;
        const rocksdb::Snapshot* snapshot = nullptr;

        bool tail_split = false;
        std::unordered_map<uint64_t, pb::TransactionInfo> prepared_txn;
    };

    struct BinlogParam {
        std::map<int64_t, BinlogDesc> ts_binlog_map;
        int64_t min_ts_in_map  = -1;
        int64_t max_ts_in_map  = -1;
        int64_t check_point_ts = -1;
        int64_t oldest_ts      = -1;
    };

        //binlog function
    void read_binlog(const pb::StoreReq* request, pb::StoreRes* response);
    void apply_binlog(const pb::StoreReq& request, braft::Closure* done);
    int write_binlog_record(SmartRecord record);
    int write_binlog_value(std::map<std::string, ExprValue> field_value_map);
    int64_t binlog_get_int64_val(const std::string& name, const std::map<std::string, ExprValue>& field_value_map);
    
    std::string binlog_get_str_val(const std::string& name, const std::map<std::string, ExprValue>& field_value_map);
    
    void binlog_get_scan_fields(std::map<int32_t, FieldInfo*>& field_ids, std::vector<int32_t>& field_slot);
    void binlog_get_field_values(std::map<std::string, ExprValue>& field_value_map, SmartRecord record);
    void binlog_reset_on_snapshot_load_restart();
    
    void binlog_reset_on_snapshot_load();
    void binlog_update_map_when_scan(const std::map<std::string, ExprValue>& field_value_map);
    int binlog_update_map_when_apply(const std::map<std::string, ExprValue>& field_value_map);
    int binlog_update_check_point();
    int get_primary_region_info(int64_t primary_region_id, pb::RegionInfo& region_info);
    
    void binlog_query_primary_region(const int64_t& start_ts, const int64_t& txn_id, pb::RegionInfo& region_info, int64_t rollback_ts);
    void binlog_fill_exprvalue(const pb::BinlogDesc& binlog_desc, pb::OpType op_type, std::map<std::string, ExprValue>& field_value_map);
    //binlog end
    void apply_kv_in_txn(const pb::StoreReq& request, braft::Closure* done, 
                         int64_t index, int64_t term);

    void apply_kv_out_txn(const pb::StoreReq& request, braft::Closure* done, 
                                  int64_t index, int64_t term);
    void apply_kv_split(const pb::StoreReq& request, braft::Closure* done, 
                                int64_t index, int64_t term);
    bool validate_version(const pb::StoreReq* request, pb::StoreRes* response);

    void set_region(const pb::RegionInfo& region_info) {
        std::lock_guard<std::mutex> lock(_region_lock);
        _region_info.CopyFrom(region_info);
    }
    void set_region_with_update_range(const pb::RegionInfo& region_info) {
        std::lock_guard<std::mutex> lock(_region_lock);
        _region_info.CopyFrom(region_info);
        // region_info更新range，替换resource
        std::shared_ptr<RegionResource> new_resource(new RegionResource);
        *new_resource = *_resource;
        new_resource->region_info = region_info;
        {
            BAIDU_SCOPED_LOCK(_ptr_mutex);
            _resource = new_resource;
        }
        //compaction时候删掉多余的数据
        if (_is_binlog_region) {
            //binlog region把start key和end key设置为空，防止filter把数据删掉
            SplitCompactionFilter::get_instance()->set_range_key(
                    _region_id, "", "");
        } else {
            SplitCompactionFilter::get_instance()->set_range_key(
                    _region_id,
                    region_info.start_key(),
                    region_info.end_key());
        }
        DB_WARNING("region_id: %ld, start_ke: %s, end_key: %s", _region_id, 
            rocksdb::Slice(region_info.start_key()).ToString(true).c_str(), 
            rocksdb::Slice(region_info.end_key()).ToString(true).c_str());
    }

    void set_region_ddl(const pb::StoreRegionDdlInfo& region_ddl_info) {
        std::lock_guard<std::mutex> lock(_region_ddl_lock);
        _region_ddl_info.CopyFrom(region_ddl_info);
    }

    // if seek_table_lines != nullptr, seek all sst for seek_table_lines
    bool has_sst_data(int64_t* seek_table_lines);
    bool ingest_has_sst_data();
    void wait_rocksdb_normal() {
        while (_rocksdb->is_any_stall()) {
            reset_timecost();
            bthread_usleep(100 * 1000 * 1000);
        }
    }
private:
    //Singleton
    RocksWrapper*       _rocksdb;
    SchemaFactory*      _factory;
    rocksdb::ColumnFamilyHandle* _data_cf;    
    rocksdb::ColumnFamilyHandle* _meta_cf;    
    std::string         _address; //ip:port
    
    //region metainfo
    pb::RegionInfo      _region_info;
    std::mutex          _region_lock;    
    //split后缓存分裂出去的region信息供baikaldb使用
    std::vector<pb::RegionInfo> _new_region_infos;
    pb::RegionInfo      _new_region_info;
    int64_t             _region_id;
    
    //merge后该region为空，记录目标region，供baikaldb使用，只会merge一次，不必使用vector
    pb::RegionInfo      _merge_region_info;
    // 倒排索引需要
    // todo liguoqiang  如何初始化这个
    std::map<int64_t, ReverseIndexBase*> _reverse_index_map;
    
    // todo 是否可以改成无锁的
    BthreadCond _disable_write_cond;
    BthreadCond _real_writing_cond;
    SplitParam _split_param;
    DllParam _ddl_param;

    std::mutex _legal_mutex;
    bool       _legal_region = true;

    TimeCost                        _time_cost; //上次收到请求的时间，每次收到请求都重置一次
    std::mutex                      _queue_lock;    
    butil::BoundedQueue<StatisticsInfo> _statistics_queue;
    StatisticsInfo _statistics_items[RECV_QUEUE_SIZE];
    std::atomic<int64_t> _qps;
    std::atomic<int64_t> _average_cost;
    bool                                _restart = false;
    //计算存储分离开关，在store定时任务中更新，避免每次dml都访问schema factory
    bool                                _storage_compute_separate = false;
    bool                                _use_ttl = false; //init时更新，表的ttl后续不会改变
    bool                                _reverse_remove_range = false; //split的数据，把拉链过滤一遍
    //raft node
    braft::Node                         _node;
    std::atomic<bool>                   _is_leader;
    int64_t                             _applied_index = 0;  //current log index
    // 表示数据版本，conf_change,no_op等不影响数据时版本不变
    int64_t                             _data_index = 0;  
    // bthread cycle: set _applied_index_lastcycle = _applied_index when _num_table_lines == 0
    int64_t                             _applied_index_lastcycle = 0;  
    TimeCost                            _lastcycle_time_cost; //定时线程上次循环的时间，更新_applied_index_lastcycle时更新
    TimeCost                            _last_split_time_cost; //上次分裂时间戳
    ApproximateInfo                     _approx_info;

    bool                                _report_peer_info = false;
    std::atomic<bool>                   _shutdown;
    bool                                _init_success = false;
    bool                                _can_heartbeat = false;

    BthreadCond                         _multi_thread_cond;
    // region stat variables
    // TODO:num_table_lines维护太麻烦，后续要考虑使用预估的方式获取
    std::atomic<int64_t>                _num_table_lines;  //total number of pk record in this region
    std::atomic<int64_t>                _num_delete_lines;  //total number of delete rows after last compact
    int64_t                             _snapshot_num_table_lines = 0;  //last snapshot number
    TimeCost                            _snapshot_time_cost;
    int64_t                             _snapshot_index = 0; //last snapshot log index
    bool                                _removed = false;
    TimeCost                            _removed_time_cost;
    TransactionPool                     _txn_pool;
    RuntimeStatePool                    _state_pool;

    // shared_ptr is not thread safe when assign
    std::mutex  _ptr_mutex;
    std::shared_ptr<RegionResource>     _resource;

    RegionControl                           _region_control;
    MetaWriter*                             _meta_writer = nullptr;
    bthread_mutex_t                         _commit_meta_mutex;
    scoped_refptr<braft::FileSystemAdaptor>  _snapshot_adaptor = nullptr;
    std::mutex          _region_ddl_lock;    
    pb::StoreRegionDdlInfo     _region_ddl_info;
    bool                                     _is_global_index = false; //是否是全局索引的region
    std::mutex       _reverse_index_map_lock;
    std::mutex       _backup_lock;
    Backup          _backup;
    //binlog
    bool _is_binlog_region = false; //是否为binlog region
    // txn_id:commit_ts
    std::map<uint64_t, int64_t> _commit_ts_map;
    bthread_mutex_t _commit_ts_map_lock;
    BthreadCond _binlog_cond;
    BinlogParam _binlog_param;
    std::string     _rocksdb_start;
    std::string     _rocksdb_end;
};

} // end of namespace
