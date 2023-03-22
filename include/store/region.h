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
#include <raft/snapshot_throttle.h>
#include <raft/repeated_timer_task.h>
#else
#include <butil/iobuf.h>
#include <butil/containers/bounded_queue.h>
#include <butil/time.h>
#include <braft/raft.h>
#include <braft/util.h>
#include <braft/storage.h>
#include <braft/snapshot_throttle.h>
#include <braft/repeated_timer_task.h>
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
#include "runtime_state.h"
#include "runtime_state_pool.h"
#include "rapidjson/document.h"
#include "rocksdb_file_system_adaptor.h"
#include "region_control.h"
#include "meta_writer.h"
#include "rpc_sender.h"
#include "exec_node.h"
#include "concurrency.h"
#include "backup.h"

#ifdef BAIDU_INTERNAL
#else
//开源编译，等raft learner开源后删除
#include <braft/raft.h>
namespace braft {
class Learner {
public:
Learner(const GroupId& group_id, const PeerId& peer_id) {
}
int init(const NodeOptions& options) {
    return 0;
}
void shutdown(Closure* done) {
}
void join() {
}
void snapshot(Closure* done) {
}
void get_status(NodeStatus* status) {
}
};
}
#endif

using google::protobuf::Message;
using google::protobuf::RepeatedPtrField;

namespace baikaldb {
DECLARE_int64(disable_write_wait_timeout_us);
DECLARE_int32(prepare_slow_down_wait);
DECLARE_int64(binlog_warn_timeout_minute);

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

struct MultiSplitRegion {
    int64_t  new_region_id;
    std::string  new_instance;
    std::vector<std::string> add_peer_instances;
    std::string start_key;
    std::string end_key;
    MultiSplitRegion(const pb::MultiSplitRegion& region_info) {
        new_region_id = region_info.new_region_id();
        new_instance = region_info.new_instance();
        for (auto adr : region_info.add_peer_instance()) {
            add_peer_instances.emplace_back(adr);
        }
    }
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

struct FollowerReadCond {
    BthreadCond cond;
    pb::ErrCode errcode;
    bool is_learner = false;
    FollowerReadCond(bool is_learner) : errcode(pb::SUCCESS), is_learner(is_learner) {};
    void set_failed() {
        if (is_learner) {
            errcode = pb::LEARNER_NOT_READY;
        } else {
            errcode = pb::NOT_LEADER;
        }
        cond.decrease_signal();
    }
    void finish_wait() {
        cond.decrease_signal();
    }
};
typedef std::shared_ptr<FollowerReadCond> SmartFollowerReadCond;

struct ReadReqsWaitExec {
    int64_t read_idx;
    std::vector<SmartFollowerReadCond> reqs;
    ReadReqsWaitExec(int64_t read_idx, std::vector<SmartFollowerReadCond>& reqs_vec) : read_idx(read_idx) {
        reqs.swap(reqs_vec);
    };
};

class NoOpTimer : public braft::RepeatedTimerTask {
public:
    NoOpTimer() {}
    virtual ~NoOpTimer() {}
    int init(Region* region, int timeout_ms) {
        int ret = RepeatedTimerTask::init(timeout_ms);
        if (region == nullptr) {
            return -1;
        }
        _region = region;
        return ret;
    }
    void reset_timer() {
        if (_is_running) {
            reset();
        } else {
            _is_running = true;
            start();
        }
    }
    void stop_timer() {
        _is_running = false;
        stop();
    }
    virtual void run();
protected:
    virtual void on_destroy() {};
    Region* _region = nullptr;
    bool _is_running = false;
};


class BinlogReadMgr {
public:
enum GetMode {
    GET = 0,
    MULTIGET,
    SEEK
};
    BinlogReadMgr(int64_t region_id, int64_t begin_ts, const std::string& capture_ip, uint64_t log_id, int64_t need_read_cnt);
    BinlogReadMgr(int64_t region_id, GetMode mode);
    ~BinlogReadMgr() { }
    int get_binlog_value(int64_t commit_ts, int64_t start_ts, pb::StoreRes* response, int64_t binlog_row_cnt);
    int fill_fake_binlog(int64_t fake_ts, std::string& binlog);
    int get_binlog_finish(pb::StoreRes* response);
    int binlog_add_to_response(int64_t commit_ts, const std::string& binlog_value, pb::StoreRes* response);
    int multiget(std::map<int64_t, std::string>& start_binlog_map);
    int seek(std::map<int64_t, std::string>& start_binlog_map);
    void print_log();

private:
    int64_t _region_id = 0;
    RocksWrapper* _rocksdb = nullptr;
    int64_t _oldest_ts_in_binlog_cf = 0;
    int64_t _total_binlog_size = 0;
    int64_t _bacth_size = 0;
    TimeCost _time;
    GetMode _mode = GET;
    bool _finish = false;
    bool _is_first_binlog = true;
    int _binlog_num = 0;
    int64_t _begin_ts = 0;
    uint64_t _log_id = 0;
    int64_t _need_read_cnt = 0;
    int64_t _binlog_total_row_cnts = 0;
    int64_t _fake_binlog_cnt = 0;
    int64_t _first_commit_ts = -1;
    int64_t _last_commit_ts = -1;
    std::map<int64_t, int64_t> _commit_start_map;
    std::map<int64_t, std::string> _start_binlog_map;
    std::map<int64_t, std::string> _fake_binlog_map;
    std::string _capture_ip;
};

class BinlogAlarm {
public:
struct TsAccessTime {
    int64_t ts = 0;
    TimeCost time;
};

void check_read_ts(const std::string& ip, int64_t region_id, int64_t begin_ts) {
    std::lock_guard<bthread::Mutex> l(_lock);
    auto it = _ip_ts_map.find(ip);
    if (it != _ip_ts_map.end()) {
        if (it->second.ts == begin_ts) {
            if (it->second.time.get_time() > FLAGS_binlog_warn_timeout_minute * 60 * 1000 * 1000LL) {
                // 长时间一直访问一个ts需要报警
                DB_WARNING("region_id: %ld, remote_side: %s, ts: %ld, read begin ts for a long time", region_id, ip.c_str(), begin_ts);
            }
        } else {
            it->second.ts = begin_ts;
            it->second.time.reset();
        }
    } else {
        _ip_ts_map[ip].ts = begin_ts;
    }

    // gc
    int64_t gc_interval = 5 * FLAGS_binlog_warn_timeout_minute * 60 * 1000 * 1000LL;
    static TimeCost map_gc_time;
    if (map_gc_time.get_time() > gc_interval) {
        auto it = _ip_ts_map.begin();
        while (it != _ip_ts_map.end()) {
            if (it->second.time.get_time() > gc_interval) {
                it = _ip_ts_map.erase(it);
            } else {
                ++it;
            }
        }

        map_gc_time.reset();
    }
}
private:
    bthread::Mutex _lock;
    std::map<std::string, TsAccessTime> _ip_ts_map; 
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
    }
    void wait_async_apply_log_queue_empty() {
        BthreadCond cond;
        cond.increase();
        _async_apply_log_queue.run([&cond]() {
            cond.decrease_signal();
        });
        cond.wait();
    }
    void shutdown() {
        if (get_version() == 0) {
            wait_async_apply_log_queue_empty();
            _async_apply_param.stop_adjust_stall();
        }
        if (_wait_read_idx_queue) {
            _wait_read_idx_queue->stop();
            _wait_read_idx_queue.reset();
            execution_queue_join(_wait_read_idx_queue_id);
        }
        if (_wait_exec_queue) {
            _wait_exec_queue->stop();
            _wait_exec_queue.reset();
            execution_queue_join(_wait_exec_queue_id);
        }
        _no_op_timer.stop();
        _no_op_timer.destroy();
        if (_need_decrease) {
            _need_decrease = false;
            Concurrency::get_instance()->recieve_add_peer_concurrency.decrease_broadcast();
        }
        bool expected_status = false;
        if (_shutdown.compare_exchange_strong(expected_status, true)) {
            is_learner() ? _learner->shutdown(NULL) : _node.shutdown(NULL);
            _init_success = false;
            DB_WARNING("raft node was shutdown, region_id: %ld", _region_id);
        }
    }

    bool is_shutdown() {
        return _shutdown.load();
    }

    void join() {
        is_learner() ? _learner->join() : _node.join();
        DB_WARNING("raft node join completely, region_id: %ld", _region_id);
        _real_writing_cond.wait();
        _disable_write_cond.wait();
        _multi_thread_cond.wait();
        DB_WARNING("_multi_thread_cond wait success, region_id: %ld", _region_id);
        _txn_pool.close();
    }
    void get_node_status(braft::NodeStatus* status) {
        is_learner() ? _learner->get_status(status) : _node.get_status(status);
    }

    Region(RocksWrapper* rocksdb, 
            SchemaFactory*  factory,
            const std::string& address,
            const braft::GroupId& groupId,
            const braft::PeerId& peerId,
            const pb::RegionInfo& region_info, 
            int64_t region_id,
            bool is_learner = false) :
                _rocksdb(rocksdb),
                _factory(factory),
                _address(address),
                _region_info(region_info),
                _region_id(region_id),
                _node(groupId, peerId),
                _is_leader(false),
                _shutdown(false),
                _num_table_lines(0),
                _num_delete_lines(0),
                _region_control(this, region_id),
                _snapshot_adaptor(new RocksdbFileSystemAdaptor(region_id)), _is_learner(is_learner),
                _not_leader_alarm(region_id, peerId) {
        //create table and add peer请求状态初始化都为IDLE, 分裂请求状态初始化为DOING
        _region_control.store_status(_region_info.status());
        _version = _region_info.version();
        _is_global_index = _region_info.has_main_table_id() &&
                   _region_info.main_table_id() != 0 &&
                   _region_info.table_id() != _region_info.main_table_id();
        _global_index_id = _region_info.table_id();
        _table_id = _is_global_index ? _region_info.main_table_id() : _region_info.table_id();
        if (_region_info.has_is_binlog_region()) {
            _is_binlog_region = _region_info.is_binlog_region();
        }
        if (_is_learner) {
            _learner.reset(new braft::Learner(groupId, peerId));
        }
        _region_uuid = butil::fast_rand();
    }

    int init(bool new_region, int32_t snapshot_times);
    void wait_table_info() {
        while (!_factory->exist_tableid(get_table_id())) {
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

    void async_apply_log_entry(google::protobuf::RpcController* controller,
          const pb::BatchStoreReq* request,
          pb::BatchStoreRes* response,
          google::protobuf::Closure* done);

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
            int64_t term,
            braft::Closure* done);

    int select(const pb::StoreReq& request, pb::StoreRes& response);
    int select(const pb::StoreReq& request, 
            const pb::Plan& plan,
            const RepeatedPtrField<pb::TupleDescriptor>& tuples,
            pb::StoreRes& response);
    int select_normal(RuntimeState& state, ExecNode* root, pb::StoreRes& response);
    int select_sample(RuntimeState& state, ExecNode* root, const pb::AnalyzeInfo& analyze_info, pb::StoreRes& response);
    void do_apply(int64_t term, int64_t index, const pb::StoreReq& request, braft::Closure* done);
    virtual void on_apply(braft::Iterator& iter);
   
    virtual void on_shutdown();
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

    void construct_heart_beat_request(pb::StoreHeartBeatRequest& request, bool need_peer_balance); 

    void construct_peers_status(pb::LeaderHeartBeat* leader_heart);
  
    void set_can_add_peer();
    
    //leader收到从metaServer心跳包中的解析出来的add_peer请求
    void add_peer(const pb::AddPeer& add_peer, SmartRegion region, ExecutionQueue& queue) {
        _region_control.add_peer(add_peer, region, queue);
    }

    RegionControl& get_region_control() {
        return _region_control;
    }

    void add_peer(const pb::AddPeer* request,  
            pb::StoreRes* response, 
            google::protobuf::Closure* done) {
        _region_control.add_peer(request, response, done);
    }

    void do_snapshot() {
        _region_control.sync_do_snapshot();
    }

    void transfer_leader_set_is_leader() {
        if (is_learner()) {
            return;
        }
        _is_leader.store(_node.is_leader());
        DB_WARNING("region_id: %ld, is_leader:%d", _region_id, _is_leader.load());
    }

    int transfer_leader_to(const braft::PeerId& peer) {
        if (is_learner()) {
            return -1;
        }
        int ret = _node.transfer_leadership_to(peer);
        if (ret == 0) {
            transfer_leader_set_is_leader();
        }
        return ret;
    }

    int transfer_leader(const pb::TransLeaderRequest& trans_leader_request, 
            SmartRegion region, ExecutionQueue& queue) {
        return _region_control.transfer_leader(trans_leader_request, region, queue);
    }

    int make_region_status_doing() {
        return _region_control.make_region_status_doing();
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
    int ingest_sst_backup(const std::string& data_sst_file, const std::string& meta_sst_file); 
    // other thread
    void reverse_merge();
    // other thread
    void reverse_merge_doing_ddl();
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
    void exec_update_primary_timestamp(const pb::StoreReq& request,
            braft::Closure* done, int64_t applied_index, int64_t term);
    
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
            const std::string& split_key,
            int64_t key_term);
    void get_split_key_for_tail_split();
    int init_new_region_leader(int64_t new_region_id, std::string instance, bool tail_split);

    void adjust_num_table_lines();
    //split第二步，发送迭代器数据
    void write_local_rocksdb_for_split();
    int generate_multi_split_keys();
    int tail_split_replay_applied_txn_for_recovery();
    int replay_applied_txn_for_recovery(
            int64_t region_id,
            const std::string& instance,
            std::string start_key,
            const std::unordered_map<uint64_t, pb::TransactionInfo>& applied_txn,
            std::string end_key = "");

    void send_log_entry_to_new_region_for_split();
    int tail_split_region_add_peer();
    int split_region_add_peer(int64_t new_region_id, std::string instance,
                              std::vector<std::string> add_peer_instances, bool async);
    void remove_one_new_region_peers(int64_t region_id, std::string leader, std::vector<std::string> peers) {
        start_thread_to_remove_region(region_id, leader);
        for (auto& peer : peers) {
            start_thread_to_remove_region(region_id, peer);
        }
    }
    void split_remove_new_region_peers() {
        if (_split_param.multi_new_regions.empty()) {
            remove_one_new_region_peers(_split_param.new_region_id, _split_param.instance, _split_param.add_peer_instances);
        } else {
            for (auto& pair : _split_param.multi_new_regions) {
                remove_one_new_region_peers(pair.new_region_id, pair.new_instance, pair.add_peer_instances);
            }
        }
    }
    //split 第三步， 通知被分裂出来的region分裂完成， 增加old_region的version, update end_key
    int send_complete_to_one_new_region(const std::string& instance, const std::vector<std::string>& peers, int64_t new_region_id, 
                                        const std::string& start_key, const std::string& end_key = "");
    void send_complete_to_new_region_for_split(); 
    //分裂第四步完成
    void complete_split();
    void transfer_leader_after_split();
    
    //从split开始之后所有的entry数据作为分裂的增量部分
    // 1说明还有数据，0说明到头了
    int get_log_entry_for_split(const int64_t start_index, 
            const int64_t expected_term,
            std::vector<pb::BatchStoreReq>& requests,
            std::vector<butil::IOBuf>& req_datas,      // cntl attachment的数据
            int64_t& split_end_index);
    
    int get_split_key(std::string& split_key, int64_t& split_key_term);
    
    bool is_splitting() {
        return _split_param.new_region_id != 0;
    }
    int64_t get_region_id() const {
        return _region_id;
    }

    void update_average_cost(int64_t request_time_cost);

    void reset_split_status() {
        if (_split_param.snapshot != nullptr) {
            _rocksdb->get_db()->ReleaseSnapshot(_split_param.snapshot);
        }
        _split_param.reset_status();
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
        std::lock_guard<std::mutex> lock(_region_lock);
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
    int64_t get_partition_id() {
        std::lock_guard<std::mutex> lock(_region_lock);
        if (_region_info.has_partition_id()) {
            return _region_info.partition_id();
        }
        return 0;
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
    int64_t get_binlog_check_point() const {
        return _binlog_param.check_point_ts;
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
        if (is_learner()) {
            butil::EndPoint leader;
            butil::str2endpoint(region_info().leader().c_str(), &leader);
            return leader;
        }
        return _node.leader_id().addr;    
    }
    
    int64_t get_used_size() {
        std::lock_guard<std::mutex> lock(_region_lock);
        return _region_info.used_size();
    }
    int64_t get_table_id() {
        return _table_id;
    }
    int64_t get_global_index_id() {
        return _global_index_id;
    }
    bool is_leader() {
        return (_is_leader.load());
    }
    void leader_start(int64_t term) {
        _is_leader.store(true);
        _not_leader_alarm.reset();
        _expected_term = term;
        DB_WARNING("leader real start, region_id: %ld term: %ld", _region_id, term);
    }
    int64_t get_version() {
        return _version;
    }
    int64_t get_dml_latency() {
        return _dml_time_cost.latency();
    }
    pb::RegionInfo& region_info() {
        return _region_info;
    }
    std::shared_ptr<RegionResource> get_resource() {
        BAIDU_SCOPED_LOCK(_ptr_mutex);
        return _resource;
    }
    bool check_region_legal_complete();

    bool compare_and_set_illegal() {
        std::unique_lock<std::mutex> lock(_legal_mutex);
        std::lock_guard<std::mutex> lock_region(_region_lock);
        if (_region_info.version() <= 0) {
            _legal_region = false;
            return true;
        }
        return false;
    }

    bool compare_and_set_legal_for_split() {
        std::unique_lock<std::mutex> lock(_legal_mutex);
        if (_legal_region) {
            std::lock_guard<std::mutex> lock_region(_region_lock);
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
        std::lock_guard<std::mutex> lock(_region_lock);
        return  (_region_info.end_key().empty());
    }
    
    bool is_head() {
        std::lock_guard<std::mutex> lock(_region_lock);
        return (_region_info.start_key().empty());
    }
    
    bool empty() {
        std::lock_guard<std::mutex> lock(_region_lock);
        return (_region_info.start_key() == _region_info.end_key() 
                && !_region_info.end_key().empty() 
                && !_region_info.start_key().empty());
    }
    
    int64_t get_timecost() {
        return _time_cost.get_time();
    }
    
    void reset_timecost() {
        return _time_cost.reset();
    }

    void set_num_table_lines(int64_t table_line) {
        MetaWriter::get_instance()->update_num_table_lines(_region_id, table_line);
        _num_table_lines.store(table_line);
        DB_WARNING("region_id: %ld, table_line:%ld", _region_id, _num_table_lines.load());
    }
    void add_num_table_lines(int64_t row_line) {
        int64_t table_line = _num_table_lines.load() + row_line;
        MetaWriter::get_instance()->update_num_table_lines(_region_id, table_line);
        _num_table_lines.store(table_line);
        DB_WARNING("region_id: %ld, table_line:%ld", _region_id, _num_table_lines.load());
    }
    bool removed() const {
        return _removed;
    }
    bool is_binlog_region() const { return _is_binlog_region; }
    void set_removed(bool removed) {
        _removed_time_cost.reset();
        _removed = removed;
    }

    int64_t removed_time_cost() const {
        return _removed_time_cost.get_time();
    }
    void adjust_split_slow_down_cost(int64_t now_cost, int64_t pre_cost) {
        if (now_cost > pre_cost) {
            _split_param.split_slow_down_cost *= 2;
        } else {
            _split_param.split_slow_down_cost += 100 * 1000;
        }
        _split_param.split_slow_down_cost = std::min(
            _split_param.split_slow_down_cost, (int64_t)5 * 1000 * 1000);
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

    int apply_partial_rollback(google::protobuf::RpcController* controller,
            SmartTransaction& txn, const pb::StoreReq* request, 
            pb::StoreRes* response);

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
    
    void exec_kv_out_txn(const pb::StoreReq* request, 
            pb::StoreRes* response,
            const char* remote_side,
            google::protobuf::Closure* done);
    
    int execute_cached_cmd(const pb::StoreReq& request, pb::StoreRes& response, 
            uint64_t txn_id, 
            SmartTransaction& txn, 
            int64_t applied_index, 
            int64_t term, 
            uint64_t log_id);

    void clear_transactions() {
        if (_shutdown || !_init_success || get_version() <= 0) {
            return;
        }
        _multi_thread_cond.increase();
        _txn_pool.clear_transactions(this);
        _multi_thread_cond.decrease_signal();
    }
    void update_ttl_info() {
        if (_shutdown || !_init_success || get_version() <= 0) {
            return;
        }

        TTLInfo ttl_info = _factory->get_ttl_duration(get_table_id());
        if (ttl_info.ttl_duration_s > 0 && ttl_info.online_ttl_expire_time_us > 0) {
            // online TTL 
            if (ttl_info.online_ttl_expire_time_us != _online_ttl_base_expire_time_us) {
                _online_ttl_base_expire_time_us = ttl_info.online_ttl_expire_time_us;
                _use_ttl = true;
                _txn_pool.update_ttl_info(_use_ttl, _online_ttl_base_expire_time_us);
                DB_WARNING("table_id: %ld, region_id: %ld, ttl_duration_s: %ld, online_ttl_expire_time_us: %ld, %s", 
                    get_table_id(), _region_id, ttl_info.ttl_duration_s, 
                    ttl_info.online_ttl_expire_time_us, timestamp_to_str(ttl_info.online_ttl_expire_time_us/1000000).c_str());
            }
        }
    }
    //blacklist中新增的sign全部cancel
    void cancel_all_blacklist_sign() {
        if (_shutdown || !_init_success || get_version() <= 0) {
            return;
        }
        SmartTable table_ptr = _factory->get_table_info_ptr(_table_id);
        if (table_ptr == nullptr) {
            return;
        }
        std::set<uint64_t> last_sign_blacklist;
        if (_last_table_ptr != nullptr) {
            last_sign_blacklist = _last_table_ptr->sign_blacklist;
        }
        std::unordered_map<uint64_t, SmartState> state_map = _state_pool.get_state_map();
        for (auto& sign : table_ptr->sign_blacklist) {
            if (last_sign_blacklist.count(sign) == 0) {
                for (auto& pair : state_map) {
                    if (pair.second->sign == sign) {
                        pair.second->cancel();
                        DB_WARNING("region_id: %ld, runtime:%p cancel", _region_id, pair.second.get());
                    }
                }
            }
        }
        _last_table_ptr = table_ptr;
    }
    void clear_orphan_transactions(braft::Closure* done, int64_t applied_index, int64_t term);
    void apply_clear_transactions_log();

    TransactionPool& get_txn_pool() {
        return _txn_pool;
    }

    void rollback_txn_before(int64_t timeout) {
        return _txn_pool.rollback_txn_before(timeout);
    }

    void start_thread_to_remove_region(int64_t drop_region_id, std::string instance_address) {
        Bthread bth(&BTHREAD_ATTR_SMALL);
        std::function<void()> remove_region_function =
            [this, drop_region_id, instance_address]() {
                DB_WARNING("remove region: %lu, peer: %s", drop_region_id, instance_address.c_str());
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
        _commit_meta_mutex.lock();
    }
    void unlock_commit_meta_mutex() {
        _commit_meta_mutex.unlock();
    }

    void put_commit_ts(const uint64_t txn_id, int64_t commit_ts) {
        std::unique_lock<bthread::Mutex> lck(_commit_ts_map_lock);
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
        std::unique_lock<bthread::Mutex> lck(_commit_ts_map_lock);
        if (_commit_ts_map.count(txn_id) == 0) {
            return -1;
        }
        return _commit_ts_map[txn_id];
    }

    void remove_local_index_data();
    void delete_local_rocksdb_for_ddl(int64_t table_id, int64_t index_id);
    int add_reverse_index(int64_t table_id, const std::set<int64_t>& index_ids);

    void process_download_sst(brpc::Controller* controller, 
        std::vector<std::string>& req_vec, SstBackupType type);
    void process_upload_sst(brpc::Controller* controller, bool is_ingest);

    void process_download_sst_streaming(brpc::Controller* controller, 
        const pb::BackupRequest* request,
        pb::BackupResponse* response);

    void process_upload_sst_streaming(brpc::Controller* controller, bool is_ingest,
        const pb::BackupRequest* request,
        pb::BackupResponse* response);
    void process_query_peers(brpc::Controller* controller,
        const pb::BackupRequest* request,
        pb::BackupResponse* response);
    void process_query_streaming_result(brpc::Controller *cntl,
                                        const pb::BackupRequest *request,
                                        pb::BackupResponse *response);
    std::shared_ptr<Region> get_ptr() {
        return shared_from_this();
    }
    uint64_t snapshot_data_size() const {
        return _snapshot_data_size;
    }
    void set_snapshot_data_size(size_t size) {
        _snapshot_data_size = size;
    }
    uint64_t snapshot_meta_size() const {
        return _snapshot_meta_size;
    }
    void set_snapshot_meta_size(size_t size) {
        _snapshot_meta_size = size;
    }
    bool is_addpeer() const {
        return _region_info.can_add_peer();
    }
    uint64_t get_approx_size() {
        //分裂后一段时间每超过10分钟，或者超过10%的数据量diff则需要重新获取
        if (_approx_info.time_cost.get_time() > 10 * 60 * 1000 * 1000LL && 
            _approx_info.last_version_time_cost.get_time() < 2 * 60 * 60 * 1000 * 1000LL) {
            return UINT64_MAX;
        } else {
            int64_t diff_lines = abs(_num_table_lines.load() - _approx_info.table_lines);
            if (diff_lines * 10 > _num_table_lines.load()) {
                // adjust_num_table_lines();
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

    int binlog_scan_when_restart();

    void binlog_timeout_check();
    
    void binlog_fake(int64_t ts, BthreadCond& cond);

    pb::PeerStatus region_status() const {
        return _region_status;
    }

    int64_t snapshot_index() const {
        return _snapshot_index;
    }

    bool is_learner() const {
        return _is_learner;
    }

    uint64_t region_uuid() const {
        return _region_uuid;
    }

    bool is_disable_write() {
        return _disable_write_cond.count() > 0;
    }

    bool is_dml_op_type(const pb::OpType& op_type) {
        if (op_type == pb::OP_INSERT 
             || op_type == pb::OP_DELETE
             || op_type == pb::OP_UPDATE
             || op_type == pb::OP_SELECT_FOR_UPDATE
             || op_type == pb::OP_PARTIAL_ROLLBACK
             || op_type == pb::OP_KV_BATCH) {
            return true;
        }
        return false;
    }
    bool is_2pc_op_type(const pb::OpType& op_type) {
        if (op_type == pb::OP_PREPARE 
             || op_type == pb::OP_ROLLBACK
             || op_type == pb::OP_COMMIT) {
            return true;
        }
        return false;
    }
    bool is_async_apply_op_type(const pb::OpType& op_type) {
        if (is_dml_op_type(op_type)
            || is_2pc_op_type(op_type)
            || op_type == pb::OP_NONE
            || op_type == pb::OP_UPDATE_PRIMARY_TIMESTAMP
            || op_type == pb::OP_KILL) {
                return true;
            }
        return false;
    }
    void check_peer_latency();
    void get_read_index(pb::StoreRes* response);
    
    // if seek_table_lines != nullptr, seek all sst for seek_table_lines
    bool has_sst_data(int64_t* seek_table_lines);

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
        std::vector<std::string> add_peer_instances;
        TimeCost total_cost;
        TimeCost no_write_time_cost;
        int64_t new_region_cost;
        
        TimeCost op_start_split;
        int64_t op_start_split_cost;
        TimeCost op_start_split_for_tail;
        int64_t op_start_split_for_tail_cost;
        TimeCost op_snapshot;
        TimeCost add_peer_cost;
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
        std::unordered_map<uint64_t, pb::TransactionInfo> applied_txn;

        // 尾分裂多region
        std::vector<MultiSplitRegion> multi_new_regions;

        int64_t sub_num_table_lines = 0;
        std::vector<pb::TransactionInfo> adjust_txns;

        void reset_status() {
            split_start_index = INT_FAST64_MAX;
            split_end_index = 0;
            split_term = 0;
            new_region_id = 0;
            split_slow_down = false;
            split_slow_down_cost = 0;
            err_code = 0;
            split_key = "";
            instance = "";
            reduce_num_lines = 0;
            tail_split = false;
            snapshot = nullptr;
            applied_txn.clear();
            add_peer_instances.clear();
            multi_new_regions.clear();
            sub_num_table_lines = 0;
            adjust_txns.clear();
        };
    };

    struct BinlogParam {
        std::map<int64_t, BinlogDesc> ts_binlog_map; // 用于缓存prewrite binlog元数据，便于收到commit binlog时快速反查
        int64_t max_ts_applied  = -1; // map中prewrite会和commit抵消删除，索引map中最大的ts实际上并不是真实的最大ts，需要该字段记录
        int64_t check_point_ts = -1; // 检查点，检查点之前的binlog都已经commit，重启之后从检查点开始扫描
        int64_t oldest_ts      = -1; // rocksdb中最小ts，如果region 某个peer迁移，binlog数据不迁移则oldest_ts改为当前ts
        std::map<int64_t, bool> timeout_start_ts_done; // 标记超时反查的start_ts, 仅用来避免重复commit导致的报警，不用于严格一致性场景
    };

        //binlog function
    void recover_binlog();
    void read_binlog(const pb::StoreReq* request, pb::StoreRes* response, const std::string& remote_side, uint64_t log_id);
    void query_binlog_ts(const pb::StoreReq* request, pb::StoreRes* response);
    void apply_binlog(const pb::StoreReq& request, braft::Closure* done);
    int write_binlog_record(SmartRecord record);
    int write_binlog_value(const std::map<std::string, ExprValue>& field_value_map);
    int64_t binlog_get_int64_val(const std::string& name, const std::map<std::string, ExprValue>& field_value_map);
    int64_t read_data_cf_oldest_ts();
    bool flash_back_need_read(const pb::StoreReq* request, const std::map<std::string, ExprValue>& field_value_map);
    
    std::string binlog_get_str_val(const std::string& name, const std::map<std::string, ExprValue>& field_value_map);
    
    void binlog_get_scan_fields(std::map<int32_t, FieldInfo*>& field_ids, std::vector<int32_t>& field_slot);
    void binlog_get_field_values(std::map<std::string, ExprValue>& field_value_map, SmartRecord record);
    int binlog_reset_on_snapshot_load_restart();
    
    int binlog_reset_on_snapshot_load();
    void binlog_update_map_when_scan(const std::map<std::string, ExprValue>& field_value_map);
    int binlog_update_map_when_apply(const std::map<std::string, ExprValue>& field_value_map, const std::string& remote_side);
    int binlog_update_check_point();
    int get_primary_region_info(int64_t primary_region_id, pb::RegionInfo& region_info);
    
    void binlog_query_primary_region(const int64_t& start_ts, const int64_t& txn_id, pb::RegionInfo& region_info, int64_t rollback_ts);
    void binlog_fill_exprvalue(const pb::BinlogDesc& binlog_desc, pb::OpType op_type, std::map<std::string, ExprValue>& field_value_map);
    //binlog end
    void apply_kv_in_txn(const pb::StoreReq& request, braft::Closure* done, 
                         int64_t index, int64_t term);

    void apply_kv_out_txn(const pb::StoreReq& request, braft::Closure* done, 
                                  int64_t index, int64_t term);
    bool validate_version(const pb::StoreReq* request, pb::StoreRes* response);
    void print_log_entry(const int64_t start_index, const int64_t end_index);
    void set_region(const pb::RegionInfo& region_info) {
        std::lock_guard<std::mutex> lock(_region_lock);
        _region_info.CopyFrom(region_info);
        _version = _region_info.version();
    }
    void set_region_with_update_range(const pb::RegionInfo& region_info) {
        std::lock_guard<std::mutex> lock(_region_lock);
        _region_info.CopyFrom(region_info);
        _version = _region_info.version();
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
            SplitCompactionFilter::get_instance()->set_filter_region_info(
                    _region_id, "", false, 0);
        } else {
            SplitCompactionFilter::get_instance()->set_filter_region_info(
                    _region_id, region_info.end_key(), 
                    _use_ttl, _online_ttl_base_expire_time_us);
        }
        DB_WARNING("region_id: %ld, start_key: %s, end_key: %s", _region_id, 
            rocksdb::Slice(region_info.start_key()).ToString(true).c_str(), 
            rocksdb::Slice(region_info.end_key()).ToString(true).c_str());
    }

    bool wait_rocksdb_normal(int64_t timeout = -1) {
        TimeCost cost;
        TimeCost total_cost;
        while (_rocksdb->is_any_stall()) {
            if (timeout > 0 && total_cost.get_time() > timeout) {
                return false;
            }
            if (cost.get_time() > 60 * 1000 * 1000) {
                DB_WARNING("region_id: %ld wait for rocksdb stall", _region_id);
                cost.reset();
            }
            reset_timecost();
            bthread_usleep(1 * 1000 * 1000);
        }
        return true;
    }

    int check_learner_snapshot();

    bool check_key_fits_region_range(SmartIndex pk_info, SmartTransaction txn,
        const pb::RegionInfo& region_info, const pb::KvOp& kv_op);

    bool check_key_exist(SmartTransaction txn, const pb::KvOp& kv_op) {
        if (kv_op.is_primary_key()) {
            MutTableKey key(kv_op.key());
            key.replace_i64(_region_id, 0);
            std::string value;
            int rc = txn->get_for_update(key.data(), &value);
            if (rc == 0) {
                return true;
            } else if (rc == -1) {
                return false;
            }
        }
        return false;
    }

    int check_follower_snapshot(const std::string& peer);

    bool learner_ready_for_read() const {
        return _learner_ready_for_read;
    }

    void update_binlog_read_max_ts(int64_t ts) {
        int64_t max_ts = _binlog_read_max_ts.load();
        while (max_ts < ts) {
            if (_binlog_read_max_ts.compare_exchange_strong(max_ts, ts)) {
                break;
            } 
            max_ts = _binlog_read_max_ts.load();
        }
    }

    void update_streaming_result(brpc::StreamId id, pb::StreamState state) {
        BAIDU_SCOPED_LOCK(_streaming_result.mutex);
        if (_streaming_result.last_update_time.get_time() > 3600 * 1000 * 1000LL) {
            DB_WARNING("clean streaming result");
            _streaming_result.state.clear();
        }
        _streaming_result.state[id] = state;
        _streaming_result.last_update_time.reset();
    }

    void update_unsafe_reverse_index_map(std::map<int64_t, ReverseIndexBase*>& reverse_index_map) {
        for (auto& pair : reverse_index_map) {
            int64_t reverse_index_id = pair.first;
            auto index_info = _factory->get_index_info(reverse_index_id);
            if (index_info.state != pb::IS_PUBLIC) {
                BAIDU_SCOPED_LOCK(_reverse_unsafe_index_map_lock);
                if (_reverse_unsafe_index_map.count(reverse_index_id) == 0) {
                    _reverse_unsafe_index_map[reverse_index_id] = pair.second;
                }
            }
        }
    }
    // follower read
    int append_pending_read(SmartFollowerReadCond c);
    static int ask_leader_read_index(void* region, bthread::TaskIterator<SmartFollowerReadCond>& iter);
    int ask_leader_read_index(std::vector<SmartFollowerReadCond>& tasks);
    static int wake_up_read_request(void* region, bthread::TaskIterator<ReadReqsWaitExec>& iter);

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
    size_t _snapshot_data_size = 0;
    size_t _snapshot_meta_size = 0;
    // 最新一次分裂的新region信息，现在一次分裂可能有多个新region
    std::vector<pb::RegionInfo>  _spliting_new_region_infos;
    int64_t             _region_id = 0;
    int64_t             _version = 0;
    int64_t             _table_id = 0; // region.main_table_id
    int64_t             _global_index_id = 0; //region.table_id
    
    //merge后该region为空，记录目标region，供baikaldb使用，只会merge一次，不必使用vector
    pb::RegionInfo      _merge_region_info;
    // 倒排索引需要
    // todo liguoqiang  如何初始化这个
    std::map<int64_t, ReverseIndexBase*> _reverse_index_map;
    std::map<int64_t, ReverseIndexBase*> _reverse_unsafe_index_map;
    // todo 是否可以改成无锁的
    BthreadCond _disable_write_cond;
    BthreadCond _real_writing_cond;
    SplitParam _split_param;

    std::mutex _legal_mutex;
    bool       _legal_region = true;

    uint64_t   _region_uuid = 0;

    TimeCost                        _time_cost; //上次收到请求的时间，每次收到请求都重置一次
    LatencyOnly                     _dml_time_cost;
    bool                                _restart = false;
    //计算存储分离开关，在store定时任务中更新，避免每次dml都访问schema factory
    bool                                _storage_compute_separate = false;
    bool                                _use_ttl = false; // online TTL会更新，只会false 变为true
    int64_t                             _online_ttl_base_expire_time_us = 0; // 存量数据过期时间，仅online TTL的表使用
    std::atomic<bool>                   _reverse_remove_range{false}; //split的数据，把拉链过滤一遍, safe reverse index合并
    std::atomic<bool>                   _reverse_unsafe_remove_range{false};//unsafe reverse index合并
    //raft node
    braft::Node                         _node;
    std::atomic<bool>                   _is_leader;
    // 一般情况下，_braft_apply_index和_applied_index是一致的
    // 只有在加速分裂进行异步发送logEntry的时候，_braft_apply_index > _applied_index
    // 两者diff值即为executionQueue里面排队的请求数
    int64_t                             _braft_apply_index = 0;
    int64_t                             _applied_index = 0;  //current log index
    int64_t                             _done_applied_index = 0; // 确保已经log已经执行完(数据已写盘)，follow read用
    // 表示数据版本，conf_change,no_op等不影响数据时版本不变
    // TODO, 裸用的地方太多, 需要整理
    int64_t                             _data_index = 0;
    int64_t                             _expected_term = -1; 
    // bthread cycle: set _applied_index_lastcycle = _applied_index when _num_table_lines == 0
    int64_t                             _applied_index_lastcycle = 0;  
    TimeCost                            _lastcycle_time_cost; //定时线程上次循环的时间，更新_applied_index_lastcycle时更新
    TimeCost                            _last_split_time_cost; //上次分裂时间戳
    ApproximateInfo                     _approx_info;

    bool                                _report_peer_info = false;
    std::atomic<bool>                   _shutdown;
    bool                                _init_success = false;
    bool                                _need_decrease = false; // addpeer时候从init到on_snapshot_load整体限制
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
    SmartTable                          _last_table_ptr; //黑名单屏蔽单线程使用

    // shared_ptr is not thread safe when assign
    std::mutex  _ptr_mutex;
    std::shared_ptr<RegionResource>     _resource;

    RegionControl                           _region_control;
    MetaWriter*                             _meta_writer = nullptr;
    bthread::Mutex                         _commit_meta_mutex;
    scoped_refptr<braft::FileSystemAdaptor>  _snapshot_adaptor = nullptr;
    bool                                     _is_global_index = false; //是否是全局索引的region
    std::mutex       _reverse_index_map_lock;
    std::mutex       _reverse_unsafe_index_map_lock;
    std::mutex       _backup_lock;
    Backup          _backup;
    //binlog
    bool _is_binlog_region = false; //是否为binlog region
    std::atomic<int64_t> _binlog_read_max_ts = { 0 }; // 读取binlog的最大ts
    // txn_id:commit_ts
    std::map<uint64_t, int64_t> _commit_ts_map;
    bthread::Mutex  _commit_ts_map_lock;
    bthread::Mutex  _binlog_param_mutex;
    BinlogParam _binlog_param;
    SmartTable  _binlog_table = nullptr;
    SmartIndex  _binlog_pri = nullptr;
    std::string     _rocksdb_start;
    std::string     _rocksdb_end;
    pb::PeerStatus  _region_status = pb::STATUS_NORMAL;
    BinlogAlarm     _binlog_alarm;

    //learner
    std::unique_ptr<braft::Learner> _learner;
    bool            _is_learner = false;
    bool            _learner_ready_for_read = false;
    TimeCost        _learner_time;

    // follower read
    bthread::ExecutionQueueId<SmartFollowerReadCond> _wait_read_idx_queue_id;
    bthread::ExecutionQueue<SmartFollowerReadCond>::scoped_ptr_t _wait_read_idx_queue;
    bthread::ExecutionQueueId<ReadReqsWaitExec> _wait_exec_queue_id;
    bthread::ExecutionQueue<ReadReqsWaitExec>::scoped_ptr_t _wait_exec_queue;
    // leader address for learner, 单线程不加锁
    std::string _leader_addr_for_read_idx;
    bool _ready_for_follower_read = true;
    // 解决零星写时主从延迟高,有写入时每100ms发一条NO OP, 停写5min后不再发NO OP
    NoOpTimer _no_op_timer; 

    //NOT_LEADER分类报警
    struct NotLeaderAlarm {
        enum AlarmType {
            ALARM_INIT              = 0,
            LEADER_INVALID          = 1,
            LEADER_RAFT_FALL_BEHIND = 2,
            LEADER_NOT_REAL_START   = 3
        };

        NotLeaderAlarm (int64_t region_id, const braft::PeerId& node_id) : 
            type(ALARM_INIT), region_id(region_id), node_id(node_id) { }

        void reset() {
            leader_start = false;
            alarm_begin_time.reset();
            last_print_time.reset();
            total_count = 0;
            interval_count = 0;
            type = ALARM_INIT;
        }

        void set_leader_start() { leader_start = true; }

        void not_leader_alarm(const braft::PeerId& leader_id);

        AlarmType type;
        std::atomic<bool> leader_start = { false };
        std::atomic<int> total_count = { 0 };
        std::atomic<int> interval_count = { 0 };
        TimeCost alarm_begin_time;
        TimeCost last_print_time;  // 每隔一段时间打印报警日志
        const int64_t region_id;
        const braft::PeerId node_id;
    };

    NotLeaderAlarm _not_leader_alarm;
    struct AsyncApplyParam {
        std::atomic<bool>  has_adjust_stall = { false };
        // 异步apply如果失败了，置标记，下次async_apply_log rpc会返回error
        // 以及在add_version会检查这个标记
        bool apply_log_failed = false;
        void start_adjust_stall() {
            if (!has_adjust_stall) {
                RocksWrapper::get_instance()->begin_split_adjust_option();
                has_adjust_stall = true;
            }
        }
        void stop_adjust_stall() {
            if (has_adjust_stall) {
                RocksWrapper::get_instance()->stop_split_adjust_option();
                has_adjust_stall = false;
            }
        }
    };
    AsyncApplyParam _async_apply_param;
    ExecutionQueue _async_apply_log_queue;
    struct StreamingResult {
        bthread::Mutex  mutex;
        std::unordered_map<brpc::StreamId, pb::StreamState> state;
        TimeCost last_update_time;
    };
    StreamingResult _streaming_result;
};

} // end of namespace
