// Copyright (c) 2018 Baidu, Inc. All Rights Reserved.
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
#include "rapidjson/document.h"
#include "meta_writer.h"

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

class TransactionPool;
class Region : public braft::StateMachine {
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

    void shutdown() {
        bool expected_status = false;
        if (_shutdown.compare_exchange_strong(expected_status, true)) {
            _node.shutdown(NULL);
            DB_WARNING("raft node was shutdown, region_id: %ld", _region_id);
        }
    }

    void join() {
        _node.join();
        DB_WARNING("raft node join completely, region_id: %ld", _region_id);
        _multi_thread_cond.wait();
        DB_WARNING("_multi_thread_cond wait success, region_id: %ld", _region_id);
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
               _init_success(false),
               _num_table_lines(0) {
        //create table and add peer请求状态初始化都为IDLE, 分裂请求状态初始化为DOING
        _status.store(_region_info.status());
    }

    int init(bool write_db, int32_t snapshot_times);

    void raft_control(google::protobuf::RpcController* controller,
            const pb::RaftControlRequest* request,
            pb::RaftControlResponse* response,
            google::protobuf::Closure* done);

    void query(google::protobuf::RpcController* controller,
            const pb::StoreReq* request,
            pb::StoreRes* response,
            google::protobuf::Closure* done);
     
    void dml(const pb::StoreReq& request, 
            pb::StoreRes& response,
            int64_t applied_index, 
            int64_t term);

    void dml_2pc(const pb::StoreReq& request, 
            pb::OpType op_type, 
            const pb::Plan& plan,
            const RepeatedPtrField<pb::TupleDescriptor>& tuples, 
            pb::StoreRes& response,
            int64_t applied_index, 
            int64_t term,
            int seq_id);

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

    //leader收到从metaServer心跳包中的解析出来的add_peer请求
    void add_peer(const pb::AddPeer& add_peer, ExecutionQueue& queue);
    void add_peer(const pb::AddPeer* request,  
            pb::StoreRes* response, 
            google::protobuf::Closure* done);
    int transfer_leader(const pb::TransLeaderRequest& trans_leader_request); 
    void construct_heart_beat_request(pb::StoreHeartBeatRequest& request, bool need_peer_balance); 
    // state machine method
    void set_can_add_peer();
    void sync_do_snapshot();
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

    int clear_data();
    void compact();

    // other thread
    void reverse_merge();

    // dump the the tuples in this region in format {{k1:v1},{k2:v2},{k3,v3}...}
    // used for debug
    std::string dump_hex();
    
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
    int get_log_entry_for_split(const int64_t start_index, 
            const int64_t expected_term,
            std::vector<pb::StoreReq>& requests, 
            int64_t& split_end_index);
    
    int get_split_key(std::string& split_key);
    int send_no_op_request(const std::string& instance,
            int64_t recevie_region_id, 
            int64_t request_version);
 
    int64_t get_region_id() {
        return _region_id;
    }

    void update_average_cost(int64_t request_time_cost);

    // other thread
    void reset_region_status() {
        pb::RegionStatus expected_status = pb::DOING;
        if (!_status.compare_exchange_strong(expected_status, pb::IDLE)) {
            DB_WARNING("region status is not doing, region_id: %ld", _region_id);
        } else {
            DB_WARNING("region status is reset to IDLE, region_id: %ld", _region_id);
        }
    }
    
    pb::RegionStatus get_status() const {
        return _status.load();
    }

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
    void get_region_info(pb::RegionInfo& region_info) {
        region_info = _region_info;
    }
    pb::RegionInfo* get_region_info() {
        return &_region_info;
    }
    int64_t get_log_index() const {
        return _applied_index;
    }
    rocksdb::ColumnFamilyHandle* get_data_cf() const {
        return _data_cf;
    }
    butil::EndPoint get_leader() {
        return _node.leader_id().addr;    
    }
    void shutdown_raft() {
        _node.shutdown(NULL);
    }
    
    int64_t get_used_size() {
        return _region_info.used_size();
    }
    int64_t get_table_id() {
        return _region_info.table_id();
    }
    bool is_leader() {
        return _is_leader.load();
    }
    int64_t get_version() {
        return _region_info.version();
    }
    bool check_split_complete();
    bool compare_and_set_fail_for_split() {
        std::unique_lock<std::mutex> lock(_split_mutex);
        if (_region_info.version() <= 0) {
            _split_success = false;
            return true;
        }
        return false;
    }
    bool compare_and_set_success_for_split() {
        std::unique_lock<std::mutex> lock(_split_mutex);
        if (_split_success) {
            _region_info.set_version(1);
            return true;
        }
        return false;
    }

    int64_t get_num_table_lines() {
        return _num_table_lines.load();
    }

    bool is_tail() {
        return  (!_region_info.has_end_key() || _region_info.end_key() == "");
    }
    void snapshot(braft::Closure* done);

    int save_prepared_txn(
            std::unordered_map<uint64_t, pb::TransactionInfo>& prepared_txn_info,
            rapidjson::Document& root);
    
    int load_prepared_txn(
            std::unordered_map<uint64_t, pb::TransactionInfo>& prepared_txn_info,
            rapidjson::Document& root);

    void save_applied_index();
    int  load_applied_index();

    int64_t get_qps() {
        return _qps.load();
    }
    int64_t get_average_cost() {
        return _average_cost.load(); 
    }
    void set_num_table_lines(int64_t table_line) {
        _num_table_lines.store(table_line);
        DB_WARNING("region_id: %ld, table_line:%ld", _region_id, _num_table_lines.load());
    }
    void start_thread_to_remove_region(int64_t drop_region_id, std::string instance_address);
    void send_remove_region_to_store(int64_t drop_region_id, std::string instance_address);
    void set_removed(bool removed) {
        _removed = removed;
    }
    void set_need_clear_data(bool need_clear_data) {
        _need_clear_data = need_clear_data;
    }

    int64_t get_peer_applied_index(const std::string& peer, int64_t region_id);
    int64_t get_split_wait_time() {
        int64_t wait_time = FLAGS_disable_write_wait_timeout_us;
        if (FLAGS_disable_write_wait_timeout_us < _split_param.split_slow_down_cost * 10) {
            wait_time = _split_param.split_slow_down_cost * 10;
        }
        if (wait_time > 30 * 1000 * 1000LL) {
            DB_WARNING("split wait time exceed 30s, region_id: %ld", _region_id);
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

    int execute_cached_cmd(const pb::StoreReq& request, pb::StoreRes& response, 
            uint64_t txn_id, 
            SmartTransaction& txn, 
            int64_t applied_index, 
            int64_t term, 
            uint64_t log_id = 0);

    void clear_transactions() {
        _txn_pool.clear_transactions();
    }

    TransactionPool& get_txn_pool() {
        return _txn_pool;
    }

    // save prepared txn info when shutdown
    void get_prepared_txn_info() {
        _prepared_txn_info.clear();
        _txn_pool.get_prepared_txn_info(_prepared_txn_info, true);
    }

    void update_resource_table() {
        std::shared_ptr<RegionResource> new_resource(new RegionResource);
        new_resource->region_info = _region_info;
        // 初始化倒排索引
        TableInfo& table_info = new_resource->table_info;
        new_resource->region_id = _region_id;
        new_resource->table_id = _region_info.table_id();
        table_info = _factory->get_table_info(_region_info.table_id());
        for (int64_t index_id : table_info.indices) {
            IndexInfo info = _factory->get_index_info(index_id);
            if (info.id == -1) {
                continue;
            }
            if (info.type == pb::I_PRIMARY) {
                new_resource->pri_info = info;
            }
            new_resource->index_infos[info.id] = info;
        }
        BAIDU_SCOPED_LOCK(_ptr_mutex);
        _resource = new_resource;
    }
    bool peers_stable() {
        std::vector<braft::PeerId> peers;
        return _node.list_peers(&peers).ok() && peers.size() >= (size_t)_region_info.replica_num();
    }
    void copy_region(pb::RegionInfo* region_info) {
        std::lock_guard<std::mutex> lock(_region_lock);
        region_info->CopyFrom(_region_info);
    }
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
   
    void write_meta_info() {
        auto ret = _writer->update_num_table_lines(_region_id, _num_table_lines.load());
        if (ret < 0) {
            DB_FATAL("write num_table_line to new key fail, region_id: %ld", _region_id);
        }
        ret = _writer->update_apply_index(_region_id, _applied_index);
        if (ret < 0) {
            DB_FATAL("write apply_index to new key fail, region_id: %ld", _region_id);
        }
        DB_WARNING("write num_table_line: %ld and apply_index: %ld to new key, region_id: %ld", 
            _num_table_lines.load(), _applied_index, _region_id);
    } 
    void save_snapshot(braft::Closure* done, 
                        rocksdb::Iterator* iter,
                        braft::SnapshotWriter*writer, 
                        MutTableKey region_prefix,
                        std::string extra,
                        pb::RegionInfo snapshot_region_info);

    int send_request_to_region(const pb::StoreReq& request,
                                const std::string& instance,
                                int64_t region_id);

    int send_init_region_to_store(const std::string instance_address, 
                                   const pb::InitRegion& init_region_request,
                                   pb::StoreRes* response);

    int _write_region_to_rocksdb(const pb::RegionInfo& region_info);
    int _whether_legal_for_add_peer(const pb::AddPeer& add_peer, pb::StoreRes* response);
    int _leader_send_init_region(const std::string& new_instance, pb::StoreRes* response);
    void _leader_add_peer(const pb::AddPeer& add_peer,
                          const std::string& new_instance, 
                          pb::StoreRes* response, 
                          google::protobuf::Closure* done);

    bool validate_version(const pb::StoreReq* request, pb::StoreRes* response);

    int _write_sst_for_region_info(braft::Closure* done,
                                   const std::string& sst_file,
                                   pb::RegionInfo& snapshot_region_info);
    int _write_sst_for_data(braft::Closure* done,
                            const std::string& sst_file,
                            rocksdb::Iterator* iter,
                            MutTableKey& region_prefix,
                            int64_t& row_count);

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
        SplitCompactionFilter::get_instance()->set_range_key(
                _region_id,
                region_info.start_key(),
                region_info.end_key());
    }

private:
    //Singleton
    RocksWrapper*       _rocksdb;
    SchemaFactory*      _factory;
    rocksdb::ColumnFamilyHandle* _data_cf;    
    rocksdb::ColumnFamilyHandle* _region_cf;    
    std::string         _address; //ip:port
    
    //region metainfo
    pb::RegionInfo      _region_info;
    std::mutex          _region_lock;    
    //split后缓存分裂出去的region信息供baikaldb使用
    std::vector<pb::RegionInfo> _new_region_infos;
    pb::RegionInfo      _new_region_info;
    int64_t             _region_id;
    // 倒排索引需要
    // todo liguoqiang  如何初始化这个
    std::map<int64_t, ReverseIndexBase*> _reverse_index_map;
    
    //region status,用在分裂和add_peer remove_peer时
    //只有leader有状态
    std::atomic<pb::RegionStatus> _status;
    // todo 是否可以改成无锁的
    BthreadCond _disable_write_cond;
    BthreadCond _real_writing_cond;
    SplitParam _split_param;
    std::mutex _split_mutex;
    bool       _split_success = true;

    TimeCost                        _time_cost; //上次收到请求的时间，每次收到请求都重置一次
    std::mutex                      _queue_lock;    
    butil::BoundedQueue<StatisticsInfo> _statistics_queue;
    StatisticsInfo _statistics_items[RECV_QUEUE_SIZE];
    std::atomic<int64_t> _qps;
    std::atomic<int64_t> _average_cost;

    //raft node
    braft::Node                         _node;
    std::atomic<bool>                   _is_leader;
    int64_t                             _applied_index = 0;  //current log index

    bool                                _report_peer_info = false;
    std::atomic<bool>                   _shutdown;
    bool                                _init_success = false;

    BthreadCond                         _multi_thread_cond;
    // region stat variables
    std::atomic<int64_t>                _num_table_lines;  //total number of pk record in this region
    int64_t                             _snapshot_num_table_lines = 0;  //last snapshot number 
    TimeCost                            _snapshot_time_cost;
    int64_t                             _snapshot_index = 0; //last snapshot log index
    bool                                _removed = false;
    bool                                _need_clear_data = true;
    TransactionPool                     _txn_pool;

    // used to save and load prepared txn info when shutdown and recovery
    std::unordered_map<uint64_t, pb::TransactionInfo> _prepared_txn_info;
    // shared_ptr is not thread safe when assign
    std::mutex  _ptr_mutex;
    std::shared_ptr<RegionResource>     _resource;
    MetaWriter*                             _writer = nullptr;
};

typedef std::shared_ptr<Region> SmartRegion;

struct SnapshotClosure : public braft::Closure {
    virtual void Run() {
        if (!status().ok()) {
            DB_WARNING("region_id: %ld  status:%s, snapshot failed.",
                        region->get_region_id(), status().error_cstr());
        }
        cond.decrease_signal();
        delete this;
    }
    SnapshotClosure(BthreadCond& cond, Region* reg) : cond(cond), region(reg) {}
    BthreadCond& cond;
    Region* region = nullptr;
    int ret = 0;
    //int retry = 0;
};
} // end of namespace
