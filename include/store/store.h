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

#include <unordered_map>
#include <string>
#ifdef BAIDU_INTERNAL
#include <baidu/rpc/server.h>
#include <raft/raft.h>
#include <raft/util.h>
#include <raft/storage.h>
#else
#include <brpc/server.h>
#include <braft/raft.h>
#include <braft/util.h>
#include <braft/storage.h>
#endif
#include <bvar/bvar.h>
#include "common.h"
#include "proto/meta.interface.pb.h"
#include "proto/store.interface.pb.h"
#include "proto/common.pb.h"
#include "region.h"
#include "schema_factory.h"
#include "rocks_wrapper.h"
#include "table_record.h"
#include "meta_server_interact.hpp"
namespace baikaldb {
DECLARE_int32(snapshot_load_num);
DECLARE_int32(raft_write_concurrency);
DECLARE_int32(service_write_concurrency);
const static uint64_t split_thresh = 100 * 1024 * 1024;
const static int max_region_map_count = 23;
inline int map_idx(int64_t region_id) {
    return region_id % max_region_map_count;
}



class Store : public pb::StoreService {
public:
    virtual ~Store();
    
    static Store* get_instance() {
        static Store _instance;
        return &_instance;
    }

    int init_before_listen(std::vector<std::int64_t>& init_region_ids);
    
    int init_after_listen(const std::vector<std::int64_t>& init_region_ids);
    //新建region，新建table 、add_peer(心跳返回)、split三个场景会调用
    //新建region，初始化raft, 并且写入到rocksdb中
    virtual void init_region(google::protobuf::RpcController* controller,
                             const pb::InitRegion* request,
                             pb::StoreRes* response,
                             google::protobuf::Closure* done);
    //raft control method
    virtual void region_raft_control(google::protobuf::RpcController* controller,
                                     const pb::RaftControlRequest* request,
                                     pb::RaftControlResponse* response,
                                     google::protobuf::Closure* done);

    virtual void query(google::protobuf::RpcController* controller,
                       const pb::StoreReq* request,
                       pb::StoreRes* response,
                       google::protobuf::Closure* done);

    //删除region和region中的数据
    virtual void remove_region(google::protobuf::RpcController* controller,
                               const pb::RemoveRegion* request,
                               pb::StoreRes* response,
                               google::protobuf::Closure* done);
    //恢复延迟删除的region
    virtual void restore_region(google::protobuf::RpcController* controller,
                                const pb::RegionIds* request,
                                pb::StoreRes* response,
                                google::protobuf::Closure* done);

    virtual void add_peer(google::protobuf::RpcController* controller,
                            const pb::AddPeer* request,
                            pb::StoreRes* response,
                            google::protobuf::Closure* done);
    virtual void get_applied_index(google::protobuf::RpcController* controller,
                                const pb::GetAppliedIndex* request,
                                pb::StoreRes* response,
                                google::protobuf::Closure* done);
    virtual void compact_region(google::protobuf::RpcController* controller,
                                const pb::RegionIds* request,
                                pb::StoreRes* response,
                                google::protobuf::Closure* done);
    virtual void snapshot_region(google::protobuf::RpcController* controller,
                                const pb::RegionIds* request,
                                pb::StoreRes* response,
                                google::protobuf::Closure* done);
    virtual void query_region(google::protobuf::RpcController* controller,
                                const pb::RegionIds* request,
                                pb::StoreRes* response,
                                google::protobuf::Closure* done);
    virtual void query_illegal_region(google::protobuf::RpcController* controller,
                                        const pb::RegionIds* request,
                                        pb::StoreRes* response,
                                        google::protobuf::Closure* done);

    virtual void backup_region(google::protobuf::RpcController* controller,
                                const pb::BackUpReq* request,
                                pb::BackUpRes* response,
                                google::protobuf::Closure* done);

    virtual void backup(google::protobuf::RpcController* controller,
        const pb::BackupRequest* request,
        pb::BackupResponse* response,
        google::protobuf::Closure* done); 

    //上报心跳
    void heart_beat_thread();

    void send_heart_beat();

    void start_db_statistics();

    void reverse_merge_thread();
    void ttl_remove_thread();
    void delay_remove_region_thread();

    void flush_memtable_thread();
    void snapshot_thread();
    void txn_clear_thread();
    
    void whether_split_thread();

    void process_merge_request(int64_t table_id, int64_t region_id);
    //发送请求到metasever, 分配region_id 和 instance
    void process_split_request(int64_t table_id, int64_t region_id, bool tail_split, std::string split_key);
   
    //将region_id的状态由DOING->IDLE, 在raft_control的done方法中调用
    void reset_region_status(int64_t region_id);
    //得到region的split_index, 在rocksdb做compact filter时使用
    int64_t get_split_index_for_region(int64_t region_id); 
    void set_can_add_peer_for_region(int64_t region_id);    
    int get_used_size_per_region(const std::vector<int64_t>& region_ids, 
                                 uint64_t* region_sizes, int64_t* region_num_lines);
   
    RocksWrapper* get_db() {
        return _rocksdb;
    }
    std::string address() const {
        return _address;
    }
    ExecutionQueue& compact_queue() {
        return _compact_queue;
    }
    
    void sub_split_num() {
        --_split_num;
    }
    bool has_prepared_tran () const {
        return _has_prepared_tran;
    }
    SmartRegion get_region(int64_t region_id) {
        if (_region_mapping.count(region_id) == 0) {
            return SmartRegion();
        } else {
            return _region_mapping.get(region_id);
        }
    }
    void set_region(SmartRegion& region) {
        if (region == NULL) {
            return;
        }
        auto region_id = region->get_region_id();
        _region_mapping.set(region_id, region);
    }
    void erase_region(int64_t region_id) {
        _region_mapping.erase(region_id);
    }
    void traverse_region_map(const std::function<void(SmartRegion& region)>& call) {
        _region_mapping.traverse(call);
    }
    void traverse_copy_region_map(const std::function<void(SmartRegion& region)>& call) {
        _region_mapping.traverse_copy(call);
    }
    void shutdown_raft() {
        _shutdown = true;
        traverse_copy_region_map([](SmartRegion& region) {
            region->shutdown();
        });
        DB_WARNING("all region was shutdown");
        traverse_copy_region_map([](SmartRegion& region) {
            region->join();
        });
        DB_WARNING("all region was join");
    }
    bool is_shutdown() const {
        return _shutdown;
    }
    void close() {
        _add_peer_queue.stop();
        _remove_region_queue.stop();
        _compact_queue.stop();
        _shutdown = true;
        _heart_beat_bth.join();
        DB_WARNING("heart beat bth join");
        _add_peer_queue.join();
        DB_WARNING("_add_peer_queue join");
        _remove_region_queue.join();
        DB_WARNING("_remove_region_queue join");
        _compact_queue.join();
        DB_WARNING("_compact_queue join");
        _split_check_bth.join();
        DB_WARNING("split check bth join");
        _merge_bth.join();
        DB_WARNING("merge bth check bth join");
        _ttl_bth.join();
        DB_WARNING("ttl bth check bth join");
        _delay_remove_region_bth.join();
        DB_WARNING("delay_remove_region_bth bth check bth join");
        _flush_bth.join();
        DB_WARNING("flush check bth join");
        _snapshot_bth.join();
        DB_WARNING("snapshot bth join");
        _txn_clear_bth.join();
        DB_WARNING("txn_clear bth join");

        _rocksdb->close();
        DB_WARNING("rockdb close, quit success");
    }
    MetaServerInteract& get_meta_server_interact() {
        return _meta_server_interact;
    }
private:
    Store(): _split_num(0),
             _disk_total("disk_total", 0),
             _disk_used("disk_used", 0),
             dml_time_cost("dml_time_cost"),
             select_time_cost("select_time_cost") {}
    
    int drop_region_from_store(int64_t drop_region_id, bool need_delay_drop);

    void update_schema_info(const pb::SchemaInfo& request);

    //判断分裂在3600S内是否完成，不完成，则自动删除该region
    void check_region_legal_complete(int64_t region_id);

    void construct_heart_beat_request(pb::StoreHeartBeatRequest& request);
    
    void process_heart_beat_response(const pb::StoreHeartBeatResponse& response);

    void monitor_memory();
    void print_properties(const std::string& name);
    void print_heartbeat_info(const pb::StoreHeartBeatRequest& request);
private:
    std::string                             _address;
    std::string                             _physical_room;
    std::string                             _resource_tag;

    RocksWrapper*                           _rocksdb;
    SchemaFactory*                          _factory;
    MetaWriter*                             _meta_writer = nullptr;
    
    // region_id => Region handler
    ThreadSafeMap<int64_t, SmartRegion> _region_mapping;

    //metaServer交互类
    MetaServerInteract _meta_server_interact;
    
    //发送心跳的线程
    Bthread _heart_beat_bth;
    //判断是否需要分裂的线程
    Bthread _split_check_bth;
    //全文索引定时merge线程
    Bthread _merge_bth;
    //TTL定期删除过期数据
    Bthread _ttl_bth;
    //延迟删除region
    Bthread _delay_remove_region_bth;

    //定时flush region meta信息，确保rocksdb的wal正常删除
    Bthread _flush_bth;
    //外部控制定时触发snapshot
    Bthread _snapshot_bth;
    // thread for transaction monitor and clear
    Bthread _txn_clear_bth;

    std::atomic<int32_t> _split_num;    
    bool _shutdown = false;
    bvar::Status<int64_t> _disk_total;
    bvar::Status<int64_t> _disk_used;

    std::vector<rocksdb::Transaction*> _recovered_txns;
    ExecutionQueue _add_peer_queue;
    ExecutionQueue _compact_queue;
    ExecutionQueue _remove_region_queue;

    bool _has_prepared_tran = true;
public:
    bool exist_prepared_log(int64_t region_id, uint64_t txn_id) {
        if (prepared_txns.find(region_id) != prepared_txns.end()
                && prepared_txns[region_id].find(txn_id) != prepared_txns[region_id].end()) {
            return true;
        } 
        return false;
    }
    bool doing_snapshot_when_stop(int64_t region_id) {
        if (doing_snapshot_regions.find(region_id) != doing_snapshot_regions.end()) {
            return true;
        }
        return false;
    }
    std::unordered_map<int64_t, std::set<uint64_t>> prepared_txns;
    std::set<int64_t>   doing_snapshot_regions;
    bvar::LatencyRecorder dml_time_cost;
    bvar::LatencyRecorder select_time_cost;
};
}
