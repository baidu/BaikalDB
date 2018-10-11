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
DECLARE_string(quit_gracefully_file);
DECLARE_int32(snapshot_load_num);
const static uint64_t split_thresh = 100 * 1024 * 1024;
const static int max_region_map_count = 23;
inline int map_idx(int64_t region_id) {
    return region_id % max_region_map_count;
}

class Store : public pb::StoreService {
public:
    const static std::string SCHEMA_IDENTIFY;
    const static std::string REGION_SCHEMA_IDENTIFY;
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

    virtual void add_peer(google::protobuf::RpcController* controller,
                            const pb::AddPeer* request,
                            pb::StoreRes* response,
                            google::protobuf::Closure* done);
    virtual void get_applied_index(google::protobuf::RpcController* controller,
                                const pb::GetAppliedIndex* request,
                                pb::StoreRes* response,
                                google::protobuf::Closure* done);
    virtual void compact_region(google::protobuf::RpcController* controller,
                                const pb::CompactRegion* request,
                                pb::StoreRes* response,
                                google::protobuf::Closure* done);
    //上报心跳
    void report_heart_beat();

    void reverse_merge_thread();
    
    void flush_region_thread();
    void snapshot_thread();

    void txn_clear_thread();

    void update_store_status();
    
    //发送请求到metasever, 分配region_id 和 instance
    void process_split_request(int64_t table_id, int64_t region_id, bool tail_split, std::string split_key);
    //将region_id的状态由DOING->IDLE, 在raft_control的done方法中调用
    void reset_region_status(int64_t region_id);
    //得到region的split_index, 在rocksdb做compact filter时使用
    int64_t get_split_index_for_region(int64_t region_id); 
    void set_can_add_peer_for_region(int64_t region_id);    
    void update_and_whether_split_for_region();
   
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
    int get_used_size_per_region(const std::vector<int64_t>& region_ids, 
                                 int64_t* region_sizes);
    void shutdown_raft() {
        _shutdown = true;
        traverse_region_map([](SmartRegion& region) {
            region->shutdown();
        });
        DB_WARNING("all region was shutdown");
        traverse_region_map([](SmartRegion& region) {
            region->join();
        });
        DB_WARNING("all region was join");
    }
    //store优雅退出，保存每个region的applied_index, 下次重启时不再加载snapshot, log_index自动做过滤
    void close() {
        _add_peer_queue.stop();
        _is_running = false;
        _heart_beat_bth.join();
        DB_WARNING("heart beat bth join");
        _add_peer_queue.join();
        DB_WARNING("_add_peer_queue join");
        _split_check_bth.join();
        DB_WARNING("split check bth join");
        _merge_bth.join();
        DB_WARNING("merge bth check bth join");
        _flush_bth.join();
        DB_WARNING("flush check bth join");
        _snapshot_bth.join();
        DB_WARNING("snapshot bth join");
        _txn_clear_bth.join();
        DB_WARNING("txn_clear bth join");

        traverse_region_map([](SmartRegion& region) {
            region->get_prepared_txn_info();
            region->get_txn_pool().close();
        });
        _rocksdb->close();
        DB_WARNING("rockdb close");

        traverse_region_map([](SmartRegion& region) {
            region->save_applied_index();
        });
        DB_WARNING("all region quit gracefully");
        std::ofstream extra_fs(FLAGS_quit_gracefully_file, 
                                std::ofstream::out | std::ofstream::trunc);
        extra_fs.close();
        DB_WARNING("touch quit_gracefully");
    }
    void sub_split_num() {
        --_split_num;
    }
    // void snapshot_load_mutex_lock() {
    //     _snapshot_load_mutex.lock();
    // }
    // void snapshot_load_mutex_unlock() {
    //     _snapshot_load_mutex.unlock();
    // }
    void send_heart_beat();

    void start_db_statistics();

    bool get_is_running() {
        return _is_running;
    }
    RocksWrapper* get_db() {
        return _rocksdb;
    }
    std::string address() const {
        return _address;
    }

    std::vector<rocksdb::Transaction*>& get_recovered_txns() {
        return _recovered_txns;
    }
private:
    void update_schema_info(const pb::SchemaInfo& request);

    //判断分裂在30S内是否完成，不完成，则自动删除该region
    void _check_split_complete(int64_t region_id);

    void _construct_heart_beat_request(pb::StoreHeartBeatRequest& request);
    
    void _process_heart_beat_response(const pb::StoreHeartBeatResponse& response);

    int _drop_region_from_store(int64_t drop_region_id);
    
    int _drop_region_from_rocksdb(int64_t region_id);
  
    void _remove_region_data(int64_t drop_region_id);

    void _remove_snapshot_path(int64_t drop_region_id);
    
    int _remove_log_entry(int64_t drop_region_id);
    Store(): snapshot_load_currency(-FLAGS_snapshot_load_num), 
             init_region_currency(-FLAGS_snapshot_load_num), 
             _split_num(0) {}
   
    void monitor_memory();
    void print_properties(const std::string& name);

public:
    //全局做snapshot_load的并发控制
    BthreadCond snapshot_load_currency;
    BthreadCond init_region_currency;
private:
    std::string                             _address;
    std::string                             _physical_room;
    std::string                             _resource_tag;

    RocksWrapper*                           _rocksdb;
    SchemaFactory*                          _factory;
    rocksdb::ColumnFamilyHandle*            _region_handle; 
    //region status,用在分裂和add_peer remove_peers时 
    //只有leader有状态
    // region_id => Region handler
    //std::mutex                              _region_mutex[max_region_map_count];
    //std::unordered_map<int64_t, SmartRegion>    _region_mapping[max_region_map_count];
    ThreadSafeMap<int64_t, SmartRegion> _region_mapping;
    int _region_init_errno = 0;

    //std::mutex                                  _snapshot_load_mutex;
    //metaServer交互类
    MetaServerInteract _meta_server_interact;
    
    bool _is_running = true;
    //发送心跳的线程
    Bthread _heart_beat_bth;
    //判断是否需要分裂的线程
    Bthread _split_check_bth;
    //全文索引定时merge线程
    Bthread _merge_bth;
    //定时flush region meta信息，确保rocksdb的wal正常删除
    Bthread _flush_bth;
    //外部控制定时触发snapshot
    Bthread _snapshot_bth;
    // thread for transaction monitor and clear
    Bthread _txn_clear_bth;

    std::atomic<int32_t> _split_num;    
    // //计算store状态统计信息的线程
    // bool _db_status_run = false;
    // Bthread _db_status_thread;
    bool _shutdown = false;

    std::vector<rocksdb::Transaction*> _recovered_txns;
    ExecutionQueue _add_peer_queue;
// private:
//     int _init_test_region(std::string start, std::string end, int64_t id);
//     int _init_test_region(uint64_t start, uint64_t end, int count);
};
}
