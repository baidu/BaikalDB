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

#include <cstring>
#include <memory>
#include <atomic>
#include <string>
#include <map>
#include <key_encoder.h>
#include <rocks_wrapper.h>
#include <bthread/mutex.h>

#include "store.interface.pb.h"
#ifdef BAIDU_INTERNAL
#include <base/arena.h>
#include <base/raw_pack.h>
#include <raft/storage.h>
#include <raft/local_storage.pb.h>
#else
#include <butil/arena.h>
#include <butil/raw_pack.h>
#include <braft/storage.h>
#include <braft/local_storage.pb.h>
#endif
#include "index_term_map.h"


namespace baikaldb {
DECLARE_bool(enable_new_raft_log_storage);

using DoubleBufLogid = butil::DoublyBufferedData<std::unordered_map<int64_t, int64_t>>;
struct LogHead {
    explicit LogHead(const rocksdb::Slice& raw) {
        butil::RawUnpacker(raw.data())
                .unpack64((uint64_t&)term)
                .unpack32((uint32_t&)type);
    }
    LogHead(int64_t term, int type) : term(term), type(type) {}
    void serialize_to(void* data) {
        butil::RawPacker(data).pack64(term).pack32(type);
    }
    int64_t term;
    int type;
};

// Implementation of LogStorage based on RocksDB
class MyRaftLogStorage : public braft::LogStorage {
typedef std::vector<std::pair<rocksdb::SliceParts, rocksdb::SliceParts>> SlicePartsVec;
public:

    /* raft_log_cf data format
     * Key:RegionId(8 bytes) + 0x01 Value: _first_log_index
     *
     * Key:RegionId(8 bytes) + 0x02 + Index(8 bytes)
     * Value : LogHead + data
     * LogHead: term(8 bytes) + EntryType(int)
     * data: DATA(IOBuf) / ConfigurationPBMeta(pb)
     */ 
    static const size_t LOG_META_KEY_SIZE = sizeof(int64_t) + 1;
    static const size_t LOG_DATA_KEY_SIZE = sizeof(int64_t) + 1 + sizeof(int64_t);
    static const uint8_t LOG_META_IDENTIFY = 0x01;                                      
    static const uint8_t LOG_DATA_IDENTIFY = 0x02;
    const static size_t LOG_HEAD_SIZE = sizeof(int64_t) + sizeof(int);
    ~MyRaftLogStorage();
    MyRaftLogStorage():_db(NULL), _old_raftlog_handle(NULL), _binlog_handle(NULL) {
        bthread_mutex_init(&_mutex, NULL);
    }
    // init logstorage, check consistency and integrity
    int init(braft::ConfigurationManager* configuration_manager) override;

    // first log index in log
    int64_t first_log_index() override {
        return _first_log_index.load(std::memory_order_relaxed);
    }   

    // last log index in log
    int64_t last_log_index() {
        return _last_log_index.load(std::memory_order_relaxed);
    }   

    // get logentry by index
    braft::LogEntry* get_entry(const int64_t index) override;

    // get logentry's term by index
    int64_t get_term(const int64_t index) override;

    // append entries to log
    int append_entry(const braft::LogEntry* entry) override;

    // append entries to log, return append success number
    int append_entries(const std::vector<braft::LogEntry*>& entries 
            , braft::IOMetric* metric) override;

    // delete logs from storage's head, [first_log_index, first_index_kept) will be discarded
    int truncate_prefix(const int64_t first_index_kept) override;

    // delete uncommitted logs from storage's tail, (last_index_kept, last_log_index] will be discarded
    int truncate_suffix(const int64_t last_index_kept) override;

    // Drop all the existing logs and reset next log index to |next_log_index|.
    // This function is called after installing snapshot from leader
    int reset(const int64_t next_log_index) override;

    // Create an instance of this kind of LogStorage with the parameters encoded 
    // in |uri|
    // Return the address referenced to the instance on success, NULL otherwise.
    LogStorage* new_instance(const std::string& uri) const override;

    static int get_first_log_index(int64_t region_id, int64_t& first_log_index, bool create_if_miss = false);
protected:

    MyRaftLogStorage(int64_t region_id, RocksWrapper* db,
                        rocksdb::ColumnFamilyHandle* raftlog_handle,
                        rocksdb::ColumnFamilyHandle* binlog_handle);

    int _get_binlog_entry(rocksdb::Slice& raftlog_value_slice, std::string& binlog_value) const;

    int _build_key_value(SlicePartsVec& kv_raftlog_vec, SlicePartsVec& kv_binlog_vec,
                        const braft::LogEntry* entry, butil::Arena& arena);

    int _construct_slice_array(void* head_buf, const butil::IOBuf& binlog_buf, rocksdb::SliceParts* raftlog_value, 
                            rocksdb::SliceParts* binlog_key, rocksdb::SliceParts* binlog_value, butil::Arena& arena);

    rocksdb::Slice* _construct_slice_array(
                void* head_buf, 
                const butil::IOBuf& buf, 
                butil::Arena& arena); 
    
    rocksdb::Slice* _construct_slice_array(
                void* head_buf, 
                const std::vector<braft::PeerId>* peers, 
                const std::vector<braft::PeerId>* old_peers,
                butil::Arena& arena); 
    
    int _parse_meta(braft::LogEntry* entry, const rocksdb::Slice& value) const;
    
    int _encode_log_data_key(void* key_buf, size_t n, int64_t index);    

    int _encode_log_meta_key(void* key_buf, size_t n);
   
    int _decode_log_data_key(const rocksdb::Slice& data_key, 
                             int64_t& region_id, 
                             int64_t& index);

    braft::LogEntry* _decode_log_entry(int64_t index, const std::string& value) const;

    int recover_log_data(std::unique_ptr<rocksdb::Iterator> iter,
            braft::ConfigurationManager* configuration_manager,
            int64_t first_log_index,
            int64_t& last_log_index);

    virtual rocksdb::Status do_raftlog_batch_write(const SlicePartsVec& raftlog_vec);
    rocksdb::Status do_binlog_batch_write(const SlicePartsVec& binlog_vec);

    std::atomic<int64_t> _first_log_index;   
    std::atomic<int64_t> _last_log_index;
    int64_t _region_id; 
    
    RocksWrapper* _db;
    rocksdb::ColumnFamilyHandle* _old_raftlog_handle;
    rocksdb::ColumnFamilyHandle* _binlog_handle;
    bool _is_binlog_region = false;

    IndexTermMap _term_map;
    bthread_mutex_t _mutex; // for term_map     
}; // class 

// Implementation of LogStorage based on RocksDB with FIFO compaction
class NewMyRaftLogStorage : public MyRaftLogStorage {
public:
    typedef std::vector<std::pair<rocksdb::SliceParts, rocksdb::SliceParts>> SlicePartsVec;
    NewMyRaftLogStorage(int64_t region_id,
                        RocksWrapper* db,
                        rocksdb::ColumnFamilyHandle* old_raftlog_handle,
                        rocksdb::ColumnFamilyHandle* binlog_handle,
                        rocksdb::ColumnFamilyHandle* raftlog_handle)
            : MyRaftLogStorage(region_id, db, old_raftlog_handle, binlog_handle),
            _new_raftlog_handle(raftlog_handle) {}

    NewMyRaftLogStorage() = default;

    int init(braft::ConfigurationManager* configuration_manager) override;
    braft::LogEntry* get_entry(int64_t index) override;
    int truncate_prefix(int64_t first_index_kept) override;
    int truncate_suffix(int64_t last_index_kept) override;
    LogStorage* new_instance(const std::string& uri) const override;

    static void update_region_first_log_idx(int64_t region_id, int64_t logid);
    static int get_region_first_log_idx(int64_t region_id, int64_t& logid);
    static void update_region_new_first_log_idx(int64_t region_id, int64_t logid);
    static int64_t get_region_new_first_log_idx(int64_t region_id);

    static void remove_region_first_log_idx(int64_t region_id);
    static void remove_region_new_first_log_idx(int64_t region_id);
    static void set_first_log_idx_map_inited(bool is_inited) {
        FIRST_LOG_IDX_MAP_INITED.store(is_inited, std::memory_order_release);
    }

    static bool is_first_log_idx_map_inited() {
        return FIRST_LOG_IDX_MAP_INITED.load(std::memory_order_acquire);
    }

    static rocksdb::Status check_sst_removeable(
            std::shared_ptr<const rocksdb::TableProperties> table_properties, bool& expired);

    ~NewMyRaftLogStorage() override = default;

private:
    rocksdb::ColumnFamilyHandle* _new_raftlog_handle = nullptr;
    std::atomic<int64_t> _new_storage_first_log_index;

    rocksdb::Status do_raftlog_batch_write(const SlicePartsVec& raftlog_vec) override;

private:
    // 所有存活的region都得在向外提供服务前在此注册
    // FIFO压缩将按照此map判断能否删除
    // region -> min_logid
    static DoubleBufLogid FIRST_LOG_IDX_MAP;

    // 从哪个logid开始是new raft storage
    // 只在region启动时更新一次
    static DoubleBufLogid NEW_RAFT_LOG_FIRST_IDX_MAP;

    // Store::init_after_listen完成后会被置为true
    // 为true时，FIRST_LOG_IDX_MAP内不存在的region_id视为已从本机器移除，raft log可以安全删除
    static std::atomic<bool> FIRST_LOG_IDX_MAP_INITED; // = false
};
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
