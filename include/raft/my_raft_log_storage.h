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
    MyRaftLogStorage():_db(NULL), _raftlog_handle(NULL), _binlog_handle(NULL) {
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

private:

    MyRaftLogStorage(int64_t region_id, RocksWrapper* db,
                        rocksdb::ColumnFamilyHandle* raftlog_handle,
                        rocksdb::ColumnFamilyHandle* binlog_handle);

    int get_binlog_entry(rocksdb::Slice& raftlog_value_slice, std::string& binlog_value);

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
    
    int _parse_meta(braft::LogEntry* entry, const rocksdb::Slice& value);
    
    int _encode_log_data_key(void* key_buf, size_t n, int64_t index);    

    int _encode_log_meta_key(void* key_buf, size_t n);
   
    int _decode_log_data_key(const rocksdb::Slice& data_key, 
                             int64_t& region_id, 
                             int64_t& index);

    std::atomic<int64_t> _first_log_index;   
    std::atomic<int64_t> _last_log_index;
    int64_t _region_id; 
    
    RocksWrapper* _db; 
    rocksdb::ColumnFamilyHandle* _raftlog_handle;
    rocksdb::ColumnFamilyHandle* _binlog_handle;
    bool _is_binlog_region = false;

    IndexTermMap _term_map;
    bthread_mutex_t _mutex; // for term_map     
}; // class 

} //namespace raft

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
