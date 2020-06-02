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
#include "common.h"
#ifdef BAIDU_INTERNAL
#include <base/arena.h>
#include <base/raw_pack.h>
#include <raft/storage.h>
#include <raft/local_storage.pb.h>
#include <raft/protobuf_file.h>
typedef braft::StableStorage RaftMetaStorage;
#else
#include <butil/arena.h>
#include <butil/raw_pack.h>
#include <braft/storage.h>
#include <braft/local_storage.pb.h>
#include <braft/protobuf_file.h>
typedef braft::RaftMetaStorage RaftMetaStorage;
#endif

namespace baikaldb {
// Implementation of RaftMetaStorage based on RocksDB
class MyRaftMetaStorage : public RaftMetaStorage {
public:
    /* raft_log_cf data format
     * Key:RegionId(8 bytes) + 0x03
     * Value : StablePBMeta
     */ 
    static const size_t RAFT_META_KEY_SIZE = sizeof(int64_t) + 1;
    static const uint8_t RAFT_META_IDENTIFY = 0x03;                     
    MyRaftMetaStorage() {
    }

    // set current term
    virtual int set_term(const int64_t term);

    // get current term
    virtual int64_t get_term();

    // set votefor information
    virtual int set_votedfor(const braft::PeerId& peer_id);

    // get votefor information
    virtual int get_votedfor(braft::PeerId* peer_id);

    // set term and peer_id
    virtual int set_term_and_votedfor(const int64_t term, const braft::PeerId& peer_id);
    // init stable storage
    virtual butil::Status init();
    // set term and votedfor information
    virtual butil::Status set_term_and_votedfor(const int64_t term, 
                            const braft::PeerId& peer_id, const braft::VersionedGroupId& group);
    // get term and votedfor information
    virtual butil::Status get_term_and_votedfor(int64_t* term, braft::PeerId* peer_id, 
                                                   const braft::VersionedGroupId& group);

    RaftMetaStorage* new_instance(const std::string& uri) const override;

private:

    MyRaftMetaStorage(int64_t region_id, RocksWrapper* db,
                        rocksdb::ColumnFamilyHandle* handle);
    int load();
    int save();

    bool _is_inited = false;
    int64_t _region_id = 0; 
    int64_t _term = 1;
    braft::PeerId _votedfor;
    RocksWrapper* _db = nullptr; 
    rocksdb::ColumnFamilyHandle* _handle = nullptr;
}; // class 

} //namespace raft

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
