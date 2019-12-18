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

#include <atomic>
#include "common.h"
#include "proto/store.interface.pb.h"

namespace baikaldb {
class Region;
class RegionControl {
typedef std::shared_ptr<Region> SmartRegion;
public:
    static int remove_data(int64_t drop_region_id);
    static void compact_data(int64_t region_id);
    static void compact_data_in_queue(int64_t region_id);
    static int remove_log_entry(int64_t drop_region_id);
    static int remove_meta(int64_t drop_region_id);
    static int remove_snapshot_path(int64_t drop_region_id);
    static int clear_all_infos_for_region(int64_t drop_region_id);
    static int ingest_data_sst(const std::string& data_sst_file, int64_t region_id);
    static int ingest_meta_sst(const std::string& meta_sst_file, int64_t region_id);

    RegionControl(Region* region, int64_t region_id): _region(region), _region_id(region_id) {}
    virtual ~RegionControl() {}
    
    void sync_do_snapshot();
    
    void raft_control(google::protobuf::RpcController* controller,
                      const pb::RaftControlRequest* request,
                      pb::RaftControlResponse* response,
                      google::protobuf::Closure* done);
    void add_peer(const pb::AddPeer& add_peer, SmartRegion region, ExecutionQueue& queue);
    void add_peer(const pb::AddPeer* request,
                    pb::StoreRes* response,
                    google::protobuf::Closure* done);
    int transfer_leader(const pb::TransLeaderRequest& trans_leader_request);
    int init_region_to_store(const std::string instance_address,
                            const pb::InitRegion& init_region_request,
                            pb::StoreRes* store_response);

    void store_status(const pb::RegionStatus& status) {
        _status.store(status);
    }
    pb::RegionStatus get_status() const {
        return _status.load();
    }
    void reset_region_status() {
        pb::RegionStatus expected_status = pb::DOING;
        if (!_status.compare_exchange_strong(expected_status, pb::IDLE)) {
            DB_WARNING("region status is not doing, region_id: %ld", _region_id);
        }
    }
    bool compare_exchange_strong(pb::RegionStatus& expected, pb::RegionStatus desire) {
        return _status.compare_exchange_strong(expected, desire);
    }
private:
    void construct_init_region_request(pb::InitRegion& init_region_request);
    int legal_for_add_peer(const pb::AddPeer& add_peer, pb::StoreRes* response);
    void node_add_peer(const pb::AddPeer& add_peer, 
                        const std::string& new_instance,
                        pb::StoreRes* response,
                        google::protobuf::Closure* done);
private: 
    Region* _region = nullptr;
    int64_t _region_id = 0;
    //region status,用在split、merge、ddl、ttl、add_peer、remove_peer时
    //保证操作串行
    //只有leader有状态 ddl?
    std::atomic<pb::RegionStatus> _status;
};
} // end of namespace
