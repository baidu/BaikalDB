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

#include "proto/meta.interface.pb.h"
#include "schema_factory.h"
#include "meta_server_interact.hpp"

namespace baikaldb {
class BaikalHeartBeat {
public:
    static void construct_heart_beat_request(pb::BaikalHeartBeatRequest& request);
    static void process_heart_beat_response_sync(const pb::BaikalHeartBeatResponse& response);
    static void process_heart_beat_response(const pb::BaikalHeartBeatResponse& response);
};

class BinlogNetworkServer  {
public:
    ~BinlogNetworkServer() = default;
    typedef ::google::protobuf::RepeatedPtrField<pb::RegionInfo> RegionVec; 
    typedef ::google::protobuf::RepeatedPtrField<pb::SchemaInfo> SchemaVec;
    
    void config(const std::string& namespace_name, const std::string& table) {
        _table_names.push_back(namespace_name + "." + table);
    }

    bool init();

    static BinlogNetworkServer* get_instance() {
        static BinlogNetworkServer server;
        return &server;
    }

    int64_t get_binlog_target_id() const {
        return _binlog_id;
    }

    const std::unordered_set<int64_t>& get_binlog_origin_ids() const {
        return _binlog_table_ids;
    }

    void report_heart_beat();

    void schema_heartbeat() {
        _heartbeat_bth.run([this]() {report_heart_beat();});
    }

    void process_heart_beat_response(const pb::BaikalHeartBeatResponse& response);

    bool process_heart_beat_response_sync(const pb::BaikalHeartBeatResponse& response);

private:
    std::vector<std::string> _table_names;
    std::unordered_set<int64_t> _binlog_table_ids;
    int64_t _binlog_id {-1};
    bool _shutdown {false};
    Bthread _heartbeat_bth;
};
}
