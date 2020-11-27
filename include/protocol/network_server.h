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

// Brief:  The defination of Network Server.
#pragma once

#include "network_socket.h"
#include "state_machine.h"
#include "epoll_info.h"
#include "machine_driver.h"
#include "proto/meta.interface.pb.h"
#include "schema_factory.h"
#include "common.h"
#include "meta_server_interact.hpp"
#include "baikal_heartbeat.h"

namespace baikaldb {
class NetworkServer {
public:
    virtual ~NetworkServer();

    static NetworkServer* get_instance() {
        static NetworkServer server;
        return &server;
    }

    // Initialization.
    bool init();

    // Start server.
    bool start();

    // Stop server.
    void stop();

    // Gracefully shutdown.
    void graceful_shutdown();

    bool get_shutdown() { return _shutdown; }

    EpollInfo* get_epoll_info() {
        return _epoll_info;
    }

private:
    // For instance.
    NetworkServer();
    NetworkServer& operator=(const NetworkServer& other);
    bool set_fd_flags(int fd);
    SmartSocket create_listen_socket();
    void construct_other_heart_beat_request(pb::BaikalOtherHeartBeatRequest& request);
    void process_other_heart_beat_response(const pb::BaikalOtherHeartBeatResponse& response);

    std::string state2str(SmartSocket client);

    int fetch_instance_info();
    int make_worker_process();
    void connection_timeout_check();
    void report_heart_beat();
    void report_other_heart_beat();
    void index_recommend(const std::string& sample_sql, int64_t table_id, 
        int64_t index_id, std::string& index_info, std::string& desc);
    void get_field_distinct_cnt(int64_t table_id, std::set<int> fileds, 
        std::map<int64_t, int>& distinct_field_map);
    void fill_field_info(int64_t table_id, std::map<int64_t, int>& distinct_field_map, 
        std::string type, std::ostringstream& os);
    void print_agg_sql();
    void store_health_check();
    
private:
    // Server info.
    uint32_t        _counter = 0;       // Using counter++ to generate socket id.
    bool            _is_init = false;   // Flag of initialization status.
    bool            _shutdown = false;  // Flag of graceful shutdown.
    // Socket info.
    SmartSocket     _service = nullptr;  // Server socket.
    EpollInfo*      _epoll_info = nullptr;      // Epoll info and fd mapping.

    // the last action time for each thread
    std::vector<ThreadTimeStamp> _last_time;
    Bthread         _conn_check_bth;
    Bthread         _heartbeat_bth;
    Bthread         _other_heartbeat_bth;
    Bthread         _agg_sql_bth;
    Bthread         _health_check_bth;
    uint32_t        _driver_thread_num;
    uint64_t        _instance_id = 0;
};
} // namespace baikal
