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
#ifdef BAIDU_INTERNAL
#include "baikal_client.h"
#else
namespace baikal {
namespace client {
class ResultSet {
public:
    uint64_t get_affected_rows() {
        return 0;
    }
};
class Service {
public:
    int query(int i, const std::string& sql, ResultSet* res) {
        return -1;
    }
};
class Manager {
public:
    int init(const std::string& path, const std::string& conf) {
        return -1;
    }
    Service* get_service(const std::string& name) {
        return nullptr;
    }
};
}
}
#endif

namespace baikaldb {
DECLARE_int32(limit_slow_sql_size);
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

    uint64_t get_instance_id() {
        return _instance_id;
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

    int32_t set_keep_tcp_alive(int socket_fd);

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
    int insert_agg_sql(const std::string &sql);
    int insert_agg_sql_by_sign(const std::string& values);
    int insert_family_table_tag(const std::string& values);
    int insert_agg_sql_by_sign(std::map<uint64_t, std::string>& sign_sql_map, 
        std::set<std::string>& family_tbl_tag_set, 
        std::set<uint64_t>& sign_to_counts);
    void insert_subquery_signs_info(std::map<uint64_t, std::set<uint64_t>>& parent_sign_to_subquery_signs, bool is_insert_success);
    int insert_subquery_values(const std::string& values);

    // 收集慢查询涉及的相关函数
    int insert_slow_query_infos(const std::string& slow_query_info_values);
    void process_slow_query_map();

private:
    // Server info.
    uint32_t        _counter = 0;       // Using counter++ to generate socket id.
    bool            _is_init = false;   // Flag of initialization status.
    bool            _shutdown = false;  // Flag of graceful shutdown.
    // Socket info.
    SmartSocket     _service = nullptr;  // Server socket.
    EpollInfo*      _epoll_info = nullptr;      // Epoll info and fd mapping.

    Bthread         _conn_check_bth;
    Bthread         _heartbeat_bth;
    Bthread         _other_heartbeat_bth;
    Bthread         _agg_sql_bth;
    Bthread         _health_check_bth;
    uint32_t        _driver_thread_num;
    uint64_t        _instance_id = 0;
    std::string     _physical_room;
    bvar::Adder<int64_t> _heart_beat_count;
    // for print_agg_sql
    baikal::client::Manager _manager;
    baikal::client::Service* _baikaldb;
public:
    // bvar保存慢查询信息
    bvar::Adder<BvarSlowQueryMap> slow_query_map;
};
} // namespace baikal
