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

#include <sys/epoll.h>
#include <sys/types.h>
#include <mutex>
#include "common.h" 

namespace baikaldb {

class NetworkSocket;

const int32_t CONFIG_MPL_EPOLL_MAX_SIZE = 65536;

class EpollInfo {
public:
    EpollInfo();
    ~EpollInfo();

    bool init();    
    int wait(uint32_t timeout);

    bool set_fd_mapping(int fd, SmartSocket sock);
    SmartSocket get_fd_mapping(int fd);
    void delete_fd_mapping(int fd);
    int get_ready_fd(int cnt);
    int get_ready_events(int cnt);

    bool poll_events_mod(SmartSocket sock, unsigned int events);
    bool poll_events_add(SmartSocket sock, unsigned int events);
    bool poll_events_delete(SmartSocket sock);

    bool all_txn_time_large_then(int64_t query_time, int64_t table_id);

private:
    SmartSocket         _fd_mapping[CONFIG_MPL_EPOLL_MAX_SIZE]; // fd -> NetworkSocket.
    std::mutex          _mutex; // 保护_fd_mapping里的shared_ptr
    int                 _epfd;
    struct epoll_event  _events[CONFIG_MPL_EPOLL_MAX_SIZE];
    size_t              _event_size;
};

} // namespace baikal

