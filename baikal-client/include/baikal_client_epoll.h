// Copyright (c) 2019 Baidu, Inc. All Rights Reserved.
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
//
// Author: Minghao Li(liminghao01@baidu.com)
//

#ifndef FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_EPOLL_H
#define FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_EPOLL_H

#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/select.h>
#include <boost/thread.hpp>
#include "baikal_client_define.h"
#include "baikal_client_util.h"
#include "baikal_client_mysql_connection.h"

namespace baikal {
namespace client {

class MysqlConnection;

const int32_t EPOLL_MAX_SIZE = 65536;

class EpollServer {
public:
    static EpollServer* get_instance() {
        static EpollServer server;
        return &server;
    }
    void start_server();

    ~EpollServer();

    bool init();
    bool set_fd_mapping(int fd, MysqlEventInfo* conn);
    MysqlEventInfo* get_fd_mapping(int fd);
    void delete_fd_mapping(int fd);

    bool poll_events_mod(int fd, unsigned int events);
    bool poll_events_add(int fd, unsigned int events);
    bool poll_events_delete(int fd);

    void set_shutdown() {
        _shutdown = true;
    }
private:
    EpollServer();

    MysqlEventInfo**    _fd_mapping; // fd -> MysqlEventInfo.
    int                 _epfd;
    struct epoll_event* _events;
    size_t              _event_size;
    bool                _is_init;
    bool                _shutdown;
};
}
} // namespace baikal

#endif
