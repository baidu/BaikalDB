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

#include "network_socket.h"
#include "epoll_info.h"

namespace baikaldb {

EpollInfo::EpollInfo(): _epfd(-1), _event_size(0) {}

EpollInfo::~EpollInfo() {
    if (_epfd > 0) {
        close(_epfd);
    }
}

bool EpollInfo::init() {
    _event_size = CONFIG_MPL_EPOLL_MAX_SIZE;
    _epfd = epoll_create(CONFIG_MPL_EPOLL_MAX_SIZE);
    if (_epfd < 0) {
        DB_FATAL("epoll_create() failed.");
        return false;
    }
    return true;
}

int EpollInfo::wait(uint32_t timeout) {
    return epoll_wait(_epfd, _events, _event_size, timeout);
}

bool EpollInfo::set_fd_mapping(int fd, SmartSocket sock) {
    if (fd < 0 || fd >= CONFIG_MPL_EPOLL_MAX_SIZE) {
        DB_FATAL("Wrong fd[%d]", fd);
        return false;
    }
    std::unique_lock<std::mutex> lock(_mutex);
    _fd_mapping[fd] = sock;
    return true;
}

SmartSocket EpollInfo::get_fd_mapping(int fd) {
    if (fd < 0 || fd >= CONFIG_MPL_EPOLL_MAX_SIZE) {
        DB_FATAL("Wrong fd[%d]", fd);
        return SmartSocket();
    }
    std::unique_lock<std::mutex> lock(_mutex);
    return _fd_mapping[fd];
}

void EpollInfo::delete_fd_mapping(int fd) {
    if (fd < 0 || fd >= CONFIG_MPL_EPOLL_MAX_SIZE) {
        return;
    }
    std::unique_lock<std::mutex> lock(_mutex);
    _fd_mapping[fd] = SmartSocket();
    return;
}

int EpollInfo::get_ready_fd(int cnt) {
    return _events[cnt].data.fd;
}

int EpollInfo::get_ready_events(int cnt) {
    return _events[cnt].events;
}

bool EpollInfo::poll_events_mod(SmartSocket sock, unsigned int events) {
    struct epoll_event ev;
    ev.events = 0;
    ev.data.ptr = NULL;
    ev.data.fd = 0;
    ev.data.u32 = 0;
    ev.data.u64 = 0;
    if (events & EPOLLIN) {
        ev.events |= EPOLLIN;
    }
    if (events & EPOLLOUT) {
        ev.events |= EPOLLOUT;
    }
    ev.events |= EPOLLERR | EPOLLHUP;
    ev.data.fd = sock->fd;

    if (0 > epoll_ctl(_epfd, EPOLL_CTL_MOD, sock->fd, &ev)){
        DB_FATAL("poll_events_mod() epoll_ctl error:%m , epfd=%d, fd=%d, event=%d\n", 
                        _epfd, sock->fd, events);
        return false;
    }
    return true;
}

bool EpollInfo::poll_events_add(SmartSocket sock, unsigned int events) {
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    if (events & EPOLLIN) {
        ev.events |= EPOLLIN;
    }
    if (events & EPOLLOUT) {
        ev.events |= EPOLLOUT;
    }
    ev.events |= EPOLLERR | EPOLLHUP;
    ev.data.fd = sock->fd;

    if (0 > epoll_ctl(_epfd, EPOLL_CTL_ADD, sock->fd, &ev)) {
        DB_FATAL("poll_events_add_socket() epoll_ctl error:%m , epfd=%d, fd=%d\n", 
                        _epfd, sock->fd);
        return false;
    }

    return true;
}

bool EpollInfo::poll_events_delete(SmartSocket sock) {
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.data.fd = sock->fd;
    
    if (epoll_ctl(_epfd, EPOLL_CTL_DEL, sock->fd, &ev) < 0) {
        DB_FATAL("poll_events_delelte_socket() epoll_ctl error:%m, epfd=%d, fd=%d\n", 
                        _epfd, sock->fd);
        return false;
    }

    return true;
}

bool EpollInfo::all_txn_time_large_then(int64_t query_time, int64_t table_id) {
    for (auto i = 0; i < CONFIG_MPL_EPOLL_MAX_SIZE; ++i) {
        auto smart_socket = get_fd_mapping(i);
        if (smart_socket == nullptr) {
            continue;
        }
        if (smart_socket->is_txn_tid_exist(table_id) && smart_socket->txn_start_time != 0) {
            if (smart_socket->txn_start_time < query_time) {
                DB_DEBUG("start_time %ld", smart_socket->txn_start_time);
                return false;
            }
        }
    }
    return true;
}

} // namespace baikal
