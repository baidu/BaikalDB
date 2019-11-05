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

#include <errno.h>
#include "baikal_client_mysql_async.h"
#include "baikal_client_define.h"
#include "baikal_client_epoll.h"
namespace baikal {
namespace client {
int bthread_wait_event(Connection *connection, int status, 
        bool* is_timeout, std::ostringstream* os = NULL) {
    if (*is_timeout) {
        CLIENT_FATAL("has timeout");
        return MYSQL_WAIT_TIMEOUT;
    }
    timespec *p = NULL;
    timespec timeout;
    int fd = mysql_get_socket(connection->get_mysql_handle());
    int event_in = 0;

    if (status & MYSQL_WAIT_READ) {
        event_in |= EPOLLIN;
    }
    if (status & MYSQL_WAIT_WRITE) {
        event_in |= EPOLLOUT;
    }
    if (status & MYSQL_WAIT_EXCEPT) {
        event_in |= (EPOLLERR | EPOLLHUP);
    }
    int ret = -1;
    const static int64_t SLEEP_TIME_MS = 500;
    if (status & MYSQL_WAIT_TIMEOUT) {
        //timeout was split into some intervals in unit of 1s
        int64_t count = mysql_get_timeout_value(connection->get_mysql_handle())
            * 1000 / SLEEP_TIME_MS;
        if (count <= 0) {
            count = 1;
        }
        int i = 0;
        if (os !=NULL) {
            *os << " fd_wait:(";
        }
        while (i < count && ret != 0) {
            if (connection->get_is_hang()) {
                CLIENT_WARNING("connection will be killed, instance:%s",
                        connection->get_instance_info().c_str());
                break;
            }
            memset(&timeout, 0, sizeof(timeout));
            clock_gettime(CLOCK_REALTIME, &timeout);
            if (os != NULL && i < 10) {
                *os << i << ":" << butil::gettimeofday_ms() % 1000000 << " ";
            }
            timeout.tv_sec += 0;
            timeout.tv_nsec += SLEEP_TIME_MS * 1000000; //500ms
            if (timeout.tv_nsec > 1000000000) {
                timeout.tv_nsec -= 1000000000; // 1000ms == 1s
                timeout.tv_sec += 1;
            }
            p = &timeout;
            ret = bthread_fd_timedwait(fd, event_in, p);
            ++i;
        }
        if (os !=NULL) {
            *os << "cnt:" << i << ")";
        }
    } else {
        CLIENT_WARNING("not wait timeout");
        ret = bthread_fd_timedwait(fd, event_in, NULL);
    }

    int event_out = 0;
    if (ret == 0) {
        if (status & MYSQL_WAIT_READ) {
            event_out |= EPOLLIN;
        }
        if (status & MYSQL_WAIT_WRITE) {
            event_out |= EPOLLOUT;
        }
    } else {
        *is_timeout = true;
        event_out |= MYSQL_WAIT_TIMEOUT;
    }
    return event_out;
}

MYSQL* mysql_async_real_connect(Connection *connection, const char *host,
        const char *user, const char *passwd,
        const char *db, unsigned int port,
        const char *unix_socket,
        unsigned long client_flag) {
    return mysql_real_connect(connection->get_mysql_handle(), 
                                host, 
                                user, 
                                passwd, 
                                db, 
                                port, 
                                unix_socket, 
                                client_flag);
    // MYSQL* mysql_out = NULL;
    // mysql_options(mysql_in, MYSQL_OPT_NONBLOCK, 0);
    // int status = mysql_real_connect_start(&mysql_out,
    //         mysql_in,
    //         host,
    //         user,
    //         passwd,
    //         db,
    //         port,
    //         unix_socket, client_flag);
    // while (status) {
    //     status = bthread_wait_event(mysql_in, status);
    //     status = mysql_real_connect_cont(&mysql_out, mysql_in, status);
    // }
    // return mysql_out;
}

int mysql_async_real_query(Connection *connection,
        const char *sql, unsigned long length, std::ostringstream* os) {
    int ret = 0;
    if (connection->get_mysql_handle() == NULL) {
        CLIENT_WARNING("server lost");
        return CR_SERVER_LOST;
    }
    try {
        int status = mysql_real_query_start(&ret, connection->get_mysql_handle(), sql, length);
        bool is_timeout = false;
        while (status) {
            status = bthread_wait_event(connection, status, &is_timeout, os);
            status = mysql_real_query_cont(&ret, connection->get_mysql_handle(), status);
        }
    } catch (...) {
        CLIENT_WARNING("exception while querying the server");
        return CR_UNKNOWN_ERROR;
    }
    return ret;
}

int bthread_ping_wait_event(Connection *connection, int status) {
    timespec *p = NULL;
    timespec timeout;
    int fd = mysql_get_socket(connection->get_mysql_handle());
    int event_in = 0;

    if (status & MYSQL_WAIT_READ) {
        event_in |= EPOLLIN;
    }
    if (status & MYSQL_WAIT_WRITE) {
        event_in |= EPOLLOUT;
    }
    if (status & MYSQL_WAIT_EXCEPT) {
        event_in |= (EPOLLERR | EPOLLHUP);
    }
    int ret = -1;
    if (status & MYSQL_WAIT_TIMEOUT) {
        //timeout was split into some intervals in unit of 1s
        //ping timeout always 500ms
        int64_t count = 1;
        int i = 0;
        while (i < count && ret != 0) {
            if (connection->get_is_hang()) {
                CLIENT_WARNING("connection will be killed, instance:%s",
                        connection->get_instance_info().c_str());
                break;
            }
            memset(&timeout, 0, sizeof(timeout));
            clock_gettime(CLOCK_REALTIME, &timeout);
            timeout.tv_sec += 0;
            timeout.tv_nsec += 500000000; //500ms
            if (timeout.tv_nsec > 1000000000) {
                timeout.tv_nsec -= 1000000000; // 1000ms == 1s
                timeout.tv_sec += 1;
            }
            p = &timeout;
            ret = bthread_fd_timedwait(fd, event_in, p);
            ++i;
        }
    } else {
        CLIENT_WARNING("not wait timeout");
        ret = bthread_fd_timedwait(fd, event_in, NULL);
    }

    int event_out = 0;
    if (ret == 0) {
        if (status & MYSQL_WAIT_READ) {
            event_out |= EPOLLIN;
        }
        if (status & MYSQL_WAIT_WRITE) {
            event_out |= EPOLLOUT;
        }
    } else {
        event_out |= MYSQL_WAIT_TIMEOUT;
    }
    return event_out;
}

int mysql_async_ping(Connection *connection) {
    int ret = 0;
    if (connection->get_mysql_handle() == NULL) {
        CLIENT_WARNING("server lost");
        return CR_SERVER_LOST;
    }
    try {
        int status = mysql_ping_start(&ret, connection->get_mysql_handle());
        while (status) {
            status = bthread_ping_wait_event(connection, status);
            status = mysql_ping_cont(&ret, connection->get_mysql_handle(), status);
        }
    } catch (...) {
        CLIENT_WARNING("exception while ping the server");
        return CR_UNKNOWN_ERROR;
    }
    return ret;
}

//void mysql_async_free_result(MYSQL_RES *res, MYSQL* /*mysql*/) {
//    return mysql_free_result(res);
//    // if (mysql == NULL) {
//    //     CLIENT_WARNING("server lost");
//    //     return;
//    // }
//    // if (res == NULL) {
//    //     CLIENT_WARNING("server lost");
//    //     return;
//    // }
//    // int status =  mysql_free_result_start(res);
//    // while (status) {
//    //     status = bthread_wait_event(mysql, status);
//    //     status = mysql_free_result_cont(res, status);
//    // }
//    // return;
//}
//
MYSQL_RES* mysql_async_store_result(Connection *connection) {
    if (!connection->get_async()) {
        return mysql_store_result(connection->get_mysql_handle());
    }
    MYSQL* mysql = connection->get_mysql_handle();
    if (mysql == NULL) {
        CLIENT_WARNING("server lost");
        return NULL;
    }
    MYSQL_RES* ret = NULL;
    int status = mysql_store_result_start(&ret, mysql);
    bool is_timeout = false;
    while (status) {
        status = bthread_wait_event(connection, status, &is_timeout);
        status = mysql_store_result_cont(&ret, mysql, status);
    }
    return ret;
}

int mysql_async_set_character_set(Connection *connection, const char* char_set) {
    return mysql_set_character_set(connection->get_mysql_handle(), char_set);
    // int ret = 0;
    // int status = mysql_set_character_set_start(&ret, mysql, char_set);
    // while (status) {
    //     status = bthread_wait_event(mysql, status);
    //     status = mysql_set_character_set_cont(&ret, mysql, status);
    // }
    // return ret;
}
}
}
