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

/**
 * @file baikal_client_async.h
 * @author zhaobaoxue(com@baidu.com)
 * @date 2017/02/09 16:44:12
 * @brief Mysql Async API
 *  
 **/

#ifndef  FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_MYSQL_ASYNC_H
#define  FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_MYSQL_ASYNC_H

#include <time.h>
#include <mysql.h>
#include <sys/epoll.h>
#include <sstream>
#ifdef BAIDU_INTERNAL
#include <bthread.h>
#include <bthread_unstable.h>
#else
#include "bthread/bthread.h"
#include "bthread/unstable.h"
#endif
#include "errmsg.h"

namespace baikal {
namespace client {
class Connection;
// non-blocking implementation
extern MYSQL* mysql_async_real_connect(Connection *connection, const char *host,
    const char *user, const char *passwd,
    const char *db, unsigned int port,
    const char *unix_socket,
    unsigned long client_flag);

// non-blocking implementation
int mysql_async_real_query(Connection *connection,
        const char *stmt_str,
        unsigned long length,
        std::ostringstream* os = NULL);

// non-blocking implementation
int mysql_async_ping(Connection *connection);

//void mysql_async_free_result(MYSQL_RES *res, Connection *connection);

MYSQL_RES* mysql_async_store_result(Connection *connection);

int mysql_async_set_character_set(Connection *connection, const char* char_set);
}
}
#endif //FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_MYSQL_ASYNC_H
