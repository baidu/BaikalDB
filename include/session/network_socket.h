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

// Brief:  The defination of Network Socket and Socket Poll.
#pragma once

#include <sys/socket.h>
#include <netinet/in.h>
#include <memory>
#include <set>
#include <mutex>
#include <list>
#include <unordered_map>
#include "user_info.h"
#include "type_utils.h"
#include "common.h"

namespace baikaldb {

const uint32_t MAX_STRING_BUF_SIZE          = 1024;
const uint32_t SEND_BUF_DEFAULT_SIZE        = 4096;
const uint32_t SELF_BUF_DEFAULT_SIZE        = 4096;

class NetworkSocket;
class QueryContext;
class DataBuffer;
class ExecNode;
typedef std::shared_ptr<NetworkSocket> SmartSocket;

enum SocketType {
    SERVER_SOCKET = 0,
    CLIENT_SOCKET = 1
};

enum SocketStatus{
    STATE_CONNECTED_CLIENT      = 1,    // STATE_CONNECTED_CLIENT
    STATE_SEND_HANDSHAKE        = 2,    // STATE_SEND_HANDSHAKE
    STATE_READ_AUTH             = 3,    // STATE_READ_AUTH
    STATE_SEND_AUTH_RESULT      = 4,    // STATE_SEND_AUTH_RESULT
    STATE_READ_QUERY            = 5,
    STATE_READ_QUERY_RESULT     = 6,    // STATE_READ_QUERY_RESULT
    STATE_READ_QUERY_RESULT_MORE= 7,    // has more result
    STATE_ERROR_REUSE           = 100,
    STATE_ERROR                 = 101   // STATE_ERROR
};

struct CachePlan {
    pb::OpType  op_type;
    int32_t     sql_id;
    ExecNode*   root = nullptr;
    std::vector<pb::TupleDescriptor> tuple_descs;
};

struct NetworkSocket {
    NetworkSocket();
    ~NetworkSocket();

    bool reset_when_err();
    void on_begin(uint64_t txn_id);
    void on_commit_rollback();
    void update_old_txn_info();
    bool transaction_has_write();
    uint64_t get_global_conn_id();

    // Socket basic infomation.
    bool                shutdown;
    int                 fd;           // Socket fd.
    bool                is_free = false;
    SocketStatus        state;        // Socket status for status machine.
    std::string         ip;           // Client ip.
    int                 port;         // Client port.
    struct sockaddr_in  addr;         // For retry when failure.
    SocketType          socket_type;  // Client to engine or engine to mysql.
    uint32_t            use_times;    // This NetworkSocket be used times.
    bool                is_authed;    // Flag for login.
    bool                is_counted;   // is counted for user max_connection check
    uint32_t            thread_idx;   // current thread id processing the socket
    std::mutex          mutex;        // mutex to protect socket from multi-thread process
    time_t              last_active;  // last active time of the socket
    timeval             connect_time;

    // Socket buffer and session infomation.
    DataBuffer*     send_buf;                       // Send buffer.
    int             send_buf_offset;
    DataBuffer*     self_buf;                       // receive buffer.
    bool            has_multi_packet;
    int             header_read_len;                // readed header length.
    bool            has_error_packet;               //
    int             packet_id;                      // Packet id for result packet(mysql protocal).
    int             packet_len;                     // Packet length for read packet.
    int             current_packet_len;
    int             packet_read_len;                // Current read length of packet.
    int             is_handshake_send_partly;       // Handshake is sended partly, go on sending.
    int             is_auth_result_send_partly;     // Auth result is sended partly,
                                                    // need to go on sending.
    int64_t         last_insert_id;
    // Socket status.
    std::string     current_db;                     // Current use database.
    int             charset_num;                    // Client charset number.
    std::string     charset_name;                   // Client charset name.

    std::string     username;
    std::shared_ptr<UserInfo>       user_info;      // userinfo for current connection
    std::shared_ptr<QueryContext>   query_ctx;      // Current query.

    int64_t         conn_id = -1;            // The client connection ID in Mysql Client-Server Protocol

    // Transaction related members
    int64_t         primary_region_id = -1;  // used for txn like Percolator
    bool            primary_region_exec_failed = false;
    bool            autocommit = true;       // The autocommit flag set by SET AUTOCOMMIT=0/1
    uint64_t        txn_id = 0;              // ID of the current transaction, 0 means out-transaction query
    uint64_t        new_txn_id = 0;          // For implicit commit commands (i.e. BEGIN after another BEGIN)
    int             seq_id = 0;              // The query sequence id within a transaction, starting from 1
    uint64_t        server_instance_id = 0;  // The global unique instance id of the current BaikalDB process, 
                                             // fetched from BaikalMeta when a BaikalDB instance starts,
    std::set<int>   need_rollback_seq;       // The sequence id for the commands need rollback within the transaction
    bthread_mutex_t region_lock;

    std::map<int, CachePlan> cache_plans; // plan of queries in a transaction
    std::map<int64_t, pb::RegionInfo> region_infos;

    // prepare releated members
    uint64_t         stmt_id = 0;  // The statement ID auto_inc in Mysql Client-Server Protocol
    std::unordered_map<std::string, std::shared_ptr<QueryContext>> prepared_plans;

    std::unordered_map<std::string, pb::ExprNode> session_vars;
    std::unordered_map<std::string, pb::ExprNode> user_vars;
};

class SocketFactory {
public:
    ~SocketFactory() {};

    static SocketFactory* get_instance() {
        static SocketFactory _instance;
        return &_instance;
    }

    SmartSocket create(SocketType type);
    void free(SmartSocket socket);
private:
    SocketFactory() {
        _cur_conn_id = 0;
    }
    SocketFactory& operator=(const SocketFactory& other);

    std::atomic<int64_t>    _cur_conn_id;
};

} // namespace baikal
