// Copyright (c) 2018 Baidu, Inc. All Rights Reserved.
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

#include <boost/make_shared.hpp>
#include "network_socket.h"
#include "query_context.h"
#include "exec_node.h"

namespace baikaldb {
static UserInfo dummy;

NetworkSocket::NetworkSocket() {
    //send_buf = boost::make_shared<DataBuffer>();
    //self_buf = boost::make_shared<DataBuffer>();
    send_buf = new DataBuffer;
    self_buf = new DataBuffer;
    if (send_buf == NULL || self_buf == NULL) {
        DB_FATAL("New send_buf and self_buf have not enough memory."
                    "send_buf_size:[%u],self_buf_size:[%d]",
                    SEND_BUF_DEFAULT_SIZE, SELF_BUF_DEFAULT_SIZE);
    }

    in_pool = false;
    send_buf_offset = 0;
    header_offset = 0;
    header_read_len = 0;
    fd = -1;
    port = 0;
    socket_type = CLIENT_SOCKET;
    use_times = 0;
    is_authed = false;
    is_counted = false;
    packet_id = 0;
    packet_len = 0;
    packet_read_len = 0;
    is_handshake_send_partly = 0;
    last_insert_id = 0;
    has_error_packet = false;
    is_auth_result_send_partly = 0;
    query_ctx.reset(new QueryContext);
    user_info.reset(new UserInfo);
    pb::ExprNode str_node;
    str_node.set_node_type(pb::STRING_LITERAL);
    str_node.set_col_type(pb::STRING);
    str_node.set_num_children(0);
    str_node.mutable_derive_node()->set_string_val("utf8");
    session_vars["character_set_database"] = str_node;
    str_node.mutable_derive_node()->set_string_val("utf8_general_ci");
    session_vars["collation_database"] = str_node;
    str_node.mutable_derive_node()->set_string_val("REPEATABLE-READ");
    session_vars["tx_isolation"] = str_node;
    str_node.mutable_derive_node()->set_string_val("Source distribution");
    session_vars["version_comment"] = str_node;

    pb::ExprNode int_node;
    int_node.set_node_type(pb::INT_LITERAL);
    int_node.set_col_type(pb::INT64);
    int_node.set_num_children(0);
    int_node.mutable_derive_node()->set_int_val(1);
    session_vars["auto_increment_increment"] = int_node;
    session_vars["autocommit"] = int_node;

    bthread_mutex_init(&region_lock, nullptr);
}

NetworkSocket::~NetworkSocket() {
    if (fd > 0) {
        close(fd);
    }
    fd = -1;
    delete self_buf;
    delete send_buf;
    self_buf = nullptr;
    send_buf = nullptr;

    for (auto& pair : cache_plans) {
        delete pair.second.root;
    }
    for (auto& pair : prepared_plans) {
        delete pair.second;
    }
    bthread_mutex_destroy(&region_lock);
}

void NetworkSocket::reset_send_buf() {
    //send_buf.reset();
    //send_buf = boost::make_shared<DataBuffer>();
    //send_buf_offset = 0;
}

bool NetworkSocket::reset() {
    if (fd > 0) {
        if (close(fd) != 0) {
            DB_FATAL("Failed to close fd.socket_type:[%u],errinfo:[%s]",
                            socket_type, strerror(errno));
            return false;
        }
    }
    send_buf->byte_array_clear();
    self_buf->byte_array_clear();
    has_error_packet = false;

    ip.clear();
    port = 0;
    memset(&addr, 0, sizeof(addr));
    fd = -1;
    in_pool = false;
    socket_type = CLIENT_SOCKET;
    use_times = 0;
    send_buf_offset = 0;
    header_read_len = 0;
    header_offset = 0;
    is_authed = false;
    is_counted = false;
    packet_id = 0;
    packet_len = 0;
    packet_read_len = 0;
    is_handshake_send_partly = 0;
    is_auth_result_send_partly = 0;
    last_insert_id = 0;
    current_db.clear();
    username.clear();

    query_ctx.reset(new QueryContext);
    user_info.reset(new UserInfo);
    autocommit = true;
    //multi_state_txn = false;
    txn_id = 0;
    new_txn_id = 0;
    seq_id = 0;
    stmt_id = 0;

    for (auto& pair : cache_plans) {
        delete pair.second.root;
    }
    cache_plans.clear();
    for (auto& pair : prepared_plans) {
        delete pair.second;
    }
    prepared_plans.clear();
    need_rollback_seq.clear();
    region_infos.clear();
    return true;
}

bool NetworkSocket::transaction_has_write() {
    if (cache_plans.size() == 0) {
        // when prepare for autocommit dml, cache_plans is empty
        return true;
    }
    for (auto& pair : cache_plans) {
        pb::OpType type = pair.second.op_type;
        if (type == pb::OP_INSERT || type == pb::OP_DELETE || type == pb::OP_UPDATE) {
            return true;
        }
    }
    return false;
}

void NetworkSocket::on_begin(uint64_t txn_id) {
    this->txn_id = txn_id;
}

void NetworkSocket::on_commit_rollback() {
    txn_id = 0;
    new_txn_id = 0;
    seq_id = 0;
    need_rollback_seq.clear();
    //multi_state_txn = !autocommit;
    for (auto& pair : cache_plans) {
        delete pair.second.root;
    }
    cache_plans.clear();
    region_infos.clear();
}

bool NetworkSocket::reset_when_err() {
    packet_len = 0;
    packet_read_len = 0;
    packet_id = 0;

    send_buf_offset = 0;
    header_read_len = 0;
    header_offset = 0;
    is_auth_result_send_partly = 0;
    is_handshake_send_partly = 0;
    self_buf->byte_array_clear();
    send_buf->byte_array_clear();
    has_error_packet = false;
    query_ctx.reset(new QueryContext);
    return 0;
}

SocketPool::~SocketPool() {
    while (_pool.size() > 0) {
        //SmartSocket sock = _pool.front();
        //sock = _pool.front();
        _pool.pop_front();
        //delete sock;
    }
    pthread_mutex_destroy(&_pool_mutex);
}

SmartSocket SocketPool::fetch(SocketType type) {
    // Fetch socket from poll.
    SmartSocket sock;
    pthread_mutex_lock(&_pool_mutex);
    if (!_pool.empty()) {
        sock = _pool.front();
        _pool.pop_front();
        sock->use_times++;
    } else {
        // Create a new socket.
        sock = SmartSocket(new NetworkSocket());
        if (sock == nullptr) {
            DB_FATAL("Failed to new NetworkSocket.");
            pthread_mutex_unlock(&_pool_mutex);
            return SmartSocket();
        }
    }
    sock->conn_id = _cur_conn_id++;
    pthread_mutex_unlock(&_pool_mutex);
    sock->shutdown = false;
    // Set socket attribute.
    if (type == SERVER_SOCKET) {
        sock->socket_type = SERVER_SOCKET;
        int ret = socket(AF_INET, SOCK_STREAM, 0);
        if (ret < 0) {
            free(sock);
            DB_FATAL("Failed to set server socket.");
            return SmartSocket();
        }
        sock->fd = ret;
    } else {
        sock->socket_type = CLIENT_SOCKET;
        sock->packet_id = 0;
    }
    sock->in_pool = false;
    sock->last_active = time(nullptr);
    gettimeofday(&(sock->connect_time), nullptr);
    return sock;
}

void SocketPool::free(SmartSocket sock) {
    if (nullptr == sock) {
        return;
    }
    if (sock->use_times > NETWORK_SOCKET_MAX_USE_TIMES) {
        DB_FATAL("network_socket sock->fd=%d sock->is_client=%d use times=%d going to free",
                        sock->fd, sock->socket_type, sock->use_times);
        sock.reset();
    } else {
        if (!sock->reset()) {
            sock.reset();
            return;
        }
        sock->mutex.unlock();
        pthread_mutex_lock(&_pool_mutex);
        //boost::mutex::scoped_lock op_lock(_pool_mutex);
        if (sock->in_pool) {
            pthread_mutex_unlock(&_pool_mutex);
            return;
        }
        _pool.push_back(sock);
        sock->in_pool = true;
        pthread_mutex_unlock(&_pool_mutex);
    }
    return;
}

} // namespace baikal
