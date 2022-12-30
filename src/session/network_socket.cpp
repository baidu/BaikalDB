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

#include <boost/make_shared.hpp>
#include "network_socket.h"
#include "query_context.h"
#include "exec_node.h"
#include "binlog_context.h"

namespace baikaldb {
DECLARE_string(db_version);
static UserInfo dummy;
bvar::Adder<int64_t> NetworkSocket::bvar_prepare_count{"bvar_prepare_count"};

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

    send_buf_offset = 0;
    header_read_len = 0;
    fd = -1;
    port = 0;
    socket_type = CLIENT_SOCKET;
    use_times = 0;
    is_authed = false;
    is_counted = false;
    has_multi_packet = false;
    packet_id = 0;
    packet_len = 0;
    current_packet_len = 0;
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
    str_node.mutable_derive_node()->set_string_val("/home/mysql/mysql/share/mysql/english/");
    session_vars["language"] = str_node;
    str_node.mutable_derive_node()->set_string_val("OFF");
    session_vars["query_cache_type"] = str_node;
    str_node.mutable_derive_node()->set_string_val(" ");
    session_vars["sql_mode"] = str_node;
    str_node.mutable_derive_node()->set_string_val("CST");
    session_vars["system_time_zone"] = str_node;
    str_node.mutable_derive_node()->set_string_val("SYSTEM");
    session_vars["time_zone"] = str_node;
    str_node.mutable_derive_node()->set_string_val(FLAGS_db_version);
    session_vars["version"] = str_node;

    pb::ExprNode int_node;
    int_node.set_node_type(pb::INT_LITERAL);
    int_node.set_col_type(pb::INT64);
    int_node.set_num_children(0);
    int_node.mutable_derive_node()->set_int_val(28800);
    session_vars["interactive_timeout"] = int_node;
    int_node.mutable_derive_node()->set_int_val(0);
    session_vars["lower_case_table_names"] = int_node;
    int_node.mutable_derive_node()->set_int_val(268435456);
    session_vars["max_allowed_packet"] = int_node;
    int_node.mutable_derive_node()->set_int_val(16384);
    session_vars["net_buffer_length"] = int_node;
    int_node.mutable_derive_node()->set_int_val(60);
    session_vars["net_write_timeout"] = int_node;
    int_node.mutable_derive_node()->set_int_val(335544320);
    session_vars["query_cache_size"] = int_node;
    int_node.mutable_derive_node()->set_int_val(28800);
    session_vars["wait_timeout"] = int_node;
    int_node.mutable_derive_node()->set_int_val(1);
    session_vars["auto_increment_increment"] = int_node;
    session_vars["autocommit"] = int_node;

    bthread_mutex_init(&region_lock, nullptr);
}

std::atomic<uint64_t> NetworkSocket::txn_id_counter(1);

NetworkSocket::~NetworkSocket() {
    //DB_WARNING_CLIENT(this, "NetworkSocket close");
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
    int pre_size = prepared_plans.size();
    bvar_prepare_count << -pre_size;
    bthread_mutex_destroy(&region_lock);
}

bool NetworkSocket::transaction_has_write() {
    if (cache_plans.size() == 0) {
        // when prepare for autocommit dml, cache_plans is empty
        return true;
    }
    for (auto& pair : cache_plans) {
        pb::OpType type = pair.second.op_type;
        if (type == pb::OP_INSERT || type == pb::OP_DELETE || 
            type == pb::OP_UPDATE || type == pb::OP_SELECT_FOR_UPDATE) {
            return true;
        }
    }
    return false;
}

bool NetworkSocket::need_send_binlog() {
    if (!open_binlog) {
        return false;
    }
    return binlog_ctx != nullptr ? binlog_ctx->has_data_changed() : false;  
}

SmartBinlogContext NetworkSocket::get_binlog_ctx() {
    if (binlog_ctx == nullptr) {
        binlog_ctx.reset(new BinlogContext);
    }
    return binlog_ctx;
}

SmartQueryContex NetworkSocket::get_query_ctx() {
    BAIDU_SCOPED_LOCK(region_lock);
    return query_ctx;
}

void NetworkSocket::reset_query_ctx(QueryContext* ctx) {
    BAIDU_SCOPED_LOCK(region_lock);
    query_ctx.reset(ctx);
}

void NetworkSocket::on_begin() {
    txn_id = get_txn_id();
    seq_id = 0;
    primary_region_id = -1;
    auto time = butil::gettimeofday_us();
    DB_DEBUG("set txn start time %ld", time);
    txn_start_time = time;
    txn_pri_region_last_exec_time = time;
}

void NetworkSocket::on_commit_rollback() {
    BAIDU_SCOPED_LOCK(region_lock);
    update_old_txn_info();
    txn_id = 0;
    seq_id = 0;
    if (txn_start_time > 0) {
        query_ctx->stat_info.txn_alive_time = butil::gettimeofday_us() - txn_start_time;
    }
    txn_start_time = 0;
    txn_pri_region_last_exec_time = 0;
    open_binlog = false;
    need_rollback_seq.clear();
    //multi_state_txn = !autocommit;
    if (not_in_load_data) {
        for (auto& pair : cache_plans) {
            delete pair.second.root;
        }
        cache_plans.clear();
    }
    region_infos.clear();
    binlog_ctx.reset();
    addr_callids_map.clear();
    clear_txn_tid_set();
}

void NetworkSocket::update_old_txn_info() {
    // for print log
    if (query_ctx->stat_info.old_txn_id == 0) {
        query_ctx->stat_info.old_txn_id = txn_id;
        query_ctx->stat_info.old_seq_id = seq_id;
    }
}

bool NetworkSocket::reset_when_err() {
    packet_len = 0;
    current_packet_len = 0;
    packet_read_len = 0;
    packet_id = 0;
    has_multi_packet = false;
    send_buf_offset = 0;
    header_read_len = 0;
    is_auth_result_send_partly = 0;
    is_handshake_send_partly = 0;
    self_buf->byte_array_clear();
    send_buf->byte_array_clear();
    has_error_packet = false;
    query_ctx.reset(new QueryContext);
    return 0;
}

uint64_t NetworkSocket::get_global_conn_id() {
    uint64_t instance_part = server_instance_id & 0x7FFFFF;
    return (instance_part << 40 | (conn_id & 0xFFFFFFFFFFUL));
}

SmartSocket SocketFactory::create(SocketType type) {
    SmartSocket sock = SmartSocket(new NetworkSocket());
    if (sock == nullptr) {
        DB_FATAL("Failed to new NetworkSocket.");
        return SmartSocket();
    }
    sock->conn_id = _cur_conn_id++;
    sock->shutdown = false;
    // Set socket attribute.
    if (type == SERVER_SOCKET) {
        sock->socket_type = SERVER_SOCKET;
        int ret = socket(AF_INET, SOCK_STREAM, 0);
        if (ret < 0) {
            DB_FATAL("Failed to set server socket.");
            return SmartSocket();
        }
        sock->fd = ret;
    } else {
        sock->socket_type = CLIENT_SOCKET;
        sock->packet_id = 0;
    }
    sock->last_active = time(nullptr);
    gettimeofday(&(sock->connect_time), nullptr);
    return sock;
}

} // namespace baikaldb
