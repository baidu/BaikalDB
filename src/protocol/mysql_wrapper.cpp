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

#include "mysql_wrapper.h"
#include <unordered_set>
#include "network_socket.h"
#include "query_context.h"
#include "packet_node.h"

namespace baikaldb {

MysqlWrapper::MysqlWrapper() {
    _err_handler = MysqlErrHandler::get_instance();
    _err_handler->init();
}

int MysqlWrapper::handshake_send(SmartSocket sock) {
    if (!sock) {
        DB_FATAL("sock==NULL");
        return RET_ERROR;
    }
    int ret = 0;
    // Check if handshake package is sent partly.
    if (sock->is_handshake_send_partly == 1) {
        ret = real_write(sock);
        sock->is_handshake_send_partly = (ret == RET_WAIT_FOR_EVENT ? 1 : 0);
        return ret;
    }
    // Send handshake package.
    const uint8_t packet_handshake_1[] =
        "\x0a"              // protocol version
        "5.0.51b\x00";      // server version

    // Send handshake package.
    const uint8_t packet_handshake_2[] =
        ""                  // connection id
        "\x26\x4f\x37\x58"  // auth-plugin-data-part-1
        "\x43\x7a\x6c\x53"  //
        "\x00"              // filter
        "\x0c\xa2"          // capability flags (lower 2 bytes)
        "\x1c"              // server language encoding :cp 1257 change to gbk
        "\x02\x00"          // server status
        "\x08\x00"          // capability flags (upper 2 bytes), CLIENT_PLUGIN_AUTH
        "\x15"              // length of auth-plugin-data
        "\x00\x00\x00\x00"
        "\x00\x00\x00\x00"
        "\x00\x00"          // 10bytes reserved
        "\x21\x25\x65\x57"
        "\x62\x35\x42\x66"
        "\x6f\x34\x62\x49"
        "\x00"              // auth-plugin-data-part-2 len=13
        "mysql_native_password\x00";    // auth-plugin name


    size_t len1 = sizeof(packet_handshake_1) - 1;
    size_t len2 = sizeof(packet_handshake_2) - 1;
    size_t len = len1 + len2 + 4;
    uint8_t packet_handshake[len];
    bzero(packet_handshake, len);

    memcpy(packet_handshake, packet_handshake_1, len1);
    memcpy(packet_handshake + len1, (uint8_t*)(&sock->conn_id), 4);
    memcpy(packet_handshake + len1 + 4, packet_handshake_2, len2);

    if (!sock->send_buf->network_queue_send_append(packet_handshake,
                                                (sizeof(packet_handshake)), 0, 0)) {
        DB_FATAL("Failed to append handshake package to socket buffer.");
        return ret;
    }
    ret = real_write(sock);
    if (ret == RET_WAIT_FOR_EVENT) {
        DB_WARNING("Handshake is sent partly.");
        sock->is_handshake_send_partly = 1;
    }
    return ret;
}

int MysqlWrapper::fill_auth_failed_packet(SmartSocket sock, const char* msg) {
    if (!sock) {
        DB_FATAL("sock == NULL");
        return RET_ERROR;
    }
    sock->send_buf->byte_array_clear();
    std::string tmp = "\xff\x15\x04#28000";
    tmp += msg;

    if (!sock->send_buf->network_queue_send_append((uint8_t*) tmp.c_str(), tmp.size() + 1, 2, 0)) {
        return RET_ERROR;
    }
    return RET_SUCCESS;
}

int MysqlWrapper::auth_result_send(SmartSocket sock) {
    if (!sock) {
        DB_FATAL("sock==NULL");
        return RET_ERROR;
    }
    int ret = 0;
    if (1 == sock->is_auth_result_send_partly) {
        ret = real_write(sock);
        sock->is_auth_result_send_partly = RET_WAIT_FOR_EVENT == ret ? 1 : 0;
        return ret;
    }
    sock->packet_id = 2;
    const uint8_t packet_ok[] = "\x00\x00\x00\x02\x00\x00\x00";
    if (!sock->send_buf->network_queue_send_append(packet_ok,
                                                sizeof(packet_ok) - 1,
                                                sock->packet_id, 0)) {
        DB_FATAL("Failed to network_queue_send_append().");
        return ret;
    }
    if (RET_WAIT_FOR_EVENT == (ret = real_write(sock))) {
        sock->is_auth_result_send_partly = 1;
    }  
    return ret;
}

int MysqlWrapper::protocol_get_char(
        uint8_t*    data,
        int         packet_len,
        uint32_t&   offset,
        uint8_t*    result) {
    if (nullptr == data || nullptr == result) {
        DB_FATAL("packet or result is null: %p, %p", data, result);
        return RET_ERROR;
    }
    if ((unsigned int)packet_len < offset + 1) {
        DB_FATAL("s->packet_len=%d + 4 < off=%d + 1", packet_len, offset);
        return RET_ERROR;
    }
    *result = data[offset++];
    return RET_SUCCESS;
}

int MysqlWrapper::protocol_get_length_fixed_int(
        uint8_t*    data,
        int32_t     packet_len,
        uint32_t&   offset,
        size_t      int_len,
        uint64_t&   result) {
    if (nullptr == data) {
        DB_FATAL("data == NULL");
        return RET_ERROR;
    }
    if ((uint32_t)packet_len < offset + int_len) {
        DB_FATAL("packet_len(%u) < offset(%u) + len(%lu)", packet_len, offset, int_len);
        return RET_ERROR;
    }
    uint64_t ret_int = 0;
    for (uint32_t i = 0; i < int_len; i++) {
        ret_int += (((uint64_t) data[offset + i]) << (i * 8));
    }
    offset += int_len;
    result = ret_int;
    return RET_SUCCESS;
}

int MysqlWrapper::protocol_get_string(
        uint8_t *data,
        int packet_len,
        uint32_t& offset,
        std::string &ret_str) {

    if (nullptr == data) {
        DB_FATAL("data is nullptr");
        return RET_ERROR;
    }
    int len = 0;
    while ((offset + len) < (unsigned int)packet_len && *(data + offset + len) != '\0') {
        ++len;
    }
    if (offset + len >= (unsigned int)packet_len) {
        DB_FATAL("protocol_get_string doesn't have \\0 char");
        return RET_ERROR;
    }
    if (len >= 0) {
        if (len + 1 > (int)MAX_STRING_BUF_SIZE) {
            DB_FATAL("String len is overflow buffer.len:[%d]", len + 1);
            return RET_ERROR;
        }
        char* tmp_buf = new (std::nothrow)char[MAX_STRING_BUF_SIZE];
        if (tmp_buf == NULL) {
            DB_FATAL("create tmp buf failed");
            return RET_ERROR;
        }
        memcpy(tmp_buf, data + offset, len);
        tmp_buf[len] = '\0';
        ret_str = tmp_buf;
        delete[]tmp_buf;

    } else {
        DB_FATAL("len <=0,len:[%d].", len);
        return RET_ERROR;
    }
    offset += len + 1;
    return RET_SUCCESS;
}

int MysqlWrapper::protocol_get_sql_string(uint8_t*  packet,
                                        int32_t     packet_len,
                                        uint32_t&   offset,
                                        std::string& sql,
                                        int sql_len) {

    if (NULL == packet) {
        DB_FATAL(" NULL == packet || NULL == off");
        return RET_ERROR;
    }
    if (packet_len < sql_len) {
        DB_FATAL("no enough data to read: packet_len=%d, sql_len=%d",
            packet_len, sql_len);
        return RET_ERROR;
    }
    sql.assign((char*)packet + offset, sql_len);
    offset += sql_len;
    return RET_SUCCESS;
}

bool MysqlWrapper::is_valid_command(uint8_t command) {
    // Include 99.9%  mysql command number.
    static std::unordered_set<uint8_t> cmd_set = {
        COM_QUIT, 
        COM_INIT_DB, 
        COM_QUERY, 
        COM_FIELD_LIST, 
        COM_CREATE_DB, 
        COM_DROP_DB,
        COM_REFRESH, 
        COM_SHUTDOWN, 
        COM_STATISTICS, 
        COM_PROCESS_INFO,
        COM_PROCESS_KILL,
        COM_DEBUG,
        COM_PING,
        COM_CHANGE_USER,
        COM_STMT_PREPARE,
        COM_STMT_EXECUTE, 
        COM_STMT_SEND_LONG_DATA,
        COM_STMT_CLOSE,
        COM_STMT_RESET,
        COM_SET_OPTION,
        COM_STMT_FETCH
    };
    return cmd_set.count(command) != 0;
}

// TODO: COM_SHUTDOWN -> shutdown the server
// TODO: COM_PROCESS_KILL -> kill pid
bool MysqlWrapper::is_shutdown_command(uint8_t command) {
    return (command == COM_QUIT         //0x01
        || command == COM_SHUTDOWN      //0x08 
        || command == COM_PROCESS_KILL);//0x0c  
}

bool MysqlWrapper::is_prepare_command(uint8_t command) {
    return (command == COM_STMT_PREPARE
        || command == COM_STMT_EXECUTE
        || command == COM_STMT_CLOSE);
}

bool MysqlWrapper::make_err_packet(SmartSocket sock, MysqlErrCode err_code, const char* format, ...) {
    if (!sock) {
        DB_FATAL("sock == null.");
        return false;
    }
    if (sock->send_buf == nullptr) {
        DB_FATAL("send_buf == null.");
        return false;
    }
    if (sock->has_error_packet) {
        // a more finer error packet has already been made
        return true;
    }
    DataBuffer* send_buf = sock->send_buf;
    if (send_buf->_size > 0) {
        send_buf->byte_array_clear();
        sock->packet_id = sock->last_packet_id;
    }
    MysqlErrorItem* item = _err_handler->get_error_item_by_code(err_code);
    if (item == nullptr) {
        DB_FATAL("no MysqlErrorItem found: %d", err_code);
        return false;
    }

    char err_msg[MAX_ERR_MSG_LEN];
    va_list args;
    va_start(args, format);
    vsnprintf(err_msg, sizeof(err_msg), format, args);
    va_end(args);

    int body_len = 9 + strlen(err_msg);
    uint8_t bytes[16];
    memset(bytes, 0, sizeof(bytes));
    bytes[0] = (body_len >> 0)  & 0xFF;
    bytes[1] = (body_len >> 8)  & 0xFF;
    bytes[2] = (body_len >> 16) & 0xFF;
    bytes[3] = (++sock->packet_id) & 0xFF;  //packet_id

    if (!send_buf->byte_array_append_len(bytes, 4)) {
        DB_FATAL("Failed to byte_array_append_len.bytes:[%s], len:[4]", bytes);
        return false;
    }
    memset(bytes, 0, sizeof(bytes));
    bytes[0] = 0xff;    // field_counf:255.

    // Error number
    bytes[1] = (uint8_t)(err_code & 0xFF);
    bytes[2] = (uint8_t)((err_code >> 8) & 0xFF);
    bytes[3] = '#';

    //sql_state, notice:put 5 charset to bytes, not including \0
    snprintf((char *)(bytes+4), 6, "%s", item->state_odbc.c_str());
    if (!send_buf->byte_array_append_len(bytes, 9))  {
        DB_FATAL("Failed to byte_array_append_len.bytes:[%s], len:[9]", bytes);
        return false;
    }
    if (!send_buf->byte_array_append_len((const uint8_t *)err_msg, strlen(err_msg))) {
        DB_FATAL("Failed to byte_array_append_len.bytes:[%s], len:[%lu]",
                        err_msg, strlen(err_msg));
        return false;
    }
    sock->query_ctx->stat_info.error_code = err_code;
    sock->has_error_packet = true;
    return true;
}

bool MysqlWrapper::make_field_packet(DataBuffer* array, const ResultField* field, const int packet_id) {
    if (array == NULL) {
        DB_FATAL("array is NULL.");
        return false;
    }

    uint8_t bytes[4];
    int start_pos = array->_size;
    if (!array->byte_array_append_len((const uint8_t *)"\x01\x00\x00", 3)) {
        DB_FATAL("byte_array_append_len failed.");
        return false;
    }

    // packet id
    bytes[0] = packet_id & 0xFF;;
    if (!array->byte_array_append_len(bytes, 1)) {
        DB_FATAL("byte_array_append_len failed.");
        return false;
    }
    // catalog
    if (!array->pack_length_coded_string(field->catalog, false)) {
        DB_FATAL("pack_length_coded_string failed.");
        return false;
    }

    // db
    if (!array->pack_length_coded_string(field->db, false)) {
        DB_FATAL("pack_length_coded_string failed.");
        return false;
    }

    // table
    if (!array->pack_length_coded_string(field->table, false)) {
        DB_FATAL("pack_length_coded_string failed.");
        return false;
    }

    // org_table
    if (!array->pack_length_coded_string(field->org_table, false)) {
        DB_FATAL("pack_length_coded_string failed.");
        return false;
    }

    // name
    if (!array->pack_length_coded_string(field->name, false)) {
        DB_FATAL("pack_length_coded_string failed.");
        return false;
    }

    // org_name
    if (!array->pack_length_coded_string(field->org_name, false)) {
        DB_FATAL("pack_length_coded_string failed.");
        return false;
    }

    // 1 byte filler
    bytes[0] = 0x0c;
    if (!array->byte_array_append_len(bytes, 1)) {
        DB_FATAL("byte_array_append_len failed.");
        return false;
    }

    // charsetnr
    bytes[0] = field->charsetnr & 0xff;
    bytes[1] = (field->charsetnr >> 8) & 0xff;
    if (!array->byte_array_append_len(bytes, 2)) {
        DB_FATAL("byte_array_append_len failed.");
        return false;
    }

    // length
    bytes[0] = field->length & 0xff;
    bytes[1] = (field->length >> 8) & 0xff;
    bytes[2] = (field->length >> 16) & 0xff;
    bytes[3] = (field->length >> 24) & 0xff;
    if (!array->byte_array_append_len(bytes, 4)) {
        DB_FATAL("byte_array_append_len failed.");
        return false;
    }

    // type
    bytes[0] = field->type & 0xff;
    if (!array->byte_array_append_len(bytes, 1)) {
        DB_FATAL("byte_array_append_len failed.");
        return false;
    }

    // flags
    bytes[0] = field->flags & 0xff;
    bytes[1] = (field->flags >> 8) & 0xff;
    if (!array->byte_array_append_len(bytes, 2)) {
        DB_FATAL("byte_array_append_len failed.");
        return false;
    }

    // decimals
    bytes[0] = field->decimals & 0xff;
    if (!array->byte_array_append_len(bytes, 1)) {
        DB_FATAL("byte_array_append_len failed.");
        return false;
    }

    // 2 bytes filler
    bytes[0] = 0;
    bytes[1] = 0;
    if (!array->byte_array_append_len(bytes, 2)) {
        DB_FATAL("byte_array_append_len failed.");
        return false;
    }

    int body_len = array->_size - start_pos - 4;
    array->_data[start_pos] = body_len & 0xff;
    array->_data[start_pos+1] = (body_len >> 8) & 0xff;
    array->_data[start_pos+2] = (body_len >> 16) & 0xff;

    return true;
}
bool MysqlWrapper::make_row_packet(DataBuffer* send_buf,
                                    const std::vector<std::string>& row,
                                    const int send_packet_id) {
    if (send_buf == NULL){
        DB_FATAL("invalid parameter, send_buf==null");
        return false;
    }
    // packet header
    int start_pos = send_buf->_size;
    uint8_t bytes[4];
    bytes[0] = '\x01';
    bytes[1] = '\x00';
    bytes[2] = '\x00';
    bytes[3] = send_packet_id & 0xFF;;
    if (!send_buf->byte_array_append_len(bytes, 4)) {
        DB_FATAL("Failed to append len. value:[%s], len:[1]", bytes);
        return false;
    }

    // package body.
    for (uint32_t cnt = 0; cnt < row.size(); ++cnt) {
        uint8_t null_byte = 0xfb;
        uint64_t length = row[cnt].size();
        if (length == 0) {
            if (!send_buf->byte_array_append_len((const uint8_t*)&null_byte, 1)) {
                DB_FATAL("Failed to append len.value:[%d],len:[1]", null_byte);
                return false;
            }
        } else {
            if (!send_buf->byte_array_append_length_coded_binary((unsigned long long)length)) {
                DB_FATAL("Failed to append length coded binary.length:[%lu]", length);
                return false;
            }
            if (!send_buf->byte_array_append_len(
                            (const uint8_t *)row[cnt].c_str(), length)) {
                DB_FATAL("Failed to append len. value:[%s],len:[%lu]",
                                row[cnt].c_str(), length);
                return false;
            }
        }
    }

    int packet_body_len = send_buf->_size - start_pos - 4;
    send_buf->_data[start_pos] = packet_body_len & 0xff;
    send_buf->_data[start_pos + 1] = (packet_body_len >> 8) & 0xff;
    send_buf->_data[start_pos + 2] = (packet_body_len >> 16) & 0xff;

    return true;
}

int MysqlWrapper::real_read_header(SmartSocket sock, int want_len, int* real_read_len) {
    if (!sock || NULL == sock->self_buf || NULL == real_read_len) {
        DB_FATAL("s == NULL || send_buf == NULL || NULL == real_read_len");
        return RET_ERROR;
    }
    // Check read want length.
    if (want_len <= 0) {
        *real_read_len = 0;
        DB_WARNING("want len <=0.want_len:[%d]", want_len);
        return RET_SUCCESS;
    }
    if (!sock->self_buf->byte_array_append_size(want_len, 1)) {
        DB_FATAL("Failed to byte_array_append_size.len:[%d]", want_len);
        return RET_ERROR;
    }
    int len = read(sock->fd, sock->self_buf->_data + sock->header_read_len, want_len);
    *real_read_len = (len >= 0 ? len : 0);
    if (sock->self_buf->_size < 4) {
        sock->self_buf->_size += *real_read_len;
    }
    if (len == -1) {
        if (errno == EAGAIN || errno == EINTR) {
            DB_TRACE("read() is wait for event.");
            return RET_WAIT_FOR_EVENT;
        } else {
            DB_WARNING("read() is failed.errno:[%d]", errno);
            return RET_SHUTDOWN;
        }
    } else if (len == 0) {
        DB_WARNING("read() len is 0 [fd=%d] want_len:[%d]", sock->fd, want_len);
        return RET_SHUTDOWN;
    }
    if (len < want_len) {
        return RET_WAIT_FOR_EVENT;
    }
    return RET_SUCCESS;
}

int MysqlWrapper::real_read(SmartSocket sock, int want_len, int* real_read_len) {
    if (!sock || NULL == sock->self_buf || NULL == real_read_len) {
        DB_FATAL("s == NULL || send_buf == NULL || NULL == real_read_len");
        return RET_ERROR;
    }
    // Check read want length.
    if (want_len <= 0) {
        *real_read_len = 0;
        DB_WARNING("want len <=0.want_len:[%d]", want_len);
        return RET_SUCCESS;
    }
    if (!sock->self_buf->byte_array_append_size(want_len, 1)) {
        DB_FATAL("Failed to byte_array_append_size.len:[%d]", want_len);
        return RET_ERROR;
    }
    int len = read(sock->fd, sock->self_buf->_data + sock->self_buf->_size, want_len);
    *real_read_len = (len >= 0 ? len : 0);
    sock->self_buf->_size += *real_read_len;

    if (len == -1) {
        if (errno == EAGAIN || errno == EINTR) {
            DB_TRACE("read() is wait for event.");
            return RET_WAIT_FOR_EVENT;
        } else {
            DB_WARNING("read() is failed.errno:[%d]", errno);
            return RET_SHUTDOWN;
        }
    } else if (len == 0) {
        DB_WARNING("read() len is 0 [fd=%d]", sock->fd);
        return RET_SHUTDOWN;
    }
    if (len < want_len) {
        return RET_WAIT_FOR_EVENT;
    }
    return RET_SUCCESS;
}

int MysqlWrapper::real_write(SmartSocket sock) {
    if (!sock || sock->send_buf == NULL) {
        DB_FATAL("sock == NULL or sock->send_buf == NULL");
        return RET_ERROR;
    }
    int ret = RET_ERROR;
    int32_t we_want = sock->send_buf->_size - sock->send_buf_offset;

    if (we_want <= 0) {
        if (sock->state == STATE_CONNECTED_CLIENT) {
            DB_WARNING("write handshake failed %s, %d, %d, we_want=%d buf_size=%lu "
                "buf_offset=%d",
                sock->ip.c_str(),
                sock->fd,
                sock->port,
                we_want,
                sock->send_buf->_size,
                sock->send_buf_offset);
        }
        return RET_SUCCESS;
    }
    int real_write = we_want;
    if (we_want > (int)MAX_WRITE_QUERY_RESULT_PACKET_LEN) {
        real_write = MAX_WRITE_QUERY_RESULT_PACKET_LEN;
    }
    int len = write(sock->fd, sock->send_buf->_data + sock->send_buf_offset, real_write);
    if (0 < len) {
        sock->send_buf_offset += len;
    } else if (len == 0) {
        return RET_SHUTDOWN;
    } else {
        switch (errno) {
            case EAGAIN:
                ret = RET_WAIT_FOR_EVENT;
                break;
            case EINTR:
                ret = RET_WAIT_FOR_EVENT;
                break;
            default:
                ret = RET_SHUTDOWN;
                break;
        }
        return ret;
    }
    if (len < real_write || we_want > (int)MAX_WRITE_QUERY_RESULT_PACKET_LEN) {
        ret = RET_WAIT_FOR_EVENT;
        return RET_WAIT_FOR_EVENT;
    }
    sock->send_buf->byte_array_clear();
    sock->self_buf->byte_array_clear();
    sock->send_buf_offset = 0;
    sock->packet_len = 0;
    return RET_SUCCESS;
}

bool MysqlWrapper::make_eof_packet(DataBuffer* send_buf, const int packet_id) {
    uint8_t bytes[4];
    bytes[0] = '\x05';
    bytes[1] = '\x00';
    bytes[2] = '\x00';
    bytes[3] = packet_id & 0xFF;
    if (!send_buf->byte_array_append_len(bytes, 4)) {
        DB_FATAL("Failed to append len.str:[%s], len[4]", bytes);
        return false;
    }
    if (!send_buf->byte_array_append_len((const uint8_t *)"\xfe\x00\x00\x02\x00", 5)) {
        DB_FATAL("Failed to append len.str:[\\xfe\\x00\\x00\\x02\\x00], len[5]");
        return false;
    }
    return true;
}

bool MysqlWrapper::make_simple_ok_packet(SmartSocket sock) {
    if (sock == nullptr) {
        DB_FATAL("sock==NULL");
        return false;
    }
    const uint8_t packet_ok[] =
        "\x00\x00"
        "\x00\x02"
        "\x00\x00\x00";
    if (sock->send_buf->_size > 0) {
        sock->send_buf->byte_array_clear();
    }
    return sock->send_buf->network_queue_send_append(packet_ok, (sizeof(packet_ok) - 1), ++sock->packet_id, 0);
}

// TODO: support "Parameter Definition Block" and "Column Definition Block"
// https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html
bool MysqlWrapper::make_stmt_prepare_ok_packet(SmartSocket sock) {
    if (sock == nullptr) {
        DB_FATAL("sock==NULL");
        return false;
    }
    auto iter = sock->prepared_plans.find(sock->query_ctx->prepare_stmt_name);
    if (iter == sock->prepared_plans.end()) {
        DB_WARNING("no prepare_stmt found, stmt_name: %s", sock->query_ctx->prepare_stmt_name.c_str());
        return false;
    }
    auto query_ctx = iter->second;
    ExecNode* plan = query_ctx->root;
    if (plan == nullptr) {
        DB_WARNING("prepare_stmt plan is null");
        return false;
    }
    PacketNode* packet_node = static_cast<PacketNode*>(plan->get_node(pb::PACKET_NODE));
    if (packet_node == nullptr) {
        DB_WARNING("prepare_stmt plan packet node is null");
        return false;
    }
    uint8_t status = '\x00';
    uint32_t stmt_id = (uint32_t)sock->stmt_id;
    uint16_t columns = packet_node->field_count();
    uint16_t parameters = query_ctx->placeholders.size();
    // DB_WARNING("stmt_id: %u, columns: %u, params: %u", stmt_id, columns, parameters);
    DataBuffer tmp_buf;
    if (!tmp_buf.byte_array_append_len(&status, 1)) {
        DB_FATAL("Failed to append status");
        return false;
    }
    uint8_t stmt_id_bytes[4];
    stmt_id_bytes[0] = (stmt_id & 0xff);
    stmt_id_bytes[1] = (stmt_id >> 8) & 0xff;
    stmt_id_bytes[2] = (stmt_id >> 16) & 0xff;
    stmt_id_bytes[3] = (stmt_id >> 24) & 0xff;
    if (!tmp_buf.byte_array_append_len(stmt_id_bytes, 4)) {
        DB_FATAL("Failed to append status");
        return false;
    }
    uint8_t columns_bytes[2];
    columns_bytes[0] = (columns & 0xff);
    columns_bytes[1] = (columns >> 8) & 0xff;
    if (!tmp_buf.byte_array_append_len(columns_bytes, 2)) {
        DB_FATAL("Failed to append status");
        return false;
    }
    uint8_t param_bytes[2];
    param_bytes[0] = (parameters & 0xff);
    param_bytes[1] = (parameters >> 8) & 0xff;
    if (!tmp_buf.byte_array_append_len(param_bytes, 2)) {
        DB_FATAL("Failed to append status");
        return false;
    }
    uint8_t filter = 0;
    tmp_buf.byte_array_append_len(&filter, 1);

    uint8_t warning_count_bytes[2];
    warning_count_bytes[0] = 0x00;
    warning_count_bytes[1] = 0x00;
    if (!tmp_buf.byte_array_append_len(warning_count_bytes, 2)) {
        DB_FATAL("Failed to append status");
        return false;
    }

    if (!sock->send_buf->network_queue_send_append(tmp_buf._data, tmp_buf._size, ++sock->packet_id, 0)) {
        DB_FATAL("Failed to append prepared_stmt init packet");
        return false;
    }
    for (int idx = 0; idx < parameters; ++idx) {
        ResultField field;
        field.catalog = "def";
        field.name = "?";
        field.type = 0xfd;
        field.charsetnr = 0x3f;
        field.flags = 0x80;
        if (!make_field_packet(sock->send_buf, &field, ++sock->packet_id)) {
            DB_FATAL("Failed to append prepared_stmt parameter packet");
            return false;
        }
    }
    if (parameters > 0) {
        make_eof_packet(sock->send_buf, ++sock->packet_id);        
    }
    if (columns > 0) {
        packet_node->pack_fields(sock->send_buf, sock->packet_id);
    }
    return true;
}

bool MysqlWrapper::make_string_packet(SmartSocket sock, const char* data) {
    if (!sock || data == NULL) {
        DB_FATAL("s == NULL || data == NULL ");
        return false;
    }
    std::string tmp = "\xff\x88\x88#88S88";
    tmp += data;
    if (!sock->send_buf->network_queue_send_append((uint8_t*)tmp.c_str(), tmp.size() + 1, ++sock->packet_id, 0)) {
        DB_FATAL("Failed to network_queue_send_append.");
        return false;
    }
    return true;
}

// return is null or not
// https://dev.mysql.com/doc/internals/en/integer.html#length-encoded-integer
int MysqlWrapper::protocol_get_length_coded_int(
        uint8_t*    data, 
        int32_t     packet_len,
        uint32_t&   offset,
        uint64_t&   result,
        bool&       is_null) {
    if ((uint32_t)packet_len < offset + 1) {
        DB_FATAL("packet_len(%d) < offset(%u) + len(%d)", packet_len, offset, 1);
        return RET_ERROR;
    }
    if (data[offset] > 254) {
        DB_FATAL("invalid length_coded header value: %u", data[offset]);
        return RET_ERROR;
    }
    is_null = false;
    result = 0;
    if (data[offset] == 251) {
        offset += 1;
        is_null = true;
        return RET_SUCCESS;
    }
    size_t int_length = 0;
    if (data[offset] <= 250) {
        result = data[offset];
        offset += 1;
    } else if (data[offset] == 252) {
        offset += 1;
        int_length = 2;
    } else if (data[offset] == 253) {
        offset += 1;
        int_length = 3;
    } else if (data[offset] == 254) {
        offset += 1;
        int_length = 8;
    }
    if (int_length == 0) {
        return RET_SUCCESS;
    }
    if (RET_SUCCESS != protocol_get_length_fixed_int(data, packet_len, offset, int_length, result)) {
        DB_WARNING("get 2-bytes integer failed");
        return RET_ERROR;
    }
    return RET_SUCCESS;
}

// https://dev.mysql.com/doc/refman/8.0/en/c-api-prepared-statement-type-codes.html
// https://dev.mysql.com/doc/internals/en/binary-protocol-value.html
// MYSQL_TYPE_TIME not correct FIXME
int MysqlWrapper::decode_binary_protocol_value(
        uint8_t*    data, 
        int32_t     packet_len,
        uint32_t&   offset,
        SignedType  type,
        pb::ExprNode& node) {

    // DB_WARNING("mysql_type: %u, is_unsigned: %d, offset: %u, packet_len: %lu", type.mysql_type, type.is_unsigned, offset, packet_len);
    switch (type.mysql_type) {
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_VAR_STRING: {
        uint64_t length = 0;
        bool is_null = false;
        if (RET_SUCCESS != protocol_get_length_coded_int(data, packet_len, offset, length, is_null)) {
            DB_FATAL("decode_binary_protocol_value failed");
            return RET_ERROR;
        }
        if (is_null) {
            DB_FATAL("decode_binary_protocol_value failed");
            return RET_ERROR;
        }
        if ((uint32_t)packet_len < offset + length) {
            DB_FATAL("packet_len(%u) < offset(%u) + len(%lu)", packet_len, offset, length);
            return RET_ERROR;
        }        
        node.set_node_type(pb::STRING_LITERAL);
        node.set_col_type(pb::STRING);
        node.set_num_children(0);
        pb::DeriveExprNode* str_node = node.mutable_derive_node();
        str_node->set_string_val((char*)data + offset, length);
        offset += length;
        break;
    }
    case MYSQL_TYPE_TINY: {
        uint64_t result;
        if (RET_SUCCESS != protocol_get_length_fixed_int(data, packet_len, offset, 1, result)) {
            DB_FATAL("decode uint8 failed");
            return RET_ERROR;
        }
        node.set_node_type(pb::INT_LITERAL);
        if (type.is_unsigned) {
            node.set_col_type(pb::UINT8);
        } else {
            node.set_col_type(pb::INT8);
        }
        node.set_num_children(0);
        pb::DeriveExprNode* str_node = node.mutable_derive_node();
        str_node->set_int_val(result);
        break;  
    }
    case MYSQL_TYPE_SHORT: {
        uint64_t result;
        if (RET_SUCCESS != protocol_get_length_fixed_int(data, packet_len, offset, 2, result)) {
            DB_FATAL("decode int64 failed");
            return RET_ERROR;
        }
        node.set_node_type(pb::INT_LITERAL);
        if (type.is_unsigned) {
            node.set_col_type(pb::UINT16);
        } else {
            node.set_col_type(pb::INT16);
        }
        node.set_num_children(0);
        pb::DeriveExprNode* str_node = node.mutable_derive_node();
        str_node->set_int_val(result);
        break;
    }
    case MYSQL_TYPE_LONG: 
    case MYSQL_TYPE_INT24: {
        uint64_t result;
        if (RET_SUCCESS != protocol_get_length_fixed_int(data, packet_len, offset, 4, result)) {
            DB_FATAL("decode int64 failed");
            return RET_ERROR;
        }
        //DB_WARNING("offset: %u, %lu", offset, result);
        node.set_node_type(pb::INT_LITERAL);
        if (type.is_unsigned) {
            node.set_col_type(pb::UINT32);
        } else {
            node.set_col_type(pb::INT32);
        }
        node.set_num_children(0);
        pb::DeriveExprNode* str_node = node.mutable_derive_node();
        str_node->set_int_val(result);
        break;
    }
    case MYSQL_TYPE_LONGLONG: {
        uint64_t result;
        if (RET_SUCCESS != protocol_get_length_fixed_int(data, packet_len, offset, 8, result)) {
            DB_FATAL("decode uint64 failed");
            return RET_ERROR;
        }
        node.set_node_type(pb::INT_LITERAL);
        if (type.is_unsigned) {
            node.set_col_type(pb::UINT64);
        } else {
            node.set_col_type(pb::INT64);
        }
        node.set_num_children(0);
        pb::DeriveExprNode* str_node = node.mutable_derive_node();
        str_node->set_int_val(result);
        break;
    }
    case MYSQL_TYPE_FLOAT: {
        if ((uint32_t)packet_len < offset + 4) {
            DB_FATAL("packet_len(%d) < offset(%u) + len(%d)", packet_len, offset, 4);
            return RET_ERROR;
        }
        uint8_t* p = (uint8_t*)(data+offset);
        float val = *reinterpret_cast<float*>(p);
        offset += 4;
        node.set_node_type(pb::DOUBLE_LITERAL);
        node.set_col_type(pb::FLOAT);
        node.set_num_children(0);
        pb::DeriveExprNode* float_node = node.mutable_derive_node();
        float_node->set_double_val(val);
        break;
    }
    case MYSQL_TYPE_DOUBLE: {
        if ((uint32_t)packet_len < offset + 8) {
            DB_FATAL("packet_len(%d) < offset(%u) + len(%d)", packet_len, offset, 8);
            return RET_ERROR;
        }
        uint8_t* p = (uint8_t*)(data+offset);
        double val = *reinterpret_cast<double*>(p);
        offset += 8;
        node.set_node_type(pb::DOUBLE_LITERAL);
        node.set_col_type(pb::DOUBLE);
        node.set_num_children(0);
        pb::DeriveExprNode* double_node = node.mutable_derive_node();
        double_node->set_double_val(val);
        break;
    }
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_TIMESTAMP: {
        uint64_t length;
        if (RET_SUCCESS != protocol_get_length_fixed_int(data, packet_len, offset, 1, length)) {
            DB_FATAL("decode uint8 failed");
            return RET_ERROR;
        }
        //DB_WARNING("lentgh: %lu type: %d", length, type.mysql_type);
        DateTime time_struct = DateTime();
        if (length == 4 || length == 7 || length == 11) {
            if (RET_SUCCESS != protocol_get_length_fixed_int(data, packet_len, offset, 2, time_struct.year)) {
                DB_FATAL("decode year failed type: %d", type.mysql_type);
                return RET_ERROR;
            }
            if (RET_SUCCESS != protocol_get_length_fixed_int(data, packet_len, offset, 1, time_struct.month)) {
                DB_FATAL("decode month failed type: %d", type.mysql_type);
                return RET_ERROR;
            }
            if (RET_SUCCESS != protocol_get_length_fixed_int(data, packet_len, offset, 1, time_struct.day)) {
                DB_FATAL("decode day failed type: %d", type.mysql_type);
                return RET_ERROR;
            }
            if (length == 7 || length == 11) {
                if (RET_SUCCESS != protocol_get_length_fixed_int(data, packet_len, offset, 1, time_struct.hour)) {
                    DB_FATAL("decode hour failed type: %d", type.mysql_type);
                    return RET_ERROR;
                }
                if (RET_SUCCESS != protocol_get_length_fixed_int(data, packet_len, offset, 1, time_struct.minute)) {
                    DB_FATAL("decode minute failed type: %d", type.mysql_type);
                    return RET_ERROR;
                }
                if (RET_SUCCESS != protocol_get_length_fixed_int(data, packet_len, offset, 1, time_struct.second)) {
                    DB_FATAL("decode second failed type: %d", type.mysql_type);
                    return RET_ERROR;
                }
            }
            if (length == 11) {
                if (RET_SUCCESS != protocol_get_length_fixed_int(data, packet_len, offset, 4, time_struct.macrosec)) {
                    DB_FATAL("decode macrosec failed type: %d", type.mysql_type);
                    return RET_ERROR;
                }
            }

        } else if (length == 0) {
            // nothing to do
        } else {
           DB_FATAL("wrong lentgh: %lu type: %d", length, type.mysql_type);
           return RET_ERROR;
        }
        node.set_num_children(0);
        pb::DeriveExprNode* date_node = node.mutable_derive_node();
        if (type.mysql_type == MYSQL_TYPE_DATE) {
            node.set_node_type(pb::DATETIME_LITERAL);
            node.set_col_type(pb::DATETIME);
            date_node->set_int_val(bin_date_to_datetime(time_struct));
        } else if (type.mysql_type == MYSQL_TYPE_DATETIME) {
            node.set_node_type(pb::DATETIME_LITERAL);
            node.set_col_type(pb::DATETIME);
            date_node->set_int_val(bin_date_to_datetime(time_struct));
        } else {
            node.set_node_type(pb::DATETIME_LITERAL);
            node.set_col_type(pb::DATETIME);
            date_node->set_int_val(bin_date_to_datetime(time_struct));
        }
        //DB_WARNING("DEBUG %s", node.DebugString().c_str());
        break;
    }
    case MYSQL_TYPE_TIME: {
        uint64_t length;
        if (RET_SUCCESS != protocol_get_length_fixed_int(data, packet_len, offset, 1, length)) {
            DB_FATAL("decode uint8 failed");
            return RET_ERROR;
        }
        //DB_WARNING("lentgh: %lu type: %d", length, type.mysql_type);
        DateTime time_struct = DateTime();
        if (RET_SUCCESS != protocol_get_length_fixed_int(data, packet_len, offset, 1, time_struct.is_negative)) {
            DB_FATAL("decode uint8 failed");
            return RET_ERROR;
        }
        if (length == 8 || length == 12) {
            if (RET_SUCCESS != protocol_get_length_fixed_int(data, packet_len, offset, 4, time_struct.day)) {
                DB_FATAL("decode day failed type: %d", type.mysql_type);
                return RET_ERROR;
            }
            if (RET_SUCCESS != protocol_get_length_fixed_int(data, packet_len, offset, 1, time_struct.hour)) {
                DB_FATAL("decode day failed type: %d", type.mysql_type);
                return RET_ERROR;
            }
            if (RET_SUCCESS != protocol_get_length_fixed_int(data, packet_len, offset, 1, time_struct.minute)) {
                DB_FATAL("decode minute failed type: %d", type.mysql_type);
                return RET_ERROR;
            }
            if (RET_SUCCESS != protocol_get_length_fixed_int(data, packet_len, offset, 1, time_struct.second)) {
                DB_FATAL("decode second failed type: %d", type.mysql_type);
                return RET_ERROR;
            }
            if (length == 12) {
                if (RET_SUCCESS != protocol_get_length_fixed_int(data, packet_len, offset, 4, time_struct.macrosec)) {
                    DB_FATAL("decode macrosec failed type: %d", type.mysql_type);
                    return RET_ERROR;
                }
            }
        } else if (length == 0) {
            // nothing to do
        } else {
           DB_FATAL("wrong lentgh: %lu type: %d", length, type.mysql_type);
           return RET_ERROR;
        }
        node.set_node_type(pb::TIME_LITERAL);
        node.set_col_type(pb::TIME);
        node.set_num_children(0);
        pb::DeriveExprNode* date_node = node.mutable_derive_node();
        date_node->set_int_val(bin_time_to_datetime(time_struct));
        //DB_WARNING("DEBUG %s", node.DebugString().c_str());
        break;
    }
    default:
        DB_FATAL("invalid type: %d", type.mysql_type);
        return RET_ERROR;
    }
    return RET_SUCCESS;
}
} // namespace baikal
