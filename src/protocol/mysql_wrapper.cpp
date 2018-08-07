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

#include "mysql_wrapper.h"
#include <unordered_set>
#include "network_socket.h"
#include "query_context.h"

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
        "\x2f\x55\x3e\x74"
        "\x50\x72\x6d\x4b"  // scramble_buf
        "\x00"              // filter
        "\x0c\xa2"          // server capabilities
        "\x1c"              // server language encoding :cp 1257 change to gbk
        "\x02\x00"          // server status
        "\x00\x00\x00\x00"
        "\x00\x00\x00\x00"
        "\x00\x00\x00\x00"
        "\x00\x56\x4c\x57"
        "\x54\x7c\x34\x2f"
        "\x2e\x37\x6b\x37"
        "\x6e\x00";

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

int MysqlWrapper::fill_auth_failed_packet(SmartSocket sock, const char* msg, int len) {
    if (!sock) {
        DB_FATAL("sock == NULL");
        return RET_ERROR;
    }
    sock->send_buf->byte_array_clear();
    const char *head_info = "\xff\x15\x04#28000";
    int tmp_len = len + strlen(head_info) + 1;

    char *tmp = new (std::nothrow) char[tmp_len];
    if (tmp == NULL) {
        DB_FATAL("allocate temp buf error!");
        return RET_ERROR;
    }
    bzero(tmp, tmp_len);

    if (tmp != strncat(tmp, head_info, strlen(head_info))) {
        DB_FATAL("strcat1 error");
        delete []tmp;
        return RET_ERROR;
    }
    if (tmp != strncat(tmp, msg, len)) {
        DB_FATAL("strcat2 error");
        delete []tmp;
        return RET_ERROR;
    }
    if (!sock->send_buf->network_queue_send_append((uint8_t*) tmp, tmp_len, 2, 0)) {
        delete []tmp;
        return RET_ERROR;
    }
    delete []tmp;
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
        SmartSocket sock,
        uint8_t* data,
        uint32_t* off,
        uint8_t* result) {
    if (nullptr == sock || nullptr == data || nullptr == off || nullptr == result) {
        DB_FATAL("sock or data is null: %p, %p", sock.get(), data);
        return RET_ERROR;
    }
    if ((unsigned int)(sock->packet_len + PACKET_HEADER_LEN) < *off + 1) {
        DB_FATAL("s->packet_len=%d + 4 < off=%d + 1", sock->packet_len, *off);
        return RET_ERROR;
    }
    *result = data[(*off)++];
    return RET_SUCCESS;
}

int MysqlWrapper::protocol_get_int_len(
        SmartSocket sock,
        uint8_t* data,
        uint32_t* off,
        size_t len,
        uint64_t* result) {
    if (!sock || nullptr == data || nullptr == off || len <= 0 || nullptr == result) {
        DB_FATAL("sock == NULL || data==NULL || NULL == off || len=%d <= 0", len);
        return RET_ERROR;
    }
    if ((unsigned int)(sock->packet_len + PACKET_HEADER_LEN) < *off + len) {
        DB_FATAL("s->packet_len=%d + 4 < off=%d + len=%d", sock->packet_len, *off, len);
        return RET_ERROR;
    }
    uint64_t ret_int = 0;
    for (uint32_t i = 0; i < len; i++) {
        ret_int += (uint8_t) data[*off + i] << (i * 8);
    }
    *off += len;
    *result = ret_int;
    return RET_SUCCESS;
}

int MysqlWrapper::protocol_get_string(
        uint8_t *data,
        int size,
        uint32_t* off,
        std::string &ret_str) {
    if (NULL == data || NULL == off) {
        DB_FATAL("data==NULL || NULL == off");
        return RET_ERROR;
    }

    int len = 0;
    while ((*off + len) < (unsigned int)size && *(data + *off + len) != '\0') {
        ++len;
    }
    if (*off + len >= (unsigned int)size) {
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
        memcpy(tmp_buf, data + *off, len);
        tmp_buf[len] = '\0';
        ret_str = tmp_buf;
        delete[]tmp_buf;

    } else {
        DB_FATAL("len <=0,len:[%d].", len);
        return RET_ERROR;
    }
    *off += len + 1;
    return RET_SUCCESS;
}

int MysqlWrapper::protocol_get_sql_string(uint8_t *packet,
                                        int32_t packet_len,
                                        uint32_t* off,
                                        std::string& sql,
                                        int sql_len) {

    if (NULL == packet || NULL == off) {
        DB_FATAL(" NULL == packet || NULL == off");
        return RET_ERROR;
    }
    if (packet_len < sql_len) {
        DB_FATAL("no enough data to read: packet_len=%d, sql_len=%d",
            packet_len, sql_len);
        return RET_ERROR;
    }

    char *sql_buf = new (std::nothrow)char[sql_len + 1];
    if (sql_buf == NULL) {
        DB_FATAL("allcate temp buffer for read sql string error");
        return RET_ERROR;
    }
    try {
        memcpy(sql_buf, packet + *off, sql_len);
    } catch (...) {
        DB_FATAL("exception in protocol_get_sql_string: %p, %p, %u",
            sql_buf, packet, *off);
        delete []sql_buf;
        return RET_ERROR;
    }
    sql_buf[sql_len] = '\0';
    sql = sql_buf;
    *off += sql_len;

    delete []sql_buf;
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
    bytes[3] = 1;  //packet_id

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
        DB_FATAL("Failed to byte_array_append_len.bytes:[%s], len:[%d]",
                        err_msg, strlen(err_msg));
        return false;
    }
    sock->query_ctx->stat_info.error_code = err_code;
    sock->has_error_packet = true;
    return true;
}

bool MysqlWrapper::make_field_packet(DataBuffer* array, const ResultField* field, int packet_id) {
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
    bytes[0] = packet_id & 0xff;
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
    if (!array->byte_array_append_len(bytes, 2)) {
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
/*
bool MysqlWrapper::make_row_packet(DataBuffer* send_buf,
                            SmartResultSet& result_set,
                            int* send_packet_id) {
    if (send_buf == NULL || send_packet_id == NULL){
        DB_FATAL("invalid parameter, send_buf==null or send_packet_id==null");
        return false;
    }
    // packet header
    int start_pos = send_buf->_size;
    uint8_t bytes[4];
    bytes[0] = '\x01';
    bytes[1] = '\x00';
    bytes[2] = '\x00';
    bytes[3] = (*send_packet_id) & 0xff;
    (*send_packet_id)++;
    if (!send_buf->byte_array_append_len(bytes, 4)) {
        DB_FATAL("Failed to append len. value:[%s], len:[1]", bytes);
        return false;
    }

    // package body.
    int size = result_set->get_field_count();
    for (int32_t cnt = 0; cnt < size; ++cnt) {
        uint8_t null_byte = 0xfb;
        const char* value = result_set->get_value(cnt);
        if (value == NULL) {
            if (!send_buf->byte_array_append_len((const uint8_t*)&null_byte, 1)) {
                DB_FATAL("Failed to append len.value:[%s],len:[1]", null_byte);
                return false;
            }
        } else {
            uint64_t length = strlen(value);
            if (!send_buf->byte_array_append_length_coded_binary((unsigned long long)length)) {
                DB_FATAL("Failed to append length coded binary.length:[%u]", length);
                return false;
            }
            if (!send_buf->byte_array_append_len((const uint8_t*)(value), length)) {
                DB_FATAL("Failed to append len. value:[%s],len:[%u]",
                                value, length);
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
*/
bool MysqlWrapper::make_row_packet(DataBuffer* send_buf,
                                    const std::vector<std::string>& row,
                                    int* send_packet_id) {
    if (send_buf == NULL || send_packet_id == NULL){
        DB_FATAL("invalid parameter, send_buf==null or send_packet_id==null");
        return false;
    }
    // packet header
    int start_pos = send_buf->_size;
    uint8_t bytes[4];
    bytes[0] = '\x01';
    bytes[1] = '\x00';
    bytes[2] = '\x00';
    bytes[3] = (*send_packet_id) & 0xff;
    (*send_packet_id)++;
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
                DB_FATAL("Failed to append len.value:[%s],len:[1]", null_byte);
                return false;
            }
        } else {
            if (!send_buf->byte_array_append_length_coded_binary((unsigned long long)length)) {
                DB_FATAL("Failed to append length coded binary.length:[%u]", length);
                return false;
            }
            if (!send_buf->byte_array_append_len(
                            (const uint8_t *)row[cnt].c_str(), length)) {
                DB_FATAL("Failed to append len. value:[%s],len:[%u]",
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
/*
bool MysqlWrapper::make_row_packet(DataBuffer* send_buf,
                                   SmartTable& table,
                                   int32_t row_idx,
                                   int* send_packet_id) {
    if (send_buf == NULL || send_packet_id == NULL){
        DB_FATAL("invalid parameter, send_buf==null or send_packet_id==null");
        return false;
    }
    // packet header
    int start_pos = send_buf->_size;
    uint8_t bytes[4];
    bytes[0] = '\x01';
    bytes[1] = '\x00';
    bytes[2] = '\x00';
    bytes[3] = (*send_packet_id) & 0xff;
    (*send_packet_id)++;
    if (!send_buf->byte_array_append_len(bytes, 4)) {
        DB_FATAL("Failed to append len. value:[%s], len:[1]", bytes);
        return false;
    }

    // package body.
    for (int32_t col_idx = 0; col_idx < table->col_size(); ++col_idx) {
        if (!send_buf->byte_array_append_table_cell(table, row_idx, col_idx)) {
            DB_FATAL("Failed to append table cell. row_idx:%d, col_idx:%d", row_idx, col_idx);
            return false;
        }
    }
    int packet_body_len = send_buf->_size - start_pos - 4;
    send_buf->_data[start_pos] = packet_body_len & 0xff;
    send_buf->_data[start_pos + 1] = (packet_body_len >> 8) & 0xff;
    send_buf->_data[start_pos + 2] = (packet_body_len >> 16) & 0xff;

    return true;
}
*/
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
    }
    else if (len == 0) {
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
    sock->send_buf_offset = 0;
    return RET_SUCCESS;
}

bool MysqlWrapper::make_eof_packet(DataBuffer* send_buf, int packet_id) {
    uint8_t bytes[4];
    bytes[0] = '\x05';
    bytes[1] = '\x00';
    bytes[2] = '\x00';
    bytes[3] = packet_id & 0xff;
    if (!send_buf->byte_array_append_len(bytes, 4)) {
        DB_FATAL("Failed to append len.str:[%s], len[4]", bytes);
        return false;
    }
    if (!send_buf->byte_array_append_len((const uint8_t *)"\xfe\x00\x00\x02\x00", 5)) {
        DB_FATAL("Failed to append len.str:[\xfe\x00\x00\x02\x00], len[5]");
        return false;
    }
    return true;
}

bool MysqlWrapper::make_simple_ok_packet(SmartSocket sock) {
    if (!sock) {
        DB_FATAL("sock==NULL");
        return RET_ERROR;
    }
    const uint8_t packet_ok[] =
        "\x00\x00"
        "\x00\x02"
        "\x00\x00\x00";
    if (sock->send_buf->_size > 0) {
        sock->send_buf->byte_array_clear();
    }
    return sock->send_buf->network_queue_send_append(packet_ok, (sizeof(packet_ok) - 1), 1, 0);
}

bool MysqlWrapper::make_string_packet(SmartSocket sock, const char* data, int len) {
    if (!sock || data == NULL || len <= 0) {
        DB_FATAL("s == NULL || data == NULL || len=%d <= 0", len);
        return false;
    }
    const char *head_info = "\xff\x88\x88#88S88";
    int tmp_len = len + strlen(head_info) + 1;
    char *tmp = new (std::nothrow) char[tmp_len];
    if (tmp == NULL) {
        DB_FATAL("allocate temp buf error!");
        return RET_ERROR;
    }
    bzero(tmp, tmp_len);

    if (tmp != strncat(tmp, head_info, strlen(head_info))) {
        DB_FATAL("strcat1 error");
        delete []tmp;
        return false;
    }
    if (tmp != strncat(tmp, data, len)) {
        DB_FATAL("strcat2 error");
        delete []tmp;
        return false;
    }
    if (!sock->send_buf->network_queue_send_append((uint8_t*)tmp, tmp_len, 1, 0)) {
        DB_FATAL("Failed to network_queue_send_append.");
        delete []tmp;
        return false;
    }
    delete []tmp;
    return true;
}

} // namespace baikal
