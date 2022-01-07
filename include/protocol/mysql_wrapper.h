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

#include "data_buffer.h"
#include "common.h"
#include "mysql_err_handler.h"
#include "network_socket.h"
#include "mysql_err_code.h"

namespace baikaldb {

// https://dev.mysql.com/doc/internals/en/capability-flags.html
enum MysqlCapability {
    CLIENT_LONG_PASSWORD                    = 1 << 0,  /* new more secure passwords */
    CLIENT_FOUND_ROWS                       = 1 << 1,  /* Found instead of affected rows */
    CLIENT_LONG_FLAG                        = 1 << 2,  /* Get all column flags */
    CLIENT_CONNECT_WITH_DB                  = 1 << 3,  /* One can specify db on connect */
    CLIENT_NO_SCHEMA                        = 1 << 4,  /* Don't allow database.table.column */
    CLIENT_COMPRESS                         = 1 << 5,  /* Can use compression protocol */
    CLIENT_ODBC                             = 1 << 6,  /* Odbc client */
    CLIENT_LOCAL_FILES                      = 1 << 7,  /* Can use LOAD DATA LOCAL */
    CLIENT_IGNORE_SPACE                     = 1 << 8,  /* Ignore spaces before '(' */
    CLIENT_PROTOCOL_41                      = 1 << 9,  /* New 4.1 protocol */
    CLIENT_INTERACTIVE                      = 1 << 10, /* This is an interactive client */
    CLIENT_SSL                              = 1 << 11, /* Switch to SSL after handshake */
    CLIENT_IGNORE_SIGPIPE                   = 1 << 12, /* IGNORE sigpipes */
    CLIENT_TRANSACTIONS                     = 1 << 13, /* Client knows about transactions */
    CLIENT_RESERVED                         = 1 << 14, /* Old flag for 4.1 protocol  */
    CLIENT_SECURE_CONNECTION                = 1 << 15, /* New 4.1 authentication */
    CLIENT_MULTI_STATEMENTS                 = 1 << 16, /* Enable/disable multi-stmt support */
    CLIENT_MULTI_RESULTS                    = 1 << 17, /* Enable/disable multi-results */
    CLIENT_PS_MULTI_RESULTS                 = 1 << 18, /* Multi-results in PS-protocol */
    CLIENT_PLUGIN_AUTH                      = 1 << 19, /* Client supports plugin authentication */
    CLIENT_CONNECT_ATTRS                    = 1 << 20, /* Client supports connection attributes */
    CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA   = 1 << 21, /* Enable authentication response packet to be larger than 255 bytes. */
    CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS     = 1 << 22, /* Don't close the connection for a connection with expired password. */
    CLIENT_SESSION_TRACK                    = 1 << 30,
    CLIENT_DEPRECATE_EOF                    = 1 << 31,
};


class NetworkSocket;
const uint32_t PACKET_LEN_MAX                    = 0x00ffffff;
const uint32_t PACKET_HEADER_LEN                 = 4;
const uint32_t MAX_ERR_MSG_LEN                   = 2048;
const uint32_t MAX_WRITE_QUERY_RESULT_PACKET_LEN = 1048576;

class MysqlWrapper {
public:
    ~MysqlWrapper() {}

    static MysqlWrapper* get_instance() {
        static MysqlWrapper wrapper;
        return &wrapper;
    }

    bool is_valid_command(uint8_t command);

    bool make_simple_ok_packet(SmartSocket s);
    bool make_stmt_prepare_ok_packet(SmartSocket sock);
    bool make_err_packet(SmartSocket sock, MysqlErrCode err_code, const char* format, ...);
    bool make_eof_packet(DataBuffer* send_buf, const int packet_id);
    bool make_string_packet(SmartSocket sock, const char* data);
    bool make_field_packet(DataBuffer* array, const ResultField* field, const int packet_id);
    bool make_row_packet(
            DataBuffer* send_buf,
            const std::vector<std::string>& row,
            const int send_packet_id);

    int handshake_send(SmartSocket sock);
    int auth_result_send(SmartSocket sock);
    int fill_auth_failed_packet(SmartSocket sock, const char* msg);
    int protocol_get_char(uint8_t* data, int packet_len, uint32_t& offset, uint8_t* result);
    int real_read_header(SmartSocket sock, int want_len, int* real_read_len);
    int real_read(SmartSocket sock, int we_want, int* ret_read_len);
    int real_write(SmartSocket sock);

    bool is_shutdown_command(uint8_t command);
    bool is_prepare_command(uint8_t command);

    int protocol_get_string(
            uint8_t*        data, 
            int32_t         packet_len, 
            uint32_t&       offset, 
            std::string&    ret_str);

    int protocol_get_sql_string(
            uint8_t*        packet, 
            int32_t         packet_len,
            uint32_t&       offset,
            std::string&    sql, 
            int             sql_len);

    int protocol_get_length_fixed_int(
            uint8_t*    data,
            int32_t     packet_len,
            uint32_t&   offset,
            size_t      int_len,
            uint64_t&   result);

    // return is null or not
    int protocol_get_length_coded_int(
            uint8_t*    data, 
            int32_t     packet_len,
            uint32_t&   offset,
            uint64_t&   result,
            bool&       is_null);

    int decode_binary_protocol_value(
            uint8_t*    data, 
            int32_t     packet_len,
            uint32_t&   offset,
            SignedType  type,
            pb::ExprNode& node);

private:
    MysqlWrapper();
    MysqlWrapper& operator=(const MysqlWrapper& other);
    MysqlErrHandler* _err_handler;
};

} // namespace baikal
