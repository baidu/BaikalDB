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
    bool make_string_packet(SmartSocket sock, const char* data, int len);
    bool make_field_packet(DataBuffer* array, const ResultField* field, const int packet_id);
    bool make_row_packet(
            DataBuffer* send_buf,
            const std::vector<std::string>& row,
            const int send_packet_id);

    int handshake_send(SmartSocket sock);
    int auth_result_send(SmartSocket sock);
    int fill_auth_failed_packet(SmartSocket sock, const char* msg, int len);
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
