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

#pragma once

#include "data_buffer.h"
#include "common.h"
#include "mysql_err_handler.h"
#include "network_socket.h"
#include "mysql_err_code.h"

namespace baikaldb {

class NetworkSocket;

const uint32_t PACKET_HEADER_LEN                 = 4;
const uint32_t MAX_ERR_MSG_LEN                   = 2048;
const uint32_t MAX_WRITE_QUERY_RESULT_PACKET_LEN = 1048576;

enum MysqlCommand : uint8_t {
    // cmd name      cmd no    Associated client function (* means not supported by baikaldb)            
    COM_SLEEP               = 0x00,   // (default, e.g. SHOW PROCESSLIST)
    COM_QUIT                = 0x01,   // mysql_close
    COM_INIT_DB             = 0x02,   // mysql_select_db
    COM_QUERY               = 0x03,   // mysql_real_query
    COM_FIELD_LIST          = 0x04,   // mysql_list_fields
    COM_CREATE_DB           = 0x05,   // mysql_create_db
    COM_DROP_DB             = 0x06,   // mysql_drop_db
    COM_REFRESH             = 0x07,   // mysql_refresh
    COM_SHUTDOWN            = 0x08,   // 
    COM_STATISTICS          = 0x09,   // mysql_stat
    COM_PROCESS_INFO        = 0x0a,   // mysql_list_processes
    COM_CONNECT             = 0x0b,   // (during authentication handshake)
    COM_PROCESS_KILL        = 0x0c,   // mysql_kill
    COM_DEBUG               = 0x0d,
    COM_PING                = 0x0e,   // mysql_ping
    COM_TIME                = 0x0f,   // (special value for slow logs?)
    COM_DELAYED_INSERT      = 0x10,
    COM_CHANGE_USER         = 0x11,   // mysql_change_user
    COM_BINLOG_DUMP         = 0x12,   // 
    COM_TABLE_DUMP          = 0x13,
    COM_CONNECT_OUT         = 0x14,
    COM_REGISTER_SLAVE      = 0x15,
    COM_STMT_PREPARE        = 0x16,
    COM_STMT_EXECUTE        = 0x17,
    COM_STMT_SEND_LONG_DATA = 0x18,
    COM_STMT_CLOSE          = 0x19,
    COM_STMT_RESET          = 0x1a,
    COM_SET_OPTION          = 0x1b,
    COM_STMT_FETCH          = 0x1c
};

// Package mysql result field.
// This struct is same as mysql protocal.
typedef struct result_field_t {
    result_field_t() {}
    ~result_field_t() {}

    std::string     catalog = "def";
    std::string     db;
    std::string     table;
    std::string     org_table;
    std::string     name;       // Name of column
    std::string     org_name;
    uint32_t        charsetnr = 0;  // Character set.
    uint32_t        flags = 1;      // Div flags.
    int32_t         type = 0;       // Type of field. See mysql_com.h for types.
    uint32_t        decimals = 0;   // Number of decimals in field.
    uint64_t        length = 0;     // Width of column (create length).
} ResultField;

class MysqlWrapper {
public:
    ~MysqlWrapper() {}

    static MysqlWrapper* get_instance() {
        static MysqlWrapper wrapper;
        return &wrapper;
    }

    bool is_valid_command(uint8_t command);

    bool make_simple_ok_packet(SmartSocket s);
    bool make_err_packet(SmartSocket sock, MysqlErrCode err_code, const char* format, ...);
    bool make_eof_packet(DataBuffer* send_buf, int packet_num);
    bool make_string_packet(SmartSocket sock, const char* data, int len);
    bool make_field_packet(DataBuffer* array, const ResultField* field, int packet_id);
    bool make_row_packet(
            DataBuffer* send_buf,
            const std::vector<std::string>& row,
            int* send_packet_id);

    int handshake_send(SmartSocket sock);
    int auth_result_send(SmartSocket sock);
    int fill_auth_failed_packet(SmartSocket sock, const char* msg, int len);

    int protocol_get_string(uint8_t *data, int size, uint32_t* off, std::string &ret_str);
    int protocol_get_int_len(
            SmartSocket sock, 
            uint8_t* data,
            uint32_t* off, 
            size_t len, 
            uint64_t* result);

    int protocol_get_char(SmartSocket sock, uint8_t* data, uint32_t* off, uint8_t* result);

    int protocol_get_sql_string(
            uint8_t *packet, 
            int32_t packet_len,
            uint32_t* off,
            std::string& sql, 
            int sql_len);

    int real_read(SmartSocket sock, int we_want, int* ret_read_len);
    int real_write(SmartSocket sock);

    bool is_shutdown_command(uint8_t command);

private:
    MysqlWrapper();
    MysqlWrapper& operator=(const MysqlWrapper& other);
    MysqlErrHandler* _err_handler;
};

} // namespace baikal
