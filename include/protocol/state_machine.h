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

#include <unordered_map>
#include <map>
#include <mutex>
#include <bvar/bvar.h>
#include "network_socket.h"
#include "epoll_info.h"
#include "mysql_wrapper.h"
#include "logical_planner.h"
#include "physical_planner.h"
#include "binlog_context.h"
#include "handle_helper.h"
#include "show_helper.h"

namespace baikaldb {

const std::string SQL_SELECT                     = "select";
const std::string SQL_SHOW                       = "show";
const std::string SQL_HANDLE                     = "handle";
const std::string SQL_EXPLAIN                    = "explain";
const std::string SQL_KILL                       = "kill";
const std::string SQL_USE                        = "use";
const std::string SQL_DESC                       = "desc";
const std::string SQL_CALL                       = "call";
const std::string SQL_SET                        = "set";
const std::string SQL_AUTOCOMMIT                 = "autocommit";
const std::string SQL_BEGIN                      = "begin";
const std::string SQL_ROLLBACK                   = "rollback";
const std::string SQL_START_TRANSACTION          = "start";
const std::string SQL_COMMIT                     = "commit";
const std::string SQL_SELECT_DATABASE            = "select database()";
const std::string SQL_SELECT_CONNECTION_ID       = "select connection_id()";


enum QUERY_TYPE {
    SQL_UNKNOWN_NUM                         = 0,
    SQL_SELECT_NUM                          = 1,
    SQL_SHOW_NUM                            = 2,
    SQL_EXPLAIN_NUM                         = 3,
    SQL_KILL_NUM                            = 4,
    SQL_USE_NUM                             = 5,
    SQL_DESC_NUM                            = 6,
    SQL_CALL_NUM                            = 7,
    SQL_SET_NUM                             = 8,
    SQL_CHANGEUSER_NUM                      = 9,
    SQL_PING_NUM                            = 10,
    SQL_STAT_NUM                            = 11,
    //SQL_START_TRANSACTION_NUM               = 16,
    //SQL_AUTOCOMMIT_1_NUM                    = 14,
    //SQL_BEGIN_NUM                           = 15,
    //SQL_ROLLBACK_NUM                        = 17,
    //SQL_COMMIT_NUM                          = 18,
    SQL_CREATE_DB_NUM                       = 19,
    SQL_DROPD_DB_NUM                        = 20,
    SQL_REFRESH_NUM                         = 21,
    SQL_PROCESS_INFO_NUM                    = 22,
    SQL_DEBUG_NUM                           = 23,
    SQL_FIELD_LIST_NUM                      = 28,
    //SQL_AUTOCOMMIT_0_NUM                    = 29,
    SQL_USE_IN_QUERY_NUM                    = 32,
    SQL_SET_NAMES_NUM                       = 34,
    SQL_SET_CHARSET_NUM                     = 35,
    SQL_SET_CHARACTER_SET_NUM               = 36,
    SQL_SET_CHARACTER_SET_CLIENT_NUM        = 37,
    SQL_SET_CHARACTER_SET_CONNECTION_NUM    = 38,
    SQL_SET_CHARACTER_SET_RESULTS_NUM       = 39,
    SQL_WRITE_NUM                           = 255
};

class StateMachine {
public:
    ~StateMachine() {
    }

    static StateMachine* get_instance() {
        static StateMachine smachine;
        return &smachine;
    }

    void run_machine(SmartSocket client, EpollInfo* epoll_info, bool shutdown);
    void client_free(SmartSocket socket, EpollInfo* epoll_info);

private:
    StateMachine(): dml_time_cost("dml_time_cost"),
                    select_time_cost("select_time_cost"),
                    sql_error("sql_error"),
                    sql_error_second("sql_error_second", &sql_error),
                    exec_sql_error("exec_sql_error"),
                    exec_sql_error_second("exec_sql_error_second", &exec_sql_error){
        _wrapper = MysqlWrapper::get_instance();
    }

    StateMachine& operator=(const StateMachine& other);

    int _auth_read(SmartSocket sock);
    int _read_packet_header(SmartSocket sock);
    int _read_packet(SmartSocket sock);
    int _query_read(SmartSocket sock);
    int _query_read_stmt_execute(SmartSocket sock);
    int _query_read_stmt_long_data(SmartSocket sock);
    int _get_query_type(std::shared_ptr<QueryContext> ctx);
    int _get_json_attributes(std::shared_ptr<QueryContext> ctx);
    bool _query_process(SmartSocket sock);
    void _parse_comment(std::shared_ptr<QueryContext> ctx);
    bool _handle_client_query_use_database(SmartSocket client);
    bool _handle_client_query_select_database(SmartSocket client);
    bool _handle_client_query_select_connection_id(SmartSocket client);
    bool _handle_client_query_common_query(SmartSocket client);
    bool _handle_client_query_desc_table(SmartSocket client);
    //int _make_common_resultset_packet(SmartSocket sock, SmartTable table);
    //int _make_common_resultset_packet(SmartSocket sock, SmartResultSet result_set);
    int _make_common_resultset_packet(SmartSocket sock,
                                        std::vector<ResultField>& fields,
                                        std::vector<std::vector<std::string> >& rows);

    int _query_result_send(SmartSocket sock);
    int _query_more(SmartSocket client, bool shutdown);
    bool _has_more_result(SmartSocket client);
    int _send_result_to_client_and_reset_status(EpollInfo* epoll_info, SmartSocket client);
    int _reset_network_socket_client_resource(SmartSocket client);
    void _print_query_time(SmartSocket client);

    bvar::LatencyRecorder dml_time_cost;
    bvar::LatencyRecorder select_time_cost;
    bvar::Adder<int> sql_error;
    bvar::Adder<int> exec_sql_error;
    bvar::PerSecond<bvar::Adder<int> > sql_error_second;
    bvar::PerSecond<bvar::Adder<int> > exec_sql_error_second;
    std::unordered_map<std::string, std::unique_ptr<bvar::LatencyRecorder> > select_by_users;
    std::unordered_map<std::string, std::unique_ptr<bvar::LatencyRecorder> > dml_by_users;
    std::mutex          _mutex;

    MysqlWrapper*   _wrapper = nullptr;

public:
    bvar::Adder<BvarMap> sql_agg_cost;
    // 索引推荐统计信息，一直积累，不清理
    bvar::Adder<BvarMap> index_recommend_st;
};

} // namespace baikal
