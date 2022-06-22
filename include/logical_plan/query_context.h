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
#include "user_info.h"
#include "proto/common.pb.h"
#include "mem_row_descriptor.h"
#include "table_record.h"
#include "runtime_state.h"
#include "base.h"
#include "expr.h"
#include "range.h"

namespace baikaldb {
DECLARE_bool(default_2pc);

class ExecNode;

// notice日志信息统计结构
struct QueryStat {
    int64_t     query_read_time;
    int64_t     query_plan_time;
    int64_t     query_exec_time;
    int64_t     result_pack_time;
    int64_t     result_send_time;
    int64_t     unit_to_req_time;
    int64_t     req_to_buf_time;
    int64_t     server_talk_time;
    int64_t     buf_to_res_time;
    int64_t     res_to_table_time;
    int64_t     table_get_row_time;
    int64_t     total_time;
    uint64_t    version;
    size_t      send_buf_size;
    int32_t     partition_key;
    
    int32_t     sql_length;
    int32_t     region_count;
    bool        hit_cache;
    timeval     start_stamp;
    timeval     send_stamp;
    timeval     end_stamp;
    //std::string traceid;
    std::string family;
    std::string table;
    std::string resource_tag;
    std::string server_ip;
    std::ostringstream sample_sql;
    std::string trace_id;
    uint64_t    sign = 0;
    int64_t     table_id = -1;

    MysqlErrCode error_code;
    std::ostringstream error_msg;

    int64_t     num_affected_rows = 0;
    int64_t     num_returned_rows = 0;
    int64_t     num_scan_rows     = 0;
    int64_t     num_filter_rows   = 0;
    uint64_t    log_id = 0;
    uint64_t    old_txn_id = 0;
    int         old_seq_id = 0;

    QueryStat() {
        reset();
    }

    void reset() {
        query_read_time     = 0;
        query_plan_time     = 0;
        query_exec_time     = 0;
        result_pack_time    = 0;
        result_send_time    = 0;
        unit_to_req_time    = 0;
        req_to_buf_time     = 0;
        server_talk_time    = 0;
        buf_to_res_time     = 0;
        res_to_table_time   = 0;
        table_get_row_time  = 0;
        total_time          = 0;
        version             = 0;
        send_buf_size       = 0;
        partition_key       = 0;
        sql_length          = 0;
        hit_cache           = false;
        gettimeofday(&(start_stamp), NULL);
        gettimeofday(&(send_stamp), NULL);
        gettimeofday(&(end_stamp), NULL);
        //traceid.clear();
        family.clear();
        table.clear();
        table_id = -1;
        server_ip.clear();
        sample_sql.str("");
        trace_id.clear();
        sign = 0;

        error_code          = ER_ERROR_FIRST;
        error_msg.str("");
        num_affected_rows   = 0;
        num_returned_rows   = 0;
        num_scan_rows       = 0;
        num_filter_rows     = 0;
        log_id              = butil::fast_rand();
        old_txn_id          = 0;
        old_seq_id          = 0;
        region_count        = 0;
    }
};

struct ExprParams {
    bool is_expr_subquery = false;
    bool is_correlated_subquery = false;
    parser::FuncType    func_type = parser::FT_COMMON;
    parser::CompareType cmp_type  = parser::CMP_ANY;
    // (a,b) in (select a,b from t) row_filed_number=2
    int  row_filed_number  = 1;
};

class QueryContext {
public:
    QueryContext() {
        enable_2pc = FLAGS_default_2pc;
    }
    QueryContext(std::shared_ptr<UserInfo> user, std::string db) : 
            cur_db(db),
            user_info(user) {
        enable_2pc = FLAGS_default_2pc;
    }

    ~QueryContext();

    void add_tuple(const pb::TupleDescriptor& tuple_desc) {
        if (tuple_desc.tuple_id() >= (int)_tuple_descs.size()) {
            _tuple_descs.resize(tuple_desc.tuple_id() + 1);
        }
        _tuple_descs[tuple_desc.tuple_id()] = tuple_desc;
    }

    pb::TupleDescriptor* get_tuple_desc(int tuple_id) {
        return &_tuple_descs[tuple_id];
    }
    std::vector<pb::TupleDescriptor>* mutable_tuple_descs() {
        return &_tuple_descs;
    }
    const std::vector<pb::TupleDescriptor>& tuple_descs() {
        return _tuple_descs;
    }
/*
    int32_t get_tuple_id(int64_t table_id) {
        for (auto& tuple_desc : _tuple_descs) {
            if (tuple_desc.table_id() == table_id) {
                return tuple_desc.tuple_id();
            }
        }
        return -1;
    }
*/
    int32_t get_slot_id(int32_t tuple_id, int32_t field_id) {
        for (const auto& slot_desc : _tuple_descs[tuple_id].slots()) {
            if (slot_desc.field_id() == field_id) {
                return slot_desc.slot_id();
            }
        }
        return -1;
    }
    SmartState get_runtime_state() {
        if (runtime_state == nullptr) {
            runtime_state.reset(new RuntimeState);
        }
        return runtime_state;
    }
    pb::PlanNode* add_plan_node() {
        return plan.add_nodes();
    }
    int create_plan_tree();

    void add_sub_ctx(std::shared_ptr<QueryContext>& ctx) {
        std::unique_lock<bthread::Mutex> lck(_kill_lock);
        sub_query_plans.emplace_back(ctx);
    }
    void set_kill_ctx(std::shared_ptr<QueryContext>& ctx) {
        std::unique_lock<bthread::Mutex> lck(_kill_lock);
        kill_ctx = ctx;
    }
    void kill_all_ctx() {
        std::unique_lock<bthread::Mutex> lck(_kill_lock);
        get_runtime_state()->cancel();
        for (auto& ctx : sub_query_plans) {
            ctx->get_runtime_state()->cancel();
        }
        if (kill_ctx) {
            kill_ctx->get_runtime_state()->cancel();
        }
    }
    void update_ctx_stat_info(RuntimeState* state, int64_t total_time);
    
    int64_t get_ctx_total_time();

public:
    std::string         sql;
    std::vector<std::string> comments;
    std::string         cur_db;
    std::string         charset;
    pb::TraceNode       trace_node;

    // new sql parser data structs
    parser::StmtNode*   stmt;
    parser::NodeType    stmt_type;
    bool                is_explain = false;
    bool                is_full_export = false;
    bool                is_straight_join = false;
    ExplainType         explain_type = EXPLAIN_NULL;

    uint8_t             mysql_cmd = COM_SLEEP;      // Command number in mysql protocal.
    int                 type;           // Query type. finer than mysql_cmd.
    int64_t             row_ttl_duration = 0; // used for /*{"duration": xxx}*/ insert ...
    QueryStat           stat_info;      // query execute result status info
    std::shared_ptr<UserInfo> user_info;

    pb::Plan            plan;
    ExecNode*           root = nullptr;
    std::map<int, ExprNode*> placeholders;
    std::string         prepare_stmt_name;
    std::vector<pb::ExprNode> param_values;

    SmartState          runtime_state;  // baikaldb side runtime state
    NetworkSocket*      client_conn = nullptr; // used for baikaldb
    // the insertion records, not grouped by region yet
    std::vector<SmartRecord>            insert_records;

    bool                succ_after_logical_plan = false;
    bool                succ_after_physical_plan = false;
    bool                return_empty = false;
    bool                new_prepared = false;  // flag for stmt_prepare
    bool                exec_prepared = false; // flag for stmt_execute
    bool                is_prepared = false;   // flag for stmt_execute
    bool                is_select = false;
    bool                need_destroy_tree = false;
    bool                has_derived_table = false;
    bool                has_information_schema = false;
    bool                is_complex = false;
    bool                use_backup = false;
    bool                need_learner_backup = false;
    int64_t             prepared_table_id = -1;
    ExprParams          expr_params;
    // field: column_id
    std::map<std::string, int32_t> field_column_id_mapping;
    // tuple_id: field: slot_id
    std::map<int64_t, std::map<std::string, int32_t>> ref_slot_id_mapping;
    // tuple_id: slot_id: column_id
    std::map<int64_t, std::map<int32_t, int32_t>>     slot_column_mapping;
    std::map<int64_t, std::shared_ptr<QueryContext>> derived_table_ctx_mapping;
    // 当前sql涉及的所有tuple
    std::set<int64_t>   current_tuple_ids;
    // 当前sql涉及的表的tuple
    std::set<int64_t>   current_table_tuple_ids;
    bool                open_binlog = false;

    // user can scan data in specific region by comments 
    // /*{"region_id":$region_id}*/ preceding a Select statement 
    int64_t             debug_region_id = -1;

    // user can scan data in specific peer by comments
    // /*{"peer_index":$peer_index}*/ preceding a Select statement
    int64_t             peer_index = -1;

    // in autocommit mode, two phase commit is disabled by default (for better formance)
    // user can enable 2pc by comments /*{"enable_2pc":1}*/ preceding a DML statement
    bool                enable_2pc = false;
    bool                is_cancelled = false;
    std::shared_ptr<QueryContext> kill_ctx;
    std::vector<std::shared_ptr<QueryContext>> sub_query_plans;
    std::unordered_map<uint64_t, std::string> long_data_vars;
    std::vector<SignedType> param_type;
    std::set<int64_t> index_ids;
    // 用于索引推荐
    std::map<int32_t, int> field_range_type;
    std::set<uint64_t> sign_blacklist;
    std::set<uint64_t> sign_forcelearner;

private:
    std::vector<pb::TupleDescriptor> _tuple_descs;
    bthread::Mutex _kill_lock;
};
} //namespace baikal
