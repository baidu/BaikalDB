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

#include <stdint.h>
#ifdef BAIDU_INTERNAL
#include <baidu/rpc/channel.h>
#else
#include <brpc/channel.h>
#endif
#include "mem_row_descriptor.h"
#include "data_buffer.h"
#include "proto/store.interface.pb.h"
#include "proto/db.interface.pb.h"
#include "transaction_pool.h"
#include "transaction.h"
#include "reverse_index.h"
#include "reverse_interface.h"
#include "vector_index.h"
#include "row_batch.h"
#include "mysql_err_code.h"
#include "trace_state.h"
#include "statistics.h"
#include "memory_profile.h"
//#include "region_resource.h"
#include <arrow/acero/exec_plan.h>

using google::protobuf::RepeatedPtrField;

namespace baikaldb {
DECLARE_int32(single_store_concurrency);
DECLARE_int32(per_txn_max_num_locks);
struct TxnLimitMap {
    static TxnLimitMap* get_instance() {
        static TxnLimitMap _instance;
        return &_instance;
    }

    bool check_txn_limit(const uint64_t txn_id, const int row_count) {
        bool too_many = false;
        auto update_func = [&too_many, &row_count] (int& count) {
            count += row_count;
            if (count > FLAGS_per_txn_max_num_locks) {
                too_many = true;
            }
        };
        _txn_limit_mapping.init_if_not_exist_else_update(txn_id, false, update_func, row_count);
        return too_many;
    }

    void erase(const uint64_t txn_id) {
        _txn_limit_mapping.erase(txn_id);
    }

private:
    TxnLimitMap() {}
    ThreadSafeMap<uint64_t, int> _txn_limit_mapping;
};

struct StateOption {
    bool store_compute_separate = false;
    bool is_binlog_region = false;
};

class QueryContext;
class NetworkSocket;

class RuntimeStatePool;
typedef std::shared_ptr<MemRowDescriptor> SmartDescriptor;
typedef std::unordered_map<int64_t, std::pair<TimeCost, SmartDescriptor>> MemRowDescriptorMap;
class RuntimeState {

public:
    RuntimeState() {
        bthread_mutex_init(&_mem_lock, NULL);
    }
    ~RuntimeState();

    // baikalStore init
    int init(const pb::StoreReq& req,
        const pb::Plan& plan, 
        const RepeatedPtrField<pb::TupleDescriptor>& tuples,
        TransactionPool* pool, StateOption option);

    // MPP非主db init
    int init(const pb::RuntimeState& pb_rs);

    // baikaldb init
    int init(QueryContext* ctx, DataBuffer* send_buf);

    void to_proto(pb::RuntimeState* pb_rs);

    void reset_vectorize_info() {
        execute_type = pb::EXEC_ROW;
        vectorlized_parallel_execution = false; 
        acero_declarations.clear();
        sign_exec_type = SignExecType::SIGN_EXEC_NOT_SET;
        arrow_input_schemas.clear(); 
        is_simple_select = true;
        use_mpp = false;
        subquery_result_table.reset();
    }
    
    // for prepared txn recovery in BaikalDB
    //int init(const pb::CachePlan& commit_plan);

    void set_reverse_index_map(const std::map<int64_t, ReverseIndexBase*>& reverse_index_map) {
        _reverse_index_map = reverse_index_map;
    }
    void set_vector_index_map(const std::map<int64_t, VectorIndex*>& vector_index_map) {
        _vector_index_map = vector_index_map;
    }
    std::map<int64_t, ReverseIndexBase*>& reverse_index_map() {
        return _reverse_index_map;
    }
    std::map<int64_t, VectorIndex*>& vector_index_map() {
        return _vector_index_map;
    }
    void conn_id_cancel(uint64_t db_conn_id);
    void cancel() {
        _is_cancelled = true;
    }
    bool is_cancelled() {
        return _is_cancelled;
    }
    pb::TupleDescriptor* get_tuple_desc(int tuple_id) {
        if (tuple_id < 0 || tuple_id >= (int32_t)_tuple_descs.size()) {
            return nullptr;
        }
        return &_tuple_descs[tuple_id];
    }
    std::vector<pb::TupleDescriptor>* mutable_tuple_descs() {
        return &_tuple_descs;
    }
    int32_t get_slot_id(int32_t tuple_id, int32_t field_id) {
        if (tuple_id >= (int32_t)_tuple_descs.size()) {
            return -1;
        }
        for (const auto& slot_desc : _tuple_descs[tuple_id].slots()) {
            if (slot_desc.field_id() == field_id) {
                return slot_desc.slot_id();
            }
        }
        return -1;
    }
    const std::vector<pb::TupleDescriptor>& tuple_descs() {
        return _tuple_descs;
    }
    MemRowDescriptor* mem_row_desc() {
        return _mem_row_desc.get();
    }
    int64_t region_id() {
        return _region_id;
    }
    int64_t region_version() {
        return _region_version;
    }
    int64_t table_id() {
        if (_resource != nullptr) {
            return _resource->region_info.table_id();
        }
        return 0;
    }
    DataBuffer* send_buf() {
        return _send_buf;
    }
    SmartTransaction txn() {
        return _txn;
    }

    void set_txn(SmartTransaction txn) {
        _txn = txn;
    }

    SmartTransaction create_txn_if_null(const Transaction::TxnOptions& txn_opts) {
        if (_txn != nullptr) {
            return _txn;
        }
        _txn = SmartTransaction(new Transaction(0, _txn_pool));
        _txn->set_resource(_resource);
        _txn->set_separate(is_separate);
        _txn->begin(txn_opts);
        return _txn;
    }
    SmartTransaction create_batch_txn() {
        auto txn = SmartTransaction(new Transaction(0, _txn_pool));
        txn->set_resource(_resource);
        txn->set_separate(is_separate);
        txn->begin(Transaction::TxnOptions());
        return txn;
    }
    void set_num_increase_rows(int64_t num) {
        _num_increase_rows = num;
    }
    int64_t num_increase_rows() const {
        return _num_increase_rows;
    }
    bool need_check_region() const {
        return _need_check_region;
    }

    void set_num_affected_rows(int64_t num) {
        _num_affected_rows = num;
    }

    void set_num_returned_rows(int64_t num) {
        _num_returned_rows = num;
    }

    void inc_num_returned_rows(int64_t num) {
        _num_returned_rows += num;
    }

    void inc_num_affected_rows(int64_t num) {
        _num_affected_rows += num;
    }

    int64_t num_affected_rows() {
        return _num_affected_rows;
    }

    int64_t num_returned_rows() {
        return _num_returned_rows;
    }
    void set_num_scan_rows(int64_t num) {
        _num_scan_rows = num;
    }
    int64_t num_scan_rows() {
        return _num_scan_rows;
    }

    void inc_num_scan_rows(int64_t num) {
        _num_scan_rows += num;
    }

    void set_read_disk_size(int64_t s) {
        _read_disk_size = s;
    }

    int64_t read_disk_size() {
        return _read_disk_size;
    }

    void set_num_filter_rows(int64_t num) {
        _num_filter_rows = num;
    }

    void inc_num_filter_rows(int64_t num) {
        _num_filter_rows += num;
    }

    void inc_num_filter_rows() {
        _num_filter_rows++;
    }

    int64_t num_filter_rows() {
        return _num_filter_rows;
    }

    int64_t db_handle_rows() {
        return _db_handle_rows;
    }

    void inc_db_handle_rows(int64_t num) {
        _db_handle_rows += num;
    }

    void set_db_handle_rows(int64_t num) {
        _db_handle_rows = num;
    }

    int64_t db_handle_bytes() {
        return _db_handle_bytes;
    }

    void inc_db_handle_bytes(int64_t bytes) {
        _db_handle_bytes += bytes;
    }

    void set_db_handle_bytes(int64_t bytes) {
        _db_handle_bytes = bytes;
    }

    void set_log_id(uint64_t logid) {
        _log_id = logid;
    }

    void set_sort_use_index() {
        _sort_use_index = true;
    }

    bool sort_use_index() {
        return _sort_use_index;
    }

    uint64_t log_id() {
        return _log_id;
    }

    size_t multiple_row_batch_capacity() {
        if (_row_batch_capacity * _multiple < ROW_BATCH_CAPACITY) {
            //两倍扩散
            _multiple *= 2;
            return _row_batch_capacity * _multiple;
        } else {
            return ROW_BATCH_CAPACITY;
        }
    }

    size_t row_batch_capacity() {
        return std::min(_row_batch_capacity * 2, ROW_BATCH_CAPACITY);
    }

    // Only used on baikaldb side
    void set_client_conn(NetworkSocket* socket) {
        _client_conn = socket;
    }

    NetworkSocket* client_conn() {
        return _client_conn;
    }

    TransactionPool* txn_pool() {
        return _txn_pool;
    }

    void set_resource(const std::shared_ptr<RegionResource>& resource) {
        _resource = resource;
    }
    void set_pool(RuntimeStatePool* pool) {
        _pool = pool;
    }
    // runtime release at last
    std::shared_ptr<RegionResource>& resource() {
        return _resource;
    }

    void set_single_sql_autocommit(bool single_sql_autocommit) {
        _single_sql_autocommit = single_sql_autocommit;
    }

    bool single_sql_autocommit() {
        return _single_sql_autocommit;
    }

    void set_optimize_1pc(bool optimize) {
        _optimize_1pc = optimize;
    }

    bool optimize_1pc() {
        return _optimize_1pc;
    }

    bool is_eos() { 
        return _eos;
    }

    bool use_backup() {
        return _use_backup;
    }

    bool need_learner_backup() const {
        return _need_learner_backup;
    }

    void set_eos() {
        _eos = true;
    }

    bool open_binlog() {
        return _open_binlog;
    }

    void set_open_binlog(bool flag) {
        _open_binlog = flag;
    }

    bool single_txn_cached() {
        return _single_txn_cached;
    }

    void set_single_txn_cached() {
        _single_txn_cached = true;
    }

    void set_single_txn_need_separate_execute(bool flag) {
        _single_txn_need_separate_execute = flag;
    }

    bool single_txn_need_separate_execute() {
        return _single_txn_need_separate_execute;
    }

    bool is_expr_subquery() {
        return _is_expr_subquery;
    }

    std::string& remote_side() {
        return _remote_side;
    }

    void set_remote_side(const std::string& remote_side) {
        _remote_side = remote_side;
    }

    void set_is_expr_subquery(bool flag) {
        _is_expr_subquery = flag;
    }

    std::vector<TraceTimeCost>* get_trace_cost() {
        return &_trace_cost_vec;
    }

    std::vector<std::vector<ExprValue>>& get_subquery_exprs() {
        return _subquery_exprs;
    }

    std::vector<std::vector<ExprValue>>* mutable_subquery_exprs() {
        return &_subquery_exprs;
    }

    void set_primary_region_id(int64_t region_id) {
        _primary_region_id = region_id;
    }

    int64_t primary_region_id() const {
        return _primary_region_id;
    }

    bool is_from_subquery() {
        return _is_from_subquery;
    }

    bool is_union_subquery() {
        return _is_union_subquery;
    }

    void set_from_subquery(bool is_from_subquery) {
        _is_from_subquery = is_from_subquery;
    }
    void set_ctx(QueryContext* ctx) {
        _ctx = ctx;
    }
    QueryContext* ctx() {
        return _ctx;
    }

    int memory_limit_exceeded(int64_t rows_to_check, int64_t bytes);
    int memory_limit_release(int64_t rows_to_check, int64_t bytes);
    int memory_limit_release_all();

    int64_t calc_single_store_concurrency(pb::OpType op_type);
    int64_t get_single_store_concurrency() {
        return _single_store_concurrency;
    }

    void set_single_store_concurrency();
    int64_t get_cost_time() {
        return time_cost.get_time();
    }
    void prepare_reset() {
        time_cost.reset();
        region_count = 0;
        _num_increase_rows = 0;
        _num_affected_rows = 0;
        _num_returned_rows = 0;
        _num_scan_rows = 0;
        _num_filter_rows = 0;
        _read_disk_size = 0;
        _is_cancelled = false;
    }
    bool is_timeout() {
        return _sql_exec_timeout > 0 && time_cost.get_time()  > _sql_exec_timeout * 1000L;
    }
    uint64_t get_query_time() {
        return time_cost.get_start_time();
    }

    bool is_ddl_work() {
        return _is_ddl_work;
    }
    void set_is_ddl_work(bool is_ddl_work) {
        _is_ddl_work = is_ddl_work;
    }

    int reset_tuple_descs_and_mem_row_descriptor(const std::vector<pb::TupleDescriptor>& tuple_descs);

    void append_acero_declaration(const arrow::acero::Declaration& dec) {
        acero_declarations.emplace_back(std::move(dec));
    }
public:
    uint64_t          txn_id = 0;
    int32_t           seq_id = 0;
    int32_t           tuple_id = -1;
    MysqlErrCode      error_code = ER_ERROR_FIRST;
    std::ostringstream error_msg;
    bool              is_full_export = false;
    bool              is_separate = false;
    bool              need_condition_again = true; // update/delete在raft状态机外再次检查条件
    BthreadCond       txn_cond;
    std::function<void(RuntimeState* state, SmartTransaction txn)> raft_func;
    bool              need_txn_limit = false;
    pb::ErrCode       err_code = pb::SUCCESS;
    bool              is_explain = false;
    ExplainType       explain_type = EXPLAIN_NULL;
    // -------- 此部分皆为收集统计信息相关 ---------------
    std::shared_ptr<CMsketch> cmsketch = nullptr;
    std::shared_ptr<HyperLogLog> hll = nullptr;
    // 需要收集哪些统计信息
    std::shared_ptr<std::set<pb::StatisticType>> statistics_types;
    // 表总行数
    int64_t          table_rows = 0;
    // 采样行数
    int64_t          sample_rows = 0;
    // -------- 此部分皆为收集统计信息相关 ----------------
    int64_t          last_insert_id = INT64_MIN; //存储baikalStore last_insert_id(expr)更新的字段
    pb::StoreRes*    response = nullptr;

    bool             need_statistics = true; // 用于动态超时的时间统计，如果请求的实例非NORMAL或着返回backup的结果，则不记入统计

    // global index ddl 使用
    int32_t            ddl_scan_size = 0;
    int32_t            region_count = 0;
    bool               ddl_pk_key_is_full = true;
    std::string        ddl_max_pk_key;
    std::string        ddl_max_router_key;
    MysqlErrCode       ddl_error_code = ER_ERROR_FIRST;
    std::unique_ptr<std::string> first_record_ptr {nullptr};
    std::unique_ptr<std::string> last_record_ptr {nullptr};
    std::vector<int64_t> ttl_timestamp_vec;

    uint64_t          sign = 0;
    bool              need_use_read_index = false;
    bool              need_read_rolling = false;
    // for re
    int                 keypoint_range = 100 * 10000;
    int                 partition_threshold = 10000;
    int                 pre_split_region_cnt = -1;
    int                 range_count_limit = 0;
    int64_t           _sql_exec_timeout = -1;
    bool              _is_ddl_work = false;

    bool              must_have_one = false;


    // for acero vectorize
    pb::ExecuteType     execute_type = pb::EXEC_ROW;
    bool                vectorlized_parallel_execution = false; 
    std::vector<arrow::acero::Declaration>  acero_declarations;
    SignExecType sign_exec_type = SignExecType::SIGN_EXEC_NOT_SET;
    std::shared_ptr<arrow::Table>  subquery_result_table; // 非相关子查询结果
    // tuple id -> arrow schema
    std::unordered_map<int, std::shared_ptr<arrow::Schema>> arrow_input_schemas; 
    bool                is_simple_select = true;
    bool                force_vectorize = false; // For store, 强制走向量化

    // mpp
    bool                use_mpp = false;

    // 单表编码转换
    bool need_convert_charset = false;
    pb::Charset connection_charset = pb::CS_UNKNOWN;
    pb::Charset table_charset = pb::CS_UNKNOWN;

    static std::string localhost_address;
    bool force_single_rpc = false;

private:
    bool _is_inited    = false;
    bool _is_cancelled = false;
    bool _eos          = false;
    bool _open_binlog  = false;
    bool _single_txn_need_separate_execute  = false;
    bool _single_txn_cached = false;
    bool _is_expr_subquery = false;
    std::vector<pb::TupleDescriptor> _tuple_descs;
    uint64_t _tuple_sign = 0;
    SmartDescriptor _mem_row_desc;
    // MemRowDescriptor _mem_row_desc;
    int64_t          _region_id = 0;
    int64_t          _region_version = 0;
    // index_id => ReverseIndex
    std::map<int64_t, ReverseIndexBase*> _reverse_index_map;
    std::map<int64_t, VectorIndex*> _vector_index_map;
    DataBuffer*     _send_buf= nullptr;
    bool _need_delete_ctx = false;
    bool _need_delete_client_conn = false;
    bool _need_check_region = true;

    int64_t _num_increase_rows = 0; //存储净新增行数
    int64_t _num_affected_rows = 0; //存储baikaldb写影响的行数
    int64_t _num_returned_rows = 0; //存储baikaldb读返回的行数
    int64_t _num_scan_rows     = 0; //存储baikalStore扫描行数
    int64_t _num_filter_rows   = 0; //存储过滤行数
    int64_t _read_disk_size = 0; // 扫描大小
    int64_t _db_handle_rows = 0; //存储baikaldb处理行数
    int64_t _db_handle_bytes = 0; //存储baikaldb处理字节数
    uint64_t _log_id = 0;

    bool              _single_sql_autocommit = true;     // used for baikaldb and store
    bool              _optimize_1pc = false;  // 2pc de-generates to 1pc when autocommit=true and
    // 如果用了排序列做索引，就不需要排序了
    bool              _sort_use_index = false;
    bool              _use_backup = false;
    bool              _need_learner_backup = false;
                                              // there is only 1 region.
    NetworkSocket*    _client_conn = nullptr; // used for baikaldb
    int64_t           _single_store_concurrency = -1; // used for baikaldb
    TransactionPool*  _txn_pool = nullptr;    // used for store
    SmartTransaction  _txn = nullptr;         // used for store
    std::shared_ptr<RegionResource> _resource;// used for store
    int64_t           _primary_region_id = -1;// used for store
    std::vector<int64_t> _scan_indices;
    size_t _row_batch_capacity = ROW_BATCH_CAPACITY;
    int _multiple = 1;
    RuntimeStatePool* _pool = nullptr;
    //trace使用
    std::vector<TraceTimeCost> _trace_cost_vec;
    std::vector<std::vector<ExprValue>> _subquery_exprs;
    // mem limit
    std::atomic<int64_t> _used_bytes{0};
    bthread_mutex_t  _mem_lock;
    SmartMemTracker  _mem_tracker = nullptr;
    std::string      _remote_side;
    TimeCost  time_cost;

    bool _is_from_subquery = false;
    QueryContext* _ctx = nullptr;

    bool _is_union_subquery = false;

    int set_mem_row_decriptor();
    //清理长期不使用的sql签名对应的MemRowDescriptor释放内存
    void clear_mem_row_descriptor();
    uint64_t tuple_descs_to_sign(const std::vector<pb::TupleDescriptor>& tuple_descs);

    //thread_local map:线程局部变量map,保存签名,  tuple_sign => pair<TimeCost, std::shared_ptr<SmartDescriptor>>, 避免重复BuildFile
    static thread_local MemRowDescriptorMap sql_sign_to_mem_row_descriptor;
};
typedef std::shared_ptr<RuntimeState> SmartState;
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
