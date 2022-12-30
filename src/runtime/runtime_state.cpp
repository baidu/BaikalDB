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

#include "runtime_state.h"
#include "runtime_state_pool.h"
#include "query_context.h"
#include "network_socket.h"
#include "packet_node.h"
namespace baikaldb {
DEFINE_int32(per_txn_max_num_locks, 1000000, "max num locks per txn default 100w");
DEFINE_int64(row_number_to_check_memory, 4096, "do memory limit when row number more than #, default: 4096");
DECLARE_int32(single_store_concurrency);
DECLARE_int64(baikaldb_alive_time_s);
DEFINE_int32(time_length_to_delete_message, 1, "hours length to delete mem_row_descriptor of sql : default one hour");
DEFINE_bool(limit_unappropriate_sql, false, "limit concurrency as one when select sql is unappropriate");
int RuntimeState::init(const pb::StoreReq& req,
        const pb::Plan& plan, 
        const RepeatedPtrField<pb::TupleDescriptor>& tuples,
        TransactionPool* pool,
        bool store_compute_separate, bool is_binlog_region) {
    //thread_local map:线程局部变量map,保存签名,  tuple_sign => pair<TimeCost, std::shared_ptr<SmartDescriptor>>, 避免重复BuildFile
    static thread_local MemRowDescriptorMap sql_sign_to_mem_row_descriptor;
    for (auto& tuple : tuples) {
        if (tuple.tuple_id() >= (int)_tuple_descs.size()) {
            _tuple_descs.resize(tuple.tuple_id() + 1);
        }
        _tuple_descs[tuple.tuple_id()] = tuple;
    }
    sign = req.sql_sign();
    uint64_t tuple_sign = tuple_descs_to_sign();

    //取出缓存的动态编译结果(按照签名)
    if (_tuple_descs.size() > 0 && sign != 0 && tuple_sign != 0) {
        if (sql_sign_to_mem_row_descriptor.count(tuple_sign) == 1) {
            _mem_row_desc = sql_sign_to_mem_row_descriptor[tuple_sign].second;
            sql_sign_to_mem_row_descriptor[tuple_sign].first.reset();//更新tuple_sign对应的使用时间
        } else {
            _mem_row_desc = std::make_shared<MemRowDescriptor>();
            int ret = _mem_row_desc->init(_tuple_descs);
            if (ret < 0) {
                DB_WARNING("_mem_row_desc init fail");
                return -1;
            }
            TimeCost start_time;
            sql_sign_to_mem_row_descriptor[tuple_sign] = {start_time, _mem_row_desc};
        }
    } else {
        _mem_row_desc = std::make_shared<MemRowDescriptor>();
        int ret = _mem_row_desc->init(_tuple_descs);
        if (ret < 0) {
            DB_WARNING("_mem_row_desc init fail");
            return -1;
        }
    }
    clear_mem_row_descriptor(sql_sign_to_mem_row_descriptor);//定期清理过期sql的mem_row_descriptor

    _region_id = req.region_id();
    _region_version = req.region_version();
    if (req.has_not_check_region()) {
        _need_check_region = !req.not_check_region();
    }
    if (is_binlog_region) {
        // binlog region 不检查
        _need_check_region = false;
    }
    int64_t limit = plan.nodes(0).limit();
    //DB_WARNING("limit:%ld", limit);
    if (limit > 0) {
        _row_batch_capacity = limit / 2 + 1;
    }
    if (req.txn_infos_size() > 0) {
        const pb::TransactionInfo& txn_info = req.txn_infos(0);
        if (txn_info.has_txn_id()) {
            txn_id = txn_info.txn_id();
        }
        if (txn_info.has_seq_id()) {
            seq_id = txn_info.seq_id();
        }
        // if (txn_info.has_autocommit()) {
        //     _autocommit = txn_info.autocommit();
        // }
        if (txn_info.has_primary_region_id()) {
            set_primary_region_id(txn_info.primary_region_id());
        }
    }
    if (pool == nullptr) {
        DB_WARNING("error: txn pool is null: %ld", _region_id);
        return -1;
    }
    is_separate = store_compute_separate;
    _log_id = req.log_id();
    _txn_pool = pool;
    _txn = _txn_pool->get_txn(txn_id);
    if (_txn != nullptr) {
        _txn->set_resource(_resource);
        _txn->set_separate(store_compute_separate);
    }
    return 0;
}

int RuntimeState::init(QueryContext* ctx, DataBuffer* send_buf) {
    //thread_local map:线程局部变量map,保存签名,  tuple_sign => pair<TimeCost, std::shared_ptr<SmartDescriptor>>, 避免重复BuildFile
    static thread_local MemRowDescriptorMap sql_sign_to_mem_row_descriptor;
    _num_increase_rows = 0; 
    _num_affected_rows = 0; 
    _num_returned_rows = 0; 
    _num_scan_rows     = 0; 
    _num_filter_rows   = 0; 
    set_client_conn(ctx->client_conn);
    if (_client_conn == nullptr) {
        return -1;
    }
    txn_id = _client_conn->txn_id;
    _log_id = ctx->stat_info.log_id;
    sign    = ctx->stat_info.sign;
    _use_backup = ctx->use_backup;
    _need_learner_backup = ctx->need_learner_backup;
    _single_store_concurrency = ctx->single_store_concurrency;
    need_use_read_index = ctx->need_use_read_index();
    // prepare 复用runtime
    if (_is_inited) {
        return 0;
    }
    _send_buf = send_buf;
    _tuple_descs = ctx->tuple_descs();
    uint64_t tuple_sign = tuple_descs_to_sign();

    //取出缓存的动态编译结果(按照签名)
    if (_tuple_descs.size() > 0 && sign != 0 && tuple_sign != 0) {
        if (sql_sign_to_mem_row_descriptor.count(tuple_sign) == 1) {
            _mem_row_desc = sql_sign_to_mem_row_descriptor[tuple_sign].second;
            sql_sign_to_mem_row_descriptor[tuple_sign].first.reset();//更新tuple_sign对应的使用时间   
        } else {
            _mem_row_desc = std::make_shared<MemRowDescriptor>();
            int ret = _mem_row_desc->init(_tuple_descs);
            if (ret < 0) {
                DB_WARNING("_mem_row_desc init fail");
                return -1;
            }
            TimeCost start_time;
            sql_sign_to_mem_row_descriptor[tuple_sign] = {start_time, _mem_row_desc};
        }
    } else {
        _mem_row_desc = std::make_shared<MemRowDescriptor>();
        int ret = _mem_row_desc->init(_tuple_descs);
        if (ret < 0) {
            DB_WARNING("_mem_row_desc init fail");
            return -1;
        }
    }
    clear_mem_row_descriptor(sql_sign_to_mem_row_descriptor);//定期清理过期sql的mem_row_descriptor

    if (ctx->open_binlog) {
        _open_binlog = true;
    }
    _is_inited = true;
    return 0;
}

int64_t RuntimeState::calc_single_store_concurrency(pb::OpType op_type) {
    // comment设置优先
    if (_single_store_concurrency > 0) {
        return _single_store_concurrency;
    }
    int64_t single_store_concurrency = FLAGS_single_store_concurrency;//默认并发度为20
    if (!FLAGS_limit_unappropriate_sql || op_type != pb::OP_SELECT) {
        return single_store_concurrency;
    }
    int64_t baikaldb_alive_time_us = SchemaFactory::get_instance()->get_baikaldb_alive_time_us();
    if (baikaldb_alive_time_us < FLAGS_baikaldb_alive_time_s * 1000 * 1000LL) {
        return single_store_concurrency;
    }
    if (sign == 0) {
        return single_store_concurrency;
    }
    auto schema_factory = SchemaFactory::get_instance();
    auto sql_stat_ptr = schema_factory->get_sql_stat(sign);
    if (sql_stat_ptr == nullptr || sql_stat_ptr->counter < SqlStatistics::SQL_COUNTS_RANGE) {
        single_store_concurrency = 1;
        DB_WARNING("select sql is unappropriate sql, need to limit concurrency as one, sql sign is [%lu]", sign);
    }
    return single_store_concurrency;
}

void RuntimeState::conn_id_cancel(uint64_t db_conn_id) {
    if (_pool != nullptr) {
        auto s = _pool->get(db_conn_id);
        if (s != nullptr) {
            s->cancel();
        }
    }
}

int RuntimeState::memory_limit_exceeded(int64_t rows_to_check, int64_t bytes) {
    if (rows_to_check < FLAGS_row_number_to_check_memory) {
        return 0;
    }
    if (_mem_tracker == nullptr) {
        // db侧region并发执行
        BAIDU_SCOPED_LOCK(_mem_lock);
        if (_mem_tracker == nullptr) {
            _mem_tracker = baikaldb::MemTrackerPool::get_instance()->get_mem_tracker(_log_id);
        }
    }
    _mem_tracker->consume(bytes);
    _used_bytes.fetch_add(bytes, std::memory_order_relaxed);
    if (_mem_tracker->has_limit_exceeded() || _mem_tracker->any_limit_exceeded()) {
        _mem_tracker->set_limit_exceeded();
        DB_WARNING("log_id:%lu memory limit Exceeded limit:%ld consumed:%ld used:%ld.", _log_id,
            _mem_tracker->bytes_limit(), _mem_tracker->bytes_consumed(), _used_bytes.load());
        BAIDU_SCOPED_LOCK(_mem_lock);
        error_code = ER_TOO_BIG_SELECT;
        error_msg.str("select reach memory limit");
        return -1;
    }
    return 0;
}

int RuntimeState::memory_limit_release(int64_t rows, int64_t bytes) {
    if (rows < FLAGS_row_number_to_check_memory) {
        return 0;
    }
    if (_mem_tracker != nullptr) {
        _mem_tracker->release(bytes);
    }
    _used_bytes.fetch_sub(bytes, std::memory_order_relaxed);
    return 0;
}

int RuntimeState::memory_limit_release_all() {
    if (_mem_tracker != nullptr) {
        _mem_tracker->release(_used_bytes);
    }
    _used_bytes = 0;
    return 0;
}

void RuntimeState::clear_mem_row_descriptor(MemRowDescriptorMap& sql_sign_to_mem_row_descriptor) {
    static thread_local TimeCost timecost; 
    int64_t time_pass = timecost.get_time();
    int64_t time_length_us = FLAGS_time_length_to_delete_message * 60 * 60 * 1000 * 1000LL; //transform hours to us 
    if (time_pass > time_length_us) { //定时清理
        timecost.reset();
        auto iter = sql_sign_to_mem_row_descriptor.begin();
        while (iter != sql_sign_to_mem_row_descriptor.end()) {
            auto& sign = iter->first;
            auto& time_cost = iter->second.first;
            auto sql_not_used_time = time_cost.get_time();//sql截止目前未被使用的时长(us)
            if (sql_not_used_time > time_length_us) {
                DB_NOTICE("current sql sign [%lu] is to to be erased, current map_descriptor_size is [%lu]", sign, sql_sign_to_mem_row_descriptor.size());
                iter = sql_sign_to_mem_row_descriptor.erase(iter);
            } else {
                iter++;
            }
        }
    }
}

uint64_t RuntimeState::tuple_descs_to_sign() {
    if (_tuple_descs.size() == 0) {
        return 0;
    }
    std::string str;
    str.reserve(100);
    for (auto& tuple : _tuple_descs) {
        std::string tmp;
        tuple.SerializeToString(&tmp);
        str += tmp;
    }
    uint64_t out[2];
    butil::MurmurHash3_x64_128(str.c_str(), str.size(), 0x1234, out);
    return out[0];
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
