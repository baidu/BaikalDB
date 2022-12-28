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
#include <boost/algorithm/string.hpp>
#include "qos.h"

namespace baikaldb {
DEFINE_int64(qps_statistics_minutes_ago,     60,     "qps_statistics_minutes_ago, default: 1h"); // 默认以前一小时的统计信息作为参考
DEFINE_int64(max_tokens_per_second,          100000, "max_tokens_per_second, default: 10w");
DEFINE_int64(use_token_bucket,               0,      "use_token_bucket, 0:close; 1:open, default: 0");
DEFINE_int64(get_token_weight,               5,      "get_token_weight, default: 5");
DEFINE_int64(min_global_extended_percent,    40,     "min_global_extended_percent, default: 40%");
DEFINE_int64(token_bucket_adjust_interval_s, 60,     "token_bucket_adjust_interval_s, default: 60s");
DEFINE_int64(token_bucket_burst_window_ms,   10,     "token_bucket_burst_window_ms, default: 10ms");
DEFINE_int64(dml_use_token_bucket,           0,      "dml_use_token_bucket, default: 0");
DEFINE_int64(sql_token_bucket_timeout_min,   5,      "sql_token_bucket_timeout_min, default: 5min");
DEFINE_int64(qos_reject_interval_s,          30,     "qos_reject_interval_s, default: 30s");
DEFINE_int64(qos_reject_ratio,               90,     "qos_reject_ratio, default: 90%");
DEFINE_int64(qos_reject_timeout_s,           30*60,  "qos_reject_timeout_s, default: 30min");
DEFINE_int64(qos_reject_max_scan_ratio,      50,     "qos_reject_max_scan_ratio, default: 50%");
DEFINE_int64(qos_reject_growth_multiple,     100,    "qos_reject_growth_multiple, default: 100倍");
DEFINE_int64(qos_need_reject,                0,      "qos_need_reject, default: 0");

// need_statistics: 超过最小超额令牌时，不计入统计信息
int64_t TokenBucket::consume(int64_t expect_tokens, int64_t* expire_time, bool* need_statistics) {
    const int64_t now     = butil::gettimeofday_us();
    const int64_t divisor = now / (FLAGS_token_bucket_burst_window_ms * 1000);
    const int64_t window_start   = divisor * (FLAGS_token_bucket_burst_window_ms * 1000);
    const int64_t window_end     = (divisor + 1) * (FLAGS_token_bucket_burst_window_ms * 1000);
    const int64_t time_need = expect_tokens * _time_per_token;
    *expire_time = window_end;
    int64_t old_time = _time.load(std::memory_order_relaxed);
    int64_t new_time = old_time;
    if (new_time > window_end) {
        return -1;
    }

    if (!_valid) {
        return -1;
    }

    if (new_time < window_start) {
        // 前一个窗口的令牌丢弃
        new_time = window_start;
    }

    while (true) {
        if (new_time > window_end) {
            return -1;
        }

        int64_t desired_time = new_time + time_need;
        if (desired_time > window_end + _time_per_token) {
            // 该窗口内令牌不足，减少令牌获取量
            expect_tokens = (window_end - new_time) / _time_per_token + 1;
            desired_time = new_time + expect_tokens * _time_per_token;
        } 

        if (_time.compare_exchange_weak(old_time, desired_time)) {
            if (_is_extended && desired_time > (window_start + _statistics_threshold)) {
                // 获取的令牌超过了最小超额令牌桶限制，不计入qps统计，防止超过阈值后等比例压缩造成正常sql被挤压
                *need_statistics = false;
            } else {
                *need_statistics = true;
            }
            return expect_tokens;
        }

        new_time = old_time;

    }

    return 0;
}

// 归还令牌
void TokenBucket::return_tokens(const int64_t tokens, const int64_t expire_time) {
    const int64_t now     = butil::gettimeofday_us();
    const int64_t divisor = now / (FLAGS_token_bucket_burst_window_ms * 1000);
    const int64_t window_start = divisor * (FLAGS_token_bucket_burst_window_ms * 1000);
    const int64_t window_end   = (divisor + 1) * (FLAGS_token_bucket_burst_window_ms * 1000);
    const int64_t time_need    = tokens * _time_per_token;

    int64_t old_time = _time.load();
    if (expire_time < now || expire_time <= window_start || expire_time < old_time) {
        return;
    }

    int64_t new_time = old_time;

    while (true) {
        
        new_time -= time_need;
        if (new_time < window_start) {
            new_time = window_start;
        }

        if (_time.compare_exchange_weak(old_time, new_time)) {
            return;
        }

        if (old_time > expire_time) {
            return;
        }

        new_time = old_time;

    }
}

QosBthreadLocal::QosBthreadLocal(QosType type, const uint64_t sign, const int64_t index_id) : _qos_type(type), 
                                                                                            _sign(sign), 
                                                                                            _index_id(index_id) {
    _bthread_id = bthread_self();
    if (_sign != 0) {
        _sqlqos_ptr = StoreQos::get_instance()->get_sql_shared_ptr(sign);
        _sqlqos_ptr->inc_index_count(index_id);
    }
    _get_token_weight = FLAGS_get_token_weight;
    _use_token_bucket = FLAGS_use_token_bucket;
    if (_sign == 0) {
        // 非sql请求不使用令牌桶
        _use_token_bucket = 0;
    }

    // DML需要单独判断是否需要限流
    if (_use_token_bucket == 1 && type == QOS_DML) {
        _use_token_bucket = FLAGS_dml_use_token_bucket;
    }
}

QosBthreadLocal::~QosBthreadLocal() {
    if (_sqlqos_ptr != nullptr) {
        // sql统计
        _sqlqos_ptr->dec_index_count(_index_id);
        _sqlqos_ptr->sql_statistics_adder(1);
        _sqlqos_ptr->get_statistics_adder(_get_count, _get_statistics_count);
        _sqlqos_ptr->scan_statistics_adder(_scan_count, _scan_statistics_count);
    }
}

void QosBthreadLocal::update_statistics() {
    if (_sqlqos_ptr != nullptr) {
        _sqlqos_ptr->get_statistics_adder(_get_count, _get_statistics_count);
        _sqlqos_ptr->scan_statistics_adder(_scan_count, _scan_statistics_count);
        _get_count = 0;
        _scan_count = 0;
        _get_statistics_count = 0;
        _scan_statistics_count = 0;
    }
}

void QosBthreadLocal::rate_limiting(int64_t tokens, bool* need_statistics) {
    if (_use_token_bucket == 0) {
        // 不限流
        *need_statistics = true;
        return;
    }

    if (tokens <= 0) {
        *need_statistics = false;
        return;
    }

    int64_t expire_time = 0;
    _sqlqos_ptr->fetch_tokens(tokens, &expire_time, &_is_commited_bucket, need_statistics);

    RocksdbVars::get_instance()->qos_fetch_tokens_count << tokens;
}

bool QosBthreadLocal::need_reject() { 
    if (_sqlqos_ptr != nullptr) {
        return _sqlqos_ptr->need_reject(_index_id); 
    }
    return false;
}

bool QosBthreadLocal::is_new_sign() {
    if (_sqlqos_ptr != nullptr) {
        return _sqlqos_ptr->is_new_sign();
    }
    return false;
}

void SqlQos::get_statistics_adder(int64_t count, int64_t statistics_count) {
    _get_real.adder(count);
    _get_statistics.adder(statistics_count);
}

void SqlQos::scan_statistics_adder(int64_t count, int64_t statistics_count) {
    _scan_real.adder(count);
    _scan_statistics.adder(statistics_count);
}

void SqlQos::sql_statistics_adder(int64_t count) {
    _sql_real.adder(count);
    _sql_statistics.adder(count);
}

int64_t SqlQos::fetch_tokens(const int64_t tokens, int64_t* token_expire_time, bool* is_committed_bucket, bool* need_statistics) {
    
    TimeCost cost;
    bool wait = false;
    ON_SCOPE_EXIT(([this, &wait, &cost, tokens]() {
        _token_fetch.adder(tokens);
        if (wait) {
            RocksdbVars::get_instance()->qos_fetch_tokens_wait_time_cost << cost.get_time();
        }
    }));

    while (true) {
        int64_t expire_time = 0;
        int64_t count = _committed_bucket.consume(tokens, &expire_time);
        if (count > 0) {
            // 从承诺令牌桶中获取成功
            *is_committed_bucket = true;
            *token_expire_time = expire_time;
            *need_statistics = true;
            return count;
        }

        // 从全局超额令牌桶中获取令牌
        count = StoreQos::get_instance()->globle_extended_bucket_consume(tokens, &expire_time, need_statistics);
        if (count > 0) {
            *is_committed_bucket = false;
            *token_expire_time = expire_time;
            return count;
        }

        RocksdbVars::get_instance()->qos_fetch_tokens_wait_count << 1;
        // 获取令牌失败，等待到下一个窗口

        int64_t timeout_us = expire_time - butil::gettimeofday_us();
        if (timeout_us < 0) {
            RocksdbVars::get_instance()->qos_fetch_tokens_wait_count << -1;
            continue;
        }

        bthread_usleep(timeout_us);
        wait = true;
        RocksdbVars::get_instance()->qos_fetch_tokens_wait_count << -1;
    }
    return 0;
}

void SqlQos::return_tokens(const int64_t tokens, const bool is_committed_bucket, const int64_t expire_time) {
    if (is_committed_bucket) {
        _committed_bucket.return_tokens(tokens, expire_time);
    } else {
        StoreQos::get_instance()->globle_extended_bucket_return_tokens(tokens, expire_time);
    }
}

void QosReject::wheather_need_reject() {
    const int window_count = FLAGS_qos_reject_interval_s * 1000 / FLAGS_token_bucket_burst_window_ms;
    const int count = _count.get_value();
    bool match_reject_condition = (count * 100 / window_count > FLAGS_qos_reject_ratio);

    DB_WARNING("count: %d, window_count: %d, need_reject: %d, match_reject_condition: %d, time: %ld",
         count, window_count, _has_reject, match_reject_condition, _reject_begin_time.get_time());

    // 已经是拒绝状态
    if (_has_reject) {
        // 拒绝状态下压力无法降低，再次选择需要拒绝的sql
        if (match_reject_condition && _reject_begin_time.get_time() > FLAGS_qos_reject_interval_s * 1000 * 1000LL) {
            if (StoreQos::get_instance()->mark_need_reject_sql() >= 0) {
                _reject_begin_time.reset();
                DB_WARNING("mark need reject again count: %d, window_count: %d", count, window_count);
            }
        } else if (_reject_begin_time.get_time() > FLAGS_qos_reject_timeout_s * 1000 * 1000LL) {
            // 超时清理拒绝标记
            StoreQos::get_instance()->clear_need_reject();
            _has_reject = false;
            DB_WARNING("clear need reject, count: %d, window_count: %d", count, window_count);
        }
    } else if (match_reject_condition) {
        if (StoreQos::get_instance()->mark_need_reject_sql() >= 0) {
            _reject_begin_time.reset();
            _has_reject = true;
            DB_WARNING("mark need reject count: %d, window_count: %d", count, window_count);
        }
    } 
}

void StoreQos::token_bucket_modify() {
    int64_t max_token        = FLAGS_max_tokens_per_second; // 最大qps限制
    int64_t get_token_weight = FLAGS_get_token_weight;      // get需要的令牌权重，即scan_token =  get_token / weight
    if (get_token_weight <= 0) {
        get_token_weight = 5;
    }
    int64_t total_get_qps    = 0;
    int64_t total_scan_qps   = 0;
    analyze_qps(total_get_qps, total_scan_qps);
    int64_t total_consume_token        = total_get_qps + total_scan_qps / get_token_weight;
    int64_t min_global_extended_tokens = max_token * FLAGS_min_global_extended_percent / 100;// 最小全局超额令牌个数
    int64_t global_extended_token      = max_token - total_consume_token;
    double  compression_ratio = 1.0; // 1.0 不压缩；< 1.0 等比例压缩

    if ((max_token - min_global_extended_tokens) < total_consume_token) {
        compression_ratio = (max_token - min_global_extended_tokens) * 1.0 / total_consume_token;
        global_extended_token = min_global_extended_tokens;
        total_consume_token = max_token - global_extended_token;
    }

    DB_WARNING("max_token: %ld, global_extended_token: %ld, total_consume_token: %ld, compression_ratio: %lf", 
                    max_token, global_extended_token, total_consume_token, compression_ratio);

    {
        int64_t now = butil::gettimeofday_us();
        DoubleBufQos::ScopedPtr ptr;
        if (_sign_sqlqos_map.Read(&ptr) != 0) {
            return;
        }
        for (auto iter : *ptr) {

            int64_t get_qps  = iter.second->rocksdb_get_qps();
            int64_t scan_qps = iter.second->rocksdb_scan_qps();
            int64_t consume_token = (get_qps + scan_qps / get_token_weight) * compression_ratio;
            // 修改承诺令牌桶速率
            iter.second->reset_committed_rate(consume_token);
            total_consume_token -= consume_token;
            if (consume_token != 0) {
                DB_WARNING("sign: %lu, committed rate: %ld", iter.first, consume_token);
            }

        }

        global_extended_token += total_consume_token;
        DB_WARNING("globle extended bucket rate: %ld", global_extended_token);
        // 修改全局超额令牌桶速率
        _globle_extended_bucket.reset_rate(global_extended_token);
    }

}

int StoreQos::mark_need_reject_sql() {
    DoubleBufQos::ScopedPtr ptr;
    if (_sign_sqlqos_map.Read(&ptr) != 0) {
        return -1;
    }
    
    if (ptr->empty()) {
        DB_WARNING("sign sqlqos map is empty");
        return -1;
    }

    std::map<uint64_t, SqlInfo> sql_with_multi_index;
    // std::map<int64_t, SqlInfo> get_qps_sqlinfo_map;
    std::set<uint64_t> affect_indexids;
    auto iter = ptr->begin();
    while (iter != ptr->end()) {
        auto cur_iter = iter++;
        std::set<uint64_t> tmp_affect_indexids = cur_iter->second->fill_sqlqos_info(sql_with_multi_index);
        for (uint64_t indexid : tmp_affect_indexids) {
            affect_indexids.insert(indexid);
        }
    }

    bool mark = false;
    // 使用多个索引的高危sql
    if (sql_with_multi_index.empty()) {
        return -1;
    }
    
    for (auto it = sql_with_multi_index.rbegin(); it != sql_with_multi_index.rend(); it++) {
        int64_t index_id = met_rejection_conditions(it->second, affect_indexids);
        if (index_id <= 0) {
            DB_WARNING("conditions not met. sign: %lu, log_term [get: %ld, scan: %ld], short_term [get: %ld, scan: %ld]", 
                it->first, it->second.get_long_term, it->second.scan_long_term, it->second.get_short_term, it->second.scan_short_term);
            continue;
        }

        auto sql_it = ptr->find(it->first);
        if (sql_it != ptr->end()) {
            DB_WARNING("sql use multi index need reject sign: %lu, log_term [get: %ld, scan: %ld], short_term [get: %ld, scan: %ld]", 
                it->first, it->second.get_long_term, it->second.scan_long_term, it->second.get_short_term, it->second.scan_short_term);
            sql_it->second->set_need_reject(index_id);
            mark = true;
        } 
    }

    // 没有标记成功
    if (!mark) {
        return -1;
    }

    return 0;
}

} // namespace baikaldb
