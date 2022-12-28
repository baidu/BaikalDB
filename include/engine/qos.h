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

#include "common.h"

namespace baikaldb {
DECLARE_int64(sql_token_bucket_timeout_min);
DECLARE_int64(min_global_extended_percent);
DECLARE_int64(qps_statistics_minutes_ago);
DECLARE_int64(max_tokens_per_second);
DECLARE_int64(use_token_bucket);
DECLARE_int64(token_bucket_burst_window_ms);
DECLARE_int64(token_bucket_adjust_interval_s);
DECLARE_int64(get_token_weight);
DECLARE_int64(qos_reject_interval_s);
DECLARE_int64(qos_reject_ratio);
DECLARE_int64(qos_reject_timeout_s);
DECLARE_int64(qos_reject_max_scan_ratio);
DECLARE_int64(qos_reject_growth_multiple);
DECLARE_int64(qos_need_reject);

enum QosType {
    QOS_SELECT           = 0, 
    QOS_DML              = 1,
    QOS_REVERSE_MERGE    = 2,
    QOS_TYPE_NUM
};

struct IndexCounter {
    IndexCounter() : count(0) { }
    TimeCost begin_time; 
    TimeCost last_time;  
    int      count;
};

struct SqlInfo {
    int64_t  scan_short_term = 0;
    int64_t  get_short_term  = 0;
    int64_t  get_long_term   = 0;
    int64_t  scan_long_term  = 0;
    uint64_t sign            = 0;
    std::map<uint64_t, IndexCounter> indexid_count;
};

class SqlQos;

struct BvarWindowInfo {
    explicit BvarWindowInfo(int64_t interval_s) : interval_s(interval_s), query_per_window(&query_sum, interval_s) { 

    }

    void adder(int64_t count) {
        if (start_time == 0) {
            start_time = butil::gettimeofday_us();
        }
        query_sum << count;
    }

    int64_t get_qps_value() {
        if (butil::gettimeofday_us() - start_time < 5 * 1000) {
            return 0;
        }
        return query_per_window.get_value(interval_s) / interval_s;
    }

    int64_t get_window_value() {
        if (butil::gettimeofday_us() - start_time < 5 * 1000) {
            return 0;
        }
        return query_per_window.get_value(interval_s);
    }

    int64_t start_time = 0;
    int64_t interval_s = 0;
    bvar::Adder<int64_t> query_sum;
    bvar::Window<bvar::Adder<int64_t> > query_per_window;
};

// 令牌桶
class TokenBucket {
public:
    explicit TokenBucket (const bool is_extended) : _is_extended(is_extended), _valid(false) {

    }

    explicit TokenBucket (const bool is_extended, const int64_t rate) : _is_extended(is_extended) {
        reset_rate(rate);
    }

    // 返回值: <= 0 获取失败；> 0 获取的令牌个数
    // 出参:expire_time   如果获取成功expire_time为过期时间，获取失败时expire_time为需要等到该时刻再次获取
    // 入参:expect_tokens        期望获取令牌个数
    int64_t consume(int64_t expect_tokens, int64_t* expire_time) {
        bool need_statistics = false;
        return consume(expect_tokens, expire_time, &need_statistics);
    }

    int64_t consume(int64_t expect_tokens, int64_t* expire_time, bool* need_statistics);

    // 归还令牌
    void return_tokens(const int64_t tokens, const int64_t expire_time);

    // 重置令牌桶速率和并发窗口
    void reset_rate(const int64_t rate) {
        int64_t min_global_extended_tokens = FLAGS_max_tokens_per_second * FLAGS_min_global_extended_percent / 100;// 最小全局超额令牌个数
        _valid = true;
        _rate = rate;
        if (rate <= 0) {
            _valid = false;
        } else {
            _time_per_token = 1000000 / rate;
            if (_time_per_token <= 0) {
                _time_per_token = 1;
            }
        }

        // 设置最小全局超额阈值，用于判断是否需要统计qps
        if (_is_extended) {
            if (rate <= min_global_extended_tokens) {
                _statistics_threshold = 0;
            } else {
                _statistics_threshold = (rate - min_global_extended_tokens) / (1000000/*1s*/ / (FLAGS_token_bucket_burst_window_ms * 1000)) * _time_per_token;
            }
            DB_WARNING("statistics_threshold: %ld", _statistics_threshold);
        }
    }

    void set_valid(const bool valid) {
        _valid = valid;
    }

private:
    const bool    _is_extended = false; // 是否为超额令牌桶
    bool    _valid = false;
    int64_t _statistics_threshold = 0;
    int64_t _rate = 0;
    int64_t _time_per_token = 0;
    std::atomic<int64_t> _time = {0};
};

// 在同一个bthread中执行
class QosBthreadLocal {
public:

    QosBthreadLocal(QosType type, const uint64_t sign, const int64_t index_id);

    ~QosBthreadLocal();

    void get_rate_limiting() {
        ++_get_count;
        bool need_statistics = false;
        rate_limiting(1, &need_statistics);
        if (need_statistics) {
            ++_get_statistics_count;
        }
    }
    
    void scan_rate_limiting() {
        // scan 攒够 _get_token_weight 次，才获取令牌
        if (++_scan_count % _get_token_weight == 0) {
            bool need_statistics = false;
            rate_limiting(1, &need_statistics);
            if (need_statistics) {
                _scan_statistics_count += _get_token_weight;
            }
        }
    }

    uint64_t bthread_id() { return _bthread_id; }

    bool need_reject();
    bool is_new_sign();

    void update_statistics();

private:
    void rate_limiting(int64_t tokens, bool* need_statistics);
    std::shared_ptr<SqlQos> _sqlqos_ptr  = nullptr;
    int64_t _get_token_weight            = 0;
    int64_t _get_count                   = 0;
    int64_t _scan_count                  = 0;
    int64_t _get_statistics_count        = 0;
    int64_t _scan_statistics_count       = 0;
    int64_t _use_token_bucket            = 1;
    bool    _is_commited_bucket          = true; // local令牌是否是从承诺令牌桶里获取的，还令牌时用

    const QosType  _qos_type;
    const uint64_t _sign    = 0;
    const int64_t _index_id = 0;

    uint64_t _bthread_id = 0;

    DISALLOW_COPY_AND_ASSIGN(QosBthreadLocal);
};

class SqlQos {
public:
    explicit SqlQos(uint64_t sign) 
        : _sign(sign)
        , _committed_bucket(false)
        , _get_statistics(FLAGS_qps_statistics_minutes_ago * 60LL)
        , _scan_statistics(FLAGS_qps_statistics_minutes_ago * 60LL)
        , _get_real(FLAGS_qos_reject_interval_s)
        , _scan_real(FLAGS_qos_reject_interval_s)
        , _token_fetch(60)
        , _sql_statistics(FLAGS_qps_statistics_minutes_ago * 60LL)
        , _sql_real(FLAGS_qos_reject_interval_s) {

        _start_time = butil::gettimeofday_us();
    }

    int64_t expect_tokens_per_sql() {
        if (_sql_qps == 0) {
            return 1;
        }
        int64_t weight = FLAGS_get_token_weight;
        if (weight <= 0) {
            weight = 5;
        }
        return (_get_qps + _scan_qps / weight) / _sql_qps + 1;
    }

    void get_statistics_adder(int64_t count, int64_t statistics_count);

    void scan_statistics_adder(int64_t count, int64_t statistics_count);

    void sql_statistics_adder(int64_t count);

    void inc_index_count(const int64_t index_id) {
        bool print_log = false;
        {
            std::lock_guard<bthread::Mutex> lock(_mutex);
            auto it = _indexid_count.find(index_id);
            if (it == _indexid_count.end()) {
                IndexCounter counter;
                counter.count++;
                _indexid_count[index_id] = counter;
                print_log = true;
            } else {
                it->second.last_time.reset();
                it->second.count++;
            }
        }

        // 不在锁中打日志
        if (print_log) {
            DB_WARNING("sign: %lu, index_id: %ld", _sign, index_id);
        }

    }

    void dec_index_count(const int64_t index_id) {
        int count = 0;
        {
            std::lock_guard<bthread::Mutex> lock(_mutex);
            auto it = _indexid_count.find(index_id);
            if (it == _indexid_count.end()) {
                return;
            }
            it->second.count--;
            count = it->second.count;
        }

        if (count < 0) {
            DB_FATAL("inc dec mismatch. sign: %ld", _sign);
        }
    }

    // 分析qps，定时执行
    void analyze_qps(int64_t& get_qps, int64_t& scan_qps) {
        _get_qps  = _get_statistics.get_qps_value();
        _scan_qps = _scan_statistics.get_qps_value();
        _sql_qps  = _sql_statistics.get_qps_value();
        get_qps   = _get_qps;
        scan_qps  = _scan_qps;
        if (get_qps != 0 || scan_qps != 0) {
            DB_WARNING("sign: %lu, get_qps: %ld, scan_qps: %ld, sql_qps: %ld, get_real: %ld, scan_real: %ld, token_fetch_qps: %ld", 
                _sign, get_qps, scan_qps, _sql_qps, _get_real.get_qps_value(), _scan_real.get_qps_value(), _token_fetch.get_qps_value());
        }

        std::lock_guard<bthread::Mutex> lock(_mutex);
        auto it = _indexid_count.begin();
        while (it != _indexid_count.end()) {
            if (it->second.last_time.get_time() > 3600 * 1000 * 1000LL) {
                _indexid_count.erase(it++);
                if (_reject_index_id.load() == it->first) {
                    _reject_index_id = it->first;
                }
            } else {
                ++it;
            }
        }
    } 
    
    std::set<uint64_t> fill_sqlqos_info(std::map<uint64_t, SqlInfo>& sql_with_multi_index) {
        SqlInfo sql_info;
        sql_info.scan_short_term = _scan_real.get_qps_value();
        sql_info.get_short_term  = _get_real.get_qps_value();
        sql_info.get_long_term   = _get_statistics.get_qps_value();
        sql_info.scan_long_term  = _scan_statistics.get_qps_value();
        sql_info.sign = _sign;
    
        std::set<uint64_t> affect_indexids;
        // 判断是否使用多索引
        bool multi_index = false;
        {
            std::lock_guard<bthread::Mutex> lock(_mutex);
            if (_indexid_count.size() >= 2) {
                multi_index = true;
                sql_info.indexid_count = _indexid_count;
                for (const auto& it : sql_info.indexid_count) {
                    affect_indexids.insert(it.first);
                }
            }
        }

        uint64_t reject_index = _reject_index_id.load(std::memory_order_relaxed);
        if (multi_index && reject_index == 0) {
            sql_with_multi_index[_sign] = sql_info;
        }  

        if (reject_index == 0) {
            affect_indexids.clear();
        }

        return affect_indexids;
    }

    int64_t rocksdb_get_qps() { return _get_qps; }

    int64_t rocksdb_scan_qps() { return _scan_qps; }

    int64_t sql_qps() { return _sql_qps; }

    int64_t start_time() { return _start_time; }

    void set_need_reject(const uint64_t index_id) { _reject_index_id = index_id; } 

    bool need_reject(const uint64_t index_id) {
        if (index_id != 0 && index_id == _reject_index_id.load(std::memory_order_relaxed)) {
            return true;
        }

        return false; 
    }

    bool is_new_sign() {
        return butil::gettimeofday_us() - _start_time <= FLAGS_qps_statistics_minutes_ago * 60LL * 1000000LL;
    }

    void reset_committed_rate(const int64_t rate) {
        _committed_bucket.reset_rate(rate);
    }

    void set_committed_valid(const bool is_valid) {
        _committed_bucket.set_valid(is_valid);
    }

    int64_t fetch_tokens(const int64_t tokens, int64_t* token_expire_time, bool* is_committed_bucket, bool* need_statistics);

    void return_tokens(const int64_t tokens, const bool is_committed_bucket, const int64_t expire_time);

private:
    std::atomic<uint64_t> _reject_index_id = { 0 }; // 需要拒绝的index_id
    uint64_t _sign = 0;
    // 承诺令牌桶
    TokenBucket _committed_bucket;

    int64_t _start_time; // 首次访问时间

    BvarWindowInfo _get_statistics;
    BvarWindowInfo _scan_statistics;
    BvarWindowInfo _get_real;
    BvarWindowInfo _scan_real;
    BvarWindowInfo _token_fetch;
    BvarWindowInfo _sql_statistics;
    BvarWindowInfo _sql_real;

    int64_t _get_qps  = 0;
    int64_t _scan_qps = 0;
    int64_t _sql_qps  = 0;
    bthread::Mutex _mutex; // 是否有不加锁的办法 TODO
    std::map<uint64_t, IndexCounter> _indexid_count;

    DISALLOW_COPY_AND_ASSIGN(SqlQos);
};

class QosReject {
public:
    QosReject() : _count(&_adder, FLAGS_qos_reject_interval_s) {
        _last_blacklist.clear();
    }

    void adder(const int count, const int64_t expire_time) {
        int64_t time = _adder_time.load(std::memory_order_relaxed);
        if (time >= expire_time) {
            // 该窗口期已经adder
            return;
        }

        // 只有在超额令牌桶获取完时才会adder，性能可控
        if (_adder_time.compare_exchange_strong(time, expire_time)) {
            // DB_WARNING("time: %ld, expire_time: %ld", time, expire_time);
            _adder << count;
        }
    }

    bool match_reject_condition() {
        const int window_count = FLAGS_qos_reject_interval_s * 1000 / FLAGS_token_bucket_burst_window_ms;
        const int count = _count.get_value();
        bool match_reject_condition = (count * 100 / window_count > FLAGS_qos_reject_ratio);
        return match_reject_condition;
    }

    void wheather_need_reject();

private:
    bool _has_reject   = false;
    std::set<uint64_t>  _last_blacklist;
    TimeCost          _reject_begin_time; // 拒绝开始的时间，超时后自动清理拒绝标记
    bvar::Adder<int>  _adder;
    bvar::Window<bvar::Adder<int>> _count;
    std::atomic<int64_t> _adder_time = {0}; // 每个窗口期只adder一次，用于辅助判断
    DISALLOW_COPY_AND_ASSIGN(QosReject);
};

using DoubleBufQos =  butil::DoublyBufferedData<std::unordered_map<uint64_t, std::shared_ptr<SqlQos>>>;
class StoreQos {
public:
    
    ~StoreQos() {
        bthread_key_delete(_bthread_local_key);
    }

    static StoreQos* get_instance() {
        static StoreQos _instance;
        return &_instance;
    }

    void analyze_qps(int64_t& total_get_qps, int64_t& total_scan_qps) {
        DoubleBufQos::ScopedPtr ptr;
        if (_sign_sqlqos_map.Read(&ptr) != 0) {
            total_get_qps = 0;
            total_scan_qps = 0;
            return;
        }
        if (ptr->empty()) {
            DB_WARNING("sign sqlqos map is empty");
            total_get_qps = 0;
            total_scan_qps = 0;
            return;
        }

        auto iter = ptr->begin();
        while (iter != ptr->end()) {
            auto cur_iter = iter++;
            int64_t get_qps  = 0;
            int64_t scan_qps = 0;

            cur_iter->second->analyze_qps(get_qps, scan_qps);

            total_get_qps  += get_qps;
            total_scan_qps += scan_qps;
        }

        DB_WARNING("total_get_qps: %ld, total_scan_qps: %ld", total_get_qps, total_scan_qps);
    }

    // return index_id
    int64_t met_rejection_conditions(const SqlInfo& sql_info, const std::set<uint64_t>& affect_indexids) {
        int64_t token_long_term = sql_info.get_long_term + sql_info.scan_long_term / FLAGS_get_token_weight;
        int64_t token_short_term = sql_info.get_short_term + sql_info.scan_short_term / FLAGS_get_token_weight;
        int64_t index_id = 0;
        bool pre_reject = false;
        bool f = false;
        if (!affect_indexids.empty()) {
            for (auto& it : sql_info.indexid_count) {
                if (affect_indexids.count(it.first) > 0) {
                    f = true;
                    break;
                }
            }
        } 
        token_long_term = token_long_term > 0 ? token_long_term : 1;
        if (f) {
            pre_reject = token_short_term > token_long_term ? true : false;
        } else {
            pre_reject = (token_short_term / token_long_term > FLAGS_qos_reject_growth_multiple) ? true : false;
        }

        if (pre_reject) {
            int64_t min_time = INT64_MAX;
            for (auto& it : sql_info.indexid_count) {
                if (min_time > it.second.begin_time.get_time()) {
                    min_time = it.second.begin_time.get_time();
                    index_id = it.first;
                }
            }
        }

        return index_id;
    }

    // 标记需要拒绝的sql
    int mark_need_reject_sql();

    void clear_need_reject() {
        DoubleBufQos::ScopedPtr ptr;
        if (_sign_sqlqos_map.Read(&ptr) != 0) {
            return;
        }
        if (ptr->empty()) {
            DB_WARNING("sign sqlqos map is empty");
            return;
        }

        auto iter = ptr->begin();
        while (iter != ptr->end()) {
            auto cur_iter = iter++;
            cur_iter->second->set_need_reject(0);
        }
    }

    void globle_extended_bucket_return_tokens(const int64_t tokens, const int64_t expire_time) {
        _globle_extended_bucket.return_tokens(tokens, expire_time);
    }

    int64_t globle_extended_bucket_consume(int64_t expect_tokens, int64_t* expire_time, bool* need_statistics) {
        int64_t count =  _globle_extended_bucket.consume(expect_tokens, expire_time, need_statistics);
        if (count <= 0) {
            _qos_reject.adder(1, *expire_time);
        }

        return count;
    }

    std::shared_ptr<SqlQos> get_sql_shared_ptr(uint64_t sign) {
        std::shared_ptr<SqlQos> qos;
        {
            DoubleBufQos::ScopedPtr ptr;
            if (_sign_sqlqos_map.Read(&ptr) != 0) {
                return nullptr; 
            }
            auto iter = ptr->find(sign);
            if (iter != ptr->end()) {
                return iter->second;
            }
        }
        if (qos == nullptr) {
            qos = std::make_shared<SqlQos>(sign);
            auto call = [sign, &qos](std::unordered_map<uint64_t, std::shared_ptr<SqlQos>>& map) {
                if (map.count(sign) == 1) {
                    qos = map[sign];
                    return 0;
                }
                map[sign] = qos;
                return 1;
            };
            _sign_sqlqos_map.Modify(call);
            return qos;
        }
        return qos;
    }

    void create_bthread_local(QosType type, const uint64_t sign, const int64_t index_id) {
        QosBthreadLocal* local = new(std::nothrow) QosBthreadLocal(type, sign, index_id);
        if (local == nullptr) {
            return;
        }
        bthread_getspecific(_bthread_local_key);
        int ret = bthread_setspecific(_bthread_local_key, local); 
        if (ret < 0) {
            delete local;
            return;
        }
    }

    void destroy_bthread_local() {
        auto local = get_bthread_local();
        if (local != nullptr) {
            delete local;
            bthread_setspecific(_bthread_local_key, nullptr);
        }
    }

    QosBthreadLocal* get_bthread_local() {
        void* data = bthread_getspecific(_bthread_local_key);
        if (data == nullptr) {
            return nullptr;
        }

        QosBthreadLocal* local = static_cast<QosBthreadLocal*>(data);
        if (local->bthread_id() != bthread_self()) {
            DB_FATAL("diff bthread");
            return nullptr;
        }
        return local;
    }

    bool is_new_sign() {
        if (_start_time.get_time() <= FLAGS_qps_statistics_minutes_ago * 2 * 60LL * 1000000LL) {
//            DB_NOTICE("StoreQos start time(s): %ld", _start_time.get_time() / 1000000LL);
            return false;
        }
        void* data = bthread_getspecific(_bthread_local_key);
        if (data == nullptr) {
            return false;
        }

        QosBthreadLocal* local = static_cast<QosBthreadLocal*>(data);
        if (local->bthread_id() != bthread_self()) {
            DB_FATAL("diff bthread");
            return false;
        }

        return local->is_new_sign();
    }

    bool need_reject() {
        if (FLAGS_qos_need_reject == 0) {
            return false;
        }
        void* data = bthread_getspecific(_bthread_local_key);
        if (data == nullptr) {
            return false;
        }

        QosBthreadLocal* local = static_cast<QosBthreadLocal*>(data);
        if (local->bthread_id() != bthread_self()) {
            DB_FATAL("diff bthread");
            return false;
        }

        return local->need_reject();
    }

    void update_statistics() {
        void* data = bthread_getspecific(_bthread_local_key);
        if (data == nullptr) {
            return;
        }

        QosBthreadLocal* local = static_cast<QosBthreadLocal*>(data);
        if (local->bthread_id() != bthread_self()) {
            DB_FATAL("diff bthread");
            return;
        }

        local->update_statistics();
    }

    void token_bucket_modify();

    void token_bucket_thread() {
        while (!_shutdown) {
            token_bucket_modify();
            bthread_usleep_fast_shutdown(FLAGS_token_bucket_adjust_interval_s * 1000 * 1000LL, _shutdown);
        }
    }

    //
    void qos_reject_thread() {
        while (!_shutdown) {
            // 判断是否需要拒绝
            _qos_reject.wheather_need_reject();
            bthread_usleep_fast_shutdown(FLAGS_qos_reject_interval_s * 1000 * 1000LL, _shutdown);
        }
    }

    int init() {
        int ret = bthread_key_create(&_bthread_local_key, nullptr);
        if (ret < 0) {
            return -1;
        }

        _token_bucket_bth.run([this]() {token_bucket_thread();});
        _qos_reject_bth.run([this]() {qos_reject_thread();});
        return 0;
    }

    void close() {
        _shutdown = true;
        _token_bucket_bth.join();
        _qos_reject_bth.join();
    }

    bool match_reject_condition() {
        return _qos_reject.match_reject_condition();
    }
    
private:

    StoreQos() 
        : _globle_extended_bucket(true, FLAGS_max_tokens_per_second),
        _bthread_local_key(INVALID_BTHREAD_KEY) {
    }

    TokenBucket _globle_extended_bucket;
    DoubleBufQos _sign_sqlqos_map;   
    QosReject  _qos_reject;
    TimeCost _start_time;      // 实例启动时间
    bool _shutdown = false;
    Bthread _token_bucket_bth; // 令牌桶定时调整线程
    Bthread _qos_reject_bth; // qos拒绝策略调整线程
    bthread_key_t _bthread_local_key;
};

} //namespace baikaldb
