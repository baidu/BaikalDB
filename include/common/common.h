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

#include <functional>
#include <execinfo.h>
#include <type_traits>
#include <fstream>
#include <cmath>
#include <set>
#include <unordered_set>
#include <unordered_map>
#include <functional>
#include <bthread/butex.h>
#include <bvar/bvar.h>
#ifdef BAIDU_INTERNAL
#include <bthread.h>
#include <base/time.h>
#include <base/third_party/murmurhash3/murmurhash3.h>
#include <base/containers/doubly_buffered_data.h>
#include <base/endpoint.h>
#include <base/base64.h>
#include <webfoot_naming.h>
#include <base/fast_rand.h>
#include "naming.pb.h"
#else
#include <bthread/bthread.h>
#include <butil/time.h>
#include <butil/third_party/murmurhash3/murmurhash3.h>
#include <butil/containers/doubly_buffered_data.h>
#include <butil/endpoint.h>
#include <butil/base64.h>
#include <butil/fast_rand.h>
#endif
#include <bthread/execution_queue.h>
#include <gflags/gflags.h>
#include "log.h"
#include "proto/common.pb.h"
#include "proto/meta.interface.pb.h"

#ifdef BAIDU_INTERNAL
namespace baidu {
namespace rpc {
}
}
namespace raft {
}
namespace butil = base;
namespace brpc = baidu::rpc;
namespace braft = raft;
#endif

namespace baikaldb {
enum RETURN_VALUE {
    RET_SUCCESS          = 0,
    RET_ERROR            = 1,   // Common error.
    RET_WAIT_FOR_EVENT   = 2,   // Interrupt by event.
    RET_SHUTDOWN         = 3,   // Internal shutdown.
    RET_AUTH_FAILED      = 4,   // Auth login failed.
    RET_COMMAND_SHUTDOWN = 5,   // Shutdown by client command.
    RET_CMD_UNSUPPORT    = 6,   // un-supported command
    RET_NO_MEMORY        = 7,
    RET_CMD_DONE         = 8
};

enum SerializeStatus {
    STMPS_SUCCESS,
    STMPS_FAIL,
    STMPS_NEED_RESIZE
};

enum SstBackupType {
    UNKNOWN_BACKUP,
    META_BACKUP,
    DATA_BACKUP
};

enum MysqlCommand : uint8_t {
    // cmd name             cmd no    Associated client function
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
enum ExplainType {
    EXPLAIN_NULL            = 0,
    ANALYZE_STATISTICS      = 1,
    SHOW_HISTOGRAM          = 2,
    SHOW_CMSKETCH           = 3,
    SHOW_PLAN               = 4,
    SHOW_TRACE              = 5,
    SHOW_TRACE2             = 6,
};

inline bool explain_is_trace(ExplainType& type) {
    return type == SHOW_TRACE || type == SHOW_TRACE2;
}

class TimeCost {
public:
    TimeCost() {
        _start = butil::gettimeofday_us();
    }

    ~TimeCost() {}

    void reset() {
        _start = butil::gettimeofday_us();
    }

    int64_t get_time() const {
        return butil::gettimeofday_us() - _start;
    }

private:
    int64_t _start;
};

// wrapper bthread::execution_queue functions for c++ style
class ExecutionQueue {
public:
    ExecutionQueue() {
        bthread::execution_queue_start(&_queue_id, nullptr, run_function, nullptr);
    }
    void run(const std::function<void()>& call) {
        bthread::execution_queue_execute(_queue_id, call);
    }
    void stop() {
        execution_queue_stop(_queue_id);
    }
    void join() {
        execution_queue_join(_queue_id);
    }
private:
    static int run_function(void* meta, bthread::TaskIterator<std::function<void()>>& iter) {
        if (iter.is_queue_stopped()) {
            return 0;
        }
        for (; iter; ++iter) {
            (*iter)();
        }
        return 0;
    }
    bthread::ExecutionQueueId<std::function<void()>> _queue_id = {0};
};
// return when timeout or shutdown
inline void bthread_usleep_fast_shutdown(int64_t interval_us, const bool& shutdown) {
    if (interval_us < 10000) {
        bthread_usleep(interval_us);
        return;
    }
    int64_t sleep_time_count = interval_us / 10000; //10ms为单位
    int time = 0;
    while (time < sleep_time_count) {
        if (shutdown) {
            return;
        }
        bthread_usleep(10000);
        ++time;
    }
}

class BthreadCond {
public:
    BthreadCond(int count = 0) {
        bthread_cond_init(&_cond, NULL);
        bthread_mutex_init(&_mutex, NULL);
        _count = count;
    }
    ~BthreadCond() {
        bthread_mutex_destroy(&_mutex);
        bthread_cond_destroy(&_cond);
    }

    int count() {
        return _count;
    }

    void increase() {
        bthread_mutex_lock(&_mutex);
        ++_count;
        bthread_mutex_unlock(&_mutex);
    }

    void decrease_signal() {
        bthread_mutex_lock(&_mutex);
        --_count;
        bthread_cond_signal(&_cond);
        bthread_mutex_unlock(&_mutex);
    }

    void decrease_broadcast() {
        bthread_mutex_lock(&_mutex);
        --_count;
        bthread_cond_broadcast(&_cond);
        bthread_mutex_unlock(&_mutex);
    }
    
    int wait(int cond = 0) {
        int ret = 0;
        bthread_mutex_lock(&_mutex);
        while (_count > cond) {
            ret = bthread_cond_wait(&_cond, &_mutex);
            if (ret != 0) {
                DB_WARNING("wait timeout, ret:%d", ret);
                break;
            }
        }
        bthread_mutex_unlock(&_mutex);
        return ret;
    }
    int increase_wait(int cond = 0) {
        int ret = 0;
        bthread_mutex_lock(&_mutex);
        while (_count + 1 > cond) {
            ret = bthread_cond_wait(&_cond, &_mutex);
            if (ret != 0) {
                DB_WARNING("wait timeout, ret:%d", ret);
                break;
            }
        }
        ++_count; // 不能放在while前面
        bthread_mutex_unlock(&_mutex);
        return ret;
    }
    int timed_wait(int64_t timeout_us, int cond = 0) {
        int ret = 0;
        timespec tm = butil::microseconds_from_now(timeout_us);
        bthread_mutex_lock(&_mutex);
        while (_count > cond) {
            ret = bthread_cond_timedwait(&_cond, &_mutex, &tm);
            if (ret != 0) {
                DB_WARNING("wait timeout, ret:%d", ret);
                break;
            }
        }
        bthread_mutex_unlock(&_mutex);
        return ret;
    }

    int increase_timed_wait(int64_t timeout_us, int cond = 0) {
        int ret = 0;
        timespec tm = butil::microseconds_from_now(timeout_us);
        bthread_mutex_lock(&_mutex);
        while (_count + 1 > cond) {
            ret = bthread_cond_timedwait(&_cond, &_mutex, &tm);
            if (ret != 0) {
                DB_WARNING("wait timeout, ret:%d", ret);
                break; 
            }
        }
        ++_count;
        bthread_mutex_unlock(&_mutex);
        return ret;
    }
    
private:
    int _count;
    bthread_cond_t _cond;
    bthread_mutex_t _mutex;
};
// wrapper bthread functions for c++ style
class Bthread {
public:
    Bthread() {
    }
    explicit Bthread(const bthread_attr_t* attr) : _attr(attr) {
    }

    void run(const std::function<void()>& call) {
        std::function<void()>* _call = new std::function<void()>;
        *_call = call;
        bthread_start_background(&_tid, _attr, 
                [](void*p) -> void* { 
                    auto call = static_cast<std::function<void()>*>(p);
                    (*call)();
                    delete call;
                    return NULL;
                }, _call);
    }
    void run_urgent(const std::function<void()>& call) {
        std::function<void()>* _call = new std::function<void()>;
        *_call = call;
        bthread_start_urgent(&_tid, _attr, 
                [](void*p) -> void* { 
                    auto call = static_cast<std::function<void()>*>(p);
                    (*call)();
                    delete call;
                    return NULL;
                }, _call);
    }
    void join() {
        bthread_join(_tid, NULL);
    }
    bthread_t id() {
        return _tid;
    }

private:
    bthread_t _tid;
    const bthread_attr_t* _attr = NULL;
};
class ConcurrencyBthread {
public:
    explicit ConcurrencyBthread(int concurrency) : 
        _concurrency(concurrency) {
    }
    ConcurrencyBthread(int concurrency, const bthread_attr_t* attr) : 
        _concurrency(concurrency),
        _attr(attr) {
    }
    void run(const std::function<void()>& call) {
        _cond.increase_wait(_concurrency);
        Bthread bth(_attr);
        bth.run([this, call]() {
            call();
            _cond.decrease_signal();
        });
    }
    void join() {
        _cond.wait();
    }

private:
    int _concurrency = 10;
    BthreadCond _cond;
    const bthread_attr_t* _attr = NULL;
};
// RAII
class ScopeGuard {
public:
    explicit ScopeGuard(std::function<void()> exit_func) : 
        _exit_func(exit_func) {}
    ~ScopeGuard() {
        if (!_is_release) {
            _exit_func();
        }
    }
    void release() {
        _is_release = true;
    }
private:
    std::function<void()> _exit_func;
    bool _is_release = false;
    DISALLOW_COPY_AND_ASSIGN(ScopeGuard);
};
#define SCOPEGUARD_LINENAME_CAT(name, line) name##line
#define SCOPEGUARD_LINENAME(name, line) SCOPEGUARD_LINENAME_CAT(name, line)
#define ON_SCOPE_EXIT(callback) ScopeGuard SCOPEGUARD_LINENAME(scope_guard, __LINE__)(callback)

template <typename KEY, typename VALUE, uint32_t MAP_COUNT = 23>
class ThreadSafeMap {
public:
    ThreadSafeMap() {
        for (uint32_t i = 0; i < MAP_COUNT; i++) {
            bthread_mutex_init(&_mutex[i], NULL);
        }
    }
    ~ThreadSafeMap() {
        for (uint32_t i = 0; i < MAP_COUNT; i++) {
            bthread_mutex_destroy(&_mutex[i]);
        }
    }
    uint32_t count(const KEY& key) {
        uint32_t idx = map_idx(key);
        BAIDU_SCOPED_LOCK(_mutex[idx]);
        return _map[idx].count(key);
    }
    uint32_t size() {
        uint32_t size = 0;
        for (uint32_t i = 0; i < MAP_COUNT; i++) {
            BAIDU_SCOPED_LOCK(_mutex[i]);
            size += _map[i].size();
        }
        return size;
    }
    void set(const KEY& key, const VALUE& value) {
        uint32_t idx = map_idx(key);
        BAIDU_SCOPED_LOCK(_mutex[idx]);
        _map[idx][key] = value;
    }
    const VALUE get(const KEY& key) {
        uint32_t idx = map_idx(key);
        BAIDU_SCOPED_LOCK(_mutex[idx]);
        if (_map[idx].count(key) == 0) {
            static VALUE tmp;
            return tmp;
        }
        return _map[idx][key];
    }
    VALUE& operator[](const KEY& key) {
        uint32_t idx = map_idx(key);
        BAIDU_SCOPED_LOCK(_mutex[idx]);
        return _map[idx][key];
    }
    void erase(const KEY& key) {
        uint32_t idx = map_idx(key);
        BAIDU_SCOPED_LOCK(_mutex[idx]);
        _map[idx].erase(key);
    }
    // 会加锁，轻量级操作采用traverse否则用copy
    void traverse(const std::function<void(VALUE& value)>& call) {
        for (uint32_t i = 0; i < MAP_COUNT; i++) {
            BAIDU_SCOPED_LOCK(_mutex[i]);
            for (auto& pair : _map[i]) {
                call(pair.second);
            }
        }
    }
    void traverse_with_key_value(const std::function<void(const KEY key, VALUE& value)>& call) {
        for (uint32_t i = 0; i < MAP_COUNT; i++) {
            BAIDU_SCOPED_LOCK(_mutex[i]);
            for (auto& pair : _map[i]) {
                call(pair.first, pair.second);
            }
        }
    }
    void traverse_copy(const std::function<void(VALUE& value)>& call) {
        for (uint32_t i = 0; i < MAP_COUNT; i++) {
            std::unordered_map<KEY, VALUE> tmp;
            {
                BAIDU_SCOPED_LOCK(_mutex[i]);
                tmp = _map[i];
            }
            for (auto& pair : tmp) {
                call(pair.second);
            }
        }
    }
    void clear() {
       for (uint32_t i = 0; i < MAP_COUNT; i++) {
            BAIDU_SCOPED_LOCK(_mutex[i]);
            _map[i].clear();
        } 
    }

    template<typename... Args>
    void init_if_not_exist_else_update(const KEY& key, 
        const std::function<void(VALUE& value)>& call, Args&&... args) {
        uint32_t idx = map_idx(key);
        BAIDU_SCOPED_LOCK(_mutex[idx]);
        auto iter = _map[idx].find(key);
        if (iter == _map[idx].end()) {
            _map[idx].insert(std::make_pair(key, VALUE(std::forward<Args>(args)...)));
        } else {
            //字段存在，才执行回调
            call(iter->second);
        }
    }

    void update(const KEY& key, const std::function<void(VALUE& value)>& call) {
        uint32_t idx = map_idx(key);
        BAIDU_SCOPED_LOCK(_mutex[idx]);
        auto iter = _map[idx].find(key);
        if (iter != _map[idx].end()) {
            call(iter->second);
        }
    }

    //返回值：true表示执行了全部遍历，false表示遍历中途退出
    bool traverse_with_early_return(const std::function<bool(VALUE& value)>& call) {
        for (uint32_t i = 0; i < MAP_COUNT; i++) {
            BAIDU_SCOPED_LOCK(_mutex[i]);
            for (auto& pair : _map[i]) {
                if (!call(pair.second)) {
                    return false;
                }
            }
        }
        return true;
    }

private:
    uint32_t map_idx(KEY key) {
        if (std::is_integral<KEY>::value) {
            return (uint32_t)key % MAP_COUNT;
        }
        // un-support other
        return 0;
    }
private:
    std::unordered_map<KEY, VALUE> _map[MAP_COUNT];
    bthread_mutex_t _mutex[MAP_COUNT];
    DISALLOW_COPY_AND_ASSIGN(ThreadSafeMap);
};

template <typename T>
class DoubleBuffer {
public:
    T* read() {
        return _data + _index;
    }
    T* read_background() {
        return _data + !_index;
    }
    void swap() {
        _index = ! _index;
    }
private:
    T _data[2];
    int _index = 0;
};

DECLARE_int64(incremental_info_gc_time);
template <typename T>
class IncrementalUpdate {
public:
    IncrementalUpdate() {
        bthread_mutex_init(&_mutex, NULL);
    }

    ~IncrementalUpdate() {
        bthread_mutex_destroy(&_mutex);
    }

    void put_incremental_info(const int64_t apply_index, T& infos) {
        BAIDU_SCOPED_LOCK(_mutex);
        auto background  = _buf.read_background();
        auto frontground = _buf.read();
        (*background)[apply_index] = infos;
        if (FLAGS_incremental_info_gc_time < _gc_time_cost.get_time()) {
            frontground->clear();
            _buf.swap();
            _gc_time_cost.reset();
        }  
    }

    // 返回值 true:需要全量更新外部处理 false:增量更新，通过update_incremental处理增量
    bool check_and_update_incremental(std::function<void(const T&)> update_incremental, int64_t& last_updated_index, const int64_t applied_index) {
        BAIDU_SCOPED_LOCK(_mutex);
        auto background  = _buf.read_background();
        auto frontground = _buf.read();
        if (frontground->size() == 0 && background->size() == 0) {
            if (last_updated_index < applied_index) {
                return true;
            }
            return false;
        } else if (frontground->size() == 0 && background->size() > 0) {
            if (last_updated_index < background->begin()->first) {              
                return true;
            } else {
                auto iter = background->upper_bound(last_updated_index);
                while (iter != background->end()) {
                    if (iter->first > applied_index) {
                        break;
                    }
                    update_incremental(iter->second);
                    last_updated_index = iter->first;
                    ++iter;
                }
                return false;
            }
        } else if (frontground->size() > 0) {
            if (last_updated_index < frontground->begin()->first) {
                return true;
            } else {
                auto iter = frontground->upper_bound(last_updated_index);
                while (iter != frontground->end()) {
                    if (iter->first > applied_index) {
                        break;
                    }
                    update_incremental(iter->second);
                    last_updated_index = iter->first;
                    ++iter;
                }
                iter = background->upper_bound(last_updated_index);
                while (iter != background->end()) {
                    if (iter->first > applied_index) {
                        break;
                    }
                    update_incremental(iter->second);
                    last_updated_index = iter->first;
                    ++iter;
                }
                return false;         
            }
        }
        return false;
    }

    void clear() {
        auto background  = _buf.read_background();
        auto frontground = _buf.read();
        background->clear();
        frontground->clear();
    }

private:
    DoubleBuffer<std::map<int64_t, T>> _buf;
    bthread_mutex_t                    _mutex;
    TimeCost                    _gc_time_cost;
};

struct BvarMap {
    struct SumCount {
        SumCount() {}
        SumCount(int64_t table_id, int64_t sum, int64_t count, int64_t affected_rows, int64_t scan_rows, int64_t filter_rows) 
            : table_id(table_id), sum(sum), count(count), affected_rows(affected_rows), scan_rows(scan_rows), filter_rows(filter_rows) {}
        SumCount& operator+=(const SumCount& other) {
            if (other.table_id > 0) {
                table_id = other.table_id;
            }
            sum += other.sum;
            count += other.count;
            affected_rows += other.affected_rows;
            scan_rows += other.scan_rows;
            filter_rows += other.filter_rows;
            return *this;
        }
        SumCount& operator-=(const SumCount& other) {
            sum -= other.sum;
            count -= other.count;
            affected_rows -= other.affected_rows;
            scan_rows -= other.scan_rows;
            filter_rows -= other.filter_rows;
            return *this;
        }
        int64_t table_id = 0;
        int64_t sum = 0;
        int64_t count = 0;
        int64_t affected_rows = 0;
        int64_t scan_rows = 0;
        int64_t filter_rows = 0;
    };
public:
    BvarMap() {}
    BvarMap(const std::string& key, int64_t index_id, int64_t table_id, int64_t cost, int64_t affected_rows, int64_t scan_rows, int64_t filter_rows) {
        internal_map[key][index_id] = SumCount(table_id, cost, 1, affected_rows, scan_rows, filter_rows);
    }
    BvarMap& operator+=(const BvarMap& other) {
        for (auto& pair : other.internal_map) {
            for (auto& pair2 : pair.second) {
                internal_map[pair.first][pair2.first] += pair2.second;
            }
        }
        return *this;
    }
    BvarMap& operator-=(const BvarMap& other) {
        for (auto& pair : other.internal_map) {
            for (auto& pair2 : pair.second) {
                internal_map[pair.first][pair2.first] -= pair2.second;
            }
        }
        return *this;
    }
public:
    std::map<std::string, std::map<int64_t, SumCount>> internal_map;
};

inline std::ostream& operator<<(std::ostream& os, const BvarMap& bm) {
    for (auto& pair : bm.internal_map) {
        for (auto& pair2 : pair.second) {
            os << pair.first << " : " << pair2.first << " : " << pair2.second.sum << "," << pair2.second.count << std::endl;
        }
    }
    return os;
}

extern int64_t timestamp_diff(timeval _start, timeval _end);
extern std::string pb2json(const google::protobuf::Message& message);
extern std::string json2pb(const std::string& json, google::protobuf::Message* message);
extern std::string to_string(int32_t number);
extern std::string to_string(uint32_t number);
extern std::string to_string(int64_t number);
extern std::string to_string(uint64_t number);
extern SerializeStatus to_string(int32_t number, char *buf, size_t size, size_t& len);
extern SerializeStatus to_string(uint32_t number, char *buf, size_t size, size_t& len);
extern SerializeStatus to_string(int64_t number, char *buf, size_t size, size_t& len);
extern SerializeStatus to_string(uint64_t number, char *buf, size_t size, size_t& len);
//extern std::string str_utf8_to_gbk(const char* input);
//extern std::string str_gbk_to_utf8(const char* input);
extern std::string remove_quote(const char* str, char quote);
extern std::string str_to_hex(const std::string& str);
void stripslashes(std::string& str);
extern void update_schema_conf_common(const std::string& table_name, const pb::SchemaConf& schema_conf, pb::SchemaConf* p_conf);
extern void update_op_version(pb::SchemaConf* p_conf, const std::string& desc);
extern int primitive_to_proto_type(pb::PrimitiveType type);
extern int get_physical_room(const std::string& ip_and_port_str, std::string& host);
extern int get_instance_from_bns(int* ret,
                          const std::string& bns_name, 
                          std::vector<std::string>& instances,
                          bool need_alive = true); 
extern bool is_digits(const std::string& str);
extern std::string url_decode(const std::string& str);
extern std::string url_encode(const std::string& str);
extern std::vector<std::string> string_split(const std::string &s, char delim);
extern std::string string_trim(std::string& str);
extern const std::string& rand_peer(pb::RegionInfo& info);
extern void other_peer_to_leader(pb::RegionInfo& info);

inline uint64_t make_sign(const std::string& key) {
    uint64_t out[2];
    butil::MurmurHash3_x64_128(key.c_str(), key.size(), 1234, out);
    return out[0];
}

inline bool float_equal(double value, double compare, double epsilon = 1e-9) {
    return std::fabs(value - compare) < epsilon;
}

//set double buffer
template<typename T>
using DoubleBufferSet = butil::DoublyBufferedData<std::unordered_set<T>>;
using DoubleBufferStringSet = DoubleBufferSet<std::string>;

inline int set_insert(std::unordered_set<std::string>& set, const std::string& item) {
    set.insert(item);
    return 1;
}

//map double buffer
template<typename Key, typename Val>
using DoubleBufferMap = butil::DoublyBufferedData<std::unordered_map<Key, Val>>;

} // namespace baikaldb

