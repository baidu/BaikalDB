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

#include <execinfo.h>
#include <type_traits>
#include <set>
#include <unordered_set>
#include <unordered_map>
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
#include "naming.pb.h"
#else
#include <bthread/bthread.h>
#include <butil/time.h>
#include <butil/third_party/murmurhash3/murmurhash3.h>
#include <butil/containers/doubly_buffered_data.h>
#include <butil/endpoint.h>
#include <butil/base64.h>
#endif
#include <bthread/execution_queue.h>
#include <gflags/gflags.h>
#include "log.h"
#include "proto/common.pb.h"

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

class TimeCost {
public:
    TimeCost() {
        _start = butil::gettimeofday_us();
    }

    ~TimeCost() {}

    void reset() {
        _start = butil::gettimeofday_us();
    }

    int64_t get_time() {
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
        for (int i = 0; i < MAP_COUNT; i++) {
            bthread_mutex_init(&_mutex[i], NULL);
        }
    }
    ~ThreadSafeMap() {
        for (int i = 0; i < MAP_COUNT; i++) {
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
        for (int i = 0; i < MAP_COUNT; i++) {
            {
                BAIDU_SCOPED_LOCK(_mutex[i]);
                size += _map[i].size();
            }
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

struct BvarMap {
    struct SumCount {
        SumCount() {}
        SumCount(int64_t sum, int64_t count) : sum(sum), count(count) {}
        SumCount& operator+=(const SumCount& other) {
            sum += other.sum;
            count += other.count;
            return *this;
        }
        SumCount& operator-=(const SumCount& other) {
            sum -= other.sum;
            count -= other.count;
            return *this;
        }
        int64_t sum = 0;
        int64_t count = 0;
    };
public:
    BvarMap() {}
    BvarMap(const std::string& key, int64_t cost) {
        internal_map[key] = SumCount(cost, 1);
    }
    BvarMap& operator+=(const BvarMap& other) {
        for (auto& pair : other.internal_map) {
            internal_map[pair.first] += pair.second;
        }
        return *this;
    }
    BvarMap& operator-=(const BvarMap& other) {
        for (auto& pair : other.internal_map) {
            internal_map[pair.first] -= pair.second;
        }
        return *this;
    }
public:
    std::map<std::string, SumCount> internal_map;
};

inline std::ostream& operator<<(std::ostream& os, const BvarMap& bm) {
    for (auto& pair : bm.internal_map) {
        os << pair.first << " : " << pair.second.sum << "," << pair.second.count << std::endl;
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
extern int end_key_compare(const std::string& key1, const std::string& key2);

extern int primitive_to_proto_type(pb::PrimitiveType type);
extern int get_physical_room(const std::string& ip_and_port_str, std::string& host);
extern int get_instance_from_bns(int* ret,
                          const std::string& bns_name, 
                          std::vector<std::string>& instances,
                          bool need_alive = true); 
extern bool is_digits(const std::string& str);
extern std::string url_decode(const std::string& str);
extern std::string url_encode(const std::string& str);

inline int end_key_compare(const std::string& key1, const std::string& key2) {
    if (key1 == key2) {
        return 0;
    }
    if (key1.empty()) {
        return 1;
    }
    if (key2.empty()) {
        return -1;
    }
    return key1.compare(key2);
}

inline uint64_t make_sign(const std::string& key) {
    uint64_t out[2];
    butil::MurmurHash3_x64_128(key.c_str(), key.size(), 1234, out);
    return out[0];
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

