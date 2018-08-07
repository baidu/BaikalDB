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

#include <execinfo.h>
#include <type_traits>
#include <unordered_map>
#include <bthread/butex.h>
#ifdef BAIDU_INTERNAL
#include <bthread.h>
#include <base/time.h>
#include <base/third_party/murmurhash3/murmurhash3.h>
#include <base/endpoint.h>
#include <base/base64.h>
#else
#include <bthread/bthread.h>
#include <butil/time.h>
#include <butil/third_party/murmurhash3/murmurhash3.h>
#include <butil/endpoint.h>
#include <butil/base64.h>
#endif
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
    RET_NO_MEMORY        = 7
};

enum SerializeStatus {
    STMPS_SUCCESS,
    STMPS_FAIL,
    STMPS_NEED_RESIZE
};

typedef unsigned char           uint8;
typedef signed char             int8;
typedef short                   int16;
typedef unsigned char           uchar;
typedef unsigned long long int  ulonglong;
typedef unsigned int            uint32;

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
        _count--;
        bthread_cond_signal(&_cond);
        bthread_mutex_unlock(&_mutex);
    }

    void decrease_broadcast() {
        bthread_mutex_lock(&_mutex);
        _count--;
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
        while (_count > cond) {
            ret = bthread_cond_wait(&_cond, &_mutex);
            if (ret != 0) {
                DB_WARNING("wait timeout, ret:%d", ret);
                break;
            }
        }
        ++_count;
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
        while (_count > cond) {
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
    void traverse(const std::function<void(VALUE& value)>& call) {
        for (uint32_t i = 0; i < MAP_COUNT; i++) {
            BAIDU_SCOPED_LOCK(_mutex[i]);
            for (auto& pair : _map[i]) {
                call(pair.second);
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
//extern int get_physical_room(const std::string& ip_and_port_str, std::string& host);

extern std::string timestamp_to_str(time_t timestamp);

extern time_t str_to_timestamp(const char* str_time);

// encode DATETIME to string format
// ref: https://dev.mysql.com/doc/internals/en/date-and-time-data-type-representation.html
extern std::string datetime_to_str(uint64_t datetime);

extern uint64_t str_to_datetime(const char* str_time);

extern time_t datetime_to_timestamp(uint64_t datetime);
extern uint64_t timestamp_to_datetime(time_t timestamp);

// inline functions
// DATE: 17 bits year*13+month  (year 0-9999, month 1-12)
//        5 bits day            (1-31)
inline uint32_t datetime_to_date(uint64_t datetime) {
    return ((datetime >> 41) & 0x3FFFFF);
}
inline uint64_t date_to_datetime(uint32_t date) {
    return (uint64_t)date << 41;
}
inline std::string date_to_str(uint32_t date) {
    int year_month = ((date >> 5) & 0x1FFFF);
    int year = year_month / 13;
    int month = year_month % 13;
    int day = (date & 0x1F);
    char buf[30] = {0};
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day);
    return std::string(buf);
}

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
} // namespace baikaldb

