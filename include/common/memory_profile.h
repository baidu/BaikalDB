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

#include <atomic>

namespace baikaldb {

class MemoryGCHandler {
public:
    virtual ~MemoryGCHandler() {}
    static MemoryGCHandler* get_instance() {
        static MemoryGCHandler _instance;
        return &_instance;
    }

    void memory_gc_thread();

    int init() {
#ifdef BAIKAL_TCMALLOC
        _memory_gc_bth.run([this]() {memory_gc_thread();});
#endif
        return 0;
    }
    void close() {
        _shutdown = true;
        _memory_gc_bth.join();
    }
private:
    MemoryGCHandler() {}
    bool _shutdown = false;
    Bthread _memory_gc_bth;

    DISALLOW_COPY_AND_ASSIGN(MemoryGCHandler);
};

class MemTracker {
public:
    explicit MemTracker(uint64_t log_id, int64_t bytes_limit, MemTracker* parent = nullptr);
    ~MemTracker();

    bool check_bytes_limit() {
        return _bytes_limit > 0 && _bytes_consumed > _bytes_limit;
    }
    bool any_limit_exceeded() {
        if (limit_exceeded() || (_parent !=nullptr && _parent->any_limit_exceeded())) {
            _limit_exceeded = true;
            return true;
        }
        return false;
    }

    bool limit_exceeded() const { return _bytes_limit >= 0 && bytes_consumed() > _bytes_limit; }

    void consume(int64_t bytes) {
        _last_active_time = butil::gettimeofday_us();
        if (bytes <= 0) {
            return ;
        }
        _bytes_consumed.fetch_add(bytes, std::memory_order_relaxed);
        if (_parent != nullptr) {
            _parent->_bytes_consumed.fetch_add(bytes, std::memory_order_relaxed);
        }
    }

    void release(int64_t bytes) {
        _last_active_time = butil::gettimeofday_us();
        if (bytes <= 0) {
            return ;
        }
        _bytes_consumed.fetch_sub(bytes, std::memory_order_relaxed);
        if (_parent != nullptr) {
            _parent->_bytes_consumed.fetch_sub(bytes, std::memory_order_relaxed);
        }
    }
    uint64_t log_id() const {
        return _log_id;
    }
    int64_t bytes_limit() const {
        return _bytes_limit;
    }
    int64_t last_active_time() const {
        return _last_active_time;
    }
    int64_t bytes_consumed() const {
        return _bytes_consumed.load(std::memory_order_relaxed);
    }
    bool has_limit_exceeded() const {
        return _limit_exceeded;
    }
    void set_limit_exceeded() {
        _limit_exceeded = true;
    }
    MemTracker* get_parent() {
        return _parent;
    }

private:

    uint64_t _log_id;
    int64_t _bytes_limit;
    int64_t  _last_active_time;
    std::atomic<int64_t> _bytes_consumed;
    MemTracker* _parent = nullptr;
    bool _limit_exceeded;
};

typedef std::shared_ptr<MemTracker> SmartMemTracker;

class MemTrackerPool {
public:
    static MemTrackerPool* get_instance() {
        static MemTrackerPool _instance;
        return &_instance;
    }

    SmartMemTracker get_mem_tracker(uint64_t log_id);

    void tracker_gc_thread();

    int init();

    void close() {
        _shutdown = true;
        _tracker_gc_bth.join();
    }

private:
    MemTrackerPool() {}
    int64_t _query_bytes_limit;
    bool _shutdown = false;
    SmartMemTracker _root_tracker = nullptr;
    Bthread _tracker_gc_bth;
    ThreadSafeMap<uint64_t, SmartMemTracker> _mem_tracker_pool;

    DISALLOW_COPY_AND_ASSIGN(MemTrackerPool);
};



} //namespace baikaldb
