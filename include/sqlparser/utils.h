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
#include <cstdint>
#ifdef BAIDU_INTERNAL
#include <base/arena.h>
#else
#include <butil/arena.h>
#endif
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

namespace parser {
// simple vector allocate by arena
template<typename T>
class Vector {
public:
    Vector() {}
    ~Vector() {}
    int32_t size() const {
        return _end - _begin;
    }
    int32_t capacity() const {
        return _mem_end - _begin;
    }
    int32_t remain() const {
        return _mem_end - _end;
    }
    void reserve(int32_t size, butil::Arena& arena) {
        if (size <= capacity()) {
            return;
        }
        int32_t old_size = Vector<T>::size();
        T* new_mem = (T*)arena.allocate(size * sizeof(T));
        if (old_size > 0) {
            memcpy(new_mem, _begin, sizeof(T) * (_end - _begin));
        }
        // arena need not delete old vector
        _begin = new_mem;
        _end = _begin + old_size;
        _mem_end = _begin + size;
    }
    void push_back(const T& value, butil::Arena& arena) {
        if (remain() > 0) {
            *_end = value;
            ++_end;
        } else {
            int32_t new_capacity = (capacity() + 1) << 1;
            reserve(new_capacity, arena);
            *_end = value;
            ++_end;
        }
    }
    T& operator[](const int32_t index) const {
        return *(_begin + index); 
    }

private:
    T* _begin = nullptr;
    T* _end = nullptr;
    T* _mem_end = nullptr;
};

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
