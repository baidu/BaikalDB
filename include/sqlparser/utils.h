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
struct String {
    char* value;
    void strdup(const char* str, int len, butil::Arena& arena) {
        value = (char*)arena.allocate(len + 1);
        memcpy(value, str, len);
        value[len] = '\0';
    }
    void strdup(const char* str, butil::Arena& arena) {
        strdup(str, strlen(str), arena);
    }
    void append(const char* str, butil::Arena& arena) {
        int len = strlen(str);
        int old_len = strlen(value);
        char* value_new = (char*)arena.allocate(len + old_len + 1);
        memcpy(value_new, value, old_len);
        memcpy(value_new, str, len);
        value_new[len + old_len] = '\0';
        value = value_new;
    }
    // cannot have constructor in union
    void set_null() {
        value = nullptr;
    }
    const char* c_str() const {
        return value;
    }
    bool empty() const {
        return (value == nullptr || value[0] == '\0');
    }
    void restore_5c() {
        size_t i = 0;
        size_t len = strlen(value);
        while (i < len) {
            if ((value[i] & 0x80) != 0) {
                if (++i >= len) {
                    return;
                }
                if (value[i] == 0x7F) {
                    value[i] = 0x5C;
                }
            }
            ++i;
        }
    }
    void stripslashes() {
        size_t slow = 0;
        size_t fast = 0;
        bool has_slash = false;
        static std::unordered_map<char, char> trans_map = {
            {'\\', '\\'},
            {'\"', '\"'},
            {'\'', '\''},
            {'r', '\r'},
            {'t', '\t'},
            {'n', '\n'},
            {'b', '\b'},
            {'Z', '\x1A'},
        };
        size_t len = strlen(value);
        while (fast < len) {
            if (has_slash) {
                if (trans_map.count(value[fast]) == 1) {
                    value[slow++] = trans_map[value[fast++]];
                } else if (value[fast] == '%' || value[fast] == '_') {
                    // like中的特殊符号，需要补全'\'
                    value[slow++] = '\\';
                    value[slow++] = value[fast++];
                }
                has_slash = false;
            } else {
                if (value[fast] == '\\') {
                    has_slash = true;
                    fast++;
                } else {
                    value[slow++] = value[fast++];
                }
            }
        }
        value[slow] = '\0';
    }
    String& to_lower_inplace() {
        if (value != nullptr) {
            int len = strlen(value);
            std::transform(value, value + len, value, ::tolower);
        }
        return *this;
    }
    std::string to_lower() const {
        if (value != nullptr) {
            std::string tmp = value;
            std::transform(tmp.begin(), tmp.end(), tmp.begin(), ::tolower);
            return tmp;
        } else {
            return std::string();
        }
    }
    // shallow copy
    String& operator=(std::nullptr_t n) {
        value = nullptr;
        return *this;
    }
    String& operator=(char* str) {
        value = str;
        return *this;
    }
    String& operator=(const char* str) {
        value = (char*)str;
        return *this;
    }

    bool operator==(const String& rhs) {
        if (empty() && rhs.empty()) {
            return true;
        }
        if (empty() || rhs.empty()) {
            return false;
        }
        return strcmp(value, rhs.value) == 0;
    }

    bool starts_with(const char* prefix) {
        if (prefix == nullptr) {
            return true;
        }
        int prefix_len = strlen(prefix);
        int value_len = strlen(value);
        if (value_len < prefix_len) {
            return false;
        }
        for (int idx = 0; idx < prefix_len; ++idx) {
            if (prefix[idx] != value[idx]) {
                return false;
            }
        }
        return true;
    }
};
inline std::ostream& operator<<(std::ostream& os, const String& str) {
    if (str.value == nullptr) {
        return os;
    }
    os << str.value;
    return os;
}

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
