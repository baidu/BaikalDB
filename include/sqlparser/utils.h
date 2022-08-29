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
#include <cstdint>
#include <cctype>
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
    size_t length;
    void strdup(const char* str, int len, butil::Arena& arena) {
        value = (char*)arena.allocate(len + 1);
        length = len;
        memcpy(value, str, len);
        value[len] = '\0';
    }
    void strdup(const char* str, butil::Arena& arena) {
        strdup(str, strlen(str), arena);
    }
    void append(const char* str, butil::Arena& arena) {
        int len = strlen(str);
        int old_len = length;
        length += len;
        char* value_new = (char*)arena.allocate(length + 1);
        memcpy(value_new, value, old_len);
        memcpy(value_new + old_len, str, len);
        value_new[length] = '\0';
        value = value_new;
    }
    // cannot have constructor in union
    void set_null() {
        value = nullptr;
        length = 0;
    }
    const char* c_str() const {
        return value;
    }
    std::string to_string() const {
        return std::string(value, length);
    }
    bool empty() const {
        return (length == 0 || value == nullptr || value[0] == '\0');
    }
    void restore_5c() {
        size_t i = 0;
        while (i < length) {
            if ((value[i] & 0x80) != 0) {
                if (++i >= length) {
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
        while (fast < length) {
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
        length = slow;
    }
    String& to_lower_inplace() {
        if (value != nullptr) {
            std::transform(value, value + length, value, ::tolower);
        }
        return *this;
    }
    std::string to_lower() const {
        if (value != nullptr) {
            std::string tmp(value, length);
            std::transform(tmp.begin(), tmp.end(), tmp.begin(), ::tolower);
            return tmp;
        } else {
            return std::string();
        }
    }
    // shallow copy
    String& operator=(std::nullptr_t n) {
        value = nullptr;
        length = 0;
        return *this;
    }
    String& operator=(char* str) {
        value = str;
        length = strlen(str);
        return *this;
    }
    String& operator=(const char* str) {
        value = (char*)str;
        length = strlen(str);
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

    bool starts_with(const char* prefix) const {
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

    bool is_print() {
        size_t start = 0;
        while (start < length) {
            if (!std::isprint(value[start++])) {
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

inline char bit_to_char(const char* str, size_t len) {
    char out = 0;
    for (size_t i = 0; i < len; i++) {
        out = out * 2 + str[i] - '0';
    }
    return out;
}

inline char hex_to_char(const char* str, size_t len) {
    char out = 0;
    for (size_t i = 0; i < len; i++) {
        if (str[i] >= 'A' && str[i] <= 'F') {
            out = out * 16 + str[i] - 'A' + 10;
        } else if (str[i] >= 'a' && str[i] <= 'f') {
            out = out * 16 + str[i] - 'a' + 10;
        } else {
            out = out * 16 + str[i] - '0';
        }
    }
    return out;
}

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
