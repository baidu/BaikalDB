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

#include "utils.h"
#include <charconv>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>
#include "arrow/vendored/fast_float/fast_float.h"

namespace baikaldb {
template<typename T> 
static void from_chars_to_value(const std::string& str, T& val) {
    if constexpr (std::is_same<T, bool>::value) {
        int tmp = 0;
        std::from_chars(str.data(), str.data() + str.size(), tmp);
        val = (tmp != 0);
    } else if constexpr (std::is_floating_point<T>::value) {
        // 浮点: 使用 fast_float
        arrow_vendored::fast_float::from_chars(str.data(), str.data() + str.size(), val);
    } else {
        // 整数: 使用 std::from_chars
        std::from_chars(str.data(), str.data() + str.size(), val);
    }
}

// 通用数值类型模板
template<typename T>
static void from_chars_to_array(const std::string& str, std::vector<T>& vec) {
    static_assert(std::is_arithmetic<T>::value, "This template only supports arithmetic types");
    
    const char* ptr = str.data();
    size_t i = 0;
    // trim 空格 [
    while (i < str.size() && (str[i] == '[' || isspace(str[i]))) {
        ++ptr;
        ++i;
    }
    size_t len = str.size();
    while (len > 0 && (str[len - 1] == ']' || isspace(str[len - 1]))) {
        --len;
    }

    while (1) {
        // 跳过前导空格
        while (ptr < str.data() + len && ptr[0] == ' ') {
            ++ptr;
        }
        if (ptr >= str.data() + len) {
            break;
        }
        if (ptr[0] == '+') {
            // std::from_chars不能跳过正号，这里手动跳过
            ++ptr;
        }
        
        T val = 0;
        if constexpr (std::is_same<T, bool>::value) {
            int tmp = 0;
            auto res = std::from_chars(ptr, str.data() + len, tmp);
            vec.emplace_back(tmp != 0);
            ptr = res.ptr;
        } else if constexpr (std::is_floating_point<T>::value) {
            auto res = arrow_vendored::fast_float::from_chars(ptr, str.data() + len, val);
            vec.emplace_back(val);
            ptr = res.ptr;
        } else {
            auto res = std::from_chars(ptr, str.data() + len, val);
            vec.emplace_back(val);
            ptr = res.ptr;
        }
        if (ptr >= str.data() + len) {
            break;
        }
        while (ptr < str.data() + len && ptr[0] == ' ') {
            ++ptr;
        }
        ++ptr;
    }
}

static void from_chars_to_float_vec(const std::string& str, std::vector<float>& vec) {
    return from_chars_to_array<float>(str, vec);
}

// std::string 特化版本，支持解析带引号的字符串数组
// 如: "a", ",ss", 'c', "ddd" 或 ["a", "b", "c"], TODO 各种引号不太好处理
template<>
inline void from_chars_to_array<std::string>(const std::string& str, std::vector<std::string>& vec) {
    if (str.empty() || str == "[]") {
        return;
    }
    vec.emplace_back(str);
}

struct ArrayDataBase {
    virtual ~ArrayDataBase() = default;
    virtual size_t size() const = 0;
    virtual std::string get_values_string(const std::string split = ", ") const = 0;
    virtual ArrayDataBase* clone() const = 0;
};

enum ArrayContainOpt {
    ContainAll = 1,
    ContainAny,
};

template <typename T>
struct ArrayValue : public ArrayDataBase {
    std::vector<T> data;
    ArrayValue() {}

    // 拷贝构造函数
    ArrayValue(const ArrayValue& other) : data(other.data) {}

    // 拷贝赋值运算符
    ArrayValue& operator=(const ArrayValue& other) {
        if (this != &other) {
            data = other.data;
        }
        return *this;
    }

    void add_value(const T& v) {
        data.emplace_back(v);
    }

    size_t size() const override { return data.size();}

    std::string get_values_string(const std::string split = ", ") const override {
        std::string ret;
        for (size_t i = 0; i < data.size(); i++) {
            if (i > 0) {
                ret += split;
            }
            if constexpr (std::is_same<T, std::string>::value) {
                ret += "\"" + data[i] + "\"";
            } else if constexpr (std::is_same<T, float>::value){
                char buf[24] = {0};
                parser::float_to_string(data[i], -1, buf, sizeof(buf));
                ret += buf;
            } else if constexpr (std::is_same<T, double>::value){
                char buf[50] = {0};
                parser::double_to_string(data[i], -1, buf, sizeof(buf));
                ret += buf;
            } else {
                ret += std::to_string(data[i]);
            }
        }
        return ret;
    }

    template <typename U>
    void copy_to(std::vector<U>& other) {
        other.reserve(data.size());
        if constexpr (std::is_same<T, std::string>::value && !std::is_same<U, std::string>::value) {
            // string->numberic
            for (const auto& d : data) {
                U val = 0;
                from_chars_to_value<U>(d, val);
                other.emplace_back(val);
            }
        } else if constexpr (!std::is_same<T, std::string>::value && std::is_same<U, std::string>::value) {
            // numberic->string
            for (const auto& d : data) {
                other.emplace_back(std::to_string(d));
            }
        } else {
            // numberic->numberic
            for (const auto& d : data) {
                other.emplace_back(d);
            }
        }
    }

    ArrayDataBase* clone() const override {
        return new(std::nothrow) ArrayValue<T>(*this);
    }

    bool contain(ArrayValue<T>* other, ArrayContainOpt opt) {
        if (other == nullptr || other->data.empty()) {
            // ARRAY_TODO 空？应该是true还是false
            return false;
        }
        std::set<T> array_data_set(data.begin(), data.end());
        switch (opt) {
            case ContainAny: {
                for (const auto& val : other->data) {
                    if (array_data_set.count(val) > 0) {
                        return true;
                    }
                }
                return false;
            }
            case ContainAll: {
                for (const auto& val : other->data) {
                    if (array_data_set.count(val) == 0) {
                        return false;
                    }
                }
                return true;
            }
            default:
                return false;
        }
    }

    T get_element(int64_t pos, bool& has_pos) {
        if (pos >= data.size()) {
            has_pos = false;
            return T();
        } 
        has_pos = true;
        return data[pos];
    }
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */