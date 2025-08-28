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

#include "internal_functions.h"
#include <openssl/md5.h>
#include <rapidjson/pointer.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include "hll_common.h"
#include "datetime.h"
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/algorithm/string.hpp>
#include <cctype>
#include <cmath>
#include <algorithm>
#include <re2/re2.h>
#include <boost/algorithm/string.hpp>
#include <boost/regex.hpp>

namespace baikaldb {
#ifdef BAIKALDB_REVISION
    DEFINE_string(db_version, "5.7.16-BaikalDB-v"BAIKALDB_REVISION, "db version");
#else
    DEFINE_string(db_version, "5.7.16-BaikalDB", "db version");
#endif
#define INPUT_CHECK_NULL \
for (auto& s: input) { \
    if (s.is_null()) { \
        return ExprValue::Null(); \
    } \
}
static const int32_t DATE_FORMAT_LENGTH = 128;
static const std::vector<std::string> day_names = {
        "Sunday", "Monday", "Tuesday", "Wednesday",
        "Thursday", "Friday", "Saturday"};
static const std::vector<std::string> month_names = {
        "January", "February", "March", "April", "May",
        "June", "July", "August", "September",
        "October", "November", "December"};
ExprValue round(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    int bits = input.size() == 2 ? input[1].get_numberic<int>() : 0;
    double base = std::pow(10, bits);
    double orgin = input[0].get_numberic<double>();
    ExprValue tmp(pb::DOUBLE);
    if (base > 0) {
        if (orgin < 0) {
            tmp._u.double_val = -::round(-orgin * base) / base;
        } else {
            tmp._u.double_val = ::round(orgin * base) / base;
        }
    }
    return tmp;
}

ExprValue floor(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::INT64);
    tmp._u.int64_val = ::floor(input[0].get_numberic<double>());
    return tmp;
}

ExprValue ceil(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::INT64);
    tmp._u.int64_val = ::ceil(input[0].get_numberic<double>());
    return tmp;
}

ExprValue abs(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::DOUBLE);
    double val = input[0].get_numberic<double>();
    if (val < 0 ) {
        val = -val;
    }
    tmp._u.double_val = val;
    return tmp;
}

ExprValue sqrt(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    double val = input[0].get_numberic<double>();
    if (val < 0) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::DOUBLE);
    tmp._u.double_val = std::sqrt(val);
    return tmp;
}

ExprValue mod(const std::vector<ExprValue>& input) {
    if (input.size() < 2 || input[0].is_null() || input[1].is_null()) {
        return ExprValue::Null();
    }
    double rhs = input[1].get_numberic<double>();
    if (float_equal(rhs, 0)) {
        return ExprValue::Null();
    }
    double lhs = input[0].get_numberic<double>();
    
    ExprValue tmp(pb::DOUBLE);
    tmp._u.double_val = std::fmod(lhs, rhs);
    return tmp;
}

ExprValue rand(const std::vector<ExprValue>& input) {
    ExprValue tmp(pb::DOUBLE);
    tmp._u.double_val = butil::fast_rand_double();
    return tmp;
}

ExprValue sign(const std::vector<ExprValue>& input) {
    if (input.size() < 1 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::INT64);
    double val = input[0].get_numberic<double>();
    tmp._u.int64_val = val > 0 ? 1 : (val < 0 ? -1 : 0);
    return tmp;
}

ExprValue sin(const std::vector<ExprValue>& input) {
    if (input.size() < 1 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::DOUBLE);
    tmp._u.double_val = std::sin(input[0].get_numberic<double>());
    return tmp;
}
ExprValue asin(const std::vector<ExprValue>& input) {
    if (input.size() < 1 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::DOUBLE);
    double  val = input[0].get_numberic<double>();
    if (val < -1 || val > 1) {
        return ExprValue::Null();
    }
    tmp._u.double_val = std::asin(val);
    return tmp;
}

ExprValue cos(const std::vector<ExprValue>& input) {
    if (input.size() < 1 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::DOUBLE);
    tmp._u.double_val = std::cos(input[0].get_numberic<double>());
    return tmp;
}

ExprValue acos(const std::vector<ExprValue>& input) {
    if (input.size() < 1 || input[0].is_null()) {
        return ExprValue::Null();
    }
    double  val = input[0].get_numberic<double>();
    if (val < -1 || val > 1) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::DOUBLE);
    tmp._u.double_val = std::acos(val);
    return tmp;
}

ExprValue tan(const std::vector<ExprValue>& input) {
    if (input.size() < 1 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::DOUBLE);
    tmp._u.double_val = std::tan(input[0].get_numberic<double>());
    return tmp;
}

ExprValue cot(const std::vector<ExprValue>& input) {
    if (input.size() < 1 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::DOUBLE);
    double val = input[0].get_numberic<double>();
    double sin_val = std::sin(val);
    double cos_val = std::cos(val);
    if (float_equal(sin_val, 0)) {
        return ExprValue::Null();
    }
    tmp._u.double_val = cos_val/sin_val;
    return tmp;
}

ExprValue atan(const std::vector<ExprValue>& input) {
    if (input.size() < 1 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::DOUBLE);
    tmp._u.double_val = std::atan(input[0].get_numberic<double>());
    return tmp;
}

ExprValue ln(const std::vector<ExprValue>& input) {
    if (input.size() < 1 || input[0].is_null()) {
        return ExprValue::Null();
    }
    double val = input[0].get_numberic<double>();
    if (val <= 0) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::DOUBLE);
    tmp._u.double_val = std::log(input[0].get_numberic<double>());
    return tmp;
}

ExprValue log(const std::vector<ExprValue>& input) {
    if (input.size() < 2 || input[0].is_null() || input[1].is_null()) {
        return ExprValue::Null();
    }
    double base = input[0].get_numberic<double>();
    double val = input[1].get_numberic<double>();
    if (base <= 0 || val <= 0 || base == 1) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::DOUBLE);
    tmp._u.double_val = std::log(val) / std::log(base);
    return tmp;
}

ExprValue pow(const std::vector<ExprValue>& input) {
    if (input.size() < 2 || input[0].is_null() || input[1].is_null()) {
        return ExprValue::Null();
    }
    double base = input[0].get_numberic<double>();
    double exp = input[1].get_numberic<double>();
    ExprValue tmp(pb::DOUBLE);
    tmp._u.double_val = std::pow(base, exp);
    return tmp;
}

ExprValue pi(const std::vector<ExprValue>& input) {
    ExprValue tmp(pb::DOUBLE);
    tmp._u.double_val = M_PI;
    return tmp;
}

ExprValue greatest(const std::vector<ExprValue>& input) {
    bool find_flag = false;
    double ret = std::numeric_limits<double>::lowest();
    for (const auto& item : input) {
        if (item.is_null()) {
            return ExprValue::Null();
        } else {
            double val = item.get_numberic<double>();
            if (!find_flag) {
                find_flag = true;
                ret = val;
            } else {
                if (val > ret) {
                    ret = val;
                }
            }
        }
    }
    if (find_flag) {
        ExprValue tmp(pb::DOUBLE);
        tmp._u.double_val = ret;
        return tmp;
    } else {
        return ExprValue::Null();
    }
}

ExprValue least(const std::vector<ExprValue>& input) {
    bool find_flag = false;
    double ret = std::numeric_limits<double>::max();
    for (const auto& item : input) {
        if (item.is_null()) {
            return ExprValue::Null();
        } else {
            double val = item.get_numberic<double>();
            if (!find_flag) {
                find_flag = true;
                ret = val;
            } else {
                if (val < ret) {
                    ret = val;
                }
            }
        }
    }
    if (find_flag) {
        ExprValue tmp(pb::DOUBLE);
        tmp._u.double_val = ret;
        return tmp;
    } else {
        return ExprValue::Null();
    }
}

ExprValue length(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::UINT32);
    tmp._u.uint32_val = input[0].get_string().size();
    return tmp;
}

ExprValue bit_length(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::UINT32);
    tmp._u.uint32_val = input[0].get_string().size() * 8;
    return tmp;
}
ExprValue bit_count(const std::vector<ExprValue>& input) {
    if (input.size() != 1 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    tmp.cast_to(pb::UINT64);
    ExprValue res(pb::INT64);
    while (tmp._u.uint64_val) {
        if (tmp._u.uint64_val & 1) {
            res._u.int64_val += 1;
        }
        tmp._u.uint64_val >>= 1;
    }
    return res;
}

ExprValue lower(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::STRING);
    tmp.str_val = input[0].get_string();
    std::transform(tmp.str_val.begin(), tmp.str_val.end(), tmp.str_val.begin(), ::tolower); 
    return tmp;
}

ExprValue lower_gbk(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::STRING);
    tmp.str_val = input[0].get_string();
    std::string& literal = tmp.str_val;
    size_t idx = 0;
    while (idx < literal.size()) {
        if ((literal[idx] & 0x80) != 0) {
            idx += 2;
        } else {
            literal[idx] = tolower(literal[idx]);
            idx++;
        }
    }
    return tmp;
}

ExprValue upper(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::STRING);
    tmp.str_val = input[0].get_string();
    std::transform(tmp.str_val.begin(), tmp.str_val.end(), tmp.str_val.begin(), ::toupper); 
    return tmp;
}

ExprValue concat(const std::vector<ExprValue>& input) {
    ExprValue tmp(pb::STRING);
    for (auto& s : input) {
        if (s.is_null()) {
            return ExprValue::Null();
        }
        tmp.str_val += s.get_string();
    }
    return tmp;
}

ExprValue substr(const std::vector<ExprValue>& input) {
    if (input.size() < 2) {
        return ExprValue::Null();
    }
    for (auto& s : input) {
        if (s.is_null()) {
            return ExprValue::Null();
        }
    }
    std::string str = input[0].get_string();
    ExprValue tmp(pb::STRING);
    int pos = input[1].get_numberic<int>();
    if (pos < 0) {
        pos = str.size() + pos;
    } else {
        --pos;
    }
    if (pos < 0 || pos >= (int)str.size()) {
        return tmp;
    }
    int len = -1;
    if (input.size() == 3) {
        len = input[2].get_numberic<int>();
        if (len <= 0) {
            return tmp;
        }
    }
    tmp.str_val = str;
    if (len == -1) {
        tmp.str_val = tmp.str_val.substr(pos);
    } else {
        tmp.str_val = tmp.str_val.substr(pos, len);
    }
    return tmp;
}

ExprValue left(const std::vector<ExprValue>& input) {
    if (input.size() < 2) {
        return ExprValue::Null();
    }
    for (auto& s : input) {
        if (s.is_null()) {
            return ExprValue::Null();
        }
    }
    ExprValue tmp(pb::STRING);
    int len = input[1].get_numberic<int>();
    if (len <= 0) {
        return tmp;
    }
    tmp.str_val = input[0].str_val;
    tmp.str_val = tmp.str_val.substr(0, len);
    return tmp;
}

ExprValue right(const std::vector<ExprValue>& input) {
    if (input.size() < 2) {
        return ExprValue::Null();
    }
    for (auto& s : input) {
        if (s.is_null()) {
            return ExprValue::Null();
        }
    }
    ExprValue tmp(pb::STRING);
    int len = input[1].get_numberic<int>();
    if (len <= 0) {
        return tmp;
    }
    int pos = input[0].str_val.size() - len;
    if (pos < 0) {
        pos = 0;
    }
    tmp.str_val = input[0].str_val;
    tmp.str_val = tmp.str_val.substr(pos);
    return tmp;
}

ExprValue trim(const std::vector<ExprValue>& input) {
    if (input.size() != 1) {
        return ExprValue::Null();
    }

    ExprValue tmp(pb::STRING);
    tmp.str_val = input[0].str_val;
    tmp.str_val.erase(0, tmp.str_val.find_first_not_of(" "));
    tmp.str_val.erase(tmp.str_val.find_last_not_of(" ") + 1); 

    return tmp;
}

ExprValue ltrim(const std::vector<ExprValue>& input) {
    if (input.size() != 1) {
        return ExprValue::Null();
    }

    ExprValue tmp(pb::STRING);
    tmp.str_val = input[0].str_val;
    tmp.str_val.erase(0, tmp.str_val.find_first_not_of(" "));

    return tmp;
}

ExprValue rtrim(const std::vector<ExprValue>& input) {
    if (input.size() != 1) {
        return ExprValue::Null();
    }

    ExprValue tmp(pb::STRING);
    tmp.str_val = input[0].str_val;
    tmp.str_val.erase(tmp.str_val.find_last_not_of(" ") + 1); 

    return tmp;
}

ExprValue lpad(const std::vector<ExprValue>& input) {
    if (input.size() != 3) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::STRING);
    std::string str = input[0].get_string();
    int64_t len = input[1].get_numberic<int64_t>();
    if (len <= 0 || len > UINT16_MAX) {
        return ExprValue::Null();
    }
    if (len <= str.length()) {
        tmp.str_val.append(str.begin(), str.begin() + len);
        return tmp;
    }
    std::string padstr = input[2].get_string();
    if (padstr.length() == 0) {
        return ExprValue::Null();
    }
    size_t padlen = len - str.length();
    while (padlen > padstr.length()) {
        tmp.str_val.append(padstr);
        padlen -= padstr.length();
    }
    tmp.str_val.append(padstr.begin(), padstr.begin() + padlen);
    tmp.str_val.append(str);
    return tmp;
}
ExprValue rpad(const std::vector<ExprValue>& input) {
    if (input.size() != 3) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::STRING);
    std::string str = input[0].get_string();
    int64_t len = input[1].get_numberic<int64_t>();
    if (len <= 0 || len > UINT16_MAX) {
        return ExprValue::Null();
    }
    if (len <= str.length()) {
        tmp.str_val.append(str.begin(), str.begin() + len);
        return tmp;
    }
    std::string padstr = input[2].get_string();
    if (padstr.length() == 0) {
        return ExprValue::Null();
    }
    tmp.str_val.append(str);
    size_t padlen = len - str.length();
    while (padlen > padstr.length()) {
        tmp.str_val.append(padstr);
        padlen -= padstr.length();
    }
    tmp.str_val.append(padstr.begin(), padstr.begin() + padlen);
    return tmp;
}

ExprValue concat_ws(const std::vector<ExprValue>& input) {
    if (input.size() < 2) {
        return ExprValue::Null();
    }

    if (input[0].is_null()) {
        return ExprValue::Null();
    }

    ExprValue tmp(pb::STRING);

    bool first_push = false;
    for (uint32_t i = 1; i < input.size(); i++) {
        if (!input[i].is_null()) {
            if (!first_push) {
                first_push = true;
                tmp.str_val = input[i].get_string();
            } else {
                tmp.str_val += input[0].get_string() + input[i].get_string();
            }
        }
    }

    if (!first_push) {
        return ExprValue::Null();
    }

    return tmp;
}

ExprValue split_part(const std::vector<ExprValue>& input) {
    // 检查输入向量的大小是否为 3
    if (input.size() != 3) {
        return ExprValue::Null();
    }
    // 检查输入向量中是否有任何一个元素为 null
    for (const auto& s : input) {
        if (s.is_null()) {
            return ExprValue::Null();
        }
    }

    // 获取输入字符串和分隔符
    std::string str = input[0].get_string();
    std::string delimiter = input[1].get_string();
    // 获取要返回的部分的索引
    int part_index = input[2].get_numberic<int>();

    // 检查索引是否合法
    if (part_index <= 0) {
        return ExprValue::Null();
    }

    // 创建一个 ExprValue 对象，用于存储结果，类型为 STRING
    ExprValue tmp(pb::STRING);

    // 分割字符串
    size_t start = 0;
    size_t end = str.find(delimiter);
    int current_part = 1;
   
    while (end != std::string::npos) {
        if (current_part == part_index) {
            tmp.str_val = str.substr(start, end - start);
            return tmp;
        }
        start = end + delimiter.length();
        end = str.find(delimiter, start);
        current_part++;
    }

    // 检查最后一部分
    if (current_part == part_index) {
        tmp.str_val = str.substr(start);
    } else {
        tmp.str_val.clear(); // 如果索引超出范围，返回空字符串
    }
    return tmp;
}

ExprValue ascii(const std::vector<ExprValue>& input) {
    if (input.size() < 1) {
        return ExprValue::Null();
    }

    if (input[0].is_null()) {
        return ExprValue::Null();
    }

    ExprValue tmp(pb::INT32);
    ExprValue in = input[0];
    in.cast_to(pb::STRING);
    tmp._u.int32_val = static_cast<uint8_t>(in.str_val[0]);

    return tmp;
}

ExprValue strcmp(const std::vector<ExprValue>& input) {
    if (input.size() != 2) {
        return ExprValue::Null();
    }

    ExprValue tmp(pb::INT32);
    
    int64_t ret = input[0].compare(input[1]);
    if (ret < 0) {
        tmp._u.int32_val = -1;
    } else if (ret > 0) {
        tmp._u.int32_val = 1;
    } else {
        tmp._u.int32_val = 0;
    }

    return tmp;
}

ExprValue insert(const std::vector<ExprValue>& input) {
    if (input.size() != 4) {
        return ExprValue::Null();
    }

    for (auto s : input) {
        if (s.is_null()) {
            return ExprValue::Null();
        }
    }

    int pos = input[1].get_numberic<int>();
    if (pos < 1) {
        return input[0];
    }
    pos -= 1;

    int len = input[2].get_numberic<int>();
    if (len <= 0) {
        return input[0];
    }
    ExprValue tmp(pb::STRING);
    tmp.str_val = input[0].str_val;
    if (pos >= input[0].str_val.size()) {
        return tmp;
    }
    tmp.str_val.replace(pos, len, input[3].str_val);

    return tmp;
}

ExprValue replace(const std::vector<ExprValue>& input) {
    if (input.size() != 3) {
        return ExprValue::Null();
    }

    if (input[0].is_null()) {
        return ExprValue::Null();
    }

    if (input[1].str_val.empty()) {
        return input[0];
    }

    ExprValue tmp(pb::STRING);
    tmp.str_val = input[0].str_val;

    for (size_t pos = 0; pos != std::string::npos; pos += input[2].str_val.length()) {
        pos = tmp.str_val.find(input[1].str_val, pos);
        if (pos != std::string::npos) {
            tmp.str_val.replace(pos, input[1].str_val.length(), input[2].str_val);
        } else {
            break;
        }
    }

    return tmp;
}

ExprValue repeat(const std::vector<ExprValue>& input) {
    if (input.size() != 2) {
        return ExprValue::Null();
    }

    if (input[0].is_null()) {
        return ExprValue::Null();
    }

    int len = input[1].get_numberic<int>();
    if (len <= 0) {
        return ExprValue::Null();
    }

    std::string val = input[0].get_string();
    ExprValue tmp(pb::STRING);
    tmp.str_val.reserve(val.size() * len);

    for (int32_t i = 0; i < len; i++) {
        tmp.str_val += val;
    }

    return tmp;
}

ExprValue reverse(const std::vector<ExprValue>& input) {
    if (input.size() != 1) {
        return ExprValue::Null();
    }

    if (input[0].is_null()) {
        return ExprValue::Null();
    }

    ExprValue tmp(pb::STRING);
    tmp.str_val = input[0].get_string();
    std::reverse(tmp.str_val.begin(), tmp.str_val.end());

    return tmp;
}

ExprValue locate(const std::vector<ExprValue>& input) {
    if (input.size() < 2 || input.size() > 3) {
        return ExprValue::Null();
    }

    for (auto s : input) {
        if (s.is_null()) {
            return ExprValue::Null();
        }
    }

    int begin_pos = 0;
    if (input.size() == 3) {
        begin_pos = input[2].get_numberic<int>() - 1;
    }
    
    ExprValue tmp(pb::INT32);
    auto pos = input[1].get_string().find(input[0].get_string(), begin_pos);
    if (pos != std::string::npos) {
        tmp._u.int32_val = pos + 1;
    } else {
        tmp._u.int32_val = 0;
    }

    return tmp;
}

ExprValue instr(const std::vector<ExprValue>& input) {
    if (input.size() != 2) {
        return ExprValue::Null();
    }

    for (auto s : input) {
        if (s.is_null()) {
            return ExprValue::Null();
        }
    }
    int begin_pos = 0;
    ExprValue tmp(pb::INT32);
    auto pos = input[0].get_string().find(input[1].get_string(), begin_pos);
    if (pos != std::string::npos) {
        tmp._u.int32_val = pos + 1;
    } else {
        tmp._u.int32_val = 0;
    }

    return tmp;
}

// @ref: https://dev.mysql.com/doc/refman/8.0/en/json-search-functions.html#function_json-extract
// @ref: https://rapidjson.org/md_doc_pointer.html#JsonPointer
ExprValue json_extract(const std::vector<ExprValue>& input) {
    if (input.size() < 2) {
        return ExprValue::Null();
    }

    bool return_list = false;
    if (input.size() > 2) {
        return_list = true;
    }

    for (auto s : input) {
        if (s.is_null()) {
            return ExprValue::Null();
        }
    }
    std::string json_str = input[0].get_string();

    std::vector<std::string> paths;
    for (int i = 1; i < input.size(); ++i) {
        std::string path = input[i].get_string();
        if (path.length() > 0 && path[0] == '$') {
            path.erase(path.begin());
        } else {
            return ExprValue::Null();
        }
        std::replace(path.begin(), path.end(), '.', '/');
        std::replace(path.begin(), path.end(), '[', '/');
        path.erase(std::remove(path.begin(), path.end(), ']'), path.end());
        paths.emplace_back(path);
    }

    rapidjson::Document doc;
    try {
        doc.Parse<0>(json_str.c_str());
        if (doc.HasParseError()) {
            rapidjson::ParseErrorCode code = doc.GetParseError();
            DB_DEBUG("parse json_str error [code:%d][%s]", code, json_str.c_str());
            return ExprValue::Null();
        }

    } catch (...) {
        DB_DEBUG("parse json_str error [%s]", json_str.c_str());
        return ExprValue::Null();
    }

    std::vector<std::string> results;
    for (const auto& path: paths) {
        rapidjson::Pointer pointer(path.c_str());
        if (!pointer.IsValid()) {
            DB_DEBUG("invalid path: [%s]", path.c_str());
            return ExprValue::Null();
        }

        const rapidjson::Value *pValue = rapidjson::GetValueByPointer(doc, pointer);
        if (pValue == nullptr) {
            DB_DEBUG("the path: [%s] does not exist in doc [%s]", path.c_str(), json_str.c_str());
            continue;
        }
        // TODO type on fly
        if (pValue->IsString()) {
            results.emplace_back(pValue->GetString());
        } else if (pValue->IsInt()) {
            results.emplace_back(std::to_string(pValue->GetInt()));
        } else if (pValue->IsInt64()) {
            results.emplace_back(std::to_string(pValue->GetInt64()));
        } else if (pValue->IsUint()) {
            results.emplace_back(std::to_string(pValue->GetUint()));
        } else if (pValue->IsUint64()) {
            results.emplace_back(std::to_string(pValue->GetUint64()));
        } else if (pValue->IsDouble()) {
            results.emplace_back(std::to_string(pValue->GetDouble()));
        } else if (pValue->IsFloat()) {
            results.emplace_back(std::to_string(pValue->GetFloat()));
        } else if (pValue->IsBool()) {
            results.emplace_back(std::to_string(pValue->GetBool()));
        } else if (pValue->IsObject() || pValue->IsArray()) {
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            pValue->Accept(writer);
            results.emplace_back(buffer.GetString());
        }
    }
    ExprValue tmp(pb::STRING);
    if (results.empty()) {
        return ExprValue::Null();
    } else if (return_list) {
        std::string return_str = "[";
        for (const auto& it: results) {
            return_str.append(it);
            return_str.append(", ");
        }
        return_str.pop_back();
        return_str.back() = ']';
        tmp.str_val = return_str;
    } else {
        tmp.str_val = results[0];
    }
    return tmp;
}

ExprValue json_extract1(const std::vector<ExprValue>& input) {
    if (input.size() != 2) {
        return ExprValue::Null();
    }

    for (auto s : input) {
        if (s.is_null()) {
            return ExprValue::Null();
        }
    }
    std::string json_str = input[0].get_string();
    std::string path = input[1].get_string();
    if (path.length() > 0 && path[0] == '$') {
        path.erase(path.begin());
    } else {
        return ExprValue::Null();
    }
    std::replace(path.begin(), path.end(), '.', '/');
    std::replace(path.begin(), path.end(), '[', '/');
    path.erase(std::remove(path.begin(), path.end(), ']'), path.end());

    rapidjson::Document doc;
    try {
        doc.Parse<0>(json_str.c_str());
        if (doc.HasParseError()) {
            rapidjson::ParseErrorCode code = doc.GetParseError();
            DB_DEBUG("parse json_str error [code:%d][%s]", code, json_str.c_str());
            return ExprValue::Null();
        }

    } catch (...) {
        DB_DEBUG("parse json_str error [%s]", json_str.c_str());
        return ExprValue::Null();
    }
    rapidjson::Pointer pointer(path.c_str());
    if (!pointer.IsValid()) {
        DB_DEBUG("invalid path: [%s]", path.c_str());
        return ExprValue::Null();
    }

    const rapidjson::Value *pValue = rapidjson::GetValueByPointer(doc, pointer);
    if (pValue == nullptr) {
        DB_DEBUG("the path: [%s] does not exist in doc [%s]", path.c_str(), json_str.c_str());
        return ExprValue::Null();
    }
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    // TODO type on fly
    ExprValue tmp(pb::STRING);
    /*
    if (pValue->IsString()) {
        tmp.str_val = pValue->GetString();
    } else if (pValue->IsInt()) {
        tmp.str_val = std::to_string(pValue->GetInt());
    } else if (pValue->IsInt64()) {
        tmp.str_val = std::to_string(pValue->GetInt64());
    } else if (pValue->IsUint()) {
        tmp.str_val = std::to_string(pValue->GetUint());
    } else if (pValue->IsUint64()) {
        tmp.str_val = std::to_string(pValue->GetUint64());
    } else if (pValue->IsDouble()) {
        tmp.str_val = std::to_string(pValue->GetDouble());
    } else if (pValue->IsFloat()) {
        tmp.str_val = std::to_string(pValue->GetFloat());
    } else if (pValue->IsBool()) {
        tmp.str_val = std::to_string(pValue->GetBool());
    }
    */
    pValue->Accept(writer);
    tmp.str_val = buffer.GetString();
    return tmp;
}

ExprValue json_type(const std::vector<ExprValue>& input) {
    if (input.size() != 1) {
        return ExprValue::Null();
    }
    ExprValue res(pb::STRING);
    if (input[0].is_int()) {
        res.str_val = "INTEGER";
    } else if (input[0].is_double()) {
        res.str_val = "DOUBLE";
    } else if (input[0].is_bool()) {
        res.str_val = "BOOLEAN";
    } else if (input[0].is_null()) {
        res.str_val = "NULL";
    } else if (input[0].is_string()) {
        rapidjson::Document root;
        root.Parse<0>(input[0].str_val.c_str());
        if (root.IsObject()) {
            res.str_val = "OBJECT";
        } else if (root.IsArray()) {
            res.str_val = "ARRAY";
        } else {
            res.str_val = "STRING";
        }
    } else {
        return ExprValue::Null();
    }
    return res;
}

ExprValue json_array(const std::vector<ExprValue>& input) {
    if (input.size() < 1) {
        return ExprValue::Null();
    }
    rapidjson::Document list;
    list.SetArray();
    for (size_t i = 0; i < input.size(); i ++) {
        list.PushBack(rapidjson::StringRef(input[i].get_string().c_str()), list.GetAllocator());
    }
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    list.Accept(writer);
    ExprValue res(pb::STRING);
    res.str_val = buffer.GetString();
    return res;
}

ExprValue json_object(const std::vector<ExprValue>& input) {
    if (input.size() < 1 || input.size() & 1) {
        return ExprValue::Null();
    }
    rapidjson::Document obj;
    obj.SetObject();
    // TODO 相同的key会重复
    for (size_t i = 0; i < input.size() ; i += 2) {
        obj.AddMember(rapidjson::StringRef(input[i].get_string().c_str()), rapidjson::StringRef(input[i + 1].get_string().c_str()), obj.GetAllocator());
    }
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    obj.Accept(writer);
    ExprValue res(pb::STRING);
    res.str_val = buffer.GetString();
    return res;
}

ExprValue json_valid(const std::vector<ExprValue>& input) {
    if (input.size() != 1) {
        return ExprValue::Null();
    }
    if (input[0].type != pb::JSON && input[0].type != pb::STRING) {
        return ExprValue::Null();
    }
    rapidjson::Document obj;
    obj.Parse<0>(input[0].str_val.c_str());
    if (obj.HasParseError()) {
       return ExprValue::False(); 
    }
    return ExprValue::True();
}

// 返回找到了第几个，如果找到了第n个还会把对应位置替换
int64_t ReplaceNthMatch(std::string& text, const re2::RE2& pattern, const std::string& rewrite, int64_t n) {
    re2::StringPiece input(text);  // Wrap the text in a StringPiece
    std::string output;
    std::string match;
    int count = 0;

    size_t pos_ahead = 0;
    // ^ 或者$ 这种， 防止FindAndConsume 死循环
    bool special_pattern = false;

    while (count < n - 1 && RE2::FindAndConsume(&input, pattern)) {
        count++;
        pos_ahead = text.size() - input.size();
        if (pos_ahead == 0 || pos_ahead == text.size()) {
            special_pattern = true;
            break;
        }
    }

    if (!special_pattern && count == n - 1) {
        std::string to_replace(input.data(), input.size());
        if (RE2::Replace(&to_replace, pattern, rewrite)) {
            text = text.substr(0, pos_ahead) + to_replace;
            return n;
        }
    }

    return count;
}

// https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-replace
// 第六个参数 match_type是个字符串
// c: Case-sensitive matching.
// i: Case-insensitive matching.
// m: Multiple-line mode. Recognize line terminators within the string. The default behavior is to match line terminators only at the start and end of the string expression.
// n: The . character matches line terminators. The default is for . matching to stop at the end of a line.
// u（不支持）: Unix-only line endings. Only the newline character is recognized as a line ending by the ., ^, and $ match operators.
ExprValue regexp_replace(const std::vector<ExprValue>& input) {

    int64_t pos = 1;
    int64_t occurrence = 0;
    bool is_case_sensitive = true;
    bool is_multi_line_mode = false;
    bool dot_match_newline = false;

    switch (input.size()) {
        case 6:{
            const std::string& match_type = input[5].get_string();
            if (std::string::npos != match_type.find('i')) {
                is_case_sensitive = false;
            }
            if (std::string::npos != match_type.find('c')) {
                is_case_sensitive = true;
            }
            if (std::string::npos != match_type.find('m')) {
                is_multi_line_mode = true;
            }
            if (std::string::npos != match_type.find('n')) {
                dot_match_newline = true;
            }
        }
        case 5:
            if (!input[4].is_int()) {
                return ExprValue::Null();
            }
            occurrence = input[4].get_numberic<int64_t>();
        case 4:
            if (!input[3].is_int()) {
                return ExprValue::Null();
            }
            pos = input[3].get_numberic<int64_t>();
            if (pos < 1) {
                return ExprValue::Null();
            }
        case 3:
            if (input[0].type != pb::PrimitiveType::STRING
                || input[1].type != pb::PrimitiveType::STRING
                || input[2].type != pb::PrimitiveType::STRING) {
                return ExprValue::Null();
            }
            break;
        default:
            return ExprValue::Null();
    }

    // (?i) 表示大小写不敏感
    std::string pattern_str = input[1].get_string();
    if (!is_case_sensitive) {
        pattern_str = "(?i)" + pattern_str;
    }
    if (dot_match_newline) {
        RE2::GlobalReplace(&pattern_str, re2::RE2("\\."), "[\\\\.\\\\s\\\\S]");
    }
    re2::RE2 pattern(pattern_str);
    if (!pattern.ok()) {
        return ExprValue::Null();
    }

    std::string expr = input[0].get_string();
    std::string repl = input[2].get_string();

    // 数字前的奇数个\变成偶数个\ (mysql的行为看起来是这样的 偶数个反斜杠是\的转义，\1等于1)
    std::string new_repl = repl;
    re2::StringPiece match;
    re2::StringPiece repl_view(repl);
    std::string::size_type offset = 0;
    // 前面全是\且不以$或则\结尾的串
    while (RE2::FindAndConsume(&repl_view, "((\\\\)*[^$\\\\])", &match)) {
        std::size_t match_length = match.length();
        if (match_length % 2 == 0) {
            auto position = repl.size() - repl_view.size();
            new_repl.erase(position - offset - 2, 1);
            offset += 1;
        }
    }
    repl = new_repl;

    repl_view = repl;
    new_repl = repl;
    // 前面全是\且以$结尾的串
    while (RE2::FindAndConsume(&repl_view, "((\\\\)*\\$)", &match)) {
        std::size_t match_length = match.length();
        auto position = repl.size() - repl_view.size();
        if (match_length % 2 == 0) {
            // 转义的$
            new_repl.erase(position - offset - 2, 1);
            offset += 1;
        } else {
            // 占位符，需要替换为 \; 此时后面应当是数字
            new_repl[position - offset - 1] = '\\';
            if (new_repl.size() < position - offset + 1
                    || new_repl[position - offset] < '0'
                    || new_repl[position - offset] > '9') {
                return ExprValue::Null();
            }
        }
    }
    repl = new_repl;

    std::string match_str = expr.substr(pos - 1, expr.size() - pos + 1);
    if (is_multi_line_mode) {
        std::vector<std::string> lines;
        boost::split(lines, match_str, boost::is_any_of("\n"));
        std::string new_match_str;
        if (occurrence == 0) {
            for (auto& line: lines) {
                RE2::GlobalReplace(&line, pattern, repl);
                new_match_str += (line + '\n');
            }
        } else {
            int meet = 0;
            for (auto& line: lines) {
                if (meet < occurrence) {
                    int count = ReplaceNthMatch(line, pattern, repl, occurrence - meet);
                    meet += count;
                }
                new_match_str += (line + '\n');
            }
        }
        // 删除尾部多余的\n
        match_str = new_match_str.substr(0, new_match_str.size() - 1);
    } else {
        if (occurrence == 0) {
            RE2::GlobalReplace(&match_str, pattern, repl);
        } else {
            ReplaceNthMatch(match_str, pattern, repl, occurrence);
        }
    }
    return ExprValue(pb::PrimitiveType::STRING, expr.substr(0, pos - 1) + match_str);
}

ExprValue substring_index(const std::vector<ExprValue>& input) {
    if (input.size() != 3) {
        return ExprValue::Null();
    }

    for (auto s : input) {
        if (s.is_null()) {
            return ExprValue::Null();
        }
    }

    int pos = input[2].get_numberic<int>();
    ExprValue tmp(pb::STRING);
    if (pos == 0) {
        return tmp;
    }
    std::vector<size_t> pos_vec;
    pos_vec.reserve(3);
    size_t last_pos = 0;
    const std::string& str = input[0].get_string();
    const std::string& sub = input[1].get_string();
    if (sub.size() == 0) {
        return tmp;
    }
    while (true) {
        size_t find_pos = str.find(sub, last_pos);
        if (find_pos != std::string::npos) {
            pos_vec.emplace_back(find_pos);
            last_pos = find_pos + sub.size();
        } else {
            break;
        }
    }
    if (pos > 0) {
        if (pos <= pos_vec.size()) {
            tmp.str_val = str.substr(0, pos_vec[pos - 1]);
        } else {
            tmp.str_val = str;
        }
    } else {
        pos = -pos;
        if (pos <= pos_vec.size()) {
            pos = pos_vec.size() - pos;
            tmp.str_val = str.substr(pos_vec[pos] + sub.size());
        } else {
            tmp.str_val = str;
        }
    }

    return tmp;
}

ExprValue unix_timestamp(const std::vector<ExprValue>& input) {
    ExprValue tmp(pb::UINT32);
    if (input.size() == 0) {
        tmp._u.uint32_val = time(NULL);
    } else {
        if (input[0].is_null()) {
            return ExprValue::Null();
        }
        ExprValue in = input[0];
        if (in.type == pb::INT64) {
            in.cast_to(pb::STRING);
        }
        tmp._u.uint32_val = in.cast_to(pb::TIMESTAMP)._u.uint32_val;
    }
    return tmp;
}

ExprValue from_unixtime(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::TIMESTAMP);
    tmp._u.uint32_val = input[0].get_numberic<uint32_t>();
    return tmp;
}

ExprValue now(const std::vector<ExprValue>& input) {
    if (input.size() > 0) {
        return ExprValue::Now(input[0].get_numberic<int>());
    }
    return ExprValue::Now(0);
}

ExprValue current_timestamp(const std::vector<ExprValue>& input) {
    if (input.size() > 0) {
        return ExprValue::Now(input[0].get_numberic<int>());
    }
    return ExprValue::Now(0);
}

ExprValue utc_timestamp(const std::vector<ExprValue>& input) {
    if (input.size() > 0) {
        return ExprValue::UTC_TIMESTAMP(input[0].get_numberic<int>());
    }
    return ExprValue::UTC_TIMESTAMP(0);
}

ExprValue utc_date(const std::vector<ExprValue>& input) {
    return ExprValue::UTC_TIMESTAMP(0).cast_to(pb::DATE);
}

ExprValue utc_time(const std::vector<ExprValue>& input) {
    return ExprValue::UTC_TIMESTAMP(0).cast_to(pb::TIME);
}

ExprValue period_add(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 2) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    tmp.cast_to(pb::INT64);
    int year = tmp._u.int64_val / 100;
    int month = tmp._u.int64_val % 100;
    ExprValue num = input[1];
    num.cast_to(pb::INT32);
    month += num._u.int32_val + year * 12 - 1;
    year = month / 12;
    month = month % 12 + 1;
    char buf[30] = {0};
    snprintf(buf, sizeof(buf), "%d%02d", year, month);
    ExprValue res(pb::STRING);
    res.str_val = buf;
    return res;
}

ExprValue period_diff(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 2) {
        return ExprValue::Null();
    }
    ExprValue dt1 = input[0];
    dt1.cast_to(pb::INT64);

    ExprValue dt2 = input[0];
    dt2.cast_to(pb::INT64);

    int month1 = dt1._u.int64_val / 100 * 12 + dt1._u.int64_val % 100;
    int month2 = dt2._u.int64_val / 100 * 12 + dt2._u.int64_val % 100;
    int diff = month1 - month2;
    ExprValue res(pb::INT32);
    res._u.int32_val = diff;
    return res;
}

ExprValue minute(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 1) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    tmp.cast_to(pb::TIME);
    ExprValue res(pb::INT32);
    res._u.int32_val = (tmp._u.int32_val >> 6 ) & 0x3F;
    return res;
}

ExprValue func_time(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 1) {
        return ExprValue::Null();
    }
    ExprValue res = input[0];
    res.cast_to(pb::TIME);
    return res;
}

ExprValue func_quarter(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 1) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    tmp.cast_to(pb::DATE);
    int year_month = ((tmp._u.int32_val >> 5) & 0x1FFFF);
    int month = year_month % 13;
    int quarter = (month - 1) / 3;
    ExprValue res(pb::INT32);
    res._u.int32_val = quarter + 1;
    return res;
}

ExprValue microsecond(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 1) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    tmp.cast_to(pb::STRING);
    int idx = 0;
    while (idx < tmp.str_val.length()) {
        if (tmp.str_val[idx] == '.') {
            break;
        }
        idx ++;
    }
    idx ++;
    int micro = 0;
    int cnt = 0;
    while (idx < tmp.str_val.length()) {
        if (tmp.str_val[idx] >= '0' && tmp.str_val[idx] <= '9') {
            micro = micro * 10 + tmp.str_val[idx] - '0';
            cnt ++;
            idx ++;
            continue;
        }
        break;
    }
    if (cnt == 0 || cnt > 6) {
        micro = 0;
    }
    ExprValue res(pb::INT32) ;
    res._u.int32_val = micro;
    return res;
}

ExprValue second(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 1) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    tmp.cast_to(pb::TIME);
    ExprValue res(pb::INT32);
    res._u.int32_val = tmp._u.int32_val & 0x3F;
    return res;
}

ExprValue timestampadd(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 3) {
        return ExprValue::Null();
    }
    ExprValue ret = input[2];
    ret.cast_to(pb::TIMESTAMP);
    ExprValue num = input[1];
    num.cast_to(pb::INT32);
    if (input[0].str_val == "second") {
        ret._u.uint32_val += num._u.int32_val;
    } else if (input[0].str_val == "minute") {
        ret._u.uint32_val += num._u.int32_val * 60;
    } else if (input[0].str_val == "hour") {
        ret._u.uint32_val += num._u.int32_val * 3600;
    } else if (input[0].str_val == "day") {
        ret._u.uint32_val += num._u.int32_val * 24 * 3600;
    } else {
        return ExprValue::Null();
    }
    return ret;
}

ExprValue timestamp(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() == 0 || input.size() > 2) {
        return ExprValue::Null();
    }
    ExprValue arg1 = input[0];
    ExprValue ret = arg1.cast_to(pb::DATETIME).cast_to(pb::TIMESTAMP);
    if (input.size() == 2) {
        ret._u.uint32_val += time_to_sec(std::vector<ExprValue>(input.begin() + 1, input.end()))._u.int32_val;
    }
    return ret;
}

ExprValue date_format(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 2) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    time_t t = tmp.cast_to(pb::TIMESTAMP)._u.uint32_val;
    struct tm t_result;
    localtime_r(&t, &t_result);
    char s[DATE_FORMAT_LENGTH];
    date_format_internal(s, sizeof(s), input[1].str_val.c_str(), &t_result);
    ExprValue format_result(pb::STRING);
    format_result.str_val = s;
    return format_result;
}
ExprValue str_to_date(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 2) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    return tmp.cast_to(pb::DATETIME);
}

ExprValue time_format(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 2) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    struct tm t_result;
    uint32_t second = tmp.cast_to(pb::TIME)._u.int32_val;

    t_result.tm_hour = (second >> 12) & 0x3FF;
    t_result.tm_min = (second >> 6) & 0x3F;
    t_result.tm_sec = second & 0x3F;
    char s[DATE_FORMAT_LENGTH];
    date_format_internal(s, sizeof(s), input[1].str_val.c_str(), &t_result);
    ExprValue format_result(pb::STRING);
    format_result.str_val = s;
    return format_result;
}

ExprValue to_days(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 1) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    tmp.cast_to(pb::TIMESTAMP);
    tmp.cast_to(pb::INT64);
    tmp._u.int64_val = (tmp._u.int64_val + 62167248000) / 86400;
    return tmp;
}

ExprValue to_seconds(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 1) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    tmp.cast_to(pb::TIMESTAMP);
    tmp.cast_to(pb::INT64);
    tmp._u.int64_val += 62167248000;
    return tmp;
}


ExprValue convert_tz(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 3){
        return ExprValue::Null();
    }
    ExprValue time = input[0];
    ExprValue from_tz = input[1];
    ExprValue to_tz = input[2];
    int from_tz_second = 0;
    int to_tz_second = 0;
    if (!tz_to_second(from_tz.str_val.c_str(), from_tz_second)) {
        return ExprValue::Null();
    }
    if (!tz_to_second(to_tz.str_val.c_str(), to_tz_second)) {
        return ExprValue::Null();
    }
    int second_diff = to_tz_second - from_tz_second;
    ExprValue ret = time.cast_to(pb::TIMESTAMP);
    ret._u.uint32_val += second_diff;
    return ret.cast_to(pb::DATETIME);
}

ExprValue timediff(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() < 2) {
        return ExprValue::Null();
    }
    ExprValue arg1 = input[0];
    ExprValue arg2 = input[1];
    int32_t seconds = arg1.cast_to(pb::TIMESTAMP)._u.uint32_val - 
        arg2.cast_to(pb::TIMESTAMP)._u.uint32_val;
    ExprValue ret(pb::TIME);
    ret._u.int32_val = seconds_to_time(seconds);
    return ret;
}
ExprValue timestampdiff(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() < 3) {
        return ExprValue::Null();
    }
    ExprValue arg2 = input[1];
    ExprValue arg3 = input[2];
    int32_t seconds = arg3.cast_to(pb::TIMESTAMP)._u.uint32_val - 
        arg2.cast_to(pb::TIMESTAMP)._u.uint32_val;
    ExprValue ret(pb::INT64);
    if (input[0].str_val == "second") {
        ret._u.int64_val = seconds;
    } else if (input[0].str_val == "minute") {
        ret._u.int64_val = seconds / 60;
    } else if (input[0].str_val == "hour") {
        ret._u.int64_val = seconds / 3600;
    } else if (input[0].str_val == "day") {
        ret._u.int64_val = seconds / (24 * 3600);
    } else {
        // un-support
        return ExprValue::Null();
    }
    return ret;
}

ExprValue curdate(const std::vector<ExprValue>& input) {
    ExprValue tmp(pb::DATE);
    uint64_t datetime = timestamp_to_datetime(time(NULL));
    tmp._u.uint32_val = (datetime >> 41) & 0x3FFFFF;
    return tmp;
}
ExprValue current_date(const std::vector<ExprValue>& input) {
    return curdate(input);
}
ExprValue curtime(const std::vector<ExprValue>& input) {
    ExprValue tmp(pb::TIME);
    uint64_t datetime = timestamp_to_datetime(time(NULL));
    tmp._u.int32_val = datetime_to_time(datetime);
    return tmp;
}
ExprValue current_time(const std::vector<ExprValue>& input) {
    return curtime(input);
}
ExprValue date(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue in = input[0];
    if (in.type == pb::INT64) {
        in.cast_to(pb::STRING);
    }
    ExprValue tmp(pb::DATE);
    uint64_t dt = in.cast_to(pb::DATETIME)._u.uint64_val;
    tmp._u.uint32_val = datetime_to_date(dt);
    return tmp;
}
ExprValue hour(const std::vector<ExprValue>& input) {
    // Mysql最大值为838，BaikalDB最大值为1023
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue in = input[0];
    time_t t = in.cast_to(pb::TIME)._u.int32_val;
    if (t < 0) {
        t = -t;
    }
    ExprValue tmp(pb::UINT32);
    tmp._u.uint32_val = (t >> 12) & 0x3FF;
    return tmp;
}
ExprValue day(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue in = input[0];
    if (in.type == pb::INT64) {
        in.cast_to(pb::STRING);
    }
    ExprValue tmp(pb::UINT32);
    uint64_t dt = in.cast_to(pb::DATETIME)._u.uint64_val;
    tmp._u.uint32_val = datetime_to_day(dt);
    return tmp;
}
ExprValue dayname(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::STRING);
    uint32_t week_num = dayofweek(input)._u.uint32_val;
    if (week_num <= day_names.size() && week_num > 0) {
        tmp.str_val = day_names[week_num - 1];
    } else {
        return ExprValue::Null();
    }
    return tmp;
}
ExprValue monthname(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    uint32_t month_num = month(input)._u.uint32_val;
    ExprValue tmp(pb::STRING);
    if (month_num <= month_names.size() && month_num > 0) {
        tmp.str_val = month_names[month_num - 1];
    } else {
        return ExprValue::Null();
    }
    return tmp;
}
ExprValue dayofweek(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue in = input[0];
    if (in.type == pb::INT64) {
        in.cast_to(pb::STRING);
    }
    uint64_t dt = in.cast_to(pb::DATETIME)._u.uint64_val;
    uint32_t year = datetime_to_year(dt);
    uint32_t month = datetime_to_month(dt);
    uint32_t day = datetime_to_day(dt);
    if (month == 0 || day == 0) {
        return ExprValue::Null();
    }
    try {
        boost::gregorian::date today(year, month, day);
        ExprValue tmp(pb::UINT32);
        /*
          DAYOFWEEK(d) 函数返回 d 对应的一周中的索引（位置）。1 表示周日，2 表示周一，……，7 表示周六
        */ 
        tmp._u.uint32_val = today.day_of_week() + 1;
        return tmp;
    } catch (std::exception& e) {
        DB_DEBUG("date error:%s", e.what());
        return ExprValue::Null();
    }
}
ExprValue dayofmonth(const std::vector<ExprValue>& input) {
    return day(input);
}
ExprValue month(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue in = input[0];
    if (in.type == pb::INT64) {
        in.cast_to(pb::STRING);
    }
    ExprValue tmp(pb::UINT32);
    uint64_t dt = in.cast_to(pb::DATETIME)._u.uint64_val;
    tmp._u.uint32_val = datetime_to_month(dt);
    return tmp;
}
ExprValue year(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue in = input[0];
    if (in.type == pb::INT64) {
        in.cast_to(pb::STRING);
    }
    ExprValue tmp(pb::UINT32);
    uint64_t dt = in.cast_to(pb::DATETIME)._u.uint64_val;
    tmp._u.uint32_val = datetime_to_year(dt);
    return tmp;    
}
ExprValue time_to_sec(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::INT32);
    ExprValue in = input[0];
    time_t time = in.cast_to(pb::TIME)._u.int32_val;
    bool minus = false;
    if (time < 0) {
        minus = true;
        time = -time;
    }
    uint32_t hour = (time >> 12) & 0x3FF;
    uint32_t min = (time >> 6) & 0x3F;
    uint32_t sec = time & 0x3F;
    uint32_t sec_sum = hour * 3600 + min * 60 + sec;
    if (!minus) {
        tmp._u.int32_val = sec_sum;
    } else {
        tmp._u.int32_val = -sec_sum;
    }
    return tmp;
}
ExprValue sec_to_time(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue in = input[0];
    if (in.type == pb::STRING) {
        in.cast_to(pb::INT32);
    }
    int32_t secs = in._u.int32_val;
    bool minus = false; 
    if (secs < 0) {
        minus = true;
        secs = - secs;
    }
    ExprValue tmp(pb::TIME);
    
    uint32_t hour = secs / 3600;
    uint32_t min = (secs - hour * 3600) / 60;
    uint32_t sec = secs % 60;
    int32_t time = 0;
    time |= sec;
    time |= (min << 6);
    time |= (hour << 12);
    if (minus) {
        time = -time;
    }
    tmp._u.int32_val = time;
    return tmp;
}
ExprValue dayofyear(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue in = input[0];
    if (in.type == pb::INT64) {
        in.cast_to(pb::STRING);
    }
    uint64_t dt = in.cast_to(pb::DATETIME)._u.uint64_val;
    uint32_t year = datetime_to_year(dt);
    uint32_t month = datetime_to_month(dt);
    uint32_t day = datetime_to_day(dt);
    if (month == 0 || day == 0) {
        return ExprValue::Null();
    }
    try {
        boost::gregorian::date today(year, month, day);
        ExprValue tmp(pb::UINT32);
        tmp._u.uint32_val = today.day_of_year();
        return tmp;
    } catch (std::exception& e) {
        DB_DEBUG("date error:%s", e.what());
        return ExprValue::Null();
    }
}
ExprValue weekday(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue in = input[0];
    if (in.type == pb::INT64) {
        in.cast_to(pb::STRING);
    }
    uint64_t dt = in.cast_to(pb::DATETIME)._u.uint64_val;
    uint32_t year = datetime_to_year(dt);
    uint32_t month = datetime_to_month(dt);
    uint32_t day = datetime_to_day(dt);
    if (month == 0 || day == 0) {
        return ExprValue::Null();
    }
    try {
        boost::gregorian::date today(year, month, day);
        ExprValue tmp(pb::UINT32);
        uint32_t day_of_week = today.day_of_week();
        if (day_of_week >= 1) {
            tmp._u.uint32_val = day_of_week - 1;
        } else {
            tmp._u.uint32_val = 6;
        }
        return tmp;
    } catch (std::exception& e) {
        DB_DEBUG("date error:%s", e.what());
        return ExprValue::Null();
    }
}
inline void calc_mode(uint32_t mode, bool& is_monday_first, bool& zero_range, bool& with_4_days) {
    switch (mode) {
        case 0:
            is_monday_first = false;
            zero_range = true;
            with_4_days = false;
            break;
        case 1:
            is_monday_first = true;
            zero_range = true;
            with_4_days = true;
            break;
        case 2:
            is_monday_first = false;
            zero_range = false;
            with_4_days = false;
            break;
        case 3:
            is_monday_first = true;
            zero_range = false;
            with_4_days = true;
            break;
        case 4:
            is_monday_first = false;
            zero_range = true;
            with_4_days = true;
            break;
        case 5:
            is_monday_first = true;
            zero_range = true;
            with_4_days = false;
            break;
        case 6:
            is_monday_first = false;
            zero_range = false;
            with_4_days = true;
            break;
        case 7:
            is_monday_first = true;
            zero_range = false;
            with_4_days = false;
            break;
        default:
            break;
    }
}

inline int days_in_year(int year) {
    if (year % 4 == 0 && (year % 100 != 0 || year%400 == 0)) {
        return 366;
    }
    return 365;
}
int calc_week(const uint64_t dt, int32_t mode, bool is_yearweek, int& year, int& weeks) {
    year = datetime_to_year(dt);
    int32_t month = datetime_to_month(dt);
    int32_t day = datetime_to_day(dt);
    if (month == 0 || day == 0) {
        return -1;
    }
    boost::gregorian::date first_day;
    boost::gregorian::date today;
    try {
        first_day = boost::gregorian::date(year, 1, 1);
        today = boost::gregorian::date(year, month, day);
    } catch (std::exception& e) {
        DB_DEBUG("date error:%s", e.what());
        return -1;
    }
    int day_of_year = today.day_of_year();
    // 第一天是星期几
    int first_weekday = first_day.day_of_week();
    bool is_monday_first = false;
    // 0-53 or 1-53
    bool zero_range = true;
    bool with_4_days = false;
    if (mode >= 0) {
        calc_mode(mode, is_monday_first, zero_range, with_4_days); 
    }
    if (is_monday_first) {
        if (first_weekday == 0) {
            first_weekday = 6;
        } else {
            first_weekday--;
        }
    }
    if (is_yearweek) {
        zero_range = false;
    }
    int contain_days = 7 - first_weekday;
    if (month == 1 && day <= contain_days) {
        if ((with_4_days && contain_days < 4) || (!with_4_days && first_weekday != 0)) {
            if (zero_range) {
                weeks = 0;
                return 0;
            } else {
                // 第一天非第一周则放到上一年最后一周
                year--;
                day_of_year += days_in_year(year);
                first_weekday = (first_weekday + 53 * 7 - days_in_year(year)) % 7;
                contain_days = 7 - first_weekday;
            }
        }
    }
    int days = 0;
    if ((with_4_days && contain_days < 4) || (!with_4_days && first_weekday != 0)) {
        days = day_of_year - contain_days - 1;
    } else {
        days = day_of_year - contain_days - 1 + 7;
    }
    weeks = days / 7 + 1;
    if (!zero_range && days >= 52 * 7) {
        // 下一年第一天是星期几
        first_weekday = (first_weekday + days_in_year(year)) % 7;
        if ((with_4_days && contain_days >=4) || (!with_4_days && first_weekday == 0)) {
            year++;
            weeks = 1;
            return 0;
        }
    }
    return 0;
}
ExprValue yearweek(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    int year = 0;
    int weeks = 0;
    ExprValue one = input[0];
    uint64_t dt = one.cast_to(pb::DATETIME)._u.uint64_val;

    int32_t mode = -1;
    if (input.size() > 1) {
        mode = input[1].get_numberic<uint32_t>() % 8;
    }

    int ret = calc_week(dt, mode, true, year, weeks);
    if (ret != 0) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::UINT32);
    tmp._u.uint32_val = year * 100 + weeks;
    return tmp;
}
ExprValue week(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    int year = 0;
    int weeks = 0;
    ExprValue one = input[0];
    uint64_t dt = one.cast_to(pb::DATETIME)._u.uint64_val;

    int32_t mode = -1;
    if (input.size() > 1) {
        mode = input[1].get_numberic<uint32_t>() % 8;
    }

    int ret = calc_week(dt, mode, false, year, weeks);
    if (ret != 0) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::UINT32);
    tmp._u.uint32_val = weeks;
    return tmp;
}
ExprValue weekofyear(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue one = input[0];
    uint64_t dt = one.cast_to(pb::DATETIME)._u.uint64_val;
    uint32_t year = datetime_to_year(dt);
    uint32_t month = datetime_to_month(dt);
    uint32_t day = datetime_to_day(dt);
    if (month == 0 || day == 0) {
        return ExprValue::Null();
    }
    try {
        boost::gregorian::date today(year, month, day);
        uint32_t week_number = today.week_number();
        ExprValue tmp(pb::UINT32);
        tmp._u.uint32_val = week_number;
        return tmp;
    } catch (std::exception& e) {
        DB_DEBUG("date error:%s", e.what());
        return ExprValue::Null();
    }
}

ExprValue datediff(const std::vector<ExprValue>& input) {
    if (input.size() != 2 || input[0].is_null() || input[1].is_null()) {
        return ExprValue::Null();
    }
    ExprValue left = input[0];
    if (left.type == pb::INT64) {
        left.cast_to(pb::STRING);
    }
    ExprValue right = input[1];
    if (right.type == pb::INT64) {
        right.cast_to(pb::STRING);
    }
    left.cast_to(pb::DATE);
    right.cast_to(pb::DATE);
    int64_t t1 = left.cast_to(pb::TIMESTAMP)._u.uint32_val;
    int64_t t2 = right.cast_to(pb::TIMESTAMP)._u.uint32_val;
    ExprValue tmp(pb::INT32);
    tmp._u.int32_val = (t1 - t2) / (3600 * 24);
    return tmp;
}

ExprValue date_add(const std::vector<ExprValue>& input) {
    if (input.size() < 3) {
            return ExprValue::Null();
    }
    for (auto& s : input) {
        if (s.is_null()) {
            return ExprValue::Null();
        }
    }
    ExprValue arg1 = input[0];
    ExprValue arg2 = input[1];
    int32_t interval = arg2.cast_to(pb::INT32)._u.int32_val;
    ExprValue ret;
    if (!arg1.is_numberic()) {
        ret = arg1.cast_to(pb::TIMESTAMP);
    } else {
        ret = arg1.cast_to(pb::STRING).cast_to(pb::TIMESTAMP);
    }
    time_t ts = ret._u.uint32_val;
    if (input[2].str_val == "second") {
        date_add_interval(ts, interval, TimeUnit::SECOND);
    } else if (input[2].str_val == "minute") {
        date_add_interval(ts, interval, TimeUnit::MINUTE);
    } else if (input[2].str_val == "hour") {
        date_add_interval(ts, interval, TimeUnit::HOUR);
    } else if (input[2].str_val == "day") {
        date_add_interval(ts, interval, TimeUnit::DAY);
    } else if (input[2].str_val == "month") {
        date_add_interval(ts, interval, TimeUnit::MONTH);
    } else if (input[2].str_val == "year") {
        date_add_interval(ts, interval, TimeUnit::YEAR);
    } else {
        // un-support
        return ExprValue::Null();
    }
    ret._u.uint32_val = ts;
    return ret;
}

ExprValue date_sub(const std::vector<ExprValue>& input) {
    if (input.size() < 3) {
            return ExprValue::Null();
    }
    for (auto& s : input) {
        if (s.is_null()) {
            return ExprValue::Null();
        }
    }
    ExprValue arg1 = input[0];
    ExprValue arg2 = input[1];
    int32_t interval = arg2.cast_to(pb::INT32)._u.int32_val;
    ExprValue ret;
    if (!arg1.is_numberic()) {
        ret = arg1.cast_to(pb::TIMESTAMP);
    } else {
        ret = arg1.cast_to(pb::STRING).cast_to(pb::TIMESTAMP);
    }
    time_t ts = ret._u.uint32_val;
    if (input[2].str_val == "second") {
        date_sub_interval(ts, interval, TimeUnit::SECOND);
    } else if (input[2].str_val == "minute") {
        date_sub_interval(ts, interval, TimeUnit::MINUTE);
    } else if (input[2].str_val == "hour") {
        date_sub_interval(ts, interval, TimeUnit::HOUR);
    } else if (input[2].str_val == "day") {
        date_sub_interval(ts, interval, TimeUnit::DAY);
    } else if (input[2].str_val == "month") {
        date_sub_interval(ts, interval, TimeUnit::MONTH);
    } else if (input[2].str_val == "year") {
        date_sub_interval(ts, interval, TimeUnit::YEAR);
    } else {
        // un-support
        return ExprValue::Null();
    }
    ret._u.uint32_val = ts;
    return ret;
}

ExprValue addtime(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 2) {
        return ExprValue::Null();
    }
    ExprValue arg2 = input[1];
    if (arg2.cast_to(pb::TIME)._u.int32_val == 0) {
        return input[0];
    }
    ExprValue arg1 = input[0];
    int seconds = time_to_seconds(arg2._u.int32_val);
    if (arg1.cast_to(pb::DATETIME)._u.int64_val != 0) {
        arg1.cast_to(pb::TIMESTAMP);
        arg1._u.int32_val += seconds;
        return arg1.cast_to(pb::STRING);
    } else {
        arg1 = input[0];
        if (arg1.cast_to(pb::TIME)._u.int32_val != 0) {
            arg1._u.int32_val = seconds_to_time(time_to_seconds(arg1._u.int32_val) + seconds);
            return arg1.cast_to(pb::STRING);
        }
    }
    return ExprValue::Null();
}

ExprValue subtime(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 2) {
        return ExprValue::Null();
    }
    ExprValue arg2 = input[1];
    if (arg2.cast_to(pb::TIME)._u.int32_val == 0) {
        return input[0];
    }
    ExprValue arg1 = input[0];
    int seconds = time_to_seconds(arg2._u.int32_val);
    if (arg1.cast_to(pb::DATETIME)._u.int64_val != 0) {
        arg1.cast_to(pb::TIMESTAMP);
        arg1._u.int32_val -= seconds;
        return arg1.cast_to(pb::STRING);
    } else {
        arg1 = input[0];
        if (arg1.cast_to(pb::TIME)._u.int32_val != 0) {
            arg1._u.int32_val = seconds_to_time(time_to_seconds(arg1._u.int32_val) - seconds);
            return arg1.cast_to(pb::STRING);
        }
    }
    return ExprValue::Null();
}

ExprValue extract(const std::vector<ExprValue>& input) {
    if (input.size() != 2 || input[0].is_null() || input[1].is_null()) {
        return ExprValue::Null();
    }
    ExprValue arg1 = input[0];
    ExprValue arg2 = input[1];
    ExprValue tmp(pb::UINT32);
    time_t t = arg2.cast_to(pb::TIMESTAMP)._u.uint32_val;
    struct tm tm;
    localtime_r(&t, &tm);
    if (input[0].str_val == "year") {
        tmp._u.uint32_val = tm.tm_year + 1900;
    } else if (input[0].str_val == "month") {
        tmp._u.uint32_val = ++tm.tm_mon;
    } else if (input[0].str_val == "day") {
        tmp._u.uint32_val = tm.tm_mday;
    } else if (input[0].str_val == "hour") {
        tmp._u.uint32_val = tm.tm_hour;
    } else if (input[0].str_val == "minute") {
        tmp._u.uint32_val = tm.tm_min;
    } else if (input[0].str_val == "second") {
        tmp._u.uint32_val = tm.tm_sec;
    } else {
        return ExprValue::Null();
    }
    return tmp;
}

ExprValue tso_to_timestamp(const std::vector<ExprValue>& input) {
    if (input.size() != 1 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue arg1 = input[0];
    ExprValue tmp(pb::TIMESTAMP);
    tmp._u.uint32_val = tso::get_timestamp_internal(arg1._u.int64_val);
    return tmp;
}

ExprValue timestamp_to_tso(const std::vector<ExprValue>& input) {
    if (input.size() != 1 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue arg_timestamp= input[0];
    ExprValue tmp(pb::INT64);
    arg_timestamp.cast_to(pb::TIMESTAMP);
    tmp._u.int64_val = timestamp_to_ts(arg_timestamp._u.uint32_val);
    return tmp;
}

ExprValue hll_add(const std::vector<ExprValue>& input) {
    if (input.size() == 0) {
        return ExprValue::Null();
    }
    if (input[0].is_null()) {
        (ExprValue&)input[0] = hll::hll_init();
    } else if (!input[0].is_hll()) {
        return ExprValue::Null();
    }
    for (size_t i = 1; i < input.size(); i++) {
        if (!input[i].is_null()) {
            hll::hll_add((ExprValue&)input[0], input[i].hash());
        }
    }
    return input[0];
}

ExprValue hll_init(const std::vector<ExprValue>& input) {
    if (input.size() == 0) {
        return ExprValue::Null();
    }
    ExprValue hll_init = hll::hll_init();
    for (size_t i = 0; i < input.size(); i++) {
        if (!input[i].is_null()) {
            hll::hll_add(hll_init, input[i].hash());
        }
    }
    return hll_init;
}

ExprValue hll_merge(const std::vector<ExprValue>& input) {
    if (input.size() == 0) {
        return ExprValue::Null();
    }
    if (!input[0].is_hll()) {
        (ExprValue&)input[0] = hll::hll_init();
    }
    for (size_t i = 1; i < input.size(); i++) {
        if (input[i].is_hll()) {
            hll::hll_merge((ExprValue&)input[0], (ExprValue&)input[i]);
        }
    }
    return input[0];
}

ExprValue hll_estimate(const std::vector<ExprValue>& input) {
    if (input.size() == 0) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::INT64);
    if (input[0].is_null()) {
        tmp._u.int64_val = 0;
        return tmp;
    }
    tmp._u.int64_val = hll::hll_estimate(input[0]);
    return tmp;
}

ExprValue case_when(const std::vector<ExprValue>& input) {
    for (size_t i = 0; i < input.size() / 2; ++i) {
        auto if_index = i * 2;
        auto then_index = i * 2 + 1;
        if (input[if_index].get_numberic<bool>()) {
            return input[then_index];
        }
    }
    //没有else分支, 返回null
    if (input.size() % 2 == 0) {
        return ExprValue();
     } else {
        return input[input.size() - 1];
    }
}

ExprValue case_expr_when(const std::vector<ExprValue>& input) {
    for (size_t i = 0; i < (input.size() - 1) / 2; ++i) {
        auto if_index = i * 2 + 1;
        auto then_index = i * 2 + 2;
        if (const_cast<ExprValue&>(input[0]).compare_diff_type(const_cast<ExprValue&>(input[if_index])) == 0) {
            return input[then_index];
        }
    }
    //没有else分支, 返回null
    if ((input.size() - 1) % 2 == 0) {
        return ExprValue();
     } else {
        return input[input.size() - 1];
    }
}

ExprValue if_(const std::vector<ExprValue>& input) {
    if (input.size() != 3) {
        return ExprValue::Null();
    }
    return input[0].get_numberic<bool>() ? input[1] : input[2];
}

ExprValue ifnull(const std::vector<ExprValue>& input) {
    if (input.size() != 2) {
        return ExprValue::Null();
    }
    return input[0].is_null() ? input[1] : input[0];
}

ExprValue isnull(const std::vector<ExprValue>& input) {
    if (input.size() != 1) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::BOOL);
    if (input[0].is_null()) {
        tmp._u.bool_val = true;
    } else {
        tmp._u.bool_val = false;
    }
   return tmp;
}

ExprValue nullif(const std::vector<ExprValue>& input) {
    if (input.size() != 2) {
        return ExprValue::Null();
    }
    ExprValue arg1 = input[0];
    ExprValue arg2 = input[1];
    if (arg1.compare_diff_type(arg2) == 0) {
        return ExprValue::Null();
    } else {
        return input[0];
    }
}
ExprValue murmur_hash(const std::vector<ExprValue>& input) {
    if (input.size() == 0) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::UINT64);
    if (input.size() == 0) {
        tmp._u.uint64_val = 0;
    } else {
        tmp._u.uint64_val = make_sign(input[0].str_val);
    }
    return tmp;
}

ExprValue md5(const std::vector<ExprValue>& input) {
    if (input.size() == 0) {
        return ExprValue::Null();
    }
    ExprValue orig = input[0];
    if (orig.type != pb::STRING) {
        orig.cast_to(pb::STRING);
    }
    ExprValue tmp(pb::STRING);
    unsigned char md5_str[16] = {0};
    MD5_CTX ctx;
    MD5_Init(&ctx);
    MD5_Update(&ctx, orig.str_val.c_str(), orig.str_val.size());
    MD5_Final(md5_str, &ctx);
    tmp.str_val.resize(32);

    int j = 0;
    static char const zEncode[] = "0123456789abcdef";
    for (uint32_t i = 0; i < 16; i ++) {
        int a = md5_str[i];
        tmp.str_val[j++] = zEncode[(a >> 4) & 0xf];
        tmp.str_val[j++] = zEncode[a & 0xf];
    }
    return tmp;
}

ExprValue sha1(const std::vector<ExprValue>& input) {
    if (input.size() == 0) {
        return ExprValue::Null();
    }
    ExprValue orig = input[0];
    if (orig.type != pb::STRING) {
        orig.cast_to(pb::STRING);
    }
    ExprValue tmp(pb::STRING);
    tmp.str_val = butil::SHA1HashString(orig.str_val);
    return tmp;
}

ExprValue sha(const std::vector<ExprValue>& input) {
    return sha1(input);
}

ExprValue rb_build(const std::vector<ExprValue>& input) {
    ExprValue tmp(pb::BITMAP);
    for (auto& val : input) {
        tmp._u.bitmap->add(val.get_numberic<uint32_t>());
    }
    return tmp;
}

ExprValue rb_and(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || !input[0].is_bitmap()) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    for (uint32_t i = 1; i < input.size(); i++) {
        if (input[i].is_bitmap()) {
            *tmp._u.bitmap &= *input[i]._u.bitmap;
        }
    }
    return tmp;
}
/*
ExprValue rb_and_cardinality(const std::vector<ExprValue>& input) {
    if (input.size() <= 1) {
        return ExprValue::Null();
    }
    ExprValue bits = rb_and(input);
    ExprValue tmp(pb::UINT64);
    if (bits._u.bitmap != nullptr) {
        tmp._u.uint64_val = bits.bitmap.cardinality();
    }
    return tmp;
}
*/

ExprValue rb_or(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || !input[0].is_bitmap()) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    for (uint32_t i = 1; i < input.size(); i++) {
        if (input[i].is_bitmap()) {
            *tmp._u.bitmap |= *input[i]._u.bitmap;
        }
    }
    return tmp;
}

/*
ExprValue rb_or_cardinality(const std::vector<ExprValue>& input) {
    if (input.size() <= 1) {
        return ExprValue::Null();
    }
    ExprValue bits = rb_or(input);
    ExprValue tmp(pb::UINT64);
    tmp._u.uint64_val = bits.bitmap.cardinality();
    return tmp;
}
*/

ExprValue rb_xor(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || !input[0].is_bitmap()) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    for (uint32_t i = 1; i < input.size(); i++) {
        if (input[i].is_bitmap()) {
            *tmp._u.bitmap ^= *input[i]._u.bitmap;
        }
    }
    return tmp;
}
/*
ExprValue rb_xor_cardinality(const std::vector<ExprValue>& input) {
    if (input.size() <= 1) {
        return ExprValue::Null();
    }
    ExprValue bits = rb_xor(input);
    ExprValue tmp(pb::UINT64);
    tmp._u.uint64_val = bits.bitmap.cardinality();
    return tmp;
}
*/

ExprValue rb_andnot(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || !input[0].is_bitmap()) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    for (uint32_t i = 1; i < input.size(); i++) {
        if (input[i].is_bitmap()) {
            *tmp._u.bitmap -= *input[i]._u.bitmap;
        }
    }
    return tmp;
}
/*
ExprValue rb_andnot_cardinality(const std::vector<ExprValue>& input) {
    if (input.size() <= 1) {
        return ExprValue::Null();
    }
    ExprValue bits = rb_andnot(input);
    ExprValue tmp(pb::UINT64);
    tmp._u.uint64_val = bits.bitmap.cardinality();
    return tmp;
}
*/

ExprValue rb_cardinality(const std::vector<ExprValue>& input) {
    if (input.size() != 1 || !input[0].is_bitmap()) {
        return ExprValue::Uint64();
    }
    ExprValue tmp(pb::UINT64);
    tmp._u.uint64_val = input[0]._u.bitmap->cardinality();
    return tmp;
}

ExprValue rb_empty(const std::vector<ExprValue>& input) {
    ExprValue tmp(pb::BOOL);
    if (input.size() != 1 || !input[0].is_bitmap()) {
        tmp._u.bool_val = false;
    } else {
        tmp._u.bool_val = input[0]._u.bitmap->isEmpty();
    }
    return tmp;
}

ExprValue rb_equals(const std::vector<ExprValue>& input) {
    ExprValue tmp(pb::BOOL);
    if (input.size() != 2 || !input[0].is_bitmap() || !input[1].is_bitmap()) {
        tmp._u.bool_val = false;
    } else {
        tmp._u.bool_val = (*input[0]._u.bitmap == *input[1]._u.bitmap);
    }
    return tmp;
}

/*
ExprValue rb_not_equals(const std::vector<ExprValue>& input) {
    ExprValue tmp = rb_equals(input);
    tmp._u.bool_val = tmp._u.bool_val ? false : true;
    return tmp;
}
*/

ExprValue rb_intersect(const std::vector<ExprValue>& input) {
    ExprValue tmp(pb::BOOL);
    if (input.size() != 2 || !input[0].is_bitmap() || !input[1].is_bitmap()) {
        tmp._u.bool_val = false;
    } else {
        tmp._u.bool_val = input[0]._u.bitmap->intersect(*input[1]._u.bitmap);
    }
    return tmp;
}

ExprValue rb_contains(const std::vector<ExprValue>& input) {
    ExprValue result(pb::BOOL);
    if (input.size() < 2 || !input[0].is_bitmap()) {
        result._u.bool_val = false;
    } else {
        for (uint32_t i = 1; i < input.size(); i++) {
            bool ret = input[0]._u.bitmap->contains(input[i].get_numberic<uint32_t>());
            result._u.bool_val = ret;
            if (!ret) {
                break;
            }
        }
    }
    return result;
}

ExprValue rb_contains_range(const std::vector<ExprValue>& input) {
    ExprValue tmp(pb::BOOL);
    if (input.size() != 3 || !input[0].is_bitmap()) {
        tmp._u.bool_val = false;
    } else {
        tmp._u.bool_val = input[0]._u.bitmap->containsRange(input[1].get_numberic<uint32_t>(), input[2].get_numberic<uint32_t>());
    }
    return tmp;    
}

ExprValue rb_add(const std::vector<ExprValue>& input) {
    if (input.size() == 0) {
        return ExprValue::Null();
    }
    if (input[0].is_null()) {
        (ExprValue&)input[0] = ExprValue::Bitmap();
    } else if (!input[0].is_bitmap()) {
        return ExprValue::Null();
    }
    ExprValue result = input[0];
    for (size_t i = 1; i < input.size(); i++) {
        result._u.bitmap->add(input[i].get_numberic<uint32_t>());
    }
    (ExprValue&)input[0] = result;
    return input[0];
}

ExprValue rb_add_range(const std::vector<ExprValue>& input) {
    if (input.size() != 3 || !input[0].is_bitmap()) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    tmp._u.bitmap->addRange(input[1].get_numberic<uint32_t>(), input[2].get_numberic<uint32_t>());
    return tmp;
}

ExprValue rb_remove(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || !input[0].is_bitmap()) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    for (uint32_t i = 1; i < input.size(); i++) {
        tmp._u.bitmap->remove(input[i].get_numberic<uint32_t>());
    }
    return tmp;
}

ExprValue rb_remove_range(const std::vector<ExprValue>& input) {
    if (input.size() != 3 || !input[0].is_bitmap()) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    uint32_t start = input[1].get_numberic<uint32_t>();
    uint32_t end = input[2].get_numberic<uint32_t>();
    for (uint32_t i = start; i < end; i++) {
        tmp._u.bitmap->remove(i);
    }
    return tmp;
}

ExprValue rb_flip_range(const std::vector<ExprValue>& input) {
    if (input.size() != 3 || !input[0].is_bitmap()) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    uint32_t start = input[1].get_numberic<uint32_t>();
    uint32_t end = input[2].get_numberic<uint32_t>();
    tmp._u.bitmap->flip(start, end);
    return tmp;    
}

ExprValue rb_flip(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || !input[0].is_bitmap()) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    for (uint32_t i = 1; i < input.size(); i++) {
        uint32_t val_tmp = input[1].get_numberic<uint32_t>();
        tmp._u.bitmap->flip(val_tmp, val_tmp + 1);
    }
    return tmp;
}

ExprValue rb_minimum(const std::vector<ExprValue>& input) {
    if (input.size() == 0) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::UINT32);
    if (input[0].is_bitmap()) {
        tmp._u.uint32_val = input[0]._u.bitmap->minimum();
    }
    return tmp;
}

ExprValue rb_maximum(const std::vector<ExprValue>& input) {
    if (input.size() == 0) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::UINT32);
    if (input[0].is_bitmap()) {
        tmp._u.uint32_val = input[0]._u.bitmap->maximum();
    }
    return tmp;
}

ExprValue rb_rank(const std::vector<ExprValue>& input) {
    if (input.size() != 2) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::UINT32);
    if (input[0].is_bitmap()) {
        uint32_t val_tmp = input[1].get_numberic<uint32_t>();
        tmp._u.uint32_val = input[0]._u.bitmap->rank(val_tmp);
    }
    return tmp;
}

ExprValue rb_jaccard_index(const std::vector<ExprValue>& input) {
    ExprValue result(pb::DOUBLE);
    if (input.size() != 2 || !input[0].is_bitmap() || !input[1].is_bitmap()) {
        result._u.double_val = 0;
    } else {
        result._u.double_val = (input[0]._u.bitmap->jaccard_index(*input[1]._u.bitmap));
    }
    return result;
}

ExprValue tdigest_build(const std::vector<ExprValue>& input) {
    if (input.size() == 0) {
        return ExprValue::Null();
    }
    ExprValue result(pb::TDIGEST);
    tdigest::td_histogram_t* t = (tdigest::td_histogram_t*)result.str_val.data();
    for (uint32_t i = 0; i < input.size(); i++) {
        tdigest::td_add(t, input[i].get_numberic<double>(), 1);
    }
    tdigest::td_serialize(result.str_val);
    return result;
}

ExprValue tdigest_add(const std::vector<ExprValue>& input) {
    if (input.size() == 0) {
        return ExprValue::Null();
    }
    if (input[0].is_null()) {
        (ExprValue&)input[0] = ExprValue::Tdigest();
    } else if (!input[0].is_tdigest()) {
        return ExprValue::Null();
    }
    ExprValue result = input[0];
    tdigest::td_normallize(result.str_val);
    tdigest::td_histogram_t* t = (tdigest::td_histogram_t*)result.str_val.data();
    for (uint32_t i = 1; i < input.size(); i++) {
        tdigest::td_add(t, input[i].get_numberic<double>(), 1);
    }
    tdigest::td_serialize(result.str_val);
    return result;
}

ExprValue tdigest_merge(const std::vector<ExprValue>& input) {
    if (input.size() == 0) {
        return ExprValue::Null();
    }
    if (input[0].is_null()) {
        (ExprValue&)input[0] = ExprValue::Tdigest();
    } else if (!input[0].is_tdigest()) {
        return ExprValue::Null();
    }
    ExprValue result = input[0];
    tdigest::td_normallize(result.str_val);
    tdigest::td_histogram_t* t = (tdigest::td_histogram_t*)result.str_val.data();
    for (uint32_t i = 1; i < input.size(); i++) {
        if (input[i].is_tdigest() && tdigest::is_td_object(input[i].str_val)) {
            tdigest::td_histogram_t* f = (tdigest::td_histogram_t*)input[i].str_val.data();
            tdigest::td_merge(t, f);
        }
    }
    tdigest::td_serialize(result.str_val);
    return result;
}

ExprValue tdigest_total_sum(const std::vector<ExprValue>& input) {
    if (input.size() != 1 || !input[0].is_tdigest()) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    ExprValue result(pb::DOUBLE);
    tdigest::td_histogram_t* t = (tdigest::td_histogram_t*)tmp.str_val.data();
    result._u.double_val = tdigest::td_total_sum(t);
    return result;
}

ExprValue tdigest_total_count(const std::vector<ExprValue>& input) {
    if (input.size() != 1 || !input[0].is_tdigest()) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    ExprValue result(pb::DOUBLE);
    tdigest::td_histogram_t* t = (tdigest::td_histogram_t*)tmp.str_val.data();
    result._u.double_val = tdigest::td_total_count(t);
    return result;
}

ExprValue tdigest_percentile(const std::vector<ExprValue>& input) {
    if (input.size() != 2 || !input[0].is_tdigest()) {
        return ExprValue::Null();
    }
    if (input[1].get_numberic<double>() > 1.0 || input[1].get_numberic<double>() < 0.0) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    ExprValue result(pb::DOUBLE);
    tdigest::td_histogram_t* t = (tdigest::td_histogram_t*)tmp.str_val.data();
    result._u.double_val = tdigest::td_value_at(t, input[1].get_numberic<double>());
    return result;
}

ExprValue tdigest_location(const std::vector<ExprValue>& input) {
    if (input.size() != 2 || !input[0].is_tdigest()) {
        return ExprValue::Null();
    }
    ExprValue result(pb::DOUBLE);
    tdigest::td_histogram_t* t = (tdigest::td_histogram_t*)input[0].str_val.data();
    result._u.double_val = tdigest::td_quantile_of(t, input[1].get_numberic<double>());
    return result;
}

ExprValue version(const std::vector<ExprValue>& input) {
    ExprValue tmp(pb::STRING);
    tmp.str_val = FLAGS_db_version;
    return tmp;
}

ExprValue last_insert_id(const std::vector<ExprValue>& input) {
    if (input.size() == 0) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    return tmp.cast_to(pb::INT64);
}

ExprValue point_distance(const std::vector<ExprValue>& input) {
    if (input.size() < 4) {
        return ExprValue::Null();
    }
    double latitude1 = input[0].get_numberic<double>();
    double longitude1 = input[1].get_numberic<double>();
    double latitude2 = input[2].get_numberic<double>();
    double longitude2 = input[3].get_numberic<double>();
    ExprValue result(pb::INT64);
    double tmp_pow1 = std::pow(std::sin((latitude1 * M_PI / 180 - latitude2 * M_PI / 180) / 2),2);
    double tmp_pow2 = std::pow(std::sin((longitude1  * M_PI / 180 - longitude2  * M_PI / 180) / 2),2);
    double tmp_cos_multi = std::cos(latitude1  * M_PI / 180) * std::cos(latitude2  * M_PI / 180);
    double tmp_sqrt = std::sqrt(tmp_pow1 + tmp_cos_multi * tmp_pow2);
    double tmp_asin = std::asin(tmp_sqrt);
    result._u.int64_val = std::round(1000 * 6378.138 * 2 * tmp_asin);
    return result;
}

ExprValue cast_to_date(const std::vector<ExprValue>& input) {
    if (input.size() != 1) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    return tmp.cast_to(pb::DATE);
}

ExprValue cast_to_datetime(const std::vector<ExprValue>& input) {
    if (input.size() != 1) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    tmp.cast_to(pb::DATETIME);
    tmp.set_precision_len(0);
    return tmp;
}

ExprValue cast_to_time(const std::vector<ExprValue>& input) {
    if (input.size() != 1) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    return tmp.cast_to(pb::TIME);
}

ExprValue cast_to_string(const std::vector<ExprValue>& input) {
    if (input.size() != 1) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    return tmp.cast_to(pb::STRING);
}

ExprValue cast_to_signed(const std::vector<ExprValue>& input) {
    if (input.size() != 1) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    return tmp.cast_to(pb::INT64);
}

ExprValue cast_to_unsigned(const std::vector<ExprValue>& input) {
    if (input.size() != 1) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    return tmp.cast_to(pb::UINT64);
}

ExprValue cast_to_double(const std::vector<ExprValue>& input) {
    if (input.size() != 1) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    return tmp.cast_to(pb::DOUBLE);
}

ExprValue find_in_set(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 2) {
        return ExprValue::Null();
    }
    std::string target = ExprValue(input[0]).cast_to(pb::STRING).str_val;
    std::string set_str = ExprValue(input[1]).cast_to(pb::STRING).str_val;
    std::vector<std::string>set;
    boost::split(set, set_str, boost::is_any_of(","));
    ExprValue res(pb::INT64);
    for (int i = 0; i < set.size(); i ++) {
        if (set[i] == target) {
            res._u.int64_val = i + 1;
            break;
        }
    }
    return res;
}

ExprValue export_set(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 4 && input.size() != 5) {
        return ExprValue::Null();
    }
    ExprValue num = input[0];
    num.cast_to(pb::INT64);
    ExprValue on = input[1];
    on.cast_to(pb::STRING);
    ExprValue off = input[2];
    off.cast_to(pb::STRING);
    ExprValue sep = input[3];
    sep.cast_to(pb::STRING);
    ExprValue len(pb::INT64);
    len._u.int64_val = 64;
    if (input.size() == 5) {
        len = input[4];
        len.cast_to(pb::INT64);
    }
    if (len._u.int64_val > 64 || len._u.int64_val < 1) {
        len._u.int64_val = 64;
    }
    ExprValue res(pb::STRING);
    for (int i = 0; i < len._u.int64_val; i ++) {
        if (i != 0) {
            res.str_val += sep.str_val;
        }
        if (num._u.int64_val & 1) {
            res.str_val += on.str_val;
        } else {
            res.str_val += off.str_val;
        }
        num._u.int64_val >>= 1;
    }
    return res;
}

ExprValue to_base64(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 1) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    tmp.cast_to(pb::STRING);
    ExprValue res(pb::STRING);
    butil::StringPiece sp(tmp.str_val);
    butil::Base64Encode(sp, &res.str_val);
    return res;
}

ExprValue from_base64(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 1) {
        return ExprValue::Null();
    }
    ExprValue tmp = input[0];
    tmp.cast_to(pb::STRING);
    ExprValue res(pb::STRING);
    butil::StringPiece sp(tmp.str_val);
    butil::Base64Decode(sp, &res.str_val);
    return res;
}

ExprValue make_set(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() < 2) {
        return ExprValue::Null();
    }
    ExprValue num = input[0];
    num.cast_to(pb::INT64);
    ExprValue res(pb::STRING);
    bool flag = false;
    ExprValue t;
    for (size_t i = 0; i < input.size() - 1; ++ i) {
        if (num._u.int64_val & 1) {
            if (flag) {
                res.str_val += ",";
            }
            t = input[i+1];
            t.cast_to(pb::STRING);
            res.str_val += t.str_val;
            flag = true;
        }
        num._u.int64_val >>= 1;
        if (num._u.int64_val == 0) {
            break;
        }
    }
    return res;
}

ExprValue oct(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 1) {
        return ExprValue::Null();
    }
    ExprValue num = input[0];
    num.cast_to(pb::INT64);
    ExprValue res(pb::STRING);
    std::stringstream ss;
    ss << std::oct << num._u.int64_val;
    res.str_val = ss.str();
    return res;
}

ExprValue hex(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 1) {
        return ExprValue::Null();
    }
    ExprValue num = input[0];
    ExprValue res(pb::STRING);
    if (num.is_numberic() ){
        num.cast_to(pb::INT64);
        std::stringstream ss;
        ss << std::hex << num._u.int64_val;
        res.str_val = ss.str();
    } else {
        num.cast_to(pb::STRING);
        char* c = new char[2 * num.str_val.size() + 1];
        int idx = 0;
        for (char ch : num.str_val) {
            sprintf(c + idx, "%02X", static_cast<uint8_t>(ch));
            idx += 2;
        }
        c[2 * num.str_val.size()] = 0;
        res.str_val = c;
        delete[] c;
    }
    std::transform(res.str_val.begin(), res.str_val.end(), res.str_val.begin(), ::toupper);
    return res;
}

ExprValue bin(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 1) {
        return ExprValue::Null();
    }
    ExprValue num = input[0];
    num.cast_to(pb::INT64);
    ExprValue res(pb::STRING);
    uint64_t u = static_cast<unsigned long long>(num._u.int64_val);
    if (u == 0) {
        res.str_val = "0";
        return res;
    }
    while (u) {
        res.str_val += (u & 1) + '0';
        u >>= 1;
    }
    std::reverse(res.str_val.begin(), res.str_val.end());
    return res;
}

ExprValue space(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 1) {
        return ExprValue::Null();
    }
    ExprValue arg1 = input[0];
    arg1.cast_to(pb::INT32);
    int num = arg1._u.int32_val;
    if (num < 1 || num > 1000000) {
        return ExprValue::Null();
    }
    ExprValue res(pb::STRING);
    for (int i = 0; i < num; i ++ ){
        res.str_val += " ";
    }
    return res;
}

ExprValue unhex(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 1) {
        return ExprValue::Null();
    }
    ExprValue argv = input[0];
    argv.cast_to(pb::STRING);
    ExprValue result(pb::STRING);
    int num = 0;
    bool flag = argv.str_val.size() % 2 == 0;
    for (char ch : argv.str_val) {
        flag = !flag;
        if (ch >= '0' && ch <= '9') {
            num = (num << 4) + (ch - '0');
        } else if (ch >= 'A' && ch <= 'F') {
            num = (num << 4) + (ch - 'A' + 10);
        } else if (ch >= 'a' && ch <= 'f') {
            num = (num << 4) + (ch - 'a' + 10);
        } else {
            return ExprValue::Null();
        }
        if (flag) {
            result.str_val += static_cast<char>(num);
            num = 0;
        }
    }
    if (num != 0) {
        result.str_val += static_cast<char>(num);
    }
    return result;
}

ExprValue elt(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() <= 1) {
        return ExprValue::Null();
    }
    ExprValue idx = input[0];
    idx.cast_to(pb::INT64);
    int64_t index = idx._u.int64_val;
    if (index < 1 || index > input.size() -1) {
        return ExprValue::Null();
    }
    ExprValue res = input[index];
    return res.cast_to(pb::STRING);
}

ExprValue char_length(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 1) {
        return ExprValue::Null();
    }
    ExprValue argv = input[0];
    argv.cast_to(pb::STRING);
    int length = 0;
    int bytes = 0;
    for (char ch : argv.str_val) {
        if ((ch & 0b10000000) == 0b00000000) {  // 1 byte
            ++length;
            bytes = 0;
        } else if ((ch & 0b11100000) == 0b11000000) {  // 2 bytes
            ++length;
            bytes = 1;
        } else if ((ch & 0b11110000) == 0b11100000) {  // 3 bytes
            bytes = 2;
        } else if ((ch & 0b11111000) == 0b11110000) {  // 4 bytes
            bytes = 3;
        } else if ((ch & 0b11111100) == 0b11111000) {  // 5 bytes
            bytes = 4;
        } else if ((ch & 0b11111110) == 0b11111100) {  // 6 bytes
            bytes = 5;
        }
        if (bytes == 0) {
            bytes = 1;
        } else {
            --bytes;
        }
    }
    ExprValue res(pb::INT32);
    res._u.int32_val = length;
    return res;
}

ExprValue format(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 2) {
        return ExprValue::Null();
    }
    ExprValue num = input[0];
    num.cast_to(pb::DOUBLE);
    ExprValue pre = input[1];
    pre.cast_to(pb::INT32);
    std::ostringstream stream;
    stream << std::fixed << std::setprecision(pre._u.int32_val) << num._u.double_val;
    std::string str = stream.str();
    int len = str.size();
    int pos = str.find('.');
    if (pos == std::string::npos) {
        pos = len;
    }
    for (int i = pos - 3; i > 0; i -= 3) {
        str.insert(i, ",");
    }
    ExprValue res(pb::STRING);
    res.str_val = str;
    return res;
}

ExprValue field(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() < 2) {
        return ExprValue::Null();
    }
    ExprValue target = input[0];
    target.cast_to(pb::STRING);
    ExprValue res(pb::INT32);
    for (size_t i = 1; i < input.size(); i ++) {
        ExprValue tmp = input[i];
        if (tmp.cast_to(pb::STRING).str_val == target.str_val) {
            res._u.int32_val = i;
            break;
        }
    }
    return res;
}

ExprValue quote(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 1) {
        return ExprValue::Null();
    }
    ExprValue in = input[0];
    in.cast_to(pb::STRING);
    std::string result = "'";
    for (char c : in.str_val) {
        if (c == '\'') {
            result += "\\'";
        } else if (c == '\\') {
            result += "\\\\";
        } else {
            result += c;
        }
    }
    result += "'";
    ExprValue res(pb::STRING);
    res.str_val = result;
    return res;
}

ExprValue func_char(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() < 1) {
        return ExprValue::Null();
    }
    ExprValue res(pb::STRING);
    for (size_t i = 0; i < input.size(); ++ i) {
        ExprValue tmp = input[i];
        if (tmp.is_null()) {
            continue;
        }
        tmp.cast_to(pb::UINT32);
        std::string cur;
        while (tmp._u.uint32_val) {
            cur += static_cast<char>(tmp._u.uint32_val & 255);
            tmp._u.uint32_val >>= 8;
        }
        std::reverse(cur.begin(), cur.end());
        res.str_val += cur;
    }
    return res;
}

ExprValue soundex(const std::vector<ExprValue>& input) {
    INPUT_CHECK_NULL;
    if (input.size() != 1) {
        return ExprValue::Null();
    }
    ExprValue argv = input[0];
    argv.cast_to(pb::STRING);
    std::string result;
    for (char ch : argv.str_val) {
        result += std::tolower(ch);
    }

    int len = argv.str_val.length();
    int i = 0;
    int size = 0;
    char last_char = 0;
    std::string code;
    while (i < len && size < 3) {
        unsigned char c = argv.str_val[i];
        int bytes = 1;
        if (c >= 0xc0 && c <= 0xdf) {
            bytes = 2;
        } else if (c >= 0xe0 && c <= 0xef) {
            bytes = 3;
        } else if (c >= 0xf0 && c <= 0xf7) {
            bytes = 4;
        } else if (c >= 0xf8 && c <= 0xfb) {
            bytes = 5;
        } else if (c >= 0xfc && c <= 0xfd) {
            bytes = 6;
        }
        if (code.empty()) {
            if (bytes == 1 && std::tolower(c) >= 'a' and std::tolower(c) <= 'z') {
                code = std::toupper(c);
            } else if (bytes > 1) {
                if (i + bytes < len) {
                    code = argv.str_val.substr(i, bytes);
                }
            }
        } else if (bytes == 1) {
            char low = std::tolower(c);
            char cur_c = '0';
            switch (low) {
                case 'b': case 'f': case 'p': case 'v':
                    cur_c = '1';
                    break;
                case 'c': case 'g': case 'j': case 'k': case 'q': case 's': case 'x': case 'z':
                    cur_c= '2';
                    break;
                case 'd': case 't':
                    cur_c = '3';
                    break;
                case 'l':
                    cur_c = '4';
                    break;
                case 'm': case 'n':
                    cur_c = '5';
                    break;
                case 'r':
                    cur_c = '6';
                    break;
            }
            if (cur_c != '0' && cur_c != last_char) {
                size += 1;
                code += cur_c;
                last_char = cur_c;
            }
        }
        i += bytes;
    }
    if (!code.empty()) {
        while (size < 3) {
            code += '0';
            size ++;
        }
    }
    ExprValue res(pb::STRING);
    res.str_val = code;
    return res;
}

// to_sql
#define TO_SQL_FUNC_DEFINE(FUNC_NAME)                               \
    std::string FUNC_NAME(const std::vector<std::string>& input) {  \
        std::string res;                                            \
        res = #FUNC_NAME;                                           \
        res += "(";                                                 \
        for (int i = 0; i < input.size(); ++i) {                    \
            res += input[i];                                        \
            if (i < input.size() - 1) {                             \
                res += ", ";                                        \
            }                                                       \
        }                                                           \
        res += ")";                                                 \
        return res;                                                 \
    }

//number functions
TO_SQL_FUNC_DEFINE(round)
TO_SQL_FUNC_DEFINE(floor)
TO_SQL_FUNC_DEFINE(ceil)
TO_SQL_FUNC_DEFINE(abs)
TO_SQL_FUNC_DEFINE(sqrt)
TO_SQL_FUNC_DEFINE(rand)
TO_SQL_FUNC_DEFINE(sign)
TO_SQL_FUNC_DEFINE(sin)
TO_SQL_FUNC_DEFINE(asin)
TO_SQL_FUNC_DEFINE(cos)
TO_SQL_FUNC_DEFINE(acos)
TO_SQL_FUNC_DEFINE(tan)
TO_SQL_FUNC_DEFINE(cot)
TO_SQL_FUNC_DEFINE(atan)
TO_SQL_FUNC_DEFINE(ln)
TO_SQL_FUNC_DEFINE(log)
TO_SQL_FUNC_DEFINE(pi)
TO_SQL_FUNC_DEFINE(greatest)
TO_SQL_FUNC_DEFINE(least)
TO_SQL_FUNC_DEFINE(pow)
//string functions
TO_SQL_FUNC_DEFINE(length)
TO_SQL_FUNC_DEFINE(bit_length)
TO_SQL_FUNC_DEFINE(lower)
TO_SQL_FUNC_DEFINE(upper)
TO_SQL_FUNC_DEFINE(concat)
TO_SQL_FUNC_DEFINE(substr)
TO_SQL_FUNC_DEFINE(left)
TO_SQL_FUNC_DEFINE(right)
TO_SQL_FUNC_DEFINE(trim)
TO_SQL_FUNC_DEFINE(ltrim)
TO_SQL_FUNC_DEFINE(rtrim)
TO_SQL_FUNC_DEFINE(concat_ws)
TO_SQL_FUNC_DEFINE(ascii)
TO_SQL_FUNC_DEFINE(strcmp)
TO_SQL_FUNC_DEFINE(insert)
TO_SQL_FUNC_DEFINE(replace)
TO_SQL_FUNC_DEFINE(repeat)
TO_SQL_FUNC_DEFINE(reverse)
TO_SQL_FUNC_DEFINE(locate)
TO_SQL_FUNC_DEFINE(substring_index)
TO_SQL_FUNC_DEFINE(lpad)
TO_SQL_FUNC_DEFINE(rpad)
TO_SQL_FUNC_DEFINE(instr)
TO_SQL_FUNC_DEFINE(json_extract)
TO_SQL_FUNC_DEFINE(regexp_replace)
TO_SQL_FUNC_DEFINE(export_set)
TO_SQL_FUNC_DEFINE(make_set)
TO_SQL_FUNC_DEFINE(oct)
TO_SQL_FUNC_DEFINE(hex)
TO_SQL_FUNC_DEFINE(unhex)
TO_SQL_FUNC_DEFINE(bin)
TO_SQL_FUNC_DEFINE(space)
TO_SQL_FUNC_DEFINE(elt)
TO_SQL_FUNC_DEFINE(char_length)
TO_SQL_FUNC_DEFINE(format)
TO_SQL_FUNC_DEFINE(field)
TO_SQL_FUNC_DEFINE(quote)
TO_SQL_FUNC_DEFINE(soundex)
// datetime functions
TO_SQL_FUNC_DEFINE(unix_timestamp)
TO_SQL_FUNC_DEFINE(from_unixtime)
TO_SQL_FUNC_DEFINE(now)
TO_SQL_FUNC_DEFINE(utc_timestamp)
TO_SQL_FUNC_DEFINE(utc_date)
TO_SQL_FUNC_DEFINE(utc_time)
TO_SQL_FUNC_DEFINE(minute)
TO_SQL_FUNC_DEFINE(second)
TO_SQL_FUNC_DEFINE(microsecond)
TO_SQL_FUNC_DEFINE(period_diff)
TO_SQL_FUNC_DEFINE(period_add)
TO_SQL_FUNC_DEFINE(date_format)
TO_SQL_FUNC_DEFINE(str_to_date)
TO_SQL_FUNC_DEFINE(time_format)
TO_SQL_FUNC_DEFINE(convert_tz)
TO_SQL_FUNC_DEFINE(timediff)
TO_SQL_FUNC_DEFINE(curdate)
TO_SQL_FUNC_DEFINE(current_date)
TO_SQL_FUNC_DEFINE(curtime)
TO_SQL_FUNC_DEFINE(current_time)
TO_SQL_FUNC_DEFINE(current_timestamp)
TO_SQL_FUNC_DEFINE(timestamp)
TO_SQL_FUNC_DEFINE(date)
TO_SQL_FUNC_DEFINE(hour)
TO_SQL_FUNC_DEFINE(day)
TO_SQL_FUNC_DEFINE(dayname)
TO_SQL_FUNC_DEFINE(dayofweek)
TO_SQL_FUNC_DEFINE(dayofmonth)
TO_SQL_FUNC_DEFINE(dayofyear)
TO_SQL_FUNC_DEFINE(month)
TO_SQL_FUNC_DEFINE(monthname)
TO_SQL_FUNC_DEFINE(year)
TO_SQL_FUNC_DEFINE(yearweek)
TO_SQL_FUNC_DEFINE(week)
TO_SQL_FUNC_DEFINE(weekofyear)
TO_SQL_FUNC_DEFINE(time_to_sec)
TO_SQL_FUNC_DEFINE(sec_to_time)
TO_SQL_FUNC_DEFINE(datediff)
TO_SQL_FUNC_DEFINE(weekday)
TO_SQL_FUNC_DEFINE(to_days)
TO_SQL_FUNC_DEFINE(to_seconds)
TO_SQL_FUNC_DEFINE(addtime)
TO_SQL_FUNC_DEFINE(subtime)
// case when functions
TO_SQL_FUNC_DEFINE(ifnull)
TO_SQL_FUNC_DEFINE(isnull)
TO_SQL_FUNC_DEFINE(nullif)
// Encryption and Compression Functions
TO_SQL_FUNC_DEFINE(md5)
TO_SQL_FUNC_DEFINE(sha1)
TO_SQL_FUNC_DEFINE(sha)
TO_SQL_FUNC_DEFINE(from_base64)
TO_SQL_FUNC_DEFINE(to_base64)
// other
TO_SQL_FUNC_DEFINE(version)
// TO_SQL_FUNC_DEFINE(last_insert_id)
TO_SQL_FUNC_DEFINE(find_in_set)

#undef TO_SQL_FUNC_DEFINE

static std::string remove_quotation_marks(const std::string& str) {
    if (str.size() < 2) {
        return str;
    }
    if (str[0] == '\"' && str[str.size() - 1] == '\"') {
        return str.substr(1, str.size() - 2);
    }
    return str;
}

// 特殊函数
std::string if_(const std::vector<std::string>& input) {
    std::string res;
    res = "if(";
    for (int i = 0; i < input.size(); ++i) {
        res += input[i];
        if (i < input.size() - 1) {
            res += ", ";
        }
    }
    res += ")";
    return res;
}

std::string func_char(const std::vector<std::string>& input) {
    std::string res;
    res = "char(";
    for (int i = 0; i < input.size(); ++i) {
        res += input[i];
        if (i < input.size() - 1) {
            res += ", ";
        }
    }
    res += ")";
    return res;
}

std::string func_time(const std::vector<std::string>& input) {
    std::string res;
    res = "time(";
    for (int i = 0; i < input.size(); ++i) {
        res += input[i];
        if (i < input.size() - 1) {
            res += ", ";
        }
    }
    res += ")";
    return res;
}

std::string func_quarter(const std::vector<std::string>& input) {
    std::string res;
    res = "quarter(";
    for (int i = 0; i < input.size(); ++i) {
        res += input[i];
        if (i < input.size() - 1) {
            res += ", ";
        }
    }
    res += ")";
    return res;
}

// ~ -
std::string bit_not(const std::vector<std::string>& input) {
    if (input.size() == 0) {
        return "";
    }
    std::string res;
    res += "(";
    res += "~";
    res += input[0];
    res += ")";
    return res;
}

std::string uminus(const std::vector<std::string>& input) {
    if (input.size() == 0) {
        return "";
    }
    std::string res;
    res += "(";
    res += "-";
    res += input[0];
    res += ")";
    return res;
}

// + - * /
std::string add(const std::vector<std::string>& input) {
    if (input.size() < 2) {
        return "";
    }
    std::string res;
    res += "(";
    res += input[0];
    res += " + ";
    res += input[1];
    res += ")";
    return res;
}

std::string minus(const std::vector<std::string>& input) {
    if (input.size() < 2) {
        return "";
    }
    std::string res;
    res += "(";
    res += input[0];
    res += " - ";
    res += input[1];
    res += ")";
    return res;
}

std::string multiplies(const std::vector<std::string>& input) {
    if (input.size() < 2) {
        return "";
    }
    std::string res;
    res += "(";
    res += input[0];
    res += " * ";
    res += input[1];
    res += ")";
    return res;
}

std::string divides(const std::vector<std::string>& input) {
    if (input.size() < 2) {
        return "";
    }
    std::string res;
    res += "(";
    res += input[0];
    res += " / ";
    res += input[1];
    res += ")";
    return res;
}

// % << >> & | ^ 
std::string mod(const std::vector<std::string>& input) {
    if (input.size() < 2) {
        return "";
    }
    std::string res;
    res += "(";
    res += input[0];
    res += " % ";
    res += input[1];
    res += ")";
    return res;
}

std::string left_shift(const std::vector<std::string>& input) {
    if (input.size() < 2) {
        return "";
    }
    std::string res;
    res += "(";
    res += input[0];
    res += " << ";
    res += input[1];
    res += ")";
    return res;
}

std::string right_shift(const std::vector<std::string>& input) {
    if (input.size() < 2) {
        return "";
    }
    std::string res;
    res += "(";
    res += input[0];
    res += " >> ";
    res += input[1];
    res += ")";
    return res;
}

std::string bit_and(const std::vector<std::string>& input) {
    if (input.size() < 2) {
        return "";
    }
    std::string res;
    res += "(";
    res += input[0];
    res += " & ";
    res += input[1];
    res += ")";
    return res;
}

std::string bit_or(const std::vector<std::string>& input) {
    if (input.size() < 2) {
        return "";
    }
    std::string res;
    res += "(";
    res += input[0];
    res += " | ";
    res += input[1];
    res += ")";
    return res;
}

std::string bit_xor(const std::vector<std::string>& input) {
    if (input.size() < 2) {
        return "";
    }
    std::string res;
    res += "(";
    res += input[0];
    res += " ^ ";
    res += input[1];
    res += ")";
    return res;
}

// == != > >= < <=
std::string eq(const std::vector<std::string>& input) {
    if (input.size() < 2) {
        return "";
    }
    std::string res;
    res += "(";
    res += input[0];
    res += " = ";
    res += input[1];
    res += ")";
    return res;
}

std::string ne(const std::vector<std::string>& input) {
    if (input.size() < 2) {
        return "";
    }
    std::string res;
    res += "(";
    res += input[0];
    res += " != ";
    res += input[1];
    res += ")";
    return res;
}

std::string gt(const std::vector<std::string>& input) {
    if (input.size() < 2) {
        return "";
    }
    std::string res;
    res += "(";
    res += input[0];
    res += " > ";
    res += input[1];
    res += ")";
    return res;
}

std::string ge(const std::vector<std::string>& input) {
    if (input.size() < 2) {
        return "";
    }
    std::string res;
    res += "(";
    res += input[0];
    res += " >= ";
    res += input[1];
    res += ")";
    return res;
}

std::string lt(const std::vector<std::string>& input) {
    if (input.size() < 2) {
        return "";
    }
    std::string res;
    res += "(";
    res += input[0];
    res += " < ";
    res += input[1];
    res += ")";
    return res;
}

std::string le(const std::vector<std::string>& input) {
    if (input.size() < 2) {
        return "";
    }
    std::string res;
    res += "(";
    res += input[0];
    res += " <= ";
    res += input[1];
    res += ")";
    return res;
}

// 目前不支持多字段全文索引，有需求可以升级
std::string match_against(const std::vector<std::string>& input) {
    if (input.size() < 3) {
        return "";
    }
    // input[0]是ROW表达式，因此MATCH后面不需要再添加括号
    std::string res;
    res += "MATCH ";
    res += input[0];
    res += " AGAINST ( ";
    res += input[1];
    res += " ";
    res += remove_quotation_marks(input[2]);
    res += ")";
    return res;
}

std::string case_when(const std::vector<std::string>& input) {
    std::string res;
    res += "(";
    res += "CASE ";
    for (int i = 0; i < input.size() / 2; ++i) {
        res += " WHEN ";
        res += input[i * 2];
        res += " THEN ";
        res += input[i * 2 + 1];
    }
    if (input.size() % 2 != 0) {
        res += " ELSE ";
        res += input[input.size() - 1];
    }
    res += " END";
    res += ")";
    return res;
}

std::string case_expr_when(const std::vector<std::string>& input) {
    if (input.size() == 0) {
        return "";
    }
    std::string res;
    res += "(";
    res += "CASE ";
    res += input[0];
    for (int i = 0; i < (input.size()-1) / 2; ++i) {
        res += " WHEN ";
        res += input[i * 2 + 1];
        res += " THEN ";
        res += input[i * 2 + 2];
    }
    if ((input.size() - 1) % 2 != 0) {
        res += " ELSE ";
        res += input[input.size() - 1];
    }
    res += " END";
    res += ")";
    return res;
}

// 时间函数
std::string timestampadd(const std::vector<std::string>& input) {
    if (input.size() < 3) {
        return "";
    }
    std::string res;
    res += "TIMESTAMPADD(";
    res += remove_quotation_marks(input[0]);
    res += ", ";
    res += input[1];
    res += ", ";
    res += input[2];
    res += ")";
    return res;
}

std::string timestampdiff(const std::vector<std::string>& input) {
    if (input.size() < 3) {
        return "";
    }
    std::string res;
    res += "TIMESTAMPDIFF(";
    res += remove_quotation_marks(input[0]);
    res += ", ";
    res += input[1];
    res += ", ";
    res += input[2];
    res += ")";
    return res;
}

std::string date_add(const std::vector<std::string>& input) {
    if (input.size() < 3) {
        return "";
    }
    std::string res;
    res += "DATE_ADD(";
    res += input[0];
    res += ", INTERVAL ";
    res += input[1];
    res += " ";
    res += remove_quotation_marks(input[2]);
    res += ")";
    return res;
}

std::string date_sub(const std::vector<std::string>& input) {
    if (input.size() < 3) {
        return "";
    }
    std::string res;
    res += "DATE_SUB(";
    res += input[0];
    res += ", INTERVAL ";
    res += input[1];
    res += " ";
    res += remove_quotation_marks(input[2]);
    res += ")";
    return res;
}

std::string extract(const std::vector<std::string>& input) {
    if (input.size() < 2) {
        return "";
    }
    std::string res;
    res += "EXTRACT(";
    res += remove_quotation_marks(input[0]);
    res += " FROM ";
    res += input[1];
    res += ")";
    return res;
}

// cast函数
std::string cast_to_date(const std::vector<std::string>& input) {
    if (input.size() < 1) {
        return "";
    }
    std::string res;
    res += "CAST(";
    res += input[0];
    res += " AS DATE)";
    return res;
}

std::string cast_to_time(const std::vector<std::string>& input) {
    if (input.size() < 1) {
        return "";
    }
    std::string res;
    res += "CAST(";
    res += input[0];
    res += " AS TIME)";
    return res;
}

std::string cast_to_datetime(const std::vector<std::string>& input) {
    if (input.size() < 1) {
        return "";
    }
    std::string res;
    res += "CAST(";
    res += input[0];
    res += " AS DATETIME)";
    return res;
}

std::string cast_to_signed(const std::vector<std::string>& input) {
    if (input.size() < 1) {
        return "";
    }
    std::string res;
    res += "CAST(";
    res += input[0];
    res += " AS SIGNED INTEGER)";
    return res;
}

std::string cast_to_unsigned(const std::vector<std::string>& input) {
    if (input.size() < 1) {
        return "";
    }
    std::string res;
    res += "CAST(";
    res += input[0];
    res += " AS UNSIGNED INTEGER)";
    return res;
}

std::string cast_to_string(const std::vector<std::string>& input) {
    if (input.size() < 1) {
        return "";
    }
    std::string res;
    res += "CAST(";
    res += input[0];
    res += " AS BINARY)";
    return res;
}

std::string cast_to_double(const std::vector<std::string>& input) {
    if (input.size() < 1) {
        return "";
    }
    std::string res;
    res += "CAST(";
    res += input[0];
    res += " AS DECIMAL(65,15))"; // 65: 总位数，15: 小数位数；保证大多数情况够用
    // 如果存在精度问题，可以尝试以下隐式转换的方式
    // res = "("
    // res += input[0];
    // res += " + 0.0)";
    return res;
}

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
