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
#include <rapidjson/stringbuffer.h>
#include "hll_common.h"
#include "datetime.h"
#include <boost/date_time/gregorian/gregorian.hpp>
#include <cctype>
#include <cmath>
#include <algorithm>

namespace baikaldb {
#ifdef BAIKALDB_REVISION
    DEFINE_string(db_version, "5.7.16-BaikalDB-v"BAIKALDB_REVISION, "db version");
#else
    DEFINE_string(db_version, "5.7.16-BaikalDB", "db version");
#endif
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
    tmp._u.double_val = ::abs(input[0].get_numberic<double>());
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

ExprValue ascii(const std::vector<ExprValue>& input) {
    if (input.size() < 1) {
        return ExprValue::Null();
    }

    if (input[0].is_null()) {
        return ExprValue::Null();
    }

    ExprValue tmp(pb::INT32);

    if (input[0].str_val.empty()) {
        tmp._u.int32_val = 0;
    } else {
        tmp._u.int32_val = static_cast<int32_t>(input[0].str_val[0]);
    }

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
    if (pos < 0) {
        return input[0];
    }

    int len = input[2].get_numberic<int>();
    if (len <= 0) {
        return input[0];
    }

    ExprValue tmp(pb::STRING);
    tmp.str_val = input[0].str_val;
    tmp.str_val.replace(pos, len, input[2].str_val);
    
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
            DB_WARNING("parse json_str error [code:%d][%s]", code, json_str.c_str());
            return ExprValue::Null();
        }

    } catch (...) {
        DB_WARNING("parse json_str error [%s]", json_str.c_str());
        return ExprValue::Null();
    }
    rapidjson::Pointer pointer(path.c_str());
    if (!pointer.IsValid()) {
        DB_WARNING("invalid path: [%s]", path.c_str());
        return ExprValue::Null();
    }

    const rapidjson::Value *pValue = rapidjson::GetValueByPointer(doc, pointer);
    if (pValue == nullptr) {
        DB_WARNING("the path: [%s] does not exist in doc [%s]", path.c_str(), json_str.c_str());
        return ExprValue::Null();
    }
    // TODO type on fly
    ExprValue tmp(pb::STRING);
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
    return tmp;
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
    return ExprValue::Now(0);
}

ExprValue utc_timestamp(const std::vector<ExprValue>& input) {
    return ExprValue::UTC_TIMESTAMP();
}

ExprValue timestamp(const std::vector<ExprValue>& input) {
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
    if (input.size() != 2) {
        return ExprValue::Null();
    }
    for (auto& s : input) {
        if (s.is_null()) {
            return ExprValue::Null();
        }
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
    if (input.size() != 2) {
        return ExprValue::Null();
    }
    for (auto& s : input) {
        if (s.is_null()) {
            return ExprValue::Null();
        }
    }
    ExprValue tmp = input[0];
    return tmp.cast_to(pb::DATETIME);
}

ExprValue time_format(const std::vector<ExprValue>& input) {
    if (input.size() != 2) {
        return ExprValue::Null();
    }
    for (auto& s : input) {
        if (s.is_null()) {
            return ExprValue::Null();
        }
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

ExprValue convert_tz(const std::vector<ExprValue>& input) {
    if (input.size() != 3){
        return ExprValue::Null();
    }
    for (auto& s : input) {
        if (s.is_null()) {
            return ExprValue::Null();
        }
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
    if (input.size() < 2) {
        return ExprValue::Null();
    }
    for (auto& s : input) {
        if (s.is_null()) {
            return ExprValue::Null();
        }
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
    if (input.size() < 3) {
        return ExprValue::Null();
    }
    for (auto& s : input) {
        if (s.is_null()) {
            return ExprValue::Null();
        }
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
ExprValue day(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue in = input[0];
    if (in.type == pb::INT64) {
        in.cast_to(pb::STRING);
    }
    ExprValue tmp(pb::UINT32);
    time_t t = in.cast_to(pb::TIMESTAMP)._u.uint32_val;
    struct tm tm;
    localtime_r(&t, &tm);
    tmp._u.uint32_val = tm.tm_mday;
    return tmp;
}
ExprValue dayname(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue tmp(pb::STRING);
    uint32_t week_num = dayofweek(input)._u.uint32_val;
    if (week_num <= day_names.size()) {
        tmp.str_val = day_names[week_num - 1];
    }
    return tmp;
}
ExprValue monthname(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    uint32_t month_num = month(input)._u.uint32_val;
    ExprValue tmp(pb::STRING);
    if (month_num <= month_names.size()) {
        tmp.str_val = month_names[month_num - 1];
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
    ExprValue tmp(pb::UINT32);
    time_t t = in.cast_to(pb::TIMESTAMP)._u.uint32_val;
    struct tm tm;
    localtime_r(&t, &tm);
    boost::gregorian::date today(tm.tm_year + 1900, ++tm.tm_mon, tm.tm_mday);
    /*
      DAYOFWEEK(d) 函数返回 d 对应的一周中的索引（位置）。1 表示周日，2 表示周一，……，7 表示周六
    */ 
    tmp._u.uint32_val = today.day_of_week() + 1;
    return tmp;
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
    time_t t = in.cast_to(pb::TIMESTAMP)._u.uint32_val;
    struct tm tm;
    localtime_r(&t, &tm);
    tmp._u.uint32_val = ++tm.tm_mon;
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
    time_t t = in.cast_to(pb::TIMESTAMP)._u.uint32_val;
    struct tm tm;
    localtime_r(&t, &tm);
    tmp._u.uint32_val = tm.tm_year + 1900;
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
    ExprValue tmp(pb::UINT32);
    time_t t = in.cast_to(pb::TIMESTAMP)._u.uint32_val;
    struct tm tm;
    localtime_r(&t, &tm);
    boost::gregorian::date today(tm.tm_year += 1900, ++tm.tm_mon, tm.tm_mday);
    tmp._u.uint32_val = today.day_of_year();
    return tmp;
}
ExprValue weekday(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue in = input[0];
    if (in.type == pb::INT64) {
        in.cast_to(pb::STRING);
    }
    ExprValue one = input[0];
    time_t t = one.cast_to(pb::TIMESTAMP)._u.uint32_val;
    struct tm tm;
    localtime_r(&t, &tm);
    boost::gregorian::date today(tm.tm_year + 1900, ++tm.tm_mon, tm.tm_mday);
    ExprValue tmp(pb::UINT32);
    uint32_t day_of_week = today.day_of_week();
    if (day_of_week >= 1) {
        tmp._u.uint32_val = day_of_week - 1;
    } else {
        tmp._u.uint32_val = 6;
    }
    return tmp;
}
ExprValue week(const std::vector<ExprValue>& input) {
    if (input.size() == 0 || input[0].is_null()) {
        return ExprValue::Null();
    }
    ExprValue one = input[0];
    time_t t = one.cast_to(pb::TIMESTAMP)._u.uint32_val;
    struct tm tm;
    localtime_r(&t, &tm);
    boost::gregorian::date today(tm.tm_year += 1900, ++tm.tm_mon, tm.tm_mday);
    uint32_t week_number = today.week_number() - 1;
    ExprValue tmp(pb::UINT32);
    if (input.size() > 1) {
        ExprValue two = input[1];
        uint32_t mode = two.cast_to(pb::UINT32)._u.uint32_val;
        if (mode > 0) {
            week_number += 1;
        }
    }
    tmp._u.uint32_val = week_number;
    return tmp;
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
    time_t t1 = left.cast_to(pb::TIMESTAMP)._u.uint32_val;
    time_t t2 = right.cast_to(pb::TIMESTAMP)._u.uint32_val;
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
    ExprValue ret = arg1.cast_to(pb::TIMESTAMP);
    if (input[2].str_val == "second") {
        ret._u.uint32_val += interval;
    } else if (input[2].str_val == "minute") {
        ret._u.uint32_val += interval * 60;
    } else if (input[2].str_val == "hour") {
        ret._u.uint32_val += interval * 3600;
    } else if (input[2].str_val == "day") {
        ret._u.uint32_val += interval * (24 * 3600);
    } else {
        // un-support
        return ExprValue::Null();
    }
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
    ExprValue ret = arg1.cast_to(pb::TIMESTAMP);
    if (input[2].str_val == "second") {
        ret._u.uint32_val -= interval;
    } else if (input[2].str_val == "minute") {
        ret._u.uint32_val -= interval * 60;
    } else if (input[2].str_val == "hour") {
        ret._u.uint32_val -= interval * 3600;
    } else if (input[2].str_val == "day") {
        ret._u.uint32_val -= interval * (24 * 3600);
    } else {
        // un-support
        return ExprValue::Null();
    }
    return ret;
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
        if (input[0].compare(input[if_index]) == 0) {
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
    return tmp.cast_to(pb::DATETIME);
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

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
