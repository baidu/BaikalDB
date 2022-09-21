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

#include "predicate.h"
#include "parser.h"
#include <boost/algorithm/string.hpp>

namespace baikaldb {

DEFINE_bool(like_predicate_use_re2, true, "LikePredicate use re2");

int InPredicate::open() {
    int ret = 0;
    ret = ExprNode::open();
    if (ret < 0) {
        DB_WARNING("ExprNode::open fail:%d", ret);
        return ret;
    }
    if (_children.size() < 2) {
        DB_WARNING("InPredicate _children.size:%lu", _children.size());
        return -1;
    }
    if (children(0)->is_row_expr()) {
        return row_expr_open();
    } else {
        return singel_open();
    }
}
ExprValue InPredicate::make_key(ExprNode* e, MemRow* row) {
    ExprValue ret(pb::STRING);
    for (size_t j = 0; j < _col_size; j++) { 
        auto v = e->children(j)->get_value(row);
        if (v.is_null()) {
            return ExprValue::Null();
        }
        ret.str_val += v.cast_to(_row_expr_types[j]).get_string();
        ret.str_val.append(1, '\0');
    }
    return ret;
}

int InPredicate::row_expr_open() {
    _is_row_expr = true;
    _col_size = children(0)->children_size();
    for (size_t i = 1; i < children_size(); i++) {
        if (!children(i)->is_constant()) {
            DB_FATAL("only support in const");
            return -1;
        }
        if (!children(i)->is_row_expr() ||
                children(i)->children_size() != _col_size) {
            DB_FATAL("Operand should contain %lu column(s)", _col_size);
            return -1;
        }
    }
    for (size_t i = 0; i < _col_size; i++) {
        std::vector<pb::PrimitiveType> types = {
            children(0)->children(i)->col_type(), 
            children(1)->children(i)->col_type()};
        if (all_int(types)) {
            _row_expr_types.push_back(pb::INT64);
        } else if (has_datetime(types)) {
            _row_expr_types.push_back(pb::DATETIME);
        } else if (has_timestamp(types)) {
            _row_expr_types.push_back(pb::TIMESTAMP);
        } else if (has_date(types)) {
            _row_expr_types.push_back(pb::DATE);
        } else if (has_time(types)) {
            _row_expr_types.push_back(pb::TIME);
        } else if (has_double(types)) {
            _row_expr_types.push_back(pb::DOUBLE);
        } else if (has_int(types)) {
            _row_expr_types.push_back(pb::DOUBLE);
        } else {
            _row_expr_types.push_back(pb::STRING);
        }
    }
    for (size_t i = 1; i < children_size(); i++) {
        ExprValue v = make_key(children(i), nullptr);
        if (!v.is_null()) {
            _str_set.insert(v.str_val);
        }
    }
    return 0;
}

int InPredicate::singel_open() {
    std::vector<pb::PrimitiveType> types = {_children[0]->col_type(), _children[1]->col_type()};
    if (all_int(types)) {
        _map_type = pb::INT64;
    } else if (has_datetime(types)) {
        _map_type = pb::DATETIME;
    } else if (has_timestamp(types)) {
        _map_type = pb::TIMESTAMP;
    } else if (has_date(types)) {
        _map_type = pb::DATE;
    } else if (has_time(types)) {
        _map_type = pb::TIME;
    } else if (has_double(types)) {
        _map_type = pb::DOUBLE;
    } else if (has_int(types)) {
        _map_type = pb::DOUBLE;
    } else {
        _map_type = pb::STRING;
    }
    for (size_t i = 1; i < _children.size(); i++) {
        if (!_children[i]->is_constant()) {
            DB_FATAL("only support in const");
            return -1;
        }
        ExprValue value = _children[i]->get_value(nullptr);
        if (!value.is_null()) {
            switch (_map_type) {
                case pb::INT64:
                case pb::TIMESTAMP:
                case pb::DATETIME:
                case pb::TIME:
                case pb::DATE:
                    _int_set.insert(value.cast_to(_map_type).get_numberic<int64_t>());
                    break;
                case pb::DOUBLE:
                    _double_set.insert(value.cast_to(_map_type).get_numberic<double>());
                    break;
                case pb::STRING:
                    _str_set.insert(value.cast_to(_map_type).get_string());
                    break;
                default:
                    break;
            }
        }
    }
    return 0;
}

ExprValue InPredicate::get_value(MemRow* row) {
    if (_is_row_expr) {
        auto v = make_key(children(0), row);
        if (v.is_null()) {
            return ExprValue::Null();
        }
        if (_str_set.count(v.str_val) == 1) {
            return ExprValue::True();
        }
        return _has_null ? ExprValue::Null() : ExprValue::False();
    }
    ExprValue value = _children[0]->get_value(row);
    if (value.is_null()) {
        return ExprValue::Null();
    }
    switch (_map_type) {
        case pb::INT64:
        case pb::TIMESTAMP:
        case pb::DATETIME:
        case pb::TIME:
        case pb::DATE:
            if (_int_set.count(value.cast_to(_map_type).get_numberic<int64_t>()) == 1) {
                return ExprValue::True();
            }
            break;
        case pb::DOUBLE:
            if (_double_set.count(value.cast_to(_map_type).get_numberic<double>()) == 1) {
                return ExprValue::True();
            }
            break;
        case pb::STRING:
            if (_str_set.count(value.cast_to(_map_type).get_string()) == 1) {
                return ExprValue::True();
            }
            break;
        default:
            break;
    }
    return _has_null ? ExprValue::Null() : ExprValue::False();
}

void LikePredicate::reset_pattern(MemRow* row) {
    _pattern = children(1)->get_value(row).get_string();
}

int LikePredicate::open() {
    if (FLAGS_like_predicate_use_re2) {
        return open_by_re2();
    } else {
        return open_by_pattern();
    }
}

int LikePredicate::open_by_pattern() {
    int ret = 0;
    ret = ExprNode::open();
    if (ret < 0) {
        DB_WARNING("ExprNode::open fail:%d", ret);
        return ret;
    }
    if (children_size() < 2) {
        DB_WARNING("LikePredicate _children.size:%lu", _children.size());
        return -1;
    }
    std::unordered_set<int32_t> slot_ids;
    children(1)->get_all_slot_ids(slot_ids);
    if (slot_ids.size() == 0) {
        reset_pattern(nullptr);
        if (_fn.fn_op() == parser::FT_EXACT_LIKE) {
            std::vector<std::string> split_pattern;
            boost::split(split_pattern, _pattern, boost::is_any_of("|"));
            if (split_pattern.size() > 1) {
                bool is_prefix = split_pattern.begin()->size() > 0 && split_pattern.begin()->front() == '%';
                bool is_postfix = split_pattern.back().size() > 0 && split_pattern.back().back() == '%';
                auto pattern_iter = split_pattern.begin();
                for(; pattern_iter != split_pattern.end(); pattern_iter++) {
                    if (pattern_iter->size() > 0) {
                        if (is_prefix && pattern_iter->front() != '%') {
                            pattern_iter->insert(0, 1, '%');
                        }

                        if (is_postfix && pattern_iter->back() != '%') {
                            pattern_iter->push_back('%');
                        }
                    }
                }
                split_pattern.swap(_patterns);
            }
        }
    } else {
        _const_pattern = false;
    }
    return 0;
}

void LikePredicate::reset_regex(MemRow* row) {
    std::string like_pattern = children(1)->get_value(row).get_string();
    if (_fn.fn_op() == parser::FT_EXACT_LIKE) {
        covent_exact_pattern(like_pattern);
        _regex_ptr.reset(new re2::RE2(_regex_pattern, _option));
    } else {
        covent_pattern(like_pattern);
        _regex_ptr.reset(new re2::RE2(_regex_pattern, _option));
    }
}

int LikePredicate::open_by_re2() {
    int ret = 0;
    ret = ExprNode::open();
    if (ret < 0) {
        DB_WARNING("ExprNode::open fail:%d", ret);
        return ret;
    }
    if (children_size() < 2) {
        DB_WARNING("LikePredicate _children.size:%lu", _children.size());
        return -1;
    }
    std::unordered_set<int32_t> slot_ids;
    children(1)->get_all_slot_ids(slot_ids);
    _option.set_utf8(false);
    _option.set_dot_nl(true);
    if (_fn.fn_op() == parser::FT_EXACT_LIKE) {
        _option.set_case_sensitive(false);
    }
    if (slot_ids.size() == 0) {
        reset_regex(nullptr);
    } else {
        _const_regex = false;
    }
    return 0;
}

void LikePredicate::covent_pattern(const std::string& pattern) {
    bool is_escaped = false;
    static std::set<char> need_escape_set = {
        '.', '*', '+', '?',
        '[', ']', '{', '}',
        '(', ')', '\\', '|',
        '^', '$'};
    for (uint32_t i = 0; i < pattern.size(); ++i) {
        if (!is_escaped && pattern[i] == '%') {
            _regex_pattern.append(".*");
        } else if (!is_escaped && pattern[i] == '_') {
            _regex_pattern.append(".");
        } else if (!is_escaped && pattern[i] == _escape_char) {
            is_escaped = true;
        } else if (need_escape_set.count(pattern[i]) == 1) {
            _regex_pattern.append("\\");
            _regex_pattern.append(1, pattern[i]);
            is_escaped = false;
        } else {
            _regex_pattern.append(1, pattern[i]);
            is_escaped = false;
        }
    }
}

void LikePredicate::covent_exact_pattern(const std::string& pattern) {
    bool is_escaped = false;
    static std::set<char> need_escape_set = {
        '.', '*', '+', '?',
        '[', ']', '{', '}',
        '(', ')', '\\',
        '^', '$'};
    for (uint32_t i = 0; i < pattern.size(); ++i) {
        if (!is_escaped && pattern[i] == '%') {
            _regex_pattern.append(".*");
        } else if (!is_escaped && pattern[i] == '_') {
            _regex_pattern.append(".");
        } else if (!is_escaped && pattern[i] == '|') {
            _regex_pattern.append(".*");
            _regex_pattern.append("|");
            _regex_pattern.append(".*");
        } else if (!is_escaped && pattern[i] == _escape_char) {
            is_escaped = true;
        } else if (need_escape_set.count(pattern[i]) == 1) {
            _regex_pattern.append("\\");
            _regex_pattern.append(1, pattern[i]);
            is_escaped = false;
        } else {
            _regex_pattern.append(1, pattern[i]);
            is_escaped = false;
        }
    }
}

ExprValue LikePredicate::get_value(MemRow* row) {
    if (FLAGS_like_predicate_use_re2) {
        return get_value_by_re2(row);
    } else {
        return get_value_by_pattern(row);
    }
}

ExprValue LikePredicate::get_value_by_re2(MemRow* row) {
    if (!_const_regex) {
        reset_regex(row);
    }
    ExprValue value = children(0)->get_value(row);
    value.cast_to(pb::STRING);
    ExprValue ret(pb::BOOL);
    try {
        ret._u.bool_val = RE2::FullMatch(value.str_val, *_regex_ptr);
        if (_regex_ptr->error_code() != 0) {
            DB_FATAL("regex error[%d]", _regex_ptr->error_code());
        }
    } catch (std::exception& e) {
        DB_FATAL("regex error:%s, _regex_pattern:%ss",
                e.what(), _regex_pattern.c_str());
        ret._u.bool_val = false;
    } catch (...) {
        DB_FATAL("regex unknown error: _regex_pattern:%ss",
                 _regex_pattern.c_str());
        ret._u.bool_val = false;
    }
    return ret;
}


void LikePredicate::hit_index(bool* is_eq, bool* is_prefix, std::string* prefix_value) {
    std::string pattern = children(1)->get_value(nullptr).get_string();
    *is_prefix = false;
    if (pattern[0] != '%' && pattern[0] != '_') {
        *is_prefix = true;
    }
    *is_eq = true;
    bool is_escaped = false;
    for (uint32_t i = 0; i < pattern.size(); ++i) {
        if (!is_escaped && pattern[i] == '%') {
            *is_eq = false;
            break;
        } else if (!is_escaped && pattern[i] == '_') {
            *is_eq = false;
            break;
        } else if (!is_escaped && pattern[i] == _escape_char) {
            is_escaped = true;
        } else {
            prefix_value->append(1, pattern[i]);
            is_escaped = false;
        }
    }
}

bool LikePredicate::like_one(const std::string& target, const std::string& pattern, pb::Charset charset) {
    bool ret = false;
    switch (charset) {
    case pb::GBK:
        {
            DB_DEBUG("GBK like target[%s], pattern[%s]", target.c_str(), pattern.c_str());
            auto like_ret = like<GBKCharset>(target, pattern);
            if (like_ret) {
                ret = *like_ret;
            } else {
                DB_WARNING("GBK like failed target[%s], pattern[%s]", target.c_str(), _pattern.c_str());
                like_ret = like<Binary>(target, pattern);
                if (like_ret) {
                    ret = *like_ret;
                }
            }
        }
        break;
    case pb::UTF8:
        {
            DB_DEBUG("UTF8 like target[%s], pattern[%s]", target.c_str(), pattern.c_str());
            auto like_ret = like<UTF8Charset>(target, pattern);
            if (like_ret) {
                ret = *like_ret;
                break;
            }
        }
        DB_WARNING("UTF8 like failed target[%s], pattern[%s]", target.c_str(), _pattern.c_str());
    default:
        {
            auto like_ret = like<Binary>(target, pattern);
            if (like_ret) {
                ret = *like_ret;
            }
        }
    }
    return ret;
}

ExprValue LikePredicate::get_value_by_pattern(MemRow* row) {
    if (!_const_pattern) {
        reset_pattern(row);
    }
    ExprValue target = children(0)->get_value(row);
    target.cast_to(pb::STRING);
    ExprValue ret(pb::BOOL);
    ret._u.bool_val = false;
    if (!_const_pattern || _patterns.size() == 0) {
        ret._u.bool_val = like_one(target.str_val, _pattern, charset());
    } else {
        for (auto& pattern : _patterns) {
            if (like_one(target.str_val, pattern, charset())) {
                ret._u.bool_val = true;
                break;
            }
        }
    }
    return ret;
}

void RegexpPredicate::reset_regex(MemRow* row) {
    _regex_pattern = children(1)->get_value(row).get_string();
    _regex_ptr.reset(new re2::RE2(_regex_pattern, _option));
}

int RegexpPredicate::open() {
    int ret = 0;
    ret = ExprNode::open();
    if (ret < 0) {
        DB_WARNING("ExprNode::open fail:%d", ret);
        return ret;
    }
    if (children_size() < 2) {
        DB_WARNING("RegexpPredicate _children.size:%lu", _children.size());
        return -1;
    }
    std::unordered_set<int32_t> slot_ids;
    children(1)->get_all_slot_ids(slot_ids);
    _option.set_utf8(false);
    _option.set_dot_nl(true);
    if (slot_ids.size() == 0) {
        reset_regex(nullptr);
    } else {
        _const_regex = false;
    }
    return 0;
}

ExprValue RegexpPredicate::get_value(MemRow* row) {
    if (!_const_regex) {
        reset_regex(row);
    }
    ExprValue value = children(0)->get_value(row);
    value.cast_to(pb::STRING);
    ExprValue ret(pb::BOOL);
    try {
        ret._u.bool_val = RE2::PartialMatch(value.str_val, *_regex_ptr);
        if (_regex_ptr->error_code() != 0) {
            DB_FATAL("regex error[%d]", _regex_ptr->error_code());
        }
    } catch (std::exception& e) {
        DB_FATAL("regex error:%s, _regex_pattern:%ss", 
                e.what(), _regex_pattern.c_str());
        ret._u.bool_val = false;
    } catch (...) {
        DB_FATAL("regex unknown error: _regex_pattern:%ss", 
                 _regex_pattern.c_str());
        ret._u.bool_val = false;
    }
    return ret;
}

size_t LikePredicate::UTF8Charset::get_char_size(size_t idx) {
    size_t num = 1;
    while (++idx < str.size() && (str[idx] & 0xC0) == 0x80) {
        num++;
    }
    return num;
}

rocksdb::Slice LikePredicate::UTF8Charset::next_code_point(size_t idx) {
    if (idx >= str.size()) {
        DB_FATAL("out of range.");
        return rocksdb::Slice();
    }
    if (!(str[idx] & 0x80)) {
        return rocksdb::Slice(&str[idx], 1);
    } else if ((str[idx] & 0xE0) == 0xC0) {
        if (get_char_size(idx) != 2) {
            return rocksdb::Slice();
        }
        return rocksdb::Slice(&str[idx], 2);
    } else if ((str[idx] & 0xF0) == 0xE0) {
        if (get_char_size(idx) != 3) {
            return rocksdb::Slice();
        }
        return rocksdb::Slice(&str[idx], 3);
    } else if ((str[idx] & 0xF0) == 0xF0) {
        if (get_char_size(idx) != 4) {
            return rocksdb::Slice();
        }
        return rocksdb::Slice(&str[idx], 4);
    }
    return rocksdb::Slice();
}

rocksdb::Slice LikePredicate::GBKCharset::next_code_point(size_t idx) {
    if (idx >= str.size()) {
        DB_FATAL("out of range.");
        return rocksdb::Slice();
    }
    if (!(str[idx] & 0x80)) {
        return rocksdb::Slice(&str[idx], 1);
    } else if (idx + 1 < str.size() && in_range(0x81, str[idx], 0xFE) && 
        (in_range(0x40, str[idx + 1], 0x7E) || in_range(0x80, str[idx + 1], 0xFE))) {
        return rocksdb::Slice(&str[idx], 2);
    }
    return rocksdb::Slice();
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
