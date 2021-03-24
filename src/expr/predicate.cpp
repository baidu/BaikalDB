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

namespace baikaldb {
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

int LikePredicate::open() {
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
    if (!children(1)->is_constant()) {
        DB_WARNING("param 2 must be constant");
        return -1;
    }
    std::string like_pattern = children(1)->get_value(nullptr).get_string();
    re2::RE2::Options option;
    option.set_utf8(false);
    option.set_dot_nl(true);
    if (_fn.fn_op() == parser::FT_EXACT_LIKE) {
        covent_exact_pattern(like_pattern);
        option.set_case_sensitive(false);
        _regex_ptr.reset(new re2::RE2(_regex_pattern, option));
    } else {
        covent_pattern(like_pattern);
        _regex_ptr.reset(new re2::RE2(_regex_pattern, option));
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

ExprValue LikePredicate::get_value(MemRow* row) {
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

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
