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

#include "predicate.h"

namespace baikaldb {
int InPredicate::type_inferer() {
    int ret = 0;
    ret = ExprNode::type_inferer();
    if (ret < 0) {
        return ret;
    }
    if (_children.size() < 2) {
        DB_WARNING("InPredicate _children.size:%u", _children.size());
        return -1;
    }
    switch (_children[0]->col_type()) {
        case pb::BOOL:
        case pb::INT8:
        case pb::INT16:
        case pb::INT32:
        case pb::INT64:
        case pb::UINT8:
        case pb::UINT16:
        case pb::UINT32:
        case pb::UINT64:
            _map_type = M_INT;
            break;
        case pb::TIMESTAMP:
            _map_type = M_TIMESTAMP;
            break;
        case pb::DATETIME:
            _map_type = M_DATETIME;
            break;
        case pb::DATE:
            _map_type = M_DATE;
            break;
        case pb::FLOAT:
        case pb::DOUBLE:
            _map_type = M_DOUBLE;
            break;
        case pb::STRING:
            _map_type = M_STRING;
            break;
        default:
            _map_type = M_NULL;
            break;
    }
    // 用于过滤重复的数据，防止索引多次读取
    std::vector<ExprNode*> tmp_children;
    tmp_children.push_back(_children[0]);
    for (size_t i = 1; i < _children.size(); i++) {
        if (!_children[i]->is_constant()) {
            DB_FATAL("only support in const");
            return -1;
        }
        _children[i]->open();
        ExprValue value = _children[i]->get_value(nullptr);
        if (value.is_null()) {
            _has_in_null = true;
            tmp_children.push_back(_children[i]);
        } else {
            switch (_map_type) {
                case M_INT:
                    if (_int_set.count(value.get_numberic<int64_t>()) == 0) {
                        _int_set.insert(value.get_numberic<int64_t>());
                        tmp_children.push_back(_children[i]);
                    } else {
                        delete _children[i];
                    }
                    break;
                case M_TIMESTAMP:
                    value.cast_to(pb::TIMESTAMP);
                    if (_int_set.count(value.get_numberic<int64_t>()) == 0) {
                        _int_set.insert(value.get_numberic<int64_t>());
                        tmp_children.push_back(_children[i]);
                    } else {
                        delete _children[i];
                    }
                    break;
                case M_DATETIME:
                    value.cast_to(pb::DATETIME);
                    if (_int_set.count(value.get_numberic<int64_t>()) == 0) {
                        _int_set.insert(value.get_numberic<int64_t>());
                        tmp_children.push_back(_children[i]);
                    } else {
                        delete _children[i];
                    }
                    break;
                case M_DATE:
                    value.cast_to(pb::DATE);
                    if (_int_set.count(value.get_numberic<int64_t>()) == 0) {
                        _int_set.insert(value.get_numberic<int64_t>());
                        tmp_children.push_back(_children[i]);
                    } else {
                        delete _children[i];
                    }
                    break;
                case M_DOUBLE:
                    if (_double_set.count(value.get_numberic<double>()) == 0) {
                        _double_set.insert(value.get_numberic<double>());
                        tmp_children.push_back(_children[i]);
                    } else {
                        delete _children[i];
                    }
                    break;
                case M_STRING:
                    if (_str_set.count(value.get_string()) == 0) {
                        _str_set.insert(value.get_string());
                        tmp_children.push_back(_children[i]);
                    } else {
                        delete _children[i];
                    }
                    break;
                default:
                    break;
            }
        }
    }
    _children.swap(tmp_children);
    return 0;
}

int InPredicate::open() {
    int ret = 0;
    ret = ExprNode::open();
    if (ret < 0) {
        DB_WARNING("ExprNode::open fail:%d", ret);
        return ret;
    }
    if (_children.size() < 2) {
        DB_WARNING("InPredicate _children.size:%u", _children.size());
        return -1;
    }
    switch (_children[0]->col_type()) {
        case pb::BOOL:
        case pb::INT8:
        case pb::INT16:
        case pb::INT32:
        case pb::INT64:
        case pb::UINT8:
        case pb::UINT16:
        case pb::UINT32:
        case pb::UINT64:
            _map_type = M_INT;
            break;
        case pb::TIMESTAMP:
            _map_type = M_TIMESTAMP;
            break;
        case pb::DATETIME:
            _map_type = M_DATETIME;
            break;
        case pb::DATE:
            _map_type = M_DATE;
            break;
        case pb::FLOAT:
        case pb::DOUBLE:
            _map_type = M_DOUBLE;
            break;
        case pb::STRING:
            _map_type = M_STRING;
            break;
        default:
            _map_type = M_NULL;
            break;
    }
    for (size_t i = 1; i < _children.size(); i++) {
        if (!_children[i]->is_constant()) {
            DB_FATAL("only support in const");
            return -1;
        }
        ExprValue value = _children[i]->get_value(nullptr);
        if (value.is_null()) {
            _has_in_null = true;
        } else {
            switch (_map_type) {
                case M_INT:
                    _int_set.insert(value.get_numberic<int64_t>());
                    break;
                case M_TIMESTAMP:
                    _int_set.insert(value.cast_to(pb::TIMESTAMP).get_numberic<int64_t>());
                    break;
                case M_DATETIME:
                    _int_set.insert(value.cast_to(pb::DATETIME).get_numberic<int64_t>());
                    break;
                case M_DATE:
                    _int_set.insert(value.cast_to(pb::DATE).get_numberic<int64_t>());
                    break;
                case M_DOUBLE:
                    _double_set.insert(value.get_numberic<double>());
                    break;
                case M_STRING:
                    _str_set.insert(value.get_string());
                    break;
                default:
                    break;
            }
        }
    }
    return 0;
}

ExprValue InPredicate::get_value(MemRow* row) {
    ExprValue value = _children[0]->get_value(row);
    if (value.is_null() && _has_in_null) {
        return ExprValue::True();
    }
    switch (_map_type) {
        case M_INT:
            if (_int_set.count(value.get_numberic<int64_t>()) == 1) {
                return ExprValue::True();
            }
            break;
        case M_TIMESTAMP:
            if (_int_set.count(value.cast_to(pb::TIMESTAMP).get_numberic<int64_t>()) == 1) {
                return ExprValue::True();
            }
            break;
        case M_DATETIME:
            if (_int_set.count(value.cast_to(pb::DATETIME).get_numberic<int64_t>()) == 1) {
                return ExprValue::True();
            }
            break;
        case M_DATE:
            if (_int_set.count(value.cast_to(pb::DATE).get_numberic<int64_t>()) == 1) {
                return ExprValue::True();
            }
            break;
        case M_DOUBLE:
            if (_double_set.count(value.get_numberic<double>()) == 1) {
                return ExprValue::True();
            }
            break;
        case M_STRING:
            if (_str_set.count(value.get_string()) == 1) {
                return ExprValue::True();
            }
            break;
        default:
            break;
    }
    return ExprValue::False();
}

int LikePredicate::open() {
    int ret = 0;
    ret = ExprNode::open();
    if (ret < 0) {
        DB_WARNING("ExprNode::open fail:%d", ret);
        return ret;
    }
    if (children_size() < 2) {
        DB_WARNING("LikePredicate _children.size:%u", _children.size());
        return -1;
    }
    if (!children(1)->is_constant()) {
        DB_WARNING("param 2 must be constant");
        return -1;
    }
    std::string like_pattern = children(1)->get_value(nullptr).get_string();
    covent_pattern(like_pattern);
    try {
        _regex = _regex_pattern;
    } catch (boost::regex_error& e) {
        DB_FATAL("regex error:%d|%s, like_pattern:%s, _regex_pattern:%ss", 
                e.code(), e.what(), like_pattern.c_str(), _regex_pattern.c_str());
        return -1;
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

ExprValue LikePredicate::get_value(MemRow* row) {
    ExprValue value = children(0)->get_value(row);
    value.cast_to(pb::STRING);
    ExprValue ret(pb::BOOL);
    ret._u.bool_val = boost::regex_match(value.str_val, _regex);
    return ret;
}

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
