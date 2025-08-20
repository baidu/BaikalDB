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

#include <set>
#include "expr_value.h"
#include "scalar_fn_call.h"
#include "re2/re2.h"
#include <boost/optional.hpp>
#include "arrow_function.h"

namespace baikaldb {
class AndPredicate : public ScalarFnCall {
public:
    virtual ExprValue get_value(MemRow* row) {
        bool has_null_val = false;
        for (int i = 0; i < children_size(); i++) {
            ExprValue val = _children[i]->get_value(row);
            if (!val.is_null() && val.get_numberic<bool>() == false) {
                return ExprValue::False(); 
            }
            if (val.is_null()) {
                has_null_val = true;
            }
        }
        if (has_null_val) {
            return ExprValue::Null();
        }
        return ExprValue::True();
    }
    virtual int transfer_to_arrow_expression() {
        std::vector<arrow::compute::Expression> args;
        args.reserve(_children.size());
        for (int i = 0; i < _children.size(); ++i) {
            if (_children[i]->transfer_to_arrow_expression() != 0) {
                return -1;
            }
            args.emplace_back(_children[i]->arrow_expr());
        } 
        _arrow_expr = arrow::compute::and_(args);
        return 0;
    }
    virtual bool can_use_arrow_vector() {
        for (auto& c : _children) {
            if (!c->can_use_arrow_vector()) {
                return false;
            }
        }
        return true;
    }
    virtual std::string to_sql(const std::unordered_map<int32_t, std::string>& slotid_fieldname_map, 
                               baikal::client::MysqlShortConnection* conn) override {
        std::string res;
        res += "(";
        for (int i = 0; i < _children.size(); ++i) {
            if (_children[i] == nullptr) {
                return "";
            }
            res += _children[i]->to_sql(slotid_fieldname_map, conn);
            if (i < _children.size() - 1) {
                res += " AND ";
            }
        }
        res += ")";
        return res;
    }
};

class OrPredicate : public ScalarFnCall {
public:
    virtual ExprValue get_value(MemRow* row) {
        bool has_null_val = false;
        for (int i = 0; i < children_size(); i++) {
            ExprValue val = _children[i]->get_value(row);
            if (!val.is_null() && val.get_numberic<bool>() == true) { // short-circuit
                return ExprValue::True(); 
            }
            if (val.is_null()) {
                has_null_val = true;
            }
        }
        if (has_null_val) {
            return ExprValue::Null();
        }
        
        return ExprValue::False();
    }
    virtual int transfer_to_arrow_expression() {
        std::vector<arrow::compute::Expression> args;
        args.reserve(_children.size());
        for (int i = 0; i < _children.size(); ++i) {
            if (_children[i]->transfer_to_arrow_expression() != 0) {
                return -1;
            }
            args.emplace_back(_children[i]->arrow_expr());
        } 
        _arrow_expr = arrow::compute::or_(args);
        return 0;
    }
    virtual bool can_use_arrow_vector() {
        for (auto& c : _children) {
            if (!c->can_use_arrow_vector()) {
                return false;
            }
        }
        return true;
    }
    virtual std::string to_sql(const std::unordered_map<int32_t, std::string>& slotid_fieldname_map, 
                               baikal::client::MysqlShortConnection* conn) override {
        std::string res;
        res += "(";
        for (int i = 0; i < _children.size(); ++i) {
            if (_children[i] == nullptr) {
                return "";
            }
            res += _children[i]->to_sql(slotid_fieldname_map, conn);
            if (i < _children.size() - 1) {
                res += " OR ";
            }
        }
        res += ")";
        return res;
    }
};

class XorPredicate : public ScalarFnCall {
public:
    virtual ExprValue get_value(MemRow* row) {
        ExprValue val1 = _children[0]->get_value(row);
        ExprValue val2 = _children[1]->get_value(row);
        if (val1.is_null() || val2.is_null()) {
            return ExprValue::Null();
        }
        if (val1.get_numberic<bool>() == val2.get_numberic<bool>()) {
            return ExprValue::False();
        }
        return ExprValue::True(); 
    }
    virtual int transfer_to_arrow_expression() {
        std::vector<arrow::compute::Expression> args;
        args.reserve(_children.size());
        for (int i = 0; i < _children.size(); ++i) {
            if (_children[i]->transfer_to_arrow_expression() != 0) {
                return -1;
            }
            args.emplace_back(_children[i]->arrow_expr());
        } 
        _arrow_expr = arrow::compute::call("xor", args);
        return 0;
    }
    virtual bool can_use_arrow_vector() {
        for (auto& c : _children) {
            if (!c->can_use_arrow_vector()) {
                return false;
            }
        }
        return true;
    }
    virtual std::string to_sql(const std::unordered_map<int32_t, std::string>& slotid_fieldname_map, 
                               baikal::client::MysqlShortConnection* conn) override {
        std::string res;
        res += "(";
        for (int i = 0; i < _children.size(); ++i) {
            if (_children[i] == nullptr) {
                return "";
            }
            res += _children[i]->to_sql(slotid_fieldname_map, conn);
            if (i < _children.size() - 1) {
                res += " XOR ";
            }
        }
        res += ")";
        return res;
    }
};

class IsNullPredicate : public ScalarFnCall {
public:
    virtual ExprValue get_value(MemRow* row) {
        ExprValue val1 = _children[0]->get_value(row);
        if (val1.is_null()) {
            return ExprValue::True();
        }
        return ExprValue::False();
    }
    virtual int transfer_to_arrow_expression() {
        std::vector<arrow::compute::Expression> args;
        args.reserve(_children.size());
        for (int i = 0; i < _children.size(); ++i) {
            if (_children[i]->transfer_to_arrow_expression() != 0) {
                return -1;
            }
            args.emplace_back(_children[i]->arrow_expr());
        } 
        _arrow_expr = arrow::compute::call("is_null", args);
        return 0;
    }
    virtual bool can_use_arrow_vector() {
        for (auto& c : _children) {
            if (!c->can_use_arrow_vector()) {
                return false;
            }
        }
        return true;
    }
    virtual std::string to_sql(const std::unordered_map<int32_t, std::string>& slotid_fieldname_map, 
                               baikal::client::MysqlShortConnection* conn) override {
        if (_children.size() == 0) {
            return "";
        }
        if (_children[0] == nullptr) {
            return "";
        }
        std::string res;
        res += "(";
        res += _children[0]->to_sql(slotid_fieldname_map, conn);
        res += " IS NULL";
        res += ")";
        return res;
    }
};

class IsTruePredicate : public ScalarFnCall {
public:
    virtual ExprValue get_value(MemRow* row) {
        ExprValue val1 = _children[0]->get_value(row);
        if (val1.get_numberic<bool>() == true) {
            return ExprValue::True();
        }
        return ExprValue::False();
    }
    virtual int transfer_to_arrow_expression() {
        if (_children[0]->transfer_to_arrow_expression() != 0) {
            return -1;
        }
        _arrow_expr = arrow::compute::call("cast", {_children[0]->arrow_expr()}, arrow::compute::CastOptions::Unsafe(arrow::boolean()));
        return 0;
    }
    virtual bool can_use_arrow_vector() {
        for (auto& c : _children) {
            if (!c->can_use_arrow_vector()) {
                return false;
            }
        }
        return true;
    }
    virtual std::string to_sql(const std::unordered_map<int32_t, std::string>& slotid_fieldname_map, 
                               baikal::client::MysqlShortConnection* conn) override {
        if (_children.size() == 0) {
            return "";
        }
        if (_children[0] == nullptr) {
            return "";
        }
        std::string res;
        res += "(";
        res += _children[0]->to_sql(slotid_fieldname_map, conn);
        res += " IS TRUE";
        res += ")";
        return res;
    }
};

class InPredicate : public ScalarFnCall {
public:
    InPredicate() {}
    virtual int open();
    virtual ExprValue get_value(MemRow* row);
    virtual bool can_use_arrow_vector() {
        if (_children[0]->is_row_expr()) {
            for (int i = 0; i < _children[0]->children_size(); ++i) {
                // 浮点数精度问题, 暂不支持
                if (is_double(_children[0]->children(i)->col_type())) {
                    return false;
                }
            }
        }
        for (auto& c : _children) {
            if (c->is_row_expr()) {
                if (!check_row_expr_is_support(_fn, c)) {
                    return false;
                }
            } else if (!c->can_use_arrow_vector()) {
                return false;
            }
        }
        return true;
    }
    virtual int transfer_to_arrow_expression();
    virtual std::string to_sql(const std::unordered_map<int32_t, std::string>& slotid_fieldname_map, 
                                   baikal::client::MysqlShortConnection* conn) override {
        if (_children.size() == 0) {
            return "";
        }
        if (_children[0] == nullptr) {
            return "";
        }
        std::string res;
        res += "(";
        res += _children[0]->to_sql(slotid_fieldname_map, conn);
        res += " IN (";
        for (int i = 1; i < _children.size(); ++i) {
            if (_children[i] == nullptr) {
                return "";
            }
            res += _children[i]->to_sql(slotid_fieldname_map, conn);
            if (i < _children.size() - 1) {
                res += ",";
            }
        }
        res += ")";
        res += ")";
        return res;
    }

private:
    int singel_open();
    int row_expr_open();
    ExprValue make_key(ExprNode* e, MemRow* row);

    pb::PrimitiveType _map_type;
    std::vector<pb::PrimitiveType> _row_expr_types;
    size_t _col_size;
    std::set<int64_t> _int_set;
    std::set<double> _double_set;
    std::set<std::string> _str_set;
};

class LikePredicate : public ScalarFnCall {
public:
    struct Binary {
        Binary(const std::string& s) : str(s) {}
        rocksdb::Slice next_code_point(size_t idx) {
            return rocksdb::Slice(&str[idx], 1);
        }
        const std::string& str;
    };
    struct UTF8Charset {
        UTF8Charset(const std::string& s) : str(s) {}
        size_t get_char_size(size_t idx);
        rocksdb::Slice next_code_point(size_t idx);
        const std::string& str;
    };
    struct GBKCharset {
        GBKCharset(const std::string& s) : str(s) {}
        bool in_range(uint8_t min, uint8_t ch, uint8_t max) {
            return (ch >= min) && (ch <= max);
        }
        rocksdb::Slice next_code_point(size_t idx);
        const std::string& str;
    };

    //todo liguoqiang
    virtual int open();
    void covent_pattern(const std::string& pattern);
    void covent_exact_pattern(const std::string& pattern);

    void hit_index(bool* is_eq, bool* is_prefix, std::string* prefix_value);
    virtual ExprValue get_value(MemRow* row);
    
    template<class Charset>
    boost::optional<bool> like(const std::string& target, const std::string& pattern);
    bool like_one(const std::string& target, const std::string& pattern, pb::Charset charset);
    virtual int transfer_to_arrow_expression();
    virtual bool can_use_arrow_vector();

    virtual std::string to_sql(const std::unordered_map<int32_t, std::string>& slotid_fieldname_map,
                               baikal::client::MysqlShortConnection* conn) override {
        if (_children.size() < 2) {
            return "";
        }
        if (_children[0] == nullptr || _children[1] == nullptr) {
            return "";
        }
        if (_fn.fn_op() == parser::FT_EXACT_LIKE) {
            // Mysql不支持exact like语法
            return "";
        }
        std::string res;
        res += "(";
        res += _children[0]->to_sql(slotid_fieldname_map, conn);
        res += " LIKE ";
        res += _children[1]->to_sql(slotid_fieldname_map, conn);
        res += ")";
        return res;
    }

private:
    ExprValue get_value_by_re2(MemRow* row);
    ExprValue get_value_by_pattern(MemRow* row);
    void reset_pattern(MemRow* row);
    std::string _pattern;
    std::vector<std::string> _patterns;
    char _escape_char = '\\';
    bool _const_pattern = true;

    int open_by_re2();
    int open_by_pattern();
    void reset_regex(MemRow* row);
    std::unique_ptr<re2::RE2> _regex_ptr;
    std::string _regex_pattern;
    bool _const_regex = true;
    re2::RE2::Options _option;
};

class RegexpPredicate : public ScalarFnCall {
public:
    virtual int open();
    virtual ExprValue get_value(MemRow* row);
    virtual int transfer_to_arrow_expression();
    virtual bool can_use_arrow_vector();
    virtual std::string to_sql(const std::unordered_map<int32_t, std::string>& slotid_fieldname_map, 
                               baikal::client::MysqlShortConnection* conn) override {
        if (_children.size() < 2) {
            return "";
        }
        if (_children[0] == nullptr || _children[1] == nullptr) {
            return "";
        }
        std::string res;
        res += "(";
        res += _children[0]->to_sql(slotid_fieldname_map, conn);
        res += " REGEXP ";
        res += _children[1]->to_sql(slotid_fieldname_map, conn);
        res += ")";
        return res;
    }

private:
    void reset_regex(MemRow* row);
    std::unique_ptr<re2::RE2> _regex_ptr;
    std::string _regex_pattern;
    bool _const_regex = true;
    re2::RE2::Options _option;
};

class NotPredicate : public ScalarFnCall {
public:
    virtual ExprValue get_value(MemRow* row) {
        ExprValue val = _children[0]->get_value(row);
        if (!val.is_null()) {
            val._u.bool_val = !val.get_numberic<bool>();
            val.type = pb::BOOL;
        }
        return val;
    }
    bool always_null_or_false() const {
        if (_children[0]->node_type() == pb::IN_PREDICATE) {
            return _children[0]->has_null();
        }
        return false;
    }
    virtual int transfer_to_arrow_expression() {
        if (_children[0]->transfer_to_arrow_expression() != 0) {
            return -1;
        }
        _arrow_expr = arrow::compute::not_(arrow::compute::call("cast", 
                                                {_children[0]->arrow_expr()}, 
                                                arrow::compute::CastOptions::Unsafe(arrow::boolean())));
        return 0;
    }
    virtual bool can_use_arrow_vector() {
        for (auto& c : _children) {
            if (!c->can_use_arrow_vector()) {
                return false;
            }
        }
        return true;
    }
    virtual std::string to_sql(const std::unordered_map<int32_t, std::string>& slotid_fieldname_map, 
                               baikal::client::MysqlShortConnection* conn) override {
        if (_children.size() == 0) {
            return "";
        }
        if (_children[0] == nullptr) {
            return "";
        }
        std::string res;
        res += "(";
        res += "NOT ";
        res += _children[0]->to_sql(slotid_fieldname_map, conn);
        res += ")";
        return res;
    }
};

template<class Charset>
boost::optional<bool> LikePredicate::like(const std::string& target, const std::string& pattern) {
    DB_DEBUG("process %s %s ", target.c_str(), pattern.c_str());
    size_t tx = 0, px = 0, ntx = 0, npx = 0;
    const static rocksdb::Slice under_score("_", 1);
    const static rocksdb::Slice percent("%", 1);
    rocksdb::Slice escape(&_escape_char, 1);

    Charset target_charset(target), pattern_charset(pattern);
    while (tx < target.size() || px < pattern.size()) {
        if (px < pattern.size()) {
            auto p_point = pattern_charset.next_code_point(px);
            if (p_point.size() == 0) {
                return boost::none;
            }
            DB_DEBUG("get pattern %s", p_point.ToString().c_str());
            if (p_point.compare(under_score) == 0) {
                if (tx < target.size()) {
                    size_t t_offset = 1;
                    auto t_point = target_charset.next_code_point(tx);
                    if (t_point.size() > 0) {
                        t_offset = t_point.size();
                    }
                    px++;
                    tx += t_offset;
                    continue;
                }
            } else if (p_point.compare(percent) == 0) {
                size_t t_offset = 1;
                if (tx < target.size()) {
                    auto t_point = target_charset.next_code_point(tx);
                    if (t_point.size() > 0) {
                        t_offset = t_point.size();
                    }
                }
                npx = px;
                ntx = tx + t_offset;
                px++;
                continue;
            } else {
                if (p_point.compare(escape) == 0 && px + escape.size() < pattern.size()) {
                    px += escape.size();
                    p_point = pattern_charset.next_code_point(px);
                    DB_DEBUG("get pattern %s", p_point.ToString().c_str());
                    if (p_point.size() == 0) {
                        return boost::none;
                    }
                }
                if (tx < target.size()) {
                    auto t_point = target_charset.next_code_point(tx);
                    DB_DEBUG("get target pattern %s", t_point.ToString().c_str());
                    if (t_point.size() == 0) {
                        return boost::none;
                    }
                    if (p_point.compare(t_point) == 0) {
                        px += p_point.size();
                        tx += t_point.size();
                        continue;
                    }
                }
            }
        }

        if (ntx > 0 && ntx <= target.size()) {
            px = npx;
            tx = ntx;
            continue;
        }
        return false;
    }
    return true;
}

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
