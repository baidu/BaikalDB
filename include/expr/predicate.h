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

namespace baikaldb {
class AndPredicate : public ScalarFnCall {
public:
    virtual ExprValue get_value(MemRow* row) {
        ExprValue val1 = _children[0]->get_value(row);
        if (!val1.is_null() && val1.get_numberic<bool>() == false) { // short-circuit
            return ExprValue::False();
        }
        ExprValue val2 = _children[1]->get_value(row);
        if (!val2.is_null() && val2.get_numberic<bool>() == false) {
            return ExprValue::False();
        }
        if (val1.is_null() || val2.is_null()) {
            return ExprValue::Null();
        }
        return ExprValue::True();
    }
};

class OrPredicate : public ScalarFnCall {
public:
    virtual ExprValue get_value(MemRow* row) {
        ExprValue val1 = _children[0]->get_value(row);
        if (!val1.is_null() && val1.get_numberic<bool>() == true) { // short-circuit
            return ExprValue::True(); 
        }
        ExprValue val2 = _children[1]->get_value(row);
        if (!val2.is_null() && val2.get_numberic<bool>() == true) {
            return ExprValue::True();
        }
        if (val1.is_null() || val2.is_null()) {
            return ExprValue::Null();
        }
        return ExprValue::False();
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
};

class InPredicate : public ScalarFnCall {
public:
    InPredicate() {}
    virtual int open();
    virtual ExprValue get_value(MemRow* row);

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
    //todo liguoqiang
    virtual int open();
    void covent_pattern(const std::string& pattern);
    void covent_exact_pattern(const std::string& pattern);
    void hit_index(bool* is_eq, bool* is_prefix, std::string* prefix_value);
    virtual ExprValue get_value(MemRow* row);

private:
    std::unique_ptr<re2::RE2> _regex_ptr;
    std::string _regex_pattern;
    char _escape_char = '\\';
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
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
