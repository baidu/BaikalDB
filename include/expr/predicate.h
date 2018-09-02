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

#include <set>
#include <boost/regex.hpp>
#include "expr_value.h"
#include "scalar_fn_call.h"

namespace baikaldb {
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
};

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
    enum MapType {
        M_INT,
        M_DOUBLE,
        M_STRING,
        M_DATETIME,
        M_TIME,
        M_DATE,
        M_TIMESTAMP,
        M_NULL
    };
public:
    InPredicate() : _has_in_null(false) {}
    virtual int open();
    virtual int type_inferer();
    virtual ExprValue get_value(MemRow* row);

private:
    MapType _map_type;
    std::set<int64_t> _int_set;
    std::set<double> _double_set;
    std::set<std::string> _str_set;
    bool _has_in_null;
};

class LikePredicate : public ScalarFnCall {
public:
    //todo liguoqiang
    virtual int open();
    void covent_pattern(const std::string& pattern);
    virtual ExprValue get_value(MemRow* row);

private:
    boost::regex _regex;
    std::string _regex_pattern;
    char _escape_char = '\\';
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
