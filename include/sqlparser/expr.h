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
#include "base.h"
#include <stdlib.h>

namespace parser {
enum ExprType {
    ET_COLUMN,
    ET_LITETAL,
    ET_FUNC,
    ET_ROW_EXPR
};
struct ExprNode : public Node {
    ExprType expr_type;
    ExprNode() {
        node_type = NT_EXPR;
    }
    virtual void to_stream(std::ostream& os) const override {}
};

enum FuncType {
    FT_COMMON,
    FT_AGG,
    // ~ ! -
    FT_BIT_NOT,
    FT_LOGIC_NOT,
    FT_UMINUS,
    // + - * /
    FT_ADD,
    FT_MINUS,
    FT_MULTIPLIES,
    FT_DIVIDES,
    // % << >> & | ^
    FT_MOD,
    FT_LS,
    FT_RS,
    FT_BIT_AND,
    FT_BIT_OR,
    FT_BIT_XOR,
    // == != > >= < <=
    FT_EQ,
    FT_NE,
    FT_GT,
    FT_GE,
    FT_LT,
    FT_LE,
    // && || xor
    FT_LOGIC_AND,
    FT_LOGIC_OR,
    FT_LOGIC_XOR,
    // is like in null true
    FT_IS_NULL,
    FT_IS_TRUE,
    FT_IS_UNKNOWN,
    FT_IN,
    FT_LIKE,
    FT_EXACT_LIKE,
    FT_BETWEEN,
    /* use in on dup key update */
    FT_VALUES
    /*
    // bulit-in agg func
    FT_COUNT,
    FT_SUM,
    FT_AVG,
    FT_MIN,
    FT_MAX,
    FT_GROUP_CONCAT,
    // bulit-in scaler func
    FT_ADDDATE,
    FT_SUBDATE,
    FT_CURDATE,
    FT_CURTIME,
    FT_DATE_ADD,
    FT_DATE_SUB,
    FT_EXTRACT,
    FT_POSITION,
    FT_NOW,
    FT_SUBSTRING,
    FT_TRIM
    */
};

struct FuncExpr : public ExprNode {
    String fn_name;
    FuncType func_type = FT_COMMON;
    bool is_not = false;
    bool distinct = false;
    bool is_star = false;
    FuncExpr() {
        fn_name = nullptr;
        expr_type = ET_FUNC;
    }
    virtual void print() const override {
        std::cout << "func:" << func_type << " fn_name:" << fn_name << std::endl;
    }
    virtual void to_stream(std::ostream& os) const override;

    static FuncExpr* new_unary_op_node(FuncType t, Node* arg1, butil::Arena& arena);
    static FuncExpr* new_binary_op_node(FuncType t, Node* arg1, Node* arg2, butil::Arena& arena);
    static FuncExpr* new_ternary_op_node(FuncType t, Node* arg1, Node* arg2, Node* arg3, butil::Arena& arena);
};

struct ColumnName : public ExprNode {
    String db;
    String table;
    String name;
    ColumnName() {
        db = nullptr;
        table = nullptr;
        name = nullptr;
        expr_type = ET_COLUMN;
    }
    virtual void print() const override {
        std::cout << this << std::endl;
    }
    virtual void to_stream(std::ostream& os) const override;
};

enum LiteralType {
    LT_INT,
    LT_DOUBLE,
    LT_STRING,
    LT_BOOL,
    LT_NULL,
    LT_PLACE_HOLDER
};

struct LiteralExpr : public ExprNode {
    LiteralType literal_type;
    union {
        bool bool_val;
        int64_t int64_val;
        double double_val;
        String str_val;
    } _u;
    LiteralExpr() {
        expr_type = ET_LITETAL;
    }
    virtual void print() const override {
        std::cout << "expr:" << expr_type << " lit:";
        switch (literal_type) {
            case LT_INT:
                std::cout << _u.int64_val;
                break;
            case LT_DOUBLE:
                std::cout << _u.double_val;
                break;
            case LT_STRING:
                std::cout << _u.str_val.value;
                break;
            case LT_BOOL:
                std::cout << _u.bool_val;
                break;
            case LT_NULL:
                std::cout << "NULL";
                break;
            case LT_PLACE_HOLDER:
                std::cout << "?(" << _u.int64_val << ")";
        }
        std::cout << std::endl;
    }
    virtual void to_stream(std::ostream& os) const override;
    virtual std::string to_string() const override;

    static LiteralExpr* make_int(const char* str, butil::Arena& arena) {
        LiteralExpr* lit = new(arena.allocate(sizeof(LiteralExpr))) LiteralExpr();
        lit->literal_type = LT_INT;
        lit->_u.int64_val = strtoull(str, NULL, 10);
        return lit;
    }

    static LiteralExpr* make_double(const char* str, butil::Arena& arena) {
        LiteralExpr* lit = new(arena.allocate(sizeof(LiteralExpr))) LiteralExpr();
        lit->literal_type = LT_DOUBLE;
        lit->_u.double_val = strtod(str, NULL);
        return lit;
    }

    static LiteralExpr* make_string(const char* str, butil::Arena& arena) {
        LiteralExpr* lit = new(arena.allocate(sizeof(LiteralExpr))) LiteralExpr();
        lit->literal_type = LT_STRING;
        // trim ' "
        lit->_u.str_val.strdup(str + 1, strlen(str) - 2, arena);
        return lit;
    }
    static LiteralExpr* make_string(String value, butil::Arena& arena) {
        LiteralExpr* lit = new(arena.allocate(sizeof(LiteralExpr))) LiteralExpr();
        lit->literal_type = LT_STRING;
        lit->_u.str_val = value;
        return lit;
    }

    static LiteralExpr* make_true(butil::Arena& arena) {
        LiteralExpr* lit = new(arena.allocate(sizeof(LiteralExpr))) LiteralExpr();
        lit->literal_type = LT_BOOL;
        lit->_u.bool_val = true;
        return lit;
    }

    static LiteralExpr* make_false(butil::Arena& arena) {
        LiteralExpr* lit = new(arena.allocate(sizeof(LiteralExpr))) LiteralExpr();
        lit->literal_type = LT_BOOL;
        lit->_u.bool_val = false;
        return lit;
    }
    static LiteralExpr* make_null(butil::Arena& arena) {
        LiteralExpr* lit = new(arena.allocate(sizeof(LiteralExpr))) LiteralExpr();
        lit->literal_type = LT_NULL;
        return lit;
    }

    static LiteralExpr* make_place_holder(int place_holder_id, butil::Arena& arena) {
        LiteralExpr* lit = new(arena.allocate(sizeof(LiteralExpr))) LiteralExpr();
        lit->literal_type = LT_PLACE_HOLDER;
        lit->_u.int64_val = place_holder_id;
        return lit;
    }
};

struct RowExpr : public ExprNode {
    RowExpr() {
        expr_type = ET_ROW_EXPR;
    }
    virtual void print() const override {
        std::cout << "row_expr, size:" << children.size() << std::endl;
    }
    virtual void to_stream(std::ostream& os) const override {
        os << "(";
        for (int i = 0; i < children.size(); i++) {
            os << children[i];
            if (i != children.size() - 1) {
                os << ", ";
            }
        }
        os << ")";
    }
};

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
