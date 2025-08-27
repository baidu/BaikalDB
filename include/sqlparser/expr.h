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
#include "arrow/vendored/fast_float/fast_float.h"

namespace parser {
enum ExprType {
    ET_COLUMN,
    ET_LITETAL,
    ET_FUNC,
    ET_ROW_EXPR,
    ET_SUB_QUERY_EXPR,
    ET_CMP_SUB_QUERY_EXPR,
    ET_EXISTS_SUB_QUERY_EXPR,
    ET_COMMON_TABLE_EXPR,
    ET_WINDOW
};
struct ExprNode : public Node {
    ExprType expr_type;
    ExprNode() {
        node_type = NT_EXPR;
    }
    bool is_subquery() {
        if (expr_type == ET_SUB_QUERY_EXPR
            || expr_type == ET_CMP_SUB_QUERY_EXPR
            || expr_type == ET_EXISTS_SUB_QUERY_EXPR) {
            return true;
        }
        return false;
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
    FT_REGEXP,
    FT_MATCH_AGAINST,
    FT_BETWEEN,
    /* use in on dup key update */
    FT_VALUES,
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
    FT_WINDOW,
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
    bool has_subquery() const;
    virtual void print() const override {
        std::cout << "func:" << func_type << " fn_name:" << fn_name << std::endl;
    }
    virtual void to_stream(std::ostream& os) const override;

    static FuncExpr* new_unary_op_node(FuncType t, Node* arg1, butil::Arena& arena);
    static FuncExpr* new_binary_op_node(FuncType t, Node* arg1, Node* arg2, butil::Arena& arena);
    static FuncExpr* new_ternary_op_node(FuncType t, Node* arg1, Node* arg2, Node* arg3, butil::Arena& arena);
};

struct SelectStmt;
struct UnionStmt;
enum CompareType {
    CMP_ANY,
    CMP_SOME,
    CMP_ALL
};
struct SubqueryExpr : public ExprNode {
    DmlNode* query_stmt = nullptr;
    SubqueryExpr() {
        is_complex = true;
        expr_type = ET_SUB_QUERY_EXPR;
    }
    virtual void print() const override {
        std::cout << this << std::endl;
    }
    virtual void set_print_sample(bool print_sample_) override {
        print_sample = print_sample_;
        query_stmt->set_print_sample(print_sample_);
    }
    virtual void set_cache_param(PlanCacheParam* p_cache_param_) override {
        p_cache_param = p_cache_param_;
        if (query_stmt != nullptr) {
            query_stmt->set_cache_param(p_cache_param_);
        }
    }
    virtual void to_stream(std::ostream& os) const override;
};

struct CompareSubqueryExpr: public ExprNode {
    ExprNode* left_expr = nullptr;
    SubqueryExpr* right_expr = nullptr;
    CompareType cmp_type = CMP_ANY;
    FuncType func_type = FT_COMMON;
    CompareSubqueryExpr() {
        is_complex = true;
        expr_type = ET_CMP_SUB_QUERY_EXPR;
    }
    virtual void print() const override {
        std::cout << this << std::endl;
    }
    virtual void set_print_sample(bool print_sample_) override {
        print_sample = print_sample_;
        left_expr->set_print_sample(print_sample_);
        right_expr->set_print_sample(print_sample_);
    }
    virtual void set_cache_param(PlanCacheParam* p_cache_param_) override {
        p_cache_param = p_cache_param_;
        if (left_expr != nullptr) {
            left_expr->set_cache_param(p_cache_param_);
        }
        if (right_expr != nullptr) {
            right_expr->set_cache_param(p_cache_param_);
        }
    }
    virtual void to_stream(std::ostream& os) const override;
    const char* get_func_name() const;
};

struct ExistsSubqueryExpr: public ExprNode {
    SubqueryExpr* query_expr = nullptr;
    bool is_not = false;
    ExistsSubqueryExpr() {
        is_complex = true;
        expr_type = ET_EXISTS_SUB_QUERY_EXPR;
    }
    virtual void print() const override {
        std::cout << this << std::endl;
    }
    virtual void set_print_sample(bool print_sample_) override {
        print_sample = print_sample_;
        if (query_expr != nullptr) {
            query_expr->set_print_sample(print_sample_);
        }
    }
    virtual void set_cache_param(PlanCacheParam* p_cache_param_) override {
        p_cache_param = p_cache_param_;
        if (query_expr != nullptr) {
            query_expr->set_cache_param(p_cache_param_);
        }
    }
    virtual void to_stream(std::ostream& os) const override;
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
    LT_PLACE_HOLDER,
    LT_HEX,
    LT_MAXVALUE
};

struct LiteralExpr : public ExprNode {
    LiteralType literal_type;
    union {
        bool bool_val;
        int64_t int64_val;
        double double_val;
        String str_val;
    } _u;
    String raw_str;

    mutable LiteralType placeholder_literal_type = LT_INT;
    mutable int placeholder_id = -1; // PreparePlanner和PlanCache复用该字段

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
            case LT_HEX:
                std::cout << raw_str.value;
                break;
            case LT_BOOL:
                std::cout << _u.bool_val;
                break;
            case LT_NULL:
                std::cout << "NULL";
                break;
            case LT_PLACE_HOLDER:
                std::cout << "?(" << _u.int64_val << ")";
                break;
            case LT_MAXVALUE:
                std::cout << "MAXVALUE";
                break;
        }
        std::cout << std::endl;
    }
    virtual void to_stream(std::ostream& os) const override;
    virtual std::string to_string() const override;
    virtual void find_placeholder(std::unordered_set<int>& placeholders) override;

    static LiteralExpr* make_int(const char* str, butil::Arena& arena) {
        LiteralExpr* lit = new(arena.allocate(sizeof(LiteralExpr))) LiteralExpr();
        lit->literal_type = LT_INT;
        lit->_u.int64_val = strtoull(str, NULL, 10);
        return lit;
    }

    static LiteralExpr* make_double(const char* str, butil::Arena& arena) {
        LiteralExpr* lit = new(arena.allocate(sizeof(LiteralExpr))) LiteralExpr();
        lit->literal_type = LT_DOUBLE;
        using ::arrow_vendored::fast_float::from_chars;
        from_chars(str, str + strlen(str), lit->_u.double_val);
        return lit;
    }

    static LiteralExpr* make_bit(const char* raw, const char* str, size_t len, butil::Arena& arena) {
        LiteralExpr* lit = new(arena.allocate(sizeof(LiteralExpr))) LiteralExpr();
        std::string out_str;
        out_str.reserve(len / 8 + 1);
        size_t pos = len % 8;
        if (pos != 0) {
            out_str.append(1, bit_to_char(str, pos));
        }
        for (; pos < len; pos += 8) {
            out_str.append(1, bit_to_char(str + pos, 8));
        }
        lit->_u.str_val.strdup(out_str.c_str(), out_str.size(), arena);
        lit->literal_type = LT_HEX;
        lit->raw_str.strdup(raw, strlen(raw), arena);
        return lit;
    }

    static LiteralExpr* make_hex(const char* raw, const char* str, size_t len, butil::Arena& arena) {
        LiteralExpr* lit = new(arena.allocate(sizeof(LiteralExpr))) LiteralExpr();
        std::string out_str;
        out_str.reserve(len / 2 + 1);
        size_t pos = len % 2;
        if (pos != 0) {
            out_str.append(1, hex_to_char(str, pos));
        }
        for (; pos < len; pos += 2) {
            out_str.append(1, hex_to_char(str + pos, 2));
        }
        lit->_u.str_val.strdup(out_str.c_str(), out_str.size(), arena);
        lit->literal_type = LT_HEX;
        lit->raw_str.strdup(raw, strlen(raw), arena);
        return lit;
    }

    static LiteralExpr* make_string(const char* str, butil::Arena& arena) {
        LiteralExpr* lit = new(arena.allocate(sizeof(LiteralExpr))) LiteralExpr();
        lit->literal_type = LT_STRING;
        // trim ' "
        if (str[0] == '"' || str[0] == '\'') {
            //处理sql "query in ('\x00text', 'text')" ，strlen(str)=1
            auto str_len = strlen(str);
            auto cut_num = (str_len == 1 || str[str_len - 1] != str[0]) ? 1 : 2;
            lit->_u.str_val.strdup(str + 1, str_len - cut_num, arena);
        } else {
            lit->_u.str_val.strdup(str, strlen(str), arena);
        }
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
        lit->placeholder_id = place_holder_id;
        return lit;
    }

    static LiteralExpr* make_maxvalue(butil::Arena& arena) {
        LiteralExpr* lit = new(arena.allocate(sizeof(LiteralExpr))) LiteralExpr();
        lit->literal_type = LT_MAXVALUE;
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

struct CommonTableExpr : public ExprNode {
    SubqueryExpr* query_expr = nullptr;
    Vector<ColumnName*> column_names;
    String name;
    CommonTableExpr() {
        is_complex = true;
        expr_type = ET_COMMON_TABLE_EXPR;
    }
    virtual void set_print_sample(bool print_sample_) override {
        print_sample = print_sample_;
        if (query_expr != nullptr) {
            query_expr->set_print_sample(print_sample_);
        }
    }
    virtual void to_stream(std::ostream& os) const override;
};

enum FrameType {
    FT_ROWS,
    FT_RANGE,
};

enum BoundType {
    BT_FOLLOWING,
    BT_PRECEDING,
    BT_CURRENT_ROW,
};

struct ByItem : public Node {
    ExprNode* expr = nullptr;
    bool is_desc = false;
    ByItem() {
        node_type = NT_BY_ITEM;
    }
    virtual bool is_complex_node() override {
        if (is_complex) {
            return true;
        }
        if (expr != nullptr && expr->is_complex_node()) {
            is_complex = true;
            return true;
        }
        return false;
    }
    virtual void set_print_sample(bool print_sample_) override {
        print_sample = print_sample_;
        if (expr != nullptr) {
            expr->set_print_sample(print_sample_);
        }
    }
    virtual void set_cache_param(PlanCacheParam* p_cache_param_) override {
        p_cache_param = p_cache_param_;
        if (expr != nullptr) {
            expr->set_cache_param(p_cache_param_);
        }
    }
    virtual void to_stream(std::ostream& os) const override {
        static const char* desc_str[] = {" ASC", " DESC"};
        os << expr << desc_str[is_desc];
    }
};

struct PartitionByClause : public Node {
    Vector<ByItem*> items;
    PartitionByClause() {
        node_type = NT_PARTITION_BY;
    }
    virtual bool is_complex_node() override {
        if (is_complex) {
            return true;
        }
        for (int i = 0; i < items.size(); ++i) {
            if (items[i] != nullptr && items[i]->is_complex_node()) {
                is_complex = true;
                return true;
            }
        }
        return false;
    }
    virtual void set_print_sample(bool print_sample_) override {
        print_sample = print_sample_;
        for (int i = 0; i < items.size(); ++i) {
            if (items[i] != nullptr) {
                items[i]->set_print_sample(print_sample_);
            }
        }
    }
    virtual void set_cache_param(PlanCacheParam* p_cache_param_) override {
        p_cache_param = p_cache_param_;
        for (int i = 0; i < items.size(); ++i) {
            if (items[i] != nullptr) {
                items[i]->set_cache_param(p_cache_param_);
            }
        }
    }
    virtual void to_stream(std::ostream& os) const override {
        for (int i = 0; i < items.size(); ++i) {
            os << " " << items[i];
            if (i != items.size() - 1) {
                os << ",";
            }
        }
    }
};

struct OrderByClause : public Node {
    Vector<ByItem*> items;
    OrderByClause() {
        node_type = NT_ORDER_BY;
    }
    virtual bool is_complex_node() override {
        if (is_complex) {
            return true;
        }
        for (int i = 0; i < items.size(); i++) {
            if (items[i] != nullptr && items[i]->is_complex_node()) {
                is_complex = true;
                return true;
            }
        }
        return false;
    }
    virtual void set_print_sample(bool print_sample_) override {
        print_sample = print_sample_;
        for (int i = 0; i < items.size(); i++) {
            items[i]->set_print_sample(print_sample_);
        }
    }
    virtual void set_cache_param(PlanCacheParam* p_cache_param_) override {
        p_cache_param = p_cache_param_;
        for (int i = 0; i < items.size(); i++) {
            if (items[i] != nullptr) {
                items[i]->set_cache_param(p_cache_param_);
            }
        }
    }
    virtual void to_stream(std::ostream& os) const override {
        for (int i = 0; i < items.size(); i++) {
            os << " " << items[i];
            if (i != items.size() -1) {
                os << ",";
            }
        }
    }
};

struct FrameBound : public Node {
    BoundType bound_type;
    bool is_unbounded = false;
    ExprNode* expr = nullptr;
    FrameBound() {
        node_type = NT_WINDOW_FRAME_BOUND;
    }
    virtual bool is_complex_node() override {
        if (is_complex) {
            return true;
        }
        if (expr != nullptr && expr->is_complex_node()) {
            is_complex = true;
            return true;
        }
        return false;
    }
    virtual void to_stream(std::ostream& os) const override {
        if (is_unbounded) {
            os << " UNBOUNDED";
        }
        if (expr != nullptr) {
            os << " " << expr;
        }
        switch (bound_type) {
        case BT_FOLLOWING:
            os << " FOLLOWING";
            break;
        case BT_PRECEDING:
            os << " PRECEDING";
            break;
        case BT_CURRENT_ROW:
            os << " CURRENT ROW";
            break;
        default:
            break;
        }
    }
    virtual void set_print_sample(bool print_sample_) override {
        print_sample = print_sample_;
        if (expr != nullptr) {
            expr->set_print_sample(print_sample_);
        }
    }
    virtual void set_cache_param(PlanCacheParam* p_cache_param_) override {
        p_cache_param = p_cache_param_;
        if (expr != nullptr) {
            expr->set_cache_param(p_cache_param_);
        }
    }
};

struct FrameExtent : public Node {
    FrameBound* frame_start = nullptr;
    FrameBound* frame_end = nullptr;
    bool use_between = false;
    FrameExtent() {
        node_type = NT_WINDOW_FRAME_EXTENT;
    }
    virtual bool is_complex_node() override {
        if (is_complex) {
            return true;
        }
        if (frame_start != nullptr && frame_start->is_complex_node()) {
            is_complex = true;
            return true;
        }
        if (frame_end != nullptr && frame_end->is_complex_node()) {
            is_complex = true;
            return true;
        }
        return false;
    }
    virtual void to_stream(std::ostream& os) const override {
        if (use_between) {
            os << " BETWEEN";
            if (frame_start != nullptr) {
                os << frame_start;
            }
            os << " AND";
            if (frame_end != nullptr) {
                os << frame_end;
            }
        } else {
            if (frame_start != nullptr) {
                os << frame_start;
            }
        }
    }
    virtual void set_print_sample(bool print_sample_) override {
        print_sample = print_sample_;
        if (frame_start != nullptr) {
            frame_start->set_print_sample(print_sample_);
        }
        if (frame_end != nullptr) {
            frame_end->set_print_sample(print_sample_);
        }
    }
    virtual void set_cache_param(PlanCacheParam* p_cache_param_) override {
        p_cache_param = p_cache_param_;
        if (frame_start != nullptr) {
            frame_start->set_cache_param(p_cache_param_);
        }
        if (frame_end != nullptr) {
            frame_end->set_cache_param(p_cache_param_);
        }
    }
};

struct WindowFrameClause : public Node {
    FrameType frame_type;
    FrameExtent* frame_extent = nullptr;
    WindowFrameClause() {
        node_type = NT_WINDOW_FRAME;
    }
    virtual bool is_complex_node() override {
        if (is_complex) {
            return true;
        }
        if (frame_extent != nullptr && frame_extent->is_complex_node()) {
            is_complex = true;
            return true;
        }
        return false;
    }
    virtual void to_stream(std::ostream& os) const override {
        switch (frame_type) {
        case FT_ROWS:
            os << " ROWS";
            break;
        case FT_RANGE:
            os << " RANGE";
            break;
        default:
            break;
        }
        if (frame_extent != nullptr) {
            os << frame_extent;
        }
    }
    virtual void set_print_sample(bool print_sample_) override {
        print_sample = print_sample_;
        if (frame_extent != nullptr) {
            frame_extent->set_print_sample(print_sample_);
        }
    }
    virtual void set_cache_param(PlanCacheParam* p_cache_param_) override {
        p_cache_param = p_cache_param_;
        if (frame_extent != nullptr) {
            frame_extent->set_cache_param(p_cache_param_);
        }
    }
};

struct WindowSpec : public Node {
    PartitionByClause* partition_by = nullptr;
    OrderByClause* order_by = nullptr;
    WindowFrameClause* frame = nullptr;
    WindowSpec() {
        node_type = NT_WINDOW_SPEC;
    }
    virtual bool is_complex_node() {
        if (is_complex) {
            return true;
        }
        if (partition_by != nullptr && partition_by->is_complex_node()) {
            is_complex = true;
            return true;
        }
        if (order_by != nullptr && order_by->is_complex_node()) {
            is_complex = true;
            return true;
        }
        if (frame != nullptr && frame->is_complex_node()) {
            is_complex = true;
            return true;
        }
        return false;
    }
    virtual void to_stream(std::ostream& os) const override {
        if (partition_by != nullptr) {
            os << " PARTITION BY" << partition_by;
        }
        if (order_by != nullptr) {
            os << " ORDER BY" << order_by;
        }
        if (frame != nullptr) {
            os << frame;
        }
    }
    virtual void set_print_sample(bool print_sample_) override {
        print_sample = print_sample_;
        if (partition_by != nullptr) {
            partition_by->set_print_sample(print_sample_);
        }
        if (order_by != nullptr) {
            order_by->set_print_sample(print_sample_);
        }
        if (frame != nullptr) {
            frame->set_print_sample(print_sample_);
        }
    }
    virtual void set_cache_param(PlanCacheParam* p_cache_param_) override {
        p_cache_param = p_cache_param_;
        if (partition_by != nullptr) {
            partition_by->set_cache_param(p_cache_param_);
        }
        if (order_by != nullptr) {
            order_by->set_cache_param(p_cache_param_);
        }
        if (frame != nullptr) {
            frame->set_cache_param(p_cache_param_);
        }
    }
};

struct WindowFuncExpr : public ExprNode {
    FuncExpr* func_expr = nullptr;     // 开窗函数
    WindowSpec* window_spec = nullptr; // 窗口定义
    // 是否忽略NULL值；MySQL当前只支持`RESPECT NULLS`，如果为true，逻辑计划执行报错，与MySQL保持一致
    bool ignore_null = false;
    // 是否按照从后往前的方向进行窗口计算；MySQL当前只支持`FROM FIRST`，如果为true，逻辑计划执行报错，与MySQL保持一致
    bool from_last = false; 
    WindowFuncExpr() {
        expr_type = ET_WINDOW;
    }
    virtual bool is_complex_node() {
        if (is_complex) {
            return true;
        }
        if (func_expr != nullptr && func_expr->is_complex_node()) {
            is_complex = true;
            return true;
        }
        if (window_spec != nullptr && window_spec->is_complex_node()) {
            is_complex = true;
            return true;
        }
        return false;
    }
    virtual void to_stream(std::ostream& os) const override {
        os << " " << func_expr;
        if (func_expr != nullptr) {
            if (func_expr->fn_name.to_string() == "NTH_VALUE") {
                if (from_last) {
                    os << " FROM LAST ";
                } else {
                    os << " FROM FIRST ";
                }
            }
            if (func_expr->fn_name.to_string() == "LEAD"
                    || func_expr->fn_name.to_string() == "LAG"
                    || func_expr->fn_name.to_string() == "FIRST_VALUE"
                    || func_expr->fn_name.to_string() == "LAST_VALUE"
                    || func_expr->fn_name.to_string() == "NTH_VALUE") {
                if (ignore_null) {
                    os << " IGNORE NULLS ";
                } else {
                    os << " RESPECT NULLS ";
                }
            }
        }
        if (window_spec != nullptr) {
            os << " OVER (" << window_spec << ")";
        }
    }
    virtual void set_print_sample(bool print_sample_) override {
        print_sample = print_sample_;
        if (func_expr != nullptr) {
            func_expr->set_print_sample(print_sample_);
        }
        if (window_spec != nullptr) {
            window_spec->set_print_sample(print_sample_);
        }
    }
    virtual void set_cache_param(PlanCacheParam* p_cache_param_) override {
        p_cache_param = p_cache_param_;
        if (func_expr != nullptr) {
            func_expr->set_cache_param(p_cache_param_);
        }
        if (window_spec != nullptr) {
            window_spec->set_cache_param(p_cache_param_);
        }
    }
};

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
