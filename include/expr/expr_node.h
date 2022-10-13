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

#include <vector>
#include <unordered_set>
#include "expr_value.h"
#include "mem_row.h"
#include "proto/expr.pb.h"

namespace baikaldb {
const int NOT_BOOL_ERRCODE = -100;
class ExprNode {
public:
    ExprNode() {}
    virtual ~ExprNode() {
        for (auto& e : _children) {
            delete e;
            e = nullptr;
        }
    }

    virtual int init(const pb::ExprNode& node) {
        _node_type = node.node_type();
        _col_type = node.col_type();
        _col_flag = node.col_flag();
        if (node.has_charset()) {
            _charset = node.charset();
        }
        return 0;
    }
    virtual void children_swap() {}

    bool is_literal() {
        switch (_node_type) {
            case pb::NULL_LITERAL:
            case pb::BOOL_LITERAL:
            case pb::INT_LITERAL:
            case pb::DOUBLE_LITERAL:
            case pb::STRING_LITERAL:
            case pb::HLL_LITERAL:
            case pb::BITMAP_LITERAL:
            case pb::DATE_LITERAL:
            case pb::DATETIME_LITERAL:
            case pb::TIME_LITERAL:
            case pb::TIMESTAMP_LITERAL:
            case pb::PLACE_HOLDER_LITERAL:
                return true;
            default:
                return false;
        }
        return false;
    }
    bool has_place_holder() {
        if (is_place_holder()) {
            return true;
        }
        for (auto c : _children) {
            if (c->has_place_holder()) {
                return true;
            }
        }
        return false;
    }
    bool has_agg() {
        if (_node_type == pb::AGG_EXPR) {
            return true;
        }
        for (auto c : _children) {
            if (c->has_agg()) {
                return true;
            }
        }
        return false;
    }
    virtual bool is_place_holder() {
        return false;
    }
    bool is_slot_ref() {
        return _node_type == pb::SLOT_REF;
    }
    bool is_constant() const {
        return _is_constant;
    }
    bool has_null() const {
        return _has_null;
    }
    virtual ExprNode* get_last_insert_id() {
        for (auto c : _children) {
            if (c->get_last_insert_id() != nullptr) {
                return c;
            }
        }
        return nullptr;
    }
    bool is_row_expr() {
        return _node_type == pb::ROW_EXPR;
    }
    int expr_optimize() {
        const_pre_calc();
        return type_inferer();
    }
    //类型推导，只在baikal执行
    virtual int type_inferer() {
        for (auto c : _children) {
            int ret = 0;
            ret = c->type_inferer();
            if (ret < 0) {
                return ret;
            }
        }
        return 0;
    }
    //常量表达式预计算,eg. id * 2 + 2 * 4 => id * 2 + 8
    //TODO 考虑做各种左右变化,eg. id + 2 - 4 => id - 2; id * 2 + 4 > 4 / 2 => id > -1
    void const_pre_calc();
    //参数校验，创建些运行时资源，比如in的map
    virtual int open() {
        for (auto e : _children) {
            int ret = 0;
            ret = e->open();
            if (ret < 0) {
                return ret;
            }
        }
        return 0;
    } 
    virtual ExprValue get_value(MemRow* row) { //对每行计算表达式
        return ExprValue::Null();
    } 
    //释放open创建的资源
    virtual void close() {
        for (auto e : _children) {
            e->close();
        }
    }

    virtual int64_t used_size() {
        int64_t size = sizeof(*this);
        for (auto c : _children) {
            size += c->used_size();
        }
        return size;
    }

    virtual void find_place_holder(std::map<int, ExprNode*>& placeholders) {
        for (size_t idx = 0; idx < _children.size(); ++idx) {
            _children[idx]->find_place_holder(placeholders);
        }
    }

    virtual void replace_slot_ref_to_literal(const std::set<int64_t>& sign_set,
                    std::map<int64_t, std::vector<ExprNode*>>& literal_maps);

    ExprNode* get_slot_ref(int32_t tuple_id, int32_t slot_id);
    ExprNode* get_parent(ExprNode* child);
    void add_child(ExprNode* expr_node) {
        _children.push_back(expr_node);
    }
    bool contains_special_operator(pb::ExprNodeType expr_node_type) {
        bool contain = false;
        recursive_contains_special_operator(expr_node_type, &contain);
        return contain;
    }
    void recursive_contains_special_operator(pb::ExprNodeType expr_node_type, bool* contain) {
        if (_node_type == expr_node_type) {
            *contain = true;
            return;
        }
        for (auto child : _children) {
            child->recursive_contains_special_operator(expr_node_type, contain);
        }
    }

    void replace_child(size_t idx, ExprNode* expr) {
        delete _children[idx];
        _children[idx] = expr;
    }

    void del_child(size_t idx) {
        _children.erase(_children.begin() + idx);
    }
    size_t children_size() {
        return _children.size();
    }
    ExprNode* children(size_t idx) {
        return _children[idx];
    }
    pb::ExprNodeType node_type() {
        return _node_type;
    }
    pb::PrimitiveType col_type() {
        return _col_type;
    }
    void set_col_type(pb::PrimitiveType col_type) {
        _col_type = col_type;
    }
    uint32_t col_flag() {
        return _col_flag;
    }

    void set_charset(pb::Charset charset) {
        _charset = charset;
    }
    pb::Charset charset() {
        return _charset;
    }
    void set_col_flag(uint32_t col_flag) {
        _col_flag = col_flag;
    }

    void flatten_or_expr(std::vector<ExprNode*>* or_exprs) {
        if (node_type() != pb::OR_PREDICATE) {
            or_exprs->push_back(this);
            return;
        }
        for (auto c : _children) {
            c->flatten_or_expr(or_exprs);
        }
    }

    virtual void transfer_pb(pb::ExprNode* pb_node);
    static void create_pb_expr(pb::Expr* expr, ExprNode* root);
    static int create_tree(const pb::Expr& expr, ExprNode** root);
    static void destroy_tree(ExprNode* root) {
        delete root;
    }
    static void get_pb_expr(const pb::Expr& from, int* idx, pb::Expr* to);

    void get_all_tuple_ids(std::unordered_set<int32_t>& tuple_ids);
    void get_all_slot_ids(std::unordered_set<int32_t>& slot_ids);
    void get_all_field_ids(std::unordered_set<int32_t>& field_ids);
    int32_t tuple_id() const {
        return _tuple_id;
    }

    void set_slot_col_type(int32_t tuple_id, int32_t slot_id, pb::PrimitiveType col_type);

    pb::PrimitiveType get_slot_col_type(int32_t slot_id);

    int32_t slot_id() const {
        return _slot_id;
    }

    void disable_replace_agg_to_slot() {
        _replace_agg_to_slot = false;
    }

    void print_expr_info();
    static bvar::Adder<int64_t>  _s_non_boolean_sql_cnts;
protected:
    pb::ExprNodeType _node_type;
    pb::PrimitiveType _col_type = pb::INVALID_TYPE;
    std::vector<ExprNode*> _children;
    uint32_t _col_flag = 0;
    pb::Charset _charset = pb::CS_UNKNOWN;
    bool    _is_constant = true;
    bool    _has_null = false;
    bool    _replace_agg_to_slot = true;
    int32_t _tuple_id = -1;
    int32_t _slot_id = -1;
    bool is_logical_and_or_not();
public:
    static int create_expr_node(const pb::ExprNode& node, ExprNode** expr_node);
private:
    static int create_tree(const pb::Expr& expr, int* idx, ExprNode* parent, ExprNode** root);
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
