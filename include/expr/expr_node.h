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

#include <vector>
#include <unordered_set>
#include "expr_value.h"
#include "mem_row.h"
#include "proto/expr.pb.h"

namespace baikaldb {
class ExprNode {
public:
    ExprNode() : _is_constant(true) {}
    virtual ~ExprNode() {
        for (auto& e : _children) {
            delete e;
            e = nullptr;
        }
    }
    virtual int init(const pb::ExprNode& node) {
        _node_type = node.node_type();
        _col_type = node.col_type();
        return 0;
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

    bool is_literal() {
        switch (_node_type) {
            case pb::NULL_LITERAL:
            case pb::BOOL_LITERAL:
            case pb::INT_LITERAL:
            case pb::DOUBLE_LITERAL:
            case pb::STRING_LITERAL:
                return true;
            default:
                return false;
        }
        return false;
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
    bool is_constant() const {
        return _is_constant;
    }

    void add_filter_index(int64_t index_id) {
        _index_ids.insert(index_id);
    }

    bool contained_by_index(std::vector<int64_t> index_ids) {
        for (auto index_id : index_ids) {
            if (_index_ids.count(index_id) == 1) {
                return true;
            }
        }
        return false;
    }

    virtual void transfer_pb(pb::ExprNode* pb_node);
    static void create_pb_expr(pb::Expr* expr, ExprNode* root);
    static int create_tree(const pb::Expr& expr, ExprNode** root);
    static void destory_tree(ExprNode* root) {
        delete root;
    }
    void get_all_tuple_ids(std::unordered_set<int32_t>& tuple_ids);
    void get_all_slot_ids(std::unordered_set<int32_t>& slot_ids);
protected:
    pb::ExprNodeType _node_type;
    pb::PrimitiveType _col_type;
    std::vector<ExprNode*> _children;
    bool     _is_constant;

    // 过滤条件对应的index_id值，用于过滤条件剪枝使用
    std::unordered_set<int64_t> _index_ids;
    
private:
    static int create_expr_node(const pb::ExprNode& node, ExprNode** expr_node);
    static int create_tree(const pb::Expr& expr, int* idx, ExprNode* parent, ExprNode** root);
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
