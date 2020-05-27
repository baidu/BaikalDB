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

#include "expr_node.h"
#include "literal.h"
#include "predicate.h"
#include "scalar_fn_call.h"
#include "agg_fn_call.h"
#include "slot_ref.h"
#include "row_expr.h"

namespace baikaldb {
// only pre_calc children nodes
void ExprNode::const_pre_calc() {
    if (_children.size() == 0 || _node_type == pb::AGG_EXPR) {
        return;
    }
    //标量函数RowExpr会走到这
    _is_constant = true;
    for (auto& c : _children) {
        c->const_pre_calc();
        if (!c->_is_constant) {
            _is_constant = false;
        }
    }
    //const表达式等着父节点来替换
    //root是const表达式则外部替换
    if (!is_row_expr() && is_constant()) {
        return;
    }
    int ret = 0;
    //把constant表达式计算成literal
    for (auto& c : _children) {
        if (c->is_row_expr()) {
            continue;
        }
        if (!c->is_constant()) {
            continue;
        }
        if (c->is_literal()) {
            continue;
        }
        // place holder被替换会导致下一次exec参数对不上
        if (c->has_place_holder()) {
            continue;
        }
        //替换,常量表达式优先类型推导
        ret = c->type_inferer();
        if (ret < 0) {
            return;
        }
        ret = c->open();
        if (ret < 0) {
            return;
        }
        ExprValue value = c->get_value(nullptr);
        c->close();
        delete c;
        c = new Literal(value);
    }
    // 111 > aaa => aaa < 111
    children_swap();
    //把agg expr替换成slot ref
    for (auto& c : _children) {
        if (c->node_type() == pb::AGG_EXPR) {
            c->type_inferer();
            ExprNode* slot = static_cast<AggFnCall*>(c)->create_slot_ref();
            delete c;
            c = slot;
        }
    }
}

ExprNode* ExprNode::get_slot_ref(int32_t tuple_id, int32_t slot_id) {
    if (_node_type == pb::SLOT_REF) {
        if (static_cast<SlotRef*>(this)->tuple_id() == tuple_id &&
                static_cast<SlotRef*>(this)->slot_id() == slot_id) {
            return this;
        }
    }
    for (auto c : _children) {
        ExprNode* node = c->get_slot_ref(tuple_id, slot_id);
        if (node != nullptr) {
            return node;
        }
    }
    return nullptr;
}

ExprNode* ExprNode::get_parent(ExprNode* child) {
    if (this == child) {
        return nullptr;
    }
    for (auto c : _children) {
        if (child == c) {
            return this;
        } else {
            ExprNode* node = c->get_parent(child);
            if (node != nullptr) {
                return node;
            }
        }
    }
    return nullptr;
}

void ExprNode::get_all_tuple_ids(std::unordered_set<int32_t>& tuple_ids) {
    if (_node_type == pb::SLOT_REF) {
        tuple_ids.insert(static_cast<SlotRef*>(this)->tuple_id());
    }
    for (auto& child : _children) {
        child->get_all_tuple_ids(tuple_ids);
    }
}

void ExprNode::get_all_slot_ids(std::unordered_set<int32_t>& slot_ids) {
    if (_node_type == pb::SLOT_REF) {
        slot_ids.insert(static_cast<SlotRef*>(this)->slot_id());
    }
    for (auto& child : _children) {
        child->get_all_slot_ids(slot_ids);
    }
}

void ExprNode::get_all_field_ids(std::unordered_set<int32_t>& field_ids) {
    if (_node_type == pb::SLOT_REF) {
        field_ids.insert(static_cast<SlotRef*>(this)->field_id());
    }
    for (auto& child : _children) {
        child->get_all_field_ids(field_ids);
    }
}


void ExprNode::transfer_pb(pb::ExprNode* pb_node) {
    pb_node->set_node_type(_node_type);
    pb_node->set_col_type(_col_type);
    pb_node->set_num_children(_children.size());
}

void ExprNode::create_pb_expr(pb::Expr* expr, ExprNode* root) {
    if (root->_index_ids.size() > 0) {
        for (auto index : root->_index_ids) {
            expr->add_index_ids(index);
        }
    }
    pb::ExprNode* pb_node = expr->add_nodes();
    root->transfer_pb(pb_node);
    for (size_t i = 0; i < root->children_size(); i++) {
        create_pb_expr(expr, root->children(i));
    }
}

int ExprNode::create_tree(const pb::Expr& expr, ExprNode** root) {
    int ret = 0;
    int idx = 0;
    if (expr.nodes_size() == 0) {
        *root = nullptr;
        return 0;
    }
    ret = create_tree(expr, &idx, nullptr, root);
    if (ret < 0) {
        return -1;
    }
    return 0;
}

int ExprNode::create_tree(const pb::Expr& expr, int* idx, ExprNode* parent, ExprNode** root) {
    if (*idx >= expr.nodes_size()) {
        DB_FATAL("idx %d > size %d", *idx, expr.nodes_size());
        return -1;
    }
    int num_children = expr.nodes(*idx).num_children();
    ExprNode* expr_node = nullptr;
    int ret = 0;
    ret = create_expr_node(expr.nodes(*idx), &expr_node);
    if (ret < 0) {
        DB_FATAL("create_expr_node fail");
        return ret;
    }
    if (*idx == 0 && expr.index_ids_size() > 0) {
        for (auto index : expr.index_ids()) {
             expr_node->add_filter_index(index);
        }
    }
    if (parent != nullptr) {
        parent->add_child(expr_node);
    } else if (root != nullptr) {
        *root = expr_node;
    } else {
        DB_FATAL("parent is null");
        delete expr_node;
        return -1;
    }
    for (int i = 0; i < num_children; i++) {
        ++(*idx);
        ret = create_tree(expr, idx, expr_node, nullptr);
        if (ret < 0) {
            return ret;
        }
    }
    return 0;
}

int ExprNode::create_expr_node(const pb::ExprNode& node, ExprNode** expr_node) {
    switch (node.node_type()) {
        case pb::SLOT_REF:
            *expr_node = new SlotRef;
            (*expr_node)->init(node);
            return 0;
        case pb::NULL_LITERAL:
        case pb::BOOL_LITERAL:
        case pb::INT_LITERAL:
        case pb::DOUBLE_LITERAL:
        case pb::STRING_LITERAL:
        case pb::HLL_LITERAL:
        case pb::DATE_LITERAL:
        case pb::DATETIME_LITERAL:
        case pb::TIME_LITERAL:
        case pb::TIMESTAMP_LITERAL:
        case pb::PLACE_HOLDER_LITERAL:
            *expr_node = new Literal;
            (*expr_node)->init(node);
            return 0;
        case pb::NOT_PREDICATE:
            *expr_node = new NotPredicate;
            (*expr_node)->init(node);
            return 0;
        case pb::AND_PREDICATE:
            *expr_node = new AndPredicate;
            (*expr_node)->init(node);
            return 0;
        case pb::OR_PREDICATE:
            *expr_node = new OrPredicate;
            (*expr_node)->init(node);
            return 0;
        case pb::XOR_PREDICATE:
            *expr_node = new XorPredicate;
            (*expr_node)->init(node);
            return 0;
        case pb::IN_PREDICATE:
            *expr_node = new InPredicate;
            (*expr_node)->init(node);
            return 0;
        case pb::IS_NULL_PREDICATE:
            *expr_node = new IsNullPredicate;
            (*expr_node)->init(node);
            return 0;
        case pb::IS_TRUE_PREDICATE:
            *expr_node = new IsTruePredicate;
            (*expr_node)->init(node);
            return 0;
        case pb::LIKE_PREDICATE:
            *expr_node = new LikePredicate;
            (*expr_node)->init(node);
            return 0;
        case pb::FUNCTION_CALL:
            *expr_node = new ScalarFnCall;
            (*expr_node)->init(node);
            return 0;
        case pb::AGG_EXPR:
            *expr_node = new AggFnCall;
            (*expr_node)->init(node);
            return 0;
        case pb::ROW_EXPR:
            *expr_node = new RowExpr;
            (*expr_node)->init(node);
            return 0;
        default:
            //unsupport expr
            DB_FATAL("unsupport node type: %d", node.node_type());
            return -1;
    }
    return -1;
}

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
