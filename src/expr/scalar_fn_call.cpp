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

#include "scalar_fn_call.h"
#include "slot_ref.h"
#include "row_expr.h"
#include "literal.h"
#include "parser.h"

namespace baikaldb {
DEFINE_bool(open_nonboolean_sql_forbid, false, "open nonboolean sqls forbid default:false");
int ScalarFnCall::init(const pb::ExprNode& node) {
    int ret = 0;
    ret = ExprNode::init(node);
    if (ret < 0) {
        return ret;
    }
    //scalar函数默认是true，只有参数中有非const的才是false
    _is_constant = true;
    if (!node.has_fn()) {
        return -1;
    }
    _fn = node.fn();
    // rand不是const
    if (node_type() == pb::FUNCTION_CALL && _fn.name() == "rand") {
        _is_constant = false;
    }
    return 0;
}

int ScalarFnCall::type_inferer() {
    int ret = 0;
    ret = ExprNode::type_inferer();
    if (ret < 0) {
        return ret;
    }
    if (_fn_call != NULL) {
        // 避免重复执行后续逻辑
        return ret;
    }
    // 兼容mysql， predicate 处理成列的类型
    switch (_fn.fn_op()) { 
        case parser::FT_EQ:
        case parser::FT_IN:
        case parser::FT_NE:
        case parser::FT_GE:
        case parser::FT_GT:
        case parser::FT_LE:
        case parser::FT_LT: {
            if (_children[0]->is_slot_ref() && _children[1]->is_constant()) {
                for (size_t i = 1; i < children_size(); i++) {
                    _children[i]->set_col_type(_children[0]->col_type());
                }
            } else if (_children[0]->is_row_expr() && !_children[0]->is_constant() && _children[1]->is_constant()) {
                std::map<size_t, SlotRef*> slots;
                static_cast<RowExpr*>(_children[0])->get_all_slot_ref(&slots);
                for (uint32_t i = 1; i < children_size(); i++) {
                    for (auto& pair : slots) {
                        size_t idx = pair.first;
                        pb::PrimitiveType tp = pair.second->col_type();
                        _children[i]->children(idx)->set_col_type(tp);
                    }
                }
            }
            break;
        } 
        default:
            break;
    }
    std::vector<pb::PrimitiveType> types;
    for (auto c : _children) {
        if (c->col_type() == pb::INVALID_TYPE && !c->is_row_expr()) {
            DB_WARNING("_children is pb::INVALID_TYPE, node:%d", c->node_type());
            return -1;
        }
        if (is_logical_and_or_not()) {
            //类型推导过程中分析表达式节点类型是否为bool型
            if (c->col_type() != pb::BOOL) {
                DB_WARNING("_children is not bool type, ScalarFnCall ExprNode type is [%s], children node_type_is [%s]", 
                    pb::ExprNodeType_Name(_node_type).c_str(),
                    pb::ExprNodeType_Name(c->node_type()).c_str());
                    ExprNode::_s_non_boolean_sql_cnts << 1;
                if (FLAGS_open_nonboolean_sql_forbid) {
                    return NOT_BOOL_ERRCODE;
                }
            }
        }
        types.push_back(c->col_type());
    }
    ret = FunctionManager::complete_fn(_fn, types);

    if (_col_type == pb::INVALID_TYPE) {
        _col_type = _fn.return_type();
    }

    // Literal type cast
    for (int i = 0; i < _fn.arg_types_size(); i++) {
        if (_children[i]->is_literal()) {
            static_cast<Literal*>(_children[i])->cast_to_col_type(_fn.arg_types(i));
        }
    }
    return 0;
}

// 111 > aaa => aaa < 111 
// (11,22) < (a,b) => (a,b) > (11,22)
void ScalarFnCall::children_swap() {
    if (_children.size() != 2) {
        return;
    }
    if (_children[0]->is_constant() && !_children[1]->is_constant() &&
        (_children[1]->is_slot_ref() || _children[1]->is_row_expr())) {
        FunctionManager* fn_manager = FunctionManager::instance();
        if (fn_manager->swap_op(_fn)) {
            std::swap(_children[0], _children[1]);
        }
    }
}

int ScalarFnCall::open() {
    int ret = 0;
    ret = ExprNode::open();
    if (ret < 0) {
        DB_WARNING("ExprNode::open fail:%d", ret);
        return ret;
    }
    if ((int)_children.size() < _fn.arg_types_size()) {
        DB_WARNING("_children.size:%lu < _fn.arg_types_size:%d", 
                _children.size(), _fn.arg_types_size());
        return -1;
    }
    if (children_size() > 0 && children(0)->is_row_expr()) {
        if (_fn.fn_op() != parser::FT_EQ &&
            _fn.fn_op() != parser::FT_NE &&
            _fn.fn_op() != parser::FT_GE &&
            _fn.fn_op() != parser::FT_GT &&
            _fn.fn_op() != parser::FT_LE &&
            _fn.fn_op() != parser::FT_LT && 
            _fn.fn_op() != parser::FT_MATCH_AGAINST) {
            DB_FATAL("Operand should contain 1 column(s)");
            return -1;
        }
        size_t col_size = children(0)->children_size();
        if (_fn.fn_op() == parser::FT_MATCH_AGAINST) {
            if (col_size > 1) {
                DB_FATAL("MATCH_AGAINST column list support only 1, size:%lu", col_size);
                return -1;
            }
            return 0;
        }
        _is_row_expr = true;
        for (size_t i = 1; i < children_size(); i++) {
            if (!children(i)->is_row_expr() ||
                children(i)->children_size() != col_size) {
                DB_FATAL("Operand should contain %lu column(s)", col_size);
                return -1;
            }
        }
    }
    /*
    if (_fn.return_type() != _col_type) {
        DB_WARNING("_fn.return_type:%d != _col_type:%d", _fn.return_type(), _col_type);
        return -1;
    }
    for (int i = 0; i < _fn.arg_types_size(); i++) {
        if (_fn.arg_types(i) != _children[i]._col_type) {
            return -1;
        }
    }*/
    FunctionManager* fn_manager = FunctionManager::instance();
    _fn_call = fn_manager->get_object(_fn.name());
    if (node_type() == pb::FUNCTION_CALL && _fn_call == NULL) {
        DB_WARNING("fn call is null, name:%s", _fn.name().c_str());
    }
    return 0;
}

ExprValue ScalarFnCall::get_value(MemRow* row) {
    if (_is_row_expr) {
        switch (_fn.fn_op()) {
            case parser::FT_EQ:
                return multi_eq_value(row);
            case parser::FT_NE:
                return multi_ne_value(row);
            case parser::FT_GE:
                return multi_ge_value(row);
            case parser::FT_GT:
                return multi_gt_value(row);
            case parser::FT_LE:
                return multi_le_value(row);
            case parser::FT_LT:
                return multi_lt_value(row);
            default:
                return ExprValue::Null();
        }
    }
    if (_fn_call == NULL) {
        return ExprValue::Null();
    }
    std::vector<ExprValue> args;
    for (auto c : _children) {
        args.emplace_back(c->get_value(row));
    }
    //类型转化
    for (int i = 0; i < _fn.arg_types_size(); i++) {
        args[i].cast_to(_fn.arg_types(i));
    }
    return _fn_call(args).cast_to(_col_type);
}

ExprValue ScalarFnCall::get_value(const ExprValue& value) {
    if (_is_row_expr) {
        return ExprValue::Null();
    }
    if (_fn_call == NULL) {
        return ExprValue::Null();
    }
    std::vector<ExprValue> args;
    for (auto c : _children) {
        args.emplace_back(c->get_value(value));
    }
    //类型转化
    for (int i = 0; i < _fn.arg_types_size(); i++) {
        args[i].cast_to(_fn.arg_types(i));
    }
    return _fn_call(args).cast_to(_col_type);
}

}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
