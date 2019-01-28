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

#include "scalar_fn_call.h"
#include "slot_ref.h"

namespace baikaldb {
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
    return 0;
}

int ScalarFnCall::type_inferer() {
    int ret = 0;
    ret = ExprNode::type_inferer();
    if (ret < 0) {
        return ret;
    }
    std::vector<pb::PrimitiveType> types;
    for (auto c : _children) {
        if (c->col_type() == pb::INVALID_TYPE) {
            DB_WARNING("_children is pb::INVALID_TYPE, node:%d", c->node_type());
            return -1;
        }
        types.push_back(c->col_type());
    }
    ret = FunctionManager::complete_fn(_fn, types);
    if (_col_type == pb::INVALID_TYPE) {
        _col_type = _fn.return_type();
    }
    return 0;
}

// 111 > aaa => aaa < 111
void ScalarFnCall::children_swap() {
    if (_children.size() != 2) {
        return;
    }
    if (_children[0]->is_constant() && _children[1]->is_slot_ref()) {
        FunctionManager* fn_manager = FunctionManager::instance();
        std::string swap_op = fn_manager->get_swap_op(_fn.name());
        if (!swap_op.empty()) {
            _fn.set_name(swap_op);
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
        DB_WARNING("_children.size:%u < _fn.arg_types_size:%d", 
                _children.size(), _fn.arg_types_size());
        return -1;
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
        return -1;
    }
    return 0;
}

ExprValue ScalarFnCall::get_value(MemRow* row) {
    std::vector<ExprValue> args;
    for (auto c : _children) {
        args.push_back(c->get_value(row));
    }
    //类型转化
    for (int i = 0; i < _fn.arg_types_size(); i++) {
        args[i].cast_to(_fn.arg_types(i));
    }
    return _fn_call(args);
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
