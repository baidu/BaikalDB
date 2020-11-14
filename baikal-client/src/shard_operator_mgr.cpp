// Copyright (c) 2019 Baidu, Inc. All Rights Reserved.
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
 
/**
 * @file baikal_client_table_shard_mgr.cpp
 * @author liuhuicong(com@baidu.com)
 * @date 2015/11/23 15:38:40
 * @brief 
 *  
 **/

#include "shard_operator_mgr.h"
#include "baikal_client_define.h"
#ifdef BAIDU_INTERNAL
#include "com_log.h"
#endif

namespace baikal {
namespace client {
boost::mutex ShardOperatorMgr::_s_singleton_lock;
ShardOperatorMgr* ShardOperatorMgr::_s_instance = NULL;
ShardOperatorMgr* ShardOperatorMgr::get_s_instance() {
    if (_s_instance == NULL) { 
        boost::mutex::scoped_lock lock(_s_singleton_lock);
        if (_s_instance == NULL) { 
            _s_instance = new ShardOperatorMgr();
            _s_instance->_init();
        }
    } 
    return _s_instance;
}

void ShardOperatorMgr::destory_s_instance() {
    delete _s_instance;
    _s_instance = NULL;
}

int ShardOperatorMgr::evaluate(const std::vector<std::string> &expression,
        uint32_t arg,
        uint32_t* result) {
    std::stack<uint32_t> data;
    std::stack<std::string> operators;
    if (expression.size() == 0) {
        CLIENT_WARNING("split expression is empty");
        *result = 0;
        return INPUTPARAM_ERROR;
    }
    for (uint32_t idx = 0; idx < expression.size(); idx++) {
        const std::string &str = expression[idx];
        if (str[0] >= '0' && str[0] <= '9') {
            uint32_t value = 0;
            for (uint32_t cur = 0; cur < str.size(); ++cur) {
                value = value * 10 + (str[cur]-'0');
            }
            data.push(value);
        } else if (str == "$") {
            data.push(arg);
        } else if (str == "(") {
            operators.push(str);
        } else if (str == ")") {
            while (!operators.empty() && operators.top() != "(") {
                if (_compute(data, operators) != SUCCESS) {
                    CLIENT_WARNING("compute error!");
                    return COMPUTE_ERROR;
                }
            }
            operators.pop();
        } else {
            if (_str_priority_map.find(str) != _str_priority_map.end()) {
                while (!operators.empty() &&
                    _get_priority(str) >= _get_priority(operators.top())) {
                    if (_compute(data, operators) != SUCCESS) {
                        CLIENT_WARNING("compute error!");
                        return COMPUTE_ERROR;
                    }
                }
                operators.push(str);
            } else {
                CLIENT_WARNING("unsupported symbol [%s]", expression[idx].c_str());
                return COMPUTE_ERROR;
            }
        }
    }
    while (!operators.empty()) {
        if (_compute(data, operators) != SUCCESS) {
            CLIENT_WARNING("compute error!");
            return COMPUTE_ERROR;
        }
    }
    *result = data.top();
    return SUCCESS;
}

int ShardOperatorMgr::split(const std::string &expression, std::vector<std::string> &res) {
    int len = expression.size();
    for (int idx = 0; idx < len; ++idx) {
        if (expression[idx] == ' ') {
            continue;
        }
        if (expression[idx] == '(' || expression[idx] == ')') {
            res.push_back(expression.substr(idx, 1));
        } else if (expression[idx] == '&') {
            res.push_back("&");
        } else if (expression[idx] == '%') {
            res.push_back("%");
        } else if (expression[idx] == '$') {
            res.push_back("$");
        } else if (expression[idx] == '<') {
            if (idx < len - 1 && expression[idx + 1] == '<') {
                res.push_back("<<");
                idx++;
            } else {
                CLIENT_WARNING("unsupported symbol [%c]", expression[idx]);
                return COMPUTE_ERROR;
            }
        } else if (expression[idx] == '>') {
            if (idx < len - 1 && expression[idx + 1] == '>') {
                res.push_back(">>");
                idx++;
            } else {
                CLIENT_WARNING("unsupported symbol [%s]", expression.c_str());
                return COMPUTE_ERROR;
            }
        } else if (expression[idx] >= '1' && expression[idx] <= '9') {
            int cur = idx;
            while (cur < len && expression[cur] >= '0' && expression[cur] <= '9') {
                cur++;
            }
            res.push_back(expression.substr(idx, cur - idx));
            idx = cur-1;
        } else if (expression[idx] == '+' ||
                expression[idx] == '-' ||
                expression[idx] == '*' ||
                expression[idx] == '/') {
            res.push_back(expression.substr(idx, 1));
        } else {
            CLIENT_WARNING("unsupported symbol [%c]", expression[idx]);
            return COMPUTE_ERROR;
        }
    }
    return SUCCESS;
}

void ShardOperatorMgr::_init() {
    //only the following binary operator are supported
    _str_op_map.insert(std::make_pair("&", SmartOp(new(std::nothrow) BAndOp)));
    _str_op_map.insert(std::make_pair("|",  SmartOp(new(std::nothrow) BOrOp)));
    _str_op_map.insert(std::make_pair(">>", SmartOp(new(std::nothrow) RShiftOp)));
    _str_op_map.insert(std::make_pair("<<", SmartOp(new(std::nothrow) LShiftOp)));
    _str_op_map.insert(std::make_pair("%",  SmartOp(new(std::nothrow) ModOp)));
    _str_op_map.insert(std::make_pair("+",  SmartOp(new(std::nothrow) PlusOp)));
    _str_op_map.insert(std::make_pair("-",  SmartOp(new(std::nothrow) MinusOp)));
    _str_op_map.insert(std::make_pair("*",  SmartOp(new(std::nothrow) MultiplyOp)));
    _str_op_map.insert(std::make_pair("/",  SmartOp(new(std::nothrow) DivOp)));

    _str_priority_map.insert(std::make_pair("%", 3));
    _str_priority_map.insert(std::make_pair("*", 3));
    _str_priority_map.insert(std::make_pair("/", 3));
    _str_priority_map.insert(std::make_pair("+", 4));
    _str_priority_map.insert(std::make_pair("-", 4));
    _str_priority_map.insert(std::make_pair("<<", 5));
    _str_priority_map.insert(std::make_pair(">>", 5));
    _str_priority_map.insert(std::make_pair("&", 8));
    _str_priority_map.insert(std::make_pair("|", 10));
    _str_priority_map.insert(std::make_pair("(", 256));
}

SmartOp ShardOperatorMgr::_get_operator(const std::string &token) {
    if (_str_op_map.find(token) != _str_op_map.end()) {
        return _str_op_map[token];
    }
    return SmartOp();
}

int ShardOperatorMgr::_get_priority(const std::string &op) {
    if (_str_priority_map.find(op) != _str_priority_map.end()) {
        return _str_priority_map[op];
    }
    return -1;
}

int ShardOperatorMgr::_compute(std::stack<uint32_t> &data, std::stack<std::string> &op) {
    if (data.size() < 2 || op.empty()) {
        CLIENT_WARNING("data stack [size=%d] or operator stack [size=%d] is empty.",
            data.size(),
            op.size());

        while (!data.empty()) {
            CLIENT_WARNING("data: [%d]", data.top());
            data.pop();
        }
        while (!op.empty()) {
            CLIENT_WARNING("op: [%s]", op.top().c_str());
            op.pop();
        }
        return COMPUTE_ERROR;
    }
    uint32_t val2 = data.top();
    data.pop();
    uint32_t val1 = data.top();
    data.pop();

    std::string opera = op.top();
    op.pop();

    SmartOp _operator = _get_operator(opera);
    if (!_operator) {
        CLIENT_WARNING("operator [%s] not defined.", opera.c_str());
        return COMPUTE_ERROR;
    }
    data.push((*(_operator.get()))(val1, val2));
    return SUCCESS;
}
} // namespace baseop
} // namespace baikal 

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
