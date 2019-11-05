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
 * @file shard_operator_mgr.h
 * @author liuhuicong(com@baidu.com)
 * @date 2015/11/23 15:22:09
 * @brief 
 *  
 **/

#ifndef  FC_DBRD_BAIKAL_CLIENT_INCLUDE_SHARD_OPERATOR_MGR_H
#define  FC_DBRD_BAIKAL_CLIENT_INCLUDE_SHARD_OPERATOR_MGR_H

#include "baikal_client_define.h"
#include <stack>
#include <string>
#include <vector>
#include "boost/shared_ptr.hpp"
#include "boost/unordered_map.hpp"

namespace baikal {

namespace client {

class BaseOp {
public:
    virtual uint32_t operator()(const uint32_t &left, const uint32_t &right) = 0;
};

class BAndOp : public BaseOp {
public:
    virtual uint32_t operator()(const uint32_t &left, const uint32_t &right) {
        return left & right;
    }   
};

class BOrOp : public BaseOp {
public:
    virtual uint32_t operator()(const uint32_t &left, const uint32_t &right) {
        return left | right;
    }   
};

class RShiftOp : public BaseOp {
public:
    virtual uint32_t operator()(const uint32_t &left, const uint32_t &right) {
        return left >> right;
    }   
};

class LShiftOp : public BaseOp {
public:
    virtual uint32_t operator()(const uint32_t &left, const uint32_t &right) {
        return left << right;
    }   
};

class ModOp : public BaseOp {
public:
    virtual uint32_t operator()(const uint32_t &left, const uint32_t &right) {
        return left % right;
    }
};

class PlusOp : public BaseOp {
public:
    virtual uint32_t operator()(const uint32_t &left, const uint32_t &right) {
        return left + right;
    }
};

class MinusOp : public BaseOp {
public:
    virtual uint32_t operator()(const uint32_t &left, const uint32_t &right) {
        return left - right;
    }
};

class MultiplyOp : public BaseOp {
public:
    virtual uint32_t operator()(const uint32_t &left, const uint32_t &right) {
        return left * right;
    }
};

class DivOp : public BaseOp {
public:
    virtual uint32_t operator()(const uint32_t &left, const uint32_t &right) {
        return left / right;
    }
};

typedef boost::shared_ptr<BaseOp> SmartOp;

// 单例模式
class ShardOperatorMgr {
public:

    static ShardOperatorMgr* get_s_instance();
    static void destory_s_instance();

    int evaluate(const std::vector<std::string> &expression, uint32_t arg, uint32_t* result);

    int split(const std::string &expression, std::vector<std::string> &res);

private:

    ShardOperatorMgr() {}
    void _init();
    SmartOp _get_operator(const std::string &token);
    int _get_priority(const std::string &op);
    int _compute(std::stack<uint32_t> &data, std::stack<std::string> &op);

private:

    // 创建单例类
    static ShardOperatorMgr* _s_instance;
    static boost::mutex _s_singleton_lock;
    boost::unordered_map<std::string, SmartOp> _str_op_map;
    boost::unordered_map<std::string, int> _str_priority_map;
};

}

} 

#endif  //FC_DBRD_BAIKAL_CLIENT_INCLUDE_SHARD_OPERATOR_MGR_H

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
