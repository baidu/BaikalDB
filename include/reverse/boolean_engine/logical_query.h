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
#include <string>
#include <vector>
#include "boolean_executor.h"

namespace baikaldb {

enum NodeType {
    AND = 1,
    OR,
    WEIGHT,
    TERM
};

class ExecutorNode {
public:
    ~ExecutorNode() {
        for (size_t i = 0; i < _sub_nodes.size(); ++i) {
            delete _sub_nodes[i];
        }
    }
    NodeType _type;
    MergeFuncT _merge_func;
    std::string _term;
    BoolArg *_arg = nullptr;//用在TermNode，传递给parser，由parser释放 
               //用在OperatorNode，传递给OperatorNode，由node释放
    std::vector<ExecutorNode*> _sub_nodes;
};

template <typename Schema>
class LogicalQuery {
public:
    typedef typename Schema::Parser Parser;
    using ReverseListSptr = typename ReverseIndex<Schema>::ReverseListSptr;
    using ReverseList = typename ReverseIndex<Schema>::ReverseList;

    LogicalQuery(Schema *schema, myrocksdb::Transaction* txn, bool is_fast = false)
            : _schema(schema), _txn(txn), _is_fast(is_fast) {
        _rocksdb = RocksWrapper::get_instance();
    }
    ~LogicalQuery(){}
    BooleanExecutor* create_executor();
    ExecutorNode _root;
private:
    BooleanExecutor* parse_executor_node(const ExecutorNode& node);
    BooleanExecutor* parse_term_node(const ExecutorNode& node);
    BooleanExecutor* parse_op_node(const ExecutorNode& node);
    void and_or_add_subnode(const ExecutorNode&, OperatorBooleanExecutor*);
    void weight_add_subnode(const ExecutorNode&, OperatorBooleanExecutor*);
    int _multi_get_reverse_list_two(
        std::vector<ReverseListSptr>& list_new_ptrs,
        std::vector<ReverseListSptr>& list_old_ptrs);

    // input and ouput are both stored in _delay_init_context
    int _multi_get_level_reverse_list(uint8_t level, std::vector<ReverseListSptr>& list_ptrs);

    Schema *_schema;
    bool _is_fast;
    RocksWrapper* _rocksdb;
    myrocksdb::Transaction* _txn;
    std::shared_ptr<ReverseDelayInitContext<Schema>> _delay_init_context = nullptr;
};

} // namespace logical_query

#include "logical_query.hpp"

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
