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

template <typename Schema>
class ExecutorNode {
public:
    typedef typename Schema::PostingNodeT PostingNodeT;
    typedef typename Schema::PrimaryIdT PrimaryIdT;
    typedef int (*MergeFuncT)(PostingNodeT&, const PostingNodeT&, BoolArg*);

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
    std::vector<ExecutorNode<Schema>*> _sub_nodes;
};

template <typename Schema>
class LogicalQuery {
public:
    typedef typename Schema::PostingNodeT PostingNodeT;
    typedef typename Schema::PrimaryIdT PrimaryIdT;
    typedef typename Schema::Parser Parser;
    LogicalQuery(Schema *schema) : _schema(schema) {}
    ~LogicalQuery(){}
    BooleanExecutor<Schema>* create_executor();  
    ExecutorNode<Schema> _root;
private:
    BooleanExecutor<Schema>* parse_executor_node(const ExecutorNode<Schema>& node);
    BooleanExecutor<Schema>* parse_term_node(const ExecutorNode<Schema>& node);
    BooleanExecutor<Schema>* parse_op_node(const ExecutorNode<Schema>& node);
    void and_or_add_subnode(const ExecutorNode<Schema>&, OperatorBooleanExecutor<Schema>*);
    void weight_add_subnode(const ExecutorNode<Schema>&, OperatorBooleanExecutor<Schema>*);
    Schema *_schema;
};

} // namespace logical_query

#include "logical_query.hpp"

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
