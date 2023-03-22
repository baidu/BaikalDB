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

namespace baikaldb {

template <typename Schema>
BooleanExecutor<Schema>* LogicalQuery<Schema>::create_executor() {
    return parse_executor_node(_root);
}

template <typename Schema>
BooleanExecutor<Schema>* LogicalQuery<Schema>::parse_executor_node(
        const ExecutorNode<Schema>& executor_node) {
    switch (executor_node._type) {
        case TERM   :
            return parse_term_node(executor_node);
        case AND    :
        case OR     :
        case WEIGHT :
            return parse_op_node(executor_node);
        default     :
            DB_WARNING("boolean executor type (%d) is invalid", executor_node._type);
            return NULL;
    }
}

template <typename Schema>
BooleanExecutor<Schema>* LogicalQuery<Schema>::parse_term_node(
        const ExecutorNode<Schema>& node) {
    Parser* parser = new Parser(_schema);
    parser->init(node._term);
    return new TermBooleanExecutor<Schema>(parser, node._term, _schema->executor_type, node._arg);
}

template <typename Schema>
BooleanExecutor<Schema>* LogicalQuery<Schema>::parse_op_node(
        const ExecutorNode<Schema>& node) {
    if (node._sub_nodes.size() == 0) {
        DB_WARNING("sub clauses of OperatorBooleanExecutor[%d] is empty", node._type);
        return NULL;
    } else {
        OperatorBooleanExecutor<Schema>* result = NULL;
        switch (node._type) {
            case AND : {
                result = new AndBooleanExecutor<Schema>(_schema->executor_type, node._arg);
                result->set_merge_func(node._merge_func);
                and_or_add_subnode(node, result);
                break;
            }
            case OR : {
                result = new OrBooleanExecutor<Schema>(_schema->executor_type, node._arg);
                result->set_merge_func(node._merge_func);
                and_or_add_subnode(node, result);
                break;
            }
            case WEIGHT : {
                result = new WeightedBooleanExecutor<Schema>(_schema->executor_type, node._arg);
                result->set_merge_func(node._merge_func);
                weight_add_subnode(node, result);
                break;
            }
            default : {
                DB_WARNING("Executor type[%d] error", node._type);
                return NULL;
            }
        }
        return result;
    }
}

template <typename Schema>
void LogicalQuery<Schema>::and_or_add_subnode(
        const ExecutorNode<Schema>& node,
        OperatorBooleanExecutor<Schema>* result) {
    for (size_t i = 0; i < node._sub_nodes.size(); ++i) {
        const ExecutorNode<Schema>* sub_node = node._sub_nodes[i];
        BooleanExecutor<Schema> *tmp= parse_executor_node(*sub_node);
        if (tmp) {
            result->add(tmp);
        }
    }
}

template <typename Schema>
void LogicalQuery<Schema>::weight_add_subnode(
        const ExecutorNode<Schema>& node,
        OperatorBooleanExecutor<Schema>* result) {
    // weight_node的结构固定，第一个op_node为must, 剩下的node为weigt_term
    WeightedBooleanExecutor<Schema>* weight_result =
            static_cast<WeightedBooleanExecutor<Schema>*>(result);
    const ExecutorNode<Schema>* sub_node = node._sub_nodes[0];
    BooleanExecutor<Schema> *tmp= parse_executor_node(*sub_node);
    if (tmp) {
        weight_result->add_must(tmp);
    }
    for (size_t i = 1; i < node._sub_nodes.size(); ++i) {
        const ExecutorNode<Schema>* sub_node = node._sub_nodes[i];
        BooleanExecutor<Schema> *tmp= parse_executor_node(*sub_node);
        if (tmp) {
            weight_result->add_not_must(tmp);
        }
    }
}

}  // namespace logical_query

// vim: set expandtab ts=4 sw=4 sts=4 tw=100: 
