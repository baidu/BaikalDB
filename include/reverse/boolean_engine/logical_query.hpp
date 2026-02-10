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
BooleanExecutor* LogicalQuery<Schema>::create_executor() {
    return parse_executor_node(_root);
}

template <typename Schema>
BooleanExecutor* LogicalQuery<Schema>::parse_executor_node(
        const ExecutorNode& executor_node) {
    switch (executor_node._type) {
        case TERM   :
            return parse_term_node(executor_node);
        case AND    :
        case OR     :
        case WEIGHT : {
            bool need_delay_init = false;
            if (_is_fast && _delay_init_context == nullptr) {
                // 只在fast模式下进行延迟初始化
                _delay_init_context = std::make_shared<ReverseDelayInitContext<Schema>>();
                need_delay_init = true;
            }
            BooleanExecutor* ret = parse_op_node(executor_node);
            if (need_delay_init) {
                std::vector<ReverseListSptr> list_new_ptrs(_delay_init_context->children.size(), nullptr);
                std::vector<ReverseListSptr> list_old_ptrs(_delay_init_context->children.size(), nullptr);
                if (_multi_get_reverse_list_two(list_new_ptrs, list_old_ptrs) != 0) {
                    DB_WARNING("multi get reverse list failed");
                    return nullptr;
                }
                for (int i = 0; i < _delay_init_context->children.size(); i++) {
                    int r = _delay_init_context->children[i]->delay_init(
                            _delay_init_context->terms[i],
                            list_new_ptrs[i],
                            list_old_ptrs[i]);
                    if (r != 0) {
                        DB_FATAL("init logicalQuery failed.");
                        return nullptr;
                    }
                }
            }
            return ret;
        }
        default     :
            DB_WARNING("boolean executor type (%d) is invalid", executor_node._type);
            return NULL;
    }
}

template <typename Schema>
BooleanExecutor* LogicalQuery<Schema>::parse_term_node(const ExecutorNode& node) {
    Parser* parser = new Parser(_schema);
    parser->init(node._term, _delay_init_context.get());
    return new TermBooleanExecutor<Schema>(parser, node._term, _schema->executor_type, node._arg);
}

template <typename Schema>
BooleanExecutor* LogicalQuery<Schema>::parse_op_node(
        const ExecutorNode& node) {
    if (node._sub_nodes.size() == 0) {
        DB_WARNING("sub clauses of OperatorBooleanExecutor[%d] is empty", node._type);
        return NULL;
    } else {
        OperatorBooleanExecutor* result = NULL;
        switch (node._type) {
            case AND : {
                result = new AndBooleanExecutor(_schema->executor_type, node._arg);
                result->set_merge_func(node._merge_func);
                and_or_add_subnode(node, result);
                break;
            }
            case OR : {
                result = new OrBooleanExecutor(_schema->executor_type, node._arg);
                result->set_merge_func(node._merge_func);
                and_or_add_subnode(node, result);
                break;
            }
            case WEIGHT : {
                result = new WeightedBooleanExecutor(_schema->executor_type, node._arg);
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
        const ExecutorNode& node,
        OperatorBooleanExecutor* result) {
    for (size_t i = 0; i < node._sub_nodes.size(); ++i) {
        const ExecutorNode* sub_node = node._sub_nodes[i];
        BooleanExecutor *tmp= parse_executor_node(*sub_node);
        if (tmp) {
            result->add(tmp);
        }
    }
}
template <typename Schema>
int LogicalQuery<Schema>::_multi_get_reverse_list_two(
        std::vector<ReverseListSptr>& list_new_ptrs,
        std::vector<ReverseListSptr>& list_old_ptrs) {
    if (_delay_init_context == nullptr) {
        return 0;
    }
    rocksdb::ReadOptions roptions;
    roptions.prefix_same_as_start = true;
    roptions.fill_cache = false;
    auto data_cf = _rocksdb->get_data_handle();
    if (data_cf == nullptr) {
        DB_WARNING("get rocksdb data column family failed");
        return -1;
    }

    if (_is_fast) {
        _multi_get_level_reverse_list(2, list_new_ptrs);
    } else {
        // should not reach here
    }
    _multi_get_level_reverse_list(3, list_old_ptrs);
    return 0;
}

template <typename Schema>
int LogicalQuery<Schema>::_multi_get_level_reverse_list(uint8_t level, std::vector<ReverseListSptr>& list_ptrs) {
    std::vector<size_t> complete_key_idx(_delay_init_context->reverse_rocksdb_keys.size(), 0);
    std::vector<rocksdb::Slice> rocksdb_keys;
    rocksdb_keys.reserve(complete_key_idx.size());
    std::vector<std::string> rocks_key_string;
    rocks_key_string.reserve(complete_key_idx.size());
    std::unordered_map<std::string, size_t> key_index_map;

    for (size_t i = 0; i < _delay_init_context->reverse_rocksdb_keys.size(); ++i) {
        std::string key = _delay_init_context->reverse_rocksdb_keys[i];
        key.append((char*)&level, sizeof(uint8_t));
        key.append(_delay_init_context->terms[i]);
        if (key_index_map.find(key) == key_index_map.end()) {
            rocks_key_string.emplace_back(key);
            rocksdb_keys.emplace_back(rocks_key_string.back());
            key_index_map[key] = rocksdb_keys.size() - 1;
        }
        complete_key_idx[i] = key_index_map[key];
    }

    rocksdb::ReadOptions roptions;
    auto data_cf = _rocksdb->get_data_handle();
    if (data_cf == nullptr) {
        DB_WARNING("get rocksdb data column family failed");
        return -1;
    }

    std::vector<rocksdb::Status> statuses(rocksdb_keys.size());
    std::vector<rocksdb::PinnableSlice> values(rocksdb_keys.size());
    _txn->MultiGet(roptions, data_cf, rocksdb_keys, values, statuses, false);
    std::vector<ReverseListSptr> reverse_list_ptrs(statuses.size(), nullptr);
    for (int i = 0; i < statuses.size(); ++i) {
        if (statuses[i].ok()) {
            ReverseListSptr tmp_ptr(new ReverseList());
            if (!tmp_ptr->ParseFromString(values[i].ToString())) {
                DB_FATAL("parse second level list from pb/arrow failed");
                return -1;
            }
            reverse_list_ptrs[i] = tmp_ptr;
        } else if (statuses[i].IsNotFound()) {

        } else {
            DB_WARNING("rocksdb get error: code=%d, msg=%s",
                    statuses[i].code(), statuses[i].ToString().c_str());
            return -1;
        }
    }
    list_ptrs.resize(complete_key_idx.size());
    for (int i = 0; i < complete_key_idx.size(); ++i) {
        list_ptrs[i] = reverse_list_ptrs[complete_key_idx[i]];
    }
    return 0;
}

template <typename Schema>
void LogicalQuery<Schema>::weight_add_subnode(
        const ExecutorNode& node,
        OperatorBooleanExecutor* result) {
    // weight_node的结构固定，第一个op_node为must, 剩下的node为weigt_term
    WeightedBooleanExecutor* weight_result =
            static_cast<WeightedBooleanExecutor*>(result);
    if (node._sub_nodes.size() == 0) {
        return;
    }
    const ExecutorNode* sub_node = node._sub_nodes[0];
    BooleanExecutor *tmp= parse_executor_node(*sub_node);
    if (tmp) {
        weight_result->add_must(tmp);
    }
    for (size_t i = 1; i < node._sub_nodes.size(); ++i) {
        const ExecutorNode* sub_node = node._sub_nodes[i];
        BooleanExecutor *tmp= parse_executor_node(*sub_node);
        if (tmp) {
            weight_result->add_not_must(tmp);
        }
    }
}

}  // namespace logical_query

// vim: set expandtab ts=4 sw=4 sts=4 tw=100: 
