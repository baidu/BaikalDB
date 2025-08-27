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
#include "boolean_executor.h"

namespace baikaldb {
int compare_id_func(const PrimaryIdT& id1, const PrimaryIdT& id2) {
    return id1.compare(id2);
}

// AndBooleanExecutor
// ------------------
AndBooleanExecutor::AndBooleanExecutor(bool_executor_type type, BoolArg* arg) {
    this->_is_null_flag = false;
    //默认设置merge_and函数，可以动态更改
    this->set_merge_func(merge_and);
    this->_type = type;
    this->_curr_node_ptr = &this->_curr_node;
    this->_curr_id_ptr = &this->_curr_id;
    this->_arg = arg;
}

AndBooleanExecutor::~AndBooleanExecutor() {
    // do nothing
    delete this->_arg;
}

const PostingNodeT* AndBooleanExecutor::current_node() {
    if (this->_is_null_flag) {
        return NULL;
    }
    return this->_curr_node_ptr;
}

const PrimaryIdT* AndBooleanExecutor::current_id() {
    if (this->_is_null_flag) {
        return NULL;
    }
    return this->_curr_id_ptr;
}

const PostingNodeT* AndBooleanExecutor::next() {
    if (this->_sub_clauses.size() == 0 || this->_is_null_flag) {
        this->_is_null_flag = true;
        return NULL;
    }
    if (this->_init_flag) {
        this->_init_flag = false;
        for (auto sub : this->_sub_clauses) {
            if (sub->next() == NULL) {
                this->_is_null_flag = true;
                return NULL;
            }
        }
    } else {
        this->_sub_clauses[this->_sub_clauses.size() - 1]->next();
    }
    return find_next();
}

const PostingNodeT* AndBooleanExecutor::advance(
        const PrimaryIdT& target_id) {
    if (this->_sub_clauses.size() == 0 || this->_is_null_flag) {
        this->_is_null_flag = true;
        return NULL;
    }
    if (this->_init_flag) {
        this->_init_flag = false;
        for (auto sub : this->_sub_clauses) {
            if (sub->advance(target_id) == NULL) {
                this->_is_null_flag = true;
                return NULL;
            }
        }
    } else if (this->current_id()->compare(target_id) >= 0) {
        return this->current_node();
    } else {
        this->_sub_clauses[this->_sub_clauses.size() - 1]->advance(target_id);
    }
    return find_next();
}

const PostingNodeT* AndBooleanExecutor::find_next() {
    uint32_t forward_idx = 0;
    uint32_t pivot_idx = this->_sub_clauses.size() - 1;
    const PostingNodeT* tmp = this->_sub_clauses[pivot_idx]->current_node();
    if (tmp == NULL) {
        this->_is_null_flag = true;
        return NULL;
    }
    this->_curr_id_ptr = this->_sub_clauses[pivot_idx]->current_id();
    while(1) {
        BooleanExecutor*& forward_exec = this->_sub_clauses[forward_idx];
        if (forward_idx != pivot_idx
                && (NULL != forward_exec->advance(*this->_curr_id_ptr))) {
            if (*forward_exec->current_id() != *this->_curr_id_ptr) {
                this->_curr_id_ptr = forward_exec->current_id();
                pivot_idx = forward_idx;
            }

            forward_idx = ((forward_idx == this->_sub_clauses.size() - 1) ? 0 : forward_idx + 1);
            continue;
        }

        if (forward_exec->current_id() == NULL) {
            this->_is_null_flag = true;
        }
        if (this->_is_null_flag) {
            return NULL;
        }
        //merge
        if (this->_type == NODE_COPY) {
            this->_curr_node = *this->_sub_clauses[0]->current_node();
            this->_curr_id = *this->_sub_clauses[0]->current_id();
        }
        if (this->_type == NODE_NOT_COPY) {
            this->_curr_node_ptr = (PostingNodeT*)this->_sub_clauses[0]->current_node();
            this->_curr_id_ptr = this->_sub_clauses[0]->current_id();
        }
        for (size_t i = 1; i < this->_sub_clauses.size(); ++i) {
            this->_merge_func(*this->_curr_node_ptr, *this->_sub_clauses[i]->current_node(), this->_arg);
        }

        return this->_curr_node_ptr;
    }
}


// OrBooleanExecutor
// ------------------
OrBooleanExecutor::OrBooleanExecutor(bool_executor_type type, BoolArg* arg) {
    this->_is_null_flag = false;
    this->set_merge_func(merge_or);
    this->_type = type;
    this->_curr_node_ptr = &this->_curr_node;
    this->_curr_id_ptr = &this->_curr_id;
    this->_arg = arg;
}


OrBooleanExecutor::~OrBooleanExecutor() {
    // do nothing
    delete this->_arg;
}

const PostingNodeT* OrBooleanExecutor::current_node() {
    if (this->_is_null_flag) {
        return NULL;
    }
    return this->_curr_node_ptr;
}

const PrimaryIdT* OrBooleanExecutor::current_id() {
    if (this->_is_null_flag) {
        return NULL;
    }
    return this->_curr_id_ptr;
}

const PostingNodeT* OrBooleanExecutor::next() {
    std::vector<BooleanExecutor*>& clauses = this->_sub_clauses;
    if (clauses.size() == 0 || this->_is_null_flag) {
        this->_is_null_flag = true;
        return NULL;
    }
    if (this->_init_flag) {
        for (auto sub : clauses) {
            sub->next();
        }
        make_heap();
        this->_init_flag = false;
    } else {
        (*clauses.begin())->next();
        shiftdown(0);
    }
    return find_next();
}

const PostingNodeT* OrBooleanExecutor::advance(
        const PrimaryIdT& target_id) {
    std::vector<BooleanExecutor*>& clauses = this->_sub_clauses;
    if (clauses.size() == 0 || this->_is_null_flag) {
        this->_is_null_flag = true;
        return NULL;
    }
    if (this->_init_flag) {
        this->_init_flag = false;
    } else if (this->current_id() == nullptr || this->current_id()->compare(target_id) >= 0) {
        return this->current_node();
    }
    while ((*clauses.begin())->current_id() != nullptr && (*clauses.begin())->current_id()->compare(target_id) < 0) {
        (*clauses.begin())->advance(target_id);
        shiftdown(0);
    }
    return find_next();
}

const PostingNodeT* OrBooleanExecutor::find_next() {
    std::vector<BooleanExecutor*>& clauses = this->_sub_clauses;

    auto min_iter = clauses.begin();

    const PrimaryIdT* min_id = (*min_iter)->current_id();
    if (NULL == min_id) {
        this->_is_null_flag = true;
        return NULL;
    }
    if (this->_type == NODE_COPY) {
        this->_curr_node = *(*min_iter)->current_node();
        this->_curr_id = *(*min_iter)->current_id();
    }
    if (this->_type == NODE_NOT_COPY) {
        this->_curr_node_ptr = (PostingNodeT*)(*min_iter)->current_node();
        this->_curr_id_ptr = (*min_iter)->current_id();
    }

    if (clauses.size() > 1) {
        for (int i = 0; i < clauses.size(); ++i) {
            if (clauses[1]->current_id() != NULL &&
                    compare_id_func(*(clauses[1]->current_id()), *this->_curr_id_ptr) == 0) {
                this->_merge_func(*this->_curr_node_ptr, *clauses[1]->current_node(), this->_arg);
                clauses[1]->next();
                shiftdown(1);
            } else {
                break;
            }
        }
    }
    if (clauses.size() > 2) {
        for (int i = 0; i < clauses.size(); ++i) {
            if (clauses[2]->current_id() != NULL &&
                    compare_id_func(*(clauses[2]->current_id()), *this->_curr_id_ptr) == 0) {
                this->_merge_func(*this->_curr_node_ptr, *clauses[2]->current_node(), this->_arg);
                clauses[2]->next();
                shiftdown(2);
            } else {
                break;
            }
        }
    }
    return this->_curr_node_ptr;
}

void OrBooleanExecutor::make_heap() {
    for (int i = static_cast<int>(this->_sub_clauses.size()) / 2 - 1; i >= 0; i--) {
        shiftdown(i);
    }
}

void OrBooleanExecutor::shiftdown(size_t index) {
    std::vector<BooleanExecutor*>& clauses = this->_sub_clauses;
    size_t left_index = index * 2 + 1;
    size_t right_index = left_index + 1;
    if (left_index >= clauses.size()) {
        return;
    }
    size_t min_index = index;
    if (left_index < clauses.size() &&
            CompareAsc()(clauses[left_index], clauses[min_index])) {
        min_index = left_index;
    }
    if (right_index < clauses.size() &&
            CompareAsc()(clauses[right_index], clauses[min_index])) {
        min_index = right_index;
    }
    if (min_index != index) {
        std::iter_swap(clauses.begin() + min_index, clauses.begin() + index);
        shiftdown(min_index);
    }
}


// WeightedBooleanExecutor
// ------------------
WeightedBooleanExecutor::WeightedBooleanExecutor(bool_executor_type type, BoolArg* arg) :
        _op_executor(NULL) {
    this->_is_null_flag = false;
    this->set_merge_func(merge_weight);
    this->_type = type;
    this->_curr_node_ptr = &this->_curr_node;
    this->_curr_id_ptr = &this->_curr_id;
    this->_arg = arg;
}

WeightedBooleanExecutor::~WeightedBooleanExecutor() {
    delete _op_executor;
    delete this->_arg;
}

const PostingNodeT* WeightedBooleanExecutor::current_node() {
    if (this->_is_null_flag) {
        return NULL;
    }
    return this->_curr_node_ptr;
}

const PrimaryIdT* WeightedBooleanExecutor::current_id() {
    if (this->_is_null_flag) {
        return NULL;
    }
    return this->_curr_id_ptr;
}

const PostingNodeT* WeightedBooleanExecutor::next() {
    if (_op_executor == NULL || this->_is_null_flag) {
        this->_is_null_flag = true;
        return NULL;
    }
    const PostingNodeT* tmp = _op_executor->next();
    if (tmp == NULL) {
        this->_is_null_flag = true;
        return NULL;
    }
    if (this->_type == NODE_COPY) {
        this->_curr_node = *_op_executor->current_node();
        this->_curr_id = *_op_executor->current_id();
    }
    if (this->_type == NODE_NOT_COPY) {
        this->_curr_node_ptr = (PostingNodeT*)_op_executor->current_node();
        this->_curr_id_ptr = _op_executor->current_id();
    }
    add_weight();
    return this->_curr_node_ptr;
}

const PostingNodeT* WeightedBooleanExecutor::advance(
        const PrimaryIdT& target_id) {
    if (_op_executor == NULL || this->_is_null_flag) {
        this->_is_null_flag = true;
        return NULL;
    }
    const PostingNodeT* tmp = _op_executor->advance(target_id);
    if (tmp == NULL) {
        this->_is_null_flag = true;
        return NULL;
    }
    if (this->_type == NODE_COPY) {
        this->_curr_node = *_op_executor->current_node();
        this->_curr_id = *_op_executor->current_id();
    }
    if (this->_type == NODE_NOT_COPY) {
        this->_curr_node_ptr = (PostingNodeT*)_op_executor->current_node();
        this->_curr_id_ptr = _op_executor->current_id();
    }
    add_weight();
    return this->_curr_node_ptr;
}

void WeightedBooleanExecutor::add_not_must(
        BooleanExecutor* executor) {
    executor->next();
    (this->_sub_clauses).emplace_back(executor);
}

void WeightedBooleanExecutor::add_must(
        BooleanExecutor* executor) {
    _op_executor = executor;
}

void WeightedBooleanExecutor::add_weight() {
    std::vector<BooleanExecutor*>& sub_clauses = this->_sub_clauses;
    for (uint32_t i = 0; i < sub_clauses.size(); i++) {
        const PrimaryIdT* id_tmp = sub_clauses[i]->current_id();
        if (id_tmp == NULL) {
            continue;
        }
        int cmp_res = compare_id_func(*id_tmp, *this->_curr_id_ptr);
        if (cmp_res > 0) {
            continue;
        } else if (cmp_res == 0) {
            this->_merge_func(*this->_curr_node_ptr, *sub_clauses[i]->current_node(), this->_arg);
        } else {
            sub_clauses[i]->advance(*this->_curr_id_ptr);
            id_tmp = sub_clauses[i]->current_id();
            if (id_tmp == NULL) {
                continue;
            }
            if (compare_id_func(*(sub_clauses[i]->current_id()), *this->_curr_id_ptr) == 0) {
                this->_merge_func(*this->_curr_node_ptr, *sub_clauses[i]->current_node(), this->_arg);
            }
        }
    }
}
}