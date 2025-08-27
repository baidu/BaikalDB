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

#include <functional>
#include <algorithm>
#include <table_record.h>

#include "proto/reverse.pb.h"

namespace baikaldb {

extern int segment(
        const std::string& word,
        const std::string& pk,
        SmartRecord record,
        pb::SegmentType segment_type,
        const std::map<std::string, int32_t>& name_field_id_map,
        pb::ReverseNodeType flag,
        std::map<std::string, ReverseNode>& res,
        const pb::Charset& charset);

extern int merge_and(ReverseNode& to, const ReverseNode& from, BoolArg* arg);
extern int merge_or(ReverseNode& to, const ReverseNode& from, BoolArg* arg);
extern  int merge_weight(ReverseNode& to, const ReverseNode& from, BoolArg* arg);

// TermBooleanExecutor
// -----------------------
template <typename Schema>
TermBooleanExecutor<Schema>::TermBooleanExecutor(
                            RindexNodeParser<Schema>* list,
                            const std::string& term,
                            bool_executor_type type,
                            BoolArg* arg) {
    _posting_list = list;
    _term = term; 
    this->_type = type;
    this->_arg = arg;
}

template <typename Schema>
TermBooleanExecutor<Schema>::~TermBooleanExecutor() {
    if (_posting_list != NULL) {
        delete _posting_list;
        _posting_list = NULL;
    }
    delete this->_arg;
}

template <typename Schema>
const PostingNodeT* TermBooleanExecutor<Schema>::current_node() {
    return _curr_node_ptr;
}

template <typename Schema>
const PrimaryIdT* TermBooleanExecutor<Schema>::current_id() {
    if (_curr_node_ptr == nullptr) {
        return nullptr;
    }
    return _curr_node_ptr->mutable_key();
}

template <typename Schema>
const PostingNodeT* TermBooleanExecutor<Schema>::next() {
    while (true) {
        if (this->_init_flag) {
            _curr_node_ptr = (PostingNodeT*)_posting_list->current_node();
            this->_init_flag = false;
        } else {
            _curr_node_ptr = (PostingNodeT*)_posting_list->next();
        }
        if (_curr_node_ptr != nullptr) {
            if (_curr_node_ptr->flag() == pb::REVERSE_NODE_DELETE) {
                //如果节点是删除状态，返回下一个
                continue;
            } else if (Schema::filter(*_curr_node_ptr, _arg)) {
                //如果需要过滤，返回下一个
                continue;
            } else if (this->_type == NODE_COPY) {
                _curr_node = *_curr_node_ptr;
                Schema::init_node(_curr_node, _term, _arg);
                _curr_node_ptr = &_curr_node;
            }
        }
        return _curr_node_ptr;
    }
}

template <typename Schema>
const PostingNodeT* TermBooleanExecutor<Schema>::advance(
        const PrimaryIdT& target_id) {
    if (this->_init_flag) {
        this->_init_flag = false;
    }
    if (current_id() == nullptr) {
        return nullptr;
    }
    if (current_id()->compare(target_id) >= 0) {
        return _curr_node_ptr;
    }
    _curr_node_ptr = (PostingNodeT*)_posting_list->advance(target_id);
    if (_curr_node_ptr != nullptr) {
        if (_curr_node_ptr->flag() == pb::REVERSE_NODE_DELETE) {
            //如果节点是删除状态，返回下一个
            return next();
        } else if (Schema::filter(*_curr_node_ptr, _arg)) {
            //如果需要过滤，返回下一个
            return next();
        } else if (this->_type == NODE_COPY) {
            _curr_node = *_curr_node_ptr;
            Schema::init_node(_curr_node, _term, _arg);
            _curr_node_ptr = &_curr_node;
        }
    }
    return _curr_node_ptr;
}
}  // namespace boolean_engine

// vim: set expandtab ts=4 sw=4 sts=4 tw=100: 
