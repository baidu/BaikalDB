
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

#include "proto/reverse.pb.h"
#include "table_record.h"
#include "slot_ref.h"

namespace baikaldb {

template<typename Schema>
int CommRindexNodeParser<Schema>::init(const std::string& term) {
    auto* exist_parser = this->_schema->get_term(term);
    if (exist_parser != NULL) {
        *this = *exist_parser;
        return 0;
    }
    this->_schema->get_reverse_list(term, _new_list_ptr, _old_list_ptr);
    _new_list = (ReverseList*)_new_list_ptr.get();
    _old_list = (ReverseList*)_old_list_ptr.get();
    _curr_node = nullptr;
    if (_new_list != nullptr && _new_list->reverse_nodes_size() > 0) {
        _list_size_new = _new_list->reverse_nodes_size();
        _curr_ix_new = 0;
        _curr_id_new = _new_list->mutable_reverse_nodes(0)->mutable_key();
        _curr_node = _new_list->mutable_reverse_nodes(0);
        _cmp_res = -1;
    } else {
        _curr_ix_new = -1;
        _cmp_res = 1;
    }
    if (_old_list != nullptr && _old_list->reverse_nodes_size() > 0) {
        _list_size_old = _old_list->reverse_nodes_size();
        _curr_ix_old = 0;
        _curr_id_old = _old_list->mutable_reverse_nodes(0)->mutable_key();
        if (_curr_ix_new != -1) {
            _cmp_res = _curr_id_new->compare(*_curr_id_old);
            if (_cmp_res > 0) {
                _curr_node = _old_list->mutable_reverse_nodes(0);
            } 
        } else {
            _curr_node = _old_list->mutable_reverse_nodes(0);
        }
    } else {
        _curr_ix_old = -1;
    }
    if (!_key_range.first.empty()) {
        advance(_key_range.first);
    }
    this->_schema->set_term(term, this);
    return 0;
} 

template<typename Schema>
const typename Schema::ReverseNode* CommRindexNodeParser<Schema>::current_node() {
    return _curr_node; 
}

template<typename Schema>
const typename Schema::PrimaryIdT* CommRindexNodeParser<Schema>::current_id() {
    if (_curr_node == nullptr) {
        return nullptr;
    }
    return _curr_node->mutable_key(); 
}

template<typename Schema>
const typename Schema::ReverseNode* CommRindexNodeParser<Schema>::next() {
    if (_curr_node == nullptr) {
        return _curr_node;
    } else {
        if (_cmp_res < 0) {
            _curr_ix_new++;
        } else if (_cmp_res == 0) {
            _curr_ix_new++;
            _curr_ix_old++;
        } else {
            _curr_ix_old++;
        }

        if (_curr_ix_new >= _list_size_new) {
            _curr_ix_new = -1;
        } else {
            _curr_id_new = _new_list->mutable_reverse_nodes(_curr_ix_new)->mutable_key();
        }
        if (_curr_ix_old >= _list_size_old) {
            _curr_ix_old = -1;
        } else {
            _curr_id_old = _old_list->mutable_reverse_nodes(_curr_ix_old)->mutable_key();
        }

        if (_curr_ix_new != -1 && _curr_ix_old != -1) {
            _cmp_res = _curr_id_new->compare(*_curr_id_old);
        } else if (_curr_ix_new != -1 && _curr_ix_old == -1) {
            _cmp_res = -1;
        } else if (_curr_ix_new == -1 && _curr_ix_old != -1){
            _cmp_res = 1;
        } else {
            _curr_node = nullptr;
            return _curr_node;
        }
        
        if (_cmp_res <= 0) { 
            _curr_node = _new_list->mutable_reverse_nodes(_curr_ix_new);
        } else {
            _curr_node = _old_list->mutable_reverse_nodes(_curr_ix_old);
        }
        if (!_key_range.second.empty() && _curr_node->key() >= _key_range.second) {
            _curr_node = nullptr;
        }  
        return _curr_node; 
    }
}

template<typename Schema>
uint32_t CommRindexNodeParser<Schema>::binary_search(uint32_t first,
                                               uint32_t last,
                                               const std::string& target_id,
                                               ReverseList* list) {
    if (first > last) {
        return -1;
    }
    //针对倒排链表特征的优化，缩小二分查找的区间
    uint32_t j = 1;
    uint32_t node_count_off = last - first;
    while (j <= node_count_off  && target_id.compare(list->reverse_nodes(first + j).key()) > 0) {
        j <<= 1;
    }
    last = first + std::min(j, node_count_off);
    first = first + (j >> 1);
    //二分查找
    int res = target_id.compare(list->reverse_nodes(last).key());
    if (res > 0) {
        return -1;
    }
    uint32_t mid = 0;
    while (first < last) {
        mid = first + ((last - first) >> 1);
        const std::string& mid_id = list->reverse_nodes(mid).key();
        res = target_id.compare(mid_id);
        if (res < 0) {
            last = mid;
        } else if (res > 0) {
            first = mid + 1;
        } else {
            return mid;
        }
    }
    return first;
}

template<typename Schema>
const typename Schema::ReverseNode*    
                CommRindexNodeParser<Schema>::advance(const std::string& target_id) {
    if (_curr_node == nullptr) {
        return _curr_node;
    } else {
        if (_curr_ix_new != -1) {
            _curr_ix_new = binary_search(_curr_ix_new, _list_size_new - 1, target_id, _new_list);
        }
        if (_curr_ix_new != -1) {
            _curr_id_new = _new_list->mutable_reverse_nodes(_curr_ix_new)->mutable_key();
        }

        if (_curr_ix_old != -1) {
            _curr_ix_old = binary_search(_curr_ix_old, _list_size_old - 1, target_id, _old_list);
        }
        if (_curr_ix_old != -1) {
            _curr_id_old = _old_list->mutable_reverse_nodes(_curr_ix_old)->mutable_key();
        }

        if (_curr_ix_new != -1 && _curr_ix_old != -1) {
            _cmp_res = _curr_id_new->compare(*_curr_id_old);
        } else if (_curr_ix_new != -1 && _curr_ix_old == -1) {
            _cmp_res = -1;
        } else if (_curr_ix_new == -1 && _curr_ix_old != -1){
            _cmp_res = 1;
        } else {
            _curr_node = nullptr;
            return _curr_node;
        }
        
        if (_cmp_res <= 0) { 
            _curr_node = _new_list->mutable_reverse_nodes(_curr_ix_new);
        } else {
            _curr_node = _old_list->mutable_reverse_nodes(_curr_ix_old);
        }
        
        if (!_key_range.second.empty() && _curr_node->key() >= _key_range.second) {
            _curr_node = nullptr;
        }  
        return _curr_node; 
    }
}

} // end of namespace
