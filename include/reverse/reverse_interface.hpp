
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

        if (_curr_ix_new == -1 || _curr_ix_new >= _list_size_new) {
            _curr_ix_new = -1;
        } else {
            _curr_id_new = _new_list->mutable_reverse_nodes(_curr_ix_new)->mutable_key();
        }
        if (_curr_ix_old == -1 || _curr_ix_old >= _list_size_old) {
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
                                               const PrimaryIdT& target_id,
                                               ReverseList* list) {
    if (first > last) {
        return -1;
    }
    //针对倒排链表特征的优化，缩小二分查找的区间
    uint32_t j = 1;
    uint32_t node_count_off = last - first;
    while (j <= node_count_off  && target_id.compare(
        ReverseTrait<typename Schema::ReverseList>::get_reverse_key(*list, first + j)) > 0) {
        j <<= 1;
    }
    last = first + std::min(j, node_count_off);
    first = first + (j >> 1);
    //二分查找
    int res = target_id.compare(ReverseTrait<typename Schema::ReverseList>::get_reverse_key(*list, last));
    if (res > 0) {
        return -1;
    }
    uint32_t mid = 0;
    while (first < last) {
        mid = first + ((last - first) >> 1);
        res = target_id.compare(ReverseTrait<typename Schema::ReverseList>::get_reverse_key(*list, mid));
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
                CommRindexNodeParser<Schema>::advance(const PrimaryIdT& target_id) {
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

//--common interface
template<typename Node, typename List>
int NewSchema<Node, List>::segment(
                    const std::string& word, 
                    const std::string& pk,
                    SmartRecord record,
                    pb::SegmentType segment_type,
                    const std::map<std::string, int32_t>& name_field_id_map,
                    pb::ReverseNodeType flag,
                    std::map<std::string, ReverseNode>& res,
                    const pb::Charset& charset) {
    // hit seg_cache, replace pk and flag
    if (res.size() > 0) {
        for (auto& pair : res) {
            pair.second.set_key(pk);
            pair.second.set_flag(flag);
        }
        return 0;
    }
    std::map<std::string, float> term_map;
    int ret = 0;
    switch (segment_type) {
        case pb::S_NO_SEGMENT:
            term_map[word] = 0;
            break;
        case pb::S_UNIGRAMS:
            ret = Tokenizer::get_instance()->simple_seg(word, 1, term_map, charset);
            break;
        case pb::S_BIGRAMS:
            ret = Tokenizer::get_instance()->simple_seg(word, 2, term_map, charset);
            break;
        case pb::S_ES_STANDARD:
            ret = Tokenizer::get_instance()->es_standard(word, term_map, charset);
            break;
#ifdef BAIDU_INTERNAL
        case pb::S_WORDRANK: 
            ret = Tokenizer::get_instance()->wordrank(word, term_map, charset);
            break;
        case pb::S_WORDRANK_Q2B_ICASE: 
            ret = Tokenizer::get_instance()->wordrank_q2b_icase(word, term_map, charset);
            break;
        case pb::S_WORDRANK_Q2B_ICASE_UNLIMIT: 
            ret = Tokenizer::get_instance()->wordrank_q2b_icase_unlimit(word, term_map, charset);
            break;
        case pb::S_WORDSEG_BASIC: 
            ret = Tokenizer::get_instance()->wordseg_basic(word, term_map, charset);
            break;
        case pb::S_WORDWEIGHT: 
            ret = Tokenizer::get_instance()->wordweight(word, term_map, charset, true);
            break;
        case pb::S_WORDWEIGHT_NO_FILTER: 
            ret = Tokenizer::get_instance()->wordweight(word, term_map, charset, false);
            break;
        case pb::S_WORDWEIGHT_NO_FILTER_SAME_WEIGHT: 
            ret = Tokenizer::get_instance()->wordweight(word, term_map, charset, false, true);
            break;
#endif
        default:
            DB_WARNING("un-support segment:%d", segment_type);
            ret = -1;
            break;
    }
    if (ret < 0) {
        return -1;
    }
    for (auto& pair : term_map) {
        ReverseNode node;
        node.set_key(pk);
        node.set_flag(flag);
        node.set_weight(pair.second);
        res[pair.first] = node;
    }
    return 0;
}

template<typename Node, typename List>
int NewSchema<Node, List>::create_executor(const std::string& search_data, 
    pb::MatchMode mode, pb::SegmentType segment_type, const pb::Charset& charset) {
    _weight_field = get_field_info_by_name(_table_info.fields, "__weight");
    _query_words_field = get_field_info_by_name(_table_info.fields, "__querywords");
    //segment
    TimeCost timer;
    std::vector<std::string> or_search;
    //DB_WARNING("zero create_exe search_data[%s]", search_data.c_str());
    // 先用规则支持 or 操作
    // TODO 用bison来支持mysql bool查询

    if (mode == pb::M_NARUTAL_LANGUAGE) {
        or_search.push_back(search_data);
    } else if (mode == pb::M_BOOLEAN) {
        // mysql boolean模式，空格表示'或'
        Tokenizer::get_instance()->split_str(search_data, or_search, ' ', charset);
    } else {
        // 报告需求，like语法用|表示'或'
        Tokenizer::get_instance()->split_str(search_data, or_search, '|', charset);
    }
    LogicalQuery<ThisType> logical_query(this);
    ExecutorNode<ThisType>* parent = nullptr;
    ExecutorNode<ThisType>* root = &logical_query._root;
    if (or_search.size() == 0) {
        _exe = NULL;
        return 0;
    } else if (or_search.size() == 1) {
        // 兼容mysql ngram Parser，自然语言是or，boolean是and
        // https://dev.mysql.com/doc/refman/8.0/en/fulltext-search-ngram.html
        if (mode == pb::M_NARUTAL_LANGUAGE) {
            root->_type = OR;
            root->_merge_func = ThisType::merge_or;
        } else {
            root->_type = AND;
            root->_merge_func = ThisType::merge_and;
        }
    } else {
        root->_type = OR;
        root->_merge_func = ThisType::merge_or;
        parent = root;
    }
    for (auto& or_item : or_search) {
        std::map<std::string, float> term_map;
        std::vector<std::string> and_terms;
        int ret = 0;
        switch (segment_type) {
            case pb::S_NO_SEGMENT:
                term_map[or_item] = 0;
                break;
            case pb::S_UNIGRAMS:
                ret = Tokenizer::get_instance()->simple_seg(or_item, 1, term_map, charset);
                break;
            case pb::S_BIGRAMS:
                ret = Tokenizer::get_instance()->simple_seg(or_item, 2, term_map, charset);
                break;
            case pb::S_ES_STANDARD:
                ret = Tokenizer::get_instance()->es_standard(or_item, term_map, charset);
                break;
#ifdef BAIDU_INTERNAL
            case pb::S_WORDRANK: 
                ret = Tokenizer::get_instance()->wordrank(or_item, term_map, charset);
                break;
            case pb::S_WORDRANK_Q2B_ICASE: 
                ret = Tokenizer::get_instance()->wordrank_q2b_icase(or_item, term_map, charset);
                break;
            case pb::S_WORDRANK_Q2B_ICASE_UNLIMIT: 
                ret = Tokenizer::get_instance()->wordrank_q2b_icase_unlimit(or_item, term_map, charset);
                break;
            case pb::S_WORDSEG_BASIC: 
                ret = Tokenizer::get_instance()->wordseg_basic(or_item, term_map, charset);
                break;
            case pb::S_WORDWEIGHT:
                ret = Tokenizer::get_instance()->wordweight(or_item, term_map, charset, true);
                break;
            case pb::S_WORDWEIGHT_NO_FILTER:
                ret = Tokenizer::get_instance()->wordweight(or_item, term_map, charset, false);
                break;
            case pb::S_WORDWEIGHT_NO_FILTER_SAME_WEIGHT: 
                ret = Tokenizer::get_instance()->wordweight(or_item, term_map, charset, false, true);
                break;
#endif
            default:
                DB_WARNING("un-support segment:%d", segment_type);
                ret = -1;
                break;
        }
        if (ret < 0) {
            DB_WARNING("[word:%s]segment error %d", or_item.c_str(), ret);
            return -1;
        }
        if (term_map.size() == 0) {
            continue;
        } 
        ExecutorNode<ThisType>* and_node = nullptr;
        if (parent != nullptr) {
            and_node = new ExecutorNode<ThisType>();
            and_node->_type = AND;
            and_node->_merge_func = CommonSchema::merge_and;
        } else {
            and_node = root;
        }
        if (term_map.size() == 1) {
            and_node->_type = TERM;
            and_node->_term = term_map.begin()->first;               
            _query_words = term_map.begin()->first;
        } else {
            for (auto& pair : term_map) {
                auto sub_node = new ExecutorNode<ThisType>();
                sub_node->_type = TERM;
                sub_node->_term = pair.first;
                and_node->_sub_nodes.push_back(sub_node);
                _query_words += pair.first;
                _query_words += ";";
            }
        }
        if (parent != nullptr) {
            parent->_sub_nodes.push_back(and_node);
        }
    }
    if (_query_words.size() > 0 && _query_words.back() == ';') {
        _query_words.pop_back();
    }
    DB_DEBUG("query_words : %s", _query_words.c_str());
    _statistic.segment_time += timer.get_time();
    timer.reset();
    _exe = logical_query.create_executor();
    _statistic.create_exe_time += timer.get_time();
    return 0;
}

template<typename Node, typename List>
int NewSchema<Node, List>::next(SmartRecord record) {
    if (!_cur_node) {
        return -1;
    }
    const Node& reverse_node = *_cur_node;
    int ret = record->decode_key(_index_info, reverse_node.key());
    if (ret < 0) {
        return -1;
    }
    try {
        if (_weight_field != nullptr) {
            auto field = record->get_field_by_idx(_weight_field->pb_idx);
            if (field != nullptr) {
                MessageHelper::set_float(field, record->get_raw_message(), reverse_node.weight());
            }
        }
        if (_query_words_field != nullptr) {
            auto field = record->get_field_by_idx(_query_words_field->pb_idx);
            if (field != nullptr) {
                MessageHelper::set_string(field, record->get_raw_message(), _query_words);
            }
        }
    } catch (std::exception& exp) {
        DB_FATAL("pack weight or query words expection %s", exp.what());
    }
    return 0;
}
} // end of namespace
