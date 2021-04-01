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
#ifdef BAIDU_INTERNAL
template <typename OUT, typename IN>
int Tokenizer::nlpc_seg(drpc::NLPCClient& client, 
             const std::string& word, 
             OUT& s_output,
             IN& s_input)
{
    if (word.empty()) {
        return -1;
    }

    std::string str_input;
    //TimeCost tt;
    //序列化输入结构
    if (!s_input->to_binary(&str_input))
    {
        DB_WARNING("wordrank serialize failed");
        return -1;
    } 
    // sync method call
    std::string str_output;
    int ret = client.call_method(str_input, str_output);
    if (ret != 0)
    {
        DB_WARNING("wordrank call method failed ret:%d", ret);
        return -1;
    }
    //DB_WARNING("call :%ld", tt.get_time());
    //tt.reset();
    //反序列化输出
    if (!s_output->from_binary(str_output))
    {
        DB_WARNING("wordrank deserialize failed");
        return -1;
    }
    //DB_WARNING("parse :%ld", tt.get_time());
    return 0;
}
#endif

template<typename ReverseNode, typename ReverseList>
int FirstLevelMSIterator<ReverseNode, ReverseList>::next(std::string& key, bool& res) {
    int ret = 0;

    while (_need_next && _node_dq.size() < 2) {
        ReverseNode node;
        ret = internal_next(&node, _need_next);
        if (ret < 0) {
            return -1;
        }

        if (!_need_next) {
            break;
        }

        if (_node_dq.empty()) {
            _node_dq.emplace_back(node);
            continue;
        }

        if (node.key() == _node_dq.back().key()) {
            _node_dq.pop_back();
            _node_dq.emplace_back(node);
            continue;
        } else {
            _node_dq.emplace_back(node);
            break;
        }
    }

    res = true;
    if (_node_dq.size() > 1) {
        _curr_node = _node_dq.front();
        _node_dq.pop_front();
        key = _curr_node.key();
        return 0;
    }

    if (!_need_next) {
        if (!_node_dq.empty()) {
            _curr_node = _node_dq.front();
            _node_dq.pop_front();
            key = _curr_node.key();
        } else {
            res = false;
        }
    }

    return 0;
}

template<typename ReverseNode, typename ReverseList>
int FirstLevelMSIterator<ReverseNode, ReverseList>::internal_next(ReverseNode* node, bool& res) {
    //当key >= _end_key时该term的拉链实际已经结束，但是merge的时候，所有的term
    //的拉链顺序在一起，所以需要把当前term的拉链遍历完，才能成功访问后续term
    std::string key;
    do {
        if (!_first) {
            _iter->Next();
        } else {
            _first = false;
        }
        bool end_flag = is_prefix_end(_iter, _prefix);
        if (end_flag) {
            res = false;
            return 0;
        }
        std::string term = get_term_from_reverse_key(_iter->key());
        if (term != _merge_term) {
            res = false;
            return 0;
        }
        res = true;
        rocksdb::Status s;
        rocksdb::ReadOptions read_opt;
        rocksdb::PinnableSlice pin_slice;
        auto data_cf = _rocksdb->get_data_handle();
        s = _txn->GetForUpdate(read_opt, data_cf, _iter->key(), &pin_slice);
        if (!s.ok()) {
           DB_WARNING("get for update failed:%s, term:%s, key:%s", s.ToString().c_str(), 
                     term.c_str(), _iter->key().ToString(true).c_str());
           return -1;
        }
        //rocksdb::Slice pin_slice = _iter->value();
        
        if (!node->ParseFromArray(pin_slice.data(), pin_slice.size())) {
            DB_FATAL("parse first level from pb failed");
            return -1;
        }
        key = node->key();
        
        if (_del) {
            auto remove_res = _txn->Delete(data_cf, _iter->key());
            if (!remove_res.ok()) {
                DB_WARNING("rocksdb delete error: code=%d, msg=%s",
                        remove_res.code(), remove_res.ToString().c_str());
                return -1;
            }
            ++g_statistic_delete_key_num;
        }
    } while ((!_key_range.first.empty() && key < _key_range.first) || 
                   (!_key_range.second.empty() && key >= _key_range.second)); 
    return 0;
}

template<typename ReverseNode, typename ReverseList>
void FirstLevelMSIterator<ReverseNode, ReverseList>::fill_node(ReverseNode* node) {
    *node = _curr_node;
    return;
}

template<typename ReverseNode, typename ReverseList>
pb::ReverseNodeType FirstLevelMSIterator<ReverseNode, ReverseList>::get_flag() {
    return _curr_node.flag();
}

template<typename ReverseNode, typename ReverseList>
ReverseNode& FirstLevelMSIterator<ReverseNode, ReverseList>::get_value() {
    return _curr_node;
}

template<typename ReverseNode, typename ReverseList>
int SecondLevelMSIterator<ReverseNode, ReverseList>::next(std::string& key, bool& res) {
    while (true) {
        if (!_first) {
            _index++;
        } else {
            _first = false;
        }
        if (_index < _list.reverse_nodes_size()) {
            //DB_WARNING("get %d index reverse node list size[%d]", _index, _list.reverse_nodes_size());
            res = true;
            key = (_list.mutable_reverse_nodes(_index))->key();
            while (_index + 1 < _list.reverse_nodes_size()) {
                if (key == _list.mutable_reverse_nodes(_index + 1)->key()) {
                    _index++;
                } else {
                    break;
                }
            }

            //key = _list.reverse_nodes(_index).key();
        } else {
            res = false;
            return 0;
        }
        if (!_key_range.first.empty() && key < _key_range.first) {
            continue;
        }
        //2层或者3层拉链是一个term独立的，当key>=end_key时，便结束
        if (!_key_range.second.empty() && key >= _key_range.second) {
            res = false;
        }
        return 0;
    }
}

template<typename ReverseNode, typename ReverseList>
void SecondLevelMSIterator<ReverseNode, ReverseList>::fill_node(ReverseNode* node) {
    *node = _list.reverse_nodes(_index);
    return;
}

template<typename ReverseNode, typename ReverseList>
pb::ReverseNodeType SecondLevelMSIterator<ReverseNode, ReverseList>::get_flag() {
    return _list.reverse_nodes(_index).flag();
}

template<typename ReverseNode, typename ReverseList>
ReverseNode& SecondLevelMSIterator<ReverseNode, ReverseList>::get_value() {
    return *(_list.mutable_reverse_nodes(_index));
}

template<typename ReverseNode, typename ReverseList>
int level_merge(MergeSortIterator<ReverseNode, ReverseList>* new_iter,
                MergeSortIterator<ReverseNode, ReverseList>* old_iter,
                ReverseList& res_list,
                bool is_del) {
    std::string new_key;
    std::string old_key;
    bool new_not_end;
    bool old_not_end;
    int ret = 0;
    ret = new_iter->next(new_key, new_not_end);
    if (ret < 0) {
        return -1;
    }
    ret = old_iter->next(old_key, old_not_end);
    if (ret < 0) {
        return -1;
    }
    int result_count = 0;
    while (true) {
        if (new_not_end && old_not_end) {
            MergeSortIterator<ReverseNode, ReverseList>* choose_iter;
            int res = new_key.compare(old_key);
            if (res < 0) {
                choose_iter = new_iter;
            } else if (res > 0) {
                choose_iter = old_iter;
            } else if (res == 0) {
                choose_iter = new_iter;
            }
            pb::ReverseNodeType flag = choose_iter->get_flag();
            if (!(is_del && (flag == pb::REVERSE_NODE_DELETE))) {
                choose_iter->add_node(res_list);
                ++result_count;
            }
            if (res < 0) {
                ret = new_iter->next(new_key, new_not_end);
                if (ret < 0) {
                    return -1;
                }
            } else if (res == 0) {               
                ret = new_iter->next(new_key, new_not_end);
                if (ret < 0) {
                    return -1;
                }
                ret = old_iter->next(old_key, old_not_end);
                if (ret < 0) {
                    return -1;
                }
            } else if (res > 0) {
                ret = old_iter->next(old_key, old_not_end);
                if (ret < 0) {
                    return -1;
                }
            }
            continue;
        } else if (new_not_end) {
            pb::ReverseNodeType flag = new_iter->get_flag();
            if (!(is_del && (flag == pb::REVERSE_NODE_DELETE))) {
                new_iter->add_node(res_list);
                ++result_count;
            }
            ret = new_iter->next(new_key, new_not_end);
            if (ret < 0) {
                return -1;
            }
            continue;
        } else if (old_not_end) {
            pb::ReverseNodeType flag = old_iter->get_flag();
            if (!(is_del && (flag == pb::REVERSE_NODE_DELETE))) {
                old_iter->add_node(res_list);
                ++result_count;
            }
            ret = old_iter->next(old_key, old_not_end);
            if (ret < 0) {
                return -1;
            }
            continue;
        } else {
            break;
        }
    }
    ReverseTrait<ReverseList>::finish(res_list);
    return result_count;
}

} // end of namespace
