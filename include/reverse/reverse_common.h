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
#include "reverse_arrow.h"
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <sstream>
#include <iomanip>
#include "proto/reverse.pb.h"
#include "rocks_wrapper.h"
#include "key_encoder.h"
#include "lru_cache.h"
#include "boolean_executor.h"
#ifdef BAIDU_INTERNAL
#include <nlpc/ver_1_0_0/wordseg_input.h>
#include <nlpc/ver_1_0_0/wordrank_output.h>
#include <nlpc/ver_1_0_0/wordseg_output.h>
#include <nlpc_client.h>
#endif

namespace baikaldb {
typedef std::shared_ptr<google::protobuf::Message> MessageSP;
typedef std::pair<std::string, std::string> KeyRange;
extern std::atomic_long g_statistic_insert_key_num;
extern std::atomic_long g_statistic_delete_key_num;

#ifdef BAIDU_INTERNAL
extern drpc::NLPCClient* wordrank_client;
extern drpc::NLPCClient* wordseg_client;
#endif
class Tokenizer {
public:
    static Tokenizer* get_instance() {
        static Tokenizer _instance;
        return &_instance;
    }
    int init();

#ifdef BAIDU_INTERNAL
    template <typename OUT>
        int nlpc_seg(drpc::NLPCClient& client, 
                const std::string& word, 
                OUT& s_output);
    int wordrank(std::string word, std::map<std::string, float>& term_map);
    int wordrank_q2b_icase(std::string word, std::map<std::string, float>& term_map);
    int wordseg_basic(std::string word, std::map<std::string, float>& term_map);
#endif
    int q2b_tolower_gbk(std::string& word);
    int es_standard_gbk(std::string word, std::map<std::string, float>& term_map);
    int simple_seg_gbk(std::string word, uint32_t word_count, std::map<std::string, float>& term_map);
    void split_str_gbk(const std::string& word, std::vector<std::string>& split_word, char delim);
private:
    Tokenizer() {};
    void normalization_gbk(std::string& word);
    void normalization_utf8(std::string& word);
    std::unordered_set<std::string> _punctuation_blank;
    std::unordered_map<std::string, std::string> _q2b_gbk;
    std::unordered_map<std::string, std::string> _q2b_utf8;
};

//自动管理原子对象
template<class T>
class AtomicManager {
public:
    AtomicManager():_atomic(NULL) {}
    ~AtomicManager() {
        if (_atomic) {
            (*_atomic)--;
        }
    }
    void set(T* value) {
        _atomic = value;
        (*_atomic)++;
    }
    T* _atomic;
};

/*

 *倒排链表分为三层，每层的存储形式不一样
 *MergeSortIterator作为倒排链表的中间层，屏蔽底层的差异性
 */
template<typename ReverseNode, typename ReverseList>
class MergeSortIterator{
public:
    virtual ~MergeSortIterator() {}
    //获取下一个元素的id，id存储在key
    //res为true，存在，res为false，无数据
    virtual int next(std::string& key, bool& res) = 0;
    //填充元素
    virtual void fill_node(ReverseNode* node) = 0;
    //获取元素的删除标记
    virtual pb::ReverseNodeType get_flag() = 0;
    //获取元素
    virtual ReverseNode& get_value() = 0;

    virtual void add_node(ReverseList&) = 0;
};
/*
 *第一层倒排链表的抽象，ReverseNode是分散的形式
 */
template<typename ReverseNode, typename ReverseList>
class FirstLevelMSIterator : public MergeSortIterator<ReverseNode, ReverseList> {
public:
    FirstLevelMSIterator(
                       std::unique_ptr<rocksdb::Iterator>& iter,
                       uint8_t prefix,
                       const KeyRange& key_range,
                       const std::string& merge_term,
                       bool del = false, 
                       RocksWrapper* rocksdb = NULL,
                       rocksdb::Transaction* txn = NULL) : 
                           _iter(iter),
                           _merge_term(merge_term),
                           _first(true),
                           _prefix(prefix),
                           _key_range(key_range),
                           _del(del),
                           _rocksdb(rocksdb),
                           _txn(txn) {}
    virtual int next(std::string& key, bool& res);
    virtual void fill_node(ReverseNode* node);
    virtual pb::ReverseNodeType get_flag();
    virtual ReverseNode& get_value();
    virtual ~FirstLevelMSIterator() {}
    virtual void add_node(ReverseList& res_list) {
        ReverseNode* tmp_node = res_list.add_reverse_nodes();
        fill_node(tmp_node);
    }
private:
    std::unique_ptr<rocksdb::Iterator>& _iter;
    const std::string& _merge_term;
    bool _first;
    ReverseNode _curr_node;
    uint8_t _prefix;
    KeyRange _key_range;
    bool _del;
    RocksWrapper* _rocksdb;
    rocksdb::Transaction* _txn;
};

//add_node对arrow进行特化
template<>
inline void FirstLevelMSIterator<ArrowReverseNode, ArrowReverseList>::add_node(ArrowReverseList& res_list) {
    res_list.add_node(_curr_node.key(), _curr_node.flag(), _curr_node.weight());
}
/*
 *第二/三层倒排链表的抽象，ReverseNode是有序数组的形式
 */
template<typename ReverseNode, typename ReverseList>
class SecondLevelMSIterator : public MergeSortIterator<ReverseNode, ReverseList> {
public:
    SecondLevelMSIterator(ReverseList& list, const KeyRange& key_range) : 
                            _list(list),
                            _key_range(key_range),
                            _index(0),
                            _first(true) {}
    virtual ~SecondLevelMSIterator() {}
    virtual int next(std::string& key, bool& res);
    virtual void fill_node(ReverseNode* node);
    virtual pb::ReverseNodeType get_flag();
    virtual ReverseNode& get_value();
    virtual void add_node(ReverseList& res_list) {
        ReverseNode* tmp_node = res_list.add_reverse_nodes();
        fill_node(tmp_node);
    }
private:
    ReverseList& _list;
    KeyRange _key_range;
    int _index;
    bool _first;
};

template<>
inline void SecondLevelMSIterator<ArrowReverseNode, ArrowReverseList>::add_node(ArrowReverseList& res_list) {
    res_list.add_node(*_list.mutable_reverse_nodes(_index));
}

//合并不同层次的倒排链表，返回合并后的长度
template<typename ReverseNode, typename ReverseList>
int level_merge(MergeSortIterator<ReverseNode, ReverseList>* new_iter, 
                MergeSortIterator<ReverseNode, ReverseList>* old_iter,
                ReverseList& res_list,
                bool is_del);

//end:true   not end:false
bool is_prefix_end(std::unique_ptr<rocksdb::Iterator>& iterator, uint8_t level);
//regionid_tableid_level_term_\0_pk  -> level
inline uint8_t get_level_from_reverse_key(const rocksdb::Slice& key) {
    return (uint8_t)key[16];
}
//regionid_tableid_level_term_\0_pk  -> term
inline const char* get_term_from_reverse_key(const rocksdb::Slice& key) {
    return key.data() + 8 + 8 + 1;
}
//regionid_tableid_*  -> tableid
inline int64_t get_tableid_from_reverse_key(const rocksdb::Slice& key) {
    return KeyEncoder::decode_i64(KeyEncoder::to_endian_u64(*(uint64_t*)(key.data() + 8)));
}

class ItemStatistic {
public:
    std::string term;
    bool is_fast = false;
    int64_t get_new = 0;
    int64_t seek_new = 0;
    int64_t seek_old = 0;
    int64_t merge_one_one = 0;
    int64_t get_two = 0;
    int64_t merge_one_two = 0;
    bool is_cache = false;
    int64_t parse = 0;
    int64_t get_three = 0;
    int64_t get_list = 0;
    int second_length = 0;
    int third_length = 0;
};
class ReverseSearchStatistic {
public:
    int64_t delete_time = 0;
    int64_t segment_time = 0;
    std::vector<ItemStatistic> term_times;
    int64_t create_exe_time = 0;
    int64_t bool_engine_time = 0;
    void print_log() {
        int64_t all_time = delete_time + segment_time + bool_engine_time;
        DB_NOTICE("Reverse index search time : all_time[%ld]{"
                  "delete_time[%ld], segment_time[%ld],"
                  "create_exe_time[%ld], bool_engine_time[%ld]}", 
                  all_time, delete_time, segment_time, create_exe_time, bool_engine_time);
        for (auto& item : term_times) {
            DB_NOTICE("Reverse index item : term[%s], get_list[%ld]{"
                       "is_fast[%d]{get_new_fast[%ld] | seek_new[%ld],"
                       "seek_old[%ld], merge_one_one[%ld], get_two[%ld, %d], merge_one_two[%ld]},"
                       "is_cache[%d], get_three[%ld, %d]{parse_nocache[%ld]}",
                       item.term.c_str(), item.get_list, item.is_fast, item.get_new, 
                       item.seek_new, item.seek_old, item.merge_one_one, item.get_two, 
                       item.second_length, item.merge_one_two, item.is_cache, item.get_three, 
                       item.third_length, item.parse);
        }
    }
};
//test api
void print_reverse_list_common(pb::CommonReverseList& list);
void print_reverse_list_xbs(pb::XbsReverseList& list);

template<typename, typename = void>
struct ReverseTrait;

template<typename ListType>
struct ReverseTrait<ListType,
    typename std::enable_if<
        std::is_same<ListType, pb::CommonReverseList>::value ||
        std::is_same<ListType, pb::XbsReverseList>::value
    >::type
> {
    using PrimaryType = std::string;
    const static bool_executor_type executor_type = NODE_NOT_COPY; 
    static void finish(ListType&) {}
    static const std::string& get_reverse_key(ListType& list, int64_t index) {
        return list.reverse_nodes(index).key();
    }

    static pb::ReverseNodeType get_flag(ListType& list, int64_t index) {
        return list.reverse_nodes(index).flag();
    }
};

template<typename ListType>
struct ReverseTrait<ListType,
    typename std::enable_if<
        std::is_same<ListType, ArrowReverseList>::value
    >::type
> {
    using PrimaryType = std::string;
    const static bool_executor_type executor_type = NODE_COPY; 
    static void finish(ListType& t) {
        t.finish();
    }
    static std::string get_reverse_key(ListType& list, int64_t index) {
        return list.get_key(index);
    }

    static pb::ReverseNodeType get_flag(ListType& list, int64_t index) {
        return list.get_flag(index);
    }
};
}// end of namespace

#include "reverse_common.hpp"

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

