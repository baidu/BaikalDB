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
#include "reverse_index.h"
#include "proto/reverse.pb.h"
#include "boolean_executor.h"
#include "logical_query.h"
#include "schema_factory.h"
#include "reverse_arrow.h"
#include "reverse_common.h"
#include "reverse_index.h"
#include <map>

namespace baikaldb {

//获取链表的接口
//如果链表底层的数据不够用，可以在这一层修改，比如xbs的query_id
template<typename Schema>
class CommRindexNodeParser : public RindexNodeParser<Schema> {
public:
    typedef typename Schema::ReverseNode ReverseNode;
    typedef typename Schema::PrimaryIdT PrimaryIdT;
    typedef typename Schema::ReverseList ReverseList;

    using ReverseListSptr = typename Schema::ReverseListSptr;
    CommRindexNodeParser(Schema* schema) : 
                            RindexNodeParser<Schema>(schema) {
        _key_range = schema->key_range();
    }
    ~CommRindexNodeParser() {
    }
    int init(const std::string& term);
    //return nullptr 代表遍历结束
    const ReverseNode* current_node();
    const PrimaryIdT* current_id();
    //只进不退
    const ReverseNode* next();
    const ReverseNode* advance(const PrimaryIdT& target_id);
private:
    //二分查找，大于或等于
    uint32_t binary_search(uint32_t first, 
                           uint32_t last, 
                           const PrimaryIdT& target_id, 
                           ReverseList* list);
    ReverseListSptr _new_list_ptr;
    ReverseListSptr _old_list_ptr;
    ReverseList* _new_list;
    ReverseList* _old_list;
    int32_t _curr_ix_new;//-1表示链表遍历结束，大于等于0表示链表当前节点
    int32_t _curr_ix_old;
    int32_t _list_size_new;//链表的长度
    int32_t _list_size_old;
    PrimaryIdT* _curr_id_new;//
    PrimaryIdT* _curr_id_old;
    int _cmp_res;//确定当前使用的node
    ReverseNode* _curr_node; // nullptr 代表遍历结束
    KeyRange _key_range;
};

//--common
template<typename Node, typename List>
class NewSchema : public SchemaBase<Node, List> {
public:
    using ReverseNode = Node;
    using ReverseList = List;
    using ReverseListSptr = std::shared_ptr<ReverseList>;
    using ThisType = NewSchema<ReverseNode, ReverseList>;
    using Parser = CommRindexNodeParser<ThisType> ;
    using IndexSearchType = ReverseIndex<ThisType>;

    static int segment(
                    const std::string& word,
                    const std::string& pk,
                    SmartRecord record,
                    pb::SegmentType segment_type,
                    const std::map<std::string, int32_t>& name_field_id_map,
                    pb::ReverseNodeType flag, 
                    std::map<std::string, ReverseNode>& res,
                    const pb::Charset& charset);
    
    static int merge_and(
                    ReverseNode& to, 
                    const ReverseNode& from, 
                    BoolArg* arg) {
        to.set_weight(to.weight() + from.weight());
        return 0; 
    }
    static int merge_or(
                    ReverseNode& to, 
                    const ReverseNode& from, 
                    BoolArg* arg) {
        to.set_weight(to.weight() + from.weight());
        return 0;
    }
    static int merge_weight(
                    ReverseNode& to, 
                    const ReverseNode& from, 
                    BoolArg* arg) {
        return 0;
    }
    //search_data 字符串格式
    //"hello world"
    int create_executor(
        const std::string& search_data, pb::MatchMode mode, pb::SegmentType segment_type, const pb::Charset& charset);
    int next(SmartRecord record);
    bool_executor_type executor_type = ReverseTrait<List>::executor_type;
    void set_term(const std::string& term, Parser* parse) {
        _temp_map[term] = parse;
    }
    Parser* get_term(const std::string& term) {
        if (_temp_map.count(term) == 1) {
            return _temp_map[term];
        }
        return NULL;
    }
    void set_index_search(IndexSearchType* index_ptr) {
        _index_ptr = index_ptr;
    }

    int get_reverse_list(
                    const std::string& term, 
                    ReverseListSptr& list_new, 
                    ReverseListSptr& list_old) {
        return _index_ptr->get_reverse_list_two(_txn, term, list_new, list_old, _is_fast);
    }

    const std::string& get_query_words () const {
        return _query_words;
    }

private:
    FieldInfo* _weight_field = nullptr;
    std::string _query_words;
    FieldInfo* _query_words_field = nullptr;
    std::map<std::string, Parser*> _temp_map;
    using SchemaBase<Node, List>::_table_info;
    using SchemaBase<Node, List>::_exe;
    using SchemaBase<Node, List>::_statistic;
    using SchemaBase<Node, List>::_cur_node;
    using SchemaBase<Node, List>::_index_info;
    using SchemaBase<Node, List>::_txn;
    using SchemaBase<Node, List>::_is_fast;

    IndexSearchType* _index_ptr;
};

using CommonSchema = NewSchema<pb::CommonReverseNode, pb::CommonReverseList>;
using ArrowSchema = NewSchema<ArrowReverseNode, ArrowReverseList>;

}//end of namespace

#include "reverse_interface.hpp"

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
