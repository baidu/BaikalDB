// Copyright (c) 2018 Baidu, Inc. All Rights Reserved.
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
    const ReverseNode* advance(const std::string& target_id);
private:
    //二分查找，大于或等于
    uint32_t binary_search(uint32_t first, 
                           uint32_t last, 
                           const std::string& target_id, 
                           ReverseList* list);
    MessageSP _new_list_ptr;
    MessageSP _old_list_ptr;
    ReverseList* _new_list;
    ReverseList* _old_list;
    int32_t _curr_ix_new;//-1表示链表遍历结束，大于等于0表示链表当前节点
    int32_t _curr_ix_old;
    uint32_t _list_size_new;//链表的长度
    uint32_t _list_size_old;
    PrimaryIdT* _curr_id_new;//
    PrimaryIdT* _curr_id_old;
    int _cmp_res;//确定当前使用的node
    ReverseNode* _curr_node; // nullptr 代表遍历结束
    KeyRange _key_range;
};

//--common
class CommonSchema : public SchemaBase<pb::CommonReverseNode, pb::CommonReverseList> {
public:
    typedef pb::CommonReverseNode ReverseNode;
    typedef pb::CommonReverseList ReverseList;
    typedef pb::CommonReverseNode PostingNodeT;
    typedef CommRindexNodeParser<CommonSchema> Parser;

    static int segment(
                    const std::string& word,
                    const std::string& pk,
                    SmartRecord record,
                    pb::SegmentType segment_type,
                    const std::map<std::string, int32_t>& name_field_id_map,
                    pb::ReverseNodeType flag, 
                    std::map<std::string, ReverseNode>& res);
    
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
        to.set_weight(std::max(to.weight(), from.weight()));
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
    int create_executor(const std::string& search_data, pb::SegmentType segment_type);
    int next(SmartRecord record);
    bool_executor_type executor_type = NODE_NOT_COPY;
private:
    std::vector<std::string> _and_terms;
    int _weight_field_id = 0;
};
//--xbs
class XbsArg : public BoolArg {
public:
    XbsArg(uint64_t query_id) : _query_id(query_id) {
    }
    uint32_t _query_id;
    std::set<uint32_t> _userid_set;
    std::set<uint32_t> _source_set;
};
class XbsNodeCmp {
public:
    bool operator()(const pb::XbsReverseNode& l, const pb::XbsReverseNode& r) {
        return l.weight() > r.weight();
    }
};

class XbsSchema : public SchemaBase<pb::XbsReverseNode, pb::XbsReverseList> {
public:
    typedef pb::XbsReverseNode ReverseNode;
    typedef pb::XbsReverseList ReverseList;
    typedef pb::XbsReverseNode PostingNodeT;
    typedef CommRindexNodeParser<XbsSchema> Parser;

    static int segment(
                    const std::string& word,
                    const std::string& pk,
                    SmartRecord record,
                    pb::SegmentType segment_type,
                    const std::map<std::string, int32_t>& name_field_id_map,
                    pb::ReverseNodeType flag, 
                    std::map<std::string, ReverseNode>& res);
    static int merge_and(ReverseNode& to, const ReverseNode& from, BoolArg* arg);
    static int merge_or(ReverseNode& to, const ReverseNode& from, BoolArg* arg);
    static int merge_weight(ReverseNode& to, const ReverseNode& from, BoolArg* arg);
    static void init_node(ReverseNode& node, const std::string& term, BoolArg* arg);
    static bool filter(const ReverseNode& node, BoolArg* arg);
    //saerch_data json格式 
    //  {"or" : [
    //              {"and" : ["term1", "term2"], "weight" : ["term1", "term3"]},
    //              {"and" : ["term1", "term2"], "weight" : ["term1", "term3"]}
    //          ]
    //  }
    int create_executor(const std::string& search_data, pb::SegmentType segment_type);
    bool valid();
    int next(SmartRecord record);
    bool_executor_type executor_type = NODE_COPY;
    static std::unordered_map<std::string, std::set<uint32_t>> xbs_black_terms;
    static int init_black_terms(const std::string& file_name);
private:
    int32_t _weight_field_id = 0;
    int32_t _pic_scores_field_id = 0;
    int32_t _userid_field_id = 0;
    int32_t _source_field_id = 0;
    std::multiset<ReverseNode, XbsNodeCmp> _res;
    std::multiset<ReverseNode, XbsNodeCmp>::iterator _it;

};

}//end of namespace

#include "reverse_interface.hpp"

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
