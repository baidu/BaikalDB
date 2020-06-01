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

#include "reverse_interface.h"
#include <fstream>
#include "proto/reverse.pb.h"
#include "table_record.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/reader.h"
#include "rapidjson/writer.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h"
#include "table_record.h"
#include "slot_ref.h"

namespace baikaldb {
//--xbs interface
const char delim_term = ';';
const char delim_score = ':';

void src_2_term_infos(const std::string &src, std::vector<std::string>& term_infos)
{
    int pos = 0;
    int size = src.length();
    int begin = 0;
    bool over = false;

    int tmp_pos = src.find(':');
    if (tmp_pos == -1 || tmp_pos == size -1) {
        return;
    }
    std::string new_src = src.substr(tmp_pos + 1, size - tmp_pos - 1);
    while (true) {
        pos = new_src.find(delim_term, begin);
        if (pos == -1) {
            pos = new_src.length();
            over = true;
        }
        term_infos.push_back(new_src.substr(begin, pos - begin));
        begin = pos + 1;
        if (over) {
            break;
        }
    }
    return;
}

int parse_term_info(
        const std::string& term_info, 
        std::string& term, 
        float& score, 
        std::vector<uint32_t>& tag_source_vec)
{
    int pos = term_info.find(delim_score);
    if (pos == -1) {
        return -1;
    }
    int len = term_info.length();
    term = term_info.substr(0, pos);
    std::string score_source_str = term_info.substr(pos + 1, len - pos - 1);
    char *next = NULL;
    score = strtof(score_source_str.c_str(), &next);
    if (*next != delim_score) {
        return -1;
    }
    uint64_t tags = strtoul(next + 1, NULL, 10);
    uint64_t tag = 1;
    uint64_t tmp = 0xFFFFFFFFFFFFFFFF;//判断后续位是否还有1的标志
    for (uint32_t i = 1; i <= 64; ++i) {
        if (tags & tag) {
            tag_source_vec.push_back(tag);
        }
        tmp ^= tag;
        if ((tags & tmp) == 0) {
            //后续位没有1，则退出
            break;
        }
        tag <<= 1;
    }  
    return 0;
}

int XbsSchema::segment(
                    const std::string& word, 
                    const std::string& pk,
                    SmartRecord record,
                    pb::SegmentType segment_type,
                    const std::map<std::string, int32_t>& name_field_id_map,
                    pb::ReverseNodeType flag,
                    std::map<std::string, ReverseNode>& res) {
    std::vector<std::string> term_infos;
    src_2_term_infos(word, term_infos);
    int32_t userid_field_id = 0;
    if (name_field_id_map.count("userid") == 1) {
        userid_field_id = name_field_id_map.at("userid");
    }
    int32_t source_field_id = 0;
    if (name_field_id_map.count("source") == 1) {
        source_field_id = name_field_id_map.at("source");
    }
    auto field = record->get_field_by_tag(userid_field_id);
    uint32_t userid = record->get_value(field).get_numberic<uint32_t>();
    auto field2 = record->get_field_by_tag(source_field_id);
    uint32_t source = record->get_value(field2).get_numberic<uint32_t>();
    for (auto& term_info : term_infos) {
        std::string term;
        float score = 0;
        std::vector<uint32_t> tag_source_vec;
        parse_term_info(term_info, term, score, tag_source_vec);
        ReverseNode node;
        node.set_key(pk);
        node.set_flag(flag);
        node.set_userid(userid);
        node.set_source(source);
        for (auto tag_source : tag_source_vec) {
            auto trigger = node.add_triggers();
            trigger->set_tag_source(tag_source);
            trigger->set_weight(score);
        }
        res[term] = node;
    }
    return 0;
}

int XbsSchema::merge_and(ReverseNode& node_to, const ReverseNode& node_from, BoolArg* arg) {
    if (node_from.flag() == pb::REVERSE_NODE_DELETE) {
        node_to.set_flag(pb::REVERSE_NODE_DELETE);
    }
    if (node_to.flag() == pb::REVERSE_NODE_DELETE) {
        node_to.clear_triggers();
        return 0;
    }
    for (int32_t i = 0; i < node_from.triggers_size(); i++) {
        *node_to.add_triggers() = node_from.triggers(i);
    }
    if (node_to.triggers_size() == 0) {
        node_to.set_flag(pb::REVERSE_NODE_DELETE);
    } 
    return 0; 
}


int XbsSchema::merge_or(ReverseNode& node_to, const ReverseNode& node_from, BoolArg* arg) {
    if (node_from.flag() == pb::REVERSE_NODE_NORMAL) {
        node_to.set_flag(pb::REVERSE_NODE_NORMAL);
    }
    if (node_to.flag() == pb::REVERSE_NODE_DELETE) {
        node_to.clear_triggers();
        return 0;
    }
    std::map<std::string, std::set<uint32_t>> trigger_to;
    for (int32_t i = 0; i < node_to.triggers_size(); i++) {
        const std::string& term = node_to.triggers(i).term(); 
        trigger_to[term].insert(node_to.triggers(i).tag_source());
    }
    for (int32_t i = 0; i < node_from.triggers_size(); i++) {
        const std::string& term = node_from.triggers(i).term(); 
        uint32_t tag_source = node_from.triggers(i).tag_source();
        if (trigger_to.count(term) > 0 && trigger_to.find(term)->second.count(tag_source) > 0) {
            continue;
        } else {
            *node_to.add_triggers() = node_from.triggers(i);
        }
    }
    if (node_to.triggers_size() == 0) {
        node_to.set_flag(pb::REVERSE_NODE_DELETE);
    } 
    return 0;
}

int XbsSchema::merge_weight(ReverseNode& node_to, const ReverseNode& node_from, BoolArg* arg) {
    /*if (node_from.flag() == pb::REVERSE_NODE_NORMAL) {
        node_to.set_flag(pb::REVERSE_NODE_NORMAL);
    }
    if (node_from.flag() == pb::REVERSE_NODE_DELETE 
            || node_to.flag() == pb::REVERSE_NODE_DELETE) {
        return 0;
    }
    std::map<uint32_t, pb::XbsReverseNode_Trigger> tag_index;
    for (int32_t i = 0; i < node_from.triggers_size(); i++) { 
        uint32_t tag_source = node_from.triggers(i).tag_source();
        tag_index[tag_source] = node_from.triggers(i);
    }
    for (int32_t i = 0; i < node_to.triggers_size(); i++) {
        uint32_t tag_source = node_to.triggers(i).tag_source();
        //修饰词的权重加到目标正排上
        if (tag_index.count(tag_source) == 1)
        {
            auto tt = node_to.mutable_triggers(i);
            auto& ft = tag_index[tag_source];
            tt->set_calc_weight(tt->calc_weight() + ft.calc_weight());
        }
    }*/
    return 0;
}

void XbsSchema::init_node(ReverseNode& node, const std::string& term, BoolArg* arg) {
    if (node.flag() == pb::REVERSE_NODE_DELETE) {
        return;
    }
    for (int32_t i = 0; i < node.triggers_size(); i++) { 
        node.mutable_triggers(i)->set_term(term);
        if (arg) {
            node.mutable_triggers(i)->set_query_id(((XbsArg*)arg)->_query_id);
        }
    }
    auto it = xbs_black_terms.find(term);
    if (it == xbs_black_terms.end()) {
        return;
    }
    std::set<uint32_t>& black_sources = it->second;
    std::vector<pb::XbsReverseNode_Trigger> triggers;
    for (int32_t i = 0; i < node.triggers_size(); i++) { 
        if (black_sources.count(node.triggers(i).tag_source()) == 0) {
            triggers.push_back(node.triggers(i));
        }
    }
    node.clear_triggers();
    if (triggers.size() == 0) {
        node.set_flag(pb::REVERSE_NODE_DELETE);
    } else {
        for (uint32_t i = 0; i < triggers.size(); ++i) {
            *node.add_triggers() = triggers[i];
        }
    }
}

bool XbsSchema::filter(const ReverseNode& node, BoolArg* arg) {
    if (arg == nullptr) {
        return false;
    }
    std::set<uint32_t>& userid_set = static_cast<XbsArg*>(arg)->_userid_set;
    std::set<uint32_t>& source_set = static_cast<XbsArg*>(arg)->_source_set;
    if (!userid_set.empty() && userid_set.count(node.userid()) == 0) {
        return true;
    }
    if (!source_set.empty() && source_set.count(node.source()) == 0) {
        return true;
    }
    return false;
}

static int init_filter_set(std::vector<ExprNode*> exprs, int32_t field_id, std::set<uint32_t>& filter_set) {
    for (auto expr : exprs) {
        if (expr->children_size() < 2) {
            return -1;
        }
        if (expr->children(0)->node_type() != pb::SLOT_REF) {
            return -1;
        }
        if (static_cast<SlotRef*>(expr->children(0))->field_id() != field_id) {
            continue;
        }
        for (uint32_t i = 1; i < expr->children_size(); i++) {
            if (!expr->children(i)->is_literal()) {
                return -1;
            }
            filter_set.insert(expr->children(i)->get_value(nullptr).get_numberic<uint32_t>());
        }
    }
    return 0;
}

int XbsSchema::create_executor(const std::string& search_data, pb::MatchMode mode, pb::SegmentType segment_type) {
    _weight_field_id = get_field_id_by_name(_table_info.fields, "__weight");
    _pic_scores_field_id = get_field_id_by_name(_table_info.fields, "__pic_scores");
    _userid_field_id = get_field_id_by_name(_table_info.fields, "userid");
    _source_field_id = get_field_id_by_name(_table_info.fields, "source");
    std::set<uint32_t> userid_set;
    std::set<uint32_t> source_set;
    init_filter_set(_conjuncts, _userid_field_id, userid_set);
    init_filter_set(_conjuncts, _source_field_id, source_set);
    LogicalQuery<XbsSchema> logical_query(this);    
    rapidjson::Document doc;
    doc.Parse<0>(search_data.c_str());
    if (doc.HasParseError()) {
        DB_WARNING("[%s] parse to json error", search_data.c_str());
        _exe = nullptr;
        return -1;
    }
    if (!doc.HasMember("or")) {
        DB_WARNING("[%s] has no or", search_data.c_str());
        _exe = nullptr;
        return -1;
    }
    auto& root = logical_query._root;
    root._type = OR;
    root._merge_func = XbsSchema::merge_or;
    const rapidjson::Value &or_value = doc["or"];
    for (uint32_t i = 0; i < or_value.Size(); ++i) {
        const rapidjson::Value &and_weight_value = or_value[i];
        if (!and_weight_value.HasMember("and") || !and_weight_value["and"].IsArray()) {
            DB_WARNING("[%s] has no and", search_data.c_str());
            continue;
        }
        const rapidjson::Value &and_value = and_weight_value["and"];
        ExecutorNode<XbsSchema>* and_node = new ExecutorNode<XbsSchema>();
        and_node->_type = AND;
        and_node->_merge_func = XbsSchema::merge_and;
        for (uint32_t j = 0; j < and_value.Size(); ++j) {
            //i is query_id
            if (!and_value[j].IsString()) {
                DB_WARNING("and_value must be string");
                return -1;
            }
            ExecutorNode<XbsSchema>* node = new ExecutorNode<XbsSchema>();
            node->_type = TERM;
            node->_term = and_value[j].GetString();
            XbsArg* arg = new XbsArg(i);
            arg->_userid_set = userid_set;
            arg->_source_set = source_set;
            node->_arg = arg;
            and_node->_sub_nodes.push_back(node);
        }
        if (and_weight_value.HasMember("weight") && and_weight_value["weight"].IsArray()) {
            ExecutorNode<XbsSchema>* weight_node = new ExecutorNode<XbsSchema>();
            weight_node->_type = WEIGHT;
            weight_node->_merge_func = XbsSchema::merge_weight;
            weight_node->_sub_nodes.push_back(and_node);
            const rapidjson::Value &weight_value = and_weight_value["weight"];
            for (uint32_t k = 0; k < weight_value.Size(); ++k) {
                if (!weight_value[k].IsString()) {
                    DB_WARNING("weight_value must be string");
                    return -1;
                }
                ExecutorNode<XbsSchema>* node = new ExecutorNode<XbsSchema>();
                node->_type = TERM;
                node->_term = weight_value[k].GetString();
                weight_node->_sub_nodes.push_back(node);
            }
            root._sub_nodes.push_back(weight_node);
        } else {
            root._sub_nodes.push_back(and_node);
        }
    }
    _exe = logical_query.create_executor();
    //get all res, and sort
    TimeCost cost;
    if (_exe != NULL) {
        while (true) {
            _cur_node = (const ReverseNode*)(_exe->next());
            if (_cur_node) {
                if (_cur_node->flag() == pb::REVERSE_NODE_NORMAL) {
                    float max_weight = 0.0;
                    for (auto& trigger : _cur_node->triggers()) {
                        if (trigger.weight() > max_weight) {
                            max_weight = trigger.weight();
                        }
                    }
                    ((ReverseNode*)_cur_node)->set_weight(max_weight);
                    _res.insert(*_cur_node);
                }
            } else {
                break;
            }
        }
    }
    //DB_WARNING("sort ReverseNode time:%ld len:%u", cost.get_time(), _res.size());
    _it = _res.begin();
    return 0;
}

bool XbsSchema::valid() {
    if (_it != _res.end()) {
        return true;
    } else {
        return false;
    }
}

int XbsSchema::next(SmartRecord record) {
    //TimeCost cost;
    if (_it == _res.end()) {
        return -1;
    }
    const pb::XbsReverseNode& reverse_node = *_it;
    _it++;
    int ret = record->decode_key(_index_info, reverse_node.key());
    if (ret < 0) {
        return -1;
    }
    if (_weight_field_id > 0) {
        MessageHelper::set_float(record->get_field_by_tag(_weight_field_id),
                record->get_raw_message(), reverse_node.weight());
    }
    if (_userid_field_id > 0) {
        MessageHelper::set_uint32(record->get_field_by_tag(_userid_field_id),
                record->get_raw_message(), reverse_node.userid());
    }
    if (_source_field_id > 0) {
        MessageHelper::set_uint32(record->get_field_by_tag(_source_field_id),
                record->get_raw_message(), reverse_node.source());
    }
    //int64_t decode_key_cost = cost.get_time();
    //cost.reset();
    
    if (_pic_scores_field_id > 0) {
        rapidjson::Document root;
        root.SetObject();
        rapidjson::Document::AllocatorType& alloc = root.GetAllocator(); 
        rapidjson::Value arr(rapidjson::kArrayType);
        for (auto& trigger : reverse_node.triggers()) {
            rapidjson::Value pic_score(rapidjson::kObjectType);
            pic_score.AddMember("tag_source", trigger.tag_source(), alloc);
            rapidjson::Value picword;
            picword.SetString(trigger.term().c_str(), trigger.term().size(), alloc);
            pic_score.AddMember("picword", picword, alloc);
            uint32_t score = trigger.weight();
            pic_score.AddMember("score", score, alloc);
            pic_score.AddMember("query_id", trigger.query_id(), alloc);
            arr.PushBack(pic_score, alloc);
        }
        root.AddMember("pic_scores", arr, alloc);
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> json_writer(buffer);
        root.Accept(json_writer);

        record->set_string(record->get_field_by_tag(_pic_scores_field_id), 
            buffer.GetString());
    }
    //DB_NOTICE("xbs time: decode_key_cost:%ld, json:%ld", decode_key_cost, cost.get_time());

    return 0;
}

std::unordered_map<std::string, std::set<uint32_t>> XbsSchema::xbs_black_terms;

int XbsSchema::init_black_terms(const std::string& file_name) {
    std::ifstream fin(file_name.c_str(), std::ios::in);
    if (!fin.is_open()) {
        DB_WARNING("init xbs black terms failed");
        return -1;
    }
    std::string line;
    while (getline(fin, line)) {
        int pos = line.find(":");
        std::string term = line.substr(0, pos);
        uint32_t source = atoi(line.substr(pos + 1).c_str());
        std::set<uint32_t> sources;
        sources.insert(source);
        xbs_black_terms[term] = sources;
    }
    return 0;
}

}// end of namespace



















/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
