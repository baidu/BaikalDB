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

namespace baikaldb {
int segment(
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

int merge_and(ReverseNode& to, const ReverseNode& from, BoolArg* arg) {
    to.set_weight(to.weight() + from.weight());
    return 0;
}

int merge_or(ReverseNode& to, const ReverseNode& from, BoolArg* arg) {
    to.set_weight(to.weight() + from.weight());
    return 0;
}
int merge_weight(ReverseNode& to, const ReverseNode& from, BoolArg* arg) {
    return 0;
}
}