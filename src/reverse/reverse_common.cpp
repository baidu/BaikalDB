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

#include "reverse_common.h"
#include "proto/reverse.pb.h"
namespace baikaldb {

std::atomic_long g_statistic_insert_key_num = {0};
std::atomic_long g_statistic_delete_key_num = {0};
#ifdef BAIDU_INTERNAL
drpc::NLPCClient* wordrank_client;
drpc::NLPCClient* wordseg_client;

int wordrank(const std::string& word, std::map<std::string, float>& term_map) {
    int8_t status;
    nlpc::ver_1_0_0::wordrank_outputPtr s_output = 
        sofa::create<nlpc::ver_1_0_0::wordrank_output>();
    status = nlpc_seg(*wordrank_client, word, s_output);
    if (status != 0) {
        //DB_WARNING("segment failed, word[%s]", word.c_str());
        return -1;
    }
    for (uint32_t i = 0; i < s_output->nlpc_trunks_pb().size(); i++) {
        std::string term;
        term = s_output->nlpc_trunks_pb()[i]->buffer();
        //DB_WARNING("term rank:%s", term.c_str());
        auto it = term_map.find(term);
        if (it == term_map.end()) {
            int32_t rank = s_output->nlpc_trunks_pb()[i]->rank();
            float weight = s_output->nlpc_trunks_pb()[i]->weight();
            if (!(rank == 2 || rank == 1)) {
                continue;
            }
            term_map[term] = weight;
        } 
    }
    return 0;
}

int wordseg_basic(const std::string& word, std::map<std::string, float>& term_map) {
    int8_t status;
    nlpc::ver_1_0_0::wordseg_outputPtr s_output =
        sofa::create<nlpc::ver_1_0_0::wordseg_output>();
    status = nlpc_seg(*wordseg_client, word, s_output);
    if (status != 0) {
        //DB_WARNING("segment failed, word[%s]", word.c_str());
        return -1;
    }
    auto scw = s_output->scw_out();
    std::string& buf = scw->wordsepbuf();
    for (uint32_t i = 0; i < scw->wsbtermcount(); i++) {
        std::string term;
        int offset = (scw->wsbtermpos().at(i)) & 0x00ffffff;
        int len = (scw->wsbtermpos().at(i)) >> 24;
        term.assign(buf, offset, len);
        //DB_WARNING("term seg:%s", term.c_str());
        auto it = term_map.find(term);
        if (it == term_map.end()) {
            term_map[term] = 0;
        } 
    }
    return 0;
}
#endif
int simple_seg_gbk(const std::string& word, std::map<std::string, float>& term_map) {
    for (uint32_t i = 0; i < word.size(); i++) {
        if ((word[i] & 0x80) != 0) {
            term_map[word.substr(i, 2)] = 0;
            //DB_WARNING("term simple:%s", word.substr(i, 2).c_str());
            i++;
        } else {
            term_map[word.substr(i, 1)] = 0;
            //DB_WARNING("term simple:%s", word.substr(i, 1).c_str());
        }
    }
    return 0;
}

bool is_prefix_end(std::unique_ptr<rocksdb::Iterator>& iterator, uint8_t level) {
    if (iterator->Valid()) {
        uint8_t level_ = get_level_from_reverse_key(iterator->key());
        if (level == level_) {
            return false;
        } else {
            return true;
        }
    }
    return true;
}

void print_reverse_list_test(pb::ReverseList& list) {
    int size = list.reverse_nodes_size();
    std::cout << "test size: " << size << std::endl;
    for (int i = 0; i < size; ++i) {
        const pb::ReverseNode& reverse_node = list.reverse_nodes(i);
        pb::common_reverse_extra extra;
        extra.ParseFromString(reverse_node.extra_info());
        std::cout << reverse_node.key() << " " << reverse_node.flag() << " " 
                  << extra.weight() << " | ";
    }
    std::cout << std::endl;
}

void print_reverse_list_common(pb::CommonReverseList& list) {
    int size = list.reverse_nodes_size();
    std::cout << "common size: " << size << std::endl;
    for (int i = 0; i < size; ++i) {
        const pb::CommonReverseNode& reverse_node = list.reverse_nodes(i);
        std::cout << reverse_node.key() << "(" << reverse_node.flag() << ") "; 
    }
    std::cout << std::endl;
}

}





















/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
