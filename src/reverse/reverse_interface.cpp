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
    uint64_t tmp = UINT64_MAX;//判断后续位是否还有1的标志
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

}// end of namespace



















/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
