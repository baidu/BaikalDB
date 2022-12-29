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

#include "reverse_common.h"
#include <cctype>
#include <unordered_set>
#include <fstream>
#include <gflags/gflags.h>
#include "proto/reverse.pb.h"
namespace baikaldb {
DEFINE_string(q2b_utf8_path, "./conf/q2b_utf8.dic", "q2b_utf8_path");
DEFINE_string(q2b_gbk_path, "./conf/q2b_gbk.dic", "q2b_gbk_path");
DEFINE_string(punctuation_path, "./conf/punctuation.dic", "punctuation_path");
DEFINE_bool(reverse_print_log, false, "reverse_print_log");
DEFINE_bool(enable_print_convert_log, false, "enable_print_convert_log");

std::atomic_long g_statistic_insert_key_num = {0};
std::atomic_long g_statistic_delete_key_num = {0};

int Iconv::utf8_to_gbk(const char* psrc, const size_t nsrc, std::string& dst) {
    if (_cd_utf8_to_gbk == (iconv_t)-1) {
        DB_FATAL("Fail to iconv open, _cd_utf8_to_gbk == (iconv_t)-1");
        return -1;
    }

    if (psrc == nullptr) {
        DB_FATAL("psrc is Empty");
        return -1;
    }
    if (nsrc <= 0) { 
        return 0; 
    }
    dst.resize(nsrc + 1);

    char* inbuf     = const_cast<char*>(psrc);
    size_t in_bytes_left  = nsrc;
    char* outbuf          = &dst[0];
    size_t out_bytes_left = dst.size();

    iconv(_cd_utf8_to_gbk, nullptr, nullptr, nullptr, nullptr);
    size_t ret = iconv(_cd_utf8_to_gbk, &inbuf, &in_bytes_left, &outbuf, &out_bytes_left);
    if (ret != 0) {
        return -1;
    }

    dst.resize(dst.size() - out_bytes_left);
    return 0;
}

int Iconv::gbk_to_utf8(const char* psrc, const size_t nsrc, std::string& dst) {
    if (_cd_gbk_to_utf8 == (iconv_t)-1) {
        DB_FATAL("Fail to iconv open, _cd_gbk_to_utf8 == (iconv_t)-1");
        return -1;
    }

    if (psrc == nullptr) {
        DB_FATAL("psrc is Empty");
        return -1;
    }
    if (nsrc <= 0) { 
        return 0; 
    }
    dst.resize(nsrc / 2 * 3 + 1);

    char* inbuf     = const_cast<char*>(psrc);
    size_t in_bytes_left  = nsrc;
    char* outbuf          = &dst[0];
    size_t out_bytes_left = dst.size();

    iconv(_cd_gbk_to_utf8, nullptr, nullptr, nullptr, nullptr);
    size_t ret = iconv(_cd_gbk_to_utf8, &inbuf, &in_bytes_left, &outbuf, &out_bytes_left);
    if (ret != 0) {
        return -1;
    }

    dst.resize(dst.size() - out_bytes_left);
    return 0;
}

int Tokenizer::init() {
    {
        std::ifstream fp(FLAGS_punctuation_path);
        _punctuation_blank.insert(" ");
        _punctuation_blank.insert("\t");
        _punctuation_blank.insert("\r");
        _punctuation_blank.insert("\n");
        while (fp.good()) {
            std::string line;
            std::getline(fp, line);
            if (line.size() == 1) {
                _punctuation_blank.insert(line);
            }
        }
    }
    {
        std::ifstream fp(FLAGS_q2b_gbk_path);
        while (fp.good()) {
            std::string line;
            std::getline(fp, line);
            auto pos = line.find('\t');
            if (pos == std::string::npos) {
                continue;
            }
            _q2b_gbk[line.substr(0, pos)] = line.substr(pos + 1, 1);
        }
    }
    {
        std::ifstream fp(FLAGS_q2b_utf8_path);
        while (fp.good()) {
            std::string line;
            std::getline(fp, line);
            auto pos = line.find('\t');
            if (pos == std::string::npos) {
                continue;
            }
            _q2b_utf8[line.substr(0, pos)] = line.substr(pos + 1, 1);
        }
    }
    return 0;
}

#ifdef BAIDU_INTERNAL
drpc::NLPCClient* wordrank_client;
drpc::NLPCClient* wordseg_client;
drpc::NLPCClient* wordweight_client;

int Tokenizer::wordweight(std::string word, std::map<std::string, float>& term_map, 
                          const pb::Charset& charset, bool is_filter, bool is_same_weight) {
    if (word.empty()) {
        return 0;
    }
    if (wordweight_client == nullptr) {
        DB_FATAL("not load wordweight dict.");
        return -1;
    }

    // convert utf8 to gbk
    if (charset == pb::UTF8) {
        if (utf8_to_gbk(word) != 0) {
            if (FLAGS_enable_print_convert_log) {
                DB_WARNING("Fail to convert utf8 to gbk, %s", word.c_str());
            }
            return -1;
        }
    }

    int8_t status;
    nlpc::ver_1_0_0::wordweight_outputPtr s_output = 
        sofa::create<nlpc::ver_1_0_0::wordweight_output>();
    nlpc::ver_1_0_0::wordweight_inputPtr s_input = ::sofa::create<nlpc::ver_1_0_0::wordweight_input>();
    s_input->set_query(word);
    status = nlpc_seg(*wordweight_client, word, s_output, s_input);
    if (status != 0) {
        // DB_WARNING("segment failed, word[%s]", word.c_str());
        return -1;
    }
    for (auto t : s_output->basic_result()) {
        std::string term = string_trim(t->word());

        // convert gbk to utf8
        if (charset == pb::UTF8) {
            if (gbk_to_utf8(term) != 0) {
                if (FLAGS_enable_print_convert_log) {
                    DB_WARNING("Fail to convert gbk to utf8, %s", term.c_str());
                }
                continue;
            }
        }

        auto it = term_map.find(term);
        if (it == term_map.end()) {
            int32_t level = t->level();
            float weight = t->weight();
            if (term == "") {
                continue;
            }
            if (is_filter && level == 0 && !float_equal(weight, 1)) {
                continue;
            }
            term_map[term] = weight;
        } else {
            term_map[term] += t->weight();
        }
    }

    if (is_same_weight && term_map.size() > 0) {
        float weight = 1.0 / term_map.size();
        for (auto& pair : term_map) {
            pair.second = weight;
        }
    }
    return 0;   
}

int Tokenizer::wordrank(std::string word, std::map<std::string, float>& term_map, const pb::Charset& charset) {
    if (word.empty()) {
        return 0;
    }

    if (wordrank_client == nullptr) {
        DB_FATAL("not load wordrank dict.");
        return -1;
    }
    
    // convert utf8 to gbk
    if (charset == pb::UTF8) {
        if (utf8_to_gbk(word) != 0) {
            if (FLAGS_enable_print_convert_log) {
                DB_WARNING("Fail to convert utf8 to gbk, %s", word.c_str());
            }
            return -1;
        }
    }

    int8_t status;
    nlpc::ver_1_0_0::wordrank_outputPtr s_output = 
        sofa::create<nlpc::ver_1_0_0::wordrank_output>();
    nlpc::ver_1_0_0::wordseg_inputPtr s_input = 
                        sofa::create<nlpc::ver_1_0_0::wordseg_input>();
    s_input->set_lang_id(0);
    s_input->set_lang_para(0);
    s_input->set_query(word);
    status = nlpc_seg(*wordrank_client, word, s_output, s_input);
    if (status != 0) {
        //DB_WARNING("segment failed, word[%s]", word.c_str());
        return -1;
    }
    for (uint32_t i = 0; i < s_output->nlpc_trunks_pb().size(); i++) {
        std::string term;
        term = string_trim(s_output->nlpc_trunks_pb()[i]->buffer());
        if (term.empty()) {
            continue;
        }

        // convert gbk to utf8
        if (charset == pb::UTF8) {
            if (gbk_to_utf8(term) != 0) {
                if (FLAGS_enable_print_convert_log) {
                    DB_WARNING("Fail to convert gbk to utf8, %s", term.c_str());
                }
                continue;
            }
        }

        //DB_WARNING("term rank:%s", term.c_str());
        auto it = term_map.find(term);
        float weight = s_output->nlpc_trunks_pb()[i]->weight();
        if (it == term_map.end()) {
            int32_t rank = s_output->nlpc_trunks_pb()[i]->rank();
            if (rank == 0) {
                continue;
            }
            term_map[term] = weight;
        } else {
            term_map[term] += weight;
        }
    }

    return 0;
}

int Tokenizer::wordseg_basic(std::string word, std::map<std::string, float>& term_map, const pb::Charset& charset) {
    if (word.empty()) {
        return 0;
    }
    if (wordseg_client == nullptr) {
        DB_FATAL("not load wordseg dict.");
        return -1;
    }
    
    q2b_tolower(word, charset);

    // convert utf8 to gbk
    if (charset == pb::UTF8) {
        if (utf8_to_gbk(word) != 0) {
            if (FLAGS_enable_print_convert_log) {
                DB_WARNING("Fail to convert utf8 to gbk, %s", word.c_str());
            }
            return -1;
        }
    }

    int8_t status;
    nlpc::ver_1_0_0::wordseg_outputPtr s_output =
        sofa::create<nlpc::ver_1_0_0::wordseg_output>();
    nlpc::ver_1_0_0::wordseg_inputPtr s_input = 
                        sofa::create<nlpc::ver_1_0_0::wordseg_input>();
    s_input->set_lang_id(0);
    s_input->set_lang_para(0);
    s_input->set_query(word);
    status = nlpc_seg(*wordseg_client, word, s_output, s_input);
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
        term = string_trim(term);
        if (_punctuation_blank.count(term) == 1) {
            continue;
        }

        // convert gbk to utf8
        if (charset == pb::UTF8) {
            if (gbk_to_utf8(term) != 0) {
                if (FLAGS_enable_print_convert_log) {
                    DB_WARNING("Fail to convert gbk to utf8, %s", term.c_str());
                }
                continue;
            }
        }

        //DB_WARNING("term seg:%s", term.c_str());
        auto it = term_map.find(term);
        if (it == term_map.end()) {
            term_map[term] = 0;
        } 
    }

    if (term_map.size() > 0) {
        float weight = 1.0 / term_map.size();
        for (auto& pair : term_map) {
            pair.second = weight;
        }
    }
    return 0;
}

int Tokenizer::wordrank_q2b_icase(
        std::string word, std::map<std::string, float>& term_map , const pb::Charset& charset) {
    q2b_tolower(word, charset);
    return wordrank(word, term_map, charset);
}

int Tokenizer::wordrank_q2b_icase_unlimit(
        std::string word, std::map<std::string, float>& term_map, const pb::Charset& charset) {
    std::vector<Tokenizer::SeperateIndex> indexs = q2b_tolower_with_index(word, charset);
    size_t index_size = indexs.size();

    // Process BOM
    size_t bom_len = 0;
    if (charset == pb::UTF8) {
        bom_len = get_utf8_bom_len(word);
    }

    size_t current_char = bom_len;
    // wordrank 切出 256个 term后会停止，设置可切除长度为 512，超出的部分，需要根据标点进行细分。
    const static size_t MAX_SEPARATOR_SIZE = 512;
    for (size_t index = 0; index < index_size; index++) {
        size_t current_index = index;
        while (indexs[index].type != SeperateType::ST_MAJOR && index < index_size) {
            index++;
        }
        if (indexs[index].index - current_char > MAX_SEPARATOR_SIZE) {
            for (; current_index <= index; current_index++) {
                if (current_index == index || indexs[current_index + 1].index - current_char + 1 > MAX_SEPARATOR_SIZE) {
                    DB_DEBUG("get wordrank str : %s", word.substr(current_char, indexs[current_index].index - current_char + 1).c_str());
                    if (wordrank(word.substr(current_char, indexs[current_index].index - current_char + 1), term_map, charset) != 0) {
                        return -1;
                    }
                    current_char = indexs[current_index].index;
                }
            }
        } else {
            DB_DEBUG("get wordrank str : %s", word.substr(current_char, indexs[index].index - current_char + 1).c_str());
            if (wordrank(word.substr(current_char, indexs[index].index - current_char + 1), term_map, charset) != 0) {
                return -1;
            }
        }

        current_char = indexs[index].index;
    }
    return 0;
}

#endif

void Tokenizer::split_str(
    const std::string& word, std::vector<std::string>& split_word, char delim, const pb::Charset& charset) {
    
    switch (charset) {
    case pb::GBK:
        split_str_gbk(word, split_word, delim);
        break;
    case pb::UTF8:
        split_str_utf8(word, split_word, delim);
        break;
    default:
        if (FLAGS_enable_print_convert_log) {
            DB_WARNING("Invalid charset[%d]", charset);
        }
        break;
    }
}

int Tokenizer::simple_seg(
    std::string word, uint32_t word_count, std::map<std::string, float>& term_map, const pb::Charset& charset) {
    
    switch (charset) {
    case pb::GBK:
        return simple_seg_gbk(word, word_count, term_map);
    case pb::UTF8:
        return simple_seg_utf8(word, word_count, term_map);
    default:
        if (FLAGS_enable_print_convert_log) {
            DB_WARNING("Invalid charset[%d]", charset);
        }
        return -1;
    }
}

int Tokenizer::es_standard(
    std::string word, std::map<std::string, float>& term_map, const pb::Charset& charset) {

    switch (charset) {
    case pb::GBK:
        return es_standard_gbk(word, term_map);
        break;
    case pb::UTF8:
        return es_standard_utf8(word, term_map);
    default:
        if (FLAGS_enable_print_convert_log) {
            DB_WARNING("Invalid charset[%d]", charset);
        }
        return -1;
    }
}

int Tokenizer::q2b_tolower(std::string& word, const pb::Charset& charset) {
    switch (charset) {
    case pb::GBK:
        q2b_tolower_gbk(word);
        break;
    case pb::UTF8:
        q2b_tolower_utf8(word);
        break;
    default:
        if (FLAGS_enable_print_convert_log) {
            DB_WARNING("Invalid charset[%d]", charset);
        }
        return -1;
    }
    return 0;
}

std::vector<Tokenizer::SeperateIndex> Tokenizer::q2b_tolower_with_index(std::string& word, const pb::Charset& charset) {
    switch (charset) {
    case pb::GBK:
        return q2b_tolower_gbk_with_index(word);
    case pb::UTF8:
        return q2b_tolower_utf8_with_index(word);
    default:
        if (FLAGS_enable_print_convert_log) {
            DB_WARNING("Invalid charset[%d]", charset);
        }
        break;
    }
    return {};
}

int Tokenizer::utf8_to_gbk(std::string& word) {
    // Process BOM
    const size_t bom_len = get_utf8_bom_len(word);
    if (word.size() <= bom_len) {
        return 0;
    }

    std::string word_tmp;
    if (iconv_tls.utf8_to_gbk(&word[0] + bom_len, word.size() - bom_len, word_tmp) != 0) {
        if (FLAGS_enable_print_convert_log) {
            DB_WARNING("Fail to convert utf8 to gbk, %s|%s", word.c_str(), word_tmp.c_str());
        }
        return -1;
    }
    std::swap(word, word_tmp);
    return 0;
}

int Tokenizer::gbk_to_utf8(std::string& word) {
    if (word.empty()) {
        return 0;
    }

    std::string word_tmp;
    if (iconv_tls.gbk_to_utf8(&word[0], word.size(), word_tmp) != 0) {
        if (FLAGS_enable_print_convert_log) {
            DB_WARNING("Fail to convert gbk to utf8, %s|%s", word.c_str(), word_tmp.c_str());
        }
        return -1;
    }
    std::swap(word, word_tmp);
    return 0;
}

// gbk
int Tokenizer::q2b_tolower_gbk(std::string& word) {
    size_t slow = 0;
    size_t fast = 0;
    while (fast < word.size()) {
        if ((word[fast] & 0x80) != 0) {
            if (_q2b_gbk.count(word.substr(fast, 2)) == 1) {
                word[slow++] = _q2b_gbk[word.substr(fast++, 2)][0];
                fast++;
            } else {
                word[slow++] = word[fast++];
                word[slow++] = word[fast++];
            }
        } else {
            if (isupper(word[fast])) {
                word[slow++] = ::tolower(word[fast++]);
            } else {
                word[slow++] = word[fast++];
            }
        }
    }
    word.resize(slow);
    return 0;
}

std::vector<Tokenizer::SeperateIndex> Tokenizer::q2b_tolower_gbk_with_index(std::string& word) {
    std::vector<Tokenizer::SeperateIndex> sep_indexs;
    sep_indexs.reserve(10);
    const static std::unordered_set<char> MAJOR_SEP {'!', '.', ';', '?'};
    size_t slow = 0;
    size_t fast = 0;
    while (fast < word.size()) {
        if ((word[fast] & 0x80) != 0) {
            if (_q2b_gbk.count(word.substr(fast, 2)) == 1) {
                word[slow++] = _q2b_gbk[word.substr(fast++, 2)][0];
                if (std::ispunct(word[slow - 1])) {
                    DB_DEBUG("insert index %zd", slow - 1);
                    if (MAJOR_SEP.count(word[slow - 1]) == 1) {
                        sep_indexs.emplace_back(slow - 1, SeperateType::ST_MAJOR);
                    } else {
                        sep_indexs.emplace_back(slow - 1, SeperateType::ST_MINOR);
                    }
                }
                fast++;
            } else {
                word[slow++] = word[fast++];
                word[slow++] = word[fast++];
            }
        } else {
            if (isupper(word[fast])) {
                word[slow++] = ::tolower(word[fast++]);
            } else {
                word[slow++] = word[fast++];
            }
        }
    }
    word.resize(slow);
    if (slow == 0) {
        return sep_indexs;
    }
    if (sep_indexs.size() == 0) {
        sep_indexs.emplace_back(slow - 1, SeperateType::ST_MAJOR);
    } else {
        if (sep_indexs.back().index != slow - 1) {
            sep_indexs.emplace_back(slow - 1, SeperateType::ST_MAJOR);
        }
    }
    return sep_indexs;
}

int Tokenizer::simple_seg_gbk(std::string word, uint32_t word_count, std::map<std::string, float>& term_map) {
    if (word.empty()) {
        return 0;
    }
    uint32_t slow_pos = 0;
    uint32_t fast_pos = 0;
    uint32_t i = 0;
    // 切第一个词
    for (uint32_t j = 0; i < word.size() && j < word_count; i++, j++) {
        if ((word[i] & 0x80) != 0) {
            i++;
        } else {
            if (isupper(word[i])) {
                word[i] = ::tolower(word[i]);
            }
        }
    }

    if (i >= word.size()) {
        term_map[word] = 1.0;
        return 0;
    } else {
        fast_pos = i;
        std::string term = word.substr(slow_pos, fast_pos - slow_pos);
        if (_punctuation_blank.count(term) == 0) {
            term_map[term] = 0;
        }
    }
    while (fast_pos < word.size()) {
        if ((word[slow_pos] & 0x80) != 0) {
            slow_pos++;
        }
        slow_pos++;
        if ((word[fast_pos] & 0x80) != 0) {
            fast_pos++;
        } else {
            if (isupper(word[fast_pos])) {
                word[fast_pos] = ::tolower(word[fast_pos]);
            }
        }
        fast_pos++;

        std::string term = word.substr(slow_pos, fast_pos - slow_pos);
        if (_punctuation_blank.count(term) == 1) {
            continue;
        }
        auto it = term_map.find(term);
        if (it == term_map.end()) {
            term_map[term] = 0;
        } 
    }

    if (term_map.size() > 0) {
        float weight = 1.0 / term_map.size();
        for (auto& pair : term_map) {
            pair.second = weight;
        }
    }
    return 0;
}

int Tokenizer::es_standard_gbk(std::string word, std::map<std::string, float>& term_map) {
    if (word.empty()) {
        return 0;
    }
    std::string term;
    bool is_word = false;
    bool is_num = false;
    bool has_point = false;
    for (uint32_t i = 0; i < word.size(); i++) {
        std::string now;
        if ((word[i] & 0x80) != 0) {
            now = word.substr(i, 2);
            i++;
            if (_q2b_gbk.count(now) == 1) {
                now = _q2b_gbk[now];
            } else {
                term_map[now] = 0;
                if (term.size() > 0) {
                    term_map[term] = 0;
                    term.clear();
                }
                is_word = false;
                is_num = false;
                has_point = false;
                continue;
            }
        } else {
            if (isupper(word[i])) {
                word[i] = ::tolower(word[i]);
            }
            now = word[i];
        }
        if (!term.empty()) {
            if (is_word && islower(now[0])) {
                term += now;
            } else if (is_num && isdigit(now[0])) {
                term += now;
            } else if (is_num && !has_point && now == ".") {
                term += now;
                has_point = true;
            } else {
                term_map[term] = 0;
                is_word = false;
                is_num = false;
                has_point = false;
                term.clear();
            }
        }
        if (term.empty()) {
            if (islower(now[0])) {
                term += now;
                is_word = true;
            } else if (isdigit(now[0])) {
                term += now;
                is_num = true;
            }
        }
    }
    if (!term.empty()) {
        term_map[term] = 0;
    }
    if (term_map.size() > 0) {
        float weight = 1.0 / term_map.size();
        for (auto& pair : term_map) {
            pair.second = weight;
        }
    }
    return 0;
}

void Tokenizer::split_str_gbk(const std::string& word, std::vector<std::string>& split_word, char delim) {
    if (word.empty()) {
        return;
    }
    // 去除前后%，适配like
    uint32_t i = 0;
    if (word[i] == '%') {
        ++i;
    }
    uint32_t size = word.size();
    if (word[size -1] == '%') {
        --size;
    }
    uint32_t last = i;
    for (; i < size; i++) {
        if ((word[i] & 0x80) != 0) {
            i++;
        } else if (word[i] == delim) {
            if (i - last > 0) {
                split_word.push_back(word.substr(last, i - last));
                //DB_NOTICE("push i %d last %d %s",i, last, split_word.back().c_str());
            } 
            last = i + 1;
        }
    }
    if (i - last > 0) {
        split_word.push_back(word.substr(last, i - last));
        //DB_NOTICE("push i %d last %d %s",i, last, split_word.back().c_str());
    } 
}

// utf8
std::vector<Tokenizer::SeperateIndex> Tokenizer::q2b_tolower_utf8_with_index(std::string& word) {
    std::vector<Tokenizer::SeperateIndex> sep_indexes;
    sep_indexes.reserve(10);
    const static std::unordered_set<char> MAJOR_SEP {'!', '.', ';', '?'};

    // Process BOM
    const size_t bom_len = get_utf8_bom_len(word);

    const size_t word_size = word.size();
    size_t slow = bom_len;
    size_t fast = bom_len;
    while (fast < word_size) {
        const size_t nremaining  = word_size - fast;
        const size_t utf8_len    = get_utf8_len(word[fast]);
        const size_t advance_len = (utf8_len < nremaining ? utf8_len : nremaining);

        if (utf8_len == 1) {
            word[slow] = (::isupper(word[fast]) ? ::tolower(word[fast]) : word[fast]);
            slow += 1;
            fast += 1;
        } else {
           if (_q2b_utf8.count(word.substr(fast, utf8_len)) == 1) {
                word[slow] = _q2b_utf8[word.substr(fast, utf8_len)][0];
                if (std::ispunct(word[slow])) {
                    if (MAJOR_SEP.count(word[slow]) == 1) {
                        sep_indexes.emplace_back(slow, SeperateType::ST_MAJOR);
                    } else {
                        sep_indexes.emplace_back(slow, SeperateType::ST_MINOR);
                    }
                }
                slow += 1;
                fast += advance_len;
            } else {
                for (size_t i = 0; i < advance_len; ++i) {
                    word[slow++] = word[fast++];
                }
            }
        }
    }
    word.resize(slow);

    if (slow == bom_len) {
        return sep_indexes;
    }
    if (sep_indexes.size() == 0) {
        sep_indexes.emplace_back(slow - 1, SeperateType::ST_MAJOR);
    } else {
        if (sep_indexes.back().index != slow - 1) {
            sep_indexes.emplace_back(slow - 1, SeperateType::ST_MAJOR);
        }
    }
    return sep_indexes;
}

int Tokenizer::q2b_tolower_utf8(std::string& word) {
    // Process BOM
    const size_t bom_len = get_utf8_bom_len(word);

    const size_t word_size = word.size();
    size_t slow = bom_len;
    size_t fast = bom_len;

    while (fast < word_size) { 
        const size_t nremaining  = word_size - fast;
        const size_t utf8_len    = get_utf8_len(word[fast]);
        const size_t advance_len = (utf8_len < nremaining ? utf8_len : nremaining);
        if (utf8_len == 1) {
            word[slow] = (::tolower(word[fast]) ? ::tolower(word[fast]) : word[fast]);
            slow += 1;
            fast += 1;
        } else {
            if (_q2b_utf8.count(word.substr(fast, utf8_len)) == 1) {
                word[slow] = _q2b_utf8[word.substr(fast, utf8_len)][0];
                slow += 1;
                fast += advance_len;
            } else {
                for (size_t i = 0; i < advance_len; ++i) {
                    word[slow++] = word[fast++];
                }
            }
        }
    }

    word.resize(slow);
    return 0;
}

int Tokenizer::es_standard_utf8(std::string word, std::map<std::string, float>& term_map) {
    if (word.empty()) {
        return 0;
    }

    // Process BOM
    const size_t bom_len = get_utf8_bom_len(word);

    bool is_word   = false;
    bool is_num    = false;
    bool has_point = false;

    std::string term;
    const size_t word_size = word.size();
    for (size_t i = bom_len; i < word_size; ++i) {
        const size_t nremaining  = word_size - i;
        const size_t utf8_len    = get_utf8_len(word[i]);
        const size_t advance_len = (utf8_len < nremaining ? utf8_len : nremaining);
        std::string now = word.substr(i, utf8_len);
        i += (advance_len - 1);

        if (now.empty()) { continue; }

        if (utf8_len == 1) {
            now[0] = (::isupper(now[0]) ? ::tolower(now[0]) : now[0]);
        } else {
            if (_q2b_utf8.count(now) == 1) {
                now = _q2b_utf8[now];
            } else {
                term_map[now] = 0;
                if (term.size() > 0) {
                    term_map[term] = 0;
                    term.clear();
                }
                is_word   = false;
                is_num    = false;
                has_point = false;
                continue;
            }
        }

        if (now.empty()) { continue; }
        if (!term.empty()) {
            if (is_word && ::islower(now[0])) {
                term += now[0];
            } else if (is_num && ::isdigit(now[0])) {
                term += now[0];
            } else if (is_num && !has_point && now[0] == '.') {
                term += now[0];
                has_point = true;
            } else {
                term_map[term] = 0;
                term.clear();
                is_word   = false;
                is_num    = false;
                has_point = false;
            }
        }
        if (term.empty()) {
            if (::islower(now[0])) {
                term += now[0];
                is_word = true;
            } else if (::isdigit(now[0])) {
                term += now[0];
                is_num = true;
            }
        }
    }

    if (!term.empty()) {
        term_map[term] = 0;
    }
    if (term_map.size() > 0) {
        float weight = 1.0 / term_map.size();
        for (auto& pair : term_map) {
            pair.second = weight;
        }
    }
    
    return 0;
}

int Tokenizer::simple_seg_utf8(std::string word, uint32_t word_count, std::map<std::string, float>& term_map) {
    if (word.empty()) {
        return 0;
    }

    // Process BOM
    const size_t bom_len = get_utf8_bom_len(word);

    const size_t word_size = word.size();
    size_t i = bom_len;
    size_t j = 0;

    // segment first [word_count] words
    for (; i < word_size; ++i) {
        if (j++ >= word_count) {
            break;
        }

        const size_t nremaining  = word_size - i;
        const size_t utf8_len    = get_utf8_len(word[i]);
        const size_t advance_len = (utf8_len < nremaining ? utf8_len : nremaining);

        if (utf8_len == 1) {
            word[i] = (::isupper(word[i]) ? ::tolower(word[i]) : word[i]);
        }
        i += (advance_len - 1);
    }
    
    size_t slow = bom_len;
    size_t fast = i;
    std::string term = word.substr(slow, fast - slow);

    if (fast >= word_size) {
        term_map[term] = 1.0;
        return 0;
    }
    
    if (_punctuation_blank.count(term) == 0) {
        term_map[term] = 0;
    }

    while (fast < word_size) {
        // slow
        {
            const size_t nremaining  = word_size - slow;
            const size_t utf8_len    = get_utf8_len(word[slow]);
            const size_t advance_len = (utf8_len < nremaining ? utf8_len : nremaining);
            slow += advance_len;
        }
        // fast
        {
            const size_t nremaining  = word_size - fast;
            const size_t utf8_len    = get_utf8_len(word[fast]);
            const size_t advance_len = (utf8_len < nremaining ? utf8_len : nremaining);
            if (1 == utf8_len) {
                word[fast] = (::isupper(word[fast]) ? ::tolower(word[fast]) : word[fast]);
            } 
            fast += advance_len;
        }
        term = word.substr(slow, fast - slow);
        if (_punctuation_blank.count(term) == 1) {
            continue;
        }
        if (term_map.find(term) == term_map.end()) {
            term_map[term] = 0;
        }
    }

    if (term_map.size() > 0) {
        float weight = 1.0 / term_map.size();
        for (auto& pair : term_map) {
            pair.second = weight;
        }
    }

    return 0;
}

void Tokenizer::split_str_utf8(const std::string& word, std::vector<std::string>& split_word, char delim) {
    if (word.empty()) {
        return;
    }

    // process BOM
    const size_t bom_len = get_utf8_bom_len(word);

    size_t i = bom_len;
    // 去除前后%，适配like
    if (word[i] == '%') {
        ++i;
    }

    size_t word_size = word.size();
    if (word[word_size - 1] == '%') {
        --word_size;
    }

    size_t last = i;
    for (; i < word_size; ++i) {
        if (word[i] == delim) {
            if (i - last > 0) {
                split_word.emplace_back(std::move(word.substr(last, i - last)));
            }
            last = i + 1;
            continue;
        }

        const size_t nremaining  = word_size - i;
        const size_t utf8_len    = get_utf8_len(word[i]);
        const size_t advance_len = (utf8_len < nremaining ? utf8_len : nremaining);
        i += (advance_len - 1);
    }

    if (i - last > 0) {
        split_word.emplace_back(std::move(word.substr(last, i - last)));
    }
}

bool is_prefix_end(std::unique_ptr<myrocksdb::Iterator>& iterator, uint8_t level) {
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
