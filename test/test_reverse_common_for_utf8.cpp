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

#include <gtest/gtest.h>
#include <iostream>
#include <fstream>
#include <string>
#include "reverse_common.h"
#include "reverse_index.h"
#include "reverse_interface.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace baikaldb {

// TEST iconv
TEST(test_gbk_to_utf8, case_all) {
    int ret = 0;
    {
        std::ifstream fin("./data/gbk_input.txt");
        std::ofstream fout("./data/utf8_output.txt");
        while (fin.good()) {
            std::string src {};
            std::getline(fin, src);
            if (src.empty()) {
                continue;
            }
            std::string dst {};
            ret = iconv_convert<pb::UTF8, pb::GBK>(dst, &src[0], src.size());
            EXPECT_TRUE(0 == ret);
            fout << dst << std::endl;
        }
        fin.close();
        fout.close();
    }
    {
        std::ifstream fin("./data/gbk_error_input.txt");
        std::ofstream fout("./data/utf8_error_output.txt");
        while (fin.good()) {
            std::string src {};
            std::getline(fin, src);
            if (src.empty()) {
                continue;
            }
            std::string dst {};
            ret = iconv_convert<pb::UTF8, pb::GBK>(dst, &src[0], src.size());
            EXPECT_FALSE(0 == ret);
            fout << dst << std::endl;
        }
        fin.close();
        fout.close();
    }
}

TEST(test_utf8_to_gbk, case_all) {
    int ret = 0;
    {
        std::ifstream fin("./data/utf8_input.txt");
        std::ofstream fout("./data/gbk_output.txt");
        while (fin.good()) {
            std::string src {};
            std::getline(fin, src);
            if (src.empty()) {
                continue;
            }
            std::string dst {};
            ret = iconv_convert<pb::GBK, pb::UTF8>(dst, &src[0], src.size());
            EXPECT_TRUE(0 == ret);
            fout << dst << std::endl;
        }
        fin.close();
        fout.close();
    }
    {
        std::ifstream fin("./data/utf8_error_input.txt");
        std::ofstream fout("./data/gbk_error_output.txt");
        while (fin.good()) {
            std::string src {};
            std::getline(fin, src);
            if (src.empty()) {
                continue;
            }
            std::string dst {};
            ret = iconv_convert<pb::GBK, pb::UTF8>(dst, &src[0], src.size());
            EXPECT_FALSE(0 == ret);
            fout << dst << std::endl;
        }
        fin.close();
        fout.close();
    }
}

// TEST Tokenizer
TEST(test_q2b_tolower, case_all) {
    Tokenizer::get_instance()->init();
    {
        std::string word = "，";
        Tokenizer::get_instance()->q2b_tolower(word, pb::UTF8);
        std::cout << word << std::endl;
        ASSERT_STREQ(word.c_str(), ",");
    }
    {
        std::string word = "A";
        Tokenizer::get_instance()->q2b_tolower(word, pb::UTF8);
        std::cout << word << std::endl;
        ASSERT_STREQ(word.c_str(), "a");
    }
    {
        std::string word = "1";
        Tokenizer::get_instance()->q2b_tolower(word, pb::UTF8);
        std::cout << word << std::endl;
        ASSERT_STREQ(word.c_str(), "1");
    }
    {
        std::string word = "是";
        Tokenizer::get_instance()->q2b_tolower(word, pb::UTF8);
        std::cout << word << std::endl;
        ASSERT_STREQ(word.c_str(), "是");
    }
    Tokenizer::get_instance()->init();
    {
        std::string word = "p.c1+11.1?-WWW营业营Ｈｅｌｌｏ　ｗｏｒｌｄ！０１２３７２１执照（精确）";
        Tokenizer::get_instance()->q2b_tolower(word, pb::UTF8);
        std::cout << word << std::endl;
        ASSERT_STREQ(word.c_str(), "p.c1+11.1?-www营业营hello world!0123721执照(精确)");
    }
}

TEST(test_split_str, case_all) {
    Tokenizer::get_instance()->init();
    {
        std::string word = "%||||%";
        std::vector<std::string> split_vec;
        Tokenizer::get_instance()->split_str(word, split_vec, '|', pb::UTF8);
        std::cout << " size:" << split_vec.size() << std::endl;
        for (auto& i : split_vec) {
            std::cout << i << std::endl;
        }
        ASSERT_EQ(0, split_vec.size());
    }
    {
        std::string word = "%4%";
        std::vector<std::string> split_vec;
        Tokenizer::get_instance()->split_str(word, split_vec, '|', pb::UTF8);
        std::cout << " size:" << split_vec.size() << std::endl;
        for (auto& i : split_vec) {
            std::cout << i << std::endl;
        }
        ASSERT_EQ(1, split_vec.size());
    }
    {
        std::string word = "%4|";
        std::vector<std::string> split_vec;
        Tokenizer::get_instance()->split_str(word, split_vec, '|', pb::UTF8);
        std::cout << " size:" << split_vec.size() << std::endl;
        for (auto& i : split_vec) {
            std::cout << i << std::endl;
        }
        ASSERT_EQ(1, split_vec.size());
    }
    {
        std::string word = "1|22|3";
        std::vector<std::string> split_vec;
        Tokenizer::get_instance()->split_str(word, split_vec, '|', pb::UTF8);
        std::cout << " size:" << split_vec.size() << std::endl;
        for (auto& i : split_vec) {
            std::cout << i << std::endl;
        }
        ASSERT_EQ(3, split_vec.size());
    }
    {
        std::string word = "1||22|3%";
        std::vector<std::string> split_vec;
        Tokenizer::get_instance()->split_str(word, split_vec, '|', pb::UTF8);
        std::cout << " size:" << split_vec.size() << std::endl;
        for (auto& i : split_vec) {
            std::cout << i << std::endl;
        }
        ASSERT_EQ(3, split_vec.size());
    }
    {
        std::string word = "|a| |ba&&a|";
        std::vector<std::string> split_vec;
        Tokenizer::get_instance()->split_str(word, split_vec, '|', pb::UTF8);
        std::cout << " size:" << split_vec.size() << std::endl;
        for (auto& i : split_vec) {
            std::cout << i << std::endl;
        }
        ASSERT_EQ(3, split_vec.size());
    }
    {
        std::string word = "%是|aa|啊%";
        std::vector<std::string> split_vec;
        Tokenizer::get_instance()->split_str(word, split_vec, '|', pb::UTF8);
        std::cout << " size:" << split_vec.size() << std::endl;
        for (auto& i : split_vec) {
            std::cout << i << std::endl;
        }
        ASSERT_EQ(3, split_vec.size());
    }
    {
        std::string word = "|我| |啊|";
        std::vector<std::string> split_vec;
        Tokenizer::get_instance()->split_str(word, split_vec, '|', pb::UTF8);
        std::cout << " size:" << split_vec.size() << std::endl;
        for (auto& i : split_vec) {
            std::cout << i << std::endl;
        }
        ASSERT_EQ(3, split_vec.size());
    }
}
TEST(test_simple_seg_utf8, case_all) {
    Tokenizer::get_instance()->init();
    {
        std::string word = "06-JO [整外] 胸部-胸综合";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg(word, 1, term_map, pb::UTF8);
        std::cout << word << " size:" << term_map.size() << std::endl;
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
        ASSERT_EQ(10, term_map.size());
    }
    {
        std::string word = "a";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg(word, 1, term_map, pb::UTF8);
        std::cout << word << " size:" << term_map.size() << std::endl;
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
        ASSERT_EQ(1, term_map.size());
    }
    {
        std::string word = "a";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg(word, 2, term_map, pb::UTF8);
        std::cout << word << " size:" << term_map.size() << std::endl;
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
        ASSERT_EQ(1, term_map.size());
    }
    {
        std::string word = "a是";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg(word, 2, term_map, pb::UTF8);
        std::cout << word << " size:" << term_map.size() << std::endl;
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
        ASSERT_EQ(1, term_map.size());
    }
    {
        std::string word = "我a是c";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg(word, 1, term_map, pb::UTF8);
        std::cout << word << " size:" << term_map.size() << std::endl;
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
        ASSERT_EQ(4, term_map.size());
    }
    {
        std::string word = "我A是c";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg(word, 2, term_map, pb::UTF8);
        std::cout << word << " size:" << term_map.size() << std::endl;
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
        ASSERT_EQ(3, term_map.size());
    }
    {
        std::string word = "我A 是c";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg(word, 2, term_map, pb::UTF8);
        std::cout << word << " size:" << term_map.size() << std::endl;
        ASSERT_EQ(4, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "我是谁!";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg(word, 2, term_map, pb::UTF8);
        std::cout << word << " size:" << term_map.size() << std::endl;
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
        ASSERT_EQ(3, term_map.size());
    }
    {
        std::string word = "我A是c";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg(word, 3, term_map, pb::UTF8);
        std::cout << word << " size:" << term_map.size() << std::endl;
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
        ASSERT_EQ(2, term_map.size());
    }
    {
        std::string word = "怎样给异地的朋友订鲜花";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg(word, 1, term_map, pb::UTF8);
        std::cout << word << " size:" << term_map.size() << std::endl;
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
        ASSERT_EQ(11, term_map.size());
    }
    {
        std::string word = "UPPERCASETEST";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg(word, 2, term_map, pb::UTF8);
        std::cout << word << " size:" << term_map.size() << std::endl;
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
        ASSERT_EQ(12, term_map.size());
        ASSERT_EQ(1, term_map.count("er"));
    }
    {
        std::string word = "UPPERCAS测试ETEST";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg(word, 1, term_map, pb::UTF8);
        std::cout << word << " size:" << term_map.size() << std::endl;
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
        ASSERT_EQ(1, term_map.count("e"));
        ASSERT_EQ(1, term_map.count("测"));
    }
    {
        // Verify BOM
        std::string word {};
        word.push_back(0xEF);
        word.push_back(0xBB);
        word.push_back(0xBF);
        word.push_back('a');
        word.push_back('b');
        word.push_back('c');
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg(word, 2, term_map, pb::UTF8);
        std::cout << word << " size:" << term_map.size() << std::endl;
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
        ASSERT_EQ(2, term_map.size());
    }
    {
        // Verify BOM
        std::string word {};
        word.push_back(0xEF);
        word.push_back('a');
        word.push_back('b');
        word.push_back('c');
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg(word, 2, term_map, pb::UTF8);
        std::cout << word << " size:" << term_map.size() << std::endl;
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
        ASSERT_EQ(1, term_map.size());
    }
}

TEST(test_es_standard_utf8, case_all) {
    Tokenizer::get_instance()->init();
    {
        std::string word = "a";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard(word, term_map, pb::UTF8);
        std::cout << word << "size:" << term_map.size() << std::endl;
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
        ASSERT_EQ(1, term_map.size());
    }
    {
        std::string word = "a是";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard(word, term_map, pb::UTF8);
        std::cout << word << "size:" << term_map.size() << std::endl;
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
        ASSERT_EQ(2, term_map.size());
    }
    {
        std::string word = "我a是c";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard(word, term_map, pb::UTF8);
        std::cout << word << "size:" << term_map.size() << std::endl;
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
        ASSERT_EQ(4, term_map.size());
    }
    {
        std::string word = "我A是c";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard(word, term_map, pb::UTF8);
        std::cout << word << "size:" << term_map.size() << std::endl;
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
        ASSERT_EQ(4, term_map.size());
    }
    {
        std::string word = "我是谁!";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard(word, term_map, pb::UTF8);
        std::cout << word << "size:" << term_map.size() << std::endl;
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
        ASSERT_EQ(3, term_map.size());
    }
    {
        std::string word = "p.c1+11.1?-营业营执照（精确）";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard(word, term_map, pb::UTF8);
        std::cout << word << " size:" << term_map.size() << std::endl;
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
        ASSERT_EQ(10, term_map.size());
    }
    {
        std::string word = "p.c1+11.1?-营业营Ｈｅｌｌｏ　ｗｏｒｌｄ！０１２３７２１执照（精确）";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard(word, term_map, pb::UTF8);
        std::cout << word << " size:" << term_map.size() << std::endl;
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
        ASSERT_EQ(13, term_map.size());
    }
    {
        std::string word = "天津农信达农业,多年从业经验的{关键词}{大棚管生产厂家},拥有农业科技,建筑设计团队.";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard(word, term_map, pb::UTF8);
        std::cout << word << " size:" << term_map.size() << std::endl;
        for (auto& i : term_map) {
           std::cout << i.first << std::endl;
        }
        ASSERT_EQ(32, term_map.size());
    }
    {
        std::string word = "0C2-";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard(word, term_map, pb::UTF8);
        std::cout << word << " size:" << term_map.size() << std::endl;
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
        ASSERT_EQ(3, term_map.size());
    }
    {
        std::string word = "KS02-C2-肉毒-瘦脸";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard(word, term_map, pb::UTF8);
        std::cout << word << " size:" << term_map.size() << std::endl;
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
        ASSERT_EQ(8, term_map.size());
    }
}

}  // namespace baikal