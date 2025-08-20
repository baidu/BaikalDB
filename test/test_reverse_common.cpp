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
#include <string.h>
#include <climits>
#include <iostream>
#include <cstdio>
#include <fstream>
#include <cstdlib>
#include <ctime>
#include <cstdint>
#include "rapidjson.h"
#include <raft/raft.h>
#include <bvar/bvar.h>
#include "reverse_common.h"
#include "reverse_index.h"
#include "reverse_interface.h"
#include "transaction_pool.h"
#include "transaction.h"
#include "rocks_wrapper.h"
#include "proto/meta.interface.pb.h"

int my_argc;
char** my_argv;

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    my_argc = argc;
    my_argv = argv;
    return RUN_ALL_TESTS();
}

namespace baikaldb {
TEST(float_parse_cost, case_all) {
    void from_chars_to_float_vec(const std::string& str, std::vector<float>& vec);
    auto call = [&](std::string vector_index){
        std::vector<float> flt_vec;
        std::cout << vector_index << "\n";
        from_chars_to_float_vec(vector_index, flt_vec);
        std::vector<float> flt_vec2;
        std::vector<std::string> split_vec;
        boost::trim_if(vector_index, boost::is_any_of(" []"));
        boost::split(split_vec, vector_index,
                boost::is_any_of(" ,"), boost::token_compress_on);
        for (auto& str : split_vec) {
            flt_vec2.emplace_back(strtof(str.c_str(), NULL));
        }
        EXPECT_EQ(flt_vec.size(), flt_vec2.size());
        for (size_t i = 0; i < flt_vec.size(); i++) {
            EXPECT_EQ(flt_vec[i], flt_vec2[i]);
            std::cout << flt_vec[i] << "\n";
        }
    };
    call("0.12472, -13.1821465  , 0.1356781,0.265489");
    call("0.1272,-13.1821465,0.1356,0.2689");
    call("0.1272,-13.1821465e-13   ,     0.1356,0.2689,   0.113");
    call(" 0.1272,-13.1821465e-13   ,     0.1356,0.2689,   0.113 ");
    call("[0.1272,-13.1821465e-13   ,     0.1356,0.2689,   0.113]");
    call(" [  ] ");
}
TEST(test_gbk_substr, case_all) {
    Tokenizer::get_instance()->init();
    std::string gbk_substr(const std::string& word, size_t len);
    {
        std::string word = "我是a你好c中";
        std::string word0 = gbk_substr(word, 0);
        std::cout << word0 << std::endl;
        ASSERT_STREQ(word0.c_str(), "");
        std::string word3 = gbk_substr(word, 3);
        std::cout << word3 << std::endl;
        ASSERT_STREQ(word3.c_str(), "我是a");
        std::string word4 = gbk_substr(word, 4);
        std::cout << word4 << std::endl;
        ASSERT_STREQ(word4.c_str(), "我是a你");
        std::string word100 = gbk_substr(word, 100);
        std::cout << word100 << std::endl;
        ASSERT_STREQ(word100.c_str(), "我是a你好c中");
    }
}
TEST(test_q2b_tolower_gbk, case_all) {
    Tokenizer::get_instance()->init();
    {
        std::string word = "，";
        Tokenizer::get_instance()->q2b_tolower_gbk(word);
        std::cout << word << std::endl;
        ASSERT_STREQ(word.c_str(), ",");
    }
    {
        std::string word = "A";
        Tokenizer::get_instance()->q2b_tolower_gbk(word);
        std::cout << word << std::endl;
        ASSERT_STREQ(word.c_str(), "a");
    }
    {
        std::string word = "1";
        Tokenizer::get_instance()->q2b_tolower_gbk(word);
        std::cout << word << std::endl;
        ASSERT_STREQ(word.c_str(), "1");
    }
    {
        std::string word = "是";
        Tokenizer::get_instance()->q2b_tolower_gbk(word);
        std::cout << word << std::endl;
        ASSERT_STREQ(word.c_str(), "是");
    }
    Tokenizer::get_instance()->init();
    {
        std::string word = "p.c1+11.1?-WWW营业营Ｈｅｌｌｏ　ｗｏｒｌｄ！０１２３７２１执照（精确）";
        Tokenizer::get_instance()->q2b_tolower_gbk(word);
        std::cout << word << std::endl;
        ASSERT_STREQ(word.c_str(), "p.c1+11.1?-www营业营hello world!0123721执照(精确)");
    }
}

TEST(test_split_str_gbk, case_all) {
    Tokenizer::get_instance()->init();
    {
        std::string word = "%||||%";
        std::vector<std::string> split_vec;
        Tokenizer::get_instance()->split_str_gbk(word, split_vec, '|');
        std::cout << "size:" << split_vec.size() << std::endl;
        ASSERT_EQ(0, split_vec.size());
        for (auto& i : split_vec) {
            std::cout << i << std::endl;
        }
    }
    {
        std::string word = "%4%";
        std::vector<std::string> split_vec;
        Tokenizer::get_instance()->split_str_gbk(word, split_vec, '|');
        std::cout << "size:" << split_vec.size() << std::endl;
        ASSERT_EQ(1, split_vec.size());
        for (auto& i : split_vec) {
            std::cout << i << std::endl;
        }
    }
    {
        std::string word = "%4|";
        std::vector<std::string> split_vec;
        Tokenizer::get_instance()->split_str_gbk(word, split_vec, '|');
        std::cout << "size:" << split_vec.size() << std::endl;
        ASSERT_EQ(1, split_vec.size());
        for (auto& i : split_vec) {
            std::cout << i << std::endl;
        }
    }
    {
        std::string word = "1|22|3";
        std::vector<std::string> split_vec;
        Tokenizer::get_instance()->split_str_gbk(word, split_vec, '|');
        std::cout << "size:" << split_vec.size() << std::endl;
        ASSERT_EQ(3, split_vec.size());
        for (auto& i : split_vec) {
            std::cout << i << std::endl;
        }
    }
    {
        std::string word = "1||22|3%";
        std::vector<std::string> split_vec;
        Tokenizer::get_instance()->split_str_gbk(word, split_vec, '|');
        std::cout << "size:" << split_vec.size() << std::endl;
        ASSERT_EQ(3, split_vec.size());
        for (auto& i : split_vec) {
            std::cout << i << std::endl;
        }
    }
    {
        std::string word = "|a| |ba&&a|";
        std::vector<std::string> split_vec;
        Tokenizer::get_instance()->split_str_gbk(word, split_vec, '|');
        std::cout << "size:" << split_vec.size() << std::endl;
        ASSERT_EQ(3, split_vec.size());
        for (auto& i : split_vec) {
            std::cout << i << std::endl;
        }
    }
    {
        std::string word = "%是|aa|啊%";
        std::vector<std::string> split_vec;
        Tokenizer::get_instance()->split_str_gbk(word, split_vec, '|');
        std::cout << "size:" << split_vec.size() << std::endl;
        ASSERT_EQ(3, split_vec.size());
        for (auto& i : split_vec) {
            std::cout << i << std::endl;
        }
    }
    {
        std::string word = "|我| |啊|";
        std::vector<std::string> split_vec;
        Tokenizer::get_instance()->split_str_gbk(word, split_vec, '|');
        std::cout << "size:" << split_vec.size() << std::endl;
        ASSERT_EQ(3, split_vec.size());
        for (auto& i : split_vec) {
            std::cout << i << std::endl;
        }
    }
}
TEST(test_simple_seg_gbk, case_all) {
    Tokenizer::get_instance()->init();
    {
        std::string word = "06-JO [整外] 胸部-胸综合";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg_gbk(word, 1, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        //ASSERT_EQ(1, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "a";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg_gbk(word, 1, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        ASSERT_EQ(1, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "a";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg_gbk(word, 2, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        ASSERT_EQ(1, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "a是";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg_gbk(word, 2, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        ASSERT_EQ(1, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "我a是c";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg_gbk(word, 1, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        ASSERT_EQ(4, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "我A是c";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg_gbk(word, 2, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        ASSERT_EQ(3, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "我A 是c";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg_gbk(word, 2, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        ASSERT_EQ(4, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "我是谁!";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg_gbk(word, 2, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        ASSERT_EQ(3, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "我A是c";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg_gbk(word, 3, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        ASSERT_EQ(2, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "怎样给异地的朋友订鲜花";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg_gbk(word, 1, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        //ASSERT_EQ(2, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "UPPERCASETEST";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg_gbk(word, 2, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        ASSERT_EQ(12, term_map.size());
        ASSERT_EQ(1, term_map.count("er"));
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "UPPERCAS测试ETEST";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->simple_seg_gbk(word, 1, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        ASSERT_EQ(1, term_map.count("e"));
        ASSERT_EQ(1, term_map.count("测"));
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
}

TEST(test_es_standard_gbk, case_all) {
    Tokenizer::get_instance()->init();
    {
        std::string word = "a";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard_gbk(word, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        ASSERT_EQ(1, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "a是";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard_gbk(word, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        ASSERT_EQ(2, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "我a是c";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard_gbk(word, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        ASSERT_EQ(4, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "我A是c";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard_gbk(word, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        ASSERT_EQ(4, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "我是谁!";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard_gbk(word, term_map);
        std::cout << word << "size:" << term_map.size() << std::endl;
        ASSERT_EQ(3, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "p.c1+11.1?-营业营执照（精确）";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard_gbk(word, term_map);
        std::cout << word << " size:" << term_map.size() << std::endl;
        ASSERT_EQ(10, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "p.c1+11.1?-营业营Ｈｅｌｌｏ　ｗｏｒｌｄ！０１２３７２１执照（精确）";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard_gbk(word, term_map);
        std::cout << word << " size:" << term_map.size() << std::endl;
        ASSERT_EQ(13, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "天津农信达农业,多年从业经验的{关键词}{大棚管生产厂家},拥有农业科技,建筑设计团队.";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard_gbk(word, term_map);
        std::cout << word << " size:" << term_map.size() << std::endl;
        ASSERT_EQ(32, term_map.size());
        //for (auto& i : term_map) {
        //    std::cout << i.first << std::endl;
        //}
    }
    {
        std::string word = "0C2-";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard_gbk(word, term_map);
        std::cout << word << " size:" << term_map.size() << std::endl;
        //ASSERT_EQ(7, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
    {
        std::string word = "KS02-C2-肉毒-瘦脸";
        std::vector<std::string> split_vec;
        std::map<std::string, float> term_map;
        Tokenizer::get_instance()->es_standard_gbk(word, term_map);
        std::cout << word << " size:" << term_map.size() << std::endl;
        ASSERT_EQ(8, term_map.size());
        for (auto& i : term_map) {
            std::cout << i.first << std::endl;
        }
    }
}

template<typename IndexType>
void arrow_test(std::string db_path, std::string word_file, const char* search_word_file) {
    auto rocksdb = RocksWrapper::get_instance();
    if (!rocksdb) {
        std::cout << "create rocksdb handler failed";
    }
    rocksdb->init(db_path.c_str());
    auto arrow_index = new IndexType(
        1, 
        1,
        5000,
        rocksdb,
        pb::GBK,
        pb::S_UNIGRAMS,
        false, // common need not cache
        true);

    std::ifstream file(word_file);
    if (!file) {
        std::cout << "no word file " << word_file;
        return;
    }
    std::string line;
    int64_t i = 0;
    while (std::getline(file, line)) {
        if (i % 10000 == 0) {
            std::cout << "insert " << i << '\n';
        }
        auto smart_transaction = std::make_shared<TransactionPool>();
        SmartTransaction txn(new Transaction(0, smart_transaction.get())); 
        txn->begin(Transaction::TxnOptions());
        std::string pk = std::to_string(i++);
        arrow_index->insert_reverse(txn, line, pk, nullptr);
        auto res = txn->commit();
        if (!res.ok()) {
            std::cout << "commit error\n";
        }
    }
    bool stop_merge = false;
    Bthread merge_thread;

    std::string key;
    int8_t region_encode = '\0';
    key.append((char*)&region_encode, sizeof(int8_t));
    
    std::string end_key;
    const uint64_t max = UINT64_MAX;
    end_key.append((char*)&max, sizeof(uint64_t));

    merge_thread.run([&stop_merge, &arrow_index, &key, &end_key](){
        while (!stop_merge) {
            pb::RegionInfo region_info;
            region_info.set_start_key(key.data());
            region_info.set_end_key(end_key.data());
            arrow_index->reverse_merge_func(region_info, false);
            bthread_usleep(1000);
        }
    });
    bthread_usleep(60000000);
    stop_merge = true;
    merge_thread.join();
    std::cout << "merge over\n";

    std::ifstream search_file(search_word_file);
    if (!search_file) {
        std::cout << "no word file ";
        return;
    }
    std::string search_line;
    while (std::getline(search_file, search_line)) {

        std::cout << "valid search word : " << search_line << '\n';
        for (auto i = 0; i < 1; ++i) {
            TimeCost tc_all;
            SmartTable ti(new TableInfo);
            SmartIndex ii(new IndexInfo);
            std::vector<ExprNode*> _con;
            auto smart_transaction = std::make_shared<TransactionPool>();
            SmartTransaction txn(new Transaction(0, smart_transaction.get())); 
            txn->begin(Transaction::TxnOptions());
            TimeCost tc;
            arrow_index->search(txn->get_txn(), ii, ti, search_line, pb::M_NONE, _con, true);
            std::cout << "valid reverse time[" << tc.get_time() << "]\n";
            TimeCost tc2;
            uint64_t valid_num = 0;
            while (arrow_index->valid()) {
                ++valid_num;
            }
            std::cout << "valid number [" << valid_num << "] time [" << tc2.get_time() << "] all_time[" << tc_all.get_time() << "]\n";
            txn->commit();
        }
    }
}

TEST(test_arrow_pb, case_all) {
    if (my_argc < 3) {
        return;
    }

    if (!strcmp(my_argv[1], "arrow")) {
        std::cout << "test arrow\n";
        arrow_test<ReverseIndex<ArrowSchema>>("./rocksdb", "word", my_argv[2]);

    } else {
        arrow_test<ReverseIndex<CommonSchema>>("./rocksdb", "word", my_argv[2]);
    }
}

}  // namespace baikal
