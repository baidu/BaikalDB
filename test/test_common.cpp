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
#include <climits>
#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <cstdint>
#include <boost/regex.hpp>
#include <boost/locale.hpp>
#include <locale>
#ifdef BAIDU_INTERNAL
#include <pb_to_json.h>
#else
#include <json2pb/pb_to_json.h>
#endif
#include <proto/meta.interface.pb.h>
#include "rapidjson.h"
#include <raft/raft.h>
#include <bvar/bvar.h>
#include "common.h"
#include "password.h"
#include "schema_factory.h"

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace baikaldb {
TEST(test_exmaple, case_all) {
    boost::regex partion_key_pattern("[0-9.]+[eE][+-][0-9]+");
    boost::cmatch what;  
    auto aa = boost::regex_search("insert into user_acct (cop_userid, fc_userid) values (1123, 1.0E+7)", what, partion_key_pattern);
    std::cout << "aa:" << aa << std::endl;
    std::cout << sizeof(std::size_t) << std::endl;
    return;
    bvar::Adder<char>  test;
    bvar::Window<bvar::Adder<char>> test_minute(&test, 6);
    for (int i = 0; i < 266; i++) {
        test << 1;
        std::cout << (int)test.get_value() << " " << (int)test_minute.get_value() << std::endl;
        usleep(1000000);
    }
}
TEST(test_stripslashes, case_all) {
    std::cout << 
        ("\x26\x4f\x37\x58"
        "\x43\x7a\x6c\x53"
        "\x21\x25\x65\x57"
        "\x62\x35\x42\x66"
        "\x6f\x34\x62\x49") << std::endl; 
    uint16_t a1 = -1;
    uint16_t a2 = 2;
    int64_t xx =  a2 - a1;
    uint32_t xx2 = -1 % 23;
    std::cout << xx2 << ":aaa\n";
    std::string str = "\\%\\a\\t";
    std::cout << "orgin:" << str << std::endl;
    stripslashes(str);
    std::cout << "new:" << str << std::endl;
    EXPECT_STREQ(str.c_str(), "\\%a\t");

    std::string str2 = "abc";
    std::cout << "orgin:" << str2 << std::endl;
    stripslashes(str2);
    std::cout << "new:" << str2 << std::endl;
    EXPECT_STREQ(str2.c_str(), "abc");
}

TEST(test_pb2json, pb2json) {
    std::cout << sizeof(pb::RegionInfo) << " " << sizeof(rapidjson::StringBuffer) 
    << " " << sizeof(Pb2JsonOptions) << " " << sizeof(TableInfo) << " " << sizeof(pb::SchemaInfo) << "\n";
    std::cout << sizeof(raft::NodeOptions) << " " << sizeof(std::string) << " " << sizeof(size_t) << " "
    << sizeof(raft::PeerId) << sizeof(butil::EndPoint) << "\n"; 
}

TEST(test_cond, wait) {
    
    BthreadCond cond;
    for (int i = 0; i < 10; i++) {
        Bthread bth;
        // increase一定要在主线程里
        cond.increase();
        bth.run([&cond] () {
            bthread_usleep(1000 * 1000);
            DB_NOTICE("cond test");
            cond.decrease_signal();
        });
    }
    cond.wait();
    DB_NOTICE("all bth done");
    sleep(1);
    {
        BthreadCond* concurrency_cond = new BthreadCond(-4);
        for (int i = 0; i < 10; i++) {
            Bthread bth;
            bth.run([concurrency_cond] () {
                    // increase_wait 放在函数中，需要确保concurrency_cond生命周期不结束
                    concurrency_cond->increase_wait();
                    DB_NOTICE("concurrency_cond2 entry");
                    bthread_usleep(1000 * 1000);
                    DB_NOTICE("concurrency_cond2 out");
                    concurrency_cond->decrease_broadcast();
                    });
        }
        DB_NOTICE("concurrency_cond2 all bth done");
    }

    {
        BthreadCond concurrency_cond(-5);
        for (int i = 0; i < 10; i++) {
            Bthread bth;
            // increase一定要在主线程里
            concurrency_cond.increase_wait();
            bth.run([&concurrency_cond] () {
                    DB_NOTICE("concurrency_cond entry");
                    bthread_usleep(1000 * 1000);
                    DB_NOTICE("concurrency_cond out");
                    concurrency_cond.decrease_signal();
                    });
        }
        concurrency_cond.wait(-5);
        DB_NOTICE("concurrency_cond all bth done");
    }
    sleep(1);

    {
        BthreadCond concurrency_cond;
        for (int i = 0; i < 10; i++) {
            Bthread bth;
            // increase一定要在主线程里
            concurrency_cond.increase_wait(5);
            bth.run([&concurrency_cond] () {
                    DB_NOTICE("concurrency_cond entry");
                    bthread_usleep(1000 * 1000);
                    DB_NOTICE("concurrency_cond out");
                    concurrency_cond.decrease_signal();
                    });
        }
        concurrency_cond.wait();
        DB_NOTICE("concurrency_cond all bth done");
    }

}

TEST(test_ConcurrencyBthread, wait) {
    ConcurrencyBthread con_bth(5);
    for (int i = 0; i < 10; i++) {
        //auto call = [i] () {
        //    bthread_usleep(1000 * 1000);
        //    DB_NOTICE("test_ConcurrencyBthread test %d", i);
        //};
        //con_bth.run(call);
        con_bth.run([i] () {
            bthread_usleep(1000 * 1000);
            DB_NOTICE("test_ConcurrencyBthread test %d", i);
        });
    }
    con_bth.join();
    DB_NOTICE("all bth done");
}
TEST(test_bthread_usleep_fast_shutdown, bthread_usleep_fast_shutdown) {
    bool shutdown = false;
    Bthread bth;
    bth.run([&]() {
            DB_NOTICE("before sleep");
            TimeCost cost;
            bthread_usleep_fast_shutdown(1000000, shutdown);
            DB_NOTICE("after sleep %ld", cost.get_time());
            ASSERT_LT(cost.get_time(), 300000);
            cost.reset();
            bool shutdown2 = false;
            bthread_usleep_fast_shutdown(1000000, shutdown2);
            DB_NOTICE("after sleep2 %ld", cost.get_time());
            ASSERT_GE(cost.get_time(), 1000000);
    });
    bthread_usleep(20000);
    shutdown=true;
    bth.join();
}

TEST(ThreadSafeMap, set) {
    ThreadSafeMap<int64_t, std::string> map;
    map.set(1123124, "abc");
    map.set(2, "b");
    map.set(3, "c");
    map.set(4, "d");
    {
        std::string str = map.get(1123124);
        ASSERT_STREQ(str.c_str(), "abc");
    }
    {
        std::string str = map.get(2);
        ASSERT_STREQ(str.c_str(), "b");
    }
    {
        std::string str = map.get(3);
        ASSERT_STREQ(str.c_str(), "c");
    }
    {
        std::string str = map.get(4);
        ASSERT_STREQ(str.c_str(), "d");
    }
}

TEST(ThreadSafeMap, count) {
    ThreadSafeMap<int64_t, std::string> map;
    map.set(1, "abc");
    map.set(2, "b");
    map.set(300, "c");
    map.set(4, "d");
    {
        ASSERT_EQ(map.count(1), 1);
    }
    {
        ASSERT_EQ(map.count(300), 1);
    }
    {
        ASSERT_EQ(map.count(5), 0);
    }
}

TEST(BvarMap, bvarmap) {
    bvar::Adder<BvarMap> bm;
    //bm << BvarMap(std::make_pair("abc", 1));
    bm << BvarMap("abc", 1, 101, 10, 1, 5, 3);
    bm << BvarMap("abc", 4, 102, 20, 2, 6, 2);
    bm << BvarMap("bcd", 5, 103, 30, 3, 7, 1);
    std::cout << bm.get_value();
}

TEST(test_gbk_regex, match) {

    //招 gbk码点
    std::string zhao1{".*\xCD\xB6.*"};
    //如何避免无效点击和恶意点击 gbk码点
    std::string val1{"\xC8\xE7\xBA\xCE\xB1\xDC\xC3\xE2\xCE\xDE\xD0\xA7\xB5\xE3\xBB\xF7\xBA\xCD\xB6\xF1\xD2\xE2\xB5\xE3\xBB\xF7"};
    // char匹配
    std::cout << "regex match result : " << boost::regex_match(val1, boost::regex(zhao1)) << '\n';
    // wchar 匹配
    // 转换成utf8字符，转换成宽字符
    auto wzhao1 = boost::locale::conv::utf_to_utf<wchar_t>(
        boost::locale::conv::to_utf<char>(zhao1, "gbk"));

    auto wzhao_regex = boost::wregex(wzhao1);
    auto wval1 = boost::locale::conv::utf_to_utf<wchar_t>(
        boost::locale::conv::to_utf<char>(val1, "gbk"));

    std::cout << "wregex match result : " << boost::regex_match(wval1, wzhao_regex) << '\n';
}

}  // namespace baikal
