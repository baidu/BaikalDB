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
#include <istream>
#include <streambuf>
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
#ifdef BAIDU_INTERNAL
#include <baidu/rpc/channel.h>
#include <baidu/rpc/selective_channel.h>
#else
#include <brpc/channel.h>
#include <brpc/selective_channel.h>
#endif
#include <proto/meta.interface.pb.h>
#include "rapidjson.h"
#include "re2/re2.h"
#include <raft/raft.h>
#include <bvar/bvar.h>
#include "common.h"
#include "password.h"
#include "schema_factory.h"
#include "transaction.h"

int main(int argc, char* argv[])
{
    google::ParseCommandLineFlags(&argc, &argv, true);
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace baikaldb {

class TimePeriodChecker {
public:
    TimePeriodChecker(int start_hour, int end_hour) : _start_hour(start_hour),_end_hour(end_hour) {}

    bool now_in_interval_period(int now) {
        /*struct tm ptm;
        time_t timep = time(NULL);
        localtime_r(&timep, &ptm);
        int now = ptm.tm_hour;*/
        // 跨夜
        if (_end_hour < _start_hour) {
            if (now >= _end_hour && now < _start_hour) {
                return false;
            }
            return true;
        } else {
            if (now >= _start_hour && now < _end_hour) {
                return true;
            }
            return false;  
        }
    }
private:
    int _start_hour;
    int _end_hour;
};

TEST(test_TimePeriodChecker, timechecker) {
    TimePeriodChecker tc1(0, 7);
    TimePeriodChecker tc2(8, 17);
    TimePeriodChecker tc3(18, 7);

    EXPECT_EQ(true, tc1.now_in_interval_period(1));
    EXPECT_EQ(false, tc1.now_in_interval_period(8));
    EXPECT_EQ(false, tc1.now_in_interval_period(18));

    EXPECT_EQ(false, tc2.now_in_interval_period(1));
    EXPECT_EQ(true, tc2.now_in_interval_period(8));
    EXPECT_EQ(false, tc2.now_in_interval_period(18));

    EXPECT_EQ(true, tc3.now_in_interval_period(1));
    EXPECT_EQ(false, tc3.now_in_interval_period(8));
    EXPECT_EQ(true, tc3.now_in_interval_period(18));
}

TEST(test_channel, channel) {
    int64_t aa = 3600 * 1000 * 1000;
    int64_t bb = 3600 * 1000 * 1000L;
    std::cout << aa << ":" << bb << std::endl;
    char buf[100] = "20210907\n";
    std::streambuf sbuf;
    sbuf.setg(buf, buf, buf + 8);
    std::istream f(&sbuf);
    std::string line;
    std::getline(f, line);
    std::cout << "size:" << line.size() << " " << f.eof() << "\n";
    line.clear();
    std::getline(f, line);
    std::cout << "size:" << line.size() << " " << f.eof() << "\n";
    TimeCost cost;
    for (int i = 0; i < 100000; i++) {
        brpc::Channel channel;
        brpc::ChannelOptions option;
        option.max_retry = 1;
        option.connect_timeout_ms = 1000;
        option.timeout_ms = 1000000;
        std::string addr = "10.77.22.157:8225";
        auto ret = channel.Init(addr.c_str(), &option);
        brpc::Controller cntl;
        if (ret != 0) {
            DB_WARNING("error");
        }
    }
    DB_WARNING("normal:%ld,%ld,%s", cost.get_time(),butil::gettimeofday_us(),
        timestamp_to_str(butil::gettimeofday_us()/1000/1000).c_str());
    cost.reset();
    for (int i = 0; i < 100000; i++) {
        brpc::SelectiveChannel channel;
        brpc::ChannelOptions option;
        option.max_retry = 1;
        option.connect_timeout_ms = 1000;
        option.timeout_ms = 1000000;
        option.backup_request_ms = 100000;
        auto ret = channel.Init("rr", &option);
        brpc::Controller cntl;
        if (ret != 0) {
            DB_WARNING("error");
        }
        std::string addr = "10.77.22.157:8225";
        brpc::Channel* sub_channel1 = new brpc::Channel;
        sub_channel1->Init(addr.c_str(), &option);
        channel.AddChannel(sub_channel1, NULL);
    }
    DB_WARNING("selective1:%ld", cost.get_time());
    cost.reset();
    for (int i = 0; i < 100000; i++) {
        brpc::SelectiveChannel channel;
        brpc::ChannelOptions option;
        option.max_retry = 1;
        option.connect_timeout_ms = 1000;
        option.timeout_ms = 1000000;
        option.backup_request_ms = 100000;
        auto ret = channel.Init("rr", &option);
        brpc::Controller cntl;
        if (ret != 0) {
            DB_WARNING("error");
        }
        std::string addr = "10.77.22.157:8225";
        brpc::Channel* sub_channel1 = new brpc::Channel;
        sub_channel1->Init(addr.c_str(), &option);
        channel.AddChannel(sub_channel1, NULL);
        brpc::Channel* sub_channel2 = new brpc::Channel;
        addr = "10.77.22.37:8225";
        sub_channel2->Init(addr.c_str(), &option);
        channel.AddChannel(sub_channel2, NULL);
    }
    DB_WARNING("selective2:%ld", cost.get_time());
}
TEST(test_exmaple, case_all) {
    int a = 10;
    int& b = a;
    int& c = b;
    std::cout << &a << " " << &b << " " << &c << std::endl;
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

TEST(regex_test, re2_regex) {
    re2::RE2::Options option;
    option.set_utf8(false);
    option.set_case_sensitive(false);
    option.set_perl_classes(true);
    re2::RE2 reg("(\\/\\*.*?\\*\\/)(.*)", option);
    std::string sql = "/*{\"ttl_duration\" : 86400}*/select * from t;";
    std::string comment;
    if (!RE2::Extract(sql, reg, "\\1", &comment)) {
        DB_WARNING("extract commit error.");
    }
    EXPECT_EQ(comment, "/*{\"ttl_duration\" : 86400}*/");
    
    if (!RE2::Replace(&(sql), reg, "\\2")) {
        DB_WARNING("extract sql error.");
    }
    EXPECT_EQ(sql, "select * from t;");
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
    stripslashes(str, true);
    std::cout << "new:" << str << std::endl;
    EXPECT_STREQ(str.c_str(), "\\%a\t");

    std::string str2 = "abc";
    std::cout << "orgin:" << str2 << std::endl;
    stripslashes(str2, true);
    std::cout << "new:" << str2 << std::endl;
    EXPECT_STREQ(str2.c_str(), "abc");
}

// TEST(test_pb2json, pb2json) {
//     std::cout << sizeof(pb::RegionInfo) << " " << sizeof(rapidjson::StringBuffer) 
//     << " " << sizeof(Pb2JsonOptions) << " " << sizeof(TableInfo) << " " << sizeof(pb::SchemaInfo) << "\n";
//     std::cout << sizeof(raft::NodeOptions) << " " << sizeof(std::string) << " " << sizeof(size_t) << " "
//     << sizeof(raft::PeerId) << sizeof(butil::EndPoint) << "\n"; 
// }
TEST(test_bthread_timer, timer) {
    
    for (int i = 0; i < 10; i++) {
        BthreadTimer tm;
        tm.run(100 * i, [i] () {
            static thread_local int a;
            int* b = &a;
            *b = i;
            DB_NOTICE("start timer test %d, %d, %p", i, *b, b);
            bthread_usleep(1000 * 1000);
            DB_NOTICE("end timer test %d, %d, %p %p", i, *b, b, &a);
        });
        if (i == 7) {
            tm.stop();
        }
    }
    sleep(3);
}


TEST(test_cond, wait) {
    
    BthreadCond cond;
    for (int i = 0; i < 10; i++) {
        Bthread bth;
        // increase一定要在主线程里
        cond.increase();
        bth.run([&cond, i] () {
            static thread_local int a;
            int* b = &a;
            *b = i;
            DB_NOTICE("start cond test %d, %d, %p", i, *b, b);
            bthread_usleep(1000 * 1000);
            DB_NOTICE("end cond test %d, %d, %p %p", i, *b, b, &a);
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
    std::atomic<bool> shutdown {false};
    bool shutdown2 {false};
    Bthread bth;
    bth.run([&]() {
            DB_NOTICE("before sleep");
            TimeCost cost;
            bthread_usleep_fast_shutdown(1000000, shutdown);
            DB_NOTICE("after sleep %ld", cost.get_time());
            ASSERT_LT(cost.get_time(), 100000);
            cost.reset();
            bthread_usleep_fast_shutdown(1000000, shutdown2);
            DB_NOTICE("after sleep2 %ld", cost.get_time());
            ASSERT_GE(cost.get_time(), 100000);
            cost.reset();
            bool shutdown3 = false;
            bthread_usleep_fast_shutdown(1000000, shutdown3);
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
    std::map<int32_t, int> field_range_type;
    uint64_t parent_sign = 2385825078143366794;
    std::set<uint64_t> subquery_signs = {8394144613061275097, 8919421716185942419};
    //bm << BvarMap(std::make_pair("abc", 1));
    bm << BvarMap("abc", 1, 101, 101, 10, 1, 5, 3, 1, field_range_type, 1, parent_sign, subquery_signs);
    bm << BvarMap("abc", 4, 102, 102, 20, 2, 6, 2, 1, field_range_type, 1, parent_sign, subquery_signs);
    bm << BvarMap("bcd", 5, 103, 103, 30, 3, 7, 1, 1, field_range_type, 1, parent_sign, subquery_signs);
    std::cout << bm.get_value();
}

TEST(LatencyOnly, LatencyOnly) {
    LatencyOnly cost;
    bvar::LatencyRecorder cost2;/*
    for (int i = 0; i < 10; i++) {
        for (int j = 0; j <= i; j++) {
            cost << j * 10;
            cost2 << j * 10;
        }
        std::cout << "i:" << i << std::endl;
        std::cout << "qps:" << cost.qps() << " vs " << cost2.qps() << std::endl;
        std::cout << "latency:" << cost.latency() << " vs " << cost2.latency() << std::endl;
        std::cout << "qps10:" << cost.qps(10) << " vs " << cost2.qps(10) << std::endl;
        std::cout << "latency10:" << cost.latency(10) << " vs " << cost2.latency(10) << std::endl;
        std::cout << "qps5:" << cost.qps(5)<< " vs " << cost2.qps(5) << std::endl;
        std::cout << "latency5:" << cost.latency(5) << " vs " << cost2.latency(5) << std::endl;
        bthread_usleep(1000000);
    }*/
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

DEFINE_int64(gflags_test_int64, 20000, "");
DEFINE_double(gflags_test_double, 0.234, "");
DEFINE_string(gflags_test_string, "abc", "");
DEFINE_bool(gflags_test_bool, true, "");

TEST(gflags_test, case_all) {
    if (!google::SetCommandLineOption("gflags_test_int64", "1000").empty()) {
        DB_WARNING("gflags_test_int64:%ld", FLAGS_gflags_test_int64);
    }
    if (!google::SetCommandLineOption("gflags_test_double", "0.123").empty()) {
        DB_WARNING("gflags_test_double:%f", FLAGS_gflags_test_double);
    }
    if (!google::SetCommandLineOption("gflags_test_string", "def").empty()) {
        DB_WARNING("gflags_test_string:%s", FLAGS_gflags_test_string.c_str());
    }
    if (!google::SetCommandLineOption("gflags_test_bool", "false").empty()) {
        DB_WARNING("gflags_test_bool:%d", FLAGS_gflags_test_bool);
    }
    if (!google::SetCommandLineOption("gflags_test_int32", "500").empty()) {
        DB_WARNING("gflags_test_int32: succ");
    } else {
        DB_WARNING("gflags_test_int32: failed");
    }
    
    if (!google::SetCommandLineOption("gflags_test_int64", "400.0").empty()) {
        DB_WARNING("gflags_test_int64: succ");
    } else {
        DB_WARNING("gflags_test_int64: failed");
    }
    if (!google::SetCommandLineOption("gflags_test_double", "300").empty()) {
        DB_WARNING("gflags_test_double: succ:%f", FLAGS_gflags_test_double);
    } else {
        DB_WARNING("gflags_test_double: failed");
    }
    if (!google::SetCommandLineOption("gflags_test_bool", "123").empty()) {
        DB_WARNING("gflags_test_bool: succ:%d", FLAGS_gflags_test_bool);
    } else {
        DB_WARNING("gflags_test_bool: failed");
    }
    update_param("gflags_test_double", "600");
    update_param("gflags_test_int32", "600");
    update_param("gflags_test_bool", "false");
}

TEST(bns_to_meta_bns_test, case_all) {
     static std::map<std::string, std::string> mapping = {
        {"31.opera-adp-baikalStore-000-nj.FENGCHAO.njjs",          "group.opera-ps-baikalMeta-000-bj.FENGCHAO.all"},
        {"28.opera-atomkv-baikalStore-000-bj.FENGCHAO.bjhw",       "group.opera-atomkv-baikalMeta-000-bj.FENGCHAO.all"},
        {"2.opera-bigtree-baikalStore-000-bj.FENGCHAO.bjyz",       "group.opera-atomkv-baikalMeta-000-bj.FENGCHAO.all"},
        {"5.opera-coffline-baikalStore-000-mix.FENGCHAO.dbl",      "group.opera-coffline-baikalMeta-000-bj.FENGCHAO.all"},
        {"83.opera-p1-baikalStore-000-bj.HOLMES.bjhw",             "group.opera-online-baikalMeta-000-bj.HOLMES.all"},
        {"45.opera-xinghe2-baikalStore-000-bj.DMP.bjhw",           "group.opera-online-baikalMeta-000-bj.DMP.all"},
        {"0.opera-adp-baikaldb-000-bj.FENGCHAO.bjyz",              "group.opera-ps-baikalMeta-000-bj.FENGCHAO.all"},
        {"0.opera-atomkv-baikaldb-000-bj.FENGCHAO.bjyz",           "group.opera-atomkv-baikalMeta-000-bj.FENGCHAO.all"},
        {"group.opera-atomkv-baikaldb-000-bj.FENGCHAO.all",        "group.opera-atomkv-baikalMeta-000-bj.FENGCHAO.all"},
        {"group.opera-detect-baikaldb-000-gz.FENGCHAO.all",        "group.opera-detect-baikalMeta-000-bj.FENGCHAO.all"},
        {"group.opera-e0-baikaldb-000-ct.FENGCHAO.all",            "group.opera-e0-baikalMeta-000-yz.FENGCHAO.all"},
        {"1.opera-aladdin-baikaldb-000-bj.FENGCHAO.dbl",           "group.opera-aladdin-baikalMeta-000-bj.FENGCHAO.all"},
        {"group.opera-aladdin-baikaldb-000-nj.FENGCHAO.all",       "group.opera-aladdin-baikalMeta-000-bj.FENGCHAO.all"},
        {"7.opera-aladdin-baikalStore-000-mix.FENGCHAO.gzhxy",     "group.opera-aladdin-baikalMeta-000-bj.FENGCHAO.all"},
        {"0.opera-hmkv-baikalStore-000-yq.FENGCHAO.yq012",         "group.opera-holmes-baikalMeta-000-yq.FENGCHAO.all"},
        {"55.opera-hm-baikalStore-000-bd.FENGCHAO.bddwd",          "group.opera-holmes-baikalMeta-000-yq.FENGCHAO.all"},
        {"group.opera-hm-baikalStore-000-bd.FENGCHAO.all.serv",    "group.opera-holmes-baikalMeta-000-yq.FENGCHAO.all"},
        {"1.opera-adp-baikalBinlog-000-bj.FENGCHAO.bjhw",          "group.opera-ps-baikalMeta-000-bj.FENGCHAO.all"},
        {"group.opera-adp-baikalBinlog-000-bj.FENGCHAO.all",       "group.opera-ps-baikalMeta-000-bj.FENGCHAO.all"},
        {"0.opera-sandbox-baikalStore-000-bd.FENGCHAO.bddwd",      "group.opera-pap-baikalMeta-000-bj.FENGCHAO.all"},
    };
    for (const auto& it : mapping) {
        std::string meta_bns = store_or_db_bns_to_meta_bns(it.first);//实例和小的服务群组的对应关系
        DB_NOTICE("bns to meta_bns: %s => %s", it.first.c_str(), meta_bns.c_str());
        ASSERT_EQ(meta_bns, it.second);
    }
}

//集群消息收集 brpc接口测试 
TEST(brpc_http_get_info_test_db, case_all) {
    static const std::string host_noah = "http://api.mt.noah.baidu.com:8557";
    static std::string query_item = "&items=matrix.cpu_used_percent,matrix.cpu_quota,matrix.cpu_used,matrix.disk_home_quota_mb,matrix.disk_home_used_mb&ttype=instance";
    static std::set<std::string> test_instance_names = {
         "45.opera-xinghe2-baikalStore-000-bj.DMP.bjhw",
         "1.opera-aladdin-baikaldb-000-bj.FENGCHAO.dbl",
         "85.opera-hm-baikalStore-000-yq.FENGCHAO.yq013"
    };
    for (auto& it : test_instance_names) {
        std::string url_instance_source_used = host_noah + "/monquery/getlastestitemdata?namespaces=" + it + query_item;
        std::string response = "";
        int res = brpc_with_http(host_noah, url_instance_source_used, response);
        DB_NOTICE("http get instance db res: %s", response.c_str());
    }
}

DEFINE_int32(bvar_test_total_time_s, 0, "");
DEFINE_int32(bvar_test_loop_time_ms, 100, "");
DEFINE_int32(bvar_test_interaval_time_s, 60, "");
TEST(bvar_window_test, bvar) {
    bvar::Adder<int> count;
    bvar::Window<bvar::Adder<int>> window_count(&count, FLAGS_bvar_test_interaval_time_s);
    TimeCost time;
    int i = 0;
    while (true) {
        if (time.get_time() > FLAGS_bvar_test_total_time_s * 1000 * 1000) {
            break;
        }

        i++;
        count << 1;
        DB_WARNING("window_count: %d, i : %d, time: %ld", window_count.get_value(), i, time.get_time());
        bthread_usleep(FLAGS_bvar_test_loop_time_ms * 1000);
    }
}
DEFINE_int64(ttl_time_us, 60, "");
TEST(ttl_test, ttl) {
    int64_t now_time = butil::gettimeofday_us();
    uint64_t ttl_storage = ttl_encode(now_time);
    char* data = reinterpret_cast<char*>(&ttl_storage);
    DB_WARNING("now_time: %ld, ttl_storage: %lu, 0x%8x%8x%8x%8x%8x%8x%8x%8x,", now_time, ttl_storage, 
        data[0] & 0xFF, data[1]& 0xFF, data[2]& 0xFF, data[3]& 0xFF, data[4]& 0xFF, data[5]& 0xFF, data[6]& 0xFF, data[7]& 0xFF);

    now_time = FLAGS_ttl_time_us;
    ttl_storage = ttl_encode(now_time);
    data = reinterpret_cast<char*>(&ttl_storage);

    rocksdb::Slice key_slice;
    key_slice.data_ = reinterpret_cast<const char*>(&ttl_storage);
    key_slice.size_ = sizeof(uint64_t);
    TupleRecord tuple_record(key_slice);
    tuple_record.verification_fields(0x7FFFFFFF);

    
    DB_WARNING("now_time: %ld, ttl_storage: %lu 0x%8x%8x%8x%8x%8x%8x%8x%8x", now_time, ttl_storage,
        data[0] & 0xFF, data[1]& 0xFF, data[2]& 0xFF, data[3]& 0xFF, data[4]& 0xFF, data[5]& 0xFF, data[6]& 0xFF, data[7]& 0xFF);
    data = reinterpret_cast<char*>(&now_time);
    DB_WARNING("now_time: %ld, 0x%8x%8x%8x%8x%8x%8x%8x%8x", now_time, 
        data[0] & 0xFF, data[1]& 0xFF, data[2]& 0xFF, data[3]& 0xFF, data[4]& 0xFF, data[5]& 0xFF, data[6]& 0xFF, data[7]& 0xFF);
    uint64_t encode = KeyEncoder::to_endian_u64(0xFFFFFFFF00000000);
    data = reinterpret_cast<char*>(&encode);
    DB_WARNING("encode: %lu, 0x%8x%8x%8x%8x%8x%8x%8x%8x", encode, 
        data[0] & 0xFF, data[1]& 0xFF, data[2]& 0xFF, data[3]& 0xFF, data[4]& 0xFF, data[5]& 0xFF, data[6]& 0xFF, data[7]& 0xFF);
}


}  // namespace baikal
