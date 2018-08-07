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
#ifdef BAIDU_INTERNAL
#include <pb_to_json.h>
#else
#include <json2pb/pb_to_json.h>
#endif
#include <proto/meta.interface.pb.h>
#include "rapidjson.h"
#include <raft/raft.h>
#include "common.h"
#include "schema_factory.h"

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace baikaldb {

TEST(test_stripslashes, case_all) {
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
    std::cout << sizeof(raft::NodeOptions) << " " << sizeof(std::string) 
    << sizeof(raft::PeerId) << sizeof(base::EndPoint) << "\n"; 
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

    BthreadCond concurrency_cond(-5);
    for (int i = 0; i < 10; i++) {
        Bthread bth;
        // increase一定要在主线程里
        concurrency_cond.increase();
        concurrency_cond.wait();
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

TEST(timestamp_to_str, str_to_timestamp) {
    std::string str = "1991-07-13 14:15:35";
    time_t time1 = str_to_timestamp(str.c_str());
    std::cout << str << " " << timestamp_to_str(time1) << std::endl;
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

}  // namespace baikal
