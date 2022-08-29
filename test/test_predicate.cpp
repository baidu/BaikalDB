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
#include <boost/regex.hpp>
#include "re2/re2.h"
#include "predicate.h"

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace baikaldb {

TEST(test_covent_pattern, case_all) {
    LikePredicate pred;
    std::string a("www.bad/aca?bd_vid");
    std::string b("www.bad/aca?bd_vid");
    EXPECT_EQ(true, *pred.like<LikePredicate::Binary>(a, b));
    EXPECT_EQ(true, *pred.like<LikePredicate::Binary>("abc", "a_c"));
    EXPECT_EQ(true, *pred.like<LikePredicate::Binary>("abc", "%"));
    EXPECT_EQ(true, *pred.like<LikePredicate::Binary>("axxx", "a%x%x"));
    EXPECT_EQ(true, *pred.like<LikePredicate::GBKCharset>("axxx", "a%x%x"));
    EXPECT_EQ(true, *pred.like<LikePredicate::GBKCharset>("中文testbd_vid中文test", "中文testbd_vid中文test"));
    EXPECT_EQ(true, *pred.like<LikePredicate::GBKCharset>("中文testbd_vid中文test", "%testbd_vid中文tes%"));
    //剂 GBK 码点： BCC1
    //患了 GBK码点：BBBC C1CB
    EXPECT_EQ(false, *pred.like<LikePredicate::GBKCharset>("患了", "%剂%"));
    EXPECT_EQ(true,  *pred.like<LikePredicate::Binary>("患了", "%剂%"));
    EXPECT_EQ(true,  *pred.like<LikePredicate::GBKCharset>("中%文", "中\\%文"));
    EXPECT_EQ(false, *pred.like<LikePredicate::GBKCharset>("中测试文", "中\\%文"));
    EXPECT_EQ(false, *pred.like<LikePredicate::GBKCharset>("中f文", "中\\_文"));
    EXPECT_EQ(true,  *pred.like<LikePredicate::GBKCharset>("中f文", "中_文"));
    EXPECT_EQ(false, *pred.like<LikePredicate::GBKCharset>("中%文", "中测试文"));
    EXPECT_EQ(true, *pred.like<LikePredicate::GBKCharset>("中aaa文", "中%文"));
    EXPECT_EQ(true, *pred.like<LikePredicate::GBKCharset>("", ""));
    EXPECT_EQ(true, *pred.like<LikePredicate::GBKCharset>("test", "te%st"));
    EXPECT_EQ(true, *pred.like<LikePredicate::Binary>("test", "te%st"));
    EXPECT_EQ(true, *pred.like<LikePredicate::GBKCharset>("test", "te%%st"));
    EXPECT_EQ(true, *pred.like<LikePredicate::Binary>("test", "te%%st"));
    EXPECT_EQ(true, *pred.like<LikePredicate::GBKCharset>("test", "%test%"));
    EXPECT_EQ(true, *pred.like<LikePredicate::Binary>("test", "%test%"));
    EXPECT_EQ(true, *pred.like<LikePredicate::GBKCharset>("test", "_%_%_%_"));
    EXPECT_EQ(true, *pred.like<LikePredicate::GBKCharset>("test", "_%_%st"));
    EXPECT_EQ(true, *pred.like<LikePredicate::GBKCharset>("3hello", "3%hello"));
    EXPECT_EQ(true, *pred.like<LikePredicate::Binary>("3hello", "3%hello"));
    EXPECT_EQ(false, *pred.like<LikePredicate::GBKCharset>("aaaaaaaaaaaaaaaaaaaaaaaaaaa", "a%a%a%a%a%a%a%a%b"));
    EXPECT_EQ(false, *pred.like<LikePredicate::Binary>("aaaaaaaaaaaaaaaaaaaaaaaaaaa", "a%a%a%a%a%a%a%a%b"));
}

}  // namespace baikal
