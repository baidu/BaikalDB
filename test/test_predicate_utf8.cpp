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
    EXPECT_EQ(true, *pred.like<LikePredicate::UTF8Charset>("axxx", "a%x%x"));
    EXPECT_EQ(true, *pred.like<LikePredicate::UTF8Charset>("中文testbd_vid中文test", "中文testbd_vid中文test"));
    EXPECT_EQ(true, *pred.like<LikePredicate::UTF8Charset>("中文testbd_vid中文test", "%testbd_vid中文tes%"));
    EXPECT_EQ(false, *pred.like<LikePredicate::UTF8Charset>("患了", "%剂%"));
    EXPECT_EQ(false, *pred.like<LikePredicate::Binary>("患了", "%剂%"));
}

}  // namespace baikal
