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
    pred.covent_pattern("%?bd\\_vid%");
    boost::regex regex;
    try {
        re2::RE2::Options option;
        option.set_utf8(false);
        pred._regex_ptr.reset(new re2::RE2(pred._regex_pattern, option));
        regex = pred._regex_pattern;
        EXPECT_EQ(
            RE2::FullMatch("www.bad/aca?bd_vid", *pred._regex_ptr),
            boost::regex_match("www.bad/aca?bd_vid", regex)
        );
        EXPECT_EQ(
            RE2::FullMatch("www.bad/aca?bd_vidxxx", *pred._regex_ptr),
            boost::regex_match("www.bad/aca?bd_vidxxx", regex)
        );
        EXPECT_EQ(
            RE2::FullMatch("www.bad/aca?bdvid", *pred._regex_ptr),
            boost::regex_match("www.bad/aca?bdvid", regex)
        );
        
        EXPECT_EQ(
            RE2::FullMatch("中文testbd_vid中文test", *pred._regex_ptr),
            boost::regex_match("中文testbd_vid中文test", regex)
        );
        std::cout << pred._regex_pattern << "\n";
    } catch (std::exception& e) {
        DB_FATAL("regex error:%s,  _regex_pattern:%s", 
                e.what(), pred._regex_pattern.c_str());
    }
}

}  // namespace baikal
