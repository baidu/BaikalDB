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
        pred._regex = pred._regex_pattern;
        std::cout << boost::regex_match("www.bad/aca?bd_vid", pred._regex) << "\n";
        std::cout << boost::regex_match("www.bad/aca?bd_vidxxx", pred._regex) << "\n";
        std::cout << boost::regex_match("www.bad/aca?bdvid", pred._regex) << "\n";
        //regex = pred._regex_pattern;
        std::cout << pred._regex_pattern << "\n";
    } catch (boost::regex_error& e) {
        DB_FATAL("regex error:%d,  _regex_pattern:%s", 
                e.code(), pred._regex_pattern.c_str());
    }
}

}  // namespace baikal
