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
