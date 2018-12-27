#include <gtest/gtest.h>
#include <climits>
#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include "expr_value.h"

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace baikaldb {

TEST(test_compare, case_all) {
    {
        ExprValue v1(pb::INT64);
        v1._u.int64_val = 65571188177;
        ExprValue v2(pb::INT64);
        v2._u.int64_val = 72856896263; 
        EXPECT_LT(v1.compare(v2), 0);
    }
    {
        ExprValue v1(pb::UINT64);
        v1._u.uint64_val = 65571188177;
        ExprValue v2(pb::UINT64);
        v2._u.uint64_val = 72856896263; 
        EXPECT_LT(v1.compare(v2), 0);
    }
    {
        ExprValue v1(pb::UINT64);
        v1._u.uint64_val = 1;
        ExprValue v2(pb::UINT64);
        v2._u.uint64_val = -1; 
        EXPECT_LT(v1.compare(v2), 0);
    }
    {
        ExprValue v1(pb::INT32);
        v1._u.int32_val = 2147483610;
        ExprValue v2(pb::INT64);
        v2._u.int32_val = -2147483610; 
        EXPECT_GT(v1.compare(v2), 0);
    }
    {
        ExprValue v1(pb::UINT32);
        v1._u.uint32_val = -1;
        ExprValue v2(pb::UINT32);
        v2._u.uint32_val = 1; 
        EXPECT_GT(v1.compare(v2), 0);
    }
    {
        ExprValue v1(pb::UINT64);
        v1._u.uint64_val = 9223372036854775800ULL;
        v1.cast_to(pb::DATETIME);
        ExprValue v2(pb::UINT64);
        v2._u.uint64_val = 9223372036854775810ULL;
        v2.cast_to(pb::DATETIME);
        std::cout << v1.compare(v2) << std::endl;
        EXPECT_LT(v1.compare(v2), 0);
    }
    {
        ExprValue v1(pb::STRING);
        v1.str_val = "2028-01-01 10:11:11";
        v1.cast_to(pb::DATETIME);
        ExprValue v2(pb::STRING);
        v2.str_val = "2011-03-27 20:57:19";
        v2.cast_to(pb::DATETIME);
        std::cout << v1.compare(v2) << std::endl;
        EXPECT_GT(v1.compare(v2), 0);
    }
    {
        ExprValue v1(pb::STRING);
        v1.str_val = "2037-10-11 01:52:41";
        v1.cast_to(pb::DATETIME);
        ExprValue v2(pb::STRING);
        v2.str_val = "2037-04-25 10:40:13";
        v2.cast_to(pb::DATETIME);
        std::cout << v1.compare(v2) << std::endl;
        EXPECT_GT(v1.compare(v2), 0);
    }
    ExprValue dt(pb::STRING);
    dt.str_val = "2018-1-1 10:11:11";
    dt.cast_to(pb::DATE);
    std::cout << dt._u.uint32_val << " " << dt.get_string() << "\n";
    ExprValue dt2(pb::STRING);
    dt2.str_val = "2018-03-27 20:57:19";
    dt2.cast_to(pb::TIMESTAMP);
    std::cout << dt2._u.uint32_val << " " << dt2.get_string() << " " << dt2.hash() << "\n";
    std::cout << &dt2._u << " " << &dt2._u.int8_val << " " << &dt2._u.int32_val << " " <<
        &dt2._u.uint64_val << "\n";
}

}  // namespace baikal
