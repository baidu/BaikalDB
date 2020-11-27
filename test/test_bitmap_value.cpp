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
#include <vector>
#include "common.h"
#include "internal_functions.h"
#include <roaring.hh>

int cnt = 0;

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    //cnt = std::stoi(argv[1]);
    return RUN_ALL_TESTS();
}

namespace baikaldb {

TEST(test_bitmap, case_all) {
    std::vector<ExprValue> vals;
    for (int i = 1; i < 1000; i *= 10) {
        ExprValue tmp(pb::UINT32);
        tmp._u.uint32_val = i;
        vals.emplace_back(tmp);
    }
    ExprValue bit1 = rb_build(vals);
    std::cout << "bitmap " << bit1._u.bitmap->toString().c_str() << std::endl;

    std::vector<ExprValue> vals_2;
    ExprValue bits_tmp(pb::BITMAP);
    vals_2.emplace_back(bits_tmp);
    for (int i = 1; i < 1000; i *= 10) {
        ExprValue tmp(pb::UINT32);
        tmp._u.uint32_val = i;
        vals_2.emplace_back(tmp);
        tmp._u.uint32_val = i + 1;
        vals_2.emplace_back(tmp);
    }
    ExprValue bit2 = rb_add(vals_2);
    std::cout << "bitmap " << bit2._u.bitmap->toString().c_str() << std::endl;

    std::vector<ExprValue> vals_3;
    vals_3.emplace_back(bit2);
    vals_3.emplace_back(bit1);
    ExprValue and_bits = rb_and(vals_3);
    std::cout << "bitmap " << and_bits._u.bitmap->toString().c_str() << std::endl;
    ExprValue or_bits = rb_or(vals_3);
    std::cout << "bitmap " << or_bits._u.bitmap->toString().c_str() << std::endl;
    ExprValue xor_bits = rb_xor(vals_3);

    std::cout << "bitmap " << xor_bits._u.bitmap->toString().c_str() << std::endl;
    ExprValue andnot_bits = rb_andnot(vals_3);
    //ASSERT_EQ(3, andnot_bits_card._u.uint64_val);
    std::cout << "bitmap " << andnot_bits._u.bitmap->toString().c_str() << std::endl;

    ExprValue tmp(pb::UINT32);
    tmp._u.uint32_val = 100;
    std::vector<ExprValue> vals_4;
    vals_4.emplace_back(bit2);
    vals_4.emplace_back(tmp);
    ExprValue contains = rb_contains(vals_4);
    ASSERT_EQ(true, contains._u.bool_val);

    ExprValue intersect = rb_intersect(vals_3);
    ASSERT_EQ(true, intersect._u.bool_val);

    ExprValue removed = rb_remove(vals_4);
    std::vector<ExprValue> vals_5;
    vals_5.emplace_back(removed);
    vals_5.emplace_back(tmp);
    contains = rb_contains(vals_5);
    ASSERT_EQ(false, contains._u.bool_val);
    
    std::vector<ExprValue> vals_6;
    vals_6.emplace_back(bit2);
    vals_6.emplace_back(bit2);
    ExprValue requal = rb_equals(vals_6);
    ASSERT_EQ(true, requal._u.bool_val);

    ExprValue a(pb::STRING);
    a.cast_to(pb::BITMAP);
    ExprValue b(pb::BITMAP);
    std::vector<ExprValue> vals_7;
    vals_7.emplace_back(a);
    vals_7.emplace_back(b);
    requal = rb_equals(vals_7);
    ASSERT_EQ(true, requal._u.bool_val);
}

}  // namespace baikal
