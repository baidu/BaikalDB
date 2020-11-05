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

#include "table_key.h"
#include "mut_table_key.h"
#include <gtest/gtest.h>
#include <climits>
#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <algorithm>

namespace baikaldb {

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    srand((unsigned)time(NULL));
    return RUN_ALL_TESTS();
}

TEST(test_append_i8, case_all) {
    MutTableKey mut_key;
    for (int16_t idx = SCHAR_MIN; idx <= SCHAR_MAX; idx++) {
        mut_key.append_i8((int8_t)idx);
    }
    EXPECT_EQ(256 * sizeof(int8_t), mut_key.size());
    TableKey key(mut_key);
    int pos = 0;
    for (int16_t idx = SCHAR_MIN; idx <= SCHAR_MAX; idx++) {
        EXPECT_EQ((int8_t)idx, key.extract_i8(pos));
        pos += sizeof(int8_t);
    }
}

TEST(test_append_i16, case_all) {
    MutTableKey mut_key;
    for (int32_t idx = SHRT_MIN; idx <= SHRT_MAX; idx++) {
        mut_key.append_i16((int16_t)idx);
    }
    EXPECT_EQ((SHRT_MAX - SHRT_MIN + 1) * sizeof(int16_t), mut_key.size());
    TableKey key(mut_key);
    int pos = 0;
    for (int32_t idx = SHRT_MIN; idx <= SHRT_MAX; idx++) {
        EXPECT_EQ((int16_t)idx, key.extract_i16(pos));
        pos += sizeof(int16_t);
    }
}

TEST(test_append_i32, case_all) {
    MutTableKey mut_key;
    int count = 0;
    for (int64_t idx = INT_MIN; idx <= INT_MAX; idx += (INT_MAX/100000)) {
        mut_key.append_i32((int32_t)idx);
        count++;
    }
    EXPECT_EQ(count * sizeof(int32_t), mut_key.size());
    TableKey key(mut_key);
    int pos = 0;
    for (int64_t idx = INT_MIN; idx <= INT_MAX; idx += (INT_MAX/100000)) {
        EXPECT_EQ((int32_t)idx, key.extract_i32(pos));
        pos += sizeof(int32_t);
    }
}

TEST(test_append_i64, case_all) {
    MutTableKey mut_key;
    int count = 0;
    uint32_t gap = (9223372036854775807UL/2036854775807UL);
    for (int64_t idx = -9223372036854775808UL; idx < 9223372036854775807UL - gap; idx += gap) {
        mut_key.append_i64(idx);
        count++;
    }
    EXPECT_EQ(count * sizeof(int64_t), mut_key.size());
    TableKey key(mut_key);
    int pos = 0;
    for (int64_t idx = -9223372036854775808UL; idx < 9223372036854775807UL - gap; idx += gap) {
        EXPECT_EQ(idx, key.extract_i64(pos));
        pos += sizeof(int64_t);
    }
}

TEST(test_append_float, case_all) {
    MutTableKey mut_key;
    int count = 0;
    srand((unsigned)time(NULL));
    std::vector<float> float_vals;
    for (uint32_t idx = 0; idx < 10000; ++idx) {
        float val = (rand() - RAND_MAX/2 + 0.0f)/RAND_MAX;
        mut_key.append_float(val);
        float_vals.push_back(val);
        count++;
    }
    EXPECT_EQ(count * sizeof(float), mut_key.size());
    TableKey key(mut_key);
    int pos = 0;
    for (uint32_t idx = 0; idx < 10000; ++idx) {
        float diff = key.extract_float(pos) - float_vals[idx];
        EXPECT_EQ(true, diff < 1e-6);
        pos += sizeof(float);
    }
}

TEST(test_append_double, case_all) {
    MutTableKey mut_key;
    int count = 0;
    srand((unsigned)time(NULL));
    std::vector<double> double_vals;
    for (uint32_t idx = 0; idx < 100000; ++idx) {
        double val = (rand() - RAND_MAX/2 + 0.0f)/RAND_MAX;
        mut_key.append_double(val);
        double_vals.push_back(val);
        count++;
    }
    EXPECT_EQ(count * sizeof(double), mut_key.size());
    TableKey key(mut_key);
    int pos = 0;
    for (uint32_t idx = 0; idx < 100000; ++idx) {
        double diff = key.extract_double(pos) - double_vals[idx];
        EXPECT_EQ(true, diff < 1e-9);
        pos += sizeof(double);
    }
}

TEST(test_sort_float, case_all) {
    srand((unsigned)time(NULL));
    std::vector<float> float_vals;
    std::vector<std::string> str_vals;

    auto append_data = [&float_vals, &str_vals](float val) {
        MutTableKey mut_key;
        mut_key.append_float(val);
        float_vals.push_back(val);
        str_vals.push_back(mut_key.data());
    };
    append_data(0);
    append_data(-0);
    append_data(0.0);
    append_data(-1e-26);
    append_data(1e-26);
    append_data(-1e26);
    append_data(1e-26);
    append_data(-3.4E+38);
    append_data(3.4E+38);

    for (uint32_t idx = 0; idx < 100000; ++idx) {
        float val = (rand() - RAND_MAX/2 + 0.0f)/RAND_MAX;
        MutTableKey mut_key;
        mut_key.append_float(val);
        float_vals.push_back(val);
        str_vals.push_back(mut_key.data());
    }
    std::sort(float_vals.begin(), float_vals.end());
    std::sort(str_vals.begin(), str_vals.end());
    for (uint32_t idx = 0; idx < float_vals.size(); ++idx) {
        TableKey key(str_vals[idx]);
        //float diff = key.extract_float(0) - float_vals[idx];
        //printf("size:%d %lf %lf\n", str_vals[idx].size(), key.extract_float(0), float_vals[idx]);
        EXPECT_EQ(key.extract_float(0), float_vals[idx]);
    }
}

TEST(test_sort_double, case_all) {
    srand((unsigned)time(NULL));
    std::vector<double> double_vals;
    std::vector<std::string> str_vals;

    auto append_data = [&double_vals, &str_vals](double val) {
        MutTableKey mut_key;
        mut_key.append_double(val);
        double_vals.push_back(val);
        str_vals.push_back(mut_key.data());
    };
    append_data(0);
    append_data(-0);
    append_data(0.0);
    append_data(-1e-26);
    append_data(1e-26);
    append_data(-1e26);
    append_data(1e-26);
    append_data(-3.4E+38);
    append_data(3.4E+38);
    append_data(-1.7E+308);
    append_data(1.7E+308);

    for (uint32_t idx = 0; idx < 100000; ++idx) {
        double val = (rand() - RAND_MAX/2 + 0.0f)/RAND_MAX;
        MutTableKey mut_key;
        mut_key.append_double(val);
        double_vals.push_back(val);
        str_vals.push_back(mut_key.data());
    }
    std::sort(double_vals.begin(), double_vals.end());
    std::sort(str_vals.begin(), str_vals.end());
    for (uint32_t idx = 0; idx < double_vals.size(); ++idx) {
        TableKey key(str_vals[idx]);
        //double diff = key.extract_double(0) - double_vals[idx];
        //printf("size:%d %lf %lf\n", str_vals[idx].size(), key.extract_double(0), double_vals[idx]);
        EXPECT_EQ(key.extract_double(0), double_vals[idx]);
    }
}

}  // namespace baikal
