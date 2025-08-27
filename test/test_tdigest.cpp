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
#include "tdigest.h"
#include "expr_value.h"
#include "internal_functions.h"

int cnt = 0;

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    //cnt = std::stoi(argv[1]);
    return RUN_ALL_TESTS();
}

namespace baikaldb {
namespace tdigest {

TEST(test_tdigest, case_all) {
    size_t mem_size = tdigest::td_required_buf_size(COMPRESSION);
    std::string td_data;
    td_data.resize(mem_size);
    uint8_t* tdigest_histogram = (uint8_t*)td_data.data();
    tdigest::td_histogram_t *t = tdigest::td_init(COMPRESSION, tdigest_histogram, mem_size);
    for (uint32_t i = 0; i < 1000000; i++) {
        for (int j = 0; j < 4; j++) {
            tdigest::td_add(t, 100.1 + j, 2);
            tdigest::td_add(t, 110.1 + j, 2);
            tdigest::td_add(t, 130.1 + j, 2);
        }
    }
    std::cout << "t_digest size: " <<  td_data.size() << std::endl;
    std::cout << "percentile 95%: " << tdigest::td_quantile_of(t, 120.0) << std::endl;
    std::cout << "percentile 95 val: " << tdigest::td_value_at(t, 0.95) << std::endl;
}

TEST(test_tdigest_expr_value, case_all) {
    ExprValue td_val(pb::TDIGEST);
    std::cout << " cast_to t_digest size: " <<  td_val.str_val.size() << " compressed size: " << td_val.get_string().size() << std::endl;
    tdigest::td_histogram_t* t = (tdigest::td_histogram_t*)td_val.str_val.data();
    for (uint32_t i = 0; i < 1000000; i++) {
        for (uint32_t j = 0; j < 4; j++) {
            tdigest::td_add(t, 100.1 + j, 2);
            tdigest::td_add(t, 110.1 + j, 2);
            tdigest::td_add(t, 130.1 + j, 2);
        }
    }
    std::cout << "t_digest size: " <<  td_val.str_val.size() << " compressed size: " << td_val.get_string().size() << std::endl;
    std::cout << "percentile 95%: " << tdigest::td_quantile_of(t, 120.0) << std::endl;
    std::cout << "percentile 95 val: " << tdigest::td_value_at(t, 0.95) << std::endl;
}

TEST(test_tdigest_internal_functions, case_all) {
    size_t mem_size = tdigest::td_required_buf_size(COMPRESSION);
    std::vector<ExprValue> vals1;
    for (int i = 1; i < 1000; i++) {
        ExprValue tmp(pb::UINT32);
        tmp._u.uint32_val = i;
        vals1.emplace_back(tmp);
    }
    ExprValue td1 = tdigest_build(vals1);
    ASSERT_EQ(td1.type, pb::TDIGEST);
    ASSERT_EQ(td1.str_val.size(), tdigest::td_actual_size((tdigest::td_histogram_t*)td1.str_val.data()));

    std::vector<ExprValue> vals2;
    vals2.emplace_back(td1);
    for (int i = 1000; i < 2000; i++) {
        ExprValue tmp(pb::UINT32);
        tmp._u.uint32_val = i;
        vals2.emplace_back(tmp);
    }
    ExprValue td2 = tdigest_add(vals2);
    ASSERT_EQ(td2.type, pb::TDIGEST);
    ASSERT_EQ(td2.str_val.size(), tdigest::td_actual_size((tdigest::td_histogram_t*)td2.str_val.data()));

    std::vector<ExprValue> vals3;
    vals3.emplace_back(td2);
    ExprValue tmp(pb::DOUBLE);
    tmp._u.double_val = 0.95;
    vals3.emplace_back(tmp);
    ExprValue ret1 = tdigest_percentile(vals3);
    tdigest::td_histogram_t* t = (tdigest::td_histogram_t*)td2.str_val.data();
    ASSERT_EQ(tdigest::td_value_at(t, 0.95), ret1._u.double_val);

    vals3[1]._u.double_val = 1050;
    ExprValue ret2 = tdigest_location(vals3);
    ASSERT_EQ(tdigest::td_quantile_of(t, 1050.0), ret2._u.double_val);
}
}

}  // namespace baikal
