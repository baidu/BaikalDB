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
#ifdef BAIDU_INTERNAL
#include <pb_to_json.h>
#else
#include <json2pb/pb_to_json.h>
#endif
#include <proto/meta.interface.pb.h>
#include "rapidjson.h"
#include <raft/raft.h>
#include "common.h"
#include "hll_common.h"
#include "schema_factory.h"

int cnt = 0;

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    //cnt = std::stoi(argv[1]);
    return RUN_ALL_TESTS();
}

namespace baikaldb {
namespace hll {

TEST(test_hll, case_all) {
    ExprValue hll = hll_init();
    int cnts[] = {50, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000, 2000000, 3000000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000, 10000000, 50000000};
    double max = 0.0;
    int last = 0;
    for (int cnt = 50; cnt < 1000000; cnt++) {
        for (int i = last; i < cnt; ++i) {
            ExprValue tmp(pb::INT64);
            tmp._u.int64_val = i;
            hll_add(hll, tmp.hash());
        }
        last = cnt;
        uint64_t tmp = hll_estimate(hll);
        std::cout << " cnt: " << cnt << " hll: " << tmp << " : " << tmp * 1.0 / cnt << std::endl;
        if (fabs(1-tmp * 1.0 / cnt) > max) {
            max = fabs(1-tmp * 1.0 / cnt);
        }
        if (cnt > 10000) {
            cnt += 10;
        }
    }
    std::cout << "max:" << max << std::endl;
}

TEST(test_hll_performace, case_all) {
    std::vector<ExprValue> vec;
    for (int i = 0; i < 5; i++) {
        ExprValue hll = hll_init();
        for (int cnt = 0; cnt < 1000; cnt++) {
            ExprValue tmp(pb::INT64);
            tmp._u.int64_val = butil::fast_rand();
            hll_add(hll, tmp.hash());
        }
        vec.push_back(hll);
    }
    {
        ExprValue merge_hll = hll_init();
        std::cout << "old:" << std::endl;
        TimeCost cost;
        for (int i = 0; i < vec.size(); i++) {
            hll_merge(merge_hll, vec[i]);
        }
        std::cout << hll_estimate(merge_hll) << "cost:" << cost.get_time() << std::endl;
    }
    {
        ExprValue merge_hll = hll_init();
        std::cout << "new:" << std::endl;
        TimeCost cost;
        for (int i = 0; i < vec.size(); i++) {
            hll_merge_agg(merge_hll, vec[i]);
        }
        std::cout << hll_estimate(merge_hll) << "cost:" << cost.get_time() << std::endl;
    }
}

}
}  // namespace baikal
