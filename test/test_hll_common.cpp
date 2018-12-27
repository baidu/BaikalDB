#include <gtest/gtest.h>
#include <climits>
#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <ctime>
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
            hll_add(hll, tmp.hash(0x1111110));
        }
        last = cnt;
        int64_t tmp = hll_estimate(hll);
        //std::cout << " cnt: " << cnt << " hll: " << tmp << " : " << tmp * 1.0 / cnt << std::endl;
        if (fabs(1-tmp * 1.0 / cnt) > max) {
            max = fabs(1-tmp * 1.0 / cnt);
        }
        if (cnt > 10000) {
            cnt += 10;
        }
    }
    std::cout << "max:" << max << std::endl;
    //stripslashes(str);
    //std::cout << "new:" << str << std::endl;
    //EXPECT_STREQ(str.c_str(), "\\%a\t");

    //std::string str2 = "abc";
    //std::cout << "orgin:" << str2 << std::endl;
    //stripslashes(str2);
    //std::cout << "new:" << str2 << std::endl;
    //EXPECT_STREQ(str2.c_str(), "abc");
}

}
}  // namespace baikal
