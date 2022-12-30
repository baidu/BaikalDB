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
#include <fstream>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include "expr_value.h"
#include "fn_manager.h"
#include "proto/expr.pb.h"
#include "parser.h"
#include "proto/meta.interface.pb.h"
#include "joiner.h"

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace baikaldb {
TEST(test_proto, case_all) {
    /*
    std::ofstream fp;
    fp.open("sign", std::ofstream::out);
    std::ifstream ifp("holmes");

    std::vector<std::string> vec;
    vec.reserve(10000000);
    while (ifp.good()) {
        std::string line;
        std::getline(ifp, line);
        vec.push_back(line);
    }
    for (uint64_t i = 0; i < 1000000000; i++) {
        fp << butil::fast_rand() << "\t" << vec[i%vec.size()] << "\n";
    }
    return;
    */
    {
        double aa = 0.000000001;
        std::ostringstream oss;
        oss << std::setprecision(15) << aa;
        std::cout << oss.str() << std::endl;
        double b = 100.123;
        char x[100];
        snprintf(x, 100, "%.12g", b);
        std::cout << "test:" << x << std::endl;
        snprintf(x, 100, "%.12g", aa);
        std::cout << "test:" << x << std::endl;
    }
    {
        double aa = 0.01;
        std::ostringstream oss;
        oss << aa;
        std::cout << oss.str() << std::endl;
    }
    {
        double aa = 0.001;
        std::ostringstream oss;
        oss << aa;
        std::cout << oss.str() << std::endl;
    }
    {
        double aa = 0.0001;
        std::ostringstream oss;
        oss << aa;
        std::cout << oss.str() << std::endl;
    }
    {
        double aa = 0.00001;
        std::ostringstream oss;
        oss << aa;
        std::cout << oss.str() << std::endl;
    {
        double aa = 0.000001;
        std::ostringstream oss;
        oss << aa;
        std::cout << oss.str() << std::endl;
    }
    }
    class LogMessageVoidify {
        public: 
            LogMessageVoidify() { }
            // This has to be an operator with a precedence lower than << but
            // higher than ?:
            void operator&(std::ostream&) { }
    };
    int n = 0;
    !0 ? void(0) : LogMessageVoidify() & std::cout << ++n;
    std::cout << "e" << n << "\n";
    {
        ExprValue v1(pb::INT64);
        v1._u.int64_val = 123372036854775800LL;
        std::cout << "debug1:" << v1._u.int64_val << std::endl;
        pb::ExprValue pb_v1;
        v1.to_proto(&pb_v1);
        ExprValue v2(pb_v1);
        EXPECT_EQ(v1.compare(v2), 0);
        double diff = v1.float_value(0) - v2.float_value(0);
        EXPECT_EQ(true, diff < 1e-6);
    }
    {
        ExprValue v1(pb::UINT64);
        v1._u.uint64_val = 65571188177;
        pb::ExprValue pb_v1;
        v1.to_proto(&pb_v1);
        ExprValue v2(pb_v1);
        EXPECT_EQ(v1.compare(v2), 0);
        double diff = v1.float_value(0) - v2.float_value(0);
        EXPECT_EQ(true, diff < 1e-6);
    }
    {
        ExprValue v1(pb::INT32);
        v1._u.uint64_val = 2147483610;
        pb::ExprValue pb_v1;
        v1.to_proto(&pb_v1);
        ExprValue v2(pb_v1);
        EXPECT_EQ(v1.compare(v2), 0);
        double diff = v1.float_value(0) - v2.float_value(0);
        EXPECT_EQ(true, diff < 1e-6);
    }
    {
        ExprValue v1(pb::UINT32);
        v1._u.uint64_val = 123456;
        pb::ExprValue pb_v1;
        v1.to_proto(&pb_v1);
        ExprValue v2(pb_v1);
        EXPECT_EQ(v1.compare(v2), 0);
        double diff = v1.float_value(0) - v2.float_value(0);
        EXPECT_EQ(true, diff < 1e-6);
    }
    {
        ExprValue v1(pb::INT8);
        v1._u.uint64_val = -1;
        pb::ExprValue pb_v1;
        v1.to_proto(&pb_v1);
        ExprValue v2(pb_v1);
        EXPECT_EQ(v1.compare(v2), 0);
        double diff = v1.float_value(0) - v2.float_value(0);
        EXPECT_EQ(true, diff < 1e-6);
    }
    {
        ExprValue v1(pb::UINT8);
        v1._u.uint64_val = 127;
        pb::ExprValue pb_v1;
        v1.to_proto(&pb_v1);
        ExprValue v2(pb_v1);
        EXPECT_EQ(v1.compare(v2), 0);
        double diff = v1.float_value(0) - v2.float_value(0);
        EXPECT_EQ(true, diff < 1e-6);
    }
    {
        ExprValue v1(pb::INT16);
        v1._u.uint64_val = -123;
        pb::ExprValue pb_v1;
        v1.to_proto(&pb_v1);
        ExprValue v2(pb_v1);
        EXPECT_EQ(v1.compare(v2), 0);
        double diff = v1.float_value(0) - v2.float_value(0);
        EXPECT_EQ(true, diff < 1e-6);
    }
    {
        ExprValue v1(pb::UINT16);
        v1._u.uint64_val = 127;
        pb::ExprValue pb_v1;
        v1.to_proto(&pb_v1);
        ExprValue v2(pb_v1);
        EXPECT_EQ(v1.compare(v2), 0);
        double diff = v1.float_value(0) - v2.float_value(0);
        EXPECT_EQ(true, diff < 1e-6);
    }
    {
        ExprValue v1(pb::UINT64);
        v1._u.uint64_val = 9223372036854775800ULL;
        v1.cast_to(pb::DATETIME);
        pb::ExprValue pb_v1;
        v1.to_proto(&pb_v1);
        ExprValue v2(pb_v1);
        EXPECT_EQ(v1.compare(v2), 0);
        double diff = v1.float_value(0) - v2.float_value(0);
        EXPECT_EQ(true, diff < 1e-6);
    }
    {
        ExprValue v1(pb::STRING);
        v1.str_val = "2028-01-01 10:11:11";
        v1.cast_to(pb::TIMESTAMP);
        pb::ExprValue pb_v1;
        v1.to_proto(&pb_v1);
        ExprValue v2(pb_v1);
        EXPECT_EQ(v1.compare(v2), 0);
        double diff = v1.float_value(0) - v2.float_value(0);
        EXPECT_EQ(true, diff < 1e-6);
    }
    {
        ExprValue v1(pb::FLOAT);
        v1._u.float_val = 1.05;
        pb::ExprValue pb_v1;
        v1.to_proto(&pb_v1);
        ExprValue v2(pb_v1);
        double diff = v1.float_value(0) - v2.float_value(0);
        EXPECT_EQ(true, diff < 1e-6);
    }
    {
        ExprValue v1(pb::DOUBLE);
        v1._u.float_val = 1.06;
        pb::ExprValue pb_v1;
        v1.to_proto(&pb_v1);
        ExprValue v2(pb_v1);
        double diff = v1.float_value(0) - v2.float_value(0);
        EXPECT_EQ(true, diff < 1e-6);
    }
    {
        ExprValue v1(pb::STRING);
        v1.str_val = "abcd";
        ExprValue v2(pb::STRING);
        v2.str_val = "abcf";
        EXPECT_EQ(v1.common_prefix_length(v2), 3);
        EXPECT_LT(v1.float_value(3), v2.float_value(3));
    }
}

TEST(test_compare, case_all) {
    class LogMessageVoidify {
        public: 
            LogMessageVoidify() { }
            // This has to be an operator with a precedence lower than << but
            // higher than ?:
            void operator&(std::ostream&) { }
    };
    int n = 0;
    !0 ? void(0) : LogMessageVoidify() & std::cout << ++n;
    std::cout << "e" << n << "\n";
    {
        ExprValue v1(pb::INT64);
        v1._u.int64_val = 123372036854775800LL;
        std::cout << "debug1:" << v1._u.int64_val << std::endl;
        v1.cast_to(pb::DOUBLE);
        std::cout << "debug2:" << v1._u.double_val << std::endl;
        v1.cast_to(pb::INT64);
        std::cout << "debug2:" << v1._u.int64_val << std::endl;
    }
    {
        ExprValue v1(pb::INT64);
        v1._u.int64_val = 1;
        v1.cast_to(pb::DOUBLE);
        std::cout << v1.get_string() << "\n";
        EXPECT_STREQ(v1.get_string().c_str(), "1");
    }
    {
        ExprValue v1(pb::INT64);
        v1._u.int64_val = 1;
        ExprValue v2(pb::INT32);
        v2._u.int32_val = 1; 
        EXPECT_EQ(v1.compare_diff_type(v2), 0);
    }
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
    {
        ExprValue v1(pb::HEX);
        v1.str_val = "\xff\xff";
        v1.cast_to(pb::INT64);
        EXPECT_EQ(v1.get_numberic<int64_t>(), 65535);
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
    {
        ExprValue tmp(pb::STRING);
        tmp.str_val = "ec8f147a-9c41-4093-a1f0-01d70f73e8fd";
        std::cout << tmp.str_val << ":" << tmp.hash() << std::endl;
    }
    {
        ExprValue tmp(pb::STRING);
        tmp.str_val = "1b164e54-ffb3-445a-9631-a3da77e5a7e8";
        std::cout << tmp.str_val << ":" << tmp.hash() << std::endl;
    }
    {
        ExprValue tmp(pb::STRING);
        tmp.str_val = "58f706d7-fc10-478f-ad1c-2a1772c35d46";
        std::cout << tmp.str_val << ":" << tmp.hash() << std::endl;
    }
    {
        ExprValue tmp(pb::STRING);
        tmp.str_val = "be69ea04-2065-488d-8817-d57fe2b77734";
        std::cout << tmp.str_val << ":" << tmp.hash() << std::endl;
    }
    {
        ExprValue tmp(pb::INT64);
        tmp._u.int64_val = 127;
        std::cout << tmp._u.int64_val << ":" << tmp.hash() << std::endl;
    }
    {
        ExprValue tmp(pb::INT64);
        tmp._u.int64_val = 128;
        std::cout << tmp._u.int64_val << ":" << tmp.hash() << std::endl;
    }
    {
        ExprValue tmp(pb::INT64);
        tmp._u.int64_val = 65535;
        std::cout << tmp._u.int64_val << ":" << tmp.hash() << std::endl;
    }
    {
        ExprValue tmp(pb::INT64);
        tmp._u.int64_val = 65536;
        std::cout << tmp._u.int64_val << ":" << tmp.hash() << std::endl;
    }
}

TEST(type_merge, type_merge) {
    FunctionManager::instance()->init();
    {
        pb::Function f;
        f.set_name("case_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<pb::PrimitiveType> types {pb::STRING, pb::STRING, pb::STRING};
        FunctionManager::instance()->complete_common_fn(f, types);
        EXPECT_EQ(pb::STRING, f.return_type());
    }
    {
        pb::Function f;
        f.set_name("case_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<pb::PrimitiveType> types {pb::STRING, pb::INT8, pb::INT64};
        FunctionManager::instance()->complete_common_fn(f, types);
        EXPECT_EQ(pb::INT64, f.return_type());
    }

    {
        pb::Function f;
        f.set_name("case_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<pb::PrimitiveType> types {pb::STRING, pb::INT8, pb::INT64, pb::DOUBLE};
        FunctionManager::instance()->complete_common_fn(f, types);
        EXPECT_EQ(pb::DOUBLE, f.return_type());
    }

    {
        pb::Function f;
        f.set_name("case_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<pb::PrimitiveType> types {pb::STRING, pb::INT64, pb::UINT64};
        FunctionManager::instance()->complete_common_fn(f, types);
        EXPECT_EQ(pb::DOUBLE, f.return_type());
    }

    {
        pb::Function f;
        f.set_name("case_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<pb::PrimitiveType> types {pb::STRING, pb::INT64, pb::UINT64, pb::DATETIME};
        FunctionManager::instance()->complete_common_fn(f, types);
        EXPECT_EQ(pb::STRING, f.return_type());
    }

    {
        pb::Function f;
        f.set_name("case_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<pb::PrimitiveType> types {pb::STRING, pb::TIME, pb::DATETIME};
        FunctionManager::instance()->complete_common_fn(f, types);
        EXPECT_EQ(pb::DATETIME, f.return_type());
    }

    {
        pb::Function f;
        f.set_name("case_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<pb::PrimitiveType> types {pb::STRING, pb::TIME, pb::INT8};
        FunctionManager::instance()->complete_common_fn(f, types);
        EXPECT_EQ(pb::STRING, f.return_type());
    }

    {
        pb::Function f;
        f.set_name("case_expr_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<pb::PrimitiveType> types {pb::STRING, pb::STRING, pb::STRING};
        FunctionManager::instance()->complete_common_fn(f, types);
        EXPECT_EQ(pb::STRING, f.return_type());
    }
    {
        pb::Function f;
        f.set_name("case_expr_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<pb::PrimitiveType> types {pb::STRING, pb::STRING, pb::INT8, pb::INT64};
        FunctionManager::instance()->complete_common_fn(f, types);
        EXPECT_EQ(pb::INT64, f.return_type());
    }

    {
        pb::Function f;
        f.set_name("case_expr_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<pb::PrimitiveType> types {pb::STRING, pb::STRING, pb::INT8, pb::INT64, pb::DOUBLE};
        FunctionManager::instance()->complete_common_fn(f, types);
        EXPECT_EQ(pb::DOUBLE, f.return_type());
    }

    {
        pb::Function f;
        f.set_name("case_expr_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<pb::PrimitiveType> types {pb::STRING, pb::STRING, pb::INT64, pb::UINT64};
        FunctionManager::instance()->complete_common_fn(f, types);
        EXPECT_EQ(pb::DOUBLE, f.return_type());
    }

    {
        pb::Function f;
        f.set_name("case_expr_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<pb::PrimitiveType> types {pb::STRING, pb::STRING, pb::INT64, pb::UINT64, pb::DATETIME};
        FunctionManager::instance()->complete_common_fn(f, types);
        EXPECT_EQ(pb::STRING, f.return_type());
    }

    {
        pb::Function f;
        f.set_name("case_expr_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<pb::PrimitiveType> types {pb::STRING, pb::STRING, pb::TIME, pb::DATETIME};
        FunctionManager::instance()->complete_common_fn(f, types);
        EXPECT_EQ(pb::DATETIME, f.return_type());
    }

    {
        pb::Function f;
        f.set_name("case_expr_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<pb::PrimitiveType> types {pb::STRING, pb::STRING, pb::TIME, pb::INT8};
        FunctionManager::instance()->complete_common_fn(f, types);
        EXPECT_EQ(pb::STRING, f.return_type());
    }
    
    {
        pb::Function f;
        f.set_name("case_expr_when");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<pb::PrimitiveType> types {pb::STRING, pb::STRING, pb::INT8, pb::INT8, pb::NULL_TYPE};
        FunctionManager::instance()->complete_common_fn(f, types);
        EXPECT_EQ(pb::INT8, f.return_type());
    }

    {
        pb::Function f;
        f.set_name("if");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<pb::PrimitiveType> types {pb::STRING, pb::STRING, pb::STRING};
        FunctionManager::instance()->complete_common_fn(f, types);
        EXPECT_EQ(pb::STRING, f.return_type());
    }
    {
        pb::Function f;
        f.set_name("if");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<pb::PrimitiveType> types {pb::STRING, pb::INT8, pb::INT64};
        FunctionManager::instance()->complete_common_fn(f, types);
        EXPECT_EQ(pb::INT64, f.return_type());
    }

    {
        pb::Function f;
        f.set_name("if");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<pb::PrimitiveType> types {pb::STRING, pb::INT64, pb::UINT64};
        FunctionManager::instance()->complete_common_fn(f, types);
        EXPECT_EQ(pb::DOUBLE, f.return_type());
    }

    {
        pb::Function f;
        f.set_name("if");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<pb::PrimitiveType> types {pb::STRING, pb::TIME, pb::DATETIME};
        FunctionManager::instance()->complete_common_fn(f, types);
        EXPECT_EQ(pb::DATETIME, f.return_type());
    }

    {
        pb::Function f;
        f.set_name("if");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<pb::PrimitiveType> types {pb::STRING, pb::TIME, pb::INT8};
        FunctionManager::instance()->complete_common_fn(f, types);
        EXPECT_EQ(pb::STRING, f.return_type());
    }
    {
        pb::Function f;
        f.set_name("if");
        f.set_fn_op(parser::FT_COMMON);
        std::vector<pb::PrimitiveType> types {pb::STRING, pb::NULL_TYPE, pb::INT8};
        FunctionManager::instance()->complete_common_fn(f, types);
        EXPECT_EQ(pb::INT8, f.return_type());
    }
    {
        std::unordered_set<ExprValueVec, ExprValueVec::HashFunction> ABCSet; 
        ExprValue v1(pb::UINT32);
        v1._u.uint32_val = 1;
        ExprValue v2(pb::UINT32);
        v2._u.uint32_val = 2;
        ExprValue v3(pb::UINT32);
        v3._u.uint32_val = 1;

        ExprValueVec vec1;
        vec1.vec.emplace_back(v1);

        ABCSet.emplace(vec1);


        ExprValueVec vec2;
        vec2.vec.emplace_back(v2);

        ABCSet.emplace(vec2);


        ExprValueVec vec3;
        vec3.vec.emplace_back(v3);

        ABCSet.emplace(vec3);
        EXPECT_EQ(ABCSet.size(), 2);
        for (auto& it : ABCSet) {
            for (auto& v : it.vec) {
                DB_WARNING("value: %u", v._u.uint32_val);
            }
        }

    }
}

}  // namespace baikal
