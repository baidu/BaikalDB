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
#include <sstream>
#include <iomanip>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include "expr_value.h"
#include "fn_manager.h"
#include "proto/expr.pb.h"
#include "parser.h"
#include "proto/meta.interface.pb.h"
#include "joiner.h"
#include "arrow/vendored/fast_float/fast_float.h"
#include "arrow/vendored/double-conversion/double-to-string.h"

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace baikaldb {
TEST(test_proto, case_all) {
    std::vector<int> vv {11,22,33,44};
    std::cout << "v:" << vv[-1] << vv[10000] << std::endl;
    float flt = 1.2;
    double db = flt;
    char buf[100] = {0};
    snprintf(buf, sizeof(buf), "%.12g", db);
    std::cout << flt << ":" << buf << "\n";
    double db2 = 1345678891284324321345.1233523;
    snprintf(buf, sizeof(buf), "%.3f", db2);
    std::cout << flt << ":" << buf << "\n";
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

TEST(float_parse_cost, case_all) {
    using ::arrow_vendored::fast_float::from_chars;
    srand(static_cast <unsigned> (time(0)));
    std::vector<std::string> float_vec_string;
    std::vector<double> float_vec;
    float_vec.reserve(10000);
    for (int i = 0; i < 10000; i++) {
        char tmp_buf[24] = {0};
        snprintf(tmp_buf, sizeof(tmp_buf), "%.12g", (rand() / (RAND_MAX + 1.0)));
        float_vec_string.push_back(tmp_buf);
        if (i % 243 == 0) {
            std::cout << float_vec_string.back() << std::endl;
        }
    }
    TimeCost cost;
    double result = 0.0;
    double result2 = 0.0;
    for (const auto& s : float_vec_string) {
        auto res = from_chars(s.data(), s.data() + s.size(), result2);
        result = result > result2 ? result : result2;
    }
    int64_t old_cost =  cost.get_time();
    cost.reset();
    float_vec.clear();
    for (const auto& s : float_vec_string) {
        result2 = strtod(s.c_str(), NULL);
        result = result > result2 ? result : result2;
    }
    int64_t new_cost =  cost.get_time();
    std::cout << "float_parse_cost ===  fast_float_cost:" << old_cost << " strtod_cost: " << new_cost << std::endl;
    float_vec_string.emplace_back("+12.12134e32");
    float_vec_string.emplace_back("-12.12134e32");
    float_vec_string.emplace_back("-12.12134e-32abc");
    float_vec_string.emplace_back("-12.12134E-32abc");
    float_vec_string.emplace_back("-12.12134E+32abc");
    float_vec_string.emplace_back("-abc");
    float_vec_string.emplace_back("-.124wq12412");
    float_vec_string.emplace_back("1.2345xxa>??");
    float_vec_string.emplace_back("\t 1.2345");
    float_vec_string.emplace_back("1.2345e991");
    float_vec_string.emplace_back("1.2345e-991");
    float_vec_string.emplace_back("1.7976931348623157e+308");
    float_vec_string.emplace_back("1.79769313486232e+308");
    for (const auto& s : float_vec_string) {
        result = 0.0;
        auto res = from_chars(s.data(), s.data() + s.size(), result);
        if (res.ec != std::errc()) { 
            std::cerr << "parsing failure" << s << "\n"; 
        }
        result2 = strtod(s.c_str(), NULL);
        EXPECT_EQ(result, result2);
        //std::cout << result << ":" << result2 << " raw:" << s << "\n";
    }
}

TEST(float_fmt_cost, case_all) {
    using ::arrow_vendored::double_conversion::DoubleToStringConverter;
    using ::arrow_vendored::double_conversion::StringBuilder;
    FLAGS_use_double_conversion = true;
    FLAGS_double_use_all_precision = true;
    srand(static_cast <unsigned> (time(0)));
    std::vector<double> float_vec;
    for (int i = 0; i < 10000; i++) {
        float_vec.push_back(rand() / (RAND_MAX + 1.));
    }
    TimeCost cost;
    for (int i = 0; i < 10000; i++) {
        char tmp_buf[50] = {0};
        snprintf(tmp_buf, sizeof(tmp_buf), "%.12g", float_vec[i]);
    }
    int64_t old_cost =  cost.get_time();
    cost.reset();
    for (int i = 0; i < 10000; i++) {
        std::stringstream ss;
        ss << std::setprecision(12) << float_vec[i];
        std::string double_string = ss.str();
    }
    int64_t new_cost =  cost.get_time();
    cost.reset();
    for (auto& dbl : float_vec) {
        char buf[50] = {0};
        int len = parser::double_to_string(dbl, -1, buf, sizeof(buf));
    }
    int64_t gg_cost =  cost.get_time();
    std::cout << "print_cost:" << old_cost << " ss_cost: " << new_cost << " gg_cost:" << gg_cost << std::endl;
    for (auto& dbl : float_vec) {
        char buf[100] = {0};
        int len = parser::double_to_string(dbl, -1, buf, sizeof(buf));
        EXPECT_EQ(dbl, strtod(buf, NULL));
        EXPECT_EQ(len, strlen(buf));
    }
    std::vector<std::pair<double, std::string>> dbl_map = {
        {777777744225350500000000000.0, "7.777777442253505e26"},
        {0.123456789123456789, "0.12345678912345678"},
        {1.7976931348623157e+308, "1.7976931348623157e308"},
        {-1.7976931348623157e+308, "-1.7976931348623157e308"},
        {-1.797693134862315789e+308, "-1.7976931348623157e308"},
        {4.940656458412465442e-324, "5e-324"},
        {0.1 * 10, "1"},
        {0.001 * 10, "0.01"},
        {0.003 * 100, "0.3"},
        //{1.003 * 100, "100.3"},
        //{1.003 * 1000, "1003"},
    };
    for (auto& [dbl, str] : dbl_map) {
        char buf[100] = {0};
        int len = parser::double_to_string(dbl, -1, buf, sizeof(buf));
        std::cout << str << ":" << buf << ">" << len << "\n";
        EXPECT_STREQ(str.c_str(), buf);
        EXPECT_EQ(str.size(), len);
    }
    EXPECT_EQ(0.003 * 100, 0.3);
    //EXPECT_EQ(1.003 * 100, 100.3) << "error 100.3"; // 精度损失，下面的case才对
    EXPECT_EQ(1.003 * 100, 100.29999999999998) << "error 100.29999999999998";
    EXPECT_EQ(1.003, 100.3 / 100) << "error 100.3 / 100"; 
    EXPECT_EQ(1.003 * 10, 10.03) << "error 1.003 * 10 10.03"; 
    FLAGS_double_use_all_precision = false;  //保留12位精度
    dbl_map = {
        {0.01, "0.01"},
        {0.00001, "0.00001"},
        {0.0000001, "0.0000001"},
        {10000000.0, "10000000"},
        {777777744225350500000000000.0, "7.77777744225e26"},
        {0.123456789123456789, "0.123456789123"},
        {1.7976931348623157e+308, "1.79769313486e308"},
        {-1.7976931348623157e+308, "-1.79769313486e308"},
        {-1.797693134862315789e+308, "-1.79769313486e308"},
        {4.940656458412465442e-324, "4.94065645841e-324"},
        {0.1 * 10, "1"},
        {0.001 * 10, "0.01"},
        {0.003 * 100, "0.3"},
        {1.003 * 100, "100.3"},
        {1.003 * 1000, "1003"},
        {0.1 * 10, "1"},
        {0.001 * 10, "0.01"},
        {1.003 * 100, "100.3"},
        {1.003 * 1000, "1003"},
    };
    for (auto& [dbl, str] : dbl_map) {
        char buf[100] = {0};
        int len = parser::double_to_string(dbl, -1, buf, sizeof(buf));
        std::cout << std::setprecision(12) << dbl << ":" << str << ":" << buf << "\n";
        EXPECT_STREQ(str.c_str(), buf);
        EXPECT_EQ(str.size(), len);
        // 测试固定小数位情况
        FLAGS_use_double_conversion = false;
        parser::float_to_string(dbl, 7, buf, sizeof(buf));
        FLAGS_use_double_conversion = true;
        char buf2[100] = {0};
        parser::float_to_string(dbl, 7, buf2, sizeof(buf2));
        std::cout << buf << ":" << buf2 << "\n";
        EXPECT_STREQ(buf, buf2);
    }
    std::vector<std::pair<float, std::string>> flt_map = {
        {0.01, "0.01"},
        {0.001, "0.001"},
        {0.0001, "0.0001"},
        {0.00001, "0.00001"},
        {0.000001, "1e-6"},
        {0.0000001, "1e-7"},
        {0.123451334, "0.123451"},
        {0.00000123451334, "1.23451e-6"},
        {10.0, "10"},
        {100.0, "100"},
        {1000.0, "1000"},
        {10000.0, "10000"},
        {100000.0, "100000"},
        {1000000.0, "1e6"},
        {10000000.0, "1e7"},
        {3.402823466E+38, "3.40282e38"},
        {9.78954123, "9.78954"},
        {987654321.0, "9.87654e8"},
        {4.84064234e-15, "4.84064e-15"},
        {0.1 * 10, "1"},
        {0.001 * 10, "0.01"},
        {1.003 * 100, "100.3"},
        {1.003 * 1000, "1003"},
    };
    for (auto&  [dbl, str] : flt_map) {
        char buf[100] = {0};
        int len = parser::float_to_string(dbl, -1, buf, sizeof(buf));
        std::cout << std::setprecision(6) << dbl << ":" << buf << ">" << len << "\n";
        EXPECT_STREQ(str.c_str(), buf);
        EXPECT_EQ(str.size(), len);
        // 测试固定小数位情况
        FLAGS_use_double_conversion = false;
        parser::float_to_string(dbl, 3, buf, sizeof(buf));
        FLAGS_use_double_conversion = true;
        char buf2[100] = {0};
        parser::float_to_string(dbl, 3, buf2, sizeof(buf2));
        std::cout << buf << ":" << buf2 << "\n";
        EXPECT_STREQ(buf, buf2);
    }
    //FLAGS_use_double_conversion = false;
    //for (auto&  [dbl, str] : flt_map) {
    //    char buf[100] = {0};
    //    int len = parser::float_to_string(dbl, -1, buf, sizeof(buf));
    //    std::cout << std::setprecision(6) << dbl << ":" << buf << ">" << len << "\n";
    //    EXPECT_STREQ(str.c_str(), buf);
    //    EXPECT_EQ(str.size(), len);
    //}
    //FLAGS_use_double_conversion = true;
    
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
        ExprValue v1(pb::FLOAT);
        v1._u.float_val = 123.234567;
        v1.float_precision_len = 3;
        std::cout << v1.get_string() << "\n";
        EXPECT_STREQ(v1.get_string().c_str(), "123.235");
    }
    {
        ExprValue v1(pb::DOUBLE);
        v1._u.double_val = 123.234567;
        v1.float_precision_len = 4;
        std::cout << v1.get_string() << "\n";
        EXPECT_STREQ(v1.get_string().c_str(), "123.2346");
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
        v1.str_val = "2037-10-11 01:52:41.123456";
        v1.float_precision_len = 4;
        v1.cast_to(pb::DATETIME);
        EXPECT_STREQ(v1.get_string().c_str(), "2037-10-11 01:52:41.1234");
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
    {
        ExprValue v1(pb::MAXVALUE_TYPE);
        ExprValue v2(pb::INT32);
        v2._u.int32_val = 2147483647; 
        EXPECT_GT(v1.compare(v2), 0);
    }
    {
        ExprValue v1(pb::MAXVALUE_TYPE);
        ExprValue v2(pb::MAXVALUE_TYPE);
        EXPECT_EQ(v1.compare(v2), 0);
    }
    {
        ExprValue v1(pb::INT32);
        v1._u.int32_val = 2147483647;
        ExprValue v2(pb::MAXVALUE_TYPE);
        EXPECT_LT(v1.compare(v2), 0);
    }
    {
        ExprValue v1(pb::MAXVALUE_TYPE);
        EXPECT_EQ(v1.get_string(), "MAXVALUE");
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
