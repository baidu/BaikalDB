// Copyright (c) 2022 Baidu, Inc. All Rights Reserved.
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

#include "meta_util.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace baikaldb {
namespace partition_utils {

TEST(test_compare, case_all) {
    pb::Expr left_expr;
    pb::Expr right_expr;
    EXPECT_EQ(create_partition_expr(pb::DATE, "2023-04-01", left_expr), 0);
    EXPECT_EQ(create_partition_expr(pb::DATE, "2023-04-02", right_expr), 0);
    EXPECT_EQ(compare(left_expr, left_expr) == 0, true);
    EXPECT_EQ(compare(left_expr, right_expr) < 0, true);
    EXPECT_EQ(compare(right_expr, left_expr) > 0, true);
}

TEST(test_min_max, case_all) {
    pb::Expr left_expr;
    pb::Expr right_expr;
    EXPECT_EQ(create_partition_expr(pb::DATE, "2023-04-01", left_expr), 0);
    EXPECT_EQ(create_partition_expr(pb::DATE, "2023-04-02", right_expr), 0);

    pb::Expr min_expr = min(left_expr, right_expr);
    pb::Expr max_expr = max(left_expr, right_expr);
    EXPECT_EQ(compare(min_expr, left_expr), 0);
    EXPECT_EQ(compare(max_expr, right_expr), 0);
}

TEST(test_is_equal, case_all) {
    std::string partition_prefix;
    pb::PrimitiveType partition_col_type = pb::DATE;
    time_t current_ts = 1682500108;
    int32_t offset = 1;
    TimeUnit time_unit = TimeUnit::DAY;
    pb::RangePartitionInfo range_partition_info_left;
    pb::RangePartitionInfo range_partition_info_right;

    EXPECT_EQ(create_dynamic_range_partition_info(partition_prefix, partition_col_type, current_ts,
                                       offset, time_unit, range_partition_info_left), 0);
    EXPECT_EQ(create_dynamic_range_partition_info(partition_prefix, partition_col_type, current_ts, 
                                       offset + 1, time_unit, range_partition_info_right), 0);
    EXPECT_EQ(is_equal(range_partition_info_left, range_partition_info_left), true);
    EXPECT_EQ(is_equal(range_partition_info_left, range_partition_info_right), false);
}

TEST(test_check_partition_expr, case_all) {
    pb::Expr partition_expr;
    EXPECT_EQ(check_partition_expr(partition_expr, false), -1);
    pb::ExprNode* p_node = partition_expr.add_nodes();
    EXPECT_EQ(check_partition_expr(partition_expr, false), -1);
    p_node->set_node_type(pb::STRING_LITERAL);
    p_node->set_num_children(0);
    p_node->set_col_type(pb::DATETIME);
    p_node->mutable_derive_node()->set_string_val("2023-05-06 10:00:00");
    EXPECT_EQ(check_partition_expr(partition_expr, false), 0);
    p_node->mutable_derive_node()->set_string_val("223-01-01 00:00:00");
    EXPECT_EQ(check_partition_expr(partition_expr, false), 0);
    p_node->mutable_derive_node()->set_string_val("0000-00-00 00:00:00");
    EXPECT_EQ(check_partition_expr(partition_expr, false), -1);
    p_node->mutable_derive_node()->set_string_val("invalid");
    EXPECT_EQ(check_partition_expr(partition_expr, false), -1);
    p_node->mutable_derive_node()->set_string_val("0000-00-01 00:00:00");
    EXPECT_EQ(check_partition_expr(partition_expr, false), -1);
    p_node->mutable_derive_node()->set_string_val("9999-13-01 00:00:00");
    EXPECT_EQ(check_partition_expr(partition_expr, false), -1);
    p_node->mutable_derive_node()->set_string_val("9999-12-01 00:60:00");
    EXPECT_EQ(check_partition_expr(partition_expr, false), -1);
    p_node->mutable_derive_node()->set_string_val("99999-12-01 00:00:00");
    EXPECT_EQ(check_partition_expr(partition_expr, false), -1);
    p_node->mutable_derive_node()->set_string_val("9999-12-31 23:59:59");
    EXPECT_EQ(check_partition_expr(partition_expr, false), -1);
    EXPECT_EQ(check_partition_expr(partition_expr, true), 0);
    p_node->mutable_derive_node()->set_string_val("0000-01-01 00:00:00");
    EXPECT_EQ(check_partition_expr(partition_expr, false), 0);
    EXPECT_EQ(check_partition_expr(partition_expr, true), -1);

    pb::Expr partition_expr_tmp;
    partition_expr.Swap(&partition_expr_tmp);
    p_node = partition_expr.add_nodes();
    p_node->set_node_type(pb::INT_LITERAL);
    p_node->set_num_children(0);
    p_node->set_col_type(pb::INT32);
    p_node->mutable_derive_node()->set_int_val(10);
    EXPECT_EQ(check_partition_expr(partition_expr, true), 0);
}

TEST(test_get_partition_value, case_all) {
    pb::Expr partition_expr;
    ExprValue partition_value;
    EXPECT_EQ(get_partition_value(partition_expr, partition_value), -1);
    EXPECT_EQ(create_partition_expr(pb::DATE, "2023-04-01", partition_expr), 0);
    EXPECT_EQ(get_partition_value(partition_expr, partition_value), 0);
    EXPECT_EQ(partition_value.get_string(), "2023-04-01");
}

TEST(test_get_min_partition_value, case_all) {
    pb::PrimitiveType partition_col_type = pb::DATE;
    std::string partition_str_val;
    EXPECT_EQ(get_min_partition_value(partition_col_type, partition_str_val), 0);
    EXPECT_EQ(partition_str_val, "0000-01-01");

    partition_col_type = pb::DATETIME;
    EXPECT_EQ(get_min_partition_value(partition_col_type, partition_str_val), 0);
    EXPECT_EQ(partition_str_val, "0000-01-01 00:00:00");

    partition_col_type = pb::INT8;
    EXPECT_EQ(get_min_partition_value(partition_col_type, partition_str_val), 0);
    EXPECT_EQ(partition_str_val, "-128");

    partition_col_type = pb::INT16;
    EXPECT_EQ(get_min_partition_value(partition_col_type, partition_str_val), 0);
    EXPECT_EQ(partition_str_val, "-32768");

    partition_col_type = pb::INT32;
    EXPECT_EQ(get_min_partition_value(partition_col_type, partition_str_val), 0);
    EXPECT_EQ(partition_str_val, "-2147483648");

    partition_col_type = pb::INT64;
    EXPECT_EQ(get_min_partition_value(partition_col_type, partition_str_val), 0);
    EXPECT_EQ(partition_str_val, "-9223372036854775808");

    partition_col_type = pb::UINT8;
    EXPECT_EQ(get_min_partition_value(partition_col_type, partition_str_val), 0);
    EXPECT_EQ(partition_str_val, "0");

    partition_col_type = pb::UINT16;
    EXPECT_EQ(get_min_partition_value(partition_col_type, partition_str_val), 0);
    EXPECT_EQ(partition_str_val, "0");

    partition_col_type = pb::UINT32;
    EXPECT_EQ(get_min_partition_value(partition_col_type, partition_str_val), 0);
    EXPECT_EQ(partition_str_val, "0");

    partition_col_type = pb::UINT64;
    EXPECT_EQ(get_min_partition_value(partition_col_type, partition_str_val), 0);
    EXPECT_EQ(partition_str_val, "0");

    partition_col_type = pb::STRING;
    EXPECT_EQ(get_min_partition_value(partition_col_type, partition_str_val), -1);
}

TEST(test_get_max_partition_value, case_all) {
    pb::PrimitiveType partition_col_type = pb::DATE;
    std::string partition_str_val;
    EXPECT_EQ(get_max_partition_value(partition_col_type, partition_str_val), 0);
    EXPECT_EQ(partition_str_val, "9999-12-31");

    partition_col_type = pb::DATETIME;
    EXPECT_EQ(get_max_partition_value(partition_col_type, partition_str_val), 0);
    EXPECT_EQ(partition_str_val, "9999-12-31 23:59:59");

    partition_col_type = pb::INT8;
    EXPECT_EQ(get_max_partition_value(partition_col_type, partition_str_val), 0);
    EXPECT_EQ(partition_str_val, "127");

    partition_col_type = pb::INT16;
    EXPECT_EQ(get_max_partition_value(partition_col_type, partition_str_val), 0);
    EXPECT_EQ(partition_str_val, "32767");

    partition_col_type = pb::INT32;
    EXPECT_EQ(get_max_partition_value(partition_col_type, partition_str_val), 0);
    EXPECT_EQ(partition_str_val, "2147483647");

    partition_col_type = pb::INT64;
    EXPECT_EQ(get_max_partition_value(partition_col_type, partition_str_val), 0);
    EXPECT_EQ(partition_str_val, "9223372036854775807");

    partition_col_type = pb::UINT8;
    EXPECT_EQ(get_max_partition_value(partition_col_type, partition_str_val), 0);
    EXPECT_EQ(partition_str_val, "255");

    partition_col_type = pb::UINT16;
    EXPECT_EQ(get_max_partition_value(partition_col_type, partition_str_val), 0);
    EXPECT_EQ(partition_str_val, "65535");

    partition_col_type = pb::UINT32;
    EXPECT_EQ(get_max_partition_value(partition_col_type, partition_str_val), 0);
    EXPECT_EQ(partition_str_val, "4294967295");

    partition_col_type = pb::UINT64;
    EXPECT_EQ(get_max_partition_value(partition_col_type, partition_str_val), 0);
    EXPECT_EQ(partition_str_val, "18446744073709551615");

    partition_col_type = pb::STRING;
    EXPECT_EQ(get_max_partition_value(partition_col_type, partition_str_val), -1);
}

TEST(test_check_range_partition_info, case_all) {
    std::string partition_prefix;
    pb::PrimitiveType partition_col_type = pb::DATE;
    time_t current_ts = 1682500108;
    int32_t offset = 1;
    TimeUnit time_unit = TimeUnit::DAY;
    pb::RangePartitionInfo range_partition_info;
    EXPECT_EQ(check_range_partition_info(range_partition_info), true);

    EXPECT_EQ(create_dynamic_range_partition_info(partition_prefix, partition_col_type, current_ts,
                                       offset, time_unit, range_partition_info), 0);
    EXPECT_EQ(check_range_partition_info(range_partition_info), true);

    range_partition_info.mutable_range()->clear_right_value();
    EXPECT_EQ(check_range_partition_info(range_partition_info), false);

    range_partition_info.mutable_range()->clear_left_value();
    EXPECT_EQ(check_range_partition_info(range_partition_info), false);
}

TEST(test_dynamic_partition_attr, case_all) {
    pb::DynamicPartitionAttr dynamic_partition_attr;
    dynamic_partition_attr.set_enable(true);
    dynamic_partition_attr.set_time_unit("DAY");
    dynamic_partition_attr.set_start(-7);
    dynamic_partition_attr.set_cold(-3);
    dynamic_partition_attr.set_end(1);
    dynamic_partition_attr.set_start_day_of_month(1);
    EXPECT_EQ(check_dynamic_partition_attr(dynamic_partition_attr), true);

    {
        pb::DynamicPartitionAttr dynamic_partition_attr_tmp = dynamic_partition_attr;
        dynamic_partition_attr_tmp.set_time_unit("INVALID_TIME_UNIT");
        EXPECT_EQ(check_dynamic_partition_attr(dynamic_partition_attr_tmp), false);
    }
    {
        pb::DynamicPartitionAttr dynamic_partition_attr_tmp = dynamic_partition_attr;
        dynamic_partition_attr_tmp.set_start(1);
        EXPECT_EQ(check_dynamic_partition_attr(dynamic_partition_attr_tmp), false);
    }
    {
        pb::DynamicPartitionAttr dynamic_partition_attr_tmp = dynamic_partition_attr;
        dynamic_partition_attr_tmp.set_cold(1);
        EXPECT_EQ(check_dynamic_partition_attr(dynamic_partition_attr_tmp), false);
    }
    {
        pb::DynamicPartitionAttr dynamic_partition_attr_tmp = dynamic_partition_attr;
        dynamic_partition_attr_tmp.set_end(-1);
        EXPECT_EQ(check_dynamic_partition_attr(dynamic_partition_attr_tmp), false);
    }
    {
        pb::DynamicPartitionAttr dynamic_partition_attr_tmp = dynamic_partition_attr;
        dynamic_partition_attr_tmp.set_end(501);
        EXPECT_EQ(check_dynamic_partition_attr(dynamic_partition_attr_tmp), false);
    }
    {
        pb::DynamicPartitionAttr dynamic_partition_attr_tmp = dynamic_partition_attr;
        dynamic_partition_attr_tmp.set_start_day_of_month(29);
        EXPECT_EQ(check_dynamic_partition_attr(dynamic_partition_attr_tmp), false);
    }
}

TEST(test_check_partition_overlapped, case_all) {
    std::string partition_prefix;
    pb::PrimitiveType partition_col_type = pb::DATE;
    time_t current_ts = 1682500108;
    int32_t offset = 0;
    TimeUnit time_unit = TimeUnit::DAY;
    pb::RangePartitionInfo range_partition_info_1;
    pb::RangePartitionInfo range_partition_info_2;
    pb::RangePartitionInfo range_partition_info_3;
    pb::RangePartitionInfo range_partition_info_repeated;

    EXPECT_EQ(create_dynamic_range_partition_info(partition_prefix, partition_col_type, current_ts,
                                       offset + 1, time_unit, range_partition_info_1), 0);
    EXPECT_EQ(create_dynamic_range_partition_info(partition_prefix, partition_col_type, current_ts,
                                       offset + 2, time_unit, range_partition_info_2), 0);
    EXPECT_EQ(create_dynamic_range_partition_info(partition_prefix, partition_col_type, current_ts,
                                       offset + 3, time_unit, range_partition_info_3), 0);
    EXPECT_EQ(create_dynamic_range_partition_info(partition_prefix, partition_col_type, current_ts,
                                       offset + 1, time_unit, range_partition_info_repeated), 0);
    range_partition_info_repeated.set_partition_name("range_partition_info_repeated");

    std::vector<pb::RangePartitionInfo> range_partition_info_vec;
    range_partition_info_vec.push_back(range_partition_info_1);
    range_partition_info_vec.push_back(range_partition_info_2);
    EXPECT_EQ(check_partition_overlapped(range_partition_info_vec, range_partition_info_3), false);
    EXPECT_EQ(check_partition_overlapped(range_partition_info_vec, range_partition_info_repeated), true);

    ::google::protobuf::RepeatedPtrField<pb::RangePartitionInfo> range_partition_infos;
    range_partition_infos.Add()->CopyFrom(range_partition_info_1);
    range_partition_infos.Add()->CopyFrom(range_partition_info_2);
    EXPECT_EQ(check_partition_overlapped(range_partition_infos, range_partition_info_3), false);
    EXPECT_EQ(check_partition_overlapped(range_partition_infos, range_partition_info_repeated), true);
}

TEST(test_get_specifed_partitions, case_all) {
    std::string partition_prefix = "p";
    pb::PrimitiveType partition_col_type = pb::DATE;
    time_t current_ts = 1682500108;
    int32_t offset = 0;
    TimeUnit time_unit = TimeUnit::DAY;
    pb::RangePartitionInfo range_partition_info_1;
    pb::RangePartitionInfo range_partition_info_2;
    pb::RangePartitionInfo range_partition_info_3;

    EXPECT_EQ(create_dynamic_range_partition_info(partition_prefix, partition_col_type, current_ts,
                                       offset + 1, time_unit, range_partition_info_1), 0);
    EXPECT_EQ(create_dynamic_range_partition_info(partition_prefix, partition_col_type, current_ts,
                                       offset + 2, time_unit, range_partition_info_2), 0);
    EXPECT_EQ(create_dynamic_range_partition_info(partition_prefix, partition_col_type, current_ts,
                                       offset + 3, time_unit, range_partition_info_3), 0);

    range_partition_info_2.set_is_cold(true);

    ::google::protobuf::RepeatedPtrField<pb::RangePartitionInfo> range_partition_infos;
    range_partition_infos.Add()->CopyFrom(range_partition_info_1);
    range_partition_infos.Add()->CopyFrom(range_partition_info_2);
    range_partition_infos.Add()->CopyFrom(range_partition_info_3);

    pb::Expr specified_expr;
    EXPECT_EQ(create_partition_expr(pb::DATE, "2023-04-29", specified_expr), 0);

    ::google::protobuf::RepeatedPtrField<pb::RangePartitionInfo> specified_range_partition_infos;
    EXPECT_EQ(get_specifed_partitions(specified_expr, range_partition_infos, specified_range_partition_infos), 0);
    ASSERT_EQ(specified_range_partition_infos.size(), 2);
    EXPECT_EQ(specified_range_partition_infos[0].partition_name(), "p20230427");
    EXPECT_EQ(specified_range_partition_infos[1].partition_name(), "p20230428");

    specified_range_partition_infos.Clear();
    EXPECT_EQ(get_specifed_partitions(specified_expr, range_partition_infos, specified_range_partition_infos, true), 0);
    ASSERT_EQ(specified_range_partition_infos.size(), 1);
    EXPECT_EQ(specified_range_partition_infos[0].partition_name(), "p20230427");
}

TEST(test_convert_to_partition_range, case_all) {
    pb::RangePartitionInfo range_partition_info;
    EXPECT_EQ(convert_to_partition_range(pb::DATE, range_partition_info), 0);

    pb::Expr less_expr;
    EXPECT_EQ(create_partition_expr(pb::DATE, "2023-04-01", less_expr), 0);
    
    range_partition_info.mutable_less_value()->Swap(&less_expr);
    EXPECT_EQ(convert_to_partition_range(pb::DATE, range_partition_info), 0);

    ASSERT_EQ(range_partition_info.range().left_value().nodes_size(), 1);
    EXPECT_EQ(range_partition_info.range().left_value().nodes(0).derive_node().string_val(), "0000-01-01");
    ASSERT_EQ(range_partition_info.range().right_value().nodes_size(), 1);
    EXPECT_EQ(range_partition_info.range().right_value().nodes(0).derive_node().string_val(), "2023-04-01");
}

TEST(test_set_partition_col_type, case_all) {
    pb::RangePartitionInfo range_partition_info;

    pb::Expr less_expr;
    EXPECT_EQ(create_partition_expr(pb::DATE, "2023-04-01", less_expr), 0);
    
    range_partition_info.mutable_less_value()->Swap(&less_expr);
    EXPECT_EQ(set_partition_col_type(pb::DATE, range_partition_info), 0);
    ASSERT_EQ(range_partition_info.less_value().nodes_size(), 1);
    EXPECT_EQ(range_partition_info.less_value().nodes(0).col_type(), pb::DATE);

    EXPECT_EQ(convert_to_partition_range(pb::DATE, range_partition_info), 0);
    ASSERT_EQ(range_partition_info.range().left_value().nodes_size(), 1);
    EXPECT_EQ(range_partition_info.range().left_value().nodes(0).col_type(), pb::DATE);
    ASSERT_EQ(range_partition_info.range().right_value().nodes_size(), 1);
    EXPECT_EQ(range_partition_info.range().right_value().nodes(0).col_type(), pb::DATE);
}

TEST(test_get_partition_range, case_all) {
    pb::RangePartitionInfo range_partition_info;

    std::pair<std::string, std::string> range_key;
    EXPECT_NE(get_partition_range(range_partition_info, range_key), 0);

    pb::Expr less_expr;
    EXPECT_EQ(create_partition_expr(pb::DATE, "2023-04-01", less_expr), 0);
    
    range_partition_info.mutable_less_value()->Swap(&less_expr);
    EXPECT_EQ(set_partition_col_type(pb::DATE, range_partition_info), 0);
    ASSERT_EQ(range_partition_info.less_value().nodes_size(), 1);
    EXPECT_EQ(range_partition_info.less_value().nodes(0).col_type(), pb::DATE);
    EXPECT_EQ(convert_to_partition_range(pb::DATE, range_partition_info), 0);

    ASSERT_EQ(range_partition_info.range().left_value().nodes_size(), 1);
    EXPECT_EQ(range_partition_info.range().left_value().nodes(0).col_type(), pb::DATE);
    ASSERT_EQ(range_partition_info.range().right_value().nodes_size(), 1);
    EXPECT_EQ(range_partition_info.range().right_value().nodes(0).col_type(), pb::DATE);

    EXPECT_EQ(get_partition_range(range_partition_info, range_key), 0);
    EXPECT_EQ(range_key.first, "0000-01-01");
    EXPECT_EQ(range_key.second, "2023-04-01");
}

} // namespace partition_utils
} // namespace baikaldb