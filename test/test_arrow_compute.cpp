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
#include "common.h"
#include "expr_value.h"
#include "arrow_function.h"
#include <arrow/ipc/writer.h>
#include <arrow/ipc/reader.h>
#include <arrow/api.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/dictionary.h>
#include <arrow/result.h>
#include <arrow/compute/function.h>
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/function_internal.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/registry.h"
#include <arrow/acero/options.h>
#include <arrow/acero/exec_plan.h>
#include <arrow/visit_data_inline.h>


int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace baikaldb {
void build_arrow_table(std::shared_ptr<arrow::Table>* table) {
    std::vector<std::shared_ptr<arrow::Array>> array_list;
    {
        std::unique_ptr<arrow::ArrayBuilder> builder = nullptr;
        arrow::MakeBuilder(arrow::default_memory_pool(), arrow::uint32(), &builder);
        dynamic_cast<arrow::UInt32Builder*>((builder).get())->Append(0);
        dynamic_cast<arrow::UInt32Builder*>((builder).get())->Append(1);
        dynamic_cast<arrow::UInt32Builder*>((builder).get())->Append(0);
        dynamic_cast<arrow::UInt32Builder*>((builder).get())->Append(1);
        dynamic_cast<arrow::UInt32Builder*>((builder).get())->Append(0);
        std::shared_ptr<arrow::Array> a;
        builder->Finish(&a);
        array_list.emplace_back(a);
    }
    {
        std::unique_ptr<arrow::ArrayBuilder> builder = nullptr;
        arrow::MakeBuilder(arrow::default_memory_pool(), arrow::uint32(), &builder);
        dynamic_cast<arrow::UInt32Builder*>((builder).get())->Append(841665);
        dynamic_cast<arrow::UInt32Builder*>((builder).get())->Append(841665);
        dynamic_cast<arrow::UInt32Builder*>((builder).get())->Append(841665);
        dynamic_cast<arrow::UInt32Builder*>((builder).get())->Append(841665);
        dynamic_cast<arrow::UInt32Builder*>((builder).get())->Append(841665);
        std::shared_ptr<arrow::Array> a;
        builder->Finish(&a);
        array_list.emplace_back(a);
    }
    {
        std::unique_ptr<arrow::ArrayBuilder> builder = nullptr;
        arrow::MakeBuilder(arrow::default_memory_pool(), arrow::uint64(), &builder);
        dynamic_cast<arrow::UInt64Builder*>((builder).get())->Append(134384483009);
        dynamic_cast<arrow::UInt64Builder*>((builder).get())->Append(100507578);
        dynamic_cast<arrow::UInt64Builder*>((builder).get())->Append(19687499137);
        dynamic_cast<arrow::UInt64Builder*>((builder).get())->Append(19687499137);
        dynamic_cast<arrow::UInt64Builder*>((builder).get())->Append(100507578);
        std::shared_ptr<arrow::Array> a;
        builder->Finish(&a);
        array_list.emplace_back(a);
    }
    {
        std::unique_ptr<arrow::ArrayBuilder> builder = nullptr;
        arrow::MakeBuilder(arrow::default_memory_pool(), arrow::uint32(), &builder);
        dynamic_cast<arrow::UInt32Builder*>((builder).get())->Append(1381619);
        dynamic_cast<arrow::UInt32Builder*>((builder).get())->Append(1381619);
        dynamic_cast<arrow::UInt32Builder*>((builder).get())->Append(1381619);
        dynamic_cast<arrow::UInt32Builder*>((builder).get())->Append(1381619);
        dynamic_cast<arrow::UInt32Builder*>((builder).get())->Append(1381619);
        std::shared_ptr<arrow::Array> a;
        builder->Finish(&a);
        array_list.emplace_back(a);
    }
    {
        std::unique_ptr<arrow::ArrayBuilder> builder = nullptr;
        arrow::MakeBuilder(arrow::default_memory_pool(), arrow::uint64(), &builder);
        dynamic_cast<arrow::UInt64Builder*>((builder).get())->Append(920714);
        dynamic_cast<arrow::UInt64Builder*>((builder).get())->Append(920714);
        dynamic_cast<arrow::UInt64Builder*>((builder).get())->Append(920714);
        dynamic_cast<arrow::UInt64Builder*>((builder).get())->Append(920714);
        dynamic_cast<arrow::UInt64Builder*>((builder).get())->Append(920714);
        std::shared_ptr<arrow::Array> a;
        builder->Finish(&a);
        array_list.emplace_back(a);
    }
    {
        std::unique_ptr<arrow::ArrayBuilder> builder = nullptr;
        arrow::MakeBuilder(arrow::default_memory_pool(), arrow::uint64(), &builder);
        dynamic_cast<arrow::UInt64Builder*>((builder).get())->Append(105320959);
        dynamic_cast<arrow::UInt64Builder*>((builder).get())->Append(105320959);
        dynamic_cast<arrow::UInt64Builder*>((builder).get())->Append(105320959);
        dynamic_cast<arrow::UInt64Builder*>((builder).get())->Append(105320959);
        dynamic_cast<arrow::UInt64Builder*>((builder).get())->Append(105320959);
        std::shared_ptr<arrow::Array> a;
        builder->Finish(&a);
        array_list.emplace_back(a);
    }
    {
        std::unique_ptr<arrow::ArrayBuilder> builder = nullptr;
        arrow::MakeBuilder(arrow::default_memory_pool(), arrow::int64(), &builder);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(1);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(1);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(1);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(1);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(1);
        std::shared_ptr<arrow::Array> a;
        builder->Finish(&a);
        array_list.emplace_back(a);
    }
    {
        std::unique_ptr<arrow::ArrayBuilder> builder = nullptr;
        arrow::MakeBuilder(arrow::default_memory_pool(), arrow::int64(), &builder);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(0);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(0);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(0);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(0);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(0);
        std::shared_ptr<arrow::Array> a;
        builder->Finish(&a);
        array_list.emplace_back(a);
    }
    {
        std::unique_ptr<arrow::ArrayBuilder> builder = nullptr;
        arrow::MakeBuilder(arrow::default_memory_pool(), arrow::int64(), &builder);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(0);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(0);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(0);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(0);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(0);
        std::shared_ptr<arrow::Array> a;
        builder->Finish(&a);
        array_list.emplace_back(a);
    }
    {
        std::unique_ptr<arrow::ArrayBuilder> builder = nullptr;
        arrow::MakeBuilder(arrow::default_memory_pool(), arrow::uint32(), &builder);
        dynamic_cast<arrow::UInt32Builder*>((builder).get())->Append(1);
        dynamic_cast<arrow::UInt32Builder*>((builder).get())->Append(1);
        dynamic_cast<arrow::UInt32Builder*>((builder).get())->Append(1);
        dynamic_cast<arrow::UInt32Builder*>((builder).get())->Append(1);
        dynamic_cast<arrow::UInt32Builder*>((builder).get())->Append(1);
        std::shared_ptr<arrow::Array> a;
        builder->Finish(&a);
        array_list.emplace_back(a);
    }
    {
        std::unique_ptr<arrow::ArrayBuilder> builder = nullptr;
        arrow::MakeBuilder(arrow::default_memory_pool(), arrow::int64(), &builder);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(2);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(5);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(6);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(1);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(8);
        std::shared_ptr<arrow::Array> a;
        builder->Finish(&a);
        array_list.emplace_back(a);
    }
    {
        std::unique_ptr<arrow::ArrayBuilder> builder = nullptr;
        arrow::MakeBuilder(arrow::default_memory_pool(), arrow::int64(), &builder);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(0);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(0);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(0);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(0);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(2);
        std::shared_ptr<arrow::Array> a;
        builder->Finish(&a);
        array_list.emplace_back(a);
    }
    {
        std::unique_ptr<arrow::ArrayBuilder> builder = nullptr;
        arrow::MakeBuilder(arrow::default_memory_pool(), arrow::int64(), &builder);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(0);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(0);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(0);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(0);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(7664);
        std::shared_ptr<arrow::Array> a;
        builder->Finish(&a);
        array_list.emplace_back(a);
    }
    {
        std::unique_ptr<arrow::ArrayBuilder> builder = nullptr;
        arrow::MakeBuilder(arrow::default_memory_pool(), arrow::int64(), &builder);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(101001);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(101002);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(101003);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(101004);
        dynamic_cast<arrow::Int64Builder*>((builder).get())->Append(101005);
        std::shared_ptr<arrow::Array> a;
        builder->Finish(&a);
        array_list.emplace_back(a);
    }
    std::vector<std::shared_ptr<arrow::Field>> fields {
            arrow::field("0_1", arrow::uint32()),
            arrow::field("0_9", arrow::uint32()),
            arrow::field("0_2", arrow::uint64()),
            arrow::field("0_3", arrow::uint32()),
            arrow::field("0_4", arrow::uint64()),
            arrow::field("0_5", arrow::uint64()),
            arrow::field("0_6", arrow::int64()),
            arrow::field("0_7", arrow::int64()),
            arrow::field("0_8", arrow::int64()),
            arrow::field("0_10", arrow::uint32()),
            arrow::field("1_1", arrow::int64()),
            arrow::field("1_2", arrow::int64()),
            arrow::field("1_3", arrow::int64()),
            arrow::field("prod_line_id_3", arrow::int64())
        };
    auto schema = std::make_shared<arrow::Schema>(fields);
    *table = arrow::Table::Make(schema, array_list, 5);
}


void run_test_run_acero_sync() {
    {
        // data_source -> aggregate -> project(slot_ref)
        std::shared_ptr<arrow::Table> table;
        build_arrow_table(&table);
        std::vector<arrow::compute::Expression> exprs = {
            arrow::compute::field_ref("0_1"),
            arrow::compute::field_ref("0_9"),
            arrow::compute::field_ref("0_2"),
            arrow::compute::field_ref("0_3"),
            arrow::compute::field_ref("0_4"),
            arrow::compute::field_ref("0_5"),
            arrow::compute::field_ref("1_1"),
            arrow::compute::field_ref("1_2"),
            arrow::compute::field_ref("1_3")};
        auto declaration = arrow::acero::Declaration::Sequence({
            {"table_source", arrow::acero::TableSourceNodeOptions(table, 5)},
            {"aggregate",
                        arrow::acero::AggregateNodeOptions{
                                /*aggregates=*/ {{"hash_first", {"0_6"}, "0_6"},
                                                {"hash_first", {"0_7"}, "0_7"},
                                                {"hash_first", {"0_8"}, "0_8"},
                                                {"hash_first", {"0_10"}, "0_10"},
                                                {"hash_sum", {"1_1"}, "1_1"},
                                                {"hash_sum", {"1_2"}, "1_2"},
                                                {"hash_sum", {"1_3"}, "1_3"}},
                                /*keys=*/{{"0_1"},{"0_9"},{"0_2"},{"0_3"},{"0_4"},{"0_5"}}}},
            {"project", arrow::acero::ProjectNodeOptions{exprs}}
        });
        auto result = arrow::acero::DeclarationToTable(std::move(declaration), /*use_threads=*/false);
        std::shared_ptr<arrow::Table> final_table = result.ValueOrDie();
        int last_row = final_table->num_rows(); 
        std::cout << "after Filter : agg row: "  << last_row << std::endl;
        std::cout << "Results : " << final_table->ToString() << std::endl;
    }
}

void run_test_case_when() {
    {
        // data_source -> project(test case_when)
        /*
        *   CASE
                when prod_line_id_3 = '101001' then '汽车垂类-有驾'
                when prod_line_id_3 = '101002' then '品牌华表'
                when prod_line_id_3 = '101003' then '品牌华表-SME'
                when prod_line_id_3 = '101004' then '爱番番'
                when prod_line_id_3 = '101005' then '搜索点击'
            end as prod_line_name_3

            CASE
                when prod_line_id_3 in (101001, 101002) then '品牌'
                when prod_line_id_3 in (101003, 101004) then '闭环电商'
                when prod_line_id_3 in (101005) then '其他'
            end as monitor_product_name
        */ 
        std::shared_ptr<arrow::Table> table;
        build_arrow_table(&table);
        
        auto builder = std::make_shared<arrow::Int64Builder>();
        std::shared_ptr<arrow::Array> array;
        builder->Append(101001);
        builder->Append(101002);
        auto s = builder->Finish(&array);
        arrow::Datum in_args1(array);
        arrow::compute::SetLookupOptions in_args1_opt{in_args1, /*skip_nulls*/true};
        builder->Reset();

        builder->Append(101003);
        builder->Append(101004);
        s = builder->Finish(&array);
        arrow::Datum in_args2(array);
        arrow::compute::SetLookupOptions in_args2_opt{in_args2, /*skip_nulls*/true};
        builder->Reset();

        builder->Append(101005);
        s = builder->Finish(&array);
        arrow::Datum in_args3(array);
        arrow::compute::SetLookupOptions in_args3_opt{in_args3, /*skip_nulls*/true};
        builder->Reset();
    
        std::vector<arrow::compute::Expression> exprs = {
            arrow::compute::field_ref("0_9"),
            arrow::compute::field_ref("0_2"),
            arrow::compute::field_ref("0_3"),
            arrow::compute::field_ref("0_4"),
            arrow::compute::field_ref("0_5"),
            arrow::compute::field_ref("1_1"),
            arrow::compute::field_ref("1_2"),
            arrow::compute::field_ref("1_3"),
            arrow::compute::call("case_when",  {
                arrow::compute::call("make_struct",{arrow::compute::call("equal", {arrow::compute::field_ref("prod_line_id_3"), arrow::compute::literal(101001)}),
                                                    arrow::compute::call("equal", {arrow::compute::field_ref("prod_line_id_3"), arrow::compute::literal(101002)}),
                                                    arrow::compute::call("equal", {arrow::compute::field_ref("prod_line_id_3"), arrow::compute::literal(101003)}),
                                                    arrow::compute::call("equal", {arrow::compute::field_ref("prod_line_id_3"), arrow::compute::literal(101004)}),
                                                    arrow::compute::call("equal", {arrow::compute::field_ref("prod_line_id_3"), arrow::compute::literal(101005)}),
                                                    }),
                arrow::compute::literal("汽车垂类-有驾"), 
                arrow::compute::literal("品牌华表"),
                arrow::compute::literal("品牌华表-SME"),
                arrow::compute::literal("爱番番"),
                arrow::compute::literal("搜索点击")
            }),
            arrow::compute::call("case_when", {
                arrow::compute::call("make_struct",{arrow::compute::call("is_in", {arrow::compute::field_ref("prod_line_id_3")}, in_args1_opt),
                                                    arrow::compute::call("is_in", {arrow::compute::field_ref("prod_line_id_3")}, in_args2_opt),
                                                    arrow::compute::call("is_in", {arrow::compute::field_ref("prod_line_id_3")}, in_args3_opt)
                                                    }),
                arrow::compute::literal("品牌"),
                arrow::compute::literal("闭环电商"),
                arrow::compute::literal("其他")
            })
        };
        std::vector<std::string> names = {"0_9", "0_2", "0_3", "0_4", "0_5", "1_1", "1_2", "1_3", "prod_line_name_3", "monitor_product_name"};
        auto declaration = arrow::acero::Declaration::Sequence({
            {"table_source", arrow::acero::TableSourceNodeOptions(table, 5)},
            {"project", arrow::acero::ProjectNodeOptions{exprs, names}}
        });
        auto result = arrow::acero::DeclarationToTable(std::move(declaration), /*use_threads=*/false);
        std::shared_ptr<arrow::Table> final_table = result.ValueOrDie();
        int last_row = final_table->num_rows(); 
        std::cout << "after Filter : agg row: "  << last_row << std::endl;
        std::cout << "Results : " << final_table->ToString() << std::endl;
    }
}

template<typename T>
struct test_string_cast_same_with_row {
    using CType = typename arrow::TypeTraits<T>::CType;
    using BuilderType = typename arrow::TypeTraits<T>::BuilderType;

    static void test(std::string func_name, std::shared_ptr<arrow::Array>& input_array, std::vector<ExprValue>& row_values) {
        std::cout << "func_name: " << func_name << std::endl;

        std::vector<arrow::Datum> input_vector = {input_array};
        auto res = arrow::compute::CallFunction(func_name, input_vector);
        EXPECT_TRUE(res.ok());

        auto output_data = *res;
        const CType* data = output_data.mutable_array()->GetValues<CType>(1);
        for (int64_t i = 0; i < output_data.length(); ++i) {
            CType expect = row_values[i].get_numberic<CType>();
            CType actual = *(data + i);
            std::cout << "row: " << i << ", str: " << row_values[i].str_val << ", expect: " << expect << ", actual: " << actual << std::endl;
            ASSERT_EQ(actual, expect);
        }
    }
};

struct test_string_cast_same_with_row_bool_type {
    static void test(std::string func_name, std::shared_ptr<arrow::Array>& input_array, std::vector<ExprValue>& row_values) {
        std::cout << "func_name: " << func_name << std::endl;

        std::vector<arrow::Datum> input_vector = {input_array};
        auto res = arrow::compute::CallFunction(func_name, input_vector);
        EXPECT_TRUE(res.ok());

        auto output_data = *res;
        const uint8_t* bitmap_buffer = output_data.mutable_array()->buffers[1]->data();
        for (int64_t i = 0; i < output_data.length(); ++i) {
            bool expect = row_values[i].get_numberic<bool>();
            bool actual = arrow::bit_util::GetBit(bitmap_buffer, i);
            std::cout << "row: " << i << ", str: " << row_values[i].str_val << ", expect: " << expect << ", actual: " << actual << std::endl;
            ASSERT_EQ(actual, expect);
        }
    }
};

void run_test_string_cast_numberic() {
    auto input = std::make_shared<arrow::LargeBinaryBuilder>();
    std::vector<ExprValue> row_values;
    std::vector<std::string> test_strs = {
        "",
        "+",
        "-",
        "100",
        "00101",
        "+102",
        "-103",
        "104abc",
        "-105abc",
        "+106abc",
        "  101003",
        "  +101004",
        "  -101004",
        "+a1005",
        "-a1006",
        " +1007.999",
        "-1008.111",
        "255", // uint8_max
        "256", // uint8_max + 1
        "65535", // uint16_max
        "65536", // uint16_max + 1
        "4294967295", // uint32_max
        "4294967296", // uint32_max + 1
        "18446744073709551615", // uint64_max
        "18446744073709551616", // uint64_max + 1
        "127",  // int8_max
        "+127",  // int8_max
        "128",  // int8_max + 1
        "+128",  // int8_max + 1
        "-128", // int8_min
        "-129", // int8_min - 1
        "32767",  // int16_max
        "+32767",  // int16_max
        "32768",  // int16_max + 1
        "+32768",  // int16_max + 1
        "-32768", // int16_min
        "-32769", // int16_min - 1
        "2147483647",  // int32_max
        "+2147483647",  // int32_max
        "2147483648",  // int32_max + 1
        "+2147483648",  // int32_max + 1
        "-2147483648", // int32_min
        "-2147483649", // int32_min - 1
        "9223372036854775807",  // int64_max
        "+9223372036854775807",  // int64_max
        "9223372036854775808",  // int64_max + 1
        "+9223372036854775808",  // int64_max + 1
        "-9223372036854775808", // int64_min
        "-9223372036854775809", // int64_min - 1
        "null",
        " +null"
    };
    for (auto& str : test_strs) {
        input->Append(str);

        ExprValue expr_val;
        expr_val.type = pb::STRING;
        expr_val.str_val = str;
        row_values.emplace_back(expr_val);
    }
    std::shared_ptr<arrow::Array> input_array;
    auto s = input->Finish(&input_array);

    test_string_cast_same_with_row<arrow::UInt8Type>::test("string_cast_uint8", input_array, row_values);
    test_string_cast_same_with_row<arrow::UInt16Type>::test("string_cast_uint16", input_array, row_values);
    test_string_cast_same_with_row<arrow::UInt32Type>::test("string_cast_uint32", input_array, row_values);
    test_string_cast_same_with_row<arrow::UInt64Type>::test("string_cast_uint64", input_array, row_values);
    test_string_cast_same_with_row<arrow::Int8Type>::test("string_cast_int8", input_array, row_values);
    test_string_cast_same_with_row<arrow::Int16Type>::test("string_cast_int16", input_array, row_values);
    test_string_cast_same_with_row<arrow::Int32Type>::test("string_cast_int32", input_array, row_values);
    test_string_cast_same_with_row<arrow::Int64Type>::test("string_cast_int64", input_array, row_values);
    test_string_cast_same_with_row<arrow::FloatType>::test("string_cast_float", input_array, row_values);
    test_string_cast_same_with_row<arrow::DoubleType>::test("string_cast_double", input_array, row_values);
    test_string_cast_same_with_row_bool_type::test("string_cast_bool", input_array, row_values);   
}

TEST(test_arrow_vector_execute, case_all) {
    run_test_run_acero_sync();
    run_test_case_when();
}   

TEST(test_string_cast_numberic, case_all) {
    EXPECT_TRUE(baikaldb::ArrowFunctionManager::instance()->RegisterAllArrowFunction() == 0);
    run_test_string_cast_numberic();
}   
}  // namespace baikal
