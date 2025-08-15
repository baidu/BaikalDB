// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
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

#pragma once
#include "common.h"
#include "expr_value.h"
#include "object_manager.h"
#include "expr_node.h"
#include <arrow/compute/api_aggregate.h>
#include <arrow/acero/options.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/api.h> 
#include <arrow/compute/cast.h>

namespace baikaldb {

#define BUILD_ARROW_EXPR_RET(c) \
    if (0 != c->transfer_to_arrow_expression()) {  \
        return -1;   \
    }

#define RETURN_NULL() \
    { \
        arrow::NullScalar null_scalar;     \
        out = arrow::compute::literal(null_scalar); \
        return 0;  \
    }

#define RETURN_NULL_IF_COLUMN_SATISFY_COND(COND) \
    if (COND) {            \
        RETURN_NULL() \
        return 0;  \
    }

// argments, argments_types, return arrow_expression
using ArrowExprBuildFun = std::function<int(std::vector<ExprNode*>&, pb::Function*, const pb::PrimitiveType&, arrow::compute::Expression&)>; 

/*
 * 对应internal funtion里能直接翻译的算子
 */ 
bool is_same_type(const pb::PrimitiveType& type1, const pb::PrimitiveType& type2, bool force_same = false);
bool check_row_expr_is_support(pb::Function& fn, ExprNode* node);
int build_arrow_expr_with_cast(ExprNode* node, pb::Function* fn, int pos);
int build_arrow_expr_with_cast(ExprNode* node, const pb::PrimitiveType& col_type, bool force_same = false);
arrow::compute::Expression arrow_cast(const arrow::compute::Expression& expr, const pb::PrimitiveType& type, const pb::PrimitiveType& cast_type);
int get_all_arrow_argments(std::vector<ExprNode*>& children, std::vector<arrow::compute::Expression>& arguments, pb::Function* fn);
int get_all_arrow_argments_for_add_minus_multiple(std::vector<ExprNode*>& children, std::vector<arrow::compute::Expression>& arguments);
// 算数
int arrow_uminus(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_add(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_minus(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_multiplies(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_divides(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_ceil(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_floor(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_round(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
// row expr比较
void build_row_expr_range_compare_expression(std::vector<ExprNode*>& children, const std::string& op, const std::string& last_param_op, arrow::compute::Expression& out);
int arrow_row_expr_eq(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_row_expr_ne(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_row_expr_ge(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_row_expr_gt(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_row_expr_le(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_row_expr_lt(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
// 比较
int arrow_eq(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_ne(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_ge(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_gt(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_le(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_lt(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_least(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_greatest(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
// 选择
int arrow_case_when(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_case_expr_when(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_if(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_if_null(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
// string
int arrow_concat(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_concat_ws(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_reverse(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_length(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_bit_length(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_substr(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_upper(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_lower(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_repeat(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
// 类型转换
int arrow_cast_to_string(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_cast_to_date(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_cast_to_datetime(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_cast_to_time(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_cast_to_double(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_cast_to_signed(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_cast_to_unsigned(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
// 位运算
int arrow_bitwise_and(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_bitwise_or(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_bitwise_xor(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_left_shift(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_right_shift(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_bitwise_not(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
// 其他
int arrow_murmur_hash(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
// 时间函数
int arrow_str_to_date(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_date_format(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_time_format(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_now(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_date(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_hour(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_date_sub(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_date_add(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_current_date(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_current_time(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_timestamp(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_time_to_sec(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_from_unixtime(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_unix_timestamp(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_week(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_yearweek(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);
int arrow_timestampdiff(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out);

/*
 * 注册使用的FunctionOptions
 */
class ExprValueCastFunctionOptions : public arrow::compute::FunctionOptions {
public:
    explicit ExprValueCastFunctionOptions(pb::PrimitiveType value);
    pb::PrimitiveType type;
};

struct ExprValueCastState : public arrow::compute::KernelState {
    pb::PrimitiveType type;
    explicit ExprValueCastState(pb::PrimitiveType type) : type(type) {}
};

class CommonTimeFunctionOptions : public arrow::compute::FunctionOptions {
public:
    CommonTimeFunctionOptions(const std::string& str_value, int64_t int_value = 0);
    std::string str_value;
    int64_t int_value = 0;
};

class ArrowFunctionManager : public ObjectManager<
                        ArrowExprBuildFun, 
                        ArrowFunctionManager> {
public:
    int RegisterAllArrowFunction();
    arrow::Status RegisterAllInteralFunction();
    arrow::Status RegisterAllDefinedFunction();
    arrow::Status RegisterAllTimeFunction();
    arrow::Status RegisterAllStringFunction();
    arrow::Status RegisterAllHashAggFunction();
    ArrowExprBuildFun get_func(int32_t func_op, const std::string& func_name, bool is_row_expr = false);
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
