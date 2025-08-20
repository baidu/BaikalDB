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

#include "datetime.h"
#include "internal_functions.h"
#include "arrow_function.h"
#include <arrow/visit_data_inline.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/array/builder_binary.h>
#include <arrow/compute/registry.h>
#include <arrow/compute/cast.h>
#include <arrow/compute/kernels/codegen_internal.h>
#include "slot_ref.h"

namespace baikaldb {
static const int32_t DATE_FORMAT_LENGTH = 128;

/*
 *  通用config
 */
class CommonTimeOptionsType : public arrow::compute::FunctionOptionsType {
public:
    static const arrow::compute::FunctionOptionsType* GetInstance() {
        static std::unique_ptr<arrow::compute::FunctionOptionsType> instance(new CommonTimeOptionsType());
        return instance.get();
    }
    const char* type_name() const override { return "CommonTimeOptionType"; }
    std::string Stringify(const  arrow::compute::FunctionOptions& options) const override {
        return type_name();
    }
    bool Compare(const arrow::compute::FunctionOptions& options,
                 const arrow::compute::FunctionOptions& other) const override {
        const auto& lop = static_cast<const CommonTimeFunctionOptions&>(options);
        const auto& rop = static_cast<const CommonTimeFunctionOptions&>(other);
        return lop.str_value == rop.str_value && lop.int_value == rop.int_value;;
    }
    std::unique_ptr<arrow::compute::FunctionOptions> Copy(const arrow::compute::FunctionOptions& options) const override {
        const auto& opts = static_cast<const CommonTimeFunctionOptions&>(options);
        return std::make_unique<CommonTimeFunctionOptions>(opts.str_value, opts.int_value);
    }
};

struct CommonTimeState : public arrow::compute::KernelState {
    std::string str_value;
    int64_t int_value = 0;
    CommonTimeState(const std::string& conf, int64_t value) : str_value(conf), int_value(value) {}
};

arrow::Result<std::unique_ptr<arrow::compute::KernelState>> InitCommonTimeState(arrow::compute::KernelContext*,
                                            const  arrow::compute::KernelInitArgs& args) {
    auto func_options = static_cast<const CommonTimeFunctionOptions*>(args.options);
    if (func_options == nullptr) {
        return std::make_unique<CommonTimeState>("", 0);
    }
    return std::make_unique<CommonTimeState>(func_options->str_value, func_options->int_value);
}

CommonTimeFunctionOptions::CommonTimeFunctionOptions(const std::string& value, int64_t int_value)
    : arrow::compute::FunctionOptions(CommonTimeOptionsType::GetInstance()), str_value(value), int_value(int_value) {}


/*
 *  以下三个实际上不会走到, 直接转成常量表达式了
 */
int arrow_current_date(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    ExprValue res = curdate(std::vector<ExprValue>{});
    if (return_type != pb::INVALID_TYPE) {
        res.cast_to(return_type);
    }
    if (0 != res.get_vectorized_scalar_expression(out)) {
        return -1;
    }
    return 0;
}

int arrow_current_time(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    ExprValue res = curtime(std::vector<ExprValue>{});
    if (return_type != pb::INVALID_TYPE) {
        res.cast_to(return_type);
    }
    if (0 != res.get_vectorized_scalar_expression(out)) {
        return -1;
    }
    return 0;
}

int arrow_now(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    int32_t precision = 0;
    if (children.size() > 0) {
        precision = children[0]->get_value(nullptr).get_numberic<int>();
    }
    ExprValue res = ExprValue::Now(precision);
    if (return_type != pb::INVALID_TYPE) {
        res.cast_to(return_type);
    }
    if (0 != res.get_vectorized_scalar_expression(out)) {
        return -1;
    }
    return 0;
}

/*
 *  行func转换为arrow表达式
 */
int arrow_str_to_date(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.size() != 2);
    return arrow_cast_to_datetime(children, fn, return_type, out);
}

int arrow_date_format(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.size() != 2);
    if (0 != build_arrow_expr_with_cast(children[0], pb::TIMESTAMP)) {
        return -1;
    }
    CommonTimeFunctionOptions option(children[1]->get_value(nullptr).get_string());
    out = arrow::compute::call("baikal_date_format", {children[0]->arrow_expr()}, std::move(option));
    return 0;
}

int arrow_time_format(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.size() != 2);
    if (0 != build_arrow_expr_with_cast(children[0], pb::TIME)) {
        return -1;
    }
    CommonTimeFunctionOptions option(children[1]->get_value(nullptr).get_string());
    out = arrow::compute::call("baikal_time_format", {children[0]->arrow_expr()}, std::move(option));
    return 0;
}

int arrow_date(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.empty());
    if (children[0]->col_type() == pb::INT64) {
        // cast_to(pb::STRING).cast_to(pb::DATETIME).cast_to(pb::DATE);
        if (0 != build_arrow_expr_with_cast(children[0], pb::STRING)) {
            return -1;
        }
        ExprValueCastFunctionOptions option(pb::STRING);
        out = arrow::compute::call("expr_value_to_date", {children[0]->arrow_expr()}, std::move(option));
        return 0;
    }  
    // cast_to(pb::DATETIME).cast_to(pb::DATE);
    if (0 != build_arrow_expr_with_cast(children[0], pb::DATETIME, true)) {
        return -1;
    }
    ExprValueCastFunctionOptions option(pb::DATETIME);
    out = arrow::compute::call("expr_value_to_date", {children[0]->arrow_expr()}, std::move(option));
    return 0;
}

int arrow_hour(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.empty());
    if (0 != build_arrow_expr_with_cast(children[0], pb::TIME)) {
        return -1;
    }
    out = arrow::compute::call("baikal_hour", {children[0]->arrow_expr()});
    return 0;
}

int arrow_date_sub(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.size() < 3);
    if (!is_numberic(children[0]->col_type())) {
        // cast_to(pb::TIMESTAMP);
        if (0 != build_arrow_expr_with_cast(children[0], pb::TIMESTAMP, true)) {
            return -1;
        }
    } else {
        // cast_to(pb::STRING).cast_to(pb::TIMESTAMP);
        if (0 != build_arrow_expr_with_cast(children[0], pb::STRING)) {
            return -1;
        }
        ExprValueCastFunctionOptions option(pb::STRING);
        auto expr = arrow::compute::call("expr_value_to_timestamp", {children[0]->arrow_expr()}, std::move(option));
        children[0]->set_arrow_expr(expr);
    }
    int32_t interval = children[1]->get_value(nullptr).get_numberic<int32_t>();
    std::string unit = children[2]->get_value(nullptr).get_string();
    CommonTimeFunctionOptions option(unit, interval);
    out = arrow::compute::call("baikal_date_sub", {children[0]->arrow_expr()}, std::move(option));
    return 0;
}

int arrow_date_add(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.size() < 3);
    if (!is_numberic(children[0]->col_type())) {
        // cast_to(pb::TIMESTAMP);
        if (0 != build_arrow_expr_with_cast(children[0], pb::TIMESTAMP, true)) {
            return -1;
        }
    } else {
        // cast_to(pb::STRING).cast_to(pb::TIMESTAMP);
        if (0 != build_arrow_expr_with_cast(children[0], pb::STRING)) {
            return -1;
        }
        ExprValueCastFunctionOptions option(pb::STRING);
        auto expr = arrow::compute::call("expr_value_to_timestamp", {children[0]->arrow_expr()}, std::move(option));
        children[0]->set_arrow_expr(expr);
    }
    int32_t interval = children[1]->get_value(nullptr).get_numberic<int32_t>();
    std::string unit = children[2]->get_value(nullptr).get_string();
    CommonTimeFunctionOptions option(unit, interval);
    out = arrow::compute::call("baikal_date_add", {children[0]->arrow_expr()}, std::move(option));
    return 0;
}

int arrow_time_to_sec(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.size() == 0);
    if (0 != build_arrow_expr_with_cast(children[0], pb::TIME, true)) {
        return -1;
    }
    out = arrow::compute::call("baikal_time_to_sec", {children[0]->arrow_expr()});
    return 0;
}

int arrow_timestamp(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.size() == 0 || children.size() > 2);
    if (0 != build_arrow_expr_with_cast(children[0], pb::DATETIME, true)) {
        return -1;
    }
    // datetime to timestamp
    ExprValueCastFunctionOptions option(pb::DATETIME);
    auto expr = arrow::compute::call("expr_value_to_timestamp", {children[0]->arrow_expr()}, std::move(option));
    
    if (children.size() == 2) {
        arrow::compute::Expression time_to_sec_expr;
        std::vector<ExprNode*> input = {children[1]};
        if (0 != arrow_time_to_sec(input, nullptr, pb::INT32, time_to_sec_expr)) {
            return -1;
        }
        out = arrow::compute::call("add", {expr, time_to_sec_expr});
        return 0;
    }
    out = expr;
    return 0;
}

int arrow_unix_timestamp(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    if (children.size() == 0) {
        uint32_t now = time(NULL);
        out = arrow::compute::literal(now);
        return 0;
    }
    if (children[0]->col_type() == pb::INT64) {
        // cast_to(pb::STRING).cast_to(pb::TIMESTAMP)
        if (0 != build_arrow_expr_with_cast(children[0], pb::STRING)) {
            return -1;
        }
        ExprValueCastFunctionOptions option(pb::STRING);
        out = arrow::compute::call("expr_value_to_timestamp", {children[0]->arrow_expr()}, std::move(option));
        return 0;
    }
    if (0 != build_arrow_expr_with_cast(children[0], pb::TIMESTAMP, true)) {
        return -1;
    }
    out = children[0]->arrow_expr();
    return 0;
}

int arrow_from_unixtime(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    // TODO: string column like "0a" will cast fail and 10004
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.size() == 0);
    if (0 != build_arrow_expr_with_cast(children[0], pb::UINT32)) {
        return -1;
    }
    out = children[0]->arrow_expr();
    return 0;
}

int arrow_week(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.empty());
    if (0 != build_arrow_expr_with_cast(children[0], pb::DATETIME)) {
        return -1;
    }
    int32_t mode = -1;
    if (children.size() > 1) {
        mode = children[1]->get_value(nullptr).get_numberic<uint32_t>() % 8;
    }
    CommonTimeFunctionOptions option("", mode);
    out = arrow::compute::call("baikal_week", {children[0]->arrow_expr()}, std::move(option));
    return 0;
}

int arrow_yearweek(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.empty());
    if (0 != build_arrow_expr_with_cast(children[0], pb::DATETIME)) {
        return -1;
    }
    int32_t mode = -1;
    if (children.size() > 1) {
        mode = children[1]->get_value(nullptr).get_numberic<uint32_t>() % 8;
    }
    CommonTimeFunctionOptions option("", mode);
    out = arrow::compute::call("baikal_yearweek", {children[0]->arrow_expr()}, std::move(option));
    return 0;
}

int arrow_timestampdiff(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.size() < 3);

    const std::string& unit = children[0]->get_value(nullptr).get_string();
    if (0 != build_arrow_expr_with_cast(children[1], pb::TIMESTAMP)) {
        return -1;
    }
    if (0 != build_arrow_expr_with_cast(children[2], pb::TIMESTAMP)) {
        return -1;
    }
    out = arrow::compute::call("subtract_checked", {arrow_cast(children[2]->arrow_expr(), pb::TIMESTAMP, pb::INT64),
                                            arrow_cast(children[1]->arrow_expr(), pb::TIMESTAMP, pb::INT64)});
    if (unit == "minute") {
        out = arrow::compute::call("divide_checked", {out, arrow::compute::literal(60)});
    } else if (unit == "hour") {
        out = arrow::compute::call("divide_checked", {out, arrow::compute::literal(3600)});
    } else if (unit == "day") {
        out = arrow::compute::call("divide_checked", {out, arrow::compute::literal(24 * 3600)});
    } else if (unit != "second") {
        RETURN_NULL();
    }
    return 0;
}
/*
 * date_format arrow实现
 */
template <typename O, typename I>
struct ExecDateFormat {
    using InputValueCType = typename arrow::TypeTraits<I>::CType;
    using BuilderType = typename arrow::TypeTraits<O>::BuilderType;

    static arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch, arrow::compute::ExecResult* out) {
        CommonTimeState* state = static_cast<CommonTimeState*>(ctx->state());
        std::string format = state->str_value;
        BuilderType builder;
        const arrow::ArraySpan& input = batch[0].array;
        RETURN_NOT_OK(arrow::VisitArraySpanInline<I>(
            input,
            [&](InputValueCType v) {
                // 核心
                struct tm t_result;
                time_t t = (uint32_t)v;
                localtime_r(&t, &t_result);
                char s[DATE_FORMAT_LENGTH];
                date_format_internal(s, sizeof(s), format.data(), &t_result);
                return builder.Append(std::string(s));
            },
            [&]() { return builder.AppendNull(); }));
        
        std::shared_ptr<arrow::Array> output_array;
        RETURN_NOT_OK(builder.Finish(&output_array));
        out->value = std::move(output_array->data());
        return arrow::Status::OK();
    }
};

/*
 * time_format arrow实现
 */
template <typename O, typename I>
struct ExecTimeFormat {
    using InputValueCType = typename arrow::TypeTraits<I>::CType;
    using BuilderType = typename arrow::TypeTraits<O>::BuilderType;

    static arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch, arrow::compute::ExecResult* out) {
        CommonTimeState* state = static_cast<CommonTimeState*>(ctx->state());
        std::string format = state->str_value;
        BuilderType builder;
        const arrow::ArraySpan& input = batch[0].array;
        RETURN_NOT_OK(arrow::VisitArraySpanInline<I>(
            input,
            [&](InputValueCType v) {
                // 核心
                uint32_t second = (uint32_t)v;
                struct tm t_result; 
                t_result.tm_hour = (second >> 12) & 0x3FF;
                t_result.tm_min = (second >> 6) & 0x3F;
                t_result.tm_sec = second & 0x3F;
                char s[DATE_FORMAT_LENGTH];
                date_format_internal(s, sizeof(s), format.data(), &t_result);
                return builder.Append(std::string(s));
            },
            [&]() { return builder.AppendNull(); }));
        
        std::shared_ptr<arrow::Array> output_array;
        RETURN_NOT_OK(builder.Finish(&output_array));
        out->value = std::move(output_array->data());
        return arrow::Status::OK();
    }
};
/*
 * time_hour arrow实现
 */
template <typename O, typename I>
struct ExecHour {
    using InputValueCType = typename arrow::TypeTraits<I>::CType;
    using OutputValueCType = typename arrow::TypeTraits<O>::CType;

    static arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch, arrow::compute::ExecResult* out) {
        CommonTimeState* state = static_cast<CommonTimeState*>(ctx->state());
        std::string format = state->str_value;
        const arrow::ArraySpan& input = batch[0].array;
        arrow::ArraySpan* out_data = out->array_span_mutable();
        const InputValueCType* in_values = input.GetValues<InputValueCType>(1);
        OutputValueCType* out_values = out_data->GetValues<OutputValueCType>(1);
        
        for (auto i = 0; i < input.length; ++i) {
            int32_t v = *in_values++;
            if (v < 0) {
                v = -v;
            }
            *out_values++ = (v >> 12) & 0x3FF;
        }
        return arrow::Status::OK();
    }
};

/*
 * date_sub arrow实现
 */
template <typename O, typename I>
struct ExecDateSub {
    using InputValueCType = typename arrow::TypeTraits<I>::CType;
    using OutputValueCType = typename arrow::TypeTraits<O>::CType;

    static arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch, arrow::compute::ExecResult* out) {
        CommonTimeState* state = static_cast<CommonTimeState*>(ctx->state());
        int32_t interval = state->int_value;
        const std::string& unit = state->str_value;
        const arrow::ArraySpan& input = batch[0].array;
        arrow::ArraySpan* out_data = out->array_span_mutable();
        const InputValueCType* in_values = input.GetValues<InputValueCType>(1);
        OutputValueCType* out_values = out_data->GetValues<OutputValueCType>(1);
        for (auto i = 0; i < input.length; ++i) {
            time_t ts = (uint32_t)(*in_values);
            in_values++;
            if (unit == "second") {
                date_sub_interval(ts, interval, TimeUnit::SECOND);
            } else if (unit == "minute") {
                date_sub_interval(ts, interval, TimeUnit::MINUTE);
            } else if (unit == "hour") {
                date_sub_interval(ts, interval, TimeUnit::HOUR);
            } else if (unit == "day") {
                date_sub_interval(ts, interval, TimeUnit::DAY);
            } else if (unit == "month") {
                date_sub_interval(ts, interval, TimeUnit::MONTH);
            } else if (unit == "year") {
                date_sub_interval(ts, interval, TimeUnit::YEAR);
            } else {
                // un-support
                *out_values++ = OutputValueCType();
                continue;
            }
            *out_values++ = timestamp_to_datetime(ts);
        }
        return arrow::Status::OK();
    }
};

/*
 * date_add arrow实现
 */
template <typename O, typename I>
struct ExecDateAdd {
    using InputValueCType = typename arrow::TypeTraits<I>::CType;
    using OutputValueCType = typename arrow::TypeTraits<O>::CType;

    static arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch, arrow::compute::ExecResult* out) {
        CommonTimeState* state = static_cast<CommonTimeState*>(ctx->state());
        int32_t interval = state->int_value;
        const std::string& unit = state->str_value;
        const arrow::ArraySpan& input = batch[0].array;
        arrow::ArraySpan* out_data = out->array_span_mutable();
        const InputValueCType* in_values = input.GetValues<InputValueCType>(1);
        OutputValueCType* out_values = out_data->GetValues<OutputValueCType>(1);
        for (auto i = 0; i < input.length; ++i) {
            time_t ts = (uint32_t)(*in_values);
            in_values++;
            if (unit == "second") {
                date_add_interval(ts, interval, TimeUnit::SECOND);
            } else if (unit == "minute") {
                date_add_interval(ts, interval, TimeUnit::MINUTE);
            } else if (unit == "hour") {
                date_add_interval(ts, interval, TimeUnit::HOUR);
            } else if (unit == "day") {
                date_add_interval(ts, interval, TimeUnit::DAY);
            } else if (unit == "month") {
                date_add_interval(ts, interval, TimeUnit::MONTH);
            } else if (unit == "year") {
                date_add_interval(ts, interval, TimeUnit::YEAR);
            } else {
                // un-support
                *out_values++ = OutputValueCType();
                continue;
            }
            *out_values++ = timestamp_to_datetime(ts);
        }
        return arrow::Status::OK();
    }
};

/*
 * time_to_sec arrow实现
 */
template <typename O, typename I>
struct ExecTimeToSec {
    using InputValueCType = typename arrow::TypeTraits<I>::CType;
    using OutputValueCType = typename arrow::TypeTraits<O>::CType;

    static arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch, arrow::compute::ExecResult* out) {
        const arrow::ArraySpan& input = batch[0].array;
        arrow::ArraySpan* out_data = out->array_span_mutable();
        const InputValueCType* in_values = input.GetValues<InputValueCType>(1);
        OutputValueCType* out_values = out_data->GetValues<OutputValueCType>(1);
        for (auto i = 0; i < input.length; ++i) {
            time_t time = (int32_t)*in_values;
            in_values++;
            bool minus = false;
            if (time < 0) {
                minus = true;
                time = -time;
            }
            uint32_t hour = (time >> 12) & 0x3FF;
            uint32_t min = (time >> 6) & 0x3F;
            uint32_t sec = time & 0x3F;
            uint32_t sec_sum = hour * 3600 + min * 60 + sec;
            if (!minus) {
                *out_values++ = sec_sum;
            } else {
                *out_values++ = -sec_sum;
            }
        }
        return arrow::Status::OK();
    }
};

/*
 * week arrow实现
 */
template <typename O, typename I>
struct ExecWeek {
    using InputValueCType = typename arrow::TypeTraits<I>::CType;
    using OutputValueCType = typename arrow::TypeTraits<O>::CType;

    static arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch, arrow::compute::ExecResult* out) {
        CommonTimeState* state = static_cast<CommonTimeState*>(ctx->state());
        int64_t mode = state->int_value;
        int year = 0;
        int weeks = 0;
        const arrow::ArraySpan& input = batch[0].array;
        const InputValueCType* in_values = input.GetValues<InputValueCType>(1);
        arrow::ArraySpan* out_data = out->array_span_mutable();
        OutputValueCType* out_values = out_data->GetValues<OutputValueCType>(1);
        uint8_t* null_bitmap = out_data->buffers[0].data;
        if (null_bitmap == nullptr) {
            return arrow::Status::IOError("output bitmap is null");
        }
        for (auto i = 0; i < input.length; ++i) {
            if (!input.IsValid(i)) {
                arrow::bit_util::ClearBit(null_bitmap, out_data->offset + i);
            } else {
                if (0 == calc_week(*in_values, mode, /*is_yearweek=*/false, year, weeks)) {
                    *out_values = weeks;
                    arrow::bit_util::SetBit(null_bitmap, out_data->offset + i);
                } else {
                    arrow::bit_util::ClearBit(null_bitmap, out_data->offset + i);                
                }
            }
            in_values++;
            out_values++;
        }
        return arrow::Status::OK();
    }
};

/*
 * yearweek arrow实现
 */
template <typename O, typename I>
struct ExecYearWeek {
    using InputValueCType = typename arrow::TypeTraits<I>::CType;
    using OutputValueCType = typename arrow::TypeTraits<O>::CType;

    static arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch, arrow::compute::ExecResult* out) {
        CommonTimeState* state = static_cast<CommonTimeState*>(ctx->state());
        int64_t mode = state->int_value;
        int year = 0;
        int weeks = 0;
        const arrow::ArraySpan& input = batch[0].array;
        const InputValueCType* in_values = input.GetValues<InputValueCType>(1);
        arrow::ArraySpan* out_data = out->array_span_mutable();
        OutputValueCType* out_values = out_data->GetValues<OutputValueCType>(1);
        uint8_t* null_bitmap = out_data->buffers[0].data;
        if (null_bitmap == nullptr) {
            return arrow::Status::IOError("output bitmap is null");
        }
        for (auto i = 0; i < input.length; ++i) {
            if (!input.IsValid(i)) {
                arrow::bit_util::ClearBit(null_bitmap, out_data->offset + i);
            } else {
                if (0 == calc_week(*in_values, mode, /*is_yearweek=*/true, year, weeks)) {
                    *out_values = year * 100 + weeks;
                    arrow::bit_util::SetBit(null_bitmap, out_data->offset + i);
                } else {
                    arrow::bit_util::ClearBit(null_bitmap, out_data->offset + i);                
                }
            }
            in_values++;
            out_values++;
        }
        return arrow::Status::OK();
    }
};

arrow::Status ArrowFunctionManager::RegisterAllTimeFunction() {
    auto registry = arrow::compute::GetFunctionRegistry();
    /*
     * date_format
     */
    {
        auto arrow_date_format = std::make_shared<arrow::compute::ScalarFunction>("baikal_date_format", arrow::compute::Arity::Unary(),
                                                    /*doc=*/arrow::compute::FunctionDoc::Empty());
        for (const std::shared_ptr<arrow::DataType>& in_ty : arrow::NumericTypes()) {
            ARROW_RETURN_NOT_OK(
                arrow_date_format->AddKernel({in_ty}, arrow::large_binary(),
                        arrow::compute::internal::GenerateNumeric<ExecDateFormat, arrow::LargeBinaryType>(*in_ty),
                        InitCommonTimeState));
        }
        ARROW_RETURN_NOT_OK(registry->AddFunction(arrow_date_format));
    }
    /*
     * time_format
     */
    {
        auto arrow_time_format = std::make_shared<arrow::compute::ScalarFunction>("baikal_time_format", arrow::compute::Arity::Unary(),
                                                    /*doc=*/arrow::compute::FunctionDoc::Empty());
        for (const std::shared_ptr<arrow::DataType>& in_ty : arrow::NumericTypes()) {
            ARROW_RETURN_NOT_OK(
                arrow_time_format->AddKernel({in_ty}, arrow::large_binary(),
                        arrow::compute::internal::GenerateNumeric<ExecTimeFormat, arrow::LargeBinaryType>(*in_ty),
                        InitCommonTimeState));
        }
        ARROW_RETURN_NOT_OK(registry->AddFunction(arrow_time_format));
    }
    /*
     * hour
     */
    {
        auto arrow_hour = std::make_shared<arrow::compute::ScalarFunction>("baikal_hour", arrow::compute::Arity::Unary(),
                                                    /*doc=*/arrow::compute::FunctionDoc::Empty());
        for (const std::shared_ptr<arrow::DataType>& in_ty : arrow::NumericTypes()) {
            ARROW_RETURN_NOT_OK(
                arrow_hour->AddKernel({in_ty}, arrow::uint32(),
                        arrow::compute::internal::GenerateNumeric<ExecHour, arrow::UInt32Type>(*in_ty),
                        InitCommonTimeState));
        }
        ARROW_RETURN_NOT_OK(registry->AddFunction(arrow_hour));
    }
    /*
     * date_sub
     */
    {
        auto arrow_date_sub = std::make_shared<arrow::compute::ScalarFunction>("baikal_date_sub", arrow::compute::Arity::Unary(),
                                                    /*doc=*/arrow::compute::FunctionDoc::Empty());
        for (const std::shared_ptr<arrow::DataType>& in_ty : arrow::NumericTypes()) {
            ARROW_RETURN_NOT_OK(
                arrow_date_sub->AddKernel({in_ty}, arrow::uint64(),
                        arrow::compute::internal::GenerateNumeric<ExecDateSub, arrow::UInt64Type>(*in_ty),
                        InitCommonTimeState));
        }
        ARROW_RETURN_NOT_OK(registry->AddFunction(arrow_date_sub));
    }
    /*
     * date_add
     */
    {
        auto arrow_date_add = std::make_shared<arrow::compute::ScalarFunction>("baikal_date_add", arrow::compute::Arity::Unary(),
                                                    /*doc=*/arrow::compute::FunctionDoc::Empty());
        for (const std::shared_ptr<arrow::DataType>& in_ty : arrow::NumericTypes()) {
            ARROW_RETURN_NOT_OK(
                arrow_date_add->AddKernel({in_ty}, arrow::uint64(),
                        arrow::compute::internal::GenerateNumeric<ExecDateAdd, arrow::UInt64Type>(*in_ty),
                        InitCommonTimeState));
        }
        ARROW_RETURN_NOT_OK(registry->AddFunction(arrow_date_add));
    }
    /*
     * time_to_sec
     */
    {
        auto time_to_sec = std::make_shared<arrow::compute::ScalarFunction>("baikal_time_to_sec", arrow::compute::Arity::Unary(),
                                                    /*doc=*/arrow::compute::FunctionDoc::Empty());
        for (const std::shared_ptr<arrow::DataType>& in_ty : arrow::NumericTypes()) {
            ARROW_RETURN_NOT_OK(
                time_to_sec->AddKernel({in_ty}, arrow::uint32(),
                        arrow::compute::internal::GenerateNumeric<ExecTimeToSec, arrow::UInt32Type>(*in_ty),
                        InitCommonTimeState));
        }
        ARROW_RETURN_NOT_OK(registry->AddFunction(time_to_sec));
    }
    /*
     * week
     */
    {
        auto week_func = std::make_shared<arrow::compute::ScalarFunction>("baikal_week", arrow::compute::Arity::Unary(),
                                                    /*doc=*/arrow::compute::FunctionDoc::Empty());
        for (const std::shared_ptr<arrow::DataType>& in_ty : arrow::NumericTypes()) {
            arrow::compute::ScalarKernel kernel({in_ty}, arrow::uint32(),
                        arrow::compute::internal::GenerateNumeric<ExecWeek, arrow::UInt32Type>(*in_ty),
                        InitCommonTimeState);
            /// Kernel expects a pre-allocated buffer to write the result bitmap
            /// into. The preallocated memory is not zeroed (except for the last byte),
            /// so the kernel should ensure to completely populate the bitmap.
            kernel.null_handling = arrow::compute::NullHandling::COMPUTED_PREALLOCATE;
            kernel.mem_allocation = arrow::compute::MemAllocation::PREALLOCATE;
            ARROW_RETURN_NOT_OK(week_func->AddKernel(kernel));
        }
        ARROW_RETURN_NOT_OK(registry->AddFunction(week_func));
    }
    /*
     * yearweek
     */
    {
        auto yearweek_func = std::make_shared<arrow::compute::ScalarFunction>("baikal_yearweek", arrow::compute::Arity::Unary(),
                                                    /*doc=*/arrow::compute::FunctionDoc::Empty());
        for (const std::shared_ptr<arrow::DataType>& in_ty : arrow::NumericTypes()) {
            arrow::compute::ScalarKernel kernel({in_ty}, arrow::uint32(),
                        arrow::compute::internal::GenerateNumeric<ExecYearWeek, arrow::UInt32Type>(*in_ty),
                        InitCommonTimeState);
            kernel.null_handling = arrow::compute::NullHandling::COMPUTED_PREALLOCATE;
            kernel.mem_allocation = arrow::compute::MemAllocation::PREALLOCATE;
            ARROW_RETURN_NOT_OK(yearweek_func->AddKernel(kernel));
        }
        ARROW_RETURN_NOT_OK(registry->AddFunction(yearweek_func));
    }
    return arrow::Status::OK();
}
}
