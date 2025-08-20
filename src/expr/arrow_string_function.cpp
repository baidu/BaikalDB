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
int arrow_concat(std::vector<ExprNode*>& children, pb::Function* fn, 
                    const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    std::vector<arrow::compute::Expression> args;
    for (auto& c : children) { 
        if (0 != build_arrow_expr_with_cast(c, pb::STRING)) {
            return -1;
        }
        args.emplace_back(c->arrow_expr());
    }
    // 连接符放在最后
    args.emplace_back(arrow::compute::literal(std::make_shared<arrow::LargeBinaryScalar>("")));
    /// A null in any input results in a null in the output. binary_join_element_wise要求args类型都一样, large_binary
    std::shared_ptr<arrow::compute::JoinOptions> option = std::make_shared<arrow::compute::JoinOptions>();
    out = arrow::compute::call("binary_join_element_wise", args, option);
    return 0;
}

int arrow_concat_ws(std::vector<ExprNode*>& children, pb::Function* fn, 
                    const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.size() < 2);
    std::vector<arrow::compute::Expression> args;
    int pos = 0;
    for (auto& c : children) { 
        if (0 != build_arrow_expr_with_cast(c, pb::STRING)) {
            return -1;
        }
        if (pos != 0) {
            args.emplace_back(c->arrow_expr());
        }
        pos++;
    }
    // 连接符放在最后
    args.emplace_back(children[0]->arrow_expr());
    /// A null in any input results in a null in the output. binary_join_element_wise要求args类型都一样, large_binary
    auto option = std::make_shared<arrow::compute::JoinOptions>(arrow::compute::JoinOptions::NullHandlingBehavior::SKIP);
    out = arrow::compute::call("binary_join_element_wise", args, option);
    return 0;
}

int arrow_reverse(std::vector<ExprNode*>& children, pb::Function* fn, 
                    const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.size() != 1);
    if (0 != build_arrow_expr_with_cast(children[0], pb::STRING)) {
        return -1;
    }
    out = arrow::compute::call("binary_reverse", {children[0]->arrow_expr()});
    return 0;
}

int arrow_length(std::vector<ExprNode*>& children, pb::Function* fn, 
                    const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.empty());
    if (0 != build_arrow_expr_with_cast(children[0], pb::STRING)) {
        return -1;
    }
    out = arrow::compute::call("binary_length", {children[0]->arrow_expr()});
    return 0;
}

int arrow_bit_length(std::vector<ExprNode*>& children, pb::Function* fn, 
                    const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.empty());
    if (0 != build_arrow_expr_with_cast(children[0], pb::STRING)) {
        return -1;
    }
    out = arrow::compute::call("multiply_checked", {
            arrow::compute::call("binary_length", {children[0]->arrow_expr()}),
            arrow::literal(8)});
    return 0;
}

int arrow_substr(std::vector<ExprNode*>& children, pb::Function* fn, 
                    const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.size() < 2); 
    int64_t start = children[1]->get_value(nullptr).get_numberic<int64_t>();
    int64_t stop = std::numeric_limits<int64_t>::max();
    if (start > 0) {
        // mysql是从1开始，而arrow是从0开始
        start -= 1;
    } 
    if (children.size() > 2) {
        int64_t len = children[2]->get_value(nullptr).get_numberic<int64_t>();
        stop = start + len;
        if (len < 0) {
            stop = start;
        }
    }
    if (0 != build_arrow_expr_with_cast(children[0], pb::STRING)) {
        return -1;
    }
    out = arrow::compute::call("binary_slice", {children[0]->arrow_expr()}, arrow::compute::SliceOptions(start, stop));
    return 0;
}

int arrow_upper(std::vector<ExprNode*>& children, pb::Function* fn, 
                    const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.empty());
    if (0 != build_arrow_expr_with_cast(children[0], pb::STRING)) {
        return -1;
    }
    out = arrow::compute::call("baikal_upper", {children[0]->arrow_expr()});
    return 0;
}

int arrow_lower(std::vector<ExprNode*>& children, pb::Function* fn, 
                    const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.empty());
    if (0 != build_arrow_expr_with_cast(children[0], pb::STRING)) {
        return -1;
    }
    out = arrow::compute::call("baikal_lower", {children[0]->arrow_expr()});
    return 0;
}

int arrow_repeat(std::vector<ExprNode*>& children, pb::Function* fn, 
                    const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.size() != 2);
    int times = children[1]->get_value(nullptr).get_numberic<int64_t>();
    RETURN_NULL_IF_COLUMN_SATISFY_COND(times <= 0);
    if (0 != build_arrow_expr_with_cast(children[0], pb::STRING)) {
        return -1;
    }
    if (0 != build_arrow_expr_with_cast(children[1], pb::INT32)) {
        return -1;
    }
    out = arrow::compute::call("binary_repeat", 
            {children[0]->arrow_expr(), children[1]->arrow_expr()});
    return 0;
}

template <typename O, typename I>
struct UpperTransfer {
    using BuilderType = typename arrow::TypeTraits<O>::BuilderType;
    static arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch, arrow::compute::ExecResult* out) {
        BuilderType builder;
        const arrow::ArraySpan& input = batch[0].array;
        RETURN_NOT_OK(arrow::compute::internal::VisitArrayValuesInline<I>(
        input,
        [&](std::string_view v) {
            std::string s(v.data(), v.length());
            std::transform(s.begin(), s.end(), s.begin(), ::toupper); 
            return builder.Append(s);
        },
        [&]() {
            return builder.AppendNull();
        }));
        std::shared_ptr<arrow::Array> output_array;
        RETURN_NOT_OK(builder.Finish(&output_array));
        out->value = std::move(output_array->data());
        return arrow::Status::OK();
    }
};

template <typename O, typename I>
struct LowerTransfer {
    using BuilderType = typename arrow::TypeTraits<O>::BuilderType;
    static arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch, arrow::compute::ExecResult* out) {
        BuilderType builder;
        const arrow::ArraySpan& input = batch[0].array;
        RETURN_NOT_OK(arrow::compute::internal::VisitArrayValuesInline<I>(
        input,
        [&](std::string_view v) {
            std::string s(v.data(), v.length());
            std::transform(s.begin(), s.end(), s.begin(), ::tolower); 
            return builder.Append(s);
        },
        [&]() {
            return builder.AppendNull();
        }));
        std::shared_ptr<arrow::Array> output_array;
        RETURN_NOT_OK(builder.Finish(&output_array));
        out->value = std::move(output_array->data());
        return arrow::Status::OK();
    }
};

arrow::Status ArrowFunctionManager::RegisterAllStringFunction() {
    auto registry = arrow::compute::GetFunctionRegistry();
    {
        auto upper = std::make_shared<arrow::compute::ScalarFunction>("baikal_upper", arrow::compute::Arity::Unary(),
                                                    /*doc=*/arrow::compute::FunctionDoc::Empty());
        for (const std::shared_ptr<arrow::DataType>& in_ty : arrow::BaseBinaryTypes()) {
            ARROW_RETURN_NOT_OK(
                upper->AddKernel({in_ty}, arrow::large_binary(),
                        arrow::compute::internal::GenerateVarBinaryToVarBinary<UpperTransfer, arrow::LargeBinaryType>(*in_ty)));
        }
        ARROW_RETURN_NOT_OK(registry->AddFunction(upper));
    }
    {
        auto lower = std::make_shared<arrow::compute::ScalarFunction>("baikal_lower", arrow::compute::Arity::Unary(),
                                                    /*doc=*/arrow::compute::FunctionDoc::Empty());
        for (const std::shared_ptr<arrow::DataType>& in_ty : arrow::BaseBinaryTypes()) {
            ARROW_RETURN_NOT_OK(
                lower->AddKernel({in_ty}, arrow::large_binary(),
                        arrow::compute::internal::GenerateVarBinaryToVarBinary<LowerTransfer, arrow::LargeBinaryType>(*in_ty)));
        }
        ARROW_RETURN_NOT_OK(registry->AddFunction(lower));
    }
    return arrow::Status::OK();
}
}
