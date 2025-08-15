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
#include "arrow_function.h"
#include <arrow/visit_data_inline.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/array/builder_binary.h>
#include <arrow/compute/registry.h>
#include <arrow/compute/cast.h>
#include <arrow/compute/kernels/codegen_internal.h>
#include "slot_ref.h"
#include "row_expr.h"

namespace baikaldb {
DEFINE_bool(enable_arrow_complex_func, false, "enable_arrow_complex_func");

const std::unordered_map<pb::PrimitiveType, std::shared_ptr<arrow::DataType>> cast_types = {
    {pb::BOOL, arrow::boolean()},

    {pb::INT8, arrow::int32()},
    {pb::INT16, arrow::int32()},
    {pb::INT32, arrow::int32()},
    {pb::TIME, arrow::int32()},

    {pb::INT64, arrow::int64()},

    {pb::UINT8, arrow::uint32()},
    {pb::UINT16, arrow::uint32()},
    {pb::UINT32, arrow::uint32()},
    {pb::TIMESTAMP, arrow::uint32()},
    {pb::DATE, arrow::uint32()},

    {pb::UINT64, arrow::uint64()},
    {pb::DATETIME, arrow::uint64()},

    {pb::FLOAT, arrow::float32()},
    {pb::DOUBLE, arrow::float64()},

    {pb::STRING, arrow::large_binary()},
    {pb::HLL, arrow::large_binary()},
    {pb::HEX, arrow::large_binary()},
    {pb::TDIGEST, arrow::large_binary()},
    {pb::BITMAP, arrow::large_binary()}
};

const std::set<pb::PrimitiveType> arrow_int_types = {
    pb::INT8,
    pb::INT16,
    pb::INT32,
    pb::INT64,

    pb::UINT8,
    pb::UINT16, 
    pb::UINT32,
    pb::UINT64,

    pb::TIME,
    pb::TIMESTAMP,
    pb::DATE,
    pb::DATETIME
};

bool check_row_expr_is_support(pb::Function& fn, ExprNode* node) {
    if (fn.fn_op() == parser::FT_EQ
        || fn.fn_op() == parser::FT_NE
        || fn.fn_op() == parser::FT_GE
        || fn.fn_op() == parser::FT_GT
        || fn.fn_op() == parser::FT_LE
        || fn.fn_op() == parser::FT_LT
        || fn.fn_op() == parser::FT_IN) {
        return static_cast<RowExpr*>(node)->can_use_arrow_vector_for_compare_sclar_exrpr();
    }
    return false;
}

bool is_same_type(const pb::PrimitiveType& type1, const pb::PrimitiveType& type2, bool force_same) {
    if (type1 == type2) {
        return true;
    }
    if (force_same) {
        return false;
    }
    if (arrow_int_types.count(type1) > 0 && arrow_int_types.count(type2) > 0) {
        return true;
    }
    if (is_double(type1) && is_double(type2)) {
        return true;
    }
    if (is_string(type1) && is_string(type2)) {
        return true;
    }
    return false;
}
/*
 * Transfer to arrow inline compute Function
 * args需要cast成fn对应的类型, 
 * 如case when的value类型需要cast成col_type
 * 保持和行一致
 * plan cache模式下的col type可能不对
 */ 
int build_arrow_expr_with_cast(ExprNode* node, pb::Function* fn, int pos) {
    if (fn == nullptr || pos >= fn->arg_types_size()) {
        BUILD_ARROW_EXPR_RET(node);
        return 0;
    }
    pb::PrimitiveType col_type = fn->arg_types(pos);
    return build_arrow_expr_with_cast(node, col_type);
}

// force_same: 强制类型一致
int build_arrow_expr_with_cast(ExprNode* node, const pb::PrimitiveType& col_type, bool force_same) {
    // if (node->is_literal() && node->col_type() != pb::NULL_TYPE) {
    //     // literal内部会先转coltype再build arrow expression
    //     // arrow nullScalar有类型, 需要cast
    //     node->set_col_type(col_type);
    //     BUILD_ARROW_EXPR_RET(node);
    //     return 0;
    // }
    if (node->node_type() == pb::HEX_LITERAL) {
        node->set_col_type(col_type);
        BUILD_ARROW_EXPR_RET(node);
        return 0;
    }
    BUILD_ARROW_EXPR_RET(node);
    if (node->col_type() != col_type 
            && (is_datetime_specic(node->col_type()) || node->col_type() == pb::STRING) 
            && (is_datetime_specic(col_type) || col_type == pb::STRING)) {
        arrow::Expression cast_expr = arrow_cast(node->arrow_expr(), node->col_type(), col_type);
        node->set_arrow_expr(cast_expr);
        return 0;
    }
    if ((node->is_slot_ref() || node->is_literal()) && is_same_type(node->col_type(), col_type, force_same)) {
        return 0;
    }
    // cast  
    if (node->node_type() == pb::ROW_EXPR) {
        return 0;
    }
    // 基础类型直接用arrow cast
    auto iter = cast_types.find(col_type);
    if (iter == cast_types.end()) {
        return 0;
    }
    std::shared_ptr<arrow::DataType> arrow_type = iter->second;
    if (node->arrow_expr().type() != nullptr && node->arrow_expr().type()->id() == arrow_type->id()) {
        return 0;
    }
    arrow::Expression cast_expr = arrow::compute::call("cast", {node->arrow_expr()}, arrow::compute::CastOptions::Unsafe(arrow_type));
    node->set_arrow_expr(cast_expr);
    return 0;
}

arrow::compute::Expression arrow_cast(const arrow::compute::Expression& expr, const pb::PrimitiveType& type, const pb::PrimitiveType& cast_type) {
    arrow::compute::Expression cast_expr = expr;
    if ((is_datetime_specic(type) || type == pb::STRING) && 
            (is_datetime_specic(cast_type) || cast_type == pb::STRING)) {
        std::string cast_func_name;
        switch (cast_type) {
            case pb::DATE:
                cast_func_name = "expr_value_to_date";
                break;
            case pb::DATETIME:
                cast_func_name = "expr_value_to_datetime";
                break;
            case pb::TIME:
                cast_func_name = "expr_value_to_time";
                break;
            case pb::TIMESTAMP:
                cast_func_name = "expr_value_to_timestamp";
                break;
            default:
                cast_func_name = "expr_value_to_string";
                break;
        }
        ExprValueCastFunctionOptions option(type);
        cast_expr = arrow::compute::call(cast_func_name, {expr}, std::move(option));
    } else {
        auto iter = cast_types.find(cast_type);
        if (iter == cast_types.end()) {
            return cast_expr;
        }
        std::shared_ptr<arrow::DataType> arrow_type = iter->second;
        cast_expr = arrow::compute::call("cast", {expr}, arrow::compute::CastOptions::Unsafe(arrow_type));
    }
    return cast_expr;
}

int get_all_arrow_argments(std::vector<ExprNode*>& children, std::vector<arrow::compute::Expression>& arguments, pb::Function* fn) {
    for (int i = 0; i < children.size(); ++i) {
        if (0 != build_arrow_expr_with_cast(children[i], fn, i)) {
            return -1;
        }
        arguments.emplace_back(children[i]->arrow_expr());
    }
    return 0;
}

// parser::FT_UMINUS:
int arrow_uminus(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    std::vector<arrow::compute::Expression> arguments;
    if (0 != get_all_arrow_argments(children, arguments, nullptr)) {
        return -1;
    }
    out = arrow::compute::call("negate", arguments);
    return 0;
}

int get_all_arrow_argments_for_add_minus_multiple(std::vector<ExprNode*>& children, std::vector<arrow::compute::Expression>& arguments) {
    std::vector<pb::PrimitiveType> args_types;
    for (auto& c : children) {
        args_types.emplace_back(c->col_type());
    }
    for (auto& c : children) {
        BUILD_ARROW_EXPR_RET(c);
        if (has_double(args_types)) {
            arguments.emplace_back(arrow::compute::call("cast", {c->arrow_expr()}, arrow::compute::CastOptions::Unsafe(arrow::float64())));
        } else if (has_uint(args_types)) {
            arguments.emplace_back(arrow::compute::call("cast", {c->arrow_expr()}, arrow::compute::CastOptions::Unsafe(arrow::uint64())));
        } else {
            arguments.emplace_back(arrow::compute::call("cast", {c->arrow_expr()}, arrow::compute::CastOptions::Unsafe(arrow::int64())));
        }
    }
    return 0;
}

// parser::FT_ADD:
int arrow_add(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    std::vector<arrow::compute::Expression> arguments;
    if (0 != get_all_arrow_argments_for_add_minus_multiple(children, arguments)) {
        return -1;
    }
    out = arrow::compute::call("add_checked", arguments);
    return 0;
}

// case parser::FT_MINUS:
int arrow_minus(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    std::vector<arrow::compute::Expression> arguments;
    if (0 != get_all_arrow_argments_for_add_minus_multiple(children, arguments)) {
        return -1;
    }
    out = arrow::compute::call("subtract_checked", arguments);
    return 0;
}

// case parser::FT_MULTIPLIES:
int arrow_multiplies(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    std::vector<arrow::compute::Expression> arguments;
    if (0 != get_all_arrow_argments_for_add_minus_multiple(children, arguments)) {
        return -1;
    }
    out = arrow::compute::call("multiply_checked", arguments);
    return 0;
}

// case parser::FT_DIVIDES:
int arrow_divides(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    for (int i = 0; i < children.size(); ++i) {
        if (0 != build_arrow_expr_with_cast(children[i], nullptr, i)) {
            return -1;
        }
    }
    // 需要特殊处理除数是0, 返回NULL
    arrow::Datum null_datum = std::make_shared<arrow::DoubleScalar>();
    out = arrow::compute::call("divide_checked", {
        arrow::compute::call("cast", {children[0]->arrow_expr()}, arrow::compute::CastOptions::Unsafe(arrow::float64())),
        arrow::compute::call("if_else", {
                arrow::compute::call("equal", {children[1]->arrow_expr(), arrow::compute::literal(0)}), 
                arrow::compute::literal(null_datum), 
                arrow::compute::call("cast", {children[1]->arrow_expr()}, arrow::compute::CastOptions::Unsafe(arrow::float64()))
        })
    });
    return 0;
}

int arrow_ceil(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.empty());
    if (0 != build_arrow_expr_with_cast(children[0], pb::DOUBLE)) {
        return -1;
    }
    out = arrow::compute::call("ceil", {children[0]->arrow_expr()});
    return 0;
}

int arrow_floor(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.empty());
    if (0 != build_arrow_expr_with_cast(children[0], pb::DOUBLE)) {
        return -1;
    }
    out = arrow::compute::call("floor", {children[0]->arrow_expr()});
    return 0;
}

int arrow_round(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.empty());
    if (0 != build_arrow_expr_with_cast(children[0], pb::DOUBLE)) {
        return -1;
    }
    int bits = 0;
    if (children.size() > 1) {
        bits = children[1]->get_value(nullptr).get_numberic<int>();
    }
    // arrow内置round精度不太好处理
    arrow::compute::RoundOptions options(bits, arrow::compute::RoundMode::HALF_TOWARDS_INFINITY);
    out = arrow::compute::call("arrow_round", {children[0]->arrow_expr(), arrow::compute::literal(bits)}, options);
    return 0;
}

void build_row_expr_range_compare_expression(std::vector<ExprNode*>& children, const std::string& op, const std::string& last_param_op, arrow::compute::Expression& out) {
    // example: (a,b,c,d)>=(1,2,3,4) =>
    // (a>1) || (a=1)&&(b>2) || (a=1,b=2)&&(c>3) || (a=1,b=2,c=3)&&(d>=4)
    std::vector<arrow::compute::Expression> sub_exprs;
    arrow::compute::Expression equal_expr;
    for (size_t i = 0; i < children[0]->children_size(); i++) {
        const arrow::compute::Expression& left = children[0]->children(i)->arrow_expr();
        const arrow::compute::Expression& right = children[1]->children(i)->arrow_expr();
        if (i == 0) {
            sub_exprs.emplace_back(arrow::compute::call(op, {left, right}));
            equal_expr = arrow::compute::call("equal", {left, right});
            continue;
        } else if (i == children[0]->children_size() - 1) {
            sub_exprs.emplace_back(arrow::compute::and_(equal_expr, arrow::compute::call(last_param_op, {left, right})));
            break;
        } else {
            sub_exprs.emplace_back(arrow::compute::and_(equal_expr, arrow::compute::call(op, {left, right})));
            equal_expr = arrow::compute::and_(equal_expr, arrow::compute::call("equal", {left, right}));
            continue;
        }
        equal_expr = arrow::compute::call("equal", {left, right});
    }
    out = arrow::compute::or_(sub_exprs);
}

int arrow_row_expr_eq(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    for (int i = 0; i < children.size(); ++i) {
        if (0 != build_arrow_expr_with_cast(children[i], fn, i)) {
            return -1;
        }
    }
    std::vector<arrow::compute::Expression> sub_exprs;
    for (size_t i = 0; i < children[0]->children_size(); i++) {
        const arrow::compute::Expression& left = children[0]->children(i)->arrow_expr();
        const arrow::compute::Expression& right = children[1]->children(i)->arrow_expr();
        sub_exprs.emplace_back(arrow::compute::call("equal", {left, right}));
    }
    out = arrow::compute::and_(sub_exprs);
    return 0;
}

int arrow_row_expr_ne(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    for (int i = 0; i < children.size(); ++i) {
        if (0 != build_arrow_expr_with_cast(children[i], fn, i)) {
            return -1;
        }
    }
    std::vector<arrow::compute::Expression> sub_exprs;
    for (size_t i = 0; i < children[0]->children_size(); i++) {
        const arrow::compute::Expression& left = children[0]->children(i)->arrow_expr();
        const arrow::compute::Expression& right = children[1]->children(i)->arrow_expr();
        sub_exprs.emplace_back(arrow::compute::call("not_equal", {left, right}));
    }
    out = arrow::compute::or_(sub_exprs);
    return 0;
}

int arrow_row_expr_ge(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    for (int i = 0; i < children.size(); ++i) {
        if (0 != build_arrow_expr_with_cast(children[i], fn, i)) {
            return -1;
        }
    }
    build_row_expr_range_compare_expression(children, "greater", "greater_equal", out);
    return 0;
}

int arrow_row_expr_gt(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    for (int i = 0; i < children.size(); ++i) {
        if (0 != build_arrow_expr_with_cast(children[i], fn, i)) {
            return -1;
        }
    }
    build_row_expr_range_compare_expression(children, "greater", "greater", out);
    return 0;
}

int arrow_row_expr_le(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    for (int i = 0; i < children.size(); ++i) {
        if (0 != build_arrow_expr_with_cast(children[i], fn, i)) {
            return -1;
        }
    }
    build_row_expr_range_compare_expression(children, "less", "less_equal", out);
    return 0;
}

int arrow_row_expr_lt(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    for (int i = 0; i < children.size(); ++i) {
        if (0 != build_arrow_expr_with_cast(children[i], fn, i)) {
            return -1;
        }
    }
    build_row_expr_range_compare_expression(children, "less", "less", out);
    return 0;
}

// parser::FT_EQ:
int arrow_eq(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    std::vector<arrow::compute::Expression> arguments;
    if (0 != get_all_arrow_argments(children, arguments, fn)) {
        return -1;
    }
    out = arrow::compute::call("equal", arguments);
    return 0;
}

// parser::FT_NE: 
int arrow_ne(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    std::vector<arrow::compute::Expression> arguments;
    if (0 != get_all_arrow_argments(children, arguments, fn)) {
        return -1;
    }
    out = arrow::compute::call("not_equal", arguments);
    return 0;
}

// parser::FT_GE: 
int arrow_ge(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    std::vector<arrow::compute::Expression> arguments;
    if (0 != get_all_arrow_argments(children, arguments, fn)) {
        return -1;
    }
    out = arrow::compute::call("greater_equal", arguments);
    return 0;
}

// parser::FT_GT: 
int arrow_gt(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    std::vector<arrow::compute::Expression> arguments;
    if (0 != get_all_arrow_argments(children, arguments, fn)) {
        return -1;
    }
    out = arrow::compute::call("greater", arguments);
    return 0;
}

// parser::FT_LE: 
int arrow_le(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    std::vector<arrow::compute::Expression> arguments;
    if (0 != get_all_arrow_argments(children, arguments, fn)) {
        return -1;
    }
    out = arrow::compute::call("less_equal", arguments);
    return 0;
}

// parser::FT_LT: 
int arrow_lt(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    std::vector<arrow::compute::Expression> arguments;
    if (0 != get_all_arrow_argments(children, arguments, fn)) {
        return -1;
    }
    out = arrow::compute::call("less", arguments);
    return 0;
}

int arrow_least(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    std::vector<arrow::compute::Expression> arguments;
    if (0 != get_all_arrow_argments(children, arguments, fn)) {
        return -1;
    }
    for (auto i = 0; i < arguments.size(); ++i) {
        if (is_string(children[i]->col_type())) {
            arguments[i] = arrow::compute::call("cast", {arguments[i]}, arrow::compute::CastOptions::Unsafe(arrow::float64()));
        }
    }
    arrow::compute::ElementWiseAggregateOptions options(/*skip_nulls*/false);
    out = arrow::compute::call("min_element_wise", arguments, std::move(options));
    return 0;
}

int arrow_greatest(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    std::vector<arrow::compute::Expression> arguments;
    if (0 != get_all_arrow_argments(children, arguments, fn)) {
        return -1;
    }
    for (auto i = 0; i < arguments.size(); ++i) {
        if (is_string(children[i]->col_type())) {
            arguments[i] = arrow::compute::call("cast", {arguments[i]}, arrow::compute::CastOptions::Unsafe(arrow::float64()));
        }
    }
    arrow::compute::ElementWiseAggregateOptions options(/*skip_nulls*/false);
    out = arrow::compute::call("max_element_wise", arguments, std::move(options));
    return 0;
}

int arrow_bitwise_and(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    std::vector<arrow::compute::Expression> arguments;
    if (0 != get_all_arrow_argments(children, arguments, fn)) {
        return -1;
    }
    out = arrow::compute::call("bit_wise_and", arguments);
    return 0;
}

int arrow_bitwise_or(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    std::vector<arrow::compute::Expression> arguments;
    if (0 != get_all_arrow_argments(children, arguments, fn)) {
        return -1;
    }
    out = arrow::compute::call("bit_wise_or", arguments);
    return 0;
}

int arrow_bitwise_xor(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    std::vector<arrow::compute::Expression> arguments;
    if (0 != get_all_arrow_argments(children, arguments, fn)) {
        return -1;
    }
    out = arrow::compute::call("bit_wise_xor", arguments);
    return 0;
}

int arrow_bitwise_not(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    std::vector<arrow::compute::Expression> arguments;
    if (0 != get_all_arrow_argments(children, arguments, fn)) {
        return -1;
    }
    out = arrow::compute::call("bit_wise_not", arguments);
    return 0;
}

int arrow_left_shift(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    std::vector<arrow::compute::Expression> arguments;
    if (0 != get_all_arrow_argments(children, arguments, fn)) {
        return -1;
    }
    out = arrow::compute::call("shift_left", arguments);
    return 0;
}

int arrow_right_shift(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    std::vector<arrow::compute::Expression> arguments;
    if (0 != get_all_arrow_argments(children, arguments, fn)) {
        return -1;
    }
    out = arrow::compute::call("shift_right", arguments);
    return 0;
}

int arrow_if_null(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.size() != 2);
    // values, cast to return type
    for (int i = 0; i < children.size(); ++i) {
        if (0 != build_arrow_expr_with_cast(children[i], return_type)) {
            return -1;
        }
    }
    out = arrow::compute::call("if_else", {arrow::compute::call("is_null", {children[0]->arrow_expr()}),
        children[1]->arrow_expr(),
        children[0]->arrow_expr()});
    return 0;
}

// parser::FT_COMMON  case_when
int arrow_case_when(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    std::vector<arrow::compute::Expression> predicates;
    std::vector<arrow::compute::Expression> case_when_arguments;
    // predicate
    for (size_t i = 0; i < children.size() / 2; ++i) {
        int predicate_idx = i * 2;
        if (0 != build_arrow_expr_with_cast(children[predicate_idx], fn, predicate_idx)) {
            return -1;
        }
        predicates.emplace_back(children[predicate_idx]->arrow_expr());
    }
    case_when_arguments.emplace_back(arrow::compute::call("make_struct", predicates));
    // values, 所有value都cast为return_type
    for (size_t i = 0; i < children.size() / 2; ++i) {
        int value_idx = i * 2 + 1;
        if (0 != build_arrow_expr_with_cast(children[value_idx], return_type)) {
            return -1;
        }
        case_when_arguments.emplace_back(children[value_idx]->arrow_expr());
    }
    //没有else分支, 返回null
    if (children.size() % 2 != 0) {
        int value_idx = children.size() - 1;
        if (0 != build_arrow_expr_with_cast(children[value_idx], return_type)) {
            return -1;
        }
        case_when_arguments.emplace_back(children[value_idx]->arrow_expr());
    }
    out = arrow::compute::call("case_when",  case_when_arguments);
    return 0;
}

// parser::FT_COMMON  case_expr_when
int arrow_case_expr_when(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    std::vector<arrow::compute::Expression> predicates;
    std::vector<arrow::compute::Expression> case_when_arguments;
    // slot ref
    if (0 != build_arrow_expr_with_cast(children[0], fn, 0)) {
        return -1;
    }
    // predicate values
    for (size_t i = 0; i < (children.size() - 1) / 2; ++i) {
        int predicate_value_idx = i * 2 + 1;
        // 类型不一致, 将when值转换为列类型(可能会有坑)
        if (0 != build_arrow_expr_with_cast(children[predicate_value_idx], children[0]->col_type())) {
            return -1;
        }
        predicates.emplace_back(arrow::compute::call("equal", {children[0]->arrow_expr(), children[predicate_value_idx]->arrow_expr()}));
    }
    case_when_arguments.emplace_back(arrow::compute::call("make_struct", predicates));
    // values, cast to return type
    for (size_t i = 0; i < (children.size() - 1) / 2; ++i) {
        int value_idx = i * 2 + 2;
        if (0 != build_arrow_expr_with_cast(children[value_idx], return_type)) {
            return -1;
        }
        case_when_arguments.emplace_back(children[value_idx]->arrow_expr());
    }
    //没有else分支, 返回null
    if ((children.size() - 1) % 2 != 0) {
        int value_idx = children.size() - 1;
        if (0 != build_arrow_expr_with_cast(children[value_idx], return_type)) {
            return -1;
        }
        case_when_arguments.emplace_back(children[value_idx]->arrow_expr());
    }
    out = arrow::compute::call("case_when",  case_when_arguments);
    return 0;
}

int arrow_if(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.size() != 3);
    // predicate
    if (0 != build_arrow_expr_with_cast(children[0], fn, 0)) {
        return -1;
    }
    // values, cast to return type
    for (int i = 1; i < children.size(); ++i) {
        if (0 != build_arrow_expr_with_cast(children[i], return_type)) {
            return -1;
        }
    }
    out = arrow::compute::call("if_else", {children[0]->arrow_expr(), 
            children[1]->arrow_expr(),
            children[2]->arrow_expr()});
    return 0;
}

int arrow_cast_to_string(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    BUILD_ARROW_EXPR_RET(children[0]);
    if (is_datetime_specic(children[0]->col_type()) 
            || is_double(children[0]->col_type())) {
        ExprValueCastFunctionOptions option(children[0]->col_type());
        out = arrow::compute::call("expr_value_to_string", {children[0]->arrow_expr()}, std::move(option));
    } else if (is_string(children[0]->col_type())) {
        out = children[0]->arrow_expr();
    } else {
        out = arrow::compute::call("cast", {children[0]->arrow_expr()}, arrow::compute::CastOptions::Unsafe(arrow::large_binary()));
    }
    return 0;
}

int arrow_cast_to_double(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.size() != 1);
    if (0 != build_arrow_expr_with_cast(children[0], pb::DOUBLE, true)) {
        return -1;
    }
    out = children[0]->arrow_expr();
    return 0;
}

int arrow_cast_to_signed(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.size() != 1);
    if (0 != build_arrow_expr_with_cast(children[0], pb::INT64, true)) {
        return -1;
    }
    out = children[0]->arrow_expr();
    return 0;
}

int arrow_cast_to_unsigned(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    RETURN_NULL_IF_COLUMN_SATISFY_COND(children.size() != 1);
    if (0 != build_arrow_expr_with_cast(children[0], pb::UINT64, true)) {
        return -1;
    }
    out = children[0]->arrow_expr();
    return 0;
}

int arrow_cast_to_date(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    BUILD_ARROW_EXPR_RET(children[0]);
    if (children[0]->col_type() == pb::STRING 
        || children[0]->col_type() == pb::DATETIME
        || children[0]->col_type() == pb::TIMESTAMP 
        || children[0]->col_type() == pb::TIME) {
        ExprValueCastFunctionOptions option(children[0]->col_type());
        out = arrow::compute::call("expr_value_to_date", {children[0]->arrow_expr()}, std::move(option));
    } else {
        out = children[0]->arrow_expr();
    }
    return 0;
}

int arrow_cast_to_datetime(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    BUILD_ARROW_EXPR_RET(children[0]);
    if (children[0]->col_type() == pb::STRING 
        || children[0]->col_type() == pb::DATE
        || children[0]->col_type() == pb::TIMESTAMP 
        || children[0]->col_type() == pb::TIME) {
        ExprValueCastFunctionOptions option(children[0]->col_type());
        out = arrow::compute::call("expr_value_to_datetime", {children[0]->arrow_expr()}, std::move(option));
    } else {
        out = children[0]->arrow_expr();
    }
    return 0;
}

int arrow_cast_to_time(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    BUILD_ARROW_EXPR_RET(children[0]);
    if (children[0]->col_type() == pb::STRING 
        || children[0]->col_type() == pb::DATETIME
        || children[0]->col_type() == pb::TIMESTAMP 
        || children[0]->col_type() == pb::DATE) {
        ExprValueCastFunctionOptions option(children[0]->col_type());
        out = arrow::compute::call("expr_value_to_time", {children[0]->arrow_expr()}, std::move(option));
    } else {
        out = children[0]->arrow_expr();
    }
    return 0;
}

int arrow_murmur_hash(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    if (children.empty()) {
        arrow::NullScalar null_scalar;
        out = arrow::compute::literal(null_scalar);
    }
    BUILD_ARROW_EXPR_RET(children[0]);
    out = arrow::compute::call("murmur_hash", {children[0]->arrow_expr()});
    return 0;
}

/*
 * Register to arrow: Function ExprValueCast
 * 整数型/字符串类型直接用arrow cast function, 已下几种需要特殊处理, 和行保持一致
 * ExprValueCast FUNCTION 目前支持:
 * ExprValueToString
 * 1. cast to string    (ExprValueCastToString)
 *   -  FLOAT/DOUBLE -> STRING   (精度统一)
 *   -  Time/Date/DateTime/Timestamp -> STRING(formated)
 * ExprValueToTimeType
 * 2. cast to date      (ExprValueCastToDate)
 *   -  STRING/Datetime/time/timestamp -> Date
 * 3. cast to time      (ExprValueCastToTime)
 *   -  STRING/Datetime/Date/timestamp -> Time
 * 4. cast to datetime  (ExprValueCastToDateTime)
 *   -  STRING/Date/Time/Timestamp -> DateTime
 * 5. cast to timestamp (ExprValueCastToTimestamp)
 *   -  STRING/Date/Time/Datetime -> Timestamp
 * TODO
 * 6. string to numberic   不失败, 或许改arrow来支持
 */

//!!!!! BE CAREFUL: 数值型的必须用array span, 字符串用array_data  !!!!!!!

template <typename O, typename I>
struct ExecExprValueToDate {
using OutputValueCType = typename arrow::TypeTraits<O>::CType;
static arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch, arrow::compute::ExecResult* out) {
    using InputValueCType = typename arrow::TypeTraits<I>::CType;

    ExprValueCastState* state = static_cast<ExprValueCastState*>(ctx->state());
    pb::PrimitiveType type = state->type;
    const arrow::ArraySpan& input = batch[0].array;
    arrow::ArraySpan* out_data = out->array_span_mutable();
    OutputValueCType* out_values = out_data->GetValues<OutputValueCType>(1);
    switch (type) {
        case pb::DATETIME: {
            const InputValueCType* in_values = input.GetValues<InputValueCType>(1);
            for (auto i = 0; i < input.length; ++i) {
                *out_values++ = datetime_to_date(*in_values++);
            }
            break;
        }
        case pb::TIME: {
            const InputValueCType* in_values = input.GetValues<InputValueCType>(1);
            for (auto i = 0; i < input.length; ++i) {
                *out_values++ = datetime_to_date(time_to_datetime(*in_values++));
            }
            break;
        }
        case pb::TIMESTAMP: {
            const InputValueCType* in_values = input.GetValues<InputValueCType>(1);
            for (auto i = 0; i < input.length; ++i) {
                *out_values++ = datetime_to_date(timestamp_to_datetime(*in_values++));
            }
            break;
        }
        default:
            return arrow::Status::TypeError("not support");
    }
    return arrow::Status::OK();
}
static arrow::Status ExecStringInput(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch, arrow::compute::ExecResult* out) {
    ExprValueCastState* state = static_cast<ExprValueCastState*>(ctx->state());
    pb::PrimitiveType type = state->type;
    const arrow::ArraySpan& input = batch[0].array;
    arrow::ArraySpan* out_data = out->array_span_mutable();
    OutputValueCType* out_values = out_data->GetValues<OutputValueCType>(1);
    if (type != pb::STRING) {
        return arrow::Status::TypeError("not support");
    }
    arrow::compute::internal::VisitArrayValuesInline<arrow::LargeBinaryType>(
        input,
        [&](std::string_view v) {
            *out_values++ = datetime_to_date(str_to_datetime(v.data(), v.length()));
        },
        [&]() {
            // null
            *out_values++ = OutputValueCType{};
        });
    return arrow::Status::OK();
}
};

template <typename O, typename I>
struct ExecExprValueToTime {
using OutputValueCType = typename arrow::TypeTraits<O>::CType;
static arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch, arrow::compute::ExecResult* out) { 
    using InputValueCType = typename arrow::TypeTraits<I>::CType;
    ExprValueCastState* state = static_cast<ExprValueCastState*>(ctx->state());
    pb::PrimitiveType type = state->type;
    const arrow::ArraySpan& input = batch[0].array;
    arrow::ArraySpan* out_data = out->array_span_mutable();
    OutputValueCType* out_values = out_data->GetValues<OutputValueCType>(1);
    switch (type) {
        case pb::DATETIME: {
            const InputValueCType* in_values = input.GetValues<InputValueCType>(1);
            for (auto i = 0; i < input.length; ++i) {
                *out_values++ = datetime_to_time(*in_values++);
            }
            break;
        }
        case pb::DATE: {
            const InputValueCType* in_values = input.GetValues<InputValueCType>(1);
            for (auto i = 0; i < input.length; ++i) {
                *out_values++ = datetime_to_time(date_to_datetime(*in_values++));
            }
            break;
        }
        case pb::TIMESTAMP: {
            const InputValueCType* in_values = input.GetValues<InputValueCType>(1);
            for (auto i = 0; i < input.length; ++i) {
                *out_values++ = datetime_to_time(timestamp_to_datetime(*in_values++));
            }
            break;
        }
        default:
            return arrow::Status::TypeError("not support");
    }
    return arrow::Status::OK();
}
static arrow::Status ExecStringInput(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch, arrow::compute::ExecResult* out) { 
    ExprValueCastState* state = static_cast<ExprValueCastState*>(ctx->state());
    pb::PrimitiveType type = state->type;
    const arrow::ArraySpan& input = batch[0].array;
    arrow::ArraySpan* out_data = out->array_span_mutable();
    OutputValueCType* out_values = out_data->GetValues<OutputValueCType>(1);
    if (type != pb::STRING) {
        return arrow::Status::TypeError("not support");
    }
    arrow::compute::internal::VisitArrayValuesInline<arrow::LargeBinaryType>(
        input,
        [&](std::string_view v) {
            *out_values++ = str_to_time(v.data(), v.length());
        },
        [&]() {
            // null
            *out_values++ = OutputValueCType{};
        });
    return arrow::Status::OK();
}
};

template <typename O, typename I>
struct ExecExprValueToDateTime {
using OutputValueCType = typename arrow::TypeTraits<O>::CType;
static arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch, arrow::compute::ExecResult* out) {
    using InputValueCType = typename arrow::TypeTraits<I>::CType;
    ExprValueCastState* state = static_cast<ExprValueCastState*>(ctx->state());
    pb::PrimitiveType type = state->type;
    const arrow::ArraySpan& input = batch[0].array;
    arrow::ArraySpan* out_data = out->array_span_mutable();
    OutputValueCType* out_values = out_data->GetValues<OutputValueCType>(1);
    switch (type) {
        case pb::TIME: {
            const InputValueCType* in_values = input.GetValues<InputValueCType>(1);
            for (auto i = 0; i < input.length; ++i) {
                *out_values++ = time_to_datetime(*in_values++);
            }
            break;
        }
        case pb::DATE: {
            const InputValueCType* in_values = input.GetValues<InputValueCType>(1);
            for (auto i = 0; i < input.length; ++i) {
                *out_values++ = date_to_datetime(*in_values++);
            }
            break;
        }
        case pb::TIMESTAMP: {
            const InputValueCType* in_values = input.GetValues<InputValueCType>(1);
            for (auto i = 0; i < input.length; ++i) {
                *out_values++ = timestamp_to_datetime(*in_values++);
            }
            break;
        }
        default:
            return arrow::Status::TypeError("not support");
    }
    return arrow::Status::OK();
}
static arrow::Status ExecStringInput(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch, arrow::compute::ExecResult* out) {
    ExprValueCastState* state = static_cast<ExprValueCastState*>(ctx->state());
    pb::PrimitiveType type = state->type;
    const arrow::ArraySpan& input = batch[0].array;
    arrow::ArraySpan* out_data = out->array_span_mutable();
    OutputValueCType* out_values = out_data->GetValues<OutputValueCType>(1);
    if (type != pb::STRING) {
        return arrow::Status::TypeError("not support");
    }
    arrow::compute::internal::VisitArrayValuesInline<arrow::LargeBinaryType>(
        input,
        [&](std::string_view v) {
            *out_values++ = str_to_datetime(v.data(), v.length());
        },
        [&]() {
            // null
            *out_values++ = OutputValueCType{};
        });
    return arrow::Status::OK();
}
};

template <typename O, typename I>
struct ExecExprValueToTimeStamp {
using OutputValueCType = typename arrow::TypeTraits<O>::CType;
static arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch, arrow::compute::ExecResult* out) { 
    using InputValueCType = typename arrow::TypeTraits<I>::CType;
    ExprValueCastState* state = static_cast<ExprValueCastState*>(ctx->state());
    pb::PrimitiveType type = state->type;
    const arrow::ArraySpan& input = batch[0].array;
    arrow::ArraySpan* out_data = out->array_span_mutable();
    OutputValueCType* out_values = out_data->GetValues<OutputValueCType>(1);
    switch (type) {
        case pb::TIME: {
            const InputValueCType* in_values = input.GetValues<InputValueCType>(1);
            for (auto i = 0; i < input.length; ++i) {
                *out_values++ = datetime_to_timestamp(time_to_datetime(*in_values++));
            }
            break;
        }
        case pb::DATE: {
            const InputValueCType* in_values = input.GetValues<InputValueCType>(1);
            for (auto i = 0; i < input.length; ++i) {
                *out_values++ = datetime_to_timestamp(date_to_datetime(*in_values++));
            }
            break;
        }
        case pb::DATETIME: {
            const InputValueCType* in_values = input.GetValues<InputValueCType>(1);
            for (auto i = 0; i < input.length; ++i) {
                *out_values++ = datetime_to_timestamp(*in_values++);
            }
            break;
        }
        default:
            return arrow::Status::TypeError("not support");
    }
    return arrow::Status::OK();
}
static arrow::Status ExecStringInput(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch, arrow::compute::ExecResult* out) { 
    ExprValueCastState* state = static_cast<ExprValueCastState*>(ctx->state());
    pb::PrimitiveType type = state->type;
    const arrow::ArraySpan& input = batch[0].array;
    arrow::ArraySpan* out_data = out->array_span_mutable();
    OutputValueCType* out_values = out_data->GetValues<OutputValueCType>(1);
    if (type != pb::STRING) {
        return arrow::Status::TypeError("not support");
    }
    arrow::compute::internal::VisitArrayValuesInline<arrow::LargeBinaryType>(
        input,
        [&](std::string_view v) {
            *out_values++ = datetime_to_timestamp(str_to_datetime(v.data(), v.length()));
        },
        [&]() {
            // null
            *out_values++ = OutputValueCType{};
        });
    return arrow::Status::OK();
}
};

template <typename O, typename I>
struct ExecExprValueToString {
static arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch, arrow::compute::ExecResult* out) {    
    using InputValueCType = typename arrow::TypeTraits<I>::CType;
    using BuilderType = typename arrow::TypeTraits<O>::BuilderType;

    ExprValueCastState* state = static_cast<ExprValueCastState*>(ctx->state());
    pb::PrimitiveType type = state->type;
    const arrow::ArraySpan& input = batch[0].array;
    BuilderType builder;
    switch (type) {
        case pb::FLOAT: {
            arrow::VisitArraySpanInline<I>(
                input,
                [&](InputValueCType v) {
                    char tmp_buf[24] = {0};
                    snprintf(tmp_buf, sizeof(tmp_buf), "%.6g", (double)v);
                    return builder.Append(std::string(tmp_buf));
                },
                [&]() { return builder.AppendNull(); });
            break;
        }
        case pb::DOUBLE: {
            arrow::VisitArraySpanInline<I>(
                input,
                [&](InputValueCType v) {
                    char tmp_buf[24] = {0};
                    snprintf(tmp_buf, sizeof(tmp_buf), "%.12g", (double)v);
                    return builder.Append(std::string(tmp_buf));
                },
                [&]() { return builder.AppendNull(); });
            break;
        }
        case pb::DATETIME: {
            arrow::VisitArraySpanInline<I>(
                input,
                [&](InputValueCType v) {
                    return builder.Append(datetime_to_str(v));
                },
                [&]() { return builder.AppendNull(); });
            break;
        }
        case pb::TIME: {
            arrow::VisitArraySpanInline<I>(
                input,
                [&](InputValueCType v) {
                    return builder.Append(time_to_str(v));
                },
                [&]() { return builder.AppendNull(); });
            break;
        }
        case pb::TIMESTAMP: {
            arrow::VisitArraySpanInline<I>(
                input,
                [&](InputValueCType v) {
                    return builder.Append(timestamp_to_str(v));
                },
                [&]() { return builder.AppendNull(); });
            break;
        }
        case pb::DATE: {
            arrow::VisitArraySpanInline<I>(
                input,
                [&](InputValueCType v) {
                    return builder.Append(date_to_str(v));
                },
                [&]() { return builder.AppendNull(); });
            break;
        }
        default:
            return arrow::Status::TypeError("not support");
    }
    std::shared_ptr<arrow::Array> output_array;
    auto status = builder.Finish(&output_array);
    if (!status.ok()) {
        return arrow::Status::IOError("array finish fail");
    }
    out->value = std::move(output_array->data());
    return arrow::Status::OK();
}
};

arrow::Status ExecMurmurHash(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch, arrow::compute::ExecResult* out) {    
    const arrow::ArraySpan& input = batch[0].array;
    arrow::ArraySpan* out_data = out->array_span_mutable();
    uint64_t* out_values = out_data->GetValues<uint64_t>(1);

    if (input.type == nullptr || !is_base_binary_like(input.type->id())) {
        for (auto i = 0; i < input.length; ++i) {
            *out_values++ = make_sign(std::string(""));
        }
        return arrow::Status::OK();
    }
    
    arrow::compute::internal::VisitArrayValuesInline<arrow::LargeBinaryType>(
        input,
        [&](std::string_view v) {
            *out_values++ = make_sign(v);
        },
        [&]() {
            // null
            *out_values++ = make_sign(std::string(""));
        });
    return arrow::Status::OK();
}

template <typename O, typename I1, typename I2>
struct ExecRound {
    using Input1ValueCType = typename arrow::TypeTraits<I1>::CType;
    using Input2ValueCType = typename arrow::TypeTraits<I2>::CType;
    using OutputValueCType = typename arrow::TypeTraits<O>::CType;
    static OutputValueCType cal_result(const Input1ValueCType& input1, const Input2ValueCType& bits) {
        OutputValueCType base = std::pow(10, bits);
        if (base > 0) {
            if (input1 < 0) {
                return  -::round(-input1 * base) / base;
            } else {
                return   ::round(input1 * base) / base;
            }
        }
        return OutputValueCType();
    };
    static arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch, arrow::compute::ExecResult* out) { 
        Input2ValueCType bits = 0;
        if (batch[1].is_scalar()) {
            bits = arrow::compute::internal::UnboxScalar<I2>::Unbox(*batch[1].scalar);
        } else {
            const arrow::ArraySpan& input = batch[1].array;
            bits = *input.GetValues<Input2ValueCType>(1);
        }
        arrow::ArraySpan* out_data = out->array_span_mutable();
        OutputValueCType* out_values = out_data->GetValues<OutputValueCType>(1);
        if (batch[0].is_scalar()) {
            const Input1ValueCType value = arrow::compute::internal::UnboxScalar<I1>::Unbox(*batch[0].scalar);
            *out_values++ = cal_result(value, bits);
        } else {
            const arrow::ArraySpan& input = batch[0].array;
            const Input1ValueCType* in_values = input.GetValues<Input1ValueCType>(1);
            for (auto i = 0; i < input.length; ++i) {
                *out_values++ = cal_result(*in_values++, bits);
            }
        }
        return arrow::Status::OK();
    }
};
/*
 * 类型转换通用config
 */
class ExprValueCastOptionsType : public arrow::compute::FunctionOptionsType {
public:
    static const arrow::compute::FunctionOptionsType* GetInstance() {
        static std::unique_ptr<arrow::compute::FunctionOptionsType> instance(new ExprValueCastOptionsType());
        return instance.get();
    }
    const char* type_name() const override { return "ExprValueCast"; }
    std::string Stringify(const  arrow::compute::FunctionOptions& options) const override {
        return type_name();
    }
    bool Compare(const arrow::compute::FunctionOptions& options,
                 const arrow::compute::FunctionOptions& other) const override {
        const auto& lop = static_cast<const ExprValueCastFunctionOptions&>(options);
        const auto& rop = static_cast<const ExprValueCastFunctionOptions&>(other);
        return lop.type == rop.type;
    }
    std::unique_ptr<arrow::compute::FunctionOptions> Copy(const arrow::compute::FunctionOptions& options) const override {
        const auto& opts = static_cast<const ExprValueCastFunctionOptions&>(options);
        return std::make_unique<ExprValueCastFunctionOptions>(opts.type);
    }
};

arrow::Result<std::unique_ptr<arrow::compute::KernelState>> InitExprValueCast(arrow::compute::KernelContext*,
                                            const  arrow::compute::KernelInitArgs& args) {
    auto func_options = static_cast<const ExprValueCastFunctionOptions*>(args.options);
    return std::make_unique<ExprValueCastState>(func_options ? func_options->type : pb::INVALID_TYPE);
}

ExprValueCastFunctionOptions::ExprValueCastFunctionOptions(pb::PrimitiveType value)
    : arrow::compute::FunctionOptions(ExprValueCastOptionsType::GetInstance()), type(value) {}

arrow::Status ArrowFunctionManager::RegisterAllDefinedFunction() {
    auto registry = arrow::compute::GetFunctionRegistry();
    /*
     * expr_value_to_string
     */
    {
        auto expr_value_to_string_func = std::make_shared<arrow::compute::ScalarFunction>("expr_value_to_string", arrow::compute::Arity::Unary(),
                                                    /*doc=*/arrow::compute::FunctionDoc::Empty());
        for (const std::shared_ptr<arrow::DataType>& in_ty : arrow::IntTypes()) {
            ARROW_RETURN_NOT_OK(expr_value_to_string_func->AddKernel({in_ty},  // 输入类型
                    arrow::large_binary(),  // 输出类型
                    arrow::compute::internal::GenerateNumeric<ExecExprValueToString, arrow::LargeBinaryType>(*in_ty),
                    InitExprValueCast));
        }
        // float -> string, 精度统一
        arrow::compute::ScalarKernel k_float_to_string({arrow::float32()}, arrow::large_binary(), 
                                                ExecExprValueToString<arrow::LargeBinaryType, arrow::FloatType>::Exec, InitExprValueCast);
        // double -> string, 精度统一
        arrow::compute::ScalarKernel k_double_to_string({arrow::float64()}, arrow::large_binary(), 
                                                ExecExprValueToString<arrow::LargeBinaryType, arrow::DoubleType>::Exec, InitExprValueCast);
        ARROW_RETURN_NOT_OK(expr_value_to_string_func->AddKernel(k_float_to_string));
        ARROW_RETURN_NOT_OK(expr_value_to_string_func->AddKernel(k_double_to_string));
        ARROW_RETURN_NOT_OK(registry->AddFunction(expr_value_to_string_func));
    }

    /*
     * expr_value_to_date
     */
    {
        auto expr_value_to_date_func = std::make_shared<arrow::compute::ScalarFunction>("expr_value_to_date", arrow::compute::Arity::Unary(),
                                                    /*doc=*/arrow::compute::FunctionDoc::Empty());
        for (const std::shared_ptr<arrow::DataType>& in_ty : arrow::IntTypes()) {
            ARROW_RETURN_NOT_OK(expr_value_to_date_func->AddKernel({in_ty},  // 输入类型
                    arrow::uint32(),  // 输出类型
                    arrow::compute::internal::GenerateNumeric<ExecExprValueToDate, arrow::UInt32Type>(*in_ty),
                    InitExprValueCast));
        } 
        arrow::compute::ScalarKernel k_string_to_date({arrow::large_binary()}, arrow::uint32(), 
                                                ExecExprValueToDate<arrow::UInt32Type, arrow::LargeBinaryType>::ExecStringInput, InitExprValueCast);
        ARROW_RETURN_NOT_OK(expr_value_to_date_func->AddKernel(k_string_to_date));
        ARROW_RETURN_NOT_OK(registry->AddFunction(expr_value_to_date_func));
    }

    /*
     * expr_value_to_datetime
     */
    {
        auto expr_value_to_datetime_func = std::make_shared<arrow::compute::ScalarFunction>("expr_value_to_datetime", arrow::compute::Arity::Unary(),
                                                    /*doc=*/arrow::compute::FunctionDoc::Empty());
        for (const std::shared_ptr<arrow::DataType>& in_ty : arrow::IntTypes()) {
            ARROW_RETURN_NOT_OK(expr_value_to_datetime_func->AddKernel({in_ty},  // 输入类型
                    arrow::uint64(),  // 输出类型
                    arrow::compute::internal::GenerateNumeric<ExecExprValueToDateTime, arrow::UInt64Type>(*in_ty),
                    InitExprValueCast));
        } 
        arrow::compute::ScalarKernel k_string_to_datetime({arrow::large_binary()}, arrow::uint64(), 
                                                ExecExprValueToDateTime<arrow::UInt64Type, arrow::LargeBinaryType>::ExecStringInput, InitExprValueCast);
        ARROW_RETURN_NOT_OK(expr_value_to_datetime_func->AddKernel(k_string_to_datetime));
        ARROW_RETURN_NOT_OK(registry->AddFunction(expr_value_to_datetime_func));
    }

    /*
     * expr_value_to_time
     */
    {
        auto expr_value_to_time_func = std::make_shared<arrow::compute::ScalarFunction>("expr_value_to_time", arrow::compute::Arity::Unary(),
                                                    /*doc=*/arrow::compute::FunctionDoc::Empty());
        for (const std::shared_ptr<arrow::DataType>& in_ty : arrow::IntTypes()) {
            ARROW_RETURN_NOT_OK(expr_value_to_time_func->AddKernel({in_ty},  // 输入类型
                    arrow::int32(),  // 输出类型
                    arrow::compute::internal::GenerateNumeric<ExecExprValueToTime, arrow::Int32Type>(*in_ty),
                    InitExprValueCast));
        } 
        arrow::compute::ScalarKernel k_string_to_time({arrow::large_binary()}, arrow::int32(), 
                                                ExecExprValueToTime<arrow::Int32Type, arrow::LargeBinaryType>::ExecStringInput, InitExprValueCast);
        ARROW_RETURN_NOT_OK(expr_value_to_time_func->AddKernel(k_string_to_time));
        ARROW_RETURN_NOT_OK(registry->AddFunction(expr_value_to_time_func));
    }

    /*
     * expr_value_to_time
     */
    {
        auto expr_value_to_timestamp_func = std::make_shared<arrow::compute::ScalarFunction>("expr_value_to_timestamp", arrow::compute::Arity::Unary(),
                                                    /*doc=*/arrow::compute::FunctionDoc::Empty());
         for (const std::shared_ptr<arrow::DataType>& in_ty : arrow::IntTypes()) {
            ARROW_RETURN_NOT_OK(expr_value_to_timestamp_func->AddKernel({in_ty},  // 输入类型
                    arrow::uint32(),  // 输出类型
                    arrow::compute::internal::GenerateNumeric<ExecExprValueToTimeStamp, arrow::UInt32Type>(*in_ty),
                    InitExprValueCast));
        } 
        arrow::compute::ScalarKernel k_string_to_timestamp({arrow::large_binary()}, arrow::uint32(), 
                                                ExecExprValueToTimeStamp<arrow::UInt32Type, arrow::LargeBinaryType>::ExecStringInput, InitExprValueCast);
        ARROW_RETURN_NOT_OK(expr_value_to_timestamp_func->AddKernel(k_string_to_timestamp));
        ARROW_RETURN_NOT_OK(registry->AddFunction(expr_value_to_timestamp_func));
    }
    /*
     * murmur_hash
     */
    {
        auto arrow_murmur_hash = std::make_shared<arrow::compute::ScalarFunction>("murmur_hash", arrow::compute::Arity::Unary(),
                                                    /*doc=*/arrow::compute::FunctionDoc::Empty());
        for (const std::shared_ptr<arrow::DataType>& in_ty : arrow::NumericTypes()) {
            ARROW_RETURN_NOT_OK(arrow_murmur_hash->AddKernel({in_ty}, arrow::uint64(), ExecMurmurHash));
        } 
        for (const std::shared_ptr<arrow::DataType>& in_ty : arrow::BaseBinaryTypes()) {
            ARROW_RETURN_NOT_OK(arrow_murmur_hash->AddKernel({in_ty}, arrow::uint64(), ExecMurmurHash));
        }
        ARROW_RETURN_NOT_OK(registry->AddFunction(arrow_murmur_hash));
    }
     /*
     * round, 两元函数, 第二列是int scalar(精度)
     */
    {
        auto arrow_round = std::make_shared<arrow::compute::ScalarFunction>("arrow_round", arrow::compute::Arity::Binary(),
                                                    /*doc=*/arrow::compute::FunctionDoc::Empty());
        ARROW_RETURN_NOT_OK(arrow_round->AddKernel({arrow::float32(), arrow::int32()}, arrow::float64(), 
                    ExecRound<arrow::DoubleType, arrow::FloatType, arrow::Int32Type>::Exec));
        ARROW_RETURN_NOT_OK(arrow_round->AddKernel({arrow::float64(), arrow::int32()}, arrow::float64(), 
                    ExecRound<arrow::DoubleType, arrow::DoubleType, arrow::Int32Type>::Exec));
        ARROW_RETURN_NOT_OK(registry->AddFunction(arrow_round));
    }
    return arrow::Status::OK();
}

arrow::Status ArrowFunctionManager::RegisterAllInteralFunction() {
    // 算术
    register_object("uminus", arrow_uminus);
    register_object("add", arrow_add);
    register_object("minus", arrow_minus);
    register_object("multiplies", arrow_multiplies);
    register_object("divides", arrow_divides);
    register_object("ceil", arrow_ceil);
    register_object("ceiling", arrow_ceil);
    register_object("floor", arrow_floor);
    register_object("round", arrow_round);

    // 比较
    register_object("eq", arrow_eq);
    register_object("row_expr_eq", arrow_row_expr_eq);
    register_object("ne", arrow_ne);
    register_object("row_expr_ne", arrow_row_expr_ne);
    register_object("gt", arrow_gt);
    register_object("row_expr_gt", arrow_row_expr_gt);
    register_object("ge", arrow_ge);
    register_object("row_expr_ge", arrow_row_expr_ge);
    register_object("lt", arrow_lt);
    register_object("row_expr_lt", arrow_row_expr_lt);
    register_object("le", arrow_le);
    register_object("row_expr_le", arrow_row_expr_le);
    register_object("least", arrow_least);
    register_object("greatest", arrow_greatest);
    
    // 位运算
    register_object("bit_and", arrow_bitwise_and);
    register_object("bit_or", arrow_bitwise_or);
    register_object("bit_xor", arrow_bitwise_xor);
    register_object("bit_not", arrow_bitwise_not);
    register_object("left_shift", arrow_left_shift);
    register_object("right_shift", arrow_right_shift);

    // 选择
    if (FLAGS_enable_arrow_complex_func) {
        register_object("case_when", arrow_case_when);
        register_object("case_expr_when", arrow_case_expr_when);
        register_object("if", arrow_if);
        register_object("ifnull", arrow_if_null);
    }
    // string
    register_object("concat", arrow_concat);
    register_object("concat_ws", arrow_concat_ws);
    register_object("length", arrow_length);
    register_object("bit_length", arrow_bit_length);
    register_object("substr", arrow_substr);
    register_object("substring", arrow_substr);
    register_object("upper", arrow_upper);
    register_object("lower", arrow_lower);
    register_object("reverse", arrow_reverse);
    register_object("repeat", arrow_repeat);
    // 类型转换
    register_object("cast_to_string", arrow_cast_to_string);
    register_object("cast_to_date", arrow_cast_to_date);
    register_object("cast_to_time", arrow_cast_to_time);
    register_object("cast_to_datetime", arrow_cast_to_datetime);
    register_object("cast_to_double", arrow_cast_to_double);
    register_object("cast_to_signed", arrow_cast_to_signed);
    register_object("cast_to_unsigned", arrow_cast_to_unsigned);

    // 时间
    register_object("str_to_date", arrow_str_to_date);
    register_object("date_format", arrow_date_format);
    register_object("time_format", arrow_time_format);
    register_object("current_timestamp", arrow_now);
    register_object("now", arrow_now);
    register_object("date", arrow_date);
    register_object("hour", arrow_hour);
    register_object("date_sub", arrow_date_sub);
    register_object("subdate", arrow_date_sub);
    register_object("date_add", arrow_date_add);
    register_object("adddate", arrow_date_add);
    register_object("curdate", arrow_current_date);
    register_object("current_date", arrow_current_date);
    register_object("curtime", arrow_current_time);
    register_object("current_time", arrow_current_time);
    register_object("timestamp", arrow_timestamp);
    register_object("time_to_sec", arrow_time_to_sec);
    register_object("from_unixtime", arrow_from_unixtime);
    register_object("unix_timestamp", arrow_unix_timestamp);
    register_object("week", arrow_week);
    register_object("yearweek", arrow_yearweek);
    register_object("timestampdiff", arrow_timestampdiff);

    // 其他
    register_object("murmur_hash", arrow_murmur_hash);
    return arrow::Status::OK();
}

int ArrowFunctionManager::RegisterAllArrowFunction() {
    auto s = RegisterAllInteralFunction();
    if (!s.ok()) {
        DB_FATAL("register arrow function fail: %s", s.ToString().c_str());
        return -1;
    }
    s = RegisterAllDefinedFunction();
    if (!s.ok()) {
        DB_FATAL("register arrow function fail: %s", s.ToString().c_str());
        return -1;
    }
    s = RegisterAllTimeFunction();
    if (!s.ok()) {
        DB_FATAL("register arrow time function fail: %s", s.ToString().c_str());
        return -1;
    }
    s = RegisterAllStringFunction();
    if (!s.ok()) {
        DB_FATAL("register arrow string function fail: %s", s.ToString().c_str());
        return -1;
    }
    s = RegisterAllHashAggFunction();
    if (!s.ok()) {
        DB_FATAL("register arrow string function fail: %s", s.ToString().c_str());
        return -1;
    }
    return 0;
}

ArrowExprBuildFun ArrowFunctionManager::get_func(int32_t func_op, const std::string& func_name, bool is_row_expr) {
    // DB_WARNING("func_op: %d, func_name: %s, is_row_expr: %d", func_op, func_name.c_str(), is_row_expr);
    switch (func_op) {
        case parser::FT_EQ:
            return is_row_expr ? get_object("row_expr_eq") : get_object("eq");
        case parser::FT_NE: 
            return is_row_expr ? get_object("row_expr_ne") : get_object("ne");
        case parser::FT_GE:
            return is_row_expr ? get_object("row_expr_ge") : get_object("ge");
        case parser::FT_GT:
            return is_row_expr ? get_object("row_expr_gt") : get_object("gt");
        case parser::FT_LE:
            return is_row_expr ? get_object("row_expr_le") : get_object("le");
        case parser::FT_LT: 
            return is_row_expr ? get_object("row_expr_lt") : get_object("lt");
        case parser::FT_UMINUS:
            return get_object("uminus");
        case parser::FT_ADD:
            return get_object("add");
        case parser::FT_MINUS:
            return get_object("minus");
        case parser::FT_MULTIPLIES:
            return get_object("multiplies");
        case parser::FT_DIVIDES: 
            return get_object("divides");
        case parser::FT_BIT_AND:
            return get_object("bit_and");
        case parser::FT_BIT_OR: 
            return get_object("bit_or");
        case parser::FT_BIT_XOR: 
            return get_object("bit_xor");
        case parser::FT_BIT_NOT: 
            return get_object("bit_not");
        case parser::FT_LS: 
            return get_object("left_shift");
        case parser::FT_RS: 
            return get_object("right_shift");
        case parser::FT_COMMON:
            return get_object(func_name);
        default:
            return nullptr;
    }
    return nullptr;
}
}
