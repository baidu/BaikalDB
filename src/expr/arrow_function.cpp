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

namespace baikaldb {

DEFINE_bool(enable_arrow_complex_func, false, "enable_arrow_complex_func");

// [TODO] Arrow CastNumberImpl 同类型直接调用 ZeroCopyCastExec
// args需要cast成fn对应的类型, 
// 如case when的value类型需要cast成col_type
// 保持和行一致
// plan cache模式下的col type可能不对

#define BUILD_ARROW_EXPR_RET(c) \
    if (0 != c->transfer_to_arrow_expression()) {  \
        return -1;   \
    }

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
    BUILD_ARROW_EXPR_RET(node);
    if (node->col_type() != col_type 
            && (is_datetime_specic(node->col_type()) || node->col_type() == pb::STRING) 
            && (is_datetime_specic(col_type) || col_type == pb::STRING)) {
        std::string cast_func_name;
        switch (col_type) {
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
        ExprValueCastFunctionOptions option(node->col_type());
        arrow::Expression cast_expr = arrow::compute::call(cast_func_name, {node->arrow_expr()}, std::move(option));
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
            arguments.emplace_back(arrow::compute::call("cast", {c->arrow_expr()}, arrow::compute::CastOptions::Safe(arrow::float64())));
        } else if (has_uint(args_types)) {
            arguments.emplace_back(arrow::compute::call("cast", {c->arrow_expr()}, arrow::compute::CastOptions::Safe(arrow::uint64())));
        } else {
            arguments.emplace_back(arrow::compute::call("cast", {c->arrow_expr()}, arrow::compute::CastOptions::Safe(arrow::int64())));
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
        arrow::compute::call("cast", {children[0]->arrow_expr()}, arrow::compute::CastOptions::Safe(arrow::float64())),
        arrow::compute::call("if_else", {
                arrow::compute::call("equal", {children[1]->arrow_expr(), arrow::compute::literal(0)}), 
                arrow::compute::literal(null_datum), 
                arrow::compute::call("cast", {children[1]->arrow_expr()}, arrow::compute::CastOptions::Safe(arrow::float64()))
        })
    });
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
    if (children.size() != 3) {
        return -1;
    }
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

int arrow_concat(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    std::vector<arrow::compute::Expression> args;
    for (auto& c : children) { 
        BUILD_ARROW_EXPR_RET(c);
        if (is_datetime_specic(c->col_type()) 
                || is_double(c->col_type())) {
            ExprValueCastFunctionOptions option(c->col_type());
            args.emplace_back(arrow::compute::call("expr_value_to_string", {c->arrow_expr()}, std::move(option)));
        } else {
            args.emplace_back(arrow::compute::call("cast", {c->arrow_expr()}, arrow::compute::CastOptions::Unsafe(arrow::large_binary())));
        }
    }
    // 连接符放在最后
    args.emplace_back(arrow::compute::literal(std::make_shared<arrow::LargeBinaryScalar>("")));
    /// A null in any input results in a null in the output. binary_join_element_wise要求args类型都一样, large_binary
    std::shared_ptr<arrow::compute::JoinOptions> option = std::make_shared<arrow::compute::JoinOptions>();
    out = arrow::compute::call("binary_join_element_wise", args, option);
    return 0;
}

int arrow_length(std::vector<ExprNode*>& children, pb::Function* fn, const pb::PrimitiveType& return_type, arrow::compute::Expression& out) {
    BUILD_ARROW_EXPR_RET(children[0]);
    out = arrow::compute::call("binary_length", {arrow::compute::call("cast", {children[0]->arrow_expr()}, arrow::compute::CastOptions::Unsafe(arrow::large_binary()))});
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
            *out_values++ = datetime_to_time(str_to_datetime(v.data(), v.length()));
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
arrow::Status ExecExprValueToString(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch, arrow::compute::ExecResult* out) {    
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

arrow::Result<std::unique_ptr<arrow::compute::KernelState>> InitExprValueCast(arrow::compute::KernelContext*,
                                            const  arrow::compute::KernelInitArgs& args) {
    auto func_options = static_cast<const ExprValueCastFunctionOptions*>(args.options);
    return std::make_unique<ExprValueCastState>(func_options ? func_options->type : pb::INVALID_TYPE);
}

ExprValueCastFunctionOptions::ExprValueCastFunctionOptions(pb::PrimitiveType value)
    : arrow::compute::FunctionOptions(ExprValueCastOptionsType::GetInstance()), type(value) {}


/*
 * Register to arrow: Function XXX (TODO)
 */

arrow::Status ArrowFunctionManager::RegisterAllDefinedFunction() {
    auto registry = arrow::compute::GetFunctionRegistry();
    /*
     * expr_value_to_string
     */
    {
        auto expr_value_to_string_func = std::make_shared<arrow::compute::ScalarFunction>("expr_value_to_string", arrow::compute::Arity::Unary(),
                                                    /*doc=*/arrow::compute::FunctionDoc::Empty());
        // Time -> string
        arrow::compute::ScalarKernel k_time_to_string({arrow::int32()}, arrow::large_binary(), 
                                                ExecExprValueToString<arrow::LargeBinaryType, arrow::Int32Type>, InitExprValueCast); 
        // TIMESTAMP/DATE -> string
        arrow::compute::ScalarKernel k_timestamp_date_to_string({arrow::uint32()}, arrow::large_binary(), 
                                                ExecExprValueToString<arrow::LargeBinaryType, arrow::UInt32Type>, InitExprValueCast);
        // DATETIME -> string 
        arrow::compute::ScalarKernel k_datetime_to_string({arrow::uint64()}, arrow::large_binary(), 
                                                ExecExprValueToString<arrow::LargeBinaryType, arrow::UInt64Type>, InitExprValueCast);
        // float -> string, 精度统一
        arrow::compute::ScalarKernel k_float_to_string({arrow::float32()}, arrow::large_binary(), 
                                                ExecExprValueToString<arrow::LargeBinaryType, arrow::FloatType>, InitExprValueCast);
        // double -> string, 精度统一
        arrow::compute::ScalarKernel k_double_to_string({arrow::float64()}, arrow::large_binary(), 
                                                ExecExprValueToString<arrow::LargeBinaryType, arrow::DoubleType>, InitExprValueCast);

        ARROW_RETURN_NOT_OK(expr_value_to_string_func->AddKernel(k_time_to_string));
        ARROW_RETURN_NOT_OK(expr_value_to_string_func->AddKernel(k_timestamp_date_to_string));
        ARROW_RETURN_NOT_OK(expr_value_to_string_func->AddKernel(k_datetime_to_string));
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
        arrow::compute::ScalarKernel k_string_to_date({arrow::large_binary()}, arrow::uint32(), 
                                                ExecExprValueToDate<arrow::UInt32Type, arrow::LargeBinaryType>::ExecStringInput, InitExprValueCast);
        arrow::compute::ScalarKernel k_timestamp_to_date({arrow::uint32()}, arrow::uint32(), 
                                                ExecExprValueToDate<arrow::UInt32Type, arrow::UInt32Type>::Exec, InitExprValueCast);
        arrow::compute::ScalarKernel k_time_to_date({arrow::int32()}, arrow::uint32(), 
                                                ExecExprValueToDate<arrow::UInt32Type, arrow::Int32Type>::Exec, InitExprValueCast);
        arrow::compute::ScalarKernel k_datetime_to_date({arrow::uint64()}, arrow::uint32(), 
                                                ExecExprValueToDate<arrow::UInt32Type, arrow::UInt64Type>::Exec, InitExprValueCast);

        ARROW_RETURN_NOT_OK(expr_value_to_date_func->AddKernel(k_string_to_date));
        ARROW_RETURN_NOT_OK(expr_value_to_date_func->AddKernel(k_timestamp_to_date));
        ARROW_RETURN_NOT_OK(expr_value_to_date_func->AddKernel(k_time_to_date));
        ARROW_RETURN_NOT_OK(expr_value_to_date_func->AddKernel(k_datetime_to_date));
        ARROW_RETURN_NOT_OK(registry->AddFunction(expr_value_to_date_func));
    }

    /*
     * expr_value_to_datetime
     */
    {
        auto expr_value_to_datetime_func = std::make_shared<arrow::compute::ScalarFunction>("expr_value_to_datetime", arrow::compute::Arity::Unary(),
                                                    /*doc=*/arrow::compute::FunctionDoc::Empty());
        arrow::compute::ScalarKernel k_string_to_datetime({arrow::large_binary()}, arrow::uint64(), 
                                                ExecExprValueToDateTime<arrow::UInt64Type, arrow::LargeBinaryType>::ExecStringInput, InitExprValueCast);
        arrow::compute::ScalarKernel k_timestamp_date_to_datetime({arrow::uint32()}, arrow::uint64(), 
                                                ExecExprValueToDateTime<arrow::UInt64Type, arrow::UInt32Type>::Exec, InitExprValueCast);
        arrow::compute::ScalarKernel k_time_to_datetime({arrow::int32()}, arrow::uint64(), 
                                                ExecExprValueToDateTime<arrow::UInt64Type, arrow::Int32Type>::Exec, InitExprValueCast);

        ARROW_RETURN_NOT_OK(expr_value_to_datetime_func->AddKernel(k_string_to_datetime));
        ARROW_RETURN_NOT_OK(expr_value_to_datetime_func->AddKernel(k_timestamp_date_to_datetime));
        ARROW_RETURN_NOT_OK(expr_value_to_datetime_func->AddKernel(k_time_to_datetime));
        ARROW_RETURN_NOT_OK(registry->AddFunction(expr_value_to_datetime_func));
    }

    /*
     * expr_value_to_time
     */
    {
        auto expr_value_to_time_func = std::make_shared<arrow::compute::ScalarFunction>("expr_value_to_time", arrow::compute::Arity::Unary(),
                                                    /*doc=*/arrow::compute::FunctionDoc::Empty());
        arrow::compute::ScalarKernel k_string_to_time({arrow::large_binary()}, arrow::int32(), 
                                                ExecExprValueToTime<arrow::Int32Type, arrow::LargeBinaryType>::ExecStringInput, InitExprValueCast);
        arrow::compute::ScalarKernel k_timestamp_date_to_time({arrow::uint32()}, arrow::int32(), 
                                                ExecExprValueToTime<arrow::Int32Type, arrow::UInt32Type>::Exec, InitExprValueCast);
        arrow::compute::ScalarKernel k_datetime_to_time({arrow::uint64()}, arrow::int32(), 
                                                ExecExprValueToTime<arrow::Int32Type, arrow::UInt64Type>::Exec, InitExprValueCast);
                                                
        ARROW_RETURN_NOT_OK(expr_value_to_time_func->AddKernel(k_string_to_time));
        ARROW_RETURN_NOT_OK(expr_value_to_time_func->AddKernel(k_timestamp_date_to_time));
        ARROW_RETURN_NOT_OK(expr_value_to_time_func->AddKernel(k_datetime_to_time));
        ARROW_RETURN_NOT_OK(registry->AddFunction(expr_value_to_time_func));
    }

    /*
     * expr_value_to_time
     */
    {
        auto expr_value_to_timestamp_func = std::make_shared<arrow::compute::ScalarFunction>("expr_value_to_timestamp", arrow::compute::Arity::Unary(),
                                                    /*doc=*/arrow::compute::FunctionDoc::Empty());
        arrow::compute::ScalarKernel k_string_to_timestamp({arrow::large_binary()}, arrow::uint32(), 
                                                ExecExprValueToTimeStamp<arrow::UInt32Type, arrow::LargeBinaryType>::ExecStringInput, InitExprValueCast);
        arrow::compute::ScalarKernel k_date_to_timestamp({arrow::uint32()}, arrow::uint32(), 
                                                ExecExprValueToTimeStamp<arrow::UInt32Type, arrow::UInt32Type>::Exec, InitExprValueCast);
        arrow::compute::ScalarKernel k_datetime_to_timestamp({arrow::uint64()}, arrow::uint32(), 
                                                ExecExprValueToTimeStamp<arrow::UInt32Type, arrow::UInt64Type>::Exec, InitExprValueCast);
        arrow::compute::ScalarKernel k_time_to_timestamp({arrow::int32()}, arrow::uint32(), 
                                                ExecExprValueToTimeStamp<arrow::UInt32Type, arrow::Int32Type>::Exec, InitExprValueCast);
        ARROW_RETURN_NOT_OK(expr_value_to_timestamp_func->AddKernel(k_string_to_timestamp));
        ARROW_RETURN_NOT_OK(expr_value_to_timestamp_func->AddKernel(k_date_to_timestamp));
        ARROW_RETURN_NOT_OK(expr_value_to_timestamp_func->AddKernel(k_datetime_to_timestamp));
        ARROW_RETURN_NOT_OK(expr_value_to_timestamp_func->AddKernel(k_time_to_timestamp));
        ARROW_RETURN_NOT_OK(registry->AddFunction(expr_value_to_timestamp_func));
    }
    return arrow::Status::OK();
}

arrow::Status ArrowFunctionManager::RegisterAllInteralFunction() {
    auto registry = arrow::compute::GetFunctionRegistry();
    // 算术
    register_object("uminus", arrow_uminus);
    register_object("add", arrow_add);
    register_object("minus", arrow_minus);
    register_object("multiplies", arrow_multiplies);
    register_object("divides", arrow_divides);

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

    // 选择
    if (FLAGS_enable_arrow_complex_func) {
        register_object("case_when", arrow_case_when);
        register_object("case_expr_when", arrow_case_expr_when);
        register_object("if", arrow_if);
    }
    // string
    register_object("concat", arrow_concat);
    register_object("length", arrow_length);
    register_object("cast_to_string", arrow_cast_to_string);

    // 时间
    register_object("str_to_date", arrow_cast_to_datetime);
    register_object("cast_to_date", arrow_cast_to_date);  
    register_object("cast_to_time", arrow_cast_to_time);  
    register_object("cast_to_datetime", arrow_cast_to_datetime);
    return arrow::Status::OK();
}

int ArrowFunctionManager::RegisterAllArrowFunction() {
    auto s = RegisterAllInteralFunction();
    if (!s.ok()) {
        DB_FATAL("register arrow function fail");
        return -1;
    }
    s = RegisterAllDefinedFunction();
    if (!s.ok()) {
        DB_FATAL("register arrow function fail");
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
        case parser::FT_COMMON:
            return get_object(func_name);
        default:
            return nullptr;
    }
    return nullptr;
}
}
