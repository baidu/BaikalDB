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

#include "fn_manager.h"
#include "operators.h"
#include "internal_functions.h"
#include "parser.h"

namespace baikaldb {
#define REGISTER_BINARY_OP(NAME, TYPE) \
    register_object(#NAME"_"#TYPE"_"#TYPE, NAME##_##TYPE##_##TYPE);
#define REGISTER_BINARY_OP_ALL_TYPES(NAME) \
    REGISTER_BINARY_OP(NAME, int) \
    REGISTER_BINARY_OP(NAME, uint) \
    REGISTER_BINARY_OP(NAME, double)
#define REGISTER_BINARY_OP_PREDICATE_ALL_TYPES(NAME) \
    REGISTER_BINARY_OP(NAME, int) \
    REGISTER_BINARY_OP(NAME, uint) \
    REGISTER_BINARY_OP(NAME, double) \
    REGISTER_BINARY_OP(NAME, string) \
    REGISTER_BINARY_OP(NAME, datetime) \
    REGISTER_BINARY_OP(NAME, time) \
    REGISTER_BINARY_OP(NAME, date) \
    REGISTER_BINARY_OP(NAME, timestamp)

static std::unordered_map<std::string, pb::PrimitiveType> return_type_map;

void FunctionManager::register_operators() {
    // ~ ! -1 -1.1
    register_object("bit_not_uint", bit_not_uint);
    register_object("logic_not_bool", logic_not_bool);
    register_object("minus_int", minus_int);
    register_object("minus_uint", minus_uint);
    register_object("minus_double", minus_double);
    // << >> & | ^ 
    REGISTER_BINARY_OP_ALL_TYPES(add);
    REGISTER_BINARY_OP_ALL_TYPES(minus);
    REGISTER_BINARY_OP_ALL_TYPES(multiplies);
    REGISTER_BINARY_OP_ALL_TYPES(divides);
    REGISTER_BINARY_OP(mod, int);
    REGISTER_BINARY_OP(mod, uint);
    // << >> & | ^
    REGISTER_BINARY_OP(left_shift, uint);
    REGISTER_BINARY_OP(right_shift, uint);
    REGISTER_BINARY_OP(bit_and, uint);
    REGISTER_BINARY_OP(bit_or, uint);
    REGISTER_BINARY_OP(bit_xor, uint);
    // ==  != > >= < <=
    REGISTER_BINARY_OP_PREDICATE_ALL_TYPES(eq);
    REGISTER_BINARY_OP_PREDICATE_ALL_TYPES(ne);
    REGISTER_BINARY_OP_PREDICATE_ALL_TYPES(gt);
    REGISTER_BINARY_OP_PREDICATE_ALL_TYPES(ge);
    REGISTER_BINARY_OP_PREDICATE_ALL_TYPES(lt);
    REGISTER_BINARY_OP_PREDICATE_ALL_TYPES(le);
    // && ||
    REGISTER_BINARY_OP(logic_and, bool);
    REGISTER_BINARY_OP(logic_or, bool);
    auto register_object_ret = [this](const std::string& name, 
            std::function<ExprValue(const std::vector<ExprValue>&)> T, 
            pb::PrimitiveType ret_type) {
        register_object(name, T);
        return_type_map[name] = ret_type;
    };
    // num funcs
    register_object_ret("round", round, pb::INT64);
    register_object_ret("floor", floor, pb::INT64);
    register_object_ret("ceil", ceil, pb::INT64);
    register_object_ret("ceiling", ceil, pb::INT64);

    // str funcs
    register_object_ret("length", length, pb::INT64);
    register_object_ret("upper", upper, pb::STRING);
    register_object_ret("lower", lower, pb::STRING);
    register_object_ret("concat", concat, pb::STRING);
    register_object_ret("substr", substr, pb::STRING);
    register_object_ret("left", left, pb::STRING);
    register_object_ret("right", right, pb::STRING);
    // date funcs
    register_object_ret("unix_timestamp", unix_timestamp, pb::UINT32);
    register_object_ret("from_unixtime", from_unixtime, pb::TIMESTAMP);
    register_object_ret("now", now, pb::DATETIME);
    register_object_ret("date_format", date_format, pb::STRING);
    register_object_ret("timediff", timediff, pb::TIME);
    register_object_ret("timestampdiff", timestampdiff, pb::INT64);
    // hll funcs
    register_object_ret("hll_add", hll_add, pb::HLL);
    register_object_ret("hll_merge", hll_merge, pb::HLL);
    register_object_ret("hll_estimate", hll_estimate, pb::INT64);
    register_object_ret("case_when", case_when, pb::STRING);
    register_object_ret("case_expr_when", case_expr_when, pb::STRING);
}

int FunctionManager::init() {
    register_operators();
    return 0;
}

int FunctionManager::complete_fn(pb::Function& fn, std::vector<pb::PrimitiveType> types) {
    switch (fn.fn_op()) {
        //predicate
        case parser::FT_EQ:
        case parser::FT_NE:
        case parser::FT_GE:
        case parser::FT_GT:
        case parser::FT_LE:
        case parser::FT_LT:
            if (has_timestamp(types)) {
                complete_fn(fn, 2, pb::TIMESTAMP, pb::BOOL);
            } else if (has_datetime(types)) {
                complete_fn(fn, 2, pb::DATETIME, pb::BOOL);
            } else if (has_date(types)) {
                complete_fn(fn, 2, pb::DATE, pb::BOOL);
            } else if (has_time(types)) {
                complete_fn(fn, 2, pb::TIME, pb::BOOL);
            } else if (has_double(types)) {
                complete_fn(fn, 2, pb::DOUBLE, pb::BOOL);
            } else if (has_uint(types)) {
                complete_fn(fn, 2, pb::UINT64, pb::BOOL);
            } else if (has_int(types)) {
                complete_fn(fn, 2, pb::INT64, pb::BOOL);
            } else {
                complete_fn(fn, 2, pb::STRING, pb::BOOL);
            }
            return 0;
            // binary
        case parser::FT_ADD:
        case parser::FT_MINUS:
        case parser::FT_MULTIPLIES:
            if (has_double(types)) {
                complete_fn(fn, 2, pb::DOUBLE, pb::DOUBLE);
            } else if (has_uint(types)) {
                complete_fn(fn, 2, pb::UINT64, pb::UINT64);
            } else {
                complete_fn(fn, 2, pb::INT64, pb::INT64);
            }
            return 0;
        case parser::FT_DIVIDES:
            complete_fn(fn, 2, pb::DOUBLE, pb::DOUBLE);
            return 0;
        case parser::FT_MOD:
            if (has_uint(types)) {
                complete_fn(fn, 2, pb::UINT64, pb::UINT64);
            } else {
                complete_fn(fn, 2, pb::INT64, pb::INT64);
            }
            return 0;
            // binary bit
        case parser::FT_BIT_AND:
        case parser::FT_BIT_OR:
        case parser::FT_BIT_XOR:
        case parser::FT_LS:
        case parser::FT_RS:
            complete_fn(fn, 2, pb::UINT64, pb::UINT64);
            return 0;
            // unary bit
        case parser::FT_BIT_NOT:
            complete_fn(fn, 1, pb::UINT64, pb::UINT64);
            return 0;
        case parser::FT_UMINUS:
            if (has_double(types)) {
                complete_fn(fn, 1, pb::DOUBLE, pb::DOUBLE);
            } else if (has_uint(types)) {
                complete_fn(fn, 1, pb::UINT64, pb::UINT64);
            } else {
                complete_fn(fn, 1, pb::INT64, pb::INT64);
            }
            return 0;
        case parser::FT_LOGIC_NOT:
            complete_fn(fn, 1, pb::BOOL, pb::BOOL);
            return 0;
        case parser::FT_LOGIC_AND:
        case parser::FT_LOGIC_OR:
        case parser::FT_LOGIC_XOR:
            complete_fn(fn, 2, pb::BOOL, pb::BOOL);
            return 0;
        case parser::FT_COMMON:
            fn.set_return_type(return_type_map[fn.name()]);
            return 0;
        /*
        case FUNC_LENGTH:
        case FUNC_LOWER:
        case FUNC_UPPER:
        case FUNC_CONCAT:
        case FUNC_LEFT:
        case FUNC_RIGHT:
        case FUNC_SUBSTR:
        case FUNC_SUBSTRING:
            complete_fn_simple(fn, 1, pb::STRING, pb::STRING);
            return 0;
        case FUNC_UNIX_TIMESTAMP:
            complete_fn_simple(fn, 0, pb::TIMESTAMP, pb::UINT32);
            return 0;
        case FUNC_FROM_UNIXTIME:
            complete_fn_simple(fn, 1, pb::UINT32, pb::TIMESTAMP);
            return 0;
        case FUNC_NOW:
            complete_fn_simple(fn, 0, pb::TIMESTAMP, pb::DATETIME);
            return 0;
        case FUNC_DATE_FORMAT:
            complete_fn_simple(fn, 1, pb::DATETIME, pb::STRING);
            return 0;
        case FUNC_HLL_ADD:
        case FUNC_HLL_MERGE:
            complete_fn_simple(fn, 1, pb::HLL, pb::HLL);
            return 0;
        case FUNC_HLL_ESTIMATE:
            complete_fn_simple(fn, 1, pb::HLL, pb::INT64);
            return 0;
        */
        default:
            //un-support
            return -1;
    }
}

void FunctionManager::complete_fn_simple(pb::Function& fn, int num_args, 
        pb::PrimitiveType arg_type, pb::PrimitiveType ret_type) {
    for (int i = 0; i < num_args; i++) {
        fn.add_arg_types(arg_type);
    }
    fn.set_return_type(ret_type);
}

void FunctionManager::complete_fn(pb::Function& fn, int num_args, 
        pb::PrimitiveType arg_type, pb::PrimitiveType ret_type) {
    fn.clear_arg_types();
    fn.clear_return_type();
    std::string arg_str;
    switch (arg_type) {
        case pb::DOUBLE:
            arg_str = "_double";
            break;
        case pb::INT64:
            arg_str = "_int";
            break;
        case pb::UINT64:
            arg_str = "_uint";
            break;
        case pb::BOOL:
            arg_str = "_bool";
            break;
        case pb::STRING:
            arg_str = "_string";
            break;
        case pb::DATETIME:
            arg_str = "_datetime";
            break;
        case pb::TIME:
            arg_str = "_time";
            break;
        case pb::DATE:
            arg_str = "_date";
            break;
        case pb::TIMESTAMP:
            arg_str = "_timestamp";
            break;
        default:
            break;
    }
    for (int i = 0; i < num_args; i++) {
        fn.add_arg_types(arg_type);
        fn.set_name(fn.name() + arg_str);
    }
    fn.set_return_type(ret_type);
}

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
