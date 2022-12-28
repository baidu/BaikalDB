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
    
#define REGISTER_SWAP_PREDICATE(NAME1, NAME2, TYPE) \
    REGISTER_BINARY_OP(NAME1, TYPE) \
    predicate_swap_map[#NAME1"_"#TYPE"_"#TYPE] = #NAME2"_"#TYPE"_"#TYPE;
#define REGISTER_SWAP_PREDICATE_ALL_TYPES(NAME1, NAME2) \
    predicate_swap_map[#NAME1] = #NAME2; \
    REGISTER_SWAP_PREDICATE(NAME1, NAME2, int) \
    REGISTER_SWAP_PREDICATE(NAME1, NAME2, uint) \
    REGISTER_SWAP_PREDICATE(NAME1, NAME2, double) \
    REGISTER_SWAP_PREDICATE(NAME1, NAME2, string) \
    REGISTER_SWAP_PREDICATE(NAME1, NAME2, datetime) \
    REGISTER_SWAP_PREDICATE(NAME1, NAME2, time) \
    REGISTER_SWAP_PREDICATE(NAME1, NAME2, date) \
    REGISTER_SWAP_PREDICATE(NAME1, NAME2, timestamp) 

static std::unordered_map<std::string, pb::PrimitiveType> return_type_map;
static std::unordered_map<std::string, std::string> predicate_swap_map;

bool FunctionManager::swap_op(pb::Function& fn) {
    if (predicate_swap_map.count(fn.name()) == 1) {
        fn.set_name(predicate_swap_map[fn.name()]);
        switch (fn.fn_op()) {
            case parser::FT_GE:
                fn.set_fn_op(parser::FT_LE);
                break;
            case parser::FT_GT:
                fn.set_fn_op(parser::FT_LT);
                break;
            case parser::FT_LE:
                fn.set_fn_op(parser::FT_GE);
                break;
            case parser::FT_LT:
                fn.set_fn_op(parser::FT_GT);
                break;
        }
        return true;
    }
    return false;
}

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
    REGISTER_SWAP_PREDICATE_ALL_TYPES(eq, eq);
    REGISTER_SWAP_PREDICATE_ALL_TYPES(ne, ne);
    REGISTER_SWAP_PREDICATE_ALL_TYPES(gt, lt);
    REGISTER_SWAP_PREDICATE_ALL_TYPES(ge, le);
    REGISTER_SWAP_PREDICATE_ALL_TYPES(lt, gt);
    REGISTER_SWAP_PREDICATE_ALL_TYPES(le, ge);
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
    register_object_ret("round", round, pb::DOUBLE);
    register_object_ret("floor", floor, pb::INT64);
    register_object_ret("abs", abs, pb::DOUBLE);
    register_object_ret("sqrt", sqrt, pb::DOUBLE);
    register_object_ret("mod", mod, pb::DOUBLE);
    register_object_ret("rand", rand, pb::DOUBLE);
    register_object_ret("sign", sign, pb::INT64);
    register_object_ret("sin", sin, pb::DOUBLE);
    register_object_ret("asin", asin, pb::DOUBLE);
    register_object_ret("cos", cos, pb::DOUBLE);
    register_object_ret("acos", acos, pb::DOUBLE);
    register_object_ret("tan", tan, pb::DOUBLE);
    register_object_ret("cot", cot, pb::DOUBLE);
    register_object_ret("atan", atan, pb::DOUBLE);
    register_object_ret("ln", ln, pb::DOUBLE);
    register_object_ret("log", log, pb::DOUBLE);
    register_object_ret("pi", pi, pb::DOUBLE);
    register_object_ret("pow", pow, pb::DOUBLE);
    register_object_ret("power", pow, pb::DOUBLE);
    register_object_ret("greatest", greatest, pb::DOUBLE);
    register_object_ret("least", least, pb::DOUBLE);
    register_object_ret("ceil", ceil, pb::INT64);
    register_object_ret("ceiling", ceil, pb::INT64);

    // str funcs
    register_object_ret("length", length, pb::INT64);
    register_object_ret("bit_length", bit_length, pb::INT64);
    register_object_ret("upper", upper, pb::STRING);
    register_object_ret("lower", lower, pb::STRING);
    register_object_ret("lower_gbk", lower_gbk, pb::STRING);
    register_object_ret("ucase", upper, pb::STRING);
    register_object_ret("lcase", lower, pb::STRING);
    register_object_ret("concat", concat, pb::STRING);
    register_object_ret("substr", substr, pb::STRING);
    register_object_ret("left", left, pb::STRING);
    register_object_ret("right", right, pb::STRING);
    register_object_ret("trim", trim, pb::STRING);
    register_object_ret("ltrim", ltrim, pb::STRING);
    register_object_ret("rtrim", rtrim, pb::STRING);
    register_object_ret("concat_ws", concat_ws, pb::STRING);
    register_object_ret("ascii", ascii, pb::INT32);
    register_object_ret("strcmp", strcmp, pb::INT32);
    register_object_ret("insert", insert, pb::STRING);
    register_object_ret("replace", replace, pb::STRING);
    register_object_ret("repeat", repeat, pb::STRING);
    register_object_ret("reverse", reverse, pb::STRING);
    register_object_ret("locate", locate, pb::INT32);
    register_object_ret("substring_index", substring_index, pb::STRING);
    register_object_ret("lpad", lpad, pb::STRING);
    register_object_ret("rpad", rpad, pb::STRING);
    register_object_ret("instr", instr, pb::INT32);
    register_object_ret("json_extract", json_extract, pb::STRING);

    // date funcs
    register_object_ret("unix_timestamp", unix_timestamp, pb::INT64);
    register_object_ret("from_unixtime", from_unixtime, pb::TIMESTAMP);
    register_object_ret("now", now, pb::DATETIME);
    register_object_ret("sysdate", now, pb::DATETIME);
    register_object_ret("utc_timestamp", utc_timestamp, pb::DATETIME);
    register_object_ret("date_format", date_format, pb::STRING);
    /*
        str_to_date实现较为复杂，需要满足任意格式的string转换为标准形式的DATETIME，现在为了方便确保str_to_date可以使用，
        默认string是标准形式的date，故其实现内容和date_format函数一致
    */ 
    register_object_ret("str_to_date", str_to_date, pb::DATETIME);
    register_object_ret("time_format", time_format, pb::STRING);
    register_object_ret("timediff", timediff, pb::TIME);
    register_object_ret("timestampdiff", timestampdiff, pb::INT64);
    register_object_ret("convert_tz", convert_tz, pb::STRING);
    register_object_ret("curdate", curdate, pb::DATE);
    register_object_ret("current_date", current_date, pb::DATE);
    register_object_ret("curtime", curtime, pb::TIME);
    register_object_ret("current_time", current_time, pb::TIME);
    register_object_ret("current_timestamp", current_timestamp, pb::TIMESTAMP);
    register_object_ret("timestamp", timestamp, pb::TIMESTAMP);
    register_object_ret("day", day, pb::UINT32);
    register_object_ret("dayname", dayname, pb::STRING);
    register_object_ret("dayofweek", dayofweek, pb::UINT32);
    register_object_ret("dayofmonth", dayofmonth, pb::UINT32);
    register_object_ret("dayofyear", dayofyear, pb::UINT32);
    register_object_ret("yearweek", yearweek, pb::UINT32);
    register_object_ret("week", week, pb::UINT32);
    register_object_ret("weekofyear", weekofyear, pb::UINT32);
    register_object_ret("month", month, pb::UINT32);
    register_object_ret("monthname", monthname, pb::STRING);
    register_object_ret("year", year, pb::UINT32);
    register_object_ret("time_to_sec", time_to_sec, pb::UINT32);
    register_object_ret("sec_to_time", sec_to_time, pb::TIME);
    register_object_ret("weekday", weekday, pb::UINT32);
    register_object_ret("datediff", datediff, pb::UINT32);
    register_object_ret("date_add", date_add, pb::DATETIME);
    register_object_ret("date_sub", date_sub, pb::DATETIME);
    register_object_ret("extract", extract, pb::UINT32);
    register_object_ret("tso_to_timestamp", tso_to_timestamp, pb::DATETIME);
    register_object_ret("timestamp_to_tso", timestamp_to_tso, pb::INT64);
    // hll funcs
    register_object_ret("hll_add", hll_add, pb::HLL);
    register_object_ret("hll_merge", hll_merge, pb::HLL);
    register_object_ret("hll_estimate", hll_estimate, pb::INT64);
    register_object_ret("hll_init", hll_init, pb::HLL);
    // condition
    register_object_ret("case_when", case_when, pb::STRING);
    register_object_ret("case_expr_when", case_expr_when, pb::STRING);
    register_object_ret("if", if_, pb::STRING);
    register_object_ret("ifnull", ifnull, pb::STRING);
    register_object_ret("nullif", nullif, pb::STRING);
    register_object_ret("isnull", isnull, pb::BOOL);
    // MurmurHash sign
    register_object_ret("murmur_hash", murmur_hash, pb::UINT64);
    register_object_ret("md5", md5, pb::STRING);
    register_object_ret("sha", md5, pb::STRING);
    register_object_ret("sha1", md5, pb::STRING);
    // bitmap funcs
    register_object_ret("rb_build", rb_build, pb::BITMAP);
    register_object_ret("rb_and", rb_and, pb::BITMAP);
    //register_object_ret("rb_and_cardinality", rb_and_cardinality, pb::UINT64);
    register_object_ret("rb_or", rb_or, pb::BITMAP);
    //register_object_ret("rb_or_cardinality", rb_or_cardinality, pb::UINT64);
    register_object_ret("rb_xor", rb_xor, pb::BITMAP);
    //register_object_ret("rb_xor_cardinality", rb_xor_cardinality, pb::UINT64);
    register_object_ret("rb_andnot", rb_andnot, pb::BITMAP);
    //register_object_ret("rb_andnot_cardinality", rb_andnot_cardinality, pb::UINT64);
    register_object_ret("rb_cardinality", rb_cardinality, pb::UINT64);
    register_object_ret("rb_empty", rb_empty, pb::BOOL);
    register_object_ret("rb_equals", rb_equals, pb::BOOL);
    //register_object_ret("rb_not_equals", rb_not_equals, pb::BOOL);
    register_object_ret("rb_intersect", rb_intersect, pb::BOOL);
    register_object_ret("rb_contains", rb_contains, pb::BOOL);
    register_object_ret("rb_contains_range", rb_contains_range, pb::BOOL);
    register_object_ret("rb_add", rb_add, pb::BITMAP);
    register_object_ret("rb_add_range", rb_add_range, pb::BITMAP);
    register_object_ret("rb_remove", rb_remove, pb::BITMAP);
    register_object_ret("rb_remove_range", rb_remove_range, pb::BITMAP);
    register_object_ret("rb_flip", rb_flip, pb::BITMAP);
    register_object_ret("rb_flip_range", rb_flip_range, pb::BITMAP);
    register_object_ret("rb_minimum", rb_minimum, pb::UINT32);
    register_object_ret("rb_maximum", rb_maximum, pb::UINT32);
    register_object_ret("rb_rank", rb_rank, pb::UINT32);
    register_object_ret("rb_jaccard_index", rb_jaccard_index, pb::DOUBLE);
    // tdigest funcs
    register_object_ret("tdigest_build", tdigest_build, pb::TDIGEST);
    register_object_ret("tdigest_add", tdigest_add, pb::TDIGEST);
    register_object_ret("tdigest_merge", tdigest_merge, pb::TDIGEST);
    register_object_ret("tdigest_total_sum", tdigest_total_sum, pb::DOUBLE);
    register_object_ret("tdigest_total_count", tdigest_total_count, pb::DOUBLE);
    register_object_ret("tdigest_percentile", tdigest_percentile, pb::DOUBLE);
    register_object_ret("tdigest_location", tdigest_location, pb::DOUBLE);

    register_object_ret("version", version, pb::STRING);
    register_object_ret("last_insert_id", last_insert_id, pb::INT64);
    //
    register_object_ret("point_distance", point_distance, pb::INT64);
    register_object_ret("cast_to_date", cast_to_date, pb::DATE);
    register_object_ret("cast_to_time", cast_to_time, pb::TIME);
    register_object_ret("cast_to_datetime", cast_to_datetime, pb::DATETIME);
    register_object_ret("cast_to_string", cast_to_string, pb::STRING);
    register_object_ret("cast_to_signed", cast_to_signed, pb::INT64);
    register_object_ret("cast_to_unsigned", cast_to_unsigned, pb::INT64);
    register_object_ret("cast_to_double", cast_to_double, pb::DOUBLE);
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
            if (all_int(types)) {
                if (has_uint(types)) {
                    complete_fn(fn, 2, pb::UINT64, pb::BOOL);
                } else {
                    complete_fn(fn, 2, pb::INT64, pb::BOOL);
                }
            } else if (has_datetime(types)) {
                complete_fn(fn, 2, pb::DATETIME, pb::BOOL);
            } else if (has_timestamp(types)) {
                complete_fn(fn, 2, pb::TIMESTAMP, pb::BOOL);
            } else if (has_date(types)) {
                complete_fn(fn, 2, pb::DATE, pb::BOOL);
            } else if (has_time(types)) {
                complete_fn(fn, 2, pb::TIME, pb::BOOL);
            } else if (has_double(types)) {
                complete_fn(fn, 2, pb::DOUBLE, pb::BOOL);
            } else if (has_int(types)) {
                complete_fn(fn, 2, pb::DOUBLE, pb::BOOL);
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
            complete_common_fn(fn, types);
            return 0;
        case parser::FT_MATCH_AGAINST:
            complete_common_fn(fn, types);
            return 0;
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
    if (fn.return_type() == ret_type) {
        // 避免prepare模式反复执行此函数,导致fn.name没有清理的bug
        return;
    }
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
        fn.set_name(fn.name() + arg_str); // 此处name没有清理,反复执行会导致内容增长
    }
    fn.set_return_type(ret_type);
}

void FunctionManager::complete_common_fn(pb::Function& fn, std::vector<pb::PrimitiveType>& types) {
    if (fn.name() == "case_when" || fn.name() == "case_expr_when") {
        size_t index = 0;
        size_t remainder = 1;
        std::vector<pb::PrimitiveType> target_types;
        pb::PrimitiveType ret_type = pb::STRING;
        if (fn.name() == "case_expr_when") {
            remainder = 0;
        }
        for (auto& c : types) {
            (void)c;
            //case_when then子句index为奇数，else子句index为最后一位
            //case_when_expr then子句index为除第0位的偶数，else子句为最后一位
            if (index != 0 && (index % 2 == remainder || index + 1 == types.size())) {
                DB_DEBUG("push col_type : [%s]", pb::PrimitiveType_Name(types[index]).c_str());
                target_types.push_back(types[index]);
            }
            ++index;
        }
        if (!has_merged_type(target_types, ret_type)) {
            DB_WARNING("no merged type.");
        }
        DB_DEBUG("merge type : [%s]", pb::PrimitiveType_Name(ret_type).c_str());
        fn.set_return_type(ret_type);

    } else if (fn.name() == "if") {
        std::vector<pb::PrimitiveType> target_types;
        pb::PrimitiveType ret_type = pb::STRING;
        if (types.size() == 3) {
            target_types.push_back(types[1]);
            target_types.push_back(types[2]);
            has_merged_type(target_types, ret_type);
        }
        DB_DEBUG("merge type : [%s]", pb::PrimitiveType_Name(ret_type).c_str());
        fn.set_return_type(ret_type);
    } else if (fn.name() == "ifnull" || fn.name() == "nullif") {
        std::vector<pb::PrimitiveType> target_types;
        pb::PrimitiveType ret_type = pb::STRING;
        if (types.size() == 2) {
            target_types.push_back(types[0]);
            target_types.push_back(types[1]);
            has_merged_type(target_types, ret_type);
        }
        DB_DEBUG("merge type : [%s]", pb::PrimitiveType_Name(ret_type).c_str());
        fn.set_return_type(ret_type);
    } else if (fn.name() == "match_against") {
        fn.set_return_type(pb::BOOL);
    }
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
