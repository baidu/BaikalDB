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

#include "operators.h"

namespace baikaldb {
#define UNARY_OP_FN(NAME, TYPE, PRIMITIVE_TYPE, VAL, OP) \
    ExprValue NAME##_##TYPE(const std::vector<ExprValue>& input) { \
        if (input[0].is_null()) { \
            return ExprValue::Null(); \
        } \
        ExprValue ret(PRIMITIVE_TYPE); \
        ret._u.VAL = OP input[0]._u.VAL; \
        return ret; \
    }

//UNARY_OP_FN(bit_not, int, pb::INT64, int64_val, ~);
UNARY_OP_FN(bit_not, uint, pb::UINT64, uint64_val, ~);
UNARY_OP_FN(logic_not, bool, pb::BOOL, bool_val, !);
UNARY_OP_FN(minus, int, pb::INT64, int64_val, -);
UNARY_OP_FN(minus, uint, pb::INT64, int64_val, -);
UNARY_OP_FN(minus, double, pb::DOUBLE, double_val, -);

#define BINARY_OP_FN(NAME, TYPE, PRIMITIVE_TYPE, VAL, OP) \
    ExprValue NAME##_##TYPE##_##TYPE(const std::vector<ExprValue>& input) { \
        if (input[0].is_null() || input[1].is_null()) { \
            return ExprValue::Null(); \
        } \
        ExprValue ret(PRIMITIVE_TYPE); \
        ret._u.VAL = input[0]._u.VAL OP input[1]._u.VAL; \
        return ret; \
    }
#define BINARY_OP_ALL_TYPES_FN(NAME, OP) \
    BINARY_OP_FN(NAME, int, pb::INT64, int64_val, OP); \
    BINARY_OP_FN(NAME, uint, pb::UINT64, uint64_val, OP); \
    BINARY_OP_FN(NAME, double, pb::DOUBLE, double_val, OP);
// + - *
BINARY_OP_ALL_TYPES_FN(add, +);
BINARY_OP_ALL_TYPES_FN(minus, -);
BINARY_OP_ALL_TYPES_FN(multiplies, *);

#define BINARY_OP_ZERO_FN(NAME, TYPE, PRIMITIVE_TYPE, VAL, OP) \
    ExprValue NAME##_##TYPE##_##TYPE(const std::vector<ExprValue>& input) { \
        if (input[0].is_null() || input[1].is_null() || input[1]._u.VAL == 0) { \
            return ExprValue::Null(); \
        } \
        ExprValue ret(PRIMITIVE_TYPE); \
        ret._u.VAL = input[0]._u.VAL OP input[1]._u.VAL; \
        return ret; \
    }
#define BINARY_OP_ALL_TYPES_ZERO_FN(NAME, OP) \
    BINARY_OP_ZERO_FN(NAME, int, pb::INT64, int64_val, OP); \
    BINARY_OP_ZERO_FN(NAME, uint, pb::UINT64, uint64_val, OP); \
    BINARY_OP_ZERO_FN(NAME, double, pb::DOUBLE, double_val, OP);
// / %
BINARY_OP_ALL_TYPES_ZERO_FN(divides, /);
BINARY_OP_ZERO_FN(mod, int, pb::INT64, int64_val, %);
BINARY_OP_ZERO_FN(mod, uint, pb::UINT64, uint64_val, %);
// << >> & | ^
//BINARY_OP_FN(left_shift, int, pb::INT64, int64_val, <<);
BINARY_OP_FN(left_shift, uint, pb::UINT64, uint64_val, <<);
BINARY_OP_FN(right_shift, uint, pb::UINT64, uint64_val, >>);
BINARY_OP_FN(bit_and, uint, pb::UINT64, uint64_val, &);
BINARY_OP_FN(bit_or, uint, pb::UINT64, uint64_val, |);
BINARY_OP_FN(bit_xor, uint, pb::UINT64, uint64_val, ^);

#define BINARY_OP_PREDICATE_FN(NAME, TYPE, VAL, OP) \
    ExprValue NAME##_##TYPE##_##TYPE(const std::vector<ExprValue>& input) { \
        if (input[0].is_null() || input[1].is_null()) { \
            return ExprValue::Null(); \
        } \
        ExprValue ret(pb::BOOL); \
        ret._u.bool_val = input[0].VAL OP input[1].VAL; \
        return ret; \
    }
#define BINARY_OP_PREDICATE_ALL_TYPES_FN(NAME, OP) \
    BINARY_OP_PREDICATE_FN(NAME, int, _u.int64_val, OP); \
    BINARY_OP_PREDICATE_FN(NAME, uint, _u.uint64_val, OP); \
    BINARY_OP_PREDICATE_FN(NAME, double, _u.double_val, OP); \
    BINARY_OP_PREDICATE_FN(NAME, string, str_val, OP); \
    BINARY_OP_PREDICATE_FN(NAME, datetime, _u.uint64_val, OP); \
    BINARY_OP_PREDICATE_FN(NAME, time, _u.int32_val, OP); \
    BINARY_OP_PREDICATE_FN(NAME, date, _u.uint32_val, OP); \
    BINARY_OP_PREDICATE_FN(NAME, timestamp, _u.uint32_val, OP);

// == != > >= < <=
BINARY_OP_PREDICATE_ALL_TYPES_FN(eq, ==);
BINARY_OP_PREDICATE_ALL_TYPES_FN(ne, !=);
BINARY_OP_PREDICATE_ALL_TYPES_FN(gt, >);
BINARY_OP_PREDICATE_ALL_TYPES_FN(ge, >=);
BINARY_OP_PREDICATE_ALL_TYPES_FN(lt, <);
BINARY_OP_PREDICATE_ALL_TYPES_FN(le, <=);
// && || ; not used, see predicate.h
//BINARY_OP_PREDICATE_FN(logic_and, bool, _u.bool_val, &&);
//BINARY_OP_PREDICATE_FN(logic_or, bool, _u.bool_val, ||);
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
