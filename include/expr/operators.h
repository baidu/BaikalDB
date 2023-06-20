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

#include <vector>
#include "expr_value.h"

namespace baikaldb {
//~ ! -1 -1.1
//ExprValue bit_not_int(const std::vector<ExprValue>& input);
ExprValue bit_not_uint(const std::vector<ExprValue>& input);
ExprValue logic_not_bool(const std::vector<ExprValue>& input);
ExprValue minus_int(const std::vector<ExprValue>& input);
ExprValue minus_uint(const std::vector<ExprValue>& input);
ExprValue minus_double(const std::vector<ExprValue>& input);

#define BINARY_OP_DEFINE(NAME, TYPE) \
    ExprValue NAME##_##TYPE##_##TYPE(const std::vector<ExprValue>& input);
#define BINARY_OP_ALL_TYPES_DEFINE(NAME) \
    BINARY_OP_DEFINE(NAME, int); \
    BINARY_OP_DEFINE(NAME, uint); \
    BINARY_OP_DEFINE(NAME, double);
#define BINARY_OP_PREDICATE_ALL_TYPES_DEFINE(NAME) \
    BINARY_OP_DEFINE(NAME, int); \
    BINARY_OP_DEFINE(NAME, uint); \
    BINARY_OP_DEFINE(NAME, double); \
    BINARY_OP_DEFINE(NAME, string); \
    BINARY_OP_DEFINE(NAME, datetime); \
    BINARY_OP_DEFINE(NAME, time); \
    BINARY_OP_DEFINE(NAME, date); \
    BINARY_OP_DEFINE(NAME, timestamp); 
// + - * /
BINARY_OP_ALL_TYPES_DEFINE(add);
BINARY_OP_ALL_TYPES_DEFINE(minus);
BINARY_OP_ALL_TYPES_DEFINE(multiplies);
BINARY_OP_ALL_TYPES_DEFINE(divides);
// % << >> & | ^
// 位运算全部作为uint64，和mysql保持一致
BINARY_OP_DEFINE(mod, int);
BINARY_OP_DEFINE(mod, uint);
BINARY_OP_DEFINE(left_shift, uint);
BINARY_OP_DEFINE(right_shift, uint);
BINARY_OP_DEFINE(bit_and, uint);
BINARY_OP_DEFINE(bit_or, uint);
BINARY_OP_DEFINE(bit_xor, uint);
// == != > >= < <=
BINARY_OP_PREDICATE_ALL_TYPES_DEFINE(eq);
BINARY_OP_PREDICATE_ALL_TYPES_DEFINE(ne);
BINARY_OP_PREDICATE_ALL_TYPES_DEFINE(gt);
BINARY_OP_PREDICATE_ALL_TYPES_DEFINE(ge);
BINARY_OP_PREDICATE_ALL_TYPES_DEFINE(lt);
BINARY_OP_PREDICATE_ALL_TYPES_DEFINE(le);
// && || xor
//BINARY_OP_DEFINE(logic_and, bool);
//BINARY_OP_DEFINE(logic_or, bool);
//BINARY_OP_DEFINE(logic_xor, bool);
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
