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

#include "expr_node.h"

namespace baikaldb {
namespace range {
enum RangeType {
    NONE, 
    RANGE,
    EQ,
    LIKE,
    LIKE_EQ,
    LIKE_PREFIX,
    MATCH_LANGUAGE,
    MATCH_BOOLEAN,
    MATCH_VECTOR,
    OR_LIKE,
    IN,
    INDEX_HAS_NULL
};

struct FieldRange {
    RangeType type = NONE;
    bool is_exact_like = false; // EXACT_LIKE，内部语法，ES迁移用
    bool is_row_expr = false; // (x,y) > (1,2) 类表达式
    bool left_open = false;
    bool right_open = false;
    std::vector<int32_t> left_row_field_ids;
    std::vector<int32_t> right_row_field_ids;
    ExprNode* left_expr = nullptr;
    ExprNode* right_expr = nullptr;
    std::vector<ExprValue> left;
    std::vector<ExprValue> right;
    std::vector<ExprValue> eq_in_values;
    std::vector<ExprValue> like_values;
    std::set<ExprNode*> conditions;
};

struct IndexRange {
    RangeType type = NONE;
    bool is_exact_like = false; // EXACT_LIKE，内部语法，ES迁移用
    bool is_row_expr = false; // (x,y) > (1,2) 类表达式
    bool left_open = false;
    bool right_open = false;
    std::vector<int32_t> left_row_field_ids;
    std::vector<int32_t> right_row_field_ids;
    ExprNode* left_expr = nullptr;
    ExprNode* right_expr = nullptr;
    std::vector<ExprValue> left;
    std::vector<ExprValue> right;
    std::vector<ExprValue> eq_in_values;
    std::vector<ExprValue> like_values;
    std::set<ExprNode*> conditions;
};
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
