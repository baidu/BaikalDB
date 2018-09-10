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

#pragma once

#include <vector>
#include "expr_value.h"

namespace baikaldb {
//string functions
ExprValue length(const std::vector<ExprValue>& input);
ExprValue lower(const std::vector<ExprValue>& input);
ExprValue upper(const std::vector<ExprValue>& input);
ExprValue concat(const std::vector<ExprValue>& input);
ExprValue substr(const std::vector<ExprValue>& input);
ExprValue left(const std::vector<ExprValue>& input);
ExprValue right(const std::vector<ExprValue>& input);
// datetime functions
ExprValue unix_timestamp(const std::vector<ExprValue>& input);
ExprValue from_unixtime(const std::vector<ExprValue>& input);
ExprValue now(const std::vector<ExprValue>& input);
ExprValue date_format(const std::vector<ExprValue>& input);
ExprValue timediff(const std::vector<ExprValue>& input);
ExprValue timestampdiff(const std::vector<ExprValue>& input);
// hll functions
ExprValue hll_add(const std::vector<ExprValue>& input);
ExprValue hll_merge(const std::vector<ExprValue>& input);
ExprValue hll_estimate(const std::vector<ExprValue>& input);
// case when functions
ExprValue case_when(const std::vector<ExprValue>& input);
ExprValue case_expr_when(const std::vector<ExprValue>& input);
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
