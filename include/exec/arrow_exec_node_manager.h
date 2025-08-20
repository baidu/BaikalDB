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

#include <string>
#include <map>
#include <unordered_map>
#include "join_node.h"
#include "exchange_sender_node.h"
#include "arrow/acero/exec_plan.h"

namespace baikaldb {
class IndexCollectorNodeOptions : public arrow::acero::ExecNodeOptions {
public:
    explicit IndexCollectorNodeOptions(RuntimeState* s, JoinNode* join, int64_t limit)
         : state(s), join_node(join), limit_cnt(limit) {}
    ~IndexCollectorNodeOptions() {
        state = nullptr;
        join_node = nullptr;
    }
    RuntimeState* state = nullptr;
    JoinNode* join_node = nullptr;
    int64_t limit_cnt = -1;
};

class UnorderedFetchNodeOptions : public arrow::acero::ExecNodeOptions {
public:
    explicit UnorderedFetchNodeOptions(int64_t offset, int64_t limit)
         : offset_cnt(offset), limit_cnt(limit) {}
    int64_t offset_cnt = -1;
    int64_t limit_cnt = -1;
};

class AceroExchangeSenderNodeOptions : public arrow::acero::ExecNodeOptions {
public:
    explicit AceroExchangeSenderNodeOptions(RuntimeState* s, ExchangeSenderNode* sender, int64_t limit) 
        : state(s), exchange_sender_node(sender), limit_cnt(limit) {}
    ~AceroExchangeSenderNodeOptions() {
    }
    RuntimeState* state = nullptr;
    ExchangeSenderNode* exchange_sender_node = nullptr;
    int64_t limit_cnt = -1;
};

class MakeDefaultAggRowWhenNoInputOptions : public arrow::acero::ExecNodeOptions {
public:
    explicit MakeDefaultAggRowWhenNoInputOptions(std::unordered_set<std::string>& count_name) : count_names(count_name) {}
    std::unordered_set<std::string> count_names;
};

class TopKNodeOptions : public arrow::acero::ExecNodeOptions {
 public:
  static constexpr std::string_view kName = "top_k";
  explicit TopKNodeOptions(arrow::compute::Ordering ordering, int64_t k_num) : ordering(std::move(ordering)), k(k_num) {}

  /// \brief The new ordering to apply to outgoing data
  arrow::compute::Ordering ordering;
  int64_t k = -1;
};

class ArrowExecNodeManager {
public:
    static int RegisterAllArrowExecNode();
}; 
}  // namespace baikal
/* vim: set ts=4 sw=4 sts=4 tw=100 */
