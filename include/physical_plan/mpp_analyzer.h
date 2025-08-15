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

#include "exec_node.h"
#include "query_context.h"
#include "select_manager_node.h"
#include "exchange_receiver_node.h"
#include "exchange_sender_node.h"
#include "dual_scan_node.h"
#include "fragment.h"

namespace baikaldb {
class MppAnalyzer {
public:
    int analyze(QueryContext* ctx);

private:
    void restore_sort_pushdown_for_join(QueryContext* ctx);

    int create_exchange_node_pair(QueryContext* ctx, 
                                  NodePartitionProperty* parent,
                                  ExchangeReceiverNode** receiver, 
                                  ExchangeSenderNode** sender,
                                  bool need_use_broadcast_type = false);

    int add_exchange(QueryContext* ctx, SmartFragment& fragment);

    int add_exchange_and_separate(QueryContext* ctx, SmartFragment& fragment, ExecNode* node, NodePartitionProperty* parent_property);

    int seperate_store_fragment(QueryContext* ctx,
                                  SmartFragment& fragment, 
                                  ExecNode* select_manager, 
                                  NodePartitionProperty* parent_property,
                                  bool need_use_broadcast_type = false);

    int seperate_db_fragment(QueryContext* ctx, SmartFragment& fragment, ExecNode* node, NodePartitionProperty* parent_property);

    int check_need_add_exchange(NodePartitionProperty* parent_propety, NodePartitionProperty* node_property, bool* need_add_exchange);

    int optimize_fragment(QueryContext* ctx, SmartFragment& fragment);

    int build_fragment(QueryContext* ctx, SmartFragment& fragment);

    int do_separate(QueryContext* ctx, 
                    ExecNode* node, 
                    std::vector<ExecNode*>& child_fragment_exchange_senders);

    int add_exchange_and_separate_for_index_join_inner_node(QueryContext* ctx, 
                    SmartFragment& fragment,
                    ExecNode* node,
                    NodePartitionProperty* parent_property,
                    std::set<ExecNode*>& need_runtime_filter_select_manager_nodes,
                    bool need_use_broadcast_type);                              

    void clear_mpp_flag(QueryContext* ctx);                  
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
