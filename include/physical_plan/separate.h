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

#include "exec_node.h"
#include "query_context.h"
#include "schema_factory.h"

namespace baikaldb {

class FetcherNode;
class PacketNode;
class TransactionNode;

class Separate {
public:
    /* 分裂原则：
     * insert、delete、update、truncate 分裂packet
     * 无scan不分裂
     * 有agg则分裂agg
     * 有sort则分裂sort
     * 有limit则分裂limit
     * 分裂packet
     */
    int analyze(QueryContext* ctx);

private:
    int seperate_for_join(const std::vector<ExecNode*>& join_nodes);
    int separate_begin(QueryContext* ctx, TransactionNode* txn_node);
    int separate_commit(QueryContext* ctx, TransactionNode* txn_node);
    int separate_rollback(QueryContext* ctx, TransactionNode* txn_node);
    int separate_autocommit_dml_1pc(QueryContext* ctx, PacketNode* packet_node, 
        std::map<int64_t, pb::RegionInfo>&);
    int separate_autocommit_dml_2pc(QueryContext* ctx, PacketNode* packet_node, 
        std::map<int64_t, pb::RegionInfo>&);

    FetcherNode* create_fetcher_node(pb::OpType op_type);
    TransactionNode* create_txn_node(pb::TxnCmdType cmd_type);
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
