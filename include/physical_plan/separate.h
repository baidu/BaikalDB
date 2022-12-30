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
#include "schema_factory.h"
#include "insert_manager_node.h"
#include "insert_node.h"
#include "update_manager_node.h"
#include "update_node.h"
#include "delete_manager_node.h"
#include "delete_node.h"

namespace baikaldb {

class FetcherNode;
class PacketNode;
class TransactionNode;
class SelectManagerNode;

class Separate {
public:
enum NodeMode {
    BOTH,
    PRIMARY,
    GLOBAL
};
    /* 分裂原则：
     * insert、delete、update、truncate 分裂packet
     * 无scan不分裂
     * 有agg则分裂agg
     * 有sort则分裂sort
     * 有limit则分裂limit
     * 分裂packet
     */
    int analyze(QueryContext* ctx);
    TransactionNode* create_txn_node(pb::TxnCmdType cmd_type, int64_t txn_lock_timeout = -1);

private:
    int separate_union(QueryContext* ctx);
    int separate_load(QueryContext* ctx);
    int separate_insert(QueryContext* ctx);
    int separate_update(QueryContext* ctx);
    int separate_delete(QueryContext* ctx);

    template<typename T>
    int separate_single_txn(QueryContext* ctx, T* node, pb::OpType op_type);

    int separate_truncate(QueryContext* ctx);
    int separate_kill(QueryContext* ctx);
    int separate_commit(QueryContext* ctx);
    int separate_rollback(QueryContext* ctx);
    int separate_begin(QueryContext* ctx);
    int separate_select(QueryContext* ctx);
    int separate_simple_select(QueryContext* ctx, ExecNode* plan);
    int separate_apply(QueryContext* ctx, const std::vector<ExecNode*>& apply_nodes);
    int separate_join(QueryContext* ctx, const std::vector<ExecNode*>& join_nodes);

    int separate_global_insert(InsertManagerNode* manager_node, InsertNode* insert_node);
    int separate_global_delete(DeleteManagerNode* manager_node, DeleteNode* delete_node, ExecNode* scan_node);
    int separate_global_update(UpdateManagerNode* manager_node, UpdateNode* update_node, ExecNode* scan_node);
    //mode:0, 生成所有index的node
    //mode:1, 只生成主键的node
    //mode:2, 只生成全局索引表的node
    int create_lock_node(int64_t table_id, pb::LockCmdType lock_type, NodeMode mode, ExecNode* manager_node);
    //生成指定索引的node, update时适用
    int create_lock_node(int64_t table_id, pb::LockCmdType lock_type, NodeMode mode, 
            const std::vector<int64_t>& global_affected_indexs,
            const std::vector<int64_t>& local_affected_indexs, 
            ExecNode* manager_node);

    int create_full_export_node(ExecNode* plan);

    SelectManagerNode* create_select_manager_node();
    bool need_separate_single_txn(QueryContext* ctx, const int64_t main_table_id);
    bool need_separate_plan(QueryContext* ctx, const int64_t main_table_id); 

    SchemaFactory* _factory = SchemaFactory::get_instance();
    int64_t _row_ttl_duration = 0;
    bool _is_first_full_export = true;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
