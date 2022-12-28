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
#include "rocksdb_scan_node.h"
#include "insert_node.h"
#include "truncate_node.h"
#include "kill_node.h"
#include "query_context.h"
#include "transaction_node.h"
#include "schema_factory.h"

namespace baikaldb {
class PlanRouter {
public:
    /* 通过主键索引获取所在的regions
     */
    int analyze(QueryContext* ctx);
    int scan_plan_router(RocksdbScanNode* scan_node,  
        const std::function<int32_t(int32_t, int32_t)>& get_slot_id,
        const std::function<pb::TupleDescriptor*(int32_t)>& get_tuple_desc,
        bool has_join,
        const std::set<ExecNode*>& escape_get_region_infos);

    int scan_node_analyze(RocksdbScanNode* scan_node, 
        QueryContext* ctx, bool has_join, const std::set<ExecNode*>& escape_get_region_infos);
 
private:
    template<typename T>
    int insert_node_analyze(T* node, QueryContext* ctx); 
   
    int truncate_node_analyze(TruncateNode* trunc_node, QueryContext* ctx);
    int kill_node_analyze(KillNode* kill_node, QueryContext* ctx);
    int transaction_node_analyze(TransactionNode* txn_node, QueryContext* ctx);
    int select_index(pb::ScanNode* scan_node, std::vector<int>& multi_reverse_index);
    bool _is_full_export = false;
};

class PartitionAnalyze {
public:
    int analyze(QueryContext* ctx);
};

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
