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

#include "plan_router.h"
#include "network_socket.h"

namespace baikaldb {
int PlanRouter::analyze(QueryContext* ctx) {
    ExecNode* plan = ctx->root;
    if (!plan->need_seperate()) {
        return 0;
    }
    std::vector<ExecNode*> scan_nodes;
    plan->get_node(pb::SCAN_NODE, scan_nodes);
    InsertNode* insert_node = static_cast<InsertNode*>(plan->get_node(pb::INSERT_NODE));
    InsertNode* replace_node = static_cast<InsertNode*>(plan->get_node(pb::REPLACE_NODE));
    TruncateNode* truncate_node = static_cast<TruncateNode*>(plan->get_node(pb::TRUNCATE_NODE));
    TransactionNode* txn_node = static_cast<TransactionNode*>(plan->get_node(pb::TRANSACTION_NODE));

    if (scan_nodes.size() != 0) {
        for (auto scan_node : scan_nodes) {
            auto ret = scan_node_analyze(static_cast<ScanNode*>(scan_node), ctx);
            if (ret != 0) {
                return ret;
            }
        }
    } else if (insert_node != nullptr) {
        return insert_node_analyze(insert_node, ctx);
    } else if (replace_node != nullptr) {
        return insert_node_analyze(replace_node, ctx);
    } else if (truncate_node != nullptr) {
        return truncate_node_analyze(truncate_node, ctx);
    } else if (txn_node != nullptr) {
        return transaction_node_analyze(txn_node, ctx);
    }
    return 0;
}

template<typename T>
int PlanRouter::insert_node_analyze(T* node, QueryContext* ctx) {
    int ret = 0;
    SchemaFactory* schema_factory = SchemaFactory::get_instance();
    int64_t table_id = node->table_id();
    IndexInfo index = schema_factory->get_index_info(table_id);
    if (index.id == -1) {
        DB_WARNING("invalid index info: %ld", table_id);
        return ret;
    }
    ret = schema_factory->get_region_by_key(
            index, ctx->insert_records, ctx->insert_region_ids, ctx->region_infos);
    if (ret < 0) {
        DB_WARNING("get_region_by_key:fail :%d", ret);
        return ret;
    }
    if (ctx->region_infos.size() == 0) {
        DB_WARNING("region_infos.size = 0");
        return -1;
    }
    return 0;
}

int PlanRouter::scan_node_analyze(ScanNode* scan_node, QueryContext* ctx) {
    SchemaFactory* schema_factory = SchemaFactory::get_instance();
    if (ctx->debug_region_id != -1) {
        pb::RegionInfo info;
        int ret = schema_factory->get_region_info(ctx->debug_region_id, info);
        if (ret != 0) {
            ctx->region_infos.clear();
            DB_WARNING("get region_info in debug mode failed, %ld", ctx->debug_region_id);
            return -1;
        }
        (*(scan_node->mutable_region_infos()))[ctx->debug_region_id] = info;
        return 0;
    }
    return scan_plan_router(scan_node);
}

int PlanRouter::scan_plan_router(ScanNode* scan_node) {
    const pb::ScanNode& pb_scan_node = scan_node->pb_node().derive_node().scan_node();
    int64_t table_id = scan_node->table_id();
    const pb::PossibleIndex* primary = nullptr;
    for (const auto& pos_index : pb_scan_node.indexes()) {
        if (pos_index.index_id() == table_id) {
            primary = &pos_index;
        }
    }
    SchemaFactory* schema_factory = SchemaFactory::get_instance(); 
    IndexInfo index = schema_factory->get_index_info(table_id);
    if (index.id == -1) {
        DB_WARNING("invalid index info: %ld", table_id);
        return -1;
    }
    scan_node->mutable_region_infos()->clear();
    auto ret = schema_factory->get_region_by_key(index, 
                                                 primary, 
                                                 *(scan_node->mutable_region_infos()));
    if (ret < 0) {
        DB_WARNING("get_region_by_key:fail :%d", ret);
        return ret;
    }
    // if (ctx->region_infos.size() == 0) {
    //     DB_WARNING("region_infos.size = 0");
    //     return -1;
    // }
    return 0;
}

int PlanRouter::truncate_node_analyze(TruncateNode* trunc_node, QueryContext* ctx) {
    int ret = 0;
    int64_t table_id = trunc_node->table_id();

    SchemaFactory* schema_factory = SchemaFactory::get_instance();
    IndexInfo index = schema_factory->get_index_info(table_id);
    if (index.id == -1) {
        DB_WARNING("invalid index info: %ld", table_id);
        return ret;
    }

    ret = schema_factory->get_region_by_key(index, nullptr, ctx->region_infos);
    if (ret < 0) {
        DB_WARNING("get_region_by_key:fail :%d", ret);
        return ret;
    }
    if (ctx->region_infos.size() == 0) {
        DB_WARNING("region_infos.size = 0");
        return -1;
    }
    return 0;
}

int PlanRouter::transaction_node_analyze(TransactionNode* txn_node, QueryContext* ctx) {
    if (txn_node->txn_cmd() == pb::TXN_BEGIN) {
        // start cmd only cached, no region info at this stage
        return 0;
    }
    // txn_node is routed in FetcherNode
    return 0;
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
