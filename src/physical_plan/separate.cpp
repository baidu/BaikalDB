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

#include "separate.h"
#include "packet_node.h"
#include "limit_node.h"
#include "agg_node.h"
#include "scan_node.h"
#include "rocksdb_scan_node.h"
#include "join_node.h"
#include "sort_node.h"
#include "filter_node.h"
#include "fetcher_node.h"
#include "insert_node.h"
#include "transaction_node.h"

namespace baikaldb {
int Separate::analyze(QueryContext* ctx) {
    if (ctx->is_explain) {
        return 0;
    }
    ExecNode* plan = ctx->root;
    PacketNode* packet_node = static_cast<PacketNode*>(plan->get_node(pb::PACKET_NODE));
    if (packet_node == nullptr) {
        return -1;
    }
    if (packet_node->children_size() == 0) {
        return 0;
    }
    //DB_WARNING("need_seperate:%d", plan->need_seperate());
    if (!plan->need_seperate()) {
        return 0;
    }
    std::vector<ExecNode*> scan_nodes;
    plan->get_node(pb::SCAN_NODE, scan_nodes);

    TransactionNode* txn_node = static_cast<TransactionNode*>(plan->get_node(pb::TRANSACTION_NODE));
    InsertNode* insert_node = static_cast<InsertNode*>(plan->get_node(pb::INSERT_NODE));

    pb::PlanNode pb_fetch_node;
    pb_fetch_node.set_node_type(pb::FETCHER_NODE);
    pb_fetch_node.set_limit(-1);
    pb::FetcherNode* pb_fetch_node_derive = 
        pb_fetch_node.mutable_derive_node()->mutable_fetcher_node();
    pb_fetch_node_derive->set_op_type(packet_node->op_type());

    std::unique_ptr<FetcherNode> fetch_node(new (std::nothrow)FetcherNode);
    if (fetch_node.get() == nullptr) {
        DB_WARNING("create fetch_node failed");
        return -1;
    }
    fetch_node->init(pb_fetch_node);

    // for insert/delete/update
    if (packet_node->op_type() != pb::OP_SELECT 
            && packet_node->op_type() != pb::OP_BEGIN
            && packet_node->op_type() != pb::OP_COMMIT
            && packet_node->op_type() != pb::OP_ROLLBACK) {
        if (scan_nodes.size() > 0) {
            auto region_infos = static_cast<RocksdbScanNode*>(scan_nodes[0])->region_infos();
            fetch_node->set_region_infos(region_infos);
        } else if (insert_node != nullptr) {
            //inset、replace node
            fetch_node->set_region_infos(insert_node->region_infos());
        } else {
            // truncate node
            fetch_node->set_region_infos(packet_node->children(0)->region_infos());
        }
        bool autocommit = ctx->runtime_state.autocommit();
        if (autocommit == false || ctx->enable_2pc == false
                || packet_node->op_type() == pb::OP_TRUNCATE_TABLE) {
            fetch_node->add_child(packet_node->children(0));
            packet_node->clear_children();
            packet_node->add_child(fetch_node.release());
        } else {
            // create multiple fetcher node and use two phase commit
            auto& region_infos = fetch_node->region_infos();
            return separate_autocommit_dml_2pc(ctx, packet_node, region_infos);
        }
        return 0;
    }

    if (packet_node->op_type() == pb::OP_BEGIN) {
        if (txn_node == nullptr) {
            return -1;
        }
        return separate_begin(ctx, txn_node);
    } else if (packet_node->op_type() == pb::OP_COMMIT) {
        if (txn_node == nullptr) {
            return -1;
        }
        return separate_commit(ctx, txn_node);
    } else if (packet_node->op_type() == pb::OP_ROLLBACK) {
        if (txn_node == nullptr) {
            return -1;
        }
        return separate_rollback(ctx, txn_node);
    }

    if (scan_nodes.size() <= 0) {
        DB_WARNING("illegal plan, scan node greater than 0");
        return -1;
    }

    std::map<int64_t, pb::RegionInfo> region_infos = static_cast<RocksdbScanNode*>(scan_nodes[0])->region_infos();
    //int region_size = region_infos.size();
    fetch_node->set_region_infos(region_infos);
    std::vector<ExecNode*> join_nodes;
    plan->get_node(pb::JOIN_NODE, join_nodes);
    if (join_nodes.size() == 0) {
        if (scan_nodes.size() > 1) {
            DB_WARNING("illegal plan");
            return -1;
        }
        /*
        // 没有join节点，并且单region全部可以下推
        // 分裂后会有问题
        if (region_size == 1) {
            fetch_node->add_child(packet_node->children(0));
            packet_node->clear_children();
            packet_node->add_child(fetch_node.release());
            return 0;
        }
        */
    }

    LimitNode* limit_node = static_cast<LimitNode*>(plan->get_node(pb::LIMIT_NODE));
    AggNode* agg_node = static_cast<AggNode*>(plan->get_node(pb::AGG_NODE));
    SortNode* sort_node = static_cast<SortNode*>(plan->get_node(pb::SORT_NODE));
    if (join_nodes.size() != 0) {
        seperate_for_join(scan_nodes); 
    } else if (agg_node != nullptr) {
        ExecNode* parent = agg_node->get_parent();
        AggNode* merge_agg_node = nullptr;
        pb::PlanNode pb_node;
        agg_node->transfer_pb(0, &pb_node);
        pb_node.set_node_type(pb::MERGE_AGG_NODE);
        pb_node.set_limit(-1);
        merge_agg_node = new AggNode;
        merge_agg_node->init(pb_node);

        fetch_node->add_child(agg_node);
        merge_agg_node->add_child(fetch_node.release());
        if (parent == nullptr) {
            //不会执行到这
            return -1;
        } else {
            parent->clear_children();
            parent->add_child(merge_agg_node);
        }
    } else if (sort_node != nullptr) {
        sort_node->transfer_fetcher_pb(pb_fetch_node_derive);

        fetch_node->init(pb_fetch_node);

        //add_child会把子节点的父节点修改，所以一定要在调用这个add_child之前把父节点保存
        ExecNode* parent = sort_node->get_parent();
        
        fetch_node->add_child(sort_node);
        if (parent == nullptr) {
            //不会执行到这
            return -1;
        } else {
            parent->clear_children();
            parent->add_child(fetch_node.release());
        }
    } else {
        if (limit_node != nullptr) {
            if (limit_node->children_size() == 0) {
                return 0;
            }
            fetch_node->add_child(limit_node->children(0));
            limit_node->clear_children();
            limit_node->add_child(fetch_node.release());
        } else {
            fetch_node->add_child(packet_node->children(0));
            packet_node->clear_children();
            packet_node->add_child(fetch_node.release());
        }
    }
    return 0;
}

FetcherNode* Separate::create_fetcher_node(pb::OpType op_type) {
    // create fetcher node
    pb::PlanNode pb_plan_node;
    pb_plan_node.set_node_type(pb::FETCHER_NODE);
    pb_plan_node.set_limit(-1);
    pb_plan_node.mutable_derive_node()->mutable_fetcher_node()->set_op_type(op_type);

    FetcherNode* fetch_node = new (std::nothrow)FetcherNode;
    if (fetch_node == nullptr) {
        DB_WARNING("create prepare_fetch_node failed");
        return nullptr;
    }
    fetch_node->init(pb_plan_node);
    return fetch_node;
}

TransactionNode* Separate::create_txn_node(pb::TxnCmdType cmd_type) {
    // create fetcher node
    pb::PlanNode pb_plan_node;
    //pb_plan_node.set_txn_id(txn_id);
    pb_plan_node.set_node_type(pb::TRANSACTION_NODE);
    pb_plan_node.set_limit(-1);
    auto txn_node = pb_plan_node.mutable_derive_node()->mutable_transaction_node();
    txn_node->set_txn_cmd(cmd_type);

    // create store txn node
    TransactionNode* store_txn_node = new (std::nothrow)TransactionNode;
    if (store_txn_node == nullptr) {
        DB_WARNING("create store_txn_node failed");
        return nullptr;
    }
    store_txn_node->init(pb_plan_node);
    return store_txn_node;
}

int Separate::separate_autocommit_dml_2pc(QueryContext* ctx, PacketNode* packet_node,
        std::map<int64_t, pb::RegionInfo>& region_infos) {
    // create baikaldb commit node
    std::unique_ptr<TransactionNode> commit_node(create_txn_node(pb::TXN_COMMIT));
    if (commit_node.get() == nullptr) {
        DB_WARNING("create commit_node failed");
        return -1;
    }
    ExecNode* dml_root = packet_node->children(0);
    packet_node->clear_children();

    // create begin fetcher node
    std::unique_ptr<FetcherNode> begin_fetch_node(create_fetcher_node(pb::OP_BEGIN));
    if (begin_fetch_node.get() == nullptr) {
        DB_WARNING("create begin_fetch_node failed");
        return -1;
    }
    // create store begin node
    std::unique_ptr<TransactionNode> store_begin_node(create_txn_node(pb::TXN_BEGIN_STORE));
    if (store_begin_node.get() == nullptr) {
        DB_WARNING("create store_begin_node failed");
        return -1;
    }
    begin_fetch_node->add_child(store_begin_node.release());

    // create dml fetcher node
    std::unique_ptr<FetcherNode> dml_fetch_node(create_fetcher_node(packet_node->op_type()));
    if (dml_fetch_node.get() == nullptr) {
        DB_WARNING("create dml_fetch_node failed");
        return -1;
    }
    dml_fetch_node->set_region_infos(region_infos);
    dml_fetch_node->add_child(dml_root);

    // create prepare fetcher node
    std::unique_ptr<FetcherNode> prepare_fetch_node(create_fetcher_node(pb::OP_PREPARE));
    if (prepare_fetch_node.get() == nullptr) {
        DB_WARNING("create prepare_fetch_node failed");
        return -1;
    }
    // create store prepare node
    std::unique_ptr<TransactionNode> store_prepare_node(create_txn_node(pb::TXN_PREPARE));
    if (store_prepare_node.get() == nullptr) {
        DB_WARNING("create store_prepare_node failed");
        return -1;
    }
    prepare_fetch_node->add_child(store_prepare_node.release());

    // create commit fetcher node
    std::unique_ptr<FetcherNode> commit_fetch_node(create_fetcher_node(pb::OP_COMMIT));
    if (commit_fetch_node.get() == nullptr) {
        DB_WARNING("create commit_fetch_node failed");
        return -1;
    }
    
    // create store commit node
    std::unique_ptr<TransactionNode> store_commit_node(create_txn_node(pb::TXN_COMMIT_STORE));
    if (store_commit_node.get() == nullptr) {
        DB_WARNING("create store_commit_node failed");
        return -1;
    }
    commit_fetch_node->add_child(store_commit_node.release());

    // create rollback fetcher node (in case of prepare failure)
    std::unique_ptr<FetcherNode> rollback_fetch_node(create_fetcher_node(pb::OP_ROLLBACK));
    if (rollback_fetch_node.get() == nullptr) {
        DB_WARNING("create rollback_fetch_node failed");
        return -1;
    }

    // create store rollback node
    std::unique_ptr<TransactionNode> store_rollback_node(create_txn_node(pb::TXN_ROLLBACK_STORE));
    if (store_rollback_node.get() == nullptr) {
        DB_WARNING("create store_rollback_node failed");
        return -1;
    }
    rollback_fetch_node->add_child(store_rollback_node.release());

    commit_node->add_child(begin_fetch_node.release());
    commit_node->add_child(dml_fetch_node.release());
    commit_node->add_child(prepare_fetch_node.release());
    commit_node->add_child(commit_fetch_node.release());
    commit_node->add_child(rollback_fetch_node.release());
    packet_node->add_child(commit_node.release());
    return 0;
}

int Separate::separate_commit(QueryContext* ctx, TransactionNode* txn_node) {
    // create prepare fetcher node
    std::unique_ptr<FetcherNode> prepare_fetch_node(create_fetcher_node(pb::OP_PREPARE));
    if (prepare_fetch_node.get() == nullptr) {
        DB_WARNING("create prepare_fetch_node failed");
        return -1;
    }
    //DB_WARNING("region size: %ld, %ld", ctx->region_infos.size(), ctx->insert_region_ids.size());
    // create store prepare node
    std::unique_ptr<TransactionNode> store_prepare_node(
        create_txn_node(pb::TXN_PREPARE));
    if (store_prepare_node.get() == nullptr) {
        DB_WARNING("create store_prepare_node failed");
        return -1;
    }
    prepare_fetch_node->add_child(store_prepare_node.release());
    txn_node->add_child(prepare_fetch_node.release());

    // create commit fetcher node
    std::unique_ptr<FetcherNode> commit_fetch_node(create_fetcher_node(pb::OP_COMMIT));
    if (commit_fetch_node.get() == nullptr) {
        DB_WARNING("create commit_fetch_node failed");
        return -1;
    }

    // create store commit node
    std::unique_ptr<TransactionNode> store_commit_node(
        create_txn_node(pb::TXN_COMMIT_STORE));
    if (store_commit_node.get() == nullptr) {
        DB_WARNING("create store_commit_node failed");
        return -1;
    }
    commit_fetch_node->add_child(store_commit_node.release());
    txn_node->add_child(commit_fetch_node.release());

    if (ctx->runtime_state.autocommit()) {
        // for set autocomit=1, need autorollback after prepare fail
        // create rollback fetcher node
        std::unique_ptr<FetcherNode> rollback_fetch_node(create_fetcher_node(pb::OP_ROLLBACK));
        if (rollback_fetch_node.get() == nullptr) {
            DB_WARNING("create rollback_fetch_node failed");
            return -1;
        }

        // create store commit node
        std::unique_ptr<TransactionNode> store_rollback_node(
            create_txn_node(pb::TXN_ROLLBACK_STORE));
        if (store_rollback_node.get() == nullptr) {
            DB_WARNING("create store_rollback_node failed");
            return -1;
        }
        rollback_fetch_node->add_child(store_rollback_node.release());
        txn_node->add_child(rollback_fetch_node.release());
    }

    if (txn_node->txn_cmd() == pb::TXN_COMMIT_BEGIN) {
        //auto client = ctx->runtime_state.client_conn();
        // create new txn begin fetcher node
        std::unique_ptr<FetcherNode> begin_fetch_node(create_fetcher_node(pb::OP_BEGIN));
        if (begin_fetch_node.get() == nullptr) {
            DB_WARNING("create begin_fetch_node failed");
            return -1;
        }
        // create store begin node
        std::unique_ptr<TransactionNode> store_begin_node(create_txn_node(pb::TXN_BEGIN_STORE));
        if (store_begin_node.get() == nullptr) {
            DB_WARNING("create store_begin_node failed");
            return -1;
        }
        begin_fetch_node->add_child(store_begin_node.release());
        txn_node->add_child(begin_fetch_node.release());
    }
    return 0;
}

int Separate::separate_rollback(QueryContext* ctx, TransactionNode* txn_node) {
    // create rollback fetcher node
    std::unique_ptr<FetcherNode> rollback_fetch_node(create_fetcher_node(pb::OP_ROLLBACK));
    if (rollback_fetch_node.get() == nullptr) {
        DB_WARNING("create rollback_fetch_node failed");
        return -1;
    }
    //DB_WARNING("region size: %ld, %ld", ctx->region_infos.size(), ctx->insert_region_ids.size());

    // create store rollback node
    std::unique_ptr<TransactionNode> store_rollback_node(create_txn_node(pb::TXN_ROLLBACK_STORE));
    if (store_rollback_node.get() == nullptr) {
        DB_WARNING("create store_rollback_node failed");
        return -1;
    }
    rollback_fetch_node->add_child(store_rollback_node.release());
    txn_node->add_child(rollback_fetch_node.release());

    if (txn_node->txn_cmd() == pb::TXN_ROLLBACK_BEGIN) {
        // create new txn begin fetcher node
        std::unique_ptr<FetcherNode> begin_fetch_node(create_fetcher_node(pb::OP_BEGIN));
        if (begin_fetch_node.get() == nullptr) {
            DB_WARNING("create begin_fetch_node failed");
            return -1;
        }
        // create store begin node
        std::unique_ptr<TransactionNode> store_begin_node(create_txn_node(pb::TXN_BEGIN_STORE));
        if (store_begin_node.get() == nullptr) {
            DB_WARNING("create store_begin_node failed");
            return -1;
        }
        begin_fetch_node->add_child(store_begin_node.release());
        txn_node->add_child(begin_fetch_node.release());
    }
    return 0;
}

int Separate::separate_begin(QueryContext* ctx, TransactionNode* txn_node) {
    // create begin fetcher node
    std::unique_ptr<FetcherNode> begin_fetch_node(create_fetcher_node(pb::OP_BEGIN));
    if (begin_fetch_node.get() == nullptr) {
        DB_WARNING("create begin_fetch_node failed");
        return -1;
    }
    //DB_WARNING("region size: %ld, %ld", ctx->region_infos.size(), ctx->insert_region_ids.size());

    // create store begin node
    std::unique_ptr<TransactionNode> store_begin_node(create_txn_node(pb::TXN_BEGIN_STORE));
    if (store_begin_node.get() == nullptr) {
        DB_WARNING("create store_begin_node failed");
        return -1;
    }
    begin_fetch_node->add_child(store_begin_node.release());
    txn_node->add_child(begin_fetch_node.release());
    return 0;
}

int Separate::seperate_for_join(const std::vector<ExecNode*>& scan_nodes) {
    for (auto& scan_node_ptr : scan_nodes) {
        ExecNode* fether_node_parent = scan_node_ptr->get_parent();
        ExecNode* fether_node_child = scan_node_ptr;
        if (fether_node_parent == nullptr) {
            DB_WARNING("fether node children is null");
            return -1;
        }
        while (fether_node_parent->node_type() == pb::TABLE_FILTER_NODE
                    || fether_node_parent->node_type() == pb::WHERE_FILTER_NODE) {
            fether_node_parent = fether_node_parent->get_parent();
            fether_node_child = fether_node_child->get_parent();
            if (fether_node_parent == nullptr) {
                DB_WARNING("fether node children is null");
                return -1;
            }
        }
        pb::PlanNode pb_fetch_node; 
        pb_fetch_node.set_node_type(pb::FETCHER_NODE);
        pb_fetch_node.set_limit(-1);
        pb::FetcherNode* pb_fetch_node_derive = 
            pb_fetch_node.mutable_derive_node()->mutable_fetcher_node();
        pb_fetch_node_derive->set_op_type(pb::OP_SELECT);
        FetcherNode* fetch_node = new FetcherNode;
        static_cast<RocksdbScanNode*>(scan_node_ptr)->set_related_fetcher_node(fetch_node);
        fetch_node->init(pb_fetch_node);
        std::map<int64_t, pb::RegionInfo> region_infos = static_cast<RocksdbScanNode*>(scan_node_ptr)->region_infos();
        fetch_node->set_region_infos(region_infos);
        fether_node_parent->replace_child(fether_node_child, fetch_node);
        fetch_node->add_child(fether_node_child);
    }
    return 0;
}

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
