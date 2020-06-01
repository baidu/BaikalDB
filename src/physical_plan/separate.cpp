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

#include "separate.h"
#include "packet_node.h"
#include "limit_node.h"
#include "agg_node.h"
#include "scan_node.h"
#include "rocksdb_scan_node.h"
#include "join_node.h"
#include "sort_node.h"
#include "filter_node.h"
#include "full_export_node.h"
#include "insert_node.h"
#include "transaction_node.h"
#include "begin_manager_node.h"
#include "commit_manager_node.h"
#include "rollback_manager_node.h"
#include "insert_manager_node.h"
#include "update_manager_node.h"
#include "delete_manager_node.h"
#include "common_manager_node.h"
#include "select_manager_node.h"
#include "union_node.h"
#include "single_txn_manager_node.h"
#include "lock_primary_node.h"
#include "lock_secondary_node.h"

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
    int ret = 0;
    pb::OpType op_type = packet_node->op_type();
    switch (op_type) {
    case pb::OP_SELECT: {
        ret = separate_select(ctx);
        break;
    }
    case pb::OP_INSERT: {
        ret = separate_insert(ctx);
        break;
    }
    case pb::OP_UPDATE: {
        ret = separate_update(ctx);
        break;
    }
    case pb::OP_DELETE: {
        ret = separate_delete(ctx);
        break;
    }
    case pb::OP_TRUNCATE_TABLE: {
        ret = separate_truncate(ctx);
        break;
    }
    case pb::OP_KILL: {
        ret = separate_kill(ctx);
        break;
    }
    case pb::OP_BEGIN: {
        ret = separate_begin(ctx);
        break;
    }
    case pb::OP_COMMIT: {
        ret = separate_commit(ctx);
        break;
    }
    case pb::OP_ROLLBACK: {
        ret = separate_rollback(ctx);
        break;
    }
    case pb::OP_UNION: {
        ret = separate_union(ctx);
        break;
    }
    default: {
        DB_FATAL("invalid op_type, op_type: %s", pb::OpType_Name(op_type).c_str());
        return -1;
    }
    }
    return ret;
}

int Separate::separate_union(QueryContext* ctx) {
    ExecNode* plan = ctx->root;
    UnionNode* union_node = static_cast<UnionNode*>(plan->get_node(pb::UNION_NODE));
    for (int i = 0; i < ctx->union_select_plans.size(); i++) {
        auto select_ctx = ctx->union_select_plans[i];
        ExecNode* select_plan = select_ctx->root;
        PacketNode* packet_node = static_cast<PacketNode*>(select_plan->get_node(pb::PACKET_NODE));
        union_node->steal_projections(packet_node->mutable_projections());
        union_node->add_child(packet_node->children(0));
        packet_node->clear_children();
        union_node->mutable_select_runtime_states()->push_back(select_ctx->get_runtime_state().get());
    }
    return 0;
}

int Separate::separate_select(QueryContext* ctx) {
    ExecNode* plan = ctx->root;
    std::vector<ExecNode*> join_nodes;
    plan->get_node(pb::JOIN_NODE, join_nodes);
    std::vector<ExecNode*> scan_nodes;
    plan->get_node(pb::SCAN_NODE, scan_nodes);
    if (ctx->is_full_export) {
        PacketNode* packet_node = static_cast<PacketNode*>(plan->get_node(pb::PACKET_NODE));
        LimitNode* limit_node = static_cast<LimitNode*>(plan->get_node(pb::LIMIT_NODE));
        std::unique_ptr<ExecNode> export_node(new (std::nothrow) FullExportNode);
        if (export_node == nullptr) {
            DB_WARNING("new export node fail");
            return -1;
        }
        pb::PlanNode pb_fetch_node;
        pb_fetch_node.set_node_type(pb::FULL_EXPORT_NODE);
        pb_fetch_node.set_limit(-1);
        export_node->init(pb_fetch_node);
        std::map<int64_t, pb::RegionInfo> region_infos =
                static_cast<RocksdbScanNode*>(scan_nodes[0])->region_infos();
        export_node->set_region_infos(region_infos);
        if (limit_node == nullptr) {
            export_node->add_child(packet_node->children(0));
            packet_node->clear_children();
            packet_node->add_child(export_node.release());
        } else {
            export_node->add_child(limit_node->children(0));
            limit_node->clear_children();
            limit_node->add_child(export_node.release());
        }
        return 0;
    }
    if (join_nodes.size() == 0) {
        if (scan_nodes.size() > 1) {
            DB_WARNING("illegal plan");
            return -1;
        }
        return separate_simple_select(ctx);
    } else {
        return separate_join(scan_nodes);
    }
}

//普通的select请求，考虑agg_node, sort_node, limit_node下推问题
int Separate::separate_simple_select(QueryContext* ctx) {
    ExecNode* plan = ctx->root;
    PacketNode* packet_node = static_cast<PacketNode*>(plan->get_node(pb::PACKET_NODE));
    LimitNode* limit_node = static_cast<LimitNode*>(plan->get_node(pb::LIMIT_NODE));
    AggNode* agg_node = static_cast<AggNode*>(plan->get_node(pb::AGG_NODE));
    SortNode* sort_node = static_cast<SortNode*>(plan->get_node(pb::SORT_NODE));
    std::vector<ExecNode*> scan_nodes;
    plan->get_node(pb::SCAN_NODE, scan_nodes);
    SelectManagerNode* manager_node_inter = static_cast<SelectManagerNode*>(plan->get_node(pb::SELECT_MANAGER_NODE));
    // 复用prepare的计划
    if (manager_node_inter != nullptr) {
        std::map<int64_t, pb::RegionInfo> region_infos =
            static_cast<RocksdbScanNode*>(scan_nodes[0])->region_infos();
        manager_node_inter->set_region_infos(region_infos);
        return 0;
    }
    pb::PlanNode pb_manager_node;
    pb_manager_node.set_node_type(pb::SELECT_MANAGER_NODE);
    pb_manager_node.set_limit(-1);

    std::unique_ptr<SelectManagerNode> manager_node(new (std::nothrow) SelectManagerNode);
    if (manager_node == nullptr) {
        DB_WARNING("create manager_node failed");
        return -1;
    }
    manager_node->init(pb_manager_node);
    std::map<int64_t, pb::RegionInfo> region_infos =
            static_cast<RocksdbScanNode*>(scan_nodes[0])->region_infos();
    manager_node->set_region_infos(region_infos);
    if (agg_node != nullptr) {
        ExecNode* parent = agg_node->get_parent();
        pb::PlanNode pb_node;
        agg_node->transfer_pb(0, &pb_node);
        pb_node.set_node_type(pb::MERGE_AGG_NODE);
        pb_node.set_limit(-1);
        std::unique_ptr<AggNode> merge_agg_node(new (std::nothrow) AggNode);
        merge_agg_node->init(pb_node);

        manager_node->add_child(agg_node);
        merge_agg_node->add_child(manager_node.release());
        if (parent == nullptr) {
            //不会执行到这
            DB_WARNING("parent is null");
            return -1;
        } else {
            parent->clear_children();
            parent->add_child(merge_agg_node.release());
        }
        return 0;
    }
    if (sort_node != nullptr) {
        manager_node->init_sort_info(sort_node->pb_node());
        // add_child会把子节点的父节点修改，所以一定要在调用这个add_child之前把父节点保存
        ExecNode* parent = sort_node->get_parent();
        manager_node->add_child(sort_node);
        if (parent == nullptr) {
            //不会执行到这
            DB_WARNING("parent is null");
            return -1;
        } else {
            parent->clear_children();
            parent->add_child(manager_node.release());
        }
        return 0;
    }
    if (limit_node != nullptr) {
        if (limit_node->children_size() == 0) {
            return 0;
        }
        manager_node->add_child(limit_node->children(0));
        limit_node->clear_children();
        limit_node->add_child(manager_node.release());
        return 0;
    }
    manager_node->add_child(packet_node->children(0));
    packet_node->clear_children();
    packet_node->add_child(manager_node.release());
    return 0;
}
int Separate::separate_join(const std::vector<ExecNode*>& scan_nodes) {
    for (auto& scan_node_ptr : scan_nodes) {
        ExecNode* manager_node_parent = scan_node_ptr->get_parent();
        ExecNode* manager_node_child = scan_node_ptr;
        if (manager_node_parent == nullptr) {
            DB_WARNING("fether node children is null");
            return -1;
        }
        while (manager_node_parent->node_type() == pb::TABLE_FILTER_NODE ||
               manager_node_parent->node_type() == pb::WHERE_FILTER_NODE) {
            manager_node_parent = manager_node_parent->get_parent();
            manager_node_child = manager_node_child->get_parent();
            if (manager_node_parent == nullptr) {
                DB_WARNING("fether node children is null");
                return -1;
            }
        }
        // 复用prepare的计划
        if (manager_node_parent->node_type() == pb::SELECT_MANAGER_NODE) {
            std::map<int64_t, pb::RegionInfo> region_infos =
                static_cast<RocksdbScanNode*>(scan_node_ptr)->region_infos();
            manager_node_parent->set_region_infos(region_infos);
            continue;
        }
        pb::PlanNode pb_manager_node;
        pb_manager_node.set_node_type(pb::SELECT_MANAGER_NODE);
        pb_manager_node.set_limit(-1);

        SelectManagerNode* manager_node = new SelectManagerNode;
        static_cast<RocksdbScanNode*>(scan_node_ptr)->set_related_manager_node(manager_node);
        std::map<int64_t, pb::RegionInfo> region_infos =
                static_cast<RocksdbScanNode*>(scan_node_ptr)->region_infos();
        manager_node->set_region_infos(region_infos);
        manager_node->init(pb_manager_node);
        manager_node_parent->replace_child(manager_node_child, manager_node);
        manager_node->add_child(manager_node_child);
    }
    return 0;
}
int Separate::separate_insert(QueryContext* ctx) {
    ExecNode* plan = ctx->root;
    InsertNode* insert_node = static_cast<InsertNode*>(plan->get_node(pb::INSERT_NODE));
    PacketNode* packet_node = static_cast<PacketNode*>(plan->get_node(pb::PACKET_NODE));
    // TODO:复用prepare的计划

    pb::PlanNode pb_manager_node;
    pb_manager_node.set_node_type(pb::INSERT_MANAGER_NODE);
    pb_manager_node.set_limit(-1);
    std::unique_ptr<InsertManagerNode> manager_node(new (std::nothrow) InsertManagerNode);
    if (manager_node == nullptr) {
        DB_WARNING("create manager_node failed");
        return -1;
    }
    manager_node->init(pb_manager_node);
    manager_node->set_records(ctx->insert_records);
    int64_t main_table_id = insert_node->table_id();
    if (SchemaFactory::get_instance()->has_global_index(main_table_id)) {
        separate_global_insert(manager_node.get(), insert_node);
    } else {
        manager_node->set_op_type(pb::OP_INSERT);
        manager_node->set_region_infos(insert_node->region_infos());
        manager_node->add_child(insert_node);
    }
    packet_node->clear_children();
    packet_node->add_child(manager_node.release());
    if (ctx->get_runtime_state()->single_sql_autocommit() && 
        (ctx->enable_2pc 
            || SchemaFactory::get_instance()->has_global_index(main_table_id))) {
        separate_single_txn(packet_node);
    }
    return 0;
}

// insert_node中的属性完全转义到manager_node中，析构insert_node
int Separate::separate_global_insert(InsertManagerNode* manager_node, InsertNode* insert_node) {
    int ret = 0;
    int64_t table_id = insert_node->table_id();
    ret = manager_node->init_insert_info(insert_node);
    if (ret < 0) {
        return -1;
    }
    // ignore
    if (manager_node->need_ignore()) {
        create_lock_node(table_id, pb::LOCK_GET, 0, manager_node);
        create_lock_node(table_id, pb::LOCK_NO, 0, manager_node);
    } else if (manager_node->is_replace()) {
        create_lock_node(table_id, pb::LOCK_GET, 0, manager_node);
        create_lock_node(table_id, pb::LOCK_GET_ONLY_PRIMARY, 1, manager_node);
        create_lock_node(table_id, pb::LOCK_DML, 0, manager_node);
    } else if (manager_node->on_dup_key_update()) {
        create_lock_node(table_id, pb::LOCK_GET, 0, manager_node);
        create_lock_node(table_id, pb::LOCK_GET_ONLY_PRIMARY, 1, manager_node);
        create_lock_node(table_id, pb::LOCK_DML, 0, manager_node);
    } else {
        // basic insert
        create_lock_node(table_id, pb::LOCK_DML, 0, manager_node);
    }
    // 复用
    delete insert_node;
    return 0;
}
int Separate::create_lock_node(
        int64_t table_id,
        pb::LockCmdType lock_type,
        int mode,
        ExecNode* manager_node) {
    auto table_info = SchemaFactory::get_instance()->get_table_info_ptr(table_id);
    if (table_info == nullptr) {
        return -1;
    }
    std::vector<int64_t> affected_indexs;
    std::set<int64_t> unique_indexs;
    std::set<int64_t> non_unique_indexs;
    for (auto index_id : table_info->indices) {
        auto index_info = SchemaFactory::get_instance()->get_index_info_ptr(index_id);
        if (index_info == nullptr) {
            return -1;
        }
        if (index_info->is_global) {
            if (index_info->type == pb::I_UNIQ) {
                unique_indexs.insert(index_id);
            } else {
                non_unique_indexs.insert(index_id);
            }
        }
    }
    for (auto id : unique_indexs) {
        affected_indexs.push_back(id);
    }
    //LOGK_GET只需要关注全局唯一索引
    if (lock_type != pb::LOCK_GET) {
        for (auto id : non_unique_indexs) {
            affected_indexs.push_back(id);
        }
    }
    return create_lock_node(table_id, lock_type, mode, affected_indexs, manager_node);
}
int Separate::create_lock_node(
        int64_t table_id,
        pb::LockCmdType lock_type,
        int mode,
        const std::vector<int64_t>& affected_indexs,
        ExecNode* manager_node) {
    //构造LockAndPutPrimaryNode
    if (mode == 0 || mode == 1) {
        std::unique_ptr<LockPrimaryNode> primary_node(new (std::nothrow) LockPrimaryNode);
        if (primary_node == nullptr) {
            DB_WARNING("create manager_node failed");
            return -1;
        }
        pb::PlanNode plan_node;
        plan_node.set_node_type(pb::LOCK_PRIMARY_NODE);
        plan_node.set_limit(-1);
        plan_node.mutable_derive_node()->mutable_lock_primary_node()->set_lock_type(lock_type);
        plan_node.mutable_derive_node()->mutable_lock_primary_node()->set_table_id(table_id);
        primary_node->init(plan_node);
        manager_node->add_child(primary_node.release());
    }
    //构造LockAndPutSecondaryNode
    if (mode == 0 || mode == 2) {
    for (auto index_id : affected_indexs) {
        auto index_info = SchemaFactory::get_instance()->get_index_info_ptr(index_id);
        if (index_info == nullptr) {
            return -1;
        }
        if (index_info->is_global) {
            std::unique_ptr<LockSecondaryNode> secondary_node(new (std::nothrow) LockSecondaryNode);
            if (secondary_node == nullptr) {
                DB_WARNING("create manager_node failed");
                return -1;
            }
            pb::PlanNode plan_node;
            plan_node.set_node_type(pb::LOCK_SECONDARY_NODE);
            plan_node.set_limit(-1);
            plan_node.mutable_derive_node()->mutable_lock_secondary_node()->set_lock_type(
                    lock_type);
            plan_node.mutable_derive_node()->mutable_lock_secondary_node()->set_global_index_id(index_id);
            plan_node.mutable_derive_node()->mutable_lock_secondary_node()->set_table_id(
                    table_id);
            secondary_node->init(plan_node);
            manager_node->add_child(secondary_node.release());
        }
    }
    }
    return 0;
}

int Separate::separate_update(QueryContext* ctx) {
    int ret = 0;
    ExecNode* plan = ctx->root;
    PacketNode* packet_node = static_cast<PacketNode*>(plan->get_node(pb::PACKET_NODE));
    UpdateNode* update_node = static_cast<UpdateNode*>(plan->get_node(pb::UPDATE_NODE));
    std::vector<ExecNode*> scan_nodes;
    plan->get_node(pb::SCAN_NODE, scan_nodes);
    pb::PlanNode pb_manager_node;
    pb_manager_node.set_node_type(pb::UPDATE_MANAGER_NODE);
    pb_manager_node.set_limit(-1);

    std::unique_ptr<UpdateManagerNode> manager_node(new (std::nothrow) UpdateManagerNode);
    if (manager_node == nullptr) {
        DB_WARNING("create manager_node failed");
        return -1;
    }
    manager_node->init(pb_manager_node);
    ret = manager_node->init_update_info(update_node);
    if (ret < 0) {
        return -1;
    }
    int64_t main_table_id = update_node->table_id();
    if (!SchemaFactory::get_instance()->has_global_index(main_table_id)) {
        auto region_infos = static_cast<RocksdbScanNode*>(scan_nodes[0])->region_infos();
        manager_node->set_op_type(pb::OP_UPDATE);
        manager_node->set_region_infos(region_infos);
        manager_node->add_child(packet_node->children(0));
    } else {
        separate_global_update(manager_node.get(), update_node, scan_nodes[0]);
    }
    packet_node->clear_children();
    packet_node->add_child(manager_node.release());
    if (ctx->get_runtime_state()->single_sql_autocommit() && 
        (ctx->enable_2pc || SchemaFactory::get_instance()->has_global_index(main_table_id))) {
        separate_single_txn(packet_node);
    }
    return 0;
}
int Separate::separate_delete(QueryContext* ctx) {
    ExecNode* plan = ctx->root;
    PacketNode* packet_node = static_cast<PacketNode*>(plan->get_node(pb::PACKET_NODE));
    DeleteNode* delete_node = static_cast<DeleteNode*>(plan->get_node(pb::DELETE_NODE));
    std::vector<ExecNode*> scan_nodes;
    plan->get_node(pb::SCAN_NODE, scan_nodes);
    int64_t main_table_id = delete_node->table_id();
    pb::PlanNode pb_manager_node;
    pb_manager_node.set_node_type(pb::DELETE_MANAGER_NODE);
    pb_manager_node.set_limit(-1);
    std::unique_ptr<DeleteManagerNode> manager_node(new (std::nothrow) DeleteManagerNode);
    if (manager_node == nullptr) {
        DB_WARNING("create manager_node failed");
        return -1;
    }
    manager_node->init(pb_manager_node);
    if (SchemaFactory::get_instance()->has_global_index(main_table_id)) {
        separate_global_delete(manager_node.get(), delete_node, scan_nodes[0]);
    } else {
        auto region_infos = static_cast<RocksdbScanNode*>(scan_nodes[0])->region_infos();
        manager_node->set_op_type(pb::OP_DELETE);
        manager_node->set_region_infos(region_infos);
        manager_node->add_child(packet_node->children(0));
    }
    packet_node->clear_children();
    packet_node->add_child(manager_node.release());
    if (ctx->get_runtime_state()->single_sql_autocommit() &&
        (ctx->enable_2pc || SchemaFactory::get_instance()->has_global_index(main_table_id))) {
        separate_single_txn(packet_node);
    }
    return 0;
}

int Separate::separate_global_update(
              UpdateManagerNode* manager_node,
              UpdateNode* update_node,
              ExecNode* scan_node) {
    int ret = 0;
    int64_t main_table_id = update_node->table_id();
    //生成delete_node_manager结点
    pb::PlanNode pb_delete_manager_node;
    pb_delete_manager_node.set_node_type(pb::DELETE_MANAGER_NODE);
    pb_delete_manager_node.set_limit(-1);
    std::unique_ptr<DeleteManagerNode> delete_manager_node(new (std::nothrow) DeleteManagerNode);
    if (delete_manager_node == nullptr) {
        DB_WARNING("create manager_node failed");
        return -1;
    }
    delete_manager_node->init(pb_delete_manager_node);
    ret = delete_manager_node->init_delete_info(update_node->pb_node().derive_node().update_node());
    if (ret < 0) {
        return -1;
    }
    pb::PlanNode pb_select_manager_node;
    pb_select_manager_node.set_node_type(pb::SELECT_MANAGER_NODE);
    pb_select_manager_node.set_limit(-1);
    std::unique_ptr<SelectManagerNode> select_manager_node(new (std::nothrow) SelectManagerNode);
    if (select_manager_node == nullptr) {
        DB_WARNING("create manager_node failed");
        return -1;
    }
    select_manager_node->init(pb_select_manager_node);
    select_manager_node->set_region_infos(scan_node->region_infos());
    select_manager_node->add_child(update_node->children(0));
    delete_manager_node->add_child(select_manager_node.release());
    manager_node->set_update_exprs(update_node->update_exprs());
    update_node->clear_children();
    update_node->clear_update_exprs();
    delete update_node;
    create_lock_node(
            main_table_id,
            pb::LOCK_GET_DML,
            1,
            manager_node->affected_index_ids(),
            delete_manager_node.get());
    create_lock_node(
            main_table_id,
            pb::LOCK_DML,
            2,
            manager_node->affected_index_ids(),
            delete_manager_node.get());
    LockPrimaryNode* pri_node = static_cast<LockPrimaryNode*>(delete_manager_node->children(1));
    pri_node->set_affect_primary(manager_node->affect_primary());
    pri_node->set_affected_index_ids(manager_node->affected_index_ids()); 

    manager_node->add_child(delete_manager_node.release());
    //生成basic_insert
    pb::PlanNode pb_insert_manager_node;
    pb_insert_manager_node.set_node_type(pb::INSERT_MANAGER_NODE);
    pb_insert_manager_node.set_limit(-1);
    std::unique_ptr<InsertManagerNode> insert_manager_node(new (std::nothrow) InsertManagerNode);
    if (insert_manager_node == nullptr) {
        DB_WARNING("create manager_node failed");
        return -1;
    }
    insert_manager_node->init(pb_insert_manager_node);
    create_lock_node(main_table_id, pb::LOCK_DML, 0, manager_node->affected_index_ids(), insert_manager_node.get());
    pri_node = static_cast<LockPrimaryNode*>(insert_manager_node->children(0));
    pri_node->set_affect_primary(manager_node->affect_primary());
    pri_node->set_affected_index_ids(manager_node->affected_index_ids());
    manager_node->add_child(insert_manager_node.release());
    return 0;
}
/*
 *                         packetNode
 *                             |
 *                       DeleteMangerNode
 *                             |
 *  SelectManagerNode    LockPrimaryNode    LockSecondaryNode
 *        |
 *     FilterNode
 *        |
 *     ScanNode
 *
 */
int Separate::separate_global_delete(
        DeleteManagerNode* manager_node,
        DeleteNode* delete_node,
        ExecNode* scan_node) {
    int ret = 0;
    int64_t main_table_id = delete_node->table_id();
    ret = manager_node->init_delete_info(delete_node->pb_node().derive_node().delete_node());
    if (ret < 0) {
        return -1;
    }
    pb::PlanNode pb_manager_node;
    pb_manager_node.set_node_type(pb::SELECT_MANAGER_NODE);
    pb_manager_node.set_limit(-1);
    std::unique_ptr<SelectManagerNode> select_manager_node(new (std::nothrow) SelectManagerNode);
    if (select_manager_node.get() == nullptr) {
        DB_WARNING("create manager_node failed");
        return -1;
    }
    select_manager_node->init(pb_manager_node);
    std::map<int64_t, pb::RegionInfo> region_infos =
            static_cast<RocksdbScanNode*>(scan_node)->region_infos();
    select_manager_node->set_region_infos(region_infos);
    select_manager_node->add_child(delete_node->children(0));
    manager_node->add_child(select_manager_node.release());
    delete_node->clear_children();

    create_lock_node(main_table_id, pb::LOCK_GET_DML, 1, manager_node);
    create_lock_node(main_table_id, pb::LOCK_DML, 2, manager_node);
    // manager_node->children(0)->add_child(delete_node->children(0));
    delete delete_node;
    return 0;
}
int Separate::separate_single_txn(PacketNode* packet_node) {
    // create baikaldb commit node
    pb::PlanNode pb_plan_node;
    pb_plan_node.set_node_type(pb::SIGNEL_TXN_MANAGER_NODE);
    pb_plan_node.set_limit(-1);
    SingleTxnManagerNode* txn_manager_node = new (std::nothrow) SingleTxnManagerNode;
    if (txn_manager_node == nullptr) {
        DB_WARNING("create store_txn_node failed");
        return -1;
    }
    txn_manager_node->init(pb_plan_node);
    txn_manager_node->set_op_type(packet_node->op_type());
    ExecNode* dml_root = packet_node->children(0);
    packet_node->clear_children();
    packet_node->add_child(txn_manager_node);
    // create store begin node
    std::unique_ptr<TransactionNode> store_begin_node(create_txn_node(pb::TXN_BEGIN_STORE));
    if (store_begin_node.get() == nullptr) {
        DB_WARNING("create store_begin_node failed");
        return -1;
    }
    // create store prepare node
    std::unique_ptr<TransactionNode> store_prepare_node(create_txn_node(pb::TXN_PREPARE));
    if (store_prepare_node.get() == nullptr) {
        DB_WARNING("create store_prepare_node failed");
        return -1;
    }
    // create store commit node
    std::unique_ptr<TransactionNode> store_commit_node(create_txn_node(pb::TXN_COMMIT_STORE));
    if (store_commit_node.get() == nullptr) {
        DB_WARNING("create store_commit_node failed");
        return -1;
    }

    // create store rollback node
    std::unique_ptr<TransactionNode> store_rollback_node(create_txn_node(pb::TXN_ROLLBACK_STORE));
    if (store_rollback_node.get() == nullptr) {
        DB_WARNING("create store_rollback_node failed");
        return -1;
    }

    txn_manager_node->add_child(store_begin_node.release());
    txn_manager_node->add_child(dml_root);
    txn_manager_node->add_child(store_prepare_node.release());
    txn_manager_node->add_child(store_commit_node.release());
    txn_manager_node->add_child(store_rollback_node.release());
    return 0;
}
int Separate::separate_truncate(QueryContext* ctx) {
    ExecNode* plan = ctx->root;
    PacketNode* packet_node = static_cast<PacketNode*>(plan->get_node(pb::PACKET_NODE));

    pb::PlanNode pb_manager_node;
    pb_manager_node.set_node_type(pb::TRUNCATE_MANAGER_NODE);
    pb_manager_node.set_limit(-1);
    std::unique_ptr<CommonManagerNode> manager_node(new (std::nothrow) CommonManagerNode);
    if (manager_node.get() == nullptr) {
        DB_WARNING("create manager_node failed");
        return -1;
    }
    manager_node->init(pb_manager_node);
    manager_node->set_op_type(pb::OP_TRUNCATE_TABLE);
    manager_node->set_region_infos(packet_node->children(0)->region_infos());
    manager_node->add_child(packet_node->children(0));
    packet_node->clear_children();
    packet_node->add_child(manager_node.release());
    return 0;
}

int Separate::separate_kill(QueryContext* ctx) {
    ExecNode* plan = ctx->root;
    PacketNode* packet_node = static_cast<PacketNode*>(plan->get_node(pb::PACKET_NODE));

    pb::PlanNode pb_manager_node;
    pb_manager_node.set_node_type(pb::KILL_MANAGER_NODE);
    pb_manager_node.set_limit(-1);
    std::unique_ptr<CommonManagerNode> manager_node(new (std::nothrow) CommonManagerNode);
    if (manager_node.get() == nullptr) {
        DB_WARNING("create manager_node failed");
        return -1;
    }
    manager_node->init(pb_manager_node);
    manager_node->set_op_type(pb::OP_KILL);
    manager_node->set_region_infos(packet_node->children(0)->region_infos());
    manager_node->add_child(packet_node->children(0));
    packet_node->clear_children();
    packet_node->add_child(manager_node.release());
    return 0;
}
int Separate::separate_commit(QueryContext* ctx) {
    ExecNode* plan = ctx->root;
    CommitManagerNode* commit_node =
            static_cast<CommitManagerNode*>(plan->get_node(pb::COMMIT_MANAGER_NODE));
    std::unique_ptr<TransactionNode> store_prepare_node(create_txn_node(pb::TXN_PREPARE));
    if (store_prepare_node.get() == nullptr) {
        DB_WARNING("create store_prepare_node failed");
        return -1;
    }
    commit_node->add_child(store_prepare_node.release());

    // create store commit node
    std::unique_ptr<TransactionNode> store_commit_node(create_txn_node(pb::TXN_COMMIT_STORE));
    if (store_commit_node.get() == nullptr) {
        DB_WARNING("create store_commit_node failed");
        return -1;
    }

    // create store rollback node
    std::unique_ptr<TransactionNode> store_rollback_node(create_txn_node(pb::TXN_ROLLBACK_STORE));
    if (store_rollback_node.get() == nullptr) {
        DB_WARNING("create store_rollback_node failed");
        return -1;
    }

    commit_node->add_child(store_commit_node.release());
    commit_node->add_child(store_rollback_node.release());
    if (commit_node->txn_cmd() == pb::TXN_COMMIT_BEGIN) {
        // create store begin node
        std::unique_ptr<TransactionNode> store_begin_node(create_txn_node(pb::TXN_BEGIN_STORE));
        if (store_begin_node.get() == nullptr) {
            DB_WARNING("create store_begin_node failed");
            return -1;
        }
        commit_node->add_child(store_begin_node.release());
    }
    return 0;
}
int Separate::separate_rollback(QueryContext* ctx) {
    ExecNode* plan = ctx->root;
    RollbackManagerNode* rollback_node =
            static_cast<RollbackManagerNode*>(plan->get_node(pb::ROLLBACK_MANAGER_NODE));

    // create store rollback node
    std::unique_ptr<TransactionNode> store_rollback_node(create_txn_node(pb::TXN_ROLLBACK_STORE));
    if (store_rollback_node.get() == nullptr) {
        DB_WARNING("create store_rollback_node failed");
        return -1;
    }
    rollback_node->add_child(store_rollback_node.release());

    if (rollback_node->txn_cmd() == pb::TXN_ROLLBACK_BEGIN) {
        // create store begin node
        std::unique_ptr<TransactionNode> store_begin_node(create_txn_node(pb::TXN_BEGIN_STORE));
        if (store_begin_node.get() == nullptr) {
            DB_WARNING("create store_begin_node failed");
            return -1;
        }
        rollback_node->add_child(store_begin_node.release());
    }
    return 0;
}

int Separate::separate_begin(QueryContext* ctx) {
    ExecNode* plan = ctx->root;
    BeginManagerNode* begin_node =
            static_cast<BeginManagerNode*>(plan->get_node(pb::BEGIN_MANAGER_NODE));
    // create store begin node
    std::unique_ptr<TransactionNode> store_begin_node(create_txn_node(pb::TXN_BEGIN_STORE));
    if (store_begin_node.get() == nullptr) {
        DB_WARNING("create store_begin_node failed");
        return -1;
    }
    begin_node->add_child(store_begin_node.release());
    DB_WARNING(
            "packet_node: %ld, begin_node: %ld, store_begin_node: %ld",
            plan,
            begin_node,
            store_begin_node.release());
    DB_WARNING("begion_node size: %d", begin_node->children_size());
    for (auto i = 0; i <  begin_node->children_size(); ++i) {
        DB_WARNING("children: %ld", begin_node->children(i));
    }
    return 0;
}

TransactionNode* Separate::create_txn_node(pb::TxnCmdType cmd_type) {
    // create fetcher node
    pb::PlanNode pb_plan_node;
    // pb_plan_node.set_txn_id(txn_id);
    pb_plan_node.set_node_type(pb::TRANSACTION_NODE);
    pb_plan_node.set_limit(-1);
    auto txn_node = pb_plan_node.mutable_derive_node()->mutable_transaction_node();
    txn_node->set_txn_cmd(cmd_type);

    // create store txn node
    TransactionNode* store_txn_node = new (std::nothrow) TransactionNode;
    if (store_txn_node == nullptr) {
        DB_WARNING("create store_txn_node failed");
        return nullptr;
    }
    store_txn_node->init(pb_plan_node);
    return store_txn_node;
}
}  // namespace baikaldb

/* vim: set ts=4 sw=4 sts=4 tw=100 */
