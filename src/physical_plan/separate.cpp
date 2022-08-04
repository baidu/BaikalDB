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
#include "dual_scan_node.h"
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
    if (ctx->is_explain && ctx->explain_type != SHOW_PLAN) {
        return 0;
    }
    ExecNode* plan = ctx->root;
    PacketNode* packet_node = static_cast<PacketNode*>(plan->get_node(pb::PACKET_NODE));
    if (packet_node == nullptr) {
        return -1;
    }
    if (packet_node->children_size() == 0 && !ctx->has_derived_table) {
        return 0;
    }
    //DB_WARNING("need_seperate:%d", plan->need_seperate());
    if (!plan->need_seperate() && !ctx->has_derived_table) {
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
    case pb::OP_LOAD: {
        ret = separate_load(ctx);
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
    for (size_t i = 0; i < ctx->sub_query_plans.size(); i++) {
        auto select_ctx = ctx->sub_query_plans[i];
        ExecNode* select_plan = select_ctx->root;
        PacketNode* packet_node = static_cast<PacketNode*>(select_plan->get_node(pb::PACKET_NODE));
        union_node->steal_projections(packet_node->mutable_projections());
        union_node->add_child(packet_node->children(0));
        packet_node->clear_children();
        auto state = select_ctx->get_runtime_state();
        if (state->init(select_ctx.get(), nullptr) < 0) {
            DB_WARNING("init runtime_state failed");
            return -1;
        }
        union_node->mutable_select_runtime_states()->emplace_back(state.get());
    }
    return 0;
}

int Separate::create_full_export_node(ExecNode* plan) {
    _is_first_full_export = false;
    std::vector<ExecNode*> scan_nodes;
    plan->get_node(pb::SCAN_NODE, scan_nodes);
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
    static_cast<RocksdbScanNode*>(scan_nodes[0])->set_related_manager_node(export_node.get());

    if (limit_node != nullptr) {
        export_node->add_child(limit_node->children(0));
        limit_node->clear_children();
        limit_node->add_child(export_node.release());
    } else if (packet_node != nullptr) {
        // 普通plan
        export_node->add_child(packet_node->children(0));
        packet_node->clear_children();
        packet_node->add_child(export_node.release());
    } else {
        // apply plan
        ExecNode* parent = plan->get_parent();
        export_node->add_child(plan);
        parent->replace_child(plan, export_node.release());
    }
    return 0;
}

int Separate::separate_select(QueryContext* ctx) {
    ExecNode* plan = ctx->root;
    std::vector<ExecNode*> join_nodes;
    plan->get_node(pb::JOIN_NODE, join_nodes);
    std::vector<ExecNode*> apply_nodes;
    plan->get_node(pb::APPLY_NODE, apply_nodes);
    if (join_nodes.size() == 0 && apply_nodes.size() == 0) {
        return separate_simple_select(ctx, plan);
    }
    if (apply_nodes.size() > 0) {
        int ret = separate_apply(ctx, apply_nodes);
        if (ret < 0) {
            return -1;
        }
    }
    if (join_nodes.size() > 0) {
        int ret = separate_join(ctx, join_nodes);
        if (ret < 0) {
            return -1;
        }
    }
    return 0;
}

//普通的select请求，考虑agg_node, sort_node, limit_node下推问题
int Separate::separate_simple_select(QueryContext* ctx, ExecNode* plan) {
    // join或者apply的情况下只有主表做full_export
    if (ctx->is_full_export && _is_first_full_export) {
        return create_full_export_node(plan);
    }
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
    std::unique_ptr<SelectManagerNode> manager_node(create_select_manager_node());
    if (manager_node == nullptr) {
        DB_WARNING("create manager_node failed");
        return -1;
    }
    if (scan_nodes.size() > 0) {
        std::map<int64_t, pb::RegionInfo> region_infos =
                static_cast<RocksdbScanNode*>(scan_nodes[0])->region_infos();
        manager_node->set_region_infos(region_infos);
        static_cast<RocksdbScanNode*>(scan_nodes[0])->set_related_manager_node(manager_node.get());
    }

    if (ctx->sub_query_plans.size() == 1) {
        auto sub_query_ctx = ctx->sub_query_plans[0];
        int ret = sub_query_ctx->get_runtime_state()->init(sub_query_ctx.get(), nullptr);
        if (ret < 0) {
            return -1;
        }
        manager_node->set_sub_query_runtime_state(sub_query_ctx->get_runtime_state().get());
        auto iter = ctx->derived_table_ctx_mapping.begin();
        int32_t tuple_id = iter->first;
        manager_node->set_slot_column_mapping(ctx->slot_column_mapping[tuple_id]);
        manager_node->set_derived_tuple_id(tuple_id);
        ExecNode* sub_query_plan = sub_query_ctx->root;
        PacketNode* packet_node = static_cast<PacketNode*>(sub_query_plan->get_node(pb::PACKET_NODE));
        manager_node->steal_projections(packet_node->mutable_projections());
        manager_node->set_sub_query_node(packet_node->children(0));
        packet_node->clear_children();
        FilterNode* filter_node = static_cast<FilterNode*>(plan->get_node(pb::WHERE_FILTER_NODE));
        if (filter_node != nullptr) {
            manager_node->add_child(filter_node->children(0));
            filter_node->clear_children();
            filter_node->add_child(manager_node.release());
            return 0;
        }
    } else if (ctx->sub_query_plans.size() != 0) {
        DB_WARNING("illegal plan, has multiple sub query ctx");
        for (auto iter : ctx->sub_query_plans) {
            DB_WARNING("sql:%s", iter->sql.c_str());
        }
        return -1;
    }

    if (agg_node != nullptr) {
        ExecNode* parent = agg_node->get_parent();
        pb::PlanNode pb_node;
        agg_node->transfer_pb(0, &pb_node);
        pb_node.set_node_type(pb::MERGE_AGG_NODE);
        pb_node.set_limit(-1);
        std::unique_ptr<AggNode> merge_agg_node(new (std::nothrow) AggNode);
        merge_agg_node->init(pb_node);

       if (ctx->sub_query_plans.size() > 0) {
            parent->replace_child(agg_node, merge_agg_node.get());
            merge_agg_node->add_child(agg_node);
            merge_agg_node.release();
            manager_node->add_child(agg_node->children(0));
            agg_node->clear_children();
            agg_node->add_child(manager_node.release());
            return 0;
        }
        manager_node->add_child(agg_node);
        merge_agg_node->add_child(manager_node.release());
        parent->replace_child(agg_node, merge_agg_node.release());
        return 0;
    }
    if (sort_node != nullptr) {
        manager_node->init_sort_info(sort_node);
        if (ctx->sub_query_plans.size() > 0) {
            manager_node->add_child(sort_node->children(0));
            sort_node->clear_children();
            sort_node->add_child(manager_node.release());
            return 0;
        }
        ExecNode* parent = sort_node->get_parent();
        manager_node->add_child(sort_node);
        parent->replace_child(sort_node, manager_node.release());
        return 0;
    }
    if (limit_node != nullptr) {
        manager_node->add_child(limit_node->children(0));
        limit_node->clear_children();
        limit_node->add_child(manager_node.release());
        return 0;
    }
    if (packet_node != nullptr) {
        // 普通plan
        manager_node->add_child(packet_node->children(0));
        packet_node->clear_children();
        packet_node->add_child(manager_node.release());
    } else {
        // apply plan
        ExecNode* parent = plan->get_parent();
        manager_node->add_child(plan);
        parent->replace_child(plan, manager_node.release());
    }
    return 0;
}

int Separate::separate_apply(QueryContext* ctx, const std::vector<ExecNode*>& apply_nodes) {
    auto sperate_simple_plan = [this, ctx](ExecNode* plan) -> int {
        std::vector<ExecNode*> join_nodes;
        plan->get_node(pb::JOIN_NODE, join_nodes);
        std::vector<ExecNode*> apply_nodes;
        plan->get_node(pb::APPLY_NODE, apply_nodes);
        if (join_nodes.size() == 0 && apply_nodes.size() == 0) {
            int ret = separate_simple_select(ctx, plan);
            if (ret < 0) {
                return -1;
            }
        }
        return 0;
    };
    for (auto& apply : apply_nodes) {
        int ret = sperate_simple_plan(apply->children(0));
        if (ret < 0) {
            return -1;
        }
        ret = sperate_simple_plan(apply->children(1));
        if (ret < 0) {
            return -1;
        }
    }
    return 0;
}
int Separate::separate_join(QueryContext* ctx, const std::vector<ExecNode*>& join_nodes) {
    for (auto& join : join_nodes) {
        std::vector<ExecNode*> scan_nodes;
        join->join_get_scan_nodes(pb::SCAN_NODE, scan_nodes);
        std::vector<ExecNode*> dual_scan_nodes;
        join->join_get_scan_nodes(pb::DUAL_SCAN_NODE, dual_scan_nodes);
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

            std::unique_ptr<ExecNode> manager_node;
            // join或者apply的情况下只有主表做full_export
            if (ctx->is_full_export && _is_first_full_export) {
                _is_first_full_export = false;
                manager_node.reset(new (std::nothrow) FullExportNode);
                pb::PlanNode pb_fetch_node;
                pb_fetch_node.set_node_type(pb::FULL_EXPORT_NODE);
                pb_fetch_node.set_limit(-1);
                manager_node->init(pb_fetch_node);
            } else {
                manager_node.reset(create_select_manager_node());
            }
            if (manager_node == nullptr) {
                DB_WARNING("create manager_node failed");
                return -1;
            }
            static_cast<RocksdbScanNode*>(scan_node_ptr)->set_related_manager_node(manager_node.get());
            std::map<int64_t, pb::RegionInfo> region_infos =
                    static_cast<RocksdbScanNode*>(scan_node_ptr)->region_infos();
            manager_node->set_region_infos(region_infos);
            manager_node_parent->replace_child(manager_node_child, manager_node.get());
            manager_node->add_child(manager_node_child);
            manager_node.release();
        }
        for (auto& scan_node_ptr : dual_scan_nodes) {
            DualScanNode* dual = static_cast<DualScanNode*>(scan_node_ptr);
            auto iter = ctx->derived_table_ctx_mapping.find(dual->tuple_id());
            if (iter == ctx->derived_table_ctx_mapping.end()) {
                DB_WARNING("illegal plan table_id:%ld _tuple_id:%d", dual->table_id(), dual->tuple_id());
                return -1;
            }
            int32_t tuple_id = iter->first;
            auto sub_query_ctx = iter->second;
            int ret = sub_query_ctx->get_runtime_state()->init(sub_query_ctx.get(), nullptr);
            if (ret < 0) {
                return -1;
            }
            ExecNode* manager_node_parent = scan_node_ptr->get_parent();
            ExecNode* manager_node_child = scan_node_ptr;
            if (manager_node_parent == nullptr) {
                DB_WARNING("fether node children is null");
                return -1;
            }

            std::unique_ptr<SelectManagerNode> manager_node(create_select_manager_node());
            if (manager_node == nullptr) {
                DB_WARNING("create manager_node failed");
                return -1;
            }
            manager_node_parent->replace_child(manager_node_child, manager_node.get());
            manager_node->add_child(manager_node_child);
            manager_node->set_sub_query_runtime_state(sub_query_ctx->get_runtime_state().get());
            manager_node->set_slot_column_mapping(ctx->slot_column_mapping[tuple_id]);
            manager_node->set_derived_tuple_id(tuple_id);
            ExecNode* sub_query_plan = sub_query_ctx->root;
            PacketNode* packet_node = static_cast<PacketNode*>(sub_query_plan->get_node(pb::PACKET_NODE));
            manager_node->steal_projections(packet_node->mutable_projections());
            manager_node->set_sub_query_node(packet_node->children(0));
            packet_node->clear_children();
            manager_node.release();
        }

        // sort_node, limit_node pushdown,暂不考虑子查询
        JoinNode* join_node = static_cast<JoinNode*>(join);
        if ((join_node->join_type() != pb::LEFT_JOIN
                && join_node->join_type() != pb::RIGHT_JOIN)
                || ctx->sub_query_plans.size() > 0) {
            continue;
        }
        LimitNode* limit_node = nullptr;
        AggNode* agg_node = nullptr;
        SortNode* sort_node = nullptr;
        ExecNode* parent = join_node->get_parent();
        while (parent->node_type() != pb::JOIN_NODE &&
               parent != ctx->root) {
            if (parent->node_type() == pb::LIMIT_NODE) {
                limit_node = static_cast<LimitNode*>(parent);
            }
            if (parent->node_type() == pb::AGG_NODE) {
                agg_node = static_cast<AggNode*>(parent);
            }
            if (parent->node_type() == pb::SORT_NODE) {
                sort_node = static_cast<SortNode*>(parent);
            }
            parent = parent->get_parent();
        }
        if (agg_node != nullptr) {
            continue;
        } else if (limit_node != nullptr) {
            parent = limit_node;
        } else if (sort_node != nullptr) {
            parent = sort_node->get_parent();
        } else {
            continue;
        }
        bool need_pushdown = true;
        std::unordered_set<int32_t> *tuple_ids = nullptr;
        ExecNode* node = nullptr;
        if (join_node->join_type() == pb::LEFT_JOIN) {
            tuple_ids = join_node->left_tuple_ids();
            node = join_node->children(0);
        } else if (join_node->join_type() == pb::RIGHT_JOIN) {
            tuple_ids = join_node->right_tuple_ids();
            node = join_node->children(1);
        }
        ExecNode* join_child = node;
        while (node->children_size() > 0) {
            if (node->node_type() == pb::JOIN_NODE) {
                break;
            }
            if (node->node_type() == pb::SELECT_MANAGER_NODE) {
                break;
            }
            node = node->children(0);
        }
        if (sort_node != nullptr) {
            for (auto expr : sort_node->slot_order_exprs()) {
                if(!join_node->expr_in_tuple_ids(*tuple_ids, expr)) {
                    need_pushdown = false;
                    break;
                }
            }
        }
        ExecNode* start = parent->children(0);
        ExecNode* end = join->get_parent();
        if (start == join) {
            need_pushdown = false;
        }
        if (!need_pushdown) {
            continue;
        }
        if (node->node_type() == pb::SELECT_MANAGER_NODE) {
            // parent(limit)->sort->filter->join->manager->child =>
            // parent(limit)->join->manager->sort->filter->child
            SelectManagerNode* manager_node = static_cast<SelectManagerNode*>(node);
            ExecNode* child = manager_node->children(0);

            end->clear_children();
            end->add_child(child);
            manager_node->clear_children();
            manager_node->add_child(start);
            parent->replace_child(start, join);
            if (sort_node != nullptr) {
                manager_node->init_sort_info(sort_node);
            }
        } else if (node->node_type() == pb::JOIN_NODE) {
            // parent(limit)->sort->filter->join->join_child->join2 =>
            // parent(limit)->join->sort->filter->join_child->join2
            ExecNode* child = join_child;
            end->clear_children();
            end->add_child(child);
            join->replace_child(join_child, start);
            parent->replace_child(start, join);
        }
    }
    return 0;
}

bool Separate::need_separate_single_txn(QueryContext* ctx, const int64_t main_table_id) {
    if (ctx->get_runtime_state()->single_sql_autocommit() && 
        (ctx->enable_2pc 
            || _factory->need_begin_txn(main_table_id)
            || ctx->open_binlog || ctx->execute_global_flow)) {
        auto client = ctx->client_conn;
        if (client != nullptr && client->txn_id == 0) {
            client->on_begin();
        }
        return true;
    }
    return false;
}

bool Separate::need_separate_plan(QueryContext* ctx, const int64_t main_table_id) {
    if (_factory->has_global_index(main_table_id) || ctx->execute_global_flow) {
        return true;
    }
    return false;
}

int Separate::separate_load(QueryContext* ctx) {
    ExecNode* plan = ctx->root;
    InsertNode* insert_node = static_cast<InsertNode*>(plan->get_node(pb::INSERT_NODE));
    ExecNode* parent = insert_node->get_parent();
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
    if (ctx->row_ttl_duration > 0) {
        manager_node->set_row_ttl_duration(ctx->row_ttl_duration);
        _row_ttl_duration = ctx->row_ttl_duration;
    }
    if (!need_separate_plan(ctx, main_table_id)) {
        manager_node->set_op_type(pb::OP_INSERT);
        manager_node->set_region_infos(insert_node->region_infos());
        manager_node->add_child(insert_node);
        manager_node->set_table_id(main_table_id);
        manager_node->set_selected_field_ids(insert_node->prepared_field_ids());
        int ret = manager_node->init_insert_info(insert_node, true);
        if (ret < 0) {
            return -1;
        }
    } else {
        int ret = separate_global_insert(manager_node.get(), insert_node);
        if (ret < 0) {
            DB_WARNING("separte global insert failed table_id:%ld", main_table_id);
            return -1;
        }
    }
    ctx->get_runtime_state()->set_single_txn_need_separate_execute(true);
    parent->clear_children();
    parent->add_child(manager_node.release());
    if (need_separate_single_txn(ctx, main_table_id)) {
        separate_single_txn(ctx, parent, pb::OP_INSERT);
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
    if (ctx->row_ttl_duration > 0) {
        manager_node->set_row_ttl_duration(ctx->row_ttl_duration);
        _row_ttl_duration = ctx->row_ttl_duration;
    }
    int64_t main_table_id = insert_node->table_id();
    if (!need_separate_plan(ctx, main_table_id)) {
        manager_node->set_op_type(pb::OP_INSERT);
        manager_node->set_region_infos(insert_node->region_infos());
        manager_node->add_child(insert_node);
        manager_node->set_table_id(main_table_id);
        manager_node->set_selected_field_ids(insert_node->prepared_field_ids());
        int ret = manager_node->init_insert_info(insert_node, true);
        if (ret < 0) {
            return -1;
        }
    } else {
        int ret = separate_global_insert(manager_node.get(), insert_node);
        if (ret < 0) {
            DB_WARNING("separte global insert failed table_id:%ld", main_table_id);
            return -1;
        }
    }

    if (ctx->sub_query_plans.size() > 0) {
        auto sub_query_ctx = ctx->sub_query_plans[0];
        int ret = sub_query_ctx->get_runtime_state()->init(sub_query_ctx.get(), nullptr);
        if (ret < 0) {
            return -1;
        }
        manager_node->set_sub_query_runtime_state(sub_query_ctx->get_runtime_state().get());
        ExecNode* sub_query_plan = sub_query_ctx->root;
        // 单语句事务DML默认会和Prepare一起发送
        ctx->get_runtime_state()->set_single_txn_need_separate_execute(true);
        PacketNode* packet_node = static_cast<PacketNode*>(sub_query_plan->get_node(pb::PACKET_NODE));
        manager_node->steal_projections(packet_node->mutable_projections());
        manager_node->set_sub_query_node(packet_node->children(0));
        packet_node->clear_children();
    }

    packet_node->clear_children();
    packet_node->add_child(manager_node.release());
    if (need_separate_single_txn(ctx, main_table_id)) {
        separate_single_txn(ctx, packet_node, pb::OP_INSERT);
    }
    return 0;
}

// insert_node中的属性完全转义到manager_node中，析构insert_node
int Separate::separate_global_insert(InsertManagerNode* manager_node, InsertNode* insert_node) {
    int64_t table_id = insert_node->table_id();
    int ret = manager_node->init_insert_info(insert_node, false);
    if (ret < 0) {
        return -1;
    }
    // ignore
    if (manager_node->need_ignore()) {
        create_lock_node(table_id, pb::LOCK_GET, Separate::BOTH, manager_node);
        create_lock_node(table_id, pb::LOCK_NO, Separate::BOTH, manager_node);
    } else if (manager_node->is_replace()) {
        create_lock_node(table_id, pb::LOCK_GET, Separate::BOTH, manager_node);
        create_lock_node(table_id, pb::LOCK_GET_ONLY_PRIMARY, Separate::PRIMARY, manager_node);
        create_lock_node(table_id, pb::LOCK_DML, Separate::BOTH, manager_node);
    } else if (manager_node->on_dup_key_update()) {
        create_lock_node(table_id, pb::LOCK_GET, Separate::BOTH, manager_node);
        create_lock_node(table_id, pb::LOCK_GET_ONLY_PRIMARY, Separate::PRIMARY, manager_node);
        create_lock_node(table_id, pb::LOCK_DML, Separate::BOTH, manager_node);
    } else {
        // basic insert
        create_lock_node(table_id, pb::LOCK_DML, Separate::BOTH, manager_node);
    }
    // 复用
    delete insert_node;
    return 0;
}

int Separate::create_lock_node(
        int64_t table_id,
        pb::LockCmdType lock_type,
        Separate::NodeMode mode,
        ExecNode* manager_node) {
    auto table_info = _factory->get_table_info_ptr(table_id);
    if (table_info == nullptr) {
        return -1;
    }
    std::vector<int64_t> global_affected_indexs;
    std::vector<int64_t> global_unique_indexs;
    std::vector<int64_t> global_non_unique_indexs;
    std::vector<int64_t> local_affected_indexs;
    for (auto index_id : table_info->indices) {
        auto index_info = _factory->get_index_info_ptr(index_id);
        if (index_info == nullptr) {
            return -1;
        }
        if (index_info->index_hint_status == pb::IHS_VIRTUAL) {
            DB_NOTICE("index info is virtual, skip.");
            continue;
        }
        if (index_info->index_hint_status == pb::IHS_DISABLE
            && index_info->state == pb::IS_DELETE_LOCAL) {
            continue;
        }
        if (index_info->is_global) {
            if (index_info->state == pb::IS_NONE) {
                DB_NOTICE("index info is NONE, skip.");
                continue;
            }
            if (index_info->type == pb::I_UNIQ) {
                global_unique_indexs.emplace_back(index_id);
            } else if (lock_type != pb::LOCK_GET) {
                //LOGK_GET只需要关注全局唯一索引
                global_non_unique_indexs.emplace_back(index_id);
            }
        } else {
            if (index_info->type == pb::I_UNIQ) {
                local_affected_indexs.emplace_back(index_id);
            } else if (lock_type != pb::LOCK_GET) {
                //LOGK_GET只需要关注全局唯一索引
                local_affected_indexs.emplace_back(index_id);
            }
        }
    }
    global_affected_indexs.insert(global_affected_indexs.end(), global_unique_indexs.begin(), global_unique_indexs.end());
    global_affected_indexs.insert(global_affected_indexs.end(), global_non_unique_indexs.begin(), global_non_unique_indexs.end());
    return create_lock_node(table_id, lock_type, mode, global_affected_indexs, local_affected_indexs, manager_node);
}
int Separate::create_lock_node(
        int64_t table_id,
        pb::LockCmdType lock_type,
        Separate::NodeMode mode,
        const std::vector<int64_t>& global_affected_indexs,
        const std::vector<int64_t>& local_affected_indexs,
        ExecNode* manager_node) {
    
    //构造LockAndPutPrimaryNode
    if (mode == Separate::BOTH || mode == Separate::PRIMARY) {
        std::unique_ptr<LockPrimaryNode> primary_node(new (std::nothrow) LockPrimaryNode);
        if (primary_node == nullptr) {
            DB_WARNING("create manager_node failed");
            return -1;
        }
        pb::PlanNode plan_node;
        plan_node.set_node_type(pb::LOCK_PRIMARY_NODE);
        plan_node.set_limit(-1);
        plan_node.set_num_children(0);
        auto lock_primary_node = plan_node.mutable_derive_node()->mutable_lock_primary_node();
        lock_primary_node->set_lock_type(lock_type);
        lock_primary_node->set_table_id(table_id);
        lock_primary_node->set_row_ttl_duration_s(_row_ttl_duration);
        primary_node->init(plan_node);
        primary_node->set_affected_index_ids(local_affected_indexs); 
        manager_node->add_child(primary_node.release());
    }
    //构造LockAndPutSecondaryNode
    if (mode == Separate::BOTH || mode == Separate::GLOBAL) {
        for (auto index_id : global_affected_indexs) {
            std::unique_ptr<LockSecondaryNode> secondary_node(new (std::nothrow) LockSecondaryNode);
            if (secondary_node == nullptr) {
                DB_WARNING("create manager_node failed");
                return -1;
            }
            pb::PlanNode plan_node;
            plan_node.set_node_type(pb::LOCK_SECONDARY_NODE);
            plan_node.set_limit(-1);
            plan_node.set_num_children(0);
            auto lock_secondary_node = plan_node.mutable_derive_node()->mutable_lock_secondary_node();
            lock_secondary_node->set_lock_type(lock_type);
            lock_secondary_node->set_global_index_id(index_id);
            lock_secondary_node->set_table_id(table_id);
            lock_secondary_node->set_row_ttl_duration_s(_row_ttl_duration);
            secondary_node->init(plan_node);
            manager_node->add_child(secondary_node.release());
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
    if (!need_separate_plan(ctx, main_table_id)) {
        auto region_infos = static_cast<RocksdbScanNode*>(scan_nodes[0])->region_infos();
        manager_node->set_op_type(pb::OP_UPDATE);
        manager_node->set_region_infos(region_infos);
        manager_node->add_child(packet_node->children(0));
    } else {
        ret = separate_global_update(manager_node.get(), update_node, scan_nodes[0]);
        if (ret < 0) {
            DB_WARNING("separte global update failed table_id:%ld", main_table_id);
            return -1;
        }
    }
    packet_node->clear_children();
    packet_node->add_child(manager_node.release());
    if (need_separate_single_txn(ctx, main_table_id)) {
        separate_single_txn(ctx, packet_node, pb::OP_UPDATE);
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
    int ret = manager_node->init_delete_info(delete_node->pb_node().derive_node().delete_node());
    if (ret < 0) {
        return -1;
    }
    if (!need_separate_plan(ctx, main_table_id)) {
        auto region_infos = static_cast<RocksdbScanNode*>(scan_nodes[0])->region_infos();
        manager_node->set_op_type(pb::OP_DELETE);
        manager_node->set_region_infos(region_infos);
        manager_node->add_child(packet_node->children(0));       
    } else {
        int ret = separate_global_delete(manager_node.get(), delete_node, scan_nodes[0]);
        if (ret < 0) {
            DB_WARNING("separte global delete failed table_id:%ld", main_table_id);
            return -1;
        }
    }
    packet_node->clear_children();
    packet_node->add_child(manager_node.release());
    if (need_separate_single_txn(ctx, main_table_id)) {
        separate_single_txn(ctx, packet_node, pb::OP_DELETE);
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

    std::unique_ptr<SelectManagerNode> select_manager_node(create_select_manager_node());
    if (select_manager_node == nullptr) {
        DB_WARNING("create manager_node failed");
        return -1;
    }
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
            Separate::PRIMARY,
            manager_node->global_affected_index_ids(),
            manager_node->local_affected_index_ids(),
            delete_manager_node.get());
    create_lock_node(
            main_table_id,
            pb::LOCK_DML,
            Separate::GLOBAL,
            manager_node->global_affected_index_ids(),
            manager_node->local_affected_index_ids(),
            delete_manager_node.get());
    LockPrimaryNode* pri_node = static_cast<LockPrimaryNode*>(delete_manager_node->children(1));
    pri_node->set_affect_primary(manager_node->affect_primary());
    FilterNode* where_filter_node = static_cast<FilterNode*>(delete_manager_node->get_node(pb::WHERE_FILTER_NODE));
    if (where_filter_node != nullptr) {
        for (auto conjunct : *(where_filter_node->mutable_conjuncts())) {
            pri_node->add_conjunct(conjunct);
        }
    }
    FilterNode* table_filter_node = static_cast<FilterNode*>(delete_manager_node->get_node(pb::TABLE_FILTER_NODE));
    if (table_filter_node != nullptr) {
        for (auto conjunct : *(table_filter_node->mutable_conjuncts())) {
            pri_node->add_conjunct(conjunct);
        }
    }

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
    create_lock_node(main_table_id, pb::LOCK_DML, Separate::BOTH, 
            manager_node->global_affected_index_ids(), 
            manager_node->local_affected_index_ids(), 
            insert_manager_node.get());
    pri_node = static_cast<LockPrimaryNode*>(insert_manager_node->children(0));
    pri_node->set_affect_primary(manager_node->affect_primary());
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
    int64_t main_table_id = delete_node->table_id();
    std::unique_ptr<SelectManagerNode> select_manager_node(create_select_manager_node());
    if (select_manager_node.get() == nullptr) {
        DB_WARNING("create manager_node failed");
        return -1;
    }
    std::map<int64_t, pb::RegionInfo> region_infos =
            static_cast<RocksdbScanNode*>(scan_node)->region_infos();
    select_manager_node->set_region_infos(region_infos);
    select_manager_node->add_child(delete_node->children(0));
    manager_node->add_child(select_manager_node.release());
    delete_node->clear_children();

    create_lock_node(main_table_id, pb::LOCK_GET_DML, Separate::PRIMARY, manager_node);
    create_lock_node(main_table_id, pb::LOCK_DML, Separate::GLOBAL, manager_node);
    // manager_node->children(0)->add_child(delete_node->children(0));

    LockPrimaryNode* pri_node = static_cast<LockPrimaryNode*>(manager_node->children(1));
    FilterNode* where_filter_node = static_cast<FilterNode*>(manager_node->get_node(pb::WHERE_FILTER_NODE));
        if (where_filter_node != nullptr) {
            for (auto conjunct : *(where_filter_node->mutable_conjuncts())) {
                pri_node->add_conjunct(conjunct);
            }
        }
    FilterNode* table_filter_node = static_cast<FilterNode*>(manager_node->get_node(pb::TABLE_FILTER_NODE));
    if (table_filter_node != nullptr) {
        for (auto conjunct : *(table_filter_node->mutable_conjuncts())) {
            pri_node->add_conjunct(conjunct);
        }
    }
    delete delete_node;
    return 0;
}

template<typename T>
int Separate::separate_single_txn(QueryContext* ctx, T* node, pb::OpType op_type) {
    // create baikaldb commit node
    pb::PlanNode pb_plan_node;
    pb_plan_node.set_node_type(pb::SIGNEL_TXN_MANAGER_NODE);
    pb_plan_node.set_limit(-1);
    pb_plan_node.set_num_children(5);
    SingleTxnManagerNode* txn_manager_node = new (std::nothrow) SingleTxnManagerNode;
    if (txn_manager_node == nullptr) {
        DB_WARNING("create store_txn_node failed");
        return -1;
    }
    txn_manager_node->init(pb_plan_node);
    txn_manager_node->set_op_type(op_type);
    ExecNode* dml_root = node->children(0);
    node->clear_children();
    node->add_child(txn_manager_node);
    // create store begin node
    std::unique_ptr<TransactionNode> store_begin_node(create_txn_node(pb::TXN_BEGIN_STORE, ctx->user_info->txn_lock_timeout));
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
        std::unique_ptr<TransactionNode> store_begin_node(create_txn_node(pb::TXN_BEGIN_STORE, ctx->user_info->txn_lock_timeout));
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
        std::unique_ptr<TransactionNode> store_begin_node(create_txn_node(pb::TXN_BEGIN_STORE, ctx->user_info->txn_lock_timeout));
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
    std::unique_ptr<TransactionNode> store_begin_node(create_txn_node(pb::TXN_BEGIN_STORE, ctx->user_info->txn_lock_timeout));
    if (store_begin_node.get() == nullptr) {
        DB_WARNING("create store_begin_node failed");
        return -1;
    }
    begin_node->add_child(store_begin_node.release());
    return 0;
}

TransactionNode* Separate::create_txn_node(pb::TxnCmdType cmd_type, int64_t txn_lock_timeout) {
    // create fetcher node
    pb::PlanNode pb_plan_node;
    // pb_plan_node.set_txn_id(txn_id);
    pb_plan_node.set_node_type(pb::TRANSACTION_NODE);
    pb_plan_node.set_limit(-1);
    pb_plan_node.set_num_children(0);
    auto txn_node = pb_plan_node.mutable_derive_node()->mutable_transaction_node();
    txn_node->set_txn_cmd(cmd_type);

    // create store txn node
    TransactionNode* store_txn_node = new (std::nothrow) TransactionNode;
    if (store_txn_node == nullptr) {
        DB_WARNING("create store_txn_node failed");
        return nullptr;
    }
    if (txn_lock_timeout > 0) {
        store_txn_node->set_txn_lock_timeout(txn_lock_timeout);
    }
    store_txn_node->init(pb_plan_node);
    return store_txn_node;
}

SelectManagerNode* Separate::create_select_manager_node() {
    pb::PlanNode pb_manager_node;
    pb_manager_node.set_node_type(pb::SELECT_MANAGER_NODE);
    pb_manager_node.set_limit(-1);

    SelectManagerNode* manager_node = new (std::nothrow) SelectManagerNode;
    if (manager_node == nullptr) {
        DB_WARNING("create manager_node failed");
        return nullptr;
    }
    manager_node->init(pb_manager_node);
    return manager_node;
}

}  // namespace baikaldb

/* vim: set ts=4 sw=4 sts=4 tw=100 */
