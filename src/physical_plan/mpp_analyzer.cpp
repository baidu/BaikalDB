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

#include "plan.pb.h"
#include "mpp_analyzer.h"
#include "join_node.h"
#include "dual_scan_node.h"
#include "vectorize_helpper.h"
#include "filter_node.h"
#include "agg_node.h"

namespace baikaldb {
DECLARE_int32(mpp_hash_partition_num);

int MppAnalyzer::analyze(QueryContext* ctx) {
    if (ctx->get_runtime_state()->execute_type == pb::EXEC_ARROW_ACERO) {
        restore_sort_pushdown_for_join(ctx);
    }
    if (!ctx->use_mpp) {
        return 0;
    }
    // 包含DBLink Mysql表的查询不支持MPP执行
    if (ctx->has_dblink_mysql) {
        return 0;
    }
    // 设置每个节点的partition_property和schema
    if (0 != ctx->root->set_partition_property_and_schema(ctx)) {
        return -1;
    }
    if (ctx->is_from_subquery || ctx->is_union_subquery) {
        return 0;
    }
    // 最外层的runtime_state需要先init, 设置exchange计划可能需要查询表全局索引
    RuntimeState& state = *ctx->get_runtime_state();
    DataBuffer* send_buffer = ctx->client_conn == nullptr ? nullptr : ctx->client_conn->send_buf;
    if (state.init(ctx, send_buffer) < 0) {
        DB_FATAL("RuntimeState init fail");
        return -1;
    }

    // 顶端packetNode是SinglePartitionType
    ctx->root->partition_property()->set_single_partition();

    ctx->root_fragment = std::make_shared<FragmentInfo>();
    SmartFragment& root_fragment = ctx->root_fragment;
    root_fragment->log_id = ctx->stat_info.log_id;
    root_fragment->root = ctx->root;
    root_fragment->fragment_id = ctx->fragment_max_id++;
    root_fragment->runtime_state = ctx->get_runtime_state().get();
    root_fragment->last_runtime_state = root_fragment->runtime_state;
    // 判断partition属性加入exchange节点并拆分fragment
    if (0 != add_exchange(ctx, root_fragment)) {
        return -1;
    }
    // 调整fragment, 如limit/limit+sort下推等
    if (0 != optimize_fragment(ctx, root_fragment)) {
        return -1;
    }
    // 构建exchange元信息, 如构建Exchange对端分布信息
    if (0 != build_fragment(ctx, root_fragment)) {
        return -1;
    }
    if (ctx->fragments.size() == 1) {
        // 没拆成多fragment, 不走mpp
        clear_mpp_flag(ctx);
        ctx->fragments.clear();
    }
    return 0;
}

void MppAnalyzer::clear_mpp_flag(QueryContext* ctx) {
    if (ctx->expr_params.is_expr_subquery
            && !ctx->expr_params.is_correlated_subquery) {
        return;
    }
    ctx->use_mpp = false;
    ctx->get_runtime_state()->use_mpp = false;
    for (auto& sub_ctx : ctx->sub_query_plans) {
        clear_mpp_flag(sub_ctx.get());
    }
    return;
}

void MppAnalyzer::restore_sort_pushdown_for_join(QueryContext* ctx) {
    ExecNode* plan = ctx->root;
    std::vector<ExecNode*> join_nodes;
    plan->get_node(pb::JOIN_NODE, join_nodes);
    for (auto& join : join_nodes) {
        JoinNode* join_node = static_cast<JoinNode*>(join);
        ExecNode* outer_table = nullptr;
        if (join_node->join_type() == pb::LEFT_JOIN) {
            outer_table = join_node->children(0);
        } else if (join_node->join_type() == pb::RIGHT_JOIN) {
            outer_table = join_node->children(1);
        } 
        if (outer_table == nullptr) {
            continue;
        }
        ExecNode* pushdowned_sort = nullptr;
        if (outer_table->node_type() == pb::SORT_NODE) {
            pushdowned_sort = outer_table;
        } else if (outer_table->node_type() == pb::SELECT_MANAGER_NODE
            && outer_table->children_size() > 0
            && outer_table->children(0)->node_type() == pb::SORT_NODE) {
            pushdowned_sort = outer_table->children(0);
        }
        if (pushdowned_sort == nullptr) {
            continue;
        }
        pb::PlanNode pb_node;
        pushdowned_sort->transfer_pb(0, &pb_node);
        pb_node.set_node_type(pb::SORT_NODE);
        std::unique_ptr<SortNode> sort_node(new (std::nothrow) SortNode);
        sort_node->init(pb_node);
        // 两种情况
        // 1. packetnode -> new_sort_node -> xxx -> join (outer) -> sort_node(limit) -> join2
        // 2. packetnode -> new_sort_node -> xxx -> join (outer) -> selectmangager(limit) -> sort_node(limit)
        ExecNode* root = ctx->root;
        ExecNode* root_child = root->children(0);
        sort_node->add_child(root_child);
        root->replace_child(root_child, sort_node.release());
    }
    return;
}

int MppAnalyzer::create_exchange_node_pair(QueryContext* ctx, 
                            NodePartitionProperty* parent,
                            ExchangeReceiverNode** receiver,
                            ExchangeSenderNode** sender,
                            bool use_broadcast_shuffle) {
    // 创建receiver
    pb::PlanNode er_plan;
    er_plan.set_node_type(pb::EXCHANGE_RECEIVER_NODE);
    er_plan.set_num_children(0);
    er_plan.set_limit(-1);
    auto pb_er_node = er_plan.mutable_derive_node()->mutable_exchange_receiver_node();
    pb_er_node->set_log_id(ctx->stat_info.log_id);
    // er的partition_property与parent相同
    pb_er_node->mutable_partition_property()->set_type(parent->type);
    ExchangeReceiverNode* er_node = new (std::nothrow) ExchangeReceiverNode;
    if (er_node == nullptr) {
        DB_FATAL("create er_node failed");
        return -1;
    }
    if (0 != er_node->init(er_plan)) {
        delete er_node;
        return -1;
    }
    er_node->set_partition_property(parent);

    // 创建sender
    pb::PlanNode es_plan;
    es_plan.set_node_type(pb::EXCHANGE_SENDER_NODE);
    es_plan.set_num_children(1);
    es_plan.set_limit(-1);
    auto pb_es_node = es_plan.mutable_derive_node()->mutable_exchange_sender_node();
    pb_es_node->set_log_id(ctx->stat_info.log_id);
    pb_es_node->mutable_partition_property()->set_type(parent->type);
    if (use_broadcast_shuffle) {
        pb_es_node->mutable_partition_property()->set_type(pb::BroadcastPartitionType);
    }
    ExchangeSenderNode* es_node = new (std::nothrow) ExchangeSenderNode;
    if (es_node == nullptr) {
        DB_FATAL("create es_node failed");
        delete er_node;
        return -1;
    }
    if (0 != es_node->init(es_plan)) {
        delete es_node;
        delete er_node;
        return -1;
    }
    es_node->partition_property()->type = pb::AnyType;
    es_node->partition_property()->add_need_cast_string_columns(parent->need_cast_string_columns);
    er_node->set_exchange_sender(es_node);
    es_node->set_exchange_receiver(er_node);
    er_node->set_node_id(ctx->node_max_id);
    es_node->set_node_id(ctx->node_max_id++);
    *receiver = er_node;
    *sender = es_node;
    return 0;
}

int MppAnalyzer::add_exchange(QueryContext* ctx, SmartFragment& fragment) {
    if (fragment == nullptr || fragment->root == nullptr) {
        DB_FATAL("fragment is null or root is null");
        return -1;
    }
    fragment->partition_property = fragment->root->partition_property();
    int ret = add_exchange_and_separate(ctx, fragment, fragment->root, fragment->partition_property);
    if (ret != 0) {
        return ret;
    }
    for (auto& next_fragment : fragment->children) {
        // next_fragment的初始属性一定是AnyType
        ret = add_exchange(ctx, next_fragment);
        if (ret != 0) {
            return ret;
        }
    }
    return 0;
}

// 对index join的inner节点, 探测是否可以拆分exchange的, 如 
//              IJN (A.id=B.id)                      
//              /           \  
//             A          NIJN (B.id=C.id) 
//                     /                  \       
//              NIJN (B.name=D.name)       C (can shuffle by C.id) 
//              /           \
//            B(wait A)      D (broadcast)
// 只会拆分 store fragment, 可能是shuffle\broadcast Type
int MppAnalyzer::add_exchange_and_separate_for_index_join_inner_node(QueryContext* ctx, 
                                           SmartFragment& fragment,
                                           ExecNode* node,
                                           NodePartitionProperty* parent_property,
                                           std::set<ExecNode*>& need_runtime_filter_select_manager_nodes,
                                           bool need_use_broadcast_exchange) {
    DB_DEBUG("node: %s, parent_property(%p): %s, node_property(%p): %s, need_runtime_filter_select_manager_nodes size: %ld", 
            pb::PlanNodeType_Name(node->node_type()).c_str(),
            fragment->partition_property,
            parent_property->print().c_str(),
            node->partition_property(),
            node->partition_property()->print().c_str(),
            need_runtime_filter_select_manager_nodes.size());
    if (node->partition_property()->has_no_input_data) {
        return 0;
    }
    // 是否到达store_fragment
    if (node->node_type() == pb::SELECT_MANAGER_NODE) {
        SelectManagerNode* select_manager = static_cast<SelectManagerNode*>(node);
        if (!select_manager->is_dual_scan()) {
            if (need_runtime_filter_select_manager_nodes.count(node) == 0) {
                return seperate_store_fragment(ctx, fragment, select_manager, parent_property, need_use_broadcast_exchange);
            } else {
                return 0;
            }
        }
    }
    // 检查分区必须完全一样
    bool need_add_exchange = false;
    if (node->partition_property()->type != pb::AnyType) {
        if (0 != check_need_add_exchange(parent_property, node->partition_property(), &need_add_exchange)) {
            return -1;
        }
    } else {
        node->set_partition_property(parent_property);
    }
    if (need_add_exchange) {
        // 该子树继续拆分需要是broadcast类型
        need_use_broadcast_exchange = true;
    }
    
    const std::unordered_set<std::string>& cast_string_hash_columns = parent_property->need_cast_string_columns;
    // 继续往下探测
    switch (node->node_type()) {
        case pb::JOIN_NODE: {
            JoinNode* join = static_cast<JoinNode*>(node);
            NodePartitionProperty outer_property;
            NodePartitionProperty inner_property;
            join->get_hash_partitions(outer_property, inner_property, cast_string_hash_columns);
            RuntimeState* now_last_runtime_state = fragment->last_runtime_state; 
            // 驱动表
            if (0 != add_exchange_and_separate_for_index_join_inner_node(
                            ctx, 
                            fragment, 
                            join->get_outter_node(), 
                            &outer_property,
                            need_runtime_filter_select_manager_nodes,
                            need_use_broadcast_exchange)) {
                return -1;
            }
            fragment->last_runtime_state = now_last_runtime_state;
            ExecNode* inner_node = join->get_inner_node();
            // 非驱动表
            if (join->is_use_index_join()
                 && ctx->get_runtime_state()->sign_exec_type != SIGN_EXEC_ARROW_FORCE_NO_INDEX_JOIN) {
                join->get_need_add_index_collector_cond_nodes(inner_node, need_runtime_filter_select_manager_nodes);
                if (0 != add_exchange_and_separate_for_index_join_inner_node(
                            ctx, 
                            fragment, 
                            inner_node, 
                            &inner_property,
                            need_runtime_filter_select_manager_nodes,
                            need_use_broadcast_exchange)) {
                    return -1;
                }
            } else {
                // no index join非驱动表继续探测是否需要加exchange
                if (0 != add_exchange_and_separate_for_index_join_inner_node(
                                ctx, 
                                fragment, 
                                inner_node, 
                                &inner_property,
                                need_runtime_filter_select_manager_nodes,
                                need_use_broadcast_exchange)) {
                    return -1;
                }
            }
            fragment->last_runtime_state = now_last_runtime_state;
            return 0;
        }
        case pb::DUAL_SCAN_NODE: {
            DualScanNode* dual_scan = static_cast<DualScanNode*>(node);
            ExecNode* subquery_node = dual_scan->sub_query_node();
            RuntimeState* subquery_runtime_state = dual_scan->sub_query_runtime_state();
            if (subquery_node == nullptr || subquery_runtime_state == nullptr) {
                DB_FATAL("dual scan node has no sub query or runtime state");
                return -1;
            }
            if (dual_scan->set_sub_query_node_partition_property(parent_property)) {
                return -1;
            }
            fragment->last_runtime_state = subquery_runtime_state;
            return add_exchange_and_separate_for_index_join_inner_node(
                            ctx, 
                            fragment, 
                            subquery_node, 
                            subquery_node->partition_property(),
                            need_runtime_filter_select_manager_nodes,
                            need_use_broadcast_exchange);
        }
        default: {
            for (auto& child : node->children()) {
                node->partition_property()->add_need_cast_string_columns(cast_string_hash_columns);
                if (0 != add_exchange_and_separate_for_index_join_inner_node(
                            ctx, 
                            fragment, 
                            child, 
                            node->partition_property(),
                            need_runtime_filter_select_manager_nodes,
                            need_use_broadcast_exchange)) {
                    return -1;
                }
            }
        }
    }
    return 0;
}

int MppAnalyzer::add_exchange_and_separate(QueryContext* ctx, 
                                           SmartFragment& fragment, 
                                           ExecNode* node, 
                                           NodePartitionProperty* parent_property) {
    bool need_add_exchange = false;

    DB_DEBUG("node: %s, parent_property(%p): %s, node_property(%p): %s", 
                pb::PlanNodeType_Name(node->node_type()).c_str(),
                fragment->partition_property,
                parent_property->print().c_str(),
                node->partition_property(),
                node->partition_property()->print().c_str());

    if (fragment->partition_property->type == pb::AnyType
          && node->partition_property()->type != pb::AnyType) {
        // 遇到第一个不是AnyType的节点, 那么这个fragment分区属性就是该节点的分区属性
        *(fragment->partition_property) = *(node->partition_property());
    }

    if (node->partition_property()->has_no_input_data) {
        return 0;
    }
    // 是否到达store_fragment
    if (node->node_type() == pb::SELECT_MANAGER_NODE) {
        SelectManagerNode* select_manager = static_cast<SelectManagerNode*>(node);
        if (!select_manager->is_dual_scan()) {
            return seperate_store_fragment(ctx, fragment, select_manager, parent_property);
        }
    }

    if (node->partition_property()->type != pb::AnyType) {
        // 判断是否需要exchange
        if (0 != check_need_add_exchange(parent_property, node->partition_property(), &need_add_exchange)){
            return -1;
        }
        if (need_add_exchange) {
            return seperate_db_fragment(ctx, fragment, node, parent_property);
        }
    } else {
        node->set_partition_property(parent_property);
    }
    const std::unordered_set<std::string>& cast_string_hash_columns = parent_property->need_cast_string_columns;
    // 无需exchange
    switch (node->node_type()) {
        case pb::JOIN_NODE: {
            JoinNode* join = static_cast<JoinNode*>(node);
            NodePartitionProperty outer_property;
            NodePartitionProperty inner_property;
            join->get_hash_partitions(outer_property, inner_property, cast_string_hash_columns);
            RuntimeState* now_last_runtime_state = fragment->last_runtime_state; 
            if (0 != add_exchange_and_separate(ctx, fragment, join->get_outter_node(), &outer_property)) {
                return -1;
            }
            fragment->last_runtime_state = now_last_runtime_state;
            // 处理非驱动表
            ExecNode* inner_node = join->get_inner_node();
            if (join->is_use_index_join()
                 && ctx->get_runtime_state()->sign_exec_type != SIGN_EXEC_ARROW_FORCE_NO_INDEX_JOIN) {
                std::set<ExecNode*> need_runtime_filter_select_manager_nodes;
                join->get_need_add_index_collector_cond_nodes(inner_node, need_runtime_filter_select_manager_nodes);
                if (0 != add_exchange_and_separate_for_index_join_inner_node(ctx, 
                            fragment, 
                            inner_node, 
                            &inner_property, 
                            need_runtime_filter_select_manager_nodes,
                            false)) {
                    return -1;
                }
            } else {
                // no index join非驱动表继续探测是否需要加exchange
                if (0 != add_exchange_and_separate(ctx, fragment, inner_node, &inner_property)) {
                    return -1;
                }
            }
            fragment->last_runtime_state = now_last_runtime_state;
            return 0;
        }
        case pb::DUAL_SCAN_NODE: {
            DualScanNode* dual_scan = static_cast<DualScanNode*>(node);
            ExecNode* subquery_node = dual_scan->sub_query_node();
            RuntimeState* subquery_runtime_state = dual_scan->sub_query_runtime_state();
            if (subquery_node == nullptr || subquery_runtime_state == nullptr) {
                DB_FATAL("dual scan node has no sub query or runtime state");
                return -1;
            }
            if (dual_scan->set_sub_query_node_partition_property(parent_property)) {
                return -1;
            }
            fragment->last_runtime_state = subquery_runtime_state;
            return add_exchange_and_separate(ctx, 
                                            fragment, 
                                            subquery_node, 
                                            subquery_node->partition_property());
        }
        default: {
            for (auto& child : node->children()) {
                node->partition_property()->add_need_cast_string_columns(cast_string_hash_columns);
                if (0 != add_exchange_and_separate(ctx, fragment, child, node->partition_property())) {
                    return -1;
                }
            }
        }
    }
    return 0;
}

int MppAnalyzer::seperate_store_fragment(QueryContext* ctx, 
                                        SmartFragment& fragment, 
                                        ExecNode* select_manager, 
                                        NodePartitionProperty* parent_property,
                                        bool need_use_broadcast_exchange) {
    // parent -> SelectManager -> store (xxx)
    ExchangeReceiverNode* receiver = nullptr;
    ExchangeSenderNode* sender = nullptr;
    if (0 != create_exchange_node_pair(ctx, parent_property, &receiver, &sender, need_use_broadcast_exchange)) {
        return -1;
    }
    receiver->set_relate_select_manager(select_manager);

    // link fragment tree
    SmartFragment store_fragment = std::make_shared<FragmentInfo>();
    store_fragment->log_id = ctx->stat_info.log_id;
    store_fragment->fragment_id = ctx->fragment_max_id++;
    store_fragment->root = sender;
    store_fragment->parent = fragment.get();
    store_fragment->partition_property = sender->partition_property();
    fragment->receivers.emplace_back(receiver);
    fragment->children.emplace_back(store_fragment);

    // link: parent -> er |  es -> store (xxx)
    ExecNode* parent = select_manager->get_parent();
    sender->add_child(select_manager->children(0));
    parent->replace_child(select_manager, receiver);
    // relate selectmanager -> es -> store (xxx)
    select_manager->mutable_children()->clear();
    select_manager->mutable_children()->emplace_back(sender);

    // set fragment info
    receiver->set_fragment_id(fragment->fragment_id);
    sender->set_fragment_id(store_fragment->fragment_id);
    receiver->set_sender_fragment_id(store_fragment->fragment_id);
    sender->set_receiver_fragment_id(fragment->fragment_id);

    // sortnode
    SortNode* sort_node = static_cast<SortNode*>(sender->get_node(pb::SORT_NODE));
    if (sort_node != nullptr) {
        receiver->init_sort_info(sort_node);
    }

    // 如果使用全局索引, 在这里串行访问全局索引, 生成主表的region信息, 供exchange使用, 否则exchange获取到的是全局索引的region信息
    // TODO 是否有更好的方法
    if (static_cast<SelectManagerNode*>(select_manager)->mpp_fetcher_global_index(fragment->last_runtime_state) != 0) {
        DB_FATAL("mpp execute fail: fetcher global index fail");
        return -1;
    }
    return 0;
}

int MppAnalyzer::seperate_db_fragment(QueryContext* ctx, SmartFragment& fragment, ExecNode* node, NodePartitionProperty* parent_property) {
    ExchangeReceiverNode* receiver = nullptr;
    ExchangeSenderNode* sender = nullptr;
    if (0 != create_exchange_node_pair(ctx, parent_property, &receiver, &sender)) {
        return -1;
    }

    // link fragment tree
    SmartFragment db_fragment = std::make_shared<FragmentInfo>();
    db_fragment->log_id = ctx->stat_info.log_id;
    db_fragment->fragment_id = ctx->fragment_max_id++;
    db_fragment->parent = fragment.get();
    db_fragment->root = sender;
    db_fragment->partition_property = sender->partition_property();
    db_fragment->runtime_state = fragment->last_runtime_state;
    db_fragment->last_runtime_state = fragment->last_runtime_state;
    fragment->receivers.emplace_back(receiver);
    fragment->children.emplace_back(db_fragment);

    // set fragment info
    receiver->set_fragment_id(fragment->fragment_id);
    sender->set_fragment_id(db_fragment->fragment_id);
    receiver->set_sender_fragment_id(db_fragment->fragment_id);
    sender->set_receiver_fragment_id(fragment->fragment_id);

    // link: parent -> er -> es -> node (xxx)
    ExecNode* parent = node->get_parent();
    if (parent->node_type() == pb::AGG_NODE) {
        AggNode* agg_node = static_cast<AggNode*>(parent);
        if (!agg_node->has_merger()) {
            // parent节点是aggnode, 没有merge agg, 则构建merge agg node
            // agg_parent -> agg -> node
            // ==>
            // agg_parent -> merge_agg -> er | es -> agg -> node (xxx)
            ExecNode* agg_parent = agg_node->get_parent();
            pb::PlanNode pb_node;
            agg_node->transfer_pb(0, &pb_node);
            pb_node.set_node_type(pb::MERGE_AGG_NODE);
            pb_node.set_limit(-1);
            std::unique_ptr<AggNode> merge_agg_node(new (std::nothrow) AggNode);
            merge_agg_node->init(pb_node);
            merge_agg_node->set_data_schema(agg_node->data_schema());
            merge_agg_node->set_partition_property(agg_node->partition_property());
            merge_agg_node->add_child(receiver);
            agg_parent->replace_child(agg_node, merge_agg_node.release());

            sender->add_child(agg_node);
            agg_node->set_has_merger(true);
            agg_node->partition_property()->set_any_partition();
            return 0;
        }
    }
    sender->add_child(node);
    parent->replace_child(node, receiver);
    return 0;
}

int MppAnalyzer::check_need_add_exchange(NodePartitionProperty* parent_propety, NodePartitionProperty* node_property, bool* need_add_exchange) {
    switch (parent_propety->type) {
        case pb::AnyType: 
            *need_add_exchange = false;
            return 0;
        case pb::SinglePartitionType:
            *need_add_exchange = (node_property->type != pb::SinglePartitionType);
            return 0;
        case pb::HashPartitionType: 
            if (node_property->type != pb::HashPartitionType) {
                *need_add_exchange = true;
                return 0;
            }
            for (auto& hash_columns : node_property->hash_partition_propertys) {
                if (parent_propety->hash_partition_propertys[0]->hash_partition_is_same(hash_columns.get())) {
                    *need_add_exchange = false;
                    return 0;
                }
            }
            *need_add_exchange = true;
            break;
        default:
            DB_FATAL("not support partition type: %d", node_property->type);
            return -1;
    }
    return 0;
}

int MppAnalyzer::optimize_fragment(QueryContext* ctx, SmartFragment& fragment) {
    if (fragment->receivers.size() == 0) {
        // store fragment
        return 0;
    }
    FragmentInfo* parent_fragment = fragment->parent;
    if (parent_fragment != nullptr
             && parent_fragment->partition_property->type == pb::SinglePartitionType) {
        
        ExchangeSenderNode* fragment_sender = static_cast<ExchangeSenderNode*>(fragment->root);
        ExchangeReceiverNode* parent_fragment_receiver = static_cast<ExchangeReceiverNode*>(fragment_sender->get_exchange_receiver_node());
        ExecNode* parent_fragment_pushdown_begin = parent_fragment_receiver->get_parent();
        
        // (sort)+limit下推
        ExecNode* push_down_copy_node = nullptr;
        ExecNode* parent_fragment_pushdown_end = nullptr;
        while (parent_fragment_pushdown_begin) {
            if (parent_fragment_pushdown_begin->node_type() != pb::SORT_NODE
                && parent_fragment_pushdown_begin->node_type() != pb::LIMIT_NODE) {
                break;
            }
            if (parent_fragment_pushdown_begin->node_type() == pb::SORT_NODE 
                    && parent_fragment_pushdown_begin->get_parent()->node_type() != pb::LIMIT_NODE) {
                // sort上游节点不是limit
                break;
            }

            pb::PlanNode pb_node;
            parent_fragment_pushdown_begin->transfer_pb(0, &pb_node);
            pb_node.set_node_type(parent_fragment_pushdown_begin->node_type());
            pb_node.set_limit(parent_fragment_pushdown_begin->get_limit());
            ExecNode* new_copy_node = nullptr;

            switch (parent_fragment_pushdown_begin->node_type()) {
                case pb::SORT_NODE: {
                    std::unique_ptr<SortNode> sort_node(new (std::nothrow) SortNode);
                    sort_node->init(pb_node);
                    new_copy_node = sort_node.release();
                    break;
                }
                case pb::LIMIT_NODE: {
                    LimitNode* limit = static_cast<LimitNode*>(parent_fragment_pushdown_begin);
                    int64_t new_limit = limit->get_offset() + limit->get_limit();

                    std::unique_ptr<LimitNode> limit_node(new (std::nothrow) LimitNode);
                    pb_node.mutable_derive_node()->mutable_limit_node()->clear_offset_expr();
                    pb_node.mutable_derive_node()->mutable_limit_node()->clear_count_expr();
                    pb_node.mutable_derive_node()->mutable_limit_node()->set_offset(0);
                    pb_node.set_limit(new_limit);
                    limit_node->init(pb_node);
                    new_copy_node = limit_node.release();
                    break;
                }
                default: 
                    break;
            }
            if (new_copy_node == nullptr) {
                break;
            }
            if (new_copy_node != parent_fragment_pushdown_begin) {
                new_copy_node->set_data_schema(parent_fragment_pushdown_begin->data_schema());
            }
            if (push_down_copy_node == nullptr) {
                push_down_copy_node = new_copy_node;
                parent_fragment_pushdown_end = new_copy_node;
            } else {
                new_copy_node->add_child(push_down_copy_node);
                push_down_copy_node = new_copy_node;
            }
            parent_fragment_pushdown_begin = parent_fragment_pushdown_begin->get_parent();
        }
        if (push_down_copy_node != nullptr) {
            ExecNode* fragment_sender_child = fragment_sender->children(0);
            parent_fragment_pushdown_end->add_child(fragment_sender_child);
            fragment_sender->replace_child(fragment_sender_child, push_down_copy_node);
        }
    }
    for (auto& child_fragment : fragment->children) {
        if (0 != optimize_fragment(ctx, child_fragment)) {
            return -1;
        }
    }
    return 0;
}

int MppAnalyzer::build_fragment(QueryContext* ctx, SmartFragment& fragment) {
    int fragment_id = fragment->fragment_id;
    ctx->fragments[fragment_id] = fragment;
    if (fragment->receivers.size() == 0) {
        // store fragment
        return 0;
    }
    if (fragment->root->node_type() == pb::PACKET_NODE) {
        // 需要特殊处理fragment0, 因为root是pack_node, 不是exchange sender
        for (auto i = 0; i < fragment->children.size(); i++) {
            SmartFragment& next_fragment = fragment->children[i];
            ExchangeSenderNode* next_fragment_sender = static_cast<ExchangeSenderNode*>(next_fragment->root);
            next_fragment_sender->add_destination(SchemaFactory::get_instance()->get_address());
            if (0 != build_fragment(ctx, next_fragment)) {
                return -1;
            }
        }
        return 0;
    }

    // other db fragment
    // 本fragment的sender, 先确定本fragment的地址
    ExchangeSenderNode* sender = static_cast<ExchangeSenderNode*>(fragment->root);
    // 先添加主db
    sender->add_address(SchemaFactory::get_instance()->get_address());
    if (fragment->partition_property->type == pb::HashPartitionType) {
        // rolling N-1 db
        int mpp_hash_partition_num = FLAGS_mpp_hash_partition_num;
        if (ctx->mpp_hash_num > 0) {
            mpp_hash_partition_num = ctx->mpp_hash_num;
        }
        std::vector<std::string> db_instances;
        if (0 != SchemaFactory::get_instance()->rolling_pick_db(mpp_hash_partition_num - 1, db_instances)) {
            DB_FATAL("pick db instances failed");
            return -1;
        }
        for (const auto& ith_db : db_instances) {
            sender->add_address(ith_db);
            ctx->db_to_fragments[ith_db].insert(fragment_id);
        }
    } 
    // 本fragment sender对应的上游receiver, 确定了本fragment分布地址, 就可以指定上游receiver的destination
    ExchangeReceiverNode* receiver = static_cast<ExchangeReceiverNode*>(sender->get_exchange_receiver_node());
    receiver->set_destination(sender->get_fragment_address()); 

    for (auto i = 0; i < fragment->children.size(); i++) {
        SmartFragment& next_fragment = fragment->children[i];
        ExchangeSenderNode* next_fragment_sender = static_cast<ExchangeSenderNode*>(next_fragment->root);
        if (fragment->partition_property->type == pb::SinglePartitionType 
            || fragment->partition_property->type == pb::HashPartitionType) {
            // 本fragment sender对应的下游sender, 确定了本fragment分布地址, 就可以指定下游sender的destination
            next_fragment_sender->set_destination(sender->get_fragment_address());
        } else {
            DB_FATAL("invalid partition type: %d", fragment->partition_property->type);
            return -1;
        }
        if (0 != build_fragment(ctx, next_fragment)) {
            return -1;
        }
    }
    return 0;
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
