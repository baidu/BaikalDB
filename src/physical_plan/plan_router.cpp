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

#include "plan_router.h"
#include "packet_node.h"
#include "network_socket.h"
#include "expr.h"
#include "slot_ref.h"
#include "scalar_fn_call.h"
#include "expr_node.h"
#include "literal.h"

namespace baikaldb {
int PlanRouter::analyze(QueryContext* ctx) {
    if (ctx->is_explain) {
        return 0;
    }
    ExecNode* plan = ctx->root;
    if (!plan->need_seperate()) {
        return 0;
    }
    _is_full_export = ctx->is_full_export;
    PacketNode* packet_node = static_cast<PacketNode*>(plan->get_node(pb::PACKET_NODE));
    if (packet_node != nullptr && packet_node->op_type() == pb::OP_LOAD) {
        return 0;
    }
    //DB_NOTICE("need_seperate:%d", plan->need_seperate());
    std::vector<ExecNode*> scan_nodes;
    std::vector<ExecNode*> dual_scan_nodes;
    plan->get_node(pb::SCAN_NODE, scan_nodes);
    plan->get_node(pb::DUAL_SCAN_NODE, dual_scan_nodes);
    InsertNode* insert_node = static_cast<InsertNode*>(plan->get_node(pb::INSERT_NODE));
    TruncateNode* truncate_node = static_cast<TruncateNode*>(plan->get_node(pb::TRUNCATE_NODE));
    KillNode* kill_node = static_cast<KillNode*>(plan->get_node(pb::KILL_NODE));
    TransactionNode* txn_node = static_cast<TransactionNode*>(plan->get_node(pb::TRANSACTION_NODE));

    size_t scan_size = scan_nodes.size() + dual_scan_nodes.size();
    if (scan_size > 0) {
        bool has_join = scan_size > 1;
        for (auto scan_node : scan_nodes) {
            auto ret = scan_node_analyze(static_cast<RocksdbScanNode*>(scan_node), ctx, has_join);
            if (ret != 0) {
                return ret;
            }
        }
    } else if (insert_node != nullptr) {
        return insert_node_analyze(insert_node, ctx);
    } else if (truncate_node != nullptr) {
        return truncate_node_analyze(truncate_node, ctx);
    } else if (kill_node != nullptr) {
        return kill_node_analyze(kill_node, ctx);
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
    auto index_ptr = schema_factory->get_index_info_ptr(table_id);
    if (index_ptr == nullptr) {
        DB_WARNING("invalid index info: %ld", table_id);
        return ret;
    }
    ret = schema_factory->get_region_by_key(
            *index_ptr, 
            ctx->insert_records, 
            node->insert_records_by_region(), 
            node->region_infos());
    if (ret < 0) {
        DB_WARNING("get_region_by_key:fail :%d", ret);
        return ret;
    }
    if (node->region_infos().size() == 0 && ctx->sub_query_plans.size() == 0) {
        DB_WARNING("region_infos.size = 0");
        return -1;
    }
    return 0;
}

int PlanRouter::scan_node_analyze(RocksdbScanNode* scan_node, QueryContext* ctx, bool has_join) {
    SchemaFactory* schema_factory = SchemaFactory::get_instance();
    if (ctx->debug_region_id != -1) {
        pb::RegionInfo info;
        int64_t table_id = scan_node->table_id();
        if (scan_node->main_scan_index() == nullptr) {
            DB_WARNING("main scan index is nullprt");
            return -1;
        }
        int ret = schema_factory->get_region_info(table_id, ctx->debug_region_id, info);
        if (ret == 0) {
            // scan_node->set_router_index_id(table_id);
            (scan_node->region_infos())[ctx->debug_region_id] = info;
            scan_node->main_scan_index()->region_infos[ctx->debug_region_id] = info;
            return 0;
        }
        //对于索引表，也支持debug模式
        TableInfo table_info = schema_factory->get_table_info(table_id);
        for (auto index_id : table_info.indices) {
            if (!schema_factory->is_global_index(index_id)) {
                continue;
            }
            ret = schema_factory->get_region_info(index_id, ctx->debug_region_id, info);
            if (ret == 0) {
                (scan_node->region_infos())[ctx->debug_region_id] = info;
                // scan_node->set_router_index_id(index_id);
                scan_node->main_scan_index()->region_infos[ctx->debug_region_id] = info;
                scan_node->set_covering_index(true);
                return 0;
            }
        }
        DB_WARNING("get region_info in debug mode failed, %ld", ctx->debug_region_id);
        return -1;
    }
    auto get_slot_id = [ctx](int32_t tuple_id, int32_t field_id)-> 
        int32_t {return ctx->get_slot_id(tuple_id, field_id);};
    auto get_tuple_desc = [ctx] (int32_t tuple_id)->
        pb::TupleDescriptor* { return ctx->get_tuple_desc(tuple_id);};
    return scan_plan_router(scan_node, get_slot_id, get_tuple_desc, has_join);
}

int PlanRouter::scan_plan_router(RocksdbScanNode* scan_node, 
    const std::function<int32_t(int32_t, int32_t)>& get_slot_id,
    const std::function<pb::TupleDescriptor*(int32_t)>& get_tuple_desc,
    bool has_join) {
    //pb::ScanNode* pb_scan_node = scan_node->mutable_pb_node()->mutable_derive_node()->mutable_scan_node();
    int64_t main_table_id = scan_node->table_id();
    SchemaFactory* schema_factory = SchemaFactory::get_instance(); 

    bool hit_global = false;
    bool covering_index = true;
    int idx = 0;
    bool is_full_export = _is_full_export;
    for (auto& scan_index_info : scan_node->scan_indexs()) {
        if (!scan_index_info.covering_index) {
            covering_index = false;
        }

        // 非首个index只有两种情况需要router，其他情况continue
        // 1. global learner
        // 2. router_index_id == index_id
        if (idx++ > 0 && scan_index_info.use_for != ScanIndexInfo::U_GLOBAL_LEARNER) {
            if (scan_index_info.router_index_id != scan_index_info.index_id) {
                continue;
            }
        }

        auto index_ptr = schema_factory->get_index_info_ptr(scan_index_info.router_index_id);
        if (index_ptr == nullptr) {
            DB_WARNING("invalid index info: %ld", scan_index_info.router_index_id);
            return -1;
        }

        if (index_ptr->is_global) {
            hit_global = true;
        }

        int ret = 0;
        switch (scan_node->router_policy()) {
        
        case RouterPolicy::RP_RANGE: {
            ret = schema_factory->get_region_by_key(main_table_id, 
                *index_ptr, scan_index_info.router_index,
                scan_index_info.region_infos,
                &scan_index_info.region_primary,
                scan_node->get_partition(),
                _is_full_export);
            // 只第一个scannode获取部分region
            _is_full_export = false;
            scan_node->set_region_infos(scan_index_info.region_infos);
            break;
        }
        case RouterPolicy::RP_REGION: {
            ret = schema_factory->get_region_by_key(scan_node->old_region_infos(), 
                scan_index_info.region_infos);
            scan_node->set_region_infos(scan_index_info.region_infos);
            break;
        }
        default:
            ret = -1;
            break;
        }
        
        if (ret < 0) {
            DB_WARNING("get_region_by_key:fail :%d", ret);
            return ret;
        }
        if (scan_index_info.router_index != nullptr && scan_index_info.region_primary.size() > 0) {
            scan_index_info.router_index->mutable_ranges()->Clear();
        }
    }
    //如果该表没有全局二级索引
    //full_export+join也需要把主键放入slot
    if (!schema_factory->has_global_index(main_table_id) && !is_full_export) {
        return 0;
    }
    bool need_put_pk = false;
    
    // 如有涉及有全局二级索引/fullexport的join表时，把主表的fields_id全部放到tuple里。
    // 该步骤后续如有性能问题的话，可以优化为在join_node里做plan router时候按需放进去。
    // 但按需放进去时， tuple已经在state->init时生成，要destory 掉，然后重新生成。这块处理一定要小心
    if (is_full_export || has_join) {
        need_put_pk = true;
    } else if (hit_global && !covering_index) {
        // 如果只是索引覆盖，则不需要进行后续的操作
        need_put_pk = true;
    }
    if (!need_put_pk) {
        return 0;
    }
    //如果不是覆盖索引，需要把主键的field_id全部加到slot_id
    pb::TupleDescriptor* tuple_desc = get_tuple_desc(scan_node->tuple_id());
    int32_t max_slot_id = tuple_desc->slots_size();
    auto pri_info = schema_factory->get_index_info_ptr(main_table_id);
    if (pri_info == nullptr) {
        DB_WARNING("pri index info not found main_table_id:%ld", main_table_id);
        return -1;
    }
    for (auto& f : pri_info->fields) {
        auto slot_id = get_slot_id(scan_node->tuple_id(), f.id);
        if (slot_id <=0) {
            auto slot = tuple_desc->add_slots();
            slot->set_slot_id(++max_slot_id);
            slot->set_tuple_id(scan_node->tuple_id());
            slot->set_table_id(scan_node->table_id());
            slot->set_field_id(f.id);
            slot->set_slot_type(f.type);
        }
    }
    return 0;
}

int PlanRouter::truncate_node_analyze(TruncateNode* trunc_node, QueryContext* ctx) {
    int ret = 0;
    int64_t table_id = trunc_node->table_id();

    SchemaFactory* schema_factory = SchemaFactory::get_instance();
    auto index_ptr = schema_factory->get_index_info_ptr(table_id);
    if (index_ptr == nullptr) {
        DB_WARNING("invalid index info: %ld", table_id);
        return ret;
    }

    ret = schema_factory->get_region_by_key(*index_ptr, nullptr, trunc_node->region_infos());
    if (ret < 0) {
        DB_WARNING("get_region_by_key:fail :%d", ret);
        return ret;
    }
    //全局二级索引也需要truncate
    TableInfo table_info = schema_factory->get_table_info(table_id);
    for (auto index_id : table_info.indices) {
        if (!schema_factory->is_global_index(index_id)) {
            continue;
        }
        auto index_ptr = schema_factory->get_index_info_ptr(index_id);
        if (index_ptr == nullptr) {
            DB_WARNING("get index ptr fail, index_id: %ld", index_id);
            return -1;
        }
        std::map<int64_t, pb::RegionInfo> index_region_infos;
        ret = schema_factory->get_region_by_key(table_id, *index_ptr, nullptr, index_region_infos);
        if (ret < 0) {
            DB_WARNING("get_region_by_key:fail :%d", ret);
            return ret;
        }
        for (auto& region_info : index_region_infos) {
            trunc_node->region_infos()[region_info.first] = region_info.second;
        }
    }
    if (trunc_node->region_infos().size() == 0) {
        DB_WARNING("region_infos.size = 0");
        return -1;
    }
    return 0;
}

int PlanRouter::kill_node_analyze(KillNode* kill_node, QueryContext* ctx) {
    // 获取上一个query的region_info
    if (ctx->kill_ctx == nullptr) {
        DB_FATAL("ctx->kill_ctx is null");
        return -1;
    }
    ExecNode* plan = ctx->kill_ctx->root;
    if (plan == nullptr) {
        if (kill_node->region_infos().size() == 0) {
//            DB_WARNING("region_infos.size = 0");
//            ctx->return_empty = true;
            return 0;
        }
        DB_FATAL("ctx->kill_ctx->root is null");
        return -1;
    }
    InsertNode* insert_node = static_cast<InsertNode*>(plan->get_node(pb::INSERT_NODE));
    std::vector<ExecNode*> scan_nodes;
    plan->get_node(pb::SCAN_NODE, scan_nodes);
    if (insert_node != nullptr) {
        kill_node->region_infos() = insert_node->region_infos();
    } else {
        for (auto s : scan_nodes) {
            kill_node->region_infos().insert(s->region_infos().begin(), s->region_infos().end());
        }
    }
    if (kill_node->region_infos().size() == 0) {
        DB_WARNING("region_infos.size = 0");
        ctx->return_empty = true;
        return 0;
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

int PartitionAnalyze::analyze(QueryContext* ctx) {
    if (ctx->is_explain) {
        return 0;
    }
    SchemaFactory* schema_factory = SchemaFactory::get_instance();
    ExecNode* plan = ctx->root;
    if (!plan->need_seperate()) {
        return 0;
    }
    std::vector<ExecNode*> scan_nodes;
    plan->get_node(pb::SCAN_NODE, scan_nodes);
    ExecNode* filter_node = plan->get_node(pb::WHERE_FILTER_NODE);
    //ExecNode* having_node = plan->get_node(pb::HAVING_FILTER_NODE);

    if (scan_nodes.size() != 0) {
        bool has_join = scan_nodes.size() > 1;
        bool has_partition = false;
        for (auto scan_node : scan_nodes) {
            auto node = static_cast<RocksdbScanNode*>(scan_node);
            if (node->get_partition_num() > 1) {
                has_partition = true;
                if (has_join) {
                    DB_WARNING("partition table can't join.");
                    return -1;
                }
            }
        }
        if (has_partition) {
            ctx->is_full_export = false;
            bool get_partition = false;
            auto scan_node = static_cast<RocksdbScanNode*>(scan_nodes[0]);
            int64_t table_id = scan_node->table_id();
            scan_node->get_partition().clear();
            if (filter_node != nullptr) {
                //如果分区字段满足条件，进行分区选择，不然返回-1
                for (const auto& expr : *filter_node->mutable_conjuncts()) {
                    std::unordered_set<int32_t> field_ids;
                    expr->get_all_field_ids(field_ids);
                    if (field_ids.count(scan_node->get_partition_field()) == 1) {
                        if (expr->node_type() == pb::FUNCTION_CALL &&
                            static_cast<ScalarFnCall*>(expr)->fn().fn_op() == parser::FT_EQ &&
                            expr->children_size() == 2 &&
                            expr->children(0)->node_type() == pb::SLOT_REF && 
                            static_cast<SlotRef*>(expr->children(0))->field_id() == scan_node->get_partition_field() &&
                            expr->children(1)->is_literal()) {
                            auto lietral_value = static_cast<Literal*>(expr->children(1))->get_value(nullptr);
                            int64_t partition_index = 0;
                            if (schema_factory->get_partition_index(table_id, lietral_value, partition_index) == 0) {
                                scan_node->get_partition().push_back(partition_index);
                                DB_DEBUG("get partition num %ld", partition_index);
                                get_partition = true;
                            } else {
                                DB_WARNING("get table %ld partition number error.", table_id);
                                return -1;
                            }
                        } else {
                            DB_WARNING("pattern not supported.");
                            return -1;
                        }
                    }
                }
            }
            if (!get_partition) {
                for (int64_t i = 0; i < scan_node->get_partition_num(); ++i) {
                    scan_node->get_partition().push_back(i);
                }
            }
        }
    }
    return 0;
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
