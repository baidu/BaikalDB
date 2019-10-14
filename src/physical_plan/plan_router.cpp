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
#include "network_socket.h"

namespace baikaldb {
int PlanRouter::analyze(QueryContext* ctx) {
    if (ctx->is_explain) {
        return 0;
    }
    ExecNode* plan = ctx->root;
    if (!plan->need_seperate()) {
        return 0;
    }
    //DB_NOTICE("need_seperate:%d", plan->need_seperate());
    std::vector<ExecNode*> scan_nodes;
    plan->get_node(pb::SCAN_NODE, scan_nodes);
    InsertNode* insert_node = static_cast<InsertNode*>(plan->get_node(pb::INSERT_NODE));
    TruncateNode* truncate_node = static_cast<TruncateNode*>(plan->get_node(pb::TRUNCATE_NODE));
    KillNode* kill_node = static_cast<KillNode*>(plan->get_node(pb::KILL_NODE));
    TransactionNode* txn_node = static_cast<TransactionNode*>(plan->get_node(pb::TRANSACTION_NODE));

    if (scan_nodes.size() != 0) {
        bool has_join = scan_nodes.size() > 1;
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
            node->records_by_region(), 
            node->region_infos());
    if (ret < 0) {
        DB_WARNING("get_region_by_key:fail :%d", ret);
        return ret;
    }
    if (node->region_infos().size() == 0) {
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
        int ret = schema_factory->get_region_info(table_id, ctx->debug_region_id, info);
        if (ret == 0) {
            scan_node->add_index_id(table_id);
            (scan_node->region_infos())[ctx->debug_region_id] = info;
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
                scan_node->add_index_id(index_id);
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
    pb::ScanNode* pb_scan_node = scan_node->mutable_pb_node()->mutable_derive_node()->mutable_scan_node();
    int64_t main_table_id = scan_node->table_id();
    SchemaFactory* schema_factory = SchemaFactory::get_instance(); 
    //做一次索引选择，如果命中全局二局索引，需要做全局二级索引的路由选择
    std::vector<int> multi_reverse_index;
    int idx = select_index(pb_scan_node, multi_reverse_index);
    if (multi_reverse_index.size() != 0) {
        idx = multi_reverse_index[0];
    }
    const pb::PossibleIndex& pos_index = pb_scan_node->indexes(idx);    
    int64_t index_id = pos_index.index_id();
    scan_node->add_index_id(index_id);
    //DB_WARNING("main table_id: %ld, index_id: %ld", main_table_id, index_id);
    int64_t router_index_id = main_table_id; 
    //如果选中的索引是全局二级索引, 则路由的index_id 为索引id
    if (multi_reverse_index.size() == 0 && schema_factory->is_global_index(index_id)) {
        router_index_id = index_id;
    }
    scan_node->set_router_index_id(router_index_id);
    //DB_WARNING("router index id %ld", router_index_id);
    pb::PossibleIndex* primary = nullptr;
    for (auto& pos_index : *pb_scan_node->mutable_indexes()) {
        if (pos_index.index_id() == router_index_id) {
            primary = &pos_index;
            //DB_WARNING("possible index: %s", primary->DebugString().c_str());
            break;
        }
    }
    auto index_ptr = schema_factory->get_index_info_ptr(router_index_id);
    if (index_ptr == nullptr) {
        DB_WARNING("invalid index info: %ld", router_index_id);
        return -1;
    }
    auto ret = schema_factory->get_region_by_key(main_table_id, 
            *index_ptr, primary,
            scan_node->region_infos(),
            scan_node->mutable_region_primary());
    if (ret < 0) {
        DB_WARNING("get_region_by_key:fail :%d", ret);
        return ret;
    }
    if (primary != nullptr && scan_node->mutable_region_primary()->size() > 0) {
        primary->mutable_ranges()->Clear();
    }
    //如果该表没有全局二级索引
    if (!schema_factory->has_global_index(main_table_id)) {
        return 0;
    }
    //或者命中的不是全局二级索引，并且不是join,直接结束 
    if (!has_join && 
        (multi_reverse_index.size() != 0 
            || !schema_factory->is_global_index(index_id))) {
        return 0;
    }
    //先判断是否是covering_index
    std::map<int32_t, int32_t> index_slot_field_map;
    auto pri_info = schema_factory->get_index_info_ptr(main_table_id);
    auto index_info = schema_factory->get_index_info_ptr(index_id);
    for (auto& f : pri_info->fields) {
        auto slot_id = get_slot_id(scan_node->tuple_id(), f.id);
        if (slot_id > 0) {
            index_slot_field_map[slot_id] = f.id;
        }
    }
    for (auto& f : index_info->fields) {
        auto slot_id = get_slot_id(scan_node->tuple_id(), f.id);
        if (slot_id > 0) {
            index_slot_field_map[slot_id] = f.id;
        }
    }
    int32_t max_slot_id = 0;
    for (auto& slot : get_tuple_desc(scan_node->tuple_id())->slots()) {
        if (slot.slot_id() > max_slot_id) {
            max_slot_id = slot.slot_id();
        }
        if (index_slot_field_map.count(slot.slot_id()) == 0) {
            scan_node->set_covering_index(false);
        }
    }
    // 如果是索引覆盖，则不需要进行后续的操作
    // 如有涉及有全局二级索引的join表时，把主表的fields_id全部放到tuple里。
    // 该步骤后续如有性能问题的话，可以优化为在join_node里做plan router的是按需放进去。
    // 但按需放进去时， tuple已经在state->init时生成，要destory 掉，然后重新生成。这块处理一定要小心
    if (scan_node->covering_index() && !has_join) { 
        return 0;
    }
    //如果不是覆盖索引，需要把主键的field_id全部加到slot_id
    pb::TupleDescriptor* tuple_desc = get_tuple_desc(scan_node->tuple_id());
    for (auto& f : pri_info->fields) {
        auto slot_id = get_slot_id(scan_node->tuple_id(), f.id);
        if (slot_id <=0) {
            auto slot_ref = tuple_desc->add_slots();
            slot_ref->set_slot_id(++max_slot_id);
            slot_ref->set_tuple_id(scan_node->tuple_id());
            slot_ref->set_table_id(scan_node->table_id());
            slot_ref->set_field_id(f.id);
            slot_ref->set_slot_type(f.type);
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
int PlanRouter::select_index(pb::ScanNode* scan_node, std::vector<int>& multi_reverse_index) {
    int sort_index = -1;

    std::multimap<uint32_t, int> prefix_ratio_id_mapping;
    std::set<int32_t> primary_fields;
    for (int i = 0; i < scan_node->indexes_size(); i++) {
        auto& pos_index = scan_node->indexes(i);
        int64_t index_id = pos_index.index_id();
        auto info_ptr = SchemaFactory::get_instance()->get_index_info_ptr(index_id);
        if (info_ptr == nullptr) {
            continue;
        }
        IndexInfo& info = *info_ptr;
        int field_count = 0;
        for (auto& range : pos_index.ranges()) {
            if (range.has_left_field_cnt() && range.left_field_cnt() > 0) {
                field_count = std::max(field_count, range.left_field_cnt());
            }
            if (range.has_right_field_cnt() && range.right_field_cnt() > 0) {
                field_count = std::max(field_count, range.right_field_cnt());
            }
        }
        float prefix_ratio = (field_count + 0.0) / info.fields.size();
        uint16_t prefix_ratio_round = prefix_ratio * 10;
        uint16_t index_priority = 0;
        if (info.type == pb::I_PRIMARY) {
            for (int j = 0; j < field_count; j++) {
                primary_fields.insert(info.fields[j].id);
            }
            index_priority = 300;
        } else if (info.type == pb::I_UNIQ) {
            index_priority = 200;
        } else if (info.type == pb::I_KEY) {
            index_priority = 100 + field_count;
        } else {
            index_priority = 0;
        }
        // 普通索引如果都包含在主键里，则不选
        if (info.type == pb::I_UNIQ || info.type == pb::I_KEY) {
            bool contain_by_primary = true;
            for (int j = 0; j < field_count; j++) {
                if (primary_fields.count(info.fields[j].id) == 0) {
                    contain_by_primary = false;
                    break;
                }
            }
            if (contain_by_primary) {
                continue;
            }
        }
        uint32_t prefix_ratio_index_score = (prefix_ratio_round << 16) | index_priority;
        //DB_WARNING("scan node insert prefix_ratio_index_score:%u, i: %d", prefix_ratio_index_score, i);
        prefix_ratio_id_mapping.insert(std::make_pair(prefix_ratio_index_score, i));

        // 优先选倒排，没有就取第一个
        switch (info.type) {
            case pb::I_FULLTEXT:
                multi_reverse_index.push_back(i);
                break;
            case pb::I_RECOMMEND:
                return i;
            default:
                break;
        }
        if (pos_index.has_sort_index() && field_count > 0) {
            sort_index = i;
        }
    }
    if (sort_index != -1) {
        return sort_index;
    }
    // ratio * 10(=0...9)相同的possible index中，按照PRIMARY, UNIQUE, KEY的优先级选择
    //DB_WARNING("prefix_ratio_id_mapping.size: %d", prefix_ratio_id_mapping.size());
    for (auto iter = prefix_ratio_id_mapping.crbegin(); iter != prefix_ratio_id_mapping.crend(); ++iter) {
        //DB_WARNING("prefix_ratio_index_score:%u, i: %d", iter->first, iter->second);
        return iter->second;
    }
    return 0;
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
