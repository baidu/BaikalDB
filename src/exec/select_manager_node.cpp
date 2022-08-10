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

#include "select_manager_node.h"
#include "filter_node.h"
#include "network_socket.h"
#include "rocksdb_scan_node.h"
#include "limit_node.h"
#include "agg_node.h"

namespace baikaldb {
int SelectManagerNode::open(RuntimeState* state) {
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), OPEN_TRACE, ([state](TraceLocalNode& local_node) {
        local_node.set_scan_rows(state->num_scan_rows());
    }));
    if (_return_empty) {
        return 0;
    }
    auto client_conn = state->client_conn();
    if (client_conn == nullptr) {
        DB_WARNING("connection is nullptr: %lu", state->txn_id);
        return -1;
    }
    client_conn->seq_id++;
    _mem_row_compare = std::make_shared<MemRowCompare>(_slot_order_exprs, _is_asc, _is_null_first);
    _sorter = std::make_shared<Sorter>(_mem_row_compare.get());
    if (_sub_query_node != nullptr) {
        return subquery_open(state);
    }
    std::vector<ExecNode*> scan_nodes;
    get_node(pb::SCAN_NODE, scan_nodes);
    if (scan_nodes.size() != 1) {
        DB_WARNING("select manager has more than one scan node, scan_node_size: %lu, txn_id: %lu, log_id:%lu",
                    scan_nodes.size(), state->txn_id, state->log_id());
        return -1;
    }
    RocksdbScanNode* scan_node = static_cast<RocksdbScanNode*>(scan_nodes[0]);

    return fetcher_store_run(state, scan_node);
}

int SelectManagerNode::subquery_open(RuntimeState* state) {
    int ret = 0;
    for (auto expr : _derived_table_projections) {
        ret = expr->open();
        if (ret < 0) {
            DB_WARNING("Expr::open fail:%d", ret);
            return ret;
        }
    }
    ret = _sub_query_node->open(_sub_query_runtime_state);
    if (ret < 0) {
        return ret;
    }
    pb::TupleDescriptor* tuple_desc = state->get_tuple_desc(_derived_tuple_id);
    if (tuple_desc == nullptr) {
        return -1;
    }
    MemRowDescriptor* mem_row_desc = state->mem_row_desc();
    bool eos = false;
    int32_t affected_rows = 0;
    do {
        RowBatch batch;
        ret = _sub_query_node->get_next(_sub_query_runtime_state, &batch, &eos);
        if (ret < 0) {
            DB_WARNING("children:get_next fail:%d", ret);
            return ret;
        }
        std::shared_ptr<RowBatch> batch_ptr = std::make_shared<RowBatch>();
        for (batch.reset(); !batch.is_traverse_over(); batch.next()) {
            MemRow* row = batch.get_row().get();
            std::unique_ptr<MemRow> dual_row = mem_row_desc->fetch_mem_row();
            for (auto iter : _slot_column_mapping) {
                int32_t outer_slot_id = iter.first;
                int32_t inter_column_id = iter.second;
                auto expr = _derived_table_projections[inter_column_id];
                ExprValue result = expr->get_value(row).cast_to(expr->col_type());
                auto slot = tuple_desc->slots(outer_slot_id - 1);
                dual_row->set_value(slot.tuple_id(), slot.slot_id(), result);
            }
            batch_ptr->move_row(std::move(dual_row));
        }
        if (batch_ptr->size() != 0) {
            affected_rows += batch_ptr->size();
            _sorter->add_batch(batch_ptr);
        }
    } while (!eos);
    _sorter->merge_sort();
    return affected_rows;
}

int SelectManagerNode::get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), GET_NEXT_TRACE, ([this](TraceLocalNode& local_node) {
        local_node.set_affect_rows(_num_rows_returned);
    }));
    if (_return_empty) {
        DB_WARNING_STATE(state, "return_empty");
        state->set_eos();
        *eos = true;
        return 0;
    }

    if (state->is_cancelled()) {
        DB_WARNING_STATE(state, "cancelled");
        *eos = true;
        return 0;
    }
    if (reached_limit()) {
        *eos = true;
        return 0;
    }
    int ret = 0;
    ret = _sorter->get_next(batch, eos);
    if (ret < 0) {
        DB_WARNING("sort get_next fail");
        return ret;
    }
    _num_rows_returned += batch->size();
    if (reached_limit()) {
        *eos = true;
        _num_rows_returned = _limit;
        return 0;
    }
    return 0;
}

int SelectManagerNode::single_fetcher_store_open(FetcherInfo* fetcher, RuntimeState* state, ExecNode* exec_node) {
    RocksdbScanNode* scan_node = static_cast<RocksdbScanNode*>(exec_node);
    ScanIndexInfo* scan_index_info = fetcher->scan_index;
    int64_t router_index_id = scan_index_info->router_index_id;
    int64_t main_table_id = scan_node->table_id();
    auto client_conn = state->client_conn();
    int ret = 0;
    // 如果命中的不是全局二级索引，或者全局二级索引是covering_index, 则直接在主表或者索引表上做scan即可
    if (router_index_id == main_table_id || scan_index_info->covering_index) {
        ret = fetcher->fetcher_store.run_not_set_state(state, fetcher->scan_index->region_infos, _children[0], 
                client_conn->seq_id, client_conn->seq_id, pb::OP_SELECT, fetcher->global_backup_type);
    } else {
        ret = open_global_index(fetcher, state, scan_node, router_index_id, main_table_id);
    }
    fetcher->fetcher_store.update_state_info(state);
    if (ret < 0) {
        DB_WARNING("select manager fetcher manager node open fail, txn_id: %lu, log_id:%lu, "
                "router_index_id: %ld, main_table_id: %ld, covering_index: %d",
                state->txn_id, state->log_id(), router_index_id, main_table_id, scan_index_info->covering_index);
        return ret;
    }

    return 0;
}

void SelectManagerNode::multi_fetcher_store_open(FetcherInfo* self_fetcher, FetcherInfo* other_fetcher,  
        RuntimeState* state, ExecNode* exec_node) {
    if (self_fetcher->dynamic_timeout_ms > 0) {
        int64_t timeout_us = self_fetcher->dynamic_timeout_ms * 1000LL;
        // 暂时以打散sleep时间的方式达到及时唤醒的目的，如果此方式存在唤醒不及时的问题，需要换成条件变量
        TimeCost cost;
        while (true) {
            // 检查另一个fetcher是否已经完成
            if (other_fetcher->status == FetcherInfo::S_SUCC) {
                return;
            } else if (other_fetcher->status == FetcherInfo::S_FAIL) {
                break;
            } else {
                // do nothing
            }
            int64_t time_used = cost.get_time();
            if (time_used > timeout_us) {
                break;
            }
            if (timeout_us - time_used < 5000) {
                bthread_usleep(timeout_us - time_used);
            } else {
                bthread_usleep(5000);
            }
        }
    }

    int ret = single_fetcher_store_open(self_fetcher, state, exec_node);
    if (ret < 0) {
        self_fetcher->status = FetcherInfo::S_FAIL;
    } else {
        self_fetcher->status = FetcherInfo::S_SUCC;
        // 取消另一个请求
        if (other_fetcher->status != FetcherInfo::S_SUCC) {
            other_fetcher->fetcher_store.cancel_rpc();
        }
    }
}

int SelectManagerNode::fetcher_store_run(RuntimeState* state, ExecNode* exec_node) {
    RocksdbScanNode* scan_node = static_cast<RocksdbScanNode*>(exec_node);
    FetcherStore* fetcher_store = nullptr;
    FetcherInfo main_fetcher;
    FetcherInfo backup_fetcher;
    ScanIndexInfo* main_scan_index = scan_node->main_scan_index();
    ScanIndexInfo* backup_scan_index = scan_node->backup_scan_index();
    int64_t dynamic_timeout_ms = FetcherStore::get_dynamic_timeout_ms(exec_node, pb::OP_SELECT, state->sign);
    int ret = 0;

    if (main_scan_index == nullptr) {
        return -1;
    }

    if (backup_scan_index != nullptr && dynamic_timeout_ms > 0 && state->txn_id == 0) {
        // 非事务情况下才进行全局二级索引降级，txn_id != 0 情况下state中会有修改，无法多个请求并发使用state
        // 可以降级
        main_fetcher.scan_index = main_scan_index;
        main_fetcher.global_backup_type = GBT_MAIN;
        backup_fetcher.scan_index = backup_scan_index;
        backup_fetcher.global_backup_type = GBT_LEARNER;
        backup_fetcher.dynamic_timeout_ms = dynamic_timeout_ms;

        Bthread main_bth;
        Bthread backup_bth;

        auto main_func = [this, &main_fetcher, &backup_fetcher, state, scan_node]() {
            multi_fetcher_store_open(&main_fetcher, &backup_fetcher, state, scan_node);
        };
        auto backup_func = [this, &main_fetcher, &backup_fetcher, state, scan_node]() {
            multi_fetcher_store_open(&backup_fetcher, &main_fetcher, state, scan_node);
        };

        main_bth.run(main_func);
        backup_bth.run(backup_func);

        // 等待两个线程都结束
        main_bth.join();
        backup_bth.join();

        if (main_fetcher.status == FetcherInfo::S_SUCC) {
            fetcher_store = &main_fetcher.fetcher_store;
        } else if (backup_fetcher.status == FetcherInfo::S_SUCC) {
            fetcher_store = &backup_fetcher.fetcher_store;
        } else {
            state->error_code = main_fetcher.fetcher_store.error_code;
            state->error_msg.str("");
            state->error_msg << main_fetcher.fetcher_store.error_msg.str();
            DB_WARNING("both router index fail, txn_id: %lu, log_id:%lu, main router index_id: %ld" 
                "backup router index_id: %ld", state->txn_id, state->log_id(), 
                main_fetcher.scan_index->router_index_id, backup_fetcher.scan_index->router_index_id);
            return -1;
        }
    } else {
        // 没有开启动态超时，不满足降级条件，只使用main router
        main_fetcher.scan_index = main_scan_index;
        fetcher_store = &main_fetcher.fetcher_store;
        ret = single_fetcher_store_open(&main_fetcher, state, exec_node);
        if (ret < 0) {
            state->error_code = fetcher_store->error_code;
            state->error_msg.str("");
            state->error_msg << fetcher_store->error_msg.str();
            DB_WARNING("single_fetcher_store_open fail, txn_id: %lu, log_id:%lu, router index_id: %ld",
                state->txn_id, state->log_id(), main_fetcher.scan_index->router_index_id);
            return -1;
        }
    } 

    for (auto& pair : fetcher_store->start_key_sort) {
        auto iter = fetcher_store->region_batch.find(pair.second);
        if (iter != fetcher_store->region_batch.end()) {
            auto& batch = iter->second;
            if (batch != nullptr && batch->size() != 0) {
                _sorter->add_batch(batch);
            }
            fetcher_store->region_batch.erase(iter);
        }
    }
    // 无sort节点时不会排序，按顺序输出
    _sorter->merge_sort();
    return fetcher_store->affected_rows.load();

}

int SelectManagerNode::open_global_index(FetcherInfo* fetcher, RuntimeState* state, ExecNode* exec_node, 
        int64_t global_index_id, int64_t main_table_id) {
    RocksdbScanNode* scan_node = static_cast<RocksdbScanNode*>(exec_node);
    auto client_conn = state->client_conn();
    //二级索引只执行scan_node，因为索引条件已经被下推到scan_node了
    ExecNode* store_exec = scan_node;
    bool need_pushdown = true;
    AggNode* agg_node = static_cast<AggNode*>(get_node(pb::AGG_NODE));
    SortNode* sort_node = static_cast<SortNode*>(get_node(pb::SORT_NODE));
    FilterNode* where_filter_node = static_cast<FilterNode*>(get_node(pb::WHERE_FILTER_NODE));
    FilterNode* table_filter_node = static_cast<FilterNode*>(get_node(pb::TABLE_FILTER_NODE));
    if (need_pushdown && _limit < 0) {
        need_pushdown = false;
    }
    if (need_pushdown && agg_node != nullptr) {
        need_pushdown = false;
    }
    if (need_pushdown && where_filter_node != nullptr &&
            where_filter_node->pruned_conjuncts().size() > 0) {
        need_pushdown = false;
    }
    if (need_pushdown && table_filter_node != nullptr &&
            table_filter_node->pruned_conjuncts().size() > 0) {
        need_pushdown = false;
    }
    if (need_pushdown && sort_node != nullptr) {
        std::unordered_set<int32_t> index_field_ids;
        SmartIndex index_info = _factory->get_index_info_ptr(global_index_id);
        for (auto& field : index_info->fields) {
            index_field_ids.insert(field.id);
        }
        std::unordered_set<int32_t> expr_field_ids;
        for (auto expr : sort_node->slot_order_exprs()) {
            expr->get_all_field_ids(expr_field_ids);
        }
        for (auto field_id : expr_field_ids) {
            if (index_field_ids.count(field_id) == 0) {
                need_pushdown = false;
                break;
            }
        }
    }
    if (need_pushdown) {
        store_exec = _children[0];
    }
    //agg or sort 不在二级索引表上执行
    /*
    if (_children[0]->node_type() != pb::SCAN_NODE
        && _children[0]->node_type() != pb::WHERE_FILTER_NODE) {
        store_exec = _children[0]->children(0);
    }*/
    auto ret = fetcher->fetcher_store.run_not_set_state(state, fetcher->scan_index->region_infos, store_exec, 
            client_conn->seq_id, client_conn->seq_id, pb::OP_SELECT, fetcher->global_backup_type);
    if (ret < 0) {
        DB_WARNING("select manager fetcher mnager node open fail, txn_id: %lu, log_id:%lu", 
                state->txn_id, state->log_id());
        return ret;
    }
    ExecNode* parent = get_parent();
    while (parent != nullptr && parent->node_type() != pb::LIMIT_NODE) {
        if (parent->node_type() != pb::SORT_NODE) {
            parent = nullptr;
            break;
        }
        parent = parent->get_parent();
    }
    LimitNode* limit = static_cast<LimitNode*>(parent);
    if (need_pushdown && limit != nullptr) {
        ret = construct_primary_possible_index_use_limit(fetcher->fetcher_store, fetcher->scan_index, state, scan_node, main_table_id, limit);
    } else {
        ret = construct_primary_possible_index(fetcher->fetcher_store, fetcher->scan_index, state, scan_node, main_table_id);
    }
    if (ret < 0) {
        DB_WARNING("construct primary possible index failed");
        return ret;
    }

    ret = fetcher->fetcher_store.run_not_set_state(state, fetcher->scan_index->region_infos, _children[0], 
            client_conn->seq_id, client_conn->seq_id, pb::OP_SELECT, fetcher->global_backup_type);
    if (ret < 0) {
        DB_WARNING("select manager fetcher mnager node open fail, txn_id: %lu, log_id:%lu", 
                state->txn_id, state->log_id());
        return ret; 
    }
    return ret;
}

int SelectManagerNode::construct_primary_possible_index_use_limit(
                      FetcherStore& fetcher_store,
                      ScanIndexInfo* scan_index_info,
                      RuntimeState* state,
                      ExecNode* exec_node,
                      int64_t main_table_id,
                      LimitNode* limit) {
    RocksdbScanNode* scan_node = static_cast<RocksdbScanNode*>(exec_node);
    int32_t tuple_id = scan_node->tuple_id();
    auto pri_info = _factory->get_index_info_ptr(main_table_id);
    if (pri_info == nullptr) {
        DB_WARNING("pri index info not found table_id:%ld", main_table_id);
        return -1;
    }
    // 不能直接清理所有索引，可能有backup请求使用scan_node
    // scan_node->clear_possible_indexes();
    // pb::ScanNode* pb_scan_node = scan_node->mutable_pb_node()->mutable_derive_node()->mutable_scan_node();
    // auto pos_index = pb_scan_node->add_indexes();
    // pos_index->set_index_id(main_table_id);
    // scan_node->set_router_index_id(main_table_id);

    scan_index_info->router_index = nullptr;
    scan_index_info->raw_index.clear();
    scan_index_info->region_infos.clear();
    scan_index_info->region_primary.clear();
    scan_index_info->router_index_id = main_table_id;
    scan_index_info->index_id = main_table_id;
    pb::PossibleIndex pos_index;
    pos_index.set_index_id(main_table_id);
    SmartRecord record_template = _factory->new_record(main_table_id);
    auto mem_row_compare = std::make_shared<MemRowCompare>(_slot_order_exprs, _is_asc, _is_null_first);
    auto tsorter = std::make_shared<Sorter>(_mem_row_compare.get());
    for (auto& pair : fetcher_store.start_key_sort) {
        auto& batch = fetcher_store.region_batch[pair.second];
        if (batch == nullptr) {
            continue;
        }
        if (batch->size() > 0){ 
            tsorter->add_batch(batch);
        }
    }
    if (tsorter->batch_size() == 0){ 
        return 0;
    }
    tsorter->merge_sort();
    bool eos = false;
    int64_t limit_cnt = limit->get_limit();
    while (!eos) {
        std::shared_ptr<RowBatch> batch = std::make_shared<RowBatch>();
        auto ret = tsorter->get_next(batch.get(), &eos);
        if (ret < 0) {
            DB_WARNING("sort get_next fail");
            return ret;
        }
        for (batch->reset(); !batch->is_traverse_over(); batch->next()) {
            std::unique_ptr<MemRow>& mem_row = batch->get_row();
            SmartRecord record = record_template->clone(false);
            for (auto& pri_field : pri_info->fields) {
                int32_t field_id = pri_field.id;
                int32_t slot_id = state->get_slot_id(tuple_id, field_id);
                if (slot_id == -1) {
                    DB_WARNING("field_id:%d tuple_id:%d, slot_id:%d", field_id, tuple_id, slot_id);
                    return -1;
                }
                record->set_value(record->get_field_by_tag(field_id), mem_row->get_value(tuple_id, slot_id));
            }
            auto range = pos_index.add_ranges();
            MutTableKey  key;
            if (record->encode_key(*pri_info.get(), key, pri_info->fields.size(), false, false) != 0) {
                DB_FATAL("Fail to encode_key left, table:%ld", pri_info->id);
                return -1;
            }
            range->set_left_key(key.data());
            range->set_left_full(key.get_full());
            range->set_right_key(key.data());
            range->set_right_full(key.get_full());
            range->set_left_field_cnt(pri_info->fields.size());
            range->set_right_field_cnt(pri_info->fields.size());
            range->set_left_open(false);
            range->set_right_open(false);
            limit_cnt --;
            if(!limit_cnt) {
                eos = true;
                break;
            }
        }
    }
    //重新做路由选择
    pos_index.SerializeToString(&scan_index_info->raw_index);
    return _factory->get_region_by_key(*pri_info, &pos_index, scan_index_info->region_infos,
                                       &scan_index_info->region_primary);
}

int SelectManagerNode::construct_primary_possible_index(
                      FetcherStore& fetcher_store,
                      ScanIndexInfo* scan_index_info, 
                      RuntimeState* state,
                      ExecNode* exec_node,
                      int64_t main_table_id) {
    RocksdbScanNode* scan_node = static_cast<RocksdbScanNode*>(exec_node);
    int32_t tuple_id = scan_node->tuple_id();
    auto pri_info = _factory->get_index_info_ptr(main_table_id);
    if (pri_info == nullptr) {
        DB_WARNING("pri index info not found table_id:%ld", main_table_id);
        return -1;
    }
    // 不能直接清理所有索引，可能有backup请求使用scan_node
    // scan_node->clear_possible_indexes();
    // pb::ScanNode* pb_scan_node = scan_node->mutable_pb_node()->mutable_derive_node()->mutable_scan_node();
    // auto pos_index = pb_scan_node->add_indexes();
    scan_index_info->router_index = nullptr;
    scan_index_info->raw_index.clear();
    scan_index_info->region_infos.clear();
    scan_index_info->region_primary.clear();
    scan_index_info->router_index_id = main_table_id;
    scan_index_info->index_id = main_table_id;
    pb::PossibleIndex pos_index;
    pos_index.set_index_id(main_table_id);
    SmartRecord record_template = _factory->new_record(main_table_id);
    for (auto& pair : fetcher_store.start_key_sort) {
        auto iter = fetcher_store.region_batch.find(pair.second);
        if (iter == fetcher_store.region_batch.end()) {
            continue;
        }
        auto& batch = iter->second;
        if (batch == nullptr || batch->size() == 0) {
            fetcher_store.region_batch.erase(iter);
            continue;
        }
        for (batch->reset(); !batch->is_traverse_over(); batch->next()) {
            std::unique_ptr<MemRow>& mem_row = batch->get_row();
            SmartRecord record = record_template->clone(false);
            for (auto& pri_field : pri_info->fields) {
                int32_t field_id = pri_field.id;
                int32_t slot_id = state->get_slot_id(tuple_id, field_id);
                if (slot_id == -1) {
                    DB_WARNING("field_id:%d tuple_id:%d, slot_id:%d", field_id, tuple_id, slot_id);
                    return -1;
                }
                record->set_value(record->get_field_by_tag(field_id), mem_row->get_value(tuple_id, slot_id));
            }
            auto range = pos_index.add_ranges();
            MutTableKey  key;
            if (record->encode_key(*pri_info.get(), key, pri_info->fields.size(), false, false) != 0) {
                DB_FATAL("Fail to encode_key left, table:%ld", pri_info->id);
                return -1;
            }
            range->set_left_key(key.data());
            range->set_left_full(key.get_full());
            range->set_right_key(key.data());
            range->set_right_full(key.get_full());
            range->set_left_field_cnt(pri_info->fields.size());
            range->set_right_field_cnt(pri_info->fields.size());
            range->set_left_open(false);
            range->set_right_open(false);
        }
        fetcher_store.region_batch.erase(iter);
    }

    pos_index.SerializeToString(&scan_index_info->raw_index);

    //重新做路由选择
    return _factory->get_region_by_key(*pri_info, &pos_index, scan_index_info->region_infos, 
                &scan_index_info->region_primary);
}
}
