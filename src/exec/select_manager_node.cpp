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
#include "query_context.h"
#include <arrow/acero/options.h>
#include "arrow_io_excutor.h"
#include "exchange_sender_node.h"
#include "join_node.h"
#include "vectorize_helpper.h"
#include "arrow_io_excutor.h"
#include "arrow_exec_node_manager.h"

namespace baikaldb {
DEFINE_bool(global_index_read_consistent, true, "double check for global and primary region consistency");
DEFINE_int32(max_select_region_count, -1, "max select sql region count limit, default:-1 means no limit");
DECLARE_int32(chunk_size);
int SelectManagerNode::open(RuntimeState* state) {
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), OPEN_TRACE, ([state](TraceLocalNode& local_node) {
        local_node.set_scan_rows(state->num_scan_rows());
    }));
    set_node_exec_type(pb::EXEC_ROW);
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

    if (_index_collector_cond != nullptr) {
        _delay_fetcher_store = true;
        set_node_exec_type(pb::EXEC_ARROW_ACERO);
    } else if (state->execute_type == pb::EXEC_ARROW_ACERO) {
        ExecNode* parent = this;
        while (parent != nullptr) {
            if (parent->node_type() == pb::JOIN_NODE 
                || parent->node_type() == pb::UNION_NODE) {
                break;
            }
            if (parent->is_delay_fetcher_store()) {
                _delay_fetcher_store = true;
                set_node_exec_type(pb::EXEC_ARROW_ACERO);
                break;
            }
            parent = parent->get_parent();
        }
    }

    std::vector<ExecNode*> dual_scan_nodes;
    get_node(pb::DUAL_SCAN_NODE, dual_scan_nodes);
    std::vector<ExecNode*> scan_nodes;
    get_node(pb::SCAN_NODE, scan_nodes);
    ExecNode* exchange_receiver_node = get_node_pass_subquery(pb::EXCHANGE_RECEIVER_NODE);
    if (exchange_receiver_node != nullptr) {
        _has_er_child = true;
    }
    if ((dual_scan_nodes.size() + scan_nodes.size() != 1) && exchange_receiver_node == nullptr) {
        // mpp fragment 可能没有scannode和dualscannode
        DB_WARNING("Invalid SelectManagerNode, dual_scan_nodes_size: %lu, scan_nodes_size: %lu, txn_id: %lu, log_id:%lu",
                    dual_scan_nodes.size(), scan_nodes.size(), state->txn_id, state->log_id());
        return -1;
    }
    if (dual_scan_nodes.size() == 1) {
        dual_scan_nodes[0]->set_delay_fetcher_store(_delay_fetcher_store);
        return subquery_open(state);
    }
    if (scan_nodes.size() == 0) {
        // mpp
        return subquery_open(state);
    }
    if (is_mysql_scan()) {
        // open子节点
        return subquery_open(state);
    }
    _scan_tuple_id = static_cast<ScanNode*>(scan_nodes[0])->tuple_id();
    if (_delay_fetcher_store) {
        return 0;
    }
    int ret = fetcher_store_run(state, scan_nodes[0]);
    if (ret < 0) {
        return -1;
    }
    if (_arrow_schema != nullptr) {
        state->arrow_input_schemas[_scan_tuple_id] = _arrow_schema;
    }
    return 0;
}

int SelectManagerNode::init(const pb::PlanNode& node){
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    const pb::SelectManagerNode& select_node = node.derive_node().select_manager_node();
    // 解析schema
    const auto& pb_schema = select_node.schema();
    if (pb_schema.size() > 0) {
        std::shared_ptr<arrow::Buffer> schema_buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(pb_schema.data()),
                                                                                   static_cast<int64_t>(pb_schema.size()));
        arrow::io::BufferReader schema_reader(schema_buffer);
        auto schema_ret = arrow::ipc::ReadSchema(&schema_reader, nullptr);
        if (schema_ret.ok()) {
            _arrow_schema = *schema_ret;
        } else {
            DB_WARNING("parser from schema error [%s]. ", schema_ret.status().ToString().c_str());
            return -1;
        }
    }
    for (auto& expr : select_node.slot_order_exprs()) {
        ExprNode* slot_order_expr = nullptr;
        ret = ExprNode::create_tree(expr, &slot_order_expr);
        if (ret < 0) {
            DB_FATAL("create slot order expr fail");
            ExprNode::destroy_tree(slot_order_expr);
            return ret;
        }
        _slot_order_exprs.push_back(slot_order_expr);
    }
    for (bool is_asc : select_node.is_asc()) {
        _is_asc.push_back(is_asc);
    }
    for (bool is_null_first : select_node.is_null_first()) {
        _is_null_first.push_back(is_null_first);
    }
    if (select_node.has_is_return_empty()) {
        _return_empty = select_node.is_return_empty();
    }
    return 0;
} 

void SelectManagerNode::transfer_pb(int64_t region_id, pb::PlanNode* pb_node) {
    ExecNode::transfer_pb(region_id, pb_node);
    auto node = pb_node->mutable_derive_node()->mutable_select_manager_node();
    if (!is_dual_scan() && !_has_er_child) {
        // 将node的schema转换为arrow格式
        _arrow_schema = VectorizeHelpper::get_arrow_schema(_data_schema);
        if (_arrow_schema == nullptr) {
            DB_FATAL("transfer receiver arrow schema fail");
            return;
        }
        // 序列化schema
        arrow::Result<std::shared_ptr<arrow::Buffer>> schema_ret = arrow::ipc::SerializeSchema(*_arrow_schema, arrow::default_memory_pool());
        if (!schema_ret.ok()) {
            DB_FATAL("arrow serialize schema fail, status: %s", schema_ret.status().ToString().c_str());
            return;
        }
        node->set_schema((*schema_ret)->data(), (*schema_ret)->size());
    }
    for (ExprNode* expr : _slot_order_exprs) {
        ExprNode::create_pb_expr(node->add_slot_order_exprs(), expr);
    }
    for (bool asc : _is_asc) {
        node->add_is_asc(asc);
    }
    for (bool null_first : _is_null_first) {
        node->add_is_null_first(null_first);
    }
    node->set_is_return_empty(_return_empty);
    return;
}

int SelectManagerNode::delay_fetcher_store(RuntimeState* state) {
    RocksdbScanNode* scan_node = static_cast<RocksdbScanNode*>(get_node(pb::SCAN_NODE));
    int ret = fetcher_store_run(state, scan_node);
    if (ret < 0) {
        return -1;
    }
    return 0;
}

int SelectManagerNode::build_arrow_declaration(RuntimeState* state) {
    if (is_dual_scan() || _has_er_child || is_mysql_scan()) {
        return _children[0]->build_arrow_declaration(state);
    }
    RocksdbScanNode* scan_node = static_cast<RocksdbScanNode*>(get_node(pb::SCAN_NODE));
    if (_delay_fetcher_store && _index_collector_cond == nullptr) {
        // 启动acero后会直接查表
        scan_node->set_no_need_runtime_filter(true);
    }
    START_LOCAL_TRACE_WITH_PARTITION_PROPERTY(get_trace(), state->get_trace_cost(), &_partition_property, OPEN_TRACE, nullptr);
    int ret = 0;
    // add SourceNode
    std::shared_ptr<FetcherStoreVectorizedReader> vectorized_reader = std::make_shared<FetcherStoreVectorizedReader>();
    ret = vectorized_reader->init(this, state);
    if (ret != 0) {
        return -1;
    } 
    std::function<arrow::Iterator<std::shared_ptr<arrow::RecordBatch>>()> iter_maker = [vectorized_reader] () {
        arrow::Iterator<std::shared_ptr<arrow::RecordBatch>> batch_it = arrow::MakeIteratorFromReader(vectorized_reader);
        return batch_it;
    };
    if (state->vectorlized_parallel_execution == false) {
        arrow::acero::Declaration dec{"record_batch_source",
                arrow::acero::RecordBatchSourceNodeOptions{state->arrow_input_schemas[_scan_tuple_id], std::move(iter_maker)}}; 
        LOCAL_TRACE_ARROW_PLAN_WITH_SCHEMA(dec, state->arrow_input_schemas[_scan_tuple_id], &_delay_fetcher_store);
        state->append_acero_declaration(dec);
    } else {
        auto executor = BthreadArrowExecutor::Make(1);
        _arrow_io_executor = *executor;
        arrow::acero::Declaration dec{"record_batch_source",
                arrow::acero::RecordBatchSourceNodeOptions{state->arrow_input_schemas[_scan_tuple_id], std::move(iter_maker), _arrow_io_executor.get()}}; 
        LOCAL_TRACE_ARROW_PLAN_WITH_SCHEMA(dec, state->arrow_input_schemas[_scan_tuple_id], &_delay_fetcher_store);
        state->append_acero_declaration(dec);
    }
    // add SortNode
    if (!_slot_order_exprs.empty()) {
        std::vector<arrow::compute::SortKey> sort_keys;
        sort_keys.reserve(_slot_order_exprs.size());
        for (int i = 0; i < _slot_order_exprs.size(); ++i) {
            int ret = _slot_order_exprs[i]->transfer_to_arrow_expression();
            if (ret != 0 || _slot_order_exprs[i]->arrow_expr().field_ref() == nullptr) {
                DB_FATAL_STATE(state, "get sort field ref fail, maybe is not slot ref");
                return -1;
            }
            auto field_ref = _slot_order_exprs[i]->arrow_expr().field_ref();
            sort_keys.emplace_back(*field_ref, _is_asc[i] ? arrow::compute::SortOrder::Ascending : arrow::compute::SortOrder::Descending);
        }
        arrow::compute::Ordering ordering{sort_keys, 
                                      _is_asc[0] ? arrow::compute::NullPlacement::AtStart : arrow::compute::NullPlacement::AtEnd};
        if (_limit < 0) {
            arrow::acero::Declaration dec{"order_by", arrow::acero::OrderByNodeOptions{ordering}};
            LOCAL_TRACE_ARROW_PLAN(dec);
            state->append_acero_declaration(dec);
        } else {
            arrow::acero::Declaration dec{"topk", TopKNodeOptions{ordering, _limit}};
            LOCAL_TRACE_ARROW_PLAN(dec);
            state->append_acero_declaration(dec);
        }
    }
    return 0;
}

int SelectManagerNode::subquery_open(RuntimeState* state) {
    if (_children.empty()) {
        DB_WARNING("has no child");
        return -1;
    }
    int ret = 0;
    ret = ExecNode::open(state);
    if (ret < 0) {
        DB_WARNING_STATE(state, "ExecNode::open fail, ret:%d", ret);
        return ret;
    }
    bool eos = false;
    int32_t affected_rows = 0;
    do {
        std::shared_ptr<RowBatch> batch = std::make_shared<RowBatch>();
        ret = _children[0]->get_next(state, batch.get(), &eos);
        if (ret < 0) {
            DB_WARNING("children::get_next fail:%d", ret);
            return ret;
        }
        set_node_exec_type(_children[0]->node_exec_type());
        if (_node_exec_type == pb::EXEC_ARROW_ACERO) {
            break;
        }
        if (batch != nullptr && batch->size() != 0) {
            affected_rows += batch->size();
            _sorter->add_batch(batch);
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
        DB_WARNING_STATE(state, "return_empty, _scan_tuple_id: %d", _scan_tuple_id);
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
    } else if (*eos == true) {
        if (state->must_have_one && _num_rows_returned == 0) {
            // 生成null返回 
            std::unique_ptr<MemRow> row = state->mem_row_desc()->fetch_mem_row();
            batch->move_row(std::move(row));
            _num_rows_returned = 1;
            return 0;
        }
    }
    return 0;
}

int SelectManagerNode::mpp_fetcher_global_index(RuntimeState* state) {
    RocksdbScanNode* scan_node = static_cast<RocksdbScanNode*>(get_node(pb::SCAN_NODE));
    if (scan_node == nullptr) {
        return 0;
    }
    
    ScanIndexInfo* scan_index_info = scan_node->main_scan_index();
    int64_t router_index_id = scan_index_info->router_index_id;
    int64_t main_table_id = scan_node->table_id();
    if (router_index_id == main_table_id || scan_index_info->covering_index) {
        return 0;
    }
    // 先查global索引
    _mem_row_compare = std::make_shared<MemRowCompare>(_slot_order_exprs, _is_asc, _is_null_first);
    _sorter = std::make_shared<Sorter>(_mem_row_compare.get());
    FetcherInfo main_fetcher;
    main_fetcher.scan_index = scan_index_info;
    return single_fetcher_store_open(&main_fetcher, state, scan_node);
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
    if (scan_node->has_merge_index()) {
        return merge_fetcher_store_run(state, exec_node);
    }
    FetcherStore* fetcher_store = nullptr;
    FetcherInfo main_fetcher;
    FetcherInfo backup_fetcher;
    ScanIndexInfo* main_scan_index = scan_node->main_scan_index();
    ScanIndexInfo* backup_scan_index = scan_node->backup_scan_index();
    int64_t dynamic_timeout_ms = FetcherStore::get_dynamic_timeout_ms(exec_node, pb::OP_SELECT, state->sign);
    int ret = 0;

    if (main_scan_index == nullptr) {
        DB_FATAL("main_scan_index is null, txn_id: %lu, log_id:%lu, scan tuple_id: %d", 
            state->txn_id, state->log_id(), scan_node->tuple_id());
        return -1;
    }
    auto ctx = state->client_conn()->query_ctx;
    if (ctx->is_select && !scan_node->has_index() && FLAGS_max_select_region_count > 0
            && main_scan_index->region_infos.size() > FLAGS_max_select_region_count
            && scan_node->get_limit() == -1) {
        ctx->stat_info.error_code = ER_SQL_REFUSE;
        ctx->stat_info.error_msg << "sql is forbid, reason is not use index and region count="
                << main_scan_index->region_infos.size();
        return -1;
    };
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
    if (state->execute_type == pb::EXEC_ARROW_ACERO && fetcher_store->arrow_schema != nullptr) {
        set_node_exec_type(pb::EXEC_ARROW_ACERO);
    } 

    if (_node_exec_type == pb::EXEC_ROW) {
        // EXEC_ROW(只会返回memrow), EXEC_ARROW_ACERO(单表查询所有region都返回memrow)
        for (auto& pair : fetcher_store->start_key_sort) {
            auto iter = fetcher_store->region_batch.find(pair.second);
            if (iter != fetcher_store->region_batch.end()) {
                auto& batch = iter->second.row_data;
                if (batch != nullptr && batch->size() != 0) {
                    _sorter->add_batch(batch);
                    if (state->execute_type == pb::EXEC_ARROW_ACERO) {
                        _region_batches.emplace_back(iter->second);
                    }
                }
                fetcher_store->region_batch.erase(iter);
            }
        }
        // 无sort节点时不会排序，按顺序输出
        _sorter->merge_sort();
    } else {
        // EXEC_ARROW_ACERO(多表查询, 或者单表查询有region返回了arrow格式)
        for (auto& pair : fetcher_store->start_key_sort) {
            auto iter = fetcher_store->region_batch.find(pair.second);
            if (iter != fetcher_store->region_batch.end()) {
                auto& batch = iter->second;
                if (batch.arrow_data != nullptr && batch.arrow_data->num_rows() > 0) {
                    _region_batches.emplace_back(batch);
                    // arrow列式执行,将返回的store_response交给select_manager_node保管其生命周期
                    _arrow_responses.emplace_back(fetcher_store->region_vectorized_response[pair.second]);
                    _arrow_batch_response.emplace_back(fetcher_store->batch_region_vectorized_response[pair.second]);
                } else if (batch.row_data != nullptr && batch.row_data->size() > 0) {
                    _region_batches.emplace_back(batch);
                }
                fetcher_store->region_batch.erase(iter);
            }
        }
        _arrow_schema = fetcher_store->arrow_schema;
    }
    return fetcher_store->affected_rows.load();
}

int SelectManagerNode::open_global_index(FetcherInfo* fetcher, RuntimeState* state, ExecNode* exec_node, 
        int64_t global_index_id, int64_t main_table_id) {
    RocksdbScanNode* scan_node = static_cast<RocksdbScanNode*>(exec_node);
    auto client_conn = state->client_conn();
    if (client_conn == nullptr) {
        DB_FATAL_STATE(state, "client_conn is null");
        return -1;
    }
    //二级索引只执行scan_node，因为索引条件已经被下推到scan_node了
    ExecNode* store_exec = scan_node;
    bool need_pushdown = true;
    bool is_mpp = false;
    AggNode* agg_node = static_cast<AggNode*>(get_node(pb::AGG_NODE));
    SortNode* sort_node = static_cast<SortNode*>(get_node(pb::SORT_NODE));
    ExchangeSenderNode* exchange_sender_node = static_cast<ExchangeSenderNode*>(get_node(pb::EXCHANGE_SENDER_NODE));
    FilterNode* where_filter_node = static_cast<FilterNode*>(get_node(pb::WHERE_FILTER_NODE));
    FilterNode* table_filter_node = static_cast<FilterNode*>(get_node(pb::TABLE_FILTER_NODE));
    if (exchange_sender_node != nullptr) {
        is_mpp = true;
    }
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
        if (store_exec->node_type() == pb::EXCHANGE_SENDER_NODE) {
            store_exec = store_exec->children(0);
        }
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
    if (client_conn->query_ctx->is_select && FLAGS_global_index_read_consistent) {
        scan_node->add_global_condition_again();
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
        ret = construct_primary_possible_index(fetcher->fetcher_store, fetcher->scan_index, state, scan_node, main_table_id, limit);
    } else {
        ret = construct_primary_possible_index(fetcher->fetcher_store, fetcher->scan_index, state, scan_node, main_table_id);
    }
    if (ret < 0) {
        DB_WARNING("construct primary possible index failed");
        return ret;
    }
    if (is_mpp) {
        // mpp模式下：
        // step1：主db访问全局索引，只走单机向量化（忽略exchange）。
        // step2：访问主表，走mpp exchange
        set_region_infos(fetcher->scan_index->region_infos);
        return 0;
    }
    // 统计全局索引扫描量等
    fetcher->fetcher_store.update_state_info(state);
    // 查主表
    ret = fetcher->fetcher_store.run_not_set_state(state, fetcher->scan_index->region_infos, _children[0], 
            client_conn->seq_id, client_conn->seq_id, pb::OP_SELECT, fetcher->global_backup_type);
    if (ret < 0) {
        DB_WARNING("select manager fetcher mnager node open fail, txn_id: %lu, log_id:%lu", 
                state->txn_id, state->log_id());
        return ret; 
    }
    return ret;
}

int SelectManagerNode::construct_primary_possible_index(
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
    auto table_info = _factory->get_table_info_ptr(main_table_id);
    if (table_info == nullptr) {
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

    if (fetcher_store.arrow_schema != nullptr) {
        return construct_primary_possible_index_vectorize(fetcher_store, 
                scan_index_info, state, exec_node, main_table_id, pri_info, limit);
    }

    pb::PossibleIndex pos_index;
    pos_index.set_index_id(main_table_id);
    SmartRecord record_template = _factory->new_record(main_table_id);
    auto tsorter = std::make_shared<Sorter>(_mem_row_compare.get());
    for (auto& pair : fetcher_store.start_key_sort) {
        auto& batch = fetcher_store.region_batch[pair.second].row_data;
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
    int64_t limit_cnt = 0x7fffffff;
    if (limit != nullptr) {
        limit_cnt = limit->get_limit();
    }
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
            if (limit != nullptr) {
                if (limit->get_num_rows_skipped() < limit->get_offset()) {
                    limit->add_num_rows_skipped(1);
                    continue;
                }
            }
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
            pos_index.set_is_eq(true);
            range->set_left_key(key.data());
            range->set_left_full(key.get_full());
            pos_index.set_left_field_cnt(pri_info->fields.size());
            pos_index.set_left_open(false);
            
            range->add_partition_ids(mem_row->get_partition_id());
            limit_cnt --;
            if(!limit_cnt) {
                eos = true;
                break;
            }
        }
    }
    //重新做路由选择
    pos_index.SerializeToString(&scan_index_info->raw_index);
    return _factory->get_region_by_key(main_table_id, *pri_info, &pos_index, scan_index_info->region_infos,
                &scan_index_info->region_primary, scan_node->get_partition());
}

int SelectManagerNode::construct_primary_possible_index_vectorize(
                      FetcherStore& fetcher_store,
                      ScanIndexInfo* scan_index_info,
                      RuntimeState* state,
                      ExecNode* exec_node,
                      int64_t main_table_id,
                      SmartIndex pri_info,
                      LimitNode* limit) {
    RocksdbScanNode* scan_node = static_cast<RocksdbScanNode*>(exec_node);
    int32_t tuple_id = scan_node->tuple_id();
    pb::PossibleIndex pos_index;
    pos_index.set_index_id(main_table_id);
    SmartRecord record_template = _factory->new_record(main_table_id);
    std::vector<std::shared_ptr<arrow::RecordBatch>> batchs;
    std::shared_ptr<Chunk> chunk;
    auto tuple = state->get_tuple_desc(tuple_id);
    for (auto& pair : fetcher_store.start_key_sort) {
        auto row_batch = fetcher_store.region_batch[pair.second].row_data;
        if (row_batch != nullptr) {
            if (chunk == nullptr) {
                chunk = std::make_shared<Chunk>();
                if (0 != chunk->init({tuple})) {
                    DB_FATAL("chunk init fail");
                    return -1;
                }
            }
            // chunk复用，内部会reset
            if (0 != row_batch->transfer_rowbatch_to_arrow({tuple}, chunk, nullptr, &fetcher_store.region_batch[pair.second].arrow_data)) {
                DB_FATAL("add row batch to chunk fail");
                return -1;
            }
        }
        if (fetcher_store.region_batch[pair.second].arrow_data == nullptr) {
            continue;
        }
        if (!fetcher_store.arrow_schema->Equals(fetcher_store.region_batch[pair.second].arrow_data->schema())
                && 0 != VectorizeHelpper::change_arrow_record_batch_schema(fetcher_store.arrow_schema, 
                    fetcher_store.region_batch[pair.second].arrow_data, 
                    &fetcher_store.region_batch[pair.second].arrow_data, 
                    true)) {
            DB_FATAL("change arrow record batch schema chunk fail");
            return -1;
        }
        batchs.push_back(fetcher_store.region_batch[pair.second].arrow_data);
    }
    if (batchs.size() == 0){ 
        return 0;
    }
    arrow::Result<std::shared_ptr<arrow::Table>> build_table = arrow::Table::FromRecordBatches(batchs);
    if (!build_table.ok()) {
        DB_FATAL_STATE(state, "build arrow table fail: %s", build_table.status().ToString().c_str());
        return -1;
    }
    std::shared_ptr<arrow::Table> table = *build_table;
    if (!_mem_row_compare->need_not_compare()) {
        arrow::compute::SortOptions option;
        if (0 != _mem_row_compare->build_arrow_sort_option(option)) {
            DB_FATAL("build arrow sort option fail");
            return -1;
        }
        arrow::Datum datum(table);
        arrow::Result<std::shared_ptr<arrow::Array>> indices = arrow::compute::SortIndices(datum, option, /*ExecContext* ctx = */ nullptr);
        if (!indices.ok()) {
            DB_FATAL("sort indices fail: %s", indices.status().ToString().c_str());
            return -1;
        }
        arrow::Result<arrow::Datum> sorted = arrow::compute::Take(table, *indices, arrow::compute::TakeOptions::NoBoundsCheck(), /*ExecContext* ctx = */ nullptr);
        if (!sorted.ok()) {
            DB_FATAL("take fail: %s", sorted.status().ToString().c_str());
            return -1;
        }
        table = sorted->table();
    }

    if (limit != nullptr) {
        int64_t offset_cnt = limit->get_offset();
        int64_t limit_cnt = limit->get_limit();
        if (offset_cnt >= table->num_rows()) {
            table = table->Slice(0, 0);
        } else {
            table = table->Slice(offset_cnt, limit_cnt);
        }
        limit->add_num_rows_skipped(offset_cnt);
    }
    std::unordered_map<int64_t, std::shared_ptr<arrow::ChunkedArray>> field_id_to_arrow_array;
    for (auto& pri_field : pri_info->fields) {
        int32_t field_id = pri_field.id;
        int32_t slot_id = state->get_slot_id(tuple_id, field_id);
        if (slot_id == -1) {
            DB_WARNING("field_id:%d tuple_id:%d, slot_id:%d", field_id, tuple_id, slot_id);
            return -1;
        }
        auto arrow_array = table->GetColumnByName(std::to_string(tuple_id) + "_" + std::to_string(slot_id));
        if (arrow_array == nullptr) {
            DB_FATAL("not find arrow column, field_id:%d tuple_id:%d, slot_id:%d", field_id, tuple_id, slot_id);
            return -1;
        }
        field_id_to_arrow_array[field_id] = arrow_array;
    }
    for (auto row_idx = 0; row_idx < table->num_rows(); ++row_idx) {
        SmartRecord record = record_template->clone(false);
        for (auto& field : field_id_to_arrow_array) {
            record->set_value(record->get_field_by_tag(field.first), VectorizeHelpper::get_vectorized_value(field.second.get(), row_idx));
        }
        auto range = pos_index.add_ranges();
        MutTableKey  key;
        if (record->encode_key(*pri_info.get(), key, pri_info->fields.size(), false, false) != 0) {
            DB_FATAL("Fail to encode_key left, table:%ld", pri_info->id);
            return -1;
        }
        pos_index.set_is_eq(true);
        range->set_left_key(key.data());
        range->set_left_full(key.get_full());
        pos_index.set_left_field_cnt(pri_info->fields.size());
        pos_index.set_left_open(false);
        // [ARROW todo] 全局索引反查partition分发
        // range->set_partition_id(mem_row->get_partition_id());
    }
    //重新做路由选择
    pos_index.SerializeToString(&scan_index_info->raw_index);
    return _factory->get_region_by_key(main_table_id, *pri_info, &pos_index, scan_index_info->region_infos,
                &scan_index_info->region_primary, scan_node->get_partition());
}



// arrow vectorize
int FetcherStoreVectorizedReader::init(SelectManagerNode* select_node, RuntimeState* state) {
    _schema = select_node->get_arrow_schema();
    _need_fetcher_store = select_node->is_delay_fetcher_store();
    int32_t scan_tuple_id = select_node->get_scan_tuple_id();
    pb::TupleDescriptor* tuple = state->get_tuple_desc(scan_tuple_id);
    if (state->is_simple_select) {
        // 没join的简单查询, or子查询
        for (auto& tuple : state->tuple_descs()) {
            _tuples.emplace_back(&tuple);
        }
    } else {
        _tuples.emplace_back(tuple);
    }
    if (_schema == nullptr) {
        // 当store返回的数据都是row pb, 但是需要列式执行, 如index join场景; 或是非index join延迟访问store
        // 1. 初始化根据tuple_id(用scannode的tuple_id)生成对应tuple对应的arrow schema 
        // 2. 列式执行过程中将返回的memrow转arrow
        // 3. 列式执行返回的record batch, 转换为db构建的schema
        _chunk = std::make_shared<Chunk>();
        if (_chunk->init(_tuples) != 0) {
            DB_FATAL("build arrow schema fail, scan_tuple_id: %d", scan_tuple_id);
            return -1;
        }
        _schema = _chunk->get_arrow_schema();
        if (_schema == nullptr) {
            DB_FATAL("build arrow schema fail");
            return -1;
        }
    } 
    state->arrow_input_schemas[scan_tuple_id] = _schema;
    _select_node = select_node;
    _state = state;
    _index_cond = select_node->get_index_collector_cond();
    return 0;
}
    
arrow::Status FetcherStoreVectorizedReader::ReadNext(std::shared_ptr<arrow::RecordBatch>* out) {
    int ret = 0;
    if (_need_fetcher_store) {
        if (_index_cond != nullptr) {
            _index_cond->cond.wait();
            if (_index_cond->index_cnt == 0) {
                // join驱动表没数据
                out->reset();
                return arrow::Status::OK();
            }
        }
        ret = _select_node->delay_fetcher_store(_state);
        if (ret != 0) {
            return arrow::Status::IOError("delay fetcher store fail");
        }
        _need_fetcher_store = false;
    }
    std::vector<RegionReturnData>& region_batches = _select_node->get_region_batches();
    if (_eos || _record_batch_idx >= region_batches.size()) {
        out->reset();
        return arrow::Status::OK();
    }
    
    if (_select_node->is_return_empty()) {
        DB_WARNING_STATE(_state, "return_empty");
        _state->set_eos();
        out->reset();
        _eos = true;
        return arrow::Status::OK();
    }

    if (_state->is_cancelled()) {
        DB_WARNING_STATE(_state, "cancelled");
        out->reset();
        _eos = true;
        return arrow::Status::OK();
    }
    if (!_select_node->need_sorter() && _select_node->reached_limit()) {
        _eos = true;
        out->reset();
        return arrow::Status::OK();
    }
    // 1. 单表查询, reader的arrow schema是store返回的, rowbatch转arrow需要转换所有的tuple(agg)
    // 2. join没子查询, reader的arrow schema可能是自己构造的(根据scannode的tuple id构造的), rowbatch转arrow只要转换该tupleid
    // 3. join子查询, 需要进行内外schema转换 [todo]
    while (_record_batch_idx < region_batches.size()) {
        auto& batch = region_batches[_record_batch_idx];
        if (batch.arrow_data != nullptr) {
            if (_row_idx_in_record_batch == 0) {
                if (!_schema->Equals(batch.arrow_data->schema())
                        && 0 != VectorizeHelpper::change_arrow_record_batch_schema(_schema, batch.arrow_data, &(batch.arrow_data), true)) {
                    return arrow::Status::IOError("change arrow record batch schema fail");
                }
            } 
            *out = batch.arrow_data->Slice(_row_idx_in_record_batch, FLAGS_chunk_size);
            _select_node->add_num_rows_returned((*out)->num_rows());
            _row_idx_in_record_batch += FLAGS_chunk_size;
            if (batch.arrow_data->num_rows() <= _row_idx_in_record_batch) {
                ++_record_batch_idx;
                _row_idx_in_record_batch = 0;
            }
            //DB_WARNING("row batch size: %ld, _record_batch_idx: %d, _row_idx_in_record_batch: %ld, values: %s", (*out)->num_rows(), _record_batch_idx, _row_idx_in_record_batch, (*out)->ToString().c_str());
            return arrow::Status::OK();
        } else if (batch.row_data != nullptr && batch.row_data->size() > 0) {
            // memrow -> arrow recordbatch
            if (_chunk == nullptr) {
                _chunk = std::make_shared<Chunk>();
                if (_chunk->init(_tuples) != 0) {
                    return arrow::Status::IOError("chunk init fail");
                }
            }
            // chunk复用，内部会reset, 行返回数据不会超过chunk_size, 不需要slice, 根据tuple内容构建recordbatch
            if (0 != batch.row_data->transfer_rowbatch_to_arrow(_tuples, _chunk, nullptr, out)) {
                return arrow::Status::IOError("transfer rowbatch to arrow fail");
            }
            // 调整schema
            if (0 != VectorizeHelpper::change_arrow_record_batch_schema(_schema, *out, out, true)) {
                return arrow::Status::IOError("change arrow record batch schema fail");
            }
            _select_node->add_num_rows_returned((*out)->num_rows());
            ++_record_batch_idx;
            _row_idx_in_record_batch = 0;
            return arrow::Status::OK();
        }
        ++_record_batch_idx;
    }
    if (_record_batch_idx >= region_batches.size()) {
        _eos = true;
        out->reset();
        return arrow::Status::OK();
    }
    return arrow::Status::OK();
}

int SelectManagerNode::merge_fetcher_store_run(RuntimeState* state, ExecNode* exec_node) {
    RocksdbScanNode* scan_node = static_cast<RocksdbScanNode*>(exec_node);
    int64_t main_table_id = scan_node->table_id();
    int32_t tuple_id = scan_node->tuple_id();
    auto pri_info = _factory->get_index_info_ptr(main_table_id);
    if (pri_info == nullptr) {
        DB_WARNING("pri index info not found table_id:%ld", main_table_id);
        return -1;
    }
    butil::FlatSet<std::string> filter;
    filter.init(12301);

    SmartRecord record_template = _factory->new_record(main_table_id);
    FilterNode* filter_node = static_cast<FilterNode*>(scan_node->get_parent());
    int64_t affected_rows = 0;


    auto remove_batch_same_row = [&](std::shared_ptr<RowBatch>& batch, std::shared_ptr<RowBatch> uniq_batch) -> int {
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
            MutTableKey  key;
            if (record->encode_key(*pri_info.get(), key, pri_info->fields.size(), false, false) != 0) {
                DB_FATAL("Fail to encode_key left, table:%ld", pri_info->id);
                return -1;
            }
            if (filter.seek(key.data()) != nullptr) {
                continue;
            }
            filter.insert(key.data());
            uniq_batch->move_row(std::move(mem_row));
            affected_rows++;
        }
        return 0;
    };

    for (auto& index_info : scan_node->merge_index_infos()) {
        // filter_node->modifiy_pruned_conjuncts_by_index(index_info._pruned_conjuncts);
        // scan_node中交换filter_node的_pruned_conjuncts
        scan_node->swap_index_info(index_info);
        if (filter_node->get_limit() != -1 && filter_node->pruned_conjuncts().empty()) {
            scan_node->set_limit(filter_node->get_limit());
        }
        FetcherStore* fetcher_store = nullptr;
        FetcherInfo main_fetcher;
        ScanIndexInfo* main_scan_index = scan_node->main_scan_index();
        int ret = 0;
        if (main_scan_index == nullptr) {
            return -1;
        }
        auto& scan_index_info = *main_scan_index;
        auto index_ptr = _factory->get_index_info_ptr(scan_index_info.router_index_id);
        if (index_ptr == nullptr) {
           DB_WARNING("invalid index info: %ld", scan_index_info.router_index_id);
           return -1;
        }
        _factory->get_region_by_key(main_table_id,
                        *index_ptr, scan_index_info.router_index,
                        scan_index_info.region_infos,
                        &scan_index_info.region_primary,
                        scan_node->get_partition());
        scan_node->set_region_infos(scan_index_info.region_infos);

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

        for (auto& pair : fetcher_store->start_key_sort) {
            auto iter = fetcher_store->region_batch.find(pair.second);
            if (iter == fetcher_store->region_batch.end()) {
                continue;
            }
            auto& batch = iter->second.row_data;
            if (batch != nullptr && batch->size() != 0) {
                std::shared_ptr<RowBatch> uniq_batch = std::make_shared<RowBatch>();
                auto ret = remove_batch_same_row(batch, uniq_batch);
                if (ret != 0) {
                    return ret;
                }
                if (uniq_batch != nullptr && uniq_batch->size() != 0) {
                    _sorter->add_batch(uniq_batch);
                }
            }
            fetcher_store->region_batch.erase(iter);
        }
    }
    // 无sort节点时不会排序，按顺序输出
    _sorter->merge_sort();
    return affected_rows;
}

}
