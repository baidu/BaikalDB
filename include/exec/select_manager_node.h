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
#include "sort_node.h"
#include "scan_node.h"
#include "limit_node.h"
#include "table_record.h"
#include "proto/store.interface.pb.h"
#include "sorter.h"
#include "mem_row_compare.h"
#include "fetcher_store.h"

namespace baikaldb {
struct FetcherInfo {
    enum Status {
        S_INIT = 0,
        S_SUCC,
        S_FAIL
    };
    int64_t dynamic_timeout_ms = -1;
    GlobalBackupType global_backup_type = GBT_INIT;
    ScanIndexInfo* scan_index = nullptr;
    std::atomic<Status> status = { S_INIT };
    FetcherStore fetcher_store;
};

class SelectManagerNode : public ExecNode {
public:
    SelectManagerNode() {
        _factory = SchemaFactory::get_instance();
    }
    virtual ~SelectManagerNode() {
        for (auto expr : _derived_table_projections) {
            ExprNode::destroy_tree(expr);
        }
        if (_sub_query_node != nullptr) {
            delete _sub_query_node;
            _sub_query_node = nullptr;
        }
    }
    virtual int open(RuntimeState* state);
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos);
    virtual void close(RuntimeState* state) {
        ExecNode::close(state);
        if (_sub_query_node != nullptr) {
            _sub_query_node->close(state);
        }
        for (auto expr : _derived_table_projections) {
            expr->close();
        }
        _sorter = nullptr;
    }
    int init_sort_info(SortNode* sort_node) {
        _slot_order_exprs = sort_node->slot_order_exprs();
        _is_asc = sort_node->is_asc();
        _is_null_first = sort_node->is_null_first();
        return 0;
    }
    int single_fetcher_store_open(FetcherInfo* fetcher, RuntimeState* state, ExecNode* exec_node);

    void multi_fetcher_store_open(FetcherInfo* self_fetcher, FetcherInfo* other_fetcher,
        RuntimeState* state, ExecNode* exec_node);
    int fetcher_store_run(RuntimeState* state, ExecNode* exec_node);
    int open_global_index(FetcherInfo* fetcher, RuntimeState* state,
                          ExecNode* exec_node,
                          int64_t global_index_id,
                          int64_t main_table_id);
    int construct_primary_possible_index(
                          FetcherStore& fetcher_store,
                          ScanIndexInfo* scan_index_info,
                          RuntimeState* state,
                          ExecNode* exec_node,
                          int64_t main_table_id);

    int construct_primary_possible_index_use_limit(
                          FetcherStore& fetcher_store,
                          ScanIndexInfo* scan_index_info,
                          RuntimeState* state,
                          ExecNode* exec_node,
                          int64_t main_table_id,
                          LimitNode* limit);

    void set_sub_query_runtime_state(RuntimeState* state) {
        _sub_query_runtime_state = state;
    }
    void steal_projections(std::vector<ExprNode*>& projections) {
        _derived_table_projections.swap(projections);
    }

    void set_sub_query_node(ExecNode* sub_query_node) {
        _sub_query_node = sub_query_node;
    }

    int subquery_open(RuntimeState* state);

    void set_slot_column_mapping(std::map<int32_t, int32_t>& slot_column_map) {
        _slot_column_mapping.swap(slot_column_map);
    }
    void set_derived_tuple_id(int32_t derived_tuple_id) {
        _derived_tuple_id = derived_tuple_id;
    }
private:
    //允许fetcher回来后排序
    std::vector<ExprNode*> _slot_order_exprs;
    std::vector<bool> _is_asc;
    std::vector<bool> _is_null_first;
    std::shared_ptr<MemRowCompare> _mem_row_compare;
    std::shared_ptr<Sorter> _sorter;
    std::map<int32_t, int32_t> _index_slot_field_map;
    SchemaFactory*  _factory = nullptr;
    RuntimeState*   _sub_query_runtime_state = nullptr;
    ExecNode*       _sub_query_node = nullptr;
    std::vector<ExprNode*>  _derived_table_projections;
    std::map<int32_t, int32_t>  _slot_column_mapping;
    int32_t         _derived_tuple_id = 0;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
