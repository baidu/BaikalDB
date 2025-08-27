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
#include "arrow_io_excutor.h"

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
    }
    virtual bool can_use_arrow_vector(RuntimeState* state) {
        for (auto& c : _children) {
            if (!c->can_use_arrow_vector(state)) {
                return false;
            }
        }
        return true;
    }
    virtual int open(RuntimeState* state);
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos);
    virtual void close(RuntimeState* state) {
        ExecNode::close(state);
        if (_pb_node.derive_node().select_manager_node().slot_order_exprs_size() > 0) {
            for (auto& expr : _slot_order_exprs) {
                ExprNode::destroy_tree(expr);
            }
        }
        _sorter = nullptr;
        _region_batches.clear();
        _arrow_responses.clear();
        _arrow_schema.reset();
        _arrow_io_executor.reset();
        _arrow_batch_response.clear();
        _index_collector_cond.reset();
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
    int merge_fetcher_store_run(RuntimeState* state, ExecNode* exec_node);

    int mpp_fetcher_global_index(RuntimeState* state);
    int open_global_index(FetcherInfo* fetcher, RuntimeState* state,
                          ExecNode* exec_node,
                          int64_t global_index_id,
                          int64_t main_table_id);

    int construct_primary_possible_index(
                          FetcherStore& fetcher_store,
                          ScanIndexInfo* scan_index_info,
                          RuntimeState* state,
                          ExecNode* exec_node,
                          int64_t main_table_id,
                          LimitNode* limit = nullptr);

    int construct_primary_possible_index_vectorize(
                      FetcherStore& fetcher_store,
                      ScanIndexInfo* scan_index_info,
                      RuntimeState* state,
                      ExecNode* exec_node,
                      int64_t main_table_id,
                      SmartIndex pri_info,
                      LimitNode* limit);

    int subquery_open(RuntimeState* state);

    int delay_fetcher_store(RuntimeState* state);

    virtual int build_arrow_declaration(RuntimeState* state);

    std::vector<RegionReturnData>& get_region_batches() {
        return _region_batches;
    }

    const std::shared_ptr<arrow::Schema>& get_arrow_schema() {
        return _arrow_schema;
    }

    int32_t get_scan_tuple_id() {
        return _scan_tuple_id;
    }
    bool need_sorter() {
        return _slot_order_exprs.size() > 0;
    }
    void set_index_collector_cond(std::shared_ptr<IndexCollectorCond> cond) {
        _index_collector_cond = cond;
    }
    std::shared_ptr<IndexCollectorCond> get_index_collector_cond() {
        return _index_collector_cond;
    }

    // 以下函数只有mpp模式会涉及
    virtual int init(const pb::PlanNode& node);
    virtual void transfer_pb(int64_t region_id, pb::PlanNode* pb_node);
    bool is_dual_scan() {
        ExecNode* dual_scan = get_node(pb::DUAL_SCAN_NODE);
        return (dual_scan != nullptr);
    }
    bool is_mysql_scan() {
        ExecNode* scan = get_node(pb::SCAN_NODE);
        if (scan == nullptr) {
            return false;
        }
        return static_cast<ScanNode*>(scan)->is_mysql_scan_node();
    }

private:
    //允许fetcher回来后排序
    std::vector<ExprNode*> _slot_order_exprs;
    std::vector<bool> _is_asc;
    std::vector<bool> _is_null_first;
    std::shared_ptr<MemRowCompare> _mem_row_compare;
    std::shared_ptr<Sorter> _sorter;
    SchemaFactory*  _factory = nullptr;
    int32_t         _scan_tuple_id = 0;

    // vectorized
    std::vector<std::shared_ptr<pb::StoreRes>> _arrow_responses;
    std::vector<std::shared_ptr<pb::BatchRegionStoreRes> > _arrow_batch_response;
    std::shared_ptr<arrow::Schema> _arrow_schema;
    std::vector<RegionReturnData>  _region_batches;
    std::shared_ptr<BthreadArrowExecutor> _arrow_io_executor;
    std::shared_ptr<IndexCollectorCond> _index_collector_cond;

    // mpp
    bool _has_er_child = false;
};

class FetcherStoreVectorizedReader : public arrow::RecordBatchReader {
public:
    FetcherStoreVectorizedReader() {}
    virtual ~FetcherStoreVectorizedReader() {}

    // for streaming output use
    std::shared_ptr<arrow::Schema> schema() const override { 
        return _schema;
    }

    int init(SelectManagerNode* select_node, RuntimeState* state);
    
    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override;

    int transfer_row_batch_to_arrow(const std::shared_ptr<RowBatch>& row_batch, std::shared_ptr<arrow::RecordBatch>* out);

private:
    SelectManagerNode* _select_node = nullptr;
    RuntimeState* _state = nullptr;
    std::shared_ptr<arrow::Schema> _schema;
    bool _eos = false;
    bool _need_fetcher_store = false;
    int _record_batch_idx = 0;
    int64_t _row_idx_in_record_batch = 0;
    std::shared_ptr<Chunk> _chunk;
    std::vector<const pb::TupleDescriptor*> _tuples;
    std::shared_ptr<IndexCollectorCond> _index_cond;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
