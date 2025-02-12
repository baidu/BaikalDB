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
#include "runtime_state.h"

namespace baikaldb {
class DualScanNode : public ExecNode {
public:
    DualScanNode() {
    }
    virtual ~DualScanNode() {
        for (auto expr : _derived_table_projections) {
            ExprNode::destroy_tree(expr);
        }
    }
    virtual void close(RuntimeState* state) {
        if (_sub_query_runtime_state != nullptr && _sub_query_node != nullptr && _sub_query_node->is_delay_fetcher_store()) {
            state->inc_num_returned_rows(_sub_query_runtime_state->num_returned_rows());
            state->inc_num_affected_rows(_sub_query_runtime_state->num_affected_rows());
            state->inc_num_scan_rows(_sub_query_runtime_state->num_scan_rows());
            state->inc_num_filter_rows(_sub_query_runtime_state->num_filter_rows());
            state->region_count += _sub_query_runtime_state->region_count;
        }
        ExecNode::close(state);
        if (_sub_query_node != nullptr) {
            _sub_query_node->close(state);
        }       
        for (auto expr : _derived_table_projections) {
            expr->close();
        }
        _intermediate_table.reset();
    }
    int init(const pb::PlanNode& node) {
        int ret = 0;
        ret = ExecNode::init(node);
        if (ret < 0) {
            DB_WARNING("ExecNode::init fail, ret:%d", ret);
            return ret;
        }
        _tuple_id = node.derive_node().scan_node().tuple_id();
        _table_id = node.derive_node().scan_node().table_id();
        _node_type = pb::DUAL_SCAN_NODE;
        return 0;
    }
    virtual int open(RuntimeState* state) override;
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos) override;
    virtual void get_all_dual_scan_node(std::vector<ExecNode*>& exec_nodes) override;
    // 条件下推子查询
    virtual int predicate_pushdown(std::vector<ExprNode*>& input_exprs) override;
    bool can_predicate_pushdown();

    int64_t table_id() const {
        return _table_id;
    }
    int32_t tuple_id() const {
        return _tuple_id;
    }
    int32_t slot_tuple_id() {
        const pb::ScanNode& pb_scan_node = pb_node().derive_node().scan_node();
        if (pb_scan_node.has_union_tuple_id()) {
            return pb_scan_node.union_tuple_id();
        }
        return _tuple_id;
    }
    bool is_union_subquery() {
        return pb_node().derive_node().scan_node().has_union_tuple_id();
    }
    virtual bool can_use_arrow_vector();
    virtual int build_arrow_declaration(RuntimeState* state);
    void set_has_subquery(bool has_subquery) {
        _has_subquery = has_subquery;
    }
    void set_sub_query_node(ExecNode* sub_query_node) {
        _sub_query_node = sub_query_node;
    }
    void set_slot_column_mapping(std::map<int32_t, int32_t>& slot_column_map) {
        _slot_column_mapping = slot_column_map;
    }
    int create_derived_table_projections(const pb::PlanNode& packet_node) {
        for (int i = 0; i < packet_node.derive_node().packet_node().projections_size(); i++) {
            ExprNode* projection = nullptr;
            auto& expr = packet_node.derive_node().packet_node().projections(i);
            int ret = ExprNode::create_tree(expr, &projection);
            if (ret < 0) {
                return ret;
            }
            if (projection == nullptr) {
                return -1;
            }
            _derived_table_projections.emplace_back(projection);
            _derived_table_projections_agg_vec.emplace_back(projection->has_agg());
        }
        return 0;
    }
    void set_sub_query_runtime_state(RuntimeState* state) {
        _sub_query_runtime_state = state;
    }
    void steal_projections(std::vector<ExprNode*>& projections) {
        for (auto expr : _derived_table_projections) {
            ExprNode::destroy_tree(expr);
        }
        _derived_table_projections.clear();
        _derived_table_projections.swap(projections);
    }
    ExecNode* sub_query_node() {
        return _sub_query_node;
    }
    RuntimeState* sub_query_runtime_state() {
        return _sub_query_runtime_state;
    }

private:
    int32_t _tuple_id = 0;
    int64_t _table_id = 0;

    bool                       _has_subquery = false;
    ExecNode*                  _sub_query_node = nullptr; // _sub_query_node包含PacketNode，是完整的子查询执行计划树
    RuntimeState*              _sub_query_runtime_state = nullptr;
    std::map<int32_t, int32_t> _slot_column_mapping;
    std::vector<ExprNode*>     _derived_table_projections;
    std::vector<bool>          _derived_table_projections_agg_vec; // 每个projection是否为聚合函数
    std::shared_ptr<arrow::Table> _intermediate_table;
    bool                       _is_runtime_filter = false; // 是否join runtime filter场景在进行谓词下推
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
