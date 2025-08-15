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

#include "agg_node.h"
#include "runtime_state.h"
#include "network_socket.h"
#include "arrow_io_excutor.h"

namespace baikaldb {
class DualScanNode : public ExecNode {
public:
    DualScanNode() {
    }
    virtual ~DualScanNode() {
        for (auto expr : _derived_table_projections) {
            ExprNode::destroy_tree(expr);
        }
        if (_sub_query_runtime_state != nullptr
                 && _sub_query_node != nullptr
                 && _sub_query_runtime_state->use_mpp) {
            ExecNode::destroy_tree(_sub_query_node);
        }
        if (_need_delete_runtime_state) {
            delete _sub_query_runtime_state;
        }
    }
    virtual void close(RuntimeState* state) {
        if (_sub_query_runtime_state != nullptr && _sub_query_node != nullptr) {
            state->inc_num_returned_rows(_sub_query_runtime_state->num_returned_rows());
            state->inc_num_affected_rows(_sub_query_runtime_state->num_affected_rows());
            state->inc_num_scan_rows(_sub_query_runtime_state->num_scan_rows());
            state->inc_num_filter_rows(_sub_query_runtime_state->num_filter_rows());
            state->region_count += _sub_query_runtime_state->region_count;
            state->inc_db_handle_rows(_sub_query_runtime_state->db_handle_rows());
            state->inc_db_handle_bytes(_sub_query_runtime_state->db_handle_bytes());
        }
        ExecNode::close(state);
        if (_sub_query_node != nullptr) {
            _sub_query_node->close(state);
        }       
        for (auto expr : _derived_table_projections) {
            expr->close();
        }
        _arrow_io_executor.reset();
        _mpp_hash_slot_refs_mapping.clear();
    }

    virtual void update_runtime_state(RuntimeState* state) {
        if (_sub_query_runtime_state != nullptr && _sub_query_node != nullptr) {
            _sub_query_node->update_runtime_state(_sub_query_runtime_state);
            state->inc_num_returned_rows(_sub_query_runtime_state->num_returned_rows());
            state->inc_num_affected_rows(_sub_query_runtime_state->num_affected_rows());
            state->inc_num_scan_rows(_sub_query_runtime_state->num_scan_rows());
            state->inc_num_filter_rows(_sub_query_runtime_state->num_filter_rows());
            state->region_count += _sub_query_runtime_state->region_count;
            state->inc_db_handle_rows(_sub_query_runtime_state->db_handle_rows());
            state->inc_db_handle_bytes(_sub_query_runtime_state->db_handle_bytes());
        }
    }

    virtual int init(const pb::PlanNode& node) override;
    virtual int open(RuntimeState* state) override;
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos) override;
    virtual void get_all_dual_scan_node(std::vector<ExecNode*>& exec_nodes) override;
    // 条件下推子查询
    virtual int predicate_pushdown(std::vector<ExprNode*>& input_exprs) override;
    virtual int prune_columns(QueryContext* ctx, 
                              const std::unordered_set<int32_t>& invalid_column_ids) override;
    virtual int agg_pushdown(QueryContext* ctx, ExecNode* agg_node) override;
    virtual void transfer_pb(int64_t region_id, pb::PlanNode* pb_node);
    bool can_predicate_pushdown();
    virtual bool can_agg_pushdown() override;
    virtual int show_explain(QueryContext* ctx, std::vector<std::map<std::string, std::string>>& output, int& next_id, int display_id);
    int child_show_explain(QueryContext* ctx, std::vector<std::map<std::string, std::string>>& output, int& next_id, int display_id);
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
    virtual bool can_use_arrow_vector(RuntimeState* state);
    virtual int set_partition_property_and_schema(QueryContext* ctx);
    virtual int build_arrow_declaration(RuntimeState* state);
    int set_sub_query_node_partition_property(NodePartitionProperty* outer_property);
    void set_has_subquery(bool has_subquery) {
        _has_subquery = has_subquery;
    }
    void set_sub_query_node(ExecNode* sub_query_node) {
        _sub_query_node = sub_query_node;
    }
    void set_sub_query_ctx(QueryContext* sub_query_ctx) {
        _sub_query_ctx = sub_query_ctx;
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
            _derived_table_projections_agg_or_window_vec.emplace_back(
                                projection->has_agg() || projection->has_window());
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
    QueryContext* sub_query_ctx() {
        return _sub_query_ctx;
    }
    RuntimeState* sub_query_runtime_state() {
        return _sub_query_runtime_state;
    }
    std::pair<int32_t, int32_t>& agg_tuple_id_pair() {
        return _agg_tuple_id_pair;
    }
    std::vector<int32_t>& agg_slot_ids() {
        return _agg_slot_ids;
    }
    int transfer_outer_slot_to_inner(ExprNode** input_expr);

private:
    // @brief   收集子查询中group by列涉及的SlotRef，如果group by列是表达式，不会收集表达式中的SlotRef中；
    //          SlotRef使用{tuple_id, slot_id}进行表示
    // @param   subquery_has_agg 判断子查询中是否包含AggNode
    // @param   group_by_slots 收集的group by列涉及的SlotRef
    void get_subquery_group_by_slots(
            bool& subquery_has_agg, std::set<std::pair<int32_t, int32_t>>& group_by_slots);

    // @brief   收集子查询中每个WindowNode的partition by列涉及的SlotRef的"交集"，如果partition by列是表达式，不会收集表达式中的SlotRef中；
    //          SlotRef使用{tuple_id, slot_id}进行表示
    // @param   subquery_has_window 判断子查询中是否包含WindowNode
    // @param   partition_by_slots 收集的WindowNode的partition by列涉及的SlotRef的"交集"
    void get_subquery_window_partition_by_slots(
            bool& subquery_has_window, std::set<std::pair<int32_t, int32_t>>& partition_by_slots);

    // 判断expr是否可以下推到子查询中
    int can_expr_predicate_pushdown(bool subquery_has_agg, 
                                    bool subquery_has_window,
                                    const std::set<std::pair<int32_t, int32_t>>& group_by_slots, 
                                    const std::set<std::pair<int32_t, int32_t>>& partition_by_slots,
                                    ExprNode* expr,
                                    bool& can_pushdown);
    // 聚合下推使用
    int create_subquery_agg_tuple(QueryContext* ctx, QueryContext* subquery_ctx, AggNode* agg_node);
    int create_subquery_agg_node(QueryContext* subquery_ctx, AggNode* agg_node, AggNode*& subquery_agg_node);

private:
    int32_t _tuple_id = 0;
    int64_t _table_id = 0;

    bool                       _has_subquery = false;
    ExecNode*                  _sub_query_node = nullptr; // _sub_query_node包含PacketNode，是完整的子查询执行计划树
    QueryContext*              _sub_query_ctx = nullptr;
    RuntimeState*              _sub_query_runtime_state = nullptr;
    std::map<int32_t, int32_t> _slot_column_mapping; // <外层slot_id，内层projection序号>
    std::vector<ExprNode*>     _derived_table_projections;
    // 每个projection是否为聚合/窗口函数，需要单独记录，避免Agg ExprNode被优化成SlotRef
    std::vector<bool>          _derived_table_projections_agg_or_window_vec;
    bool                       _is_runtime_filter = false; // 是否join runtime filter场景在进行谓词下推
    bool                       _need_delete_runtime_state = false; // 自己new出来的需要删除runtime state
    // 聚合下推使用
    std::pair<int32_t, int32_t> _agg_tuple_id_pair = {-1, -1}; // <外层tuple_id, 内层tuple_id> 
    std::vector<int32_t> _agg_slot_ids; // 聚合tuple slot_id
    std::unordered_set<int32_t> _multi_distinct_agg_slot_ids; // 多distinct对应的tuple slot_id
    std::shared_ptr<BthreadArrowExecutor> _arrow_io_executor; 

    // mpp使用
    std::unordered_map<std::string, std::string> _mpp_hash_slot_refs_mapping;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
