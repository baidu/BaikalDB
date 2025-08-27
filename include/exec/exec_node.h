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

#include <vector>
#include "table_record.h"
#include "expr_node.h"
#include "row_batch.h"
#include "proto/plan.pb.h"
#include "proto/meta.interface.pb.h"
#include "mem_row_descriptor.h"
#include "runtime_state.h"
#include "mpp_property.h"
namespace baikaldb {

inline bool is_dml_op_type(const pb::OpType& op_type) {
        if (op_type == pb::OP_INSERT
             || op_type == pb::OP_DELETE
             || op_type == pb::OP_UPDATE
             || op_type == pb::OP_SELECT_FOR_UPDATE
             || op_type == pb::OP_PARTIAL_ROLLBACK
             || op_type == pb::OP_KV_BATCH) {
            return true;
        }
        return false;
    }
inline bool is_cold_data_op_type(const pb::OpType& op_type) {
        if (op_type != pb::OP_SELECT 
             && op_type != pb::OP_TRUNCATE_TABLE
             && op_type != pb::OP_ROLLUP_REGION_INIT
             && op_type != pb::OP_ROLLUP_REGION_FINISH
             && op_type != pb::OP_ROLLUP_REGION_FAILED) {
            return true;
        }
        return false;
    }
inline bool is_unsupport_rollup_ddl_type(const pb::StoreReq* request) {
        if (request->op_type() == pb::OP_DELETE
             || request->op_type() == pb::OP_UPDATE) {
            return true;
        }
        if (request->op_type() == pb::OP_INSERT) {
            for (int i = 0; i < request->plan().nodes_size(); ++i) {
                const auto& node = request->plan().nodes(i);
                if (node.derive_node().insert_node().is_merge() == false) {
                    return true;
                }
            }
        }
        return false;
    }
inline bool is_2pc_op_type(const pb::OpType& op_type) {
    if (op_type == pb::OP_PREPARE
            || op_type == pb::OP_ROLLBACK
            || op_type == pb::OP_COMMIT) {
        return true;
    }
    return false;
}

struct CreateExecOptions {
    bool use_column_storage = false;
};

class RuntimeState;
class QueryContext;
class ExecNode {
public:
    ExecNode() {
    }
    virtual ~ExecNode() {
        for (auto& e : _children) {
            SAFE_DELETE(e);
        }
    }
    virtual int init(const pb::PlanNode& node);
    /* ret: 0 - SUCCESS
     *     -1 - ERROR
     *     -2 - EMPTY_RESULT
     */
    virtual int expr_optimize(QueryContext* ctx);
    int common_expr_optimize(std::vector<ExprNode*>* exprs);

    //input: 需要下推的条件
    //input_exprs 既是输入参数，也是输出参数
    //output:能推的条件尽量下推，不能推的条件做一个filter node, 连接到节点的上边
    virtual int predicate_pushdown(std::vector<ExprNode*>& input_exprs);

    // @brief 下推聚合节点
    // @param ctx: 当前查询的上下文
    // @param agg_node: 需要下推的聚合节点
    virtual int agg_pushdown(QueryContext* ctx, ExecNode* agg_node);
    virtual bool can_agg_pushdown();

    virtual void remove_additional_predicate(std::vector<ExprNode*>& input_exprs);
    // 在计划树中插入一个filter node，filter node是该节点的父节点
    void add_filter_node(const std::vector<ExprNode*>& input_exprs, pb::PlanNodeType type = pb::TABLE_FILTER_NODE);
    // 在计划树中插入一个filter node，filter node是该节点的子节点
    void add_filter_node_as_child(const std::vector<ExprNode*>& input_exprs, pb::PlanNodeType type = pb::TABLE_FILTER_NODE);
    
    virtual void get_all_dual_scan_node(std::vector<ExecNode*>& exec_nodes);
    void get_node(const pb::PlanNodeType node_type, std::vector<ExecNode*>& exec_nodes);
    ExecNode* get_node(const pb::PlanNodeType node_type);
    // 获取执行计划树中最后一个指定类型的节点
    ExecNode* get_last_node(const pb::PlanNodeType node_type);
    ExecNode* get_node_pass_subquery(const pb::PlanNodeType node_type);
    void get_node_pass_subquery(const pb::PlanNodeType node_type, std::vector<ExecNode*>& exec_nodes);
    ExecNode* get_parent() {
        return _parent;
    }
    ExecNode* get_parent_node(const pb::PlanNodeType node_type) {
        if (_node_type == node_type) {
            return this;
        }

        ExecNode* parent = _parent;
        while (parent != nullptr && parent != this) {
            if (parent->_node_type == node_type) {
                return parent;
            } else {
                parent = parent->_parent;
            }
        }
        return nullptr;
    }
    void join_get_scan_nodes(const pb::PlanNodeType node_type, std::vector<ExecNode*>& exec_nodes);
    bool need_seperate();
    virtual int open(RuntimeState* state);
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
        *eos = true;
        return 0;
    }
    virtual void close(RuntimeState* state) {
        _num_rows_returned = 0;
        _return_empty = false;
        _delay_fetcher_store = false;
        _node_exec_type = pb::EXEC_ROW;
        for (auto e : _children) {
            e->close(state);
        }
    }

    virtual void reset(RuntimeState* state) {
        _num_rows_returned = 0;
        _return_empty = false;
        for (auto e : _children) {
            e->reset(state);
        }
    }
    virtual std::vector<ExprNode*>* mutable_conjuncts() {
        return NULL;
    }
    virtual void find_place_holder(std::unordered_multimap<int, ExprNode*>& placeholders) {
        for (size_t idx = 0; idx < _children.size(); ++idx) {
            _children[idx]->find_place_holder(placeholders);
        }
    }
    virtual void replace_slot_ref_to_literal(const std::set<int64_t>& sign_set, std::map<int64_t, std::vector<ExprNode*>>& literal_maps) {
        for (auto child : _children) {
            if (child->node_type() == pb::JOIN_NODE || child->node_type() == pb::APPLY_NODE) {
                continue;
            }
            child->replace_slot_ref_to_literal(sign_set, literal_maps);
        }
    }
    /**
     * @brief 生成explain的每行记录
     * @param ctx 查询上下文，从中获取subquery给子查询explain使用
     * @param output 输出，每个表扫描对应一条
     * @param next_id 全局该生成的下一个id
     * @param display_id join子节点id应该显示为一样，-1表示使用next_id作为display_id
     * @return 返回本节点的id 设置虚拟表名时使用 <derived3> 这样的
    */
    virtual int show_explain(QueryContext* ctx, std::vector<std::map<std::string, std::string>>& output, int& next_id, int display_id) {
        int return_id = (display_id != -1 ? display_id : next_id);
        for (auto child : _children) {
            child->show_explain(ctx, output, next_id, display_id);
        }
        return return_id;
    }

    static std::vector<std::map<std::string, std::string>>::iterator 
    find_explain_record_by_id(std::vector<std::map<std::string, std::string>>& output, int id) {
        return std::find_if(output.begin(), output.end(), [id](const std::map<std::string, std::string>& explain_map) {
            auto it = explain_map.find("id");
            if (it != explain_map.end() && it->second == std::to_string(id)) {
                return true;
            }
            return false;
        });
    }

    virtual bool check_satisfy_condition(MemRow* row) {
        for (auto child : _children) {
            if (!child->check_satisfy_condition(row)) {
                return false;
            }
        }
        return true;
    }
    ExecNode* get_specified_node(const pb::PlanNodeType node_type) {
        if (node_type == _node_type) {
            return this;
        }
        for (auto child : _children) {
            if (child->node_type() == pb::JOIN_NODE || child->node_type() == pb::APPLY_NODE) {
                return nullptr;
            }
            ExecNode* exec_node = child->get_specified_node(node_type);
            if (exec_node != nullptr) {
                return exec_node;
            }
        }
        return nullptr;
    }
    bool only_has_specified_node(const std::unordered_set<pb::PlanNodeType>& node_types) {
        if (node_types.find(_node_type) == node_types.end()) {
            return false;
        }
        for (auto child : _children) {
            if (child == nullptr) {
                DB_FATAL("child is nulptr");
                return false;
            }
            bool ret = child->only_has_specified_node(node_types);
            if (!ret) {
                return false;
            }
        }
        return true;
    }

    void set_parent(ExecNode* parent_node) {
        _parent = parent_node;
    }
    void create_trace();
    void set_trace(pb::TraceNode* trace) {
        _trace = trace;
    }
    pb::TraceNode* get_trace() {
        return _trace;
    }
    void add_child(ExecNode* exec_node) {
        if (exec_node == nullptr) {
            return ;
        }
        _children.push_back(exec_node);
        exec_node->set_parent(this);
    }
    void add_child(ExecNode* exec_node, size_t idx) {
        if (exec_node == nullptr || idx > _children.size()) {
            return ;
        }
        _children.insert(_children.begin() + idx, exec_node);
        exec_node->set_parent(this);
    }
    void clear_children() {
        for (auto child : _children) {
            if (child->_parent == this) {
                child->_parent = NULL;
            }
        }
        _children.clear();
    }
    int replace_child(ExecNode* old_child, ExecNode* new_child) {
        for (auto& child : _children) {
            if (child == old_child) {
                new_child->set_parent(this);
                if (old_child->_parent == this) {
                    old_child->_parent = NULL;
                }
                child = new_child;
                return 0;
            }
        }
        return -1;
    }
    size_t children_size() {
        return _children.size();
    }
    ExecNode* children(size_t idx) {
        if (_children.size() == 0) {
            return nullptr;
        }
        return _children[idx];
    }
    std::vector<ExecNode*> children() {
        return _children;
    }
    std::vector<ExecNode*>* mutable_children() {
        return &_children;
    }
    void set_return_empty() {
        _return_empty = true;
        for (auto child : _children) {
            child->set_return_empty();
        }
    }
    bool reached_limit() {
        return _limit != -1 && _num_rows_returned >= _limit;
    }
    bool will_reach_limit(int64_t add_size) {
        return _limit != -1 && _num_rows_returned + add_size >= _limit;
    }
    void set_limit(int64_t limit) {
        _limit = limit;
        if (_limit < 0) {
            _limit = -1;
        }
    }
    virtual void reset_limit(int64_t limit) {
        _limit = limit;
        for (auto child : _children) {
            child->reset_limit(limit);
        }
    }
    int64_t get_limit() {
        return _limit;
    }
    pb::PlanNode* mutable_pb_node() {
        return &_pb_node;
    }
    const pb::PlanNode& pb_node() {
        return _pb_node;
    }
    pb::PlanNodeType node_type() {
        return _node_type;
    }
    bool is_filter_node() {
        return _node_type == pb::TABLE_FILTER_NODE ||
            _node_type == pb::WHERE_FILTER_NODE ||
            _node_type == pb::HAVING_FILTER_NODE;
    }
    std::map<int64_t, pb::RegionInfo>& region_infos() {
        return _region_infos;
    }
    void set_region_infos(std::map<int64_t, pb::RegionInfo> region_infos) {
        _region_infos.swap(region_infos);
    }
    //除了表达式外大部分直接沿用保存的pb
    virtual void transfer_pb(int64_t region_id, pb::PlanNode* pb_node);
    static void create_pb_plan(int64_t region_id, pb::Plan* plan, ExecNode* root);
    static int create_tree(const pb::Plan& plan, ExecNode** root, const CreateExecOptions& options);
    static void destroy_tree(ExecNode* root) {
        delete root;
    }
    virtual int push_cmd_to_cache(RuntimeState* state,
                                  pb::OpType op_type,
                                  ExecNode* store_request,
                                  int seq_id);
    std::map<int64_t, std::vector<SmartRecord>>& get_return_records() {
        return _return_records;
    }
    std::map<int64_t, std::vector<SmartRecord>>& get_return_old_records() {
        return _return_old_records;
    }
    virtual pb::LockCmdType lock_type() { return pb::LOCK_INVALID; }
    bool local_index_binlog() const {
        return _local_index_binlog;
    }
    const std::vector<int64_t>& get_partition() const {
        return _partitions;
    }
    void replace_partition(const std::set<int64_t>& partition_ids, bool is_manual) {
        // 之前人工指定过partition(p20)，则不再计算
        if (_is_manual) {
            return;
        }
        _is_manual = is_manual;
        // fullexport + join + partition表会多轮计算partition
        _partitions.clear();
        _partitions.assign(partition_ids.begin(), partition_ids.end());
    }
    bool is_get_keypoint() {
        return _is_get_keypoint;
    }
    bool set_has_optimized(bool has_optimized) {
        _has_optimized = has_optimized;
    }
    bool has_optimized() {
        if (_has_optimized) {
            return true;
        }
        for (auto child : _children) {
            if (child->has_optimized()) {
                return true;
            }
        } 
        return false;
    }

    void add_num_rows_returned(int64_t row) {
        _num_rows_returned += row;
    }
    bool is_return_empty() {
        return _return_empty;
    }
    // for vectorized execute
    virtual bool can_use_arrow_vector(RuntimeState* state) {
        return false;
    }
    virtual int build_arrow_declaration(RuntimeState* state) {
        DB_FATAL_STATE(state, "not support type: %d", _node_type);
        return -1;
    }
    void set_node_exec_type(const pb::ExecuteType& type) {
        _node_exec_type = type;
    }

    void set_delay_fetcher_store(bool delay) {
        _delay_fetcher_store = delay;
    }

    bool is_delay_fetcher_store() {
        return _delay_fetcher_store;
    }

    pb::ExecuteType node_exec_type() {
        return _node_exec_type;
    }

    void set_data_schema(const std::set<ColumnInfo>& schema) {
        _data_schema = schema;
    }

    std::set<ColumnInfo>& data_schema() {
        return _data_schema;
    }

    virtual int set_partition_property_and_schema(QueryContext* ctx) {
        int no_valid_input_child_size = 0;
        _partition_property.type = pb::AnyType;
        for (auto& c : _children) {
            if (0 != c->set_partition_property_and_schema(ctx)) {
                return -1;
            }
            if (c->partition_property()->has_no_input_data) {
                no_valid_input_child_size += 1;
            }
        }
        if (_children.size() > 0) {
            _data_schema.insert(_children[0]->data_schema().begin(), 
                                _children[0]->data_schema().end());
            _partition_property = *(_children[0]->partition_property());
        }
        if (_return_empty || (_children.size() > 0 && no_valid_input_child_size == _children.size())) {
            _partition_property.has_no_input_data = true;
        }
        return 0;
    }

    void set_partition_property(NodePartitionProperty* partition_property) {
        _partition_property.type = partition_property->type;
        _partition_property.hash_partition_propertys = partition_property->hash_partition_propertys;
        _partition_property.add_need_cast_string_columns(partition_property->need_cast_string_columns);
    }

    NodePartitionProperty* partition_property() {
        return &_partition_property;
    }
    
    bool shrink_partition_property(std::shared_ptr<HashPartitionColumns> hash_columns, NodePartitionProperty* partition_property);

    virtual void update_runtime_state(RuntimeState* state) {
        for (auto& child : _children) {
            child->update_runtime_state(state);
        }
    }

    virtual int prune_columns(QueryContext* ctx, const std::unordered_set<int32_t>& invalid_column_ids) {
        for (auto child : _children) {
            if (child == nullptr) {
                return -1;
            }
            int ret = child->prune_columns(ctx, invalid_column_ids);
            if (ret != 0) {
                return ret;
            }
        }
        return 0;
    }

    static void encode_exprs_key(std::vector<ExprNode*>& exprs, MemRow* row, MutTableKey& key);

protected:
    bool _has_optimized = false;
    int64_t _limit = -1;
    int64_t _num_rows_returned = 0;
    bool _is_explain = false;
    bool _is_get_keypoint = false;
    bool _return_empty = false;
    pb::PlanNodeType _node_type;
    std::vector<ExecNode*> _children;
    ExecNode* _parent = nullptr;
    pb::PlanNode _pb_node;
    std::map<int64_t, pb::RegionInfo> _region_infos;
    pb::TraceNode* _trace = nullptr;
    bool  _local_index_binlog = false;
    bool  _is_manual = false;
    std::vector<int64_t> _partitions {0};

    // mpp
    NodePartitionProperty _partition_property;

    pb::ExecuteType _node_exec_type = pb::EXEC_ROW;  // node实际上执行的类型
    bool _delay_fetcher_store = false;
    
    //返回给baikaldb的结果
    std::map<int64_t, std::vector<SmartRecord>> _return_old_records;
    std::map<int64_t, std::vector<SmartRecord>> _return_records;

    // source schema for mpp
    std::set<ColumnInfo> _data_schema;
private:
    static int create_tree(const pb::Plan& plan, int* idx, ExecNode* parent, 
                           ExecNode** root, const CreateExecOptions& options);
    static int create_exec_node(const pb::PlanNode& node, ExecNode** exec_node, const CreateExecOptions& options);
};
typedef std::shared_ptr<pb::TraceNode> SmartTrace;
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
