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

// Brief:  The wrapper of Baidu SQL Parser lib.
#pragma once

#include "logical_planner.h"
#include "query_context.h"

namespace baikaldb {

class SelectPlanner : public LogicalPlanner {
public:

    SelectPlanner(QueryContext* ctx) : 
        LogicalPlanner(ctx),
        _select(nullptr) {}

    SelectPlanner(QueryContext* ctx, const SmartUniqueIdCtx& uniq_ctx, const SmartPlanTableCtx& plan_state) : 
        LogicalPlanner(ctx, uniq_ctx, plan_state),
        _select(nullptr) {}

    virtual ~SelectPlanner() {}

    virtual int plan() override;

private:
    // methods to create plan nodes
    void create_dual_scan_node();

    int create_limit_node();

    int create_agg_node();

    int create_window_and_sort_nodes();

    int parse_select_star(parser::SelectField* field);
    int parse_select_field(parser::SelectField* field);
    // method to parse SQL elements
    int parse_select_fields();

    void add_single_table_columns(const std::string& table_name, TableInfo* table_info);
    
    int parse_with();

    int parse_where();

    int parse_groupby();

    int _parse_having();

    int parse_orderby();

    int parse_limit();

    int subquery_rewrite();
    
    int minmax_remove();
  
    bool is_full_export();

    void get_slot_column_mapping();

    // create tuples for table scan
    void create_agg_tuple_desc();
    bool is_fullexport_condition();
    bool is_range_compare(ExprNode* expr);
    bool check_single_conjunct(ExprNode* conjunct);
    bool check_conjuncts(std::vector<ExprNode*>& conjuncts);
    bool is_pk_consistency(const std::vector<FieldInfo>& pk_fields_in_factory, const std::vector<int32_t>& select_pk_fields);
    void get_conjuncts_condition(std::vector<ExprNode*>& conjuncts);

    // for base subscribe
    int get_base_subscribe_scan_ref_slot();

    // for snapshot blacklist
    void add_snapshot_blacklist_to_where_filters();

    // non-prepare plan cache
    virtual int plan_cache_get() override;
    virtual int plan_cache_add() override;
    virtual bool enable_plan_cache() override {
        return _ctx != nullptr 
                    && !_ctx->is_base_subscribe /* 基准导出场景 */
                    && !_ctx->is_complex /* 不包含复杂查询 */
                    && !_ctx->is_full_export /* 全量导出场景 */
                    && _ctx->sub_query_plans.size() == 0 /* 不包含子查询或视图 */
                    && !_ctx->has_unable_cache_expr; /* 不包含不能缓存的函数 */
    }
    int replace_select_names();

    int check_multi_distinct(); 
    void check_multi_distinct_in_select(int& multi_distinct_cnt, 
                                    bool& multi_col_single_child,
                                    std::set<std::string>& name_set);
    void check_multi_distinct_in_having(int& multi_distinct_cnt, 
                                    bool& multi_col_single_child,
                                    std::set<std::string>& name_set);
    void check_multi_distinct_in_orderby(int& multi_distinct_cnt, 
                                    bool& multi_col_single_child,
                                    std::set<std::string>& name_set);
    void check_multi_distinct_in_node(const parser::ExprNode* item, 
                                    int& multi_distinct_cnt, 
                                    bool& multi_col_single_child,
                                    std::set<std::string>& name_set);

    // 将窗口相同的开窗函数合并到同一个WindowNode中
    int merge_window_node();
    int create_window_tuple_desc();

private:
    parser::SelectStmt*                 _select;

    std::vector<pb::SlotDescriptor>     _group_slots;

    std::vector<pb::Expr>   _where_filters;
    std::vector<pb::Expr>   _having_filters;
    std::vector<pb::Expr>   _group_exprs;

    pb::Expr                _limit_offset;
    pb::Expr                _limit_count;
    std::string             _table_name;

    // 非相关子查询重写后，使用WindowNode进行min/max操作
    std::vector<pb::WindowNode> _subquery_rewrite_window_nodes;
};
} //namespace baikal

