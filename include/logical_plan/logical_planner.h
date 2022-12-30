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

#include <unordered_set>
#include "schema_factory.h"
#include "proto/plan.pb.h"
#include "query_context.h"
#include "dml.h"

namespace baikaldb {

enum TermType {
    TERM_INVALID,
    TERM_FIELD,
    TERM_INTEGER,
    TERM_DOUBLE,
    TERM_STRING,
    TERM_BOOL,
    TERM_NULL
};

static const int MAX_SQL_EXP_SIZE = 100;

// Join Node is a binary tree, which must own both left & right children
struct JoinMemTmp {
    pb::JoinNode join_node;
    JoinMemTmp* left_node = nullptr;
    JoinMemTmp* right_node = nullptr;
    std::multiset<std::string> left_full_table_names; //db.table跟该join相关的所有table
    std::multiset<std::string> right_full_table_names;
    bool is_derived_table = false;
    std::set<std::int64_t> use_indexes;
    std::set<std::int64_t> force_indexes;
    std::set<std::int64_t> ignore_indexes;
    virtual ~JoinMemTmp() {
        delete left_node;
        delete right_node;
    }
};

struct ApplyMemTmp {
    pb::ApplyNode apply_node;
    JoinMemTmp* outer_node = nullptr;
    pb::Plan    inner_plan;
};

struct ScanTupleInfo {
    int32_t tuple_id = -1;
    int64_t table_id = -1;
    int32_t slot_cnt = 1;
    std::unordered_map<int32_t, pb::SlotDescriptor> field_slot_mapping;

    ScanTupleInfo() {}
};

struct CreateExprOptions {
    CreateExprOptions() {}
    bool use_alias = false;
    bool can_agg = false;
    bool is_select_field = false;
    bool is_values = false;
    bool max_one_row = false;
    bool is_not = false;
    bool partition_expr = false;
    int row_expr_size = 1;
    pb::CompareType compare_type = pb::CMP_NULL;
};

struct PlanTableContext {
    int32_t              tuple_cnt = 0;
    int32_t              derived_table_id = -1;
    //table_name => db
    std::unordered_map<std::string, std::unordered_set<std::string>> table_dbs_mapping;
    //field_name=>db.table
    std::unordered_map<std::string, std::unordered_set<std::string>> field_tbls_mapping;
    //db => databaseinfo
    std::unordered_map<std::string, DatabaseInfo>  database_info;
    //db.table => tableinfo
    std::unordered_map<std::string, SmartTable>    table_info;
    //db.table.field => fieldinfo
    std::unordered_map<std::string, FieldInfo*>    field_info;
    //db.table => ScanTupleInfo
    std::unordered_map<std::string, ScanTupleInfo>  table_tuple_mapping;
};

typedef std::shared_ptr<PlanTableContext> SmartPlanTableCtx;

class LogicalPlanner {
public:
    LogicalPlanner(QueryContext* ctx) : _ctx(ctx) {
        _factory = SchemaFactory::get_instance();
        _plan_table_ctx.reset(new (std::nothrow)PlanTableContext);
    }

    LogicalPlanner(QueryContext* ctx, const SmartPlanTableCtx& plan_state) : _ctx(ctx), _plan_table_ctx(plan_state) {
        _factory = SchemaFactory::get_instance();
    }

    virtual ~LogicalPlanner() {
        delete _join_root;
        delete _apply_root;
    }

    virtual int plan() = 0;

    static int analyze(QueryContext* ctx);
    
    static std::map<parser::JoinType, pb::JoinType> join_type_mapping;
    std::vector<std::string>& select_names() {
        return _select_names;
    }
    std::vector<pb::Expr>& select_exprs() {
        return _select_exprs;
    }
    std::multimap<std::string, size_t>& select_alias_mapping() {
        return _select_alias_mapping;
    }

protected:
    int gen_subquery_plan(parser::DmlNode* subquery, const SmartPlanTableCtx& plan_state,
             const ExprParams& expr_params);
    // add table used in SQL to the context
    // and then do validation using schema info
    int add_table(const std::string& database, const std::string& table,
            const std::string& alias, const bool is_derived_table);

    int add_derived_table(const std::string& database, const std::string& table,
        const std::string& alias);

    std::unordered_set<std::string> get_possible_databases(const std::string& table) {
        if ( _plan_table_ctx->table_dbs_mapping.count(try_to_lower(table)) != 0) {
            return _plan_table_ctx->table_dbs_mapping[try_to_lower(table)];
        }
        return std::unordered_set<std::string>();
    }

    std::unordered_set<std::string> get_possible_tables(std::string field) {
        std::transform(field.begin(), field.end(), field.begin(), ::tolower);
        if ( _plan_table_ctx->field_tbls_mapping.count(field) != 0) {
            return _plan_table_ctx->field_tbls_mapping[field];
        }
        return std::unordered_set<std::string>();
    }

    TableInfo* get_table_info_ptr(const std::string& table) {
        auto iter = _plan_table_ctx->table_info.find(try_to_lower(table));
        if (iter != _plan_table_ctx->table_info.end()) {
            return iter->second.get();
        }
        return nullptr;
    }

    FieldInfo* get_field_info_ptr(const std::string& field) {
        auto iter = _plan_table_ctx->field_info.find(try_to_lower(field));
        if (iter != _plan_table_ctx->field_info.end()) {
            return iter->second;
        }
        return nullptr;
    }
   
    int parse_db_tables(const parser::TableName* table_name);
    int parse_db_tables(const parser::TableSource* table_source);
    int parse_db_tables(const parser::Node* table_refs, JoinMemTmp** join_root_ptr);
    
    int create_join_node_from_item_join(const parser::JoinNode* join_item,
                                        JoinMemTmp** join_root_ptr);
    int parse_using_cols(const parser::Vector<parser::ColumnName*>& using_col, 
                        JoinMemTmp* join_node_mem);
    int fill_join_table_infos(JoinMemTmp* join_node_mem);

    int create_join_node_from_table_name(const parser::TableName* table_name,
                                              JoinMemTmp** join_root_ptr);
    int create_join_node_from_table_source(const parser::TableSource* table_source,
                                              JoinMemTmp** join_root_ptr);
    
    int create_join_node_from_terminator(const std::string db, 
                                         const std::string table, 
                                         const std::string alias,
                                         const bool is_derived_table,
                                         const std::vector<std::string>& use_index_names,
                                         const std::vector<std::string>& force_index_names,
                                         const std::vector<std::string>& ignore_index_names, 
                                         JoinMemTmp** join_root_ptr); 

    int parse_db_name_from_table_name(const parser::TableName* table_name, 
                                      std::string& db, 
                                      std::string& table);
    int parse_db_name_from_table_source(const parser::TableSource* table_source, 
                                        std::string& db, 
                                        std::string& table, 
                                        std::string& alias,
                                        bool& is_derived_table);
    // return empty str if failed
    std::string get_field_alias_name(const parser::ColumnName* col);

    // make AND exprs to expr vector
    int flatten_filter(const parser::ExprNode* item, std::vector<pb::Expr>& filters,
        const CreateExprOptions& options);

    void create_order_func_slot();

    // @agg format: agg_func(col_name) / count_star()
    std::vector<pb::SlotDescriptor>& get_agg_func_slot(
            const std::string& agg, const std::string& fn_name, bool& new_slot);

    int create_agg_expr(const parser::FuncExpr* expr_item, pb::Expr& expr, const CreateExprOptions& options);

    // (col between A and B) ==> (col >= A) and (col <= B)
    int create_between_expr(const parser::FuncExpr* item, pb::Expr& expr, const CreateExprOptions& options);
    int create_values_expr(const parser::FuncExpr* item, pb::Expr& expr);

    // TODO in next stage: fill full func name
    // fill arg_types, return_type(col_type) and has_var_args
    int create_scala_func_expr(const parser::FuncExpr* item, pb::Expr& expr, parser::FuncType op, 
            const CreateExprOptions& options);

    int create_expr_tree(const parser::Node* item, pb::Expr& expr, const CreateExprOptions& options);

    int create_orderby_exprs(parser::OrderByClause* order);

    //TODO: error_code
    //TODO: ColumnType len
    int create_term_slot_ref_node(const parser::ColumnName* term, pb::Expr& expr, const CreateExprOptions& options);

    int create_alias_node(const parser::ColumnName* term, pb::Expr& expr);

    //TODO: primitive len for STRING, BOOL and NULL
    int create_term_literal_node(const parser::LiteralExpr* term, pb::Expr& expr);
    // (a,b)
    int create_row_expr_node(const parser::RowExpr* term, pb::Expr& expr, const CreateExprOptions& options);

    void create_scan_tuple_descs();
    void create_values_tuple_desc(); 
    void create_order_by_tuple_desc();

    ScanTupleInfo* get_scan_tuple(const std::string& table_name, int64_t table_id);

    // get or create a new SlotDescriptor with the given field name
    // used for create slot_ref
    pb::SlotDescriptor& get_scan_ref_slot(const std::string& alias_name, 
            int64_t table, int32_t field, pb::PrimitiveType type);
    pb::SlotDescriptor& get_values_ref_slot(int64_t table, int32_t field, pb::PrimitiveType type);

    // create common plan nodes 
    int create_packet_node(pb::OpType op_type);
    int create_filter_node(std::vector<pb::Expr>& filters, pb::PlanNodeType type);
    int create_sort_node();
    int create_scan_nodes();
    int create_join_and_scan_nodes(JoinMemTmp* join_root, ApplyMemTmp* apply_root);


    void set_dml_txn_state(int64_t table_id);
    void plan_begin_txn();
    void plan_commit_txn();
    void plan_rollback_txn();
    void plan_commit_and_begin_txn();
    void plan_rollback_and_begin_txn();

    int generate_sql_sign(QueryContext* ctx, parser::StmtNode* stmt);

private:
    int create_n_ary_predicate(const parser::FuncExpr* item, 
            pb::Expr& expr,
            pb::ExprNodeType type,
            const CreateExprOptions& options);
    int create_in_predicate(const parser::FuncExpr* func_item, 
            pb::Expr& expr,
            const CreateExprOptions& options);
    int exec_subquery_expr(QueryContext* sub_ctx, QueryContext* ctx);
    int create_common_subquery_expr(const parser::SubqueryExpr* item, pb::Expr& expr,
            const CreateExprOptions& options, bool& is_correlate);
    int handle_in_subquery(const parser::FuncExpr* func_item,
            pb::Expr& expr,
            const CreateExprOptions& options);
    int handle_scalar_subquery(const parser::FuncExpr* func_item,
            pb::Expr& expr,
            const CreateExprOptions& options);
    int handle_common_subquery(const parser::ExprNode* expr_item,
            pb::Expr& expr,
            const CreateExprOptions& options);
    int construct_apply_node(QueryContext* ctx,
        pb::Expr& expr,
        const pb::JoinType join_type,
        const CreateExprOptions& options);
    int handle_compare_subquery(const parser::ExprNode* item, pb::Expr& expr,
            const CreateExprOptions& options);
    int handle_exists_subquery(const parser::ExprNode* item, pb::Expr& expr,
            const CreateExprOptions& options);
    void construct_literal_expr(const ExprValue& value, pb::ExprNode* node);
    int construct_in_predicate_node(const parser::FuncExpr* func_item, pb::Expr& expr, pb::ExprNode** node);
protected:
    QueryContext*       _ctx = nullptr;
    std::shared_ptr<QueryContext> _cur_sub_ctx = nullptr;
    SmartPlanTableCtx      _plan_table_ctx;
    SchemaFactory*      _factory = nullptr;

    std::vector<pb::TupleDescriptor>  _scan_tuples;
    pb::TupleDescriptor _values_tuple;  // INSERT ... ON DUPLICATE KEY UPDATE values() tuple
    ScanTupleInfo _values_tuple_info;

    int32_t                 _agg_tuple_id = -1;
    int32_t                 _agg_slot_cnt = 1;
    std::vector<pb::Expr>   _agg_funcs;
    std::vector<pb::Expr>   _distinct_agg_funcs;
    std::vector<pb::Expr>   _orderby_agg_exprs;
    std::unordered_map<std::string, std::vector<pb::SlotDescriptor>> _agg_slot_mapping;

    // table names, the order in From clause is preserved
    std::set<std::string> _table_names;

    // table_alias => db.table
    //std::unordered_map<std::string, std::string>  _table_alias_mapping;

    // alias => index in _select_exprs (or _select_names)
    std::multimap<std::string, size_t> _select_alias_mapping;
    std::vector<pb::Expr>       _select_exprs;
    std::vector<std::string>    _select_names;

    int32_t                     _order_tuple_id = -1;
    int32_t                     _order_slot_cnt = 1;
    std::vector<pb::SlotDescriptor> _order_slots;
    std::vector<pb::Expr>       _order_exprs;
    std::vector<bool>           _order_ascs;

    JoinMemTmp*                 _join_root = nullptr;
    ApplyMemTmp*                _apply_root = nullptr;
    bool                        _is_correlate_subquery_expr = false;
    int32_t                     _column_id = 0;
    // 同一层级的操作表集合，e.g:join的左右表
    std::vector<std::string>       _current_tables;
    std::vector<std::string>       _partition_names;
};
} //namespace baikal
