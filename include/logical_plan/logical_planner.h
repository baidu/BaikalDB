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
    std::set<std::int64_t> use_indexes;
    std::set<std::int64_t> ignore_indexes;
    virtual ~JoinMemTmp() {
        delete left_node;
        delete right_node;
    }
};

struct ScanTupleInfo {
    int32_t tuple_id = -1;
    int64_t table_id = -1;
    int32_t slot_cnt = 1;
    std::unordered_map<int32_t, pb::SlotDescriptor> field_slot_mapping;

    ScanTupleInfo() {}
};

class LogicalPlanner {
public:
    LogicalPlanner(QueryContext* ctx) : _tuple_cnt(0), _ctx(ctx) {
        _factory = SchemaFactory::get_instance();
    }

    virtual ~LogicalPlanner() {
        delete _join_root;
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
    // add table used in SQL to the context
    // and then do validation using schema info
    int add_table(const std::string& database, const std::string& table,
            const std::string& alias);

    std::unordered_set<std::string> get_possible_databases(const std::string& table) {
        if (_table_dbs_mapping.count(table) != 0) {
            return _table_dbs_mapping[table];
        }
        return std::unordered_set<std::string>();
    }

    std::unordered_set<std::string> get_possible_tables(std::string field) {
        std::transform(field.begin(), field.end(), field.begin(), ::tolower);
        if (_field_tbls_mapping.count(field) != 0) {
            return _field_tbls_mapping[field];
        }
        return std::unordered_set<std::string>();
    }

    TableInfo* get_table_info_ptr(const std::string& table) {
        auto iter = _table_info.find(table);
        if (iter != _table_info.end()) {
            return iter->second.get();
        }
        return nullptr;
    }

    FieldInfo* get_field_info_ptr(const std::string& field) {
        auto iter = _field_info.find(field);
        if (iter != _field_info.end()) {
            return iter->second;
        }
        return nullptr;
    }

    // int64_t get_index_id(const std::string& database) {
    //     auto iter = _index_id_mapping.find(database);
    //     if (iter != _index_id_mapping.end()) {
    //         return iter->second;
    //     }
    //     return -1;
    // }
   
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
                                         const std::vector<std::string>& use_index_names,
                                         const std::vector<std::string>& ignore_index_names, 
                                         JoinMemTmp** join_root_ptr); 

    int parse_db_name_from_table_name(const parser::TableName* table_name, 
                                      std::string& db, 
                                      std::string& table);
    int parse_db_name_from_table_source(const parser::TableSource* table_source, 
                                        std::string& db, 
                                        std::string& table, 
                                        std::string& alias);
    // return empty str if failed
    std::string get_field_full_name(const parser::ColumnName* col);

    // make AND exprs to expr vector
    int flatten_filter(const parser::ExprNode* item, std::vector<pb::Expr>& filters, bool use_alias);

    void create_order_func_slot();

    // @agg format: agg_func(col_name) / count_star()
    std::vector<pb::SlotDescriptor>& get_agg_func_slot(
            const std::string& agg, const std::string& fn_name, bool& new_slot);

    int create_agg_expr(const parser::FuncExpr* expr_item, pb::Expr& expr, bool use_alias);

    // (col between A and B) ==> (col >= A) and (col <= B)
    int create_between_expr(const parser::FuncExpr* item, pb::Expr& expr, bool use_alias);
    int create_values_expr(const parser::FuncExpr* item, pb::Expr& expr);

    // TODO in next stage: fill full func name
    // fill arg_types, return_type(col_type) and has_var_args
    int create_scala_func_expr(const parser::FuncExpr* item, pb::Expr& expr, parser::FuncType op, bool use_alias);

    int create_expr_tree(const parser::Node* item, pb::Expr& expr, bool use_alias);

    int create_orderby_exprs(parser::OrderByClause* order);

    //TODO: error_code
    //TODO: ColumnType len
    int create_term_slot_ref_node(const parser::ColumnName* term, pb::Expr& expr, bool is_values = false);

    int create_alias_node(const parser::ColumnName* term, pb::Expr& expr);

    //TODO: primitive len for STRING, BOOL and NULL
    int create_term_literal_node(const parser::LiteralExpr* term, pb::Expr& expr);
    // (a,b)
    int create_row_expr_node(const parser::RowExpr* term, pb::Expr& expr, bool use_alias);

    void create_scan_tuple_descs();
    void create_values_tuple_desc(); 
    void create_order_by_tuple_desc();

    ScanTupleInfo* get_scan_tuple(int64_t table);

    // get or create a new SlotDescriptor with the given field name
    // used for create slot_ref
    pb::SlotDescriptor& get_scan_ref_slot(int64_t table, int32_t field, pb::PrimitiveType type);
    pb::SlotDescriptor& get_values_ref_slot(int64_t table, int32_t field, pb::PrimitiveType type);

    // create common plan nodes 
    int create_packet_node(pb::OpType op_type);
    int create_filter_node(std::vector<pb::Expr>& filters, pb::PlanNodeType type);
    int create_sort_node();
    int create_scan_nodes();
    int create_join_and_scan_nodes(baikaldb::JoinMemTmp* join_root);


    void set_dml_txn_state(int64_t table_id);
    uint64_t get_txn_id();
    void plan_begin_txn();
    void plan_commit_txn();
    void plan_rollback_txn();
    void plan_commit_and_begin_txn();
    void plan_rollback_and_begin_txn();

private:
    int create_n_ary_predicate(const parser::FuncExpr* item, 
            pb::Expr& expr,
            pb::ExprNodeType type,
            bool use_alias);
    int create_in_predicate(const parser::FuncExpr* item, 
            pb::Expr& expr,
            pb::ExprNodeType type,
            bool use_alias);

    static std::atomic<uint64_t> _txn_id_counter;

protected:
    int32_t             _tuple_cnt;
    QueryContext*       _ctx;
    SchemaFactory*      _factory;

    std::vector<pb::TupleDescriptor>  _scan_tuples;
    pb::TupleDescriptor _values_tuple;  // INSERT ... ON DUPLICATE KEY UPDATE values() tuple
    ScanTupleInfo _values_tuple_info;

    //table_name => db
    std::unordered_map<std::string, std::unordered_set<std::string>> _table_dbs_mapping;
    //field_name=>db.table
    std::unordered_map<std::string, std::unordered_set<std::string>> _field_tbls_mapping;
    
    std::unordered_map<int64_t, ScanTupleInfo>  _table_tuple_mapping;

    int32_t                 _agg_tuple_id = -1;
    int32_t                 _agg_slot_cnt = 1;
    std::vector<pb::Expr>   _agg_funcs;
    std::vector<pb::Expr>   _distinct_agg_funcs;
    std::unordered_map<std::string, std::vector<pb::SlotDescriptor>> _agg_slot_mapping;

    //db => databaseinfo
    std::unordered_map<std::string, DatabaseInfo>  _database_info;
    //db.table => tableinfo
    std::unordered_map<std::string, SmartTable>    _table_info;

    // table names, the order in From clause is preserved
    std::vector<std::string> _table_names;

    //db.table.field => fieldinfo
    std::unordered_map<std::string, FieldInfo*>    _field_info;

    // table_alias => db.table
    std::unordered_map<std::string, std::string>  _table_alias_mapping;
    //std::unordered_map<std::string, int64_t>    _index_id_mapping;

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
};
} //namespace baikal

