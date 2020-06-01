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

#include "logical_planner.h"
#include <memory>
#include <algorithm>
#include <iterator>
#include <boost/algorithm/string.hpp>
#include "common.h"
#include "select_planner.h"
#include "insert_planner.h"
#include "delete_planner.h"
#include "update_planner.h"
#include "union_planner.h"
#include "ddl_planner.h"
#include "setkv_planner.h"
#include "transaction_planner.h"
#include "kill_planner.h"
#include "prepare_planner.h"
#include "predicate.h"
#include "network_socket.h"
#include "parser.h"
#include "mysql_err_code.h"

namespace bthread {
DECLARE_int32(bthread_concurrency); //bthread.cpp
}

namespace baikaldb {
DECLARE_string(log_plat_name);

std::atomic<uint64_t> LogicalPlanner::_txn_id_counter(1);

std::map<parser::JoinType, pb::JoinType> LogicalPlanner::join_type_mapping {
        { parser::JT_NONE, pb::NULL_JOIN},    
        { parser::JT_INNER_JOIN, pb::INNER_JOIN},
        { parser::JT_LEFT_JOIN, pb::LEFT_JOIN},
        { parser::JT_RIGHT_JOIN, pb::RIGHT_JOIN}
    };
// TODO: OP_DIV2

int LogicalPlanner::create_n_ary_predicate(const parser::FuncExpr* func_item, 
        pb::Expr& expr,
        pb::ExprNodeType type,
        bool use_alias) {

    pb::ExprNode* node = expr.add_nodes();
    node->set_col_type(pb::BOOL);
    //split NOT_NULL/NOT_IN/NOT_LIKE into two predicates
    if (func_item->is_not) {
        node->set_node_type(pb::NOT_PREDICATE);
        node->set_num_children(1);
        pb::Function* item = node->mutable_fn();
        item->set_name("logic_not");
        item->set_fn_op(parser::FT_LOGIC_NOT);

        //add real predicate (OP_IS_NULL/OP_IN/OP_LIKE) node
        node = expr.add_nodes();
        node->set_col_type(pb::BOOL);
    }
    node->set_node_type(type);
    pb::Function* func = node->mutable_fn();
    if (func_item->fn_name.empty()) {
        DB_WARNING("op:%d fn_name is empty", func_item->func_type);
        return -1;
    }
    func->set_name(func_item->fn_name.c_str());
    func->set_fn_op(func_item->func_type);

    for (int32_t idx = 0; idx < func_item->children.size(); ++idx) {
        if (0 != create_expr_tree(func_item->children[idx], expr, use_alias)) {
            DB_WARNING("create child expr failed");
            return -1;
        }
    }
    node->set_num_children(func_item->children.size());
    return 0;
}

int LogicalPlanner::create_in_predicate(const parser::FuncExpr* func_item, 
        pb::Expr& expr,
        pb::ExprNodeType type, 
        bool use_alias) {

    pb::ExprNode* node = expr.add_nodes();
    node->set_col_type(pb::BOOL);
    //split NOT_NULL/NOT_IN/NOT_LIKE into two predicates
    if (func_item->is_not) {
        node->set_node_type(pb::NOT_PREDICATE);
        node->set_num_children(1);
        pb::Function* item = node->mutable_fn();
        item->set_name("logic_not");
        item->set_fn_op(parser::FT_LOGIC_NOT);

        //add real predicate (OP_IS_NULL/OP_IN/OP_LIKE) node
        node = expr.add_nodes();
        node->set_col_type(pb::BOOL);
    }
    node->set_node_type(type);
    pb::Function* func = node->mutable_fn();
    if (func_item->fn_name.empty()) {
        DB_WARNING("op:%d fn_name is empty", func_item->func_type);
        return -1;
    }
    func->set_name(func_item->fn_name.value);
    func->set_fn_op(func_item->func_type);
    parser::ExprNode* arg1 = (parser::ExprNode*)func_item->children[0];
    if (arg1->expr_type == parser::ET_ROW_EXPR) {
        DB_WARNING("un-support RowExpr in, [%s]", func_item->to_string().c_str());
        //return -1;
    }
    
    if (0 != create_expr_tree(arg1, expr, use_alias)) {
        DB_WARNING("create child 1 expr failed");
        return -1;
    }
    parser::ExprNode* arg2 = (parser::ExprNode*)func_item->children[1];
    for (int i = 0; i < arg2->children.size(); i++) {
        if (0 != create_expr_tree(arg2->children[i], expr, use_alias)) {
            DB_WARNING("create child expr failed");
            return -1;
        }
    }
    node->set_num_children(arg2->children.size() + 1);
    return 0;
}

// TODO move to a caller
// static, planner entrance 
int LogicalPlanner::analyze(QueryContext* ctx) {
    if (!ctx) {
        return -1;
    }
    TimeCost cost;
    if (ctx->mysql_cmd == COM_STMT_PREPARE 
            || ctx->mysql_cmd == COM_STMT_EXECUTE 
            || ctx->mysql_cmd == COM_STMT_CLOSE) {

        if (ctx->mysql_cmd == COM_STMT_PREPARE) {
            ctx->stmt_type = parser::NT_NEW_PREPARE;
        } else if (ctx->mysql_cmd == COM_STMT_EXECUTE) {
            ctx->stmt_type = parser::NT_EXEC_PREPARE;
        } else if (ctx->mysql_cmd == COM_STMT_CLOSE) {
            ctx->stmt_type = parser::NT_DEALLOC_PREPARE;
        }
        std::unique_ptr<LogicalPlanner> planner;
        planner.reset(new PreparePlanner(ctx));
        if (planner->plan() != 0) {
            DB_WARNING("gen stmt plan failed, mysql_type:%d, stmt_type:%d", ctx->mysql_cmd, ctx->stmt_type);
            return -1;
        }
        return 0;
    }
    parser::SqlParser parser;
    parser.charset = ctx->charset;
    parser.parse(ctx->sql);
    if (parser.error != parser::SUCC) {
        ctx->stat_info.error_code = ER_SYNTAX_ERROR;
        ctx->stat_info.error_msg << "syntax error! errno: " << parser.error
                                 << " errmsg: " << parser.syntax_err_str;
        DB_WARNING("parsing error! errno: %d, errmsg: %s, sql: %s", 
            parser.error, 
            parser.syntax_err_str.c_str(),
            ctx->sql.c_str());
        return -1;
    }
    if (parser.result.size() != 1) {
        DB_WARNING("multi-stmt is not supported, sql: %s", ctx->sql.c_str());
        return -1;
    }
    if (parser.result[0] == nullptr) {
        DB_WARNING("sql parser stmt is null, sql: %s", ctx->sql.c_str());
        return -1;
    }
    if (parser.result[0]->node_type == parser::NT_EXPLAIN) {
        parser::ExplainStmt* stmt = static_cast<parser::ExplainStmt*>(parser.result[0]);
        ctx->stmt = stmt->stmt;
        std::string format(stmt->format.c_str());
        if (format == "trace") {
            ctx->explain_type = SHOW_TRACE;
        } else if (format == "trace2") {
            ctx->explain_type = SHOW_TRACE2;
        } else if (format == "plan") {
            ctx->explain_type = SHOW_PLAN;
        } else if (format == "analyze") { 
            ctx->explain_type = ANALYZE_STATISTICS;
        } else if (format == "histogram") {
            ctx->explain_type = SHOW_HISTOGRAM;
            ctx->is_explain = true;
        } else if (format == "cmsketch") {
            ctx->explain_type = SHOW_CMSKETCH;
            ctx->is_explain = true;
        } else {
            ctx->is_explain = true;
        }
        //DB_WARNING("stmt format:%s", format.c_str());
    } else {
        ctx->stmt = parser.result[0];
    }
    ctx->stmt_type = ctx->stmt->node_type;
    if (ctx->stmt_type != parser::NT_SELECT && (ctx->is_explain || explain_is_trace(ctx->explain_type))) {
        DB_WARNING("not support explain except select");
        return -1;
    }

    std::unique_ptr<LogicalPlanner> planner;
    switch (ctx->stmt_type) {
    case parser::NT_SELECT:
        planner.reset(new SelectPlanner(ctx));
        break;
    case parser::NT_INSERT:
        planner.reset(new InsertPlanner(ctx));
        break;
    case parser::NT_UPDATE:
        planner.reset(new UpdatePlanner(ctx));
        break;
    case parser::NT_DELETE:
    case parser::NT_TRUNCATE:
        planner.reset(new DeletePlanner(ctx));
        break;
    case parser::NT_CREATE_TABLE:
    case parser::NT_CREATE_DATABASE:
    case parser::NT_DROP_TABLE:
    case parser::NT_RESTORE_TABLE:
    case parser::NT_DROP_DATABASE:
    case parser::NT_ALTER_TABLE:
        planner.reset(new DDLPlanner(ctx));
        ctx->succ_after_logical_plan = true;
        break;
    case parser::NT_SET_CMD:
        planner.reset(new SetKVPlanner(ctx));
        break;
    case parser::NT_START_TRANSACTION:
    case parser::NT_COMMIT_TRANSACTION:
    case parser::NT_ROLLBACK_TRANSACTION:
        planner.reset(new TransactionPlanner(ctx));
        break;
    case parser::NT_NEW_PREPARE:
    case parser::NT_EXEC_PREPARE:
    case parser::NT_DEALLOC_PREPARE:
        planner.reset(new PreparePlanner(ctx));
        break;
    case parser::NT_KILL:
        planner.reset(new KillPlanner(ctx));
        break;
    case parser::NT_UNION:
        planner.reset(new UnionPlanner(ctx));
        break;
    default:
        DB_WARNING("invalid command type: %d", ctx->stmt_type);
        return -1;
    }
    if (planner->plan() != 0) {
        DB_WARNING("gen plan failed, type:%d", ctx->stmt_type);
        return -1;
    }
    ctx->stmt->set_print_sample(true);
    auto stat_info = &(ctx->stat_info);
    pb::OpType op_type = pb::OP_NONE;
    if (ctx->plan.nodes_size() > 0 && ctx->plan.nodes(0).node_type() == pb::PACKET_NODE) {
        op_type = ctx->plan.nodes(0).derive_node().packet_node().op_type();
    }

    if (!stat_info->family.empty() && !stat_info->table.empty() ) {
        std::string resource_tag;
        auto table_ptr = planner->get_table_info_ptr(stat_info->family + "." + stat_info->table);
        if (table_ptr == nullptr) {
            resource_tag = "unknown";
        } else {
            resource_tag = table_ptr->resource_tag;
        }
        stat_info->table_id = table_ptr->id;
        stat_info->sample_sql << "family_table_tag_optype_plat=[" << stat_info->family << "\t"
            << stat_info->table << "\t" << resource_tag << "\t" << op_type << "\t"
            << FLAGS_log_plat_name << "] sql=[" << ctx->stmt << "]";
    }
    return 0;
}

//TODO, add table alias
int LogicalPlanner::add_table(const std::string& database, const std::string& table,
        const std::string& alias) {
    std::string _namespace = _ctx->user_info->namespace_;
    std::string _username =  _ctx->user_info->username;
    
    DatabaseInfo db;
    if (_database_info.count(database) == 0) {
        int64_t databaseid = -1;
        if (0 != _factory->get_database_id(_namespace + "." + database, databaseid)) {
            DB_WARNING("unknown _namespace:%s database: %s", _namespace.c_str(), database.c_str());
            _ctx->stat_info.error_code = ER_BAD_DB_ERROR;
            _ctx->stat_info.error_msg << "database " << database << " not exist";
            return -1;
        }
        db = _factory->get_database_info(databaseid);
        if (db.id == -1) {
            DB_WARNING("no database found with id: %ld", databaseid);
            _ctx->stat_info.error_code = ER_BAD_DB_ERROR;
            _ctx->stat_info.error_msg << "database " << database << " not exist";
            return -1;
        }
        _database_info.insert(std::make_pair(database, db));
    } else {
        db = _database_info[database];
    }

    if (_table_info.count(database + "." + table) == 0) {
        int64_t tableid = -1;
        if (0 != _factory->get_table_id(_namespace + "." + database + "." + table, tableid)) {
            DB_WARNING("unknown table: %s.%s", database.c_str(), table.c_str());
            _ctx->stat_info.error_code = ER_NO_SUCH_TABLE;
            _ctx->stat_info.error_msg << "table: " << database << "." << table << " not exist";
            return -1;
        }
        auto tbl_ptr = _factory->get_table_info_ptr(tableid);
        if (tbl_ptr == nullptr) {
            DB_WARNING("no table found with id: %ld", tableid);
            _ctx->stat_info.error_code = ER_NO_SUCH_TABLE;
            _ctx->stat_info.error_msg << "table: " << database << "." << table << " not exist";
            return -1;
        }
        auto& tbl = *tbl_ptr;
        // validate user permission
        pb::OpType op_type = pb::OP_INSERT;
        if (_ctx->stmt_type == parser::NT_SELECT) {
            op_type = pb::OP_SELECT;
        }
        if (!_ctx->user_info->allow_op(op_type, db.id, tbl.id)) {
            DB_WARNING("user %s has no permission to access: %s.%s, db.id:%ld, tbl.id:%ld", 
                _username.c_str(), database.c_str(), table.c_str(), db.id, tbl.id);
            _ctx->stat_info.error_code = ER_TABLEACCESS_DENIED_ERROR;
            _ctx->stat_info.error_msg << "user " << _username 
                                      << " has no permission to access " 
                                      << database << "." << table;
            return -1;
        }
        get_scan_tuple(tbl.id);

        for (uint32_t idx = 0; idx < tbl.fields.size(); ++idx) {
            std::string& field_name = tbl.fields[idx].name;
            std::vector<std::string> items;
            boost::split(items, field_name, boost::is_any_of("."));
            std::string col_name = items[items.size() - 1];
            std::transform(col_name.begin(), col_name.end(), col_name.begin(), ::tolower);
            _field_tbls_mapping[col_name].insert(tbl.name);
            std::string new_field_name;
            for (size_t idx = 0; idx < items.size() - 1; ++idx) {
               new_field_name.append(items[idx] + "."); 
            }
            new_field_name.append(col_name);
            _field_info.insert(std::make_pair(new_field_name, &tbl.fields[idx]));
        }

        _table_info.insert(std::make_pair(database + "." + table, tbl_ptr));
        _table_names.push_back(database + "." + table);
        if (_table_dbs_mapping[table].count(database) == 0) {
            _table_dbs_mapping[table].insert(database);
        }
        std::string table_alias;
        if (!alias.empty()) {
            table_alias = alias;
        } else {
            table_alias = database + "." + table;
        }
        _table_alias_mapping.insert(std::make_pair(table_alias, database + "." + table));
    }
    return 0;
}

int LogicalPlanner::parse_db_tables(const parser::TableName* table_name) {
    std::string database;
    std::string table;
    std::string alias;
    if (parse_db_name_from_table_name(table_name, database, table) < 0) {
        DB_WARNING("parse db name from table name fail");
        return -1;
    }
    if (0 != add_table(database, table, alias)) {
        DB_WARNING("invalid database or table:%s.%s", database.c_str(), table.c_str());
        return -1;
    }
    return 0;
}

int LogicalPlanner::parse_db_tables(const parser::TableSource* table_source) {
    std::string database;
    std::string table;
    std::string alias;
    if (parse_db_name_from_table_source(table_source, database, table, alias) < 0) {
        DB_WARNING("parse db name from table name fail");
        return -1;
    }
    if (0 != add_table(database, table, alias)) {
        DB_WARNING("invalid database or table:%s.%s", database.c_str(), table.c_str());
        return -1;
    }
    return 0;
}

int LogicalPlanner::parse_db_tables(const parser::Node* table_refs, JoinMemTmp** join_root_ptr) {
    if (table_refs == nullptr) {
        DB_WARNING("table item is null");
        return -1;
    }
    if (table_refs->node_type == parser::NT_TABLE) {
        //DB_WARNING("tbls item is table name");
        if (0 != create_join_node_from_table_name((parser::TableName*)table_refs, join_root_ptr)) {
            DB_WARNING("parse_db_table_item failed from linked list");
            return -1;
        }
    } else if (table_refs->node_type == parser::NT_TABLE_SOURCE) {
        // DB_WARNING("tbls item is table source");
        if (0 != create_join_node_from_table_source((parser::TableSource*)table_refs, join_root_ptr)) {
            DB_WARNING("parse_db_table_item failed from linked list");
            return -1;
        }
    } else if (table_refs->node_type == parser::NT_JOIN) {
        //DB_WARNING("tbls item is item join");
        parser::JoinNode* join_item = (parser::JoinNode*)(table_refs);
        if (0 != create_join_node_from_item_join(join_item, join_root_ptr)) {
            DB_WARNING("parse_db_table_item failed from item join");
            return -1;
        }     
    } else {
        DB_WARNING("unsupport table type");
        return -1;
    }
    return 0;
}

//一个join中解析table, 直接生成join节点
int LogicalPlanner::create_join_node_from_item_join(const parser::JoinNode* join_item, 
                                                    JoinMemTmp** join_root_ptr) {
    std::unique_ptr<JoinMemTmp> join_node_mem(new JoinMemTmp);
    join_node_mem->join_node.set_join_type(join_type_mapping[join_item->join_type]); 
    
    if (join_item->left  == nullptr) {
        DB_WARNING("joint node must have left node");
        return -1;
    }
    if (0 != parse_db_tables(join_item->left, &(join_node_mem->left_node))) {
        DB_WARNING("parse left node fail");
        return -1;
    }

    if (join_item->right == nullptr) {
        DB_WARNING("joint node must have right node");
        return -1;
    }
    if (0 != parse_db_tables(join_item->right, &(join_node_mem->right_node))) {
        DB_WARNING("parse right node fail");
        return -1;
    }
    auto ret = fill_join_table_infos(join_node_mem.get());    

    if (ret < 0) {
        DB_WARNING("fill join table info fail");
        return ret;
    }
    parser::ExprNode* condition = join_item->expr;
    //暂时不支持没有条件的join 
    /*
    if (condition == nullptr && join_item->using_col.size() == 0) {
        DB_WARNING("has no join condition");
        return -1;
    }
    */
    if (condition != nullptr) {
        std::vector<pb::Expr> join_filters;
        if (0 != flatten_filter(condition, join_filters, false)) {
            DB_WARNING("flatten_filter failed");
            return -1;
        }
        for (uint32_t idx = 0; idx < join_filters.size(); ++idx) {
            pb::Expr* expr = join_node_mem->join_node.add_conditions();
            expr->CopyFrom(join_filters[idx]);
        }
    }
    ret = parse_using_cols(join_item->using_col, join_node_mem.get());
    if (ret < 0) {
        DB_WARNING("_parse using cols fail");
        return ret;
    }
    *join_root_ptr = join_node_mem.release();
    return 0;
}

int LogicalPlanner::parse_using_cols(const parser::Vector<parser::ColumnName*>& using_col, JoinMemTmp* join_node_mem) {
    //解析using cols
    /*
    for (int i = 0; i < using_col.size(); ++i) {
        std::string field_name = using_col[i]->name.value;
        std::unordered_set<std::string> possible_table_names = get_possible_tables(field_name);
        if (possible_table_names.size() < 2) {
            DB_WARNING("using filed not in more than 2 table");
            return -1;
        }
        int32_t related_table_count = 0;
        std::string left_full_field;
        std::string right_full_field;
        for (auto& table_name : possible_table_names) {
            if (join_node_mem->left_full_table_names.count(table_name) != 0) {
                ++related_table_count;
                left_full_field = table_name + "." + field_name;
            }
        }
        if (related_table_count != 1) {
            DB_WARNING("related table count not equal to 2");
            return -1;
        }

        related_table_count = 0;
        for (auto& table_name : possible_table_names) {
            if (join_node_mem->right_full_table_names.count(table_name) != 0) {
                ++related_table_count;
                right_full_field = table_name + "." + field_name;
            }
        }
        if (related_table_count != 1) {
            DB_WARNING("related table count not equal to 2");
            return -1;
        }

        pb::Expr expr;
        pb::ExprNode* node = expr.add_nodes();
        node->set_node_type(pb::FUNCTION_CALL);
        node->set_col_type(pb::INVALID_TYPE);
        pb::Function* func = node->mutable_fn();
        func->set_fn_op(OP_EQ);
        func->set_name(scala_func_mapping[OP_EQ]);
        node->set_num_children(2);
        if (0 != create_term_slot_ref_node(left_full_field.c_str(), expr, false)) {
            DB_WARNING("left_full_field using field create term slot fail");
            return -1;
        }
        if (0 != create_term_slot_ref_node(right_full_field.c_str(), expr, false)) {
            DB_WARNING("left_full_field using field create term slot fail");
            return -1;
        }
        pb::Expr* join_expr = join_node_mem->join_node.add_conditions();
        join_expr->CopyFrom(expr);
    }*/
    return 0;
}
int LogicalPlanner::fill_join_table_infos(JoinMemTmp* join_node_mem) {
    for (auto& table_name : join_node_mem->left_node->left_full_table_names) {
        int64_t table_id = _table_info[table_name]->id;
        int32_t tuple_id = _table_tuple_mapping[table_id].tuple_id;
        if (join_node_mem->left_full_table_names.count(table_name) != 0) {
            DB_WARNING("not support self join");
            return -1;
        }
        join_node_mem->join_node.add_left_tuple_ids(tuple_id);
        join_node_mem->join_node.add_left_table_ids(table_id);
        join_node_mem->left_full_table_names.insert(table_name);
    } 
    for (auto& table_name : join_node_mem->left_node->right_full_table_names) {
        int64_t table_id = _table_info[table_name]->id;
        int32_t tuple_id = _table_tuple_mapping[table_id].tuple_id;
        if (join_node_mem->left_full_table_names.count(table_name) != 0) {
            DB_WARNING("not support self join");
            return -1;
        }
        join_node_mem->join_node.add_left_tuple_ids(tuple_id);
        join_node_mem->join_node.add_left_table_ids(table_id);
        join_node_mem->left_full_table_names.insert(table_name);
    }

    for (auto& table_name : join_node_mem->right_node->left_full_table_names) {
        int64_t table_id = _table_info[table_name]->id;
        int32_t tuple_id = _table_tuple_mapping[table_id].tuple_id;
        if (join_node_mem->right_full_table_names.count(table_name) != 0) {
            DB_WARNING("not support self join");
            return -1;
        }
        join_node_mem->join_node.add_right_tuple_ids(tuple_id);
        join_node_mem->join_node.add_right_table_ids(table_id);
        join_node_mem->right_full_table_names.insert(table_name);
    }
    for (auto& table_name : join_node_mem->right_node->right_full_table_names) {
        int64_t table_id = _table_info[table_name]->id;
        int32_t tuple_id = _table_tuple_mapping[table_id].tuple_id;
        if (join_node_mem->right_full_table_names.count(table_name) != 0) {
            DB_WARNING("not support self join");
            return -1;
        }
        join_node_mem->join_node.add_right_tuple_ids(tuple_id);
        join_node_mem->join_node.add_right_table_ids(table_id);
        join_node_mem->right_full_table_names.insert(table_name);
    }
    return 0;
}

//join节点的叶子节点
int LogicalPlanner::create_join_node_from_table_name(const parser::TableName* table_name, JoinMemTmp** join_root_ptr) {
    std::string db;
    std::string table;
    if (parse_db_name_from_table_name(table_name, db, table) < 0) {
        DB_WARNING("parser db name from table name fail");
        return -1;
    }
    return create_join_node_from_terminator(db, table, 
            std::string(), 
            std::vector<std::string>{}, 
            std::vector<std::string>{}, 
            join_root_ptr); 
}

int LogicalPlanner::create_join_node_from_table_source(const parser::TableSource* table_source, JoinMemTmp** join_root_ptr) {
    std::string db;
    std::string table;
    std::string alias;
    if (parse_db_name_from_table_source(table_source, db, table, alias) < 0) {
        DB_WARNING("parser db name from table name fail");
        return -1;
    }
    std::vector<std::string> use_index_names;
    std::vector<std::string> ignore_index_names;
    for (int i = 0; i < table_source->index_hints.size(); ++i) {
        if (table_source->index_hints[i]->hint_type == parser::IHT_HINT_USE
                || table_source->index_hints[i]->hint_type == parser::IHT_HINT_FORCE) {
            for (int j = 0; j < table_source->index_hints[i]->index_name_list.size(); ++j) {
                use_index_names.push_back(table_source->index_hints[i]->index_name_list[j].value);
            }
        }
        if (table_source->index_hints[i]->hint_type == parser::IHT_HINT_IGNORE) {
            for (int j = 0; j < table_source->index_hints[i]->index_name_list.size(); ++j) {
                ignore_index_names.push_back(table_source->index_hints[i]->index_name_list[j].value);
            }
        }
    }
    return create_join_node_from_terminator(db, table, alias, use_index_names, ignore_index_names, join_root_ptr);
}

int LogicalPlanner::create_join_node_from_terminator(const std::string db, 
            const std::string table, 
            const std::string alias, 
            const std::vector<std::string>& use_index_names, 
            const std::vector<std::string>& ignore_index_names, 
            JoinMemTmp** join_root_ptr) {
    if (0 != add_table(db, table, alias)) {
        DB_WARNING("invalid database or table:%s.%s", db.c_str(), table.c_str());
        return -1;
    }
    std::unique_ptr<JoinMemTmp> join_node_mem(new JoinMemTmp);
    //叶子节点
    join_node_mem->join_node.set_join_type(pb::NULL_JOIN);
    //特殊处理一下，把跟节点的表名直接放在左子树上
    join_node_mem->left_full_table_names.insert(db + "." + table);
    //这些表在这些map中肯定存在，不需要再判断
    int64_t table_id = _table_info[db + "." + table]->id;
    int32_t tuple_id = _table_tuple_mapping[table_id].tuple_id;
    join_node_mem->join_node.add_left_tuple_ids(tuple_id);
    join_node_mem->join_node.add_left_table_ids(table_id);
    *join_root_ptr = join_node_mem.release();
    for (auto& index_name : use_index_names) {
        int64_t index_id = 0;
        auto ret = _factory->get_index_id(table_id, index_name, index_id);
        if (ret != 0) {
            DB_WARNING("index_name: %s in table:%s not exist", 
                        index_name.c_str(), (db + "." + table).c_str());
            return -1;
        }
        (*join_root_ptr)->use_indexes.insert(index_id);
    }
    //如果用户指定了use_index, 则主键永远放到use_index里
    if ((*join_root_ptr)->use_indexes.size() != 0) {
        (*join_root_ptr)->use_indexes.insert(table_id);
    }
    for (auto& index_name : ignore_index_names) {
        int64_t index_id = 0;
        auto ret = _factory->get_index_id(table_id, index_name, index_id);
        if (ret != 0) {
            DB_WARNING("index_name: %s in table:%s not exist",
                    index_name.c_str(), (db + "." + table).c_str());
            return -1;
        }
        (*join_root_ptr)->ignore_indexes.insert(index_id);
    }
    return 0;
}

int LogicalPlanner::parse_db_name_from_table_name(
            const parser::TableName* table_name, 
            std::string& db, 
            std::string& table) {
    if (table_name == nullptr) {
        DB_WARNING("table name is null");
        return -1;
    }
    if (table_name->table.empty()) {
        DB_WARNING("empty tbl name,sql:%s", _ctx->sql.c_str());
        return -1;
    }
    table = table_name->table.value;
    if (!table_name->db.empty()) {
        db = table_name->db.value;
    } else if (!_ctx->cur_db.empty()) {
        db = _ctx->cur_db;
    } else {
        _ctx->stat_info.error_code = ER_NO_DB_ERROR;
        _ctx->stat_info.error_msg << "No database selected";
        return -1;
    }
    _ctx->stat_info.family = db;
    _ctx->stat_info.table = table;
    return 0;
}

int LogicalPlanner::parse_db_name_from_table_source(const parser::TableSource* table_source, 
                                                    std::string& db, 
                                                    std::string& table,
                                                    std::string& alias) {
    if (table_source == nullptr) {
        DB_WARNING("table source is null");
        return -1;
    }
    parser::TableName* table_name = table_source->table_name;
    if (parse_db_name_from_table_name(table_name, db, table) < 0) {
        DB_WARNING("parser db name from table name fail");
        return -1;
    }
    if (!table_source->as_name.empty()) {
        alias = table_source->as_name.value;
    }
    return 0;
}

int LogicalPlanner::flatten_filter(const parser::ExprNode* item, std::vector<pb::Expr>& filters, bool use_alias) {
    if (item->expr_type == parser::ET_FUNC) {
        parser::FuncExpr* func = (parser::FuncExpr*)item;
        if (func->func_type == parser::FT_LOGIC_AND) {
            for (int i = 0; i < func->children.size(); i++) {
                if (func->children[i]->node_type != parser::NT_EXPR) {
                    DB_FATAL("type error");
                    return -1;
                }
                parser::ExprNode* child = (parser::ExprNode*)func->children[i];
                if (flatten_filter(child, filters, use_alias) != 0) {
                    DB_WARNING("parse AND child error");
                    return -1;
                }
            }
            return 0;
        } 
        if (func->func_type == parser::FT_BETWEEN) {
            pb::Expr ge_expr;
            pb::Expr le_expr;
            if (0 != create_scala_func_expr(func, ge_expr, parser::FT_GE, use_alias)) {
                DB_WARNING("create_scala_func_expr failed");
                return -1;
            }
            if (0 != create_scala_func_expr(func, le_expr, parser::FT_LE, use_alias)) {
                DB_WARNING("create_scala_func_expr failed");
                return -1;
            }
            filters.push_back(ge_expr);
            filters.push_back(le_expr);
            return 0;
        }
    }
    pb::Expr root;
    if (0 != create_expr_tree(item, root, use_alias)) {
        DB_WARNING("error pasing common expression");
        return -1;
    }
    filters.push_back(root);
    return 0;
}

void LogicalPlanner::create_order_func_slot() {
    if (_order_tuple_id == -1) {
        _order_tuple_id = _tuple_cnt++;
    }
    pb::SlotDescriptor slot;
    slot.set_slot_id(_order_slot_cnt++);
    slot.set_tuple_id(_order_tuple_id);
    slot.set_slot_type(pb::INVALID_TYPE);
    _order_slots.push_back(slot);
}

std::vector<pb::SlotDescriptor>& LogicalPlanner::get_agg_func_slot(
        const std::string& agg, const std::string& fn_name, bool& new_slot) {
    if (_agg_tuple_id == -1) {
        _agg_tuple_id = _tuple_cnt++;
    }

    std::vector<pb::SlotDescriptor>* slots = nullptr;
    auto iter = _agg_slot_mapping.find(agg);
    if (iter != _agg_slot_mapping.end()) {
        slots = &iter->second;
        new_slot = false;
    } else {
        slots = &_agg_slot_mapping[agg];
        slots->resize(1);
        (*slots)[0].set_slot_id(_agg_slot_cnt++);
        (*slots)[0].set_tuple_id(_agg_tuple_id);
        (*slots)[0].set_slot_type(pb::STRING);
        if (fn_name == "avg") {
            // create intermediate slot
            slots->push_back((*slots)[0]);
            (*slots)[1].set_slot_id(_agg_slot_cnt++);
        }
        new_slot = true;
    }
    return *slots;
}

int LogicalPlanner::create_agg_expr(const parser::FuncExpr* expr_item, pb::Expr& expr, bool use_alias) {
    static std::unordered_set<std::string> support_agg = {
        "count", "sum", "avg", "min", "max", "hll_add_agg", "hll_merge_agg"
    };
    if (support_agg.count(expr_item->fn_name.to_lower()) == 0) {
        DB_WARNING("un-supported agg op or func: %s", expr_item->fn_name.c_str());
        return -1;
    }
    bool new_slot = true;
    auto& slots = get_agg_func_slot(
            expr_item->to_string(), expr_item->fn_name.to_lower(), new_slot);
    if (slots.size() < 1) {
        DB_WARNING("wrong number of agg slots");
        return -1;
    }

    // create agg expr node
    pb::Expr agg_expr;
    pb::ExprNode* node = agg_expr.add_nodes();
    node->set_node_type(pb::AGG_EXPR);
    node->set_col_type(pb::INVALID_TYPE);
    pb::Function* func = node->mutable_fn();
    func->set_name(expr_item->fn_name.to_lower());
    func->set_fn_op(expr_item->func_type);
    func->set_has_var_args(false);

    pb::DeriveExprNode* derive_node = node->mutable_derive_node();
    derive_node->set_tuple_id(slots[0].tuple_id());
    derive_node->set_slot_id(slots[0].slot_id());
    if (slots.size() == 1) {
        derive_node->set_intermediate_slot_id(slots[0].slot_id());
    } else if (slots.size() == 2) {
        derive_node->set_intermediate_slot_id(slots[1].slot_id());
    } else {
        return -1;
    }
    
    bool count_star = expr_item->is_star;
    //count_star 参数为空
    if (!count_star) {
        // count_distinct 参数为expr list，其他聚合参数为单一expr或slot ref
        for (int i = 0; i < expr_item->children.size(); i++) {
            if (0 != create_expr_tree(expr_item->children[i], agg_expr, use_alias)) {
                DB_WARNING("create child expr failed");
                return -1;
            }
        }
    } else {
        func->set_name(func->name() + "_star");
    }
    // min max无需distinct
    if (expr_item->distinct && func->name() != "max" && func->name() != "min") {
        func->set_name(func->name() + "_distinct");
    }
    node->set_num_children(expr_item->children.size());
    expr.MergeFrom(agg_expr);
    if (new_slot) {
        if (expr_item->distinct && func->name() != "max" && func->name() != "min") {
            _distinct_agg_funcs.push_back(agg_expr);
        } else {
            _agg_funcs.push_back(agg_expr);
        }
    }
    return 0;
}

int LogicalPlanner::create_between_expr(const parser::FuncExpr* item, pb::Expr& expr, bool use_alias) {
    pb::ExprNode* and_node = expr.add_nodes();
    and_node->set_col_type(pb::BOOL);
    and_node->set_node_type(pb::AND_PREDICATE);
    and_node->set_num_children(2);
    pb::Function* func = and_node->mutable_fn();
    func->set_name("logic_and");
    func->set_fn_op(parser::FT_LOGIC_AND);

    if (0 != create_scala_func_expr(item, expr, parser::FT_GE, use_alias)) {
        DB_WARNING("create_scala_func_expr failed");
        return -1;
    }
    if (0 != create_scala_func_expr(item, expr, parser::FT_LE, use_alias)) {
        DB_WARNING("create_scala_func_expr failed");
        return -1;
    }
    return 0;
}

int LogicalPlanner::create_values_expr(const parser::FuncExpr* func_item, pb::Expr& expr) {
    if (_ctx->stmt_type != parser::NT_INSERT) {
        pb::ExprNode* node = expr.add_nodes();
        node->set_node_type(pb::NULL_LITERAL);
        node->set_col_type(pb::NULL_TYPE);
        return 0;
    }
    // values param must be ColumnName
    if (0 != create_term_slot_ref_node((parser::ColumnName*)func_item->children[0], expr, true)) {
        DB_WARNING("create_term_slot_ref_node failed");
        return -1;
    }
    return 0;
}

// TODO in next stage: fill func name
// fill arg_types, return_type(col_type) and has_var_args
int LogicalPlanner::create_scala_func_expr(const parser::FuncExpr* item, 
        pb::Expr& expr, parser::FuncType op, bool use_alias) {
    if (op == parser::FT_COMMON) {
        if (FunctionManager::instance()->get_object(item->fn_name.to_lower()) == nullptr) {
            DB_WARNING("un-supported scala op or func: %s", item->fn_name.c_str());
            return -1;
        } 
    }

    pb::ExprNode* node = expr.add_nodes();
    node->set_node_type(pb::FUNCTION_CALL);
    node->set_col_type(pb::INVALID_TYPE);
    pb::Function* func = node->mutable_fn();
    if (item->fn_name.empty()) {
        DB_WARNING("op:%d fn_name is empty", op);
    }
    func->set_name(item->fn_name.to_lower());
    func->set_fn_op(op);

    // between => (>= && <=)
    if (item->func_type == parser::FT_BETWEEN && item->children.size() == 3) {
        create_expr_tree(item->children[0], expr, use_alias);
        node->set_num_children(2);
        if (op == parser::FT_GE) {
            func->set_name("ge");
            create_expr_tree(item->children[1], expr, use_alias);
        } else if (op == parser::FT_LE) {
            func->set_name("le");
            create_expr_tree(item->children[2], expr, use_alias);
        }
        return 0;
    }
    for (int32_t idx = 0; idx < item->children.size(); ++idx) {
        if (0 != create_expr_tree(item->children[idx], expr, use_alias)) {
            DB_WARNING("create child expr failed");
            return -1;
        }
    }
    node->set_num_children(item->children.size());
    return 0;
}

int LogicalPlanner::create_expr_tree(const parser::Node* item, pb::Expr& expr, bool use_alias) {
    if (item == nullptr) {
        return -1;
    }
    if (item->node_type != parser::NT_EXPR) {
        DB_FATAL("type error:%d", item->node_type);
        return -1;
    }
    const parser::ExprNode* expr_item = (const parser::ExprNode*)item;
    if (expr_item->expr_type == parser::ET_LITETAL) {
        if (0 != create_term_literal_node((parser::LiteralExpr*)expr_item, expr)) {
            if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                _ctx->stat_info.error_code = ER_BAD_FIELD_ERROR;
                _ctx->stat_info.error_msg << "Invalid literal '" << expr_item->to_string() << "'";
            }
            DB_WARNING("create_term_literal_node failed");
            return -1;
        }
    } else if (expr_item->expr_type == parser::ET_COLUMN) {
        int ret = -1;
        if (use_alias) {
            ret = create_alias_node(static_cast<const parser::ColumnName*>(expr_item), expr);
        }
        if (ret == -2) {
            if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                _ctx->stat_info.error_code = ER_NON_UNIQ_ERROR;
                _ctx->stat_info.error_msg << "Column \'" << expr_item->to_string() << "\' is ambiguous";
            }
            return -1;
        } else if (ret == -1) {
            if (0 != create_term_slot_ref_node(static_cast<const parser::ColumnName*>(expr_item), expr)) {
                if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                    _ctx->stat_info.error_code = ER_BAD_FIELD_ERROR;
                    _ctx->stat_info.error_msg << "Unknown column \'" << expr_item->to_string() << "\'";
                }
                return -1;
            }
        }
    } else if (expr_item->expr_type == parser::ET_ROW_EXPR) {
        if (create_row_expr_node(static_cast<const parser::RowExpr*>(expr_item), expr, use_alias) != 0) {
            DB_WARNING("create_row_expr_node failed");
            return -1;
        }
    } else if (expr_item->expr_type == parser::ET_FUNC) {
        parser::FuncExpr* func = (parser::FuncExpr*)expr_item;
        switch (func->func_type) {
            case parser::FT_LOGIC_NOT:
                return create_n_ary_predicate(func, expr, pb::NOT_PREDICATE, use_alias);
            case parser::FT_LOGIC_AND:
                return create_n_ary_predicate(func, expr, pb::AND_PREDICATE, use_alias);
            case parser::FT_LOGIC_OR:
                return create_n_ary_predicate(func, expr, pb::OR_PREDICATE, use_alias);
            case parser::FT_LOGIC_XOR:
                return create_n_ary_predicate(func, expr, pb::XOR_PREDICATE, use_alias);
            case parser::FT_IS_NULL:
            case parser::FT_IS_UNKNOWN:
                return create_n_ary_predicate(func, expr, pb::IS_NULL_PREDICATE, use_alias);
            case parser::FT_IS_TRUE:
                return create_n_ary_predicate(func, expr, pb::IS_TRUE_PREDICATE, use_alias);
            case parser::FT_IN:
                return create_in_predicate(func, expr, pb::IN_PREDICATE, use_alias);
            case parser::FT_LIKE:
            case parser::FT_EXACT_LIKE:
                return create_n_ary_predicate(func, expr, pb::LIKE_PREDICATE, use_alias);
            case parser::FT_BETWEEN:
                return create_between_expr(func, expr, use_alias);
            case parser::FT_AGG:
                return create_agg_expr(func, expr, use_alias);
            case parser::FT_VALUES:
                return create_values_expr(func, expr);
            // same as scala func
            case parser::FT_BIT_NOT:
            case parser::FT_UMINUS:
            case parser::FT_ADD:
            case parser::FT_MINUS:
            case parser::FT_MULTIPLIES:
            case parser::FT_DIVIDES:
            case parser::FT_MOD:
            case parser::FT_LS:
            case parser::FT_RS:
            case parser::FT_BIT_AND:
            case parser::FT_BIT_OR:
            case parser::FT_BIT_XOR:
            case parser::FT_EQ:
            case parser::FT_NE:
            case parser::FT_GT:
            case parser::FT_GE:
            case parser::FT_LT:
            case parser::FT_LE:
            case parser::FT_MATCH_AGAINST:
            case parser::FT_COMMON:
                return create_scala_func_expr(func, expr, func->func_type, use_alias);
            // todo:support
            default:
                DB_WARNING("un-supported func_type:%d fn_name:%s", 
                        func->func_type, func->fn_name.c_str());
                return -1;
        }
    } else {
        DB_WARNING("un-supported expr_type: %d", expr_item->expr_type);
        return -1;
    }
    return 0;
}

std::string LogicalPlanner::get_field_full_name(const parser::ColumnName* column) {
    std::string full_field_name;
    if (!column->db.empty()) {
        full_field_name +=column->db.c_str();
        full_field_name += ".";
        full_field_name +=column->table.c_str();
        full_field_name += ".";
        // todo : to_lower
        full_field_name += column->name.to_lower();
        return full_field_name;
    } else if (!column->table.empty()) {
        if (_table_alias_mapping.count(column->table.c_str()) == 1) {
            full_field_name += _table_alias_mapping[column->table.c_str()];
            full_field_name += ".";
            full_field_name += column->name.to_lower();
            return full_field_name;
        }
        //table.field_name
        auto dbs = get_possible_databases(column->table.c_str());
        if (dbs.size() == 0) {
            if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                _ctx->stat_info.error_code = ER_WRONG_TABLE_NAME;
                _ctx->stat_info.error_msg << "Incorrect table name \'" << column->to_string() << "\'";
            }
            DB_WARNING("no database found for field: %s", column->to_string().c_str());
            return "";
        } else if (dbs.size() > 1) {
            if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                _ctx->stat_info.error_code = ER_AMBIGUOUS_FIELD_TERM;
                _ctx->stat_info.error_msg << "column  \'" << column->name << "\' is ambiguous";
            }
            DB_WARNING("ambiguous field_name: %s", column->to_string().c_str());
            return "";
        }
        full_field_name += *dbs.begin();
        full_field_name += ".";
        full_field_name +=column->table.c_str();
        full_field_name += ".";
        full_field_name += column->name.to_lower();
        return full_field_name;
    } else if (!column->name.empty()) {
        //field_name
        auto tables = get_possible_tables(column->name.c_str());
        if (tables.size() == 0) {
            if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                _ctx->stat_info.error_code = ER_WRONG_COLUMN_NAME;
                _ctx->stat_info.error_msg << "Incorrect column name \'" << column->to_string() << "\'";
            }
            DB_WARNING("no table found for field: %s", column->to_string().c_str());
            return "";
        } else if (tables.size() > 1) {
            DB_WARNING("ambiguous field_name: %s", column->to_string().c_str());
            if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                _ctx->stat_info.error_code = ER_AMBIGUOUS_FIELD_TERM;
                _ctx->stat_info.error_msg << "column \'" << column->name << "\' is ambiguous";
            }
            return "";
        }
        full_field_name += *tables.begin();
        full_field_name += ".";
        full_field_name += column->name.to_lower();
        return full_field_name;
    } else {
        DB_FATAL("column.name is null");
        return "";
    }
    return full_field_name;
}

ScanTupleInfo* LogicalPlanner::get_scan_tuple(int64_t table) {
    ScanTupleInfo* tuple_info = nullptr;
    auto iter = _table_tuple_mapping.find(table);
    if (iter != _table_tuple_mapping.end()) {
        tuple_info = &(iter->second);
    } else {
        tuple_info = &_table_tuple_mapping[table];
        tuple_info->tuple_id = _tuple_cnt++;
        tuple_info->table_id = table;
        tuple_info->slot_cnt = 1;
    }
    return tuple_info;
}

pb::SlotDescriptor& LogicalPlanner::get_scan_ref_slot(int64_t table, 
        int32_t field, pb::PrimitiveType type) {
    ScanTupleInfo* tuple_info = get_scan_tuple(table);
    // if (iter != _table_tuple_mapping.end()) {
    //     tuple_info = &(iter->second);
    // } else {
    //     tuple_info = &_table_tuple_mapping[table];
    //     tuple_info->tuple_id = _tuple_cnt++;
    //     tuple_info->table_id = table;
    //     tuple_info->slot_cnt = 1;
    // }
    pb::SlotDescriptor *slot_desc = nullptr;

    auto& inner_map = tuple_info->field_slot_mapping;
    auto inner_iter = inner_map.find(field);
    if (inner_iter != inner_map.end()) {
        slot_desc = &(inner_iter->second);
        slot_desc->set_ref_cnt(slot_desc->ref_cnt() + 1);
    } else {
        slot_desc = &inner_map[field];
        slot_desc->set_slot_id(tuple_info->slot_cnt++);
        slot_desc->set_slot_type(type);
        slot_desc->set_tuple_id(tuple_info->tuple_id);
        slot_desc->set_field_id(field);
        slot_desc->set_table_id(table);
        slot_desc->set_ref_cnt(1);
    }
    return *slot_desc;
}

pb::SlotDescriptor& LogicalPlanner::get_values_ref_slot(int64_t table, 
        int32_t field, pb::PrimitiveType type) {
    auto& tuple_info = _values_tuple_info;
    if (tuple_info.tuple_id == -1) {
        tuple_info.tuple_id = _tuple_cnt++;
        tuple_info.table_id = table;
        tuple_info.slot_cnt = 1;
    }
    tuple_info.table_id = table;
    pb::SlotDescriptor *slot_desc = nullptr;

    auto& inner_map = tuple_info.field_slot_mapping;
    auto inner_iter = inner_map.find(field);
    if (inner_iter != inner_map.end()) {
        slot_desc = &(inner_iter->second);
    } else {
        slot_desc = &inner_map[field];
        slot_desc->set_slot_id(tuple_info.slot_cnt++);
        slot_desc->set_slot_type(type);
        slot_desc->set_tuple_id(tuple_info.tuple_id);
        slot_desc->set_field_id(field);
        slot_desc->set_table_id(table);
    }
    return *slot_desc;
}

// -2 means alias name ambiguous
int LogicalPlanner::create_alias_node(const parser::ColumnName* column, pb::Expr& expr) {
    if (!column->db.empty()) {
        DB_WARNING("alias no db");
        return -1;
    }
    if (!column->table.empty()) {
        //DB_WARNING("alias no table");
        return -1;
    }
    std::string lower_name = column->name.to_lower();
    int match_count = _select_alias_mapping.count(lower_name);
    if (match_count > 1) {
        DB_WARNING("column name: %s is ambiguous", column->name.c_str());
        return -2;
    } else if (match_count == 0) {
        //DB_WARNING("invalid column name: %s", column->name.c_str());
        return -1;
    } else {
        auto iter = _select_alias_mapping.find(lower_name);
        if (iter != _select_alias_mapping.end() && iter->second >= _select_exprs.size()) {
            //DB_WARNING("invalid column name: %s", column->name.c_str());
            return -1;
        }
        expr.MergeFrom(_select_exprs[iter->second]);
    }
    return 0;
}

//TODO: error_code
int LogicalPlanner::create_term_slot_ref_node(
        const parser::ColumnName* col_expr, 
        pb::Expr& expr,
        bool values) {
    std::stringstream ss;
    col_expr->to_stream(ss);
    std::string origin_name = ss.str();
    std::unordered_map<std::string, pb::ExprNode>* vars = nullptr;
    std::string var_name;
    auto client = _ctx->client_conn;
    if (boost::algorithm::istarts_with(origin_name, "@@global.")) {
        // TODO handle set global variable
        return -1;
    } else if (boost::algorithm::istarts_with(origin_name, "@@session.")) {
        var_name = origin_name.substr(strlen("@@session."));
        vars = &client->session_vars;
    } else if (boost::algorithm::istarts_with(origin_name, "@@local.")) {
        var_name = origin_name.substr(strlen("@@local."));
        vars = &client->session_vars;
    } else if (boost::algorithm::istarts_with(origin_name, "@@")) {
        var_name = origin_name.substr(2);
        vars = &client->session_vars;
    } else if (boost::algorithm::istarts_with(origin_name, "@")) {
        // user variable term
        var_name = origin_name.substr(1);
        vars = &client->user_vars;
    }
    if (vars != nullptr) {
        auto iter = vars->find(var_name);
        pb::ExprNode* node = expr.add_nodes();
        if (iter != vars->end()) {
            node->CopyFrom(iter->second);
        } else {
            node->set_num_children(0);
            node->set_node_type(pb::NULL_LITERAL);
            node->set_col_type(pb::NULL_TYPE);
        }
        return 0;
    }

    std::string full_name = get_field_full_name(col_expr);
    if (full_name.empty()) {
        DB_WARNING("get full field name failed: %s", col_expr->to_string().c_str());
        return -1;
    }
    FieldInfo* field_info = nullptr;
    if (nullptr == (field_info = get_field_info_ptr(full_name))) {
        //_ctx->set_error_code(-1);
        //_ctx->set_error_msg(std::string("invalid field name in: ").append(term));
        DB_WARNING("invalid field name: %s", col_expr->to_string().c_str());
        return -1;
    }
    //DB_WARNING("field_info->id: %d", field_info->id);
    pb::SlotDescriptor slot;
    if (values) {
        slot = get_values_ref_slot(field_info->table_id, field_info->id, field_info->type);
    } else {
        slot = get_scan_ref_slot(field_info->table_id, field_info->id, field_info->type);
    }

    pb::ExprNode* node = expr.add_nodes();
    node->set_node_type(pb::SLOT_REF);
    node->set_col_type(field_info->type);
    node->set_num_children(0);
    node->mutable_derive_node()->set_tuple_id(slot.tuple_id()); //TODO
    node->mutable_derive_node()->set_slot_id(slot.slot_id());
    node->mutable_derive_node()->set_field_id(slot.field_id());
    return 0;
}

//TODO: primitive len for STRING, BOOL and NULL
int LogicalPlanner::create_term_literal_node(const parser::LiteralExpr* literal, pb::Expr& expr) {
    pb::ExprNode* node = expr.add_nodes();
    node->set_num_children(0);

    switch (literal->literal_type) {
        case parser::LT_INT:
            node->set_node_type(pb::INT_LITERAL);
            node->set_col_type(pb::INT64);
            node->mutable_derive_node()->set_int_val(literal->_u.int64_val);
            break;
        case parser::LT_DOUBLE:
            node->set_node_type(pb::DOUBLE_LITERAL);
            node->set_col_type(pb::DOUBLE);
            node->mutable_derive_node()->set_double_val(literal->_u.double_val);
            break;
        case parser::LT_STRING: 
            node->set_node_type(pb::STRING_LITERAL);
            node->set_col_type(pb::STRING);
            node->mutable_derive_node()->set_string_val(literal->_u.str_val.c_str());
            break;
        case parser::LT_BOOL:
            node->set_node_type(pb::BOOL_LITERAL);
            node->set_col_type(pb::BOOL);
            node->mutable_derive_node()->set_bool_val(literal->_u.bool_val);
            break;
        case parser::LT_NULL:
            node->set_node_type(pb::NULL_LITERAL);
            node->set_col_type(pb::NULL_TYPE);
            break;
        case parser::LT_PLACE_HOLDER:
            node->set_node_type(pb::PLACE_HOLDER_LITERAL);
            node->set_col_type(pb::PLACE_HOLDER);
            node->mutable_derive_node()->set_int_val(literal->_u.int64_val);
            break;
        default:
            DB_WARNING("create_term_literal_node failed: %d", literal->literal_type);
            return -1;
    }
    return 0;
}

int LogicalPlanner::create_row_expr_node(const parser::RowExpr* item, pb::Expr& expr, bool use_alias) {
    pb::ExprNode* node = expr.add_nodes();
    node->set_col_type(pb::INVALID_TYPE);
    node->set_node_type(pb::ROW_EXPR);
    for (int32_t idx = 0; idx < item->children.size(); ++idx) {
        if (0 != create_expr_tree(item->children[idx], expr, use_alias)) {
            DB_WARNING("create child expr failed");
            return -1;
        }
    }
    node->set_num_children(item->children.size());
    return 0;
}

int LogicalPlanner::create_orderby_exprs(parser::OrderByClause* order) {
    parser::Vector<parser::ByItem*> order_items = order->items;
    for (int idx = 0; idx < order_items.size(); ++idx) {
        bool is_asc = !order_items[idx]->is_desc;
        
        // create order by expr node
        pb::Expr order_expr;
        if (0 != create_expr_tree(order_items[idx]->expr, order_expr, true)) {
            DB_WARNING("create group expr failed");
            return -1;
        }
#ifdef BAIDU_INTERNAL
        const pb::ExprNode& node = order_expr.nodes(0);
        //如果此处是字符串常量(如'userid')，则看做是slot_ref, 按照对应的列名排序
        if (node.node_type() == pb::STRING_LITERAL) {
            parser::ColumnName tmp_col;
            std::string tmp_str = node.derive_node().string_val();
            tmp_col.name = tmp_str.c_str();
            order_expr.Clear();
            if (create_term_slot_ref_node(&tmp_col, order_expr) != 0) {
                DB_WARNING("create group term expr failed");
                return -1;
            }
        } else {
            if (node.node_type() != pb::SLOT_REF && node.node_type() != pb::AGG_EXPR) {
                //DB_WARNING("un-supported order-by node type: %d", node.node_type());
                //return -1;
                create_order_func_slot();
            }
        } 
#else
        const pb::ExprNode& node = order_expr.nodes(0);
        if (node.node_type() != pb::SLOT_REF && node.node_type() != pb::AGG_EXPR) {
            //DB_WARNING("un-supported order-by node type: %d", node.node_type());
            //return -1;
            create_order_func_slot();
        }
#endif
        _order_exprs.push_back(order_expr);
        _order_ascs.push_back(is_asc);
    }
    return 0;
}

void LogicalPlanner::create_scan_tuple_descs() {
    for (auto& pair : _table_tuple_mapping) {
        int64_t tableid = pair.first;
        auto& tuple_info = pair.second;
        auto& slot_map = tuple_info.field_slot_mapping;

        pb::TupleDescriptor tuple_desc;
        tuple_desc.set_tuple_id(tuple_info.tuple_id);
        tuple_desc.set_table_id(tableid);

        // slot_id => slot desc mapping
        std::map<int32_t, pb::SlotDescriptor> id_slot_mapping;
        //reorder the slot descriptors by slot id
        for (auto& kv : slot_map) {
            const pb::SlotDescriptor& desc = kv.second;
            id_slot_mapping.insert(std::make_pair(desc.slot_id(), desc));
        }
        for (auto& kv : id_slot_mapping) {
            const pb::SlotDescriptor& desc = kv.second;
            pb::SlotDescriptor* slot = tuple_desc.add_slots();
            slot->CopyFrom(desc);
        }
        _scan_tuples.push_back(tuple_desc);
        _ctx->add_tuple(tuple_desc);
    }
}

void LogicalPlanner::create_values_tuple_desc() {
    if (_values_tuple_info.tuple_id == -1) {
        return;
    }
    _values_tuple.set_tuple_id(_values_tuple_info.tuple_id);
    _values_tuple.set_table_id(_values_tuple_info.table_id);

    // slot_id => slot desc mapping
    std::map<int32_t, pb::SlotDescriptor> id_slot_mapping;
    auto& slot_map = _values_tuple_info.field_slot_mapping;
    //reorder the slot descriptors by slot id
    for (auto& kv : slot_map) {
        const pb::SlotDescriptor& desc = kv.second;
        id_slot_mapping.insert(std::make_pair(desc.slot_id(), desc));
    }
    for (auto& kv : id_slot_mapping) {
        const pb::SlotDescriptor& desc = kv.second;
        pb::SlotDescriptor* slot = _values_tuple.add_slots();
        slot->CopyFrom(desc);
    }
    _ctx->add_tuple(_values_tuple);
}

void LogicalPlanner::create_order_by_tuple_desc() {
    if (_order_tuple_id == -1) {
        return;
    }
    pb::TupleDescriptor order_tuple;
    order_tuple.set_tuple_id(_order_tuple_id);
    for (auto& slot : _order_slots) {
        *order_tuple.add_slots() = slot;
    }
    _ctx->add_tuple(order_tuple);
}

int LogicalPlanner::create_packet_node(pb::OpType op_type) {
    pb::PlanNode* pack_node = _ctx->add_plan_node();
    pack_node->set_node_type(pb::PACKET_NODE);
    pack_node->set_limit(-1);
    pack_node->set_is_explain(_ctx->is_explain);
    pack_node->set_num_children(1); //TODO 
    pb::DerivePlanNode* derive = pack_node->mutable_derive_node();
    pb::PacketNode* pack = derive->mutable_packet_node();

    pack->set_op_type(op_type);
    if (op_type != pb::OP_SELECT) {
        return 0;
    }
    for (auto& expr : _select_exprs) {
        auto proj = pack->add_projections();
        proj->CopyFrom(expr);
    }
    for (auto& name : _select_names) {
        pack->add_col_names(name);
    }
    return 0;
}

int LogicalPlanner::create_filter_node(std::vector<pb::Expr>& filters, 
        pb::PlanNodeType type) {
    if (filters.size() == 0) {
        return 0;
    }
    pb::PlanNode* where_node = _ctx->add_plan_node();
    where_node->set_node_type(type);
    where_node->set_limit(-1);
    where_node->set_is_explain(_ctx->is_explain);
    where_node->set_num_children(1); //TODO 
    pb::DerivePlanNode* derive = where_node->mutable_derive_node();
    pb::FilterNode* filter = derive->mutable_filter_node();
    
    for (uint32_t idx = 0; idx < filters.size(); ++idx) {
        pb::Expr* expr = filter->add_conjuncts();
        expr->CopyFrom(filters[idx]);
    }
    //DB_WARNING("filter node: %s", pb2json(*where_node).c_str());
    return 0;
}

int LogicalPlanner::create_sort_node() {
    if (_order_exprs.size() == 0) {
        return 0;
    }
    pb::PlanNode* sort_node = _ctx->add_plan_node();
    sort_node->set_node_type(pb::SORT_NODE);
    sort_node->set_limit(-1);
    sort_node->set_is_explain(_ctx->is_explain);
    sort_node->set_num_children(1); //TODO
    pb::DerivePlanNode* derive = sort_node->mutable_derive_node();
    pb::SortNode* sort = derive->mutable_sort_node();
    
    if (_order_exprs.size() != _order_ascs.size()) {
        DB_WARNING("order expr format error");
        return -1;
    }
    for (uint32_t idx = 0; idx < _order_exprs.size(); ++idx) {
        pb::Expr* order_expr = sort->add_order_exprs();
        pb::Expr* slot_order_expr = sort->add_slot_order_exprs();
        order_expr->CopyFrom(_order_exprs[idx]);
        slot_order_expr->CopyFrom(_order_exprs[idx]);
        sort->add_is_asc(_order_ascs[idx]);
        sort->add_is_null_first(_order_ascs[idx]);
    }
    sort->set_tuple_id(_order_tuple_id);
    return 0;
}
int LogicalPlanner::create_join_and_scan_nodes(JoinMemTmp* join_root) {
    if (join_root == NULL) {
        DB_WARNING("join root is null");
        return -1;
    }
    //叶子节点
    if (join_root->join_node.join_type() == pb::NULL_JOIN) {
        pb::PlanNode* scan_node = _ctx->add_plan_node();
        scan_node->set_node_type(pb::SCAN_NODE);
        scan_node->set_limit(-1);
        scan_node->set_is_explain(_ctx->is_explain);
        scan_node->set_num_children(0);
        pb::DerivePlanNode* derive = scan_node->mutable_derive_node();
        pb::ScanNode* scan = derive->mutable_scan_node();
        scan->set_tuple_id(join_root->join_node.left_tuple_ids(0));
        scan->set_table_id(join_root->join_node.left_table_ids(0));
        scan->set_engine(_factory->get_table_engine(scan->table_id()));
        for (auto index_id : join_root->use_indexes) {
            scan->add_use_indexes(index_id);
        }
        for (auto index_id : join_root->ignore_indexes) {
            scan->add_ignore_indexes(index_id);
        }
        return 0;
    }
    //如果不是根节点必须是左右孩子都有
    if (join_root->left_node == NULL || join_root->right_node == NULL) {
        DB_WARNING("create join node fail");
        return -1;
    }
    pb::PlanNode* join_node = _ctx->add_plan_node();
    join_node->set_node_type(pb::JOIN_NODE);
    join_node->set_limit(-1);
    join_node->set_is_explain(_ctx->is_explain);
    join_node->set_num_children(2);
    pb::DerivePlanNode* derive = join_node->mutable_derive_node();
    pb::JoinNode* join = derive->mutable_join_node();
    *join = join_root->join_node;
    if (0 != create_join_and_scan_nodes(join_root->left_node)) {
        DB_WARNING("create left child when join node fail");
        return -1;
    }
    if (0 != create_join_and_scan_nodes(join_root->right_node)) {
        DB_WARNING("create right child when join node fail");
        return -1;
    }
    return 0;
}
int LogicalPlanner::create_scan_nodes() {
    for (auto& tuple_desc : _scan_tuples) {
        pb::PlanNode* scan_node = _ctx->add_plan_node();
        scan_node->set_node_type(pb::SCAN_NODE);
        scan_node->set_limit(-1);
        scan_node->set_is_explain(_ctx->is_explain);
        scan_node->set_num_children(0); //TODO 
        pb::DerivePlanNode* derive = scan_node->mutable_derive_node();
        pb::ScanNode* scan = derive->mutable_scan_node();
        scan->set_tuple_id(tuple_desc.tuple_id());
        scan->set_table_id(tuple_desc.table_id());
        scan->set_engine(_factory->get_table_engine(scan->table_id()));
    }
    return 0;
}

void LogicalPlanner::set_dml_txn_state(int64_t table_id) {
    auto client = _ctx->client_conn;
    if (client->txn_id == 0) {
        if (_ctx->enable_2pc
            || _factory->has_global_index(table_id)) {
            client->txn_id = get_txn_id();
            client->seq_id = 0;
        } else {
            client->txn_id = 0;
            client->seq_id = 0;
        }
        //DB_WARNING("DEBUG client->txn_id:%ld client->seq_id: %d", client->txn_id, client->seq_id);
        _ctx->get_runtime_state()->set_single_sql_autocommit(true);
    } else {
        //DB_WARNING("DEBUG client->txn_id:%ld client->seq_id: %d", client->txn_id, client->seq_id);
        _ctx->get_runtime_state()->set_single_sql_autocommit(false);
    }
}

// TODO: instance_part may overflow and wrapped
uint64_t LogicalPlanner::get_txn_id() {
    auto client = _ctx->client_conn;
    uint64_t instance_part = client->server_instance_id & 0x7FFFFF;

    uint64_t txn_id_part = _txn_id_counter.fetch_add(1);

    // TODO: request meta_server for a txn id
    return (instance_part << 40 | (txn_id_part & 0xFFFFFFFFFFUL));
}

void LogicalPlanner::plan_begin_txn() {
    create_packet_node(pb::OP_BEGIN);
    pb::PlanNode* plan_node = _ctx->add_plan_node();
    plan_node->set_node_type(pb::BEGIN_MANAGER_NODE);
    plan_node->set_limit(-1);
    pb::DerivePlanNode* derived_node = plan_node->mutable_derive_node();
    pb::TransactionNode* txn_node = derived_node->mutable_transaction_node();
    
    txn_node->set_txn_cmd(pb::TXN_BEGIN);

    auto client = _ctx->client_conn;
    uint64_t txn_id = get_txn_id();
    client->on_begin(txn_id);
    client->seq_id = 0;
    return;
}

void LogicalPlanner::plan_commit_txn() {
    create_packet_node(pb::OP_COMMIT);
    pb::PlanNode* plan_node = _ctx->add_plan_node();
    plan_node->set_limit(-1);
    plan_node->set_node_type(pb::COMMIT_MANAGER_NODE);
    pb::DerivePlanNode* derived_node = plan_node->mutable_derive_node();
    pb::TransactionNode* txn_node = derived_node->mutable_transaction_node();
    
    //txn_node->set_txn_id(_ctx->txn_id);
    txn_node->set_txn_cmd(pb::TXN_COMMIT);
}

void LogicalPlanner::plan_commit_and_begin_txn() {
    create_packet_node(pb::OP_COMMIT);
    pb::PlanNode* plan_node = _ctx->add_plan_node();
    plan_node->set_node_type(pb::COMMIT_MANAGER_NODE);
    plan_node->set_limit(-1);
    pb::DerivePlanNode* derived_node = plan_node->mutable_derive_node();
    pb::TransactionNode* txn_node = derived_node->mutable_transaction_node();
    
    txn_node->set_txn_cmd(pb::TXN_COMMIT_BEGIN);

    auto client = _ctx->client_conn;
    client->new_txn_id = get_txn_id();
    return;
}

void LogicalPlanner::plan_rollback_txn() {
    create_packet_node(pb::OP_ROLLBACK);
    pb::PlanNode* plan_node = _ctx->add_plan_node();
    plan_node->set_node_type(pb::ROLLBACK_MANAGER_NODE);
    plan_node->set_limit(-1);
    pb::DerivePlanNode* derived_node = plan_node->mutable_derive_node();
    pb::TransactionNode* txn_node = derived_node->mutable_transaction_node();
    
    DB_WARNING("plan_rollback_txn");
    //txn_node->set_txn_id(_ctx->txn_id);
    txn_node->set_txn_cmd(pb::TXN_ROLLBACK);
}

void LogicalPlanner::plan_rollback_and_begin_txn() {
    create_packet_node(pb::OP_ROLLBACK);
    pb::PlanNode* plan_node = _ctx->add_plan_node();
    plan_node->set_node_type(pb::ROLLBACK_MANAGER_NODE);
    plan_node->set_limit(-1);
    pb::DerivePlanNode* derived_node = plan_node->mutable_derive_node();
    pb::TransactionNode* txn_node = derived_node->mutable_transaction_node();
    
    txn_node->set_txn_cmd(pb::TXN_ROLLBACK_BEGIN);

    auto client = _ctx->client_conn;
    client->new_txn_id = get_txn_id();
}
} //namespace
