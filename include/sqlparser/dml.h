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
#include "base.h"
#include "expr.h"

namespace parser {

static const char* priority_str[] = {"", " LOW_PRIORITY", " DELAYED", " HIGH_PRIORITY"};
static const char* for_lock_str[] = {"", " FOR UPDATE", " IN SHARED MODE"};
enum PriorityEnum {
    PE_NO_PRIORITY = 0,
    PE_LOW_PRIORITY = 1,
    PE_DELAYED_PRIORITY = 2,
    PE_HIGH_PRIORITY = 3,
};

enum IndexHintType {
    IHT_HINT_NONE = 0,
    IHT_HINT_USE = 1,
    IHT_HINT_IGNORE = 2,
    IHT_HINT_FORCE  = 3,
};

enum IndexHintScope {
    IHS_HINT_NONE = 0,
    IHS_HINT_SCAN = 1,
    IHS_HINT_JOIN = 2,
    IHS_HINT_ORDER_BY = 3,
    IHS_HINT_GROUP_BY = 4,
};

enum JoinType {
    JT_NONE = 0,
    JT_INNER_JOIN = 1,
    JT_LEFT_JOIN = 2,
    JT_RIGHT_JOIN = 3,
};

struct IndexHint : public Node {
    IndexHintType hint_type = IHT_HINT_NONE;
    IndexHintScope hint_scope = IHS_HINT_NONE;
    Vector<String> index_name_list; 
    IndexHint() {
        node_type = NT_INDEX_HINT; 
    }
    virtual void to_stream(std::ostream& os) const override {
        static const char* hint_type_str[] = {"", " USE INDEX", " IGNORE INDEX", " FORCE INDEX"};
        static const char* hint_scope_str[] = {"", " FOR SCAN", " FOR JOIN", " FOR ORDER_BY", " FOR GROUP_BY"};
        os << hint_type_str[hint_type] << hint_scope_str[hint_scope] << " (";
        for (int i = 0; i < index_name_list.size(); ++i) {
            os << " " << index_name_list[i];
            if (i != index_name_list.size() - 1) {
                os << ",";
            }
        }
        os << " )";
    }
};

struct TableName : public Node {
    String db;
    String table;
    TableName() {
        node_type = NT_TABLE;
        db = nullptr;
        table = nullptr;
    }
    virtual void to_stream(std::ostream& os) const override {
        if (!db.empty()) {
            os << db << ".";
        }
        os << table;
    }
};

struct TableSource : public Node {
    TableName*  table_name;
    String  as_name;
    Vector<IndexHint*> index_hints;
    TableSource() {
        node_type = NT_TABLE_SOURCE;
    }
    virtual void to_stream(std::ostream& os) const override {
        os << " " << table_name;
        if (!as_name.empty()) {
            os << " AS " << as_name.value;
        }
        for (int i = 0; i < index_hints.size(); ++i) {
            os << index_hints[i];
        }
    }
};

struct ResultSetNode : public Node {
    //std::vector<ResultField*> result_fields;
    ResultSetNode() {
        node_type = NT_RESULT_SET;
    }
};

struct JoinNode : public Node {
    Node* left = nullptr;
    Node* right = nullptr;
    JoinType join_type = JT_INNER_JOIN;
    ExprNode* expr = nullptr;
    Vector<ColumnName*> using_col;
    bool is_natural = false;
    bool is_straight = false;
    JoinNode() {
        node_type = NT_JOIN;
    }
    virtual void set_print_sample(bool print_sample_) {
        print_sample = print_sample_;
        if (left != nullptr) {
            left->set_print_sample(print_sample_);
        }
        if (right != nullptr) {
            right->set_print_sample(print_sample_);
        }
        if (expr != nullptr) {
            expr->set_print_sample(print_sample_);
        }
    }
    virtual void to_stream(std::ostream& os) const override {
        static const char* natural_str[] = {"", " NATURE"};
        static const char* straight_str[] = {"", " STRAIGHT"};
        static const char* join_type_str[] = {"", " inner join", " left join", " right join"};
        os << " (" << left << " )" << natural_str[is_natural] << straight_str[is_straight] << join_type_str[join_type] << " (" << right << " )";
        if (expr != nullptr) {
            os << " ON ";
            os << expr;
        }
        if (using_col.size() != 0) {
            os << " USING (";
        }
        for (int i = 0; i < using_col.size(); ++i) {
            os << using_col[i];
            if (i != using_col.size() -1) {
                os << ", ";
            }
        }
        if (using_col.size() != 0) {
            os << ")";
        }
    }    
};

struct ByItem : public Node {
    ExprNode* expr = nullptr;
    bool is_desc = false;
    ByItem() {
        node_type = NT_BY_ITEM;
    }
    virtual void set_print_sample(bool print_sample_) {
        print_sample = print_sample_;
        if (expr != nullptr) {
            expr->set_print_sample(print_sample_);
        }
    }
    virtual void to_stream(std::ostream& os) const override {
        static const char* desc_str[] = {" ASC", " DESC"};
        os << expr << desc_str[is_desc];
    }
};

struct GroupByClause : public Node {
    Vector<ByItem*> items;
    GroupByClause() {
        node_type = NT_GROUP_BY;
    }
    virtual void set_print_sample(bool print_sample_) {
        print_sample = print_sample_;
        for (int i = 0; i < items.size(); i++) {
            items[i]->set_print_sample(print_sample_);
        }
    }
    virtual void to_stream(std::ostream& os) const override {
        for (int i = 0; i < items.size(); i++) {
            os << " " << items[i];
            if (i != items.size() -1) {
                os << ",";
            }
        }
    }
};

struct OrderByClause : public Node {
    Vector<ByItem*> items;
    OrderByClause() {
        node_type = NT_ORDER_BY;
    }
    virtual void set_print_sample(bool print_sample_) {
        print_sample = print_sample_;
        for (int i = 0; i < items.size(); i++) {
            items[i]->set_print_sample(print_sample_);
        }
    }
    virtual void to_stream(std::ostream& os) const override {
        for (int i = 0; i < items.size(); i++) {
            os << " " << items[i];
            if (i != items.size() -1) {
                os << ",";
            }
        }
    }
};

struct LimitClause : public Node {
    ExprNode* offset;
    ExprNode* count;
    LimitClause() {
       node_type = NT_LIMIT;
       offset = nullptr;
       count = nullptr;
    }
    virtual void set_print_sample(bool print_sample_) {
        print_sample = print_sample_;
        if (offset != nullptr) {
            offset->set_print_sample(print_sample_);
        }
        if (count != nullptr) {
            count->set_print_sample(print_sample_);
        }
    }
    virtual void to_stream(std::ostream& os) const override {
        os << " " << offset << ", " << count;
    }
};

struct WildCardField : public Node {
    String table_name;
    String db_name;
    WildCardField() {
        node_type = NT_WILDCARD;
        table_name = nullptr;
        db_name = nullptr;
    }
    virtual void to_stream(std::ostream& os) const override {
        if (!db_name.empty()) {
            os << db_name << ".";
        }
        if (!table_name.empty()) {
            os << table_name << ".";
        }
        os << "*";
    }
};
struct SelectField : public Node {
    int offset = 0;
    ExprNode* expr = nullptr;
    String as_name;
    WildCardField* wild_card = nullptr;
    SelectField() {
        node_type = NT_SELECT_FEILD;
        as_name = nullptr;
    }
    virtual void set_print_sample(bool print_sample_) {
        print_sample = print_sample_;
        if (expr != nullptr) {
            expr->set_print_sample(print_sample_);
        }
    }
    virtual void to_stream(std::ostream& os) const override {
        os << " " << expr << wild_card;
        if (!as_name.empty()) {
            os << " AS " << as_name;
        }
    }
};

struct Assignment : public Node {
    ColumnName* name = nullptr;
    ExprNode* expr = nullptr;
    Assignment() {
        node_type = NT_ASSIGNMENT;
    }
    virtual void set_print_sample(bool print_sample_) {
        print_sample = print_sample_;
        if (expr != nullptr) {
            expr->set_print_sample(print_sample_);
        }
    }
    virtual void to_stream(std::ostream& os) const override {
        os << name << " = " << expr;
    }
};

struct InsertStmt : public DmlNode {
    PriorityEnum priority;
    bool is_replace = false;
    bool is_ignore = false;
    TableName* table_name = nullptr;
    Vector<ColumnName*> columns;
    Vector<RowExpr*> lists;
    Vector<Assignment*> on_duplicate;
    InsertStmt() {
        node_type = NT_INSERT;
    }
    virtual void set_print_sample(bool print_sample_) {
        print_sample = print_sample_;
        for (int i = 0; i < lists.size(); i++) {
            lists[i]->set_print_sample(print_sample_);
        }
        for (int i = 0; i < on_duplicate.size(); i++) {
            on_duplicate[i]->set_print_sample(print_sample_);
        }
    }
    static InsertStmt* New(butil::Arena& arena) {
        InsertStmt* insert = new(arena.allocate(sizeof(InsertStmt)))InsertStmt();
        insert->columns.reserve(10, arena);
        insert->lists.reserve(10, arena);
        insert->on_duplicate.reserve(10, arena);
        return insert;
    }
    virtual void to_stream(std::ostream& os) const override {
        static const char* desc_str[] = {" INSERT", " REPLACE"};
        static const char* ignore_str[] = {"", " IGNORE"};
        os << desc_str[is_replace];
        os << priority_str[priority] << ignore_str[is_ignore] << " INTO ";
        if (table_name != nullptr) {
            table_name->to_stream(os);
        }
        if (columns.size() > 0) {
            os << "(";
        }
        for (int i = 0; i < columns.size(); ++i) {
            columns[i]->to_stream(os);
            if (i != columns.size() - 1) {
                os << ", ";
            }
        }
        if (columns.size() > 0) {
            os << ")";
        }
        os << " VALUES";
        if (print_sample) {
            os << " (?) ";
        } else {
            for (int i = 0; i < lists.size(); ++i) {
                os << " ";
                lists[i]->to_stream(os);
                if (i != lists.size() - 1) {
                    os << ",";
                }
            }
        }
        if (on_duplicate.size() != 0) {
            os << " ON DUPLICATE KEY UPDATE";
        }
        for (int i = 0; i < on_duplicate.size(); ++i) {
            os << " ";
            on_duplicate[i]->to_stream(os);
            if (i != on_duplicate.size() - 1) {
                os << ",";
            }
        }
    }
};

struct TruncateStmt: public DmlNode {
    TableName* table_name = nullptr;
    TruncateStmt() {
        node_type = NT_TRUNCATE;
    }
    virtual void to_stream(std::ostream& os) const override {
        os << "TRUNCATE " << table_name;
    }
};

struct DeleteStmt : public DmlNode {
    PriorityEnum priority;
    bool is_ignore = false;
    bool is_quick = false;
    Node* from_table = nullptr; //table_name or join or tableresource
    Vector<TableName*> delete_table_list;
    ExprNode* where = nullptr;
    OrderByClause* order = nullptr;
    LimitClause* limit = nullptr;
    DeleteStmt() {
        node_type = NT_DELETE;
    }
    virtual void set_print_sample(bool print_sample_) {
        print_sample = print_sample_;
        if (from_table != nullptr) {
            from_table->set_print_sample(print_sample_);
        }
        if (where != nullptr) {
            where->set_print_sample(print_sample_);
        }
        if (order != nullptr) {
            order->set_print_sample(print_sample_);
        }
        if (limit != nullptr) {
            limit->set_print_sample(print_sample_);
        }
    }
    virtual void to_stream(std::ostream& os) const override {
        static const char* ignore_str[] = {"", " IGNORE"};
        static const char* quick_str[] = {"", " QUICK"};
        os << "DELETE";
        os << priority_str[priority];
        os << quick_str[is_quick] << ignore_str[is_ignore];
        if (delete_table_list.size() != 0) {
            for (int i = 0; i < delete_table_list.size(); ++i) {
                os << " ";
                delete_table_list[i]->to_stream(os);
                if (i != delete_table_list.size() - 1) {
                    os << ",";
                }
            }
        }
        os << " FROM ";
        from_table->to_stream(os);
        if (where != nullptr) {
            os << " WHERE ";
            where->to_stream(os);
        }
        if (order != nullptr) {
            os << " ORDER BY";
            order->to_stream(os);
        }
        if (limit != nullptr) {
            os << " LIMIT";
            limit->to_stream(os);
        }
    }
};

struct UpdateStmt : public DmlNode {
    PriorityEnum priority;
    bool is_ignore = false;
    Node* table_refs = nullptr; // join or tableresource
    Vector<Assignment*> set_list;
    ExprNode* where = nullptr;
    OrderByClause* order = nullptr;
    LimitClause* limit = nullptr;
    UpdateStmt() {
        node_type = NT_UPDATE;
    }
    virtual void set_print_sample(bool print_sample_) {
        print_sample = print_sample_;
        if (table_refs != nullptr) {
            table_refs->set_print_sample(print_sample_);
        }
        if (where != nullptr) {
            where->set_print_sample(print_sample_);
        }
        for (int i = 0; i < set_list.size(); i++) {
            set_list[i]->set_print_sample(print_sample_);
        }
        if (order != nullptr) {
            order->set_print_sample(print_sample_);
        }
        if (limit != nullptr) {
            limit->set_print_sample(print_sample_);
        }
    }
    virtual void to_stream(std::ostream& os) const override {
        static const char* ignore_str[] = {"", " IGNORE"};
        os << "UPDATE";
        os << ignore_str[is_ignore];
        os << table_refs << " SET";
        for (int i = 0; i < set_list.size(); ++i) {
            os << " " << set_list[i];
            if (i != set_list.size() - 1) {
                os << ",";
            }
        }
        if (where != nullptr) {
            os << " WHERE " << where;
        }
        if (order != nullptr) {
            os << " ORDER BY" << order;
        }
        if (limit != nullptr) {
            os << " LIMIT" << limit;
        }
    }
};

struct SelectStmtOpts {
    bool distinct = false;
    bool sql_cache = false;
    bool calc_found_rows = false;
    bool straight_join = false;
    PriorityEnum priority;
    void to_stream(std::ostream& os) const {
       static const char* distinct_str[] = {"", " DISTINCT"};
       static const char* sql_cache_str[] = {"", " SQL_CACHE"};
       static const char* calc_found_rows_str[] = {"", " SQL_CALC_FOUND_ROWS"};
       static const char* straight_join_str[] = {"", " STRAIGHT_JOIN"};
       os << distinct_str[distinct] << straight_join_str[straight_join] <<  sql_cache_str[sql_cache] << calc_found_rows_str[calc_found_rows];
       os << priority_str[priority];
    }
};

enum SelectLock {
    SL_NONE,
    SL_FOR_UPDATE,
    SL_IN_SHARE,
};

struct SelectStmt : public DmlNode {
    SelectStmtOpts* select_opt = nullptr;
    Vector<SelectField*> fields;
    Node* table_refs = nullptr;
    ExprNode* where = nullptr;
    GroupByClause* group = nullptr;
    ExprNode* having = nullptr;
    OrderByClause* order = nullptr;
    LimitClause* limit = nullptr;
    bool is_in_braces = false;
    SelectLock lock = SL_NONE;
    SelectStmt() {
        node_type = NT_SELECT;
    }
    virtual void set_print_sample(bool print_sample_) {
        print_sample = print_sample_;
        for (int i = 0; i < fields.size(); i++) {
            fields[i]->set_print_sample(print_sample_);
        }
        if (table_refs != nullptr) {
            table_refs->set_print_sample(print_sample_);
        }
        if (where != nullptr) {
            where->set_print_sample(print_sample_);
        }
        if (group != nullptr) {
            group->set_print_sample(print_sample_);
        }
        if (having != nullptr) {
            having->set_print_sample(print_sample_);
        }
        if (order != nullptr) {
            order->set_print_sample(print_sample_);
        }
        if (limit != nullptr) {
            limit->set_print_sample(print_sample_);
        }
    }

    virtual void to_stream(std::ostream& os) const override {
        if (is_in_braces) {
            os << "(";
        }
        os << "SELECT";
        select_opt->to_stream(os);
        for (int i = 0; i < fields.size(); ++i) {
            if (i != 0) {
                os << ",";
            }
            os << fields[i];
        }
        if (table_refs != nullptr) {
            os << " FROM" << table_refs;
        }
        if (where != nullptr) {
            os << " WHERE " << where;
        }
        if (group != nullptr) {
            os << " GROUP BY" << group;
        }
        if (having != nullptr) {
            os << " HAVING" << having;
        }
        if (order != nullptr) {
            os << " ORDER BY" << order;
        }
        if (limit != nullptr) {
            os << " LIMIT" << limit;
        }
        os << for_lock_str[lock];
        if (is_in_braces) {
            os << ")";
        }
    }
};

struct UnionStmt : public DmlNode {
    bool distinct = false;   // 只支持全部去重
    Vector<SelectStmt*> select_stmts;
    OrderByClause* order = nullptr;
    LimitClause* limit = nullptr;
    SelectLock lock = SL_NONE;
    UnionStmt() {
        node_type = NT_UNION;
    }
    virtual void set_print_sample(bool print_sample_) {
        for (int i = 0; i < select_stmts.size(); i++) {
            select_stmts[i]->set_print_sample(print_sample_);
        }
    }

    virtual void to_stream(std::ostream& os) const override {
         for (int i = 0; i < select_stmts.size(); i++) {
            select_stmts[i]->to_stream(os);
            if (i < select_stmts.size() -1) {
                if (distinct) {
                    os << " UNION ";
                } else {
                    os << " UNION ALL ";
                }
            }
        }
        if (order != nullptr) {
            os << " ORDER BY" << order;
        }
        if (limit != nullptr) {
            os << " LIMIT" << limit;
        }
        os << for_lock_str[lock];
    }
};

enum ShowType {
    SHOW_NONE,
    SHOW_ENGINES,
    SHOW_DATABASES,
    SHOW_TABLES,
    SHOW_TABLE_STATUS,
    SHOW_COLUMNS,
    SHOW_WARNINGS,
    SHOW_CHARSET,
    SHOW_VARIABLES,
    SHOW_STATUS,
    SHOW_COLLATION,
    SHOW_CREATE_TABLE,
    SHOW_GTRANTS,
    SHOW_TRIGGERS,
    SHOW_PROCEDURE_STATUS,
    SHOW_INDEX,
    SHOW_PROCESS_LIST,
    SHOW_CREATE_DATABASE,
    SHOW_EVNETS,
    SHOW_PLUGINS,
    SHOW_PROFILES,
    SHOW_PRIVILEGES,
    SHOW_MASTER_STATUS
};

struct ShowStmt : public DmlNode {
    ShowType show_type;
    String db_name;
    TableName* table_name = nullptr;
    ColumnName* column_name = nullptr;
    bool is_full = false;
    bool is_global = false;
    ExprNode* where = nullptr;
};

struct ExplainStmt : public DmlNode {
    StmtNode* stmt = nullptr;
    String format;
    ExplainStmt() {
        node_type = NT_EXPLAIN;
    }
};

} 

/* vim: set ts=4 sw=4 sts=4 tw=100 */
