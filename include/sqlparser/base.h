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
#include <cstdint>
#include <cstring>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <unordered_map>

#ifdef BAIDU_INTERNAL
#include <base/arena.h>
#else
#include <butil/arena.h>
#endif
#include "utils.h"

namespace parser {
enum NodeType {
    NT_BASE,
    NT_STMT,
    NT_DML,
    NT_DDL,
    NT_TABLE,
    NT_COLUMN,
    NT_JOIN,
    NT_TABLE_SOURCE,
    NT_INDEX_HINT,
    /*DML*/
    NT_BY_ITEM,
    NT_GROUP_BY,
    NT_ORDER_BY,
    NT_LIMIT,
    NT_WILDCARD,
    NT_SELECT_FEILD,
    NT_RESULT_SET,
    NT_ASSIGNMENT,
    NT_INSERT,
    NT_UPDATE,
    NT_DELETE,
    NT_SELECT,
    NT_UNION,
    NT_TRUNCATE,
    NT_SHOW,
    NT_EXPLAIN,
    /*EXPR*/
    NT_EXPR,

    /*DDL*/
    NT_COLUMN_OPT, // ColumnOption in ddl.h
    NT_COLUMN_DEF, // ColumnDef in ddl.h
    NT_CONSTRAINT,
    NT_INDEX_OPT,
    NT_TABLE_OPT,
    NT_CREATE_TABLE, // CreateTableStmt in ddl.h
    NT_FLOAT_OPT,    
    NT_TYPE_OPT,     // TypeOption (unsigned, zerofill) 
    NT_FIELD_TYPE,   // TypeOption (unsigned, zerofill) 
    NT_DROP_TABLE,
    NT_RESTORE_TABLE,
    NT_DATABASE_OPT,
    NT_CREATE_DATABASE,
    NT_DROP_DATABASE,
    NT_ALTER_TABLE,
    NT_ALTER_SEPC,
    NT_FIELDS,
    NT_FIELDS_ITEM,
    NT_LINES,
    NT_LOAD_DATA,

    NT_START_TRANSACTION,
    NT_COMMIT_TRANSACTION,
    NT_ROLLBACK_TRANSACTION,
    NT_SET_CMD,
    NT_VAR_ASSIGN,
    NT_NEW_PREPARE,
    NT_EXEC_PREPARE,
    NT_DEALLOC_PREPARE,
    NT_KILL
};

struct Node {
    virtual ~Node() {}
    NodeType node_type = NT_BASE;
    bool print_sample = false;
    bool is_complex = false; // join和子查询认为是复杂查询
    virtual void set_print_sample(bool print_sample_) {
        print_sample = print_sample_;
        for (int i = 0; i < children.size(); i++) {
            children[i]->set_print_sample(print_sample_);
        }
    }
    virtual bool is_complex_node() {
        if (is_complex) {
            return true;
        }
        for (int i = 0; i < children.size(); i++) {
            if (children[i]->is_complex_node()) {
                is_complex = true;
                return true;
            }
        }
        return false;
    }
    // children 可以用来作为子树，函数参数，列表等功能
    Vector<Node*> children;
    virtual void print() const {
        std::cout << "type:" << node_type << std::endl;
    }
    // to_stream usage: 
    //   std::ostringstream os;
    //   node->to_stream(os); or os << node;
    //   os.str();
    virtual void to_stream(std::ostream& os) const {}

    virtual std::string to_string() const {
        std::ostringstream os;
        to_stream(os);
        return os.str();
    }
};
inline std::ostream& operator<<(std::ostream& os, const Node* node) {
    if (node != nullptr) {
        node->to_stream(os);
    }
    return os;
}
inline std::ostream& operator<<(std::ostream& os, const Node& node) {
    node.to_stream(os);
    return os;
}

struct StmtNode : public Node {
    StmtNode() {
        node_type = NT_STMT;
    }
};

struct DmlNode : public StmtNode {
    DmlNode() {
        node_type = NT_DML;
    }
};

struct DdlNode : public StmtNode {
    DdlNode() {
        node_type = NT_DDL;
    }
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
