// Copyright (c) 2018 Baidu, Inc. All Rights Reserved.
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
    NT_TRUNCATE,
    NT_SHOW,
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
    NT_DATABASE_OPT,
    NT_CREATE_DATABASE,
    NT_DROP_DATABASE,
    NT_ALTER_TABLE,
    NT_ALTER_SEPC,

    NT_START_TRANSACTION,
    NT_COMMIT_TRANSACTION,
    NT_ROLLBACK_TRANSACTION,
    NT_SET_CMD,
    NT_VAR_ASSIGN
};

struct String {
    char* value;
    void strdup(const char* str, int len, butil::Arena& arena) {
        value = (char*)arena.allocate(len + 1);
        memcpy(value, str, len);
        value[len] = '\0';
    }
    void strdup(const char* str, butil::Arena& arena) {
        strdup(str, strlen(str), arena);
    }
    void append(const char* str, butil::Arena& arena) {
        int len = strlen(str);
        int old_len = strlen(value);
        char* value_new = (char*)arena.allocate(len + old_len + 1);
        memcpy(value_new, value, old_len);
        memcpy(value_new, str, len);
        value_new[len + old_len] = '\0';
        value = value_new;
    }
    // cannot have constructor in union
    void set_null() {
        value = nullptr;
    }
    const char* c_str() const {
        return value;
    }
    bool empty() const {
        return (value == nullptr || value[0] == '\0');
    }
    void restore_5c() {
        size_t i = 0;
        size_t len = strlen(value);
        while (i < len) {
            if ((value[i] & 0x80) != 0) {
                if (++i >= len) {
                    return;
                }
                if (value[i] == 0x7F) {
                    value[i] = 0x5C;
                }
            }
            ++i;
        }
    }
    void stripslashes() {
        size_t slow = 0;
        size_t fast = 0;
        bool has_slash = false;
        static std::unordered_map<char, char> trans_map = {
            {'\\', '\\'},
            {'\"', '\"'},
            {'\'', '\''},
            {'r', '\r'},
            {'t', '\t'},
            {'n', '\n'},
            {'b', '\b'},
            {'Z', '\x1A'},
        };
        size_t len = strlen(value);
        while (fast < len) {
            if (has_slash) {
                if (trans_map.count(value[fast]) == 1) {
                    value[slow++] = trans_map[value[fast++]];
                } else if (value[fast] == '%' || value[fast] == '_') {
                    // like中的特殊符号，需要补全'\'
                    value[slow++] = '\\';
                    value[slow++] = value[fast++];
                }
                has_slash = false;
            } else {
                if (value[fast] == '\\') {
                    has_slash = true;
                    fast++;
                } else {
                    value[slow++] = value[fast++];
                }
            }
        }
        value[slow] = '\0';
    }
    String& to_lower_inplace() {
        if (value != nullptr) {
            int len = strlen(value);
            std::transform(value, value + len, value, ::tolower);
        }
        return *this;
    }
    std::string to_lower() const {
        if (value != nullptr) {
            std::string tmp = value;
            std::transform(tmp.begin(), tmp.end(), tmp.begin(), ::tolower);
            return tmp;
        } else {
            return std::string();
        }
    }
    // shallow copy
    String& operator=(std::nullptr_t n) {
        value = nullptr;
        return *this;
    }
    String& operator=(char* str) {
        value = str;
        return *this;
    }
    String& operator=(const char* str) {
        value = (char*)str;
        return *this;
    }

    bool operator==(const String& rhs) {
        if (empty() && rhs.empty()) {
            return true;
        }
        if (empty() || rhs.empty()) {
            return false;
        }
        return strcmp(value, rhs.value) == 0;
    }
    
};
inline std::ostream& operator<<(std::ostream& os, const String& str) {
    if (str.value == nullptr) {
        return os;
    }
    os << str.value;
    return os;
}

struct Node {
    NodeType node_type = NT_BASE;
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

    std::string to_string() const {
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
