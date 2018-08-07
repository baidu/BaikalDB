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
#include <string>
#include <vector>
#include <iostream>
#include "dml.h"
#include "ddl.h"
#include "misc.h"

namespace parser {
enum ParseError {
    SUCC = 0,
    SYNTAX_ERROR = 1,
};

struct SqlParser {
    std::string sql;
    std::string charset;
    std::string collation;
    std::vector<StmtNode*> result;
    ParseError error = SUCC;
    std::string syntax_err_str;
    butil::Arena arena;
    void parse(const std::string& sql);
};

inline void print_stmt(Node* node, int ii = 0) {
    std::cout << "i:" << ii << " type:" << node->node_type << " ";
    node->print();
    if (node->node_type == NT_EXPR) {
        std::cout << "to_string:" << node << std::endl;
    }
    for (int i = 0; i < node->children.size(); i++) {
        print_stmt(node->children[i], ii + 1);
    }
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
