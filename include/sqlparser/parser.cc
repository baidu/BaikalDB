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

#include "parser.h"
#include "sql_lex.flex.h"

extern int sql_parse(yyscan_t scanner, parser::SqlParser* parser);
namespace parser {
void SqlParser::parse(const std::string& sql) {
    yyscan_t scanner;
    sql_lex_init(&scanner);
    YY_BUFFER_STATE bp;
    //char buf[100] = "insert into t1 (a,b) values (1,2),(3,4);";
    bp = sql__scan_bytes(sql.c_str(), sql.size(), scanner);
    sql__switch_to_buffer(bp, scanner);
    sql_parse(scanner, this);
    sql__delete_buffer(bp, scanner);
    sql_lex_destroy(scanner);
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
