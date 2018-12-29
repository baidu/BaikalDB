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
#include "common.h"
#ifdef BAIDU_INTERNAL
#include "uconv.h"
#endif
#include "sql_lex.flex.h"

extern int sql_parse(yyscan_t scanner, parser::SqlParser* parser);
namespace parser {
void SqlParser::change_5c_to_7f(std::string& sql) {
    size_t i = 0;
    while (i < sql.size()) {
        if ((sql[i] & 0x80) != 0) {
            if (++i >= sql.size()) {
                return;
            }
            if (sql[i] == 0x5C) {
                sql[i] = 0x7F; // gbk second byte can not be 0x7F
                has_5c = true;
            }
        }
        ++i;
    }
}
void SqlParser::parse(const std::string& sql_) {
    std::string sql = sql_;
#ifdef BAIDU_INTERNAL
    // 内部编码设置不准，需要自动获取
    // todo: 整理后，去除这个逻辑
    int len = is_utf8_strict(sql.c_str(), sql.size(), true);
    if (len > 0) {
        is_gbk = false;
    } else {
        is_gbk = true;
    }
#else
    if (charset == "gbk") {
        is_gbk = true;
    } else {
        is_gbk = false;
    }
#endif
    if (is_gbk) {
        change_5c_to_7f(sql);
    }
    yyscan_t scanner;
    sql_lex_init(&scanner);
    YY_BUFFER_STATE bp;
    bp = sql__scan_bytes(sql.c_str(), sql.size(), scanner);
    sql__switch_to_buffer(bp, scanner);
    sql_parse(scanner, this);
    sql__delete_buffer(bp, scanner);
    sql_lex_destroy(scanner);
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
