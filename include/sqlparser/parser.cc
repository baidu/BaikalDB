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

#include "parser.h"
#ifdef BAIDU_INTERNAL
#include <bthread.h>
#include "uconv.h"
#include "log.h"
#include <gflags/gflags.h>
#endif
#include "sql_lex.flex.h"

extern int sql_parse(yyscan_t scanner, parser::SqlParser* parser);
namespace parser {
#ifdef BAIDU_INTERNAL
DEFINE_bool(use_is_utf8_strict, true, "is_utf8_strict");
#endif
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
    if (FLAGS_use_is_utf8_strict) {
        int len = is_utf8_strict(sql.c_str(), sql.size(), true);
        if (len > 0) {
            is_gbk = false;
        } else {
            is_gbk = true;
        }
        bool is_asc = true;
        for (char c : sql) {
            if (!IS_ASCII(c)) {
                is_asc = false;
                break;
            }
        }
        if (!is_asc) {
            if (is_gbk != (charset == "gbk")) {
                DB_FATAL("is_utf8_strict fail, sql:%s", sql_.c_str());
            }
        }
    } else {
        if (charset == "gbk") {
            is_gbk = true;
        } else {
            is_gbk = false;
        }
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
    bp->yy_bs_lineno = 1;
    bp->yy_bs_column = 0;
    sql__switch_to_buffer(bp, scanner);
    sql_parse(scanner, this);
    sql__delete_buffer(bp, scanner);
    sql_lex_destroy(scanner);
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
