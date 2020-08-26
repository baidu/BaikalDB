#!/bin/bash
cd include/sqlparser && /opt/compiler/gcc-4.8.2/bin/flex sql_lex.l && /opt/compiler/gcc-4.8.2/bin/bison sql_parse.y
