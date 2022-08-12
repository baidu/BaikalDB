%{
// Copyright 2013 The ql Authors. All rights reserved.  
// Use of this source code is governed by a BSD-style   
// license that can be found in the LICENSES/QL-LICENSE file.   
// Copyright 2015 PingCAP, Inc. 
// Modifications copyright (C) 2018-present, Baidu.com, Inc.    
// Licensed under the Apache License, Version 2.0 (the "License");  
// you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at  
//  
//     http://www.apache.org/licenses/LICENSE-2.0   
//  
// Unless required by applicable law or agreed to in writing, software  
// distributed under the License is distributed on an "AS IS" BASIS,    
// See the License for the specific language governing permissions and  
// limitations under the License.

#include <stdio.h>
#include <iostream>
#define YY_DECL
#include "parser.h"
using parser::SqlParser;
using parser::InsertStmt;
using parser::Node;
using parser::Assignment;
using parser::ExprNode;
using parser::FuncType;
using parser::FuncExpr;
using parser::StmtNode;
using parser::String;
using parser::RowExpr;
using parser::ColumnName;
using parser::TableName;
using parser::PriorityEnum;
using parser::CreateTableStmt;

using namespace parser;
#include "sql_lex.flex.h"
#include "sql_parse.yacc.hh"
extern int sql_lex(YYSTYPE* yylval, YYLTYPE* yylloc, yyscan_t yyscanner, SqlParser* parser);
extern int sql_error(YYLTYPE* yylloc, yyscan_t yyscanner, SqlParser* parser, const char* s);
#define new_node(T) new(parser->arena.allocate(sizeof(T)))T()

%}

%defines
%output "sql_parse.yacc.cc"
%name-prefix "sql_"
%error-verbose
%lex-param {yyscan_t yyscanner}
%lex-param {SqlParser* parser}
%parse-param {yyscan_t yyscanner}
%parse-param {SqlParser* parser}

%token-table
%pure-parser
%verbose

%union
{
    int integer;
    Node* item;  // item can be used as list/array
    ExprNode*       expr;
    StmtNode*       stmt;
    Assignment*     assign;
    String          string;
    Vector<String>* string_list;
    IndexHint*      index_hint;
    SelectStmtOpts* select_opts;
    SelectField*    select_field;
    GroupByClause*  group_by;
    OrderByClause*  order_by;
    ByItem*         by_item;
    LimitClause*    limit;
}

%token 
/* The following tokens belong to ReservedKeyword. */
    ADD
    ALL
    ALTER
    ANALYZE
    AND
    AS
    ASC
    BETWEEN
    BIGINT
    BINARY
    BLOB
    BY
    CASCADE
    CASE
    CHANGE
    CHARACTER
    CHAR
    CHECK
    COLLATE
    COLUMN
    CONSTRAINT
    CONVERT
    CREATE
    CROSS
    CURRENT_USER
    DATABASE
    DATABASES
    DECIMAL
    DEFAULT
    DELAYED
    DELETE
    DESC
    DESCRIBE
    DISTINCT
    DISTINCTROW
    DIV
    DOUBLE
    DROP
    DUAL
    ELSE
    ENCLOSED
    ESCAPED
    EXISTS
    EXPLAIN
    FALSE
    FLOAT
    FOR
    FORCE
    FOREIGN
    FROM
    FULLTEXT
    GENERATED
    GRANT
    GROUP
    HAVING
    HIGH_PRIORITY
    IF
    IGNORE
    IN
    INDEX
    INFILE
    INNER
    INTEGER
    INTERVAL
    INTO
    IS
    INSERT
    INT
    INT1
    INT2
    INT3
    INT4
    INT8
    JOIN
    KEY
    KEYS
    KILL
    LEFT
    LIKE
    EXACT_LIKE
    LIMIT
    LINES
    LOAD
    LOCALTIME
    LOCALTIMESTAMP
    LOCK
    LONGBLOB
    LONGTEXT
    LOW_PRIORITY
    MATCH
    MAXVALUE
    MEDIUMBLOB
    MEDIUMINT
    MEDIUMTEXT
    MOD
    NOT
    NO_WRITE_TO_BINLOG
    NULLX
    NUMERIC
    NVARCHAR
    ON
    OPTION
    OR
    ORDER
    OUTER
    PACK_KEYS
    PARTITION
    PRECISION
    PROCEDURE
    SHARD_ROW_ID_BITS
    RANGE
    READ
    REAL
    REFERENCES
    REGEXP
    RENAME
    REPEAT
    REPLACE
    RESTRICT
    REVOKE
    RIGHT
    RLIKE
    SELECT
    SET
    SHOW
    SMALLINT
    SQL
    SQL_CALC_FOUND_ROWS
    STARTING
    STRAIGHT_JOIN
    TABLE
    STORED
    TERMINATED
    OPTIONALLY
    THEN
    TINYBLOB
    TINYINT
    TINYTEXT
    TO
    TRIGGER
    TRUE
    UNIQUE
    UNION
    UNLOCK
    UNSIGNED
    UPDATE
    USAGE
    USE
    USING
    UTC_DATE
    UTC_TIME
    VALUES
    LONG
    VARCHAR
    VARBINARY
    _BINARY
    VIRTUAL
    WHEN
    WHERE
    WRITE
    WITH
    XOR
    ZEROFILL
    NATURAL
%token<string>
    /* The following tokens belong to ReservedKeywork. */
    CURRENT_DATE
    BOTH
    CURRENT_TIME
    DAY_HOUR
    DAY_MICROSECOND
    DAY_MINUTE
    DAY_SECOND
    HOUR_MICROSECOND
    HOUR_MINUTE
    HOUR_SECOND
    LEADING
    MINUTE_MICROSECOND
    MINUTE_SECOND
    SECOND_MICROSECOND
    TRAILING
    YEAR_MONTH
    PRIMARY

%token<string>
    /* The following tokens belong to UnReservedKeyword. */
    ACTION
    AFTER
    AGAINST
    ALWAYS
    ALGORITHM
    ANY
    ASCII
    AUTO_INCREMENT
    AVG_ROW_LENGTH
    AVG
    BEGINX
    WORK
    BINLOG
    BIT
    BOOLEAN
    BOOL
    BTREE
    BYTE
    CASCADED
    CHARSET
    CHECKSUM
    CLEANUP
    CLIENT
    COALESCE
    COLLATION
    COLUMNS
    COMMENT
    COMMIT
    COMMITTED
    COMPACT
    COMPRESSED
    COMPRESSION
    CONNECTION
    CONSISTENT
    DAY
    DATA
    DATE
    DATETIME
    DEALLOCATE
    DEFINER
    DELAY_KEY_WRITE
    DISABLE
    DO
    DUPLICATE
    DYNAMIC
    ENABLE
    END
    ENGINE
    ENGINES
    ENUM
    EVENT
    EVENTS
    ESCAPE
    EXCLUSIVE
    EXECUTE
    FIELDS
    FIRST
    FIXED
    FLUSH
    FORMAT
    FULL
    FUNCTION
    GRANTS
    HASH
    HOUR
    IDENTIFIED
    ISOLATION
    INDEXES
    INVOKER
    JSON
    KEY_BLOCK_SIZE
    LANGUAGE
    LOCAL
    LESS
    LEVEL
    MASTER
    MICROSECOND
    MINUTE
    MODE
    MODIFY
    MONTH
    MAX_ROWS
    MAX_CONNECTIONS_PER_HOUR
    MAX_QUERIES_PER_HOUR
    MAX_UPDATES_PER_HOUR
    MAX_USER_CONNECTIONS
    MERGE
    MIN_ROWS
    NAMES
    NATIONAL
    NO
    NONE
    OFFSET
    ONLY
    PASSWORD
    PARTITIONS
    PLUGINS
    PREPARE
    PRIVILEGES
    PROCESS
    PROCESSLIST
    PROFILES
    QUARTER
    QUERY
    QUERIES
    QUICK
    RECOVER
    RESTORE 
    REDUNDANT
    RELOAD
    REPEATABLE
    REPLICATION
    REVERSE
    ROLLBACK
    ROUTINE
    ROW
    ROW_COUNT
    ROW_FORMAT
    SECOND
    SECURITY
    SEPARATOR
    SERIALIZABLE
    SESSION
    SHARE
    SHARED
    SIGNED
    SLAVE
    SNAPSHOT
    SQL_CACHE
    SQL_NO_CACHE
    START
    STATS_PERSISTENT
    STATUS
    SUPER
    SOME
    SWAP
    GLOBAL
    TABLES
    TEMPORARY
    TEMPTABLE
    TEXT
    THAN
    TIME
    TIMESTAMP
    TRACE
    TRANSACTION
    TRIGGERS
    TRUNCATE
    UNCOMMITTED
    UNKNOWN
    USER
    UNDEFINED
    VALUE
    VARIABLES
    VIEW
    WARNINGS
    WEEK
    YEAR
    HLL
    BITMAP
    TDIGEST
    LEARNER

    /* The following tokens belong to builtin functions. */
    ADDDATE
    BIT_AND
    BIT_OR
    BIT_XOR
    CAST
    COUNT
    CURDATE
    CURTIME
    CURRENT_TIMESTAMP
    DATE_ADD
    DATE_SUB
    EXTRACT
    GROUP_CONCAT
    MAX
    MID
    MIN
    NOW
    UTC_TIMESTAMP
    POSITION
    SESSION_USER
    STD
    STDDEV
    STDDEV_POP
    STDDEV_SAMP
    SUBDATE
    SUBSTR
    SUBSTRING
    TIMESTAMPADD
    TIMESTAMPDIFF
    SUM
    SYSDATE
    SYSTEM_USER
    TRIM
    VARIANCE
    VAR_POP
    VAR_SAMP
    USER_AGG

%token EQ_OP ASSIGN_OP  MOD_OP  GE_OP  GT_OP LE_OP LT_OP NE_OP AND_OP OR_OP NOT_OP LS_OP RS_OP CHINESE_DOT 
%token <string> IDENT 
%token <expr> STRING_LIT INTEGER_LIT DECIMAL_LIT PLACE_HOLDER_LIT

%type <string> 
    AllIdent 
    TableAsNameOpt 
    TableAsName 
    ConstraintKeywordOpt 
    IndexName 
    StringName 
    OptCharset 
    OptCollate
    DBName
    FunctionNameCurtime
    FunctionaNameCurdate
    FunctionaNameDateRelate
    FunctionNameDateArithMultiForms
    FunctionNameDateArith
    FunctionNameSubstring
    VarName
    AllIdentOrPrimary
    FieldAsNameOpt
    FieldAsName
    ShowDatabaseNameOpt
    ShowTableAliasOpt
    LinesTerminated
    Starting
    ResourceTag

%type <expr> 
    RowExprList
    RowExpr
    ColumnName
    ExprList
    Expr
    ElseOpt
    NumLiteral
    SignedLiteral
    Literal
    SimpleExpr
    FunctionCall
    Operators
    CompareSubqueryExpr
    WhereClause
    WhereClauseOptional
    PredicateOp
    LikeEscapeOpt
    HavingClauseOptional
    BuildInFun
    SumExpr
    TimestampUnit
    FunctionCallNonKeyword
    FunctionCallKeyword
    TimeUnit
    FuncDatetimePrecListOpt
    TrimDirection
    DefaultValue
    ShowLikeOrWhereOpt
    FulltextSearchModifierOpt
    SubSelect
    IsolationLevel

%type <item> 
    ColumnNameListOpt 
    ColumnNameList 
    TableName
    AssignmentList 
    ByList 
    IndexHintListOpt 
    IndexHintList 
    TableNameList
    SelectFieldList
    WhenClauseList
    WhenClause
    Fields
    Lines

%type <item> 
    TableElementList 
    TableElement 
    ColumnDef 
    ColumnOptionList 
    ColumnOption 
    Constraint 
    ConstraintElem 
    Type 
    NumericType 
    StringType 
    BlobType 
    TextType 
    DateAndTimeType 
    FieldOpt 
    FieldOpts 
    FloatOpt 
    Precision 
    StringList 
    TableOption 
    TableOptionList 
    CreateTableOptionListOpt
    DatabaseOption
    DatabaseOptionListOpt
    DatabaseOptionList
    VarAssignItem
    AlterSpecList
    AlterSpec
    ColumnDefList
    IndexOptionList
    IndexOption
    IndexType
    PartitionOpt
    PartitionRangeList
    PartitionRange
    TransactionChar
    FieldItemList
    FieldItem
    ColumnNameOrUserVarListOptWithBrackets
    LoadDataSetSpecOpt

%type <item> OnDuplicateKeyUpdate 
%type <item> 
    EscapedTableRef
    TableRef
    TableRefs 
    TableFactor
    JoinTable

%type <stmt> 
    MultiStmt
    Statement
    InsertStmt
    InsertValues
    ValueList
    UpdateStmt
    ReplaceStmt
    DeleteStmt
    SelectStmtBasic
    SelectStmtFromDual
    SelectStmtFromTable
    SelectStmt
    UnionSelect
    UnionClauseList
    UnionStmt
    TruncateStmt 
    ShowStmt
    ShowTargetFilterable
    ExplainStmt
    ExplainableStmt
    KillStmt

%type <stmt> 
    CreateTableStmt
    DropTableStmt
    RestoreTableStmt
    CreateDatabaseStmt
    DropDatabaseStmt
    StartTransactionStmt
    CommitTransactionStmt
    RollbackTransactionStmt
    SetStmt
    VarAssignList
    AlterTableStmt
    NewPrepareStmt
    ExecPrepareStmt
    DeallocPrepareStmt
    TransactionChars
    LoadDataStmt

%type <assign> Assignment
%type <integer>
    PriorityOpt 
    IgnoreOptional
    QuickOptional
    Order
    IndexHintType
    IndexHintScope
    JoinType
    IfNotExists
    IfExists
    IntegerType
    BooleanType
    FixedPointType
    FloatingPointType
    BitValueType
    OptFieldLen
    FieldLen
    OptBinary
    IsOrNot
    InOrNot
    AnyOrAll
    LikeOrNot
    RegexpOrNot
    BetweenOrNot
    SelectStmtCalcFoundRows
    SelectStmtSQLCache
    SelectStmtStraightJoin
    DistinctOpt
    DefaultFalseDistinctOpt
    DefaultTrueDistinctOpt
    BuggyDefaultFalseDistinctOpt
    SelectLockOpt
    GlobalScope
    OptFull
    UnionOpt
    CompareOp
    IgnoreLines
    DuplicateOpt
    LocalOpt
    ForceOrNot
    GlobalOrLocal

%type <string_list> IndexNameList VarList
%type <index_hint> IndexHint
%type <select_opts> SelectStmtOpts
%type <select_field> SelectField
%type <group_by> GroupBy GroupByOptional
%type <order_by> OrderBy OrderByOptional
%type <by_item> ByItem;
%type <limit> LimitClause;

%nonassoc empty
%nonassoc lowerThanSetKeyword
%nonassoc lowerThanKey
%nonassoc KEY

%left tableRefPriority
%left XOR OR
%left AND
%left EQ_OP NE_OP GE_OP GT_OP LE_OP LT_OP IS LIKE IN 
%left '|'
%left '&'
%left LS_OP RS_OP
%left '+' '-'
%left '*' '/' MOD_OP  MOD
%left '^'
%right '~' NEG NOT NOT_OP
%right '.'
%nonassoc '('
%nonassoc QUICK

%%

// Parse Entrance
MultiStmt:
    Statement {
         parser->result.push_back($1);
    }
    | MultiStmt ';' Statement {
        parser->result.push_back($3);
    }
    | MultiStmt ';' {
        $$ = $1;
    }
    ;

Statement:
    InsertStmt
    | ReplaceStmt
    | UpdateStmt
    | DeleteStmt
    | TruncateStmt
    | CreateTableStmt
    | SelectStmt
    | UnionStmt
    | DropTableStmt
    | RestoreTableStmt
    | CreateDatabaseStmt
    | DropDatabaseStmt
    | StartTransactionStmt
    | CommitTransactionStmt
    | RollbackTransactionStmt
    | SetStmt
    | ShowStmt
    | AlterTableStmt
    | NewPrepareStmt
    | ExecPrepareStmt
    | DeallocPrepareStmt
    | ExplainStmt
    | KillStmt
    | LoadDataStmt
    ;

InsertStmt:
    INSERT PriorityOpt IgnoreOptional IntoOpt TableName InsertValues OnDuplicateKeyUpdate {
        InsertStmt* insert = (InsertStmt*)$6;
        insert->priority = (PriorityEnum)$2;
        insert->is_ignore = (bool)$3;
        insert->table_name = (TableName*)$5;
        if ($7 != nullptr) {
            for (int i = 0; i < $7->children.size(); ++i) {
                (insert->on_duplicate).push_back((Assignment*)$7->children[i], parser->arena);
            }
        }
        $$ = insert;
    }
    ;

ReplaceStmt:
    REPLACE PriorityOpt IntoOpt TableName InsertValues {
        InsertStmt* insert = (InsertStmt*)$5;
        insert->priority = (PriorityEnum)$2;
        insert->is_replace = true;
        insert->table_name = (TableName*)$4;
        $$ = insert;
    }
    ;

PriorityOpt:
    {
        $$ = PE_NO_PRIORITY; 
    }
    | LOW_PRIORITY {
        $$ = PE_LOW_PRIORITY;
    }
    | HIGH_PRIORITY {
        $$ = PE_HIGH_PRIORITY;
    }
    | DELAYED {
        $$ = PE_DELAYED_PRIORITY;
    }
    ;
IgnoreOptional:
    {
        $$ = false; // false
    }
    | IGNORE {
        $$ = true; //true;
    }
    ;
QuickOptional:
    %prec empty {
        $$ = false;
    }
    | QUICK {
        $$ = true;
    }
    ;
IntoOpt:
    {}
    | INTO
    ;

InsertValues:
    '(' ColumnNameListOpt ')' values_sym ValueList {
        InsertStmt* insert = (InsertStmt*)$5;
        if ($2 != nullptr) {
            for (int i = 0; i < $2->children.size(); i++) {
                insert->columns.push_back((ColumnName*)($2->children[i]), parser->arena);
            }
        }
        $$ = insert;
    }
    | '(' ColumnNameListOpt ')' SelectStmt {
        InsertStmt* insert = InsertStmt::New(parser->arena);
        if ($2 != nullptr) {
            for (int i = 0; i < $2->children.size(); i++) {
                insert->columns.push_back((ColumnName*)($2->children[i]), parser->arena);
            }
        }
        insert->subquery_stmt = (SelectStmt*)$4;
        $$ = insert;
    }
    | '(' ColumnNameListOpt ')' '(' SelectStmt ')' {
        InsertStmt* insert = InsertStmt::New(parser->arena);
        if ($2 != nullptr) {
            for (int i = 0; i < $2->children.size(); i++) {
                insert->columns.push_back((ColumnName*)($2->children[i]), parser->arena);
            }
        }
        SelectStmt* select = (SelectStmt*)$5;
        select->is_in_braces = true;
        insert->subquery_stmt = select;
        $$ = insert;
    }
    | '(' ColumnNameListOpt ')' UnionStmt {
        InsertStmt* insert = InsertStmt::New(parser->arena);
        if ($2 != nullptr) {
            for (int i = 0; i < $2->children.size(); i++) {
                insert->columns.push_back((ColumnName*)($2->children[i]), parser->arena);
            }
        }
        insert->subquery_stmt = (UnionStmt*)$4;
        $$ = insert;
    }
    | '(' SelectStmt ')' {
        InsertStmt* insert = InsertStmt::New(parser->arena);
        SelectStmt* select = (SelectStmt*)$2;
        select->is_in_braces = true;
        insert->subquery_stmt = select;
        $$ = insert;
    }
    | SelectStmt {
        InsertStmt* insert = InsertStmt::New(parser->arena);
        insert->subquery_stmt = (SelectStmt*)$1;
        $$ = insert;
    }
    | '(' UnionStmt ')' {
        InsertStmt* insert = InsertStmt::New(parser->arena);
        UnionStmt* union_stmt = (UnionStmt*)$2;
        union_stmt->is_in_braces = true;
        insert->subquery_stmt = union_stmt;
        $$ = insert;
    }
    | UnionStmt {
        InsertStmt* insert = InsertStmt::New(parser->arena);
        insert->subquery_stmt = (UnionStmt*)$1;
        $$ = insert;
    }
    | values_sym ValueList {
        InsertStmt* insert = (InsertStmt*)$2;
        $$ = insert;
    }
    | SET AssignmentList {
        InsertStmt* insert = InsertStmt::New(parser->arena);
        RowExpr* row_expr = new_node(RowExpr);
        for (int i = 0; i < $2->children.size(); i++) {
            Assignment* assign = (Assignment*)$2->children[i];
            insert->columns.push_back(assign->name, parser->arena);
            row_expr->children.push_back(assign->expr, parser->arena);
        }
        insert->lists.push_back(row_expr, parser->arena);
        $$ = insert;
    }
    ;

values_sym:
    VALUE | VALUES
    ;
OnDuplicateKeyUpdate:
    {
        $$ = nullptr;    
    }
    | ON DUPLICATE KEY UPDATE AssignmentList {
        $$ = $5;
    }
    ;
AssignmentList:
    Assignment {
        Node* list = new_node(Node);
        list->children.reserve(10, parser->arena);
        list->children.push_back($1, parser->arena);
        $$ = list;
    }
    | AssignmentList ',' Assignment {
        $1->children.push_back($3, parser->arena);
        $$ = $1;
    }
    ;
Assignment:
    ColumnName EqAssign Expr {
        Assignment* assign = new_node(Assignment);
        assign->name =(ColumnName*)$1;
        assign->expr = $3;
        $$ = assign;
    }
    ;
EqAssign:
    EQ_OP | ASSIGN_OP
    ;

ValueList:
    '(' ExprList ')' {
        InsertStmt* insert = InsertStmt::New(parser->arena);
        insert->lists.push_back((RowExpr*)$2, parser->arena);
        $$ = insert;
    }
    | ValueList ',' '(' ExprList ')' {
        InsertStmt* insert = (InsertStmt*)$1;
        insert->lists.push_back((RowExpr*)$4, parser->arena);
        $$ = insert;
    }
    ;
// for IN_PREDICATE
RowExprList:
    RowExpr {
        RowExpr* row = new_node(RowExpr);
        row->children.reserve(10, parser->arena);
        row->children.push_back($1, parser->arena);
        $$ = row;
    }
    | RowExprList ',' RowExpr {
        RowExpr* row = (RowExpr*)$1;
        row->children.push_back($3, parser->arena);
        $$ = row;
    }
    ;
RowExpr:
    '(' ExprList ',' Expr ')' {
        $2->children.push_back($4, parser->arena);
        $$ = (RowExpr*)$2;
    }
    | ROW '(' ExprList ',' Expr ')' {
        $3->children.push_back($5, parser->arena);
        $$ = (RowExpr*)$3;
    }
    ;
ExprList:
    Expr {
        RowExpr* row = new_node(RowExpr);
        row->children.reserve(10, parser->arena);
        row->children.push_back($1, parser->arena);
        $$ = row;
    } 
    | ExprList ',' Expr {
        RowExpr* row = (RowExpr*)$1;
        row->children.push_back($3, parser->arena);
        $$ = row;
    }
    ;
ColumnNameListOpt:
    /* empty */ {
        $$ = nullptr;
    }
    | ColumnNameList {
        $$ = $1;
    }
    ;

ColumnNameList:
    ColumnName {
        Node* list = new_node(Node);
        list->children.reserve(10, parser->arena);
        list->children.push_back($1, parser->arena);
        $$ = list;
    }
    | ColumnNameList ',' ColumnName {
        $1->children.push_back($3, parser->arena);
        $$ = $1;
     }
    ;

ColumnName:
    AllIdent {
        ColumnName* name = new_node(ColumnName);
        name->name = $1;
        $$ = name;
    }
    | AllIdent '.' AllIdent {
        ColumnName* name = new_node(ColumnName);
        name->table = $1;
        name->name = $3;
        $$ = name;
    }
    | AllIdent '.' AllIdent '.' AllIdent {
        ColumnName* name = new_node(ColumnName);
        name->db = $1;
        name->table = $3;
        name->name = $5;
        $$ = name;
    }
    ;

TableName:
    AllIdent {
        TableName* name = new_node(TableName);
        name->table = $1;
        $$ = name;
    }
    | AllIdent '.' AllIdent {
        TableName* name = new_node(TableName);
        name->db = $1;
        name->table = $3;
        $$ = name;
    } 
    ;

TableNameList:
    TableName {
        Node* list = new_node(Node);
        list->children.reserve(10, parser->arena);
        list->children.push_back($1, parser->arena);
        $$ = list;
    }
    | TableNameList ',' TableName{
        $1->children.push_back($3, parser->arena);
        $$ = $1;
    }
    ;

UpdateStmt:
    UPDATE PriorityOpt IgnoreOptional TableRef SET AssignmentList WhereClauseOptional OrderByOptional LimitClause {
        UpdateStmt* update = new_node(UpdateStmt);
        update->priority = (PriorityEnum)$2;
        update->is_ignore = $3;
        update->table_refs = $4;
        update->set_list.reserve($6->children.size(), parser->arena);
        for (int i = 0; i < $6->children.size(); i++) {
            Assignment* assign = (Assignment*)$6->children[i];
            update->set_list.push_back(assign, parser->arena);
        }
        update->where = $7;
        update->order = $8;
        update->limit = $9;
        $$ = update;
    }
    | UPDATE PriorityOpt IgnoreOptional TableRefs SET AssignmentList WhereClauseOptional {
        UpdateStmt* update = new_node(UpdateStmt);
        update->priority = (PriorityEnum)$2;
        update->is_ignore = $3;
        update->table_refs = $4;
        update->set_list.reserve($6->children.size(), parser->arena);
        for (int i = 0; i < $6->children.size(); i++) {
            Assignment* assign = (Assignment*)$6->children[i];
            update->set_list.push_back(assign, parser->arena);
        }
        update->where = $7;
        $$ = update;
    }
    ;
TruncateStmt:
    TRUNCATE TABLE TableName {
        TruncateStmt* truncate_stmt = new_node(TruncateStmt);
        truncate_stmt->table_name = (TableName*)$3;
        $$ = truncate_stmt;
    }
    | TRUNCATE TableName {
        TruncateStmt* truncate_stmt = new_node(TruncateStmt);
        truncate_stmt->table_name = (TableName*)$2;
        $$ = truncate_stmt;
    }
    ;
DeleteStmt:
    DELETE PriorityOpt QuickOptional IgnoreOptional FROM TableName WhereClauseOptional OrderByOptional LimitClause {
        DeleteStmt* delete_stmt = new_node(DeleteStmt);
        delete_stmt->priority = (PriorityEnum)$2;
        delete_stmt->is_quick = $3;
        delete_stmt->is_ignore = (bool)$4;
        delete_stmt->from_table = $6;
        delete_stmt->where = $7;
        delete_stmt->order = $8;
        delete_stmt->limit = $9;
        $$ = delete_stmt;
    }
    | DELETE PriorityOpt QuickOptional IgnoreOptional TableNameList FROM TableRefs WhereClauseOptional {
        DeleteStmt* delete_stmt = new_node(DeleteStmt);
        delete_stmt->priority = (PriorityEnum)$2;
        delete_stmt->is_quick = (bool)$3;
        delete_stmt->is_ignore = (bool)$4;
        for (int i = 0; i < $5->children.size(); i++) {
            delete_stmt->delete_table_list.push_back((TableName*)$5->children[i], parser->arena);
        }
        delete_stmt->from_table = $7;
        delete_stmt->where = $8;
        $$ = delete_stmt;
    }
    ;

TableRefs:
    EscapedTableRef {
        $$ = $1;    
    }
    | TableRefs ',' EscapedTableRef {
        JoinNode* join_node = new_node(JoinNode);
        join_node->left = $1;
        join_node->right = $3;
        $$ = join_node;    
    }
    ;
EscapedTableRef:
    TableRef %prec lowerThanSetKeyword {
        $$ = $1; 
    }
    | '{' AllIdent TableRef '}' {
        $$ = $3;
    }
    ;
TableRef:
    TableFactor {
        $$ = $1; 
    }
    | JoinTable {
        $$ = $1;
    }
    ;

TableFactor:
    TableName TableAsNameOpt IndexHintListOpt {
        TableSource* table_source = new_node(TableSource);
        table_source->table_name = (TableName*)$1;
        table_source->as_name = $2;
        if ($3 != nullptr) {
            for (int i = 0; i < $3->children.size(); i++) {
                table_source->index_hints.push_back((IndexHint*)($3->children[i]), parser->arena);
            }
        }
        $$ = table_source;
    }
    | '(' SelectStmt ')' TableAsName {
        TableSource* table_source = new_node(TableSource);
        SelectStmt* select = (SelectStmt*)$2;
        select->is_in_braces = true;
        table_source->derived_table = select;
        table_source->as_name = $4;
        $$ = table_source;
    }
    | '(' UnionStmt ')' TableAsName {
        TableSource* table_source = new_node(TableSource);
        UnionStmt* union_stmt = (UnionStmt*)$2;
        union_stmt->is_in_braces = true;
        table_source->derived_table = union_stmt;
        table_source->as_name = $4;
        $$ = table_source;
    }
    | '(' TableRefs ')' {
        $$ = $2; 
    }
    ;

TableAsNameOpt:
    {
        $$ = nullptr;
    }
    | TableAsName {
        $$ = $1; 
    }
    ;

TableAsName:
    {
        $$ = nullptr;                
    }
    | AllIdent {
        $$ = $1;
    }
    | AS AllIdent {
        $$ = $2;
    }
    ;

IndexHintListOpt: 
    {
        $$ = nullptr;                
    }
    | IndexHintList {
        $$ = $1; 
    }
    ;

ForceOrNot:
    {
        $$ = false;
    } 
    | FORCE {
        $$ = true;
    }
    ;

IndexHintList:
    IndexHint {
        Node* list = new_node(Node);
        list->children.reserve(10, parser->arena);
        list->children.push_back($1, parser->arena);
        $$ = list;
    }
    | IndexHintList IndexHint {
        $1->children.push_back($2, parser->arena);
        $$ = $1;
    }
    ;

IndexHint:
    IndexHintType IndexHintScope '(' IndexNameList ')' {
        IndexHint* index_hint = new_node(IndexHint);
        index_hint->hint_type = (IndexHintType)$1;
        index_hint->hint_scope = (IndexHintScope)$2;
        if ($4 != nullptr) {
            for (int i = 0; i < $4->size(); ++i) {
                index_hint->index_name_list.push_back((*$4)[i], parser->arena); 
            }
        }
        $$ = index_hint;
    }
    ;

IndexHintType:
    USE KeyOrIndex {
        $$ = IHT_HINT_USE; 
    }
    | IGNORE KeyOrIndex {
        $$ = IHT_HINT_IGNORE;
    }
    | FORCE KeyOrIndex {
        $$ = IHT_HINT_FORCE;
    }
    ;

KeyOrIndex: 
    KEY | INDEX
    ;

KeyOrIndexOpt:
    {
    }
    | KeyOrIndex
    ;

IndexHintScope:
    {
        $$ = IHS_HINT_SCAN;
    }
    | FOR JOIN {
        $$ = IHS_HINT_JOIN;
    }
    | FOR ORDER BY {
        $$ = IHS_HINT_ORDER_BY;
    }
    | FOR GROUP BY {
        $$ = IHS_HINT_GROUP_BY;
    }
    ;

IndexNameList: {
        $$ = nullptr; 
    }
    | AllIdentOrPrimary {
        Vector<String>* string_list = new_node(Vector<String>);
        string_list->reserve(10, parser->arena);
        string_list->push_back($1, parser->arena);
        $$ = string_list;
    }
    | IndexNameList ',' AllIdentOrPrimary {
        $1->push_back($3, parser->arena);
        $$ = $1;
    }
    ;

AllIdentOrPrimary: 
    AllIdent{
        $$ = $1;
    }
    | PRIMARY {
        $$ = $1;
    }
    ;
JoinTable:
    /* Use %prec to evaluate production TableRef before cross join */
    TableRef CrossOpt TableRef %prec tableRefPriority {
        JoinNode* join_node = new_node(JoinNode);
        join_node->left = $1;
        join_node->right = $3;
        $$ = join_node;
    }
    | TableRef CrossOpt TableRef ON Expr {
        JoinNode* join_node = new_node(JoinNode);
        join_node->left = $1;
        join_node->right = $3;
        join_node->expr = $5;
        $$ = join_node;
    }
    | TableRef CrossOpt TableRef USING '(' ColumnNameList ')' {
        JoinNode* join_node = new_node(JoinNode);
        join_node->left =  $1;
        join_node->right = $3;
        for (int i = 0; i < $6->children.size(); i++) {
            join_node->using_col.push_back((ColumnName*)($6->children[i]), parser->arena);
        }
        $$ = join_node;
    }
    | TableRef JoinType OuterOpt JOIN TableRef ON Expr {
        JoinNode* join_node = new_node(JoinNode);
        join_node->left =  $1;
        join_node->join_type = (JoinType)$2; 
        join_node->right = $5;
        join_node->expr = $7;
        $$ = join_node;
    }
    | TableRef JoinType OuterOpt JOIN TableRef USING '(' ColumnNameList ')' {
        JoinNode* join_node = new_node(JoinNode);
        join_node->left = $1;
        join_node->join_type = (JoinType)$2; 
        join_node->right = $5;
        for (int i = 0; i < $8->children.size(); i++) {
            join_node->using_col.push_back((ColumnName*)($8->children[i]), parser->arena);
        }
        $$ = join_node;
    }
    | TableRef STRAIGHT_JOIN TableRef {
        JoinNode* join_node = new_node(JoinNode);
        join_node->left = $1;
        join_node->right = $3;
        join_node->is_straight = true;
        $$ = join_node;
    }
    | TableRef STRAIGHT_JOIN TableRef ON Expr {
        JoinNode* join_node = new_node(JoinNode);
        join_node->left = $1;
        join_node->right = $3;
        join_node->is_straight = true;
        join_node->expr = $5;
        $$ = join_node;
    }
    | TableRef NATURAL JOIN TableRef {
        JoinNode* join_node = new_node(JoinNode);
        join_node->left = $1;
        join_node->right = $4;
        join_node->is_natural = true;
        $$ = join_node;
    }
    | TableRef NATURAL INNER JOIN TableRef {
        JoinNode* join_node = new_node(JoinNode);
        join_node->left = $1;
        join_node->right = $5;
        join_node->is_natural = true;
        $$ = join_node;
    }
    | TableRef NATURAL JoinType OuterOpt JOIN TableRef {
        JoinNode* join_node = new_node(JoinNode);
        join_node->left = $1;
        join_node->join_type = (JoinType)$3;
        join_node->right = $6;
        join_node->is_natural = true;
        $$ = join_node;
    }
    ;

JoinType:
    LEFT {
        $$ = JT_LEFT_JOIN; 
    }
    | RIGHT {
        $$ = JT_RIGHT_JOIN;
    }
    ;
OuterOpt:
    {}
    | OUTER
    ;

CrossOpt:
    JOIN | CROSS JOIN | INNER JOIN
    ;

LimitClause:
    {
        $$ = nullptr;
    }
    | LIMIT SimpleExpr {
        if ($2->expr_type != parser::ET_LITETAL) {
            sql_error(&@2, yyscanner, parser, "limit expr only support INT_LIT or PLACE_HOLDER");
            return -1;
        }
        LiteralExpr* count = (LiteralExpr*)$2;
        if (count->literal_type != parser::LT_INT && count->literal_type != parser::LT_PLACE_HOLDER) {
            sql_error(&@2, yyscanner, parser, "limit expr only support INT_LIT or PLACE_HOLDER");
            return -1;
        }
        LimitClause* limit = new_node(LimitClause);
        limit->count = count;
        limit->offset = LiteralExpr::make_int("0", parser->arena);
        $$ = limit;
    }
    | LIMIT SimpleExpr ',' SimpleExpr {
        if ($2->expr_type != parser::ET_LITETAL) {
            sql_error(&@2, yyscanner, parser, "limit expr only support INT_LIT or PLACE_HOLDER");
            return -1;
        }
        LiteralExpr* offset = (LiteralExpr*)$2;
        if (offset->literal_type != parser::LT_INT && offset->literal_type != parser::LT_PLACE_HOLDER) {
            sql_error(&@2, yyscanner, parser, "limit expr only support INT_LIT or PLACE_HOLDER");
            return -1;
        }

        if ($4->expr_type != parser::ET_LITETAL) {
            sql_error(&@2, yyscanner, parser, "limit expr only support INT_LIT or PLACE_HOLDER");
            return -1;
        }
        LiteralExpr* count = (LiteralExpr*)$4;
        if (count->literal_type != parser::LT_INT && count->literal_type != parser::LT_PLACE_HOLDER) {
            sql_error(&@2, yyscanner, parser, "limit expr only support INT_LIT or PLACE_HOLDER");
            return -1;
        }
        LimitClause* limit = new_node(LimitClause);
        limit->offset = offset;
        limit->count = count;
        $$ = limit;
    }
    | LIMIT SimpleExpr OFFSET SimpleExpr {
        if ($2->expr_type != parser::ET_LITETAL) {
            sql_error(&@2, yyscanner, parser, "limit expr only support INT_LIT or PLACE_HOLDER");
            return -1;
        }
        LiteralExpr* count = (LiteralExpr*)$2;
        if (count->literal_type != parser::LT_INT && count->literal_type != parser::LT_PLACE_HOLDER) {
            sql_error(&@2, yyscanner, parser, "limit expr only support INT_LIT or PLACE_HOLDER");
            return -1;
        }

        if ($4->expr_type != parser::ET_LITETAL) {
            sql_error(&@2, yyscanner, parser, "limit expr only support INT_LIT or PLACE_HOLDER");
            return -1;
        }
        LiteralExpr* offset = (LiteralExpr*)$4;
        if (offset->literal_type != parser::LT_INT && offset->literal_type != parser::LT_PLACE_HOLDER) {
            sql_error(&@2, yyscanner, parser, "limit expr only support INT_LIT or PLACE_HOLDER");
            return -1;
        }
        LimitClause* limit = new_node(LimitClause);
        limit->offset = offset;
        limit->count = count;
        $$ = limit;
    }
    ;

WhereClause:
    WHERE Expr {
        $$ = $2;
    }
    ;
WhereClauseOptional: 
    {
        $$ = nullptr;
    }
    | WhereClause {
        $$ = $1;
    }
    ;
HavingClauseOptional: 
    {
        $$ = nullptr;
    }
    | HAVING Expr {
        $$ = $2;
    }
    ;
OrderByOptional: 
    {
        $$ = nullptr; 
    }
    | OrderBy {
        $$ = $1;
    }
    ;
OrderBy:
    ORDER BY ByList {
        OrderByClause* order = new_node(OrderByClause);
        for (int i = 0; i < $3->children.size(); i++) {
            order->items.push_back((ByItem*)($3->children[i]), parser->arena);
        }
        $$ = order;
    }
    ;

GroupByOptional:
    {
        $$ = nullptr;
    }
    | GroupBy {
        $$ = $1;
    }
    ;
GroupBy:
    GROUP BY ByList 
    {
        GroupByClause* group = new_node(GroupByClause);
        for (int i = 0; i < $3->children.size(); ++i) {
            group->items.push_back((ByItem*)$3->children[i], parser->arena);
        }
        $$ = group;
    }
    ;
ByList:
    ByItem {
        Node* arr = new_node(Node);
        arr->children.push_back($1, parser->arena);
        $$ = arr;
    }
    | ByList ',' ByItem {
        $1->children.push_back($3, parser->arena);
        $$ = $1;
    }
    ;
ByItem:
    Expr Order 
    {
        ByItem* item = new_node(ByItem);
        item->expr = $1;
        item->is_desc = $2;
        $$ = item;
    }

Order:
    /* EMPTY */ {
        $$ = false; // ASC by default
    }
    | ASC {
        $$ = false;
    }
    | DESC {
        $$ = true;
    }
    ;
SelectStmtOpts:
    DefaultFalseDistinctOpt PriorityOpt SelectStmtStraightJoin  SelectStmtSQLCache SelectStmtCalcFoundRows {
        $$ = new_node(SelectStmtOpts);
        $$->distinct = $1;
        $$->priority = (PriorityEnum)$2;
        $$->straight_join = $3;
        $$->sql_cache = $4;
        $$->calc_found_rows = $5;
    }
    ;
SelectStmtBasic:
    SELECT SelectStmtOpts SelectFieldList {
        SelectStmt* select = new_node(SelectStmt);
        select->select_opt = $2;
        for (int i = 0; i < $3->children.size(); ++i) {
            select->fields.push_back((SelectField*)$3->children[i], parser->arena);
        }
        $$ = select;
    }
    ;
SelectStmtFromDual:
    SelectStmtBasic FromDual WhereClauseOptional {
        SelectStmt* select = (SelectStmt*)$1;
        select->where = $3;
        $$ = select;
    }
    ;
SelectStmtFromTable:
    SelectStmtBasic FROM TableRefs WhereClauseOptional GroupByOptional HavingClauseOptional {
        SelectStmt* select = (SelectStmt*)$1;
        select->table_refs = $3;
        select->where = $4;
        select->group = $5;
        select->having = $6;
        $$ = select;
    } 
    ;

SelectStmt:
    SelectStmtBasic OrderByOptional LimitClause SelectLockOpt {
        SelectStmt* select = (SelectStmt*)$1;
        select->order = $2;
        select->limit = $3;
        select->lock = (SelectLock)$4;
        $$ = select;
    }
    | SelectStmtFromDual LimitClause SelectLockOpt {
        SelectStmt* select = (SelectStmt*)$1;
        select->limit = $2;
        select->lock = (SelectLock)$3;
        $$ = select;
    }
    | SelectStmtFromTable OrderByOptional LimitClause SelectLockOpt {
        SelectStmt* select = (SelectStmt*)$1;
        select->order = $2;
        select->limit = $3;
        select->lock = (SelectLock)$4;
        $$ = select;
    }
    ;
SelectLockOpt:
    /* empty */
    {
        $$ = SL_NONE;
    }
    | FOR UPDATE {
        $$ = SL_FOR_UPDATE;
    }
    | LOCK IN SHARE MODE {
        $$ = SL_IN_SHARE;
    }
    ;
FromDual:
    FROM DUAL
    ;

SelectStmtCalcFoundRows: {
        $$ = false;
    }
    | SQL_CALC_FOUND_ROWS {
        $$ = true;
    }
    ;
SelectStmtSQLCache:
    {
        $$ = false;
    }
    | SQL_CACHE {
        $$ = true;
    }
    | SQL_NO_CACHE {
        $$ = false;
    }
    ;
SelectStmtStraightJoin:
    {
        $$ = false;
    }
    | STRAIGHT_JOIN {
        $$ = true;
    }
    ;
SelectFieldList:
    SelectField {
        Node* list = new_node(Node);
        list->children.reserve(10, parser->arena);
        list->children.push_back($1, parser->arena);
        $$ = list;
    }
    | SelectFieldList ',' SelectField {
        $1->children.push_back($3, parser->arena);
        $$ = $1;
    }
    ;
SelectField:
    '*' {
       SelectField* select_field = new_node(SelectField);
       select_field->wild_card = new_node(WildCardField);
       select_field->wild_card->table_name.set_null();
       select_field->wild_card->db_name.set_null();
       $$ = select_field;
    }
    | AllIdent '.' '*' {
        SelectField* select_field = new_node(SelectField);
        select_field->wild_card = new_node(WildCardField);
        select_field->wild_card->table_name = $1;
        select_field->wild_card->db_name.set_null();
        $$ = select_field;
    }
    | AllIdent '.' AllIdent '.' '*' {
        SelectField* select_field = new_node(SelectField);
        select_field->wild_card = new_node(WildCardField);
        select_field->wild_card->db_name = $1;
        select_field->wild_card->table_name = $3;
        $$ = select_field;
    }
    | Expr FieldAsNameOpt {
        SelectField* select_field = new_node(SelectField);
        select_field->expr = $1;
        select_field->as_name = $2;
        if (select_field->expr->expr_type != ET_COLUMN 
            && select_field->expr->expr_type != ET_LITETAL) {
            select_field->org_name.strdup(@1.start, @1.end - @1.start, parser->arena);
        }
        $$ = select_field;
    }
    | '{' AllIdent Expr '}' FieldAsNameOpt {
        SelectField* select_field = new_node(SelectField);
        select_field->expr = $3;
        select_field->as_name = $5;
        $$ = select_field;
    }
    ;
FieldAsNameOpt:
    /* EMPTY */
    {
        $$ = nullptr;
    }
    | FieldAsName {
        $$ = $1;
    }

FieldAsName:
    AllIdent {
        $$ = $1;
    }
    | AS AllIdent {
        $$ = $2;
    }
    | STRING_LIT {
        $$ = ((LiteralExpr*)$1)->_u.str_val;
    }
    | AS STRING_LIT {
        $$ = ((LiteralExpr*)$2)->_u.str_val;
    }
    ;

SubSelect:
    '(' SelectStmt ')' {
        SubqueryExpr* sub_query = new_node(SubqueryExpr);
        sub_query->query_stmt = (SelectStmt*)$2;
        $$ = sub_query;
    }
    | '(' UnionStmt ')' {
        SubqueryExpr* sub_query = new_node(SubqueryExpr);
        sub_query->query_stmt = (UnionStmt*)$2;
        $$ = sub_query;
    }
    ;

// See https://dev.mysql.com/doc/refman/5.7/en/union.html
UnionStmt:
    UnionClauseList UNION UnionOpt SelectStmtBasic OrderByOptional LimitClause SelectLockOpt {
        UnionStmt* union_stmt = (UnionStmt*)$1;
        if (!union_stmt->distinct) {
            union_stmt->distinct = $3;
        }
        union_stmt->select_stmts.push_back((SelectStmt*)$4, parser->arena);
        union_stmt->order = $5;
        union_stmt->limit = $6;        
        union_stmt->lock = (SelectLock)$7;
        $$ = union_stmt;
    }
    | UnionClauseList UNION UnionOpt SelectStmtFromTable OrderByOptional LimitClause SelectLockOpt {
        UnionStmt* union_stmt = (UnionStmt*)$1;
        if (!union_stmt->distinct) {
            union_stmt->distinct = $3;
        }
        union_stmt->select_stmts.push_back((SelectStmt*)$4, parser->arena);
        union_stmt->order = $5;
        union_stmt->limit = $6;        
        union_stmt->lock = (SelectLock)$7;
        $$ = union_stmt;
    }
    | UnionClauseList UNION UnionOpt '(' SelectStmt ')' OrderByOptional LimitClause SelectLockOpt {
        UnionStmt* union_stmt = (UnionStmt*)$1;
        if (!union_stmt->distinct) {
            union_stmt->distinct = $3;
        }
        SelectStmt* select = (SelectStmt*)$5;
        select->is_in_braces = true;
        union_stmt->select_stmts.push_back(select, parser->arena);
        union_stmt->order = $7;
        union_stmt->limit = $8;
        union_stmt->lock = (SelectLock)$9;
        $$ = union_stmt;
    }
    ;

UnionClauseList:
    UnionSelect {
        UnionStmt* union_stmt = new_node(UnionStmt);
        union_stmt->select_stmts.push_back((SelectStmt*)$1, parser->arena);
        $$ = union_stmt;
    }
    | UnionClauseList UNION UnionOpt UnionSelect {
        UnionStmt* union_stmt = (UnionStmt*)$1;
        if (!union_stmt->distinct) {
            union_stmt->distinct = $3;
        }
        union_stmt->select_stmts.push_back((SelectStmt*)$4, parser->arena);
        $$ = union_stmt;
    }
    ;

UnionSelect:
    SelectStmt {
        $$ = $1;
    }
    | '(' SelectStmt ')' {
        SelectStmt* select = (SelectStmt*)$2;
        select->is_in_braces = true;
        $$ = select;
    }
    ;

UnionOpt:
    DefaultTrueDistinctOpt
    ;

/*Expr*/
Expr:
    Operators { $$ = $1;}
    | PredicateOp { $$ = $1;}
    | SimpleExpr { $$ = $1;}
    ;

FunctionCall:
    BuildInFun {
        $$ = $1;
    }
    | IDENT '(' ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = $1;
        $$ = fun;
    }
    | IDENT '(' ExprList ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = $1;
        fun->children = $3->children;
        $$ = fun;
    }
    ;

BuildInFun:
    SumExpr
    | FunctionCallNonKeyword 
    | FunctionCallKeyword
    ;
FuncDatetimePrecListOpt:
    {
        $$ = nullptr;
    }
    | INTEGER_LIT {
        $$ = $1;        
    } 
    ;
FunctionNameDateArithMultiForms:
    ADDDATE | SUBDATE
    ;
FunctionNameDateArith:
     DATE_ADD | DATE_SUB
     ;
FunctionNameSubstring:
    SUBSTR | SUBSTRING
    ;

TimestampUnit:
    MICROSECOND {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    }
    | SECOND {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    }
    | MINUTE {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    }
    | HOUR {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    }
    | DAY {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    }
    | WEEK {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    } 
    | MONTH {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    } 
    | QUARTER {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    }
    | YEAR {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    }
    ;

TimeUnit:
    MICROSECOND {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    }
    | SECOND {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    } 
    | MINUTE {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    } 
    | HOUR {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    }
    | DAY {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    } 
    | WEEK {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    }
    | MONTH {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    }
    | QUARTER {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    }
    | YEAR {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    }
    | SECOND_MICROSECOND {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    }
    | MINUTE_MICROSECOND {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    }
    | MINUTE_SECOND {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    }
    | HOUR_MICROSECOND {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    }
    | HOUR_SECOND {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    }
    | HOUR_MINUTE {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    }
    | DAY_MICROSECOND {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    }
    | DAY_SECOND {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    }
    | DAY_MINUTE {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    }
    | DAY_HOUR {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    }
    | YEAR_MONTH {
        $$ = LiteralExpr::make_string($1.to_lower_inplace(), parser->arena);
    }
    ;

TrimDirection:
    BOTH {
        $$ = LiteralExpr::make_string($1, parser->arena);
    }
    | LEADING {
        $$ = LiteralExpr::make_string($1, parser->arena);
    }
    | TRAILING {
        $$ = LiteralExpr::make_string($1, parser->arena);
    }
    ;

FunctionNameCurtime:
    CURTIME | CURRENT_TIME
    ;

FunctionaNameCurdate:
    CURDATE | CURRENT_DATE
    ;

FunctionaNameDateRelate:
    DAY | MONTH | YEAR
    ;

FunctionCallNonKeyword: 
    FunctionNameCurtime '(' FuncDatetimePrecListOpt ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = $1;
        if ($3 != nullptr) {
            fun->children.push_back($3, parser->arena);
        }
        $$ = fun;
    }
    | CURRENT_TIME {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = $1;
        $$ = fun; 
    }
    | FunctionaNameCurdate '(' ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = $1;
        $$ = fun; 
    }
    | CURRENT_DATE {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = $1;
        $$ = fun; 
    }
    | FunctionaNameDateRelate '(' Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = $1;
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | WEEK '(' Expr ',' Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = $1;
        fun->children.push_back($3, parser->arena);
        fun->children.push_back($5, parser->arena);
        $$ = fun;      
    }
    | WEEK '(' Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = $1;
        fun->children.push_back($3, parser->arena);
        $$ = fun;         
    }
    | SYSDATE '(' FuncDatetimePrecListOpt ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = $1;
        if ($3 != nullptr) {
            fun->children.push_back($3, parser->arena);
        }
        $$ = fun;
    }
    | NOW '(' ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = $1;
        $$ = fun; 
    }
    | NOW '(' INTEGER_LIT ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = $1;
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | FunctionNameCurTimestamp {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "current_timestamp"; 
        $$ = fun; 
    }
    | FunctionNameCurTimestamp '(' ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "current_timestamp"; 
        $$ = fun; 
    }
    | FunctionNameCurTimestamp '(' INTEGER_LIT ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "current_timestamp"; 
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | UTC_TIMESTAMP '(' ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = $1;
        $$ = fun;
    }
    | TIMESTAMP '(' ExprList ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = $1;
        fun->children = $3->children;
        $$ = fun;
    }
    | FunctionNameDateArithMultiForms '(' Expr ',' Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = $1;
        fun->children.push_back($3, parser->arena);
        fun->children.push_back($5, parser->arena);
        LiteralExpr* day_expr = LiteralExpr::make_string("DAY", parser->arena);
        fun->children.push_back(day_expr, parser->arena);
        $$ = fun;
    }
    | FunctionNameDateArithMultiForms '(' Expr ',' INTERVAL Expr TimeUnit ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = $1;
        fun->children.push_back($3, parser->arena);
        fun->children.push_back($6, parser->arena);
        fun->children.push_back($7, parser->arena);
        $$ = fun;
    }
    | FunctionNameDateArith '(' Expr ',' INTERVAL Expr TimeUnit ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = $1;
        fun->children.push_back($3, parser->arena);
        fun->children.push_back($6, parser->arena);
        fun->children.push_back($7, parser->arena);
        $$ = fun;
    }
    | CONVERT '(' Expr ',' DATE ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "cast_to_date";
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | CAST '(' Expr AS DATE ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "cast_to_date";
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | CONVERT '(' Expr ',' DATETIME ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "cast_to_datetime";
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | CAST '(' Expr AS DATETIME ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "cast_to_datetime";
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | CONVERT '(' Expr ',' TIME ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "cast_to_time";
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | CAST '(' Expr AS TIME ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "cast_to_time";
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | CONVERT '(' Expr ',' CHAR ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "cast_to_string";
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | CAST '(' Expr AS CHAR ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "cast_to_string";
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | CONVERT '(' Expr ',' SIGNED INTEGER')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "cast_to_signed";
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | CONVERT '(' Expr ',' SIGNED ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "cast_to_signed";
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | CAST '(' Expr AS SIGNED INTEGER')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "cast_to_signed";
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | CAST '(' Expr AS SIGNED ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "cast_to_signed";
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | CONVERT '(' Expr ',' UNSIGNED INTEGER')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "cast_to_unsigned";
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | CONVERT '(' Expr ',' UNSIGNED')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "cast_to_unsigned";
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | CAST '(' Expr AS UNSIGNED INTEGER')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "cast_to_unsigned";
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | CAST '(' Expr AS UNSIGNED ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "cast_to_unsigned";
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | CONVERT '(' Expr ',' BINARY ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "cast_to_string";
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | CAST '(' Expr AS BINARY ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "cast_to_string";
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | USER '(' ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = $1;
        $$ = fun; 
    }
    | SYSTEM_USER '(' ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = $1;
        $$ = fun; 
    }
    | SESSION_USER '(' ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = $1;
        $$ = fun; 
    }
    | EXTRACT '(' TimeUnit FROM Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = $1;
        fun->children.push_back($3, parser->arena);
        fun->children.push_back($5, parser->arena);
        $$ = fun;
    }
    | POSITION '(' Expr IN Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = $1;
        fun->children.push_back($3, parser->arena);
        fun->children.push_back($5, parser->arena);
        $$ = fun;
    }
    | FunctionNameSubstring '(' Expr ',' Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "substr";
        fun->children.push_back($3, parser->arena);
        fun->children.push_back($5, parser->arena);
        $$ = fun;
    }
    | FunctionNameSubstring '(' Expr FROM Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "substr";
        fun->children.push_back($3, parser->arena);
        fun->children.push_back($5, parser->arena);
        $$ = fun;
    }
    | FunctionNameSubstring '(' Expr ',' Expr ',' Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "substr";
        fun->children.push_back($3, parser->arena);
        fun->children.push_back($5, parser->arena);
        fun->children.push_back($7, parser->arena);
        $$ = fun;
    }
    | FunctionNameSubstring '(' Expr FROM Expr FOR Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "substr";
        fun->children.push_back($3, parser->arena);
        fun->children.push_back($5, parser->arena);
        fun->children.push_back($7, parser->arena);
        $$ = fun;
    }
    | TIMESTAMPADD '(' TimestampUnit ',' Expr ',' Expr ')' {
        $$ = nullptr;
    }
    | TIMESTAMPDIFF '(' TimestampUnit ',' Expr ',' Expr ')' {
        FuncExpr* fun = FuncExpr::new_ternary_op_node(FT_COMMON, $3, $5, $7, parser->arena);
        fun->fn_name = "timestampdiff";
        $$ = fun;
    }
    | TRIM '(' Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = $1;
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | TRIM '(' Expr FROM Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = $1;
        fun->children.push_back($5, parser->arena);
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | TRIM '(' TrimDirection FROM Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = $1;
        fun->children.push_back($5, parser->arena);
        fun->children.push_back(nullptr, parser->arena);
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | TRIM '(' TrimDirection Expr FROM Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = $1;
        fun->children.push_back($6, parser->arena);
        fun->children.push_back($4, parser->arena);
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | MOD '(' Expr ',' Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "mod";
        fun->children.push_back($3, parser->arena);
        fun->children.push_back($5, parser->arena);
        $$ = fun;
    }
    ;
FunctionCallKeyword:
    VALUES '(' ColumnName ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "values";
        fun->func_type = FT_VALUES;
        fun->children.push_back($3, parser->arena);
        $$ = fun; 
    }
    | LEFT '(' Expr ',' Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "left";
        fun->func_type = FT_COMMON;
        fun->children.push_back($3, parser->arena);
        fun->children.push_back($5, parser->arena);
        $$ = fun; 
    }
    | RIGHT '(' Expr ',' Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "right";
        fun->func_type = FT_COMMON;
        fun->children.push_back($3, parser->arena);
        fun->children.push_back($5, parser->arena);
        $$ = fun; 
    }
    | REPLACE '(' Expr ',' Expr ',' Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "replace";
        fun->func_type = FT_COMMON;
        fun->children.push_back($3, parser->arena);
        fun->children.push_back($5, parser->arena);
        fun->children.push_back($7, parser->arena);
        $$ = fun; 
    }
    | IF '(' Expr ',' Expr ',' Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "if";
        fun->func_type = FT_COMMON;
        fun->children.push_back($3, parser->arena);
        fun->children.push_back($5, parser->arena);
        fun->children.push_back($7, parser->arena);
        $$ = fun; 
    }
    | DATABASE '(' ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "database";
        $$ = fun;
    }
    | DEFAULT '(' ColumnName ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "default";
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    ;
SumExpr:
    AVG '(' BuggyDefaultFalseDistinctOpt Expr')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->func_type = FT_AGG;
        fun->fn_name = $1;
        fun->distinct = $3;
        fun->children.push_back($4, parser->arena);
        $$ = fun;    
    }
    | BIT_AND '(' Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->func_type = FT_AGG;
        fun->fn_name = $1;
        fun->distinct = false;
        fun->children.push_back($3, parser->arena);
        $$ = fun; 
    }
    | BIT_AND '(' ALL Expr ')' { 
        FuncExpr* fun = new_node(FuncExpr);
        fun->func_type = FT_AGG;
        fun->fn_name = $1;
        fun->distinct = false;
        fun->children.push_back($4, parser->arena);
        $$ = fun;
    }
    | BIT_OR '(' Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->func_type = FT_AGG;
        fun->fn_name = $1;
        fun->distinct = false;
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | BIT_OR '(' ALL Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->func_type = FT_AGG;
        fun->fn_name = $1;
        fun->distinct = false;
        fun->children.push_back($4, parser->arena);
        $$ = fun; 
    }
    | BIT_XOR '(' Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->func_type = FT_AGG;
        fun->fn_name = $1;
        fun->distinct = false;
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | BIT_XOR '(' ALL Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->func_type = FT_AGG;
        fun->fn_name = $1;
        fun->distinct = false;
        fun->children.push_back($4, parser->arena);
        $$ = fun;
    }
    | COUNT '(' DistinctKwd ExprList ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->func_type = FT_AGG;
        fun->fn_name = $1;
        fun->distinct = true;
        fun->children = $4->children;
        $$ = fun;
    }
    | COUNT '(' ALL Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->func_type = FT_AGG;
        fun->fn_name = $1;
        fun->distinct = false;
        fun->children.push_back($4, parser->arena);
        $$ = fun;
    }
    | COUNT '(' Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->func_type = FT_AGG;
        fun->fn_name = $1;
        fun->distinct = false;
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | COUNT '(' '*' ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->func_type = FT_AGG;
        fun->fn_name = $1;
        fun->distinct = false; 
        fun->is_star = true;
        $$ = fun;
    }
    | MAX '(' BuggyDefaultFalseDistinctOpt Expr')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->func_type = FT_AGG;
        fun->fn_name = $1;
        fun->distinct = $3;
        fun->children.push_back($4, parser->arena);
        $$ = fun;
    }
    | MIN '(' BuggyDefaultFalseDistinctOpt Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->func_type = FT_AGG;
        fun->fn_name = $1;
        fun->distinct = $3;
        fun->children.push_back($4, parser->arena);
        $$ = fun;
    }
    | SUM '(' BuggyDefaultFalseDistinctOpt Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->func_type = FT_AGG;
        fun->fn_name = $1;
        fun->distinct = $3;
        fun->children.push_back($4, parser->arena);
        $$ = fun;
    }
    | GROUP_CONCAT '(' Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->func_type = FT_AGG;
        fun->fn_name = $1;
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    | USER_AGG '(' Expr ')' {
        FuncExpr* fun = new_node(FuncExpr);
        fun->func_type = FT_AGG;
        fun->fn_name = $1;
        fun->children.push_back($3, parser->arena);
        $$ = fun;
    }
    ;

DistinctKwd:
    DISTINCT
    | DISTINCTROW
    ;

DistinctOpt:
    ALL {
        $$ = false;
    }
    | DistinctKwd {
        $$ = true;
    }
    ;

DefaultFalseDistinctOpt:
    {
        $$ = false;
    }
    | DistinctOpt
    ;

DefaultTrueDistinctOpt:
    {
        $$ = true;
    }
    | DistinctOpt
    ;

BuggyDefaultFalseDistinctOpt:
    DefaultFalseDistinctOpt
    | DistinctKwd ALL {
        $$ = true;
    }
    ;

AllIdent:
    IDENT {}
    // UnReservedKeyword 
    | ACTION
    | AFTER
    | ALWAYS
    | ALGORITHM
    | ANY
    | ASCII
    | AUTO_INCREMENT
    | AVG_ROW_LENGTH
    | AVG
    | BEGINX
    | WORK
    | BINLOG
    | BIT
    | BOOLEAN
    | BOOL
    | BTREE
    | BYTE
    | CASCADED
    | CHARSET
    | CHECKSUM
    | CLEANUP
    | CLIENT
    | COALESCE
    | COLLATION
    | COLUMNS
    | COMMENT
    | COMMIT
    | COMMITTED
    | COMPACT
    | COMPRESSED
    | COMPRESSION
    | CONNECTION
    | CONSISTENT
    | DAY
    | DATA
    | DATE
    | DATETIME
    | DEALLOCATE
    | DEFINER
    | DELAY_KEY_WRITE
    | DISABLE
    | DO
    | DUPLICATE
    | DYNAMIC
    | ENABLE
    | END
    | ENGINE
    | ENGINES
    | ENUM
    | EVENT
    | EVENTS
    | ESCAPE
    | EXCLUSIVE
    | EXECUTE
    | FIELDS
    | FIRST
    | FIXED
    | FLUSH
    | FORMAT
    | FULL
    | FUNCTION
    | GRANTS
    | HASH
    | HOUR
    | IDENTIFIED
    | ISOLATION
    | INDEXES
    | INVOKER
    | JSON
    | KEY_BLOCK_SIZE
    | LANGUAGE
    | LOCAL
    | LESS
    | LEVEL
    | MASTER
    | MICROSECOND
    | MINUTE
    | MODE
    | MODIFY
    | MONTH
    | MAX_ROWS
    | MAX_CONNECTIONS_PER_HOUR
    | MAX_QUERIES_PER_HOUR
    | MAX_UPDATES_PER_HOUR
    | MAX_USER_CONNECTIONS
    | MERGE
    | MIN_ROWS
    | NAMES
    | NATIONAL
    | NO
    | NONE
    | OFFSET
    | ONLY
    | PASSWORD
    | PARTITIONS
    | PLUGINS
    | PREPARE
    | PRIVILEGES
    | PROCESS
    | PROCESSLIST
    | PROFILES
    | QUARTER
    | QUERY
    | QUERIES
    | QUICK
    | RECOVER
    | RESTORE
    | REDUNDANT
    | RELOAD
    | REPEATABLE
    | REPLICATION
    | REVERSE
    | ROLLBACK
    | ROUTINE
    | ROW
    | ROW_COUNT
    | ROW_FORMAT
    | SECOND
    | SECURITY
    | SEPARATOR
    | SERIALIZABLE
    | SESSION
    | SHARE
    | SHARED
    | SIGNED
    | SLAVE
    | SNAPSHOT
    | SQL_CACHE
    | SQL_NO_CACHE
    | START
    | STATS_PERSISTENT
    | STATUS
    | SUPER
    | SOME
    | GLOBAL
    | TABLES
    | TEMPORARY
    | TEMPTABLE
    | TEXT
    | THAN
    | TIME
    | TIMESTAMP
    | TRACE
    | TRANSACTION
    | TRIGGERS
    | TRUNCATE
    | UNCOMMITTED
    | UNKNOWN
    | USER
    | UNDEFINED
    | VALUE
    | VARIABLES
    | VIEW
    | WARNINGS
    | WEEK
    | YEAR
    /* builtin functions. */
    | ADDDATE
    | BIT_AND
    | BIT_OR
    | BIT_XOR
    | CAST
    | COUNT
    | CURDATE
    | CURTIME
    | DATE_ADD
    | DATE_SUB
    | EXTRACT
    | GROUP_CONCAT
    | MAX
    | MID
    | MIN
    | NOW
    | CURRENT_TIMESTAMP
    | UTC_TIMESTAMP
    | POSITION
    | SESSION_USER
    | STD
    | STDDEV
    | STDDEV_POP
    | STDDEV_SAMP
    | SUBDATE
    | SUBSTR
    | SUBSTRING
    | TIMESTAMPADD
    | TIMESTAMPDIFF 
    | SUM
    | SYSDATE
    | SYSTEM_USER
    | TRIM
    | VARIANCE
    | VAR_POP
    | VAR_SAMP
    ;

NumLiteral:
    INTEGER_LIT {
    }
    | DECIMAL_LIT {
    }
    ;

Literal:
    NULLX {
        $$ = LiteralExpr::make_null(parser->arena);
    }
    | TRUE {
        $$ = LiteralExpr::make_true(parser->arena);
    }
    | FALSE {
        $$ = LiteralExpr::make_false(parser->arena);
    }
    | NumLiteral {
    }
    | STRING_LIT {
    }
    | _BINARY STRING_LIT {
        $$ = $2;
    }
    ;

SimpleExpr:
    Literal {
    }
    | PLACE_HOLDER_LIT {
    }
    | ColumnName {
    }
    | RowExpr {
    }
    | FunctionCall {
    }
    | SubSelect { 
    }
    | EXISTS SubSelect {
        ExistsSubqueryExpr*  exists_expr = new_node(ExistsSubqueryExpr);
        exists_expr->query_expr = (SubqueryExpr*)$2;
        $$ = exists_expr;
    }
    | '-' SimpleExpr %prec NEG {
        $$ = FuncExpr::new_unary_op_node(FT_UMINUS, $2, parser->arena);
    }
    | '+' SimpleExpr %prec NEG { 
        $$ = $2;
    }
    | NOT SimpleExpr {
        $$ = FuncExpr::new_unary_op_node(FT_LOGIC_NOT, $2, parser->arena);
    }
    | NOT_OP SimpleExpr {
        $$ = FuncExpr::new_unary_op_node(FT_LOGIC_NOT, $2, parser->arena);
    }
    | MATCH '(' ColumnNameList ')' AGAINST '(' SimpleExpr FulltextSearchModifierOpt ')' {
        RowExpr* row = new_node(RowExpr);
        row->children.reserve($3->children.size(), parser->arena);
        for (int i = 0; i < $3->children.size(); i++) {
            row->children.push_back($3->children[i], parser->arena);
        }
        $$ = FuncExpr::new_ternary_op_node(FT_MATCH_AGAINST, row, $7, $8, parser->arena);
    }
    | '~' SimpleExpr {
        $$ = FuncExpr::new_unary_op_node(FT_BIT_NOT, $2, parser->arena);
    }
    | '(' Expr ')' {
        $$ = $2;
    }
    | CASE Expr WhenClauseList ElseOpt END {
        FuncExpr* fun = new_node(FuncExpr);  
        fun->fn_name = "case_expr_when";
        fun->children.push_back($2, parser->arena);
        for (int i = 0; i < $3->children.size(); i++) {
            fun->children.push_back($3->children[i], parser->arena);
        }
        if ($4 != nullptr) {      
            fun->children.push_back($4, parser->arena);
        }
        $$ = fun;
    }
    | CASE WhenClauseList ElseOpt END {
        FuncExpr* fun = new_node(FuncExpr);
        fun->fn_name = "case_when";
        for (int i = 0; i < $2->children.size(); i++) {
            fun->children.push_back($2->children[i], parser->arena);
        }   
        if ($3 != nullptr) {       
            fun->children.push_back($3, parser->arena);
        }
        $$ = fun;
    }
    ;

FulltextSearchModifierOpt:
    {
        $$ = LiteralExpr::make_string("IN NATURAL LANGUAGE MODE", parser->arena);
    }
    | IN NATURAL LANGUAGE MODE {
        $$ = LiteralExpr::make_string("IN NATURAL LANGUAGE MODE", parser->arena);
    }
    | IN BOOLEAN MODE {
        $$ = LiteralExpr::make_string("IN BOOLEAN MODE", parser->arena);
    }
    ;

WhenClauseList:
    WhenClause {
        Node* list = new_node(Node);
        list->children.reserve(10, parser->arena);
        for (int i = 0; i < $1->children.size(); i++) {
            list->children.push_back($1->children[i], parser->arena);
        }
        $$ = list;
    }
    | WhenClauseList WhenClause {
        for (int i = 0; i < $2->children.size(); i++) {
            $1->children.push_back($2->children[i], parser->arena);
        }
        $$ = $1;
    }
    ;
WhenClause:
    WHEN Expr THEN Expr {
        Node* list = new_node(Node);
        list->children.reserve(2, parser->arena);
        list->children.push_back($2, parser->arena);
        list->children.push_back($4, parser->arena);
        $$ = list;
    }
    ;
ElseOpt:
    {
        $$ = nullptr;
    }
    | ELSE Expr {
        $$ = $2;
    }
    ;

Operators:
    CompareSubqueryExpr {
        $$ = $1;
    }
    | Expr '+' Expr {
        $$ = FuncExpr::new_binary_op_node(FT_ADD, $1, $3, parser->arena);
    }
    | Expr '-' Expr {
        $$ = FuncExpr::new_binary_op_node(FT_MINUS, $1, $3, parser->arena);
    }
    | Expr '*' Expr {
        $$ = FuncExpr::new_binary_op_node(FT_MULTIPLIES, $1, $3, parser->arena);
    }
    | Expr '/' Expr {
        $$ = FuncExpr::new_binary_op_node(FT_DIVIDES, $1, $3, parser->arena);
    }
    | Expr MOD Expr {
        $$ = FuncExpr::new_binary_op_node(FT_MOD, $1, $3, parser->arena);
    } 
    | Expr MOD_OP Expr {
        $$ = FuncExpr::new_binary_op_node(FT_MOD, $1, $3, parser->arena);
    } 
    | Expr LS_OP Expr {
        $$ = FuncExpr::new_binary_op_node(FT_LS, $1, $3, parser->arena);
    }
    | Expr RS_OP Expr {
        $$ = FuncExpr::new_binary_op_node(FT_RS, $1, $3, parser->arena);
    }
    | Expr '&' Expr {
        $$ = FuncExpr::new_binary_op_node(FT_BIT_AND, $1, $3, parser->arena);
    }
    | Expr '|' Expr {
        $$ = FuncExpr::new_binary_op_node(FT_BIT_OR, $1, $3, parser->arena);
    }
    | Expr '^' Expr {
        $$ = FuncExpr::new_binary_op_node(FT_BIT_XOR, $1, $3, parser->arena);
    }
    | Expr CompareOp Expr %prec EQ_OP {
        $$ = FuncExpr::new_binary_op_node((FuncType)$2, $1, $3, parser->arena);
    }
    | Expr AND Expr {
        $$ = FuncExpr::new_binary_op_node(FT_LOGIC_AND, $1, $3, parser->arena);
    }
    | Expr OR Expr {
        $$ = FuncExpr::new_binary_op_node(FT_LOGIC_OR, $1, $3, parser->arena);
    }
    | Expr XOR Expr {
        $$ = FuncExpr::new_binary_op_node(FT_LOGIC_XOR, $1, $3, parser->arena);
    }
    ;

CompareOp:
    GE_OP {
        $$ = FT_GE;
    }
    | GT_OP {
        $$ = FT_GT;
    }
    | LE_OP {
        $$ = FT_LE;
    }
    | LT_OP {
        $$ = FT_LT;
    }
    | NE_OP {
        $$ = FT_NE;
    }
    | EQ_OP {
        $$ = FT_EQ;
    }
    ;

CompareSubqueryExpr:
    Expr CompareOp AnyOrAll SubSelect %prec EQ_OP {
        CompareSubqueryExpr* comp_sub_query = new_node(CompareSubqueryExpr);
        comp_sub_query->left_expr = $1;
        comp_sub_query->func_type = (FuncType)$2;
        comp_sub_query->cmp_type = (parser::CompareType)$3;
        comp_sub_query->right_expr = (SubqueryExpr*)$4;
        $$ = comp_sub_query;
    }

PredicateOp:
    Expr IsOrNot NULLX %prec IS {
        FuncExpr* fun = FuncExpr::new_unary_op_node(FT_IS_NULL, $1, parser->arena);
        fun->is_not = $2;
        $$ = fun;
    }
    | Expr IsOrNot TRUE %prec IS {
        FuncExpr* fun = FuncExpr::new_unary_op_node(FT_IS_TRUE, $1, parser->arena);
        fun->is_not = $2;
        $$ = fun;
    }
    | Expr IsOrNot FALSE %prec IS {
        FuncExpr* fun = FuncExpr::new_unary_op_node(FT_IS_TRUE, $1, parser->arena);
        fun->is_not = !$2;
        $$ = fun;
    }
    | Expr IsOrNot UNKNOWN %prec IS {
        FuncExpr* fun = FuncExpr::new_unary_op_node(FT_IS_UNKNOWN, $1, parser->arena);
        fun->is_not = $2;
        $$ = fun;
    }
    | SimpleExpr InOrNot '(' ExprList ')' %prec IN {
        FuncExpr* fun = FuncExpr::new_binary_op_node(FT_IN, $1, $4, parser->arena);
        fun->is_not = $2;
        $$ = fun;
    }
    | RowExpr InOrNot '(' RowExprList ')' %prec IN {
        FuncExpr* fun = FuncExpr::new_binary_op_node(FT_IN, $1, $4, parser->arena);
        fun->is_not = $2;
        $$ = fun;
    }
    | RowExpr InOrNot SubSelect {
        FuncExpr* fun = FuncExpr::new_binary_op_node(FT_IN, $1, $3, parser->arena);
        fun->is_not = $2;
        $$ = fun;
    }
    | SimpleExpr InOrNot SubSelect {
        FuncExpr* fun = FuncExpr::new_binary_op_node(FT_IN, $1, $3, parser->arena);
        fun->is_not = $2;
        $$ = fun;
    }
    /*
    | SimpleExpr LikeOrNot SubSelect LikeEscapeOpt %prec LIKE {
        FuncExpr* fun = FuncExpr::new_ternary_op_node(FT_LIKE, $1, $3, $4, parser->arena);
        fun->is_not = $2;
        $$ = fun;
    }*/
    | SimpleExpr LikeOrNot SimpleExpr LikeEscapeOpt %prec LIKE {
        FuncExpr* fun = FuncExpr::new_ternary_op_node(FT_LIKE, $1, $3, $4, parser->arena);
        fun->is_not = $2;
        $$ = fun;
    }
    | SimpleExpr EXACT_LIKE SimpleExpr LikeEscapeOpt %prec LIKE {
        FuncExpr* fun = FuncExpr::new_ternary_op_node(FT_EXACT_LIKE, $1, $3, $4, parser->arena);
        $$ = fun;
    }
    | SimpleExpr RegexpOrNot SimpleExpr %prec LIKE {
        FuncExpr* fun = FuncExpr::new_binary_op_node(FT_REGEXP, $1, $3, parser->arena);
        fun->is_not = $2;
        $$ = fun;
    }
    | SimpleExpr BetweenOrNot SimpleExpr AND SimpleExpr {
        FuncExpr* fun = FuncExpr::new_ternary_op_node(FT_BETWEEN, $1, $3, $5, parser->arena);
        fun->is_not = $2;
        $$ = fun;
    }
    ;
IsOrNot:
    IS {
        $$ = false;
    }
    | IS NOT {
        $$ = true;
    }
    ;
InOrNot:
    IN {
        $$ = false;
    }
    | NOT IN {
        $$ = true;
    }
    ;
AnyOrAll:
    ANY {
		$$ = parser::CMP_ANY;
	}
    | SOME {
		$$ = parser::CMP_SOME;
	}
    | ALL {
		$$ = parser::CMP_ALL;
	}
    ;
LikeOrNot:
    LIKE {
        $$ = false;
    }
    | NOT LIKE {
        $$ = true;
    }
    ;
LikeEscapeOpt:
    {
        $$ = LiteralExpr::make_string("'\\'", parser->arena);
    }
    |   "ESCAPE" STRING_LIT {
        $$ = $2;
    }
    ;
RegexpOrNot:
    REGEXP {
        $$ = false;
    }
    | RLIKE {
        $$ = false;
    }
    | NOT REGEXP {
        $$ = true;
    }
    | NOT RLIKE {
        $$ = true;
    }
    ;
BetweenOrNot:
    BETWEEN {
        $$ = false;
    }
    | NOT BETWEEN {
        $$ = true;
    }
    ;


/*create table statement*/
// TODO: create table xx like xx
CreateTableStmt:
    CREATE TABLE IfNotExists TableName '(' TableElementList ')' CreateTableOptionListOpt
    {
        CreateTableStmt* stmt = new_node(CreateTableStmt);
        stmt->if_not_exist = $3;
        stmt->table_name = (TableName*)($4);
        for (int idx = 0; idx < $6->children.size(); ++idx) {
            if ($6->children[idx]->node_type == NT_COLUMN_DEF) {
                stmt->columns.push_back((ColumnDef*)($6->children[idx]), parser->arena);
            } else if ($6->children[idx]->node_type == NT_CONSTRAINT) {
                stmt->constraints.push_back((Constraint*)($6->children[idx]), parser->arena);
            }
        }
        for (int idx = 0; idx < $8->children.size(); ++idx) {
            stmt->options.push_back((TableOption*)($8->children[idx]), parser->arena);
        }
        //stmt->options = $8->children;
        $$ = stmt;
    }
    ;

IfNotExists:
    {
        $$ = false;
    }
    | IF NOT EXISTS
    {
        $$ = true;
    }
    ;

IfExists:
    {
        $$ = false;
    }
    | IF EXISTS
    {
        $$ = true;
    }
    ;

TableElementList:
    TableElement
    {
        Node* list = new_node(Node);
        list->children.reserve(10, parser->arena);
        if ($1 != nullptr) {
            list->children.push_back($1, parser->arena);
        }
        $$ = list;
    }
    | TableElementList ',' TableElement
    {
        if ($3 != nullptr) {
            $1->children.push_back($3, parser->arena);
        }
        $$ = $1;
    }
    ;

TableElement:
    ColumnDef
    {
        $$ = $1;
    }
    | Constraint
    {
        $$ = $1;
    }
    | CHECK '(' Expr ')'
    {
        /* Nothing to do now */
        $$ = nullptr;
    }
    ;

ColumnDef:
    ColumnName Type ColumnOptionList
    {
        ColumnDef* column = new_node(ColumnDef);
        column->name = (ColumnName*)$1;
        column->type = (FieldType*)$2;
        for (int idx = 0; idx < $3->children.size(); ++idx) {
            column->options.push_back((ColumnOption*)($3->children[idx]), parser->arena);
        }
        $$ = column;
    }
    ;

ColumnDefList:
    ColumnDef
    {
        Node* list = new_node(Node);
        if ($1 != nullptr) {
            list->children.push_back($1, parser->arena);
        }
        $$ = list;
    }
    | ColumnDefList ',' ColumnDef
    {
        if ($3 != nullptr) {
            $1->children.push_back($3, parser->arena);
        }
        $$ = $1;
    }
    ;

ColumnOptionList:
    {
        $$ = new_node(Node);
    }
    | ColumnOptionList ColumnOption
    {
        $1->children.push_back($2, parser->arena);
        $$ = $1;
    }
    ;

ColumnOption:
    NOT NULLX
    {
        ColumnOption* option = new_node(ColumnOption);
        option->type = COLUMN_OPT_NOT_NULL;
        $$ = option;
    }
    | NULLX
    {
        ColumnOption* option = new_node(ColumnOption);
        option->type = COLUMN_OPT_NULL;
        $$ = option;
    }
    | AUTO_INCREMENT
    {
        ColumnOption* option = new_node(ColumnOption);
        option->type = COLUMN_OPT_AUTO_INC;
        $$ = option;
    }
    | PrimaryOpt KEY
    {
        ColumnOption* option = new_node(ColumnOption);
        option->type = COLUMN_OPT_PRIMARY_KEY;
        $$ = option;
    }
    | UNIQUE %prec lowerThanKey
    {
        ColumnOption* option = new_node(ColumnOption);
        option->type = COLUMN_OPT_UNIQ_KEY;
        $$ = option;
    }
    | UNIQUE KEY
    {
        ColumnOption* option = new_node(ColumnOption);
        option->type = COLUMN_OPT_UNIQ_KEY;
        $$ = option;
    }
    | DEFAULT DefaultValue
    {
        ColumnOption* option = new_node(ColumnOption);
        option->type = COLUMN_OPT_DEFAULT_VAL;
        option->expr = $2;
        $$ = option;
    }
    | ON UPDATE FunctionCallCurTimestamp
    {
        FuncExpr* current_timestamp = new_node(FuncExpr);
        current_timestamp->func_type = FT_COMMON;
        current_timestamp->fn_name = "current_timestamp";
        ColumnOption* option = new_node(ColumnOption);
        option->type = COLUMN_OPT_ON_UPDATE;
        option->expr = current_timestamp;
        $$ = option;
    }
    | COMMENT STRING_LIT
    {
        ColumnOption* option = new_node(ColumnOption);
        option->type = COLUMN_OPT_COMMENT;
        option->expr = $2;
        $$ = option;
    }
    ;
    
SignedLiteral:
    Literal {}
    | '+' NumLiteral {
        $$ = $2;
    }
    | '-' NumLiteral
    {
        LiteralExpr* literal = (LiteralExpr*)$2;
        if (literal->literal_type == parser::LT_INT) {
        	literal->_u.int64_val = 0 - literal->_u.int64_val;
        } else {
        	literal->_u.double_val = 0 - literal->_u.double_val;
        }
        $$ = literal;
    }
    ;

DefaultValue:
    FunctionCallCurTimestamp
    {
        FuncExpr* current_timestamp = new_node(FuncExpr);
        current_timestamp->func_type = FT_COMMON;
        current_timestamp->fn_name = "current_timestamp";
        $$ = (ExprNode*)current_timestamp;
    }
    | SignedLiteral
    {
        $$ = $1;
    }

FunctionCallCurTimestamp:
    NOW '(' ')'
    | FunctionNameCurTimestamp
    | FunctionNameCurTimestamp '(' ')'
    ;
FunctionNameCurTimestamp:
    CURRENT_TIMESTAMP 
    | LOCALTIME 
    | LOCALTIMESTAMP 
    ;

PrimaryOpt:
    {}
    | PRIMARY
    ;

DefaultKwdOpt:
    {}
    | DEFAULT
    ;

Constraint:
    ConstraintKeywordOpt ConstraintElem
    {
        if (!$1.empty()) {
            ((Constraint*)$2)->name = $1;
        }
        $$ = $2;
    }
    ;

ConstraintKeywordOpt:
    {
        $$ = nullptr;
    }
    | CONSTRAINT
    {
        $$ = nullptr;
    }
    | CONSTRAINT AllIdent
    {
        $$ = $2;
    }
    ;

ConstraintElem:
    PRIMARY KEY '(' ColumnNameList ')' IndexOptionList
    {
        Constraint* item = new_node(Constraint);
        item->type = CONSTRAINT_PRIMARY;
        for (int idx = 0; idx < $4->children.size(); ++idx) {
            item->columns.push_back((ColumnName*)($4->children[idx]), parser->arena);
        }
        item->index_option = (IndexOption*)$6;
        $$ = item;
    }
    | FULLTEXT KeyOrIndexOpt IndexName '(' ColumnNameList ')' IndexOptionList
    {
        Constraint* item = new_node(Constraint);
        item->type = CONSTRAINT_FULLTEXT;
        item->name = $3;
        for (int idx = 0; idx < $5->children.size(); ++idx) {
            item->columns.push_back((ColumnName*)($5->children[idx]), parser->arena);
        }
        item->index_option = (IndexOption*)$7;
        $$ = item;
    }
    | KeyOrIndex IndexName '(' ColumnNameList ')' IndexOptionList
    {
        Constraint* item = new_node(Constraint);
        item->type = CONSTRAINT_INDEX;
        item->name = $2;
        for (int idx = 0; idx < $4->children.size(); ++idx) {
            item->columns.push_back((ColumnName*)($4->children[idx]), parser->arena);
        }
        item->index_option = (IndexOption*)$6;
        $$ = item;
    }
    | UNIQUE KeyOrIndexOpt IndexName '(' ColumnNameList ')' IndexOptionList
    {
        Constraint* item = new_node(Constraint);
        item->type = CONSTRAINT_UNIQ;
        item->name = $3;
        for (int idx = 0; idx < $5->children.size(); ++idx) {
            item->columns.push_back((ColumnName*)($5->children[idx]), parser->arena);
        }
        item->index_option = (IndexOption*)$7;
        $$ = item;
    }
    | KeyOrIndex GlobalOrLocal IndexName '(' ColumnNameList ')' IndexOptionList
    {
        Constraint* item = new_node(Constraint);
        item->type = CONSTRAINT_INDEX;
        item->index_dist = static_cast<IndexDistibuteType>($2);
        item->name = $3;
        for (int idx = 0; idx < $5->children.size(); ++idx) {
            item->columns.push_back((ColumnName*)($5->children[idx]), parser->arena);
        }
        item->index_option = (IndexOption*)$7;
        $$ = item;
    }
    | UNIQUE KeyOrIndexOpt GlobalOrLocal IndexName '(' ColumnNameList ')' IndexOptionList
    {
        Constraint* item = new_node(Constraint);
        item->type = CONSTRAINT_UNIQ;
        item->index_dist = static_cast<IndexDistibuteType>($3);
        item->name = $4;
        for (int idx = 0; idx < $6->children.size(); ++idx) {
            item->columns.push_back((ColumnName*)($6->children[idx]), parser->arena);
        }
        item->index_option = (IndexOption*)$8;
        $$ = item;
    }
    ;

GlobalOrLocal:
    GLOBAL 
    {
        $$ = INDEX_DIST_GLOBAL;
    }
    | LOCAL 
    {
        $$ = INDEX_DIST_LOCAL;
    }
    ;

IndexName:
    {
        $$ = nullptr;
    }
    | AllIdent
    {
        $$ = $1;
    }
    ;

IndexOptionList:
    {
        $$ = nullptr;
    }
    | IndexOptionList IndexOption {
        if ($2 != nullptr) {
            $$ = $2;
        } else {
            $$ = $1;
        }
    }
    ;

IndexOption:
    KEY_BLOCK_SIZE EqOpt INTEGER_LIT {
        $$ = nullptr;
    }
    | IndexType {
        $$ = nullptr;
    }
    | COMMENT STRING_LIT {
        IndexOption* op = new_node(IndexOption);
        op->comment = ((LiteralExpr*)$2)->_u.str_val;
        $$ = op;
    }
    ;

IndexType:
    USING BTREE {
        $$ = nullptr;
    }
    | USING HASH {
        $$ = nullptr;
    }
    ;

/*************************************Type Begin***************************************/
Type:
    NumericType
    {
        $$ = $1;
    }
    | StringType
    {
        $$ = $1;
    }
    | DateAndTimeType
    {
        $$ = $1;
    }
    ;

NumericType:
    IntegerType OptFieldLen FieldOpts
    {
        FieldType* field_type = new_node(FieldType);
        field_type->type = (MysqlType)$1;
        field_type->total_len = $2;
        Node* list_node = $3;
        for (int idx = 0; idx < list_node->children.size(); ++idx) {
            TypeOption* type_opt = (TypeOption*)(list_node->children[idx]);
            if (type_opt->is_unsigned) {
                field_type->flag |= MYSQL_FIELD_FLAG_UNSIGNED;
            }
            if (type_opt->is_zerofill) {
                field_type->flag |= MYSQL_FIELD_FLAG_ZEROFILL;
            }
        }
        $$ = field_type;
    }
    | BooleanType FieldOpts
    {
        FieldType* field_type = new_node(FieldType);
        field_type->type = (MysqlType)$1;
        field_type->total_len = 1;
        Node* list_node = $2;
        for (int idx = 0; idx < list_node->children.size(); ++idx) {
            TypeOption* type_opt = (TypeOption*)(list_node->children[idx]);
            if (type_opt->is_unsigned) {
                field_type->flag |= MYSQL_FIELD_FLAG_UNSIGNED;
            }
            if (type_opt->is_zerofill) {
                field_type->flag |= MYSQL_FIELD_FLAG_ZEROFILL;
            }
        }
        $$ = field_type;
    }
    | FixedPointType FloatOpt FieldOpts
    {
        FieldType* field_type = new_node(FieldType);
        field_type->type = (MysqlType)$1;

        FloatOption* float_opt = (FloatOption*)$2;
        field_type->total_len = float_opt->total_len;
        field_type->float_len = float_opt->float_len;

        Node* list_node = $2;
        for (int idx = 0; idx < list_node->children.size(); ++idx) {
            TypeOption* type_opt = (TypeOption*)(list_node->children[idx]);
            if (type_opt->is_unsigned) {
                field_type->flag |= MYSQL_FIELD_FLAG_UNSIGNED;
            }
            if (type_opt->is_zerofill) {
                field_type->flag |= MYSQL_FIELD_FLAG_ZEROFILL;
            }
        }
        $$ = field_type;
    }
    | FloatingPointType FloatOpt FieldOpts
    {
        FieldType* field_type = new_node(FieldType);
        field_type->type = (MysqlType)$1;

        FloatOption* float_opt = (FloatOption*)$2;
        field_type->total_len = float_opt->total_len;
        if (field_type->type == MYSQL_TYPE_FLOAT) {
            if (field_type->total_len > MYSQL_FLOAT_PRECISION) {
                field_type->type = MYSQL_TYPE_DOUBLE;
            }
        }
        field_type->float_len = float_opt->float_len;

        Node* list_node = $2;
        for (int idx = 0; idx < list_node->children.size(); ++idx) {
            TypeOption* type_opt = (TypeOption*)(list_node->children[idx]);
            if (type_opt->is_unsigned) {
                field_type->flag |= MYSQL_FIELD_FLAG_UNSIGNED;
            }
            if (type_opt->is_zerofill) {
                field_type->flag |= MYSQL_FIELD_FLAG_ZEROFILL;
            }
        }
        $$ = field_type;
    }
    | BitValueType OptFieldLen
    {
        FieldType* field_type = new_node(FieldType);
        field_type->type = (MysqlType)$1;
        
        if ($2 == -1 || $2 == 0) {
            field_type->total_len = 1;
        } else if ($2 > 64) {
            sql_error(&@2, yyscanner, parser, "bit length should between 1 and 64");
        } else {
            field_type->total_len = $2;
        }
        $$ = field_type;
    }
    ;

IntegerType:
    TINYINT
    {
        $$ = MYSQL_TYPE_TINY;
    }
    | SMALLINT
    {
        $$ = MYSQL_TYPE_SHORT;
    }
    | MEDIUMINT
    {
        $$ = MYSQL_TYPE_INT24;
    }
    | INT
    {
        $$ = MYSQL_TYPE_LONG;
    }
    | INT1
    {
        $$ = MYSQL_TYPE_TINY;
    }
    | INT2
    {
        $$ = MYSQL_TYPE_SHORT;
    }
    | INT3
    {
        $$ = MYSQL_TYPE_INT24;
    }
    | INT4
    {
        $$ = MYSQL_TYPE_LONG;
    }
    | INT8
    {
        $$ = MYSQL_TYPE_LONGLONG;
    }
    | INTEGER
    {
        $$ = MYSQL_TYPE_LONG;
    }
    | BIGINT
    {
        $$ = MYSQL_TYPE_LONGLONG;
    }
    ;

BooleanType:
    BOOL
    {
        $$ = MYSQL_TYPE_TINY;
    }
    |    BOOLEAN
    {
        $$ = MYSQL_TYPE_TINY;
    }
    ;

FixedPointType:
    DECIMAL
    {
        $$ = MYSQL_TYPE_NEWDECIMAL;
    }
    | NUMERIC
    {
        $$ = MYSQL_TYPE_NEWDECIMAL;
    }
    ;

FloatingPointType:
    FLOAT
    {
        $$ = MYSQL_TYPE_FLOAT;
    }
    | REAL
    {
        $$ = MYSQL_TYPE_DOUBLE;
    }
    | DOUBLE
    {
        $$ = MYSQL_TYPE_DOUBLE;
    }
    | DOUBLE PRECISION
    {
        $$ = MYSQL_TYPE_DOUBLE;
    }
    ;

BitValueType:
    BIT
    {
        $$ = MYSQL_TYPE_BIT;
    }
    ;

StringType:
    NationalOpt CHAR FieldLen OptBinary OptCharset OptCollate
    {
        FieldType* field_type = new_node(FieldType);
        field_type->type = MYSQL_TYPE_STRING;
        field_type->total_len = $3;
        field_type->charset = $5;
        field_type->collate = $6;
        if ($4 == true) {
            field_type->flag |= MYSQL_FIELD_FLAG_BINARY;
        }
        $$ = field_type;
    }
    | NationalOpt CHAR OptBinary OptCharset OptCollate
    {
        FieldType* field_type = new_node(FieldType);
        field_type->type = MYSQL_TYPE_STRING;
        field_type->charset = $4;
        field_type->collate = $5;
        if ($3 == true) {
            field_type->flag |= MYSQL_FIELD_FLAG_BINARY;
        }
        $$ = field_type;
    }
    | Varchar FieldLen OptBinary OptCharset OptCollate
    {
        FieldType* field_type = new_node(FieldType);
        field_type->type = MYSQL_TYPE_VARCHAR;
        field_type->total_len = $2;
        field_type->charset = $4;
        field_type->collate = $5;
        if ($3 == true) {
            field_type->flag |= MYSQL_FIELD_FLAG_BINARY;
        }
        $$ = field_type;
    }
    | BINARY OptFieldLen
    {
        FieldType* field_type = new_node(FieldType);
        field_type->type = MYSQL_TYPE_STRING;
        field_type->total_len = $2;
        field_type->charset.strdup("BINARY", parser->arena);
        field_type->collate.strdup("BINARY", parser->arena);
        field_type->flag |= MYSQL_FIELD_FLAG_BINARY;
        $$ = field_type;
    }
    | VARBINARY FieldLen
    {
        FieldType* field_type = new_node(FieldType);
        field_type->type = MYSQL_TYPE_VARCHAR;
        field_type->total_len = $2;
        field_type->charset.strdup("BINARY", parser->arena);
        field_type->collate.strdup("BINARY", parser->arena);
        field_type->flag |= MYSQL_FIELD_FLAG_BINARY;
        $$ = field_type;
    }
    | BlobType
    {  
        FieldType* field_type = (FieldType*)$1;
        field_type->charset.strdup("BINARY", parser->arena);
        field_type->collate.strdup("BINARY", parser->arena);
        field_type->flag |= MYSQL_FIELD_FLAG_BINARY;
        $$ = field_type;
    }
    | TextType OptBinary OptCharset OptCollate
    {
        FieldType* field_type = (FieldType*)$1;
        field_type->charset = $3;
        field_type->collate = $4;
        if ($2 == true) {
            field_type->flag |= MYSQL_FIELD_FLAG_BINARY;
        }
        $$ = field_type;
    }
    | ENUM '(' StringList ')' OptCharset OptCollate
    {
        FieldType* field_type = new_node(FieldType);
        field_type->type = MYSQL_TYPE_ENUM;
        $$ = field_type;
    }
    | SET '(' StringList ')' OptCharset OptCollate
    {
        FieldType* field_type = new_node(FieldType);
        field_type->type = MYSQL_TYPE_SET;
        $$ = field_type;
    }
    | JSON
    {
        FieldType* field_type = new_node(FieldType);
        field_type->type = MYSQL_TYPE_JSON;
        $$ = field_type;
    }
    | HLL 
    {
        FieldType* field_type = new_node(FieldType);
        field_type->type = MYSQL_TYPE_HLL;
        $$ = field_type;
    }
    | BITMAP {
        FieldType* field_type = new_node(FieldType);
        field_type->type = MYSQL_TYPE_BITMAP;
        $$ = field_type;
    }
    | TDIGEST {
        FieldType* field_type = new_node(FieldType);
        field_type->type = MYSQL_TYPE_TDIGEST;
        $$ = field_type;
    }
    ;

NationalOpt:
    {}
    |    NATIONAL
    ;

Varchar:
    NATIONAL VARCHAR
    | VARCHAR
    | NVARCHAR
    ;

BlobType:
    TINYBLOB
    {
        FieldType* field_type = new_node(FieldType);
        field_type->type = MYSQL_TYPE_TINY_BLOB;
        $$ = field_type;
    }
    | BLOB OptFieldLen
    {
        FieldType* field_type = new_node(FieldType);
        field_type->type = MYSQL_TYPE_BLOB;
        field_type->total_len = $2;
        $$ = field_type;
    }
    | MEDIUMBLOB
    {
        FieldType* field_type = new_node(FieldType);
        field_type->type = MYSQL_TYPE_MEDIUM_BLOB;
        $$ = field_type;
    }
    | LONGBLOB
    {
        FieldType* field_type = new_node(FieldType);
        field_type->type = MYSQL_TYPE_LONG_BLOB;
        $$ = field_type;
    }
    ;

TextType:
    TINYTEXT
    {
        FieldType* field_type = new_node(FieldType);
        field_type->type = MYSQL_TYPE_TINY_BLOB;
        $$ = field_type;
    }
    | TEXT OptFieldLen
    {
        FieldType* field_type = new_node(FieldType);
        field_type->type = MYSQL_TYPE_BLOB;
        field_type->total_len = $2;
        $$ = field_type;
    }
    | MEDIUMTEXT
    {
        FieldType* field_type = new_node(FieldType);
        field_type->type = MYSQL_TYPE_MEDIUM_BLOB;
        $$ = field_type;
    }
    | LONGTEXT
    {
        FieldType* field_type = new_node(FieldType);
        field_type->type = MYSQL_TYPE_LONG_BLOB;
        $$ = field_type;
    }
    | LONG VARCHAR
    {
        FieldType* field_type = new_node(FieldType);
        field_type->type = MYSQL_TYPE_MEDIUM_BLOB;
        $$ = field_type;
    }
    ;

DateAndTimeType:
    DATE
    {
        FieldType* field_type = new_node(FieldType);
        field_type->type = MYSQL_TYPE_DATE;
        $$ = field_type;
    }
    | DATETIME OptFieldLen
    {
        // TODO: fractional seconds precision
        FieldType* field_type = new_node(FieldType);
        field_type->type = MYSQL_TYPE_DATETIME;
        $$ = field_type;
    }
    | TIMESTAMP OptFieldLen
    {
        // TODO: fractional seconds precision
        FieldType* field_type = new_node(FieldType);
        field_type->type = MYSQL_TYPE_TIMESTAMP;
        $$ = field_type;
    }
    | TIME OptFieldLen
    {
        // TODO: fractional seconds precision
        FieldType* field_type = new_node(FieldType);
        field_type->type = MYSQL_TYPE_TIME;
        $$ = field_type;
    }
    | YEAR OptFieldLen
    {
        FieldType* field_type = new_node(FieldType);
        field_type->type = MYSQL_TYPE_YEAR;
        field_type->total_len = $2;
        if (field_type->total_len != -1 && field_type->total_len != 4) {
            sql_error(&@2, yyscanner, parser, "YEAR length must be set 4.");
            return -1;
        }
        $$ = field_type;
    }
    ;

OptFieldLen:
    /* empty == (-1) == unspecified field length*/
    {
        $$ = -1;
    }
    | FieldLen
    {
        $$ = $1;
    }
    ;

FieldLen:
    '(' INTEGER_LIT ')'
    {
        $$ = ((LiteralExpr*)$2)->_u.int64_val;
    }
    ;

FieldOpt:
    UNSIGNED
    {
        TypeOption* type_opt = new_node(TypeOption);
        type_opt->is_unsigned = true;
        $$ = type_opt;
    }
    | SIGNED
    {
        TypeOption* type_opt = new_node(TypeOption);
        type_opt->is_unsigned = false;
        $$ = type_opt;
    }
    | ZEROFILL
    {
        TypeOption* type_opt = new_node(TypeOption);
        type_opt->is_unsigned = true;
        type_opt->is_zerofill = true;
        $$ = type_opt;
    }
    ;

FieldOpts:
    {
        $$ = new_node(Node);
    }
    | FieldOpts FieldOpt
    {
        $1->children.push_back($2, parser->arena);
        $$ = $1;
    }
    ;

FloatOpt:
    {
        FloatOption* float_opt = new_node(FloatOption);
        $$ = float_opt;
    }
    |    FieldLen
    {
        FloatOption* float_opt = new_node(FloatOption);
        float_opt->total_len = $1;
        $$ = float_opt;
    }
    |    Precision
    {
        $$ = $1;
    }
    ;

Precision:
    '(' INTEGER_LIT ',' INTEGER_LIT ')'
    {
        FloatOption* float_opt = new_node(FloatOption);
        float_opt->total_len = ((LiteralExpr*)$2)->_u.int64_val;
        float_opt->float_len = ((LiteralExpr*)$4)->_u.int64_val;
        $$ = float_opt;
    }
    ;

OptBinary:
    {
        $$ = false;
    }
    | BINARY
    {
        $$ = true;
    }
    ;

OptCharset:
    {
        $$ = nullptr;
    }
    | CharsetKw StringName
    {
        $$ = $2;
    }
    ;

CharsetKw:
    CHARACTER SET 
    | CHARSET
    ;

OptCollate:
    {
        $$ = nullptr;
    }
    | COLLATE StringName
    {
        $$ = $2;
    }
    ;

StringList:
    STRING_LIT
    {
        Node* list = new_node(Node);
        list->children.push_back($1, parser->arena);
        $$ = list;
    }
    | StringList ',' STRING_LIT
    {
        $1->children.push_back($3, parser->arena
        );
        $$ = $1;
    }
    ;

StringName:
    STRING_LIT
    {
        $$ = ((LiteralExpr*)$1)->_u.str_val;
    }
    | AllIdent
    {
        $$ = $1;
    }
    ;

CreateTableOptionListOpt:
    {
        $$ = new_node(Node);
    }
    | TableOptionList 
    {
        $$ = $1;
    }
    ;

TableOptionList:
    TableOption
    {
        Node* list = new_node(Node);
        list->children.reserve(10, parser->arena);
        list->children.push_back($1, parser->arena);
        $$ = list;
    }
    | TableOptionList TableOption
    {
        $1->children.push_back($2, parser->arena);
        $$ = $1;
    }
    | TableOptionList ','  TableOption
    {
        $1->children.push_back($3, parser->arena);
        $$ = $1;
    }
    ;

TableOption:
    ENGINE EqOpt StringName
    {
        TableOption* option = new_node(TableOption);
        option->type = TABLE_OPT_ENGINE;
        option->str_value = $3;
        $$ = option;
    }
    | DefaultKwdOpt CharsetKw EqOpt StringName
    {
        TableOption* option = new_node(TableOption);
        option->type = TABLE_OPT_CHARSET;
        option->str_value = $4;
        $$ = option;
    }
    | DefaultKwdOpt COLLATE EqOpt StringName
    {
        TableOption* option = new_node(TableOption);
        option->type = TABLE_OPT_COLLATE;
        option->str_value = $4;
        $$ = option;
    }
    | AUTO_INCREMENT EqOpt INTEGER_LIT
    {
        TableOption* option = new_node(TableOption);
        option->type = TABLE_OPT_AUTO_INC;
        option->uint_value = ((LiteralExpr*)$3)->_u.int64_val;
        $$ = option;
    }
    | COMMENT EqOpt STRING_LIT
    {
        TableOption* option = new_node(TableOption);
        option->type = TABLE_OPT_COMMENT;
        option->str_value = ((LiteralExpr*)$3)->_u.str_val;
        $$ = option;
    }
    | AVG_ROW_LENGTH EqOpt INTEGER_LIT
    {
        TableOption* option = new_node(TableOption);
        option->type = TABLE_OPT_AVG_ROW_LENGTH;
        option->uint_value = ((LiteralExpr*)$3)->_u.int64_val;
        $$ = option;
    }
    | KEY_BLOCK_SIZE EqOpt INTEGER_LIT
    {
        TableOption* option = new_node(TableOption);
        option->type = TABLE_OPT_KEY_BLOCK_SIZE;
        option->uint_value = ((LiteralExpr*)$3)->_u.int64_val;
        $$ = option;
    }
    | PARTITION BY PartitionOpt
    {
        TableOption* option = (TableOption*)$3;
        option->type = TABLE_OPT_PARTITION;
        $$ = option;
    }
    ;

PartitionRange:
    PARTITION AllIdent VALUES LESS THAN '(' Expr ')'
    {
        PartitionRange* p = new_node(PartitionRange);
        p->name = $2;
        p->less_expr = $7;
        $$ = p;
    }
    ;

PartitionRangeList:
    PartitionRange
    {
        Node* list = new_node(Node);
        list->children.push_back($1, parser->arena);
        $$ = list;
    }
    | PartitionRangeList ','  PartitionRange
    {
        Node* list = $1;
        list->children.push_back($3, parser->arena);
        $$ = list;
    }
    ;

PartitionOpt:
    RANGE '(' Expr ')' '(' PartitionRangeList ')'
    {
        PartitionOption* option = new_node(PartitionOption);
        option->type = PARTITION_RANGE;
        option->expr = $3;
        for (int i = 0; i < $6->children.size(); i++) {
            option->range.push_back(static_cast<PartitionRange*>($6->children[i]), parser->arena);
        }
        $$ = option;
    }
    | HASH '(' Expr ')' PARTITIONS INTEGER_LIT 
    {
        PartitionOption* option = new_node(PartitionOption);
        option->type = PARTITION_HASH;
        option->expr = $3;
        option->partition_num = ((LiteralExpr*)$6)->_u.int64_val;
        $$ = option;
    }
    ;
EqOpt:
    {}
    | EQ_OP
    ;

// Drop Table(s) Statement
DropTableStmt:
    DROP TableOrTables IfExists TableNameList RestrictOrCascadeOpt
    {
        DropTableStmt* stmt = new_node(DropTableStmt);
        stmt->if_exist = $3;
        for (int i = 0; i < $4->children.size(); i++) {
            stmt->table_names.push_back((TableName*)$4->children[i], parser->arena);
        }
        $$ = stmt;
    }
    ;

TableOrTables: 
    TABLE | TABLES
    ;

RestrictOrCascadeOpt:
    {}
    | RESTRICT
    | CASCADE
    ;

// Restore Table(s) Statement
RestoreTableStmt:
    RESTORE TableOrTables TableNameList 
    {
        RestoreTableStmt* stmt = new_node(RestoreTableStmt);
        for (int i = 0; i < $3->children.size(); i++) {
            stmt->table_names.push_back((TableName*)$3->children[i], parser->arena);
        }
        $$ = stmt;
    }
    ;

// Create Database Statement
CreateDatabaseStmt:
    CREATE DATABASE IfNotExists DBName DatabaseOptionListOpt
    {
        CreateDatabaseStmt* stmt = new_node(CreateDatabaseStmt);
        stmt->if_not_exist = $3;
        stmt->db_name = $4;
        for (int idx = 0; idx < $5->children.size(); ++idx) {
            stmt->options.push_back((DatabaseOption*)$5->children[idx], parser->arena);
        }
        $$ = stmt;
    }
    ;

DBName:
    AllIdent {
        $$ = $1;
    }
    ;

ResourceTag:
    AllIdent {
        $$ = $1;
    }
    ;
DatabaseOption:
    DefaultKwdOpt CharsetKw EqOpt StringName
    {
        DatabaseOption* option = new_node(DatabaseOption);
        option->type = DATABASE_OPT_CHARSET;
        option->str_value = $4;
        $$ = option;
    }
    | DefaultKwdOpt COLLATE EqOpt StringName
    {
        DatabaseOption* option = new_node(DatabaseOption);
        option->type = DATABASE_OPT_COLLATE;
        option->str_value = $4;
        $$ = option;
    }
    ;

DatabaseOptionListOpt:
    {
        $$ = new_node(Node);
    }
    | DatabaseOptionList
    {
        $$ = $1;
    }
    ;

DatabaseOptionList:
    DatabaseOption
    {
        Node* list = new_node(Node);
        list->children.push_back($1, parser->arena);
        $$ = list;
    }
    | DatabaseOptionList DatabaseOption
    {
        Node* list = $1;
        list->children.push_back($2, parser->arena);
        $$ = list;
    }
    ;

DropDatabaseStmt:
    DROP DATABASE IfExists DBName
    {
        DropDatabaseStmt* stmt = new_node(DropDatabaseStmt);
        stmt->if_exist = $3;
        stmt->db_name = $4;
        $$ = stmt;
    }
    ;

WorkOpt:
    {}
    | WORK
    ;

StartTransactionStmt:
    START TRANSACTION
    {
        $$ = new_node(StartTxnStmt);
    }
    | BEGINX WorkOpt
    {
        $$ = new_node(StartTxnStmt);;
    }
    ;

CommitTransactionStmt:
    COMMIT WorkOpt
    {
        $$ = new_node(CommitTxnStmt);
    }
    ;

RollbackTransactionStmt:
    ROLLBACK WorkOpt
    {
        $$ = new_node(RollbackTxnStmt);
    }
    ;

SetStmt:
    SET VarAssignList
    {
        $$ = $2;
    }
    | SET GLOBAL TRANSACTION TransactionChars
    {
        $$ = $4;
    }
    | SET SESSION TRANSACTION TransactionChars 
    {
        $$ = $4;
    }
    | SET TRANSACTION TransactionChars 
    {
        $$ = $3;
    }
    ;

VarAssignList:
    VarAssignItem
    {   
        SetStmt* set = new_node(SetStmt);
        set->var_list.push_back((VarAssign*)$1, parser->arena);
        $$ = set;
    }
    | VarAssignList ',' VarAssignItem
    {
        ((SetStmt*)$1)->var_list.push_back((VarAssign*)$3, parser->arena);
        $$ = $1;
    }
    ;

VarAssignItem:
    VarName EQ_OP Expr
    {
        VarAssign* assign = new_node(VarAssign);
        assign->key = $1;
        if ($3 != nullptr && $3->expr_type == ET_COLUMN) {
            ColumnName* col_name = (ColumnName*)$3;
            if (col_name->name.to_lower() == "off") {
                // SET XXX = OFF
                // OFF to 0
                assign->value = LiteralExpr::make_int("0", parser->arena);
            } else {
                assign->value = $3;
            }
        } else {
            assign->value = $3;
        }
        $$ = assign;
    }
    | VarName EQ_OP ON
    {
        VarAssign* assign = new_node(VarAssign);
        assign->key = $1;
        assign->value = LiteralExpr::make_int("1", parser->arena);
        $$ = assign;
    }
    | VarName EQ_OP DEFAULT
    {
        VarAssign* assign = new_node(VarAssign);
        assign->key = $1;
        $$ = assign;
    }
    | VarName ASSIGN_OP Expr
    {
        VarAssign* assign = new_node(VarAssign);
        assign->key = $1;
        if ($3 != nullptr && $3->expr_type == ET_COLUMN) {
            ColumnName* col_name = (ColumnName*)$3;
            if (col_name->name.to_lower() == "off") {
                // SET XXX = OFF
                // OFF to 0
                assign->value = LiteralExpr::make_int("0", parser->arena);
            } else {
                assign->value = $3;
            }
        } else {
            assign->value = $3;
        }
        $$ = assign;
    }
    | CharsetKw AllIdent
    {
        VarAssign* assign = nullptr;
        if ($2.empty() == false) {
            assign = new_node(VarAssign);
            assign->key.strdup("CHARACTER SET", parser->arena);
            assign->value = LiteralExpr::make_string($2.value, parser->arena);
        }
        $$ = assign;
    }
    ;

VarName:
    AllIdent
    {
        $$ = $1;
    }
    | GLOBAL AllIdent
    {
        String str;
        if ($2.empty() == false) {
            str.strdup("@@global.", parser->arena);
            str.append($2.c_str(), parser->arena);
        }
        $$ = str;
    }
    | SESSION AllIdent
    {
        String str;
        if ($2.empty() == false) {
            str.strdup("@@session.", parser->arena);
            str.append($2.c_str(), parser->arena);
        }
        $$ = str;
    }
    | LOCAL AllIdent
    {
        String str;
        if ($2.empty() == false) {
            str.strdup("@@local.", parser->arena);
            str.append($2.c_str(), parser->arena);
        }
        $$ = str;
    }
    ;

TransactionChars:
    TransactionChar
    {
        SetStmt* set = new_node(SetStmt);
        set->var_list.push_back((VarAssign*)$1, parser->arena);
        $$ = set;
    }
    | TransactionChars ',' TransactionChar 
    {
        ((SetStmt*)$1)->var_list.push_back((VarAssign*)$3, parser->arena);
        $$ = $1;
    }
    ;

TransactionChar: 
    ISOLATION LEVEL IsolationLevel 
    {
        VarAssign* assign = new_node(VarAssign);
        assign->key.strdup("@@isolation.", parser->arena);
        assign->value = $3;
        $$ = assign;
    }
    ;
//    | READ WRITE
//    {
//    }
//    | READ ONLY
//    {
//    }
//    ;

IsolationLevel:
    REPEATABLE READ
    {
        $$ = LiteralExpr::make_int("1", parser->arena);
    }
    | READ COMMITTED
    {
        $$ = LiteralExpr::make_int("2", parser->arena);
    }
    | READ UNCOMMITTED
    {
        $$ = LiteralExpr::make_int("3", parser->arena);
    }
    | SERIALIZABLE
    {
        $$ = LiteralExpr::make_int("4", parser->arena);
    }
    ;

ShowStmt:
    SHOW ShowTargetFilterable ShowLikeOrWhereOpt {
        $$ = nullptr;
    }
    | SHOW CREATE TABLE TableName {
        $$ = nullptr;
    }
    | SHOW CREATE DATABASE DBName {
        $$ = nullptr;
    }
    | SHOW GRANTS {
        // See https://dev.mysql.com/doc/refman/5.7/en/show-grants.html
        $$ = nullptr;
    }
/*
    | SHOW GRANTS FOR Username {
        $$ = nullptr;
    }
*/
    | SHOW MASTER STATUS {
        $$ = nullptr;
    }
    | SHOW OptFull PROCESSLIST {
        $$ = nullptr;
    }
    | SHOW PROFILES {
        $$ = nullptr;
    }
    | SHOW PRIVILEGES {
        $$ = nullptr;
    }
    ;
ShowIndexKwd:
    INDEX
    | INDEXES
    | KEYS
    ;

FromOrIn:
    FROM | IN

ShowTargetFilterable:
    ENGINES {
        $$ = nullptr;
    }
    | DATABASES {
        $$ = nullptr;
    }
    | CharsetKw {
        $$ = nullptr;
    }
    | OptFull TABLES ShowDatabaseNameOpt {
        $$ = nullptr;
    }
    | TABLE STATUS ShowDatabaseNameOpt {
        $$ = nullptr;
    }
    | ShowIndexKwd FromOrIn TableName {
        $$ = nullptr;
    }

    | ShowIndexKwd FromOrIn AllIdent FromOrIn AllIdent {
        $$ = nullptr;
    }

    | OptFull COLUMNS ShowTableAliasOpt ShowDatabaseNameOpt {
        $$ = nullptr;
    }
    | OptFull FIELDS ShowTableAliasOpt ShowDatabaseNameOpt {
        // SHOW FIELDS is a synonym for SHOW COLUMNS
        $$ = nullptr;
    }
    | WARNINGS {
        $$ = nullptr;
    }
    | GlobalScope VARIABLES {
        $$ = nullptr;
    }
    | GlobalScope STATUS {
        $$ = nullptr;
    }
    | COLLATION {
        $$ = nullptr;
    }
    | TRIGGERS ShowDatabaseNameOpt {
        $$ = nullptr;
    }
    | PROCEDURE STATUS {
        $$ = nullptr;
    }
    | FUNCTION STATUS
    {
        $$ = nullptr;
    }
    | EVENTS ShowDatabaseNameOpt
    {
        $$ = nullptr;
    }
    | PLUGINS {
        $$ = nullptr;
    }
    ;
ShowLikeOrWhereOpt: {
        $$ = nullptr;
    }
    | LIKE SimpleExpr {
        $$ = nullptr;
    }
    | WHERE Expr {
        $$ = nullptr;
    }
    ;
GlobalScope:
    {
        $$ = false;
    }
    | GLOBAL {
        $$ = true;
    }
    | SESSION {
        $$ = false;
    }
    ;
OptFull:
    {
        $$ = false;
    }
    | FULL {
        $$ = true;
    }
    ;
ShowDatabaseNameOpt:
    {
        $$ = nullptr;
    }
    | FromOrIn DBName {
        $$ = nullptr;
    }
    ;
ShowTableAliasOpt:
    FromOrIn TableName {
        $$ = nullptr;
    }
    ;

AlterTableStmt:
    ALTER IgnoreOptional TABLE TableName AlterSpecList
    {
        AlterTableStmt* stmt = new_node(AlterTableStmt);
        stmt->ignore = $2;
        stmt->table_name = (TableName*)$4;
        for (int idx = 0; idx < $5->children.size(); ++idx) {
            stmt->alter_specs.push_back((AlterTableSpec*)($5->children[idx]), parser->arena);
        }
        $$ = stmt;
    }
    ;

AlterSpecList:
    AlterSpec
    {
        Node* list = new_node(Node);
        list->children.push_back($1, parser->arena);
        $$ = list;
    }
    | AlterSpecList ',' AlterSpec
    {
        $1->children.push_back($3, parser->arena);
        $$ = $1;
    }
    ;

ColumnKwdOpt:
    {}
    | COLUMN
    ;

AsOrToOpt:
    {}
    | AS
    {}
    | TO
    {}

ColumnPosOpt:
    {}
    | FIRST ColumnName
    {}
    | AFTER ColumnName
    {}
    ;

AlterSpec:
    TableOptionList
    {
        AlterTableSpec* spec = new_node(AlterTableSpec);
        spec->spec_type = ALTER_SPEC_TABLE_OPTION;
        for (int idx = 0; idx < $1->children.size(); ++idx) {
            spec->table_options.push_back((TableOption*)($1->children[idx]), parser->arena);
        }
        $$ = spec;
    }
    | ADD ColumnKwdOpt ColumnDef ColumnPosOpt
    {
        AlterTableSpec* spec = new_node(AlterTableSpec);
        spec->spec_type = ALTER_SPEC_ADD_COLUMN;
        spec->new_columns.push_back((ColumnDef*)$3, parser->arena);
        $$ = spec;
    }
    | ADD ColumnKwdOpt '(' ColumnDefList ')'
    {
        AlterTableSpec* spec = new_node(AlterTableSpec);
        spec->spec_type = ALTER_SPEC_ADD_COLUMN;
        for (int idx = 0; idx < $4->children.size(); ++idx) {
            spec->new_columns.push_back((ColumnDef*)($4->children[idx]), parser->arena);
        }
        $$ = spec;
    }
    | DROP ColumnKwdOpt AllIdent
    {
        AlterTableSpec* spec = new_node(AlterTableSpec);
        spec->spec_type = ALTER_SPEC_DROP_COLUMN;
        spec->column_name = $3;
        $$ = spec;
    }
    | RENAME COLUMN AllIdent TO ColumnName
    {
        AlterTableSpec* spec = new_node(AlterTableSpec);
        spec->spec_type = ALTER_SPEC_RENAME_COLUMN;
        spec->column_name = $3;

        ColumnDef* new_column = new_node(ColumnDef);
        new_column->name = (ColumnName*)$5;
        spec->new_columns.push_back(new_column, parser->arena);
        $$ = spec;
    }
    | RENAME AsOrToOpt TableName
    {
        AlterTableSpec* spec = new_node(AlterTableSpec);
        spec->spec_type = ALTER_SPEC_RENAME_TABLE;
        spec->new_table_name = (TableName*)$3;
        $$ = spec;
    }
    | SWAP AsOrToOpt TableName
    {
        AlterTableSpec* spec = new_node(AlterTableSpec);
        spec->spec_type = ALTER_SPEC_SWAP_TABLE;
        spec->new_table_name = (TableName*)$3;
        $$ = spec;
    }
    | ADD ConstraintKeywordOpt ConstraintElem
    {
        AlterTableSpec* spec = new_node(AlterTableSpec);
        spec->spec_type = ALTER_SPEC_ADD_INDEX;
        spec->new_constraints.push_back((Constraint*)$3, parser->arena);
        $$ = spec;
    }
    | ADD VIRTUAL ConstraintKeywordOpt ConstraintElem
    {
        AlterTableSpec* spec = new_node(AlterTableSpec);
        spec->spec_type = ALTER_SPEC_ADD_INDEX;
        spec->is_virtual_index = true;
        spec->new_constraints.push_back((Constraint*)$4, parser->arena);
        $$ = spec;
    }
    | DROP KeyOrIndex IndexName ForceOrNot
    {
        AlterTableSpec* spec = new_node(AlterTableSpec);
        spec->spec_type = ALTER_SPEC_DROP_INDEX;
        spec->index_name = $3;
        spec->force = $4;
        $$ = spec;
    }
    | DROP VIRTUAL KeyOrIndex IndexName
    {
        AlterTableSpec* spec = new_node(AlterTableSpec);
        spec->spec_type = ALTER_SPEC_DROP_INDEX;
        spec->is_virtual_index = true;
        spec->index_name = $4;
        $$ = spec;
    }
    | RESTORE KeyOrIndex IndexName
    {
        AlterTableSpec* spec = new_node(AlterTableSpec);
        spec->spec_type = ALTER_SPEC_RESTORE_INDEX;
        spec->index_name = $3;
        $$ = spec;
    }
    | ADD LEARNER ResourceTag
    {
        AlterTableSpec* spec = new_node(AlterTableSpec);
        spec->spec_type = ALTER_SPEC_ADD_LEARNER;
        spec->resource_tag = $3;
        $$ = spec;
    }
    | DROP LEARNER ResourceTag
    {
        AlterTableSpec* spec = new_node(AlterTableSpec);
        spec->spec_type = ALTER_SPEC_DROP_LEARNER;
        spec->resource_tag = $3;
        $$ = spec;
    }
    | MODIFY COLUMN SET AssignmentList WhereClauseOptional
    {
        AlterTableSpec* spec = new_node(AlterTableSpec);
        spec->spec_type = ALTER_SPEC_MODIFY_COLUMN;
        spec->set_list.reserve($4->children.size(), parser->arena);
        for (int i = 0; i < $4->children.size(); i++) {
            Assignment* assign = (Assignment*)$4->children[i];
            spec->set_list.push_back(assign, parser->arena);
        }
        spec->where = $5;
        $$ = spec;
    }
    ;

// Prepare Statement
NewPrepareStmt:
    PREPARE AllIdent FROM STRING_LIT
    {
        NewPrepareStmt* stmt = new_node(NewPrepareStmt);
        stmt->name = $2;
        stmt->sql = ((LiteralExpr*)$4)->_u.str_val;
        $$ = stmt;
    }
    | PREPARE AllIdent FROM VarName
    {
        if ($4.starts_with("@@") ) {
            sql_error(&@2, yyscanner, parser, "user variable cannot start with @@");
            return -1;
        }
        if ($4.starts_with("@") == false) {
            sql_error(&@2, yyscanner, parser, "only user variable is permitted in USING clause");
            return -1;
        }
        NewPrepareStmt* stmt = new_node(NewPrepareStmt);
        stmt->name = $2;
        stmt->sql = $4;
        $$ = stmt;
    }
    ;

ExecPrepareStmt:
    EXECUTE AllIdent
    {
        ExecPrepareStmt* stmt = new_node(ExecPrepareStmt);
        stmt->name = $2;
        $$ = stmt;
    }
    | EXECUTE AllIdent USING VarList
    {
        ExecPrepareStmt* stmt = new_node(ExecPrepareStmt);
        stmt->name = $2;
        for (int idx = 0; idx < $4->size(); ++idx) {
            if ((*$4)[idx].starts_with("@@")) {
                sql_error(&@2, yyscanner, parser, "user variable cannot start with @@");
                return -1;
            }
            if ((*$4)[idx].starts_with("@") == false) {
                sql_error(&@2, yyscanner, parser, "only user variable is permitted in USING clause");
                return -1;
            }
            stmt->param_list.push_back((*$4)[idx], parser->arena);
        }
        $$ = stmt;
    }
    ;
    
VarList:
    VarName
    {
        Vector<String>* string_list = new_node(Vector<String>);
        string_list->reserve(5, parser->arena);
        string_list->push_back($1, parser->arena);
        $$ = string_list;        
    }
    | VarList ',' VarName
    {
        $1->push_back($3, parser->arena);
        $$ = $1;
    }
    ;

DeallocPrepareStmt:
    DEALLOCATE PREPARE AllIdent
    {
        DeallocPrepareStmt* stmt = new_node(DeallocPrepareStmt);
        stmt->name = $3;
        $$ = stmt;
    }
    | DROP PREPARE AllIdent
    {
        DeallocPrepareStmt* stmt = new_node(DeallocPrepareStmt);
        stmt->name = $3;
        $$ = stmt;
    }
    ;

ExplainSym:
    EXPLAIN | DESCRIBE | DESC
    ;

ExplainStmt:
    ExplainSym ExplainableStmt {
        ExplainStmt* explain = new_node(ExplainStmt);
        explain->stmt = $2;
        explain->format = "row";
        $$ = explain;
    }
    | ExplainSym FORMAT EQ_OP STRING_LIT ExplainableStmt {
        ExplainStmt* explain = new_node(ExplainStmt);
        explain->format = ((LiteralExpr*)$4)->_u.str_val;
        explain->stmt = $5;
        $$ = explain;
    }
    ;

KillStmt:
    KILL INTEGER_LIT {
        KillStmt* k = new_node(KillStmt);
        k->conn_id = ((LiteralExpr*)$2)->_u.int64_val;
        $$ = k;
    }
    | KILL CONNECTION INTEGER_LIT {
        KillStmt* k = new_node(KillStmt);
        k->conn_id = ((LiteralExpr*)$3)->_u.int64_val;
        $$ = k;
    }
    | KILL QUERY INTEGER_LIT {
        KillStmt* k = new_node(KillStmt);
        k->conn_id = ((LiteralExpr*)$3)->_u.int64_val;
        k->is_query = true;
        $$ = k;
    }
    ;

/**************************************LoadDataStmt*****************************************
 * See https://dev.mysql.com/doc/refman/5.7/en/load-data.html
 *******************************************************************************************/
LoadDataStmt:
    LOAD DATA LocalOpt INFILE STRING_LIT DuplicateOpt INTO TABLE TableName OptCharset Fields Lines IgnoreLines ColumnNameOrUserVarListOptWithBrackets LoadDataSetSpecOpt
    {
        LoadDataStmt* load = new_node(LoadDataStmt);
        load->is_local = $3;
        load->path = ((LiteralExpr*)$5)->_u.str_val;
        load->on_duplicate_handle = (OnDuplicateKeyHandle)$6;
        load->table_name = (TableName*)$9;
        load->char_set = $10;
        load->fields_info = (FieldsClause*)$11;
        load->lines_info = (LinesClause*)$12;
        load->ignore_lines = $13;
        if ($14 != nullptr) {
            load->columns.reserve($14->children.size(), parser->arena);
            for (int i = 0; i < $14->children.size(); i++) {
                ColumnName* column = (ColumnName*)$14->children[i];
                load->columns.push_back(column, parser->arena);
            }
        }
        if ($15 != nullptr) {
            load->set_list.reserve($15->children.size(), parser->arena);
            for (int i = 0; i < $15->children.size(); i++) {
                Assignment* assign = (Assignment*)$15->children[i];
                load->set_list.push_back(assign, parser->arena);
            }
        }
        $$ = load;
    }
    ;

// 只支持文件和baikaldb在一台机器
LocalOpt:
    {
        $$ = true;
    }
    |  LOCAL {
        $$ = true;
    }
    ;

IgnoreLines:
    {
        $$ = 0;
    }
    | IGNORE INTEGER_LIT LINES
    {
        $$ = ((LiteralExpr*)$2)->_u.int64_val;
    }
    ;

DuplicateOpt:
    {
        $$ = ON_DUPLICATE_KEY_ERROR;
    }
    | IGNORE
    {
        $$ = ON_DUPLICATE_KEY_IGNORE;
    }
    | REPLACE
    {
        $$ = ON_DUPLICATE_KEY_REPLACE;
    }
    ;

Fields:
    {
        FieldsClause* fields_info = new_node(FieldsClause);
        fields_info->terminated = "\t";
        fields_info->escaped = "\\";
        fields_info->enclosed = nullptr;
        $$ = fields_info;
    }
    | FieldsOrColumns FieldItemList {
        FieldsClause* fields_info = new_node(FieldsClause);
        fields_info->terminated = "\t";
        fields_info->escaped = "\\";
        fields_info->enclosed = nullptr;
        for (int i = 0; i < $2->children.size(); i++) {
            FieldItem* item = (FieldItem*)$2->children[i];
            switch (item->type) {
                case LOAD_TERMINATED: {
                    fields_info->terminated = item->value;
                    break;
                }
                case LOAD_ENCLOSED: {
                    fields_info->enclosed = item->value;
                    fields_info->opt_enclosed = item->opt_enclosed;
                    break;
                }
                case LOAD_ESCAPED: {
                    fields_info->escaped = item->value;
                    break;
                }
            }
        }
        $$ = fields_info;
    }
    ;

FieldsOrColumns:
    FIELDS
    |  COLUMNS
    ;

FieldItemList:
    FieldItem
    {
        Node* list = new_node(Node);
        list->children.reserve(5, parser->arena);
        list->children.push_back($1, parser->arena);
        $$ = list;
    }
    | FieldItemList FieldItem
    {
        $1->children.push_back($2, parser->arena);
        $$ = $1;
    }
    ;

FieldItem:
    TERMINATED BY STRING_LIT
    {
        FieldItem* field_item = new_node(FieldItem);
        field_item->type = LOAD_TERMINATED;
        field_item->value = ((LiteralExpr*)$3)->_u.str_val;;
        $$ = field_item;
    }
    | OPTIONALLY ENCLOSED BY STRING_LIT
    {
        FieldItem* field_item = new_node(FieldItem);
        field_item->type = LOAD_ENCLOSED;
        field_item->value = ((LiteralExpr*)$4)->_u.str_val;;
        field_item->opt_enclosed = true;
        if (field_item->value.to_string() != "\\" && field_item->value.length > 1) {
            sql_error(&@2, yyscanner, parser, "wrong terminator.");
            return -1;
        }
        $$ = field_item;
    }
    | ENCLOSED BY STRING_LIT
    {
        FieldItem* field_item = new_node(FieldItem);
        field_item->type = LOAD_ENCLOSED;
        field_item->value = ((LiteralExpr*)$3)->_u.str_val;;
        if (field_item->value.to_string()  != "\\" && field_item->value.length > 1) {
            sql_error(&@2, yyscanner, parser, "wrong terminator.");
            return -1;
        }
        $$ = field_item;
    }
    | ESCAPED BY STRING_LIT
    {
        FieldItem* field_item = new_node(FieldItem);
        field_item->type = LOAD_ESCAPED;
        field_item->value = ((LiteralExpr*)$3)->_u.str_val;;
        if (field_item->value.to_string() != "\\" && field_item->value.length > 1) {
            sql_error(&@2, yyscanner, parser, "wrong terminator.");
            return -1;
        }
        $$ = field_item;
    }
    ;

Lines:
    {
        LinesClause* lines = new_node(LinesClause);
        lines->terminated = "\n";
        lines->starting = nullptr;
        $$ = lines;
    }
    |	LINES Starting LinesTerminated
    {
        LinesClause* lines = new_node(LinesClause);
        lines->starting = $2;
        lines->terminated = $3;
        $$ = lines;
    }
    ;

Starting:
    {
        $$ = "";
    }
    | STARTING BY STRING_LIT
    {
        $$ = ((LiteralExpr*)$3)->_u.str_val;;
    }
    ;

LinesTerminated:
    {
        $$ = "\n";
    }
    | TERMINATED BY STRING_LIT
    {
        $$ = ((LiteralExpr*)$3)->_u.str_val;;
    }
    ;

ColumnNameOrUserVarListOptWithBrackets:
    {
        $$ = nullptr;
    }
    | '(' ColumnNameListOpt ')'
    {
        $$ = $2;
    }
    ;

LoadDataSetSpecOpt:
    {
        $$ = nullptr;
    }
    | SET AssignmentList
    {
        $$ = $2;
    }
    ;

ExplainableStmt:
    SelectStmt
    | DeleteStmt
    | UpdateStmt
    | InsertStmt
    | ReplaceStmt
    | LoadDataStmt
    ;

//CreateUserStmt:
//    CREATE USER IfNotExists UserSpecList {
//        // See https://dev.mysql.com/doc/refman/5.7/en/create-user.html
//    }
//    ;
//
//UserSpecList:
//    UserSpec {
//    }
//    | UserSpecList ',' UserSpec {
//    }
//    ;
//
//UserSpec:
//    Username AuthOption {
//    }
//    ;
//
//Username:
//    STRING_LIT {
//    }
//    | STRING_LIT '@' STRING_LIT {
//    }
//    ;
//
//AuthOption:
//    {
//        $$ = nil;
//    }
//    | IDENTIFIED BY STRING_LIT {
//        $$ = $3;
//    }
//    | IDENTIFIED BY PASSWORD STRING_LIT {
//        $$ = $4;
//    }
//    ;

%%
int sql_error(YYLTYPE* yylloc, yyscan_t yyscanner, SqlParser *parser, const char *s) {    
    parser->error = parser::SYNTAX_ERROR;
    std::ostringstream os;
    os << s << ", near " << std::string(yylloc->start, yylloc->end - yylloc->start);
    os << "] key:" << sql_get_text(yyscanner);
    parser->syntax_err_str = os.str();
    return 1;
    //printf("sql_error");
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
