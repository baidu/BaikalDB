%{

#include <stdio.h>
#include <string.h>
#include "parser.h"
using parser::SqlParser;
using parser::InsertStmt;
using parser::Node;
using parser::ExprNode;
using parser::StmtNode;
using parser::String;
using parser::RowExpr;
using parser::ColumnName;
using parser::TableName;
using namespace parser;
#include "sql_parse.yacc.hh"
using parser::LiteralExpr;
using parser::SqlParser;
 //redefine the lex
#define YY_DECL int sql_lex(YYSTYPE *yylval, YYLTYPE* yylloc, yyscan_t yyscanner, SqlParser* parser)
#define YY_USER_ACTION yylloc->start = yytext;\
  yylloc->end = yytext + yyleng;
void un_reserved_keyword(YYSTYPE *yylval, yyscan_t yyscanner, SqlParser* parser);
%}

%option noyywrap case-insensitive yylineno
%option warn
%option 8bit
%option reentrant
%option prefix="sql_"
%option outfile="sql_lex.flex.cc"
%option header-file="sql_lex.flex.h"

%%
ADD { return ADD; /* The following tokens belong to ReservedKeyword. */}
ALL { return ALL; }
ALTER { return ALTER; }
ANALYZE { return ANALYZE; }
AND { return AND; }
AS { return AS; }
ASC { return ASC; }
BETWEEN { return BETWEEN; }
BIGINT { return BIGINT; }
BINARY { return BINARY; }
BLOB { return BLOB; }
BOTH { un_reserved_keyword(yylval, yyscanner, parser); return BOTH; }
BY { return BY; }
CASCADE { return CASCADE; }
CASE { return CASE; }
CHANGE { return CHANGE; }
CHARACTER { return CHARACTER; }
CHAR { return CHAR; }
CHECK { return CHECK; }
COLLATE { return COLLATE; }
COLUMN { return COLUMN; }
CONSTRAINT { return CONSTRAINT; }
CONVERT { un_reserved_keyword(yylval, yyscanner, parser); return CONVERT;}
CREATE { return CREATE; }
CROSS { return CROSS; }
CURRENT_DATE { un_reserved_keyword(yylval, yyscanner, parser); return CURRENT_DATE; }
CURRENT_TIME { un_reserved_keyword(yylval, yyscanner, parser); return CURRENT_TIME; }
CURRENT_USER { return CURRENT_USER; }
DATABASE { return DATABASE; }
DATABASES { return DATABASES; }
DAY_HOUR { un_reserved_keyword(yylval, yyscanner, parser); return DAY_HOUR; }
DAY_MICROSECOND { un_reserved_keyword(yylval, yyscanner, parser); return DAY_MICROSECOND; }
DAY_MINUTE { un_reserved_keyword(yylval, yyscanner, parser); return DAY_MINUTE; }
DAY_SECOND { un_reserved_keyword(yylval, yyscanner, parser); return DAY_SECOND; }
DECIMAL { return DECIMAL; }
DEFAULT { return DEFAULT; }
DELAYED { return DELAYED; }
DELETE { return DELETE; }
DESC { return DESC; }
DESCRIBE { return DESCRIBE; }
DISTINCT { return DISTINCT; }
DISTINCTROW { return DISTINCTROW; }
DIV { return DIV; }
DOUBLE { return DOUBLE; }
DROP { return DROP; }
DUAL { return DUAL; }
ELSE { return ELSE; }
ENCLOSED { return ENCLOSED; }
ESCAPED { return ESCAPED; }
EXISTS { return EXISTS; }
EXPLAIN { return EXPLAIN; }
FALSE { return FALSE; }
FLOAT { return FLOAT; }
FOR { return FOR; }
FORCE { return FORCE; }
FOREIGN { return FOREIGN; }
FROM { return FROM; }
FULLTEXT { return FULLTEXT; }
GENERATED { return GENERATED; }
GRANT { return GRANT; }
GROUP { return GROUP; }
HAVING { return HAVING; }
HIGH_PRIORITY { return HIGH_PRIORITY; }
HOUR_MICROSECOND { un_reserved_keyword(yylval, yyscanner, parser); return HOUR_MICROSECOND; }
HOUR_MINUTE { un_reserved_keyword(yylval, yyscanner, parser); return HOUR_MINUTE; }
HOUR_SECOND { un_reserved_keyword(yylval, yyscanner, parser); return HOUR_SECOND; }
IF { return IF; }
IGNORE { return IGNORE; }
IN { return IN; }
INDEX { return INDEX; }
INFILE { return INFILE; }
INNER { return INNER; }
INTEGER { return INTEGER; }
INTERVAL { return INTERVAL; }
INTO { return INTO; }
IS { return IS; }
INSERT { return INSERT; }
INT { return INT; }
INT1 { return INT1; }
INT2 { return INT2; }
INT3 { return INT3; }
INT4 { return INT4; }
INT8 { return INT8; }
JOIN { return JOIN; }
KEY { return KEY; }
KEYS { return KEYS; }
KILL { return KILL; }
LEADING { un_reserved_keyword(yylval, yyscanner, parser); return LEADING; }
LEFT { return LEFT; }
LIKE { return LIKE; }
EXACT_LIKE { return EXACT_LIKE; }
LIMIT { return LIMIT; }
LINES { return LINES; }
LOAD { return LOAD; }
LOCALTIME { return LOCALTIME; }
LOCALTIMESTAMP { return LOCALTIMESTAMP; }
LOCK { return LOCK; }
LONGBLOB { return LONGBLOB; }
LONGTEXT { return LONGTEXT; }
LOW_PRIORITY { return LOW_PRIORITY; }
MATCH {return MATCH; }
MAXVALUE { return MAXVALUE; }
MEDIUMBLOB { return MEDIUMBLOB; }
MEDIUMINT { return MEDIUMINT; }
MEDIUMTEXT { return MEDIUMTEXT; }
MINUTE_MICROSECOND { un_reserved_keyword(yylval, yyscanner, parser); return MINUTE_MICROSECOND; }
MINUTE_SECOND { un_reserved_keyword(yylval, yyscanner, parser); return MINUTE_SECOND; }
MOD { un_reserved_keyword(yylval, yyscanner, parser); return MOD; }
NOT { return NOT; }
NO_WRITE_TO_BINLOG { return NO_WRITE_TO_BINLOG; }
NULL { return NULLX; }
NUMERIC { return NUMERIC; }
NVARCHAR { return NVARCHAR; }
ON { return ON; }
OPTION { return OPTION; }
OR { return OR; }
ORDER { return ORDER; }
OUTER { return OUTER; }
PACK_KEYS { return PACK_KEYS; }
PARTITION { return PARTITION; }
PRECISION { return PRECISION; }
PRIMARY { un_reserved_keyword(yylval, yyscanner, parser); return PRIMARY; }
PROCEDURE { return PROCEDURE; }
SHARD_ROW_ID_BITS { return SHARD_ROW_ID_BITS; }
RANGE { return RANGE; }
READ { return READ; }
REAL { return REAL; }
REFERENCES { return REFERENCES; }
REGEXP { return REGEXP; }
RENAME { return RENAME; }
REPEAT { return REPEAT; }
REPLACE { return REPLACE; }
RESTRICT { return RESTRICT; }
REVOKE { return REVOKE; }
RIGHT { return RIGHT; }
RLIKE { return RLIKE; }
SECOND_MICROSECOND { un_reserved_keyword(yylval, yyscanner, parser); return SECOND_MICROSECOND; }
SELECT { return SELECT; }
SET { return SET; }
SHOW { return SHOW; }
SMALLINT { return SMALLINT; }
SQL { return SQL; }
SQL_CALC_FOUND_ROWS { return SQL_CALC_FOUND_ROWS; }
STARTING { return STARTING; }
STRAIGHT_JOIN { return STRAIGHT_JOIN; }
TABLE { return TABLE; }
STORED { return STORED; }
TERMINATED { return TERMINATED; }
OPTIONALLY { return OPTIONALLY; }
THEN { return THEN; }
TINYBLOB { return TINYBLOB; }
TINYINT { return TINYINT; }
TINYTEXT { return TINYTEXT; }
TO { return TO; }
TRAILING { un_reserved_keyword(yylval, yyscanner, parser); return TRAILING; }
TRIGGER { return TRIGGER; }
TRUE { return TRUE; }
UNIQUE { return UNIQUE; }
UNION { return UNION; }
UNLOCK { return UNLOCK; }
UNSIGNED { return UNSIGNED; }
UPDATE { return UPDATE; }
USAGE { return USAGE; }
USE { return USE; }
USING { return USING; }
UTC_DATE { return UTC_DATE; }
UTC_TIME { return UTC_TIME; }
VALUES { return VALUES; }
LONG { return LONG; }
VARCHAR { return VARCHAR; }
VARBINARY { return VARBINARY; }
_BINARY { return _BINARY; }
VIRTUAL { return VIRTUAL; }
WHEN { return WHEN; }
WHERE { return WHERE; }
WRITE { return WRITE; }
WITH { return WITH; }
XOR { return XOR; }
YEAR_MONTH { un_reserved_keyword(yylval, yyscanner, parser); return YEAR_MONTH; }
ZEROFILL { return ZEROFILL; }
NATURAL { return NATURAL; }

ACTION { un_reserved_keyword(yylval, yyscanner, parser); return ACTION; /* The following tokens belong to UnReservedKeyword. */}
AFTER { un_reserved_keyword(yylval, yyscanner, parser); return AFTER; }
AGAINST {un_reserved_keyword(yylval, yyscanner, parser); return AGAINST; }
ALWAYS { un_reserved_keyword(yylval, yyscanner, parser); return ALWAYS; }
ALGORITHM { un_reserved_keyword(yylval, yyscanner, parser); return ALGORITHM; }
ANY { un_reserved_keyword(yylval, yyscanner, parser); return ANY; }
ASCII { un_reserved_keyword(yylval, yyscanner, parser); return ASCII; }
AUTO_INCREMENT { un_reserved_keyword(yylval, yyscanner, parser); return AUTO_INCREMENT; }
AVG_ROW_LENGTH { un_reserved_keyword(yylval, yyscanner, parser); return AVG_ROW_LENGTH; }
AVG { un_reserved_keyword(yylval, yyscanner, parser); return AVG; }
BEGIN { un_reserved_keyword(yylval, yyscanner, parser); return BEGINX; }
WORK  { un_reserved_keyword(yylval, yyscanner, parser); return WORK; }
BINLOG { un_reserved_keyword(yylval, yyscanner, parser); return BINLOG; }
BIT { un_reserved_keyword(yylval, yyscanner, parser); return BIT; }
BOOLEAN { un_reserved_keyword(yylval, yyscanner, parser); return BOOLEAN; }
BOOL { un_reserved_keyword(yylval, yyscanner, parser); return BOOL; }
BTREE { un_reserved_keyword(yylval, yyscanner, parser); return BTREE; }
BYTE { un_reserved_keyword(yylval, yyscanner, parser); return BYTE; }
CASCADED { un_reserved_keyword(yylval, yyscanner, parser); return CASCADED; }
CHARSET { un_reserved_keyword(yylval, yyscanner, parser); return CHARSET; }
CHECKSUM { un_reserved_keyword(yylval, yyscanner, parser); return CHECKSUM; }
CLEANUP { un_reserved_keyword(yylval, yyscanner, parser); return CLEANUP; }
CLIENT { un_reserved_keyword(yylval, yyscanner, parser); return CLIENT; }
COALESCE { un_reserved_keyword(yylval, yyscanner, parser); return COALESCE; }
COLLATION { un_reserved_keyword(yylval, yyscanner, parser); return COLLATION; }
COLUMNS { un_reserved_keyword(yylval, yyscanner, parser); return COLUMNS; }
COMMENT { un_reserved_keyword(yylval, yyscanner, parser); return COMMENT; }
COMMIT { un_reserved_keyword(yylval, yyscanner, parser); return COMMIT; }
COMMITTED { un_reserved_keyword(yylval, yyscanner, parser); return COMMITTED; }
COMPACT { un_reserved_keyword(yylval, yyscanner, parser); return COMPACT; }
COMPRESSED { un_reserved_keyword(yylval, yyscanner, parser); return COMPRESSED; }
COMPRESSION { un_reserved_keyword(yylval, yyscanner, parser); return COMPRESSION; }
CONNECTION { un_reserved_keyword(yylval, yyscanner, parser); return CONNECTION; }
CONSISTENT { un_reserved_keyword(yylval, yyscanner, parser); return CONSISTENT; }
DAY { un_reserved_keyword(yylval, yyscanner, parser); return DAY; }
DATA { un_reserved_keyword(yylval, yyscanner, parser); return DATA; }
DATE { un_reserved_keyword(yylval, yyscanner, parser); return DATE; }
DATETIME { un_reserved_keyword(yylval, yyscanner, parser); return DATETIME; }
DEALLOCATE { un_reserved_keyword(yylval, yyscanner, parser); return DEALLOCATE; }
DEFINER { un_reserved_keyword(yylval, yyscanner, parser); return DEFINER; }
DELAY_KEY_WRITE { un_reserved_keyword(yylval, yyscanner, parser); return DELAY_KEY_WRITE; }
DISABLE { un_reserved_keyword(yylval, yyscanner, parser); return DISABLE; }
DO { un_reserved_keyword(yylval, yyscanner, parser); return DO; }
DUPLICATE { un_reserved_keyword(yylval, yyscanner, parser); return DUPLICATE; }
DYNAMIC { un_reserved_keyword(yylval, yyscanner, parser); return DYNAMIC; }
ENABLE { un_reserved_keyword(yylval, yyscanner, parser); return ENABLE; }
END { un_reserved_keyword(yylval, yyscanner, parser); return END; }
ENGINE { un_reserved_keyword(yylval, yyscanner, parser); return ENGINE; }
ENGINES { un_reserved_keyword(yylval, yyscanner, parser); return ENGINES; }
ENUM { un_reserved_keyword(yylval, yyscanner, parser); return ENUM; }
EVENT { un_reserved_keyword(yylval, yyscanner, parser); return EVENT; }
EVENTS { un_reserved_keyword(yylval, yyscanner, parser); return EVENTS; }
ESCAPE { un_reserved_keyword(yylval, yyscanner, parser); return ESCAPE; }
EXCLUSIVE { un_reserved_keyword(yylval, yyscanner, parser); return EXCLUSIVE; }
EXECUTE { un_reserved_keyword(yylval, yyscanner, parser); return EXECUTE; }
FIELDS { un_reserved_keyword(yylval, yyscanner, parser); return FIELDS; }
FIRST { un_reserved_keyword(yylval, yyscanner, parser); return FIRST; }
FIXED { un_reserved_keyword(yylval, yyscanner, parser); return FIXED; }
FLUSH { un_reserved_keyword(yylval, yyscanner, parser); return FLUSH; }
FORMAT { un_reserved_keyword(yylval, yyscanner, parser); return FORMAT; }
FULL { un_reserved_keyword(yylval, yyscanner, parser); return FULL; }
FUNCTION { un_reserved_keyword(yylval, yyscanner, parser); return FUNCTION; }
GRANTS { un_reserved_keyword(yylval, yyscanner, parser); return GRANTS; }
HASH { un_reserved_keyword(yylval, yyscanner, parser); return HASH; }
HOUR { un_reserved_keyword(yylval, yyscanner, parser); return HOUR; }
IDENTIFIED { un_reserved_keyword(yylval, yyscanner, parser); return IDENTIFIED; }
ISOLATION { un_reserved_keyword(yylval, yyscanner, parser); return ISOLATION; }
INDEXES { un_reserved_keyword(yylval, yyscanner, parser); return INDEXES; }
INVOKER { un_reserved_keyword(yylval, yyscanner, parser); return INVOKER; }
JSON { un_reserved_keyword(yylval, yyscanner, parser); return JSON; }
KEY_BLOCK_SIZE { un_reserved_keyword(yylval, yyscanner, parser); return KEY_BLOCK_SIZE; }
LANGUAGE { un_reserved_keyword(yylval, yyscanner, parser); return LANGUAGE; }
LOCAL { un_reserved_keyword(yylval, yyscanner, parser); return LOCAL; }
LESS { un_reserved_keyword(yylval, yyscanner, parser); return LESS; }
LEVEL { un_reserved_keyword(yylval, yyscanner, parser); return LEVEL; }
MASTER { un_reserved_keyword(yylval, yyscanner, parser); return MASTER; }
MICROSECOND { un_reserved_keyword(yylval, yyscanner, parser); return MICROSECOND; }
MINUTE { un_reserved_keyword(yylval, yyscanner, parser); return MINUTE; }
MODE { un_reserved_keyword(yylval, yyscanner, parser); return MODE; }
MODIFY { un_reserved_keyword(yylval, yyscanner, parser); return MODIFY; }
MONTH { un_reserved_keyword(yylval, yyscanner, parser); return MONTH; }
MAX_ROWS { un_reserved_keyword(yylval, yyscanner, parser); return MAX_ROWS; }
MAX_CONNECTIONS_PER_HOUR { un_reserved_keyword(yylval, yyscanner, parser); return MAX_CONNECTIONS_PER_HOUR; }
MAX_QUERIES_PER_HOUR { un_reserved_keyword(yylval, yyscanner, parser); return MAX_QUERIES_PER_HOUR; }
MAX_UPDATES_PER_HOUR { un_reserved_keyword(yylval, yyscanner, parser); return MAX_UPDATES_PER_HOUR; }
MAX_USER_CONNECTIONS { un_reserved_keyword(yylval, yyscanner, parser); return MAX_USER_CONNECTIONS; }
MERGE { un_reserved_keyword(yylval, yyscanner, parser); return MERGE; }
MIN_ROWS { un_reserved_keyword(yylval, yyscanner, parser); return MIN_ROWS; }
NAMES { un_reserved_keyword(yylval, yyscanner, parser); return NAMES; }
NATIONAL { un_reserved_keyword(yylval, yyscanner, parser); return NATIONAL; }
NO { un_reserved_keyword(yylval, yyscanner, parser); return NO; }
NONE { un_reserved_keyword(yylval, yyscanner, parser); return NONE; }
OFFSET { un_reserved_keyword(yylval, yyscanner, parser); return OFFSET; }
ONLY { un_reserved_keyword(yylval, yyscanner, parser); return ONLY; }
PASSWORD { un_reserved_keyword(yylval, yyscanner, parser); return PASSWORD; }
PARTITIONS { un_reserved_keyword(yylval, yyscanner, parser); return PARTITIONS; }
PLUGINS { un_reserved_keyword(yylval, yyscanner, parser); return PLUGINS; }
PREPARE { un_reserved_keyword(yylval, yyscanner, parser); return PREPARE; }
PRIVILEGES { un_reserved_keyword(yylval, yyscanner, parser); return PRIVILEGES; }
PROCESS { un_reserved_keyword(yylval, yyscanner, parser); return PROCESS; }
PROCESSLIST { un_reserved_keyword(yylval, yyscanner, parser); return PROCESSLIST; }
PROFILES { un_reserved_keyword(yylval, yyscanner, parser); return PROFILES; }
QUARTER { un_reserved_keyword(yylval, yyscanner, parser); return QUARTER; }
QUERY { un_reserved_keyword(yylval, yyscanner, parser); return QUERY; }
QUERIES { un_reserved_keyword(yylval, yyscanner, parser); return QUERIES; }
QUICK { un_reserved_keyword(yylval, yyscanner, parser); return QUICK; }
RECOVER { un_reserved_keyword(yylval, yyscanner, parser); return RECOVER; }
RESTORE { un_reserved_keyword(yylval, yyscanner, parser); return RESTORE; }
REDUNDANT { un_reserved_keyword(yylval, yyscanner, parser); return REDUNDANT; }
RELOAD { un_reserved_keyword(yylval, yyscanner, parser); return RELOAD; }
REPEATABLE { un_reserved_keyword(yylval, yyscanner, parser); return REPEATABLE; }
REPLICATION { un_reserved_keyword(yylval, yyscanner, parser); return REPLICATION; }
REVERSE { un_reserved_keyword(yylval, yyscanner, parser); return REVERSE; }
ROLLBACK { un_reserved_keyword(yylval, yyscanner, parser); return ROLLBACK; }
ROUTINE { un_reserved_keyword(yylval, yyscanner, parser); return ROUTINE; }
ROW { un_reserved_keyword(yylval, yyscanner, parser); return ROW; }
ROW_COUNT { un_reserved_keyword(yylval, yyscanner, parser); return ROW_COUNT; }
ROW_FORMAT { un_reserved_keyword(yylval, yyscanner, parser); return ROW_FORMAT; }
SECOND { un_reserved_keyword(yylval, yyscanner, parser); return SECOND; }
SECURITY { un_reserved_keyword(yylval, yyscanner, parser); return SECURITY; }
SEPARATOR { un_reserved_keyword(yylval, yyscanner, parser); return SEPARATOR; }
SERIALIZABLE { un_reserved_keyword(yylval, yyscanner, parser); return SERIALIZABLE; }
SESSION { un_reserved_keyword(yylval, yyscanner, parser); return SESSION; }
SHARE { un_reserved_keyword(yylval, yyscanner, parser); return SHARE; }
SHARED { un_reserved_keyword(yylval, yyscanner, parser); return SHARED; }
SIGNED { un_reserved_keyword(yylval, yyscanner, parser); return SIGNED; }
SLAVE { un_reserved_keyword(yylval, yyscanner, parser); return SLAVE; }
SNAPSHOT { un_reserved_keyword(yylval, yyscanner, parser); return SNAPSHOT; }
SQL_CACHE { un_reserved_keyword(yylval, yyscanner, parser); return SQL_CACHE; }
SQL_NO_CACHE { un_reserved_keyword(yylval, yyscanner, parser); return SQL_NO_CACHE; }
START { un_reserved_keyword(yylval, yyscanner, parser); return START; }
STATS_PERSISTENT { un_reserved_keyword(yylval, yyscanner, parser); return STATS_PERSISTENT; }
STATUS { un_reserved_keyword(yylval, yyscanner, parser); return STATUS; }
SUPER { un_reserved_keyword(yylval, yyscanner, parser); return SUPER; }
SOME { un_reserved_keyword(yylval, yyscanner, parser); return SOME; }
SWAP { un_reserved_keyword(yylval, yyscanner, parser); return SWAP; }
GLOBAL { un_reserved_keyword(yylval, yyscanner, parser); return GLOBAL; }
TABLES { un_reserved_keyword(yylval, yyscanner, parser); return TABLES; }
TEMPORARY { un_reserved_keyword(yylval, yyscanner, parser); return TEMPORARY; }
TEMPTABLE { un_reserved_keyword(yylval, yyscanner, parser); return TEMPTABLE; }
TEXT { un_reserved_keyword(yylval, yyscanner, parser); return TEXT; }
THAN { un_reserved_keyword(yylval, yyscanner, parser); return THAN; }
TIME { un_reserved_keyword(yylval, yyscanner, parser); return TIME; }
TIMESTAMP { un_reserved_keyword(yylval, yyscanner, parser); return TIMESTAMP; }
TRACE { un_reserved_keyword(yylval, yyscanner, parser); return TRACE; }
TRANSACTION { un_reserved_keyword(yylval, yyscanner, parser); return TRANSACTION; }
TRIGGERS { un_reserved_keyword(yylval, yyscanner, parser); return TRIGGERS; }
TRUNCATE { un_reserved_keyword(yylval, yyscanner, parser); return TRUNCATE; }
UNCOMMITTED { un_reserved_keyword(yylval, yyscanner, parser); return UNCOMMITTED; }
UNKNOWN { un_reserved_keyword(yylval, yyscanner, parser); return UNKNOWN; }
USER { un_reserved_keyword(yylval, yyscanner, parser); return USER; }
UNDEFINED { un_reserved_keyword(yylval, yyscanner, parser); return UNDEFINED; }
VALUE { un_reserved_keyword(yylval, yyscanner, parser); return VALUE; }
VARIABLES { un_reserved_keyword(yylval, yyscanner, parser); return VARIABLES; }
VIEW { un_reserved_keyword(yylval, yyscanner, parser); return VIEW; }
WARNINGS { un_reserved_keyword(yylval, yyscanner, parser); return WARNINGS; }
WEEK { un_reserved_keyword(yylval, yyscanner, parser); return WEEK; }
YEAR { un_reserved_keyword(yylval, yyscanner, parser); return YEAR; }
HLL { un_reserved_keyword(yylval, yyscanner, parser); return HLL; }
BITMAP { un_reserved_keyword(yylval, yyscanner, parser); return BITMAP; }
TDIGEST { un_reserved_keyword(yylval, yyscanner, parser); return TDIGEST; }
LEARNER { un_reserved_keyword(yylval, yyscanner, parser); return LEARNER; }

ADDDATE { un_reserved_keyword(yylval, yyscanner, parser); return ADDDATE; /* The following tokens belong to builtin functions. */}
BIT_AND { un_reserved_keyword(yylval, yyscanner, parser); return BIT_AND; }
BIT_OR { un_reserved_keyword(yylval, yyscanner, parser); return BIT_OR; }
BIT_XOR { un_reserved_keyword(yylval, yyscanner, parser); return BIT_XOR; }
CAST { un_reserved_keyword(yylval, yyscanner, parser); return CAST; }
COUNT { un_reserved_keyword(yylval, yyscanner, parser); return COUNT; }
CURDATE { un_reserved_keyword(yylval, yyscanner, parser); return CURDATE; }
CURTIME { un_reserved_keyword(yylval, yyscanner, parser); return CURTIME; }
DATE_ADD { un_reserved_keyword(yylval, yyscanner, parser); return DATE_ADD; }
DATE_SUB { un_reserved_keyword(yylval, yyscanner, parser); return DATE_SUB; }
EXTRACT { un_reserved_keyword(yylval, yyscanner, parser); return EXTRACT; }
GROUP_CONCAT { un_reserved_keyword(yylval, yyscanner, parser); return GROUP_CONCAT; }
MAX { un_reserved_keyword(yylval, yyscanner, parser); return MAX; }
MID { un_reserved_keyword(yylval, yyscanner, parser); return MID; }
MIN { un_reserved_keyword(yylval, yyscanner, parser); return MIN; }
NOW { un_reserved_keyword(yylval, yyscanner, parser); return NOW; }
CURRENT_TIMESTAMP { un_reserved_keyword(yylval, yyscanner, parser); return CURRENT_TIMESTAMP; }
UTC_TIMESTAMP { un_reserved_keyword(yylval, yyscanner, parser); return UTC_TIMESTAMP; }
POSITION { un_reserved_keyword(yylval, yyscanner, parser); return POSITION; }
SESSION_USER { un_reserved_keyword(yylval, yyscanner, parser); return SESSION_USER; }
STD { un_reserved_keyword(yylval, yyscanner, parser); return STD; }
STDDEV { un_reserved_keyword(yylval, yyscanner, parser); return STDDEV; }
STDDEV_POP { un_reserved_keyword(yylval, yyscanner, parser); return STDDEV_POP; }
STDDEV_SAMP { un_reserved_keyword(yylval, yyscanner, parser); return STDDEV_SAMP; }
SUBDATE { un_reserved_keyword(yylval, yyscanner, parser); return SUBDATE; }
SUBSTR { un_reserved_keyword(yylval, yyscanner, parser); return SUBSTR; }
SUBSTRING { un_reserved_keyword(yylval, yyscanner, parser); return SUBSTRING; }
TIMESTAMPADD { un_reserved_keyword(yylval, yyscanner, parser); return TIMESTAMPADD; }
TIMESTAMPDIFF { un_reserved_keyword(yylval, yyscanner, parser); return TIMESTAMPDIFF; }
SUM { un_reserved_keyword(yylval, yyscanner, parser); return SUM; }
SYSDATE { un_reserved_keyword(yylval, yyscanner, parser); return SYSDATE; }
SYSTEM_USER { un_reserved_keyword(yylval, yyscanner, parser); return SYSTEM_USER; }
TRIM { un_reserved_keyword(yylval, yyscanner, parser); return TRIM; }
VARIANCE { un_reserved_keyword(yylval, yyscanner, parser); return VARIANCE; }
VAR_POP { un_reserved_keyword(yylval, yyscanner, parser); return VAR_POP; }
VAR_SAMP { un_reserved_keyword(yylval, yyscanner, parser); return VAR_SAMP; }
([A-Za-z0-9_]+)_AGG {un_reserved_keyword(yylval, yyscanner, parser); return USER_AGG;}

! { return NOT_OP;}
= { return EQ_OP; }
:= { return ASSIGN_OP; }
% { return MOD_OP; }
\>= { return GE_OP; }
\> { return GT_OP; }
\<= { return LE_OP; }
\< { return LT_OP; }
!=|\<\> { return NE_OP; }
&& { return AND; }
\|\| { return OR; }
\<\< { return LS_OP; }
\>\> { return RS_OP; }

[0-9]+ {
    //integer
    yylval->expr = LiteralExpr::make_int(yytext, parser->arena);
    return INTEGER_LIT;
}
[0-9]+E[-+]?[0-9]+ |
[0-9]+\.[0-9]*E[-+]?[0-9]+ |
\.[0-9]+E[-+]?[0-9]+ |
[0-9]+\.[0-9]+    |
[0-9]+\.        |
\.[0-9]+ {
    //float point number
    yylval->expr = LiteralExpr::make_double(yytext, parser->arena);
    return DECIMAL_LIT;
}

0[b][0-1]+ {
    // bit
    yylval->expr = LiteralExpr::make_bit(yytext + 2, strlen(yytext) - 2, parser->arena);
    return STRING_LIT;
}

[bB]\'[0-1]*\' {
    // bit
    yylval->expr = LiteralExpr::make_bit(yytext + 2, strlen(yytext) - 3, parser->arena);
    return STRING_LIT;
}

0[x][0-9a-fA-F]+ {
    // hex
    yylval->expr = LiteralExpr::make_hex(yytext + 2, strlen(yytext) - 2, parser->arena);
    return STRING_LIT;
}

[xX]\'[0-9a-fA-F]*\' {
    // hex
    yylval->expr = LiteralExpr::make_hex(yytext + 2, strlen(yytext) - 3, parser->arena);
    return STRING_LIT;
}

\"([^\\\"]|\\.)*\" |
\'([^\\\']|\\.)*\' {
    //string
    LiteralExpr* lit;
    lit = LiteralExpr::make_string(yytext, parser->arena);
    lit->_u.str_val.stripslashes(); // move from logical_plan
    if (parser->is_gbk && parser->has_5c) {
        lit->_u.str_val.restore_5c();
    }
    yylval->expr = lit;
    return STRING_LIT;
}

"?" {
    yylval->expr = LiteralExpr::make_place_holder(parser->place_holder_id, parser->arena);
    parser->place_holder_id++;
    return PLACE_HOLDER_LIT;
}

(([A-Za-z0-9_]+)|(`[^`]+`))((@([A-Za-z0-9_*]+|(`[^`]`)+))*) {
    //identifier
    if (yytext[0] != '`') {
        yylval->string.strdup(yytext, parser->arena);
    } else {
        yylval->string.strdup(yytext + 1, strlen(yytext) - 2 , parser->arena);
    }
    return IDENT;
}

@@?[A-Za-z][A-Za-z0-9_.$]* {
    //variable
    yylval->string.strdup(yytext, parser->arena);
    return IDENT;
}

"--"[ \t].* {/* single line comment; do nothing */}
"#".* {/* single line comment; do nothing */}

[ \t\r\n] { }
[\0] { return 0; }

[-,\.*+(){}|&/^~;] { return yytext[0]; }
[][] { return yytext[0]; }

"/*"([*]*(([^*/])+([/])*)*)*"*/" {/* comment; do nothing */}

. { return CHINESE_DOT; }

%%
void un_reserved_keyword(YYSTYPE* yylval, yyscan_t yyscanner, SqlParser* parser) {
    char* text = yyget_text(yyscanner);
    yylval->string.strdup(text, parser->arena);
}
        
/* vim: set ts=4 sw=4 sts=4 tw=100 noet: */

