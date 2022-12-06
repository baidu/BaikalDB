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

namespace parser {

struct StartTxnStmt : public StmtNode {
    StartTxnStmt() {
        node_type = NT_START_TRANSACTION;
    }
};

struct CommitTxnStmt : public StmtNode {
    CommitTxnStmt() {
        node_type = NT_COMMIT_TRANSACTION;
    }
};

struct RollbackTxnStmt : public StmtNode {
    RollbackTxnStmt() {
        node_type = NT_ROLLBACK_TRANSACTION;
    }
};

struct VarAssign : public Node {
    String    key;
    ExprNode* value = nullptr;
    VarAssign() {
        node_type = NT_VAR_ASSIGN;
        key = nullptr;
    }
};

struct SetStmt : public StmtNode {
    Vector<VarAssign*> var_list;
    SetStmt() {
        node_type = NT_SET_CMD;
    }
};

struct NewPrepareStmt : public StmtNode {
    String  name;
    String  sql;
    NewPrepareStmt() {
        node_type = NT_NEW_PREPARE;
        name = nullptr;
        sql = nullptr;
    }
};

struct ExecPrepareStmt : public StmtNode {
    String  name;
    Vector<String> param_list;
    ExecPrepareStmt() {
        node_type = NT_EXEC_PREPARE;
        name = nullptr;
    }
};

struct DeallocPrepareStmt : public StmtNode {
    String  name;
    DeallocPrepareStmt() {
        node_type = NT_DEALLOC_PREPARE;
        name = nullptr;
    }
};

struct KillStmt : public StmtNode {
    int64_t conn_id = 0;
    bool is_query = false;
    KillStmt() {
        node_type = NT_KILL;
    }
};

enum NamespaceOptionType : unsigned char {
    NAMESPACE_OPT_NONE = 0,
    NAMESPACE_OPT_QUOTA,
    NAMESPACE_OPT_RESOURCE_TAG
};

struct NamespaceOption : public Node {
    NamespaceOptionType type = NAMESPACE_OPT_NONE;
    String          str_value;
    uint64_t        uint_value = 0;

    NamespaceOption() {
        node_type = NT_NAMESPACE_OPT;
    }
};

struct CreateNamespaceStmt : public StmtNode {
    bool                if_not_exists  = false;
    String              ns_name;
    Vector<NamespaceOption*> options;
    CreateNamespaceStmt() {
        node_type = NT_CREATE_NAMESPACE;
        ns_name = nullptr;
    }
};
struct DropNamespaceStmt : public StmtNode {
    bool                if_exists  = false;
    String              ns_name;
    DropNamespaceStmt() {
        node_type = NT_DROP_NAMESPACE;
        ns_name = nullptr;
    }
};
struct AlterNamespaceStmt : public StmtNode {
    bool                if_exists  = false;
    String              ns_name;
    Vector<NamespaceOption*> options;
    AlterNamespaceStmt() {
        node_type = NT_ALTER_NAMESPACE;
        ns_name = nullptr;
    }
};

// https://github.com/mysql/mysql-server/blob/5.6/sql/sql_acl.h
enum MysqlACL: uint32_t {
    SELECT_ACL      =   (1U << 0), //SELECT  Enable use of SELECT. Levels: Global, database, table, column.
    INSERT_ACL      =   (1U << 1), //INSERT  Enable use of INSERT. Levels: Global, database, table, column.
    UPDATE_ACL      =   (1U << 2), //UPDATE  Enable use of UPDATE. Levels: Global, database, table, column.
    DELETE_ACL      =   (1U << 3), //DELETE  Enable use of DELETE. Level: Global, database, table.
    CREATE_ACL      =   (1U << 4), //CREATE  Enable database and table creation. Levels: Global, database, table.
    DROP_ACL        =   (1U << 5), //DROP    Enable databases, tables, and views to be dropped. Levels: Global, database, table.
    RELOAD_ACL      =   (1U << 6), //RELOAD  Enable use of FLUSH operations. Level: Global.
    SHUTDOWN_ACL    =   (1U << 7), //SHUTDOWN    Enable use of mysqladmin shutdown. Level: Global.
    PROCESS_ACL     =   (1U << 8), //PROCESS Enable the user to see all processes with SHOW PROCESSLIST. Level: Global.
    FILE_ACL        =   (1U << 9), //FILE    Enable the user to cause the server to read or write files. Level: Global.
    GRANT_ACL       =   (1U << 10), //GRANT OPTION    Enable privileges to be granted to or removed from other accounts. Levels: Global, database, table, routine, proxy.
    REFERENCES_ACL  =   (1U << 11), //REFERENCES  Enable foreign key creation. Levels: Global, database, table, column.
    INDEX_ACL       =   (1U << 12), //INDEX   Enable indexes to be created or dropped. Levels: Global, database, table.
    ALTER_ACL       =   (1U << 13), //ALTER Enable use of ALTER TABLE. Levels: Global, database, table.
    SHOW_DB_ACL     =   (1U << 14), //SHOW DATABASES  Enable SHOW DATABASES to show all databases. Level: Global.
    SUPER_ACL       =   (1U << 15), //SUPER   Enable use of other administrative operations such as CHANGE MASTER TO, KILL, PURGE BINARY LOGS, SET GLOBAL, and mysqladmin debug command. Level: Global.
    CREATE_TMP_ACL  =   (1U << 16), //CREATE TEMPORARY TABLES Enable use of CREATE TEMPORARY TABLE. Levels: Global, database.
    LOCK_TABLES_ACL =   (1U << 17), //LOCK TABLES Enable use of LOCK TABLES on tables for which you have the SELECT privilege. Levels: Global, database.
    EXECUTE_ACL     =   (1U << 18), //EXECUTE Enable the user to execute stored routines. Levels: Global, database, routine.
    REPL_SLAVE_ACL  =   (1U << 19), //REPLICATION SLAVE   Enable replicas to read binary log events from the source. Level: Global.
    REPL_CLIENT_ACL =   (1U << 20), //REPLICATION CLIENT  Enable the user to ask where source or replica servers are. Level: Global.
    CREATE_VIEW_ACL =   (1U << 21), //CREATE VIEW Enable views to be created or altered. Levels: Global, database, table.
    SHOW_VIEW_ACL   =   (1U << 22), //SHOW VIEW   Enable use of SHOW CREATE VIEW. Levels: Global, database, table.
    CREATE_PROC_ACL =   (1U << 23), //CREATE ROUTINE  Enable stored routine creation. Levels: Global, database.
    ALTER_PROC_ACL  =   (1U << 24), //ALTER ROUTINE Enable stored routines to be altered or dropped. Levels: Global, database, routine.
    CREATE_USER_ACL =   (1U << 25), //CREATE USER Enable use of CREATE USER, DROP USER, RENAME USER, and REVOKE ALL PRIVILEGES. Level: Global.
    EVENT_ACL       =   (1U << 26), //EVENT   Enable use of events for the Event Scheduler. Levels: Global, database.
    TRIGGER_ACL     =   (1U << 27), //TRIGGER Enable trigger operations. Levels: Global, database, table.
    CREATE_TABLESPACE_ACL   = (1U << 28), //CREATE TABLESPACE   Enable tablespaces and log file groups to be created, altered, or dropped. Level: Global.
    PROXY_ACL       =   (1U << 29), //PROXY   Enable user proxying. Level: From user to user.
    NO_ACCESS_ACL   =   (1U << 30), //USAGE   Synonym for “no privileges”
    ALL_ACL         =   (1U << 31),
};

#define DB_ACLS \
(UPDATE_ACL | SELECT_ACL | INSERT_ACL | DELETE_ACL | CREATE_ACL | DROP_ACL | \
 REFERENCES_ACL | INDEX_ACL | ALTER_ACL | CREATE_TMP_ACL | \
 LOCK_TABLES_ACL | EXECUTE_ACL | CREATE_VIEW_ACL | SHOW_VIEW_ACL | \
 CREATE_PROC_ACL | ALTER_PROC_ACL | EVENT_ACL | TRIGGER_ACL)

#define TABLE_ACLS \
(SELECT_ACL | INSERT_ACL | UPDATE_ACL | DELETE_ACL | CREATE_ACL | DROP_ACL | \
 REFERENCES_ACL | INDEX_ACL | ALTER_ACL | CREATE_VIEW_ACL | \
 SHOW_VIEW_ACL | TRIGGER_ACL)

#define GLOBAL_ACLS \
(SELECT_ACL | INSERT_ACL | UPDATE_ACL | DELETE_ACL | CREATE_ACL | DROP_ACL | \
 RELOAD_ACL | SHUTDOWN_ACL | PROCESS_ACL | FILE_ACL | \
 REFERENCES_ACL | INDEX_ACL | ALTER_ACL | SHOW_DB_ACL | SUPER_ACL | \
 CREATE_TMP_ACL | LOCK_TABLES_ACL | REPL_SLAVE_ACL | REPL_CLIENT_ACL | \
 EXECUTE_ACL | CREATE_VIEW_ACL | SHOW_VIEW_ACL | CREATE_PROC_ACL | \
 ALTER_PROC_ACL | CREATE_USER_ACL | EVENT_ACL | TRIGGER_ACL | \
 CREATE_TABLESPACE_ACL)


// UserIdentity represents username and hostname.
struct UserIdentity : public Node {
    String  username;
    String  hostname;
    bool    current_user = false;
};

struct AuthOption : public Node {
    String  auth_string; // by default
    String  hash_string; // not support yet
    String  auth_plugin; // not support yet
};

struct UserSpec : public Node {
    UserIdentity*    user;
    AuthOption*      auth_opt;
};

// CreateUserStmt creates user account.
// See https://dev.mysql.com/doc/refman/5.7/en/create-user.html
struct CreateUserStmt : public StmtNode {
    CreateUserStmt() {
        node_type = NT_CREATE_USER;
        namespace_name = nullptr;
    }
    bool                if_not_exists  = false;
    Vector<UserSpec*>   specs;
    String              namespace_name;
};

struct DropUserStmt : public StmtNode {
    DropUserStmt() {
        node_type = NT_DROP_USER;
    }
    bool                    if_exists  = false;
    Vector<UserSpec*>       specs;
};

struct AlterUserStmt : public StmtNode {
    AlterUserStmt() {
        node_type = NT_ALTER_USER;
        namespace_name = nullptr;
    }
    bool                    if_exists  = false;
    Vector<UserSpec*>       specs;
    String                  namespace_name;
};

enum GrantLevelType {
    GRANT_LEVEL_NONE = 0,
    GRANT_LEVEL_GLOBAL = 1,
    GRANT_LEVEL_DB = 2,
    GRANT_LEVEL_TABLE = 3
};

struct PrivType : public Node {
    MysqlACL type;
    uint32_t get_priv_acl(GrantLevelType priv_level) {
        uint32_t acl = type;
        if (acl == ALL_ACL) {
            switch (priv_level) {
                case GRANT_LEVEL_GLOBAL:
                    acl = GLOBAL_ACLS;
                    break;
                case GRANT_LEVEL_DB:
                    acl = DB_ACLS;
                    break;
                case GRANT_LEVEL_TABLE:
                    acl = TABLE_ACLS;
                    break;
            }
        }
        return acl;
    }
};

struct PrivLevel : public Node {
    GrantLevelType level;
    String  db_name;
    String  table_name;
};

struct PrivStmt : public StmtNode {
    Vector<PrivType*>       privs;
    PrivLevel*              priv_level = nullptr;
    Vector<UserSpec*>       specs;
    bool                    with_grant = false;
    String                  namespace_name;
    uint32_t get_acl() {
        uint32_t acl = 0;
        for (int32_t i = 0; i < privs.size(); i++) {
            PrivType* priv = privs[i];
            acl |= priv->get_priv_acl(priv_level->level);
        }
        if (with_grant) {
            acl |= GRANT_ACL;
        }
        return acl;
    }

};

struct GrantStmt : public PrivStmt {
    GrantStmt() {
        node_type = NT_GRANT;
    }
};

struct RevokeStmt : public PrivStmt {
    RevokeStmt() {
        node_type = NT_REVOKE;
    }
};


//
//struct RestoreTableStmt : public DdlNode {
//    Vector<TableName*> table_names;
//
//    RestoreTableStmt() {
//        node_type = NT_RESTORE_TABLE;
//    }
//};


}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
