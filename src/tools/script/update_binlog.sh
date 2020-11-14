#!/bin/sh
#Created on 2020-6-22

echo -e "\n"
curl -d '{
    "op_type":"OP_UNLINK_BINLOG",
    "table_info": {
        "table_name": "tuuu",
        "database": "Feed",
        "namespace_name": "FENGCHAO"
    },
    "binlog_info": {
        "table_name": "tt",
        "database": "Feed",
        "namespace_name": "FENGCHAO"
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"
