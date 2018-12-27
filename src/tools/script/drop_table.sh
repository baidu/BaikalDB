#!/bin/sh
#Created on 2017-11-22 

echo -e "\n"
echo -e "drop table\n"
curl -d '{
   "op_type":"OP_DROP_TABLE",
    "table_info": {
        "table_name": "clue_history",
        "database": "COP_EXTERNAL",
        "namespace_name": "FENGCHAO"
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type" : "QUERY_SCHEMA"
}' http://$1/MetaService/query


curl -d '{
   "op_type":"OP_DROP_TABLE",
    "table_info": {
        "table_name": "ideacontent_test",
        "database": "FC_Content",
        "namespace_name": "FENGCHAO"
    }
}' http://$1/MetaService/meta_manager
