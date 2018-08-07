#!/bin/sh
#Created on 2017-11-22 

echo -e "\n"
echo -e "drop table\n"
curl -d '{
   "op_type":"OP_DROP_TABLE",
    "table_info": {
        "table_name": "TestTb",
        "database": "TestDb",
        "namespace_name": "TEST_NAMESPACE"
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type" : "QUERY_SCHEMA"
}' http://$1/MetaService/query


curl -d '{
   "op_type":"OP_DROP_TABLE",
    "table_info": {
        "table_name": "TestTb_test",
        "database": "TestDb",
        "namespace_name": "TEST_NAMESPACE"
    }
}' http://$1/MetaService/meta_manager
