#!/bin/sh
#Created on 2017-11-22 
#测试场景：完成的meta_server流程

echo -e "\n"
#创建table
echo -e "rename table\n"
curl -d '{
    "op_type":"OP_RENAME_TABLE",
    "table_info": {
        "table_name": "TestTb",
        "new_table_name": "TestTb_new",
        "database": "TestDb",
        "namespace_name": "TEST_NAMESPACE"
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

#查询table
curl -d '{
    "op_type" : "QUERY_SCHEMA"
}' http://$1/MetaService/query
