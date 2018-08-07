#!/bin/sh
#Created on 2017-11-22 
#测试场景：完成的meta_server流程

echo -e "\n"
#创建table
echo -e "修改列名\n"
curl -d '{
    "op_type":"OP_ADD_FIELD",
    "table_info": {
        "table_name": "TestTb",
        "database": "TestDB",
        "namespace_name": "TEST_NAMESPACE",
        "fields": [ {
                        "field_name" : "test",
                        "mysql_type": 6 
                    },
                    {
                        "field_name" : "test_word",
                        "mysql_type": 13
                    }
                    ]
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

#查询table
curl -d '{
    "op_type" : "QUERY_SCHEMA" 
}' http://$1/MetaService/query
echo -e "\n"

