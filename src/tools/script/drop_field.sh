#!/bin/sh
#Created on 2017-11-22 
#测试场景：完成的meta_server流程

echo -e "\n"
#创建table
echo -e "drop列名\n"
curl -d '{
    "op_type":"OP_DROP_FIELD",
    "table_info": {
        "table_name": "wordinfo_new",
        "database": "TEST",
        "namespace_name": "TEST",
        "fields": [ {
                        "field_name" : ""
                    },
                    {
                        "field_name" : ""
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

