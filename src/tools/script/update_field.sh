#!/bin/sh
#Created on 2020-12-17 
#测试场景：完成的modify field流程

echo -e "\n"
#comment字段用protobuf的bytes类型表示,请求时需要进行base64编码
#可以在线编码https://www.base64encode.org/,"noskip" base64编码后为: "bm9za2lw"
echo -e "修改列信息\n"
curl -d '{
    "op_type":"OP_MODIFY_FIELD",
    "table_info": {
        "table_name": "t",
        "database": "TestDB",
        "namespace_name": "TEST",
        "fields": [ {
                        "field_name" : "id",
                        "comment" : "bm9za2lw" 
                    }]
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

#查询table
curl -d '{
    "op_type" : "QUERY_SCHEMA"
}' http://$1/MetaService/query
echo -e "\n"

