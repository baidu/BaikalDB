#!/bin/sh
#Created on 2017-11-22 
#测试场景：完成的meta_server流程

#创建namespace
echo -e "创建namespace\n"
curl -d '{
    "op_type":"OP_CREATE_NAMESPACE",
    "namespace_info":{
        "namespace_name": "TEST_NAMESPACE",
        "quota": 1048576
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

#查询nemaspace
curl -d '{
    "op_type" : "QUERY_NAMESPACE"
}' http://$1/MetaService/query
echo -e "\n"

