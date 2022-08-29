#!/bin/sh
#Created on 2017-11-22 
#测试场景：完成的meta_server流程


#创建database
echo -e "创建database\n"
curl -d '{
    "op_type":"OP_CREATE_DATABASE",
    "database_info": {
        "database":"TestDB",
        "namespace_name":"TEST_NAMESPACE",
        "quota": 524288
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"
