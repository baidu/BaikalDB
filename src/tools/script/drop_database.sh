#!/bin/sh
#Created on 2017-11-22 
#测试场景：完成的meta_server流程


#删除database
echo -e "drop database\n"
curl -d '{
    "op_type":"OP_DROP_DATABASE",
    "database_info": {
        "database":"TEST",
        "namespace_name":"TEST"
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

#查询database
curl -d '{
    "op_type" : "QUERY_DATABASE" 
}' http://$1/MetaService/query
echo -e "\n"

