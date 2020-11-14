#!/bin/sh
#Created on 2017-11-22 
#测试场景：完成的meta_server流程

#增加权限
curl -d '{
    "op_type":"OP_ADD_PRIVILEGE",
    "user_privilege" : {
        "username" : "******",
        "password" : "******",
        "namespace_name" : "TEST",
        "privilege_database" : [{
                                    "database" : "TEST",
                                    "database_rw" : 2                        
                                }]
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

#查询权限
curl -d '{
    "op_type" : "QUERY_USERPRIVILEG"
}' http://$1/MetaService/query
echo -e "\n"
