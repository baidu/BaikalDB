#!/bin/sh
#Created on 2017-11-22 
#测试场景：完成的meta_server流程

# handle add_user namespace username password json

#创建用户
curl -d '{
    "op_type":"OP_CREATE_USER",
    "user_privilege" : {
        "username" : "baikal_user",
        "password" : "59ACp2Bl#",
        "namespace_name" : "FENGCHAO",
        "privilege_database" : [{
                                    "database" : "BAIKALDB_ONES_CENTER",
                                    "database_rw" : 2                            
                                }],
        "bns":["preonline", "offline"],
        "ip":["127.0.0.1", "127.0.0.2"]
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

#查询权限
curl -d '{
    "op_type" : "QUERY_USERPRIVILEG"
}' http://$1/MetaService/query
echo -e "\n"
