#!/bin/sh
#Created on 2017-11-22 

#删除权限
curl -d '{
    "op_type":"OP_DROP_PRIVILEGE",
    "user_privilege" : {
        "username" : "root",
        "password" : "root",
        "namespace_name" : "FENGCHAO",
        "privilege_table" : [{
                                    "database" : "FC_Content",
                                    "table_name" : "wordinfo"                           
                                }]
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

#查询权限
curl -d '{
    "op_type" : "QUERY_USERPRIVILEG"
}' http://$1/MetaService/query
echo -e "\n"
