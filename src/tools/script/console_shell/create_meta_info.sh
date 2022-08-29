#!/bin/bash
  
echo -e " create namespace\n"
curl -d '{
    "op_type":"OP_CREATE_NAMESPACE",
    "namespace_info":{
        "namespace_name": "CLUSTER_STATUS",
        "quota": 5368709120
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

#create database 5G
echo -e "create database\n"
curl -d '{
    "op_type":"OP_CREATE_DATABASE",
    "database_info": {
        "database":"cluster_status",
        "namespace_name":"CLUSTER_STATUS",
        "quota": 5368709120
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

echo -e "create user\n"
curl -d '{
    "op_type":"OP_CREATE_USER",
    "user_privilege" : {
        "username" : "******",
        "password" : "******",
        "namespace_name" : "CLUSTER_STATUS",
        "privilege_database" : [{
                                    "database" : "cluster_status",
                                    "database_rw" : 2
                                }],
        "bns":["preonline", "offline"],
        "ip":["127.0.0.1", "127.0.0.2"]
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"



